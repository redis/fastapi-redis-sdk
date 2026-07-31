"""Unit tests for RateLimitBackend (against fakeredis + error injection)."""

from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis.aioredis
import pytest
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError

from redis_fastapi.ratelimit_backend import (
    RateLimitBackend,
    SyncRateLimitBackend,
    _BackendCapabilities,
    probe_increx_support,
)


@pytest.fixture()
def backend(fake_async_redis: fakeredis.aioredis.FakeRedis) -> RateLimitBackend:
    return RateLimitBackend(fake_async_redis)


@pytest.mark.unit
@pytest.mark.asyncio
class TestHit:
    async def test_allows_up_to_limit_then_rejects(
        self, backend: RateLimitBackend
    ) -> None:
        outcomes = [
            (await backend.hit("ip", limit=3, window=60)).allowed for _ in range(5)
        ]
        assert outcomes == [True, True, True, False, False]

    async def test_remaining_counts_down(self, backend: RateLimitBackend) -> None:
        r1 = await backend.hit("ip", limit=3, window=60)
        r2 = await backend.hit("ip", limit=3, window=60)
        assert (r1.remaining, r2.remaining) == (2, 1)
        assert r1.limit == 3

    async def test_reset_after_within_window(self, backend: RateLimitBackend) -> None:
        result = await backend.hit("ip", limit=3, window=60)
        assert 0 < result.reset_after <= 60
        assert result.reset_at >= result.reset_after

    async def test_cost_consumes_multiple(self, backend: RateLimitBackend) -> None:
        result = await backend.hit("ip", limit=10, window=60, cost=4)
        assert result.allowed
        assert result.remaining == 6

    async def test_cost_exceeding_remaining_rejected(
        self, backend: RateLimitBackend
    ) -> None:
        await backend.hit("ip", limit=5, window=60, cost=4)
        result = await backend.hit("ip", limit=5, window=60, cost=4)
        assert result.allowed is False
        assert result.remaining == 1  # counter unchanged at 4

    async def test_scope_isolation(self, backend: RateLimitBackend) -> None:
        a = await backend.hit("ip", limit=1, window=60, scope="a")
        b = await backend.hit("ip", limit=1, window=60, scope="b")
        assert a.allowed and b.allowed  # different scopes, separate counters

    async def test_key_is_flat_without_hash_tag(
        self, backend: RateLimitBackend
    ) -> None:
        # Rate-limit keys must NOT use a Redis Cluster hash-tag: co-locating a
        # whole scope on one slot would create a hot shard.  The scope is a
        # plain segment so per-client counters spread across slots.
        key = backend._build_key("1.2.3.4", scope="search")
        assert "{" not in key and "}" not in key
        assert key.endswith(":search:1.2.3.4")

    async def test_distinct_identifiers_separate(
        self, backend: RateLimitBackend
    ) -> None:
        a = await backend.hit("ip-1", limit=1, window=60)
        b = await backend.hit("ip-2", limit=1, window=60)
        assert a.allowed and b.allowed


@pytest.mark.unit
@pytest.mark.asyncio
class TestCapabilityDetection:
    """Server-capability detection is shared and paid once per process."""

    async def test_result_reports_backend(self, backend: RateLimitBackend) -> None:
        # fakeredis rejects INCREX but runs Lua (via lupa), so the check is
        # served by the real Lua tier — the production path for Redis < 8.8.
        result = await backend.hit("ip", limit=3, window=60)
        assert result.backend == "lua"

    async def test_shared_cache_detects_once_across_instances(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        # Two per-request backends sharing one capability cache must ask the
        # server about INCREX exactly once between them, not once each.
        caps = _BackendCapabilities()
        with patch.object(
            fake_async_redis,
            "execute_command",
            wraps=fake_async_redis.execute_command,
        ) as spy:
            first = RateLimitBackend(fake_async_redis, capabilities=caps)
            await first.hit("ip", limit=5, window=60)
            second = RateLimitBackend(fake_async_redis, capabilities=caps)
            await second.hit("ip", limit=5, window=60)

        probes = [c for c in spy.call_args_list if c.args[:2] == ("COMMAND", "INFO")]
        assert len(probes) == 1
        assert caps.supports_increx is False

    async def test_probe_never_sends_increx_to_an_old_server(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        # Detection is by capability lookup, not by firing INCREX and reading
        # the error, so a server without the command never receives it.
        # `_increx` is the only caller of `pipeline()`, making the
        # pipeline-creation count the "did we try INCREX" count.
        with patch.object(
            fake_async_redis, "pipeline", wraps=fake_async_redis.pipeline
        ) as spy:
            result = await RateLimitBackend(fake_async_redis).hit(
                "ip", limit=5, window=60
            )

        assert spy.call_count == 0
        assert result.backend == "lua"

    async def test_probe_reports_support(self) -> None:
        # A server that advertises INCREX answers COMMAND INFO with a one-entry
        # table keyed by the command name.
        client = AsyncMock()
        client.execute_command.return_value = {"increx": {"name": "increx"}}
        assert await probe_increx_support(client) is True

    async def test_probe_reports_absence(self) -> None:
        # redis-py indexes into the nil entry the server returns for a command
        # it does not have; that TypeError is the "unsupported" answer.
        client = AsyncMock()
        client.execute_command.side_effect = TypeError(
            "'NoneType' object is not subscriptable"
        )
        assert await probe_increx_support(client) is False

    async def test_probe_is_undecided_when_redis_is_down(self) -> None:
        # An unreachable server answers nothing, which must not be cached as
        # either capability — the next request asks again.
        assert await probe_increx_support(_BrokenRedis()) is None  # type: ignore[arg-type]

    async def test_undecided_probe_is_not_cached(self) -> None:
        caps = _BackendCapabilities()
        backend = RateLimitBackend(_BrokenRedis(), capabilities=caps)  # type: ignore[arg-type]
        await backend.hit("ip", limit=5, window=60)
        assert caps.supports_increx is None

    async def test_probe_targets_a_node_in_cluster_mode(self) -> None:
        # COMMAND INFO is keyless, so a cluster client has no slot to route it
        # by; the probe names a node rather than depending on default routing.
        client = MagicMock(spec=AsyncRedisCluster)
        client.execute_command = AsyncMock(return_value={"increx": {}})
        assert await probe_increx_support(client) is True
        assert client.execute_command.await_args.kwargs["target_nodes"] is not None

    async def test_default_cache_is_per_instance(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        # Without a shared cache, each backend keeps its own detection state
        # (isolation for standalone/test use).
        a = RateLimitBackend(fake_async_redis)
        b = RateLimitBackend(fake_async_redis)
        await a.hit("ip", limit=5, window=60)
        assert a._caps is not b._caps
        assert b._caps.supports_increx is None


@pytest.mark.unit
@pytest.mark.asyncio
class TestResetPeek:
    async def test_reset_clears_counter(self, backend: RateLimitBackend) -> None:
        await backend.hit("ip", limit=1, window=60)
        assert (await backend.hit("ip", limit=1, window=60)).allowed is False
        assert await backend.reset("ip") is True
        assert (await backend.hit("ip", limit=1, window=60)).allowed is True

    async def test_peek_does_not_consume(self, backend: RateLimitBackend) -> None:
        await backend.hit("ip", limit=2, window=60)
        peek = await backend.peek("ip", limit=2, window=60)
        assert peek.remaining == 1
        # peek again — unchanged
        assert (await backend.peek("ip", limit=2, window=60)).remaining == 1


class _BrokenPipeline:
    """Pipeline stand-in that fails on execution (mirrors a downed server).

    Creating the pipeline and queueing commands succeeds — as it does with a
    real client — but the round-trip in ``execute()`` raises, so the backend's
    error handling (fail-open / fail-closed) is exercised.
    """

    def execute_command(self, *args: object, **kwargs: object) -> "_BrokenPipeline":
        return self

    def pttl(self, *args: object, **kwargs: object) -> "_BrokenPipeline":
        return self

    async def execute(self, *args: object, **kwargs: object) -> object:
        raise RedisConnectionError("redis down")


class _BrokenRedis:
    """Async Redis stand-in whose commands always fail."""

    def pipeline(self, *args: object, **kwargs: object) -> _BrokenPipeline:
        return _BrokenPipeline()

    async def execute_command(self, *args: object, **kwargs: object) -> object:
        raise RedisConnectionError("redis down")

    async def get(self, *args: object) -> object:
        raise RedisConnectionError("redis down")

    async def delete(self, *args: object) -> object:
        raise RedisConnectionError("redis down")


@pytest.mark.unit
@pytest.mark.asyncio
class TestFailureModes:
    async def test_fail_open_allows(self) -> None:
        backend = RateLimitBackend(_BrokenRedis())  # type: ignore[arg-type]
        result = await backend.hit("ip", limit=1, window=60, fail_closed=False)
        assert result.allowed is True

    async def test_fail_closed_rejects(self) -> None:
        backend = RateLimitBackend(_BrokenRedis())  # type: ignore[arg-type]
        result = await backend.hit("ip", limit=1, window=60, fail_closed=True)
        assert result.allowed is False
        assert result.remaining == 0

    async def test_reset_returns_false_on_error(self) -> None:
        # A Redis error while deleting the counter must be swallowed and
        # reported as "nothing was cleared" rather than propagating.
        backend = RateLimitBackend(_BrokenRedis())  # type: ignore[arg-type]
        assert await backend.reset("ip") is False

    async def test_peek_degrades_on_error(self) -> None:
        # A Redis error while reading must yield a degraded result, not raise.
        backend = RateLimitBackend(_BrokenRedis())  # type: ignore[arg-type]
        result = await backend.peek("ip", limit=5, window=60)
        assert result.degraded is True


class _RefusingPipeline:
    """Pipeline whose execution is refused with a fixed error message."""

    def __init__(self, message: str) -> None:
        self._message = message

    def execute_command(self, *args: object, **kwargs: object) -> "_RefusingPipeline":
        return self

    def pttl(self, *args: object, **kwargs: object) -> "_RefusingPipeline":
        return self

    async def execute(self, *args: object, **kwargs: object) -> object:
        raise RedisError(self._message)


class _IncrexRefusingRedis:
    """fakeredis whose INCREX pipeline is refused, everything else working.

    Models the cluster failure mode: a cluster client maps a command to a hash
    slot from the server's ``COMMAND`` table *before* sending it, so against a
    server that has no INCREX the pipeline is rejected client-side, with a
    message the server never produced.  Lua still works, because redis-py
    special-cases EVAL and takes the slot straight from the key argument.
    """

    def __init__(self, inner: fakeredis.aioredis.FakeRedis, message: str) -> None:
        self._inner = inner
        self._message = message

    def pipeline(self, *args: object, **kwargs: object) -> _RefusingPipeline:
        return _RefusingPipeline(self._message)

    def register_script(self, script: str) -> object:
        return self._inner.register_script(script)

    async def execute_command(self, *args: object, **kwargs: object) -> object:
        return await self._inner.execute_command(*args, **kwargs)


@pytest.mark.unit
@pytest.mark.asyncio
class TestClusterFallback:
    """INCREX rejection in cluster mode must reach Lua, not the degraded path.

    Against a pre-8.8 cluster the rejection is raised by redis-py itself and
    worded differently from the server's ``unknown command``.  Reading that as
    a backend outage would silently disable rate limiting for the process.
    """

    CLUSTER_REFUSAL = "INCREX command doesn't exist in Redis commands"

    async def test_client_side_refusal_falls_back_to_lua(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        caps = _BackendCapabilities(supports_increx=True)  # stale/raced detection
        client = _IncrexRefusingRedis(fake_async_redis, self.CLUSTER_REFUSAL)
        backend = RateLimitBackend(client, capabilities=caps)  # type: ignore[arg-type]

        result = await backend.hit("ip", limit=3, window=60)

        assert result.backend == "lua"
        assert result.degraded is False
        assert result.allowed is True
        assert caps.supports_increx is False  # downgraded for the process

    async def test_counter_still_enforced_after_fallback(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        # The regression this guards: every request coming back "allowed" and
        # degraded, i.e. no rate limiting at all on a pre-8.8 cluster.
        client = _IncrexRefusingRedis(fake_async_redis, self.CLUSTER_REFUSAL)
        backend = RateLimitBackend(
            client,  # type: ignore[arg-type]
            capabilities=_BackendCapabilities(supports_increx=True),
        )
        outcomes = [(await backend.hit("ip", limit=2, window=60)) for _ in range(4)]
        assert [o.allowed for o in outcomes] == [True, True, False, False]
        assert not any(o.degraded for o in outcomes)

    async def test_arity_error_is_not_treated_as_unsupported(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        # A malformed INCREX call is our bug, not an old server.  Downgrading
        # on it would hide the defect behind a working Lua path forever.
        caps = _BackendCapabilities(supports_increx=True)
        client = _IncrexRefusingRedis(
            fake_async_redis, "ERR wrong number of arguments for 'increx' command"
        )
        backend = RateLimitBackend(client, capabilities=caps)  # type: ignore[arg-type]

        result = await backend.hit("ip", limit=3, window=60, fail_closed=False)

        assert result.degraded is True  # surfaced, not silently absorbed
        assert caps.supports_increx is True  # no permanent downgrade

    async def test_server_unknown_command_still_falls_back(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        # The standalone wording keeps working alongside the cluster one.
        caps = _BackendCapabilities(supports_increx=True)
        client = _IncrexRefusingRedis(
            fake_async_redis, "ERR unknown command 'INCREX', with args beginning with"
        )
        backend = RateLimitBackend(client, capabilities=caps)  # type: ignore[arg-type]

        assert (await backend.hit("ip", limit=3, window=60)).backend == "lua"
        assert caps.supports_increx is False


@pytest.mark.unit
@pytest.mark.asyncio
class TestCostValidation:
    """`cost` is developer-supplied, so a nonsensical value fails loudly."""

    @pytest.mark.parametrize("cost", [0, -1, -100])
    async def test_rejects_non_positive_cost(
        self, backend: RateLimitBackend, cost: int
    ) -> None:
        with pytest.raises(ValueError, match="cost must be >= 1"):
            await backend.hit("ip", limit=5, window=60, cost=cost)

    async def test_zero_cost_does_not_create_the_key(
        self, backend: RateLimitBackend, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        # It used to create the counter at 0 and start the window's TTL, so the
        # first request that did consume something got a shortened window.
        with pytest.raises(ValueError):
            await backend.hit("ip", limit=5, window=60, cost=0)
        assert await fake_async_redis.keys("*") == []

    async def test_negative_cost_cannot_refund_the_counter(
        self, backend: RateLimitBackend
    ) -> None:
        # A decrement would let a caller hand back requests it already spent.
        for _ in range(3):
            await backend.hit("ip", limit=3, window=60)
        with pytest.raises(ValueError):
            await backend.hit("ip", limit=3, window=60, cost=-2)
        assert (await backend.hit("ip", limit=3, window=60)).allowed is False

    def test_sync_facade_rejects_too(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        import anyio

        backend = SyncRateLimitBackend(RateLimitBackend(fake_async_redis))

        async def _run() -> None:
            def _call() -> None:
                backend.hit("ip", limit=5, window=60, cost=0)

            with pytest.raises(ValueError):
                await anyio.to_thread.run_sync(_call)

        anyio.run(_run)


@pytest.mark.unit
class TestSyncFacade:
    def test_sync_hit(self, fake_async_redis: fakeredis.aioredis.FakeRedis) -> None:
        import anyio

        backend = SyncRateLimitBackend(RateLimitBackend(fake_async_redis))

        async def _run() -> None:
            # SyncRateLimitBackend must be driven from a worker thread.
            results = await anyio.to_thread.run_sync(
                lambda: [
                    backend.hit("ip", limit=2, window=60).allowed for _ in range(3)
                ]
            )
            assert results == [True, True, False]

        anyio.run(_run)
