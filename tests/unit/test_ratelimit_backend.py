"""Unit tests for RateLimitBackend (against fakeredis + error injection)."""

from unittest.mock import patch

import fakeredis.aioredis
import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from redis_fastapi.ratelimit_backend import (
    RateLimitBackend,
    SyncRateLimitBackend,
    _BackendCapabilities,
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
        # Two per-request backends sharing one capability cache must probe the
        # unsupported INCREX command exactly once between them, not once each.
        # INCREX is issued through a pipeline (INCREX + PTTL in one round-trip),
        # and `_increx` is the only caller of `pipeline()` in `hit()`, so the
        # pipeline-creation count is the INCREX-probe count.
        caps = _BackendCapabilities()
        with patch.object(
            fake_async_redis,
            "pipeline",
            wraps=fake_async_redis.pipeline,
        ) as spy:
            first = RateLimitBackend(fake_async_redis, capabilities=caps)
            await first.hit("ip", limit=5, window=60)
            second = RateLimitBackend(fake_async_redis, capabilities=caps)
            await second.hit("ip", limit=5, window=60)

        assert spy.call_count == 1
        assert caps.supports_increx is False

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
