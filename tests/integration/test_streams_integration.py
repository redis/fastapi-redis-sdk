"""Integration tests for resumable streams against a real Redis server.

The feature needs Redis 8.6.2 or later (``XADD ... IDMP``, ``DELEX``,
``SET ... IFEQ``).  On a server that has them every test here runs; on an
older one (the 7.4 CI entry) only :class:`TestUnsupportedServer` runs, and
checks that the feature turns itself off while the application keeps running.

Apps are driven through ``TestClient``; a few tests use the backend directly
with a real async client, where an HTTP round trip would only add noise.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import Awaitable, Callable, Iterator
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
import redis as sync_redis
import redis.asyncio as async_redis
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from redis.crc import key_slot
from redis.exceptions import TimeoutError as RedisTimeoutError

from redis_fastapi import (
    FastAPIRedis,
    StreamBackend,
    StreamBackendDep,
    StreamBusy,
    StreamExpired,
    StreamFenced,
    StreamFinished,
    StreamNotFound,
    StreamWriter,
)
from redis_fastapi.config import get_settings
from redis_fastapi.stream_backend import _keys, _StreamCapabilities
from tests.conftest import REDIS_URL, requires_redis


def _server_supports_streams() -> bool | None:
    """``True`` / ``False`` for a reachable server, ``None`` when unreachable."""
    try:
        client = sync_redis.Redis.from_url(REDIS_URL)
        client.ping()
    except Exception:
        return None
    try:
        reply = client.execute_command("COMMAND", "INFO", "XCFGSET", "DELEX")
    except TypeError:
        return False
    finally:
        client.close()
    entries = list(reply.values()) if isinstance(reply, dict) else list(reply)
    return len(entries) == 2 and all(entries)


_SUPPORT = _server_supports_streams()
requires_streams = pytest.mark.skipif(
    _SUPPORT is not True, reason="Redis without resumable-stream support"
)
requires_old_redis = pytest.mark.skipif(
    _SUPPORT is not False, reason="needs a Redis without stream support"
)


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


@pytest.fixture()
def settings_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[Callable[..., None]]:
    """Set ``REDIS_*`` variables for one test, with a fresh settings cache."""

    def apply(**values: Any) -> None:
        for name, value in values.items():
            monkeypatch.setenv(f"REDIS_{name.upper()}", str(value))
        get_settings.cache_clear()

    monkeypatch.setenv("REDIS_URL", REDIS_URL)
    get_settings.cache_clear()
    yield apply
    get_settings.cache_clear()


@pytest.fixture()
def scope() -> str:
    return f"chat:{uuid.uuid4().hex[:8]}"


@pytest.fixture()
async def backend(
    settings_env: Callable[..., None], real_async_redis: async_redis.Redis
) -> StreamBackend:
    """A backend on a real async client, outside any app."""
    instance = StreamBackend(lambda: real_async_redis)
    instance._start(_StreamCapabilities())
    assert await instance.detect() is True
    return instance


Producer = Callable[[StreamWriter], Awaitable[None]]


def _app(producer: Producer | None = None) -> FastAPI:
    """An app with a start route, an events route and a cancel route."""
    app = FastAPI()
    FastAPIRedis(app).lifespan().streams()

    @app.post("/{scope}/runs/{run_id}")
    async def start(scope: str, run_id: str, streams: StreamBackendDep) -> Any:
        if producer is None:
            writer = await streams.open(scope=scope, stream_id=run_id)
        else:
            writer = await streams.start(producer, scope=scope, stream_id=run_id)
        return {"stream_id": writer.stream_id, "created": writer.created}

    @app.get("/{scope}/runs/{run_id}/events")
    async def events(
        scope: str,
        run_id: str,
        request: Request,
        streams: StreamBackendDep,
        override: str | None = None,
        expires_in: float | None = None,
    ) -> Any:
        expires_at = (
            datetime.now(timezone.utc) + timedelta(seconds=expires_in)
            if expires_in is not None
            else None
        )
        return await streams.sse(
            run_id,
            request=request,
            scope=scope,
            after=override,
            expires_at=expires_at,
        )

    @app.post("/{scope}/runs/{run_id}/cancel")
    async def cancel(scope: str, run_id: str, streams: StreamBackendDep) -> Any:
        return {"cancelled": await streams.cancel(run_id, scope=scope)}

    @app.get("/{scope}/active")
    async def active(scope: str, streams: StreamBackendDep) -> Any:
        return {"stream_id": await streams.active(scope)}

    return app


def _read(
    client: TestClient, url: str, headers: dict[str, str] | None = None
) -> tuple[int, list[dict[str, Any]]]:
    """Read an SSE response to its end.  Returns the status and the frames."""
    frames: list[dict[str, Any]] = []
    with client.stream("GET", url, headers=headers or {}) as response:
        status = response.status_code
        current: dict[str, Any] = {}
        for line in response.iter_lines():
            if not line:
                if current:
                    frames.append(current)
                current = {}
                continue
            if line.startswith(":"):
                current.setdefault("comment", line)
                continue
            name, _, value = line.partition(": ")
            if name == "data":
                current["data"] = json.loads(value)
            else:
                current[name] = value
        if current:
            frames.append(current)
    return status, [f for f in frames if "event" in f]


def _deltas(count: int, final: bool = True) -> Producer:
    async def produce(writer: StreamWriter) -> None:
        for i in range(count):
            await writer.append("delta", {"i": i})
        if final:
            await writer.finish("message", {"count": count})

    return produce


async def _collect(
    backend: StreamBackend, stream_id: str, scope: str, after: str | None = None
) -> list[Any]:
    events = []
    async with await backend.follow(stream_id, scope=scope, after=after) as reader:
        async for event in reader:
            events.append(event)
    return events


async def _alive_ms(redis: Any, keys: Any) -> int:
    """The heartbeat field's remaining lifetime in ms, or a negative code."""
    reply = await redis.execute_command("HPTTL", keys.meta, "FIELDS", 1, "alive")
    return int(reply[0])


async def _flatten(pages: Any) -> list[Any]:
    """Every entry the read loop yields, with ``None`` for each keepalive."""
    seen: list[Any] = []
    async for page in pages:
        seen.extend(page or [None])
    return seen


_ASGI = {"type": "http", "asgi": {"spec_version": "2.4"}}


async def _wait_forever() -> Any:
    await asyncio.sleep(3600)


def _get(query: bytes = b"") -> Request:
    return Request({"type": "http", "headers": [], "query_string": query})


# ---------------------------------------------------------------------------
# Server without support
# ---------------------------------------------------------------------------


@requires_redis
@requires_old_redis
@pytest.mark.integration
class TestUnsupportedServer:
    def test_feature_turns_off_and_the_app_runs(
        self,
        settings_env: Callable[..., None],
        caplog: pytest.LogCaptureFixture,
        scope: str,
    ) -> None:
        app = _app()

        @app.get("/health")
        async def health() -> Any:
            return {"ok": True}

        with TestClient(app) as client:
            assert client.get("/health").status_code == 200
            assert client.post(f"/{scope}/runs/r1").status_code == 503
            assert client.get(f"/{scope}/runs/r1/events").status_code == 503
            assert client.get(f"/{scope}/active").status_code == 503
        assert "Resumable streams are off" in caplog.text


# ---------------------------------------------------------------------------
# End to end over HTTP
# ---------------------------------------------------------------------------


@requires_redis
@requires_streams
@pytest.mark.integration
class TestHttp:
    def test_live_read_then_exact_resume(
        self, settings_env: Callable[..., None], real_redis: Any, scope: str
    ) -> None:
        with TestClient(_app(_deltas(5))) as client:
            assert client.post(f"/{scope}/runs/r1").json()["created"] is True
            status, frames = _read(client, f"/{scope}/runs/r1/events")
            assert status == 200
            assert [f["event"] for f in frames] == ["delta"] * 5 + ["message"]
            assert [f["data"]["i"] for f in frames[:5]] == [0, 1, 2, 3, 4]
            ids = [f["id"] for f in frames]

            _, resumed = _read(
                client, f"/{scope}/runs/r1/events", {"Last-Event-ID": ids[1]}
            )
            assert [f["id"] for f in resumed] == ids[2:]

    def test_cursor_order(
        self, settings_env: Callable[..., None], real_redis: Any, scope: str
    ) -> None:
        with TestClient(_app(_deltas(4))) as client:
            client.post(f"/{scope}/runs/r1")
            _, frames = _read(client, f"/{scope}/runs/r1/events")
            ids = [f["id"] for f in frames]
            url = f"/{scope}/runs/r1/events"

            # A newer header beats an older query: nothing replays twice.
            _, by_header = _read(
                client, f"{url}?after={ids[0]}", {"Last-Event-ID": ids[2]}
            )
            assert [f["id"] for f in by_header] == ids[3:]
            # The query alone.
            _, by_query = _read(client, f"{url}?after={ids[2]}")
            assert [f["id"] for f in by_query] == ids[3:]
            # A value the handler passes beats both.
            _, by_handler = _read(
                client,
                f"{url}?after={ids[2]}&override={ids[0]}",
                {"Last-Event-ID": ids[2]},
            )
            assert [f["id"] for f in by_handler] == ids[1:]

    def test_reconnect_after_the_end_is_204(
        self, settings_env: Callable[..., None], real_redis: Any, scope: str
    ) -> None:
        with TestClient(_app(_deltas(2))) as client:
            client.post(f"/{scope}/runs/r1")
            _, frames = _read(client, f"/{scope}/runs/r1/events")
            final = frames[-1]["id"]
            status, again = _read(
                client, f"/{scope}/runs/r1/events", {"Last-Event-ID": final}
            )
            assert status == 204 and again == []

    def test_unknown_and_foreign_streams_are_404(
        self, settings_env: Callable[..., None], real_redis: Any, scope: str
    ) -> None:
        with TestClient(_app(_deltas(1))) as client:
            client.post(f"/{scope}/runs/r1")
            _read(client, f"/{scope}/runs/r1/events")
            other = f"chat:{uuid.uuid4().hex[:8]}"
            assert client.get(f"/{other}/runs/r1/events").status_code == 404
            assert client.post(f"/{other}/runs/r1/cancel").status_code == 404
            assert client.get(f"/{scope}/runs/nope/events").status_code == 404
            missing = client.get(f"/{scope}/runs/nope/events").json()
            assert missing["code"] == "stream_not_found"

    def test_an_invalid_id_is_422_on_every_route(
        self, settings_env: Callable[..., None], real_redis: Any, scope: str
    ) -> None:
        with TestClient(_app()) as client:
            for response in (
                client.post(f"/{scope}/runs/bad.id"),
                client.get(f"/{scope}/runs/bad.id/events"),
                client.post(f"/{scope}/runs/bad.id/cancel"),
                client.get("/bad scope/active"),
            ):
                assert response.status_code == 422
                assert response.json()["code"] == "invalid_stream_name"

    @pytest.mark.parametrize(
        ("query", "headers"),
        [
            ("after=$", {}),
            ("after=123", {}),
            ("after=%2B", {}),
            (f"after={2**64}-0", {}),
            ("after=1-0&after=2-0", {}),
            ("", {"Last-Event-ID": "junk"}),
        ],
    )
    def test_invalid_cursors_are_422(
        self,
        settings_env: Callable[..., None],
        real_redis: Any,
        scope: str,
        query: str,
        headers: dict[str, str],
    ) -> None:
        with TestClient(_app(_deltas(1))) as client:
            client.post(f"/{scope}/runs/r1")
            response = client.get(f"/{scope}/runs/r1/events?{query}", headers=headers)
            assert response.status_code == 422

    def test_cursor_ahead_is_409(
        self, settings_env: Callable[..., None], real_redis: Any, scope: str
    ) -> None:
        with TestClient(_app()) as client:
            client.post(f"/{scope}/runs/r1")  # no producer: no entries yet
            assert client.get(f"/{scope}/runs/r1/events?after=5-0").status_code == 409

        other = f"{scope}-b"  # r1 still holds the first scope's slot
        with TestClient(_app(_deltas(1))) as client:
            client.post(f"/{other}/runs/r2")
            _read(client, f"/{other}/runs/r2/events")
            far = f"{int(time.time() * 1000) + 10**9}-0"
            assert client.get(f"/{other}/runs/r2/events?after={far}").status_code == 409

    def test_retried_start_returns_the_same_run(
        self, settings_env: Callable[..., None], real_redis: Any, scope: str
    ) -> None:
        with TestClient(_app(_deltas(3))) as client:
            first = client.post(f"/{scope}/runs/r1").json()
            second = client.post(f"/{scope}/runs/r1").json()
            assert first == {"stream_id": "r1", "created": True}
            assert second == {"stream_id": "r1", "created": False}
            _, frames = _read(client, f"/{scope}/runs/r1/events")
            assert [f["event"] for f in frames].count("delta") == 3  # one producer

    def test_busy_scope_is_409(
        self, settings_env: Callable[..., None], real_redis: Any, scope: str
    ) -> None:
        with TestClient(_app()) as client:
            client.post(f"/{scope}/runs/r1")
            response = client.post(f"/{scope}/runs/r2")
            assert response.status_code == 409
            assert "r1" in response.json()["detail"]
            assert response.json()["code"] == "stream_busy"
            assert client.get(f"/{scope}/active").json() == {"stream_id": "r1"}

    def test_cancel_stops_a_spawned_producer(
        self, settings_env: Callable[..., None], real_redis: Any, scope: str
    ) -> None:
        async def forever(writer: StreamWriter) -> None:
            while True:
                await writer.append("delta", {})
                await asyncio.sleep(0.02)

        with TestClient(_app(forever)) as client:
            client.post(f"/{scope}/runs/r1")
            time.sleep(0.1)
            assert client.post(f"/{scope}/runs/r1/cancel").json() == {"cancelled": True}
            _, frames = _read(client, f"/{scope}/runs/r1/events")
            assert frames[-1]["event"] == "stream.end"
            assert frames[-1]["data"] == {"reason": "cancelled"}
            assert client.get(f"/{scope}/active").json() == {"stream_id": None}
            assert client.post(f"/{scope}/runs/r1/cancel").json() == {
                "cancelled": False
            }

    @pytest.mark.parametrize(
        ("raises", "expected"),
        [
            (None, {"reason": "done"}),
            (RuntimeError("boom"), {"reason": "error", "code": "producer_error"}),
        ],
    )
    def test_spawn_writes_the_end_with_its_reason(
        self,
        settings_env: Callable[..., None],
        real_redis: Any,
        scope: str,
        raises: Exception | None,
        expected: dict[str, Any],
    ) -> None:
        async def produce(writer: StreamWriter) -> None:
            await writer.append("delta", {})
            if raises is not None:
                raise raises

        with TestClient(_app(produce)) as client:
            client.post(f"/{scope}/runs/r1")
            _, frames = _read(client, f"/{scope}/runs/r1/events")
            assert frames[-1]["event"] == "stream.end"
            assert frames[-1]["data"] == expected

    def test_an_error_code_attribute_is_reported(
        self, settings_env: Callable[..., None], real_redis: Any, scope: str
    ) -> None:
        class ModelError(Exception):
            code = "model_error"

        async def produce(writer: StreamWriter) -> None:
            raise ModelError

        with TestClient(_app(produce)) as client:
            client.post(f"/{scope}/runs/r1")
            _, frames = _read(client, f"/{scope}/runs/r1/events")
            assert frames[-1]["data"] == {"reason": "error", "code": "model_error"}

    def test_own_finish_means_no_extra_end(
        self, settings_env: Callable[..., None], real_redis: Any, scope: str
    ) -> None:
        with TestClient(_app(_deltas(1))) as client:
            client.post(f"/{scope}/runs/r1")
            _, frames = _read(client, f"/{scope}/runs/r1/events")
            assert [f["event"] for f in frames] == ["delta", "message"]

    def test_credential_deadline_and_connection_cap(
        self, settings_env: Callable[..., None], real_redis: Any, scope: str
    ) -> None:
        settings_env(stream_heartbeat_seconds=0.2, stream_alive_ttl_seconds=60)
        with TestClient(_app()) as client:
            client.post(f"/{scope}/runs/r1")
            # A deadline already passed: 401 before the first byte.
            assert (
                client.get(f"/{scope}/runs/r1/events?expires_in=-1").status_code == 401
            )
            # A deadline during the stream: a credential_expired event.
            _, frames = _read(client, f"/{scope}/runs/r1/events?expires_in=0.5")
            assert frames[-1]["event"] == "stream.error"
            assert frames[-1]["data"] == {"code": "credential_expired"}

        settings_env(
            stream_heartbeat_seconds=0.2,
            stream_alive_ttl_seconds=60,
            stream_max_connection_seconds=0.5,
        )
        other = f"{scope}-b"  # r1 still holds the first scope's slot
        with TestClient(_app()) as client:
            client.post(f"/{other}/runs/r2")
            started = time.monotonic()
            status, frames = _read(client, f"/{other}/runs/r2/events")
            assert status == 200 and frames == []  # the cap closes with no event
            assert time.monotonic() - started < 5

    def test_deadlines_are_kept_between_heartbeats(
        self, settings_env: Callable[..., None], real_redis: Any, scope: str
    ) -> None:
        # The default 15 s heartbeat: the deadline cuts the blocking read
        # short, so the stream closes on time and not at the next heartbeat.
        settings_env(stream_max_connection_seconds=0.5)
        with TestClient(_app()) as client:
            client.post(f"/{scope}/runs/r1")
            started = time.monotonic()
            _, frames = _read(client, f"/{scope}/runs/r1/events?expires_in=0.3")
            assert frames[-1]["data"] == {"code": "credential_expired"}
            assert time.monotonic() - started < 2

            started = time.monotonic()
            status, frames = _read(client, f"/{scope}/runs/r1/events")
            assert status == 200 and frames == []
            assert time.monotonic() - started < 2

    def test_shutdown_ends_spawned_producers(
        self, settings_env: Callable[..., None], real_redis: Any, scope: str
    ) -> None:
        async def forever(writer: StreamWriter) -> None:
            while True:
                await writer.append("delta", {})
                await asyncio.sleep(0.05)

        with TestClient(_app(forever)) as client:
            client.post(f"/{scope}/runs/r1")
            time.sleep(0.15)
        keys = _keys(scope, "r1")
        last = real_redis.xrevrange(keys.stream, count=1)
        assert last[0][1]["t"] == "stream.end"
        assert json.loads(last[0][1]["d"]) == {"reason": "cancelled"}

    def test_keys_follow_the_prefix_setting(
        self, settings_env: Callable[..., None], real_redis: Any, scope: str
    ) -> None:
        settings_env(prefix="myapp")
        with TestClient(_app(_deltas(1))) as client:
            client.post(f"/{scope}/runs/r1")
            _read(client, f"/{scope}/runs/r1/events")
        assert real_redis.exists(f"myapp:stream:{{{scope}}}:r1")
        assert real_redis.exists(f"myapp:stream-meta:{{{scope}}}:r1")
        assert not real_redis.exists(f"redis:fastapi:stream:{{{scope}}}:r1")


# ---------------------------------------------------------------------------
# The backend directly
# ---------------------------------------------------------------------------


@requires_redis
@requires_streams
@pytest.mark.integration
class TestBackend:
    async def test_follow_reads_everything_and_stops_at_the_end(
        self, backend: StreamBackend, scope: str
    ) -> None:
        writer = await backend.open(scope=scope, stream_id="r1")
        for i in range(3):
            await writer.append("delta", {"i": i})
        final = await writer.finish("message", {"ok": True})
        events = await _collect(backend, "r1", scope)
        assert [e.type for e in events] == ["delta"] * 3 + ["message"]
        assert events[-1].final and events[-1].id == final
        assert [e.data for e in events[:3]] == [{"i": 0}, {"i": 1}, {"i": 2}]

    async def test_a_start_retried_after_the_run_ended_is_not_created(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        writer = await backend.open(scope=scope, stream_id="r1")
        await writer.append("delta", {})
        await writer.finish("message", {})
        keys = _keys(scope, "r1")
        assert not await real_async_redis.exists(keys.active)  # slot freed

        retry = await backend.open(scope=scope, stream_id="r1")
        assert retry.created is False
        assert not await real_async_redis.exists(keys.active)  # not held again
        assert await real_async_redis.hget(keys.meta, "status") == "finished"
        with pytest.raises(StreamFenced):
            await retry.append("delta", {})
        # Another run can start in the scope.
        assert (await backend.open(scope=scope, stream_id="r2")).created is True

    async def test_a_start_retried_while_the_run_goes_on_is_not_created(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        writer = await backend.open(scope=scope, stream_id="r1")
        await writer.append("delta", {})
        retry = await backend.open(scope=scope, stream_id="r1")
        assert retry.created is False
        with pytest.raises(StreamFenced):
            await retry.append("delta", {})
        await writer.append("delta", {})  # the run's own writer is unaffected
        assert await backend.active(scope) == "r1"

    async def test_a_start_that_loses_the_watch_race_reads_again(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        keys = _keys(scope, "r1")
        original = real_async_redis.pipeline
        calls = {"n": 0}

        def pipeline(*args: Any, **kwargs: Any) -> Any:
            pipe = original(*args, **kwargs)
            calls["n"] += 1
            if calls["n"] == 2:  # the first read, after the WATCH
                execute = pipe.execute

                async def then_race(*a: Any, **kw: Any) -> Any:
                    replies = await execute(*a, **kw)
                    # Another client touches the slot before EXEC.
                    await real_async_redis.set(keys.active, "r1", ex=60)
                    return replies

                pipe.execute = then_race
            return pipe

        real_async_redis.pipeline = pipeline  # type: ignore[method-assign]
        try:
            writer = await backend.open(scope=scope, stream_id="r1")
        finally:
            real_async_redis.pipeline = original  # type: ignore[method-assign]
        assert calls["n"] == 4  # two attempts, each a WATCH and a read
        assert writer.created is True
        assert await real_async_redis.hget(keys.meta, "writer") == writer._writer_id

    async def test_a_start_whose_reply_was_lost_is_still_created(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        original = real_async_redis.pipeline
        calls = {"n": 0}

        def pipeline(*args: Any, **kwargs: Any) -> Any:
            pipe = original(*args, **kwargs)
            calls["n"] += 1
            if calls["n"] == 1:  # the first transaction, which creates the run
                execute = pipe.execute

                async def flaky(*a: Any, **kw: Any) -> Any:
                    await execute(*a, **kw)
                    raise RedisTimeoutError("reply lost")  # applied, but unseen

                pipe.execute = flaky
            return pipe

        real_async_redis.pipeline = pipeline  # type: ignore[method-assign]
        try:
            writer = await backend.open(scope=scope, stream_id="r1")
        finally:
            real_async_redis.pipeline = original  # type: ignore[method-assign]
        # The second attempt finds its own writer ID in meta: not a retry.
        assert writer.created is True
        await writer.append("delta", {})

    async def test_a_slot_left_without_meta_is_taken_over(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        keys = _keys(scope, "r1")
        # The run's meta expired or was evicted while its slot remained.
        await real_async_redis.set(keys.active, "r1", ex=60)
        writer = await backend.open(scope=scope, stream_id="r1")
        assert writer.created is True
        assert await real_async_redis.hget(keys.meta, "writer") == writer._writer_id
        assert await real_async_redis.get(keys.active) == "r1"

    async def test_concurrent_starts_of_one_run_create_it_once(
        self, backend: StreamBackend, scope: str
    ) -> None:
        writers = await asyncio.gather(
            *(backend.open(scope=scope, stream_id="r1") for _ in range(8))
        )
        assert sum(w.created for w in writers) == 1

    async def test_concurrent_starts_of_different_runs_admit_one(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        results = await asyncio.gather(
            *(backend.open(scope=scope, stream_id=f"r{i}") for i in range(8)),
            return_exceptions=True,
        )
        created = [r for r in results if isinstance(r, StreamWriter)]
        assert len(created) == 1 and created[0].created is True
        assert all(isinstance(r, StreamBusy) for r in results if r not in created)
        # Only the winner left keys behind: no other run has meta.
        for i in range(8):
            meta = _keys(scope, f"r{i}").meta
            assert bool(await real_async_redis.exists(meta)) == (
                f"r{i}" == created[0].stream_id
            )

    async def test_shutdown_survives_a_lost_cancel(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        # Python 3.10 and 3.11 asyncio.wait_for can lose a cancel that lands as
        # a socket write completes; this producer loses the first one on purpose.
        lost = {"n": 0}

        async def loses_a_cancel(writer: StreamWriter) -> None:
            while True:
                try:
                    await asyncio.sleep(0.02)
                except asyncio.CancelledError:
                    lost["n"] += 1
                    if lost["n"] > 1:
                        raise

        writer = await backend.open(scope=scope, stream_id="r1")
        backend.spawn(writer, loses_a_cancel)
        await asyncio.sleep(0.05)
        await asyncio.wait_for(backend.aclose(), 5)
        assert lost["n"] == 2
        last = await real_async_redis.xrevrange(_keys(scope, "r1").stream, count=1)
        assert last[0][1]["t"] == "stream.end"
        assert json.loads(last[0][1]["d"]) == {"reason": "cancelled"}

    async def test_shutdown_right_after_spawn_still_ends_the_stream(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        async def forever(writer: StreamWriter) -> None:
            while True:
                await asyncio.sleep(0.02)

        writer = await backend.open(scope=scope, stream_id="r1")
        backend.spawn(writer, forever)
        await asyncio.wait_for(backend.aclose(), 5)  # before the task has run
        last = await real_async_redis.xrevrange(_keys(scope, "r1").stream, count=1)
        assert last[0][1]["t"] == "stream.end"
        assert json.loads(last[0][1]["d"]) == {"reason": "cancelled"}

    async def test_shutdown_gives_up_on_a_producer_that_never_stops(
        self,
        backend: StreamBackend,
        scope: str,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        import redis_fastapi.stream_backend as stream_backend

        monkeypatch.setattr(stream_backend, "_SHUTDOWN_GRACE_SECONDS", 0.5)
        stop = asyncio.Event()

        async def stubborn(writer: StreamWriter) -> None:
            while not stop.is_set():
                try:
                    await asyncio.sleep(0.02)
                except asyncio.CancelledError:
                    pass  # never stops

        writer = await backend.open(scope=scope, stream_id="r1")
        task = backend.spawn(writer, stubborn)
        await asyncio.sleep(0.05)  # the producer is running
        started = time.monotonic()
        await asyncio.wait_for(backend.aclose(), 5)
        assert time.monotonic() - started < 3
        assert "did not stop" in caplog.text
        stop.set()
        await asyncio.wait_for(task, 5)

    async def test_append_after_finish_raises(
        self, backend: StreamBackend, scope: str
    ) -> None:
        writer = await backend.open(scope=scope)
        first = await writer.finish("message", {})
        with pytest.raises(StreamFinished):
            await writer.append("delta", {})
        assert await writer.finish("message", {}) == first  # idempotent

    async def test_a_writer_and_a_reader_ending_leave_one_final_entry(
        self,
        settings_env: Callable[..., None],
        real_async_redis: Any,
        scope: str,
    ) -> None:
        settings_env(stream_heartbeat_seconds=0.2, stream_alive_ttl_seconds=0.3)
        backend = StreamBackend(lambda: real_async_redis)
        backend._start(_StreamCapabilities(supported=True))
        writer = await backend.open(scope=scope, stream_id="r1")
        await writer.append("delta", {})
        await asyncio.sleep(0.4)  # the heartbeat lapses
        events, final = await asyncio.gather(
            _collect(backend, "r1", scope), writer.finish("message", {})
        )
        assert events[-1].id == final
        entries = await real_async_redis.xrange(_keys(scope, "r1").stream)
        assert sum(1 for _, fields in entries if fields.get("f") == "1") == 1

    async def test_a_replaced_writer_cannot_end_the_new_run(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        old = await backend.open(scope=scope, stream_id="r1")
        await old.append("delta", {"n": 1})
        new = await backend.attach("r1", scope=scope)
        with pytest.raises(StreamFenced):
            await old.finish("message", {"by": "old"})
        keys = _keys(scope, "r1")
        entries = await real_async_redis.xrange(keys.stream)
        assert all(fields.get("f") != "1" for _, fields in entries)
        assert await real_async_redis.hget(keys.meta, "status") == "running"
        await new.append("delta", {"n": 2})  # the run goes on
        final = await new.finish("message", {"by": "new"})
        assert (await _collect(backend, "r1", scope))[-1].id == final

    async def test_a_spawned_writer_replaced_mid_run_ends_quietly(
        self,
        backend: StreamBackend,
        scope: str,
        real_async_redis: Any,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        attached = asyncio.Event()

        async def produce(writer: StreamWriter) -> None:
            await writer.append("delta", {})
            await attached.wait()  # returns before the heartbeat sees the fence

        writer = await backend.open(scope=scope, stream_id="r1")
        task = backend.spawn(writer, produce)
        await asyncio.sleep(0.05)
        await backend.attach("r1", scope=scope)
        attached.set()
        await asyncio.wait_for(task, 5)
        entries = await real_async_redis.xrange(_keys(scope, "r1").stream)
        assert all(fields.get("f") != "1" for _, fields in entries)
        assert "Could not end" not in caplog.text

    async def test_attach_fences_the_old_writer(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        old = await backend.open(scope=scope, stream_id="r1")
        await old.append("delta", {"n": 1})
        new = await backend.attach("r1", scope=scope)
        with pytest.raises(StreamFenced):
            await old.append("delta", {"n": 2})  # this one lands, then fences
        with pytest.raises(StreamFenced):
            await old.append("delta", {"n": 3})  # refused locally
        await new.append("delta", {"n": 4})
        entries = await real_async_redis.xrange(_keys(scope, "r1").stream)
        assert [json.loads(f["d"])["n"] for _, f in entries] == [1, 2, 4]
        # The new writer continues the sequence numbers without a hole.
        assert [int(f["q"]) for _, f in entries] == [1, 2, 2]

    async def test_attach_rejects_unknown_and_finished(
        self, backend: StreamBackend, scope: str
    ) -> None:
        with pytest.raises(StreamNotFound):
            await backend.attach("nope", scope=scope)
        writer = await backend.open(scope=scope, stream_id="r1")
        await writer.finish("message", {})
        with pytest.raises(StreamFinished):
            await backend.attach("r1", scope=scope)

    async def test_a_retried_append_after_a_timeout_stores_one_entry(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        writer = await backend.open(scope=scope, stream_id="r1")
        original = real_async_redis.pipeline
        calls = {"n": 0}

        def pipeline(*args: Any, **kwargs: Any) -> Any:
            pipe = original(*args, **kwargs)
            execute = pipe.execute

            async def flaky(*a: Any, **kw: Any) -> Any:
                result = await execute(*a, **kw)
                calls["n"] += 1
                if calls["n"] == 1:
                    raise RedisTimeoutError("reply lost")  # applied, but unseen
                return result

            pipe.execute = flaky
            return pipe

        real_async_redis.pipeline = pipeline  # type: ignore[method-assign]
        try:
            entry_id = await writer.append("delta", {"once": True})
        finally:
            real_async_redis.pipeline = original  # type: ignore[method-assign]
        assert calls["n"] == 2
        entries = await real_async_redis.xrange(_keys(scope, "r1").stream)
        assert [stored for stored, _ in entries] == [entry_id]

    async def test_a_late_append_after_expiry_does_not_recreate_the_stream(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        writer = await backend.open(scope=scope, stream_id="r1")
        await writer.append("delta", {})
        keys = _keys(scope, "r1")
        await real_async_redis.delete(keys.stream)
        with pytest.raises(StreamExpired):
            await writer.append("delta", {})
        assert not await real_async_redis.exists(keys.stream)

    async def test_meta_outlives_the_stream(
        self,
        settings_env: Callable[..., None],
        real_async_redis: Any,
        scope: str,
    ) -> None:
        settings_env(stream_done_ttl_seconds=60, stream_gone_ttl_seconds=600)
        backend = StreamBackend(lambda: real_async_redis)
        backend._start(_StreamCapabilities(supported=True))
        writer = await backend.open(scope=scope, stream_id="r1")
        await writer.append("delta", {})
        await writer.finish("message", {})
        keys = _keys(scope, "r1")
        assert 55 <= await real_async_redis.ttl(keys.stream) <= 60
        assert 655 <= await real_async_redis.ttl(keys.meta) <= 660

    async def test_expired_is_410_then_404(
        self,
        settings_env: Callable[..., None],
        real_async_redis: Any,
        scope: str,
    ) -> None:
        settings_env(stream_done_ttl_seconds=1, stream_gone_ttl_seconds=2)
        backend = StreamBackend(lambda: real_async_redis)
        backend._start(_StreamCapabilities(supported=True))
        writer = await backend.open(scope=scope, stream_id="r1")
        await writer.append("delta", {})
        await writer.finish("message", {})
        await asyncio.sleep(1.5)
        with pytest.raises(StreamExpired):
            await _collect(backend, "r1", scope)
        await asyncio.sleep(2.5)
        with pytest.raises(StreamNotFound):
            await _collect(backend, "r1", scope)

    async def test_not_started_is_not_expired(
        self, backend: StreamBackend, scope: str
    ) -> None:
        await backend.open(scope=scope, stream_id="r1")
        plan = await backend._prepare("r1", scope=scope, after=None)
        assert plan is not None and plan.first == []

    async def test_trimmed_history_is_410(
        self,
        settings_env: Callable[..., None],
        real_async_redis: Any,
        scope: str,
    ) -> None:
        settings_env(stream_maxlen=10)
        backend = StreamBackend(lambda: real_async_redis)
        backend._start(_StreamCapabilities(supported=True))
        writer = await backend.open(scope=scope, stream_id="r1")
        first = await writer.append("delta", {"n": 0})
        for n in range(1, 300):  # past a node, so the approximate trim acts
            await writer.append("delta", {"n": n})
        await writer.finish("message", {})
        with pytest.raises(StreamExpired):
            await _collect(backend, "r1", scope, after=first)
        with pytest.raises(StreamExpired):
            await _collect(backend, "r1", scope)  # no cursor means 0-0

    async def test_a_trim_during_the_read_is_a_gap_event(
        self,
        settings_env: Callable[..., None],
        real_async_redis: Any,
        scope: str,
    ) -> None:
        settings_env(stream_replay_page=2)
        backend = StreamBackend(lambda: real_async_redis)
        backend._start(_StreamCapabilities(supported=True))
        writer = await backend.open(scope=scope, stream_id="r1")
        for n in range(6):
            await writer.append("delta", {"n": n})
        plan = await backend._prepare("r1", scope=scope, after=None)
        assert plan is not None and len(plan.first) == 2
        # Entries 3 and 4 vanish while the reader is between pages.
        keys = _keys(scope, "r1")
        entries = await real_async_redis.xrange(keys.stream)
        await real_async_redis.xdel(keys.stream, entries[2][0], entries[3][0])
        await writer.finish("message", {})
        seen = [backend._event(e) for e in await _flatten(backend._pages(plan)) if e]
        assert [(e.type, e.data) for e in seen[:3]] == [
            ("delta", {"n": 0}),
            ("delta", {"n": 1}),
            ("stream.gap", {"missing": 2}),
        ]
        assert seen[-1].final

    async def test_producer_lost_is_ended_by_the_reader(
        self,
        settings_env: Callable[..., None],
        real_async_redis: Any,
        scope: str,
    ) -> None:
        settings_env(stream_heartbeat_seconds=0.2, stream_alive_ttl_seconds=0.5)
        backend = StreamBackend(lambda: real_async_redis)
        backend._start(_StreamCapabilities(supported=True))
        writer = await backend.open(scope=scope, stream_id="r1")  # never spawned
        await writer.append("delta", {"n": 1})
        first, second = await asyncio.gather(
            _collect(backend, "r1", scope), _collect(backend, "r1", scope)
        )
        for events in (first, second):
            assert events[-1].type == "stream.end"
            assert events[-1].data == {"reason": "producer_lost"}
            assert events[-1].final
        keys = _keys(scope, "r1")
        entries = await real_async_redis.xrange(keys.stream)
        assert sum(1 for _, f in entries if f.get("f") == "1") == 1
        assert await backend.active(scope) is None
        # The slow producer comes back: it gets the reader's final entry.
        assert await writer.finish("message", {}) == entries[-1][0]
        assert await _collect(backend, "r1", scope, after=entries[-1][0]) == []

    async def test_a_stale_writer_does_not_free_a_newer_run(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        stale = await backend.open(scope=scope, stream_id="r1")
        keys = _keys(scope, "r1")
        # The slot moved on to a newer run, for example after it expired.
        await real_async_redis.set(keys.active, "r2", ex=60)
        await stale.finish("message", {})
        assert await real_async_redis.get(keys.active) == "r2"

    async def test_extend_keeps_ttls_and_the_heartbeat(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        writer = await backend.open(scope=scope, stream_id="r1")
        await writer.append("delta", {})
        await writer.extend(timedelta(hours=1))
        keys = _keys(scope, "r1")
        assert 3590 <= await real_async_redis.ttl(keys.stream) <= 3600
        assert await _alive_ms(real_async_redis, keys) > 3_500_000
        assert 3590 <= await real_async_redis.ttl(keys.active) <= 3600

    async def test_keepalive_refreshes_the_heartbeat(
        self,
        settings_env: Callable[..., None],
        real_async_redis: Any,
        scope: str,
    ) -> None:
        settings_env(stream_heartbeat_seconds=0.2, stream_alive_ttl_seconds=0.5)
        backend = StreamBackend(lambda: real_async_redis)
        backend._start(_StreamCapabilities(supported=True))
        writer = await backend.open(scope=scope, stream_id="r1")
        keys = _keys(scope, "r1")
        async with writer.keepalive():
            await asyncio.sleep(1.0)  # twice the heartbeat field's lifetime
            assert await real_async_redis.hexists(keys.meta, "alive")
        await asyncio.sleep(0.7)
        assert not await real_async_redis.hexists(keys.meta, "alive")
        assert await real_async_redis.hget(keys.meta, "status") == "running"

    async def test_an_append_refreshes_the_active_slot(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        writer = await backend.open(scope=scope, stream_id="r1")
        keys = _keys(scope, "r1")
        await real_async_redis.expire(keys.active, 5)
        await writer.append("delta", {})
        ttl = get_settings().stream_active_ttl_seconds
        assert ttl - 2 <= await real_async_redis.ttl(keys.active) <= ttl

    async def test_keepalive_refreshes_the_active_slot(
        self,
        settings_env: Callable[..., None],
        real_async_redis: Any,
        scope: str,
    ) -> None:
        settings_env(stream_heartbeat_seconds=0.2, stream_alive_ttl_seconds=0.5)
        backend = StreamBackend(lambda: real_async_redis)
        backend._start(_StreamCapabilities(supported=True))
        writer = await backend.open(scope=scope, stream_id="r1")
        keys = _keys(scope, "r1")
        await real_async_redis.expire(keys.active, 5)
        async with writer.keepalive():
            await asyncio.sleep(0.5)
        ttl = get_settings().stream_active_ttl_seconds
        assert ttl - 2 <= await real_async_redis.ttl(keys.active) <= ttl

    async def test_cancel_flag_reaches_a_plain_producer(
        self, backend: StreamBackend, scope: str
    ) -> None:
        writer = await backend.open(scope=scope, stream_id="r1")
        await writer.append("delta", {})
        assert writer.cancelled is False
        assert await backend.cancel("r1", scope=scope) is True
        await writer.append("delta", {})
        assert writer.cancelled is True

    async def test_cancel_never_leaves_meta_without_a_ttl(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        await backend.open(scope=scope, stream_id="r1")
        await backend.cancel("r1", scope=scope)
        assert await real_async_redis.ttl(_keys(scope, "r1").meta) > 0

    async def test_keys_share_a_slot(self, backend: StreamBackend, scope: str) -> None:
        keys = _keys(scope, "r1")
        run_keys = (keys.stream, keys.meta, keys.active)
        assert len({key_slot(k.encode()) for k in run_keys}) == 1

    async def test_max_readers_answers_503(
        self,
        settings_env: Callable[..., None],
        real_async_redis: Any,
        scope: str,
    ) -> None:
        # TestClient runs a streaming response to its end before returning,
        # so two readers cannot overlap through it; drive ASGI directly.
        settings_env(
            stream_max_readers=1,
            stream_heartbeat_seconds=0.2,
            stream_alive_ttl_seconds=60,
        )
        backend = StreamBackend(lambda: real_async_redis)
        backend._start(_StreamCapabilities(supported=True))
        await backend.open(scope=scope, stream_id="r1")
        request = _get()
        first = await backend.sse("r1", request=request, scope=scope)
        second = await backend.sse("r1", request=request, scope=scope)
        asgi = _ASGI
        sent: list[Any] = []
        receive = _wait_forever

        async def discard(message: Any) -> None:
            return None

        async def keep(message: Any) -> None:
            sent.append(message)

        running = asyncio.create_task(first(asgi, receive, discard))
        await asyncio.sleep(0.1)
        assert backend._readers == 1
        await second(asgi, receive, keep)
        assert sent[0]["status"] == 503
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)
        assert backend._readers == 0

    async def test_a_cancel_flag_from_another_process_stops_a_spawned_producer(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        started = asyncio.Event()

        async def forever(writer: StreamWriter) -> None:
            while True:
                await writer.append("delta", {})
                started.set()
                await asyncio.sleep(0.02)

        writer = await backend.open(scope=scope, stream_id="r1")
        task = backend.spawn(writer, forever)
        await started.wait()
        # Another process sets the flag; this process's task is not touched.
        await real_async_redis.hset(_keys(scope, "r1").meta, "cancel", "1")
        await asyncio.wait_for(task, 5)
        events = await _collect(backend, "r1", scope)
        assert events[-1].type == "stream.end"
        assert events[-1].data == {"reason": "cancelled"}

    async def test_the_heartbeat_finds_a_fence_and_writes_no_end(
        self,
        settings_env: Callable[..., None],
        real_async_redis: Any,
        scope: str,
    ) -> None:
        settings_env(stream_heartbeat_seconds=0.1, stream_alive_ttl_seconds=5)
        backend = StreamBackend(lambda: real_async_redis)
        backend._start(_StreamCapabilities(supported=True))

        async def quiet(writer: StreamWriter) -> None:
            await writer.append("delta", {})
            await asyncio.sleep(60)  # a long tool call: no appends

        writer = await backend.open(scope=scope, stream_id="r1")
        task = backend.spawn(writer, quiet)
        await asyncio.sleep(0.05)
        await backend.attach("r1", scope=scope)
        await asyncio.wait_for(task, 5)
        entries = await real_async_redis.xrange(_keys(scope, "r1").stream)
        assert all(fields.get("f") != "1" for _, fields in entries)

    async def test_a_failed_read_is_retried(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        from redis.exceptions import ConnectionError as RedisConnectionError

        writer = await backend.open(scope=scope, stream_id="r1")
        await writer.append("delta", {"n": 0})
        plan = await backend._prepare("r1", scope=scope, after=None)
        assert plan is not None
        original = real_async_redis.xread
        failures = {"left": 1}

        async def flaky(*args: Any, **kwargs: Any) -> Any:
            if failures["left"]:
                failures["left"] -= 1
                raise RedisConnectionError("blip")
            return await original(*args, **kwargs)

        real_async_redis.xread = flaky  # type: ignore[method-assign]
        try:
            await writer.finish("message", {})
            seen = await _flatten(backend._pages(plan))
        finally:
            real_async_redis.xread = original  # type: ignore[method-assign]
        assert None in seen  # a keepalive while retrying
        assert [e.type for e in seen if e is not None] == ["delta", "message"]

    async def test_follow_closes_when_left_early(
        self, backend: StreamBackend, scope: str
    ) -> None:
        writer = await backend.open(scope=scope, stream_id="r1")
        for i in range(3):
            await writer.append("delta", {"i": i})
        reader = await backend.follow("r1", scope=scope)
        async with reader:
            async for event in reader:
                assert event.data == {"i": 0}
                pages = reader._pages
                break
        assert pages is not None and pages.ag_frame is None  # type: ignore[attr-defined]
        assert [e async for e in reader] == []  # closed for good

    async def test_follow_checks_before_the_first_event(
        self, backend: StreamBackend, scope: str
    ) -> None:
        # The checks run at the await, so a WebSocket can refuse before accept.
        with pytest.raises(StreamNotFound):
            await backend.follow("nope", scope=scope)
        writer = await backend.open(scope=scope, stream_id="r1")
        final = await writer.finish("message", {})
        assert [
            e async for e in await backend.follow("r1", scope=scope, after=final)
        ] == []

    async def test_the_end_marker_alone_stops_a_reconnect(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        writer = await backend.open(scope=scope, stream_id="r1")
        await writer.append("delta", {})
        final = await writer.finish("message", {})
        # Meta holds no final ID: the entry's end marker decides.
        assert not await real_async_redis.hexists(_keys(scope, "r1").meta, "final")
        assert await backend._prepare("r1", scope=scope, after=final) is None
        response = await backend.sse("r1", request=_get(), scope=scope, after=final)
        assert response.status_code == 204

    async def test_a_cursor_past_the_final_entry_gets_204(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        stale = await backend.open(scope=scope, stream_id="r1")
        await stale.append("delta", {})
        fresh = await backend.attach("r1", scope=scope)
        await fresh.finish("message", {})
        with pytest.raises(StreamFenced):
            await stale.append("delta", {})  # lands after the end, then fenced
        last = await real_async_redis.xrevrange(_keys(scope, "r1").stream, count=1)
        past = last[0][0]
        assert last[0][1].get("f") is None  # the stale entry, not the end
        assert await backend._prepare("r1", scope=scope, after=past) is None
        response = await backend.sse("r1", request=_get(), scope=scope, after=past)
        assert response.status_code == 204

    async def test_a_full_first_page_does_not_end_the_read(
        self,
        settings_env: Callable[..., None],
        real_async_redis: Any,
        scope: str,
    ) -> None:
        settings_env(stream_replay_page=2)
        backend = StreamBackend(lambda: real_async_redis)
        backend._start(_StreamCapabilities(supported=True))
        writer = await backend.open(scope=scope, stream_id="r1")
        first = await writer.append("delta", {"i": 0})
        for i in range(1, 4):
            await writer.append("delta", {"i": i})
        await writer.finish("message", {})
        # Two pages of entries after the cursor, the final one in the second.
        events = await _collect(backend, "r1", scope, after=first)
        assert [e.data for e in events] == [{"i": 1}, {"i": 2}, {"i": 3}, {}]
        assert events[-1].final

    async def test_meta_holding_only_the_heartbeat_counts_as_none(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        keys = _keys(scope, "r1")
        # What a late heartbeat leaves after meta expired: only the field.
        await real_async_redis.execute_command(
            "HSETEX", keys.meta, "PX", 5000, "FIELDS", 1, "alive", "1"
        )
        with pytest.raises(StreamNotFound):
            await backend.follow("r1", scope=scope)
        with pytest.raises(StreamNotFound):
            await backend.attach("r1", scope=scope)
        assert (await backend.open(scope=scope, stream_id="r1")).created is True

    async def test_a_lost_producer_never_recreates_an_expired_stream(
        self,
        settings_env: Callable[..., None],
        real_async_redis: Any,
        scope: str,
    ) -> None:
        settings_env(stream_heartbeat_seconds=0.2, stream_alive_ttl_seconds=0.3)
        backend = StreamBackend(lambda: real_async_redis)
        backend._start(_StreamCapabilities(supported=True))
        writer = await backend.open(scope=scope, stream_id="r1")
        await writer.append("delta", {})
        plan = await backend._prepare("r1", scope=scope, after=None)
        assert plan is not None
        keys = _keys(scope, "r1")
        # Expired mid-read: the stream is gone, and the heartbeat with it.
        await real_async_redis.delete(keys.stream)
        await real_async_redis.hdel(keys.meta, "alive")
        seen = await asyncio.wait_for(_flatten(backend._pages(plan)), 5)
        assert [e.type for e in seen if e is not None] == ["delta"]
        assert not await real_async_redis.exists(keys.stream)
        with pytest.raises(StreamExpired):
            await backend.follow("r1", scope=scope)

    async def test_a_lost_producer_that_never_appended_gets_an_end(
        self,
        settings_env: Callable[..., None],
        real_async_redis: Any,
        scope: str,
    ) -> None:
        settings_env(stream_heartbeat_seconds=0.2, stream_alive_ttl_seconds=0.3)
        backend = StreamBackend(lambda: real_async_redis)
        backend._start(_StreamCapabilities(supported=True))
        await backend.open(scope=scope, stream_id="r1")  # dies before appending
        events = await asyncio.wait_for(_collect(backend, "r1", scope), 5)
        assert [(e.type, e.data) for e in events] == [
            ("stream.end", {"reason": "producer_lost"})
        ]

    async def test_a_cancel_never_interrupts_the_final_write(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        writer = await backend.open(scope=scope, stream_id="r1")
        entered, gate = asyncio.Event(), asyncio.Event()
        end = writer._end

        async def slow_end(type: str, data: Any) -> str:
            entered.set()
            await gate.wait()  # a cancel here would drop the final entry
            return await end(type, data)

        writer._end = slow_end  # type: ignore[method-assign]
        task = backend.spawn(writer, _deltas(1, final=False))
        await asyncio.wait_for(entered.wait(), 5)
        assert await backend.cancel("r1", scope=scope) is True
        gate.set()
        await asyncio.wait_for(task, 5)
        events = await _collect(backend, "r1", scope)
        assert events[-1].data == {"reason": "done"}

    async def test_shutdown_never_interrupts_a_done_final_write(
        self, backend: StreamBackend, scope: str
    ) -> None:
        writer = await backend.open(scope=scope, stream_id="r1")
        entered, gate = asyncio.Event(), asyncio.Event()
        end = writer._end

        async def slow_end(type: str, data: Any) -> str:
            entered.set()
            await gate.wait()
            return await end(type, data)

        writer._end = slow_end  # type: ignore[method-assign]
        backend.spawn(writer, _deltas(1, final=False))
        await asyncio.wait_for(entered.wait(), 5)
        closing = asyncio.create_task(backend.aclose())
        await asyncio.sleep(0.6)  # past at least one repeated cancel
        gate.set()
        await asyncio.wait_for(closing, 5)
        backend._closing = False
        events = await _collect(backend, "r1", scope)
        assert events[-1].data == {"reason": "done"}

    async def test_a_replay_page_is_one_send(
        self, backend: StreamBackend, scope: str
    ) -> None:
        writer = await backend.open(scope=scope, stream_id="r1")
        for i in range(5):
            await writer.append("delta", {"i": i})
        await writer.finish("message", {})
        response = await backend.sse("r1", request=_get(), scope=scope)
        bodies: list[bytes] = []

        async def keep(message: Any) -> None:
            if message["type"] == "http.response.body" and message.get("body"):
                bodies.append(message["body"])

        await response(_ASGI, _wait_forever, keep)
        assert bodies[0].startswith(b"retry: ")
        assert 1000 <= int(bodies[0].split()[1]) <= 3000
        assert bodies[1].count(b"event: ") == 6  # every frame in one chunk

    async def test_start_spawns_once(self, backend: StreamBackend, scope: str) -> None:
        first = await backend.start(_deltas(2), scope=scope, stream_id="r1")
        second = await backend.start(_deltas(2), scope=scope, stream_id="r1")
        assert first.created is True and second.created is False
        events = await _collect(backend, "r1", scope)
        assert [e.type for e in events] == ["delta", "delta", "message"]
