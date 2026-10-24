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
import contextlib
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
        writer = await streams.open(scope=scope, stream_id=run_id)
        if writer.created and producer is not None:
            streams.spawn(writer, producer)
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
    async with contextlib.aclosing(  # type: ignore[type-var]
        backend.follow(stream_id, scope=scope, after=after)
    ) as stream:
        async for event in stream:
            events.append(event)
    return events


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
        assert real_redis.exists(f"myapp:stream:{{{scope}:r1}}")
        assert real_redis.exists(f"myapp:stream-meta:{{{scope}:r1}}")
        assert not real_redis.exists(f"redis:fastapi:stream:{{{scope}:r1}}")


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

    async def test_append_after_finish_raises(
        self, backend: StreamBackend, scope: str
    ) -> None:
        writer = await backend.open(scope=scope)
        first = await writer.finish("message", {})
        with pytest.raises(StreamFinished):
            await writer.append("delta", {})
        assert await writer.finish("message", {}) == first  # idempotent

    async def test_two_writers_ending_leave_one_final_entry(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        writer = await backend.open(scope=scope, stream_id="r1")
        await writer.append("delta", {})
        other = await backend.attach("r1", scope=scope)
        ids = await asyncio.gather(
            other.finish("message", {"by": "other"}),
            writer._end("stream.end", {"reason": "done"}),
        )
        assert ids[0] == ids[1]
        entries = await real_async_redis.xrange(_keys(scope, "r1").stream)
        assert sum(1 for _, fields in entries if fields.get("f") == "1") == 1

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
            result = await writer.append("delta", {"once": True})
        finally:
            real_async_redis.pipeline = original  # type: ignore[method-assign]
        assert calls["n"] == 2
        entries = await real_async_redis.xrange(_keys(scope, "r1").stream)
        assert [entry_id for entry_id, _ in entries] == [result.id]

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
        settings_env(stream_ttl_done=60, stream_expiry_window=600)
        backend = StreamBackend(lambda: real_async_redis)
        backend._start(_StreamCapabilities(supported=True))
        writer = await backend.open(scope=scope, stream_id="r1")
        await writer.append("delta", {})
        await writer.finish("message", {})
        keys = _keys(scope, "r1")
        assert 55 <= await real_async_redis.ttl(keys.stream) <= 60
        assert 655 <= await real_async_redis.ttl(keys.meta) <= 660
        assert not await real_async_redis.exists(keys.alive)

    async def test_expired_is_410_then_404(
        self,
        settings_env: Callable[..., None],
        real_async_redis: Any,
        scope: str,
    ) -> None:
        settings_env(stream_ttl_done=1, stream_expiry_window=2)
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
            await _collect(backend, "r1", scope, after=first.id)
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
        seen = []
        async for event in backend._events(plan):
            if event is not None:
                seen.append(event)
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

    async def test_suspend_extends_ttls_and_the_heartbeat(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        writer = await backend.open(scope=scope, stream_id="r1")
        await writer.append("delta", {})
        await writer.suspend(timedelta(hours=1))
        keys = _keys(scope, "r1")
        assert 3590 <= await real_async_redis.ttl(keys.stream) <= 3600
        assert await real_async_redis.ttl(keys.alive) > 3500
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
            await asyncio.sleep(1.0)  # twice the heartbeat key's lifetime
            assert await real_async_redis.exists(keys.alive)
        await asyncio.sleep(0.7)
        assert not await real_async_redis.exists(keys.alive)

    async def test_cancel_flag_reaches_a_plain_producer(
        self, backend: StreamBackend, scope: str
    ) -> None:
        writer = await backend.open(scope=scope, stream_id="r1")
        assert (await writer.append("delta", {})).cancelled is False
        assert await backend.cancel("r1", scope=scope) is True
        result = await writer.append("delta", {})
        assert result.cancelled is True and writer.cancelled

    async def test_cancel_never_leaves_meta_without_a_ttl(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        await backend.open(scope=scope, stream_id="r1")
        await backend.cancel("r1", scope=scope)
        assert await real_async_redis.ttl(_keys(scope, "r1").meta) > 0

    async def test_keys_share_a_slot(self, backend: StreamBackend, scope: str) -> None:
        keys = _keys(scope, "r1")
        assert (
            len({key_slot(k.encode()) for k in (keys.stream, keys.meta, keys.alive)})
            == 1
        )

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
        request = Request({"type": "http", "headers": [], "query_string": b""})
        first = await backend.sse("r1", request=request, scope=scope)
        second = await backend.sse("r1", request=request, scope=scope)
        asgi = {"type": "http", "asgi": {"spec_version": "2.4"}}
        sent: list[Any] = []

        async def receive() -> Any:
            await asyncio.sleep(3600)

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
            seen = []
            async for event in backend._events(plan):
                seen.append(event)
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
        stream = backend.follow("r1", scope=scope)
        async with contextlib.aclosing(stream):  # type: ignore[type-var]
            async for event in stream:
                assert event.data == {"i": 0}
                break
        assert stream.ag_frame is None  # type: ignore[attr-defined]
