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
import inspect
import json
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
import redis as sync_redis
import redis.asyncio as async_redis
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from redis.exceptions import TimeoutError as RedisTimeoutError

from redis_fastapi import (
    FastAPIRedis,
    StreamBackend,
    StreamBackendDep,
    StreamExpired,
    StreamFinished,
    StreamNotFound,
    StreamWriter,
)
from redis_fastapi.config import get_settings
from redis_fastapi.stream_backend import _key, _StreamCapabilities
from tests.conftest import REDIS_URL, about, aexpire_now, requires_redis
from tests.stream_support import collect, stream_backend


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
) -> AsyncIterator[StreamBackend]:
    """A backend on a real async client, outside any app."""
    instance = StreamBackend(lambda: real_async_redis)
    instance._start(_StreamCapabilities())
    assert await instance.detect() is True
    yield instance
    await instance.aclose()


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

    @app.post("/{scope}/runs/{run_id}/cancel", status_code=204)
    async def cancel(scope: str, run_id: str, streams: StreamBackendDep) -> None:
        await streams.cancel(run_id, scope=scope)

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
            assert client.post(f"/{scope}/runs/r1/cancel").status_code == 503
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
                client.get("/bad scope/runs/r1/events"),
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
        far = f"{int(time.time() * 1000) + 10**9}-0"
        with TestClient(_app()) as client:
            client.post(f"/{scope}/runs/r1")  # no producer: no events yet
            assert client.get(f"/{scope}/runs/r1/events?after={far}").status_code == 409

        other = f"{scope}-b"  # r1 still holds the first scope's slot
        with TestClient(_app(_deltas(1))) as client:
            client.post(f"/{other}/runs/r2")
            _read(client, f"/{other}/runs/r2/events")
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
            assert client.post(f"/{scope}/runs/r1/cancel").status_code == 204
            _, frames = _read(client, f"/{scope}/runs/r1/events")
            assert frames[-1]["event"] == "stream.end"
            assert frames[-1]["data"] == {"reason": "cancelled"}
            # A second cancel changes nothing.
            assert client.post(f"/{scope}/runs/r1/cancel").status_code == 204
            _, again = _read(client, f"/{scope}/runs/r1/events")
            assert [f["data"] for f in again if f.get("data", {}).get("reason")] == [
                {"reason": "cancelled"}
            ]

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

        settings_env(stream_max_connection_duration=1)
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
        settings_env(stream_max_connection_duration=1)
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
        keys = _key(scope, "r1")
        last = real_redis.xrevrange(keys, count=1)
        assert last[0][1]["t"] == "stream.end"
        assert json.loads(last[0][1]["d"]) == {"reason": "cancelled"}

    def test_keys_follow_the_prefix_setting(
        self, settings_env: Callable[..., None], real_redis: Any, scope: str
    ) -> None:
        settings_env(prefix="myapp")
        with TestClient(_app(_deltas(1))) as client:
            client.post(f"/{scope}/runs/r1")
            _read(client, f"/{scope}/runs/r1/events")
        assert real_redis.exists(f"myapp:stream:{scope}:r1")
        assert not real_redis.exists(f"redis:fastapi:stream:{scope}:r1")
        # One key per run: nothing else under the prefix.
        assert list(real_redis.scan_iter("myapp:*")) == [f"myapp:stream:{scope}:r1"]


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
        events = await collect(backend, "r1", scope)
        assert [e.type for e in events] == ["delta"] * 3 + ["message"]
        assert events[-1].final and events[-1].id == final
        assert [e.data for e in events[:3]] == [{"i": 0}, {"i": 1}, {"i": 2}]

    async def test_a_start_retried_after_the_run_ended_is_not_created(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        writer = await backend.open(scope=scope, stream_id="r1")
        await writer.append("delta", {})
        await writer.finish("message", {})
        keys = _key(scope, "r1")
        ttl = await real_async_redis.ttl(keys)
        retry = await backend.open(scope=scope, stream_id="r1")
        assert retry.created is False
        assert await real_async_redis.ttl(keys) <= ttl  # not extended again
        with pytest.raises(ValueError, match="retried start"):
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
        with pytest.raises(ValueError, match="retried start"):
            await retry.append("delta", {})
        await writer.append("delta", {})  # the run's own writer is unaffected
        entries = await real_async_redis.xrange(_key(scope, "r1"))
        shown = [f["t"] for _, f in entries if f["t"] != "stream.start"]
        assert shown == ["delta", "delta"]

    async def test_a_start_whose_reply_was_lost_is_not_created_twice(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        # EXEC applied but its reply was lost: the retry finds the stream it
        # made, so it reports a retried start; its second start entry is one
        # readers skip.
        original = real_async_redis.pipeline
        calls = {"n": 0}

        def pipeline(*args: Any, **kwargs: Any) -> Any:
            pipe = original(*args, **kwargs)
            calls["n"] += 1
            if calls["n"] == 1:
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
        assert calls["n"] == 2
        assert writer.created is False
        entries = await real_async_redis.xrange(_key(scope, "r1"))
        assert {f["t"] for _, f in entries} == {"stream.start"}

    async def test_concurrent_starts_of_one_run_create_it_once(
        self, backend: StreamBackend, scope: str
    ) -> None:
        writers = await asyncio.gather(
            *(backend.open(scope=scope, stream_id="r1") for _ in range(8))
        )
        assert sum(w.created for w in writers) == 1

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
        last = await real_async_redis.xrevrange(_key(scope, "r1"), count=1)
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
        last = await real_async_redis.xrevrange(_key(scope, "r1"), count=1)
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

    async def test_a_producer_whose_end_failed_is_reported_lost(
        self, settings_env: Callable[..., None], real_async_redis: Any, scope: str
    ) -> None:
        from redis_fastapi import StreamStoreError

        settings_env(stream_heartbeat_interval=1, stream_silence_timeout=3)
        backend = stream_backend(real_async_redis)
        try:
            writer = await backend.open(scope=scope, stream_id="r1")

            async def unreachable(type: str, data: Any) -> str:
                raise StreamStoreError("Redis did not answer")

            writer._end = unreachable  # type: ignore[method-assign]
            await asyncio.wait_for(backend.spawn(writer, _deltas(1, final=False)), 5)
            events = await asyncio.wait_for(collect(backend, "r1", scope), 5)
        finally:
            await backend.aclose()
        assert events[-1].data == {"reason": "producer_lost"}

    async def test_finish_after_a_reader_ended_the_run_adds_nothing(
        self, settings_env: Callable[..., None], real_async_redis: Any, scope: str
    ) -> None:
        settings_env(stream_heartbeat_interval=1, stream_silence_timeout=3)
        stalled = stream_backend(real_async_redis)
        writer = await stalled.open(scope=scope, stream_id="r1")
        reader = stream_backend(real_async_redis)
        events = await asyncio.wait_for(collect(reader, "r1", scope), 10)
        assert events[-1].data == {"reason": "producer_lost"}
        # The stalled producer comes back: it gets the reader's end.
        assert await writer.finish("message", {"late": True}) == events[-1].id
        entries = await real_async_redis.xrange(_key(scope, "r1"))
        assert [f["t"] for _, f in entries if f.get("f") == "1"] == ["stream.end"]

    async def test_a_retried_append_after_a_timeout_stores_one_entry(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        writer = await backend.open(scope=scope, stream_id="r1")
        original = real_async_redis.execute_command
        calls = {"n": 0}

        async def flaky(*args: Any, **kwargs: Any) -> Any:
            result = await original(*args, **kwargs)
            calls["n"] += 1
            if calls["n"] == 1:
                raise RedisTimeoutError("reply lost")  # applied, but unseen
            return result

        real_async_redis.execute_command = flaky  # type: ignore[method-assign]
        try:
            entry_id = await writer.append("delta", {"once": True})
        finally:
            real_async_redis.execute_command = original  # type: ignore[method-assign]
        assert calls["n"] == 2
        entries = await real_async_redis.xrange(_key(scope, "r1"))
        events = [stored for stored, f in entries if f["t"] != "stream.start"]
        assert events == [entry_id]

    async def test_an_append_after_a_failed_one_is_not_dropped(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        # The first append lands but every reply is lost, so it fails.  The
        # next append must not reuse its sequence number: Redis would return
        # the first event's ID and drop the second.
        from redis_fastapi import StreamStoreError

        writer = await backend.open(scope=scope, stream_id="r1")
        original = real_async_redis.execute_command

        async def unanswered(*args: Any, **kwargs: Any) -> Any:
            await original(*args, **kwargs)
            raise RedisTimeoutError("reply lost")

        real_async_redis.execute_command = unanswered  # type: ignore[method-assign]
        try:
            with pytest.raises(StreamStoreError):
                await writer.append("delta", {"n": 1})
        finally:
            real_async_redis.execute_command = original  # type: ignore[method-assign]
        await writer.append("delta", {"n": 2})
        entries = await real_async_redis.xrange(_key(scope, "r1"))
        events = [json.loads(f["d"]) for _, f in entries if f["t"] == "delta"]
        assert events == [{"n": 1}, {"n": 2}]

    async def test_a_redis_outage_is_not_the_producers_silence(
        self, settings_env: Callable[..., None], real_async_redis: Any, scope: str
    ) -> None:
        from redis.exceptions import ConnectionError as RedisConnectionError

        settings_env(stream_heartbeat_interval=1, stream_silence_timeout=3)
        backend = stream_backend(real_async_redis)
        writer = await backend.open(scope=scope, stream_id="r1")
        original = real_async_redis.xread
        outage_ends = time.monotonic() + 4  # longer than the silence timeout

        async def flaky(*args: Any, **kwargs: Any) -> Any:
            if time.monotonic() < outage_ends:
                raise RedisConnectionError("unreachable")
            return await original(*args, **kwargs)

        async def produce_after_the_outage() -> None:
            # Long after the last entry, but within a silence timeout of
            # reachable Redis: only the reset after the outage keeps it alive.
            await asyncio.sleep(max(0.0, outage_ends - time.monotonic()) + 2)
            await writer.append("delta", {})
            await writer.finish("message", {})

        real_async_redis.xread = flaky  # type: ignore[method-assign]
        try:
            producing = asyncio.create_task(produce_after_the_outage())
            events = await asyncio.wait_for(collect(backend, "r1", scope), 15)
            await producing
        finally:
            real_async_redis.xread = original  # type: ignore[method-assign]
            await backend.aclose()
        assert [e.type for e in events] == ["delta", "message"]

    async def test_a_late_append_after_expiry_does_not_recreate_the_stream(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        writer = await backend.open(scope=scope, stream_id="r1")
        await writer.append("delta", {})
        keys = _key(scope, "r1")
        await real_async_redis.delete(keys)
        with pytest.raises(StreamExpired):
            await writer.append("delta", {})
        assert not await real_async_redis.exists(keys)

    async def test_an_expired_run_is_404(
        self,
        settings_env: Callable[..., None],
        real_async_redis: Any,
        scope: str,
    ) -> None:
        settings_env()
        backend = StreamBackend(lambda: real_async_redis)
        backend._start(_StreamCapabilities(supported=True))
        writer = await backend.open(scope=scope, stream_id="r1")
        await writer.append("delta", {})
        await writer.finish("message", {})
        keys = _key(scope, "r1")
        assert about(await real_async_redis.ttl(keys), 120)
        await aexpire_now(real_async_redis, keys)  # stream_done_ttl ran out
        with pytest.raises(StreamNotFound):
            await collect(backend, "r1", scope)

    async def test_a_run_with_no_events_yet_reads_as_empty(
        self, backend: StreamBackend, scope: str
    ) -> None:
        await backend.open(scope=scope, stream_id="r1")
        plan = await backend._prepare("r1", scope=scope, after=None)
        assert plan is not None
        assert [f["t"] for _, f in plan.first] == ["stream.start"]  # never shown

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
            await collect(backend, "r1", scope, after=first)
        with pytest.raises(StreamExpired):
            await collect(backend, "r1", scope)  # no cursor means 0-0

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
        keys = _key(scope, "r1")
        entries = await real_async_redis.xrange(keys)
        # entries[0] is stream.start, so these are the third and fourth deltas.
        await real_async_redis.xdel(keys, entries[3][0], entries[4][0])
        await writer.finish("message", {})
        seen = [backend._event(e) for e in await _flatten(backend._pages(plan)) if e]
        assert [(e.type, e.data) for e in seen[:3]] == [
            ("delta", {"n": 0}),
            ("delta", {"n": 1}),
            ("stream.gap", {"missing": 2}),
        ]
        assert seen[-1].final

    async def test_open_gives_the_run_its_lifetime(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        await backend.open(scope=scope, stream_id="r1")
        lifetime = get_settings().stream_max_run
        assert about(await real_async_redis.ttl(_key(scope, "r1")), lifetime)

    async def test_an_append_leaves_the_ttls_alone(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        writer = await backend.open(scope=scope, stream_id="r1")
        keys = _key(scope, "r1")
        await real_async_redis.expire(keys, 2000)
        await writer.append("delta", {})
        assert about(await real_async_redis.ttl(keys), 2000)

    async def test_extend_never_shortens(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        writer = await backend.open(scope=scope, stream_id="r1")
        await writer.extend(timedelta(hours=2))
        keys = _key(scope, "r1")
        assert 7190 <= await real_async_redis.ttl(keys) <= 7200
        await writer.extend(60)
        assert 7190 <= await real_async_redis.ttl(keys) <= 7200

    async def test_max_readers_answers_503(
        self,
        settings_env: Callable[..., None],
        real_async_redis: Any,
        scope: str,
    ) -> None:
        # TestClient runs a streaming response to its end before returning,
        # so two readers cannot overlap through it; drive ASGI directly.
        settings_env(stream_max_readers=1)
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
        assert await backend._prepare("r1", scope=scope, after=final) is None
        response = await backend.sse("r1", request=_get(), scope=scope, after=final)
        assert response.status_code == 204

    async def test_a_cursor_past_the_final_entry_reads_nothing(
        self, settings_env: Callable[..., None], real_async_redis: Any, scope: str
    ) -> None:
        # Only a hand-made cursor gets here: readers stop at the final entry,
        # so no client is ever sent an ID after it.
        settings_env(stream_heartbeat_interval=1, stream_silence_timeout=3)
        backend = stream_backend(real_async_redis)
        writer = await backend.open(scope=scope, stream_id="r1")
        await writer.append("delta", {})
        await writer.finish("message", {})
        keys = _key(scope, "r1")
        await real_async_redis.xadd(keys, {"t": "delta", "d": "{}", "q": "9"})
        last = await real_async_redis.xrevrange(keys, count=1)
        past = last[0][0]
        assert last[0][1].get("f") is None  # the late entry, not the end
        events = await asyncio.wait_for(collect(backend, "r1", scope, after=past), 10)
        assert events == []

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
        events = await collect(backend, "r1", scope, after=first)
        assert [e.data for e in events] == [{"i": 1}, {"i": 2}, {"i": 3}, {}]
        assert events[-1].final

    async def test_a_cancel_during_the_final_write_leaves_one_end(
        self, backend: StreamBackend, scope: str, real_async_redis: Any
    ) -> None:
        writer = await backend.open(scope=scope, stream_id="r1")
        entered, gate = asyncio.Event(), asyncio.Event()
        end = writer._end

        async def slow_end(type: str, data: Any) -> str:
            entered.set()
            await gate.wait()  # a task cancel here would drop the final write
            return await end(type, data)

        writer._end = slow_end  # type: ignore[method-assign]
        task = backend.spawn(writer, _deltas(1, final=False))
        await asyncio.wait_for(entered.wait(), 5)
        # cancel() writes its own end first; the producer's end, which the
        # cancel must not interrupt, then finds it and adds nothing.
        await backend.cancel("r1", scope=scope)
        gate.set()
        await asyncio.wait_for(task, 5)
        assert not task.cancelled()
        events = await collect(backend, "r1", scope)
        assert events[-1].data == {"reason": "cancelled"}
        entries = await real_async_redis.xrange(_key(scope, "r1"))
        assert sum(f.get("f") == "1" for _, f in entries) == 1

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
        events = await collect(backend, "r1", scope)
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
        events = await collect(backend, "r1", scope)
        assert [e.type for e in events] == ["delta", "delta", "message"]


@requires_redis
@requires_streams
@pytest.mark.integration
@pytest.mark.parametrize(
    "client_options",
    [
        pytest.param({}, id="default"),
        pytest.param({"protocol": 2}, id="resp2"),
        pytest.param({"protocol": 3}, id="resp3-legacy"),
        pytest.param({"protocol": 3, "legacy_responses": False}, id="resp3-modern"),
    ],
)
@pytest.mark.parametrize("decode", [False, True], ids=["bytes", "text"])
async def test_client_response_modes_replay_and_live(
    settings_env: Callable[..., None],
    scope: str,
    client_options: dict[str, Any],
    decode: bool,
) -> None:
    import os

    if (
        "legacy_responses" in client_options
        and "legacy_responses" not in inspect.signature(async_redis.Redis).parameters
    ):
        pytest.skip("This redis-py version has no modern-response option")
    settings_env()
    factory = (
        async_redis.RedisCluster
        if os.getenv("STREAMS_TEST_CLUSTER") == "1"
        else async_redis.Redis
    )
    async with factory.from_url(
        REDIS_URL, decode_responses=decode, **client_options
    ) as client:
        instance = StreamBackend(lambda: client)
        instance._start(_StreamCapabilities())
        try:
            writer = await instance.open(scope=scope, stream_id="compatibility")
            first = await writer.append("delta", {"n": 1})
            plan = await instance._prepare(writer.stream_id, scope=scope, after=None)
            assert plan is not None
            async with await instance.follow(writer.stream_id, scope=scope) as events:
                replay = await asyncio.wait_for(anext(events), 5)
                assert replay is not None and replay.id == first
                waiting = asyncio.create_task(anext(events))
                live = await writer.append("delta", {"n": 2})
                event = await asyncio.wait_for(waiting, 5)
                assert event is not None and event.id == live
                assert event.data == {"n": 2}
                final = await writer.finish("done", {"n": 3})
                event = await asyncio.wait_for(anext(events), 5)
                assert event is not None and event.id == final and event.final
            assert await collect(instance, writer.stream_id, scope, after=final) == []
        finally:
            await instance.aclose()
            keys = _key(scope, "compatibility")
            await client.delete(keys)


@requires_redis
@requires_streams
@pytest.mark.integration
async def test_a_key_that_is_not_a_stream_is_a_store_error(
    backend: StreamBackend, scope: str, real_async_redis: Any
) -> None:
    from redis_fastapi import StreamStoreError

    await real_async_redis.set(_key(scope, "r1"), "not-a-stream")
    with pytest.raises(StreamStoreError):
        await collect(backend, "r1", scope)
    with pytest.raises(StreamStoreError):
        await backend.open(scope=scope, stream_id="r1")
