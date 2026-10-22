"""Explicit integration must not inherit host configuration or own its clients."""

from contextlib import asynccontextmanager

import httpx
import pytest
from fastapi import FastAPI, Request

from redis_fastapi import (
    StreamBackend,
    StreamBackendDep,
    StreamError,
    StreamSettings,
    add_redis_streams,
    get_stream_backend,
    sse_response,
)

from .test_streams import publish


async def test_explicit_apps_ignore_environment_and_preserve_clients(
    deployment, monkeypatch, tmp_path
):
    backend, blocking, *_ = deployment
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(
        "REDIS_STREAM_BUCKET_COUNT=invalid\nREDIS_OTEL_ENABLED=true\n"
    )
    monkeypatch.setenv("REDIS_URL", "redis://unreachable.invalid:1")
    monkeypatch.setenv("REDIS_STREAM_BUCKET_COUNT", "invalid")
    monkeypatch.setenv("REDIS_OTEL_REDIS_ENABLED", "true")

    # Settings construction and registration must not consult cached settings either.
    def forbidden():
        pytest.fail("Explicit integration read global settings")

    monkeypatch.setattr("redis_fastapi.streams.get_settings", forbidden)
    settings = StreamSettings(**backend.settings.model_dump())
    other = StreamBackend(
        backend._store.client, settings=settings, blocking_client=blocking
    )
    observed = []

    @asynccontextmanager
    async def existing(app):
        observed.append("start")
        assert (
            await get_stream_backend(Request({"type": "http", "app": app})) is backend
        )
        yield {"application_state": True}
        observed.append("stop")

    first = FastAPI(lifespan=existing)
    second = FastAPI()
    add_redis_streams(first, backend=backend)
    add_redis_streams(first, backend=backend)
    add_redis_streams(second, backend=other)
    with pytest.raises(ValueError):
        add_redis_streams(first, backend=other)
    with pytest.raises(ValueError):
        add_redis_streams(first, backend=backend, manage_lifespan=False)

    @first.get("/events")
    async def events(request: Request, streams: StreamBackendDep):
        return await sse_response(request, backend=streams, stream="explicit")

    async with first.router.lifespan_context(first) as state:
        assert state == {"application_state": True}
        async with second.router.lifespan_context(second):
            assert (
                await get_stream_backend(Request({"type": "http", "app": second}))
                is other
            )
            await backend.create("explicit", ttl=60)
            first_id = await publish(backend, "explicit", 1)
            final_id = await publish(backend, "explicit", 2, final=True)
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=first), base_url="http://test"
            ) as client:
                response = await client.get(
                    "/events", headers={"Last-Event-ID": first_id}
                )
                assert response.status_code == 200
                assert f"id: {final_id}" in response.text
                assert f"id: {first_id}" not in response.text
                assert (
                    await client.get("/events", headers={"Last-Event-ID": final_id})
                ).status_code == 204
                missing = await client.get("/events?after=999999999999999999-0")
                assert missing.status_code == 409
                assert missing.json()["detail"]["code"] == "cursor_ahead"
    assert observed == ["start", "stop"]
    for app in (first, second):
        assert not hasattr(app.state, "_redis_stream_backend")
        assert not hasattr(app.state, "_redis")
    assert await backend._store.client.ping()
    assert await blocking.ping()


async def test_caller_manages_backend_lifecycle(deployment):
    backend, blocking, *_ = deployment
    app = FastAPI()
    add_redis_streams(app, backend=backend, manage_lifespan=False)
    async with app.router.lifespan_context(app):
        assert (
            await get_stream_backend(Request({"type": "http", "app": app})) is backend
        )
        with pytest.raises(StreamError):
            async with backend.subscribe("not-started"):
                pass
        async with backend:
            await backend.create("caller", ttl=60)
            await publish(backend, "caller", 1, final=True)
            async with backend.subscribe("caller") as events:
                assert len([e async for e in events]) == 1
    assert await blocking.ping()


async def test_startup_failure_releases_runtime_and_allows_retry(
    deployment, monkeypatch
):
    from redis_fastapi._stream_relay import _StreamRelay

    backend, blocking, *_ = deployment
    app = FastAPI()
    add_redis_streams(app, backend=backend)
    start = _StreamRelay.start

    async def failing_start(self):
        await start(self)
        raise RuntimeError("Startup interrupted")

    with monkeypatch.context() as patch:
        patch.setattr(_StreamRelay, "start", failing_start)
        with pytest.raises(RuntimeError, match="Startup interrupted"):
            async with app.router.lifespan_context(app):
                pytest.fail("Startup should fail")
    assert not hasattr(app.state, "_redis_stream_backend")
    assert await blocking.ping()
    async with app.router.lifespan_context(app):
        await backend.create("retry-start", ttl=60)
        await publish(backend, "retry-start", 1, final=True)
        async with backend.subscribe("retry-start") as events:
            assert len([e async for e in events]) == 1


async def test_cancellation_stops_relays_without_closing_clients(deployment):
    import asyncio

    import anyio

    backend, blocking, *_ = deployment
    entered = asyncio.Event()

    async def run():
        with anyio.CancelScope() as scope:
            async with backend:
                await backend.create("cancel-runtime", ttl=60)
                await publish(backend, "cancel-runtime", 1)
                async with backend.subscribe("cancel-runtime") as events:
                    assert (await anext(events)).data == {"n": 1}
                    entered.set()
                    scope.cancel()
                    await anyio.sleep_forever()

    task = asyncio.create_task(run())
    await asyncio.wait_for(entered.wait(), 5)
    await asyncio.wait_for(task, 5)
    with pytest.raises(StreamError):
        async with backend.subscribe("cancel-runtime"):
            pass
    assert await blocking.ping()
    assert await backend._store.client.ping()
    # A fresh runtime must start and stop cleanly after the canceled one.
    async with backend:
        await publish(backend, "cancel-runtime", 2, final=True)
        async with backend.subscribe("cancel-runtime") as events:
            assert len([e async for e in events]) == 2


async def test_existing_lifespan_failure_cleans_injected_backend(deployment):
    backend, blocking, *_ = deployment

    @asynccontextmanager
    async def failing_lifespan(app):
        raise RuntimeError("Application startup failed")
        yield  # pragma: no cover

    app = FastAPI(lifespan=failing_lifespan)
    add_redis_streams(app, backend=backend)
    with pytest.raises(RuntimeError, match="Application startup failed"):
        async with app.router.lifespan_context(app):
            pytest.fail("Startup should fail")
    assert not hasattr(app.state, "_redis_stream_backend")
    assert await blocking.ping()
    async with backend:
        await backend.create("after-app-failure", ttl=60)
