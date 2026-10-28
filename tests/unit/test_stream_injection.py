"""Configuration and registration contracts for a backend on the app's own clients."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from redis.asyncio import Redis
from starlette.requests import Request

from redis_fastapi import (
    FastAPIRedis,
    StreamBackend,
    StreamConfigurationError,
    add_redis_streams,
    get_stream_backend,
)
from redis_fastapi.config import get_settings

# Every option, so a backend built with them reads no settings at all.
ALL_OPTIONS: dict[str, Any] = {
    "key_prefix": "myapp:sse",
    "active_ttl": 600,
    "done_ttl": 120,
    "gone_ttl": 3600,
    "maxlen": 10_000,
    "max_event_bytes": 65_536,
    "heartbeat_interval": 15,
    "alive_ttl": 45,
    "replay_page": 100,
    "max_readers": 12,
    "max_connection_duration": 300,
    "read_retry_window": 30,
    "send_timeout": 10,
}


@pytest.fixture()
def fresh_settings() -> Any:
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_every_option_given_reads_no_environment(
    monkeypatch: pytest.MonkeyPatch, fresh_settings: Any
) -> None:
    monkeypatch.setenv("REDIS_STREAM_MAX_READERS", "invalid")
    monkeypatch.setenv("REDIS_PREFIX", "{invalid}")
    backend = StreamBackend(Redis(socket_timeout=30), **ALL_OPTIONS)
    assert backend._policy.prefix == "myapp:sse"
    assert backend._policy.stream_max_readers == 12


def test_options_left_out_come_from_the_settings(
    monkeypatch: pytest.MonkeyPatch, fresh_settings: Any
) -> None:
    monkeypatch.setenv("REDIS_PREFIX", "ambient")
    monkeypatch.setenv("REDIS_STREAM_MAX_READERS", "7")
    backend = StreamBackend(Redis(socket_timeout=30), key_prefix="mine")
    assert backend._policy.prefix == "mine"
    assert backend._policy.stream_max_readers == 7


def test_settings_are_read_once(
    monkeypatch: pytest.MonkeyPatch, fresh_settings: Any
) -> None:
    backend = StreamBackend(Redis(socket_timeout=30))
    monkeypatch.setenv("REDIS_PREFIX", "later")
    get_settings.cache_clear()
    assert backend._policy.prefix == "redis:fastapi"


def test_durations_take_a_timedelta(fresh_settings: Any) -> None:
    backend = StreamBackend(
        Redis(socket_timeout=30),
        active_ttl=timedelta(minutes=2),
        heartbeat_interval=timedelta(seconds=5),
        alive_ttl=timedelta(seconds=20),
    )
    assert backend._policy.stream_active_ttl == 120
    assert backend._policy.stream_heartbeat_interval == 5
    assert backend._policy.stream_alive_ttl == 20


@pytest.mark.parametrize(
    ("values", "match"),
    [
        ({"max_readers": 0}, "max_readers"),
        ({"heartbeat_interval": 0}, "heartbeat_interval"),
        ({"gone_ttl": -1}, "gone_ttl"),
        ({"send_timeout": 1.5}, "send_timeout"),
        ({"max_connection_duration": True}, "max_connection_duration"),
        ({"key_prefix": "{x}"}, "prefix"),
        ({"heartbeat_interval": 10, "alive_ttl": 10}, "alive_ttl"),
    ],
)
def test_invalid_options_rejected(
    values: dict[str, Any], match: str, fresh_settings: Any
) -> None:
    with pytest.raises(StreamConfigurationError, match=match):
        StreamBackend(Redis(socket_timeout=30), **{**ALL_OPTIONS, **values})


def test_zero_is_allowed_where_it_means_off(fresh_settings: Any) -> None:
    backend = StreamBackend(Redis(socket_timeout=30), gone_ttl=0, read_retry_window=0)
    assert backend._policy.stream_gone_ttl == 0
    assert backend._policy.stream_read_retry_window == 0


def test_registration(fresh_settings: Any) -> None:
    app = FastAPI()
    backend = StreamBackend(Redis(socket_timeout=30))
    assert add_redis_streams(app, backend=backend) is backend
    assert add_redis_streams(app, backend=backend) is backend
    assert add_redis_streams(app) is backend
    with pytest.raises(StreamConfigurationError, match="already has"):
        add_redis_streams(app, backend=StreamBackend(Redis(socket_timeout=30)))


@pytest.mark.parametrize(
    "extra", [{"coder": object}, {"key_prefix": "x"}, {"max_readers": 2}]
)
def test_backend_with_options_rejected(
    extra: dict[str, Any], fresh_settings: Any
) -> None:
    backend = StreamBackend(Redis(socket_timeout=30))
    with pytest.raises(StreamConfigurationError, match="alongside"):
        add_redis_streams(FastAPI(), backend=backend, **extra)


@pytest.mark.parametrize(
    "extra", [{"coder": object}, {"key_prefix": "x"}, {"max_readers": 2}]
)
def test_repeated_backend_with_options_rejected(
    extra: dict[str, Any], fresh_settings: Any
) -> None:
    app = FastAPI()
    backend = StreamBackend(Redis(socket_timeout=30))
    add_redis_streams(app, backend=backend)
    with pytest.raises(StreamConfigurationError, match="alongside"):
        add_redis_streams(app, backend=backend, **extra)
    assert app.state._redis_streams is backend


def test_builder_passes_options(fresh_settings: Any) -> None:
    app = FastAPI()
    FastAPIRedis(app).streams(key_prefix="built", max_readers=3)
    backend = app.state._redis_streams
    assert backend._policy.prefix == "built"
    assert backend._policy.stream_max_readers == 3


async def test_dependency_is_app_scoped(fresh_settings: Any) -> None:
    apps = [FastAPI(), FastAPI()]
    backends = [
        StreamBackend(Redis(socket_timeout=30)),
        StreamBackend(Redis(socket_timeout=30)),
    ]
    for app, backend in zip(apps, backends, strict=True):
        FastAPIRedis(app).streams(backend=backend)
        request = Request({"type": "http", "app": app, "headers": []})
        assert await get_stream_backend(request) is backend


@pytest.mark.parametrize("shared_pool", [False, True])
async def test_shared_reader_connections_rejected(
    shared_pool: bool, fresh_settings: Any
) -> None:
    commands = Redis(socket_timeout=30)
    reader = (
        Redis(connection_pool=commands.connection_pool) if shared_pool else commands
    )
    backend = StreamBackend(commands, blocking_client=reader)
    with pytest.raises(StreamConfigurationError, match="share"):
        async with backend:
            pytest.fail("startup should fail")
    assert not backend._running
    await commands.aclose()


async def test_reader_socket_timeout_validated(fresh_settings: Any) -> None:
    command = Redis(socket_timeout=1)  # A short command timeout is allowed.
    reader = Redis(socket_timeout=1)
    backend = StreamBackend(command, blocking_client=reader)
    with pytest.raises(StreamConfigurationError, match="socket_timeout"):
        async with backend:
            pytest.fail("startup should fail")
    await reader.aclose()
    await command.aclose()


async def test_builder_reader_socket_timeout_validated(fresh_settings: Any) -> None:
    from redis_fastapi.lifespan import redis_lifespan

    reader = Redis(socket_timeout=1)
    app = FastAPI()
    FastAPIRedis(app).streams(blocking_client=reader)
    with pytest.raises(StreamConfigurationError, match="socket_timeout"):
        async with redis_lifespan(app):
            pytest.fail("startup should fail")
    assert app.state._redis.async_pool is None
    assert not app.state._redis_streams._running
    await reader.aclose()


@pytest.mark.parametrize("failure", [RuntimeError("startup"), asyncio.CancelledError()])
async def test_startup_failure_releases_lifecycle(
    failure: BaseException,
    monkeypatch: pytest.MonkeyPatch,
    fresh_settings: Any,
) -> None:
    client = Redis(socket_timeout=30)
    backend = StreamBackend(client)
    monkeypatch.setattr(backend, "detect", AsyncMock(side_effect=failure))
    with pytest.raises(type(failure)):
        async with backend:
            pytest.fail("startup should fail")
    assert backend._closing
    assert not backend._running
    assert not backend._tasks
    monkeypatch.setattr(backend, "detect", AsyncMock(return_value=True))
    async with backend:
        with pytest.raises(StreamConfigurationError, match="already running"):
            async with backend:
                pytest.fail("a second start should fail")
    await client.aclose()


async def test_redis_lifespan_runs_an_injected_backend(
    monkeypatch: pytest.MonkeyPatch, fresh_settings: Any
) -> None:
    events = []
    client = Redis(socket_timeout=30)
    backend = StreamBackend(client)
    caps = backend._caps
    monkeypatch.setattr(backend, "detect", AsyncMock(return_value=True))

    @asynccontextmanager
    async def original(app: FastAPI):
        assert backend._running
        events.append("start")
        yield {"value": 42}
        assert backend._running
        events.append("stop")

    app = FastAPI(lifespan=original)
    FastAPIRedis(app).lifespan().streams(backend=backend)
    async with app.router.lifespan_context(app) as state:
        assert state == {"value": 42}
        # Its own support cache, not the pool's.
        assert backend._caps is not app.state._redis.stream_capabilities
        assert backend._caps is not caps
    assert events == ["start", "stop"]
    assert not backend._running
    await client.aclose()


@pytest.mark.parametrize("failure", [RuntimeError("startup"), asyncio.CancelledError()])
async def test_redis_lifespan_cleans_pool_when_injected_startup_fails(
    failure: BaseException,
    monkeypatch: pytest.MonkeyPatch,
    fresh_settings: Any,
) -> None:
    from redis_fastapi.deps import _PoolState
    from redis_fastapi.lifespan import redis_lifespan

    client = Redis(socket_timeout=30)
    backend = StreamBackend(client)
    pool = AsyncMock()
    monkeypatch.setattr(_PoolState, "build_async_pool", lambda: pool)
    monkeypatch.setattr(backend, "detect", AsyncMock(side_effect=failure))
    app = FastAPI()
    add_redis_streams(app, backend=backend)
    with pytest.raises(type(failure)):
        async with redis_lifespan(app):
            pytest.fail("startup should fail")
    pool.aclose.assert_awaited_once()
    assert app.state._redis.async_pool is None
    assert not backend._running
    await client.aclose()
