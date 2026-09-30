"""Tests for custom pool injection (``pool_factory`` / ``redis_pool_factory``).

Deployments whose connection parameters are not static environment variables
(Sentinel-managed masters, secret-manager-issued passwords) need to hand the
lifespan a pool they built themselves. These tests assert the three contract
points: the factory's pool is the one dependencies resolve to, an async
factory is awaited, and cluster mode refuses the override explicitly.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from redis.asyncio import ConnectionPool as AsyncConnectionPool

from redis_fastapi import FastAPIRedis
from redis_fastapi.deps import get_async_redis
from redis_fastapi.lifespan import redis_lifespan


def _make_pool() -> AsyncConnectionPool:
    # Never connected in these tests — identity is what matters.
    return AsyncConnectionPool.from_url("redis://injection-test:6379/5")


@pytest.mark.asyncio
async def test_sync_factory_pool_is_used() -> None:
    pool = _make_pool()
    app = FastAPI()
    FastAPIRedis(app).lifespan(pool_factory=lambda: pool)

    async with app.router.lifespan_context(app):
        state = app.state._redis
        assert state.async_pool is pool
        client = state.get_async_client()
        assert client.connection_pool is pool


@pytest.mark.asyncio
async def test_async_factory_is_awaited() -> None:
    pool = _make_pool()

    async def factory() -> AsyncConnectionPool:
        return pool

    app = FastAPI()
    FastAPIRedis(app).lifespan(pool_factory=factory)

    async with app.router.lifespan_context(app):
        assert app.state._redis.async_pool is pool


@pytest.mark.asyncio
async def test_state_attribute_alone_is_honoured() -> None:
    """Setting ``app.state.redis_pool_factory`` directly (the plain
    ``FastAPI(lifespan=redis_lifespan)`` path) works without the builder."""
    pool = _make_pool()
    app = FastAPI(lifespan=redis_lifespan)
    app.state.redis_pool_factory = lambda: pool

    async with app.router.lifespan_context(app):
        assert app.state._redis.async_pool is pool


@pytest.mark.asyncio
async def test_cluster_mode_refuses_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REDIS_CLUSTER", "true")
    from redis_fastapi.config import get_settings

    get_settings.cache_clear()
    try:
        app = FastAPI()
        FastAPIRedis(app).lifespan(pool_factory=_make_pool)
        with pytest.raises(ValueError, match="cluster mode"):
            async with app.router.lifespan_context(app):
                pass  # pragma: no cover — must not be reached
    finally:
        get_settings.cache_clear()


@pytest.mark.asyncio
async def test_no_factory_builds_from_settings() -> None:
    """Regression guard: without a factory the settings-built pool remains."""
    app = FastAPI()
    FastAPIRedis(app).lifespan()

    async with app.router.lifespan_context(app):
        assert app.state._redis.async_pool is not None
