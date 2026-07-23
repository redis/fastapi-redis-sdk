"""FastAPI dependency providers for async Redis clients."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Annotated, TypeAlias

if TYPE_CHECKING:
    from redis_fastapi.cache_backend import CacheBackend, SyncCacheBackend
    from redis_fastapi.pubsub import PubSubManager

from fastapi import Depends, FastAPI, Request
from redis.asyncio import ConnectionPool as AsyncConnectionPool
from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster

from redis_fastapi.config import get_settings

logger = logging.getLogger(__name__)

# Type alias for async clients (standalone or cluster)
AsyncClient: TypeAlias = AsyncRedis | AsyncRedisCluster


class _PoolState:
    """Connection pool / cluster state stored on ``app.state._redis``.

    Initialised by the lifespan and accessed via :func:`_get_pool_state`.
    Client instances are cached so that ``get_async_redis()`` returns
    the same wrapper on every call, avoiding the overhead of creating
    (and closing) a new wrapper per request.
    """

    async_pool: AsyncConnectionPool | None = None
    async_cluster: AsyncRedisCluster | None = None
    _async_client: AsyncRedis | None = None

    # -- pool / cluster builders (static) -----------------------------------

    @staticmethod
    def build_async_pool() -> AsyncConnectionPool:
        """Create an async ``ConnectionPool`` from settings (URL or KV)."""
        settings = get_settings()
        kw = settings.connection_kwargs()
        url = kw.pop("url", None)
        if url is not None:
            return AsyncConnectionPool.from_url(url, **kw)
        return AsyncConnectionPool(**kw)

    @staticmethod
    def build_async_cluster() -> AsyncRedisCluster:
        """Create an async ``RedisCluster`` from settings."""
        settings = get_settings()
        kw = settings.connection_kwargs()
        url = kw.pop("url", None)
        if url is not None:
            return AsyncRedisCluster.from_url(url, **kw)
        return AsyncRedisCluster(**kw)

    # -- client accessors ---------------------------------------------------

    def get_async_client(self) -> AsyncClient:
        """Return a cached async Redis client backed by the shared pool.

        Raises:
            RuntimeError: If no pool/cluster has been initialized by the
                lifespan.  Call ``FastAPIRedis(app).lifespan()`` or use
                ``redis_lifespan`` before injecting Redis dependencies.
        """
        settings = get_settings()
        if settings.cluster:
            cluster = self.async_cluster
            if cluster is None:
                raise RuntimeError(
                    "Redis cluster not initialised — no lifespan has been "
                    "registered.  Call FastAPIRedis(app).lifespan() or "
                    "compose redis_lifespan in your own lifespan handler."
                )
            return cluster
        pool = self.async_pool
        if pool is None:
            raise RuntimeError(
                "Redis connection pool not initialised — no lifespan has "
                "been registered.  Call FastAPIRedis(app).lifespan() or "
                "compose redis_lifespan in your own lifespan handler."
            )
        client = self._async_client
        if client is None or client.connection_pool is not pool:
            client = AsyncRedis(connection_pool=pool)
            self._async_client = client
        return client

    def clear(self) -> None:
        """Reset cached clients (called during lifespan shutdown)."""
        self._async_client = None


def _get_pool_state(app: FastAPI) -> _PoolState:
    """Return the ``_PoolState`` attached to *app*, creating one if needed."""
    state: _PoolState | None = getattr(app.state, "_redis", None)
    if state is None:
        state = _PoolState()
        app.state._redis = state
    return state


async def get_async_redis(request: Request) -> AsyncClient:
    """Return an async Redis client backed by the shared connection pool.

    Returns a cached client instance - the same wrapper is reused
    across calls to avoid per-request overhead.

    In cluster mode returns an ``AsyncRedisCluster`` instance.

    Raises:
        RuntimeError: If no lifespan has initialized the pool.
    """
    return _get_pool_state(request.app).get_async_client()


async def get_cache_backend(request: Request) -> CacheBackend:
    """Return a :class:`CacheBackend` backed by the shared async pool."""
    from redis_fastapi.cache_backend import CacheBackend  # noqa: WPS433

    client = await get_async_redis(request)
    return CacheBackend(client)


async def get_sync_cache_backend(request: Request) -> SyncCacheBackend:
    """Return a :class:`SyncCacheBackend` for use in sync endpoints.

    The underlying async :class:`CacheBackend` is resolved on the event
    loop; the returned wrapper bridges each call back via
    :func:`anyio.from_thread.run`.
    """
    from redis_fastapi.cache_backend import SyncCacheBackend  # noqa: WPS433

    backend = await get_cache_backend(request)
    return SyncCacheBackend(backend)


async def get_pubsub_manager(
    redis: AsyncClient = Depends(get_async_redis),
) -> PubSubManager:
    """Return a :class:`PubSubManager` backed by the shared async pool.

    Use for publishing messages from HTTP endpoints.  For WebSocket
    endpoints, construct ``PubSubManager`` directly::

        from redis_fastapi import PubSubManager
        from redis_fastapi.deps import _get_pool_state

        redis = _get_pool_state(websocket.app).get_async_client()
        pubsub = PubSubManager(redis)
    """
    from redis_fastapi.pubsub import PubSubManager  # noqa: WPS433

    return PubSubManager(redis)


AsyncRedisDep = Annotated[AsyncClient, Depends(get_async_redis)]
CacheBackendDep = Annotated["CacheBackend", Depends(get_cache_backend)]
SyncCacheBackendDep = Annotated["SyncCacheBackend", Depends(get_sync_cache_backend)]
PubSubManagerDep = Annotated["PubSubManager", Depends(get_pubsub_manager)]
