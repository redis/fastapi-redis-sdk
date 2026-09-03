"""FastAPI dependency providers for async Redis clients."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Annotated, TypeAlias

if TYPE_CHECKING:
    from redis_fastapi.cache_backend import CacheBackend, SyncCacheBackend
    from redis_fastapi.ratelimit_backend import (
        RateLimitBackend,
        SyncRateLimitBackend,
        _BackendCapabilities,
    )
    from redis_fastapi.session_backend import _StoreCapabilities

from fastapi import Depends, FastAPI, Request
from redis.asyncio import ConnectionPool as AsyncConnectionPool
from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster

from redis_fastapi.config import get_settings

# Imported at runtime, not under TYPE_CHECKING, and that is load-bearing.
# FastAPI resolves an endpoint's annotations with ``get_type_hints``, which
# evaluates the forward reference inside ``Annotated[...]`` against *this*
# module's namespace.  A name that exists only for the type checker raises
# NameError there, and FastAPI then treats the parameter as an ordinary query
# parameter - so the endpoint answers 422 instead of receiving its session.
# Under ``from __future__ import annotations`` in the caller's module this is
# the only spelling that works.  There is no import cycle: session_backend
# never imports deps at module level.
from redis_fastapi.session_backend import RedisSessionStore, SyncSessionStore
from redis_fastapi.sessions import Session

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

    # Shared, pool-lifetime rate-limit capability cache (INCREX / EVAL support
    # and the registered Lua script).  Lazily created by
    # ``get_rate_limit_backend`` and reset on shutdown so detection is paid
    # once per process, not per request.  The type is a TYPE_CHECKING-only
    # forward ref (annotations are lazy here), so this needs no runtime import
    # and there is no import cycle: ratelimit_backend never imports deps.
    ratelimit_capabilities: _BackendCapabilities | None = None

    # The same idea for the session store: HSETEX support is a property of the
    # server, so it is discovered once per pool rather than on every request.
    session_capabilities: _StoreCapabilities | None = None

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
        self.ratelimit_capabilities = None
        self.session_capabilities = None


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
    from redis_fastapi.cache_backend import CacheBackend

    client = await get_async_redis(request)
    return CacheBackend(client)


async def get_sync_cache_backend(request: Request) -> SyncCacheBackend:
    """Return a :class:`SyncCacheBackend` for use in sync endpoints.

    The underlying async :class:`CacheBackend` is resolved on the event
    loop; the returned wrapper bridges each call back via
    :func:`anyio.from_thread.run`.
    """
    from redis_fastapi.cache_backend import SyncCacheBackend

    backend = await get_cache_backend(request)
    return SyncCacheBackend(backend)


async def get_rate_limit_backend(request: Request) -> RateLimitBackend:
    """Return a :class:`RateLimitBackend` backed by the shared async pool.

    The backend is constructed per request, but its server-capability cache is
    sourced from (and stored on) the pool state so INCREX / EVAL detection is
    paid once per process rather than re-probed on every request.
    """
    from redis_fastapi.ratelimit_backend import (
        RateLimitBackend,
        _BackendCapabilities,
    )

    state = _get_pool_state(request.app)
    if state.ratelimit_capabilities is None:
        state.ratelimit_capabilities = _BackendCapabilities()
    client = await get_async_redis(request)
    return RateLimitBackend(client, capabilities=state.ratelimit_capabilities)


async def get_sync_rate_limit_backend(request: Request) -> SyncRateLimitBackend:
    """Return a :class:`SyncRateLimitBackend` for use in sync endpoints.

    The underlying async :class:`RateLimitBackend` is resolved on the event
    loop; the returned wrapper bridges each call back via
    :func:`anyio.from_thread.run`.
    """
    from redis_fastapi.ratelimit_backend import SyncRateLimitBackend

    backend = await get_rate_limit_backend(request)
    return SyncRateLimitBackend(backend)


async def get_session_store(request: Request) -> RedisSessionStore:
    """Return a :class:`RedisSessionStore` backed by the shared async pool.

    Built per request, but its server-capability cache lives on the pool
    state, so ``HSETEX`` detection is paid once per process rather than
    re-probed on every request.
    """
    from redis_fastapi.session_backend import RedisSessionStore, _StoreCapabilities

    state = _get_pool_state(request.app)
    if state.session_capabilities is None:
        state.session_capabilities = _StoreCapabilities()
    client = await get_async_redis(request)
    return RedisSessionStore(client, capabilities=state.session_capabilities)


async def get_sync_session_store(request: Request) -> SyncSessionStore:
    """Return a :class:`SyncSessionStore` for use in sync endpoints.

    The underlying async store is resolved on the event loop; the returned
    wrapper bridges each call back via :func:`anyio.from_thread.run`.
    """
    from redis_fastapi.session_backend import SyncSessionStore

    store = await get_session_store(request)
    return SyncSessionStore(store)


async def get_session(request: Request) -> Session:
    """Return the session the middleware already loaded for this request.

    Performs no I/O.  The read happened before the application ran, because
    ``request.session`` is a synchronous property and cannot await.
    """
    from redis_fastapi.sessions import session_of

    return session_of(request)


AsyncRedisDep = Annotated[AsyncClient, Depends(get_async_redis)]
CacheBackendDep = Annotated["CacheBackend", Depends(get_cache_backend)]
SyncCacheBackendDep = Annotated["SyncCacheBackend", Depends(get_sync_cache_backend)]
RateLimitBackendDep = Annotated["RateLimitBackend", Depends(get_rate_limit_backend)]
SyncRateLimitBackendDep = Annotated[
    "SyncRateLimitBackend", Depends(get_sync_rate_limit_backend)
]
SessionStoreDep = Annotated[RedisSessionStore, Depends(get_session_store)]
SyncSessionStoreDep = Annotated[SyncSessionStore, Depends(get_sync_session_store)]
SessionDep = Annotated[Session, Depends(get_session)]
