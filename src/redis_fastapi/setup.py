"""Builder for fastapi-redis-sdk app setup.

Provides a fluent API for configuring Redis integration with FastAPI::

    from fastapi import FastAPI
    from redis_fastapi import FastAPIRedis

    app = FastAPI()
    FastAPIRedis(app).lifespan().caching()

Each method returns ``self`` so calls can be chained.  The builder
composes with any existing lifespan by wrapping
``app.router.lifespan_context`` - multiple libraries can each add
their own lifespan logic without conflicting.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from redis_fastapi.cache import add_redis_caching
from redis_fastapi.lifespan import redis_lifespan

if TYPE_CHECKING:
    from fastapi import FastAPI

    from redis_fastapi.rate import Rate
    from redis_fastapi.ratelimit import Identifier, OnLimitExceeded, SkipWhen


class FastAPIRedis:
    """Fluent builder for fastapi-redis-sdk app setup.

    Usage::

        app = FastAPI()

        # Connection pools only
        FastAPIRedis(app).lifespan()

        # Connection pools + DI-based caching
        FastAPIRedis(app).lifespan().caching()

    The builder wraps any existing lifespan on the app - it does **not**
    replace it.  This means multiple libraries can each call their own
    setup without conflicting.
    """

    def __init__(self, app: FastAPI) -> None:
        self._app = app

    def _has_middleware(self, cls: type) -> bool:
        """Check whether *cls* is already registered in ``app.user_middleware``."""
        return any(m.cls is cls for m in self._app.user_middleware)  # type: ignore[comparison-overlap]

    def lifespan(self, *, pool_factory: Any = None) -> FastAPIRedis:
        """Manage Redis connection pools across the application lifecycle.

        Wraps the existing ``app.router.lifespan_context`` so that Redis
        pools are available for the duration of the app (and for any
        other lifespan handlers already registered).

        Supports both standalone and OSS Cluster modes based on
        ``get_settings().cluster``.

        Args:
            pool_factory: Optional zero-argument callable (sync or async)
                returning the ``redis.asyncio.ConnectionPool`` to use
                instead of one built from environment settings. Lets
                deployments whose connection parameters are not static env
                vars — Sentinel-managed clusters, secret-manager-issued
                passwords — supply their own pool while keeping every
                caching and rate-limiting dependency unchanged. The
                lifespan owns the returned pool and closes it on shutdown.
                Not supported in cluster mode.

        Calling this method more than once on the same app is a no-op.
        """
        if pool_factory is not None:
            self._app.state.redis_pool_factory = pool_factory
        if getattr(self._app.router.lifespan_context, "_redis_lifespan", False):
            return self

        existing = self._app.router.lifespan_context

        @asynccontextmanager
        async def wrapped(
            app: FastAPI,
        ) -> AsyncIterator[Mapping[str, Any] | None]:
            async with redis_lifespan(app):
                async with existing(app) as state:
                    yield state

        wrapped._redis_lifespan = True  # type: ignore[attr-defined]
        self._app.router.lifespan_context = wrapped  # type: ignore[assignment]
        return self

    def caching(self) -> FastAPIRedis:
        """Register the ``CacheHitException`` handler and capture middleware.

        Required for ``cache()``, ``cache_evict()``, and ``cache_put()``
        DI dependencies to work.

        Calling this method more than once on the same app is a no-op.
        """
        from redis_fastapi.cache import CacheResponseCaptureMiddleware

        if self._has_middleware(CacheResponseCaptureMiddleware):
            return self
        add_redis_caching(self._app)
        return self

    def rate_limiting(
        self,
        *,
        global_rate: str | Rate | tuple[int, int] | None = None,
        identifier: Identifier | None = None,
        scope: str = "",
        skip_when: SkipWhen | None = None,
        on_limit_exceeded: OnLimitExceeded | None = None,
        ietf_headers: bool | None = None,
        fail_closed: bool | None = None,
    ) -> FastAPIRedis:
        """Register the ``RateLimitExceeded`` handler and rate-limit middleware.

        Required for the ``rate_limit()`` dependency to work.  Passing
        *global_rate* (e.g. ``"1000/minute"``) — or setting
        ``REDIS_RATE_LIMIT_DEFAULT_LIMIT`` — also enables an app-wide limiter
        applied to every route.

        Calling this method more than once on the same app is a no-op.
        """
        from redis_fastapi.ratelimit import (
            RateLimitMiddleware,
            add_redis_rate_limiting,
        )

        if self._has_middleware(RateLimitMiddleware):
            return self
        add_redis_rate_limiting(
            self._app,
            global_rate=global_rate,
            identifier=identifier,
            scope=scope,
            skip_when=skip_when,
            on_limit_exceeded=on_limit_exceeded,
            ietf_headers=ietf_headers,
            fail_closed=fail_closed,
        )
        return self

    def otel(self) -> FastAPIRedis:
        """Enable OpenTelemetry instrumentation for cache operations.

        Emits spans and metrics for ``cache()``, ``cache_evict()``,
        ``cache_put()``, and ``CacheBackend`` operations.  Composes with
        ``FastAPIInstrumentor`` (HTTP spans) and redis-py native OTel
        (command spans).

        Requires ``pip install fastapi-redis-sdk[otel]``.
        """
        from redis_fastapi.telemetry import enable_telemetry

        enable_telemetry()
        return self
