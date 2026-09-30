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

from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from redis_fastapi.cache import add_redis_caching
from redis_fastapi.lifespan import redis_lifespan

if TYPE_CHECKING:
    from fastapi import FastAPI
    from starlette.requests import Request

    from redis_fastapi.rate import Rate
    from redis_fastapi.ratelimit import Identifier, OnLimitExceeded, SkipWhen
    from redis_fastapi.sessions import CookieSpec, Session
    from redis_fastapi.types import Challenge


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

    def lifespan(self) -> FastAPIRedis:
        """Manage Redis connection pools across the application lifecycle.

        Wraps the existing ``app.router.lifespan_context`` so that Redis
        pools are available for the duration of the app (and for any
        other lifespan handlers already registered).

        Supports both standalone and OSS Cluster modes based on
        ``get_settings().cluster``.

        Calling this method more than once on the same app is a no-op.
        """
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

    def sessions(
        self,
        *,
        principal_of: Callable[[Session], Any] | None = None,
        principal_keys: list[str] | None = None,
        subject_of: Callable[[Session], str | None] | None = None,
        cookie_builder: Callable[[CookieSpec], str] | None = None,
        descriptor_of: Callable[[Request, Session], dict[str, Any]] | None = None,
        skip: Callable[[Request], bool] | None = None,
        challenge: Challenge | None = None,
        **store_options: Any,
    ) -> FastAPIRedis:
        """Register the session middleware.

        Required for ``request.session``, ``SessionDep`` and
        ``SessionStoreDep`` to work.  Rotation after a sign-in or a privilege
        change is automatic - the middleware detects the change rather than
        waiting to be told, so there is no rotation call to forget::

            FastAPIRedis(app).lifespan().sessions(
                principal_keys=["user_id", "role"]
            )

        Calling this method more than once on the same app is a no-op.

        Args:
            principal_of: Pure function whose changing return value triggers a
                rotation.  Defaults to reading ``session_principal_keys``.
            principal_keys: The session keys to watch, for the common case
                where a function is overkill.
            subject_of: Which subject a session is indexed under; ``None``
                leaves it out of the index.
            cookie_builder: Renders the ``Set-Cookie`` value, for setting and
                for clearing it.
            descriptor_of: What a device listing shows for this session - an
                IP, a user agent, a device name.
            **store_options: Passed to :func:`add_redis_sessions` - ``store``,
                ``store_factory``, ``coder``, ``encryptor``, ``id_factory``,
                ``key_prefix``, ``idle_ttl``, ``absolute_ttl``, ``gc_ttl``.
            skip: Requests that need no session at all, at zero Redis cost.
            challenge: The ``WWW-Authenticate`` header on a ``valid_session()``
                rejection: a FastAPI security scheme, a string, or a callable.
                Omitted by default.
        """
        from redis_fastapi.sessions import SessionMiddleware, add_redis_sessions

        if self._has_middleware(SessionMiddleware):
            return self
        add_redis_sessions(
            self._app,
            principal_of=principal_of,
            principal_keys=principal_keys,
            subject_of=subject_of,
            cookie_builder=cookie_builder,
            descriptor_of=descriptor_of,
            skip=skip,
            challenge=challenge,
            **store_options,
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
