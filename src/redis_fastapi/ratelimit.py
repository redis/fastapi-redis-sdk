"""Dependency-injection based rate limiting for FastAPI.

Exposes:

* :func:`rate_limit` - a ``Depends()``-compatible per-route dependency,
* :class:`RateLimitMiddleware` - an app-wide global limiter / header injector,
* :func:`ip_identifier` - the ready-made default key strategy (pass your own
  callable via ``identifier=`` to key by user, API key, etc.),
* :class:`RateLimitExceeded` - the control-flow exception returning a 429,
* :func:`add_redis_rate_limiting` - one-time app wiring.

All limits use the INCREX window-counter pattern via
:class:`~redis_fastapi.ratelimit_backend.RateLimitBackend`.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from inspect import isawaitable
from typing import Any, cast

from fastapi import Depends, FastAPI, Request, Response
from fastapi.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from redis_fastapi.config import get_settings
from redis_fastapi.deps import get_rate_limit_backend
from redis_fastapi.rate import Rate, parse_rate
from redis_fastapi.ratelimit_backend import (
    RateLimitBackend,
    RateLimitResult,
    _validate_cost,
)
from redis_fastapi.telemetry import (
    ratelimit_span,
    record_rate_limit_request,
    timed_rate_limit,
)

logger = logging.getLogger(__name__)

# request.state attribute carrying the result so the middleware can emit headers.
_STATE_ATTR = "redis_rate_limit"

# Callable signatures (sync or async are both accepted at runtime).
#
# An ``Identifier`` returns the *client identity* segment of the counter key
# (e.g. an IP or user id) — **not** the whole key.  The backend composes the
# final ``{prefix}:{scope}:{identifier}`` key, adding the scope and prefix
# itself, so an identifier only answers "which client is this?".  It is
# deliberately narrower than the caching ``KeyBuilder`` (which builds a fuller
# key and takes ``scope``/``prefix`` args that a rate-limit identifier never
# needs).
Identifier = Callable[[Request], "str | Awaitable[str]"]
SkipWhen = Callable[[Request], "bool | Awaitable[bool]"]
OnLimitExceeded = Callable[[Request, RateLimitResult], "Response | Awaitable[Response]"]


@dataclass
class _RateLimitState:
    """Stashed on ``request.state`` so the middleware can emit headers."""

    result: RateLimitResult
    window: int
    emit_headers: bool
    ietf_headers: bool


# ---------------------------------------------------------------------------
# Identifiers / key strategies
# ---------------------------------------------------------------------------


def _client_ip(request: Request, *, trust_proxy: bool | None = None) -> str:
    """Return the client IP, honoring ``X-Forwarded-For`` when trusted.

    Args:
        request: The incoming request.
        trust_proxy: Override the ``REDIS_RATE_LIMIT_TRUST_PROXY`` setting.
    """
    trusted = (
        get_settings().rate_limit_trust_proxy if trust_proxy is None else trust_proxy
    )
    if trusted:
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()
    if request.client is not None:
        return request.client.host
    return "unknown"


def ip_identifier(request: Request) -> str:
    """Default identifier: the client IP.

    Returns the per-client identity **only**.  The counter is separated per
    route by the *scope* segment (which :func:`rate_limit` defaults to the
    matched route template), so identifiers stay path-agnostic.  Matches the
    :data:`Identifier` signature.
    """
    return _client_ip(request)


def _route_scope(request: Request) -> str:
    """Return the matched route's path **template** (e.g. ``/items/{id}``).

    This is deliberately the template, not ``request.url.path``: keying on the
    concrete path would give ``/items/1`` and ``/items/2`` separate counters
    (a trivial limit bypass) and explode metric-label cardinality.  Keying on
    the template makes all values of a path parameter share one bucket, i.e.
    per-endpoint scoping.  Falls back to the concrete path if no route is on the
    ASGI scope (should not happen for a matched dependency).
    """
    route = request.scope.get("route")
    template = getattr(route, "path", None)
    return template or request.url.path


async def _resolve_identifier(identifier: Identifier, request: Request) -> str:
    """Call an identifier (sync or async) and return the client-identity string."""
    result = identifier(request)
    if isawaitable(result):
        result = await result
    return result


# ---------------------------------------------------------------------------
# Response / header helpers (shared by the dependency and the middleware)
# ---------------------------------------------------------------------------


def _metric_result(result: RateLimitResult) -> str:
    """Map a check outcome to its telemetry ``result`` label.

    A degraded (Redis-unreachable) result is reported as ``"error"`` regardless
    of whether it failed open or closed, so an outage is visible in metrics
    instead of masquerading as a normal ``allowed`` / ``limited`` outcome.
    """
    if result.degraded:
        return "error"
    return "allowed" if result.allowed else "limited"


def _rate_limit_headers(
    result: RateLimitResult, window: int, *, emit_headers: bool, ietf_headers: bool
) -> dict[str, str]:
    """Build the rate-limit response headers for *result*.

    The two families are independent: ``emit_headers`` controls the legacy
    ``X-RateLimit-*`` trio and ``ietf_headers`` the standards-track
    ``RateLimit`` / ``RateLimit-Policy`` fields.  Either, both, or neither can
    be enabled — asking for IETF headers does **not** force the ``X-*`` ones.
    """
    headers: dict[str, str] = {}
    if emit_headers:
        headers["X-RateLimit-Limit"] = str(result.limit)
        headers["X-RateLimit-Remaining"] = str(result.remaining)
        headers["X-RateLimit-Reset"] = str(result.reset_at)
    if ietf_headers:
        headers["RateLimit-Policy"] = f'"default";q={result.limit};w={window}'
        headers["RateLimit"] = f'"default";r={result.remaining};t={result.reset_after}'
    return headers


def _apply_headers(
    raw_headers: list[tuple[bytes, bytes]], state: _RateLimitState
) -> list[tuple[bytes, bytes]]:
    """Append rate-limit headers (ASGI form) unless already present."""
    if not state.emit_headers and not state.ietf_headers:
        return raw_headers
    existing = {name.lower() for name, _ in raw_headers}
    extra: list[tuple[bytes, bytes]] = []
    for name, value in _rate_limit_headers(
        state.result,
        state.window,
        emit_headers=state.emit_headers,
        ietf_headers=state.ietf_headers,
    ).items():
        if name.lower().encode() not in existing:
            extra.append((name.encode(), value.encode()))
    return raw_headers + extra


async def _build_429(
    request: Request,
    result: RateLimitResult,
    window: int,
    *,
    on_limit_exceeded: OnLimitExceeded | None,
    emit_headers: bool,
    ietf_headers: bool,
) -> Response:
    """Build the 429 response (custom callable or default) with headers set."""
    if on_limit_exceeded is not None:
        response = on_limit_exceeded(request, result)
        if isawaitable(response):
            response = await response
    else:
        response = JSONResponse({"detail": "Too Many Requests"}, status_code=429)
    response.headers["Retry-After"] = str(result.retry_after)
    if emit_headers or ietf_headers:
        for name, value in _rate_limit_headers(
            result, window, emit_headers=emit_headers, ietf_headers=ietf_headers
        ).items():
            response.headers.setdefault(name, value)
    return response


def _is_more_constraining(new: RateLimitResult, current: RateLimitResult) -> bool:
    """Is *new* the tighter of two limits applied to the same request?

    A rejection always wins.  Between two allowed results, the one with fewer
    requests left is the one the client will hit first.
    """
    if new.allowed != current.allowed:
        return not new.allowed
    return new.remaining < current.remaining


def _store_state(request: Request, state: _RateLimitState) -> None:
    """Stash *state* for header emission, keeping the most constraining limit.

    A request can be checked twice - the global limiter in the middleware, then
    a per-route ``rate_limit()`` dependency - and only one set of headers goes
    out.  Last-write-wins would report the route's counter even when the global
    one is nearly exhausted, so a client would read ``Remaining: 47`` and then
    take a 429 on its next request with no warning.  The headers describe the
    limit that binds first, which is what
    `draft-ietf-httpapi-ratelimit-headers
    <https://datatracker.ietf.org/doc/draft-ietf-httpapi-ratelimit-headers/>`_
    expects of a single-policy response.
    """
    current = getattr(request.state, _STATE_ATTR, None)
    if isinstance(current, _RateLimitState) and not _is_more_constraining(
        state.result, current.result
    ):
        return
    setattr(request.state, _STATE_ATTR, state)


async def _apply_limit(
    request: Request,
    backend: RateLimitBackend,
    *,
    identifier: Identifier,
    rate: Rate,
    scope: str,
    cost: int,
    skip_when: SkipWhen | None,
    on_limit_exceeded: OnLimitExceeded | None,
    emit_headers: bool,
    ietf_headers: bool,
    fail_closed: bool | None,
    span_name: str,
) -> Response | None:
    """Run one rate-limit check, shared by the per-route dependency and the
    global middleware limiter.

    Performs the full choreography: honor *skip_when*, resolve the client
    identity, run ``backend.hit`` inside a timed span, stash the result on
    ``request.state`` for header injection, and record the metric.

    Returns the 429 :class:`~starlette.responses.Response` when the request is
    over the limit, or ``None`` when it is allowed or bypassed.  Callers decide
    what to do with a returned response — the dependency raises it as
    :class:`RateLimitExceeded`, the middleware returns it directly — which is
    the only behavioral difference between the two call sites.
    """
    if skip_when is not None:
        skip = skip_when(request)
        if isawaitable(skip):
            skip = await skip
        if skip:
            record_rate_limit_request(result="bypass", scope=scope)
            return None

    # The identity is per-client only; route separation lives in the scope.
    identity = await _resolve_identifier(identifier, request)
    with ratelimit_span(
        span_name,
        attributes={"ratelimit.scope": scope, "ratelimit.limit": rate.limit},
    ) as span:
        with timed_rate_limit(scope):
            result = await backend.hit(
                identity,
                limit=rate.limit,
                window=rate.window,
                scope=scope,
                cost=cost,
                fail_closed=fail_closed,
            )
        if span is not None and result.backend:
            span.set_attribute("ratelimit.backend", result.backend)

    _store_state(
        request, _RateLimitState(result, rate.window, emit_headers, ietf_headers)
    )
    record_rate_limit_request(result=_metric_result(result), scope=scope)
    if result.allowed:
        return None
    return await _build_429(
        request,
        result,
        rate.window,
        on_limit_exceeded=on_limit_exceeded,
        emit_headers=emit_headers,
        ietf_headers=ietf_headers,
    )


# ---------------------------------------------------------------------------
# Control-flow exception (mirrors CacheHitException)
# ---------------------------------------------------------------------------


class RateLimitExceeded(Exception):
    """Raised by ``rate_limit()`` when a request exceeds its limit.

    Intentional control flow, not an error: the registered handler returns
    the carried 429 :class:`~starlette.responses.Response`.  Register it via
    :func:`add_redis_rate_limiting`.
    """

    #: Marker for monitoring tools / exception filters.
    __rate_limit__: bool = True

    def __init__(self, response: Response) -> None:
        super().__init__()
        self.response = response
        self.__suppress_context__ = True


async def rate_limit_exceeded_handler(request: Request, exc: Exception) -> Response:
    """Return the 429 response carried by :class:`RateLimitExceeded`."""
    return cast(RateLimitExceeded, exc).response


# ---------------------------------------------------------------------------
# rate_limit() - per-route DI factory
# ---------------------------------------------------------------------------


def _resolve_rate(
    rate: str | Rate | tuple[int, int] | None,
    limit: int | None,
    window: int | None,
) -> Rate:
    """Resolve the (rate | limit+window) arguments into a single :class:`Rate`."""
    if rate is not None:
        return parse_rate(rate)
    if limit is not None and window is not None:
        return Rate(limit, window)
    raise ValueError(
        "rate_limit() requires either a `rate` spec or both `limit` and `window`"
    )


def rate_limit(
    rate: str | Rate | tuple[int, int] | None = None,
    *,
    limit: int | None = None,
    window: int | None = None,
    scope: str = "",
    identifier: Identifier | None = None,
    cost: int = 1,
    skip_when: SkipWhen | None = None,
    on_limit_exceeded: OnLimitExceeded | None = None,
    emit_headers: bool | None = None,
    ietf_headers: bool | None = None,
    fail_closed: bool | None = None,
) -> Callable[..., Awaitable[None]]:
    """Return a ``Depends()``-compatible per-route rate-limit dependency.

    Requires ``FastAPIRedis(app).rate_limiting()`` (or
    :func:`add_redis_rate_limiting`) so the 429 handler and header middleware
    are registered.

    Args:
        rate: Fluent rate spec (``"100/minute"``), a :class:`Rate`, or a
            ``(limit, window)`` tuple.  Mutually exclusive with *limit*/*window*.
        limit: Request limit (use with *window* instead of *rate*).
        window: Window in seconds (use with *limit*).
        scope: The counter's namespace segment, and the unit of sharing.
            Defaults to the **matched route's path template** (e.g.
            ``/items/{id}``), so each route gets its own per-client counter and
            all values of a path parameter share one bucket. Set an explicit *scope* to
            share **one** counter across every route that uses the same value, to
            isolate two limits stacked on the same route, or to give a route's counter
            a stable label.
        identifier: Key strategy.  Defaults to :func:`ip_identifier`.
        cost: How many units this request consumes.  Default ``1``.
        skip_when: Predicate (sync/async); when truthy the request is not counted.
        on_limit_exceeded: Builds the 429 response (sync/async).  Defaults to a
            JSON ``{"detail": "Too Many Requests"}``.
        emit_headers: Emit ``X-RateLimit-*`` headers.  Defaults to the setting.
        ietf_headers: Also emit IETF ``RateLimit`` headers.  Defaults to the setting.
        fail_closed: Reject when Redis is unreachable.  Defaults to the setting.

    Raises:
        ValueError: If *cost* is not at least 1, or the rate arguments are
            incomplete.  Both are raised at decoration time, so a bad route
            fails at import rather than on its first request.
    """
    _validate_cost(cost)
    resolved = _resolve_rate(rate, limit, window)
    _identifier: Identifier = identifier or ip_identifier
    settings = get_settings()
    _emit = settings.rate_limit_emit_headers if emit_headers is None else emit_headers
    _ietf = settings.rate_limit_ietf_headers if ietf_headers is None else ietf_headers

    async def _dependency(
        request: Request,
        backend: RateLimitBackend = Depends(get_rate_limit_backend),
    ) -> None:
        # An empty scope defaults to the route template, giving each route its
        # own per-client counter; an explicit scope shares/labels the bucket.
        effective_scope = scope or _route_scope(request)
        response = await _apply_limit(
            request,
            backend,
            identifier=_identifier,
            rate=resolved,
            scope=effective_scope,
            cost=cost,
            skip_when=skip_when,
            on_limit_exceeded=on_limit_exceeded,
            emit_headers=_emit,
            ietf_headers=_ietf,
            fail_closed=fail_closed,
            span_name="ratelimit.hit",
        )
        # A dependency cannot return a response, so a rejection is raised and
        # turned into the 429 by the registered handler.
        if response is not None:
            raise RateLimitExceeded(response)

    return _dependency


# ---------------------------------------------------------------------------
# RateLimitMiddleware - app-wide global limiter + header injection
# ---------------------------------------------------------------------------


@dataclass
class _GlobalLimiter:
    """Configuration for the app-wide global limit."""

    rate: Rate
    scope: str
    identifier: Identifier
    skip_when: SkipWhen | None
    on_limit_exceeded: OnLimitExceeded | None
    emit_headers: bool
    ietf_headers: bool
    fail_closed: bool | None


class RateLimitMiddleware:
    """ASGI middleware that enforces an optional global limit and emits headers.

    Two responsibilities, both transparent:

    1. **Header injection** (always): when a ``rate_limit()`` dependency (or
       the global limiter below) stored a result on ``request.state``, the
       ``X-RateLimit-*`` headers are added to the outgoing response.
    2. **Global limiting** (when *limiter* is set): every request is checked
       against an app-wide limit before reaching any route; over-limit
       requests get a 429 directly.

    Registered by :func:`add_redis_rate_limiting`.
    """

    def __init__(self, app: ASGIApp, *, limiter: _GlobalLimiter | None = None) -> None:
        self.app = app
        self._limiter = limiter

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive=receive)

        if self._limiter is not None:
            response = await self._enforce_global(request)
            if response is not None:
                await response(scope, receive, send)
                return

        async def send_with_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                state = getattr(request.state, _STATE_ATTR, None)
                if isinstance(state, _RateLimitState):
                    message["headers"] = _apply_headers(
                        list(message.get("headers", [])), state
                    )
            await send(message)

        await self.app(scope, receive, send_with_headers)

    async def _enforce_global(self, request: Request) -> Response | None:
        """Run the global limit; return a 429 response when over the limit."""
        limiter = cast(_GlobalLimiter, self._limiter)
        return await _apply_limit(
            request,
            await get_rate_limit_backend(request),
            identifier=limiter.identifier,
            rate=limiter.rate,
            scope=limiter.scope,
            cost=1,
            skip_when=limiter.skip_when,
            on_limit_exceeded=limiter.on_limit_exceeded,
            emit_headers=limiter.emit_headers,
            ietf_headers=limiter.ietf_headers,
            fail_closed=limiter.fail_closed,
            span_name="ratelimit.global",
        )


# ---------------------------------------------------------------------------
# add_redis_rate_limiting() - one-time app setup
# ---------------------------------------------------------------------------


def add_redis_rate_limiting(
    app: FastAPI,
    *,
    global_rate: str | Rate | tuple[int, int] | None = None,
    identifier: Identifier | None = None,
    scope: str = "",
    skip_when: SkipWhen | None = None,
    on_limit_exceeded: OnLimitExceeded | None = None,
    ietf_headers: bool | None = None,
    fail_closed: bool | None = None,
) -> None:
    """Register the 429 handler and the rate-limit middleware.

    Prefer the builder API::

        FastAPIRedis(app).lifespan().rate_limiting()

    A global limiter is wired when *global_rate* is provided, or when
    ``REDIS_RATE_LIMIT_DEFAULT_LIMIT`` is positive.

    Args:
        app: The FastAPI application instance.
        global_rate: Enables the app-wide limiter at this rate.
        identifier: Identifier for the global limiter.  Defaults to :func:`ip_identifier`.
        scope: Scope for the global limiter's counters.
        skip_when: Skip predicate for the global limiter.
        on_limit_exceeded: Custom 429 builder for the global limiter.
        ietf_headers: Emit IETF headers for the global limiter.  Defaults to setting.
        fail_closed: Reject on Redis errors for the global limiter.  Defaults to setting.
    """
    settings = get_settings()

    # Tells the lifespan that a startup INCREX probe is worth its round trip;
    # cache-only apps never set it.  Safe before startup: the builder runs at
    # app-construction time, the lifespan reads it later.
    app.state._redis_rate_limiting = True

    app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)

    limiter: _GlobalLimiter | None = None
    rate = _global_rate(global_rate, settings)
    if rate is not None:
        limiter = _GlobalLimiter(
            rate=rate,
            scope=scope,
            identifier=identifier or ip_identifier,
            skip_when=skip_when,
            on_limit_exceeded=on_limit_exceeded,
            emit_headers=settings.rate_limit_emit_headers,
            ietf_headers=(
                settings.rate_limit_ietf_headers
                if ietf_headers is None
                else ietf_headers
            ),
            fail_closed=fail_closed,
        )

    app.add_middleware(RateLimitMiddleware, limiter=limiter)


def _global_rate(
    global_rate: str | Rate | tuple[int, int] | None, settings: Any
) -> Rate | None:
    """Resolve the global rate from the explicit arg or the settings default."""
    if global_rate is not None:
        return parse_rate(global_rate)
    if settings.rate_limit_default_limit > 0:
        return Rate(
            settings.rate_limit_default_limit, settings.rate_limit_default_window
        )
    return None
