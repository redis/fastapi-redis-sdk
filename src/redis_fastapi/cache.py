"""DI-based caching for fastapi-redis-sdk.

``cache()``, ``cache_evict()``, and ``cache_put()`` are **dependency factories**
that return callables suitable for ``Depends()``.

Setup::

    from redis_fastapi import FastAPIRedis, cache

    app = FastAPI()
    FastAPIRedis(app).lifespan().caching()

Usage::

    @app.get("/items", dependencies=[Depends(cache(ttl=60))])
    async def get_items():
        return {"items": [1, 2, 3]}
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from inspect import isawaitable
from typing import TYPE_CHECKING, Any, cast

from fastapi import Depends, Request
from redis.exceptions import RedisError
from starlette.responses import Response
from starlette.status import HTTP_304_NOT_MODIFIED
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from redis_fastapi.config import CACHE_STATUS_HEADER, get_settings
from redis_fastapi.deps import AsyncClient, _get_pool_state, get_async_redis
from redis_fastapi.telemetry import (
    cache_span,
    record_cache_eviction,
    record_cache_request,
    record_cache_write,
    timed_operation,
)
from redis_fastapi.types import KeyBuilder

if TYPE_CHECKING:
    from fastapi import FastAPI

logger: logging.Logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Key builder
# ---------------------------------------------------------------------------


def _urlencode(value: str) -> str:
    return value.replace(":", "%3A").replace("&", "%26").replace("=", "%3D")


def default_key_builder(
    request: Request,
    eviction_group: str = "",
    prefix: str = "",
) -> str:
    """Build a cache key from the request path and query string.

    Slashes in the path are replaced with colons.
    Query params are sorted and appended as ``key=value`` pairs.

    When an *eviction_group* is provided it is wrapped in Redis hash-tag
    braces (``{eviction_group}``) so that all keys in the same group
    are guaranteed to map to the same hash slot.  This is required
    for Lua-based bulk eviction to work in Redis Cluster and is
    harmless in standalone mode.

    Args:
        request: The incoming HTTP request.
        eviction_group: (optional) Extra group segment inserted into the key. If left empty, no group segment is used.
        prefix: (optional) Key prefix prepended before the group. If left empty, the default prefix is used.

    Returns:
        The colon-delimited cache key string.
    """
    path = request.url.path.strip("/").replace("/", ":")
    parts: list[str] = []
    if prefix:
        parts.append(prefix)
    if eviction_group:
        parts.append(f"{{{eviction_group}}}")
    if path:
        parts.append(path)
    if request.query_params:
        qs = "&".join(
            f"{k}={_urlencode(v)}" for k, v in sorted(request.query_params.items())
        )
        parts.append(qs)
    return ":".join(parts)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_cache_control(header: str | None) -> dict[str, str | bool]:
    """Parse a ``Cache-Control`` header into a directive dict.

    Boolean directives (``no-cache``, ``no-store``) are stored as
    ``True``; value directives (``max-age=60``) are stored as strings.

    Args:
        header: Raw ``Cache-Control`` header value, or ``None``.

    Returns:
        A dict mapping lowercase directive names to ``True`` or their
        string values.
    """
    if not header:
        return {}
    directives: dict[str, str | bool] = {}
    for part in header.split(","):
        part = part.strip()
        if "=" in part:
            key, _, value = part.partition("=")
            directives[key.strip().lower()] = value.strip()
        elif part:
            directives[part.lower()] = True
    return directives


def _is_stale_for_client(
    remaining_ttl: int, ttl: int, cc: dict[str, str | bool]
) -> bool:
    """Return ``True`` when the cached entry is too old for the client's max-age.

    ``remaining_ttl`` is the Redis TTL remaining; ``ttl`` is the original TTL
    used when the entry was stored.  The entry's age is ``ttl - remaining_ttl``.

    Args:
        remaining_ttl: Seconds left on the Redis key.
        ttl: Original TTL the entry was stored with.
        cc: Parsed ``Cache-Control`` directives from the request.

    Returns:
        ``True`` if the entry's age meets or exceeds the client's ``max-age``.
    """
    client_max_age = cc.get("max-age")
    if client_max_age is None:
        return False
    try:
        max_age = int(str(client_max_age))
    except (ValueError, TypeError):
        return False
    age = ttl - remaining_ttl
    return age >= max_age


def _cache_control_value(max_age: int, private: bool) -> str:
    """Build a ``Cache-Control`` response header value.

    When *max_age* is ``0`` (no TTL), no ``max-age`` directive is emitted;
    the header contains only ``no-cache`` (always revalidate via ETag).

    Args:
        max_age: The ``max-age`` value in seconds.  ``0`` means no expiry.
        private: Whether to include the ``private`` directive.

    Returns:
        The formatted header value string.
    """
    if max_age <= 0:
        base = "no-cache"
    else:
        base = f"max-age={max_age}"
    if private:
        return f"private, {base}"
    return base


# ---------------------------------------------------------------------------
# CacheHitException - short-circuit on cache hit
# ---------------------------------------------------------------------------


class CacheHitException(Exception):
    """Raised by the ``cache()`` dependency when a cache hit is found.

    This is **intentional control flow**, not an error.  FastAPI's dependency
    injection system has no mechanism for a dependency to short-circuit an
    endpoint and return a response directly, so an exception caught by a
    registered handler is the standard workaround (used by fastapi-cache2,
    cashews, and others).

    The registered exception handler returns the pre-built ``Response``
    directly, skipping the endpoint.  Register the handler via
    :func:`add_redis_caching`.

    Attributes:
        response: The pre-built :class:`~starlette.responses.Response` to return.
        __cache_hit__: Always ``True``.  Monitoring tools and exception filters
            can check ``getattr(exc, '__cache_hit__', False)`` to distinguish
            cache-hit exceptions from real errors.
    """

    #: Marker for monitoring tools / exception filters.
    __cache_hit__: bool = True

    def __init__(self, response: Response) -> None:
        super().__init__()
        self.response = response
        # Suppress the "During handling of …" chained-traceback noise
        # when this exception is raised inside a try/except block.
        self.__suppress_context__ = True


async def cache_hit_exception_handler(request: Request, exc: Exception) -> Response:
    """Return the cached response carried by the exception.

    Args:
        request: The incoming HTTP request (unused but required by FastAPI).
        exc: The :class:`CacheHitException` instance.

    Returns:
        The pre-built :class:`~starlette.responses.Response` from the exception.
    """
    return cast(CacheHitException, exc).response


# ---------------------------------------------------------------------------
# CachePending - stored in request.state for response capture
# ---------------------------------------------------------------------------


@dataclass
class CachePending:
    """Signals :class:`CacheResponseCaptureMiddleware` to store the response.

    Set by ``cache()`` (on miss) and ``cache_put()`` in
    ``request.state.redis_cache_pending``.  The middleware reads it once
    the response is complete, writes the entry to Redis, and adds
    ``X-Redis-Cache: MISS``, ``ETag``, and ``Cache-Control`` headers.
    """

    key: str
    ttl: int
    private: bool = False
    redis: Any = field(default=None)
    write_through: bool = False


# ---------------------------------------------------------------------------
# cache() - DI factory for read-path caching
# ---------------------------------------------------------------------------


async def _read_cache_entry(
    redis: AsyncClient,
    cache_key: str,
    ttl: int,
    cc: dict[str, str | bool],
    force_refresh: bool,
) -> tuple[bytes | str | None, int]:
    """Read a cache entry from Redis and apply staleness checks.

    Returns:
        ``(cached_data, remaining_ttl)`` on a usable hit, or
        ``(None, 0)`` on miss / error / stale.
    """
    if force_refresh:
        return None, 0

    try:
        pipe = redis.pipeline()
        pipe.get(cache_key)
        pipe.ttl(cache_key)
        cached_data, raw_ttl = await pipe.execute()
        remaining_ttl = max(raw_ttl, 0) if cached_data else 0
    except (RedisError, OSError):
        logger.warning("Error reading cache key '%s':", cache_key, exc_info=True)
        return None, 0

    if cached_data and _is_stale_for_client(remaining_ttl, ttl, cc):
        return None, 0

    return cached_data, remaining_ttl


def _build_hit_response(
    cached_data: bytes | str,
    remaining_ttl: int,
    request: Request,
    private: bool,
) -> Response:
    """Deserialize a cache entry and return a ready-to-send ``Response``.

    Returns a ``304 Not Modified`` when the client's ``If-None-Match``
    matches the stored ETag, otherwise a full ``200`` response.

    Raises:
        json.JSONDecodeError: If *cached_data* is not valid JSON.
        KeyError: If the entry is missing required keys.
    """
    entry = json.loads(cached_data)
    body_bytes = (
        entry["body"].encode() if isinstance(entry["body"], str) else entry["body"]
    )
    etag: str = entry["etag"]
    cc_value = _cache_control_value(remaining_ttl, private)

    if request.headers.get("if-none-match") == etag:
        return Response(
            status_code=HTTP_304_NOT_MODIFIED,
            headers={
                CACHE_STATUS_HEADER: "HIT",
                "ETag": etag,
                "Cache-Control": cc_value,
            },
        )

    return Response(
        content=body_bytes,
        media_type="application/json",
        headers={
            CACHE_STATUS_HEADER: "HIT",
            "ETag": etag,
            "Cache-Control": cc_value,
        },
    )


def cache(
    ttl: int | None = None,
    *,
    eviction_group: str = "",
    cache_prefix: str | None = None,
    key_builder: KeyBuilder | None = None,
    private: bool = False,
) -> Any:
    """Return a ``Depends()``-compatible dependency for response caching.

    On a **cache hit** the dependency raises :class:`CacheHitException`
    (caught by the registered exception handler) so the endpoint never
    executes.  On a **cache miss** it stores a :class:`CachePending` in
    ``request.state`` and yields; the capture middleware writes the
    response to Redis after the endpoint returns.

    Requires ``FastAPIRedis(app).caching()`` (or :func:`add_redis_caching`).

    Args:
        ttl: Time-to-live in seconds.  Defaults to ``settings.default_ttl``
            (``0`` by default, meaning no automatic expiration).
        eviction_group: Extra group segment inserted into the cache key.
        cache_prefix: Override the key prefix.  Defaults to
            ``settings.pattern_prefix("cache")``.
        key_builder: Custom key builder (sync or async).  Defaults to
            :func:`default_key_builder`.
        private: Emit ``Cache-Control: private, max-age=N``.

    Returns:
        An async generator dependency suitable for use with ``Depends()``.
    """
    _settings = get_settings()
    _ttl: int = ttl if ttl is not None else _settings.default_ttl
    _prefix: str = (
        cache_prefix if cache_prefix is not None else _settings.pattern_prefix("cache")
    )
    _key_builder: KeyBuilder = key_builder or default_key_builder

    # Flow: bypass → read cache → HIT (raise) or MISS (yield to endpoint)
    async def _dependency(
        request: Request,
        redis: AsyncClient = Depends(get_async_redis),
    ) -> AsyncGenerator[None, None]:
        cc = _parse_cache_control(request.headers.get("Cache-Control"))

        # 1. Bypass: skip caching for non-GET or no-store requests
        if request.method != "GET" or "no-store" in cc:
            record_cache_request(result="bypass", eviction_group=eviction_group)
            yield
            return

        # 2. Resolve cache key (may be async)
        cache_key = _key_builder(request, eviction_group=eviction_group, prefix=_prefix)
        if isawaitable(cache_key):
            cache_key = await cache_key

        with cache_span(
            "cache.get",
            attributes={
                "cache.key": cache_key,
                "cache.eviction_group": eviction_group,
                "cache.ttl": _ttl,
            },
        ) as span:
            # 3. Attempt cache read (handles no-cache, staleness, errors)
            with timed_operation("get", eviction_group=eviction_group):
                cached_data, remaining_ttl = await _read_cache_entry(
                    redis,
                    cache_key,
                    _ttl,
                    cc,
                    force_refresh="no-cache" in cc,
                )

            # 4. HIT: short-circuit via exception — endpoint never runs
            if cached_data:
                record_cache_request(result="hit", eviction_group=eviction_group)
                if span is not None:
                    span.set_attribute("cache.hit", True)
                try:
                    raise CacheHitException(
                        _build_hit_response(
                            cached_data, remaining_ttl, request, private
                        )
                    )
                except (json.JSONDecodeError, KeyError) as exc:
                    logger.warning("Invalid cache entry for key %s: %s", cache_key, exc)

            # 5. MISS: mark pending so the capture middleware stores the response
            record_cache_request(result="miss", eviction_group=eviction_group)
            if span is not None:
                span.set_attribute("cache.hit", False)

        request.state.redis_cache_pending = CachePending(
            key=cache_key, ttl=_ttl, private=private, redis=redis
        )
        yield

    return _dependency


# ---------------------------------------------------------------------------
# cache_evict() - DI factory for cache invalidation
# ---------------------------------------------------------------------------


async def _evict_by_key(
    redis: AsyncClient,
    request: Request,
    key_builder: KeyBuilder,
    eviction_group: str,
    prefix: str,
) -> None:
    """Delete a single cache key derived from the request."""
    cache_key = key_builder(request, eviction_group=eviction_group, prefix=prefix)
    if isawaitable(cache_key):
        cache_key = await cache_key
    with cache_span(
        "cache.evict",
        attributes={
            "cache.key": cache_key,
            "cache.eviction_group": eviction_group,
            "cache.evict_type": "key",
        },
    ):
        with timed_operation("evict", eviction_group=eviction_group):
            await redis.delete(cache_key)


async def _evict_by_group(
    redis: AsyncClient,
    eviction_group: str,
) -> None:
    """Clear all keys in *eviction_group* via :class:`CacheBackend`."""
    from redis_fastapi.cache_backend import CacheBackend  # noqa: PLC0415

    with cache_span(
        "cache.evict",
        attributes={
            "cache.eviction_group": eviction_group,
            "cache.evict_type": "group",
        },
    ):
        with timed_operation("evict", eviction_group=eviction_group):
            backend = CacheBackend(redis)
            await backend.delete_group(eviction_group or None)


def cache_evict(
    *,
    eviction_group: str = "",
    key_builder: KeyBuilder | None = None,
    prefix: str | None = None,
) -> Any:
    """Return a ``Depends()``-compatible dependency for cache invalidation.

    The eviction runs **after** the endpoint succeeds.  If the endpoint
    raises, no eviction is performed.

    When *key_builder* is provided, the specific key matching the
    current request is deleted.  When omitted, the **entire eviction group**
    is cleared.  If both are provided the *key_builder* takes precedence.

    .. warning::

        When called **without** a *key_builder* **and** with an empty
        *eviction_group* (the default), **all** cache keys under the global
        prefix are deleted — effectively a full cache wipe.

    Args:
        eviction_group: Cache eviction group to evict from.  When empty and no
            *key_builder* is provided, **all** cached keys are deleted.
        key_builder: Custom key builder.  When omitted the entire eviction group
            is cleared.
        prefix: Override the key prefix.

    Returns:
        An async generator dependency suitable for use with ``Depends()``.
    """
    _settings = get_settings()
    _prefix: str = prefix if prefix is not None else _settings.pattern_prefix("cache")
    _key_builder: KeyBuilder | None = key_builder

    if not eviction_group and _key_builder is None:
        raise ValueError(
            "cache_evict() requires at least one of 'eviction_group' or "
            "'key_builder'. Omitting both deletes ALL cache keys under the "
            "global prefix — if this is intentional, provide an eviction_group "
            "explicitly."
        )

    # Flow: yield to endpoint → on success evict key or group
    async def _dependency(
        request: Request,
        redis: AsyncClient = Depends(get_async_redis),
    ) -> AsyncGenerator[None, None]:
        # 1. Let the endpoint run first
        endpoint_successful = False
        try:
            yield
            endpoint_successful = True
        finally:
            # 2. Only evict after a successful endpoint response
            if endpoint_successful:
                try:
                    # 3. Delete specific key or clear entire eviction group
                    if _key_builder is not None:
                        await _evict_by_key(
                            redis, request, _key_builder, eviction_group, _prefix
                        )
                    else:
                        await _evict_by_group(redis, eviction_group)
                    evict_type = "key" if _key_builder is not None else "group"
                    record_cache_eviction(
                        evict_type=evict_type, eviction_group=eviction_group
                    )
                except (RedisError, OSError):
                    logger.warning(
                        "cache_evict failed for eviction_group=%r",
                        eviction_group,
                        exc_info=True,
                    )

    return _dependency


# ---------------------------------------------------------------------------
# cache_put() - DI factory for write-through caching
# ---------------------------------------------------------------------------


def cache_put(
    *,
    ttl: int | None = None,
    eviction_group: str = "",
    key_builder: KeyBuilder | None = None,
    prefix: str | None = None,
    private: bool = False,
) -> Any:
    """Return a ``Depends()``-compatible dependency for write-through caching.

    The endpoint always executes.  The capture middleware stores the
    serialized response in Redis so that subsequent ``cache()`` reads
    see the fresh data.

    Args:
        ttl: Time-to-live in seconds.  Defaults to ``settings.default_ttl``
            (``0`` by default, meaning no automatic expiration).
        eviction_group: Cache eviction group to write into.
        key_builder: Custom key builder.  Defaults to :func:`default_key_builder`.
        prefix: Override the key prefix.
        private: Emit ``Cache-Control: private, max-age=N``.

    Returns:
        An async generator dependency suitable for use with ``Depends()``.
    """
    _settings = get_settings()
    _ttl: int = ttl if ttl is not None else _settings.default_ttl
    _prefix: str = prefix if prefix is not None else _settings.pattern_prefix("cache")
    _key_builder: KeyBuilder = key_builder or default_key_builder

    # Flow: resolve key → mark pending as write-through → yield to endpoint
    async def _dependency(
        request: Request,
        redis: AsyncClient = Depends(get_async_redis),
    ) -> AsyncGenerator[None, None]:
        # 1. Resolve cache key (could be async)
        cache_key = _key_builder(request, eviction_group=eviction_group, prefix=_prefix)
        if isawaitable(cache_key):
            cache_key = await cache_key

        with cache_span(
            "cache.put",
            attributes={
                "cache.key": cache_key,
                "cache.eviction_group": eviction_group,
                "cache.ttl": _ttl,
            },
        ):
            # 2. Mark as write-through; capture middleware writes after endpoint
            request.state.redis_cache_pending = CachePending(
                key=cache_key,
                ttl=_ttl,
                private=private,
                redis=redis,
                write_through=True,
            )
            yield

    return _dependency


# ---------------------------------------------------------------------------
# CacheResponseCaptureMiddleware
# ---------------------------------------------------------------------------


# Maximum response body size (in bytes) that the middleware will buffer
# for caching.  Responses larger than this are passed through without
# being stored in Redis, preventing unbounded memory consumption.
MAX_CACHEABLE_BODY_SIZE: int = 10 * 1024 * 1024  # 10 MiB


async def _flush_oversized_response(
    send: Send,
    message: Message,
    response_status: int,
    response_headers: list[tuple[bytes, bytes]],
    response_body: bytearray,
    pending: CachePending | None,
) -> None:
    """Flush already-buffered data and forward *message* on oversized responses.

    Called when the accumulated body exceeds ``MAX_CACHEABLE_BODY_SIZE``.
    Sends the buffered ``http.response.start``, any previously buffered
    body as a partial chunk, and then the current *message*.
    """
    logger.warning(
        "Response exceeds MAX_CACHEABLE_BODY_SIZE (%d bytes), "
        "skipping cache for key '%s'",
        MAX_CACHEABLE_BODY_SIZE,
        getattr(pending, "key", "?"),
    )
    await send(
        {
            "type": "http.response.start",
            "status": response_status,
            "headers": response_headers,
        }
    )
    if response_body:
        await send(
            {
                "type": "http.response.body",
                "body": bytes(response_body),
                "more_body": True,
            }
        )
        response_body.clear()
    await send(message)


async def _store_cache_entry(
    pending: CachePending,
    body_bytes: bytes,
    app: Any,
) -> list[tuple[bytes, bytes]]:
    """Write a cache entry to Redis and return extra response headers.

    Returns:
        A list of ``(name, value)`` header pairs to append to the
        outgoing response (``X-Redis-Cache``, ``ETag``, ``Cache-Control``).
    """
    etag = f'W/"{hashlib.blake2b(body_bytes, digest_size=16).hexdigest()}"'
    cc_value = _cache_control_value(pending.ttl, pending.private)
    extra_headers: list[tuple[bytes, bytes]] = [
        (CACHE_STATUS_HEADER.lower().encode(), b"MISS"),
        (b"etag", etag.encode()),
        (b"cache-control", cc_value.encode()),
    ]
    entry = {
        "body": body_bytes.decode(errors="replace"),
        "etag": etag,
    }
    try:
        redis = pending.redis
        if redis is None:
            redis = _get_pool_state(app).get_async_client()
        with cache_span(
            "cache.set",
            attributes={"cache.key": pending.key, "cache.ttl": pending.ttl},
        ):
            with timed_operation("set"):
                set_kwargs: dict[str, Any] = {}
                if pending.ttl > 0:
                    set_kwargs["ex"] = pending.ttl
                await redis.set(pending.key, json.dumps(entry), **set_kwargs)
        write_type = "write_through" if pending.write_through else "miss_fill"
        record_cache_write(write_type=write_type)
    except (RedisError, OSError):
        logger.warning(
            "Error writing cache key '%s':",
            pending.key,
            exc_info=True,
        )
    return extra_headers


class CacheResponseCaptureMiddleware:
    """ASGI middleware that intercepts responses and stores them in Redis.

    Transparent: only buffers when ``request.state.redis_cache_pending``
    has been set by a ``cache()`` or ``cache_put()`` dependency.
    Otherwise, messages pass through with zero overhead.

    Responses exceeding ``MAX_CACHEABLE_BODY_SIZE`` are passed through
    without caching to prevent unbounded memory usage.

    Registered automatically by :func:`add_redis_caching`.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope)
        response_body = bytearray()
        response_status = 200
        response_headers: list[tuple[bytes, bytes]] = []
        passthrough = False
        pending: CachePending | None = None

        # Flow: start → buffer body chunks → on final chunk: cache + send
        async def capture_send(message: Message) -> None:
            nonlocal response_body, response_status, response_headers
            nonlocal passthrough, pending

            # 1. Response start: decide whether to buffer or passthrough
            if message["type"] == "http.response.start":
                pending = getattr(request.state, "redis_cache_pending", None)
                if pending is None:
                    passthrough = True
                    await send(message)
                    return
                response_status = message["status"]
                response_headers = list(message.get("headers", []))
                return

            if message["type"] != "http.response.body":
                return

            # 2. No pending cache op — forward body as-is
            if passthrough:
                await send(message)
                return

            # 3. Guard against oversized responses
            chunk = message.get("body", b"")
            if len(response_body) + len(chunk) > MAX_CACHEABLE_BODY_SIZE:
                passthrough = True
                await _flush_oversized_response(
                    send,
                    message,
                    response_status,
                    response_headers,
                    response_body,
                    pending,
                )
                return

            # 4. Accumulate chunks until the final one
            response_body.extend(chunk)
            if message.get("more_body", False):
                return

            # 5. Final chunk: write to Redis (on 2xx) then send the full response
            body_bytes = bytes(response_body)
            extra_headers: list[tuple[bytes, bytes]] = []
            if pending is not None and 200 <= response_status < 300:
                extra_headers = await _store_cache_entry(
                    pending,
                    body_bytes,
                    request.app,
                )

            await send(
                {
                    "type": "http.response.start",
                    "status": response_status,
                    "headers": response_headers + extra_headers,
                }
            )
            await send({"type": "http.response.body", "body": body_bytes})

        await self.app(scope, receive, capture_send)


# ---------------------------------------------------------------------------
# add_redis_caching() - one-time app setup
# ---------------------------------------------------------------------------


def add_redis_caching(app: FastAPI) -> None:
    """Register the exception handler and capture middleware.

    Prefer the builder API instead of calling this directly::

        FastAPIRedis(app).lifespan().caching()

    Args:
        app: The FastAPI application instance.
    """
    app.add_exception_handler(CacheHitException, cache_hit_exception_handler)
    app.add_middleware(CacheResponseCaptureMiddleware)
