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
from email.utils import parsedate_to_datetime
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

# Set when a cache()/cache_put() dependency is built without an explicit TTL
# and therefore falls back to ``settings.default_ttl``.  The lifespan
# eviction-safety check reads this so it stays quiet for apps that set a TTL
# on every route.
_default_ttl_in_use = False


def relies_on_default_ttl() -> bool:
    """Return ``True`` if any cache dependency falls back to ``default_ttl``."""
    return _default_ttl_in_use


# ---------------------------------------------------------------------------
# Key builder
# ---------------------------------------------------------------------------


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
        qs = ":".join(f"{k}={v}" for k, v in sorted(request.query_params.items()))
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
# Cache scope - which representations may be stored
# ---------------------------------------------------------------------------


# A cache entry is a JSON document carrying the body as a text field, so a
# stored representation has to survive a UTF-8 round trip. Refer to the
# architecture section of the guide for details
CACHEABLE_MEDIA_TYPES: frozenset[str] = frozenset(
    {
        "application/json",
        "application/xml",
        "application/javascript",
    }
)

# Structured-syntax suffixes (RFC 6838 section 4.2.8) that are text by definition.
CACHEABLE_MEDIA_SUFFIXES: tuple[str, ...] = ("+json", "+xml")

# Character sets a stored body may declare.  Anything else - latin-1, say -
# would decode to bytes other than the ones that were written.
CACHEABLE_CHARSETS: frozenset[str] = frozenset({"utf-8", "utf8", "us-ascii", "ascii"})

# ---------------------------------------------------------------------------
# Entry format - which header fields an entry carries
# ---------------------------------------------------------------------------

# Header field bytes are latin-1, not UTF-8.  RFC 9110 section 5.5 constrains
# field values to US-ASCII and tells a recipient to "treat other allowed octets
# in field content (i.e., obs-text) as opaque data", and latin-1 is the codec
# that keeps that promise: it maps 0x00-0xFF one-to-one onto U+0000-U+00FF, so
# any header byte survives a decode/encode round trip unchanged.  Starlette
# encodes and decodes raw headers the same way, so entries read back byte-for-
# byte identical to what the endpoint sent.
# https://www.rfc-editor.org/rfc/rfc9110.html#section-5.5
HEADER_ENCODING: str = "latin-1"

# RFC 9111 section 3.1 requires a cache to store every received response
# header field, including unrecognized ones, so that new fields keep working
# without the cache having to learn them.  That is why this is a denylist: an
# entry stores whatever the endpoint sent apart from the groups below, each of
# which is either re-emitted from scratch or must not be replayed at all.

# Re-emitted on every hit from the entry's own TTL and validator.  Storing
# them would put a second copy beside the live one.
OWNED_HEADERS: frozenset[bytes] = frozenset(
    {b"cache-control", b"etag", CACHE_STATUS_HEADER.lower().encode()}
)

# RFC 9110 section 7.6.1: connection-specific fields, which a recipient must
# remove before forwarding.  ``Connection`` also names further fields, read
# per response in :func:`_excluded_from_storage`.
HOP_BY_HOP_HEADERS: frozenset[bytes] = frozenset(
    {
        b"connection",
        b"keep-alive",
        b"proxy-connection",
        b"te",
        b"transfer-encoding",
        b"upgrade",
    }
)

# RFC 9111 section 3.1: proxy-specific fields MUST NOT be stored unless the
# cache puts the proxy's identity in the key, which this one does not.
PROXY_HEADERS: frozenset[bytes] = frozenset(
    {b"proxy-authenticate", b"proxy-authentication-info", b"proxy-authorization"}
)

# Framing, recomputed from the replayed body.  A stored length that no longer
# matches would corrupt the response.
FRAMING_HEADERS: frozenset[bytes] = frozenset({b"content-length"})

# Excluded by decision rather than by specification:
#
# * ``Date`` - a hit is stamped fresh by the server while ``max-age`` counts
#   down.  Replaying a stored ``Date`` makes a downstream cache derive the
#   age twice and treat every hit as stale on arrival; see the "Do not add an
#   Age header on its own" warning in the caching guide.
# * ``Set-Cookie`` - an entry is shared.  Replaying one caller's cookie to the
#   next caller would be a session leak, so a cached route sets no cookies.
POLICY_EXCLUDED_HEADERS: frozenset[bytes] = frozenset({b"date", b"set-cookie"})

# Ceiling on the serialised header block.  A route with heavy metadata can
# otherwise store more bytes of headers than of body, and the block is
# rebuilt into a response on every hit.
MAX_CACHEABLE_HEADER_SIZE: int = 8 * 1024

# RFC 9110 section 15.4.5: a 304 carries validators and cache metadata, not
# the representation metadata a 200 would.  Replaying the whole stored block
# on a 304 would send a Content-Type for a response that has no content.
NOT_MODIFIED_HEADERS: frozenset[bytes] = frozenset(
    {b"etag", b"cache-control", b"vary", b"content-location", b"expires"}
)

# ``(route, reason)`` pairs already logged, so a refused route warns once
# rather than on every request.
_WARNED_REFUSALS: set[tuple[str, str]] = set()


def _find_header(headers: list[tuple[bytes, bytes]], name: bytes) -> bytes | None:
    """Return the first value for raw ASGI header *name* (lowercase), if present.

    Args:
        headers: Raw ASGI header pairs from ``http.response.start``.
        name: Lowercase header name to look for.

    Returns:
        The raw header value, or ``None`` when the header is absent.
    """
    for key, value in headers:
        if key.lower() == name:
            return value
    return None


def _split_content_type(raw: bytes | None) -> tuple[str, str | None]:
    """Split a ``Content-Type`` value into its media type and charset.

    Both are lowercased.  A missing header yields ``("", None)``, which no
    allowlist entry matches, so an untyped response is refused rather than
    guessed at.

    Args:
        raw: Raw ``Content-Type`` header value, or ``None``.

    Returns:
        A ``(media_type, charset)`` pair; *charset* is ``None`` when the
        header declares none.
    """
    if raw is None:
        return "", None
    media_type, _, params = raw.decode(HEADER_ENCODING).partition(";")
    charset: str | None = None
    for param in params.split(";"):
        key, _, value = param.partition("=")
        if key.strip().lower() == "charset":
            charset = value.strip().strip('"').lower()
            break
    return media_type.strip().lower(), charset


def _is_cacheable_media_type(media_type: str) -> bool:
    """Whether *media_type* names a representation this cache can replay.

    Args:
        media_type: Lowercased media type, without parameters.

    Returns:
        ``True`` for text and the JSON/XML families, ``False`` otherwise.
    """
    if media_type in CACHEABLE_MEDIA_TYPES:
        return True
    if media_type.startswith("text/"):
        return True
    return media_type.startswith("application/") and media_type.endswith(
        CACHEABLE_MEDIA_SUFFIXES
    )


def _merge_headers(
    base: list[tuple[bytes, bytes]],
    overrides: list[tuple[bytes, bytes]],
) -> list[tuple[bytes, bytes]]:
    """Return *base* with every field named in *overrides* replaced, not joined.

    The fields this middleware sets are single-valued.  RFC 9110 section 8.8.3
    defines ``ETag = entity-tag`` - one tag, not a list - so appending ours
    beside a validator the endpoint already set would emit a field no client
    can parse, and a conditional request echoing it would never match.
    ``Cache-Control`` appended the same way yields two ``max-age`` directives.

    Args:
        base: Raw ASGI headers as the endpoint produced them.
        overrides: Headers this middleware owns; each replaces every earlier
            occurrence of the same name.

    Returns:
        The merged header list, with *overrides* last.
    """
    owned = {name.lower() for name, _ in overrides}
    return [(k, v) for k, v in base if k.lower() not in owned] + overrides


def _excluded_from_storage(response_headers: list[tuple[bytes, bytes]]) -> set[bytes]:
    """Return the lowercase field names this entry must not store.

    The fixed groups are joined by whatever ``Connection`` names in this
    particular response, which RFC 9110 section 7.6.1 makes connection
    specific for that message only.

    Args:
        response_headers: Raw ASGI response headers.

    Returns:
        Lowercase header names to leave out of the entry.
    """
    excluded = set(
        OWNED_HEADERS
        | HOP_BY_HOP_HEADERS
        | PROXY_HEADERS
        | FRAMING_HEADERS
        | POLICY_EXCLUDED_HEADERS
    )
    for name, value in response_headers:
        if name.lower() == b"connection":
            excluded.update(
                token.strip().lower() for token in value.split(b",") if token.strip()
            )
    return excluded


def _storable_headers(
    response_headers: list[tuple[bytes, bytes]],
) -> list[list[str]]:
    """Return the header fields to store, in the order they were received.

    Pairs rather than a mapping: a response may carry several ``Link`` or
    ``Set-Cookie`` fields, and both their repetition and their order are part
    of what it means.

    Args:
        response_headers: Raw ASGI response headers.

    Returns:
        ``[name, value]`` pairs, lowercased names, JSON-serialisable.
    """
    excluded = _excluded_from_storage(response_headers)
    return [
        [name.decode(HEADER_ENCODING).lower(), value.decode(HEADER_ENCODING)]
        for name, value in response_headers
        if name.lower() not in excluded
    ]


def _entry_headers(entry: dict[str, Any]) -> list[tuple[str, str]]:
    """Return an entry's stored header pairs, in the order they were received.

    Args:
        entry: The decoded cache entry.

    Returns:
        ``(name, value)`` pairs.

    Raises:
        KeyError: If the entry carries no header block.  The caller treats
            that as a miss rather than serving a shape it cannot read.
    """
    return [(name, value) for name, value in entry["headers"]]


def _is_not_modified(
    request: Request, etag: str, stored: list[tuple[str, str]]
) -> bool:
    """Whether this conditional request can be answered with a ``304``.

    ``If-None-Match`` decides on its own when present: RFC 9110
    section 13.2.2 requires a recipient to ignore ``If-Modified-Since`` when
    the request carries an entity-tag precondition.  ``If-Modified-Since`` is
    evaluated only against a ``Last-Modified`` the entry actually stored.

    Args:
        request: The incoming request.
        etag: The entry's stored validator.
        stored: The entry's stored header pairs.

    Returns:
        ``True`` when the client's copy is still current.
    """
    if_none_match = request.headers.get("if-none-match")
    if if_none_match is not None:
        return if_none_match == etag

    since = request.headers.get("if-modified-since")
    if since is None:
        return False
    last_modified = next(
        (value for name, value in stored if name == "last-modified"), None
    )
    if last_modified is None:
        return False
    try:
        return parsedate_to_datetime(last_modified) <= parsedate_to_datetime(since)
    except (TypeError, ValueError):
        # RFC 9110 section 13.1.3: a date the recipient cannot parse is not a
        # precondition, so fall through and send the body.
        return False


def _hit_headers(
    stored: list[tuple[str, str]],
    etag: str,
    cc_value: str,
    *,
    not_modified: bool,
) -> list[tuple[bytes, bytes]]:
    """Build the raw header list for a hit, preserving repeated fields.

    Args:
        stored: Header pairs from the entry.
        etag: The stored validator.
        cc_value: ``Cache-Control`` for the entry's remaining TTL.
        not_modified: Whether this is a ``304``, which carries only the
            fields in :data:`NOT_MODIFIED_HEADERS`.

    Returns:
        Raw ASGI header pairs, with the fields this library owns last.
    """
    pairs = [
        (name.encode(HEADER_ENCODING), value.encode(HEADER_ENCODING))
        for name, value in stored
        if not not_modified or name.encode(HEADER_ENCODING) in NOT_MODIFIED_HEADERS
    ]
    return _merge_headers(
        pairs,
        [
            (CACHE_STATUS_HEADER.lower().encode(), b"HIT"),
            (b"etag", etag.encode(HEADER_ENCODING)),
            (b"cache-control", cc_value.encode(HEADER_ENCODING)),
        ],
    )


def _route_label(request: Request) -> str:
    """Return a stable, log-safe name for the route that served *request*."""
    route = request.scope.get("route")
    return str(getattr(route, "path", None) or request.url.path)


def _warn_refusal_once(request: Request, reason: str) -> None:
    """Log why a response was not stored, once per route and reason.

    Args:
        request: The request whose response was refused.
        reason: The refusal reason from :func:`_storage_refusal`.
    """
    marker = (_route_label(request), reason)
    if marker in _WARNED_REFUSALS:
        return
    _WARNED_REFUSALS.add(marker)
    logger.warning(
        "%s was served but not cached: %s.  cache() stores serializable text "
        "representations only; see 'Cache scope' in the architecture guide.",
        marker[0],
        reason,
    )


def _storage_refusal(
    request: Request,
    pending: CachePending,
    response_status: int,
    response_headers: list[tuple[bytes, bytes]],
) -> str | None:
    """Return why this response must not be stored, or ``None`` to store it.

    Every branch is a rule from RFC 9111 or a limit of the entry format, and
    "Cache scope" in the architecture guide documents them one for one, so a
    refusal in the log can be looked up.

    Args:
        request: The request being served.
        pending: The pending cache operation, which says whether this is a
            read-path fill or a write-through.
        response_status: Status code from ``http.response.start``.
        response_headers: Raw ASGI response headers.

    Returns:
        A short reason string, or ``None`` when the response may be stored.
    """
    # RFC 9111 section 3: store only a status code the cache can replay.  The
    # entry holds no status, so the hit path always answers 200 - which makes
    # 200 the only status a read-path fill may store.  206 is the one that
    # bites: the key carries no Range, so a stored partial body would be
    # replayed to the next client as a complete 200.
    #
    # Write-through is the exception.  There the stored body is deliberately
    # installed as the representation for a *later GET*, so the status of the
    # PUT or POST that produced it never reaches a client; any 2xx will do.
    if pending.write_through:
        if not 200 <= response_status < 300:
            return f"status {response_status} is not 2xx"
    elif response_status != 200:
        return f"status {response_status} is not 200"

    # RFC 9111 section 3.3: a cache that implements neither Range nor
    # Content-Range MUST NOT store partial content, and MUST NOT answer a
    # request from a partial entry.
    if _find_header(response_headers, b"content-range") is not None:
        return "response carries Content-Range"
    if "range" in request.headers:
        return "request carried Range"

    # RFC 9111 sections 3 and 5.2.2.7: a Redis entry is a shared cache, so the
    # response's own no-store and private directives bind us.  Every
    # Cache-Control line is read, not just the first: a directive that forbids
    # storage must not be missed because something appended a second header.
    cc_lines = [
        value.decode(HEADER_ENCODING)
        for key, value in response_headers
        if key.lower() == b"cache-control"
    ]
    if cc_lines:
        cc = _parse_cache_control(",".join(cc_lines))
        if "no-store" in cc:
            return "response set Cache-Control: no-store"
        if "private" in cc:
            return "response set Cache-Control: private"

    media_type, charset = _split_content_type(
        _find_header(response_headers, b"content-type")
    )
    if not _is_cacheable_media_type(media_type):
        return f"content type '{media_type or 'none'}' is not a text representation"
    if charset is not None and charset not in CACHEABLE_CHARSETS:
        return f"charset '{charset}' is not UTF-8"

    # RFC 9111 section 4.1: a stored response whose Vary is "*" may never be
    # reused for a later request, so storing one only builds entries that must
    # not be served.  The lookup ignores Vary, which makes refusing the store
    # the only place this can be honoured.
    raw_vary = _find_header(response_headers, b"vary")
    if raw_vary is not None and "*" in [
        v.strip() for v in raw_vary.decode(HEADER_ENCODING).split(",")
    ]:
        return "response set Vary: *"

    # RFC 9110 section 8.4: Content-Encoding states what decoding has to be
    # applied to obtain the data in the media type Content-Type names.  An
    # encoded body is refused by name rather than left to fail the UTF-8
    # decode later, which would report a compressed response as a bytes
    # problem and tell the operator the wrong thing.
    raw_encoding = _find_header(response_headers, b"content-encoding")
    if raw_encoding is not None:
        codings = [
            c.strip().lower() for c in raw_encoding.decode(HEADER_ENCODING).split(",")
        ]
        applied = [c for c in codings if c and c != "identity"]
        if applied:
            return f"response carries Content-Encoding: {', '.join(applied)}"

    block_size = len(json.dumps(_storable_headers(response_headers)))
    if block_size > MAX_CACHEABLE_HEADER_SIZE:
        return (
            f"header block is {block_size} bytes, over the "
            f"{MAX_CACHEABLE_HEADER_SIZE}-byte limit"
        )
    return None


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

    The response is rebuilt from the header fields the entry stored, so a
    hit carries the representation metadata the endpoint set rather than a
    reconstruction of it.  Returns a ``304 Not Modified`` when the client's
    ``If-None-Match`` matches the stored ETag, or when its
    ``If-Modified-Since`` is not older than a stored ``Last-Modified``.

    Raises:
        json.JSONDecodeError: If *cached_data* is not valid JSON.
        KeyError: If the entry is missing required keys, or was written by a
            version this reader does not know.
    """
    entry = json.loads(cached_data)
    body_bytes = (
        entry["body"].encode() if isinstance(entry["body"], str) else entry["body"]
    )
    etag: str = entry["etag"]
    stored = _entry_headers(entry)
    cc_value = _cache_control_value(remaining_ttl, private)

    if _is_not_modified(request, etag, stored):
        not_modified = Response(status_code=HTTP_304_NOT_MODIFIED)
        not_modified.raw_headers = _hit_headers(
            stored, etag, cc_value, not_modified=True
        )
        return not_modified

    # raw_headers is assigned rather than passed as a mapping because a
    # mapping cannot express a repeated field, and Link, Set-Cookie and Vary
    # may all legitimately appear more than once.
    response = Response(content=body_bytes)
    response.raw_headers = _hit_headers(stored, etag, cc_value, not_modified=False) + [
        (b"content-length", str(len(body_bytes)).encode(HEADER_ENCODING))
    ]
    return response


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
    if ttl is None:
        global _default_ttl_in_use
        _default_ttl_in_use = True
    _ttl: int = ttl if ttl is not None else _settings.default_ttl
    _prefix: str = (
        _settings.pattern_prefix("cache") if cache_prefix is None else cache_prefix
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
                try:
                    hit_response = _build_hit_response(
                        cached_data, remaining_ttl, request, private
                    )
                except Exception as exc:
                    logger.warning("Invalid cache entry for key %s: %s", cache_key, exc)
                else:
                    record_cache_request(result="hit", eviction_group=eviction_group)
                    if span is not None:
                        span.set_attribute("cache.hit", True)
                    raise CacheHitException(hit_response)

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
    from redis_fastapi.cache_backend import CacheBackend

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
    _prefix: str = _settings.pattern_prefix("cache") if prefix is None else prefix
    _key_builder: KeyBuilder | None = key_builder

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
    if ttl is None:
        global _default_ttl_in_use
        _default_ttl_in_use = True
    _ttl: int = ttl if ttl is not None else _settings.default_ttl
    _prefix: str = _settings.pattern_prefix("cache") if prefix is None else prefix
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
            "headers": _merge_headers(
                response_headers,
                [(CACHE_STATUS_HEADER.lower().encode(), b"BYPASS")],
            ),
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
    body_text: str,
    response_headers: list[tuple[bytes, bytes]],
    app: Any,
) -> list[tuple[bytes, bytes]]:
    """Write a cache entry to Redis and return the headers this cache owns.

    The entry carries the body, the validator, and every header field the
    endpoint sent apart from the groups named in :func:`_excluded_from_storage`
    - the fields this library re-emits, the connection-specific and proxy
    fields RFC 9111 section 3.1 excludes, the recomputed framing, and ``Date``
    and ``Set-Cookie`` by decision.  It carries no format marker: the keyspace
    it is written to is the marker.

    Args:
        pending: The pending cache operation set by the dependency.
        body_bytes: The complete response body, used for the ETag.
        body_text: The same body decoded as UTF-8, stored in the entry.
        response_headers: Raw ASGI headers as the endpoint produced them,
            read for the fields the entry preserves.
        app: The FastAPI application, used to recover a client if the
            dependency did not carry one.

    Returns:
        A list of ``(name, value)`` header pairs that replace any the
        endpoint set (``X-Redis-Cache``, ``ETag``, ``Cache-Control``).
    """
    # An endpoint that set its own validator keeps it.  Replaying the origin's
    # tag preserves a strong validator, which a hash of the body cannot be,
    # and it keeps the ETag a client sees on the miss identical to the one it
    # gets on the hit - otherwise the first conditional request after a miss
    # can never match.
    raw_etag = _find_header(response_headers, b"etag")
    etag = (
        raw_etag.decode(HEADER_ENCODING)
        if raw_etag is not None
        else f'W/"{hashlib.blake2b(body_bytes, digest_size=16).hexdigest()}"'
    )

    cc_value = _cache_control_value(pending.ttl, pending.private)
    extra_headers: list[tuple[bytes, bytes]] = [
        (CACHE_STATUS_HEADER.lower().encode(), b"MISS"),
        (b"etag", etag.encode()),
        (b"cache-control", cc_value.encode()),
    ]
    entry: dict[str, Any] = {
        "body": body_text,
        "etag": etag,
        "headers": _storable_headers(response_headers),
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

            # Any other message type (pathsend, zerocopysend, trailers, debug, …):
            # we can't buffer/cache it, so flush the buffered start and pass it
            # through unmarked: nothing was stored, so it must not claim a MISS,
            # and tests/integration/test_cache_extension_messages.py pins the
            # absence of a cache status header here.  This is the one refusal
            # with no BYPASS marker - see "Extension responses" in the guide.
            # Trailer fields reach the client but never the entry, which is what
            # RFC 9111 section 3.1 permits a cache to do with them.
            if message["type"] != "http.response.body":
                if not passthrough:
                    await send(
                        {
                            "type": "http.response.start",
                            "status": response_status,
                            "headers": response_headers,
                        }
                    )
                    passthrough = True
                await send(message)
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

            # 5. Final chunk: store the response when the rules allow, then
            #    send it either way.  A refused response is served normally and
            #    marked BYPASS, so nothing is ever withheld over a refusal.
            body_bytes = bytes(response_body)
            extra_headers: list[tuple[bytes, bytes]] = []
            if pending is not None:
                reason = _storage_refusal(
                    request, pending, response_status, response_headers
                )
                if reason is None:
                    try:
                        body_text = body_bytes.decode()
                    except UnicodeDecodeError:
                        # The allowlist admitted the media type, but these
                        # bytes are not the UTF-8 it promised.  Refuse rather
                        # than store a body that cannot survive the round trip.
                        reason = "body is not valid UTF-8"
                    else:
                        extra_headers = await _store_cache_entry(
                            pending,
                            body_bytes,
                            body_text,
                            response_headers,
                            request.app,
                        )
                if reason is not None:
                    _warn_refusal_once(request, reason)
                    extra_headers = [(CACHE_STATUS_HEADER.lower().encode(), b"BYPASS")]

            await send(
                {
                    "type": "http.response.start",
                    "status": response_status,
                    "headers": _merge_headers(response_headers, extra_headers),
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
    app.state._redis_caching = True
    app.add_exception_handler(CacheHitException, cache_hit_exception_handler)
    app.add_middleware(CacheResponseCaptureMiddleware)
