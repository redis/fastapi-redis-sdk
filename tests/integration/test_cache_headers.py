"""Integration tests for header fidelity across a cache hit, against real Redis.

``tests/integration/test_cache_payloads.py`` answers *whether* a response may
be stored.  This module answers what a stored response comes back **as**: which
header fields survive the round trip, which are deliberately withheld, and what
a client that depends on one of them actually receives on the second request.

Each test names the use case it protects.  They exist because every one of
these failed the same way before the header block was stored — correct on the
first request, quietly degraded on every request after it, with no error
anywhere to notice.
"""

from __future__ import annotations

import json
from collections.abc import Generator

import pytest
import redis as sync_redis
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.gzip import GZipMiddleware
from starlette.responses import Response

from redis_fastapi.cache import MAX_CACHEABLE_HEADER_SIZE, cache
from redis_fastapi.setup import FastAPIRedis
from tests.conftest import requires_redis

LAST_MODIFIED = "Wed, 10 Sep 2026 12:00:00 GMT"


@pytest.fixture()
def flushed(real_redis: sync_redis.Redis) -> Generator[sync_redis.Redis, None, None]:
    real_redis.flushdb()
    yield real_redis
    real_redis.flushdb()


def _app(headers: dict[str, str], *, body: str = '[{"id": 1}]') -> FastAPI:
    app = FastAPI()
    FastAPIRedis(app).lifespan().caching()

    @app.get("/items", dependencies=[Depends(cache(ttl=300))])
    async def items() -> Response:
        return Response(
            content=body, media_type="application/json", headers=dict(headers)
        )

    return app


# ===================================================================
# The use cases a four-field entry could not serve
# ===================================================================


@requires_redis
@pytest.mark.integration
@pytest.mark.parametrize(
    ("use_case", "field", "value"),
    [
        ("pagination by Link (RFC 8288)", "Link", '</items?page=2>; rel="next"'),
        ("total counts for a data grid", "X-Total-Count", "4211"),
        (
            "file download under a filename",
            "Content-Disposition",
            'attachment; filename="report.csv"',
        ),
        ("localized representation", "Content-Language", "de-DE"),
        ("date-based validation", "Last-Modified", LAST_MODIFIED),
        ("response integrity (RFC 9530)", "Content-Digest", "sha-256=:abc:"),
        ("message signature (RFC 9421)", "Signature-Input", 'sig1=("@status")'),
        ("representation URI", "Content-Location", "/items?page=1"),
        ("application-specific metadata", "X-Deprecation-Notice", "use /v2/items"),
    ],
)
def test_endpoint_header_survives_the_hit(
    use_case: str, field: str, value: str, flushed: sync_redis.Redis
) -> None:
    """A header the endpoint computed must reach the client on the hit too."""
    with TestClient(_app({field: value})) as client:
        miss = client.get("/items")
        hit = client.get("/items")

    assert miss.headers["X-Redis-Cache"] == "MISS"
    assert hit.headers["X-Redis-Cache"] == "HIT", use_case
    assert miss.headers[field] == value
    assert hit.headers.get(field) == value, use_case


@requires_redis
@pytest.mark.integration
def test_repeated_fields_keep_their_repetition_and_order(
    flushed: sync_redis.Redis,
) -> None:
    """Several ``Link`` headers is legal and meaningful; a mapping loses them."""
    app = FastAPI()
    FastAPIRedis(app).lifespan().caching()

    @app.get("/paged", dependencies=[Depends(cache(ttl=300))])
    async def paged() -> Response:
        response = Response(content="[]", media_type="application/json")
        response.headers.append("Link", '</paged?page=2>; rel="next"')
        response.headers.append("Link", '</paged?page=9>; rel="last"')
        return response

    with TestClient(app) as client:
        miss = client.get("/paged")
        hit = client.get("/paged")

    assert hit.headers["X-Redis-Cache"] == "HIT"
    assert miss.headers.get_list("link") == [
        '</paged?page=2>; rel="next"',
        '</paged?page=9>; rel="last"',
    ]
    assert hit.headers.get_list("link") == miss.headers.get_list("link")


# ===================================================================
# Conditional requests
# ===================================================================


@requires_redis
@pytest.mark.integration
@pytest.mark.parametrize(
    ("since", "expected", "why"),
    [
        (LAST_MODIFIED, 304, "same instant - the client's copy is current"),
        ("Thu, 11 Sep 2026 12:00:00 GMT", 304, "client's copy is newer"),
        ("Tue, 09 Sep 2026 12:00:00 GMT", 200, "client's copy predates the entry"),
    ],
)
def test_if_modified_since_against_a_stored_last_modified(
    since: str, expected: int, why: str, flushed: sync_redis.Redis
) -> None:
    with TestClient(_app({"Last-Modified": LAST_MODIFIED})) as client:
        client.get("/items")
        conditional = client.get("/items", headers={"If-Modified-Since": since})

    assert conditional.status_code == expected, why


@requires_redis
@pytest.mark.integration
def test_not_modified_carries_cache_metadata_only(flushed: sync_redis.Redis) -> None:
    """RFC 9110 section 15.4.5: a 304 describes no representation.

    The entry holds a Content-Type, a Link and a Content-Language.  Replaying
    the whole block would describe a body this response does not contain.
    """
    app = _app(
        {
            "Link": '</items?page=2>; rel="next"',
            "Content-Language": "de-DE",
            "Vary": "Accept-Language",
            "Content-Location": "/items?page=1",
        }
    )
    with TestClient(app) as client:
        miss = client.get("/items")
        validated = client.get(
            "/items", headers={"If-None-Match": miss.headers["etag"]}
        )

    assert validated.status_code == 304
    assert validated.content == b""
    # Carried: validator, cache metadata, Vary, Content-Location.
    assert validated.headers["etag"] == miss.headers["etag"]
    assert validated.headers["vary"] == "Accept-Language"
    assert validated.headers["content-location"] == "/items?page=1"
    assert "cache-control" in validated.headers
    # Withheld: anything describing the absent body.
    assert validated.headers.get("content-type") is None
    assert validated.headers.get("content-language") is None
    assert validated.headers.get("link") is None


# ===================================================================
# Fields an entry deliberately withholds
# ===================================================================


@requires_redis
@pytest.mark.integration
def test_endpoint_cookies_are_never_stored_or_replayed(
    flushed: sync_redis.Redis,
) -> None:
    """A shared entry must not hand one caller's session to the next.

    The miss sets the cookie because that response is the endpoint's own.  The
    hit does not, which is why a cached route cannot establish a session.
    """
    with TestClient(_app({"Set-Cookie": "session=abc123; Path=/"})) as client:
        miss = client.get("/items")
        hit = client.get("/items")

    assert miss.headers.get("set-cookie") == "session=abc123; Path=/"
    assert hit.headers.get("set-cookie") is None

    entry = flushed.get(flushed.keys("*")[0])
    assert "abc123" not in entry, "no cookie value may reach Redis"


@requires_redis
@pytest.mark.integration
def test_middleware_cookies_still_reach_every_caller(
    flushed: sync_redis.Redis,
) -> None:
    """The documented remedy for the cookie limitation.

    Middleware runs outside the cache, so it is applied to the hit as well.
    A cookie set there is not stored and not shared - it is recomputed per
    response, which is exactly what a session cookie needs.
    """
    app = FastAPI()

    class IssueCookie(BaseHTTPMiddleware):
        async def dispatch(self, request, call_next):  # type: ignore[no-untyped-def]
            response = await call_next(request)
            response.set_cookie("session", f"per-caller-{request.url.path}")
            return response

    app.add_middleware(IssueCookie)
    FastAPIRedis(app).lifespan().caching()

    @app.get("/items", dependencies=[Depends(cache(ttl=300))])
    async def items() -> dict:
        return {"ok": True}

    with TestClient(app) as client:
        miss = client.get("/items")
        hit = client.get("/items")

    assert hit.headers["X-Redis-Cache"] == "HIT"
    # Starlette quotes a value containing a slash; compare the two responses
    # rather than a literal.
    assert "per-caller-/items" in miss.headers["set-cookie"]
    assert hit.headers["set-cookie"] == miss.headers["set-cookie"]


@requires_redis
@pytest.mark.integration
def test_date_is_never_stored_or_replayed(flushed: sync_redis.Redis) -> None:
    """Replaying a stored Date would make a downstream cache count age twice.

    In production the ASGI server stamps a fresh ``Date`` on every response,
    including a hit; ``TestClient`` does not, which is why this asserts the
    exclusion rather than the stamping. An endpoint that sets ``Date`` by hand
    has it on its own response and never on a response served from the entry.
    """
    stale = "Mon, 01 Jan 2024 00:00:00 GMT"
    with TestClient(_app({"Date": stale})) as client:
        miss = client.get("/items")
        hit = client.get("/items")

    assert hit.headers["X-Redis-Cache"] == "HIT"
    assert miss.headers.get("date") == stale
    assert hit.headers.get("date") is None
    assert stale not in flushed.get(flushed.keys("*")[0])


@requires_redis
@pytest.mark.integration
def test_connection_specific_fields_are_stripped(flushed: sync_redis.Redis) -> None:
    """RFC 9110 section 7.6.1 / RFC 9111 section 3.1 exceptions."""
    app = _app(
        {
            "Connection": "X-Hop-Only",
            "X-Hop-Only": "internal",
            "Keep-Alive": "timeout=5",
            "Proxy-Authenticate": "Basic",
            "X-Kept": "public",
        }
    )
    with TestClient(app) as client:
        client.get("/items")
        hit = client.get("/items")

    entry = flushed.get(flushed.keys("*")[0]).lower()
    for stripped in ("x-hop-only", "keep-alive", "proxy-authenticate"):
        assert stripped not in entry, stripped
    assert "x-kept" in entry
    assert hit.headers.get("x-kept") == "public"


@requires_redis
@pytest.mark.integration
def test_oversized_header_block_is_served_but_not_stored(
    flushed: sync_redis.Redis,
) -> None:
    """Metadata is capped the way bodies are, and refused the same way."""
    app = _app({"X-Huge": "v" * (MAX_CACHEABLE_HEADER_SIZE + 1)})
    with TestClient(app) as client:
        first = client.get("/items")
        second = client.get("/items")

    assert first.headers["X-Redis-Cache"] == "BYPASS"
    assert second.headers["X-Redis-Cache"] == "BYPASS"
    assert first.status_code == second.status_code == 200
    assert first.json() == [{"id": 1}], "the response is delivered whole"
    assert flushed.keys("*") == []


# ===================================================================
# Middleware ordering
# ===================================================================


@requires_redis
@pytest.mark.integration
def test_compression_registered_after_caching_caches_normally(
    flushed: sync_redis.Redis,
) -> None:
    """Starlette applies the last-registered middleware outermost.

    Registered after ``caching()``, compression wraps the cache: the entry
    holds the identity representation and both the miss and the hit are
    compressed on the way out.
    """
    app = FastAPI()
    FastAPIRedis(app).lifespan().caching()
    app.add_middleware(GZipMiddleware, minimum_size=1)

    @app.get("/items", dependencies=[Depends(cache(ttl=300))])
    async def items() -> dict:
        return {"ok": True}

    with TestClient(app) as client:
        miss = client.get("/items", headers={"Accept-Encoding": "gzip"})
        hit = client.get("/items", headers={"Accept-Encoding": "gzip"})

    assert miss.headers["X-Redis-Cache"] == "MISS"
    assert hit.headers["X-Redis-Cache"] == "HIT"
    assert hit.headers.get("content-encoding") == "gzip"
    assert hit.json() == {"ok": True}
    assert len(flushed.keys("*")) == 1


@requires_redis
@pytest.mark.integration
def test_compression_registered_before_caching_disables_the_cache(
    flushed: sync_redis.Redis,
) -> None:
    """The trap this ordering causes, pinned so the guide cannot go stale.

    Registered before ``caching()``, compression sits *inside* the cache, so
    the middleware sees gzip bytes with a Content-Encoding it refuses.  Every
    response is a BYPASS for every client that accepts gzip - which is every
    browser.
    """
    app = FastAPI()
    app.add_middleware(GZipMiddleware, minimum_size=1)
    FastAPIRedis(app).lifespan().caching()

    @app.get("/items", dependencies=[Depends(cache(ttl=300))])
    async def items() -> dict:
        return {"ok": True}

    with TestClient(app) as client:
        first = client.get("/items", headers={"Accept-Encoding": "gzip"})
        second = client.get("/items", headers={"Accept-Encoding": "gzip"})

    assert first.headers["X-Redis-Cache"] == "BYPASS"
    assert second.headers["X-Redis-Cache"] == "BYPASS"
    assert second.json() == {"ok": True}, "the response is still correct"
    assert flushed.keys("*") == [], "nothing was cached"


@requires_redis
@pytest.mark.integration
def test_entry_without_a_header_block_degrades_to_a_miss(
    flushed: sync_redis.Redis,
) -> None:
    """A malformed entry must not be half-read.

    With no format marker in the value, a missing header block is the only
    signal that an entry cannot be replayed - so it has to produce a miss
    rather than an exception or a mislabelled body.
    """
    calls = [0]
    app = FastAPI()
    FastAPIRedis(app).lifespan().caching()

    @app.get("/items", dependencies=[Depends(cache(ttl=300))])
    async def items() -> dict:
        calls[0] += 1
        return {"from": "endpoint"}

    with TestClient(app) as client:
        client.get("/items")
        key = flushed.keys("*")[0]
        flushed.set(key, json.dumps({"body": '{"from":"broken"}', "etag": 'W/"b"'}))

        resp = client.get("/items")

    assert resp.status_code == 200
    assert resp.headers["X-Redis-Cache"] == "MISS"
    assert resp.json() == {"from": "endpoint"}
    assert calls[0] == 2, "the endpoint ran again instead of serving a bad entry"


@requires_redis
@pytest.mark.integration
async def test_backend_delete_group_still_clears_decorator_entries(
    flushed: sync_redis.Redis,
) -> None:
    """``CacheBackend`` and ``cache()`` share one prefix.

    ``delete_group()`` therefore clears both - the guarantee stated under
    "Keys interoperate with cache()" in the caching guide.
    """
    import redis.asyncio as aioredis

    from redis_fastapi.cache_backend import CacheBackend

    app = FastAPI()
    FastAPIRedis(app).lifespan().caching()

    @app.get("/grouped", dependencies=[Depends(cache(ttl=300, eviction_group="items"))])
    async def grouped() -> dict:
        return {"ok": True}

    with TestClient(app) as client:
        assert client.get("/grouped").headers["X-Redis-Cache"] == "MISS"
        assert client.get("/grouped").headers["X-Redis-Cache"] == "HIT"
        assert len(flushed.keys("*")) == 1

        aclient = aioredis.Redis()
        try:
            removed = await CacheBackend(aclient, eviction_group="items").delete_group()
        finally:
            await aclient.aclose()

        assert removed == 1, "the decorator's entry was in the group"
        assert flushed.keys("*") == []
        assert client.get("/grouped").headers["X-Redis-Cache"] == "MISS"
