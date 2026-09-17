"""Integration tests for which payloads ``cache()`` stores, and which it refuses.

One table drives the whole matrix.  Every row is a complete request/response
shape - media type, charset, body bytes, response headers, request headers,
status and response class - paired with the one thing that matters: whether
the entry may be stored.

Both outcomes are asserted end to end against real Redis:

* **Stored** - the second request is a ``HIT``, the endpoint ran once, and the
  body *and* content type come back exactly as they went in.
* **Refused** - both requests are a ``BYPASS``, the endpoint ran twice, Redis
  holds nothing, and the client still receives the untouched response.

The second half is the point.  A refusal must never change what the caller
gets; it only means the library stored nothing.  See "Cache scope" in
``docs/guide/architecture.md`` and "RFC 9111 conformance" in
``docs/guide/caching.md``.
"""

from __future__ import annotations

import gzip
import json
from collections.abc import Generator
from dataclasses import dataclass, field
from pathlib import Path

import pytest
import redis as sync_redis
from fastapi import Depends, FastAPI, Request
from fastapi.testclient import TestClient
from starlette.responses import FileResponse, Response, StreamingResponse

from redis_fastapi.cache import cache
from redis_fastapi.setup import FastAPIRedis
from tests.conftest import requires_redis

# ---------------------------------------------------------------------------
# The matrix
# ---------------------------------------------------------------------------

JSON_BODY = b'{"value": 1}'
UNICODE_BODY = '{"text": "日本語 \U0001f389 مرحبا"}'.encode()
PNG_BODY = b"\x89PNG\r\n\x1a\n\x00\x01\x02\xff\xfe"
LATIN1_BODY = "caf\xe9".encode("latin-1")


@dataclass(frozen=True)
class Case:
    """One payload shape and whether the cache may store it."""

    name: str
    body: bytes
    media_type: str | None
    stored: bool
    why: str
    status: int = 200
    shape: str = "response"  # response | streaming | file
    extra_headers: dict[str, str] = field(default_factory=dict)
    request_headers: dict[str, str] = field(default_factory=dict)


CASES: list[Case] = [
    # -- text representations: stored ------------------------------------
    Case("json", JSON_BODY, "application/json", True, "the default case"),
    Case("json-unicode", UNICODE_BODY, "application/json", True, "non-ASCII UTF-8"),
    Case("json-empty", b"", "application/json", True, "empty body is still a body"),
    Case(
        "problem-json",
        b'{"title": "x"}',
        "application/problem+json",
        True,
        "+json suffix",
    ),
    Case("ld-json", b'{"@id": "x"}', "application/ld+json", True, "+json suffix"),
    Case("xml", b"<a>1</a>", "application/xml", True, "allowlisted"),
    Case("atom-xml", b"<feed/>", "application/atom+xml", True, "+xml suffix"),
    Case("javascript", b"var a = 1;", "application/javascript", True, "allowlisted"),
    Case("text-plain", b"hello", "text/plain", True, "text/* family"),
    Case("text-html", b"<p>hi</p>", "text/html", True, "text/* family"),
    Case("text-csv", b"a,b\n1,2\n", "text/csv", True, "text/* family"),
    Case("charset-utf8", b"hello", "text/plain; charset=utf-8", True, "declared UTF-8"),
    Case(
        "charset-ascii",
        b"hello",
        "text/plain; charset=us-ascii",
        True,
        "ASCII is UTF-8",
    ),
    Case(
        "streaming-text", b"chunk1chunk2", "text/plain", True, "buffered, then stored"
    ),
    Case("file-text", b"file contents\n", "text/plain", True, "a file of text is text"),
    Case(
        "cache-control-public",
        JSON_BODY,
        "application/json",
        True,
        "public does not forbid storage",
        extra_headers={"Cache-Control": "public, max-age=60"},
    ),
    # -- binary: refused -------------------------------------------------
    Case("octet-stream", PNG_BODY, "application/octet-stream", False, "binary"),
    Case("image-png", PNG_BODY, "image/png", False, "binary"),
    Case("application-pdf", b"%PDF-1.4\x00\xff", "application/pdf", False, "binary"),
    Case("msgpack", b"\x82\xa1a\x01", "application/msgpack", False, "binary"),
    Case("protobuf", b"\x08\x96\x01", "application/x-protobuf", False, "binary"),
    Case("streaming-binary", PNG_BODY, "image/png", False, "binary", shape="streaming"),
    Case("file-binary", PNG_BODY, "image/png", False, "binary", shape="file"),
    # -- text-ish but not UTF-8: refused ---------------------------------
    Case(
        "charset-latin1",
        LATIN1_BODY,
        "text/plain; charset=iso-8859-1",
        False,
        "not UTF-8",
    ),
    Case(
        "charset-utf16",
        "hi".encode("utf-16"),
        "text/plain; charset=utf-16",
        False,
        "not UTF-8",
    ),
    Case(
        "json-invalid-utf8",
        b'\xff\xfe{"a": 1}',
        "application/json",
        False,
        "allowlisted type, but the bytes are not the UTF-8 it promised",
    ),
    # -- representation metadata: preserved ------------------------------
    Case(
        "endpoint-etag",
        JSON_BODY,
        "application/json",
        True,
        "an endpoint's own validator is replayed, not replaced by our hash",
        extra_headers={"ETag": '"strong-v7"'},
    ),
    Case(
        "vary-header",
        JSON_BODY,
        "application/json",
        True,
        "Vary is stored and replayed so intermediaries still see it",
        extra_headers={"Vary": "Accept-Language"},
    ),
    Case(
        "content-encoding-identity",
        JSON_BODY,
        "application/json",
        True,
        "identity is the absence of an encoding",
        extra_headers={"Content-Encoding": "identity"},
    ),
    Case(
        "pagination-headers",
        JSON_BODY,
        "application/json",
        True,
        "Link and X-Total-Count are what a paginating client follows",
        extra_headers={
            "Link": '</items?page=2>; rel="next"',
            "X-Total-Count": "4211",
        },
    ),
    Case(
        "download-headers",
        b"a,b\n1,2\n",
        "text/csv",
        True,
        "a cached export must still download under its filename",
        extra_headers={
            "Content-Disposition": 'attachment; filename="report.csv"',
            "Content-Language": "de-DE",
        },
    ),
    Case(
        "validator-headers",
        JSON_BODY,
        "application/json",
        True,
        "Last-Modified and Content-Location are representation metadata",
        extra_headers={
            "Last-Modified": "Wed, 10 Sep 2026 12:00:00 GMT",
            "Content-Location": "/payload?page=1",
        },
    ),
    # -- representation metadata that forbids storage: refused -----------
    Case(
        "content-encoding-gzip",
        gzip.compress(JSON_BODY),
        "application/json",
        False,
        "RFC 9110 section 8.4 - the entry cannot carry the decoding step",
        extra_headers={"Content-Encoding": "gzip"},
    ),
    Case(
        "vary-star",
        JSON_BODY,
        "application/json",
        False,
        "RFC 9111 section 4.1 - a Vary: * response may never be reused",
        extra_headers={"Vary": "*"},
    ),
    # -- no declared representation: refused -----------------------------
    Case("no-content-type", b"raw", None, False, "nothing says how to replay it"),
    # -- RFC 9111 storage rules: refused ---------------------------------
    Case("status-201", JSON_BODY, "application/json", False, "not 200", status=201),
    Case(
        "response-no-store",
        JSON_BODY,
        "application/json",
        False,
        "RFC 9111 section 3",
        extra_headers={"Cache-Control": "no-store"},
    ),
    Case(
        "response-private",
        JSON_BODY,
        "application/json",
        False,
        "RFC 9111 section 5.2.2.7 - a Redis entry is a shared cache",
        extra_headers={"Cache-Control": "private, max-age=60"},
    ),
    Case(
        "request-range-full-200",
        b"0123456789",
        "text/plain",
        False,
        "RFC 9111 section 3.3 - the key carries no Range",
        request_headers={"Range": "bytes=0-3"},
    ),
    Case(
        "file-range-206",
        b"0123456789" * 8,
        "text/plain",
        False,
        "RFC 9111 section 3.3 - a 206 must never be replayed as a 200",
        shape="file",
        request_headers={"Range": "bytes=0-15"},
    ),
]


def _build_app(case: Case, tmp_path: Path, calls: list[int]) -> FastAPI:
    app = FastAPI()
    FastAPIRedis(app).lifespan().caching()

    if case.shape == "file":
        target = tmp_path / f"{case.name}.bin"
        target.write_bytes(case.body)

    @app.get("/payload", dependencies=[Depends(cache(ttl=300))])
    async def payload() -> Response:
        calls[0] += 1
        if case.shape == "file":
            return FileResponse(
                target,
                media_type=case.media_type,
                headers=dict(case.extra_headers),
                status_code=case.status,
            )
        if case.shape == "streaming":
            return StreamingResponse(
                iter([case.body]),
                media_type=case.media_type,
                headers=dict(case.extra_headers),
                status_code=case.status,
            )
        return Response(
            content=case.body,
            media_type=case.media_type,
            headers=dict(case.extra_headers),
            status_code=case.status,
        )

    return app


@pytest.fixture()
def flushed(real_redis: sync_redis.Redis) -> Generator[sync_redis.Redis, None, None]:
    real_redis.flushdb()
    yield real_redis
    real_redis.flushdb()


@requires_redis
@pytest.mark.integration
@pytest.mark.parametrize("case", CASES, ids=lambda c: c.name)
def test_payload_matrix(case: Case, tmp_path: Path, flushed: sync_redis.Redis) -> None:
    calls = [0]
    app = _build_app(case, tmp_path, calls)

    with TestClient(app) as client:
        r1 = client.get("/payload", headers=case.request_headers)
        r2 = client.get("/payload", headers=case.request_headers)

    keys = flushed.keys("*")

    if case.stored:
        assert r1.headers.get("X-Redis-Cache") == "MISS", case.why
        assert r2.headers.get("X-Redis-Cache") == "HIT", case.why
        assert calls[0] == 1, "the hit must not re-run the endpoint"

        # The representation must survive the round trip whole: same bytes,
        # same content type.  Replaying text/csv as application/json is the
        # bug this matrix exists to catch.
        assert r2.content == r1.content == case.body

        # Every field the endpoint sent comes back on the hit, apart from the
        # groups an entry deliberately leaves out.  Comparing the whole set
        # rather than a few named fields is what makes the *next* dropped
        # header fail here instead of in production.
        #
        # Skipped: stamped per response (date, server), recomputed from the
        # replayed body (content-length), deliberately different (the cache
        # status), or time-dependent (cache-control counts down).
        skipped = {"date", "server", "content-length", "x-redis-cache", "cache-control"}
        miss_fields = {
            (k.lower(), v) for k, v in r1.headers.items() if k.lower() not in skipped
        }
        hit_fields = {
            (k.lower(), v) for k, v in r2.headers.items() if k.lower() not in skipped
        }
        assert miss_fields == hit_fields

        assert len(keys) == 1, f"expected exactly one entry, got {keys}"
        entry = json.loads(flushed.get(keys[0]))
        assert entry["body"] == case.body.decode()
        assert "v" not in entry, "entries carry no format marker"
        stored = dict(entry["headers"])
        assert stored["content-type"] == r1.headers["content-type"]
        assert "date" not in stored, "Date must not be stored - it double-counts age"
        assert "content-length" not in stored, "framing is recomputed"
    else:
        assert r1.headers.get("X-Redis-Cache") == "BYPASS", case.why
        assert r2.headers.get("X-Redis-Cache") == "BYPASS", case.why
        assert calls[0] == 2, "a refused response must be produced every time"
        assert keys == [], f"nothing may be stored, found {keys}"

        # A refusal changes nothing the caller can see.
        assert r1.status_code == r2.status_code
        assert r1.content == r2.content
        assert r1.headers.get("content-type") == r2.headers.get("content-type")
        if case.media_type is not None:
            assert r1.headers.get("content-type") is not None


@requires_redis
@pytest.mark.integration
def test_oversized_payload_refused(flushed: sync_redis.Redis) -> None:
    """A body over ``MAX_CACHEABLE_BODY_SIZE`` is delivered but not stored.

    The oversized path flushes the buffered start before it knows the final
    size, so this is the one refusal whose ``BYPASS`` marker is added by
    ``_flush_oversized_response`` rather than by the storage decision.
    """
    from redis_fastapi.cache import MAX_CACHEABLE_BODY_SIZE

    payload = "x" * (MAX_CACHEABLE_BODY_SIZE + 1)
    calls = [0]

    app = FastAPI()
    FastAPIRedis(app).lifespan().caching()

    @app.get("/huge", dependencies=[Depends(cache(ttl=300))])
    async def huge() -> dict:
        calls[0] += 1
        return {"data": payload}

    with TestClient(app) as client:
        r1 = client.get("/huge")
        r2 = client.get("/huge")

    assert r1.status_code == r2.status_code == 200
    assert r1.json()["data"] == payload, "the body must be delivered whole"
    assert r2.json()["data"] == payload
    assert r1.headers.get("X-Redis-Cache") == "BYPASS"
    assert r2.headers.get("X-Redis-Cache") == "BYPASS"
    assert calls[0] == 2
    assert flushed.keys("*") == []


@requires_redis
@pytest.mark.integration
@pytest.mark.parametrize("put_status", [200, 201])
def test_write_through_stores_any_2xx(
    put_status: int, flushed: sync_redis.Redis
) -> None:
    """``cache_put()`` may store a 201, because the 201 never gets replayed.

    The body is installed as the representation for a later GET, which answers
    200 with it.  The read path refuses a 201 for the opposite reason: there,
    the stored response *is* the one the next client receives.
    """
    from redis_fastapi.cache import cache_put, default_key_builder

    app = FastAPI()
    FastAPIRedis(app).lifespan().caching()
    get_calls = [0]

    @app.get(
        "/widgets/{widget_id}",
        dependencies=[Depends(cache(ttl=300, eviction_group="widgets"))],
    )
    async def read_widget(widget_id: str) -> dict:
        get_calls[0] += 1
        return {"id": widget_id, "source": "endpoint"}

    @app.put(
        "/widgets/{widget_id}",
        status_code=put_status,
        dependencies=[
            Depends(
                cache_put(
                    eviction_group="widgets",
                    key_builder=default_key_builder,
                    ttl=300,
                )
            )
        ],
    )
    async def write_widget(widget_id: str) -> dict:
        return {"id": widget_id, "source": "write-through"}

    with TestClient(app) as client:
        put = client.put("/widgets/w1")
        assert put.status_code == put_status

        got = client.get("/widgets/w1")

    # The write-through body was installed, so the GET is a hit that never
    # reached the endpoint - and it answers 200 regardless of the PUT status.
    assert got.status_code == 200
    assert got.headers.get("X-Redis-Cache") == "HIT"
    assert got.json()["source"] == "write-through"
    assert get_calls[0] == 0
    assert len(flushed.keys("*")) == 1


@requires_redis
@pytest.mark.integration
def test_documented_per_user_key_builder_recipe(flushed: sync_redis.Redis) -> None:
    """The per-user ``key_builder`` recipe from the caching guide works.

    This is the documented remedy for RFC 9111 section 3.5 - a shared entry
    would otherwise serve the first caller's body to everyone.  The guide
    shows this code, so it is tested rather than merely asserted.
    """
    from redis_fastapi.cache import default_key_builder

    def key_per_user(request, eviction_group="", prefix=""):  # type: ignore[no-untyped-def]
        base = default_key_builder(
            request, eviction_group=eviction_group, prefix=prefix
        )
        return f"{base}:u:{request.headers.get('x-user', 'anon')}"

    app = FastAPI()
    FastAPIRedis(app).lifespan().caching()

    @app.get(
        "/me/profile",
        dependencies=[Depends(cache(ttl=300, private=True, key_builder=key_per_user))],
    )
    async def profile(request: Request) -> dict:
        return {"user": request.headers.get("x-user")}

    with TestClient(app) as client:
        alice1 = client.get("/me/profile", headers={"X-User": "alice"})
        bob1 = client.get("/me/profile", headers={"X-User": "bob"})
        alice2 = client.get("/me/profile", headers={"X-User": "alice"})

    assert alice1.headers["X-Redis-Cache"] == "MISS"
    # Bob must not be served Alice's entry.
    assert bob1.headers["X-Redis-Cache"] == "MISS"
    assert bob1.json() == {"user": "bob"}
    assert alice2.headers["X-Redis-Cache"] == "HIT"
    assert alice2.json() == {"user": "alice"}
    assert len(flushed.keys("*")) == 2, "one entry per caller"


@requires_redis
@pytest.mark.integration
def test_documented_negotiated_key_builder_recipe(flushed: sync_redis.Redis) -> None:
    """The ``Accept-Language`` ``key_builder`` recipe from the guide works.

    ``Vary`` is ignored, so folding the negotiated header into the key is the
    documented way to keep one URL's variants apart.
    """
    from redis_fastapi.cache import default_key_builder

    def key_with_language(request, eviction_group="", prefix=""):  # type: ignore[no-untyped-def]
        base = default_key_builder(
            request, eviction_group=eviction_group, prefix=prefix
        )
        lang = request.headers.get("accept-language", "*")
        return f"{base}:lang={lang}"

    app = FastAPI()
    FastAPIRedis(app).lifespan().caching()

    @app.get(
        "/articles/{slug}",
        dependencies=[Depends(cache(ttl=300, key_builder=key_with_language))],
    )
    async def article(slug: str, request: Request) -> dict:
        return {"lang": request.headers.get("accept-language")}

    with TestClient(app) as client:
        en = client.get("/articles/x", headers={"Accept-Language": "en"})
        de = client.get("/articles/x", headers={"Accept-Language": "de"})
        en2 = client.get("/articles/x", headers={"Accept-Language": "en"})

    assert en.headers["X-Redis-Cache"] == "MISS"
    assert de.headers["X-Redis-Cache"] == "MISS"
    assert de.json() == {"lang": "de"}, "the German reader must not get English"
    assert en2.headers["X-Redis-Cache"] == "HIT"
    assert en2.json() == {"lang": "en"}
    assert len(flushed.keys("*")) == 2
