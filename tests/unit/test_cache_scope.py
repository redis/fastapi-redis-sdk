"""Unit tests for cache scope: which representations may be stored, and why.

The integration matrix in ``tests/integration/test_cache_payloads.py`` covers
the end-to-end behaviour against real Redis.  This file pins the pieces that
matrix cannot reach: the header parsers on their own, the refusal reasons
themselves, warn-once logging, and entries written before the content type was
part of the format.
"""

from __future__ import annotations

import json
import logging

import fakeredis.aioredis
import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from starlette.requests import Request
from starlette.responses import Response

from redis_fastapi.cache import (
    _WARNED_REFUSALS,
    MAX_CACHEABLE_HEADER_SIZE,
    CachePending,
    _excluded_from_storage,
    _is_cacheable_media_type,
    _merge_headers,
    _split_content_type,
    _storable_headers,
    _storage_refusal,
    _warn_refusal_once,
    cache,
)
from redis_fastapi.config import get_settings
from redis_fastapi.deps import get_async_redis
from redis_fastapi.setup import FastAPIRedis


def _request(path: str = "/x", headers: dict[str, str] | None = None) -> Request:
    raw = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "headers": raw,
            "query_string": b"",
        }
    )


def _fake_dep(fake: fakeredis.aioredis.FakeRedis):
    async def _dep() -> fakeredis.aioredis.FakeRedis:
        return fake

    return _dep


# ===================================================================
# Content-Type parsing
# ===================================================================


@pytest.mark.unit
class TestSplitContentType:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            (None, ("", None)),
            (b"application/json", ("application/json", None)),
            (b"text/plain; charset=utf-8", ("text/plain", "utf-8")),
            (b"TEXT/PLAIN; CHARSET=UTF-8", ("text/plain", "utf-8")),
            (b'text/plain; charset="utf-8"', ("text/plain", "utf-8")),
            (b"text/plain ; charset = utf-8 ", ("text/plain", "utf-8")),
            (b"text/html; charset=iso-8859-1", ("text/html", "iso-8859-1")),
            (b"multipart/form-data; boundary=xyz", ("multipart/form-data", None)),
        ],
    )
    def test_split(self, raw: bytes | None, expected: tuple[str, str | None]) -> None:
        assert _split_content_type(raw) == expected


@pytest.mark.unit
class TestIsCacheableMediaType:
    @pytest.mark.parametrize(
        "media_type",
        [
            "application/json",
            "application/xml",
            "application/javascript",
            "application/problem+json",
            "application/atom+xml",
            "text/plain",
            "text/html",
            "text/csv",
            "text/anything-at-all",
        ],
    )
    def test_allowed(self, media_type: str) -> None:
        assert _is_cacheable_media_type(media_type) is True

    @pytest.mark.parametrize(
        "media_type",
        [
            "",
            "image/png",
            "image/svg+xml",
            "application/octet-stream",
            "application/pdf",
            "application/msgpack",
            "application/x-protobuf",
            "audio/mpeg",
            "video/mp4",
            "multipart/form-data",
        ],
    )
    def test_refused(self, media_type: str) -> None:
        assert _is_cacheable_media_type(media_type) is False

    def test_image_svg_xml_is_refused_despite_suffix(self) -> None:
        """``+xml`` only admits the ``application/`` tree.

        ``image/svg+xml`` is text in practice, but admitting it would mean
        admitting an ``image/*`` type, and the next reader of the allowlist
        would reasonably read that as "images are cacheable".
        """
        assert _is_cacheable_media_type("image/svg+xml") is False


# ===================================================================
# Refusal reasons
# ===================================================================


@pytest.mark.unit
class TestStorageRefusal:
    @staticmethod
    def _pending(*, write_through: bool = False) -> CachePending:
        return CachePending(key="k", ttl=60, write_through=write_through)

    def test_json_200_is_stored(self) -> None:
        assert (
            _storage_refusal(
                _request(),
                self._pending(),
                200,
                [(b"content-type", b"application/json")],
            )
            is None
        )

    @pytest.mark.parametrize("status", [201, 202, 203, 204, 206, 301, 404, 500])
    def test_non_200_refused_on_read_path(self, status: int) -> None:
        reason = _storage_refusal(
            _request(),
            self._pending(),
            status,
            [(b"content-type", b"application/json")],
        )
        assert reason is not None
        assert str(status) in reason

    @pytest.mark.parametrize("status", [200, 201, 202])
    def test_any_2xx_stored_on_write_through(self, status: int) -> None:
        """Write-through installs the body for a later GET.

        The status of the PUT that produced it never reaches a client, so it
        does not constrain storage the way a read-path fill does.
        """
        assert (
            _storage_refusal(
                _request(),
                self._pending(write_through=True),
                status,
                [(b"content-type", b"application/json")],
            )
            is None
        )

    @pytest.mark.parametrize("status", [304, 400, 500])
    def test_non_2xx_refused_on_write_through(self, status: int) -> None:
        reason = _storage_refusal(
            _request(),
            self._pending(write_through=True),
            status,
            [(b"content-type", b"application/json")],
        )
        assert reason is not None and "2xx" in reason

    def test_content_range_refused(self) -> None:
        reason = _storage_refusal(
            _request(),
            self._pending(),
            200,
            [
                (b"content-type", b"text/plain"),
                (b"content-range", b"bytes 0-9/100"),
            ],
        )
        assert reason == "response carries Content-Range"

    def test_request_range_refused(self) -> None:
        reason = _storage_refusal(
            _request(headers={"Range": "bytes=0-9"}),
            self._pending(),
            200,
            [(b"content-type", b"text/plain")],
        )
        assert reason == "request carried Range"

    @pytest.mark.parametrize(
        ("header", "expected"),
        [
            (b"no-store", "response set Cache-Control: no-store"),
            (b"private", "response set Cache-Control: private"),
            (b"private, max-age=60", "response set Cache-Control: private"),
            (b"public, no-store", "response set Cache-Control: no-store"),
        ],
    )
    def test_response_cache_control_refused(self, header: bytes, expected: str) -> None:
        reason = _storage_refusal(
            _request(),
            self._pending(),
            200,
            [(b"content-type", b"application/json"), (b"cache-control", header)],
        )
        assert reason == expected

    @pytest.mark.parametrize(
        "header", [b"public, max-age=60", b"no-cache", b"must-revalidate"]
    )
    def test_permissive_cache_control_allowed(self, header: bytes) -> None:
        """Only ``no-store`` and ``private`` forbid storage (RFC 9111 §3)."""
        assert (
            _storage_refusal(
                _request(),
                self._pending(),
                200,
                [(b"content-type", b"application/json"), (b"cache-control", header)],
            )
            is None
        )

    def test_missing_content_type_refused(self) -> None:
        reason = _storage_refusal(_request(), self._pending(), 200, [])
        assert reason is not None and "none" in reason

    def test_binary_content_type_refused(self) -> None:
        reason = _storage_refusal(
            _request(), self._pending(), 200, [(b"content-type", b"image/png")]
        )
        assert reason is not None and "image/png" in reason

    def test_non_utf8_charset_refused(self) -> None:
        reason = _storage_refusal(
            _request(),
            self._pending(),
            200,
            [(b"content-type", b"text/plain; charset=iso-8859-1")],
        )
        assert reason is not None and "iso-8859-1" in reason


# ===================================================================
# Representation metadata: what the entry preserves
# ===================================================================


@pytest.mark.unit
class TestEncodedAndUnreusableResponses:
    """Refusals that come from the response's own metadata."""

    @staticmethod
    def _pending() -> CachePending:
        return CachePending(key="k", ttl=60)

    @pytest.mark.parametrize(
        ("header", "expected"),
        [
            (b"gzip", "response carries Content-Encoding: gzip"),
            (b"br", "response carries Content-Encoding: br"),
            (b"gzip, br", "response carries Content-Encoding: gzip, br"),
        ],
    )
    def test_encoded_body_refused_by_name(self, header: bytes, expected: str) -> None:
        """An encoded body is named as such, not reported as bad UTF-8.

        A gzip stream fails the UTF-8 decode anyway, so the response was
        already refused - but the reason told the operator the bytes were
        broken rather than that the response was compressed.
        """
        reason = _storage_refusal(
            _request(),
            self._pending(),
            200,
            [(b"content-type", b"application/json"), (b"content-encoding", header)],
        )
        assert reason == expected

    def test_identity_encoding_is_not_an_encoding(self) -> None:
        assert (
            _storage_refusal(
                _request(),
                self._pending(),
                200,
                [
                    (b"content-type", b"application/json"),
                    (b"content-encoding", b"identity"),
                ],
            )
            is None
        )

    def test_vary_star_refused(self) -> None:
        """RFC 9111 section 4.1: a ``Vary: *`` response may never be reused."""
        reason = _storage_refusal(
            _request(),
            self._pending(),
            200,
            [(b"content-type", b"application/json"), (b"vary", b"*")],
        )
        assert reason == "response set Vary: *"

    def test_ordinary_vary_is_stored(self) -> None:
        assert (
            _storage_refusal(
                _request(),
                self._pending(),
                200,
                [
                    (b"content-type", b"application/json"),
                    (b"vary", b"Accept-Language"),
                ],
            )
            is None
        )


@pytest.mark.unit
class TestOwnedHeadersReplaceRatherThanAppend:
    """``ETag`` and ``Cache-Control`` are single-valued fields."""

    def test_merge_replaces_every_earlier_occurrence(self) -> None:
        merged = _merge_headers(
            [
                (b"etag", b'"endpoint"'),
                (b"cache-control", b"public, max-age=600"),
                (b"x-keep", b"kept"),
                (b"ETag", b'"second"'),
            ],
            [(b"etag", b'W/"ours"'), (b"cache-control", b"max-age=60")],
        )
        assert merged == [
            (b"x-keep", b"kept"),
            (b"etag", b'W/"ours"'),
            (b"cache-control", b"max-age=60"),
        ]

    def test_endpoint_etag_survives_and_revalidates(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        """An endpoint's own validator is replayed, not replaced by a hash.

        RFC 9110 section 8.8.3 defines ``ETag = entity-tag`` - a single tag.
        Appending ours beside the endpoint's produced a field holding two,
        and a client echoing it back got a full 200 instead of a 304.
        """
        app = FastAPI()
        FastAPIRedis(app).caching()

        @app.get("/tagged", dependencies=[Depends(cache(ttl=300))])
        async def tagged() -> Response:
            return Response(
                content='{"v": 1}',
                media_type="application/json",
                headers={"ETag": '"strong-v7"'},
            )

        app.dependency_overrides[get_async_redis] = _fake_dep(fake_async_redis)
        get_settings.cache_clear()
        try:
            with TestClient(app) as c:
                miss = c.get("/tagged")
                hit = c.get("/tagged")
                revalidated = c.get(
                    "/tagged", headers={"If-None-Match": miss.headers["etag"]}
                )
            assert miss.headers["etag"] == '"strong-v7"'
            assert hit.headers["etag"] == '"strong-v7"'
            assert hit.headers["X-Redis-Cache"] == "HIT"
            assert revalidated.status_code == 304
        finally:
            get_settings.cache_clear()

    def test_cache_control_is_not_duplicated(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        """We own ``Cache-Control`` on a cached route, on the miss as well.

        Appending produced ``public, max-age=600, max-age=600`` on the miss
        and a bare ``max-age`` on the hit - two different policies for one
        entry.
        """
        app = FastAPI()
        FastAPIRedis(app).caching()

        @app.get("/policy", dependencies=[Depends(cache(ttl=600))])
        async def policy() -> Response:
            return Response(
                content='{"v": 1}',
                media_type="application/json",
                headers={"Cache-Control": "public, max-age=600"},
            )

        app.dependency_overrides[get_async_redis] = _fake_dep(fake_async_redis)
        get_settings.cache_clear()
        try:
            with TestClient(app) as c:
                miss = c.get("/policy")
                hit = c.get("/policy")
            assert miss.headers["cache-control"].count("max-age") == 1
            assert miss.headers["cache-control"] == hit.headers["cache-control"]
        finally:
            get_settings.cache_clear()

    def test_vary_is_stored_and_replayed(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        """``Vary`` reaches the client on a hit, so intermediaries still see it.

        The lookup ignores it, but dropping it from the response would make
        one stored variant look like the only variant to a downstream cache
        as well as to us.
        """
        app = FastAPI()
        FastAPIRedis(app).caching()

        @app.get("/negotiated", dependencies=[Depends(cache(ttl=300))])
        async def negotiated() -> Response:
            return Response(
                content='{"v": 1}',
                media_type="application/json",
                headers={"Vary": "Accept-Language"},
            )

        app.dependency_overrides[get_async_redis] = _fake_dep(fake_async_redis)
        get_settings.cache_clear()
        try:
            with TestClient(app) as c:
                miss = c.get("/negotiated")
                hit = c.get("/negotiated")
                validated = c.get(
                    "/negotiated", headers={"If-None-Match": hit.headers["etag"]}
                )
            assert miss.headers["vary"] == "Accept-Language"
            assert hit.headers["vary"] == "Accept-Language"
            # RFC 9110 section 15.4.5 lists Vary among the fields a 304 carries.
            assert validated.status_code == 304
            assert validated.headers["vary"] == "Accept-Language"
        finally:
            get_settings.cache_clear()


# ===================================================================
# Warn-once
# ===================================================================


@pytest.mark.unit
class TestWarnOnce:
    def test_same_route_and_reason_warns_once(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        _WARNED_REFUSALS.clear()
        request = _request("/dupe")
        with caplog.at_level(logging.WARNING, logger="redis_fastapi.cache"):
            for _ in range(5):
                _warn_refusal_once(request, "because")
        assert len(caplog.records) == 1
        assert "/dupe" in caplog.records[0].getMessage()
        assert "because" in caplog.records[0].getMessage()

    def test_distinct_reasons_each_warn(self, caplog: pytest.LogCaptureFixture) -> None:
        _WARNED_REFUSALS.clear()
        request = _request("/multi")
        with caplog.at_level(logging.WARNING, logger="redis_fastapi.cache"):
            _warn_refusal_once(request, "reason one")
            _warn_refusal_once(request, "reason two")
        assert len(caplog.records) == 2

    def test_refused_route_logs_once_across_requests(
        self,
        fake_async_redis: fakeredis.aioredis.FakeRedis,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        _WARNED_REFUSALS.clear()
        app = FastAPI()
        FastAPIRedis(app).caching()

        @app.get("/png", dependencies=[Depends(cache(ttl=300))])
        async def png() -> Response:
            return Response(content=b"\x89PNG", media_type="image/png")

        app.dependency_overrides[get_async_redis] = _fake_dep(fake_async_redis)
        get_settings.cache_clear()
        try:
            with caplog.at_level(logging.WARNING, logger="redis_fastapi.cache"):
                with TestClient(app) as c:
                    for _ in range(4):
                        assert c.get("/png").headers["X-Redis-Cache"] == "BYPASS"
            refusals = [r for r in caplog.records if "not cached" in r.getMessage()]
            assert len(refusals) == 1
            assert "/png" in refusals[0].getMessage()
        finally:
            get_settings.cache_clear()


# ===================================================================
# Entry format: forwards and backwards
# ===================================================================


@pytest.mark.unit
class TestEntryFormat:
    async def test_stored_entry_carries_the_header_block(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app = FastAPI()
        FastAPIRedis(app).caching()

        @app.get("/csv", dependencies=[Depends(cache(ttl=300))])
        async def csv() -> Response:
            return Response(content="a,b\n", media_type="text/csv")

        app.dependency_overrides[get_async_redis] = _fake_dep(fake_async_redis)
        get_settings.cache_clear()
        try:
            with TestClient(app) as c:
                r1 = c.get("/csv")
                r2 = c.get("/csv")
            assert r2.headers["X-Redis-Cache"] == "HIT"
            assert r2.headers["content-type"] == r1.headers["content-type"]
            assert r2.headers["content-type"] == "text/csv; charset=utf-8"

            keys = await fake_async_redis.keys("*")
            entry = json.loads(await fake_async_redis.get(keys[0]))
            assert entry["body"] == "a,b\n"
            assert "v" not in entry, "entries carry no format marker"
            assert dict(entry["headers"])["content-type"] == "text/csv; charset=utf-8"
            assert "encoding" not in entry, "bodies are stored as text, not base64"
        finally:
            get_settings.cache_clear()


@pytest.mark.unit
class TestStorableHeaders:
    """Which fields reach the entry, and which are left out."""

    def test_endpoint_fields_are_stored_in_order(self) -> None:
        stored = _storable_headers(
            [
                (b"content-type", b"application/json"),
                (b"Link", b'</a>; rel="next"'),
                (b"X-Total-Count", b"4211"),
            ]
        )
        assert stored == [
            ["content-type", "application/json"],
            ["link", '</a>; rel="next"'],
            ["x-total-count", "4211"],
        ]

    def test_repeated_fields_survive_as_repeats(self) -> None:
        """A mapping would collapse these; the entry keeps both, in order."""
        stored = _storable_headers(
            [
                (b"content-type", b"application/json"),
                (b"link", b'</a>; rel="next"'),
                (b"link", b'</z>; rel="last"'),
            ]
        )
        assert [v for k, v in stored if k == "link"] == [
            '</a>; rel="next"',
            '</z>; rel="last"',
        ]

    @pytest.mark.parametrize(
        "excluded",
        [
            b"cache-control",  # this library re-emits it
            b"etag",  # stored as its own field
            b"x-redis-cache",  # ours
            b"content-length",  # recomputed from the replayed body
            b"date",  # replaying it double-counts the entry's age
            b"set-cookie",  # a shared entry must not replay a session
            b"connection",  # RFC 9110 section 7.6.1
            b"transfer-encoding",
            b"keep-alive",
            b"upgrade",
            b"te",
            b"proxy-authenticate",  # RFC 9111 section 3.1, a MUST NOT
            b"proxy-authorization",
            b"proxy-authentication-info",
        ],
    )
    def test_excluded_fields_never_reach_the_entry(self, excluded: bytes) -> None:
        stored = _storable_headers(
            [(b"content-type", b"application/json"), (excluded, b"whatever")]
        )
        assert stored == [["content-type", "application/json"]]

    def test_connection_names_further_fields_to_drop(self) -> None:
        """``Connection`` lists fields that are specific to that message."""
        excluded = _excluded_from_storage([(b"connection", b"X-Hop-Only, Keep-Alive")])
        assert b"x-hop-only" in excluded

        stored = _storable_headers(
            [
                (b"content-type", b"application/json"),
                (b"connection", b"X-Hop-Only"),
                (b"x-hop-only", b"internal"),
                (b"x-kept", b"public"),
            ]
        )
        assert stored == [
            ["content-type", "application/json"],
            ["x-kept", "public"],
        ]


@pytest.mark.unit
class TestHitCarriesEndpointHeaders:
    """The eight use cases that a four-field entry could not serve."""

    @staticmethod
    def _app(fake: fakeredis.aioredis.FakeRedis, headers: dict[str, str]) -> FastAPI:
        app = FastAPI()
        FastAPIRedis(app).caching()

        @app.get("/items", dependencies=[Depends(cache(ttl=300))])
        async def items() -> Response:
            return Response(
                content='[{"id": 1}]',
                media_type="application/json",
                headers=headers,
            )

        app.dependency_overrides[get_async_redis] = _fake_dep(fake)
        return app

    def test_pagination_and_representation_metadata_replay(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        sent = {
            "Link": '</items?page=2>; rel="next"',
            "X-Total-Count": "4211",
            "Content-Language": "de-DE",
            "Content-Disposition": 'attachment; filename="items.json"',
            "Last-Modified": "Wed, 10 Sep 2026 12:00:00 GMT",
            "Content-Location": "/items?page=1",
            "Content-Digest": "sha-256=:abc:",
        }
        get_settings.cache_clear()
        try:
            with TestClient(self._app(fake_async_redis, sent)) as c:
                miss = c.get("/items")
                hit = c.get("/items")
            assert miss.headers["X-Redis-Cache"] == "MISS"
            assert hit.headers["X-Redis-Cache"] == "HIT"
            for name, value in sent.items():
                assert hit.headers.get(name) == value, name
        finally:
            get_settings.cache_clear()

    def test_cookies_are_not_replayed(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        """A shared entry must not hand one caller's session to the next."""
        get_settings.cache_clear()
        try:
            app = self._app(fake_async_redis, {"Set-Cookie": "session=abc123; Path=/"})
            with TestClient(app) as c:
                miss = c.get("/items")
                hit = c.get("/items")
            assert miss.headers.get("set-cookie") == "session=abc123; Path=/"
            assert hit.headers.get("set-cookie") is None
        finally:
            get_settings.cache_clear()

    def test_not_modified_carries_no_representation_metadata(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        """RFC 9110 section 15.4.5 limits what a 304 may carry.

        The stored block holds a Content-Type and a Link; replaying the whole
        block on a 304 would describe a representation the response does not
        contain.
        """
        get_settings.cache_clear()
        try:
            app = self._app(
                fake_async_redis,
                {"Link": '</items?page=2>; rel="next"', "Vary": "Accept-Language"},
            )
            with TestClient(app) as c:
                miss = c.get("/items")
                validated = c.get(
                    "/items", headers={"If-None-Match": miss.headers["etag"]}
                )
            assert validated.status_code == 304
            # Carried: the validator, the cache metadata, Vary.
            assert validated.headers["etag"] == miss.headers["etag"]
            assert validated.headers["vary"] == "Accept-Language"
            assert "cache-control" in validated.headers
            # Not carried: anything describing a body that is not there.
            assert validated.headers.get("content-type") is None
            assert validated.headers.get("link") is None
        finally:
            get_settings.cache_clear()


@pytest.mark.unit
class TestConditionalRequests:
    """Both validators, and their precedence."""

    @staticmethod
    def _app(fake: fakeredis.aioredis.FakeRedis, headers: dict[str, str]) -> FastAPI:
        app = FastAPI()
        FastAPIRedis(app).caching()

        @app.get("/doc", dependencies=[Depends(cache(ttl=300))])
        async def doc() -> Response:
            return Response(
                content='{"v": 1}', media_type="application/json", headers=headers
            )

        app.dependency_overrides[get_async_redis] = _fake_dep(fake)
        return app

    LAST_MODIFIED = "Wed, 10 Sep 2026 12:00:00 GMT"

    @pytest.mark.parametrize(
        ("since", "expected"),
        [
            (LAST_MODIFIED, 304),  # same instant: unchanged
            ("Thu, 11 Sep 2026 12:00:00 GMT", 304),  # client is newer
            ("Tue, 09 Sep 2026 12:00:00 GMT", 200),  # client is older
            ("not a date", 200),  # unparseable: no precondition
        ],
    )
    def test_if_modified_since(
        self,
        since: str,
        expected: int,
        fake_async_redis: fakeredis.aioredis.FakeRedis,
    ) -> None:
        get_settings.cache_clear()
        try:
            app = self._app(fake_async_redis, {"Last-Modified": self.LAST_MODIFIED})
            with TestClient(app) as c:
                c.get("/doc")  # fill
                r = c.get("/doc", headers={"If-Modified-Since": since})
            assert r.status_code == expected
        finally:
            get_settings.cache_clear()

    def test_if_modified_since_ignored_without_a_stored_last_modified(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        get_settings.cache_clear()
        try:
            with TestClient(self._app(fake_async_redis, {})) as c:
                c.get("/doc")
                r = c.get("/doc", headers={"If-Modified-Since": self.LAST_MODIFIED})
            assert r.status_code == 200
        finally:
            get_settings.cache_clear()

    def test_if_none_match_takes_precedence(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        """RFC 9110 section 13.2.2: an entity-tag precondition wins.

        The date here would justify a 304 on its own.  Because the ETag does
        not match, the body must be sent anyway.
        """
        get_settings.cache_clear()
        try:
            app = self._app(fake_async_redis, {"Last-Modified": self.LAST_MODIFIED})
            with TestClient(app) as c:
                c.get("/doc")
                r = c.get(
                    "/doc",
                    headers={
                        "If-None-Match": 'W/"stale"',
                        "If-Modified-Since": self.LAST_MODIFIED,
                    },
                )
            assert r.status_code == 200
        finally:
            get_settings.cache_clear()


@pytest.mark.unit
class TestHeaderBlockCap:
    """A route may not store an unbounded amount of metadata."""

    @staticmethod
    def _pending() -> CachePending:
        return CachePending(key="k", ttl=60)

    def test_oversized_block_refused(self) -> None:
        reason = _storage_refusal(
            _request(),
            self._pending(),
            200,
            [
                (b"content-type", b"application/json"),
                (b"x-huge", b"v" * (MAX_CACHEABLE_HEADER_SIZE + 1)),
            ],
        )
        assert reason is not None
        assert "over the" in reason

    def test_ordinary_block_allowed(self) -> None:
        assert (
            _storage_refusal(
                _request(),
                self._pending(),
                200,
                [
                    (b"content-type", b"application/json"),
                    (b"link", b'</a>; rel="next"'),
                    (b"x-total-count", b"4211"),
                ],
            )
            is None
        )


@pytest.mark.unit
class TestDuplicateCacheControlHeaders:
    """A forbidding directive on a second header line must still be seen."""

    @staticmethod
    def _pending() -> CachePending:
        return CachePending(key="k", ttl=60)

    @pytest.mark.parametrize("forbidding", [b"no-store", b"private"])
    def test_second_line_is_read(self, forbidding: bytes) -> None:
        reason = _storage_refusal(
            _request(),
            self._pending(),
            200,
            [
                (b"content-type", b"application/json"),
                (b"cache-control", b"public, max-age=60"),
                (b"cache-control", forbidding),
            ],
        )
        assert reason == f"response set Cache-Control: {forbidding.decode()}"

    def test_all_permissive_lines_still_stored(self) -> None:
        assert (
            _storage_refusal(
                _request(),
                self._pending(),
                200,
                [
                    (b"content-type", b"application/json"),
                    (b"cache-control", b"public"),
                    (b"cache-control", b"max-age=60"),
                ],
            )
            is None
        )
