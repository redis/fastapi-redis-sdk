"""Integration tests for ASGI extension messages through the cache middleware.

ASGI servers advertise response extensions the capture middleware cannot
buffer: Granian's ``http.response.pathsend``, uvicorn's
``http.response.zerocopysend``, Hypercorn's ``http.response.trailers``, and
``http.response.debug``.  While a ``cache()`` dependency is active the
middleware withholds ``http.response.start`` until the body is complete -- so
on any of these it must flush the buffered start, fall back to passthrough and
forward the message verbatim, rather than swallowing the whole response.

These tests drive the full ASGI stack (real Redis, real lifespan, real DI) with
a recording ``send``: ``TestClient`` silently drops message types it does not
know about, so it cannot show what the middleware actually emitted.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
import redis as sync_redis
from fastapi import Depends, FastAPI, Response
from starlette.types import Message, Receive, Scope, Send

from redis_fastapi.cache import cache
from redis_fastapi.config import CACHE_STATUS_HEADER
from redis_fastapi.setup import FastAPIRedis
from tests.conftest import requires_redis

# Extension messages that replace the response body entirely: the server sends
# ``http.response.start`` and then one of these instead of any body chunk.
BODYLESS_EXTENSION_MESSAGES: dict[str, Message] = {
    "pathsend": {
        "type": "http.response.pathsend",
        "path": "/tmp/report.json",  # never opened, only forwarded
    },
    "zerocopysend": {
        "type": "http.response.zerocopysend",
        "file": 7,
        "offset": 0,
        "count": 128,
    },
    "debug": {
        "type": "http.response.debug",
        "info": {"template": "report.html", "context": {}},
    },
}


class _ExtensionResponse(Response):
    """Emit ``http.response.start`` followed by a raw extension message.

    Mirrors what Granian does for ``pathsend``: a normal start message, then
    the extension message, and no ``http.response.body`` at all.
    """

    def __init__(self, message: Message) -> None:
        super().__init__(status_code=200, media_type="application/json")
        self._message = message

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": self.status_code,
                "headers": self.raw_headers,
            }
        )
        await send(dict(self._message))


class _TrailersResponse(Response):
    """Emit a body chunk and then ``http.response.trailers``.

    Unlike the bodyless extensions above, trailers arrive *after* body chunks
    the middleware has already buffered.
    """

    chunk = b'{"value": 1}'

    def __init__(self) -> None:
        super().__init__(status_code=200, media_type="application/json")

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": self.status_code,
                "headers": self.raw_headers,
            }
        )
        await send(
            {"type": "http.response.body", "body": self.chunk, "more_body": True}
        )
        await send(
            {
                "type": "http.response.trailers",
                "headers": [(b"x-checksum", b"deadbeef")],
                "more_trailers": False,
            }
        )


def _build_app() -> FastAPI:
    app = FastAPI()
    FastAPIRedis(app).lifespan().caching()

    @app.get("/cached/{kind}", dependencies=[Depends(cache(ttl=300))])
    async def cached_extension(kind: str) -> Response:
        return _ExtensionResponse(BODYLESS_EXTENSION_MESSAGES[kind])

    @app.get("/plain/{kind}")
    async def plain_extension(kind: str) -> Response:
        return _ExtensionResponse(BODYLESS_EXTENSION_MESSAGES[kind])

    @app.get("/cached-trailers", dependencies=[Depends(cache(ttl=300))])
    async def cached_trailers() -> Response:
        return _TrailersResponse()

    @app.get("/items", dependencies=[Depends(cache(ttl=300))])
    async def get_items() -> dict[str, int]:
        return {"value": 1}

    return app


async def _call(app: FastAPI, path: str) -> list[Message]:
    """Run one GET through the raw ASGI interface, recording every send()."""
    sent: list[Message] = []

    scope: Scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"testserver")],
        "client": ("testclient", 50000),
        "server": ("testserver", 80),
        "extensions": {
            "http.response.pathsend": {},
            "http.response.zerocopysend": {},
            "http.response.trailers": {},
            "http.response.debug": {},
        },
    }

    async def receive() -> Message:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: Message) -> None:
        sent.append(message)

    await app(scope, receive, send)
    return sent


def _headers(message: Message) -> dict[bytes, bytes]:
    return {name.lower(): value for name, value in message.get("headers", [])}


@pytest.fixture()
async def ext_app(real_redis: sync_redis.Redis) -> AsyncIterator[FastAPI]:
    """App with lifespan-managed pools against a freshly flushed Redis."""
    real_redis.flushdb()
    app = _build_app()
    async with app.router.lifespan_context(app):
        yield app


@requires_redis
@pytest.mark.integration
class TestBodylessExtensionsWhileBuffering:
    """A cache() dependency is pending, so the start message was withheld."""

    @pytest.mark.parametrize("kind", sorted(BODYLESS_EXTENSION_MESSAGES))
    async def test_start_flushed_then_message_forwarded(
        self, ext_app: FastAPI, kind: str
    ) -> None:
        expected = BODYLESS_EXTENSION_MESSAGES[kind]

        sent = await _call(ext_app, f"/cached/{kind}")

        assert [m["type"] for m in sent] == [
            "http.response.start",
            expected["type"],
        ]
        # The buffered start is released unchanged...
        assert sent[0]["status"] == 200
        assert _headers(sent[0])[b"content-type"] == b"application/json"
        # ...and the extension message is passed through verbatim.
        assert sent[1] == expected

    @pytest.mark.parametrize("kind", sorted(BODYLESS_EXTENSION_MESSAGES))
    async def test_no_cache_headers_injected(self, ext_app: FastAPI, kind: str) -> None:
        """Nothing was stored, so the response must not claim a MISS/ETag."""
        sent = await _call(ext_app, f"/cached/{kind}")

        headers = _headers(sent[0])
        assert CACHE_STATUS_HEADER.lower().encode() not in headers
        assert b"etag" not in headers

    @pytest.mark.parametrize("kind", sorted(BODYLESS_EXTENSION_MESSAGES))
    async def test_nothing_written_to_redis(
        self, ext_app: FastAPI, kind: str, real_redis: sync_redis.Redis
    ) -> None:
        await _call(ext_app, f"/cached/{kind}")

        assert real_redis.keys(f"*{kind}*") == []

    @pytest.mark.parametrize("kind", sorted(BODYLESS_EXTENSION_MESSAGES))
    async def test_repeat_request_is_not_served_from_cache(
        self, ext_app: FastAPI, kind: str
    ) -> None:
        """An empty/garbage entry must not be cached and replayed as a HIT."""
        expected = BODYLESS_EXTENSION_MESSAGES[kind]

        await _call(ext_app, f"/cached/{kind}")
        sent = await _call(ext_app, f"/cached/{kind}")

        assert [m["type"] for m in sent] == [
            "http.response.start",
            expected["type"],
        ]
        assert CACHE_STATUS_HEADER.lower().encode() not in _headers(sent[0])


@requires_redis
@pytest.mark.integration
class TestBodylessExtensionsInPassthrough:
    """No cache() dependency: the start message was already forwarded."""

    @pytest.mark.parametrize("kind", sorted(BODYLESS_EXTENSION_MESSAGES))
    async def test_no_duplicate_start_is_emitted(
        self, ext_app: FastAPI, kind: str
    ) -> None:
        expected = BODYLESS_EXTENSION_MESSAGES[kind]

        sent = await _call(ext_app, f"/plain/{kind}")

        assert [m["type"] for m in sent] == [
            "http.response.start",
            expected["type"],
        ]
        assert sent[1] == expected


@requires_redis
@pytest.mark.integration
class TestTrailersAfterBufferedBody:
    """Trailers arrive after body chunks the middleware already buffered."""

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "Trailers flush the buffered start but not the buffered body, "
            "so body chunks received before the trailers are dropped."
        ),
    )
    async def test_buffered_body_is_flushed_before_trailers(
        self, ext_app: FastAPI
    ) -> None:
        sent = await _call(ext_app, "/cached-trailers")

        assert [m["type"] for m in sent] == [
            "http.response.start",
            "http.response.body",
            "http.response.trailers",
        ]
        assert sent[1]["body"] == _TrailersResponse.chunk

    async def test_response_is_not_swallowed(self, ext_app: FastAPI) -> None:
        """Whatever happens to the body, start + trailers must be emitted."""
        sent = await _call(ext_app, "/cached-trailers")

        types = [m["type"] for m in sent]
        assert types[0] == "http.response.start"
        assert types[-1] == "http.response.trailers"
        assert sent[-1]["headers"] == [(b"x-checksum", b"deadbeef")]

    async def test_nothing_written_to_redis(
        self, ext_app: FastAPI, real_redis: sync_redis.Redis
    ) -> None:
        await _call(ext_app, "/cached-trailers")

        assert real_redis.keys("*trailers*") == []


@requires_redis
@pytest.mark.integration
class TestNormalResponsesStillCached:
    """Guard: the extension-message branch must not shadow the body path."""

    async def test_miss_then_hit(self, ext_app: FastAPI) -> None:
        status_header = CACHE_STATUS_HEADER.lower().encode()

        first = await _call(ext_app, "/items")
        assert [m["type"] for m in first] == [
            "http.response.start",
            "http.response.body",
        ]
        assert _headers(first[0])[status_header] == b"MISS"

        second = await _call(ext_app, "/items")
        assert _headers(second[0])[status_header] == b"HIT"
        assert second[1]["body"] == first[1]["body"]


@requires_redis
@pytest.mark.integration
class TestExtensionMessagesDoNotPoisonOtherKeys:
    """A pathsend response must not affect unrelated cached routes."""

    async def test_items_still_cacheable_after_pathsend(self, ext_app: FastAPI) -> None:
        await _call(ext_app, "/cached/pathsend")

        first = await _call(ext_app, "/items")
        second = await _call(ext_app, "/items")

        status_header = CACHE_STATUS_HEADER.lower().encode()
        assert _headers(first[0])[status_header] == b"MISS"
        assert _headers(second[0])[status_header] == b"HIT"
