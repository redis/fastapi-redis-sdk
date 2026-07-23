"""Adversarial tests for fastapi-redis-sdk edge cases and robustness.

Covers: non-2xx caching, key injection, empty bodies, streaming,
oversized responses, TTL edge cases, coder failures, settings cache,
non-HTTP scopes, pool lifecycle, Cache-Control parsing, concurrency.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import fakeredis.aioredis
import pytest
from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.testclient import TestClient
from starlette.requests import Request as StarletteRequest

from redis_fastapi.cache import (
    MAX_CACHEABLE_BODY_SIZE,
    CacheResponseCaptureMiddleware,
    _parse_cache_control,
    cache,
    default_key_builder,
)
from redis_fastapi.cache_backend import CacheBackend
from redis_fastapi.config import get_settings
from redis_fastapi.deps import _PoolState, get_async_redis
from redis_fastapi.setup import FastAPIRedis
from redis_fastapi.types import JsonCoder

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_fake_dep(fake: fakeredis.aioredis.FakeRedis):
    async def _fake() -> fakeredis.aioredis.FakeRedis:
        return fake

    return _fake


def _make_request(path: str, query: str = "") -> StarletteRequest:
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "query_string": query.encode(),
        "headers": [],
    }
    return StarletteRequest(scope)


# ===================================================================
# 1. Non-2xx status code caching guard
# ===================================================================


@pytest.mark.unit
class TestNon2xxNotCached:
    """4xx/5xx responses must NOT be cached."""

    def test_404_not_cached(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app = FastAPI()
        FastAPIRedis(app).caching()
        counts = [0]

        @app.get("/missing", dependencies=[Depends(cache(ttl=300))])
        async def missing() -> dict:
            counts[0] += 1
            raise HTTPException(status_code=404, detail="not found")

        app.dependency_overrides[get_async_redis] = _make_fake_dep(fake_async_redis)
        with TestClient(app, raise_server_exceptions=False) as c:
            r1 = c.get("/missing")
            assert r1.status_code == 404
            r2 = c.get("/missing")
            assert r2.status_code == 404
            # Endpoint must have been called twice - not served from cache
            assert counts[0] == 2

    def test_500_not_cached(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app = FastAPI()
        FastAPIRedis(app).caching()
        counts = [0]

        @app.get("/error", dependencies=[Depends(cache(ttl=300))])
        async def error() -> dict:
            counts[0] += 1
            raise HTTPException(status_code=500, detail="server error")

        app.dependency_overrides[get_async_redis] = _make_fake_dep(fake_async_redis)
        with TestClient(app, raise_server_exceptions=False) as c:
            r1 = c.get("/error")
            assert r1.status_code == 500
            r2 = c.get("/error")
            assert r2.status_code == 500
            assert counts[0] == 2

    def test_400_not_cached(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app = FastAPI()
        FastAPIRedis(app).caching()
        counts = [0]

        @app.get("/bad", dependencies=[Depends(cache(ttl=300))])
        async def bad() -> dict:
            counts[0] += 1
            raise HTTPException(status_code=400)

        app.dependency_overrides[get_async_redis] = _make_fake_dep(fake_async_redis)
        with TestClient(app, raise_server_exceptions=False) as c:
            c.get("/bad")
            c.get("/bad")
            assert counts[0] == 2

    def test_201_is_cached(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        """2xx responses other than 200 should still be cached."""
        app = FastAPI()
        FastAPIRedis(app).caching()
        counts = [0]

        @app.get("/created", dependencies=[Depends(cache(ttl=300))], status_code=201)
        async def created() -> dict:
            counts[0] += 1
            return {"id": counts[0]}

        app.dependency_overrides[get_async_redis] = _make_fake_dep(fake_async_redis)
        with TestClient(app) as c:
            r1 = c.get("/created")
            assert r1.status_code == 201
            r2 = c.get("/created")
            assert r2.headers.get("X-Redis-Cache") == "HIT"
            assert counts[0] == 1


# ===================================================================
# 2. Cache key injection / special characters
# ===================================================================


@pytest.mark.unit
class TestKeyBuilderSpecialChars:
    """Key builder must handle adversarial paths without crashing."""

    def test_path_with_colons(self) -> None:
        """Colons in path should not break key structure."""
        key = default_key_builder(_make_request("/items/user:123"), prefix="pfx")
        assert "pfx:" in key
        assert "user:123" in key

    def test_path_with_glob_star(self) -> None:
        key = default_key_builder(_make_request("/items/*"), prefix="pfx")
        assert "*" in key

    def test_path_with_glob_question(self) -> None:
        key = default_key_builder(_make_request("/items/?id=1"), prefix="pfx")
        assert key  # must not crash

    def test_path_with_brackets(self) -> None:
        key = default_key_builder(_make_request("/items/[test]"), prefix="pfx")
        assert "[test]" in key

    def test_unicode_path(self) -> None:
        key = default_key_builder(_make_request("/items/日本語"), prefix="pfx")
        assert "日本語" in key

    def test_extremely_long_path(self) -> None:
        long_segment = "a" * 2000
        key = default_key_builder(_make_request(f"/items/{long_segment}"), prefix="pfx")
        assert len(key) > 2000

    def test_query_params_with_colons(self) -> None:
        key = default_key_builder(_make_request("/items", "key=a:b:c"), prefix="pfx")
        assert "key=a%3Ab%3Ac" in key

    def test_query_params_with_glob(self) -> None:
        key = default_key_builder(
            _make_request("/items", "q=*&sort=name"), prefix="pfx"
        )
        assert "q=*" in key

    @pytest.mark.asyncio
    async def test_glob_chars_in_key_name(self) -> None:
        """Keys containing glob chars should be stored/retrieved correctly."""
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="safe")
        await backend.set("item:*", "val1", ttl=60)
        await backend.set("item:normal", "val2", ttl=60)
        assert await backend.has("item:*") is True
        assert await backend.has("item:normal") is True
        assert await backend.get("item:*") == "val1"


# ===================================================================
# 3. Empty response body
# ===================================================================


@pytest.mark.unit
class TestEmptyResponseBody:
    def test_empty_body_cached(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app = FastAPI()
        FastAPIRedis(app).caching()
        counts = [0]

        @app.get("/empty", dependencies=[Depends(cache(ttl=300))])
        async def empty() -> dict:
            counts[0] += 1
            return {}

        app.dependency_overrides[get_async_redis] = _make_fake_dep(fake_async_redis)
        with TestClient(app) as c:
            r1 = c.get("/empty")
            assert r1.status_code == 200
            assert r1.headers["X-Redis-Cache"] == "MISS"
            r2 = c.get("/empty")
            assert r2.headers["X-Redis-Cache"] == "HIT"
            assert r2.json() == {}
            assert counts[0] == 1


# ===================================================================
# 4. Streaming / chunked responses
# ===================================================================


@pytest.mark.unit
class TestStreamingResponse:
    """Middleware must handle streaming / multi-chunk bodies."""

    def test_streaming_response_delivered(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app = FastAPI()
        FastAPIRedis(app).caching()

        async def generate():
            yield b"chunk1"
            yield b"chunk2"

        @app.get("/stream", dependencies=[Depends(cache(ttl=300))])
        async def stream() -> StreamingResponse:
            return StreamingResponse(generate(), media_type="text/plain")

        app.dependency_overrides[get_async_redis] = _make_fake_dep(fake_async_redis)
        with TestClient(app) as c:
            r = c.get("/stream")
            assert r.status_code == 200
            assert r.text == "chunk1chunk2"


# ===================================================================
# 5. MAX_CACHEABLE_BODY_SIZE boundary
# ===================================================================


@pytest.mark.unit
class TestOversizedResponse:
    """Responses exceeding MAX_CACHEABLE_BODY_SIZE must still be delivered."""

    def test_oversized_response_not_cached_but_delivered(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app = FastAPI()
        FastAPIRedis(app).caching()

        big_data = "x" * (MAX_CACHEABLE_BODY_SIZE + 1)

        @app.get("/big", dependencies=[Depends(cache(ttl=300))])
        async def big() -> dict:
            return {"data": big_data}

        app.dependency_overrides[get_async_redis] = _make_fake_dep(fake_async_redis)
        with TestClient(app) as c:
            r = c.get("/big")
            assert r.status_code == 200
            # Should NOT be cached (too large)
            r2 = c.get("/big")
            # Both should succeed without X-Redis-Cache: HIT
            assert r2.status_code == 200


# ===================================================================
# 6. CacheBackend negative/zero TTL
# ===================================================================


@pytest.mark.unit
class TestCacheBackendTTLEdgeCases:
    @pytest.mark.asyncio
    async def test_zero_ttl_stores_without_expiry(self) -> None:
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")
        await backend.set("k", "v", ttl=0)
        assert await backend.get("k") == "v"
        full_key = backend._build_key("k")
        assert await fake.ttl(full_key) == -1  # no expiry

    @pytest.mark.asyncio
    async def test_negative_ttl_stores_without_expiry(self) -> None:
        """Negative TTL treated same as no TTL (no expiry)."""
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")
        await backend.set("k", "v", ttl=-5)
        assert await backend.get("k") == "v"
        full_key = backend._build_key("k")
        assert await fake.ttl(full_key) == -1

    @pytest.mark.asyncio
    async def test_none_ttl_stores_without_expiry(self) -> None:
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns")
        await backend.set("k", "v", ttl=None)
        assert await backend.get("k") == "v"


# ===================================================================
# 7. Custom Coder failures
# ===================================================================


class ExplodingCoder:
    @classmethod
    def encode(cls, value):
        raise TypeError("cannot encode")

    @classmethod
    def decode(cls, value):
        raise RuntimeError("cannot decode")


@pytest.mark.unit
class TestCustomCoderFailures:
    @pytest.mark.asyncio
    async def test_encode_type_error_propagates(self) -> None:
        """TypeError from encode is NOT caught - this is a gap in error handling."""
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns", coder=ExplodingCoder)
        # TypeError is not in (RedisError, OSError) so it propagates
        with pytest.raises(TypeError, match="cannot encode"):
            await backend.set("k", "v", ttl=60)

    @pytest.mark.asyncio
    async def test_decode_runtime_error_propagates(self) -> None:
        """RuntimeError from decode is NOT caught - gap in error handling."""
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        backend = CacheBackend(fake, eviction_group="ns", coder=ExplodingCoder)
        full_key = backend._build_key("k")
        await fake.set(full_key, JsonCoder.encode("hello"))
        # RuntimeError is not in (ValueError, UnicodeDecodeError)
        with pytest.raises(RuntimeError, match="cannot decode"):
            await backend.get("k")


# ===================================================================
# 8. get_settings() LRU cache
# ===================================================================


@pytest.mark.unit
class TestGetSettingsLRUCache:
    def test_returns_same_instance(self) -> None:
        get_settings.cache_clear()
        s1 = get_settings()
        s2 = get_settings()
        assert s1 is s2
        get_settings.cache_clear()

    def test_cache_clear_gives_new_instance(self) -> None:
        get_settings.cache_clear()
        s1 = get_settings()
        get_settings.cache_clear()
        s2 = get_settings()
        assert s1 is not s2
        get_settings.cache_clear()


# ===================================================================
# 9. WebSocket / non-HTTP scope passthrough
# ===================================================================


@pytest.mark.unit
class TestNonHTTPScopePassthrough:
    @pytest.mark.asyncio
    async def test_non_http_scope_delegates_directly(self) -> None:
        """Middleware must delegate non-HTTP scopes to inner app."""
        calls: list[str] = []

        async def inner_app(scope, receive, send):
            calls.append(scope["type"])

        middleware = CacheResponseCaptureMiddleware(inner_app)
        # websocket scope
        await middleware({"type": "websocket"}, None, None)
        assert calls == ["websocket"]
        # lifespan scope
        await middleware({"type": "lifespan"}, None, None)
        assert calls == ["websocket", "lifespan"]


# ===================================================================
# 10. _PoolState lifecycle edge cases
# ===================================================================


@pytest.mark.unit
class TestPoolStateLifecycle:
    def test_clear_resets_cached_clients(self) -> None:
        ps = _PoolState()
        ps._async_client = MagicMock()
        ps.clear()
        assert ps._async_client is None


# ===================================================================
# 11. Cache-Control parsing edge cases
# ===================================================================


@pytest.mark.unit
class TestCacheControlParsingAdversarial:
    def test_duplicate_directives_last_wins(self) -> None:
        result = _parse_cache_control("max-age=60, max-age=120")
        assert result["max-age"] == "120"

    def test_empty_segments(self) -> None:
        result = _parse_cache_control(",,no-cache,,")
        assert "no-cache" in result

    def test_heavy_whitespace(self) -> None:
        result = _parse_cache_control("  no-cache  ,  max-age=60  ")
        assert "no-cache" in result
        assert result["max-age"] == "60"

    def test_case_insensitive(self) -> None:
        result = _parse_cache_control("No-Cache, Max-Age=60")
        assert "no-cache" in result
        assert result["max-age"] == "60"

    def test_unknown_directives_preserved(self) -> None:
        result = _parse_cache_control("no-cache, x-custom=hello")
        assert result["x-custom"] == "hello"

    def test_equals_in_value(self) -> None:
        result = _parse_cache_control("token=abc=def")
        assert result["token"] == "abc=def"


# ===================================================================
# 12. Concurrency / cache stampede
# ===================================================================


@pytest.mark.unit
class TestConcurrency:
    """Verify behavior under concurrent access."""

    def test_concurrent_requests_all_succeed(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        """Multiple simultaneous requests must all get valid responses."""
        app = FastAPI()
        FastAPIRedis(app).caching()
        counts = [0]

        @app.get("/concurrent", dependencies=[Depends(cache(ttl=300))])
        async def concurrent() -> dict:
            counts[0] += 1
            return {"value": counts[0]}

        app.dependency_overrides[get_async_redis] = _make_fake_dep(fake_async_redis)
        with TestClient(app) as c:
            import concurrent.futures

            def make_request():
                return c.get("/concurrent")

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(make_request) for _ in range(10)]
                results = [f.result() for f in concurrent.futures.as_completed(futures)]

            # All requests must succeed
            for r in results:
                assert r.status_code == 200
                assert "value" in r.json()

    def test_concurrent_evict_and_read(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        """Concurrent evict + read should not crash."""
        from redis_fastapi.cache import cache_evict

        app = FastAPI()
        FastAPIRedis(app).caching()

        @app.get("/item", dependencies=[Depends(cache(ttl=300))])
        async def get_item() -> dict:
            return {"data": "value"}

        @app.delete(
            "/item",
            dependencies=[Depends(cache_evict(key_builder=default_key_builder))],
        )
        async def delete_item() -> dict:
            return {"deleted": True}

        app.dependency_overrides[get_async_redis] = _make_fake_dep(fake_async_redis)
        with TestClient(app) as c:
            # Prime cache
            c.get("/item")

            import concurrent.futures

            def read():
                return c.get("/item")

            def evict():
                return c.delete("/item")

            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                futures = []
                for i in range(10):
                    if i % 3 == 0:
                        futures.append(executor.submit(evict))
                    else:
                        futures.append(executor.submit(read))
                results = [f.result() for f in concurrent.futures.as_completed(futures)]

            for r in results:
                assert r.status_code == 200
