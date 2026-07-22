"""Tests for DI-based caching: cache(), cache_evict(), cache_put()."""

import sys
from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis.aioredis
import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from starlette.requests import Request
from starlette.responses import Response

from redis_fastapi.cache import (
    CacheHitException,
    CachePending,
    _cache_control_value,
    _is_stale_for_client,
    _parse_cache_control,
    cache,
    cache_evict,
    cache_put,
    default_key_builder,
)
from redis_fastapi.config import RedisSettings
from redis_fastapi.deps import get_async_redis
from redis_fastapi.setup import FastAPIRedis

# ---------------------------------------------------------------------------
# Helper: create a test app with DI-based caching wired to fake Redis
# ---------------------------------------------------------------------------


def _make_app(
    fake: fakeredis.aioredis.FakeRedis,
) -> tuple[FastAPI, list[int]]:
    app = FastAPI()
    FastAPIRedis(app).caching()
    counts: list[int] = [0]

    @app.get("/cached", dependencies=[Depends(cache(ttl=300))])
    async def cached_endpoint() -> dict:
        counts[0] += 1
        return {"value": counts[0]}

    async def _fake() -> fakeredis.aioredis.FakeRedis:
        return fake

    app.dependency_overrides[get_async_redis] = _fake
    return app, counts


# ===================================================================
# cache() - MISS / HIT / no-store / no-cache / ETag / non-GET
# ===================================================================


@pytest.mark.unit
class TestCacheMissHit:
    """First GET → MISS with cache headers; second GET → HIT."""

    def test_first_miss_second_hit(self, client: TestClient) -> None:
        r1 = client.get("/cached")
        assert r1.status_code == 200
        assert r1.headers.get("X-Redis-Cache") == "MISS"
        val1 = r1.json()["value"]

        r2 = client.get("/cached")
        assert r2.status_code == 200
        assert r2.headers.get("X-Redis-Cache") == "HIT"
        assert r2.json()["value"] == val1

    def test_cache_control_header_on_miss(self, client: TestClient) -> None:
        r = client.get("/cached")
        assert "max-age=" in r.headers.get("Cache-Control", "")

    def test_etag_header_present(self, client: TestClient) -> None:
        r = client.get("/cached")
        assert r.headers.get("ETag", "").startswith('W/"')


@pytest.mark.unit
class TestNoStore:
    """Cache-Control: no-store → bypass cache entirely."""

    def test_no_store_bypasses_cache(self, client: TestClient) -> None:
        r1 = client.get("/cached")
        assert r1.headers.get("X-Redis-Cache") == "MISS"

        r2 = client.get("/cached", headers={"Cache-Control": "no-store"})
        assert "X-Redis-Cache" not in r2.headers
        assert r2.json()["value"] == r1.json()["value"] + 1


@pytest.mark.unit
class TestNoCache:
    """Cache-Control: no-cache → force refresh (re-execute endpoint)."""

    def test_no_cache_forces_refresh(self, client: TestClient) -> None:
        r1 = client.get("/cached")
        assert r1.headers.get("X-Redis-Cache") == "MISS"
        val1 = r1.json()["value"]

        r2 = client.get("/cached", headers={"Cache-Control": "no-cache"})
        assert r2.headers.get("X-Redis-Cache") == "MISS"
        assert r2.json()["value"] == val1 + 1


@pytest.mark.unit
class TestETagAndNotModified:
    """If-None-Match with matching ETag → 304 Not Modified."""

    def test_if_none_match_returns_304(self, client: TestClient) -> None:
        r1 = client.get("/cached")
        etag = r1.headers["ETag"]
        client.get("/cached")  # prime HIT
        r3 = client.get("/cached", headers={"If-None-Match": etag})
        assert r3.status_code == 304


@pytest.mark.unit
class TestNonGetMethod:
    """Non-GET requests are never subject to cache() read-path."""

    def test_post_not_cached(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app = FastAPI()
        FastAPIRedis(app).caching()
        counts: list[int] = [0]

        @app.post("/action", dependencies=[Depends(cache(ttl=300))])
        async def action() -> dict:
            counts[0] += 1
            return {"value": counts[0]}

        async def _fake() -> fakeredis.aioredis.FakeRedis:
            return fake_async_redis

        app.dependency_overrides[get_async_redis] = _fake
        with TestClient(app) as c:
            r1 = c.post("/action")
            r2 = c.post("/action")
            assert "X-Redis-Cache" not in r1.headers
            assert "X-Redis-Cache" not in r2.headers
            assert r2.json()["value"] == r1.json()["value"] + 1


@pytest.mark.unit
class TestAsyncKeyBuilder:
    """Awaitable key builder is awaited correctly."""

    def test_async_key_builder(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app = FastAPI()
        FastAPIRedis(app).caching()

        async def my_key_builder(request: Request, **kw: object) -> str:
            return "custom-async-key"

        @app.get(
            "/async-kb",
            dependencies=[Depends(cache(ttl=300, key_builder=my_key_builder))],
        )
        async def ep() -> dict:
            return {"ok": True}

        async def _fake() -> fakeredis.aioredis.FakeRedis:
            return fake_async_redis

        app.dependency_overrides[get_async_redis] = _fake
        with TestClient(app) as c:
            r = c.get("/async-kb")
            assert r.status_code == 200
            assert r.headers["X-Redis-Cache"] == "MISS"

            r2 = c.get("/async-kb")
            assert r2.headers["X-Redis-Cache"] == "HIT"


# ===================================================================
# Redis error resilience
# ===================================================================


@pytest.mark.unit
class TestRedisErrors:
    """Redis errors handled gracefully - endpoint still returns 200."""

    def test_read_error_falls_through_to_miss(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app, counts = _make_app(fake_async_redis)
        with TestClient(app) as c:
            c.get("/cached")
            assert counts[0] == 1

            # Pipeline-based read: patch pipeline().execute() to raise
            bad_pipe = AsyncMock()
            bad_pipe.get = MagicMock(return_value=bad_pipe)
            bad_pipe.ttl = MagicMock(return_value=bad_pipe)
            bad_pipe.execute = AsyncMock(side_effect=ConnectionError("boom"))
            with patch.object(
                fake_async_redis,
                "pipeline",
                return_value=bad_pipe,
            ):
                r = c.get("/cached")
                assert r.status_code == 200
                assert r.headers["X-Redis-Cache"] == "MISS"
                assert counts[0] == 2

    def test_write_error_still_returns_response(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app, _ = _make_app(fake_async_redis)
        with patch.object(
            fake_async_redis,
            "set",
            AsyncMock(side_effect=ConnectionError("boom")),
        ):
            with TestClient(app) as c:
                r = c.get("/cached")
                assert r.status_code == 200
                assert r.json()["value"] == 1
                assert r.headers["X-Redis-Cache"] == "MISS"


# ===================================================================
# Prefix / settings overrides
# ===================================================================


@pytest.mark.unit
class TestCachePrefixOverride:
    """cache(prefix=...) injects a custom prefix into the key."""

    def test_custom_prefix_used_in_key(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        captured: list[str] = []
        app = FastAPI()
        FastAPIRedis(app).caching()

        def spy_kb(request: Request, **kw: object) -> str:
            key = default_key_builder(request, **kw)
            captured.append(key)
            return key

        @app.get(
            "/items",
            dependencies=[
                Depends(cache(ttl=300, cache_prefix="myapp:custom", key_builder=spy_kb))
            ],
        )
        async def ep() -> dict:
            return {"ok": True}

        async def _fake() -> fakeredis.aioredis.FakeRedis:
            return fake_async_redis

        app.dependency_overrides[get_async_redis] = _fake
        with TestClient(app) as c:
            c.get("/items")
        assert len(captured) == 1
        assert captured[0].startswith("myapp:custom:")

    def test_default_prefix_from_settings(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        captured: list[str] = []
        app = FastAPI()
        FastAPIRedis(app).caching()

        def spy_kb(request: Request, **kw: object) -> str:
            key = default_key_builder(request, **kw)
            captured.append(key)
            return key

        @app.get(
            "/items",
            dependencies=[Depends(cache(ttl=300, key_builder=spy_kb))],
        )
        async def ep() -> dict:
            return {"ok": True}

        async def _fake() -> fakeredis.aioredis.FakeRedis:
            return fake_async_redis

        app.dependency_overrides[get_async_redis] = _fake
        with TestClient(app) as c:
            c.get("/items")
        assert captured[0].startswith("redis:fastapi:cache:")

    def test_custom_ttl_from_settings(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        """cache() with no explicit ttl uses settings.default_ttl."""
        custom = RedisSettings(default_ttl=999)
        app = FastAPI()
        FastAPIRedis(app).caching()

        cache_module = sys.modules["redis_fastapi.cache"]
        with patch.object(cache_module, "get_settings", return_value=custom):

            @app.get("/items", dependencies=[Depends(cache())])
            async def ep() -> dict:
                return {"ok": True}

        async def _fake() -> fakeredis.aioredis.FakeRedis:
            return fake_async_redis

        app.dependency_overrides[get_async_redis] = _fake
        with TestClient(app) as c:
            r = c.get("/items")
            assert r.status_code == 200
            assert "max-age=999" in r.headers["Cache-Control"]


# ===================================================================
# Corrupt cache entry
# ===================================================================


@pytest.mark.unit
class TestCorruptCacheEntry:
    """Invalid JSON stored in Redis is treated as a MISS."""

    def test_corrupt_entry_falls_through(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app, counts = _make_app(fake_async_redis)
        with TestClient(app) as c:
            c.get("/cached")
            assert counts[0] == 1

            # Pipeline returns corrupt JSON from execute()
            bad_pipe = AsyncMock()
            bad_pipe.get = MagicMock(return_value=bad_pipe)
            bad_pipe.ttl = MagicMock(return_value=bad_pipe)
            bad_pipe.execute = AsyncMock(return_value=["not-valid-json{{{", 100])
            with patch.object(
                fake_async_redis,
                "pipeline",
                return_value=bad_pipe,
            ):
                r2 = c.get("/cached")
                assert r2.status_code == 200
                assert counts[0] == 2


# ===================================================================
# cache_evict() DI factory
# ===================================================================


@pytest.mark.unit
class TestCacheEvict:
    """Unit tests for the cache_evict() DI factory."""

    def test_evict_with_key_builder(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app = FastAPI()
        FastAPIRedis(app).caching()
        counts: list[int] = [0]

        @app.get(
            "/products/{product_id}",
            dependencies=[Depends(cache(ttl=300, eviction_group="products"))],
        )
        async def get_product(product_id: int) -> dict:
            counts[0] += 1
            return {"id": product_id, "v": counts[0]}

        @app.delete(
            "/products/{product_id}",
            dependencies=[
                Depends(
                    cache_evict(
                        eviction_group="products", key_builder=default_key_builder
                    )
                )
            ],
        )
        async def delete_product(product_id: int) -> dict:
            return {"deleted": product_id}

        async def _fake() -> fakeredis.aioredis.FakeRedis:
            return fake_async_redis

        app.dependency_overrides[get_async_redis] = _fake
        with TestClient(app) as c:
            r1 = c.get("/products/42")
            assert r1.headers["X-Redis-Cache"] == "MISS"
            r2 = c.get("/products/42")
            assert r2.headers["X-Redis-Cache"] == "HIT"
            c.delete("/products/42")
            r4 = c.get("/products/42")
            assert r4.headers["X-Redis-Cache"] == "MISS"
            assert r4.json()["v"] == 2

    def test_evict_group_when_no_key_builder(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        from redis_fastapi.cache_backend import CacheBackend

        backend = CacheBackend(fake_async_redis, eviction_group="ns")
        app = FastAPI()
        FastAPIRedis(app).caching()

        @app.post("/seed/{key}")
        async def seed(key: str) -> dict:
            await backend.set(key, "v", ttl=300, eviction_group="items")
            return {"ok": True}

        @app.get("/has/{key}")
        async def has_key(key: str) -> dict:
            return {"exists": await backend.has(key, eviction_group="items")}

        @app.post(
            "/clear",
            dependencies=[Depends(cache_evict(eviction_group="items"))],
        )
        async def clear_items() -> dict:
            return {"ok": True}

        async def _fake() -> fakeredis.aioredis.FakeRedis:
            return fake_async_redis

        app.dependency_overrides[get_async_redis] = _fake
        with TestClient(app) as c:
            c.post("/seed/a")
            c.post("/seed/b")
            c.post("/clear")
            assert c.get("/has/a").json()["exists"] is False
            assert c.get("/has/b").json()["exists"] is False

    def test_evict_not_called_on_endpoint_exception(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app = FastAPI()
        FastAPIRedis(app).caching()
        counts: list[int] = [0]

        @app.get(
            "/items/{id}",
            dependencies=[Depends(cache(ttl=300))],
        )
        async def get_item(id: int) -> dict:
            counts[0] += 1
            return {"id": id, "v": counts[0]}

        @app.delete(
            "/items/{id}",
            dependencies=[
                Depends(cache_evict(eviction_group="", key_builder=default_key_builder))
            ],
        )
        async def broken_delete(id: int) -> dict:
            raise ValueError("endpoint error")  # noqa: EM101

        async def _fake() -> fakeredis.aioredis.FakeRedis:
            return fake_async_redis

        app.dependency_overrides[get_async_redis] = _fake
        with TestClient(app, raise_server_exceptions=False) as c:
            c.get("/items/1")
            c.get("/items/1")  # HIT
            c.delete("/items/1")  # endpoint raises → eviction skipped
            r = c.get("/items/1")
            assert r.headers["X-Redis-Cache"] == "HIT"

    def test_evict_no_group_no_key_builder_raises(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        """cache_evict() with no eviction_group and no key_builder raises ValueError."""
        with pytest.raises(ValueError, match="cache_evict\\(\\) requires"):
            cache_evict()

    def test_evict_all_with_explicit_group(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        """Use an explicit eviction_group to wipe all keys under a prefix."""
        from redis_fastapi.cache_backend import CacheBackend

        backend = CacheBackend(fake_async_redis, eviction_group="ns")
        app = FastAPI()
        FastAPIRedis(app).caching()

        @app.post("/seed/{grp}/{key}")
        async def seed(grp: str, key: str) -> dict:
            await backend.set(key, "v", ttl=300, eviction_group=grp)
            return {"ok": True}

        @app.get("/has/{grp}/{key}")
        async def has_key(grp: str, key: str) -> dict:
            return {"exists": await backend.has(key, eviction_group=grp)}

        @app.post("/wipe/{grp}")
        async def wipe(grp: str) -> dict:
            await backend.delete_group(grp)
            return {"ok": True}

        async def _fake() -> fakeredis.aioredis.FakeRedis:
            return fake_async_redis

        app.dependency_overrides[get_async_redis] = _fake
        with TestClient(app) as c:
            c.post("/seed/alpha/k1")
            c.post("/seed/beta/k2")
            assert c.get("/has/alpha/k1").json()["exists"] is True
            assert c.get("/has/beta/k2").json()["exists"] is True

            c.post("/wipe/alpha")
            c.post("/wipe/beta")

            assert c.get("/has/alpha/k1").json()["exists"] is False
            assert c.get("/has/beta/k2").json()["exists"] is False


# ===================================================================
# cache_put() DI factory
# ===================================================================


@pytest.mark.unit
class TestCachePut:
    """Unit tests for the cache_put() DI factory."""

    def test_put_writes_same_key_as_cache(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app = FastAPI()
        FastAPIRedis(app).caching()
        handler_counts: list[int] = [0]

        @app.get(
            "/products/{product_id}",
            dependencies=[Depends(cache(ttl=300, eviction_group="products"))],
        )
        async def get_product(product_id: int) -> dict:
            handler_counts[0] += 1
            return {"id": product_id, "v": handler_counts[0]}

        @app.put(
            "/products/{product_id}",
            dependencies=[
                Depends(
                    cache_put(
                        eviction_group="products",
                        key_builder=default_key_builder,
                        ttl=300,
                    )
                )
            ],
        )
        async def update_product(product_id: int) -> dict:
            return {"id": product_id, "updated": True}

        async def _fake() -> fakeredis.aioredis.FakeRedis:
            return fake_async_redis

        app.dependency_overrides[get_async_redis] = _fake
        with TestClient(app) as c:
            r1 = c.get("/products/42")
            assert r1.headers["X-Redis-Cache"] == "MISS"
            c.put("/products/42")
            r3 = c.get("/products/42")
            assert r3.headers["X-Redis-Cache"] == "HIT"
            assert r3.json() == {"id": 42, "updated": True}

    def test_put_defaults_to_default_key_builder(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app = FastAPI()
        FastAPIRedis(app).caching()

        @app.get(
            "/items/{item_id}",
            dependencies=[Depends(cache(ttl=300, eviction_group="items"))],
        )
        async def get_item(item_id: int) -> dict:
            return {"id": item_id, "original": True}

        @app.put(
            "/items/{item_id}",
            dependencies=[Depends(cache_put(eviction_group="items", ttl=300))],
        )
        async def update_item(item_id: int) -> dict:
            return {"id": item_id, "updated": True}

        async def _fake() -> fakeredis.aioredis.FakeRedis:
            return fake_async_redis

        app.dependency_overrides[get_async_redis] = _fake
        with TestClient(app) as c:
            c.get("/items/1")
            c.put("/items/1")
            r = c.get("/items/1")
            assert r.headers["X-Redis-Cache"] == "HIT"
            assert r.json() == {"id": 1, "updated": True}


# ===================================================================
# Pure helper-function tests
# ===================================================================


def _should_skip(request: Request) -> bool:
    """Inline helper reproducing the skip logic from ``cache()``."""
    if request.method != "GET":
        return True
    cc = _parse_cache_control(request.headers.get("Cache-Control"))
    return "no-store" in cc


@pytest.mark.unit
class TestShouldSkip:
    """Skip logic: True for non-GET or no-store."""

    def test_non_get_skipped(self) -> None:
        scope = {
            "type": "http",
            "method": "POST",
            "path": "/",
            "query_string": b"",
            "headers": [],
        }
        assert _should_skip(Request(scope)) is True

    def test_no_store_skipped(self) -> None:
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/",
            "query_string": b"",
            "headers": [(b"cache-control", b"no-store")],
        }
        assert _should_skip(Request(scope)) is True

    def test_get_not_skipped(self) -> None:
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/",
            "query_string": b"",
            "headers": [],
        }
        assert _should_skip(Request(scope)) is False


@pytest.mark.unit
class TestParseCacheControl:
    def test_empty(self) -> None:
        assert _parse_cache_control(None) == {}
        assert _parse_cache_control("") == {}

    def test_boolean_directives(self) -> None:
        assert _parse_cache_control("no-cache, no-store") == {
            "no-cache": True,
            "no-store": True,
        }

    def test_value_directives(self) -> None:
        assert _parse_cache_control("max-age=60") == {"max-age": "60"}

    def test_mixed_directives(self) -> None:
        assert _parse_cache_control("public, max-age=300, no-transform") == {
            "public": True,
            "max-age": "300",
            "no-transform": True,
        }


@pytest.mark.unit
class TestIsStaleForClient:
    def test_no_max_age(self) -> None:
        assert _is_stale_for_client(200, 300, {}) is False

    def test_fresh_enough(self) -> None:
        assert _is_stale_for_client(200, 300, {"max-age": "120"}) is False

    def test_at_boundary_is_stale(self) -> None:
        assert _is_stale_for_client(180, 300, {"max-age": "120"}) is True

    def test_too_old(self) -> None:
        assert _is_stale_for_client(100, 300, {"max-age": "60"}) is True

    def test_max_age_zero(self) -> None:
        assert _is_stale_for_client(299, 300, {"max-age": "0"}) is True

    def test_invalid_max_age(self) -> None:
        assert _is_stale_for_client(200, 300, {"max-age": "abc"}) is False


@pytest.mark.unit
class TestCacheControlValue:
    def test_public(self) -> None:
        assert _cache_control_value(60, False) == "max-age=60"

    def test_private(self) -> None:
        assert _cache_control_value(60, True) == "private, max-age=60"

    def test_zero_ttl_public(self) -> None:
        assert _cache_control_value(0, False) == "no-cache"

    def test_zero_ttl_private(self) -> None:
        assert _cache_control_value(0, True) == "private, no-cache"


# ===================================================================
# Client max-age
# ===================================================================


@pytest.mark.unit
class TestRequestMaxAge:
    def test_max_age_zero_forces_refresh(self, client: TestClient) -> None:
        r1 = client.get("/cached")
        val1 = r1.json()["value"]
        client.get("/cached")  # HIT
        r3 = client.get("/cached", headers={"Cache-Control": "max-age=0"})
        assert r3.headers.get("X-Redis-Cache") == "MISS"
        assert r3.json()["value"] == val1 + 1

    def test_large_max_age_allows_hit(self, client: TestClient) -> None:
        r1 = client.get("/cached")
        val1 = r1.json()["value"]
        r2 = client.get("/cached", headers={"Cache-Control": "max-age=9999"})
        assert r2.headers.get("X-Redis-Cache") == "HIT"
        assert r2.json()["value"] == val1


# ===================================================================
# private directive
# ===================================================================


@pytest.mark.unit
class TestPrivateDirective:
    def test_private_cache_control_header(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app = FastAPI()
        FastAPIRedis(app).caching()

        @app.get(
            "/private",
            dependencies=[Depends(cache(ttl=120, private=True))],
        )
        async def private_endpoint() -> dict:
            return {"secret": "data"}

        async def _fake() -> fakeredis.aioredis.FakeRedis:
            return fake_async_redis

        app.dependency_overrides[get_async_redis] = _fake
        with TestClient(app) as c:
            r1 = c.get("/private")
            assert "private" in r1.headers["Cache-Control"]
            assert "max-age=120" in r1.headers["Cache-Control"]
            r2 = c.get("/private")
            assert "private" in r2.headers["Cache-Control"]

    def test_default_no_private(self, client: TestClient) -> None:
        cc = client.get("/cached").headers.get("Cache-Control", "")
        assert "private" not in cc
        assert "max-age=" in cc


# ===================================================================
# CacheHitException / CachePending / Redis builder
# ===================================================================


@pytest.mark.unit
class TestCacheHitException:
    def test_stores_response(self) -> None:
        resp = Response(content="cached", status_code=200)
        exc = CacheHitException(resp)
        assert exc.response is resp

    def test_cache_hit_marker(self) -> None:
        exc = CacheHitException(Response(content="x"))
        assert exc.__cache_hit__ is True

    def test_suppress_context(self) -> None:
        exc = CacheHitException(Response(content="x"))
        assert exc.__suppress_context__ is True


@pytest.mark.unit
class TestCachePendingDataclass:
    def test_defaults(self) -> None:
        p = CachePending(key="k", ttl=60)
        assert p.key == "k"
        assert p.ttl == 60
        assert p.private is False
        assert p.redis is None

    def test_with_redis(self) -> None:
        sentinel = object()
        p = CachePending(key="k", ttl=60, private=True, redis=sentinel)
        assert p.private is True
        assert p.redis is sentinel


@pytest.mark.unit
class TestRedisBuilderCaching:
    def test_registers_middleware(self) -> None:
        from redis_fastapi.cache import CacheResponseCaptureMiddleware

        app = FastAPI()
        FastAPIRedis(app).caching()
        assert any(m.cls is CacheResponseCaptureMiddleware for m in app.user_middleware)

    def test_end_to_end(self, fake_async_redis: fakeredis.aioredis.FakeRedis) -> None:
        app, _ = _make_app(fake_async_redis)
        with TestClient(app) as c:
            assert c.get("/cached").headers["X-Redis-Cache"] == "MISS"
            assert c.get("/cached").headers["X-Redis-Cache"] == "HIT"


# ===================================================================
# Edge-case coverage: async key_builder in evict/put, evict error path,
# middleware fallback when CachePending.redis is None
# ===================================================================


@pytest.mark.unit
class TestCacheEvictAsyncKeyBuilder:
    """cache_evict() with an async key_builder awaits the coroutine."""

    def test_async_key_builder_in_evict(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app = FastAPI()
        FastAPIRedis(app).caching()

        async def async_kb(request: Request, **kw: object) -> str:
            return default_key_builder(request, **kw)

        @app.get(
            "/items/{id}",
            dependencies=[Depends(cache(ttl=300, key_builder=async_kb))],
        )
        async def get_item(id: int) -> dict:
            return {"id": id}

        @app.delete(
            "/items/{id}",
            dependencies=[Depends(cache_evict(key_builder=async_kb))],
        )
        async def delete_item(id: int) -> dict:
            return {"deleted": id}

        async def _fake() -> fakeredis.aioredis.FakeRedis:
            return fake_async_redis

        app.dependency_overrides[get_async_redis] = _fake
        with TestClient(app) as c:
            c.get("/items/1")
            assert c.get("/items/1").headers["X-Redis-Cache"] == "HIT"
            c.delete("/items/1")
            assert c.get("/items/1").headers["X-Redis-Cache"] == "MISS"


@pytest.mark.unit
class TestCacheEvictErrorPath:
    """cache_evict() logs warning when Redis delete fails."""

    def test_evict_redis_error_logged(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app = FastAPI()
        FastAPIRedis(app).caching()

        @app.get(
            "/items/{id}",
            dependencies=[Depends(cache(ttl=300))],
        )
        async def get_item(id: int) -> dict:
            return {"id": id}

        @app.delete(
            "/items/{id}",
            dependencies=[Depends(cache_evict(key_builder=default_key_builder))],
        )
        async def delete_item(id: int) -> dict:
            return {"deleted": id}

        async def _fake() -> fakeredis.aioredis.FakeRedis:
            return fake_async_redis

        app.dependency_overrides[get_async_redis] = _fake
        with TestClient(app) as c:
            c.get("/items/1")
            with patch.object(
                fake_async_redis,
                "delete",
                AsyncMock(side_effect=ConnectionError("boom")),
            ):
                # Should not raise - error is logged, response still returned
                r = c.delete("/items/1")
                assert r.status_code == 200
            # Cache was NOT evicted because delete failed
            assert c.get("/items/1").headers["X-Redis-Cache"] == "HIT"


@pytest.mark.unit
class TestCachePutAsyncKeyBuilder:
    """cache_put() with an async key_builder awaits the coroutine."""

    def test_async_key_builder_in_put(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app = FastAPI()
        FastAPIRedis(app).caching()

        async def async_kb(request: Request, **kw: object) -> str:
            return default_key_builder(request, **kw)

        @app.get(
            "/items/{id}",
            dependencies=[Depends(cache(ttl=300, key_builder=async_kb))],
        )
        async def get_item(id: int) -> dict:
            return {"id": id, "original": True}

        @app.put(
            "/items/{id}",
            dependencies=[Depends(cache_put(ttl=300, key_builder=async_kb))],
        )
        async def update_item(id: int) -> dict:
            return {"id": id, "updated": True}

        async def _fake() -> fakeredis.aioredis.FakeRedis:
            return fake_async_redis

        app.dependency_overrides[get_async_redis] = _fake
        with TestClient(app) as c:
            c.get("/items/1")
            c.put("/items/1")
            r = c.get("/items/1")
            assert r.headers["X-Redis-Cache"] == "HIT"
            assert r.json() == {"id": 1, "updated": True}


@pytest.mark.unit
class TestMiddlewareWriteError:
    """CacheResponseCaptureMiddleware logs warning on Redis write failure."""

    def test_write_failure_still_returns_response(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app, _ = _make_app(fake_async_redis)
        with patch.object(
            fake_async_redis,
            "set",
            AsyncMock(side_effect=ConnectionError("boom")),
        ):
            with TestClient(app) as c:
                r = c.get("/cached")
                assert r.status_code == 200
                assert r.json()["value"] == 1
                # Headers still show MISS (write failed but response delivered)
                assert r.headers["X-Redis-Cache"] == "MISS"


@pytest.mark.unit
class TestMiddlewarePendingRedisNone:
    """Middleware falls back to _get_pool_state() when CachePending.redis is None."""

    def test_pending_with_no_redis_uses_pool_state(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        cache_mod = sys.modules["redis_fastapi.cache"]

        app = FastAPI()
        FastAPIRedis(app).caching()

        @app.get("/manual")
        async def manual(request: Request) -> dict:
            # Manually set CachePending with redis=None to trigger fallback
            request.state.redis_cache_pending = CachePending(
                key="test:manual", ttl=60, redis=None
            )
            return {"manual": True}

        # Build a mock _PoolState whose get_async_client returns our fake redis.
        mock_ps = MagicMock()
        mock_ps.get_async_client.return_value = fake_async_redis

        with patch.object(cache_mod, "_get_pool_state", return_value=mock_ps):
            with TestClient(app) as c:
                r = c.get("/manual")
                assert r.status_code == 200
                assert r.json() == {"manual": True}
                assert r.headers["X-Redis-Cache"] == "MISS"


# ===================================================================
# Redis builder
# ===================================================================


@pytest.mark.unit
class TestRedisBuilder:
    """Unit tests for the FastAPIRedis builder fluent API."""

    def test_caching_returns_self(self) -> None:
        app = FastAPI()
        builder = FastAPIRedis(app)
        assert builder.caching() is builder

    def test_lifespan_returns_self(self) -> None:
        app = FastAPI()
        builder = FastAPIRedis(app)
        assert builder.lifespan() is builder

    def test_full_chain(self) -> None:
        from redis_fastapi.cache import CacheResponseCaptureMiddleware

        app = FastAPI()
        result = FastAPIRedis(app).lifespan().caching()
        assert isinstance(result, FastAPIRedis)
        assert any(m.cls is CacheResponseCaptureMiddleware for m in app.user_middleware)

    def test_wraps_existing_lifespan(self) -> None:
        """Builder preserves existing lifespan by wrapping it."""
        app = FastAPI()
        original = app.router.lifespan_context
        FastAPIRedis(app).lifespan()
        assert app.router.lifespan_context is not original

    def test_otel_returns_self(self) -> None:
        app = FastAPI()
        builder = FastAPIRedis(app)
        assert builder.otel() is builder

    def test_otel_calls_enable_telemetry(self) -> None:
        with patch("redis_fastapi.telemetry.enable_telemetry") as mock_enable:
            app = FastAPI()
            FastAPIRedis(app).otel()
            mock_enable.assert_called_once()

    def test_full_chain_with_otel(self) -> None:
        from redis_fastapi.cache import CacheResponseCaptureMiddleware

        app = FastAPI()
        result = FastAPIRedis(app).lifespan().caching().otel()
        assert isinstance(result, FastAPIRedis)
        assert any(m.cls is CacheResponseCaptureMiddleware for m in app.user_middleware)

    def test_caching_idempotent(self) -> None:
        """Calling .caching() twice does not register duplicate middleware."""
        from redis_fastapi.cache import CacheResponseCaptureMiddleware

        app = FastAPI()
        builder = FastAPIRedis(app)
        builder.caching()
        builder.caching()
        cache_middleware = [
            m for m in app.user_middleware if m.cls is CacheResponseCaptureMiddleware
        ]
        assert len(cache_middleware) == 1

    def test_lifespan_idempotent(self) -> None:
        """Calling .lifespan() twice does not double-wrap the lifespan."""
        app = FastAPI()
        builder = FastAPIRedis(app)
        builder.lifespan()
        wrapped_once = app.router.lifespan_context
        builder.lifespan()
        assert app.router.lifespan_context is wrapped_once

    def test_idempotency_across_builder_instances(self) -> None:
        """Two FastAPIRedis() builders for the same app share idempotency state."""
        from redis_fastapi.cache import CacheResponseCaptureMiddleware

        app = FastAPI()
        FastAPIRedis(app).caching()
        FastAPIRedis(app).caching()
        cache_middleware = [
            m for m in app.user_middleware if m.cls is CacheResponseCaptureMiddleware
        ]
        assert len(cache_middleware) == 1


# ===================================================================
# Sync endpoint compatibility tests
# ===================================================================


def _make_sync_app(
    fake: fakeredis.aioredis.FakeRedis,
) -> tuple[FastAPI, list[int]]:
    """Build a minimal app with SYNC endpoints using cache DI factories."""
    app = FastAPI()
    FastAPIRedis(app).caching()
    counts: list[int] = [0]

    @app.get("/sync-cached", dependencies=[Depends(cache(ttl=300))])
    def sync_cached_endpoint() -> dict:  # NOTE: sync def, not async def
        counts[0] += 1
        return {"value": counts[0]}

    @app.get(
        "/sync-cached-ns",
        dependencies=[Depends(cache(ttl=300, eviction_group="syncns"))],
    )
    def sync_cached_ns() -> dict:
        counts[0] += 1
        return {"ns_value": counts[0]}

    async def _fake() -> fakeredis.aioredis.FakeRedis:
        return fake

    app.dependency_overrides[get_async_redis] = _fake
    return app, counts


@pytest.mark.unit
class TestSyncEndpointCache:
    """Verify cache() works correctly with sync (non-async) endpoints."""

    def test_sync_miss_then_hit(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app, counts = _make_sync_app(fake_async_redis)
        with TestClient(app) as c:
            r1 = c.get("/sync-cached")
            assert r1.status_code == 200
            assert r1.headers.get("X-Redis-Cache") == "MISS"
            val1 = r1.json()["value"]

            r2 = c.get("/sync-cached")
            assert r2.status_code == 200
            assert r2.headers.get("X-Redis-Cache") == "HIT"
            assert r2.json()["value"] == val1

    def test_sync_etag_304(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app, _ = _make_sync_app(fake_async_redis)
        with TestClient(app) as c:
            r1 = c.get("/sync-cached")
            etag = r1.headers["ETag"]
            c.get("/sync-cached")  # prime HIT
            r3 = c.get("/sync-cached", headers={"If-None-Match": etag})
            assert r3.status_code == 304

    def test_sync_no_store_bypass(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app, counts = _make_sync_app(fake_async_redis)
        with TestClient(app) as c:
            r1 = c.get("/sync-cached")
            assert r1.headers.get("X-Redis-Cache") == "MISS"
            r2 = c.get("/sync-cached", headers={"Cache-Control": "no-store"})
            assert "X-Redis-Cache" not in r2.headers
            assert r2.json()["value"] == r1.json()["value"] + 1

    def test_sync_no_cache_refresh(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app, counts = _make_sync_app(fake_async_redis)
        with TestClient(app) as c:
            r1 = c.get("/sync-cached")
            val1 = r1.json()["value"]
            r2 = c.get("/sync-cached", headers={"Cache-Control": "no-cache"})
            assert r2.headers.get("X-Redis-Cache") == "MISS"
            assert r2.json()["value"] == val1 + 1

    def test_sync_eviction_group_isolation(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app, _ = _make_sync_app(fake_async_redis)
        with TestClient(app) as c:
            c.get("/sync-cached")
            c.get("/sync-cached-ns")
            r1 = c.get("/sync-cached")
            r2 = c.get("/sync-cached-ns")
            assert r1.headers.get("X-Redis-Cache") == "HIT"
            assert r2.headers.get("X-Redis-Cache") == "HIT"


@pytest.mark.unit
class TestSyncEndpointCacheEvict:
    """Verify cache_evict() works with sync endpoints."""

    def test_sync_evict_with_key_builder(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app = FastAPI()
        FastAPIRedis(app).caching()
        counts: list[int] = [0]

        @app.get(
            "/products/{product_id}",
            dependencies=[Depends(cache(ttl=300, eviction_group="products"))],
        )
        def sync_get_product(product_id: int) -> dict:
            counts[0] += 1
            return {"id": product_id, "v": counts[0]}

        @app.delete(
            "/products/{product_id}",
            dependencies=[
                Depends(
                    cache_evict(
                        eviction_group="products", key_builder=default_key_builder
                    )
                )
            ],
        )
        def sync_delete_product(product_id: int) -> dict:
            return {"deleted": product_id}

        async def _fake() -> fakeredis.aioredis.FakeRedis:
            return fake_async_redis

        app.dependency_overrides[get_async_redis] = _fake
        with TestClient(app) as c:
            r1 = c.get("/products/42")
            assert r1.headers["X-Redis-Cache"] == "MISS"
            r2 = c.get("/products/42")
            assert r2.headers["X-Redis-Cache"] == "HIT"
            c.delete("/products/42")
            r4 = c.get("/products/42")
            assert r4.headers["X-Redis-Cache"] == "MISS"
            assert r4.json()["v"] == 2


@pytest.mark.unit
class TestSyncEndpointCachePut:
    """Verify cache_put() works with sync endpoints."""

    def test_sync_put_writes_through(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app = FastAPI()
        FastAPIRedis(app).caching()

        @app.get(
            "/items/{item_id}",
            dependencies=[Depends(cache(ttl=300, eviction_group="items"))],
        )
        def sync_get_item(item_id: int) -> dict:
            return {"id": item_id, "original": True}

        @app.put(
            "/items/{item_id}",
            dependencies=[Depends(cache_put(eviction_group="items", ttl=300))],
        )
        def sync_update_item(item_id: int) -> dict:
            return {"id": item_id, "updated": True}

        async def _fake() -> fakeredis.aioredis.FakeRedis:
            return fake_async_redis

        app.dependency_overrides[get_async_redis] = _fake
        with TestClient(app) as c:
            c.get("/items/1")
            c.put("/items/1")
            r = c.get("/items/1")
            assert r.headers["X-Redis-Cache"] == "HIT"
            assert r.json() == {"id": 1, "updated": True}
