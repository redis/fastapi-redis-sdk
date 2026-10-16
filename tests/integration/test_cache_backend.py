"""Integration tests for CacheBackend DI operations.

Tests basic get/set round-trips, default/fallback values, has() existence
checks, timedelta TTL, eviction-group bulk-clear, and SyncCacheBackendDep.
"""

from __future__ import annotations

from collections.abc import Generator
from datetime import timedelta

import pytest
import redis as sync_redis
import redis.asyncio as async_redis
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel

from redis_fastapi import CacheBackend, CacheBackendDep, FastAPIRedis
from redis_fastapi.deps import SyncCacheBackendDep
from tests.conftest import requires_redis

# Track endpoint call counts to prove cache hits skip computation.
_counters: dict[str, int] = {}


def _increment(key: str) -> int:
    _counters[key] = _counters.get(key, 0) + 1
    return _counters[key]


class Product(BaseModel):
    id: int
    name: str
    price: float


def _build_app() -> FastAPI:
    app = FastAPI()
    FastAPIRedis(app).lifespan()

    @app.get("/products/{product_id}")
    async def get_product(product_id: int, cache: CacheBackendDep) -> Product:
        cached = await cache.get(f"product:{product_id}", eviction_group="products")
        if cached is not None:
            return Product(**cached)
        product = Product(
            id=product_id,
            name=f"Product {product_id}",
            price=10.0 * _increment(f"product:{product_id}"),
        )
        await cache.set(
            f"product:{product_id}",
            product.model_dump(),
            ttl=300,
            eviction_group="products",
        )
        return product

    # ---- default / fallback values ----

    @app.put("/settings/{key}")
    async def set_setting(key: str, cache: CacheBackendDep) -> dict:
        value = f"custom-{key}"
        await cache.set(f"setting:{key}", value, ttl=300, eviction_group="settings")
        return {"value": value}

    @app.get("/settings/{key}")
    async def get_setting(key: str, cache: CacheBackendDep) -> dict:
        _increment(f"setting:{key}")
        value = await cache.get(
            f"setting:{key}", default="default-value", eviction_group="settings"
        )
        return {"key": key, "value": value}

    # ---- has() existence check ----

    @app.get("/check-warm/{product_id}")
    async def check_warm(product_id: int, cache: CacheBackendDep) -> dict:
        _increment(f"check-warm:{product_id}")
        is_warm = await cache.has(f"product:{product_id}", eviction_group="products")
        if is_warm:
            return {"warm": True, "product": None}
        _increment(f"expensive-work:{product_id}")
        return {"warm": False, "product": product_id}

    # ---- timedelta TTL ----

    @app.get("/session/{session_id}")
    async def get_session(session_id: str, cache: CacheBackendDep) -> dict:
        cached = await cache.get(f"sess:{session_id}", eviction_group="sessions")
        if cached is not None:
            return cached
        session = {
            "session_id": session_id,
            "counter": _increment(f"sess:{session_id}"),
        }
        await cache.set(
            f"sess:{session_id}",
            session,
            ttl=timedelta(minutes=30),
            eviction_group="sessions",
        )
        return session

    # ---- eviction-group bulk clear ----

    @app.delete("/cache/{group}")
    async def clear_group(group: str, cache: CacheBackendDep) -> dict:
        deleted = await cache.delete_group(group)
        return {"deleted": deleted}

    @app.delete("/cache-all")
    async def clear_all(cache: CacheBackendDep) -> dict:
        """Wipe all keys — no eviction group specified."""
        deleted = await cache.delete_group()
        return {"deleted": deleted}

    return app


@pytest.fixture()
def client(real_redis: sync_redis.Redis) -> Generator[TestClient, None, None]:
    _counters.clear()
    app = _build_app()
    with TestClient(app) as c:
        yield c
    real_redis.flushdb()


# ---------------------------------------------------------------------------
# Basic get / set round-trip
# ---------------------------------------------------------------------------


@requires_redis
@pytest.mark.integration
class TestDICacheHitMiss:
    """Basic DI caching: get/set round-trip."""

    def test_first_call_misses_second_hits(self, client: TestClient) -> None:
        r1 = client.get("/products/1")
        assert r1.json()["price"] == 10.0
        assert _counters["product:1"] == 1

        r2 = client.get("/products/1")
        assert r2.json()["price"] == 10.0
        assert _counters["product:1"] == 1

    def test_different_ids_are_separate(self, client: TestClient) -> None:
        client.get("/products/1")
        client.get("/products/2")
        assert _counters["product:1"] == 1
        assert _counters["product:2"] == 1

        client.get("/products/1")
        client.get("/products/2")
        assert _counters["product:1"] == 1
        assert _counters["product:2"] == 1

    def test_pydantic_model_round_trip(self, client: TestClient) -> None:
        r1 = client.get("/products/42")
        r2 = client.get("/products/42")
        assert r1.json() == r2.json()
        assert r2.json()["id"] == 42
        assert r2.json()["name"] == "Product 42"


# ---------------------------------------------------------------------------
# Default / fallback values
# ---------------------------------------------------------------------------


@requires_redis
@pytest.mark.integration
class TestDefaultFallback:
    """CacheBackend.get(default=...) for graceful degradation."""

    def test_returns_default_when_not_set(self, client: TestClient) -> None:
        r = client.get("/settings/theme")
        assert r.json()["value"] == "default-value"

    def test_returns_stored_value_after_set(self, client: TestClient) -> None:
        client.put("/settings/theme")
        r = client.get("/settings/theme")
        assert r.json()["value"] == "custom-theme"

    def test_default_is_not_cached(self, client: TestClient) -> None:
        """Getting a default should not store it."""
        client.get("/settings/lang")
        client.get("/settings/lang")
        assert _counters["setting:lang"] == 2


# ---------------------------------------------------------------------------
# has() existence check
# ---------------------------------------------------------------------------


@requires_redis
@pytest.mark.integration
class TestHasExistenceCheck:
    """CacheBackend.has() to avoid expensive work."""

    def test_cold_cache_triggers_expensive_work(self, client: TestClient) -> None:
        r = client.get("/check-warm/1")
        assert r.json()["warm"] is False
        assert _counters["expensive-work:1"] == 1

    def test_warm_cache_skips_expensive_work(self, client: TestClient) -> None:
        client.get("/products/1")
        r = client.get("/check-warm/1")
        assert r.json()["warm"] is True
        assert "expensive-work:1" not in _counters

    def test_evict_makes_cache_cold_again(self, client: TestClient) -> None:
        client.get("/products/1")
        assert client.get("/check-warm/1").json()["warm"] is True
        # Namespace clear makes the key cold again
        client.delete("/cache/products")
        assert client.get("/check-warm/1").json()["warm"] is False
        assert _counters["expensive-work:1"] == 1


# ---------------------------------------------------------------------------
# timedelta TTL
# ---------------------------------------------------------------------------


@requires_redis
@pytest.mark.integration
class TestTimedeltaTTL:
    """timedelta for TTL instead of raw seconds."""

    def test_session_cached_with_timedelta(self, client: TestClient) -> None:
        r1 = client.get("/session/abc123")
        assert r1.json()["session_id"] == "abc123"
        assert r1.json()["counter"] == 1

        r2 = client.get("/session/abc123")
        assert r2.json() == r1.json()
        assert _counters["sess:abc123"] == 1


# ---------------------------------------------------------------------------
# Namespace operations
# ---------------------------------------------------------------------------


@requires_redis
@pytest.mark.integration
class TestNamespaceOperations:
    """Bulk invalidation by eviction group."""

    def test_delete_group(self, client: TestClient) -> None:
        client.get("/products/1")
        client.get("/products/2")
        client.get("/products/3")

        r = client.delete("/cache/products")
        assert r.json()["deleted"] == 3

        client.get("/products/1")
        client.get("/products/2")
        client.get("/products/3")
        assert _counters["product:1"] == 2
        assert _counters["product:2"] == 2
        assert _counters["product:3"] == 2

    def test_eviction_group_isolation(self, client: TestClient) -> None:
        """Clearing one eviction group doesn't affect another."""
        client.get("/products/1")
        client.get("/session/abc")

        client.delete("/cache/products")

        client.get("/session/abc")
        assert _counters["sess:abc"] == 1

        client.get("/products/1")
        assert _counters["product:1"] == 2

    def test_delete_empty_group_returns_zero(self, client: TestClient) -> None:
        """Deleting an eviction group with no keys returns 0."""
        r = client.delete("/cache/nonexistent")
        assert r.json()["deleted"] == 0

    @pytest.mark.parametrize("group", ["?", "a*", "*", "[", "a[b]", "a\\"])
    async def test_delete_group_escapes_glob_characters(
        self, real_async_redis: async_redis.Redis, group: str
    ) -> None:
        """Glob characters in a group name match only that group."""
        backend = CacheBackend(real_async_redis)
        others = ["a", "ab", "admin", "b"]
        for grp in [*others, group]:
            await backend.set("items", 1, eviction_group=grp)

        assert await backend.delete_group(group) == 1
        assert not await backend.has("items", eviction_group=group)
        for grp in others:
            assert await backend.has("items", eviction_group=grp)

    def test_delete_all_without_group(self, client: TestClient) -> None:
        """delete_group() with no eviction group wipes all cache keys."""
        client.get("/products/1")
        client.get("/session/abc")

        r = client.delete("/cache-all")
        assert r.json()["deleted"] >= 2

        # Both groups should now be cold
        client.get("/products/1")
        assert _counters["product:1"] == 2
        client.get("/session/abc")
        assert _counters["sess:abc"] == 2

    def test_delete_group_via_sync_backend(self, real_redis: sync_redis.Redis) -> None:
        """SyncCacheBackendDep.delete_group works from sync endpoints."""
        app = FastAPI()
        FastAPIRedis(app).lifespan()

        @app.post("/seed")
        def seed(cb: SyncCacheBackendDep) -> dict:
            cb.set("a", 1, eviction_group="bulk")
            cb.set("b", 2, eviction_group="bulk")
            cb.set("c", 3, eviction_group="bulk")
            return {"ok": True}

        @app.delete("/clear")
        def clear(cb: SyncCacheBackendDep) -> dict:
            return {"deleted": cb.delete_group("bulk")}

        with TestClient(app) as c:
            c.post("/seed")
            r = c.delete("/clear")
            assert r.json()["deleted"] == 3

            # Second clear should find nothing
            r = c.delete("/clear")
            assert r.json()["deleted"] == 0

        real_redis.flushdb()


# ---------------------------------------------------------------------------
# SyncCacheBackendDep
# ---------------------------------------------------------------------------


@requires_redis
@pytest.mark.integration
class TestSyncCacheBackendDep:
    """SyncCacheBackendDep works from sync endpoints via anyio bridge."""

    def test_sync_get_set_round_trip(self, real_redis: sync_redis.Redis) -> None:
        app = FastAPI()
        FastAPIRedis(app).lifespan()

        @app.post("/write")
        def write(cb: SyncCacheBackendDep) -> dict:
            cb.set("k1", {"a": 1}, ttl=60)
            return {"ok": True}

        @app.get("/read")
        def read(cb: SyncCacheBackendDep) -> dict:
            val = cb.get("k1")
            return {"value": val}

        @app.get("/exists")
        def exists(cb: SyncCacheBackendDep) -> dict:
            return {"exists": cb.has("k1")}

        @app.delete("/remove")
        def remove(cb: SyncCacheBackendDep) -> dict:
            return {"deleted": cb.delete("k1")}

        with TestClient(app) as tc:
            r = tc.post("/write")
            assert r.status_code == 200

            r = tc.get("/read")
            assert r.status_code == 200
            assert r.json()["value"] == {"a": 1}

            r = tc.get("/exists")
            assert r.json()["exists"] is True

            r = tc.delete("/remove")
            assert r.json()["deleted"] is True

            r = tc.get("/exists")
            assert r.json()["exists"] is False

        real_redis.flushdb()
