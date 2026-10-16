"""Integration tests for key_builder-based cache/cache_evict/cache_put DI factories.

Verifies that ``cache()``, ``cache_evict(key_builder=...)``, and
``cache_put(key_builder=...)`` share the same Redis key, tested end-to-end
with real Redis.  Also checks that distinct query strings get distinct keys.
"""

from __future__ import annotations

from collections.abc import Generator

import pytest
import redis as sync_redis
from fastapi import Depends, FastAPI, Request
from fastapi.testclient import TestClient

from redis_fastapi import (
    FastAPIRedis,
    cache,
    cache_evict,
    cache_put,
    default_key_builder,
)
from tests.conftest import requires_redis

_kb_counters: dict[str, int] = {}


def _kb_increment(key: str) -> int:
    _kb_counters[key] = _kb_counters.get(key, 0) + 1
    return _kb_counters[key]


def _build_key_builder_app() -> FastAPI:
    app = FastAPI()
    FastAPIRedis(app).lifespan().caching()

    @app.get(
        "/kb/products/{product_id}",
        dependencies=[Depends(cache(ttl=300, eviction_group="kb-products"))],
    )
    async def get_product(product_id: int) -> dict:
        return {"id": product_id, "v": _kb_increment(f"kb-product:{product_id}")}

    @app.delete(
        "/kb/products/{product_id}",
        dependencies=[
            Depends(
                cache_evict(
                    eviction_group="kb-products", key_builder=default_key_builder
                )
            )
        ],
    )
    async def delete_product(product_id: int) -> dict:
        return {"deleted": product_id}

    @app.put(
        "/kb/products/{product_id}",
        dependencies=[
            Depends(
                cache_put(
                    eviction_group="kb-products",
                    key_builder=default_key_builder,
                    ttl=300,
                )
            )
        ],
    )
    async def replace_product(product_id: int) -> dict:
        return {"id": product_id, "name": f"Updated {product_id}", "v": 0}

    return app


_kb_app = _build_key_builder_app()


@pytest.fixture()
def kb_client(real_redis: sync_redis.Redis) -> Generator[TestClient, None, None]:
    _kb_counters.clear()
    with TestClient(_kb_app) as c:
        yield c
    real_redis.flushdb()


@requires_redis
@pytest.mark.integration
class TestCacheEvictKeyBuilder:
    """cache_evict with key_builder targets the same Redis key as cache()."""

    def test_evict_invalidates_cache_entry(self, kb_client: TestClient) -> None:
        r1 = kb_client.get("/kb/products/42")
        assert r1.headers["X-Redis-Cache"] == "MISS"
        assert r1.json()["v"] == 1

        r2 = kb_client.get("/kb/products/42")
        assert r2.headers["X-Redis-Cache"] == "HIT"
        assert r2.json()["v"] == 1

        r3 = kb_client.delete("/kb/products/42")
        assert r3.status_code == 200

        r4 = kb_client.get("/kb/products/42")
        assert r4.headers["X-Redis-Cache"] == "MISS"
        assert r4.json()["v"] == 2

    def test_evict_is_targeted_across_ids(self, kb_client: TestClient) -> None:
        """Evicting product 1 does not affect product 2."""
        kb_client.get("/kb/products/1")
        kb_client.get("/kb/products/2")
        kb_client.delete("/kb/products/1")

        r = kb_client.get("/kb/products/2")
        assert r.headers["X-Redis-Cache"] == "HIT"
        assert _kb_counters["kb-product:2"] == 1


@requires_redis
@pytest.mark.integration
class TestCachePutKeyBuilder:
    """cache_put with key_builder writes to the same Redis key as cache()."""

    def test_put_populates_cache_for_get(self, kb_client: TestClient) -> None:
        r1 = kb_client.put("/kb/products/42")
        assert r1.json() == {"id": 42, "name": "Updated 42", "v": 0}

        r2 = kb_client.get("/kb/products/42")
        assert r2.headers["X-Redis-Cache"] == "HIT"
        assert r2.json() == {"id": 42, "name": "Updated 42", "v": 0}
        assert "kb-product:42" not in _kb_counters

    def test_put_overwrites_existing_cache(self, kb_client: TestClient) -> None:
        r1 = kb_client.get("/kb/products/7")
        assert r1.headers["X-Redis-Cache"] == "MISS"
        assert r1.json()["v"] == 1

        kb_client.put("/kb/products/7")

        r2 = kb_client.get("/kb/products/7")
        assert r2.headers["X-Redis-Cache"] == "HIT"
        assert r2.json() == {"id": 7, "name": "Updated 7", "v": 0}
        assert _kb_counters["kb-product:7"] == 1


def _build_query_app() -> FastAPI:
    app = FastAPI()
    FastAPIRedis(app).lifespan().caching()

    @app.get("/search", dependencies=[Depends(cache(ttl=60))])
    async def search(request: Request) -> dict:
        return {"params": request.query_params.multi_items()}

    return app


@requires_redis
@pytest.mark.integration
class TestQueryStringKeys:
    """Distinct query strings must never share a cache key."""

    @pytest.mark.parametrize(
        ("first", "second"),
        [
            ("a=1:z=2", "a=1&z=2"),  # ':' in a value
            ("a=%253A", "a=%3A"),  # literal '%3A' vs ':'
            ("a%3D1%26z=2", "a=1&z=2"),  # '=' and '&' in a name
            ("a=1&a=2", "a=2"),  # repeated name
        ],
    )
    def test_distinct_query_strings_get_distinct_keys(
        self, real_redis: sync_redis.Redis, first: str, second: str
    ) -> None:
        with TestClient(_build_query_app()) as client:
            r1 = client.get(f"/search?{first}")
            r2 = client.get(f"/search?{second}")
        assert r1.json() != r2.json()
        assert r2.headers["X-Redis-Cache"] == "MISS"
