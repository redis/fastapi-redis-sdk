r"""A product catalogue: cache what shoppers read, never what editors preview.

Three things a shop's API asks of its cache:

* ``GET /products`` - the listing everyone loads.  Cached for 60 s by
  ``cache()``, which also sends ``Cache-Control``, an ``ETag`` and answers
  ``If-None-Match`` with ``304``.
* ``GET /products/{id}`` - a product page.  Published products are cached;
  drafts are not, because an editor previewing one must see each change.
  Only the handler knows the status, so it uses ``CacheBackend`` directly.
* ``PUT /products/{id}`` - an edit.  ``cache_evict()`` drops the whole
  ``products`` group once the update succeeds, listing and pages alike.

The "database" is a dict, so the example needs nothing but Redis.

Run it::

    export REDIS_URL=redis://localhost:6379/0
    uv run --with "fastapi[standard]" fastapi dev examples/caching/catalogue.py

    curl -i localhost:8000/products            # x-redis-cache: MISS, then HIT
    curl localhost:8000/products/1             # "source": "computed", then "cache"
    curl localhost:8000/products/2             # a draft: always "computed"
    curl -X PUT localhost:8000/products/1 -H 'content-type: application/json' \
         -d '{"price": 9.5}'
    curl -i localhost:8000/products            # MISS: the edit evicted it
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, FastAPI, HTTPException
from pydantic import BaseModel

from redis_fastapi import CacheBackendDep, FastAPIRedis, cache, cache_evict

PRODUCTS: dict[int, dict[str, Any]] = {
    1: {"id": 1, "name": "Kettle", "price": 12.0, "status": "published"},
    2: {"id": 2, "name": "Toaster", "price": 30.0, "status": "draft"},
    3: {"id": 3, "name": "Teapot", "price": 18.0, "status": "published"},
}

router = APIRouter()


def _now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


@router.get(
    "/products",
    dependencies=[Depends(cache(ttl=60, eviction_group="products"))],
)
async def list_products() -> dict[str, Any]:
    published = [p for p in PRODUCTS.values() if p["status"] == "published"]
    return {"products": published, "generated_at": _now()}


@router.get("/products/{product_id}")
async def get_product(product_id: int, cache: CacheBackendDep) -> dict[str, Any]:
    key = f"product:{product_id}"
    cached = await cache.get(key, eviction_group="products")
    if cached is not None:
        return {**cached, "source": "cache"}

    product = PRODUCTS.get(product_id)
    if product is None:
        raise HTTPException(404, "no such product")
    page = {**product, "generated_at": _now()}
    if product["status"] == "published":
        await cache.set(key, page, ttl=60, eviction_group="products")
    return {**page, "source": "computed"}


class ProductUpdate(BaseModel):
    price: float


@router.put(
    "/products/{product_id}",
    dependencies=[Depends(cache_evict(eviction_group="products"))],
)
async def update_product(product_id: int, update: ProductUpdate) -> dict[str, Any]:
    if product_id not in PRODUCTS:
        raise HTTPException(404, "no such product")  # nothing changed: no eviction
    PRODUCTS[product_id]["price"] = update.price
    return PRODUCTS[product_id]


app = FastAPI(title="Product catalogue")
app.include_router(router)
FastAPIRedis(app).lifespan().caching()
