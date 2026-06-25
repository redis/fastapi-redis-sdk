# API Reference

## Setup

```python
from fastapi import FastAPI
from redis_fastapi import FastAPIRedis

app = FastAPI()
FastAPIRedis(app).lifespan()            # connection pools only
FastAPIRedis(app).lifespan().caching()   # + DI caching support
```

Or compose the lifespan directly:

```python
from redis_fastapi import redis_lifespan
app = FastAPI(lifespan=redis_lifespan)
```

`redis_lifespan` is an async context manager that creates the shared async connection pool on startup and closes it on shutdown. Accessing the pool without a registered lifespan raises `RuntimeError`.

---

## Redis client dependencies

### `AsyncRedisDep`

```python
from redis_fastapi import AsyncRedisDep

@app.get("/")
async def handler(redis: AsyncRedisDep):
    await redis.get("key")
```

`Annotated[AsyncRedis | AsyncRedisCluster, Depends(get_async_redis)]` — returns a cached async Redis client backed by the shared connection pool. Returns `AsyncRedisCluster` when `settings.cluster` is `True`.

### `get_async_redis()`

```python
async def get_async_redis(request: Request) -> AsyncRedis | AsyncRedisCluster
```

Async function underlying `AsyncRedisDep`. Returns the same client instance on every call (no per-request overhead). Raises `RuntimeError` if no lifespan has initialised the pool.

---

## Cache backend dependencies

### `CacheBackendDep`

```python
from redis_fastapi import CacheBackendDep

@app.get("/dashboard/{user_id}")
async def dashboard(user_id: int, cache: CacheBackendDep):
    cached = await cache.get(f"stats:{user_id}", eviction_group="dashboard")
    if cached:
        return cached
    result = await compute_dashboard(user_id)
    await cache.set(f"stats:{user_id}", result, ttl=300, eviction_group="dashboard")
    return result
```

`Annotated[CacheBackend, Depends(get_cache_backend)]` — async cache backend with `get`/`set`/`delete`/`has`/`delete_group`. Use for conditional caching, cascade invalidation, and dynamic TTL.

`CacheBackend(redis, coder=...)` accepts any `Coder`. Use `FastAPIJsonCoder`
for values supported by FastAPI's `jsonable_encoder()`, or
`pydantic_model_coder(Model)` when cache hits should decode back into Pydantic
model instances.

### `SyncCacheBackendDep`

```python
from redis_fastapi import SyncCacheBackendDep

@app.get("/sync-dashboard/{user_id}")
def dashboard(user_id: int, cache: SyncCacheBackendDep):
    cached = cache.get(f"stats:{user_id}", eviction_group="dashboard")
    if cached:
        return cached
    result = compute_dashboard(user_id)
    cache.set(f"stats:{user_id}", result, ttl=300, eviction_group="dashboard")
    return result
```

`Annotated[SyncCacheBackend, Depends(get_sync_cache_backend)]` — blocking facade over `CacheBackend`, bridges async calls via `anyio.from_thread.run`. **Only works from sync endpoints** running in FastAPI's worker threads.

### `get_cache_backend()` / `get_sync_cache_backend()`

```python
async def get_cache_backend(request: Request) -> CacheBackend
async def get_sync_cache_backend(request: Request) -> SyncCacheBackend
```

Factory functions underlying the type aliases above.

---

## DI caching factories

All three factories return `Depends()`-compatible dependencies. Requires `FastAPIRedis(app).caching()` (or `add_redis_caching(app)`).

### `cache()`

```python
from fastapi import Depends
from redis_fastapi import cache

@app.get("/items", dependencies=[Depends(cache(ttl=60, eviction_group="items"))])
async def get_items():
    ...
```

On a **cache hit** the endpoint is skipped (response served from Redis). On a **miss** the response is captured and stored. Adds `X-Redis-Cache` (HIT/MISS), `Cache-Control`, and `ETag` headers with 304 Not Modified support.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `ttl` | `int \| None` | `settings.default_ttl` | Cache TTL in seconds (`0` = no expiry) |
| `eviction_group` | `str` | `""` | Namespace segment in the cache key |
| `cache_prefix` | `str \| None` | `settings.pattern_prefix("cache")` | Key prefix override |
| `key_builder` | `KeyBuilder \| None` | `default_key_builder` | Custom key builder |
| `private` | `bool` | `False` | Emit `Cache-Control: private` |

### `cache_evict()`

```python
from redis_fastapi import cache_evict, default_key_builder

@app.delete("/items/{item_id}", dependencies=[Depends(cache_evict(eviction_group="items", key_builder=default_key_builder))])
async def delete_item(item_id: int):
    ...
```

Eviction runs **after** the endpoint succeeds. With a `key_builder`, deletes the matching key. Without one, clears the **entire eviction group**.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `eviction_group` | `str` | `""` | Namespace to evict from |
| `key_builder` | `KeyBuilder \| None` | `None` | Specific key; omit to clear eviction group |
| `prefix` | `str \| None` | `settings.pattern_prefix("cache")` | Key prefix override |

### `cache_put()`

```python
from redis_fastapi import cache_put, default_key_builder

@app.put("/items/{item_id}", dependencies=[Depends(cache_put(eviction_group="items", key_builder=default_key_builder, ttl=300))])
async def update_item(item_id: int, body: Item):
    ...
```

Write-through: endpoint always executes, response is stored so the next `cache()` read is a HIT.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `ttl` | `int \| None` | `settings.default_ttl` | Cache TTL in seconds |
| `eviction_group` | `str` | `""` | Namespace to write into |
| `key_builder` | `KeyBuilder \| None` | `default_key_builder` | Custom key builder |
| `prefix` | `str \| None` | `settings.pattern_prefix("cache")` | Key prefix override |
| `private` | `bool` | `False` | Emit `Cache-Control: private` |

---

## `default_key_builder`

```python
def default_key_builder(request: Request, eviction_group: str = "", prefix: str = "") -> str
```

Builds a cache key from the request path (slashes → colons) and sorted query params. Eviction group is wrapped in Redis hash-tag braces (`{eviction_group}`) for Cluster slot consistency.
