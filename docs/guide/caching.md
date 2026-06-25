# Caching

fastapi-redis-sdk provides two caching patterns. This guide covers each one,
starting with the most common.

| Pattern                                     | Best for                                                                           |
|---------------------------------------------|------------------------------------------------------------------------------------|
| `cache()` / `cache_evict()` / `cache_put()` | Most use cases (type-safe, per-endpoint read/write/invalidate)                     |
| `CacheBackend`                              | Advanced use cases (complex invalidation, conditional logic, intermediate results) |

Both can be combined in the same application.  See
[Architecture](architecture.md) for how connection pools are managed across
the application lifecycle.

---

## 1. Caching factories

Three **dependency factories** cover the full read / invalidate / write-through
lifecycle.  They return callables suitable for `Depends()` and integrate
fully with FastAPI's dependency-injection system. For more details on this design
decision, see the [Architecture](architecture.md) section.

| Factory         | Purpose                                                                                                                                      |
|-----------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| `cache()`       | Cache GET responses (read path)                                                                                                              |
| `cache_evict()` | [Invalidate](https://redis.io/glossary/cache-invalidation/) cache entries after a write succeeds                                             |
| `cache_put()`   | [Write-through](https://redis.io/blog/three-ways-to-maintain-cache-consistency/) — store the return value so subsequent reads see fresh data |

### Setup

Use the `Redis` builder to configure the app.  A single fluent call
sets up connection pools, the exception handler, and the capture middleware:

```python
from fastapi import Depends, FastAPI
from redis_fastapi import FastAPIRedis, cache, cache_evict, cache_put, default_key_builder

app = FastAPI()
FastAPIRedis(app).lifespan().caching()
```

The builder wraps any existing lifespan — multiple libraries can each
register their own without conflicting.

### Basic usage

```python
# READ — cache the response
@app.get("/products/{product_id}", dependencies=[Depends(cache(ttl=300, eviction_group="products"))])
async def get_product(product_id: int):
    return await db.get_product(product_id)

# INVALIDATE — evict the cached entry when the resource is deleted
@app.delete(
    "/products/{product_id}",
    dependencies=[Depends(cache_evict(eviction_group="products", key_builder=default_key_builder))],
)
async def delete_product(product_id: int):
    await db.delete(product_id)
    return {"deleted": product_id}

# WRITE-THROUGH — update the cached entry so the next GET is a HIT
@app.put(
    "/products/{product_id}",
    dependencies=[Depends(cache_put(eviction_group="products", key_builder=default_key_builder, ttl=300))],
)
async def replace_product(product_id: int, body: Product):
    return await db.update(product_id, body)
```

`cache_evict()` and `cache_put()` always execute the endpoint first;
cache operations happen **after** success.

### Options

**cache()** — read-path caching:

```python
Depends(cache(
    ttl=120,                    # seconds (default: 0 = no expiry)
    eviction_group="v2",             # extra segment in the cache key
    prefix="custom:prefix",     # override the default key prefix
    key_builder=my_key_builder, # custom key function (sync or async)
    private=True,               # emit Cache-Control: private (see below)
))
```

**cache_evict()** — invalidation on write:

```python
Depends(cache_evict(
    eviction_group="products",               # eviction group to evict from
    key_builder=default_key_builder,    # evict the matching key (omit to clear entire eviction group)
    prefix="custom:prefix",             # override the default key prefix
))
```

**cache_put()** — write-through on write:

```python
Depends(cache_put(
    eviction_group="products",               # eviction group to write into
    key_builder=default_key_builder,    # key builder (default: default_key_builder)
    prefix="custom:prefix",             # override the default key prefix
    ttl=300,                            # seconds (default: 0 = no expiry)
    private=True,                       # emit Cache-Control: private
))
```

`private=True` works the same way here as on `cache()` — it adds the
[`Cache-Control: private`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Cache-Control#private)
directive so CDNs and shared proxies do not store the response.  The
entry is still written to Redis for fast subsequent reads; only
intermediate HTTP caches are told to stay out.  See
[Response directives](#http-cache-headers) for more detail.

```python
# User updates their own profile — cache the result in Redis,
# but prevent CDNs from serving Alice's profile to Bob.
@app.put(
    "/me/profile",
    dependencies=[Depends(cache_put(ttl=60, private=True))],
)
async def update_profile(body: Profile, user: User = Depends(get_current_user)):
    return await db.update_profile(user.id, body)
```

### Cache keys

Keys follow the pattern `{prefix}:{{eviction_group}}:{path}:{sorted_query_params}`.
Slashes become colons; query parameters are sorted alphabetically.

When an eviction group is provided it is wrapped in Redis
[hash-tag](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/#hash-tags)
braces (`{eviction_group}`).  This guarantees that all keys in the same
eviction group map to the **same hash slot**, which is required for
Lua-based bulk eviction in Redis Cluster and is harmless in standalone
mode.

| Request (eviction_group=`products`) | Key |
|---------|-----|
| `GET /api/v1/items` | `redis:fastapi:cache:{products}:api:v1:items` |
| `GET /items?z=2&a=1` | `redis:fastapi:cache:{products}:items:a=1:z=2` |

Without an eviction group, no hash tag is added:

| Request (no eviction group) | Key |
|---------|-----|
| `GET /api/v1/items` | `redis:fastapi:cache:api:v1:items` |

All three factories use the same `key_builder` function (defaulting to
`default_key_builder`), which builds the key from the incoming `Request`.
This means the GET, DELETE, and PUT on the same path all resolve to the
**exact same cache key** automatically — no manual key matching required.

|                 | Omit `key_builder`           | Pass `default_key_builder` | Pass custom  |
|-----------------|------------------------------|----------------------------|--------------|
| `cache()`       | uses `default_key_builder`   | same                       | uses custom  |
| `cache_put()`   | uses `default_key_builder`   | same                       | uses custom  |
| `cache_evict()` | **clears entire eviction group**  | deletes single key         | uses custom  |

To clear an entire eviction group instead of a single key, omit `key_builder`:

```python
@app.post("/admin/clear-products", dependencies=[Depends(cache_evict(eviction_group="products"))])
async def clear_products():
    return {"ok": True}
```

For complex invalidation that doesn't map to a single URL path (cross-path
eviction, multi-key invalidation, conditional logic), use `CacheBackend`
directly — see [section 2](#2-cachebackend).

#### Eviction groups and Redis Cluster

In Redis Cluster, keys are distributed across nodes based on their hash
slot (CRC16 of the key modulo 16384).  Without hash tags, keys in the
same logical eviction group would be scattered across multiple nodes, making
bulk operations like `delete_group()` unreliable — `SCAN` only sees
keys on the node it runs on, and Lua scripts cannot touch keys in
different slots.

Hash tags solve this: Redis only hashes the substring inside `{…}` when
computing the slot.  Because all keys in an eviction group share the same
`{eviction_group}` tag, they are guaranteed to land on the **same node and
slot**.  This makes the Lua-based `SCAN` + `UNLINK` script used by
`delete_group()` correct and atomic.

**Trade-off — hot slots:** All keys in one eviction group concentrate on a
single node.  For typical HTTP response caching this is not a problem
(eviction groups are small-to-moderate in size).  If an eviction group grows very
large, consider splitting it into multiple smaller eviction groups to
distribute load across the cluster.

### HTTP cache headers

Every `cache()` response includes these headers automatically:

| Header | Value |
|--------|-------|
| `X-Redis-Cache` | `HIT` or `MISS` |
| [`Cache-Control`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control) | `max-age=<remaining_ttl>` when TTL > 0, or `no-cache` when TTL = 0 (always revalidate via ETag). Adds `private` prefix when `private=True`. |
| [`ETag`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag) | Weak ETag of the cached body |

**Request directives** — the following `Cache-Control` directives sent by the
client are respected:

- [`If-None-Match`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/If-None-Match) with a matching ETag returns [**304 Not Modified**](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/304).
- `Cache-Control: no-cache` forces a cache refresh.
- `Cache-Control: no-store` bypasses caching entirely.
- `Cache-Control: max-age=N` — a cached entry older than *N* seconds is
  treated as a cache miss and the endpoint re-executes.
  `max-age=0` is equivalent to `no-cache`.

**Response directives** — use `private=True` on the factory to emit
`Cache-Control: private, max-age=…`.  This tells CDNs and shared proxies
**not** to store the response — only the end-user's browser may cache it. See
[MDN: Cache-Control: private](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control#private) and
[MDN: Private caches](https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/Caching#private_caches)

```python
# User-specific data — must not be cached by a CDN
@app.get("/me/profile", dependencies=[Depends(cache(ttl=60, private=True))])
async def my_profile(user: User = Depends(get_current_user)):
    return user.profile
```

### Testing

The DI factories integrate with FastAPI's `dependency_overrides`, so
unit tests can swap the real Redis client for a fake without any
monkey-patching:

```python
import fakeredis.aioredis
from redis_fastapi import FastAPIRedis, cache, get_async_redis

app = FastAPI()
FastAPIRedis(app).caching()

@app.get("/items", dependencies=[Depends(cache(ttl=60))])
async def get_items():
    return {"items": [1, 2, 3]}

# In tests:
fake = fakeredis.aioredis.FakeRedis()
app.dependency_overrides[get_async_redis] = lambda: fake

with TestClient(app) as client:
    r1 = client.get("/items")
    assert r1.headers["X-Redis-Cache"] == "MISS"
    r2 = client.get("/items")
    assert r2.headers["X-Redis-Cache"] == "HIT"
```

### Error handling

* If the endpoint raises an exception, no cache operations are performed.
* If the cache operation itself fails (e.g., Redis is down), the error is logged
and the endpoint's return value is still delivered to the caller.

---

## 2. `CacheBackend`

`CacheBackendDep` injects a `CacheBackend` instance for patterns that the
DI factories cannot express: conditional caching, intermediate result caching,
cross-group cascade invalidation, dynamic TTL, and atomic
read-modify-write.

`CacheBackend` only needs a Redis connection — `.caching()` is **not**
required.  If you're only using `CacheBackend` (no `cache()` / `cache_evict()`
/ `cache_put()`), setup is just:

```python
app = FastAPI()
FastAPIRedis(app).lifespan()
```

For simple invalidation or write-through, prefer `cache_evict()` /
`cache_put()` instead (which do require `.caching()`).

### API

| Method | Description |
|--------|-------------|
| `get(key, *, default=None, eviction_group=None)` | Retrieve and deserialize. Returns `default` on miss. |
| `set(key, value, *, ttl=None, eviction_group=None)` | Serialize and store. `ttl` accepts `int` or `timedelta`. |
| `delete(key, *, eviction_group=None)` | Delete a single entry. Returns `True` if it existed. |
| `has(key, *, eviction_group=None)` | Check existence without deserializing (Redis `EXISTS`). |
| `delete_group(eviction_group=None)` | Delete all keys in an eviction group. Returns the count. |

The basic [cache-aside](https://redis.io/learn/howtos/solutions/microservices/caching)
pattern (get → miss → compute → set → return) is exactly what `cache()` does
automatically.  Use `CacheBackendDep` when you need control that the DI
factories cannot express:

### FastAPI-aware serialization

By default, `CacheBackend` uses `JsonCoder`, a thin wrapper around
`json.dumps()` / `json.loads()`. Use `FastAPIJsonCoder` when cached values
contain objects FastAPI already knows how to serialize, such as Pydantic
models, `datetime`, `UUID`, enums, or dataclasses:

```python
from redis_fastapi import CacheBackend, FastAPIJsonCoder

cache = CacheBackend(redis, coder=FastAPIJsonCoder)
```

For typed model round-trips, create a model-specific coder. Decoded cache hits
come back as Pydantic model instances:

```python
from typing import Annotated
from fastapi import Depends
from pydantic import BaseModel
from redis_fastapi import AsyncRedisDep, CacheBackend, pydantic_model_coder

class Product(BaseModel):
    id: int
    name: str

ProductCoder = pydantic_model_coder(Product)

async def get_product_cache(redis: AsyncRedisDep) -> CacheBackend:
    return CacheBackend(redis, coder=ProductCoder)

ProductCacheDep = Annotated[CacheBackend, Depends(get_product_cache)]

@app.get("/products/{product_id}")
async def get_product(product_id: int, cache: ProductCacheDep) -> Product:
    cached = await cache.get(f"product:{product_id}", eviction_group="products")
    if cached is not None:
        return cached
    product = await db.get_product(product_id)
    await cache.set(f"product:{product_id}", product, ttl=300, eviction_group="products")
    return product
```

### Conditional caching

Cache only when business rules are met - `@cache` always caches the result:

```python
@app.get("/items/{item_id}")
async def get_item(item_id: int, cache: CacheBackendDep):
    cached = await cache.get(f"item:{item_id}", eviction_group="items")
    if cached is not None:
        return cached

    item = await db.get_item(item_id)

    if item["status"] == "published":
        await cache.set(f"item:{item_id}", item, ttl=300, eviction_group="items")

    return item
```

### Intermediate result caching

Cache sub-computations independently so they can be invalidated separately:

```python
@app.get("/dashboard/{user_id}")
async def dashboard(user_id: int, cache: CacheBackendDep):
    orders = await cache.get(f"orders:{user_id}", eviction_group="dashboard")
    if orders is None:
        orders = await compute_order_summary(user_id)
        await cache.set(f"orders:{user_id}", orders, ttl=60, eviction_group="dashboard")

    recommendations = await cache.get(f"reco:{user_id}", eviction_group="dashboard")
    if recommendations is None:
        recommendations = await generate_recommendations(user_id)
        await cache.set(f"reco:{user_id}", recommendations, ttl=120, eviction_group="dashboard")

    return {"orders": orders, "recommendations": recommendations}
```

### Cascade invalidation (across eviction groups)

A single write can invalidate caches in multiple eviction groups:

```python
@app.put("/profile/{user_id}")
async def update_profile(user_id: int, body: ProfileUpdate, cache: CacheBackendDep):
    await db.update_profile(user_id, body)

    # Cascade: profile, dashboard, and user list all become stale
    await cache.delete(f"profile:{user_id}", eviction_group="profiles")
    await cache.delete(f"orders:{user_id}", eviction_group="dashboard")
    await cache.delete("all", eviction_group="users")
    return {"ok": True}
```

### Dynamic TTL

Set TTL based on the data itself - decorators cannot express this because
TTL is fixed at decoration time:

```python
@app.get("/content/{content_id}")
async def get_content(content_id: int, cache: CacheBackendDep):
    cached = await cache.get(f"content:{content_id}", eviction_group="content")
    if cached is not None:
        return cached

    content = await db.get_content(content_id)
    ttl = 3600 if content["premium"] else 300
    await cache.set(f"content:{content_id}", content, ttl=ttl, eviction_group="content")
    return content
```

### Atomic read-modify-write

Read a cached value, modify it, and write it back. Decorators cannot express
this because the cache operation depends on the existing cached value:

```python
@app.post("/products/{product_id}/view")
async def record_view(product_id: int, cache: CacheBackendDep):
    views = await cache.get(f"views:{product_id}", default=0, eviction_group="analytics")
    views += 1
    await cache.set(f"views:{product_id}", views, ttl=3600, eviction_group="analytics")
    return {"product_id": product_id, "views": views}
```

### Existence check (has)

Avoid expensive work when the cache is warm without deserializing the value:

```python
@app.get("/warm-check/{product_id}")
async def check_warm(product_id: int, cache: CacheBackendDep):
    if await cache.has(f"product:{product_id}", eviction_group="products"):
        return {"warm": True}

    # Only do expensive work when cache is cold
    await run_expensive_recomputation(product_id)
    return {"warm": False}
```

### Default / fallback values

Return a fallback instead of `None` when the cache is empty:

```python
@app.get("/settings/{key}")
async def get_setting(key: str, cache: CacheBackendDep):
    value = await cache.get(f"setting:{key}", default="default-value", eviction_group="settings")
    return {"key": key, "value": value}
```

### timedelta TTL

`set()` accepts both `int` (seconds) and `timedelta`:

```python
from datetime import timedelta

await cache.set("session:abc", data, ttl=timedelta(minutes=30), eviction_group="sessions")
```

---

## Combining patterns

Both patterns can coexist in the same application:

```python
from fastapi import Depends, FastAPI
from redis_fastapi import (
  Redis, cache, cache_evict, cache_put, default_key_builder, CacheBackendDep,
)

app = FastAPI()
FastAPIRedis(app).lifespan().caching()


# cache(): read-path caching
@app.get("/users/{user_id}", dependencies=[Depends(cache(ttl=60, eviction_group="users"))])
async def get_user(user_id: int) -> User:
  return await db.get_user(user_id)


# cache_evict(): invalidate the cached entry on delete
@app.delete(
  "/users/{user_id}",
  dependencies=[Depends(cache_evict(eviction_group="users", key_builder=default_key_builder))],
)
async def delete_user(user_id: int):
  await db.delete_user(user_id)


# cache_put(): write-through on update
@app.put(
  "/products/{product_id}",
  dependencies=[Depends(cache_put(eviction_group="products", ttl=300))],
)
async def replace_product(product_id: int, body: Product):
  return await db.update(product_id, body)


# CacheBackend: complex conditional logic
@app.post("/checkout")
async def checkout(cart: Cart, cache: CacheBackendDep):
  order = await process_order(cart)
  await cache.delete(f"cart:{cart.user_id}", eviction_group="carts")
  await cache.delete(f"stats:{cart.user_id}", eviction_group="dashboard")
  return order
```

---

## Feature comparison

✅ = built-in &nbsp; 🔧 = possible with manual code &nbsp; ❌ = not applicable

| Feature                                                                                         |    `cache()`    | `cache_evict()` / `cache_put()` | CacheBackend |
|-------------------------------------------------------------------------------------------------|:---------------:|:-------------------------------:|:------------:|
| **HTTP compliance**                                                                             |                 |                                 |              |
| [304 Not Modified](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/304)                |        ✅        |                ✅                |      🔧      |
| [ETag](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag) generation               |        ✅        |                ✅                |      🔧      |
| [Cache-Control](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control) header |        ✅        |                ✅                |      🔧      |
| Client [`max-age`](#http-cache-headers) respected                                               |        ✅        |                ❌                |      ❌       |
| Client [`no-cache`](#http-cache-headers) (force refresh)                                        |        ✅        |                ❌                |      ❌       |
| Client [`no-store`](#http-cache-headers) (bypass cache)                                         |        ✅        |                ❌                |      ❌       |
| [`private` / `public`](#http-cache-headers) directive                                           |        ✅        |                ✅                |      🔧      |
| [`X-Redis-Cache`](#http-cache-headers) status header                                            |        ✅        |                ✅                |      🔧      |
| **Caching control**                                                                             |                 |                                 |              |
| Per-endpoint TTL                                                                                |        ✅        |                ✅                |      ✅       |
| [Group](#cache-keys) support                                                                    |        ✅        |                ✅                |      ✅       |
| [Group eviction](#cache-keys)                                                                   |        ❌        |       ✅ (no key_builder)        |      ✅       |
| [Key-level invalidation](https://redis.io/glossary/cache-invalidation/)                         |        ❌        |          ✅ key_builder          |      ✅       |
| [Write-through](#options)                                                                       |        ❌        |         ✅ `cache_put()`         |      🔧      |
| [Conditional caching](#conditional-caching)                                                     |        ❌        |                ❌                |      ✅       |
| Custom key builder                                                                              |        ✅        |                ✅                |      ❌       |
| Custom key prefix                                                                               |        ✅        |                ✅                |      ❌       |
| Custom coder                                                                                    |        ❌        |                ❌                |      ✅       |
| **Testing**                                                                                     |                 |                                 |              |
| `dependency_overrides`                                                                          |        ✅        |                ✅                |      ✅       |
| No monkey-patching needed                                                                       |        ✅        |                ✅                |      ✅       |
| **Data handling**                                                                               |                 |                                 |              |
| Pydantic models                                                                                 |        ✅        |                ✅                |      ✅       |
| Type safety                                                                                     |        ✅        |                ✅                |      ✅       |
| **Error handling**                                                                              |                 |                                 |              |
| Redis failure graceful degradation                                                              | ✅ auto-fallback |         ✅ auto-fallback         |      🔧      |

---

## Quick reference

| Scenario                                | Recommended                                                    |
|-----------------------------------------|----------------------------------------------------------------|
| Most GET endpoints                      | [`cache()`](#1-caching-factories)                              |
| User-specific / authenticated endpoints | [`cache(private=True)`](#http-cache-headers)                   |
| POST/PUT that invalidates a GET         | [`cache_evict()`](#basic-usage)                                |
| Write-through (update cache on write)   | [`cache_put()`](#basic-usage)                                  |
| Complex multi-step invalidation         | [`CacheBackend`](#cascade-invalidation-across-eviction-groups) |
| Conditional caching (business rules)    | [`CacheBackend`](#conditional-caching)                         |
| Cache sub-computations independently    | [`CacheBackend`](#intermediate-result-caching)                 |
| Public catalog, high traffic            | [`cache()`](#1-caching-factories)                              |

---

## Best practices

1. **Use `FastAPIRedis(app).lifespan().caching()`** for app setup.
2. **Start with `cache()`** for GET endpoints — it is the simplest option.
3. **Add `cache_evict()`** on write endpoints that should invalidate cached reads.
4. **Use `cache_put()`** when the write result should immediately warm the cache.
5. **Switch to CacheBackend** when you need conditional logic or complex flows.
6. **Always set explicit TTLs** — see [TTL defaults](#ttl-defaults) below.
8. **Use eviction groups** to group related keys and enable bulk invalidation.
9. **Use `dependency_overrides`** in tests — no monkey-patching needed.
10. **Do not over-cache** — cache only what is expensive to recompute.

---

## TTL defaults

By default, `default_ttl` is **0** — cache entries have **no automatic
expiration** and persist until explicitly evicted (via `cache_evict()`,
`delete_group()`, or Redis memory eviction policies like `allkeys-lru`).

This is a deliberate design choice:

1. **A caching library's job is to cache, not to expire.** Expiry is an
   application-level policy decision. Only you know whether your data changes
   every second or every month — a library-imposed default (e.g. 60 seconds,
   5 minutes) is wrong for most use cases. The library should provide excellent
   TTL *support*, not impose a TTL *opinion*.

2. **The real protection against stale data is explicit invalidation.**
   `cache_evict()` and `cache_put()` factories, plus `CacheBackend.delete()`
   and `delete_group()`, give you precise control over when stale data
   is removed. TTL is a coarse safety net, not a substitute for proper
   invalidation.

3. **The real protection against memory exhaustion is Redis itself.**
   Configure [`maxmemory`](https://redis.io/docs/latest/develop/reference/eviction/)
   and an eviction policy (e.g. `allkeys-lru`) on the server side.
   Application-level TTL defaults are not required to prevent unbounded
   memory growth.

4. **Consistency with the ecosystem reduces friction.** Spring Cache, Ehcache,
   Caffeine, fastapi-cache2, PSR-6/PSR-16, and virtually every other caching
   framework defaults to no expiry. Developers porting from any of these
   won't be surprised by silent key expiry.

5. **Making the user set TTL explicitly is a feature, not a bug.** It forces
   you to think about freshness requirements for your specific data, rather
   than silently accepting an arbitrary value that may or may not be
   appropriate.

**We strongly recommend setting an explicit TTL on every cached endpoint.**
Choose a value that matches your data's volatility:

| Data type                   | Suggested TTL                        |
|-----------------------------|--------------------------------------|
| Reference / config data     | 1 – 24 hours                         |
| Product catalog             | 5 – 30 minutes                       |
| User profile                | 5 – 15 minutes                       |
| API response (general)      | 1 – 5 minutes                        |
| Real-time / financial data  | Use explicit invalidation, not TTL   |

```python
# Good: explicit TTL tailored to the data
@app.get("/products/{id}", dependencies=[Depends(cache(ttl=600, eviction_group="products"))])

# Acceptable: rely on explicit eviction for freshness
@app.get("/config", dependencies=[Depends(cache(eviction_group="config"))])

# Set a global default if most of your endpoints share a common TTL
# via environment variable:
#   REDIS_DEFAULT_TTL=300
# or programmatically:
#   settings.default_ttl = 300
```

---

## Further reading

### fastapi-redis-sdk documentation

- [Configuration Guide](configuration.md) - Redis connection settings
- [API Reference](../api/configuration.md) - Full API documentation

### HTTP caching (MDN)

- [HTTP Caching](https://developer.mozilla.org/en-US/docs/Web/HTTP/Caching) - How browsers and servers negotiate cached responses

### Redis caching guides

- [Cache Optimization Strategies](https://redis.io/blog/guide-to-cache-optimization-strategies/) - Comprehensive overview of lazy loading, write-through, write-behind, and cache prefetching
- [Three Ways to Maintain Cache Consistency](https://redis.io/blog/three-ways-to-maintain-cache-consistency/) - Invalidation, write-through, and TTL-based approaches
- [Cache Prefetching](https://redis.io/learn/howtos/solutions/caching-architecture/cache-prefetching) - Proactive caching for predictable access patterns
- [Distributed Caching](https://redis.io/glossary/distributed-caching/) - Scaling caches across multiple nodes
- [Client-Side Caching](https://redis.io/docs/latest/develop/reference/client-side-caching/) - Redis Tracking for application-level caching
