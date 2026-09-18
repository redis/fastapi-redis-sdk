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
| `cache_put()`   | [Write-through](https://redis.io/blog/three-ways-to-maintain-cache-consistency/) - store the return value so subsequent reads see fresh data |

### Setup

Use the `Redis` builder to configure the app.  A single fluent call
sets up connection pools, the exception handler, and the capture middleware:

```python
from fastapi import Depends, FastAPI
from redis_fastapi import FastAPIRedis, cache, cache_evict, cache_put, default_key_builder

app = FastAPI()
FastAPIRedis(app).lifespan().caching()
```

The builder wraps any existing lifespan - multiple libraries can each
register their own without conflicting.

### Basic usage

```python
# READ - cache the response
@app.get("/products/{product_id}", dependencies=[Depends(cache(ttl=300, eviction_group="products"))])
async def get_product(product_id: int):
    return await db.get_product(product_id)

# INVALIDATE - evict the cached entry when the resource is deleted
@app.delete(
    "/products/{product_id}",
    dependencies=[Depends(cache_evict(eviction_group="products", key_builder=default_key_builder))],
)
async def delete_product(product_id: int):
    await db.delete(product_id)
    return {"deleted": product_id}

# WRITE-THROUGH - update the cached entry so the next GET is a HIT
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

**cache()** - read-path caching:

```python
Depends(cache(
    ttl=120,                    # seconds (default: 0 = no expiry)
    eviction_group="v2",             # extra segment in the cache key
    prefix="custom:prefix",     # override the default key prefix
    key_builder=my_key_builder, # custom key function (sync or async)
    private=True,               # emit Cache-Control: private (see below)
))
```

**cache_evict()** - invalidation on write:

```python
Depends(cache_evict(
    eviction_group="products",               # eviction group to evict from
    key_builder=default_key_builder,    # evict the matching key (omit to clear entire eviction group)
    prefix="custom:prefix",             # override the default key prefix
))
```

**cache_put()** - write-through on write:

```python
Depends(cache_put(
    eviction_group="products",               # eviction group to write into
    key_builder=default_key_builder,    # key builder (default: default_key_builder)
    prefix="custom:prefix",             # override the default key prefix
    ttl=300,                            # seconds (default: 0 = no expiry)
    private=True,                       # emit Cache-Control: private
))
```

`private=True` works the same way here as on `cache()` - it adds the
[`Cache-Control: private`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Cache-Control#private)
directive so CDNs and shared proxies do not store the response.  The
entry is still written to Redis for fast subsequent reads; only
intermediate HTTP caches are told to stay out.  See
[Response directives](#http-cache-headers) for more detail.

```python
# User updates their own profile - cache the result in Redis,
# but prevent CDNs from serving Alice's profile to Bob.
@app.put(
    "/me/profile",
    dependencies=[Depends(cache_put(ttl=60, private=True))],
)
async def update_profile(body: Profile, user: User = Depends(get_current_user)):
    return await db.update_profile(user.id, body)
```

### Cache keys

Keys follow the pattern
`{prefix}:{{eviction_group}}:{path}:{sorted_query_params}`.
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
**exact same cache key** automatically - no manual key matching required.

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
directly - see [section 2](#2-cachebackend-imperative-api).

#### Eviction groups and Redis Cluster

In Redis Cluster, keys are distributed across nodes based on their hash
slot (CRC16 of the key modulo 16384).  Without hash tags, keys in the
same logical eviction group would be scattered across multiple nodes, making
bulk operations like `delete_group()` unreliable - `SCAN` only sees
keys on the node it runs on, and Lua scripts cannot touch keys in
different slots.

Hash tags solve this: Redis only hashes the substring inside `{…}` when
computing the slot.  Because all keys in an eviction group share the same
`{eviction_group}` tag, they are guaranteed to land on the **same node and
slot**.  This makes the Lua-based `SCAN` + `UNLINK` script used by
`delete_group()` correct and atomic.

**Trade-off - hot slots:** All keys in one eviction group concentrate on a
single node.  For typical HTTP response caching this is not a problem
(eviction groups are small-to-moderate in size).  If an eviction group grows very
large, consider splitting it into multiple smaller eviction groups to
distribute load across the cluster.

#### Upgrading across a stored-shape change

Entries carry no format marker, and `CacheBackend` keys by the same prefix as
`cache()`. So when a release changes the shape of a stored entry, clear the
cache prefix as part of the upgrade rather than letting two shapes meet.

A rolling deploy cannot do that safely: while old and new pods both serve
traffic they share one keyspace, and flushing at any point in the rollout
leaves the old pods free to repopulate the old shape for the new ones to read.
Upgrade across a shape change with a maintenance window — stop the old
release, clear the prefix, start the new one. The release notes call out the
releases where this applies.

!!! tip "Invalidating on every deploy"

    To get a cold cache on **every** deploy — because your handlers changed
    what they return, say — put a build identifier in the prefix:

    ```bash
    REDIS_PREFIX=redis:fastapi:build-a3f91c
    ```

    Keys then move wholesale each release, old ones expire on their TTL, and
    nothing needs invalidating by hand. This also covers the shape-change
    case, since a new build never reads the previous build's keys.

### HTTP cache headers

Every `cache()` response includes these headers automatically:

| Header | Value |
|--------|-------|
| `X-Redis-Cache` | `HIT`, `MISS`, or `BYPASS` when the response was served but not stored (see [Cache scope](architecture.md#cache-scope)) |
| [`Cache-Control`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control) | `max-age=<remaining_ttl>` when TTL > 0, or `no-cache` when TTL = 0 (always revalidate via ETag). Adds `private` prefix when `private=True`. |
| [`ETag`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag) | The endpoint's own `ETag` when it set one, strong validators included; otherwise a weak tag derived from the body |

**Request directives** - the following `Cache-Control` directives sent by the
client are respected:

- [`If-None-Match`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/If-None-Match) with a matching ETag returns [**304 Not Modified**](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/304).
- [`If-Modified-Since`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/If-Modified-Since) returns **304** when the endpoint set a `Last-Modified` and the client's copy is not older. `If-None-Match` takes precedence when both are sent, as [RFC 9110 §13.2.2](https://www.rfc-editor.org/rfc/rfc9110.html#section-13.2.2) requires.
- `Cache-Control: no-cache` forces a cache refresh.
- `Cache-Control: no-store` bypasses caching entirely.
- `Cache-Control: max-age=N` - a cached entry older than *N* seconds is
  treated as a cache miss and the endpoint re-executes.
  `max-age=0` is equivalent to `no-cache`.

**Response directives** - use `private=True` on the factory to emit
`Cache-Control: private, max-age=…`.  This tells CDNs and shared proxies
**not** to store the response - only the end-user's browser may cache it. See
[MDN: Cache-Control: private](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control#private) and
[MDN: Private caches](https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/Caching#private_caches)

```python
# User-specific data - must not be cached by a CDN
@app.get("/me/profile", dependencies=[Depends(cache(ttl=60, private=True))])
async def my_profile(user: User = Depends(get_current_user)):
    return user.profile
```

### Cache scope

`cache()` stores **text representations** only — JSON, plain text, HTML, CSV,
XML, JavaScript. It refuses anything else, including all binary, and a refusal
never changes what the caller receives: the response is served whole and marked
`X-Redis-Cache: BYPASS`, with the reason logged once per route. An entry also
replays the header fields your endpoint set, apart from the ones this library
owns and the ones it withholds by policy — `Date` and `Set-Cookie` among them.

For the full allowlist, every refusal reason, the headers a hit carries, and
the middleware ordering that compression requires, see
[Cache scope](architecture.md#cache-scope) in the architecture guide.

### RFC 9111 conformance

[RFC 9111](https://www.rfc-editor.org/rfc/rfc9111.html) governs HTTP caching,
and a Redis entry shared between requests is a **shared cache** under it. Where
this library follows the specification and where it deliberately does not:

| Section                                                             | Requirement                                                                                                                                                                                      | This library                                                                                                                                                                                                                                                                                                                                                                            |
|---------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [§3](https://www.rfc-editor.org/rfc/rfc9111.html#section-3)         | Store only a cacheable method and a status the cache can reproduce                                                                                                                               | **Follows.** `cache()` reads and stores `GET` only, and stores only a `200`. An entry carries no status, so a stored `201` would come back as a `200`; refusing it keeps the status honest. `cache_put()` stores any `2xx`, because its body is installed as the representation for a *later* `GET` and the writing response is never replayed.                                         |
| [§3.1](https://www.rfc-editor.org/rfc/rfc9111.html#section-3.1)     | "Caches MUST include all received response header fields … when storing a response"                                                                                                              | **Follows in part.** An entry stores every received header field, including unrecognized ones, apart from five groups: the fields this library re-emits, the connection-specific and proxy-specific fields §3.1 itself excludes, the recomputed `Content-Length`, and `Date` and `Set-Cookie` by decision. Trailer fields are discarded, which §3.1 permits, and are never merged into the header block. The two remaining deviations are deliberate: replaying `Date` would double-count the entry's age (see the warning below), and replaying `Set-Cookie` from a shared entry would leak a session. See [Headers a hit carries](architecture.md#headers-a-hit-carries). |
| [§3.2](https://www.rfc-editor.org/rfc/rfc9111.html#section-3.2)     | Update stored header fields on validation                                                                                                                                                        | **Not applicable.** This is an origin-side cache. It never revalidates against an upstream.                                                                                                                                                                                                                                                                                             |
| [§3.3](https://www.rfc-editor.org/rfc/rfc9111.html#section-3.3)     | "a cache MUST NOT store incomplete or partial-content responses if it does not support the Range and Content-Range header fields", and MUST NOT send a partial response without marking it `206` | **Follows.** Three guards: a response carrying `Content-Range` is refused, a request carrying `Range` is refused, and any status other than `200` is refused. Without them a `206` would be stored under the full-resource key and replayed to the next client as a truncated `200`.                                                                                                    |
| [§3.4](https://www.rfc-editor.org/rfc/rfc9111.html#section-3.4)     | Combining partial content                                                                                                                                                                        | **Not applicable.** No partial entry is ever stored.                                                                                                                                                                                                                                                                                                                                    |
| [§3.5](https://www.rfc-editor.org/rfc/rfc9111.html#section-3.5)     | "A shared cache MUST NOT use a cached response to a request with an Authorization header field" unless the response permits shared storage                                                       | **Deviates.** Read the warning below before caching an authenticated route.                                                                                                                                                                                                                                                                                                             |
| [§4](https://www.rfc-editor.org/rfc/rfc9111.html#section-4)         | Reuse only on a matching URI and method, while fresh                                                                                                                                             | **Follows.** The key is the path plus sorted query, only `GET` reads it, and the Redis TTL is the freshness lifetime.                                                                                                                                                                                                                                                                   |
| [§4](https://www.rfc-editor.org/rfc/rfc9111.html#section-4)         | "a cache MUST generate an Age header field"                                                                                                                                                      | **Deviates deliberately.** `Cache-Control: max-age` is emitted already reduced by the entry's age, so a downstream cache works out the same remaining freshness an `Age` header would give it. Sending both would subtract the age twice and expire the response early. **Read the warning below before changing this.**                                                                |
| [§4.1](https://www.rfc-editor.org/rfc/rfc9111.html#section-4.1)     | Match every request header nominated by `Vary`                                                                                                                                                   | **Deviates in part.** The key never includes a request header, so `Vary` is not honoured on lookup. It *is* stored and replayed, so a cache downstream still receives the origin's instruction, and a response carrying `Vary: *` — which may never be reused — is refused rather than stored. See [`Vary` is not honoured](architecture.md#vary-is-not-honoured).                              |
| [§4.2](https://www.rfc-editor.org/rfc/rfc9111.html#section-4.2)     | Freshness                                                                                                                                                                                        | **Follows.** Redis owns it. An expired entry is a missing key, so a stale entry cannot be read.                                                                                                                                                                                                                                                                                         |
| [§4.3](https://www.rfc-editor.org/rfc/rfc9111.html#section-4.3)     | Validation                                                                                                                                                                                       | **Follows.** `If-None-Match` against the stored `ETag` returns `304`, and `If-Modified-Since` against a stored `Last-Modified` does too — with `If-None-Match` taking precedence when both are present, as §13.2.2 requires. An endpoint that sets its own `ETag` keeps it, strong validator included; when it sets none we generate a weak tag from the body, and a weak validator cannot serve a range request — one more reason `Range` is refused. |
| [§4.4](https://www.rfc-editor.org/rfc/rfc9111.html#section-4.4)     | "A cache MUST invalidate the target URI … when it receives a non-error status code in response to an unsafe request method"                                                                      | **Deviates.** A `POST`, `PUT`, `PATCH` or `DELETE` does not drop the entry by itself. Put `cache_evict()` or `cache_put()` on the writing route — see [Combining patterns](#combining-patterns).                                                                                                                                                                                        |
| [§5.2.1](https://www.rfc-editor.org/rfc/rfc9111.html#section-5.2.1) | Request directives                                                                                                                                                                               | **Follows in part.** Honours `no-store`, `no-cache` and `max-age`. Ignores `min-fresh`, `max-stale`, `only-if-cached` and `no-transform`.                                                                                                                                                                                                                                               |
| [§5.2.2](https://www.rfc-editor.org/rfc/rfc9111.html#section-5.2.2) | Response directives                                                                                                                                                                              | **Follows in part.** Refuses to store a response that sets `no-store` or `private`, since a Redis entry is shared. Emits `private` when `private=True`.                                                                                                                                                                                                                                 |

!!! warning "Authenticated routes are not keyed per user"

    A `cache()` entry is **shared**. The key does not include the caller, so a
    route behind `Authorization` serves the first caller's body to everyone
    until the TTL expires. RFC 9111 §3.5 forbids exactly this, and the library
    does not stop you — `private=True` changes the `Cache-Control` header it
    emits, not the key it writes.

    Before caching a route whose body depends on who is asking, fold the
    caller into the key:

    ```python
    def key_per_user(request, eviction_group="", prefix=""):
        base = default_key_builder(
            request, eviction_group=eviction_group, prefix=prefix
        )
        return f"{base}:u:{request.state.user_id}"

    @app.get(
        "/me/profile",
        dependencies=[
            Depends(cache(ttl=60, private=True, key_builder=key_per_user))
        ],
    )
    async def my_profile(user: User = Depends(get_current_user)): ...
    ```

    If the body is identical for every caller — an auth check that gates
    access without changing the content — the shared entry is correct and
    needs no change.

!!! warning "Do not add an Age header on its own"

    On a hit the library emits `Cache-Control: max-age=<seconds the Redis key
    has left>` and **no** `Age` header, while the ASGI server stamps a fresh
    `Date` on every response. A hit therefore looks like a response generated
    just now that happens to have less life left:

    ```text
    hit 1   cache-control: max-age=120   date: 13:43:17   (redis ttl 120)
    hit 2   cache-control: max-age=117   date: 13:43:20   (redis ttl 117)
    hit 3   cache-control: max-age=114   date: 13:43:23   (redis ttl 114)
    ```

    Those three facts are **one decision, not three**. A downstream cache
    works out `current_age` from the `Age` header and from `Date`, then serves
    the response while `max-age > current_age`. Because `Date` is always now
    and `Age` is absent, `current_age` starts at zero, so the reduced
    `max-age` *is* the whole remaining lifetime. The arithmetic comes out
    right — but only because all three parts agree.

    Change one part on its own and it stops coming out right. Take an entry
    with a 300-second TTL and 120 seconds left, so its age is 180:

    - **Add an `Age` header while `max-age` stays reduced** and the age counts
      twice. The hit carries `max-age=120` and `Age: 180`; a downstream cache
      computes `current_age = 180`, finds `120 > 180` false, and treats every
      hit as **stale on arrival**. Downstream caching collapses silently — the
      responses still look correct in a browser.
    - **Store and replay the entry's original `Date`** and the same
      double-count happens with no `Age` header at all, because the cache then
      derives the age from `Date` instead of from zero.

    To conform to RFC 9111 §4, change the whole encoding together: store the
    configured TTL in the entry, emit that constant value as `max-age`, and
    emit `Age` as `stored_ttl - remaining_ttl`. A hit then carries
    `max-age=300` with `Age: 180`, which leaves the same 120 seconds, and
    replaying `Date` becomes safe as well.

    Half of that change is worse than none of it.

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

## 2. `CacheBackend` - imperative API

For caching that doesn't map to "one endpoint, one response" - a TTL that
depends on the data, a decision to cache taken after the work is done, or
sub-computations cached independently of the response - inject
`CacheBackendDep`.  It needs only a Redis connection; `.caching()` is not
required for the backend itself.

```python
from redis_fastapi import CacheBackendDep

@app.get("/items/{item_id}")
async def get_item(item_id: int, cache: CacheBackendDep):
    cached = await cache.get(f"item:{item_id}", eviction_group="items")
    if cached is not None:
        return cached
    item = await db.get_item(item_id)
    await cache.set(f"item:{item_id}", item, ttl=300, eviction_group="items")
    return item
```

That is [cache-aside](https://redis.io/learn/howtos/solutions/microservices/caching)
written by hand - and it is exactly what `cache()` does for you, so if this is
all you need, use `cache()`.  The recipes below are the cases it cannot express.

If you are using only `CacheBackend` (no `cache()` / `cache_evict()` /
`cache_put()`), setup is just:

```python
app = FastAPI()
FastAPIRedis(app).lifespan()
```

| Method                                              | Description                                                                                                                         |
|-----------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| `get(key, *, default=None, eviction_group=None)`    | Retrieve and deserialize.  Returns `default` on miss.                                                                               |
| `set(key, value, *, ttl=None, eviction_group=None)` | Serialize and store.  `ttl` accepts `int` seconds or `timedelta`; omit it to fall back to `default_ttl`, or pass `0` for no expiry. |
| `delete(key, *, eviction_group=None)`               | Delete a single entry.  Returns `True` if it existed.                                                                               |
| `has(key, *, eviction_group=None)`                  | Check existence without deserializing (Redis `EXISTS`).                                                                             |
| `delete_group(eviction_group=None)`                 | Delete every key in an eviction group.  Returns the count.                                                                          |

Four things about the semantics are worth knowing before you build on them:

* **A miss and an outage look identical.**  `get` returns `default` (`None`
  unless you pass one) not only on a miss but also on a Redis error or a decode
  failure, and `set` silently does nothing when Redis is down.  That is
  deliberate - a cache outage degrades to recomputation instead of a 500 - but
  unlike `RateLimitResult.degraded` there is no flag distinguishing the two.
  The failure is logged and visible in the [metrics](observability.md); your
  handler cannot see it.
* **`ttl=None` means no expiry**, as does any value below `1`.  The key stays
  until it is deleted or evicted by Redis' own memory policy.  Pass an explicit
  TTL unless you mean forever.
* **`delete_group()` with no group is a full cache wipe.**  With neither a call
  argument nor an instance-level group, it deletes every key under the cache
  prefix.  It logs a warning and proceeds.
* **Keys interoperate with `cache()`.**  Both produce
  `{prefix}:{eviction_group}:...`, so `delete_group("items")` clears
  decorator-written and backend-written entries alike - see
  [Cache keys](#cache-keys).

A `SyncCacheBackend` facade (`SyncCacheBackendDep`) offers the same methods for
sync endpoints, delegating to the async backend on a worker thread.

### When to reach for the backend

`cache()` fixes everything at decoration time: one TTL, a key derived from the
request, and "cache whatever the handler returns".  It wraps the handler, so the
response is the only thing it can see or store, and the handler never learns
whether a cache was involved.

Reach for the backend when one of those has to change - the TTL, the key, the
*decision* to cache, or the granularity (part of the work rather than the whole
response) - or when there is no `Request` at all.  For plain invalidation or
write-through, prefer `cache_evict()` / `cache_put()`, which do require
`.caching()`.  The recipes below cover the common cases.

#### Recipe: typed values with Pydantic models

`cache()` stores the serialized HTTP response, so there is nothing to type on the
way back.  The backend hands values to *your* code, which makes the coder worth
choosing.

By default, `CacheBackend` uses `JsonCoder`, a thin wrapper around
`json.dumps()` / `json.loads()`, which cannot serialize Pydantic models,
`datetime`, `UUID`, enums, or `Decimal`. To cache a Pydantic model, wrap it with
`pydantic_model_coder()`: the resulting coder serializes with the model's own
JSON encoder on write and validates back into a model instance on read, so cache
hits come back fully typed.

Because a coder is model-specific, you select it the **DI-native** way - by
declaring a model-specific `CacheBackend` provider and injecting it, exactly as
you would with `get_db` or `get_current_user`. The default `CacheBackendDep`
stays on `JsonCoder`; this provider is a parallel dependency that carries the
`Product` coder (and its eviction group):

```python
from typing import Annotated
from datetime import datetime
from uuid import UUID

from fastapi import Depends
from pydantic import BaseModel
from redis_fastapi import AsyncRedisDep, CacheBackend, pydantic_model_coder

class Product(BaseModel):
    id: UUID
    name: str
    created_at: datetime

# Wire the coder into a model-specific CacheBackend provider, then alias it.
ProductCoder = pydantic_model_coder(Product)

async def get_product_cache(redis: AsyncRedisDep) -> CacheBackend:
    return CacheBackend(redis, coder=ProductCoder, eviction_group="products")

ProductCacheDep = Annotated[CacheBackend, Depends(get_product_cache)]

@app.get("/products/{product_id}")
async def get_product(product_id: UUID, cache: ProductCacheDep) -> Product:
    cached = await cache.get(f"product:{product_id}")
    if cached is not None:
        return cached                          # a real Product instance, fully typed
    product = await db.get_product(product_id)
    await cache.set(f"product:{product_id}", product, ttl=300)
    return product
```

`get_product_cache` reuses the shared connection pool via `AsyncRedisDep`, so the
model-specific backend costs nothing extra beyond the coder. Add one such
provider per model you cache.

#### Recipe: conditional caching

Cache only when a business rule is met.  `cache()` has no say in the matter - it
stores whatever the handler returns - so a draft or a partial result gets cached
alongside the publishable ones:

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

#### Recipe: intermediate result caching

Cache the expensive *parts* rather than the response, so each part carries its
own TTL and can be invalidated on its own schedule.  `cache()` sees one response
and one TTL, which means a cheap 60-second fragment and an expensive 2-hour one
must share whichever number you picked:

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

#### Recipe: cascade invalidation across eviction groups

One write makes several unrelated caches stale.  `cache_evict()` clears the group
attached to its own route, so a fan-out across groups needs explicit deletes:

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

#### Recipe: TTL from the data

Let the value decide how long it lives.  `cache(ttl=...)` is resolved when the
route is defined, before any data exists, so it cannot tell a premium record from
a free one:

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

#### Recipe: read-modify-write a cached value

Derive the new value from the cached one.  `cache()` never shows the handler what
is in the cache, so any update that depends on the current value has to be done
here:

```python
@app.post("/products/{product_id}/view")
async def record_view(product_id: int, cache: CacheBackendDep):
    views = await cache.get(f"views:{product_id}", default=0, eviction_group="analytics")
    views += 1
    await cache.set(f"views:{product_id}", views, ttl=3600, eviction_group="analytics")
    return {"product_id": product_id, "views": views}
```

!!! warning "This read-modify-write is not atomic"

    Two concurrent requests can both read `views=5` and both write `6`, losing a
    view.  That is acceptable for an approximate counter and wrong for anything
    you bill or audit.  When the count has to be exact, let Redis do the
    arithmetic with `INCR` on the raw client (`AsyncRedisDep`) instead of a
    get/set pair - a cache backend is a value store, not a counter:

    ```python
    @app.post("/products/{product_id}/view")
    async def record_view(product_id: int, redis: AsyncRedisDep):
        views = await redis.incr(f"views:{product_id}")   # atomic, one round trip
        return {"product_id": product_id, "views": views}
    ```

#### Recipe: skip expensive work when the cache is warm

`has` answers "is this cached?" without transferring or deserializing the value -
useful when the decision costs less than the payload:

```python
@app.get("/warm-check/{product_id}")
async def check_warm(product_id: int, cache: CacheBackendDep):
    if await cache.has(f"product:{product_id}", eviction_group="products"):
        return {"warm": True}

    # Only do expensive work when cache is cold
    await run_expensive_recomputation(product_id)
    return {"warm": False}
```

#### Recipe: caching outside HTTP

There is no `Request` in a background worker, a scheduled job, or a queue
consumer - but the cache is the same one your endpoints read, so a worker can
warm it or reuse it.  Construct a `CacheBackend` from any Redis client:

```python
from datetime import timedelta

from redis_fastapi import CacheBackend

async def refresh_exchange_rates(redis):
    cache = CacheBackend(redis, eviction_group="rates")   # no Request needed
    rates = await fetch_from_vendor()                     # paid API, called once
    await cache.set("fx:latest", rates, ttl=timedelta(minutes=15))

@app.get("/rates")
async def rates(cache: CacheBackendDep):
    # Endpoints read what the worker wrote — same key, same eviction group.
    return await cache.get("fx:latest", default={}, eviction_group="rates")
```

This is also the shape to use when a value is expensive to produce but cheap to
serve: pay for it on a schedule rather than on whichever unlucky request finds
the cache cold.

The `default=` argument keeps the miss path from leaking `None` into your
response - `default={}` above, or `default=0` in the counter recipe - and `ttl`
accepts a `timedelta` wherever seconds would do.

Notice what the recipes have in common: each one **reads the cache before
deciding what to do next**.  That is the structural difference from `cache()`,
which resolves the cache entirely outside the handler and hands it a decision
already made.

### Keys and eviction groups

Backend keys follow the same scheme as decorator keys -
`{prefix}:{eviction_group}:{your key}`, with the group wrapped in Redis hash-tag
braces so a whole group lands on one cluster slot.  The consequences are covered
under [Cache keys](#cache-keys); the two that matter most here:

* The `eviction_group` argument can be set per call or once on the instance
  (`CacheBackend(redis, eviction_group="products")`), which is what the Pydantic
  provider above does.
* A group is the unit of bulk eviction.  Keys written by `cache()` and keys
  written by the backend sit in the same namespace, so grouping them together is
  what makes one `delete_group()` clear both.

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
| [Conditional caching](#recipe-conditional-caching)                                                     |        ❌        |                ❌                |      ✅       |
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

| Scenario                                 | Recommended                                                           |
|------------------------------------------|-----------------------------------------------------------------------|
| Most GET endpoints                       | [`cache()`](#1-caching-factories)                                     |
| User-specific / authenticated endpoints  | [`cache(private=True)`](#http-cache-headers)                          |
| POST/PUT that invalidates a GET          | [`cache_evict()`](#basic-usage)                                       |
| Write-through (update cache on write)    | [`cache_put()`](#basic-usage)                                         |
| Public catalog, high traffic             | [`cache()`](#1-caching-factories)                                     |
| Complex multi-step invalidation          | [`CacheBackend`](#recipe-cascade-invalidation-across-eviction-groups)  |
| Conditional caching (business rules)     | [`CacheBackend`](#recipe-conditional-caching)                          |
| Cache sub-computations independently     | [`CacheBackend`](#recipe-intermediate-result-caching)                  |
| TTL that depends on the data             | [`CacheBackend`](#recipe-ttl-from-the-data)                            |
| Typed values (Pydantic models)           | [`CacheBackend`](#recipe-typed-values-with-pydantic-models)            |
| Background job / no `Request` available  | [`CacheBackend`](#recipe-caching-outside-http)                         |
| Exact counters                           | `AsyncRedisDep` + `INCR` - [not a cache](#recipe-read-modify-write-a-cached-value) |

---

## Best practices

1. **Use `FastAPIRedis(app).lifespan().caching()`** for app setup.
2. **Start with `cache()`** for GET endpoints - it is the simplest option.
3. **Add `cache_evict()`** on write endpoints that should invalidate cached reads.
4. **Use `cache_put()`** when the write result should immediately warm the cache.
5. **Switch to CacheBackend** when you need conditional logic or complex flows.
6. **Always set explicit TTLs** - see [TTL defaults](#ttl-defaults) below.
8. **Use eviction groups** to group related keys and enable bulk invalidation.
9. **Use `dependency_overrides`** in tests - no monkey-patching needed.
10. **Do not over-cache** - cache only what is expensive to recompute.

---

## TTL defaults

By default, `default_ttl` is **0** - cache entries have **no automatic
expiration** and persist until explicitly evicted (via `cache_evict()`,
`delete_group()`, or Redis memory eviction policies like `allkeys-lru`).

`default_ttl` applies to both public APIs.  `cache()`, `cache_put()`, and
`CacheBackend.set()` all fall back to it when `ttl` is omitted; pass `ttl=0`
to store without an expiry whatever the default is.

This is a deliberate design choice:

1. **A caching library's job is to cache, not to expire.** Expiry is an
   application-level policy decision. Only you know whether your data changes
   every second or every month - a library-imposed default (e.g. 60 seconds,
   5 minutes) is wrong for most use cases. The library should provide excellent
   TTL *support*, not impose a TTL *opinion*.

2. **The real protection against stale data is explicit invalidation.**
   `cache_evict()` and `cache_put()` factories, plus `CacheBackend.delete()`
   and `delete_group()`, give you precise control over when stale data
   is removed. TTL is a coarse safety net, not a substitute for proper
   invalidation.

3. **Redis can protect you against memory exhaustion - under one policy family.**
   Only the `allkeys-*` [eviction policies](https://redis.io/docs/latest/develop/reference/eviction/)
   reclaim keys that carry no TTL.  `noeviction` (the Redis OSS default) and
   every `volatile-*` policy evict only keys that already have an expiry, so
   entries cached without one can never be reclaimed: Redis rejects writes
   with an OOM error instead of evicting.  Note that `volatile-lru` is the
   default on AWS ElastiCache, Azure Cache for Redis, and Google Memorystore,
   so relying on `default_ttl = 0` there means changing your server's
   eviction policy, not accepting its default.

   The SDK checks this for you.  When caching is wired and a route falls back
   to a `default_ttl` of `0`, the lifespan reads `INFO memory` once at startup
   and warns if the server cannot evict un-expiring keys.  Set an
   `allkeys-*` policy, set `REDIS_DEFAULT_TTL`, or silence the check with
   `REDIS_WARN_UNBOUNDED_CACHE=false`.

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
