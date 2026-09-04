# API Reference

## Setup

```python
from fastapi import FastAPI
from redis_fastapi import FastAPIRedis

app = FastAPI()
FastAPIRedis(app).lifespan()                    # connection pools only
FastAPIRedis(app).lifespan().caching()          # + DI caching support
FastAPIRedis(app).lifespan().rate_limiting()    # + DI rate limiting support
FastAPIRedis(app).lifespan().sessions()         # + server-side sessions
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

`Annotated[AsyncRedis | AsyncRedisCluster, Depends(get_async_redis)]` - returns a cached async Redis client backed by the shared connection pool. Returns `AsyncRedisCluster` when `settings.cluster` is `True`.

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

`Annotated[CacheBackend, Depends(get_cache_backend)]` - async cache backend with `get`/`set`/`delete`/`has`/`delete_group`. Use for conditional caching, cascade invalidation, and dynamic TTL.

`CacheBackend(redis, coder=...)` accepts any `Coder`. Use
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

`Annotated[SyncCacheBackend, Depends(get_sync_cache_backend)]` - blocking facade over `CacheBackend`, bridges async calls via `anyio.from_thread.run`. **Only works from sync endpoints** running in FastAPI's worker threads.

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

---

## Rate limit backend dependencies

### `RateLimitBackendDep`

```python
from fastapi import HTTPException
from redis_fastapi import RateLimitBackendDep

@app.post("/send-email")
async def send_email(to: str, limiter: RateLimitBackendDep):
    result = await limiter.hit(f"email:{to}", limit=3, window=3600)
    if not result.allowed:
        raise HTTPException(429, f"retry in {result.retry_after}s")
    ...
```

`Annotated[RateLimitBackend, Depends(get_rate_limit_backend)]` - imperative window-counter backend for custom keys, dynamic cost, or non-HTTP flows. Needs only a Redis connection; `.rate_limiting()` is **not** required for the backend itself.

| Method                                                                                       | Description                                 |
|----------------------------------------------------------------------------------------------|---------------------------------------------|
| `hit(identifier, *, limit, window, scope=None, cost=1, fail_closed=None) -> RateLimitResult` | Consume `cost` units against the counter.   |
| `peek(identifier, *, limit, window, scope=None) -> RateLimitResult`                          | Report current state **without** consuming. |
| `reset(identifier, *, scope=None) -> bool`                                                   | Clear the counter; `True` if it existed.    |

### `SyncRateLimitBackendDep`

```python
from redis_fastapi import SyncRateLimitBackendDep

@app.post("/sync-send")
def sync_send(to: str, limiter: SyncRateLimitBackendDep):
    if not limiter.hit(f"email:{to}", limit=3, window=3600).allowed:
        raise HTTPException(429, "slow down")
    ...
```

`Annotated[SyncRateLimitBackend, Depends(get_sync_rate_limit_backend)]` - blocking facade over `RateLimitBackend`, bridges async calls via `anyio.from_thread.run`. **Only works from sync endpoints** running in FastAPI's worker threads.

### `get_rate_limit_backend()` / `get_sync_rate_limit_backend()`

```python
async def get_rate_limit_backend(request: Request) -> RateLimitBackend
async def get_sync_rate_limit_backend(request: Request) -> SyncRateLimitBackend
```

Factory functions underlying the type aliases above. The backend's server-capability cache (INCREX / EVAL support) lives on the pool state, so detection is paid once per process rather than per request.

---

## DI rate limiting

### `rate_limit()`

```python
from fastapi import Depends
from redis_fastapi import rate_limit

@app.get("/api", dependencies=[Depends(rate_limit("100/minute"))])
async def api():
    ...
```

Returns a `Depends()`-compatible dependency. Requires `FastAPIRedis(app).rate_limiting()` (or `add_redis_rate_limiting(app)`) so the 429 handler and header middleware are registered. Allowed responses carry `X-RateLimit-*` headers; over-limit requests raise a 429 with `Retry-After`.

| Parameter           | Type                                                          | Default                            | Description                                                                                           |
|---------------------|---------------------------------------------------------------|------------------------------------|-------------------------------------------------------------------------------------------------------|
| `rate`              | `str \| Rate \| tuple[int, int] \| None`                      | `None`                             | Fluent rate (`"100/minute"`), `Rate`, or `(limit, window)`. Mutually exclusive with `limit`/`window`. |
| `limit`             | `int \| None`                                                 | `None`                             | Request limit (use with `window`).                                                                    |
| `window`            | `int \| None`                                                 | `None`                             | Window in seconds (use with `limit`).                                                                 |
| `scope`             | `str`                                                         | matched route template             | Counter namespace **and** unit of sharing.                                                            |
| `identifier`        | `Identifier \| None`                                          | `ip_identifier`                    | Returns the per-client **identity** segment of the key.                                               |
| `cost`              | `int`                                                         | `1`                                | Units this request consumes.                                                                          |
| `skip_when`         | `Callable[[Request], bool]` (sync/async)                      | `None`                             | Truthy ⇒ request not counted.                                                                         |
| `on_limit_exceeded` | `Callable[[Request, RateLimitResult], Response]` (sync/async) | `None`                             | Builds the 429 response.                                                                              |
| `emit_headers`      | `bool \| None`                                                | `settings.rate_limit_emit_headers` | Emit the `X-RateLimit-*` trio.                                                                        |
| `ietf_headers`      | `bool \| None`                                                | `settings.rate_limit_ietf_headers` | Also emit IETF `RateLimit` / `RateLimit-Policy`.                                                      |
| `fail_closed`       | `bool \| None`                                                | `settings.rate_limit_fail_closed`  | Reject (vs allow) when Redis is unreachable.                                                          |

### `add_redis_rate_limiting()` / `FastAPIRedis.rate_limiting()`

```python
FastAPIRedis(app).lifespan().rate_limiting(global_rate="1000/minute")
# or, without the builder:
add_redis_rate_limiting(app, global_rate="1000/minute")
```

Registers the `RateLimitExceeded` handler and the `RateLimitMiddleware` (header injection + optional global limiter). Idempotent. A global limiter is wired when `global_rate` is provided, or when `REDIS_RATE_LIMIT_DEFAULT_LIMIT > 0`.

| Parameter           | Type                                                          | Default                            | Description                                    |
|---------------------|---------------------------------------------------------------|------------------------------------|------------------------------------------------|
| `global_rate`       | `str \| Rate \| tuple[int, int] \| None`                      | `None`                             | Enables an app-wide limiter at this rate.      |
| `identifier`        | `Identifier \| None`                                          | `ip_identifier`                    | Identity strategy for the global limiter.      |
| `scope`             | `str`                                                         | `""`                               | Scope for the global limiter's counters.       |
| `skip_when`         | `Callable[[Request], bool]` (sync/async)                      | `None`                             | Skip predicate for the global limiter.         |
| `on_limit_exceeded` | `Callable[[Request, RateLimitResult], Response]` (sync/async) | `None`                             | Custom 429 builder for the global limiter.     |
| `ietf_headers`      | `bool \| None`                                                | `settings.rate_limit_ietf_headers` | Emit IETF headers on global responses.         |
| `fail_closed`       | `bool \| None`                                                | `settings.rate_limit_fail_closed`  | Reject on Redis errors for the global limiter. |

---

## Identifiers and rate definitions

### `ip_identifier` / `Identifier`

```python
def ip_identifier(request: Request) -> str          # default identifier (client IP)

Identifier = Callable[[Request], str | Awaitable[str]]
```

An `Identifier` returns the **client identity** segment of the counter key (e.g. an IP or user id) - the backend adds `scope` and `prefix`. Pass any callable as `identifier=` to key by user, API key, or tenant. `ip_identifier` returns `request.client.host` and never parses `X-Forwarded-For` - configure proxy headers at the ASGI server (Uvicorn `--proxy-headers`), or supply your own identifier. See [Behind a proxy](../guide/rate-limiting.md#behind-a-proxy).

### `CannotIdentifyClient`

```python
class CannotIdentifyClient(Exception)
```

Raise from an `Identifier` that cannot tell callers apart. The limiter then treats the check like a Redis outage: `RateLimitResult.degraded` is `True`, the `error` metric is recorded, and `fail_closed` decides whether the request passes uncounted or gets a 429 - so the request is never counted against a shared placeholder bucket. `ip_identifier` raises it when the ASGI scope has no client address. See [Callers that cannot be identified](../guide/rate-limiting.md#callers-that-cannot-be-identified).

### `Rate` / `parse_rate()`

```python
from redis_fastapi import Rate, parse_rate

Rate(limit=100, window=60)      # limit within a window (seconds)
Rate.per_minute(100)            # also per_second / per_hour / per_day
parse_rate("100/minute")        # "5/second", "10/15seconds", "100 per 2 minutes"
```

`parse_rate(spec: str | Rate | tuple[int, int]) -> Rate` coerces any accepted form into a frozen `Rate(limit, window)`. `str(rate)` round-trips to the canonical spec.

---

## `RateLimitResult`

```python
@dataclass(frozen=True)
class RateLimitResult:
    allowed: bool
    limit: int
    remaining: int
    reset_after: int        # seconds until the window resets
    reset_at: int           # Unix epoch second of reset (X-RateLimit-Reset)
    retry_after: int        # seconds to wait before retrying (Retry-After)
    backend: str = ""       # "increx" | "lua"; empty for peek / Redis-unreachable
    degraded: bool = False  # True when served by the fail-open/closed fallback
```

Returned by `hit()` / `peek()`, and stashed on `request.state` so the middleware can emit headers.

---

## `RateLimitExceeded` / `RateLimitMiddleware`

`RateLimitExceeded(Exception)` - control-flow exception raised by `rate_limit()` when a request is over the limit; carries the prebuilt 429 `Response`. It is turned into that response automatically once `add_redis_rate_limiting` (or `.rate_limiting()`) has registered its handler.

`RateLimitMiddleware` - ASGI middleware that injects `X-RateLimit-*` headers on allowed responses and, when a global limit is configured, enforces it before routing. Registered for you by `add_redis_rate_limiting`; you rarely construct it directly.


---

## Session dependencies

### `SessionDep`

```python
from redis_fastapi import SessionDep

@app.post("/login")
async def login(session: SessionDep):
    session["user_id"] = 42          # writing the identity rotates the ID
```

`Annotated[Session, Depends(get_session)]` - the session payload as a `dict`
subclass. Loaded before the application runs, so this performs no I/O.
`request.session` returns the same object.

### `SessionStateDep`

```python
from redis_fastapi import SessionStateDep, SessionStoreDep

@app.post("/logout")
async def logout(state: SessionStateDep, store: SessionStoreDep):
    await store.revoke(state)
```

`Annotated[SessionState, Depends(get_session_state)]` - everything about the
session that is not its data: `session_id`, `subject`, `created`,
`absolute_remaining`, and the `revoked` / `rotated` flags. Operations that act
on the session rather than its contents take this rather than the payload.

### `SessionStoreDep` / `SyncSessionStoreDep`

```python
async def handler(store: SessionStoreDep): ...   # async endpoints
def handler(store: SyncSessionStoreDep): ...     # def endpoints
```

`Annotated[SessionStore, Depends(get_session_store)]`. Typed as the abstract
base, not the Redis implementation, so a substituted backend type-checks.
`SyncSessionStore` bridges each call via `anyio.from_thread.run` and is usable
only from FastAPI worker threads.

### `get_session()` / `get_session_state()` / `get_session_store()` / `get_sync_session_store()`

```python
async def get_session_store(request: Request) -> SessionStore
```

`get_session_store` resolves in three steps: an entry in
`app.dependency_overrides`, then a `store` or `store_factory` given to
`add_redis_sessions`, then a `RedisSessionStore` built from the remaining
options. It consults the override map itself, so one override covers both the
middleware and your handlers.

---

## Session setup

### `add_redis_sessions()` / `FastAPIRedis.sessions()`

```python
FastAPIRedis(app).lifespan().sessions(
    principal_keys=["user_id", "role"],
    subject_of=lambda s: s.get("tenant_id"),
    descriptor_of=lambda req, s: {"ip": req.client.host},
    cookie_builder=my_builder,
    skip=lambda req: req.url.path.startswith("/health"),
    store=None, store_factory=None,
    coder=JsonCoder, encryptor=None, id_factory=None, key_prefix=None,
    idle_ttl=None, absolute_ttl=None, gc_ttl=None,
)
```

| Argument | Purpose |
|---|---|
| `principal_of` / `principal_keys` | What a change to triggers a rotation. Must be pure and return a verified identity. |
| `subject_of` | What the reverse index is keyed on. `None` leaves the session out of it. |
| `descriptor_of` | What a device listing shows. Stored beside the session, never inside it. |
| `cookie_builder` | Renders `Set-Cookie`, for setting **and** clearing. |
| `skip` | Requests needing no session, at zero Redis cost. |
| `store` / `store_factory` | Supply a whole store. Mutually exclusive. |
| `coder`, `encryptor`, `id_factory`, `key_prefix`, `idle_ttl`, `absolute_ttl`, `gc_ttl` | Passed to the store constructor. TTLs accept `int` seconds or `timedelta`. |

---

## `SessionStore`

Abstract base owning the lifecycle. `RedisSessionStore` is the shipped
implementation; `SessionStoreProtocol` is the structural type for substituting
one without inheritance.

| Method | Purpose |
|---|---|
| `load(session_id, *, refresh=True)` | Read a session and restart its idle clock. `None` for every way it can be absent. |
| `create(session_id, record)` | Write a session that does not exist yet. The only method that writes the absolute deadline. |
| `save(session_id, record)` | Update the payload, and only the payload. |
| `touch(session_id)` | Restart the idle clock alone. |
| `delete(session_id)` | Remove the session. |
| `rotate(state, *, subject=None, descriptor=None)` | New identifier, old key deleted first. Returns the new ID. |
| `reauthenticate(state)` | Ask the middleware to rotate on the way out. |
| `revoke(state, *, subject=None)` | End this session and drop its index entry. |
| `revoke_id(session_id, *, subject)` | End one session, refusing an ID not indexed under that subject. |
| `revoke_all(subject)` | End every session of a subject. Returns keys removed. |
| `list_for_subject(subject)` | Live sessions, verified before being reported. |
| `count_for_subject(subject, *, limit=None)` | Upper-bound count; exact only at or above `limit`. Raises rather than failing open. |
| `index(subject, session_id, record, *, absolute_remaining, extra=None)` | Record a session under its subject. |
| `new_id()` / `is_valid_id(value)` | Generate and validate identifiers. |
| `new_record(data, *, created=None)` | Build an envelope, carrying `created` forward. |
| `session_id(state)` | The current identifier, or `None`. |

A new backend implements the abstract primitives: `_read`, `_write`,
`_expire`, `_delete`, `_delete_many`, `_index_add`, `_index_remove`,
`_index_clear`, `_index_members`, `_alive`.

---

## Session data types

| Type | Contents |
|---|---|
| `Session` | `dict` subclass with `accessed` / `modified` flags and `raw()`. Nothing else. |
| `SessionState` | `data`, `session_id`, `subject`, `created`, `absolute_remaining`, `revoked`, `rotated`. |
| `SessionRecord` | `data` plus `SessionMetadata`. |
| `SessionMetadata` | `created`, `last_access`, `lifetime`. |
| `SessionInfo` | One listing row: `session_id`, `created`, `last_access`, `descriptor`. |
| `LoadedSession` | `record` plus `absolute_remaining`, returned by `load()`. |
| `CookieSpec` | Cookie attributes; `cleared()` returns the deletion form. |
| `Outcome` | What a response owes the session. Eight members. |
| `Deadline` | `ABSENT` / `UNBOUNDED` — a deadline that is not a number. |

---

## Session errors

```
SessionError
├── SessionConfigurationError    a missing or invalid setting
└── SessionStoreError            the store failed; wraps the driver error
```

A failed **read** yields an empty session unless `session_fail_closed` is set.
A failed **write** always raises. `count_for_subject` always raises, because
there the store is the authorization answer.

---

## `SessionEvents`

```python
events = SessionEvents(redis, key_prefix="redis:fastapi", db=0)

@events.on_session_end
async def _(session_id: str, cause: Cause) -> None: ...
```

Started and stopped by the lifespan when `session_events_enabled` is set.
`events.tier` is `"field"` when the server can deliver events and `"none"`
otherwise — in which case handlers never fire. Requires Redis 8.8 and
`notify-keyspace-events` including a subkey flag plus `h`.
