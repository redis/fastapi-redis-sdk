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

`Annotated[AsyncRedis | AsyncRedisCluster, Depends(get_async_redis)]` - returns a cached async Redis client backed by the shared connection pool. Returns `AsyncRedisCluster` when `settings.cluster` is `True`.  When `settings.sentinel` is `True`, returns `AsyncRedis` connected to the primary that the Sentinels report.

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

On a **cache hit** the endpoint is skipped (response served from Redis). On a **miss** the response is captured and stored. Adds `X-Redis-Cache` (`HIT`/`MISS`/`BYPASS`), `Cache-Control` and `ETag` headers, with `304 Not Modified` for `If-None-Match` and `If-Modified-Since`.

An entry stores the response's own header fields and replays them on a hit, so `Link`, `Content-Disposition`, `Content-Language` and your own headers survive. Only text representations are stored, and `Date` and `Set-Cookie` are deliberately withheld — see [Cache scope](../guide/architecture.md#cache-scope) and [Headers a hit carries](../guide/architecture.md#headers-a-hit-carries).

| Parameter         | Type                 | Default                            | Description                                                                                                                                                                                       |
|-------------------|----------------------|------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `ttl`             | `int \| None`        | `settings.default_ttl`             | Cache TTL in seconds (`0` = no expiry)                                                                                                                                                            |
| `eviction_group`  | `str`                | `""`                               | Namespace segment in the cache key                                                                                                                                                                |
| `cache_prefix`    | `str \| None`        | `settings.pattern_prefix("cache")` | Key prefix override                                                                                                                                                                               |
| `key_builder`     | `KeyBuilder \| None` | `default_key_builder`              | Custom key builder                                                                                                                                                                                |
| `private`         | `bool`               | `False`                            | Emit `Cache-Control: private`. Added automatically when `valid_session()` gated the request                                                                                                       |
| `vary_on_session` | `bool \| None`       | `None`                             | `True`: one entry per user, `private` and `Vary: Cookie` on the miss and the hit. `False`: one shared entry, no `Vary: Cookie`. `None`: a response that read the session is served but not stored |
| `no_store`        | `bool`               | `False`                            | Emit `Cache-Control: no-store` on the miss and every hit; the entry is still kept in Redis. Added automatically under `valid_session(issued_within=...)`                                          |

### `cache_evict()`

```python
from redis_fastapi import cache_evict, default_key_builder

@app.delete("/items/{item_id}", dependencies=[Depends(cache_evict(eviction_group="items", key_builder=default_key_builder))])
async def delete_item(item_id: int):
    ...
```

Eviction runs **after** the endpoint succeeds. With a `key_builder`, deletes the matching key. Without one, clears the **entire eviction group**.

| Parameter        | Type                 | Default                            | Description                                |
|------------------|----------------------|------------------------------------|--------------------------------------------|
| `eviction_group` | `str`                | `""`                               | Namespace to evict from                    |
| `key_builder`    | `KeyBuilder \| None` | `None`                             | Specific key; omit to clear eviction group |
| `prefix`         | `str \| None`        | `settings.pattern_prefix("cache")` | Key prefix override                        |

### `cache_put()`

```python
from redis_fastapi import cache_put, default_key_builder

@app.put("/items/{item_id}", dependencies=[Depends(cache_put(eviction_group="items", key_builder=default_key_builder, ttl=300))])
async def update_item(item_id: int, body: Item):
    ...
```

Write-through: endpoint always executes, response is stored so the next `cache()` read is a HIT.

| Parameter        | Type                 | Default                            | Description                   |
|------------------|----------------------|------------------------------------|-------------------------------|
| `ttl`            | `int \| None`        | `settings.default_ttl`             | Cache TTL in seconds          |
| `eviction_group` | `str`                | `""`                               | Namespace to write into       |
| `key_builder`    | `KeyBuilder \| None` | `default_key_builder`              | Custom key builder            |
| `prefix`         | `str \| None`        | `settings.pattern_prefix("cache")` | Key prefix override           |
| `private`        | `bool`               | `False`                            | Emit `Cache-Control: private` |

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

`Annotated[Session, Depends(get_session)]` - the session data as a `dict`
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
`absolute_remaining`, `fields`, and the `revoked` / `rotated` flags. Operations
that act on the session rather than its contents take this rather than the
data.

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

## `valid_session()`

```python
from fastapi import Depends
from redis_fastapi import valid_session

@app.post("/checkout", dependencies=[Depends(valid_session())])
async def checkout(): ...

@app.post("/account/email", dependencies=[
    Depends(current_user),
    Depends(valid_session(issued_within=600, on_reject=my_rejection)),
])
async def change_email(): ...
```

A dependency that rejects a request without a valid session: the cookie must
name a record that was live in Redis when the request arrived, and nothing
earlier in the request may have ended it. It does not ask who the session
belongs to.

| Parameter       | Type                                                                               | Default | Description                                                                                                                                                                    |
|-----------------|------------------------------------------------------------------------------------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `issued_within` | `int \| timedelta \| None`                                                         | `None`  | Also require the session ID to have been issued at most this long ago. Adds the reason `"stale"` and makes the response `Cache-Control: no-store`. Not an authentication check |
| `on_reject`     | `OnReject[SessionRejection]`, or `OnReject[RecencyRejection]` with `issued_within` | `None`  | Receives the request and the reason, returns the response (sync or async). Default: `401`                                                                                      |

| Reason          | Meaning                                                                                        |
|-----------------|------------------------------------------------------------------------------------------------|
| `"missing"`     | No usable session: no cookie, a malformed one, or one an earlier dependency revoked or emptied |
| `"expired"`     | A well-formed cookie naming no live session                                                    |
| `"unavailable"` | The read failed and the request continued without a session                                    |
| `"stale"`       | Only with `issued_within`: the session ID was issued longer ago than that                      |

The default rejection is `HTTPException(401, "No valid session")`, with a
`WWW-Authenticate` header only when the `challenge` setting is configured. For
`"stale"` it is a `401` with the body
`{"detail": ..., "error": "stale_session", "issued_within": N}`.

The gate marks the session as read. When it passes, a following
`cache(vary_on_session=False)` adds `private`. Listed **after** such a
`cache()`, it raises `SessionConfigurationError` on the first request. It also
raises that error on a route excluded by `skip`, and raises `TypeError` when
`on_reject` returns something other than a `Response`.

| Type               | Values                                                                                                    |
|--------------------|-----------------------------------------------------------------------------------------------------------|
| `SessionRejection` | `Literal["missing", "expired", "unavailable"]`                                                            |
| `RecencyRejection` | `SessionRejection` plus `"stale"`                                                                         |
| `OnReject[R]`      | `Callable[[Request, R], Response \| Awaitable[Response]]`                                                 |
| `Challenge`        | `str \| SecurityBase \| Callable[[Request, str], str \| None]`                                            |
| `SessionRejected`  | The exception that carries a rejection response out of the dependency. Control flow, not a `SessionError` |

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
    challenge=None,
    store=None, store_factory=None,
    encryptor=None, id_factory=None, key_prefix=None,
    idle_ttl=None, absolute_ttl=None, gc_ttl=None,
    searchable={"device_id": "tag", "cart.total": "numeric"},
)
```

| Argument                                                                      | Purpose                                                                                                                                                                                                                                                                               |
|-------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `principal_of` / `principal_keys`                                             | What a change to triggers a rotation. Must be pure and return a verified identity.                                                                                                                                                                                                    |
| `subject_of`                                                                  | The subject of a session, stored in the field `s:sub`. The searches by subject use it. `None` makes the session anonymous: no search by subject finds it.                                                                                                                             |
| `descriptor_of`                                                               | What a device listing shows. Stored in the session hash as the field `s:desc`, in plaintext, and never part of `request.session`. The list methods return it.                                                                                                                         |
| `cookie_builder`                                                              | Renders `Set-Cookie`, for setting **and** clearing.                                                                                                                                                                                                                                   |
| `skip`                                                                        | Requests needing no session, at zero Redis cost.                                                                                                                                                                                                                                      |
| `challenge`                                                                   | The `WWW-Authenticate` header on a `valid_session()` rejection: a FastAPI security scheme, a string, or a callable of the request and the reason. Omitted by default; `Basic` is refused.                                                                                             |
| `store` / `store_factory`                                                     | Supply a whole store. Mutually exclusive.                                                                                                                                                                                                                                             |
| `encryptor`, `id_factory`, `key_prefix`, `idle_ttl`, `absolute_ttl`, `gc_ttl` | Passed to the store constructor. TTLs accept `int` seconds or `timedelta`.                                                                                                                                                                                                            |
| `searchable`                                                                  | The attributes `find()` and `count()` can search: `{path: type}`, with the type one of `"tag"`, `"numeric"`, `"text"` or `"geo"`. A path names a leaf, such as `"cart.total"`. Needs the Query Engine. Cannot be combined with `encryptor`: setup raises `SessionConfigurationError`. |
| `coder`                                                                       | Accepted, and not used for sessions. Each value is stored as JSON text.                                                                                                                                                                                                               |

---

## `SessionStore`

Abstract base owning the lifecycle. `RedisSessionStore` is the shipped
implementation; `SessionStoreProtocol` is the structural type for substituting
one without inheritance.

A session is one Redis hash: system fields `s:created`, `s:seen`, `s:sub` and
`s:desc`, and one `a:<path>` field for each leaf of the session. See
[Fields in the hash](../guide/sessions.md#fields-in-the-hash).

### Lifecycle

| Method                                                                                 | Purpose                                                                                                                                                                       |
|----------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `load(session_id, *, refresh=True)`                                                    | Read a session and restart its idle clock. `None` for every way it can be absent. A hash without `s:created` is not a session.                                                |
| `load_with_status(session_id, *, refresh=True)`                                        | `load()`, plus whether the read itself failed.                                                                                                                                |
| `session_age(state)`                                                                   | Seconds since the session ID was issued, from the time left on the absolute clock. `None` without a stored session, or for one created under a longer `session_absolute_ttl`. |
| `create(session_id, record, *, subject=None, descriptor=None, ttls=None)`              | Write a session that does not exist yet. The only method that writes `s:created`, and so the absolute deadline. Returns the stored fields.                                    |
| `save(session_id, record, *, baseline=None, subject=None, descriptor=None, ttls=None)` | Write the fields that differ from `baseline`, the fields the last load or write returned. Never writes `s:created`. Returns the stored fields.                                |
| `touch(session_id)`                                                                    | Restart the idle clock alone.                                                                                                                                                 |
| `delete(session_id)`                                                                   | Remove the session.                                                                                                                                                           |
| `rotate(state, *, subject=..., descriptor=None, ttls=None)`                            | New identifier, old key deleted first. Starts a new absolute clock and keeps the time left on each attribute TTL. Omit `subject` to keep the current one. Returns the new ID. |
| `reauthenticate(state)`                                                                | Ask the middleware to rotate on the way out.                                                                                                                                  |
| `revoke(state, *, subject=None)`                                                       | End this session. `subject` is accepted and not used.                                                                                                                         |
| `revoke_id(session_id, *, subject)`                                                    | End one session, refusing an ID whose session has another subject. Runs no search.                                                                                            |
| `new_id()` / `is_valid_id(value)`                                                      | Generate and validate identifiers.                                                                                                                                            |
| `new_record(data, *, created=None)`                                                    | Build a `SessionRecord`, carrying `created` forward.                                                                                                                          |
| `session_id(state)`                                                                    | The current identifier, or `None`.                                                                                                                                            |

`ttls` maps the name of a top-level attribute to a TTL in seconds, or to
`None` to remove the TTL. The middleware fills it from `Session.set(...,
ttl=)` and `Session.persist()`.

### Search

Each method in this table needs the Redis Query Engine and raises
`SessionSearchUnavailableError` without it. Each raises
`SessionSearchTimeoutError` when the server reports a timeout. None fails
open.

| Method                                         | Purpose                                                                                             |
|------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| `list_for_subject(subject)`                    | The sessions of a subject, newest access first. At most 1,000.                                      |
| `count_for_subject(subject, *, limit=None)`    | How many sessions a subject has. The count is exact; `limit` is accepted and not used.              |
| `revoke_all(subject)`                          | End every session of a subject. Returns keys removed.                                               |
| `list_active(within, *, limit=100, offset=0)`  | One page of the sessions used in the last `within` seconds, newest access first.                    |
| `revoke_idle(older_than)`                      | End every session not used for `older_than` seconds. Returns keys removed.                          |
| `find(name, *, <filter>, limit=100, offset=0)` | One page of the sessions whose searchable attribute `name` matches the filter, newest access first. |
| `count(name, *, <filter>)`                     | How many sessions `find()` would match.                                                             |

`<filter>` is exactly one of the four keyword arguments below, and it must fit
the declared type of `name`:

| Declared type | Filter                    | Value                                     |
|---------------|---------------------------|-------------------------------------------|
| `tag`         | `equals=`                 | `str` or `bool`                           |
| `numeric`     | `between=(low, high)`     | Finite numbers; either may be `None`      |
| `text`        | `text=`                   | `str`, Redis query syntax, sent unchanged |
| `geo`         | `near=(lon, lat, meters)` | Finite numbers                            |

`limit` must be at least 1, `offset` at least 0, and `offset + limit` at most
10,000. A call that breaks one of these rules raises `ValueError`, and so does
a name that is not in `searchable` and text that Redis rejects for its syntax.

`RedisSessionStore.ensure_indexes()` creates the indexes: the built-in one,
`<prefix>:si`, and, with `searchable`, the custom one, `<prefix>:sa:<hash>`.
The lifespan calls it at startup, and a search calls it when an index is
missing.

---

## Session data types

| Type              | Contents                                                                                                          |
|-------------------|-------------------------------------------------------------------------------------------------------------------|
| `Session`         | `dict` subclass with `accessed` / `modified` flags, `raw()`, `set(name, value, *, ttl=None)` and `persist(name)`. |
| `SessionState`    | `data`, `session_id`, `subject`, `created`, `absolute_remaining`, `fields`, `revoked`, `rotated`.                 |
| `SessionRecord`   | `data` plus `SessionMetadata`.                                                                                    |
| `SessionMetadata` | `created`, `last_access`.                                                                                         |
| `SessionInfo`     | One row of a list or a search: `session_id`, `created`, `last_access`, `descriptor`.                              |
| `LoadedSession`   | `record`, `absolute_remaining` and `fields`, returned by `load()`.                                                |
| `CookieSpec`      | Cookie attributes; `cleared()` returns the deletion form.                                                         |
| `Outcome`         | What a response owes the session. Eight members.                                                                  |
| `Deadline`        | `ABSENT` / `UNBOUNDED` — a deadline that is not a number.                                                         |

### `Session.set()` / `Session.persist()`

```python
session.set("flash", "Settings saved.", ttl=60)   # expires 60 seconds after the save
session.persist("flash")                          # remove the TTL, keep the value
```

| Method                          | Purpose                                                                                                                                              |
|---------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| `set(name, value, *, ttl=None)` | Write a top-level attribute. `ttl` is a positive `int` of seconds; any other value raises `ValueError`. Without `ttl` it is `session[name] = value`. |
| `persist(name)`                 | Remove the TTL of a top-level attribute and keep its value. Does nothing for an attribute that is absent or has no TTL.                              |

The TTL starts at the save and does not slide. A write of a different value
without `ttl` removes it; a write of an equal value keeps it. A TTL on a
searchable attribute raises `SessionConfigurationError` at the save. See
[Attribute TTL](../guide/sessions.md#attribute-ttl).

---

## Session errors

```
SessionError
├── SessionConfigurationError            a missing or invalid setting
├── SessionAttributeTypeError            a searchable attribute holds a value of another type; also a TypeError
└── SessionStoreError                    the store failed; wraps the driver error
    └── SessionSearchError               a search could not give a complete answer
        ├── SessionSearchUnavailableError    the server has no Query Engine
        └── SessionSearchTimeoutError        the server reported a timeout
```

`SessionAttributeTypeError` is raised at the save, before anything is
written. `SessionConfigurationError` is also raised at the save when a
searchable attribute was given a TTL. A wrong call - an invalid `ttl`, a name
that is not searchable, a filter of the wrong type, a page out of range -
raises `ValueError`, which is not a `SessionError`.

`SessionRejected` is not a `SessionError`: it carries a `valid_session()`
rejection out of the dependency, and the handler that `.sessions()` registers
returns its response.

A failed **read** yields an empty session unless `session_fail_closed` is set.
A failed **write** always raises. A failed search always raises, because
there the store is the answer: a `count_for_subject` that returned `0` would
let every sign-in past a cap on concurrent sessions.
