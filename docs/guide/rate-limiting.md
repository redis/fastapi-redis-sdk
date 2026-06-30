# Rate Limiting

fastapi-redis-sdk provides Redis-backed **fixed-window rate limiting** as a
FastAPI dependency.  It uses [`INCREX`](https://redis.io/docs/latest/commands/increx/)
when the Redis server supports it (Redis OSS 8.8+) and falls back to an
atomic Lua script otherwise — all transparently.

| Feature | Support |
|---------|---------|
| Fixed-window counter | ✅ |
| `INCREX` (Redis 8.8+) | ✅ |
| Lua fallback (Redis 7.4+) | ✅ |
| One-time capability detection | ✅ |
| Custom key builder (sync/async) | ✅ |
| Custom key prefix | ✅ |
| Async and sync endpoints | ✅ |
| OpenTelemetry spans & metrics | ✅ |
| Rate-limit response headers (`Retry-After`) | ✅ |

---

## 1. Basic usage

```python
from fastapi import Depends, FastAPI
from redis_fastapi import FastAPIRedis, rate_limit

app = FastAPI()
FastAPIRedis(app).lifespan()

@app.get("/items", dependencies=[Depends(rate_limit(limit=100, window=60))])
async def get_items():
    return {"ok": True}
```

This allows **up to 100 requests per 60-second window** per client IP + method + path.

### Parameters

| Argument      | Type     | Default | Description |
|---------------|----------|---------|-------------|
| `limit`       | `int`    | (required) | Maximum requests in the window. Must be ≥ 1. |
| `window`      | `int`    | (required) | Window length in seconds. Must be ≥ 1. |
| `key_builder` | `Callable` | `default_rate_limit_key_builder` | Sync or async function that returns a rate-limit key string. Receives the `Request` object. |
| `prefix`      | `str` | `"redis:fastapi:ratelimit"` | Redis key prefix. |

---

## 2. How it works

### Fixed-window counter

- The first request creates a Redis counter with a TTL.
- Each request increments the counter.
- When the counter reaches `limit`, further requests are rejected with
  `429 Too Many Requests`.
- After the TTL expires, the counter resets and requests are allowed again.

### Redis command path

When the connected server supports INCREX (Redis OSS 8.8+), the dependency
executes a single atomic command per request:

```redis
INCREX redis:fastapi:ratelimit:<key> BYINT 1 UBOUND <limit> EX <window> ENX
```

The `ENX` flag ensures the TTL is set **only when the key is created**,
preserving fixed-window semantics.

### Lua fallback

When INCREX is unavailable, the library falls back to an equivalent Lua script
that performs the same atomic increment, bound check, and TTL management.

### One-time detection

INCREX capability is detected on the first rate-limit request and cached in the
application state.  Subsequent requests use the known path directly — no
per-request fallback overhead.

---

## 3. Rate-limit key

### Default key

The default key uses the client IP, HTTP method, and URL path:

```
redis:fastapi:ratelimit:127.0.0.1:GET:api:v1:items
```

The default key builder is exported as
`redis_fastapi.rate_limit.default_rate_limit_key_builder`.

### Custom key builder

Pass a `key_builder` function to scope limits differently — for example, by
authenticated user ID:

```python
from fastapi import Request

def user_key_builder(request: Request) -> str:
    return request.headers.get("X-User-Id", "anonymous")

@app.get("/profile", dependencies=[Depends(rate_limit(limit=30, window=60, key_builder=user_key_builder))])
async def get_profile():
    return {"user": "data"}
```

Async key builders are supported — if the function is a coroutine, it is
awaited automatically.

### Custom prefix

Override the Redis key prefix if the default conflicts with other keys:

```python
Depends(rate_limit(limit=100, window=60, prefix="myapp:ratelimit"))
```

---

## 4. Behaviour on block

When a request exceeds the limit, the dependency raises `HTTPException` with:

- **Status code:** `429 Too Many Requests`
- **Detail:** `"Too Many Requests"`
- **Header:** `Retry-After: <seconds>`

The endpoint body is never executed for blocked requests.

---

## 5. Error handling

If Redis is unreachable or returns an error, the rate limiter **fails open**:
the request is allowed, the error is logged, and telemetry records
`result="error"`.

This matches the project's caching error policy — a Redis outage should not
take down the application.

---

## 6. OpenTelemetry

When OTel is enabled (via `FastAPIRedis(app).otel()` or
`redis_fastapi.enable_telemetry()`), rate-limit operations emit:

### Span

| Name | Attributes |
|------|------------|
| `rate_limit.check` | `rate_limit.key`, `rate_limit.limit`, `rate_limit.window`, `rate_limit.allowed`, `rate_limit.remaining`, `rate_limit.backend` (increx or lua) |

### Metric

| Name | Type | Labels |
|------|------|--------|
| `redis_fastapi.rate_limit.requests` | Counter | `result` (allowed, blocked, error) |

---

## 7. Sync endpoints

The rate-limit dependency is an `async def` dependency, so FastAPI handles it
correctly before both async and sync endpoints:

```python
@app.get("/sync", dependencies=[Depends(rate_limit(limit=5, window=60))])
def sync_endpoint():
    return {"ok": True}
```

---

## 8. Testing

Use `dependency_overrides` to swap the real Redis client for a fake:

```python
import fakeredis.aioredis
from fastapi.testclient import TestClient
from redis_fastapi import FastAPIRedis, rate_limit, get_async_redis

app = FastAPI()
FastAPIRedis(app).lifespan()

@app.get("/limited", dependencies=[Depends(rate_limit(limit=3, window=60))])
async def limited():
    return {"ok": True}

fake = fakeredis.aioredis.FakeRedis()
app.dependency_overrides[get_async_redis] = lambda: fake

with TestClient(app) as client:
    r1 = client.get("/limited")
    assert r1.status_code == 200
    r2 = client.get("/limited")
    assert r2.status_code == 200
    r3 = client.get("/limited")
    assert r3.status_code == 200
    r4 = client.get("/limited")
    assert r4.status_code == 429
```

---

## 9. Reference

```python
redis_fastapi.rate_limit(
    limit: int,
    window: int,
    *,
    key_builder: Callable[[Request], str | Awaitable[str]] | None = None,
    prefix: str | None = None,
) -> Depends
```

### Result object

`RateLimitResult` (dataclass) is the internal result type, also exported from
`redis_fastapi`:

| Field        | Type      | Description |
|--------------|-----------|-------------|
| `allowed`    | `bool`    | Whether the request is allowed |
| `current`    | `int`     | Current counter value after the request |
| `remaining`  | `int`     | Requests remaining in the window |
| `retry_after` | `int`    | Seconds until the window resets |
| `backend`    | `str`     | `"increx"` or `"lua"` |

### Default key builder

```python
redis_fastapi.rate_limit.default_rate_limit_key_builder(
    request: Request,
) -> str
```

Returns `"{client_ip}:{method}:{path}"`.
