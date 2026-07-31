# Rate Limiting

fastapi-redis-sdk provides distributed rate limiting backed by Redis, exposed
the same way as caching: a dependency factory, an app-wide middleware, and an
imperative backend.  Because the counters live in Redis, limits hold across
every worker and pod - an in-process limiter would silently multiply the limit
by the number of processes.

| Pattern                       | Best for                                                            |
|-------------------------------|---------------------------------------------------------------------|
| `rate_limit()` dependency     | Per-route limits (most use cases)                                   |
| Global limiter (middleware)   | One app-wide limit applied to every request                         |
| `RateLimitBackend`            | Imperative checks, custom keys/cost, non-HTTP flows                 |

All three share one window-counter implementation.  See
[Architecture](architecture.md) for how connection pools are managed across the
application lifecycle.

---

## Setup

Use the `FastAPIRedis` builder.  A single fluent call registers the 429
exception handler and the header/limiter middleware:

```python
from fastapi import Depends, FastAPI
from redis_fastapi import FastAPIRedis, rate_limit

app = FastAPI()
FastAPIRedis(app).lifespan().rate_limiting()
```

`rate_limiting()` is **required** for the `rate_limit()` dependency to work - it
wires the handler that turns an over-limit check into a `429` and the middleware
that attaches `X-RateLimit-*` headers to allowed responses.  Calling it more
than once is a no-op, and it composes with `.caching()`:

```python
FastAPIRedis(app).lifespan().caching().rate_limiting()
```

---

## 1. Per-route limits - `rate_limit()`

`rate_limit()` returns a `Depends()`-compatible dependency.  Add it to a route's
`dependencies=[...]`; the endpoint signature stays untouched.

```python
@app.get("/api", dependencies=[Depends(rate_limit("100/minute"))])
async def api():
    return {"ok": True}
```

The first 100 requests in each 60-second window return `200` with a decreasing
`X-RateLimit-Remaining`; the 101st returns `429 Too Many Requests` with a
`Retry-After` header.

### Defining the rate

A rate couples a **limit** (max requests) with a **window** (seconds).  Pass it
in whichever form reads best - all funnel through `parse_rate`:

```python
rate_limit("100/minute")             # fluent string DSL
rate_limit("10/15seconds")           # multiples are supported
rate_limit("100 per 2 minutes")      # 'per' spelling, with a multiplier
rate_limit(Rate.per_hour(1000))      # typed constructor
rate_limit(limit=5, window=10)       # explicit limit + window (seconds)
rate_limit((5, 10))                  # (limit, window) tuple
```

The string DSL is `"<count>/<unit>"` or `"<count> per <n> <unit>"`, where unit is
`second`, `minute`, `hour`, or `day` (short forms `s`/`m`/`h`/`d` also work).
A `Rate` round-trips back to its canonical string via `str(rate)`.

### Options

```python
Depends(rate_limit(
    "100/minute",                      # the rate (or limit=/window=)
    scope="search",                    # counter namespace (default: route template)
    identifier=my_identifier,          # key strategy (default: ip_identifier)
    cost=1,                            # units this request consumes (default 1)
    skip_when=lambda r: r.headers.get("X-Internal") == "1",
    on_limit_exceeded=my_429_builder,  # custom 429 response (sync or async)
    emit_headers=True,                 # X-RateLimit-* headers (default: setting)
    ietf_headers=False,                # IETF RateLimit headers (default: setting)
    fail_closed=False,                 # reject if Redis is down (default: setting)
))
```

| Option              | Purpose                                                                                                                                                                                                                                                                                                   |
|---------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `scope`             | Counter namespace **and** the unit of sharing.  Defaults to the matched route template (`/items/{id}`), giving each route its own per-client counter.  An explicit `scope` shares one counter across every route that uses it (see [Sharing one limit across routes](#sharing-one-limit-across-routes)).  |
| `identifier`        | Returns the per-client **identity** segment of the key (not the whole key - scope/prefix are added by the library).  `ip_identifier` (default), or your own callable to key by user / API key / tenant.                                                                                                   |
| `cost`              | How many units one request consumes - e.g. a bulk endpoint may cost more.  Must be at least `1`; anything lower raises `ValueError` at decoration time.                                                                                                                                                   |
| `skip_when`         | Predicate over the `Request`; when truthy the request is **not counted**.                                                                                                                                                                                                                                 |
| `on_limit_exceeded` | Callable `(request, result) -> Response` building the 429 (sync or async).                                                                                                                                                                                                                                |
| `emit_headers`      | Toggle the `X-RateLimit-*` trio for this route.                                                                                                                                                                                                                                                           |
| `ietf_headers`      | Additionally emit the IETF `RateLimit` / `RateLimit-Policy` headers.                                                                                                                                                                                                                                      |
| `fail_closed`       | Override the fail-open/closed behaviour when Redis is unreachable.                                                                                                                                                                                                                                        |

### Conditional skip and custom responses

```python
from fastapi.responses import JSONResponse

@app.get("/api", dependencies=[Depends(rate_limit(
    "100/minute",
    skip_when=lambda r: r.headers.get("X-Internal") == "1",   # internal traffic bypasses
    on_limit_exceeded=lambda r, res: JSONResponse(            # custom 429 body
        {"error": "slow down", "retry_after": res.retry_after}, status_code=429,
    ),
))])
async def api():
    return {"ok": True}
```

`skip_when` and `on_limit_exceeded` may be sync or async.  The default 429 body
is `{"detail": "Too Many Requests"}`.

### Sharing one limit across routes

By default each route counts independently - the counter's scope defaults to the
route template, so `100/minute` on `/search` and `100/minute` on `/autocomplete`
are two separate budgets.  Give the dependency an explicit `scope` to make a
**single** counter span several routes - useful when a group of endpoints should
draw from one budget per client:

```python
search_limit = rate_limit("100/minute", scope="search-api")

@app.get("/search", dependencies=[Depends(search_limit)])
async def search(): ...

@app.get("/autocomplete", dependencies=[Depends(search_limit)])
async def autocomplete(): ...
# A client gets 100/minute across BOTH routes combined, not 100 each.
```

Every route carrying the same `scope` and `identifier` increments the same
counter, because the explicit scope replaces the per-route template in the key.

---

## 2. Identifiers

The identifier decides **who gets counted** - it returns the *client identity*
only.  Route separation is handled by the `scope` (which defaults to the route
template), so identifiers stay path-agnostic.

### What makes a counter unique

Every counter is one Redis key, assembled from three parts:

```
{prefix} : {scope} : {identifier}
```

`scope` and `identifier` are **orthogonal axes** - one answers *which routes
share a counter*, the other *which clients share a counter*:

| Part         | Answers            | Knob                                                               | Default                           |
|--------------|--------------------|--------------------------------------------------------------------|-----------------------------------|
| `prefix`     | which app          | `REDIS_PREFIX` (app-wide)                                          | `redis:fastapi:ratelimit`         |
| `scope`      | which **routes**   | `scope=` on `rate_limit()`                                         | the matched route template        |
| `identifier` | which **clients**  | `identifier=` (and `REDIS_RATE_LIMIT_TRUST_PROXY` for the default) | client IP via `ip_identifier`     |

Two requests hit the **same** counter only when all three match.  Examples for
client `1.2.3.4` on route `/items/{id}` (the resulting counter key is shown as a
comment above each call):

```python
# redis:fastapi:ratelimit:/items/{id}:1.2.3.4
rate_limit("100/min")

# redis:fastapi:ratelimit:search:1.2.3.4
rate_limit("100/min", scope="search")

# redis:fastapi:ratelimit:/items/{id}:user:42
rate_limit("100/min", identifier=user_identifier)

# redis:fastapi:ratelimit:search:key-abc
rate_limit("100/min", scope="search", identifier=api_key_identifier)
```

So: change `scope` to regroup **routes**, change `identifier` to repartition
**clients** - independently.

One identifier ships built in: **`ip_identifier`** (the default), which returns
the client IP.  To count by anything else - an authenticated user, an API key, a
tenant - pass your own callable `(request) -> str` (sync or async) as
`identifier=`.  It returns only the **client identity** - the scope and prefix
are added by the library - so it just needs to return a stable string for the
client; the library deliberately ships no auth-aware helper so it stays
decoupled from your authentication model:

```python
def user_identifier(request):
    user = getattr(request.state, "user", None)
    return f"user:{user.id}" if user else request.client.host

def api_key_identifier(request):
    return request.headers.get("X-API-Key", "anonymous")

@app.get("/me", dependencies=[Depends(rate_limit("100/minute", identifier=user_identifier))])
async def me(): ...
```

The callable type is exported as `Identifier` if you want to annotate your own
(mirroring caching's `KeyBuilder`) - handy, for example, for a factory that
builds one:

```python
from redis_fastapi import Identifier

def header_identifier(header: str) -> Identifier:
    def identify(request) -> str:
        return request.headers.get(header, "anonymous")
    return identify

tenant_id = header_identifier("X-Tenant-ID")   # an Identifier
```

!!! note "Partition the counter, don't vary the limit"
    A custom identifier only *partitions* the counter - every client it
    distinguishes still gets the **same** limit, counted separately.  Selecting a
    **different** limit by user, group, or API-key tier (a quota rules engine) is
    intentionally out of scope.

### Behind a proxy

By default the client IP is `request.client.host`.  When the app runs behind a
trusted proxy or load balancer, enable `REDIS_RATE_LIMIT_TRUST_PROXY=true` so the
first hop of `X-Forwarded-For` is used instead.

!!! warning
    Only trust `X-Forwarded-For` when a proxy you control sets it - clients can
    otherwise spoof the header to evade limits.

---

## 3. Response headers

When `emit_headers` is on (the default), every checked response carries the
legacy `X-RateLimit-*` trio:

| Header                  | Meaning                                              |
|-------------------------|------------------------------------------------------|
| `X-RateLimit-Limit`     | The configured limit for the window                  |
| `X-RateLimit-Remaining` | Requests left in the current window (≥ 0)            |
| `X-RateLimit-Reset`     | Unix epoch second at which the window resets         |
| `Retry-After`           | Seconds to wait before retrying (on `429` responses) |

Set `ietf_headers=True` (per route or via `REDIS_RATE_LIMIT_IETF_HEADERS`) to
emit the [IETF draft](https://datatracker.ietf.org/doc/html/draft-ietf-httpapi-ratelimit-headers)
fields:

```
RateLimit-Policy: "default";q=100;w=60
RateLimit: "default";r=42;t=37
```

The two families are **independent switches**: `emit_headers` controls the
`X-RateLimit-*` trio and `ietf_headers` the IETF fields. Enable either, both,
or neither — e.g. `emit_headers=False, ietf_headers=True` emits **only** the
standards-track headers. The default (X-only) keeps the widely-deployed
convention while the IETF set stays opt-in. (`Retry-After` is always set on a
`429`, independent of both.)

---

## 4. Global (app-wide) limiter

Apply one limit to **every** request without per-route dependencies by passing
`global_rate` to the builder:

```python
from redis_fastapi import FastAPIRedis

app = FastAPI()
FastAPIRedis(app).lifespan().rate_limiting(
    global_rate="1000/minute",   # applies to every route
    identifier=my_identifier,    # optional; ip_identifier is the default
)
```

The global limiter is inherently **shared** - it never appends the route path,
so the limit is one budget per client across the whole app (not per endpoint).
It accepts the same `identifier`, `scope`, `skip_when`, `on_limit_exceeded`,
`ietf_headers`, and `fail_closed` options as the per-route dependency.  It can be enabled by
configuration alone - set `REDIS_RATE_LIMIT_DEFAULT_LIMIT` to a positive value
(with `REDIS_RATE_LIMIT_DEFAULT_WINDOW`) and the middleware is wired
automatically:

```bash
export REDIS_RATE_LIMIT_DEFAULT_LIMIT=1000
export REDIS_RATE_LIMIT_DEFAULT_WINDOW=60
```

Per-route `rate_limit()` dependencies stack on top of the global limit - a
request must pass both.

Both checks run, but only one set of `X-RateLimit-*` headers can go out, so the
response reports **the limit that binds first**: a rejection if either check
rejected, otherwise whichever has fewer requests remaining.  A route allowing
100/minute behind a global 1000/minute therefore advertises the global counter
once the client has spent most of it, rather than a roomy per-route number that
would leave the next 429 unexplained.

---

## 5. `RateLimitBackend` - imperative API

For checks that don't map to a single route - custom keys, dynamic cost, or
non-HTTP code paths - inject `RateLimitBackendDep`.  It needs only a Redis
connection; `.rate_limiting()` is not required for the backend itself.

```python
from redis_fastapi import RateLimitBackendDep

@app.post("/send-email")
async def send_email(to: str, limiter: RateLimitBackendDep):
    result = await limiter.hit(f"email:{to}", limit=3, window=3600)
    if not result.allowed:
        raise HTTPException(429, f"retry in {result.retry_after}s")
    await deliver(to)
    return {"sent": True}
```

| Method                                                                      | Description                                                         |
|-----------------------------------------------------------------------------|---------------------------------------------------------------------|
| `hit(identifier, *, limit, window, scope=None, cost=1, fail_closed=None)`   | Consume `cost` units; returns a `RateLimitResult`.                  |
| `peek(identifier, *, limit, window, scope=None)`                            | Report current state **without** consuming.                         |
| `reset(identifier, *, scope=None)`                                          | Clear the counter.  Returns `True` if it existed.                   |

Every call returns a `RateLimitResult`:

```python
RateLimitResult(allowed, limit, remaining, reset_after, reset_at, retry_after,
                backend, degraded)
```

`backend` names the execution tier that served the check (`"increx"` or
`"lua"`); it is empty for non-consuming `peek` reads and the Redis-unreachable
path.  `degraded` is `True` when Redis was unreachable and the
result is a fail-open/fail-closed fallback rather than a real counter check -
these checks are also counted under the `error` result in the
[metrics](observability.md).

A `SyncRateLimitBackend` facade (`SyncRateLimitBackendDep`) offers the same
methods for sync endpoints, delegating to the async backend on a worker thread.

### When to reach for the backend

The `rate_limit()` dependency keys on the HTTP client, runs **before** the
handler, never reads the body, and always counts with a fixed cost.  Reach for
the backend when one of those needs to change - the key, the cost, or the
*decision to count* is only known **inside** the handler - or when there's no
`Request` at all.  The recipes below cover the common cases.

#### Recipe: limit a downstream resource, not the request

The thing you're protecting is an expensive side effect - an email/SMS gateway,
a paid vendor API - and the budget belongs to a **business entity**, not the
caller's IP:

```python
@app.post("/notify")
async def notify(to: str, limiter: RateLimitBackendDep):
    # 3 emails per recipient per hour, enforced across every worker
    if not (await limiter.hit(f"email:{to}", limit=3, window=3600)).allowed:
        raise HTTPException(429, "too many emails to this recipient")
    await send_email(to)
    return {"sent": True}
```

#### Recipe: per-tenant fairness (key from the body)

In a multi-tenant API, one noisy tenant must not starve the others - and behind
a shared gateway they may all share an IP.  Key on a `tenant_id` taken from the
body (or a header), which the dependency can't see:

```python
@app.post("/reports")
async def build_report(body: ReportRequest, limiter: RateLimitBackendDep):
    if not (await limiter.hit(f"tenant:{body.tenant_id}", limit=20, window=60)).allowed:
        raise HTTPException(429, "tenant report quota exceeded")
    return await generate(body)
```

#### Recipe: brute-force login lockout

Count **only failed** logins, key on the submitted username, and clear the
counter on success.  The dependency can't do this - it increments before the
handler knows whether auth succeeded.  This combines all three methods:

```python
@app.post("/login")
async def login(body: LoginBody, limiter: RateLimitBackendDep):
    key = f"login:{body.username}"
    if (await limiter.peek(key, limit=5, window=900)).remaining == 0:
        raise HTTPException(429, "account temporarily locked")

    if not await verify(body.username, body.password):
        result = await limiter.hit(key, limit=5, window=900)   # count the failure
        raise HTTPException(401, f"invalid credentials ({result.remaining} left)")

    await limiter.reset(key)                                   # clear on success
    return {"token": ...}
```

#### Recipe: runtime-computed cost (token budget)

`cost` on the dependency is fixed at decoration time.  When each request
consumes a variable amount - LLM tokens, batch size, query complexity - compute
it in the handler and pass it to `hit`:

```python
@app.post("/complete")
async def complete(body: CompletionBody, limiter: RateLimitBackendDep):
    cost = max(1, estimate_tokens(body.prompt))                # known only now
    result = await limiter.hit(
        f"tokens:{body.api_key}", limit=100_000, window=86_400, cost=cost,
    )
    if not result.allowed:
        raise HTTPException(429, f"daily budget exhausted; retry in {result.retry_after}s")
    return {"charged": cost, "remaining": result.remaining}
```

`cost` must be at least `1`, hence the `max(1, ...)`: an estimator that returns
`0` for an empty prompt would otherwise raise `ValueError`.  Zero is rejected
rather than treated as free because the counter reports no increment, which the
allow/deny rule reads as *rejected* - a request charged nothing would be turned
away.  Negative values are rejected for the sharper reason that they decrement
the counter, letting a caller refund requests it already spent.

#### Recipe: multiple limits in one handler (burst + sustained)

Enforce more than one budget for the same key - e.g. a short burst ceiling and a
longer sustained one - using a distinct `scope` per limit:

```python
@app.post("/messages")
async def send_message(user_id: str, limiter: RateLimitBackendDep):
    for limit, window, scope in ((5, 1, "burst"), (1000, 3600, "sustained")):
        result = await limiter.hit(f"user:{user_id}", limit=limit, window=window, scope=scope)
        if not result.allowed:
            raise HTTPException(429, f"{scope} limit exceeded")
    return {"sent": True}
```

#### Recipe: rate limiting outside HTTP

There's no `Request` in a background worker, a queue consumer, or a WebSocket
loop - but you still want **distributed** throttling across the whole fleet.
Construct a `RateLimitBackend` from any Redis client:

```python
from redis_fastapi import RateLimitBackend

async def worker(job, redis):
    limiter = RateLimitBackend(redis)            # no Request, just a Redis client
    # Cap calls to a flaky vendor API to 60/min across all workers.
    if not (await limiter.hit("vendor:acme", limit=60, window=60)).allowed:
        await job.requeue(delay=5)
        return
    await call_vendor(job)
```

#### Recipe: show remaining quota, and reset on demand

`peek` reports the current state **without** consuming a slot - ideal for a
"requests left" widget or a pre-flight check.  `reset` clears a counter, e.g. an
admin unlocking an account or a quota reset after an upgrade:

```python
@app.get("/quota/{user_id}")
async def quota(user_id: str, limiter: RateLimitBackendDep):
    state = await limiter.peek(f"user:{user_id}", limit=1000, window=3600)
    return {"remaining": state.remaining, "resets_in": state.reset_after}

@app.post("/admin/unlock/{username}")
async def unlock(username: str, limiter: RateLimitBackendDep):
    await limiter.reset(f"login:{username}")
    return {"unlocked": username}
```

Several recipes key on a **request-body value** (`username`, `api_key`,
`tenant_id`).  The limiter still never reads the body itself - *your* handler
does, then hands the key to `hit` / `peek` - so the body-agnostic design of the
dependency is preserved.

### Scopes and keys

Keys follow a **flat** scheme - `{prefix}:{scope}:{identifier}` - where `scope`
defaults to the matched route **template**:

- `redis:fastapi:ratelimit:/items/{id}:1.2.3.4` - default (per-route) scope.
  All values of the path parameter share this bucket, so `/items/1` and
  `/items/2` cannot each get a fresh budget.
- `redis:fastapi:ratelimit:search-api:1.2.3.4` - an explicit `scope="search-api"`
  shared across every route that uses it.

The `scope` is both the namespace and the unit of sharing: two routes share a
counter exactly when their `scope` **and** `identifier` line up.  Leaving the
scope empty means "use my route template", which keeps routes independent while
letting all values of a path parameter share one counter (so `/items/1` and
`/items/2` cannot each claim a fresh budget).

!!! info "No Redis Cluster hash-tag - on purpose"
    Unlike the [caching](caching.md#cache-keys) keys, rate-limit keys are **not**
    wrapped in `{…}` hash-tag braces.  Caching needs the hash-tag so a whole
    eviction group lands on one slot for bulk `delete_group()` eviction.  Rate
    limiting has **no bulk operation** - every `hit` / `peek` / `reset` touches a
    single key - so co-locating a scope would buy nothing and actively hurt:
    pinning every counter in a busy scope (e.g. a global limit) to one node
    creates a **hot shard** and defeats the cluster's horizontal scaling.  The
    flat key lets Redis spread per-client counters across all slots by CRC16 of
    the full key.

---

## 6. Algorithm and Redis support

The limiter uses the Redis
[window-counter pattern](https://redis.io/docs/latest/commands/increx/#pattern-window-counter-rate-limiter):
increment a counter, set the TTL only on the first request of the window, and
reject once the count would exceed the limit.

The increment is **atomic** - there is no read-modify-write race, so concurrent
requests can never overshoot the limit.  The backend picks the best mechanism
available and remembers the result per process:

1. **`INCREX`** - a single atomic command (Redis **8.8+**).
2. **Lua fallback** - an equivalent atomic `EVAL` script (Redis **2.6+**).

Both tiers are atomic; there is deliberately no non-atomic degrade. A server
that supports neither (scripting disabled) is treated as a backend error and
takes the [fail-open/closed](#7-error-handling) path rather than counting with a
race. No configuration or new dependency is needed; pre-8.8 servers
transparently use the Lua path.

Which tier applies is settled by asking the server (`COMMAND INFO INCREX`) once
per pool, during startup, and the answer is logged at `INFO`.  Detection is a
capability lookup rather than "send `INCREX` and interpret the error" because a
cluster client resolves a command to a hash slot from the server's own command
table *before* sending it - so on a pre-8.8 cluster `INCREX` is refused
client-side, with wording no server ever produces.  Guessing from error text
there would misread an old cluster as an outage and degrade every request.
An app that starts while Redis is unreachable simply defers the question to its
first request; nothing is cached until the server actually answers.

---

## 7. Error handling

If Redis is **unreachable**, the limiter **fails open** by default - the request
is allowed and a warning is logged, so a Redis outage cannot take down the API.
Set `REDIS_RATE_LIMIT_FAIL_CLOSED=true` (or `fail_closed=True` per call) to
reject instead when correctness matters more than availability.

Either way the outage stays visible: the check's `RateLimitResult.degraded` is
`True`, and it is counted under the `error` result of the
`redis_fastapi.ratelimit.requests` metric (see [Observability](observability.md))
rather than as a normal `allowed` / `limited` outcome - so a fail-open spike does
not silently masquerade as healthy traffic.

---

## 8. Configuration

All settings live on `RedisSettings` under the `REDIS_` env prefix (same
mechanism as the caching options - see [Configuration](configuration.md)):

| Setting                            | Env var                            | Default | Purpose                                              |
|------------------------------------|------------------------------------|---------|------------------------------------------------------|
| `rate_limit_default_limit`         | `REDIS_RATE_LIMIT_DEFAULT_LIMIT`   | `0`     | Global limit; `0` disables the app-wide limiter.     |
| `rate_limit_default_window`        | `REDIS_RATE_LIMIT_DEFAULT_WINDOW`  | `60`    | Global window, in seconds.                           |
| `rate_limit_emit_headers`          | `REDIS_RATE_LIMIT_EMIT_HEADERS`    | `true`  | Emit the `X-RateLimit-*` trio.                       |
| `rate_limit_ietf_headers`          | `REDIS_RATE_LIMIT_IETF_HEADERS`    | `false` | Also emit IETF `RateLimit` / `RateLimit-Policy`.     |
| `rate_limit_trust_proxy`           | `REDIS_RATE_LIMIT_TRUST_PROXY`     | `false` | Honour `X-Forwarded-For` for the client IP.          |
| `rate_limit_fail_closed`           | `REDIS_RATE_LIMIT_FAIL_CLOSED`     | `false` | Reject (vs allow) when Redis is unreachable.         |

```bash
export REDIS_URL=redis://localhost:6379
export REDIS_RATE_LIMIT_DEFAULT_LIMIT=1000   # >0 enables the global limiter
export REDIS_RATE_LIMIT_TRUST_PROXY=true
export REDIS_RATE_LIMIT_IETF_HEADERS=true
```

Per-call arguments (`emit_headers`, `ietf_headers`, `fail_closed`) override the
settings; when omitted they inherit the configured default.

---

## 9. Testing

The dependency and backend integrate with FastAPI's `dependency_overrides`, so
tests can swap in a fake Redis without monkey-patching:

```python
import fakeredis.aioredis
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from redis_fastapi import FastAPIRedis, rate_limit, get_async_redis

app = FastAPI()
FastAPIRedis(app).rate_limiting()

@app.get("/limited", dependencies=[Depends(rate_limit("2/minute"))])
async def limited():
    return {"ok": True}

fake = fakeredis.aioredis.FakeRedis()
app.dependency_overrides[get_async_redis] = lambda: fake

with TestClient(app) as client:
    assert client.get("/limited").status_code == 200
    assert client.get("/limited").status_code == 200
    blocked = client.get("/limited")
    assert blocked.status_code == 429
    assert "Retry-After" in blocked.headers
```

`fakeredis` lacks `INCREX` but runs Lua (via the `fakeredis[lua]` extra), so this
exercises the real `EVAL` path — the production path for Redis 2.6–8.7. Add
integration tests against a real Redis 8.8+ to cover the `INCREX` path and to
assert that `asyncio.gather` of more-than-limit concurrent requests allows
exactly the limit (proving atomicity).

---

## Quick reference

| Scenario                                     | Recommended                                                              |
|----------------------------------------------|--------------------------------------------------------------------------|
| Limit one endpoint                           | [`rate_limit()`](#1-per-route-limits-rate_limit)                         |
| Limit per user / API key / tenant            | [`rate_limit(identifier=...)`](#2-identifiers) with your own callable    |
| One limit for the whole app                  | [Global limiter](#4-global-app-wide-limiter)                             |
| One budget shared by several routes          | [`rate_limit(scope="name")`](#sharing-one-limit-across-routes)           |
| Internal traffic should bypass               | [`skip_when=...`](#conditional-skip-and-custom-responses)                |
| Custom 429 body                              | [`on_limit_exceeded=...`](#conditional-skip-and-custom-responses)        |
| Non-HTTP / custom keys / dynamic cost        | [`RateLimitBackend`](#5-ratelimitbackend-imperative-api)                 |
| Standards-track headers                      | [`ietf_headers=True`](#3-response-headers)                               |

---

## Best practices

1. **Use `FastAPIRedis(app).lifespan().rate_limiting()`** for setup.
2. **Start with `rate_limit("N/unit")`** on the routes that need protection.
3. **Pick the identifier deliberately** - `ip_identifier` for anonymous traffic,
   or a small custom callable keyed by user / API key for authenticated traffic.
4. **Enable `trust_proxy` only behind a proxy you control**, never on a directly
   exposed app.
5. **Keep fail-open** unless rejecting traffic during a Redis outage is genuinely
   safer for your API than allowing it.
6. **Use an explicit `scope`** when a group of routes should draw from one
   budget per client; leave it unset for per-route limits (the default).
7. **Use `dependency_overrides`** in tests - no monkey-patching needed.

---

## Further reading

- [Configuration Guide](configuration.md) - Redis connection settings
- [Observability Guide](observability.md) - OTEL spans & metrics for rate-limit checks
- [Caching Guide](caching.md) - the sibling feature, configured the same way
- [API Reference](../api/reference.md) - full public API
- [Redis window-counter rate limiter](https://redis.io/docs/latest/commands/increx/#pattern-window-counter-rate-limiter)
- [IETF RateLimit header fields](https://datatracker.ietf.org/doc/html/draft-ietf-httpapi-ratelimit-headers)
