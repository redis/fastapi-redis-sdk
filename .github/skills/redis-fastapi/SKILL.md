---
name: fastapi-redis-sdk
description: >
  fastapi-redis-sdk development skill. Use when writing code, tests, or configuration
  for the fastapi-redis-sdk library — the official Redis integration for FastAPI.
  Covers project setup (uv + nox), DI-based caching patterns, connection
  lifecycle, async/sync endpoints, testing conventions, and CI workflows.
  Do NOT use for general Redis or FastAPI questions unrelated to this library.
license: MIT
---

# fastapi-redis-sdk

Official Redis integration for FastAPI — connection management and DI-based
caching with automatic key consistency.

## Project layout

```
src/redis_fastapi/       # Library source (single flat package)
  __init__.py            # Public API re-exports
  setup.py               # FastAPIRedis fluent builder
  lifespan.py            # redis_lifespan async context manager
  deps.py                # FastAPI DI factories & type aliases
  config.py              # RedisSettings (pydantic-settings)
  cache.py               # cache(), cache_evict(), cache_put() + middleware
  cache_backend.py       # CacheBackend (async) + SyncCacheBackend
  telemetry.py           # Optional OpenTelemetry instrumentation
  types.py               # Coder, KeyBuilder protocols
tests/
  unit/                  # fakeredis-based, no real Redis needed
  integration/           # Requires Redis on localhost:6379
pyproject.toml           # uv build backend, dependency groups, tool config
noxfile.py               # CI-mirroring sessions (lint, typecheck, security, tests, docs)
```

## Setup and tooling

Package manager: **uv**. Task runner: **nox** (with `uv` venv backend).

```bash
uv sync --all-groups          # Install all deps
uv run nox                    # Run ALL CI checks locally
uv run nox -s lint            # Lint + format check only
uv run nox -s typecheck       # mypy
uv run nox -s tests-3.12      # Full suite on a specific Python version
uv run nox -s tests_unit-3.12 # Unit suite only (fakeredis, no server)
uv run nox -s tests_integration-3.12  # Integration suite only (needs Redis)
uv run nox -s fix             # Auto-fix lint/format
uv run nox -s docs_serve      # Live-reload docs at localhost:8000
```

## Architecture rules

### Lifespan is mandatory

Redis pools MUST be initialised via the app lifespan. There is no fallback.
Accessing the pool without a lifespan raises `RuntimeError`.

```python
app = FastAPI()
FastAPIRedis(app).lifespan().caching()  # Always call .lifespan()
```

### Async-first, sync via bridge

- All DI factories (`get_async_redis`, `get_cache_backend`) are async.
- FastAPI runs them correctly even for sync endpoints (threadpool).
- `SyncCacheBackend` wraps async calls via `anyio.from_thread.run` —
  use only from sync endpoints running in FastAPI's worker threads.

### Dependency injection types

```python
from redis_fastapi import AsyncRedisDep, CacheBackendDep, SyncCacheBackendDep

# Async endpoint — use AsyncRedisDep or CacheBackendDep
async def endpoint(redis: AsyncRedisDep): ...
async def endpoint(cache: CacheBackendDep): ...

# Sync endpoint — use SyncCacheBackendDep
def endpoint(cache: SyncCacheBackendDep): ...
```

### Lifespan wrapping

`.lifespan()` **wraps** the app's existing lifespan — it does not replace it.
Multiple builder calls nest around whatever is already there. For explicit
ordering, skip `.lifespan()` and compose manually with `redis_lifespan`:

```python
from redis_fastapi import FastAPIRedis, redis_lifespan

@asynccontextmanager
async def my_lifespan(app):
    async with redis_lifespan(app):
        async with db_lifespan(app):
            yield

app = FastAPI(lifespan=my_lifespan)
FastAPIRedis(app).caching()   # no .lifespan() — user owns it
```

See `docs/guide/architecture.md` § Lifespan wrapping for details.

### Caching patterns

Two patterns, same pool:

1. **DI factories** — `cache(ttl=N, eviction_group="x")`, `cache_evict(...)`,
   `cache_put(...)` as `Depends()`. Requires `.caching()` on setup.
   - Automatic HTTP semantics (ETag, 304, Cache-Control, X-Redis-Cache).
   - Only GET requests are cached; non-GET bypasses cache entirely.
   - Graceful degradation: Redis failures log warnings, never crash.
2. **CacheBackend** — `get`/`set`/`delete`/`has`/`delete_group`.
   For conditional logic, cascade invalidation, dynamic TTL.
   - Accepts `timedelta` for TTL (DI factories accept `int` seconds only).
   - No automatic HTTP headers — add manually if needed.

Choose `cache()` for most GET endpoints. Choose `CacheBackend` when you
need conditional caching, multi-step invalidation, or custom serializers.
`cache_evict()`/`cache_put()` bridge writes back to the same cache keys.

### Cache hit/miss internals

- **Hit:** DI dependency raises `CacheHitException` → exception handler
  returns cached response. Endpoint never executes.
- **Miss:** `CacheResponseCaptureMiddleware` buffers the response body
  and stores it in Redis after the endpoint returns.
- No full ASGI middleware for reads — benchmarks showed no gain over DI.
  See `docs/guide/architecture.md` § Why not a full ASGI middleware.

### Storage model

Cached entries are Redis **string** keys with eviction-group prefixes.
Namespace deletion uses `SCAN` + `DEL`. Hash-based storage (faster
eviction-group deletion via single `DEL`) is a future opt-in gated on
Redis ≥ 8.0. See `docs/guide/architecture.md` § Storage model.

### Telemetry

Three independent OTel layers (each opt-in):

1. **HTTP spans** — `opentelemetry-instrumentation-fastapi` (external).
2. **Cache operation spans + metrics** — `FastAPIRedis(app)...otel()` or
   `REDIS_OTEL_ENABLED=true`. Install `fastapi-redis-sdk[otel]`.
3. **Redis command spans** — `REDIS_OTEL_REDIS_ENABLED=true` or
   `opentelemetry-instrumentation-redis` (not both).

See `docs/guide/architecture.md` § Telemetry for span names and metrics.

## Testing conventions

- **Unit tests** (`tests/unit/`): use `fakeredis.aioredis`, no real Redis.
- **Integration tests** (`tests/integration/`): real Redis, decorated with
  `@requires_redis` (auto-skip if server unreachable).
- `filterwarnings = ["error"]` in pytest config — all warnings are errors.
- Coverage threshold: 80% (`--cov-fail-under=80`). The integration-only
  session lowers this to 70 (`_INTEGRATION_COV_FLOOR` in `noxfile.py`) —
  83 integration tests alone cover ~78% of `src/`.
- CI runs the two suites in **separate jobs**: `unit-tests` runs
  `tests_unit` across 3 OSes x 5 Pythons, `integration-tests` runs
  `tests_integration` against Redis 8.8 and 7.4. See
  `docs/guide/compatibility.md` § 4.
- `asyncio_mode = "auto"` — no need for `@pytest.mark.asyncio`.

### Test fixtures

- `fake_async_redis` — fakeredis instance, available in unit tests.
- `real_redis` / `real_async_redis` — real Redis clients for integration.
- Every integration test flushes the DB on teardown.

## Configuration

All settings via env vars prefixed `REDIS_` or `.env` file.
Key vars: `REDIS_URL`, `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`,
`REDIS_SSL`, `REDIS_CLUSTER`, `REDIS_PREFIX`, `REDIS_DEFAULT_TTL`.

## Code style

- **ruff** for linting + formatting (line-length 88).
- **mypy** strict mode, Python 3.10 target.
- `from __future__ import annotations` in all source files.
- Type aliases use `Union[]` (not `X | Y`) for Python 3.10 compat in
  `deps.py` and `types.py` (ruff rule UP007 ignored there).

## CI

CI is defined in `.github/workflows/ci.yml` and delegates to nox sessions.
Do NOT add inline `uv run ruff` / `uv run mypy` commands to CI — use
`uv run nox -s <session>` so CI and local checks stay in sync.

## Do NOT change

- **Build system.** The project uses `uv_build` as its build backend.
  NEVER replace it with `hatchling`, `setuptools`, `flit`, or any other
  build backend. All build config lives in `pyproject.toml` under
  `[build-system]` and `[tool.uv.build-backend]`.
- **DI approach.** Caching (for example) is implemented as `Depends()` factories
  (`cache()`, `cache_evict()`, `cache_put()`), NOT as decorators. NEVER
  refactor DI based solutions to use a decorator-based approach (`@cache`). The DI
  pattern is a deliberate design decision, see /guide/architecture.md
  for more information.

### Cache key format

Default key: `{prefix}:{eviction_group}:{path}:{sorted_query_params}`.
Eviction group is wrapped in hash-tag braces `{ns}` for Redis Cluster
slot alignment. Query params are sorted alphabetically for determinism.
Headers are NOT part of the key — use a custom `key_builder` for
header-dependent responses (e.g. `Accept`).

### TTL behavior

Fixed-window expiry. Accessing a cached entry does NOT extend its TTL.
`ttl=0` or `ttl=None` means no automatic expiration.

### Error handling

All Redis errors (`RedisError`, `OSError`) are caught and logged as
warnings. Cache reads return `None`/miss; writes are silently dropped.
Exceptions from endpoints are never cached — only successful responses.
Corrupted cache data is treated as a miss (auto-fallback).

## Common pitfalls

- `anyio.from_thread.run` takes a **zero-arg callable** returning an
  awaitable, NOT a coroutine object. Wrap with `lambda`.
- `SyncCacheBackendDep` must be imported at module level when using
  `from __future__ import annotations`, or FastAPI cannot resolve the type.
- Do not use `uv add --dev` in CI workflows — deps are managed by nox
  sessions and `pyproject.toml` dependency groups.
- `cache()` reads `get_settings()` at dependency-creation time (when the
  module loads), not per-request. Runtime setting changes won't be picked
  up by already-registered routes.
