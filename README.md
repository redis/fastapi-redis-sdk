# Official FastAPI integration for Redis

Idiomatic Redis integration for FastAPI - connection management and DI-based caching with automatic key consistency.

[![Integration](https://github.com/redis/fastapi-redis-sdk/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/redis/fastapi-redis-sdk/actions/workflows/ci.yml)
[![PyPI - Version](https://img.shields.io/pypi/v/fastapi-redis-sdk)](https://pypi.org/project/fastapi-redis-sdk/)
[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue&logo=redis)](https://www.python.org/downloads/)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v0.json)](https://astral.sh/ruff)
[![PyPI Downloads](https://img.shields.io/pypi/dm/fastapi-redis-sdk)](https://pypistats.org/packages/fastapi-redis-sdk)
[![codecov](https://codecov.io/gh/redis/fastapi-redis-sdk/branch/main/graph/badge.svg?token=yenl5fzxxr)](https://codecov.io/gh/redis/fastapi-redis-sdk)
[![Guide](https://img.shields.io/badge/mkdocs-guide-526CFE?logo=materialformkdocs&logoColor=white)](https://redis.github.io/fastapi-redis-sdk/)


[![Discord](https://img.shields.io/discord/697882427875393627.svg?style=social&logo=discord)](https://discord.gg/redis)
[![Twitch](https://img.shields.io/twitch/status/redisinc?style=social)](https://www.twitch.tv/redisinc)
[![YouTube](https://img.shields.io/youtube/channel/views/UCD78lHSwYqMlyetR0_P4Vig?style=social)](https://www.youtube.com/redisinc)
[![Twitter](https://img.shields.io/twitter/follow/redisinc?style=social)](https://twitter.com/redisinc)
[![Stack Exchange questions](https://img.shields.io/stackexchange/stackoverflow/t/fastapi-redis-sdk?style=social&logo=stackoverflow&label=Stackoverflow)](https://stackoverflow.com/questions/tagged/fastapi-redis-sdk)

### Features

- **Fluent setup** — `FastAPIRedis(app).lifespan().caching()` configures pools and caching in one chain, attaching to the [FastAPI lifespan events](https://fastapi.tiangolo.com/advanced/events/)
- **Dependency injection** — `cache()`, `cache_evict()`, `cache_put()` as `Depends()` factories, plus `CacheBackend` for complex invalidation and conditional logic
- **HTTP-native caching** — [`ETag`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/ETag), [`304 Not Modified`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/304), [`Cache-Control`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Cache-Control) directives out of the box
- **Rate limiting** — `rate_limit()` dependency with a fluent rate language (`"10/second"`), `X-RateLimit-*` / `Retry-After` headers, and distributed per-client counters
- **Testable** — full `dependency_overrides` support; no need for monkey-patching
- **Pydantic-validated configuration** — fully configurable via environment variables or via an `.env` file

### Requirements

| Dependency   | Supported versions |
|--------------|--------------------|
| Python       | 3.10 to 3.14       |
| FastAPI      | 0.115+             |
| redis-py     | 6.0+               |
| Pydantic     | 2.0+               |
| Redis server | 7.4+               |

## Installation

```bash
pip install fastapi-redis-sdk
```

## Caching

Cache GET responses with a dependency — on a hit the endpoint is skipped, on a miss the response is stored after it returns:

```python
from fastapi import Depends, FastAPI
from redis_fastapi import FastAPIRedis, cache

app = FastAPI()
FastAPIRedis(app).lifespan().caching()

@app.get("/products/{product_id}", dependencies=[Depends(cache(ttl=300, eviction_group="products"))])
async def get_product(product_id: int):
    return await db.get_product(product_id)
```

`cache_evict()` and `cache_put()` handle invalidation and write-through with matching keys, and `CacheBackendDep` exposes imperative `get`/`set`/`delete`/`has`/`delete_group` for conditional logic. Cached responses carry `X-Redis-Cache` (HIT/MISS), `Cache-Control`, and `ETag` headers with 304 Not Modified support.

See the [Caching Guide](docs/guide/caching.md) for the full patterns, `CacheBackend` usage, Pydantic model caching, feature comparison, and best practices.

## Rate limiting

Protect an endpoint with the `rate_limit()` dependency. Stack two limits to cap **bursts** and **sustained** traffic at once — a short high-rate window plus a longer low-rate window:

```python
from fastapi import Depends, FastAPI
from redis_fastapi import FastAPIRedis, rate_limit

app = FastAPI()
FastAPIRedis(app).lifespan().rate_limiting()

@app.get(
    "/search",
    dependencies=[
        Depends(rate_limit("10/second", scope="search:burst")),      # burst
        Depends(rate_limit("100/minute", scope="search:sustained")),  # sustained
    ],
)
async def search():
    return {"results": [...]}
```

Both limits count per client IP by default; a request must satisfy both, and the distinct `scope` keeps the two counters independent on the same route. When either is exceeded the request gets a `429 Too Many Requests` with `Retry-After`, and every response carries `X-RateLimit-Limit` / `-Remaining` / `-Reset`. Counters live in Redis, so limits hold across every worker and pod.

See the [Rate Limiting Guide](docs/guide/rate-limiting.md) for identifiers, the global limiter, custom responses, IETF headers, and the imperative backend.

## Configuration

All settings are read from environment variables (prefixed `REDIS_`) or a `.env` file. Set `REDIS_URL` for the simplest setup:

```bash
export REDIS_URL=redis://user:pass@host:6379/0
```

Or configure individual fields:

```bash
export REDIS_HOST=redis.example.com
export REDIS_PORT=6380
export REDIS_PASSWORD=secret
```

Additional options: TLS (`REDIS_SSL`, `REDIS_SSL_CERTFILE`, etc.), connection pool (`REDIS_MAX_CONNECTIONS`, `REDIS_SOCKET_TIMEOUT`), OSS Cluster mode (`REDIS_CLUSTER=true`), key prefix (`REDIS_PREFIX`), and default cache TTL (`REDIS_DEFAULT_TTL`, default `0` = no expiry).

For programmatic configuration:

```python
from redis_fastapi import get_settings

settings = get_settings()
settings.url = "redis://custom:6379/0"
settings.default_ttl = 120
```

See the [Configuration Guide](docs/guide/configuration.md) for the full environment variable reference and API details.
