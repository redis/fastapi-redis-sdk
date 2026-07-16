# FastAPI Redis

**Official FastAPI integration for Redis** — connection management and DI-based
caching with automatic key consistency.

[![Integration](https://github.com/redis/fastapi-redis-sdk/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/redis/fastapi-redis-sdk/actions/workflows/ci.yml)
[![PyPI - Version](https://img.shields.io/pypi/v/fastapi-redis-sdk)](https://pypi.org/project/fastapi-redis-sdk/)
[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue&logo=redis)](https://www.python.org/downloads/)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v0.json)](https://astral.sh/ruff)
[![codecov](https://codecov.io/gh/redis/fastapi-redis-sdk/branch/main/graph/badge.svg?token=yenl5fzxxr)](https://codecov.io/gh/redis/fastapi-redis-sdk)
[![Guide](https://img.shields.io/badge/mkdocs-guide-526CFE?logo=materialformkdocs&logoColor=white)](https://redis.github.io/fastapi-redis-sdk/)


[![Discord](https://img.shields.io/discord/697882427875393627.svg?style=social&logo=discord)](https://discord.gg/redis)
[![Twitch](https://img.shields.io/twitch/status/redisinc?style=social)](https://www.twitch.tv/redisinc)
[![YouTube](https://img.shields.io/youtube/channel/views/UCD78lHSwYqMlyetR0_P4Vig?style=social)](https://www.youtube.com/redisinc)
[![Twitter](https://img.shields.io/twitter/follow/redisinc?style=social)](https://twitter.com/redisinc)
[![Stack Exchange questions](https://img.shields.io/stackexchange/stackoverflow/t/fastapi-redis-sdk?style=social&logo=stackoverflow&label=Stackoverflow)](https://stackoverflow.com/questions/tagged/fastapi-redis-sdk)
---

## Features

- **Fluent setup** — `FastAPIRedis(app).lifespan().caching()` configures pools and caching in one chain, attaching to the [FastAPI lifespan events](https://fastapi.tiangolo.com/advanced/events/)
- **Dependency injection** — `cache()`, `cache_evict()`, `cache_put()` as `Depends()` factories, plus `CacheBackend` for complex invalidation and conditional logic
- **HTTP-native caching** — [`ETag`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/ETag), [`304 Not Modified`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/304), [`Cache-Control`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Cache-Control) directives out of the box
- **Testable** — full `dependency_overrides` support; no need for monkey-patching
- **Pydantic-validated configuration** — fully configurable via environment variables or via an `.env` file

## Installation

```bash
pip install fastapi-redis-sdk
```

Requires Python 3.10+ and a Redis 7.4+ server.

See the [Installation](getting-started/installation.md) page for full
requirements and alternative package managers.

## Quick start

```python
from fastapi import FastAPI
from redis_fastapi import FastAPIRedis, AsyncRedisDep

app = FastAPI()
FastAPIRedis(app).lifespan()

@app.get("/items")
async def get_items(redis: AsyncRedisDep):
    return {"items": await redis.get("items")}
```

Connection pools are managed automatically and closed on shutdown. The builder
wraps any existing lifespan — multiple libraries can each register their own
without conflicting.

## Caching

Two caching patterns for different needs, sharing the same connection pool:

| Pattern                                     | Best for                                 |
|---------------------------------------------|------------------------------------------|
| `cache()`, `cache_evict()`, `cache_put()`   | Most endpoints — read/write/invalidate   |
| **CacheBackend**                            | Complex invalidation, conditional logic  |

```python
from fastapi import Depends
from redis_fastapi import FastAPIRedis, cache, cache_evict, cache_put, default_key_builder

app = FastAPI()
FastAPIRedis(app).lifespan().caching()


# READ — cache GET responses
@app.get("/products/{product_id}", dependencies=[Depends(cache(ttl=300, eviction_group="products"))])
async def get_product(product_id: int):
    return await db.get_product(product_id)


# INVALIDATE — evict the cached entry on delete
@app.delete(
    "/products/{product_id}",
    dependencies=[Depends(cache_evict(eviction_group="products", key_builder=default_key_builder))],
)
async def delete_product(product_id: int):
    await db.delete(product_id)


# WRITE-THROUGH — update the cache so the next GET is a HIT
@app.put(
    "/products/{product_id}",
    dependencies=[Depends(cache_put(eviction_group="products", key_builder=default_key_builder, ttl=300))],
)
async def replace_product(product_id: int, body: Product):
    return await db.update(product_id, body)
```

All three factories share the same `key_builder`, so GET, DELETE, and PUT on the
same path target the exact same cache key. Responses include `X-Redis-Cache`
(HIT/MISS), [`Cache-Control`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Cache-Control), and [`ETag`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/ETag) headers with [`304 Not Modified`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/304) support.

See the [Caching Guide](guide/caching.md) for detailed examples, feature
comparison, and best practices.

## Configuration

All settings are read from environment variables (prefixed `REDIS_`) or a
`.env` file:

```bash
export REDIS_URL=redis://user:pass@host:6379/0
```

Or configure individual fields:

```bash
export REDIS_HOST=redis.example.com
export REDIS_PORT=6380
export REDIS_PASSWORD=secret
```

See the [Configuration Guide](guide/configuration.md) for the full environment
variable reference.

