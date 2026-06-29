# FastAPI Redis

**Official FastAPI integration for Redis** - connection management and DI-based
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

Idiomatic Redis for FastAPI: manage connection pools through the app lifespan,
cache responses and rate-limit endpoints with native `Depends()` factories, and
get OpenTelemetry instrumentation out of the box. Install with
`pip install fastapi-redis-sdk` (Python 3.10+, Redis 7.4+).

## Documentation

### Getting Started

- [**Installation**](getting-started/installation.md) - supported versions and
  how to install with pip, uv, or Poetry.
- [**Quick Start**](getting-started/quick-start.md) - wire up Redis and cache
  your first endpoint in a handful of lines.

### User Guide

- [**Architecture**](guide/architecture.md) - the decisions behind the library:
  why dependency injection over decorators, the connection lifecycle, the
  strings-vs-hashes storage model, and the telemetry layers.
- [**Caching**](guide/caching.md) - the `cache()` / `cache_evict()` /
  `cache_put()` factories, the imperative `CacheBackend`, caching Pydantic
  models, and HTTP cache headers (`ETag`, `Cache-Control`, `304`).
- [**Rate Limiting**](guide/rate-limiting.md) - distributed per-client limits:
  the per-route dependency and global limiter, the fluent rate language,
  identifiers, and `X-RateLimit-*` / `Retry-After` headers.
- [**Observability**](guide/observability.md) - OpenTelemetry spans and metrics
  for cache and rate-limit operations, and how to enable each layer.
- [**Configuration**](guide/configuration.md) - Pydantic-Settings configuration
  and the full `REDIS_*` environment variable reference.
- [**Benchmarks**](guide/benchmarks.md) - performance measured against popular
  caching libraries.

### API Reference

- [**API Reference**](api/reference.md) - public functions, DI dependencies, and
  classes.
- [**Configuration API**](api/configuration.md) - `RedisSettings` fields plus the
  `Coder` and `KeyBuilder` types.

