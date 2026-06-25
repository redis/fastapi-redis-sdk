# Configuration API

## `RedisSettings`

```python
from redis_fastapi import RedisSettings
```

Pydantic Settings model holding all connection and behaviour settings.

Automatically reads from:
1. Environment variables (prefixed with `REDIS_`)
2. `.env` file (if present)
3. Default values

### Fields

| Field | Type | Default | Validation | Description |
|-------|------|---------|------------|-------------|
| `url` | `str \| None` | `None` | - | Full Redis URL |
| `host` | `str` | `"localhost"` | - | Redis host |
| `port` | `int` | `6379` | 1-65535 | Redis port |
| `db` | `int` | `0` | 0-15 | Database number |
| `username` | `str \| None` | `None` | - | Redis username |
| `password` | `SecretStr \| None` | `None` | - | Redis password (secure) |
| `ssl` | `bool` | `False` | - | Enable TLS |
| `ssl_certfile` | `str \| None` | `None` | - | Client certificate |
| `ssl_keyfile` | `str \| None` | `None` | - | Client key |
| `ssl_ca_certs` | `str \| None` | `None` | - | CA bundle |
| `ssl_check_hostname` | `bool` | `False` | - | Verify hostname |
| `max_connections` | `int \| None` | `None` | >= 1 | Max pool size |
| `socket_timeout` | `float \| None` | `None` | >= 0 | Socket timeout |
| `socket_connect_timeout` | `float \| None` | `None` | >= 0 | Connect timeout |
| `cluster` | `bool` | `False` | - | OSS Cluster mode |
| `prefix` | `str` | `"redis:fastapi"` | - | Key prefix |
| `default_ttl` | `int` | `0` | >= 0 | Default cache TTL (0 = no expiry) |
| `otel_enabled` | `bool` | `False` | - | Enable OTel cache spans/metrics |
| `otel_redis_enabled` | `bool` | `False` | - | Enable redis-py native OTel |

### Methods

#### `connection_kwargs() -> dict`

Returns the full set of kwargs for pool/client construction.

Automatically extracts the secret value from `password` field if present.

```python
settings = get_settings()
kwargs = settings.connection_kwargs()
# → {"host": "localhost", "port": 6379, "password": "actual_password", ...}
```

#### `pattern_prefix(pattern: str) -> str`

Returns the full prefix for a given pattern name.

```python
settings = get_settings()
settings.pattern_prefix("cache")  # "redis:fastapi:cache"
```

## `get_settings()`

```python
from redis_fastapi import get_settings
```

Returns a cached `RedisSettings` instance.

Uses `@lru_cache` to ensure the same instance is returned on every call, preventing multiple reads of environment variables and `.env` file.

**Usage:**

```python
# Direct usage
settings = get_settings()
print(settings.host)

# As FastAPI dependency
from typing import Annotated
from fastapi import Depends

@app.get("/config")
async def show_config(config: Annotated[RedisSettings, Depends(get_settings)]):
    return {"host": config.host}
```

**Testing:**

```python
def get_test_settings():
    return RedisSettings(host="test-redis", db=1)

app.dependency_overrides[get_settings] = get_test_settings
```

## `enable_telemetry()`

```python
from redis_fastapi import enable_telemetry
```

Activate OpenTelemetry instrumentation for cache operations.

Requires `pip install fastapi-redis-sdk[otel]`.  If the `opentelemetry` packages
are not installed, a warning is logged and telemetry remains disabled.

Safe to call multiple times — subsequent calls are no-ops.

Typically you don't call this directly; use the builder instead:

```python
FastAPIRedis(app).lifespan().caching().otel()  # calls enable_telemetry() internally
```

Or set the environment variable `REDIS_OTEL_ENABLED=true`.

See the [Configuration guide — OpenTelemetry](../guide/configuration.md#opentelemetry) for full details.

## Types

### `Coder` (Protocol)

```python
class Coder(Protocol):
    @classmethod
    def encode(cls, value: Any) -> str: ...
    @classmethod
    def decode(cls, value: str) -> Any: ...
```

### `JsonCoder`

Default `Coder` implementation using `json.dumps` / `json.loads`.

### `FastAPIJsonCoder`

```python
from redis_fastapi import FastAPIJsonCoder
```

JSON coder that runs values through FastAPI's `jsonable_encoder()` before
calling `json.dumps()`. Use it for cache values containing Pydantic models,
`datetime`, `UUID`, enums, dataclasses, and other FastAPI-compatible values.

```python
from redis_fastapi import CacheBackend, FastAPIJsonCoder

cache = CacheBackend(redis, coder=FastAPIJsonCoder)
await cache.set("item:1", item)
```

### `pydantic_model_coder()`

```python
from redis_fastapi import pydantic_model_coder

ProductCoder = pydantic_model_coder(Product)
```

Creates a `Coder` class for one Pydantic model type. Encoded values can be
model instances or data that Pydantic can validate into that model; decoded
values are returned as model instances.

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
```

### `KeyBuilder`

```python
KeyBuilder = Callable[..., str | Awaitable[str]]
```

Callable that receives `(request, eviction_group, prefix)` and returns a cache key.
