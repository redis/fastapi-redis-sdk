# Configuration

Configuration uses **Pydantic Settings** following [FastAPI's recommended pattern](https://fastapi.tiangolo.com/advanced/settings).

## Features

* **Automatic validation** - Port (1-65535), DB (0-15), timeout ranges
* **.env file support** - Automatically reads from `.env` file
* **Type safety** - Auto-converts environment variable strings
* **Secret handling** - Passwords stored as `SecretStr` (won't print in logs)
* **Better errors** - Clear validation messages
* **Dependency injection** - Easy testing with `app.dependency_overrides`

## Quick Start

```python
from redis_fastapi import get_settings

# Get settings instance
settings = get_settings()
print(settings.host)  # → "localhost"

# Use as dependency (recommended for endpoints)
from typing import Annotated
from fastapi import Depends
from redis_fastapi import RedisSettings

@app.get("/config")
async def show_config(config: Annotated[RedisSettings, Depends(get_settings)]):
    return {"host": config.host, "port": config.port}
```

## Configuration Methods

### 1. Environment Variables (Recommended)

All variables are prefixed with `REDIS_`:

```bash
# Connection - URL mode (simplest)
export REDIS_URL=redis://user:pass@host:6379/0

# OR Connection - KV mode (individual fields)
export REDIS_HOST=redis.example.com
export REDIS_PORT=6380
export REDIS_DB=1
export REDIS_USERNAME=default
export REDIS_PASSWORD=secret

# TLS/SSL
export REDIS_SSL=true
export REDIS_SSL_CERTFILE=/path/to/cert.pem
export REDIS_SSL_KEYFILE=/path/to/key.pem
export REDIS_SSL_CA_CERTS=/path/to/ca.pem
export REDIS_SSL_CHECK_HOSTNAME=true

# Connection Pool
export REDIS_MAX_CONNECTIONS=50
export REDIS_SOCKET_TIMEOUT=5.0
export REDIS_SOCKET_CONNECT_TIMEOUT=2.0

# Cluster
export REDIS_CLUSTER=true

# Caching
export REDIS_PREFIX=myapp
export REDIS_DEFAULT_TTL=300

# Telemetry (OpenTelemetry)
export REDIS_OTEL_ENABLED=true
export REDIS_OTEL_REDIS_ENABLED=true
```

When `REDIS_URL` is set it takes precedence over individual fields.

### 2. .env File (Automatic)

Create a `.env` file in your project root:

```bash
# .env
REDIS_URL=redis://localhost:6379/0
REDIS_DEFAULT_TTL=300
REDIS_PREFIX=myapp
REDIS_MAX_CONNECTIONS=50
```

Pydantic Settings **automatically loads** the `.env` file - no extra code needed!

### 3. Programmatic Configuration

```python
from redis_fastapi import RedisSettings

# Create custom instance
custom = RedisSettings(
    host="redis.example.com",
    port=6380,
    db=1,
    password="secret",
    default_ttl=300,
)

# Use in your code
kwargs = custom.connection_kwargs()
```

### 4. Dependency Injection (Testing)

```python
from redis_fastapi import get_settings, RedisSettings

def get_test_settings():
    return RedisSettings(host="test-redis", db=1)

# Override in tests
app.dependency_overrides[get_settings] = get_test_settings
```

## Connection Modes

### URL Mode (Simplest)

Use a single connection URL:

```bash
# Basic
export REDIS_URL=redis://localhost:6379/0

# With password
export REDIS_URL=redis://:password@localhost:6379/0

# With username and password
export REDIS_URL=redis://username:password@localhost:6379/0

# TLS/SSL
export REDIS_URL=rediss://localhost:6380/0

# Cluster (multiple nodes)
export REDIS_URL=redis://node1:6379,node2:6379,node3:6379
```

### KV Mode (Individual Fields)

Set connection details separately:

```bash
export REDIS_HOST=redis.example.com
export REDIS_PORT=6380
export REDIS_DB=1
export REDIS_USERNAME=default
export REDIS_PASSWORD=secret
```

When `REDIS_URL` is set, it takes precedence over individual fields.

## OSS Cluster Mode

```bash
export REDIS_CLUSTER=true
export REDIS_URL=redis://node1:6379,node2:6379,node3:6379
```

When enabled, `AsyncRedisDep` yields an `AsyncRedisCluster`.

## Key Prefix

All pattern data is prefixed with `redis:fastapi` by default, producing keys like `redis:fastapi:cache:...`.

```bash
export REDIS_PREFIX=myapp:redis
# keys become: myapp:redis:cache:...
```

Individual DI factories can override their prefix:

```python
@app.get("/items", dependencies=[Depends(cache(prefix="custom:prefix"))])
async def my_endpoint():
    ...
```

## Validation and Type Safety

Pydantic automatically validates and converts values:

```bash
# ✔ Valid
export REDIS_PORT=6379        # str → int
export REDIS_SSL=true         # str → bool
export REDIS_DB=1             # str → int

# 𝚇 Invalid - raises ValidationError
export REDIS_PORT=999999      # Port must be 1-65535
export REDIS_DB=20            # DB must be 0-15
export REDIS_SOCKET_TIMEOUT=-1  # Timeout must be >= 0
```

### Secret Handling

Passwords are stored as `SecretStr` and won't print in logs:

```python
settings = get_settings()
print(settings.password)  # → **********

# Extract secret value when needed
password = settings.password.get_secret_value()  # → "actual_password"
```

## All Environment Variables

| Variable | Type | Default | Validation | Description |
|----------|------|---------|------------|-------------|
| `REDIS_URL` | `str` | - | - | Full Redis URL (takes precedence) |
| `REDIS_HOST` | `str` | `localhost` | - | Redis host |
| `REDIS_PORT` | `int` | `6379` | 1-65535 | Redis port |
| `REDIS_DB` | `int` | `0` | 0-15 | Database number |
| `REDIS_USERNAME` | `str` | - | - | Redis username |
| `REDIS_PASSWORD` | `SecretStr` | - | - | Redis password (secure) |
| `REDIS_SSL` | `bool` | `false` | - | Enable TLS |
| `REDIS_SSL_CERTFILE` | `str` | - | - | Client certificate path |
| `REDIS_SSL_KEYFILE` | `str` | - | - | Client key path |
| `REDIS_SSL_CA_CERTS` | `str` | - | - | CA bundle path |
| `REDIS_SSL_CHECK_HOSTNAME` | `bool` | `false` | - | Verify server hostname |
| `REDIS_MAX_CONNECTIONS` | `int` | - | >= 1 | Max pool connections |
| `REDIS_SOCKET_TIMEOUT` | `float` | - | >= 0 | Socket timeout (seconds) |
| `REDIS_SOCKET_CONNECT_TIMEOUT` | `float` | - | >= 0 | Connect timeout (seconds) |
| `REDIS_CLUSTER` | `bool` | `false` | - | Enable OSS Cluster mode |
| `REDIS_PREFIX` | `str` | `redis:fastapi` | - | Integration-wide key prefix |
| `REDIS_DEFAULT_TTL` | `int` | `0` | >= 0 | Default cache TTL in seconds (0 = no expiry) |
| `REDIS_OTEL_ENABLED` | `bool` | `false` | - | Enable OTel cache spans/metrics |
| `REDIS_OTEL_REDIS_ENABLED` | `bool` | `false` | - | Enable redis-py native OTel |

## Production Configuration Examples

### Development

```bash
# .env
REDIS_URL=redis://localhost:6379/0
REDIS_PREFIX=myapp:dev
REDIS_DEFAULT_TTL=60
```

### Staging

```bash
# .env
REDIS_URL=redis://:staging_password@redis-staging:6379/0
REDIS_PREFIX=myapp:staging
REDIS_DEFAULT_TTL=300
REDIS_MAX_CONNECTIONS=50
REDIS_SOCKET_TIMEOUT=5.0
```

### Production

```bash
# .env
REDIS_URL=rediss://username:password@redis.prod.example.com:6380/0
REDIS_SSL=true
REDIS_SSL_CHECK_HOSTNAME=true
REDIS_PREFIX=myapp
REDIS_DEFAULT_TTL=600
REDIS_MAX_CONNECTIONS=100
REDIS_SOCKET_TIMEOUT=5.0
REDIS_SOCKET_CONNECT_TIMEOUT=2.0
```

### Production with Redis Cluster

```bash
# .env
REDIS_CLUSTER=true
REDIS_URL=redis://node1:6379,node2:6379,node3:6379
REDIS_PASSWORD=cluster_password
REDIS_PREFIX=myapp
REDIS_DEFAULT_TTL=600
REDIS_MAX_CONNECTIONS=100
```

### FastAPI Cloud

[FastAPI Cloud](https://fastapicloud.com/) has a built-in
[Redis Cloud integration](https://fastapicloud.com/docs/integrations/redis-integration/).
When you connect a Redis Cloud database to your app, FastAPI Cloud automatically
injects the connection string as the `REDIS_URL` environment variable — which is
exactly what `fastapi-redis-sdk` reads by default. No extra configuration is needed:

1. In the FastAPI Cloud dashboard, go to your app's **Storage** tab.
2. Connect (or create) a Redis Cloud database.
3. Optionally trigger a redeployment.

Your app will pick up `REDIS_URL` on the next deploy. If you need additional
tuning (TTL, prefix, pool size, etc.), set those as separate environment
variables in the dashboard or via the CLI:

```bash
fastapi cloud env set REDIS_PREFIX myapp
fastapi cloud env set REDIS_DEFAULT_TTL 600
fastapi cloud env set REDIS_MAX_CONNECTIONS 100
```

For sensitive values use the `--secret` flag:

```bash
fastapi cloud env set --secret REDIS_PASSWORD your_password
```
!!! note "Demo App"
    The repository includes a ready-to-deploy demo app at `examples/main.py`
    with endpoints for health checks, Redis `PING`, cached responses, and
    `CacheBackend` usage. The entrypoint is already configured in `pyproject.toml`
    (`[tool.fastapi] entrypoint`), so FastAPI Cloud picks it up automatically.

See the [FastAPI Cloud environment variables docs](https://fastapicloud.com/docs/builds-and-deployments/environment-variables/)
for details on managing secrets and bulk-importing `.env` files.

## Accessing Settings in Code

```python
from redis_fastapi import get_settings

# Get settings instance
settings = get_settings()

# Read configuration
print(f"Connected to: {settings.host}:{settings.port}")
print(f"Default TTL: {settings.default_ttl}")
print(f"Prefix: {settings.prefix}")

# Build pattern prefix
cache_prefix = settings.pattern_prefix("cache")
# → "redis:fastapi:cache"
```

## OpenTelemetry

fastapi-redis-sdk can emit spans and metrics for every cache operation via
[OpenTelemetry](https://opentelemetry.io/).  Telemetry is **opt-in** — when
disabled (the default) all instrumentation is a zero-cost no-op.

### Install the optional dependency

```bash
pip install fastapi-redis-sdk[otel]
```

### Enable via the builder (recommended)

```python
from fastapi import FastAPI
from redis_fastapi import FastAPIRedis

app = FastAPI()
FastAPIRedis(app).lifespan().caching().otel()
```

### Enable via environment variable

```bash
export REDIS_OTEL_ENABLED=true
```

When `REDIS_OTEL_ENABLED=true`, calling `.otel()` is not required — the
telemetry module is activated automatically at startup.

### redis-py native OTel

fastapi-redis-sdk instruments the **cache layer** (hit/miss, write, eviction).  If
you also want low-level **Redis command** spans and connection-pool metrics, enable
the redis-py native integration:

```bash
export REDIS_OTEL_REDIS_ENABLED=true
```

Or both together:

```bash
export REDIS_OTEL_ENABLED=true
export REDIS_OTEL_REDIS_ENABLED=true
```

If you already use `opentelemetry-instrumentation-redis` externally, leave
`REDIS_OTEL_REDIS_ENABLED=false` to avoid duplicate instrumentation.

### What is emitted

**Spans** — one per cache operation (`cache.get`, `cache.set`, `cache.evict`,
`cache.put`, and the `cache.backend.*` equivalents).  Each span carries
attributes such as `cache.key`, `cache.hit`, `cache.eviction_group`, and
`cache.ttl`.

**Metrics** (counters and histograms):

| Metric | Type | Description |
|--------|------|-------------|
| `redis_fastapi.cache.requests` | Counter | Lookups by `result` (`hit` / `miss` / `bypass`) |
| `redis_fastapi.cache.writes` | Counter | Writes by `type` (`miss_fill` / `write_through`) |
| `redis_fastapi.cache.evictions` | Counter | Evictions by `type` (`key` / `group`) |
| `redis_fastapi.cache.latency` | Histogram | Operation duration by `operation` |

### Non-intrusiveness guarantee

A failure inside the telemetry layer **never** breaks a cache operation or HTTP
response.  All OTel calls are wrapped in `try/except` and errors are logged at
`DEBUG` level only.

For a deeper look at how the three observability layers compose, see the
[Telemetry section in the Architecture guide](architecture.md#telemetry).

---

## Troubleshooting

### Validation Errors

If you see `ValidationError`, check your environment variables:

```bash
# Check current values
env | grep REDIS

# Common issues:
# - REDIS_PORT must be 1-65535
# - REDIS_DB must be 0-15
# - REDIS_SOCKET_TIMEOUT must be >= 0
# - Boolean values: use "true"/"false", "1"/"0", or "yes"/"no"
```

### Connection Refused

```bash
# Check if Redis is running
redis-cli ping

# Verify connection settings
echo $REDIS_URL
# or
echo $REDIS_HOST $REDIS_PORT
```

### TLS/SSL Errors

```bash
# Enable SSL
export REDIS_SSL=true

# Use rediss:// URL scheme
export REDIS_URL=rediss://localhost:6380/0

# Disable hostname checking if needed (not recommended for production)
export REDIS_SSL_CHECK_HOSTNAME=false
```

