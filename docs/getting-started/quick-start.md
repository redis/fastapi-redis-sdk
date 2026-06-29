# Quick Start

## Create the app

Enable both features in one builder chain, then add them to endpoints as
dependencies:

```python
from fastapi import Depends, FastAPI
from redis_fastapi import FastAPIRedis, cache, rate_limit

app = FastAPI()

# Shared connection pool (lifespan) + caching and rate-limiting support
FastAPIRedis(app).lifespan().caching().rate_limiting()

# Caching - the GET response is stored in Redis for 60s
@app.get("/items", dependencies=[Depends(cache(ttl=60))])
async def get_items():
    return {"items": [1, 2, 3]}

# Rate limiting - at most 10 requests per minute per client IP
@app.get("/search", dependencies=[Depends(rate_limit("10/minute"))])
async def search():
    return {"results": ["a", "b", "c"]}
```

- **`FastAPIRedis(app)`** - builder that wires Redis into the app.
- **`.lifespan()`** - manages a shared async connection pool
  (startup → create pool, shutdown → close).  Wraps any existing
  lifespan, so it composes with other libraries.
- **`.caching()`** - registers the exception handler and capture
  middleware needed by the `cache()` / `cache_evict()` / `cache_put()`
  dependency factories.
- **`.rate_limiting()`** - registers the `429` handler and header
  middleware needed by the `rate_limit()` dependency.
- **`Depends(cache(ttl=60))`** - on cache hit the endpoint is skipped
  entirely; on miss the response is stored in Redis for subsequent requests.
- **`Depends(rate_limit("10/minute"))`** - the first 10 requests pass; the
  11th gets `429 Too Many Requests` with a `Retry-After` header. Counts are
  kept in Redis, so the limit holds across every worker and pod.

Read more in the [Caching](../guide/caching.md) and
[Rate Limiting](../guide/rate-limiting.md) guides.

## Configure Redis

Point the library at your Redis server with a `.env` file or
environment variables:

=== ".env file"

    ```dotenv
    REDIS_URL=redis://localhost:6379/0
    ```

=== "Environment variable"

    ```bash
    export REDIS_URL=redis://localhost:6379/0
    ```

Then start the app:

```bash
uvicorn myapp:app
```

See [Configuration](../guide/configuration.md) for all options.

