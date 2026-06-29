"""Demo app for FastAPI Cloud deployment.

Run locally::

    fastapi dev examples/main.py

Deployed on FastAPI Cloud this app is discovered automatically via the
``[tool.fastapi]`` entrypoint in ``pyproject.toml``.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure redis_fastapi is importable even when the package isn't installed
# (e.g. FastAPI Cloud deploys source directly without `pip install -e .`).
_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from redis_fastapi import (
    AsyncRedisDep,
    CacheBackendDep,
    FastAPIRedis,
    RateLimitBackendDep,
    RateLimitResult,
    cache,
    cache_evict,
    default_key_builder,
    get_settings,
    rate_limit,
)
from redis_fastapi.config import RedisSettings

app = FastAPI(
    title="fastapi-redis-sdk demo",
    description="Minimal app showcasing fastapi-redis-sdk on FastAPI Cloud.",
)
# Caching + rate limiting.  A global limiter is enabled only when
# REDIS_RATE_LIMIT_DEFAULT_LIMIT > 0 (kept off here so the per-route demos are
# easy to observe).
FastAPIRedis(app).lifespan().caching().rate_limiting()


@app.get("/")
async def root() -> dict[str, str]:
    """Health check — no Redis needed."""
    return {"status": "ok", "library": "fastapi-redis-sdk"}


@app.get("/ping")
async def ping(redis: AsyncRedisDep) -> dict[str, str]:
    """PING the connected Redis server."""
    pong: str = await redis.ping()  # type: ignore[assignment]
    return {"ping": str(pong)}


@app.get("/config")
async def show_config(
    settings: Annotated[RedisSettings, Depends(get_settings)],
) -> dict[str, str | int | bool]:
    """Return non-sensitive connection settings."""
    return {
        "host": settings.host,
        "port": settings.port,
        "db": settings.db,
        "cluster": settings.cluster,
        "prefix": settings.prefix,
        "default_ttl": settings.default_ttl,
    }


@app.get(
    "/cache-demo",
    dependencies=[Depends(cache(ttl=30, eviction_group="demo"))],
)
async def cache_demo() -> dict[str, str]:
    """Response is cached for 30 seconds — check ``X-Redis-Cache`` header."""
    from datetime import datetime, timezone

    return {"generated_at": datetime.now(tz=timezone.utc).isoformat()}


@app.delete(
    "/cache-demo",
    dependencies=[
        Depends(cache_evict(eviction_group="demo", key_builder=default_key_builder))
    ],
)
async def evict_cache_demo() -> dict[str, str]:
    """Evict the ``/cache-demo`` entry."""
    return {"evicted": "demo"}


@app.get("/items/{item_id}")
async def get_item(item_id: int, cache: CacheBackendDep) -> dict[str, object]:
    """Conditional caching: only cache items with status ``published``.

    Unlike ``cache()`` which always caches, this uses ``CacheBackend``
    to decide at runtime whether the result is worth caching.

    - ``published`` items are cached for 60 s.
    - ``draft`` items are never cached — always recomputed.
    """
    cached = await cache.get(f"item:{item_id}", eviction_group="items")
    if cached is not None:
        return {**cached, "source": "cache"}

    # Simulate a DB lookup — odd IDs are drafts, even IDs are published.
    from datetime import datetime, timezone

    status = "published" if item_id % 2 == 0 else "draft"
    item = {
        "id": item_id,
        "status": status,
        "fetched_at": datetime.now(tz=timezone.utc).isoformat(),
    }

    # Only cache published items
    if status == "published":
        await cache.set(f"item:{item_id}", item, ttl=60, eviction_group="items")

    return {**item, "source": "computed"}


@app.delete("/items/{item_id}")
async def delete_item(item_id: int, cache: CacheBackendDep) -> dict[str, object]:
    """Evict a single item from the cache."""
    deleted = await cache.delete(f"item:{item_id}", eviction_group="items")
    return {"id": item_id, "deleted": deleted}


# --- Rate limiting demos --------------------------------------------------


@app.get(
    "/limited",
    dependencies=[Depends(rate_limit("5/10seconds", scope="demo"))],
)
async def limited() -> dict[str, str]:
    """Allow 5 requests per 10 seconds.

    Watch the ``X-RateLimit-Remaining`` header count down; the 6th request
    within the window returns ``429`` with a ``Retry-After`` header.
    """
    return {"status": "ok"}


def _skip_internal(request: Request) -> bool:
    """Exempt internal calls (``X-Internal: 1``) from the limiter."""
    return request.headers.get("X-Internal") == "1"


def _slow_down(request: Request, result: RateLimitResult) -> JSONResponse:
    """Custom 429 body for the limited endpoint below."""
    return JSONResponse(
        {"error": "slow down", "retry_after": result.retry_after},
        status_code=429,
    )


@app.get(
    "/limited-custom",
    dependencies=[
        Depends(
            rate_limit(
                "3/minute",
                scope="custom",
                skip_when=_skip_internal,
                on_limit_exceeded=_slow_down,
            )
        )
    ],
)
async def limited_custom() -> dict[str, str]:
    """3 requests/minute, skips ``X-Internal`` calls, custom 429 body."""
    return {"status": "ok"}


@app.get("/limited-manual")
async def limited_manual(request: Request, limiter: RateLimitBackendDep) -> dict[str, object]:
    """Imperative rate limiting via ``RateLimitBackend`` for custom logic."""
    client = request.client.host if request.client else "unknown"
    result = await limiter.hit(client, limit=2, window=10, scope="manual")
    return {"allowed": result.allowed, "remaining": result.remaining}


# A single budget shared across two routes: an explicit ``scope`` names one
# bucket, so calls to /shared-a and /shared-b draw from ONE 3/10s counter.
# (Without a scope, each route defaults to its own route-template bucket.)
_shared_limit = rate_limit("3/10seconds", scope="shared-demo")


@app.get("/shared-a", dependencies=[Depends(_shared_limit)])
async def shared_a() -> dict[str, str]:
    """Shares its 3-requests/10s budget with ``/shared-b``."""
    return {"route": "a"}


@app.get("/shared-b", dependencies=[Depends(_shared_limit)])
async def shared_b() -> dict[str, str]:
    """Shares its 3-requests/10s budget with ``/shared-a``."""
    return {"route": "b"}


# --- Imperative recipes (RateLimitBackend) --------------------------------
# These show *why* the imperative backend exists: the key, the cost, and the
# decision to count are only known inside the handler.


class LoginBody(BaseModel):
    username: str
    password: str


async def _verify(username: str, password: str) -> bool:
    """Stand-in auth check (replace with your real user store)."""
    return password == "correct-horse"  # noqa: S105 - demo only


@app.post("/login")
async def login(body: LoginBody, limiter: RateLimitBackendDep) -> dict[str, str]:
    """Brute-force protection: count only *failed* logins, reset on success.

    The declarative dependency can't express this — it would increment on
    every request, before the handler knows whether auth succeeded.  Here we
    key on the submitted username (a request-body value), ``peek`` to enforce
    the lockout without consuming, ``hit`` only on failure, and ``reset`` on
    success.  Limit: 5 failures per 15 minutes.
    """
    key = f"login:{body.username}"
    if (await limiter.peek(key, limit=5, window=900)).remaining == 0:
        raise HTTPException(status_code=429, detail="account temporarily locked")

    if not await _verify(body.username, body.password):
        result = await limiter.hit(key, limit=5, window=900)  # count the failure
        raise HTTPException(
            status_code=401,
            detail=f"invalid credentials ({result.remaining} attempts left)",
        )

    await limiter.reset(key)  # clear the failure counter on success
    return {"status": "ok", "token": "demo-token"}


class CompletionBody(BaseModel):
    api_key: str
    prompt: str


@app.post("/complete")
async def complete(
    body: CompletionBody, limiter: RateLimitBackendDep
) -> dict[str, object]:
    """Token-metered quota: the cost is computed at runtime from the payload.

    ``rate_limit(cost=...)`` is fixed at decoration time; here each request
    consumes a variable number of units (a crude token estimate) against a
    per-API-key daily budget of 100 units.
    """
    cost = max(1, len(body.prompt) // 4)  # ~1 token per 4 chars (demo estimate)
    result = await limiter.hit(
        f"tokens:{body.api_key}", limit=100, window=86_400, cost=cost, scope="tokens"
    )
    if not result.allowed:
        raise HTTPException(
            status_code=429,
            detail=f"daily token budget exhausted; retry in {result.retry_after}s",
        )
    return {"charged": cost, "remaining": result.remaining}
