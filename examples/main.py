"""The deployable demo: the catalogue and the public API in one app.

``pyproject.toml`` points FastAPI Cloud at ``examples.main:app``, so this file
stays small: it adds a health check and a Redis ``PING``, and includes the
routers of the two use cases.  Each use case also runs on its own - see
``examples/caching/catalogue.py`` and ``examples/rate_limiting/public_api.py``
for what they do and how to try them.

Run locally::

    export REDIS_URL=redis://localhost:6379/0
    uv run --with "fastapi[standard]" fastapi dev examples/main.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure redis_fastapi is importable even when the package isn't installed
# (e.g. FastAPI Cloud deploys source directly without `pip install -e .`).
_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from fastapi import FastAPI

from examples.caching.catalogue import router as catalogue
from examples.rate_limiting.public_api import router as public_api
from redis_fastapi import AsyncRedisDep, FastAPIRedis

app = FastAPI(
    title="fastapi-redis-sdk demo",
    description="A cached product catalogue and a rate-limited public API.",
)
FastAPIRedis(app).lifespan().caching().rate_limiting()
app.include_router(catalogue)
app.include_router(public_api)


@app.get("/")
async def root() -> dict[str, str]:
    """Health check - no Redis needed."""
    return {"status": "ok", "library": "fastapi-redis-sdk"}


@app.get("/ping")
async def ping(redis: AsyncRedisDep) -> dict[str, str]:
    """PING the connected Redis server."""
    pong: str = await redis.ping()  # type: ignore[assignment]
    return {"ping": str(pong)}
