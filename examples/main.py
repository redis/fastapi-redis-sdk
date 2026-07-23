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

import anyio
from fastapi import Depends, FastAPI, WebSocket

from redis_fastapi import (
    AsyncRedisDep,
    CacheBackendDep,
    FastAPIRedis,
    PubSubManager,
    PubSubManagerDep,
    cache,
    cache_evict,
    default_key_builder,
    get_settings,
)
from redis_fastapi.config import RedisSettings
from redis_fastapi.deps import _get_pool_state

app = FastAPI(
    title="fastapi-redis-sdk demo",
    description="Minimal app showcasing fastapi-redis-sdk on FastAPI Cloud.",
)
FastAPIRedis(app).lifespan().caching()


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


@app.post("/notify")
async def notify(message: str, pubsub: PubSubManagerDep) -> dict[str, int]:
    """Publish a message to the ``notifications`` channel."""
    count = await pubsub.publish("notifications", message)
    return {"subscribers": count}


@app.websocket("/ws/chat/{room}")
async def chat(websocket: WebSocket, room: str) -> None:
    """WebSocket chat room backed by Redis Pub/Sub.

    Connect: ``ws://localhost:8000/ws/chat/lobby``
    """
    await websocket.accept()
    redis = _get_pool_state(websocket.app).get_async_client()
    pubsub = PubSubManager(redis)
    channel = f"chat:{room}"
    await pubsub.subscribe(channel)

    async def _send() -> None:
        async for message in pubsub.listen():
            await websocket.send_text(message["data"].decode())

    async def _receive() -> None:
        async for data in websocket.iter_text():
            await pubsub.publish(channel, data)

    try:
        async with anyio.create_task_group() as tg:
            tg.start_soon(_send)
            tg.start_soon(_receive)
    except Exception:
        pass
    finally:
        await pubsub.close()
