"""Observability demo app — showcases all three OpenTelemetry layers.

Run via ``docker compose up --build`` from this directory (see README.md).
The app is instrumented with ``opentelemetry-instrument`` (Layer 1 — HTTP
spans + SDK/OTLP export), ``.otel()`` (Layer 2 — cache & rate-limit spans and
metrics), and ``REDIS_OTEL_REDIS_ENABLED=true`` (Layer 3 — Redis command spans).

Every request produces one nested trace across the three layers, plus the
``redis_fastapi.cache.*`` and ``redis_fastapi.ratelimit.*`` metrics.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

# Make redis_fastapi importable straight from the repo source, without
# installing the package (mirrors examples/main.py).
_src = str(Path(__file__).resolve().parents[2] / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from fastapi import Depends, FastAPI

from redis_fastapi import FastAPIRedis, cache, rate_limit

app = FastAPI(
    title="fastapi-redis-sdk — observability demo",
    description="Emits OTel traces + metrics across all three instrumentation layers.",
)

# Layer 2 is turned on by .otel(); Layer 1 (FastAPI HTTP spans) and Layer 3
# (Redis commands, via REDIS_OTEL_REDIS_ENABLED) are wired in the environment.
FastAPIRedis(app).lifespan().caching().rate_limiting().otel()


@app.get("/")
async def root() -> dict[str, str]:
    """Health check — no Redis, so no cache/rate-limit spans."""
    return {"status": "ok"}


@app.get(
    "/products/{product_id}",
    dependencies=[Depends(cache(ttl=30, eviction_group="products"))],
)
async def get_product(product_id: int) -> dict[str, object]:
    """Cached for 30s.

    First call → ``cache.get`` (miss) then ``cache.set``; subsequent calls →
    ``cache.get`` (hit) and the endpoint body is skipped.  Watch the
    ``cache.hit`` span attribute and the ``redis_fastapi.cache.requests`` metric.
    """
    return {
        "id": product_id,
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
    }


@app.get(
    "/limited",
    dependencies=[Depends(rate_limit("5/10seconds", scope="demo"))],
)
async def limited() -> dict[str, str]:
    """5 requests / 10s per client.

    Each call emits a ``ratelimit.hit`` span carrying ``ratelimit.backend``
    (``increx`` on Redis 8.8+, otherwise ``lua``); the 6th within the window
    returns 429 and increments ``redis_fastapi.ratelimit.requests`` with
    ``result="limited"``.
    """
    return {"status": "ok"}
