"""The incident triage demo: an agent that streams its answer through Redis.

One app shows the SDK's four features in one story (see
``specs/resumable-streams-demo.md``): sessions, caching, rate limiting and
resumable streams.

Run locally (Redis 8.6.2 or later)::

    docker compose -f examples/incident_triage_app/compose.yaml up -d redis
    export REDIS_URL=redis://localhost:6379/0 REDIS_SESSION_COOKIE_HTTPS_ONLY=false
    uv run --with "fastapi[standard]" --with anthropic \\
        fastapi dev examples/incident_triage_app/main.py

Then open http://localhost:8000.  Without ``ANTHROPIC_API_KEY`` the scripted
planner answers.
"""

from __future__ import annotations

import os
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

# Ensure redis_fastapi and the examples package are importable when the
# package isn't installed (e.g. FastAPI Cloud deploys the source directly).
_root = Path(__file__).resolve().parents[2]
for _path in (str(_root / "src"), str(_root)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

# A pool of 20 fits the free database's 30 connections (spec, section 8.2).
os.environ.setdefault("REDIS_MAX_CONNECTIONS", "20")

# The demo's stream clocks and limits.  Set before anything reads the
# settings: tools.py builds its cache() dependencies at import time, and the
# settings are read once and cached.  A short heartbeat makes a lost producer
# show within seconds; 8 readers fit the free database; a 60-second
# connection cap makes a revoked phone lose its stream within a minute even
# without the page's poll.
for _name, _value in {
    "REDIS_STREAM_TTL_ACTIVE": "600",
    "REDIS_STREAM_TTL_DONE": "120",
    "REDIS_STREAM_MAXLEN": "500",
    "REDIS_STREAM_HEARTBEAT_SECONDS": "5",
    "REDIS_STREAM_ALIVE_TTL_SECONDS": "15",
    "REDIS_STREAM_MAX_READERS": "8",
    "REDIS_STREAM_MAX_CONNECTION_SECONDS": "60",
}.items():
    os.environ.setdefault(_name, _value)

from fastapi import FastAPI
from starlette.requests import Request

from examples.incident_triage_app import config, lens, planner, runs, session, tools
from examples.incident_triage_app.devices import DeviceCookies
from redis_fastapi import FastAPIRedis


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # The SDK's lifespan wraps this one: it detects stream support before
    # this runs, and ends spawned producers (stream.end, reason cancelled)
    # after it returns, before the pool closes.
    planner.connect(app)
    try:
        yield
    finally:
        await planner.disconnect()


def _everyone(request: Request) -> str:
    return "all"


def _no_session(request: Request) -> bool:
    """Static files, the health check and the shared tools need no session."""
    path = request.url.path
    return (
        not path.startswith("/api/")
        or path.startswith("/api/tools/")
        or path == "/api/health"
    )


app = FastAPI(
    title="Incident triage demo",
    description="An on-call agent on fastapi-redis-sdk: sessions, caching, "
    "rate limiting and resumable streams.",
    lifespan=lifespan,
)
FastAPIRedis(app).lifespan().caching().rate_limiting(
    global_rate=config.GLOBAL_RATE, identifier=_everyone
).sessions(
    idle_ttl=config.SESSION_IDLE_TTL,
    absolute_ttl=config.SESSION_ABSOLUTE_TTL,
    skip=_no_session,
).streams()
app.add_middleware(DeviceCookies)  # outermost: before the session middleware

app.include_router(session.router)
app.include_router(tools.router)
app.include_router(runs.router)
app.include_router(lens.router)
app.frontend("/", directory=str(Path(__file__).parent / "web"))
