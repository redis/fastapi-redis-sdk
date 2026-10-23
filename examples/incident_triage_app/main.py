"""The incident triage demo: an agent that streams its answer through Redis.

One app shows the SDK's four features in one story (see
``specs/resumable-streams-demo.md``): sessions, caching, rate limiting and
resumable streams.  Resumable streams are not in the SDK yet, so
``streams_stub.py`` stands in for them with the designed API.

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

from fastapi import FastAPI
from starlette.requests import Request

from examples.incident_triage_app import config, lens, planner, runs, session, tools
from examples.incident_triage_app.devices import DeviceCookies
from examples.incident_triage_app.streams_stub import install as install_streams
from redis_fastapi import FastAPIRedis
from redis_fastapi.deps import _get_pool_state


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    planner.connect(app)
    # What .streams() will do at startup: detect support once per pool.  The
    # stub reaches the pool through the SDK's internal accessor, as the real
    # feature will from inside the SDK.
    await app.state.streams.detect(_get_pool_state(app).get_async_client())
    try:
        yield
    finally:
        await app.state.streams.shutdown()  # stream.end entries, so readers end
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
)
install_streams(app, config.STREAMS)
app.add_middleware(DeviceCookies)  # outermost: before the session middleware

app.include_router(session.router)
app.include_router(tools.router)
app.include_router(runs.router)
app.include_router(lens.router)
app.frontend("/", directory=str(Path(__file__).parent / "web"))
