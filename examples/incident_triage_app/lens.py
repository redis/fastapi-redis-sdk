"""The Redis lens and the health check.

``GET /api/lens`` shows the Redis keys behind the caller's own state, and
nothing else.  ``GET /api/health`` reports each feature as available or not,
with a reason, so an old Redis disables the chat instead of crashing the app.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, Request

from examples.incident_triage_app import config
from examples.incident_triage_app.health import features
from examples.incident_triage_app.session import subject_of
from examples.incident_triage_app.streams_stub import StreamBackendDep
from examples.incident_triage_app.tools import SERVICES
from redis_fastapi import AsyncRedisDep, RateLimitBackendDep, SessionStoreDep
from redis_fastapi.sessions import session_state_of

router = APIRouter(prefix="/api", tags=["lens"])

# Written by deploy/bundle.py: the commit a deployed app runs.  Absent locally.
_BUILD = Path(__file__).with_name("build.json")
BUILD = json.loads(_BUILD.read_text()) if _BUILD.exists() else None


@router.get("/health")
async def health(health: dict[str, Any] = Depends(features)) -> dict[str, Any]:
    from examples.incident_triage_app.model import MODEL, model_configured

    return {
        **health,
        "planner": f"model · {MODEL}" if model_configured() else "scripted",
        "services": SERVICES,
        "build": BUILD,
    }


def _key(name: str, type: str, fields: dict[str, Any] | None) -> dict[str, Any]:
    return {"name": name, "type": type, "fields": fields}


@router.get("/lens")
async def lens(
    request: Request,
    streams: StreamBackendDep,
    store: SessionStoreDep,
    redis: AsyncRedisDep,
    limits: RateLimitBackendDep,
    subject: str = Depends(subject_of),
    stream_id: str | None = None,
) -> dict[str, Any]:
    keys: list[dict[str, Any]] = []

    # The run: the stream ID the page follows, or the active one.
    scope = f"triage:{subject}"
    view = None
    if streams.supported is not False:  # streams off: show the other keys
        stream_id = stream_id or await streams.active(scope)
        view = streams.inspect(stream_id, scope=scope) if stream_id else None
    for name, fields in (view or {}).items():
        type = (
            "Stream" if ":stream:" in name else "Hash" if "-meta:" in name else "String"
        )
        keys.append(_key(name, type, fields))

    # The session: the key's TTL is the absolute clock, field d's the idle one.
    session_id = session_state_of(request).session_id
    session_key = getattr(store, "session_key", None)
    if session_id and session_key is not None:
        name = session_key(session_id)
        absolute = await redis.ttl(name)
        idle = await redis.execute_command("HTTL", name, "FIELDS", 1, "d")
        keys.append(
            _key(
                name,
                "Hash",
                {
                    "idle TTL": f"{idle[0]} s of {config.SESSION_IDLE_TTL}",
                    "absolute TTL": f"{absolute:,} s of {config.SESSION_ABSOLUTE_TTL:,}",
                },
            )
        )

    # The two counters this engineer can see.
    quota = await limits.peek(
        subject,
        limit=config.RUN_LIMIT,
        window=config.RUN_WINDOW,
        scope=config.RUN_SCOPE,
    )
    budget = await limits.peek(
        "all",
        limit=config.MODEL_LIMIT,
        window=config.MODEL_WINDOW,
        scope=config.MODEL_SCOPE,
    )
    meters = [
        {
            "label": f"Runs · {subject}",
            "used": quota.limit - quota.remaining,
            "limit": quota.limit,
            "ttl": quota.reset_after if quota.remaining < quota.limit else None,
        },
        {
            "label": "Model runs today · everyone",
            "used": budget.limit - budget.remaining,
            "limit": budget.limit,
            "ttl": budget.reset_after if budget.remaining < budget.limit else None,
        },
    ]
    return {"subject": subject, "stream_id": stream_id, "keys": keys, "meters": meters}
