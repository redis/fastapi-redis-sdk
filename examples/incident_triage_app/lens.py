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
from examples.incident_triage_app.tools import SERVICES
from redis_fastapi import (
    AsyncRedisDep,
    RateLimitBackendDep,
    SessionStoreDep,
    StreamBackendDep,
    get_settings,
)
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


def _text(value: Any) -> Any:
    return value.decode() if isinstance(value, bytes) else value


async def _stream_keys(redis: Any, scope: str, stream_id: str) -> list[dict[str, Any]]:
    """The run's Redis key, read as it is, under the SDK's key layout.

    The layout is documented in the streams guide: one stream per run, named
    from the SDK's prefix setting, the scope and the run ID.
    """
    stream = f"{get_settings().prefix}:stream:{scope}:{stream_id}"
    if not await redis.exists(stream):
        return []
    last = await redis.xrevrange(stream, count=1)
    return [
        _key(
            stream,
            "Stream",
            {
                "XLEN": await redis.xlen(stream),
                "last ID": _text(last[0][0]) if last else None,
                "TTL": await redis.ttl(stream),
            },
        )
    ]


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

    # The run: the stream ID the page follows, or the engineer's current one,
    # which this app keeps itself (runs.py).
    from examples.incident_triage_app.runs import _current, _scope

    current = _current(subject)
    current_id = _text(await redis.get(current))
    if current_id is not None:
        keys.append(
            _key(
                current,
                "String",
                {"value": current_id, "TTL": await redis.ttl(current)},
            )
        )
    if streams.supported is not False:  # streams off: show the other keys
        stream_id = stream_id or current_id
        if stream_id:
            keys.extend(await _stream_keys(redis, _scope(subject), stream_id))

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
