"""Which features this Redis server supports, checked once and cached.

An old or unreachable Redis turns features off one by one, with a reason, so
the app degrades instead of crashing (spec, section 8.3).
"""

from __future__ import annotations

import time
from typing import Any

from fastapi import Depends, HTTPException, Request
from redis.exceptions import RedisError

from examples.incident_triage_app import config
from redis_fastapi import AsyncRedisDep, StreamBackend, StreamBackendDep

_RECHECK_SECONDS = 30


def _version(text: str) -> tuple[int, ...]:
    return tuple(int(part) for part in text.split(".")[:3] if part.isdigit())


def _feature(
    version: tuple[int, ...] | None, needed: tuple[int, int, int]
) -> dict[str, Any]:
    wanted = ".".join(map(str, needed))
    if version is None:
        return {"available": False, "reason": "Redis is unreachable"}
    if version < needed:
        found = ".".join(map(str, version))
        return {"available": False, "reason": f"needs Redis {wanted}, found {found}"}
    return {"available": True}


async def _streams_feature(
    streams: StreamBackend, version: tuple[int, ...] | None
) -> dict[str, Any]:
    """Streams detect their own support, as the SDK's other features do.

    The SDK probes ``COMMAND INFO XCFGSET DELEX`` at startup and caches the
    answer; an unknown answer (Redis was down at startup) is asked again here.
    """
    if version is None:
        return {"available": False, "reason": "Redis is unreachable"}
    supported = await streams.detect()
    if supported is None:
        return {"available": False, "reason": "Redis is unreachable"}
    if not supported:
        return {"available": False, "reason": "needs Redis 8.6.2 or later"}
    return {"available": True}


async def features(
    request: Request, redis: AsyncRedisDep, streams: StreamBackendDep
) -> dict[str, Any]:
    """Read the server version once, and again every 30 s while it fails."""
    state = request.app.state
    cached = getattr(state, "health", None)
    if cached and (cached["redis"]["reachable"] or time.monotonic() < state.health_at):
        return dict(cached)
    try:
        info = await redis.info("server")
        version: tuple[int, ...] | None = _version(str(info["redis_version"]))
        redis_part = {"reachable": True, "version": info["redis_version"]}
    except (RedisError, OSError) as exc:
        version = None
        redis_part = {"reachable": False, "error": type(exc).__name__}
    health = {
        "redis": redis_part,
        "features": {
            "sessions": _feature(version, config.SESSIONS_MIN_VERSION),
            "caching": _feature(version, (6, 0, 0)),
            "rate_limiting": _feature(version, (6, 0, 0)),
            "streams": await _streams_feature(streams, version),
        },
    }
    state.health, state.health_at = health, time.monotonic() + _RECHECK_SECONDS
    return dict(health)


def _require(feature: str, what: str) -> Any:
    async def dependency(health: dict[str, Any] = Depends(features)) -> None:
        state = health["features"][feature]
        if not state["available"]:
            raise HTTPException(503, f"{what} is off: {state['reason']}")

    return dependency


require_streams = _require("streams", "Chat")
require_sessions = _require("sessions", "Sign-in")
