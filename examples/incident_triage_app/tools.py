"""Fake observability APIs: slow, seeded, and cached for everyone.

Each tool sleeps as a real metrics or deploy API would, then returns the same
seeded data every run.  ``cache(ttl=300)`` stores the answer per query string,
and the endpoints never read the session, so all engineers share one entry:
the second engineer to ask about checkout gets a ``HIT`` in a few
milliseconds (story step 5).
"""

from __future__ import annotations

import asyncio
from typing import Any, Literal

from fastapi import APIRouter, Depends

from redis_fastapi import cache

# The one list of services.  The tool ``enum``, the request model and the page
# all read it, so a model cannot ask about a service the tools do not know.
SERVICES: dict[str, dict[str, Any]] = {
    "checkout": {
        "alert": "p99 latency above 2 s",
        "severity": "critical",
        "since": "14:05",
    },
    "search": {"alert": "error rate above 1%", "severity": "warning", "since": "13:48"},
    "accounts": {"alert": None, "severity": "ok", "since": None},
}
Service = Literal["checkout", "search", "accounts"]

TOOL_TTL = 300

_METRICS: dict[str, dict[str, Any]] = {
    "checkout": {
        "window": "last hour",
        "p50_ms": {"before": 120, "after": 160},
        "p99_ms": {"before": 400, "after": 2300},
        "changed_at": "14:03",
        "payment_timeouts": {"before": 0, "after": 1214},
    },
    "search": {
        "window": "last hour",
        "p50_ms": {"before": 45, "after": 48},
        "p99_ms": {"before": 310, "after": 330},
        "error_rate_pct": {"before": 0.2, "after": 1.4},
        "changed_at": "13:46",
    },
    "accounts": {
        "window": "last hour",
        "p50_ms": {"before": 30, "after": 31},
        "p99_ms": {"before": 180, "after": 176},
        "changed_at": None,
    },
}

_DEPLOYS: dict[str, list[dict[str, Any]]] = {
    "checkout": [
        {
            "at": "14:02",
            "version": "v2.31",
            "previous": "v2.30",
            "change": "payment client retries raised from 1 to 3 (PAYMENTS_MAX_RETRIES)",
        }
    ],
    "search": [
        {
            "at": "13:44",
            "version": "v8.4.0",
            "previous": "v8.3.2",
            "change": "query parser upgrade; stricter handling of quoted terms",
        }
    ],
    "accounts": [],
}

router = APIRouter(prefix="/api/tools", tags=["tools"])


@router.get("/metrics", dependencies=[Depends(cache(ttl=TOOL_TTL))])
async def metrics(service: Service) -> dict[str, Any]:
    """Latency and error metrics for *service* over the last hour."""
    await asyncio.sleep(1.5)
    return {"service": service, **_METRICS[service]}


@router.get("/deploys", dependencies=[Depends(cache(ttl=TOOL_TTL))])
async def deploys(service: Service) -> dict[str, Any]:
    """Deploys of *service* in the last hour."""
    await asyncio.sleep(0.8)
    return {"service": service, "window": "last hour", "deploys": _DEPLOYS[service]}
