"""The planner: call the tools, then stream a report into the run's stream.

There are two planners with one signature, ``(req, w)``: the scripted one
here, and the model one in ``model.py``.  Redis sees no difference between
them.  ``run_planner`` holds the rules for falling back from one to the other.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass
from typing import Any

import httpx
from fastapi import FastAPI
from pydantic import BaseModel, Field

from examples.incident_triage_app.tools import Service
from redis_fastapi import StreamWriter

logger = logging.getLogger(__name__)

# 150 ms after each word-sized delta: a report of about 100 words takes 15 to
# 20 seconds, long enough to drop the connection or open the phone by hand.
# Both planners use it.
PACE = 0.15


class TriageRequest(BaseModel):
    run_id: str | None = Field(default=None, pattern=r"^[A-Za-z0-9_-]{8,64}$")
    service: Service = "checkout"
    prompt: str = Field(default="Why is checkout slow?", min_length=1, max_length=500)


class PlannerFailed(Exception):
    """A planner could not finish.  ``code`` goes into the ``stream.end`` data."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class Planner:
    name: str
    run: Callable[[TriageRequest, StreamWriter], Awaitable[None]]


# -- Tool calls -------------------------------------------------------------

# The planner calls the app's own tool endpoints in process, so the calls pass
# through the same middleware and dependencies as an outside caller's would.
_tools: httpx.AsyncClient | None = None


def connect(app: FastAPI) -> None:
    global _tools
    _tools = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://tools.internal"
    )


async def disconnect() -> None:
    if _tools is not None:
        await _tools.aclose()


async def call_tool(w: StreamWriter, name: str, **params: Any) -> dict[str, Any]:
    assert _tools is not None, "planner.connect(app) runs in the lifespan"
    while True:
        started = time.perf_counter()
        r = await _tools.get(f"/api/tools/{name}", params=params)
        if r.status_code != 429:
            r.raise_for_status()
            await w.append(
                "tool",
                {
                    "name": name,
                    "params": params,
                    "cache": r.headers.get("X-Redis-Cache", "MISS"),
                    "ms": round((time.perf_counter() - started) * 1000),
                },
            )
            result: dict[str, Any] = r.json()
            return result
        # The global rate limit covers these calls too: wait as told, and say so.
        wait = int(r.headers.get("Retry-After", "1"))
        await w.append("status", {"text": f"{name} is busy, retrying in {wait} s"})
        await asyncio.sleep(wait)


# -- The scripted planner ---------------------------------------------------


def report(req: TriageRequest, metrics: dict[str, Any], deploys: dict[str, Any]) -> str:
    """The seeded report, in the Markdown subset the page renders."""
    p50, p99 = metrics["p50_ms"], metrics["p99_ms"]
    recent = deploys["deploys"]
    if not recent or metrics.get("changed_at") is None:
        return "\n".join(
            [
                f"**No incident found** for {req.service} in the last hour.",
                f"- p99 latency is steady at about {p99['after']} ms, "
                f"and p50 at about {p50['after']} ms.",
                "- No deploy shipped in the last hour.",
                "**Next steps**",
                "1. Check the upstream services this one calls.",
                "2. Ask again if the alert keeps firing.",
            ]
        )
    deploy = recent[0]
    lines = [
        f"**Likely cause:** the {req.service} {deploy['version']} deploy at {deploy['at']}.",
        f"- p99 latency rose from {p99['before']} ms to {p99['after']:,} ms at "
        f"{metrics['changed_at']}. p50 moved from {p50['before']} to {p50['after']} ms, "
        "so a slow minority of requests causes it.",
        f"- {deploy['version']} changed this: {deploy['change']}.",
    ]
    if "payment_timeouts" in metrics:
        timeouts = metrics["payment_timeouts"]
        lines.append(
            f"- Payment timeouts rose from {timeouts['before']} to {timeouts['after']:,} "
            f"since {metrics['changed_at']}, and there were none before the deploy."
        )
    if "error_rate_pct" in metrics:
        errors = metrics["error_rate_pct"]
        lines.append(
            f"- The error rate rose from {errors['before']}% to {errors['after']}% "
            f"since {metrics['changed_at']}."
        )
    lines += [
        "**Next steps**",
        f"1. Roll back {req.service} to {deploy['previous']}.",
        f"2. Watch p99 for 10 minutes. It should drop under {p99['before'] + 100} ms.",
        "3. Fix the change before it ships again, and add a test for it.",
    ]
    return "\n".join(lines)


def chunks(text: str) -> Iterator[str]:
    """Split the report into word-sized deltas, keeping the whitespace."""
    word = ""
    for ch in text:
        if ch.isspace() and word and not word[-1].isspace():
            yield word
            word = ""
        word += ch
    if word:
        yield word


async def triage_scripted(req: TriageRequest, w: StreamWriter) -> None:
    await w.append("status", {"text": "Checking the metrics and the deploys"})
    metrics = await call_tool(w, "metrics", service=req.service)
    deploys = await call_tool(w, "deploys", service=req.service)
    for chunk in chunks(report(req, metrics, deploys)):
        await w.append("delta", {"text": chunk})
        await asyncio.sleep(PACE)
    # spawn() writes the final "stream.end" entry when this returns


SCRIPTED = Planner("scripted", triage_scripted)


# -- Choosing and falling back ----------------------------------------------


class _CountingWriter:
    """Passes appends through and counts the deltas, for the fallback rule."""

    def __init__(self, w: StreamWriter) -> None:
        self._w = w
        self.stream_id = w.stream_id
        self.deltas = 0

    async def append(self, type: str, data: Any) -> str:
        if type == "delta":
            self.deltas += 1
        return await self._w.append(type, data)


async def run_planner(
    planner: Planner, req: TriageRequest, w: StreamWriter, note: str | None
) -> None:
    """Run *planner*, falling back to the script if the model fails early.

    Before the first delta, a model failure hands the run to the scripted
    planner, so the engineer still gets a report.  After it, the run fails
    with ``model_error``: two planners' text in one answer would confuse.
    """
    if note:
        await w.append("status", {"text": note})
    if planner is SCRIPTED:
        await triage_scripted(req, w)
        return
    counting = _CountingWriter(w)
    try:
        await planner.run(req, counting)  # type: ignore[arg-type]
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        if counting.deltas:
            logger.warning("model planner failed mid-answer: %s", exc)
            raise PlannerFailed("model_error") from exc
        logger.warning("model planner failed before any text: %s", exc)
        await w.append(
            "status", {"text": "The model is unavailable; using the scripted planner"}
        )
        await triage_scripted(req, w)
