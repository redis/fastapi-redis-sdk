"""Start, find, follow and cancel a triage run.

An engineer runs one triage at a time, and a second device finds it.  The SDK
keeps only the run's own stream, so this app keeps the pointer to each
engineer's current run itself: one key, claimed when a run starts and freed
when it ends.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from examples.incident_triage_app.config import (
    MODEL_LIMIT,
    MODEL_SCOPE,
    MODEL_WINDOW,
    RUN_LIMIT,
    RUN_SCOPE,
    RUN_WINDOW,
)
from examples.incident_triage_app.health import require_streams
from examples.incident_triage_app.model import MODEL_PLANNER, model_configured
from examples.incident_triage_app.planner import (
    SCRIPTED,
    TriageRequest,
    run_planner,
)
from examples.incident_triage_app.session import by_subject, subject_of
from redis_fastapi import (
    AsyncRedisDep,
    RateLimitBackendDep,
    StreamBackendDep,
    StreamWriter,
    get_settings,
    rate_limit,
    session_expiry,
)

router = APIRouter(prefix="/api", tags=["runs"])
logger = logging.getLogger(__name__)

run_quota = rate_limit(
    limit=RUN_LIMIT,
    window=RUN_WINDOW,
    scope=RUN_SCOPE,
    identifier=by_subject,
    ietf_headers=True,
)


def _scope(subject: str) -> str:
    return f"triage:{subject}"


# How long a claim may wait for its run to open before a missing stream
# means the run is gone.
_OPEN_GRACE = 10


def _current(subject: str) -> str:
    """The key naming the engineer's current run."""
    return f"{get_settings().prefix}:triage-current:{subject}"


async def _run_over(redis: Any, subject: str, run_id: str) -> bool:
    """Whether a run is gone, or has its final entry: its claim is stale.

    A few entries can follow the final one - a late ping, or a producer that
    has not noticed a cancel - so the newest few are checked, not the last.
    """
    stream = f"{get_settings().prefix}:stream:{_scope(subject)}:{run_id}"
    recent = await redis.xrevrange(stream, count=20)
    if not recent:
        # A claim is taken a moment before its run opens: while it is fresh,
        # a missing stream is a run about to start, not a finished one.
        claimed_for = get_settings().stream_max_run - await redis.ttl(_current(subject))
        return claimed_for > _OPEN_GRACE
    return any(
        _text(name) == "f" and _text(value) == "1"
        for _, fields in recent
        for name, value in fields.items()
    )


async def _claim(redis: Any, subject: str, run_id: str) -> None:
    """Make *run_id* the engineer's current run, or answer 409.

    A retried start finds its own claim.  A claim whose run ended without
    freeing it - its process died - is taken over.
    """
    key = _current(subject)
    lifetime = get_settings().stream_max_run
    if await redis.set(key, run_id, nx=True, ex=lifetime):
        return
    holder = await redis.get(key)
    if holder is None:  # released a moment ago: try once more
        if await redis.set(key, run_id, nx=True, ex=lifetime):
            return
        holder = await redis.get(key)
    holder = _text(holder) if holder is not None else None
    if holder == run_id:
        return
    if holder is not None and await _run_over(redis, subject, holder):
        # Only while the stale claim still holds: a concurrent start wins once.
        swapped = await redis.execute_command(
            "SET", key, run_id, "IFEQ", holder, "EX", lifetime
        )
        if swapped:
            return
    raise HTTPException(409, "A triage run is already active")


async def _release(redis: Any, subject: str, run_id: str) -> None:
    await redis.execute_command("DELEX", _current(subject), "IFEQ", run_id)


@router.post(
    "/triage/runs",
    status_code=202,
    # In this order: a signed-out caller gets 401 before the quota counts it.
    dependencies=[Depends(subject_of), Depends(require_streams), Depends(run_quota)],
)
async def start(
    body: TriageRequest,
    streams: StreamBackendDep,
    limits: RateLimitBackendDep,
    redis: AsyncRedisDep,
    subject: str = Depends(subject_of),
) -> dict[str, str]:
    # The claim first: a second run gets 409 before any budget is spent.  A
    # retried start finds its own claim, then the same run with
    # created=False: no second producer, and no second budget slot.
    await _claim(redis, subject, body.run_id)
    writer = await streams.open(scope=_scope(subject), stream_id=body.run_id)
    if not writer.created:
        return {"stream_id": writer.stream_id}
    planner = SCRIPTED
    note = None
    try:
        if model_configured():
            budget = await limits.hit(
                "all",
                limit=MODEL_LIMIT,
                window=MODEL_WINDOW,
                scope=MODEL_SCOPE,
                fail_closed=True,
            )
            if budget.allowed:
                planner = MODEL_PLANNER
            else:
                note = f"Daily model budget spent; back in {budget.retry_after} s"
    except BaseException:
        # No producer will run: end the run and free the engineer.  A Redis
        # failure that got us here may fail the cancel too; the claim is
        # released whatever happens, and the first error is the one raised.
        try:
            await streams.cancel(writer.stream_id, scope=_scope(subject))
        except Exception:
            logger.warning("Could not end run %s", writer.stream_id, exc_info=True)
        finally:
            await _release(redis, subject, body.run_id)
        raise

    async def produce(w: StreamWriter) -> None:
        try:
            await run_planner(planner, body, w, note)
        finally:
            await _release(redis, subject, body.run_id)

    streams.spawn(writer, produce)
    return {"stream_id": writer.stream_id, "planner": planner.name}


@router.get("/triage/runs/active")
async def active(
    redis: AsyncRedisDep, subject: str = Depends(subject_of)
) -> dict[str, str | None]:
    current = await redis.get(_current(subject))
    if current is None or await _run_over(redis, subject, _text(current)):
        return {"stream_id": None}
    return {"stream_id": _text(current)}


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


@router.get("/streams/{stream_id}/events")
async def events(
    stream_id: str,
    request: Request,
    streams: StreamBackendDep,
    subject: str = Depends(subject_of),
) -> Response:
    # Never behind cache(): the stream is private to one engineer.  subject_of
    # is the access check.  The stream's name is the scope plus the ID, and the
    # scope comes from the session, so another engineer's run ID names no
    # stream here and gets a 404.  The response ends when the session does,
    # and the reconnect runs subject_of again.
    return await streams.sse(
        stream_id,
        request=request,
        scope=_scope(subject),
        expires_at=session_expiry(request),  # sse() reads the cursor itself
    )


@router.post("/streams/{stream_id}/cancel", status_code=204)
async def cancel(
    stream_id: str,
    streams: StreamBackendDep,
    redis: AsyncRedisDep,
    subject: str = Depends(subject_of),
) -> Response:
    await streams.cancel(stream_id, scope=_scope(subject))
    await _release(redis, subject, stream_id)
    return Response(status_code=204)
