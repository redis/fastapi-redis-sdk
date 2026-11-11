"""Start, find, follow and cancel a triage run.

An engineer runs one triage at a time, and a second device finds it.  The SDK
keeps only the run's own stream, so this app keeps the pointer to each
engineer's current run itself: one key, claimed when a run starts and freed
when it ends.  ``StreamBackend.finished()`` tells a stale claim from a live
one.
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
    StreamBackend,
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


def _current(subject: str) -> str:
    """The key naming the engineer's current run."""
    return f"{get_settings().prefix}:triage-current:{subject}"


async def _run_over(streams: StreamBackend, subject: str, run_id: str) -> bool:
    """Whether the run no longer holds the engineer's slot: ended, or gone."""
    return await streams.finished(run_id, scope=_scope(subject)) is not False


async def _claim(
    redis: Any, streams: StreamBackend, subject: str, run_id: str
) -> None:
    """Make *run_id* the engineer's current run, or answer 409.

    Called after the run's open(), so a claim always names a run that
    exists.  A retried start finds its own claim.  A claim whose run ended
    without freeing it - its process died - is taken over.
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
    holder = text(holder)
    if holder == run_id:
        return
    if holder is not None and await _run_over(streams, subject, holder):
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
    # Open first, then claim: finished() can then tell a stale claim's run
    # from this one.  A retried start finds the same run with created=False:
    # no second producer, no second claim, no second budget slot.  A second
    # run while one goes on is cancelled and gets 409, before any budget is
    # spent.
    writer = await streams.open(scope=_scope(subject), stream_id=body.run_id)
    if not writer.created:
        return {"stream_id": writer.stream_id}
    try:
        await _claim(redis, streams, subject, body.run_id)
    except HTTPException:
        await streams.cancel(writer.stream_id, scope=_scope(subject))
        raise
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
    redis: AsyncRedisDep, streams: StreamBackendDep, subject: str = Depends(subject_of)
) -> dict[str, str | None]:
    current = text(await redis.get(_current(subject)))
    if current is None or await _run_over(streams, subject, current):
        return {"stream_id": None}
    return {"stream_id": current}


def text(value: Any) -> str | None:
    """A reply value as text; the SDK's pool returns bytes."""
    if value is None:
        return None
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
