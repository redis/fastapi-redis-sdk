"""Start, find, follow and cancel a triage run."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request, Response

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
    RateLimitBackendDep,
    StreamBackendDep,
    rate_limit,
    session_expiry,
)

router = APIRouter(prefix="/api", tags=["runs"])

run_quota = rate_limit(
    limit=RUN_LIMIT,
    window=RUN_WINDOW,
    scope=RUN_SCOPE,
    identifier=by_subject,
    ietf_headers=True,
)


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
    subject: str = Depends(subject_of),
) -> dict[str, str]:
    # open() first: a busy scope raises StreamBusy (409) before any budget is
    # spent.  A retried start finds the same run and returns created=False:
    # no second producer, and no second budget slot.
    writer = await streams.open(scope=f"triage:{subject}", stream_id=body.run_id)
    if not writer.created:
        return {"stream_id": writer.stream_id}
    planner = SCRIPTED
    note = None
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
    streams.spawn(writer, lambda w: run_planner(planner, body, w, note))
    return {"stream_id": writer.stream_id, "planner": planner.name}


@router.get("/triage/runs/active")
async def active(
    streams: StreamBackendDep, subject: str = Depends(subject_of)
) -> dict[str, str | None]:
    return {"stream_id": await streams.active(f"triage:{subject}")}


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
        scope=f"triage:{subject}",
        expires_at=session_expiry(request),  # sse() reads the cursor itself
    )


@router.post("/streams/{stream_id}/cancel", status_code=204)
async def cancel(
    stream_id: str, streams: StreamBackendDep, subject: str = Depends(subject_of)
) -> Response:
    await streams.cancel(stream_id, scope=f"triage:{subject}")
    return Response(status_code=204)
