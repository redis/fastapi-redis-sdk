"""The incident triage example's current-run claim, against real Redis.

The SDK keeps only each run's stream; the example keeps a pointer to each
engineer's current run.  These tests check that the claim refuses a second
run, takes over one that ended or never opened, and is not fooled by writes
after a run's end.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi import HTTPException

from examples.incident_triage_app.runs import _claim, _current, _release, _run_over
from redis_fastapi import StreamBackend
from tests.stream_support import collect

pytestmark = pytest.mark.integration

Case = tuple[StreamBackend, Any, str]
SUBJECT = "alex"
SCOPE = f"triage:{SUBJECT}"


async def test_a_retried_claim_finds_its_own(stream_case: Case) -> None:
    _, client, _ = stream_case
    await _claim(client, SUBJECT, "a")
    await _claim(client, SUBJECT, "a")
    assert await client.get(_current(SUBJECT)) == "a"


async def test_a_running_run_keeps_its_claim(stream_case: Case) -> None:
    backend, client, _ = stream_case
    await _claim(client, SUBJECT, "a")
    writer = await backend.open(scope=SCOPE, stream_id="a")
    await writer.append("delta", {})
    with pytest.raises(HTTPException) as refused:
        await _claim(client, SUBJECT, "b")
    assert refused.value.status_code == 409


async def test_a_fresh_claim_is_not_taken_before_its_run_opens(
    stream_case: Case,
) -> None:
    _, client, _ = stream_case
    await _claim(client, SUBJECT, "a")  # its open() has not run yet
    with pytest.raises(HTTPException):
        await _claim(client, SUBJECT, "b")


async def test_an_ended_run_hands_over_its_claim(stream_case: Case) -> None:
    backend, client, _ = stream_case
    await _claim(client, SUBJECT, "a")
    writer = await backend.open(scope=SCOPE, stream_id="a")
    await writer.finish("message", {})  # its process died before releasing
    await _claim(client, SUBJECT, "b")
    assert await client.get(_current(SUBJECT)) == "b"


async def test_writes_after_the_end_do_not_revive_a_run(stream_case: Case) -> None:
    backend, client, _ = stream_case
    writer = await backend.open(scope=SCOPE, stream_id="a")
    await backend.cancel("a", scope=SCOPE)
    for _ in range(30):  # a producer elsewhere that missed the cancel
        await writer.append("delta", {})
    assert await _run_over(client, SUBJECT, "a")
    assert (await collect(backend, "a", SCOPE))[-1].data == {"reason": "cancelled"}


async def test_release_frees_only_its_own_claim(stream_case: Case) -> None:
    _, client, _ = stream_case
    await _claim(client, SUBJECT, "a")
    await _release(client, SUBJECT, "b")
    assert await client.get(_current(SUBJECT)) == "a"
    await _release(client, SUBJECT, "a")
    assert await client.get(_current(SUBJECT)) is None
