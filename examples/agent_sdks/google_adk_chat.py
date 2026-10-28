"""A Google ADK agent whose answer survives a dropped connection.

The agent answers questions about service latency and calls one tool,
``latency``, to look the numbers up.  Each question is one run, streamed
through Redis, so a client that loses its connection reconnects with
``Last-Event-ID`` and gets the rest.  Each chat is one ADK session, so the
agent remembers earlier questions in the same chat.  The producer is the
Google ADK recipe from the streams guide.

* ``POST /chats/{chat_id}/runs/{run_id}`` starts a run with ``{"text": ...}``.
* ``GET /chats/{chat_id}/runs/{run_id}/events`` streams it as SSE: ``delta``,
  ``tool_call`` and ``tool_result`` events, then a final ``message``.
* ``DELETE /chats/{chat_id}/runs/{run_id}`` cancels it, and the model call or
  tool the agent waits on.

Needs Redis 8.6.2+ and a Gemini API key.  ``AGENT_MODEL`` picks the model.
The sessions live in memory, so they are lost on restart.

Run it::

    export REDIS_URL=redis://localhost:6379/0 GOOGLE_API_KEY=...
    uv run --with "fastapi[standard]" --with google-adk fastapi dev examples/agent_sdks/google_adk_chat.py

    curl -X POST localhost:8000/chats/c1/runs/r1 -H 'Content-Type: application/json' -d '{"text": "Why is checkout slow?"}'
    curl -N localhost:8000/chats/c1/runs/r1/events
    curl -N localhost:8000/chats/c1/runs/r1/events -H 'Last-Event-ID: <an id from above>'
"""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable

from fastapi import FastAPI, Request, Response
from google.adk.agents import LlmAgent
from google.adk.agents.run_config import RunConfig, StreamingMode
from google.adk.runners import InMemoryRunner
from google.genai import types
from pydantic import BaseModel

from redis_fastapi import FastAPIRedis, StreamBackendDep, StreamWriter

APP = "triage"
USER = "on-call"  # one user; take it from your authentication instead
P99_SECONDS = {"checkout": 2.3, "search": 0.4, "login": 0.2}


def latency(service: str) -> dict[str, object]:
    """Return a service's p99 latency."""
    return {"service": service, "p99_seconds": P99_SECONDS.get(service)}


runner = InMemoryRunner(
    agent=LlmAgent(
        name=APP,
        model=os.environ.get("AGENT_MODEL", "gemini-2.5-flash"),
        instruction="You help on-call engineers. Look up latency before you answer.",
        tools=[latency],
    ),
    app_name=APP,
)


def run_turn(
    prompt: str, *, user_id: str, session_id: str
) -> Callable[[StreamWriter], Awaitable[None]]:
    async def produce(writer: StreamWriter) -> None:
        message = types.Content(role="user", parts=[types.Part(text=prompt)])
        config = RunConfig(streaming_mode=StreamingMode.SSE)
        answer = ""
        async for event in runner.run_async(
            user_id=user_id,
            session_id=session_id,
            new_message=message,
            run_config=config,
        ):
            if event.partial:
                # A chunk of the answer.  The final, non-partial event repeats
                # the whole text, so only chunks become deltas.
                parts = event.content.parts if event.content else None
                for part in parts or []:
                    if part.text and not part.thought:
                        await writer.append("delta", {"text": part.text})
                continue
            for call in event.get_function_calls():
                await writer.append(
                    "tool_call", {"id": call.id, "name": call.name, "args": call.args}
                )
            for response in event.get_function_responses():
                await writer.append(
                    "tool_result", {"id": response.id, "output": response.response}
                )
            if event.is_final_response() and event.content and event.content.parts:
                answer = "".join(
                    part.text or "" for part in event.content.parts if not part.thought
                )
        await writer.finish("message", {"text": answer})

    return produce


app = FastAPI()
FastAPIRedis(app).lifespan().streams()


class Prompt(BaseModel):
    text: str


@app.post("/chats/{chat_id}/runs/{run_id}", status_code=202)
async def start(
    chat_id: str, run_id: str, body: Prompt, streams: StreamBackendDep
) -> dict[str, str]:
    # The ADK session must exist before the turn starts.
    sessions = runner.session_service
    if not await sessions.get_session(app_name=APP, user_id=USER, session_id=chat_id):
        await sessions.create_session(app_name=APP, user_id=USER, session_id=chat_id)
    writer = await streams.start(
        run_turn(body.text, user_id=USER, session_id=chat_id),
        scope=f"chat:{chat_id}",
        stream_id=run_id,
    )
    return {"stream_id": writer.stream_id}


@app.get("/chats/{chat_id}/runs/{run_id}/events")
async def events(
    chat_id: str,
    run_id: str,
    request: Request,
    streams: StreamBackendDep,
    after: str | None = None,
) -> Response:
    return await streams.sse(
        run_id, request=request, scope=f"chat:{chat_id}", after=after
    )


@app.delete("/chats/{chat_id}/runs/{run_id}")
async def cancel(
    chat_id: str, run_id: str, streams: StreamBackendDep
) -> dict[str, bool]:
    return {"cancelled": await streams.cancel(run_id, scope=f"chat:{chat_id}")}
