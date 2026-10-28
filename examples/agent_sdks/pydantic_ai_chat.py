"""A PydanticAI agent whose answer survives a dropped connection.

The agent answers questions about service latency and calls one tool,
``latency``, to look the numbers up.  The tool returns a dataclass, which the
default stream coder encodes as it is.  Each question is one run, streamed
through Redis, so a client that loses its connection reconnects with
``Last-Event-ID`` and gets the rest.  The producer is the PydanticAI recipe
from the streams guide.

* ``POST /chats/{chat_id}/runs/{run_id}`` starts a run with ``{"text": ...}``.
* ``GET /chats/{chat_id}/runs/{run_id}/events`` streams it as SSE: ``delta``,
  ``tool_call`` and ``tool_result`` events, then a final ``message``.
* ``DELETE /chats/{chat_id}/runs/{run_id}`` cancels it, and the model call or
  tool the agent waits on.

Needs Redis 8.6.2+ and a key for the model's provider.  ``AGENT_MODEL`` picks
the model, in PydanticAI's ``provider:model`` form; ``AGENT_MODEL=test`` runs
PydanticAI's test model, which needs no key.

Run it::

    export REDIS_URL=redis://localhost:6379/0 OPENAI_API_KEY=sk-...
    uv run --with "fastapi[standard]" --with "pydantic-ai-slim[openai]" fastapi dev examples/agent_sdks/pydantic_ai_chat.py

    curl -X POST localhost:8000/chats/c1/runs/r1 -H 'Content-Type: application/json' -d '{"text": "Why is checkout slow?"}'
    curl -N localhost:8000/chats/c1/runs/r1/events
    curl -N localhost:8000/chats/c1/runs/r1/events -H 'Last-Event-ID: <an id from above>'
"""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from fastapi import FastAPI, Request, Response
from pydantic import BaseModel
from pydantic_ai import (
    Agent,
    AgentRunResultEvent,
    FunctionToolCallEvent,
    FunctionToolResultEvent,
    PartDeltaEvent,
    PartStartEvent,
    TextPart,
    TextPartDelta,
)

from redis_fastapi import FastAPIRedis, StreamBackendDep, StreamWriter

P99_SECONDS = {"checkout": 2.3, "search": 0.4, "login": 0.2}


@dataclass
class Latency:
    service: str
    p99_seconds: float | None


agent = Agent(
    os.environ.get("AGENT_MODEL", "openai:gpt-5-mini"),
    instructions="You help on-call engineers. Look up latency before you answer.",
)


@agent.tool_plain
def latency(service: str) -> Latency:
    """Return a service's p99 latency."""
    return Latency(service=service, p99_seconds=P99_SECONDS.get(service))


def run_turn(prompt: str) -> Callable[[StreamWriter], Awaitable[None]]:
    async def produce(writer: StreamWriter) -> None:
        async with agent.run_stream_events(prompt) as events:
            async for event in events:
                if isinstance(event, PartStartEvent) and isinstance(
                    event.part, TextPart
                ):
                    if event.part.content:
                        await writer.append("delta", {"text": event.part.content})
                elif isinstance(event, PartDeltaEvent) and isinstance(
                    event.delta, TextPartDelta
                ):
                    await writer.append("delta", {"text": event.delta.content_delta})
                elif isinstance(event, FunctionToolCallEvent):
                    await writer.append(
                        "tool_call",
                        {
                            "id": event.part.tool_call_id,
                            "name": event.part.tool_name,
                            "args": event.part.args_as_dict(),
                        },
                    )
                elif isinstance(event, FunctionToolResultEvent):
                    await writer.append(
                        "tool_result",
                        {"id": event.part.tool_call_id, "output": event.part.content},
                    )
                elif isinstance(event, AgentRunResultEvent):
                    await writer.finish("message", {"text": event.result.output})

    return produce


app = FastAPI()
FastAPIRedis(app).lifespan().streams()


class Prompt(BaseModel):
    text: str


@app.post("/chats/{chat_id}/runs/{run_id}", status_code=202)
async def start(
    chat_id: str, run_id: str, body: Prompt, streams: StreamBackendDep
) -> dict[str, str]:
    writer = await streams.start(
        run_turn(body.text), scope=f"chat:{chat_id}", stream_id=run_id
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
