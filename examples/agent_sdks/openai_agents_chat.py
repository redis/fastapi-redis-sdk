"""An OpenAI Agents SDK agent whose answer survives a dropped connection.

The agent answers questions about service latency and calls one tool,
``latency``, to look the numbers up.  Each question is one run, streamed
through Redis, so a client that loses its connection reconnects with
``Last-Event-ID`` and gets the rest.  The producer is the OpenAI Agents SDK
recipe from the streams guide.

* ``POST /chats/{chat_id}/runs/{run_id}`` starts a run with ``{"text": ...}``.
* ``GET /chats/{chat_id}/runs/{run_id}/events`` streams it as SSE: ``delta``,
  ``tool_call`` and ``tool_result`` events, then a final ``message``.
* ``DELETE /chats/{chat_id}/runs/{run_id}`` cancels it, and the model call or
  tool the agent waits on.

Needs Redis 8.6.2+ and an OpenAI API key.  ``AGENT_MODEL`` picks the model.

Run it::

    export REDIS_URL=redis://localhost:6379/0 OPENAI_API_KEY=sk-...
    uv run --with "fastapi[standard]" --with openai-agents fastapi dev examples/agent_sdks/openai_agents_chat.py

    curl -X POST localhost:8000/chats/c1/runs/r1 -H 'Content-Type: application/json' -d '{"text": "Why is checkout slow?"}'
    curl -N localhost:8000/chats/c1/runs/r1/events
    curl -N localhost:8000/chats/c1/runs/r1/events -H 'Last-Event-ID: <an id from above>'
"""

from __future__ import annotations

import json
import os
from collections.abc import Awaitable, Callable

from agents import Agent, Runner, function_tool
from fastapi import FastAPI, Request, Response
from openai.types.responses import ResponseTextDeltaEvent
from pydantic import BaseModel

from redis_fastapi import FastAPIRedis, StreamBackendDep, StreamWriter

P99_SECONDS = {"checkout": 2.3, "search": 0.4, "login": 0.2}


@function_tool
def latency(service: str) -> str:
    """Return a service's p99 latency."""
    p99 = P99_SECONDS.get(service)
    return f"{service}: p99 {p99} s" if p99 is not None else f"{service}: unknown"


agent = Agent(
    name="triage",
    instructions="You help on-call engineers. Look up latency before you answer.",
    model=os.environ.get("AGENT_MODEL", "gpt-5-mini"),
    tools=[latency],
)


def run_turn(prompt: str) -> Callable[[StreamWriter], Awaitable[None]]:
    async def produce(writer: StreamWriter) -> None:
        result = Runner.run_streamed(agent, prompt)
        async for event in result.stream_events():
            if event.type == "raw_response_event":
                if isinstance(event.data, ResponseTextDeltaEvent):
                    await writer.append("delta", {"text": event.data.delta})
            elif event.type == "run_item_stream_event":
                if event.name == "tool_called":
                    call = event.item.raw_item
                    await writer.append(
                        "tool_call",
                        {
                            "id": call.call_id,
                            "name": call.name,
                            "args": json.loads(call.arguments or "{}"),
                        },
                    )
                elif event.name == "tool_output":
                    await writer.append(
                        "tool_result",
                        {
                            "id": event.item.raw_item["call_id"],
                            "output": event.item.output,
                        },
                    )
        await writer.finish("message", {"text": str(result.final_output)})

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
