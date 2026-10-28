"""A LangGraph agent that streams its answer and pauses for approval.

The graph is the usual agent loop, a model node and a ``ToolNode``, with one
step between them: before any tool runs, the graph asks the engineer with
``interrupt()``.  The question ends the run with a ``turn.interrupted`` event,
and the answer starts a new run on a new stream.  The checkpointer carries the
graph between the two, keyed by the chat.  The producer is the LangGraph
recipe from the streams guide.

* ``POST /chats/{chat_id}/runs/{run_id}`` starts a run, either a question,
  ``{"text": ...}``, or the answer to the last interrupt, ``{"approve": true}``.
* ``GET /chats/{chat_id}/runs/{run_id}/events`` streams it as SSE: ``delta``,
  ``tool_call`` and ``tool_result`` events, then a final ``message`` or
  ``turn.interrupted``.
* ``DELETE /chats/{chat_id}/runs/{run_id}`` cancels it, and the model call or
  tool the graph waits on.

Needs Python 3.11+, because ``interrupt()`` in an async graph does, Redis
8.6.2+ and an OpenAI API key.  ``AGENT_MODEL`` picks the model.  The
checkpoints live in memory, so they are lost on restart.

Run it::

    export REDIS_URL=redis://localhost:6379/0 OPENAI_API_KEY=sk-...
    uv run --python 3.12 --with "fastapi[standard]" --with langgraph --with langchain-openai fastapi dev examples/agent_sdks/langgraph_chat.py

    curl -X POST localhost:8000/chats/c1/runs/r1 -H 'Content-Type: application/json' -d '{"text": "Why is checkout slow?"}'
    curl -N localhost:8000/chats/c1/runs/r1/events     # ends with turn.interrupted
    curl -X POST localhost:8000/chats/c1/runs/r2 -H 'Content-Type: application/json' -d '{"approve": true}'
    curl -N localhost:8000/chats/c1/runs/r2/events     # the tool result, then the answer
"""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from typing import Annotated, Any, TypedDict

from fastapi import FastAPI, HTTPException, Request, Response
from langchain_core.messages import (
    AIMessage,
    AIMessageChunk,
    AnyMessage,
    ToolMessage,
)
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode, tools_condition
from langgraph.types import Command, interrupt
from pydantic import BaseModel

from redis_fastapi import FastAPIRedis, StreamBackendDep, StreamWriter

P99_SECONDS = {"checkout": 2.3, "search": 0.4, "login": 0.2}


@tool
def latency(service: str) -> str:
    """Return a service's p99 latency."""
    p99 = P99_SECONDS.get(service)
    return f"{service}: p99 {p99} s" if p99 is not None else f"{service}: unknown"


class State(TypedDict):
    messages: Annotated[list[AnyMessage], add_messages]


model = ChatOpenAI(model=os.environ.get("AGENT_MODEL", "gpt-5-mini")).bind_tools(
    [latency]
)


async def agent(state: State, config: RunnableConfig) -> dict[str, Any]:
    # Pass the config on to the model, or on Python 3.10 the "messages"
    # stream mode gets the whole reply instead of its tokens.
    return {"messages": [await model.ainvoke(state["messages"], config)]}


async def approve(state: State) -> dict[str, Any]:
    calls = state["messages"][-1].tool_calls  # type: ignore[union-attr]
    if interrupt({"question": "Run these lookups?", "tool_calls": calls}):
        return {}
    declined = [
        ToolMessage("The engineer declined this lookup.", tool_call_id=call["id"])
        for call in calls
    ]
    return {"messages": declined}


def after_approval(state: State) -> str:
    return "agent" if isinstance(state["messages"][-1], ToolMessage) else "tools"


builder = StateGraph(State)
builder.add_node("agent", agent)
builder.add_node("approve", approve)
builder.add_node("tools", ToolNode([latency]))
builder.add_edge(START, "agent")
builder.add_conditional_edges("agent", tools_condition, {"tools": "approve", END: END})
builder.add_conditional_edges("approve", after_approval, ["agent", "tools"])
builder.add_edge("tools", "agent")
graph = builder.compile(checkpointer=InMemorySaver())


def run_turn(
    inputs: Any, config: RunnableConfig | None = None
) -> Callable[[StreamWriter], Awaitable[None]]:
    async def produce(writer: StreamWriter) -> None:
        answer = ""
        async for mode, chunk in graph.astream(
            inputs, config, stream_mode=["messages", "updates"]
        ):
            if mode == "messages":
                message, _metadata = chunk
                if isinstance(message, AIMessageChunk) and message.text:
                    await writer.append("delta", {"text": message.text})
                continue
            if "__interrupt__" in chunk:
                # The graph waits for a person.  End this turn; the answer
                # resumes the graph as a new turn, on a new stream.
                interrupts = [item.value for item in chunk["__interrupt__"]]
                await writer.finish("turn.interrupted", {"interrupts": interrupts})
                return
            for update in chunk.values():
                for message in (update or {}).get("messages", []):
                    if isinstance(message, ToolMessage):
                        await writer.append(
                            "tool_result",
                            {"id": message.tool_call_id, "output": message.content},
                        )
                    elif isinstance(message, AIMessage) and message.tool_calls:
                        for call in message.tool_calls:
                            await writer.append(
                                "tool_call",
                                {
                                    "id": call["id"],
                                    "name": call["name"],
                                    "args": call["args"],
                                },
                            )
                    elif isinstance(message, AIMessage):
                        answer = message.text
        await writer.finish("message", {"text": answer})

    return produce


app = FastAPI()
FastAPIRedis(app).lifespan().streams()


class Turn(BaseModel):
    text: str | None = None
    approve: bool | None = None


@app.post("/chats/{chat_id}/runs/{run_id}", status_code=202)
async def start(
    chat_id: str, run_id: str, body: Turn, streams: StreamBackendDep
) -> dict[str, str]:
    thread: RunnableConfig = {"configurable": {"thread_id": chat_id}}
    if body.approve is not None:
        inputs: Any = Command(resume=body.approve)
    elif body.text is not None:
        inputs = {"messages": [("user", body.text)]}
    else:
        raise HTTPException(422, "Send text or approve")
    writer = await streams.start(
        run_turn(inputs, thread), scope=f"chat:{chat_id}", stream_id=run_id
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
