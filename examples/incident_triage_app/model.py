"""The model planner: a short manual tool loop on Claude Haiku 5.5.

A manual loop, not the SDK's tool runner, because every text delta must go to
the Redis stream and every tool call through the app's own endpoints, where
caching and rate limiting apply.

Runs only when ``ANTHROPIC_API_KEY`` is set and the ``anthropic`` package is
installed; otherwise the scripted planner runs.
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any

from examples.incident_triage_app.planner import (
    PACE,
    Planner,
    PlannerFailed,
    TriageRequest,
    call_tool,
)
from examples.incident_triage_app.streams_stub import StreamWriter
from examples.incident_triage_app.tools import SERVICES

try:
    import anthropic
except ImportError:  # the demo runs without it, on the scripted planner
    anthropic = None  # type: ignore[assignment]

MODEL = "claude-haiku-5-5"

SYSTEM = """\
You help an on-call engineer find out why a service misbehaves.

Call both tools for the service the engineer names: metrics and deploys. Write
nothing before the tool calls. Then answer in this exact shape, under 150 words, with no other headings:

**Likely cause:** one sentence.
- one bullet per piece of evidence, with the numbers from the tools
**Next steps**
1. numbered steps, most urgent first

Use only facts from the tool results. If they show no incident, say so."""

_SERVICE = {"type": "string", "enum": list(SERVICES)}

TOOLS: list[dict[str, Any]] = [
    {
        "name": "metrics",
        "description": "Latency and error metrics for one service over the last hour.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {"service": _SERVICE},
            "required": ["service"],
            "additionalProperties": False,
        },
    },
    {
        "name": "deploys",
        "description": "Deploys of one service in the last hour, with what each changed.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {"service": _SERVICE},
            "required": ["service"],
            "additionalProperties": False,
        },
    },
]

_client: Any = None


def model_configured() -> bool:
    return anthropic is not None and bool(os.environ.get("ANTHROPIC_API_KEY"))


def _get_client() -> Any:
    global _client
    if _client is None:
        _client = anthropic.AsyncAnthropic()  # reads ANTHROPIC_API_KEY
    return _client


async def run_tool(w: StreamWriter, call: Any) -> dict[str, Any]:
    if call.name not in {tool["name"] for tool in TOOLS}:
        return {
            "type": "tool_result",
            "tool_use_id": call.id,
            "content": f"unknown tool {call.name}",
            "is_error": True,
        }
    result = await call_tool(w, call.name, **call.input)
    return {
        "type": "tool_result",
        "tool_use_id": call.id,
        "content": json.dumps(result),
    }


async def triage_model(req: TriageRequest, w: StreamWriter) -> None:
    client = _get_client()
    messages: list[dict[str, Any]] = [
        {"role": "user", "content": f"Service: {req.service}\n\n{req.prompt}"}
    ]
    while True:
        async with client.messages.stream(
            model=MODEL,
            max_tokens=4000,  # a cost cap; a report needs far less
            system=SYSTEM,
            tools=TOOLS,
            output_config={"effort": "low"},
            messages=messages,
        ) as stream:
            async for text in stream.text_stream:
                await w.append("delta", {"text": text})
                await asyncio.sleep(PACE)
            message = await stream.get_final_message()
        if message.stop_reason != "tool_use":
            if message.stop_reason in ("refusal", "max_tokens"):
                raise PlannerFailed(message.stop_reason)
            return  # spawn() writes the final "stream.end" entry
        messages.append({"role": "assistant", "content": message.content})
        calls = [b for b in message.content if b.type == "tool_use"]
        results = await asyncio.gather(*(run_tool(w, c) for c in calls))
        messages.append({"role": "user", "content": list(results)})


MODEL_PLANNER = Planner("model", triage_model)
