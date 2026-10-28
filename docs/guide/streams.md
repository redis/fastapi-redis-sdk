# Resumable streams

The SDK can carry an agent's streamed output through Redis, so a client that
loses its connection resumes from the last event it saw. A dropped VPN, a
closed laptop, a reload or a second device gets the rest of the answer, with
no gap and no repeat. The run itself never depends on the connection: a
disconnect does not stop it.

```python
from fastapi import Depends, FastAPI, Request
from redis_fastapi import FastAPIRedis, StreamBackendDep, StreamWriter

app = FastAPI()
FastAPIRedis(app).lifespan().streams()


async def run_agent(writer: StreamWriter) -> None:
    async for chunk in model.stream("Why is checkout slow?"):
        await writer.append("delta", {"text": chunk})


@app.post("/chats/{chat_id}/runs", status_code=202,
          dependencies=[Depends(require_chat_access)])
async def start(chat_id: str, run_id: str, streams: StreamBackendDep) -> dict:
    writer = await streams.start(run_agent, scope=f"chat:{chat_id}", stream_id=run_id)
    return {"stream_id": writer.stream_id}


@app.get("/chats/{chat_id}/runs/{stream_id}/events",
         dependencies=[Depends(require_chat_access)])
async def events(chat_id: str, stream_id: str, request: Request,
                 streams: StreamBackendDep):
    return await streams.sse(stream_id, request=request, scope=f"chat:{chat_id}")
```

`require_chat_access` is your own access check; the SDK has none (see
[Access](#access)). In the browser, an `EventSource` on the events route is
all a client needs: it reconnects by itself and sends the last event ID.

---

## How it works

One Redis Stream holds one run. Each event is a stream entry, and the entry ID
is the cursor: it goes out as the SSE `id:`, and the browser sends it back in
`Last-Event-ID` when it reconnects. The response replays the entries after the
cursor and then waits for new ones, in one read loop, so nothing is spliced
from two sources.

The producer and the reader share nothing but Redis. The producer is a task
that `spawn()` starts outside the request, and any process of the application
can serve the reader. When a client disconnects, only its reader ends. When an
API process restarts, the browser reconnects to another one.

---

## Setup

```python
FastAPIRedis(app).lifespan().streams()
```

The feature needs **Redis 8.6.2 or later**: it uses idempotent `XADD … IDMP`,
`DELEX` and `SET … IFEQ`. Caching, rate limiting and sessions keep their own,
lower requirements.

At startup the SDK asks the server whether it has the commands, the way the
rate limiter and sessions check for theirs (`COMMAND INFO XCFGSET DELEX`).
Without them the feature turns itself off. The application starts, its other
features work, the log has one warning, and every stream route answers `503`.
If Redis is unreachable at startup, the first stream call asks again.

The check confirms the command set, not the patch release. Redis 8.6.0 and
8.6.1 have the commands but carry defects in `IDMP`, so use 8.6.2 or later;
8.6.6 or later is advised for its stream security fixes.

---

## Starting a run

`start()` creates the run, starts your producer on it, and returns its
`StreamWriter`:

```python
writer = await streams.start(run_agent, scope="chat:42", stream_id=run_id)
```

- **`scope`** names what the run belongs to, usually the resource your access
  check just authorized, such as a chat. It is part of every key (see
  [Access](#access)).
- **`stream_id`** is optional. Let the browser generate one before it sends
  the request, and a retried `POST` finds the same run: `start()` starts no
  second producer, and the writer it returns has `created` set to `False`.
  This holds while the run goes on and for
  `stream_done_ttl + stream_gone_ttl` after it ends; later,
  the same ID starts a new run. Without a `stream_id`, the SDK generates one.
- **One active run per scope.** By default a run takes the scope's slot, and a
  second `open()` in the same scope raises `StreamBusy` (`409`), whose
  `stream_id` names the running one. Pass `exclusive=False` to allow several
  runs at once.

Scopes may use letters, digits and `_ . : -`, up to 200 characters. Stream IDs
may use letters, digits and `_ -`, up to 64. Any other value raises
`StreamInvalidName`, answered with `422`, on every method.

To do something between creating the run and starting the producer, such as
spending a budget only for a new run, call `open()` and `spawn()` yourself:

```python
writer = await streams.open(scope="chat:42", stream_id=run_id)
if writer.created:  # False for a retried start: the run already has a producer
    streams.spawn(writer, run_agent)
```

`spawn()` refuses a writer whose `created` is `False` with `ValueError`.

### Producing

`spawn()`, which `start()` calls, runs your producer as a task the backend
owns. It keeps a reference
so the task is not garbage-collected, refreshes the producer heartbeat while
it runs, and cancels it at shutdown.

```python
async def run_agent(writer: StreamWriter) -> None:
    for tool in plan:
        result = await call(tool)
        await writer.append("tool", {"name": tool.name, "result": result})
    async for chunk in model.stream(prompt):
        await writer.append("delta", {"text": chunk})
    await writer.finish("message", {"id": message_id})   # optional
```

`append(type, data)` stores one event and returns its entry ID. The type is
any name you choose, up to
128 characters with no line breaks; names starting with `stream.` belong to
the SDK. The data goes through the payload coder and may be at most
`stream_max_event_bytes`. The default coder, `JsonableCoder`, writes JSON and
also takes Pydantic models, dataclasses and datetimes, which is what agent
SDKs hand you; readers get plain JSON back. A timeout or a dropped connection
is retried with the same idempotency key, so a retried append never stores
the event twice.

For the OpenAI Agents SDK, PydanticAI, Google ADK and LangGraph, see
[Agent SDK recipes](#agent-sdk-recipes).

### Ending

Every stream ends with exactly one final event. Readers stop after it, and a
reconnect after it gets `204`, which stops `EventSource` for good.

- Call `writer.finish(type, data)` to make your own last event the final one,
  for example the finished message.
- Otherwise `spawn()` writes a final `stream.end` event when your producer
  returns, raises or is cancelled. Its data gives the reason:
  `{"reason": "done"}`, `{"reason": "error", "code": "producer_error"}` or
  `{"reason": "cancelled"}`. An exception with a string `code` attribute
  reports that code instead.

After the final event the writer refuses further appends with
`StreamFinished`. Every final event shares one idempotency key, so a stream
holds at most one, even if two writers end it at the same time.

---

## Reading

```python
return await streams.sse(stream_id, request=request, scope=scope)
```

`sse()` does every check that decides the status code before the first byte,
then streams. It reads the cursor from the request itself, in this order:

1. A cursor you pass as `after`, for when you know better, for example after
   loading state together with the cursor that matches it.
2. The `Last-Event-ID` header.
3. An `?after=` query parameter, for clients that cannot set headers.
4. None: the whole stream.

Do not copy the `?after=` query into `after`. `EventSource` keeps the URL it
was built with but sends a newer `Last-Event-ID` on every reconnect, and a
copied query would win and replay entries twice.

A cursor must be a full entry ID such as `1700000000000-0`. `0-0` means the
whole stream. The special IDs `$`, `+` and `-`, a bare timestamp, and a
repeated header or query parameter are refused with `422`.

The response sends a `: ping` comment every `stream_heartbeat_interval`, sets
`Cache-Control: no-store` and `X-Accel-Buffering: no`, and closes after the
final event. Its `retry:` hint asks the browser to reconnect after one to three
seconds, chosen at random per connection, so clients that a deploy closed at
the same moment do not all return at once. Never put the SSE route behind `cache()`, and exclude it from
compression middleware that buffers the body.

### Reading without HTTP

`follow()` is the same read for code that is not an SSE response. It runs
every check when you await it, so a WebSocket handler can refuse a bad request
before it accepts the connection, and it returns a `StreamReader`:

```python
reader = await streams.follow(stream_id, scope=scope, after=cursor)
await websocket.accept()
async with reader:
    async for event in reader:
        await websocket.send_json({"id": event.id, "type": event.type,
                                   "data": event.data})
```

`async with` closes the read at once if you leave the loop early. A reader
whose cursor is at or past the final event is empty. A `follow()` reader takes
no reader slot, so `stream_max_readers` does not limit it; bound such readers
yourself.

---

## Access

The SDK does not decide who may read a stream. Your FastAPI dependency checks
access first, as for any other route, and the SDK carries events.

What the SDK does is bind each stream to its scope. The scope is part of the
stream's Redis key, so a stream ID from another scope names a key that does
not exist, and gets the same `404` as an unknown stream. A user who may read
chat A cannot read chat B's run by sending its ID under chat A's URL. Build the
scope from the object your dependency authorized, not from the raw path, when
you resolve aliases or tenant-local IDs.

Because access attaches to the resource and not to a session, every member of
a shared chat, every tab and every device of a user reaches the same streams.
A second device finds the current run with `active()`:

```python
@app.get("/chats/{chat_id}/runs/active",
         dependencies=[Depends(require_chat_access)])
async def active(chat_id: str, streams: StreamBackendDep) -> dict:
    return {"stream_id": await streams.active(f"chat:{chat_id}")}
```

### Ending a connection with the credential

Your access check runs once, when the connection opens. To end an open stream
when the credential does, pass its deadline:

```python
from redis_fastapi import session_expiry

return await streams.sse(stream_id, request=request, scope=scope,
                         expires_at=session_expiry(request))
```

`session_expiry()` returns the absolute deadline of the SDK session. With
other credentials, pass your own, for example a JWT's `exp`. At the deadline
the response sends a `stream.error` event with code `credential_expired` and
closes. Every connection also closes after `stream_max_connection_duration`,
with no event. The browser then reconnects through your dependency, so a
revoked session keeps reading for at most that long.

---

## Stopping a run

A closed connection never stops a run. To stop one, expose your own endpoint:

```python
@app.post("/chats/{chat_id}/runs/{stream_id}/cancel", status_code=204,
          dependencies=[Depends(require_chat_access)])
async def stop(chat_id: str, stream_id: str, streams: StreamBackendDep) -> None:
    await streams.cancel(stream_id, scope=f"chat:{chat_id}")
```

`cancel()` sets a flag in Redis and returns `False` if the run had already
ended. A producer started by `spawn()` in the same process is cancelled at
once; one in another process sees the flag at its next append or heartbeat. A
producer you run yourself checks `writer.cancelled` after each append and calls
`finish()`. A cancel that arrives while the producer writes its final event
lets that write finish.

---

## Producers that stop

A producer keeps a short-lived heartbeat in Redis, an `alive` field in the
run's metadata hash with its own TTL, which every append and every heartbeat
refreshes. When a reader has waited a full heartbeat with no new entry, it
checks that field. If the field has lapsed and the stream has no final event, the producer is gone, and the reader writes the final event the
producer never wrote: `stream.end` with `{"reason": "producer_lost"}`. Every
reader then stops, and reconnects get `204` instead of waiting again.

- **Long tool calls.** `spawn()` runs `writer.keepalive()` around your
  producer. If you run a producer yourself, wrap long waits in
  `async with writer.keepalive():`.
- **Slow waits.** `writer.extend(ttl)` keeps the stream, the heartbeat and
  the active slot for `ttl` seconds, for a run that waits on something slow.
- **Another process continues.** `streams.attach(stream_id, scope=...)`
  returns a writer that continues the run's sequence numbers. The old writer is
  fenced: its next append or heartbeat raises `StreamFenced`, and at most that
  one append lands after the takeover. Its `finish()` raises `StreamFenced`
  before it writes, so it can never end the new writer's run.

A reader that cannot write, because its Redis ACL is read-only, sends a
`stream.error` event with code `producer_lost` and closes instead.

---

## Agent SDK recipes

Agent SDKs such as the OpenAI Agents SDK, PydanticAI, Google ADK and LangGraph
run the agent loop and give you its events as an async iterator. They have no
server of their own, so you host them in your FastAPI application, and a
producer is a short loop that turns their events into appends.

Each recipe below maps its SDK's events to the same four event types, so the
browser code is the same whichever SDK you use:

| Type          | Data                            | Written with |
|---------------|---------------------------------|--------------|
| `delta`       | `{"text": ...}`                 | `append()`   |
| `tool_call`   | `{"id": ..., "name": ..., "args": ...}` | `append()` |
| `tool_result` | `{"id": ..., "output": ...}`    | `append()`   |
| `message`     | `{"text": ...}`, the whole answer | `finish()` |

Each recipe returns a producer for one turn. Start it from your route as any
other producer:

```python
@app.post("/chats/{chat_id}/runs/{run_id}", status_code=202,
          dependencies=[Depends(require_chat_access)])
async def start(chat_id: str, run_id: str, body: Prompt,
                streams: StreamBackendDep) -> dict:
    writer = await streams.start(run_turn(body.text),
                                 scope=f"chat:{chat_id}", stream_id=run_id)
    return {"stream_id": writer.stream_id}
```

Each recipe is also a complete app in
[`examples/agent_sdks/`](https://github.com/redis/fastapi-redis-sdk/tree/main/examples/agent_sdks),
with the start, events and cancel routes. These SDKs often rename and reshape
their events, so check a recipe against the version you run. Checked with
`openai-agents` 0.24, `pydantic-ai-slim` 2.54, `google-adk` 2.7, and
`langgraph` 1.2 with `langchain-core` 1.7.

### Rules for every SDK

- **Map events, do not store them whole.** An SDK event often holds the agent
  and its tools, which no coder can encode. The ones that can be encoded are
  large: an ADK event is about 1.6 KB for a three-word chunk, and PydanticAI's
  final event holds the whole message history. Pick the fields the browser
  needs. Values such as a tool's Pydantic or dataclass result can go in as
  they are, because the default coder encodes them.
- **Send deltas, not snapshots.** Every event must fit in
  `stream_max_event_bytes`. A large tool result, such as retrieved documents,
  is better sent as a summary.
- **Cancel works as it is.** `cancel()` cancels the producer task, and the
  cancel reaches the model call or tool the SDK is waiting on. `spawn()` then
  writes `stream.end` with `{"reason": "cancelled"}`. This holds for all four
  SDKs.
- **Errors end the stream.** An exception from the SDK, such as a provider
  error, ends the stream with `{"reason": "error", "code": "producer_error"}`.
- **A pause for a person ends the turn.** When the agent waits for an
  approval or an answer, end the turn with
  `finish("turn.interrupted", ...)`, and start the answer as a new turn with a
  new stream ID. A stream left open while a person decides needs `extend()`
  against its TTL, and holds the scope's active slot all that time. The
  LangGraph recipe shows the pattern; the OpenAI Agents SDK
  (`result.interruptions`), PydanticAI (deferred tools) and ADK (long-running
  tools) work the same way.
- **The conversation lives elsewhere.** One stream carries one turn. Keep the
  conversation in the SDK's own session or checkpoint store.

Not covered in this release: producers in sync code (CrewAI, ADK's sync
`Runner.run`), and durable execution, where the SDK appends from a Temporal,
DBOS or Prefect worker. Both need a writer outside the FastAPI process (see
[Limitations](#limitations)).

### OpenAI Agents SDK

```python
import json

from agents import Agent, Runner
from openai.types.responses import ResponseTextDeltaEvent

agent = Agent(name="triage", instructions="...", tools=[latency])


def run_turn(prompt: str):
    async def produce(writer: StreamWriter) -> None:
        result = Runner.run_streamed(agent, prompt)
        async for event in result.stream_events():
            if event.type == "raw_response_event":
                if isinstance(event.data, ResponseTextDeltaEvent):
                    await writer.append("delta", {"text": event.data.delta})
            elif event.type == "run_item_stream_event":
                if event.name == "tool_called":
                    call = event.item.raw_item
                    await writer.append("tool_call", {
                        "id": call.call_id,
                        "name": call.name,
                        "args": json.loads(call.arguments or "{}"),
                    })
                elif event.name == "tool_output":
                    await writer.append("tool_result", {
                        "id": event.item.raw_item["call_id"],
                        "output": event.item.output,
                    })
        await writer.finish("message", {"text": str(result.final_output)})

    return produce
```

- Text arrives as raw Responses API events, and tool calls and their results
  as run items. Every other event can be left out.
- `result.final_output` is set once the loop ends.
- When the producer is cancelled, `stream_events()` cancels the run and the
  tool it waits on. No `result.cancel()` is needed.

### PydanticAI

```python
from pydantic_ai import (
    Agent, AgentRunResultEvent, FunctionToolCallEvent, FunctionToolResultEvent,
    PartDeltaEvent, PartStartEvent, TextPart, TextPartDelta,
)

agent = Agent(model, tools=[latency])


def run_turn(prompt: str):
    async def produce(writer: StreamWriter) -> None:
        async with agent.run_stream_events(prompt) as events:
            async for event in events:
                if isinstance(event, PartStartEvent) and isinstance(event.part, TextPart):
                    if event.part.content:
                        await writer.append("delta", {"text": event.part.content})
                elif isinstance(event, PartDeltaEvent) and isinstance(event.delta, TextPartDelta):
                    await writer.append("delta", {"text": event.delta.content_delta})
                elif isinstance(event, FunctionToolCallEvent):
                    await writer.append("tool_call", {
                        "id": event.part.tool_call_id,
                        "name": event.part.tool_name,
                        "args": event.part.args_as_dict(),
                    })
                elif isinstance(event, FunctionToolResultEvent):
                    await writer.append("tool_result", {
                        "id": event.part.tool_call_id,
                        "output": event.part.content,
                    })
                elif isinstance(event, AgentRunResultEvent):
                    await writer.finish("message", {"text": event.result.output})

    return produce
```

- `run_stream_events()` is an async context manager; use it with `async with`.
- The first text of a reply comes in `PartStartEvent`, and only the rest in
  `PartDeltaEvent`. Read both, or the answer loses its first chunk.
- `AgentRunResultEvent` holds the whole message history. Send only its
  `output`.
- A tool may return a Pydantic model or a dataclass; the default coder
  encodes it.

### Google ADK

```python
from google.adk.agents import LlmAgent
from google.adk.agents.run_config import RunConfig, StreamingMode
from google.adk.runners import Runner
from google.genai import types

runner = Runner(app_name="triage",
                agent=LlmAgent(name="triage", model=model, tools=[latency]),
                session_service=session_service)


def run_turn(prompt: str, *, user_id: str, session_id: str):
    async def produce(writer: StreamWriter) -> None:
        message = types.Content(role="user", parts=[types.Part(text=prompt)])
        config = RunConfig(streaming_mode=StreamingMode.SSE)
        answer = ""
        async for event in runner.run_async(user_id=user_id, session_id=session_id,
                                            new_message=message, run_config=config):
            if event.partial:
                # A chunk of the answer.  The final, non-partial event repeats
                # the whole text, so only chunks become deltas.
                parts = event.content.parts if event.content else None
                for part in parts or []:
                    if part.text and not part.thought:
                        await writer.append("delta", {"text": part.text})
                continue
            for call in event.get_function_calls():
                await writer.append("tool_call",
                                    {"id": call.id, "name": call.name, "args": call.args})
            for response in event.get_function_responses():
                await writer.append("tool_result",
                                    {"id": response.id, "output": response.response})
            if event.is_final_response() and event.content and event.content.parts:
                answer = "".join(part.text or "" for part in event.content.parts
                                 if not part.thought)
        await writer.finish("message", {"text": answer})

    return produce
```

- Without `StreamingMode.SSE`, ADK sends no partial events, and the answer
  arrives in one piece.
- The last, non-partial event repeats the whole answer. Sending it as a delta
  would show the answer twice.
- Leave out `thought` parts unless your interface shows the model's
  reasoning.
- The ADK session must exist before the turn starts. Create it in the start
  route, or keep one per chat.

### LangGraph

```python
from langchain_core.messages import AIMessage, AIMessageChunk, ToolMessage

graph = builder.compile(checkpointer=checkpointer)


def run_turn(inputs, config=None):
    async def produce(writer: StreamWriter) -> None:
        answer = ""
        async for mode, chunk in graph.astream(inputs, config,
                                               stream_mode=["messages", "updates"]):
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
                        await writer.append("tool_result", {
                            "id": message.tool_call_id, "output": message.content})
                    elif isinstance(message, AIMessage) and message.tool_calls:
                        for call in message.tool_calls:
                            await writer.append("tool_call", {
                                "id": call["id"], "name": call["name"],
                                "args": call["args"]})
                    elif isinstance(message, AIMessage):
                        answer = message.text
        await writer.finish("message", {"text": answer})

    return produce
```

Start a turn with `run_turn({"messages": [("user", text)]}, thread)` and
resume after an interrupt with `run_turn(Command(resume=answer), thread)`,
where `thread` is `{"configurable": {"thread_id": chat_id}}`. Each is its own
run with its own stream ID; the checkpointer carries the graph between them.

- Use the `messages` and `updates` stream modes. Tokens come from `messages`;
  tool calls, tool results and interrupts from `updates`. Avoid `values`: it
  sends the whole state after every step.
- `message.text` joins the text blocks of a message, so it also works for
  providers that send content as a list of blocks.
- On Python 3.10, a node must pass its `config` on to the model
  (`await model.ainvoke(messages, config)`); without it the `messages` mode
  gets the whole reply at once instead of its tokens. `interrupt()` in an
  async graph needs Python 3.11 or later.

---

## Errors and status codes

Before the first byte, a problem is an ordinary status code. Your own
dependency answers `401` or `403` before the SDK runs.

| Status | Cause                                                                    | Exception                 |
|--------|--------------------------------------------------------------------------|---------------------------|
| `204`  | The cursor is at or past the final event                                 | -                         |
| `401`  | `expires_at` had already passed                                          | `StreamCredentialExpired` |
| `404`  | No such stream in this scope                                             | `StreamNotFound`          |
| `409`  | The scope has an active run; the cursor is past the stream's last entry  | `StreamBusy`, `StreamCursorAhead` |
| `410`  | The stream expired, or entries after the cursor were removed             | `StreamExpired`           |
| `422`  | A malformed or repeated cursor; a scope or stream ID outside the rules   | `StreamInvalidCursor`, `StreamInvalidName` |
| `503`  | Too many live readers; Redis unavailable; the server lacks support       | -, `StreamStoreError`, `StreamUnsupported` |

`StreamError` is the base of every stream exception. The body is
`{"detail": ..., "code": ...}`, where `code` is a stable name such as
`stream_not_found`, `stream_expired` or `cursor_ahead` (see the
[API reference](../api/reference.md#stream-errors) for the full list), and a
`503` carries `Retry-After`. Branch on `code`, not on `detail`. A handler you
register for `StreamError`, or for one of its subclasses, takes precedence.

After the first byte the status is sent, so the response handles a problem in
one of two ways, and the client needs no list of error codes:

- **Passing**: Redis down for longer than `stream_read_retry_window`, a
  shutdown, the connection cap, a client that stops reading. The response
  closes with no event, and `EventSource` reconnects and resumes from
  `Last-Event-ID`. A shorter Redis failure never reaches the client: the reader
  retries and keeps sending pings.
- **Over**: a dead producer, or a stream that vanished. The stream ends with a
  final event, or the reconnect gets `204`, `410` or `404`, and `EventSource`
  stops.

### Gaps

A stream keeps about `stream_maxlen` entries. A reader that falls so far behind
that trimming removes entries it has not read gets a `stream.gap` event with
`{"missing": n}`, and the read goes on. A reconnect whose cursor already points
into removed history gets `410`, and so does a request with no cursor once
anything was removed. Reload the state from your own database then. A client
whose last event was exactly the newest one trimmed also gets `410`, though it
missed nothing.

---

## The browser side

```javascript
const events = new EventSource(`/chats/${chatId}/runs/${runId}/events`);

events.addEventListener("delta", (e) => append(JSON.parse(e.data).text));
events.addEventListener("stream.gap", (e) => markIncomplete(JSON.parse(e.data)));
events.addEventListener("stream.end", (e) => {
  events.close();
  showOutcome(JSON.parse(e.data).reason);   // done, error, cancelled, producer_lost
});
events.addEventListener("stream.error", (e) => {
  events.close();
  if (JSON.parse(e.data).code === "credential_expired") signIn();
});
```

- The client need not know which event is last. The server closes after the
  final event, the browser reconnects with its cursor, gets `204`, and stops.
- `EventSource` hides the status of a refused connection. To show why a
  stream failed, ask a normal endpoint, or use a `fetch`-based SSE client.
- `EventSource` cannot send an `Authorization` header. Cookie sessions work as
  they are; with bearer tokens, use a `fetch`-based client. Never put a token
  in the URL.
- To survive a page reload, save the last event ID with the state it produced,
  and pass it as `?after=` when you build the next `EventSource`.
- Receiving an event is not storing it, so your handlers must tolerate a
  replay.

---

## Limits

- **Live readers.** At most `stream_max_readers` per process; over the limit a
  new reader gets `503` with `Retry-After`. A reader takes its slot when its
  response starts, and every way the response ends frees it.
- **Slow clients.** A send that waits longer than `stream_send_timeout`
  closes the response, so a frozen tab does not hold a slot. A dead client on
  an idle stream is not caught this way; the connection cap bounds that case.
- **Connections.** Each live reader holds a pool connection for each blocking
  read. With `REDIS_MAX_CONNECTIONS` set, size it for `stream_max_readers` plus
  your other traffic. The driver's `socket_timeout` must be longer than
  `stream_heartbeat_interval`; `.streams()` refuses a setup where it is not.
- **Redis failover.** Replication is asynchronous, so a failover can lose
  entries the old primary had acknowledged. A reader whose cursor is past the
  loss gets `409`; one behind it sees a `stream.gap`.
- **Eviction.** Every stream key has a TTL. Where Redis is shared with other
  data, `maxmemory-policy volatile-lrm` evicts finished, idle streams first.

---

## Limitations

- **FastAPI applications only.** Producers and readers run inside an
  application set up with `.streams()`. Nothing stops another process from
  using the same keys, but that is not supported, and the key layout may
  change.
- **Producers do not survive a deploy.** A producer started by `spawn()` dies
  with its process. Its readers get `producer_lost`, and the user starts a new
  run.
- **SSE only.** There is no WebSocket reader; `follow()` is the starting point
  for one.
- **Async producers only.** A producer in sync code, such as CrewAI or ADK's
  sync `Runner.run`, has no writer yet.

---

## Redis keys

Every key uses the SDK's `prefix` setting (`REDIS_PREFIX`, default
`redis:fastapi`):

```
<prefix>:stream:{<scope>}:<id>        Stream   the events
<prefix>:stream-meta:{<scope>}:<id>   Hash     status, cancel, writer, started, alive
<prefix>:stream-active:{<scope>}      String   the scope's active run
```

The `{<scope>}` hash tag keeps a scope's keys in one Cluster slot, so one
transaction starts a run and takes the slot, and one transaction ends it and
frees the slot. It also puts all of a scope's runs on one shard, so make the
scope one resource, such as a chat, and not one value for the whole
application. The `alive` field in the metadata hash is the producer heartbeat;
it has its own TTL, `stream_alive_ttl`. The metadata hash outlives the
stream by `stream_gone_ttl`,
so a late reconnect gets `410` (expired) instead of `404` (unknown). The prefix
must not contain braces.

---

## Settings

Every setting is an environment variable prefixed `REDIS_`, so
`stream_active_ttl` is `REDIS_STREAM_ACTIVE_TTL`. Durations are whole
seconds, as everywhere else in the SDK. See
[Configuration](configuration.md) for how these are loaded.

| Setting                          | Default  | Notes                                                                |
|----------------------------------|----------|----------------------------------------------------------------------|
| `stream_active_ttl`              | `600`    | TTL of a running stream, refreshed by every append                   |
| `stream_done_ttl`                | `120`    | How long a finished stream stays readable                            |
| `stream_gone_ttl`                | `3600`   | How long metadata outlives the stream, for `410` instead of `404`    |
| `stream_maxlen`                  | `10000`  | Approximate entries kept per stream                                  |
| `stream_max_event_bytes`         | `65536`  | Largest encoded event                                                |
| `stream_heartbeat_interval`      | `15`     | SSE ping, blocking-read bound, producer heartbeat                    |
| `stream_alive_ttl`               | `45`     | Lifetime of the producer heartbeat field; longer than the heartbeat  |
| `stream_replay_page`             | `100`    | Entries per read                                                     |
| `stream_max_readers`             | `256`    | Live SSE readers per process                                         |
| `stream_max_connection_duration` | `300`    | Longest SSE connection before the client reconnects                  |
| `stream_read_retry_window`       | `30`     | How long a reader retries a failing Redis read                       |
| `stream_send_timeout`            | `10`     | Longest wait for one send to a client                                |

---

## Telemetry

With [telemetry](observability.md) enabled, the feature emits
`redis_fastapi.streams.operations` (by operation and result),
`redis_fastapi.streams.latency` and the `redis_fastapi.streams.readers` gauge.
No scope, stream ID or event data becomes a label.
