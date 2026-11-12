# Redis Streams over SSE, and agent tasks on top: proposed design

Date: 2026-11-11
Status: proposal, for discussion on PR #100
Replaces: the public shape of [`resumable-streams-design.md`](resumable-streams-design.md).
Most of its reader mechanics carry over (cursor parsing, the before-first-byte
checks, reader slots, send timeouts, frame-injection rules); this document says
where.

## 1. Why split the feature

The current design starts from one use case, an agent turn, and builds the SSE
integration around it. Every stream the SDK serves must be a *run*: a key the
SDK created, entries in the SDK's `t`/`d`/`q`/`f` layout, a background producer
the SDK owns, one final entry, a lifetime and an idempotency window. That has
three costs:

- **It can't serve a stream the application already has.** An activity feed, a
  notification stream, job progress written by a worker in another language:
  none of these can go behind `sse()`, which is the most common reason to
  want SSE backed by Redis Streams.
- **Run machinery leaks into the read path.** Readers check producer
  liveness and write the final entry themselves (so a reader needs write
  permission), track writer sequence numbers for `stream.gap`, and the whole
  feature needs Redis 8.6.2 for `XADD ... IDMP` and `XCFGSET`.
- **The run rules are hard to get right.** Most review findings on #106 were
  in run lifecycle code, not in the SSE path.

This proposal splits the feature in two:

- **Part A, the core:** expose any Redis Stream as an SSE endpoint, with
  `Last-Event-ID` resume. Producers are anything that runs `XADD`. Readers
  only read. Works on every Redis the SDK supports (7.4+).
- **Part B, agent tasks:** an optional helper for "one agent task, streamed,
  resumable, cancellable, able to pause for input", on the A2A v1.0 data
  model (tasks, contexts, task states, status and artifact updates). It is
  built only on Part A's public API plus its own writes, and also works on
  Redis 7.4+.

## 2. Terms

| Term | Meaning |
|---|---|
| stream | A Redis Stream key. In Part A, any key the application names. |
| entry | One stream entry: an ID (`<ms>-<seq>`) and a field map. |
| cursor | An entry ID. The SSE `id:` of each event, and what the browser sends back as `Last-Event-ID`. |
| reader | One SSE response (or one `read()` iterator) following a stream. |
| run | Today's design: one agent turn, the unit every stream had to be. |
| task | Part B only: one A2A task, stored as one stream plus a small status hash (B3). |

---

# Part A: the core, Redis Streams as an SSE backend

## A1. Goals and non-goals

Goals:

1. Serve any Redis Stream the application authorizes as `text/event-stream`.
2. A client that reconnects with `Last-Event-ID` gets the entries after its
   cursor that are still in the stream, with no repeat. Entries the
   application trimmed or deleted are simply not sent (A6).
3. Readers never write to Redis.
4. Bounded resources per process: a cap on live readers, a send timeout for
   slow clients, a longest connection time.
5. Works on Redis 7.4+, standalone, Cluster and Sentinel, with no capability
   probe.
6. Works for fetch-based clients (which see status codes) and for
   `EventSource` (which doesn't).

Non-goals:

- Producing events. `publish()` (A8) is a convenience, not a requirement.
- Any notion of a run, an end, or a producer. A stream ends only if the
  application says how to recognize the end (`until`, A4).
- Consumer groups. Each reader has its own cursor, held by the client.
- WebSocket in the first release. `read()` is the transport-neutral iterator a
  WebSocket handler would use.

## A2. Model

- The application gives the SDK a **key**. The SDK never builds keys for Part A
  and never assumes a field layout. `key()` (A3) is an optional helper that
  applies the SDK's prefix and validates the parts.
- The **entry ID is the SSE `id:`**, always. It is not configurable, because
  resume depends on it.
- **One `XREAD BLOCK` loop** serves replay and live tail, page by page, as in
  the current design (section 4 there). There is no splice between replay and
  live.
- The application **authorizes** before calling `sse()`, in its own
  dependency. The key is not a credential.

## A3. Public API

```python
class RedisStreams:                      # one per app, via StreamsDep
    def key(self, *parts: str) -> str: ...
        # "<prefix>:" + ":".join(parts); each part validated (A9)

    async def sse(
        self,
        key: str,
        *,
        request: Request,
        start: Literal["first", "last"] = "first",
        if_missing: Literal["404", "wait"] = "404",
        format: Callable[[StreamEntry], SSEEvent | None] | None = None,
        until: Callable[[StreamEntry], bool] | None = None,
        deadline: datetime | None = None,
    ) -> Response: ...

    async def read(
        self,
        key: str,
        *,
        after: str | None = None,
        start: Literal["first", "last"] = "first",
        if_missing: Literal["404", "wait"] = "404",
        skip: Callable[[StreamEntry], bool] | None = None,
        until: Callable[[StreamEntry], bool] | None = None,
        deadline: datetime | None = None,
        ticks: bool = False,
    ) -> StreamSubscription: ...
        # async iterator of StreamEntry (and StreamTick with ticks=True);
        # async context manager. Holds a reader slot until closed.
        # .cursor: the last entry read, skipped ones included.
        # .ended_by: "until" | "deadline" | "closed" once iteration stops;
        #   already "until" on return when the cursor's own entry matches.

    def cursor(self, request: Request) -> str | None: ...
        # Last-Event-ID, then ?after=; 422 on a malformed one (A4)

    def respond(
        self,
        items: AsyncIterator[SSEEvent | StreamTick],
        *,
        request: Request,
    ) -> Response: ...
        # frames items as text/event-stream (A7); closes `items` on exit

    async def publish(
        self,
        key: str,
        fields: Mapping[str, str | bytes | int | float],
        *,
        maxlen: int | None = None,
        approximate: bool = True,
        ttl: int | None = None,
    ) -> str: ...                        # the entry ID

@dataclass(frozen=True)
class StreamEntry:
    id: str
    fields: dict[str, str]               # decoded as UTF-8

@dataclass(frozen=True)
class StreamTick:
    quiet_for: float                     # seconds of successful empty reads
                                         # since the last entry or failed read

@dataclass(frozen=True)
class SSEEvent:
    data: str | None                     # None with an id: an id-only frame (A5)
    event: str | None = None             # None: no "event:" line, i.e. "message"
    id: str | None = None                # the "id:" line; sse() sets the entry ID
```

Parameters of `sse()` and `read()`:

| Parameter | Meaning |
|---|---|
| `start` | Where to begin when the client sends no cursor. `"first"` replays the whole stream. `"last"` sends only entries added after the request; the SDK resolves it to the stream's `last-generated-id` before the first byte, so a reconnect never depends on `$`. |
| `if_missing` | `"404"`: a missing key answers 404. `"wait"`: a missing key is followed as empty, for feeds whose key appears with the first `XADD`. |
| `format` | Turns an entry into an event, or `None` to skip it. Default in A5. |
| `until` | Marks an entry as the stream's last for this reader: it is sent, then the response ends. A reconnect whose cursor is such an entry gets 204. Without `until`, a stream never ends. |
| `ticks` (`read()` only) | Also yield a `StreamTick` after every successful `XREAD BLOCK` that returned nothing, about once per `stream_heartbeat_interval` of quiet. A failed read yields no tick and resets `quiet_for` to zero, so an outage never counts as quiet. `sse()` turns ticks into `: ping`. |
| `skip` (`read()` only) | Entries the iterator doesn't yield. `sse()` skips through `format` returning `None`. |
| `deadline` | The response ends at this time, with no event (A7). An application passes the reader's credential expiry. |

**`sse()` is `read()` plus `respond()`.** It is exactly:

```python
cursor = self.cursor(request)
sub = await self.read(key, after=cursor, start=start, if_missing=if_missing,
                      until=until, deadline=deadline, ticks=True)
if sub.ended_by == "until":
    await sub.aclose()
    return Response(status_code=204)
return self.respond(frames(sub, format), request=request)
# frames(): an entry becomes format(entry) with id=entry.id, or an
# id-only SSEEvent when format returns None; a tick passes through.
# With start="last", an id-only frame with sub.cursor goes first.
```

An application that needs its own end condition, such as Part B's silence
check, writes its own `frames()` over `read()` and passes it to `respond()`.
The core has no notion of a stream going quiet: SSE has none, and a quiet
feed isn't over.

There is no `after=` parameter on `sse()`. The cursor comes from the request
(A4). The examples on #100 showed how easily a handler passes a stale query
value that beats a newer `Last-Event-ID`; not offering the parameter removes
that mistake. `read()` takes `after` because it has no request.

Setup and dependency:

```python
FastAPIRedis(app).lifespan().streams()     # Part A
# StreamsDep = Annotated[RedisStreams, Depends(get_streams)]
```

Example: a notification feed written by a worker in another process.

```python
@app.get("/users/{user_id}/notifications")
async def notifications(user_id: str, request: Request, streams: StreamsDep,
                        _: None = Depends(require_self)):
    return await streams.sse(
        streams.key("notify", user_id), request=request,
        start="last", if_missing="wait",
        deadline=session_expiry(request),
    )
```

## A4. Before the first byte

`sse()` does every check that decides the status code before it returns the
response, as today. In order:

1. **Cursor:** `Last-Event-ID`, then `?after=`. The header wins because the
   browser updates it on every event; the query is for a page that reloads
   and restores its last ID. Malformed or out of range: **422**.
   No cursor: from `start`.
2. **Deadline** already passed: **401**.
3. **Reader cap** (`stream_max_readers`) reached in this process: **503** with
   `Retry-After`, before any Redis call, so a reconnect storm costs Redis
   nothing.
4. **One transaction:** `TYPE`, `XINFO STREAM`, `XRANGE cursor cursor`
   (the cursor's own entry, for `until`), the first page
   (`XRANGE (cursor + COUNT page`). With `start="last"` the
   cursor isn't known when the transaction is queued: the two ranges are
   left out, the cursor becomes `last-generated-id`, and the first page is
   empty by construction. `TYPE` decides the key's state, so no error text
   is parsed; when it isn't `stream`, `XINFO`'s error is ignored. From the
   replies:
   - `TYPE` is `none`: **404**, or follow as empty with `if_missing="wait"`;
   - `TYPE` is anything other than `stream`: **500**, code
     `stream_wrong_type` (the application named the wrong key);
   - cursor above `last-generated-id`: **409** (another stream's cursor, a
     client bug, or a failover that lost writes);
   - the cursor's own entry matches `until`: **204**.


Every error body is `{"detail": ..., "code": ...}`. `EventSource` stops on any
non-200, including 204 and 503; the guide keeps its note that a client must
recreate `EventSource` itself to recover from a 503.

## A5. Entries to events

`format` returns the `data` and `event` of an SSE frame; the SDK adds `id:`.

Default `format`:

- An entry with a `data` field: `data` is that field, `event` is the `event`
  field if present.
- Any other entry: `data` is the JSON object of all fields, no `event`.

So `XADD key * event delta data '{"text":"hi"}'` becomes:

```
id: 1731330000000-0
event: delta
data: {"text":"hi"}
```

Injection rules stay as today (design section 9): the encoder rejects an
`event` with CR, LF or NUL, or longer than 128 characters (the entry is
skipped and logged, since the reader can't refuse a write it didn't make), and
splits `data` at every line break into several `data:` lines.

**The cursor is the last entry *read*, not the last one sent.** Two cases
would otherwise leave the client's `Last-Event-ID` behind the reader:

- Entries `format` skips (returns `None`). Without care, every reconnect
  replays them.
- `start="last"`: the resolved cursor is never sent, so a client that drops
  before the first event would resolve "last" again on reconnect and miss
  what arrived in between.

So the SDK sends **id-only frames**: `id: <entry ID>` followed by a blank
line, with no `data:`. Under the WHATWG parser this sets the `EventSource`'s
last event ID and dispatches no event. Fetch-based client libraries may not
follow that step: some likely deliver it as a `message` with empty `data`.
The guide's fetch example skips messages with empty data, and open question 3
tracks checking the common libraries. One goes out right after
`retry:` with the resolved cursor, and one after any chunk whose last entries
were skipped.

## A6. Trimmed history

**Trimmed or deleted entries.** The core doesn't detect them. A stream loses
entries only when the application asks for it (`maxlen` on `publish()`,
`XTRIM`, `MAXLEN` on its own `XADD`, `XDEL`), and for a trimmed stream
dropping old entries is the point. `XREAD` from a cursor already returns
what is still after it, so a client that reconnects with a cursor older than
the oldest entry continues from the oldest entry, and a client that falls
behind while connected skips what was trimmed meanwhile. The guide says so.
An application that must not skip doesn't trim, as Part B doesn't.

This drops today's 410 (`StreamExpired`), `_history_lost`, and the
`first-entry`/`entries-added`/`max-deleted-entry-id` comparisons. It isn't
part of SSE: `EventSource` treats a 410 like any other error and stops.

Minimal reader ACL: `+xread +xrange +xinfo|stream +type`, and
`+multi +exec` for the connect transaction.

## A7. The response, and how it ends

The body loop is today's `_pages()` without the run logic: no sequence
numbers, no gap events, no reader-written end, no hidden types.

- First frame `retry: <1000-3000 ms, random per connection>`.
- Each read's events go out as one chunk under `stream_send_timeout`.
- A `StreamTick` sends `: ping`. `XREAD BLOCK` is bounded by
  `stream_heartbeat_interval` and by the time left before the next deadline.
- `respond()` owns the framing, the send timeout, the longest connection
  time and shutdown; `read()` owns the reader slot, the deadline and the
  retry window. Closing the response closes the subscription.
- Id-only frames keep the client's cursor at the last entry read (A5).

The response ends:

| When | Wire | Browser |
|---|---|---|
| An entry matches `until` | that event | reconnect gets 204 and stops, or the client closes |
| The items iterator ends (an application's own `frames()`) | whatever it yielded last | reconnects, unless the client closes on that event |
| `deadline` | nothing | reconnect hits the app's dependency (401, or a fresh credential) |
| `stream_max_connection_duration` | nothing | reconnects and resumes |
| Redis down past `stream_read_retry_window` | nothing | reconnects and resumes |
| App shutting down | nothing | reconnects, to another process |
| Client too slow (`stream_send_timeout`) | nothing | a live client reconnects |

There is no `credential_expired` event. `deadline` closes the connection and
the application's own dependency answers the reconnect. The trade-off: an
`EventSource` client can't tell why it stopped. A fetch-based client sees the
401.

## A8. `publish()`

A thin `XADD` in the default field layout, for applications that don't
already have a producer:

- `XADD key [MAXLEN ~ n] * <fields>`; with `ttl`, `EXPIRE key ttl` in the same
  `MULTI`.
- No retry on an ambiguous failure (a timeout after sending): a retried `XADD`
  without `IDMP` could store the event twice, and readers would deliver both.
  The error reaches the caller, which decides.
- Usable from any process with a client; it isn't tied to the FastAPI app.

## A9. Settings, errors, requirements

Settings: the reader-side settings keep their current names and meaning, so
nothing changes for operators:

`stream_heartbeat_interval`, `stream_replay_page`, `stream_max_readers`,
`stream_max_connection_duration`, `stream_read_retry_window`,
`stream_send_timeout`.

The startup checks stay: `socket_timeout` above the heartbeat interval, and
`max_connections` above `stream_max_readers` (each live reader holds a
connection during its `BLOCK`). The guide says that on Cluster this bound is
per node.

Errors: `StreamError` (base), `StreamNotFound` 404, `StreamCursorAhead` 409, `StreamInvalidCursor` 422,
`StreamInvalidName` 422 (from `key()`), `StreamCredentialExpired` 401,
`StreamStoreError` 503, `StreamConfigurationError`. `StreamUnsupported` and
`StreamFinished` leave the core.

`key()` parts: 1-200 characters of `A-Z a-z 0-9 _ . -`. No `:` inside a part,
so different parts can never join into the same key. No braces, so no part
can set a Cluster hash tag. Part B builds its own keys with its own rules
(B2).

Redis: 7.4+, the SDK's floor. Every command used (`XREAD`, `XRANGE`,
`XINFO STREAM` fields from 7.0, `XADD`, `EXPIRE`)
exists there. No
probe, no feature switch: the 7.4 CI entry runs the stream tests too.
Cluster: one key per read and per transaction. Sentinel: the SDK's pool.

## A10. What carries over, what goes

Carries over from `streams.py` / `stream_backend.py`, mostly unchanged:
`parse_cursor`, `select_cursor`, `encode_frame`, `_SSEResponse` (as
`respond()`: send timeout, longest connection; the reader slot moves to
`read()`), `_prepare` (minus the run checks and the 410), `_pages` (minus the run
logic), the reader cap,
retry window and shutdown handling, the exception handler.

Goes from the core: `StreamWriter`, `open`, `start`, `spawn`, `cancel`,
`finished`, `follow` (replaced by `read`), `_history_lost` and
`StreamExpired` (A6), `probe_streams_support`,
`StreamUnsupported`, `IDMP_WINDOW`, `XCFGSET`, the `t`/`d`/`q`/`f` layout,
`stream.gap`, `stream.error`, reader-written ends.

## A11. Tests (real Redis)

- Replay then live, one loop; reconnect with `Last-Event-ID` gets exactly the
  rest; `?after=` used only without the header.
- `start="last"`: a client that drops before its first event and reconnects
  gets what arrived in between (the id-only frame).
- 404, `if_missing="wait"`, wrong key type, 409, 422.
- Trimmed history: a reconnect with a cursor older than the oldest entry gets
  200 and continues from the oldest entry; an `XDEL`ed entry after the cursor
  is not sent.
- `format` default and custom, skipped entries with an id-only frame after
  them, a reconnect that doesn't replay skipped entries, injection-safe
  encoding.
- `until`: the event is sent, then 204 on reconnect.
- Ticks: one per empty read, `quiet_for` growing across them, reset by an
  entry and by a failed read; none during an outage.
- `respond()` over an application's own iterator: framing, id-only frames,
  ticks as `: ping`, the iterator closed on client disconnect.
- Reading on a replica and under a user with only the minimal reader ACL.
- Reader cap (503 before Redis), slot freed on every exit, send timeout,
  `deadline`, max connection duration, Redis restart within and past the
  retry window, shutdown.
- Cluster and Sentinel topologies, as `test_stream_topologies.py` does now.
- Redis 7.4 and 8.8 in CI.

## A12. Later

- **Shared tail.** Many readers on one hot stream in one process share one
  `XREAD`, instead of one connection each. The biggest scaling gain for feeds.
- **Multiplexed reads.** One `XREAD` over many keys (same slot on Cluster).
- **WebSocket**, on `read()`.
- `XREAD ... MAXSIZE` (8.10) to bound a page by bytes, when the floor allows.

---

# Part B: agent tasks on the A2A model, an optional helper on the core

Part B uses the data model of [A2A v1.0](https://a2a-protocol.org/latest/specification/)
(Agent2Agent): tasks, contexts, task states, status and artifact updates,
messages and parts. It does not implement A2A's endpoints, Agent Card or
push notifications; the application keeps its own routes and authorization,
as with Part A. Event payloads use A2A's JSON shapes, so an A2A client
library can parse them, and a later A2A server binding can reuse the same
storage (B9).

## B1. Goals and non-goals

Goals:

1. One agent task streams through Redis and survives a disconnect, a reload
   and a second device (all from Part A).
2. A retried send (same `messageId`) doesn't start a second producer.
3. A cancel ends the task for readers at once, and stops the producer even if
   it runs in another process.
4. Every task reaches one visible state, and a terminal state never changes.
5. A task can pause for input (`TASK_STATE_INPUT_REQUIRED`,
   `TASK_STATE_AUTH_REQUIRED`) and continue on the next message.
6. A dead producer is detected without readers writing to Redis.
7. A task's status is O(1) to read.
8. Redis 7.4+.

Non-goals: the agent runtime, A2A's HTTP/JSON-RPC/gRPC bindings, the Agent
Card, push notifications, `ListTasks` (B9), durable storage past the task's
lifetime, producers in worker processes (the producer runs in the FastAPI
app, as today).

## B2. Model and terms

| A2A | Here |
|---|---|
| Task | One unit of agent work: a stream and a status hash. `taskId` is generated by the SDK (A2A doesn't let clients pick it). |
| `contextId` | Groups the tasks of one conversation. The application passes one (a chat ID) or the SDK generates one. It is not a credential: the application authorizes before every call, as in Part A. Only `send()` needs it; every other call finds the task from its `taskId` (B3). |
| Message | A turn with `role` `ROLE_USER` or `ROLE_AGENT`, a client-chosen `messageId`, and `parts`. |
| Part | Exactly one of `text`, `data` (any JSON), `url`, `raw` (base64 bytes), plus optional `mediaType`, `filename`, `metadata`. |
| Artifact | An output of the task (`artifactId`, `name`, `parts`). Streamed in chunks with `append` and `lastChunk`. |
| TaskStatus | `state`, optional `message`, `timestamp`. |
| TaskState | `SUBMITTED`, `WORKING`, `INPUT_REQUIRED`, `AUTH_REQUIRED` (interrupted), `COMPLETED`, `FAILED`, `CANCELED`, `REJECTED` (terminal), all with the `TASK_STATE_` prefix. |
| StreamResponse | What a reader receives: exactly one of `task`, `message`, `statusUpdate`, `artifactUpdate`. |

Python types mirror these shapes (dataclasses with A2A's camelCase JSON), so
the wire format is A2A's. Replacing them with the `a2a-sdk` models is B9.

What replaces what from the run design:

| Runs | Tasks |
|---|---|
| run, `run_id` (client-chosen) | task, `taskId` (SDK-generated) |
| `scope` | `contextId` |
| start token | the `messageId`, stored with the attempt token (B4) |
| `emit(type, data)` with free event types | `artifact()`, `message()`, `status()` on the writer |
| `run.end {"reason": done/error/cancelled}` | a terminal `statusUpdate`: `COMPLETED`, `FAILED`, `CANCELED`, `REJECTED` |
| `lost` | a reader-made `statusUpdate` `FAILED`, marked in `metadata` (B4) |
| end key (string) | status hash (current state, and the terminal latch) |

## B3. Keys and entries

A context's keys share one Cluster hash tag, so the send script can create a
task and record its `messageId` atomically. The tag is the first 8 hex
characters of `sha1(contextId)`, and the SDK puts it at the front of every
task ID it generates:

```
tag    = sha1(contextId).hexdigest()[:8]       # e.g. "3f9a0c1e"
taskId = f"{tag}-{uuid4().hex}"                # e.g. "3f9a0c1e-5b2d…"
```

So the keys of a task can be rebuilt from its `taskId` alone, as A2A's
`GetTask(taskId)` expects:

| Key | Type | Content |
|---|---|---|
| `<prefix>:a2a:{<tag>}:task:<taskId>` | Stream | The task's events. |
| `<prefix>:a2a:{<tag>}:task:<taskId>:status` | Hash | `state`, `status` (the current TaskStatus JSON), `owner` (B4) and `context` (the `contextId`). |
| `<prefix>:a2a:{<tag>}:msg:<contextId>:<messageId>` | String | `<taskId> <attempt token>`: the send that took this `messageId` in this context. |

The hash keeps task IDs short and the same length, makes the tag readable
without separator rules, and doesn't put the context ID (a chat ID, perhaps
derived from a user) into every task ID that reaches clients and logs. Two
contexts whose tags collide just share a slot; their keys still differ. A2A
treats task IDs as opaque, so the prefix is allowed.

The stream and the status hash expire together. A `msg` key lives up to the
task's maximum lifetime, so it can outlive a task that ended early (B4). A hot conversation puts its
tasks on one node; that is the price of the atomic send.

Names: a `contextId` is 1-128 of `A-Z a-z 0-9 _ . -` (a UUID fits), a
`messageId` 1-128 of the same. A `taskId` must match `[0-9a-f]{8}-[0-9a-f]{32}`;
anything else is `TaskInvalidId` (422) before any Redis call. No `:` and no
braces, so keys can't collide and no ID can set a hash tag.

Entries use Part A's layout, plus one field the reader can test without
parsing JSON:

| `event` | Extra field | `data` | Seen by clients |
|---|---|---|---|
| `task` | | `{"task": Task}`: the `SUBMITTED` task, with the user's message in `history` | yes, first |
| `message` | | `{"message": Message}`: a user message (a continuation) or an agent message | yes |
| `artifact-update` | | `{"artifactUpdate": ...}` | yes |
| `status-update` | `state` | `{"statusUpdate": ...}` | yes |
| `ping` | | empty | no |

`data` is the A2A `StreamResponse` JSON for the REST binding, so a client
parses one shape. Every event carries `taskId` and `contextId`, as A2A
requires.

## B4. Lifecycle

**States.** The scripts below are the only writers of `state`:

```
SUBMITTED ─▶ WORKING ─▶ COMPLETED | FAILED | CANCELED | REJECTED
               ▲  │
               │  ▼
   INPUT_REQUIRED | AUTH_REQUIRED      (no producer runs while interrupted)
```

A cancel is allowed from any non-terminal state. Nothing leaves a terminal
state. `task_max_lifetime` bounds the whole task, waits for input included;
a continuation doesn't extend it. A task that reaches it simply expires: its
keys go, `get_task()` returns `None` and readers end (B6), with no terminal
state recorded.

**Ownership.** Every `send()` call draws an attempt token. The status hash
keeps the token of the attempt whose producer currently owns the task
(`owner`), and every producer write names it. A producer from an earlier
attempt (one that went interrupted, then lost a reply) can't write over the
attempt that continued the task: its writes are refused, and it stops.
`cancel()` names no owner and is accepted from anyone.

**Send, a new task.** One script over the `msg`, stream and status keys (one
slot):

```lua
-- KEYS[1] = msg key, KEYS[2] = stream, KEYS[3] = status
-- ARGV[1] = "<taskId> <token>", ARGV[2] = token,
-- ARGV[3] = the task event JSON (SUBMITTED), ARGV[4] = WORKING status JSON,
-- ARGV[5] = max lifetime (ms), ARGV[6] = the WORKING status-update event JSON,
-- ARGV[7] = contextId
local taken = redis.call('GET', KEYS[1])
if taken then return {'taken', taken} end
redis.call('XADD', KEYS[2], '*', 'event', 'task', 'data', ARGV[3])
redis.call('XADD', KEYS[2], '*', 'event', 'status-update',
           'state', 'TASK_STATE_WORKING', 'data', ARGV[6])
redis.call('HSET', KEYS[3], 'state', 'TASK_STATE_WORKING', 'status', ARGV[4],
           'owner', ARGV[2], 'context', ARGV[7])
redis.call('PEXPIRE', KEYS[2], ARGV[5])
redis.call('PEXPIRE', KEYS[3], ARGV[5])
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[5])
return {'created', ARGV[1]}
```

**Send, a continuation** (`task_id` set):

```lua
-- KEYS as above; ARGV[1] = "<taskId> <token>", ARGV[2] = token,
-- ARGV[3] = the user's message event JSON, ARGV[4] = WORKING status JSON,
-- ARGV[5] = the WORKING status-update event JSON, ARGV[6] = taskId,
-- ARGV[7] = contextId
local taken = redis.call('GET', KEYS[1])
if taken then
  if string.sub(taken, 1, #ARGV[6] + 1) ~= ARGV[6] .. ' ' then return {'conflict'} end
  return {'taken', taken}
end
if redis.call('EXISTS', KEYS[2]) == 0 then return {'missing'} end
if redis.call('HGET', KEYS[3], 'context') ~= ARGV[7] then return {'missing'} end
local state = redis.call('HGET', KEYS[3], 'state')
if state ~= 'TASK_STATE_INPUT_REQUIRED' and state ~= 'TASK_STATE_AUTH_REQUIRED' then
  return {'busy', state}
end
redis.call('XADD', KEYS[2], '*', 'event', 'message', 'data', ARGV[3])
redis.call('XADD', KEYS[2], '*', 'event', 'status-update',
           'state', 'TASK_STATE_WORKING', 'data', ARGV[5])
redis.call('HSET', KEYS[3], 'state', 'TASK_STATE_WORKING', 'status', ARGV[4],
           'owner', ARGV[2])
redis.call('SET', KEYS[1], ARGV[1], 'PX', redis.call('PTTL', KEYS[2]))
return {'created', ARGV[1]}
```

Replies, for both:

- `created`, or `taken` holding this call's own token (the SDK retried the
  script after losing its reply): created, so exactly one producer starts.
- `taken` with another token: a retried request with the same `messageId`.
  The SDK then checks that the task's stream still exists (`EXISTS`; the `msg`
  key can outlive a task that ended early, see below) and returns the
  existing `taskId` with `created=False`, or `TaskNotFound`.
- `conflict`: the `messageId` already belongs to another task in this
  context. `TaskMessageConflict` (409).
- `missing`: `TaskNotFound`. `busy`: `TaskNotInterrupted` (409), for a task
  that is working or has ended.

`send()` also normalizes the message before the script: it sets the message's
`contextId` and `taskId` to the ones the task is stored under, and rejects a
message whose own IDs disagree (422). The `msg` key lives up to the task's
maximum lifetime, so a `messageId` is deduplicated for that long; a task that
ends early expires sooner (`task_done_ttl`), which is why a `taken` reply is
followed by the `EXISTS` check.

Shutdown and ambiguous failures keep the run design's rules: `send()` counts
as an in-flight start; one that created the task and finds shutdown in
progress writes `CANCELED`; one whose script fails ambiguously past the
retries makes one best-effort `FAILED` transition, as this attempt's owner,
before raising.

Both send scripts leave the task `WORKING` under the new owner, so the
producer starts `fn` at once; a cancel that lands before its first write
makes that write raise `TaskEnded`. The task entry still records the task as
`SUBMITTED`, followed by the `WORKING` update, as A2A's sequence goes.

**Transitions.** One script writes every later status change: the writer's
`status()`, an interrupt, every end, and cancel:

```lua
-- KEYS[1] = stream, KEYS[2] = status
-- ARGV[1] = new state, ARGV[2] = status JSON, ARGV[3] = event JSON,
-- ARGV[4] = owner token ("" for cancel), ARGV[5] = done TTL (ms)
local TERMINAL = {TASK_STATE_COMPLETED=1, TASK_STATE_FAILED=1,
                  TASK_STATE_CANCELED=1, TASK_STATE_REJECTED=1}
if redis.call('EXISTS', KEYS[1]) == 0 then return {'missing'} end
local cur = redis.call('HMGET', KEYS[2], 'state', 'owner')
if TERMINAL[cur[1]] then return {'terminal', cur[1]} end
if ARGV[4] ~= '' and cur[2] ~= ARGV[4] then return {'not_owner', cur[1]} end
if cur[1] == ARGV[1] and ARGV[1] ~= 'TASK_STATE_WORKING' then
  return {'applied', cur[1]}                     -- a retry of this change
end
redis.call('XADD', KEYS[1], '*', 'event', 'status-update', 'state', ARGV[1], 'data', ARGV[3])
redis.call('HSET', KEYS[2], 'state', ARGV[1], 'status', ARGV[2])
if TERMINAL[ARGV[1]] then
  redis.call('PEXPIRE', KEYS[1], ARGV[5], 'LT')
  redis.call('PEXPIRE', KEYS[2], redis.call('PTTL', KEYS[1]))
end
return {'applied', ARGV[1]}
```

- The script decides what is terminal, so no caller can end a task without
  `task_done_ttl` applying.
- `terminal`: `cancel()` raises `TaskNotCancelable` and nothing is written;
  a producer stops. `not_owner`: the producer stops and writes nothing more.
  `missing`: `TaskExpired` inside `fn`, `TaskNotFound` from `cancel()`.
- An interrupt or an end is retried on an ambiguous failure. A retried
  interrupt finds the state already set by its own owner and gets `applied`
  without writing again. A retried end gets `terminal` with its own target
  state, which the caller counts as success, as `cancel()` does. A `status()`
  progress update (`WORKING` to `WORKING`) is not retried, since its retry
  would add an entry.
- As in the run design: the first terminal state wins and stays, repeated
  cancels add nothing and never extend the lifetime (`LT`), and the status
  hash expires with the stream.

**Writer.** `fn(task: TaskWriter, request: TaskRequest)`. `TaskRequest` holds
`task_id`, `context_id`, the user's `message`, and whether this is a
continuation. The writer:

```python
class TaskWriter:
    task_id: str
    context_id: str
    async def artifact(self, artifact_id: str, parts: list[Part], *,
                       name: str | None = None, append: bool = False,
                       last_chunk: bool = False) -> str: ...
    async def message(self, parts: list[Part]) -> str: ...          # ROLE_AGENT
    async def status(self, message: list[Part] | None = None) -> str: ...
        # a WORKING update with a message: progress, a tool call
    def require_input(self, message: list[Part]) -> None: ...      # also require_auth()
    def reject(self, message: list[Part]) -> None: ...
```

- `artifact()`, `message()` and the keepalive's pings go through one small
  script that checks before it writes, so nothing lands after a terminal or
  interrupted state, or from another attempt:

  ```lua
  -- KEYS[1] = stream, KEYS[2] = status; ARGV[1] = owner, ARGV[2] = event, ARGV[3] = data
  local cur = redis.call('HMGET', KEYS[2], 'state', 'owner')
  if not cur[1] then return {'missing'} end
  if cur[1] ~= 'TASK_STATE_WORKING' or cur[2] ~= ARGV[1] then
    return {'ended'}
  end
  return {'ok', redis.call('XADD', KEYS[1], 'NOMKSTREAM', '*',
                           'event', ARGV[2], 'data', ARGV[3])}
  ```

  `ended` raises `TaskEnded`, which unwinds `fn` (a cancel from another
  process, or another attempt now owns the task); `missing` raises
  `TaskExpired`. No retry on a driver error, as in the run design: a retried
  `XADD` could duplicate a chunk.
- `status()` runs the transition script with `WORKING`.
- How `fn` finishes sets the state: return → `COMPLETED`; return after
  `require_input()` / `require_auth()` → that interrupted state; after
  `reject()` → `REJECTED`; an exception → `FAILED`, with a generic message
  and `metadata.code`; cancelled → `CANCELED`.
- Limits: `task_max_events` entries and `task_max_event_bytes` per entry,
  as today.

**Producer task.** As in the run design: `send()` spawns `fn` in a set the
helper owns, with a keepalive that wakes every `stream_heartbeat_interval`:

- `HMGET status state owner`. Terminal, another owner, or no status at all
  (the task expired): cancel the task, so a cancel from another process stops
  the producer within a heartbeat. An expired task's end transition then gets
  `missing` and writes nothing.
- Once `task_silence_timeout / 3` has passed since the last write, a ping on
  every wake; after a failed write, a ping every second until one lands.
  Wakes are a heartbeat apart, so the longest gap a live producer leaves is
  `T/3 + H`, at most two thirds of the timeout when `T >= 3H`.

The keepalive is stopped, and its in-flight ping awaited, before the
producer's last transition, so it can't cancel the task's own end and no ping
lands after it. While a task is interrupted, no producer runs; the next
message's `send()` starts one, under a new owner.

**Cancel.** `cancel()` runs the transition script with `CANCELED` and no
owner. `missing`: `TaskNotFound`. `terminal`: `TaskNotCancelable` (A2A's
`TaskNotCancelableError`). A cancel whose reply is lost and is retried gets
`terminal` with `CANCELED`, and the SDK treats that as its own success.

**Lost producer.** As in the run design, each reader decides on its own
successful reads, and nobody writes. A task is lost to a reader when its
state, as that reader last saw it, is `SUBMITTED` or `WORKING` and a
`StreamTick` arrives with `quiet_for >= task_silence_timeout`. The reader then
sends, with no `id:`,

```json
{"statusUpdate": {"taskId": "…", "contextId": "…",
  "status": {"state": "TASK_STATE_FAILED", "timestamp": "…",
             "message": {"messageId": "lost-<taskId>", "taskId": "…", "contextId": "…",
                         "role": "ROLE_AGENT", "parts": [{"text": "The agent stopped responding."}]}},
  "metadata": {"redisFastapi": {"lost": true}}}}
```

and ends. The `messageId` is derived from the task, so every reader and
reconnect reports the same message. A2A has no "lost" state; `FAILED` with a
marker is the closest mapping, and `get_task()` reports the same.

A lost report is provisional, which the guide says: it is not stored, and a
producer that was only stalled (an event loop blocked past the timeout) can
still write a real terminal state later, which `get_task()` and new readers
then show. A client that doesn't close on the lost update reconnects, replays,
and hears it again after another timeout. An interrupted task is never lost:
it has no producer by design.

**Status.** `get_task()` is one read-only script (`EVAL_RO`, both keys in one
slot) returning `HGETALL status`, the stream's last entry ID and Redis's
`TIME`. It returns a `Task` with `id`, `contextId` and `status`; a `SUBMITTED`
or `WORKING` task whose last entry is older than the silence timeout comes
back `FAILED` with the lost marker. `include_artifacts=True` or a
`history_length` also folds the stream (`XRANGE`, page by page, up to the
first terminal entry): artifacts by `artifactId`, a chunk with `append`
added to the artifact's parts, one without it replacing them; `history`
from the `task` and `message` entries, newest last, cut to `history_length`.
That part is O(entries), and only runs when asked for.

**Shutdown.** As in the run design: refuse new sends, wait for in-flight
ones, yield once, cancel the producers, give them the grace period to write
`CANCELED`, close the pool.

## B5. API

```python
FastAPIRedis(app).lifespan().streams().tasks()
# TasksDep = Annotated[Tasks, Depends(get_tasks)]

class Tasks:
    async def send(self, fn: Callable[[TaskWriter, TaskRequest], Awaitable[None]],
                   message: Message, *, context_id: str | None = None,
                   task_id: str | None = None) -> SendResult: ...
        # task_id=None: a new task. task_id set: continue an interrupted one.
    async def get_task(self, task_id: str, *, context_id: str | None = None,
                       include_artifacts: bool = False,
                       history_length: int | None = 0) -> Task | None: ...
    async def cancel(self, task_id: str, *, context_id: str | None = None) -> Task: ...
    async def sse(self, task_id: str, *, context_id: str | None = None, request: Request,
                  deadline: datetime | None = None) -> Response: ...
    async def events(self, task_id: str, *, context_id: str | None = None,
                     after: str | None = None, deadline: datetime | None = None,
                     stop_at_interrupt: bool = False) -> AsyncIterator[StreamResponse]: ...
        # read(ticks=True) through the same task_items() as sse() (B6), lost update included

@dataclass(frozen=True)
class SendResult:
    task_id: str
    context_id: str
    created: bool          # False: a retried send; nothing was spawned
```

Only `send()` needs a context: a new task's `context_id` (or one the SDK
generates), and a continuation's from `context_id` or the message's own
`contextId`, because the `msg` key is per context. A continuation in the
wrong context gets `TaskNotFound`. Every other call finds the task from its `task_id`,
as in A2A. The optional `context_id` is a check for applications that
authorize by conversation: the SDK compares it with the task's stored
`context` and answers `TaskNotFound` on a mismatch, so a task in another
conversation looks the same as no task. Without it, the returned `Task`
carries its `contextId` for the application to check. A `task_id` whose tag
doesn't match the given `context_id` fails the same way without touching
Redis.

Errors: `TaskNotFound` (404), `TaskNotCancelable` (409),
`TaskNotInterrupted` (409), `TaskMessageConflict` (409), `TaskInvalidId` and a
message whose IDs disagree with the call (422), plus Part A's. Inside `fn`:
`TaskEnded`, `TaskExpired`. A2A's own
error codes belong to an A2A binding (B9).

A2A's `SendStreamingMessage` is `send()` then `sse()` in one handler, and
`SubscribeToTask` is `sse()`. `SendMessage` in its blocking form is `send()`
then `events(stop_at_interrupt=True)` until it ends.

## B6. How it uses the core

`Tasks.sse()` is Part A's `read()` and `respond()` with its own `frames()`:

```python
TERMINAL = {"TASK_STATE_COMPLETED", "TASK_STATE_FAILED",
            "TASK_STATE_CANCELED", "TASK_STATE_REJECTED"}
LIVE = {"TASK_STATE_SUBMITTED", "TASK_STATE_WORKING"}

async def task_items(sub, state):
    """Typed items, shared by sse() and events()."""
    async for item in sub:
        if isinstance(item, StreamTick):
            expired_check = state not in LIVE or item.quiet_for >= settings.task_silence_timeout
            if expired_check and not await exists(key):
                return                                     # the task expired
            if state in LIVE and item.quiet_for >= settings.task_silence_timeout:
                yield Lost(lost_update(task_id, context_id))
                return
            yield Quiet()
        elif item.fields["event"] == "ping":
            yield Hidden(item.id)
        else:
            state = item.fields.get("state", state)
            yield Event(item.id, item.fields["data"], state)

async def frames(sub, state):                              # for sse()
    async for it in task_items(sub, state):
        match it:
            case Quiet():      yield StreamTick(0)         # ": ping"
            case Hidden(id):   yield SSEEvent(data=None, id=id)   # id-only
            case Lost(data):   yield SSEEvent(data=data)          # no id
            case Event(id, data, _): yield SSEEvent(data=data, id=id)  # no "event:"

sub = await streams.read(key, after=streams.cursor(request), ticks=True,
                         until=lambda e: e.fields.get("state") in TERMINAL,
                         deadline=deadline)       # 422, 401, 503, 404 as in A4
if sub.ended_by == "until":
    await sub.aclose()
    return Response(status_code=204)
state, context_id = await status_of(key)         # HMGET status state context
# a context_id passed to sse() that differs: TaskNotFound (404)
return streams.respond(frames(sub, state), request=request)
```

- No `event:` line: A2A's SSE uses only `data:`, so every frame is a
  `message` event and the client looks at which key the JSON has.
- `id:` on every frame is an addition to A2A, which has no resume: A2A
  clients ignore it, and `EventSource` resumes exactly.
- The response ends at a terminal state. It stays open at an interrupted
  state, where A2A closes the stream: `EventSource` would reconnect at once
  anyway, and staying open lets the continuation arrive on the same
  connection. An interrupted task is never reported lost.
- `read()` runs first, so Part A's checks keep their order and the reader
  cap still answers before any Redis call. The state read after it is only a
  starting point: the replay runs to the stream's tip and updates it, so a
  transition between the two reads is caught.
- A task that reaches `task_max_lifetime` ends the response silently, and
  the reconnect gets 404. An interrupted task's reader checks `EXISTS` on
  every tick; a live task's reader checks once, on the tick that would report
  it lost, so an expired task is never reported as a dead producer.
- `events()` runs the same `task_items()` and yields parsed `StreamResponse`s
  for `Event` and `Lost`, skipping `Quiet` and `Hidden`; with
  `stop_at_interrupt`, it stops after an `Event` whose state is interrupted.
- A subscription with no cursor replays the task from its `task` entry,
  where A2A sends one snapshot of the current task. The information is the
  same; folding it into a snapshot is open question 6.

Nothing in Part A knows about tasks.

## B7. Example

```python
@app.post("/contexts/{context_id}/messages",
          dependencies=[Depends(require_context_access)])
async def send(context_id: str, body: Message, tasks: TasksDep):
    result = await tasks.send(triage, body, context_id=context_id,
                              task_id=body.task_id)
    return {"taskId": result.task_id, "contextId": result.context_id}

@app.get("/contexts/{context_id}/tasks/{task_id}/events",
         dependencies=[Depends(require_context_access)])
async def events(context_id: str, task_id: str, request: Request, tasks: TasksDep):
    return await tasks.sse(task_id, context_id=context_id, request=request,
                           deadline=session_expiry(request))

@app.get("/contexts/{context_id}/tasks/{task_id}",
         dependencies=[Depends(require_context_access)])
async def get(context_id: str, task_id: str, tasks: TasksDep):
    return await tasks.get_task(task_id, context_id=context_id,
                                include_artifacts=True)

@app.post("/contexts/{context_id}/tasks/{task_id}:cancel",
          dependencies=[Depends(require_context_access)])
async def cancel(context_id: str, task_id: str, tasks: TasksDep):
    return await tasks.cancel(task_id, context_id=context_id)

async def triage(task: TaskWriter, request: TaskRequest) -> None:
    first = True
    async for chunk in model.stream(request.message.text()):
        await task.artifact("answer", [Part(text=chunk)], append=not first)
        first = False
    await task.artifact("answer", [Part(text="")], append=True, last_chunk=True)
```

Client:

```js
const es = new EventSource(`/contexts/${ctx}/tasks/${task}/events`);
es.onmessage = (e) => {
  const ev = JSON.parse(e.data);
  if (ev.artifactUpdate) append(ev.artifactUpdate.artifact.parts);
  if (ev.statusUpdate) {
    const s = ev.statusUpdate.status.state;
    if (s === "TASK_STATE_INPUT_REQUIRED") askUser(ev.statusUpdate.status.message);
    if (TERMINAL.has(s)) { es.close(); showOutcome(ev.statusUpdate.status); }
  }
};
```

## B8. Settings

Renamed, since #100 isn't on `main`: `task_max_lifetime` (was
`stream_max_run`), `task_done_ttl`, `task_max_events`, `task_max_event_bytes`,
`task_silence_timeout` (at least three heartbeat intervals, checked at
startup; B4). No upper
bound tied to `IDMP`.

## B9. Later

- **`ListTasks`**: a sorted set per context, scored by status timestamp,
  kept by the two scripts.
- **An A2A server binding**: the REST and JSON-RPC endpoints, error codes and
  Agent Card on top of `Tasks`. JSON-RPC wraps each event in an envelope
  carrying the request's `id`, which `frames()` can add per connection.
- **`a2a-sdk` models and interfaces**: use its types instead of ours, or
  implement its task-store and event-queue interfaces on these keys, once
  checked against A2A v1.0.
- **Push notifications.**

## B10. Tests (real Redis)

- Send: a new task gets `created=True` and one producer; a retried send with
  the same `messageId` gets the same `taskId`, `created=False`, and spawns
  nothing; a script reply lost and retried by the SDK still counts as
  created.
- Continue: a message to an `INPUT_REQUIRED` task starts a producer and the
  task goes `WORKING`; to a `WORKING` or ended task, `TaskNotInterrupted`;
  to an unknown one, `TaskNotFound`; with a `messageId` another task already
  used, `TaskMessageConflict`; with IDs that disagree with the call, 422.
- Ownership: a producer from an earlier attempt that retries its interrupt,
  or writes `FAILED`, after a continuation started a new producer is refused
  and stops; the new producer's task is untouched.
- States: return, `require_input()`, `require_auth()`, `reject()`, an
  exception and a cancel each leave the expected state; a terminal state
  never changes, and racing ends leave the first one.
- Cancel: of a terminal task, `TaskNotCancelable` and no entry; of an
  interrupted one, `CANCELED`; from another process, the producer stops within
  a heartbeat, and its next artifact or ping is refused and never lands after
  the `CANCELED` entry; a cancel retried after a lost reply succeeds.
- Retried interrupts and ends add no second entry; a retried `status()` isn't
  attempted.
- Repeated cancels and ends near `task_max_lifetime` don't extend the TTL; the
  stream and status hash expire together; a duplicate `messageId` after an
  early end gets `TaskNotFound`, not a `taskId` with no task.
- Expiry while interrupted: a connected reader's response ends, and the
  reconnect gets 404.
- SSE: frames carry no `event:` line and parse as A2A `StreamResponse`s; pings
  are id-only; the response ends at a terminal state and stays open across
  `INPUT_REQUIRED` into the continuation; a reconnect at the terminal event
  gets 204.
- Lost producer: a killed producer gives every reader, and `events()`, a
  `FAILED` update with the lost marker and the same `messageId` after the
  silence timeout, and no reader writes; an interrupted task never does; a
  Redis outage longer than the timeout doesn't; a healthy producer at a
  timeout of three heartbeats is never reported lost.
- `get_task()`: one script by default; with artifacts, chunks joined in
  order and nothing after the terminal entry; with `history_length`, the
  newest messages.
- Shutdown: running producers end `CANCELED`; a send that overlaps shutdown
  creates nothing or ends `CANCELED`.
- Task IDs: `get_task()`, `cancel()`, `sse()` and `events()` work from the
  task ID alone; a wrong `context_id` answers `TaskNotFound` (with a matching
  tag, after reading the stored context; with another tag, before any Redis
  call); a malformed task ID answers 422 before any Redis call; a
  continuation in the wrong context answers `TaskNotFound`.
- Redis 7.4 and 8.8; Cluster (a context's keys in one slot, colliding tags
  sharing a slot without interference) and Sentinel.

---

## 3. Open questions

1. **Retrying a write.** No retry means a network blip ends a task with
   `FAILED`. On Redis 8.6.2+ `XADD ... IDMP` would make retries safe. Worth a
   second write path, or is failing the task honest enough?
2. **`deadline` without an event.** Fine for fetch clients; `EventSource`
   clients can't tell why they stopped. Is that acceptable?
3. **Id-only frames (A5).** They rely on the WHATWG parser setting the last
   event ID before it drops an event with no data. Browsers follow that;
   fetch-based client libraries should be checked (`@microsoft/fetch-event-source`,
   the AI SDK).
4. **Naming.** `RedisStreams`/`StreamsDep` and `Tasks`/`TasksDep`, or keep
   `StreamBackend` for the core to reduce churn on #100?
5. **Migration.** #100 isn't on `main`, so nothing public breaks. The agent
   SDK examples and the incident-triage app move to Part B and to A2A's event
   shapes (a delta becomes an `artifactUpdate` with `append`); the guide
   splits into "Streaming a Redis Stream over SSE" and "Agent tasks".
6. **Snapshot on subscribe.** A2A starts a subscription with one `Task`
   snapshot of the current state. Part B replays the task's events instead,
   which carries the same information and costs nothing extra. Folding the
   replay into a snapshot (B4's `get_task()` fold, sent with the last folded
   entry's ID) would shorten long replays and match A2A; is it worth it?
7. **Interrupted states and A2A.** Part B keeps the response open at
   `INPUT_REQUIRED`, where A2A closes it (B6). Right for our API; an A2A
   binding (B9) would close it.
