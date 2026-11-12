# Redis Streams over SSE, and agent runs on top: proposed design

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
- **Part B, agent runs:** an optional helper for "one agent turn, streamed,
  resumable, cancellable". It is built only on Part A's public API plus its own
  writes, and also works on Redis 7.4+.

## 2. Terms

| Term | Meaning |
|---|---|
| stream | A Redis Stream key. In Part A, any key the application names. |
| entry | One stream entry: an ID (`<ms>-<seq>`) and a field map. |
| cursor | An entry ID. The SSE `id:` of each event, and what the browser sends back as `Last-Event-ID`. |
| reader | One SSE response (or one `read()` iterator) following a stream. |
| run | Part B only: one agent turn, stored as one stream plus one small end key. |

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

# Part B: agent runs, an optional helper on the core

## B1. Goals and non-goals

Goals:

1. One agent turn streams through Redis and survives a disconnect, a reload and
   a second device (all from Part A).
2. A retried start request doesn't start a second producer.
3. A stop request ends the run for readers at once, and stops the producer
   even if it runs in another process.
4. Every run ends with one visible outcome: `done`, `error`, `cancelled` or
   `lost`.
5. A dead producer is detected without readers writing to Redis.
6. Run state is O(1) to read.
7. Redis 7.4+.

Non-goals: the agent runtime, durable storage of messages, a job queue,
producers in worker processes (the producer runs in the FastAPI app, as
today), "one active run per chat" (the application's pointer, as today).

## B2. Keys and entries

One run is two keys in one Cluster slot:

| Key | Type | Content |
|---|---|---|
| `<prefix>:run:{<scope>:<run_id>}` | Stream | The run's entries. |
| `<prefix>:run:{<scope>:<run_id>}:end` | String | The end reason. Set once (`SET NX`). |

The end key is what lets the helper drop `IDMP`: it gives one end reason, a
cancel signal another process can see, and an O(1) status, from one `SET NX`.
The end script (B4) writes it only while the stream exists, and gives it the
stream's own expiry, so it never outlives its run.

Names, as today: a scope is 1-200 of `A-Z a-z 0-9 _ . : -` (today's
`_SCOPE_RE`, so `chat:<id>` works), and a run ID is 1-64 of
`A-Z a-z 0-9 _ -` (today's `_ID_RE`, no `:`). No braces in either. Because the
run ID can't contain `:`, the split between scope and ID is unique.

Entries use Part A's default layout, `event` and `data`:

| `event` | Written by | Seen by clients |
|---|---|---|
| `run.start` | `start()` | no |
| application types (`delta`, `tool_call`, ...) | `emit()` | yes |
| `run.ping` | the producer, after `stream_silence_timeout / 2` of quiet | no |
| `run.end` | the first end; data `{"reason": ...}` plus `"code"` for `error` | yes, last |

Stored reasons: `done`, `error`, `cancelled`. `lost` is never stored: readers
that find a dead producer report it without writing (B4).

`emit()` refuses types starting with `run.`. A stream can hold more than one
`run.end` (two ends racing, or a producer's last emit landing after a cancel);
readers stop at the first, so the rest are never read.

## B3. API

```python
FastAPIRedis(app).lifespan().streams().runs()
# RunsDep = Annotated[Runs, Depends(get_runs)]

class Runs:
    async def start(self, fn: Callable[[RunWriter], Awaitable[None]], *,
                    scope: str, run_id: str | None = None) -> RunHandle: ...
    async def cancel(self, run_id: str, *, scope: str) -> None: ...
    async def status(self, run_id: str, *, scope: str) -> RunStatus | None: ...
    async def sse(self, run_id: str, *, scope: str, request: Request,
                  deadline: datetime | None = None) -> Response: ...
    async def events(self, run_id: str, *, scope: str,
                     after: str | None = None) -> StreamSubscription: ...

@dataclass(frozen=True)
class RunHandle:
    run_id: str
    created: bool          # False: a retried start; nothing was spawned

class RunWriter:
    run_id: str
    scope: str
    async def emit(self, type: str, data: Any) -> str: ...   # entry ID

@dataclass(frozen=True)
class RunStatus:
    state: Literal["running", "ended", "lost"]
    reason: str | None     # when ended: "done" | "error" | "cancelled"
                           # "lost": no end stored, producer silent
```

Compared with today's `StreamBackend`:

| Today | Here |
|---|---|
| `open()` + `spawn()`, or `start()` | `start()` only. `fn` runs only for a new run, so work that must happen once (charging a budget) goes at the top of `fn`. |
| `StreamWriter.append()` | `RunWriter.emit()` |
| `finish(type, data)` | Return from `fn`. The last content is an ordinary `emit()`; the end is always `run.end {"reason": "done"}`. |
| `keepalive()` | Internal, always on. |
| `finished()` (scans the stream) | `status()`: one `GET` plus `XINFO`. |
| `follow()` | `events()`, on Part A's `read(ticks=True)` with the silence check (B4); hidden types and ticks aren't yielded, and a lost run yields a synthesized `run.end` with reason `lost` (no ID), as `sse()` does. |
| `stream.gap`, `stream.error` | Gone. |

## B4. Lifecycle

**Start.** `start()` draws a random attempt token, then runs one `MULTI`:

```
XADD key * event run.start data {"token": <token>}   # creates the stream
EXPIRE key stream_max_run NX
XRANGE key - + COUNT 1                                # the first run.start
```

`created` is "the first `run.start` carries my token", not the `EXPIRE NX`
reply. The `EXPIRE NX` reply identifies the creating *transaction*, so when
the reply is lost and the SDK retries, it would say 0 and no producer would
start anywhere. The token survives the retry, because it's the same `start()`
call. A start from another request has another token, so it gets
`created=False`, spawns nothing, and adds a hidden `run.start` that readers
skip.

Shutdown can begin while the `MULTI` is in flight. `start()` counts itself as
an in-flight start for its whole duration, and `aclose()` waits for in-flight
starts before it collects producer tasks. A start that finds the app shutting
down after its `MULTI`, **and created the run**, writes a `cancelled` end
before returning, so no run is left without a producer (the orphan from the
#106 review). A start with `created=False` writes nothing: the run belongs to
a producer elsewhere, which may be healthy. A start that finds the app
shutting down before the `MULTI` creates nothing.

If the start `MULTI` fails ambiguously and the SDK's retries run out, the run
may exist without a producer, and a retried HTTP request (a new `start()`
call, with a new token) would get `created=False`. So before raising,
`start()` makes one best-effort run of the end script with `error`. The retried
request then finds an ended run, and `status()` says so, rather than a run that
turns `lost` a minute later. If that end fails too, the run turns `lost`; the
guide says to retry with a new `run_id` after a failed start.

**Emit.** One `MULTI`: `XADD key NOMKSTREAM * event <type> data <json>`,
`EXISTS key:end`. Still one round trip.

- `XADD` returned nil: the stream expired; `emit()` raises `RunExpired`.
- `EXISTS` returned 1: the run was ended elsewhere (a cancel from another
  process). `emit()` raises `RunEnded`, which unwinds `fn`; the helper writes
  nothing more.
- A driver error: `StreamStoreError`, no retry (a retried `XADD` without `IDMP`
  could duplicate a delta, which readers would show). Unless `fn` handles it,
  the run ends with `error`.

Limits as today: `stream_maxlen` events per run (renamed
`stream_max_events`, since nothing trims), `stream_max_event_bytes` per event.

**Producer task.** `start()` spawns `fn` as a task the helper owns and keeps
in a set (no overwrite by key; the #106 finding). Around `fn`, a keepalive
loop wakes every `stream_heartbeat_interval`:

- `EXISTS key:end`. If set, cancel the task: a cancel from another process
  stops the producer within one heartbeat, even while `fn` waits on a model
  call. Today a producer in another process is never told.
- If nothing was emitted for `stream_silence_timeout / 2`, emit `run.ping`
  (in the same `MULTI`). After a write fails, the keepalive retries the ping
  every second until one succeeds, so readers see the producer within about a
  second of Redis coming back. A reader's quiet count restarts after the outage
  (A3, `ticks`), so that ping lands long before it reaches the silence
  timeout.

Once `fn` returns, raises or is cancelled, the task records its end reason and
the keepalive stops before the end is written, so the keepalive can't cancel
the task's own end. Today's `_ending` flag and `_cancel_unless_ending` exist
for the same reason.

**End.** One Lua script over both keys (same slot), used by every end
(producer done, producer error, cancel, shutdown, a failed start):

```lua
-- KEYS[1] = stream, KEYS[2] = end key; ARGV = reason, payload, done_ttl_ms
if redis.call('EXISTS', KEYS[1]) == 0 then return false end
if redis.call('EXISTS', KEYS[2]) == 1 then return redis.call('GET', KEYS[2]) end
redis.call('XADD', KEYS[1], '*', 'event', 'run.end', 'data', ARGV[2])
if redis.call('SET', KEYS[2], ARGV[1], 'NX') then
  redis.call('PEXPIRE', KEYS[1], ARGV[3], 'LT')
  redis.call('PEXPIRE', KEYS[2], redis.call('PTTL', KEYS[1]))
end
return redis.call('GET', KEYS[2])                      -- the reason that won
```

- A missing stream gets nothing, so a cancel of an unknown or expired run
  answers `StreamNotFound` and leaves no end key behind for a later run with
  the same ID.
- The first end wins the `SET` and writes the first `run.end`, so `status()`
  and readers agree. A later end finds the end key and writes nothing, so
  repeated cancels can't grow the stream. (A producer's last `emit()` can still
  land after a cancel; it is never read.) `XADD` stays before `SET`: if `XADD`
  failed, the script stops before `SET`, and Lua doesn't roll back writes.
- `LT` only shortens the lifetime: a run lives at most `stream_done_ttl`
  after its end and never past `stream_max_run`, and repeated cancels don't
  extend it (the other #106 finding).
- The end key gets the stream's remaining lifetime, so the two expire at the
  same millisecond.

This is the helper's only script. A script is simpler here than `IDMP`, and
the earlier design avoided Lua mainly because `IDMP` made it unnecessary.
Neither key needs `IDMP` or `XCFGSET`.

**Cancel.** The end transaction with `cancelled`. Readers stop at the
`run.end`. A local producer's task is cancelled at once; a remote one within a
heartbeat (above). Cancelling an ended run changes nothing. A missing run:
`StreamNotFound`.

**Lost producer.** Nobody writes anything. Each reader decides for itself:
a run is *lost* to a reader when a `StreamTick` arrives with `quiet_for` of
at least `stream_silence_timeout`, meaning that reader has read Redis
successfully for that long and seen no entry, pings included. The reader
then sends `run.end {"reason": "lost"}` with no `id:`, since it isn't stored,
and ends. No reader writes, and an outage never counts toward the timeout,
because failed reads produce no ticks and reset `quiet_for`.

A client closes on `run.end` (B6). One that reconnects anyway replays the
run, then waits a full silence timeout of its own before hearing `lost`
again. That wait is the cost of measuring on the reader's clock rather than
Redis's: no clock offset, no `EVAL_RO`, no extra reader permissions, and a
reconnect never gets a permanent 204 from a verdict reached during an outage.

The cost: if a producer was only stalled (an event loop blocked for longer
than the silence timeout) and emits again later, a new reader sees the run
continue, while earlier readers were told `lost`. The producer can't detect
this itself, since a blocked loop runs no keepalive either. The guide says
not to block the event loop in a producer, and that a `lost` verdict can be
wrong in that one case. There is no self-stop on failed writes: those happen
during a Redis outage, which the run is meant to survive.

**Shutdown.** Refuse new starts, wait for in-flight starts, then yield once to
the loop (today's `asyncio.sleep(0)`) so every spawned task has entered its
`try` (a task cancelled before its first step never writes its end). Then
cancel every producer task, give them `_SHUTDOWN_GRACE_SECONDS` to write
`cancelled` ends, and close the pool, in a `finally` that the shutdown can't
skip.

**Status.** One `MULTI` (one time snapshot, so the two keys can't straddle
their shared expiry): `EXISTS key`, `GET key:end`, `XINFO STREAM key`, and the
clock through `EVAL_RO` on `key`. Missing stream: `None`. End reason set:
`ended`. Newest entry older than the silence timeout: `lost`. Otherwise
`running`. This is the one place that reads Redis's clock, through
`EVAL_RO` because a keyless `TIME` can't be queued in a redis-py Cluster
transaction; it runs in the API process, not in a reader. `status()` has no
outage guard; it reports what the data says
now, so right after an outage it can say `lost` for a run whose producer is
about to ping.

## B5. How it uses the core

`Runs.sse()` is Part A's `read()` and `respond()` with its own `frames()`:

```python
HIDDEN = ("run.start", "run.ping")

async def frames(sub):
    async for item in sub:
        if isinstance(item, StreamTick):
            if item.quiet_for >= settings.stream_silence_timeout:
                yield SSEEvent(event="run.end", data='{"reason":"lost"}')
                return
            yield item                                    # ": ping"
        elif item.fields.get("event") in HIDDEN:
            yield SSEEvent(data=None, id=item.id)         # id-only
        else:
            yield SSEEvent(data=item.fields["data"],
                           event=item.fields["event"], id=item.id)

sub = await streams.read(key, after=streams.cursor(request), ticks=True,
                         until=lambda e: e.fields.get("event") == "run.end",
                         deadline=deadline)
if sub.ended_by == "until":
    await sub.aclose()
    return Response(status_code=204)
return streams.respond(frames(sub), request=request)
```

Nothing in Part A knows about runs. Part B's writes (`start`, `emit`, the end
transaction, pings) are its own; its reads are all Part A.

## B6. Example

```python
@app.post("/chats/{chat_id}/runs/{run_id}", status_code=202,
          dependencies=[Depends(require_chat_access)])
async def start(chat_id: str, run_id: str, body: Prompt, runs: RunsDep):
    handle = await runs.start(run_turn(body.text), scope=f"chat:{chat_id}", run_id=run_id)
    return {"run_id": handle.run_id}

@app.get("/chats/{chat_id}/runs/{run_id}/events",
         dependencies=[Depends(require_chat_access)])
async def events(chat_id: str, run_id: str, request: Request, runs: RunsDep):
    return await runs.sse(run_id, scope=f"chat:{chat_id}", request=request,
                          deadline=session_expiry(request))

@app.delete("/chats/{chat_id}/runs/{run_id}", status_code=204,
            dependencies=[Depends(require_chat_access)])
async def cancel(chat_id: str, run_id: str, runs: RunsDep):
    await runs.cancel(run_id, scope=f"chat:{chat_id}")

def run_turn(prompt: str):
    async def produce(w: RunWriter) -> None:
        async for chunk in model.stream(prompt):
            await w.emit("delta", {"text": chunk})
        await w.emit("message", {"text": final_text})
    return produce
```

Client:

```js
const es = new EventSource(`/chats/${chat}/runs/${run}/events`);
es.addEventListener("delta", (e) => append(JSON.parse(e.data).text));
es.addEventListener("run.end", (e) => { es.close(); showOutcome(JSON.parse(e.data).reason); });
```

## B7. Settings

Part B owns the run settings: `stream_max_run`, `stream_done_ttl`,
`stream_max_events` (was `stream_maxlen`), `stream_max_event_bytes`,
`stream_silence_timeout` (still more than twice the heartbeat interval).
`stream_max_run` loses its 86,400 cap, which only existed for the `IDMP`
window. `stream_done_ttl` needs no upper bound either.

## B8. Tests (real Redis)

- Retried start: same run, one producer, `created=False`. A start whose first
  `MULTI` reply is lost and that the SDK retries still gets `created=True`.
- Cancel of a missing run: `StreamNotFound`, no end key left; a later start
  with that ID runs normally.
- Repeated cancels of an ended run add no entries.
- A start whose `MULTI` fails ambiguously past the retries leaves an `error`
  end, and a retried request sees an ended run.
- A start with `created=False` during shutdown doesn't end the other
  process's run.
- Emit after a remote cancel raises `RunEnded`; an idle remote producer is
  cancelled within one heartbeat.
- Two ends racing (producer done and cancel): one reason in `status()`, and
  readers see the reason that won the `SET`.
- Repeated cancels don't extend the TTL; an end near `stream_max_run` doesn't
  either.
- Lost producer: kill the task without an end; every reader gets
  `run.end {"reason": "lost"}` after the silence timeout, and no reader wrote.
- Redis down longer than the silence timeout while the producer lives: no
  reader reports `lost`, including one that reconnects the moment Redis is
  back, and the run finishes `done` for every reader.
- Shutdown: running producers end `cancelled`; a start that overlaps the
  shutdown creates nothing, or ends `cancelled`.
- `status()` for running, ended, lost and missing, in a constant number of
  commands.
- Redis 7.4 and 8.8; Cluster (both keys in one slot) and Sentinel.

## 3. Open questions

1. **Retrying an emit.** No retry means a network blip ends a run with
   `error`. On Redis 8.6.2+ `XADD ... IDMP` would make retries safe. Worth a
   second write path, or is ending the run honest enough?
2. **`deadline` without an event.** Fine for fetch clients; `EventSource`
   clients can't tell why they stopped. Is that acceptable?
3. **Id-only frames (A5).** They rely on the WHATWG parser setting the last
   event ID before it drops an event with no data. Browsers follow that;
   fetch-based client libraries should be checked (`@microsoft/fetch-event-source`,
   the AI SDK).
4. **Naming.** `RedisStreams`/`StreamsDep` and `Runs`/`RunsDep`, or keep
   `StreamBackend` for the core to reduce churn on #100?
5. **Migration.** #100 isn't on `main`, so nothing public breaks. The agent
   SDK examples and the incident-triage app move to Part B; the guide splits
   into "Streaming a Redis Stream over SSE" and "Resumable agent runs".
