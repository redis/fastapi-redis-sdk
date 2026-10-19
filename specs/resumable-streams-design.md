# Resumable streams: proposed design

Date: 2026-10-15
Status: proposal, for review
Inputs: [`resumable-streams-research.md`](resumable-streams-research.md), the
Redis Open Source release notes for 7.4 through 8.10, the Redis command pages
for `XADD`, `XREAD`, `XINFO STREAM` and `DELEX`, and the SDK's existing
session, cache and rate-limit modules.

## 1. Summary

Add a fourth feature to the SDK, beside caching, rate limiting and sessions:
a Redis-backed event log that lets a client resume an agent's streamed output
after a dropped connection.

The design follows the Glance model from the research. One Redis Stream holds
one turn. The stream entry ID is the cursor. Server-Sent Events (SSE) is the
first transport. The agent's run is decoupled from the HTTP request, so a
disconnect never stops it.

This feature requires Redis 8.6.2 or later. Caching, rate limiting and
sessions keep their own, lower floor. The higher floor lets the first release
use idempotent `XADD`, `DELEX` and `volatile-lrm` without fallback paths,
capability probes or a Lua script. The SDK checks the server version once and
fails with a clear error if it is too old (section 11).

## 2. Goals and non-goals

Goals:

1. A client resumes from its last event with no gap and no silent duplicate.
2. The run survives a client disconnect, a tab reload and a gateway restart.
3. A second tab or device can follow the same run.
4. An expired or truncated stream returns an explicit error, never a restart.
5. A stop request is separate from a disconnect.
6. One active run per scope (for example per chat), with a safe retry of the
   start request.
7. Only the stream's owner can read it.
8. Works on Redis 8.6.2 or later, standalone, Cluster and Sentinel.

Non-goals:

- The agent runtime. Callers still run model calls and tools.
- Durable storage of messages. The application database stays the record.
  A stream is transport, as in Glance.
- A WebSocket transport in the first release. The core stays
  transport-neutral so one can be added later (section 12).
- A durable job queue for starting runs (section 5.3 explains why).

## 3. What the Redis releases change

The research assumed a plain Redis Streams design. The 7.4 to 8.10 notes add
some useful tools and some traps. Each item below states what the SDK does
with it.

### 3.1 Features used

| Feature | Since | Use in this design |
|---|---|---|
| `XREAD ... STREAMS key +` | 7.4 | Read the last entry cheaply, to find the writer's last sequence number. |
| `XINFO STREAM` field `max-deleted-entry-id` | 5.0 field; correct after trimming from 7.4.2 | Detect a gap exactly (section 7.2). |
| `XADD ... NOMKSTREAM` | 6.2 | A late write cannot resurrect a stream that expired or was cleaned up (section 6.2). |
| `XADD ... IDMP producer-id idempotent-id` | 8.6 | A retried append after a timeout does not store a duplicate (section 6.3). |
| `DELEX key IFEQ value` | 8.4 | Release the "active run" slot only if it still names this run (section 7.1). |
| `volatile-lrm` eviction policy | 8.6 | Lets operators evict finished idle streams first (section 3.3). |
| Compact hashes | 8.10 | None to write. Meta hashes share one schema, so they qualify automatically when the server has the feature. |

Notes on this table:

- **Why 8.6.2 and not 8.6.0.** IDMP had defects in 8.6.0 and 8.6.1. The 8.6.2
  notes list an AOF rewrite fix (`XIDMPRECORD`), expiry that failed after an
  RDB load, and further IDMP fixes. Since every append uses IDMP, the floor is
  the first release with those fixes. This also covers the 7.4.2 `XTRIM`
  tombstone fix that gap detection needs.
- **Deferred to a later phase.** `XREAD ... MAXSIZE` and `MAXCOUNT` (8.10)
  bound a replay page by bytes and by entries across streams. They sit above
  the floor, so the first release does not use them. `COUNT` plus a cap on
  event size bounds a page well enough (section 6.5).
- **`XCFGSET` is not used in the first release.** IDMP retention follows the
  server settings `stream-idmp-duration` and `stream-idmp-maxsize`. I have not
  read their defaults, so they must be checked before release. If they are
  too large for token-by-token streams, a later change sets a small window per
  stream with `XCFGSET`. The two Redis doc pages disagree on that command's
  option names (`IDMP-DURATION` and `IDMP-MAXSIZE` on the `XADD` page,
  `DURATION` and `MAXSIZE` in the `XINFO STREAM` example), so test it on a
  real server first.

### 3.2 Features evaluated and not adopted

| Feature | Why not |
|---|---|
| Consumer groups: `XREADGROUP`, `XACKDEL`, `XDELEX`, `XNACK`, `XREADGROUP CLAIM` | Groups share one stream among competing consumers and track pending entries per consumer. A resumable stream needs the opposite: each reader has its own cursor, and the client holds it. Plain `XREAD` fans out to every reader without server state. Groups would add state with no benefit. They suit a job queue (section 5.3). |
| Pub/Sub beside the stream (Vercel, LibreChat) | `XREAD BLOCK` already wakes on `XADD`. A second path means a second source of truth. Glance named the splice between replay and live as the hard part. One `XREAD` loop has no splice. |
| Hash field expiration (7.4), `HSETEX`/`HGETEX` (8.0) | The session store needs two clocks. A stream needs one TTL per key. |
| Keyspace notifications (`OVERWRITTEN`, `TYPE_CHANGED`, hash subkey events) | Nothing here needs them. |
| `INCREX` | Not needed in the stream code. The start endpoint should compose with the SDK's existing `rate_limit()` dependency. |

### 3.3 Operational facts from the notes

- **ACL bypass in `XREAD` and `XREADGROUP` (#15478).** The keys checked by the
  ACL could differ from the keys the command read. Within the supported
  range, the fix shipped in 8.6.6, 8.8.2 and 8.10.1. A deployment that
  isolates tenants by ACL key pattern must run one of those or later. The
  docs must say so.
- **Stream `RESTORE` payload defects** (use-after-free, remote code execution)
  were fixed in 8.6.5 and later releases of the lines above. This is not a
  design input, but the docs should link it with the item above. In practice
  the docs should advise 8.6.6 or later on the 8.6 line.
- **Eviction.** Release 8.6 adds `volatile-lrm` and `allkeys-lrm`, which evict
  the least recently *modified* key. Every stream key here carries a TTL, so
  `volatile-*` policies can evict it. My inference, not a documented
  guarantee: under `volatile-lrm`, a stream receiving appends is the last to go,
  and finished idle streams go first. An evicted stream surfaces as a 410, never
  as corruption. The docs should recommend `volatile-lrm` where the SDK shares a
  Redis with other features.
- **CI.** The test matrix runs Redis 8.8 and 7.4 (`ci.yml`). The stream tests
  run on the 8.8 entry, which is above the floor. The 7.4 entry skips them.
  The matrix needs no new entry.

## 4. Architecture

```
 client ──POST /runs──────────────▶ API process ── open() ──▶ Redis (meta, active slot)
        ◀─202 {stream_id}──────────                 spawn() ─▶ producer task ──append()─▶ Redis Stream
                                                                       ▲ heartbeat: alive key, cancel flag
 client ──GET /streams/{id}/events (Last-Event-ID)─▶ any API process
        ◀── SSE: replay from cursor, then live tail ──────  XREAD BLOCK ◀── Redis Stream
```

One `XREAD BLOCK` loop serves both replay and live tail. A reconnecting client
passes its last entry ID. Redis returns everything after it at once, then the
loop blocks for new entries. No code splices two sources.

The producer can run in the API process (a task the SDK holds a reference to),
or in a separate worker that uses the same `open()` and `append()` calls.
Readers cannot tell the difference.

## 5. Design decisions

### 5.1 Cursor

The Redis entry ID is the cursor and the SSE `id:`. The client returns it in
`Last-Event-ID` (the browser sends it automatically on an `EventSource`
reconnect) or in an `?after=` query parameter for clients that cannot set
headers. This is the choice of Glance, LangGraph and nebula-chat in the
research. A request with no cursor replays from the start, as LangGraph does
with `-` and the AI SDK does with its replay default.

The server validates a cursor against `^\d+-\d+$`. Anything else is a 400.

### 5.2 Terminal state lives in the stream

The last entry of a finished stream has type `done`, `error` or `cancelled`.
A reader stops only when it reads one of these. It never decides that a run
ended from the meta hash. Stardrift's race (a subscriber deleted before its
final read) came from relying on out-of-band state. The SDK also never
deletes a stream at completion. It shortens the TTL and lets Redis remove it.

### 5.3 No work queue in this feature

Glance's gateway posts the turn to an agent service and gets a 202. How the
agent service receives the turn (HTTP, queue, task) is outside this SDK.
The feature starts at `open()`. A durable dispatch queue, where `XREADGROUP
CLAIM` and `XNACK` would apply, is a separate feature.

### 5.4 Disconnect never cancels

A closed reader ends only that reader. Stopping a run needs an explicit
`cancel()` call, which the application exposes as its own endpoint. This
matches the AI SDK's current guidance and LangGraph's default
(`cancel_on_disconnect=false`).

### 5.5 Dead producers are detected by Redis, not by timeout alone

Glance reports that it learns of a dead agent loop only by timeout. Here the
producer keeps a short-lived `alive` key. A reader that times out waiting for
an entry checks it. If the key is gone and no terminal entry exists, the reader
sends an `error` event with code `producer_lost` to its client. This follows
the session store's rule: Redis enforces the clock, and no process compares
timestamps.

### 5.6 Ownership

Every stream records an `owner` string (for example the session subject).
Reading, resuming and cancelling check it. A caller who is not the owner gets
the same 404 as for a stream that does not exist, so a stream ID reveals
nothing. Without this, an ID alone grants access. The research flagged that gap
in the AI SDK's approach.

## 6. Redis data model

### 6.1 Keys

```
redis:fastapi:stream:{<id>}         Stream   entries; key TTL
redis:fastapi:stream-meta:{<id>}    Hash     status, owner, cancel; key TTL
redis:fastapi:stream-alive:{<id>}   String   producer heartbeat; PX TTL
redis:fastapi:stream-active:<scope> String   -> <id>; one active run per scope; EX TTL
```

The `{<id>}` hash tag puts the first three keys in one Cluster slot, so a
pipeline over them is legal. The `active` key lives in its own slot and is only
ever touched on its own. Meta hashes all use the same four field names, so
Redis 8.10's compact hash encoding applies without any code on our side.

### 6.2 Entry format

Three short fields per entry, as the session store uses a one-character field
name for its payload:

| Field | Meaning |
|---|---|
| `t` | Event type. `done`, `error` and `cancelled` are reserved and terminal. |
| `d` | Payload, encoded by the SDK's `Coder` (JSON by default). |
| `q` | Producer sequence number, starting at 1. |

Writes use `NOMKSTREAM` for every append after the first. If the stream has
expired (a producer paused longer than the TTL), the append returns nil and the
writer raises `StreamExpired`. Without it, a late write would create a new key
with no TTL.

### 6.3 Duplicate protection

A client-side timeout on `XADD` leaves the writer unsure whether Redis applied
it. A blind retry could store the event twice.

Every append uses `XADD ... IDMP <writer-id> <q>`. A repeat of the same pair
returns the original entry ID and stores nothing. The writer may therefore
retry any append that failed with a timeout or a connection error. The
reader needs no duplicate filter, because the log holds no duplicates.

Retention of the idempotency records follows the server settings (section
3.1). The window only has to cover the last few in-flight appends.

### 6.4 TTLs

| State | Stream and meta TTL | Set when |
|---|---|---|
| Running | `stream_ttl_active` (default 10 min) | Every append refreshes it in the same round trip. |
| Finished | `stream_ttl_done` (default 2 min) | After the terminal entry is written. |
| Suspended | Set by the caller | `writer.suspend(ttl)` extends it, for a turn that waits on a slow tool (Glance's case). |

Because each append refreshes the TTL, a producer killed mid-run leaves a key
that expires by itself. This is Glance's atomic-TTL property, obtained by
refreshing rather than by setting once.

`XADD` does not change a key's TTL, so the refresh is a separate `EXPIRE` in
the same transaction.

### 6.5 Size limit

Each `XADD` carries `MAXLEN ~ stream_maxlen` (default 10,000 entries).
Trimming can remove entries a slow client has not read. Section 7.2 turns that
into an error instead of a silent gap. The writer rejects any event larger than
`stream_max_event_bytes`. With `COUNT` set to `stream_replay_page`, this bounds
one replay page at the product of the two settings.

## 7. Behavior

### 7.1 Writer

`open(scope, owner, stream_id=None)`:

1. If `scope` is given, `SET stream-active:<scope> <id> NX EX <ttl>`. On
   failure, read the value. If it equals `<id>`, this is a retry of the same
   start request, so attach to it. If not, raise `StreamBusy` with the
   existing ID. A client-supplied `stream_id` (for example an idempotency key)
   makes a retried `POST` safe, which is Vercel's idempotent entry point.
2. Create the meta hash (`status=running`, `owner`) with its TTL.

`append(type, data)` runs one transaction:

- `XADD` with `NOMKSTREAM` (after the first), `IDMP` and the `MAXLEN ~` cap.
- `EXPIRE` on the stream and the meta hash.
- `HGET` on the meta hash for the cancel flag.

It returns the entry ID and the cancel flag in one round trip.

`finish()`, `fail(code)` and `cancelled()` append the terminal entry, set the
finished TTLs, delete the `alive` key, and release the active slot.

Releasing the slot uses `DELEX stream-active:<scope> IFEQ <id>`. The release
must compare, because a stale writer must not free the slot of a newer run.

`keepalive()` is an async context manager that refreshes `alive` and polls the
cancel flag every `stream_heartbeat_seconds`. It covers long tool calls, when
no appends happen.

`attach(stream_id)` lets a different process continue a suspended stream. The
new writer reads the last entry with `XREAD STREAMS key +` to learn the last
`q`, and uses a new writer ID so IDMP does not confuse the two.

### 7.2 Reader

`follow(stream_id, owner, after=None)`:

1. Read the meta hash. Missing or wrong owner raises `StreamNotFound`. Missing
   because it expired raises `StreamExpired`. The two differ only if the
   active registry or the application records that the ID once existed;
   otherwise both are a 404. This is an open question (section 14).
2. With a cursor, run `XINFO STREAM`. If `cursor < max-deleted-entry-id`, some
   entry after the cursor was deleted or trimmed, so raise `StreamExpired`
   (HTTP 410). The test is exact: a gap exists if and only if an entry newer
   than the cursor was removed.
3. Loop: `XREAD BLOCK <block_ms> COUNT <page> STREAMS key <cursor>`. Yield
   each entry, advance the cursor, and stop after a terminal entry.
4. On an empty reply, yield a keepalive. If the `alive` key is gone and the
   stream has no terminal entry, yield `error: producer_lost` and stop.

`XREAD` on a stream that does not exist yet simply blocks, so a reader that
arrives before the first event works. `open()` has already created the meta
hash, so ownership can be checked first.

The loop bounds each replay page. Socket.IO's Redis Streams adapter hit a
reconnect storm because restore reads were unbounded: about 250 simultaneous
reconnects ran large `XRANGE` reads and froze live pushes (see the research
follow-up). A paged `XREAD` with `COUNT` keeps each client's work small and
fair.

### 7.3 Cancel

`cancel(stream_id, owner)` sets `cancel=1` in the meta hash after the owner
check. The writer sees it on its next `append()` or heartbeat, so the latency
is at most one heartbeat interval. For producers started by `spawn()`, the SDK
turns the flag into `asyncio` cancellation of the task and writes a
`cancelled` terminal entry.

### 7.4 Spawn

Starlette cancels work bound to a request when the client disconnects. A
producer must not run inside the request. `spawn(writer, fn)` starts it as a
task the backend owns:

- It keeps a strong reference, so the task is not garbage collected.
- It calls `finish()` when `fn` returns, `fail()` when it raises, and
  `cancelled()` on cancellation.
- It runs `keepalive()` around `fn`.
- On application shutdown, the lifespan cancels the remaining tasks, which
  write `cancelled` entries so readers do not hang.

An in-process task dies with its process. Readers then see `producer_lost`
after the `alive` key expires. A producer that must survive a deploy runs in a
separate worker and uses `open()` and `append()` directly.

## 8. Public API

Names follow the existing modules: a `StreamBackend` like `CacheBackend`, a
dependency alias like `CacheBackendDep`, and an exception family like
`SessionError`.

```python
class StreamBackend:
    async def open(self, *, scope: str | None, owner: str,
                   stream_id: str | None = None) -> StreamWriter: ...
    async def attach(self, stream_id: str, *, owner: str) -> StreamWriter: ...
    def spawn(self, writer: StreamWriter,
              fn: Callable[[StreamWriter], Awaitable[None]]) -> asyncio.Task[None]: ...
    def follow(self, stream_id: str, *, owner: str,
               after: str | None = None) -> AsyncIterator[StreamEvent]: ...
    def sse(self, stream_id: str, *, owner: str, request: Request,
            after: str | None = None) -> Response: ...
    async def cancel(self, stream_id: str, *, owner: str) -> bool: ...
    async def active(self, scope: str, *, owner: str) -> str | None: ...

class StreamWriter:
    stream_id: str
    async def append(self, type: str, data: Any) -> AppendResult: ...
    async def finish(self) -> None: ...
    async def fail(self, code: str) -> None: ...
    async def suspend(self, ttl: int | timedelta) -> None: ...
    def keepalive(self) -> AbstractAsyncContextManager[None]: ...

@dataclass(frozen=True)
class StreamEvent:
    id: str          # Redis entry ID; the cursor
    type: str
    data: Any
```

`follow()` is the transport-neutral core. `sse()` wraps it. A WebSocket handler
would iterate `follow()` and send each event.

Example endpoints:

```python
@app.post("/chats/{chat_id}/runs", status_code=202,
          dependencies=[Depends(rate_limit("10/minute"))])
async def start(chat_id: str, body: RunIn, streams: StreamBackendDep,
                user_id: str = Depends(current_user)):
    writer = await streams.open(scope=f"chat:{chat_id}", owner=user_id,
                                stream_id=body.run_id)
    streams.spawn(writer, lambda w: run_agent(body, w))
    return {"stream_id": writer.stream_id}

@app.get("/streams/{stream_id}/events")
async def events(stream_id: str, request: Request, streams: StreamBackendDep,
                 user_id: str = Depends(current_user), after: str | None = None):
    return streams.sse(stream_id, owner=user_id, request=request, after=after)

@app.post("/streams/{stream_id}/cancel", status_code=204)
async def stop(stream_id: str, streams: StreamBackendDep,
               user_id: str = Depends(current_user)):
    await streams.cancel(stream_id, owner=user_id)
```

```python
async def run_agent(body, w: StreamWriter) -> None:
    async for chunk in model.stream(body.prompt):
        await w.append("delta", {"text": chunk})
    # spawn() writes the terminal "done" entry when this returns
```

## 9. SSE response

- `id:` is the entry ID, `event:` is the type, `data:` is the JSON payload.
- A comment line (`: ping`) goes out every `stream_heartbeat_seconds` (default
  15). The same interval bounds each `XREAD BLOCK`.
- A `retry:` field sets the browser's reconnect delay.
- Headers: `Content-Type: text/event-stream`, `Cache-Control: no-store`,
  `X-Accel-Buffering: no`. The response is private to one user, so it must
  never pass through the SDK's `cache()` decorator.
- Errors found before the first byte use ordinary status codes: 404 for
  unknown or foreign, 410 for expired or truncated, 409 for a busy scope.
  After streaming starts, an error is an `event: error` with a code, then the
  connection closes.
- Compression middleware that buffers the body defeats streaming. The docs
  should say to exclude this route.

## 10. Configuration

Added to `RedisSettings` with the existing `REDIS_` prefix. Defaults are
starting points, to be tuned by benchmark.

| Setting | Default | Meaning |
|---|---|---|
| `stream_ttl_active` | 600 s | TTL refreshed on every append. |
| `stream_ttl_done` | 120 s | Reconnect window after a terminal entry. |
| `stream_maxlen` | 10,000 | Approximate entry cap per stream. |
| `stream_max_event_bytes` | 65,536 | Largest accepted event. |
| `stream_heartbeat_seconds` | 15 | SSE ping, `alive` refresh and cancel poll. |
| `stream_alive_ttl_seconds` | 45 | Lifetime of the `alive` key. |
| `stream_replay_page` | 100 | Entries per `XREAD`. |
| `stream_max_readers` | 256 | Concurrent live readers per process. |

The reader holds one connection for the whole `BLOCK`, so `stream_max_readers`
plus the application's other traffic must fit in the connection pool. Over the
limit, the endpoint answers 503 with `Retry-After`. Also, the driver's
`socket_timeout` must exceed `BLOCK` time, or the read fails with a timeout.
The settings validator should reject a combination where it does not.

## 11. Integration with the SDK

| Area | Change |
|---|---|
| New module | `stream_backend.py`: `StreamBackend`, `StreamWriter`, key builders, the server version check. |
| New module | `streams.py`: the SSE response, the cursor parser, the exception handlers. |
| `exceptions.py` | Add `StreamError` (base), `StreamNotFound`, `StreamExpired`, `StreamBusy`, `StreamStoreError`, `StreamConfigurationError`. Update the module docstring, which says "session feature". |
| `deps.py` | `get_stream_backend`, `StreamBackendDep`, and a flag on `_PoolState` recording that the version check passed, so it runs once per pool. |
| `setup.py` | `FastAPIRedis.streams()`: register the handlers and the shutdown hook. Calling it twice is a no-op, like the other builder methods. |
| `telemetry.py` | `stream_span`, `record_stream_operation(operation, result)`, `record_stream_latency`. Counters for appends, resumes, gap errors (410), `producer_lost`, live readers and replay bytes. |
| `__init__.py` | Export the public names. |
| Docs | `docs/guide/streams.md`, a nav entry in `mkdocs.yml`, an API reference section. The docs workflow builds in strict mode, so every link must resolve. |
| Sessions guide | The existing "WebSockets have no session" limitation applies to a future WebSocket reader. Link to it. |

**Version check.** There is one path through the code, so there is no
capability probe. On first use of the backend, the SDK reads `redis_version`
from `INFO server` and raises `StreamConfigurationError` if it is below
8.6.2. The message names the version found and the version required. The
result is cached per pool. In Cluster mode, a rolling upgrade can leave nodes
on different versions; the check reads one node, so the docs should tell
operators to finish the upgrade first. Redis-compatible servers that report an
older `redis_version` fail the check, and whether any of them implements IDMP
is outside this design.

The SDK supports Python 3.10, which lacks `asyncio.timeout`. The reader loop
uses `asyncio.wait_for` or the driver's own `BLOCK` timeout.

A sync writer, for producers running in sync worker code, follows the pattern
of the other backends and comes in a later phase.

## 12. WebSocket path

Not built in phase 1. The core was shaped so it can be added:

- The cursor and the log are transport-independent. A WebSocket reader would
  iterate `follow()` and send the cursor in a first message or a query
  parameter, since the browser cannot set handshake headers.
- Authentication must happen during the HTTP handshake, as the sessions guide
  already describes.
- A WebSocket has no built-in resume. The server would reuse this log as
  Discord's gateway and Socket.IO's recovery do, with the same bounded replay.
- The research found WebSocket demand concentrated in voice and telephony, not
  in browser text agents. Add it when a user needs it.

## 13. Testing

Behavior to prove, each against a real Redis in CI, with fakeredis only where
it supports the command:

1. Resume from a cursor returns exactly the entries after it: none lost, none
   repeated.
2. A cursor older than a trimmed entry returns 410, never a partial replay.
3. A killed producer leads to `producer_lost` within the `alive` TTL.
4. A stream past its TTL returns 410 or 404 as designed.
5. A non-owner gets the same response as an unknown ID.
6. Cancel stops the producer within one heartbeat.
7. A retried start request returns the same stream.
8. A retried append after an injected timeout stores one entry and shows one
   event to the client.
9. A late `append` after expiry raises and does not recreate the key.
10. A reconnect storm (many readers, large replay) leaves live delivery on
    other streams within a latency bound.
11. Stream, meta and `alive` keys share a Cluster slot, and a multi-key
    pipeline on them succeeds.
12. Releasing the active slot by a stale writer does not free a newer run.

The stream tests run against Redis 8.8, which `ci.yml` already tests, and are
skipped on the 7.4 entry. A unit test also checks that a server reporting a
version below 8.6.2 raises `StreamConfigurationError`. Whether fakeredis
supports `XADD` `IDMP`, `NOMKSTREAM` and `XINFO STREAM` fields is unverified;
check before relying on it. If it does not, the stream tests need a real
server.

## 14. Risks and open questions

Risks:

| Risk | Mitigation |
|---|---|
| Blocked readers exhaust the connection pool. | `stream_max_readers`, a documented sizing rule, a 503 on overload. A dedicated pool is a later option. |
| Trimming makes slow clients see 410. | Generous default `stream_maxlen`; clear error; the database holds the final message. |
| Users on Redis below 8.6.2 cannot use the feature. | Accepted (decision 1). Clear error at first use; other SDK features keep their lower floor. |
| Server defaults for IDMP retention are unknown to this design. | Read `stream-idmp-duration` and `stream-idmp-maxsize` defaults before release; add `XCFGSET` later if they are too large. |
| Docs disagree on `XCFGSET` option names. | Matters only if `XCFGSET` is added; verify on a real server then. |
| Eviction under memory pressure removes active streams. | Document `volatile-lrm`; the failure is a clean 410. |
| In-process producers die on deploy. | Readers get `producer_lost`; document the worker path. |

Decisions:

1. **Minimum server version: decided 2026-10-15.** This feature requires Redis
   8.6.2 or later, and the first release is simplified to match: no fallback
   paths, no capability probes, no Lua. See sections 3.1 and 11.

Open questions for the owner:

2. **404 versus 410 for an expired stream.** Telling them apart needs a record
   that the ID once existed. Is a uniform 404 acceptable?
3. **Scope of `spawn()`.** Should the SDK own in-process producer tasks, or
   only provide `open()` and `append()` and leave task management to callers?
4. **Dedicated pool for readers.** Share the pool and document sizing, or add a
   separate pool setting now?
5. **Default `stream_maxlen`.** 10,000 is a guess. Is there a typical agent
   turn size to design for?
6. **Naming.** `StreamBackend` matches caching and rate limiting. Sessions use
   `Store`. Which fits better?

## 15. Phasing

1. **Phase 1.** Async `StreamBackend`, writer, `follow()`, `sse()`, cancel,
   active slot, `spawn()`, the version check, telemetry, docs, tests on the
   existing 8.8 CI entry.
2. **Phase 2.** Sync writer. Optional byte-bounded replay with `MAXSIZE`, and
   thread-level reads across several turns with `MAXCOUNT`, both on 8.10 or
   later. Event coalescing for token deltas, which LibreChat does to cut write
   load, at the cost of a small delay. Per-stream IDMP tuning with `XCFGSET`,
   if the server defaults prove too large.
3. **Phase 3, only on demand.** WebSocket reader. A durable dispatch queue on
   consumer groups.

## 16. Sources

Redis:

- [Redis Open Source release notes index](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/)
  and its pages for [7.4](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisce-7.4-release-notes/),
  [8.0](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisos-8.0-release-notes/),
  [8.2](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisos-8.2-release-notes/),
  [8.4](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisos-8.4-release-notes/),
  [8.6](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisos-8.6-release-notes/),
  [8.8](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisos-8.8-release-notes/),
  [8.10](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisce/redisos-8.10-release-notes/)
- [`XADD`](https://redis.io/docs/latest/commands/xadd/),
  [`XREAD`](https://redis.io/docs/latest/commands/xread/),
  [`XINFO STREAM`](https://redis.io/docs/latest/commands/xinfo-stream/),
  [`DELEX`](https://redis.io/docs/latest/commands/delex/)

Systems and patterns (from the research and its follow-up):

- [Glance](https://engg.glance.com/building-resumable-real-time-llm-streaming-with-redis-streams-09cfa9e79358)
  and [Vercel `resumable-stream`](https://github.com/vercel/resumable-stream)
- [Stardrift](https://stardrift.ai/blog/streaming-resumptions)
- [LibreChat resumable streams](https://www.librechat.ai/docs/features/resumable_streams)
- [LangSmith join run stream](https://docs.langchain.com/langsmith/agent-server-api/thread-runs/join-run-stream)
- [OpenAI background mode](https://developers.openai.com/api/docs/guides/background)
- [Socket.IO connection state recovery](https://socket.io/docs/v4/connection-state-recovery),
  [Redis Streams adapter](https://socket.io/docs/v4/redis-streams-adapter/) and
  the [reconnect-storm PR](https://github.com/Mereithhh/very-happy/pull/416)
- [Discord gateway](https://docs.discord.com/developers/events/gateway)
