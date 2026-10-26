# Resumable streams: proposed design

Date: 2026-10-15, updated 2026-10-23
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
use idempotent `XADD`, `DELEX` and `volatile-lrm` without fallback paths or
a Lua script. The SDK detects support once per pool, as the rate limiter and
sessions do. On a server without support it turns the feature off, and the
rest of the application keeps running (section 11).

## 2. Goals and non-goals

Goals:

1. A client resumes from its last event with no gap and no silent duplicate.
2. The run survives a client disconnect, a tab reload and a gateway restart.
3. A second tab or device can follow the same run.
4. An expired or truncated stream is reported explicitly, never restarted or
   skipped in silence.
5. A stop request is separate from a disconnect.
6. One active run per scope (for example per chat), with a safe retry of the
   start request.
7. The application decides who may read, resume or cancel a stream. A stream
   ID is never a credential, and an open connection ends no later than the
   reader's credential (for example the session) expires.
8. Works on Redis 8.6.2 or later, standalone, Cluster and Sentinel.

Non-goals:

- The agent runtime. Callers still run model calls and tools.
- Durable storage of messages. The application database stays the record.
  A stream is transport, as in Glance.
- A WebSocket transport in the first release. The core stays
  transport-neutral so one can be added later (section 12).
- A durable job queue for starting runs (section 5.3 explains why).
- Writers and readers outside a FastAPI application in phase 1. Every writer
  and every reader runs inside a FastAPI app that set up the feature with
  `FastAPIRedis.streams()`, and gets its backend through `StreamBackendDep`.
  A separate worker process (Arq, Celery, a plain `asyncio` script) cannot
  write to a stream. Nothing in Redis prevents it: the keys and commands are
  the same. But phase 1 offers no public constructor, settings model or
  lifecycle for code outside FastAPI, and none of it is supported. A
  producer that must survive a deploy is therefore out of scope for phase 1.
  Section 15 lists the interface for workers as a later step.

## 3. What the Redis releases change

The research assumed a plain Redis Streams design. The 7.4 to 8.10 notes add
some useful tools and some traps. Each item below states what the SDK does
with it.

### 3.1 Features used

| Feature | Since | Use in this design |
|---|---|---|
| `XREAD ... STREAMS key +` | 7.4 | Read the last entry cheaply, to find the writer's last sequence number. |
| `XINFO STREAM` fields `max-deleted-entry-id`, `entries-added`, `length`, `first-entry`, `last-generated-id` | 7.0 | Detect removed history and a cursor ahead of the stream (section 7.2). |
| `XADD ... NOMKSTREAM` | 6.2 | A late write cannot resurrect a stream that expired or was cleaned up (section 6.2). |
| `XADD ... IDMP producer-id idempotent-id` | 8.6 | A retried append after a timeout does not store a duplicate (section 6.3). |
| `DELEX key IFEQ value` | 8.4 | Release the "active run" slot only if it still names this run (section 7.1). |
| `SET key value IFEQ value` | 8.4 | Refresh the "active run" slot only if it still names this run (section 7.1). Checked on Redis 8.8 on 2026-10-23. |
| `volatile-lrm` eviction policy | 8.6 | Lets operators evict finished idle streams first (section 3.3). |
| Compact hashes | 8.10 | None to write. Meta hashes share one schema, so they qualify automatically when the server has the feature. |

Notes on this table:

- **Why 8.6.2 and not 8.6.0.** IDMP had defects in 8.6.0 and 8.6.1. The 8.6.2
  notes list an AOF rewrite fix (`XIDMPRECORD`), expiry that failed after an
  RDB load, and further IDMP fixes. Since every append uses IDMP, the floor is
  the first release with those fixes.
- **Trimming does not move `max-deleted-entry-id`.** Checked on Redis 8.8 on
  2026-10-23: `XDEL` raises the field, but trimming, with `XTRIM` or with
  `XADD … MAXLEN ~`, leaves it at `0-0`. Trimming shows only as
  `entries-added` exceeding `length`, with the oldest entries gone. Section
  7.2 detects removed history from both.
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
  `DURATION` and `MAXSIZE` in the `XINFO STREAM` example). Redis 8.8 accepts
  `IDMP-DURATION` and `IDMP-MAXSIZE` (checked on 2026-10-23).

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
  and finished idle streams go first. An evicted stream surfaces as a 410 while
  its meta hash survives, and as a 404 once that is gone too (section 6.1),
  never as corruption. The docs should recommend `volatile-lrm` where the SDK shares a
  Redis with other features.
- **CI.** The test matrix runs Redis 8.8 and 7.4 (`ci.yml`). The stream tests
  run on the 8.8 entry, which is above the floor. On the 7.4 entry, one test
  checks that the feature turns itself off (section 11). The matrix needs no
  new entry.

## 4. Architecture

```
 client ──POST /runs──────────────▶ API process ── open() ──▶ Redis (meta, active slot)
        ◀─202 {stream_id}──────────                 spawn() ─▶ producer task ──append()─▶ Redis Stream
                                                                       ▲ heartbeat: alive key, cancel flag
 client ──GET /streams/{id}/events (Last-Event-ID)─▶ any API process
        ◀── SSE: replay from cursor, then live tail ──────  XREAD BLOCK ◀── Redis Stream
```

One `XREAD BLOCK` loop serves both replay and live tail. A reconnecting client
passes its last entry ID. Redis returns the entries after it page by page, then
the loop blocks for new entries. No code splices two sources.

The producer runs in the FastAPI application, as a task the SDK holds a
reference to (section 7.4). Any process of the application can read, and the
readers cannot tell which process writes. Producers outside the application
are not supported in phase 1 (section 2).

## 5. Design decisions

### 5.1 Cursor

The Redis entry ID is the cursor and the SSE `id:`. The client returns it in
`Last-Event-ID` (the browser sends it automatically on an `EventSource`
reconnect) or in an `?after=` query parameter for clients that cannot set
headers. This is the choice of Glance, LangGraph and nebula-chat in the
research.

A request with no cursor is the same as cursor `0-0`: it asks for the whole
stream, as LangGraph does with `-` and the AI SDK does with its replay
default. If any entry was ever trimmed or deleted, the request gets a 410,
never a partial replay. Without this rule, a reader with no cursor would start
silently at the oldest entry left after trimming.

**Which cursor wins.** `sse()` reads the cursor from the request itself, in
this order:

1. A value the handler passes as `after`. The application uses it when it
   knows better, for example when it loaded state from its database together
   with the cursor that matches that state.
2. `Last-Event-ID`.
3. `?after=`.
4. None: `0-0`, as above.

The header beats the query because `EventSource` keeps the URL it was built
with. A client that built `EventSource("…?after=X")` after a reload sends
`Last-Event-ID: Y`, which is newer, on its own reconnects. If the query won,
every reconnect would replay from X, and the client would see the entries
between X and Y twice. A route must therefore not copy the query into `after`;
`sse()` reads it. `follow()` has no request, so it takes only an explicit
`after`.

**Validation.** At the HTTP boundary, before any Redis read:

- A cursor must match `^\d+-\d+$`, with each part at most 2^64 − 1. This
  rejects the special IDs `$`, `+` and `-`, and the bare `<ms>` form that Redis
  also accepts. `$` in particular would mean "only new entries" and skip
  history in silence.
- `0-0` is allowed and means the whole stream.
- More than one `Last-Event-ID` header, or more than one `after` query
  parameter, is refused. A proxy may join two headers into `a, b`, and guessing
  between them would hide a client bug.
- Every cursor present is validated, even one that loses under the order
  above, so a broken client is seen early.

A failed check raises `StreamInvalidCursor`, which `sse()` answers with 422, as
FastAPI answers other validation errors.

**Comparison.** The SDK parses a cursor into a pair of integers once, at the
boundary, and compares pairs. As strings, `"999-0"` sorts after `"1000-0"` and
`"5-9"` after `"5-10"`, so a text comparison would give wrong answers in the
gap check, the 204 check and the ahead check (section 7.2).

**A cursor ahead of the stream.** A cursor above the stream's
`last-generated-id` points at entries the stream never produced. It comes from
another stream, from a client bug, or from a failover that lost writes: the
client read entries that the old primary acknowledged but the replica never
received. After the replica takes over, new entries get IDs above its own last
ID, which can still be below the client's cursor, so `XREAD` would skip them
in silence. The reader refuses such a cursor with `StreamCursorAhead`, which
`sse()` answers with 409, and the client reloads from the application's
database. `last-generated-id` is the right bound because trimming and deletion
never lower it. Before the first append the stream does not exist, so any
cursor other than `0-0` is ahead.

### 5.2 One end marker, in the stream

A reader needs one fact at the end: no more entries will come. Without it, a
reader blocks until the TTL runs out, and `EventSource` reconnects forever.
How the run ended (success, failure, a stop) does not affect replay, cursors
or reconnects, so the SDK does not model it. It belongs in the data of the
last event. No stream layer in the research has several end types either.
Where outcomes exist (A2A tasks, for example), they come from the run or task
model above the stream.

So a stream has a single end marker: an entry field `f=1` on its final entry.
The final entry is an ordinary event, and the application chooses its type
and data. `finish()` writes it (section 7.1). When the SDK ends a run itself,
in `spawn()` (section 7.4) or because a reader found the producer dead
(section 5.5), it writes a final event of type `stream.end` with the reason in
its data.

A reader stops only when it reads the final entry. It never decides that a
run ended from the meta hash. Stardrift's race (a subscriber deleted before
its final read) came from relying on out-of-band state. Meta records the final
entry's ID only so that a reconnect after the end can get a 204 (section 7.2).
The SDK also never deletes a stream at completion. It shortens the TTL and
lets Redis remove it.

The client needs no knowledge of which event is last. The server closes the
response after the final entry. `EventSource` reconnects with the final
cursor, gets a 204, and by the SSE specification stops for good. A client may
close earlier on an event it knows, but it does not have to.

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
an entry checks it. If the key is gone and no final entry exists, the stream
would never get its end marker, so **the reader ends it**. It writes the final
entry a producer would have written: `stream.end` with data
`{"reason": "producer_lost"}`, and with the same fixed IDMP pair as every final
entry (section 6.3). It sets `status=finished`, `final` and the finished TTLs,
and releases the active slot with `DELEX … IFEQ`. From then on a dead producer
is an ordinary end: every reader stops at the final entry, and a reconnect gets
a 204, so `EventSource` stops for good. Without this, every reconnect would
wait out the `alive` timeout, find no end, and repeat until the TTL ran out.

Several readers may notice at once. The fixed IDMP pair makes them write one
final entry between them, and each gets its ID back. A producer that was only
slow, not dead, loses the race: its own `finish()` gets the reader's final
entry back, and any later append is harmless, because readers stop at the
first final entry. Starting the run again is never the SDK's decision; it
would be a new run with a new stream.

Writing needs write access. A reader whose Redis ACL is read-only cannot end
the stream, so it sends `stream.error` with code `producer_lost` and closes,
and the client should close too.

This follows the session store's rule: Redis enforces the clock, and no
process compares timestamps.

### 5.6 Authorization stays with the application

The SDK has no access model. The application checks access in its own FastAPI
dependency, before it calls the SDK, as the sessions guide does with
`require_user`. The SDK's session feature can supply the caller's identity, but
a live session proves only that the session exists, not that its user may read
a given stream. Access to a resource such as a chat stays application policy.

The SDK adds two things that the application cannot easily build itself:

- **Names derived from the authorized resource.** A stream's full name is its
  scope plus its ID, and the scope is part of the Redis key (section 6.1).
  Every call that reaches a stream (`follow()`, `sse()`, `cancel()`,
  `attach()`) takes both. The application builds the scope from the resource
  its dependency just authorized, for example `chat:{chat.id}`. A stream ID
  from another chat then names a key that does not exist, so it gets the same
  404 as an unknown stream, and the SDK compares nothing. A stream ID alone
  never grants access, which the research asked for (recurring decision 7),
  and the application cannot forget the check, because the scope is part of
  the address. PR #89 uses the same idea with one stream per chat. We keep one
  stream per turn, so the turn's ID joins the scope in the name.

  Access attaches to the resource, not to a session. So every member of a
  shared chat, every tab and every device of a user that passes the
  application's check builds the same scope and reaches the same streams.
  Nothing is written per session. A second device finds the current run with
  `active(scope)`, and a finished run's ID from the application's own record
  of the chat.

  The application must build the scope from the object its dependency
  returned, not from the raw path, whenever it resolves aliases or
  tenant-local IDs. Otherwise one scope string could name two resources.
- **Connection deadline.** Authorization happens once, when the connection
  opens. `sse()` takes an `expires_at` deadline and closes the response by
  then. With the SDK's sessions, `session_expiry(request)` returns the
  session's absolute deadline, which Redis counts down. A
  `stream_max_connection_seconds` cap (default 300) also ends every
  connection. The browser then reconnects with `Last-Event-ID`, and the
  application's dependency runs again. A revoked or signed-out session
  therefore keeps reading for at most that cap.

The stream TTL stays independent of the session TTL. The stream TTL bounds one
run's data and cleans up after a dead producer. A session bounds a user's
sign-in. Several sessions can follow one run, and a producer task that
appends has no session at all, so the end of one session must not end the
stream.

## 6. Redis data model

### 6.1 Keys

```
<prefix>:stream:{<scope>}:<id>        Stream   entries; key TTL t
<prefix>:stream-meta:{<scope>}:<id>   Hash     status, cancel, final, writer, started;
                                               key TTL t + stream_gone_ttl_seconds
<prefix>:stream-alive:{<scope>}:<id>  String   producer heartbeat; PX TTL
<prefix>:stream-active:{<scope>}      String   -> <id>; one active run per scope;
                                               EX stream_active_ttl_seconds, refreshed by
                                               append() and keepalive()
```

`<prefix>` is the SDK's `prefix` setting (`REDIS_PREFIX`, default
`redis:fastapi`), as for every other feature: the cache uses `<prefix>:cache`,
the rate limiter `<prefix>:ratelimit`, and sessions `<prefix>:session:` and
`<prefix>:sessions-of:`. With the default, a stream key reads
`redis:fastapi:stream:{chat:42}:run1`. Two applications that share a Redis
and set different prefixes therefore never share a stream, and an ACL key
pattern on the prefix covers streams too. `.streams()` rejects a prefix that
contains `{` or `}`: Redis hashes on the first braces in a key, so such a
prefix would put every stream in one Cluster slot.

The scope is part of every stream key, so a stream is reachable only through
the scope it was opened in (section 5.6). Scopes match
`^[A-Za-z0-9_.:-]{1,200}$` and stream IDs match `^[A-Za-z0-9_-]{1,64}$`. The
SDK rejects anything else. Neither may contain a brace, so no value can break
out of the hash tag. A stream ID cannot contain `:`, so `{<scope>}:<id>`
splits in exactly one way, and two different pairs never give the same key.

The `{<scope>}` hash tag puts all four keys, and every run of the scope, in one
Cluster slot, so a transaction over the slot and the run's keys is legal. That
lets `open()` take the slot and create the run in one transaction, and lets the
final entry free the slot in the transaction that writes it (section 7.1).
The cost is that a scope's runs share one shard. A scope names one resource,
such as a chat, so this load is small; a single scope for a whole application
would put every stream on one shard, and the guide warns against it. Meta
hashes all use the same field
names, so Redis 8.10's compact hash encoding applies without any code on our
side.

Meta fields:

| Field | Set by | Meaning |
|---|---|---|
| `status` | `open()`, `finish()` | `running` or `finished`. |
| `cancel` | `cancel()` | `1` once a stop was requested. |
| `final` | `finish()` | The final entry's ID, for the 204 after the end (section 7.2). |
| `writer` | `open()`, `attach()` | The current writer's ID, for the fence (section 7.1). |
| `started` | The first append | `1` once the stream key exists. |

**Meta outlives the stream.** Every `EXPIRE` that gives the stream TTL `t`
gives meta `t + stream_gone_ttl_seconds`. After the stream expires, meta stays for
that window as a record that the stream existed. A reconnect in the window
then gets a 410, not a 404 (section 7.2). Meta holds no message content, so
keeping it is cheap: a few short fields in a compact hash, about 100 bytes.
Meta lives under the scope's name too, so a reader from another scope looks up
a key that never existed, gets a 404, and learns nothing.

A write to meta outside the append transaction (the final entry's ID, the
cancel flag, a new writer on `attach()`) runs with `PEXPIRE … NX` in the same
transaction. If meta expired a moment before, `HSET` would create it again
with no TTL, and it would never expire. `PEXPIRE … NX` sets a TTL only on a key
that has none, so it repairs exactly that case and leaves a live meta's TTL
alone.

`started` is what tells "expired" from "not started yet". Before the first
append, meta exists and the stream key does not, which is also how an expired
stream looks. The flag separates the two without comparing clocks, which keeps
the rule that Redis enforces time and no process compares timestamps.

After the window, meta expires too, and a reconnect gets a 404. Eviction under
memory pressure can remove meta early, with the same result. A stream that
never existed can never get a 410.

### 6.2 Entry format

Short fields per entry, as the session store uses a one-character field name
for its payload:

| Field | Meaning |
|---|---|
| `t` | Event type, chosen by the application. Types that start with `stream.` are reserved for the SDK: `stream.end` for a run the SDK ends itself, and `stream.gap` and `stream.error` for the reader's own events, which are never stored. `append()` and `finish()` reject any type with that prefix, and any type that is empty, longer than 128 characters, or contains CR, LF or NUL (section 9). |
| `d` | Payload, encoded by the SDK's `Coder` (JSON by default). |
| `q` | Producer sequence number, starting at 1. `attach()` continues it, so a stream's `q` values have no holes. The reader uses that to detect gaps (section 7.2). |
| `f` | `1` on the final entry only (section 5.2). Absent on every other entry. |

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

Every final entry, from `finish()` or from `spawn()`, uses one fixed pair
instead: `IDMP end final`. A stream can therefore hold at most one final entry,
even if two writers end it at once. The second gets the first one's ID back.

This protection covers an ambiguous write, not a producer that runs again.
With `spawn()` no second producer ever writes to a stream: when a task dies,
its run is lost, a reader ends the stream with reason `producer_lost`, and the user starts a new run
with a new stream. A producer can only run again when the application puts the
work on a durable job queue that redelivers it after a crash. Such a queue is
out of scope (section 5.3), and even then, a model run again produces
different tokens, so keys could not remove them as duplicates. Keys supplied
by the application, for deterministic events such as tool results, belong
with the durable dispatch queue in phase 3 (section 15).

### 6.4 TTLs

| State | Stream TTL | Set when |
|---|---|---|
| Running | `stream_active_ttl_seconds` (default 10 min) | Every append refreshes it in the same round trip. |
| Finished | `stream_done_ttl_seconds` (default 2 min) | After the final entry is written. |
| Extended | Set by the caller | `writer.extend(ttl)` keeps it for `ttl`, for a turn that waits on a slow tool (Glance's case). |

In every state, meta gets the stream's TTL plus `stream_gone_ttl_seconds`
(default 1 h), in the same command batch (section 6.1).

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

`open(scope, stream_id=None, exclusive=True)`:

1. `WATCH` the slot and the meta hash; with `exclusive=False`, only meta.
   With `exclusive=False` the run takes no slot, so a scope can hold several
   runs at once.
2. Read meta's `status` and `writer`, and the slot.
   - `writer` is this call's own writer ID: an earlier attempt's `EXEC`
     applied but its reply was lost. The run is this call's: `created` is
     `True`.
   - Meta exists: a retried start of a known run, running or finished, for
     example a `POST` retried after a fast run ended. Return a writer whose
     `created` is `False`, and change nothing. The run's meta, not the slot,
     marks a retry, because the slot is freed when a run ends but meta
     outlives it by `stream_done_ttl_seconds + stream_gone_ttl_seconds`. After that, the
     same ID starts a new run.
   - The slot names another run: raise `StreamBusy` with its ID.
3. Otherwise run one `MULTI`: `SET stream-active:{<scope>} <id> EX
   <stream_active_ttl_seconds>` (if `exclusive`), create the meta hash
   (`status=running`, `writer=<id>`) with TTL `stream_active_ttl_seconds +
   stream_gone_ttl_seconds`, and set `alive`.
4. If another start changed a watched key after the `WATCH`, `EXEC` aborts:
   go back to step 1, up to three times. A timeout or a dropped connection is
   retried the same way.

The slot and meta therefore appear together or not at all. No crash or error
can leave a slot that names a run without meta, and two concurrent starts of
one run create it once. A slot that names `<id>` while meta is gone (expired
or evicted) is taken over in step 3; the old producer, if any, is fenced at its
next append or heartbeat.

The handle returned for a retry writes nothing; it refuses with
`StreamFenced`, because the run's own producer writes. A client-supplied
`stream_id` (for example an idempotency key) makes a retried `POST` safe,
which is Vercel's idempotent entry point.

`append(type, data)` runs one transaction:

- `XADD` with `NOMKSTREAM` (after the first), `IDMP` and the `MAXLEN ~` cap.
- `PEXPIRE` on the stream (`t`) and on the meta hash (`t` plus
  `stream_gone_ttl_seconds`). Every duration setting is in seconds and may be
  fractional, so the feature sends milliseconds (`PEXPIRE`, `PX`).
- On the first append only, `HSET started 1` on the meta hash.
- `SET` on the `alive` key with its PX TTL, so a producer that appends counts
  as alive even without `keepalive()`.
- For an exclusive run, `SET stream-active:{<scope>} <id> IFEQ <id> PX
  <stream_active_ttl_seconds>`, so a run that appends keeps its slot even without
  `keepalive()`.
- `HMGET` on the meta hash for the cancel flag and the `writer` field.

A timeout or a dropped connection, after which Redis may or may not have
applied the transaction, is retried with backoff, up to three attempts. Every
command in it is safe to repeat: the `IDMP` pair makes the `XADD` idempotent,
and `PEXPIRE`, `HSET` and `SET` converge. An error from Redis becomes a
`StreamStoreError` with fixed text: Redis's own message can name the ACL user
and the key, which holds the scope.

It returns the entry ID in one round trip, and records the cancel flag in
`writer.cancelled`. If `writer` names another writer, `attach()` has replaced
this one: the writer raises `StreamFenced` and stops. This is the fence.

`finish(type, data=None)` appends the final entry: an ordinary event of the
application's chosen type, with `f=1`. The same transaction sets
`status=finished` in meta and the finished TTLs, deletes the `alive` key and
releases the active slot. The final event and the end marker are one append,
so no crash can fall between the last data and the end. A retry of `finish()`
after a timeout is safe, because the final entry uses its fixed IDMP pair
(section 6.3).

The final entry's ID is known only once `XADD` returns, so a second step
records `final=<entry ID>` in meta. That step can fail, or a crash can fall
before it. Readers do not depend on it: the final entry itself carries `f=1`,
and a reconnect whose cursor names that entry gets the 204 either way (section
7.2).

Unlike an append, `finish()` checks the fence before it writes. It `WATCH`es
meta, reads `writer` on another connection, and runs the transaction only if
meta still names this writer; if meta changed before `EXEC`, it checks again.
A stale final entry would end the new writer's run, which no reader could
undo, so it costs one extra round trip, once per run. A writer that finds
another writer raises `StreamFenced` and writes nothing.

After `finish()`, the writer refuses further appends with `StreamFinished`.
This is the seal. It covers the writer's own process and costs no Redis
command. A stale writer in another process could still append after the end.
That is harmless: every reader stops at the first final entry, and a reconnect
at or past `final` gets a 204 (section 7.2), so no reader ever sees the late
entry.

Releasing the slot uses `DELEX stream-active:{<scope>} IFEQ <id>`. The release
must compare, because a stale writer must not free the slot of a newer run.
The comparison also makes it safe for a run opened with `exclusive=False`,
whose ID the slot never names.

`keepalive()` is an async context manager that refreshes `alive` and polls the
cancel flag and the `writer` field every `stream_heartbeat_seconds`. It covers
long tool calls, when no appends happen.

`keepalive()` also refreshes the active slot, in the same transaction, for a
run that goes long without appending. The refresh must compare, like the
release, so a stale writer cannot extend a newer run's slot: `SET
stream-active:{<scope>} <id> IFEQ <id> PX <stream_active_ttl_seconds>`. Redis 8.4 added
the `IFEQ` condition to `SET` together with `DELEX`. On Redis 8.8 it sets the
key only when the value matches, and answers nil otherwise (checked on
2026-10-23). Without the refresh, a run longer than
`stream_active_ttl_seconds` would lose its slot, and a second run could start in the
same scope.

`attach(stream_id, scope)` lets another process of the same FastAPI
application continue an extended stream. The
new writer reads the last entry with `XREAD STREAMS key +` to learn the last
`q`, and uses a new writer ID so IDMP does not confuse the two. If meta says
`status=finished`, `attach()` raises `StreamFinished`. Otherwise it sets
`writer=<new id>` in meta, which fences the old writer.

The fence limits the damage of a stale writer but cannot prevent it. A stale
writer learns of the fence only from the reply to its next append, so that one
entry has already landed. Preventing it would need a `WATCH` round trip on
every append, or a Lua script, which this design avoids. `open()` and
`finish()` can afford `WATCH` because each runs once per run. The case is
rare: it needs an extended run that a second process attaches while the first
is still alive.

`extend(ttl)` keeps the stream, meta, the `alive` key and the active slot for
`ttl` seconds, for a run that waits on a slow tool or that another process
will continue. A retry handle, a fenced writer and a finished writer refuse it
with `StreamFenced` or `StreamFinished`: only the run's own writer may keep it
alive.

### 7.2 Reader

`follow(stream_id, scope, after=None)` runs steps 1 to 3 when it is awaited,
before any event, and returns a `StreamReader` that runs steps 4 to 6 as it is
iterated. So a WebSocket handler can refuse a connection with a close code
before it accepts it, as `sse()` answers with a status:

1. Read the meta hash and check that the stream key exists, in one round
   trip. Both keys are named from `scope` and `stream_id`. Then decide:
   - Meta missing: raise `StreamNotFound` (404). This covers an unknown ID and
     an ID from another scope alike, so a foreign stream never reveals that it
     exists or expired.
   - `started=1` and the stream key missing: raise `StreamExpired` (410).
   - Otherwise go on. A stream that has not started yet waits in step 5.
2. Take the cursor chosen and validated as in section 5.1, or `0-0` if there
   is none.
3. Check the cursor once, in one transaction: `MULTI`, `XINFO STREAM key`,
   `XRANGE key <cursor> <cursor>`, `XRANGE key (<cursor> + COUNT <page>`,
   `EXEC`. The first page uses an exclusive `XRANGE`, not `XREAD`: redis-py
   cannot queue `XREAD`, whose key position varies, in an async Cluster
   transaction. Then decide in this order:
   - `cursor > last-generated-id`: raise `StreamCursorAhead` (HTTP 409,
     section 5.1). This comes first, so a cursor past a finished stream's last
     ID is refused, not taken as "read to the end".
   - Meta has a `final` ID and the cursor is at or past it, or the cursor's
     own entry has `f=1`: the reader has already read the end. The reader
     `follow()` returns is empty, and `sse()` answers 204, which stops
     `EventSource` for good. The second test covers a `final` field that was
     never recorded (section 7.1).
   - History after the cursor was removed: raise `StreamExpired` (HTTP 410).
     An `XDEL` of an entry newer than the cursor raises
     `max-deleted-entry-id` above the cursor, which is exact. Trimming does not
     move that field (section 3.1); it shows only as `entries-added` exceeding
     `length`. So once anything was removed, a cursor whose own entry is gone
     and that is older than the first retained entry counts as a gap, and so
     does a cursor of `0-0`. If every entry was trimmed, a cursor below
     `last-generated-id` counts as a gap. The trimming test is conservative: a
     client whose last entry was exactly the newest one trimmed also gets 410,
     although it missed nothing. That is safe, and rare with the default
     `stream_maxlen`.

   The transaction makes the checks and the first page one snapshot, so no
   trim can fall between them. `XRANGE` returns the cursor's own entry, whose `q` is the
   starting point for step 5. For `0-0` the starting `q` is 0. If the cursor's
   entry is gone, the first entry read sets it.
4. Yield each entry, advance the cursor and `q`, and stop after the final
   entry (`f=1`).
5. Loop: `XREAD BLOCK <block_ms> COUNT <page> STREAMS key <cursor>`. Before
   each entry, compare its `q` with the last one. If it is more than one
   higher, entries were trimmed while the reader fell behind. Yield a
   `stream.gap` event with no ID and data `{"missing": <count>}`, then go on
   with the entry. Handle each entry as in step 4.
6. On an empty reply, yield a keepalive. If the `alive` key is gone and the
   stream has no final entry, end the stream with `stream.end` and reason
   `producer_lost` (section 5.5), yield that final entry and stop. The reader
   writes that entry with the same `XADD` builder and the same transaction as
   a writer's final entry, without a `q`, which no reader knows. Once the run
   has started, it sends `NOMKSTREAM`, so a stream that expired mid-read stays
   expired: the `XADD` returns nil, the reader stops, and the reconnect gets
   410. Only a run that never appended gets a new stream, holding the end
   alone.

The loop yields each read's entries as one page. The SSE response writes a
page as one chunk, so a replay of `stream_replay_page` entries is one send
under one send timeout, not one per entry; `follow()` decodes the page and
yields its events one by one. The SSE path never decodes a payload: it sends
the stored text as it is.

The SSE response passes the loop a deadline, the earlier of `expires_at` and
the connection cap. No `XREAD BLOCK` runs past it, so the response ends on
time, not up to one heartbeat late.

A `follow()` reader takes no reader slot, so `stream_max_readers` does not
count it. The slot belongs to the SSE response, which can take it before the
first byte and free it on every exit path; a reader object has no such point.
An application that serves `follow()` readers, for example over WebSocket,
bounds them itself.

**Problems after the first byte.** The status code is already sent, so the
reader handles every later problem in one of two ways, and neither needs the
client to know a code:

| Class | Cases | What the reader does | What the client sees |
|---|---|---|---|
| Passing | Redis fails for longer than `stream_read_retry_seconds`; the process shuts down; the connection cap is reached; a send blocks for longer than `stream_send_timeout_seconds` | Closes, with no event | `EventSource` reconnects, to any process, with `Last-Event-ID`, and resumes |
| Over | The producer died; the stream or its meta vanished mid-read, for example by eviction | Ends the stream with `stream.end` (dead producer), or just closes (stream gone) | The reconnect gets 204, or 410 or 404, and `EventSource` stops |

A short Redis failure does not reach the client at all. The reader retries the
failed read with backoff, keeps sending pings, and closes only once the
failure outlasts `stream_read_retry_seconds`.

**A client that stops reading.** A frozen or throttled tab, a suspended
laptop, a phone that lost its network, or a peer that vanished without closing
the connection stops reading. The socket's send buffer fills, the server
applies backpressure, and the reader waits inside `send()`. While it waits it
holds a reader slot, which counts against `stream_max_readers`, so enough
stuck readers make healthy clients get 503. It does not hold a Redis
connection, because it reads nothing while it waits, and its memory does not
grow, because it pulls from Redis only when it can send. So a time limit is
enough, and no byte limit is needed, unlike a design that pushes into a queue
per subscriber.

The SDK's SSE response sends every chunk under
`asyncio.wait_for(send(...), stream_send_timeout_seconds)`. Past the limit it
closes with no event: none could get through a blocked socket. A live client
reconnects with `Last-Event-ID`, and its replay starts where it really
stopped. The response then closes the body generator, so the slot is freed at
once. `StreamingResponse` calls `send()` itself, so this needs the SDK's own
response class, which `sse()` returns anyway.

The timeout helps only while data flows: a large replay or a fast token
stream fills the buffer, and then `send()` blocks. On an idle stream a dead
client is not detected this way, because 15-byte pings take very long to fill
a send buffer, and the kernel accepts them. That case stays bounded by
`stream_max_connection_seconds`. The timeout matters most in a reconnect storm,
when many slow clients replay large streams at once and would otherwise hold
their slots until the connection cap.

The only event the reader adds is `stream.error` with code
`credential_expired` at `expires_at` (section 9). It is a hint, not a need:
the reconnect would get a 401 anyway, but `EventSource` never shows a
response's status, so the hint saves the client a probe to learn why.

A gap found during the read does not end the stream. Only a reader far slower
than the writer meets it, and that reader has already shown most of the
answer. A 410 would throw that away, and the client could not recover the
lost entries anyway. The `stream.gap` event lets the client mark the answer as
incomplete and keep reading. The check costs no Redis command: `q` comes with
each entry, IDMP keeps duplicates out of the log, and `attach()` keeps `q`
without holes.

Before the first append the stream key does not exist. `XINFO STREAM` then
returns an error inside `EXEC`, and `XREAD` returns nil. The reader treats that
as no gap and no entries, and step 5 blocks, so a reader that arrives before
the first event works. A cursor other than `0-0` on such a stream is ahead and
gets a 409. `open()` has already created the meta
hash, so step 1 finds it.

The loop bounds each replay page. Socket.IO's Redis Streams adapter hit a
reconnect storm because restore reads were unbounded: about 250 simultaneous
reconnects ran large `XRANGE` reads and froze live pushes (see the research
follow-up). A paged `XREAD` with `COUNT` keeps each client's work small and
fair.

### 7.3 Cancel

`cancel(stream_id, scope)` sets `cancel=1` in the meta hash, if it exists;
an unknown stream raises `StreamNotFound`. The writer sees it on its next `append()` or heartbeat, so the latency
is at most one heartbeat interval. For producers started by `spawn()`, the SDK
turns the flag into `asyncio` cancellation of the task and writes a
`stream.end` final entry with reason `cancelled`. A producer outside `spawn()`
checks `writer.cancelled` after each append and calls `finish()` itself.

A cancel never interrupts a final write. Once `spawn()` starts writing the
final entry, for any reason, neither `cancel()` nor the shutdown cancels that
task again: a second `asyncio` cancel would abort the write, and readers would
wait out the heartbeat to see `producer_lost` instead of the real reason.

### 7.4 Spawn

Starlette cancels work bound to a request when the client disconnects. A
producer must not run inside the request. `spawn(writer, fn)` starts it as a
task the backend owns:

- It keeps a strong reference, so the task is not garbage collected.
- It refuses, with `ValueError`, a handle from a retried start (`created` is
  `False`), whose run already has a producer, and a writer it already runs.
  Forgetting the `created` check would otherwise start a task whose first
  append is fenced, and that would hide the real task from `cancel()`.
  `start(fn, scope, stream_id, exclusive)` calls `open()` and then `spawn()`
  only for a new run, so the usual start route needs no check at all.
- If `fn` has not called `finish()` itself, `spawn()` writes the final entry.
  Its type is `stream.end`, and its data gives the reason:
  `{"reason": "done"}` when `fn` returns, `{"reason": "error", "code": ...}`
  when it raises, and `{"reason": "cancelled"}` on cancellation. The code is
  the exception's `code` attribute if it is a string, and `producer_error`
  otherwise, so an exception's class name or message never reaches the
  client. A writer fenced by `attach()` writes no final entry. An
  application that wants its own final event calls `finish()` at the end of
  `fn`.
- It runs `keepalive()` around `fn`.
- On application shutdown, the lifespan cancels the remaining tasks, which
  write `stream.end` entries with reason `cancelled`, so readers do not hang.

An in-process task dies with its process. After the `alive` key expires, a
reader ends the stream with reason `producer_lost` (section 5.5). Phase 1 has
no way to run a producer that survives a deploy, because producers outside the
FastAPI application are not supported (section 2).

### 7.5 Commands per entry point

This section lists every command the feature sends, by the API entry point
that sends it. It was checked with `MONITOR` on Redis 8.8 on 2026-10-26.
It leaves out what the connection itself sends (`HELLO`, `CLIENT SETINFO`).

Short names:

| Name | Meaning |
|---|---|
| `S`, `M`, `A`, `K` | The stream, meta, `alive` and active slot keys (section 6.1) |
| `<id>`, `<wid>`, `<q>` | The stream ID, the writer's ID, and the entry's sequence number |
| `<ttl>` | The TTL passed to `extend()`, in ms |
| `t_a`, `t_d`, `w` | `stream_active_ttl_seconds`, `stream_done_ttl_seconds`, `stream_gone_ttl_seconds`, in ms |
| `alive_ms`, `hb_ms` | `stream_alive_ttl_seconds` and `stream_heartbeat_seconds`, in ms |
| `max`, `page` | `stream_maxlen`, `stream_replay_page` |
| `[...]` | Sent only in the case the note names |

On Cluster, `COMMAND INFO` goes to a random node. Every other command goes to
the node that owns the scope's slot, because all four keys share it.

**Support probe.** Sent once at startup, and again by the first entry point
that checks support (`start`, `open`, `attach`, `cancel`, `active`,
`follow`, `sse`)
while the answer is unknown. Writer methods do not check.

```
COMMAND INFO XCFGSET DELEX
```

**`open(scope, stream_id, exclusive)`.** Two round trips when the run exists
or another run holds the slot, three when it creates the run. With
`exclusive=False`, the `WATCH` names only `M`, and the `MULTI` sends no `SET K`.

```
WATCH K M
MULTI; HMGET M status writer; GET K; EXEC         -- on another connection
then one of:
  MULTI                                           -- create
    SET K <id> PX t_a
    HSET M status running writer <wid>
    PEXPIRE M t_a+w
    SET A 1 PX alive_ms
  EXEC
  UNWATCH                                         -- retried start, or busy
```

If `EXEC` aborts because a watched key changed, or a timeout hides its
result, all steps repeat, up to three attempts.

**`append(type, data)`.** One round trip. A timeout or a dropped connection
repeats it, up to three attempts.

```
MULTI
  XADD S [NOMKSTREAM] IDMP <wid> <q> MAXLEN ~ max * t <type> d <data> q <q>
  [HSET M started 1]                              -- first entry only
  PEXPIRE S t_a
  PEXPIRE M t_a+w
  SET A 1 PX alive_ms
  [SET K <id> IFEQ <id> PX t_a]                   -- exclusive run
  HMGET M cancel writer
EXEC
```

`NOMKSTREAM` is sent once the stream exists: after the first append, and for
a writer from `attach()` on a stream that already has entries.

**`finish(type, data)`**, and the final entry that `spawn()` writes
(`type` is `stream.end`). Four round trips: the fence check, the write, and
the second step. A `WATCH` that aborts repeats the first three, and a timeout
repeats them as for `append`.

```
WATCH M
HGET M writer                                     -- on another connection
MULTI
  XADD S [NOMKSTREAM] IDMP end final MAXLEN ~ max * t <type> d <data> q <q> f 1
  [HSET M started 1]                              -- no entry before it
  HSET M status finished
  PEXPIRE S t_d
  PEXPIRE M t_d+w
  DEL A
  DELEX K IFEQ <id>
  HMGET M cancel writer
EXEC
MULTI; HSET M final <entry ID>; PEXPIRE M t_d+w NX; EXEC
```

If `HGET` names another writer, `finish()` sends `UNWATCH` in place of the
`MULTI` and raises `StreamFenced`.

**`extend(ttl)`.** One round trip.

```
MULTI
  PEXPIRE S <ttl>
  PEXPIRE M <ttl>+w
  SET A 1 PX <ttl>
  [SET K <id> IFEQ <id> PX <ttl>]                 -- exclusive run
EXEC
```

**`keepalive()`**, which `spawn()` runs around the producer. One round trip
every `stream_heartbeat_seconds`.

```
MULTI
  SET A 1 PX alive_ms
  [SET K <id> IFEQ <id> PX t_a]                   -- exclusive run
  HMGET M cancel writer
EXEC
```

**`attach(stream_id, scope)`.** Two round trips.

```
MULTI; HGETALL M; XREVRANGE S + - COUNT 1; GET K; EXEC
MULTI; HSET M writer <wid>; PEXPIRE M t_d+w NX; EXEC
```

**`cancel(stream_id, scope)`.** One round trip if the run has ended, two
otherwise. A producer from `spawn()` in this process is then cancelled, and
writes its final entry as `finish()` does.

```
HGET M status
[MULTI; HSET M cancel 1; PEXPIRE M t_d+w NX; EXEC]  -- still running
```

**`active(scope)`.** One round trip.

```
GET K
```

**`follow(stream_id, scope, after)`** and **`sse(...)`**, which reads through
`follow`'s steps. First, one round trip checks the cursor and reads the first
page. A cursor at or past the final entry stops here (204). `start()` sends
what `open()` sends, and the producer what `append()` and `finish()` send.

```
MULTI
  HGETALL M
  EXISTS S
  XINFO STREAM S
  [XRANGE S <cursor> <cursor>]                    -- a cursor other than 0-0
  XRANGE S (<cursor> + COUNT page
EXEC
```

Then, until the final entry, one blocking read at a time:

```
XREAD BLOCK hb_ms COUNT page STREAMS S <last ID>
```

The SSE response shortens `hb_ms` to the time left before its deadline.

When a full-length read returns nothing, the reader checks the producer:

```
MULTI; EXISTS A; HMGET M status started; EXEC
```

If `A` is gone and the run has not finished, the reader ends the stream for
the lost producer (section 5.5), then reads again and gets that final entry:

```
MULTI
  XADD S [NOMKSTREAM] IDMP end final MAXLEN ~ max * t stream.end d {"reason": "producer_lost"} f 1
  [HSET M started 1]                              -- the run never appended
  HSET M status finished
  PEXPIRE S t_d
  PEXPIRE M t_d+w
  DEL A
  DELEX K IFEQ <id>
  HMGET M cancel writer
EXEC
MULTI; HSET M final <entry ID>; PEXPIRE M t_d+w NX; EXEC
```

`NOMKSTREAM` is sent once `started` is set, so the `XADD` never recreates an
expired stream.

**No commands of their own:** `spawn()` (it runs `keepalive()` and the final
write above), `session_expiry()` (it reads the request's session state), and
the shutdown in `aclose()` (the producers it cancels write their final
entries).

The feature sends no Lua script, no `KEYS` or `SCAN`, no Pub/Sub command, no
consumer-group command, and no `XCFGSET`. The probe only asks whether
`XCFGSET` exists.

## 8. Public API

Names follow the existing modules: a `StreamBackend` like `CacheBackend`, a
dependency alias like `CacheBackendDep`, and an exception family like
`SessionError`.

```python
class StreamBackend:
    async def start(self, fn: Callable[[StreamWriter], Awaitable[None]], *,
                    scope: str, stream_id: str | None = None,
                    exclusive: bool = True) -> StreamWriter: ...
    async def open(self, *, scope: str, stream_id: str | None = None,
                   exclusive: bool = True) -> StreamWriter: ...
    async def attach(self, stream_id: str, *, scope: str) -> StreamWriter: ...
    def spawn(self, writer: StreamWriter,
              fn: Callable[[StreamWriter], Awaitable[None]]) -> asyncio.Task[None]: ...
    async def follow(self, stream_id: str, *, scope: str,
                     after: str | None = None) -> StreamReader: ...
    async def sse(self, stream_id: str, *, request: Request, scope: str,
                  after: str | None = None,   # an override; the request's own cursor otherwise
                  expires_at: datetime | None = None) -> Response: ...
    async def cancel(self, stream_id: str, *, scope: str) -> bool: ...
    async def active(self, scope: str) -> str | None: ...

def session_expiry(request: Request) -> datetime | None: ...

class StreamWriter:
    stream_id: str
    scope: str
    created: bool    # False for a retried start: do not start a producer
    cancelled: bool  # a stop was requested, as last seen
    finished: bool   # this writer wrote the final entry
    async def append(self, type: str, data: Any) -> str: ...
    async def finish(self, type: str, data: Any = None) -> str: ...
    async def extend(self, ttl: float | timedelta) -> None: ...
    def keepalive(self) -> AbstractAsyncContextManager[None]: ...

class StreamReader:  # an async iterator of StreamEvent, and its own context manager
    def __aiter__(self) -> StreamReader: ...
    async def __anext__(self) -> StreamEvent: ...
    async def aclose(self) -> None: ...

@dataclass(frozen=True)
class StreamEvent:
    id: str | None   # Redis entry ID; the cursor. None for a reader's own event.
    type: str
    data: Any
    final: bool      # True on the stream's final entry
```

`append()` and `finish()` return the entry's ID. `append()` records the cancel
flag in `writer.cancelled`.

`start()` is the usual start route in one call: `open()`, then `spawn()` for a
new run only. `open()` and `spawn()` stay public for a route that does
something between them, for example spending a budget only for a new run.

`follow()` is the transport-neutral core. `sse()` wraps it. It is a coroutine
for the same reason `sse()` is: its checks run before any event. A WebSocket
handler awaits it, accepts the connection, and iterates the reader inside
`async with`.

Every entry point refuses a scope or stream ID outside the rules of section
6.1 with `StreamInvalidName`, a `ValueError` answered with 422. Each
`StreamError` subclass carries its HTTP status and a stable `code`, and
`.streams()` registers one handler for `StreamError` that answers with
`{"detail": ..., "code": ...}`. A handler the application registers for
`StreamError` or a subclass takes precedence.

`sse()` is a coroutine: `return await streams.sse(...)`. Every check that
decides the status code (404, 410, 409, 422, 204, 401) reads Redis, and all of
them must finish before the first byte, so the response is built only after
they pass.

`session_expiry()` returns the absolute deadline of the request's SDK session,
or `None` if the request has no session or the session has no deadline. An
application with other credentials passes its own deadline, for example a
JWT's `exp`.

Example endpoints. `require_chat_access` belongs to the application, not to the
SDK:

```python
async def require_chat_access(chat_id: str, session: SessionDep) -> None:
    user_id = session.get("user_id")
    if user_id is None:
        raise HTTPException(status_code=401)
    if not await chats.is_member(chat_id, user_id):
        raise HTTPException(status_code=404)

@app.post("/chats/{chat_id}/runs", status_code=202,
          dependencies=[Depends(require_chat_access),
                        Depends(rate_limit("10/minute"))])
async def start(chat_id: str, body: RunIn, streams: StreamBackendDep):
    # A retried start finds the run and spawns nothing.
    writer = await streams.start(lambda w: run_agent(body, w),
                                 scope=f"chat:{chat_id}", stream_id=body.run_id)
    return {"stream_id": writer.stream_id}

@app.get("/chats/{chat_id}/runs/{stream_id}/events",
         dependencies=[Depends(require_chat_access)])
async def events(chat_id: str, stream_id: str, request: Request,
                 streams: StreamBackendDep):
    # sse() reads Last-Event-ID and ?after= itself; do not pass the query in.
    return await streams.sse(stream_id, request=request, scope=f"chat:{chat_id}",
                             expires_at=session_expiry(request))

@app.post("/chats/{chat_id}/runs/{stream_id}/cancel", status_code=204,
          dependencies=[Depends(require_chat_access)])
async def stop(chat_id: str, stream_id: str, streams: StreamBackendDep):
    await streams.cancel(stream_id, scope=f"chat:{chat_id}")
```

The routes carry the chat ID so that the application can authorize the chat.
The SDK names the stream from the chat's scope and the run's ID, so a run ID
from another chat names no stream and gets a 404.

```python
async def run_agent(body, w: StreamWriter) -> None:
    async for chunk in model.stream(body.prompt):
        await w.append("delta", {"text": chunk})
    await w.finish("message", {"id": body.message_id})
    # without finish(), spawn() writes a "stream.end" entry when this returns
```

## 9. SSE response

- `id:` is the entry ID, `event:` is the type, `data:` is the JSON payload.
- **No frame injection.** The type goes into the `event:` line as it is. A
  type with a line break would end the frame and could start a forged one,
  with its own `id:`, which would also move the client's cursor. So
  `append()` and `finish()` reject a type that contains CR, LF or NUL, is
  empty, or is longer than 128 characters, with `ValueError`, before anything
  reaches Redis. The payload is safe with the default JSON coder, which never
  emits a raw line break, but a custom `Coder` might. The SSE encoder
  therefore splits the encoded payload at every CR, LF or CRLF and writes one
  `data:` line per part, which the browser joins back with `\n`.
- After the final entry, the server closes the response. The browser's
  reconnect at the final cursor gets a 204, which ends `EventSource` retries
  (section 5.2).
- A comment line (`: ping`) goes out every `stream_heartbeat_seconds` (default
  15). The same interval bounds each `XREAD BLOCK`.
- A `retry:` field sets the browser's reconnect delay to a random value
  between 1 and 3 seconds, chosen per connection. Short, because a planned
  close (the connection cap, a slow client, a deploy) happens while the run
  goes on, and a longer delay would leave the reader idle. Random, so the
  clients a deploy closed at once do not all reconnect at once.
- Each read's entries go out as one chunk, so a replay page is one send.
- Headers: `Content-Type: text/event-stream`, `Cache-Control: no-store`,
  `X-Accel-Buffering: no`. The response is private to one user, so it must
  never pass through the SDK's `cache()` decorator.
- Errors found before the first byte use ordinary status codes: 404 for an
  unknown stream (which includes an ID from another scope), 410 for expired
  or truncated, 409 for a busy scope and for a cursor ahead of the stream,
  422 for a malformed or repeated cursor or an invalid name, and 503 when the
  server lacks support and the feature is off (section 11). The body is
  `{"detail": ..., "code": ...}` with a stable `code`; over
  `stream_max_readers` the code is `stream_capacity_exceeded`. The application's own dependency
  returns 401 or 403 before the SDK runs.
- After streaming starts, a passing problem closes the connection with no
  event, and a stream that is over ends with its final entry or closes, as in
  section 7.2. The client needs no list of error codes.
- A gap found during the read is an `event: stream.gap` with no `id:` and
  data `{"missing": <count>}`. It does not close the connection (section 7.2).
- The response ends at `expires_at` with an `event: stream.error` whose code is
  `credential_expired` and no `id:`. It tells the client why, so it can ask the
  user to sign in again instead of showing a failed connection. The same
  event, with code `producer_lost`, is the fallback for a reader that cannot
  write (section 5.5). These are the only two `stream.error` codes. At
  `stream_max_connection_seconds` the response ends with no event, and the
  browser reconnects by itself through the application's dependency. An
  `expires_at` already in the past raises `StreamCredentialExpired`, which
  the handlers answer with 401 before the first byte.
- **Reader slots.** `sse()` only prepares the response: it chooses the
  cursor and runs the checks that answer 404, 410, 409 and 422, none of which
  reserves anything. The SDK's SSE response class takes the reader slot when
  the response runs, at the start of its ASGI call and before any header goes
  out. Over `stream_max_readers` it sends 503 with `Retry-After` instead, so
  admission is still decided before the first byte. Otherwise it streams
  inside `try/finally`, and the `finally` releases the slot. A response that
  is built but never sent (an error in middleware after the handler returns,
  a client gone before the first byte) therefore never takes a slot, and one
  that runs releases it on every exit path: a normal end, a disconnect, the
  send timeout, a cancellation or an error. Counting the reader when `sse()`
  builds the response instead would leak a slot for each response never sent,
  until every new reader got 503.
- Compression middleware that buffers the body defeats streaming. The docs
  should say to exclude this route.

## 10. Configuration

Added to `RedisSettings` with the existing `REDIS_` prefix. Defaults are
starting points, to be tuned by benchmark.

| Setting | Default | Meaning |
|---|---|---|
| `stream_active_ttl_seconds` | 600 s | TTL refreshed on every append. |
| `stream_done_ttl_seconds` | 120 s | Reconnect window after the final entry. |
| `stream_gone_ttl_seconds` | 3,600 s | How long meta outlives the stream, so a late reconnect gets 410, not 404. |
| `stream_maxlen` | 10,000 | Approximate entry cap per stream. |
| `stream_max_event_bytes` | 65,536 | Largest accepted event. |
| `stream_heartbeat_seconds` | 15 | SSE ping, `alive` refresh and cancel poll. |
| `stream_alive_ttl_seconds` | 45 | Lifetime of the `alive` key. |
| `stream_replay_page` | 100 | Entries per `XREAD`. |
| `stream_max_readers` | 256 | Concurrent live readers per process. |
| `stream_max_connection_seconds` | 300 | Longest SSE connection before the client must reconnect and be authorized again. |
| `stream_read_retry_seconds` | 30 | How long a reader retries a failing Redis read, still sending pings, before it closes and lets the client reconnect. |
| `stream_send_timeout_seconds` | 10 | Longest wait for one send to a client before the reader closes and frees its slot. |

The reader holds one connection for the whole `BLOCK`, and returns it to the
pool between reads, so `stream_max_readers` plus the application's other
traffic must fit in the connection pool. Over the
limit, the endpoint answers 503 with `Retry-After` (section 9, reader slots). Also, the driver's
`socket_timeout` must exceed `BLOCK` time, or the read fails with a timeout.
The settings validator should reject a combination where it does not.

## 11. Integration with the SDK

| Area | Change |
|---|---|
| New module | `stream_backend.py`: `StreamBackend`, `StreamWriter`, key builders, `probe_streams_support()`. |
| New module | `streams.py`: the SSE response, the cursor parser, the exception handlers, `session_expiry()`. |
| `exceptions.py` | Add `StreamError` (base), `StreamNotFound`, `StreamExpired`, `StreamBusy`, `StreamFinished`, `StreamFenced`, `StreamInvalidCursor`, `StreamCursorAhead`, `StreamCredentialExpired`, `StreamUnsupported`, `StreamStoreError`, `StreamConfigurationError`. Update the module docstring, which says "session feature". |
| `deps.py` | `get_stream_backend`, `StreamBackendDep`, and a `stream_capabilities` entry on `_PoolState`, beside `ratelimit_capabilities` and `session_capabilities`, so support is detected once per pool. Unlike `get_cache_backend()`, which builds a backend per request, `get_stream_backend()` returns the one `StreamBackend` that `.streams()` created at startup. That backend keeps state for the whole process: the tasks from `spawn()`, the live reader count for `stream_max_readers`, and the support detection. A backend built per request would lose its spawned tasks when the request ended. |
| `setup.py` | `FastAPIRedis.streams()`: register the handlers and the shutdown hook. Calling it twice is a no-op, like the other builder methods. |
| `telemetry.py` | `stream_span`, `record_stream_operation(operation, result)`, `record_stream_latency`. Counters for appends, resumes, gap errors (410), `producer_lost`, live readers, replay bytes and slow-consumer disconnects. |
| `__init__.py` | Export the public names. |
| Docs | `docs/guide/streams.md`, a nav entry in `mkdocs.yml`, an API reference section. The docs workflow builds in strict mode, so every link must resolve. |
| Sessions guide | Show the stream routes as an example of an access dependency, and `session_expiry()` as the way to bound an open stream. The existing "WebSockets have no session" limitation applies to a future WebSocket reader. Link to it. |

**Support detection.** The feature detects server support the way the other
features do. The rate limiter asks `COMMAND INFO INCREX`, and sessions ask
`COMMAND INFO HSETEX`, once per pool. Streams ask
`COMMAND INFO XCFGSET DELEX`. `XCFGSET` arrived in Redis 8.6 together with
`XADD … IDMP`, and `DELEX` (with `SET … IFEQ`) in 8.4, so both present means
the server has the commands this feature uses. Like the others, the probe
asks `COMMAND INFO` rather than sending the command and reading the error,
because on Cluster redis-py rejects an unknown command on the client side. It
runs at startup when `.streams()` is set up, and is best-effort:

- **Both commands present:** the feature is on.
- **Either missing:** the feature is off. The SDK logs one warning at startup
  ("resumable streams are off: this Redis has no XCFGSET / DELEX; they need
  Redis 8.6.2 or later"), and every stream entry point (`open()`, `attach()`,
  `follow()`, `sse()`, `cancel()`, `active()`) raises `StreamUnsupported`,
  which the SSE handlers answer with 503. The application starts and its
  other features work. This is the streams form of the others' fallback: they
  switch to an older command, and streams, which have no fallback path, switch
  off.
- **Redis unreachable at startup:** the result stays unknown, startup goes
  on, and the first stream call probes again, as the rate limiter does.

The result is cached on the pool state for the life of the process. In
Cluster mode the probe asks a random node, as the others do, so the docs tell
operators to finish a rolling upgrade before relying on the feature.

The probe confirms the 8.6 command set, not the patch release. Redis 8.6.0 and
8.6.1 have the commands but carry the IDMP defects listed in section 3.1, and
the probe cannot tell them apart from 8.6.2. The docs state 8.6.2 as the
requirement, and advise 8.6.6 or later (section 3.3). `COMMAND INFO` also
needs no `INFO` permission, so an ACL user without `+info` can use the
feature.

**Client library.** The writer sends `XADD … IDMP`, `DELEX … IFEQ` and
`SET … IFEQ` with `execute_command`, as the session store sends `HSETEX`.
Every write runs in a `MULTI` transaction, and async `RedisCluster` supports
transactions only from redis-py 6.2. In 6.0 and 6.1 it raises "transaction is
deprecated in cluster mode". The SDK's floor is therefore `redis>=6.2.0`.

The SDK supports Python 3.10, which lacks `asyncio.timeout`. The reader loop
uses `asyncio.wait_for` or the driver's own `BLOCK` timeout.

A sync writer, for producers in sync code, follows the pattern of the other
backends and comes in a later phase. Like the async API, it will serve FastAPI
applications only until the worker interface below exists.

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
2. A cursor older than a trimmed entry returns 410, never a partial replay,
   whether the entries were trimmed (`MAXLEN ~`, past a stream node) or
   deleted (`XDEL`). A request with no cursor on a stream that was ever
   trimmed also returns 410. A cursor whose own entry survived returns no
   error. A trim that lands while a slow reader is still reading produces one
   `stream.gap` event with the right `missing` count, and the reader goes on.
3. A killed producer leads, within the `alive` TTL, to one `stream.end`
   final entry with reason `producer_lost`, even when several readers notice
   at once, and the active slot is released. A reconnect then gets 204. A
   read-only reader sends `stream.error` `producer_lost` instead. A slow
   producer that comes back gets the reader's final entry from `finish()`.
4. A stream past its TTL returns 410 while its meta lives, and 404 after
   `stream_gone_ttl_seconds`. A stream with meta but no first append is not
   reported as expired. A run longer than `stream_active_ttl_seconds` keeps its active
   slot, and a stale writer cannot extend a newer run's slot.
5. A stream ID used under another scope gets the same 404 as an unknown ID,
   for read, cancel and attach. Scopes and IDs outside the allowed characters
   are rejected, and no two (scope, ID) pairs give the same key.
6. Cancel stops the producer within one heartbeat.
7. A retried start request returns the same stream.
8. A retried append after an injected timeout stores one entry and shows one
   event to the client.
9. A late `append` after expiry raises and does not recreate the key.
10. A reconnect storm (many readers, large replay) leaves live delivery on
    other streams within a latency bound.
11. Stream, meta, `alive` and active slot keys share a Cluster slot, and a
    multi-key transaction on them succeeds. This runs in the one-off Cluster check below,
    not in CI.
12. Releasing the active slot by a stale writer does not free a newer run.
13. A response ends at `expires_at` with `credential_expired`, and at
    `stream_max_connection_seconds` with no event. A reconnect after the
    session is revoked fails in the application's dependency.
14. A reader stops after the final entry, and a reconnect at or past it gets
    a 204, also when meta never recorded the final ID. An append after
    `finish()` raises `StreamFinished`; an entry a stale writer adds after the
    end reaches no reader.
15. `spawn()` writes `stream.end` with the right reason when `fn` returns,
    raises or is cancelled, and writes nothing when `fn` called `finish()`.
16. A writer and a reader that end a stream at once leave one final entry. A
    cancel or a shutdown that arrives while `spawn()` writes the final entry
    does not stop that write.
17. After `attach()`, the old writer's next append or heartbeat raises
    `StreamFenced`, and at most one of its entries lands after the new
    writer started. Its `finish()` raises `StreamFenced` and writes nothing,
    and a spawned producer that is replaced ends without a final entry and
    without a warning.
18. A handler `after` beats `Last-Event-ID`, which beats `?after=`. A
    reconnect that sends both a newer header and an older query replays
    nothing twice.
19. `$`, `+`, `-`, a bare `<ms>`, a part above 2^64 − 1, a part longer than
    20 digits, non-ASCII digits, and two headers or two query values each get
    422, even when another cursor would win. A scope or stream ID outside the
    rules gets 422 with code `invalid_stream_name` on every route.
20. Cursors compare as numbers: a cursor with fewer digits than the stream's
    IDs is treated as older, not newer.
21. A cursor above `last-generated-id` gets 409, also after entries were
    trimmed and on a stream with no entries yet.
22. A Redis failure shorter than `stream_read_retry_seconds` keeps the
    connection open with pings and loses no entry. A longer one, a shutdown
    and the connection cap each close with no event, and the reconnect resumes
    from `Last-Event-ID`.
23. A client that stops reading during a large replay is closed within
    `stream_send_timeout_seconds`, its reader slot is freed at once, and the
    slow-consumer counter rises. Its reconnect resumes from its own last
    entry.
24. A response that `sse()` built but that never ran takes no reader slot. A
    response over `stream_max_readers` sends 503 before any byte and takes no
    slot. A response that ends normally, by disconnect, by send timeout, by
    cancellation or by error releases its slot.
25. An event type with CR, LF or NUL, an empty one, or one over 128
    characters is rejected before Redis. A payload whose encoding holds line
    breaks reaches the browser as several `data:` lines and decodes to the
    same text.
26. A retried start returns `created=False`, starts no second producer, and
    its writer refuses to append. This holds while the run goes on and after
    it ends. Concurrent starts of one run create it once; a start that loses
    the `WATCH` race reads again; a start whose reply was lost still gets
    `created=True`.
27. A cancel or an `attach()` on a meta hash that expired a moment before
    leaves a key with a TTL, never one without.
28. A producer that appends but never runs `keepalive()` is not declared
    lost while it keeps appending, and keeps its active slot.
29. A reader that finds a lost producer never recreates a stream that expired
    mid-read, and the reconnect gets 410. A lost producer that never appended
    gets a stream holding only the end.
30. `follow()` raises its errors at the `await`, before any event, and its
    reader closes for good on `async with` exit.
31. `start()` spawns once for a retried start. `spawn()` refuses a retry
    handle and a writer it already runs; `extend()` refuses a retry handle.
32. Every error body holds a stable `code`, one handler serves the whole
    family, and an application's own `StreamError` handler is kept.
33. `expires_at` and the connection cap end a response on time with the
    default 15-second heartbeat. A replay page goes out as one chunk, after a
    `retry:` hint between 1 and 3 seconds.

The stream tests run against Redis 8.8, which `ci.yml` already tests. On the
7.4 entry, one test checks that the probe finds no `XCFGSET`, that the
application still starts, and that every stream entry point raises
`StreamUnsupported` and every SSE route answers 503. Another checks that an
unreachable Redis at startup leaves the result unknown and the first call
probes again. Whether fakeredis
supports `XADD` `IDMP`, `NOMKSTREAM` and `XINFO STREAM` fields is unverified;
check before relying on it. If it does not, the stream tests need a real
server.

**Cluster and Sentinel.** Goal 8 covers both, but CI runs one standalone
Redis, and a standalone pass does not prove sharded behavior. Before the first
release, a one-off local check runs the stream tests against a Redis Cluster
with at least three primaries and against a Sentinel setup with one replica.
It is not part of the pipeline, and its scripts and configuration are not
committed. It must show at least these:

- Test 11 on Cluster: the stream, meta, `alive` and active slot keys share a
  slot, and the start (`WATCH` and `MULTI`), append, end and first-page
  transactions succeed. Streams in different scopes land on different
  primaries, and each still works.
- Support detection asks a random node and gives the same answer on every
  primary.
- On Sentinel, a failover during a run: readers reconnect to the new primary
  through the SDK's Sentinel client, and the cursor rules hold (a reader past
  lost entries gets 409, a reader behind them sees `stream.gap`).

The result, with the date, the Redis version and the topology, is written
into this section.

Partial check, 2026-10-26: Redis 8.8.0, a Cluster of three primaries and no
replicas, redis-py 8.0.1, Python 3.10. A script, not the test suite, showed
that the four keys share a slot; that eight concurrent starts of one run
create it once and concurrent starts of other runs get `StreamBusy`; that an
append, `keepalive()` and `suspend()` (now `extend()`) refresh the slot; that `finish()` frees
it; that a retry after the end is not created; that `attach()` and a reader
work; and that a lost producer is ended and its slot freed. The same check
found that the first page's `XREAD` failed inside the transaction, which is
why it is an `XRANGE` now. Different primaries per scope and Sentinel remain
unchecked. Later changes are not checked again automatically; the
check is repeated by hand when a change touches keys, transactions or
connection handling.

## 14. Risks and open questions

Risks:

| Risk | Mitigation |
|---|---|
| Blocked readers exhaust the connection pool. | `stream_max_readers`, a documented sizing rule, a 503 on overload. A dedicated pool is a later option. |
| Trimming makes new readers with no cursor, and reconnects with an old cursor, see 410. A very slow reader sees a `stream.gap` event. | Generous default `stream_maxlen`; clear error; the database holds the final message. |
| Users on Redis below 8.6.2 cannot use the feature. | Accepted (decision 1). The feature turns itself off with one startup warning and 503 on its routes; other SDK features keep their lower floor. |
| Cluster or Sentinel behavior breaks after the one-off check, unnoticed by CI. | Accepted. Repeat the local check by hand when a change touches keys, transactions or connection handling (section 13). |
| Redis 8.6.0 and 8.6.1 pass the probe despite their IDMP defects. | The docs require 8.6.2, and advise 8.6.6 or later. |
| Server defaults for IDMP retention are unknown to this design. | Read `stream-idmp-duration` and `stream-idmp-maxsize` defaults before release; add `XCFGSET` later if they are too large. |
| Docs disagree on `XCFGSET` option names. | Settled: Redis 8.8 accepts `IDMP-DURATION` and `IDMP-MAXSIZE`. |
| Eviction under memory pressure removes active streams. | Document `volatile-lrm`; the failure is a clean 410. |
| In-process producers die on deploy. | A reader ends the stream with reason `producer_lost`. Phase 1 offers no producer that survives a deploy; the docs say so, and a worker interface is a later step (section 15). |
| Users write to streams from their own worker processes anyway, since the keys and commands are open. | The docs state that only FastAPI applications are supported writers in phase 1, and the key layout may change without notice for other writers. |
| Readers need write access to end a dead producer's stream. | Document the ACL. A read-only reader falls back to `stream.error` `producer_lost`. |
| A revoked session keeps an open connection. | The connection ends at `stream_max_connection_seconds`, and the reconnect fails in the application's dependency. |
| An application forgets its access dependency. | The guide shows the dependency on every route. Without it, the scope alone protects nothing: anyone can name any scope. |
| An application builds the scope from a raw path ID that it resolves as an alias or tenant-local ID. | The guide says to build the scope from the object the dependency returned, as PR #89 does. |

Decisions:

1. **Minimum server version: decided 2026-10-15, revised 2026-10-23.** This
   feature requires Redis 8.6.2 or later, and the first release is simplified
   to match: no fallback paths and no Lua. Support is detected with
   `COMMAND INFO` once per pool, as for the other features, and a server
   without support turns the feature off instead of stopping the application.
   See sections 3.1 and 11.
2. **Authorization: decided 2026-10-23.** The application's FastAPI
   dependency decides access. The SDK has no `owner` and compares nothing on
   read: a stream's key is derived from the authorized scope and the stream's
   ID, so an ID from another scope names no stream. The SDK ends a connection
   at the
   credential deadline (for example the session's) or at
   `stream_max_connection_seconds`. The stream TTL does not follow the session
   TTL. See section 5.6.
3. **404 versus 410: decided 2026-10-23.** Meta outlives the stream by
   `stream_gone_ttl_seconds`, and a `started` flag marks a stream that has
   received its first append. A reconnect to an expired stream gets 410 within
   the window and 404 after it. See sections 6.1 and 7.2.

Open questions for the owner:
4. **Scope of `spawn()`.** Should the SDK own in-process producer tasks, or
   only provide `open()` and `append()` and leave task management to callers?
5. **Dedicated pool for readers.** Share the pool and document sizing, or add a
   separate pool setting now?
6. **Default `stream_maxlen`.** 10,000 is a guess. Is there a typical agent
   turn size to design for?
7. **Naming.** `StreamBackend` matches caching and rate limiting. Sessions use
   `Store`. Which fits better?

## 15. Phasing

Phase 1 is the whole straight path: start a run, stream it, resume after a
disconnect with no gap or duplicate, cancel, time out a dead producer, and
clean up finished streams. That means:

1. **Phase 1.** Async `StreamBackend`, writer, `follow()`, `sse()`, cancel,
   active slot, `spawn()`, support detection, telemetry, docs, tests on the
   existing 8.8 CI entry.
2. **Phase 2.** Sync writer. Optional byte-bounded replay with `MAXSIZE`, and
   thread-level reads across several turns with `MAXCOUNT`, both on 8.10 or
   later. Event coalescing for token deltas, which LibreChat does to cut write
   load, at the cost of a small delay. Per-stream IDMP tuning with `XCFGSET`,
   if the server defaults prove too large. An interface for writers and
   readers outside FastAPI, if users ask for it: a public `StreamBackend(redis,
   *, settings=None)`, one instance per process, an async context manager for
   its startup and shutdown, and a rule that workers and the API share every
   setting that shapes stored data (prefix, TTLs, `stream_gone_ttl_seconds`,
   `stream_maxlen`, `stream_max_event_bytes`).
3. **Phase 3, only on demand.** WebSocket reader. A durable dispatch queue on
   consumer groups, together with idempotency keys supplied by the
   application, so a redelivered job does not repeat its deterministic events
   (section 6.3).

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
