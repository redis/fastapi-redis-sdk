# Resumable streams: our design compared with PR #89

Date: 2026-10-21, updated 2026-10-23, 2026-10-26 and 2026-10-27
Status: analysis, for review
Inputs: [`resumable-streams-design.md`](resumable-streams-design.md) and
[PR #89](https://github.com/redis/fastapi-redis-sdk/pull/89)
(`docs/specs/resumable-events.md`, by Julien Ruaux).

Both designs rest on the same base: Redis Streams, the entry ID as the SSE
cursor, one source of truth with no Pub/Sub, idempotent `XADD IDMP`, no
consumer groups, and a disconnect that never cancels the work. The two designs
differ in scope, in how readers are served, and in who owns lifecycle and
access.

This document sorts the differences by the phases in section 15 of our design.
Sections 2 to 5 cover phase 1. Section 6 collects the differences that belong
to phase 2 or phase 3, as a separate future step.

## 1. Scope: agent runs versus a generic event log

This is the root difference, and most of the others follow from it.

- **Ours** is built around agent turns. One stream holds one turn. The SDK owns
  the run's lifecycle: `open()`, `spawn()`, `cancel()`, one active run per
  scope, and `producer_lost` detection.
- **PR #89** is a general event log for chat, notifications and progress
  updates. It has no agent concepts, no job runner, no cancel, no active slot
  and no liveness check. Its example uses one long-lived stream per *chat*, not
  per turn.

So the PR has no answer to our goals 5 (stop is separate from disconnect), 6
(one active run per scope) or the dead-producer case. It leaves all three to
the application.

## 2. Phase 1 differences

| Area | Ours | PR #89 |
|---|---|---|
| **Redis floor** | 8.6.2, because 8.6.0 and 8.6.1 have IDMP defects. Support is detected with `COMMAND INFO XCFGSET DELEX` once per pool, as the rate limiter and sessions detect theirs, so no `INFO` permission is needed; a server without support turns the feature off (503) and the app starts. The probe cannot tell 8.6.0 and 8.6.1 from 8.6.2. | 8.6. Side-effect-free command probes check capabilities, so the SDK needs no `INFO` permission. |
| **Authorization** | Adopted from the PR on 2026-10-23: the application's FastAPI dependency decides, and the SDK has no `owner`. Like the PR, the stream's name comes from the authorized resource: the key holds the scope and the stream ID, so an ID from another scope names no stream and gets a 404. The SDK compares nothing on read. | The application's FastAPI dependency decides. The stream name is built from the authorized resource (`chat:{chat.id}`). The SDK has no owner or scope check. |
| **Connection deadline** | Adopted from the PR: `expires_at`, filled from the SDK session's deadline by `session_expiry(request)`, and a 5-minute cap. | An `expires_at` credential deadline, and a 5-minute cap. |
| **TTL** | Sliding: every append refreshes it (10 min active, 2 min done, `extend()`). | Absolute, set at `create()`, never extended. A renewal API is deferred. An expired identity is never reused. |
| **Creation** | `open()` takes the active slot and writes meta in one `WATCH`/`MULTI` transaction (changed 2026-10-26), so a run is never half made. Meta marks a retried start. The first `XADD` creates the stream; later appends use `NOMKSTREAM`. | An explicit, idempotent `create()`. A `WATCH`/`MULTI` transaction writes a hidden bootstrap entry and sets the deadline. A different configuration for an existing stream is a conflict. |
| **Idempotency keys** | SDK-internal: writer ID plus sequence `q`, and one fixed pair for the final entry. Keys from the application are deferred to phase 3 (section 6.3). `attach()` fences the old writer. | Supplied by the application: a stable `producer` and an `idempotency_key` per logical event. |
| **Append checks** | One `MULTI`: `XADD` + `PEXPIRE` + `HMGET` of the cancel flag and the writer. `finish()` alone checks the writer under `WATCH` first (2026-10-26), so a replaced writer cannot end the new writer's run. | `WATCH` on meta to check existence, deadline and sealed state, with bounded retries. |
| **Trimming and gaps** | `XADD MAXLEN ~`. One check before replay gives a 410: `max-deleted-entry-id` for `XDEL`, and `entries-added` against `length` and the first entry for trimming, which does not move that field. No cursor counts as `0-0`. A gap found later, from a jump in `q`, is a `stream.gap` event, and the reader goes on. | Bans untracked `MAXLEN`. The backend trims in a guarded transaction and advances a `history_floor` in meta. |
| **Terminal state** | Changed 2026-10-23: one end marker, `f=1`, on a final entry whose type and data the application chooses. `finish()` writes it in one append and sets `status=finished` in the same transaction; meta records no final entry ID (changed 2026-10-26). The writer refuses later appends, and readers ignore anything after the first final entry. When the SDK ends a run itself, `spawn()` writes `stream.end` with the reason in its data. | `finish()` writes the final event and seals the log in one `WATCH`/`MULTI` step. Finality is SDK metadata, so the application keeps its own event names. |
| **404 versus 410** | Adopted 2026-10-23: meta outlives the stream by `stream_gone_ttl_seconds` (1 h), and a `started` flag tells an expired stream from one not started yet. 410 within the window, 404 after it. | Settled: meta outlives payload by 24 h, so a known expired stream returns 410. |
| **Errors after headers** | Decided 2026-10-23: no action field. A passing problem (Redis down past a retry bound, shutdown, the connection cap) closes with no event, and `EventSource` resumes. A stream that is over ends: a reader writes `stream.end` for a dead producer, or a vanished stream gets 410 or 404 on reconnect. Only `stream.error` `credential_expired` remains, as a hint, plus `producer_lost` for a read-only reader. The `stream.` prefix is reserved. | A `redis.stream.control` event with no ID and an action (`reload`, `reconnect`, `reauthenticate`). The `redis.stream.` prefix is reserved. |
| **Status codes** | 422 bad or repeated cursor, 404, 410 expired or truncated, 409 busy and `cursor_ahead`, 503 overload, 204 for a cursor at or past the final entry. A cursor before trimmed history stays 410, where the PR uses 409 `history_unavailable`. | 422 bad cursor, 409 `history_unavailable` and `cursor_ahead`, 410 expired, 503 overload or Redis unavailable, 204 for a sealed log already read to the end. |
| **Cursor rules** | Adopted 2026-10-23: handler value, then `Last-Event-ID`, then query; validated at the boundary (no `$`, `+`, `-`, bare `<ms>`, or parts above 2^64 − 1); numeric comparison; `0-0` allowed; duplicates rejected; a cursor above `last-generated-id` gets 409. | Explicit order: handler value, then `Last-Event-ID`, then query. Numeric comparison, `0-0` allowed, duplicate values rejected, a cursor past the tail rejected. |
| **Admission** | `stream_max_readers` (256) live readers per process; 503 over the limit. | 1,000 subscribers and 256 active streams per process; 503 `stream_capacity_exceeded` over either. |
| **Slow consumers** | Adopted 2026-10-23: a send timeout (`stream_send_timeout_seconds`, 10 s) closes a reader whose client stops reading. No queue, because the reader pulls from Redis only when it can send. | A 10 s send timeout disconnects a blocked subscriber. |
| **Key names** | `<prefix>:stream:{<scope>}:<id>` and siblings, all hashed on the scope so the active slot shares their Cluster slot (changed 2026-10-26), with `<prefix>` from the SDK's `prefix` setting, as every other feature uses (adopted 2026-10-23). No layout version. Scopes and IDs are limited to characters that cannot break the hash tag, so no encoding is needed. | `settings.pattern_prefix("streams")`, a layout version (`v1`), base64url-encoded names, and an optional `key_builder`. |

## 3. Public API differences in phase 1

This section lists every public API difference in phase 1 scope. It excludes
the relay settings and the separate blocking client, which section 6 covers.

### 3.1 Setup, construction and packaging

| Item | Ours | PR #89 |
|---|---|---|
| Fluent setup | `FastAPIRedis.streams()` | `FastAPIRedis(app).streams(...)`; same idea. |
| Functional setup | `add_redis_streams(app, *, coder=None)`, the same as `.streams()`, as every feature has its `add_redis_*` function. No injected backend and no `manage_lifespan`. | `add_redis_streams(app, backend=None, manage_lifespan=True, ...)`. Registers an injected backend; `manage_lifespan=False` leaves startup and shutdown to the application. |
| Settings | New `stream_*` fields on `RedisSettings`. | The same fields on `RedisSettings`, plus `StreamSettings`, a model that never reads the environment. Used for direct construction in workers. |
| Construction | Not supported in phase 1. `StreamBackend` is exported for type hints, and its constructor has the shape of the other backends (`client`, `coder`, `capabilities`), but `.streams()` creates the one backend per process at startup, and `get_stream_backend` returns it. Only FastAPI applications write and read (design section 2). | `StreamBackend(command_client, settings=...)` with an application-owned client. The backend is an async context manager and has `initialize()` for append-only workers. |
| Dependency | `StreamBackendDep`, `get_stream_backend` | Same names. |
| Key customization | None. | `key_builder(stream, *, prefix)`. |
| Packaging | No new extra. | A `streams` extra for FastAPI 0.135+ (`fastapi.sse.EventSourceResponse`). |

### 3.2 Writing

| Item | Ours | PR #89 |
|---|---|---|
| Stream identity | The pair (`scope`, `stream_id`). The application builds the scope from the authorized resource; the SDK generates the ID, or the caller passes one (for example an idempotency key). One stream per turn. | One application-chosen `stream` string, for example `chat:{id}`. One stream per chat. |
| Create | `await start(fn, *, scope, stream_id=None, exclusive=True) -> StreamWriter` opens the run and spawns `fn` only for a new one (2026-10-26). `await open(...)` and `spawn()` remain for a route that does work between them. | `await create(stream, *, ttl) -> None` |
| Writer object | `StreamWriter` with `stream_id`. Every write goes through it. | None. Every write is a backend method that takes the stream name. |
| Append | `await writer.append(type, data) -> str` (the entry ID; changed 2026-10-26, was `AppendResult`). The cancel flag is `writer.cancelled`. Data goes through the SDK's `Coder`; the default, `JsonableCoder` (2026-10-27), also takes Pydantic models, dataclasses and datetimes. | `await append(stream, *, event, data, producer, idempotency_key) -> str` (the cursor). JSON only. |
| Finish | `await writer.finish(type, data=None) -> str` writes the application's final event with the end marker and returns its ID. Later appends raise `StreamFinished`. A writer that `attach()` replaced raises `StreamFenced` and writes nothing. | `await finish(stream, *, event, data, producer, idempotency_key)` writes the application's final event and seals. |
| Fail | None. A failure is the data of the final event; `spawn()` writes `stream.end` with `{"reason": "error"}`. | None. |
| Continue elsewhere | `await attach(stream_id, *, scope) -> StreamWriter` | Not needed: any process can append with the same `producer`. |
| Extend TTL | `await writer.extend(ttl)` (renamed from `suspend()` on 2026-10-26: it pauses nothing). Only the run's own writer may call it. | None. A renewal API is deferred. |
| Heartbeat | `writer.keepalive()` | None. |

### 3.3 Reading

| Item | Ours | PR #89 |
|---|---|---|
| Core read | `await follow(stream_id, *, scope, after=None) -> StreamReader` (changed 2026-10-26). The checks run at the `await`, before any event, as `sse()` runs them before the first byte. The `StreamReader` is an async iterator and its own async context manager, and reads nothing until iterated. | `subscribe(stream, *, after=None)`: an async context manager that yields an async iterator and releases resources on every exit path. |
| SSE | `backend.sse(stream_id, *, request, scope, after=None, expires_at=None) -> Response`, a plain method. | `await sse_response(request, *, backend, stream, after=None, expires_at=None)`, a module function. The route declares `response_class=EventSourceResponse`. |
| Session deadline | `session_expiry(request) -> datetime \| None` reads the SDK session's absolute deadline. | None. The application supplies `expires_at` itself. |
| Event model | `StreamEvent(id, type, data, final)`. `id` is `None` for the reader's own events. | `StreamEvent` with `id`, `event`, `data`, and finality as separate metadata. |
| Reserved names | The `stream.` prefix: `stream.end` (stored), `stream.gap` and `stream.error` (reader only). | The `redis.stream.` event prefix. Event names with CR, LF or NUL are rejected. |

### 3.4 Run control (ours only)

`spawn(writer, fn) -> asyncio.Task`, `cancel(stream_id, *, scope) -> bool`,
`active(scope) -> str | None`, and `producer_lost` detection, which ends a dead
producer's stream with `stream.end`.
The PR has no equivalent for any of them.

### 3.5 Errors and HTTP contract

| Item | Ours | PR #89 |
|---|---|---|
| Exceptions | `StreamError`, `StreamNotFound`, `StreamExpired`, `StreamBusy`, `StreamFinished`, `StreamFenced`, `StreamInvalidCursor`, `StreamInvalidName`, `StreamCursorAhead`, `StreamCredentialExpired`, `StreamUnsupported`, `StreamStoreError`, `StreamConfigurationError`. Since 2026-10-26 each class carries its status and a stable `code` (`stream_not_found`, `stream_expired`, `cursor_ahead`, `invalid_stream_name`, `stream_unavailable` and so on), one handler serves the family, and the body is `{"detail", "code"}`; the reader cap answers `stream_capacity_exceeded`. | A typed `StreamError` family with stable codes: `stream_not_found`, `stream_expired`, `history_unavailable`, `cursor_ahead`, `invalid_stream_request`, `stream_capacity_exceeded`, `stream_unavailable`, and a conflict on `create()`. |
| Status codes | See section 2. A bad scope or stream ID is 422 on every route (2026-10-26); it was a 500 on write and a 404 on read. | See section 2. |
| Error after headers | Mostly none: the connection closes, or the stream ends with `stream.end`. `event: stream.error`, no `id`, only for `credential_expired` at `expires_at` and for a read-only reader's `producer_lost`. | `event: redis.stream.control`, no `id`, data holds a code and an action. `reauthenticate` at `expires_at`. |
| Sealed and fully read | 204 for a cursor at or past the final entry, decided from the stream: the cursor's own entry is final, or the run has finished and nothing after the cursor is final. Adopted. | 204, which stops `EventSource` retries. |
| Cursor input | Handler `after`, then `Last-Event-ID`, then `?after=`, read by `sse()` itself. Duplicates rejected. Adopted. | Handler `after`, then `Last-Event-ID`, then `?after=`. Duplicates rejected. |

### 3.6 Configuration

| Setting | Ours | PR #89 |
|---|---|---|
| TTL | `stream_active_ttl_seconds` 600 s, `stream_done_ttl_seconds` 120 s, sliding. Renamed on 2026-10-26 from `stream_ttl_active` and `stream_ttl_done`: every stream duration is now a float named `*_seconds`. | A `ttl` per `create()` call, absolute. |
| Expiry recognition | `stream_gone_ttl_seconds` 3,600 s, renamed from `stream_expiry_window`: meta kept that long after the stream. Adopted. | Meta kept 24 h after the payload deadline. |
| Entry cap | `stream_maxlen` 10,000. | 10,000 application events; same. |
| Event size | `stream_max_event_bytes` 65,536. | 64 KiB; same. |
| Heartbeat | `stream_heartbeat_seconds` 15. | 15 s keepalive comment; same. |
| Producer liveness | `stream_alive_ttl_seconds` 45. | None. |
| Replay page | `stream_replay_page` 100. | Byte and entry budgets per read (section 6). |
| Admission | `stream_max_readers` 256. | 1,000 subscribers and 256 active streams. |
| Connection lifetime | `stream_max_connection_seconds` 300, or less with `expires_at`. Adopted. | 5 min, or less with `expires_at`. |
| Send timeout | `stream_send_timeout_seconds` 10 s. Adopted. | 10 s. |

## 4. The trade-offs that matter

**Per-append cost.** Our append is one `MULTI` (`XADD` + `EXPIRE` + `HGET`) in
one round trip. The PR checks the deadline and sealed state under `WATCH` on
every append, with bounded retries. That costs extra round trips, which matters
for token-by-token output.

**Gap detection.** The PR keeps a `history_floor` in meta and trims in its
own transaction. We read what Redis already records: `max-deleted-entry-id`
catches an `XDEL`, and `entries-added`, `length` and the first entry catch
trimming. Trimming does not move `max-deleted-entry-id`, which we found on a
real server (Redis 8.8), so our trimming test is conservative rather than
exact: a client whose last entry was exactly the newest one trimmed gets 410
although it missed nothing. The PR's floor is exact in that case, at the cost
of a trim transaction and a ban on `MAXLEN`.

**Idempotency reach.** The PR's keys reach further: they survive a producer
that crashes and runs again, where ours would store the repeated events a
second time. But in our phase 1 no producer ever runs again. The SSE reader is
not tied to the producer, and a task from `spawn()` that dies is not restarted:
a reader ends its stream with reason `producer_lost`, and the user starts a
new run with a new stream.
A producer only runs again when the application puts the work on a durable
queue that redelivers it, which both designs leave out. Even then, a model run
again produces different tokens, so keys help only with deterministic events
such as tool results. We therefore keep our keys, which cover a retry after a
timeout, and defer application keys to phase 3 with the dispatch queue. Two
cheap parts stay in phase 1: a fixed IDMP pair on the final entry, so a stream
has at most one, and a fence, so a writer replaced by `attach()` stops at its
next append.

**Authorization.** The PR's split is cleaner for a generic SDK: the application
decides access, and the SDK carries events. Our old `owner` string was simpler
to use, but it built in one access model. We have adopted the split, and the
PR's way of binding a stream to its resource: the name is derived from what
the application authorized. The PR needs nothing more, because its stream
name is the chat. We keep one stream per turn, so a chat has many streams, and
a turn's ID alone could be replayed under another chat. Putting the scope in
the key closes that without any check: a turn ID from another chat names a key
that does not exist. Access then attaches to the resource, not to a session,
so shared chats, several tabs and a second device all work. A list of streams
kept in the session would not: session data belongs to one session, so the
second device would not have it, and two tabs would race to write it. The
PR's credential deadline and 5-minute lifetime close
a gap our design never addressed: a revoked user kept an open stream forever.
With the SDK's sessions, the deadline comes from the session.

**Terminal state.** We had three end types (`done`, `error`, `cancelled`); the
PR has one sealed final event with application-chosen names. We adopted the
PR's model, for a reason the PR does not state: resumability needs only one
fact at the end, that no more entries will come. How a run ended is data about
the run, not about the stream, and no stream layer in the research models it.
Our version needs no `WATCH`. The writer refuses appends after `finish()`, and
a late append from a stale writer is harmless, because readers stop at the
first final entry and a reconnect after it gets a 204. We kept one thing the
PR lacks: when the SDK ends a run itself, in `spawn()`, it still writes a final
event (`stream.end`), so readers never wait for a run the SDK knows is over.

**Session TTL.** We considered tying the stream TTL to the session TTL, and
rejected it. The stream TTL bounds one run's data and cleans up after a dead
producer. A session bounds a user's sign-in. Several sessions can follow one
run, and a worker that appends has no session, so the end of one session must
not end the stream. Only the connection follows the session.

## 5. What to address first

### 5.1 Adopt in phase 1

Ranked by how much harm the gap does in our design today.

1. ~~**A reconnect after the end.**~~ Adopted in the design on 2026-10-23
   (design sections 5.2 and 7.2). `EventSource` reconnects by itself after the
   server closes, with the final entry's ID as its cursor. Before, the reader
   then blocked until the TTL ended, or reported a false `producer_lost`. Now
   meta records the final entry's ID, and a cursor at or past it gets a 204,
   which stops `EventSource` for good. With this, the client needs no
   knowledge of which event is last.
2. ~~**One final entry and a writer fence.**~~ Adopted in the design on
   2026-10-23 (design sections 6.3 and 7.1). Every final entry uses the fixed
   pair `IDMP end final`, so two writers that end a stream at once leave one
   final entry. `attach()` records the new writer in meta, and the old writer
   raises `StreamFenced` on its next append or heartbeat. Keys supplied by the
   application, proposed here before, moved to phase 3 (section 6.3): with
   `spawn()` no producer runs again, so they have nothing to protect yet.
3. ~~**Credential deadline, connection lifetime and access.**~~ Adopted in the
   design on 2026-10-23 (design section 5.6, decision 2). `sse()` takes
   `expires_at`, `session_expiry(request)` fills it from the SDK session, and
   `stream_max_connection_seconds` (default 300) caps every connection. The
   SDK dropped `owner`; the application's dependency decides access, and the
   stream key is derived from the authorized scope.
4. ~~**404 versus 410.*~~* Adopted in the design on 2026-10-23 (design sections
   6.1 and 7.2, decision 3). Meta outlives the stream by `stream_gone_ttl_seconds`
   (1 h), and the first append sets `started=1` in meta. A flag, not a payload
   deadline, tells an expired stream from one not started yet, so no process
   compares clocks. Within the window a reconnect gets 410, and after it 404.
5. ~~**Cursor rules.**~~ Adopted in the design on 2026-10-23 (design sections
   5.1 and 7.2). The order is handler value, then `Last-Event-ID`, then query,
   and `sse()` reads the request's cursor itself, so a route cannot let a stale
   query beat a newer header. Cursors are validated and parsed into integer
   pairs at the boundary; `0-0` is allowed; duplicates and malformed values get
   422. A cursor above `last-generated-id` gets 409, which catches writes lost
   in a failover.~~~
6. ~~**An action on errors after headers.** Decided on 2026-10-23 as not
   needed (design sections 5.5 and 7.2). The reserved `stream.` prefix is
   adopted. The `action` field is not: in our design every problem after the
   headers is either passing, so the server closes and `EventSource` resumes
   from `Last-Event-ID`, or over, so the stream ends and the reconnect gets
   204, 410 or 404. For a dead producer, the reader writes the `stream.end`
   the producer never wrote, with the fixed IDMP pair, so reconnects stop
   instead of waiting out the `alive` timeout again and again. The PR needs
   actions because it has no end marker for a dead producer and treats a trim
   during the read as fatal, which we handle with `stream.gap`. The one event
   left is `stream.error` `credential_expired`, a hint that saves the client
   a probe, because `EventSource` never shows the 401.~~
7. ~~**Send timeout.** Adopted in the design on 2026-10-23 (design section
   7.2), as a low-priority improvement, not a defect fix. A client that stops
   reading leaves the reader waiting in `send()`, holding a reader slot. It
   holds no Redis connection, as this item first claimed, because the reader
   reads nothing while it waits. The connection cap already bounds the wait at
   5 minutes; the send timeout shortens it to 10 s while data flows, which
   matters in a reconnect storm. It cannot catch a dead client on an idle
   stream, because pings never fill the send buffer; the cap still covers
   that case.~~
8. ~~**A public constructor for workers.** Decided on 2026-10-23: out of
   phase 1 (design section 2, non-goals). Phase 1 supports writers and
   readers inside a FastAPI application only. A worker could reach the same
   keys, but phase 1 offers no public constructor, settings model or lifecycle
   for it, and does not support it. The worker interface moved to the future
   step (section 6.1). The design now states one rule this item exposed,
   which FastAPI needs too: there is one `StreamBackend` per process, created
   by `.streams()`, because it holds the tasks from `spawn()` and the reader
   count.~~
9. ~~**Versioned key names.** Decided on 2026-10-23 (design section 6.1).
   Keys take their prefix from the SDK's `prefix` setting, as every other
   feature does, so applications with different prefixes never share a stream
   and ACL patterns on the prefix cover streams. `.streams()` rejects a prefix
   with braces. A layout version is not adopted, and none is planned: stream
   data lives for minutes, so a future layout change can wait for old keys to
   expire, and we add versioning only if users ask for it.~~
10. ~~**Capability probes.** Settled on 2026-10-23 (design section 11),
    together with item 13. The `INFO` version check is gone. The feature
    detects support with `COMMAND INFO XCFGSET DELEX` once per pool, the way
    the rate limiter and sessions detect `INCREX` and `HSETEX`. This needs no
    `INFO` permission and sends no command to a scratch key. A server without
    support turns the feature off: one warning at startup, `StreamUnsupported`
    from every entry point, 503 on the SSE routes, and the rest of the
    application keeps running. The cost: 8.6.0 and 8.6.1 have the commands, so
    the probe cannot catch their IDMP defects; the docs require 8.6.2.~~

**Added after a second review of the PR, 2026-10-23.** The PR has not changed
since the first review. These four items come from parts of it we had not yet
compared, and are critical for phase 1. Nine more from the same review are
deferred to phase 2 (section 6.1). Item 11 is a defect confirmed in the demo
stub, which the design does not rule out, and item 13 was checked against the
installed redis-py.

11. ~~**A reader slot taken but never released.** Fixed in the design on
    2026-10-23 (design section 9, reader slots) and in the demo stub.
    `sse()` only prepares the response; the SDK's SSE response class takes
    the slot when it runs, before any header, sends 503 if none is free, and
    releases the slot in a `finally`. A response that is never sent never
    takes one. Keyspace notifications were considered and rejected: the slot
    is a counter in the process, not a Redis key, and stream expiry would
    free it minutes late, if the notification arrived at all.~~
12. ~~**SSE frame injection through event types.** Fixed in the design on
    2026-10-23 (design sections 6.2 and 9) and in the demo stub. `append()`
    and `finish()` reject a type that contains CR, LF or NUL, is empty, or is
    longer than 128 characters. The SSE encoder writes a payload with line
    breaks as several `data:` lines, so a custom `Coder` cannot break a frame
    either.~~
13. ~~**The redis-py floor.** Settled on 2026-10-23 (design section 11), and
    revised on 2026-10-26. The writer sends `XADD … IDMP`, `DELEX … IFEQ` and
    `SET … IFEQ` with `execute_command`, as the session store sends `HSETEX`.
    The 2026-10-23 decision kept the SDK's `redis>=6.0.0`, which was wrong:
    every stream write runs in a transaction, and async `RedisCluster`
    supports transactions only from redis-py 6.2. The floor is now
    `redis>=6.2.0`. Server support is detected as in item 10, and the feature
    turns off when it is missing.~~
14. ~~**Cluster and Sentinel are claimed but not tested.** Settled on
    2026-10-23 (design section 13). Goal 8 stays. Before the first release, a
    one-off local check runs the stream tests against a Cluster with at least
    three primaries and a Sentinel setup with a failover. It is not part of
    the pipeline and is not committed; its result is recorded in the design,
    and it is repeated by hand when a change touches keys, transactions or
    connection handling. A partial check ran on 2026-10-26 (design section
    13); it found that the reader's first page failed on Cluster, which is
    fixed.~~

Considered from the PR's API and not suggested: `subscribe()` in place of
`follow()` (`follow()` now returns a reader that is its own context manager,
section 5.3); `sse_response()` as a module function (with FastAPI as the only
supported host, `sse()` on the backend is enough); an injectable backend for
`add_redis_streams()`, `StreamSettings` and `key_builder` (out of phase 1,
design section 2);
`create()` with conflict detection (`open()` covers our case); and 409
`history_unavailable` in place of our 410 (the client does the same thing for
both).

### 5.2 Keep our design

- **Sliding TTL and `extend()`.** An absolute TTL cannot cover a turn that
  waits on a slow tool. It also makes the application manage a new stream
  identity each time a delivery window ends. The stream TTL also stays
  independent of the session TTL (section 4).
- **Redis's own stream fields** over `history_floor`. `max-deleted-entry-id`
  and `entries-added` against `length` need no trim transaction and no ban on
  `MAXLEN`. The trimming test is conservative, not exact (section 4).
- **One `MULTI` per append** over `WATCH` with retries. Only `open()` and
  `finish()` use `WATCH`, each once per run.
- **`open()`** over `create()` with a bootstrap entry.
- **Run control:** `spawn()`, `cancel()`, the active slot and
  `producer_lost`. These are our goals 5 and 6. The PR leaves them to the
  application.
- **The 8.6.2 floor.** We should tell the PR author about the IDMP fixes in
  8.6.2.

### 5.3 API changes from the implementation review, 2026-10-26

A review of the implementation against this comparison changed the API before
the first release, while it is still cheap to change. Each item is in the
design (sections 7 and 8) and the guide.

1. **`start(fn, scope, stream_id)`** opens a run and spawns `fn` only when the
   run is new. Every caller had to write `if writer.created: spawn(...)`, and
   one who forgot replaced the real task in the backend's table with one whose
   first append was fenced. `spawn()` now refuses such a handle with
   `ValueError`.
2. **`follow()` is a coroutine** that returns a `StreamReader`. Its checks
   used to run at the first `anext()`, too late for a WebSocket handler, which
   must close with a code before it accepts. The reader is an async context
   manager, which gives the safety of the PR's `subscribe()` without its
   extra layer.
3. **`append()` returns the entry ID**, as `finish()` does. `AppendResult` is
   gone; `writer.cancelled` already held the flag.
4. **`suspend()` is `extend()`**, and refuses a writer that does not own the
   stream.
5. **One `StreamError` handler, with stable codes**, from deferred item 5. The
   old per-subclass registration replaced any handler the application had set.
6. **`StreamInvalidName` (422)** for a bad scope or stream ID on every route,
   and a cursor pattern that accepts only ASCII digits, at most 20 per part.
7. **Setting names**: `stream_active_ttl_seconds`, `stream_done_ttl_seconds`
   and `stream_gone_ttl_seconds`, so every duration reads the same way.
8. **`StreamEvent` has only its four documented fields.** The stored payload
   no longer rides on it as a private field.

The same review fixed defects behind the API: a cancel or a shutdown no longer
interrupts a final write that is under way; a replaced writer cannot end the
new run; a reconnect at the final entry gets 204 even when meta lacks the
final ID (since 2026-10-26 meta has no such field, and the heartbeat is a meta
field with its own TTL, not a key); a reader ending a lost producer never recreates an expired stream;
Redis error text no longer reaches the response body; the SSE `retry:` hint
is 1 to 3 seconds, not the 15-second heartbeat; a replay page is one send; and
`expires_at` and the connection cap no longer wait for the next heartbeat.

### 5.4 Default coder, 2026-10-27

`JsonableCoder` is the streams default. Producers often hold Pydantic models
or dataclasses, on which `JsonCoder` raised `TypeError`. The new coder
converts what `json.dumps` cannot encode with
`pydantic_core.to_jsonable_python`, and readers decode plain JSON. Caching and
sessions keep `JsonCoder`. The PR has no producer side, so this does not
change the comparison above.

## 6. Future step: phase 2 and phase 3 differences

These differences do not affect the phase 1 API. We return to them when phase
2 starts.

### 6.1 Phase 2

| Area | Ours | PR #89 |
|---|---|---|
| **Reader model** | One `XREAD BLOCK` per reader in phase 1. Shared relay buckets are a later option (section 6.2). | Part of its first release: 16 fixed relay buckets per process. Each bucket runs one multi-stream `XREAD BLOCK` and copies events to bounded per-subscriber queues. Replay runs on separate, bounded reads, at most 8 at a time. |
| **Replay to live** | No splice: one loop does both. | An explicit handoff: replay up to a boundary, join the relay, drop duplicates in an overlap buffer, restart on overflow. |
| **Cluster keys** | Hash tag per scope: `{<scope>}`, shared by the scope's runs and its active slot. | Hash tag per bucket: `{streams:v1:bucket:N}`, chosen by SHA-256 modulo B. Changing B means moving to a new namespace. |
| **`MAXCOUNT`/`MAXSIZE`** | Phase 2, on Redis 8.10 or later. | Optional. Probes select them where the server accepts them; elsewhere the SDK enforces 256-entry and 1 MiB budgets per read with rotating groups of keys. |
| **IDMP retention** | Server defaults. `XCFGSET` per stream in phase 2, if the defaults prove too large. | `XCFGSET` at creation: 24 h and 10,000 IDs per producer, the Redis 8.6 maximum. |
| **Slow-consumer queues** | No queues in model A. | 256 events and 1 MiB per subscriber; past either, the subscriber is disconnected. |
| **Relay resources** | A dedicated reader pool, committed in section 6.2. | A separate `blocking_client` passed to `StreamBackend`. Settings for bucket count, a 1 s block timeout, batch budgets and replay concurrency. |
| **Sync writer** | Phase 2. | None proposed. No difference. |
| **Event coalescing** | Phase 2. | None. |
| **Writers outside FastAPI** | Not supported in phase 1. A later step, if users ask: a public `StreamBackend(redis, *, settings=None)`, one instance per process, an async context manager for startup and shutdown, and settings that shape stored data shared with the API. | In its first release: `StreamBackend(command_client, settings=...)`, an environment-free `StreamSettings`, `add_redis_streams(app, backend=..., manage_lifespan=...)` and `initialize()` for append-only workers. |

**Deferred from the second review.** These items came from the second review
of the PR (2026-10-23). None is critical for phase 1; each waits for phase 2.

1. ~~**`follow()` left open after an early exit.**~~ Done on 2026-10-26
   (section 5.3, item 2): `follow()` returns a `StreamReader` that is an async
   context manager. The original note: `follow()` is an async
   generator. A caller that leaves its `async for` early (a WebSocket handler
   that breaks out of the loop, for example) does not close it, so whatever it
   holds stays held until garbage collection. The PR's `subscribe()` is an
   async context manager for this reason, which "releases them on every exit
   path". Fix, keeping the API: `follow()` acquires nothing before its first
   iteration, and the docs and any SDK adapter wrap it in
   `contextlib.aclosing()`.
2. **Blocked readers and the pool.** Each live reader holds a pool connection
   for its whole `XREAD BLOCK`. The PR reserves blocking capacity "separately
   from ordinary operations so idle relays cannot exhaust the pool needed to
   append the events that wake them". In our design nothing starves by
   default, because the SDK's pool is unbounded (`max_connections=None`).
   With a limit set, design section 10 already requires that
   `stream_max_readers` plus the other traffic fit in the pool, and the
   settings validator rejects a combination that does not. A dedicated
   reader pool (section 6.2, open question 5 in the design) is an
   optimization for later.
3. **Graceful shutdown with open SSE connections.** A server stopping for a
   deploy waits for running responses, and an SSE response runs until the
   connection cap. The design says a shutdown closes readers but names no
   trigger, and the lifespan's shutdown may only run after those connections
   have closed; this needs checking with uvicorn. Without a trigger, every
   deploy waits up to `stream_max_connection_seconds`. The PR's shutdown
   "stops admission, closes subscriptions within a grace period". Fix: on the
   server's shutdown signal, stop admitting readers (503) and close live ones
   with no event, so clients reconnect elsewhere; the docs recommend the
   server's graceful-shutdown timeout.
4. ~~**Redis failures before the first byte.**~~ Done: the first round trip
   raises `StreamStoreError`, answered with 503 `stream_unavailable` and
   `Retry-After`, and since 2026-10-26 its text is fixed, never Redis's own.
   The original note: The design maps unknown,
   expired, busy and bad-cursor cases to status codes, but not a Redis outage
   during the reader's first round trip. The PR answers 503
   `stream_unavailable` and never exposes keys or raw Redis exceptions. Fix:
   the same, with `Retry-After`.
5. ~~**Error bodies with a stable code.**~~ Done on 2026-10-26 (section 5.3,
   item 5). The original note: Our handlers return a free-text
   `detail`. The PR's typed errors carry stable codes (`stream_not_found`,
   `stream_expired`, `cursor_ahead` and so on). A fetch-based client, or a
   page that probes after `EventSource` fails, can then branch on the code,
   not on wording. Fix: every stream error body holds a `code`.
6. **Client guidance in the guide.** The PR spells out several client rules
   our design leaves unsaid. All are documentation:
   - `EventSource` cannot send an `Authorization` header, so apps that use
     bearer tokens need a fetch-based client. A token never goes in the URL.
   - `EventSource` hides the status of a refused connection. Probe, or use a
     fetch-based client.
   - Save the cursor together with the state it produced, and recover with
     the handler's `after` (design section 5.1). This is the snapshot-cursor
     handoff that LangGraph lacks (research, issue 2880).
   - Receiving an event is not storing it, so handlers must tolerate replay.
   - Change the session before the response starts; a write inside the stream
     is lost.
7. **What a failover can lose.** Redis replication is asynchronous, so
   entries the old primary acknowledged can be lost when a replica takes
   over. The PR states this and makes no stronger claim. Ours detects both
   sides: a reader past the loss gets 409 `cursor_ahead`, and a reader behind
   it sees a jump in `q` and gets `stream.gap`. Fix: state this in the design
   and the guide, and say the SDK does not use `WAIT`.
8. **No identifiers in metric labels.** The PR forbids event data,
   credentials, keys, user IDs and stream IDs as metric labels. Our telemetry
   list does not say it. Scopes and stream IDs would make label cardinality
   grow without bound. Fix: one sentence in the telemetry row.
9. **OpenAPI for the SSE route.** Since `sse()` reads `?after=` itself, the
   route declares no query parameter and no `text/event-stream` response, so
   the generated docs show neither. The PR declares
   `response_class=EventSourceResponse`, which needs FastAPI 0.135. Fix,
   within our FastAPI floor: the guide shows `responses=` and `openapi_extra`
   for the route. Moving to FastAPI's own SSE support can wait.

### 6.2 Connections versus complexity

The PR caps blocking connections at B per process, however many subscribers
there are. Ours holds one connection per live reader.

We will start with one `XREAD BLOCK` connection per reader (model A) and keep
shared relay buckets (model B) as an optional, later performance improvement.
Model A is far simpler: one loop does both replay and live tail with no splice.
Model B needs a replay-to-live handoff and per-subscriber queues. It also needs
the 8.10 `MAXCOUNT`/`MAXSIZE` caps, or SDK-side budgets that rebuild them on
older servers. Model A's connection count follows live readers, which is
roughly concurrent running turns times tabs, not online users. So it scales to
the tens of thousands of readers most users of this SDK will run. On
connection-capped managed Redis the limits apply per shard, so our `{<scope>}`
hash tag turns adding shards into the scale-out path without any code change,
as long as readers spread over many scopes.
Model B saves only about readers-per-worker ÷ B connections. With many small
workers (one per pod), that saving often shrinks to little or nothing, so model
B earns its complexity only in dense deployments. We therefore commit to a few
model-A fixes now, chief among them a dedicated reader pool that returns 503
when full and no blocking on finished streams. We add model B behind the
unchanged `follow()` API once benchmarks show a deployment that needs it.

### 6.3 Phase 3

Phase 3 holds the WebSocket reader and a durable dispatch queue, each only on
demand. The PR covers SSE only and excludes job queues, as we do. One
difference belongs here:

| Area | Ours | PR #89 |
|---|---|---|
| **Keys from the application** | Phase 3, with the dispatch queue. A redelivered job calls `attach(stream_id)` with the ID from its payload, and keys stop it repeating deterministic events such as tool results. Token deltas stay unkeyed, because a model run again produces different tokens. | In its first release: `producer` and `idempotency_key` on every `append()` and `finish()`. |

When application keys land, a duplicate must not use up a `q` value, or the reader reports
a false gap. Stream IDs only grow, so the writer can treat a returned ID at or
below its last one as a duplicate and keep `q` unchanged. Keyed events also
need an IDMP window longer than the time to recover a job, which may bring the
`XCFGSET` work forward.
