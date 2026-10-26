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
  `stream_done_ttl_seconds + stream_gone_ttl_seconds` after it ends; later,
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
the SDK. The data goes through the payload coder, JSON by default, and may be
at most `stream_max_event_bytes`. A timeout or a dropped connection is retried
with the same idempotency key, so a retried append never stores the event
twice.

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

The response sends a `: ping` comment every `stream_heartbeat_seconds`, sets
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
closes. Every connection also closes after `stream_max_connection_seconds`,
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

A producer keeps a short-lived heartbeat key in Redis, which every append and
every heartbeat refreshes. When a reader has waited a full heartbeat with no
new entry, it checks that key. If the key has lapsed and the stream has no
final event, the producer is gone, and the reader writes the final event the
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

- **Passing**: Redis down for longer than `stream_read_retry_seconds`, a
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
- **Slow clients.** A send that waits longer than `stream_send_timeout_seconds`
  closes the response, so a frozen tab does not hold a slot. A dead client on
  an idle stream is not caught this way; the connection cap bounds that case.
- **Connections.** Each live reader holds a pool connection for each blocking
  read. With `REDIS_MAX_CONNECTIONS` set, size it for `stream_max_readers` plus
  your other traffic. The driver's `socket_timeout` must be longer than
  `stream_heartbeat_seconds`; `.streams()` refuses a setup where it is not.
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

---

## Redis keys

Every key uses the SDK's `prefix` setting (`REDIS_PREFIX`, default
`redis:fastapi`):

```
<prefix>:stream:{<scope>}:<id>        Stream   the events
<prefix>:stream-meta:{<scope>}:<id>   Hash     status, cancel, final, writer, started
<prefix>:stream-alive:{<scope>}:<id>  String   the producer heartbeat
<prefix>:stream-active:{<scope>}      String   the scope's active run
```

The `{<scope>}` hash tag keeps a scope's keys in one Cluster slot, so one
transaction starts a run and takes the slot, and one transaction ends it and
frees the slot. It also puts all of a scope's runs on one shard, so make the
scope one resource, such as a chat, and not one value for the whole
application. The metadata hash outlives the stream by `stream_gone_ttl_seconds`,
so a late reconnect gets `410` (expired) instead of `404` (unknown). The prefix
must not contain braces.

---

## Settings

Every setting is an environment variable prefixed `REDIS_`, so
`stream_active_ttl_seconds` is `REDIS_STREAM_ACTIVE_TTL_SECONDS`. See
[Configuration](configuration.md) for how these are loaded.

| Setting                         | Default  | Notes                                                                |
|---------------------------------|----------|----------------------------------------------------------------------|
| `stream_active_ttl_seconds`     | `600`    | TTL of a running stream, refreshed by every append                   |
| `stream_done_ttl_seconds`       | `120`    | How long a finished stream stays readable                            |
| `stream_gone_ttl_seconds`       | `3600`   | How long metadata outlives the stream, for `410` instead of `404`    |
| `stream_maxlen`                 | `10000`  | Approximate entries kept per stream                                  |
| `stream_max_event_bytes`        | `65536`  | Largest encoded event                                                |
| `stream_heartbeat_seconds`      | `15`     | SSE ping, blocking-read bound, producer heartbeat                    |
| `stream_alive_ttl_seconds`      | `45`     | Lifetime of the producer heartbeat key; longer than the heartbeat    |
| `stream_replay_page`            | `100`    | Entries per read                                                     |
| `stream_max_readers`            | `256`    | Live SSE readers per process                                         |
| `stream_max_connection_seconds` | `300`    | Longest SSE connection before the client reconnects                  |
| `stream_read_retry_seconds`     | `30`     | How long a reader retries a failing Redis read                       |
| `stream_send_timeout_seconds`   | `10`     | Longest wait for one send to a client                                |

---

## Telemetry

With [telemetry](observability.md) enabled, the feature emits
`redis_fastapi.streams.operations` (by operation and result),
`redis_fastapi.streams.latency` and the `redis_fastapi.streams.readers` gauge.
No scope, stream ID or event data becomes a label.
