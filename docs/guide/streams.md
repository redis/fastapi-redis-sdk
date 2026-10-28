# Resumable events

Streaming adds a finite Redis event log: publishers append JSON events, and
clients replay and follow the log through authenticated HTTP endpoints. Event
production is independent of subscriptions. Disconnecting does not cancel work.

This implements the generic API proposed in [PR #89](https://github.com/redis/fastapi-redis-sdk/pull/89).
The contribution is under qualification; see the limitations below.

## Install and configure

```bash
pip install 'fastapi-redis-sdk[streams]'
export REDIS_URL=redis://localhost:6379
```

Streaming requires Redis database version 8.6 with native producer idempotency.
Startup probes every placement bucket for the Redis 8.10 `XREAD MAXCOUNT`/`MAXSIZE`
options and uses them where available. Other buckets use standard `XREAD COUNT`
with SDK-bounded, rotating key groups. Redis 8.10 is an optimization, not a requirement. The extra requires FastAPI 0.135+ and redis-py
8.0.1+. Existing caching, rate limiting, and session installations keep their
existing requirements; SSE classes are imported only when streaming is enabled.
An unsupported or unreachable Redis fails streaming startup.

```python
from fastapi import Depends, FastAPI, Request
from fastapi.responses import StreamingResponse
from redis_fastapi import FastAPIRedis, StreamBackendDep, sse_response

from myapp.chats import Chat, require_chat_read_access

app = FastAPI()
FastAPIRedis(app).lifespan().streams()

@app.get(
    "/chats/{chat_id}/events",
    response_class=StreamingResponse,
    responses={200: {"content": {"text/event-stream": {}}}},
)
async def chat_events(
    request: Request,
    streams: StreamBackendDep,
    chat: Chat = Depends(require_chat_read_access),
):
    return await sse_response(request, backend=streams, stream=chat.event_stream_id)
```

`Chat` and its dependency belong to the application. Authenticate, load the
resource within the verified tenant, and authorize access before calling the
helper. A stream name or cursor never grants access. The backend dependency
itself does not read history. FastAPI security schemes, dependency overrides,
and normal denial responses remain application-owned.

**Why StreamingResponse in the decorator?** FastAPI's native
`EventSourceResponse` marker selects generator routing, including in FastAPI
0.138.2, where an ordinary async endpoint returning a response is not awaited
on that path. The helper returns an `EventSourceResponse` instance with its own
UTF-8 framing, so use `StreamingResponse` for the route declaration. The explicit
response content documents SSE in OpenAPI. This corrects the proposal's example.

## Explicit integration for applications and higher-level libraries

Use `StreamSettings` and inject a backend when the application owns configuration
and Redis connections. `StreamSettings` is a validated, environment-free model:
it reads neither `REDIS_*` nor `.env`, and has no connection or telemetry settings.
Direct `StreamBackend` construction defaults to this model, not global settings.
The fluent `.lifespan().streams()` path remains an optional environment adapter.

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis
from redis_fastapi import (
    StreamBackend, StreamSettings, add_redis_streams,
)

# The application supplies this URL; no implicit environment lookup is performed.
redis_url = "redis://localhost:6379/0"
commands = Redis.from_url(redis_url, max_connections=16)
relays = Redis.from_url(redis_url, max_connections=4)
backend = StreamBackend(
    commands,
    blocking_client=relays,
    settings=StreamSettings(prefix="myapp:sse", stream_bucket_count=4),
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        async with backend:
            yield
    finally:
        await relays.aclose()
        await commands.aclose()


app = FastAPI(lifespan=lifespan)
add_redis_streams(app, backend=backend, manage_lifespan=False)
```

The existing authorized route above works unchanged with `StreamBackendDep`.
Registration installs the same typed HTTP error mapping in either setup mode;
`sse_response()` also accepts an explicitly supplied backend directly.

`manage_lifespan=False` means the application must enter and exit the backend.
Without that option, `add_redis_streams(app, backend=backend)` wraps the existing
app lifespan and starts the backend before entering it; it stops the backend
after that lifespan exits. In this mode the supplied Redis clients must already
be usable before the original lifespan starts and must remain open until backend
shutdown finishes. Neither registration mode closes supplied clients. Use the
caller-managed form above when the original lifespan owns those clients.

Do not call `.lifespan()` merely to register an injected backend: that explicitly
opts into the separate environment-managed Redis pools and telemetry lifecycle.
`FastAPIRedis(app).streams(backend=backend)` is the equivalent fluent registration.

Identical registration is a no-op. Changing the backend, lifecycle mode, or
mixing injected backends with environment overrides raises a configuration error.
Configuration and dependency registration are app-scoped; two apps can have
different backends. A backend runtime has one owner and cannot be entered twice
concurrently. Failed startup cleans up partially started relay tasks.

Command/replay and blocking clients must have independent connection resources,
point to the same database, and agree on credentials/topology. Two client objects
sharing one pool are rejected. The backend cannot prove arbitrary custom-client
pool independence or matching endpoints; the caller must ensure them. Reserve at
least one relay connection per configured bucket (per-node limits for Cluster),
keep the command pool independently available, and set blocking socket timeouts
above `stream_block_ms`. Client ownership does not change the topology qualification
limits below. No explicit setup path initializes or shuts down global telemetry.

Higher-level libraries such as Redis Iris can re-export `StreamSettings`,
`StreamBackend`, `StreamBackendDep`, `get_stream_backend`, `add_redis_streams`,
`sse_response`, `StreamEvent`, and the public `StreamError` hierarchy without
using private pool state or modifying the process environment. RedisSettings
remains accepted as an explicit adapter; its resolved streaming values are copied
into StreamSettings, excluding connection and telemetry fields.

## Publish and finish

Create the stream in the application's write workflow, before starting work:

```python
await streams.create(stream_id, ttl=86400)
cursor = await streams.append(
    stream_id,
    event="message",
    data={"message_id": message.id, "text": message.text},
    producer="chat-server",
    idempotency_key=str(message.id),
)
await streams.finish(
    stream_id,
    event="chat.closed",
    data={"chat_id": chat.id},
    producer="chat-server",
    idempotency_key=f"{chat.id}:closed",
)
```

Use a stable producer and a stable event identity across retries. Redis native
IDMP protection is bounded by duration, capacity, and stream lifetime: defaults
are 24 hours and 10,000 IDs per producer. It does not compare retry payloads or
deduplicate application side effects. Retrying an event whose payload was trimmed
can return its original cursor without restoring the payload.

`finish` puts an authoritative final marker in the final event itself. Concurrent
writers serialize with same-slot WATCH/MULTI/EXEC; later appends fail. A matching
finish retry returns the retained final cursor. Reusing an ordinary event's retry
identity for finish raises a conflict. An ordinary `message.completed` event need
not seal a chat containing later messages.

Creation retries preserve the original absolute expiry. Conflicting creation
settings fail. Reads and writes never extend the deadline. Payload expires first;
content-free metadata remains for expiry recognition, then also expires. Use a
new unique stream identity for a new delivery window; after metadata expiry the
SDK cannot detect reuse. The application's database owns permanent chat state.

A worker can use `StreamBackend(client, settings=settings)` and call
`await backend.initialize()` before publishing. It does not need FastAPI or a
relay. For non-HTTP subscriptions, provide a **separate** blocking client:

```python
async with StreamBackend(client, settings=settings, blocking_client=relay_client) as backend:
    async with backend.subscribe(stream_id, after=cursor) as events:
        async for event in events:
            print(event.id, event.event, event.data)
```

Both clients remain caller-owned. Workers and gateways must use the same prefix,
serialization, bucket count, and stream settings. Stop the backend before closing
its clients. Streaming is asyncio-based; no synchronous facade is supplied.

## Resume and recovery

Events carry Redis entry IDs. Resume is strictly after the cursor. Selection is
explicit helper `after`, then `Last-Event-ID`, then query `after`, then `0-0`.
Every supplied cursor is validated, even if overridden. Duplicate headers/query
values, special Redis cursors, empty IDs, malformed IDs, and future IDs fail.

Same-origin cookie users can use `EventSource`; bearer-token applications use a
streaming fetch client or another SSE client supporting Authorization headers.
Credentials never belong in URLs. On reconnect, authorization runs again.
Persist a cursor after applying its event if refresh recovery is needed.

Before headers, errors return `detail.code`:

| Condition | Status/code |
| --- | --- |
| Unknown stream | 404 / `stream_not_found` |
| Known expired stream | 410 / `stream_expired` |
| History removed | 409 / `history_unavailable` |
| Cursor ahead | 409 / `cursor_ahead` |
| Other lifecycle conflict | 409 / `stream_conflict` |
| Invalid input | 422 / `invalid_stream_request` |
| Process admission exhausted | 503 / `stream_capacity_exceeded` |
| Redis unavailable or inconsistent storage | 503 / `stream_unavailable` |
| Final event already consumed | 204, preventing EventSource retries |

Authentication dependencies retain their own status and challenge headers.
After headers, recoverable failures send a cursorless `redis.stream.control`
event when possible, then close. `action` is `reload`, `reconnect`, or
`reauthenticate`. Keepalives and control messages have no ID. A broken or blocked
socket may receive no control message; EOF alone does not prove completion.

For reload, fetch application state together with a matching event cursor.
Starting blindly at the first retained event cannot reconstruct trimmed changes.
A native EventSource client cannot inspect initial error response bodies; use a
normal authenticated state endpoint or a fetch-based client for recovery.

`expires_at` accepts a timezone-aware datetime derived from verified credentials.
Connections end by that deadline or the configured connection lifetime, whichever
comes first. This bounds reauthorization latency; it is not immediate revocation.
Do not mutate sessions after headers or apply `cache()`/`cache_put()` to SSE routes.

## Client compatibility

The streams extra requires redis-py `>=8.0.1,<9`. Tests cover redis-py 8.0.1 and
8.1.0 with the protocol unset, explicit RESP2, and explicit RESP3 with both
legacy and modern responses. Each mode is exercised with binary and decoded
responses on standalone Redis and a three-primary OSS Cluster. XREAD replies
are normalized before live delivery.

These tests do not qualify Cluster failure recovery. The upstream
[watched-transaction retry fix](https://github.com/redis/redis-py/pull/4486)
is not included in the tested released clients. Before qualifying Cluster
recovery, select a released client containing that fix and rerun transaction,
network-loss, and failover tests. Do not treat a successful normal-operation
Cluster test as that qualification.

## Resources and settings

The environment-managed setup uses the normal `REDIS_` prefix. `.streams(stream_...=...)` overrides
stream settings for that application; invalid or conflicting setup fails.

| Setting suffix | Default |
| --- | --- |
| `STREAM_BUCKET_COUNT` | 16, fixed for a storage namespace |
| `STREAM_MAX_EVENT_BYTES` | 65,536 encoded storage bytes |
| `STREAM_MAX_EVENTS` | 10,000 application events |
| `STREAM_IDMP_DURATION` / `STREAM_IDMP_MAXSIZE` | 86,400 seconds / 10,000 IDs per producer |
| `STREAM_EXPIRY_RECOGNITION` | 86,400 seconds |
| `STREAM_MAX_DURATION` / `STREAM_KEEPALIVE` | 300 / 15 seconds |
| `STREAM_SEND_TIMEOUT` / `STREAM_PREPARE_TIMEOUT` | 10 / 10 seconds |
| `STREAM_QUEUE_EVENTS` / `STREAM_QUEUE_BYTES` | 256 / 1,048,576 per subscriber |
| `STREAM_MAX_SUBSCRIBERS` / `STREAM_MAX_ACTIVE_STREAMS` | 1,000 / 256 per process |
| `STREAM_BLOCK_MS` | 1,000 |
| `STREAM_READ_COUNT` / `STREAM_READ_MAXCOUNT` | 32 per stream / 256 per read |
| `STREAM_READ_MAXBYTES` | 1,048,576 |
| `STREAM_REPLAY_CONCURRENCY` / `STREAM_REPLAY_BYTES` | 8 / 1,048,576 per replay page |

One live blocking reader runs per active bucket. For B buckets and P processes,
at most P × B reads block at once, independent of subscriber count. This is not
a total socket bound: ordinary commands, replay, topology discovery, and idle
connections are additional. Cluster pool limits apply per node. Blocking readers
use separate resources so they cannot occupy the command pool.

`stream_read_maxcount` and `stream_read_maxbytes` control aggregate batches.
At startup, command probes negotiate read limits per placement bucket; no INFO
permission or assumption about a proxy's version string is required. A mixed
cluster uses the native path only for buckets that accepted the optional syntax.
Connection failures fail startup rather than being mistaken for missing features.
Capability results last for that backend instance; restart after changing server
capabilities. An unsupported native read after startup fails the subscription
with a resumable error rather than silently changing behavior mid-read.

On Redis 8.10-capable buckets, send `COUNT`, `MAXCOUNT`, and `MAXSIZE`, rotating
first-key order for fairness. Native MAXSIZE is a target: the server can exceed
it when emitting an entry/key. Maximum event sizes, validated key size admission,
and bounded subscriber buffers still bound that overshoot; MAXSIZE alone is not
a hard heap-memory limit.

On other buckets, standard COUNT applies per key. Reserve 64 bytes of reply
framing, each selected key's UTF-8 bytes plus 64 bytes of stream framing, and
E + 512 bytes per entry, where E is the maximum validated encoded event size.
Choose a rotating group and per-key COUNT whose total conservative cost fits the
byte and aggregate count budgets. Startup rejects a byte budget unable to hold
one maximum event and the longest permitted key for the configured prefix. The
budget bounds encoded replies from SDK-owned logs, not total Python heap or
responses from externally corrupted storage.

When a standard-read bucket has more streams than fit one read, rotate disjoint
groups through one blocking reader. With N streams and group size G, an idle
full round takes at most `ceil(N / G) * stream_block_ms`, plus command and
processing time. Long keys can reduce G. This latency/capacity tradeoff does not
add readers: membership is refreshed after every read, and busy groups cannot
monopolize the reader. No polling fallback is introduced.

Replay pages have a conservative count bound derived from the maximum encoded
event size and replay byte budget. Live queues enforce count and byte bounds;
a slow subscriber never blocks the shared reader. Historical replay does not
rewind that reader. Registration captures a replay boundary and buffers overlap.

Keys use an injective UTF-8/base64url identity encoding and SHA-256 bucket mapping:

```text
<prefix>:streams:{streams:v1:bucket:<n>}:<encoded-identity>:events
<prefix>:streams:{streams:v1:bucket:<n>}:<encoded-identity>:meta
```

Prefixes containing braces are rejected. A persistent namespace manifest records
layout and bucket count. Every key in a bucket shares a slot; transactions and
multi-stream reads stay in that slot. Change placement only in a new namespace.
Reads use primaries. Stream names must be unique across application tenants.

Retention removes bounded prefixes using XDEL. Its native
`max-deleted-entry-id` records the removed boundary in the same command; metadata
mirrors that boundary. Readers consult both so failed metadata writes cannot hide
lost history. The bootstrap and final entry remain until expiry. No Lua, consumer
groups, untracked trimming, or Pub/Sub delivery path is involved. External writes
to SDK-owned keys are unsupported.

Creation is initialized as unreadable, then marked ready only after checking the
committed configuration and deadlines. Redis execution errors do not roll back
other commands. Partial initialization fails closed; a complete initialization
whose readiness step was interrupted can be validated by a creation retry.

## Qualification and remaining limitations

Tests use disposable Redis 8.6.7 standalone and three-primary Cluster for the
baseline, plus Redis 8.10.2 for native read-limit negotiation, with isolated
namespaces, bounded observed-state synchronization, and owned-process teardown.
Run them with:

```bash
STREAMS_REDIS_SERVER=/path/to/redis-8.6.7/src/redis-server uv run nox -s streams-3.12
```

Streaming has its own 80% coverage gate. Redis 7.4/8.8 integration jobs measure
legacy modules using `tests/coverage-legacy.ini`; unit and full-suite reports
continue to include every module.

The Redis Software and Redis Cloud documentation lists Redis database version
8.6 as supported. This is the database compatibility version, not the Software
cluster version. See [Software release notes](https://redis.io/docs/latest/operate/rs/release-notes/rs-8-2-releases/)
and [Cloud version management](https://redis.io/docs/latest/operate/rc/databases/version-management/).
A matching command set does not establish deployment qualification.

Enterprise/Cloud qualification requires an explicitly supplied sharded database endpoint through
`STREAMS_ENTERPRISE_URL` and `STREAMS_ALLOW_EXTERNAL=1`. Missing coverage is
reported as skipped. Sentinel streaming is rejected until failover is qualified.
Persistence, failover, resharding, cross-region delivery, sustained capacity, and
the full IDMP eviction boundaries need separate qualification. No exactly-once
processing or unconditional Redis failure survival is claimed.

The initial implementation has no custom key builder, renewal API, automatic
producer retry policy, or application snapshot service. Default limits are
provisional; they are not published throughput or latency claims.

## Reuse provenance

The subscription cleanup, relay scheduling, and transaction patterns were adapted
from [Agent Mesh at d94c269](https://github.com/redislabsdev/agent-mesh/tree/d94c2699852481b683e8302da6319380357b836f/sdk/python)
and the SDK's [resumable-events spike](https://github.com/redis/fastapi-redis-sdk/tree/488128615ef7edc3f5c155459f98a73992fa7e0d/experiments/resumable_events).
Separate catch-up and live delivery, byte-bounded queues, and final-event sealing
follow the spike. Agent execution and Task-specific types are outside this API.
