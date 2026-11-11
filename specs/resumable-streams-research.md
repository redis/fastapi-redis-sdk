# Resumable streams for agents: research findings

Date: 2026-10-15

## Scope and method

This note surveys how production systems let a client resume an agent's
streamed output after a dropped connection. It covers use cases, API surface
and architecture. It does not cover implementation code.

- The two reference sources were the Glance engineering article and Vercel's
  `resumable-stream` library.
- A direct fetch of the Glance article returned 403. The details below come
  from a mirror copy.
- I excluded sample apps and blog demos that had no production evidence.
- Some findings come from issue threads and vendor posts. These are weaker
  evidence than documentation, and the text marks them.

## The core idea

Every source describes the same pattern, whatever the stack:

1. **Generation is decoupled from the client connection.** The agent keeps
   running when the client drops.
2. **Events go to a shared, ordered log** with a per-event position.
3. **The client remembers its last position.** On reconnect it asks for
   everything after that, then continues live.
4. **The log is transport, not storage.** The database holds the final record.
   An expired log returns an explicit error.

The two references take different routes to this pattern.

## Two architectural families

| | Vercel `resumable-stream` | Glance (Redis Streams) |
|---|---|---|
| Mechanism | Pub/Sub, plus `INCR` to elect a producer | One Redis Stream per turn |
| Producer | The request that started the stream. It stays alive and relays to late joiners. | A separate agent service that writes events to the stream |
| Cursor | Numeric offset, `resumeAt` | Redis entry ID, carried as SSE `id` and `Last-Event-ID` |
| Strength | Almost no overhead when nobody resumes (one `INCR`, one `SUBSCRIBE`). Fits serverless. | Survives producer and gateway restarts. Any gateway pod can resume. |
| Limits | Strings only. Producer must stay alive. The README documents no TTL or persistence. Returns nothing once the stream is done. | Extra hop, with p99 about 10-20 ms. A dead agent is detected only by timeout. |

### Vercel `resumable-stream`

Source: <https://github.com/vercel/resumable-stream>

Purpose: wrap a string stream (such as an SSE response) so a client can pick
up after a drop, or a second client can follow along. It targets serverless
environments without sticky load balancing.

API surface:

- `createResumableStreamContext({ waitUntil, publisher?, subscriber? })`
- `resumableStream(streamId, makeStream, resumeAt?)`. This is idempotent: the
  same call starts a new stream or resumes an existing one. It returns a falsy
  value when the stream has finished, and the examples answer with HTTP 422.
- `createNewResumableStream(streamId, makeStream)` for a POST handler.
- `resumeExistingStream(streamId, resumeAt?)` for a GET handler.
- A generic entry point accepts Upstash, Valkey or other clients, given a
  publisher and a subscriber.

Architecture:

- The first call for a `streamId` becomes the producer. It runs to completion
  even if the original reader disconnects.
- A later call publishes a request on a channel and waits. The producer
  replies with its buffered messages, then the new chunks.
- The README does not document TTL, persistence or multi-consumer
  coordination.

### Glance

Source:
<https://engg.glance.com/building-resumable-real-time-llm-streaming-with-redis-streams-09cfa9e79358>

Use case: an agent whose turns take tens of seconds and emit tool selections,
tool results, progress updates and finished UI blocks, to clients on consumer
networks.

Requirements: server-side streaming, resumption per turn, no coupling between
gateway and agent topology, and survival through deploys.

Alternatives and why they failed:

- **Direct client-to-agent SSE.** Auth, rate limiting, history, analytics and
  client-format translation leaked into the inference service. The team split
  out a gateway.
- **gRPC streaming between services.** The turn lived inside the connection.
  "A gRPC stream is a pipe, and we needed a buffer." Replay would need a second
  store, and the streams were tied to one agent pod.
- **Kafka.** Offsets belong to partitions, but the unit of resumption is the
  turn. One partition per turn is impractical.

Design:

- The gateway creates the turn ID and the agent returns a 202. The reader
  knows where to listen before anything is written, which avoids a startup
  race.
- The agent appends each event to a Redis Stream named for the turn. The
  gateway blocks on the same key and relays events over SSE.
- The Redis entry ID is the single cursor, sent as the SSE `id` and returned
  in `Last-Event-ID`.
- The TTL is set atomically with the first event, so a killed pod cannot leave
  a key that never expires. After completion, the TTL shrinks to the reconnect
  window.
- Streams are transport. History lives in the database. An expired stream
  returns 410.
- Only completed UI blocks are sent, not token deltas.
- A turn can suspend for an async tool and be finished by a different pod.
- Recovery relies on what the client received, not the gateway's cursor.
  Failures can cause duplicates, never silent gaps.

Reported limits: timeout detection is imprecise (a 60 s timeout may fire
between 60 and 90 s), a dead agent loop is discovered only by timeout, and the
writer is not told when a client leaves. The article gives no throughput or
reliability figures. It gives a p99 of about 10-20 ms for the added hop, with
spikes to 40-60 ms.

## Real-world systems

### Vercel AI SDK

Docs: <https://ai-sdk.dev/docs/ai-sdk-ui/chatbot-resume-streams>

- Client: `useChat({ resume: true })`. On mount it sends a GET to a per-chat
  stream route. The server answers 204 when no stream is active.
- Server: the POST handler registers the stream through `consumeSseStream`.
  The GET route calls `resumeExistingStream`.
- The developer supplies Redis and a persistence layer that tracks the active
  stream ID per chat.
- Known caveat: aborting on tab close breaks resumption. The newer guidance is
  to use `resume: true` for reconnects and a separate stop endpoint for
  deliberate cancellation
  (<https://ai-sdk.dev/docs/troubleshooting/abort-breaks-resumable-streams>).

### OpenAI Responses API (background mode)

Docs: <https://developers.openai.com/api/docs/guides/background>

- Create with `background=true` and `stream=true`. Store each event's
  `sequence_number` as the cursor.
- Resume with `GET /v1/responses/{id}?stream=true&starting_after=N`. Polling
  and `POST /v1/responses/{id}/cancel` also exist.
- Azure OpenAI and OCI document the same pattern.
- A community thread reports a limit of about 5 minutes on resuming. I saw only
  its title, so verify before relying on it.
- A LiteLLM issue reports that a proxy mishandled `starting_after`.

### LangGraph / LangSmith Agent Server

API: <https://docs.langchain.com/langsmith/agent-server-api/thread-runs/join-run-stream>

- `join_stream` exists at run level and thread level.
- Resumption uses `Last-Event-ID`, and works when the run was created with
  `stream_resumable=true`.
- `cancel_on_disconnect` defaults to false.
- The Python SDK tracks the last event ID and resends it on automatic
  reconnect.
- Open issues show a client pitfall. A hook that does not expose the event ID
  replays from the start and duplicates the in-progress message
  (<https://github.com/langchain-ai/langgraphjs/issues/2996>). Another issue
  notes there is no defined handoff between a state snapshot and a stream
  cursor (<https://github.com/langchain-ai/langgraphjs/issues/2880>).
- Reimplementations keep events for about 10 minutes after the run ends.

### LibreChat

Docs: <https://www.librechat.ai/docs/features/resumable_streams>

- Open source, used in production.
- Solves reloads, restarts, multiple tabs, device switching, background
  generation and shared chats.
- Single-instance mode keeps state in memory. Redis mode shares state across
  instances, so a generation can resume on another instance and survive
  rolling deploys.
- Redis roles: Streams hold chunks, Hashes hold job metadata, Pub/Sub carries
  live events, and a TTL expires finished jobs. Redis Cluster is supported
  through hash-tagged keys.
- An optional coalescing window (for example 25 ms) batches deltas to cut
  Redis load, at the cost of that much latency.
- On reconnect the server sends the missed content as one sync event.

### Stardrift

Post: <https://stardrift.ai/blog/streaming-resumptions>

- A travel agent whose tasks last minutes and involve dozens of tool calls.
- Path: SSE, then a third-party service, then workers writing to Redis
  Streams, then in-house resumption.
- Design: a key mapping chat ID to the active message ID, plus one Redis
  stream per chat and message.
- Pitfalls:
  - The chat hook did not expose message IDs until a stream finished.
  - Reconnection fires on every remount, even for finished streams.
  - Deleting the stream at completion races with the subscriber's final read.
- Lesson: skip resumability at first; add it when the product needs it.

### MCP Streamable HTTP

Spec: <https://modelcontextprotocol.io/specification/2025-11-25/basic/transports>

- Revisions 2025-03-26 through 2025-11-25 allow SSE event IDs and replay on
  `Last-Event-ID`. The server must not replay messages from a different
  stream, and event IDs should identify the originating stream.
- The spec defines no event store. Implementers supply one.
- Revision 2026-07-28 removed resumability. A broken response stream loses the
  in-flight request, and the client reissues it.
- Resumability at the protocol level is therefore not settled.

### A2A

Docs: <https://a2a-protocol.org/latest/topics/streaming-and-async/>

- `tasks/resubscribe` (v0.3 and earlier) became `SubscribeToTask` in v1.0.
- The server sends the current task snapshot first, then updates, and closes
  the stream at a terminal or interrupted state.
- Backfill of missed events was implementation-dependent in older versions.
  Clients are told to persist the task ID, resubscribe with backoff and
  de-duplicate.

### Electric Durable Streams

Announcement: <https://electric.ax/blog/2025/12/09/announcing-durable-streams>

- A protocol, not an LLM product: each stream is a URL with opaque, monotonic
  offsets. `PUT` creates, `POST` appends, `GET` reads. Catch-up and live reads
  (long-poll or SSE) are unified.
- The client tracks its own progress, so the server holds no per-client
  session state.
- Electric is the only named production user. It describes the design as
  extracted from its Postgres sync engine.

### Ably (vendor view)

Post: <https://ably.com/blog/ai-chat-stream-resumption>

- Argues that Redis buffers suffice for short responses, a single device and a
  stable network.
- Names four scale problems: write amplification, ambiguity between stop and
  disconnect, no second-device support, and enterprise networks that cut
  long-lived SSE connections.
- Proposes "durable sessions": conversation state that exists independently of
  any participant.
- Vendor content, with no named customers.

## Recurring design decisions

1. **Cursor.** Use the log's native position (Redis entry ID) as SSE `id`, and
   read `Last-Event-ID` or an `?after=` parameter. Glance, LangGraph and
   nebula-chat converge here. OpenAI uses a sequence number, and Vercel a
   numeric `resumeAt`.
2. **Expired or unknown cursor.** Return an explicit error (Glance uses 410).
   Never restart silently, because that is how duplicates reach the user.
   Agentback treats a resume past the buffer as terminal.
3. **Stop versus disconnect.** Keep a dedicated cancel endpoint. A disconnect
   must not cancel the run (AI SDK, LangGraph default).
4. **Active-stream registry.** A chat-to-active-stream mapping lets a GET
   answer "is anything running?" (204 in the AI SDK).
5. **Terminal events.** The stream needs an explicit end marker. Cleanup must
   not race the final read (Stardrift).
6. **TTL.** Set it atomically at creation, shorten it after completion, and
   keep the database as the record.
7. **Authorization.** A stream ID alone must not grant access. The resume
   route needs an ownership check (flagged by Ably; enforced by agentback).
8. **Granularity.** Whole events (Glance) or token deltas (LibreChat, which
   coalesces them to reduce load).
9. **Multiple consumers.** Tabs and devices come for free once the log is
   separate from the connection.
10. **Serverless.** Only Vercel's design targets it, and it needs
    `waitUntil` or `after`. A separate worker needs no such primitive.
11. **Run lifetime.** Replay returns only what was already produced. If the
    agent is bound to the request, a disconnect kills the run. In Starlette the
    request is cancelled on disconnect, so durable resumption needs the run
    submitted as a task or worker (agentomatic PR #5).

## Implications for fastapi-redis-sdk

The repo has Redis-backed backends for cache, sessions and rate limiting
(`cache_backend.py`, `session_backend.py`, `ratelimit_backend.py`) with guides
under `../docs/guide`. A stream primitive would follow that shape.

The evidence favors the Glance model over Vercel's for a FastAPI SDK. Python
deployments commonly run several workers behind a load balancer, and the
request-bound producer in Vercel's design conflicts with Starlette's
cancel-on-disconnect behavior.

Candidate scope:

- A Redis Streams writer and reader for one stream per turn.
- An SSE response helper that honors `Last-Event-ID`.
- An active-stream registry per chat or session.
- Atomic TTL at creation, a shorter TTL after completion, and a 410-style
  error for expired streams.
- A cancel hook, separate from disconnect.
- An ownership hook for the resume route.

Out of scope: the agent runtime. Callers still run generation in a task or
worker.

## Gaps in the evidence

- Vercel's README says nothing on TTL, persistence or multi-consumer
  coordination.
- No source except Glance publishes latency figures, and none publish
  reliability figures.
- The OpenAI resume time limit is unverified.
- Several points come from issue threads and vendor posts.

## Sources

- Vercel resumable-stream: <https://github.com/vercel/resumable-stream>
- Glance: <https://engg.glance.com/building-resumable-real-time-llm-streaming-with-redis-streams-09cfa9e79358>
- AI SDK resume streams: <https://ai-sdk.dev/docs/ai-sdk-ui/chatbot-resume-streams>
- AI SDK abort caveat: <https://ai-sdk.dev/docs/troubleshooting/abort-breaks-resumable-streams>
- OpenAI background mode: <https://developers.openai.com/api/docs/guides/background>
- LangSmith join run stream: <https://docs.langchain.com/langsmith/agent-server-api/thread-runs/join-run-stream>
- LangGraph JS issues: <https://github.com/langchain-ai/langgraphjs/issues/2996>, <https://github.com/langchain-ai/langgraphjs/issues/2880>
- LibreChat: <https://www.librechat.ai/docs/features/resumable_streams>
- Stardrift: <https://stardrift.ai/blog/streaming-resumptions>
- MCP transports: <https://modelcontextprotocol.io/specification/2025-11-25/basic/transports>
- A2A streaming: <https://a2a-protocol.org/latest/topics/streaming-and-async/>
- Electric Durable Streams: <https://electric.ax/blog/2025/12/09/announcing-durable-streams>
- Ably: <https://ably.com/blog/ai-chat-stream-resumption>
- agentomatic PR #5: <https://github.com/UnicoLab/agentomatic/pull/5>
- agentback PR #56: <https://github.com/ninemindai/agentback/pull/56>
- nebula-chat #355: <https://github.com/itsDaiton/nebula-chat/issues/355>
