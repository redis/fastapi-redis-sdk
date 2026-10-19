# WebSockets and resumable agent streams

Date: 2026-10-16
Companion to [`resumable-streams-research.md`](resumable-streams-research.md)
and [`resumable-streams-design.md`](resumable-streams-design.md).

## Question

Where do WebSockets fit the resumable-stream use case? Is the transport common
for agents, what are its drawbacks, and is its absence a gap in the SDK's
offering?

## Short answer

WebSockets are a minority transport for agent output. Real systems use them in
a few niches. For browser-facing text and tool-event streams, SSE is the
common choice. The absence is a real but narrow gap. The SDK has no stream
feature yet, so the question is about the design. The SDK already documents
that WebSockets get no session in this release (`docs/guide/sessions.md`,
"Limitation: WebSockets have no session in this release").

Scope: real systems only. Many sources are practitioner blogs, and the text
marks them. This note covers text chat only. Voice, audio and telephony are
out of scope, so the earlier material on them is removed.

## How agents use a FastAPI endpoint

The dominant shape is one HTTP request, then a typed event stream back over
SSE.

- AG-UI, the agent-to-frontend protocol, defaults to this. A client POSTs a
  run input and receives typed events. It also allows WebSockets and webhooks,
  and says the choice does not change core logic
  ([docs](https://docs.ag-ui.com/concepts/architecture)).
- The Vercel AI SDK, OpenAI background mode, LangGraph and A2A (all in the
  earlier research) use SSE or plain HTTP.
- The traffic is lopsided: one prompt in, hundreds of events out. Practitioner
  write-ups say SSE works with ordinary proxies, load balancers and HTTP auth,
  and needs no sticky sessions
  ([example](https://dev.to/pruthviraj_janwade_180b10/stop-using-websockets-for-ai-chat-apps-why-we-switched-to-server-sent-events-sse-in-production-1k54),
  blog evidence).

Mid-run client messages (interrupt, approve a tool, redirect) are the case
people cite for WebSockets. SSE-based systems handle them with a separate
POST, such as a cancel endpoint or a resumed run. AG-UI describes its flow as
effectively bidirectional for this reason.

## Where WebSockets do fit

| Niche | Evidence |
|---|---|
| Tool-call-heavy agent loops | OpenAI added a [WebSocket mode](https://developers.openai.com/api/docs/guides/websocket-mode) to the Responses API. It keeps a persistent connection and sends only new input plus `previous_response_id`. Reported gains reach 40% on workflows with 20 or more tool calls (Vercel, Cline and Cursor reported 30-40%, via [InfoQ](https://www.infoq.com/news/2026/05/openai-websocket-responses-api/)). Connections cap at 60 minutes. |
| Long-lived, bidirectional sessions | A WebSocket proposal for MCP ([issue #1288](https://github.com/modelcontextprotocol/modelcontextprotocol/issues/1288)) was closed as dormant on 2026-06-26, with no maintainer sponsor. MCP is moving toward stateless operation, and the 2026-07-28 revision removed resumability ([SEP-2575](https://modelcontextprotocol.io/seps/2575-stateless-mcp)). |

The OpenAI WebSocket mode is a link between your server and the model. It cuts
round-trip cost inside your agent loop. It does not replace the browser-facing
stream, so it is a different layer from end-user resumption.

## Is it common?

For browser-facing text and tool-event streaming, no. SSE is the usual choice,
and both the Vercel and Glance designs assume it. WebSockets appear in
persistent server-to-server links and in tool-call-heavy agent loops.

## Drawbacks

- **Resume is not built in.** SSE has `Last-Event-ID` in the protocol. A
  WebSocket has no such feature, so session IDs, sequence numbers and replay
  are yours to build.
- **Real systems build it anyway.**
  - Discord's gateway resumes with a `session_id` and the last sequence
    number, and the server replays missed events
    ([docs](https://docs.discord.com/developers/events/gateway)).
  - Socket.IO ships connection state recovery: a session ID plus a per-packet
    offset, with a two-minute default window. It works with the in-memory and
    Redis Streams adapters. It does not work with the classic Redis Pub/Sub
    adapter, which cannot persist packets
    ([docs](https://socket.io/docs/v4/connection-state-recovery),
    [adapter](https://socket.io/docs/v4/redis-streams-adapter/)).
  - A production PR against the Socket.IO Redis Streams adapter reports a
    reconnect storm. About 250 simultaneous reconnects ran unbounded `XRANGE`
    reads and froze live pushes. The fixes were a bounded stream length,
    bounded restore reads and separate Redis connections
    ([PR](https://github.com/Mereithhh/very-happy/pull/416)).
- **Multi-worker fan-out.** Uvicorn workers do not share memory. Guides
  recommend Redis Pub/Sub between workers, and note that it loses messages if
  a worker lags, with Streams as the durable option (blog evidence,
  [example](https://dev.to/ayush_kumar_085a0f2c54e3f/fastapi-websockets-production-ready-setup-and-scaling-45pp)).
- **Authentication.** Browsers cannot set custom headers on the handshake. The
  workarounds are cookies, first-message auth, or short-lived tokens in the
  query string with an Origin check. This matches the SDK's own note: the
  cookie arrives with the handshake, but nothing can be set afterward.
- **Stop versus disconnect.** A dropped socket and a deliberate stop look
  alike unless the protocol adds a cancel message. This is the same problem
  the earlier research found in SSE systems.
- **Infrastructure.** Idle-connection timeouts, heartbeats and proxy behavior
  need attention. Enterprise networks that cut long-lived streams also affect
  SSE, so this is not unique to WebSockets.

## Fit with the log-based design

The Glance-style pattern (a per-turn Redis Stream, the entry ID as cursor, a
TTL, an explicit error on expiry) does not depend on the transport. Socket.IO's
Redis Streams adapter and Discord's gateway use the same idea: a sequenced log
plus a session identity. A WebSocket would be a second reader of the same log.
It would send the cursor in a message or a query parameter, not in
`Last-Event-ID`.

The gap is at the transport edge. It is not an architecture gap.

## Is it a gap in the offering?

- The research and the SSE-first systems do not cover it. Every reference
  system uses SSE or HTTP. That reflects how agent output is delivered today,
  but it leaves mid-run control unexamined (see "Bidirectional conversations").
- The SDK already lists WebSockets as a known limitation for sessions. A
  stream feature would need the same handshake-authentication guidance.

Recommendation: ship SSE first and keep the cursor and the log
transport-neutral, so a WebSocket reader can be added later. Treat WebSocket
as a later addition, driven by a measured need that SSE plus a control stream
cannot meet. Do not make it a launch requirement. The evidence for browser
text agents, including the measurements below, is against it.

## Gaps in the evidence

- Most SSE-versus-WebSocket claims come from blogs, not documentation.
- I found no production account of resumable agent output built over a
  WebSocket. The closest are Discord and Socket.IO, which are not agent
  systems.
- The MCP WebSocket proposal's closing note cites missing sponsorship. I read
  only the first part of the comment thread, so other objections may exist.

## Why WebSockets are a minority transport: the arguments

**Limit of this section.** I found no usage-share data: no survey, no traffic
count, no download ratio comparing SSE and WebSocket for agent output. "Minority"
therefore rests on what providers, protocols and frameworks document as their
default. Each argument below is a documented fact with its source. Blog
opinions are left out. Where a fact has a limit, the text says so.

### Argument 1: the three major model APIs stream text over SSE

| Provider | Documented text-streaming transport | WebSocket offered for |
|---|---|---|
| Anthropic | "set `"stream": true` to incrementally stream the response using server-sent events (SSE)" ([docs](https://platform.claude.com/docs/en/build-with-claude/streaming)) | Not mentioned on that page |
| OpenAI | "This guide focuses on HTTP streaming (`stream=true`) over server-sent events (SSE)" ([docs](https://developers.openai.com/api/docs/guides/streaming-responses)) | A separate [WebSocket mode](https://developers.openai.com/api/docs/guides/websocket-mode) and the Realtime API |
| Google Gemini | REST streaming at `...?alt=sse` ([docs](https://ai.google.dev/gemini-api/docs/text-generation)) | The Live API: "uses WebSockets for real-time communication" ([docs](https://ai.google.dev/gemini-api/docs/live-api/get-started-websocket)) |

Data: 3 of 3 providers use SSE for ordinary text streaming. Two of the three
also offer a WebSocket product, both for persistent or real-time use, not as the
default path.

Limit: this shows the upstream convention. It does not measure what
applications use between their own server and browser.

### Argument 2: agent protocols pick HTTP transports

- **MCP.** The specification "defines two standard transport mechanisms":
  stdio and Streamable HTTP. A WebSocket transport proposal (SEP-1288) was
  closed as dormant on 2026-06-26. The thread cites no maintainer sponsorship,
  and I saw no documented technical rejection
  ([spec](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports),
  [proposal](https://github.com/modelcontextprotocol/modelcontextprotocol/issues/1288)).
- **A2A.** The specification defines three bindings: JSON-RPC, gRPC and
  HTTP+JSON, and lists Server-Sent Events among the standards it reuses. I did
  not read the whole 256,000-character page, so a WebSocket mention outside the
  part I read is possible
  ([spec](https://a2a-protocol.org/latest/specification/)).
- **AG-UI.** Its client lists "HTTP SSE" first. WebSockets appear only in the
  transport-agnostic section
  ([docs](https://docs.ag-ui.com/concepts/architecture)).
- **Vercel AI SDK.** Its stream protocol "uses Server-Sent Events (SSE)
  format" ([docs](https://ai-sdk.dev/docs/ai-sdk-ui/stream-protocol)).

Data: of four agent-facing protocols, none defines WebSocket as a standard
transport.

### Argument 3: the benefit WebSocket sells grows with round trips

OpenAI states the only figure it publishes for its WebSocket mode: "For
rollouts with 20+ tool calls, we have seen up to roughly 40% faster end-to-end
execution" ([docs](https://developers.openai.com/api/docs/guides/websocket-mode)).
The gain comes from skipping per-turn setup on a persistent connection. A
single-turn chat stream has no such round trips to save. The same page states
a cost: connections last at most 60 minutes, and the cache the speed-up
depends on "disappears for every lane" when the connection closes.

Limit: the 40% is a vendor figure for one provider's server-to-model link.

### Argument 4: SSE has resume in the protocol; WebSocket has none

The HTML specification defines the `Last-Event-ID` header: the client reports
its last event ID on reconnect, and the browser reconnects on its own
([spec](https://html.spec.whatwg.org/multipage/server-sent-events.html),
[MDN](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events)).
The MDN WebSocket page describes no reconnection or replay. Teams that need
them build them, as Discord and Socket.IO did (see Drawbacks).

### Argument 5: browser WebSocket API limits

- MDN: "The `WebSocket` API has no way to apply backpressure", so a fast
  sender can fill the client's memory or CPU
  ([MDN](https://developer.mozilla.org/en-US/docs/Web/API/WebSocket)).
  `WebSocketStream` is the alternative MDN points to.
- In the MCP proposal's thread, a maintainer noted that the browser
  WebSocket API makes setting headers hard. The author agreed that browsers
  do not expose them and proposed cookies
  ([thread](https://github.com/modelcontextprotocol/modelcontextprotocol/issues/1288)).

### What I checked and do not use as an argument

- **"Serverless platforms block WebSockets."** Vercel's own guide says its
  functions "serve WebSocket connections natively". It still advises SSE for
  one-way AI response streaming, and warns that connections close at the
  function's maximum duration
  ([guide](https://vercel.com/kb/guide/do-vercel-serverless-functions-support-websocket-connections)).
- **"SSE is limited to 6 connections."** That limit applies "when not used
  over HTTP/2". Over HTTP/2 the default is 100 streams
  ([MDN](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events)).
- **Latency comparisons and "proxies block SSE" claims.** They come from
  vendor and blog posts, and I found no primary measurement.

## Gaps WebSockets would fill

For each problem I give the evidence, whether WebSocket solves it, and what
SSE offers instead. The SDK has no stream feature yet, so "gap" means a need
the design in `resumable-streams-design.md` would not meet.

| # | Problem | Evidence | Does WebSocket solve it? | SSE alternative | Verdict |
|---|---|---|---|---|---|
| 1 | A resumable WebSocket reader for agent runs | OpenAI's WebSocket mode loses its cache on close. Its recovery steps start a new connection and continue with `previous_response_id`. The page does not say an in-flight response can be resumed ([docs](https://developers.openai.com/api/docs/guides/websocket-mode)) | Not alone. A replay log is needed | Built in via `Last-Event-ID` | **Ecosystem gap.** A sequenced log, as in the design, is what a WebSocket reader would lack. Same fact as Discord and Socket.IO, which built it themselves |
| 2 | Many concurrent runs over one connection | OpenAI WebSocket mode allows 16 in-flight responses and 32 named streams per connection | Yes | HTTP/2 allows about 100 streams by default (MDN) | **Weak.** Only an HTTP/1.1 limit of 6 per browser and domain bites |
| 3 | Server pushes with no request open | SEP-1288's author says Streamable HTTP serves server-initiated messages poorly. MCP's later revision removed the standalone GET stream | Yes, in the author's view | A long-lived GET stream, which MCP is moving away from | **Unproven.** One author's claim, in a closed proposal |
| 4 | Cost of idle long-lived connections on serverless | SEP-1288 cites hibernatable WebSockets on Cloudflare | Yes, on that platform | Reconnect and replay from the log | **Unverified.** Vendor-specific and from the same proposal |
| 5 | Proxies that drop idle SSE | The HTML spec says "legacy proxy servers" may drop connections after a short timeout and advises a comment line about every 15 seconds | Not clearly. SEP-1288 claims better support for WebSocket, with no data | Keep-alive comments. The design already sends a ping every 15 s | **Not a gap.** Mitigated in the design |
| 6 | Fast cancel while a run is silent (long tool call or waiting for a person) | The design (sections 7.1 and 7.3) checks the cancel flag on every append, so cancel lands at the next event while output flows. During silence only the heartbeat poll sees it, up to 15 s by default | Yes, but so would any push channel | A blocking read on a control stream, or a short poll | **Design weakness, not a WebSocket gap.** Fix it in the design |

### Reading the table

- **No row needs WebSocket for text chat.** Rows 2 to 5 are weak, unverified
  or already handled.
- **Row 1 is a gap in this SDK's own offer.** A WebSocket reader is only as
  good as the log behind it. The log in the design supplies the missing
  piece, which is why the design keeps `follow()` transport-neutral.
- **Row 6 is a design question, not a WebSocket one.** While output flows,
  cancel is as fast as the next event. The 15-second worst case applies only
  during silence. Lowering the poll interval or blocking on a control stream
  fixes it, and neither needs WebSocket. See "How the control stream compares
  with the design".

Conclusion: the evidence supports the earlier recommendation. Build SSE first,
keep the log and cursor transport-neutral, and add a WebSocket reader only if
a measured need appears that SSE plus a control stream cannot meet.

## Measured: SSE versus WebSocket in our use case

The question: does WebSocket deliver the same events faster, cheaper or
smaller than SSE for the design in `resumable-streams-design.md`?

### Method

- One FastAPI app, one uvicorn worker, serving the same Redis Stream over
  both transports. Each reader runs the same `XREAD BLOCK` loop and sends one
  frame per event. Only the transport differs, so the Redis work is identical.
- Redis 8.7 on the same machine (Apple M3 Max, 14 cores). Client, producer,
  server and Redis ran as separate processes, talking over **loopback**.
- Load: 1, 100 and 400 streams, one reader each, 20 events/s per stream,
  events of about 125 bytes, 10 s per run, 3 repetitions. Cells show medians.
- Latency is the client's receive time minus a timestamp the producer wrote
  into the event just before `XADD`. Both processes read the same clock.
- Server CPU comes from the server process's CPU time. Memory is the server's
  resident set growth after the connections opened, divided by the stream
  count.
- Wire bytes: SSE counts the lines read plus an estimate of the HTTP chunk
  framing. WebSocket counts the payload plus the 2-byte frame header.
  Neither includes TCP, IP or TLS.
- The harness is in `/tmp/wsbench` (`server.py`, `bench.py`, `run.py`). It is
  not in the repository.

### Steady streaming

| Streams | Transport | p50 latency | p99 latency | Server CPU per event | KB per connection | Bytes per event |
|---|---|---|---|---|---|---|
| 100 | SSE | 9.7 ms | 16.8 ms | 105 us | 64 | 127 |
| 100 | WebSocket | 9.6 ms | 17.6 ms | 108 us | 59 | 123 |
| 100 | WebSocket + deflate | 9.7 ms | 17.1 ms | 112 us | 104 | 123 |
| 400 | SSE | 17.0 ms | 30.8 ms | 67 us | 52 | 127 |
| 400 | WebSocket | 17.8 ms | 32.5 ms | 71 us | 53 | 123 |
| 400 | WebSocket + deflate | 19.2 ms | 41.6 ms | 77 us | 100 | 123 |

The 1-stream rows are in the raw data, but their memory figures reflect
process start-up, so I do not use them.

- **Latency:** the two transports are indistinguishable. The delay grows with
  the stream count because of Redis and Python queueing, and it affects both.
- **CPU:** WebSocket used about 3% to 6% more per event. At 400 streams the
  three runs did not overlap (SSE 65 to 67 us, WebSocket 70 to 72 us). Three
  runs is a small sample, and I did not profile the cause.
- **Memory:** equal at 100 and 400 streams.
- **Compression:** `permessage-deflate` (on by default in uvicorn's
  `websockets` implementation) doubled memory per connection and raised p99
  at 400 streams. Events this small do not benefit.

### Resume and cancel

- **Replay** of 1,000 entries from a cursor, 30 repetitions: first event at
  2.2 ms (SSE) and 2.4 ms (WebSocket); full replay at 22.8 ms and 25.2 ms at
  p50. SSE had one slow outlier I did not explain, so I do not read the p99
  difference as real.
- **Cancel round trip** (client request to server acknowledgement), 300
  repetitions:

  | Request | p50 |
  |---|---|
  | POST on a kept-alive connection | 0.53 ms |
  | POST on a new connection | 0.49 ms |
  | WebSocket message | 0.25 ms |

  On a real network, a kept-alive POST costs one round trip, the same as a
  WebSocket message. WebSocket saves nothing there. A cold connection adds
  the TCP handshake, and TLS adds more, which WebSocket avoids only if its
  connection is already open.
- **Control-stream wake-up** (the fix for cancel latency in row 6 above): a
  reader blocked on `XREAD BLOCK` woke 500 times after an `XADD` from another
  connection. p50 was 2.9 ms, p99 6.7 ms and max 8.4 ms, measured from the
  start of the `XADD` call. This is far below the 15-second worst case
  of the heartbeat poll, which applies only while the producer is silent.

### What is on the wire

I captured the application-level bytes of one event over a raw socket.

SSE over HTTP/1.1 (chunked transfer encoding). One event is one chunk:

```
66\r\n                                  chunk header: size 0x66 = 102 bytes
id: 1792147789452-0\n                   field labels cost bytes too
event: delta\n
data: {"ts":1,"i":0,"x":"aaaa...a"}\n
\n                                      blank line ends the event
\r\n                                    chunk trailer
```

WebSocket (RFC 6455) text frame, server to client:

```
81 66                                   FIN + text opcode; length 0x66 = 102
{"id":"1792147789452-0","t":"delta","d":{"ts":1,"i":0,"x":"aaaa...a"}}
```

| | SSE | WebSocket |
|---|---|---|
| Payload | 102 bytes | 102 bytes |
| Framing per event | 6 bytes (`66\r\n` and `\r\n`) | 2 bytes |
| Total | 108 bytes | 104 bytes |
| Opening exchange | HTTP 200 with headers, once | HTTP 101 upgrade, once |

Why the saving is small:

1. **The payloads are the same size.** SSE spends bytes on the labels `id:`,
   `event:` and `data:` and on newlines. The WebSocket message in my harness
   spends the same bytes on the JSON keys `"id"`, `"t"` and `"d"`. A different
   envelope, such as a bare `["id","t",d]` array, would shrink the WebSocket
   payload. The same shortening is possible in SSE by dropping the `event:`
   line and putting the type inside `data:`. The comparison is about
   framing, not about which format is tidier.
2. **Framing is 4 bytes out of about 105.** Below the application, each event
   also pays for IP and TCP headers (about 40 to 52 bytes) and, over TLS 1.3,
   about 22 bytes of record overhead. These are protocol constants, not
   measurements from this run. Counting them, the 4-byte saving is about 2%
   of the bytes on the wire.
3. **Bytes are not the limit.** 108 bytes take under 1 us to send at 1 Gbit/s
   and under 9 us at 100 Mbit/s. The measured latency is 3 to 19 ms and the
   server spends 67 to 110 us of CPU per event. Time goes to Redis, Python and
   queueing, which the transport does not touch.
4. **"Less chatty" is a statement about requests, not streams.** An SSE
   response is one long HTTP response, so it is not chatty. The saving
   appears for client-to-server messages. A WebSocket frame from the client
   adds 6 bytes (2 header plus a 4-byte mask). A POST repeats the request line
   and headers each time, which with cookies or an `Authorization` header can
   run to hundreds of bytes. That matters for answers and cancels, which are
   rare, and not for the event stream.

### Limits of the measurements

- One machine, loopback, one Python worker, no TLS, no proxy, no real
  network. Latency on a real LAN or the internet is dominated by round trips,
  where the two transports tie.
- At most 400 streams and 3 repetitions. The CPU difference is consistent but
  small, and I did not profile it.
- Each event is sent as its own frame on both transports. Batching events per
  frame would help both.
- It does not cover many readers per stream, idle cost on serverless
  platforms, or reconnect storms.
- A test across a real LAN is pending. The candidate host, `bolt.lan`, is
  reachable over SSH, but my account there cannot use the Docker socket.

## Bidirectional conversations

The scenario: while answering one prompt, the agent needs more information
and asks the user a follow-up question. The answer belongs to the same turn.

### What real systems do

| System | How the question travels | How the answer comes back |
|---|---|---|
| LangGraph | A node calls `interrupt(value)`. The run pauses, state is saved by the checkpointer, and it "waits indefinitely". The client sees the interrupt payload ([docs](https://docs.langchain.com/oss/python/langgraph/interrupts)) | The client re-invokes the graph with `Command(resume=...)` on the same thread. The node restarts from its beginning ([docs](https://docs.langchain.com/oss/python/langgraph/interrupts)) |
| MCP elicitation | The server sends an `elicitation/create` request inside the current request ([spec](https://modelcontextprotocol.io/specification/2025-11-25/client/elicitation)) | The client answers with `accept`, `decline` or `cancel`. On Streamable HTTP, every client message is a new HTTP POST ([spec](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)). The server must tie the state to the user, not to the session ID alone |
| A2A | The task enters `input-required`, an interrupted and non-terminal state ([docs](https://a2a-protocol.org/latest/topics/life-of-a-task/)). My earlier search found that the server closes the stream in that state | The client sends a later message with the same `contextId`, and optionally the `taskId` |
| Vercel AI SDK | The stream ends with a `tool-approval-request` part ([docs](https://ai-sdk.dev/docs/ai-sdk-core/tools-and-tool-calling)) | The app adds a `tool-approval-response` to the messages and calls again. This takes two model calls |

Every one of these uses a second HTTP request for the answer. None needs a
WebSocket. They differ in whether the run stays alive while it waits
(MCP) or is rebuilt from saved state (LangGraph, A2A, AI SDK).

### Proposal for this SDK

Keep SSE for output. Add a small **control stream** per run as the inbound
channel. Redis carries both directions, so it works across processes and does
not depend on which worker holds the connection.

```
agent (producer)                 Redis                                      browser
  |-- ask("Which region?") ---> XADD stream  {t:input_required, id:r1} --> SSE event
  |   waits on XREAD BLOCK         |                                          |
  |   stream-ctl:{id}              |      POST /streams/{id}/input {r1, "EU"} |
  |                                |<---- XADD stream-ctl  (IDMP pid=r1) -----|
  |<-- wakes (about 3 ms) ---------|                                          |
  |-- XADD stream {t:input_received, id:r1} --> other tabs clear the prompt
  |-- continues the same turn on the same stream, same cursor
```

Rules:

1. **One turn, one stream.** The question and the answer are entries in the
   same log. A client that reconnects sees an unanswered `input_required` in
   the replay and can show the prompt again.
2. **Producer API.** `answer = await writer.ask(payload, timeout=...)`. It
   appends the `input_required` entry with a `request_id`, extends the stream
   TTL to cover the timeout, and blocks on the control stream.
3. **Answer endpoint.** `streams.answer(stream_id, owner, request_id, value)`
   checks ownership, checks that `request_id` is pending, then appends to the
   control stream with `XADD ... IDMP <request_id> ...`. The first answer wins.
   A second tab or a retry gets the original entry back and changes nothing.
4. **Cancel uses the same channel.** A cancel is another control entry. This
   gives a cancel that lands in milliseconds even while the producer is silent
   (see the comparison below).
5. **Waiting for long.** Two modes, both available because the log and the
   control stream do not belong to any process:
   - *In-process wait.* The producer task stays alive. If its process dies,
     readers see `producer_lost`.
   - *Suspended wait.* The producer saves its own state (for example a
     LangGraph checkpoint), writes `writer.suspend(ttl)` and exits. A later
     process calls `attach()` and continues on the same stream. Saving agent
     state stays the application's job.
6. **No answer.** When the timeout passes, `ask()` raises `InputTimeout`. The
   default is to end the run with `fail("input_timeout")`. The application can
   catch it and choose another path.
7. **A new message while a question is open.** The scope is still busy, so
   `open()` raises `StreamBusy`. The application decides whether to treat the
   text as the answer or reject it. The SDK does not guess.
8. **Security.** The answer endpoint uses the same owner-scoped IDs as reads.
   Limit the answer size, and make `request_id` unguessable.

### How the control stream compares with the design

`resumable-streams-design.md` already has an inbound path: a `cancel` field in
the meta hash. `cancel()` sets it. `append()` reads it in the same transaction
(sections 7.1 and 7.3), and `keepalive()` reads it every heartbeat.

| | Design today: `cancel` flag | Proposal: control stream |
|---|---|---|
| What it carries | One bit | Any message: an answer, a cancel, later commands |
| How the producer learns | Polls. Free on each append, because the read rides in the same round trip. Otherwise every `stream_heartbeat_seconds` | Blocks on `XREAD BLOCK`, or polls the stream on each append |
| Latency | The next append while output flows. Up to 15 s during silence | About 3 ms to wake a blocked reader (measured, loopback). The next append, if the producer only polls |
| Redis cost | None extra | One blocked connection per run that is waiting. Nothing while it only polls |
| Several messages, order | No | Yes, in order |
| Duplicates | Setting a flag twice is harmless | `IDMP` on `request_id`: the first answer wins, retries change nothing |
| Keys | A field in a hash that already exists | One new key in the same Cluster slot, with the stream's TTL and a small `MAXLEN` |
| Can wait for a person | No. A flag has no payload and nothing to block on | Yes |

So the flag cannot carry an answer. That gap is the reason to add a stream,
not cancel speed.

Three points follow:

1. **My earlier "15-second cancel" was the worst case, not the usual one.**
   While the agent streams tokens, the flag is seen at the next append, within
   milliseconds. The delay shows up only in silence: a long tool call, or a
   run waiting for a person. I have corrected the earlier wording above.
2. **Blocking doubles as the heartbeat.** Inside `ask()`, the producer can loop
   on `XREAD BLOCK <heartbeat>`. Each timeout refreshes the `alive` key, so no
   separate heartbeat task is needed while waiting.
3. **The cost to watch is held connections.** A person may take minutes to
   answer, and each waiting run holds one Redis connection. Polling the control
   stream once a second avoids that, at the price of up to a second of delay,
   which a person will not notice. The choice can be a setting.

Options:

| Option | Change to the design | Result |
|---|---|---|
| A. Keep the flag only | None | Cancel works. `ask()` cannot be built |
| B. Flag for cancel, plus a control stream for answers | `cancel()` also appends a `cancel` entry, so a waiting `ask()` wakes at once. The flag stays for producers that are streaming | Cancel is unchanged and fast while output flows. Answers work. Cancel is fast during a wait |
| C. Control stream replaces the flag | Remove the flag and the `HGET` in `append()`. Every producer polls or blocks on the stream | One mechanism. More Redis work per run, and a bigger change to section 7 |

I recommend **B**. It adds the one missing capability and leaves the cheap,
already-designed cancel path alone. A cancel in the middle of a long tool call
stays slow in either A or B, unless the heartbeat interval is shortened (a
one-second poll costs one command per second per run). Option C removes
that special case but costs more than the problem justifies.

What the control stream does not do: it cannot start a worker. In the
suspended mode, an answer lands in Redis, but something must launch the
process that calls `attach()`. That trigger belongs to the application (a
queue or a task runner), as section 5.3 of the design says about dispatch.

### Where WebSocket would still help

A WebSocket would carry the answer on the connection that is already open. The
measured saving is under a millisecond on loopback, and nothing on a real
network compared with a kept-alive POST. It would also bring back the
stop-versus-disconnect ambiguity and the handshake-authentication workarounds.
It could pay off for very chatty interaction, such as sending partial input
while the user types. That is not a chat-turn follow-up question, so I do not
recommend it for this feature.

### Limits of this section

- The control-stream design is a proposal. Only the wake-up latency is
  measured. The ask and answer flow is untested.
- I read the MCP and LangGraph pages in full or in excerpt, but the A2A
  stream-closing behavior comes from an earlier search summary. Check the A2A
  specification before relying on it.
- LangGraph's page does not say whether the process stays alive while paused.

## Sources

- [AG-UI architecture](https://docs.ag-ui.com/concepts/architecture)
- [OpenAI WebSocket mode](https://developers.openai.com/api/docs/guides/websocket-mode)
- [InfoQ on WebSocket mode](https://www.infoq.com/news/2026/05/openai-websocket-responses-api/)
- [Discord gateway](https://docs.discord.com/developers/events/gateway)
- [Socket.IO connection state recovery](https://socket.io/docs/v4/connection-state-recovery)
- [Socket.IO Redis Streams adapter](https://socket.io/docs/v4/redis-streams-adapter/)
- [Reconnect-storm PR](https://github.com/Mereithhh/very-happy/pull/416)
- [MCP SEP-2575](https://modelcontextprotocol.io/seps/2575-stateless-mcp)
- [MCP WebSocket proposal #1288](https://github.com/modelcontextprotocol/modelcontextprotocol/issues/1288)
- [FastAPI WebSocket scaling guide](https://dev.to/ayush_kumar_085a0f2c54e3f/fastapi-websockets-production-ready-setup-and-scaling-45pp)
- [SSE switch account](https://dev.to/pruthviraj_janwade_180b10/stop-using-websockets-for-ai-chat-apps-why-we-switched-to-server-sent-events-sse-in-production-1k54)
- [Anthropic streaming](https://platform.claude.com/docs/en/build-with-claude/streaming)
- [OpenAI streaming responses](https://developers.openai.com/api/docs/guides/streaming-responses)
- [Gemini text generation](https://ai.google.dev/gemini-api/docs/text-generation)
- [Gemini Live API](https://ai.google.dev/gemini-api/docs/live-api/get-started-websocket)
- [MCP transports (2025-11-25)](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)
- [A2A specification](https://a2a-protocol.org/latest/specification/)
- [AI SDK stream protocol](https://ai-sdk.dev/docs/ai-sdk-ui/stream-protocol)
- [HTML SSE specification](https://html.spec.whatwg.org/multipage/server-sent-events.html)
- [MDN: server-sent events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events)
- [MDN: WebSocket](https://developer.mozilla.org/en-US/docs/Web/API/WebSocket)
- [Vercel WebSocket guide](https://vercel.com/kb/guide/do-vercel-serverless-functions-support-websocket-connections)
- [LangGraph interrupts](https://docs.langchain.com/oss/python/langgraph/interrupts)
- [MCP elicitation](https://modelcontextprotocol.io/specification/2025-11-25/client/elicitation)
- [A2A life of a task](https://a2a-protocol.org/latest/topics/life-of-a-task/)
- [AI SDK tool calling](https://ai-sdk.dev/docs/ai-sdk-core/tools-and-tool-calling)
