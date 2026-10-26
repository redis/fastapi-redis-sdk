# Demo app: an incident triage agent

Date: 2026-10-19
Revised: 2026-10-20. The topic changed from trip planning to incident triage
(decision 5), and the feature set shrank to a minimal one (decision 6).
Revised: 2026-10-24. The demo uses the SDK's resumable streams; the
in-memory stub is gone (decision 12).
Revised: 2026-10-23. Stream access follows the streams design's decision 2:
the app checks access, each stream's name includes its scope, and an open
stream ends when the session does (decision 8). Runs end with the design's
single end marker: `spawn()` writes a final `stream.end` entry whose data gives
the reason (decision 9).
Status: proposal, for review
Inputs: [`resumable-streams-design.md`](resumable-streams-design.md), the SDK's
caching, rate-limit and session modules, the FastAPI `frontend()` docs, and
the FastAPI Cloud and Redis Cloud docs listed in section 10.

## 1. Summary

Build one FastAPI app around one story: a chat agent that helps an on-call
engineer find out why a service is slow. The engineer asks a question, the
agent calls two tools, and it streams the answer back over Server-Sent Events
(SSE).

The story needs every SDK feature for a real reason:

| Feature | Why the triage agent needs it |
|---|---|
| Sessions | The engineer signs in, and the agent's runs belong to them. The same person follows the run on a phone on the way to the incident room, and signs the phone out when they leave it behind. |
| Caching | The agent's tools query slow observability APIs. Everyone paged for the same alert asks about the same service. |
| Rate limiting | Each run costs money, so each engineer gets a quota, and the model has a daily budget. |
| Resumable streams | Answers take time. VPNs drop, laptops close and redeploys happen, often during an incident. |

We build only the parts that touch Redis. The agent is a small tool loop on
Claude Haiku 5.5 (section 5.6). A scripted planner with the same interface
takes over when the model is not available. The app lives in
`examples/incident_triage_app/` and deploys to FastAPI Cloud. FastAPI Cloud
injects the Redis Cloud credentials as `REDIS_URL` and the Anthropic key as
`ANTHROPIC_API_KEY`. The database runs Redis 8.6.2 or later.

## 2. The story

Alex is on call for the checkout service. At 14:05 the pager fires:
"checkout p99 latency above 2 s".

1. Alex signs in on a laptop and asks: "Why is checkout slow?"
2. The agent checks the latency metrics and the recent deploys, then streams a
   likely cause and the next steps.
3. Alex's VPN drops. When it returns, the answer continues from the exact
   token where it stopped.
4. Alex walks to the incident room, opens the app on a phone and signs in.
   The phone follows the same answer live.
5. Sam, also paged, asks about checkout. Sam's tool calls return at once from
   the cache.
6. Alex asks six times in a minute and hits the quota. The app says when the
   next run is allowed.
7. Alex leaves the phone in the incident room, so Alex signs out everywhere.
   The phone loses access to the run.

Each step maps to one demo control (section 6). The visitor can play the
story in order or press any control at any time.

## 3. Goals and non-goals

Goals:

1. Each step of the story shows one strength of one feature, visibly.
2. Every run behaves the same towards Redis: the same event types, the same
   tool calls and keys, and the same failure paths. The model's wording may
   differ.
3. A redeploy is one command, and an empty database is a valid start.
4. The app degrades instead of crashing when Redis is missing or too old.
5. It fits the free Redis Cloud database.

Non-goals:

- A real incident tool: no real metrics or deploy sources, no paging, no
  stored chat history. The tool data is seeded, so no outside text reaches
  the model, and no tool result can carry a prompt injection.
- Agent quality. A small model with a short prompt is enough; the demo is
  about what Redis does around it (section 5.6).
- Durable data. Everything expires.
- More than one example of a feature. Each SDK feature appears where the
  story needs it, once (decision 6).

## 4. Architecture

```
 browser ──POST /api/session──────────────▶ sessions: sign in, rotate ID
         ──POST /api/triage/runs──────────▶ rate_limit (per engineer) ─▶ streams.open() ─▶ spawn(planner)
         ◀─202 {stream_id}─────────────────
         ──GET /api/streams/{id}/events───▶ streams.sse() ◀── XREAD BLOCK ◀── Redis Stream
                                                                               ▲
 planner task ──GET /api/tools/*──▶ cache() ─▶ slow fake API                    │
             ──append(tool, delta)──────────────────────────────────────────────┘
```

One app, one process type, one connection pool. The planner calls the app's
own tool endpoints through `httpx.AsyncClient(transport=ASGITransport(app))`.
The calls stay in the process but pass through the app's middleware and
dependencies, so caching and the global rate limit work exactly as they would
for an outside caller.

## 5. The Redis parts

### 5.1 Sessions

| Endpoint | What it does |
|---|---|
| `POST /api/session` | Signs in with a handle and no password. Sets `user_id`, which rotates the session ID. |
| `DELETE /api/sessions` | Signs out everywhere (`revoke_all`). |

The session subject is the identity for two other features:

- **Run scope.** One active run per subject, in scope `triage:<subject>`. That
  is how the phone finds the run in step 4. The scope is also part of every
  stream's name (`stream:{triage:<subject>}:<id>`). Every stream endpoint
  builds it from the session, so Sam, signed in, names a stream under
  `triage:sam` when he asks for Alex's run ID. That stream does not exist, and
  he gets the same `404` as for any unknown stream.
- **Rate-limit identifier.** The run quota counts per subject (section 5.4).

Access to streams is the app's job, as the streams design requires. The
`subject_of` dependency is the access check: every stream endpoint requires a
session, so a signed-out phone gets a `401`. The SDK has no owner and
compares nothing: the scope in the name does the work. The phone and the
laptop are two sessions of one subject, so they build the same scope and reach
the same run.

Runs stay private to one engineer, as in a chat. Sam starts a separate run and
shares Alex's cached tool results, not Alex's stream.

Clocks: idle 5 minutes, absolute 30 minutes. The session loads at the start of
a request, and a long SSE response does not refresh the idle clock. The
stream endpoint passes `session_expiry(request)` to `sse()`, so a response
ends at the session's absolute deadline with a `stream.error` event whose code is
`credential_expired`. Every response also ends after 60 seconds
(`stream_max_connection_seconds`), with no event. The browser then reconnects
by itself, and `subject_of` runs again. A reconnect after the idle clock runs
out, or after a sign-out, gets a `401`, and the UI sends the engineer back to
sign-in. The stream TTL does not follow the session: a run goes on after its
session ends.

### 5.2 Caching

| Endpoint | Latency without cache | TTL |
|---|---|---|
| `GET /api/tools/metrics?service=` | 1,500 ms | 5 minutes |
| `GET /api/tools/deploys?service=` | 800 ms | 5 minutes |

Both tools cover the last hour. The fake APIs sleep, then return seeded data.
Each endpoint uses `Depends(cache(ttl=300))`. Tool endpoints do not read the
session, so all engineers share one cache. That is the point of step 5.

The agent reports each tool call as a stream event with the `X-Redis-Cache`
value and the latency, so the chat shows "metrics: HIT, 4 ms" next to
"metrics: MISS, 1,512 ms". On a hit the endpoint never runs.

The SSE endpoint must never pass through `cache()`. It is private to one
engineer.

### 5.3 The planner

```python
async def triage(req: TriageRequest, w: StreamWriter) -> None:
    await w.append("status", {"text": "Checking the metrics"})
    metrics = await call_tool(w, "metrics", service=req.service)
    deploys = await call_tool(w, "deploys", service=req.service)
    for chunk in report(req, metrics, deploys):  # seeded template
        await w.append("delta", {"text": chunk})
        await asyncio.sleep(PACE)
    # spawn() writes the final "stream.end" entry when this returns


async def call_tool(w: StreamWriter, name: str, **params) -> dict:
    while True:
        r = await tools.get(f"/api/tools/{name}", params=params)
        if r.status_code != 429:
            await w.append("tool", {"name": name,
                                    "cache": r.headers["X-Redis-Cache"],
                                    "ms": elapsed_ms(r)})
            return r.json()
        wait = int(r.headers["Retry-After"])
        await w.append("status", {"text": f"{name} is busy, retrying in {wait} s"})
        await asyncio.sleep(wait)
```

The retry loop stays because the global rate limit (section 5.4) also applies
to the planner's in-process tool calls. Under load, the planner waits as
`Retry-After` says instead of failing the run.

Each service has seeded data; only checkout has an incident. Its report reads
like this: "At 14:02 checkout v2.31 shipped. It raised the payment client's
retries from 1 to 3. Since then p99 latency rose from 400 ms to 2.3 s, and
payment timeouts rose from 0 to 1,214. Next step: roll back to v2.30."

`PACE` is 150 ms after each word-sized delta, so a report of about 100 words
takes 15 to 20 seconds. That is long enough to drop the connection and switch
devices by hand. Both planners use it.

### 5.4 Rate limiting

| Where | Limit | Identifier | Purpose |
|---|---|---|---|
| `POST /api/triage/runs` | 5 a minute | Session subject | Each run costs money. Step 6. |
| All routes | One global limit | A constant | Keeps the app under the free database's 100 operations a second. Not a demo control. |
| `POST /api/triage/runs`, model planner only | 500 a day | `"all"` | Bounds the model bill (section 5.6). |

The run limit identifies engineers by session, not by IP, because
`ip_identifier` ignores `X-Forwarded-For` (section 9, risk 3).

The run limit is a dependency, so it counts every start request, including one
that then gets `409` because a run is already active. "Ask again ×6" in step 6
relies on that: the busy starts count, and the sixth gets `429`.

The UI shows `X-RateLimit-Remaining`, `X-RateLimit-Reset` and the IETF
`RateLimit` headers next to the send button.

### 5.5 Resumable streams

| Endpoint | SDK call |
|---|---|
| `POST /api/triage/runs` | `streams.open(scope=f"triage:{subject}", stream_id=body.run_id)`, then `streams.spawn(writer, triage)` |
| `GET /api/triage/runs/active` | `streams.active(f"triage:{subject}")` |
| `GET /api/streams/{id}/events` | `await streams.sse(id, request=request, scope=f"triage:{subject}", expires_at=session_expiry(request))` |
| `POST /api/streams/{id}/cancel` | `streams.cancel(id, scope=f"triage:{subject}")` |

Every endpoint depends on `subject_of`, so access is checked before any SDK
call. The routes need no chat ID, unlike the design's example, because the
scope comes from the session, not from the URL.

The browser makes `run_id` before it sends, so a double click or a retried
`POST` returns the same stream. The scope allows one active run per engineer.
Cancel is the way out of a run with the wrong question; without it, the
engineer waits for the run to end before asking again.

### 5.6 The model planner

The planner is one function, `triage(req, w)`. There are two versions with the
same signature: the model planner and the scripted planner of section 5.3.
Redis does not see the difference.

| Setting | Planner |
|---|---|
| `ANTHROPIC_API_KEY` set (the deployed default) | The model planner, on `claude-haiku-5-5` at `effort: "low"`, with streaming. |
| `ANTHROPIC_API_KEY` unset, for example a local run | The scripted planner. |

The model planner is a short manual tool loop. It uses a manual loop, not the
SDK's tool runner, because it must append every text delta to the Redis
stream and send every tool call through the app's own endpoints:

```python
client = anthropic.AsyncAnthropic()  # reads ANTHROPIC_API_KEY


async def triage_model(req: TriageRequest, w: StreamWriter) -> None:
    messages = [{"role": "user", "content": req.prompt}]
    while True:
        async with client.messages.stream(
            model="claude-haiku-5-5",
            max_tokens=4000,  # a cost cap; a report needs far less
            system=SYSTEM,
            tools=TOOLS,
            output_config={"effort": "low"},
            messages=messages,
        ) as stream:
            async for text in stream.text_stream:
                await w.append("delta", {"text": text})
                await asyncio.sleep(PACE)
            message = await stream.get_final_message()
        if message.stop_reason != "tool_use":
            if message.stop_reason in ("refusal", "max_tokens"):
                raise PlannerFailed(message.stop_reason)
            return  # spawn() writes the final "stream.end" entry
        messages.append({"role": "assistant", "content": message.content})
        calls = [b for b in message.content if b.type == "tool_use"]
        results = await asyncio.gather(*(run_tool(w, c) for c in calls))
        messages.append({"role": "user", "content": list(results)})
```

`run_tool` calls `call_tool` from section 5.3 and wraps the result in a
`tool_result` block. Parallel tool calls run together, and all results go back
in one user message. `PACE` keeps the model's answer as slow as the script's,
so the visitor has time to press "Drop VPN". The token cost stays the same.

Tool definitions:

- Each tool sets `strict: true`, so the API validates every tool input.
- `service` is an `enum` of the seeded services. The model then sends the same
  parameter for the same question, so the cache keys match and step 5 (Sam's
  cache hits) still works with a model.
- `eager_input_streaming` stays off. The input is one short field, and the
  API's own validation under `strict: true` matters more here.

Failure handling:

| When | What happens |
|---|---|
| A `429`, `5xx` or connection error from Anthropic | The SDK retries twice by default. |
| The model fails before the first `delta` | The scripted planner takes over and a `status` event says so. The engineer still gets a report. |
| The model fails after text has streamed | The planner raises `PlannerFailed("model_error")`, and `spawn()` ends the run with `stream.end` and data `{"reason": "error", "code": "model_error"}`. Mixing two planners' text in one answer would confuse the reader. |
| `refusal` or `max_tokens` stop reason | Treated as a failure, by the same two rules. Haiku 5.5 has no server-side refusal fallback. |

Haiku 5.5's default effort is `medium`. Set `low` explicitly; a two-tool
planner does not need more.

Cost: Haiku 5.5 is $0.10 per million input tokens and $0.50 per million output
tokens (Anthropic API rates as of 2026-10-06). At about 8,000 input and 1,000
output tokens per run, a run costs about $0.0013. The estimate is not
measured.

#### Daily model budget

The rate limiter also bounds the bill: at most 500 model runs a day, shared by
all engineers.

This limit cannot be a `Depends(rate_limit(...))` dependency. A dependency
answers `429` when the limit is reached, but the start endpoint should fall
back to the scripted planner instead. So the handler calls the backend
directly, through `RateLimitBackendDep`, after the per-engineer limit has
passed:

```python
@app.post("/api/triage/runs", status_code=202,
          dependencies=[Depends(rate_limit("5/minute", identifier=by_subject))])
async def start(body: RunIn, streams: StreamBackendDep,
                limits: RateLimitBackendDep, subject: str = Depends(subject_of)):
    # open() first: a busy scope raises StreamBusy (409) before any budget is spent
    writer = await streams.open(scope=f"triage:{subject}", stream_id=body.run_id)
    if not writer.created:
        return {"stream_id": writer.stream_id}  # a retried start; no second budget slot
    planner = triage_scripted
    note = None
    if model_configured():
        budget = await limits.hit("all", limit=500, window=86_400,
                                  scope="model-runs", fail_closed=True)
        if budget.allowed:
            planner = triage_model
        else:
            note = f"Daily model budget spent; back in {budget.retry_after} s"
    streams.spawn(writer, lambda w: run_planner(planner, body, w, note))
    return {"stream_id": writer.stream_id, "planner": planner.name}
```

`run_planner` appends `note`, if any, as the first `status` event, then calls
the planner. `planner.name` stands for a short label, such as `"model"` or
`"scripted"`, that the UI shows.

How the SDK behaves here:

- **A rejected hit does not count.** When a run would pass the limit, the
  backend leaves the counter as it is. Scripted runs never use the budget.
- **The window starts at the first model run** and lasts 24 hours. It is a
  fixed window, so it does not reset at midnight. On Redis 8.6.2 the backend
  uses its Lua script, because `INCREX` needs 8.8. The result is the same.
- **`fail_closed=True`.** If Redis cannot be reached, the hit counts as
  rejected, so an outage never leads to unbounded spend. The demo cannot
  stream without Redis anyway.
- **A retried start costs nothing more.** The browser's `run_id` makes a
  repeated `POST` return the same stream: `open()` returns a writer whose
  `created` is `False`, and the handler returns before `hit()`, so a retry
  starts no second planner and takes no second slot.
- **A busy scope costs nothing.** The handler calls `open()` before `hit()`.
  If another run is already active, `open()` raises `StreamBusy` (`409`)
  before the budget is touched.
- **A model failure still uses its slot.** The call was made and billed, so
  the count is correct.

When the budget is spent, the engineer still gets a report, and the `status`
event says when model runs come back. At the estimate above, 500 Haiku runs
cost about $0.65 a day.

## 6. Demo controls

One page. The chat is on the left, the Redis lens (section 7) on the right,
and a row of controls below.

Static mocks of every screen, with the HTTP and SSE traffic and the lens
state for each, are in [`mockups/incident-triage/`](mockups/incident-triage/index.html).
Open `index.html` in a browser; it needs no server.

| Control | Story step | What the visitor sees | Feature shown |
|---|---|---|---|
| Send | 1, 2 | Tool cards with cache badges, then streamed text | Caching, streams |
| Drop VPN | 3 | The `EventSource` closes; the run goes on | Disconnect never cancels |
| Reconnect | 3 | Resume from the last entry ID; the page counts the entries since then and any repeats | Resume from cursor |
| Open on phone | 4 | A second window signs in with the same handle and follows the run | Sessions per subject, `active()`, fan-out |
| Be Sam | 5 | A window with another handle asks the same question; both tools HIT | Shared cache |
| Ask again ×6 | 6 | Busy starts get `409`; the sixth start gets `429` and a countdown | Per-engineer quota |
| Sign out everywhere | 7 | The phone's next request gets `401`. Its open stream ends within 60 seconds, and the reconnect gets `401` | `revoke_all`, connection deadline |

One browser plays all three devices. A `device` query parameter (`laptop`,
`phone` or `sam`) gives each device its own session cookie: `devices.py`
renames the cookie on the way in and out, and the SDK sees an ordinary
`session` cookie. A real app does not need this, because each device is its own
browser.

Each signed-in device calls `GET /api/triage/runs/active` every 4 seconds. That
is how the phone finds the run in step 4. In step 7 the poll shows the `401`
within 4 seconds, before the 60-second connection cap ends the stream (section
9, risk 7).

## 7. Redis lens

The right-hand pane shows the Redis keys behind the engineer's own state,
live. The names use the SDK's default prefix, `redis:fastapi`; the demo does
not set `REDIS_PREFIX`. `GET /api/lens` reads them and returns only the caller's keys:

| Key | Shown |
|---|---|
| `redis:fastapi:stream:{triage:<subject>}:<id>` | `XLEN`, TTL, last entry ID |
| `redis:fastapi:stream-meta:{triage:<subject>}:<id>` | Status, cancel flag, final entry ID, TTL |
| `redis:fastapi:stream-alive:{triage:<subject>}:<id>` | Present or not, TTL in ms |
| `redis:fastapi:stream-active:{triage:<subject>}` | The stream ID it names, TTL |
| Session record | Idle and absolute TTLs |
| Run quota | Count and TTL of the current window |
| Daily model budget | Count and TTL of the current window |

## 8. Replayable and self-healing

### 8.1 State and inputs

- Only Redis holds state. Every key has a TTL. An empty database is a valid
  start, so there is nothing to seed or migrate. Nothing calls `FLUSHDB`.
- The fake observability APIs use seeded data, so tool results are the same
  every run. The model is the only outside service. Without it the scripted
  planner runs, so a missing key, an outage or a spent daily budget never
  breaks the demo.
- The web files are plain HTML, CSS and JavaScript, committed as they are and
  served with `app.frontend("/", directory=str(Path(__file__).parent / "web"))`.
  The path is relative to the module, so it works from any working
  directory. FastAPI Cloud does not run frontend builds, so plain files keep
  the deploy to one command.
- FastAPI Cloud injects both secrets, `REDIS_URL` and `ANTHROPIC_API_KEY`.
  Only `REDIS_URL` is required. The demo timings are fixed in code.

### 8.2 Free-tier budget

| Limit (free database) | Demo setting |
|---|---|
| 30 connections | Pool of 20 (`max_connections=20`); `stream_max_readers=8`. Over the limit, the SSE endpoint answers `503` with `Retry-After`. |
| 30 MB memory | Stream `maxlen` of 500; every TTL 30 minutes or less. |
| 100 operations a second | The global rate limit in section 5.4. |

### 8.3 Failure handling

- **Startup.** The lifespan pings Redis, and the SDK detects stream support
  the way its other features do: `COMMAND INFO XCFGSET DELEX`, once. Neither
  check fails the app. On a server without the commands, the SDK logs one
  warning and turns streams off: every stream entry point raises
  `StreamUnsupported`, which answers `503`. If Redis is down at startup, the
  answer stays unknown and the health check asks again.
- **Health.** `GET /api/health` reports each feature as available or not, with
  a reason, and which planner is active. The target database runs 8.6.2 or
  later. If the app ever meets an older server, for example a developer's
  local Redis, the chat is disabled and the page shows "needs Redis 8.6.2 or
  later". Sessions need Redis 8.0, so on an older server sign-in answers `503`
  with the reason too. The tools and the lens still answer. Checked on
  2026-10-23 against Redis 8.8 (streams on, a run starts) and 7.4 (the app
  starts, streams off, one warning).
- **Client.** `EventSource` reconnects by itself and sends `Last-Event-ID`.
  `sse()` reads the cursor itself, in the design's order: `Last-Event-ID`
  before the `?after=` cursor the page built the `EventSource` with, because
  the header is newer. The route does not copy the query into `after`. A
  malformed or repeated cursor gets `422`, and a cursor past the run's last ID
  gets `409`; the page says so. On load, the page calls
  `/api/triage/runs/active` and follows any run in progress, so a reload never
  loses an answer. A `credential_expired` event sends the device back to
  sign-in; the run goes on.
- **Slow clients.** The SDK sends every chunk under a 10-second limit. A
  device whose tab freezes in the middle of a replay is closed, its reader
  slot is freed at once, and the browser resumes from `Last-Event-ID` when it
  wakes. With only 8 reader slots (section 8.2), this keeps one frozen phone
  from taking a slot for the full connection cap.
- **Dead producers.** If a planner task dies, for example in a redeploy, its
  `alive` key expires and the reader writes the `stream.end` entry the planner
  never wrote, with `reason` `producer_lost`. The page shows "The agent stopped
  unexpectedly", and the reconnect gets `204`, so the browser stops retrying.
  The engineer asks again, which starts a new run. The demo has no worker
  process to avoid this: the streams design supports producers inside the
  FastAPI application only in phase 1, so a redeploy ends an answer in
  progress. Readers survive a redeploy; producers do not.
- **Finished streams.** The planners never call `finish()`. `spawn()` writes
  the final `stream.end` entry, with `reason` `done`, `error` or `cancelled`,
  and the page shows the outcome from that data. A reconnect past the final
  entry gets `204`, and the page says the run has ended.
- **Expired streams.** A finished stream stays readable for `stream_done_ttl_seconds`
  (default 2 minutes). A resume after that gets `410`. The page says the
  answer has expired and never starts the run again by itself.

### 8.4 Deploy

| When | Step |
|---|---|
| Once | Create the app, and note its UUID. |
| Once | Link a Redis Cloud database to the app in the FastAPI Cloud dashboard. The docs list no CLI command for this. |
| Once | Make `ANTHROPIC_API_KEY` available to the app as a secret. It is already injected in the target environment. |
| Once | Create a deploy token. |
| Every deploy | `uv run nox -f examples/incident_triage_app/deploy/noxfile.py`, with the token in `FASTAPI_CLOUD_TOKEN` and the app's UUID in `FASTAPI_CLOUD_APP_ID`. By hand, not from CI. |
| Local | Docker Compose with `redis:8.8` and the app. |

The steps, with commands, are in
[`examples/incident_triage_app/deploy/README.md`](../examples/incident_triage_app/deploy/README.md).

FastAPI Cloud installs what the deployed folder's `pyproject.toml` lists. The
root `pyproject.toml` describes the SDK, which does not depend on `httpx` or
`anthropic`, and its FastAPI floor predates `app.frontend()`. So the demo does
not deploy from the repository root. `deploy/bundle.py` writes a separate
folder instead, outside the repository:

- `pyproject.toml`: the SDK's dependencies, plus `fastapi[standard]>=0.138`,
  `httpx` and `anthropic`; Python pinned to 3.13, as FastAPI Cloud advises for
  an application; `[tool.fastapi] entrypoint` set to
  `examples.incident_triage_app.main:app`.
- `src/redis_fastapi/`: the SDK at this commit. `main.py` puts `src/` on
  `sys.path`, so the streams feature needs no release.
- `examples/incident_triage_app/`, with a `build.json` that names the commit.
  `GET /api/health` reports it, so a smoke test can tell which commit is live.

The nox session, in `deploy/noxfile.py` and not in the root noxfile, builds the folder, installs its dependencies and imports the
app before it calls `fastapi deploy <folder> --app-id <ID>`, so a broken
import fails before the upload. It reads the token and the app ID from the
environment only; the repository is public, so neither is written to a file.
The CLI takes the app's UUID, not its name, and a deploy token cannot look the
UUID up.

The root `[tool.fastapi] entrypoint` still names `examples.main:app`.
Deploying the demo to the app that serves `examples.main:app` replaces it, as
decided; `examples/main.py` still runs locally with
`fastapi dev examples/main.py`.

### 8.5 Layout

The folder name uses underscores, not hyphens, because the entrypoint is a
Python import path and `examples.incident-triage-app` cannot be imported. It
matches the other example packages, such as `examples/rate_limiting/`.

```
examples/incident_triage_app/
  __init__.py
  main.py         app, FastAPIRedis(app).lifespan().caching().rate_limiting().sessions(),
                  streams setup, app.frontend()
  config.py       the demo's limits and clocks
  health.py       the server version check, and the features it turns off
  devices.py      one session cookie per device, for the demo only (section 6)
  session.py      sign in, sign out everywhere
  tools.py        fake observability APIs with cache()
  planner.py      call_tool(), the scripted planner, and the choice of planner
  model.py        the model planner (section 5.6)
  runs.py         start, active, events, cancel
  lens.py         /api/lens, /api/health
  deploy/         bundle.py, which writes the folder FastAPI Cloud deploys;
                  noxfile.py, which deploys it; the setup steps (section 8.4)
  web/            index.html, app.js, style.css
  compose.yaml    redis:8.8 and the app, for local runs
  README.md
```

## 9. Risks and open questions

Risks:

| # | Risk | Mitigation |
|---|---|---|
| 1 | The story depends on the SDK's streams feature, which needs Redis 8.6.2 or later. | The target database runs 8.6.2 or later. On an older server the SDK turns streams off, the chat answers `503`, and the page says why (section 8.3). |
| 2 | The model makes the wording differ between runs and adds a cost. | Redis behaviour does not depend on the wording. A daily budget bounds the cost, and the script takes over when it is spent. |
| 2a | Two copies of the same start `POST` arriving together could start the planner twice and use two budget slots. | Resolved: `open()` returns `created=False` for the second copy, and the handler spawns and calls `hit()` only when `created` is `True`. `open()` takes the slot and creates the run in one `WATCH`/`MULTI` transaction, so exactly one copy creates it. |
| 3 | `ip_identifier` ignores `X-Forwarded-For`, so behind FastAPI Cloud's proxy all engineers may share one budget. | Identify engineers by session subject. |
| 4 | FastAPI Cloud's scaling is unknown to this design: replica count, scale to zero and SSE timeouts. Its autoscaling docs page returned 404. | Runs live in Redis, so readers can follow a run from any replica. A planner runs on the replica that started it, so scale to zero, or a deploy, ends the answers in progress, and their readers see `producer_lost`. Finished answers, sessions, the cache and the quotas live in Redis and survive. |
| 5 | An in-process call through `ASGITransport` shares the event loop with the SSE readers. | Tool latency is simulated with `asyncio.sleep`, so nothing blocks the loop. |
| 6 | A crashed producer holds the active slot until a reader notices or the slot's TTL runs out (10 minutes), so its engineer cannot start a new run. | The page follows the active run on load, and its reader ends a dead producer's stream (`producer_lost`), which releases the slot within the heartbeat key's 15 seconds. With no reader, the slot lasts until its TTL. |
| 7 | An open SSE response does not check the session again. After "sign out everywhere", the phone keeps reading until the connection ends. | Taken into the streams design (decision 2): the response ends at the session's deadline and after `stream_max_connection_seconds`, 60 seconds here, and the reconnect gets `401`. The page's 4-second poll still shows the `401` sooner. |
| 8 | `StreamEvent` has no `q`, so the browser cannot prove that no entry was lost on a resume. | The page reports the entries since the cursor and any repeats by entry ID. Feed back to the streams design if a "0 lost" check is wanted. |

Decisions:

1. **Location: decided 2026-10-19, renamed 2026-10-20.**
   `examples/incident_triage_app/`, deployed as
   `examples.incident_triage_app.main:app` (section 8.4).
2. **Database: decided 2026-10-19.** A Redis Cloud database on 8.6.2 or later
   is available. Section 8.2's budget assumes the free tier's limits; a larger
   plan only adds headroom.
3. **Model: decided 2026-10-19.** An Anthropic API key is injected in the
   FastAPI Cloud environment. The model planner on Claude Haiku 5.5 is the
   default; the scripted planner is the fallback (section 5.6).
4. **Model budget: decided 2026-10-19.** At most 500 model runs per 24 hours,
   about $0.65 at the estimate, enforced by the SDK's rate-limit backend
   (section 5.6).
5. **Topic: decided 2026-10-20.** Incident triage replaces trip planning. Most
   of the expected audience builds, runs or leads backend systems, and on-call
   work gives each story step a real reason. Only the domain changed: the
   tools, the seeded data, the names and the prose.
6. **Minimal feature set: decided 2026-10-20.** A feature stays only if the
   story needs it or no other part of the demo shows the same thing. Removed:

   | Removed | Why |
   |---|---|
   | The `logs` tool | Showed the same caching as `metrics` and `deploys`. Two tools still make a believable cause. |
   | Different tool TTLs (1 and 10 minutes) | Showed nothing new, and a 1-minute TTL could make Sam's step 5 miss. Both tools use 5 minutes. |
   | `GET /api/services`, its `ETag` and `304` | A third cached endpoint. The page and the tool `enum` use one constant list. |
   | The `window` parameter and the distinct-key check (#73) | Not part of the story. Both tools cover the last hour. |
   | `cost=` on the run limit | A second form of the same per-engineer quota. |
   | The shared tool limit (20 a minute) | A second per-route limit. The `429` retry loop stays, for the global limit. |
   | Sign out this device, and the device list | `revoke_all` shows revocation; a second sign-out showed the same thing. |
   | "Come back later" (`410`) | Needed a shortened TTL and a wait. The page still handles `410` (section 8.3). |
   | `TRIAGE_PLANNER` and `TRIAGE_MODEL` | `PACE` gives both planners the same speed, so a scripted override is not needed for the live effect. |
   | The telemetry strip and `.otel()` | Repeated what the tool cards, the quota chips and the resume banner already show, and added a dependency. |
   | Sonnet and Opus prices, and the free-provider table | Background for decisions 3 and 4, which are made. |

   Kept for resilience, though no story step shows them: the global rate
   limit, the `429` retry loop, the scripted fallback, `fail_closed=True`,
   health and degraded mode, `active()` on page load, and `410` handling.

7. **Deploy: decided 2026-10-20.** The demo deploys from a folder that
   `deploy/bundle.py` writes, with its own `pyproject.toml`, through the
   manual nox session in `deploy/noxfile.py` (section 8.4). The SDK's `pyproject.toml` gains
   no demo dependencies.
8. **Stream access: decided 2026-10-23.** The demo follows the streams
   design's decision 2. `subject_of` is the access check, every stream's name
   includes the scope `triage:<subject>`, and `sse()` gets `expires_at` from
   `session_expiry(request)`. The connection cap is 60 seconds, so step 7
   works even without the page's poll.
9. **End marker: decided 2026-10-23.** The demo follows the streams design's
   section 5.2. The page listens for `stream.end` and `stream.error` only, and
   reads the outcome from the data, not from the event type. The lens shows
   meta `status` as `running` or `finished`, and the `final` entry ID.
10. **Cursor rules: decided 2026-10-23.** The demo follows the streams
    design's section 5.1. The SDK validates cursors at the boundary, compares
    them as numbers, treats no cursor as `0-0`, and refuses a cursor past the
    stream's last ID. The events route passes no `after`.
11. **Problems after the first byte: decided 2026-10-23.** The demo follows
    the streams design's section 7.2. A dead planner's stream is ended by the
    reader, and every other problem closes the connection with no event, so
    `EventSource` resumes. The page handles only `stream.end` and the
    `credential_expired` hint; it needs no list of error codes.

12. **The SDK's streams: decided 2026-10-24.** The demo uses
    `FastAPIRedis(app).streams()` and `StreamBackendDep`; `streams_stub.py` is
    gone. The stream clocks are `REDIS_STREAM_*` defaults set in `main.py`
    before any setting is read: a 5-second heartbeat, a 15-second heartbeat
    key, 8 readers, a 500-entry cap and a 60-second connection cap. A retried
    start returns `created=False`, so it starts no second planner and spends
    no budget. The lens reads the stream's keys from Redis under the layout
    the streams guide documents. The page shows a `stream.gap` as a note that
    the answer is incomplete.

No open questions remain.

## 10. Sources

- [FastAPI: Frontend](https://fastapi.tiangolo.com/tutorial/frontend/)
- [FastAPI Cloud: Deploy a Full-Stack App](https://fastapicloud.com/docs/builds-and-deployments/frontend/)
- [FastAPI Cloud: Redis Cloud Integration](https://fastapicloud.com/docs/integrations/redis-integration/)
- [Redis Cloud Essentials plans](https://redis.io/docs/latest/operate/rc/subscriptions/view-essentials-subscription/essentials-plan-details/)
- [Redis Cloud changelog, May 2026](https://redis.io/docs/latest/operate/rc/changelog/may-2026/)
- Anthropic model prices: the Claude API reference, cached 2026-10-06
