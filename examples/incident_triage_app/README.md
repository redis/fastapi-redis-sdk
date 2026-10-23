# Incident triage demo

An on-call agent that finds out why a service is slow and streams its answer
through Redis. One story uses all four features of `fastapi-redis-sdk`:

| Feature | Where the story needs it |
|---|---|
| Sessions | Alex signs in on a laptop, follows the run on a phone, and signs out everywhere after leaving the phone behind. |
| Caching | The agent's tools query slow observability APIs. Sam asks about the same service, and every tool call is a cache hit. |
| Rate limiting | Five runs a minute per engineer, 500 model runs a day for everyone, and one global limit for the free database. |
| Resumable streams | The VPN drops and the answer resumes from the last entry. A reload, a second device or a redeploy never loses the answer. |

The design is in [`specs/resumable-streams-demo.md`](../../specs/resumable-streams-demo.md).

**Resumable streams are not in the SDK yet.** `streams_stub.py` stands in for
them. It has the API of the
[streams design](../../specs/resumable-streams-design.md), but it keeps its
state in memory, not in Redis: the state does not survive a restart and is not
shared between replicas. When the feature lands, only the imports change.

## Run it

You need Redis 8.6.2 or later.

With Docker Compose, Redis and the app together:

```sh
docker compose -f examples/incident_triage_app/compose.yaml up
```

Or with your own Redis:

```sh
export REDIS_URL=redis://localhost:6379/0 REDIS_SESSION_COOKIE_HTTPS_ONLY=false
uv run --with "fastapi[standard]" --with anthropic \
    fastapi dev examples/incident_triage_app/main.py
```

Open <http://localhost:8000>. Set `ANTHROPIC_API_KEY` to use the model planner
on Claude Haiku 5.5. Without it, the scripted planner gives the same report
every run.

## Deploy it

`uv run nox -f examples/incident_triage_app/deploy/noxfile.py` deploys the demo to FastAPI Cloud, with the SDK at
the commit you have checked out. It reads the deploy token from
`FASTAPI_CLOUD_TOKEN` and the app's UUID from `FASTAPI_CLOUD_APP_ID`. The one-time setup is in
[`deploy/README.md`](deploy/README.md).

## Play the story

The controls under the chat act on Alex's laptop:

1. **Send.** The agent calls `metrics` and `deploys` (both `MISS`, 0.8 to
   1.5 s each), then streams a report.
2. **Drop VPN**, then **Reconnect.** The run goes on while the page is
   offline; the lens shows `XLEN` growing. Reconnect resumes after the last
   entry ID.
3. **Open on phone.** A second device signs in as `alex`, finds the active
   run and follows it from the start.
4. **Be Sam.** Another engineer asks the same question. Both tools are `HIT`.
5. **Ask again ×6.** Busy starts get `409` and still count. The sixth start
   gets `429` with a countdown.
6. **Sign out everywhere.** Every session for `alex` is revoked. The phone
   gets a `401` within 4 seconds.

Click a device to point the Redis lens at it. "On the wire" shows each request,
its rate-limit and cache headers, and the SSE frames.

Close a window with its red button to play its step again. The device signs
out, and a run it started is cancelled. A closed laptop opens again at the
sign-in form when you press any of its controls.

## Endpoints

| Endpoint | What it does |
|---|---|
| `POST /api/session` | Sign in with `{"handle": "alex"}`. |
| `DELETE /api/session` | Sign this device out. |
| `DELETE /api/sessions` | Sign out everywhere. |
| `POST /api/triage/runs` | Start a run: `{"run_id", "service", "prompt"}`. `202`, or `409` if one is active, or `429`. |
| `GET /api/triage/runs/active` | The active run's stream ID, or `null`. |
| `GET /api/streams/{id}/events` | The run as SSE. Resume with `Last-Event-ID` or `?after=<entry ID>`. |
| `POST /api/streams/{id}/cancel` | Stop the run. |
| `GET /api/tools/metrics?service=` | Seeded metrics, cached for 5 minutes. |
| `GET /api/tools/deploys?service=` | Seeded deploys, cached for 5 minutes. |
| `GET /api/lens` | The caller's Redis keys and counters. |
| `GET /api/health` | Each feature, available or not, and why. |

Every endpoint except the tools and the health check accepts `?device=laptop`,
`phone` or `sam`. One browser plays all three devices, and `devices.py` gives
each its own session cookie. A real app does not need this.

With `curl`, a cookie jar plays one device:

```sh
curl -c jar -b jar -X POST localhost:8000/api/session \
     -H 'content-type: application/json' -d '{"handle": "alex"}'
curl -b jar -X POST localhost:8000/api/triage/runs \
     -H 'content-type: application/json' -d '{"service": "checkout"}'
curl -N -b jar localhost:8000/api/streams/<stream_id>/events
```
