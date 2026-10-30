# fastapi-redis-sdk examples

Each example is a small, complete app built around one real use case. Every
module docstring explains what the app does and lists the `curl` steps to try
it.

## Prerequisites

- Python 3.10+
- A Redis 7.4+ server (local or [Redis Cloud](https://redis.io/cloud/))
- [uv](https://docs.astral.sh/uv/)

```bash
# from the repository root
uv sync --all-groups
export REDIS_URL=redis://localhost:6379/0
```

## The examples

| Feature | Example | What it shows | Run |
|---|---|---|---|
| Caching | [`caching/catalogue.py`](caching/catalogue.py) | A product catalogue: a cached listing with `ETag`s, product pages cached only once published, and an edit that evicts the catalogue | `uv run --with "fastapi[standard]" fastapi dev examples/caching/catalogue.py` |
| Rate limiting | [`rate_limiting/public_api.py`](rate_limiting/public_api.py) | A public API: search and autocomplete sharing one budget, a sign-in that locks out password guessing, and an endpoint metered by tokens | `uv run --with "fastapi[standard]" fastapi dev examples/rate_limiting/public_api.py` |
| Sessions | [`sessions/oauth_challenge.py`](sessions/oauth_challenge.py) | Sign-in with [Authlib](https://docs.authlib.org/) against a mock provider, and the `WWW-Authenticate` header an OAuth client reads on a refusal - including step-up with `max_age` | `uv run --with "fastapi[standard]" --with authlib fastapi dev examples/sessions/oauth_challenge.py` |
| Sessions | [`sessions/csrf.py`](sessions/csrf.py) | CSRF protection with a token kept in the session, replaced at sign-in | `uv run --with "fastapi[standard]" --with python-multipart fastapi dev examples/sessions/csrf.py` |
| Sessions | [`sessions/encryption.py`](sessions/encryption.py) | The session payload encrypted at rest with AES-GCM | `uv run --with "fastapi[standard]" --with cryptography fastapi dev examples/sessions/encryption.py` |
| Observability | [`observability/`](observability/) | Traces and metrics in Grafana, with Docker Compose | see its [README](observability/README.md) |
| All four, and resumable streams | [`incident_triage_app/`](incident_triage_app/) | An on-call agent that streams its answer through Redis: resume after a dropped connection, a second device, a shared tool cache and per-engineer quotas. Needs Redis 8.6.2+; streams come from a stub until the SDK has them | see its [README](incident_triage_app/README.md) |

## Agent SDKs on resumable streams

Each app hosts one agent SDK and streams its answer through Redis, so a client
that drops its connection reconnects with `Last-Event-ID` and gets the rest.
All four map the SDK's events to the same `delta`, `tool_call`, `tool_result`
and `message` events, and have the same start, events and cancel routes. They
are the recipes from the [streams guide](../docs/guide/streams.md#agent-sdk-recipes).
They need Redis 8.6.2+ and an API key for the model's provider; `AGENT_MODEL`
picks the model.

| SDK | Example | What it adds | Run |
|---|---|---|---|
| OpenAI Agents SDK | [`agent_sdks/openai_agents_chat.py`](agent_sdks/openai_agents_chat.py) | The base recipe | `uv run --with "fastapi[standard]" --with openai-agents fastapi dev examples/agent_sdks/openai_agents_chat.py` |
| PydanticAI | [`agent_sdks/pydantic_ai_chat.py`](agent_sdks/pydantic_ai_chat.py) | A tool that returns a dataclass; `AGENT_MODEL=test` runs without a key | `uv run --with "fastapi[standard]" --with "pydantic-ai-slim[openai]" fastapi dev examples/agent_sdks/pydantic_ai_chat.py` |
| Google ADK | [`agent_sdks/google_adk_chat.py`](agent_sdks/google_adk_chat.py) | One ADK session per chat, so the agent remembers earlier questions | `uv run --with "fastapi[standard]" --with google-adk fastapi dev examples/agent_sdks/google_adk_chat.py` |
| LangGraph | [`agent_sdks/langgraph_chat.py`](agent_sdks/langgraph_chat.py) | An approval before each tool call: `interrupt()` ends the turn, and the answer starts the next. Needs Python 3.11+ | `uv run --python 3.12 --with "fastapi[standard]" --with langgraph --with langchain-openai fastapi dev examples/agent_sdks/langgraph_chat.py` |

Each module docstring lists the `curl` steps.

The session examples run over plain HTTP, so set
`REDIS_SESSION_COOKIE_HTTPS_ONLY=false` for them; the encryption example also
needs a `SESSION_KEY`. Their docstrings give the exact commands.

## The deployable demo

[`main.py`](main.py) combines the catalogue and the public API in one app,
with a health check (`GET /`) and a Redis `PING` (`GET /ping`). It is the app
FastAPI Cloud deploys:

```bash
uv run --with "fastapi[standard]" fastapi dev examples/main.py
```

To deploy it to [FastAPI Cloud](https://fastapicloud.com/):

1. Push the repo to GitHub.
2. Link the repo in the FastAPI Cloud dashboard (or run `fastapi cloud deploy`).
3. In the app's **Storage** tab, connect a Redis Cloud database. FastAPI Cloud
   sets `REDIS_URL` for you.
4. The `[tool.fastapi] entrypoint` in `pyproject.toml` points to
   `examples.main:app`, so the app is found on deploy.

Optionally set more variables:

```bash
fastapi cloud env set REDIS_PREFIX myapp
fastapi cloud env set REDIS_DEFAULT_TTL 300
```

The [incident triage demo](incident_triage_app/) deploys separately, with
`uv run nox -f examples/incident_triage_app/deploy/noxfile.py`: see [its deploy guide](incident_triage_app/deploy/README.md).
