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
