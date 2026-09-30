# Contributing

## Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/)
- Redis (for integration tests)

## Setup

```bash
uv sync --all-groups
```

## Quick validation

Run **all** CI checks locally with a single command:

```bash
uv run nox
```

This runs lint, type-check, security scan, and the full test suite — the
same sessions that CI executes on every pull request.

List available sessions:

```bash
uv run nox -l
```

## Lint and format

```bash
# Check (same as CI)
uv run nox -s lint

# Auto-fix lint and format issues
uv run nox -s fix
```

## Type checking

```bash
uv run nox -s typecheck
```

## Security scan

```bash
uv run nox -s security
```

## Tests

### Run the full suite

```bash
uv run nox -s tests
```

Coverage is enabled by default (`--cov=src`, 80% minimum). HTML report is
written to `htmlcov/`.

Nox runs the suite against every supported Python version (3.10 – 3.14).
To target a single version:

```bash
uv run nox -s tests-3.12
```

### Pass extra arguments to pytest

Append arguments after `--`:

```bash
# Run unit tests only
uv run nox -s tests -- tests/unit/

# Run a specific test
uv run nox -s tests -- tests/unit/test_cache.py::TestCacheMissHit::test_first_miss_second_hit

# Skip slow tests
uv run nox -s tests -- -m "not slow"

# Disable coverage for faster iteration
uv run nox -s tests -- --no-cov
```

### Integration tests

Integration tests require a running Redis instance (defaults to
`localhost:6379`). Tests that need Redis are decorated with `@requires_redis`
and will be skipped automatically if the server is unreachable.

```bash
uv run nox -s tests -- tests/integration/
```

Point to a different instance:

```bash
REDIS_URL=redis://host:6380/1 uv run nox -s tests -- tests/integration/
```

### Debugging tests

```bash
# Full output, no capture, long tracebacks
uv run nox -s tests -- tests/unit/test_cache.py::TestCacheMissHit -s --tb=long --no-cov

# Drop into the debugger on failure
uv run nox -s tests -- --pdb --no-cov
```

Or use `uv run pytest` directly for the fastest iteration loop (skips nox
virtualenv setup):

```bash
uv run pytest -s --no-cov tests/unit/test_config.py
```

## Documentation

Documentation is built with [MkDocs](https://www.mkdocs.org/) and the
[Material theme](https://squidfunk.github.io/mkdocs-material/).

### Serve locally

```bash
uv run nox -s docs_serve
```

Open [http://127.0.0.1:8000](http://127.0.0.1:8000) — changes are
live-reloaded.

### Build

```bash
uv run nox -s docs_build
```

The built site is written to `site/`. Add `-- --strict` to fail on
warnings (used in CI):

```bash
uv run nox -s docs_build -- --strict
```

### Deployment

`.github/workflows/docs.yml` builds the site in strict mode and deploys it
to GitHub Pages with [mike](https://github.com/jimporter/mike), which keeps
each version in its own folder on the `gh-pages` branch:

| Version | Built from | URL |
|---|---|---|
| `latest` | every docs change pushed to `main` | <https://redis.github.io/fastapi-redis-sdk/latest/> (the site root redirects here) |
| `feature/<X>` | every docs change pushed to branch `feature/<X>` in this repository | `https://redis.github.io/fastapi-redis-sdk/feature-<X>/` |

mike does not allow `/` in a folder name, so the preview for
`feature/session-management` lives at `/feature-session-management/`. The
version selector still shows it as `feature/session-management`.

To preview a larger docs change, push it to a `feature/<X>` branch in this
repository, or merge a fork's pull request into one. Deleting the branch
deletes its preview. A pull request gets the strict build but no preview.

The preview shows the branch as it is, not merged with `main`. Merge `main`
into the branch first to see the guide as it will be after the merge.

### Structure

```
docs/
├── index.md                    # Home page
├── getting-started/
│   ├── installation.md
│   └── quick-start.md
├── guide/
│   ├── caching.md
│   └── configuration.md
├── api/
│   ├── dependencies.md
│   └── configuration.md
└── stylesheets/
    └── extra.css
```

Configuration is in `mkdocs.yml` at the repository root.

