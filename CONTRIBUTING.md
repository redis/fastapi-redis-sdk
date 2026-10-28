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

## Releasing

The version lives in `pyproject.toml`, `uv.lock` and
`src/redis_fastapi/__init__.py`, and branches hold the version they will release
next:

- `main` holds the next minor or major version, for example `0.9.0`.
- A release branch `X.Y.x`, for example `0.9.x`, holds the next patch release.

Releases come only from release branches. Workflows execute the release, and
change a branch's version through a pull request, avoiding direct push.

### Create a release branch

Run **Actions → Create Release Branch** on `main`, and choose whether `main`
moves to the next `minor` or `major` version. The workflow:

1. creates `0.9.x` from `main`, which holds `0.9.0`;
2. opens a pull request that bumps `main` to `0.10.0` (or `1.0.0`).

Merge that pull request soon, so `main` and the new branch don't show the
same version for long.

### Release

Publish a release on the GitHub **Releases** page:

- **Tag:** a new tag named after the version the branch holds, for example
  `0.9.1`. The tag has no `v` prefix.
- **Target:** the release branch, for example `0.9.x`.
- **Notes:** write them, or use **Generate release notes**. For a patch, choose
  the previous patch on the same branch as the previous tag.
- **Set as the latest release:** clear this box for a patch of an older
  series.

GitHub creates the tag, and publishing starts **Release to PyPI**, which:

1. checks that the tag matches the version the tagged commit holds, and that
   the commit is on the target branch;
2. runs the security scan and the tests;
3. publishes the packages to PyPI and attaches them to the release;
4. opens a pull request that bumps the branch to the next patch, for
   example `0.9.2`.

Merge the bump pull request before the next release from that branch.
Until then, the branch still holds the released version.

If the check fails, nothing reaches PyPI, but the release and its tag exist.
Delete both, fix the cause, and publish again:

```bash
gh release delete 0.9.1 --cleanup-tag --yes
```

To try a release, run **Actions → Release to PyPI** by hand on the release
branch. It publishes to TestPyPI, or only builds if you choose a dry run. It
never tags, attaches packages or opens a pull request.

### Patch releases

Merge a fix into `main` first, then backport it to the release branch with a
pull request, and publish the release from that branch. Never backport the
bump commits from `main`.

### If a run fails

Re-run the failed jobs. A re-run is safe: it skips files already on PyPI,
replaces packages already attached to the release, and updates a bump pull
request already open. Re-running the bump job also rebuilds its pull request from
the latest branch, which resolves conflicts in `uv.lock`.

