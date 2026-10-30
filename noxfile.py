"""Nox sessions — local mirror of CI checks.

Run all checks:         nox
Run one session:        nox -s lint
List sessions:          nox -l
Serve docs locally:     nox -s docs-serve

CI uses these same sessions so there is a single source of truth
for linting, type-checking, security scanning, and testing.

Dependency versions live solely in pyproject.toml's [dependency-groups];
each session installs the group(s) it needs via `uv sync` so there is no
second copy of the dependency list to keep in step.
"""

from __future__ import annotations

import nox

nox.options.default_venv_backend = "uv"
nox.options.reuse_existing_virtualenvs = True

SOURCES = ["src/", "tests/"]
PYTHON_VERSIONS = ["3.10", "3.11", "3.12", "3.13", "3.14"]
PYTHON_DEFAULT = "3.12"


def uv_sync(session: nox.Session, *args: str) -> None:
    """Install dependencies into the session venv with ``uv sync``.

    Versions are resolved from pyproject.toml / uv.lock, keeping pyproject.toml
    the single source of truth. ``--active`` targets the nox-created venv.
    """
    session.run_install(
        "uv",
        "sync",
        "--active",
        *args,
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )


# ---------------------------------------------------------------------------
# Lint & format
# ---------------------------------------------------------------------------
@nox.session(python=PYTHON_DEFAULT)
def lint(session: nox.Session) -> None:
    """Run ruff linter and formatter checks."""
    uv_sync(session, "--only-group", "lint")
    session.run("ruff", "check", *SOURCES)
    session.run("ruff", "format", "--check", *SOURCES)


# ---------------------------------------------------------------------------
# Type checking
# ---------------------------------------------------------------------------
@nox.session(python=PYTHON_DEFAULT)
def typecheck(session: nox.Session) -> None:
    """Run mypy on source code."""
    uv_sync(session, "--extra", "otel", "--no-default-groups", "--group", "typecheck")
    session.run("mypy", "src/")


# ---------------------------------------------------------------------------
# Security scan
# ---------------------------------------------------------------------------
@nox.session(python=PYTHON_DEFAULT)
def security(session: nox.Session) -> None:
    """Run bandit security scanner."""
    uv_sync(session, "--only-group", "security")
    # JSON report for CI artifact upload (non-fatal); text report (fatal).
    session.run(
        "bandit",
        "-r",
        "src/",
        "-f",
        "json",
        "-o",
        "bandit-report.json",
        success_codes=[0, 1],
    )
    session.run("bandit", "-r", "src/")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
# Integration tests alone cover ~78% of src/ - they exercise the library
# end-to-end rather than exhaustively, so the global --cov-fail-under=80 in
# pyproject.toml does not apply to them.  70 leaves headroom while still
# catching a real drop.  Full-suite coverage stays gated at 80.
_INTEGRATION_COV_FLOOR = "70"


@nox.session(python=PYTHON_VERSIONS)
def tests(session: nox.Session) -> None:
    """Run the full test suite with coverage."""
    uv_sync(session, "--extra", "otel", "--no-default-groups", "--group", "test")
    # posargs replace the default path so `nox -s tests-3.12 -- tests/unit`
    # narrows the run instead of silently collecting everything.  Note this
    # session keeps the full-suite 80% gate, so narrowing it will usually trip
    # --cov-fail-under; use tests_unit / tests_integration to select a suite.
    session.run("pytest", *(session.posargs or ["tests/"]), "-v")


@nox.session(python=PYTHON_VERSIONS, default=False)
def tests_unit(session: nox.Session) -> None:
    """Run the fakeredis-backed unit suite.  Needs no Redis server."""
    uv_sync(session, "--extra", "otel", "--no-default-groups", "--group", "test")
    session.run("pytest", "tests/unit", *session.posargs)


# Resumable streams need Redis 8.6.2 or later.  On an older server (the 7.4 CI
# entry) the stream tests skip by design and only the "feature turns itself
# off" test runs, so the stream modules are left out of coverage there rather
# than lowering the floor for every server.  The probe is the SDK's own
# support check, run inside the session's environment.
_STREAMS_PROBE = """
import os
import redis

url = os.environ.get("REDIS_URL") or "redis://{}:{}/0".format(
    os.environ.get("REDIS_HOST", "localhost"), os.environ.get("REDIS_PORT", "6379")
)
try:
    client = redis.Redis.from_url(url)
    client.ping()
except Exception:
    print("unreachable")
    raise SystemExit(0)
try:
    reply = client.execute_command("COMMAND", "INFO", "XCFGSET", "DELEX")
except TypeError:
    print("unsupported")
    raise SystemExit(0)
entries = list(reply.values()) if isinstance(reply, dict) else list(reply)
print("supported" if len(entries) == 2 and all(entries) else "unsupported")
"""
_STREAM_MODULES = "*/redis_fastapi/stream*.py"


@nox.session(python=PYTHON_VERSIONS, default=False)
def tests_integration(session: nox.Session) -> None:
    """Run the integration suite against a real Redis server on localhost:6379.

    Tests are marked ``@requires_redis`` and skip when no server is reachable.
    That cannot pass vacuously: with all of them skipped coverage drops to
    ~35% and the floor below fails the session, so a missing server surfaces
    as a failure rather than a green all-skipped run.

    Against a Redis without resumable-stream support, the stream modules are
    left out of coverage: their tests skip there by design.
    """
    uv_sync(session, "--extra", "otel", "--no-default-groups", "--group", "test")
    support = session.run("python", "-c", _STREAMS_PROBE, silent=True)
    if isinstance(support, str) and support.strip() == "unsupported":
        session.log("Redis lacks stream support; stream modules are out of coverage")
        session.env["COV_OMIT_STREAMS"] = _STREAM_MODULES
    session.run(
        "pytest",
        "tests/integration",
        f"--cov-fail-under={_INTEGRATION_COV_FLOOR}",
        *session.posargs,
    )


# ---------------------------------------------------------------------------
# Documentation
# ---------------------------------------------------------------------------
@nox.session(python=PYTHON_DEFAULT, default=False)
def docs_build(session: nox.Session) -> None:
    """Build the documentation site in strict mode (CI parity)."""
    uv_sync(session, "--only-group", "docs")
    session.run("mkdocs", "build", "--strict", *session.posargs)


@nox.session(python=PYTHON_DEFAULT, default=False)
def docs_serve(session: nox.Session) -> None:
    """Serve the documentation site locally with live reload."""
    uv_sync(session, "--only-group", "docs")
    session.run("mkdocs", "serve", *session.posargs)


# ---------------------------------------------------------------------------
# Auto-fix helpers (not run by default or CI)
# ---------------------------------------------------------------------------
@nox.session(python=PYTHON_DEFAULT, default=False)
def fix(session: nox.Session) -> None:
    """Auto-fix lint and format issues."""
    uv_sync(session, "--only-group", "lint")
    session.run("ruff", "check", "--fix", *SOURCES)
    session.run("ruff", "format", *SOURCES)


@nox.session(python=PYTHON_DEFAULT, default=False)
def streams_clients(session: nox.Session) -> None:
    """Exercise stream replay and live delivery with selected redis-py clients."""
    import os

    version = os.getenv("STREAMS_REDIS_PY_VERSION", "8.0.1")
    if version not in {"8.0.1", "8.1.0"}:
        session.error("STREAMS_REDIS_PY_VERSION must be 8.0.1 or 8.1.0")
    uv_sync(session, "--extra", "otel", "--no-default-groups", "--group", "test")
    session.run_install(
        "uv", "pip", "install", f"redis=={version}",
        env={"VIRTUAL_ENV": session.virtualenv.location},
    )
    session.run(
        "pytest", "tests/integration/test_streams_integration.py",
        "-k", "client_response_modes", "-o", "addopts=", "-q", "-rs",
        *session.posargs,
    )
