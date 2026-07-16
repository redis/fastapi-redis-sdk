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
        "bandit", "-r", "src/",
        "-f", "json", "-o", "bandit-report.json",
        success_codes=[0, 1],
    )
    session.run("bandit", "-r", "src/")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
@nox.session(python=PYTHON_VERSIONS)
def tests(session: nox.Session) -> None:
    """Run the full test suite with coverage."""
    uv_sync(session, "--extra", "otel", "--no-default-groups", "--group", "test")
    session.run("pytest", "tests/", "-v", *session.posargs)


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
