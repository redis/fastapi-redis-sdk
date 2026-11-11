# /// script
# requires-python = ">=3.11"
# ///
"""Build the folder that FastAPI Cloud deploys for the incident triage demo.

FastAPI Cloud installs what the ``pyproject.toml`` of the deployed folder
lists.  The repository root's ``pyproject.toml`` describes the SDK, and the
demo needs more than the SDK does: ``httpx``, ``anthropic`` and a FastAPI with
``app.frontend()``.  So this script writes a separate, self-contained folder:

    <out>/
      pyproject.toml          the SDK's dependencies plus the demo's
      .fastapicloudignore     keeps a local check's .venv out of the upload
      src/redis_fastapi/      the SDK at this commit
      examples/incident_triage_app/

``main.py`` puts ``src/`` on ``sys.path``, so the SDK runs from this commit and
needs no release.  Deploy the folder with ``fastapi deploy <out>``.

Usage::

    uv run examples/incident_triage_app/deploy/bundle.py <out> [--check]

``--check`` installs the folder's dependencies and imports the app, as the
build on FastAPI Cloud will.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import tomllib

ROOT = Path(__file__).resolve().parents[3]
APP = ROOT / "examples" / "incident_triage_app"
ENTRYPOINT = "examples.incident_triage_app.main:app"

# What the demo needs beyond the SDK.  app.frontend() arrived in FastAPI 0.138.
DEMO_DEPENDENCIES = [
    "fastapi[standard]>=0.138",
    "httpx>=0.27",
    "anthropic>=1.15,<2",
]
# Pinned, as FastAPI Cloud advises for an application: it otherwise uses the
# newest Python, before every dependency has wheels for it.
PYTHON = "==3.13.*"

MARKER = ".triage-bundle"


def _commit() -> str | None:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return out.stdout.strip()


def _toml_list(items: list[str]) -> str:
    return "[\n" + "".join(f"    {json.dumps(item)},\n" for item in items) + "]"


def build(out: Path) -> None:
    if out.exists():
        if any(out.iterdir()) and not (out / MARKER).exists():
            sys.exit(f"{out} is not empty and was not made by this script")
        shutil.rmtree(out)

    project = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    ignore = shutil.ignore_patterns("__pycache__", "*.pyc", "deploy")

    shutil.copytree(
        ROOT / "src" / "redis_fastapi", out / "src" / "redis_fastapi", ignore=ignore
    )
    (out / "examples").mkdir()
    shutil.copy2(ROOT / "examples" / "__init__.py", out / "examples" / "__init__.py")
    shutil.copytree(APP, out / "examples" / "incident_triage_app", ignore=ignore)

    (out / "examples" / "incident_triage_app" / "build.json").write_text(
        json.dumps(
            {
                "commit": _commit(),
                "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
        )
        + "\n"
    )
    (out / "pyproject.toml").write_text(
        f"""\
# Written by examples/incident_triage_app/deploy/bundle.py.  Do not edit.
[project]
name = "incident-triage-demo"
version = {json.dumps(project["version"])}
requires-python = {json.dumps(PYTHON)}
dependencies = {_toml_list(project["dependencies"] + DEMO_DEPENDENCIES)}

[tool.fastapi]
entrypoint = {json.dumps(ENTRYPOINT)}
"""
    )
    (out / ".fastapicloudignore").write_text(".venv/\n__pycache__/\n")
    (out / MARKER).write_text("")


def check(out: Path) -> None:
    """Install the dependencies and import the app, as the cloud build will."""
    subprocess.run(
        [
            "uv",
            "run",
            "--directory",
            str(out),
            "python",
            "-c",
            f"from {ENTRYPOINT.replace(':', ' import ')}; print('ok:', app.title)",
        ],
        check=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("out", type=Path, help="the folder to write")
    parser.add_argument(
        "--check", action="store_true", help="install and import the app"
    )
    args = parser.parse_args()
    out = args.out.resolve()
    if out == ROOT or ROOT in out.parents:
        # Inside the repository, the upload would follow its .gitignore, which
        # may leave out the very folder being deployed.
        sys.exit("write the folder outside the repository")
    build(out)
    print(f"wrote {out}")
    if args.check:
        check(out)


if __name__ == "__main__":
    main()
