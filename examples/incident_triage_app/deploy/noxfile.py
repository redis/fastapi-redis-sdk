"""Deploy the incident triage demo to FastAPI Cloud, by hand.

Kept apart from the root noxfile: it is not a check, and CI never runs it.
From the repository root::

    export FASTAPI_CLOUD_TOKEN=<deploy token>
    export FASTAPI_CLOUD_APP_ID=<app UUID>
    uv run nox -f examples/incident_triage_app/deploy/noxfile.py

See README.md in this folder.
"""

from __future__ import annotations

import os
import tempfile
import uuid
from pathlib import Path

import nox

# An absolute path, so the session works from any directory.
BUNDLE = Path(__file__).with_name("bundle.py")


@nox.session(python=False)
def deploy(session: nox.Session) -> None:
    """Build the deploy folder, check that the app imports, and deploy it.

    Reads the deploy token from ``FASTAPI_CLOUD_TOKEN`` and the app's UUID
    from ``FASTAPI_CLOUD_APP_ID``.  Never put either in a file: the repository
    is public.
    """
    if not os.environ.get("FASTAPI_CLOUD_TOKEN"):
        session.error("Set FASTAPI_CLOUD_TOKEN to a FastAPI Cloud deploy token")
    app_id = os.environ.get("FASTAPI_CLOUD_APP_ID", "")
    try:
        uuid.UUID(app_id)
    except ValueError:
        # The CLI takes the UUID only, and a deploy token cannot look it up.
        session.error(
            "Set FASTAPI_CLOUD_APP_ID to the app's UUID, shown next to its "
            f"name in the dashboard header (got {app_id!r})"
        )
    out = Path(tempfile.mkdtemp(prefix="incident-triage-")) / "app"
    session.run("uv", "run", str(BUNDLE), str(out), "--check", external=True)
    session.run(
        "uvx",
        "--from",
        "fastapi[standard]>=0.138",
        "fastapi",
        "deploy",
        str(out),
        "--app-id",
        app_id,
        external=True,
    )
