"""The demo's limits and clocks, fixed in code (spec, section 8.1)."""

from __future__ import annotations

from examples.incident_triage_app.streams_stub import StreamSettings

# Sessions: short clocks, so expiry is visible in a demo.
SESSION_IDLE_TTL = 300
SESSION_ABSOLUTE_TTL = 1800

# The run quota, per engineer.  The scope names the counter, so the lens can
# peek at the same one the dependency counts.
RUN_LIMIT = 5
RUN_WINDOW = 60
RUN_SCOPE = "triage-runs"

# The daily model budget, shared by everyone.
MODEL_LIMIT = 500
MODEL_WINDOW = 86_400
MODEL_SCOPE = "model-runs"

# Keeps the app under the free database's 100 operations a second.
GLOBAL_RATE = "1200/minute"

# Short heartbeat and alive clocks, so a lost producer shows within seconds.
# A 60-second connection cap, so a revoked phone loses its stream within a
# minute even without the page's poll.
STREAMS = StreamSettings(
    ttl_active=600,
    ttl_done=120,
    maxlen=500,
    heartbeat_seconds=5,
    alive_ttl_seconds=15,
    max_readers=8,
    max_connection_seconds=60,
)

# Sessions need Redis 8.0.  Streams detect their own support (streams_stub).
SESSIONS_MIN_VERSION = (8, 0, 0)
