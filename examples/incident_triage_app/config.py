"""The demo's limits and clocks, fixed in code (spec, section 8.1)."""

from __future__ import annotations

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

# The stream clocks are REDIS_STREAM_* settings, set in main.py.

# Sessions need Redis 8.0.  Streams detect their own support.
SESSIONS_MIN_VERSION = (8, 0, 0)
