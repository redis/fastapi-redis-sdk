"""Exceptions for the session and resumable-stream features.

A module of their own so that ``session_backend`` can raise them without
importing ``sessions``.  That import used to run the wrong way - the storage
half depended on the request-facing half purely so it could mutate a
``Session`` - and removing it is what turns the two-file split into a real
layering boundary rather than a mutual-friend arrangement.  The stream errors
live here for the same reason: ``stream_backend`` raises them, and ``streams``
maps them to HTTP status codes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from starlette.responses import Response


class SessionError(Exception):
    """Base for every error this feature raises.

    Catching this catches the whole feature.  A driver exception never reaches
    application code - the store wraps it in :class:`SessionStoreError`.
    """


class SessionConfigurationError(SessionError):
    """A session setting is missing, invalid, or contradicts another one."""


class SessionStoreError(SessionError):
    """The store could not complete an operation.

    Raised for every failed **write**, whatever ``session_fail_closed`` is set
    to, because losing a login or a rotation must never be silent.  Failed
    reads raise this only when ``session_fail_closed`` is set; otherwise they
    yield an empty session.  Section 7 of the design explains the asymmetry.

    The one read that never fails open is ``count_for_subject``: there the
    store *is* the authorization answer, and a permissive default would wave
    logins past a concurrent-session cap exactly when Redis is unhealthy.
    """


class SessionRejected(Exception):
    """Carries the rejection response out of ``valid_session()``.

    Intentional control flow, not an error, so it subclasses ``Exception`` and
    not :class:`SessionError`: a handler that catches ``SessionError`` to
    report store failures must not catch a rejected request.  The handler
    ``add_redis_sessions`` registers returns the carried response.
    """

    def __init__(self, response: Response) -> None:
        super().__init__()
        self.response = response
        self.__suppress_context__ = True


class StreamError(Exception):
    """Base for every error the resumable-stream feature raises.

    Catching this catches the whole feature.  The SSE handlers that
    ``FastAPIRedis.streams()`` registers map each subclass to a status code.
    """


class StreamNotFound(StreamError):
    """No stream under this scope and ID.  Answered with 404.

    An ID from another scope names a key that does not exist, so it lands here
    too, and a stream ID reveals nothing about streams in other scopes.
    """


class StreamExpired(StreamError):
    """The stream is gone, or entries after the cursor were trimmed.  410."""


class StreamBusy(StreamError):
    """The scope already has an active run.  Answered with 409.

    Attributes:
        stream_id: The ID of the run that holds the scope.
    """

    def __init__(self, stream_id: str) -> None:
        super().__init__(f"the scope already has an active run: {stream_id}")
        self.stream_id = stream_id


class StreamCursorAhead(StreamError):
    """The cursor is past the stream's last generated ID.  Answered with 409.

    The cursor came from another stream, from a client bug, or from a failover
    that lost writes the client had already read.
    """


class StreamInvalidCursor(StreamError, ValueError):
    """A malformed, out-of-range or repeated cursor.  Answered with 422."""


class StreamCredentialExpired(StreamError):
    """``expires_at`` had already passed when the stream was requested.  401."""


class StreamUnsupported(StreamError):
    """The Redis server lacks the commands this feature needs.  503.

    Detected once per pool; see :func:`~redis_fastapi.stream_backend.probe_streams_support`.
    """


class StreamFinished(StreamError):
    """A write to a stream that already has its final entry."""


class StreamFenced(StreamError):
    """Another writer took over this stream with ``attach()``.

    The writer stops.  At most the append that reported the fence landed after
    the new writer started.
    """


class StreamStoreError(StreamError):
    """Redis could not complete a stream operation.  Answered with 503."""


class StreamConfigurationError(StreamError):
    """A stream setting is invalid, or the feature was not set up."""
