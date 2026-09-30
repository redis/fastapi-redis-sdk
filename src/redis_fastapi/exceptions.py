"""Exceptions for the session feature.

A module of their own so that ``session_backend`` can raise them without
importing ``sessions``.  That import used to run the wrong way - the storage
half depended on the request-facing half purely so it could mutate a
``Session`` - and removing it is what turns the two-file split into a real
layering boundary rather than a mutual-friend arrangement.
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
