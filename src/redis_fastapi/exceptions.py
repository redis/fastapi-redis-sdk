"""Exceptions for the session feature.

A module of their own so that ``session_backend`` can raise them without
importing ``sessions``.  That import used to run the wrong way - the storage
half depended on the request-facing half purely so it could mutate a
``Session`` - and removing it is what turns the two-file split into a real
layering boundary rather than a mutual-friend arrangement.
"""

from __future__ import annotations


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
