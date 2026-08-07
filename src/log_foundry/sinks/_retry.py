"""Shared retry waiting for the sinks (SPEC-027)."""

from __future__ import annotations

import math
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import threading

__all__ = ["MAX_WAIT", "clamp_server_delay", "wait"]

MAX_WAIT = 86_400.0
"""Hard ceiling on any single wait, in seconds — a day.

Not a policy, a totality guard. ``time.sleep`` and ``Event.wait`` both raise ``OverflowError``
past the platform's ``time_t``, so ``wait(1e18)`` would raise on the drain thread, which is the
one thing this module exists to prevent.
"""


def wait(delay: float, stop: threading.Event | None = None) -> None:
    """Waits for a delay, returning early if the stop event is set (SPEC-027 FR-002).

    The worker owns a single drain thread by design (arch §9), so a sink's backoff is a global
    pause on log delivery, held across ``shutdown()``, which joins that thread — which is why
    sinks wait on an ``Event`` rather than calling ``time.sleep``. It does not abort an
    in-flight network call, only the pause between attempts: cancelling a socket mid-write is
    not something this library attempts.

    Args:
      delay: Seconds to wait. A finite delay larger than :data:`MAX_WAIT` is capped rather than
        passed through, because both primitives raise ``OverflowError`` past the platform's
        ``time_t`` and "total" has to mean total.
      stop: The worker's shutdown event when the sink was given one, and ``None`` for a sink
        used standalone, which then waits exactly as it did before this spec. With no event
        there is nothing to wait on, so that path falls back to ``time.sleep``.

    Returns:
      None.

    Raises:
      None. A non-positive or non-finite delay returns immediately, so a caller that computed
        one from arithmetic — or from a destination's header — cannot turn a backoff into an
        exception on the drain thread.
    """
    if not (delay > 0) or math.isinf(delay):
        return
    delay = min(delay, MAX_WAIT)
    if stop is None:
        time.sleep(delay)
        return
    stop.wait(delay)


def clamp_server_delay(value: float | None, ceiling: float) -> float | None:
    """Bounds a server-supplied delay, since a destination's advice is not an instruction.

    ``Retry-After`` used to go straight to ``time.sleep`` with no ceiling and no sign check: a
    measured ``Retry-After: 8`` with the default retries blocked ``shutdown()`` for 22 seconds,
    a header of ``86400`` would stall logging for a day, and a negative one makes ``time.sleep``
    raise. Zero is rejected rather than honoured as "retry now", because a destination that is
    rate-limiting has asked us to slow down and a header saying "wait zero seconds" is far more
    likely truncated than meant.

    Args:
      value: The parsed header value, or ``None`` when there was none or it was in HTTP-date
        form the parser declined. ``NaN`` falls out of the same comparison that rejects a
        negative, which is why the test is ``not (value > 0)`` rather than ``value <= 0`` — the
        latter reads False for ``NaN``, letting it through to ``min()``, which returns it.
      ceiling: The caller's ceiling, not a constant: a platform that legitimately asks for a
        two-minute pause should be allowed one, and a caller with an execution deadline should
        be able to lower it. An unusable ceiling is a misconfiguration rather than an
        instruction to stop waiting, and is answered like a missing header — left unchecked,
        ``min(value, 0)`` returns ``0.0``, which the caller reads as an honoured delay and
        retries with no backoff at all, and ``min(value, nan)`` returns the value in full.

    Returns:
      The bounded delay, or ``None`` to mean "fall back to exponential backoff".

    Raises:
      None.
    """
    if value is None or not (value > 0) or math.isinf(value):
        return None
    if not (ceiling > 0):
        return None
    return min(value, ceiling)
