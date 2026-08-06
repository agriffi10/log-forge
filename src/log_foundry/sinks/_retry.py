"""Shared retry waiting for the sinks (SPEC-027).

Every sink that retries does so by sleeping the one thread that delivers anything. The worker
owns a single drain thread by design (arch §9), so a sink's backoff is not a local decision — it
is a global pause on log delivery, and it is held across ``shutdown()``, which joins that thread.

Two rules follow, and this module is where both are applied once rather than at fifteen call
sites:

* **A wait is interruptible.** Sinks wait on an ``Event`` rather than calling ``time.sleep``, so a
  shutdown cuts an in-progress backoff short instead of holding the drain thread for its full
  delay. The worker's own backoff already did this (``worker.py``'s ``_stop.wait``); the sinks had
  no access to the signal.
* **A server-supplied delay is advice, not an instruction.** ``Retry-After`` arrives from the
  destination and went straight to ``time.sleep`` with no ceiling and no sign check: a measured
  ``Retry-After: 8`` with the default ``max_retries=3`` blocked ``shutdown()`` for 22 seconds, a
  header of ``86400`` would stall logging for a day, and a negative one makes ``time.sleep``
  raise — absorbed inside a span, but reaching the caller on the orphan path.

This module imports nothing from its own package (``_diag``'s rule, for the same reason: it sits
below everything that might want it).
"""

from __future__ import annotations

import math
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import threading

__all__ = ["clamp_server_delay", "wait"]


def wait(delay: float, stop: threading.Event | None = None) -> None:
    """Wait ``delay`` seconds, returning early if ``stop`` is set (SPEC-027 FR-002).

    Total. A non-positive or non-finite delay returns immediately rather than raising, so a
    caller that computed one from arithmetic — or from a destination's header — cannot turn a
    backoff into an exception on the drain thread.

    ``stop`` is the worker's shutdown event when the sink was given one, and ``None`` for a sink
    used standalone, which then waits exactly as it did before this spec. ``Event.wait`` is used
    rather than ``time.sleep`` in both cases: with no event there is nothing to wait on, so the
    ``None`` path falls back to ``time.sleep``.

    It does **not** abort an in-flight network call — only the pause between attempts. Cancelling
    a socket mid-write is not something this library attempts.
    """
    if not (delay > 0) or math.isinf(delay):
        return
    if stop is None:
        time.sleep(delay)
        return
    stop.wait(delay)


def clamp_server_delay(value: float | None, ceiling: float) -> float | None:
    """Bound a server-supplied delay; ``None`` means "fall back to exponential backoff".

    ``None`` in and ``None`` out is the ordinary case: no ``Retry-After``, or one in HTTP-date
    form that the parser declined. Everything else is validated against what a *delay* can be —
    finite, positive, and no longer than the caller's ceiling.

    Zero is rejected rather than honoured as "retry now". A destination that is rate-limiting has
    asked us to slow down, and a header saying "wait zero seconds" is far more likely a broken or
    truncated value than a real instruction to hammer it; the exponential backoff is the safer
    reading. ``NaN`` falls out of the same comparison that rejects a negative: every comparison
    against ``NaN`` is ``False``, which is why the test is written ``not (value > 0)`` rather than
    ``value <= 0`` — the latter reads ``False`` for ``NaN``, letting it through to ``min()``,
    which returns it.

    The ceiling is the caller's, not a constant: a platform that legitimately asks for a
    two-minute pause should be allowed one, and a caller with an execution deadline should be able
    to lower it below the default.
    """
    if value is None or not (value > 0) or math.isinf(value):
        return None
    return min(value, ceiling)
