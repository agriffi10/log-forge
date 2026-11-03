"""Shared bounding of the sinks' waits and timeouts (SPEC-027, SPEC-047)."""

from __future__ import annotations

import math
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import threading

__all__ = ["MAX_WAIT", "clamp_server_delay", "usable_timeout", "wait"]

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


def _exact_number(value: float) -> float:
    """Returns an ``int`` or ``float`` instance as the exact builtin it holds (SPEC-065 FR-004).

    ``int.__index__`` and ``float.__float__`` are the unbound builtin accessors: each reads the
    number from the instance's own storage and ignores anything a subclass overrides —
    ``__index__``, ``__int__``, ``__float__`` and every comparison. The validators here used to
    test the caller's object itself, so a subclass whose ``__le__`` or ``__gt__`` lied passed the
    test holding a zero; measured at ``71e42dd``, ``MemorySink(maxlen=LeSpoof(0))`` kept nothing
    and ``flush()`` reported ``ok=True``. A ``bool`` is an ``int`` and takes the ``int`` branch,
    so ``True`` becomes ``1``. Anything that is neither is returned unchanged, so a caller that
    has already refused a non-number never sees this branch and one that has not keeps the
    comparison's own ``TypeError``, which :func:`usable_timeout` documents as unchanged.

    Args:
      value: The caller's number, possibly a subclass instance.

    Returns:
      The exact ``int`` or ``float`` the value holds, or the value itself when it is neither.

    Raises:
      None.
    """
    if isinstance(value, int):
        return int.__index__(value)
    if isinstance(value, float):
        return float.__float__(value)
    return value


def usable_timeout(value: float, default: float) -> float:
    """Returns a timeout that actually bounds something, or the caller's default (SPEC-047).

    ``0`` switches a bounded wait off entirely and ``inf`` restores the unbounded one the bound
    exists to remove, so neither is accepted. The test is ``not (0 < value < inf)`` rather than a
    pair of comparisons, because ``NaN`` compares ``False`` to everything and would otherwise slip
    through — :func:`clamp_server_delay`'s reasoning for ``Retry-After``, applied to a value the
    caller supplies rather than one a destination does.

    The test runs on the exact builtin (SPEC-065 FR-004), through :func:`_exact_number`, so an
    ``int`` or ``float`` subclass is judged by the number it holds rather than by what its
    comparisons say, and what is returned is that exact ``int`` or ``float``. A non-number is
    passed to the comparison unchanged and raises its own ``TypeError``, as before.

    Args:
      value: The caller's timeout.
      default: The fallback, passed in rather than named here because each sink's default is its
        own — ``KafkaSink``'s flush and ``NATSSink``'s publish budget are unrelated numbers, and a
        constant baked in here would silently give one sink the other's.

    Returns:
      The value as the exact ``int`` or ``float`` it holds if it bounds anything, else the
      default.

    Raises:
      None.
    """
    exact = _exact_number(value)
    return exact if 0 < exact < float("inf") else default


def require_timeout(value: float, name: str, owner: str) -> float:
    """Returns a timeout that bounds something, or raises (SPEC-049 FR-001).

    **The raising sibling of :func:`usable_timeout`, and the discriminator between them is one
    rule.** The library *floors* a value that works today and *refuses* one that is already
    broken; a newly added argument is refused too, having no working configuration to protect.
    So ``KafkaSink(flush_timeout=-1)`` and ``NATSSink(publish_timeout=-1)`` still fall back to
    their module defaults through ``usable_timeout`` — those configurations deliver events — while
    ``HTTPSink(timeout=-1)``, which raised an uncounted ``ValueError`` out of every batch for the
    life of the process, is refused here. Two functions, one rule, sorted by whether the caller
    has something to lose.

    The test is ``not (0 < value < inf)``, identical to ``usable_timeout``'s and for its reason:
    ``NaN`` compares ``False`` to everything, so the obvious pair of comparisons lets it through.
    It runs on the exact builtin (SPEC-065 FR-004), through :func:`_exact_number` after the
    ``isinstance`` check, so an ``int`` or ``float`` subclass whose comparisons or ``__float__``
    are overridden to look positive is judged by the number it holds, and the message names that
    number. What is returned is that exact ``int`` or ``float``; ``True`` is returned as ``1``.

    Args:
      value: The caller's timeout, an ``int`` or a ``float``.
      name: The argument's name, for the message.
      owner: The class or function refusing it, for the message.

    Returns:
      The timeout as the exact ``int`` or ``float`` it holds, when it bounds something.

    Raises:
      TypeError: When the value is not a number — ``None`` was a runtime-working, ``mypy``-invalid
        0.x spelling of "no bound", and the bare comparison would have raised an unnamed
        ``TypeError`` from inside ``<`` (SPEC-049, system-frame review).
      ValueError: When the exact value is zero, negative, infinite or ``NaN``.
    """
    if not isinstance(value, int | float):
        raise TypeError(f"{owner} {name} must be a number of seconds, not {type(value).__name__}")
    exact = _exact_number(value)
    if not 0 < exact < float("inf"):
        raise ValueError(
            f"{owner} {name} must be a positive, finite number of seconds, not {exact!r}"
        )
    return exact


def require_positive(value: int, name: str, owner: str) -> int:
    """Returns a count that can do work, or raises (SPEC-049 FR-002).

    :func:`require_timeout`'s rule applied to a count rather than a duration: refused because a
    non-positive chunk size is already broken, not merely odd. ``ClickHouseSink(chunk_size=-5)``
    made the chunker yield nothing, so ``emit`` returned having inserted no rows with ``losses()``
    at zero — silent total loss — and ``chunk_size=0`` raised out of ``range()`` on every batch.

    The test runs on the exact ``int`` (SPEC-065 FR-004): ``int.__index__(value)`` reads the
    integer from the instance's own storage, so an ``int`` subclass whose ``__le__`` lies is
    judged by the number it holds — a ``LeSpoof(0)`` used to pass and ``MemorySink`` kept
    nothing — and the message names that number. The exact ``int`` is what is returned, so a
    caller that uses the return in ``range`` or arithmetic never sees the subclass's operators;
    ``True`` is returned as ``1``. This is the sink-side twin of ``config._require_positive``
    (invariant 6): both doors give the same verdict on the same subclass.

    Args:
      value: The caller's count, an ``int``.
      name: The argument's name, for the message.
      owner: The class refusing it, for the message.

    Returns:
      The count as the exact ``int`` it holds, when it is positive.

    Raises:
      TypeError: When the value is not an integer, so the argument is named rather than the
        comparison raising an unnamed one.
      ValueError: When the exact value is zero or negative.
    """
    if not isinstance(value, int):
        raise TypeError(f"{owner} {name} must be an integer, not {type(value).__name__}")
    exact = int.__index__(value)
    if exact <= 0:
        raise ValueError(f"{owner} {name} must be a positive integer, not {exact!r}")
    return exact
