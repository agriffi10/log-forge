"""Sink-lifecycle facilities shared by both delivery paths (SPEC-033 FR-005)."""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING

from log_foundry import _diag

if TYPE_CHECKING:
    from log_foundry.sinks.base import Sink

DEFAULT_CLOSER_GRACE = 2.0
"""Seconds a shutdown gives an outstanding swapped-out close to finish.

Deliberately much smaller than the shutdown budget it is carved from. This is a last chance for
a close that is *nearly* done, not a second full attempt: it already had the swap's whole budget
(``DEFAULT_SWAP_TIMEOUT``) before ``shutdown`` was ever called, so one still running here is far
more likely stuck than slow, and every second spent on it is a second the process does not exit.
"""

_closers: list[threading.Thread] = []
_closers_lock = threading.Lock()


def release(sink: Sink, *, detached: bool = False) -> threading.Thread | None:
    """Closes a sink on the library's behalf — the one path by which it ever does (SPEC-042 FR-002).

    Eight sites closed a sink directly before this existed, and guarding only the three the
    lifecycle owns was measurably insufficient: a forked child that wraps an **inherited** sink in
    a ``MultiSink`` of its own reaches the inner sink through the wrapper, so the parent's
    structural sink was closed twice with all three lifecycle sites guarded. Routing every
    library closer through one function is what gives the ownership question one home.

    **The guard moves here; the error handling does not.** This propagates whatever ``close()``
    raises, because the callers do not agree today and must not be made to: four absorb, under
    three distinct ``_diag`` texts naming the site (SPEC-029), ``MultiSink`` also increments its
    ``failed`` counter, and ``FilteringSink``/``TransformSink`` propagate under a documented
    ``Raises:``. Folding the ``try/except`` in here would drop absorbed close failures out of
    ``Health.sink.failed`` — a SPEC-026 regression — and falsify two of those clauses.

    Args:
      sink: The sink to close.
      detached: Whether to close on a daemon thread rather than inline. Detached is for a sink
        the caller has stopped delivering to and must not block on (SPEC-030 FR-003).

    Returns:
      The started closer thread for a detached release, or ``None`` — both when an inline close
      completed and when the platform would not give the process another thread.

    Raises:
      Exception: Whatever an inline ``close()`` raised. A detached release raises nothing: the
        thread body absorbs, since there is no caller left to hand it to.
    """
    if detached:
        return _start_closer(sink)
    sink.close()
    return None


def _start_closer(sink: Sink) -> threading.Thread | None:
    """Starts a daemon close of a sink no longer being delivered to (SPEC-030 FR-003).

    The thread is returned rather than joined, so a caller holding a lock can start under it and
    wait after releasing it — ``decorator._swap_sink`` mutates its records under the process-wide
    ``_worker_lock`` and must not hold that across a wait of the swap's whole budget (SPEC-033
    FR-002). Callers that hold no lock join it immediately and are equivalent to the single call
    this replaced.

    The thread is a **daemon**, and it is :func:`join_closers` that makes that safe rather than
    merely available. A non-daemon thread was tried and is worse on its own: CPython joins
    non-daemon threads *before* running ``atexit``, so one hung close stops the exit drain from
    ever running and loses everything buffered in the **live** sink. A daemon alone is worse in
    the opposite case: a close that is slow but *succeeding* is killed at exit, losing whatever
    it was flushing.

    Args:
      sink: The sink that was swapped out.

    Returns:
      The started thread, or ``None`` when the platform would not give the process another one.

    Raises:
      None. ``Thread.start`` raises when the process is out of threads, and a swap that cannot
        spawn one must leave the sink open and say so rather than fall back to an inline close —
        the fallback would reintroduce the unbounded wait this exists to remove, in the one
        situation where the process is already under resource pressure.
    """
    closer = threading.Thread(
        target=_close_guarded,
        args=(sink,),
        name="log-foundry-sink-close",
        daemon=True,
    )
    try:
        closer.start()
    except Exception as exc:
        _diag.absorbed(
            "starting the thread that closes a swapped-out sink",
            exc,
            "it is left open and may still hold its resources",
        )
        return None
    with _closers_lock:
        _closers[:] = [old for old in _closers if old.is_alive()]
        _closers.append(closer)
    return closer


def _close_guarded(sink: Sink) -> None:
    """Closes a swapped-out sink on its own thread, absorbing a failure.

    The guard is what makes the thread safe to leave unattended: an exception escaping here
    would reach CPython's thread bootstrap, which prints a full traceback carrying the
    exception's message — the user data arch §6 keeps out of anything the library says about
    itself. It goes back through :func:`release` rather than calling ``close()`` itself, so the
    thread body is one of the eight callers rather than a ninth close (SPEC-042 FR-002).

    Args:
      sink: The sink to close.

    Returns:
      None.

    Raises:
      None.
    """
    try:
        release(sink)
    except Exception as exc:
        _diag.absorbed("closing a swapped-out sink", exc, "it may still hold its resources")


def join_closers(timeout: float | None) -> None:
    """Gives outstanding swapped-out closes their last chance before the process exits.

    **The cap is the mechanism.** The wait is the smaller of :data:`DEFAULT_CLOSER_GRACE` and
    what remains of the shutdown's own budget: capped so a stuck close cannot hold a process at
    exit for the whole shutdown budget, and carved from that budget so it cannot extend it either.

    The registry is process-global rather than per-worker because a close started before any
    worker existed must still be counted and still be granted this grace (SPEC-033 FR-005).

    Args:
      timeout: Seconds remaining in the shutdown's budget, further capped by
        :data:`DEFAULT_CLOSER_GRACE` and shared across every outstanding close. ``None`` takes
        the cap rather than waiting indefinitely — an unbounded shutdown is a caller's choice
        about draining events, not a licence for a stuck close to hold the exit.

    Returns:
      None.

    Raises:
      None. A join on a thread that has already finished is a no-op, and one that has not is
        abandoned at the deadline — which is the daemon's contract, not a failure.
    """
    with _closers_lock:
        closers = [closer for closer in _closers if closer.is_alive()]
        _closers[:] = closers
    grace = DEFAULT_CLOSER_GRACE if timeout is None else min(timeout, DEFAULT_CLOSER_GRACE)
    deadline = time.monotonic() + grace
    for closer in closers:
        closer.join(max(0.0, deadline - time.monotonic()))


def closing_count() -> int:
    """Counts the swapped-out closes running at this instant, backing ``Health.closing_sinks``.

    A live fact rather than an inference from a timeout: an expired join reports nothing, since
    a slow close and a stuck one cannot be told apart at that moment, so this gauge is what an
    operator reads instead. It falls as well as rises.

    Args:
      None.

    Returns:
      The number of closer threads still alive.

    Raises:
      None.
    """
    with _closers_lock:
        _closers[:] = [closer for closer in _closers if closer.is_alive()]
        return len(_closers)


def offer_stop_signal(sink: Sink, stop: threading.Event) -> None:
    """Gives a sink an interruptible-wait signal, if it advertises somewhere to put one.

    The dependency stays one-way (SPEC-027 FR-002): ``sinks`` must not import ``worker``, so the
    holder of the event pushes rather than the sink pulling. It is probed with ``hasattr``, the
    same optional-protocol shape SPEC-026 uses for ``losses()`` — a sink without the attribute
    simply never gets one and backs off uninterruptibly, exactly as before.

    Args:
      sink: The sink to offer the signal to.
      stop: The event that is set when delivery should stop waiting.

    Returns:
      None.

    Raises:
      None. A sink whose ``log_foundry_stop_signal`` is a read-only property, or whose
      ``__setattr__``
        objects, loses interruptibility rather than preventing the caller from proceeding.
    """
    try:
        if hasattr(sink, "log_foundry_stop_signal"):
            sink.log_foundry_stop_signal = stop
    except Exception as exc:
        _diag.absorbed("handing the sink its stop signal", exc, "its backoff stays uninterruptible")
