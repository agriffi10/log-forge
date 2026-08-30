"""MultiSink — fan one batch out to several sinks (arch §8, SPEC-006 FR-002)."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from log_foundry import _diag, _lifecycle
from log_foundry.sinks.base import SinkLosses, read_losses

if TYPE_CHECKING:
    from log_foundry.sinks.base import Sink

__all__ = ["MultiSink"]


class MultiSink:
    """A :class:`~log_foundry.sinks.base.Sink` that forwards each batch to every child sink.

    One ``configure(sink=...)`` can echo to stdout and ship to SQS by wrapping both here.
    Children receive the batch sequentially in construction order, and a single child whose
    ``emit`` or ``close`` raises is isolated, so one broken destination never fails the whole
    fan-out or the worker's retry — mirroring the worker's own survive-a-sink-failure boundary
    (arch §9).

    Attributes:
      failed: Count of child ``emit``/``close`` calls that raised. This counts calls, not
        batches: since total failure re-raises (SPEC-017 FR-004) the worker retries the same
        batch, so one batch against an all-down fan-out of *n* children increments this by *n*
        per attempt, with a stderr line each. That is the visible cost of the loss no longer
        being silent.

    It takes **no** transport lock (SPEC-028 FR-002) — it holds no transport, and a lock spanning
    a child's ``emit`` would serialize every destination behind the slowest one. And it
    **adds no post-close guard** (SPEC-032 FR-003), because the post-close rule is each child's:
    ``close()`` here only forwards, so a guard added at this level would refuse batches the
    children would have taken, while a child that must refuse already does and is counted here
    like any other failure.
    """

    def __init__(self, *sinks: Sink) -> None:
        """Binds the fan-out to its children, in the order they will be called.

        Args:
          *sinks: The child sinks.

        Returns:
          None.

        Raises:
          None.
        """
        self._sinks = sinks
        self.failed = 0
        self._silent_failed = 0
        self._counter_lock = threading.Lock()
        self._stop_signal: threading.Event | None = None

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Forwards the batch to every child in construction order, isolating failures (FR-002).

        A failing child that reports **nothing** of its own is counted here, in events, because
        otherwise it is invisible: ``losses()`` sums only children with a ``losses()``, so a
        destination that has delivered nothing since the process started reported zero loss
        forever (SPEC-036 FR-005). Which children are silent is decided by ``read_losses`` per
        call, the same probe the aggregate uses, so a child that gains a ``losses()`` later moves
        categories on its own — at the cost that the aggregate can then *fall*, which is why the
        counter is per child rather than a single total.

        Partial success stays isolated: a retry there would re-deliver the batch to the children
        that already took it, and duplicates are worse than the one failure already counted on
        ``failed`` and written to stderr. Total failure delivered nothing, so it has no
        duplicates to create and is the one case worth retrying. A child's class name goes in
        the diagnostic's detail rather than in the ``where``, because only the detail is bounded
        and a class name is a runtime value (SPEC-029 FR-002).

        Args:
          batch: The events to forward.

        Returns:
          None.

        Raises:
          Exception: The first child's exception when every child failed, so the worker's
            bounded retry sees the total loss (SPEC-017 FR-004). It is raised outside any
            ``except`` block, so ``__context__`` is untouched and it propagates by identity with
            its original traceback. An empty ``MultiSink`` stays a no-op rather than raising,
            which is what stops a misconfigured fan-out retrying every batch to exhaustion.
        """
        first_error: Exception | None = None
        delivered = 0
        for sink in self._sinks:
            try:
                sink.emit(batch)
            except Exception as err:
                silent = read_losses(sink) is None
                with self._counter_lock:
                    self.failed += 1
                    if silent:
                        self._silent_failed += len(batch)
                if first_error is None:
                    first_error = err
                _diag.absorbed(
                    "emitting to a MultiSink child", err, f"{type(sink).__name__} skipped"
                )
            else:
                delivered += 1
        if delivered == 0 and first_error is not None:
            raise first_error

    @property
    def log_foundry_stop_signal(self) -> threading.Event | None:
        """The worker's shutdown event, forwarded to whatever actually holds the retry loop.

        The worker sets this on the configured sink (SPEC-027 FR-002), and a wrapper is not
        where the waiting happens. Without the forward the attribute is set on an object that
        never waits, and the backoff one level down stays uninterruptible — which is the whole
        defect, moved rather than fixed.

        Args:
          None.

        Returns:
          The stop signal, or ``None`` if none was offered.

        Raises:
          None.
        """
        return self._stop_signal

    @log_foundry_stop_signal.setter
    def log_foundry_stop_signal(self, signal: threading.Event | None) -> None:
        """Forwards the stop signal to every child.

        Children are not probed with ``hasattr`` first: setting it on a child that never reads
        it is harmless, and a child that refuses must not stop its siblings from getting theirs.

        Args:
          signal: The worker's shutdown event, or ``None``.

        Returns:
          None.

        Raises:
          None.
        """
        self._stop_signal = signal
        for sink in self._sinks:
            try:
                sink.log_foundry_stop_signal = signal  # type: ignore[attr-defined]
            except Exception as err:
                _diag.absorbed(
                    "handing a MultiSink child its stop signal",
                    err,
                    f"{type(sink).__name__} stays uninterruptible",
                )

    def losses(self) -> SinkLosses | None:
        """Sums the children's losses so a fan-out reports the whole tree (SPEC-026 FR-002).

        Nesting is handled for free, since a child ``MultiSink`` is just another sink with a
        ``losses()``. ~~``MultiSink.failed`` is deliberately absent from the total: it counts
        child calls that raised, not events, so adding it to a per-event figure would produce a
        number with no unit, and the children already report their own loss in events.~~ —
        superseded in part by SPEC-036 FR-005. ``failed`` is still absent, and still for that
        reason. What was wrong was concluding that the fan-out therefore reports nothing: a child
        with no ``losses()`` contributed nothing to the total, so a permanently dead destination
        was invisible with ``health()`` reading all zeros. ``_silent_failed`` is the same loss in
        the **right unit** — events from failing children that report nothing — so a reporting
        child is still left to report itself and nothing is counted twice.

        The figure is a **total**, not a breakdown: it cannot say which child is failing. The
        per-batch stderr line names the class (:meth:`emit`), which is where that lives.

        ``close()`` deliberately does not move ``_silent_failed``. Its failure path has no batch,
        so there is no event count to add, and a ``+1`` there would mix units in exactly the way
        that keeps ``failed`` out of this sum. A client buffer lost to a failed close is of
        unknown size, and an invented number is worse than an absent one.

        Args:
          None.

        Returns:
          The summed losses, or ``None`` when no child reported anything **and** no silent child
          has lost a batch. FR-003 separates "reports nothing" from "reports no loss", and a tree
          of silent children that has lost nothing has still not given a clean bill of health —
          but one that *has* lost something now says so, which is the whole of FR-005.

        Raises:
          None. A child without ``losses()`` contributes zero, and a child whose accessor raises
            or returns the wrong shape is skipped rather than allowed to take the aggregate —
            and ``health()`` with it — down.
        """
        children = [read_losses(sink) for sink in self._sinks]
        reported = [child for child in children if child is not None]
        with self._counter_lock:
            silent_failed = self._silent_failed
        if not reported and not silent_failed:
            return None
        return SinkLosses(
            dropped=sum(child.dropped for child in reported),
            failed=sum(child.failed for child in reported) + silent_failed,
        )

    def close(self) -> None:
        """Closes every child, isolating a failing one so the rest still close (FR-002).

        Unlike :meth:`emit` this keeps the unconditional isolate-and-continue behaviour even on
        total failure, because a failed close has nothing to retry. Each child goes through
        ``_lifecycle.release`` rather than being closed here, so a child this process may not
        release is refused by the same guard the lifecycle's own closers consult (SPEC-042
        FR-002) — the wrapper route is how a forked child reached a parent's sink with all three
        lifecycle sites already guarded.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        for sink in self._sinks:
            try:
                _lifecycle.release(sink, owner=self)
            except Exception as err:
                with self._counter_lock:
                    self.failed += 1
                _diag.absorbed("closing a MultiSink child", err, f"{type(sink).__name__} skipped")
