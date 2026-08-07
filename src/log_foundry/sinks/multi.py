"""MultiSink — fan one batch out to several sinks (arch §8, SPEC-006 FR-002)."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from log_foundry import _diag
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
        self._counter_lock = threading.Lock()
        self._stop_signal: threading.Event | None = None

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Forwards the batch to every child in construction order, isolating failures (FR-002).

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
                with self._counter_lock:
                    self.failed += 1
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
    def stop_signal(self) -> threading.Event | None:
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

    @stop_signal.setter
    def stop_signal(self, signal: threading.Event | None) -> None:
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
                sink.stop_signal = signal  # type: ignore[attr-defined]
            except Exception as err:
                _diag.absorbed(
                    "handing a MultiSink child its stop signal",
                    err,
                    f"{type(sink).__name__} stays uninterruptible",
                )

    def losses(self) -> SinkLosses | None:
        """Sums the children's losses so a fan-out reports the whole tree (SPEC-026 FR-002).

        Nesting is handled for free, since a child ``MultiSink`` is just another sink with a
        ``losses()``. ``MultiSink.failed`` is deliberately absent from the total: it counts
        child calls that raised, not events, so adding it to a per-event figure would produce a
        number with no unit, and the children already report their own loss in events.

        Args:
          None.

        Returns:
          The summed losses, or ``None`` when no child reported anything, including an empty
          fan-out. FR-003 separates "reports nothing" from "reports no loss", and a tree of
          silent children has not given a clean bill of health; one reporting child is enough to
          make the total meaningful, since the silent ones contribute zero.

        Raises:
          None. A child without ``losses()`` contributes zero, and a child whose accessor raises
            or returns the wrong shape is skipped rather than allowed to take the aggregate —
            and ``health()`` with it — down.
        """
        children = [read_losses(sink) for sink in self._sinks]
        reported = [child for child in children if child is not None]
        if not reported:
            return None
        return SinkLosses(
            dropped=sum(child.dropped for child in reported),
            failed=sum(child.failed for child in reported),
        )

    def close(self) -> None:
        """Closes every child, isolating a failing one so the rest still close (FR-002).

        Unlike :meth:`emit` this keeps the unconditional isolate-and-continue behaviour even on
        total failure, because a failed close has nothing to retry.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        for sink in self._sinks:
            try:
                sink.close()
            except Exception as err:
                with self._counter_lock:
                    self.failed += 1
                _diag.absorbed("closing a MultiSink child", err, f"{type(sink).__name__} skipped")
