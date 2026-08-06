"""MultiSink — fan one batch out to several sinks (arch §8, SPEC-006 FR-002).

One ``configure(sink=...)`` can echo to stdout *and* ship to SQS by wrapping both in a
``MultiSink``. Children receive the batch sequentially in construction order; a single child
whose ``emit`` (or ``close``) raises is isolated — the failure is counted on ``failed`` and
logged to stderr, and its siblings still run — so one broken destination never fails the whole
fan-out or the worker's retry. This mirrors the worker's own survive-a-sink-failure isolation
boundary (arch §9; best-practices §7 sanctions the broad catch here).

The one exception is **total** failure: if every child raised, ``emit`` re-raises rather than
reporting success, so the worker's retry engages (SPEC-017 FR-004). Swallowing there meant a
``MultiSink`` whose destinations were all down reported healthy on every batch and the loss was
invisible. ``close()`` keeps the unconditional isolate-and-continue behaviour — a failed close
has nothing to retry.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from log_foundry import _diag
from log_foundry.sinks.base import SinkLosses, read_losses

if TYPE_CHECKING:
    import threading

    from log_foundry.sinks.base import Sink

__all__ = ["MultiSink"]


class MultiSink:
    """A :class:`~log_foundry.sinks.base.Sink` that forwards each batch to every child sink.

    Attributes:
        failed: Count of child ``emit``/``close`` **calls** that raised. Note this counts calls,
            not batches: since total failure re-raises (SPEC-017 FR-004), the worker retries the
            same batch, so one batch against an all-down fan-out of *n* children increments this
            by *n* per attempt — ``n * (max_retries + 1)`` in total, with a stderr line each.
            That is the visible cost of the loss no longer being silent.
    """

    def __init__(self, *sinks: Sink) -> None:
        self._sinks = sinks
        self.failed = 0
        self._stop_signal: threading.Event | None = None

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Forward ``batch`` to every child in construction order, isolating failures (FR-002).

        When **every** child failed, re-raise the first child's exception so the worker's bounded
        retry sees the total loss (SPEC-017 FR-004). Partial success stays isolated: a retry there
        would re-deliver the batch to the children that already took it, and duplicates are worse
        than the one failure already counted on ``failed`` and written to stderr. Total failure
        delivered nothing, so it has no duplicates to create and is the one case worth retrying.
        """
        first_error: Exception | None = None
        delivered = 0
        for sink in self._sinks:
            try:
                sink.emit(batch)
            except Exception as err:  # isolation boundary: one child must not fail the rest
                self.failed += 1
                if first_error is None:
                    first_error = err
                # The class name goes in the *detail*, not in ``where``: only the detail is
                # bounded, and a child's ``__name__`` is a runtime value (SPEC-029 FR-002).
                _diag.absorbed(
                    "emitting to a MultiSink child", err, f"{type(sink).__name__} skipped"
                )
            else:
                delivered += 1
        if delivered == 0 and first_error is not None:
            # Raised outside any ``except`` block, so ``__context__`` is untouched and the
            # exception propagates by identity with its original traceback. The ``is not None``
            # is also what keeps an empty ``MultiSink`` a no-op rather than a raise — otherwise a
            # misconfigured fan-out with no children would retry every batch to exhaustion.
            raise first_error

    @property
    def stop_signal(self) -> threading.Event | None:
        """The worker's shutdown event, forwarded to whatever actually holds the retry loop.

        The worker sets this on the *configured* sink (SPEC-027 FR-002), and a wrapper is not
        where the waiting happens. Without the forward the attribute is set on an object that
        never waits, and the backoff one level down stays uninterruptible — which is the whole
        defect, moved rather than fixed.
        """
        return self._stop_signal

    @stop_signal.setter
    def stop_signal(self, signal: threading.Event | None) -> None:
        self._stop_signal = signal
        for sink in self._sinks:
            # Not ``hasattr`` first: setting it on a child that never reads it is harmless, and
            # a child that refuses must not stop its siblings from getting theirs.
            try:
                sink.stop_signal = signal  # type: ignore[attr-defined]
            except Exception as err:
                _diag.absorbed(
                    "handing a MultiSink child its stop signal",
                    err,
                    f"{type(sink).__name__} stays uninterruptible",
                )

    def losses(self) -> SinkLosses | None:
        """Sum the children's losses so a fan-out reports the whole tree (SPEC-026 FR-002).

        Never raises: a child without ``losses()`` contributes zero, and a child whose accessor
        raises or returns the wrong shape is skipped rather than allowed to take the aggregate —
        and ``health()`` with it — down. Nesting is handled for free, since a child ``MultiSink``
        is just another sink with a ``losses()``.

        ``MultiSink.failed`` is deliberately absent from the total. It counts child *calls* that
        raised, not events, so adding it to a per-event figure would produce a number with no
        unit. The children already report their own loss in events.

        ``None`` when *no* child reported anything — including an empty fan-out. FR-003 separates
        "reports nothing" from "reports no loss", and a tree of silent children has not given a
        clean bill of health; summing them to zero would claim one on their behalf. One reporting
        child is enough to make the total meaningful, since the silent ones contribute zero.
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
        """Close every child, isolating a failing child so the rest still close (FR-002)."""
        for sink in self._sinks:
            try:
                sink.close()
            except Exception as err:  # close-all: an earlier failure must not skip the rest
                self.failed += 1
                _diag.absorbed("closing a MultiSink child", err, f"{type(sink).__name__} skipped")
