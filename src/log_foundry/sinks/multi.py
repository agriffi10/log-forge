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

if TYPE_CHECKING:
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

    def close(self) -> None:
        """Close every child, isolating a failing child so the rest still close (FR-002)."""
        for sink in self._sinks:
            try:
                sink.close()
            except Exception as err:  # close-all: an earlier failure must not skip the rest
                self.failed += 1
                _diag.absorbed("closing a MultiSink child", err, f"{type(sink).__name__} skipped")
