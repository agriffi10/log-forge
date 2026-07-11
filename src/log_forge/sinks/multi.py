"""MultiSink — fan one batch out to several sinks (arch §8, SPEC-006 FR-002).

One ``configure(sink=...)`` can echo to stdout *and* ship to SQS by wrapping both in a
``MultiSink``. Children receive the batch sequentially in construction order; a single child
whose ``emit`` (or ``close``) raises is isolated — the failure is counted on ``failed`` and
logged to stderr, and its siblings still run — so one broken destination never fails the whole
fan-out or the worker's retry. This mirrors the worker's own survive-a-sink-failure isolation
boundary (arch §9; best-practices §7 sanctions the broad catch here).
"""

from __future__ import annotations

import sys

from log_forge.sinks.base import Sink

__all__ = ["MultiSink"]


class MultiSink:
    """A :class:`~log_forge.sinks.base.Sink` that forwards each batch to every child sink.

    Attributes:
        failed: Count of child ``emit``/``close`` calls that raised and were isolated.
    """

    def __init__(self, *sinks: Sink) -> None:
        self._sinks = sinks
        self.failed = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Forward ``batch`` to every child in construction order, isolating failures (FR-002)."""
        for sink in self._sinks:
            try:
                sink.emit(batch)
            except Exception as err:  # isolation boundary: one child must not fail the rest
                self.failed += 1
                sys.stderr.write(
                    f"log-forge: MultiSink child {type(sink).__name__}.emit "
                    f"failed and was skipped: {err!r}\n"
                )

    def close(self) -> None:
        """Close every child, isolating a failing child so the rest still close (FR-002)."""
        for sink in self._sinks:
            try:
                sink.close()
            except Exception as err:  # close-all: an earlier failure must not skip the rest
                self.failed += 1
                sys.stderr.write(
                    f"log-forge: MultiSink child {type(sink).__name__}.close "
                    f"failed and was skipped: {err!r}\n"
                )
