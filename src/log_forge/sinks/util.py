"""Small utility sinks: StderrSink, NullSink, MemorySink (arch §8, SPEC-008).

Three tiny zero-dependency sinks that round out the local family: send NDJSON to stderr (the
twelve-factor convention — logs on stderr, app output on stdout), discard everything (to disable
output or benchmark the pipeline), or collect events into an in-process list (for tests and
notebooks). Like every sink they operate purely on already-built event dicts (arch §8).
"""

from __future__ import annotations

import sys
from typing import TextIO

from log_forge.sinks.stdout import StdoutSink

__all__ = ["StderrSink", "NullSink", "MemorySink"]


class StderrSink(StdoutSink):
    """The :class:`~log_forge.sinks.stdout.StdoutSink` shape, defaulting to ``sys.stderr`` (FR-004).

    Writes each event as one ``json.dumps`` line and flushes, exactly like ``StdoutSink`` — only the
    default stream differs. An explicit ``stream`` (e.g. a ``StringIO``) can be injected for capture.
    """

    def __init__(self, stream: TextIO | None = None) -> None:
        super().__init__(stream if stream is not None else sys.stderr)


class NullSink:
    """A :class:`~log_forge.sinks.base.Sink` that discards every event (FR-005).

    Useful to disable output without unwiring the pipeline, or to benchmark everything up to the
    sink. ``dropped`` counts how many events were discarded, so a benchmark can assert throughput.
    """

    def __init__(self) -> None:
        self.dropped = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Discard the batch, counting the events dropped (FR-005)."""
        self.dropped += len(batch)

    def close(self) -> None:
        """No-op — nothing is held (FR-005)."""


class MemorySink:
    """A :class:`~log_forge.sinks.base.Sink` that collects events into an in-process list (FR-006).

    ``.events`` is a plain ``list`` exposing every event in arrival order, ideal for asserting in
    tests or eyeballing in a notebook. With ``maxlen`` set it behaves as a bounded ring, keeping only
    the most recent ``maxlen`` events; the list object identity is stable, so a held reference keeps
    seeing updates.
    """

    def __init__(self, maxlen: int | None = None) -> None:
        self.events: list[dict[str, object]] = []
        self._maxlen = maxlen

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Append the batch in order, trimming to the most recent ``maxlen`` if bounded (FR-006)."""
        self.events.extend(batch)
        if self._maxlen is not None and len(self.events) > self._maxlen:
            del self.events[: len(self.events) - self._maxlen]

    def close(self) -> None:
        """No-op — collected events remain readable after close (FR-006)."""
