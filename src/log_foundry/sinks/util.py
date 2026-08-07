"""Small utility sinks: StderrSink, NullSink, MemorySink (arch §8, SPEC-008)."""

from __future__ import annotations

import sys
import threading
from typing import TextIO

from log_foundry.sinks.stdout import StdoutSink

__all__ = ["MemorySink", "NullSink", "StderrSink"]


class StderrSink(StdoutSink):
    """The :class:`~log_foundry.sinks.stdout.StdoutSink` shape, defaulting to stderr (FR-004).

    It writes each event as one ``json.dumps`` line and flushes, exactly like ``StdoutSink`` —
    only the default stream differs, following the twelve-factor convention of logs on stderr
    and app output on stdout.
    """

    def __init__(self, stream: TextIO | None = None) -> None:
        """Binds the sink to an output stream.

        Args:
          stream: The stream to write to, defaulting to ``sys.stderr``. An explicit one, such
            as a ``StringIO``, can be injected for capture.

        Returns:
          None.

        Raises:
          None.
        """
        super().__init__(stream if stream is not None else sys.stderr)


class NullSink:
    """A :class:`~log_foundry.sinks.base.Sink` that discards every event (FR-005).

    This is useful to disable output without unwiring the pipeline, or to benchmark everything
    up to the sink. It deliberately exposes no ``losses()`` (SPEC-026 FR-002): discarding is
    what this sink is for, and reporting it as loss would make ``health()``'s alert idiom fire
    on every batch for anyone who chose this sink to turn logging off. The counter stays
    readable on the instance.

    It takes **no** transport lock (SPEC-028 FR-002) and **adds no post-close guard**
    (SPEC-032 FR-003): there is no transport and ``close()`` releases nothing, so discarding a
    batch after close is the same operation as discarding one before it.
    """

    def __init__(self) -> None:
        """Starts the discarded-event counter at zero.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.dropped = 0
        self._counter_lock = threading.Lock()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Discards the batch, counting the events dropped (FR-005).

        Args:
          batch: The events to discard.

        Returns:
          None.

        Raises:
          None.
        """
        with self._counter_lock:
            self.dropped += len(batch)

    def close(self) -> None:
        """Does nothing, since nothing is held (FR-005).

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """


class MemorySink:
    """A :class:`~log_foundry.sinks.base.Sink` that collects events into a list (FR-006).

    ``.events`` is a plain list exposing every event in arrival order, for asserting in tests or
    eyeballing in a notebook. With ``maxlen`` set it behaves as a bounded ring, keeping only the
    most recent events; the list object identity is stable, so a held reference keeps seeing
    updates.

    It takes **no** transport lock (SPEC-028 FR-002) and **adds no post-close guard**
    (SPEC-032 FR-003): there is no transport, and ``close()`` releases nothing — a test that
    closes the sink and then asserts on a later batch still sees it in ``.events``.
    """

    def __init__(self, maxlen: int | None = None) -> None:
        """Starts an empty collection, optionally bounded.

        Args:
          maxlen: The most events to keep, or ``None`` to keep all of them.

        Returns:
          None.

        Raises:
          None.
        """
        self.events: list[dict[str, object]] = []
        self._maxlen = maxlen

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Appends the batch in order, trimming to the most recent events if bounded (FR-006).

        Args:
          batch: The events to collect.

        Returns:
          None.

        Raises:
          None.
        """
        self.events.extend(batch)
        if self._maxlen is not None and len(self.events) > self._maxlen:
            del self.events[: len(self.events) - self._maxlen]

    def close(self) -> None:
        """Does nothing, since collected events remain readable after close (FR-006).

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
