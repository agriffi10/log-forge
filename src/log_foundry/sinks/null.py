"""NullSink — discard every event, counting what it discarded (arch §8, SPEC-008)."""

from __future__ import annotations

import threading

__all__ = ["NullSink"]


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
