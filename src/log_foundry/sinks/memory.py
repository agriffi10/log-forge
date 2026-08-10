"""MemorySink — collect events in process, for tests and inspection (arch §8, SPEC-008)."""

from __future__ import annotations

__all__ = ["MemorySink"]


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
