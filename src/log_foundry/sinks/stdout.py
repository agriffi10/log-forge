"""StdoutSink — JSON lines to a stream (arch §8, guide Phase 5)."""

from __future__ import annotations

import json
import sys
from typing import TextIO

__all__ = ["StdoutSink"]


class StdoutSink:
    """Writes each event as one JSON line to a stream.

    This is the zero-dependency default sink, for local dev and container log scraping. Like
    every sink it receives already-built event dicts and knows nothing about spans or context.
    """

    def __init__(self, stream: TextIO | None = None) -> None:
        """Binds the sink to an output stream.

        Args:
          stream: The stream to write to, defaulting to ``sys.stdout``.

        Returns:
          None.

        Raises:
          None.
        """
        self._stream = stream if stream is not None else sys.stdout

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Writes every event in the batch as a ``json.dumps`` line, then flushes.

        Args:
          batch: The events to write.

        Returns:
          None.

        Raises:
          Exception: Whatever the stream raises on write or flush.
        """
        for event in batch:
            self._stream.write(json.dumps(event) + "\n")
        self._stream.flush()

    def close(self) -> None:
        """Flushes the stream, since nothing is buffered inside the sink itself.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the stream raises on flush.
        """
        self._stream.flush()
