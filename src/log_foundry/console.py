"""Console echo — a synchronous, human-readable output path (SPEC-002 FR-002)."""

from __future__ import annotations

import sys
from typing import TextIO

__all__ = ["ConsoleWriter"]


class ConsoleWriter:
    """Renders events as human-readable ``LEVEL   message`` lines to a stream.

    This is separate from the async :class:`~log_foundry.sinks.base.Sink`: where the sink
    ships structured JSON downstream, the console writer surfaces a line immediately so an
    operator sees it without waiting for the async flush. It is deliberately dumb, rendering
    an already-built event dict and knowing nothing about spans, and echo is additive — an
    echoed event still rides the normal pipeline to the sink.
    """

    def __init__(self, stream: TextIO | None = None) -> None:
        """Binds the writer to an output stream.

        Args:
          stream: The stream to write to, defaulting to ``sys.stderr``.

        Returns:
          None.

        Raises:
          None.
        """
        self._stream = stream if stream is not None else sys.stderr

    def write(self, event: dict[str, object]) -> None:
        """Writes one ``{level:<7} {message}`` line and flushes immediately.

        Args:
          event: An already-built event carrying ``level`` and ``message``.

        Returns:
          None.

        Raises:
          KeyError: If the event is missing ``level`` or ``message``.
        """
        level = str(event["level"])
        message = str(event["message"])
        self._stream.write(f"{level:<7} {message}\n")
        self._stream.flush()
