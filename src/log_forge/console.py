"""Console echo — a synchronous, human-readable output path (SPEC-002 FR-002).

Separate from the async :class:`~log_forge.sinks.base.Sink`: where the sink ships structured
JSON downstream, the console writer surfaces a plain ``LEVEL   message`` line *immediately*
(to a terminal user or a Lambda's stdout → CloudWatch) so an operator sees it without waiting
for the async flush. It is deliberately dumb — it renders an already-built event dict and
knows nothing about spans. Echo is additive: an echoed event still rides the normal pipeline
to the sink.
"""

from __future__ import annotations

import sys
from typing import TextIO

__all__ = ["ConsoleWriter"]


class ConsoleWriter:
    """Render events as human-readable ``LEVEL   message`` lines to ``stream``."""

    def __init__(self, stream: TextIO | None = None) -> None:
        self._stream = stream if stream is not None else sys.stderr

    def write(self, event: dict[str, object]) -> None:
        """Write one ``{level:<7} {message}`` line and flush immediately."""
        level = str(event["level"])
        message = str(event["message"])
        self._stream.write(f"{level:<7} {message}\n")
        self._stream.flush()
