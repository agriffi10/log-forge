"""StdoutSink — JSON lines to a stream (arch §8, guide Phase 5).

The zero-dependency default sink: great for local dev and container log scraping. Like every
sink it receives *already-built* event dicts and knows nothing about spans or context — that
dumbness is what makes sinks trivially swappable.
"""

from __future__ import annotations

import json
import sys
from typing import TextIO

__all__ = ["StdoutSink"]


class StdoutSink:
    """Write each event as one JSON line to ``stream`` (default ``sys.stdout``)."""

    def __init__(self, stream: TextIO | None = None) -> None:
        self._stream = stream if stream is not None else sys.stdout

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Write every event in ``batch`` as a ``json.dumps`` line, then flush."""
        for event in batch:
            self._stream.write(json.dumps(event) + "\n")
        self._stream.flush()

    def close(self) -> None:
        """Flush the stream (nothing is buffered inside the sink itself)."""
        self._stream.flush()
