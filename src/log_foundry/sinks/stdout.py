"""StdoutSink — JSON lines to a stream (arch §8, guide Phase 5)."""

from __future__ import annotations

import json
import sys
from typing import TextIO

__all__ = ["StderrSink", "StdoutSink"]


class StdoutSink:
    """Writes each event as one JSON line to a stream.

    This is the zero-dependency default sink, for local dev and container log scraping. Like
    every sink it receives already-built event dicts and knows nothing about spans or context.

    It takes **no** transport lock (SPEC-028 FR-002), and the honest reason is that locking it
    was out of scope there rather than that it needs none: ``emit`` writes one line per event, so
    two threads' *batches* do interleave, exactly as they did in ``FileSink`` before its lock.
    Individual lines survive — ``TextIOWrapper.write`` holds its own lock — but line integrity is
    not the guarantee at stake, as ``test_file_sink_keeps_each_batch_contiguous_under_concurrent
    _emitters`` records. Interleaved batches are harmless in a line-oriented stream that a
    scraper reads per line, which is what this sink is for; a caller needing contiguity should
    use ``FileSink``.

    It **adds no post-close guard** (SPEC-032 FR-003), because ``close()`` only flushes — the
    stream belongs to the process, not to this sink, so a later batch still lands.


    It keeps **no** client buffer (SPEC-036 FR-002): ``emit`` flushes the stream before it
    returns, so nothing of this sink's is left pending between calls.
    """

    def __init__(self, stream: TextIO | None = None) -> None:
        """Binds the sink to an output stream, once, at construction.

        The binding is deliberate and permanent for the life of the sink: a later
        ``contextlib.redirect_stdout`` or a test's capture of ``sys.stdout`` is not honoured,
        because the attribute was resolved here. Passing ``stream=`` explicitly is how a caller
        — a test above all — captures the output (SPEC-031 FR-003).

        Args:
          stream: The stream to write to, defaulting to ``sys.stdout`` as resolved now.

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


class StderrSink(StdoutSink):
    """The :class:`~log_foundry.sinks.stdout.StdoutSink` shape, defaulting to stderr (FR-004).

    It writes each event as one ``json.dumps`` line and flushes, exactly like ``StdoutSink`` —
    only the default stream differs, following the twelve-factor convention of logs on stderr
    and app output on stdout.


    It keeps **no** client buffer (SPEC-036 FR-002): ``emit`` flushes the stream before it
    returns, so nothing of this sink's is left pending between calls.
    """

    def __init__(self, stream: TextIO | None = None) -> None:
        """Binds the sink to an output stream, once, at construction.

        The binding is resolved here and not re-read per write, so a later
        ``contextlib.redirect_stderr`` is not honoured — the same property
        :class:`~log_foundry.sinks.stdout.StdoutSink` documents (SPEC-031 FR-003), restated
        because this override means none of that docstring is inherited.

        Args:
          stream: The stream to write to, defaulting to ``sys.stderr`` as resolved now. An
            explicit one, such as a ``StringIO``, can be injected for capture.

        Returns:
          None.

        Raises:
          None.
        """
        super().__init__(stream if stream is not None else sys.stderr)
