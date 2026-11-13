"""StdoutSink — JSON lines to a stream (arch §8, guide Phase 5)."""

from __future__ import annotations

import json
import sys
import threading
from typing import TextIO

from log_foundry.sinks._deferral import Deferral
from log_foundry.sinks._encode import encode_events
from log_foundry.sinks.base import SinkLosses

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

    It encodes the whole batch before its first write (SPEC-069 FR-001), so an event it cannot
    encode costs only itself and is reported through :meth:`losses`.
    """

    def __init__(self, stream: TextIO | None = None) -> None:
        """Binds the sink to an output stream, once, at construction.

        It also creates the counter an unencodable event is committed to and the lock that commit
        takes (SPEC-069 FR-001). The lock guards the counter alone and is never held across a
        write, so the sink still takes no transport lock.

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
        self._unencodable = 0
        self._counter_lock = threading.Lock()

    def losses(self) -> SinkLosses | None:
        """Reports the events this sink could not encode, once it has any (SPEC-069 FR-001).

        ``None`` until something is counted, the rule ``wrapped_losses`` applies, so a process
        that only logs clean events still reads ``health().sink is None``.

        Args:
          None.

        Returns:
          ``None`` while nothing is counted, else the counters.

        Raises:
          None.
        """
        with self._counter_lock:
            if not self._unencodable:
                return None
            return SinkLosses(dropped=self._unencodable, failed=0)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Writes every event in the batch as a ``json.dumps`` line, then flushes.

        Every event is encoded before the first write (SPEC-069 FR-001). One that cannot be
        encoded is dropped, counted and announced only when this call returns, through the
        deferral helper; before, the lines ahead of it were already written when the encode
        raised, and the worker's retry wrote them again. A batch of nothing encodable touches
        the stream not at all and returns (FR-002). A stream that raises partway through still
        leaves the lines it took, which the retry writes again: an accepted edge for 1.2.0,
        recorded as an exception to invariant 7 in the driver-error decision.

        Args:
          batch: The events to write.

        Returns:
          None.

        Raises:
          Exception: Whatever the stream raises on write or flush, leaving nothing counted.
        """
        with Deferral(self) as pending:
            lines = encode_events(batch, self._encode, pending, sink=type(self).__name__)
            if batch and not lines:
                return
            for _, line in lines:
                self._stream.write(line)
            self._stream.flush()

    def _encode(self, event: dict[str, object]) -> str:
        """Encodes one event as its newline-terminated JSON line.

        Args:
          event: The event to encode.

        Returns:
          The line to write.

        Raises:
          TypeError: If the event is not JSON-serializable, which ``encode_events`` turns into a
            drop of this one event. Neither source of events produces one: assembly coerces every
            value (``sanitize``), and a ``TransformSink`` checks and coerces what its function
            returns (SPEC-058 FR-001).
        """
        return json.dumps(event) + "\n"

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
