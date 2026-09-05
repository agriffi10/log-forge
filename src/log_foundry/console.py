"""Console echo — a synchronous, human-readable output path (SPEC-002 FR-002)."""

from __future__ import annotations

import sys
import threading
from typing import TextIO

from log_foundry import _diag

__all__ = ["ConsoleWriter"]


class ConsoleWriter:
    """Renders events as human-readable ``LEVEL   message`` lines to a stream.

    This is separate from the async :class:`~log_foundry.sinks.base.Sink`: where the sink
    ships structured JSON downstream, the console writer surfaces a line immediately so an
    operator sees it without waiting for the async flush. It is deliberately dumb, rendering
    an already-built event dict and knowing nothing about spans, and echo is additive — an
    echoed event still rides the normal pipeline to the sink.

    The default stream is **stderr**, not stdout (SPEC-031 FR-003, which corrected two
    documents that said otherwise). It is the twelve-factor convention ``StderrSink`` already
    cites — logs on stderr, the application's own output on stdout — so an echo cannot corrupt
    a program whose stdout is a data stream someone pipes.

    **The writer owns its stream's failures** (SPEC-054 FR-005). ``api._log`` used to absorb
    them one line at a time: measured, 200,000 echoed events piped into ``head -1`` wrote
    199,970 identical stderr lines, one per event, forever. A ``BrokenPipeError``, or the
    ``ValueError`` a closed file raises, is a stream that will not come back, so it is announced
    once and echo is disabled for the life of the writer. Any other ``Exception`` may be
    transient — ``EAGAIN`` on a non-blocking terminal, ``ENOSPC`` on a file — so the writer keeps
    trying and announces the first failure and then every :data:`_diag.WARN_EVERY`-th with the
    running total, the queue-full site's idiom. The state lives on the writer rather than the
    module so a ``ConsoleWriter(stream=…)`` built for a test starts clean, and the counter is
    taken under a lock on the failure path only, since ``_log`` reaches the process-global
    writer from arbitrary application threads; the disable flag is a set-only latch read
    unlocked, as ``Worker.submit`` reads ``_shutdown_done``.
    """

    def __init__(self, stream: TextIO | None = None) -> None:
        """Binds the writer to an output stream, once, at construction.

        The binding is deliberate and permanent for the life of the writer: a later
        ``contextlib.redirect_stderr`` or a test's capture of ``sys.stderr`` is not honoured,
        because the attribute was resolved here. ``api._console`` is built at import, so in
        practice a process's echo stream is fixed before any test runs. Passing ``stream=``
        explicitly is how a caller — a test above all — captures the output (SPEC-031 FR-003).

        Args:
          stream: The stream to write to, defaulting to ``sys.stderr`` as resolved now.

        Returns:
          None.

        Raises:
          None.
        """
        self._stream = stream if stream is not None else sys.stderr
        self._disabled = False
        self._failures = 0
        self._lock = threading.Lock()

    def write(self, event: dict[str, object]) -> None:
        """Writes one ``{level:<7} {message}`` line and flushes immediately.

        A stream fault is absorbed here rather than propagated: a permanent one disables the
        writer after one announcement, a transient one is counted and announced on the throttle
        (see the class docstring). The ``KeyError`` for an event missing ``level`` or
        ``message`` still escapes, because that is a library defect rather than a stream fault,
        and ``api._log``'s outer guard is the total guard for it.

        Args:
          event: An already-built event carrying ``level`` and ``message``.

        Returns:
          None.

        Raises:
          KeyError: If the event is missing ``level`` or ``message``.
        """
        if self._disabled:
            return
        level = str(event["level"])
        message = str(event["message"])
        try:
            self._stream.write(f"{level:<7} {message}\n")
            self._stream.flush()
        except (BrokenPipeError, ValueError) as exc:
            self._disabled = True
            _diag.absorbed(
                "echoing to the console",
                exc,
                "echo is disabled for the rest of the process; events still reach the sink",
            )
        except Exception as exc:
            with self._lock:
                self._failures += 1
                total = self._failures
            if total == 1 or total % _diag.WARN_EVERY == 0:
                _diag.absorbed(
                    "echoing to the console",
                    exc,
                    f"{total} echo(es) failed; count is cumulative",
                )
