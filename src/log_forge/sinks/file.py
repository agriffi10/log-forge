"""FileSink + RotatingFileSink — durable local NDJSON on disk (arch §8, SPEC-008).

Not every deployment ships to a cloud queue; local dev, debugging, air-gapped hosts, and simple
archival just want events on the local disk. These are the zero-dependency file sinks: one appends
NDJSON to a single file; the other bounds on-disk growth by rotating on a size and/or time trigger
while retaining a fixed number of numbered backups. Like every sink they receive *already-built*
event dicts and know nothing about spans or context (that dumbness is what makes sinks swappable).

Synchronous stdlib writes only — no async/mmap/O_DIRECT — and a single-process, single-worker-thread
writer is assumed (arch §9); cross-process coordination is out of scope (SPEC-008).
"""

from __future__ import annotations

import json
import os
import time
from typing import TextIO

__all__ = ["FileSink", "RotatingFileSink"]

# Time-trigger unit codes -> seconds, mirroring stdlib TimedRotatingFileHandler's vocabulary
# (subset). Matched case-insensitively; the rollover interval is ``interval * _WHEN_SECONDS[when]``.
_WHEN_SECONDS = {
    "S": 1,
    "M": 60,
    "H": 60 * 60,
    "D": 60 * 60 * 24,
}


class FileSink:
    """A :class:`~log_forge.sinks.base.Sink` that appends events as NDJSON to one file.

    The file is opened once in append text mode at construction, so a missing parent directory
    surfaces immediately (at ``configure`` time) rather than on the first flush. The file is created
    if absent and appended to — never truncated — if it already exists.
    """

    def __init__(self, path: str, *, encoding: str = "utf-8") -> None:
        self._path = path
        self._encoding = encoding
        self._stream: TextIO = open(path, "a", encoding=encoding)
        self._closed = False

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Write every event as one ``json.dumps`` line terminated by ``\\n``, then flush (FR-001)."""
        for event in batch:
            self._stream.write(json.dumps(event) + "\n")
        self._stream.flush()

    def close(self) -> None:
        """Flush and close the file handle; a second call is a no-op (FR-001)."""
        if self._closed:
            return
        self._stream.flush()
        self._stream.close()
        self._closed = True


class RotatingFileSink:
    """A :class:`~log_forge.sinks.base.Sink` that rotates the active NDJSON file to bound its growth.

    Two independent triggers, either or both enabled:

    * **size** — with ``max_bytes > 0``, rotate *before* the write that would push the active file
      past ``max_bytes`` (so it never grows unbounded).
    * **time** — with a ``when`` unit code (``"S"``/``"M"``/``"H"``/``"D"``) and ``interval``, rotate
      on the first emit after ``interval`` units have elapsed since the last rotation.

    Rotation renames the active file through numbered backups (``path.1`` … ``path.N``), prunes any
    backup beyond ``backup_count``, and opens a fresh active file. ``backup_count=0`` keeps no
    backups (the active file is simply truncated/replaced). No event is lost across a rotation: the
    rotate happens *before* the pending event is written, and the event lands in the fresh file.
    """

    def __init__(
        self,
        path: str,
        *,
        max_bytes: int = 0,
        backup_count: int = 0,
        when: str | None = None,
        interval: int = 1,
    ) -> None:
        self._path = path
        self._encoding = "utf-8"
        self._max_bytes = max_bytes
        self._backup_count = backup_count
        self._interval_seconds = self._rollover_seconds(when, interval)
        self._stream: TextIO = open(path, "a", encoding=self._encoding)
        # Byte size of the active file, tracked explicitly so ``max_bytes`` is measured in bytes even
        # through a text-mode stream. Seeded from any pre-existing file we appended to.
        self._size = os.path.getsize(path) if os.path.exists(path) else 0
        self._next_rollover = self._schedule_next()
        self._closed = False

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Append each event, rotating first whenever a size/time trigger fires (FR-002)."""
        for event in batch:
            line = json.dumps(event) + "\n"
            data = len(line.encode(self._encoding))  # byte cost, for size accounting
            if self._should_rotate(data):
                self._rotate()
            self._stream.write(line)
            self._size += data
        self._stream.flush()

    def close(self) -> None:
        """Flush and close the active handle; a second call is a no-op (FR-002)."""
        if self._closed:
            return
        self._stream.flush()
        self._stream.close()
        self._closed = True

    # -- internals ----------------------------------------------------------------------

    @staticmethod
    def _rollover_seconds(when: str | None, interval: int) -> float | None:
        """Translate a ``when`` unit code + ``interval`` into a rollover period in seconds.

        Returns ``None`` (no time trigger) when ``when`` is ``None``; raises ``ValueError`` on an
        unrecognized unit code so a typo fails loudly at construction, not silently at runtime.
        """
        if when is None:
            return None
        try:
            unit = _WHEN_SECONDS[when.upper()]
        except KeyError:
            raise ValueError(
                f"invalid 'when' unit {when!r}; expected one of {sorted(_WHEN_SECONDS)}"
            ) from None
        return unit * interval

    def _schedule_next(self) -> float | None:
        """Absolute monotonic-wallclock time of the next time-based rotation (or ``None``)."""
        if self._interval_seconds is None:
            return None
        return time.time() + self._interval_seconds

    def _should_rotate(self, incoming: int) -> bool:
        """Decide whether to rotate before writing ``incoming`` bytes (size and/or time)."""
        if (
            self._max_bytes > 0
            and self._size > 0
            and self._size + incoming > self._max_bytes
        ):
            return True
        if self._next_rollover is not None and time.time() >= self._next_rollover:
            return True
        return False

    def _rotate(self) -> None:
        """Close the active file, shift/prune numbered backups, and open a fresh active file."""
        self._stream.close()
        if self._backup_count > 0:
            # Shift path.(N-1) -> path.N downward, dropping anything past backup_count, then move
            # the active file to path.1. Highest-numbered backup is the oldest.
            for i in range(self._backup_count - 1, 0, -1):
                src = f"{self._path}.{i}"
                dst = f"{self._path}.{i + 1}"
                if os.path.exists(src):
                    if os.path.exists(dst):
                        os.remove(dst)
                    os.replace(src, dst)
            first = f"{self._path}.1"
            if os.path.exists(first):
                os.remove(first)
            if os.path.exists(self._path):
                os.replace(self._path, first)
        elif os.path.exists(self._path):
            # No backups retained: drop the active file so reopening starts empty.
            os.remove(self._path)
        self._stream = open(self._path, "a", encoding=self._encoding)
        self._size = 0
        self._next_rollover = self._schedule_next()
