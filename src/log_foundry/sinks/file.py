"""FileSink + RotatingFileSink — durable local NDJSON on disk (arch §8, SPEC-008)."""

from __future__ import annotations

import json
import os
import threading
import time
from typing import TextIO

__all__ = ["FileSink", "RotatingFileSink"]

_WHEN_SECONDS = {
    "S": 1,
    "M": 60,
    "H": 60 * 60,
    "D": 60 * 60 * 24,
}


class FileSink:
    """A :class:`~log_foundry.sinks.base.Sink` that appends events as NDJSON to one file.

    Not every deployment ships to a cloud queue; local dev, debugging, air-gapped hosts and
    simple archival just want events on the local disk. Writes are synchronous stdlib calls
    only.

    Writers within the process are serialized on a lock, because ``emit`` may be called
    concurrently (SPEC-028 FR-002) — this module claimed a single worker thread until that spec
    measured the orphan path emitting on application threads at the same time. Cross-*process*
    coordination remains out of scope: two processes appending to one path are on their own.
    """

    def __init__(self, path: str, *, encoding: str = "utf-8") -> None:
        """Opens the file in append text mode.

        Opening at construction means a missing parent directory surfaces immediately, at
        ``configure`` time, rather than on the first flush. The file is created if absent and
        appended to — never truncated — if it already exists.

        Args:
          path: The file to append to.
          encoding: The text encoding to write in.

        Returns:
          None.

        Raises:
          OSError: If the file cannot be opened.
        """
        self._path = path
        self._encoding = encoding
        self._stream: TextIO = open(path, "a", encoding=encoding)
        self._closed = False
        self._lock = threading.Lock()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Writes every event as one newline-terminated ``json.dumps`` line, then flushes.

        The lock covers the whole batch rather than each line (SPEC-028 FR-002). A text stream
        does not promise that one ``write`` is atomic against another, so per-line locking could
        still interleave two events' bytes; batch-wide locking also keeps a batch contiguous in
        the file, which is what makes the output readable.

        Args:
          batch: The events to write.

        Returns:
          None.

        Raises:
          OSError: If the write or flush fails (FR-001).
        """
        with self._lock:
            for event in batch:
                self._stream.write(json.dumps(event) + "\n")
            self._stream.flush()

    def close(self) -> None:
        """Flushes and closes the file handle, with a second call a no-op (FR-001).

        Taking the same lock ``emit`` takes means a close waits for an in-flight write rather
        than pulling the stream out from under it (SPEC-028 FR-002).

        Args:
          None.

        Returns:
          None.

        Raises:
          OSError: If the flush or close fails.
        """
        with self._lock:
            if self._closed:
                return
            self._stream.flush()
            self._stream.close()
            self._closed = True


class RotatingFileSink:
    """A :class:`~log_foundry.sinks.base.Sink` that rotates its NDJSON file to bound growth.

    Two independent triggers may be enabled, either or both. With a positive ``max_bytes`` it
    rotates before the write that would push the active file past that size, so the file never
    grows unbounded; with a ``when`` unit code and an interval it rotates on the first emit after
    that period has elapsed since the last rotation.

    Rotation renames the active file through numbered backups, prunes any beyond the backup
    count, and opens a fresh active file — a backup count of zero keeps none, simply replacing
    the active file. No event is lost across a rotation, because the rotate happens before the
    pending event is written and the event lands in the fresh file.

    A rotation rebinds the active stream, so it is the sink where concurrent writers did real
    damage: a second thread mid-``emit`` could write to the handle rotation had just closed, or
    to the pre-rotation file it had already renamed away. Both are serialized on a lock
    (SPEC-028 FR-002).
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
        """Opens the active file and arms whichever rotation triggers were configured.

        The active file's byte size is tracked explicitly, so ``max_bytes`` is measured in bytes
        even through a text-mode stream, and it is seeded from any pre-existing file appended
        to.

        Args:
          path: The active file to append to.
          max_bytes: The size trigger, or 0 to disable it.
          backup_count: How many numbered backups to retain.
          when: The time-trigger unit code, matched case-insensitively, or ``None`` to disable
            it. The vocabulary mirrors a subset of the stdlib ``TimedRotatingFileHandler``'s.
          interval: How many units make up one rollover period.

        Returns:
          None.

        Raises:
          ValueError: If the unit code is unrecognized.
          OSError: If the file cannot be opened.
        """
        self._path = path
        self._encoding = "utf-8"
        self._max_bytes = max_bytes
        self._backup_count = backup_count
        self._interval_seconds = self._rollover_seconds(when, interval)
        self._stream: TextIO = open(path, "a", encoding=self._encoding)
        self._size = os.path.getsize(path) if os.path.exists(path) else 0
        self._next_rollover = self._schedule_next()
        self._closed = False
        self._lock = threading.Lock()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Appends each event, rotating first whenever a size or time trigger fires (FR-002).

        The lock spans the whole batch, so the decide-rotate-write-account sequence is
        indivisible (SPEC-028 FR-002). Guarding only ``_rotate`` would not be enough: the
        ``_should_rotate`` check and the write that follows it must see the same stream, or a
        rotation between them sends the line to a closed handle.

        Args:
          batch: The events to write.

        Returns:
          None.

        Raises:
          OSError: If a write, flush or rotation fails.
        """
        with self._lock:
            for event in batch:
                line = json.dumps(event) + "\n"
                data = len(line.encode(self._encoding))
                if self._should_rotate(data):
                    self._rotate()
                self._stream.write(line)
                self._size += data
            self._stream.flush()

    def close(self) -> None:
        """Flushes and closes the active handle, with a second call a no-op (FR-002).

        Taking the same lock ``emit`` takes means a close waits for an in-flight write, and in
        particular never lands between a rotation and the write it was making room for
        (SPEC-028 FR-002).

        Args:
          None.

        Returns:
          None.

        Raises:
          OSError: If the flush or close fails.
        """
        with self._lock:
            if self._closed:
                return
            self._stream.flush()
            self._stream.close()
            self._closed = True

    @staticmethod
    def _rollover_seconds(when: str | None, interval: int) -> float | None:
        """Translates a unit code and interval into a rollover period in seconds.

        Args:
          when: The unit code, or ``None`` for no time trigger.
          interval: How many units make up one period.

        Returns:
          The period in seconds, or ``None`` when there is no time trigger.

        Raises:
          ValueError: If the unit code is unrecognized, so a typo fails loudly at construction
            rather than silently at runtime.
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
        """Returns the wall-clock time of the next time-based rotation.

        Args:
          None.

        Returns:
          The absolute time, or ``None`` when there is no time trigger.

        Raises:
          None.
        """
        if self._interval_seconds is None:
            return None
        return time.time() + self._interval_seconds

    def _should_rotate(self, incoming: int) -> bool:
        """Decides whether to rotate before writing the next event.

        Args:
          incoming: The byte cost of the event about to be written.

        Returns:
          True when the size or time trigger has fired.

        Raises:
          None.
        """
        if (
            self._max_bytes > 0
            and self._size > 0
            and self._size + incoming > self._max_bytes
        ):
            return True
        return self._next_rollover is not None and time.time() >= self._next_rollover

    def _rotate(self) -> None:
        """Closes the active file, shifts and prunes backups, then opens a fresh active file.

        Backups shift downward from the highest number, which is the oldest, dropping anything
        past the backup count, and the active file becomes ``path.1``. With no backups retained
        the active file is simply removed so reopening starts empty.

        Args:
          None.

        Returns:
          None.

        Raises:
          OSError: If a rename, removal or reopen fails.
        """
        self._stream.close()
        if self._backup_count > 0:
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
            os.remove(self._path)
        self._stream = open(self._path, "a", encoding=self._encoding)
        self._size = 0
        self._next_rollover = self._schedule_next()
