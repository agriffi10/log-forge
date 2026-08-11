"""FileSink + RotatingFileSink — durable local NDJSON on disk (arch §8, SPEC-008)."""

from __future__ import annotations

import json
import os
import threading
import time
from typing import TextIO

from log_foundry.sinks.base import SinkDeliveryError

__all__ = ["FileSink", "RotatingFileSink"]

_WHEN_SECONDS = {
    "S": 1,
    "M": 60,
    "H": 60 * 60,
    "D": 60 * 60 * 24,
}


def _reopen_discarding(stream: TextIO, path: str, encoding: str) -> TextIO:
    """Strands an inherited stream's pending bytes and returns a fresh one on the same path.

    ``os.dup2`` points the inherited descriptor at ``os.devnull``, so the buffer this process
    inherited can only ever reach the null device — whether it is flushed deliberately, by the
    interpreter at exit, or by the garbage collector when the old object is dropped. The two
    steps are written in this order for readability and **not** because the order is what makes
    it safe: the replacement takes a different descriptor either way, since the inherited one is
    still occupied, and nothing else is running to flush anything in between. Reopening rather
    than reusing the descriptor is what gives the child a stream of its own, and it picks up the
    currently active file if the parent rotated.

    ``dup2`` leaves the redirected descriptor **inheritable** across an ``exec``, where the one
    ``open`` produced carried ``O_CLOEXEC``. That is a behaviour change and it is harmless: the
    descriptor names the null device, so what an exec'd process inherits is a handle on nothing.

    Args:
      stream: The inherited stream, still holding whatever the parent had not flushed.
      path: The file to reopen. **Append mode**, never write mode: the child shares this file
        with the parent and with whatever was written before either existed, so truncating here
        would destroy a log to protect it — strictly worse than the duplication this prevents.
      encoding: The text encoding to open it in, carried across so a child does not start
        writing a differently-encoded second half into the parent's file.

    Returns:
      The replacement stream.

    Raises:
      OSError: If the descriptor cannot be redirected or the path cannot be reopened.
      ValueError: If the stream has no usable descriptor, which means there is no inherited
        buffer this can strand and the caller must not carry on as though there were.
    """
    devnull = os.open(os.devnull, os.O_WRONLY)
    try:
        os.dup2(devnull, stream.fileno())
    finally:
        os.close(devnull)
    return open(path, "a", encoding=encoding)


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
        if not batch:
            return
        with self._lock:
            if self._closed:
                raise SinkDeliveryError(
                    f"FileSink wrote none of {len(batch)} event(s): the sink is closed"
                )
            for event in batch:
                self._stream.write(json.dumps(event) + "\n")
            self._stream.flush()

    def discard_buffered_after_fork(self) -> None:
        """Throws away the parent's unflushed bytes in a forked child (SPEC-039 FR-004).

        ``emit`` writes a whole batch into a **buffered** stream and flushes once at the end, so
        a fork landing inside it leaves both processes holding the same pending bytes and both
        writing them: measured, the event at the fork point appeared on disk twice. Without a
        ``before`` handler there is nowhere to empty the buffer from (FR-001), so the child
        strands it instead — the parent's copy is untouched and still reaches disk exactly once.

        No lock is taken, for the reason ``decorator._rebuild_worker_after_fork`` gives: there is
        one thread here by construction, and the lock was re-initialised moments earlier, so
        taking it could only wait on a holder that cannot exist. A hook that blocks here blocks a
        child that has not yet returned from ``fork``, where no watchdog can reach it.

        Args:
          None.

        Returns:
          None.

        Raises:
          OSError: If the descriptor cannot be redirected or the file cannot be reopened.
          ValueError: If the stream has no usable descriptor. ``_fork`` absorbs and announces
            either, since a child that cannot strand its buffer still has working locks.
        """
        if self._closed:
            return
        self._stream = _reopen_discarding(self._stream, self._path, self._encoding)

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
            self._closed = True
            self._stream.flush()
            self._stream.close()


class RotatingFileSink:
    """A :class:`~log_foundry.sinks.base.Sink` that rotates its NDJSON file to bound growth.

    Two independent triggers may be enabled, either or both. With a positive ``max_bytes`` it
    rotates before the write that would push the active file past that size, so the file never
    grows unbounded; with a ``when`` unit code and an interval it rotates on the first emit after
    that period has elapsed since the last rotation. That period is measured on the monotonic
    clock (SPEC-031 FR-001), so a wall-clock step in either direction neither defers a rotation
    nor forces an early one.

    Rotation renames the active file through numbered backups, prunes any beyond the backup
    count, and opens a fresh active file — a backup count of zero keeps none, simply replacing
    the active file. Backups are numbered rather than timestamped, so no filename derives from
    a clock at all and the monotonic deadline has no naming consequence. **The pending event is
    not lost across a rotation**, because the rotate happens before it is written and it lands in
    the fresh file (SPEC-038 FR-012 AC-5). That is the whole of the claim: it says nothing about
    events *already written*, which retention governs — at ``backup_count=0`` every one of them
    is destroyed at each rollover, which is why that is no longer the default.

    No counter is added for what retention discards, at any ``backup_count``. This sink is a
    bounded ring buffer, retention *is* the configuration, and discarding the oldest generation
    is that configuration working — the precedent being ``MemorySink(maxlen)``, which behaves as
    a bounded ring, counts nothing and implements no ``losses()``. Neither ``dropped`` (defined
    as discarded *before* attempting delivery) nor ``failed`` fits an event that was written and
    flushed to disk.

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
        backup_count: int = 1,
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
          backup_count: How many numbered backups to retain. **The default is 1** (SPEC-038
            FR-012): at ``0`` a rotation calls ``os.remove`` on the active file, so
            ``RotatingFileSink("app.log", max_bytes=10_000_000)`` silently threw away 10 MB at
            every rollover. ``0`` still truncates, unchanged, for a caller who wants that.

            The cost of the new default is disk: **2 x max_bytes under a size trigger, or one
            full rollover period under a time-only one** — and ``max_bytes`` defaults to ``0``,
            which bounds nothing, so a time-triggered sink's ceiling is whatever one period
            writes.
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
        if not batch:
            return
        with self._lock:
            if self._closed:
                raise SinkDeliveryError(
                    f"RotatingFileSink wrote none of {len(batch)} event(s): the sink is closed"
                )
            for event in batch:
                line = json.dumps(event) + "\n"
                data = len(line.encode(self._encoding))
                if self._should_rotate(data):
                    self._rotate()
                self._stream.write(line)
                self._size += data
            self._stream.flush()

    def discard_buffered_after_fork(self) -> None:
        """Throws away the parent's unflushed bytes in a forked child (SPEC-039 FR-004).

        Identical to :meth:`FileSink.discard_buffered_after_fork` and measured on this class
        too, because the window is the same one: a whole batch written into a buffered stream
        and flushed once at the end.

        ``_size`` is deliberately left as the parent set it. It counts bytes this sink believes
        are in the active file, and the moment two processes append to one path that is an
        approximation whichever way it is computed (FR-005 AC-1) — re-reading the file's size
        here would claim a precision a shared handle cannot support, and the only consequence of
        the stale count is a rotation that fires marginally early.

        Args:
          None.

        Returns:
          None.

        Raises:
          OSError: If the descriptor cannot be redirected or the file cannot be reopened.
          ValueError: If the stream has no usable descriptor. ``_fork`` absorbs and announces
            either, since a child that cannot strand its buffer still has working locks.
        """
        if self._closed:
            return
        self._stream = _reopen_discarding(self._stream, self._path, self._encoding)

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
            self._closed = True
            self._stream.flush()
            self._stream.close()

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
        """Returns the monotonic-clock deadline for the next time-based rotation (SPEC-031).

        Monotonic rather than wall-clock for the reason ``Span.start_ts`` is: a backward step —
        an NTP correction, a container clock sync — larger than the interval would otherwise
        defer every time-based rotation until wall-clock caught up, silently defeating this
        class's promise to bound on-disk growth. Nothing here is a timestamp anyone reads; it is
        only ever compared against another reading of the same clock.

        Args:
          None.

        Returns:
          The deadline as a ``time.monotonic()`` reading, or ``None`` when there is no time
          trigger.

        Raises:
          None.
        """
        if self._interval_seconds is None:
            return None
        return time.monotonic() + self._interval_seconds

    def _should_rotate(self, incoming: int) -> bool:
        """Decides whether to rotate before writing the next event.

        The time trigger reads ``time.monotonic()``, the same clock :meth:`_schedule_next`
        wrote the deadline on; comparing the two clocks is what the SPEC-031 fix removes.

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
        return self._next_rollover is not None and time.monotonic() >= self._next_rollover

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
