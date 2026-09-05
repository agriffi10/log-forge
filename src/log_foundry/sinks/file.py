"""FileSink + RotatingFileSink — durable local NDJSON on disk (arch §8, SPEC-008)."""

from __future__ import annotations

import json
import os
import threading
import time
from typing import TextIO

from log_foundry import _diag
from log_foundry.sinks._retry import require_positive
from log_foundry.sinks.base import SinkDeliveryError

__all__ = ["FileSink", "RotatingFileSink"]

_WHEN_SECONDS = {
    "S": 1,
    "M": 60,
    "H": 60 * 60,
    "D": 60 * 60 * 24,
}


def _rollover_seconds(when: str | None, interval: int) -> float | None:
    """Translates a unit code and interval into a rollover period in seconds.

    A module-level function rather than a ``@staticmethod``, which ``python.md`` §9 forbids and
    which this was the only instance of in the package (SPEC-049 FR-007).

    Args:
      when: The unit code, or ``None`` for no time trigger.
      interval: How many units make up one period.

    Returns:
      The period in seconds, or ``None`` when there is no time trigger.

    Raises:
      ValueError: If the unit code is unrecognized, so a typo fails loudly at construction rather
        than silently at runtime, or if the interval is not positive. The interval is checked even
        when there is no time trigger, because an interval that means nothing is a caller who
        believes one is armed (SPEC-049 FR-003).
    """
    require_positive(interval, "interval", "RotatingFileSink")
    if when is None:
        return None
    try:
        unit = _WHEN_SECONDS[when.upper()]
    except KeyError:
        raise ValueError(
            f"invalid 'when' unit {when!r}; expected one of {sorted(_WHEN_SECONDS)}"
        ) from None
    return unit * interval


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


    It keeps **no** client buffer (SPEC-036 FR-002): ``emit`` flushes the stream before it
    returns, so nothing of this sink's is left pending between calls.
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

    def reacquire_after_fork(self) -> None:
        """Re-opens the file so this child holds its own descriptor (SPEC-039 FR-004, SPEC-042).

        ``emit`` writes a whole batch into a **buffered** stream and flushes once at the end, so
        a fork landing inside it leaves both processes holding the same pending bytes and both
        writing them: measured, the event at the fork point appeared on disk twice. Without a
        ``before`` handler there is nowhere to empty the buffer from (FR-001), so the child
        strands it instead — the parent's copy is untouched and still reaches disk exactly once.

        No lock is taken, for the reason ``_lifecycle._rebuild_worker_after_fork`` gives: there is
        one thread here by construction, and the lock was re-initialised moments earlier, so
        taking it could only wait on a holder that cannot exist. A hook that blocks here blocks a
        child that has not yet returned from ``fork``, where no watchdog can reach it.

        A **closed** sink returns without re-acquiring anything, which is a trivially true claim
        rather than an empty one: there is no transport left to hold, so nothing a later close
        could destroy. Stated because ``sinks/base.py`` says returning normally *is* the claim,
        and this is the one shipped sink that can return having done nothing.

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


    It keeps **no** client buffer (SPEC-036 FR-002): ``emit`` flushes the stream before it
    returns, so nothing of this sink's is left pending between calls.
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
          interval: How many units make up one rollover period. **Refused when non-positive**
            (SPEC-049 FR-003), and refused even when ``when`` is ``None``, where it is inert: an
            interval that means nothing is a caller who believes a time trigger is armed. Zero or
            negative put the rollover deadline permanently in the past, so ``_should_rotate`` fired
            on every event — measured, three emits of five events left **two** lines on disk out of
            fifteen.

            ``max_bytes`` and ``backup_count`` are **floored at zero rather than refused**, which
            is the other half of SPEC-049 FR-001's rule. A negative of either *works* today:
            ``_should_rotate`` tests ``self._max_bytes > 0`` and ``_rotate`` tests
            ``self._backup_count > 0``, so it behaves exactly as the documented ``0`` — nothing is
            lost, nothing raises, no counter moves. Refusing a configuration that works would be a
            breaking change; flooring is a no-op that makes the value legible.

        Returns:
          None.

        Raises:
          ValueError: If the unit code is unrecognized, or the interval is not positive.
          OSError: If the file cannot be opened.
        """
        self._path = path
        self._encoding = "utf-8"
        self._max_bytes = max(max_bytes, 0)
        self._backup_count = max(backup_count, 0)
        self._interval_seconds = _rollover_seconds(when, interval)
        self._stream: TextIO = open(path, "a", encoding=self._encoding)
        self._size = os.path.getsize(path) if os.path.exists(path) else 0
        self._next_rollover = self._schedule_next()
        self._rotation_failing = False
        self._closed = False
        self._lock = threading.Lock()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Appends each event, rotating first whenever a size or time trigger fires (FR-002).

        The lock spans the whole batch, so the decide-rotate-write-account sequence is
        indivisible (SPEC-028 FR-002). Guarding only ``_rotate`` would not be enough: the
        ``_should_rotate`` check and the write that follows it must see the same stream, or a
        rotation between them sends the line to a closed handle.

        **The batch is flushed before a rotation is attempted** (SPEC-048 FR-006). ``_rotate``
        begins by closing the stream, which flushes it, while this loop otherwise flushes once at
        the end — so under the canonical rotation failure, a full or read-only filesystem, it is
        that flush that raises and the batch's buffered lines are gone before any rename is tried.
        Flushing here means every event of the batch is on disk before the rotation can fail, at
        every one of ``_rotate``'s raise sites rather than only at the renames. A flush that
        raises *here* is deliberately not absorbed: nothing was written, so it is the
        genuinely-total failure the worker's retry exists for.

        Args:
          batch: The events to write.

        Returns:
          None.

        Raises:
          OSError: If a write or a flush fails. A failed *rotation* no longer raises; see
            :meth:`_rotate_or_continue`.
          SinkDeliveryError: If the sink is closed.
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
                    self._stream.flush()
                    self._rotate_or_continue()
                self._stream.write(line)
                self._size += data
            self._stream.flush()

    def reacquire_after_fork(self) -> None:
        """Re-opens the file so this child holds its own descriptor (SPEC-039 FR-004, SPEC-042).

        Identical to :meth:`FileSink.reacquire_after_fork` and measured on this class
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

    def _rotate_or_continue(self) -> None:
        """Rotates, or absorbs the failure and carries on writing to the un-rotated file.

        A rotation that raised used to cost the batch twice. ``_rotate`` closes the active stream
        first, so the events already written in this batch were on disk and the ``OSError``
        propagated out of ``emit`` — the worker then re-sent the whole batch and wrote them again.
        Measured: an 8-event batch failing after 3 were written put 11 lines on disk, 3 of them
        duplicates. A persistent failure was worse: the sink kept a **closed** stream and every
        later batch raised a raw ``PermissionError``, which is not a ``SinkDeliveryError`` and has
        no ``losses()`` behind it.

        Absorbing it costs nothing and duplicates nothing. The active file simply exceeds
        ``max_bytes`` until a rotation succeeds, which is what happens anyway when rotation is
        impossible — the trade SPEC-027 FR-004 already took, that a leaked resource beats a
        corrupt write.

        ``_next_rollover`` is re-armed as well as ``_size``: ``_rotate`` sets it on its last line,
        so an absorbed failure would otherwise leave a **time** trigger permanently in the past.

        **The re-arm does not damp the size trigger, and the diagnostic is what carries that.**
        ``_size`` is re-seeded from a file that is now over ``max_bytes``, so ``_should_rotate``'s
        size branch stays true and every subsequent event attempts a rotation again — measured at
        598 attempts over 600 events. The attempts are cheap and lose nothing, but an unthrottled
        stderr write per event is not: ``PostgresSink._reconnect_if_broken`` records the same rule
        for the same reason, that a diagnostic which floods is one an operator stops reading. So
        the failure is announced **once per outage** and the flag clears on the next successful
        rotation. The remaining per-event attempt is recorded in ``architecture.md`` §12 rather
        than fixed here, because damping it means deferring a rotation the caller asked for.

        **The reopen can itself raise, and that is not absorbed.** It is the same
        ``open(self._path, "a")`` call ``_rotate`` ends with, so whatever defeats it there —
        a read-only mount, ``EMFILE``, a directory that lost write permission — defeats it here.
        At that point this sink has no stream and cannot continue, so there is nothing to absorb
        *into*; the ``OSError`` reaches ``emit`` and the worker retries the batch, duplicating the
        prefix the pre-rotation flush had already written. That residue is recorded rather than
        fixed: it is unchanged from before SPEC-048, the surviving events are on disk rather than
        lost, and inventing a half-open state to avoid a duplicate would trade a visible
        duplication for a silent loss.

        Args:
          None.

        Returns:
          None.

        Raises:
          OSError: If the *reopen* fails, per the paragraph above. A failed **rotation** is
            absorbed and announced through ``_diag``; the batch continues, nothing is dropped, no
            counter moves, and this class still has no ``losses()``.
        """
        try:
            self._rotate()
        except OSError as err:
            self._stream = open(self._path, "a", encoding=self._encoding)
            self._size = os.path.getsize(self._path) if os.path.exists(self._path) else 0
            self._next_rollover = self._schedule_next()
            if not self._rotation_failing:
                self._rotation_failing = True
                _diag.absorbed("rotating RotatingFileSink", err)
            return
        self._rotation_failing = False

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
