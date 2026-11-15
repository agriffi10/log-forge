"""FileSink + RotatingFileSink — durable local NDJSON on disk (arch §8, SPEC-008)."""

from __future__ import annotations

import codecs
import errno
import io
import json
import os
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Protocol

from log_foundry import _diag
from log_foundry.sanitize import _int_text
from log_foundry.sinks import _engine
from log_foundry.sinks._classify import is_retryable
from log_foundry.sinks._driver_error import delivery_error
from log_foundry.sinks._encode import encode_round, unencodable_in_run
from log_foundry.sinks._engine import When, emit_via_deliver, run_context
from log_foundry.sinks._guard import ClosedGuard, GuardMode
from log_foundry.sinks._refusal import final_refusal
from log_foundry.sinks._retry import floor_count, require_positive
from log_foundry.sinks._rounds import Lost, mark_sent, raised_line
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict

if TYPE_CHECKING:
    import threading

    from log_foundry.sinks.base import SinkLosses

__all__ = ["FileSink", "RotatingFileSink"]

_WHEN_SECONDS = {
    "S": 1,
    "M": 60,
    "H": 60 * 60,
    "D": 60 * 60 * 24,
}

_RETRY_ERRNOS: Final = frozenset(
    code
    for name in ("ENOSPC", "EDQUOT", "EFBIG", "EIO", "EAGAIN", "EMFILE", "ENFILE")
    if (code := getattr(errno, name, None)) is not None
)
"""The ``errno`` values a resend can cure, each skipped on a platform that lacks it (SPEC-082).

Space can be freed (``ENOSPC``), a quota or a file-size limit raised (``EDQUOT``, ``EFBIG``), a
descriptor released (``EMFILE``, ``ENFILE``), and an I/O error or a would-block may pass
(``EIO``, ``EAGAIN``). Nothing else is retried: a permission or a read-only mount fails the same
way on the next attempt.
"""

_TORN: Final = "torn"
"""The run-notes key holding ``(event, tail)`` for the occurrence a write of this run cut."""


def _rollover_seconds(when: str | None, interval: int) -> float | None:
    """Translates a unit code and interval into a rollover period in seconds.

    A module-level function rather than a ``@staticmethod``, which ``python.md`` §9 forbids and
    which this was the only instance of in the package (SPEC-049 FR-007).

    The interval ``unit * interval`` multiplies is the exact ``int`` :func:`require_positive`
    returns (SPEC-065 FR-004), not the caller's object: a subclass whose ``__rmul__`` returned
    ``0`` gave a period of ``0`` when the return was discarded, measured at ``71e42dd``.

    The period must convert to a ``float``, because the deadline is ``time.monotonic()`` plus it:
    measured at ``7dc881f``, ``when="S", interval=10**400`` raised a raw ``OverflowError: int too
    large to convert to float`` out of that sum *after* the file had been opened, so the refusal
    left the file on disk and its stream unclosed, and named neither ``interval`` nor the class
    (SPEC-066). So the conversion is tried here, before anything is opened, and a period that
    overflows is refused with ``ValueError`` naming ``interval``. Every period that converts is
    still accepted, a century or a googol of seconds alike: a long period is a time trigger
    that does not fire, which works.

    Args:
      when: The unit code, or ``None`` for no time trigger.
      interval: How many units make up one period, a positive ``int``.

    Returns:
      The period in seconds, or ``None`` when there is no time trigger.

    Raises:
      TypeError: If the interval is not an ``int``.
      ValueError: If the unit code is unrecognized, so a typo fails loudly at construction rather
        than silently at runtime, or if the interval is not positive. The interval is checked even
        when there is no time trigger, because an interval that means nothing is a caller who
        believes one is armed (SPEC-049 FR-003). Also if the period is too large to convert to a
        ``float``, checked only when there is a time trigger, since only then is it summed.
    """
    interval = require_positive(interval, "interval", "RotatingFileSink")
    if when is None:
        return None
    try:
        unit = _WHEN_SECONDS[when.upper()]
    except KeyError:
        raise ValueError(
            f"invalid 'when' unit {when!r}; expected one of {sorted(_WHEN_SECONDS)}"
        ) from None
    period = unit * interval
    try:
        float(period)
    except OverflowError:
        raise ValueError(
            f"RotatingFileSink interval is too large: {_int_text(interval)} units of {when!r} "
            f"is beyond the largest float number of seconds"
        ) from None
    return period


@dataclass(eq=False, slots=True)
class _Tail:
    """The unwritten remainder of a line a write cut partway (SPEC-082 FR-002).

    Its prefix is on disk, so nothing else may reach the file before these bytes do. The record
    belongs to the sink, and any section finishes it; the run that cut it keeps the same object
    in its notes and learns whether it is still pending by comparing identities, which is why
    equality is identity here (``eq=False``) and the record shrinks in place.

    Attributes:
      remaining: The bytes still to write; shortened by what each write call takes.
    """

    remaining: bytes


class _Owner(Protocol):
    """What a write sequence reads and writes on the file sink it serves.

    Attributes:
      _tail: The pending tail, or ``None`` when no line is cut.
      log_foundry_counters: The engine's counters, which a failed run's count registers against.
    """

    _tail: _Tail | None
    log_foundry_counters: SinkCounters

    @property
    def _stream(self) -> io.FileIO:
        """The unbuffered handle, read at each use because a rotation rebinds it.

        Args:
          None.

        Returns:
          The handle.

        Raises:
          None.
        """

    def _baseline(self) -> int:
        """Returns the sink's own byte bookkeeping as it stands before a write sequence.

        Args:
          None.

        Returns:
          A value :meth:`_follow` reads back.

        Raises:
          None.
        """

    def _follow(self, baseline: int, taken: int) -> None:
        """Sets the sink's own byte bookkeeping to ``baseline`` moved by ``taken`` bytes.

        An assignment from the two arguments, never an increment, so running it twice with the
        same arguments leaves what running it once leaves.

        Args:
          baseline: What :meth:`_baseline` returned before the sequence's first write call.
          taken: The bytes the sequence's write calls took.

        Returns:
          None.

        Raises:
          None.
        """


def _curable(err: Exception) -> bool:
    """Reports whether a resend can cure what a file sink's handle raised (SPEC-082 FR-004).

    Args:
      err: The exception.

    Returns:
      True only for an ``OSError`` whose ``errno`` is in :data:`_RETRY_ERRNOS`. An ``errno`` of
      ``None`` and an exception that is not an ``OSError`` at all are not.

    Raises:
      None. A caller asks through ``is_retryable``, which reads a raise here as False.
    """
    return isinstance(err, OSError) and err.errno in _RETRY_ERRNOS


def _taken(counts: list[int], size: int) -> int:
    """Adds up the counts a write sequence stored, stopping at the first that is not a count.

    A count is an ``int`` greater than zero and no greater than the bytes still unwritten. A
    handle that returns anything else, ``None`` or zero among them, has not said what it took,
    so the sum stops there and :func:`_take` ends the sequence.

    Args:
      counts: What each write call of the sequence returned, in order.
      size: The length of the bytes the sequence set out to write.

    Returns:
      The bytes the write calls took.

    Raises:
      None.
    """
    total = 0
    for count in counts:
        if not isinstance(count, int) or not 0 < count <= size - total:
            break
        total += count
    return total


def _take(stream: io.FileIO, data: bytes, counts: list[int]) -> None:
    """Writes ``data`` until every byte is taken or a write call stops, storing each call's count.

    **Each count is stored by the call that made it** (SPEC-082 FR-002). ``counts.extend(map(...))``
    appends what ``stream.write`` returned inside C, with no bytecode boundary between the write
    returning and its count being in the list. A signal handler that raises (a
    ``KeyboardInterrupt``, the ``signal.alarm`` idiom's ``TimeoutError``) is delivered at a
    bytecode boundary, so with ``wrote = stream.write(chunk)`` it lands after the call returned
    and before the assignment, and the count is lost: measured, a blocking write to a full pipe
    that a signal cut short returned its partial count and lost it every time. Nothing here
    reads the count before it is stored.

    Whatever is raised in the loop propagates unchanged, an ``OSError`` from a write call, any
    other ``Exception`` and a ``BaseException`` alike, with ``counts`` holding every count stored
    before it: the caller brings its state to ``counts`` on that path as on the normal one. A
    write call that returns something that is not a count (:func:`_taken`) has not said what it
    took, so the sequence ends there as a write that would block, which keeps the loop from
    spinning on a handle that takes nothing.

    Args:
      stream: The unbuffered handle.
      data: The bytes to write.
      counts: The list each write call's return is appended to; empty when the sequence starts.

    Returns:
      None.

    Raises:
      OSError: ``EAGAIN``, when a write call returned no count.
      BaseException: Whatever a write call, or a signal handler running in the loop, raised.
    """
    size = len(data)
    taken = 0
    while taken < size:
        counts.extend(map(stream.write, (data[taken:] if taken else data,)))
        reached = _taken(counts, size)
        if reached == taken:
            raise OSError(errno.EAGAIN, "the write call took no bytes")
        taken = reached


def _shorten(owner: _Owner, tail: _Tail, data: bytes, baseline: int, counts: list[int]) -> None:
    """Brings a tail, and the sink's own bookkeeping, to the counts its write calls stored.

    The one step of a tail's write sequence (SPEC-082 FR-002). Every write is an assignment
    computed from ``data`` and ``counts``, so it is idempotent: :func:`_finish_tail` runs it once
    after the sequence and once more if anything passed through the sequence or the first run.

    Args:
      owner: The sink.
      tail: The record the sequence set out to finish.
      data: The tail's bytes as they stood before the sequence's first write call.
      baseline: The sink's bookkeeping before that call.
      counts: What each write call returned.

    Returns:
      None.

    Raises:
      None.
    """
    taken = _taken(counts, len(data))
    owner._follow(baseline, taken)
    tail.remaining = data[taken:]
    if not tail.remaining:
        owner._tail = None


def _finish_tail(owner: _Owner) -> None:
    """Writes the pending tail, if any, shortening it by what was taken.

    Called first in every write section and by the release a ``close()`` runs, because the
    tail's prefix is on disk and any other byte appended after it would join it into one line
    that does not parse. The record shrinks in place and ``owner._tail`` is cleared only when
    nothing remains, so a run holding the same record still finds it pending by identity.

    The one step, :func:`_shorten`, runs after the write calls, and once more if anything was
    raised by them or inside its first run, so the tail and the sink's bookkeeping follow the
    stored counts whatever passed through. It covers the exception that passes through; a second
    one landing while the step runs for the first is not covered.

    Args:
      owner: The sink.

    Returns:
      None. No tail is pending afterwards.

    Raises:
      BaseException: What stopped the tail's writes, with part of it still pending, raised
        after the tail and the sink's bookkeeping were brought to the counts stored before it.
    """
    tail = owner._tail
    if tail is None:
        return
    data = tail.remaining
    counts: list[int] = []
    baseline = owner._baseline()
    try:
        _take(owner._stream, data, counts)
        _shorten(owner, tail, data, baseline, counts)
    except BaseException:
        _shorten(owner, tail, data, baseline, counts)
        raise


def _release(owner: _Owner) -> None:
    """Tries the pending tail once, then closes the handle whatever that try did.

    What both sinks hand their closed guard, which runs it once with the lock held. The tail is
    tried because a ``close()`` is the last section this process runs on the file, and a line
    left cut there stays cut. A tail that cannot be finished does not keep the descriptor open.

    Args:
      owner: The sink.

    Returns:
      None.

    Raises:
      BaseException: What stopped the tail's write, an ``OSError`` for a failed write call,
        raised after the handle is closed; or what the close itself raised.
    """
    try:
        _finish_tail(owner)
    finally:
        owner._stream.close()


def _owes_mark(stream: io.FileIO, mark: bytes) -> bool:
    """Reports whether a handle just opened owes its file a byte order mark.

    A mark is owed only when the codec has one, the handle is seekable and it stands at offset 0
    when opened, which is the test 1.x's text stream made: an empty regular file gets one mark,
    a non-empty one none, and a path that cannot seek, a FIFO, none. The handle is closed if
    asking it raises, so a construction or a fork hook that fails here leaks no descriptor.

    Args:
      stream: The handle, as ``open`` returned it.
      mark: The codec's byte order mark, empty when it has none.

    Returns:
      Whether the first line to reach the file leads with the mark.

    Raises:
      OSError: If the handle's offset cannot be read; the handle is closed first.
    """
    try:
        return bool(mark) and stream.seekable() and stream.tell() == 0
    except BaseException:
        stream.close()
        raise


class _Writes:
    """One round's writes: the pending tail first, then lines, each judged by the bytes taken.

    Built inside a sink's guarded section, after the round's events are encoded, and used by
    that sink's own ``_round``, which holds the ``except`` that asks its predicate. A method
    here raises what stopped it only after the sink's state and this round's outcomes follow the
    counts the write calls stored, so the handler settles a round that is already consistent.

    **The torn occurrence belongs to the run** (SPEC-082 FR-002). When a write of this run cut a
    line, the run's notes hold the event object and the :class:`_Tail` cut from it. In a later
    round that occurrence is the first offered event that is that object: every position before
    a tear was ``ACCEPTED`` or ``DROPPED`` in the tearing round, both final, so the engine
    offers none of them again and offers the rest in their original order. It is read here once,
    before any write call, and judged by identity against the sink's pending tail whenever it is
    needed: still that record, its tail is pending; any other, the tail was finished, by this
    round or by another section.
    """

    __slots__ = ("_batch", "_context", "_fates", "_landed", "_own", "_owner", "_positions", "_torn")

    def __init__(self, owner: _Owner, batch: list[dict[str, object]], positions: list[int]) -> None:
        """Starts over the round's encodable positions, reading the run's torn occurrence.

        Args:
          owner: The sink.
          batch: The round's batch.
          positions: The positions of the events that encoded, in batch order.

        Returns:
          None.

        Raises:
          None.
        """
        self._owner = owner
        self._batch = batch
        self._positions = positions
        self._context = run_context()
        self._fates = [Outcome.DROPPED] * len(batch)
        self._landed: set[int] = set()
        self._torn: int | None = None
        self._own: tuple[int, _Tail, object] | None = None
        note = self._context.notes(owner).get(_TORN)
        if isinstance(note, tuple):
            event, tail = note
            for position in positions:
                if batch[position] is event:
                    self._own = (position, tail, note)
                    break

    def _account_own(self) -> None:
        """Settles this run's torn occurrence from the sink's pending tail, idempotently.

        Its tail still pending, the occurrence is this round's torn event. Its tail finished, by
        this round's own tail write or by another section, the occurrence has landed whole: it
        is ``ACCEPTED`` with nothing written for it, and the run's note is dropped so a later
        occurrence of the same object is written as a line of its own. Called after the tail's
        write and again before a round is settled as lost, so an exception between the two
        leaves nothing half-decided.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        if self._own is None:
            return
        position, tail, note = self._own
        if tail is self._owner._tail:
            self._torn = position
            return
        self._landed.add(position)
        notes = self._context.notes(self._owner)
        if notes.get(_TORN) is note:
            del notes[_TORN]

    def finish_tail(self) -> None:
        """Writes the pending tail before any other byte, then settles the torn occurrence.

        When the tail's writes are stopped, the round's handler settles the occurrence instead
        (:meth:`lose`), so it is judged against the sink's tail on that path too: this run's
        occurrence whose own tail was already finished is ``ACCEPTED`` even while another run's
        tail is pending and cannot be written, or it would be offered and written whole again.

        Args:
          None.

        Returns:
          None.

        Raises:
          BaseException: What stopped the tail's writes, when a tail is still pending. The round
            then writes nothing else, and no rotation is tried.
        """
        _finish_tail(self._owner)
        self._account_own()

    def rest(self, pairs: list[tuple[int, bytes]]) -> list[tuple[int, bytes]]:
        """Returns the lines still to write, in batch order.

        Args:
          pairs: The round's encoded lines by position.

        Returns:
          Those whose position has not landed.

        Raises:
          None.
        """
        if not self._landed:
            return pairs
        return [pair for pair in pairs if pair[0] not in self._landed]

    def write(self, lines: list[tuple[int, bytes]], lead: bytes = b"") -> None:
        """Writes the lines as one run of bytes and judges each by the bytes taken.

        One join and, on a healthy file, one write call for the whole batch. The judging is the
        sequence's one step (:meth:`_judge`): it runs after the write calls, and once more if an
        exception passed through them or through its first run, so the tail, the sink's own
        bookkeeping and this round's outcomes follow the stored counts whatever was raised.

        Args:
          lines: ``(position, line)`` in batch order; none while a tail is pending.
          lead: Bytes owed before the first line, a byte order mark, counted with that line.

        Returns:
          None.

        Raises:
          BaseException: What stopped the write calls, after the lines are judged.
        """
        if not lines:
            return
        owner = self._owner
        data = lead + b"".join([line for _, line in lines])
        counts: list[int] = []
        baseline = owner._baseline()
        try:
            _take(owner._stream, data, counts)
            self._judge(lines, len(lead), data, baseline, counts)
        except BaseException:
            self._judge(lines, len(lead), data, baseline, counts)
            raise

    def _judge(
        self,
        lines: list[tuple[int, bytes]],
        lead: int,
        data: bytes,
        baseline: int,
        counts: list[int],
    ) -> None:
        """Brings the sink's state and this round's outcomes to the counts the calls stored.

        A line whose last byte lies below the bytes taken has landed. The line the count falls
        strictly inside is torn: its unwritten remainder becomes the sink's tail, and the run's
        notes record the event and that record. A count that stops on a line boundary tears
        nothing. The lead is counted with the first line, so a write that takes part of a byte
        order mark tears the first line and the rest of the mark is in the tail. Every write
        here is an assignment computed from the arguments, so the step is idempotent.

        Args:
          lines: The lines the sequence wrote, by position.
          lead: The length of the bytes written before the first line.
          data: The bytes the sequence set out to write.
          baseline: The sink's bookkeeping before the first write call.
          counts: What each write call returned.

        Returns:
          None.

        Raises:
          None.
        """
        owner = self._owner
        taken = _taken(counts, len(data))
        owner._follow(baseline, taken)
        landed = len(lines)
        torn: int | None = None
        if taken < len(data):
            landed = 0
            start = 0
            end = lead
            for position, line in lines:
                end += len(line)
                if end > taken:
                    if start < taken:
                        tail = _Tail(data[taken:end])
                        owner._tail = tail
                        self._context.notes(owner)[_TORN] = (self._batch[position], tail)
                        torn = position
                    break
                landed += 1
                start = end
        self._landed.update([position for position, _ in lines[:landed]])
        self._torn = torn

    def lose(self, error: Exception, retryable: bool, name: str) -> None:
        """Settles every position that did not land as lost to ``error``, and announces it once.

        The lost events are ``RETRYABLE`` when the sink's predicate retries the error and
        ``REJECTED`` otherwise. **The torn event is the exception** (SPEC-082 FR-002): it is
        ``RETRYABLE`` only when the error is retried and the round is inside an engine run and
        is not that run's last, so a later round will be offered it and finish its tail. In
        every other case it is ``UNKNOWN``: on a run's last round, under an error no resend
        cures, and in a direct ``deliver`` outside any run, whose notes end with the call. Its
        prefix is on disk and the next section writes its tail, so it may land; reported lost it
        could fail the run, and a catcher that resends whole would write the line again.

        One line through ``Lost``, as SPEC-074's sinks write it: ``LAST`` for a retried error and
        ``ALWAYS`` otherwise, naming the class, the exception's type and its ``errno`` when it
        carries one. A run that fails on this round counts its unencodable events in ``failed``
        with the rest, as 1.x's ``emit`` counted a failed batch whole (``When.FAILED``), and the
        inline adapter raises the error as both ``SinkDeliveryError`` and its own type
        (``raise_on_failure``), or the original where that dual type cannot be built.

        Args:
          error: What stopped the round.
          retryable: Whether the sink's ``_retryable_error`` retries it.
          name: The sink's class name, written on the line.

        Returns:
          None.

        Raises:
          None.
        """
        self._account_own()
        lost = [position for position in self._positions if position not in self._landed]
        if not lost:
            return
        context = self._context
        again = retryable and context._in_run() and not context.last
        for position in lost:
            self._fates[position] = Outcome.RETRYABLE if retryable else Outcome.REJECTED
        if self._torn is not None:
            self._fates[self._torn] = Outcome.RETRYABLE if again else Outcome.UNKNOWN
        kind = f"{type(error).__name__} {_diag.errno_of(error)}".rstrip()
        whole = unencodable_in_run(self._owner)
        context.count(self._owner.log_foundry_counters, "failed_events", whole, when=When.FAILED)
        losses = Lost(self._owner)
        losses.add(
            [self._batch[position] for position in lost],
            when=When.LAST if retryable else When.ALWAYS,
            key=kind,
            line=raised_line("event", name, kind),
            whole=whole,
        )
        losses.register()
        built = delivery_error(error)
        context.raise_on_failure(error if built is None else built)

    def verdict(self) -> Verdict:
        """Returns the round's verdict: landed positions ``ACCEPTED``, unencodable ``DROPPED``.

        Args:
          None.

        Returns:
          One outcome per event of the round's batch.

        Raises:
          None.
        """
        for position in self._landed:
            self._fates[position] = Outcome.ACCEPTED
        return Verdict(outcomes=tuple(self._fates))


class FileSink:
    """A :class:`~log_foundry.sinks.base.Sink` that appends events as NDJSON to one file.

    Not every deployment ships to a cloud queue; local dev, debugging, air-gapped hosts and
    simple archival just want events on the local disk. Writes are synchronous stdlib calls
    only.

    Writers within the process are serialized on a lock, because ``emit`` may be called
    concurrently (SPEC-028 FR-002) — this module claimed a single worker thread until that spec
    measured the orphan path emitting on application threads at the same time. Cross-*process*
    coordination remains out of scope: two processes appending to one path are on their own.

    **It delivers through the engine and reports each event by the bytes the file took**
    (SPEC-082). ``deliver`` writes a round's lines through an unbuffered binary handle and adds
    up what each write call returned: a line taken whole is ``ACCEPTED``, and the engine resends
    only what did not land, where 1.x raised and the worker wrote the whole batch again over the
    lines that had landed. A line a write cut partway is finished, once, before anything else
    reaches the file (:class:`_Tail`). A run of one round, an inline ``emit`` or a ``MultiSink``
    child, has no later round: the events after a failure there are counted, not written.

    It keeps **no** client buffer (SPEC-036 FR-002): the handle is unbuffered, so a write call
    that returned has handed its bytes to the operating system and nothing of this sink's is
    left pending between calls but a tail, which the next section writes first.

    Attributes:
      log_foundry_counters: The engine's counters for this sink, which :meth:`losses` reads.
      log_foundry_stop_signal: The event the engine's waits are cut short by, set by the library.
    """

    def __init__(self, path: str, *, encoding: str = "utf-8") -> None:
        """Opens the file for appending, unbuffered.

        Opening at construction means a missing parent directory surfaces immediately, at
        ``configure`` time, rather than on the first flush. The file is created if absent and
        appended to — never truncated — if it already exists.

        The encoding is checked before the file is opened, by building the text layer 1.x's
        ``open()`` built: a codec that is unknown, or is not a text encoding (``"base64"``,
        which an incremental encoder would accept), is refused with the ``LookupError`` 1.x
        raised, and a refused construction leaves no file behind. A byte order mark is owed only
        on a seekable handle at offset 0 (:func:`_owes_mark`).

        Args:
          path: The file to append to.
          encoding: The text encoding to write in.

        Returns:
          None.

        Raises:
          LookupError: If the encoding is unknown or is not a text encoding (SPEC-082).
          OSError: If the file cannot be opened.
        """
        self._path = path
        self._encoding = encoding
        self._codec = io.TextIOWrapper(io.BytesIO(), encoding=encoding).encoding
        self._bom = codecs.getincrementalencoder(self._codec)().encode("")
        self._stream: io.FileIO = open(path, "ab", buffering=0)
        self._bom_owed = _owes_mark(self._stream, self._bom)
        self._tail: _Tail | None = None
        self._guard = ClosedGuard(GuardMode.SERIAL)
        self.log_foundry_counters = SinkCounters()
        self.log_foundry_stop_signal: threading.Event | None = None

    @property
    def _closed(self) -> bool:
        """Whether a close has begun: the 1.x name, read-only, answered by the guard (SPEC-076).

        A 1.x subclass reading ``self._closed`` keeps working. It reads true from the moment a
        close begins, while that close still waits, which is the guard's ``closed``; assigning it
        raises ``AttributeError``, since the guard owns the state.

        Args:
          None.

        Returns:
          The guard's ``closed``.

        Raises:
          None.
        """
        return self._guard.closed

    @property
    def _lock(self) -> threading.Lock:
        """The guard's transport lock under its 1.x name, read-only (SPEC-076 FR-005).

        A 1.x subclass taking ``with self._lock:`` around its own work serialises with the
        guard's sections and its close, as it did with the 1.x lock; assigning it raises
        ``AttributeError``.

        Args:
          None.

        Returns:
          The lock each admitted section and the close hold.

        Raises:
          None.
        """
        return self._guard._lock

    @property
    def _counter_lock(self) -> threading.Lock:
        """The counters' lock, which every commit and ``losses()`` read takes.

        The 1.x name, now the engine counters' own lock, so a ``losses()`` poll still never
        waits behind a write (SPEC-028 FR-003). Assigning it replaces the counters' lock, so a
        test installing a counting lock here counts every acquisition the engine makes
        (SPEC-074 FR-001).

        Args:
          None.

        Returns:
          The lock.

        Raises:
          None.
        """
        return self.log_foundry_counters._counter_lock

    @_counter_lock.setter
    def _counter_lock(self, lock: threading.Lock) -> None:
        """Replaces the counters' lock.

        Args:
          lock: The new lock.

        Returns:
          None.

        Raises:
          None.
        """
        self.log_foundry_counters._counter_lock = lock

    def losses(self) -> SinkLosses | None:
        """Reports the engine's totals for this sink, once it has any (SPEC-069, SPEC-082).

        ``dropped`` is the events the encode step could not encode, counted only by a run that
        settled; ``failed`` the events that did not land, each once per run and never once per
        worker attempt, an upper bound: a torn event reported ``UNKNOWN`` is counted and may
        still land when the next section finishes its line. ``None`` until something is counted,
        so a process that only logs clean events still reads ``health().sink is None``. Read
        under the counter lock, never the write lock (SPEC-028 FR-003).

        Args:
          None.

        Returns:
          ``None`` while nothing is counted, else the counters.

        Raises:
          None.
        """
        found = self.log_foundry_counters.losses()
        if not (found.dropped or found.failed):
            return None
        return found

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Delivers the batch through the engine inline, keeping the 1.x ``emit`` contract.

        One round, as 1.x's ``emit`` was one call. It returns when anything landed or a line was
        cut, with what did not land counted in ``failed`` and not written: the events after a
        failure are lost to this call, counted, where 1.x raised and a catcher that resent the
        batch whole wrote its head twice (SPEC-082 FR-002). It raises only when the round landed
        nothing, and then as 1.x did (SPEC-069 FR-004): the handle's own error as an instance of
        both ``SinkDeliveryError`` and its own class, so ``except OSError``,
        ``except PermissionError`` and ``except SinkDeliveryError`` all still catch it.

        Args:
          batch: The events to write. An empty batch is a no-op.

        Returns:
          None.

        Raises:
          OSError: When the round landed nothing because a write call failed, as an instance of
            ``SinkDeliveryError`` too, with the original as its ``__cause__`` — or the original
            itself, unchanged, if the dual-typed exception cannot be built. Any other
            ``Exception`` the handle raised is raised the same way, as both types.
          SinkDeliveryError: When the sink is closed, by identity, with nothing counted.
        """
        emit_via_deliver(self, batch)

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        """Runs one round and reports each event's fate by index (SPEC-082 FR-001).

        Args:
          batch: The events.

        Returns:
          The round's verdict.

        Raises:
          None. A ``BaseException`` that is not an ``Exception`` propagates from the handle.
        """
        return _engine.one_round(self._round, batch)

    def _round(self, batch: list[dict[str, object]]) -> Verdict:
        """Writes the round's lines under the lock and reports each by the bytes taken.

        Every event is encoded once per run and before the lock is taken
        (:func:`~log_foundry.sinks._encode.encode_round`), an unencodable one ``DROPPED`` with
        SPEC-069's line; a round of nothing encodable writes nothing, a pending tail included.
        A closed sink's round is a refusal carrying 1.x's closed error. The lock covers the
        whole round rather than each line (SPEC-028 FR-002), which keeps a batch contiguous in
        the file and a tail's bytes directly after its prefix.

        The pending tail goes first, then the lines as one run of bytes, an owed byte order mark
        leading the first. **Any** ``Exception`` raised in there settles the round from the
        bytes taken before it, not only an ``OSError`` from a write call: with ``OSError`` alone
        handled, a handle that took nine bytes and raised ``ValueError`` left a cut line no tail
        recorded, and a whole-batch resend glued the next line to it (SPEC-082 FR-001).

        Args:
          batch: The events this round is offered.

        Returns:
          A verdict with one outcome per event of ``batch``.

        Raises:
          None. A ``BaseException`` that is not an ``Exception`` propagates from the handle,
            after the tail and the owed mark were brought to the counts the write calls stored.
        """
        pairs = encode_round(self, batch, self._encode, sink=type(self).__name__)
        with self._guard.enter() as section:
            if not section:
                run_context().refuse(
                    final_refusal(
                        self, f"FileSink wrote none of {len(batch)} event(s): the sink is closed"
                    )
                )
                return Verdict.uniform(Outcome.REJECTED, len(batch))
            writes = _Writes(self, batch, [position for position, _ in pairs])
            if pairs:
                mark_sent(self, [batch[position] for position, _ in pairs])
                try:
                    writes.finish_tail()
                    writes.write(writes.rest(pairs), self._bom if self._bom_owed else b"")
                except Exception as err:
                    writes.lose(err, is_retryable(self._retryable_error, err), type(self).__name__)
            return writes.verdict()

    def _baseline(self) -> int:
        """Returns whether a byte order mark is owed, as a write sequence's baseline.

        Args:
          None.

        Returns:
          ``1`` while the mark is owed, else ``0``.

        Raises:
          None.
        """
        return int(self._bom_owed)

    def _follow(self, baseline: int, taken: int) -> None:
        """Keeps the byte order mark owed only while the handle has taken nothing.

        Any byte this handle has taken means the file's first bytes are spoken for: the mark
        went with them, or its remainder is in the tail. A sequence that took nothing leaves the
        mark owed, so a later round writes it.

        Args:
          baseline: Whether the mark was owed before the sequence's first write call.
          taken: The bytes the sequence's write calls took.

        Returns:
          None.

        Raises:
          None.
        """
        self._bom_owed = bool(baseline) and not taken

    def _retryable_error(self, err: Exception) -> bool:
        """Reports whether a failed write is worth another round (SPEC-082 FR-004, SPEC-070).

        Args:
          err: The exception the round met.

        Returns:
          True only for an ``OSError`` whose ``errno`` a resend can cure (:data:`_RETRY_ERRNOS`).

        Raises:
          None. :func:`~log_foundry.sinks._classify.is_retryable` reads a raise here as False.
        """
        return _curable(err)

    def _encode(self, event: dict[str, object]) -> bytes:
        """Encodes one event as its terminated JSON line, in the file's encoding.

        The bytes 1.x's text stream wrote for the line: the terminator is ``os.linesep``, which
        that stream translated ``"\\n"`` to, and the codec's byte order mark is cut from the
        front, since the mark leads the file and not each line. ``json.dumps`` output is ASCII,
        so encoding each line alone gives what the stream's incremental encoder gave.

        Args:
          event: The event to encode.

        Returns:
          The line's bytes.

        Raises:
          TypeError: If the event is not JSON-serializable, which ``encode_round`` turns into a
            drop of this one event. Neither source of events produces one: assembly coerces every
            value (``sanitize``), and a ``TransformSink`` checks and coerces what its function
            returns (SPEC-058 FR-001).
        """
        data = (json.dumps(event) + os.linesep).encode(self._codec)
        if self._bom:
            return data[len(self._bom) :]
        return data

    def reacquire_after_fork(self) -> None:
        """Drops the parent's tail and opens a descriptor of this child's own (SPEC-082 FR-005).

        A pending tail is the parent's: its prefix came from the parent's write, and the parent
        finishes it. A child that wrote it too would put those bytes on disk twice, so the tail
        is dropped first, before anything here can raise. The reopen is 1.x's (SPEC-039 FR-004,
        SPEC-042): it gives the child a descriptor of its own, in **append** mode since the file
        is shared with the parent, and picks up the currently active file. The handle is
        unbuffered, so nothing inherited is left to strand and the ``dup2`` step 1.x took is
        gone; the inherited object is closed, which closes only this child's copy of the
        descriptor and writes nothing.

        The new handle and whether it owes a byte order mark are both in hand before
        ``_stream`` is replaced, so nothing can raise between replacing it and closing the
        inherited one.

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
          OSError: If the file cannot be reopened, or the inherited handle cannot be closed.
            ``_fork`` absorbs and announces it; the child then keeps the inherited descriptor,
            with the parent's tail already dropped.
        """
        if self._guard.closed:
            return
        self._tail = None
        fresh = open(self._path, "ab", buffering=0)
        owed = _owes_mark(fresh, self._bom)
        inherited = self._stream
        self._stream = fresh
        self._bom_owed = owed
        inherited.close()

    def close(self) -> None:
        """Finishes a pending tail and closes the handle, with a second call a no-op (FR-001).

        Taking the same lock a round takes means a close waits for an in-flight write rather
        than pulling the handle out from under it (SPEC-028 FR-002).
        The lock is the guard's (SPEC-076 FR-002): once a close begins every later round is
        refused at once, the one round inside, if any, is waited for, and the release runs
        once; a second close waits for the first's release, then returns.

        Args:
          None.

        Returns:
          None.

        Raises:
          OSError: If the pending tail could not be finished, after the handle is closed
            anyway, or if the close itself fails.
        """
        self._guard.close(self._release)

    def _release(self) -> None:
        """Tries the pending tail once and closes the handle (:func:`_release`).

        :meth:`close` hands this to the guard, which runs it once, with the lock held and
        the state already ``CLOSED``.

        Args:
          None.

        Returns:
          None.

        Raises:
          OSError: If the tail's write or the close fails.
        """
        _release(self)


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
    a bounded ring and counts nothing. Neither ``dropped`` (defined as discarded *before*
    attempting delivery) nor ``failed`` fits an event that was written to disk. ``losses()``
    reports only an event it could not encode and an event that did not land; retention moves
    neither counter.

    A rotation rebinds the active handle, so it is the sink where concurrent writers did real
    damage: a second thread mid-write could write to the handle rotation had just closed, or
    to the pre-rotation file it had already renamed away. Both are serialized on a lock
    (SPEC-028 FR-002).

    **It delivers through the engine, as** :class:`FileSink` **does** (SPEC-082): each event is
    reported by the bytes the file took, a line cut partway is finished before anything else
    reaches its file and before any rotation, and the engine resends only what did not land.

    It keeps **no** client buffer (SPEC-036 FR-002): the handle is unbuffered, so a write call
    that returned has handed its bytes to the operating system and nothing of this sink's is
    left pending between calls but a tail, which the next section writes first.

    Attributes:
      log_foundry_counters: The engine's counters for this sink, which :meth:`losses` reads.
      log_foundry_stop_signal: The event the engine's waits are cut short by, set by the library.
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

        The active file's byte size is tracked explicitly, in the bytes the write calls took,
        and it is seeded from any pre-existing file appended to.

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

            Both must be an ``int`` (SPEC-066 FR-002), judged before the file is opened, so a
            refused construction leaves nothing on disk. A ``float`` was floored straight through:
            ``backup_count=2.5`` or ``inf`` raised out of ``range()`` on the first rollover *after*
            the stream was closed, so the sink was dead for the life of the process, ``nan``
            disabled backups, and ``max_bytes=nan`` disabled the size trigger. A ``bool`` or
            ``int`` subclass is read as the ``int`` it holds.

        Returns:
          None.

        Raises:
          TypeError: If ``max_bytes`` or ``backup_count`` is not an ``int`` (SPEC-066).
          ValueError: If the unit code is unrecognized, the interval is not positive, or the
            period it makes is too large to convert to a ``float`` number of seconds (SPEC-066) —
            each refused before the file is opened.
          OSError: If the file cannot be opened.
        """
        self._path = path
        self._encoding = "utf-8"
        self._max_bytes = floor_count(max_bytes, 0, "max_bytes", "RotatingFileSink")
        self._backup_count = floor_count(backup_count, 0, "backup_count", "RotatingFileSink")
        self._interval_seconds = _rollover_seconds(when, interval)
        self._stream: io.FileIO = open(path, "ab", buffering=0)
        self._size = os.path.getsize(path) if os.path.exists(path) else 0
        self._next_rollover = self._schedule_next()
        self._rotation_failing = False
        self._tail: _Tail | None = None
        self._guard = ClosedGuard(GuardMode.SERIAL)
        self.log_foundry_counters = SinkCounters()
        self.log_foundry_stop_signal: threading.Event | None = None

    @property
    def _closed(self) -> bool:
        """Whether a close has begun: the 1.x name, read-only, answered by the guard (SPEC-076).

        A 1.x subclass reading ``self._closed`` keeps working. It reads true from the moment a
        close begins, while that close still waits, which is the guard's ``closed``; assigning it
        raises ``AttributeError``, since the guard owns the state.

        Args:
          None.

        Returns:
          The guard's ``closed``.

        Raises:
          None.
        """
        return self._guard.closed

    @property
    def _lock(self) -> threading.Lock:
        """The guard's transport lock under its 1.x name, read-only (SPEC-076 FR-005).

        A 1.x subclass taking ``with self._lock:`` around its own work serialises with the
        guard's sections and its close, as it did with the 1.x lock; assigning it raises
        ``AttributeError``.

        Args:
          None.

        Returns:
          The lock each admitted section and the close hold.

        Raises:
          None.
        """
        return self._guard._lock

    @property
    def _counter_lock(self) -> threading.Lock:
        """The counters' lock, which every commit and ``losses()`` read takes.

        The 1.x name, now the engine counters' own lock, so a ``losses()`` poll still never
        waits behind a write (SPEC-028 FR-003). Assigning it replaces the counters' lock, so a
        test installing a counting lock here counts every acquisition the engine makes
        (SPEC-074 FR-001).

        Args:
          None.

        Returns:
          The lock.

        Raises:
          None.
        """
        return self.log_foundry_counters._counter_lock

    @_counter_lock.setter
    def _counter_lock(self, lock: threading.Lock) -> None:
        """Replaces the counters' lock.

        Args:
          lock: The new lock.

        Returns:
          None.

        Raises:
          None.
        """
        self.log_foundry_counters._counter_lock = lock

    def losses(self) -> SinkLosses | None:
        """Reports the engine's totals for this sink, once it has any (SPEC-069, SPEC-082).

        ``dropped`` is the events the encode step could not encode, counted only by a run that
        settled; ``failed`` the events that did not land, each once per run and never once per
        worker attempt, an upper bound: a torn event reported ``UNKNOWN`` is counted and may
        still land when the next section finishes its line. ``None`` until something is counted,
        so a process that only logs clean events still reads ``health().sink is None``. Read
        under the counter lock, never the write lock (SPEC-028 FR-003).

        Args:
          None.

        Returns:
          ``None`` while nothing is counted, else the counters.

        Raises:
          None.
        """
        found = self.log_foundry_counters.losses()
        if not (found.dropped or found.failed):
            return None
        return found

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Delivers the batch through the engine inline, keeping the 1.x ``emit`` contract.

        One round, rotating first whenever a size or time trigger fires (FR-002). It returns
        when anything landed or a line was cut, with what did not land counted in ``failed`` and
        not written (SPEC-082 FR-002), and raises only when the round landed nothing, as 1.x
        did (SPEC-069 FR-004): the handle's own error as an instance of both
        ``SinkDeliveryError`` and its own class.

        Args:
          batch: The events to write. An empty batch is a no-op.

        Returns:
          None.

        Raises:
          OSError: When the round landed nothing because a write call failed or a rotation could
            not reopen the file, as an instance of ``SinkDeliveryError`` too, with the original
            as its ``__cause__`` — or the original itself, unchanged, if the dual-typed
            exception cannot be built. Any other ``Exception`` the handle raised is raised the
            same way. A failed *rotation* does not raise; see :meth:`_rotate_or_continue`.
          SinkDeliveryError: When the sink is closed, by identity, with nothing counted.
        """
        emit_via_deliver(self, batch)

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        """Runs one round and reports each event's fate by index (SPEC-082 FR-003).

        Args:
          batch: The events.

        Returns:
          The round's verdict.

        Raises:
          None. A ``BaseException`` that is not an ``Exception`` propagates from the handle.
        """
        return _engine.one_round(self._round, batch)

    def _round(self, batch: list[dict[str, object]]) -> Verdict:
        """Writes the round's lines under the lock, rotating first where a trigger fires.

        The lock spans the whole round, so the decide-rotate-write-account sequence is
        indivisible (SPEC-028 FR-002). Guarding only ``_rotate`` would not be enough: the
        ``_should_rotate`` check and the write that follows it must see the same handle, or a
        rotation between them sends the line to a closed one.

        The rotation decision stays per event, before that event's line, on the same triggers.
        Lines are staged until one is due a rotation, and **the staged lines are written, every
        write call for them returned, before the rotation is attempted** (SPEC-048 FR-006, as it
        reads on an unbuffered handle): a write that fails before the rotation point attempts no
        rotation, and when the reopen after a failed rotation raises, the lines written before
        it are already ``ACCEPTED`` and only the rest are lost, where 1.x resent the whole batch
        over them.

        **A rotation never starts while a tail is pending** (SPEC-082 FR-003). The tail is tried
        first, into the handle that holds its prefix, and a tail that cannot be finished ends
        the round before any trigger is asked.

        Every event is encoded once per run and before the lock is taken, exactly as
        :meth:`FileSink._round` does, and **any** ``Exception`` raised between the first write
        call and the last settles the round from the bytes taken before it.

        Args:
          batch: The events this round is offered.

        Returns:
          A verdict with one outcome per event of ``batch``.

        Raises:
          None. A ``BaseException`` that is not an ``Exception`` propagates from the handle,
            after the tail and ``_size`` were brought to the counts the write calls stored.
        """
        pairs = encode_round(self, batch, self._encode, sink=type(self).__name__)
        with self._guard.enter() as section:
            if not section:
                run_context().refuse(
                    final_refusal(
                        self,
                        f"RotatingFileSink wrote none of {len(batch)} event(s): the sink is closed",
                    )
                )
                return Verdict.uniform(Outcome.REJECTED, len(batch))
            writes = _Writes(self, batch, [position for position, _ in pairs])
            if pairs:
                mark_sent(self, [batch[position] for position, _ in pairs])
                try:
                    self._ensure_open()
                    writes.finish_tail()
                    staged: list[tuple[int, bytes]] = []
                    held = 0
                    for pair in writes.rest(pairs):
                        if self._should_rotate(len(pair[1]), held):
                            writes.write(staged)
                            staged, held = [], 0
                            self._rotate_or_continue()
                        staged.append(pair)
                        held += len(pair[1])
                    writes.write(staged)
                except Exception as err:
                    writes.lose(err, is_retryable(self._retryable_error, err), type(self).__name__)
            return writes.verdict()

    def _baseline(self) -> int:
        """Returns the active file's tracked size, as a write sequence's baseline.

        Args:
          None.

        Returns:
          ``_size`` before the sequence's first write call.

        Raises:
          None.
        """
        return self._size

    def _follow(self, baseline: int, taken: int) -> None:
        """Sets the tracked size to what it was plus the bytes the write calls took.

        So ``_size`` grows by the bytes the write calls returned, a tail's included, and never
        by the bytes the sink meant to write (SPEC-082 FR-003).

        Args:
          baseline: ``_size`` before the sequence's first write call.
          taken: The bytes the sequence's write calls took.

        Returns:
          None.

        Raises:
          None.
        """
        self._size = baseline + taken

    def _retryable_error(self, err: Exception) -> bool:
        """Reports whether a failed write or reopen is worth another round (SPEC-082, SPEC-070).

        Args:
          err: The exception the round met.

        Returns:
          True only for an ``OSError`` whose ``errno`` a resend can cure (:data:`_RETRY_ERRNOS`).

        Raises:
          None. :func:`~log_foundry.sinks._classify.is_retryable` reads a raise here as False.
        """
        return _curable(err)

    def _encode(self, event: dict[str, object]) -> bytes:
        """Encodes one event as its terminated JSON line, in UTF-8.

        The bytes 1.x's text stream wrote for the line, terminated by ``os.linesep``; the
        rotation check reads their length, so the size trigger counts what is written.

        Args:
          event: The event to encode.

        Returns:
          The line's bytes.

        Raises:
          TypeError: If the event is not JSON-serializable, which ``encode_round`` turns into a
            drop of this one event. Neither source of events produces one: assembly coerces every
            value (``sanitize``), and a ``TransformSink`` checks and coerces what its function
            returns (SPEC-058 FR-001).
        """
        return (json.dumps(event) + os.linesep).encode(self._encoding)

    def reacquire_after_fork(self) -> None:
        """Drops the parent's tail and opens a descriptor of this child's own (SPEC-082 FR-005).

        Identical to :meth:`FileSink.reacquire_after_fork`, less the byte order mark this class
        never writes: the tail is the parent's to finish, so it is dropped first, and the
        reopen in append mode picks up the active file after a parent's rotation.

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
          OSError: If the file cannot be reopened, or the inherited handle cannot be closed.
            ``_fork`` absorbs and announces it; the child then keeps the inherited descriptor,
            with the parent's tail already dropped.
        """
        if self._guard.closed:
            return
        self._tail = None
        fresh = open(self._path, "ab", buffering=0)
        inherited = self._stream
        self._stream = fresh
        inherited.close()

    def close(self) -> None:
        """Finishes a pending tail and closes the handle, with a second call a no-op (FR-002).

        Taking the same lock a round takes means a close waits for an in-flight write, and in
        particular never lands between a rotation and the write it was making room for
        (SPEC-028 FR-002).
        The lock is the guard's (SPEC-076 FR-002): once a close begins every later round is
        refused at once, the one round inside, if any, is waited for, and the release runs
        once; a second close waits for the first's release, then returns.

        Args:
          None.

        Returns:
          None.

        Raises:
          OSError: If the pending tail could not be finished, after the handle is closed
            anyway, or if the close itself fails.
        """
        self._guard.close(self._release)

    def _release(self) -> None:
        """Tries the pending tail once and closes the active handle (:func:`_release`).

        :meth:`close` hands this to the guard, which runs it once, with the lock held and
        the state already ``CLOSED``.

        Args:
          None.

        Returns:
          None.

        Raises:
          OSError: If the tail's write or the close fails.
        """
        _release(self)

    def _ensure_open(self) -> None:
        """Reopens the active file when a failed rotation left the sink holding a closed handle.

        ``_rotate`` closes the handle first and :meth:`_rotate_or_continue` reopens it when the
        rotation fails, but that reopen can fail too — a read-only mount, ``EMFILE`` — and the
        sink is left holding the handle ``_rotate`` closed. Every later write then raised a plain
        ``ValueError: I/O operation on closed file``: uncounted, with no line, not a
        ``SinkDeliveryError``, and permanent even after the cause cleared (SPEC-069 frame-2
        review). Reopening here, under the lock and before the first write, turns a failure into
        an ``OSError`` the round settles like any other, and a success into a recovered sink.
        The size is re-read from the file, as the absorbed-rotation path does. A handle double
        with no ``closed`` attribute reads as open. No tail is ever pending here: a rotation
        never starts with one.

        Args:
          None.

        Returns:
          None.

        Raises:
          OSError: If the file still cannot be opened.
        """
        if not getattr(self._stream, "closed", False):
            return
        self._stream = open(self._path, "ab", buffering=0)
        self._size = os.path.getsize(self._path) if os.path.exists(self._path) else 0

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

    def _should_rotate(self, incoming: int, held: int = 0) -> bool:
        """Decides whether to rotate before writing the next event.

        The time trigger reads ``time.monotonic()``, the same clock :meth:`_schedule_next`
        wrote the deadline on; comparing the two clocks is what the SPEC-031 fix removes.

        Args:
          incoming: The byte cost of the event about to be written.
          held: The bytes of this round staged ahead of it and not yet written, which the size
            trigger counts with the file's tracked size (SPEC-082).

        Returns:
          True when the size or time trigger has fired.

        Raises:
          None.
        """
        size = self._size + held
        if self._max_bytes > 0 and size > 0 and size + incoming > self._max_bytes:
            return True
        return self._next_rollover is not None and time.monotonic() >= self._next_rollover

    def _rotate_or_continue(self) -> None:
        """Rotates, or absorbs the failure and carries on writing to the un-rotated file.

        A rotation that raised used to cost the batch twice. ``_rotate`` closes the active handle
        first, so the events already written in this batch were on disk and the ``OSError``
        propagated out of ``emit`` — the worker then re-sent the whole batch and wrote them again.
        Measured: an 8-event batch failing after 3 were written put 11 lines on disk, 3 of them
        duplicates. A persistent failure was worse: the sink kept a **closed** stream and every
        later batch raised a raw ``PermissionError``, which is not a ``SinkDeliveryError`` and had
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

        **The reopen can itself raise, and that is not absorbed; it is reported per event.** It
        is the same ``open(self._path, "ab")`` call ``_rotate`` ends with, so whatever defeats it
        there — a read-only mount, ``EMFILE``, a directory that lost write permission — defeats
        it here. At that point this sink has no handle and cannot continue, so there is nothing
        to absorb *into*, and the ``OSError`` reaches the round. The round has already written
        the lines staged before the rotation, every write call returned, so they are
        ``ACCEPTED``, and only the event the rotation was for and those after it are
        ``RETRYABLE`` or ``REJECTED`` as the predicate answers (SPEC-082 FR-003). Before, the
        error left ``emit`` and the worker resent the whole batch over the prefix on disk; that
        residue is closed. The next round's :meth:`_ensure_open` reopens the file.

        Args:
          None.

        Returns:
          None.

        Raises:
          OSError: If the *reopen* fails, per the paragraph above. A failed **rotation** is
            absorbed and announced through ``_diag``; the batch continues, nothing is dropped, no
            counter moves: a failed rotation is not a loss, so ``losses()`` does not report it.
        """
        try:
            self._rotate()
        except OSError as err:
            self._stream = open(self._path, "ab", buffering=0)
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
        the active file is simply removed so reopening starts empty. The close flushes nothing:
        the handle is unbuffered, and the round wrote every staged line before calling this.

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
        self._stream = open(self._path, "ab", buffering=0)
        self._size = 0
        self._next_rollover = self._schedule_next()
