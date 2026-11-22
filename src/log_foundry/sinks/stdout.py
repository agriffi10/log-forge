"""StdoutSink and StderrSink — JSON lines to a stream, on the delivery engine (SPEC-084)."""

from __future__ import annotations

import errno
import io
import json
import sys
from typing import TYPE_CHECKING, Final, TextIO

from log_foundry import _diag
from log_foundry.sinks import _engine
from log_foundry.sinks._classify import is_retryable
from log_foundry.sinks._driver_error import delivery_error
from log_foundry.sinks._encode import encode_round, unencodable_in_run
from log_foundry.sinks._engine import When, emit_via_deliver, run_context
from log_foundry.sinks._rounds import Lost, mark_sent, raised_line
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict

if TYPE_CHECKING:
    import threading

    from log_foundry.sinks.base import SinkLosses

__all__ = ["StderrSink", "StdoutSink"]

_RETRYABLE_ERRNOS: Final = frozenset(
    code
    for name in ("ENOSPC", "EDQUOT", "EFBIG", "EIO", "EAGAIN")
    if (code := getattr(errno, name, None)) is not None
)
"""The ``errno`` values a resend to a stream can cure, each skipped where absent (SPEC-084 FR-003).

Space can be freed (``ENOSPC``), a quota or a file-size limit raised (``EDQUOT``, ``EFBIG``), and an
I/O error or a would-block may pass (``EIO``, ``EAGAIN``, which ``BlockingIOError`` from a
non-blocking stream carries). ``EMFILE`` and ``ENFILE``, which the file sinks retry, are left out
because a stream sink opens nothing, and ``EINTR`` never reaches the sink because CPython retries it
internally (PEP 475). Anything else, ``EPIPE`` above all, fails the same way on the next attempt.
"""

_PRE: Final = "pre"
"""The round's stage while its pre-flush runs."""

_WRITE: Final = "write"
"""The round's stage while its lines are handed to the stream, one ``write`` each."""

_FINAL: Final = "final"
"""The round's stage while its final ``flush()`` runs."""


def _unbuffered(stream: object) -> bool:
    """Reports whether the stream is exactly the shape ``python -u`` gives ``sys.stdout``.

    That shape is a ``TextIOWrapper`` with ``write_through`` set over a raw ``FileIO``: each
    ``write`` is one ``write(2)``, so on a blocking descriptor a ``write`` that returned put its
    line in the kernel, and one that raised took nothing. On a non-blocking descriptor a ``write``
    that would block returns having written nothing, and ``TextIOWrapper`` ignores that, so a
    returned ``write`` may not have landed; no raise happens there, so this check is never asked
    and the sink cannot tell. The types are compared exactly, never with ``isinstance``, so no
    caller code runs and a subclass, which may buffer as it likes, gets the general rule. The round
    reads it only after a call raised, so a clean round pays nothing.

    Args:
      stream: The sink's stream.

    Returns:
      True only for that exact shape.

    Raises:
      None. Both attributes are read from exact CPython types, whose accessors do not raise on a
        closed or a detached stream.
    """
    return (
        type(stream) is io.TextIOWrapper
        and bool(stream.write_through)
        and type(stream.buffer) is io.FileIO
    )


class StdoutSink:
    """Writes each event as one JSON line to a stream, reporting each by what the stream took.

    This is the zero-dependency default sink, for local dev and container log scraping. Like
    every sink it receives already-built event dicts and knows nothing about spans or context.

    **It delivers through the engine** (SPEC-084). A round flushes the stream first (the
    *pre-flush*), hands it one ``write`` per encodable line, then flushes once. A line the stream
    was handed is never handed again: after a raise the stream does not say whether it kept or
    dropped what it had not yet written, so a handed line is ``UNKNOWN``, counted in ``failed`` as
    an upper bound and announced as one that may have landed, never resent. A line not yet handed
    is ``RETRYABLE`` when a resend can cure the error and ``REJECTED`` otherwise. Two cases are
    exact: a raise that is not an ``OSError``, or is ``io.UnsupportedOperation``, took nothing,
    and on the unbuffered shape ``python -u`` gives ``sys.stdout``, over a blocking descriptor, a
    ``write`` that returned landed and one that raised took nothing. A **fault latch** makes a sustained fault fail: once
    a round leaves a handed line ``UNKNOWN``, later rounds call such lines ``REJECTED`` until a
    round's final flush returns. Only the batch during which a fault begins is counted rather than
    raised; every later one fails.

    What it cannot close, because it cannot see inside a stream it does not own: a stream that
    discards the tail of a line it cut leaves a torn prefix the next line joins; a short write on
    the unbuffered shape tears a line called ``ACCEPTED``, and over a non-blocking descriptor a
    write that would block writes nothing and drops whole lines called ``ACCEPTED``; the latch can over-count a line a
    partial write did land, and hand such a line to a catcher that resends whole; another
    writer's fault can drop this sink's pending lines, which its returned flush then calls
    ``ACCEPTED``; and a forked child's flush writes the parent's kept bytes too.

    It takes **no** transport lock (SPEC-028 FR-002), and the honest reason is that locking it
    was out of scope there rather than that it needs none: a round writes one line per event, so
    two threads' *batches* do interleave, exactly as they did in ``FileSink`` before its lock.
    Individual lines survive — ``TextIOWrapper.write`` holds its own lock — but line integrity is
    not the guarantee at stake, as ``test_file_sink_keeps_each_batch_contiguous_under_concurrent
    _emitters`` records. Interleaved batches are harmless in a line-oriented stream that a
    scraper reads per line, which is what this sink is for; a caller needing contiguity should
    use ``FileSink``. The latch is one plain ``bool`` that every section reads and writes without
    a lock, which the Python rulebook's §13 warns against relying on: here any interleaving of two
    sections' writes leaves a valid latch, set by a fault or cleared by a clean round, and the
    worst a race costs is one round judged under the other state.

    It **adds no post-close guard** (SPEC-032 FR-003), because ``close()`` only flushes — the
    stream belongs to the process, not to this sink, so a later batch still lands.

    It keeps **no** client buffer (SPEC-036 FR-002): every round flushes the stream before it
    returns, so the sink itself keeps nothing between calls, but the stream may still hold lines a
    run reported ``UNKNOWN``.

    It encodes the whole batch before its first write, once per run (SPEC-069 FR-001), so an
    event it cannot encode costs only itself, ``DROPPED`` and reported through :meth:`losses`.

    Attributes:
      log_foundry_counters: The engine's counters for this sink, which :meth:`losses` reads.
      log_foundry_stop_signal: The event the engine's waits are cut short by, set by the library.
    """

    def __init__(self, stream: TextIO | None = None) -> None:
        """Binds the sink to an output stream, once, at construction.

        The binding is deliberate and permanent for the life of the sink: a later
        ``contextlib.redirect_stdout`` or a test's capture of ``sys.stdout`` is not honoured,
        because the attribute was resolved here. Passing ``stream=`` explicitly is how a caller
        — a test above all — captures the output (SPEC-031 FR-003).

        It also builds the engine's counters, whose lock is theirs rather than this sink's, and
        clears the fault latch. Nothing here blocks or takes another lock, which is why the
        library can build the default sink under its config lock.

        Args:
          stream: The stream to write to, defaulting to ``sys.stdout`` as resolved now.

        Returns:
          None.

        Raises:
          None.
        """
        self._stream = stream if stream is not None else sys.stdout
        self._faulted = False
        self.log_foundry_counters = SinkCounters()
        self.log_foundry_stop_signal: threading.Event | None = None

    def losses(self) -> SinkLosses | None:
        """Reports the engine's totals for this sink, once it has any (SPEC-069, SPEC-084).

        ``dropped`` is the events the encode step could not encode, counted only by a run that
        settled; ``failed`` the events that did not land, each once per run, an upper bound: a
        handed line reported ``UNKNOWN``, or ``REJECTED`` under the latch, may still have landed.
        ``None`` until something is counted, so a process that only logs clean events still
        reads ``health().sink is None``.

        Args:
          None.

        Returns:
          ``None`` while nothing is counted, else the counters.

        Raises:
          None.
        """
        found = self.log_foundry_counters.losses()
        if found.dropped == 0 and found.failed == 0 and found.resent == 0:
            return None
        return found

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Delivers the batch through the engine inline, keeping the 1.x ``emit`` contract.

        One round, as 1.x's ``emit`` was one call. It returns when the run settled — anything was
        accepted, or a handed line's fate is unknown — with what did not land counted in
        ``failed`` and not written again. It raises when the run failed: nothing was accepted and
        no handed line's fate is unknown, which includes a round whose handed lines the fault
        latch made ``REJECTED``. So on a buffered stream the batch during which a fault begins
        returns where 1.x raised, and later batches raise. It raises as 1.x did: the stream's own
        error, as an instance of both ``SinkDeliveryError`` and its own class, so
        ``except OSError``, ``except BrokenPipeError`` and ``except SinkDeliveryError`` all still
        catch it.

        Args:
          batch: The events to write. An empty batch is a no-op.

        Returns:
          None.

        Raises:
          Exception: What the stream raised, when the run failed, as an instance of
            ``SinkDeliveryError`` too with the original as its ``__cause__`` — or the original
            itself, unchanged, if the dual-typed exception cannot be built.
        """
        emit_via_deliver(self, batch)

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        """Runs one round and reports each event's fate by index (SPEC-084 FR-001).

        Args:
          batch: The events.

        Returns:
          The round's verdict.

        Raises:
          None. A ``BaseException`` that is not an ``Exception`` propagates from the stream.
        """
        return _engine.one_round(self._round, batch)

    def _round(self, batch: list[dict[str, object]]) -> Verdict:
        """Pre-flushes, writes the round's lines and flushes, judging each by what was handed.

        Every event is encoded once per run (:func:`~log_foundry.sinks._encode.encode_round`), an
        unencodable one ``DROPPED`` with SPEC-069's line, and a round of nothing encodable touches
        the stream not at all. Every encodable event is marked sent before the pre-flush, so a
        line's attempt count includes a round whose pre-flush raised.

        **The pre-flush** means a stream holding bytes from an earlier fault raises before
        anything of this round is handed, so this round's lines are resendable rather than
        handed and ambiguous. One handler covers the three stages, and it asks the predicate
        itself; the stage and the count of writes that returned are all it needs to judge.

        Args:
          batch: The events this round is offered.

        Returns:
          A verdict with one outcome per event of ``batch``.

        Raises:
          None. A ``BaseException`` that is not an ``Exception`` propagates from the stream.
        """
        pairs = encode_round(self, batch, self._encode, sink=_diag.type_name(self))
        fates = [Outcome.DROPPED] * len(batch)
        if not pairs:
            return Verdict(outcomes=tuple(fates))
        mark_sent(self, [batch[position] for position, _ in pairs])
        stream = self._stream
        stage = _PRE
        handed = 0
        try:
            stream.flush()
            stage = _WRITE
            for _, line in pairs:
                stream.write(line)
                handed += 1
            stage = _FINAL
            stream.flush()
        except Exception as err:
            retryable = is_retryable(self._retryable_error, err)
            self._lose(batch, pairs, fates, err, retryable=retryable, stage=stage, handed=handed)
            return Verdict(outcomes=tuple(fates))
        self._faulted = False
        for position, _ in pairs:
            fates[position] = Outcome.ACCEPTED
        return Verdict(outcomes=tuple(fates))

    def _lose(
        self,
        batch: list[dict[str, object]],
        pairs: list[tuple[int, str]],
        fates: list[Outcome],
        error: Exception,
        *,
        retryable: bool,
        stage: str,
        handed: int,
    ) -> None:
        """Settles a round a stream call raised in, by which call raised and what it raised.

        The general rule (SPEC-084 FR-002): a raising pre-flush handed nothing; an ``OSError``
        from the ``write`` of line ``i`` may have taken part or all of it, so lines up to and
        including ``i`` were handed; any other exception from that ``write`` took nothing, since
        the ``io`` contract raises ``ValueError`` before any I/O and an encoding error comes
        before the binary layer, so lines before ``i`` were handed. ``io.UnsupportedOperation``,
        which a stream opened for reading raises, is both an ``OSError`` and a ``ValueError`` with
        no ``errno``, and is raised before any I/O, so it too took nothing. A raising final flush
        leaves every line handed. A handed line is ``UNKNOWN``, or ``REJECTED`` while the latch
        is set, and sets the latch; a line not handed is ``RETRYABLE`` or ``REJECTED`` as the
        predicate answers. **On the unbuffered shape** each ``write`` is one ``write(2)``, so
        every line whose ``write`` returned is ``ACCEPTED``, whatever was raised, and nothing is
        handed ambiguously.

        One line per group through ``Lost``: the ``UNKNOWN`` lines and the latched ones each
        ``ALWAYS`` under a key of their own, ending ``; they may have landed`` as the HTTP
        family's unknown line does, and the rest ``LAST`` when retried and ``ALWAYS`` otherwise.
        A run that fails on this round counts its unencodable events in ``failed`` with the rest
        (``When.FAILED``), and the inline adapter raises the stream's error as both its own type
        and ``SinkDeliveryError``, or the original where that cannot be built.

        Args:
          batch: The round's batch.
          pairs: The round's encoded lines by position, in batch order.
          fates: The round's outcomes, filled in here.
          error: What the stream raised.
          retryable: Whether the sink's predicate retries it.
          stage: Which call raised: the pre-flush, a ``write`` or the final flush.
          handed: How many ``write`` calls returned before the raise.

        Returns:
          None.

        Raises:
          None.
        """
        rest = Outcome.RETRYABLE if retryable else Outcome.REJECTED
        accepted = 0
        unsure = 0
        if stage == _WRITE:
            if _unbuffered(self._stream):
                accepted = handed
            elif _diag.is_a(error, OSError) and not _diag.is_a(error, io.UnsupportedOperation):
                unsure = handed + 1
            else:
                unsure = handed
        elif stage == _FINAL:
            if _unbuffered(self._stream):
                accepted = len(pairs)
            else:
                unsure = len(pairs)
        latched = self._faulted
        if unsure:
            self._faulted = True
        unknown = Outcome.REJECTED if latched else Outcome.UNKNOWN
        handed_positions: list[int] = []
        lost: list[int] = []
        for index, (position, _) in enumerate(pairs):
            if index < accepted:
                fates[position] = Outcome.ACCEPTED
            elif index < unsure:
                fates[position] = unknown
                handed_positions.append(position)
            else:
                fates[position] = rest
                lost.append(position)
        context = run_context()
        name = _diag.type_name(self)
        name_of = _diag._STR_STR(_diag._STORED_NAME(type(error)))
        kind = f"{name_of} {_diag.errno_of(error)}".rstrip()
        whole = unencodable_in_run(self)
        context.count(self.log_foundry_counters, "failed_events", whole, when=When.FAILED)
        losses = Lost(self)
        if handed_positions:
            suffix = "; after an earlier fault" if latched else "; outcome unknown"
            losses.add(
                [batch[position] for position in handed_positions],
                when=When.ALWAYS,
                key=f"{kind} {'latched' if latched else 'unknown'}",
                line=raised_line("event", name, kind, f"{suffix}; they may have landed"),
                whole=0 if lost else whole,
            )
        if lost:
            losses.add(
                [batch[position] for position in lost],
                when=When.LAST if retryable else When.ALWAYS,
                key=kind,
                line=raised_line("event", name, kind),
                whole=whole,
            )
        losses.register()
        built = delivery_error(error)
        context.raise_on_failure(error if built is None else built)

    def _retryable_error(self, err: Exception) -> bool:
        """Reports whether a stream's error is worth another round (SPEC-084 FR-003, SPEC-070).

        Args:
          err: The exception the round met.

        Returns:
          True only for an ``OSError`` whose ``errno`` a resend can cure
          (:data:`_RETRYABLE_ERRNOS`). An ``errno`` of ``None``, ``EPIPE``, a ``ValueError`` from a
          closed stream and a ``UnicodeEncodeError`` are not.

        Raises:
          None. :func:`~log_foundry.sinks._classify.is_retryable` reads a raise here as False.
        """
        return _diag.is_a(err, OSError) and err.errno in _RETRYABLE_ERRNOS

    def _encode(self, event: dict[str, object]) -> str:
        """Encodes one event as its newline-terminated JSON line.

        Args:
          event: The event to encode.

        Returns:
          The line to write.

        Raises:
          TypeError: If the event is not JSON-serializable, which ``encode_round`` turns into a
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
          Exception: Whatever the stream raises on flush, with nothing counted.
        """
        self._stream.flush()


class StderrSink(StdoutSink):
    """The :class:`~log_foundry.sinks.stdout.StdoutSink` shape, defaulting to stderr (FR-004).

    It writes each event as one ``json.dumps`` line and flushes, exactly like ``StdoutSink``,
    whose engine round, fault latch, predicate and counters it inherits (SPEC-084) — only the
    default stream differs, following the twelve-factor convention of logs on stderr and app
    output on stdout.

    It keeps **no** client buffer (SPEC-036 FR-002): every round flushes the stream before it
    returns, so the sink itself keeps nothing between calls, but the stream may still hold lines a
    run reported ``UNKNOWN``.

    Attributes:
      log_foundry_counters: The engine's counters for this sink, inherited from ``StdoutSink``.
      log_foundry_stop_signal: The event the engine's waits are cut short by, set by the library.
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
