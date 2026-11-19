"""Result types for the two public calls that answered five questions with one bit (SPEC-034)."""

from __future__ import annotations

from dataclasses import dataclass

__all__ = ["ContinueResult", "FlushResult"]


@dataclass(frozen=True, kw_only=True)
class _Result:
    """A verdict that reads as a boolean and can say why (SPEC-034 FR-007).

    ``flush()`` returned one bit for five distinct outcomes — timed out, worker retired, drain
    thread died, queue too full for the marker, a batch abandoned — and a Lambda handler needs
    "the worker is retired, my code is wrong" separated from "the sink is slow". A ``NamedTuple``
    cannot be retrofitted here: a non-empty tuple is always truthy, so every ``if flush():``
    would silently start passing.

    The type exists now, before ``1.0.0``, precisely so that later reasons are additive. It grows
    by new ``reason`` values and by appended keyword fields, and never by changing
    :meth:`__bool__` (SPEC-089 FR-004).

    Attributes:
      ok: Whether the operation succeeded, and what ``bool()`` reports.
      reason: A short stable token naming *why* when it did not, or ``None`` when it did. New
        tokens may appear in any release; code should branch on ``bool()`` and treat an unknown
        reason as "some other failure" rather than matching exhaustively.
    """

    ok: bool
    reason: str | None = None

    def __bool__(self) -> bool:
        """Reports the verdict, so every existing ``if flush():`` keeps its meaning.

        Args:
          None.

        Returns:
          Whether the operation succeeded.

        Raises:
          None.
        """
        return self.ok


@dataclass(frozen=True, kw_only=True)
class FlushResult(_Result):
    """What :func:`log_foundry.flush` returns.

    ``reason`` is ``None`` on success. The tokens it can carry today are ``"timed-out"``,
    ``"retired"``, ``"thread-died"``, ``"queue-full"``, ``"abandoned"``, ``"sink-flush"`` and
    ``"re-entrant"``.

    ``"re-entrant"`` is SPEC-097's (FR-003, FR-004): the call was made where it would wait on
    itself, so it returned at once having done nothing — on the background drain thread, from a
    sink's ``emit`` or a ``sink_factory`` the worker built; from a signal handler or a finalizer
    that interrupted the library inside one of its lock sections, or inside the library's own
    ``flush()`` wait on its marker; or inside a span sweep's hold, from a collector callback, where
    it reported ``"abandoned"`` before SPEC-097. Its counts are zero.

    ``"sink-flush"`` is the one SPEC-036 added (FR-002 AC-8) and it is worth distinguishing: the
    queue drained cleanly and the **sink's own client buffer** did not, so the events are past
    this library and inside a driver. ``"abandoned"`` is the neighbouring case where this call
    could not confirm they were handed over. It has **four** producers — a drain that spent its
    retries, an expiring ``shutdown()`` answering pessimistically, a marker that arrived after
    that, and one that never reached the queue at all because it was full — so it means "not
    confirmed delivered" and never "confirmed lost" (SPEC-050 FR-001). New tokens may appear in
    any release, which is what this type exists for — branch on ``bool()``.

    **A truthy result says the drain that carried the events finished and abandoned no batch
    while the call was outstanding, not that the sink accepted them** (SPEC-089). A settled run that
    lost events does not make the result falsy; the loss is counted in the five fields below
    instead, and :attr:`lost` is their total. Each field is named for the ``health().events``
    term it counts, and is the change in that term over the call's **window**: from just before
    ``flush()`` sweeps the open spans (every registered span since SPEC-091) to the moment the
    drain answers the call's marker (or, where no drain answered it, to when the call reads for
    itself). The
    counts are process-wide over that window, so they **over-count**: they include other
    threads' events that were ahead of the marker, other threads' submissions that overflowed
    while the call waited, and any orphan log's ``sink_lost`` in the window. A non-zero count
    says the process lost events while the call ran, not that this caller's were among them. A
    zero count covers only the terms the ledger counts: a loss a sink takes inside an ``emit``
    that returned (a failed Kafka delivery report, a ``MultiSink`` child that raised, an
    ``emit``-only sink's own internal loss) is in none of them and stays in ``health().sink``,
    and so are events still pending when the window ends and events on spans the call did not
    sweep. ``repr()`` and ``==`` include the counts, so a result whose window lost something is
    not equal to ``FlushResult(ok=True)``.

    Attributes:
      overflow_lost: Events whose submission met a full queue in the window, the sweep's own
        hand-over included.
      batch_lost: Events of a batch abandoned after its retries, or whose retry was declined,
        in the window.
      sink_lost: Events of a run that settled in the window and ended neither ``ACCEPTED`` nor
        ``SKIPPED``, an orphan log's included.
      drain_lost: Events the drain thread held when it died in the window.
      stranded: Events left in the queue of a drain that ended in the window: its thread ended,
        or it had debited the last of what it held.
    """

    overflow_lost: int = 0
    batch_lost: int = 0
    sink_lost: int = 0
    drain_lost: int = 0
    stranded: int = 0

    @property
    def lost(self) -> int:
        """The events the process lost in the call's window: the sum of the five counts.

        Args:
          None.

        Returns:
          ``overflow_lost + batch_lost + sink_lost + drain_lost + stranded``.

        Raises:
          None.
        """
        return (
            self.overflow_lost + self.batch_lost + self.sink_lost + self.drain_lost + self.stranded
        )


@dataclass(frozen=True)
class ContinueResult(_Result):
    """What :func:`log_foundry.continue_trace` returns.

    ``reason`` is ``None`` on success. Today it distinguishes ``"nothing-supplied"`` — no
    argument carried a context — from ``"rejected"``, which means something *was* supplied and
    was malformed. Those two read identically as ``False``, and the second is a caller bug where
    the first is often a deliberate "continue if there is one to continue".
    """
