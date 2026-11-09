"""The delivery engine: bounded rounds over a sink's ``deliver``, counted once (SPEC-072)."""

from __future__ import annotations

import enum
import math
import threading
import time
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, cast

from log_foundry import _diag
from log_foundry.sinks import _retry
from log_foundry.sinks._classify import ErrorPredicate, is_retryable
from log_foundry.sinks._deferral import Deferral
from log_foundry.sinks.base import _ABSENT, SinkDeliveryError, _class_member, _static_value
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict, _bound_deliver, _probe

if TYPE_CHECKING:
    from collections.abc import Callable

    from log_foundry.sinks._deferral import _Counted

    type Deliver = Callable[[list[dict[str, object]]], object]

BACKOFF_BASE: Final = 0.1
"""The sinks' 1.x backoff within a cycle: the wait before attempt ``i + 1`` is ``0.1 * 2**i``.

It is the formula each sink module defining its own ``_BACKOFF_BASE`` applies, restarting
at ``i = 0`` in every cycle, written here once for every sink that adopts the engine.
"""

CYCLE_BACKOFF_BASE: Final = 0.01
"""The worker's 1.x wait between whole-batch attempts: ``min(0.01 * 2**c, CYCLE_BACKOFF_CAP)``."""

CYCLE_BACKOFF_CAP: Final = 0.5
"""The ceiling of :func:`cycle_backoff`, as ``Worker._emit`` capped it in 1.x."""

DEFAULT_MAX_RETRY_AFTER: Final = 30.0
"""The ceiling on a verdict's ``retry_after`` for a sink that declares no usable one.

The number ``HTTPSink`` carries today. A sink's own ``max_retry_after`` replaces it when its
type is exactly ``int`` or ``float``, so ``inf`` honours a hint in full, as 1.x ``HTTPSink`` does.
"""

_EXPONENT_CAP: Final = 60
"""The largest exponent either backoff takes, so no ``max_retries`` raises ``OverflowError``.

``0.1 * 2**60`` is far past ``_retry.MAX_WAIT`` and ``0.01 * 2**60`` far past the cycle cap, so
the bound never changes a wait the existing caps would not already have set. In 1.x the worker's
own loop computed ``0.01 * 2**attempt`` unbounded and raised ``OverflowError`` on the drain thread
from attempt 1025; its loop now calls :func:`cycle_backoff` too.
"""

_MAX_WAIT_INT: Final = int(_retry.MAX_WAIT)
""":data:`_retry.MAX_WAIT` as an ``int``, so a huge ``int`` hint is compared unconverted."""

_RAISED: Final = object()
"""The fate of an event in a round whose ``deliver`` raised an ``Exception``."""

_UNREAD: Final = object()
"""The fate of an event in a round whose verdict could not be read."""

_RESENDABLE_IF_IDEMPOTENT: Final = (Outcome.UNKNOWN, _UNREAD, _RAISED)
"""Fates resent only by an idempotent sink; ``_RAISED`` also asks the sink's predicate."""


@dataclass(frozen=True, kw_only=True)
class Delivery:
    """What one engine run did with a batch, by final fate, and whether the run failed.

    A run **fails** when no event was ``ACCEPTED`` (``SKIPPED`` is not accepted), at least one
    ended ``REJECTED``, unresolved or in a raising round, and none ended ``UNKNOWN`` or in an
    unreadable round. Otherwise it **settles**. Whoever receives a failed run owns the batch's
    count: the worker's abandon or declined line, the orphan path's ``orphan_lost``, or the
    adapter's raise and its catcher.

    Attributes:
      attempts: ``deliver`` calls made.
      cycles: Cycles started; the worker's abandon line names it as "emit attempts".
      accepted: Events that ended ``ACCEPTED``.
      rejected: Events that ended ``REJECTED``.
      dropped: Events that ended ``DROPPED``.
      unresolved: Events still ``RETRYABLE`` when the rounds ran out or one was declined.
      raised: Events in a round whose ``deliver`` raised, not resent.
      unknown: Events ``UNKNOWN`` or in an unreadable round, not resent.
      declined: Whether a further round was refused by the decline predicate.
      skipped: Events that ended ``SKIPPED``; in no engine counter.
      failed: Whether the run failed, by the rule above.
    """

    attempts: int
    cycles: int
    accepted: int
    rejected: int
    dropped: int
    unresolved: int
    raised: int
    unknown: int
    declined: bool
    skipped: int
    failed: bool


_SETTLED_EMPTY: Final = Delivery(
    attempts=0,
    cycles=0,
    accepted=0,
    rejected=0,
    dropped=0,
    unresolved=0,
    raised=0,
    unknown=0,
    declined=False,
    skipped=0,
    failed=False,
)
"""An empty batch's run: nothing called, nothing counted, settled."""


def cycle_backoff(cycle: int) -> float:
    """Returns the wait after whole-batch cycle ``cycle``: the worker's 1.x backoff, written once.

    ``Worker._emit``'s own ``emit`` loop calls this in place of its literal, so the engine and the
    1.x loop cannot drift apart. The exponent is bounded by :data:`_EXPONENT_CAP`.

    Args:
      cycle: The zero-based cycle that just ended; a negative reads as zero.

    Returns:
      ``min(CYCLE_BACKOFF_BASE * 2**cycle, CYCLE_BACKOFF_CAP)``.

    Raises:
      None.
    """
    return min(math.ldexp(CYCLE_BACKOFF_BASE, min(max(cycle, 0), _EXPONENT_CAP)), CYCLE_BACKOFF_CAP)


def _backoff(position: int) -> float:
    """Returns the within-cycle wait before attempt ``position + 1``, the sinks' 1.x backoff.

    Args:
      position: The zero-based attempt within the cycle that just failed.

    Returns:
      ``BACKOFF_BASE * 2**position`` with a bounded exponent; ``_retry.wait`` caps the wait.

    Raises:
      None.
    """
    return math.ldexp(BACKOFF_BASE, min(max(position, 0), _EXPONENT_CAP))


def _count(value: object) -> int:
    """Reads a retry count as an exact ``int`` floored at zero, else zero.

    Only an exact ``int`` counts, so a ``bool``, a ``float``, a ``str``, an ``int`` subclass and
    a property object all read as "declared none", which on every path gives the same rounds as
    zero. The floor matches the 1.x worker's ``max(max_retries, 0)``.

    Args:
      value: The sink's ``max_retries`` as read statically, or the worker's.

    Returns:
      The count.

    Raises:
      None.
    """
    if type(value) is int:
        return max(value, 0)
    return 0


def _seconds(value: object) -> float | None:
    """Reads a duration whose type is exactly ``int`` or ``float``, else ``None``.

    An ``int`` is brought into ``[-1, MAX_WAIT]`` by exact integer comparison before it is
    converted, because ``clamp_server_delay`` raises ``OverflowError`` on ``10**400``
    (``math.isinf`` converts it), and an exact-``int`` hint passes the type rule. A ``float`` is
    passed through, ``nan`` and ``inf`` included, for ``clamp_server_delay`` to answer.

    Args:
      value: A hint or a ceiling, of any type.

    Returns:
      The duration as a ``float``, or ``None`` when its type is not exactly ``int`` or ``float``.

    Raises:
      None.
    """
    kind = type(value)
    if kind is int:
        whole = cast("int", value)
        if whole > _MAX_WAIT_INT:
            return _retry.MAX_WAIT
        if whole < -1:
            return -1.0
        return float(whole)
    if kind is float:
        return cast("float", value)
    return None


def _ceiling(sink: object) -> float:
    """Returns the sink's ``max_retry_after`` when it is exactly an ``int`` or ``float``.

    Args:
      sink: The sink, read statically.

    Returns:
      The ceiling, or :data:`DEFAULT_MAX_RETRY_AFTER` when the sink declares none or a non-number.

    Raises:
      None.
    """
    found = _seconds(_static_value(sink, "max_retry_after"))
    return DEFAULT_MAX_RETRY_AFTER if found is None else found


def stop_signal_of(sink: object) -> threading.Event | None:
    """Returns the sink's ``log_foundry_stop_signal`` when it is a ``threading.Event``.

    Read statically, so a property is never called and reads as no event.

    Args:
      sink: The sink.

    Returns:
      The event, or ``None``.

    Raises:
      None.
    """
    found = _static_value(sink, "log_foundry_stop_signal")
    if issubclass(type(found), threading.Event):
        return cast("threading.Event", found)
    return None


def _idempotent(sink: object) -> bool:
    """Whether the sink declares ``log_foundry_idempotent`` exactly ``True``, read statically.

    Args:
      sink: The sink.

    Returns:
      True only for the ``True`` object; a truthy non-``bool`` is not a declaration.

    Raises:
      None.
    """
    return _static_value(sink, "log_foundry_idempotent") is True


def _predicate_of(sink: object) -> ErrorPredicate | None:
    """Returns the sink's ``_retryable_error`` ready to call, or ``None``.

    Found statically on an instance, a method comes back as the class's **unbound** function,
    which ``is_retryable`` would call one argument short: it raises inside the wrapper, answers
    False, and every predicate would refuse silently. So a predicate found on the class is bound
    through its type's ``__get__`` (which also unwraps a ``staticmethod``), as SPEC-070's
    contract hands the engine the bound ``sink._retryable_error``; one in the instance's own
    ``__dict__`` is used as found, as ordinary access would. A separate function so the unbound
    mutant can be pinned as a test.

    Args:
      sink: The sink.

    Returns:
      A callable predicate, or ``None`` when there is none or binding it raised.

    Raises:
      None.
    """
    try:
        found = _static_value(sink, "_retryable_error")
        if found is _ABSENT:
            return None
        member = _class_member(type(sink), "_retryable_error")
        if member is not None and member[0] is found:
            binder = _class_member(type(found), "__get__")
            if binder is not None:
                found = cast("Callable[..., object]", binder[0])(found, sink, type(sink))
        if not callable(found):
            return None
    except Exception:
        return None
    return cast("ErrorPredicate", found)


def outcomes_of(verdict: object, n: int) -> tuple[Outcome, ...] | None:
    """Returns a verdict's outcomes when it is well formed for ``n`` events, else ``None``.

    The engine's one definition of a malformed verdict, and its only reader of ``.outcomes``:
    not a ``Verdict``, outcomes that are not an exact tuple of exact ``Outcome`` members, a
    length other than ``n``, or a field that raises when read. Both fields are read inside the
    guard, so a ``retry_after`` that raises makes the verdict malformed too. SPEC-075's
    wrappers share it.

    Args:
      verdict: What ``deliver`` returned.
      n: How many events the round handed it.

    Returns:
      The exact tuple, or ``None``.

    Raises:
      None. A ``BaseException`` from a field still propagates.
    """
    try:
        if not issubclass(type(verdict), Verdict):
            return None
        held = cast("Verdict", verdict)
        outcomes = held.outcomes
        _ = held.retry_after
        if type(outcomes) is not tuple:
            return None
        if tuple.__len__(outcomes) != n:
            return None
        for item in tuple.__iter__(outcomes):
            if type(item) is not Outcome:
                return None
    except Exception:
        return None
    return outcomes


def _hint_of(verdict: object) -> float | None:
    """Reads an accepted verdict's ``retry_after`` as a usable number, or ``None``.

    Asked only after :func:`outcomes_of` accepted the verdict, so a malformed one never supplies
    a wait; read again under its own guard, since a subclass may answer differently twice.

    Args:
      verdict: A verdict :func:`outcomes_of` accepted.

    Returns:
      The hint as a ``float``, or ``None``.

    Raises:
      None.
    """
    try:
        value = cast("Verdict", verdict).retry_after
    except Exception:
        return None
    return _seconds(value)


def _snapshot(batch: list[dict[str, object]]) -> list[dict[str, object]]:
    """Copies the caller's batch once, so a ``deliver`` that mutates it cannot move an index.

    A ``list`` (a subclass included) is copied through ``list.__iter__``, so no subclass hook
    runs; anything else is iterated as given.

    Args:
      batch: The batch as handed to the engine.

    Returns:
      A new list of the same event objects.

    Raises:
      Exception: Whatever iterating a non-list batch raises.
    """
    if issubclass(type(batch), list):
        return list(list.__iter__(batch))
    return list(batch)


class When(enum.Enum):
    """When a run-context count or line commits, judged once the run has ended.

    Attributes:
      ALWAYS: Whatever the run's outcome.
      SETTLED: Only when the run settles; a per-event drop, which a failed run's catcher owns.
      LAST: Only when the round that registered it is the run's last, so a count of something a
        later round may still fix is made once, for the round that ended the run.
    """

    ALWAYS = "always"
    SETTLED = "settled"
    LAST = "last"


class RunContext:
    """One engine run's counts and lines, committed once when the run ends (SPEC-073 FR-002).

    The engine's own totals are events into ``SinkCounters``; a shipped sink's documented
    counters count other things — ``HTTPSink.failed`` counts requests — and must keep their 1.x
    names and units. So a sink registers its own counts here, each against a named owner, and the
    engine commits them through one ``Deferral`` per owner, under that owner's ``_counter_lock``
    read at commit time, then writes every committed line outside every lock, in registration
    order. It is family-neutral: nothing here names a sink type.

    Owners are kept by identity (``id``), never hashed or compared, so no third-party
    ``__hash__`` or ``__eq__`` runs. When its run ends the context is cleared, so it holds no sink
    afterwards and adds no global that holds one (invariant 12).

    An *immediate* context is what :func:`run_context` hands a caller outside any run: each count
    and line commits at once, as a one-round run that is its own last and settles.

    Attributes:
      round: The 1-based number of the round in progress.
    """

    __slots__ = ("_entries", "_immediate", "_lines", "_notes", "_resends_unknown", "_round")

    def __init__(self, *, immediate: bool = False) -> None:
        """Starts an empty context at round one.

        Args:
          immediate: Whether every count and line commits at once.

        Returns:
          None.

        Raises:
          None.
        """
        self._round = 1
        self._immediate = immediate
        self._notes: dict[int, tuple[object, dict[str, object]]] = {}
        self._entries: list[tuple[_Counted, str, int, When, int]] = []
        self._lines: list[tuple[Callable[[], None], When, int]] = []
        self._resends_unknown: bool | None = None

    @property
    def round(self) -> int:
        """The 1-based round in progress; a line naming attempts uses it.

        Args:
          None.

        Returns:
          The round number.

        Raises:
          None.
        """
        return self._round

    def notes(self, owner: object) -> dict[str, object]:
        """Returns the per-run, per-owner ``dict`` an owner keeps across the run's rounds.

        Args:
          owner: The object the notes belong to, matched by identity.

        Returns:
          The same ``dict`` on every call for that owner within this run.

        Raises:
          None.
        """
        found = self._notes.get(id(owner))
        if found is None:
            found = (owner, {})
            self._notes[id(owner)] = found
        return found[1]

    def count(self, owner: _Counted, name: str, n: int, *, when: When) -> None:
        """Registers ``n`` against ``owner.name``, committed when the run ends as ``when`` says.

        A zero is not recorded, so it can never make a commit take a lock for nothing.

        Args:
          owner: The object whose counter it is; the commit takes its ``_counter_lock``.
          name: The owner's counter attribute.
          n: How much to add.
          when: Which run endings commit it.

        Returns:
          None.

        Raises:
          None.
        """
        if not n:
            return
        if self._immediate:
            _commit([(owner, {name: n})], [])
            return
        self._entries.append((owner, name, n, when, self._round))

    def announce(self, line: Callable[[], None], *, when: When) -> None:
        """Registers a diagnostic line, written after the commit when ``when`` keeps it.

        A site passes a ``lambda`` wrapping its own ``_diag`` call, so the detail stays inside a
        call ``tests/test_diag.py`` reads for a ``repr``.

        Args:
          line: Called with no arguments once the counts are committed, outside every lock.
          when: Which run endings write it.

        Returns:
          None.

        Raises:
          None.
        """
        if self._immediate:
            _commit([], [line])
            return
        self._lines.append((line, when, self._round))

    def resends_unknown(self, owner: object) -> bool:
        """Reports whether this run resends an ``UNKNOWN`` event: whether its sink is idempotent.

        A run decides resends on the sink it was handed, which for a delegate forwarding
        ``deliver`` is not the sink whose round registers the count, so a count keyed on the
        resend reads the run's answer here rather than its own class. A detached or immediate
        context has no run sink, and answers for ``owner``.

        Args:
          owner: The sink asking, read statically when the context has no run sink.

        Returns:
          True when an ``UNKNOWN`` event may be offered again in a later round.

        Raises:
          None.
        """
        if self._resends_unknown is None:
            return _idempotent(owner)
        return self._resends_unknown

    def _run_sink_resends(self, idempotent: bool) -> None:
        """Records whether the run's own sink is idempotent, for :meth:`resends_unknown`.

        Args:
          idempotent: The run sink's ``log_foundry_idempotent``, read statically.

        Returns:
          None.

        Raises:
          None.
        """
        self._resends_unknown = idempotent

    def _written_elsewhere(self) -> None:
        """Records that a line for this round's loss was already written by a detached run.

        A 1.x shim commits its reader's count and line in a detached run of its own, so its
        caller sees the counters move as in 1.x; the run that called it then holds no line for
        that loss. This registers a line that writes nothing, ``ALWAYS``, so the run counts the
        loss as announced: a guard keyed on "registered no line" adds none, and the engine's
        settled line is not written beside the one already on stderr.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        if not self._immediate:
            self._lines.append((_written, When.ALWAYS, self._round))

    def drops(self, owner: _Counted) -> _Drops:
        """Returns a ``Deferral``-shaped view that registers every count and line ``SETTLED``.

        Args:
          owner: The owner the view counts into.

        Returns:
          The view.

        Raises:
          None.
        """
        return _Drops(self, owner)

    def _mark(self) -> tuple[int, int]:
        """Returns a point :meth:`_rewind` can return the context to.

        Args:
          None.

        Returns:
          How many counts and how many lines are registered so far.

        Raises:
          None.
        """
        return len(self._entries), len(self._lines)

    def _rewind(self, mark: tuple[int, int]) -> None:
        """Discards every count and line registered since ``mark``.

        A caller guarding a hook that may register a count and then raise uses it to replace
        what the hook left with its own, so the loss is counted and announced once. An immediate
        context has committed each entry already, so nothing is left to discard. It assumes no
        other thread registers on this context between the mark and the rewind, as a context is
        not thread-safe: a ``deliver`` fanning inner rounds out over threads that share it would
        have one round's rewind discard a sibling's entries.

        Args:
          mark: What :meth:`_mark` returned.

        Returns:
          None.

        Raises:
          None.
        """
        del self._entries[mark[0] :]
        del self._lines[mark[1] :]

    def _begin(self, number: int) -> None:
        """Records the round about to run.

        Args:
          number: Its 1-based number.

        Returns:
          None.

        Raises:
          None.
        """
        self._round = number

    def _kept(self, when: When, registered: int, *, settled: bool, final: int) -> bool:
        """Whether an entry registered in one round commits, given how the run ended.

        Args:
          when: The entry's rule.
          registered: The round it was registered in.
          settled: Whether the run settled.
          final: The run's last round.

        Returns:
          True when it commits.

        Raises:
          None.
        """
        if when is When.ALWAYS:
            return True
        if when is When.SETTLED:
            return settled
        return registered == final

    def _committed(
        self, *, settled: bool, final: int
    ) -> tuple[list[tuple[_Counted, dict[str, int]]], list[Callable[[], None]]]:
        """Selects what commits: each owner's counts in first-registration order, then the lines.

        Args:
          settled: Whether the run settled.
          final: The run's last round.

        Returns:
          ``(owners, lines)``: per owner its counts by name, and the lines in registration order.

        Raises:
          None.
        """
        owners: list[tuple[_Counted, dict[str, int]]] = []
        position: dict[int, int] = {}
        for owner, name, n, when, registered in self._entries:
            if not self._kept(when, registered, settled=settled, final=final):
                continue
            at = position.get(id(owner))
            if at is None:
                at = position[id(owner)] = len(owners)
                owners.append((owner, {}))
            counts = owners[at][1]
            counts[name] = counts.get(name, 0) + n
        lines = [
            line
            for line, when, registered in self._lines
            if self._kept(when, registered, settled=settled, final=final)
        ]
        return owners, lines

    def _close(self) -> None:
        """Drops every reference the context holds, so no owner outlives its run here.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self._notes.clear()
        self._entries.clear()
        self._lines.clear()


def _written() -> None:
    """Writes nothing: the line :meth:`RunContext._written_elsewhere` registers.

    Args:
      None.

    Returns:
      None.

    Raises:
      None.
    """


class _Drops:
    """A ``Deferral``-shaped view of a run context that registers everything ``SETTLED``.

    It keeps the 1.x ``pending`` parameter of ``HTTPSink._items`` and ``_drop_refused`` whole:
    a per-event drop is counted only by a run that settles, as it was only by an ``emit`` that
    returned, and a ``Deferral`` still fits the same parameter.
    """

    __slots__ = ("_context", "_owner")

    def __init__(self, context: RunContext, owner: _Counted) -> None:
        """Binds the view to its context and owner.

        Args:
          context: The run context.
          owner: The owner every count names.

        Returns:
          None.

        Raises:
          None.
        """
        self._context = context
        self._owner = owner

    def count(self, counter: str, n: int = 1) -> None:
        """Registers ``n`` against the owner's ``counter``, committed only by a settled run.

        Args:
          counter: The owner's attribute.
          n: How much to add.

        Returns:
          None.

        Raises:
          None.
        """
        self._context.count(self._owner, counter, n, when=When.SETTLED)

    def announce(self, line: Callable[[], None]) -> None:
        """Registers a line written only by a settled run.

        Args:
          line: The line, a ``lambda`` wrapping a ``_diag`` call.

        Returns:
          None.

        Raises:
          None.
        """
        self._context.announce(line, when=When.SETTLED)


_RUNS: ContextVar[tuple[RunContext, ...]] = ContextVar("log_foundry_runs", default=())
"""The engine runs in progress on the calling context, innermost last.

A ``ContextVar`` rather than a thread-local, so an asyncio task, a thread and a run nested inside
another run's round each see only their own; a new thread starts empty. ``_run`` pushes and pops
it in a ``finally``, and each context is cleared when its run ends.
"""


def run_context() -> RunContext:
    """Returns the context of the innermost engine run on the calling context.

    Outside any run it returns a fresh *immediate* context, whose counts and lines commit at once,
    so a direct call of a sink's reader still moves its counters rather than losing them.

    Args:
      None.

    Returns:
      The context.

    Raises:
      None.
    """
    runs = _RUNS.get()
    if runs:
        return runs[-1]
    return RunContext(immediate=True)


def _verdict_failed(verdict: object, n: int) -> bool:
    """Applies the run-failed rule to one round's verdict, as a one-round run's tally would.

    Args:
      verdict: What the round returned.
      n: How many events it was handed.

    Returns:
      True when the round would fail a run; an unreadable verdict settles, as in :func:`_run`.

    Raises:
      None.
    """
    outcomes = outcomes_of(verdict, n)
    if outcomes is None:
        return False
    return _tally(list(outcomes), 1, 1, False).failed


def detached[T](call: Callable[[], T], failed: Callable[[T], bool]) -> T:
    """Runs ``call`` as a one-round run of its own, committing its context when it returns.

    The context is pushed whatever else is running, so a count inside it is committed here and
    never by an enclosing run. It is its own last round and settles unless ``failed`` says
    otherwise. An ``Exception`` from ``call`` commits it as a failed run and propagates; a
    ``BaseException`` commits nothing.

    Args:
      call: The work to run.
      failed: Whether its result fails the run.

    Returns:
      What ``call`` returned.

    Raises:
      Exception: Whatever ``call`` raises, after the failed run's commit.
      BaseException: Whatever ``call`` raises that is not an ``Exception``, uncommitted.
    """
    context = RunContext()
    token = _RUNS.set((*_RUNS.get(), context))
    try:
        try:
            result = call()
        except Exception:
            _commit(*context._committed(settled=False, final=1))
            raise
        _commit(*context._committed(settled=not failed(result), final=1))
        return result
    finally:
        _RUNS.reset(token)
        context._close()


def one_round(
    round_fn: Callable[[list[dict[str, object]]], Verdict], batch: list[dict[str, object]]
) -> Verdict:
    """Runs one round as a sink's ``deliver``: inside a run as part of it, outside one detached.

    Inside an engine run the round counts into that run's context, each count under its own
    owner, so a delegate forwarding ``deliver`` commits into the sink that made the count. A
    direct ``sink.deliver(batch)`` outside any run commits its own ledger when it returns, as a
    one-round run that is its own last and settles unless its verdict fails.

    Args:
      round_fn: The sink's round.
      batch: The events.

    Returns:
      The round's verdict.

    Raises:
      BaseException: Whatever the round raises that is not an ``Exception``.
    """
    if _RUNS.get():
        return round_fn(batch)
    size = len(batch)
    return detached(lambda: round_fn(batch), lambda verdict: _verdict_failed(verdict, size))


def _uncounted(fault: Exception, n: int, what: str) -> None:
    """Announces a commit that raised, so a misspelt counter never drops a count silently.

    Args:
      fault: What the commit raised.
      n: How many were not counted.
      what: What they were, named by type and unit only.

    Returns:
      None.

    Raises:
      None.
    """
    _diag.absorbed("counting a delivery", fault, f"{n} {what} not counted")


def _commit(owners: list[tuple[_Counted, dict[str, int]]], lines: list[Callable[[], None]]) -> None:
    """Commits a run context's selected counts, one ``Deferral`` per owner, then its lines.

    Each owner's counts commit under that owner's ``_counter_lock``, read at commit time, so a
    fork repair that rebound it is honoured. The lines are written after every count, outside
    every lock, in registration order, and even when an owner's commit raised, since an
    announced loss is better than a silent one.

    Args:
      owners: Each owner with its counts by name.
      lines: The lines to write.

    Returns:
      None.

    Raises:
      None.
    """
    for owner, counts in owners:
        try:
            with Deferral(owner) as ledger:
                for name, n in counts.items():
                    ledger.count(name, n)
        except Exception as fault:
            _uncounted(
                fault,
                sum(counts.values()),
                f"count(s) for {_diag._STR_STR(_diag._STORED_NAME(type(owner)))}",
            )
    for line in lines:
        line()


def _wait(delay: float, stop: threading.Event | None) -> threading.Event | None:
    """Waits between rounds on the stop event, falling back to a sleep if waiting on it raises.

    The event is the sink's or the worker's; a subclass whose ``wait`` raises is announced once
    and dropped for the rest of the run, so no ``Exception`` escapes the engine and no round
    spins without its wait.

    Args:
      delay: Seconds to wait.
      stop: The stop event, or ``None``.

    Returns:
      The event to use for the next wait.

    Raises:
      None.
    """
    try:
        _retry.wait(delay, stop)
    except Exception as exc:
        _diag.absorbed("waiting between delivery rounds", exc, "later waits sleep instead")
        _retry.wait(delay, None)
        return None
    return stop


def _run(
    sink: object,
    deliver: Deliver,
    counters: SinkCounters,
    batch: list[dict[str, object]],
    *,
    sink_retries: object,
    worker_retries: object,
    stop: threading.Event | None,
    decline: Callable[[float], bool] | None,
) -> Delivery:
    """Runs the rounds for one batch, resending only what may land, and commits once.

    The batch is copied at entry and only the copy is indexed, so a ``deliver`` that clears or
    extends the list it was given changes nothing here. Round one hands ``deliver`` the batch as
    given; each later round a new list of the events still to send, in their original order.
    Each verdict's indices are mapped back to the copy, and an event with a final outcome is never
    offered again.

    **The budget keeps the 1.x total**: ``(s + 1) * (w + 1)`` rounds, ``s`` the sink's and ``w``
    the worker's ``max_retries``, each an exact ``int`` floored at zero or else zero, run as
    ``w + 1`` cycles of ``s + 1``. The orphan path and the adapter pass ``w`` as zero. Before a
    later round the engine waits: a usable hint from the previous verdict, bounded by the sink's
    ceiling; else :func:`cycle_backoff` at a cycle boundary; else the sinks' backoff, restarting
    each cycle. After the wait the decline predicate is asked with the run's longest round, and a
    ``True`` ends the run. The first round is never declined. No lock is held while ``deliver``
    runs; the counters' lock is a leaf taken only by the commit.

    **An ``Exception`` from ``deliver``** fails its round's events and is resent only by an
    idempotent sink whose predicate accepts it; an unreadable verdict or ``UNKNOWN`` is resent
    only by an idempotent sink. A ``BaseException`` propagates and nothing is committed.

    **The commit** is one ``Deferral`` on the counters at the end: ``failed_events`` takes every
    event that ended rejected, unresolved, raised or unknown, and ``dropped_events`` the dropped
    ones only when the run settled, since a failed run's drops belong to its catcher. A settled
    run's loss is announced in one ``_diag.lost`` line naming counts and rounds, after the
    commit; then each distinct exception type that escaped ``deliver`` gets one ``absorbed`` line,
    record first, announce second, by type and never by text.

    **The run's context** (SPEC-073 FR-002) is pushed onto :data:`_RUNS` for the whole run and
    popped in a ``finally``, so a round reads its own :func:`run_context` and a run nested inside
    a round has its own. Each round's number is recorded on it before ``deliver`` is called.
    After the engine's own commit, the counts the sink registered commit through :func:`_commit`
    as the run ended — ``ALWAYS``, ``SETTLED`` for a settled run, ``LAST`` for the final round —
    and then their lines. A settled run whose sink will write a line of its own gets no engine
    line, so a loss is never announced twice; that is SPEC-072 FR-003's refinement, which needs
    this context and is added with it. A ``BaseException`` commits nothing of either.

    Args:
      sink: The sink, read statically for its idempotence and predicate.
      deliver: The callable to run each round.
      counters: The counters the commit writes.
      batch: The events.
      sink_retries: The sink's ``max_retries`` as read statically.
      worker_retries: The worker's ``max_retries``, or zero/``None`` for one cycle.
      stop: The event every wait is cut short by, or ``None`` to sleep.
      decline: Asked before each further round with the run's longest round; True ends the run.

    Returns:
      What the run did.

    Raises:
      BaseException: Whatever ``deliver`` raises that is not an ``Exception``, uncommitted.
    """
    events = _snapshot(batch)
    total_events = len(events)
    if not total_events:
        return _SETTLED_EMPTY
    context = RunContext()
    token = _RUNS.set((*_RUNS.get(), context))
    try:
        per_cycle = _count(sink_retries) + 1
        rounds_allowed = per_cycle * (_count(worker_retries) + 1)
        idempotent = _idempotent(sink)
        context._run_sink_resends(idempotent)
        ceiling = _ceiling(sink)
        fate: list[object] = [Outcome.RETRYABLE] * total_events
        pending = list(range(total_events))
        escaped: list[Exception] = []
        attempts = 0
        longest = 0.0
        hint: float | None = None
        declined = False
        while True:
            if attempts:
                cycle, position = divmod(attempts, per_cycle)
                delay = None if hint is None else _retry.clamp_server_delay(hint, ceiling)
                if delay is None:
                    delay = cycle_backoff(cycle - 1) if position == 0 else _backoff(position - 1)
                stop = _wait(delay, stop)
                if decline is not None and decline(longest):
                    declined = True
                    break
            offered = batch if not attempts else [events[index] for index in pending]
            n = len(pending)
            attempts += 1
            context._begin(attempts)
            hint = None
            started = time.monotonic()
            try:
                result = deliver(offered)
            except Exception as exc:
                longest = max(longest, time.monotonic() - started)
                if not any(type(seen) is type(exc) for seen in escaped):
                    escaped.append(exc)
                predicate = _predicate_of(sink) if idempotent else None
                resend = predicate is not None and is_retryable(predicate, exc)
                for index in pending:
                    fate[index] = _RAISED
                if not resend:
                    pending = []
            else:
                longest = max(longest, time.monotonic() - started)
                outcomes = outcomes_of(result, n)
                if outcomes is None:
                    for index in pending:
                        fate[index] = _UNREAD
                    if not idempotent:
                        pending = []
                else:
                    hint = _hint_of(result)
                    still: list[int] = []
                    for index, outcome in zip(pending, outcomes, strict=True):
                        fate[index] = outcome
                        if outcome is Outcome.RETRYABLE or (
                            idempotent and outcome is Outcome.UNKNOWN
                        ):
                            still.append(index)
                    pending = still
            if not pending or attempts >= rounds_allowed:
                break
        delivery = _tally(fate, attempts, -(-attempts // per_cycle), declined)
        unconfirmed = delivery.rejected + delivery.unresolved + delivery.raised + delivery.unknown
        dropped = 0 if delivery.failed else delivery.dropped
        name = _diag._STR_STR(_diag._STORED_NAME(type(sink)))
        owners, lines = context._committed(settled=not delivery.failed, final=attempts)
        try:
            with Deferral(counters) as ledger:
                ledger.count("failed_events", unconfirmed)
                ledger.count("dropped_events", dropped)
                if not delivery.failed and unconfirmed + dropped and not lines:
                    ledger.announce(
                        lambda: _diag.lost(
                            "event",
                            unconfirmed + dropped,
                            f"rejected {delivery.rejected}, unresolved {delivery.unresolved}, "
                            f"raised {delivery.raised}, unknown {delivery.unknown}, dropped {dropped} "
                            f"after {delivery.attempts} delivery round(s) to {name}",
                        )
                    )
        except Exception as fault:
            _uncounted(fault, unconfirmed + dropped, "event(s)")
        _commit(owners, lines)
        for one in escaped:
            _diag.absorbed("delivering a batch", one, f"its round's events not confirmed by {name}")
        return delivery
    finally:
        _RUNS.reset(token)
        context._close()


def _tally(fate: list[object], attempts: int, cycles: int, declined: bool) -> Delivery:
    """Counts the final fates of one run and applies the run-failed rule.

    Args:
      fate: Each event's final fate, by index in the run's copy of the batch.
      attempts: ``deliver`` calls made.
      cycles: Cycles started.
      declined: Whether the run was declined.

    Returns:
      The run's record.

    Raises:
      None.
    """
    accepted = sum(1 for one in fate if one is Outcome.ACCEPTED)
    rejected = sum(1 for one in fate if one is Outcome.REJECTED)
    unresolved = sum(1 for one in fate if one is Outcome.RETRYABLE)
    raised = sum(1 for one in fate if one is _RAISED)
    unknown = sum(1 for one in fate if one is Outcome.UNKNOWN or one is _UNREAD)
    return Delivery(
        attempts=attempts,
        cycles=cycles,
        accepted=accepted,
        rejected=rejected,
        dropped=sum(1 for one in fate if one is Outcome.DROPPED),
        unresolved=unresolved,
        raised=raised,
        unknown=unknown,
        declined=declined,
        skipped=sum(1 for one in fate if one is Outcome.SKIPPED),
        failed=not accepted and bool(rejected + unresolved + raised) and not unknown,
    )


def run_delivery(
    sink: object,
    batch: list[dict[str, object]],
    *,
    worker_retries: int | None,
    stop: threading.Event | None,
    decline: Callable[[float], bool] | None = None,
) -> Delivery | None:
    """Delivers a batch through the sink's ``deliver`` when it offers one, else returns ``None``.

    The probe runs once, here, so a swap cannot change the sink between it and the run, and the
    run counts into the counters the offer was decided on. ``None`` tells the caller to use
    ``emit``, unchanged.

    Args:
      sink: The sink, of any type.
      batch: The events.
      worker_retries: The worker's ``max_retries``, or ``None`` on the orphan path.
      stop: The event every wait is cut short by.
      decline: The worker's prediction, asked before each further round.

    Returns:
      What the run did, or ``None`` when the sink does not offer ``deliver``.

    Raises:
      BaseException: Whatever ``deliver`` raises that is not an ``Exception``.
    """
    probe = _probe(sink)
    if probe is None:
        return None
    deliver, counters = probe
    return _run(
        sink,
        deliver,
        counters,
        batch,
        sink_retries=_static_value(sink, "max_retries"),
        worker_retries=worker_retries,
        stop=stop,
        decline=decline,
    )


def emit_via_deliver(
    sink: object,
    batch: list[dict[str, object]],
    *,
    deliver: Callable[[list[dict[str, object]]], Verdict] | None = None,
) -> None:
    """Runs the engine inline as an engine sink's ``emit``, keeping the 1.x ``emit`` contract.

    What a sink that defines ``deliver`` makes its ``emit``, so a third-party wrapper calling
    ``inner.emit(batch)``, or a subclass overriding ``emit`` and calling ``super().emit(batch)``,
    sees what 1.x showed it: a normal return on a partial outcome, an unreadable verdict or an
    ``UNKNOWN`` event, the sink's own retries spent first, ``dropped`` counted only when the call
    returns, and a raise when the run fails. An empty batch returns first, before any member is
    checked, since ``emit([])`` never raises. ``deliver`` is the given callable, or the class's
    found by the probe's static lookup **without** the ``emit``-override rule (this is the
    ``emit`` an override reaches through ``super()``), where a ``deliver = None`` is passed over
    and the walk goes on: an opt-out means "do not offer", not "do not have". It runs one cycle
    of the sink's own ``max_retries``, waits on the sink's ``log_foundry_stop_signal`` and is
    never declined; to the worker's prediction it is one attempt, as a 1.x ``emit`` is.

    Args:
      sink: The sink whose ``emit`` this is.
      batch: The events.
      deliver: The ``deliver`` to run, or ``None`` for the static lookup.

    Returns:
      None.

    Raises:
      TypeError: When the sink has no usable ``deliver`` or no ``log_foundry_counters``
        ``SinkCounters``, naming its stored type name and the missing member, so a misspelt
        member is a counted failure rather than events that vanish.
      SinkDeliveryError: When the run failed, naming counts and the sink's type, never a value.
      BaseException: Whatever ``deliver`` raises that is not an ``Exception``.
    """
    events = _snapshot(batch)
    if not events:
        return
    found: object = deliver if callable(deliver) else None
    if deliver is None:
        try:
            found = _bound_deliver(sink, override_rule=False, skip_none=True)
        except Exception:
            found = None
    if found is None:
        raise TypeError(
            f"{_diag._STR_STR(_diag._STORED_NAME(type(sink)))} reaches emit_via_deliver with no "
            "usable deliver"
        )
    counters = _static_value(sink, "log_foundry_counters")
    if type(counters) is not SinkCounters:
        raise TypeError(
            f"{_diag._STR_STR(_diag._STORED_NAME(type(sink)))} reaches emit_via_deliver with no "
            "log_foundry_counters SinkCounters"
        )
    delivery = _run(
        sink,
        cast("Deliver", found),
        counters,
        events,
        sink_retries=_static_value(sink, "max_retries"),
        worker_retries=0,
        stop=stop_signal_of(sink),
        decline=None,
    )
    if delivery.failed:
        raise SinkDeliveryError(
            f"delivered none of {len(events)} event(s): rejected {delivery.rejected}, "
            f"unresolved {delivery.unresolved}, raised {delivery.raised} after "
            f"{delivery.attempts} delivery round(s) to "
            f"{_diag._STR_STR(_diag._STORED_NAME(type(sink)))}"
        )
