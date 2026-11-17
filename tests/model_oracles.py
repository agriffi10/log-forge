"""SPEC-085's constants, failure classes and pure oracles, which judge one cell from its records.

Every oracle here is a pure function over recorded data: it reads instants, counts and names that
the child process recorded, and raises a :class:`ModelFailure` whose signatures say which rule
broke, on which call and by how much. Nothing here reads a clock, starts a thread or touches the
library, so each oracle is held by a fixture corpus in ``tests/test_model_oracles.py`` that asserts
silence as well as failure.

**Every duration is a subtraction of two recorded instants, and every comparison fails only when a
value is too large.** No oracle asserts a lower bound on how long anything took (FR-003), and
``tests/test_timing_floors.py`` cannot see a subtraction of two recorded instants, so that rule is
held by ``test_no_oracle_fails_a_duration_for_being_short`` rather than by the timing-floor gate.

This module imports the standard library only. It is one of the four files a run at an older
commit copies over that commit's ``tests/`` (FR-005), and ``tests/conftest.py``'s summary hook
calls :func:`margin_line` from here so that the hook stays a thin caller those commits lack.

**SPEC-086 adds the gating model's half** (``tests/test_invariants_model.py``): the late-write
rule, the soak's event record and its unowed-repeat rule, the two switches' parsers, the rerun
command and the gate's margin line. The model file is copied to an older commit with this module
for the same reason the cells are, and ``tests/conftest.py`` validates the switches through the
parsers here before any file is collected.
"""

from __future__ import annotations

import json
import os
import re
import shlex
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

EPSILON = 1.0
"""Seconds a call's own time may exceed its budget.

Over twice the slowest return measured under load, twice ``tests/test_timing_floors.py``'s
``STALL_ALLOWANCE``, and half the shortest self-wait the cells must see, the 2.0 s closer grace.
"""

MARGIN_REDERIVE = 0.5
"""A worst margin over passing cells above this, on a CI leg, stops the merge (FR-003)."""

HANG_AFTER = 5.0
"""Own-time seconds past budget plus ``EPSILON`` before the watchdog ends a child."""

WAITER_HOLD = 4.0
"""How long an armed ``waiter`` waits when nothing cuts the wait short."""

REACH_TIMEOUT = 10.0
"""The worker path's reaching flush, above any finite wait a script can make."""

GATE_TIMEOUT = 5.0
"""Each of ``swap-emit``'s two waits, and a ``waiter`` cell's wait for its sink to enter."""

CELL_TIMEOUT = 60
"""``run_in_child``'s timeout for a cell.

Above the longest watchdog deadline, 36 s for the exit drain, plus every finite budget a cell can
spend before it: 10 s reaching, 5 s shutting down and 8 s in a script. With ``run_in_child``'s
default of 10 s, two of the 112 ``reentrant`` ``setter`` cells at ``b2528383`` die of the alarm
before the watchdog can report.
"""

MARGIN = "model_margin"
"""The property each cell records its worst ``own time - budget`` under, and the summary reads."""

WATCHDOG = "!MODEL-HANG "
"""The prefix of the one line the watchdog writes before the stacks of every thread."""


class ModelFailure(AssertionError):
    """A rule a cell broke, carrying one ``Class:key`` signature per thing it found.

    One raise may carry several keys, since one oracle can find several things at once: both
    ledger identities, every count at which one sink was closed twice, or several of a child's
    checks. The verdict is the set of every signature every oracle raised.

    Attributes:
      keys: The keys found, in the order the oracle found them.
      detail: Text for a failure message: the measured numbers behind the keys.
    """

    def __init__(self, *keys: str, detail: str = "") -> None:
        """Records the keys and the detail.

        Args:
          *keys: One or more keys, such as ``"repeat@1"``.
          detail: The measurement the keys were read from.

        Returns:
          None.

        Raises:
          None.
        """
        super().__init__(" ".join(f"{type(self).__name__}:{key}" for key in keys), detail)
        self.keys = keys
        self.detail = detail

    @property
    def signatures(self) -> tuple[str, ...]:
        """Returns one ``Class:key`` signature per key, in order.

        Args:
          None.

        Returns:
          The signatures.

        Raises:
          None.
        """
        return tuple(f"{type(self).__name__}:{key}" for key in self.keys)


class HangFound(ModelFailure):
    """A call's own time passed its budget plus ``EPSILON`` plus ``HANG_AFTER``, or a child hung."""


class ChildCrashed(ModelFailure):
    """The cell's child ended without a report and not by a watchdog; never an expected failure."""


class NotExercised(ModelFailure):
    """The cell's sink code never ran, or a ``swap-emit`` gate ran out."""


class CallRaised(ModelFailure):
    """An exception escaped a library call (invariant 1)."""


class BoundExceeded(ModelFailure):
    """A call with a finite timeout overran it (invariant 3)."""


class Stalled(ModelFailure):
    """A call given no timeout did not return at once."""


class WaitNotCut(ModelFailure):
    """A wait outlived the stop that should have cut it short (invariant 3)."""


class CloseBroken(ModelFailure):
    """A close rule broke (invariant 5)."""


class LedgerBroken(ModelFailure):
    """A ledger identity does not balance (invariant 2)."""


class StateBroken(ModelFailure):
    """The cell did not end ``CLOSED`` with no token held."""


class ChildBroken(ModelFailure):
    """A forked child's report broke a check, or the child did not report (invariant 12)."""


@dataclass(frozen=True)
class Interval:
    """One stretch of model sink code: a script, an armed wait, or a fork and its reaping.

    Attributes:
      thread: The ident of the thread the stretch ran on.
      began: The monotonic instant it began.
      ended: The monotonic instant it ended.
    """

    thread: int
    began: float
    ended: float


@dataclass(frozen=True)
class Call:
    """One library call a cell, a script or a forked child made.

    Attributes:
      name: An operation, or ``"configure"``, ``"reach-flush"``, ``"final-shutdown"``, ``"exit"``.
      thread: The ident of the thread that made it.
      budget: Its timeout, or the default it spends; ``0.0`` for a call given no timeout.
      began: The monotonic instant it began.
      ended: The monotonic instant it returned, or the reading instant while it is in flight.
    """

    name: str
    thread: int
    budget: float
    began: float
    ended: float


@dataclass(frozen=True)
class Wait:
    """One armed ``waiter`` wait and the stop that should cut it short.

    Attributes:
      began: The instant the wait began.
      ended: The instant the wait ended.
      stop_entered: The instant the cell entered the stop.
    """

    began: float
    ended: float
    stop_entered: float


@dataclass(frozen=True)
class Closes:
    """One sink's writes and the write count at each of its closes.

    Attributes:
      writes: The delivery calls that entered the sink.
      at_entry: The write count as each close entered.
      at_return: The write count as each close returned.
    """

    writes: int
    at_entry: tuple[int, ...]
    at_return: tuple[int, ...]


@dataclass(frozen=True)
class Ledger:
    """The terms of the two accounting identities (invariant 2).

    Attributes:
      spans: Traced calls made.
      span_ends: ``span.end`` events delivered.
      queued: ~~``health().queued`` less the flush markers left on the queue~~ the
        ``span.end`` events in submissions still on the worker's queue (SPEC-091 FR-004), since
        a swept submission holds no ``span.end``.
      dropped: ``health().dropped``.
      abandoned: ``span.end`` events in batches the worker abandoned.
      orphans: ``info`` calls made.
      orphans_delivered: Orphan events delivered.
      orphan_lost: ``health().orphan_lost``.
    """

    spans: int
    span_ends: int
    queued: int
    dropped: int
    abandoned: int
    orphans: int
    orphans_delivered: int
    orphan_lost: int


@dataclass(frozen=True)
class ChildReport:
    """What a ``forker``'s child reported, or how it ended without a report.

    Attributes:
      ending: ``"reported"``, ``"hung"`` or ``"crashed"``.
      orphan_in: Indices of the sinks its orphan log landed in.
      span_in: Indices of the sinks its span landed in.
      counters_moved: Whether ``queued``, ``dropped``, ``failed_batches`` or ``orphan_lost`` rose.
      closed_inherited: Indices of the inherited sinks it closed.
      state: Its lifecycle state's name after its own ``shutdown()``.
    """

    ending: str
    orphan_in: tuple[int, ...]
    span_in: tuple[int, ...]
    counters_moved: bool
    closed_inherited: tuple[int, ...]
    state: str


Verdict = frozenset[str]


def own_time(call: Call, intervals: Sequence[Interval]) -> float:
    """Returns a call's elapsed time less the sink code it could have been waiting on.

    The subtraction is the union of every interval's overlap with the call, on any thread,
    because a close runs on a closer thread and a delivery on the drain thread: subtracting only
    the caller's thread flags a ``shutdown()`` that is correctly waiting for a slow close. The one
    interval left out is a stretch on the call's own thread that began at or before the call and
    ended at or after it, which is the sink code the call was made from.

    Args:
      call: The call.
      intervals: Every stretch of model sink code recorded, closed or read as ending now.

    Returns:
      The call's own time in seconds.

    Raises:
      None.
    """
    pieces: list[tuple[float, float]] = []
    for stretch in intervals:
        if (
            stretch.thread == call.thread
            and stretch.began <= call.began
            and stretch.ended >= call.ended
        ):
            continue
        low, high = max(stretch.began, call.began), min(stretch.ended, call.ended)
        if high > low:
            pieces.append((low, high))
    pieces.sort()
    covered = 0.0
    edge = call.began
    for low, high in pieces:
        start = max(low, edge)
        if high > start:
            covered += high - start
            edge = high
    return (call.ended - call.began) - covered


def check_call(call: Call, intervals: Sequence[Interval]) -> None:
    """Holds a call to ``own time <= budget + EPSILON``.

    Args:
      call: The call, returned.
      intervals: Every stretch of model sink code recorded.

    Returns:
      None.

    Raises:
      BoundExceeded: If a call with a finite budget broke the rule (invariant 3).
      Stalled: If a call given no timeout broke it: it had nothing to wait for and waited.
    """
    own = own_time(call, intervals)
    if own <= call.budget + EPSILON:
        return
    detail = f"{call.name}: own time {own:.3f} s against a budget of {call.budget:.3f} s"
    if call.budget > 0.0:
        raise BoundExceeded(call.name, detail=detail)
    raise Stalled(call.name, detail=detail)


def overdue(flying: Sequence[Call], intervals: Sequence[Interval]) -> Call | None:
    """Returns the first in-flight call whose own time has passed the watchdog's deadline.

    The deadline is own time, not elapsed time, so a call waiting on sink code that is still
    running elsewhere is not ended for it. Kept pure so its corpus needs no six-second wait.

    Args:
      flying: The calls in flight, each read as ending now.
      intervals: Every stretch of sink code, open ones read as ending now.

    Returns:
      The overdue call, or ``None``.

    Raises:
      None.
    """
    for call in flying:
        if own_time(call, intervals) > call.budget + EPSILON + HANG_AFTER:
            return call
    return None


def check_wait(wait: Wait) -> None:
    """Holds a wait in flight at its stop to ending within ``EPSILON`` of the stop's entry.

    Nothing is asserted about how long a wait was served otherwise: a wait that ended before its
    stop, or at once, is silent.

    Args:
      wait: The wait and its stop.

    Returns:
      None.

    Raises:
      WaitNotCut: If the wait outlived its stop by more than ``EPSILON`` (invariant 3).
    """
    if served(wait) > EPSILON:
        raise WaitNotCut("wait", detail=f"served {served(wait):.3f} s past its stop")


def served(wait: Wait) -> float:
    """Returns how long a wait ran past the entry of the stop that should cut it.

    Args:
      wait: The wait and its stop.

    Returns:
      Seconds from the stop's entry to the wait's end; negative for a wait that ended first.

    Raises:
      None.
    """
    return wait.ended - wait.stop_entered


def check_repeat(closes: Closes) -> None:
    """Holds a sink to no close entering at the same write count as the close before it.

    Args:
      closes: One sink's record.

    Returns:
      None.

    Raises:
      CloseBroken: ``repeat@<count>``, one key for each count at which it happened.
    """
    counts: list[int] = []
    for before, after in zip(closes.at_entry, closes.at_entry[1:], strict=False):
        if after == before and after not in counts:
            counts.append(after)
    if counts:
        raise CloseBroken(*(f"repeat@{count}" for count in counts), detail=f"{closes}")


def check_closed(closes: Closes) -> None:
    """Holds a sink that was written to to having had a close (invariant 5's first observable).

    Args:
      closes: One sink's record, after the cell's ``shutdown()``.

    Returns:
      None.

    Raises:
      CloseBroken: ``never-closed@<writes>`` for a written sink with no close.
    """
    if closes.writes > 0 and not closes.at_entry:
        raise CloseBroken(f"never-closed@{closes.writes}", detail=f"{closes}")


def check_released(closes: Closes) -> None:
    """Holds a sink to no write entering after its last close entered.

    Judged after the exit drain, because a sink written to after its close is owed a second
    close, and the exit drain is what pays it. A sink with no close is silent here:
    :func:`check_closed` owns that case.

    Args:
      closes: One sink's record, after the exit drain.

    Returns:
      None.

    Raises:
      CloseBroken: ``unreleased@<count at the last close>``.
    """
    if closes.at_entry and closes.at_entry[-1] < closes.writes:
        raise CloseBroken(f"unreleased@{closes.at_entry[-1]}", detail=f"{closes}")


def changes_overlapped(calls: Sequence[Call]) -> bool:
    """Returns whether two sink-changing ``configure`` calls overlapped in time, on any threads.

    That is the one case the repeat-close rule is not judged in: a sink change re-entered by a
    sink's own code runs unserialized, and of two changes running alongside each other the
    register records that a sink may be closed twice.

    Args:
      calls: Every call recorded.

    Returns:
      Whether one ``configure`` began before another ended.

    Raises:
      None.
    """
    changes = [call for call in calls if call.name == "configure"]
    return any(
        first.began < second.ended and second.began < first.ended
        for index, first in enumerate(changes)
        for second in changes[index + 1 :]
    )


def check_ledger(ledger: Ledger) -> None:
    """Holds both accounting identities, reporting each that breaks with its signed excess.

    Args:
      ledger: The terms.

    Returns:
      None.

    Raises:
      LedgerBroken: ``spans<excess>`` and ``orphans<excess>``, as in ``spans+1``.
    """
    keys: list[str] = []
    spans = ledger.span_ends + ledger.queued + ledger.dropped + ledger.abandoned - ledger.spans
    if spans:
        keys.append(f"spans{spans:+d}")
    orphans = ledger.orphans_delivered + ledger.orphan_lost - ledger.orphans
    if orphans:
        keys.append(f"orphans{orphans:+d}")
    if keys:
        raise LedgerBroken(*keys, detail=f"{ledger}")


def check_end(state: str, tokens: int) -> None:
    """Holds the runtime to ``CLOSED`` with no swap or stop token after the cell's shutdown.

    Args:
      state: The lifecycle state's name.
      tokens: The swap and stop tokens held.

    Returns:
      None.

    Raises:
      StateBroken: ``end``.
    """
    if state != "CLOSED" or tokens:
        raise StateBroken("end", detail=f"state {state} with {tokens} token(s)")


def check_child(report: ChildReport) -> None:
    """Holds a forked child to its four checks, or names how it ended without reporting.

    Args:
      report: The child's report.

    Returns:
      None.

    Raises:
      ChildBroken: ``hung`` or ``crashed`` for a child that did not report; otherwise every one
        of ``two-sinks``, ``uncounted``, ``closed-inherited`` and ``state`` that applies.
    """
    if report.ending != "reported":
        raise ChildBroken(report.ending, detail=f"{report}")
    keys: list[str] = []
    if len(set(report.orphan_in) | set(report.span_in)) > 1:
        keys.append("two-sinks")
    if (not report.orphan_in or not report.span_in) and not report.counters_moved:
        keys.append("uncounted")
    if report.closed_inherited:
        keys.append("closed-inherited")
    if report.state != "CLOSED":
        keys.append("state")
    if keys:
        raise ChildBroken(*keys, detail=f"{report}")


_THREAD = re.compile(r"^(?:Current thread|Thread) (0x[0-9a-fA-F]+)")
_FRAME = re.compile(r'^\s+File "([^"]+)", line \d+ in (\S+)')


def _watchdog_line(output: str) -> dict[str, object] | None:
    """Returns the watchdog's record, read only at the start of the output or of a line.

    Args:
      output: A child's output.

    Returns:
      The decoded record, or ``None`` when no line starts with the watchdog's prefix.

    Raises:
      None.
    """
    for line in output.splitlines():
        if line.startswith(WATCHDOG):
            try:
                record = json.loads(line[len(WATCHDOG) :])
            except ValueError:
                return None
            return record if isinstance(record, dict) else None
    return None


def has_watchdog_line(output: str) -> bool:
    """Returns whether a child's output holds the watchdog's line.

    Args:
      output: A child's output.

    Returns:
      Whether the watchdog ended the child.

    Raises:
      None.
    """
    return _watchdog_line(output) is not None


def hang_signature(output: str, package: str) -> tuple[str, dict[str, tuple[str, ...]]]:
    """Reads the hung call and the innermost library frame of its thread from a child's output.

    ``faulthandler`` writes thread idents and no names, so the watchdog's line carries an
    ``ident=name`` map of the live threads, and each named thread's library frames are returned
    under its name: that is how a reader tells the drain thread's stack from the others.

    Args:
      output: The child's output: the watchdog's line, then ``faulthandler``'s dump.
      package: The directory of the ``log_foundry`` package the child imported.

    Returns:
      ``HangFound:<call>@<module>.<function>``, or ``@none`` when the hung call's thread has no
      frame under ``package``; and each named thread's frames under ``package``, innermost first,
      as ``module.function``.

    Raises:
      None.
    """
    record = _watchdog_line(output) or {}
    call = str(record.get("call", "unknown"))
    hung = int(str(record.get("thread", "0")), 0)
    names_raw = record.get("threads", {})
    names = {int(k, 0): str(v) for k, v in names_raw.items()} if isinstance(names_raw, dict) else {}
    root = os.path.realpath(package) + os.sep
    frames: dict[int, list[str]] = {}
    current: int | None = None
    for line in output.splitlines():
        head = _THREAD.match(line)
        if head:
            current = int(head.group(1), 16)
            frames.setdefault(current, [])
            continue
        frame = _FRAME.match(line)
        if frame is None or current is None:
            continue
        path = os.path.realpath(frame.group(1))
        if not path.startswith(root):
            continue
        module = path[len(root) :].removesuffix(".py").replace(os.sep, ".")
        frames[current].append(f"{module}.{frame.group(2)}")
    innermost = frames.get(hung, [])
    where = innermost[0] if innermost else "none"
    stacks = {names.get(ident, hex(ident)): tuple(found) for ident, found in frames.items()}
    return f"HangFound:{call}@{where}", stacks


def margin_line(reports: Iterable[object]) -> str | None:
    """Returns the session's worst-margin line over passing cells, or ``None`` when none reported.

    Read from each report's recorded properties, because a worker's module state does not reach
    the controller under ``-n 12`` and a recorded property does. A pinned cell's report is filed
    under ``xfailed``, not ``passed``, so its margin is not in the line: four of them sit 2.00 s
    over by design.

    Args:
      reports: The session's ``passed`` reports.

    Returns:
      One line naming the worst ``own time - budget`` and its test id.

    Raises:
      None.
    """
    worst = _worst(reports, MARGIN)
    if worst is None:
        return None
    mark = "within" if worst[0] <= MARGIN_REDERIVE else "ABOVE"
    return (
        f"model cells: worst own time less budget over passing cells is {worst[0]:.3f} s at "
        f"{worst[1]} ({mark} the {MARGIN_REDERIVE:.3f} s re-derive mark)"
    )


def gate_margin_line(reports: Iterable[object]) -> str | None:
    """Returns the gating model's worst-margin line over its passing runs, or ``None`` (SPEC-086).

    The twin of :func:`margin_line` for ``tests/test_invariants_model.py``: each gating run
    records its worst ``elapsed - budget`` under :data:`GATE_MARGIN`, a property of its own, so
    neither line reads the other's runs. A soak item records none, because it does not gate.

    Args:
      reports: The session's ``passed`` reports.

    Returns:
      One line naming the worst ``elapsed - budget`` and its test id.

    Raises:
      None.
    """
    worst = _worst(reports, GATE_MARGIN)
    if worst is None:
        return None
    mark = "within" if worst[0] <= MARGIN_REDERIVE else "ABOVE"
    return (
        f"model gate: worst elapsed less budget over passing runs is {worst[0]:.3f} s at "
        f"{worst[1]} ({mark} the {MARGIN_REDERIVE:.3f} s re-derive mark)"
    )


def _worst(reports: Iterable[object], prop: str) -> tuple[float, str] | None:
    """Returns the largest value recorded under one property over call-phase reports, and its id.

    Args:
      reports: The session's ``passed`` reports.
      prop: The recorded property's name.

    Returns:
      ``(margin, nodeid)``, or ``None`` when no report recorded the property.

    Raises:
      None.
    """
    worst: tuple[float, str] | None = None
    for report in reports:
        if getattr(report, "when", None) != "call":
            continue
        for name, value in getattr(report, "user_properties", ()) or ():
            if name != prop:
                continue
            margin = float(value)
            nodeid = str(getattr(report, "nodeid", "?"))
            if worst is None or margin > worst[0]:
                worst = (margin, nodeid)
    return worst


# -- SPEC-086: the gating model's close count, the soak's record, and the two switches ------------

GATE_MARGIN = "model_gate_margin"
"""The property each gating run of ``tests/test_invariants_model.py`` records its margin under."""

SEEDS_VARIABLE = "LOG_FOUNDRY_MODEL_SEEDS"
"""The environment variable that names the seeds both halves of the model run."""

SOAK_VARIABLE = "LOG_FOUNDRY_MODEL_SOAK"
"""The environment variable that adds the soak items, when it holds exactly ``1``."""

_SEED_ITEM = re.compile(r"[0-9]+(?:-[0-9]+)?")


def check_late_write(closes: Closes) -> None:
    """Holds a sink to no write entering after its last close returned (SPEC-086 FR-001).

    Judged only after a step that closes, because a log made after a ``shutdown()`` leaves its
    sink written and owed until the next one, which invariant 5 records as correct. A write that
    entered while a close was running is counted at that close's return, so it is silent here.
    A sink with no returned close is silent: :func:`check_closed` owns a written sink never closed.

    Args:
      closes: One sink's record, read after a ``shutdown()`` or the exit drain returned.

    Returns:
      None.

    Raises:
      CloseBroken: ``late-write@<count at the last close's return>``.
    """
    if closes.at_return and closes.writes > closes.at_return[-1]:
        raise CloseBroken(f"late-write@{closes.at_return[-1]}", detail=f"{closes}")


@dataclass(frozen=True)
class Arm:
    """The library armed a sink in its owed record (``_CloseRegistry.arm``).

    Attributes:
      by_writer: Whether the arm was recorded on a thread inside ``api._note_orphan_emit``, the
        orphan path's call that arms a sink for the write it is about to make.
    """

    by_writer: bool


@dataclass(frozen=True)
class Take:
    """The library took a sink out of its owed record to close it (``_CloseRegistry.take``)."""


@dataclass(frozen=True)
class Write:
    """A delivery call entered the sink.

    Attributes:
      thread: The name of the thread that made it.
    """

    thread: str


@dataclass(frozen=True)
class Close:
    """A close entered the sink.

    Attributes:
      count: The delivery calls that had entered the sink when this close entered.
      thread: The name of the closing thread.
      at: The monotonic instant the close entered.
    """

    count: int
    thread: str
    at: float


SinkEvent = Arm | Take | Write | Close


def take_of_each_close(events: Sequence[SinkEvent]) -> list[int | None]:
    """Returns, for each close in order, the index of the take it discharges, or ``None``.

    Each close discharges the earliest take of the sink not yet discharged that was recorded
    before it entered. Earliest and not latest, because two takes can both precede the closes
    they own: a ``configure()`` hands its close to a closer thread that has not started when a
    ``shutdown()`` takes the re-armed sink and closes it inline, so the closes enter in the
    reverse of the takes' order and the latest take would serve both. Whichever close a take is
    paired with, the stretch between two closes' takes is the same. A close with no take left
    to discharge is ``None``: the record missed a take, which is a wiring failure, never a
    reason to judge that close against a wider window.

    Args:
      events: One sink's events, in the order recorded.

    Returns:
      One entry per :class:`Close`, in order.

    Raises:
      None.
    """
    pending: list[int] = []
    owners: list[int | None] = []
    for index, event in enumerate(events):
        if isinstance(event, Take):
            pending.append(index)
        elif isinstance(event, Close):
            owners.append(pending.pop(0) if pending else None)
    return owners


def check_unowed_repeat(events: Sequence[SinkEvent]) -> None:
    """Holds a sink to no second close at one write count that no writer's arm made owed.

    SPEC-086 FR-003. Two consecutive closes that entered at one count are a finding unless an
    arm recorded on a thread inside ``api._note_orphan_emit`` lies strictly between the take the
    first close discharges and the take the second discharges: that is a writer arming the sink
    again after the first take, which invariant 5 records as owed a second close (SPEC-045). A
    sink change's arm between the takes excuses nothing, since that is #339's double close. A
    close with no take of its own is reported as ``untaken`` and excuses nothing either.

    Args:
      events: One sink's events, in the order recorded.

    Returns:
      None.

    Raises:
      CloseBroken: ``repeat@<count>`` once for each count at which it happened, and
        ``untaken@<count>`` for each close with no take to discharge.
    """
    closes = [(index, event) for index, event in enumerate(events) if isinstance(event, Close)]
    owners = take_of_each_close(events)
    keys: list[str] = []
    for (_, close), owner in zip(closes, owners, strict=True):
        if owner is None and f"untaken@{close.count}" not in keys:
            keys.append(f"untaken@{close.count}")
    pairs = zip(closes, closes[1:], owners, owners[1:], strict=False)
    for (_, first), (_, second), first_take, second_take in pairs:
        if first.count != second.count or f"repeat@{second.count}" in keys:
            continue
        if first_take is not None and second_take is not None:
            low, high = sorted((first_take, second_take))
            between = events[low + 1 : high]
            if any(isinstance(event, Arm) and event.by_writer for event in between):
                continue
        keys.append(f"repeat@{second.count}")
    if keys:
        raise CloseBroken(*keys, detail=f"{list(events)}")


def parse_seeds(value: str) -> tuple[int, ...]:
    """Reads :data:`SEEDS_VARIABLE`: a comma-separated list of integers and inclusive ``a-b`` ranges.

    Strict, because a fallback would let a mistyped soak run the defaults and report green: an
    empty value or element, anything but ASCII digits and one hyphen, and a descending range are
    each refused. Whitespace around an element is ignored; the seeds come back sorted, each once.

    Args:
      value: The variable's value.

    Returns:
      The seeds.

    Raises:
      ValueError: Naming the variable and quoting the value, for anything it refuses.
    """
    seeds: set[int] = set()
    for item in value.split(","):
        item = item.strip()
        if not _SEED_ITEM.fullmatch(item):
            raise ValueError(_refusal(SEEDS_VARIABLE, value, f"{item!r} is not a seed or a range"))
        low, _, high = item.partition("-")
        first, last = int(low), int(high or low)
        if last < first:
            raise ValueError(_refusal(SEEDS_VARIABLE, value, f"{item!r} runs downward"))
        seeds.update(range(first, last + 1))
    return tuple(sorted(seeds))


def parse_soak(value: str) -> bool:
    """Reads :data:`SOAK_VARIABLE`, whose one accepted value is ``1``.

    Args:
      value: The variable's value.

    Returns:
      ``True``.

    Raises:
      ValueError: Naming the variable and quoting the value, for any other value, empty included.
    """
    if value != "1":
        raise ValueError(_refusal(SOAK_VARIABLE, value, "the one accepted value is 1"))
    return True


def seeds_from(environ: Mapping[str, str], default: Iterable[int]) -> tuple[int, ...]:
    """Returns the seeds :data:`SEEDS_VARIABLE` names, or the default when it is unset.

    Args:
      environ: The environment.
      default: The seeds a run with the variable unset uses.

    Returns:
      The seeds.

    Raises:
      ValueError: From :func:`parse_seeds`, for a set value it refuses.
    """
    value = environ.get(SEEDS_VARIABLE)
    return tuple(default) if value is None else parse_seeds(value)


def soak_from(environ: Mapping[str, str]) -> bool:
    """Returns whether :data:`SOAK_VARIABLE` asks for the soak items.

    Args:
      environ: The environment.

    Returns:
      ``False`` when the variable is unset, ``True`` when it holds ``1``.

    Raises:
      ValueError: From :func:`parse_soak`, for a set value it refuses.
    """
    value = environ.get(SOAK_VARIABLE)
    return False if value is None else parse_soak(value)


def _refusal(variable: str, value: str, why: str) -> str:
    """Returns the message for a switch value that is refused.

    Args:
      variable: The variable's name.
      value: Its value.
      why: What is wrong with it.

    Returns:
      The message.

    Raises:
      None.
    """
    return f"{variable}={value!r} is refused: {why}; unset it to run the defaults"


def rerun_command(nodeid: str, seed: int, *, soak: bool) -> str:
    """Returns the one command that re-runs a model item alone, serially, at its seed.

    Args:
      nodeid: The item's id.
      seed: Its seed.
      soak: Whether it is a soak item, which needs :data:`SOAK_VARIABLE` set as well.

    Returns:
      A command for a POSIX shell, from the repository root.

    Raises:
      None.
    """
    switches = f"{SOAK_VARIABLE}=1 " if soak else ""
    return f"{switches}{SEEDS_VARIABLE}={seed} poetry run pytest -n 0 {shlex.quote(nodeid)}"
