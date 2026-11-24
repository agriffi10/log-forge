"""SPEC-101: a kept sink behind a caller's wrapper is closed after that wrapper, never beside it.

A **caller's wrapper** is a holder whose ``close`` is not defined in the package. ``Leaves``
leaves its member alone in ``close()``; ``Mine`` closes its member itself. Since SPEC-087 a sink
two graphs share is kept across a ``configure()`` and closed by the graph that holds it, which
for a caller's wrapper was whatever its ``close()`` did: behind ``Leaves`` the sink was closed by
nobody, and behind ``Mine`` it was closed beside the record's own close where the record also
owed it. The library now closes a sink it wrote to after each close of the caller's wrapper that
holds it (FR-001), orders a taken member behind a taken caller's wrapper (FR-002), and does both
on every path that closes a holder (FR-003).

The doubles keep accepting after ``close()``, so a close count is read at the write count each
close entered, and a close that overlaps another is counted. Serves invariants 1, 2, 3, 5, 6, 11
and 12.
"""

from __future__ import annotations

import gc
import json
import os
import pathlib
import subprocess
import sys
import threading
import time
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry
from conftest import join_graced
from log_foundry import _fork, _lifecycle
from log_foundry import worker as worker_mod
from log_foundry.sinks.multi import MultiSink
from log_foundry.sinks.transform import TransformSink
from test_fork_lifecycle import run_in_child

if TYPE_CHECKING:
    from collections.abc import Callable

GATE = 10.0
"""How long any parked call waits for the test to release it; never reached on a green run."""

ROOT = pathlib.Path(__file__).resolve().parents[1]
PROBES = ROOT / "docs" / "audits" / "2026-11-14-probes" / "lifecycle"

_MARK = threading.local()
"""Set on a thread while a ``Mine`` is inside its own close of its member."""


class Rec:
    """A sink that keeps accepting after ``close()``, recording each close and any overlap.

    ``at_entry`` holds the write count as each close entered, ``by_wrapper`` whether a caller's
    wrapper made that close. ``gate``, when set, holds every close open until it is set, so two
    closes that are not ordered overlap for certain.
    """

    log_foundry_stop_signal: threading.Event | None = None

    def __init__(self, name: str = "A") -> None:
        self.name = name
        self.writes = 0
        self.events = 0
        self.at_entry: list[int] = []
        self.by_wrapper: list[bool] = []
        self.entered_at: list[float] = []
        self.threads: list[str] = []
        self.closing = 0
        self.overlaps = 0
        self.slow_emit = 0.0
        self.inside = 0
        self.gate: threading.Event | None = None
        self.raises: BaseException | None = None
        self._lock = threading.Lock()

    @property
    def closes(self) -> int:
        """How many closes entered."""
        return len(self.at_entry)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Counts one write and keeps the batch."""
        with self._lock:
            self.writes += 1
            self.inside += 1
        try:
            if self.slow_emit:
                time.sleep(self.slow_emit)
            with self._lock:
                self.events += len(batch)
        finally:
            with self._lock:
                self.inside -= 1

    def close(self) -> None:
        """Records the close, holds it on the gate when one is set, and raises when asked."""
        with self._lock:
            self.at_entry.append(self.writes)
            self.by_wrapper.append(bool(getattr(_MARK, "on", False)))
            self.entered_at.append(time.monotonic())
            self.threads.append(threading.current_thread().name)
            self.closing += 1
            if self.closing > 1:
                self.overlaps += 1
        try:
            gate = self.gate
            if gate is not None:
                gate.wait(GATE)
            time.sleep(0.01)
            if self.raises is not None:
                raise self.raises
        finally:
            with self._lock:
                self.closing -= 1

    def __repr__(self) -> str:
        """Names the sink and its record, so a failure reads without a debugger."""
        return (
            f"<{self.name} writes={self.writes} at_entry={self.at_entry} "
            f"by_wrapper={self.by_wrapper} overlaps={self.overlaps}>"
        )


class Leaves:
    """A caller's wrapper whose ``close()`` leaves its member alone."""

    def __init__(self, inner: object) -> None:
        self.inner = inner

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Forwards the batch."""
        self.inner.emit(batch)  # type: ignore[attr-defined]

    def close(self) -> None:
        """Closes nothing."""


class Mine:
    """A caller's wrapper whose ``close()`` closes its member, marking its thread meanwhile."""

    def __init__(self, inner: object) -> None:
        self.inner = inner
        self.closed_at: list[float] = []

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Forwards the batch."""
        self.inner.emit(batch)  # type: ignore[attr-defined]

    def close(self) -> None:
        """Closes the member with its own code, marked."""
        _MARK.on = True
        try:
            self.inner.close()  # type: ignore[attr-defined]
        finally:
            _MARK.on = False
            self.closed_at.append(time.monotonic())


class Pair:
    """A caller's wrapper over two members, leaving both alone in ``close()``."""

    def __init__(self, first: object, second: object) -> None:
        self.first = first
        self.second = second

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Forwards the batch to both."""
        self.first.emit(batch)  # type: ignore[attr-defined]
        self.second.emit(batch)  # type: ignore[attr-defined]

    def close(self) -> None:
        """Closes nothing."""


@log_foundry.trace
def _span() -> None:
    """One span: a start and an end event on the worker path."""


def _write(path: str, n: int = 1) -> None:
    """Writes ``n`` events with no span, or ``n`` spans and a flush."""
    for _ in range(n):
        if path == "spans":
            _span()
        else:
            log_foundry.info("x")
    if path == "spans":
        log_foundry.flush(timeout=5.0)


def _losses() -> tuple[int, ...]:
    """Every loss term ``health()`` reports."""
    h = log_foundry.health()
    return (h.dropped, h.failed_batches, h.orphan_lost, h.in_span_lost, h.submitted_after_shutdown)


def _wait(predicate: Callable[[], bool], timeout: float = GATE) -> bool:
    """Polls ``predicate`` until it holds or ``timeout`` passes."""
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


def _unconfirmed(monkeypatch: pytest.MonkeyPatch, slow: Rec, new: object) -> None:
    """Swaps to ``new`` with a budget the drain into ``slow`` outlasts, so the fence is unconfirmed.

    Each call adds one to ``incomplete_swaps``, which it asserts, so two calls in a row leave
    three sinks owed.
    """
    before = log_foundry.health().incomplete_swaps
    slow.slow_emit = 0.5
    _span()
    monkeypatch.setattr(worker_mod, "DEFAULT_SWAP_TIMEOUT", 0.1)
    log_foundry.configure(sink=new)
    monkeypatch.setattr(worker_mod, "DEFAULT_SWAP_TIMEOUT", 5.0)
    assert log_foundry.health().incomplete_swaps == before + 1, "the premise: unconfirmed"
    assert _wait(lambda: slow.inside == 0)
    slow.slow_emit = 0.0


def _first(first: str, a: Rec, b: Rec) -> object:
    """The sink the first ``configure()`` installs: A itself, or a shipped ``MultiSink(A, B)``."""
    return MultiSink(a, b) if first == "multi" else a


# -- FR-001: a written sink beneath a caller's wrapper is closed after each library close ------


@pytest.mark.parametrize("path", ["orphan", "spans"])
@pytest.mark.parametrize("first", ["installed", "multi"])
def test_leaves_over_a_written_sink_closes_it_once(first: str, path: str) -> None:
    """FR-001 AC1: every event reaches A, no loss term moves, and A is closed exactly once."""
    a, b = Rec("A"), Rec("B")
    log_foundry.configure(service="leaves", sink=_first(first, a, b))
    _write(path)
    log_foundry.configure(sink=Leaves(a))
    _write(path, 2)
    assert a.closes == 0, a
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    per = 2 if path == "spans" else 1
    assert a.events == 3 * per and _losses() == (0, 0, 0, 0, 0), a
    assert a.closes == 1 and a.overlaps == 0 and a.by_wrapper == [False], a


@pytest.mark.parametrize("path", ["orphan", "spans"])
@pytest.mark.parametrize("first", ["installed", "multi"])
def test_mine_over_a_written_sink_closes_it_twice_in_sequence(first: str, path: str) -> None:
    """FR-001 AC4: A is closed by ``Mine.close()``, then by the library after it, never at once."""
    a, b = Rec("A"), Rec("B")
    log_foundry.configure(service="mine", sink=_first(first, a, b))
    _write(path)
    mine = Mine(a)
    log_foundry.configure(sink=mine)
    _write(path, 2)
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    per = 2 if path == "spans" else 1
    assert a.events == 3 * per and _losses() == (0, 0, 0, 0, 0), a
    assert a.closes == 2 and a.overlaps == 0 and a.by_wrapper == [True, False], a
    assert a.entered_at[1] >= mine.closed_at[0], (a.entered_at, mine.closed_at)


@pytest.mark.parametrize("wrapper", ["Leaves", "Mine", "MultiSink"])
def test_a_rearmed_holder_closes_its_member_again(wrapper: str) -> None:
    """FR-001 AC2: a holder written again after its close closes its member again, after it.

    ``configure(A)``, a log, ``configure(W(A))``, a log, ``shutdown()``, a log, ``shutdown()``:
    A is closed at write counts 2 and 3, as a shipped ``MultiSink(A)`` closes it through its
    forward (SPEC-045), and behind ``Mine`` each count reads twice, the wrapper's and then the
    library's.
    """
    a = Rec("A")
    log_foundry.configure(service="rearmed", sink=a)
    log_foundry.info("one")
    holder = {"Leaves": Leaves, "Mine": Mine, "MultiSink": MultiSink}[wrapper](a)
    log_foundry.configure(sink=holder)
    log_foundry.info("two")
    log_foundry.shutdown(timeout=5.0)
    log_foundry.info("three")
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    expected = [2, 2, 3, 3] if wrapper == "Mine" else [2, 3]
    assert a.at_entry == expected and a.overlaps == 0, a


def test_the_leaves_remaining_holder_probe_prints_one_close() -> None:
    """FR-001 AC3: the remaining holder's member is attached to it, and closed once, after it.

    ``leaves_remaining_holder.py`` parks the drain thread inside A through ``Leaves(A)``, expires a
    ``shutdown()``, installs ``MultiSink(A, B)`` and expires another: the take of the
    ``MultiSink`` spares A, which lies beneath ``Leaves(A)`` still in the record. On SPEC-087's tree
    A was then closed by nobody (``A.closes=0``); the spared forward now attaches A to ``Leaves``.
    """
    env = dict(os.environ, PYTHONPATH=str(ROOT / "src"))
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-B", str(PROBES / "leaves_remaining_holder.py")],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "A.closes=1 A.closed_while_inside=0" in result.stdout, result.stdout
    assert "A.refused=0" in result.stdout, result.stdout


def test_a_member_close_that_raises_is_absorbed_and_the_others_still_close(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """FR-001 AC6: one ``_diag`` line names a close after the wrapper, and B is still closed."""
    a, b = Rec("A"), Rec("B")
    log_foundry.configure(service="raises", sink=MultiSink(a, b))
    log_foundry.info("one")
    log_foundry.configure(sink=Pair(a, b))
    log_foundry.info("two")
    a.raises = RuntimeError("the member's close failed")
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert (a.closes, b.closes) == (1, 1), (a, b)
    err = capsys.readouterr().err
    lines = [line for line in err.splitlines() if "after the wrapper that holds it" in line]
    assert len(lines) == 1, err
    assert "closing a sink after the wrapper that holds it (RuntimeError)" in lines[0], lines


def _live(entries: dict[int, Any], ref_of: Callable[[Any], object]) -> list[object]:
    """The referents of a record's entries, dead ones included as ``None``."""
    return [_lifecycle._referent(ref_of(value)) for value in entries.values()]


@pytest.mark.parametrize("wrapper", ["Leaves", "Mine"])
def test_the_records_stay_bounded_over_200_rounds(wrapper: str) -> None:
    """FR-001 AC7: 200 rounds of ``configure(A)``, a log, ``configure(W(A))``, a log.

    The two records hold only live sinks at the last round, and after ``shutdown()`` one
    attachment, for the configured wrapper, and two written sinks, A and that wrapper.
    """
    kind = Leaves if wrapper == "Leaves" else Mine
    a = Rec("A")
    log_foundry.configure(service="rounds", sink=a)
    for _ in range(200):
        log_foundry.configure(sink=a)
        log_foundry.info("a")
        log_foundry.configure(sink=kind(a))
        log_foundry.info("w")
    join_graced()
    gc.collect()
    closes = _lifecycle._state._closes
    with closes._lock:
        written = _live(closes._written, lambda ref: ref)
        after = _live(closes._after, lambda value: value[0])
    assert None not in written and None not in after, (written, after)
    assert len(after) <= 2 and len(written) <= 3, (written, after)
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    gc.collect()
    configured = log_foundry.get_config().sink
    with closes._lock:
        written = _live(closes._written, lambda ref: ref)
        after = _live(closes._after, lambda value: value[0])
        members = [[m.referent() for m in value[1]] for value in closes._after.values()]
    assert after == [configured] and members == [[a]], (after, members)
    assert sorted(map(id, written)) == sorted([id(a), id(configured)]), written


class _Meta(type):
    """A metaclass whose hooks raise, so a read that ran one would raise or be seen."""

    ran: list[str] = []  # noqa: RUF012

    def __getattribute__(cls, name: str) -> Any:
        if name in {"__mro__", "__dict__", "__module__", "close"}:
            _Meta.ran.append(name)
            raise RuntimeError("a metaclass hook ran")
        return super().__getattribute__(name)

    def __eq__(cls, other: object) -> bool:
        _Meta.ran.append("__eq__")
        raise RuntimeError("a metaclass hook ran")

    def __hash__(cls) -> int:
        _Meta.ran.append("__hash__")
        raise RuntimeError("a metaclass hook ran")


class _Hostile(metaclass=_Meta):
    """A caller's wrapper whose class answers every hook with a raise."""

    def close(self) -> None:
        """Closes nothing."""


class _InheritsShipped(TransformSink):
    """A subclass of a shipped wrapper that inherits its ``close``."""


class _OverridesShipped(TransformSink):
    """A subclass of a shipped wrapper that overrides ``close``."""

    def close(self) -> None:
        """Closes nothing."""


class _NoClose:
    """A holder with no ``close`` on its class, only on the instance."""

    def __init__(self) -> None:
        self.close = lambda: None


@pytest.mark.parametrize(
    ("holder", "ships"),
    [
        (MultiSink(Rec()), True),
        (_InheritsShipped(Rec(), lambda event: event), True),
        (_OverridesShipped(Rec(), lambda event: event), False),
        (Leaves(Rec()), False),
        (_NoClose(), False),
        (_Hostile(), False),
    ],
    ids=["shipped", "inherits", "overrides", "plain", "instance-only", "hostile-metaclass"],
)
def test_ships_close_reads_the_defining_class(holder: object, ships: bool) -> None:
    """The terms: a caller's wrapper is a holder whose ``close`` is not defined in the package.

    The defining class is read statically, so a metaclass whose hooks raise is never asked: the
    hostile row reads as a caller's wrapper with no hook run.
    """
    _Meta.ran.clear()
    assert _lifecycle._ships_close(holder) is ships
    assert _Meta.ran == []
    assert _fork._member_defined_here(holder, "close") is ships


# -- FR-002: one decision closes a member after a caller's wrapper it takes with it ------------


@pytest.mark.parametrize("wrapper", ["Mine", "Leaves"])
def test_an_unconfirmed_swap_into_a_callers_wrapper(
    wrapper: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FR-002 AC1-2: one take takes ``W(A)`` and A; A is closed after W, never beside it.

    Every close of A is held open on a gate a timer sets 0.3 s in, so two closes that are not in
    sequence overlap for certain: on SPEC-097's tree the ``Mine`` row reads ``overlaps=1``.
    """
    a = Rec("A")
    log_foundry.configure(service="unconfirmed", sink=a)
    _write("spans")
    holder = Mine(a) if wrapper == "Mine" else Leaves(a)
    _unconfirmed(monkeypatch, a, holder)
    _write("spans", 2)
    assert _lifecycle._state._closes.owes(a), "the premise: the record owes A and the wrapper"
    a.gate = threading.Event()
    timer = threading.Timer(0.3, a.gate.set)
    timer.start()
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    timer.join()
    expected = [True, False] if wrapper == "Mine" else [False]
    assert a.by_wrapper == expected and a.overlaps == 0, a


@pytest.mark.parametrize("wrapper", ["Mine", "Leaves"])
def test_a_swaps_stale_take_orders_a_member_after_a_callers_wrapper(
    wrapper: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FR-002's second site: the swap's stale take takes ``W(A)`` and A, and orders A after W.

    A log with no span reads A before a swap to ``W(A)`` keeps it, then arms it again, so the
    record owes both with no worker; ``configure(B)`` then takes both as stale.
    """
    from test_shared_sink_swap import _straddle

    a, b = Rec("A"), Rec("B")
    log_foundry.configure(service="stale", sink=a)
    log_foundry.info("one")
    holder = Mine(a) if wrapper == "Mine" else Leaves(a)
    _straddle(monkeypatch, a, holder)
    assert _lifecycle._state._closes.owes(holder), "the premise: the wrapper is owed too"
    a.gate = threading.Event()
    timer = threading.Timer(0.3, a.gate.set)
    timer.start()
    log_foundry.configure(sink=b)
    join_graced()
    timer.join()
    expected = [True, False] if wrapper == "Mine" else [False]
    assert a.by_wrapper == expected and a.overlaps == 0, a
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert a.by_wrapper == expected, a


def _slow_holder(mode: str, monkeypatch: pytest.MonkeyPatch) -> tuple[Rec, float, list[float]]:
    """``spec101_slow_holder.py`` at 1 s: an unconfirmed ``configure(A)``, then a holder over A.

    Returns A, the instant the ``shutdown()`` was called, and the instants that bound A's close
    from the holder's side: when the slow wrapper's ``close()`` returned, or when Z's close ended.
    """
    marks: list[float] = []

    class Slow:
        def __init__(self, member: Rec) -> None:
            self.member = member

        def emit(self, batch: list[dict[str, object]]) -> None:
            self.member.emit(batch)

        def close(self) -> None:
            time.sleep(1.0)
            marks.append(time.monotonic())

    class SlowZ(Rec):
        def close(self) -> None:
            time.sleep(1.0)
            marks.append(time.monotonic())

    a = Rec("A")
    log_foundry.configure(service="slow-holder", sink=a)
    _write("spans")
    holder = MultiSink(a, SlowZ("Z")) if mode == "multi" else Slow(a)
    _unconfirmed(monkeypatch, a, holder)
    _write("spans")
    started = time.monotonic()
    log_foundry.shutdown(timeout=0.5)
    join_graced()
    return a, started, marks


def test_a_slow_callers_wrapper_delays_its_member_and_a_shipped_one_does_not(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-002 AC3 at 1 s: behind a slow caller's wrapper A closes after it; behind a shipped one,
    before its slow member Z has finished closing.

    ``slow``'s floor is safe against a loaded machine: the wrapper's 1 s sleep starts after the
    clock is read, so A, closed after it returns, cannot enter sooner. ``multi`` asserts an order,
    not a time: A entered before Z's close returned, which ordering A behind the shipped
    ``MultiSink`` would invert, since its forward then skips A and A waits for Z.
    """
    a, started, marks = _slow_holder("slow", monkeypatch)
    assert a.closes == 1 and marks, (a, marks)
    assert a.entered_at[0] >= marks[0] and a.entered_at[0] - started >= 1.0, (a, started, marks)
    _lifecycle._shutdown_worker()


def test_a_shipped_holder_does_not_delay_its_member(monkeypatch: pytest.MonkeyPatch) -> None:
    """FR-002 AC3's ``multi`` row: a shipped holder is never ordered, so A is not held for Z."""
    a, _started, marks = _slow_holder("multi", monkeypatch)
    assert a.closes == 1 and marks, (a, marks)
    assert a.entered_at[0] < marks[0], (a.entered_at, marks)


# -- FR-003: every twin path, an interrupt, a fork ---------------------------------------------

_EXIT_CHILD = """
import atexit, json, threading
state = {{}}


def report():
    a = state["a"]
    print(json.dumps(dict(at_entry=a.at_entry, by_wrapper=a.by_wrapper, overlaps=a.overlaps)))


atexit.register(report)
import log_foundry

mark = threading.local()


class Rec:
    def __init__(self):
        self.writes = 0
        self.at_entry = []
        self.by_wrapper = []
        self.closing = 0
        self.overlaps = 0

    def emit(self, batch):
        self.writes += 1

    def close(self):
        self.at_entry.append(self.writes)
        self.by_wrapper.append(bool(getattr(mark, "on", False)))
        self.closing += 1
        self.overlaps += self.closing > 1
        threading.Event().wait(0.01)
        self.closing -= 1


class Leaves:
    def __init__(self, inner):
        self.inner = inner

    def emit(self, batch):
        self.inner.emit(batch)

    def close(self):
        pass


class Mine(Leaves):
    def close(self):
        mark.on = True
        try:
            self.inner.close()
        finally:
            mark.on = False


@log_foundry.trace
def span():
    pass


def write(n=1):
    for _ in range(n):
        span() if {path!r} == "spans" else log_foundry.info("x")
    if {path!r} == "spans":
        log_foundry.flush(timeout=5.0)


a = Rec()
state["a"] = a
log_foundry.configure(service="exit", sink=a)
write()
log_foundry.configure(sink={wrapper}(a))
write(2)
"""
"""The exit-drain child: its reporter is registered before the library's handler, so, ``atexit``
being last in, first out, it reads A's record after the drain."""


@pytest.mark.parametrize("path", ["orphan", "spans"])
@pytest.mark.parametrize("wrapper", ["Leaves", "Mine"])
def test_the_exit_drain_closes_a_member_after_its_holder(wrapper: str, path: str) -> None:
    """FR-003 AC1: FR-001's first criterion ended by the exit drain, in a child interpreter."""
    env = dict(os.environ, PYTHONPATH=str(ROOT / "src"))
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-B", "-c", _EXIT_CHILD.format(path=path, wrapper=wrapper)],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    record = json.loads(result.stdout.strip().splitlines()[-1])
    expected = [True, False] if wrapper == "Mine" else [False]
    assert record["by_wrapper"] == expected and record["overlaps"] == 0, record
    assert len(set(record["at_entry"])) == 1, record


@pytest.mark.parametrize("wrapper", ["Leaves", "Mine"])
def test_a_child_forked_after_a_callers_wrapper_closes_nothing(wrapper: str) -> None:
    """FR-003 AC2: the child writes once and shuts down, closing A zero times; then the parent
    closes A as FR-001 says.

    ``releasable`` refuses the inherited wrapper before any after-holder close could run, and the
    child cleared both records in ``_clear_after_fork``.
    """
    a = Rec("A")
    log_foundry.configure(service="fork", sink=a)
    log_foundry.info("one")
    holder = Mine(a) if wrapper == "Mine" else Leaves(a)
    log_foundry.configure(sink=holder)

    def child() -> str:
        before = a.closes
        written = _lifecycle._state._closes.was_written(a)
        log_foundry.info("child")
        log_foundry.shutdown(timeout=5.0)
        in_flight = len(_lifecycle._state._closes.in_flight_ids())
        return json.dumps({"closed": a.closes - before, "written": written, "in_flight": in_flight})

    reaped = run_in_child(child)
    assert json.loads(reaped.output) == {"closed": 0, "written": False, "in_flight": 0}, reaped
    log_foundry.info("parent")
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    expected = [True, False] if wrapper == "Mine" else [False]
    assert a.by_wrapper == expected and a.overlaps == 0, a


def test_a_child_forked_after_an_unconfirmed_swap_into_mine_discharges_the_ordered_member(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-003's refused holder: the child's take orders A behind the inherited ``Mine(A)``.

    ``releasable`` refuses ``Mine(A)`` in the child, so its release closes nothing and nothing
    closes after it; the members ordered behind it are released on their own, refused in turn,
    and each entry is discharged. Without that, A's entry stays in flight in the child.
    """
    a = Rec("A")
    log_foundry.configure(service="fork-ordered", sink=a)
    _write("spans")
    _unconfirmed(monkeypatch, a, Mine(a))
    assert _lifecycle._state._closes.owes(a), "the premise: the record owes A and Mine(A)"

    def child() -> str:
        before = a.closes
        log_foundry.shutdown(timeout=5.0)
        in_flight = len(_lifecycle._state._closes.in_flight_ids())
        return json.dumps({"closed": a.closes - before, "in_flight": in_flight})

    reaped = run_in_child(child)
    assert json.loads(reaped.output) == {"closed": 0, "in_flight": 0}, reaped
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert a.by_wrapper == [True, False] and a.overlaps == 0, a


def _interrupt_choice(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raises ``KeyboardInterrupt`` from the next ``_inline_close_choice``: after the take."""
    real = _lifecycle._inline_close_choice
    fired: list[int] = []

    def boom(*args: Any, **kwargs: Any) -> Any:
        if not fired:
            fired.append(1)
            raise KeyboardInterrupt("injected after the take, before the hand-off")
        return real(*args, **kwargs)

    monkeypatch.setattr(_lifecycle, "_inline_close_choice", boom)


def test_an_interrupt_after_the_take_keeps_the_member_attached_to_the_put_back_holder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-003 AC3 (``spec101_interrupt.py``): ``Leaves(A)`` is put back, A stays attached to it,
    and the next ``shutdown()`` closes A once."""
    a = Rec("A")
    log_foundry.configure(service="interrupt", sink=a)
    log_foundry.info("one")
    holder = Leaves(a)
    log_foundry.configure(sink=holder)
    log_foundry.info("two")
    _interrupt_choice(monkeypatch)
    with pytest.raises(KeyboardInterrupt):
        log_foundry.shutdown(timeout=2.0)
    monkeypatch.undo()
    assert _lifecycle._state._closes.owes(holder) and a.closes == 0, a
    with _lifecycle._state._closes._lock:
        entry = _lifecycle._state._closes._after[id(holder)]
        attached = [(m.referent(), m.closing) for m in entry[1]]
    assert attached == [(a, None)], attached
    log_foundry.shutdown(timeout=2.0)
    join_graced()
    assert a.at_entry == [2], a


def test_an_interrupt_puts_back_every_member_ordered_behind_a_put_back_holder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-003, the put-back is transitive: ``Mine2(Mine1(A))``, all three owed and taken.

    The take orders A behind ``Mine1`` and ``Mine1`` behind ``Mine2``; the interrupt then lands
    before ``Mine2`` is handed off. Each is put back with its entry discharged, so nothing is left
    in flight, and all three are owed for the next call.
    """
    a = Rec("A")
    log_foundry.configure(service="nested", sink=a)
    _write("spans")
    mine1 = Mine(a)
    _unconfirmed(monkeypatch, a, mine1)
    mine2 = Mine(mine1)
    _unconfirmed(monkeypatch, a, mine2)
    closes = _lifecycle._state._closes
    assert closes.owes(a) and closes.owes(mine1) and closes.owes(mine2), "the premise"
    _interrupt_choice(monkeypatch)
    with pytest.raises(KeyboardInterrupt):
        log_foundry.shutdown(timeout=2.0)
    monkeypatch.undo()
    assert closes.in_flight_ids() == set(), closes.in_flight_ids()
    assert closes.owes(a) and closes.owes(mine1) and closes.owes(mine2), a
    assert a.closes == 0, a
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert a.closes >= 1 and a.overlaps == 0 and not closes.owed, a


def test_a_member_close_raising_keyboard_interrupt_puts_back_the_ordered_rest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-003 AC4: ``Pair(M1, M2)`` takes both members ordered; M1's close raises an interrupt.

    The interrupt reaches the caller, as an inline close's does, and M2, ordered behind the same
    holder and not yet closed, is owed when the call returns, with no close of it in flight.
    """
    m1, m2 = Rec("M1"), Rec("M2")
    log_foundry.configure(service="member-interrupt", sink=m1)
    _write("spans")
    _unconfirmed(monkeypatch, m1, m2)
    pair = Pair(m1, m2)
    _unconfirmed(monkeypatch, m2, pair)
    closes = _lifecycle._state._closes
    assert closes.owes(m1) and closes.owes(m2) and closes.owes(pair), "the premise"
    m1.raises = KeyboardInterrupt("from the member's close")
    with pytest.raises(KeyboardInterrupt):
        log_foundry.shutdown(timeout=5.0)
    m1.raises = None
    assert m1.closes == 1 and m2.closes == 0, (m1, m2)
    assert closes.owes(m2) and closes.in_flight_ids() == set(), closes.in_flight_ids()
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    assert m2.closes == 1 and m2.overlaps == 0, m2


@pytest.mark.parametrize("wrapper", ["Leaves", "Mine"])
def test_a_member_ordered_once_stays_attached(
    wrapper: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The plan review's M2: an ordered attachment sits beside the persistent one.

    ``configure(A)``, a write, an unconfirmed ``configure(W(A))``, two span writes,
    ``shutdown()``, one orphan log, ``shutdown()``. The first ``shutdown()`` orders A behind W and
    closes it once; the persistent attachment made at the configure outlives that, so the second
    ``shutdown()``, which closes W again for the orphan log, closes A again after it.
    """
    a = Rec("A")
    log_foundry.configure(service="ordered-once", sink=a)
    _write("spans")
    holder = Mine(a) if wrapper == "Mine" else Leaves(a)
    _unconfirmed(monkeypatch, a, holder)
    _write("spans", 2)
    log_foundry.shutdown(timeout=5.0)
    first = list(a.at_entry)
    log_foundry.info("orphan")
    log_foundry.shutdown(timeout=5.0)
    join_graced()
    per = 2 if wrapper == "Mine" else 1
    assert len(first) == per and len(a.at_entry) == 2 * per, a
    assert a.at_entry[-1] == a.writes > first[-1] and a.overlaps == 0, a


@pytest.mark.parametrize("wrapper", ["Leaves", "Mine"])
def test_a_detached_stop_closes_the_member_after_its_holder(wrapper: str) -> None:
    """FR-003, SPEC-097's command path: a stop deferred during a sink change runs at its end.

    The wrapper's stop-signal setter calls ``shutdown()`` while ``configure(W(A))`` is handing it
    its signal; the change defers that stop and runs it at its end, which closes W. A is closed on
    the thread that closed W, after it.
    """
    a = Rec("A")

    class Stopping:
        def __init__(self, inner: Rec) -> None:
            self.inner = inner
            self.stopped = False
            self.closed_by: list[str] = []

        @property
        def log_foundry_stop_signal(self) -> threading.Event | None:
            return None

        @log_foundry_stop_signal.setter
        def log_foundry_stop_signal(self, event: threading.Event | None) -> None:
            if not self.stopped:
                self.stopped = True
                log_foundry.shutdown(timeout=2.0)

        def emit(self, batch: list[dict[str, object]]) -> None:
            self.inner.emit(batch)

        def close(self) -> None:
            self.closed_by.append(threading.current_thread().name)
            if wrapper == "Mine":
                _MARK.on = True
                try:
                    self.inner.close()
                finally:
                    _MARK.on = False

    log_foundry.configure(service="detached", sink=a)
    log_foundry.info("one")
    holder = Stopping(a)
    log_foundry.configure(sink=holder)  # type: ignore[arg-type]
    join_graced()
    assert holder.stopped and holder.closed_by, holder.closed_by
    expected = [True, False] if wrapper == "Mine" else [False]
    assert a.by_wrapper == expected and a.overlaps == 0, a
    assert a.threads[-1] == holder.closed_by[-1], (a.threads, holder.closed_by)
