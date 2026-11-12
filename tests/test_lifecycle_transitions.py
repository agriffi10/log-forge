"""SPEC-078 FR-003 and FR-004: the stored state, and an answer for every lifecycle call in every state.

The transition table is six calls by five states. Every cell is driven here by a test registered
with :func:`cell`, and :func:`test_every_cell_of_the_table_has_a_test` fails on a cell with none.
A cell in the ``SWAPPING`` or ``DRAINING`` column is reached by holding the call that put the
process there inside, at an injected preemption point in the style of
``tests/test_lifecycle_races.py``:

- ``Worker.retarget``, parked on entry, for a swap with a live worker;
- a superseded sink's ``close()``, parked, for a swap with **no** worker, which ``BUILD`` needs;
- ``_lifecycle._close_owed``, parked before its first take, for a drain inside its window;
- ``_CloseRegistry.wait_graced``, parked at the call whose ``closes`` is ``None``, for a drain's
  tail, after it has lowered ``_shutdown_running``.

"→" in the spec's table is what ``state`` reads when the call returns, and in the two transient
columns the call that put the process there is still inside, so each such test reads ``state``
twice: once while the holder is parked, once after it has left.

The rest of the file is FR-003's and FR-004's other criteria: the AST scan of where ``_current``
is written and ``_move`` is called, with its fixture corpus; the docstrings that state both shapes
of a span after ``shutdown()``; and a child forked with a swap and a shutdown in flight.
"""

from __future__ import annotations

import ast
import itertools
import pathlib
import re
import threading
import time
from collections import Counter
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry
from conftest import join_graced
from log_foundry import _lifecycle
from log_foundry._lifecycle import State, _Op
from log_foundry.worker import Worker
from test_fork_lifecycle import run_in_child

if TYPE_CHECKING:
    from collections.abc import Callable

_SRC = pathlib.Path(_lifecycle.__file__).resolve().parent
GATE_WAIT = 10.0
"""Seconds a test waits for a parked thread to arrive, or to leave once released."""

CELLS: dict[tuple[_Op, State], list[str]] = {}
"""Every test that drives a cell of the table, by cell."""

RECORDERS: list[Rec] = []
"""Every recorder the running test built, for the accounting check after it."""

SENT: list[str] = []
"""Every message the running test logged, through :func:`span` or :func:`log`."""


@pytest.fixture(autouse=True)
def every_event_is_accounted_for_and_every_written_sink_closed() -> Any:
    """FR-004 AC-7, after every test in this file: invariants 2 and 5 hold in every cell.

    After the test, and after everything it parked has been released, the process is shut down
    and its detached closes given the grace. Then every message the test logged is either
    delivered to a recorder, at most once, or counted once — a span by
    ``submitted_after_shutdown``, an orphan log or a span's buffered event by ``orphan_lost`` or
    ``in_span_lost`` — and every recorder something was written to has been closed. A cell whose
    event vanished, or whose sink was left open, fails here even when its own assertions pass.
    """
    RECORDERS.clear()
    SENT.clear()
    yield
    log_foundry.shutdown(timeout=5.0)
    join_graced(5.0)
    sent = Counter(SENT)
    delivered = Counter(
        str(event.get("message"))
        for recorder in RECORDERS
        for event in recorder.events
        if event.get("message") in sent
    )
    twice = {message: n for message, n in delivered.items() if n > sent[message]}
    assert not twice, f"invariant 2: delivered more often than logged: {twice}"
    health = log_foundry.health()
    counted = health.submitted_after_shutdown + health.orphan_lost + health.in_span_lost
    undelivered = sum((sent - delivered).values())
    assert undelivered == counted, (
        f"invariant 2: {undelivered} logged event(s) were neither delivered nor counted once "
        f"(counted {counted}); sent {dict(sent)}, delivered {dict(delivered)}"
    )
    unclosed = [recorder for recorder in RECORDERS if recorder.events and not recorder.closes]
    assert not unclosed, f"invariant 5: written to and never closed: {unclosed}"


def cell(op: _Op, state: State) -> Callable[[Callable[..., None]], Callable[..., None]]:
    """Registers the decorated test as driving the cell ``(op, state)``."""

    def register(test: Callable[..., None]) -> Callable[..., None]:
        CELLS.setdefault((op, state), []).append(test.__name__)
        return test

    return register


class Rec:
    """A sink that keeps its events and records each close and the thread that ran it."""

    log_foundry_stop_signal: threading.Event | None = None

    def __init__(self, name: str) -> None:
        self.name = name
        self.events: list[dict[str, object]] = []
        self.closed_on: list[str] = []
        RECORDERS.append(self)

    @property
    def closes(self) -> int:
        """How many times it was closed."""
        return len(self.closed_on)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Keeps the batch."""
        self.events.extend(batch)

    def close(self) -> None:
        """Records the closing thread's name."""
        self.closed_on.append(threading.current_thread().name)

    def __repr__(self) -> str:
        """Names it, with its counts."""
        return f"<{self.name} events={len(self.events)} closed_on={self.closed_on}>"


class ParkedCloseRec(Rec):
    """A recorder whose first ``close()`` parks until released, so a swap stays inside its join."""

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.entered = threading.Event()
        self.release = threading.Event()

    def close(self) -> None:
        """Parks on the first call, then records."""
        if not self.closed_on and not self.entered.is_set():
            self.entered.set()
            self.release.wait(GATE_WAIT * 3)
        super().close()


class Gate:
    """A preemption point: the parked thread sets ``entered`` and waits for ``release``."""

    def __init__(self) -> None:
        self.entered = threading.Event()
        self.release = threading.Event()
        self.used = False

    def park(self) -> None:
        """Parks the calling thread, the first time only."""
        if self.used:
            return
        self.used = True
        self.entered.set()
        assert self.release.wait(GATE_WAIT * 3), "a parked call was never released"


@log_foundry.trace
def _traced(message: str) -> None:
    log_foundry.info(message)


def span(message: str = "span") -> None:
    """One traced call carrying one event, recorded for the accounting check."""
    SENT.append(message)
    _traced(message)


def log(message: str) -> None:
    """One orphan log, recorded for the accounting check."""
    SENT.append(message)
    log_foundry.info(message)


def state() -> State:
    """What ``state`` reports now."""
    return _lifecycle._state.state


def current() -> State:
    """The stored settled state, read directly."""
    return _lifecycle._state._current


def background(work: Callable[[], object]) -> threading.Thread:
    """Starts ``work`` on a daemon thread and returns it."""
    thread = threading.Thread(target=work, daemon=True)
    thread.start()
    return thread


def finish(thread: threading.Thread) -> None:
    """Joins a background call, failing rather than hanging if it never leaves."""
    thread.join(GATE_WAIT)
    assert not thread.is_alive(), "a parked call did not leave once released"


def park_retarget(monkeypatch: pytest.MonkeyPatch) -> Gate:
    """Parks the first ``Worker.retarget`` on entry, before it drains anything."""
    gate = Gate()
    real = Worker.retarget

    def parked(self: Worker, new_sink: Any, deadline: float | None = None) -> Any:
        gate.park()
        return real(self, new_sink, deadline)

    monkeypatch.setattr(Worker, "retarget", parked)
    return gate


def park_close_owed(monkeypatch: pytest.MonkeyPatch) -> Gate:
    """Parks the first ``_close_owed`` call before its take: a drain inside its window."""
    gate = Gate()
    real = _lifecycle._close_owed

    def parked(*args: Any, **kwargs: Any) -> bool:
        gate.park()
        return real(*args, **kwargs)

    monkeypatch.setattr(_lifecycle, "_close_owed", parked)
    return gate


def park_the_tail(monkeypatch: pytest.MonkeyPatch, then: Callable[[], None] | None = None) -> Gate:
    """Parks the shutdown's last graced join, identified by ``closes is None``: a drain's tail."""
    gate = Gate()
    real = _lifecycle._CloseRegistry.wait_graced

    def parked(self: Any, op: Any, closes: Any) -> None:
        if closes is None and not gate.used:
            if then is not None:
                gate.used = True
                then()
            else:
                gate.park()
        real(self, op, closes)

    monkeypatch.setattr(_lifecycle._CloseRegistry, "wait_graced", parked)
    return gate


def worker_on(sink: Rec) -> Worker:
    """Configures ``sink`` and builds the process worker on it through one span."""
    log_foundry.configure(service="cells", sink=sink)
    span("build")
    log_foundry.flush(timeout=5.0)
    worker = _lifecycle._state.live_worker()
    assert worker is not None
    return worker


def orphan_on(sink: Rec) -> None:
    """Configures ``sink`` and arms it through one orphan log, with no worker."""
    log_foundry.configure(service="cells", sink=sink)
    log("orphan")
    assert _lifecycle._state.worker_exists() is None


def derived_current(runtime: Any) -> State:
    """Rules 3 to 6 without the config term, written here, liveness from the epoch directly."""
    worker = runtime._worker
    if worker is not None and runtime.retirements <= worker._epoch:
        return State.RUNNING
    if runtime.retirements > 0:
        return State.CLOSED
    if runtime._closes.owed:
        return State.RUNNING
    return State.UNCONFIGURED


# --------------------------------------------------------------------------- BUILD


@cell(_Op.BUILD, State.UNCONFIGURED)
def test_build_from_unconfigured_builds_and_arms() -> None:
    a = Rec("A")
    log_foundry.configure(service="cells", sink=a)
    assert _lifecycle._state._column() is State.UNCONFIGURED
    span()
    assert _lifecycle._state.worker_exists() is not None
    assert id(a) in _lifecycle._state._closes.owed, "the build arms its sink"
    assert state() is State.RUNNING and current() is State.RUNNING


@cell(_Op.BUILD, State.RUNNING)
def test_build_from_running_builds() -> None:
    a = Rec("A")
    orphan_on(a)
    assert current() is State.RUNNING
    span()
    assert _lifecycle._state.live_worker() is not None
    assert state() is State.RUNNING


@cell(_Op.BUILD, State.SWAPPING)
def test_build_during_a_swap_builds_and_the_swap_continues() -> None:
    old, new = ParkedCloseRec("old"), Rec("new")
    orphan_on(old)
    swap = background(lambda: log_foundry.configure(sink=new))
    try:
        assert old.entered.wait(GATE_WAIT), "the swap is inside its join of the superseded close"
        assert state() is State.SWAPPING
        span("built")
        worker = _lifecycle._state.worker_exists()
        assert worker is not None and worker.sink is new, "a worker built, on the configured sink"
        assert state() is State.SWAPPING, "the swap is still inside"
    finally:
        old.release.set()
    finish(swap)
    assert state() is State.RUNNING
    log_foundry.flush(timeout=5.0)
    assert any(event.get("message") == "built" for event in new.events), new
    assert old.closes == 1, "the superseded sink was closed once the swap left"


@cell(_Op.BUILD, State.DRAINING)
def test_build_inside_a_drains_window_is_the_late_worker_it_stops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The in-window half. Deleting ``_move`` from the late-worker section reddens this."""
    a = Rec("A")
    orphan_on(a)
    gate = park_close_owed(monkeypatch)
    stop = background(lambda: log_foundry.shutdown(timeout=5.0))
    try:
        assert gate.entered.wait(GATE_WAIT)
        span()
        worker = _lifecycle._state.worker_exists()
        assert worker is not None and _lifecycle._state._late_worker is worker
        assert state() is State.DRAINING
    finally:
        gate.release.set()
    finish(stop)
    assert _lifecycle._state.live_worker() is None, "the drain stopped the worker it registered"
    assert state() is State.CLOSED and current() is State.CLOSED


@cell(_Op.BUILD, State.DRAINING)
def test_build_in_a_drains_tail_is_a_live_worker_the_drain_does_not_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The tail half: built after ``_shutdown_running`` was lowered, it delivers."""
    a = Rec("A")
    orphan_on(a)
    gate = park_the_tail(monkeypatch)
    stop = background(lambda: log_foundry.shutdown(timeout=5.0))
    try:
        assert gate.entered.wait(GATE_WAIT)
        span()
        worker = _lifecycle._state.worker_exists()
        assert worker is not None and _lifecycle._state._late_worker is not worker
        assert state() is State.DRAINING
    finally:
        gate.release.set()
    finish(stop)
    assert _lifecycle._state.live_worker() is worker, "the tail-built worker stays live"
    assert state() is State.RUNNING and current() is State.RUNNING


@cell(_Op.BUILD, State.CLOSED)
def test_build_after_a_worker_less_shutdown_builds_a_live_worker_that_delivers() -> None:
    """FR-004 AC-4, the first shape: ``CLOSED`` returns to ``RUNNING`` (SPEC-044 FR-001)."""
    a = Rec("A")
    orphan_on(a)
    log_foundry.shutdown(timeout=5.0)
    assert state() is State.CLOSED
    span("after")
    log_foundry.flush(timeout=5.0)
    assert any(event.get("message") == "after" for event in a.events), a
    assert state() is State.RUNNING and current() is State.RUNNING


@cell(_Op.BUILD, State.CLOSED)
def test_a_span_after_a_workers_shutdown_is_counted_and_stays_closed() -> None:
    """FR-004 AC-4, the second shape: no build, a count, and ``CLOSED`` (SPEC-030)."""
    worker_on(Rec("A"))
    log_foundry.shutdown(timeout=5.0)
    before = log_foundry.health().submitted_after_shutdown
    span("after")
    assert log_foundry.health().submitted_after_shutdown == before + 1
    assert state() is State.CLOSED and current() is State.CLOSED


# --------------------------------------------------------------------------- ARM


@cell(_Op.ARM, State.UNCONFIGURED)
def test_arm_from_unconfigured_arms() -> None:
    a = Rec("A")
    log_foundry.configure(service="cells", sink=a)
    log("first")
    assert id(a) in _lifecycle._state._closes.owed
    assert state() is State.RUNNING and current() is State.RUNNING


@cell(_Op.ARM, State.RUNNING)
def test_arm_in_running_arms_a_sink_not_owed() -> None:
    worker_on(Rec("A"))
    c = Rec("C")
    _lifecycle._note_orphan_emit(c)
    assert id(c) in _lifecycle._state._closes.owed
    assert state() is State.RUNNING


@cell(_Op.ARM, State.SWAPPING)
def test_arm_during_a_swap_arms(monkeypatch: pytest.MonkeyPatch) -> None:
    worker_on(Rec("A"))
    gate = park_retarget(monkeypatch)
    swap = background(lambda: log_foundry.configure(sink=Rec("B")))
    c = Rec("C")
    try:
        assert gate.entered.wait(GATE_WAIT)
        _lifecycle._note_orphan_emit(c)
        assert id(c) in _lifecycle._state._closes.owed
        assert state() is State.SWAPPING
    finally:
        gate.release.set()
    finish(swap)
    assert state() is State.RUNNING


@cell(_Op.ARM, State.DRAINING)
def test_arm_during_a_drain_arms_and_the_drain_closes_it(monkeypatch: pytest.MonkeyPatch) -> None:
    worker_on(Rec("A"))
    gate = park_close_owed(monkeypatch)
    stop = background(lambda: log_foundry.shutdown(timeout=5.0))
    c = Rec("C")
    try:
        assert gate.entered.wait(GATE_WAIT)
        _lifecycle._note_orphan_emit(c)
        assert id(c) in _lifecycle._state._closes.owed
        assert state() is State.DRAINING
    finally:
        gate.release.set()
    finish(stop)
    assert c.closes == 1, "the drain closed what was armed inside it"
    assert state() is State.CLOSED


@cell(_Op.ARM, State.CLOSED)
def test_arm_after_a_shutdown_arms_again_and_stays_closed() -> None:
    a = Rec("A")
    orphan_on(a)
    log_foundry.shutdown(timeout=5.0)
    log("after")
    assert id(a) in _lifecycle._state._closes.owed, "owed a further close (SPEC-045 FR-002)"
    assert state() is State.CLOSED and current() is State.CLOSED


# --------------------------------------------------------------------------- SWAP


@cell(_Op.SWAP, State.UNCONFIGURED)
def test_swap_from_unconfigured_has_nothing_to_swap() -> None:
    a, b = Rec("A"), Rec("B")
    log_foundry.configure(service="cells", sink=a)
    log_foundry.configure(sink=b)
    assert not _lifecycle._state._closes.owed and not _lifecycle._state._swaps_running
    assert state() is State.RUNNING, "the config has resolved a sink"
    assert current() is State.UNCONFIGURED, "and the runtime owes nothing"


@cell(_Op.SWAP, State.RUNNING)
def test_swap_in_running_retargets(monkeypatch: pytest.MonkeyPatch) -> None:
    worker = worker_on(Rec("A"))
    b = Rec("B")
    inside: list[State] = []
    real = Worker.retarget

    def sampled(self: Worker, new_sink: Any, deadline: float | None = None) -> Any:
        inside.append(state())
        return real(self, new_sink, deadline)

    monkeypatch.setattr(Worker, "retarget", sampled)
    log_foundry.configure(sink=b)
    assert inside == [State.SWAPPING], "a swap with work passes through SWAPPING"
    assert worker.sink is b
    assert state() is State.RUNNING


@cell(_Op.SWAP, State.SWAPPING)
def test_a_second_swap_waits_for_the_first_then_swaps(monkeypatch: pytest.MonkeyPatch) -> None:
    """The cell's answer since ``config._sink_lock``: wait for the swap in flight, then swap.

    It was "runs alongside the swap in flight" at SPEC-078, and that interleaving is what closed a
    sink twice (``tests/test_concurrent_configure.py``). The second call is held on the lock while
    the first is parked inside its retarget, so neither the worker nor the config moves to it;
    released, the first finishes and the second swaps as in ``RUNNING``.
    """
    a = Rec("A")
    worker = worker_on(a)
    b, c = Rec("B"), Rec("C")
    gate = park_retarget(monkeypatch)
    first = background(lambda: log_foundry.configure(sink=b))
    try:
        assert gate.entered.wait(GATE_WAIT)
        second = background(lambda: log_foundry.configure(sink=c))
        second.join(0.3)
        assert second.is_alive(), "the second swap waits for the first rather than running"
        assert worker.sink is a, "nothing has retargeted the worker yet"
        assert log_foundry.get_config().sink is b, "the config names the first call's sink"
        assert state() is State.SWAPPING, "the first swap is still inside"
    finally:
        gate.release.set()
    finish(first)
    finish(second)
    assert worker.sink is c, "the second swap, run after the first, retargeted last"
    assert log_foundry.get_config().sink is c, "and the config names the sink delivery goes to"
    assert state() is State.RUNNING
    log_foundry.shutdown(timeout=5.0)
    join_graced(5.0)
    assert (a.closes, b.closes, c.closes) == (1, 1, 1), "each sink closed once"


class SlowCloseRec(Rec):
    """A recorder whose close takes long enough that a swap joining it is visible."""

    CLOSE_SECONDS = 2.0

    def close(self) -> None:
        """Takes its time, then records."""
        time.sleep(self.CLOSE_SECONDS)
        super().close()


class _Recording(set):  # type: ignore[type-arg]
    """A token set that remembers every token ever added to it."""

    def __init__(self) -> None:
        super().__init__()
        self.added: list[object] = []

    def add(self, token: object) -> None:
        self.added.append(token)
        super().add(token)


@cell(_Op.SWAP, State.DRAINING)
@pytest.mark.parametrize("path", ["worker", "orphan"])
def test_a_swap_during_a_drain_with_no_live_worker_declines(
    path: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FR-004 AC-2, the changed cell. A closes on the calling thread; B is never closed.

    At ``fda134a``, with the swap landing between ``Worker.stop`` returning and the take, A's
    close ran on ``log-foundry-sink-close`` and B's on the calling thread; without the swap A
    closed on the calling thread and B never. The swap now declines at entry: no token, no
    retarget, no close and no arm.
    """
    a, b = SlowCloseRec("A"), Rec("B")
    if path == "worker":
        worker_on(a)
    else:
        orphan_on(a)
    recording = _Recording()
    monkeypatch.setattr(_lifecycle._state, "_swaps_running", recording)
    real = _lifecycle._close_owed
    seen: dict[str, object] = {}

    def swap_first(*args: Any, **kwargs: Any) -> bool:
        if not seen:
            seen["column"] = _lifecycle._state._column()
            started = time.monotonic()
            log_foundry.configure(sink=b)
            seen["took"] = time.monotonic() - started
            seen["owed"] = id(b) in _lifecycle._state._closes.owed
            seen["tokens"] = list(recording.added)
        return real(*args, **kwargs)

    monkeypatch.setattr(_lifecycle, "_close_owed", swap_first)
    caller = threading.current_thread().name
    log_foundry.shutdown(timeout=5.0)

    assert seen["column"] is State.DRAINING, "the premise: the swap landed inside the drain"
    assert seen["tokens"] == [] and seen["owed"] is False, "the swap declined at entry"
    assert seen["took"] < 1.0, (
        f"configure(sink=B) took {seen['took']:.2f}s: a swap that did not decline joins A's "
        f"{SlowCloseRec.CLOSE_SECONDS}s close"
    )
    assert a.closed_on == [caller], f"A's close stayed on the calling thread: {a}"
    assert b.closes == 0, f"B, which nothing was written to, was closed: {b}"


@cell(_Op.SWAP, State.DRAINING)
def test_a_swap_during_a_drain_retargets_the_late_worker(monkeypatch: pytest.MonkeyPatch) -> None:
    """A live worker in the drain still swaps: here the late worker the drain then stops."""
    a, b = Rec("A"), Rec("B")
    orphan_on(a)
    gate = park_close_owed(monkeypatch)
    stop = background(lambda: log_foundry.shutdown(timeout=5.0))
    try:
        assert gate.entered.wait(GATE_WAIT)
        span()
        worker = _lifecycle._state.live_worker()
        assert worker is not None and _lifecycle._state._late_worker is worker
        log_foundry.configure(sink=b)
        assert worker.sink is b, "the late worker was retargeted"
        assert state() is State.DRAINING
    finally:
        gate.release.set()
    finish(stop)
    assert b.closes >= 1, "and the drain closed the sink it adopted"
    assert state() is State.CLOSED


@cell(_Op.SWAP, State.DRAINING)
def test_a_swap_in_a_drains_tail_retargets_the_worker_built_there(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-004 AC-3, the tail probe: a worker built in the tail is retargeted, and C gets spans."""
    b, c = Rec("B"), Rec("C")
    orphan_on(b)
    built: list[Worker] = []

    def in_the_tail() -> None:
        span("tail")
        worker = _lifecycle._state.worker_exists()
        assert worker is not None
        built.append(worker)
        log_foundry.configure(sink=c)

    park_the_tail(monkeypatch, then=in_the_tail)
    log_foundry.shutdown(timeout=5.0)

    assert built, "the premise: a span ran in the tail"
    worker = built[0]
    assert _lifecycle._state.live_worker() is worker, "the tail-built worker is live"
    assert worker.sink is c, "and the swap retargeted it"
    span("next")
    log_foundry.flush(timeout=5.0)
    assert any(event.get("message") == "next" for event in c.events), c
    assert state() is State.RUNNING


@cell(_Op.SWAP, State.DRAINING)
def test_a_swap_after_the_drains_last_take_runs_the_no_worker_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The decline holds only while the drain has a take ahead of it (first diff review).

    In a no-worker drain's tail, after its last take, an orphan log re-arms A and a
    ``configure(sink=C)`` lands. Nothing left in that drain takes A, so declining there left A
    closed once and then owed and open after ``shutdown()`` returned. The swap runs its
    no-worker branch instead: A is released, detached, and C is armed.
    """
    a, c = Rec("A"), Rec("C")
    orphan_on(a)

    def in_the_tail() -> None:
        log("re-armed in the tail")
        log_foundry.configure(sink=c)

    park_the_tail(monkeypatch, then=in_the_tail)
    log_foundry.shutdown(timeout=5.0)
    join_graced(5.0)
    assert id(a) not in _lifecycle._state._closes.owed, f"A was left owed and open: {a}"
    assert a.closes == 2, f"closed by the drain, and again once the swap superseded it: {a}"
    assert id(c) in _lifecycle._state._closes.owed, "and C is armed"


class HookCloseRec(Rec):
    """A recorder that runs a hook inside its first ``close()``, before recording it."""

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.on_first_close: Callable[[], None] | None = None

    def close(self) -> None:
        """Runs the hook once, inside the close, then records."""
        hook, self.on_first_close = self.on_first_close, None
        if hook is not None:
            hook()
        super().close()


@cell(_Op.SWAP, State.DRAINING)
def test_a_swap_during_the_bystander_wait_declines_because_a_second_take_follows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ``waiting`` term of the first pass's last-take guard (second diff review).

    X's swapped-out close is still in flight when the shutdown takes A, so the first pass waits on
    it as a bystander and a second pass will follow. A swap landing in that wait — after an orphan
    log re-armed A — must still decline: the second pass takes A again, on the calling thread. A
    first pass that marked itself the last take let the swap release A onto a daemon and arm B,
    which the second pass then closed though nothing was ever written to it.
    """
    x, a, b = ParkedCloseRec("X"), Rec("A"), Rec("B")
    orphan_on(x)
    monkeypatch.setattr(log_foundry.worker, "DEFAULT_SWAP_TIMEOUT", 0.05)
    log_foundry.configure(sink=a)
    assert x.entered.wait(GATE_WAIT), "the premise: X's close is in flight"
    log("to A")
    real = _lifecycle._CloseRegistry.wait_graced
    fired: list[bool] = []

    def in_the_bystander_wait(self: Any, op: Any, closes: Any) -> None:
        if closes is not None and not fired:
            fired.append(True)
            log("re-arm A")
            log_foundry.configure(sink=b)
            x.release.set()
        real(self, op, closes)

    monkeypatch.setattr(_lifecycle._CloseRegistry, "wait_graced", in_the_bystander_wait)
    caller = threading.current_thread().name
    try:
        log_foundry.shutdown(timeout=5.0)
    finally:
        x.release.set()
    assert fired, "the premise: the shutdown waited on X as a bystander"
    assert b.closes == 0, f"B, which nothing was written to, was closed: {b}"
    assert a.closed_on == [caller, caller], (
        f"A's re-armed close left the calling thread for a daemon: {a.closed_on}"
    )


@cell(_Op.SWAP, State.DRAINING)
def test_a_swap_inside_a_no_worker_drains_inline_close_declines_while_a_late_worker_may_come() -> (
    None
):
    """The ``late_possible`` term of the first pass's last-take guard (second diff review).

    In a no-worker drain the first pass cannot be the last take while a late worker may still be
    registered for it: that is decided only in the section that lowers ``_shutdown_running``. A
    swap landing inside A's inline close, after an orphan log re-armed A, must decline. Taking the
    first pass as the last take let it arm B, and the exit drain then closed B unwritten and moved
    A's second close onto a fan-out thread.
    """
    a, b = HookCloseRec("A"), Rec("B")
    orphan_on(a)

    def inside_the_inline_close() -> None:
        log("re-arm A")
        log_foundry.configure(sink=b)

    a.on_first_close = inside_the_inline_close
    caller = threading.current_thread().name
    log_foundry.shutdown(timeout=5.0)
    assert sorted(s.name for s in _lifecycle._state._closes.owed.values()) == ["A"], (
        "only the re-armed A is owed after the shutdown"
    )
    _lifecycle._shutdown_worker(timeout=5.0)
    assert b.closes == 0, f"B, which nothing was written to, was closed: {b}"
    assert a.closed_on == [caller, caller], (
        f"A's second close left the calling thread: {a.closed_on}"
    )


class SamplingRec(Rec):
    """A recorder that notes the lifecycle state at each close."""

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.states: list[State] = []

    def close(self) -> None:
        """Records the state the close ran in, then the close."""
        self.states.append(state())
        super().close()


@cell(_Op.SWAP, State.CLOSED)
def test_swap_after_a_shutdown_supersedes_what_is_owed_and_arms_the_new_sink() -> None:
    a, b = SamplingRec("A"), Rec("B")
    orphan_on(a)
    log_foundry.shutdown(timeout=5.0)
    log("after")
    log_foundry.configure(sink=b)
    _lifecycle._state._closes.wait_graced(_lifecycle._Operation(_Op.STOP, 5.0), None)
    assert id(b) in _lifecycle._state._closes.owed and id(a) not in _lifecycle._state._closes.owed
    assert a.closes == 2, "closed at the shutdown, and again once superseded (SPEC-045 FR-002)"
    assert a.states == [State.DRAINING, State.SWAPPING], "the supersede ran inside SWAPPING"
    assert state() is State.CLOSED and current() is State.CLOSED


# --------------------------------------------------------------------------- FLUSH


@cell(_Op.FLUSH, State.UNCONFIGURED)
def test_flush_from_unconfigured_touches_nothing() -> None:
    assert log_foundry.flush(timeout=1.0)
    assert _lifecycle._state.worker_exists() is None and not _lifecycle._state._closes.owed
    assert state() is State.UNCONFIGURED and current() is State.UNCONFIGURED


@cell(_Op.FLUSH, State.RUNNING)
def test_flush_in_running_leaves_the_state() -> None:
    worker_on(Rec("A"))
    log_foundry.flush(timeout=5.0)
    assert state() is State.RUNNING


@cell(_Op.FLUSH, State.SWAPPING)
def test_flush_during_a_swap_leaves_the_state(monkeypatch: pytest.MonkeyPatch) -> None:
    worker_on(Rec("A"))
    gate = park_retarget(monkeypatch)
    swap = background(lambda: log_foundry.configure(sink=Rec("B")))
    try:
        assert gate.entered.wait(GATE_WAIT)
        log_foundry.flush(timeout=5.0)
        assert state() is State.SWAPPING
    finally:
        gate.release.set()
    finish(swap)
    assert state() is State.RUNNING, "then as the swap leaves it"


@cell(_Op.FLUSH, State.DRAINING)
def test_flush_during_a_drain_leaves_the_state(monkeypatch: pytest.MonkeyPatch) -> None:
    worker_on(Rec("A"))
    gate = park_close_owed(monkeypatch)
    stop = background(lambda: log_foundry.shutdown(timeout=5.0))
    try:
        assert gate.entered.wait(GATE_WAIT)
        log_foundry.flush(timeout=1.0)
        assert state() is State.DRAINING
    finally:
        gate.release.set()
    finish(stop)
    assert state() is State.CLOSED, "then as the drain leaves it"


@cell(_Op.FLUSH, State.CLOSED)
def test_flush_after_a_shutdown_leaves_the_state() -> None:
    worker_on(Rec("A"))
    log_foundry.shutdown(timeout=5.0)
    log_foundry.flush(timeout=1.0)
    assert state() is State.CLOSED and current() is State.CLOSED


def test_a_worker_a_flush_sweep_builds_moves_the_state_by_the_build_row() -> None:
    """FR-003 AC-8: ``flush()`` moves nothing itself; a worker its sweep builds is a ``BUILD``."""
    a = Rec("A")
    log_foundry.configure(service="cells", sink=a)
    inside: dict[str, object] = {}

    @log_foundry.trace
    def flushes_inside_a_span() -> None:
        log("buffered on the open span")
        assert current() is State.UNCONFIGURED, "the premise: nothing built yet"
        log_foundry.flush(timeout=5.0)
        inside["worker"] = _lifecycle._state.worker_exists()
        inside["current"] = current()

    flushes_inside_a_span()
    assert inside["worker"] is not None, "the sweep built a worker"
    assert inside["current"] is State.RUNNING


# --------------------------------------------------------------------------- STOP


@cell(_Op.STOP, State.UNCONFIGURED)
def test_stop_from_unconfigured_moves_the_count() -> None:
    log_foundry.shutdown(timeout=5.0)
    assert _lifecycle._state.retirements == 1
    assert state() is State.CLOSED and current() is State.CLOSED


@cell(_Op.STOP, State.RUNNING)
def test_stop_in_running_drains_and_closes() -> None:
    a = Rec("A")
    worker_on(a)
    log_foundry.shutdown(timeout=5.0)
    assert a.closes == 1
    assert state() is State.CLOSED and current() is State.CLOSED


@cell(_Op.STOP, State.SWAPPING)
def test_stop_during_a_swap_waits_for_its_decision_and_the_retarget_declines(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The cell's answer since the fix after SPEC-079: retire, wait for the swap, then drain.

    It was "drains and closes; the swap's retarget declines" at SPEC-078, and a shutdown landing
    between a swap's retarget and its decision closed the displaced sink twice and left the one
    installed open (``tests/test_shutdown_mid_swap.py``). The swap is parked on entry to its
    retarget; the shutdown retires the process, then waits for the swap's decision rather than
    draining, so it is still inside when the swap is released. The retarget then declines, the
    swap arms B, and the shutdown closes A and B once each.
    """
    a, b = Rec("A"), Rec("B")
    worker = worker_on(a)
    gate = park_retarget(monkeypatch)
    swap = background(lambda: log_foundry.configure(sink=b))
    try:
        assert gate.entered.wait(GATE_WAIT)
        assert state() is State.SWAPPING, "the swap is inside"
        stop = background(lambda: log_foundry.shutdown(timeout=5.0))
        stop.join(0.3)
        assert stop.is_alive(), "the shutdown waits for the swap's decision"
        assert a.closes == 0, "and has drained and closed nothing yet"
    finally:
        gate.release.set()
    finish(swap)
    finish(stop)
    assert worker.sink is a, "the retarget declined (SPEC-033 FR-002)"
    assert (a.closes, b.closes) == (1, 1), "the shutdown closed the worker's sink and the armed B"
    assert state() is State.CLOSED


@cell(_Op.STOP, State.DRAINING)
def test_a_second_stop_during_a_drain_joins_as_a_second_caller(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    a = Rec("A")
    worker_on(a)
    gate = park_close_owed(monkeypatch)
    stop = background(lambda: log_foundry.shutdown(timeout=5.0))
    try:
        assert gate.entered.wait(GATE_WAIT)
        log_foundry.shutdown(timeout=5.0)
        assert state() is State.DRAINING, "the first call is still inside"
    finally:
        gate.release.set()
    finish(stop)
    assert a.closes == 1, "closed once across both callers"
    assert state() is State.CLOSED


@cell(_Op.STOP, State.CLOSED)
def test_stop_after_a_shutdown_closes_what_was_armed_since() -> None:
    a = Rec("A")
    orphan_on(a)
    log_foundry.shutdown(timeout=5.0)
    log("after")
    log_foundry.shutdown(timeout=5.0)
    assert a.closes == 2
    assert _lifecycle._state.retirements == 2
    assert state() is State.CLOSED and current() is State.CLOSED


# --------------------------------------------------------------------------- FORK


def _child_report() -> str:
    """What a forked child reports: its token sets, its stored state, and that state derived."""
    runtime = _lifecycle._state
    return (
        f"{len(runtime._swaps_running)},{len(runtime._stops_running)},"
        f"{runtime._current.name},{derived_current(runtime).name}"
    )


def _fork_and_read() -> tuple[str, str, str, str]:
    child = run_in_child(_child_report)
    assert child.finished, child.output
    swaps, stops, stored, derived = child.output.split(",")
    return swaps, stops, stored, derived


@cell(_Op.FORK, State.UNCONFIGURED)
def test_fork_from_unconfigured() -> None:
    assert _fork_and_read() == ("0", "0", "UNCONFIGURED", "UNCONFIGURED")


@cell(_Op.FORK, State.RUNNING)
def test_fork_in_running() -> None:
    worker_on(Rec("A"))
    assert _fork_and_read() == ("0", "0", "RUNNING", "RUNNING")


@cell(_Op.FORK, State.SWAPPING)
def test_fork_during_a_swap_inherits_the_stored_state(monkeypatch: pytest.MonkeyPatch) -> None:
    worker_on(Rec("A"))
    gate = park_retarget(monkeypatch)
    swap = background(lambda: log_foundry.configure(sink=Rec("B")))
    try:
        assert gate.entered.wait(GATE_WAIT)
        swaps, stops, stored, derived = _fork_and_read()
    finally:
        gate.release.set()
    finish(swap)
    assert (swaps, stops) == ("0", "0"), "the child inherited a swap token"
    assert stored in {"RUNNING", "CLOSED"} and stored == derived


@cell(_Op.FORK, State.DRAINING)
def test_fork_during_a_drain_inherits_the_stored_state(monkeypatch: pytest.MonkeyPatch) -> None:
    worker_on(Rec("A"))
    gate = park_close_owed(monkeypatch)
    stop = background(lambda: log_foundry.shutdown(timeout=5.0))
    try:
        assert gate.entered.wait(GATE_WAIT)
        swaps, stops, stored, derived = _fork_and_read()
    finally:
        gate.release.set()
    finish(stop)
    assert (swaps, stops) == ("0", "0"), "the child inherited a stop token"
    assert stored == derived == "CLOSED"


@cell(_Op.FORK, State.CLOSED)
def test_fork_after_a_shutdown() -> None:
    worker_on(Rec("A"))
    log_foundry.shutdown(timeout=5.0)
    assert _fork_and_read() == ("0", "0", "CLOSED", "CLOSED")


def test_a_child_forked_with_a_swap_and_a_shutdown_in_flight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-003 AC-7, both at once: no token survives, and the stored state is the derived one."""
    old, new = ParkedCloseRec("old"), Rec("new")
    orphan_on(old)
    swap = background(lambda: log_foundry.configure(sink=new))
    gate = park_close_owed(monkeypatch)
    stop: threading.Thread | None = None
    try:
        assert old.entered.wait(GATE_WAIT), "a swap is inside its join"
        stop = background(lambda: log_foundry.shutdown(timeout=5.0))
        assert gate.entered.wait(GATE_WAIT), "and a shutdown is inside its drain"
        runtime = _lifecycle._state
        assert runtime._swaps_running and runtime._stops_running, "the premise: both in flight"
        swaps, stops, stored, derived = _fork_and_read()
    finally:
        old.release.set()
        gate.release.set()
    finish(swap)
    if stop is not None:
        finish(stop)
    assert (swaps, stops) == ("0", "0"), "the child inherited a token"
    assert stored == derived, f"the child's stored state {stored} is not its derived {derived}"


# --------------------------------------------------------------------------- totality


def test_every_cell_of_the_table_has_a_test() -> None:
    """FR-004 AC-1: the table is total — six calls by five states, thirty cells, each driven."""
    missing = sorted(
        (op.name, st.name) for op, st in itertools.product(_Op, State) if (op, st) not in CELLS
    )
    assert not missing, f"cells with no test: {missing}"
    assert len(CELLS) == len(_Op) * len(State) == 30


def test_the_registry_names_tests_that_exist() -> None:
    """The totality check is not vacuous: every name it holds is a test in this module."""
    names = {name for tests in CELLS.values() for name in tests}
    assert all(callable(globals().get(name)) for name in names), names


# --------------------------------------------------------------------------- FR-003 AC-1

CURRENT_WRITTEN_ELSEWHERE = "_current is written outside Runtime._move and Runtime.__init__"
MOVE_OUTSIDE_THE_FOUR = "_state._move() is called outside the five sites that can change it"
MOVE_MISSING = "a section that can change the stored state does not call _state._move()"
MOVE_BEFORE_A_WRITE = "_state._move() comes before a write of _worker, retirements or the record"

MOVE_SITES = {
    "_get_worker": 1,
    "_note_orphan_emit": 1,
    "_shutdown_worker": 2,
    "_clear_after_fork": 1,
}
"""The four critical sections that can change the stored state, and the child's re-settle.

``_clear_after_fork`` joined at SPEC-079 (FR-004): a fork landing inside one of the four sections,
between a field write and its ``_move``, leaves the child's stored state behind its fields, so the
child's handler re-settles it under the lock the roster replaced.
"""

_WRITES = re.compile(r"^_state\.(_worker|retirements)$")
_RECORD_CALLS = frozenset({"arm", "take", "put_back"})


def _scopes(tree: ast.AST) -> dict[int, str]:
    scopes: dict[int, str] = {}

    def visit(node: ast.AST, qual: tuple[str, ...]) -> None:
        for child in ast.iter_child_nodes(node):
            here = qual
            if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
                here = (*qual, child.name)
            scopes[id(child)] = ".".join(here) or "<module>"
            visit(child, here)

    visit(tree, ())
    return scopes


def _is_move(node: ast.AST) -> bool:
    return isinstance(node, ast.Call) and ast.unparse(node.func) == "_state._move"


def _writes_a_rule_field(node: ast.AST) -> bool:
    if isinstance(node, ast.Assign | ast.AugAssign | ast.AnnAssign):
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        return any(_WRITES.match(ast.unparse(t)) for t in targets)
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in _RECORD_CALLS
        and ast.unparse(node.func.value) == "_state._closes"
    )


def stored_state_findings(source: str) -> list[str]:
    """Every reason the stored state's writer or its call sites break FR-003 AC-1's rule."""
    tree = ast.parse(source)
    scopes = _scopes(tree)
    findings: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign | ast.AugAssign | ast.AnnAssign):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            written = any(isinstance(t, ast.Attribute) and t.attr == "_current" for t in targets)
            if written and scopes[id(node)] not in {"Runtime._move", "Runtime.__init__"}:
                findings.append(CURRENT_WRITTEN_ELSEWHERE)
    counts: dict[str, int] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.With):
            continue
        moves = [n for n in ast.walk(node) if _is_move(n)]
        if not moves:
            continue
        scope = scopes[id(node)]
        counts[scope] = counts.get(scope, 0) + len(moves)
        last_write = max((n.lineno for n in ast.walk(node) if _writes_a_rule_field(n)), default=0)
        if any(move.lineno < last_write for move in moves):
            findings.append(MOVE_BEFORE_A_WRITE)
    loose = [
        n
        for n in ast.walk(tree)
        if _is_move(n)
        and not any(isinstance(w, ast.With) and n in list(ast.walk(w)) for w in ast.walk(tree))
    ]
    if loose:
        findings.append(MOVE_OUTSIDE_THE_FOUR)
    for scope, count in counts.items():
        if count > MOVE_SITES.get(scope, 0):
            findings.append(MOVE_OUTSIDE_THE_FOUR)
    for scope, count in MOVE_SITES.items():
        if counts.get(scope, 0) < count:
            findings.append(MOVE_MISSING)
    return findings


def test_the_stored_state_is_written_in_one_place_and_moved_in_four() -> None:
    """FR-003 AC-1, over the live source: the four sections and, since SPEC-079, the fork handler."""
    source = (_SRC / "_lifecycle.py").read_text(encoding="utf-8")
    assert stored_state_findings(source) == []


_LIVE_SHAPE = """
class Runtime:
    def __init__(self):
        self._current = 0
    def _move(self):
        self._current = 1
def _get_worker():
    with _state._lock:
        _state._worker = worker
        _state._closes.arm(sink)
        _state._move()
def _note_orphan_emit(sink):
    with _state._lock:
        _state._closes.arm(sink)
        _state._move()
def _shutdown_worker():
    with _state._lock:
        _state.retirements += 1
        _state._move()
    with _state._lock:
        _state.retirements += 1
        _state._move()
def _clear_after_fork():
    with _state._lock:
        _state._shutdown_running = 0
        _state._late_worker = None
        _state._closes.arm(worker.sink)
        _state._move()
"""


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (
            lambda s: s + "def _swap_sink():\n    with _state._lock:\n        _state._move()\n",
            MOVE_OUTSIDE_THE_FOUR,
        ),
        (
            lambda s: s.replace(
                "        _state._closes.arm(sink)\n        _state._move()\ndef _note",
                "        _state._move()\n        _state._closes.arm(sink)\ndef _note",
            ),
            MOVE_BEFORE_A_WRITE,
        ),
        (
            lambda s: s.replace(
                "    def _move(self):",
                "    def other(self):\n        self._current = 2\n    def _move(self):",
            ),
            CURRENT_WRITTEN_ELSEWHERE,
        ),
        (
            lambda s: s.replace(
                "def _note_orphan_emit(sink):\n    with _state._lock:\n        _state._closes.arm(sink)\n"
                "        _state._move()\n",
                "def _note_orphan_emit(sink):\n    with _state._lock:\n        _state._closes.arm(sink)\n",
            ),
            MOVE_MISSING,
        ),
        (lambda s: s + "def f():\n    _state._move()\n", MOVE_OUTSIDE_THE_FOUR),
        (
            lambda s: s.replace(
                "        _state._closes.arm(worker.sink)\n        _state._move()\n",
                "        _state._closes.arm(worker.sink)\n",
            ),
            MOVE_MISSING,
        ),
    ],
    ids=[
        "a-fifth-section",
        "before-the-arm",
        "another-writer",
        "a-section-without",
        "unlocked",
        "the-fork-handler-without",
    ],
)
def test_the_stored_state_scan_refuses_each_shape(mutate: Any, reason: str) -> None:
    """Each check of the scan fires on its own shape, asserted by its reason."""
    mutated = mutate(_LIVE_SHAPE)
    assert mutated != _LIVE_SHAPE, "the fixture's mutation did not apply"
    assert reason in stored_state_findings(mutated)


def test_the_stored_state_scan_is_silent_on_the_live_shape() -> None:
    """Half of a gate's regressions are false positives."""
    assert stored_state_findings(_LIVE_SHAPE) == []


# --------------------------------------------------------------------------- FR-004 AC-6

_STALE_SHUTDOWN_CLAIM = re.compile(
    r"(?i)\bthis is terminal\b|terminal by design|nothing restarts"
    r"|shutdown(?:\(\))?`*\s+is terminal\b"
)


def _docstrings(path: pathlib.Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    nodes = [tree, *ast.walk(tree)]
    found = []
    for node in nodes:
        if isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            doc = ast.get_docstring(node)
            if doc:
                found.append(doc)
    return found


def stale_shutdown_claims(docstring: str) -> list[str]:
    """Sentences calling the state after ``shutdown()`` one from which nothing restarts."""
    flat = " ".join(docstring.split())
    return [match.group(0) for match in _STALE_SHUTDOWN_CLAIM.finditer(flat)]


def test_no_docstring_calls_the_state_after_shutdown_terminal() -> None:
    """FR-004 AC-6: ``CLOSED`` keeps its arrows, so no docstring may say nothing restarts."""
    hits = [
        (path.name, claim)
        for path in sorted(_SRC.rglob("*.py"))
        for doc in _docstrings(path)
        for claim in stale_shutdown_claims(doc)
    ]
    assert not hits, hits


@pytest.mark.parametrize(
    ("text", "stale"),
    [
        ("This is terminal — the worker does not come back — and idempotent.", True),
        ("``shutdown()`` is terminal by design, and nothing restarts after it.", True),
        (":meth:`shutdown` is terminal and the worker never comes back.", True),
        ("``CLOSED`` is not terminal: a span builds a live worker.", False),
        ("It retires the background worker it stops — that worker never comes back.", False),
    ],
)
def test_the_terminal_claim_scan_names_what_it_refuses(text: str, stale: bool) -> None:
    """The scan is not vacuous, and it leaves a worker-scoped sentence alone."""
    assert bool(stale_shutdown_claims(text)) is stale, text


def test_the_two_docstrings_state_both_shapes_of_a_later_span() -> None:
    """FR-004 AC-6: ``Runtime`` and the public ``shutdown()`` each state both shapes."""
    shutdown_doc = " ".join((log_foundry.shutdown.__doc__ or "").split())
    runtime_doc = " ".join((_lifecycle.Runtime.__doc__ or "").split())
    assert "starts a fresh worker and is delivered" in shutdown_doc
    assert "submitted_after_shutdown" in shutdown_doc
    assert "builds a live worker" in runtime_doc
    assert "accepted, reported" in runtime_doc


# --------------------------------------------------------------------------- FR-005

TIMEOUT_READ_OUTSIDE_THE_OPERATION = "the call's timeout is read outside its _Operation(...)"
DEADLINE_NOT_FROM_THE_OPERATION = "a stop, retarget, claim_stop or join is not given op.deadline"
STOP_IGNORES_THE_RECORDED_DEADLINE = "Worker.stop's join does not end at the deadline claimed"

_SLOW_STEPS = frozenset({"stop", "retarget", "claim_stop", "join"})
_OPERATION_CALLS = ("_swap_sink", "_shutdown_worker")


def _is_op_deadline(node: ast.AST) -> bool:
    text = ast.unparse(node)
    return text in {"op.deadline", "_remaining(op.deadline)"}


def _function(tree: ast.AST, name: str) -> ast.FunctionDef | None:
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    return None


def deadline_findings(lifecycle_source: str, worker_source: str) -> list[str]:
    """Every reason a slow step of a ``SWAP`` or ``STOP`` does not read its operation's deadline.

    In ``_swap_sink`` and ``_shutdown_worker`` the ``timeout`` parameter is read once, as the
    argument of ``_Operation(...)``, and every call to ``stop``, ``retarget``, ``claim_stop`` or
    ``join`` is passed ``op.deadline`` or ``_remaining(op.deadline)`` — or, for ``stop``, the
    ``claimed=`` verdict of a ``claim_stop(op.deadline)`` in the same function, which records that
    deadline for the join (ruling B1). That route is accepted only while ``Worker.stop`` itself
    joins until the recorded ``self._exit_deadline`` and not until ``timeout``.
    """
    findings: list[str] = []
    tree = ast.parse(lifecycle_source)
    for name in _OPERATION_CALLS:
        function = _function(tree, name)
        if function is None:
            continue
        loads = [
            n
            for n in ast.walk(function)
            if isinstance(n, ast.Name) and n.id == "timeout" and isinstance(n.ctx, ast.Load)
        ]
        constructions = [
            n
            for n in ast.walk(function)
            if isinstance(n, ast.Call)
            and ast.unparse(n.func) == "_Operation"
            and any(isinstance(a, ast.Name) and a.id == "timeout" for a in n.args)
        ]
        if len(loads) != 1 or len(constructions) != 1:
            findings.append(TIMEOUT_READ_OUTSIDE_THE_OPERATION)
        claimed_names = {
            target.id
            for n in ast.walk(function)
            if isinstance(n, ast.Assign)
            for target in n.targets
            if isinstance(target, ast.Name)
            and isinstance(n.value, ast.Call)
            and isinstance(n.value.func, ast.Attribute)
            and n.value.func.attr == "claim_stop"
            and any(_is_op_deadline(a) for a in n.value.args)
        }
        for call in ast.walk(function):
            if not (isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)):
                continue
            if call.func.attr not in _SLOW_STEPS:
                continue
            values = [*call.args, *(kw.value for kw in call.keywords)]
            if any(_is_op_deadline(v) for v in values):
                continue
            claimed = [kw.value for kw in call.keywords if kw.arg == "claimed"]
            if (
                call.func.attr == "stop"
                and claimed
                and isinstance(claimed[0], ast.Name)
                and claimed[0].id in claimed_names
            ):
                continue
            findings.append(DEADLINE_NOT_FROM_THE_OPERATION)
    worker_tree = ast.parse(worker_source)
    stop = next(
        (
            node
            for cls in ast.walk(worker_tree)
            if isinstance(cls, ast.ClassDef) and cls.name == "Worker"
            for node in cls.body
            if isinstance(node, ast.FunctionDef) and node.name == "stop"
        ),
        None,
    )
    if stop is not None:
        recorded = {
            target.id
            for n in ast.walk(stop)
            if isinstance(n, ast.Assign) and "self._exit_deadline" in ast.unparse(n.value)
            for target in n.targets
            if isinstance(target, ast.Name)
        }
        joins = [
            n
            for n in ast.walk(stop)
            if isinstance(n, ast.Call) and ast.unparse(n.func) == "self._thread.join"
        ]
        for join in joins:
            names = {n.id for arg in join.args for n in ast.walk(arg) if isinstance(n, ast.Name)}
            if not names & recorded or "timeout" in names:
                findings.append(STOP_IGNORES_THE_RECORDED_DEADLINE)
        if not joins:
            findings.append(STOP_IGNORES_THE_RECORDED_DEADLINE)
    return findings


def _live(name: str) -> str:
    return (_SRC / name).read_text(encoding="utf-8")


def test_every_slow_step_reads_its_operations_deadline() -> None:
    """FR-005 AC-1 (as amended by ruling B1), over the live sources."""
    assert deadline_findings(_live("_lifecycle.py"), _live("worker.py")) == []


_STOP_LIVE = """
class Worker:
    def stop(self, timeout=None, *, claimed=None):
        own = None if timeout is None else time.monotonic() + timeout
        first = self.claim_stop(own) if claimed is None else claimed
        deadline = self._exit_deadline if first else own
        self._thread.join(None if deadline is None else max(0.0, deadline - time.monotonic()))
"""

_SHUTDOWN_LIVE = """
def _shutdown_worker(timeout=30.0):
    op = _Operation(_Op.STOP, timeout)
    claimed = False
    with _state._lock:
        claimed = worker.claim_stop(op.deadline)
    worker.stop(op.budget, claimed=claimed)
    late_worker.stop(_remaining(op.deadline))
def _swap_sink(new_sink, timeout=5.0):
    op = _Operation(_Op.SWAP, timeout)
    outcome = worker.retarget(new_sink, op.deadline)
    for closer in closers:
        closer.join(_remaining(op.deadline))
"""


@pytest.mark.parametrize(
    ("lifecycle", "worker", "reason"),
    [
        (
            _SHUTDOWN_LIVE.replace(
                "worker.stop(op.budget, claimed=claimed)", "worker.stop(timeout, claimed=claimed)"
            ),
            _STOP_LIVE,
            TIMEOUT_READ_OUTSIDE_THE_OPERATION,
        ),
        (
            _SHUTDOWN_LIVE.replace(
                "worker.stop(op.budget, claimed=claimed)", "worker.stop(timeout, claimed=False)"
            ),
            _STOP_LIVE,
            DEADLINE_NOT_FROM_THE_OPERATION,
        ),
        (
            _SHUTDOWN_LIVE.replace(
                "late_worker.stop(_remaining(op.deadline))", "late_worker.stop()"
            ),
            _STOP_LIVE,
            DEADLINE_NOT_FROM_THE_OPERATION,
        ),
        (
            _SHUTDOWN_LIVE.replace(
                "closer.join(_remaining(op.deadline))", "closer.join(deadline - monotonic())"
            ),
            _STOP_LIVE,
            DEADLINE_NOT_FROM_THE_OPERATION,
        ),
        (
            _SHUTDOWN_LIVE,
            _STOP_LIVE.replace(
                "        deadline = self._exit_deadline if first else own\n",
                "        deadline = time.monotonic() + timeout if first else own\n",
            ),
            STOP_IGNORES_THE_RECORDED_DEADLINE,
        ),
        (
            _SHUTDOWN_LIVE,
            _STOP_LIVE.replace(
                "self._thread.join(None if deadline is None else max(0.0, deadline - "
                "time.monotonic()))",
                "self._thread.join(timeout)",
            ),
            STOP_IGNORES_THE_RECORDED_DEADLINE,
        ),
        (
            _SHUTDOWN_LIVE,
            _STOP_LIVE.replace(
                "self._thread.join(None if deadline is None else max(0.0, deadline - "
                "time.monotonic()))",
                "self._thread.join(timeout if deadline is None else min(timeout, deadline))",
            ),
            STOP_IGNORES_THE_RECORDED_DEADLINE,
        ),
    ],
    ids=[
        "a-raw-timeout-at-the-call-site",
        "a-stop-with-no-claim-and-a-raw-timeout",
        "a-stop-with-no-deadline",
        "a-join-on-a-local-deadline",
        "a-claimed-stop-that-recomputes-its-deadline",
        "a-claimed-stop-that-joins-for-timeout",
        "a-claimed-stop-that-joins-for-the-lesser-of-both",
    ],
)
def test_the_deadline_scan_refuses_each_shape(lifecycle: str, worker: str, reason: str) -> None:
    """Each check refuses its own shape, by its reason; the fixture's edit must have applied."""
    assert (lifecycle, worker) != (_SHUTDOWN_LIVE, _STOP_LIVE), "the fixture did not change"
    assert reason in deadline_findings(lifecycle, worker)


def test_the_deadline_scan_is_silent_on_the_live_shape() -> None:
    """And the shape the library has is not refused."""
    assert deadline_findings(_SHUTDOWN_LIVE, _STOP_LIVE) == []


class StuckEmitRec(Rec):
    """A recorder whose first ``emit`` parks until released, and which logs every interval."""

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.entered = threading.Event()
        self.release = threading.Event()
        self.intervals: list[tuple[str, str, float, float]] = []
        self._intervals_lock = threading.Lock()

    def _note(self, kind: str, began: float) -> None:
        with self._intervals_lock:
            self.intervals.append((kind, threading.current_thread().name, began, time.monotonic()))

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Parks on the first emit, then keeps the batch."""
        began = time.monotonic()
        if not self.entered.is_set():
            self.entered.set()
            self.release.wait(GATE_WAIT * 6)
        super().emit(batch)
        self._note("emit", began)

    def close(self) -> None:
        """Records the close and its interval."""
        began = time.monotonic()
        super().close()
        self._note("close", began)


def _returns_within(work: Callable[[], object]) -> None:
    thread = background(work)
    thread.join(GATE_WAIT)
    assert not thread.is_alive(), "the call did not return while the drain thread was stuck"


def test_no_library_close_begins_while_the_drain_thread_is_inside_the_sink() -> None:
    """FR-005 AC-2 and AC-3: past its deadline the drain is left inside ``emit``, and nothing closes.

    Each take asks ``Runtime.held``, which answers from the drain thread's liveness, never from a
    deadline. So a second ``shutdown()``, a ``configure(sink=...)`` and the exit drain all return
    while the thread is still inside the sink, and none of them calls its ``close()``; and across
    the whole run no close interval overlaps an emit interval on the drain thread.
    """
    a, b = StuckEmitRec("A"), Rec("B")
    log_foundry.configure(service="cells", sink=a)
    span("stuck")
    log_foundry.flush(timeout=0.2)
    try:
        assert a.entered.wait(GATE_WAIT), "the drain thread is inside A's emit"
        log_foundry.shutdown(timeout=0.3)
        assert a.closes == 0, "the expired shutdown left A open"
        _returns_within(lambda: log_foundry.shutdown(timeout=0.3))
        assert a.closes == 0, "a second shutdown closed A under the drain thread"
        _returns_within(lambda: log_foundry.configure(sink=b))
        assert a.closes == 0, "a configure() closed A under the drain thread"
        _returns_within(lambda: _lifecycle._shutdown_worker(timeout=0.3))
        assert a.closes == 0, "the exit drain closed A under the drain thread"
    finally:
        a.release.set()
    worker = _lifecycle._state.worker_exists()
    assert worker is not None
    worker._thread.join(GATE_WAIT)
    log_foundry.shutdown(timeout=5.0)
    assert a.closes == 1, "once the drain thread ended, A was closed, once"
    drain = worker._thread.name
    emits = [(s, e) for kind, who, s, e in a.intervals if kind == "emit" and who == drain]
    closes = [(s, e) for kind, _, s, e in a.intervals if kind == "close"]
    assert emits, "the premise: the drain thread emitted"
    overlaps = [(c, m) for c in closes for m in emits if c[0] < m[1] and m[0] < c[1]]
    assert not overlaps, f"a close overlapped an emit on the drain thread: {overlaps}"
