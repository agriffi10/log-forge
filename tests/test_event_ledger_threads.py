"""SPEC-088 FR-001, FR-003 and FR-005 under threads: exact counts, no lock, a sound read order."""

import _thread
import contextvars
import threading
import time

import pytest

import ledger_check
import log_foundry as lf
from conftest import install_worker, run_concurrently
from ledger_sinks import GatedSink, PlanSink, every, ledger, reading
from log_foundry import Outcome, _ledger, decorator
from log_foundry import worker as worker_mod


@pytest.fixture(autouse=True)
def _fresh_ledger(_reset_config):
    """Starts each test on an empty ledger, after the suite's reset has run."""
    ledger_check.begin()
    yield
    _ledger._between = None


@lf.trace
def _logs_once() -> None:
    lf.info("inside")


def _worker_exits_and_losses(e) -> int:
    """Every exit and loss term: what ``accepted`` may never read below."""
    return e.delivered + e.skipped + e.lost


def _sound(seen, bad: list, *, traced: bool = True) -> None:
    """Records a reading that breaks a property every reading must keep while calls run.

    The ``accepted`` bound is the worker path's (FR-001 AC-10's load is traced calls): every
    exit and loss there is read before the credits. On the orphan path ``orphan_lost`` is read
    after the orphan credits, so a call that credits and fails between the two reads is a loss
    whose credit the reading missed, the negative orphan residual FR-001 admits.
    """
    e = seen.ledger
    if e.unaccounted:
        bad.append(("unaccounted", e))
    if traced and e.accepted < _worker_exits_and_losses(e):
        bad.append(("accepted below exits and losses", e))
    if seen.orphan_residual > 0:
        bad.append(("orphan residual above zero", seen.orphan_residual))
    if seen.meeting_residual is not None and seen.meeting_residual > 0:
        bad.append(("meeting residual above zero", seen.meeting_residual))
    if seen.drain_residual > 0:
        bad.append(("drain residual above zero", seen.drain_residual))


def _trace_and_read(calls: int, tracers: int, *, step=None) -> list:
    """Runs ``tracers`` threads of traced calls and one reader; returns the unsound readings."""
    bad: list = []
    finished = [0]
    lock = threading.Lock()
    if step is not None:
        _ledger._between = step

    def work(index: int, iteration: int) -> None:
        if index == tracers:
            readings = 0
            while True:
                with lock:
                    done = finished[0] == tracers
                _sound(reading(), bad)
                readings += 1
                if done:
                    break
            bad.append(("readings", readings))
            return
        try:
            for _ in range(calls):
                _logs_once()
        finally:
            with lock:
                finished[0] += 1

    errors = run_concurrently(work, tracers + 1)
    _ledger._between = None
    assert not errors, errors
    return bad


def _unsound(bad: list) -> list:
    return [entry for entry in bad if entry[0] != "readings"]


def test_eight_tracing_threads_and_a_reader_see_no_imbalance() -> None:
    """FR-001 AC-10: every reading sound, and exact once the threads have joined."""
    lf.configure(sink=GatedSink())
    bad = _trace_and_read(2_000, 8)
    assert not _unsound(bad), _unsound(bad)[:5]
    assert next(entry[1] for entry in bad if entry[0] == "readings") > 10
    assert lf.flush(30)
    e = ledger()
    assert e.unaccounted == 0 and e.pending == 0, e
    assert e.accepted == 3 * 8 * 2_000, e


def test_a_reading_held_between_cells_stays_sound() -> None:
    """The read-order properties, with a call between each read of a pass (plan section 3)."""
    lf.configure(sink=GatedSink())
    bad = _trace_and_read(1_000, 4, step=lambda name, index: time.sleep(0))
    assert not _unsound(bad), _unsound(bad)[:5]
    assert lf.flush(30)
    assert ledger().pending == 0


def test_a_span_shared_across_threads_never_reads_open_spans_below_zero() -> None:
    """Plan finding 1: a hand-over in one cell debiting credits in another.

    An executor thread whose cell is registered first logs into the owner's span through a
    copied context, as ``asyncio.to_thread`` does, while a reading is held after it has read the
    executor's span credits and before it reads the owner's; then the owner closes the span. Read
    cell by cell, the owner's hand-over of 3 meets the credits of 2 the reading saw, and
    ``open_spans`` reads minus 1. Read hand-overs first, it cannot.
    """
    lf.configure(sink=GatedSink())
    handoff: dict[str, object] = {}
    executor_go, executor_done, owner_go, owner_done, in_span = (
        threading.Event() for _ in range(5)
    )

    def executor() -> None:
        lf.info("registers this thread's cell first")
        executor_go.wait(10)
        ctx = handoff["ctx"]
        ctx.run(lf.info, "from the executor")  # type: ignore[attr-defined]
        executor_done.set()

    pool = threading.Thread(target=executor)
    pool.start()
    while not _ledger._registry.pair[1]:
        time.sleep(0.001)

    @lf.trace
    def owner() -> None:
        handoff["ctx"] = contextvars.copy_context()
        in_span.set()
        owner_go.wait(10)

    held: list = []

    def hold(name: str, index: int) -> None:
        if name == "span" and index == 1 and not held:
            held.append(True)
            executor_go.set()
            executor_done.wait(10)
            owner_go.set()
            owner_done.wait(10)

    def read() -> None:
        in_span.wait(10)
        _ledger._between = hold
        handoff["reading"] = reading()
        _ledger._between = None

    reader = threading.Thread(target=read)
    reader.start()
    owner()
    owner_done.set()
    reader.join(10)
    pool.join(10)
    seen = handoff["reading"]
    assert held, "the reading was held between the two cells"
    assert seen.ledger.unaccounted == 0, seen.ledger  # type: ignore[attr-defined]
    assert seen.ledger.accepted >= _worker_exits_and_losses(seen.ledger)  # type: ignore[attr-defined]
    assert lf.flush(5)
    e = ledger()
    assert e.unaccounted == 0 and e.pending == 0 and e.accepted == 4, e


@pytest.mark.parametrize("outcome", [Outcome.REJECTED, Outcome.ACCEPTED], ids=["refusing", "accepting"])
@pytest.mark.parametrize("between", [False, True], ids=["plain", "between-reads"])
def test_the_orphan_residual_never_reads_positive_while_orphans_run(outcome, between, capsys) -> None:
    """The orphan half of the read order, under six threads logging orphans (FR-003 AC-4's load)."""
    lf.configure(sink=PlanSink(every(outcome)))
    bad: list = []
    finished = [0]
    lock = threading.Lock()
    if between:
        _ledger._between = lambda name, index: None

    def work(index: int, iteration: int) -> None:
        if index == 6:
            while True:
                with lock:
                    done = finished[0] == 6
                _sound(reading(), bad, traced=False)
                if done:
                    return
        try:
            for _ in range(500):
                lf.info("orphan")
        finally:
            with lock:
                finished[0] += 1

    errors = run_concurrently(work, 7)
    _ledger._between = None
    capsys.readouterr()
    assert not errors, errors
    assert not bad, bad[:5]
    seen = reading()
    assert seen.orphan_residual == 0 and seen.ledger.accepted == 3_000, seen.ledger


@pytest.mark.parametrize("lock", ["registration", "loss", "worker", "sweep"])
def test_a_traced_call_returns_while_each_ledger_lock_is_held(lock: str) -> None:
    """FR-005 AC-1. ``_sweep_lock``, which every close already takes, is the control that blocks."""
    worker = worker_mod.Worker(GatedSink(), flush_interval=0.02)
    install_worker(worker)
    _logs_once()
    held = {
        "registration": _ledger._registry.lock,
        "loss": decorator._loss_lock,
        "worker": worker._lock,
        "sweep": decorator._sweep_lock,
    }[lock]
    taken, done = threading.Event(), threading.Event()

    def holder() -> None:
        with held:
            taken.set()
            done.wait(10)

    ready, go, came_back = threading.Event(), threading.Event(), threading.Event()

    def caller_body() -> None:
        _logs_once()
        ready.set()
        go.wait(10)
        _logs_once()
        came_back.set()

    caller = threading.Thread(target=caller_body)
    caller.start()
    assert ready.wait(5)
    thread = threading.Thread(target=holder)
    thread.start()
    assert taken.wait(5)
    go.set()
    returned = came_back.wait(0.5)
    done.set()
    thread.join(5)
    caller.join(5)
    assert returned is (lock != "sweep"), lock


def test_eight_threads_under_a_tight_switch_interval_count_exactly() -> None:
    """FR-005 AC-2: 1,200,000 events, every one in one term."""
    lf.configure(sink=GatedSink())

    def work(index: int, iteration: int) -> None:
        for _ in range(50_000):
            _logs_once()

    errors = run_concurrently(work, 8)
    assert not errors, errors
    assert lf.flush(60)
    e = ledger()
    assert e.accepted == 1_200_000, e
    assert e.pending == 0 and e.unaccounted == 0, e


def test_cells_of_ended_raw_threads_are_retired() -> None:
    """FR-005 AC-5: threads ``threading`` sees as dummies still have their cells retired."""
    lf.configure(sink=GatedSink())
    _logs_once()
    theirs: list[object] = []
    lock = threading.Lock()
    finished = [0]
    baseline = _thread._count()

    def body() -> None:
        try:
            _logs_once()
            lf.info("orphan")
            with lock:
                theirs.append(_ledger.cell())
        finally:
            with lock:
                finished[0] += 1

    for _ in range(2_000):
        _thread.start_new_thread(body, ())
    deadline = time.monotonic() + 60
    while (finished[0] < 2_000 or _thread._count() > baseline) and time.monotonic() < deadline:
        time.sleep(0.01)
    assert finished[0] == 2_000
    assert lf.flush(30)
    _base, live = _ledger.snapshot()
    ids = {id(one) for one in theirs}
    assert not [one for one in live if id(one) in ids], "a dead thread's cell was kept"
    e = ledger()
    assert e.accepted == 3 + 2_000 * 4, e
    assert e.pending == 0 and e.unaccounted == 0, e


class _LogsWhenCollected:
    """An object in a cycle whose ``__del__`` logs, so only the collector frees it."""

    def __init__(self) -> None:
        self.me = self

    def __del__(self) -> None:
        lf.info("logged from __del__")


def test_a_log_from_a_collection_inside_the_registration_lock_returns(monkeypatch) -> None:
    """Plan review M1: re-entry on the registration lock, on a thread with no cell yet.

    A collection inside the lock runs a ``__del__`` that logs; that thread has no cell, so the log
    registers one, which takes the same lock again. With a plain ``Lock`` it waited on its own
    thread forever. The collection is forced inside the fold, which runs under the lock, rather
    than left to the allocator's thresholds.
    """
    import gc

    lf.configure(sink=GatedSink())
    real = _ledger._folded
    collected: list[bool] = []

    def collecting(pair):
        if not collected:
            collected.append(True)
            gc.collect()
        return real(pair)

    outcome: list[object] = []

    def cell_less() -> None:
        gc.disable()
        try:
            _LogsWhenCollected()
            monkeypatch.setattr(_ledger, "_folded", collecting)
            outcome.append(ledger())
        finally:
            gc.enable()

    thread = threading.Thread(target=cell_less, daemon=True)
    thread.start()
    thread.join(10)
    returned = not thread.is_alive()
    monkeypatch.setattr(_ledger, "_folded", real)
    assert returned, "the reading waited on its own thread's registration lock"
    assert collected, "the premise: the collection ran inside the lock"
    e = ledger()
    assert e.accepted == 1 and e.delivered == 1 and e.unaccounted == 0, e
