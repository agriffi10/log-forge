"""SPEC-089 FR-002, FR-003 and FR-004: what a `flush()` call's window lost, counted on its result."""

import ast
import gc
import inspect
import os
import pathlib
import select
import signal
import sys
import textwrap
import threading
import time

import pytest

import ledger_check
import log_foundry as lf
from conftest import Stopwatch, _reset_worker, cpu_window, install_worker
from ledger_sinks import GatedSink, PlanSink, every
from log_foundry import FlushResult, Outcome, _ledger, _lifecycle, api, decorator
from log_foundry import worker as worker_mod
from test_fork_lifecycle import CHILD_TIMEOUT, _set_dumpable, run_in_child

A, R, J, D, U = (
    Outcome.ACCEPTED,
    Outcome.RETRYABLE,
    Outcome.REJECTED,
    Outcome.DROPPED,
    Outcome.UNKNOWN,
)
COUNTS = ("overflow_lost", "batch_lost", "sink_lost", "drain_lost", "stranded")
ROOT = pathlib.Path(__file__).resolve().parent.parent


def _counts(result: FlushResult) -> dict[str, int]:
    """The five counts of a result, by name."""
    return {name: getattr(result, name) for name in COUNTS}


def _only(**expected: int) -> dict[str, int]:
    """The five counts, zero except those named."""
    return {name: expected.get(name, 0) for name in COUNTS}


def _quiet_worker(sink, **settings) -> worker_mod.Worker:
    """Installs a process worker no submission of a case can trigger, so a flush emits the batch."""
    merged = {"batch_size": 10_000, "flush_interval": 3600.0, "max_retries": 0}
    merged.update(settings)
    lf.configure(sink=sink)
    worker = worker_mod.Worker(sink, **merged)
    install_worker(worker)
    return worker


@lf.trace
def _span(i: int = 0) -> None:
    lf.info("inside", i=i)


@lf.trace
def _lost_span() -> None:
    lf.info("lost")


@lf.trace
def _kept_span() -> None:
    lf.info("kept")


@lf.trace
def _bare_span() -> None:
    pass


class _Call:
    """Runs a call on its own thread, keeping its result or what it raised."""

    def __init__(self, fn, *args) -> None:
        self.result = None
        self.error: BaseException | None = None
        self.thread = threading.Thread(target=self._run, args=(fn, args), daemon=True)
        self.thread.start()

    def _run(self, fn, args) -> None:
        try:
            self.result = fn(*args)
        except BaseException as exc:
            self.error = exc

    def join(self, timeout: float = 30.0):
        self.thread.join(timeout)
        assert not self.thread.is_alive(), "the call did not return"
        assert self.error is None, repr(self.error)
        return self.result


def _wait_for(predicate, what: str, timeout: float = 10.0) -> None:
    """Polls ``predicate`` until it holds, failing with ``what`` past ``timeout``."""
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        time.sleep(0.001)


def _marker_queued(worker) -> bool:
    """Whether a flush marker is in the worker's queue."""
    with worker._queue.mutex:
        return any(isinstance(item, worker_mod._FlushMarker) for item in worker._queue.queue)


@pytest.fixture
def markers(monkeypatch) -> list:
    """Records every flush marker built, with the thread that built it."""
    built: list = []
    base = worker_mod._FlushMarker

    class Spy(base):
        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            self.built_on = threading.current_thread()
            built.append(self)

    monkeypatch.setattr(worker_mod, "_FlushMarker", Spy)
    return built


# ── FR-002 ───────────────────────────────────────────────────────────────────────────────

_P09_LOSSY = [
    (
        "1 accepted, 29 never land",
        lambda b: [A] + [R] * (len(b) - 1) if len(b) == 30 else [R] * len(b),
        29,
    ),
    ("1 accepted, 29 rejected", lambda b: [A] + [J] * (len(b) - 1), 29),
    ("all dropped", every(D), 30),
    ("all unknown", every(U), 30),
]


@pytest.mark.parametrize(
    ("plan", "lost"), [row[1:] for row in _P09_LOSSY], ids=[row[0] for row in _P09_LOSSY]
)
def test_p09_lossy_plans_return_truthy_with_their_loss_in_sink_lost(plan, lost, markers) -> None:
    """FR-002 AC-1: the loss is on the result, truthiness unchanged, and the drain closed it."""
    sink = PlanSink(plan)
    _quiet_worker(sink)
    for i in range(10):
        _span(i)
    result = lf.flush(10)
    assert result, result
    assert _counts(result) == _only(sink_lost=lost), result
    assert sink.calls == 1
    counted = [m for m in markers if m.counted]
    assert len(counted) == 1 and counted[0].ledger is not None, "the drain closed the window"


def test_p09_all_rejected_is_abandoned_and_counted_in_batch_lost() -> None:
    """FR-002 AC-2."""
    sink = PlanSink(every(J))
    _quiet_worker(sink)
    for i in range(10):
        _span(i)
    result = lf.flush(10)
    assert not result and result.reason == "abandoned", result
    assert _counts(result) == _only(batch_lost=30), result


def test_an_accepting_sink_returns_a_result_equal_to_a_plain_success() -> None:
    """FR-002 AC-3."""
    sink = PlanSink(every(A))
    _quiet_worker(sink)
    for i in range(10):
        _span(i)
    assert lf.flush(10) == FlushResult(ok=True)
    assert len(sink.accepted) == 30


def test_a_batch_abandoned_before_the_call_is_outside_the_window() -> None:
    """FR-002 AC-4: p09's last case, the worker's own trigger abandoning a batch first."""
    sink = GatedSink()
    sink.raise_on = lambda call, batch: ConnectionError("down") if call == 1 else None
    _quiet_worker(sink, batch_size=10)
    before = lf.health().events.batch_lost
    for i in range(10):
        _span(i)
    _wait_for(lambda: lf.health().events.batch_lost - before >= 30, "the trigger's abandon")
    _span(99)
    result = lf.flush(10)
    assert result and result.lost == 0, result
    assert lf.health().events.batch_lost - before == 30
    assert len(sink.events) == 3


def test_another_threads_drops_ahead_of_the_marker_are_counted() -> None:
    """FR-002 AC-5: the over-count, characterised over 200 runs on one worker."""
    sink = PlanSink(lambda b: [D if e["function"] == "_lost_span" else A for e in b])
    _quiet_worker(sink)
    failures = []
    for run in range(200):
        other = threading.Thread(target=lambda: [_lost_span() for _ in range(3)])
        other.start()
        other.join()
        before = len(sink.accepted)
        _kept_span()
        _kept_span()
        result = lf.flush(10)
        took = len(sink.accepted) - before
        if not (result and _counts(result) == _only(sink_lost=9) and took == 6):
            failures.append((run, result, took))
    assert not failures, failures[:5]


def test_unbuildable_events_on_other_threads_stay_out_of_the_counts(monkeypatch) -> None:
    """FR-002 AC-6: ``in_span_lost`` and ``orphan_lost`` are in no count."""
    sink = GatedSink()
    _quiet_worker(sink)
    _span(1)
    _span(2)
    sink.gate.clear()
    call = _Call(lf.flush, 10)
    try:
        assert sink.entered.wait(10)
        before = lf.health()

        def boom(*args: object, **kwargs: object) -> dict:
            """Fails where the event is built."""
            raise RuntimeError("cannot build")

        def unbuildable() -> None:
            with monkeypatch.context() as patched:
                patched.setattr(api, "build_event", boom)
                lf.info("orphan")
                _span(3)

        helper = threading.Thread(target=unbuildable)
        helper.start()
        helper.join(10)
        after = lf.health()
    finally:
        sink.gate.set()
    result = call.join()
    assert (after.orphan_lost - before.orphan_lost, after.in_span_lost - before.in_span_lost) == (
        1,
        1,
    )
    assert result and _counts(result) == _only(), result
    assert lf.flush(10)


def test_a_flush_inside_a_span_counts_the_buffer_it_sweeps() -> None:
    """FR-002 AC-7, its first half: the swept buffer is the call's own."""
    sink = PlanSink(every(D))
    _quiet_worker(sink)
    seen: list[FlushResult] = []

    @lf.trace
    def flushes_inside() -> None:
        lf.info("buffered")
        seen.append(lf.flush(10))

    flushes_inside()
    assert seen[0] and _counts(seen[0]) == _only(sink_lost=2), seen[0]
    lf.flush(10)


def test_a_span_open_on_another_thread_ships_its_logs_and_holds_its_start() -> None:
    """FR-002 AC-7, its second half, inverted by SPEC-091 FR-004.

    ~~Another thread's open span is pending, not counted~~: a ``flush()`` now sweeps every
    registered span, so the other thread's 2 logs are handed to this sink by the call's registry
    sweep, inside its window. The sink drops every event, so they are counted as ``sink_lost``
    on this call, where before SPEC-091 they stayed on the span and the call counted nothing.
    The span's ``span.start`` stays held for its close, so ``open_spans`` reads 1.
    """
    sink = PlanSink(every(D))
    _quiet_worker(sink)
    parked = threading.Event()
    release = threading.Event()

    @lf.trace
    def holds_open() -> None:
        lf.info("buffered elsewhere")
        lf.info("buffered elsewhere too")
        parked.set()
        release.wait(10)

    other = threading.Thread(target=holds_open)
    other.start()
    try:
        assert parked.wait(10)
        result = lf.flush(10)
        assert result and _counts(result) == _only(sink_lost=2), result
        assert lf.health().events.open_spans == 1, "the held span.start"
    finally:
        release.set()
        other.join(10)
    lf.flush(10)


def test_a_marker_the_exit_drain_answers_carries_its_window(markers) -> None:
    """FR-002 AC-8: ``_final_drain`` closes the window as ``_drain`` does."""
    gate = threading.Event()
    entered = threading.Event()

    def plan(batch: list) -> list:
        """Holds the first batch, accepts it, and drops every later one."""
        if not entered.is_set():
            entered.set()
            gate.wait(10)
            return [A] * len(batch)
        return [D] * len(batch)

    sink = PlanSink(plan)
    worker = _quiet_worker(sink, batch_size=1)
    try:
        _span(0)
        assert entered.wait(10)
        _span(1)
        _span(2)
        flusher = _Call(lf.flush, 10)
        _wait_for(lambda: _marker_queued(worker), "the flush's marker in the queue")
        stopper = _Call(lf.shutdown, 10)
        _wait_for(worker._stop.is_set, "shutdown setting _stop")
    finally:
        gate.set()
    result = flusher.join()
    stopper.join()
    assert _counts(result) == _only(sink_lost=6), result
    mine = [m for m in markers if m.built_on is flusher.thread]
    assert len(mine) == 1 and mine[0].ledger is not None
    assert sink.calls == 2, "the exit drain carried the second batch"


def test_the_window_ends_at_the_drains_answer_not_the_callers_wake(monkeypatch) -> None:
    """FR-002 AC-9: a caller held after its answer does not count a later flush's loss."""
    sink = PlanSink(lambda b: [D if e["function"] == "_lost_span" else A for e in b])
    worker = _quiet_worker(sink)
    real = worker._flush_marked
    first = threading.current_thread()
    first_answered = threading.Event()
    second_done = threading.Event()

    def held(timeout, *, counted):
        """Returns the real answer, holding the first caller until the second has returned."""
        answer = real(timeout, counted=counted)
        if threading.current_thread() is first:
            first_answered.set()
            second_done.wait(10)
        return answer

    monkeypatch.setattr(worker, "_flush_marked", held)
    _kept_span()
    out: dict[str, FlushResult] = {}

    def second() -> None:
        first_answered.wait(10)
        for _ in range(3):
            _lost_span()
        out["second"] = lf.flush(10)
        second_done.set()

    helper = threading.Thread(target=second)
    helper.start()
    out["first"] = lf.flush(10)
    helper.join(10)
    assert _counts(out["second"]) == _only(sink_lost=9), out
    assert _counts(out["first"]) == _only(), out


# ── FR-003 ───────────────────────────────────────────────────────────────────────────────


def test_a_timed_out_flush_counts_no_loss_and_leaves_the_held_batch_in_flight() -> None:
    """FR-003 AC-1: characterises a timed-out call; the held batch is pending, not lost."""
    gate = threading.Event()
    sink = PlanSink(lambda b: (gate.wait(10), [A] * len(b))[1])
    _quiet_worker(sink)
    for i in range(3):
        _span(i)
    try:
        result = lf.flush(0.2)
        assert not result and result.reason == "timed-out", result
        assert result.lost == 0, result
        assert lf.health().events.in_flight == 9
    finally:
        gate.set()
    assert lf.flush(10)


def test_a_retired_flush_counts_the_swept_buffer_as_stranded() -> None:
    """FR-003 AC-2: no drain answers, so the caller reads, and the swept buffer is stranded."""
    sink = PlanSink(every(A))
    lf.configure(sink=sink)
    _span(0)
    lf.shutdown(10)
    seen: list[FlushResult] = []

    @lf.trace
    def after_shutdown() -> None:
        lf.info("buffered")
        seen.append(lf.flush(10))

    after_shutdown()
    assert not seen[0] and seen[0].reason == "retired", seen[0]
    assert _counts(seen[0]) == _only(stranded=2), seen[0]


def test_a_sweep_that_overflows_counts_overflow_lost() -> None:
    """FR-003 AC-3: the window opens before the sweep, so the sweep's own overflow is counted."""
    sink = GatedSink()
    sink.gate.clear()
    worker = _quiet_worker(sink, batch_size=1, max_queue=2)
    seen: list[FlushResult] = []

    @lf.trace
    def flushes_inside() -> None:
        lf.info("buffered")
        seen.append(lf.flush(0.2))

    try:
        _span(0)
        assert sink.entered.wait(10)
        _span(1)
        _span(2)
        assert worker._queue.full()
        flushes_inside()
    finally:
        sink.gate.set()
    assert not seen[0], seen[0]
    assert _counts(seen[0]) == _only(overflow_lost=2), seen[0]
    lf.flush(10)


def _dying_on_the_flush(sink: GatedSink) -> FlushResult:
    """One run of FR-003 AC-4: the drain dies on the flush's batch with 6 events queued behind."""
    sink.gate.clear()
    sink.raise_on = lambda call, batch: SystemExit(1) if call == 1 else None

    def queue_behind() -> None:
        sink.entered.wait(10)
        _bare_span()
        _bare_span()
        _bare_span()
        sink.gate.set()

    helper = threading.Thread(target=queue_behind)
    for i in range(3):
        _span(i)
    helper.start()
    try:
        return lf.flush(5)
    finally:
        sink.gate.set()
        helper.join(10)


def test_a_drain_dying_under_the_flush_counts_drain_lost_and_stranded_every_time() -> None:
    """FR-003 AC-4: 200 of 200 under a 1e-6 switch interval, each on a fresh worker."""
    failures = []
    original = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        for run in range(200):
            sink = GatedSink()
            _quiet_worker(sink)
            result = _dying_on_the_flush(sink)
            if result or _counts(result) != _only(drain_lost=9, stranded=6):
                failures.append((run, result))
    finally:
        sys.setswitchinterval(original)
    assert not failures, failures[:5]


def _dying_in_the_exit_drain() -> tuple[FlushResult, worker_mod.Worker]:
    """One run of FR-003 AC-5: the exit drain dies on the batch ahead of the flush's marker."""
    sink = GatedSink()
    sink.gate.clear()
    sink.raise_on = lambda call, batch: SystemExit(1) if call == 2 else None
    worker = _quiet_worker(sink, batch_size=1)
    try:
        _span(0)
        assert sink.entered.wait(10)
        _span(1)
        _span(2)
        flusher = _Call(lf.flush, 5)
        _wait_for(lambda: _marker_queued(worker), "the flush's marker in the queue")
        stopper = _Call(lf.shutdown, 5)
        _wait_for(worker._stop.is_set, "shutdown setting _stop")
    finally:
        sink.gate.set()
    result = flusher.join()
    stopper.join()
    return result, worker


def test_a_drain_dying_in_the_exit_drain_counts_drain_lost_every_time() -> None:
    """FR-003 AC-5: 200 of 200 under a 1e-6 switch interval."""
    failures = []
    original = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        for run in range(200):
            result, _ = _dying_in_the_exit_drain()
            if result or _counts(result) != _only(drain_lost=6):
                failures.append((run, result))
            unbalanced = ledger_check.problems()
            if unbalanced:
                failures.append((run, unbalanced))
            _reset_worker()
    finally:
        sys.setswitchinterval(original)
    assert not failures, failures[:5]


def _hold_after(worker, name: str, release: threading.Event, entered: threading.Event) -> None:
    """Patches one of ``worker``'s methods to call the real one, then hold until released."""
    real = getattr(worker, name)

    def holding(*args, **kwargs):
        try:
            return real(*args, **kwargs)
        finally:
            entered.set()
            release.wait(10)

    setattr(worker, name, holding)


def _hold_before(worker, name: str, release: threading.Event, entered: threading.Event) -> None:
    """Patches one of ``worker``'s methods to hold until released, then call the real one."""
    real = getattr(worker, name)

    def holding(*args, **kwargs):
        entered.set()
        release.wait(10)
        return real(*args, **kwargs)

    setattr(worker, name, holding)


@pytest.mark.parametrize("answerer", ["drain", "exit-drain"])
def test_a_marker_answered_while_unwinding_waits_for_the_drain_to_end(answerer) -> None:
    """FR-003 AC-4 and AC-5, deterministically: an unwinding answer is waited out (invariant 6).

    The drain is held inside ``_release_marker``, after the marker was answered and before
    ``_drain`` returns, so ``_drain_finished`` is still clear: only ``unwinding`` can send the
    caller to wait.
    """
    release = threading.Event()
    held = threading.Event()
    sink = GatedSink()
    if answerer == "drain":
        sink.raise_on = lambda call, batch: SystemExit(1)
        worker = _quiet_worker(sink)
        for i in range(3):
            _span(i)
        _hold_after(worker, "_release_marker", release, held)
        flusher = _Call(lf.flush, 5)
        expected = _only(drain_lost=9)
    else:
        sink.gate.clear()
        sink.raise_on = lambda call, batch: SystemExit(1) if call == 2 else None
        worker = _quiet_worker(sink, batch_size=1)
        _span(0)
        assert sink.entered.wait(10)
        _span(1)
        _span(2)
        _hold_after(worker, "_release_marker", release, held)
        flusher = _Call(lf.flush, 5)
        _wait_for(lambda: _marker_queued(worker), "the flush's marker in the queue")
        stopper = _Call(lf.shutdown, 5)
        _wait_for(worker._stop.is_set, "shutdown setting _stop")
        sink.gate.set()
        expected = _only(drain_lost=6)
    try:
        assert held.wait(10), "the drain answered the marker"
        assert not worker._drain_finished.is_set()
        time.sleep(0.2)
        assert flusher.thread.is_alive(), "the flush returned while the drain was still dying"
    finally:
        release.set()
    result = flusher.join()
    if answerer == "exit-drain":
        stopper.join()
    assert not result and _counts(result) == expected, result


def test_a_marker_answered_by_the_closing_sweep_reads_while_the_thread_lives() -> None:
    """FR-003 AC-17: a marker the closing sweep answers is read at once, ``stranded`` included.

    The marker sits behind the batch the drain dies on, so ``_release_waiters`` answers it
    pessimistically and marks nothing. That sweep runs after ``_terminal_failure`` has debited
    what the thread held and set ``_drain_accounted``, and the drain is held inside it, so its
    thread is still alive: the queue behind it is ``stranded`` all the same, and the call returns
    without waiting for the thread to end (the second diff review, MAJOR 2).
    """
    sink = GatedSink()
    sink.gate.clear()
    sink.raise_on = lambda call, batch: SystemExit(1)
    worker = _quiet_worker(sink, batch_size=1)
    release = threading.Event()
    held = threading.Event()
    try:
        _span(0)
        assert sink.entered.wait(10)
        flusher = _Call(lf.flush, 5)
        _wait_for(lambda: _marker_queued(worker), "the flush's marker in the queue")
        _span(1)
        _hold_after(worker, "_release_waiters", release, held)
        sink.gate.set()
        assert held.wait(10), "the closing sweep answered the marker"
        flusher.thread.join(5.0)
        assert not flusher.thread.is_alive(), "the flush waited for the drain thread to end"
        assert worker._thread.is_alive(), "the drain is still held"
    finally:
        sink.gate.set()
        release.set()
    result = flusher.join()
    assert not result and _counts(result) == _only(drain_lost=3, stranded=3), result


def test_a_flush_reaching_a_drain_stopped_before_its_debit_waits_for_the_debit() -> None:
    """FR-003 AC-17, the second dying arm: ``_drain_finished`` set, the thread alive, no debit.

    The drain is held on its way into ``_terminal_failure``, after ``_run`` set
    ``_drain_finished`` and before the debit. A flush made then gets its verdict from
    ``_flush_marked`` at once and its marker is never answered unwinding, so only the second arm
    sends it to wait; without it the call reads ``drain_lost`` and ``stranded`` as zero.
    """
    sink = GatedSink()
    sink.raise_on = lambda call, batch: SystemExit(1)
    worker = _quiet_worker(sink, batch_size=1)
    release = threading.Event()
    held = threading.Event()
    _hold_before(worker, "_terminal_failure", release, held)
    try:
        _span(0)
        assert held.wait(10), "the drain died"
        assert worker._drain_finished.is_set() and not worker._drain_accounted.is_set()
        _span(1)
        flusher = _Call(lf.flush, 5)
        time.sleep(0.2)
        assert flusher.thread.is_alive(), "the flush read before the dying drain debited"
    finally:
        release.set()
    result = flusher.join()
    assert not result and result.reason == "thread-died", result
    assert _counts(result) == _only(drain_lost=3, stranded=3), result


def test_a_flush_meeting_a_clean_exit_before_its_sweep_waits_only_for_the_sweep() -> None:
    """FR-003: ``_run`` sets ``_drain_accounted`` on a clean exit too, so the second arm ends.

    A shutdown's drain has stopped, with ``_drain_finished`` set, and is held on its way into the
    closing sweep, so a flush made then finds the thread alive and waits on the event, which the
    spy confirms. ``_run``'s outermost ``finally`` sets it once the sweep returns; without that
    the wait would last the rest of the call's 2.0 s, so the ceiling after the release is half.
    """
    worker = _quiet_worker(PlanSink(every(A)))
    release = threading.Event()
    held = threading.Event()
    _hold_before(worker, "_release_waiters", release, held)
    accounted = worker._drain_accounted
    real_wait = accounted.wait
    waiting = threading.Event()

    def spying_wait(timeout=None):
        waiting.set()
        return real_wait(timeout)

    accounted.wait = spying_wait
    stopper = _Call(lf.shutdown, 5)
    try:
        assert held.wait(10), "the clean exit reached its sweep"
        assert worker._drain_finished.is_set() and worker._thread.is_alive()
        flusher = _Call(lf.flush, 2.0)
        assert waiting.wait(10), "the flush waits on the drain's accounting"
    finally:
        clock = Stopwatch()
        release.set()
    result = flusher.join()
    elapsed = clock.stop()
    stopper.join()
    del accounted.wait
    assert elapsed < 1.0, elapsed
    assert not result and result.reason == "retired", result


DYING_EMIT = 1.0
"""Seconds AC-6's sink holds its batch before raising, spent from the flush's own budget."""

AC6_TIMEOUT = 2.0
"""AC-6's ``flush`` timeout: the drain dies ``DYING_EMIT`` into it, leaving a second to wait."""


def test_the_wait_for_a_dying_drain_is_bounded_and_idle() -> None:
    """FR-003 AC-6: the wait ends at the call's deadline, and spends no CPU while it waits.

    The sink spends ``DYING_EMIT`` of the call's ``AC6_TIMEOUT`` before it raises, and the dying
    drain is held on its way into ``_terminal_failure``, before its debit sets
    ``_drain_accounted``, so the wait has only what is left of the deadline. Correct code returns
    near 2.0 s; a wait that restarts the deadline gets a fresh 2.0 s and returns near 3.0 s, so the
    ceiling of 2.5 s is half the extra second either side, and the spy on the event's ``wait``
    pins the same claim without a clock: the wait is taken in slices so that a forked thread can
    leave it (the execution-frame review, MINOR 1), and the slices' timeouts together can be at
    most what the emit left. A wait that never ends is caught by the guarded join and by a slice
    of ``None``, and a wait that polls by the CPU bound and by the number of slices, which can be
    at most one per ``_ACCOUNTING_SLICE`` of what was left and one for the remainder.
    """
    sink = GatedSink()

    def dies_late(call, batch):
        time.sleep(DYING_EMIT)
        return SystemExit(1)

    sink.raise_on = dies_late
    worker = _quiet_worker(sink)
    release = threading.Event()
    held = threading.Event()
    _hold_before(worker, "_terminal_failure", release, held)
    waits: list[tuple[threading.Thread, float | None]] = []
    accounted = worker._drain_accounted
    real_wait = accounted.wait

    def spying_wait(timeout=None):
        waits.append((threading.current_thread(), timeout))
        return real_wait(timeout)

    accounted.wait = spying_wait
    _span(0)
    seen: dict[str, object] = {}

    def timed_flush() -> None:
        with cpu_window() as window:
            clock = Stopwatch()
            seen["result"] = lf.flush(AC6_TIMEOUT)
            elapsed = clock.stop()
        seen["elapsed"] = elapsed
        seen["bounded"] = elapsed < 2.5
        seen["cpu"] = window.spent

    try:
        flusher = threading.Thread(target=timed_flush, daemon=True)
        flusher.start()
        flusher.join(6.0)
        assert not flusher.is_alive(), "the wait for a dying drain was not bounded"
        assert held.is_set() and worker._thread.is_alive(), "the drain is still held"
        assert not accounted.is_set(), "the drain is held before its debit"
    finally:
        release.set()
        del accounted.wait
    assert seen["bounded"], seen
    assert seen["cpu"] < 0.05, seen
    assert not seen["result"], seen
    mine = [timeout for thread, timeout in waits if thread is flusher]
    left = AC6_TIMEOUT - DYING_EMIT
    assert mine, "the flush did not wait for the dying drain"
    assert None not in mine, mine
    assert sum(mine) <= left, mine
    assert len(mine) <= left / _lifecycle._ACCOUNTING_SLICE + 1, mine


def test_a_flush_on_the_dying_drain_thread_does_not_wait_for_itself() -> None:
    """FR-003 AC-7: a flush from the dying drain's own line returns, and raises nothing.

    A flush that waited on its own thread would wait out its whole ``flush(5)``, so the ceiling
    is half of that, 2.5 s, and a wait that raised would be absorbed and announced, a second
    stderr line. The line is written after ``_drain_accounted`` is set, so the event wait would
    return at once even without the drain-thread guard; the guard is defence in depth, and this
    test asserts the outcome rather than the guard.
    """
    sink = GatedSink()
    sink.raise_on = lambda call, batch: SystemExit(1)
    worker = _quiet_worker(sink)
    seen: dict[str, object] = {"lines": []}

    class Flushing:
        """A stderr whose first write flushes, from whichever thread writes."""

        def write(self, text: str) -> int:
            seen["lines"].append(text)  # type: ignore[union-attr]
            if len(seen["lines"]) == 1:  # type: ignore[arg-type]
                seen["thread"] = threading.current_thread()
                clock = Stopwatch()
                try:
                    seen["result"] = lf.flush(5)
                except BaseException as exc:
                    seen["error"] = exc
                elapsed = clock.stop()
                seen["elapsed"] = elapsed
                seen["prompt"] = elapsed < 2.5
            return len(text)

        def flush(self) -> None:
            pass

    _span(0)
    original = sys.stderr
    sys.stderr = Flushing()
    try:
        lf.flush(5)
        worker._thread.join(10)
    finally:
        sys.stderr = original
    assert seen["thread"] is worker._thread
    assert "error" not in seen, seen
    assert seen["prompt"], seen
    lines = seen["lines"]
    assert len(lines) == 1 and "worker thread stopped" in lines[0], lines


def test_retarget_markers_carry_no_ledger(markers) -> None:
    """FR-003 AC-8, its first half: a swap's internal flushes ask the drain for nothing."""
    _quiet_worker(PlanSink(every(A)))
    _span(0)
    lf.configure(sink=PlanSink(every(A)))
    assert markers, "the swap flushed"
    assert all(not m.counted and m.ledger is None for m in markers), markers


def test_a_swap_does_not_wait_for_a_dying_drain() -> None:
    """FR-003 AC-8, its second half: a swap never waits on a dying drain."""
    sink = GatedSink()
    sink.raise_on = lambda call, batch: SystemExit(1)
    worker = _quiet_worker(sink, batch_size=1)
    release = threading.Event()
    held = threading.Event()
    _hold_after(worker, "_terminal_failure", release, held)
    try:
        _span(0)
        assert held.wait(10), "the drain died"
        swap = _Call(lambda: lf.configure(sink=PlanSink(every(A))))
        swap.thread.join(2.0)
        assert not swap.thread.is_alive(), "the swap waited for the dying drain"
        assert worker._thread.is_alive(), "the drain is still held"
    finally:
        release.set()


def test_a_sink_flush_failure_carries_the_drains_window() -> None:
    """FR-003 AC-9."""

    class FailsToFlush(PlanSink):
        def flush(self) -> None:
            raise RuntimeError("client buffer")

    _quiet_worker(FailsToFlush(every(D)))
    _span(0)
    result = lf.flush(10)
    assert not result and result.reason == "sink-flush", result
    assert _counts(result) == _only(sink_lost=3), result


def test_with_no_worker_an_orphan_drop_in_the_window_is_counted(monkeypatch) -> None:
    """FR-003 AC-10: the third over-count, in a process with no worker."""
    lf.configure(sink=PlanSink(every(D)))

    def orphan_elsewhere() -> None:
        other = threading.Thread(target=lambda: lf.info("orphan"))
        other.start()
        other.join(10)

    monkeypatch.setattr(decorator, "_sweep_open_spans", orphan_elsewhere)
    result = lf.flush(10)
    assert result and _counts(result) == _only(sink_lost=1), result
    assert _lifecycle._state._worker is None


def _unwrapped_returns(source: str, function: str) -> list[tuple[int, str]]:
    """Every ``return`` in ``function`` whose value is not a call to ``_with_counts``."""
    tree = ast.parse(textwrap.dedent(source))
    found = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == function
    ]
    assert len(found) == 1, f"{function} not found"
    offenders = []
    for node in ast.walk(found[0]):
        if not isinstance(node, ast.Return):
            continue
        value = node.value
        wrapped = (
            isinstance(value, ast.Call)
            and isinstance(value.func, ast.Name)
            and value.func.id == "_with_counts"
        )
        if not wrapped:
            offenders.append((node.lineno, "return does not pass through _with_counts"))
    return offenders


def test_every_return_in_flush_worker_attaches_the_counts() -> None:
    """FR-003 AC-11: derived from the tree, and the count found is asserted too."""
    source = inspect.getsource(_lifecycle._flush_worker)
    returns = [n for n in ast.walk(ast.parse(textwrap.dedent(source))) if isinstance(n, ast.Return)]
    assert len(returns) == 4
    assert _unwrapped_returns(source, "_flush_worker") == []


def test_the_return_check_reports_an_unwrapped_return_and_passes_a_wrapped_one() -> None:
    """FR-003 AC-11, the checker's own corpus: it fails on a bare return and stays silent else."""
    bare = (
        "def _flush_worker():\n"
        "    if x:\n"
        "        return _with_counts(FlushResult(ok=False), start, end)\n"
        "    return FlushResult(ok=True)\n"
    )
    assert _unwrapped_returns(bare, "_flush_worker") == [
        (4, "return does not pass through _with_counts")
    ]
    wrapped = (
        "def _flush_worker():\n"
        "    if x:\n"
        "        return _with_counts(FlushResult(ok=False), start, end)\n"
        "    return _with_counts(drained, start, end)\n"
    )
    assert _unwrapped_returns(wrapped, "_flush_worker") == []


def test_a_child_forked_inside_the_window_counts_its_own_losses(monkeypatch) -> None:
    """FR-003 AC-12: a window a fork crosses is the child's, never negative."""
    sink = PlanSink(every(D))
    lf.configure(sink=sink)

    @lf.trace
    def five_events() -> None:
        lf.info("one")
        lf.info("two")
        lf.info("three")

    five_events()
    assert _counts(lf.flush(10)) == _only(sink_lost=5)
    real_sweep = decorator._sweep_open_spans
    parent = os.getpid()
    read_fd, write_fd = os.pipe()
    child: dict[str, int] = {}

    def fork_in_the_window() -> None:
        dumpable = _set_dumpable(0)
        try:
            gc.collect()
            pid = os.fork()
        finally:
            if os.getpid() == parent:
                _set_dumpable(dumpable)
        if pid == 0:
            signal.alarm(CHILD_TIMEOUT)
            _bare_span()
        else:
            child["pid"] = pid
        real_sweep()

    monkeypatch.setattr(decorator, "_sweep_open_spans", fork_in_the_window)
    try:
        result = lf.flush(10)
        if os.getpid() != parent:
            os.write(write_fd, f"{bool(result)} {_counts(result)}".encode())
    finally:
        if os.getpid() != parent:
            os._exit(0)
    os.close(write_fd)
    try:
        deadline = time.monotonic() + CHILD_TIMEOUT + 2.0
        chunks = []
        while time.monotonic() < deadline:
            if not select.select([read_fd], [], [], max(0.0, deadline - time.monotonic()))[0]:
                break
            chunk = os.read(read_fd, 4096)
            if not chunk:
                break
            chunks.append(chunk)
    finally:
        os.close(read_fd)
        if "pid" in child:
            try:
                os.kill(child["pid"], signal.SIGKILL)
            except ProcessLookupError:
                pass
            os.waitpid(child["pid"], 0)
    reported = b"".join(chunks).decode()
    assert reported == f"True {_only(sink_lost=2)}", reported
    assert result and _counts(result) == _only(), result


def test_a_count_never_reads_negative() -> None:
    """FR-003 AC-12, the clamp: a count that falls in one process is published as zero."""
    pid = os.getpid()
    start = (pid, worker_mod.EventLedger(sink_lost=5, stranded=1))
    end = (pid, worker_mod.EventLedger(sink_lost=2, stranded=4))
    result = _lifecycle._with_counts(FlushResult(ok=True), start, end)
    assert _counts(result) == _only(stranded=3), result


def test_a_fault_in_the_drains_snapshot_is_absorbed_and_the_caller_reads(
    monkeypatch, capsys
) -> None:
    """FR-003 AC-13: the drain survives a fault in its snapshot, and the caller closes instead."""

    def run() -> tuple[FlushResult, worker_mod.Worker]:
        worker = _quiet_worker(PlanSink(every(D)))
        for i in range(3):
            _span(i)
        return lf.flush(10), worker

    uninjected, _ = run()
    capsys.readouterr()
    real = _lifecycle._ledger_snapshot
    faults: list[str] = []

    def faulty() -> worker_mod.EventLedger:
        worker = _lifecycle._state.worker_exists()
        if not faults and worker is not None and threading.current_thread() is worker._thread:
            faults.append(threading.current_thread().name)
            raise LookupError("injected")
        return real()

    monkeypatch.setattr(_lifecycle, "_ledger_snapshot", faulty)
    injected, worker = run()
    err = capsys.readouterr().err
    assert faults, "the drain's snapshot was reached"
    assert err.count("(LookupError)") == 1, err
    assert worker._thread.is_alive()
    assert injected == uninjected == FlushResult(ok=True, sink_lost=9), (injected, uninjected)


@pytest.mark.parametrize("read", ["start", "end"])
def test_a_fault_in_the_callers_snapshot_is_absorbed_with_zero_counts(
    read, monkeypatch, capsys
) -> None:
    """FR-003 AC-14: no worker, so the caller reads both ends itself.

    A start that faults leaves the call uncounted, so the end is not read at all and the fault
    is still announced once (the second diff review, MINOR 5).
    """
    lf.configure(sink=PlanSink(every(D)))
    lf.info("orphan")
    real = _lifecycle._ledger_snapshot
    calls: list[int] = []

    def faulty() -> worker_mod.EventLedger:
        calls.append(1)
        if len(calls) == (1 if read == "start" else 2):
            raise LookupError("injected")
        return real()

    def orphan_in_the_window() -> None:
        lf.info("orphan in the window")

    monkeypatch.setattr(decorator, "_sweep_open_spans", orphan_in_the_window)
    uninjected = lf.flush(10)
    assert uninjected.sink_lost == 1, uninjected
    capsys.readouterr()
    monkeypatch.setattr(_lifecycle, "_ledger_snapshot", faulty)
    result = lf.flush(10)
    err = capsys.readouterr().err
    assert len(calls) == (1 if read == "start" else 2), calls
    assert err.count("(LookupError)") == 1, err
    assert result == FlushResult(ok=True), result


# ── The second diff review: the registration lock, a held stderr, a fork, a persistent fault ──


class _ClosingSink(GatedSink):
    """A :class:`GatedSink` that records its close."""

    def __init__(self) -> None:
        super().__init__()
        self.closed = False

    def close(self) -> None:
        self.closed = True


def test_a_flush_made_inside_the_registration_lock_returns_at_once() -> None:
    """FR-003 AC-15 (BLOCKER 1): the drain never waits on the lock a ``flush()`` caller is inside.

    A ``__del__`` or a signal handler running inside the ledger's registration lock is the case
    the lock is re-entrant for. The drain answering this call's marker tries that lock rather
    than waiting on it, so the call returns at once and reads its own end, re-entering the lock it
    holds. A drain that waited would hold the call to its whole ``flush(2.0)``, so the ceiling is
    half of that.
    """
    sink = PlanSink(every(D))
    _quiet_worker(sink)
    _span(0)
    with _ledger._registry.lock:
        clock = Stopwatch()
        result = lf.flush(2.0)
        elapsed = clock.stop()
    assert elapsed < 1.0, (elapsed, result)
    assert result and _counts(result) == _only(sink_lost=3), result


def test_an_unbounded_flush_made_inside_the_registration_lock_returns() -> None:
    """FR-003 AC-15, unbounded: a drain waiting on the lock never answered ``flush(None)``.

    Run in a child, which its watchdog kills if the call never returns, so a regression fails
    this test rather than leaving a thread inside the registration lock for the rest of the run.
    """

    def work() -> str:
        _quiet_worker(PlanSink(every(D)))
        _span(0)
        with _ledger._registry.lock:
            result = lf.flush(None)
        return f"{bool(result)} {_counts(result)}"

    child = run_in_child(work)
    assert child.finished, child.output
    assert child.output == f"True {_only(sink_lost=3)}", child.output


def test_a_flush_from_a_collector_callback_inside_the_lock_returns_at_once() -> None:
    """FR-003 AC-15, the probe's shape: the collector runs inside the lock on a first credit.

    A thread's first log takes the registration lock to register its cell, and with the
    collector's threshold at 1 an allocation there runs ``gc.callbacks`` inside the section, the
    shape a ``__del__`` takes. The callback flushes with a 1 s budget, so a drain waiting on the
    lock shows as a timed-out call. The drain thread's own callbacks are left out: a flush made
    on the drain thread waits on itself whatever the lock does.
    """
    worker = _quiet_worker(PlanSink(every(A)))
    _span(0)
    assert lf.flush(10)
    results: list[tuple[str | None, float]] = []
    busy = threading.local()

    def callback(phase: str, info: dict) -> None:
        if phase != "start" or getattr(busy, "on", False):
            return
        if threading.current_thread() is worker._thread:
            return
        if not _ledger._registry.lock._is_owned():
            return
        busy.on = True
        try:
            clock = Stopwatch()
            result = lf.flush(1.0)
            results.append((result.reason, clock.stop()))
        finally:
            busy.on = False

    def first_log() -> None:
        gc.set_threshold(1)
        try:
            _span(1)
        finally:
            gc.set_threshold(*thresholds)

    thresholds = gc.get_threshold()
    gc.callbacks.append(callback)
    try:
        for _ in range(5):
            thread = threading.Thread(target=first_log, daemon=True)
            thread.start()
            thread.join(10)
            assert not thread.is_alive(), "a first log did not return"
    finally:
        gc.callbacks.remove(callback)
        gc.set_threshold(*thresholds)
    assert results, "no flush ran inside the registration lock"
    slow = [(reason, elapsed) for reason, elapsed in results if reason or elapsed >= 0.5]
    assert not slow, results


def test_a_shutdown_made_inside_the_registration_lock_closes_the_sink() -> None:
    """FR-003 AC-15, the shutdown shape: another thread's flush is outstanding on the drain.

    The drain is inside the emit carrying that flush's batch when this thread enters the lock,
    opens the sink's gate and calls ``shutdown(1)``. A drain that then waited on the lock to seal
    the marker expired the shutdown, which left the sink open.
    """
    sink = _ClosingSink()
    sink.gate.clear()
    _quiet_worker(sink)
    _span(0)
    try:
        flusher = _Call(lf.flush, 5)
        assert sink.entered.wait(10), "the drain is inside the flush's emit"
        with _ledger._registry.lock:
            sink.gate.set()
            time.sleep(0.05)
            clock = Stopwatch()
            lf.shutdown(timeout=1)
            elapsed = clock.stop()
    finally:
        sink.gate.set()
    result = flusher.join()
    assert elapsed < 0.9 and sink.closed, (elapsed, sink.closed)
    assert result and _counts(result) == _only(), result


def test_a_flush_started_while_another_thread_holds_the_lock_waits_for_it() -> None:
    """MAJOR 3, characterised: a flush reads its start under the registration lock.

    As ``health()`` does, so a ``flush()`` made while another thread is inside the lock's
    section waits for that section to end before it starts. Here the holder shuts the library
    down inside its section, so the waiting flush finds the worker retired; the shutdown itself
    is prompt and closes the sink, because nothing the drain does waits on the lock.
    """
    sink = _ClosingSink()
    _quiet_worker(sink)
    _span(0)
    with _ledger._registry.lock:
        flusher = _Call(lf.flush, 5)
        time.sleep(0.2)
        assert flusher.thread.is_alive(), "the flush read its start inside another's section"
        clock = Stopwatch()
        lf.shutdown(timeout=1)
        elapsed = clock.stop()
    result = flusher.join()
    assert elapsed < 0.9 and sink.closed, (elapsed, sink.closed)
    assert not result and result.reason == "retired", result


class _LockedStderr:
    """A stderr that serialises writes under a lock, and flushes from inside one of them.

    The lock is re-entrant so that a line the flush itself writes on the same thread cannot
    hang the test; the drain thread's line waits on it all the same.
    """

    def __init__(self, timeout: float | None) -> None:
        self.lock = threading.RLock()
        self.flush_on: threading.Thread | None = None
        self.timeout = timeout
        self.seen: dict[str, object] = {}
        self.lines: list[str] = []

    def write(self, text: str) -> int:
        with self.lock:
            if threading.current_thread() is self.flush_on:
                self.flush_on = None
                clock = Stopwatch()
                self.seen["result"] = lf.flush(self.timeout)
                self.seen["elapsed"] = clock.stop()
            self.lines.append(text)
        return len(text)

    def flush(self) -> None:
        pass

    def write_from_a_new_thread(self) -> threading.Thread:
        """Writes one application line from a fresh thread, which flushes inside the write."""

        def app() -> None:
            self.flush_on = threading.current_thread()
            self.write("an application's own line\n")

        thread = threading.Thread(target=app, daemon=True)
        thread.start()
        return thread


def test_a_flush_holding_the_stderr_a_dying_drain_writes_to_does_not_wait_on_its_line() -> None:
    """FR-003 AC-16: the caller waits for the dying drain's debit, never for its stderr line.

    The application thread holds the stderr lock while it flushes, and the drain dies under that
    flush, so its line waits on the lock. The debit is made before the line, so the call returns
    at once with ``drain_lost`` counted. A wait for the thread to end lasted the whole
    ``flush(2.0)``, so the ceiling is half of that.
    """
    sink = GatedSink()
    sink.raise_on = lambda call, batch: SystemExit(1)
    worker = _quiet_worker(sink)
    for i in range(3):
        _span(i)
    err = _LockedStderr(2.0)
    original = sys.stderr
    sys.stderr = err
    try:
        app = err.write_from_a_new_thread()
        app.join(10)
        assert not app.is_alive(), "the application's write did not return"
        worker._thread.join(10)
        assert not worker._thread.is_alive()
    finally:
        sys.stderr = original
    result = err.seen["result"]
    assert isinstance(result, FlushResult)
    assert err.seen["elapsed"] < 1.0, err.seen
    assert not result and _counts(result) == _only(drain_lost=9), result
    assert any("worker thread stopped" in line for line in err.lines), err.lines


def test_an_unbounded_flush_holding_that_stderr_returns() -> None:
    """FR-003 AC-16, unbounded: waiting for the thread to end deadlocked ``flush(None)``.

    Run in a child, which its watchdog kills if the call never returns.
    """

    def work() -> str:
        sink = GatedSink()
        sink.raise_on = lambda call, batch: SystemExit(1)
        _quiet_worker(sink)
        _span(0)
        err = _LockedStderr(None)
        sys.stderr = err
        app = err.write_from_a_new_thread()
        app.join(CHILD_TIMEOUT - 3)
        result = err.seen.get("result")
        if app.is_alive() or not isinstance(result, FlushResult):
            return f"hung: alive={app.is_alive()} result={result!r}"
        return f"{result.reason} {_counts(result)}"

    child = run_in_child(work)
    assert child.finished, child.output
    assert child.output == f"abandoned {_only(drain_lost=3)}", child.output


def test_a_fork_after_the_drain_sealed_the_marker_reads_the_childs_own_end(monkeypatch) -> None:
    """FR-003 AC-18: a snapshot the drain stamped in the parent never closes a child's window.

    The fork lands on the caller's thread after the drain answered and sealed the marker, before
    the caller closes its window, which a signal handler on that thread could do. The child then
    holds the parent's snapshot, the parent's drop of this call's 3 events included; it reads its
    own end instead, and its restarted ledger has lost nothing.
    """
    sink = PlanSink(every(D))
    _quiet_worker(sink)
    _span(0)
    assert _counts(lf.flush(10)) == _only(sink_lost=3)
    _span(1)
    real_close = _lifecycle._close_window
    parent = os.getpid()
    read_fd, write_fd = os.pipe()
    child: dict[str, int] = {}

    def fork_then_close(worker, marker, deadline, start):
        assert marker is not None and marker.ledger_pid == parent, "the drain sealed it"
        dumpable = _set_dumpable(0)
        try:
            gc.collect()
            pid = os.fork()
        finally:
            if os.getpid() == parent:
                _set_dumpable(dumpable)
        if pid == 0:
            signal.alarm(CHILD_TIMEOUT)
        else:
            child["pid"] = pid
        return real_close(worker, marker, deadline, start)

    monkeypatch.setattr(_lifecycle, "_close_window", fork_then_close)
    try:
        result = lf.flush(10)
        if os.getpid() != parent:
            os.write(write_fd, f"{bool(result)} {_counts(result)}".encode())
    finally:
        if os.getpid() != parent:
            os._exit(0)
    os.close(write_fd)
    try:
        deadline = time.monotonic() + CHILD_TIMEOUT + 2.0
        chunks = []
        while time.monotonic() < deadline:
            if not select.select([read_fd], [], [], max(0.0, deadline - time.monotonic()))[0]:
                break
            chunk = os.read(read_fd, 4096)
            if not chunk:
                break
            chunks.append(chunk)
    finally:
        os.close(read_fd)
        if "pid" in child:
            try:
                os.kill(child["pid"], signal.SIGKILL)
            except ProcessLookupError:
                pass
            os.waitpid(child["pid"], 0)
    reported = b"".join(chunks).decode()
    assert reported == f"True {_only()}", reported
    assert result and _counts(result) == _only(sink_lost=3), result


def _fork_on_this_thread(parent: int, child: dict[str, int]) -> None:
    """Forks on the calling thread, as a signal handler on it could, and arms the child's alarm."""
    dumpable = _set_dumpable(0)
    try:
        gc.collect()
        pid = os.fork()
    finally:
        if os.getpid() == parent:
            _set_dumpable(dumpable)
    if pid == 0:
        signal.alarm(CHILD_TIMEOUT)
    else:
        child["pid"] = pid


def _flush_in_both(parent: int, child: dict[str, int]) -> tuple[FlushResult, str]:
    """Runs ``flush(None)`` on this thread, which forks inside it; returns both sides' results.

    The child writes its verdict, its counts and whether it returned within 2.5 s down a pipe and
    leaves through ``os._exit``. A child that never returns is ended by its alarm, and the parent
    reads until its own deadline and then kills, so a child stuck in the wait reports nothing.
    """
    read_fd, write_fd = os.pipe()
    try:
        clock = Stopwatch()
        result = lf.flush(None)
        elapsed = clock.stop()
        if os.getpid() != parent:
            os.write(write_fd, f"{elapsed < 2.5} {result.reason} {_counts(result)}".encode())
    finally:
        if os.getpid() != parent:
            os._exit(0)
    os.close(write_fd)
    try:
        deadline = time.monotonic() + CHILD_TIMEOUT + 2.0
        chunks = []
        while time.monotonic() < deadline:
            if not select.select([read_fd], [], [], max(0.0, deadline - time.monotonic()))[0]:
                break
            chunk = os.read(read_fd, 4096)
            if not chunk:
                break
            chunks.append(chunk)
    finally:
        os.close(read_fd)
        if "pid" in child:
            try:
                os.kill(child["pid"], signal.SIGKILL)
            except ProcessLookupError:
                pass
            os.waitpid(child["pid"], 0)
    return result, b"".join(chunks).decode()


def test_a_child_forked_during_the_wait_for_a_dying_drain_leaves_it() -> None:
    """FR-003: a fork inside the wait for a dying drain leaves the child's ``flush(None)`` free.

    The execution-frame review, MINOR 1 (invariants 3 and 12). The parent's drain dies on the
    flush's batch and is held on its way into ``_terminal_failure``, so the caller waits on
    ``_drain_accounted``. The fork lands on the caller's thread inside that wait, at its second
    slice, as a signal handler's would. The child resumes the worker with a live drain that clears
    the event and sets it only when it ends, so a wait that kept going in the child would never
    return; the child stops at its next slice and reads its own end, a window the fork crossed and
    in which the child lost nothing. The parent releases its drain and counts what it held.
    """
    sink = GatedSink()
    sink.raise_on = lambda call, batch: SystemExit(1)
    worker = _quiet_worker(sink)
    release = threading.Event()
    held = threading.Event()
    _hold_before(worker, "_terminal_failure", release, held)
    parent = os.getpid()
    caller = threading.current_thread()
    child: dict[str, int] = {}
    slices: list[float | None] = []
    accounted = worker._drain_accounted
    real_wait = accounted.wait

    def forking_wait(timeout=None):
        if threading.current_thread() is caller and os.getpid() == parent:
            slices.append(timeout)
            if len(slices) == 2:
                _fork_on_this_thread(parent, child)
                if os.getpid() == parent:
                    release.set()
        return real_wait(timeout)

    accounted.wait = forking_wait
    _span(0)
    try:
        result, reported = _flush_in_both(parent, child)
    finally:
        release.set()
        del accounted.wait
    assert "pid" in child, f"the flush waited {len(slices)} slice(s) and never forked"
    assert reported == f"True abandoned {_only()}", reported
    assert not result and _counts(result) == _only(drain_lost=3), result


def test_a_child_forked_before_the_wait_for_a_dying_drain_skips_it(monkeypatch) -> None:
    """FR-003: a fork after the dying drain answered, before the wait, skips the child's wait.

    The execution-frame review, MINOR 1. The fork lands on the caller's thread between the
    marker's unwinding answer and ``_close_window``, the window a signal handler could fork in.
    The window's start was read in the parent, so the child does not wait on its resumed drain's
    event, which that live drain sets only when it ends; it reads its own end at once.
    """
    sink = GatedSink()
    sink.raise_on = lambda call, batch: SystemExit(1)
    worker = _quiet_worker(sink)
    release = threading.Event()
    held = threading.Event()
    _hold_before(worker, "_terminal_failure", release, held)
    real_close = _lifecycle._close_window
    parent = os.getpid()
    child: dict[str, int] = {}

    def fork_then_close(worker, marker, deadline, start):
        assert marker is not None and marker.unwinding, "the dying drain answered it"
        assert start is not None and start[0] == parent
        _fork_on_this_thread(parent, child)
        if os.getpid() == parent:
            release.set()
        return real_close(worker, marker, deadline, start)

    monkeypatch.setattr(_lifecycle, "_close_window", fork_then_close)
    _span(0)
    try:
        result, reported = _flush_in_both(parent, child)
    finally:
        release.set()
    assert "pid" in child, "the flush never closed its window"
    assert reported == f"True abandoned {_only()}", reported
    assert not result and _counts(result) == _only(drain_lost=3), result


def test_a_child_of_a_dead_drain_counts_its_own_queue_as_queued() -> None:
    """``_drain_accounted`` is cleared where a forked child starts a drain thread.

    The parent's drain died, so its event is set, and the fork carries an event's set state
    across. The child resumes the worker with a new thread, held inside its first emit here, so
    the span queued behind it is ``queued``; an event left set would read it as ``stranded``, a
    loss the child never took.
    """
    sink = GatedSink()
    sink.raise_on = lambda call, batch: SystemExit(1) if call == 1 else None
    worker = _quiet_worker(sink, batch_size=1)
    _span(0)
    worker._thread.join(10)
    assert not worker._thread.is_alive() and worker._drain_accounted.is_set()

    def work() -> str:
        live = _lifecycle._state._worker
        cleared = not live._drain_accounted.is_set()
        sink.gate.clear()
        sink.entered.clear()
        _span(1)
        if not sink.entered.wait(CHILD_TIMEOUT - 3):
            return "the child's drain never emitted"
        _span(2)
        events = lf.health().events
        sink.gate.set()
        return f"{cleared} queued={events.queued} stranded={events.stranded}"

    child = run_in_child(work)
    assert child.finished, child.output
    assert child.output == "True queued=3 stranded=0", child.output


def test_a_persistent_ledger_fault_writes_one_line_per_flush(monkeypatch, capsys) -> None:
    """FR-003 AC-19: a call whose start could not be read asks for no counts, so it reads once.

    Each read that faults is announced, so a call that went on to ask the drain for a snapshot
    and to read its own end wrote three lines per ``flush()`` under a fault that never clears.
    """
    _quiet_worker(PlanSink(every(A)))
    _span(0)
    assert lf.flush(10)
    capsys.readouterr()

    def broken() -> worker_mod.EventLedger:
        raise LookupError("injected")

    monkeypatch.setattr(_lifecycle, "_ledger_snapshot", broken)
    results = []
    for i in range(3):
        _span(i)
        results.append(lf.flush(10))
    err = capsys.readouterr().err
    assert err.count("(LookupError)") == 3, err
    assert results == [FlushResult(ok=True)] * 3, results


# ── FR-004 ───────────────────────────────────────────────────────────────────────────────

_DELIVERY_PHRASES = (
    "sink took them",
    "evidence of delivery",
    "means they were delivered",
    "everything landed",
    "has delivered them",
)
_FR004_FILES = (
    "README.md",
    "docs/invariants.md",
    (
        "docs/decisions/pipeline/"
        "flush-reports-delivery-and-answers-from-the-drain-that-carried-the-events.md"
    ),
    (
        "docs/decisions/public-api/"
        "a-result-that-can-grow-a-reason-must-stop-being-a-bool-before-10-not-after.md"
    ),
    "src/log_foundry/results.py",
    "src/log_foundry/__init__.py",
    "src/log_foundry/worker.py",
    "src/log_foundry/_lifecycle.py",
)


def test_no_named_file_says_a_truthy_flush_means_delivery() -> None:
    """FR-004 AC-6: a regrowth guard over the eight files the criterion names."""
    hits = [
        (name, phrase)
        for name in _FR004_FILES
        for phrase in _DELIVERY_PHRASES
        if phrase in (ROOT / name).read_text(encoding="utf-8")
    ]
    assert not hits, hits
