"""SPEC-068: what the library says and does once a shutdown's join has expired.

Four defects shared one moment — the join has given up, the drain thread is still inside the
sink — and each was a lie or a leak there. The expiry line and the unconfirmed-swap line called
queued submissions lost while their future was open (FR-001, FR-002). A stop owed by a caller on
the drain thread itself raised or waited on itself (FR-006), and the once-only gate it holds is
handed to the next caller that can join (FR-004's one exception). A worker stopped directly and
then forked resumed a drain that exited at once (FR-005). FR-003's decision, that an expired
shutdown is final, is pinned in `tests/test_shutdown_lifecycle.py`.

Every bounded wait here asserts its CPU time as well as its wall time, through
`test_worker_exit_budget._measured`, whose window is process-wide across threads, so a spinning
drain thread is caught as well as a spinning caller. Every window covers idle waits only. A
wedged sink is released in a `finally`, so no blocked drain thread outlives its test.
"""

from __future__ import annotations

import io
import os
import re
import sys
import threading
import time

import pytest

import log_foundry
from conftest import install_worker, retire_process, run_concurrently
from log_foundry import _diag, _lifecycle
from log_foundry import worker as worker_mod
from log_foundry.worker import Worker
from test_fork_lifecycle import report_from_child, run_in_child
from test_worker_exit_budget import (
    _CPU_BOUND,
    _SLACK,
    _GatedSink,
    _lost_lines,
    _measured,
    _MeasuredThread,
    _SlowFailSink,
)

_EXPIRY = (
    "log-foundry: shutdown timed out after {T}; the sink is left open because the worker thread "
    "is still using it"
)
"""The expiry line's fixed opening, as FR-001 words it; clauses are appended by each test."""

_SWAP = (
    "log-foundry: the previous sink could not be confirmed drained within {T}; it is left open "
    "until a shutdown() finds the drain thread ended"
)
"""The unconfirmed-swap line's fixed opening, as FR-002 words it."""

_SWAP_T = re.compile(r"confirmed drained within (\S+);")

_LONGEST_T = -1.23456789e-100
"""A timeout `_bounded_seconds` renders at its longest finite width, 14 characters."""


def _span(n: int, events: int = 1) -> list[dict[str, object]]:
    """One submission carrying `events` events, tagged so a delivery can be traced to it."""
    return [{"message": f"span-{n}", "i": i} for i in range(events)]


def _wedged(sink: _GatedSink, **kwargs: object) -> Worker:
    """Builds a `batch_size=1` worker and returns once its drain thread is inside the gate."""
    kwargs.setdefault("batch_size", 1)
    kwargs.setdefault("flush_interval", 60.0)
    worker = Worker(sink, **kwargs)  # type: ignore[arg-type]
    worker.submit(_span(0))
    assert sink.in_emit.wait(5.0), "the premise: the drain thread is inside the first emit"
    return worker


def _release(sink: _GatedSink, worker: Worker) -> None:
    """Lets a wedged emit return and waits for the drain thread to finish what it holds."""
    sink.release.set()
    worker._thread.join(5.0)


# --------------------------------------------------------------------------- FR-001: pending


def test_pending_writes_the_detail_with_no_loss_prefix(capsys) -> None:
    """FR-001 AC-1. The fourth writer names no count: what it reports is not lost yet."""
    _diag.pending("a; b")
    assert capsys.readouterr().err == "log-foundry: a; b\n"


def test_pending_escapes_a_newline_into_one_line(capsys) -> None:
    """FR-001 AC-1. The detail is escaped as `lost`'s is, so it cannot forge a second line."""
    _diag.pending("first\nlog-foundry: forged")
    err = capsys.readouterr().err
    assert err == "log-foundry: first\\nlog-foundry: forged\n"
    assert err.count("\n") == 1


def test_pending_cuts_a_long_detail_to_the_bound(capsys) -> None:
    """FR-001 AC-1. Escape first, then cut, then mark the cut — `_suffix`'s rule exactly."""
    _diag.pending("x" * 5000)
    assert capsys.readouterr().err == "log-foundry: " + "x" * _diag._MAX_DETAIL + "…\n"


def test_pending_cuts_after_escaping(capsys) -> None:
    """FR-001 AC-1. A newline just inside the bound grows past it once escaped, and is cut there.

    Cutting before escaping would keep all 200 raw characters and write 201 after the escape.
    """
    _diag.pending("x" * (_diag._MAX_DETAIL - 1) + "\n" + "y" * 10)
    shown = "x" * (_diag._MAX_DETAIL - 1) + "\\"
    assert capsys.readouterr().err == f"log-foundry: {shown}…\n"


def test_pending_writes_nothing_for_an_empty_detail(capsys) -> None:
    """FR-001 AC-1. A bare prefix tells a reader nothing they could act on."""
    _diag.pending("")
    assert capsys.readouterr().err == ""


def test_pending_absorbs_a_stream_fault(monkeypatch) -> None:
    """FR-001 AC-1. A stream fault is swallowed, as for every `_diag` writer."""

    class _Raising:
        calls = 0

        def write(self, text: str) -> int:
            type(self).calls += 1
            raise OSError(32, "Broken pipe")

    monkeypatch.setattr(sys, "stderr", _Raising())
    assert _diag.pending("a detail") is None
    assert _Raising.calls == 1, "it tried, and swallowed the fault"


# --------------------------------------------------------------------------- FR-001: the line


def _case_a(capsys) -> tuple[_GatedSink, Worker, str, float]:
    """Expires a `shutdown(timeout=0.3)` with a 1-event span in the emit and 2 spans queued.

    The queued spans carry 2 and 1 events, so the four events submitted can be summed against
    every `lost` line later. Returns the sink (still parked), the worker, stderr as it stood at
    the expiry, and the shutdown's wall time.
    """
    sink = _GatedSink()
    worker = _wedged(sink)
    worker.submit(_span(1, events=2))
    worker.submit(_span(2, events=1))
    install_worker(worker)
    elapsed = _measured(lambda: retire_process(worker, 0.3))
    assert worker.stopped_reason == "ShutdownTimeout", "the premise: the join expired"
    return sink, worker, capsys.readouterr().err, elapsed


def test_case_a_expiry_line_reports_in_flight_and_queued_submissions(capsys) -> None:
    """FR-001 AC-2. The queued clause counts the two spans, not the sentinel `stop()` queued."""
    sink, worker, err, elapsed = _case_a(capsys)
    try:
        assert elapsed < 0.3 + _SLACK, f"the shutdown returned within its budget: {elapsed:.2f}s"
        assert err == (
            _EXPIRY.format(T="0.3s")
            + "; 1 event(s) in flight, counted when the emit returns; 2 submission(s) still queued"
            + "\n"
        )
    finally:
        _release(sink, worker)


def test_case_a_released_to_fail_announces_each_event_lost_once(capsys) -> None:
    """FR-001 AC-3. Four events submitted, four announced lost — `fda134a` announced seven."""
    sink, worker, err, _ = _case_a(capsys)
    sink.fail = True
    _release(sink, worker)
    later = capsys.readouterr().err

    assert [(n, what) for n, what, _ in _lost_lines(later)] == [(1, "event"), (3, "event")], later
    assert sum(n for n, _, _ in _lost_lines(err + later)) == 4, err + later
    assert "item(s)" not in err + later


def test_case_a_released_to_succeed_announces_no_loss(capsys) -> None:
    """FR-001 AC-4. Had the expiry line said lost, this would be a false loss."""
    sink, worker, err, _ = _case_a(capsys)
    sink.fail = False
    _release(sink, worker)

    assert len(sink.delivered) == 4
    assert worker.failed_batches == 0
    assert "lost" not in err + capsys.readouterr().err


def test_a_queued_flush_marker_is_not_counted_as_a_submission(capsys) -> None:
    """FR-001 AC-6. A marker left queued by a timed-out flush is not a submission."""
    sink = _GatedSink()
    worker = _wedged(sink)
    try:
        assert not worker.flush(timeout=0.05), "the premise: the flush timed out, marker queued"
        worker.stop(0.2)
        assert worker._queue.qsize() == 2, "the premise: the marker and the sentinel are queued"
        assert capsys.readouterr().err == (
            _EXPIRY.format(T="0.2s") + "; 1 event(s) in flight, counted when the emit returns\n"
        )
    finally:
        _release(sink, worker)


def test_an_unreadable_queue_drops_the_queued_clause_and_raises_nothing(capsys) -> None:
    """FR-001 AC-7. A queue whose internals cannot be read costs the clause, never the line.

    A span is queued behind the wedge, so a read that worked would have written the clause.
    """

    class _Unenterable:
        def __enter__(self) -> None:
            raise RuntimeError("no")

        def __exit__(self, *exc: object) -> None:
            return None

    sink = _GatedSink()
    worker = _wedged(sink)
    worker.submit(_span(1))
    mutex = worker._queue.mutex
    worker._queue.mutex = _Unenterable()  # type: ignore[assignment]
    try:
        worker.stop(0.2)
        assert capsys.readouterr().err == (
            _EXPIRY.format(T="0.2s") + "; 1 event(s) in flight, counted when the emit returns\n"
        )
    finally:
        worker._queue.mutex = mutex
        _release(sink, worker)


def test_the_expiry_line_fits_the_detail_bound(capsys) -> None:
    """FR-001 AC-8. The longest `T` with six-digit counts is written whole, with no `…`."""
    sink = _GatedSink()
    worker = _wedged(sink)
    worker._in_flight = 999999
    worker._queued_submissions = lambda: 999999  # type: ignore[method-assign]
    try:
        worker.stop(_LONGEST_T)
        err = capsys.readouterr().err
        assert err == (
            _EXPIRY.format(T="-1.23457e-100s")
            + "; 999999 event(s) in flight, counted when the emit returns"
            + "; 999999 submission(s) still queued\n"
        )
        assert "…" not in err
    finally:
        _release(sink, worker)


# --------------------------------------------------------------------------- FR-002


def _unfenced_swap(queued: int) -> tuple[_GatedSink, Worker]:
    """Wedges the drain in the old sink with `queued` spans behind it, then swaps unconfirmed.

    The spans behind the wedge carry 2 and then 1 events, so the events they hold are known.
    Returns the old sink (still parked) and the worker, now delivering to the new sink.
    """
    old = _GatedSink()
    worker = _wedged(old)
    for n in range(queued):
        worker.submit(_span(n + 1, events=2 - n))
    install_worker(worker)
    return old, worker


def _swap_line(err: str) -> tuple[str, str]:
    """Returns the swap line and the `T` it rendered, checking `T` is within the swap's budget.

    `retarget` recomputes the budget from the deadline, so `T` is a hair under the 0.05 given.
    """
    lines = [line for line in err.splitlines() if "could not be confirmed drained" in line]
    assert len(lines) == 1, err
    match = _SWAP_T.search(lines[0])
    assert match is not None, lines[0]
    rendered = match.group(1)
    assert rendered.endswith("s") and 0.0 < float(rendered[:-1]) <= 0.05, rendered
    return lines[0], rendered


def test_the_unfenced_swap_line_counts_queued_submissions(capsys) -> None:
    """FR-002 AC-1. Two spans queued behind the wedge; the swap's own marker is not counted."""
    old, worker = _unfenced_swap(queued=2)
    try:
        _lifecycle._swap_sink(_SlowFailSink(0.0, fail_times=0), timeout=0.05)  # type: ignore[arg-type]
        line, rendered = _swap_line(capsys.readouterr().err)
        assert line == (
            _SWAP.format(T=rendered)
            + "; 2 submission(s) still queued, which may reach the new sink"
        )
        assert worker.incomplete_swaps == 1
    finally:
        old.fail = False
        _release(old, worker)


def test_the_unfenced_swap_line_with_only_its_marker_has_no_queued_clause(capsys) -> None:
    """FR-002 AC-2. The retarget's unanswered marker is queued and is not a submission."""
    old, worker = _unfenced_swap(queued=0)
    try:
        _lifecycle._swap_sink(_SlowFailSink(0.0, fail_times=0), timeout=0.05)  # type: ignore[arg-type]
        assert worker._queue.qsize() == 1, "the premise: only the retarget's marker is queued"
        err = capsys.readouterr().err
        line, rendered = _swap_line(err)
        assert line == _SWAP.format(T=rendered)
        assert "lost" not in err
    finally:
        old.fail = False
        _release(old, worker)


def test_spans_queued_at_an_unfenced_swap_are_announced_lost_once(capsys) -> None:
    """FR-002 AC-3. The queued spans fail into the new sink; each event is announced once."""
    old, worker = _unfenced_swap(queued=2)
    new = _SlowFailSink(0.0)
    _lifecycle._swap_sink(new, timeout=0.05)  # type: ignore[arg-type]
    old.fail = False
    old.release.set()
    deadline = time.monotonic() + 5.0
    while worker.failed_batches < 2 and time.monotonic() < deadline:
        threading.Event().wait(0.01)
    retire_process(worker, 5.0)
    err = capsys.readouterr().err

    assert len(old.delivered) == 1, "the wedged span reached the old sink"
    assert worker.failed_batches == 2, "both queued spans failed into the new sink"
    assert sum(n for n, _, _ in _lost_lines(err)) == 3, err
    assert "item(s)" not in err


def test_the_unfenced_swap_line_fits_the_detail_bound(capsys) -> None:
    """FR-002 AC-4. The longest `T` with a six-digit count is written whole, with no `…`."""
    sink = _SlowFailSink(0.0, fail_times=0)
    worker = Worker(sink, batch_size=1, flush_interval=0.01)  # type: ignore[arg-type]
    worker._queued_submissions = lambda: 999999  # type: ignore[method-assign]
    try:
        worker._record_incomplete_swap(_LONGEST_T, sink)  # type: ignore[arg-type]
        err = capsys.readouterr().err
        assert err == (
            _SWAP.format(T="-1.23457e-100s")
            + "; 999999 submission(s) still queued, which may reach the new sink\n"
        )
        assert "…" not in err
    finally:
        worker.stop(2.0)


# --------------------------------------------------------------------------- FR-004


def _expiry_lines(err: str) -> int:
    """How many `shutdown timed out` lines stderr holds."""
    return err.count("shutdown timed out")


def test_two_concurrent_expiring_shutdowns_write_one_line(capsys) -> None:
    """FR-004 AC-1, a regression pin. Only the caller that won the gate joins and announces.

    Against `stop()` deciding the gate on `_drain_settled` rather than on `claim_stop`, both
    callers see it clear, both join, and the wedged sink gets two lines.
    """
    sink = _GatedSink()
    worker = _wedged(sink)
    install_worker(worker)
    elapsed: list[float] = []
    lock = threading.Lock()

    def work(_index: int, _iteration: int) -> None:
        started = time.monotonic()
        _lifecycle._shutdown_worker(0.3)
        with lock:
            elapsed.append(time.monotonic() - started)

    try:
        cpu = time.process_time()
        errors = run_concurrently(work, threads=2)
        spent = time.process_time() - cpu
        err = capsys.readouterr().err
        assert not errors, errors
        assert _expiry_lines(err) == 1, err
        assert worker.stopped_reason == "ShutdownTimeout"
        assert len(elapsed) == 2 and max(elapsed) < 0.3 + _SLACK, elapsed
        assert spent < _CPU_BOUND, f"the two calls spent {spent:.3f}s of CPU: not idle"
    finally:
        _release(sink, worker)


def test_a_caller_parked_after_a_lost_claim_returns_at_once_after_the_expiry(
    capsys, monkeypatch
) -> None:
    """FR-004 AC-2, a regression pin, with an injected preemption point after a lost claim.

    The loser is parked between losing `claim_stop` and reading `draining`, and released only
    once the winner's join has expired and its line is written. It then finds the drain
    settled and returns. Against a losing branch that joins the drain thread instead, it waits
    out its whole 3 s budget on a thread that is still wedged.
    """
    sink = _GatedSink()
    worker = _wedged(sink)
    install_worker(worker)
    parked = threading.Event()
    release = threading.Event()
    measured: dict[str, float] = {}
    real_stop = Worker.stop

    def parking_stop(
        self: Worker,
        timeout: float | None = worker_mod.DEFAULT_SHUTDOWN_TIMEOUT,
        *,
        claimed: bool | None = None,
    ) -> None:
        if threading.current_thread().name == "loser" and claimed is False:
            parked.set()
            assert release.wait(10.0)
            measured["released"] = time.monotonic()
            measured["cpu"] = time.process_time()
        real_stop(self, timeout, claimed=claimed)

    def lose() -> None:
        _lifecycle._shutdown_worker(3.0)
        measured["returned"] = time.monotonic()
        measured["spent"] = time.process_time() - measured["cpu"]

    monkeypatch.setattr(Worker, "stop", parking_stop)
    winner = _MeasuredThread(lambda: _lifecycle._shutdown_worker(0.3), name="winner")
    loser = threading.Thread(target=lose, name="loser")
    try:
        winner.start()
        deadline = time.monotonic() + 5.0
        while not worker._stopped and time.monotonic() < deadline:
            threading.Event().wait(0.001)
        loser.start()
        assert parked.wait(5.0), "the loser lost its claim and is parked before `draining`"
        winner.finish(0.3 + _SLACK + 1.0)
        assert _expiry_lines(capsys.readouterr().err) == 1, "the winner's line is written"

        release.set()
        loser.join(10.0)
        assert not loser.is_alive()
        took = measured["returned"] - measured["released"]
        assert took < 0.1, f"the loser waited {took:.2f}s after its release"
        assert measured["spent"] < _CPU_BOUND, measured
        assert capsys.readouterr().err == "", "the loser writes nothing"
        assert sink.closes == 0, "the drain thread is still inside the sink"
    finally:
        release.set()
        _release(sink, worker)


def test_a_later_shutdown_does_not_reopen_a_declined_retry(capsys) -> None:
    """FR-004 AC-3, a regression pin. The first caller's deadline is the one the drain uses.

    Against a losing `claim_stop` that overwrites `_exit_deadline`, the released emit fails
    into a deadline 5 s away and the drain makes 4 attempts instead of 1.
    """
    sink = _GatedSink()
    worker = _wedged(sink)
    install_worker(worker)
    try:
        retire_process(worker, 0.3)
        first = worker._exit_deadline
        assert worker.stopped_reason == "ShutdownTimeout", "the premise: the first one expired"

        elapsed = _measured(lambda: _lifecycle._shutdown_worker(5.0))
        assert elapsed < 0.1, f"the later caller waited {elapsed:.2f}s"
        assert worker._exit_deadline == first, "the record is write-once"
    finally:
        released = time.monotonic()
        _release(sink, worker)
    joined = time.monotonic() - released
    err = capsys.readouterr().err

    assert not worker._thread.is_alive() and joined < 1.0, f"the thread took {joined:.2f}s"
    assert len(sink.attempts) == 1, f"no retry was re-opened: {sink.attempts}"
    assert worker.failed_batches == 1
    lost = _lost_lines(err)
    assert len(lost) == 1 and "shutdown() deadline" in lost[0][2], err
    retire_process(worker, 3.0)
    assert sink.closes == 1, "a later call finds the thread ended and closes the sink once"


# --------------------------------------------------------------------------- FR-006


class _ReentrantSink:
    """A sink whose first `emit` calls `log_foundry.shutdown()` from the drain thread.

    The calls are made with the timeouts in `inner`, in order, each measured where it runs —
    wall time, CPU time, what it raised, and how many closes the sink had seen when it
    returned — and kept for the test thread to assert. Before the first call the emit waits for
    `go`, so a test can queue spans behind it; with `after_claim` it also waits until that
    worker's gate has been claimed by another caller, which orders the two claims without a
    sleep. With `wedge` the first emit then blocks on `release` until the test lets it go.
    """

    def __init__(
        self,
        inner: tuple[float, ...] = (5.0,),
        *,
        wedge: bool = False,
        fail: bool = False,
    ) -> None:
        self.inner = inner
        self.wedge = wedge
        self.fail = fail
        self.go = threading.Event()
        self.go.set()
        self.after_claim: Worker | None = None
        self.in_emit = threading.Event()
        self.inner_done = threading.Event()
        self.release = threading.Event()
        self.results: list[tuple[float, float, str | None, int]] = []
        self.attempts = 0
        self.delivered: list[dict[str, object]] = []
        self.closes = 0
        self._lock = threading.Lock()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Calls `shutdown()` on the first attempt, then wedges, fails or delivers."""
        with self._lock:
            self.attempts += 1
            first = self.attempts == 1
        if first:
            self.in_emit.set()
            assert self.go.wait(5.0)
            if self.after_claim is not None:
                until = time.monotonic() + 5.0
                while not self.after_claim._stopped and time.monotonic() < until:
                    threading.Event().wait(0.001)
            for timeout in self.inner:
                self.results.append(self._measured_shutdown(timeout))
            self.inner_done.set()
            if self.wedge:
                self.release.wait(30.0)
        if self.fail:
            raise RuntimeError("down")
        with self._lock:
            self.delivered.extend(batch)

    def _measured_shutdown(self, timeout: float) -> tuple[float, float, str | None, int]:
        """Runs one `shutdown()` on this thread and reports what it cost and did."""
        cpu = time.process_time()
        wall = time.monotonic()
        raised: str | None = None
        try:
            log_foundry.shutdown(timeout=timeout)
        except BaseException as exc:
            raised = type(exc).__name__
        return time.monotonic() - wall, time.process_time() - cpu, raised, self.closes

    def close(self) -> None:
        """Counts the close."""
        with self._lock:
            self.closes += 1


def _reentrant(sink: _ReentrantSink, *, queued: int = 0) -> Worker:
    """Installs a `batch_size=1` worker over `sink`, starts its first emit, queues `queued` spans.

    The queued spans carry 2 events each and are queued before `go` is set, so they are behind
    the emit when its `shutdown()` runs.
    """
    worker = Worker(sink, batch_size=1, flush_interval=60.0)  # type: ignore[arg-type]
    install_worker(worker)
    if queued:
        sink.go.clear()
    worker.submit(_span(0))
    assert sink.in_emit.wait(5.0), "the premise: the drain thread is inside the first emit"
    for n in range(queued):
        worker.submit(_span(n + 1, events=2))
    sink.go.set()
    return worker


def _inner_result(sink: _ReentrantSink, index: int = 0) -> tuple[float, float, str | None, int]:
    """Waits for the drain-thread calls to finish and returns one of their measurements."""
    assert sink.inner_done.wait(5.0), "the drain-thread shutdown() never returned"
    return sink.results[index]


def test_a_shutdown_inside_emit_returns_and_the_queue_is_delivered(capsys) -> None:
    """FR-006 AC-1. The first caller on the drain thread neither joins itself nor raises.

    At `fda134a` this call raised `RuntimeError` ("cannot join current thread") out of a
    public call documented to raise nothing.
    """
    sink = _ReentrantSink((5.0,))
    worker = _reentrant(sink, queued=1)
    try:
        elapsed, spent, raised, _ = _inner_result(sink)
        worker._thread.join(5.0)
        assert raised is None, f"shutdown() inside emit raised {raised}"
        assert elapsed < 0.5 and spent < _CPU_BOUND, (elapsed, spent)
        assert worker._exit_deadline is not None, "the drain-thread caller recorded its deadline"
        assert not worker._thread.is_alive(), "the drain ended once the emit returned"
        assert "shutdown timed out" not in capsys.readouterr().err
        assert worker.stopped_reason is None, "nothing timed out"
        assert len(sink.delivered) == 3, "the batch and the span queued behind it"
    finally:
        sink.release.set()
        retire_process(worker, 3.0)


def test_a_shutdown_inside_a_failing_emit_leaves_every_event_counted(capsys) -> None:
    """FR-006 AC-2, a regression pin. The batch and the queue are counted by the normal lines.

    Against a drain-thread branch that discards the queue before returning — replacing
    `self._queue`, as the fork repair does — the 2 queued events vanish uncounted.
    """
    sink = _ReentrantSink((5.0,), fail=True)
    worker = _reentrant(sink, queued=1)
    try:
        _inner_result(sink)
        worker._thread.join(5.0)
        err = capsys.readouterr().err
        assert sum(n for n, _, _ in _lost_lines(err)) == 3, err
        assert worker.failed_batches == 2
    finally:
        retire_process(worker, 3.0)


@pytest.mark.parametrize("order", ["before_thread_ends", "after_thread_ends"])
def test_a_shutdown_inside_emit_never_closes_the_sink_it_is_inside(order: str) -> None:
    """FR-006 AC-3, a regression pin. The closer leaves a sink its own thread is inside.

    Against `may_be_inside` answering `False` for the calling thread, the drain-thread call's
    closer closes the sink inline from inside its own `emit`. A later call from another thread
    then closes it exactly once, whether it runs while the drain thread is still alive — it
    takes the owed join over, the gate is released, and the thread ends inside its budget — or
    after the thread has ended.
    """
    sink = _ReentrantSink((5.0,), wedge=order == "before_thread_ends")
    worker = _reentrant(sink)
    try:
        _, _, raised, closes_at_return = _inner_result(sink)
        assert raised is None
        assert closes_at_return == 0, "the inner call closed the sink it was inside"
        if order == "before_thread_ends":
            main = _MeasuredThread(lambda: _lifecycle._shutdown_worker(3.0), name="main")
            main.start()
            until = time.monotonic() + 5.0
            while worker._join_owed and time.monotonic() < until:
                threading.Event().wait(0.001)
            assert not worker._join_owed, "the main call took the owed join over"
            sink.release.set()
            main.finish(3.0 + _SLACK)
        else:
            worker._thread.join(5.0)
            assert not worker._thread.is_alive()
            retire_process(worker, 3.0)
        assert sink.closes == 1, f"closed {sink.closes} times"
        retire_process(worker, 1.0)
        assert sink.closes == 1, "and a further call leaves the count at 1"
    finally:
        sink.release.set()


def _handed_over(capsys, sink: _ReentrantSink) -> tuple[Worker, float, float, float]:
    """Wedges after a drain-thread `shutdown()`, then runs a main-thread `shutdown(0.5)`.

    Returns the worker, the main call's wall time and the instants it was called and returned.
    """
    worker = _reentrant(sink)
    _inner_result(sink)
    capsys.readouterr()
    called = time.monotonic()
    elapsed = _measured(lambda: _lifecycle._shutdown_worker(0.5))
    return worker, elapsed, called, time.monotonic()


def test_a_wedge_after_a_drain_thread_shutdown_is_announced_by_the_next_caller(capsys) -> None:
    """FR-006 AC-4. The join the drain thread could not make is made by the next caller.

    Without the handover the main call waits out its budget on `_drain_settled` and writes
    nothing; under `atexit` that was a 30.12 s exit with no line and no close.
    """
    sink = _ReentrantSink((5.0,), wedge=True)
    try:
        worker, elapsed, called, returned = _handed_over(capsys, sink)
        err = capsys.readouterr().err
        assert elapsed < 0.5 + _SLACK, f"the main call took {elapsed:.2f}s"
        assert err.count("shutdown timed out after 0.5s") == 1, err
        assert worker.stopped_reason == "ShutdownTimeout"
        recorded = worker._exit_deadline
        assert recorded is not None and called + 0.5 <= recorded <= returned + 0.5, (
            "the deadline is the main caller's, not the drain-thread caller's"
        )
        assert sink.closes == 0, "the drain thread is still inside the sink"
    finally:
        sink.release.set()


def test_only_one_caller_takes_the_owed_join_over(capsys) -> None:
    """FR-006 AC-5. Once taken, the owed join is gone, and a third caller loses as before.

    Against a handover that leaves `_join_owed` set, the third call takes the gate over again,
    joins for its whole 3 s and writes a second line.
    """
    sink = _ReentrantSink((5.0,), wedge=True)
    try:
        _handed_over(capsys, sink)
        capsys.readouterr()
        elapsed = _measured(lambda: _lifecycle._shutdown_worker(3.0))
        assert elapsed < 0.1, f"the third call took {elapsed:.2f}s"
        assert capsys.readouterr().err == "", "and wrote nothing"
    finally:
        sink.release.set()


def test_a_second_drain_thread_shutdown_does_not_take_the_owed_join(capsys) -> None:
    """FR-006 AC-6. A loser on the drain thread returns, and the join stays owed.

    Against a drain-thread loser that clears `_join_owed`, the main call loses too, waits out
    its budget on `_drain_settled` and writes nothing.
    """
    sink = _ReentrantSink((5.0, 5.0), wedge=True)
    try:
        worker = _reentrant(sink)
        elapsed, spent, raised, _ = _inner_result(sink, 1)
        assert raised is None and elapsed < 0.1 and spent < _CPU_BOUND, (elapsed, spent, raised)
        assert worker._join_owed, "the second drain-thread call left the join owed"
        capsys.readouterr()

        called = time.monotonic()
        _measured(lambda: _lifecycle._shutdown_worker(0.5))
        returned = time.monotonic()
        assert capsys.readouterr().err.count("shutdown timed out after 0.5s") == 1
        recorded = worker._exit_deadline
        assert recorded is not None and called + 0.5 <= recorded <= returned + 0.5
    finally:
        sink.release.set()


@pytest.mark.parametrize(("main_timeout", "inner_timeout"), [(3.0, 1.0), (1.0, 5.0)])
def test_a_drain_thread_shutdown_that_loses_returns_at_once(
    main_timeout: float, inner_timeout: float, capsys
) -> None:
    """FR-006 AC-7 and AC-8. A loser on the drain thread never waits on its own thread.

    The main call is joining; the sink's `emit` waits until that claim is visible and only then
    calls `shutdown()`, so the inner call provably loses. At `fda134a` it waited on
    `_drain_settled`, which only this thread's exit or the main call's expiry could set: with
    budgets 3.0 and 1.0 it blocked 1.01 s, and with 1.0 and 5.0 it made the main join expire.
    """
    sink = _ReentrantSink((inner_timeout,))
    sink.go.clear()
    worker = Worker(sink, batch_size=1, flush_interval=60.0)  # type: ignore[arg-type]
    install_worker(worker)
    sink.after_claim = worker
    worker.submit(_span(0))
    assert sink.in_emit.wait(5.0)
    sink.go.set()

    called = time.monotonic()
    main = _MeasuredThread(lambda: _lifecycle._shutdown_worker(main_timeout), name="main")
    main.start()
    elapsed = main.finish(main_timeout + _SLACK + 1.0)
    returned = time.monotonic()

    took, spent, raised, _ = _inner_result(sink)
    assert raised is None and took < 0.1 and spent < _CPU_BOUND, (took, spent, raised)
    recorded = worker._exit_deadline
    assert recorded is not None, "the premise"
    assert called + main_timeout <= recorded <= returned + main_timeout, (
        "the premise: the main caller won the gate, so the inner call lost"
    )
    assert elapsed < main_timeout + _SLACK, f"the main call took {elapsed:.2f}s"
    assert worker.stopped_reason is None, "the main join did not expire"
    assert "shutdown timed out" not in capsys.readouterr().err
    assert sink.closes == 1, f"closed {sink.closes} times"


def test_the_owed_join_is_taken_over_once_by_concurrent_callers(capsys) -> None:
    """FR-006, the single-use handover under contention, through the `shutdown()` route.

    After a drain-thread win and a wedge, two `shutdown(0.3)` calls start together. Exactly one
    takes the owed join over, joins and announces; the other loses. The recorded deadline is one
    of the two callers', bracketed by its own call and return.
    """
    sink = _ReentrantSink((5.0,), wedge=True)
    spans: list[tuple[float, float]] = []
    lock = threading.Lock()

    def work(_index: int, _iteration: int) -> None:
        called = time.monotonic()
        _lifecycle._shutdown_worker(0.3)
        with lock:
            spans.append((called, time.monotonic()))

    try:
        worker = _reentrant(sink)
        _inner_result(sink)
        capsys.readouterr()
        errors = run_concurrently(work, threads=2)
        assert not errors, errors
        assert _expiry_lines(capsys.readouterr().err) == 1
        recorded = worker._exit_deadline
        assert recorded is not None
        assert any(c + 0.3 <= recorded <= r + 0.3 for c, r in spans), (recorded, spans)
        assert not worker._join_owed
    finally:
        sink.release.set()


class _RendezvousLock:
    """Wraps a worker's `_lock` so the first acquisition by each of two stoppers waits for both.

    Each stopper has necessarily done everything `claim_stop` does before its lock by the time
    it arrives here, so a decision read outside the lock is read by both before either can
    change it. That is the interleaving `run_concurrently` reaches only by chance.
    """

    def __init__(self, inner: threading.Lock) -> None:
        self.inner = inner
        self.barrier = threading.Barrier(2, timeout=5.0)
        self.seen: set[str] = set()

    def __enter__(self) -> bool:
        name = threading.current_thread().name
        if name.startswith("stopper") and name not in self.seen:
            self.seen.add(name)
            self.barrier.wait()
        return self.inner.__enter__()

    def __exit__(self, *exc: object) -> None:
        self.inner.__exit__(*exc)


def test_the_owed_join_is_taken_over_once_when_both_claims_meet_at_the_lock(capsys) -> None:
    """FR-006, the single-use handover with both claims forced to meet at `Worker._lock`.

    `_shutdown_worker` claims under the owner's lock, which serializes two `shutdown()` calls
    before `Worker._lock` is reached; two direct `stop()` calls claim under no outer lock. Both
    are held at the lock until both have arrived. Against a handover decided on a read of
    `_join_owed` taken outside the lock, both take the gate over, both join, and the wedge is
    announced twice.
    """
    sink = _ReentrantSink((5.0,), wedge=True)
    try:
        worker = _reentrant(sink)
        _inner_result(sink)
        capsys.readouterr()
        worker._lock = _RendezvousLock(worker._lock)  # type: ignore[assignment]
        stoppers = [
            threading.Thread(target=worker.stop, args=(0.3,), name=f"stopper-{i}") for i in (0, 1)
        ]
        for thread in stoppers:
            thread.start()
        for thread in stoppers:
            thread.join(5.0)
        assert not any(thread.is_alive() for thread in stoppers)
        assert _expiry_lines(capsys.readouterr().err) == 1
        assert not worker._join_owed
    finally:
        sink.release.set()


# --------------------------------------------------------------------------- FR-005

_STARTED = "CHILD-STARTED;"
"""Written by the child before anything else, so a child that crashed cannot read as a pass."""


class _PidSink:
    """Records which process delivered each batch; in a child, optionally slow and failing.

    The parent's emits always deliver, so the worker can be stopped having delivered a span.
    With `child_fails`, an emit in any other process idles for `child_seconds` and raises.
    """

    def __init__(self, *, child_fails: bool = False, child_seconds: float = 0.5) -> None:
        self.parent = os.getpid()
        self.child_fails = child_fails
        self.child_seconds = child_seconds
        self.log_foundry_stop_signal: threading.Event | None = None
        self.emits: list[tuple[int, int, bool]] = []
        self.closes = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Records the attempt, then delivers, or in a child with `child_fails` idles and raises."""
        failing = self.child_fails and os.getpid() != self.parent
        self.emits.append((os.getpid(), len(batch), not failing))
        if failing:
            threading.Event().wait(self.child_seconds)
            raise RuntimeError("down in the child")

    def close(self) -> None:
        """Counts the close."""
        self.closes += 1


def _directly_stopped(sink: _PidSink) -> Worker:
    """A worker that delivered one span and was stopped by `Worker.stop`, never by `shutdown()`.

    A direct stop never moves the retirement count, so the fork handler resumes this worker;
    both halves of that premise are asserted, with the stop event the child will inherit set.
    """
    worker = Worker(sink, batch_size=10, flush_interval=30.0)  # type: ignore[arg-type]
    install_worker(worker)
    worker.submit(_span(0))
    worker.stop(5.0)
    assert [n for _, n, ok in sink.emits if ok] == [1], "the premise: the parent delivered"
    assert _lifecycle._state.live_worker() is worker, "the premise: the child will resume it"
    assert worker._stop.is_set(), "the premise: the child inherits a set stop event"
    return worker


def _child_fields(child: object) -> dict[str, str]:
    """Checks the positive control and parses the child's `key=value` report."""
    assert child.finished, f"the child did not exit cleanly: {child}"  # type: ignore[attr-defined]
    output: str = child.output  # type: ignore[attr-defined]
    assert output.startswith(_STARTED), f"the child never reported its start: {output!r}"
    return dict(field.split("=", 1) for field in output[len(_STARTED) :].split())


def test_a_directly_stopped_worker_resumes_delivering_in_a_forked_child() -> None:
    """FR-005 AC-1, AC-2 and AC-4. The resumed child has a drain thread that runs.

    At `fda134a` the child inherited `_stopped` and a set `_stop`, its drain thread exited
    before the first submission, nothing was delivered and its `shutdown()` recorded no
    deadline. Its sink is the parent's, which the child must never close.
    """
    sink = _PidSink()
    worker = _directly_stopped(sink)

    def child() -> str:
        report_from_child(_STARTED)
        alive = worker._thread.is_alive()
        worker.submit(_span(1))
        cpu = time.process_time()
        wall = time.monotonic()
        log_foundry.shutdown(timeout=2.0)
        elapsed, spent = time.monotonic() - wall, time.process_time() - cpu
        mine = sum(n for pid, n, ok in sink.emits if pid == os.getpid() and ok)
        return (
            f"alive={alive} delivered={mine} elapsed={elapsed:.3f} cpu={spent:.4f} "
            f"deadline={worker._exit_deadline is not None} closes={sink.closes}"
        )

    fields = _child_fields(run_in_child(child))
    assert fields["alive"] == "True", fields
    assert fields["delivered"] == "1", f"the child delivered its own span: {fields}"
    assert float(fields["elapsed"]) < 0.5, fields
    assert float(fields["cpu"]) < _CPU_BOUND, fields
    assert fields["deadline"] == "True", fields
    assert fields["closes"] == "0", "the child never closes a sink its parent acquired"


def test_a_resumed_child_declines_retries_against_its_own_deadline() -> None:
    """FR-005 AC-3 and AC-4. The child's exit drain fits the child's own budget.

    A 0.5 s failing sink under `shutdown(timeout=1.2)`: two attempts, then the third is
    declined. `fda134a` and a fix that resets `_stopped` alone both make no attempt at all.
    """
    sink = _PidSink(child_fails=True)
    worker = _directly_stopped(sink)

    def child() -> str:
        report_from_child(_STARTED)
        captured = io.StringIO()
        sys.stderr = captured
        worker.submit(_span(1))
        cpu = time.process_time()
        wall = time.monotonic()
        log_foundry.shutdown(timeout=1.2)
        elapsed, spent = time.monotonic() - wall, time.process_time() - cpu
        attempts = sum(1 for pid, _, _ in sink.emits if pid == os.getpid())
        lost = _lost_lines(captured.getvalue())
        declined = sum(1 for _, _, rest in lost if "shutdown() deadline" in rest)
        return (
            f"attempts={attempts} elapsed={elapsed:.3f} cpu={spent:.4f} lost={len(lost)} "
            f"declined={declined} failed={worker.failed_batches} closes={sink.closes}"
        )

    fields = _child_fields(run_in_child(child))
    assert fields["attempts"] == "2", fields
    assert float(fields["elapsed"]) < 1.2 + _SLACK, fields
    assert float(fields["cpu"]) < _CPU_BOUND, fields
    assert (fields["lost"], fields["declined"]) == ("1", "1"), fields
    assert fields["failed"] == "1", fields
    assert fields["closes"] == "0", "the child never closes a sink its parent acquired"


class _SignalSink:
    """A sink that declares `log_foundry_stop_signal`, so the worker offers it the stop event."""

    def __init__(self) -> None:
        self.log_foundry_stop_signal: threading.Event | None = None

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Delivers nowhere."""

    def close(self) -> None:
        """Nothing to close."""


def test_resume_resets_the_stop_gate_and_replaces_a_set_stop_event() -> None:
    """FR-005 AC-5. The resume branch gives a stopped worker what `__init__` gives a new one."""
    sink = _SignalSink()
    worker = Worker(sink, batch_size=1, flush_interval=0.01)  # type: ignore[arg-type]
    worker.stop(2.0)
    worker._join_owed = True
    inherited = worker._stop
    assert worker._stopped and inherited.is_set(), "the premise: a directly stopped worker"

    worker._reinit_after_fork(resume=True)
    try:
        assert worker._stopped is False
        assert worker._join_owed is False
        assert worker._exit_deadline is None
        assert worker._stop is not inherited and worker._stop.is_set() is False
        assert worker._stop is _lifecycle._state._stop, "the owner and the worker share it"
        assert sink.log_foundry_stop_signal is worker._stop, "and the sink was offered it"
    finally:
        worker.stop(2.0)
        worker._thread.join(5.0)
    assert not worker._thread.is_alive()


def test_resume_keeps_an_unset_stop_event() -> None:
    """FR-005 AC-5. A worker never stopped keeps the very event it had.

    Called outside a fork, the original drain thread is still alive, blocked in `get` on the
    queue the reinit replaced; that queue is handed a sentinel at teardown so both threads end.
    """
    sink = _SignalSink()
    worker = Worker(sink, batch_size=1, flush_interval=0.01)  # type: ignore[arg-type]
    before = worker._stop
    original_thread, original_queue = worker._thread, worker._queue
    assert not before.is_set(), "the premise: never stopped"

    worker._reinit_after_fork(resume=True)
    try:
        assert worker._stop is before
        assert worker._stop is _lifecycle._state._stop
    finally:
        original_queue.put_nowait(worker_mod._SHUTDOWN)
        worker.stop(2.0)
        worker._thread.join(5.0)
        original_thread.join(5.0)
    assert not worker._thread.is_alive() and not original_thread.is_alive()


def test_retire_leaves_the_stop_gate_and_event_as_inherited() -> None:
    """FR-005 AC-6, a regression pin. A fork does not undo a stop on the retired branch.

    Against the resets applied on both branches, a retired child is handed a stop gate it can
    win again and a stop event that is no longer set.
    """
    worker = Worker(_SignalSink(), batch_size=1, flush_interval=0.01)  # type: ignore[arg-type]
    worker.stop(2.0)
    worker._join_owed = True
    inherited, deadline = worker._stop, worker._exit_deadline

    worker._reinit_after_fork(resume=False)

    assert worker._stopped is True
    assert worker._join_owed is True
    assert worker._stop is inherited and inherited.is_set()
    assert worker._exit_deadline == deadline
