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

import re
import sys
import threading
import time

from conftest import install_worker, retire_process
from log_foundry import _diag, _lifecycle
from log_foundry.worker import Worker
from test_worker_exit_budget import (
    _SLACK,
    _GatedSink,
    _lost_lines,
    _measured,
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
