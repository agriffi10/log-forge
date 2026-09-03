"""SPEC-004 — background flush worker: submit, batching, retry, backpressure, shutdown.

These construct real ``Worker`` instances with local test sinks (recording / sleepy /
blocking / flaky) and prefer draining via ``shutdown()`` over sleeping. Where a test must
observe an auto-flush *before* shutdown (count/time triggers), it polls with a bounded
`_wait_until` rather than a fixed sleep.
"""

import os
import pathlib
import subprocess
import sys
import threading
import time

import pytest

import log_foundry as log_foundry_mod
from log_foundry import _lifecycle
from log_foundry import worker as worker_mod

FlushResult = log_foundry_mod.FlushResult

Worker = worker_mod.Worker
_SHUTDOWN_SENTINEL = worker_mod._SHUTDOWN


def _wait_until(predicate, timeout: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


def _span(msg) -> list[dict]:
    """One span's worth of events (a single-event list, as submit() receives)."""
    return [{"message": msg}]


class RecordingSink:
    def __init__(self) -> None:
        self.batches: list[list[dict]] = []
        self.closed = 0
        self._lock = threading.Lock()

    def emit(self, batch: list[dict]) -> None:
        with self._lock:
            self.batches.append(list(batch))

    def close(self) -> None:
        self.closed += 1

    @property
    def events(self) -> list[dict]:
        with self._lock:
            return [e for b in self.batches for e in b]


class _BoundedEvent(threading.Event):
    """An `Event` whose `wait()` is bounded even when the caller passes no timeout.

    `BlockingSink.emit` waits without one, which is right for tests that always release it. A
    test asserting `submit` did **not** wait needs the failure to be a failed assertion rather
    than a hung suite, so it substitutes this: unreleased, the emit ends after 30 s and the
    assertion reports how long `submit` took.
    """

    def wait(self, timeout: float | None = None) -> bool:
        """Waits, substituting a 30 s bound for an unbounded caller.

        Args:
          timeout: Seconds to wait, or `None` for the substituted bound.

        Returns:
          Whether the event was set.

        Raises:
          None.
        """
        return super().wait(30.0 if timeout is None else timeout)


class BlockingSink(RecordingSink):
    def __init__(self) -> None:
        super().__init__()
        self.in_emit = threading.Event()
        self.release = threading.Event()

    def emit(self, batch: list[dict]) -> None:
        self.in_emit.set()
        self.release.wait()
        super().emit(batch)


class FlakySink(RecordingSink):
    """Raises on the first ``fail_times`` emit attempts, then records normally."""

    def __init__(self, fail_times: int) -> None:
        super().__init__()
        self.fail_times = fail_times
        self.attempts = 0

    def emit(self, batch: list[dict]) -> None:
        self.attempts += 1
        if self.attempts <= self.fail_times:
            raise RuntimeError("transient sink failure")
        super().emit(batch)


class AlwaysFailSink(RecordingSink):
    def __init__(self) -> None:
        super().__init__()
        self.attempts = 0

    def emit(self, batch: list[dict]) -> None:
        self.attempts += 1
        raise RuntimeError("sink down")


# -- FR-001: non-blocking submit --------------------------------------------------------


def test_submit_returns_before_emit_completes() -> None:
    """FR-001. `submit` hands off; it does not wait on the sink.

    The budget is deliberately generous and the **gap** is what carries the claim. This asserted
    `< 0.1` against a 0.3 s sleep, a gap of 3x — tight enough that the scheduler, not the
    library, decides the outcome under load, and a test proving an operation is *not* bounded
    fails on its own setup long before it fails on its subject. Against a sink that blocks until
    released the gap is the whole 30 s wait, so a synchronous `submit` misses by two orders of
    magnitude, and the test still finishes in milliseconds because the release comes right after
    the assertion. Bounded rather than unbounded so a regression **fails** rather than hanging.
    """
    sink = BlockingSink()
    sink.release = _BoundedEvent()
    w = Worker(sink, batch_size=1, flush_interval=0.01)

    start = time.monotonic()
    w.submit(_span("x"))
    elapsed = time.monotonic() - start

    sink.release.set()
    assert elapsed < 1.0, f"submit blocked for {elapsed:.2f}s on a sink that waits 30s"

    w.shutdown()
    assert any(e["message"] == "x" for e in sink.events)


# -- FR-002: batching -------------------------------------------------------------------


def test_batching_no_loss_in_order() -> None:
    sink = RecordingSink()
    w = Worker(sink, batch_size=5, flush_interval=0.05)
    for i in range(23):
        w.submit(_span(i))
    w.shutdown()

    assert [e["message"] for e in sink.events] == list(range(23))


def test_emit_receives_a_flat_list_of_dicts() -> None:
    sink = RecordingSink()
    w = Worker(sink, batch_size=3, flush_interval=0.05)
    for i in range(6):
        w.submit(_span(i))
    w.shutdown()

    assert sink.batches, "at least one batch emitted"
    for batch in sink.batches:
        assert isinstance(batch, list)
        assert all(isinstance(e, dict) for e in batch)


def test_batch_size_triggers_emit_before_shutdown() -> None:
    sink = RecordingSink()
    w = Worker(sink, batch_size=3, flush_interval=100.0)  # long interval: only count triggers
    try:
        for i in range(3):
            w.submit(_span(i))
        assert _wait_until(lambda: len(sink.events) >= 3), "batch_size should trigger a flush"
    finally:
        w.shutdown()


def test_flush_interval_triggers_emit_before_shutdown() -> None:
    sink = RecordingSink()
    w = Worker(sink, batch_size=1000, flush_interval=0.05)  # large count: only time triggers
    try:
        w.submit(_span("t"))
        assert _wait_until(lambda: len(sink.events) >= 1), "flush_interval should trigger a flush"
    finally:
        w.shutdown()


# -- FR-003: retry with backoff ---------------------------------------------------------


def test_retry_survives_transient_failure() -> None:
    sink = FlakySink(fail_times=2)
    w = Worker(sink, batch_size=1, flush_interval=0.02, max_retries=3)
    w.submit(_span("r"))
    w.shutdown()

    assert any(e["message"] == "r" for e in sink.events), "batch delivered after retries"
    assert sink.attempts >= 3, "the failing attempts were retried"
    assert w.failed_batches == 0


def test_worker_survives_emit_exception_and_keeps_processing() -> None:
    sink = FlakySink(fail_times=1)
    w = Worker(sink, batch_size=1, flush_interval=0.02, max_retries=3)
    w.submit(_span("a"))
    assert _wait_until(lambda: any(e["message"] == "a" for e in sink.events))
    w.submit(_span("b"))
    w.shutdown()

    assert {"a", "b"} <= {e["message"] for e in sink.events}


def test_batch_abandoned_after_retry_bound_is_counted(capsys) -> None:
    sink = AlwaysFailSink()
    w = Worker(sink, batch_size=1, flush_interval=0.02, max_retries=2)
    w.submit(_span("x"))
    w.shutdown()

    assert w.failed_batches >= 1, "an unrecoverable batch is counted, not silently dropped"
    assert "lost 1 event(s)" in capsys.readouterr().err, "the line carries the count"
    assert sink.events == [], "nothing was ever successfully emitted"
    assert sink.attempts >= 3, "tried max_retries + 1 times before abandoning"


# -- FR-004: backpressure (drop-newest + count) -----------------------------------------


def test_full_queue_drops_newest_and_counts() -> None:
    sink = BlockingSink()
    w = Worker(sink, batch_size=1, flush_interval=0.02, max_queue=1)
    try:
        w.submit(_span(0))  # pulled by the thread → emit blocks, holding the worker
        assert sink.in_emit.wait(2.0), "worker should have entered emit"

        w.submit(_span(1))  # queue has room (max_queue=1)
        w.submit(_span(2))  # full → dropped
        w.submit(_span(3))  # full → dropped
        assert w.dropped == 2
    finally:
        sink.release.set()
        w.shutdown()

    delivered = {e["message"] for e in sink.events}
    assert delivered == {0, 1}, "dropped submissions never reached the sink"


def test_dropped_counter_is_observable() -> None:
    sink = BlockingSink()
    w = Worker(sink, batch_size=1, max_queue=1)
    try:
        w.submit(_span("a"))
        sink.in_emit.wait(2.0)
        w.submit(_span("b"))
        w.submit(_span("c"))
        assert isinstance(w.dropped, int) and w.dropped >= 1
    finally:
        sink.release.set()
        w.shutdown()


# -- regression: idle worker must block, not busy-spin ----------------------------------


def test_idle_worker_does_not_busy_spin(monkeypatch) -> None:
    import queue as queue_module

    counts = {"get": 0}

    class CountingQueue(queue_module.Queue):
        def get(self, *args, **kwargs):
            counts["get"] += 1
            return super().get(*args, **kwargs)

    monkeypatch.setattr(worker_mod.queue, "Queue", CountingQueue)

    sink = RecordingSink()
    w = Worker(sink, batch_size=100, flush_interval=0.05)
    try:
        time.sleep(0.4)
        # A correct worker blocks ~one get() per flush_interval while idle (~8 over 0.4s). A
        # busy-spin (last_flush never advancing) would be tens of thousands to millions.
        assert counts["get"] < 100, f"idle worker appears to busy-spin: {counts['get']} get() calls"
    finally:
        w.shutdown()


# -- FR-005: graceful shutdown ----------------------------------------------------------


def test_shutdown_drains_buffered_events_and_closes_sink() -> None:
    sink = RecordingSink()
    w = Worker(sink, batch_size=100, flush_interval=100.0)  # nothing auto-flushes
    for i in range(5):
        w.submit(_span(i))
    assert sink.events == [], "still buffered before shutdown"

    w.shutdown()
    assert [e["message"] for e in sink.events] == list(range(5)), "drain emits the tail"
    assert sink.closed == 1


def test_shutdown_is_idempotent() -> None:
    sink = RecordingSink()
    w = Worker(sink, batch_size=100, flush_interval=100.0)
    w.submit(_span("x"))
    w.shutdown()
    w.shutdown()  # must not raise or double-close

    assert sink.closed == 1


# -- FR-006: lazy per-process worker ----------------------------------------------------


def test_decorator_worker_is_lazy_and_single() -> None:
    import log_foundry

    log_foundry.configure(service="t", version="0", env="t", sink=RecordingSink())
    _lifecycle._state._worker = None  # ensure a clean lazy creation for this assertion

    w1 = _lifecycle._get_worker()
    w2 = _lifecycle._get_worker()
    try:
        assert w1 is w2, "one worker per process, reused"
        assert w1.sink is log_foundry.get_config().sink, "worker built from the configured sink"
    finally:
        w1.shutdown()
        _lifecycle._state._worker = None


# -- SPEC-013 FR-002: flush() drains without retiring the worker ------------------------


def test_flush_drains_and_leaves_the_worker_running() -> None:
    sink = RecordingSink()
    # Neither trigger can fire on its own: if these events arrive, flush() delivered them.
    w = Worker(sink, batch_size=1000, flush_interval=100.0)
    try:
        for i in range(5):
            w.submit(_span(i))
        assert sink.events == [], "still buffered before the flush"

        result = w.flush(timeout=5.0)
        assert result
        assert isinstance(result, FlushResult), (
            "reverting Worker.flush to a bare bool was caught by exactly one test in 1265, "
            "and only incidentally -- `assert not x` is satisfied by any falsy value"
        )
        assert result.reason is None
        assert [e["message"] for e in sink.events] == list(range(5))

        # The whole point: unlike shutdown(), everything is still alive afterwards.
        assert sink.closed == 0, "flush must not close the sink"
        assert w._thread.is_alive(), "flush must not retire the worker thread"

        w.submit(_span("after"))
        assert w.flush(timeout=5.0)
        assert sink.events[-1]["message"] == "after", "the worker still works after a flush"
    finally:
        w.shutdown()


def test_flush_does_not_merely_wait_out_the_flush_interval() -> None:
    sink = RecordingSink()
    w = Worker(sink, batch_size=1000, flush_interval=100.0)
    try:
        for i in range(20):
            w.submit(_span(i))

        start = time.monotonic()
        assert w.flush(timeout=5.0)
        elapsed = time.monotonic() - start

        assert [e["message"] for e in sink.events] == list(range(20)), "ordering holds"
        # A flush() implemented as a sleep, or one that let the batching triggers decide,
        # could not have delivered these inside a 100s interval.
        assert elapsed < 1.0, f"flush should return promptly, took {elapsed:.3f}s"
    finally:
        w.shutdown()


def test_flush_marker_never_reaches_the_sink() -> None:
    """The marker must be excluded from `pending`, not appended like a list of events."""
    sink = RecordingSink()
    w = Worker(sink, batch_size=1000, flush_interval=100.0)
    try:
        w.submit(_span("a"))
        w.submit(_span("b"))
        assert w.flush(timeout=5.0)

        for batch in sink.batches:
            for event in batch:
                assert isinstance(event, dict), f"a non-event reached the sink: {event!r}"
        assert [e["message"] for e in sink.events] == ["a", "b"], "exactly the submitted events"
    finally:
        w.shutdown()


def test_shutdown_answers_a_marker_left_in_the_queue() -> None:
    """`_final_drain` has its own copy of the exclusion guard, so it needs its own test."""
    sink = BlockingSink()
    w = Worker(sink, batch_size=1, flush_interval=100.0)
    flushed: list[bool] = []

    w.submit(_span("a"))  # pulled by the thread → emit blocks, holding the worker
    assert sink.in_emit.wait(2.0), "worker should have entered emit"
    w.submit(_span("b"))  # queues up behind the blocked worker

    flusher = threading.Thread(target=lambda: flushed.append(w.flush(timeout=5.0)))
    flusher.start()
    stopper = threading.Thread(target=w.shutdown)
    try:
        assert _wait_until(lambda: w._queue.qsize() >= 2), "marker should be queued behind 'b'"
        stopper.start()
        # White-box on purpose: waiting for `_stop` guarantees the worker leaves the main loop
        # when released and reaches `_final_drain`, rather than racing through `_run` instead.
        assert _wait_until(w._stop.is_set), "shutdown should have signalled the stop"
    finally:
        sink.release.set()

    stopper.join(5.0)
    flusher.join(5.0)

    assert [bool(r) for r in flushed] == [True], "a flush racing shutdown is answered by the final drain"
    assert {e["message"] for e in sink.events} == {"a", "b"}, "and its events really did land"
    for batch in sink.batches:
        assert all(isinstance(e, dict) for e in batch), "no marker leaked into the final batch"


def test_flush_is_repeatable_and_concurrent() -> None:
    sink = RecordingSink()
    w = Worker(sink, batch_size=1000, flush_interval=100.0)
    try:
        w.submit(_span("first"))
        assert w.flush(timeout=5.0)
        w.submit(_span("second"))
        assert w.flush(timeout=5.0)
        assert [e["message"] for e in sink.events] == ["first", "second"]

        # Each concurrent call gets its own marker and its own event.
        results: list[bool] = []
        lock = threading.Lock()

        def _flush() -> None:
            ok = w.flush(timeout=5.0)
            with lock:
                results.append(ok)

        w.submit(_span("third"))
        threads = [threading.Thread(target=_flush) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(5.0)

        assert [bool(r) for r in results] == [True] * 4, "every concurrent flush is answered"
        assert sink.events[-1]["message"] == "third"
    finally:
        w.shutdown()


def test_flush_does_not_consume_the_shutdown_flag() -> None:
    sink = RecordingSink()
    w = Worker(sink, batch_size=1000, flush_interval=100.0)
    w.submit(_span("x"))
    assert w.flush(timeout=5.0)
    assert w.flush(timeout=5.0)

    w.submit(_span("tail"))
    w.shutdown()  # must still be a full, working shutdown

    assert any(e["message"] == "tail" for e in sink.events), "shutdown still drains"
    assert sink.closed == 1, "shutdown still closes the sink"


# -- SPEC-013 FR-003: flush() cannot hang, and cannot resurrect a dead worker ------------


def test_flush_after_shutdown_returns_false_promptly() -> None:
    sink = RecordingSink()
    w = Worker(sink, batch_size=1000, flush_interval=100.0)
    w.shutdown()

    start = time.monotonic()
    assert not w.flush(timeout=5.0), "nothing will ever consume the marker now"
    elapsed = time.monotonic() - start
    # The bug this guards: waiting out `timeout` here turns a logging problem into an
    # invocation that blows its execution deadline.
    assert elapsed < 1.0, f"must not wait out the timeout, took {elapsed:.3f}s"


def test_flush_honours_its_timeout_on_a_wedged_sink() -> None:
    sink = BlockingSink()
    w = Worker(sink, batch_size=1, flush_interval=0.01)
    try:
        w.submit(_span("stuck"))
        assert sink.in_emit.wait(2.0), "worker should be wedged inside emit"

        start = time.monotonic()
        result = w.flush(timeout=0.1)  # must not raise
        elapsed = time.monotonic() - start

        assert not result, "a drain that could not complete reports a falsy result"
        assert result.reason == "timed-out", (
            "a slow destination and a retired worker need different fixes, so they need "
            "different reasons -- `assert not result` alone passes for all five"
        )
        assert elapsed < 1.0, f"timeout not honoured, took {elapsed:.3f}s"
    finally:
        sink.release.set()
        w.shutdown()


def test_flush_does_not_raise_when_the_sink_always_fails() -> None:
    sink = AlwaysFailSink()
    w = Worker(sink, batch_size=1000, flush_interval=100.0, max_retries=0)
    try:
        w.submit(_span("doomed"))
        # The drain ran and the failure is reported through the return value and
        # failed_batches, not by raising at the caller (SPEC-021 FR-001 changed the first of
        # those from True; see test_flush_reports_false_when_the_batch_is_abandoned).
        result = w.flush(timeout=5.0)
        assert not result
        assert result.reason == "abandoned", "the drain ran and gave up; the thread is fine"
        assert sink.events == [], "nothing was ever successfully emitted"
        assert w.failed_batches >= 1, "the failure is counted, not swallowed silently"
    finally:
        w.shutdown()


def test_flush_with_timeout_none_returns_true_on_a_healthy_worker() -> None:
    sink = RecordingSink()
    w = Worker(sink, batch_size=1000, flush_interval=100.0)
    try:
        w.submit(_span("n"))
        assert w.flush(timeout=None)
        assert [e["message"] for e in sink.events] == ["n"]
    finally:
        w.shutdown()


# -- SPEC-013 FR-003/FR-005: the public `lf.flush()` entry point -------------------------


def test_module_flush_is_a_no_op_when_nothing_was_ever_logged() -> None:
    import log_foundry

    _lifecycle._state._worker = None
    start = time.monotonic()
    assert log_foundry.flush(), "no worker means nothing to drain"
    assert time.monotonic() - start < 1.0
    # Building a worker here would start a thread and register atexit purely to flush nothing.
    assert _lifecycle._state._worker is None, "flush must not create a worker"


def test_module_flush_after_shutdown_returns_false_promptly() -> None:
    import log_foundry

    log_foundry.configure(service="t", version="0", env="t", sink=RecordingSink())
    _lifecycle._get_worker()
    log_foundry.shutdown()

    start = time.monotonic()
    assert not log_foundry.flush(timeout=5.0)
    assert time.monotonic() - start < 1.0, "must not block on a worker that is gone"


def test_module_flush_delivers_a_traced_call_and_logging_continues() -> None:
    """The Lambda pattern: drain before returning, then be invoked again on the same worker."""
    import log_foundry

    sink = RecordingSink()
    log_foundry.configure(service="t", version="0", env="t", sink=sink)
    _lifecycle._state._worker = None
    # Neither batching trigger can fire in the life of this test.
    _lifecycle._state._worker = worker_mod.Worker(sink, batch_size=1000, flush_interval=100.0)

    @log_foundry.trace
    def handler() -> str:
        log_foundry.info("invoked")
        return "ok"

    try:
        handler()
        assert log_foundry.flush(timeout=5.0)
        first = [e["message"] for e in sink.events]
        assert "invoked" in first, "the first invocation's events were drained"
        assert sink.closed == 0, "the sink is still open"

        handler()  # the failure mode this whole spec exists for: does the *second* one log?
        assert log_foundry.flush(timeout=5.0)
        assert [e["message"] for e in sink.events].count("invoked") == 2
    finally:
        _lifecycle._state._worker.shutdown()
        _lifecycle._state._worker = None


def test_flush_is_exported() -> None:
    import log_foundry

    assert "flush" in log_foundry.__all__
    assert callable(log_foundry.flush)


# -- SPEC-017 FR-005: health snapshot + audible overflow ---------------------------------


def test_health_reflects_live_counters() -> None:
    sink = BlockingSink()
    w = Worker(sink, batch_size=1, max_queue=1)
    try:
        w.submit(_span("a"))
        sink.in_emit.wait(2.0)
        w.submit(_span("b"))
        w.submit(_span("c"))  # full -> dropped

        h = w.health()
        assert h.dropped >= 1
        assert h.failed_batches == 0
        assert isinstance(h.queued, int)
    finally:
        sink.release.set()
        w.shutdown()


def test_health_counts_failed_batches() -> None:
    class AlwaysFail(RecordingSink):
        def emit(self, batch):
            raise RuntimeError("down")

    w = Worker(AlwaysFail(), batch_size=1, max_retries=1)
    try:
        w.submit(_span("a"))
        w.flush(timeout=5.0)
        assert w.health().failed_batches == 1
    finally:
        w.shutdown()


def test_health_is_readable_after_shutdown() -> None:
    sink = RecordingSink()
    w = Worker(sink, batch_size=1)
    w.submit(_span("a"))
    w.shutdown()

    h = w.health()  # must not raise
    assert h.queued == 0, "the final drain consumed the queue, markers included"
    assert h.dropped == 0 and h.failed_batches == 0


def test_module_health_returns_zeros_without_creating_a_worker() -> None:
    """Asking after the health of a process that never logged must not start a thread."""
    import threading

    import log_foundry

    _lifecycle._state._worker = None
    before = threading.active_count()

    h = log_foundry.health()

    # Compared field-wise, not as a whole tuple: SPEC-019 appended `stopped_reason`, and the
    # advertised way to read a snapshot has always been by attribute.
    assert (h.queued, h.dropped, h.failed_batches) == (0, 0, 0)
    assert h.stopped_reason is None
    assert _lifecycle._state._worker is None, "health() must not create a worker"
    assert threading.active_count() == before, "health() must not start a thread"


def test_health_is_exported() -> None:
    import log_foundry

    assert "health" in log_foundry.__all__
    assert "Health" in log_foundry.__all__


def test_first_overflow_drop_warns_once(capsys) -> None:
    sink = BlockingSink()
    w = Worker(sink, batch_size=1, max_queue=1)
    try:
        w.submit(_span("a"))
        sink.in_emit.wait(2.0)
        w.submit(_span("b"))
        w.submit(_span("c"))  # first drop -> one line
        w.submit(_span("d"))  # second drop -> silent (throttled)

        err = capsys.readouterr().err
        assert err.count("log queue full") == 1
        assert "log-foundry:" in err
        assert "lost 1 submission(s)" in err
    finally:
        sink.release.set()
        w.shutdown()


def test_overflow_warning_is_throttled(capsys) -> None:
    """2,500 drops must produce exactly 3 lines (drops 1, 1000, 2000), not 2,500."""
    sink = BlockingSink()
    w = Worker(sink, batch_size=1, max_queue=1)
    try:
        w.submit(_span("a"))
        sink.in_emit.wait(2.0)
        w.submit(_span("filler"))  # occupies the single queue slot
        for i in range(2500):
            w.submit(_span(i))  # all dropped

        err = capsys.readouterr().err
        assert w.dropped == 2500
        assert err.count("log queue full") == 3
    finally:
        sink.release.set()
        w.shutdown()


def test_overflow_warning_never_raises_into_the_caller(monkeypatch) -> None:
    """submit() runs on the *caller's* thread — an unwritable stderr must not reach the app.

    Regression: the FR-005 warning was added unguarded, which reintroduced exactly the
    raise-into-the-caller failure FR-001 exists to remove.
    """
    import io

    class BrokenStderr(io.TextIOBase):
        def write(self, s: str) -> int:
            raise ValueError("I/O operation on closed file")

    sink = BlockingSink()
    w = Worker(sink, batch_size=1, max_queue=1)
    try:
        w.submit(_span("a"))
        sink.in_emit.wait(2.0)
        w.submit(_span("b"))  # fills the queue

        monkeypatch.setattr("sys.stderr", BrokenStderr())
        w.submit(_span("c"))  # full -> first drop -> warns -> stderr raises internally

        assert w.dropped == 1, "the drop is still counted even when the warning cannot be written"
    finally:
        sink.release.set()
        w.shutdown()


# -- SPEC-019: the drain thread's terminal-failure path ---------------------------------


class TerminalSink(RecordingSink):
    """A sink whose ``emit`` raises past ``_emit``'s ``except Exception`` and ends the thread."""

    def __init__(self, exc: BaseException) -> None:
        super().__init__()
        self._exc = exc

    def emit(self, batch: list[dict]) -> None:
        raise self._exc


def _dead(w) -> bool:
    return w.health().stopped_reason is not None


def test_system_exit_from_sink_is_recorded_and_announced(capsys) -> None:
    """The motivating case: CPython's thread bootstrap discards SystemExit without a trace."""
    w = Worker(TerminalSink(SystemExit(1)), batch_size=1)
    w.submit(_span("a"))
    assert _wait_until(lambda: _dead(w)), "the terminal failure should be recorded"
    assert w.health().stopped_reason == "SystemExit"
    assert _wait_until(lambda: not w._thread.is_alive()), "the thread must not keep draining"
    err = capsys.readouterr().err
    assert err.count("\n") == 1
    assert err.startswith(
        "log-foundry: absorbed a failure while draining the log queue (SystemExit)"
    )
    assert "worker thread stopped" in err
    assert "1 undrained event-list(s) held and 0 queued item(s) undelivered" in err
    assert "nothing further will be delivered" in err


def test_keyboard_interrupt_from_sink_is_recorded() -> None:
    w = Worker(TerminalSink(KeyboardInterrupt()), batch_size=1)
    w.submit(_span("a"))
    assert _wait_until(lambda: _dead(w))
    assert w.health().stopped_reason == "KeyboardInterrupt"


def test_a_bare_base_exception_subclass_is_recorded_by_type_name() -> None:
    class Detonation(BaseException):
        pass

    w = Worker(TerminalSink(Detonation("boom")), batch_size=1)
    w.submit(_span("a"))
    assert _wait_until(lambda: _dead(w))
    assert w.health().stopped_reason == "Detonation"


def test_the_exception_message_is_never_reported(capsys) -> None:
    """arch §6: a sink's exception text can carry event data, so only the type is reported."""
    w = Worker(TerminalSink(SystemExit("secret-token-abc123")), batch_size=1)
    w.submit(_span("a"))
    # Waits on the thread *exiting*, not on the record: the record is stored before the stderr
    # write, so polling it would let this assertion run against an stderr not yet written to and
    # pass vacuously. The thread cannot exit until the write has been attempted.
    assert _wait_until(lambda: not w._thread.is_alive())
    assert w.health().stopped_reason == "SystemExit"
    assert "secret-token-abc123" not in capsys.readouterr().err


def test_an_ordinary_exception_does_not_set_stopped_reason() -> None:
    """The non-terminal path is untouched: retried, counted, and the thread keeps running."""
    sink = AlwaysFailSink()
    w = Worker(sink, batch_size=1, max_retries=1)
    w.submit(_span("a"))
    assert _wait_until(lambda: w.health().failed_batches == 1)
    h = w.health()
    assert h.stopped_reason is None
    assert w._thread.is_alive(), "an Exception is absorbed by _emit; the worker survives it"
    w.submit(_span("b"))
    assert _wait_until(lambda: sink.attempts >= 4), "still draining after the abandoned batch"
    w.shutdown()


def test_callers_are_unaffected_after_the_worker_dies() -> None:
    """No path into the app raises once the thread is gone (architecture §4)."""
    w = Worker(TerminalSink(SystemExit(1)), batch_size=1)
    w.submit(_span("a"))
    assert _wait_until(lambda: _dead(w))
    w.submit(_span("b"))  # queued, never drained — must not raise
    assert not w.flush(timeout=0.2), "flush reports failure rather than burning its timeout"
    w.shutdown()  # joins an already-dead thread and closes the sink
    w.shutdown()  # still idempotent


def test_a_decorated_function_is_unaffected_after_the_worker_dies() -> None:
    """The clause of FR-001 that reaches user code: @trace still returns normally (arch §4)."""
    import log_foundry

    log_foundry.configure(service="t", sink=TerminalSink(SystemExit(1)))

    @log_foundry.trace
    def work() -> str:
        return "ok"

    assert work() == "ok"
    log_foundry.flush(timeout=2.0)  # force the drain that kills the thread
    w = _lifecycle._state._worker
    assert w is not None
    assert _wait_until(lambda: not w._thread.is_alive())

    assert work() == "ok", "a dead worker must not change what a decorated function returns"
    assert log_foundry.health().stopped_reason == "SystemExit"


def test_stopped_reason_survives_shutdown() -> None:
    w = Worker(TerminalSink(SystemExit(1)), batch_size=1)
    w.submit(_span("a"))
    assert _wait_until(lambda: _dead(w))
    w.shutdown()
    assert w.health().stopped_reason == "SystemExit", "shutdown must not clear the diagnosis"


def test_a_clean_run_reports_no_terminal_failure(capsys) -> None:
    sink = RecordingSink()
    w = Worker(sink, batch_size=1)
    w.submit(_span("a"))
    w.shutdown()
    h = w.health()
    assert h.stopped_reason is None
    assert (h.queued, h.dropped, h.failed_batches) == (0, 0, 0)
    assert capsys.readouterr().err == ""


def test_a_process_that_never_logged_reports_no_terminal_failure() -> None:
    """The trap that ruled out an ``alive`` flag: no worker exists, and none has died."""
    import log_foundry

    h = log_foundry.health()
    assert h.stopped_reason is None
    assert (h.queued, h.dropped, h.failed_batches) == (0, 0, 0)


def test_health_is_read_by_attribute_and_no_longer_by_position() -> None:
    """Was `test_existing_health_fields_keep_their_positions` (SPEC-034 FR-008 AC-2b).

    Its whole body was positional — `h[0]`, `h[3]`, `h[4]`, `h[5..7]`, `h[8]`, `len(h) == 9` —
    which is precisely the contract FR-008 refuses to freeze at 1.0. Six specs appended a field
    apiece, and each one had to argue that the indices before it were undisturbed; with a
    dataclass there are no indices to disturb, and two more fields are due immediately
    (SPEC-036, SPEC-037).

    So the test inverts rather than being deleted: it now asserts that every field is reachable
    by name **and** that the tuple protocol is gone, which is the breaking half of AC-2 and the
    thing a caller's `d, f = ...` would otherwise keep working against by accident.
    """
    w = Worker(RecordingSink(), batch_size=1)
    w.submit(_span("a"))
    w.shutdown()
    h = w.health()

    # Values, not types: `X == X` and `isinstance(x, int)` are true of anything the annotation
    # already promises, and the first version of this test asserted exactly that -- residue of
    # mechanically rewriting `(h[0], h[1], h[2]) == (h.queued, ...)` to match its own right side.
    assert (h.queued, h.dropped, h.failed_batches) == (0, 0, 0)
    assert h.stopped_reason is None, "a clean shutdown is not a failure (SPEC-019)"
    assert h.sink is None, "RecordingSink reports no losses"
    assert h.retired is True, "shutdown() was called two lines above"
    assert (h.submitted_after_shutdown, h.incomplete_swaps, h.closing_sinks) == (0, 0, 0)

    with pytest.raises(TypeError):
        _ = h[0]  # type: ignore[index]
    with pytest.raises(TypeError):
        len(h)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        _a, _b = h  # type: ignore[misc]


def test_the_record_survives_an_unwritable_stderr(monkeypatch) -> None:
    """The line is written once and cannot be re-emitted, so the record must not ride on it."""
    import io

    class BrokenStderr(io.TextIOBase):
        def write(self, s: str) -> int:
            raise ValueError("I/O operation on closed file")

    monkeypatch.setattr("sys.stderr", BrokenStderr())
    w = Worker(TerminalSink(SystemExit(1)), batch_size=1)
    w.submit(_span("a"))
    # Thread exit, not the record: polling the record could return before the write is attempted,
    # leaving the failing write to land on the real stderr after monkeypatch teardown.
    assert _wait_until(lambda: not w._thread.is_alive())
    assert w.health().stopped_reason == "SystemExit", "recording precedes announcing"


# -- SPEC-021 FR-001: flush() reports whether the drain it forced was delivered ----------


def test_flush_reports_true_when_the_events_reach_the_sink() -> None:
    """The `True` contract, stated against sink receipt rather than against the drain running."""
    sink = RecordingSink()
    w = Worker(sink, batch_size=1000, flush_interval=100.0)
    try:
        w.submit(_span("landed"))
        assert w.flush(timeout=5.0)
        assert [e["message"] for e in sink.events] == ["landed"], "True means these landed"
        assert w.failed_batches == 0
    finally:
        w.shutdown()


def test_flush_reports_false_when_the_batch_is_abandoned() -> None:
    """The false success SPEC-021 removes: the drain ran, the events died with the retries."""
    sink = AlwaysFailSink()
    w = Worker(sink, batch_size=1000, flush_interval=100.0, max_retries=0)
    try:
        w.submit(_span("doomed"))
        abandoned = w.flush(timeout=5.0)
        assert not abandoned, "a drain that delivered nothing is not a success"
        assert abandoned.reason == "abandoned"
        assert w.failed_batches == 1, "and the failure it reports is the abandoned batch"
        assert sink.events == []
    finally:
        w.shutdown()


def test_a_failing_flush_returns_promptly_rather_than_at_the_timeout() -> None:
    """`False` must come from the answered marker, not from the caller waiting out `timeout`."""
    sink = AlwaysFailSink()
    # A real retry budget, so the bound below is load-bearing: three attempts plus backoff take
    # a measurable but bounded time, and anything that strands the waiter blows a 30s timeout.
    w = Worker(sink, batch_size=1000, flush_interval=100.0, max_retries=2)
    try:
        w.submit(_span("doomed"))
        start = time.monotonic()
        assert not w.flush(timeout=30.0)
        elapsed = time.monotonic() - start
        assert sink.attempts == 3, "the retry budget really was spent inside the flush"
        assert elapsed < 5.0, f"the waiter was stranded, took {elapsed:.3f}s"
    finally:
        w.shutdown()


def test_flush_reports_true_when_there_was_nothing_pending() -> None:
    """An empty drain is a successful one — a quiet process must not read as a failing one."""
    sink = RecordingSink()
    w = Worker(sink, batch_size=1000, flush_interval=100.0)
    try:
        assert w.flush(timeout=5.0), "nothing to deliver is not a delivery failure"
        assert sink.events == []
        # And again after a successful flush has already drained everything.
        w.submit(_span("a"))
        assert w.flush(timeout=5.0)
        assert w.flush(timeout=5.0), "the second flush has nothing left to do"
    finally:
        w.shutdown()


def test_flush_reports_true_when_the_sink_recovers_mid_retry() -> None:
    """Retries are part of the delivery, not a failure of it: the events did reach the sink."""
    sink = FlakySink(fail_times=2)
    w = Worker(sink, batch_size=1000, flush_interval=100.0, max_retries=3)
    try:
        w.submit(_span("eventually"))
        assert w.flush(timeout=5.0)
        assert [e["message"] for e in sink.events] == ["eventually"]
        assert sink.attempts == 3, "two failures then the delivery"
        assert w.failed_batches == 0, "a recovered batch is not an abandoned one"
    finally:
        w.shutdown()


def test_flush_reports_false_on_a_dead_worker() -> None:
    """Unchanged by this spec, and now the *same* answer as a drain that delivered nothing."""
    w = Worker(TerminalSink(SystemExit(1)), batch_size=1)
    try:
        w.submit(_span("a"))
        assert _wait_until(lambda: not w._thread.is_alive())
        dead = w.flush(timeout=5.0)
        assert not dead
        assert dead.reason == "thread-died"
    finally:
        w.shutdown()


def test_flush_racing_shutdown_reports_the_final_drains_outcome() -> None:
    """The `_final_drain` copy of the rule: answered from the tail emit, with its outcome.

    The mirror of ``test_shutdown_answers_a_marker_left_in_the_queue``, which pins the `True`
    side of the same path.
    """

    class BlockingThenFailingSink(BlockingSink):
        def emit(self, batch: list[dict]) -> None:
            self.in_emit.set()
            self.release.wait()
            raise RuntimeError("sink is down")

    sink = BlockingThenFailingSink()
    w = Worker(sink, batch_size=1, flush_interval=100.0, max_retries=0)
    flushed: list[bool] = []

    w.submit(_span("a"))  # pulled by the thread → emit blocks, holding the worker
    assert sink.in_emit.wait(2.0), "worker should have entered emit"
    w.submit(_span("b"))  # queues up behind the blocked worker

    flusher = threading.Thread(target=lambda: flushed.append(w.flush(timeout=5.0)))
    flusher.start()
    stopper = threading.Thread(target=w.shutdown)
    try:
        assert _wait_until(lambda: w._queue.qsize() >= 2), "marker should be queued behind 'b'"
        stopper.start()
        assert _wait_until(w._stop.is_set), "shutdown should have signalled the stop"
    finally:
        sink.release.set()

    stopper.join(5.0)
    flusher.join(5.0)

    assert [bool(r) for r in flushed] == [False], "the final drain's emit failed, so the flush it answered failed"
    assert sink.events == []


def test_shutdown_still_returns_nothing_and_still_drains() -> None:
    """FR-001 changes what `flush()` reports; `shutdown()` deliberately reports nothing."""
    sink = RecordingSink()
    w = Worker(sink, batch_size=1000, flush_interval=100.0)
    w.submit(_span("tail"))
    assert w.shutdown() is None, "shutdown must not grow a return value"
    assert [e["message"] for e in sink.events] == ["tail"], "and it still drains"
    assert sink.closed == 1, "and still closes the sink"


class BlockingFailSink(RecordingSink):
    """Blocks inside ``emit`` until released, then fails — so a test can queue markers behind an
    emit that is *known* to be in flight, instead of racing the worker for the ordering."""

    def __init__(self) -> None:
        super().__init__()
        self.in_emit = threading.Event()
        self.release = threading.Event()

    def emit(self, batch: list[dict]) -> None:
        self.in_emit.set()
        self.release.wait()
        raise RuntimeError("sink down")


def test_every_flush_outstanding_over_a_lost_batch_reports_the_loss() -> None:
    """All four flushes cover the same events and are queued before the emit fails. Answering
    each from its *own* emit told the three that found `pending` already cleared that all was
    well — the same false success, one interleaving further out."""
    sink = BlockingFailSink()
    w = Worker(sink, batch_size=1, flush_interval=100.0, max_retries=0)
    results: list[bool] = []
    lock = threading.Lock()

    def _flush() -> None:
        ok = w.flush(timeout=5.0)
        with lock:
            results.append(ok)

    try:
        w.submit(_span("doomed"))
        assert sink.in_emit.wait(2.0), "the emit must be in flight before the flushes are called"
        threads = [threading.Thread(target=_flush) for _ in range(4)]
        for t in threads:
            t.start()
        # Every marker is queued while the doomed emit is still running, so no flush can be
        # called after the abandonment and legitimately report success.
        assert _wait_until(lambda: w._queue.qsize() >= 4), "all four markers queued"
        sink.release.set()
        for t in threads:
            t.join(5.0)

        assert [bool(r) for r in results] == [False] * 4, f"every flush must report the loss, got {results}"
        assert w.failed_batches == 1, "one batch, one abandonment — reported to all four"
        assert sink.events == []
    finally:
        sink.release.set()
        w.shutdown()


def test_a_flush_called_after_the_abandonment_reports_success() -> None:
    """The boundary of the rule above, pinned rather than avoided.

    A flush *outstanding* when a batch is abandoned reports the loss; one called afterwards has
    an empty window and reports success. Two concurrent flushes therefore need not agree — which
    is not a race in the mechanism but the same trade as
    `test_a_loss_that_predates_the_call_does_not_stick_to_later_flushes`, seen from the other
    side. `health().failed_batches` is what reports the loss to a caller who asked too late.
    """
    sink = BlockingFailSink()
    w = Worker(sink, batch_size=1, flush_interval=100.0, max_retries=0)
    try:
        w.submit(_span("doomed"))
        assert sink.in_emit.wait(2.0)
        sink.release.set()
        assert _wait_until(lambda: w.failed_batches == 1), "the abandonment must complete first"

        assert w.flush(timeout=5.0), "nothing pending, and nothing lost since the call"
        assert w.health().failed_batches == 1, "the loss is reported by health(), not by flush()"
    finally:
        w.shutdown()


def test_a_loss_that_predates_the_call_does_not_stick_to_later_flushes() -> None:
    """The other side of the same rule. A flush reports on what was lost while it was
    outstanding; a batch abandoned before it was called is `health().failed_batches`' business.
    Answering from a running "has anything ever failed" flag would make every later empty flush
    report a failure it did not incur — and an empty drain is a successful one."""
    sink = FlakySink(fail_times=1)
    w = Worker(sink, batch_size=1, flush_interval=100.0, max_retries=0)
    try:
        w.submit(_span("lost"))
        assert _wait_until(lambda: w.failed_batches == 1), "the trigger should abandon it"

        assert w.flush(timeout=5.0), "nothing pending, and nothing lost since"
        assert w.failed_batches == 1, "the loss is still on the record where it belongs"

        w.submit(_span("kept"))  # the sink has recovered by now
        assert w.flush(timeout=5.0), "a healthy drain must not inherit the old failure"
        assert [e["message"] for e in sink.events] == ["kept"]
        assert w.flush(timeout=5.0), "and it does not come back on the next empty flush"
    finally:
        w.shutdown()


def test_flush_reports_false_when_the_queue_is_too_full_for_the_marker() -> None:
    """The fourth pre-existing `False` path, which had no test of its own."""
    sink = BlockingSink()
    w = Worker(sink, batch_size=1, flush_interval=100.0, max_queue=1)
    try:
        w.submit(_span("a"))  # pulled by the thread → emit blocks, holding the worker
        assert sink.in_emit.wait(2.0), "worker should have entered emit"
        w.submit(_span("b"))  # fills the queue (max_queue=1)

        start = time.monotonic()
        full = w.flush(timeout=0.1)
        assert not full, "the marker never got in, so nothing was drained"
        assert full.reason == "queue-full"
        assert time.monotonic() - start < 1.0, "and the put timeout bounds the wait"
    finally:
        sink.release.set()
        w.shutdown()


def test_a_flush_answered_by_a_dying_final_drain_is_not_stranded() -> None:
    """A BaseException from the final emit must release the waiter, not hold it to `timeout`."""

    class BlockingTerminalSink(RecordingSink):
        def __init__(self) -> None:
            super().__init__()
            self.in_emit = threading.Event()
            self.release = threading.Event()

        def emit(self, batch: list[dict]) -> None:
            self.in_emit.set()
            self.release.wait()
            raise SystemExit(1)

    sink = BlockingTerminalSink()
    w = Worker(sink, batch_size=1, flush_interval=100.0)
    flushed: list[tuple[bool, float]] = []

    def _flush() -> None:
        start = time.monotonic()
        ok = w.flush(timeout=10.0)
        flushed.append((ok, time.monotonic() - start))

    w.submit(_span("a"))
    assert sink.in_emit.wait(2.0), "worker should have entered emit"
    w.submit(_span("b"))
    flusher = threading.Thread(target=_flush)
    flusher.start()
    stopper = threading.Thread(target=w.shutdown)
    try:
        assert _wait_until(lambda: w._queue.qsize() >= 2), "marker should be queued behind 'b'"
        stopper.start()
        assert _wait_until(w._stop.is_set), "shutdown should have signalled the stop"
    finally:
        sink.release.set()

    stopper.join(10.0)
    flusher.join(10.0)

    assert len(flushed) == 1, "the waiter was never released"
    ok, elapsed = flushed[0]
    assert not ok, "the batch died with the thread; that is not a delivery"
    assert elapsed < 5.0, f"released promptly, not at the timeout — took {elapsed:.3f}s"


# -- SPEC-021 FR-002: the terminal-failure line accounts for everything undelivered ------


class BlockingTerminalSink(RecordingSink):
    """Blocks inside ``emit`` until released, then ends the thread — so the test can queue
    work behind the dying worker deterministically rather than racing it."""

    def __init__(self, exc: BaseException) -> None:
        super().__init__()
        self._exc = exc
        self.in_emit = threading.Event()
        self.release = threading.Event()

    def emit(self, batch: list[dict]) -> None:
        self.in_emit.set()
        self.release.wait()
        raise self._exc


def test_the_line_reports_both_what_was_held_and_what_was_queued(capsys) -> None:
    """Held alone under-reads the loss: nothing will drain the queue either."""
    sink = BlockingTerminalSink(SystemExit("tok3n-not-in-the-line"))
    w = Worker(sink, batch_size=1, flush_interval=100.0)
    w.submit(_span("held"))  # pulled by the thread → emit blocks with this in `pending`
    assert sink.in_emit.wait(2.0), "worker should have entered emit"
    for i in range(3):
        w.submit(_span(i))  # queued behind the blocked worker
    assert _wait_until(lambda: w._queue.qsize() == 3)
    sink.release.set()

    assert _wait_until(lambda: not w._thread.is_alive())
    err = capsys.readouterr().err
    assert err.count("\n") == 1, "still exactly one line"
    assert "(SystemExit)" in err, "type name only"
    assert "SystemExit(" not in err, "the type, not the repr"
    assert "1 undrained event-list(s) held and 3 queued item(s) undelivered" in err
    assert "tok3n" not in err, "still never the exception's message"


def test_the_line_is_written_even_when_the_queue_size_is_unavailable(capsys) -> None:
    """`qsize()` is not guaranteed on every platform's queue; the diagnosis must not ride on it."""
    w = Worker(TerminalSink(SystemExit(1)), batch_size=1)

    def _unavailable() -> int:
        raise NotImplementedError("qsize is not supported here")

    w._queue.qsize = _unavailable  # type: ignore[method-assign]
    w.submit(_span("a"))

    assert _wait_until(lambda: not w._thread.is_alive())
    assert w.stopped_reason == "SystemExit", "the record is still set, and set first"
    err = capsys.readouterr().err
    assert err.count("\n") == 1
    assert "1 undrained event-list(s) held and ? queued item(s) undelivered" in err
    assert "nothing further will be delivered" in err


# -- SPEC-025 FR-004: shutdown() is total, and a failed close is announced ----------------


class _CloseFailsSink:
    """Emits fine, fails to close — a socket already reset, a client mid-teardown.

    ``emit`` is deliberately slow and ``close`` records how much had been emitted by the time it
    ran: asserting on ``batches`` *after* ``shutdown()`` returns cannot tell "close after drain"
    from "close before drain", since both end with everything emitted.
    """

    def __init__(self, exc: BaseException | None = None, emit_delay: float = 0.0) -> None:
        self.exc = exc or OSError("cannot close")
        self.emit_delay = emit_delay
        self.batches: list[list[dict]] = []
        self.close_calls = 0
        self.emitted_at_close = -1

    def emit(self, batch: list[dict]) -> None:
        time.sleep(self.emit_delay)
        self.batches.append(list(batch))

    def close(self) -> None:
        self.close_calls += 1
        self.emitted_at_close = sum(len(b) for b in self.batches)
        raise self.exc


def test_shutdown_returns_normally_when_the_sink_cannot_close(capsys) -> None:
    sink = _CloseFailsSink()
    worker = Worker(sink, batch_size=10, flush_interval=60.0)
    worker.submit(_span("a"))

    worker.shutdown()  # must not raise

    err = capsys.readouterr().err
    assert "absorbed a failure while closing the sink (OSError)" in err
    assert "it may still hold its resources" in err, "the consequence is the point of the line"
    assert "cannot close" not in err, "the message is never written (arch §6)"
    assert err.count("\n") == 1, "one line, as SPEC-019's own stderr tests assert"


def test_shutdown_drains_and_emits_before_attempting_the_close() -> None:
    """The close runs after the join, so a failure there loses cleanup, not events.

    Closing a sink while its drain thread is still running would mean `emit()` on a closed sink,
    so this asserts what had been emitted *at close time*, not after `shutdown()` returned.
    """
    sink = _CloseFailsSink(emit_delay=0.05)
    worker = Worker(sink, batch_size=10, flush_interval=60.0)
    worker.submit(_span("a"))
    worker.submit(_span("b"))

    worker.shutdown()

    assert [e["message"] for batch in sink.batches for e in batch] == ["a", "b"]
    assert sink.emitted_at_close == 2, "the drain had completed before the close was attempted"
    assert sink.close_calls == 1


def test_a_second_shutdown_is_still_a_no_op_and_still_does_not_raise() -> None:
    """The once-only flag stays ahead of the close: a second close() on a sink that partially
    released its resources is worse than leaving it unclosed."""
    sink = _CloseFailsSink()
    worker = Worker(sink, batch_size=10, flush_interval=60.0)
    worker.submit(_span("a"))

    worker.shutdown()
    worker.shutdown()  # must not raise, and must not retry the close

    assert sink.close_calls == 1


def test_health_stays_readable_after_a_failed_close() -> None:
    sink = _CloseFailsSink()
    worker = Worker(sink, batch_size=10, flush_interval=60.0)
    worker.submit(_span("a"))
    worker.shutdown()

    health = worker.health()
    assert health.queued == 0
    assert health.dropped == 0
    assert health.failed_batches == 0, "the events were delivered; only the close failed"
    assert health.stopped_reason is None, "a failed close is not a dead thread (SPEC-019)"


def test_a_keyboardinterrupt_from_close_still_propagates() -> None:
    """FR-004 draws the same line as FR-001 and FR-003: Exception, never BaseException.

    The second call also pins where the once-only flag sits. It is set *before* the close, so
    even an escape leaves `shutdown()` spent: moved after the close, this path would retry and
    call `close()` twice on a sink that may already have released some of its resources.
    """
    sink = _CloseFailsSink(KeyboardInterrupt())
    worker = Worker(sink, batch_size=10, flush_interval=60.0)
    worker.submit(_span("a"))

    with pytest.raises(KeyboardInterrupt):
        worker.shutdown()

    try:
        worker.shutdown()  # spent, not retryable — and still silent
    except KeyboardInterrupt:
        # Caught rather than allowed to propagate: an escaping KeyboardInterrupt aborts the
        # whole pytest session instead of failing this test, which reads in CI as an
        # interrupted run rather than a regression.
        pytest.fail("shutdown() retried a close that had already failed")
    assert sink.close_calls == 1


def test_the_public_shutdown_is_total_too(monkeypatch) -> None:
    """FR-004's criterion names `log_foundry.shutdown()`, whose delegate is deliberately
    unguarded — it relies entirely on `Worker.shutdown()` being total."""
    import log_foundry
    sink = _CloseFailsSink()
    log_foundry.configure(service="t", sink=sink)

    @log_foundry.trace(name="work")
    def work() -> int:
        return 42

    work()
    log_foundry.shutdown()  # must not raise
    log_foundry.shutdown()  # idempotent, still silent

    assert sink.close_calls == 1
    assert [e["message"] for batch in sink.batches for e in batch] == ["span.start", "span.end"]
    monkeypatch.setattr(_lifecycle._state, "_worker", None)


_ATEXIT_PROGRAM = """
import sys
import log_foundry as lf

class CloseFailsSink:
    def emit(self, batch): pass
    def close(self): raise OSError("cannot close")

lf.configure(service="t", env="t", sink=CloseFailsSink())

@lf.trace(name="work")
def work(): return 1

work()
sys.exit({code})
"""


@pytest.mark.parametrize("code", [0, 3])
def test_an_atexit_shutdown_prints_no_traceback_at_interpreter_shutdown(code, tmp_path) -> None:
    """FR-004: the failure must not surface as "Exception ignored in atexit callback".

    A real subprocess, because that is the only way to reach the interpreter-shutdown path —
    ``atexit`` handlers do not run inside pytest, and CPython's handling of an exception escaping
    one is exactly what this criterion is about.

    The exit-status half of the criterion was **already true** before the guard: CPython absorbs
    a non-``SystemExit`` exception from an ``atexit`` callback and carries on, so the status was
    never at risk (measured against the unguarded code at exit 0, exit 3 and an uncaught
    exception). It is asserted anyway to keep that fact pinned. What the guard actually changes
    is the block below it — the traceback, and with it the exception's *message*, which can carry
    a value from the event that provoked the failure (arch §6).
    """
    src = pathlib.Path(log_foundry_mod.__file__).resolve().parent.parent
    script = tmp_path / "prog.py"
    script.write_text(_ATEXIT_PROGRAM.format(code=code))

    # Suppressed here rather than by widening the tests-wide ignores: argv is this interpreter
    # plus a script this test just wrote into pytest's own tmp_path. No shell, no caller-supplied
    # input — the same reasoning `scripts/make-sbom.py` carries in pyproject.
    result = subprocess.run(  # noqa: S603
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": str(src)},
        timeout=60,
        check=False,
    )

    assert result.returncode == code, "the process keeps its own exit status"
    assert "Exception ignored in atexit callback" not in result.stderr
    assert "Traceback" not in result.stderr
    assert "cannot close" not in result.stderr, "the message is never written (arch §6)"
    assert "absorbed a failure while closing the sink (OSError)" in result.stderr


# -- SPEC-029 FR-003: a diagnostic can never be the failure ---------------------------------


class _RaisingStderr:
    """A ``sys.stderr`` whose ``write`` fails — a closed fd, a broken pipe, a daemonized process."""

    def __init__(self) -> None:
        self.calls = 0

    def write(self, text: str) -> int:
        self.calls += 1
        raise ValueError("I/O operation on closed file")

    def flush(self) -> None:
        return None


def test_a_broken_stderr_does_not_kill_the_drain_thread(monkeypatch) -> None:
    """The motivating defect: ``_emit``'s write was the one unguarded site on this thread.

    Unguarded, the ``ValueError`` rose out of ``_emit``, through ``_drain``, into ``_run``'s
    terminal handler — so announcing *one* abandoned batch cost every batch after it, and
    ``health()`` reported ``stopped_reason='ValueError'`` for a fault that had nothing to do with
    the sink.
    """
    stream = _RaisingStderr()
    monkeypatch.setattr(sys, "stderr", stream)
    sink = AlwaysFailSink()
    w = Worker(sink, batch_size=1, max_retries=0)
    try:
        w.submit(_span("a"))
        assert _wait_until(lambda: w.health().failed_batches == 1), (
            "the counter moves before the announcement, so a failed write cannot lose it"
        )
        assert stream.calls >= 1, "it did try to write"

        h = w.health()
        assert h.stopped_reason is None, "a stderr fault is not a terminal worker failure"
        assert w._thread.is_alive()

        w.submit(_span("b"))
        assert _wait_until(lambda: w.health().failed_batches == 2), "still draining"
    finally:
        w.shutdown()


def test_a_broken_stderr_still_records_the_terminal_failure(monkeypatch) -> None:
    """Record-before-announce: the line is best-effort, ``stopped_reason`` is not."""
    monkeypatch.setattr(sys, "stderr", _RaisingStderr())
    w = Worker(TerminalSink(SystemExit(1)), batch_size=1)
    w.submit(_span("a"))

    assert _wait_until(lambda: w.health().stopped_reason == "SystemExit")


def test_a_broken_stderr_does_not_reach_the_caller_on_overflow(monkeypatch) -> None:
    """``submit`` runs on the app's thread: a warning about dropped logs must not fail a call."""
    sink = BlockingSink()
    w = Worker(sink, batch_size=1, max_queue=1)
    try:
        w.submit(_span("a"))
        sink.in_emit.wait(2.0)
        w.submit(_span("b"))
        monkeypatch.setattr(sys, "stderr", _RaisingStderr())

        w.submit(_span("c"))  # first drop -> would warn, and the write raises

        assert w.health().dropped == 1
    finally:
        sink.release.set()
        w.shutdown()


class _Detonation(BaseException):
    """Not an ``Exception``: the one class of fault ``_diag`` deliberately lets through."""


class _DetonatingStderr:
    """A stream whose ``write`` raises past ``_diag``'s guard, killing the announcement mid-flight.

    A custom ``BaseException`` rather than ``KeyboardInterrupt`` on purpose — an interrupt escaping
    a worker thread reads in CI as an aborted session rather than a failed assertion.
    """

    def write(self, text: str) -> int:
        raise _Detonation("stderr detonated")

    def flush(self) -> None:
        return None


# The detonation escapes the worker thread by design — that is the fault being modelled — and
# pytest reports any such escape as a warning. Silenced per-test so it does not read as flakiness.
@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_the_batch_counter_survives_an_announcement_that_detonates(monkeypatch) -> None:
    """Record-before-announce (FR-003), tested where the ordering is actually observable.

    While every write is guarded the ordering looks free — announce-then-record still records.
    It stops being free for a ``BaseException``, which ``_diag`` passes through by design: with
    the record placed after the write, the loss the counter exists to report is never counted.
    """
    monkeypatch.setattr(sys, "stderr", _DetonatingStderr())
    w = Worker(AlwaysFailSink(), batch_size=1, max_retries=0)
    try:
        w.submit(_span("a"))
        assert _wait_until(lambda: w.health().failed_batches == 1)
    finally:
        w.shutdown()


# The detonation escapes the worker thread by design — that is the fault being modelled — and
# pytest reports any such escape as a warning. Silenced per-test so it does not read as flakiness.
@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_stopped_reason_survives_an_announcement_that_detonates(monkeypatch) -> None:
    """The same ordering on the line that is written exactly once and cannot be re-emitted."""
    monkeypatch.setattr(sys, "stderr", _DetonatingStderr())
    w = Worker(TerminalSink(SystemExit(1)), batch_size=1)
    w.submit(_span("a"))

    assert _wait_until(lambda: not w._thread.is_alive())
    assert w.health().stopped_reason == "SystemExit", (
        "recorded before the announcement that killed the announcing"
    )


# -- SPEC-030 FR-001: a retired worker still receiving submissions is visible ---------------


def test_a_clean_shutdown_reports_retired_with_a_zero_count() -> None:
    """Correct usage: shut down, then stop logging. Retired is a state, not yet a fault."""
    w = Worker(RecordingSink(), batch_size=1)
    w.submit(_span("a"))
    w.shutdown()

    h = w.health()
    assert h.retired is True
    assert h.submitted_after_shutdown == 0


def test_submissions_after_shutdown_are_counted() -> None:
    """The measured defect: three submissions delivered nothing and every counter read zero."""
    sink = RecordingSink()
    w = Worker(sink, batch_size=1)
    w.shutdown()

    for i in range(3):
        w.submit(_span(i))

    h = w.health()
    assert h.submitted_after_shutdown == 3
    assert h.retired is True
    assert sink.events == [], "the worker is retired; nothing drains these"


def test_stopped_reason_stays_none_through_post_shutdown_submissions() -> None:
    """SPEC-019 FR-003 unchanged: a clean shutdown is not a terminal failure, ever."""
    w = Worker(RecordingSink(), batch_size=1)
    w.shutdown()
    w.submit(_span("a"))

    assert w.health().stopped_reason is None


def test_a_worker_that_was_never_shut_down_reports_false_and_zero() -> None:
    w = Worker(RecordingSink(), batch_size=1)
    try:
        w.submit(_span("a"))
        assert _wait_until(lambda: w.health().queued == 0)

        h = w.health()
        assert h.retired is False
        assert h.submitted_after_shutdown == 0
    finally:
        w.shutdown()


def test_a_process_that_never_logged_reports_the_zeroed_lifecycle_snapshot() -> None:
    """No worker means nothing was retired — and asking must not build one to say so."""
    import log_foundry

    _lifecycle._state._worker = None
    h = log_foundry.health()

    assert (h.retired, h.submitted_after_shutdown, h.incomplete_swaps) == (False, 0, 0)
    assert _lifecycle._state._worker is None, "health() must not create a worker"


def test_decorated_calls_after_a_module_shutdown_are_counted() -> None:
    """End to end, through the public API: the serverless mistake, made and then seen."""
    import log_foundry

    sink = RecordingSink()
    log_foundry.configure(service="t", version="0", env="t", sink=sink)

    @log_foundry.trace
    def handler() -> str:
        return "ok"

    handler()
    assert log_foundry.flush(timeout=5.0)
    delivered = len(sink.events)
    assert delivered > 0

    log_foundry.shutdown()  # the mistake: terminal, called where flush() was meant

    for _ in range(3):
        assert handler() == "ok", "the caller is never affected (SPEC-025)"

    h = log_foundry.health()
    assert h.submitted_after_shutdown == 3
    assert h.retired is True
    assert h.stopped_reason is None, "a clean shutdown is not a terminal failure"
    assert len(sink.events) == delivered, "nothing more reached the sink"
    # The documented alert idiom must now fire on a state that used to read entirely healthy.
    assert h.retired and h.submitted_after_shutdown


# -- SPEC-030 FR-002: the first post-shutdown submission warns ------------------------------


def test_the_first_post_shutdown_submission_warns_once(capsys) -> None:
    w = Worker(RecordingSink(), batch_size=1)
    w.shutdown()
    capsys.readouterr()  # discard anything shutdown itself wrote

    w.submit(_span("a"))
    w.submit(_span("b"))  # throttled -> silent

    err = capsys.readouterr().err
    assert err.count("logged after shutdown()") == 1
    assert "log-foundry:" in err
    assert "lost 1 submission(s)" in err
    assert "flush()" in err, "the line must name what to use instead"


def test_the_post_shutdown_warning_is_throttled(capsys) -> None:
    """2,500 submissions must produce exactly 3 lines (1, 1000, 2000), as overflow does."""
    w = Worker(RecordingSink(), batch_size=1, max_queue=10_000)
    w.shutdown()
    capsys.readouterr()

    for i in range(2500):
        w.submit(_span(i))

    err = capsys.readouterr().err
    assert w.health().submitted_after_shutdown == 2500
    assert err.count("logged after shutdown()") == 3


def test_a_shutdown_with_no_later_logging_writes_nothing(capsys) -> None:
    w = Worker(RecordingSink(), batch_size=1)
    w.submit(_span("a"))
    w.shutdown()

    assert capsys.readouterr().err == "", "correct usage must stay silent"


def test_the_post_shutdown_warning_never_reaches_the_caller(monkeypatch) -> None:
    """submit() runs on the caller's thread; SPEC-029 FR-003 applies to this line too."""
    import io

    class BrokenStderr(io.TextIOBase):
        def write(self, s: str) -> int:
            raise ValueError("I/O operation on closed file")

    w = Worker(RecordingSink(), batch_size=1)
    w.shutdown()
    monkeypatch.setattr("sys.stderr", BrokenStderr())

    w.submit(_span("a"))  # warns -> stderr raises internally -> must not escape

    assert w.health().submitted_after_shutdown == 1, "recording precedes announcing"


def test_the_post_shutdown_counter_survives_an_announcement_that_detonates(monkeypatch) -> None:
    """Record-before-announce, tested where the ordering is observable (SPEC-029 FR-003).

    While every write is guarded the ordering looks free — announce-then-record still records.
    It stops being free for a ``BaseException``, which ``_diag`` passes through by design: with
    the record placed after the write, the loss the counter exists to report is never counted,
    and here the exception reaches the application's own thread on its way out.
    """
    w = Worker(RecordingSink(), batch_size=1)
    w.shutdown()
    monkeypatch.setattr(sys, "stderr", _DetonatingStderr())

    with pytest.raises(_Detonation):
        w.submit(_span("a"))

    assert w.health().submitted_after_shutdown == 1, "recorded before the announcement"


def test_a_live_worker_pays_nothing_for_the_check(capsys) -> None:
    """The normal path must not warn, count, or otherwise notice the flag."""
    sink = RecordingSink()
    w = Worker(sink, batch_size=1)
    try:
        for i in range(50):
            w.submit(_span(i))
        assert _wait_until(lambda: len(sink.events) == 50)

        assert w.health().submitted_after_shutdown == 0
        assert capsys.readouterr().err == ""
    finally:
        w.shutdown()


# -- SPEC-030 follow-up: a hung swapped-out close must not hijack interpreter shutdown ------


_HUNG_SWAP_CLOSE_PROGRAM = """
import faulthandler
import threading
import log_foundry as lf
from log_foundry import _lifecycle, worker as _worker

# A wedged child would otherwise fail as a bare 60s subprocess timeout with no stdout and no
# clue where it stopped -- the exact symptom the non-daemon design produces, so the failure
# would be indistinguishable from the regression this test exists to catch.
faulthandler.dump_traceback_later(20, exit=True)

# The swap budget is what this child spends waiting on the hung close, so shrink it: at the
# 5s default this test costs 5s of every CI run to prove something about the millisecond after.
_worker.DEFAULT_SWAP_TIMEOUT = 0.3

class HangingCloseSink:
    '''The sink being swapped away from. Its close() never returns.'''
    def emit(self, batch): pass
    def close(self): threading.Event().wait()

class BufferingSink:
    '''The sink swapped *in* — the live one, whose events only land on close().'''
    def __init__(self): self.buffered = 0
    def emit(self, batch): self.buffered += len(batch)
    def close(self): print(f"LIVE SINK CLOSED, delivered={self.buffered}", flush=True)

live = BufferingSink()
lf.configure(service="t", env="t", sink=HangingCloseSink())

@lf.trace(name="work")
def work(): return 1

work()
lf.flush()
lf.configure(sink=live)   # swaps; the old sink's close() hangs forever on its own thread
work()
work()
"""


def test_a_hung_swapped_out_close_does_not_stop_the_exit_drain(tmp_path) -> None:
    """The reason the closer is a daemon, measured rather than argued (review finding F1).

    CPython joins non-daemon threads *before* running ``atexit``, so a non-daemon closer stuck
    in a hung ``close()`` stops the exit drain from ever running: the live sink is never drained
    or closed, every event buffered in it is lost, and the application's own ``atexit`` handlers
    do not run either. Measured that way, the process hung until it was killed. A daemon lets
    ``atexit`` finish first, so the sink still receiving events is closed properly and only the
    fenced-out one is abandoned.

    Four events, not two: each of the two calls after the swap emits ``span.start`` and
    ``span.end``.
    """
    src = pathlib.Path(log_foundry_mod.__file__).resolve().parent.parent
    script = tmp_path / "hung_swap_close.py"
    script.write_text(_HUNG_SWAP_CLOSE_PROGRAM)

    # See the sibling atexit test for why this is suppressed: this interpreter plus a script the
    # test just wrote into pytest's own tmp_path, no shell and no caller-supplied input.
    result = subprocess.run(  # noqa: S603
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": str(src)},
        timeout=60,
        check=False,
    )

    assert result.returncode == 0, "the process must exit rather than hang on the closer"
    assert "LIVE SINK CLOSED" in result.stdout, (
        "atexit ran, so the sink still receiving events was drained and closed"
    )
    assert "delivered=4" in result.stdout, "and its buffered events reached it"


# -- SPEC-031 FR-005: _release_waiters against queue.Queue's internals -----------------------


def test_release_waiters_answers_markers_among_queued_event_lists() -> None:
    """A CPython change to ``Queue.mutex``/``Queue.queue`` must fail here, not go silent.

    ``_release_waiters`` swallows its own exceptions, so without this the symptom of a broken
    private access would be flush waiters timing out after a terminal worker failure — nothing
    raised, nothing logged. The mixed queue is the case that matters: the markers must be
    picked out of real submissions, and those submissions must survive the sweep because
    ``health().queued`` and the terminal-failure line report them as the evidence of what was
    lost (architecture.md §13).

    The worker is shut down *before* the queue is seeded, which is both the real scenario — this
    method exists for a drain thread that is gone — and the only way to make the test
    deterministic. Setting ``_stop`` is not enough and looks like it is: the thread is parked in
    ``queue.get(timeout=flush_interval)`` and only re-reads the flag after that returns, so it
    is still on the queue and consumes the first item put there. The earlier shape passed only
    because the puts fit inside one switch interval, and failed as ``deque mutated during
    iteration`` under a tightened one.
    """
    sink = RecordingSink()
    worker = Worker(sink, batch_size=1000, flush_interval=60.0)
    worker.shutdown()
    assert not worker._thread.is_alive(), "nothing may consume what this test puts on the queue"

    first = worker_mod._FlushMarker(seen_failures=0)
    second = worker_mod._FlushMarker(seen_failures=0)
    worker._queue.put_nowait(_span("a"))
    worker._queue.put_nowait(first)
    worker._queue.put_nowait(_span("b"))
    worker._queue.put_nowait(second)
    worker._queue.put_nowait(_span("c"))
    depth = worker._queue.qsize()

    worker._release_waiters()

    assert first.event.is_set(), "a marker mid-queue was not answered"
    assert second.event.is_set(), "a second marker mid-queue was not answered"
    assert first.delivered is False, "each keeps its pessimistic verdict, which is the truth"
    assert second.delivered is False
    assert worker._queue.qsize() == depth, (
        "the markers were read, not consumed — the queued event-lists are the evidence"
    )
    remaining = [item for item in worker._queue.queue if isinstance(item, list)]
    assert remaining == [_span("a"), _span("b"), _span("c")]


def test_release_waiters_is_a_no_op_with_no_markers_queued() -> None:
    """Asserted on the contents rather than ``qsize()``, which has a shutdown window.

    ``Worker.shutdown`` sets ``_stop`` and puts ``_SHUTDOWN`` non-atomically, so a drain thread
    that finishes between the two leaves the sentinel queued forever. That is a reporting
    artifact rather than loss — it is a sentinel, not an event — but it is enough to flake a
    bare ``qsize() == 0``.
    """
    sink = RecordingSink()
    worker = Worker(sink)
    worker.shutdown()

    worker._release_waiters()  # must not raise

    assert [item for item in worker._queue.queue if isinstance(item, list)] == []
    assert not [item for item in worker._queue.queue if isinstance(item, worker_mod._FlushMarker)]


# -- The shutdown sentinel cannot be stranded (flake from SPEC-025's dd10712) ----------------


def test_the_sentinel_is_queued_before_stop_is_set() -> None:
    """The ordering *is* the fix, so it is asserted directly rather than only by its effect.

    Both ways out of the drain loop — taking the sentinel, or seeing ``_stop`` — can only
    happen once the sentinel is already queued, so one of ``get`` or ``_final_drain`` must
    consume it. The reverse order left a window in which the loop read ``_stop``, exited, and
    finished its final drain first, stranding the sentinel in a queue nothing would read.
    """
    worker = Worker(RecordingSink(), batch_size=10, flush_interval=0.01)
    order: list[str] = []
    real_put, real_set = worker._queue.put_nowait, worker._stop.set

    def recording_put(item: object) -> None:
        if item is _SHUTDOWN_SENTINEL:
            order.append("put")
        real_put(item)

    def recording_set() -> None:
        order.append("set")
        real_set()

    worker._queue.put_nowait = recording_put  # type: ignore[method-assign]
    worker._stop.set = recording_set  # type: ignore[method-assign]

    worker.shutdown()

    assert order == ["put", "set"], "the sentinel must be queued before _stop is set"


def test_no_sentinel_is_queued_for_a_thread_that_is_already_gone() -> None:
    """The ordering argument assumes a running loop; a dead drain will never read a wake-up.

    Queueing one for a thread that died terminally (SPEC-019) would strand it permanently —
    the very symptom this change removes, reappearing on the one path the ordering cannot
    reach.
    """
    worker = Worker(RecordingSink(), batch_size=10, flush_interval=0.01)
    worker._stop.set()
    worker._thread.join(timeout=5)
    assert not worker._thread.is_alive()

    worker.shutdown()

    assert _SHUTDOWN_SENTINEL not in list(worker._queue.queue)
    assert worker.health().queued == 0


def test_no_sentinel_survives_a_completed_shutdown_under_load() -> None:
    """The flake itself, at the load that made it visible rather than merely occasional.

    The rate is load-dependent, which is why it read as a rare CI flake rather than a race:
    rare when idle, and repeatedly between roughly 1 in 14 and 1 in 50 with contention. Spinner
    threads are what make it reproducible, so the guard uses them too.
    """
    stop_spinning = threading.Event()

    def burn() -> None:
        while not stop_spinning.is_set():
            pass

    spinners = [threading.Thread(target=burn, daemon=True) for _ in range(4)]
    original = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    for spinner in spinners:
        spinner.start()
    try:
        stranded = 0
        stalled = 0
        for _ in range(300):
            worker = Worker(RecordingSink(), batch_size=10, flush_interval=60.0)
            worker.submit(_span("a"))
            worker.shutdown(timeout=2.0)
            if _SHUTDOWN_SENTINEL in list(worker._queue.queue):
                stranded += 1
            if worker.health().stopped_reason is not None:
                stalled += 1
    finally:
        stop_spinning.set()
        sys.setswitchinterval(original)
        for spinner in spinners:
            spinner.join(timeout=5)

    assert stranded == 0, f"{stranded}/300 shutdowns stranded the sentinel"
    assert stalled == 0, (
        f"{stalled}/300 shutdowns expired: the drain loop did not break on the sentinel, so a "
        f"thread that took it before _stop was set blocked for another flush_interval"
    )


def test_a_flush_marker_queued_after_the_final_drain_is_still_answered() -> None:
    """The sibling race the ordering cannot reach — and the one that hangs a caller.

    ``flush()`` can pass its liveness check microseconds before the thread finishes and queue a
    marker nothing will answer. Measured stranding one in 13 of 400 raced shutdowns. The caller
    then sits out its whole timeout, and ``flush(timeout=None)`` — documented as supported —
    waits forever. ``shutdown()`` answers it on the way out.
    """
    sink = RecordingSink()
    worker = Worker(sink, batch_size=10, flush_interval=0.01)
    worker._stop.set()
    worker._thread.join(timeout=5)
    assert not worker._thread.is_alive()

    marker = worker_mod._FlushMarker(seen_failures=0)
    worker._queue.put_nowait(marker)

    worker.shutdown()

    assert marker.event.is_set(), "a caller with timeout=None would otherwise wait forever"
    assert marker.delivered is False, "the drain that would have carried it is gone"


def test_an_expired_shutdown_leaves_the_sentinel_but_answers_the_waiters() -> None:
    """The sentinel stays the live thread's; the markers do not (SPEC-050 FR-001).

    ~~A thread still running may yet consume it, so the sweep must not run on that path …
    sweeping here would resolve a marker pessimistically as undelivered while the drain that is
    about to carry it is still running, turning a ``flush()`` that was going to succeed into one
    that reports failure.~~ — **superseded by SPEC-050 FR-001.** The claim was right about what
    the sweep costs and wrong about what it saves. What it costs is a pessimistic *verdict* on a
    batch the live drain still delivers, since the events are answered again by ``_final_drain``
    and reach the sink either way. What not sweeping costs is a ``flush(timeout=None)`` — which
    the API documents as supported — parked forever on a drain this call has just given up on,
    measured as an application thread still alive after ``shutdown`` returned. An unbounded hang
    is strictly worse than an actionable false negative, so the trade is taken and stated in the
    spec rather than left implicit.

    The sentinel half is unchanged and still asserted: it is the live thread's wake-up, nothing
    else consumes it, and the sweep reads markers without touching it.
    """
    sink = BlockingSink()
    worker = Worker(sink, batch_size=1, flush_interval=60.0)
    worker.submit(_span("a"))
    assert _wait_until(lambda: sink.in_emit.is_set()), "the sink must be inside emit"
    marker = worker_mod._FlushMarker(seen_failures=0)
    worker._queue.put_nowait(marker)

    worker.shutdown(timeout=0.2)  # expires; the thread is still in emit

    assert worker._thread.is_alive(), "the premise: the drain thread outlived the join"
    assert worker.health().stopped_reason == "ShutdownTimeout"
    assert _SHUTDOWN_SENTINEL in list(worker._queue.queue), (
        "the sentinel is still the live thread's to consume"
    )
    assert marker.event.is_set(), "a caller with timeout=None would otherwise wait forever"
    assert marker.delivered is False, "pessimistic: this call has given up on that drain"

    sink.release.set()
    worker._thread.join(timeout=5)
    assert sink.events == [{"message": "a"}], (
        "and the events still reach the sink — the verdict was pessimistic, not the delivery"
    )


def test_post_shutdown_submissions_are_still_counted_and_queued() -> None:
    """SPEC-030's evidence must survive everything shutdown does on the way out."""
    sink = RecordingSink()
    worker = Worker(sink, batch_size=10, flush_interval=0.01)
    worker.shutdown()

    worker.submit(_span("late"))
    worker.submit(_span("later"))

    health = worker.health()
    assert health.queued == 2
    assert health.submitted_after_shutdown == 2
    assert list(worker._queue.queue) == [_span("late"), _span("later")]


def _flush_on_watchdog(worker, timeout: float | None) -> list[bool]:
    """Run flush() on its own thread so a hang fails in five seconds instead of hanging pytest."""
    result: list[bool] = []
    caller = threading.Thread(
        target=lambda: result.append(worker.flush(timeout=timeout)), daemon=True
    )
    caller.start()
    caller.join(timeout=5)
    assert not caller.is_alive(), "flush() hung on a marker nothing will answer"
    return result


def test_an_unbounded_flush_does_not_hang_on_a_marker_nothing_will_answer() -> None:
    """The genuine strand: the pre-checks pass, then the drain finishes, then the put lands.

    The marker is queued behind a thread that has already run its final drain, and no
    `shutdown()` follows to sweep for it. A bounded caller sits out its timeout, which SPEC-021
    accepts; an unbounded one waited forever. The drain is finished *before* the put here, so
    the interleaving is exact rather than raced for.
    """
    sink = RecordingSink()
    worker = Worker(sink, batch_size=10, flush_interval=0.01)
    real_put = worker._queue.put

    def die_then_put(item: object, timeout: float | None = None) -> None:
        worker._stop.set()
        worker._thread.join(timeout=5)
        real_put(item, timeout=timeout)

    worker._queue.put = die_then_put  # type: ignore[method-assign]

    assert [bool(r) for r in _flush_on_watchdog(worker, None)] == [False], "nothing carried it, so it did not deliver"


def test_a_flush_the_drain_answered_before_exiting_still_reports_delivery() -> None:
    """The other half, and a regression this PR introduced before review caught it.

    Here the marker *is* consumed — the drain's final pass answers it with `delivered=True` —
    and only then does the thread exit. Reporting the liveness check instead of the marker made
    that read as failure: measured 200 out of 200. It is not merely cosmetic, since `swap_sink`
    reads a False as an unconfirmed drain, counts `incomplete_swaps`, leaves the previous sink
    open and writes a loss line — for a swap that completed.
    """
    sink = RecordingSink()
    worker = Worker(sink, batch_size=10, flush_interval=0.01)
    worker.submit(_span("a"))
    real_put = worker._queue.put

    def put_then_die(item: object, timeout: float | None = None) -> None:
        real_put(item, timeout=timeout)
        worker._stop.set()
        worker._thread.join(timeout=5)

    worker._queue.put = put_then_die  # type: ignore[method-assign]

    assert [bool(r) for r in _flush_on_watchdog(worker, None)] == [True], "a drain carried it and said so"
    assert sink.events == [{"message": "a"}]


def test_an_unbounded_flush_does_not_hang_after_a_terminal_drain_failure(capsys) -> None:
    """End to end: a real terminal failure, then a flush whose marker nothing will read."""
    worker = Worker(TerminalSink(SystemExit("boom")), batch_size=1, flush_interval=0.01)
    worker.submit(_span("a"))  # the sink raises past _emit's guard and ends the thread
    assert _wait_until(lambda: not worker._thread.is_alive(), timeout=5.0)

    assert [bool(r) for r in _flush_on_watchdog(worker, None)] == [False]
    capsys.readouterr()


def test_the_drain_finished_flag_is_set_before_the_threads_own_sweep() -> None:
    """The placement *is* the guarantee: a marker landing between them is answered by nobody.

    A review found the previous version pre-set the flag by hand, so it exercised the check in
    `flush()` and never the placement in `_run` — moving the `set()` after the sweep survived
    the whole suite while reintroducing a permanent `flush(timeout=None)` hang. The first
    observation is the load-bearing one: `shutdown()` sweeps a second time after the join, by
    which point the flag is set either way, and asserting on the last reading would pass under
    exactly the mutation this exists to catch.
    """
    worker = Worker(RecordingSink(), batch_size=10, flush_interval=0.01)
    observations: list[bool] = []
    real_sweep = worker._release_waiters

    def recording_sweep() -> None:
        observations.append(worker._drain_finished.is_set())
        real_sweep()

    worker._release_waiters = recording_sweep  # type: ignore[method-assign]

    worker.shutdown()

    assert observations, "the drain thread must sweep on its way out, not only shutdown()"
    assert observations[0] is True, "the flag must already be set when the thread's sweep runs"


def test_a_marker_landing_during_the_terminal_line_is_answered_by_the_threads_own_sweep(
    capsys,
) -> None:
    """`shutdown()` never runs here, so only `_run`'s own sweep can answer it.

    `_terminal_failure` writes to stderr between the drain stopping and the sweep, and a
    `flush()` can land in that window. Without a sweep on this path the caller waits forever.
    """
    worker = Worker(TerminalSink(SystemExit("boom")), batch_size=1, flush_interval=0.01)
    marker = worker_mod._FlushMarker(seen_failures=0)
    real_terminal = worker._terminal_failure

    def terminal_then_queue_a_marker(exc: BaseException, undrained: int) -> None:
        real_terminal(exc, undrained)
        worker._queue.put_nowait(marker)

    worker._terminal_failure = terminal_then_queue_a_marker  # type: ignore[method-assign]
    worker.submit(_span("a"))

    assert _wait_until(lambda: not worker._thread.is_alive(), timeout=5.0)
    assert marker.event.is_set(), "nothing else will ever read it"
    capsys.readouterr()


def test_no_sentinel_is_queued_while_the_terminal_line_is_being_written(capsys) -> None:
    """The thread is alive throughout `_terminal_failure`, so `is_alive()` is the wrong gate.

    stderr can block on a slow reader, making that window arbitrarily long — and a sentinel
    queued in it is read by nobody, which is the symptom this change exists to remove.
    """
    worker = Worker(TerminalSink(SystemExit("boom")), batch_size=1, flush_interval=0.01)
    entered, proceed = threading.Event(), threading.Event()
    real_terminal = worker._terminal_failure

    def blocking_terminal(exc: BaseException, undrained: int) -> None:
        entered.set()
        proceed.wait(timeout=5)
        real_terminal(exc, undrained)

    worker._terminal_failure = blocking_terminal  # type: ignore[method-assign]
    worker.submit(_span("a"))
    assert entered.wait(timeout=5), "the drain must be inside the terminal line"
    assert worker._thread.is_alive(), "the premise: is_alive() still reads True here"

    closing = threading.Thread(target=lambda: worker.shutdown(timeout=1.0), daemon=True)
    closing.start()
    assert _wait_until(lambda: worker._stop.is_set(), timeout=5.0), "shutdown got past its put"

    assert _SHUTDOWN_SENTINEL not in list(worker._queue.queue), (
        "the drain has stopped reading, so a sentinel here would never be consumed"
    )

    proceed.set()
    closing.join(timeout=10)
    worker._thread.join(timeout=5)
    capsys.readouterr()


def test_an_unbounded_flush_does_not_hang_after_the_sweep_while_the_thread_lives(capsys) -> None:
    """The gap between the sweep and the thread's exit — where `is_alive()` alone still lies.

    A marker landing here is behind the only sweep that was ever going to read it, yet the
    thread is still alive, so a liveness-only check would let the caller wait: forever with
    `timeout=None`, because nothing sweeps again. `_drain_finished` is already set, which is
    the whole reason `flush()` consults it and not just liveness. The thread is held past the
    sweep so the window is exact rather than raced for.
    """
    worker = Worker(TerminalSink(SystemExit("boom")), batch_size=1, flush_interval=0.01)
    swept, release = threading.Event(), threading.Event()
    real_sweep = worker._release_waiters

    def sweep_then_hold() -> None:
        real_sweep()
        swept.set()
        release.wait(timeout=5)

    worker._release_waiters = sweep_then_hold  # type: ignore[method-assign]
    worker.submit(_span("a"))
    assert swept.wait(timeout=5), "the drain must be past its sweep"
    assert worker._thread.is_alive(), "the premise: liveness still reads True here"

    try:
        assert [bool(r) for r in _flush_on_watchdog(worker, None)] == [False]
    finally:
        release.set()
        worker._thread.join(timeout=5)
    capsys.readouterr()


def test_the_fast_return_reports_an_abandoned_drain_as_failure(capsys) -> None:
    """SPEC-021's whole subject, reachable through the fast return this PR added.

    `flush()` returning True for a drain that was abandoned is the false success SPEC-021
    exists to prevent — "a false success exactly where flush() matters most". The fast return
    must therefore report `delivered`, not merely that the marker was answered: a review found
    that dropping the `and marker.delivered` conjunct survived all 1105 tests, because the
    sibling test pins only the True direction.
    """
    worker = Worker(AlwaysFailSink(), batch_size=10, flush_interval=0.01, max_retries=0)
    worker.submit(_span("a"))
    real_put = worker._queue.put

    def put_then_die(item: object, timeout: float | None = None) -> None:
        real_put(item, timeout=timeout)
        worker._stop.set()
        worker._thread.join(timeout=5)

    worker._queue.put = put_then_die  # type: ignore[method-assign]

    result = _flush_on_watchdog(worker, None)

    capsys.readouterr()
    assert worker.health().failed_batches == 1, "the premise: the drain abandoned the batch"
    assert [bool(r) for r in result] == [False], (
        "answered is not delivered — the batch it carried was abandoned"
    )


# -- SPEC-050 FR-001: an expired shutdown releases the waiters it stranded ---------------


def test_a_parked_unbounded_flush_is_released_when_shutdown_expires(capsys) -> None:
    """FR-001 AC-1, AC-2. `flush(timeout=None)` behind a stuck sink is answered, not stranded.

    The reproduction the spec was written from: the expiry branch returned before
    `_release_waiters`, so a caller the API documents as supported waited forever on a drain this
    very call had just given up on. Measured before the fix, `flusher still waiting after
    shutdown gave up: True`.

    The verdict is asserted, not merely the wakeup: `abandoned` and not `timed-out` is what says
    the marker was *answered* pessimistically rather than left to run out a clock, and the two
    are indistinguishable from the fact that the thread finished.
    """
    sink = BlockingSink()
    worker = Worker(sink, batch_size=1, flush_interval=0.01)
    worker.submit(_span("a"))
    assert _wait_until(lambda: sink.in_emit.is_set()), "the premise: the drain is inside emit"

    verdict: list[FlushResult] = []
    flusher = threading.Thread(
        target=lambda: verdict.append(worker.flush(timeout=None)), daemon=True
    )
    flusher.start()
    assert _wait_until(lambda: any(isinstance(i, worker_mod._FlushMarker)
                                   for i in list(worker._queue.queue))), "marker never queued"

    worker.shutdown(timeout=0.3)
    flusher.join(timeout=2.0)
    still_waiting = flusher.is_alive()
    # Released unconditionally: a regression strands this thread on an unbounded wait, and a
    # failing assertion that also hangs the runner reports nothing useful.
    sink.release.set()
    worker._thread.join(timeout=5)
    flusher.join(timeout=5)
    capsys.readouterr()

    assert not still_waiting, "an unbounded flush must not outlive the shutdown that gave up"
    assert verdict and verdict[0].ok is False
    assert verdict[0].reason == "abandoned", (
        "answered pessimistically by the sweep, not left to time out"
    )


def test_a_marker_the_drain_already_took_is_released_too(capsys) -> None:
    """FR-001. The half the audit's own prescribed remedy did not cover.

    `_release_waiters` answers markers by reading `self._queue.queue`, so it reaches a marker only
    while the marker is still *in* the queue. Whether it is depends on a race the caller does not
    control: if the drain thread is already inside `emit` when `flush()` puts the marker, it stays
    queued and is swept; if the drain dequeues it and *then* blocks in `emit`, it is held in that
    thread's local and nothing answers it.

    The sibling test above pins the first ordering, and it passes with this fix reverted — it was
    written from a probe that waited for the sink to be entered before flushing, which forces the
    easy ordering. This one forces the other: no wait, so the marker is taken and the drain blocks
    with it in hand. Measured on the shipped fix before this one: `items in queue: 0, markers
    visible: 0`, and the flushing thread still alive after `shutdown` returned.
    """
    sink = BlockingSink()
    worker = Worker(sink, batch_size=10, flush_interval=60.0)
    worker.submit(_span("a"))

    verdict: list[FlushResult] = []
    flusher = threading.Thread(
        target=lambda: verdict.append(worker.flush(timeout=None)), daemon=True
    )
    flusher.start()
    assert sink.in_emit.wait(5.0), "the premise: the drain is inside emit"
    with worker._queue.mutex:
        queued = [i for i in worker._queue.queue if isinstance(i, worker_mod._FlushMarker)]
    assert queued == [], (
        "the premise: the drain took the marker before blocking, so a queue sweep cannot see it"
    )

    worker.shutdown(timeout=0.3)
    flusher.join(timeout=2.0)
    still_waiting = flusher.is_alive()
    sink.release.set()
    worker._thread.join(timeout=5)
    flusher.join(timeout=5)
    capsys.readouterr()

    assert not still_waiting, "a marker in flight in a wedged drain is answered by nobody"
    assert verdict and verdict[0].reason == "abandoned"


def test_a_marker_taken_by_the_final_drain_is_released_too(capsys) -> None:
    """FR-001. `_final_drain` takes markers too, and its emit can wedge just as the loop's can.

    `shutdown` queues the sentinel and joins; the drain leaves its loop, `_final_drain` pulls the
    remaining markers out of the queue and blocks inside the tail emit holding them. The join then
    expires, and the sweep on the expiry branch is the only thing left that can answer them — so
    the registration in `_final_drain` is load-bearing on exactly this path and on no other.
    """
    sink = BlockingSink()
    worker = Worker(sink, batch_size=10, flush_interval=60.0)
    worker.submit(_span("a"))
    marker = worker_mod._FlushMarker(seen_failures=0)
    worker._queue.put_nowait(marker)

    worker.shutdown(timeout=0.5)

    assert worker._thread.is_alive(), "the premise: the final drain is wedged in emit"
    assert sink.in_emit.is_set(), "the premise: it got as far as the sink"
    with worker._queue.mutex:
        assert not [i for i in worker._queue.queue if isinstance(i, worker_mod._FlushMarker)], (
            "the premise: the final drain took the marker, so a queue sweep cannot see it"
        )
    assert marker.event.is_set(), "the expiry sweep must reach a marker the final drain holds"
    assert marker.delivered is False

    sink.release.set()
    worker._thread.join(timeout=5)
    capsys.readouterr()


def test_the_taken_marker_record_does_not_grow(capsys) -> None:
    """FR-001. The deregistration is the guard whose failure is silent.

    A `_release_marker` that did nothing would leak one `_FlushMarker` and its `Event` per
    `flush()`, forever, and the entire suite stays green against it — measured, 1000 flushes
    leaving 1000 residual entries. Nothing else here would ever notice, which is precisely the
    shape this repo mutation-tests for.

    Both terminating paths are driven: an emit that returns normally, and one that raises, since
    the deregistration sits in a `finally` and only the second proves it.
    """
    sink = FlakySink(fail_times=0)
    worker = Worker(sink, batch_size=1, flush_interval=0.01, max_retries=0)
    for i in range(200):
        if i % 3 == 0:
            sink.fail_times = sink.attempts + 1  # make the next emit raise
        worker.submit(_span(i))
        worker.flush(timeout=5.0)
    capsys.readouterr()

    assert worker._taken_markers == [], (
        f"{len(worker._taken_markers)} markers retained after 200 flushes — one per flush is a "
        f"leak of an Event apiece for the life of the process"
    )
    worker.shutdown(timeout=5.0)
    capsys.readouterr()
    assert worker._taken_markers == [], "and the final drain releases the ones it took"


def test_a_flush_that_arrives_after_the_sweep_is_not_left_waiting(capsys) -> None:
    """FR-001. The third arrival ordering: the marker is queued after the sweep has run.

    `flush()`'s post-put re-check tested `_drain_finished` and `is_alive()`. On the expiry path
    neither holds — the drain is alive and still inside `emit` — so a marker landing after that
    sweep found every condition false and waited on a drain the process had already given up on.
    Measured: still waiting three seconds later, and released only by re-running the sweep by
    hand, which is also the control showing the block is the arrival order and nothing else.
    With `timeout=None` it is permanent, and on a non-daemon thread the interpreter joins it at
    exit, so the process does not exit either.

    `_drain_settled` set with `_drain_finished` clear is uniquely the expiry branch, which is why
    the verdict can be `abandoned` for a thread that is demonstrably alive.
    """
    sink = BlockingSink()
    worker = Worker(sink, batch_size=1, flush_interval=0.01)
    worker.submit(_span("a"))
    assert sink.in_emit.wait(5.0), "the premise: the drain is wedged inside emit"

    at_put, release = threading.Event(), threading.Event()
    real_put = worker._queue.put

    def parked_put(item: object, block: bool = True, timeout: float | None = None) -> None:
        """Holds this flush's marker until the shutdown has swept and given up."""
        if isinstance(item, worker_mod._FlushMarker):
            at_put.set()
            release.wait(20)
        real_put(item, block, timeout)

    worker._queue.put = parked_put  # type: ignore[method-assign]
    verdict: list[FlushResult] = []
    flusher = threading.Thread(
        target=lambda: verdict.append(worker.flush(timeout=None)), daemon=True
    )
    flusher.start()
    assert at_put.wait(5.0), "the premise: the flusher is past its guards and about to put"

    worker.shutdown(timeout=0.3)
    assert worker._thread.is_alive() and not worker._drain_finished.is_set(), (
        "the premise: the expiry branch, where only _drain_settled is set"
    )
    release.set()
    flusher.join(timeout=3.0)
    still_waiting = flusher.is_alive()
    sink.release.set()
    worker._thread.join(timeout=5)
    flusher.join(timeout=5)
    capsys.readouterr()

    assert not still_waiting, "a marker queued after the sweep is answered by nobody"
    assert verdict and verdict[0].reason == "abandoned", (
        "the drain was given up on, not dead — 'thread-died' would be false of a live thread"
    )


def test_a_flush_arriving_after_a_dead_drain_still_reports_thread_died(capsys) -> None:
    """FR-001. The new condition must not relabel the outcome it was not written for.

    `given_up` is `_drain_settled` **and not** `_drain_finished`, which is uniquely the expiry
    branch — every other setter sets both. Dropping the second half survives the rest of the
    suite, because the sibling test returns at `flush()`'s *early* liveness guard and never
    reaches the post-put re-check at all. This one does reach it: the marker is held at the put
    until the drain has died, so `is_alive()` is already false when it lands.

    A drain that died is `"thread-died"` and a drain that was given up on is `"abandoned"`. Both
    are falsy and a caller branching on `bool()` cannot tell, which is exactly why the reason has
    to stay truthful for the reader who looks.
    """
    sink = TerminalSink(SystemExit(1))
    worker = Worker(sink, batch_size=1, flush_interval=0.01)

    at_put, release = threading.Event(), threading.Event()
    real_put = worker._queue.put

    def parked_put(item: object, block: bool = True, timeout: float | None = None) -> None:
        """Holds this flush's marker until the drain thread has terminated."""
        if isinstance(item, worker_mod._FlushMarker):
            at_put.set()
            release.wait(20)
        real_put(item, block, timeout)

    worker._queue.put = parked_put  # type: ignore[method-assign]
    verdict: list[FlushResult] = []
    flusher = threading.Thread(
        target=lambda: verdict.append(worker.flush(timeout=5.0)), daemon=True
    )
    flusher.start()
    assert at_put.wait(5.0), "the premise: the flusher is past its guards"

    worker.submit(_span("a"))  # kills the drain through TerminalSink
    assert _wait_until(lambda: not worker._thread.is_alive()), "the premise: the drain died"
    assert worker._drain_finished.is_set() and worker._drain_settled.is_set(), (
        "the premise: a terminal exit sets both flags, unlike the expiry branch"
    )
    release.set()
    flusher.join(timeout=5.0)
    capsys.readouterr()

    assert verdict and verdict[0].reason == "thread-died", (
        "a drain that died is not a drain that was given up on"
    )
    worker.shutdown(timeout=5.0)
    capsys.readouterr()


def test_a_marker_taken_after_the_drain_settled_answers_itself(capsys) -> None:
    """FR-001. The last gap: between `Queue.get` returning a marker and it being recorded.

    In that window the marker is in neither the queue nor the record, so a sweep landing there
    misses it — and because the premise is a sink whose `emit` never returns, the drain never
    reaches its own closing sweep either, so the strand is permanent. Measured at 1 in 300 with
    `shutdown(timeout=0)`, a public argument.

    `_drain_settled` is set immediately before the sweep on both the expiry and terminal paths, so
    a marker taken after that point answers itself.

    Asserted through `flush()` rather than by calling the private method, so what it pins is the
    caller's outcome and not the two lines that produce it. The window is widened rather than
    simulated: the queue's `get` is interposed so the drain parks *after* it has the marker and
    before it returns, which is precisely the gap, and it makes a 1-in-300 race deterministic
    without changing the code under test.
    """
    sink = BlockingSink()
    worker = Worker(sink, batch_size=10, flush_interval=60.0)
    worker.submit(_span("a"))
    assert _wait_until(lambda: worker._queue.qsize() == 0), "the span must reach `pending` first"
    live, took, may_return = threading.Event(), threading.Event(), threading.Event()
    real_get = worker._queue.get

    def parked_get(block: bool = True, timeout: float | None = None) -> object:
        """Parks the drain with the marker in hand, in the window before it is recorded."""
        live.set()
        item = real_get(block, timeout)
        if isinstance(item, worker_mod._FlushMarker):
            took.set()
            may_return.wait(10)
        return item

    worker._queue.get = parked_get  # type: ignore[method-assign]
    # The drain is parked inside the *unwrapped* `get`, which would hand it the marker without
    # ever entering the wrapper. One more submission wakes it so it re-enters through the wrapper;
    # neither trigger fires at two items, so both just accumulate in `pending`.
    worker.submit(_span("b"))
    assert live.wait(5.0), "the interposed get was never reached"

    verdict: list[FlushResult] = []
    flusher = threading.Thread(
        target=lambda: verdict.append(worker.flush(timeout=None)), daemon=True
    )
    flusher.start()
    assert took.wait(5.0), "the premise: the drain holds the marker and has not recorded it"
    with worker._queue.mutex:
        assert not [i for i in worker._queue.queue if isinstance(i, worker_mod._FlushMarker)]
    assert not sink.in_emit.is_set(), "the premise: it has not started emitting yet"
    assert worker._taken_markers == [], "the premise: it is in neither the queue nor the record"

    worker.shutdown(timeout=0)  # its sweep finds both populations empty
    may_return.set()  # the drain records the marker, then wedges in the sink for good
    flusher.join(timeout=5.0)
    still_waiting = flusher.is_alive()
    sink.release.set()
    worker._thread.join(timeout=5)
    capsys.readouterr()

    assert not still_waiting, (
        "a marker taken after the sweep had already run is answered by nothing else"
    )
    assert verdict and verdict[0].reason == "abandoned", (
        "the self-answer is the pessimistic verdict the sweep would have given"
    )


def test_a_released_marker_answered_again_still_delivers_once(capsys) -> None:
    """FR-001 AC-4. The sweep costs a verdict, never a delivery.

    `_release_waiters` reads markers without consuming them, so one it answers is still in the
    queue for `_final_drain` to answer again when the emit returns. That second answer reaches
    nobody — the waiter woke on the first — and the events must reach the sink exactly once,
    which is the half of the trade FR-001 claims is unaffected.
    """
    sink = BlockingSink()
    worker = Worker(sink, batch_size=1, flush_interval=0.01)
    worker.submit(_span("a"))
    assert _wait_until(lambda: sink.in_emit.is_set())
    marker = worker_mod._FlushMarker(seen_failures=0)
    worker._queue.put_nowait(marker)

    worker.shutdown(timeout=0.3)
    assert marker.event.is_set(), "the premise: the sweep answered it"

    sink.release.set()
    worker._thread.join(timeout=5)
    capsys.readouterr()

    assert sink.events == [{"message": "a"}], "one delivery, whatever the verdict said"


def test_a_clean_shutdown_still_answers_each_marker_with_the_real_outcome(capsys) -> None:
    """FR-001 AC-3, and the guard that the new call did not displace the old one.

    A `shutdown()` whose join succeeds is the untouched path: the marker is answered by the
    drain that carried it, so it reports delivered rather than the sweep's pessimism.
    """
    sink = RecordingSink()
    worker = Worker(sink, batch_size=1, flush_interval=0.01)
    worker.submit(_span("a"))
    result = worker.flush(timeout=2.0)
    worker.shutdown(timeout=2.0)
    capsys.readouterr()

    assert bool(result) is True, "a confirmed drain still reports delivery"
    assert worker.health().stopped_reason is None, "and no ShutdownTimeout on a clean join"


def test_the_expiry_branch_still_reports_shutdown_timeout(capsys) -> None:
    """FR-001 AC-3. The record and the line are unchanged by the sweep that now follows them."""
    sink = BlockingSink()
    worker = Worker(sink, batch_size=1, flush_interval=0.01)
    worker.submit(_span("a"))
    assert _wait_until(lambda: sink.in_emit.is_set())

    worker.shutdown(timeout=0.2)
    err = capsys.readouterr().err

    assert worker.health().stopped_reason == "ShutdownTimeout"
    assert err.count("shutdown timed out after") == 1, "exactly one line, as before"
    assert "the sink is left open" in err

    sink.release.set()
    worker._thread.join(timeout=5)


# -- SPEC-050 FR-002: a second shutdown waits for an in-flight inline close --------------


class _CloseIsDeliverySink(RecordingSink):
    """Buffers on emit and delivers on close, so a killed close is measurable as lost events."""

    def __init__(self, close_seconds: float = 0.0) -> None:
        super().__init__()
        self.wire: list[dict] = []
        self.in_close = threading.Event()
        self.may_finish = threading.Event()
        self._close_seconds = close_seconds

    def close(self) -> None:
        """Puts the buffer on the wire, slowly enough that a caller returning through it shows."""
        self.in_close.set()
        if self._close_seconds:
            time.sleep(self._close_seconds)
        else:
            self.may_finish.wait(30)
        self.wire.extend(self.events)
        self.closed += 1


def test_a_second_shutdown_waits_for_an_inline_close_it_did_not_claim(capsys) -> None:
    """FR-002 AC-1. The reproduction: `atexit` returned through a close that was still running.

    Measured before the fix with this sink shape — `closes started=1 finished=0 wire=0
    buffered=12` at 0.31 s — because `_close_if_owed` found the close already claimed and
    returned. The claim is about the *second* caller, so it is the second call that is timed.
    """
    sink = _CloseIsDeliverySink(close_seconds=0.6)
    worker = Worker(sink, batch_size=1, flush_interval=0.01)
    worker.submit(_span("a"))
    assert _wait_until(lambda: bool(sink.events)), "the premise: the batch reached the sink"

    first = threading.Thread(target=lambda: worker.shutdown(timeout=5.0))
    first.start()
    assert sink.in_close.wait(5.0), "the premise: the first caller is inside close()"

    worker.shutdown(timeout=5.0)
    capsys.readouterr()

    assert sink.closed == 1, "the second caller waited; it did not close a second time"
    assert sink.wire == [{"message": "a"}], "and the close it waited for delivered"
    first.join(timeout=5)


def test_the_waiting_caller_does_not_close_a_second_time(capsys) -> None:
    """FR-002 AC-7. Waiting is not closing.

    Distinct from the assertion above because that one is satisfied by a close count of 1 for a
    caller that skipped the wait entirely; this pins the pair — it waited *and* closed nothing.
    """
    sink = _CloseIsDeliverySink(close_seconds=0.4)
    worker = Worker(sink, batch_size=1, flush_interval=0.01)
    first = threading.Thread(target=lambda: worker.shutdown(timeout=5.0))
    first.start()
    assert sink.in_close.wait(5.0)

    start = time.monotonic()
    worker.shutdown(timeout=5.0)
    elapsed = time.monotonic() - start
    capsys.readouterr()

    assert sink.closed == 1
    assert elapsed >= 0.1, f"the second caller returned in {elapsed:.3f}s without waiting"
    first.join(timeout=5)


def test_a_stuck_close_costs_the_second_caller_only_the_closer_grace(capsys) -> None:
    """FR-002 AC-4. The wait is capped, so a stuck close cannot hold the exit for the budget.

    The gap is what makes this a bound rather than a coincidence: the close never returns at all,
    and the budget offered is 30 s, so a caller that returns inside a few seconds can only have
    been capped. `DEFAULT_CLOSER_GRACE` is read rather than written, so re-deriving that constant
    does not silently make this assertion vacuous.
    """
    sink = _CloseIsDeliverySink()  # never finishes: may_finish is never set
    worker = Worker(sink, batch_size=1, flush_interval=0.01)
    first = threading.Thread(target=lambda: worker.shutdown(timeout=None), daemon=True)
    first.start()
    assert sink.in_close.wait(5.0)

    start = time.monotonic()
    worker.shutdown(timeout=30.0)
    elapsed = time.monotonic() - start
    capsys.readouterr()

    grace = _lifecycle.DEFAULT_CLOSER_GRACE
    assert elapsed < grace + 2.0, f"waited {elapsed:.2f}s against a {grace}s cap on a 30s budget"
    sink.may_finish.set()


def test_a_zero_timeout_second_shutdown_does_not_inherit_the_other_deadline(capsys) -> None:
    """FR-002 AC-5. The new arithmetic is exactly what could get this wrong.

    `_closer_grace` carves from the caller's own deadline, so a zero budget must yield a zero
    wait rather than the cap — the failure mode being a `min()` that dropped the caller's term.
    """
    sink = _CloseIsDeliverySink()
    worker = Worker(sink, batch_size=1, flush_interval=0.01)
    first = threading.Thread(target=lambda: worker.shutdown(timeout=None), daemon=True)
    first.start()
    assert sink.in_close.wait(5.0)

    start = time.monotonic()
    worker.shutdown(timeout=0)
    elapsed = time.monotonic() - start
    capsys.readouterr()

    assert elapsed < 0.5, f"shutdown(timeout=0) waited {elapsed:.2f}s on another caller's close"
    sink.may_finish.set()


def test_a_second_shutdown_with_no_close_in_flight_returns_at_once(capsys) -> None:
    """FR-002 AC-6. The ordinary case pays nothing for the new wait."""
    sink = RecordingSink()
    worker = Worker(sink, batch_size=1, flush_interval=0.01)
    worker.submit(_span("a"))
    worker.shutdown(timeout=5.0)

    start = time.monotonic()
    worker.shutdown(timeout=5.0)
    elapsed = time.monotonic() - start
    capsys.readouterr()

    assert sink.closed == 1
    assert elapsed < 0.5, f"an idempotent shutdown with nothing running waited {elapsed:.2f}s"


def test_the_closing_slot_is_emptied_for_a_forked_child() -> None:
    """FR-002 AC-8. A child inherits no closer thread, so it must inherit no promise of one.

    `_fork._fresh_primitive` carries an `Event`'s set state across a fork, so an inherited slot
    would answer the child's *own* later close instantly — its background `shutdown()` claims a
    close, `atexit` reads "already finished" and returns, and the child exits through it. Both
    branches are asserted because a resumed child and a retired one inherit the same slot.

    The owed-swap record goes with it: a child stranded nothing, and keeping the parent's sinks
    costs a strong reference each and a refused release at every child exit.
    """
    for resume in (True, False):
        worker = Worker(RecordingSink(), batch_size=1, flush_interval=0.01)
        worker._closing = threading.Event()
        worker._closing.set()  # as an inherited, already-answered close would arrive
        worker._unclosed_swaps = [RecordingSink()]  # and a sink the child never stranded
        worker._taken_markers = [worker_mod._FlushMarker(seen_failures=0)]  # and a caller it
        # cannot answer: the flush() waiting on it is on a thread that did not survive the fork

        worker._reinit_after_fork(resume=resume)

        assert worker._closing is None, f"the slot survived the fork with resume={resume}"
        assert worker._unclosed_swaps == [], (
            f"the child inherited the parent's stranded sinks with resume={resume}"
        )
        assert worker._taken_markers == [], (
            f"the child inherited the parent's in-flight flush markers with resume={resume}"
        )
        worker.shutdown(timeout=2.0)


# -- SPEC-050 FR-004: a sink stranded by an unconfirmed swap is closed at shutdown -------


class _ClientBufferingSink(RecordingSink):
    """Holds events in a client buffer that only `close()` puts on the wire."""

    def __init__(self, emit_seconds: float = 0.0) -> None:
        super().__init__()
        self.wire: list[dict] = []
        self._emit_seconds = emit_seconds

    def emit(self, batch: list[dict]) -> None:
        """Buffers, slowly enough that a swap's drain cannot be confirmed within its budget."""
        if self._emit_seconds:
            time.sleep(self._emit_seconds)
        super().emit(batch)

    def close(self) -> None:
        """Delivers the buffer; a sink never closed therefore reads as lost events."""
        self.wire.extend(self.events)
        self.closed += 1


def _strand(worker: Worker, old: _ClientBufferingSink, new: object) -> None:
    """Performs a swap whose drain cannot be confirmed, leaving `old` recorded and open."""
    assert worker.swap_sink(new, timeout=0.05) is True  # type: ignore[arg-type]
    assert worker.incomplete_swaps >= 1, "the premise: the drain was not confirmed"
    assert any(s is old for s in worker._unclosed_swaps), "the premise: it was recorded"


def test_a_sink_stranded_by_an_unconfirmed_swap_is_closed_at_shutdown(capsys) -> None:
    """FR-004 AC-1. The reproduction: nine events died in a client buffer that was never closed.

    Measured before the fix, `A.closes=0 A.buf(unflushed)=9 A.wire=0` after a clean `shutdown()`.
    The wire is asserted rather than the close count alone, because for this sink shape the close
    *is* the delivery and a close that happened but delivered nothing would pass a count check.
    """
    old = _ClientBufferingSink(emit_seconds=0.4)
    worker = Worker(old, batch_size=1, flush_interval=0.01)
    for i in range(3):
        worker.submit(_span(i))
    assert _wait_until(lambda: bool(old.events)), "the premise: events reached the old sink"
    new = RecordingSink()
    _strand(worker, old, new)

    worker.shutdown(timeout=10.0)
    capsys.readouterr()

    assert old.closed == 1, "the stranded sink is closed exactly once"
    assert old.wire == old.events, "and its buffer reached the wire"
    assert worker._unclosed_swaps == [], "the record is discharged"


def test_an_expired_shutdown_leaves_a_stranded_sink_for_the_next_call(capsys) -> None:
    """FR-004 AC-2. The drain thread may still be inside its emit, so an expired call declines.

    And the deferral is the point: `_close_if_owed` is where the close lives precisely so the
    `atexit` call that follows an expired one performs it, rather than the record dying with the
    first attempt.
    """
    old = _ClientBufferingSink(emit_seconds=0.3)
    worker = Worker(old, batch_size=1, flush_interval=0.01)
    worker.submit(_span("a"))
    blocking = BlockingSink()
    _strand(worker, old, blocking)
    worker.submit(_span("b"))
    assert _wait_until(lambda: blocking.in_emit.is_set()), "the premise: the drain is wedged"

    worker.shutdown(timeout=0.3)
    assert worker._thread.is_alive(), "the premise: the join expired"
    assert old.closed == 0, "a live drain thread means no close is decided yet"
    assert any(s is old for s in worker._unclosed_swaps), "and the record survives the attempt"

    # The idempotent path is where `_close_if_owed` is actually reached with the thread still
    # alive — the expiry branch returns before it — so this is what pins the liveness guard.
    worker.shutdown(timeout=0.3)
    assert worker._thread.is_alive(), "the premise: still wedged"
    assert old.closed == 0, (
        "the drain thread may still be inside the stranded sink's emit; it must not be closed"
    )
    assert any(s is old for s in worker._unclosed_swaps), "and the record still survives"

    blocking.release.set()
    worker._thread.join(timeout=5)
    worker.shutdown(timeout=5.0)
    capsys.readouterr()

    assert old.closed == 1, "the next call finds the thread finished and closes it then"


def test_a_stranded_sink_readopted_as_the_live_sink_is_closed_once(capsys) -> None:
    """FR-004 AC-3, route one. The live-sink branch closes it, so the record must let go.

    Without the prune both branches of `_close_if_owed` reach the same object, and
    `_lifecycle.release` latches nothing about "already closed" — so the second close is silent.
    """
    old = _ClientBufferingSink(emit_seconds=0.3)
    worker = Worker(old, batch_size=1, flush_interval=0.01)
    worker.submit(_span("a"))
    _strand(worker, old, RecordingSink())

    assert worker.swap_sink(old, timeout=5.0) is True, "re-adopt it as the live sink"
    assert worker._unclosed_swaps == [], "re-adoption must drop it from the owed record"

    worker.shutdown(timeout=10.0)
    capsys.readouterr()

    assert old.closed == 1, f"closed {old.closed} times"


def test_a_stranded_sink_swapped_out_again_confirmed_is_closed_once(capsys) -> None:
    """FR-004 AC-3, route two. `_close_swapped_out` takes it, so the record must let go.

    The second swap's drain *is* confirmed, so the confirmed branch closes it — while the record
    from the first, unconfirmed swap still named it.
    """
    old = _ClientBufferingSink(emit_seconds=0.3)
    worker = Worker(old, batch_size=1, flush_interval=0.01)
    worker.submit(_span("a"))
    _strand(worker, old, RecordingSink())
    assert worker.swap_sink(old, timeout=5.0) is True
    old._emit_seconds = 0.0

    assert worker.swap_sink(RecordingSink(), timeout=5.0) is True, "confirmed this time"
    worker.shutdown(timeout=10.0)
    capsys.readouterr()

    assert old.closed == 1, f"closed {old.closed} times"


def test_two_unconfirmed_swaps_close_both_stranded_sinks_once(capsys) -> None:
    """FR-004 AC-4. The record is a list, so a second stranding does not displace the first."""
    first = _ClientBufferingSink(emit_seconds=0.3)
    worker = Worker(first, batch_size=1, flush_interval=0.01)
    worker.submit(_span("a"))
    second = _ClientBufferingSink(emit_seconds=0.3)
    _strand(worker, first, second)
    worker.submit(_span("b"))
    third = RecordingSink()
    _strand(worker, second, third)

    worker.shutdown(timeout=10.0)
    capsys.readouterr()

    assert (first.closed, second.closed) == (1, 1), "both stranded sinks closed exactly once"
    assert third.closed == 1, "and the live sink by its own branch"


def test_a_confirmed_swap_records_nothing(capsys) -> None:
    """FR-004 AC-6. The negative case: the record stays empty where the drain was confirmed.

    Without this a change that recorded on *every* swap would pass every other test here — the
    prune would hide it — while doubling the closes on the one path that already had a closer.
    """
    old = _ClientBufferingSink()
    worker = Worker(old, batch_size=1, flush_interval=0.01)
    worker.submit(_span("a"))
    new = RecordingSink()

    assert worker.swap_sink(new, timeout=5.0) is True
    assert worker.incomplete_swaps == 0, "the premise: this drain was confirmed"
    assert worker._unclosed_swaps == [], "a confirmed swap owes nothing"

    worker.shutdown(timeout=5.0)
    capsys.readouterr()
    assert old.closed == 1, f"closed {old.closed} times"


def test_a_stuck_stranded_close_does_not_extend_the_shutdown_budget(capsys) -> None:
    """FR-004 AC-5. The stranded close is detached, so only `_join_closers` waits on it.

    The stranded sink's `close()` never returns and the live sink's is instant, so a shutdown
    that ran the stranded close inline would not return at all.
    """
    class _NeverCloses(_ClientBufferingSink):
        def close(self) -> None:
            """Never returns, so an inline close here would hang the shutdown outright."""
            self.closed += 1
            time.sleep(30)

    old = _NeverCloses(emit_seconds=0.3)
    worker = Worker(old, batch_size=1, flush_interval=0.01)
    worker.submit(_span("a"))
    _strand(worker, old, RecordingSink())

    start = time.monotonic()
    worker.shutdown(timeout=5.0)
    elapsed = time.monotonic() - start
    capsys.readouterr()

    grace = _lifecycle.DEFAULT_CLOSER_GRACE
    assert elapsed < 5.0 + grace + 2.0, f"shutdown took {elapsed:.2f}s on a close that never ends"
    assert old.closed == 1, "the close was started, just not waited on"


def test_the_incomplete_swap_line_no_longer_says_the_sink_stays_open(capsys) -> None:
    """FR-004 AC-8, AC-9. The announcement had to change with the behaviour it describes.

    The old text said the sink "is left open" full stop, which FR-004 makes false: it is left
    open only until a `shutdown()` that finds the drain thread ended. A line that outlives the
    claim it was written for is the class this repo's docstring rule exists for.
    """
    old = _ClientBufferingSink(emit_seconds=0.3)
    worker = Worker(old, batch_size=1, flush_interval=0.01)
    worker.submit(_span("a"))
    _strand(worker, old, RecordingSink())
    err = capsys.readouterr().err

    assert err.count("could not be confirmed drained") == 1, "exactly one line per swap"
    assert "shutdown() that finds the drain thread ended closes it" in err
    assert worker.incomplete_swaps == 1

    worker.shutdown(timeout=10.0)
    capsys.readouterr()


# -- SPEC-050 FR-005: a submission that lands after the final drain is counted -----------


def test_a_submission_racing_the_final_drain_is_counted(capsys) -> None:
    """FR-005 AC-1, AC-5. The unlocked read is fine; returning on it alone was not.

    A caller preempted between reading `_shutdown_done` and its `put_nowait` queues its item
    after the final drain, where nothing will read it — measured `queued=1
    submitted_after_shutdown=0` with no line, so the documented `retired` + counter pair could
    not fire. The preemption point is injected at the put rather than raced for, because a race
    test that passes against the bug it exists to catch is worse than none.
    """
    sink = RecordingSink()
    worker = Worker(sink, batch_size=1, flush_interval=0.01)
    at_put, release = threading.Event(), threading.Event()
    real_put = worker._queue.put_nowait

    def parked_put(item: object) -> None:
        """Parks exactly where `submit` sits after its unlocked flag read.

        It un-patches itself first, so only the submitter is parked: `shutdown` puts its own
        sentinel through this same object, and leaving the patch in place deadlocked the two.
        """
        worker._queue.put_nowait = real_put  # type: ignore[method-assign]
        at_put.set()
        release.wait(10)
        real_put(item)

    submitter = threading.Thread(target=lambda: worker.submit(_span("late")))
    worker._queue.put_nowait = parked_put  # type: ignore[method-assign]
    submitter.start()
    assert at_put.wait(5.0), "the premise: the submitter is parked at its put"

    worker.shutdown(timeout=5.0)
    release.set()
    submitter.join(timeout=5)
    err = capsys.readouterr().err

    health = worker.health()
    assert health.submitted_after_shutdown == 1, "the item was queued where nothing will read it"
    assert "logged after shutdown()" in err, "and announced, not only counted"
    assert health.queued == 1, "still visible as queued, unchanged"


def test_a_submission_after_a_latched_shutdown_is_counted_exactly_once(capsys) -> None:
    """FR-005 AC-2. The post-put read must not double-count what the pre-put read caught.

    The `not retired` conjunct is the whole guard, and dropping it leaves every post-shutdown
    submission counted twice — a doubling no other assertion here would notice.
    """
    worker = Worker(RecordingSink(), batch_size=1, flush_interval=0.01)
    worker.shutdown(timeout=5.0)

    worker.submit(_span("a"))
    worker.submit(_span("b"))
    capsys.readouterr()

    assert worker.health().submitted_after_shutdown == 2, "two submissions, two counts"


def test_a_submission_before_shutdown_is_not_counted(capsys) -> None:
    """FR-005 AC-3. The ordinary path is untouched by the second read."""
    worker = Worker(RecordingSink(), batch_size=1, flush_interval=0.01)
    worker.submit(_span("a"))
    worker.shutdown(timeout=5.0)
    capsys.readouterr()

    assert worker.health().submitted_after_shutdown == 0


def test_a_submission_dropped_while_racing_shutdown_is_not_counted_as_stranded(capsys) -> None:
    """FR-005 AC-6. An item the queue refused cannot be stranded in it.

    The window is the same one AC-1 is about, with the put *failing*: a caller reads the flag as
    clear, `shutdown()` latches it, and the put then raises `queue.Full`. Without the `return` in
    that branch the post-put read runs on a submission that never joined the queue, reporting one
    loss in two fields — `dropped` and `submitted_after_shutdown` — where a reader has no way to
    tell it was one event.

    The queue has to stay full across the shutdown for the put to fail at all, which is why the
    drain thread is wedged and the shutdown is left to expire.
    """
    sink = BlockingSink()
    worker = Worker(sink, batch_size=1, flush_interval=60.0, max_queue=1)
    worker.submit(_span("a"))
    assert _wait_until(lambda: sink.in_emit.is_set()), "the premise: the drain is wedged"
    worker.submit(_span("b"))  # fills the queue, and nothing will drain it

    at_put, release = threading.Event(), threading.Event()
    real_put = worker._queue.put_nowait

    def parked_put(item: object) -> None:
        """Parks where `submit` sits after its unlocked read, then un-patches itself."""
        worker._queue.put_nowait = real_put  # type: ignore[method-assign]
        at_put.set()
        release.wait(10)
        real_put(item)

    submitter = threading.Thread(target=lambda: worker.submit(_span("c")))
    worker._queue.put_nowait = parked_put  # type: ignore[method-assign]
    submitter.start()
    assert at_put.wait(5.0), "the premise: the submitter is parked with the flag still clear"

    worker.shutdown(timeout=0.3)
    assert worker._thread.is_alive(), "the premise: the join expired, so the queue is still full"
    release.set()
    submitter.join(timeout=5)
    capsys.readouterr()

    health = worker.health()
    assert health.dropped == 1, f"the premise: the put was refused, dropped={health.dropped}"
    assert health.submitted_after_shutdown == 0, (
        "an item that never joined the queue cannot be stranded in it"
    )
    sink.release.set()
    worker._thread.join(timeout=5)
