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

log_foundry_mod = pytest.importorskip("log_foundry")
worker_mod = pytest.importorskip("log_foundry.worker")
Worker = worker_mod.Worker


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


class SleepySink(RecordingSink):
    def __init__(self, delay: float) -> None:
        super().__init__()
        self.delay = delay

    def emit(self, batch: list[dict]) -> None:
        time.sleep(self.delay)
        super().emit(batch)


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
    sink = SleepySink(delay=0.3)
    w = Worker(sink, batch_size=1, flush_interval=0.01)

    start = time.monotonic()
    w.submit(_span("x"))
    elapsed = time.monotonic() - start
    assert elapsed < 0.1, "submit must not block on the sink's emit"

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
    from log_foundry import decorator

    log_foundry.configure(service="t", version="0", env="t", sink=RecordingSink())
    decorator._worker = None  # ensure a clean lazy creation for this assertion

    w1 = decorator._get_worker()
    w2 = decorator._get_worker()
    try:
        assert w1 is w2, "one worker per process, reused"
        assert w1.sink is log_foundry.get_config().sink, "worker built from the configured sink"
    finally:
        w1.shutdown()
        decorator._worker = None


# -- SPEC-013 FR-002: flush() drains without retiring the worker ------------------------


def test_flush_drains_and_leaves_the_worker_running() -> None:
    sink = RecordingSink()
    # Neither trigger can fire on its own: if these events arrive, flush() delivered them.
    w = Worker(sink, batch_size=1000, flush_interval=100.0)
    try:
        for i in range(5):
            w.submit(_span(i))
        assert sink.events == [], "still buffered before the flush"

        assert w.flush(timeout=5.0) is True
        assert [e["message"] for e in sink.events] == list(range(5))

        # The whole point: unlike shutdown(), everything is still alive afterwards.
        assert sink.closed == 0, "flush must not close the sink"
        assert w._thread.is_alive(), "flush must not retire the worker thread"

        w.submit(_span("after"))
        assert w.flush(timeout=5.0) is True
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
        assert w.flush(timeout=5.0) is True
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
        assert w.flush(timeout=5.0) is True

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

    assert flushed == [True], "a flush racing shutdown is answered by the final drain"
    assert {e["message"] for e in sink.events} == {"a", "b"}, "and its events really did land"
    for batch in sink.batches:
        assert all(isinstance(e, dict) for e in batch), "no marker leaked into the final batch"


def test_flush_is_repeatable_and_concurrent() -> None:
    sink = RecordingSink()
    w = Worker(sink, batch_size=1000, flush_interval=100.0)
    try:
        w.submit(_span("first"))
        assert w.flush(timeout=5.0) is True
        w.submit(_span("second"))
        assert w.flush(timeout=5.0) is True
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

        assert results == [True] * 4, "every concurrent flush is answered"
        assert sink.events[-1]["message"] == "third"
    finally:
        w.shutdown()


def test_flush_does_not_consume_the_shutdown_flag() -> None:
    sink = RecordingSink()
    w = Worker(sink, batch_size=1000, flush_interval=100.0)
    w.submit(_span("x"))
    assert w.flush(timeout=5.0) is True
    assert w.flush(timeout=5.0) is True

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
    assert w.flush(timeout=5.0) is False, "nothing will ever consume the marker now"
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

        assert result is False, "a drain that could not complete reports False"
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
        assert w.flush(timeout=5.0) is False
        assert sink.events == [], "nothing was ever successfully emitted"
        assert w.failed_batches >= 1, "the failure is counted, not swallowed silently"
    finally:
        w.shutdown()


def test_flush_with_timeout_none_returns_true_on_a_healthy_worker() -> None:
    sink = RecordingSink()
    w = Worker(sink, batch_size=1000, flush_interval=100.0)
    try:
        w.submit(_span("n"))
        assert w.flush(timeout=None) is True
        assert [e["message"] for e in sink.events] == ["n"]
    finally:
        w.shutdown()


# -- SPEC-013 FR-003/FR-005: the public `lf.flush()` entry point -------------------------


def test_module_flush_is_a_no_op_when_nothing_was_ever_logged() -> None:
    import log_foundry
    from log_foundry import decorator

    decorator._worker = None
    start = time.monotonic()
    assert log_foundry.flush() is True, "no worker means nothing to drain"
    assert time.monotonic() - start < 1.0
    # Building a worker here would start a thread and register atexit purely to flush nothing.
    assert decorator._worker is None, "flush must not create a worker"


def test_module_flush_after_shutdown_returns_false_promptly() -> None:
    import log_foundry
    from log_foundry import decorator

    log_foundry.configure(service="t", version="0", env="t", sink=RecordingSink())
    decorator._get_worker()
    log_foundry.shutdown()

    start = time.monotonic()
    assert log_foundry.flush(timeout=5.0) is False
    assert time.monotonic() - start < 1.0, "must not block on a worker that is gone"


def test_module_flush_delivers_a_traced_call_and_logging_continues() -> None:
    """The Lambda pattern: drain before returning, then be invoked again on the same worker."""
    import log_foundry
    from log_foundry import decorator

    sink = RecordingSink()
    log_foundry.configure(service="t", version="0", env="t", sink=sink)
    decorator._worker = None
    # Neither batching trigger can fire in the life of this test.
    decorator._worker = worker_mod.Worker(sink, batch_size=1000, flush_interval=100.0)

    @log_foundry.trace
    def handler() -> str:
        log_foundry.info("invoked")
        return "ok"

    try:
        handler()
        assert log_foundry.flush(timeout=5.0) is True
        first = [e["message"] for e in sink.events]
        assert "invoked" in first, "the first invocation's events were drained"
        assert sink.closed == 0, "the sink is still open"

        handler()  # the failure mode this whole spec exists for: does the *second* one log?
        assert log_foundry.flush(timeout=5.0) is True
        assert [e["message"] for e in sink.events].count("invoked") == 2
    finally:
        decorator._worker.shutdown()
        decorator._worker = None


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
    from log_foundry import decorator

    decorator._worker = None
    before = threading.active_count()

    h = log_foundry.health()

    # Compared field-wise, not as a whole tuple: SPEC-019 appended `stopped_reason`, and the
    # advertised way to read a snapshot has always been by attribute.
    assert (h.queued, h.dropped, h.failed_batches) == (0, 0, 0)
    assert h.stopped_reason is None
    assert decorator._worker is None, "health() must not create a worker"
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
    assert w.flush(timeout=0.2) is False, "flush reports failure rather than burning its timeout"
    w.shutdown()  # joins an already-dead thread and closes the sink
    w.shutdown()  # still idempotent


def test_a_decorated_function_is_unaffected_after_the_worker_dies() -> None:
    """The clause of FR-001 that reaches user code: @trace still returns normally (arch §4)."""
    import log_foundry
    from log_foundry import decorator

    log_foundry.configure(service="t", sink=TerminalSink(SystemExit(1)))

    @log_foundry.trace
    def work() -> str:
        return "ok"

    assert work() == "ok"
    log_foundry.flush(timeout=2.0)  # force the drain that kills the thread
    w = decorator._worker
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


def test_existing_health_fields_keep_their_positions() -> None:
    sink = RecordingSink()
    w = Worker(sink, batch_size=1)
    w.submit(_span("a"))
    w.shutdown()
    h = w.health()
    assert (h[0], h[1], h[2]) == (h.queued, h.dropped, h.failed_batches)
    assert h[3] is h.stopped_reason
    # SPEC-026 appended ``sink`` exactly as SPEC-019 appended ``stopped_reason``, and SPEC-030
    # appended four more after it: every field that came before keeps its index, so positional
    # reads written against any earlier shape hold.
    assert len(h) == 9
    assert h[4] is h.sink
    assert (h[5], h[6], h[7]) == (h.retired, h.submitted_after_shutdown, h.incomplete_swaps)
    assert h[8] == h.closing_sinks


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
        assert w.flush(timeout=5.0) is True
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
        assert w.flush(timeout=5.0) is False, "a drain that delivered nothing is not a success"
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
        assert w.flush(timeout=30.0) is False
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
        assert w.flush(timeout=5.0) is True, "nothing to deliver is not a delivery failure"
        assert sink.events == []
        # And again after a successful flush has already drained everything.
        w.submit(_span("a"))
        assert w.flush(timeout=5.0) is True
        assert w.flush(timeout=5.0) is True, "the second flush has nothing left to do"
    finally:
        w.shutdown()


def test_flush_reports_true_when_the_sink_recovers_mid_retry() -> None:
    """Retries are part of the delivery, not a failure of it: the events did reach the sink."""
    sink = FlakySink(fail_times=2)
    w = Worker(sink, batch_size=1000, flush_interval=100.0, max_retries=3)
    try:
        w.submit(_span("eventually"))
        assert w.flush(timeout=5.0) is True
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
        assert w.flush(timeout=5.0) is False
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

    assert flushed == [False], "the final drain's emit failed, so the flush it answered failed"
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

        assert results == [False] * 4, f"every flush must report the loss, got {results}"
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

        assert w.flush(timeout=5.0) is True, "nothing pending, and nothing lost since the call"
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

        assert w.flush(timeout=5.0) is True, "nothing pending, and nothing lost since"
        assert w.failed_batches == 1, "the loss is still on the record where it belongs"

        w.submit(_span("kept"))  # the sink has recovered by now
        assert w.flush(timeout=5.0) is True, "a healthy drain must not inherit the old failure"
        assert [e["message"] for e in sink.events] == ["kept"]
        assert w.flush(timeout=5.0) is True, "and it does not come back on the next empty flush"
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
        assert w.flush(timeout=0.1) is False, "the marker never got in, so nothing was drained"
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
    assert ok is False, "the batch died with the thread; that is not a delivery"
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
    log_foundry = pytest.importorskip("log_foundry")
    decorator = pytest.importorskip("log_foundry.decorator")
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
    monkeypatch.setattr(decorator, "_worker", None)


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
    from log_foundry import decorator

    decorator._worker = None
    h = log_foundry.health()

    assert (h.retired, h.submitted_after_shutdown, h.incomplete_swaps) == (False, 0, 0)
    assert decorator._worker is None, "health() must not create a worker"


def test_decorated_calls_after_a_module_shutdown_are_counted() -> None:
    """End to end, through the public API: the serverless mistake, made and then seen."""
    import log_foundry

    sink = RecordingSink()
    log_foundry.configure(service="t", version="0", env="t", sink=sink)

    @log_foundry.trace
    def handler() -> str:
        return "ok"

    handler()
    assert log_foundry.flush(timeout=5.0) is True
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
from log_foundry import worker as _worker

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
