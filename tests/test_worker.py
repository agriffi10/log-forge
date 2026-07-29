"""SPEC-004 — background flush worker: submit, batching, retry, backpressure, shutdown.

These construct real ``Worker`` instances with local test sinks (recording / sleepy /
blocking / flaky) and prefer draining via ``shutdown()`` over sleeping. Where a test must
observe an auto-flush *before* shutdown (count/time triggers), it polls with a bounded
`_wait_until` rather than a fixed sleep.
"""

import threading
import time

import pytest

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


def test_batch_abandoned_after_retry_bound_is_counted() -> None:
    sink = AlwaysFailSink()
    w = Worker(sink, batch_size=1, flush_interval=0.02, max_retries=2)
    w.submit(_span("x"))
    w.shutdown()

    assert w.failed_batches >= 1, "an unrecoverable batch is counted, not silently dropped"
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
        # The drain completed — the events were passed to sink.emit, which is the guarantee.
        # The sink's failure is reported through failed_batches, not by raising at the caller.
        assert w.flush(timeout=5.0) is True
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
        assert "dropped 1 submission(s)" in err
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
    assert err.startswith("log-foundry: worker thread stopped on SystemExit")
    assert "1 undrained event-list(s)" in err
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
    assert len(h) == 4


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
