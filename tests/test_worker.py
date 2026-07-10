"""SPEC-004 — background flush worker: submit, batching, retry, backpressure, shutdown.

These construct real ``Worker`` instances with local test sinks (recording / sleepy /
blocking / flaky) and prefer draining via ``shutdown()`` over sleeping. Where a test must
observe an auto-flush *before* shutdown (count/time triggers), it polls with a bounded
`_wait_until` rather than a fixed sleep.
"""

import threading
import time

import pytest

worker_mod = pytest.importorskip("log_forge.worker")
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
    import log_forge
    from log_forge import decorator

    log_forge.configure(service="t", version="0", env="t", sink=RecordingSink())
    decorator._worker = None  # ensure a clean lazy creation for this assertion

    w1 = decorator._get_worker()
    w2 = decorator._get_worker()
    try:
        assert w1 is w2, "one worker per process, reused"
        assert w1.sink is log_forge.get_config().sink, "worker built from the configured sink"
    finally:
        w1.shutdown()
        decorator._worker = None
