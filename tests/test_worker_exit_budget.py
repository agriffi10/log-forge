"""SPEC-062: the exit drain's retries fit the caller's budget.

`Worker._emit` times each attempt and, before each retry, declines it when the batch's longest
attempt so far plus the join margin is predicted to end after the `shutdown()` deadline the
worker recorded. These tests pin the rule's arithmetic with the D/T pairs the spec chose, the
order the deadline is recorded in on every route that sets a stop event, the counting and
wording of a declined retry, the paths the budget must leave alone, the twin sites a stop can
land on, and the expiry line's in-flight count.

Every worker here is hand-built with `batch_size=10, flush_interval=60.0` unless a test says
otherwise, so the first attempt is always in `_final_drain` right after the stop, and the
deadline is recorded by the real `_shutdown_worker` through `retire_process` — never by the
test. Attempt counts are asserted after `worker._thread.join(5)` wherever the join may have
expired, so a count read while the drain thread is still inside the sink cannot pass by timing.
"""

from __future__ import annotations

import re
import threading
import time

import pytest

import log_foundry
from conftest import install_worker, retire_process
from log_foundry import _lifecycle
from log_foundry import worker as worker_mod
from log_foundry.worker import Worker

_LOST_LINE = re.compile(r"^log-foundry: lost (\d+) (\w+)\(s\)(.*)$", re.MULTILINE)
_SLACK = 0.3
"""Seconds of wall-clock slack the spec grants every `shutdown()` return."""
_CPU_BOUND = 0.05
"""Seconds of CPU a `shutdown()` may spend across the whole process, per FR-001's criterion."""


class _SlowFailSink:
    """A sink whose `emit` idles for a fixed span on a fresh `Event`, then raises.

    `fail_times=None` fails every attempt; `fail_times=1` fails the first and delivers from the
    second. `durations` overrides the idle span per attempt, in order, falling back to `seconds`
    once exhausted. Each attempt is tagged with the calling thread's name by a list append, so a
    test driving the worker and the orphan path against one sink can split them without a
    shared counter.
    """

    def __init__(
        self,
        seconds: float = 0.5,
        *,
        fail_times: int | None = None,
        durations: list[float] | None = None,
    ) -> None:
        self.seconds = seconds
        self.fail_times = fail_times
        self.durations = list(durations or [])
        self.attempts: list[str] = []
        self.handed: list[list[dict[str, object]]] = []
        self.ends: list[float] = []
        self.delivered: list[dict[str, object]] = []
        self.closes = 0
        self._lock = threading.Lock()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Idles, then fails or delivers according to the attempt number."""
        with self._lock:
            index = len(self.attempts)
            self.attempts.append(threading.current_thread().name)
            self.handed.append(list(batch))
            seconds = self.durations[index] if index < len(self.durations) else self.seconds
        threading.Event().wait(seconds)
        with self._lock:
            self.ends.append(time.monotonic())
            fails = self.fail_times is None or index < self.fail_times
            if not fails:
                self.delivered.extend(batch)
        if fails:
            raise RuntimeError("CANARY-secret destination is down")

    def close(self) -> None:
        """Counts the close; the assertions here are on when it happened."""
        with self._lock:
            self.closes += 1


class _GatedSink:
    """A sink whose `emit` parks on an event the test controls, then fails or delivers.

    The injected preemption point for the stop-mid-sequence and in-flight cases is this sink's
    own `emit`: a stop provably lands while the drain thread is inside it, with nothing patched.
    `release` stays set once set, so later attempts pass straight through.
    """

    def __init__(self) -> None:
        self.in_emit = threading.Event()
        self.release = threading.Event()
        self.fail = True
        self.attempts: list[str] = []
        self.delivered: list[dict[str, object]] = []
        self.closes = 0
        self._lock = threading.Lock()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Parks until released, then fails or records the batch per `fail`."""
        with self._lock:
            self.attempts.append(threading.current_thread().name)
        self.in_emit.set()
        self.release.wait(30.0)
        if self.fail:
            raise RuntimeError("down")
        with self._lock:
            self.delivered.extend(batch)

    def close(self) -> None:
        """Counts the close."""
        with self._lock:
            self.closes += 1


class _RecordingEvent(threading.Event):
    """A stop event whose first `set()` records what the worker's deadline was at that instant.

    Installed as `_lifecycle._state._stop` before the worker is built, so the worker inherits
    it through `refresh_stop_signal()` and both `_shutdown_worker`'s `_state._stop.set()` and
    `Worker.stop`'s `self._stop.set()` land here. `conftest._reset_worker` replaces the
    process's event after every test, so it never leaks.
    """

    def __init__(self) -> None:
        super().__init__()
        self.worker: Worker | None = None
        self.seen: list[object] = []
        self.recorded = threading.Event()

    def set(self) -> None:
        """Records the deadline the worker holds at the first set, then sets."""
        if self.worker is not None and not self.seen:
            self.seen.append(getattr(self.worker, "_exit_deadline", "unset"))
            self.recorded.set()
        super().set()


def _install_recording_event() -> _RecordingEvent:
    """Puts a `_RecordingEvent` in the process slot a worker built next will inherit."""
    event = _RecordingEvent()
    with _lifecycle._state._lock:
        _lifecycle._state._stop = event
    return event


def _worker(sink: object, **kwargs: object) -> Worker:
    """Builds a worker whose first emit is in `_final_drain`, unless the test says otherwise."""
    kwargs.setdefault("batch_size", 10)
    kwargs.setdefault("flush_interval", 60.0)
    return Worker(sink, **kwargs)  # type: ignore[arg-type]


def _three_events() -> list[dict[str, object]]:
    """One submission carrying three events, the spec's probe shape."""
    return [{"message": "CANARY-value", "n": n} for n in range(3)]


def _shutdown_measured(worker: Worker, timeout: float | None) -> float:
    """Runs the real `shutdown()` path and returns its wall time, asserting its CPU time.

    `time.process_time()` is process-wide across threads, which is deliberate: the busy-spin
    invariant 3 guards against would be in the drain thread's retry loop, and `thread_time()`
    would see only the caller idling in `join`. The only threads alive in the window are this
    test's — the caller in `join`, the drain thread inside the sink's `Event.wait` — so the
    bound is a property of the process and needs no load allowance.
    """
    if _lifecycle._state.worker_exists() is not worker:
        install_worker(worker)
    cpu = time.process_time()
    wall = time.monotonic()
    _lifecycle._shutdown_worker(timeout)
    elapsed, spent = time.monotonic() - wall, time.process_time() - cpu
    assert spent < _CPU_BOUND, (
        f"shutdown(timeout={timeout}) spent {spent:.3f}s of CPU: the wait is not idle"
    )
    return elapsed


def _lost_lines(err: str) -> list[tuple[int, str, str]]:
    """Every `lost N <what>(s)` line on stderr as (count, what, detail)."""
    return [(int(n), what, rest) for n, what, rest in _LOST_LINE.findall(err)]


# --------------------------------------------------------------------------- FR-001


def test_a_retry_predicted_past_the_deadline_is_declined_and_the_sink_is_closed() -> None:
    """FR-001 AC-1. D=0.5, T=1.2: the third attempt would end at 1.6, so it is not started.

    At `71e42dd` the same sink made 3 attempts, the join expired, `stopped_reason` read
    `ShutdownTimeout` and `close()` ran 0 times.
    """
    sink = _SlowFailSink(0.5)
    worker = _worker(sink)
    worker.submit(_three_events())

    elapsed = _shutdown_measured(worker, 1.2)
    closes_at_return = sink.closes
    worker._thread.join(5.0)

    assert len(sink.attempts) == 2, (
        f"the third attempt was predicted to end at 1.6 > 1.2: {sink.attempts}"
    )
    assert elapsed < 1.2 + _SLACK, f"shutdown returned at {elapsed:.2f}s"
    assert worker.stopped_reason is None, "the join did not expire"
    assert closes_at_return == 1, "the sink was closed by the time shutdown returned"


def test_a_tighter_budget_declines_the_first_retry() -> None:
    """FR-001 AC-2. D=0.5, T=0.9: `0.5 + 0.5 + 0.1 > 0.9`, so only the first attempt is made."""
    sink = _SlowFailSink(0.5)
    worker = _worker(sink)
    worker.submit(_three_events())

    _shutdown_measured(worker, 0.9)
    closes_at_return = sink.closes
    worker._thread.join(5.0)

    assert len(sink.attempts) == 1, sink.attempts
    assert closes_at_return == 1


def test_a_wider_budget_permits_two_retries() -> None:
    """FR-001 AC-3. D=0.5, T=1.8: attempts end at 0.5, 1.0, 1.5; the fourth would end at 2.0."""
    sink = _SlowFailSink(0.5)
    worker = _worker(sink)
    worker.submit(_three_events())

    _shutdown_measured(worker, 1.8)
    closes_at_return = sink.closes
    worker._thread.join(5.0)

    assert len(sink.attempts) == 3, sink.attempts
    assert closes_at_return == 1


def test_a_retry_that_fits_is_made(capsys) -> None:
    """FR-001 AC-4. A sink that fails once and then delivers must still get its second attempt.

    At `71e42dd` this delivers and closes at 1.00 s, so a rule that declined it would break
    invariant 2 rather than serve invariant 3.
    """
    sink = _SlowFailSink(0.5, fail_times=1)
    worker = _worker(sink)
    worker.submit(_three_events())

    elapsed = _shutdown_measured(worker, 1.2)

    assert len(sink.delivered) == 3, "the second attempt was made and delivered"
    assert worker.failed_batches == 0
    assert sink.closes == 1
    assert elapsed < 1.0 + _SLACK, f"closed by about 1.0 s, not {elapsed:.2f}"
    assert "lost" not in capsys.readouterr().err


def test_an_ample_budget_costs_nothing() -> None:
    """FR-001 AC-5. Fast failures under a 5 s budget still get every `max_retries + 1` attempt."""
    sink = _SlowFailSink(0.0)
    worker = _worker(sink)
    worker.submit(_three_events())

    _shutdown_measured(worker, 5.0)

    assert len(sink.attempts) == worker.max_retries + 1 == 4, sink.attempts
    assert sink.closes == 1


def test_a_zero_timeout_makes_the_first_attempt_only() -> None:
    """FR-001 AC-6. `timeout=0`: the first attempt is always made and no retry is.

    The join itself expires by design here, so there is no close assertion: the sink is left
    open with the thread still inside its first attempt, which is SPEC-027 FR-004.
    """
    sink = _SlowFailSink(0.05)
    worker = _worker(sink)
    worker.submit(_three_events())

    _shutdown_measured(worker, 0)
    worker._thread.join(5.0)

    assert not worker._thread.is_alive(), "the drain thread ended after its one attempt"
    assert len(sink.attempts) == 1, sink.attempts


def test_the_estimator_is_the_longest_attempt() -> None:
    """FR-001 AC-7. Attempts of 0.1, 0.8, 0.3 under T=1.95: the fourth is declined.

    Before it, `1.2 + 0.8 + 0.1 > 1.95` — whereas the last attempt (0.3), the first (0.1) and
    the mean (0.4) would each have permitted it.
    """
    sink = _SlowFailSink(0.0, durations=[0.1, 0.8, 0.3])
    worker = _worker(sink)
    worker.submit(_three_events())

    _shutdown_measured(worker, 1.95)
    worker._thread.join(5.0)

    assert len(sink.attempts) == 3, sink.attempts
    assert sink.closes == 1


def test_the_nats_case_at_one_tenth_scale() -> None:
    """FR-001 AC-8. `emit` 1.0 s, `max_retries=3`, T=3.0: two attempts, the drain ends at 2.0.

    The third is declined at `2.0 + 1.0 + 0.1 > 3.0`, which at full scale is the NATS default
    `publish_timeout=10` against the 30 s join that `architecture.md` section 12 recorded.
    """
    sink = _SlowFailSink(1.0)
    worker = _worker(sink, max_retries=3)
    worker.submit(_three_events())

    elapsed = _shutdown_measured(worker, 3.0)
    closes_at_return = sink.closes

    assert not worker._thread.is_alive(), "the drain ended inside the budget"
    assert len(sink.attempts) == 2, sink.attempts
    assert elapsed < 2.0 + _SLACK, (
        f"the drain ended at about 2.0 s, shutdown returned at {elapsed:.2f}"
    )
    assert closes_at_return == 1


def test_the_join_margin_is_added_to_the_prediction(monkeypatch) -> None:
    """The margin is part of the rule, not decoration: patched to 0.7 it declines what 0.1 permits.

    D=0.2, T=1.0: `0.2 + 0.2 + 0.7 > 1.0` declines the first retry, where without the margin
    `0.4` would permit it by 0.6 s. No D/T pair at the real margin can tell the two apart by
    more than the spec's own headroom rule allows, which is why the constant is read at each
    decision rather than bound at definition.
    """
    monkeypatch.setattr(worker_mod, "_JOIN_MARGIN", 0.7)
    sink = _SlowFailSink(0.2)
    worker = _worker(sink)
    worker.submit(_three_events())

    _shutdown_measured(worker, 1.0)
    worker._thread.join(5.0)

    assert len(sink.attempts) == 1, sink.attempts


def test_the_deadline_is_recorded_before_the_owners_event_is_set() -> None:
    """FR-001 AC-10, the real path. `_state._stop.set()` already finds the deadline recorded.

    The event is the worker's own, and setting it is what wakes a backoff: a wake that found
    no deadline would permit a retry the budget should have declined.
    """
    event = _install_recording_event()
    sink = _SlowFailSink(0.0)
    log_foundry.configure(service="t", sink=sink)
    worker = _worker(sink)
    install_worker(worker)
    event.worker = worker
    assert worker._stop is event, "the premise: the worker holds the process's event"

    before = time.monotonic()
    log_foundry.shutdown(timeout=1.0)

    assert event.seen, "the owner's set reached the worker's event"
    recorded = event.seen[0]
    assert isinstance(recorded, float), f"the deadline was recorded before the set: {recorded!r}"
    assert before + 1.0 <= recorded <= before + 1.0 + _SLACK, recorded - before


def test_a_direct_stop_records_before_its_own_set() -> None:
    """FR-001 AC-10, the direct route. `Worker.stop` claims before `self._stop.set()`."""
    event = _install_recording_event()
    worker = _worker(_SlowFailSink(0.0))
    event.worker = worker
    assert worker._stop is event

    worker.stop(0.5)

    assert isinstance(event.seen[0], float), f"recorded before the set: {event.seen!r}"


def test_the_record_is_write_once() -> None:
    """FR-001 AC-11. A second stop or shutdown with a later deadline leaves the first standing."""
    sink = _GatedSink()
    worker = _worker(sink, batch_size=1)
    worker.submit([{"a": 1}])
    assert sink.in_emit.wait(5.0)
    try:
        worker.stop(0.2)
        first = worker._exit_deadline
        assert isinstance(first, float), "the premise: the first stop recorded its deadline"

        worker.stop(5.0)
        assert worker._exit_deadline == first, "a second direct stop did not overwrite it"
        retire_process(worker, 9.0)
        assert worker._exit_deadline == first, "nor did a later shutdown()"
    finally:
        sink.release.set()
    worker._thread.join(5.0)


def test_an_unbounded_first_caller_is_not_shortened_by_a_bounded_second() -> None:
    """Write-once is keyed on the stop gate, not on `None`: `timeout=None` is a recorded value.

    A record keyed on `_exit_deadline is None` would let the bounded second caller overwrite
    the unbounded first one's `None` and shorten the join that caller asked to be unbounded.
    """
    event = _install_recording_event()
    sink = _GatedSink()
    worker = _worker(sink, batch_size=1)
    install_worker(worker)
    event.worker = worker
    worker.submit([{"a": 1}])
    assert sink.in_emit.wait(5.0)

    unbounded = threading.Thread(target=lambda: _lifecycle._shutdown_worker(None))
    unbounded.start()
    assert event.recorded.wait(5.0), "the unbounded caller recorded its deadline"
    assert event.seen == [None]

    _lifecycle._shutdown_worker(0.2)
    assert worker._exit_deadline is None, "the bounded second caller did not overwrite None"

    sink.release.set()
    unbounded.join(10.0)
    assert not unbounded.is_alive()
    assert len(sink.attempts) == 4, f"no deadline, so every attempt was made: {sink.attempts}"
    assert sink.closes == 1


# --------------------------------------------------------------------------- FR-002


def test_a_declined_retry_is_counted_once_and_says_why(capsys) -> None:
    """FR-002 AC-1. One count, one line, naming the attempts made and the deadline."""
    sink = _SlowFailSink(0.5)
    worker = _worker(sink)
    worker.submit(_three_events())

    _shutdown_measured(worker, 1.2)
    worker._thread.join(5.0)
    err = capsys.readouterr().err

    assert worker.failed_batches == 1
    lines = [line for line in _lost_lines(err) if line[:2] == (3, "event")]
    assert len(lines) == 1, err
    detail = lines[0][2]
    assert "2 emit attempt" in detail, detail
    assert "deadline" in detail, detail
    assert "abandoned after" not in err, "the exhausted line is not written as well"


def test_exhausting_the_budget_inside_the_deadline_keeps_the_old_line(capsys) -> None:
    """FR-002 AC-2. Fast failure under an ample budget writes the line `71e42dd` wrote."""
    sink = _SlowFailSink(0.0)
    worker = _worker(sink)
    worker.submit(_three_events())

    _shutdown_measured(worker, 5.0)
    err = capsys.readouterr().err

    assert err == "log-foundry: lost 3 event(s); batch abandoned after 4 emit attempts\n"


@pytest.mark.parametrize("ending", ["declined", "abandoned"])
def test_one_count_and_one_line_per_batch_whichever_way_it_ends(ending: str, capsys) -> None:
    """FR-002 AC-3. The ledger: delivered `span.end` + queued + dropped + lost-by-line = spans.

    Invariant 2's span ledger states no failed-batch term, and `tests/test_invariants_model.py`
    balances without one only because `failed_batches == 0` there. A `@trace`d span emits its
    `span.start` as well as its `span.end`, so the line's count is the whole batch and the
    ledger's lost term is the `span.end` events inside the batch the line announced — the line
    is held to that batch's size, so the term reproduces from the line rather than from the
    sink.
    """
    sink = _SlowFailSink(0.5 if ending == "declined" else 0.0)
    log_foundry.configure(service="t", sink=sink)
    worker = _worker(sink)
    install_worker(worker)

    @log_foundry.trace
    def span() -> None:
        return None

    spans = 3
    for _ in range(spans):
        span()

    _shutdown_measured(worker, 1.2 if ending == "declined" else 5.0)
    worker._thread.join(5.0)
    err = capsys.readouterr().err
    health = log_foundry.health()

    lines = _lost_lines(err)
    assert len(lines) == 1 and health.failed_batches == 1, (err, health)
    announced = sum(n for n, what, _ in lines if what == "event")
    lost_batch = sink.handed[0]
    assert all(batch == lost_batch for batch in sink.handed), "one batch, retried as itself"
    assert announced == len(lost_batch), f"the line counts the whole batch: {err}"
    lost_ends = sum(1 for e in lost_batch if e.get("message") == "span.end")
    delivered = sum(1 for e in sink.delivered if e.get("message") == "span.end")
    assert delivered + health.queued + health.dropped + lost_ends == spans, (
        f"{delivered} + {health.queued} + {health.dropped} + {lost_ends} != {spans}: {err}"
    )
    assert ("deadline" in err) == (ending == "declined"), err


def test_a_flush_marker_answered_by_a_declined_drain_reports_abandoned() -> None:
    """FR-002 AC-4. A marker taken by `_drain` and answered after a decline reads `abandoned`.

    Fails against a mutant in which the declined retry does not move `failed_batches`: the
    marker's `_nothing_lost_since` then reads delivered.
    """
    sink = _GatedSink()
    worker = _worker(sink)
    install_worker(worker)
    worker.submit(_three_events())
    results: list[object] = []
    flusher = threading.Thread(target=lambda: results.append(worker.flush(timeout=10.0)))
    flusher.start()
    assert sink.in_emit.wait(5.0)
    with worker._lock:
        taken = list(worker._taken_markers)
    assert taken, "the premise: the marker was taken by _drain and its emit is in flight"

    stopper = threading.Thread(target=lambda: retire_process(worker, 0.5))
    stopper.start()
    deadline = time.monotonic() + 5.0
    while worker._exit_deadline is None and time.monotonic() < deadline:
        time.sleep(0.005)
    recorded_at = time.monotonic()
    assert worker._exit_deadline is not None, "the stop recorded its deadline"
    threading.Event().wait(max(0.0, recorded_at + 0.3 - time.monotonic()))
    sink.release.set()

    flusher.join(10.0)
    stopper.join(10.0)
    worker._thread.join(5.0)
    assert results, "flush() returned"
    result = results[0]
    assert not result, f"the marker was answered by a drain that lost its batch: {result!r}"
    assert getattr(result, "reason", None) == "abandoned", result
    assert len(sink.attempts) == 1, f"the retry was declined: {sink.attempts}"


def test_the_decline_line_names_a_type_and_counts_only(capsys) -> None:
    """FR-002 AC-5. Neither an event's value nor the exception's message reaches stderr."""
    sink = _SlowFailSink(0.5)
    worker = _worker(sink)
    worker.submit(_three_events())

    _shutdown_measured(worker, 1.2)
    worker._thread.join(5.0)
    err = capsys.readouterr().err

    assert "lost 3 event(s)" in err
    assert "CANARY" not in err, err
