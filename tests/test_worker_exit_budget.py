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
from typing import TYPE_CHECKING

import pytest

import log_foundry
from conftest import install_worker, retire_process, run_concurrently
from log_foundry import _lifecycle
from log_foundry import worker as worker_mod
from log_foundry.worker import Worker

if TYPE_CHECKING:
    from collections.abc import Callable

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
        self.handed: list[tuple[str, list[dict[str, object]]]] = []
        self.ends: list[float] = []
        self.delivered: list[dict[str, object]] = []
        self.closes = 0
        self._lock = threading.Lock()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Idles, then fails or delivers according to the attempt number."""
        with self._lock:
            index = len(self.attempts)
            self.attempts.append(threading.current_thread().name)
            self.handed.append((threading.current_thread().name, list(batch)))
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


def _measured(call: Callable[[], object]) -> float:
    """Runs a shutdown call and returns its wall time, asserting its CPU time.

    `time.process_time()` is process-wide across threads, which is deliberate: the busy-spin
    invariant 3 guards against would be in the drain thread's retry loop, and `thread_time()`
    would see only the caller idling in `join`. The only threads alive in the window are this
    test's — the caller in `join`, the drain thread inside the sink's `Event.wait` — so the
    bound is a property of the process and needs no load allowance.
    """
    cpu = time.process_time()
    wall = time.monotonic()
    call()
    elapsed, spent = time.monotonic() - wall, time.process_time() - cpu
    assert spent < _CPU_BOUND, f"the shutdown spent {spent:.3f}s of CPU: the wait is not idle"
    return elapsed


def _shutdown_measured(worker: Worker, timeout: float | None) -> float:
    """Runs the real `shutdown()` path over `worker` through `_measured`."""
    if _lifecycle._state.worker_exists() is not worker:
        install_worker(worker)
    return _measured(lambda: _lifecycle._shutdown_worker(timeout))


class _MeasuredThread(threading.Thread):
    """Runs a shutdown call through `_measured` on a thread, keeping its wall time or failure.

    An assertion that fails on a worker thread is otherwise discarded with the thread, which
    would turn the CPU bound into decoration; `finish` re-raises it on the test's thread.
    """

    def __init__(self, call: Callable[[], object], name: str = "stopper") -> None:
        super().__init__(name=name)
        self.call = call
        self.elapsed: float | None = None
        self.failure: BaseException | None = None

    def run(self) -> None:
        """Measures the call, keeping whatever it raised."""
        try:
            self.elapsed = _measured(self.call)
        except BaseException as exc:
            self.failure = exc

    def finish(self, timeout: float) -> float:
        """Joins, re-raises a failure from the thread, and returns the call's wall time."""
        self.join(timeout)
        assert not self.is_alive(), f"{self.name} did not return within {timeout}s"
        if self.failure is not None:
            raise self.failure
        assert self.elapsed is not None
        return self.elapsed


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
    lost_batch = sink.handed[0][1]
    assert all(batch == lost_batch for _, batch in sink.handed), "one batch, retried as itself"
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

    stopper = _MeasuredThread(lambda: retire_process(worker, 0.5))
    stopper.start()
    deadline = time.monotonic() + 5.0
    while worker._exit_deadline is None and time.monotonic() < deadline:
        time.sleep(0.005)
    recorded_at = time.monotonic()
    assert worker._exit_deadline is not None, "the stop recorded its deadline"
    threading.Event().wait(max(0.0, recorded_at + 0.3 - time.monotonic()))
    sink.release.set()

    flusher.join(10.0)
    stopper.finish(10.0)
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


# --------------------------------------------------------------------------- FR-003


def test_an_unbounded_shutdown_retries_as_before(capsys) -> None:
    """FR-003 AC-1. `timeout=None` records `None`, which declines nothing."""
    sink = _SlowFailSink(0.2)
    worker = _worker(sink)
    worker.submit(_three_events())

    _shutdown_measured(worker, None)

    assert len(sink.attempts) == worker.max_retries + 1 == 4, sink.attempts
    assert "abandoned after 4 emit attempts" in capsys.readouterr().err


def test_ordinary_draining_records_no_deadline_and_retries_as_before() -> None:
    """FR-003 AC-2. A drain nobody asked to stop holds no deadline and makes every attempt."""
    sink = _SlowFailSink(0.0)
    worker = Worker(sink, batch_size=1, flush_interval=0.01)
    worker.submit([{"a": 1}])

    deadline = time.monotonic() + 5.0
    while worker.failed_batches == 0 and time.monotonic() < deadline:
        assert worker._exit_deadline is None, "no stop was requested, so nothing recorded one"
        time.sleep(0.005)

    assert worker.failed_batches == 1, "the premise: the batch ran out of attempts"
    assert worker._exit_deadline is None
    assert len(sink.attempts) == 4, sink.attempts
    worker.stop(5.0)


def test_flush_is_not_budgeted() -> None:
    """FR-003 AC-3. `flush(timeout=0.5)` times out on its own clock and the drain retries on."""
    sink = _SlowFailSink(0.4)
    worker = _worker(sink)
    worker.submit(_three_events())

    started = time.monotonic()
    result = worker.flush(timeout=0.5)
    elapsed = time.monotonic() - started

    assert result.reason == "timed-out", result
    assert 0.4 < elapsed < 0.5 + _SLACK, f"flush returned on its own timeout: {elapsed:.2f}"
    deadline = time.monotonic() + 5.0
    while worker.failed_batches == 0 and time.monotonic() < deadline:
        time.sleep(0.01)
    assert len(sink.attempts) == 4, f"the drain retried on after flush returned: {sink.attempts}"
    assert worker._exit_deadline is None, "a flush records no deadline"
    worker.stop(5.0)


def test_a_swap_mid_retry_declines_nothing(capsys) -> None:
    """FR-003 AC-4. `retarget`'s own deadline is a different thing and records none here.

    `retarget` reassigns `self.sink` whether or not the fence is confirmed, so the retries of
    the batch in flight reach the **new** sink after the swap: what this pins is that every one
    of them is made, across the two sinks, and that the exhausted line is the one written.
    """
    old = _SlowFailSink(0.3)
    new = _SlowFailSink(0.0)
    worker = Worker(old, batch_size=1, flush_interval=60.0)
    worker.submit([{"a": 1}])
    deadline = time.monotonic() + 5.0
    while not old.attempts and time.monotonic() < deadline:
        time.sleep(0.005)
    assert old.attempts, "the premise: the old sink is mid-retry"

    outcome = worker.retarget(new, deadline=time.monotonic() + 0.2)

    assert outcome.verdict == "unfenced", outcome
    assert worker._exit_deadline is None, "a swap records no exit deadline"
    settle = time.monotonic() + 5.0
    while worker.failed_batches == 0 and time.monotonic() < settle:
        time.sleep(0.01)
    attempts = len(old.attempts) + len(new.attempts)
    assert attempts == 4, f"no retry of that batch was declined: {old.attempts} {new.attempts}"
    err = capsys.readouterr().err
    assert "abandoned after 4 emit attempts" in err and "deadline" not in err, err
    assert any(held is old for held in worker._held), "the old sink is left for a later shutdown"
    assert worker.incomplete_swaps == 1
    worker.stop(5.0)


# --------------------------------------------------------------------------- FR-004


class _ParkedCloseSink:
    """An orphan-path sink whose first `close()` parks until released.

    The injected preemption point for the late-worker case: a `shutdown()` that found no
    worker is provably inside this close, past its first critical section, when the late
    worker is built. It declares `log_foundry_stop_signal` because `offer_stop_signal` probes
    for it with `hasattr`.
    """

    log_foundry_stop_signal: threading.Event | None = None

    def __init__(self) -> None:
        self.in_close = threading.Event()
        self.may_finish = threading.Event()
        self.closes = 0
        self._lock = threading.Lock()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Keeps nothing; this sink is here to be closed."""

    def close(self) -> None:
        """Parks the first close until the test releases it."""
        with self._lock:
            self.closes += 1
            first = self.closes == 1
        self.in_close.set()
        if first:
            self.may_finish.wait(10.0)


def _stop_mid_drain(timeout: float) -> tuple[_GatedSink, Worker, float]:
    """Lands a `shutdown(timeout)` while attempt 1 is in flight in `_drain`, then releases.

    `batch_size=1` so `_drain` emits by count; the stop is called 0.1 s after attempt 1
    starts, and the gate is released to fail at t_rec + 0.2, where t_rec is when the record was
    observed through the `_RecordingEvent` hook — so the decline margin does not depend on
    thread-start latency. Returns the sink, the worker and the shutdown's elapsed time.
    """
    event = _install_recording_event()
    sink = _GatedSink()
    worker = _worker(sink, batch_size=1)
    install_worker(worker)
    event.worker = worker
    worker.submit([{"a": 1}])
    assert sink.in_emit.wait(5.0)
    attempt_started = time.monotonic()
    threading.Event().wait(max(0.0, attempt_started + 0.1 - time.monotonic()))

    stopper = _MeasuredThread(lambda: retire_process(worker, timeout))
    stopper.start()
    assert event.recorded.wait(5.0), "the stop recorded its deadline before setting the event"
    recorded_at = time.monotonic()
    assert isinstance(event.seen[0], float), event.seen
    threading.Event().wait(max(0.0, recorded_at + 0.2 - time.monotonic()))
    sink.release.set()
    elapsed = stopper.finish(timeout + 10.0)
    worker._thread.join(5.0)
    return sink, worker, elapsed


def test_a_stop_landing_mid_sequence_in_drain_declines_the_retry() -> None:
    """FR-004 AC-1. One retry site serves `_drain` too: `0.3 + 0.3 + 0.1 > 0.6` declines."""
    sink, worker, elapsed = _stop_mid_drain(0.5)

    assert len(sink.attempts) == 1, sink.attempts
    assert elapsed < 0.5 + _SLACK, f"shutdown returned at {elapsed:.2f}"
    assert sink.closes == 1
    assert worker.stopped_reason is None


def test_the_same_ordering_with_a_wide_budget_makes_every_attempt() -> None:
    """FR-004 AC-2. The same stop with T=30 permits every attempt: the gate stays open."""
    sink, worker, _ = _stop_mid_drain(30.0)

    assert len(sink.attempts) == worker.max_retries + 1 == 4, sink.attempts
    assert sink.closes == 1


def test_a_late_worker_declines_against_the_remaining_budget() -> None:
    """FR-004 AC-3. A worker built during a `shutdown()` is stopped with what is left of T.

    The shutdown is parked inside the orphan sink's `close()`, past its first critical section,
    and the late worker is built over a 0.5 s failing sink. The orphan close is released at
    once, so the remaining budget is within a few ms of 1.5: decision 1 permits by at least 0.3,
    decision 2 declines by at least 0.1.
    """
    orphan = _ParkedCloseSink()
    log_foundry.configure(service="t", sink=orphan)
    log_foundry.info("arm the orphan record")

    shutdown = _MeasuredThread(lambda: log_foundry.shutdown(timeout=1.5))
    shutdown.start()
    assert orphan.in_close.wait(5.0), "the shutdown is inside the orphan close, past its latch"

    late_sink = _SlowFailSink(0.5)
    log_foundry.configure(sink=late_sink)

    @log_foundry.trace
    def work() -> int:
        return 1

    work()
    late = _lifecycle._state._late_worker
    assert late is not None, "the late worker was registered for the running shutdown"
    event = _RecordingEvent()
    event.worker = late
    late._stop = event
    with _lifecycle._state._lock:
        _lifecycle._state._stop = event

    orphan.may_finish.set()
    elapsed = shutdown.finish(30.0)
    late._thread.join(5.0)

    assert elapsed < 1.5 + _SLACK, f"shutdown(timeout=1.5) returned at {elapsed:.2f}"
    assert isinstance(event.seen[0], float), f"recorded before the late worker's set: {event.seen}"
    assert len(late_sink.attempts) == 2, (
        f"declined against the remaining budget: {late_sink.attempts}"
    )
    assert late_sink.closes == 1, "the second pass closed the late worker's sink"


def test_two_concurrent_shutdowns_follow_the_recorded_deadline() -> None:
    """FR-004 AC-4. Whichever caller wins the gate, the attempts match the deadline recorded.

    The two candidates are 0.9 and 1.8 s, decided by the nearer one rather than by "the first
    caller": `_stopped` and the record are one decision, so the recorded deadline is the joining
    caller's and `close()` runs once whichever wins.
    """
    sink = _SlowFailSink(0.5)
    worker = _worker(sink)
    install_worker(worker)
    worker.submit(_three_events())

    def work(index: int, _iteration: int) -> None:
        log_foundry.shutdown(timeout=(0.9, 1.8)[index])

    started = time.monotonic()
    errors = run_concurrently(work, threads=2)
    worker._thread.join(5.0)

    assert not errors, errors
    recorded = worker._exit_deadline
    assert isinstance(recorded, float)
    budget = recorded - started
    expected = 1 if budget < 1.35 else 3
    assert len(sink.attempts) == expected, (
        f"recorded budget {budget:.2f}s predicts {expected} attempt(s): {sink.attempts}"
    )
    assert sink.closes == 1, f"close() ran once, by the caller whose join matched: {sink.closes}"
    assert worker.stopped_reason is None, "and the recording caller's join did not expire"


def test_the_recording_caller_is_the_joining_caller_whichever_arrives_first(
    monkeypatch,
) -> None:
    """FR-004 AC-4 made deterministic: the gate and the record cannot go to different callers.

    Thread A claims `shutdown(timeout=1.8)` and is parked before its `stop`; the main thread's
    `shutdown(timeout=0.9)` then arrives, loses the claim, waits its own budget and returns; A is
    released and joins against 1.8. Against a mutant that records the deadline first-writer-wins
    but lets `stop` decide the gate on its own, the 0.9 caller does the join while the drain
    declines against 1.8: its join expires at 0.9 as attempt 2 ends at 1.0, `stopped_reason`
    reads `ShutdownTimeout` and `close()` never runs. `run_concurrently` reaches that
    interleaving about one time in twenty; the park reaches it every time.
    """
    sink = _SlowFailSink(0.5)
    worker = _worker(sink)
    install_worker(worker)
    worker.submit(_three_events())
    parked = threading.Event()
    release = threading.Event()
    real_stop = Worker.stop

    def parking_stop(
        self: Worker,
        timeout: float | None = worker_mod.DEFAULT_SHUTDOWN_TIMEOUT,
        *,
        claimed: bool | None = None,
    ) -> None:
        if threading.current_thread().name == "A":
            parked.set()
            assert release.wait(10.0)
        real_stop(self, timeout, claimed=claimed)

    monkeypatch.setattr(Worker, "stop", parking_stop)
    first = _MeasuredThread(lambda: _lifecycle._shutdown_worker(1.8), name="A")
    started = time.monotonic()
    first.start()
    assert parked.wait(5.0), "A claimed its stop and is parked before performing it"

    second = _measured(lambda: _lifecycle._shutdown_worker(0.9))
    release.set()
    elapsed = first.finish(10.0)
    worker._thread.join(5.0)

    recorded = worker._exit_deadline
    assert isinstance(recorded, float) and recorded - started > 1.5, "A's deadline governs"
    assert second < 0.9 + _SLACK, f"the losing caller returned within its own budget: {second:.2f}"
    assert elapsed < 1.8 + _SLACK, f"A returned within its own budget: {elapsed:.2f}"
    assert worker.stopped_reason is None, "the join belonged to the caller whose deadline governs"
    assert sink.closes == 1, f"and that caller closed the sink: {sink.closes}"


def test_shutdown_flush_and_health_together_leave_no_thread_blocked() -> None:
    """FR-004 AC-5. The new lock nesting deadlocks nowhere `run_concurrently` can reach."""
    sink = _SlowFailSink(0.2)
    worker = _worker(sink)
    install_worker(worker)
    worker.submit(_three_events())

    def work(index: int, _iteration: int) -> None:
        if index == 0:
            log_foundry.shutdown(timeout=1.0)
        elif index == 1:
            log_foundry.flush(timeout=1.0)
        else:
            log_foundry.health()

    errors = run_concurrently(work, threads=3)

    assert not errors, errors
    worker._thread.join(5.0)
    assert not worker._thread.is_alive()


def test_both_delivery_paths_under_one_shutdown_balance_the_ledger() -> None:
    """FR-004 AC-6. The worker path is budgeted; the orphan path keeps its single attempt.

    One always-failing 0.5 s sink serves both: an in-span `info()` reaches it through the
    worker, an out-of-span `info()` on a second thread reaches it directly. Attempts are tagged
    by thread name with a list append, never a shared counter.
    """
    sink = _SlowFailSink(0.5)
    log_foundry.configure(service="t", sink=sink)
    worker = _worker(sink)
    install_worker(worker)

    @log_foundry.trace
    def span() -> None:
        log_foundry.info("in span")

    span()
    orphan_elapsed: list[float] = []

    def orphan() -> None:
        started = time.monotonic()
        log_foundry.info("orphan")
        orphan_elapsed.append(time.monotonic() - started)

    caller = threading.Thread(target=orphan, name="orphan-caller")
    caller.start()
    elapsed = _shutdown_measured(worker, 1.2)
    caller.join(5.0)
    assert not caller.is_alive()
    worker._thread.join(5.0)
    health = log_foundry.health()

    assert elapsed < 1.2 + _SLACK, f"shutdown returned at {elapsed:.2f}"
    assert orphan_elapsed and orphan_elapsed[0] < 1.2 + _SLACK, (
        f"the orphan call ended within T plus slack: {orphan_elapsed}"
    )
    by_thread = {name: sink.attempts.count(name) for name in set(sink.attempts)}
    assert by_thread == {"log-foundry-worker": 2, "orphan-caller": 1}, by_thread
    orphan_delivered = sum(1 for e in sink.delivered if e.get("message") == "orphan")
    assert orphan_delivered + health.orphan_lost == 1, health
    worker_batch = next(batch for name, batch in sink.handed if name == "log-foundry-worker")
    lost_ends = sum(1 for e in worker_batch if e.get("message") == "span.end")
    delivered_ends = sum(1 for e in sink.delivered if e.get("message") == "span.end")
    assert delivered_ends + health.queued + health.dropped + lost_ends == 1, health
    assert health.failed_batches == 1
    assert sink.closes == 1


# --------------------------------------------------------------------------- FR-005


def _expire_on_a_wedged_batch(capsys) -> tuple[_GatedSink, Worker, str]:
    """Expires a `shutdown(timeout=0.5)` on a sink wedged in its first `_final_drain` emit.

    `batch_size=10` and a long `flush_interval` so nothing emits before the stop; the three
    events are inside the sink's first attempt when the join gives up. Returns the sink, the
    worker and stderr as it stood at the expiry, with the sink still parked.
    """
    sink = _GatedSink()
    worker = _worker(sink)
    worker.submit(_three_events())

    _shutdown_measured(worker, 0.5)

    assert sink.in_emit.is_set(), "the premise: the drain thread is inside the first emit"
    assert worker.stopped_reason == "ShutdownTimeout", "the premise: the join expired"
    return sink, worker, capsys.readouterr().err


def test_the_expiry_line_counts_the_batch_in_flight(capsys) -> None:
    """FR-005 AC-1 and AC-2. The batch inside the emit is named; the empty queue is not.

    Since SPEC-068 FR-001 the expiry line is a `_diag.pending` line rather than a `lost` one,
    and its queued clause appears only when a submission is queued, so here it has none.
    Releasing the sink to fail then declines the retry, and the `lost` figures across stderr
    sum to 3 events: the expiry line counted the batch as in flight, never as lost.
    """
    sink, worker, err = _expire_on_a_wedged_batch(capsys)

    assert _lost_lines(err) == [], "the expiry line announces no loss"
    assert err == (
        "log-foundry: shutdown timed out after 0.5s; the sink is left open because the worker "
        "thread is still using it; 3 event(s) in flight, counted when the emit returns\n"
    ), err

    sink.fail = True
    sink.release.set()
    worker._thread.join(5.0)
    later = capsys.readouterr().err

    assert len(sink.attempts) == 1, f"the retry was declined past the deadline: {sink.attempts}"
    assert worker.failed_batches == 1
    lost = _lost_lines(later)
    assert [(n, what) for n, what, _ in lost] == [(3, "event")], later
    assert "deadline" in lost[0][2], later
    assert sum(n for n, _, _ in _lost_lines(err + later)) == 3, "3 events, not 6"


def test_releasing_the_wedged_emit_to_succeed_announces_no_loss(capsys) -> None:
    """FR-005 AC-3. A late return can still deliver, which is why the line did not say lost."""
    sink, worker, err = _expire_on_a_wedged_batch(capsys)
    assert "3 event(s) in flight" in err

    sink.fail = False
    sink.release.set()
    worker._thread.join(5.0)

    assert len(sink.delivered) == 3
    assert worker.failed_batches == 0
    assert "lost" not in capsys.readouterr().err


def test_the_expiry_line_is_unchanged_when_nothing_is_in_flight(capsys) -> None:
    """FR-005 AC-4, as SPEC-068 FR-001 revised it: no clause when nothing is in flight or queued.

    The only deterministic route to "expired with nothing in flight" is to park the drain
    thread **before** `_emit` writes the count: `_emit_pending` calls `self._emit`, so an
    instance attribute over the method parks it there. Until SPEC-068 the line opened
    `lost 1 item(s); ` here, and the 1 was the shutdown sentinel `stop()` had just queued. The
    line is now a `_diag.pending` one, which claims no loss, and its queued clause counts
    submissions only, so the sentinel is not in it and the line ends after its first part.
    This supersedes SPEC-062 FR-005 AC-4's byte-for-byte promise against `71e42dd`.
    """
    sink = _SlowFailSink(0.0, fail_times=0)
    worker = Worker(sink, batch_size=1, flush_interval=60.0)
    parked = threading.Event()
    release = threading.Event()
    real_emit = worker._emit

    def gated_emit(event_lists: list[list[dict[str, object]]]) -> None:
        parked.set()
        release.wait(30.0)
        real_emit(event_lists)

    worker._emit = gated_emit  # type: ignore[method-assign]
    worker.submit([{"a": 1}])
    assert parked.wait(5.0), "the drain thread is parked before _emit runs"
    assert worker._in_flight == 0, "the premise: nothing is in flight"

    worker.stop(0.2)
    err = capsys.readouterr().err
    release.set()
    worker._thread.join(5.0)

    assert err == (
        "log-foundry: shutdown timed out after 0.2s; the sink is left open because the worker "
        "thread is still using it\n"
    )


class _InterruptingSink:
    """A sink whose `emit` raises a `BaseException`, which `_emit` does not absorb."""

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Raises straight through `_emit` to `_run`."""
        raise KeyboardInterrupt

    def close(self) -> None:
        """Nothing to close."""


@pytest.mark.parametrize("route", ["delivered", "abandoned", "declined", "escaping"])
def test_in_flight_is_zero_after_every_route(route: str) -> None:
    """FR-005 AC-5. `_in_flight` is reset in a `finally`, so an escaping exception resets it too."""
    sink: object
    timeout = 5.0
    if route == "delivered":
        sink = _SlowFailSink(0.0, fail_times=0)
    elif route == "abandoned":
        sink = _SlowFailSink(0.0)
    elif route == "declined":
        sink = _SlowFailSink(0.3)
        timeout = 0.5
    else:
        sink = _InterruptingSink()
    worker = _worker(sink)
    worker.submit(_three_events())

    worker.stop(timeout)
    worker._thread.join(5.0)

    assert not worker._thread.is_alive()
    assert worker._in_flight == 0, route
    expected = {
        "delivered": (0, None),
        "abandoned": (1, None),
        "declined": (1, None),
        "escaping": (0, "KeyboardInterrupt"),
    }[route]
    assert (worker.failed_batches, worker.stopped_reason) == expected, "the premise: the route ran"


@pytest.mark.parametrize("resume", [True, False])
def test_in_flight_is_reset_in_the_forked_child(resume: bool) -> None:
    """FR-005 AC-5, the fork. A child inherits the count for a drain thread it does not have.

    The deadline is reset only where a drain thread is started: a worker stopped by a direct
    `Worker.stop()` never moved the retirement count, so `_rebuild_worker_after_fork` resumes
    it, and without the reset every retry in that child would be declined forever. A retired
    child's record is left as it stands.
    """
    worker = _worker(_SlowFailSink(0.0, fail_times=0))
    worker.stop(5.0)
    assert not worker._thread.is_alive(), "the premise: the parent's drain thread has ended"
    worker._in_flight = 3
    worker._exit_deadline = 1.0

    worker._reinit_after_fork(resume=resume)
    try:
        assert worker._in_flight == 0
        assert worker._exit_deadline == (None if resume else 1.0)
    finally:
        # A resumed thread here inherits the stop event the parent's stop() set — a real fork
        # replaces that event before this handler runs — so its drain loop exits at once;
        # joining it is what proves nothing is leaked, on either branch.
        worker._thread.join(5.0)
        assert not worker._thread.is_alive(), "no drain thread outlives the test"
