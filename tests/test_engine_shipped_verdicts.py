"""SPEC-074 FR-001 — what the engine carries so a shipped sink keeps its 1.x counts and lines.

Every case runs against :class:`Shaped`, an in-test sink built the way a converted shipped sink
is: ``log_foundry_counters`` an instance attribute, ``_counter_lock`` a property over the
counters' lock, ``failed`` a property over ``failed_events`` (less ``dropped_unadjudicated`` for
the Kinesis shape) that stays assignable, and ``dropped_oversized`` / ``dropped_unadjudicated``
plain attributes written only through the run context. Each round is a step the test scripts,
given the batch, the run context and the sink. Serves invariants 2, 7 and 11.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any, Self

import pytest

import log_foundry
from conftest import install_worker
from log_foundry import _diag
from log_foundry.sinks import _engine
from log_foundry.sinks._engine import RunContext, When, emit_via_deliver
from log_foundry.sinks._rounds import Lost, mark_sent
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict
from log_foundry.worker import Worker

A, R, J, D, U = (
    Outcome.ACCEPTED,
    Outcome.RETRYABLE,
    Outcome.REJECTED,
    Outcome.DROPPED,
    Outcome.UNKNOWN,
)

Step = Callable[[list[dict[str, object]], RunContext, "Shaped"], Verdict]


def events(n: int) -> list[dict[str, object]]:
    """``n`` distinct events."""
    return [{"message": f"e{i}", "i": i} for i in range(n)]


class CountingLock:
    """A lock that counts its acquisitions and snapshots a reading at every release."""

    def __init__(self, snapshot: Callable[[], object] | None = None) -> None:
        self._lock = threading.Lock()
        self.acquisitions = 0
        self.snapshots: list[object] = []
        self._snapshot = snapshot

    def __enter__(self) -> Self:
        self._lock.acquire()
        self.acquisitions += 1
        return self

    def __exit__(self, *exc: object) -> None:
        if self._snapshot is not None:
            self.snapshots.append(self._snapshot())
        self._lock.release()

    def acquire(self, *args: Any, **kwargs: Any) -> bool:
        return self._lock.acquire(*args, **kwargs)

    def release(self) -> None:
        self._lock.release()


class Shaped:
    """A sink shaped as SPEC-074 converts a shipped one, whose rounds a test scripts."""

    log_foundry_stop_signal: threading.Event | None = None

    def __init__(self, script: list[Step], *, max_retries: int = 0, kinesis: bool = False) -> None:
        self.log_foundry_counters = SinkCounters()
        self.max_retries = max_retries
        self.dropped_oversized = 0
        self.dropped_unadjudicated = 0
        self._kinesis = kinesis
        self.script = script
        self.calls = 0

    @property
    def _counter_lock(self) -> Any:
        return self.log_foundry_counters._counter_lock

    @_counter_lock.setter
    def _counter_lock(self, lock: Any) -> None:
        self.log_foundry_counters._counter_lock = lock

    @property
    def failed(self) -> int:
        counters = self.log_foundry_counters
        return counters.failed_events - (self.dropped_unadjudicated if self._kinesis else 0)

    @failed.setter
    def failed(self, value: int) -> None:
        with self._counter_lock:
            extra = self.dropped_unadjudicated if self._kinesis else 0
            self.log_foundry_counters.failed_events = value + extra

    def losses(self) -> SinkLosses:
        return self.log_foundry_counters.losses()

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        return _engine.one_round(self._round, batch)

    def _round(self, batch: list[dict[str, object]]) -> Verdict:
        step = self.script[min(self.calls, len(self.script) - 1)]
        self.calls += 1
        return step(batch, _engine.run_context(), self)

    def emit(self, batch: list[dict[str, object]]) -> None:
        emit_via_deliver(self, batch)


def uniform(outcome: Outcome) -> Step:
    """A step answering every event with ``outcome``."""
    return lambda batch, context, sink: Verdict.uniform(outcome, len(batch))


def lost_lines(err: str) -> list[str]:
    return [line for line in err.splitlines() if line.startswith("log-foundry: lost")]


def _worker(sink: object, **kwargs: Any) -> Worker:
    kwargs.setdefault("batch_size", 10)
    kwargs.setdefault("flush_interval", 60.0)
    worker = Worker(sink, **kwargs)  # type: ignore[arg-type]
    install_worker(worker)
    return worker


# --- the sub-counts and their terms ---------------------------------------------------------


def _one_oversized(outcome: Outcome) -> Step:
    """Event 0 dropped as oversized, counted ``SETTLED``; the rest answered ``outcome``."""

    def step(batch: list[dict[str, object]], context: RunContext, sink: Shaped) -> Verdict:
        context.count(sink, "dropped_oversized", 1, when=When.SETTLED)
        return Verdict(outcomes=(D, *(outcome,) * (len(batch) - 1)))

    return step


def test_a_settled_drop_counts_its_sub_count_and_its_term() -> None:
    sink = Shaped([_one_oversized(A)])
    sink.emit(events(3))
    assert sink.dropped_oversized == 1
    assert sink.losses().dropped == 1


def test_a_failed_run_commits_neither() -> None:
    sink = Shaped([_one_oversized(J)])
    with pytest.raises(SinkDeliveryError):
        sink.emit(events(3))
    assert sink.dropped_oversized == 0
    assert sink.losses().dropped == 0


def _unadjudicated(batch: list[dict[str, object]], context: RunContext, sink: Shaped) -> Verdict:
    context.count(sink, "dropped_unadjudicated", 2, when=When.ALWAYS)
    return Verdict(outcomes=(A, U, U))


def test_an_always_count_beside_unknown_events() -> None:
    sink = Shaped([_unadjudicated])
    sink.emit(events(3))
    assert sink.dropped_unadjudicated == 2


def test_a_kinesis_shaped_failed_excludes_the_unadjudicated() -> None:
    sink = Shaped([_unadjudicated], kinesis=True)
    sink.emit(events(3))
    assert sink.failed == 0, "1.x's .failed never held the unadjudicated"
    assert sink.losses().failed == 2, "1.x's losses().failed summed the two"


@pytest.mark.parametrize("kinesis", [False, True])
def test_assigning_failed_writes_failed_events_under_the_lock(kinesis: bool) -> None:
    sink = Shaped([_unadjudicated], kinesis=kinesis)
    sink.emit(events(3))
    lock = CountingLock()
    sink._counter_lock = lock
    sink.failed = 0
    assert lock.acquisitions == 1
    assert sink.failed == 0
    expected = sink.dropped_unadjudicated if kinesis else 0
    assert sink.losses().failed == expected


# --- one lock, one commit ---------------------------------------------------------------------


def test_a_counting_lock_on_the_sink_is_the_counters_lock() -> None:
    sink = Shaped([_one_oversized(A)])
    lock = CountingLock()
    sink._counter_lock = lock
    assert sink.log_foundry_counters._counter_lock is lock


def test_a_counting_run_and_a_losses_read_take_one_acquisition_each() -> None:
    sink = Shaped([_one_oversized(A)])
    lock = CountingLock()
    sink._counter_lock = lock
    sink.emit(events(3))
    assert lock.acquisitions == 1, "the sub-count and the totals commit under one acquisition"
    sink.losses()
    assert lock.acquisitions == 2


def _paired(sink: Shaped) -> tuple[int, int]:
    return sink.log_foundry_counters.dropped_events, sink.dropped_oversized


def _poll(sink: Shaped, runs: int) -> tuple[list[tuple[int, int]], list[object]]:
    """Runs ``runs`` commits while a reader polls under the lock; returns both readings."""
    seen: list[tuple[int, int]] = []
    done = threading.Event()
    lock = CountingLock(snapshot=lambda: _paired(sink))
    sink._counter_lock = lock

    def reader() -> None:
        while not done.is_set():
            with lock:
                seen.append(_paired(sink))

    thread = threading.Thread(target=reader)
    thread.start()
    try:
        for _ in range(runs):
            sink.emit(events(1))
    finally:
        done.set()
        thread.join(10.0)
    return seen, lock.snapshots


def _drop_one(batch: list[dict[str, object]], context: RunContext, sink: Shaped) -> Verdict:
    context.count(sink, "dropped_oversized", 1, when=When.SETTLED)
    return Verdict.uniform(D, len(batch))


def test_a_poller_never_sees_a_half_commit() -> None:
    sink = Shaped([_drop_one])
    seen, released = _poll(sink, 1000)
    assert sink.dropped_oversized == 1000
    assert all(a == b for a, b in seen), "a reader saw the two writes from different commits"
    assert all(a == b for a, b in released), "a release left the two writes apart"  # type: ignore[misc]


def test_a_planted_second_acquisition_is_caught(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_engine, "_shares_lock", lambda owner, counters: False)
    sink = Shaped([_drop_one])
    _, released = _poll(sink, 20)
    assert any(a != b for a, b in released), "the mutant's split commit went unseen"  # type: ignore[misc]


# --- the attempt count of a line ------------------------------------------------------------


def _carried(batch: list[dict[str, object]], context: RunContext, sink: Shaped) -> Verdict:
    """Round 1 sends only event 0 (event 1 held); later rounds send both; all stay retryable."""
    sent = batch if context.round > 1 else [event for event in batch if event["i"] == 0]
    mark_sent(sink, sent)
    lost = Lost(sink)
    lost.add(
        batch,
        when=When.LAST,
        key="retryable",
        line=lambda n, count: _diag.lost("event", count, f"Shaped, {n} attempt(s), Throttled"),
    )
    lost.register()
    return Verdict.uniform(R, len(batch))


def test_each_line_names_the_calls_that_carried_its_events(
    capsys: pytest.CaptureFixture[str],
) -> None:
    sink = Shaped([_carried], max_retries=2)
    with pytest.raises(SinkDeliveryError):
        sink.emit(events(2))
    assert lost_lines(capsys.readouterr().err) == [
        "log-foundry: lost 1 event(s); Shaped, 3 attempt(s), Throttled",
        "log-foundry: lost 1 event(s); Shaped, 2 attempt(s), Throttled",
    ]


def test_a_later_accepted_round_writes_no_line(capsys: pytest.CaptureFixture[str]) -> None:
    sink = Shaped([_carried, _carried, uniform(A)], max_retries=2)
    sink.emit(events(2))
    assert lost_lines(capsys.readouterr().err) == []
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


# --- final rejections -------------------------------------------------------------------------


def _rejecting(marked: set[int], outcomes: tuple[Outcome, ...] = (J, J)) -> Step:
    def step(batch: list[dict[str, object]], context: RunContext, sink: Shaped) -> Verdict:
        context.final_rejections(marked)
        return Verdict(outcomes=outcomes)

    return step


def test_final_rejections_settle_a_run_lost_only_to_them() -> None:
    sink = Shaped([_rejecting({0, 1})])
    sink.emit(events(2))
    assert sink.log_foundry_counters.failed_events == 2


def test_one_unmarked_rejection_fails_the_run() -> None:
    sink = Shaped([_rejecting({0})])
    with pytest.raises(SinkDeliveryError):
        sink.emit(events(2))


def test_marking_a_non_rejected_index_is_an_escaped_value_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    sink = Shaped([_rejecting({0}, (A, J))])
    with pytest.raises(SinkDeliveryError, match="raised 2"):
        sink.emit(events(2))
    assert "ValueError" in capsys.readouterr().err


def test_a_marked_verdict_still_reads_through_outcomes_of() -> None:
    verdict = Verdict(outcomes=(J, J))
    assert _engine.outcomes_of(verdict, 2) == (J, J)
    assert _engine.outcomes_of(verdict, 3) is None


# --- the refusal marker -----------------------------------------------------------------------


class Refused(Exception):
    """The exception a 1.x ``emit`` raised for the refusal."""


def _refusing(outcome: Outcome, error: Exception) -> Step:
    def step(batch: list[dict[str, object]], context: RunContext, sink: Shaped) -> Verdict:
        context.refuse(error)
        return Verdict.uniform(outcome, len(batch))

    return step


def test_a_rejected_refusal_raises_its_error_by_identity_and_counts_nothing() -> None:
    error = Refused("closed")
    sink = Shaped([_refusing(J, error)], max_retries=3)
    with pytest.raises(Refused) as info:
        sink.emit(events(2))
    assert info.value is error
    assert sink.calls == 1
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


def test_a_rejected_refusal_is_one_failed_batch_on_the_worker_path() -> None:
    sink = Shaped([_refusing(J, Refused("closed"))], max_retries=3)
    worker = _worker(sink, max_retries=2)
    worker.submit(events(2))
    assert not worker.flush(timeout=20.0)
    assert sink.calls == 1
    assert worker.failed_batches == 1
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


def test_a_rejected_refusal_is_one_orphan_loss() -> None:
    sink = Shaped([_refusing(J, Refused("closed"))])
    log_foundry.configure(service="t", sink=sink)
    before = log_foundry.health().orphan_lost
    log_foundry.info("orphan")
    assert sink.calls == 1
    assert log_foundry.health().orphan_lost == before + 1
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


def test_a_retryable_refusal_is_resent_then_raises_its_error() -> None:
    error = Refused("disconnected")
    sink = Shaped([_refusing(R, error)], max_retries=2)
    with pytest.raises(Refused) as info:
        sink.emit(events(2))
    assert info.value is error
    assert sink.calls == 3
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


def test_a_refusal_then_an_acceptance_counts_nothing() -> None:
    sink = Shaped([_refusing(R, Refused("x")), uniform(A)], max_retries=2)
    sink.emit(events(2))
    assert sink.calls == 2
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


def test_a_refusal_after_a_landed_round_settles_with_one_line(
    capsys: pytest.CaptureFixture[str],
) -> None:
    def first(batch: list[dict[str, object]], context: RunContext, sink: Shaped) -> Verdict:
        return Verdict(outcomes=(A, R, R))

    sink = Shaped([first, _refusing(J, Refused("closed"))], max_retries=2)
    sink.emit(events(3))
    assert sink.losses() == SinkLosses(dropped=0, failed=2)
    assert lost_lines(capsys.readouterr().err) == [
        "log-foundry: lost 2 event(s); refused by Shaped after others landed, Refused"
    ]


# --- sink-counted rounds ----------------------------------------------------------------------


def _counted(batch: list[dict[str, object]], context: RunContext, sink: Shaped) -> Verdict:
    context.sink_counted(range(len(batch)))
    context.count(sink.log_foundry_counters, "failed_events", 1, when=When.ALWAYS)
    context.announce(
        lambda: _diag.lost("event", 1, "Shaped publish, RuntimeError"), when=When.ALWAYS
    )
    return Verdict.uniform(R, len(batch))


def test_sink_counted_rounds_count_only_what_the_sink_registered(
    capsys: pytest.CaptureFixture[str],
) -> None:
    sink = Shaped([_counted])
    worker = _worker(sink, max_retries=2)
    worker.submit(events(3))
    assert not worker.flush(timeout=20.0)
    assert sink.calls == 3
    assert sink.failed == 3
    assert worker.failed_batches == 1
    assert lost_lines(capsys.readouterr().err) == [
        "log-foundry: lost 1 event(s); Shaped publish, RuntimeError",
        "log-foundry: lost 1 event(s); Shaped publish, RuntimeError",
        "log-foundry: lost 1 event(s); Shaped publish, RuntimeError",
        "log-foundry: lost 3 event(s); batch abandoned after 3 emit attempts",
    ]


def test_a_sink_counted_round_then_an_acceptance(capsys: pytest.CaptureFixture[str]) -> None:
    sink = Shaped([_counted, uniform(A)])
    worker = _worker(sink, max_retries=2)
    worker.submit(events(3))
    assert worker.flush(timeout=20.0)
    assert sink.failed == 1
    assert lost_lines(capsys.readouterr().err) == [
        "log-foundry: lost 1 event(s); Shaped publish, RuntimeError"
    ]


def test_sink_counting_a_rejected_index_is_an_escaped_value_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    def wrong(batch: list[dict[str, object]], context: RunContext, sink: Shaped) -> Verdict:
        context.sink_counted({0})
        return Verdict.uniform(J, len(batch))

    sink = Shaped([wrong])
    with pytest.raises(SinkDeliveryError, match="raised 2"):
        sink.emit(events(2))
    assert "ValueError" in capsys.readouterr().err


# --- a BaseException ------------------------------------------------------------------------


def test_a_keyboard_interrupt_from_deliver_commits_nothing() -> None:
    def interrupt(batch: list[dict[str, object]], context: RunContext, sink: Shaped) -> Verdict:
        context.count(sink, "dropped_oversized", 1, when=When.ALWAYS)
        context.count(sink.log_foundry_counters, "failed_events", 1, when=When.ALWAYS)
        raise KeyboardInterrupt

    sink = Shaped([interrupt])
    with pytest.raises(KeyboardInterrupt):
        sink.emit(events(2))
    assert sink.dropped_oversized == 0
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


def test_marks_outside_any_run_do_nothing() -> None:
    context = _engine.run_context()
    context.final_rejections({0})
    context.refuse(Refused("x"))
    context.sink_counted({0})
    assert context._marks() == ((), None, ())


# --- frame-2 review: the one-lock commit is all or nothing ----------------------------------------


class _Misspelt:
    """A sink sharing its counters' lock that registers a sub-count under a name it lacks."""

    max_retries = 0

    def __init__(self) -> None:
        self.log_foundry_counters = SinkCounters()
        self.dropped_oversized = 0

    @property
    def _counter_lock(self) -> threading.Lock:
        return self.log_foundry_counters._counter_lock

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        _engine.run_context().count(self, "dropped_oversize", 1, when=When.ALWAYS)
        return Verdict.uniform(Outcome.REJECTED, len(batch))

    def emit(self, batch: list[dict[str, object]]) -> None:
        emit_via_deliver(self, batch)

    def losses(self) -> SinkLosses:
        return self.log_foundry_counters.losses()


def test_a_misspelt_sub_count_commits_nothing_so_the_line_is_true(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The engine commits a sink's sub-counts with its totals in one ``Deferral``.

    One that names no attribute used to leave ``failed_events`` committed while the line said
    the whole total was not counted (invariant 11); the commit now reads every value before it
    writes any, so the counters and the line agree.
    """
    sink = _Misspelt()
    _engine.run_delivery(sink, [{"i": 0}, {"i": 1}], worker_retries=0, stop=None)
    assert sink.losses() == SinkLosses(dropped=0, failed=0)
    assert sink.dropped_oversized == 0
    err = capsys.readouterr().err
    assert "(AttributeError); 2 event(s) not counted" in err, err
