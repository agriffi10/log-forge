"""SPEC-090 FR-001 and FR-005 AC1: a batch grows while a backlog waits, and no setting is added.

A batch grows past ``batch_size`` submissions only while more are waiting, by whole submissions,
toward a size target the drain measures from its sink's own rate: half a flush interval of it,
never past ``_BACKLOG_BATCH_CAP`` events. With nothing waiting it is the count trigger's batch.

**How a test gets a known target.** :func:`_plugged` sends one 1,000-event submission through a
``flush()`` into a held sink call, and the test queues its backlog behind it while it is held.
With a 60 s interval the plug's measurement is ``1000 / elapsed * 30``, at or above the cap for
any hold under 30 s, so the target is the cap without any test writing ``_target``. Each hold is
released only once the backlog behind it is queued, so the drain sees the whole backlog waiting.
"""

from __future__ import annotations

import inspect
import json
import threading
import time
from typing import Any

import pytest

import ledger_check
import log_foundry
from conftest import install_worker, retire_process
from log_foundry import worker as worker_mod
from log_foundry.worker import Worker, _FlushMarker
from test_fork_lifecycle import run_in_child

_CAP = 1_000
_PAUSE = 0.002


class _Sink:
    """Records each batch it is handed, and holds or fails the calls a test names.

    Attributes:
      handed: The size of every batch handed to ``emit``, failed ones included, in call order.
      batches: Every batch whose ``emit`` returned, as handed.
      threads: The thread each call ran on.
      fail: Call numbers, from 1, that raise after any hold.
      per_event: Seconds the call idles per event it was handed, once per call.
      longest: The longest call, in seconds.
      clock: A fake clock to advance by ``per_event`` per event instead of idling, or ``None``.
    """

    def __init__(self, per_event: float = 0.0, clock: _Clock | None = None) -> None:
        self.handed: list[int] = []
        self.batches: list[list[dict[str, object]]] = []
        self.threads: list[str] = []
        self.fail: set[int] = set()
        self.per_event = per_event
        self.longest = 0.0
        self.clock = clock
        self.watch: _FlushMarker | None = None
        self.answered: list[bool] = []
        self._holds: dict[int, threading.Event] = {}
        self._entered: dict[int, threading.Event] = {}
        self._lock = threading.Lock()

    def hold(self, call: int) -> threading.Event:
        """Holds call number ``call`` until the returned event is set."""
        gate = threading.Event()
        with self._lock:
            self._holds[call] = gate
        return gate

    def entered(self, call: int) -> threading.Event:
        """The event set once call number ``call`` has begun."""
        with self._lock:
            return self._entered.setdefault(call, threading.Event())

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Records the call, holds it if asked, idles per event, then fails or records it."""
        with self._lock:
            self.handed.append(len(batch))
            self.threads.append(threading.current_thread().name)
            call = len(self.handed)
            gate = self._holds.get(call)
            entered = self._entered.setdefault(call, threading.Event())
            if self.watch is not None:
                self.answered.append(self.watch.event.is_set())
        entered.set()
        if gate is not None:
            gate.wait(30.0)
        began = time.monotonic()
        if self.clock is not None:
            self.clock.advance(self.per_event * len(batch))
        elif self.per_event:
            threading.Event().wait(self.per_event * len(batch))
        self.longest = max(self.longest, time.monotonic() - began)
        if call in self.fail:
            raise ConnectionError("down")
        with self._lock:
            self.batches.append(list(batch))

    def close(self) -> None:
        """Releases nothing."""

    def events(self) -> int:
        """Events in every batch whose ``emit`` returned."""
        with self._lock:
            return sum(len(batch) for batch in self.batches)

    def sizes(self) -> list[int]:
        """The size of every batch whose ``emit`` returned."""
        with self._lock:
            return [len(batch) for batch in self.batches]


class _Clock:
    """A ``time.monotonic`` stand-in that moves only when a sink advances it."""

    def __init__(self) -> None:
        self.now = 1_000.0
        self._lock = threading.Lock()

    def __call__(self) -> float:
        """Reads the fake time."""
        return self.now

    def advance(self, seconds: float) -> None:
        """Moves the fake time on."""
        with self._lock:
            self.now += seconds


def _span(seq: int, events: int = 2) -> list[dict[str, object]]:
    """One submission of ``events`` events, each tagged with ``seq``."""
    return [{"message": "m", "seq": seq, "i": i} for i in range(events)]


def _until(predicate: Any, seconds: float = 20.0) -> None:
    """Waits until ``predicate()`` holds, counting pauses rather than reading a clock."""
    for _ in range(int(seconds / _PAUSE)):
        if predicate():
            return
        threading.Event().wait(_PAUSE)
    raise AssertionError(f"timed out waiting for {predicate}")


def _parked(worker: Worker) -> bool:
    """Whether the drain is waiting in ``queue.get`` on an empty queue: at rest."""
    q = worker._queue
    with q.mutex:
        return bool(q.not_empty._waiters) and not q.queue  # type: ignore[attr-defined]


def _plugged(
    sink: _Sink, *, batch_size: int = 10, max_retries: int = 3, plug: int = _CAP
) -> tuple[Worker, threading.Event]:
    """Builds and installs a 60 s worker whose first call, a ``plug``-event batch, is held."""
    gate = sink.hold(1)
    worker = Worker(sink, batch_size=batch_size, flush_interval=60.0, max_retries=max_retries)
    install_worker(worker)
    worker.submit(_span(-1, events=plug))
    worker.flush(0)
    assert sink.entered(1).wait(10), "the premise: the plug is inside the held call"
    return worker, gate


# ---------------------------------------------------------------------------- FR-001 rule 1


def test_nothing_waiting_sends_ten_spans_per_batch_as_today() -> None:
    """FR-001 AC4, first half: one span every 30 ms for 100 spans goes in ten batches of 20.

    The submitter waits for the drain to be parked on an empty queue before each next span, so
    a loaded runner cannot queue two behind a count trigger: with nothing waiting the count
    trigger fires at ten, whatever the size target the first batch measured.
    """
    sink = _Sink()
    worker = Worker(sink, batch_size=10, flush_interval=60.0)
    install_worker(worker)
    for seq in range(100):
        threading.Event().wait(0.03)
        worker.submit(_span(seq))
        _until(lambda: _parked(worker))
    _until(lambda: sink.events() == 200)
    assert sink.sizes() == [20] * 10


def test_a_tenth_lone_submission_is_emitted_at_ten_whatever_the_target() -> None:
    """FR-001 AC4, second half: with the target at the cap, ten lone spans are one batch of 20."""
    sink = _Sink()
    worker, gate = _plugged(sink)
    gate.set()
    _until(lambda: _parked(worker) and sink.events() == _CAP)
    assert worker._size_target() == _CAP, "the premise: the plug measured the cap"
    for seq in range(10):
        worker.submit(_span(seq))
        _until(lambda: _parked(worker))
    _until(lambda: sink.events() == _CAP + 20)
    assert sink.sizes() == [_CAP, 20]


# ---------------------------------------------------------------------------- FR-001 rule 2-3


def test_the_cap_is_a_ceiling_and_a_crossing_submission_is_carried() -> None:
    """FR-001 AC5: 499 two-event spans, a 600-event span and ten more go as 998 then 620.

    The 600-event span would take the batch past the cap, so the batch is emitted without it
    and it begins the next, which the ten small spans then join.
    """
    sink = _Sink()
    worker, gate = _plugged(sink)
    for seq in range(499):
        worker.submit(_span(seq))
    worker.submit(_span(499, events=600))
    for seq in range(500, 510):
        worker.submit(_span(seq))
    gate.set()
    _until(lambda: sink.events() == _CAP + 998 + 620)
    assert sink.sizes() == [_CAP, 998, 620]
    assert max(sink.sizes()) <= _CAP


def test_a_carried_submission_is_in_flight_from_its_take() -> None:
    """FR-001's last paragraph: a carried submission is counted where it is taken.

    The carry's own emit is held, and inside it the worker balances on its first reading,
    with ``in_flight`` holding the batch being emitted and the carried submission both.
    """
    sink = _Sink()
    worker, gate = _plugged(sink)
    carry = sink.hold(2)
    for seq in range(499):
        worker.submit(_span(seq))
    worker.submit(_span(499, events=600))
    gate.set()
    assert sink.entered(2).wait(10), "the premise: the carry's emit is inside the sink"
    try:
        assert sink.handed[1] == 998, "the premise: call 2 is the carry"
        assert ledger_check.worker_problems(worker, settle=0.0) == []
        assert log_foundry.health().events.in_flight == 998 + 600
    finally:
        carry.set()
    assert worker.flush(10)
    assert sink.sizes() == [_CAP, 998, 600]


def test_a_carry_emit_restarts_the_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    """A carry emit restarts the time trigger (plan review, 2026-11-16).

    A fake clock moves only inside the sink, 1 ms per event, so each batch of a three-event
    backlog measures the same half-interval target of about 500 events and every batch after
    the first is a carry of 498. Had a carry left ``last_flush`` where the first emit put it,
    the time trigger would fire on the take after the second carry and send one span alone; no
    batch but the last may hold fewer than ``batch_size`` submissions.
    """
    clock = _Clock()
    monkeypatch.setattr(time, "monotonic", clock)
    sink = _Sink(per_event=0.001, clock=clock)
    gate = sink.hold(1)
    worker = Worker(sink, batch_size=10, flush_interval=1.0)
    install_worker(worker)
    for seq in range(1_000):
        worker.submit(_span(seq, events=3))
    assert sink.entered(1).wait(10)
    gate.set()
    _until(lambda: sink.events() == 3_000)
    monkeypatch.undo()
    sizes = sink.sizes()
    assert len(sizes) > 4, f"the premise: the backlog went in several carries: {sizes}"
    assert all(size >= 30 for size in sizes[:-1]), sizes


# ---------------------------------------------------------------------------- FR-001 rule 4


def test_a_per_event_sink_is_never_held_past_one_second() -> None:
    """FR-001 AC6: a 2 ms-per-event sink sent 200 spans at once gets all 400, each call short.

    The sink idles once per call for 2 ms times the events it was handed, so a batch grown past
    half an interval's worth would hold it past the interval. Some batch must also grow past ten
    spans, or this would pass on a tree without the rule.
    """
    sink = _Sink(per_event=0.002)
    gate = sink.hold(1)
    worker = Worker(sink)
    install_worker(worker)
    for seq in range(200):
        worker.submit(_span(seq))
    assert sink.entered(1).wait(10)
    gate.set()
    _until(lambda: sink.events() == 400, seconds=30.0)
    assert sink.longest < 1.0, f"an emit held the sink {sink.longest:.3f}s"
    assert max(sink.sizes()) > 20, sink.sizes()


def test_a_frozen_clock_gives_the_cap_and_the_drain_survives(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-001 AC7: with ``time.monotonic`` constant, 2,000 spans arrive whole, none past the cap.

    Every emit then measures zero elapsed, which gives the cap rather than dividing by zero on
    the drain thread, where an exception is terminal.
    """
    monkeypatch.setattr(time, "monotonic", lambda: 1_000.0)
    sink = _Sink()
    gate = sink.hold(1)
    worker = Worker(sink, batch_size=10, flush_interval=1.0)
    install_worker(worker)
    for seq in range(2_000):
        worker.submit(_span(seq))
    assert sink.entered(1).wait(10)
    gate.set()
    _until(lambda: sink.events() == 4_000 or not worker._thread.is_alive())
    monkeypatch.undo()
    assert sink.events() == 4_000
    assert max(sink.sizes()) <= _CAP
    assert sink.sizes()[1] == _CAP, "the zero-elapsed measurement gave the cap"
    assert worker.stopped_reason is None
    assert log_foundry.health().stopped_reason is None


# ---------------------------------------------------------------------------- FR-001 rule 4-6


@pytest.mark.parametrize("after", ["abandoned", "configure", "straddled"])
def test_the_next_backlog_batch_is_todays_after(
    after: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-001 AC8: after an abandoned batch, a ``configure(sink=...)`` or a straddled swap.

    The batch after each holds exactly ``batch_size`` submissions though a backlog is waiting
    behind it, and the batch after that is measured afresh and grows.
    """
    old = _Sink()
    worker, gate = _plugged(old, max_retries=0)
    if after == "abandoned":
        old.fail.add(2)
        held = old.hold(2)
        for seq in range(100):
            worker.submit(_span(seq))
        gate.set()
        assert old.entered(2).wait(10)
        for seq in range(100, 200):
            worker.submit(_span(seq))
        held.set()
        _until(lambda: old.events() == _CAP + 200)
        assert old.handed == [_CAP, 200, 20, 180]
        return
    new = _Sink()
    if after == "configure":
        gate.set()
        _until(lambda: _parked(worker) and old.events() == _CAP)
        assert worker._size_target() == _CAP, "the premise: the old sink measured the cap"
        log_foundry.configure(sink=new)
        assert worker.sink is new, "the premise: configure retargeted this worker"
        held = new.hold(1)
        for seq in range(100):
            worker.submit(_span(seq))
        assert new.entered(1).wait(10)
        held.set()
    else:
        outcome = worker.retarget(new, deadline=time.monotonic() + 0.05)
        assert outcome.verdict == "unfenced", "the premise: the swap straddled the held emit"
        for seq in range(100):
            worker.submit(_span(seq))
        gate.set()
    _until(lambda: new.events() == 200)
    capsys.readouterr()
    assert new.handed == [20, 180]


def test_a_forked_childs_first_backlog_batch_is_todays() -> None:
    """FR-001 AC8, the fork: a child's first batch under a backlog holds ``batch_size`` spans.

    The parent is plugged to the cap first, so a child that inherited the target would grow its
    first batch; the child reports the sizes its sink was handed.
    """
    sink = _Sink()
    worker, gate = _plugged(sink)
    gate.set()
    _until(lambda: _parked(worker) and sink.events() == _CAP)
    assert worker._size_target() == _CAP, "the premise: the parent measured the cap"

    def work() -> str:
        sink.handed.clear()
        sink.batches.clear()
        target = worker._target
        for seq in range(100):
            worker.submit(_span(seq))
        _until(lambda: sink.events() == 200, seconds=10.0)
        return json.dumps({"target": target, "handed": sink.handed})

    child = run_in_child(work)
    assert child.finished, child.output
    report = json.loads(child.output)
    assert report["target"] == 0.0
    assert report["handed"][0] == 20, report


@pytest.mark.parametrize("resume", [True, False])
def test_the_batching_fields_are_reset_in_the_forked_child(resume: bool) -> None:
    """FR-001 rule 6 and AC11: every field this spec adds is reset but the sink generation."""
    worker = Worker(_Sink(), batch_size=10, flush_interval=60.0)
    worker.stop(5.0)
    assert not worker._thread.is_alive(), "the premise: the parent's drain thread has ended"
    worker._pending_events = 7
    worker._target = 500.0
    worker._target_generation = 3
    worker._sink_generation = 7
    worker._exit_batches = 2
    worker._exit_longest = 0.4
    worker._exit_declining = True
    worker._exit_abandoned_seen = True
    worker._abandoned_tally = (40, 2)
    worker._declined_tally = (60, 3)

    worker._reinit_after_fork(resume=resume)
    try:
        assert (worker._pending_events, worker._target) == (0, 0.0)
        assert (worker._sink_generation, worker._target_generation) == (7, 7)
        assert (worker._exit_batches, worker._exit_longest) == (0, 0.0)
        assert (worker._exit_declining, worker._exit_abandoned_seen) == (False, False)
        assert (worker._abandoned_tally, worker._declined_tally) == ((0, 0), (0, 0))
    finally:
        if resume:
            worker.stop(2.0)
        worker._thread.join(5.0)
        assert not worker._thread.is_alive(), "no drain thread outlives the test"


# ---------------------------------------------------------------------------- FR-001 markers


def test_a_flush_behind_a_backlog_is_answered_after_the_batches_ahead_of_it() -> None:
    """FR-001 AC9: a ``flush()`` behind a backlog is answered once the events ahead of it go.

    The sink records, at each call, whether the marker had been answered: every batch carrying
    events ahead of the marker arrives before it is, the first batch behind it after, and no
    batch holds events from both sides.
    """
    sink = _Sink()
    worker, gate = _plugged(sink)
    for seq in range(600):
        worker.submit(_span(seq))
    result: list[object] = []
    flusher = threading.Thread(target=lambda: result.append(worker.flush(30)))
    flusher.start()

    def queued_marker() -> _FlushMarker | None:
        with worker._queue.mutex:
            found = [item for item in worker._queue.queue if type(item) is _FlushMarker]
        return found[-1] if found else None

    _until(lambda: queued_marker() is not None)
    sink.watch = queued_marker()
    for seq in range(600, 900):
        worker.submit(_span(seq))
    gate.set()
    flusher.join(30)
    _until(lambda: sink.events() == _CAP + 1_800)
    assert result and result[0], result
    sides = [{int(event["seq"]) < 600 for event in batch} for batch in sink.batches[1:]]
    assert all(len(side) == 1 for side in sides), "a batch held events from both sides"
    pairs = list(zip(sides, sink.answered, strict=True))
    ahead = [answered for side, answered in pairs if True in side]
    behind = [answered for side, answered in pairs if False in side]
    assert ahead and not any(ahead), "a batch ahead of the marker arrived after it was answered"
    assert behind and behind[0], "the first batch behind the marker arrived before it was answered"


def test_a_patched_cap_of_fifty_sends_batches_of_exactly_fifty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-001 AC10 and rule 7: the cap is read at each use, so a patched cap of 50 binds."""
    monkeypatch.setattr(worker_mod, "_BACKLOG_BATCH_CAP", 50)
    sink = _Sink()
    gate = sink.hold(1)
    worker = Worker(sink, batch_size=10, flush_interval=60.0)
    install_worker(worker)
    for seq in range(10 + 25 * 20):
        worker.submit(_span(seq))
    assert sink.entered(1).wait(10)
    gate.set()
    _until(lambda: sink.events() == 2 * (10 + 25 * 20))
    assert sink.sizes() == [20] + [50] * 20


# ---------------------------------------------------------------------------- FR-001 AC3


def test_take_complete_and_the_measurement_run_only_on_the_drain_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-001 AC3: every call of ``_take``, ``_complete`` and ``_set_target`` is the drain's.

    Driven through a steady backlog and an exit backlog, so both drains are seen; the static
    half is ``tests/test_ledger_write_sites.py``'s drain-only set.
    """
    seen: dict[str, set[str]] = {"_take": set(), "_complete": set(), "_set_target": set()}
    for name in seen:
        real = getattr(Worker, name)

        def wrapped(
            self: Worker, *args: Any, _real: Any = real, _name: str = name, **kw: Any
        ) -> Any:
            seen[_name].add(threading.current_thread().name)
            return _real(self, *args, **kw)

        monkeypatch.setattr(Worker, name, wrapped)
    sink = _Sink()
    worker, gate = _plugged(sink)
    for seq in range(300):
        worker.submit(_span(seq))
    held = sink.hold(2)
    gate.set()
    assert sink.entered(2).wait(10)
    for seq in range(300, 330):
        worker.submit(_span(seq))
    stopper = threading.Thread(target=retire_process, args=(worker, 10.0))
    stopper.start()
    _until(worker._stop.is_set)
    held.set()
    stopper.join(15)
    assert sink.events() == _CAP + 660
    assert all(threads == {"log-foundry-worker"} for threads in seen.values()), seen


# ---------------------------------------------------------------------------- FR-005 AC1

_SIGNATURES = {
    "configure": (
        "(*, service: 'str | None' = None, version: 'str | None' = None, env: 'str | None' = "
        "None, sink: 'Sink | None' = None, defaults: 'Mapping[str, object] | None' = None, "
        "max_value_bytes: 'int | None' = None, max_stack_bytes: 'int | None' = None, max_keys: "
        "'int | None' = None, max_depth: 'int | None' = None, sink_factory: 'Callable[[], Sink] "
        "| None' = None) -> 'None'"
    ),
    "Worker.__init__": (
        "(self, sink: 'Sink', *, batch_size: 'int' = 10, flush_interval: 'float' = 1.0, "
        "max_queue: 'int' = 10000, max_retries: 'int' = 3, offer_signal: 'bool' = True) -> "
        "'None'"
    ),
    "flush": "(timeout: float | None = 5.0) -> log_foundry.results.FlushResult",
    "shutdown": "(timeout: float | None = 30.0) -> None",
    "health": "() -> log_foundry.worker.Health",
}
"""The five signatures FR-005 AC1 names, captured at ``75e7bf1e``, before this spec's build."""

_ALL = [
    "Config",
    "ContinueResult",
    "DEFAULT_SHUTDOWN_TIMEOUT",
    "DEFAULT_SWAP_TIMEOUT",
    "EventLedger",
    "FlushResult",
    "Health",
    "Outcome",
    "Sink",
    "SinkCounters",
    "SinkDeliveryError",
    "SinkLosses",
    "Verdict",
    "__version__",
    "configure",
    "continue_trace",
    "critical",
    "current_baggage_header",
    "current_trace_context",
    "current_traceparent",
    "debug",
    "emit_via_deliver",
    "error",
    "flush",
    "flush_sink",
    "get_baggage",
    "get_config",
    "health",
    "info",
    "read_deliver",
    "read_losses",
    "reset_context",
    "set_baggage",
    "shutdown",
    "trace",
    "warning",
]
"""``log_foundry.__all__``, sorted, captured at ``75e7bf1e``."""


def test_no_public_signature_changes() -> None:
    """FR-005 AC1: capacity is not a setting, so no signature and no public name moved."""
    found = {
        "configure": log_foundry.configure,
        "Worker.__init__": Worker.__init__,
        "flush": log_foundry.flush,
        "shutdown": log_foundry.shutdown,
        "health": log_foundry.health,
    }
    for name, function in found.items():
        assert str(inspect.signature(function)) == _SIGNATURES[name], name
    assert sorted(log_foundry.__all__) == _ALL
    assert not hasattr(log_foundry, "_BACKLOG_BATCH_CAP")
