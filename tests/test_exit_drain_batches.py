"""SPEC-090 FR-002 and FR-003: the exit drain sends its backlog in batches, within its budget.

The exit drain continues the batch the steady drain was building, takes only what was queued
when it began, sends it by the steady drain's rule, answers each flush marker where it meets it
through the one helper both drains call, and declines a later exit batch it predicts cannot
finish before the ``shutdown()`` deadline, counting what it declined in one line.

**How a test gets an exit backlog.** :func:`_at_exit` builds a ~~60 s~~ one-hour worker whose
first call, ten spans taken by the count trigger before any stop, waits on the worker's own stop
event, so it returns the instant a ``shutdown()`` claims the stop; the backlog queued behind it is
then the exit drain's, and its first batch begins with no delay a loaded runner could stretch.
The cap is patched to 20 events, ten of these two-event spans, wherever a test needs every exit
batch to be exactly ten spans whatever the measured target.

**Why an hour** (diff review, 2026-11-16). That first call's measurement includes its wait for
the stop, and it sets the target the exit batches read: ``20 / elapsed * interval / 2``. At 60 s
a cap of 100 needed that wait under 6 s, a premise nothing stated; at an hour the target is at
least 1,200 events for any wait the sink allows, which is 30 s, so it is the cap for every cap a
test here patches.
"""

from __future__ import annotations

import ast
import collections
import math
import pathlib
import re
import signal
import threading
import time
from typing import Any

import pytest

import ledger_check
import log_foundry
from conftest import install_worker, retire_process
from ledger_sinks import ledger
from log_foundry import Outcome, SinkCounters, Verdict, _diag, _lifecycle, emit_via_deliver
from log_foundry import worker as worker_mod
from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.sqs import SQSSink
from log_foundry.worker import _SHUTDOWN, Worker, _FlushMarker
from test_sinks_http import FakeResponse
from test_worker_exit_budget import _SLACK, _lost_lines, _measured

_PAUSE = 0.002
_EXPIRY_FIGURE = re.compile(r"; (\d+) event\(s\) in flight \(health\(\)\.events\.in_flight\)")
_LOG_LINE = re.compile(r"^log-foundry: ", re.MULTILINE)


class _ExitSink:
    """A sink whose first call waits for the stop, and whose later calls follow the test's plan.

    Attributes:
      handed: The size of every batch handed to ``emit``, in call order.
      batches: Every batch whose ``emit`` returned.
      first_waits_on: The event the first call waits on; the worker's stop event.
      first_fails: Whether the first call raises once it is released.
      first_seconds: Seconds the first call idles once released.
      seconds: Seconds every later call idles.
      fails: Whether every later call raises.
      raise_on: Call numbers, from 1, that raise ``KeyboardInterrupt``.
      fail_calls: Call numbers, from 1, that raise ``ConnectionError``.
      closes: ``close()`` calls.
    """

    def __init__(self, *, seconds: float = 0.0, fails: bool = False) -> None:
        self.handed: list[int] = []
        self.batches: list[list[dict[str, object]]] = []
        self.first_waits_on: threading.Event | None = None
        self.first_fails = False
        self.first_seconds = 0.0
        self.seconds = seconds
        self.fails = fails
        self.raise_on: set[int] = set()
        self.fail_calls: set[int] = set()
        self.on_call: Any = None
        self.watch: _FlushMarker | None = None
        self.answered: list[bool] = []
        self.closes = 0
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
        """Waits, holds or idles as planned, then raises or records the batch."""
        with self._lock:
            self.handed.append(len(batch))
            call = len(self.handed)
            gate = self._holds.get(call)
            entered = self._entered.setdefault(call, threading.Event())
            if self.watch is not None:
                self.answered.append(self.watch.event.is_set())
        entered.set()
        if self.on_call is not None:
            self.on_call(call)
        if call == 1 and self.first_waits_on is not None:
            self.first_waits_on.wait(30.0)
        if gate is not None:
            gate.wait(30.0)
        idle = self.first_seconds if call == 1 else self.seconds
        if idle:
            threading.Event().wait(idle)
        if call in self.raise_on:
            raise KeyboardInterrupt
        if call in self.fail_calls or (call == 1 and self.first_fails) or (call > 1 and self.fails):
            raise ConnectionError("down")
        with self._lock:
            self.batches.append(list(batch))

    def close(self) -> None:
        """Counts the close."""
        with self._lock:
            self.closes += 1

    def events(self) -> int:
        """Events in every batch whose ``emit`` returned."""
        with self._lock:
            return sum(len(batch) for batch in self.batches)


class _EngineSink:
    """A ``deliver`` sink: its first call waits for the stop and accepts, later ones retry.

    Attributes:
      calls: ``deliver`` calls made.
      seconds: Seconds every later call idles.
      accepts: Whether every later call accepts its batch rather than answering RETRYABLE.
      raises: Whether every later call raises ``ConnectionError`` instead of answering.
      accepted: Events accepted by every call.
    """

    max_retries = 0

    def __init__(
        self, *, seconds: float = 0.3, accepts: bool = False, raises: bool = False
    ) -> None:
        self.log_foundry_counters = SinkCounters()
        self.calls = 0
        self.seconds = seconds
        self.accepts = accepts
        self.raises = raises
        self.accepted = 0
        self.first_waits_on: threading.Event | None = None

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        """Accepts the first batch once the stop is claimed; later ones idle, then as planned."""
        self.calls += 1
        if self.calls == 1:
            assert self.first_waits_on is not None
            self.first_waits_on.wait(30.0)
            self.accepted += len(batch)
            return Verdict(outcomes=(Outcome.ACCEPTED,) * len(batch))
        threading.Event().wait(self.seconds)
        if self.raises:
            raise ConnectionError("down")
        if self.accepts:
            self.accepted += len(batch)
            return Verdict(outcomes=(Outcome.ACCEPTED,) * len(batch))
        return Verdict(outcomes=(Outcome.RETRYABLE,) * len(batch))

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Emits through the adapter."""
        emit_via_deliver(self, batch)

    def losses(self) -> object:
        """The engine's counters."""
        return self.log_foundry_counters.losses()

    def close(self) -> None:
        """Releases nothing."""


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


def _at_exit(
    sink: Any,
    queued: int,
    *,
    batch_size: int = 10,
    max_retries: int = 3,
    flush_interval: float = 3_600.0,
) -> Worker:
    """Builds and installs a worker holding one pre-stop batch, with ``queued`` spans behind it.

    ``flush_interval`` is an hour unless a test says otherwise (the module docstring says why).
    SPEC-105's tests pass 1.0 s, its fake clock's interval: their pre-stop call fails, so the
    target at the stop is zero whatever the interval, or, where it is delivered, the test
    charges it on that clock.
    """
    worker = Worker(
        sink, batch_size=batch_size, flush_interval=flush_interval, max_retries=max_retries
    )
    sink.first_waits_on = worker._stop
    install_worker(worker)
    for seq in range(batch_size):
        worker.submit(_span(seq))
    _until(lambda: worker._in_flight == 2 * batch_size)
    for seq in range(batch_size, batch_size + queued):
        worker.submit(_span(seq))
    return worker


def _ten_span_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patches the cap to 20 events, so every batch is exactly ten two-event spans."""
    monkeypatch.setattr(worker_mod, "_BACKLOG_BATCH_CAP", 20)


def _queued_marker(worker: Worker) -> _FlushMarker | None:
    """The last flush marker in the worker's queue, or ``None``."""
    with worker._queue.mutex:
        found = [item for item in worker._queue.queue if type(item) is _FlushMarker]
    return found[-1] if found else None


def _flush_behind(worker: Worker, results: dict[str, object], name: str) -> threading.Thread:
    """Starts a ``flush()`` on a thread and returns once its marker is queued."""
    before = _queued_marker(worker)
    thread = threading.Thread(target=lambda: results.__setitem__(name, worker.flush(15)))
    thread.start()
    _until(lambda: _queued_marker(worker) not in (None, before))
    return thread


def _retire_on_thread(worker: Worker, timeout: float | None) -> threading.Thread:
    """Runs the real shutdown on a thread and returns once the stop is claimed and set."""
    thread = threading.Thread(target=retire_process, args=(worker, timeout))
    thread.start()
    _until(worker._stop.is_set)
    return thread


def _deadline_moves_after_first(
    monkeypatch: pytest.MonkeyPatch, name: str, deadline: Any
) -> list[float | None]:
    """Moves the exit deadline to ``deadline()`` once the first exit call of ``name`` returns.

    ``name`` is ``"_emit"``, whose call counts only for an exit batch, or
    ``"_decline_exit_batch"``. Moving the deadline later stands in for a caller off the drain
    thread taking over an owed join (``Worker.claim_stop``), which records its own deadline, so
    only the exit budget's latch can decline what follows: the prediction alone would permit it.
    """
    real = getattr(Worker, name)
    moved: list[float | None] = []

    def wrapped(self: Worker, *args: Any, **kw: Any) -> None:
        try:
            real(self, *args, **kw)
        finally:
            if not moved and (name != "_emit" or kw.get("exit_batch")):
                self._exit_deadline = deadline()
                moved.append(self._exit_deadline)

    monkeypatch.setattr(Worker, name, wrapped)
    return moved


def _later() -> float:
    """A deadline a minute away, which the prediction alone never declines against."""
    return worker_mod.time.monotonic() + 60.0


# ---------------------------------------------------------------------------- FR-002


def test_an_exit_backlog_goes_in_batches_no_larger_than_the_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-002 AC1, in the suite: with the cap at 100, 300 spans at exit go in batches of 100."""
    monkeypatch.setattr(worker_mod, "_BACKLOG_BATCH_CAP", 100)
    sink = _ExitSink()
    worker = _at_exit(sink, 300)
    retire_process(worker, 10.0)
    assert sink.events() == 620
    assert sink.handed[1:] == [100] * 6, sink.handed


def test_an_expiry_inside_the_third_exit_batch_names_what_the_drain_holds(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-002 AC2: the expiry line's in-flight figure is ``health().events.in_flight``.

    Two exit batches go, the third is held past the deadline, and ten spans are still queued.
    The figure is at most one batch and one submission, and what the sink received, what is
    queued and that figure add up to every event accepted.
    """
    _ten_span_batches(monkeypatch)
    sink = _ExitSink()
    held = sink.hold(4)
    worker = _at_exit(sink, 40)
    accepted = 2 * 50
    try:
        retire_process(worker, 1.0)
        assert worker.stopped_reason == "ShutdownTimeout", "the premise: the join expired"
        assert sink.entered(4).is_set(), "the premise: the third exit batch is in the sink"
        err = capsys.readouterr().err
        figure = int(_EXPIRY_FIGURE.search(err).group(1))  # type: ignore[union-attr]
        reading = ledger()
        assert figure == log_foundry.health().events.in_flight == reading.in_flight
        assert figure <= 20 + 2
        assert "; 10 submission(s) still queued" in err, err
        assert sink.events() + reading.queued + figure == accepted
    finally:
        held.set()
        worker._thread.join(10)


def test_a_marker_met_at_exit_is_answered_in_queue_order(monkeypatch: pytest.MonkeyPatch) -> None:
    """FR-002 AC3: six batches of ten events, and the marker answered after the first.

    ``batch_size=1``, a 60 s interval and the cap patched to 10; the first batch is measured
    before the stop, so the target is at the cap. Thirty two-event spans are queued at exit
    with a ``flush()`` marker after the fifth.

    **That premise is asserted, not assumed** (diff review, 2026-11-16). The first batch's
    measurement includes its wait for the stop, and at the criterion's 60 s interval two events
    reach a target of 10 only if that wait is under 6 s; the sink reads the target as the first
    exit batch begins, so a slow runner fails here by name rather than as a wrong batch size.
    """
    monkeypatch.setattr(worker_mod, "_BACKLOG_BATCH_CAP", 10)
    sink = _ExitSink()
    worker = Worker(sink, batch_size=1, flush_interval=60.0)
    sink.first_waits_on = worker._stop
    targets: list[float] = []
    sink.on_call = lambda call: targets.append(worker._size_target()) if call == 2 else None
    install_worker(worker)
    worker.submit(_span(0))
    assert sink.entered(1).wait(10)
    for seq in range(1, 6):
        worker.submit(_span(seq))
    results: dict[str, object] = {}
    flusher = _flush_behind(worker, results, "marker")
    sink.watch = _queued_marker(worker)
    for seq in range(6, 31):
        worker.submit(_span(seq))
    retire_process(worker, 10.0)
    flusher.join(15)
    assert targets == [10], f"the premise: the first batch measured the cap, read {targets}"
    assert results["marker"], results
    assert sink.handed[1:] == [10] * 6, sink.handed
    assert sink.answered == [False] + [True] * 5, sink.answered


def test_a_caller_logging_after_shutdown_cannot_keep_the_exit_drain_running(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-002 AC4: the exit drain takes nothing queued after it began.

    The sink submits five more spans from inside the exit drain's first batch, after the drain
    fixed its bound. None is delivered, all five stay queued, and each is counted in
    ``submitted_after_shutdown``.
    """
    _ten_span_batches(monkeypatch)
    in_final = threading.Event()
    real = Worker._final_drain

    def marked(self: Worker, pending: list[list[dict[str, object]]]) -> None:
        in_final.set()
        real(self, pending)

    monkeypatch.setattr(Worker, "_final_drain", marked)
    sink = _ExitSink()
    worker = _at_exit(sink, 30)

    def late(call: int) -> None:
        if in_final.is_set() and call == 2:
            for seq in range(1_000, 1_005):
                worker.submit(_span(seq))

    sink.on_call = late
    retire_process(worker, 10.0)
    worker._thread.join(10)
    assert len(sink.handed) == 4, "the premise: three exit batches, the late spans arrived in one"
    delivered = {
        int(event["seq"])  # type: ignore[call-overload]
        for batch in sink.batches
        for event in batch
    }
    assert not delivered & set(range(1_000, 1_005)), "the exit drain took a late submission"
    assert worker._queued_submissions() == 5
    assert worker.submitted_after_shutdown == 5


def test_a_base_exception_in_the_second_exit_batch_leaves_every_marker_answered(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-002 AC5: markers answered before the exception stay answered, every later is released.

    Marker A follows the first exit batch and is answered truthy. Marker B's own batch raises
    ``KeyboardInterrupt``, so B is answered by the helper's ``finally``, falsy. Marker C, still
    queued, is released by the drain thread's exit. Each ``flush()`` returns well within its
    timeout. The ``unwinding`` half of the criterion is owed until SPEC-089 is on ``main``.
    """
    _ten_span_batches(monkeypatch)
    sink = _ExitSink()
    sink.raise_on.add(3)
    worker = _at_exit(sink, 10)
    results: dict[str, object] = {}
    flushers = [_flush_behind(worker, results, "A")]
    for seq in range(20, 25):
        worker.submit(_span(seq))
    flushers.append(_flush_behind(worker, results, "B"))
    for seq in range(25, 35):
        worker.submit(_span(seq))
    flushers.append(_flush_behind(worker, results, "C"))
    retire_process(worker, 10.0)
    for flusher in flushers:
        flusher.join(5)
        assert not flusher.is_alive(), "a flush() was left waiting"
    assert worker.stopped_reason == "KeyboardInterrupt", "the premise: the second batch raised"
    assert sink.handed == [20, 20, 10], sink.handed
    assert results["A"], results
    assert not results["B"] and not results["C"], results
    capsys.readouterr()


_ANSWERER_MISSING = "a drain does not call self._answer_marker"
_ANSWERER_SETS = "a drain sets a marker's event itself"
_ANSWERER_WRITES = "a drain writes a marker's delivered itself"
_ANSWERER_REGISTERS = "a drain takes or releases a marker itself"
_DRAINS = ("_drain", "_final_drain")


def answerer_findings(source: str) -> list[tuple[str, str]]:
    """Every way ``Worker._drain`` or ``Worker._final_drain`` answers a marker but by the helper.

    Args:
      source: The text of a module defining ``Worker``.

    Returns:
      One ``(reason, method)`` pair per refusal, empty when both drains answer only through
      ``self._answer_marker``.
    """
    tree = ast.parse(source)
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "Worker")
    methods = {n.name: n for n in cls.body if isinstance(n, ast.FunctionDef)}
    found: list[tuple[str, str]] = []
    for name in _DRAINS:
        method = methods.get(name)
        if method is None:
            found.append((_ANSWERER_MISSING, name))
            continue
        calls_helper = False
        for node in ast.walk(method):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                target = node.func
                if target.attr == "_answer_marker" and ast.unparse(target.value) == "self":
                    calls_helper = True
                if target.attr in ("_take_marker", "_release_marker"):
                    found.append((_ANSWERER_REGISTERS, name))
                if (
                    target.attr == "set"
                    and isinstance(target.value, ast.Attribute)
                    and target.value.attr == "event"
                ):
                    found.append((_ANSWERER_SETS, name))
            if isinstance(node, ast.Assign | ast.AugAssign | ast.AnnAssign):
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                found.extend(
                    (_ANSWERER_WRITES, name)
                    for written in targets
                    if isinstance(written, ast.Attribute) and written.attr == "delivered"
                )
        if not calls_helper:
            found.append((_ANSWERER_MISSING, name))
    return found


def test_both_drains_answer_a_marker_only_through_answer_marker() -> None:
    """FR-002 AC6: the twins answer a marker in one place, so its verdict cannot diverge."""
    source = pathlib.Path(worker_mod.__file__).read_text(encoding="utf-8")
    assert answerer_findings(source) == []


_ANSWERER_TEMPLATE = """
class Worker:
    def _drain(self, pending):
        item = self._queue.get()
        {drain}

    def _final_drain(self, pending):
        item = self._queue.get_nowait()
        self._answer_marker(pending, item)
"""

_ANSWERER_CORPUS = {
    "sets-event": ("self._answer_marker(pending, item)\n        item.event.set()", _ANSWERER_SETS),
    "no-call": ("self._emit_pending(pending)", _ANSWERER_MISSING),
    "writes-delivered": (
        "self._answer_marker(pending, item)\n        item.delivered = True",
        _ANSWERER_WRITES,
    ),
    "takes-marker": (
        "self._take_marker(item)\n        self._answer_marker(pending, item)",
        _ANSWERER_REGISTERS,
    ),
}


@pytest.mark.parametrize("case", sorted(_ANSWERER_CORPUS))
def test_the_answerer_gate_refuses_its_shapes(case: str) -> None:
    """Each check of the answerer gate fires for its own reason, and only that reason."""
    body, reason = _ANSWERER_CORPUS[case]
    found = answerer_findings(_ANSWERER_TEMPLATE.format(drain=body))
    assert found == [(reason, "_drain")], (case, found)


def test_the_answerer_gate_is_silent_on_the_helper_alone() -> None:
    """Both drains calling only the helper pass, so a refusal above is the shape's."""
    source = _ANSWERER_TEMPLATE.format(drain="self._answer_marker(pending, item)")
    assert answerer_findings(source) == []


def test_the_exit_drain_continues_a_batch_with_the_steady_drains_boundaries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-002 AC7: one queue and one target, two drains, the same batch boundaries.

    Both workers have ``batch_size=5``, the cap patched to ~~7~~ 20 and a measured first batch,
    so their target is ~~7~~ 20. The steady one drains the submissions normally. The other is
    handed the shutdown sentinel after the third, so ``_drain`` breaks holding three and
    ``_final_drain`` continues that batch with the rest.

    **Why 20, and the marker first** (diff review, 2026-11-16). At 7 every five submissions
    already passed the target, so no batch grew past ``batch_size`` and none was carried, and an
    exit drain that appended and emitted at five submissions made the same boundaries. At 20,
    five of these submissions fall under the target, so a batch grows, and one is carried, on
    both drains. The plug is 1,000 events, so its measurement is the cap for any hold under
    1,500 s. The steady worker's ``flush()`` marker is queued before its plug is released, so
    the queue is never empty under a data item and ``waiting`` reads as the exit drain's does.
    """
    monkeypatch.setattr(worker_mod, "_BACKLOG_BATCH_CAP", 20)
    sizes = [1, 2, 3, 1, 4, 2, 1, 1, 3, 2, 1, 1, 5, 2, 1, 3, 1, 1, 2, 2, 4, 1, 1, 1, 2, 3]
    boundaries: list[list[int]] = []
    submissions: list[list[int]] = []
    for exiting in (False, True):
        sink = _ExitSink()
        gate = sink.hold(1)
        worker = Worker(sink, batch_size=5, flush_interval=60.0)
        worker.submit(_span(-1, events=1_000))
        worker.flush(0)
        assert sink.entered(1).wait(10)
        for seq, size in enumerate(sizes):
            worker.submit(_span(seq, events=size))
            if exiting and seq == 2:
                worker._queue.put_nowait(_SHUTDOWN)
        results: dict[str, object] = {}
        flusher = None if exiting else _flush_behind(worker, results, "steady")
        gate.set()
        if flusher is None:
            worker._thread.join(10)
            assert not worker._thread.is_alive(), "the premise: the sentinel ended the drain"
        else:
            flusher.join(15)
            assert results["steady"], results
        boundaries.append(sink.handed[1:])
        submissions.append([len({event["seq"] for event in batch}) for batch in sink.batches[1:]])
        worker.stop(5.0)
    assert sum(boundaries[0]) == sum(sizes)
    assert max(submissions[0]) > 5, f"the premise: a batch grew past batch_size: {submissions}"
    carried = [
        events < 20
        for events, held in zip(boundaries[0][:-1], submissions[0][:-1], strict=True)
        if held > 5
    ]
    assert any(carried), f"the premise: a grown batch went short of the target: {boundaries}"
    assert boundaries[1] == boundaries[0]
    assert submissions[1] == submissions[0]


# ---------------------------------------------------------------------------- FR-003


class _SlowAfterStop(_ExitSink):
    """The spec review's probe sink: the first call waits for the stop, later ones fail slowly."""

    def __init__(self) -> None:
        super().__init__(seconds=0.5, fails=True)


def test_three_exit_batches_behind_a_slow_failing_sink_fit_the_budget(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-003 AC1, in the suite: ``spec090_exit_budget.py tree`` with three exit batches.

    A sink that fails after 0.5 s, thirty spans at exit, ``shutdown(timeout=1.2)``: the first
    exit batch makes two attempts and declines its third; the other two are declined
    unattempted. The join completes and ``close()`` runs once, as SPEC-062 measured with one.
    """
    _ten_span_batches(monkeypatch)
    sink = _SlowAfterStop()
    worker = _at_exit(sink, 30)
    elapsed = _measured(lambda: retire_process(worker, 1.2))
    closes_at_return = sink.closes
    worker._thread.join(5.0)
    err = capsys.readouterr().err
    assert elapsed < 1.2 + _SLACK, f"shutdown returned at {elapsed:.2f}s"
    assert worker.stopped_reason is None, "the join completed"
    assert closes_at_return == 1
    assert len(sink.handed) - 1 == 2, sink.handed
    assert worker.failed_batches == 3
    lost = [(n, detail) for n, _what, detail in _lost_lines(err)]
    assert [n for n, _ in lost] == [20, 40], err
    assert "retry declined after 2 emit attempt(s)" in lost[0][1]
    assert "2 exit batch(es) declined unattempted" in lost[1][1]


_SLOW_CALL = 0.6
"""Seconds a slow healthy exit call takes: more than ``_JOIN_MARGIN``, so a prediction that
ignored it would start a call the join cannot wait for."""


def test_a_slow_exit_counts_what_it_declined_in_one_line(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-003 AC2, in the suite: a slow healthy sink at exit ends in budget with ``close()`` run.

    The batches the budget declines are counted in one line, and that line and what the sink
    received add up to every event accepted.

    **Each call takes ~~0.05 s~~ 0.6 s** (diff review, 2026-11-16). At 0.05 s, under the 0.1 s
    ``_JOIN_MARGIN``, a prediction that never timed a successful attempt overran by less than
    the margin and the join still completed, so nothing pinned that timing. At 0.6 s under a
    1.0 s budget the first exit batch ends near 0.6 s and the second is predicted to end near
    1.3 s, so it is declined; a prediction from no timing would start it and end it near
    1.2 s, past the deadline, and the join would expire. The gap is 0.3 s either side of the
    budget for the stop's own latency.
    """
    monkeypatch.setattr(worker_mod, "_BACKLOG_BATCH_CAP", 40)
    sink = _ExitSink(seconds=_SLOW_CALL)
    worker = _at_exit(sink, 390)
    retire_process(worker, 1.0)
    closes_at_return = sink.closes
    worker._thread.join(5.0)
    err = capsys.readouterr().err
    assert worker.stopped_reason is None, "the join completed"
    assert closes_at_return == 1
    assert len(sink.handed) - 1 == 1, sink.handed
    declined = [(n, d) for n, _w, d in _lost_lines(err) if "declined unattempted" in d]
    assert len(declined) == 1, err
    assert sink.events() + declined[0][0] == 2 * 400


def test_an_engine_runs_time_feeds_the_exit_budget(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-003, the engine twin of AC2: a ``deliver`` run's time predicts the next exit batch.

    An engine sink whose later runs accept after 0.6 s: the first exit batch's run is timed
    whole, and the second is declined before its first ``deliver``, so the join completes. Were
    the run not timed, the second would start and end past the 1.0 s deadline.
    """
    monkeypatch.setattr(worker_mod, "_BACKLOG_BATCH_CAP", 40)
    sink = _EngineSink(seconds=_SLOW_CALL, accepts=True)
    worker = _at_exit(sink, 390)
    retire_process(worker, 1.0)
    worker._thread.join(5.0)
    err = capsys.readouterr().err
    assert worker.stopped_reason is None, "the join completed"
    assert sink.calls - 1 == 1, "one deliver call made at exit"
    declined = [(n, d) for n, _w, d in _lost_lines(err) if "declined unattempted" in d]
    assert len(declined) == 1, err
    assert sink.accepted + declined[0][0] == 2 * 400


def test_a_dead_sink_at_exit_writes_at_most_three_lines(capsys: pytest.CaptureFixture[str]) -> None:
    """FR-003 AC3, in the suite: 2,000 spans behind a sink that always raises, three lines.

    The pre-stop batch and the first exit batch each write their own abandon line, and every
    later exit batch is one tally, so the lines name every event accepted and no more.

    **The 198 is unchanged by an abandoned exit batch keeping the target** (diff review,
    2026-11-16): the pre-stop batch failed on the steady drain, which resets the target to zero,
    so every exit batch is ten spans whether or not the exit resets it again. This is the
    residual ``architecture.md`` §12 files, a target already zero at the stop; the two tests
    after this one cover a target at the cap.
    """
    sink = _ExitSink(fails=True)
    sink.first_fails = True
    worker = _at_exit(sink, 1_990)
    retire_process(worker, 30.0)
    worker._thread.join(5.0)
    err = capsys.readouterr().err
    lines = _LOG_LINE.findall(err)
    assert len(lines) <= 3, err
    assert sum(n for n, _w, _d in _lost_lines(err)) == 4_000, err
    assert "198 more exit batch(es) abandoned" in err, err


_DEAD_BACKLOG = 2_000
"""Spans queued at exit behind a dead sink: 4,000 events, four exit batches at the cap."""


def _dead_exit_bound(events: int) -> int:
    """The exit batches a dead sink may be handed: the backlog over the cap, and one carry."""
    return math.ceil(events / worker_mod._BACKLOG_BATCH_CAP) + 1


def test_a_dead_engine_sink_at_exit_is_handed_batches_at_the_cap(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """FR-001 rule 4 at exit: an abandoned exit batch leaves the target as it was.

    The pre-stop batch is delivered, so the target is the cap, and then every ``deliver``
    raises. Each failed run writes the engine's own ``absorbed`` line; ~~which the exit tallies
    cannot aggregate, so the lines grow with the number of exit batches~~ — refined by SPEC-103:
    from the first abandoned exit batch on, those lines are folded into one per distinct line,
    and the calls are unchanged. Before SPEC-090 the whole backlog was one call and two lines;
    the bound asserted here is the backlog over the cap, plus one for a carry, in calls, and
    that plus two in lines, an upper bound the fold only lowers. Had the first abandoned exit
    batch reset the target, the rest would go ten spans a call: about 150 calls here (diff
    review, 2026-11-16).
    """
    sink = _EngineSink(seconds=0.0, raises=True)
    worker = _at_exit(sink, _DEAD_BACKLOG)
    retire_process(worker, 30.0)
    worker._thread.join(5.0)
    err = capsys.readouterr().err
    bound = _dead_exit_bound(2 * _DEAD_BACKLOG)
    assert sink.calls - 1 <= bound, f"{sink.calls - 1} deliver calls at exit, bound {bound}"
    assert len(_LOG_LINE.findall(err)) <= bound + 2, err
    assert worker.failed_batches == sink.calls - 1


def test_a_refused_http_sink_at_exit_is_handed_batches_at_the_cap(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """FR-001 rule 4 at exit, through a shipped sink: a refusing ``HTTPSink`` keeps the cap.

    ``HTTPSink`` writes its own line per failed request, and from the first abandoned exit
    batch on those lines are folded (SPEC-103), while the requests are unchanged. Its opener
    answers the pre-stop batch once the stop is claimed, so the target is the cap, then refuses
    every request. With no retries at either level, each exit batch is one request, so the
    opener calls are bounded as the engine twin's are, where a reset target made them about 150,
    and the lines, an upper bound the fold only lowers, likewise.
    """
    calls = [0]
    stop: list[threading.Event] = []

    def opener(request: Any, timeout: float | None = None) -> FakeResponse:
        calls[0] += 1
        if calls[0] == 1:
            stop[0].wait(30.0)
            return FakeResponse(200)
        raise ConnectionRefusedError(61, "refused")

    sink = HTTPSink("http://127.0.0.1:1/x", opener=opener, max_retries=0)
    worker = Worker(sink, batch_size=10, flush_interval=3_600.0, max_retries=0)
    stop.append(worker._stop)
    install_worker(worker)
    for seq in range(10):
        worker.submit(_span(seq))
    _until(lambda: calls[0] == 1)
    for seq in range(10, 10 + _DEAD_BACKLOG):
        worker.submit(_span(seq))
    retire_process(worker, 30.0)
    worker._thread.join(5.0)
    err = capsys.readouterr().err
    bound = _dead_exit_bound(2 * _DEAD_BACKLOG)
    assert calls[0] - 1 <= bound, f"{calls[0] - 1} requests at exit, bound {bound}"
    assert len(_LOG_LINE.findall(err)) <= bound + 2, err
    assert worker.failed_batches == calls[0] - 1


def test_an_unbounded_shutdown_declines_nothing(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-003 AC4: with no deadline every exit batch makes ``max_retries + 1`` attempts."""
    _ten_span_batches(monkeypatch)
    sink = _ExitSink(fails=True)
    worker = _at_exit(sink, 30, max_retries=2)
    retire_process(worker, None)
    err = capsys.readouterr().err
    assert len(sink.handed) - 1 == 3 * 3, sink.handed
    assert worker.failed_batches == 3
    assert "declined" not in err, err


def test_a_deadline_already_past_still_makes_the_first_exit_batchs_first_attempt(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-003 AC5: ``shutdown(timeout=0)``; one exit attempt, every later batch declined.

    The declined batches may be announced in one line or two, depending on whether the
    expired ``stop()`` took the tally before or between them; together they name 40 events.
    """
    _ten_span_batches(monkeypatch)
    sink = _ExitSink()
    worker = _at_exit(sink, 30)
    retire_process(worker, 0)
    worker._thread.join(5.0)
    err = capsys.readouterr().err
    assert len(sink.handed) - 1 == 1, sink.handed
    assert sink.events() == 40
    declined = [n for n, _w, d in _lost_lines(err) if "declined unattempted" in d]
    assert sum(declined) == 40, err
    assert worker.failed_batches == 2


def test_the_abandon_tally_is_written_once_before_the_expiry_line(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-003 AC6: two tallied exit batches are announced once, by ``stop()``, before its line.

    ``max_retries=0``: the first three exit batches fail at once, the first with its own line
    and the next two tallied; the fourth is held past the deadline. Once it is released and the
    drain thread ends, nothing announces those two batches again.
    """
    _ten_span_batches(monkeypatch)
    sink = _ExitSink()
    sink.fail_calls = {2, 3, 4}
    held = sink.hold(5)
    worker = _at_exit(sink, 40, max_retries=0)
    try:
        retire_process(worker, 0.5)
        assert worker.stopped_reason == "ShutdownTimeout", "the premise: the join expired"
    finally:
        held.set()
        worker._thread.join(10)
    err = capsys.readouterr().err
    tally = "lost 40 event(s); 2 more exit batch(es) abandoned"
    assert err.count(tally) == 1, err
    assert err.count("more exit batch(es) abandoned") == 1, err
    assert err.index(tally) < err.index("shutdown timed out"), err


def test_an_engine_run_that_declines_makes_every_later_exit_batch_declined(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-003 AC7: an engine run's declined retry declines the later exit batches unattempted.

    **The deadline moves a minute later once that run returns** (diff review, 2026-11-16), as
    a caller taking over an owed join can move it. Without that the prediction from the run's
    0.3 s declined the later batches by itself, and a run that never set the latch passed.
    """
    _ten_span_batches(monkeypatch)
    moved = _deadline_moves_after_first(monkeypatch, "_emit", _later)
    sink = _EngineSink()
    worker = _at_exit(sink, 30)
    retire_process(worker, 0.5)
    assert len(moved) == 1, "the premise: the deadline moved after the first exit batch"
    worker._thread.join(5.0)
    err = capsys.readouterr().err
    assert sink.calls - 1 == 1, "one deliver call made at exit"
    assert worker.failed_batches == 3
    lost = [(n, d) for n, _w, d in _lost_lines(err)]
    assert [n for n, _ in lost] == [20, 40], err
    assert "retry declined" in lost[0][1]
    assert "2 exit batch(es) declined unattempted" in lost[1][1]


def test_a_declined_retry_at_exit_makes_every_later_exit_batch_declined(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-003, the 1.x twin of AC7: a declined retry declines the later exit batches unattempted.

    A sink whose later calls fail after 0.3 s, under a 0.5 s budget: the first exit batch's
    retry is predicted to end near 0.7 s and is declined. The deadline then moves a minute
    later, as a caller taking over an owed join can move it, so only the latch that declined
    retry set can decline the two batches behind it.
    """
    _ten_span_batches(monkeypatch)
    moved = _deadline_moves_after_first(monkeypatch, "_emit", _later)
    sink = _ExitSink(seconds=0.3, fails=True)
    worker = _at_exit(sink, 30)
    retire_process(worker, 0.5)
    worker._thread.join(10.0)
    err = capsys.readouterr().err
    assert len(moved) == 1, "the premise: the deadline moved after the first exit batch"
    assert len(sink.handed) - 1 == 1, sink.handed
    assert worker.failed_batches == 3
    lost = [(n, d) for n, _w, d in _lost_lines(err)]
    assert [n for n, _ in lost] == [20, 40], err
    assert "retry declined after 1 emit attempt(s)" in lost[0][1]
    assert "2 exit batch(es) declined unattempted" in lost[1][1]


def test_a_declined_exit_batch_makes_every_later_one_declined(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-003: a batch declined by the prediction declines every later one, deadline moved or not.

    A healthy sink whose later calls take 0.3 s, under a 0.5 s budget: the first exit batch is
    delivered, the second is predicted to end near 0.7 s and declined, and the deadline then
    moves a minute later, so only the latch that decline set can decline the third.
    """
    _ten_span_batches(monkeypatch)
    moved = _deadline_moves_after_first(monkeypatch, "_decline_exit_batch", _later)
    sink = _ExitSink(seconds=0.3)
    worker = _at_exit(sink, 30)
    retire_process(worker, 0.5)
    worker._thread.join(10.0)
    err = capsys.readouterr().err
    assert len(moved) == 1, "the premise: the deadline moved after the first declined batch"
    assert sink.handed[1:] == [20], sink.handed
    assert worker.failed_batches == 2
    declined = [n for n, _w, d in _lost_lines(err) if "2 exit batch(es) declined unattempted" in d]
    assert declined == [40], err


def test_a_deadline_of_none_after_a_decline_declines_nothing(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-003: "a deadline of None declines nothing" holds after the latch is set.

    The first exit batch's retry is declined against a 0.5 s budget, which sets the latch.
    Then the deadline becomes ``None``, as when a ``stop(None)`` off the drain thread takes over
    an owed join and records its own. The two batches behind it are attempted and delivered,
    and no line claims a deadline there is not (diff review, 2026-11-16).
    """
    _ten_span_batches(monkeypatch)
    moved = _deadline_moves_after_first(monkeypatch, "_emit", lambda: None)
    sink = _ExitSink(seconds=0.3)
    sink.fail_calls = {2}
    worker = _at_exit(sink, 30)
    retire_process(worker, 0.5)
    worker._thread.join(10.0)
    err = capsys.readouterr().err
    assert moved == [None], "the premise: the deadline became None after the first exit batch"
    assert "retry declined after 1 emit attempt(s)" in err, "the premise: the latch was set"
    assert sink.handed[1:] == [20, 20, 20], sink.handed
    assert sink.events() == 20 + 40
    assert "declined unattempted" not in err, err
    assert worker.failed_batches == 1
    assert worker._declined_tally == (0, 0)


def test_a_retry_declined_before_the_exit_batches_does_not_decline_them(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-003: only an exit batch's declined retry declines the later exit batches.

    The batch in the sink when the stop lands is not an exit batch. Its slow failure has its
    retry declined against a 1.0 s deadline, but the exit batches behind it are fast, each is
    predicted to fit, and all three are delivered.
    """
    _ten_span_batches(monkeypatch)
    sink = _ExitSink()
    sink.first_fails = True
    sink.first_seconds = 0.6
    worker = _at_exit(sink, 30)
    retire_process(worker, 1.0)
    worker._thread.join(5.0)
    err = capsys.readouterr().err
    assert "retry declined after 1 emit attempt(s)" in err, "the premise: the retry was declined"
    assert sink.events() == 60, sink.handed
    assert "declined unattempted" not in err, err
    assert worker.failed_batches == 1


def test_once_an_exit_batch_is_declined_every_later_one_is() -> None:
    """FR-003: the latch holds even when the prediction alone would permit the batch.

    That is the case after a later caller takes an owed join over and moves the deadline
    later; here the deadline is simply far away once the latch is set.
    """
    sink = _ExitSink()
    worker = Worker(sink, batch_size=10, flush_interval=60.0)
    worker.stop(5.0)
    worker._exit_deadline = worker_mod.time.monotonic() + 60.0
    worker._exit_batches = 1
    worker._exit_declining = True
    batch = _span(0)
    worker._hold(batch)
    worker._emit_pending([batch])
    assert sink.handed == []
    assert worker.failed_batches == 1
    assert worker._declined_tally == (2, 1)
    assert (worker._in_flight, worker._batch_lost) == (0, 2)


def test_a_declined_exit_batch_leaves_the_target_as_it_was(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-001 rule 4's last sentence: a batch declined unattempted sets no target."""
    _ten_span_batches(monkeypatch)
    measured: list[int] = []
    real = Worker._set_target

    def counting(self: Worker, events: int, *args: Any, **kw: Any) -> None:
        measured.append(events)
        real(self, events, *args, **kw)

    monkeypatch.setattr(Worker, "_set_target", counting)
    sink = _ExitSink()
    worker = _at_exit(sink, 30)
    retire_process(worker, 0)
    worker._thread.join(5.0)
    capsys.readouterr()
    assert worker.failed_batches == 2, "the premise: two exit batches were declined"
    assert measured == [20, 20], "only the two batches attempted set the target"


# ---------------------------------------------------------------------------- SPEC-103 FR-001

_FOLDED = re.compile(r"; (\d+) line\(s\) folded at exit$")
_ABSORBED_FOLDED = re.compile(r"^absorbed (\d+) failure\(s\) while (.*)$")
_LOST = re.compile(r"^lost (\d+) (.*)$")


def _line_key(line: str) -> tuple[tuple[str, str], int, int | None]:
    """A sink line's key, the count it names, and its folded-line count when it is folded."""
    body = line.removeprefix("log-foundry: ")
    folded: int | None = None
    tail = _FOLDED.search(body)
    if tail is not None:
        folded = int(tail.group(1))
        body = body[: tail.start()]
    lost = _LOST.match(body)
    if lost is not None:
        return ("lost", lost.group(2)), int(lost.group(1)), folded
    absorbed = _ABSORBED_FOLDED.match(body)
    if absorbed is not None:
        return ("absorbed", absorbed.group(2)), int(absorbed.group(1)), folded
    if body.startswith("absorbed a failure while "):
        return ("absorbed", body.removeprefix("absorbed a failure while ")), 1, folded
    return ("pending", body), folded or 1, folded


def _sink_lines(err: str, mark: str) -> list[str]:
    """The ``log-foundry:`` lines carrying ``mark``, the sink's own, in order."""
    return [line for line in err.splitlines() if line.startswith("log-foundry: ") and mark in line]


class _FoldingSink(_ExitSink):
    """An ``_ExitSink`` whose every call writes one line of each kind first, keyed by parity."""

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Writes a ``lost``, an ``absorbed`` and a ``pending`` line, then behaves as planned."""
        call = len(self.handed) + 1
        _diag.lost("event", call % 3 + 1, f"Folding, key {call % 2}")
        _diag.absorbed("sending a batch", ConnectionError(), f"Folding, key {call % 2}")
        _diag.pending(f"Folding, a request may still land, key {call % 2}")
        super().emit(batch)


def _exit_of(sink: Any, queued: int, capsys: pytest.CaptureFixture[str]) -> str:
    """Runs one exit of a worker built here, uninstalled, and returns what it wrote to stderr.

    The worker is built directly and stopped with ``Worker.stop``, so one test can run two exits:
    the process's lifecycle is not involved, and the exit drain is the same.
    """
    capsys.readouterr()
    worker = Worker(sink, batch_size=10, flush_interval=3_600.0, max_retries=0)
    sink.first_waits_on = worker._stop
    for seq in range(10):
        worker.submit(_span(seq))
    _until(lambda: worker._in_flight == 20)
    for seq in range(10, 10 + queued):
        worker.submit(_span(seq))
    worker.stop(30.0)
    worker._thread.join(5.0)
    assert not worker._thread.is_alive(), "the premise: the exit drain ended"
    return capsys.readouterr().err


def test_a_folded_exit_writes_each_distinct_line_once_with_the_unfolded_totals(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-001 AC3: one exit, held shut and as built, writes the same lines with the same totals.

    Every exit batch is ten spans, one call, and every call after the pre-stop one fails, so the
    first exit batch is the first abandoned one: its lines and the pre-stop call's are written as
    they are, and the five calls after it are folded. Held shut, ``_diag.fold_into`` is a no-op,
    so nothing folds. Per key, the built run's folded count plus its unfolded counts must equal
    what the unfolded run's lines name, and each folded line must precede the abandon tally.
    """
    _ten_span_batches(monkeypatch)
    with monkeypatch.context() as shut:
        shut.setattr(_diag, "fold_into", lambda table: None)
        unfolded = _exit_of(_FoldingSink(fails=True), 60, capsys)
    built = _exit_of(_FoldingSink(fails=True), 60, capsys)
    assert _FOLDED.pattern not in unfolded
    assert "folded at exit" not in unfolded, "the premise: the held-shut run folds nothing"

    expected: collections.Counter[tuple[str, str]] = collections.Counter()
    expected_lines: collections.Counter[tuple[str, str]] = collections.Counter()
    for line in _sink_lines(unfolded, "Folding"):
        key, count, _ = _line_key(line)
        expected[key] += count
        expected_lines[key] += 1
    got: collections.Counter[tuple[str, str]] = collections.Counter()
    folded_lines: dict[tuple[str, str], tuple[int, int]] = {}
    unfolded_lines: collections.Counter[tuple[str, str]] = collections.Counter()
    for line in _sink_lines(built, "Folding"):
        key, count, folded = _line_key(line)
        got[key] += count
        if folded is None:
            unfolded_lines[key] += 1
        else:
            assert key not in folded_lines, f"a key folded twice: {line}"
            folded_lines[key] = (count, folded)
    assert {kind for kind, _ in folded_lines} == {"lost", "absorbed", "pending"}, built
    assert set(got) == set(expected), (built, unfolded)
    assert got == expected, "a folded total differs from what the unfolded lines name"
    for key, (count, folded) in folded_lines.items():
        assert folded == expected_lines[key] - unfolded_lines[key], key
        if key[0] != "lost":
            assert count == folded, f"{key}: an absorbed or pending count is its folded lines"
    lines = built.splitlines()
    tally = next(i for i, line in enumerate(lines) if "more exit batch(es) abandoned" in line)
    assert all(i < tally for i, line in enumerate(lines) if "folded at exit" in line), (
        "a folded line was written after the abandon tally"
    )


class _TwinSink(_ExitSink):
    """A 1.x sink whose every later call writes the twins' ``lost`` and ``absorbed`` lines."""

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Writes the two lines on every call after the first, then behaves as planned."""
        if self.handed:
            _diag.lost("event", len(batch), "Twin, refused")
            _diag.absorbed("sending a batch", ConnectionError(), "Twin")
        super().emit(batch)


class _TwinEngineSink(_EngineSink):
    """An engine sink whose every later call writes the twins' lines, then answers RETRYABLE."""

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        """Writes the two lines on every call after the first, then answers as planned."""
        if self.calls:
            _diag.lost("event", len(batch), "Twin, refused")
            _diag.absorbed("sending a batch", ConnectionError(), "Twin")
        return super().deliver(batch)


def test_a_dead_1x_sink_and_a_dead_engine_sink_fold_to_the_same_lines(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-001 AC4 (invariant 6): the engine run and the 1.x ``emit`` are folded alike."""
    _ten_span_batches(monkeypatch)
    one_x = _exit_of(_TwinSink(fails=True), 60, capsys)
    engine = _exit_of(_TwinEngineSink(seconds=0.0), 60, capsys)
    folded_1x = [line for line in _sink_lines(one_x, "Twin") if "folded at exit" in line]
    folded_engine = [line for line in _sink_lines(engine, "Twin") if "folded at exit" in line]
    assert folded_1x == [
        "log-foundry: lost 100 event(s); Twin, refused; 5 line(s) folded at exit",
        (
            "log-foundry: absorbed 5 failure(s) while sending a batch (ConnectionError); Twin; "
            "5 line(s) folded at exit"
        ),
    ], one_x
    assert folded_engine == folded_1x, engine


class _PlannedEngineSink(_EngineSink):
    """An engine sink answering RETRYABLE after the first call, idling on the calls planned."""

    def __init__(self, idle: dict[int, float]) -> None:
        super().__init__(seconds=0.0)
        self.idle = idle

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        """Writes a sink line, idles as planned, then answers as the base sink does."""
        call = self.calls + 1
        if call > 1:
            _diag.lost("event", 1, "Retry, sink line")
            threading.Event().wait(self.idle.get(call, 0.0))
        return super().deliver(batch)


def _declining_1x(worker_retries: int) -> Any:
    """A 1.x sink failing every later call, call 4 after idling 0.6 s, each writing a line."""
    sink = _ExitSink(fails=True)

    def on_call(call: int) -> None:
        if call > 1:
            _diag.lost("event", 1, "Retry, sink line")
        if call == 4:
            threading.Event().wait(_SLOW_CALL)

    sink.on_call = on_call
    return sink


@pytest.mark.parametrize("path", ["1.x", "engine"])
def test_a_retry_declined_after_the_fold_began_is_written_once_unfolded(
    path: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-001 AC5: SPEC-062's ``retry declined`` line is the worker's, outside the window.

    ``max_retries=1``: the first exit batch fails both attempts, quickly, and is the first
    abandoned one; the second exit batch's first attempt idles 0.6 s and fails, so against a
    1.0 s budget its retry is declined. That batch's sink line is folded; the decline line is not.
    """
    _ten_span_batches(monkeypatch)
    sink: Any = _declining_1x(1) if path == "1.x" else _PlannedEngineSink({4: _SLOW_CALL})
    worker = _at_exit(sink, 40, max_retries=1)
    retire_process(worker, 1.0)
    worker._thread.join(5.0)
    err = capsys.readouterr().err
    declined = [line for line in err.splitlines() if "retry declined after" in line]
    assert len(declined) == 1, err
    assert "folded at exit" not in declined[0], declined
    assert declined[0].startswith("log-foundry: lost 20 event(s); retry declined after 1 emit")
    assert (
        "log-foundry: lost 40 event(s); 2 exit batch(es) declined unattempted; each was "
        "predicted to end after the shutdown() deadline"
    ) in err.splitlines(), err
    assert "log-foundry: lost 1 event(s); Retry, sink line; 1 line(s) folded at exit" in err, err


def _snapshot_at(sink: _ExitSink, call: int, capsys: pytest.CaptureFixture[str]) -> str:
    """Stderr so far, read once call number ``call`` has begun and while it is held."""
    _until(sink.entered(call).is_set)
    return capsys.readouterr().err


def test_lines_before_the_first_abandoned_exit_batch_is_counted_are_written_at_once(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-001 AC6: the steady drain's, an earlier exit batch's and the first abandoned one's.

    Call 1 is the steady drain's, call 2 an exit batch that succeeds, call 3 the first exit
    batch abandoned; each writes its line. Call 4 is held, and stderr read then already holds all
    three, unfolded, while call 4's own line, written inside the window, is not there yet.
    """
    _ten_span_batches(monkeypatch)
    sink = _ExitSink()
    sink.fail_calls = {3, 4}
    sink.on_call = lambda call: _diag.lost("event", 1, f"Early, call {call}")
    held = sink.hold(4)
    worker = _at_exit(sink, 30, max_retries=0)
    retiring = _retire_on_thread(worker, 30.0)
    try:
        snapshot = _snapshot_at(sink, 4, capsys)
    finally:
        held.set()
        retiring.join(30.0)
        worker._thread.join(5.0)
    for call in (1, 2, 3):
        assert f"log-foundry: lost 1 event(s); Early, call {call}\n" in snapshot, snapshot
    assert "Early, call 4" not in snapshot, snapshot
    assert "folded at exit" not in snapshot, snapshot
    assert "Early, call 4; 1 line(s) folded at exit" in capsys.readouterr().err


def test_an_expired_join_writes_the_folded_lines_once_before_its_line_and_closes_the_fold(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-001 AC7: ``stop()`` writes the folded lines before its expiry line, and closes the fold.

    Call 2 is the first abandoned exit batch; call 3 is folded and fails; call 4 writes a line,
    then waits past the 0.5 s deadline. Once ``shutdown()`` has returned, call 4 is released and
    writes a second line, then waits again: that line is already on stderr, unfolded, before the
    call returns. No folded row is written twice.
    """
    _ten_span_batches(monkeypatch)
    release, wrote_after, may_return = threading.Event(), threading.Event(), threading.Event()
    sink = _ExitSink()
    sink.fail_calls = {2, 3, 4}

    def on_call(call: int) -> None:
        if call == 3:
            _diag.lost("event", 1, "Expiry, call 3")
        if call == 4:
            _diag.lost("event", 1, "Expiry, mid-call")
            release.wait(30.0)
            _diag.lost("event", 1, "Expiry, after expiry")
            wrote_after.set()
            may_return.wait(30.0)

    sink.on_call = on_call
    worker = _at_exit(sink, 40, max_retries=0)
    try:
        retire_process(worker, 0.5)
        assert worker.stopped_reason == "ShutdownTimeout", "the premise: the join expired"
        at_return = capsys.readouterr().err
        release.set()
        assert wrote_after.wait(10.0)
        while_inside = capsys.readouterr().err
    finally:
        release.set()
        may_return.set()
        worker._thread.join(10.0)
    at_end = capsys.readouterr().err
    expiry = at_return.index("shutdown timed out")
    for text in ("Expiry, call 3", "Expiry, mid-call"):
        folded = f"log-foundry: lost 1 event(s); {text}; 1 line(s) folded at exit"
        assert at_return.index(folded) < expiry, at_return
    assert while_inside == "log-foundry: lost 1 event(s); Expiry, after expiry\n", while_inside
    everything = at_return + while_inside + at_end
    assert everything.count("folded at exit") == 2, everything
    assert everything.count("Expiry, after expiry") == 1, everything


def test_a_flush_answered_after_the_fold_began_returns_before_its_batchs_lines(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-001 AC8: a marker met at exit emits an exit batch, whose lines are folded.

    Fifteen spans are queued ahead of the marker and ten behind it, so the exit drain sends ten
    (call 2, the first abandoned), then the five ahead of the marker (call 3, folded), answers
    the ``flush()``, and holds call 4. The ``flush()`` has returned by then, and call 3's line is
    nowhere on stderr; it appears folded at the end of the exit.
    """
    _ten_span_batches(monkeypatch)
    sink = _ExitSink(fails=True)
    sink.on_call = lambda call: _diag.lost("event", 1, f"Flush, call {call}")
    held = sink.hold(4)
    worker = _at_exit(sink, 15, max_retries=0)
    results: dict[str, object] = {}
    flushing = _flush_behind(worker, results, "flush")
    for seq in range(100, 110):
        worker.submit(_span(seq))
    retiring = _retire_on_thread(worker, 30.0)
    try:
        snapshot = _snapshot_at(sink, 4, capsys)
        flushing.join(10.0)
        assert "flush" in results, "the premise: the flush() returned while call 4 is held"
        snapshot += capsys.readouterr().err
    finally:
        held.set()
        retiring.join(30.0)
        worker._thread.join(5.0)
    assert sink.handed[:3] == [20, 20, 10], sink.handed
    assert "Flush, call 3" not in snapshot, snapshot
    assert "Flush, call 3; 1 line(s) folded at exit" in capsys.readouterr().err


def test_a_line_from_a_thread_the_sink_starts_is_written_at_once(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-001 AC10: the window is the drain thread's, so a sink's own thread writes at once."""
    _ten_span_batches(monkeypatch)
    sink = _ExitSink(fails=True)

    def on_call(call: int) -> None:
        if call == 3:
            own = threading.Thread(
                target=lambda: _diag.lost("event", 1, "OwnThread, from the sink's thread")
            )
            own.start()
            own.join(5.0)

    sink.on_call = on_call
    held = sink.hold(4)
    worker = _at_exit(sink, 30, max_retries=0)
    retiring = _retire_on_thread(worker, 30.0)
    try:
        snapshot = _snapshot_at(sink, 4, capsys)
    finally:
        held.set()
        retiring.join(30.0)
        worker._thread.join(5.0)
    assert "log-foundry: lost 1 event(s); OwnThread, from the sink's thread\n" in snapshot


class _ReenteringRows(dict[tuple[str, ...], list[int]]):
    """A fold table's rows whose lookup writes a ``_diag`` line, as a finalizer there would.

    A recorder looks its key up while it holds ``_diag._FOLD_LOCK``, so this ``get`` runs inside
    the locked section, on the recording thread, exactly where the collector can run a
    ``__del__``. Each nested write records whether the lock was held by its own thread then.

    Attributes:
      drain: The thread whose lookups count as re-entries.
      kind: Which folding writer the nested write calls: ``lost``, ``absorbed`` or ``pending``.
      nested: For each lookup on that thread, whether ``_FOLD_LOCK`` was held during it.
    """

    def __init__(self, drain: threading.Thread, kind: str) -> None:
        super().__init__()
        self.drain = drain
        self.kind = kind
        self.nested: list[bool] = []

    def get(self, key: tuple[str, ...], default: Any = None) -> Any:  # type: ignore[override]
        """Writes a line of its own when called on the drain thread, then looks the key up."""
        if threading.current_thread() is self.drain:
            self.nested.append(_diag._FOLD_LOCK.locked())
            if self.kind == "lost":
                _diag.lost("object", 1, "from inside the fold lock")
            elif self.kind == "absorbed":
                _diag.absorbed("finalizing an object", ValueError(), "from inside the fold lock")
            else:
                _diag.pending("from inside the fold lock")
        return super().get(key, default)


@pytest.mark.parametrize("kind", ["absorbed", "lost", "pending"])
def test_a_finalizer_that_writes_inside_the_fold_lock_does_not_deadlock_the_exit(
    kind: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """SPEC-103 plan review B1 (invariant 3): a recorder never waits on the fold lock.

    The collector can run a ``__del__`` on the drain thread inside a recorder's locked section,
    and one that writes a ``_diag`` line re-enters a writer on the thread holding the lock: a
    recorder that waited for the lock waited on its own thread for good, and ``stop()``'s take
    then never returned. **The re-entry is made deterministic** rather than left to the
    collector: the worker's table gets rows whose lookup, made under the lock, writes a line.
    A version driven by ``gc.set_threshold(1, 1, 1)`` failed its own premise in 2 of 2 parallel
    runs, because whether the collector fires inside the section depends on what ran before it
    in the process. The exit is otherwise real: folded exit batches behind a dead sink,
    ``stop(1.0)`` on a thread, which must return within a bound, and every event counted once
    the drain ends. The nested write is each folding writer in turn, since each carries its own
    inline acquire.
    """
    _ten_span_batches(monkeypatch)
    sink = _FoldingSink(fails=True)
    worker = Worker(sink, batch_size=10, flush_interval=3_600.0, max_retries=0)
    rows = _ReenteringRows(worker._thread, kind)
    worker._exit_fold.rows = rows
    sink.first_waits_on = worker._stop
    for seq in range(10):
        worker.submit(_span(seq))
    _until(lambda: worker._in_flight == 20)
    for seq in range(10, 310):
        worker.submit(_span(seq))
    stopping = threading.Thread(target=worker.stop, args=(1.0,), daemon=True)
    stopping.start()
    stopping.join(15.0)
    assert not stopping.is_alive(), "stop() did not return: the fold lock deadlocked"
    worker._thread.join(15.0)
    assert not worker._thread.is_alive(), "the drain thread deadlocked on the fold lock"
    err = capsys.readouterr().err
    assert rows.nested, "the premise: a recorder looked a key up on the drain thread"
    assert all(rows.nested), "the premise: every nested write ran while the fold lock was held"
    assert err.count("from inside the fold lock\n") == len(rows.nested), (
        "each nested write is written at once, unfolded"
    )
    assert "folded at exit" in err, "the outer records still folded"
    assert worker._delivered + worker._batch_lost == 2 * 310, "an event was left uncounted"


class _SignallingRows(dict[tuple[str, ...], list[int]]):
    """A fold table's rows whose first read on the main thread raises ``SIGUSR1`` there.

    ``stop()``'s closing take reads the rows while it holds ``_diag._FOLD_LOCK``, so the handler
    runs inside that section, as any signal handler arriving there would.

    Attributes:
      raised: Whether the signal has been raised.
    """

    def __init__(self) -> None:
        super().__init__()
        self.raised = False

    def items(self) -> Any:
        """Raises the signal once, on the main thread, then reads the rows."""
        if not self.raised and threading.current_thread() is threading.main_thread():
            self.raised = True
            signal.raise_signal(signal.SIGUSR1)
        return super().items()


_NESTED_BUDGET = 5.0
"""The budget of the ``shutdown()`` a signal handler calls inside the closing take, in seconds."""


def test_a_shutdown_from_a_handler_inside_the_closing_take_is_not_held_by_the_drain(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """SPEC-103 diff review frame 2 (invariant 3): the drain's take gives way to a closing take.

    ``shutdown(0.5)``'s join expires while call 4 idles 1 s, so ``stop()``'s closing take runs
    and holds ``_diag._FOLD_LOCK``. A ``SIGUSR1`` handler runs there and calls ``shutdown()``
    again, which loses the claim and waits for the drain to settle. The drain, once call 4
    returns, reaches its own take at the end of the exit. When that take waited for the lock
    without bound, it waited on the handler's thread, and the handler waited on it: measured by
    the reviewer, the nested call took its whole budget, or never returned with ``None``. Now the
    take sees the table closed and gives way, so the nested call returns once call 4 does.
    """
    assert threading.current_thread() is threading.main_thread(), "the premise: signals land here"
    _ten_span_batches(monkeypatch)
    sink = _ExitSink(fails=True)
    sink.on_call = lambda call: threading.Event().wait(1.0) if call == 4 else None
    worker = _at_exit(sink, 60, max_retries=0)
    rows = _SignallingRows()
    worker._exit_fold.rows = rows
    nested: list[tuple[float, bool, bool]] = []

    def handler(signum: int, frame: object) -> None:
        held, closed = _diag._FOLD_LOCK.locked(), worker._exit_fold.closed
        started = time.monotonic()
        log_foundry.shutdown(_NESTED_BUDGET)
        nested.append((time.monotonic() - started, held, closed))

    previous = signal.signal(signal.SIGUSR1, handler)
    try:
        retire_process(worker, 0.5)
    finally:
        signal.signal(signal.SIGUSR1, previous)
        worker._thread.join(10.0)
    capsys.readouterr()
    assert rows.raised, "the premise: the closing take read the rows on the main thread"
    assert len(nested) == 1, "the premise: the handler ran"
    waited, held, closed = nested[0]
    assert held and closed, "the premise: the handler ran inside the closing take"
    assert waited < _NESTED_BUDGET - 2.0, (
        f"the nested shutdown() waited {waited:.2f}s: the drain's take was held behind the lock "
        f"the handler's own thread held"
    )
    assert not worker._thread.is_alive(), "the drain thread ended"


# ---------------------------------------------------------------------------- SPEC-103 FR-002


class _SlowdownClock:
    """A monotonic clock that only the black-holed client moves."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class _SlowdownClient:
    """An SQS client whose first request waits for the stop; every request raises."""

    def __init__(self, clock: _SlowdownClock, slow_at: float) -> None:
        self.clock = clock
        self.slow_at = slow_at
        self.sizes: list[int] = []
        self.first_waits_on: threading.Event | None = None

    def send_message_batch(self, *, QueueUrl: str, Entries: list[Any]) -> None:
        """Costs 0.5 ms on the clock until ``slow_at``, then 0.3 s, and raises."""
        self.sizes.append(len(Entries))
        if len(self.sizes) == 1:
            assert self.first_waits_on is not None
            self.first_waits_on.wait(30.0)
        else:
            self.clock.now += 0.3 if self.clock.now >= self.slow_at else 0.0005
        raise ConnectionError("black-holed")


@pytest.mark.parametrize("slow_at", [0.004, 0.01, 0.02])
def test_a_sqs_sink_that_turns_slow_mid_exit_ends_before_the_deadline(
    slow_at: float, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-002 AC4 (invariant 3): exit batches do not grow, so a slowdown cannot outrun the budget.

    ``spec103_sqs_slowdown_exit.py``'s shape through the shipped ``shutdown(0.5)``: a black-holed
    ``SQSSink`` whose requests cost 0.5 ms on a clock only the client moves until ``slow_at``,
    then 0.3 s. ``time.monotonic`` and ``_lifecycle.monotonic`` are patched process-wide, as the
    probe patches them, so the 7f400f51 numbers stay comparable: run in a worktree at that
    commit, this test failed with the drain ending 1.60, 2.81 and 6.42 s past the deadline.

    **The drain's end on that clock is the assertion that carries the guard**: the join and the
    close pass at 7f400f51 too. The join's own wait is the deadline on the fake clock less the
    fake now, which is real seconds, so a loaded runner (``-n 12``) can expire it while the drain
    is still well inside the fake budget. The worker thread's ``join`` is therefore given 30 real
    seconds more, which changes no fake-clock arithmetic: a drain that overran the fake deadline
    still fails the first assertion, however long the real join waited for it.
    """
    clock = _SlowdownClock()
    monkeypatch.setattr(time, "monotonic", clock)
    monkeypatch.setattr(_lifecycle, "monotonic", clock)
    client = _SlowdownClient(clock, slow_at)
    sink = SQSSink("https://sqs/q", client=client, max_retries=0)
    closes = [0]
    monkeypatch.setattr(sink, "close", lambda: closes.__setitem__(0, closes[0] + 1))
    worker = Worker(sink, batch_size=10, flush_interval=1.0, max_retries=0)
    real_join = worker._thread.join
    monkeypatch.setattr(
        worker._thread,
        "join",
        lambda timeout=None: real_join(None if timeout is None else timeout + 30.0),
    )
    client.first_waits_on = worker._stop
    install_worker(worker)
    for seq in range(10):
        worker.submit([{"message": "m", "seq": seq}])
    _until(lambda: bool(client.sizes))
    for seq in range(10, 2010):
        worker.submit([{"message": "m", "seq": seq}])
    ended: list[float] = []

    def watch() -> None:
        worker._drain_finished.wait(60.0)
        ended.append(clock.now)

    watcher = threading.Thread(target=watch)
    watcher.start()
    log_foundry.shutdown(0.5)
    closes_at_return = closes[0]
    real_join(60.0)
    watcher.join(60.0)
    capsys.readouterr()
    deadline = worker._exit_deadline
    assert deadline is not None
    assert ended, "the drain never finished"
    assert ended[0] < deadline, f"the drain ended {ended[0] - deadline:+.2f}s past the deadline"
    assert worker.stopped_reason is None, "the join completed"
    assert closes_at_return == 1, "close() ran once"


# ---------------------------------------------------------------------------- SPEC-105

_COST = 0.00390625
"""Seconds an exit call costs per event on SPEC-105 FR-001's clock: 256 events a second."""

_FIT_MARGIN = 0.0390625
"""``_JOIN_MARGIN`` as FR-001's clock patches it, exact in binary as every figure there is."""

_FAIL_COST = 0.0625
"""Seconds a failing exit call costs on FR-001's clock, whatever it was handed."""

_DECLINED = re.compile(r"(\d+) exit batch\(es\) declined unattempted")


class _FitRun:
    """One exit driven on SPEC-105 FR-001's clock, and what it saw.

    Attributes:
      clock: The clock only the sinks move, starting at 0.0.
      worker: The worker, once built.
      sink: The sink the worker was built with.
      calls: Sink calls made, across every sink the run charges.
      rates: ``_exit_rate`` as each charged call began, the pre-stop call excepted.
      snapshots: After each ``_emit_pending`` that called a sink, the worker's ``target``,
        ``generation``, ``rate`` and ``size_target`` and the clock's ``now``; the first is the
        pre-stop batch's, then one per exit batch handed to a sink, whichever sink it was.
      first_charged: Set once the pre-stop call has been charged.
      err: What the run wrote to stderr.
    """

    def __init__(self, clock: _SlowdownClock) -> None:
        self.clock = clock
        self.worker: Worker | None = None
        self.sink: Any = None
        self.calls = 0
        self.rates: list[float] = []
        self.snapshots: list[dict[str, float]] = []
        self.first_charged = threading.Event()
        self.err = ""


class _ChargedEngineSink(_EngineSink):
    """A ``deliver`` sink whose first call fails once the stop is claimed, after ``first_seconds``.

    Every call first runs ``on_call`` with its number, as ``_ExitSink`` does, which is where a
    test charges it; a later call then raises if ``fail_calls`` names it and accepts otherwise.

    Attributes:
      handed: The size of every batch handed to ``deliver``, in call order.
      first_seconds: Seconds the first call idles once the stop is claimed, before it raises.
      fail_calls: Call numbers, from 1, that raise ``ConnectionError``.
      on_call: Called with each call's number before it answers, or ``None``.
      closes: ``close()`` calls.
    """

    def __init__(self) -> None:
        super().__init__(seconds=0.0, accepts=True)
        self.handed: list[int] = []
        self.first_seconds = 0.0
        self.fail_calls: set[int] = set()
        self.on_call: Any = None
        self.closes = 0

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        """Fails the first call once the stop is claimed; later calls are charged, then answer."""
        self.calls += 1
        call = self.calls
        self.handed.append(len(batch))
        if self.on_call is not None:
            self.on_call(call)
        if call == 1:
            assert self.first_waits_on is not None
            self.first_waits_on.wait(30.0)
            if self.first_seconds:
                threading.Event().wait(self.first_seconds)
            raise ConnectionError("down")
        if call in self.fail_calls:
            raise ConnectionError("down")
        self.accepted += len(batch)
        return Verdict(outcomes=(Outcome.ACCEPTED,) * len(batch))

    def close(self) -> None:
        """Counts the close."""
        self.closes += 1


def _charging(
    run: _FitRun,
    sink: Any,
    costs: dict[int, float],
    *,
    per_event: float = _COST,
    main: bool = False,
    hook: Any = None,
) -> None:
    """Charges each of ``sink``'s calls on the run's clock: ``costs`` by call number, else per event.

    ``main`` marks the sink the worker was built with, whose first call is the pre-stop batch:
    it is charged before that call waits for the stop, sets ``first_charged`` and records no
    rate, since the worker may not be in ``run`` yet. ``hook(run, call)`` runs before the charge.
    """

    def on_call(call: int) -> None:
        pre_stop = main and call == 1
        if not pre_stop:
            assert run.worker is not None
            run.rates.append(run.worker._exit_rate)
        if hook is not None:
            hook(run, call)
        run.clock.now += costs.get(call, per_event * sink.handed[call - 1])
        run.calls += 1
        if pre_stop:
            run.first_charged.set()

    sink.on_call = on_call


def _reassign(worker: Worker, new_sink: Any) -> None:
    """Changes the worker's sink as ``Worker.retarget``'s locked section does.

    ``retarget`` itself declines once ``shutdown()`` has moved the retirement count, so it cannot
    reassign during an exit drain; this does what its reassign does, under the same lock: the
    sink, the generation bump and the sinks the drain may be inside.
    """
    with worker._lock:
        worker.sink = new_sink
        worker._sink_generation += 1
        worker._held = [held for held in worker._held if held is not new_sink] + [new_sink]


def _fit_exit(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    *,
    deadline: float | None,
    spans: int,
    fail: frozenset[int] = frozenset(),
    free: frozenset[int] = frozenset(),
    engine: bool = False,
    pre_stop_cost: float | None = None,
    hook: Any = None,
    between: Any = None,
) -> _FitRun:
    """Runs one exit on SPEC-105 FR-001's clock and returns what it saw.

    **The clock.** ``time.monotonic`` and ``_lifecycle.monotonic`` read a clock only the sinks
    move, from 0.0, as SPEC-103 FR-002's test patches them; ``_JOIN_MARGIN`` is 0.0390625 s, the
    interval 1.0 s, ``max_retries`` 0 and ``batch_size`` 10 two-event spans. The pre-stop call
    costs nothing and fails, so the target is zero at the stop, unless ``pre_stop_cost`` is
    given, when it costs that, before the stop, and is delivered. Every later call costs
    0.00390625 s an event, a call in ``fail`` costs 0.0625 s and raises, and one in ``free``
    costs nothing; calls are numbered from 1, the pre-stop call first.

    **The deadline is the shipped path's.** ``retire_process(worker, deadline)`` runs once the
    pre-stop call is charged, so ``claim_stop`` records the clock's reading plus ``deadline``:
    exactly ``deadline`` while the pre-stop call is free, the instant the first exit batch then
    begins at, which is FR-001's premise. **The join gets 30 real seconds more**, as SPEC-103
    FR-002's test gives it: its own wait is the fake deadline less the fake now, in real
    seconds, which a loaded runner could spend while the drain is still inside the fake budget.
    No fake-clock figure moves, and every run asserts the drain ended and the join completed,
    so the declined line is the drain's own, written once.

    ``hook(run, call)`` runs at the start of each of the first sink's calls; ``between(run,
    index)`` after each ``_emit_pending`` that called any sink, ``index`` counting the pre-stop
    batch as 0. Either may change the worker's sink with :func:`_reassign`.
    """
    clock = _SlowdownClock()
    monkeypatch.setattr(time, "monotonic", clock)
    monkeypatch.setattr(_lifecycle, "monotonic", clock)
    monkeypatch.setattr(worker_mod, "_JOIN_MARGIN", _FIT_MARGIN)
    run = _FitRun(clock)
    costs = {1: pre_stop_cost or 0.0} | dict.fromkeys(fail, _FAIL_COST)
    costs |= dict.fromkeys(free, 0.0)
    sink: Any = _ChargedEngineSink() if engine else _ExitSink()
    if not engine:
        sink.first_fails = pre_stop_cost is None
    sink.fail_calls = set(fail)
    _charging(run, sink, costs, main=True, hook=hook)
    real = Worker._emit_pending

    def snapshotting(self: Worker, pending: list[list[dict[str, object]]]) -> None:
        calls = run.calls
        real(self, pending)
        if run.calls == calls:
            return
        run.snapshots.append(
            {
                "target": self._target,
                "generation": float(self._target_generation),
                "rate": self._exit_rate,
                "size_target": self._size_target(),
                "now": run.clock.now,
            }
        )
        if between is not None:
            between(run, len(run.snapshots) - 1)

    monkeypatch.setattr(Worker, "_emit_pending", snapshotting)
    worker = _at_exit(sink, spans, max_retries=0, flush_interval=1.0)
    run.worker = worker
    run.sink = sink
    assert run.first_charged.wait(10), "the premise: the pre-stop call was charged"
    real_join = worker._thread.join
    monkeypatch.setattr(
        worker._thread,
        "join",
        lambda timeout=None: real_join(None if timeout is None else timeout + 30.0),
    )
    retire_process(worker, deadline)
    real_join(60.0)
    run.err = capsys.readouterr().err
    assert not worker._thread.is_alive(), "the drain ended"
    assert worker.stopped_reason is None, "the join completed"
    return run


def _declined(err: str) -> list[tuple[int, int]]:
    """Every declined line on stderr, as (events, batches)."""
    found = [(n, _DECLINED.search(detail)) for n, _what, detail in _lost_lines(err)]
    return [(n, int(match.group(1))) for n, match in found if match is not None]


def test_after_a_delivered_exit_batch_the_next_fits_the_time_left(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """SPEC-105 FR-001 AC1 and FR-003 AC4 (invariant 3): the second exit batch fits the budget.

    The first exit batch, 20 events, ends at 0.078125 and records 256 events a second; the rate
    rule would make the next 128 events, ending at 0.578125, past the 0.328125 deadline. The
    bound gives 256 × (0.328125 − 0.078125 − 0.0390625) = 54, which ends at 0.2890625: the time
    left less the margin is then exactly 0.0, below the longest attempt, so the guard leaves the
    rate rule's 128.0 standing rather than a bound of zero, and the rest is declined.
    """
    run = _fit_exit(monkeypatch, capsys, deadline=0.328125, spans=500)
    assert run.sink.handed[1:] == [20, 54], run.sink.handed
    assert run.clock.now == 0.2890625
    assert 0.328125 - run.clock.now - _FIT_MARGIN == 0.0
    assert run.worker is not None
    assert run.worker._target == 128.0, "the guard kept the rate rule's target, not zero"
    assert _declined(run.err) == [(926, 8)], run.err


def test_the_engine_twin_is_handed_the_same_fitted_batches(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """SPEC-105 FR-001 AC2 (invariant 6): a ``deliver`` sink is handed the same 20 and 54."""
    run = _fit_exit(monkeypatch, capsys, deadline=0.328125, spans=500, engine=True)
    assert run.sink.handed[1:] == [20, 54], run.sink.handed


def test_an_exit_batch_the_clock_cannot_see_records_no_rate(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """SPEC-105 FR-001 AC3 (invariant 3): a first exit batch that costs no time records no rate.

    Its elapsed is zero, so ``_set_target`` gives the cap and no rate bounds the next batch,
    which is the whole remaining backlog. Before any rate exists, "set to 0.0" and "left as it
    was" read alike here; the straddle branch of the same rule is held by the test after this.
    """
    run = _fit_exit(monkeypatch, capsys, deadline=0.328125, spans=500, free=frozenset({2}))
    assert run.sink.handed[1:] == [20, 980], run.sink.handed
    assert run.rates[1] == 0.0, "_exit_rate when the second exit batch reached the sink"


def test_a_delivered_exit_batch_across_a_sink_change_records_no_rate(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """SPEC-105 FR-001: a delivered exit batch whose emit straddled a sink change records 0.0.

    It measured a sink no longer installed. The sink changes inside the first exit batch's call,
    which then delivers; without the generation check the rate would read 256.0.
    """

    def hook(run: _FitRun, call: int) -> None:
        if call == 2:
            assert run.worker is not None
            new = _ExitSink()
            _charging(run, new, {})
            _reassign(run.worker, new)

    run = _fit_exit(monkeypatch, capsys, deadline=0.328125, spans=500, hook=hook)
    assert run.sink.handed[1:] == [20], "the premise: the first exit batch went to the old sink"
    assert run.snapshots[1]["rate"] == 0.0, run.snapshots[1]


def test_a_flapping_sink_at_exit_is_held_to_the_time_left_after_each_failure(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """SPEC-105 FR-002 AC1 and AC2 (invariants 3 and 5): each failure re-fits the kept target.

    The first exit batch delivers at 256 events a second; the next three fail after 0.0625 s
    each, and each failure lowers the kept target to what that rate fits in the time now left,
    so the fifth, delivered, returns at 0.5390625, the deadline less the margin. Without the
    bound on the failed path the batches are 20, 118, 118, 118 and 118, and the fifth ends at
    0.7265625, past the 0.578125 deadline.
    """
    run = _fit_exit(monkeypatch, capsys, deadline=0.578125, spans=500, fail=frozenset({3, 4, 5}))
    assert run.sink.handed[1:] == [20, 118, 102, 86, 70], run.sink.handed
    assert run.snapshots[5]["now"] == 0.5390625, run.snapshots


def test_an_exit_batch_failing_across_a_sink_change_leaves_the_target_as_it_was(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """SPEC-105 FR-002 AC3 (invariant 6): no bound after a failed exit batch that straddled.

    The second exit batch, 118 events under a 118.0 target, fails across a sink change at
    0.140625, where the bound with the recorded 256 events a second would lower the target to
    102.0. The target and its generation are left exactly as they were before it. This pins the
    state, not an outcome: the stale-generation target reads 0.0 either way.
    """
    before: list[tuple[float, int]] = []

    def hook(run: _FitRun, call: int) -> None:
        if call == 3:
            assert run.worker is not None
            before.append((run.worker._target, run.worker._target_generation))
            new = _ExitSink()
            _charging(run, new, {})
            _reassign(run.worker, new)

    run = _fit_exit(
        monkeypatch, capsys, deadline=0.578125, spans=500, fail=frozenset({3}), hook=hook
    )
    assert before == [(118.0, 0)], "the premise: the bound would lower this target to 102.0"
    after = run.snapshots[2]
    assert (after["target"], after["generation"]) == (118.0, 0.0), after


def test_after_a_sink_change_between_exit_batches_the_new_sink_records_its_own_rate(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """SPEC-105 FR-002 AC4 (invariant 6): the old rate lowers only a target already read as zero.

    After the first exit batch (256 events a second) the sink changes to one costing 0.0078125 s
    an event, whose first exit batch fails after 0.0625 s. The old rate then lowers the old
    sink's 118.0 target to 102.0, which ``_size_target`` reads as zero, so the next batch is the
    count trigger's; the new sink delivers it and records its own 128.0, not the old 256.0.
    """
    new = _ExitSink()
    new.first_fails = True

    def between(run: _FitRun, index: int) -> None:
        if index == 1:
            assert run.worker is not None
            _charging(run, new, {1: _FAIL_COST}, per_event=0.0078125)
            _reassign(run.worker, new)

    run = _fit_exit(monkeypatch, capsys, deadline=0.578125, spans=500, between=between)
    assert new.handed[:2] == [20, 20], new.handed
    failed = run.snapshots[2]
    assert failed["size_target"] == 0.0, failed
    assert failed["rate"] == 256.0, "the premise: the old sink's rate stood through the failure"
    assert (failed["target"], failed["generation"]) == (102.0, 0.0), failed
    assert run.snapshots[3]["rate"] == 128.0, run.snapshots[3]


def test_a_dead_sink_at_exit_records_no_rate_and_keeps_its_target(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """SPEC-105 FR-002 AC5 (invariant 2): a dead sink with no exit batch delivered is unchanged.

    The pre-stop batch is delivered, so the target is the cap, and every exit batch fails at
    once. No exit batch is delivered, so no rate is recorded and the kept target stands: 4,000
    events go in four batches at the cap, as ``main`` hands them. With a bound applied before
    any rate was recorded, the target would be zero and the backlog would go ten spans a call.
    """
    sink = _ExitSink(fails=True)
    rates: list[float] = []
    holder: list[Worker] = []
    sink.on_call = lambda call: rates.append(holder[0]._exit_rate) if call > 1 else None
    worker = _at_exit(sink, 2_000, max_retries=0)
    holder.append(worker)
    retire_process(worker, 30.0)
    worker._thread.join(5.0)
    capsys.readouterr()
    assert sink.handed[1:] == [1_000] * 4, sink.handed
    assert rates == [0.0] * 4, rates
    assert worker._exit_rate == 0.0


def test_the_bound_is_not_applied_once_the_deadline_is_within_the_margin(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """SPEC-105 FR-003 AC2 (invariants 2 and 3): with no time left, the declined backlog stands.

    After the first exit batch the time left less the margin is negative. The guard leaves the
    rate rule's 128.0, so the declined backlog is 8 batches; a bound applied there would drive
    the target below zero and cut it into 49.
    """
    run = _fit_exit(monkeypatch, capsys, deadline=0.09765625, spans=500)
    assert run.sink.handed[1:] == [20], run.sink.handed
    assert run.worker is not None
    assert run.worker._target == 128.0
    assert _declined(run.err) == [(980, 8)], run.err


def test_the_bound_is_not_applied_where_the_prediction_declines_the_next_batch(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """SPEC-105 FR-003 AC3 (invariants 2 and 3): the guard, not ``room > 0``, keeps 3 batches.

    The second exit batch, 122 events, ends at 0.5546875; the time left less the margin is then
    positive but below the longest attempt, so the next batch is declined whatever its size. A
    guard weakened to ``room > 0`` would lower the target to about 1.6 and name 13 batches.
    """
    run = _fit_exit(monkeypatch, capsys, deadline=0.6, spans=200)
    assert run.sink.handed[1:] == [20, 122], run.sink.handed
    assert _declined(run.err) == [(258, 3)], run.err


@pytest.mark.parametrize("deadline", [64.0, None])
def test_an_ample_deadline_hands_the_unbounded_batches(
    deadline: float | None,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """SPEC-105 FR-005 AC1 (invariant 3): where the budget does not bind, nothing changes.

    At 256 events a second and a 1.0 s interval the rate rule gives 128 events a batch. With a
    64.0 deadline and with none, 2,000 spans go as 20, then 31 batches of 128, then the 12 left:
    both runs meet one sequence, so the bound changed nothing an ample budget sends.
    """
    run = _fit_exit(monkeypatch, capsys, deadline=deadline, spans=2_000)
    assert run.sink.handed[1:] == [20] + [128] * 31 + [12], run.sink.handed


def test_a_batch_ending_inside_the_last_margin_is_trimmed_and_its_tail_declined(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """SPEC-105 FR-005 AC2 (invariants 2 and 3): the band Andrew chose on 2026-11-17.

    Without the bound the second exit batch is 128 events ending at 0.578125, inside the last
    margin before 0.6, and nothing is declined. With it the batch is 122 events and the 6 left
    are declined, counted and announced.
    """
    run = _fit_exit(monkeypatch, capsys, deadline=0.6, spans=74)
    assert run.sink.handed[1:] == [20, 122], run.sink.handed
    assert _declined(run.err) == [(6, 1)], run.err


_TAKE = 2.0**-20
"""Seconds that pass between a fit and the next prediction: the take of the next batch's items."""


def _take_after(index: int) -> Any:
    """A ``between`` hook that moves the clock on by ``_TAKE`` after exit batch ``index``."""

    def between(run: _FitRun, i: int) -> None:
        if i == index:
            run.clock.now += _TAKE

    return between


@pytest.mark.parametrize("engine", [False, True], ids=["1x", "engine"])
def test_a_batch_declined_after_the_guard_passed_restores_the_rate_rules_target(
    engine: bool, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """SPEC-105 FR-003, diff review frame 2 (invariants 2 and 6): the guard and the prediction differ.

    The guard is asked when an exit batch returns and the prediction when the next one is
    emitted, after its items were taken. With a 0.1953125 deadline the room after the first exit
    batch equals its 0.078125 attempt exactly, so the guard passes and lowers the target to 20;
    ``_TAKE`` later the prediction declines the next batch. That batch was already taken at the
    lowered target, so it is 20 events, and the decline then restores the rate rule's 128.0, so
    the rest is cut as ``main`` cuts it: 9 declined batches where ``main`` names 8, and 49
    without the restore.
    """
    run = _fit_exit(
        monkeypatch, capsys, deadline=0.1953125, spans=500, engine=engine, between=_take_after(1)
    )
    assert run.sink.handed[1:] == [20], run.sink.handed
    assert _declined(run.err) == [(980, 9)], run.err
    assert run.worker is not None
    assert run.worker._target == 128.0


def test_the_guard_admits_a_batch_whose_room_equals_the_longest_attempt(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """SPEC-105 FR-003, diff review frame 2 (invariant 3): the guard's boundary is ``>=``.

    The same 0.1953125 deadline with no time between the fit and the prediction: the room
    after the first exit batch equals its attempt, so the bound gives 20 and the prediction
    starts a batch of 20, which returns at 0.15625. A guard of ``room > longest`` would leave
    the rate rule's 128, which the prediction also starts, and which ends at 0.578125.
    """
    run = _fit_exit(monkeypatch, capsys, deadline=0.1953125, spans=500)
    assert run.sink.handed[1:] == [20, 20], run.sink.handed
    assert run.snapshots[2]["now"] == 0.15625, run.snapshots
    assert _declined(run.err) == [(960, 8)], run.err


def test_the_first_declined_batch_is_cut_at_the_rate_rules_target(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """SPEC-105 FR-003, diff review frame 2 (invariants 2 and 3): the guard's refusal is visible.

    The second exit batch, 122 events, ends at 0.5546875 under a 0.6 deadline; the room left is
    then positive but below the longest attempt, so the guard leaves the rate rule's 128 and the
    256 events left are declined in 2 batches. A guard weakened to ``room > 0``, or none, would
    lower the target to about 1.6, so the first declined batch would be ten spans and the
    decline's restore would cut the 236 behind it in 2 more: 3 batches.
    """
    run = _fit_exit(monkeypatch, capsys, deadline=0.6, spans=199)
    assert run.sink.handed[1:] == [20, 122], run.sink.handed
    assert _declined(run.err) == [(256, 2)], run.err


def test_a_declined_exit_batch_with_no_rate_recorded_leaves_the_target_as_it_was(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """SPEC-090 FR-001 rule 4 under SPEC-105: a decline restores nothing when no rate exists.

    The first exit batch costs no time, so it records no rate and ``_set_target`` gives the cap.
    Under a 0.03125 deadline, inside the margin, every later batch is declined, and the 1,980
    events left go at the cap in 2 batches. A restore made without a rate would set the target
    to zero after the first, and cut the 980 behind it into 49 more.
    """
    run = _fit_exit(monkeypatch, capsys, deadline=0.03125, spans=1_000, free=frozenset({2}))
    assert run.sink.handed[1:] == [20], run.sink.handed
    assert _declined(run.err) == [(1_980, 2)], run.err
    assert run.worker is not None
    assert run.worker._target == 1_000.0


def test_a_pre_stop_batch_delivered_while_the_stop_is_claimed_records_no_rate(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """SPEC-105 FR-005 AC4 (invariant 6): a batch begun before the stop is not an exit batch.

    The pre-stop call costs 0.078125 and the stop is claimed during it, with a deadline of
    0.328125, under which the bound would lower its 128.0 to 54. It is delivered after the
    claim, but its first attempt began before it, so the rate rule alone sets its target.
    """
    run = _fit_exit(monkeypatch, capsys, deadline=0.25, spans=500, pre_stop_cost=0.078125)
    assert run.snapshots[0]["target"] == 128.0, run.snapshots[0]
    assert run.snapshots[0]["rate"] == 0.0, run.snapshots[0]
    assert run.sink.handed[1] == 128, run.sink.handed


_RECOVERED_COST = 0.002
"""Seconds a sink that recovered by exit costs an event, on the real clock (SPEC-105 FR-004)."""

_TIGHT_BUDGET = 0.45
"""The ``shutdown()`` budget at which ``main`` expires its join behind that sink."""


def _recovered_sink(sink: Any) -> Any:
    """Makes ``sink`` FR-004's: a pre-stop call that idles 0.2 s and fails, then 2 ms an event."""
    sink.first_seconds = 0.2
    if isinstance(sink, _ExitSink):
        sink.first_fails = True

    def on_call(call: int) -> None:
        if call > 1:
            threading.Event().wait(_RECOVERED_COST * sink.handed[call - 1])

    sink.on_call = on_call
    return sink


def test_a_tight_shutdown_completes_its_join_behind_a_sink_that_recovered_by_exit(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """SPEC-105 FR-004 AC1 and AC2 (invariants 2, 3 and 5): the join completes, the sink closes.

    2,000 two-event spans queued behind a pre-stop call that idles 0.2 s once the stop is
    claimed and fails, so the target is zero and the exit drain has about 0.25 s. The first
    exit batch, 20 events, costs about 0.04 s; the rate rule would make the next about 250
    events, 0.5 s, and on ``main`` the join expired. The bound sizes it to end by the deadline
    less the margin, and the rest is declined.

    **What makes this robust on a loaded runner.** The join waits on the real clock, and the
    bound reads that same clock after each batch, so a late wake from the idle or a slow first
    batch only shrinks or declines what follows; the rate is measured on the loaded machine in
    the batch just before. What no bound absorbs is one call running more than ``_JOIN_MARGIN``
    past its own measured rate, which is invariant 3's designed margin. No batch size is
    asserted. ``_SLACK`` covers only the return after the join: the inline ``close()`` and the
    caller being scheduled. An expired join also returns within the budget, so
    ``stopped_reason`` and the close count are what tell this tree from one without the bound.
    """
    sink = _recovered_sink(_ExitSink())
    worker = _at_exit(sink, 2_000, max_retries=0, flush_interval=1.0)
    started = time.monotonic()
    retire_process(worker, _TIGHT_BUDGET)
    elapsed = time.monotonic() - started
    closes_at_return = sink.closes
    worker._thread.join(5.0)
    err = capsys.readouterr().err
    assert elapsed < _TIGHT_BUDGET + _SLACK, f"shutdown returned at {elapsed:.2f}s"
    assert worker.stopped_reason is None, err
    assert "shutdown timed out" not in err, err
    assert closes_at_return == 1
    abandoned = [n for n, _what, detail in _lost_lines(err) if "batch abandoned" in detail]
    assert abandoned == [20], err
    declined = sum(n for n, _batches in _declined(err))
    assert sink.events() + 20 + declined == 4_020, err
    assert ledger_check.worker_problems(worker) == []


def test_the_engine_twin_completes_its_join_at_a_tight_budget(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """SPEC-105 FR-004 AC4 (invariant 6): the same shape through ``deliver``, ``max_retries=0``."""
    sink = _recovered_sink(_ChargedEngineSink())
    worker = _at_exit(sink, 2_000, max_retries=0, flush_interval=1.0)
    retire_process(worker, _TIGHT_BUDGET)
    closes_at_return = sink.closes
    worker._thread.join(5.0)
    err = capsys.readouterr().err
    assert worker.stopped_reason is None, err
    assert closes_at_return == 1
    assert ledger_check.worker_problems(worker) == []
