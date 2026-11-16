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
import math
import pathlib
import re
import threading
from typing import Any

import pytest

import log_foundry
from conftest import install_worker, retire_process
from ledger_sinks import ledger
from log_foundry import Outcome, SinkCounters, Verdict, emit_via_deliver
from log_foundry import worker as worker_mod
from log_foundry.sinks.http import HTTPSink
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


def _at_exit(sink: Any, queued: int, *, batch_size: int = 10, max_retries: int = 3) -> Worker:
    """Builds and installs a worker holding one pre-stop batch, with ``queued`` spans behind it."""
    worker = Worker(sink, batch_size=batch_size, flush_interval=3_600.0, max_retries=max_retries)
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
    raises. Each failed run writes the engine's own ``absorbed`` line, which the exit tallies
    cannot aggregate, so the lines grow with the number of exit batches. Before SPEC-090 the
    whole backlog was one call and two lines; the bound asserted here is the backlog over the
    cap, plus one for a carry, in calls, and that plus two lines (the pre-stop path writes none,
    the first abandoned exit batch writes the worker's line, the tally one more). Had the first
    abandoned exit batch reset the target, the rest would go ten spans a call: about 150 calls
    and 150 lines here (diff review, 2026-11-16).
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

    ``HTTPSink`` writes its own line per failed request, which the exit tallies cannot
    aggregate either. Its opener answers the pre-stop batch once the stop is claimed, so the
    target is the cap, then refuses every request. With no retries at either level, each exit
    batch is one request, so the opener calls and the lines are bounded as the engine twin's
    are, where a reset target made them about 150 each.
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
