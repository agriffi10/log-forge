"""SPEC-072 FR-005 and FR-006: the worker, orphan and inline-adapter paths run the engine.

Worker cases build a real ``Worker`` with ``batch_size=10, flush_interval=60.0`` (the exit-budget
file's shape) so nothing drains before the test asks; orphan cases go through ``log_foundry.info``
with no span. Serves invariants 2, 3, 4, 6 and 7: the worker and orphan twins keep one ledger,
retries stay on the caller's thread on the orphan path, ``emit`` and ``deliver`` of one sink are a
twin pair, total failure raises and partial loss reports, and no ``dropped`` count is made twice
across a catcher's retry.
"""

from __future__ import annotations

import ast
import pathlib
import re
import threading
import time
from typing import Any

import pytest

import log_foundry
from conftest import install_worker, retire_process
from log_foundry import _lifecycle
from log_foundry import worker as worker_mod
from log_foundry.sinks import _engine
from log_foundry.sinks._engine import emit_via_deliver, run_delivery
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses, read_losses
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict
from log_foundry.worker import Worker
from test_worker_exit_budget import _lost_lines, _measured
from verdict_sinks import (
    InheritsAdapter,
    MisspeltCounters,
    OptOut,
    Scripted,
    Stamping,
    ThirdPartyWrapper,
    WaitRecorder,
    events,
    metaclass_named,
)

A, R, J, D, U, S = (
    Outcome.ACCEPTED,
    Outcome.RETRYABLE,
    Outcome.REJECTED,
    Outcome.DROPPED,
    Outcome.UNKNOWN,
    Outcome.SKIPPED,
)
_WORKER = pathlib.Path(worker_mod.__file__)
_ENGINE = pathlib.Path(_engine.__file__)


def _worker(sink: object, **kwargs: Any) -> Worker:
    """A worker that drains only when asked, installed as the process worker."""
    kwargs.setdefault("batch_size", 10)
    kwargs.setdefault("flush_interval", 60.0)
    worker = Worker(sink, **kwargs)  # type: ignore[arg-type]
    install_worker(worker)
    return worker


def _drained(worker: Worker, batch: list[dict[str, object]]) -> bool:
    """Submits one batch and flushes it, returning the flush's truth."""
    worker.submit(batch)
    return bool(worker.flush(timeout=20.0))


def _counts(sink: Any) -> tuple[int, int]:
    """``(dropped_events, failed_events)``."""
    losses = sink.log_foundry_counters.losses()
    return losses.dropped, losses.failed


class _EmitRaises(Scripted):
    """An engine sink whose own ``emit`` must never run: ``deliver`` is defined beside it."""

    deliver = Scripted.deliver

    def emit(self, batch: list[dict[str, object]]) -> None:
        raise AssertionError("emit was called for a sink that offers deliver")


# --- FR-005: the worker path -------------------------------------------------------------


def test_an_engine_sink_is_delivered_through_deliver_on_both_paths() -> None:
    """FR-005 AC1: the worker and an orphan ``info()`` call ``deliver``, never ``emit``."""
    sink = _EmitRaises([A])
    worker = _worker(sink)
    assert _drained(worker, events(3))
    assert sink.calls == 1 and len(sink.rounds[0]) == 3
    assert sink.threads[0] == worker._thread.name

    log_foundry.configure(service="paths", sink=sink)
    log_foundry.info("orphan")
    assert sink.calls == 2 and sink.threads[1] == threading.current_thread().name
    assert log_foundry.health().orphan_lost == 0


def test_a_total_failure_is_one_abandoned_batch_on_the_worker_path(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """FR-005 AC2: 12 calls in one run, one abandoned batch, 1.x's exact line, falsy flush."""
    sink = Scripted([R], max_retries=2)
    worker = _worker(sink, max_retries=3)
    flushed = _drained(worker, events(5))
    assert sink.calls == 12
    assert all(len(r) == 5 for r in sink.rounds), "no whole-batch resend around the run"
    assert worker.failed_batches == 1
    assert not flushed
    assert _counts(sink) == (0, 5)
    assert capsys.readouterr().err == (
        "log-foundry: lost 5 event(s); batch abandoned after 4 emit attempts\n"
    )


def test_a_raising_deliver_is_one_abandoned_batch_on_the_worker_path(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """FR-005 AC2: the same counter and flush; the line counts the one cycle the run started."""
    sink = Scripted([ConnectionRefusedError("secret-token")], max_retries=2)
    worker = _worker(sink, max_retries=3)
    flushed = _drained(worker, events(2))
    assert sink.calls == 1
    assert worker.failed_batches == 1 and not flushed
    assert _counts(sink) == (0, 2)
    assert capsys.readouterr().err == (
        "log-foundry: absorbed a failure while delivering a batch (ConnectionRefusedError); its "
        "round's events not confirmed by Scripted\n"
        "log-foundry: lost 2 event(s); batch abandoned after 1 emit attempts\n"
    )


def test_a_partial_outcome_settles_on_the_worker_path(capsys: pytest.CaptureFixture[str]) -> None:
    """FR-005 AC3: ``[ACCEPTED, REJECTED]`` is no abandoned batch, a truthy flush, one line."""
    sink = Scripted([[A, J]])
    worker = _worker(sink)
    assert _drained(worker, events(2))
    assert worker.failed_batches == 0
    assert _counts(sink) == (0, 1)
    err = capsys.readouterr().err
    assert len(err.splitlines()) == 1 and err.startswith("log-foundry: lost 1 event(s); rejected 1")


def test_a_malformed_verdict_settles_on_the_worker_path() -> None:
    """FR-005 AC3: an unreadable verdict is counted and the flush stays true."""
    sink = Scripted([lambda batch: {"not": "a verdict"}])
    worker = _worker(sink)
    assert _drained(worker, events(2))
    assert worker.failed_batches == 0 and _counts(sink) == (0, 2)


def test_a_deliver_sink_without_max_retries_gets_the_workers_retries() -> None:
    """FR-005 AC4: ``w + 1`` calls for a sink that declares no ``max_retries``."""
    sink = Scripted([R])
    worker = _worker(sink, max_retries=2)
    assert not _drained(worker, events(1))
    assert sink.calls == 3


@pytest.mark.parametrize(("timeout", "calls"), [(1.2, 2), (0.9, 1), (1.8, 3)])
def test_the_exit_drain_declines_through_deliver(
    timeout: float, calls: int, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-005 AC5 and FR-002 AC7: SPEC-062's D=0.5 cases, one close, the declined wording.

    The sink declares no ``max_retries``, so the run is ``w + 1`` cycles of one attempt with the
    worker's 0.01-0.04 s cycle waits between them; with ``s >= 1`` the within-cycle 0.1 s wait
    would put ``0.5 + 0.1 + 0.5 + 0.1`` exactly on the 1.2 s knife edge.
    """
    sink = Scripted([R], seconds=0.5)
    worker = _worker(sink)
    worker.submit(events(3))
    elapsed = _measured(lambda: _lifecycle._shutdown_worker(timeout))
    closes_at_return = sink.closes
    worker._thread.join(5.0)
    assert sink.calls == calls
    assert closes_at_return == 1
    assert worker.failed_batches == 1
    assert elapsed < timeout + 0.3
    lost = _lost_lines(capsys.readouterr().err)
    assert lost == [
        (
            3,
            "event",
            (
                f"; retry declined after {calls} emit attempt(s); the next was predicted to end "
                "after the shutdown() deadline"
            ),
        )
    ]


def test_an_expired_join_counts_the_events_inside_deliver(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """FR-005 AC5: ``_in_flight`` is set around the run, so the expiry line counts a deliver."""
    inside, release = threading.Event(), threading.Event()

    def wedged(batch: list[dict[str, object]]) -> Verdict:
        inside.set()
        release.wait(30.0)
        return Verdict.uniform(A, len(batch))

    sink = Scripted([wedged])
    worker = _worker(sink)
    worker.submit(events(4))
    try:
        _lifecycle._shutdown_worker(0.2)
        assert inside.is_set(), "the premise: the drain thread is inside deliver"
        err = capsys.readouterr().err
    finally:
        release.set()
        worker._thread.join(5.0)
    assert "4 event(s) in flight, counted when the emit returns" in err, err
    assert worker._in_flight == 0


def test_the_workers_predicate_declines_through_deliver() -> None:
    """FR-002 AC7, unit form: a deadline of None declines nothing; a passed one still makes one."""
    worker = _worker(Scripted([A]))
    sink = Scripted([R])
    run_delivery(
        sink, events(1), worker_retries=3, stop=WaitRecorder(), decline=worker._retry_declined
    )
    assert sink.calls == 4, "no deadline: every round the budget allows"
    worker._exit_deadline = time.monotonic() - 1.0
    late = Scripted([R])
    delivery = run_delivery(
        late, events(1), worker_retries=3, stop=WaitRecorder(), decline=worker._retry_declined
    )
    assert late.calls == 1, "the first round past the deadline is still made"
    assert delivery is not None and delivery.declined and delivery.failed


def _join_margin_compares(source: str) -> list[str]:
    """The scope of every comparison that names ``_JOIN_MARGIN`` anywhere inside it.

    Every ``Compare`` node in the module is read, at any depth and in any scope — a method, an
    async function, a lambda, a class body or the module itself — and named by the qualified
    path of the scopes enclosing it (``Worker._retry_declined``, ``f.<lambda>``, ``<module>``).
    A use of the name outside a comparison — its assignment, an argument — is not one.
    """
    found: list[str] = []

    def visit(node: ast.AST, scope: tuple[str, ...]) -> None:
        for child in ast.iter_child_nodes(node):
            inner = scope
            if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
                inner = (*scope, child.name)
            elif isinstance(child, ast.Lambda):
                inner = (*scope, "<lambda>")
            if isinstance(child, ast.Compare) and any(
                isinstance(n, ast.Name) and n.id == "_JOIN_MARGIN" for n in ast.walk(child)
            ):
                found.append(".".join(scope) or "<module>")
            visit(child, inner)

    visit(ast.parse(source), ())
    return found


def test_the_join_margin_comparison_is_written_once_in_retry_declined() -> None:
    """FR-002 AC9: exactly one comparison naming ``_JOIN_MARGIN``, in ``Worker._retry_declined``."""
    worker_source = _WORKER.read_text(encoding="utf-8")
    assert any(
        isinstance(n, ast.AnnAssign | ast.Assign)
        and "_JOIN_MARGIN" in {t.id for t in ast.walk(n) if isinstance(t, ast.Name)}
        for n in ast.parse(worker_source).body
    ), "worker.py no longer defines _JOIN_MARGIN"
    owners = [f"worker.py:{scope}" for scope in _join_margin_compares(worker_source)]
    owners += [
        f"_engine.py:{scope}"
        for scope in _join_margin_compares(_ENGINE.read_text(encoding="utf-8"))
    ]
    assert owners == ["worker.py:Worker._retry_declined"], owners


def test_the_join_margin_scan_reads_every_scope_and_only_comparisons() -> None:
    """The scan's corpus: each scope a comparison can hide in is named, and other uses are not."""
    source = (
        "_JOIN_MARGIN = 0.1\n"
        "keep = [_JOIN_MARGIN, min(_JOIN_MARGIN, 1)]\n"
        "early = 1 > _JOIN_MARGIN\n"
        "class W:\n"
        "    def a(self, t):\n"
        "        return t - _JOIN_MARGIN < 0\n"
        "    async def b(self):\n"
        "        return (lambda x: x + _JOIN_MARGIN >= 2)(1)\n"
        "    def c(self):\n"
        "        return f(_JOIN_MARGIN), _JOIN_MARGIN * 2\n"
    )
    assert _join_margin_compares(source) == ["<module>", "W.a", "W.b.<lambda>"]
    assert _join_margin_compares("_JOIN_MARGIN = 0.1\nx = f(_JOIN_MARGIN)\n") == []


def _emit_method() -> ast.FunctionDef:
    tree = ast.parse(_WORKER.read_text(encoding="utf-8"))
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "Worker")
    found = next(
        (n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == "_emit"), None
    )
    assert found is not None, "Worker._emit is gone: the backoff guard lost its anchor"
    return found


def test_worker_py_holds_no_backoff_literal_in_a_wait_call() -> None:
    """FR-002 AC4: no ``0.01``/``0.5`` in ``Worker._emit``, and every wait takes ``cycle_backoff``."""
    emit = _emit_method()
    literals = [
        node.lineno
        for node in ast.walk(emit)
        if isinstance(node, ast.Constant)
        and node.value in (0.01, 0.5)
        and type(node.value) is float
    ]
    assert literals == [], f"a backoff literal is back in Worker._emit at {literals}"
    waits = [
        node
        for node in ast.walk(emit)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "wait"
    ]
    assert waits, "no wait call found in Worker._emit: the guard cannot see the loop"
    for call in waits:
        assert len(call.args) == 1 and ast.unparse(call.args[0]).startswith(
            "_engine.cycle_backoff("
        ), ast.unparse(call)
    whole = ast.parse(_WORKER.read_text(encoding="utf-8"))
    for node in ast.walk(whole):
        if isinstance(node, ast.Call) and (
            (isinstance(node.func, ast.Attribute) and node.func.attr == "wait")
            or (isinstance(node.func, ast.Name) and node.func.id == "wait")
        ):
            assert not any(
                isinstance(c, ast.Constant) and c.value in (0.01, 0.5) and type(c.value) is float
                for c in ast.walk(node)
            ), ast.unparse(node)


class _AlwaysFails:
    def __init__(self) -> None:
        self.attempts = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        self.attempts += 1
        raise RuntimeError("down")

    def close(self) -> None:
        pass


def test_the_workers_emit_loop_survives_a_huge_max_retries(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """P8: the 1.x loop raised ``OverflowError`` from attempt 1025; the bounded exponent ends it."""
    sink = _AlwaysFails()
    worker = _worker(sink, max_retries=1100)
    stop = threading.Event()
    stop.set()
    worker._stop = stop
    worker._emit([[{"m": 1}]])
    assert sink.attempts == 1101
    assert worker.failed_batches == 1
    assert _lost_lines(capsys.readouterr().err) == [
        (1, "event", "; batch abandoned after 1101 emit attempts")
    ]


def _mutating(mutation: str, outcomes: tuple[Outcome, ...]) -> Any:
    """A ``deliver`` step that clears or extends the list it was handed, then answers."""

    def mutate(handed: list[dict[str, object]]) -> Verdict:
        n = len(handed)
        if mutation == "clear":
            handed.clear()
        else:
            handed.extend(events(7, "x"))
        return Verdict(outcomes=outcomes[:n])

    return mutate


@pytest.mark.parametrize("mutation", ["clear", "extend"])
def test_a_deliver_that_mutates_its_argument_through_a_real_worker(
    mutation: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """M1: a failed run's line counts the batch as it was before the run, not as left."""
    sink = Scripted([_mutating(mutation, (J, J, J))])
    worker = _worker(sink, max_retries=0)
    flushed = _drained(worker, events(3))
    assert worker._thread.is_alive() and worker.stopped_reason is None
    assert not flushed and worker.failed_batches == 1
    assert _counts(sink) == (0, 3)
    assert _lost_lines(capsys.readouterr().err) == [
        (3, "event", "; batch abandoned after 1 emit attempts")
    ]


@pytest.mark.parametrize("mutation", ["clear", "extend"])
def test_a_later_round_indexes_the_snapshot_through_a_real_worker(
    mutation: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """M1: round two is built from the engine's copy, so no ``Exception`` reaches the worker.

    Round one empties (or grows) the list it was handed and leaves event 0 retryable; were the
    engine indexing that list, round two would raise ``IndexError`` on the drain thread.
    """
    sink = Scripted([_mutating(mutation, (R, J, A)), A])
    worker = _worker(sink, max_retries=1)
    batch = events(3)
    flushed = _drained(worker, batch)
    assert worker._thread.is_alive() and worker.stopped_reason is None
    assert flushed and worker.failed_batches == 0
    assert sink.calls == 2 and sink.rounds[1] == [batch[0]]
    assert _counts(sink) == (0, 1)
    assert _lost_lines(capsys.readouterr().err) == [
        (
            1,
            "event",
            (
                "; rejected 1, unresolved 0, raised 0, unknown 0, dropped 0 after 2 "
                "delivery round(s) to Scripted"
            ),
        )
    ]


def test_an_opt_out_subclass_runs_its_own_emit_and_never_the_inherited_deliver() -> None:
    """FR-001 AC6 through a real worker: ``deliver = None`` with its own ``emit``."""
    sink = OptOut([AssertionError("the inherited deliver ran")])
    worker = _worker(sink)
    assert _drained(worker, events(2))
    assert [e["i"] for e in sink.emitted] == [0, 1]
    assert sink.calls == 0


def test_an_opt_out_that_inherits_the_adapter_still_runs_the_engine_through_emit() -> None:
    """Ruling 4: not offered (the worker calls ``emit``), yet the adapter walks past the ``None``."""
    sink = InheritsAdapter([[A, R], [A]], max_retries=1)
    assert run_delivery(sink, events(1), worker_retries=0, stop=None) is None
    worker = _worker(sink)
    assert _drained(worker, events(2))
    assert sink.calls == 2 and _counts(sink) == (0, 0)


# --- FR-005: the orphan path -----------------------------------------------------------------


_ORPHAN_CASES = {
    "accepted": ([A], 0),
    "dropped": ([D], 0),
    "unknown": ([U], 0),
    "skipped": ([S], 0),
    "malformed": ([lambda batch: None], 0),
    "rejected": ([J], 1),
    "raising": ([ConnectionError("x")], 1),
}


@pytest.mark.parametrize("case", sorted(_ORPHAN_CASES))
def test_the_orphan_path_counts_a_loss_only_for_a_failed_run(case: str) -> None:
    """FR-005 AC6: ``orphan_lost`` moves for a rejected or raising run only; nothing raises."""
    script, lost = _ORPHAN_CASES[case]
    sink = Scripted(script)
    log_foundry.configure(service="paths", sink=sink)
    log_foundry.info("orphan")
    assert sink.calls == 1
    assert log_foundry.health().orphan_lost == lost


def test_orphan_retries_stay_on_the_callers_thread() -> None:
    """FR-005 AC6: ``max_retries=2`` gives 3 calls, all on the caller's thread, then one loss."""
    sink = Scripted([R], max_retries=2)
    log_foundry.configure(service="paths", sink=sink)
    log_foundry.info("orphan")
    assert sink.calls == 3
    assert set(sink.threads) == {threading.current_thread().name}
    assert log_foundry.health().orphan_lost == 1


_TWIN_CASES = {
    "accepted": [A],
    "retryable": [R],
    "rejected": [J],
    "dropped": [D],
    "unknown": [U],
    "skipped": [S],
    "raising": [ConnectionError("x")],
    "malformed": [lambda batch: Verdict(outcomes=())],
}


@pytest.mark.parametrize("case", sorted(_TWIN_CASES))
def test_the_worker_and_orphan_twins_leave_the_same_losses(case: str) -> None:
    """FR-005 AC7: one event via ``Worker.submit`` and via an orphan ``info`` count alike."""
    by_worker = Scripted(_TWIN_CASES[case])
    worker = _worker(by_worker)
    _drained(worker, events(1))
    retire_process()
    by_orphan = Scripted(_TWIN_CASES[case])
    log_foundry.configure(service="paths", sink=by_orphan)
    log_foundry.info("orphan")
    assert read_losses(by_worker) == read_losses(by_orphan)


def test_a_swap_between_emit_and_deliver_sinks_takes_effect_per_batch() -> None:
    """FR-005 AC8: the probe is read per batch, so each configure takes the next batch."""

    class EmitOnly:
        def __init__(self) -> None:
            self.events: list[dict[str, object]] = []

        def emit(self, batch: list[dict[str, object]]) -> None:
            self.events.extend(batch)

        def close(self) -> None:
            pass

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("in")

    engine, plain = Scripted([A]), EmitOnly()
    log_foundry.configure(service="paths", sink=engine)
    work()
    assert log_foundry.flush(timeout=10.0)
    first = engine.calls
    log_foundry.configure(service="paths", sink=plain)
    work()
    assert log_foundry.flush(timeout=10.0)
    assert engine.calls == first and plain.events
    log_foundry.configure(service="paths", sink=engine)
    work()
    assert log_foundry.flush(timeout=10.0)
    assert engine.calls > first


def test_counts_survive_a_shutdown_and_a_new_worker() -> None:
    """FR-003 AC5: the counters live on the sink, so a new worker adds to the same totals."""
    sink = Scripted([J])
    log_foundry.configure(service="paths", sink=sink)
    log_foundry.info("one")
    log_foundry.shutdown(timeout=5.0)
    log_foundry.configure(service="paths", sink=sink)
    log_foundry.info("two")
    assert _counts(sink) == (0, 2)


def test_counts_survive_a_swap_and_back() -> None:
    """FR-003 AC5: ``configure(sink=b)`` then ``configure(sink=a)`` leaves ``a``'s totals."""
    a, b = Scripted([J]), Scripted([D])
    log_foundry.configure(service="paths", sink=a)
    log_foundry.info("a")
    log_foundry.configure(service="paths", sink=b)
    log_foundry.info("b")
    log_foundry.configure(service="paths", sink=a)
    assert _counts(a) == (0, 1) and _counts(b) == (1, 0)
    assert log_foundry.health().sink == SinkLosses(dropped=0, failed=1)


# --- FR-006: the inline adapter ------------------------------------------------------------


def test_the_adapter_spends_the_sinks_budget_then_raises() -> None:
    """FR-006 AC1: ``max_retries=2`` and always ``RETRYABLE``: 3 calls, then the raise."""
    sink = Scripted([R], max_retries=2)
    sink.log_foundry_stop_signal = WaitRecorder()
    with pytest.raises(SinkDeliveryError) as caught:
        sink.emit(events(4))
    assert sink.calls == 3
    assert _counts(sink) == (0, 4)
    assert str(caught.value) == (
        "delivered none of 4 event(s): rejected 0, unresolved 4, raised 0 after 3 delivery "
        "round(s) to Scripted"
    )


@pytest.mark.parametrize(
    ("script", "counts"),
    [([A], (0, 0)), ([[A, J]], (0, 1)), ([D], (2, 0)), ([S], (0, 0))],
    ids=["all-accepted", "accepted-rejected", "all-dropped", "all-skipped"],
)
def test_the_adapter_returns_for_every_settled_batch(
    script: list[Any], counts: tuple[int, int]
) -> None:
    """FR-006 AC2: a settled run returns ``None``, its counts moved."""
    sink = Scripted(script)
    assert sink.emit(events(2)) is None  # type: ignore[func-returns-value]
    assert _counts(sink) == counts


def test_the_adapter_raises_for_skipped_beside_rejected() -> None:
    """FR-006 AC2: ``SKIPPED`` is not accepted, so ``[SKIPPED, REJECTED]`` fails."""
    with pytest.raises(SinkDeliveryError):
        Scripted([[S, J]]).emit(events(2))


def test_the_adapter_ignores_an_empty_batch_before_checking_anything() -> None:
    """FR-006 AC2: ``emit([])`` returns without calling ``deliver``, even for a broken sink."""
    sink = Scripted([AssertionError("deliver ran")])
    assert sink.emit([]) is None  # type: ignore[func-returns-value]
    assert sink.calls == 0
    broken = MisspeltCounters()
    assert broken.emit([]) is None  # type: ignore[func-returns-value]
    assert emit_via_deliver(object(), []) is None  # type: ignore[func-returns-value]


@pytest.mark.parametrize("script", [[U], [lambda batch: "unreadable"]], ids=["unknown", "unread"])
def test_the_adapter_never_raises_for_an_unadjudicated_run(script: list[Any]) -> None:
    """FR-006 AC3: ``UNKNOWN`` or an unreadable verdict returns, even with nothing accepted."""
    sink = Scripted(script)
    assert sink.emit(events(2)) is None  # type: ignore[func-returns-value]
    assert _counts(sink) == (0, 2)


def test_the_adapter_raises_when_deliver_raised_and_nothing_landed() -> None:
    """FR-006 AC3: an escaped exception with nothing accepted is a failed run, so it raises."""
    with pytest.raises(SinkDeliveryError, match="raised 2 after 1 delivery round"):
        Scripted([ConnectionError("x")]).emit(events(2))


def test_a_misspelt_counters_attribute_is_a_counted_failure() -> None:
    """FR-006 AC4: ``TypeError`` naming the type, and through a worker a failed batch."""
    sink = MisspeltCounters()
    with pytest.raises(TypeError) as caught:
        sink.emit(events(1))
    assert str(caught.value) == (
        "MisspeltCounters reaches emit_via_deliver with no log_foundry_counters SinkCounters"
    )
    assert sink.calls == 0
    worker = _worker(sink, max_retries=0)
    assert not _drained(worker, events(1))
    assert worker.failed_batches == 1


def test_a_catchers_retry_of_an_adapter_raise_counts_no_drop() -> None:
    """FR-006 AC5: ``[REJECTED, DROPPED]`` raises with no drop, through a wrapper and a retry."""
    sink = Scripted([[J, D]])
    with pytest.raises(SinkDeliveryError):
        sink.emit(events(2))
    assert _counts(sink) == (0, 1)
    fresh = Scripted([[J, D]])
    worker = _worker(ThirdPartyWrapper(fresh), max_retries=1)
    assert not _drained(worker, events(2))
    assert fresh.calls == 2, "both worker attempts reached the adapter"
    assert _counts(fresh) == (0, 2)
    assert worker.failed_batches == 1


@pytest.mark.parametrize("shape", ["wrapper", "stamping-subclass"])
def test_a_flaky_engine_sink_delivers_each_event_once(shape: str) -> None:
    """FR-006 AC6: ``[ACCEPTED, RETRYABLE]`` then ``[ACCEPTED]``: each event once, stamped."""
    if shape == "wrapper":
        inner = Scripted([[A, R], [A]], max_retries=1)
        sink: Any = ThirdPartyWrapper(inner)
    else:
        inner = Stamping([[A, R], [A]], max_retries=1)
        sink = inner
    worker = _worker(sink)
    assert _drained(worker, events(2))
    delivered = [inner.rounds[0][0], inner.rounds[1][0]]
    assert [e["i"] for e in delivered] == [0, 1]
    assert _counts(inner) == (0, 0)
    if shape == "stamping-subclass":
        assert all(e.get("stamped") is True for r in inner.rounds for e in r)


def test_a_shutdown_cuts_the_adapters_backoff_short() -> None:
    """FR-006 AC7: the adapter waits on ``log_foundry_stop_signal``."""
    sink = Scripted([Verdict(outcomes=(R,), retry_after=10.0), A], max_retries=1)
    sink.max_retry_after = 60.0  # type: ignore[attr-defined]
    stop = threading.Event()
    sink.log_foundry_stop_signal = stop
    timer = threading.Timer(0.1, stop.set)
    timer.start()
    try:
        elapsed = _measured(lambda: sink.emit(events(1)))
    finally:
        timer.cancel()
    assert elapsed < 0.5 and sink.calls == 2


def test_the_adapters_typeerror_names_the_stored_class_name() -> None:
    """FR-006 AC8: a metaclass ``__name__`` property can neither rename nor run."""
    calls: list[str] = []
    meta = metaclass_named("Impostor", calls)
    cls = meta("StoredName", (MisspeltCounters,), {})
    with pytest.raises(TypeError, match=r"^StoredName reaches emit_via_deliver"):
        cls().emit(events(1))
    nodeliver = meta("NoDeliver", (), {"log_foundry_counters": SinkCounters()})
    with pytest.raises(TypeError, match=r"^NoDeliver reaches emit_via_deliver with no usable"):
        emit_via_deliver(nodeliver(), events(1))
    assert calls == []


def test_an_explicit_deliver_is_used_and_a_non_callable_one_refused() -> None:
    """FR-006: ``deliver=`` names the callable; one that is not callable is refused."""
    sink = InheritsAdapter([J])
    seen: list[int] = []

    def explicit(batch: list[dict[str, object]]) -> Verdict:
        seen.append(len(batch))
        return Verdict.uniform(A, len(batch))

    emit_via_deliver(sink, events(2), deliver=explicit)
    assert seen == [2] and sink.calls == 0
    with pytest.raises(TypeError, match="no usable deliver"):
        emit_via_deliver(sink, events(1), deliver="not callable")  # type: ignore[arg-type]


def test_a_class_with_no_deliver_after_a_none_is_refused() -> None:
    """The adapter's walk past ``deliver = None`` ends at the MRO's end, refusing, not crashing."""
    cls = type("Nothing", (), {"deliver": None, "log_foundry_counters": SinkCounters()})
    with pytest.raises(TypeError, match=r"^Nothing reaches emit_via_deliver with no usable"):
        emit_via_deliver(cls(), events(1))


def test_the_stop_signal_is_read_statically() -> None:
    """A property stop signal is never called; only an ``Event`` instance is used."""

    class Signals:
        @property
        def log_foundry_stop_signal(self) -> threading.Event:
            raise AssertionError("a property stop signal ran")

    assert _engine.stop_signal_of(Signals()) is None
    holder = type("H", (), {})()
    holder.log_foundry_stop_signal = "not an event"  # type: ignore[attr-defined]
    assert _engine.stop_signal_of(holder) is None
    holder.log_foundry_stop_signal = WaitRecorder()  # type: ignore[attr-defined]
    assert type(_engine.stop_signal_of(holder)) is WaitRecorder


def test_the_api_reports_an_orphan_loss_by_count_only(capsys: pytest.CaptureFixture[str]) -> None:
    """The orphan line names the rounds, never the event; the caller sees nothing raised."""
    sink = Scripted([J])
    log_foundry.configure(service="paths", sink=sink)
    log_foundry.info("CANARY-orphan-value")
    err = capsys.readouterr().err
    assert re.findall(
        r"log-foundry: lost 1 event\(s\); orphan log not delivered after 1 "
        r"delivery round\(s\)",
        err,
    ), err
    assert "CANARY" not in err
