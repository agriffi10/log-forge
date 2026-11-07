"""SPEC-072 FR-002 to FR-004: the delivery engine against scripted test sinks.

The engine is driven through ``run_delivery`` directly, with a recording stop event, so every
round, wait and count is the engine's own and no worker is involved; the worker and orphan paths
are ``tests/test_delivery_paths.py``. Serves invariants 1, 2, 3 and 7: no ``Exception`` escapes
the engine, every event lands in exactly one final fate and is counted at most once per run,
every wait is bounded, idle and cut by the stop event, and a ``deliver`` that raises every call
is a failed batch while an unreadable verdict settles counted.
"""

from __future__ import annotations

import ast
import io
import math
import pathlib
import re
import sys
import threading
import time
import urllib.error
from typing import Any

import pytest

from log_foundry.sinks import _engine
from log_foundry.sinks._engine import Delivery, outcomes_of, run_delivery
from log_foundry.sinks.base import SinkLosses, read_losses
from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict
from test_worker_exit_budget import _measured
from verdict_sinks import Scripted, ThirdPartyWrapper, WaitRecorder, events

A, R, J, D, U, S = (
    Outcome.ACCEPTED,
    Outcome.RETRYABLE,
    Outcome.REJECTED,
    Outcome.DROPPED,
    Outcome.UNKNOWN,
    Outcome.SKIPPED,
)
_ENGINE = pathlib.Path(_engine.__file__)
_WORKER = _ENGINE.parent.parent / "worker.py"
_SRC = _ENGINE.parent.parent


def _deliver(
    sink: object,
    batch: list[dict[str, object]] | None = None,
    *,
    worker_retries: int | None = None,
    stop: threading.Event | None = None,
    decline: Any = None,
) -> Delivery:
    """Runs one engine run and asserts the sink offered ``deliver``."""
    delivery = run_delivery(
        sink,
        events(1) if batch is None else batch,
        worker_retries=worker_retries,
        stop=WaitRecorder() if stop is None else stop,
        decline=decline,
    )
    assert delivery is not None, "the premise: the sink offers deliver"
    return delivery


def _counts(sink: Scripted) -> tuple[int, int]:
    """``(dropped_events, failed_events)``."""
    losses = sink.log_foundry_counters.losses()
    return losses.dropped, losses.failed


def test_an_emit_only_sink_is_not_run() -> None:
    """``None`` tells the caller to use ``emit``; nothing is called."""

    class EmitOnly:
        def emit(self, batch: list[dict[str, object]]) -> None:
            raise AssertionError("never called by the engine")

    assert run_delivery(EmitOnly(), events(2), worker_retries=3, stop=None) is None


def test_an_empty_batch_settles_before_anything_else() -> None:
    """An empty batch calls nothing, counts nothing and settles, whatever else is degenerate."""
    sink = Scripted([RuntimeError("never")], max_retries="nonsense")
    delivery = _deliver(sink, [], worker_retries=-5, stop=None)
    assert delivery == _engine._SETTLED_EMPTY
    assert sink.calls == 0
    assert _counts(sink) == (0, 0)


# --- FR-002: rounds, budget, schedule ---------------------------------------------------------


def test_only_retryable_events_are_offered_again_by_identity_in_order() -> None:
    """FR-002 AC1: round two gets events 1 and 3, round three event 1, each by identity."""
    sink = Scripted([[A, R, J, R], [R, A], [A]], max_retries=5)
    batch = events(4)
    delivery = _deliver(sink, batch)
    assert sink.handed[0] is batch, "round one is handed the batch as given"
    assert [[id(e) for e in r] for r in sink.rounds] == [
        [id(e) for e in batch],
        [id(batch[1]), id(batch[3])],
        [id(batch[1])],
    ]
    assert (delivery.accepted, delivery.rejected, delivery.attempts) == (3, 1, 3)
    assert not delivery.failed


@pytest.mark.parametrize(
    ("declared", "calls"),
    [(0, 4), (3, 16), (None, 4)],
    ids=["s0", "s3", "declares-none"],
)
def test_the_worker_path_budget_is_the_1x_total(declared: int | None, calls: int) -> None:
    """FR-002 AC2: ``(s + 1) * (w + 1)`` rounds with ``w = 3``, ``w + 1`` when none is declared."""
    sink = Scripted([R]) if declared is None else Scripted([R], max_retries=declared)
    delivery = _deliver(sink, events(2), worker_retries=3)
    assert sink.calls == calls
    assert delivery.unresolved == 2 and delivery.failed
    assert _counts(sink) == (0, 2)


@pytest.mark.parametrize(("declared", "calls"), [(2, 3), (None, 1)], ids=["s2", "declares-none"])
def test_the_orphan_path_budget_is_the_sinks_own(declared: int | None, calls: int) -> None:
    """FR-002 AC2: the orphan path has no worker resend, so ``s + 1`` calls."""
    sink = Scripted([R]) if declared is None else Scripted([R], max_retries=declared)
    _deliver(sink, worker_retries=None)
    assert sink.calls == calls


def test_nine_retryables_then_accepted_lands_on_the_tenth_call() -> None:
    """FR-002 AC2: an outage 1.x survived by its multiplied budget is still survived."""
    sink = Scripted([R] * 9 + [A], max_retries=3)
    batch = events(2)
    delivery = _deliver(sink, batch, worker_retries=3)
    assert sink.calls == 10
    assert delivery.accepted == 2 and not delivery.failed
    assert all([id(e) for e in r] == [id(e) for e in batch] for r in sink.rounds)
    assert _counts(sink) == (0, 0)


@pytest.mark.parametrize(
    ("declared", "worker", "calls"),
    [
        (None, None, 1),
        (2.5, 3, 4),
        ("3", 3, 4),
        (True, 3, 4),
        (-4, 3, 4),
        (-4, None, 1),
        (3, -2, 4),
        (3, True, 4),
        (property(lambda self: 9), 3, 4),
    ],
    ids=[
        "none",
        "float",
        "str",
        "bool",
        "negative",
        "negative-orphan",
        "negative-worker",
        "bool-worker",
        "property",
    ],
)
def test_a_count_that_is_not_an_exact_int_reads_as_none(
    declared: object, worker: object, calls: int
) -> None:
    """Only an exact ``int`` counts, floored at zero; anything else is "declared none"."""
    sink = Scripted([R])
    if isinstance(declared, property):
        cls = type("WithProperty", (Scripted,), {"max_retries": declared})
        sink = cls([R])
    elif declared is not None:
        sink.max_retries = declared
    _deliver(sink, worker_retries=worker)  # type: ignore[arg-type]
    assert sink.calls == calls


def test_a_huge_max_retries_does_not_overflow_the_budget() -> None:
    """``10**18`` on both layers: the arithmetic stays exact and the run ends on its outcome."""
    sink = Scripted([R, R, R, R, A], max_retries=10**18)
    delivery = _deliver(sink, worker_retries=10**18)
    assert sink.calls == 5 and delivery.accepted == 1


def test_the_default_schedule_is_the_1x_schedule() -> None:
    """FR-002 AC3: fifteen waits, the sinks' backoff restarting each cycle, 2.87 s in all."""
    stop = WaitRecorder()
    _deliver(Scripted([R], max_retries=3), worker_retries=3, stop=stop)
    expected = [0.1, 0.2, 0.4, 0.01, 0.1, 0.2, 0.4, 0.02, 0.1, 0.2, 0.4, 0.04, 0.1, 0.2, 0.4]
    assert stop.waits == expected
    assert max(w for w in stop.waits if w is not None) <= 0.4
    assert math.isclose(math.fsum(w for w in stop.waits if w is not None), 2.87, abs_tol=1e-9)


def test_the_orphan_schedule_is_one_cycle() -> None:
    """FR-002 AC3: the orphan path with ``s = 3`` waits ``0.1, 0.2, 0.4``."""
    stop = WaitRecorder()
    _deliver(Scripted([R], max_retries=3), worker_retries=None, stop=stop)
    assert stop.waits == [0.1, 0.2, 0.4]


def test_no_cycle_wait_exceeds_the_cap_at_twenty_worker_retries() -> None:
    """FR-002 AC3: with ``w = 20`` every between-cycle wait is at most ``0.5``."""
    stop = WaitRecorder()
    _deliver(Scripted([R]), worker_retries=20, stop=stop)
    assert len(stop.waits) == 20
    assert stop.waits[:7] == [0.01, 0.02, 0.04, 0.08, 0.16, 0.32, 0.5]
    assert set(stop.waits[6:]) == {0.5}


def test_the_backoff_exponent_is_bounded() -> None:
    """No position or cycle overflows: the waits stay finite floats for any count."""
    assert _engine.cycle_backoff(10**9) == 0.5
    assert _engine.cycle_backoff(-3) == 0.01
    assert math.isfinite(_engine._backoff(10**9))
    assert _engine._backoff(-1) == 0.1


def _hinted(hint: object, **attrs: object) -> list[float | None]:
    """The waits of a two-round orphan run whose first verdict carries ``hint``."""
    sink = Scripted([Verdict(outcomes=(R,), retry_after=hint), A], max_retries=1)  # type: ignore[arg-type]
    for name, value in attrs.items():
        setattr(sink, name, value)
    stop = WaitRecorder()
    _deliver(sink, stop=stop)
    assert sink.calls == 2
    return stop.waits


@pytest.mark.parametrize(
    ("hint", "attrs", "waited"),
    [
        (2.5, {"max_retry_after": 1.0}, 1.0),
        (100, {}, 30.0),
        (100, {"max_retry_after": "30"}, 30.0),
        (100, {"max_retry_after": True}, 30.0),
        (100, {"max_retry_after": float("inf")}, 100.0),
        (100, {"max_retry_after": 10**400}, 100.0),
        (10**400, {}, 30.0),
        (10**400, {"max_retry_after": float("inf")}, 86_400.0),
        (0.5, {}, 0.5),
        (2, {"max_retry_after": 5}, 2.0),
    ],
    ids=[
        "ceiling-1",
        "default-30",
        "str-ceiling-default",
        "bool-ceiling-default",
        "inf-ceiling-honours",
        "huge-int-ceiling",
        "huge-int-hint",
        "huge-int-hint-inf-ceiling",
        "small",
        "int-both",
    ],
)
def test_a_hint_is_bounded_by_the_sinks_ceiling(
    hint: object, attrs: dict[str, object], waited: float
) -> None:
    """FR-002 AC4: a usable hint replaces the backoff, under the sink's ``max_retry_after``."""
    assert _hinted(hint, **attrs) == [waited]


@pytest.mark.parametrize(
    ("hint", "attrs"),
    [
        (0, {}),
        (-1, {}),
        (-(10**400), {}),
        (float("inf"), {}),
        (float("nan"), {}),
        ("3", {}),
        (True, {}),
        (None, {}),
        (type("F", (float,), {})(2.0), {}),
        (2.5, {"max_retry_after": 0}),
        (2.5, {"max_retry_after": -1}),
        (2.5, {"max_retry_after": float("nan")}),
    ],
    ids=[
        "zero",
        "negative",
        "huge-negative",
        "inf",
        "nan",
        "str",
        "bool",
        "none",
        "float-subclass",
        "ceiling-zero",
        "ceiling-negative",
        "ceiling-nan",
    ],
)
def test_an_unusable_hint_waits_the_backoff_and_never_raises(
    hint: object, attrs: dict[str, object]
) -> None:
    """FR-002 AC4: the backoff, ``0.1`` before the second attempt, and no exception."""
    assert _hinted(hint, **attrs) == [0.1]


def test_a_hint_between_cycles_replaces_the_cycle_wait() -> None:
    """FR-002 AC4: on the worker path a hint falls on the cycle boundary too."""
    sink = Scripted([Verdict(outcomes=(R,), retry_after=2.0), A])
    stop = WaitRecorder()
    _deliver(sink, worker_retries=3, stop=stop)
    assert stop.waits == [2.0], "the cycle wait 0.01 was replaced by the hint"


def test_a_malformed_verdicts_hint_is_never_read() -> None:
    """The hint is read only after ``outcomes_of`` accepted the verdict (one short here)."""
    sink = Scripted([Verdict(outcomes=(), retry_after=7.0), A])
    sink.log_foundry_idempotent = True  # type: ignore[attr-defined]
    sink.max_retries = 1
    stop = WaitRecorder()
    delivery = _deliver(sink, stop=stop)
    assert stop.waits == [0.1]
    assert delivery.accepted == 1


def _assignments(name: str, tree: ast.Module) -> int:
    count = 0
    for node in ast.walk(tree):
        targets: list[ast.expr] = []
        if isinstance(node, ast.Assign):
            targets = node.targets
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
        count += sum(1 for t in targets if isinstance(t, ast.Name) and t.id == name)
    return count


def test_each_backoff_constant_is_assigned_once_in_the_engine() -> None:
    """FR-002 AC4: each schedule constant has exactly one assignment, a count not a set."""
    tree = ast.parse(_ENGINE.read_text(encoding="utf-8"))
    counts = {
        name: _assignments(name, tree)
        for name in ("BACKOFF_BASE", "CYCLE_BACKOFF_BASE", "CYCLE_BACKOFF_CAP")
    }
    assert counts == {"BACKOFF_BASE": 1, "CYCLE_BACKOFF_BASE": 1, "CYCLE_BACKOFF_CAP": 1}


def test_a_stop_cuts_a_hinted_wait_short_and_idly() -> None:
    """FR-002 AC5: a stop set 0.1 s into a 10 s hinted wait returns within 0.5 s, idle."""
    sink = Scripted([Verdict(outcomes=(R,), retry_after=10.0), A], max_retries=1)
    sink.max_retry_after = 60.0  # type: ignore[attr-defined]
    stop = threading.Event()
    timer = threading.Timer(0.1, stop.set)
    timer.start()
    try:
        elapsed = _measured(lambda: _deliver(sink, stop=stop))
    finally:
        timer.cancel()
    assert elapsed < 0.5, elapsed
    assert sink.calls == 2, "a shutdown shortens a wait and never skips the round it allows"


def test_two_thousand_retries_end_without_overflow() -> None:
    """FR-002 AC6: ``max_retries=2000``, stop set, 2001 calls and no ``OverflowError``."""
    stop = threading.Event()
    stop.set()
    sink = Scripted([R], max_retries=2000)
    delivery = _deliver(sink, stop=stop)
    assert sink.calls == 2001
    assert delivery.unresolved == 1 and delivery.failed


def test_the_engine_holds_no_lock_while_the_sink_runs() -> None:
    """FR-002 AC8: inside ``deliver`` the counters' lock is free."""
    acquired: list[bool] = []

    def probe(batch: list[dict[str, object]]) -> Verdict:
        lock = sink.log_foundry_counters._counter_lock
        took = lock.acquire(blocking=False)
        acquired.append(took)
        if took:
            lock.release()
        return Verdict.uniform(J, len(batch))

    sink = Scripted([R, probe], max_retries=1)
    _deliver(sink, events(2))
    assert sink.calls == 2
    assert acquired == [True], "free in a later round, after a round that counted nothing"
    assert _counts(sink) == (0, 2)


def test_the_decline_predicate_is_asked_after_each_wait_with_the_longest_round() -> None:
    """FR-002 item 5: never before round one, always after the wait; True ends the run declined.

    The deliver calls, the waits and the predicate append to one list, so the order is asserted
    rather than only the count: a predicate asked before its wait is a different list.
    """
    order: list[str] = []
    asked: list[float] = []

    class Ordered(WaitRecorder):
        def wait(self, timeout: float | None = None) -> bool:
            order.append("wait")
            return super().wait(timeout)

    def decline(longest: float) -> bool:
        order.append("decline")
        asked.append(longest)
        return len(asked) == 2

    def answer(batch: list[dict[str, object]]) -> Verdict:
        order.append("deliver")
        return Verdict.uniform(R, len(batch))

    sink = Scripted([answer], max_retries=5, seconds=0.02)
    delivery = _deliver(sink, events(2), decline=decline, stop=Ordered())
    assert order == ["deliver", "wait", "decline", "deliver", "wait", "decline"]
    assert sink.calls == 2
    assert len(asked) == 2 and min(asked) >= 0.02
    assert delivery.declined and delivery.unresolved == 2 and delivery.failed
    assert _counts(sink) == (0, 2)


# --- FR-003: counting ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("verdict", "dropped", "failed", "run_failed"),
    [
        ([A, J, D, U], 1, 2, False),
        ([D, D, D], 3, 0, False),
        ([J, J], 0, 2, True),
        ([J, D], 0, 1, True),
        ([J, U], 0, 2, False),
        ([S, J], 0, 1, True),
        ([S, S, S], 0, 0, False),
    ],
    ids=[
        "mixed",
        "all-dropped",
        "all-rejected",
        "rejected-dropped",
        "rejected-unknown",
        "skipped-rejected",
        "all-skipped",
    ],
)
def test_final_outcomes_land_in_the_engines_counters(
    verdict: list[Outcome],
    dropped: int,
    failed: int,
    run_failed: bool,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """FR-003 AC1: each final fate's counter, and the run-failed rule, per the table."""
    sink = Scripted([verdict], max_retries=3)
    delivery = _deliver(sink, events(len(verdict)), worker_retries=3)
    assert _counts(sink) == (dropped, failed)
    assert delivery.failed is run_failed
    assert sink.calls == 1, "no final outcome is offered again"
    if verdict == [S, S, S]:
        assert capsys.readouterr().err == ""


def test_an_event_counts_once_per_run_not_once_per_round() -> None:
    """FR-003 AC2: two events retryable through three rounds count two, as does a declined run."""
    sink = Scripted([R], max_retries=2)
    _deliver(sink, events(2))
    assert sink.calls == 3
    assert _counts(sink) == (0, 2)
    declined = Scripted([R], max_retries=5)
    _deliver(declined, events(2), decline=lambda longest: True)
    assert declined.calls == 1
    assert _counts(declined) == (0, 2)


@pytest.mark.parametrize(
    "error",
    [KeyboardInterrupt("k"), SystemExit(3), __import__("asyncio").CancelledError()],
    ids=["keyboard-interrupt", "system-exit", "cancelled"],
)
def test_a_baseexception_from_deliver_propagates_and_commits_nothing(
    error: BaseException,
) -> None:
    """FR-003 AC3: by identity, and with an earlier accepting round left uncommitted."""
    sink = Scripted([[A, R], error], max_retries=2)
    with pytest.raises(BaseException) as caught:
        _deliver(sink, events(2))
    assert caught.value is error
    assert _counts(sink) == (0, 0)


def test_losses_failed_and_read_losses_agree_through_a_wrapper() -> None:
    """FR-003 AC4: ``losses()``, ``.failed`` and a third-party wrapper's ``read_losses`` agree."""
    sink = Scripted([[A, J, D, U]])
    _deliver(sink, events(4))
    wrapper = ThirdPartyWrapper(sink)
    assert read_losses(sink) == SinkLosses(dropped=1, failed=2)
    assert sink.failed == 2
    assert read_losses(wrapper) == read_losses(sink)


def _class(path: pathlib.Path, name: str) -> ast.ClassDef:
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.ClassDef) and node.name == name:
            return node
    raise AssertionError(f"{name} is gone from {path.name}: the never-deliver check lost its class")


@pytest.mark.parametrize(
    ("module", "cls"),
    [("kafka", "KafkaSink"), ("pubsub", "GooglePubSubSink"), ("multi", "MultiSink")],
)
def test_kafka_pubsub_and_multi_never_offer_deliver(module: str, cls: str) -> None:
    """FR-003 AC7: neither member is defined or assigned, and the probe says ``None``."""
    import importlib

    node = _class(_SRC / "sinks" / f"{module}.py", cls)
    defined = {
        item.name for item in node.body if isinstance(item, ast.FunctionDef | ast.AsyncFunctionDef)
    }
    stored = {
        target.attr
        for item in ast.walk(node)
        if isinstance(item, ast.Assign | ast.AnnAssign)
        for target in (item.targets if isinstance(item, ast.Assign) else [item.target])
        if isinstance(target, ast.Attribute)
    }
    named = {
        target.id
        for item in node.body
        if isinstance(item, ast.Assign | ast.AnnAssign)
        for target in (item.targets if isinstance(item, ast.Assign) else [item.target])
        if isinstance(target, ast.Name)
    }
    for member in ("deliver", "log_foundry_counters"):
        assert member not in defined | stored | named, f"{cls} carries {member}"
    live = getattr(importlib.import_module(f"log_foundry.sinks.{module}"), cls)
    assert run_delivery(object.__new__(live), events(1), worker_retries=3, stop=None) is None


def test_a_settled_runs_line_names_counts_and_rounds_never_a_value(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """FR-003 AC9: one line, counts by term and rounds and the stored type name only."""
    canary = "CANARY-event-value"
    sink = Scripted([[A, R, J, D, U], [R], [J]], max_retries=2)
    batch = [{"message": canary, "i": i} for i in range(5)]
    _deliver(sink, batch)
    err = capsys.readouterr().err
    assert err == (
        "log-foundry: lost 4 event(s); rejected 2, unresolved 0, raised 0, unknown 1, "
        "dropped 1 after 3 delivery round(s) to Scripted\n"
    )
    assert canary not in err


def test_the_commit_absorbs_a_counter_the_sink_replaced(capsys: pytest.CaptureFixture[str]) -> None:
    """A counters field rebound to a non-number fails the commit, which is absorbed, not raised."""
    sink = Scripted([J])
    sink.log_foundry_counters.failed_events = "broken"  # type: ignore[assignment]
    delivery = _deliver(sink, events(2))
    assert delivery.failed
    err = capsys.readouterr().err
    assert err == (
        "log-foundry: absorbed a failure while counting a delivery (TypeError); "
        "2 event(s) not counted\n"
    )


def test_the_engine_module_routes_on_the_real_type_only() -> None:
    """SPEC-071 FR-005: no ``isinstance`` call anywhere in ``sinks/_engine.py``."""
    tree = ast.parse(_ENGINE.read_text(encoding="utf-8"))
    calls = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "isinstance"
    ]
    assert calls == [], f"sinks/_engine.py calls isinstance at lines {calls}"


# --- FR-004: the two unknown outcomes -------------------------------------------------------


def test_a_raising_deliver_fails_its_round_naming_only_the_type(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """FR-004 AC1: called once, every event failed, the run failed, the type and no text."""
    sink = Scripted([ConnectionRefusedError("secret-token")], max_retries=3)
    delivery = _deliver(sink, events(3), worker_retries=3)
    assert sink.calls == 1
    assert _counts(sink) == (0, 3)
    assert delivery.failed and delivery.raised == 3 and delivery.cycles == 1
    err = capsys.readouterr().err
    assert err == (
        "log-foundry: absorbed a failure while delivering a batch (ConnectionRefusedError); "
        "its round's events not confirmed by Scripted\n"
    )


class _Idempotent(Scripted):
    log_foundry_idempotent = True

    def __init__(self, *args: Any, answer: Any = True, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.answer = answer
        self.asked: list[Exception] = []

    def _retryable_error(self, err: Exception) -> bool:
        self.asked.append(err)
        if isinstance(self.answer, BaseException):
            raise self.answer
        return self.answer  # type: ignore[no-any-return]


def test_an_idempotent_sink_resends_what_its_predicate_accepts() -> None:
    """FR-004 AC2: all ``n`` events resent once, and a second-round accept counts nothing."""
    error = ConnectionRefusedError("x")
    sink = _Idempotent([error, A], max_retries=3)
    batch = events(3)
    delivery = _deliver(sink, batch)
    assert sink.calls == 2
    assert [id(e) for e in sink.rounds[1]] == [id(e) for e in batch]
    assert sink.asked == [error], "the predicate was bound and asked with the exception"
    assert _counts(sink) == (0, 0) and not delivery.failed


class _NoPredicate(Scripted):
    log_foundry_idempotent = True


@pytest.mark.parametrize("answer", [False, RuntimeError("predicate-secret"), "absent"])
def test_an_idempotent_sink_resends_nothing_its_predicate_refuses(answer: object) -> None:
    """FR-004 AC2: a predicate that refuses, raises or is absent resends nothing."""
    if answer == "absent":
        sink: Scripted = _NoPredicate([ConnectionRefusedError("x"), A], max_retries=3)
    else:
        sink = _Idempotent([ConnectionRefusedError("x"), A], max_retries=3, answer=answer)
    delivery = _deliver(sink, events(2))
    assert sink.calls == 1
    assert delivery.failed and _counts(sink) == (0, 2)


def test_a_non_idempotent_sink_never_asks_its_predicate() -> None:
    """The predicate is consulted only for a sink that declared itself idempotent."""
    sink = _Idempotent([ConnectionRefusedError("x"), A], max_retries=3)
    sink.log_foundry_idempotent = False  # type: ignore[misc]
    _deliver(sink)
    assert sink.calls == 1 and sink.asked == []


@pytest.mark.parametrize("shape", ["staticmethod", "instance-dict", "non-bool", "raising-bind"])
def test_every_predicate_shape_answers_through_is_retryable(shape: str) -> None:
    """A static predicate is unwrapped, an instance one used as found, a non-bool read by truth."""

    class Binder:
        def __get__(self, obj: object, cls: type) -> Any:
            raise RuntimeError("bind-secret")

    namespace: dict[str, Any] = {"log_foundry_idempotent": True}
    if shape == "staticmethod":
        namespace["_retryable_error"] = staticmethod(lambda err: True)
    elif shape == "non-bool":
        namespace["_retryable_error"] = lambda self, err: [1]
    elif shape == "raising-bind":
        namespace["_retryable_error"] = Binder()
    cls = type("Shaped", (Scripted,), namespace)
    sink = cls([ConnectionRefusedError("x"), A], max_retries=2)
    if shape == "instance-dict":
        sink._retryable_error = lambda err: True
    _deliver(sink)
    assert sink.calls == (1 if shape == "raising-bind" else 2)


class _HTTPEngine(HTTPSink):
    log_foundry_idempotent = True

    def __init__(self) -> None:
        super().__init__("http://127.0.0.1:9/")
        self.log_foundry_counters = SinkCounters()
        self.calls = 0

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        self.calls += 1
        if self.calls == 1:
            raise urllib.error.URLError("refused")
        return Verdict.uniform(Outcome.ACCEPTED, len(batch))


def test_httpsinks_inherited_predicate_answers_through_the_engine() -> None:
    """FR-004 AC3: ``HTTPSink._retryable_error``, inherited and bound, answers True."""
    sink = _HTTPEngine()
    delivery = _deliver(sink, events(2))
    assert sink.calls == 2
    assert sink.log_foundry_counters.losses().failed == 0 and not delivery.failed


def test_an_unbound_predicate_would_refuse_everything(monkeypatch: pytest.MonkeyPatch) -> None:
    """FR-004 AC3's mutant, pinned: the static lookup passed unbound answers False, one call."""
    import inspect

    monkeypatch.setattr(
        _engine, "_predicate_of", lambda sink: inspect.getattr_static(sink, "_retryable_error")
    )
    sink = _HTTPEngine()
    _deliver(sink, events(2))
    assert sink.calls == 1


@pytest.mark.parametrize("declared", [1, "yes", False, property(lambda self: True)])
def test_a_truthy_non_bool_is_not_an_idempotence_declaration(declared: object) -> None:
    """FR-004 AC4: only ``True`` declares; ``1``, ``"yes"`` and a property do not."""
    cls = type("Declares", (_Idempotent,), {"log_foundry_idempotent": declared})
    sink = cls([ConnectionRefusedError("x"), A], max_retries=3)
    _deliver(sink)
    assert sink.calls == 1


class _OutcomesRaise(Verdict):
    @property
    def outcomes(self) -> tuple[Outcome, ...]:  # type: ignore[override]
        raise RuntimeError("outcomes-secret")


class _RetryAfterRaises(Verdict):
    @property
    def retry_after(self) -> float | None:  # type: ignore[override]
        raise RuntimeError("retry-after-secret")


def _bare_string_outcome(n: int) -> Verdict:
    verdict = Verdict.uniform(A, n)
    object.__setattr__(verdict, "outcomes", ("accepted",) * n)
    return verdict


_MALFORMED = {
    "none": lambda n: None,
    "dict": lambda n: {"outcomes": (A,) * n},
    "one-short": lambda n: Verdict.uniform(A, n - 1),
    "one-long": lambda n: Verdict.uniform(A, n + 1),
    "bare-string": _bare_string_outcome,
    "outcomes-raise": lambda n: object.__new__(_OutcomesRaise),
    "retry-after-raises": lambda n: _with_outcomes(object.__new__(_RetryAfterRaises), n),
    "list-outcomes": lambda n: _with_list(n),
}


def _with_outcomes(verdict: Verdict, n: int) -> Verdict:
    object.__setattr__(verdict, "outcomes", (A,) * n)
    return verdict


def _with_list(n: int) -> Verdict:
    verdict = Verdict.uniform(A, n)
    object.__setattr__(verdict, "outcomes", [A] * n)
    return verdict


@pytest.mark.parametrize("shape", sorted(_MALFORMED))
@pytest.mark.parametrize("idempotent", [False, True], ids=["default", "idempotent"])
def test_a_malformed_verdict_settles_counted(shape: str, idempotent: bool) -> None:
    """FR-004 AC5: counted ``failed``, settled, resent once only by an idempotent sink."""
    sink = Scripted([lambda batch: _MALFORMED[shape](len(batch)), A], max_retries=2)
    if idempotent:
        sink.log_foundry_idempotent = True  # type: ignore[attr-defined]
    delivery = _deliver(sink, events(2))
    if idempotent:
        assert sink.calls == 2 and delivery.accepted == 2 and _counts(sink) == (0, 0)
    else:
        assert sink.calls == 1 and delivery.unknown == 2 and _counts(sink) == (0, 2)
    assert not delivery.failed


def test_an_exception_whose_str_raises_is_still_absorbed(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """FR-004 AC5: only the type is read, so a hostile ``__str__`` cannot stop the line."""

    class Hostile(Exception):
        def __str__(self) -> str:
            raise RuntimeError("str-secret")

        def __repr__(self) -> str:
            raise RuntimeError("repr-secret")

    sink = Scripted([Hostile()])
    delivery = _deliver(sink, events(1))
    assert delivery.failed
    assert "absorbed a failure while delivering a batch (Hostile)" in capsys.readouterr().err


@pytest.mark.parametrize("shape", sorted(_MALFORMED))
def test_outcomes_of_is_total(shape: str) -> None:
    """FR-004 AC6: ``None`` for every malformed shape, never an exception."""
    assert outcomes_of(_MALFORMED[shape](3), 3) is None


def test_outcomes_of_returns_the_exact_tuple() -> None:
    """FR-004 AC6: a well-formed verdict of length ``n`` gives its own tuple, ``n = 0`` included."""
    verdict = Verdict(outcomes=(A, J, S))
    assert outcomes_of(verdict, 3) is verdict.outcomes
    assert outcomes_of(Verdict(outcomes=()), 0) == ()
    assert outcomes_of(verdict, 4) is None


def test_outcomes_of_is_the_engines_one_verdict_test() -> None:
    """FR-004 AC6: outside ``outcomes_of`` no ``.outcomes`` read and no ``Verdict`` type test."""
    tree = ast.parse(_ENGINE.read_text(encoding="utf-8"))
    home = next(
        (n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "outcomes_of"), None
    )
    assert home is not None, "outcomes_of is gone from sinks/_engine.py"
    inside = {id(node) for node in ast.walk(home)}
    assert any(
        isinstance(node, ast.Attribute) and node.attr == "outcomes" for node in ast.walk(home)
    ), "outcomes_of no longer reads .outcomes: the locate check lost its anchor"
    found: list[str] = []
    for node in ast.walk(tree):
        if id(node) in inside:
            continue
        if isinstance(node, ast.Attribute) and node.attr == "outcomes":
            found.append(f"{node.lineno}: reads .outcomes")
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in {"isinstance", "issubclass"}
            and any(
                isinstance(n, ast.Name) and n.id in {"Verdict", "Outcome"}
                for arg in node.args[1:]
                for n in ast.walk(arg)
            )
        ):
            found.append(f"{node.lineno}: {ast.unparse(node)}")
        if isinstance(node, ast.Compare) and any(
            isinstance(op, ast.Is | ast.IsNot) for op in node.ops
        ):
            sides = [node.left, *node.comparators]
            if any(
                isinstance(s, ast.Call) and isinstance(s.func, ast.Name) and s.func.id == "type"
                for s in sides
            ) and any(isinstance(s, ast.Name) and s.id in {"Verdict", "Outcome"} for s in sides):
                found.append(f"{node.lineno}: {ast.unparse(node)}")
        if (
            isinstance(node, ast.Call)
            and ast.unparse(node.func) in {"len", "tuple.__len__"}
            and any(
                isinstance(n, ast.Name) and n.id in {"verdict", "result"} for n in ast.walk(node)
            )
        ):
            found.append(f"{node.lineno}: {ast.unparse(node)}")
    assert found == [], found


def test_an_unknown_outcome_settles_and_is_resent_only_by_an_idempotent_sink() -> None:
    """FR-004 AC7: counted and not resent by default; resent by an idempotent sink."""
    default = Scripted([U, A], max_retries=3)
    delivery = _deliver(default, events(2))
    assert default.calls == 1 and not delivery.failed and _counts(default) == (0, 2)
    idempotent = Scripted([U, A], max_retries=3)
    idempotent.log_foundry_idempotent = True  # type: ignore[attr-defined]
    delivery = _deliver(idempotent, events(2))
    assert idempotent.calls == 2 and delivery.accepted == 2 and _counts(idempotent) == (0, 0)


def test_a_raise_after_an_accepting_round_settles_and_counts_its_round_only() -> None:
    """FR-004 AC8: round one's accepted events stay uncounted; round two's raise counts its own."""
    sink = Scripted([[A, R, A, R], ConnectionError("x")], max_retries=3)
    delivery = _deliver(sink, events(4))
    assert sink.calls == 2
    assert delivery.accepted == 2 and delivery.raised == 2 and not delivery.failed
    assert _counts(sink) == (0, 2)


# --- ruling M1: the batch is snapshotted ---------------------------------------------------


@pytest.mark.parametrize("mutation", ["clear", "extend"])
def test_a_deliver_that_mutates_its_argument_changes_nothing(mutation: str) -> None:
    """The engine indexes its own copy, so every event keeps one counted fate."""
    batch = events(3)

    def mutate(handed: list[dict[str, object]]) -> Verdict:
        n = len(handed)
        if mutation == "clear":
            handed.clear()
        else:
            handed.extend(events(5, "x"))
        return Verdict(outcomes=(R, J, A)[:n] if n == 3 else (A,) * n)

    sink = Scripted([mutate, A], max_retries=1)
    delivery = _deliver(sink, batch)
    assert sink.calls == 2
    assert [e["i"] for e in sink.rounds[1]] == [0]
    assert (delivery.accepted, delivery.rejected) == (2, 1)
    assert _counts(sink) == (0, 1)


# --- ruling 11: one absorbed line per type, written after the commit ----------------------


def test_each_escaped_type_is_announced_once_after_the_commit() -> None:
    """Record first, announce second; one line per distinct type however many rounds raised."""
    sink = _Idempotent(
        [ConnectionError("a"), ConnectionError("b"), TimeoutError("c"), ConnectionError("d"), J],
        max_retries=6,
    )
    seen: list[tuple[str, int]] = []

    class Spy(io.StringIO):
        def write(self, text: str) -> int:
            seen.append((text, sink.log_foundry_counters.losses().failed))
            return len(text)

    stderr, sys.stderr = sys.stderr, Spy()
    try:
        delivery = _deliver(sink, events(1))
    finally:
        sys.stderr = stderr
    assert sink.calls == 5 and delivery.rejected == 1 and delivery.failed
    tail = "; its round's events not confirmed by _Idempotent\n"
    assert seen == [
        (f"log-foundry: absorbed a failure while delivering a batch (ConnectionError){tail}", 1),
        (f"log-foundry: absorbed a failure while delivering a batch (TimeoutError){tail}", 1),
    ], "each type once, and every line written after the commit moved failed_events"


def test_a_failed_commit_still_announces_the_escaped_type(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The type line does not ride on the commit: a broken counter costs the count only."""
    sink = Scripted([ValueError("v")])
    sink.log_foundry_counters.failed_events = None  # type: ignore[assignment]
    _deliver(sink)
    lines = capsys.readouterr().err.splitlines()
    assert [re.sub(r";.*", "", line) for line in lines] == [
        "log-foundry: absorbed a failure while counting a delivery (TypeError)",
        "log-foundry: absorbed a failure while delivering a batch (ValueError)",
    ]


def test_a_stop_event_whose_wait_raises_costs_only_its_waits(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A hostile stop event is announced once and the run goes on sleeping instead."""

    class Broken(threading.Event):
        def wait(self, timeout: float | None = None) -> bool:
            raise RuntimeError("wait-secret")

    sink = Scripted([R, R, A], max_retries=2)
    started = time.monotonic()
    delivery = _deliver(sink, stop=Broken())
    assert sink.calls == 3 and delivery.accepted == 1
    assert time.monotonic() - started >= 0.3 - 0.05, "the backoff was slept, not skipped"
    err = capsys.readouterr().err
    assert err.count("absorbed a failure while waiting between delivery rounds") == 1


# --- the static reads run none of a hostile class's code (first diff review, m-1) ----------


def _recording_metaclass(calls: list[str]) -> type:
    """A metaclass whose ``__eq__``, ``__hash__``, ``__mro__`` and ``__name__`` record each call."""

    def eq(cls: type, other: object) -> bool:
        calls.append("__eq__")
        return cls is other

    def hashed(cls: type) -> int:
        calls.append("__hash__")
        return id(cls)

    def mro(cls: type) -> tuple[type, ...]:
        calls.append("__mro__")
        return type.__dict__["__mro__"].__get__(cls)  # type: ignore[no-any-return]

    def name(cls: type) -> str:
        calls.append("__name__")
        return "Claimed"

    return type(
        "_RecordingMeta",
        (type,),
        {"__eq__": eq, "__hash__": hashed, "__mro__": property(mro), "__name__": property(name)},
    )


def _hostile_engine_sink(calls: list[str], script: list[Any]) -> Any:
    """An idempotent engine sink built by :func:`_recording_metaclass`, with a predicate."""
    namespace = {
        "log_foundry_idempotent": True,
        "_retryable_error": lambda self, err: True,
        "max_retry_after": 0.5,
    }
    cls = _recording_metaclass(calls)("HostileSink", (Scripted,), namespace)
    sink = cls(script, max_retries=3)
    calls.clear()
    return sink


def test_no_static_read_runs_a_hostile_metaclass_hook(capsys: pytest.CaptureFixture[str]) -> None:
    """``read_deliver``, the fork reach, ``run_delivery`` and the adapter read only builtins.

    ``inspect.getattr_static`` hashed and compared the class through its ``lru_cache``d
    shadowed-``__dict__`` check: measured ``['eq', 'hash']`` for ``read_deliver`` and four ``eq``
    for ``run_delivery``. The script below takes every static read the engine makes — the probe,
    the counters, ``max_retries``, ``max_retry_after``, the idempotence flag, the predicate and
    its bind, the stop signal — and the commit's stored-name line.
    """
    from log_foundry import _fork
    from log_foundry.sinks._engine import emit_via_deliver
    from log_foundry.sinks.verdict import read_deliver

    calls: list[str] = []
    sink = _hostile_engine_sink(
        calls, [ConnectionError("x"), Verdict(outcomes=(R, J), retry_after=0.1), [D], A]
    )
    sink.log_foundry_stop_signal = WaitRecorder()
    assert read_deliver(sink) is not None
    assert _fork._counters_reach(sink, SinkCounters) is sink.log_foundry_counters
    delivery = _deliver(sink, events(2), worker_retries=1)
    assert sink.calls == 3 and delivery.dropped == 1 and delivery.rejected == 1
    sink.script = [J]
    with pytest.raises(Exception, match=r"to HostileSink$"):
        emit_via_deliver(sink, events(1))
    assert "HostileSink" in capsys.readouterr().err
    assert calls == []


@pytest.mark.parametrize(
    "shape",
    ["data-descriptor-first", "instance-dict-second", "class-attribute-last", "slot", "unset-slot"],
)
def test_the_static_read_keeps_cpythons_lookup_order(shape: str) -> None:
    """A class data descriptor, then the instance dict, then a class attribute; slots resolved."""
    from log_foundry.sinks.base import _ABSENT, _static_value

    marker = SinkCounters()
    if shape == "data-descriptor-first":
        prop = property(lambda self: marker)
        obj = type("D", (), {"x": prop})()
        obj.__dict__["x"] = "shadowed"
        assert _static_value(obj, "x") is prop
    elif shape == "instance-dict-second":
        obj = type("I", (), {"x": "class"})()
        obj.x = marker  # type: ignore[attr-defined]
        assert _static_value(obj, "x") is marker
    elif shape == "class-attribute-last":
        obj = type("C", (), {"x": marker})()
        assert _static_value(obj, "x") is marker
    else:
        cls = type("S", (), {"__slots__": ("x",)})
        obj = cls()
        if shape == "slot":
            obj.x = marker  # type: ignore[attr-defined]
            assert _static_value(obj, "x") is marker
        else:
            assert _static_value(obj, "x") is _ABSENT


def test_a_shadowed_dict_is_read_through_the_builtin_descriptor_further_along() -> None:
    """A subclass's ``__dict__`` property never runs; the base's builtin getset reads the dict."""
    ran: list[str] = []
    base = type("Base", (), {})
    shadow = type("Shadow", (base,), {"__dict__": property(lambda self: ran.append("p") or {})})
    obj = shadow()
    base.__dict__["__dict__"].__set__(obj, {"x": 7})
    from log_foundry.sinks.base import _static_value

    assert _static_value(obj, "x") == 7
    assert ran == []
