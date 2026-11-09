"""SPEC-075 FR-004 — ``MultiSink`` offers no ``deliver`` and keeps calling each child's ``emit``.

``multi.py``'s ``emit`` is unchanged; what changed is what an engine child's ``emit`` does: it is
SPEC-072's adapter, which runs the child's engine inline. These tests characterise the fan-out
over such children: the rounds stay inside one call, the absorbed count is unchanged, the extra
count in ``failed`` is invariant 2's recorded upper bound, and the forwarded stop signal still
cuts a child's wait. Serves invariants 2, 3 and 7.
"""

from __future__ import annotations

import ast
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from conftest import install_worker
from log_foundry.sinks import _engine, _retry
from log_foundry.sinks.base import SinkLosses
from log_foundry.sinks.filtering import FilteringSink
from log_foundry.sinks.multi import MultiSink
from log_foundry.sinks.verdict import Outcome, Verdict, read_deliver
from log_foundry.worker import Worker
from verdict_sinks import Raw, Scripted, events

A, R, J, D, U = (
    Outcome.ACCEPTED,
    Outcome.RETRYABLE,
    Outcome.REJECTED,
    Outcome.DROPPED,
    Outcome.UNKNOWN,
)
_REAL_WAIT = _retry.wait
_MULTI = Path(__file__).resolve().parents[1] / "src" / "log_foundry" / "sinks" / "multi.py"


@pytest.fixture(autouse=True)
def _no_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("log_foundry.sinks._retry.wait", lambda _delay, _stop=None: None)


def worked(sink: Any, batch: list[dict[str, object]], retries: int) -> int:
    """Delivers ``batch`` through a real ``Worker``: its ``failed_batches``."""
    worker = Worker(sink, batch_size=1000, flush_interval=60.0, max_retries=retries)
    install_worker(worker)
    worker.submit(batch)
    worker.flush(timeout=30.0)
    return worker.health().failed_batches


def test_multisink_offers_nothing_and_reaches_a_wrapper_child_through_its_emit() -> None:
    """AC1: no ``deliver`` and no counters in ``multi.py``; a wrapper child's ``deliver`` unused."""
    tree = ast.parse(_MULTI.read_text(encoding="utf-8"))
    multi = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "MultiSink")
    names = {m.name for m in multi.body if isinstance(m, ast.FunctionDef)}
    stores = {
        t.attr
        for n in ast.walk(multi)
        if isinstance(n, ast.Assign)
        for t in n.targets
        if isinstance(t, ast.Attribute)
    }
    assert "deliver" not in names and "log_foundry_counters" not in stores | names
    assert read_deliver(MultiSink(Scripted())) is None

    frames: list[list[tuple[str, str]]] = []

    def record(batch: list[dict[str, object]]) -> Verdict:
        frame = sys._getframe(1)
        seen = []
        while frame is not None:
            seen.append((Path(frame.f_code.co_filename).name, frame.f_code.co_name))
            frame = frame.f_back
        frames.append(seen)
        return Verdict.uniform(A, len(batch))

    inner = Scripted([record])
    child = FilteringSink(inner)
    assert read_deliver(child) is not None, "the premise: the wrapper child offers deliver"
    MultiSink(child).emit(events(2))
    assert len(frames) == 1
    assert ("filtering.py", "emit") in frames[0]
    assert ("filtering.py", "deliver") not in frames[0]


def test_an_engine_childs_rounds_stay_inside_one_fan_out() -> None:
    """AC2: ``A`` is called as one ``A.emit`` calls it, never times the worker's budget."""
    n = 2
    a = Scripted([R], max_retries=2)
    b = Scripted()
    multi = MultiSink(a, b)
    assert worked(multi, events(n), retries=3) == 0
    assert a.calls == 3
    assert a.log_foundry_counters.failed_events == n
    assert multi._absorbed == n
    assert multi.losses() == SinkLosses(dropped=0, failed=2 * n)


def test_a_drop_is_counted_once_and_only_by_a_settled_child() -> None:
    """AC3: a failed child run commits no drop; a settled one commits its own, and nothing here."""
    a = Scripted([[J, D]])
    multi = MultiSink(a, Scripted())
    multi.emit(events(2))
    assert a.log_foundry_counters.dropped_events == 0
    assert multi.losses().dropped == 0
    a = Scripted([[A, D]])
    multi = MultiSink(a, Scripted())
    multi.emit(events(2))
    assert a.log_foundry_counters.dropped_events == 1
    assert multi._absorbed == 0
    assert multi.losses().dropped == 1


@pytest.mark.parametrize("step", [U, Raw(None)], ids=["unknown", "malformed"])
def test_a_settled_child_never_raises_into_the_fan_out(step: Any) -> None:
    """AC4: ``UNKNOWN`` or a malformed verdict settles, so the fan-out returns."""
    multi = MultiSink(Scripted([step]), Scripted([step]))
    assert worked(multi, events(2), retries=1) == 0
    assert multi._absorbed == 0


def test_total_failure_still_reraises_and_each_attempt_runs_every_childs_engine() -> None:
    """AC5: 4 calls each (2 per worker attempt), one failed batch, nothing absorbed."""
    n = 2
    a, b = Scripted([R], max_retries=1), Scripted([R], max_retries=1)
    multi = MultiSink(a, b)
    assert worked(multi, events(n), retries=1) == 1
    assert (a.calls, b.calls) == (4, 4)
    assert multi._absorbed == 0
    assert a.log_foundry_counters.failed_events == 2 * n
    assert b.log_foundry_counters.failed_events == 2 * n


def test_the_forwarded_stop_signal_cuts_an_engine_childs_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AC6: the child's inline engine waits on the signal ``MultiSink`` forwarded to it."""
    monkeypatch.setattr(_retry, "wait", _REAL_WAIT)
    monkeypatch.setattr(_engine, "BACKOFF_BASE", 30.0)
    child = Scripted([R], max_retries=1)
    multi = MultiSink(child)
    stop = threading.Event()
    multi.log_foundry_stop_signal = stop
    assert child.log_foundry_stop_signal is stop
    started = time.monotonic()
    timer = threading.Timer(0.2, stop.set)
    timer.start()
    try:
        with pytest.raises(Exception, match="delivered none"):
            multi.emit(events(1))
    finally:
        timer.cancel()
    assert time.monotonic() - started < 10.0
    assert child.calls == 2
