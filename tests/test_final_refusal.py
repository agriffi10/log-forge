"""A refusal no retry can cure costs one ``emit`` call, not ``max_retries + 1``.

A closed or closing shipped sink, and a forked child's ``SQLiteSink``, raise the error
``sinks/_refusal.final_refusal`` builds. The worker abandons such a batch at the attempt that
raised it, moving ``failed_batches`` once and writing the abandoned line with the attempts it
made, where before it resent the batch ``max_retries`` times with a backoff wait between, about
85 ms a batch. Any other raise keeps the resend, a third-party sink's ``SinkDeliveryError`` and a
dual-typed driver error included, and ``MultiSink`` re-raises a final refusal only when every
child's raise was one. Serves invariants 2, 3, 6 and 7.
"""

from __future__ import annotations

import ast
import sqlite3
from pathlib import Path
from typing import Any

import pytest

import log_foundry
import sink_rigs
from conftest import install_worker
from log_foundry.sinks import _engine
from log_foundry.sinks._driver_error import delivery_error
from log_foundry.sinks._engine import emit_via_deliver
from log_foundry.sinks._refusal import final_refusal, is_final_refusal
from log_foundry.sinks.base import SinkDeliveryError
from log_foundry.sinks.multi import MultiSink
from log_foundry.worker import Worker
from test_sink_post_close import EMIT_ONLY_CLOSED, ENGINE_CLOSED, Built

LIMIT = 5.0
RETRIES = 3

SINKS = Path(__file__).resolve().parent.parent / "src" / "log_foundry" / "sinks"

EMIT_ONLY = (*sorted(EMIT_ONLY_CLOSED), "GooglePubSubSink")
"""Every shipped sink with no ``deliver`` that refuses after ``close()``."""


def _batch(n: int = 2) -> list[dict[str, object]]:
    return [sink_rigs.ok(f"e{i}") for i in range(n)]


class EmitCount:
    """Counts the calls a class's ``emit`` receives, delegating each."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, cls: type) -> None:
        self.calls = 0
        original = cls.emit  # type: ignore[attr-defined]
        counter = self

        def emit(sink: Any, batch: list[dict[str, object]]) -> Any:
            counter.calls += 1
            return original(sink, batch)

        monkeypatch.setattr(cls, "emit", emit)


def _drain(sink: Any) -> tuple[bool, int]:
    """Runs one batch through a real ``Worker`` with ``max_retries=3``; flush verdict, failures."""
    worker = Worker(sink, batch_size=1000, flush_interval=60.0, max_retries=RETRIES)
    install_worker(worker)
    worker.submit(_batch(2))
    ok = bool(worker.flush(timeout=LIMIT))
    return ok, log_foundry.health().failed_batches


def _abandoned(err: str) -> list[str]:
    return [line for line in err.splitlines() if "batch abandoned after" in line]


@pytest.mark.parametrize(
    ("name", "state"), [*((name, "closed") for name in EMIT_ONLY), ("SQLiteSink", "forked")]
)
def test_one_refused_batch_costs_one_emit_call_not_four(
    name: str,
    state: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The regression: ``max_retries=3`` once cost 4 calls and 3 backoffs; now one call.

    ``forked`` applies to ``SQLiteSink`` alone, whose pid check runs before anything else in
    ``emit``; the pid is put back before teardown so the borrowed connection is closed as usual.
    """
    rig = sink_rigs.RIGS[name](monkeypatch, tmp_path)
    sink = rig.sink
    if state == "closed":
        sink.close()
    else:
        own_pid = sink._pid
        sink._pid = -1
    count = EmitCount(monkeypatch, type(sink))
    before = rig.calls()
    capsys.readouterr()
    try:
        ok, failed = _drain(sink)
    finally:
        if state == "forked":
            sink._pid = own_pid
    assert count.calls == 1, f"{name} {state}: {count.calls} emit call(s) for one refused batch"
    assert not ok, "a refused batch reported a flush success"
    assert failed == 1
    assert rig.calls() == before, f"{name} reached its driver while {state}"
    assert _abandoned(capsys.readouterr().err) == [
        "log-foundry: lost 2 event(s); batch abandoned after 1 emit attempts"
    ]


class _Raises:
    """A third-party sink whose ``emit`` raises a fixed exception every time."""

    def __init__(self, error: Exception) -> None:
        self.error = error

    def emit(self, batch: list[dict[str, object]]) -> None:
        raise self.error


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(SinkDeliveryError("ThirdPartySink delivered none: the sink is closed"), id="unmarked"),
        pytest.param(delivery_error(sqlite3.OperationalError("database is locked")), id="dual-typed"),
        pytest.param(ConnectionError("refused"), id="transient"),
    ],
)
def test_any_other_raise_keeps_the_resend(
    error: Exception, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The silent twin: what ``final_refusal`` did not build is resent, as in 1.x."""
    monkeypatch.setattr(_engine, "cycle_backoff", lambda attempt: 0.0)
    count = EmitCount(monkeypatch, _Raises)
    capsys.readouterr()
    ok, failed = _drain(_Raises(error))
    assert count.calls == RETRIES + 1
    assert not ok and failed == 1
    assert _abandoned(capsys.readouterr().err) == [
        f"log-foundry: lost 2 event(s); batch abandoned after {RETRIES + 1} emit attempts"
    ]


def test_a_refusal_after_a_transient_failure_stops_the_resend_where_it_lands(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A sink that fails transiently, then closes: the line counts the attempts actually made."""
    monkeypatch.setattr(_engine, "cycle_backoff", lambda attempt: 0.0)
    errors = [ConnectionError("refused"), final_refusal("ThenClosed: the sink is closed")]

    class Closes:
        def emit(self, batch: list[dict[str, object]]) -> None:
            raise errors[min(count.calls - 1, 1)]

    count = EmitCount(monkeypatch, Closes)
    capsys.readouterr()
    ok, failed = _drain(Closes())
    assert count.calls == 2
    assert not ok and failed == 1
    assert _abandoned(capsys.readouterr().err) == [
        "log-foundry: lost 2 event(s); batch abandoned after 2 emit attempts"
    ]


@pytest.mark.parametrize("name", EMIT_ONLY)
def test_every_emit_only_closed_refusal_is_final_and_still_exactly_sink_delivery_error(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The mark changes nothing a caller sees: the type is exact and the text is today's."""
    rig = sink_rigs.RIGS[name](monkeypatch, tmp_path)
    rig.sink.close()
    with pytest.raises(SinkDeliveryError) as raised:
        rig.sink.emit(_batch(2))
    assert type(raised.value) is SinkDeliveryError
    assert is_final_refusal(raised.value)
    if name in EMIT_ONLY_CLOSED:
        assert str(raised.value) == EMIT_ONLY_CLOSED[name](2)


def test_a_forked_sqlite_sinks_refusal_is_final(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The forked-child refusal, raised before the guard, carries the mark too."""
    rig = sink_rigs.RIGS["SQLiteSink"](monkeypatch, tmp_path)
    own_pid = rig.sink._pid
    rig.sink._pid = -1
    try:
        with pytest.raises(SinkDeliveryError) as raised:
            rig.sink.emit(_batch(1))
    finally:
        rig.sink._pid = own_pid
    assert is_final_refusal(raised.value)
    assert "a forked child acquired no connection" in str(raised.value)


@pytest.mark.parametrize("name", sorted(ENGINE_CLOSED))
def test_a_closed_engine_sinks_emit_raises_a_final_refusal(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Through the inline adapter, as a ``MultiSink`` or third-party wrapper reaches it."""
    built = Built(name, monkeypatch, tmp_path)
    built.target.close()
    with pytest.raises(SinkDeliveryError) as raised:
        if name == "SocketTransport":
            built.outer.emit(_batch(1))
        else:
            emit_via_deliver(built.outer, _batch(1))
    assert type(raised.value) is SinkDeliveryError
    assert is_final_refusal(raised.value)


def test_a_forked_nats_sinks_emit_raises_a_final_refusal(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """``NATSSink``'s forked-child refusal, the precedent for ``SQLiteSink``'s, is final too."""
    built = Built("NATSSink", monkeypatch, tmp_path)
    own_pid = built.target._pid
    built.target._pid = -1
    try:
        with pytest.raises(SinkDeliveryError) as raised:
            emit_via_deliver(built.target, _batch(1))
    finally:
        built.target._pid = own_pid
    assert is_final_refusal(raised.value)
    assert "a forked child acquired no connection" in str(raised.value)


def test_a_fan_out_of_closed_children_costs_one_attempt(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Every child refused for good, an engine child among them: one fan-out call."""
    file_rig = sink_rigs.RIGS["FileSink"](monkeypatch, tmp_path)
    engine = Built("ClickHouseSink", monkeypatch, tmp_path)
    file_rig.sink.close()
    engine.target.close()
    fan_out = MultiSink(file_rig.sink, engine.outer)
    count = EmitCount(monkeypatch, MultiSink)
    ok, failed = _drain(fan_out)
    assert count.calls == 1
    assert not ok and failed == 1


@pytest.mark.parametrize("closed_first", [True, False], ids=["closed-first", "closed-last"])
def test_a_fan_out_with_one_curable_child_keeps_the_resend(
    closed_first: bool, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A closed child must not stop the resend a sibling's transient failure is owed."""
    monkeypatch.setattr(_engine, "cycle_backoff", lambda attempt: 0.0)
    file_rig = sink_rigs.RIGS["FileSink"](monkeypatch, tmp_path)
    file_rig.sink.close()
    transient = ConnectionError("refused")
    children = [file_rig.sink, _Raises(transient)]
    fan_out = MultiSink(*(children if closed_first else children[::-1]))
    with pytest.raises(ConnectionError) as raised:
        fan_out.emit(_batch(1))
    assert raised.value is transient
    count = EmitCount(monkeypatch, MultiSink)
    ok, failed = _drain(fan_out)
    assert count.calls == RETRIES + 1
    assert not ok and failed == 1


def test_a_fan_out_of_final_refusals_raises_the_first_childs() -> None:
    """When every raise is final the first child's is re-raised, by identity, as before."""
    first, second = final_refusal("A: the sink is closed"), final_refusal("B: the sink is closed")
    with pytest.raises(SinkDeliveryError) as raised:
        MultiSink(_Raises(first), _Raises(second)).emit(_batch(1))
    assert raised.value is first


class _Hostile(Exception):
    """An exception whose every attribute read raises."""

    def __getattribute__(self, name: str) -> Any:
        raise RuntimeError(name)


def test_the_mark_is_read_only_from_an_exact_marked_sink_delivery_error() -> None:
    """Total, and narrow: a subclass, an unmarked instance and a hostile class are not final."""
    marked = final_refusal("X: the sink is closed")
    assert type(marked) is SinkDeliveryError and str(marked) == "X: the sink is closed"
    assert is_final_refusal(marked)

    class Sub(SinkDeliveryError):
        pass

    sub = Sub("x")
    vars(sub).update(vars(marked))
    assert not is_final_refusal(sub)
    assert not is_final_refusal(SinkDeliveryError("X: the sink is closed"))
    assert not is_final_refusal(_Hostile())
    assert not is_final_refusal(delivery_error(sqlite3.OperationalError("x")) or ValueError())


def section_problems(source: str) -> list[str]:
    """Every closed-guard section in ``source`` whose refusal is not a ``final_refusal``.

    A section is ``with <...>.enter() as <name>:``; its refusal is the body of the
    ``if not <name>:`` inside it, which must call ``final_refusal`` and must not build a
    ``SinkDeliveryError`` itself. A section with no such ``if`` is reported too, since it would
    refuse nothing at all.
    """
    problems: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.With):
            continue
        for item in node.items:
            call = item.context_expr
            if not (
                isinstance(call, ast.Call)
                and isinstance(call.func, ast.Attribute)
                and call.func.attr == "enter"
                and isinstance(item.optional_vars, ast.Name)
            ):
                continue
            name = item.optional_vars.id
            refusals = [
                inner
                for inner in ast.walk(node)
                if isinstance(inner, ast.If)
                and isinstance(inner.test, ast.UnaryOp)
                and isinstance(inner.test.op, ast.Not)
                and isinstance(inner.test.operand, ast.Name)
                and inner.test.operand.id == name
            ]
            if not refusals:
                problems.append(f"line {node.lineno}: section {name!r} has no `if not {name}:`")
            for refusal in refusals:
                called = {
                    c.func.id
                    for stmt in refusal.body
                    for c in ast.walk(stmt)
                    if isinstance(c, ast.Call) and isinstance(c.func, ast.Name)
                }
                if "SinkDeliveryError" in called:
                    problems.append(
                        f"line {refusal.lineno}: builds SinkDeliveryError, not final_refusal"
                    )
                elif "final_refusal" not in called:
                    problems.append(f"line {refusal.lineno}: refuses without final_refusal")
    return problems


SECTION_CORPUS = [
    (
        (
            "with self._guard.enter() as section:\n"
            "    if not section:\n"
            "        raise final_refusal('X: the sink is closed')\n"
        ),
        [],
    ),
    (
        (
            "with self._guard.enter() as section:\n"
            "    if not section:\n"
            "        run_context().refuse(final_refusal('X: the sink is closed'))\n"
            "        return None\n"
        ),
        [],
    ),
    (
        (
            "with self._guard.enter() as section:\n"
            "    if not section:\n"
            "        raise SinkDeliveryError('X: the sink is closed')\n"
        ),
        ["line 2: builds SinkDeliveryError, not final_refusal"],
    ),
    (
        (
            "with self._guard.enter() as section:\n"
            "    if not section:\n"
            "        return None\n"
        ),
        ["line 2: refuses without final_refusal"],
    ),
    (
        (
            "with self._guard.enter() as admitted:\n"
            "    send()\n"
        ),
        ["line 1: section 'admitted' has no `if not admitted:`"],
    ),
    (
        (
            "with open(path) as handle:\n"
            "    if not handle:\n"
            "        raise SinkDeliveryError('x')\n"
        ),
        [],
    ),
]
"""The checker's own corpus: two compliant shapes, each failure reason, and a silent case."""


@pytest.mark.parametrize(("source", "expected"), SECTION_CORPUS)
def test_the_section_checker_reports_each_reason(source: str, expected: list[str]) -> None:
    assert section_problems(source) == expected


def test_every_closed_guard_section_refuses_with_a_final_refusal() -> None:
    """Derived from the tree, with a floor: the twelve sections at the time of writing."""
    sections = 0
    problems: list[str] = []
    for path in sorted(SINKS.glob("*.py")):
        source = path.read_text()
        sections += source.count(".enter() as ")
        problems += [f"{path.name}: {p}" for p in section_problems(source)]
    assert sections >= 12, f"only {sections} closed-guard section(s) found; the roster went quiet"
    assert problems == []
