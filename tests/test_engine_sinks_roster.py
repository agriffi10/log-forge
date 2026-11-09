"""SPEC-074 FR-006 — the engine sinks are derived from the tree and held to one rule.

The population is every class under ``src/log_foundry/sinks/`` that defines ``deliver`` in its
own body, less the named exclusions, each with its reason. Each must assign
``self.log_foundry_counters`` in ``__init__``; each but ``SocketTransport`` must define ``def
emit(self, batch)`` whose body is the one call ``emit_via_deliver(self, batch)``, on the class
that defines ``deliver``, with no class below it defining ``emit``; its module must neither call
``wait`` nor assign a backoff constant; and it may write a counter only in ``__init__``, the
``failed`` setter or the ``_counter_lock`` setter, never registering a run-context count against
``failed``, whose setter takes the lock a commit holds. The floor is 13, exactly the thirteen.
A runtime half builds each sink over its double and reads what the AST cannot. The gate carries
a fixture corpus asserting each check's reason, a floor case, and a silent case. Serves
invariants 2, 6, 7, 10 and 12.
"""

from __future__ import annotations

import ast
import textwrap
from pathlib import Path
from typing import Any

import pytest

import log_foundry
from conftest import install_worker
from log_foundry.sinks import _engine
from log_foundry.sinks.base import _static_value
from log_foundry.sinks.nats import NATSSink
from log_foundry.sinks.verdict import SinkCounters, read_deliver
from log_foundry.worker import Worker
from sink_rigs import RIGS, ok
from test_fork_lifecycle import _held_by_a_parent_thread, run_in_child

SINKS = Path(__file__).resolve().parents[1] / "src" / "log_foundry" / "sinks"

ENGINE_SINKS = frozenset(
    {
        "SQSSink",
        "SNSSink",
        "KinesisSink",
        "FirehoseSink",
        "ClickHouseSink",
        "MongoDBSink",
        "PostgresSink",
        "_RedisSink",
        "RabbitMQSink",
        "AzureEventHubsSink",
        "NATSSink",
        "SocketTransport",
        "SyslogSink",
    }
)
FLOOR = 13

_WRAPPER_REASON = (
    "SPEC-075's wrapper: its emit is a 1.x body, its losses() is a 1.x formula, and its deliver "
    "catches nothing"
)

EXCLUSIONS: dict[str, str] = {
    "HTTPSink": "the HTTP family is SPEC-073's, its emit runs the engine over _round",
    "FilteringSink": _WRAPPER_REASON,
    "TransformSink": _WRAPPER_REASON,
    "LogstashSink": _WRAPPER_REASON,
}
"""Classes defining ``deliver`` the roster passes over, each with its reason."""

NO_EMIT = {"SocketTransport": "a transport, not a structural Sink: deliver and no emit (FR-005)"}

COUNTERS = frozenset(
    {"failed", "failed_events", "dropped_events", "dropped_oversized", "dropped_unadjudicated"}
)
WRITERS = frozenset({"__init__", "failed", "_counter_lock"})
"""Where a counter store is allowed: construction and the two setters."""


def _defines(cls: ast.ClassDef, name: str) -> ast.FunctionDef | None:
    for node in cls.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    return None


def _bases(cls: ast.ClassDef) -> set[str]:
    return {b.id if isinstance(b, ast.Name) else getattr(b, "attr", "") for b in cls.bases}


def _is_adapter_body(fn: ast.FunctionDef) -> bool:
    body = list(fn.body)
    if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
        body = body[1:]
    if len(body) != 1 or not isinstance(body[0], ast.Expr):
        return False
    call = body[0].value
    named = (isinstance(call, ast.Call) and isinstance(call.func, ast.Name)) and call.func.id
    return (
        isinstance(call, ast.Call)
        and named == "emit_via_deliver"
        and [ast.unparse(a) for a in call.args] == ["self", "batch"]
        and not call.keywords
        and [a.arg for a in fn.args.args] == ["self", "batch"]
    )


def _waits(tree: ast.Module) -> bool:
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and (
            (isinstance(node.func, ast.Name) and node.func.id == "wait")
            or (
                isinstance(node.func, ast.Attribute)
                and node.func.attr == "wait"
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "_retry"
            )
        ):
            return True
        targets = node.targets if isinstance(node, ast.Assign) else []
        if any(isinstance(t, ast.Name) and t.id.endswith("BACKOFF_BASE") for t in targets):
            return True
    return False


def _stores(cls: ast.ClassDef) -> list[str]:
    found: list[str] = []
    for fn in cls.body:
        if not isinstance(fn, ast.FunctionDef) or fn.name in WRITERS:
            continue
        for node in ast.walk(fn):
            targets: list[ast.expr] = []
            if isinstance(node, ast.Assign):
                targets = list(node.targets)
            elif isinstance(node, ast.AugAssign):
                targets = [node.target]
            found.extend(
                f"{fn.name} stores {ast.unparse(target)}"
                for target in targets
                if isinstance(target, ast.Attribute) and target.attr in COUNTERS
            )
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "count"
                and len(node.args) >= 2
                and isinstance(node.args[1], ast.Constant)
                and node.args[1].value == "failed"
            ):
                found.append(f"{fn.name} registers a count against failed")
    return found


def engine_sink_findings(root: Path, floor: int = FLOOR) -> tuple[set[str], list[str]]:
    """Every engine sink under ``root`` and every way one breaks FR-006's rule."""
    classes: dict[str, tuple[ast.ClassDef, ast.Module, str]] = {}
    for path in sorted(root.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in tree.body:
            if isinstance(node, ast.ClassDef):
                classes[node.name] = (node, tree, path.name)
    found: set[str] = set()
    findings: list[str] = []
    for name, (cls, tree, module) in classes.items():
        if _defines(cls, "deliver") is None or name in EXCLUSIONS:
            continue
        found.add(name)
        where = f"{module} {name}"
        init = _defines(cls, "__init__")
        if init is None or not any(
            isinstance(t, ast.Attribute) and t.attr == "log_foundry_counters"
            for n in ast.walk(init)
            if isinstance(n, ast.Assign | ast.AnnAssign)
            for t in (n.targets if isinstance(n, ast.Assign) else [n.target])
        ):
            findings.append(f"{where}: __init__ assigns no self.log_foundry_counters")
        emit = _defines(cls, "emit")
        if name not in NO_EMIT:
            if emit is None:
                findings.append(f"{where}: defines deliver but no emit beside it")
            elif not _is_adapter_body(emit):
                findings.append(
                    f"{where}: emit's body is not the one call emit_via_deliver(self, batch)"
                )
        for other, (sub, _, _) in classes.items():
            if name in _bases(sub) and _defines(sub, "emit") is not None:
                findings.append(f"{where}: {other} defines emit below the class defining deliver")
        if _waits(tree):
            findings.append(f"{where}: its module calls wait or assigns a backoff constant")
        findings.extend(f"{where}: {store}" for store in _stores(cls))
    if len(found) < floor:
        findings.append(f"the scan found {len(found)} engine sinks, under its floor of {floor}")
    return found, findings


def test_the_engine_sinks_are_the_thirteen() -> None:
    found, findings = engine_sink_findings(SINKS)
    assert findings == [], "\n".join(findings)
    assert found == ENGINE_SINKS


@pytest.mark.parametrize("wrapper", ["FilteringSink", "TransformSink", "LogstashSink"])
def test_removing_a_wrapper_exclusion_reddens_naming_it(
    wrapper: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SPEC-075 FR-001: each wrapper now defines ``deliver``, so its exclusion is load-bearing.

    Before SPEC-075 no wrapper defined ``deliver`` and removing the entry changed nothing; now the
    roster finds the class and names its 1.x ``emit``, and with every exclusion in place the
    population is still exactly the thirteen.
    """
    monkeypatch.delitem(EXCLUSIONS, wrapper)
    found, findings = engine_sink_findings(SINKS)
    assert wrapper in found
    assert any(
        wrapper in f and "emit's body is not the one call emit_via_deliver(self, batch)" in f
        for f in findings
    ), findings


_GOOD = '''
class Good:
    def __init__(self):
        self.log_foundry_counters = SinkCounters()

    def deliver(self, batch):
        return one_round(self._round, batch)

    def emit(self, batch):
        """The adapter."""
        emit_via_deliver(self, batch)
'''

_PLANTS = {
    "no counters": (
        _GOOD.replace("        self.log_foundry_counters = SinkCounters()\n", "        pass\n"),
        "__init__ assigns no self.log_foundry_counters",
    ),
    "emit not the adapter": (
        _GOOD.replace("        emit_via_deliver(self, batch)", "        self.deliver(batch)"),
        "emit's body is not the one call emit_via_deliver(self, batch)",
    ),
    "emit below deliver": (
        _GOOD + "\n\nclass Sub(Good):\n    def emit(self, batch):\n        pass\n",
        "Sub defines emit below the class defining deliver",
    ),
    "a dropped_oversized store": (
        _GOOD + "\n    def _round(self, batch):\n        self.dropped_oversized += 1\n",
        "_round stores self.dropped_oversized",
    ),
    "a count against failed": (
        _GOOD
        + "\n    def _round(self, batch):\n        run_context().count(self, 'failed', 1, when=W)\n",
        "_round registers a count against failed",
    ),
    "a wait": (_GOOD + "\n\ndef helper():\n    wait(1.0)\n", "calls wait or assigns a backoff"),
}


@pytest.mark.parametrize("plant", sorted(_PLANTS))
def test_the_roster_names_each_planted_defect(plant: str, tmp_path: Path) -> None:
    source, reason = _PLANTS[plant]
    (tmp_path / "m.py").write_text(textwrap.dedent(source), encoding="utf-8")
    _, findings = engine_sink_findings(tmp_path, floor=1)
    assert any("Good" in f and reason in f for f in findings), findings


def test_a_conforming_fixture_is_silent(tmp_path: Path) -> None:
    (tmp_path / "m.py").write_text(textwrap.dedent(_GOOD), encoding="utf-8")
    assert engine_sink_findings(tmp_path, floor=1) == ({"Good"}, [])


def test_a_tree_of_twelve_fails_the_floor(tmp_path: Path) -> None:
    body = "".join(_GOOD.replace("class Good", f"class Good{i}") for i in range(12))
    (tmp_path / "m.py").write_text(textwrap.dedent(body), encoding="utf-8")
    found, findings = engine_sink_findings(tmp_path)
    assert len(found) == 12
    assert findings == ["the scan found 12 engine sinks, under its floor of 13"]


# --- the runtime half ---------------------------------------------------------------------------


class _NATSClient:
    async def publish(self, subject: str, payload: bytes) -> None:
        pass

    async def close(self) -> None:
        pass


_RIGGED = (
    "SQSSink",
    "SNSSink",
    "KinesisSink",
    "FirehoseSink",
    "ClickHouseSink",
    "MongoDBSink",
    "PostgresSink",
    "RedisStreamsSink",
    "RedisListSink",
    "RabbitMQSink",
    "AzureEventHubsSink",
    "SyslogSink",
)


def _build(name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Any:
    if name == "NATSSink":
        return NATSSink("logs", client=_NATSClient(), publish_timeout=1.0)
    if name == "SocketTransport":
        return RIGS["SyslogSink"](monkeypatch, tmp_path).sink._socket
    return RIGS[name](monkeypatch, tmp_path).sink


_ALL = (*_RIGGED, "NATSSink", "SocketTransport")


@pytest.mark.parametrize("name", _ALL)
def test_every_engine_sink_reports_its_counters(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    sink = _build(name, monkeypatch, tmp_path)
    try:
        assert "log_foundry_counters" in vars(sink)
        assert type(_static_value(sink, "log_foundry_counters")) is SinkCounters
        assert sink.losses() == sink.log_foundry_counters.losses()
        if name != "SocketTransport":
            bound = read_deliver(sink)
            assert bound is not None and bound.__self__ is sink  # type: ignore[attr-defined]
        sink.failed = 7
        assert sink.failed == 7, "assignable, as 1.x's attribute was"
        extra = getattr(sink, "dropped_unadjudicated", 0)
        assert sink.log_foundry_counters.failed_events == 7 + extra
        sink.failed = 0
    finally:
        sink.close()


def test_no_shipped_sink_declares_itself_idempotent() -> None:
    declared = [
        f"{path.name} {node.name}"
        for path in sorted(SINKS.glob("*.py"))
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
        if isinstance(node, ast.ClassDef)
        for item in node.body
        if isinstance(item, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "log_foundry_idempotent" for t in item.targets)
    ]
    assert declared == []


@pytest.mark.parametrize("name", (*_RIGGED, "NATSSink"))
def test_an_emit_override_is_delivered_through_its_emit(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """SPEC-072's override rule on every engine sink: the worker calls the subclass's ``emit``."""
    sink = _build(name, monkeypatch, tmp_path)
    seen: list[int] = []
    base = type(sink)

    def emit(self: Any, batch: list[dict[str, object]]) -> None:
        seen.append(len(batch))
        base.emit(self, batch)

    sink.__class__ = type(f"Stamping{base.__name__}", (base,), {"emit": emit})
    try:
        assert read_deliver(sink) is None
        worker = Worker(sink, batch_size=10, flush_interval=60.0, max_retries=2)
        install_worker(worker)
        worker.submit([ok("a")])
        assert worker.flush(timeout=30.0)
        assert seen == [1]
        assert sink.losses().failed == 0
    finally:
        sink.close()


@pytest.mark.parametrize("name", _RIGGED)
def test_an_emit_override_counts_a_lost_batch_once_per_emit_call_as_1x(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The override's failing twin: each ``emit`` call is its own run, so it counts as 1.x did.

    A subclass that overrides ``emit`` keeps the worker's whole-batch loop, so a batch lost to a
    down destination is counted once per ``emit`` call — ``(w + 1) · n`` under a worker with
    ``max_retries=w`` — with one line per call naming that call's single attempt, as 1.x's
    ``emit`` counted it. Only the engine's own path counts a lost event once per batch.
    """
    rig = RIGS[name](monkeypatch, tmp_path)
    sink = rig.sink
    seen: list[int] = []
    base = type(sink)

    def emit(self: Any, batch: list[dict[str, object]]) -> None:
        seen.append(len(batch))
        base.emit(self, batch)

    sink.__class__ = type(f"Stamping{base.__name__}", (base,), {"emit": emit})
    rig.state.down = True
    retries = 2
    try:
        worker = Worker(sink, batch_size=10, flush_interval=60.0, max_retries=retries)
        install_worker(worker)
        worker.submit([ok("a"), ok("b")])
        assert not worker.flush(timeout=30.0)
        assert seen == [2] * (retries + 1)
        assert sink.failed == (retries + 1) * 2
        lines = [
            line
            for line in capsys.readouterr().err.splitlines()
            if line.startswith("log-foundry: lost") and "1 attempt(s)" in line
        ]
        assert len(lines) == retries + 1, lines
        assert all(line.startswith("log-foundry: lost 2 ") for line in lines), lines
    finally:
        sink.close()


@pytest.mark.parametrize("name", _ALL)
def test_a_forked_child_completes_an_emit_while_the_parent_holds_the_counter_lock(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    sink = _build(name, monkeypatch, tmp_path)
    owner = sink if name != "SocketTransport" else None
    if owner is not None:
        log_foundry.configure(service="fork", version="0", env="test", sink=owner)
    else:
        holder_sink = RIGS["SyslogSink"](monkeypatch, tmp_path).sink
        holder_sink._socket = sink
        holder_sink.log_foundry_counters = sink.log_foundry_counters
        log_foundry.configure(service="fork", version="0", env="test", sink=holder_sink)
    released, holder = _held_by_a_parent_thread(sink.log_foundry_counters._counter_lock)

    def emit_once() -> str:
        """One emit that commits a count — an unencodable event beside a good one — then a read.

        Both take the counters' lock the parent thread held at the fork, so neither returns
        unless the fork repair rebound it.
        """
        batch: list[dict[str, object]] = [ok("child"), {"blob": b"unencodable"}]
        try:
            if name == "SocketTransport":
                _engine.emit_via_deliver(sink, batch)
            else:
                sink.emit(batch)
        except Exception as err:
            return f"raised {type(err).__name__} {sink.losses().dropped}"
        return f"returned {sink.losses().dropped}"

    try:
        child = run_in_child(emit_once, timeout=5)
    finally:
        released.set()
        holder.join(5.0)
        sink.close()
    assert child.finished, f"the child {child.ending}, having written {child.output!r}"
    expected = "raised SinkDeliveryError 0" if name == "NATSSink" else "returned 1"
    assert child.output == expected, child.output
