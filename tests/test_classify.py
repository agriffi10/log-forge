"""SPEC-070 — what a sink retries: the predicates, the twelve loops, the vendor sets and the rosters.

Every retry loop under ``sinks/`` used to answer "is this failure worth another attempt?" its own
way: ten retried any exception, two let anything but a transport error escape ``emit`` raw. Each
loop now asks its class's ``_retryable_error`` through ``is_retryable``, and what the predicate
does not name fails at its first attempt, counted and announced exactly as exhaustion is.

The loop sweep builds eleven of the twelve loops from ``tests/sink_rigs.py`` (``SyslogSink`` for
``SocketTransport``, ``RedisListSink`` for ``_RedisSink``) and ``HTTPSink`` from a raising opener,
and is held to the population the loop roster derives, so a thirteenth loop needs an entry here.
"""

from __future__ import annotations

import ast
import errno
import os
import sys
import urllib.error
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

import driver_standins as standins
import log_foundry
import sink_rigs
from log_foundry.sinks import _classify
from log_foundry.sinks._classify import (
    TRANSIENT_BUILTINS,
    aws_retryable,
    driver_classes,
    is_retryable,
    sender_fault,
)
from log_foundry.sinks.base import SinkDeliveryError
from log_foundry.sinks.http import HTTPSink
from log_foundry.worker import Worker
from test_sinks_http import FakeResponse

if TYPE_CHECKING:
    from collections.abc import Callable

_SINKS = Path(__file__).resolve().parents[1] / "src" / "log_foundry" / "sinks"
"""Derived from this file, never from ``log_foundry.__file__``: an editable install can resolve
to another tree, and a lint that scans the wrong tree passes because it found nothing."""

_EXTRAS_EXPECTED = os.environ.get("LOG_FOUNDRY_EXTRAS") == "1"


# --- FR-001: the module ----------------------------------------------------------------------


def test_the_module_is_a_leaf() -> None:
    """FR-001 AC-1: ``_classify`` imports nothing from its own package, checked on its AST."""
    tree = ast.parse((_SINKS / "_classify.py").read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            assert not any(alias.name.startswith("log_foundry") for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            assert not (node.module or "").startswith("log_foundry")
            assert node.level == 0, "a relative import is an intra-package import"


def test_the_module_defines_the_arcs_names_and_exports_none() -> None:
    for name in (
        "ErrorPredicate",
        "EntryPredicate",
        "TRANSIENT_BUILTINS",
        "driver_classes",
        "is_retryable",
    ):
        assert hasattr(_classify, name), name
    assert (ConnectionError, TimeoutError) == TRANSIENT_BUILTINS
    assert not hasattr(_classify, "__all__")


def test_is_retryable_passes_the_answer_through() -> None:
    assert is_retryable(lambda err: True, ValueError()) is True
    assert is_retryable(lambda err: False, ValueError()) is False


class _Unreadable:
    def __bool__(self) -> bool:
        raise RuntimeError("no truth value")


def test_a_predicate_that_raises_or_answers_unreadably_is_not_retryable() -> None:
    """FR-001 AC-7: the wrapper is what makes every predicate total."""

    def raises(err: Exception) -> bool:
        raise RuntimeError("the predicate broke")

    assert is_retryable(raises, ConnectionError()) is False
    assert is_retryable(lambda err: _Unreadable(), ConnectionError()) is False  # type: ignore[arg-type,return-value]


def test_a_base_exception_from_a_predicate_propagates() -> None:
    def interrupted(err: Exception) -> bool:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        is_retryable(interrupted, ConnectionError())


# --- FR-002: driver_classes ------------------------------------------------------------------


def test_driver_classes_reads_only_what_is_loaded_and_imports_nothing(monkeypatch) -> None:
    """FR-002 AC-8."""
    monkeypatch.delitem(sys.modules, "lf_no_such_driver", raising=False)
    monkeypatch.setitem(sys.modules, "lf_mapped_to_none", None)
    module = standins.build(standins.REDIS)
    module.NotAClass = 3  # type: ignore[attr-defined]
    module.NotAnException = int  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "lf_stand_in", module)
    before = set(sys.modules)

    assert driver_classes("lf_no_such_driver", "ConnectionError") == ()
    assert driver_classes("lf_mapped_to_none", "ConnectionError") == ()
    assert driver_classes("lf_stand_in") == ()
    assert driver_classes(
        "lf_stand_in", "Missing", "ConnectionError", "NotAClass", "NotAnException", "TimeoutError"
    ) == (module.ConnectionError, module.TimeoutError)

    assert set(sys.modules) == before


def test_driver_classes_reads_a_raising_lookup_as_missing(monkeypatch) -> None:
    class _Lazy:
        def __getattr__(self, name: str) -> Any:
            raise RuntimeError("a lazy import failed")

    monkeypatch.setitem(sys.modules, "lf_lazy", _Lazy())
    assert driver_classes("lf_lazy", "ConnectionError") == ()


# --- FR-002: the AWS predicate ---------------------------------------------------------------


@pytest.fixture
def botocore(monkeypatch) -> Any:
    return standins.install(monkeypatch, standins.BOTOCORE)


@pytest.mark.parametrize(
    ("status", "code", "retried"),
    [
        (503, "Anything", True),
        (500, None, True),
        (429, "X", True),
        (400, "ThrottlingException", True),
        (400, "ProvisionedThroughputExceededException", True),
        (400, "KMSThrottlingException", True),
        (400, "RequestTimeout", True),
        (400, "InvalidArgumentException", False),
        (403, "AccessDenied", False),
        (404, "ResourceNotFoundException", False),
        ("503", "X", False),
        (True, "X", False),
        (400, ["ThrottlingException"], False),
        (400, 5, False),
    ],
)
def test_a_client_error_is_read_on_its_status_and_code(botocore, status, code, retried) -> None:
    """FR-002 AC-5, at the predicate: a ``list`` code or a ``str`` status reads as neither."""
    assert aws_retryable(standins.client_error(botocore, status, code)) is retried


@pytest.mark.parametrize(
    "response",
    [None, "x", {}, {"Error": None}, {"Error": {"Code": None}}, {"ResponseMetadata": "x"}],
    ids=["none", "str", "empty", "error-none", "code-none", "metadata-str"],
)
def test_a_malformed_client_error_fails_at_once_without_raising(botocore, response) -> None:
    error = botocore.ClientError(response, "Op")
    assert aws_retryable(error) is False


def test_botocores_transport_classes_are_retried_and_its_others_are_not(botocore) -> None:
    assert aws_retryable(botocore.ConnectionError())
    assert aws_retryable(botocore.HTTPClientError())
    assert not aws_retryable(botocore.ParamValidationError())
    assert not aws_retryable(botocore.NoCredentialsError())
    assert aws_retryable(ConnectionError())
    assert aws_retryable(TimeoutError())
    assert not aws_retryable(TypeError())


def test_without_botocore_only_the_builtins_are_retried(monkeypatch) -> None:
    module = standins.build(standins.BOTOCORE)
    monkeypatch.delitem(sys.modules, standins.BOTOCORE, raising=False)
    assert not aws_retryable(module.ConnectionError())
    assert aws_retryable(ConnectionError())


# --- FR-003: sender_fault --------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "fault"),
    [
        (None, False),
        (False, False),
        (0, False),
        ("", False),
        (True, True),
        (1, True),
        ("true", True),
        ("false", True),
    ],
)
def test_sender_fault_is_the_truthiness_sqs_always_tested(value, fault) -> None:
    """FR-003: ``bool(entry.get("SenderFault"))``, so ``SQSSink`` is unchanged byte for byte."""
    assert sender_fault({"SenderFault": value}) is fault
    assert sender_fault({}) is False


# --- FR-001: the loop roster -----------------------------------------------------------------

_LOOP_FLOOR = 12
"""The twelve retry loops measured when SPEC-070 was drafted and re-measured when it was built.

Exactly the count rather than below it: a refactor that hides a loop from the scan — a ``while``,
a bound named otherwise — must fail here rather than shrink the population silently. Raise it
when a loop is added; lower it only when one is deleted, in the same change.
"""


@dataclass(frozen=True)
class _Loop:
    module: str
    owner: str | None
    line: int


def _is_retry_loop(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.For | ast.AsyncFor)
        and isinstance(node.iter, ast.Call)
        and isinstance(node.iter.func, ast.Name)
        and node.iter.func.id == "range"
        and "retries" in ast.unparse(node.iter)
    )


def _asks_the_predicate(handler: ast.ExceptHandler) -> bool:
    """Whether the handler calls ``is_retryable(self._retryable_error, <its own name>)``."""
    for node in ast.walk(handler):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "is_retryable"
            and len(node.args) == 2
            and isinstance(node.args[0], ast.Attribute)
            and node.args[0].attr == "_retryable_error"
            and isinstance(node.args[0].value, ast.Name)
            and node.args[0].value.id == "self"
            and isinstance(node.args[1], ast.Name)
            and node.args[1].id == handler.name
        ):
            return True
    return False


def _calls_the_predicate_directly(handler: ast.ExceptHandler) -> bool:
    return any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "_retryable_error"
        for node in ast.walk(handler)
    )


def retry_loop_findings(
    root: Path, floor: int = _LOOP_FLOOR
) -> tuple[list[_Loop], list[str]]:
    """Every retry loop under ``root`` and every way one fails FR-001's rule.

    The rule: the loop sits in a class that defines ``_retryable_error`` in its own body, and
    every ``except`` of every ``try`` inside the loop names the exception and asks
    ``is_retryable(self._retryable_error, <that name>)``. A direct ``self._retryable_error(err)``
    does not count — it bypasses the wrapper that makes the predicate total. Fewer loops than
    ``floor`` is a finding too, so a scan that collapsed cannot pass by finding nothing to judge.
    """
    loops: list[_Loop] = []
    findings: list[str] = []

    def visit(node: ast.AST, path: Path, owner: ast.ClassDef | None) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.ClassDef):
                visit(child, path, child)
                continue
            if _is_retry_loop(child):
                where = f"{path.name}:{child.lineno} {owner.name if owner else '<module>'}"
                loops.append(_Loop(path.name, owner.name if owner else None, child.lineno))
                if owner is None:
                    findings.append(f"{where}: a retry loop outside any class")
                elif not any(
                    isinstance(m, ast.FunctionDef) and m.name == "_retryable_error"
                    for m in owner.body
                ):
                    findings.append(f"{where}: the class defines no _retryable_error")
                handlers = [
                    h for t in ast.walk(child) if isinstance(t, ast.Try) for h in t.handlers
                ]
                if not handlers:
                    findings.append(f"{where}: the loop catches nothing")
                for handler in handlers:
                    if handler.name is None:
                        findings.append(f"{where}: the handler does not name the exception")
                    elif not _asks_the_predicate(handler):
                        reason = (
                            "calls self._retryable_error directly, bypassing is_retryable"
                            if _calls_the_predicate_directly(handler)
                            else "never asks is_retryable(self._retryable_error, err)"
                        )
                        findings.append(f"{where}: the handler {reason}")
            visit(child, path, owner)

    for path in sorted(root.glob("*.py")):
        visit(ast.parse(path.read_text(encoding="utf-8")), path, None)
    if len(loops) < floor:
        findings.append(
            f"the scan found {len(loops)} retry loops, under its floor of {floor}: a refactor "
            "hid one from it, or one was deleted without lowering the floor"
        )
    return loops, findings


def test_every_retry_loop_asks_its_sinks_predicate() -> None:
    """FR-001 AC-10, over the shipped tree."""
    _, findings = retry_loop_findings(_SINKS)
    assert findings == []


_CORPUS_HEAD = '"""A fixture sink module."""\n\nfrom x import is_retryable\n\n'


def _corpus_class(
    name: str, handler_body: str, *, predicate: bool = True, as_name: str = " as err"
) -> str:
    method = "    def _retryable_error(self, err):\n        return True\n\n" if predicate else ""
    return (
        f"class {name}:\n"
        f'    """Fixture."""\n\n'
        f"{method}"
        "    def _send(self):\n"
        "        for attempt in range(self.max_retries + 1):\n"
        "            try:\n"
        "                self.client.call()\n"
        f"            except Exception{as_name}:\n"
        f"                {handler_body}\n"
        "\n"
    )


_GOOD = "if attempt < self.max_retries and is_retryable(self._retryable_error, err): continue"


@pytest.mark.parametrize(
    ("source", "reason"),
    [
        (_corpus_class("NeverAsks", "continue"), "NeverAsks: the handler never asks"),
        (
            _corpus_class("AsksDirectly", "if self._retryable_error(err): continue"),
            "AsksDirectly: the handler calls self._retryable_error directly",
        ),
        (
            _corpus_class("WrongName", "if is_retryable(self._retryable_error, other): continue"),
            "WrongName: the handler never asks",
        ),
        (
            _corpus_class("Unnamed", "continue", as_name=""),
            "Unnamed: the handler does not name the exception",
        ),
        (
            _corpus_class("NoPredicate", _GOOD, predicate=False),
            "NoPredicate: the class defines no _retryable_error",
        ),
        (
            (
                "def module_level(self):\n"
                "    for attempt in range(retries + 1):\n"
                "        try:\n"
                "            self.client.call()\n"
                "        except Exception as err:\n"
                "            if is_retryable(self._retryable_error, err): continue\n"
            ),
            "<module>: a retry loop outside any class",
        ),
    ],
    ids=["never-asks", "direct-call", "wrong-name", "unnamed", "no-predicate", "module-level"],
)
def test_the_loop_roster_names_the_class_and_the_reason(tmp_path, source, reason) -> None:
    (tmp_path / "fixture.py").write_text(_CORPUS_HEAD + source, encoding="utf-8")
    loops, findings = retry_loop_findings(tmp_path, floor=1)
    assert len(loops) == 1
    assert len(findings) == 1, findings
    assert reason in findings[0], findings[0]
    assert findings[0].startswith("fixture.py:")


@pytest.mark.parametrize(("count", "silent"), [(11, False), (12, True)])
def test_the_loop_roster_fails_eleven_conforming_loops_on_its_floor(
    tmp_path, count: int, silent: bool
) -> None:
    """Twelve conforming loops are silent; eleven fail on the floor alone, by the same path."""
    body = "".join(_corpus_class(f"Good{i}", _GOOD) for i in range(count))
    (tmp_path / "fixture.py").write_text(_CORPUS_HEAD + body, encoding="utf-8")
    loops, findings = retry_loop_findings(tmp_path)
    assert len(loops) == count
    if silent:
        assert findings == []
    else:
        expected = (
            "the scan found 11 retry loops, under its floor of 12: a refactor hid one from it, "
            "or one was deleted without lowering the floor"
        )
        assert findings == [expected]


# --- FR-001: the twelve loops, swept ---------------------------------------------------------


class _RaisingOpener:
    """An opener whose every request raises ``error`` (``KeyboardInterrupt`` included)."""

    def __init__(self) -> None:
        self.calls = 0
        self.error: BaseException | None = None

    def __call__(self, request: Any, timeout: float | None = None) -> Any:
        self.calls += 1
        if self.error is not None:
            raise self.error
        return FakeResponse(200, b"{}")


@dataclass
class _Built:
    """One loop's sink, how to count its client calls, set its error and find its loop owner."""

    sink: Any
    owner: Any
    calls: Callable[[], int]
    set_error: Callable[[BaseException], None]
    waits: list[float]


def _rig(name: str) -> Callable[[pytest.MonkeyPatch, Path, int], _Built]:
    def build(mp: pytest.MonkeyPatch, tmp: Path, retries: int) -> _Built:
        rig = sink_rigs.RIGS[name](mp, tmp)
        owner = rig.owner if name == "SyslogSink" else rig.sink
        attr = "_max_retries" if hasattr(owner, "_max_retries") else "max_retries"
        setattr(owner, attr, retries)
        waits: list[float] = []
        module = sys.modules[type(owner).__module__]
        mp.setattr(module, "wait", lambda delay, stop=None: waits.append(delay))

        def set_error(error: BaseException) -> None:
            rig.state.error = error

        return _Built(rig.sink, owner, rig.calls, set_error, waits)

    return build


def _http(mp: pytest.MonkeyPatch, tmp: Path, retries: int) -> _Built:
    opener = _RaisingOpener()
    sink = HTTPSink("http://h", opener=opener, max_retries=retries)
    waits: list[float] = []
    sink._sleep_backoff = lambda attempt, retry_after: waits.append(attempt)  # type: ignore[method-assign]

    def set_error(error: BaseException) -> None:
        opener.error = error

    return _Built(sink, sink, lambda: opener.calls, set_error, waits)


_LOOPS: dict[str, Callable[[pytest.MonkeyPatch, Path, int], _Built]] = {
    "SocketTransport": _rig("SyslogSink"),
    "HTTPSink": _http,
    "ClickHouseSink": _rig("ClickHouseSink"),
    "AzureEventHubsSink": _rig("AzureEventHubsSink"),
    "FirehoseSink": _rig("FirehoseSink"),
    "KinesisSink": _rig("KinesisSink"),
    "MongoDBSink": _rig("MongoDBSink"),
    "PostgresSink": _rig("PostgresSink"),
    "RabbitMQSink": _rig("RabbitMQSink"),
    "_RedisSink": _rig("RedisListSink"),
    "SNSSink": _rig("SNSSink"),
    "SQSSink": _rig("SQSSink"),
}
"""Each loop class, by the class that owns the loop, and how to build it at a chosen bound."""


def test_the_sweep_covers_every_retry_loop_the_roster_finds() -> None:
    loops, _ = retry_loop_findings(_SINKS)
    assert {loop.owner for loop in loops} == set(_LOOPS)


_EVENTS = [{"log_id": "a", "level": "INFO", "message": "one"}]


def _outcome(built: _Built) -> type[BaseException] | None:
    try:
        built.sink.emit(list(_EVENTS))
    except SinkDeliveryError as err:
        return type(err)
    return None


def _losses(built: _Built) -> Any:
    from log_foundry.sinks.base import read_losses

    return read_losses(built.sink)


def _lost(err: str) -> list[str]:
    return [line for line in err.splitlines() if "log-foundry: lost" in line]


@pytest.mark.parametrize("name", sorted(_LOOPS))
def test_a_type_error_is_one_attempt_counted_as_exhaustion_counts(
    name: str, monkeypatch, tmp_path, capsys
) -> None:
    """FR-001 AC-2: one call, no wait, one line saying ``1 attempt``, exhaustion's own exit."""
    exhausted = _LOOPS[name](monkeypatch, tmp_path, 0)
    exhausted.set_error(ConnectionError("down"))
    expected_outcome = _outcome(exhausted)
    expected_losses = _losses(exhausted)
    capsys.readouterr()

    built = _LOOPS[name](monkeypatch, tmp_path, 3)
    built.set_error(TypeError("the caller's own code"))
    outcome = _outcome(built)

    assert built.calls() == 1, "a TypeError is not retried"
    assert built.waits == [], "and takes no backoff wait"
    assert outcome is expected_outcome
    assert _losses(built) == expected_losses, "the counter exhaustion moves rises the same"
    lines = _lost(capsys.readouterr().err)
    assert len(lines) == 1, lines
    assert "1 attempt(s)" in lines[0] and "TypeError" in lines[0], lines[0]
    assert "the caller's own code" not in lines[0]


@pytest.mark.parametrize("name", sorted(_LOOPS))
def test_an_accepted_exception_is_attempted_max_retries_plus_one(
    name: str, monkeypatch, tmp_path, capsys
) -> None:
    """FR-001 AC-3."""
    built = _LOOPS[name](monkeypatch, tmp_path, 2)
    built.set_error(ConnectionError("down"))
    _outcome(built)

    assert built.calls() == 3
    assert len(built.waits) == 2
    lines = _lost(capsys.readouterr().err)
    assert len(lines) == 1 and "3 attempt(s)" in lines[0], lines


@pytest.mark.parametrize("name", sorted(_LOOPS))
def test_a_keyboard_interrupt_propagates_and_moves_no_counter(
    name: str, monkeypatch, tmp_path
) -> None:
    """FR-001 AC-8."""
    built = _LOOPS[name](monkeypatch, tmp_path, 3)
    before = _losses(built)
    built.set_error(KeyboardInterrupt())

    with pytest.raises(KeyboardInterrupt):
        built.sink.emit(list(_EVENTS))

    assert built.calls() == 1
    assert _losses(built) == before


@pytest.mark.parametrize("name", sorted(_LOOPS))
def test_a_predicate_that_raises_fails_at_once_naming_the_original(
    name: str, monkeypatch, tmp_path, capsys
) -> None:
    """FR-001 AC-7: the sink makes one attempt and the line names the call's exception."""
    built = _LOOPS[name](monkeypatch, tmp_path, 3)

    def broken(err: Exception) -> bool:
        raise LookupError("the predicate broke")

    monkeypatch.setattr(built.owner, "_retryable_error", broken)
    built.set_error(ConnectionError("down"))
    _outcome(built)

    assert built.calls() == 1
    lines = _lost(capsys.readouterr().err)
    assert len(lines) == 1, lines
    assert "ConnectionError" in lines[0] and "LookupError" not in lines[0]


@pytest.mark.parametrize(
    ("name", "cleanup"),
    [("RabbitMQSink", "_reset"), ("PostgresSink", "_rollback"), ("SocketTransport", "_reset")],
)
def test_the_cleanup_runs_once_before_a_type_error_gives_up(
    name: str, cleanup: str, monkeypatch, tmp_path
) -> None:
    """FR-001 AC-6: before the counter moves."""
    built = _LOOPS[name](monkeypatch, tmp_path, 3)
    original = getattr(built.owner, cleanup)
    seen: list[int] = []

    def spy() -> None:
        seen.append(_losses(built).failed)
        original()

    monkeypatch.setattr(built.owner, cleanup, spy)
    built.set_error(TypeError("x"))
    _outcome(built)

    assert seen == [0], "called exactly once, before the batch was counted"
    assert _losses(built).failed > 0


# --- FR-001: the HTTP, socket and Sentry specifics -------------------------------------------


def test_an_opener_runtime_error_is_one_request_counted_and_raised(capsys) -> None:
    """FR-001 AC-4: measured before as ``RuntimeError`` out of ``emit`` with ``failed=0``."""
    opener = _RaisingOpener()
    opener.error = RuntimeError("injected")
    sink = HTTPSink("http://h", opener=opener, max_retries=3)

    with pytest.raises(SinkDeliveryError):
        sink.emit(list(_EVENTS))

    assert opener.calls == 1
    assert sink.losses().failed == 1
    assert "HTTPSink, 1 attempt(s), connection error, RuntimeError" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("error", "sends"),
    [
        (ValueError("I/O operation on closed file"), 1),
        (OSError(errno.EMSGSIZE, "Message too long"), 1),
        (OSError(errno.ECONNREFUSED, "Connection refused"), 4),
    ],
    ids=["ValueError", "EMSGSIZE", "ECONNREFUSED"],
)
def test_the_socket_transport_retries_only_a_transient_os_error(
    error: Exception, sends: int, monkeypatch, tmp_path
) -> None:
    """FR-001 AC-4."""
    built = _LOOPS["SocketTransport"](monkeypatch, tmp_path, 3)
    built.set_error(error)
    _outcome(built)

    assert built.calls() == sends
    assert built.owner.losses().failed == 1


def _sentry(opener: _RaisingOpener, monkeypatch) -> Any:
    from log_foundry.sinks.sentry import SentrySink

    sink = SentrySink("http://k@sentry.local/1", backend="http", max_retries=3, opener=opener)
    assert sink._http is not None
    monkeypatch.setattr(sink._http, "_sleep_backoff", lambda attempt, retry_after: None)
    return sink


def test_sentry_keeps_an_unretried_exception_on_its_own_counter(monkeypatch, capsys) -> None:
    """FR-001 AC-5: one request, ``.transport_errors`` +1, the held sink's ``failed`` +0."""
    opener = _RaisingOpener()
    opener.error = RuntimeError("injected")
    sink = _sentry(opener, monkeypatch)

    with pytest.raises(SinkDeliveryError):
        sink.emit([{"level": "ERROR"}])

    assert opener.calls == 1
    assert sink.transport_errors == 1
    assert sink._http.failed == 0
    assert "log-foundry: lost 1 event(s); SentrySink, RuntimeError" in capsys.readouterr().err


def test_sentry_still_retries_and_counts_a_transport_error(monkeypatch) -> None:
    opener = _RaisingOpener()
    opener.error = urllib.error.URLError("unreachable")
    sink = _sentry(opener, monkeypatch)

    with pytest.raises(SinkDeliveryError):
        sink.emit([{"level": "ERROR"}])

    assert opener.calls == 4
    assert sink._http.failed == 1
    assert sink.transport_errors == 0


# --- FR-001: both delivery paths -------------------------------------------------------------


def test_the_worker_and_the_orphan_path_each_make_one_call_per_attempt(
    monkeypatch, tmp_path
) -> None:
    """FR-001 AC-9 (invariant 6): a Redis ``TypeError`` is 3 calls through ``Worker(max_retries=2)``,
    one per worker attempt — not 3 x 4 — and 1 call outside a span."""
    through_worker = _LOOPS["_RedisSink"](monkeypatch, tmp_path, 3)
    through_worker.set_error(TypeError("x"))
    worker = Worker(through_worker.sink, batch_size=1, max_retries=2)
    try:
        worker.submit([{"level": "INFO", "message": "m"}])
        assert not worker.flush(timeout=30.0)
    finally:
        worker.stop()
    assert through_worker.calls() == 3
    assert through_worker.sink.losses().failed == 3, "one per worker attempt"

    orphan = _LOOPS["_RedisSink"](monkeypatch, tmp_path, 3)
    orphan.set_error(TypeError("x"))
    log_foundry.configure(service="t", sink=orphan.sink)
    log_foundry.info("outside a span")
    assert orphan.calls() == 1
    assert orphan.sink.losses().failed == 1


# --- FR-002: each vendor set, against stand-in drivers ---------------------------------------


def _named(name: str, **kwargs: Any) -> Callable[[Any], Exception]:
    def make(module: Any) -> Exception:
        error: Exception = getattr(module, name)(**kwargs)
        return error

    make.__name__ = name
    return make


def _client(status: object, code: object) -> Callable[[Any], Exception]:
    def make(module: Any) -> Exception:
        return standins.client_error(module, status, code)

    make.__name__ = f"ClientError-{status}-{code}"
    return make


def _pg(message: str = "", sqlstate: object = None) -> Callable[[Any], Exception]:
    def make(module: Any) -> Exception:
        error: Exception = module.OperationalError(message, sqlstate=sqlstate)
        return error

    make.__name__ = f"OperationalError-{sqlstate}"
    return make


_AWS_RETRIED = [
    _named("ConnectionError"),
    _named("HTTPClientError"),
    _client(503, "X"),
    _client(400, "ThrottlingException"),
]
_AWS_ONCE = [
    _named("ParamValidationError"),
    _named("NoCredentialsError"),
    _client(400, "InvalidArgumentException"),
    _client(403, "AccessDenied"),
    _client(400, "ResourceNotFoundException"),
]

_ROWS: list[tuple[str, str, list[Callable[[Any], Exception]], list[Callable[[Any], Exception]]]] = [
    *[
        (name, standins.BOTOCORE, _AWS_RETRIED, _AWS_ONCE)
        for name in ("SQSSink", "SNSSink", "KinesisSink", "FirehoseSink")
    ],
    (
        "ClickHouseSink",
        standins.CLICKHOUSE,
        [_named("OperationalError")],
        [_named(n) for n in ("DatabaseError", "DataError", "ProgrammingError", "InterfaceError")],
    ),
    (
        "MongoDBSink",
        standins.PYMONGO,
        [
            _named(n)
            for n in (
                "ConnectionFailure",
                "AutoReconnect",
                "NetworkTimeout",
                "NotPrimaryError",
                "ServerSelectionTimeoutError",
                "WaitQueueTimeoutError",
            )
        ]
        + [_named("OperationFailure", labels=("RetryableWriteError",))],
        [
            _named(n)
            for n in (
                "OperationFailure",
                "WriteError",
                "DuplicateKeyError",
                "WTimeoutError",
                "InvalidDocument",
                "DocumentTooLarge",
                "InvalidOperation",
                "ConfigurationError",
            )
        ],
    ),
    (
        "RedisListSink",
        standins.REDIS,
        [
            _named(n)
            for n in ("ConnectionError", "TimeoutError", "TryAgainError", "ClusterDownError")
        ],
        [
            _named(n)
            for n in (
                "ResponseError",
                "DataError",
                "AuthenticationError",
                "AuthorizationError",
                "ExternalAuthProviderError",
            )
        ],
    ),
    (
        "RabbitMQSink",
        standins.PIKA,
        [_named(n) for n in ("AMQPConnectionError", "StreamLostError", "ChannelWrongStateError")],
        [
            _named(n)
            for n in (
                "AuthenticationError",
                "ProbableAuthenticationError",
                "ProbableAccessDeniedError",
                "IncompatibleProtocolError",
                "ConnectionClosedByClient",
                "ConnectionOpenAborted",
                "ChannelClosedByBroker",
                "ProtocolSyntaxError",
            )
        ],
    ),
    (
        "AzureEventHubsSink",
        standins.AZURE,
        [_named(n) for n in ("ConnectionLostError", "ConnectError", "OperationTimeoutError")],
        [
            _named(n)
            for n in (
                "AuthenticationError",
                "EventDataError",
                "EventDataSendError",
                "ClientClosedError",
                "EventHubError",
            )
        ],
    ),
    (
        "PostgresSink",
        standins.PSYCOPG,
        [
            _pg("could not receive data from server"),
            _pg("", "40001"),
            _pg("", "08006"),
            _pg("", "53300"),
            _pg("", "57P01"),
            _pg("", "55P03"),
            _pg("", "55006"),
        ],
        [
            _pg("", "28P01"),
            _pg("", "54000"),
            _pg("", "55000"),
            _pg("", "42P01"),
            _pg("", 22001),
            _pg("", ""),
            _pg("", "4"),
            _pg('connection failed: FATAL:  password authentication failed for user "postgres"'),
            _named("DataError"),
            _named("StringDataRightTruncation"),
            _named("IntegrityError"),
            _named("ProgrammingError"),
            _named("InternalError"),
            _named("InterfaceError"),
            _named("NotSupportedError"),
        ],
    ),
]
"""FR-002's table, row by row: the sink, its driver module, what is retried and what is not."""

_CASES = [
    pytest.param(
        sink, key, make, retried, id=f"{sink}-{make.__name__}-{'retried' if retried else 'once'}"
    )
    for sink, key, retried_list, once_list in _ROWS
    for retried, makes in ((True, retried_list), (False, once_list))
    for make in makes
]


@pytest.mark.parametrize(("sink", "key", "make", "retried"), _CASES)
def test_each_vendor_class_is_attempted_as_its_row_says(
    sink: str, key: str, make: Callable[[Any], Exception], retried: bool, monkeypatch, tmp_path
) -> None:
    """FR-002 AC-1, through each loop: ``max_retries + 1`` attempts, or one."""
    module = standins.install(monkeypatch, key)
    loop = {"RedisListSink": "_RedisSink"}.get(sink, sink)
    built = _LOOPS[loop](monkeypatch, tmp_path, 2)
    built.set_error(make(module))
    _outcome(built)
    assert built.calls() == (3 if retried else 1)


_PREDICATE_PROBES = [
    ("ClickHouseSink", standins.CLICKHOUSE, "OperationalError"),
    ("MongoDBSink", standins.PYMONGO, "AutoReconnect"),
    ("_RedisSink", standins.REDIS, "ConnectionError"),
    ("RabbitMQSink", standins.PIKA, "AMQPConnectionError"),
    ("AzureEventHubsSink", standins.AZURE, "ConnectionLostError"),
    ("SQSSink", standins.BOTOCORE, "HTTPClientError"),
]


@pytest.mark.parametrize(("loop", "key", "name"), _PREDICATE_PROBES)
def test_each_predicate_resolves_its_driver_per_call(
    loop: str, key: str, name: str, monkeypatch, tmp_path
) -> None:
    """FR-002 AC-4: absent on the first call, loaded before the second — retried on the second."""
    built = _LOOPS[loop](monkeypatch, tmp_path, 2)
    module = standins.build(key)
    error = getattr(module, name)()
    monkeypatch.delitem(sys.modules, key, raising=False)
    assert built.owner._retryable_error(error) is False

    monkeypatch.setitem(sys.modules, key, module)
    assert built.owner._retryable_error(error) is True


def test_postgres_resolves_its_driver_per_call(monkeypatch, tmp_path) -> None:
    built = _LOOPS["PostgresSink"](monkeypatch, tmp_path, 2)
    module = standins.build(standins.PSYCOPG)
    error = module.OperationalError("", sqlstate="40001")
    monkeypatch.delitem(sys.modules, standins.PSYCOPG, raising=False)
    assert built.owner._retryable_error(error) is False
    monkeypatch.setitem(sys.modules, standins.PSYCOPG, module)
    assert built.owner._retryable_error(error) is True


@pytest.mark.parametrize("loop", ["SQSSink", "SNSSink", "KinesisSink", "FirehoseSink"])
@pytest.mark.parametrize(
    ("response", "calls"),
    [
        ({"Error": {"Code": "X"}, "ResponseMetadata": {"HTTPStatusCode": 503}}, 3),
        (
            {"Error": {"Code": "ThrottlingException"}, "ResponseMetadata": {"HTTPStatusCode": 400}},
            3,
        ),
        (
            {
                "Error": {"Code": "InvalidArgumentException"},
                "ResponseMetadata": {"HTTPStatusCode": 400},
            },
            1,
        ),
        (None, 1),
        ({"ResponseMetadata": {"HTTPStatusCode": 400}}, 1),
        ({"Error": {"Code": ["ThrottlingException"]}}, 1),
        ({"Error": {"Code": "X"}, "ResponseMetadata": {"HTTPStatusCode": "503"}}, 1),
    ],
    ids=["503", "throttled", "invalid", "none", "no-error", "list-code", "str-status"],
)
def test_each_aws_sink_reads_a_client_error_on_status_and_code(
    loop: str, response: Any, calls: int, botocore, monkeypatch, tmp_path
) -> None:
    """FR-002 AC-5, through each of the four AWS loops."""
    built = _LOOPS[loop](monkeypatch, tmp_path, 2)
    built.set_error(botocore.ClientError(response, "Op"))
    _outcome(built)
    assert built.calls() == calls


# --- FR-002: the extras leg ------------------------------------------------------------------


def _driver_class_calls(root: Path) -> tuple[dict[str, set[str]], list[str]]:
    """Every ``driver_classes`` call under ``root``: module to the names it asks for."""
    found: dict[str, set[str]] = {}
    findings: list[str] = []
    for path in sorted(root.glob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "driver_classes"
            ):
                continue
            if not all(isinstance(a, ast.Constant) and isinstance(a.value, str) for a in node.args):
                findings.append(f"{path.name}:{node.lineno}: a non-literal driver_classes argument")
                continue
            module, *names = (a.value for a in node.args)  # type: ignore[attr-defined]
            found.setdefault(module, set()).update(names)
    return found, findings


def test_every_driver_classes_call_is_literal_and_the_scan_finds_all_seven_drivers() -> None:
    found, findings = _driver_class_calls(_SINKS)
    assert findings == []
    assert set(found) == set(standins.TREES), "the stand-ins model every driver a sink reads"
    for module, names in found.items():
        missing = names - set(standins.TREES[module])
        assert not missing, f"{module}: no stand-in for {sorted(missing)}"


def hierarchy_mismatches(module_name: str, real: Any) -> list[str]:
    """Every pair of stand-in classes whose subclass relation the real driver does not share.

    Both directions, for every ordered pair in the stand-in tree: a link the stand-in models and
    the driver lacks, and a link the driver has that the stand-in leaves out, are each a
    mismatch, because a predicate's exclusion is tested against the stand-in's model.
    """
    stand = standins.build(module_name)
    names = list(standins.TREES[module_name])
    mismatches: list[str] = []
    for a in names:
        for b in names:
            modelled = issubclass(getattr(stand, a), getattr(stand, b))
            actual = issubclass(getattr(real, a), getattr(real, b))
            if modelled != actual:
                mismatches.append(
                    f"{module_name}: issubclass({a}, {b}) is {actual} in the driver, "
                    f"{modelled} in the stand-in"
                )
    return mismatches


def test_every_class_a_predicate_names_exists_in_the_installed_driver() -> None:
    """FR-002 AC-7: under ``LOG_FOUNDRY_EXTRAS=1`` (the integration workflow's extras leg).

    Every name a predicate resolves exists and is an exception class, so a rename cannot empty a
    set unnoticed; every class the stand-ins model exists in the real driver; and every subclass
    relation between them agrees in both directions, so an exclusion tested against a stand-in is
    an exclusion the driver shares.
    """
    import importlib

    if not _EXTRAS_EXPECTED:
        pytest.skip("needs the optional extras; LOG_FOUNDRY_EXTRAS=1 makes this a failure")
    found, _ = _driver_class_calls(_SINKS)
    for module_name, tree in standins.TREES.items():
        real = importlib.import_module(module_name)
        for name in found[module_name]:
            cls = getattr(real, name, None)
            assert isinstance(cls, type) and issubclass(cls, BaseException), (module_name, name)
        for name in tree:
            assert isinstance(getattr(real, name, None), type), (module_name, name)
        assert hierarchy_mismatches(module_name, real) == []


def test_the_hierarchy_check_reports_a_link_either_side_lacks(monkeypatch) -> None:
    """The two-way comparison above, against a hand-built "driver", both directions."""
    real = standins.build(standins.REDIS)
    tree = dict(standins.TREES[standins.REDIS])
    monkeypatch.setitem(standins.TREES, standins.REDIS, tree)
    assert hierarchy_mismatches(standins.REDIS, real) == []

    tree["AuthorizationError"] = ("RedisError",)
    mismatches = hierarchy_mismatches(standins.REDIS, real)
    assert (
        "redis.exceptions: issubclass(AuthorizationError, ConnectionError) is True in the "
        "driver, False in the stand-in"
    ) in mismatches


# --- FR-003: the SenderFault roster ----------------------------------------------------------


def sender_fault_findings(root: Path) -> tuple[set[str], list[str]]:
    """Classes reaching ``sender_fault``, and every read of ``SenderFault`` outside it."""
    readers: set[str] = set()
    findings: list[str] = []

    def visit(node: ast.AST, path: Path, owner: str, function: str) -> None:
        for child in ast.iter_child_nodes(node):
            inner_owner, inner_function = owner, function
            if isinstance(child, ast.ClassDef):
                inner_owner = child.name
            elif isinstance(child, ast.FunctionDef):
                inner_function = child.name
            if isinstance(child, ast.Name) and child.id == "sender_fault" and owner:
                readers.add(owner)
            if _reads_sender_fault(child) and not (
                path.name == "_classify.py" and function == "sender_fault"
            ):
                findings.append(
                    f"{path.name}:{child.lineno} {owner or '<module>'}: reads "
                    f"SenderFault itself rather than through sender_fault"
                )
            visit(child, path, inner_owner, inner_function)

    for path in sorted(root.glob("*.py")):
        visit(ast.parse(path.read_text(encoding="utf-8")), path, "", "")
    return readers, findings


def _reads_sender_fault(node: ast.AST) -> bool:
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        return node.func.attr == "get" and any(
            isinstance(a, ast.Constant) and a.value == "SenderFault" for a in node.args
        )
    if isinstance(node, ast.Subscript):
        return isinstance(node.slice, ast.Constant) and node.slice.value == "SenderFault"
    return False


def test_sqs_and_sns_read_sender_fault_only_through_the_shared_function() -> None:
    """FR-003 AC-5 (invariant 6): the twins answer the one field the same way."""
    readers, findings = sender_fault_findings(_SINKS)
    assert findings == []
    assert {"SQSSink", "SNSSink"} <= readers


def test_the_sender_fault_roster_reports_a_class_reading_the_field_itself(tmp_path) -> None:
    (tmp_path / "fixture.py").write_text(
        '"""Fixture."""\n\n'
        "class ReadsItself:\n"
        '    """Fixture."""\n\n'
        "    def _send(self, item):\n"
        '        return item.get("SenderFault") or item["SenderFault"]\n\n'
        "class Shared:\n"
        '    """Fixture."""\n\n'
        "    def _retryable_entry(self, entry):\n"
        "        return not sender_fault(entry)\n",
        encoding="utf-8",
    )
    readers, findings = sender_fault_findings(tmp_path)
    assert readers == {"Shared"}
    assert len(findings) == 2
    assert all("ReadsItself: reads SenderFault itself" in f for f in findings), findings


# --- FR-003: Kinesis and Firehose ------------------------------------------------------------


@dataclass(frozen=True)
class _Stream:
    loop: str
    method: str
    count_key: str
    array_key: str


_STREAMS = [
    _Stream("KinesisSink", "put_records", "FailedRecordCount", "Records"),
    _Stream("FirehoseSink", "put_record_batch", "FailedPutCount", "RequestResponses"),
]


class _Scripted:
    """A Kinesis or Firehose client answering each call with the next scripted reply."""

    def __init__(self, replies: list[Any]) -> None:
        self.replies = replies
        self.sent: list[int] = []

    def reply(self, records: list[Any]) -> Any:
        self.sent.append(len(records))
        return self.replies[min(len(self.sent) - 1, len(self.replies) - 1)]

    def put_records(self, *, StreamName: str, Records: list[Any]) -> Any:
        return self.reply(Records)

    def put_record_batch(self, *, DeliveryStreamName: str, Records: list[Any]) -> Any:
        return self.reply(Records)


def _stream_sink(stream: _Stream, replies: list[Any], monkeypatch) -> tuple[Any, _Scripted]:
    from log_foundry.sinks.firehose import FirehoseSink
    from log_foundry.sinks.kinesis import KinesisSink

    client = _Scripted(replies)
    cls = KinesisSink if stream.loop == "KinesisSink" else FirehoseSink
    sink = cls("s", client=client, max_retries=1)
    monkeypatch.setattr(sys.modules[cls.__module__], "wait", lambda *_a, **_k: None)
    return sink, client


_NO_COUNT = object()
"""Stands for "the reply carries no failed count at all"."""


def _reply(stream: _Stream, count: object, codes: list[object] | None) -> dict[str, Any]:
    reply: dict[str, Any] = {}
    if count is not _NO_COUNT:
        reply[stream.count_key] = count
    if codes is not None:
        reply[stream.array_key] = [{} if c is None else {"ErrorCode": c} for c in codes]
    return reply


_TWO = [{"log_id": "a", "level": "INFO"}, {"log_id": "b", "level": "INFO"}]


@pytest.mark.parametrize("stream", _STREAMS, ids=lambda s: s.loop)
@pytest.mark.parametrize(
    ("count", "codes"),
    [
        (0, ["InternalFailure", "InternalFailure"]),
        (_NO_COUNT, ["InternalFailure", "InternalFailure"]),
        (2, [None, None]),
    ],
    ids=["zero-beside-coded", "absent-beside-coded", "two-beside-none"],
)
def test_a_failed_count_its_entries_contradict_is_unadjudicated(
    stream: _Stream, count: object, codes: list[object], monkeypatch, capsys
) -> None:
    """FR-003 AC-8 to AC-10: measured before as nothing counted, nothing printed."""
    sink, client = _stream_sink(stream, [_reply(stream, count, codes)], monkeypatch)

    sink.emit(list(_TWO))

    assert client.sent == [2], "one call, not resent"
    assert sink.dropped_unadjudicated == 2
    assert sink.failed == 0
    lines = _lost(capsys.readouterr().err)
    assert len(lines) == 1, lines
    assert "could not adjudicate" in lines[0]
    expected = "no failed count" if count is _NO_COUNT else f"failed count {count}"
    assert expected in lines[0], "the line names the count the entries contradict"


@pytest.mark.parametrize("stream", _STREAMS, ids=lambda s: s.loop)
@pytest.mark.parametrize(
    ("first", "sent", "unadjudicated"),
    [
        ((0, [None, None]), [2], 0),
        ((_NO_COUNT, [None, None]), [2], 0),
        ((0, None), [2], 0),
        ((_NO_COUNT, None), [2], 0),
        ((1, [None, "InternalFailure"]), [2, 1], 0),
        ((True, [None, "InternalFailure"]), [2], 2),
        (("1", [None, "InternalFailure"]), [2], 2),
        ((None, [None, None]), [2], 2),
        ((1, ["InternalFailure"]), [2], 2),
    ],
    ids=[
        "zero",
        "absent",
        "zero-no-array",
        "absent-no-array",
        "one-coded",
        "bool",
        "str",
        "explicit-none",
        "short-array",
    ],
)
def test_consistent_replies_read_as_before_and_odd_counts_are_unadjudicated(
    stream: _Stream, first: tuple[object, Any], sent: list[int], unadjudicated: int, monkeypatch
) -> None:
    """FR-003 AC-11, and D6 as the plan review settled it: an explicit ``None`` is not an int."""
    count, codes = first
    sink, client = _stream_sink(
        stream, [_reply(stream, count, codes), _reply(stream, 0, [None])], monkeypatch
    )

    sink.emit(list(_TWO))

    assert client.sent == sent
    assert sink.dropped_unadjudicated == unadjudicated
    assert sink.failed == 0


@pytest.mark.parametrize("stream", _STREAMS, ids=lambda s: s.loop)
@pytest.mark.parametrize(
    "code",
    [
        "ProvisionedThroughputExceededException",
        "ServiceUnavailableException",
        "InternalFailure",
        "ValidationException",
        5,
        ["x"],
    ],
    ids=["provisioned", "unavailable", "internal", "undocumented", "int", "list"],
)
def test_every_failed_record_is_retried_documented_or_not(
    stream: _Stream, code: object, monkeypatch, capsys
) -> None:
    """FR-003 AC-6: ``max_retries + 1`` attempts, then counted ``failed`` once."""
    replies = [_reply(stream, 1, [None, code]), _reply(stream, 1, [code])]
    sink, client = _stream_sink(stream, replies, monkeypatch)

    assert sink._retryable_entry({"ErrorCode": code}) is True
    sink.emit(list(_TWO))

    assert client.sent == [2, 1], "the one failed record resent, the bound spent"
    assert sink.failed == 1
    assert sink.dropped_unadjudicated == 0
    assert len(_lost(capsys.readouterr().err)) == 1


@pytest.mark.parametrize("stream", _STREAMS, ids=lambda s: s.loop)
def test_a_record_with_no_error_code_is_not_resent(stream: _Stream, monkeypatch) -> None:
    """FR-003 AC-7."""
    sink, client = _stream_sink(stream, [_reply(stream, 0, [None, {}])], monkeypatch)
    sink.emit(list(_TWO))
    assert client.sent == [2]
    assert (sink.failed, sink.dropped_unadjudicated) == (0, 0)


# --- FR-003: SQS and SNS, through the worker -------------------------------------------------


class _Rejecting:
    """An SQS or SNS client that rejects every entry, as ``SenderFault`` says."""

    def __init__(self, sender_fault: object) -> None:
        self.calls = 0
        self.sender_fault = sender_fault

    def _failed(self, entries: list[dict[str, Any]]) -> dict[str, Any]:
        self.calls += 1
        return {
            "Successful": [],
            "Failed": [
                {"Id": e["Id"], "Code": "InvalidParameterValue", "SenderFault": self.sender_fault}
                for e in entries
            ],
        }

    def send_message_batch(self, *, QueueUrl: str, Entries: list[dict[str, Any]]) -> Any:
        return self._failed(Entries)

    def publish_batch(self, *, TopicArn: str, PublishBatchRequestEntries: list[Any]) -> Any:
        return self._failed(PublishBatchRequestEntries)


def _queue_sink(kind: str, client: _Rejecting, monkeypatch) -> Any:
    from log_foundry.sinks.sns import SNSSink
    from log_foundry.sinks.sqs import SQSSink

    sink = (
        SQSSink("https://sqs/q", client=client, max_retries=3)
        if kind == "SQSSink"
        else SNSSink("arn", client=client, max_retries=3)
    )
    monkeypatch.setattr(sys.modules[type(sink).__module__], "wait", lambda *_a, **_k: None)
    return sink


@pytest.mark.parametrize("kind", ["SQSSink", "SNSSink"])
@pytest.mark.parametrize(
    ("value", "calls"),
    [(None, 4), (False, 4), (0, 4), ("absent", 4), (True, 1), (1, 1)],
    ids=["none", "false", "zero", "absent", "true", "one"],
)
def test_sender_fault_truthiness_decides_the_twins_alike(
    kind: str, value: object, calls: int, monkeypatch
) -> None:
    """FR-003 AC-2."""
    client = _Rejecting(value)
    if value == "absent":
        original = client._failed

        def without(entries: list[dict[str, Any]]) -> dict[str, Any]:
            reply = original(entries)
            for item in reply["Failed"]:
                item.pop("SenderFault")
            return reply

        client._failed = without  # type: ignore[method-assign]
    sink = _queue_sink(kind, client, monkeypatch)

    if calls == 1:
        sink.emit(list(_TWO))
    else:
        with pytest.raises(SinkDeliveryError):
            sink.emit(list(_TWO))

    assert client.calls == calls
    assert sink.failed == 2


def test_the_sqs_and_sns_sender_fault_batches_have_equal_ledgers_through_a_worker(
    monkeypatch,
) -> None:
    """FR-003 AC-4 (invariant 6): each sent once per worker attempt — here, once."""
    ledgers = {}
    for kind in ("SQSSink", "SNSSink"):
        client = _Rejecting(True)
        sink = _queue_sink(kind, client, monkeypatch)
        worker = Worker(sink, batch_size=2, max_retries=2)
        try:
            worker.submit(list(_TWO))
            delivered = bool(worker.flush(timeout=30.0))
            health = worker.health()
        finally:
            worker.stop()
        ledgers[kind] = (client.calls, delivered, health.failed_batches, sink.losses())
    assert ledgers["SQSSink"] == ledgers["SNSSink"]
    assert ledgers["SNSSink"][0] == 1, "a sender fault is sent once and the batch returns"
    assert ledgers["SNSSink"][1] is True, "the worker's flush reports the batch delivered"
