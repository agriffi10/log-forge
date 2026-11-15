"""SPEC-085's corpora: the oracles, the child runner and the gates on the model files.

The cells in ``tests/test_model_cells.py`` are judged by pure oracles and run by a watched child,
and a gate run only against the tree it guards shows that the tree passes, not that the gate
works. So each oracle here is driven over recorded data with the exact signatures it must give,
silence included, and the child runner is driven over bodies whose ending is known: one that
parks, one that raises, one that crashes, one that returns. The source gates over the model files
each carry accepted and rejected samples, in the manner of
``test_the_fork_collection_rule_can_actually_fail``, so a rule that goes quiet is seen here.

The runner tests fork; each costs one child, and the two that wait for the watchdog cost its
deadline of ``EPSILON`` plus ``HANG_AFTER`` seconds of own time.
"""

from __future__ import annotations

import ast
import functools
import json
import os
import pathlib
import re
import signal
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

import pytest

import log_foundry
import model_oracles
import model_pins
import model_sinks
import test_model_cells
from model_oracles import (
    EPSILON,
    HANG_AFTER,
    MARGIN,
    MARGIN_REDERIVE,
    WAITER_HOLD,
    WATCHDOG,
    Call,
    ChildReport,
    Closes,
    Interval,
    Ledger,
    ModelFailure,
    Wait,
    changes_overlapped,
    check_call,
    check_child,
    check_closed,
    check_end,
    check_ledger,
    check_released,
    check_repeat,
    check_wait,
    hang_signature,
    margin_line,
    overdue,
    own_time,
)
from model_pins import PINS, Cell, PinnedVerdict, cell_id, cells, judge, pin_of, scheduled, withheld
from model_sinks import ChildRun, Forker, Reentrant, Waiter, factory, install_sink, run_body
from test_model_cells import CellHooks, cell_body

TESTS = pathlib.Path(__file__).parent

MODEL_FILES = (
    "model_oracles.py",
    "model_sinks.py",
    "model_pins.py",
    "test_model_cells.py",
    "test_model_oracles.py",
)
"""Every model file that exists so far; the skip and fork gates read each of them."""

COPIED_FILES = ("model_oracles.py", "model_sinks.py", "model_pins.py", "test_model_cells.py")
"""The model files a run at an older commit imports, which the import gate holds."""


def _signatures(oracle: object, *args: object) -> frozenset[str]:
    """Runs an oracle and returns the signatures it raised, empty when it was silent.

    Args:
      oracle: The oracle.
      *args: Its arguments.

    Returns:
      The signatures.

    Raises:
      None.
    """
    try:
        oracle(*args)  # type: ignore[operator]
    except ModelFailure as failure:
        return frozenset(failure.signatures)
    return frozenset()


def _call(began: float, ended: float, *, budget: float = 0.0, thread: int = 1) -> Call:
    """Builds a call named ``c`` for a corpus row.

    Args:
      began: Its start.
      ended: Its end.
      budget: Its budget.
      thread: Its thread.

    Returns:
      The call.

    Raises:
      None.
    """
    return Call("c", thread, budget, began, ended)


# -- own time and the time oracle ----------------------------------------------------------


@pytest.mark.parametrize(
    ("intervals", "expected"),
    [
        pytest.param((), 10.0, id="no-sink-code"),
        pytest.param((Interval(2, 0.0, 10.0),), 0.0, id="another-thread-covers-all"),
        pytest.param((Interval(2, 1.0, 5.0), Interval(3, 3.0, 7.0)), 4.0, id="union-not-sum"),
        pytest.param((Interval(1, -1.0, 11.0),), 10.0, id="own-thread-made-from"),
        pytest.param((Interval(1, 2.0, 5.0),), 7.0, id="own-thread-inside"),
        pytest.param((Interval(2, -5.0, 2.0), Interval(3, 9.0, 20.0)), 7.0, id="clipped"),
    ],
)
def test_own_time_subtracts_sink_code_as_a_union_across_threads(
    intervals: tuple[Interval, ...], expected: float
) -> None:
    """A call from 0 to 10 s on thread 1, against each shape of sink code FR-003 names."""
    assert own_time(_call(0.0, 10.0), intervals) == pytest.approx(expected)


@pytest.mark.parametrize(
    ("call", "intervals", "expected"),
    [
        pytest.param(_call(0.0, 3.0, budget=3.0), (), frozenset(), id="whole-budget"),
        pytest.param(
            _call(0.0, 3.0 + EPSILON, budget=3.0), (), frozenset(), id="budget-plus-epsilon"
        ),
        pytest.param(
            _call(0.0, 4.5, budget=3.0), (), frozenset({"BoundExceeded:c"}), id="over-budget"
        ),
        pytest.param(_call(0.0, 1.5), (), frozenset({"Stalled:c"}), id="no-timeout-over"),
        pytest.param(_call(0.0, 9.0), (Interval(2, 0.0, 9.0),), frozenset(), id="overlapped-whole"),
    ],
)
def test_the_time_oracle_by_signature(
    call: Call, intervals: tuple[Interval, ...], expected: frozenset[str]
) -> None:
    """``BoundExceeded`` for a finite budget, ``Stalled`` for none, silence within ``EPSILON``."""
    assert _signatures(check_call, call, intervals) == expected


def test_the_watchdog_ends_a_call_on_its_own_time_only() -> None:
    """The watchdog's decision reads own time, never elapsed time."""
    deadline = EPSILON + HANG_AFTER
    covered = _call(0.0, 10.0 + deadline)
    assert overdue([covered], [Interval(2, 0.0, 10.0 + deadline)]) is None
    late = _call(0.0, deadline + 0.5)
    assert overdue([late], []) is late
    assert overdue([_call(0.0, deadline - 0.5)], []) is None


@pytest.mark.parametrize(
    ("wait", "expected"),
    [
        pytest.param(Wait(0.0, 1.5, 1.0), frozenset(), id="cut-within-epsilon"),
        pytest.param(Wait(0.0, 0.5, 1.0), frozenset(), id="ended-before-stop"),
        pytest.param(
            Wait(0.0, 1.0 + WAITER_HOLD, 1.0), frozenset({"WaitNotCut:wait"}), id="served"
        ),
    ],
)
def test_the_wait_oracle_by_signature(wait: Wait, expected: frozenset[str]) -> None:
    """A wait in flight at its stop ends within ``EPSILON`` of the stop's entry."""
    assert _signatures(check_wait, wait) == expected


def _close_rules(closes: Closes) -> frozenset[str]:
    """Returns the signatures of all three close rules over one record.

    Args:
      closes: The record.

    Returns:
      The signatures.

    Raises:
      None.
    """
    return (
        _signatures(check_repeat, closes)
        | _signatures(check_closed, closes)
        | _signatures(check_released, closes)
    )


@pytest.mark.parametrize(
    ("closes", "expected"),
    [
        pytest.param(Closes(2, (1, 2), (2, 2)), frozenset(), id="written-in-close-then-drained"),
        pytest.param(Closes(1, (1, 1), (1, 1)), frozenset({"CloseBroken:repeat@1"}), id="repeat"),
        pytest.param(
            Closes(3, (1, 1, 3, 3), (1, 1, 3, 3)),
            frozenset({"CloseBroken:repeat@1", "CloseBroken:repeat@3"}),
            id="two-repeats",
        ),
        pytest.param(
            Closes(2, (1,), (1,)), frozenset({"CloseBroken:unreleased@1"}), id="unreleased"
        ),
        pytest.param(
            Closes(2, (), ()), frozenset({"CloseBroken:never-closed@2"}), id="never-closed"
        ),
        pytest.param(Closes(0, (), ()), frozenset(), id="never-written"),
        pytest.param(Closes(0, (0, 0), (0, 0)), frozenset({"CloseBroken:repeat@0"}), id="twice"),
    ],
)
def test_the_close_rules_by_signature(closes: Closes, expected: frozenset[str]) -> None:
    """Repeat, closed and released, with writes counted at entry (invariant 5)."""
    assert _close_rules(closes) == expected


@pytest.mark.parametrize(
    ("calls", "expected"),
    [
        pytest.param(
            (Call("configure", 1, 5.0, 0.0, 2.0), Call("configure", 2, 5.0, 1.0, 3.0)),
            True,
            id="two-threads",
        ),
        pytest.param(
            (Call("configure", 1, 5.0, 0.0, 3.0), Call("configure", 1, 5.0, 1.0, 2.0)),
            True,
            id="nested-one-thread",
        ),
        pytest.param(
            (Call("configure", 1, 5.0, 0.0, 1.0), Call("configure", 1, 5.0, 1.0, 2.0)),
            False,
            id="back-to-back",
        ),
        pytest.param(
            (Call("configure", 1, 5.0, 0.0, 3.0), Call("flush", 2, 2.0, 1.0, 2.0)),
            False,
            id="beside-a-flush",
        ),
    ],
)
def test_two_sink_changes_overlap_only_when_one_began_before_the_other_ended(
    calls: tuple[Call, ...], expected: bool
) -> None:
    """The repeat-close exemption reads the overlap from the calls the cell timed."""
    assert changes_overlapped(calls) is expected


def _ledger(**changes: int) -> Ledger:
    """Builds a balanced ledger of one span and one orphan, with fields changed.

    Args:
      **changes: Fields to change.

    Returns:
      The ledger.

    Raises:
      None.
    """
    fields = {
        "spans": 1,
        "span_ends": 1,
        "queued": 0,
        "dropped": 0,
        "abandoned": 0,
        "orphans": 1,
        "orphans_delivered": 1,
        "orphan_lost": 0,
    }
    fields.update(changes)
    return Ledger(**fields)


@pytest.mark.parametrize(
    ("ledger", "expected"),
    [
        pytest.param(_ledger(), frozenset(), id="balanced"),
        pytest.param(_ledger(span_ends=0, abandoned=1), frozenset(), id="abandoned-term"),
        pytest.param(_ledger(queued=1), frozenset({"LedgerBroken:spans+1"}), id="spans-plus"),
        pytest.param(_ledger(span_ends=0), frozenset({"LedgerBroken:spans-1"}), id="spans-minus"),
        pytest.param(
            _ledger(orphans_delivered=0), frozenset({"LedgerBroken:orphans-1"}), id="orphans"
        ),
        pytest.param(
            _ledger(queued=2, orphan_lost=1),
            frozenset({"LedgerBroken:spans+2", "LedgerBroken:orphans+1"}),
            id="both",
        ),
    ],
)
def test_the_ledger_oracle_by_signature(ledger: Ledger, expected: frozenset[str]) -> None:
    """Both identities, each with its signed excess (invariant 2)."""
    assert _signatures(check_ledger, ledger) == expected


@pytest.mark.parametrize(
    ("state", "tokens", "expected"),
    [
        ("CLOSED", 0, frozenset()),
        ("RUNNING", 0, frozenset({"StateBroken:end"})),
        ("CLOSED", 1, frozenset({"StateBroken:end"})),
    ],
)
def test_the_end_state_oracle_by_signature(
    state: str, tokens: int, expected: frozenset[str]
) -> None:
    """The cell ends ``CLOSED`` with no swap or stop token held."""
    assert _signatures(check_end, state, tokens) == expected


def _report(**changes: object) -> ChildReport:
    """Builds a passing child report with fields changed.

    Args:
      **changes: Fields to change.

    Returns:
      The report.

    Raises:
      None.
    """
    fields: dict[str, object] = {
        "ending": "reported",
        "orphan_in": (1,),
        "span_in": (1,),
        "counters_moved": False,
        "closed_inherited": (),
        "state": "CLOSED",
    }
    fields.update(changes)
    return ChildReport(**fields)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("report", "expected"),
    [
        pytest.param(_report(), frozenset(), id="silent"),
        pytest.param(
            _report(orphan_in=(), span_in=(), counters_moved=True), frozenset(), id="counted"
        ),
        pytest.param(_report(span_in=(0,)), frozenset({"ChildBroken:two-sinks"}), id="two-sinks"),
        pytest.param(_report(span_in=()), frozenset({"ChildBroken:uncounted"}), id="uncounted"),
        pytest.param(
            _report(closed_inherited=(0,)),
            frozenset({"ChildBroken:closed-inherited"}),
            id="closed-inherited",
        ),
        pytest.param(_report(state="RUNNING"), frozenset({"ChildBroken:state"}), id="state"),
        pytest.param(_report(ending="hung"), frozenset({"ChildBroken:hung"}), id="hung"),
        pytest.param(_report(ending="crashed"), frozenset({"ChildBroken:crashed"}), id="crashed"),
        pytest.param(
            _report(span_in=(0,), state="RUNNING"),
            frozenset({"ChildBroken:two-sinks", "ChildBroken:state"}),
            id="two-checks",
        ),
    ],
)
def test_the_child_oracle_by_signature(report: ChildReport, expected: frozenset[str]) -> None:
    """A forked child's four checks, and its ending when it did not report (invariant 12)."""
    assert _signatures(check_child, report) == expected


def test_no_oracle_fails_a_duration_for_being_short() -> None:
    """FR-003: no assertion in the model files is a lower bound on a duration.

    ``tests/test_timing_floors.py`` cannot see a subtraction of two recorded instants, so its
    silence over the model files does not show this. A call, a wait and an in-flight call of
    zero length are silent under each oracle that reads a duration.
    """
    assert _signatures(check_call, _call(5.0, 5.0, budget=3.0), ()) == frozenset()
    assert _signatures(check_call, _call(5.0, 5.0), ()) == frozenset()
    assert _signatures(check_wait, Wait(5.0, 5.0, 5.0)) == frozenset()
    assert overdue([_call(5.0, 5.0)], []) is None


def test_the_table_of_failure_classes_is_these_eleven() -> None:
    """FR-002's table, by name, and each signature is ``Class:key``."""
    names = {cls.__name__ for cls in ModelFailure.__subclasses__()}
    assert names == {
        "HangFound",
        "ChildCrashed",
        "NotExercised",
        "CallRaised",
        "BoundExceeded",
        "Stalled",
        "WaitNotCut",
        "CloseBroken",
        "LedgerBroken",
        "StateBroken",
        "ChildBroken",
    }
    for cls in ModelFailure.__subclasses__():
        assert cls("k1", "k2").signatures == (f"{cls.__name__}:k1", f"{cls.__name__}:k2")


_DUMP = (
    "\n"
    + WATCHDOG
    + '{"call": "flush-none", "thread": "0x2", '
    + '"threads": {"0x1": "MainThread", "0x2": "log-foundry-worker", "0x3": "helper"}}\n'
    + "Thread 0x0000000000000002 (most recent call first):\n"
    + '  File "/pkg/log_foundry/sinks/_factory.py", line 9 in build\n'
    + '  File "/pkg/log_foundry/worker.py", line 10 in flush\n'
    + '  File "/elsewhere/tests/model_sinks.py", line 3 in call\n'
    + "\n"
    + "Thread 0x0000000000000003 (most recent call first):\n"
    + '  File "/elsewhere/tests/model_sinks.py", line 4 in park\n'
    + "\n"
    + "Current thread 0x0000000000000001 (most recent call first):\n"
    + '  File "/pkg/log_foundry/_lifecycle.py", line 1 in _swap_sink\n'
)


def test_the_hang_signature_names_the_innermost_library_frame() -> None:
    """The verdict names the hung thread's innermost frame under the package, or ``@none``."""
    signature, stacks = hang_signature(_DUMP, "/pkg/log_foundry")
    assert signature == "HangFound:flush-none@sinks._factory.build"
    assert stacks["log-foundry-worker"] == ("sinks._factory.build", "worker.flush")
    assert stacks["helper"] == ()
    assert stacks["MainThread"] == ("_lifecycle._swap_sink",)
    parked = _DUMP.replace('"thread": "0x2"', '"thread": "0x3"')
    assert hang_signature(parked, "/pkg/log_foundry")[0] == "HangFound:flush-none@none"
    assert not model_oracles.has_watchdog_line("!ValueError: boom " + _DUMP.strip())


def test_the_margin_line_reads_passing_cells_only() -> None:
    """The worst margin and its id are named; other phases and unmarked reports are ignored."""

    def report(nodeid: str, when: str, *props: tuple[str, object]) -> SimpleNamespace:
        return SimpleNamespace(nodeid=nodeid, when=when, user_properties=list(props))

    reports = [
        report("a", "call", (MARGIN, "0.010")),
        report("b", "call", (MARGIN, "0.250"), ("other", "x")),
        report("c", "setup", (MARGIN, "0.900")),
        report("d", "call"),
    ]
    line = margin_line(reports)
    assert line is not None
    assert line.startswith("model cells:")
    assert " 0.250 s at b " in line
    assert "within" in line
    assert margin_line([report("d", "call")]) is None
    above = margin_line([report("e", "call", (MARGIN, str(MARGIN_REDERIVE + 0.1)))])
    assert above is not None
    assert "ABOVE" in above


# -- the child runner -----------------------------------------------------------------------


def _parks(run: ChildRun) -> None:
    """Parks on a held lock inside a call given no timeout."""
    lock = threading.Lock()
    lock.acquire()
    run.call("parked", 0.0, lock.acquire)


def _raises(run: ChildRun) -> None:
    """Raises out of the body, outside any call."""
    del run
    raise ValueError("out of the cell's own code")


def _segfaults(run: ChildRun) -> None:
    """Ends the child with ``SIGSEGV``."""
    del run
    os.kill(os.getpid(), signal.SIGSEGV)


def _returns(run: ChildRun) -> None:
    """Returns at once."""
    del run


def _never_reaches(run: ChildRun) -> None:
    """Declares a site and never visits it."""
    run.expect_site()


def _call_raises(run: ChildRun) -> None:
    """Makes a call whose work raises."""

    def boom() -> None:
        raise RuntimeError("inside the call")

    run.call("boom", 0.0, boom)


def _forked_child_parks(run: ChildRun) -> None:
    """Forks a child that parks inside a call."""

    def child(inner: ChildRun) -> dict[str, object]:
        _parks(inner)
        return {}

    run.fork(child)


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        pytest.param(_parks, frozenset({"HangFound:parked@none"}), id="parks"),
        pytest.param(_raises, frozenset({"ChildCrashed:raised-ValueError"}), id="raises"),
        pytest.param(_segfaults, frozenset({"ChildCrashed:SIGSEGV"}), id="segfaults"),
        pytest.param(_returns, frozenset(), id="returns"),
        pytest.param(_never_reaches, frozenset({"NotExercised:site"}), id="never-reached"),
        pytest.param(_call_raises, frozenset({"CallRaised:boom"}), id="call-raises"),
        pytest.param(_forked_child_parks, frozenset({"ChildBroken:hung"}), id="child-parks"),
    ],
)
def test_the_child_runner_reports_each_ending_by_signature(
    body: object, expected: frozenset[str]
) -> None:
    """FR-002's runner corpus: each body's whole verdict, and a hang's stacks in its text."""
    outcome = run_body(body)  # type: ignore[arg-type]
    assert outcome.verdict == expected, outcome.text
    if body is _parks:
        assert outcome.text.count("Thread 0x") + outcome.text.count("thread 0x") >= 2
        assert "model-watchdog" in outcome.text
    if body is _returns:
        assert outcome.file is not None
        assert outcome.margin is None


def test_a_child_ended_by_its_alarm_with_no_watchdog_line_is_a_silent_hang() -> None:
    """A child that dies of its alarm with nothing in flight reads ``HangFound:silent``."""

    def sleeps(run: ChildRun) -> None:
        del run
        time.sleep(30)

    assert run_body(sleeps, timeout=1).verdict == frozenset({"HangFound:silent"})


def test_every_oracle_runs_whatever_the_others_found() -> None:
    """A raising call, an unreached site and a failing oracle all reach the verdict."""

    def three(run: ChildRun) -> None:
        run.expect_site()
        _call_raises(run)
        run.check(check_end, "RUNNING", 0)

    outcome = run_body(three)
    assert outcome.verdict == frozenset(
        {"CallRaised:boom", "NotExercised:site", "StateBroken:end"}
    ), outcome.text


def test_a_call_waiting_on_sink_code_elsewhere_is_judged_on_its_own_time() -> None:
    """A call given no timeout that waits two seconds on another thread's sink code is silent."""

    def waits(run: ChildRun) -> None:
        entered = threading.Event()

        def sink_code() -> None:
            with run.sink_code():
                entered.set()
                time.sleep(2.0)

        helper = threading.Thread(target=sink_code)
        helper.start()
        entered.wait(5.0)
        run.call("waits", 0.0, helper.join)

    outcome = run_body(waits)
    assert outcome.verdict == frozenset(), outcome.text


# -- the source gates -----------------------------------------------------------------------

_SKIPS = frozenset({"skip", "skipif", "importorskip"})


def _named(node: ast.AST) -> str | None:
    """Returns the name an identifier node spells, or ``None``.

    Args:
      node: Any node.

    Returns:
      A ``Name``'s id, an ``Attribute``'s attribute, or an imported alias's last part.

    Raises:
      None.
    """
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.alias):
        return node.asname or node.name.rsplit(".", 1)[-1]
    return None


def skip_offenders(source: str) -> list[str]:
    """Returns every skip and every ``xfail`` that is not strict with a ``raises=`` in a source.

    Identifiers are read, never text, so the words in a string or a docstring are not sites. An
    ``xfail`` whose ``run=`` is anything but a literal ``True`` is a skip in practice, since it
    may not run the test, so it offends however strict it is, and so does one spreading
    ``**`` keywords, which could carry such a ``run=``.

    Args:
      source: A module's text.

    Returns:
      One entry per offending identifier.

    Raises:
      SyntaxError: If the source does not parse.
    """
    tree = ast.parse(source)
    strict: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and _named(node.func) == "xfail":
            keywords = {keyword.arg: keyword.value for keyword in node.keywords}
            flag = keywords.get("strict")
            runs = keywords.get("run", ast.Constant(True))
            if (
                isinstance(flag, ast.Constant)
                and flag.value is True
                and "raises" in keywords
                and None not in keywords
                and isinstance(runs, ast.Constant)
                and runs.value is True
            ):
                strict.add(id(node.func))
    offenders: list[str] = []
    for node in ast.walk(tree):
        name = _named(node)
        if name in _SKIPS:
            offenders.append(f"{name} at line {getattr(node, 'lineno', 0)}")
        elif name == "xfail" and id(node) not in strict:
            offenders.append(f"a non-strict xfail at line {getattr(node, 'lineno', 0)}")
    return offenders


def fork_offenders(source: str) -> list[str]:
    """Returns every fork in a source that does not go through ``run_in_child``.

    Args:
      source: A module's text.

    Returns:
      One entry per call to ``os.fork`` or ``os.forkpty``, import of either, and import of
      ``multiprocessing``.

    Raises:
      SyntaxError: If the source does not parse.
    """
    offenders: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Attribute) and node.attr in {"fork", "forkpty"}:
            if isinstance(node.value, ast.Name) and node.value.id == "os":
                offenders.append(f"os.{node.attr} at line {node.lineno}")
        elif isinstance(node, ast.ImportFrom) and node.module == "os":
            if any(alias.name in {"fork", "forkpty"} for alias in node.names):
                offenders.append(f"a fork imported at line {node.lineno}")
        elif isinstance(node, ast.Import | ast.ImportFrom):
            modules = (
                [alias.name for alias in node.names]
                if isinstance(node, ast.Import)
                else [node.module or ""]
            )
            if any(module.split(".")[0] == "multiprocessing" for module in modules):
                offenders.append(f"multiprocessing at line {node.lineno}")
    return offenders


_RUNNER = "test_fork_lifecycle"
_RUNNER_NAMES = frozenset({"run_in_child", "_child_pipe"})
_MODEL_MODULES = frozenset({"model_oracles", "model_sinks", "model_pins"})


def import_offenders(source: str, local: frozenset[str]) -> list[str]:
    """Returns every import from ``tests/`` other than the child runner and the model files.

    Args:
      source: A module's text.
      local: The module names ``tests/`` holds.

    Returns:
      One entry per module imported from ``tests/`` other than the runner and the model
      helpers, and per name taken from the runner other than ``run_in_child`` and
      ``_child_pipe``, by ``from`` import or by attribute.

    Raises:
      SyntaxError: If the source does not parse.
    """
    offenders: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            for alias in node.names:
                top = alias.name.split(".")[0]
                if top in local and top not in _MODEL_MODULES | {_RUNNER}:
                    offenders.append(f"import {alias.name} at line {node.lineno}")
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            top = node.module.split(".")[0]
            if top == _RUNNER:
                taken = {alias.name for alias in node.names} - _RUNNER_NAMES
                offenders += [f"{_RUNNER}.{name} at line {node.lineno}" for name in sorted(taken)]
            elif top in local and top not in _MODEL_MODULES:
                offenders.append(f"from {node.module} at line {node.lineno}")
        elif (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id == _RUNNER
            and node.attr not in _RUNNER_NAMES
        ):
            offenders.append(f"{_RUNNER}.{node.attr} at line {node.lineno}")
    return offenders


def _local_modules() -> frozenset[str]:
    """Returns the module names ``tests/`` holds.

    Args:
      None.

    Returns:
      The stems of ``tests/*.py``.

    Raises:
      None.
    """
    return frozenset(path.stem for path in TESTS.glob("*.py"))


def _model_sources(names: tuple[str, ...]) -> dict[str, str]:
    """Reads the named model files, failing when one is missing so the roster cannot shrink.

    Args:
      names: File names under ``tests/``.

    Returns:
      Each file's text by name.

    Raises:
      None.
    """
    missing = [name for name in names if not (TESTS / name).is_file()]
    assert not missing, f"a model file is missing: {missing}"
    return {name: (TESTS / name).read_text(encoding="utf-8") for name in names}


def test_the_model_files_hold_no_skip_and_no_xfail_that_is_not_strict() -> None:
    """FR-004: no ``skip``, ``skipif``, ``importorskip`` or non-strict ``xfail`` in the files."""
    offenders = {name: skip_offenders(text) for name, text in _model_sources(MODEL_FILES).items()}
    assert not any(offenders.values()), offenders


@pytest.mark.parametrize(
    ("source", "refused"),
    [
        ("pytest.mark.xfail(strict=True, raises=E, reason='r')", False),
        ("'skip and xfail in a string'", False),
        ("pytest.mark.skip(reason='r')", True),
        ("pytest.skip('r')", True),
        ("pytest.mark.skipif(True, reason='r')", True),
        ("pytest.importorskip('x')", True),
        ("from pytest import skip", True),
        ("pytest.mark.xfail", True),
        ("pytest.mark.xfail(strict=False, raises=E)", True),
        ("pytest.mark.xfail(strict=True)", True),
        ("pytest.mark.xfail(strict=True, raises=E, run=True)", False),
        ("pytest.mark.xfail(strict=True, raises=E, run=False)", True),
        ("pytest.mark.xfail(strict=True, raises=E, run=flag)", True),
        ("pytest.mark.xfail(strict=True, raises=E, **options)", True),
        ("pytest.xfail('r')", True),
    ],
)
def test_the_skip_rule_can_actually_fail(source: str, refused: bool) -> None:
    """The skip rule's accepted and rejected samples."""
    assert bool(skip_offenders(source)) is refused


def test_every_fork_in_the_model_files_goes_through_run_in_child() -> None:
    """FR-002: no model file forks except through ``run_in_child``.

    The suite's fork lint globs ``test_*.py``, so it never reads a ``model_*.py`` helper; this
    reads them all. A subprocess that starts a new interpreter is not a fork of the test process.
    """
    offenders = {name: fork_offenders(text) for name, text in _model_sources(MODEL_FILES).items()}
    assert not any(offenders.values()), offenders


@pytest.mark.parametrize(
    ("source", "refused"),
    [
        ("run_in_child(work, timeout=60)", False),
        ("'os.fork() in a string'", False),
        ("os.fork()", True),
        ("os.forkpty()", True),
        ("from os import fork", True),
        ("import multiprocessing", True),
        ("from multiprocessing import Process", True),
    ],
)
def test_the_fork_rule_can_actually_fail(source: str, refused: bool) -> None:
    """The fork rule's accepted and rejected samples."""
    assert bool(fork_offenders(source)) is refused


def test_the_model_files_import_from_tests_only_the_child_runner() -> None:
    """FR-005: the files a run at an older commit imports take only the runner from ``tests/``.

    Scoped to those files, so a later spec's additions to this corpus are not refused.
    """
    local = _local_modules()
    offenders = {
        name: import_offenders(text, local) for name, text in _model_sources(COPIED_FILES).items()
    }
    assert not any(offenders.values()), offenders


@pytest.mark.parametrize(
    ("source", "refused"),
    [
        ("import test_fork_lifecycle\ntest_fork_lifecycle._child_pipe", False),
        ("from test_fork_lifecycle import run_in_child", False),
        ("import model_oracles\nimport log_foundry", False),
        ("from test_fork_lifecycle import _Child", True),
        ("import test_fork_lifecycle\ntest_fork_lifecycle.CHILD_TIMEOUT", True),
        ("from conftest import install_worker", True),
        ("import test_invariants_model", True),
    ],
)
def test_the_import_rule_can_actually_fail(source: str, refused: bool) -> None:
    """The import rule's accepted and rejected samples."""
    local = frozenset({"conftest", "test_fork_lifecycle", "test_invariants_model", *_MODEL_MODULES})
    assert bool(import_offenders(source, local)) is refused


def test_a_cell_runs_in_a_child_with_the_cell_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """FR-002: a cell's child is made by ``model_sinks.run_in_child`` with the literal 60."""
    seen: list[int] = []

    def spy(work: object, *, timeout: int) -> SimpleNamespace:
        del work
        seen.append(timeout)
        return SimpleNamespace(finished=False, blocked=True, status=0, output="")

    monkeypatch.setattr(model_sinks, "run_in_child", spy)
    outcome = test_model_cells.run_cell(Cell("reentrant", "emit", "orphan", ("info",)))
    assert outcome.verdict == frozenset({"HangFound:silent"})
    assert seen == [60]


# -- the personalities --------------------------------------------------------------------


class _Asked:
    """Hooks that record every visit they are asked about and answer as a rule says.

    Attributes:
      asked: ``(site, writes at the ask, lock held at the ask, inside a script at the ask)``.
      supplied: Every sink ``new_sink`` returned.
    """

    def __init__(self, answer: object = None) -> None:
        """Builds the hooks over a rule ``(site, nth visit to it) -> script``."""
        self.answer = answer
        self.asked: list[tuple[str, int, bool, bool]] = []
        self.supplied: list[object] = []

    def script(self, sink: object, site: str) -> tuple[str, ...] | None:
        """Records the visit, then answers by the rule."""
        lock = getattr(sink, "_lock", None)
        held = bool(lock is not None and lock.locked())
        writes = int(getattr(sink, "writes", -1))
        self.asked.append((site, writes, held, model_sinks.current().in_script()))
        nth = sum(1 for asked in self.asked if asked[0] == site)
        return self.answer(site, nth) if callable(self.answer) else None

    def new_sink(self) -> object:
        """Supplies a quiet ``reentrant`` sink and records it."""
        sink = Reentrant(_Asked())
        self.supplied.append(sink)
        return sink


def _orphan(run: ChildRun) -> None:
    """Logs once outside a span, through the run's timer."""
    run.count("info")
    run.call("info", 0.0, lambda: log_foundry.info("orphan"))


def test_a_reentrant_sink_asks_its_hooks_at_every_visit() -> None:
    """FR-001: hooks answering at the second ``emit`` visit run the script then, not at the first.

    Two ``info()`` calls make two visits; the script's own ``info`` makes a third write, whose
    visit is inside the script and so is not asked.
    """

    def body(run: ChildRun) -> None:
        hooks = _Asked(lambda site, nth: ("info",) if site == "emit" and nth == 2 else None)
        sink = Reentrant(hooks)
        run.call("configure", 5.0, lambda: install_sink(sink))
        _orphan(run)
        _orphan(run)
        run.prop(
            "emits", json.dumps([writes for site, writes, _, _ in hooks.asked if site == "emit"])
        )
        run.prop("writes", sink.writes)

    outcome = run_body(body)
    assert outcome.verdict == frozenset(), outcome.text
    assert json.loads(outcome.properties["emits"]) == [1, 2]
    assert outcome.properties["writes"] == "3"


def test_a_scripts_configure_installs_the_sink_its_hooks_supply() -> None:
    """FR-001: a ``configure+configure`` script asks ``new_sink`` twice and installs the second."""

    def body(run: ChildRun) -> None:
        hooks = _Asked(lambda site, nth: ("configure", "configure") if site == "emit" else None)
        sink = Reentrant(hooks)
        run.call("configure", 5.0, lambda: install_sink(sink))
        _orphan(run)
        _orphan(run)
        run.prop("supplied", len(hooks.supplied))
        run.prop("last", json.dumps([s.count("orphan") for s in hooks.supplied]))  # type: ignore[attr-defined]

    outcome = run_body(body)
    assert outcome.verdict == frozenset(), outcome.text
    assert outcome.properties["supplied"] == "2"
    assert json.loads(outcome.properties["last"]) == [0, 1]


def test_a_factory_asks_its_hooks_at_every_build() -> None:
    """FR-001: a ``factory``'s callable asks its hooks each time the library builds a sink."""

    def body(run: ChildRun) -> None:
        hooks = _Asked()
        build = factory(hooks)
        for _ in range(2):
            run.call(
                "configure",
                5.0,
                lambda: log_foundry.configure(service="m", sink_factory=lambda: build()),
            )
            _orphan(run)
        run.prop("builds", sum(1 for site, *_ in hooks.asked if site == "factory"))

    outcome = run_body(body)
    assert outcome.verdict == frozenset(), outcome.text
    assert outcome.properties["builds"] == "2"


def test_a_thread_inside_a_script_starts_no_second_script() -> None:
    """FR-001: a visit by a thread already inside a script does not call the ``script`` hook.

    Hooks that answer at every visit are asked once for the first ``emit``; the script's own
    ``info`` writes again from inside the script, and that visit returns without asking, so no
    answer is drawn and discarded during a script.
    """

    def body(run: ChildRun) -> None:
        hooks = _Asked(lambda site, nth: ("info",) if site == "emit" else None)
        sink = Reentrant(hooks)
        run.call("configure", 5.0, lambda: install_sink(sink))
        _orphan(run)
        run.prop("emits", sum(1 for site, *_ in hooks.asked if site == "emit"))
        run.prop("inside", sum(1 for *_, inside in hooks.asked if inside))
        run.prop("writes", sink.writes)

    outcome = run_body(body)
    assert outcome.verdict == frozenset(), outcome.text
    assert outcome.properties["emits"] == "1"
    assert outcome.properties["inside"] == "0"
    assert outcome.properties["writes"] == "2"


def test_no_personality_holds_its_lock_where_its_code_runs() -> None:
    """FR-001: at every site a script can run from, the personality's own lock is free."""

    def body(run: ChildRun) -> None:
        hooks = _Asked()
        sink = Reentrant(hooks)
        run.call("configure", 5.0, lambda: install_sink(sink))
        _orphan(run)
        run.call("shutdown", 3.0, lambda: log_foundry.shutdown(timeout=3.0))
        build = factory(hooks)
        run.call("configure", 5.0, lambda: log_foundry.configure(service="m", sink_factory=build))
        _orphan(run)
        run.prop("sites", json.dumps(sorted({site for site, *_ in hooks.asked})))
        run.prop("held", sum(1 for _, _, held, _ in hooks.asked if held))

    outcome = run_body(body)
    assert outcome.verdict == frozenset(), outcome.text
    assert set(json.loads(outcome.properties["sites"])) >= {"setter", "emit", "close", "factory"}
    assert outcome.properties["held"] == "0"


def test_a_cell_whose_site_is_never_reached_is_not_exercised() -> None:
    """FR-001: a cell cannot pass by doing nothing; an unreached site is ``NotExercised:site``."""
    cell = Cell("reentrant", "nowhere", "orphan", ("info",))
    outcome = run_body(functools.partial(cell_body, cell=cell))
    assert outcome.verdict == frozenset({"NotExercised:site"}), outcome.text


def test_a_swap_emit_gate_that_runs_out_is_not_exercised() -> None:
    """FR-001: a ``swap-emit`` gate with no sink change to open a token is ``NotExercised:gate``."""

    def body(run: ChildRun) -> None:
        hooks = CellHooks(Cell("reentrant", "swap-emit", "worker", ("info",)), gate=0.2)
        hooks.first = Reentrant(hooks)
        run.call("configure", 5.0, lambda: install_sink(hooks.first))
        run.count("span")
        run.call("span", 0.0, model_sinks._model_span)
        run.call("reach-flush", 10.0, lambda: log_foundry.flush(timeout=10.0))
        run.call("shutdown", 5.0, lambda: log_foundry.shutdown(timeout=5.0))

    outcome = run_body(body)
    assert outcome.verdict == frozenset({"NotExercised:gate"}), outcome.text


@pytest.mark.parametrize("workers", [["-n", "0"], []], ids=["serial", "distributed"])
def test_the_session_prints_the_worst_margin_line(workers: list[str]) -> None:
    """FR-003: one line names the worst margin over passing cells, under ``-n 0`` and ``-n 12``.

    A nested session over one passing cell and one pinned cell, from the repo root with this
    interpreter. The pinned cell's margin is not in the line. The nested session is a new
    interpreter started with ``subprocess``, not a fork of this process.
    """
    passing = "tests/test_model_cells.py::test_cell[reentrant-emit-orphan-info]"
    pinned = "tests/test_model_cells.py::test_cell[reentrant-close-orphan-shutdown-none]"
    env = {name: value for name, value in os.environ.items() if not name.startswith("PYTEST_")}
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "-q", *workers, passing, pinned],
        cwd=TESTS.parent,
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    lines = [line for line in result.stdout.splitlines() if line.startswith("model cells:")]
    assert result.returncode == 0, result.stdout[-4000:] + result.stderr[-2000:]
    assert "1 passed, 1 xfailed" in result.stdout, result.stdout[-2000:]
    assert len(lines) == 1, result.stdout[-4000:]
    assert "reentrant-emit-orphan-info]" in lines[0]
    assert "shutdown-none" not in lines[0]


# -- the table and its pins ---------------------------------------------------------------


def test_the_table_and_the_pins_hold_these_counts() -> None:
    """FR-001 and FR-004: the generated table's size and the pins' arithmetic, as literals."""
    table = cells()
    by = {
        name: sum(1 for cell in table if cell.personality == name)
        for name in ("reentrant", "factory", "forker", "waiter")
    }
    assert by == {"reentrant": 504, "factory": 112, "forker": 9, "waiter": 12}
    assert len(table) == 637
    assert [pin.name for pin in PINS] == [
        "drain-flush",
        "nested-shutdown",
        "empty-submission",
        "orphan-arm-in-close",
        "installed-closed-twice",
        "fork-mid-swap",
        "orphan-write-after-close",
    ]
    pinned = [cell for cell in table if pin_of(cell) is not None]
    held = [cell for cell in table if withheld(cell)]
    assert len(pinned) == 20
    assert len(held) == 168
    assert len(table) - len(pinned) - len(held) == 449
    assert len(scheduled()) == 469


def test_every_cell_id_is_unique_and_spells_its_cell() -> None:
    """FR-001: one id per cell, ``personality-site-path-script`` with ``+`` between operations."""
    ids = [cell_id(cell) for cell in cells()]
    assert len(set(ids)) == len(ids)
    assert "reentrant-emit-worker-flush+shutdown" in ids
    assert "factory-factory-orphan-span+flush-none" in ids


def test_every_pinned_cell_is_in_the_table_and_under_one_pin() -> None:
    """FR-004: a pin names only cells of the table, and no cell has two pins."""
    table = set(cells())
    named = [cell for pin in PINS for cell in pin.cells]
    assert set(named) <= table
    assert len(named) == len(set(named))


def open_item_leads() -> list[str]:
    """Returns the bold lead of every bullet between ``### Open`` and ``### Resolved`` in §12.

    A lead wraps across lines there, so it is read across the wrap, and whitespace collapsed.

    Returns:
      The leads, in order.
    """
    text = (TESTS.parent / "docs" / "architecture.md").read_text(encoding="utf-8")
    section = text.split("\n### Open\n", 1)[1].split("\n### Resolved\n", 1)[0]
    leads: list[str] = []
    for bullet in re.split(r"\n(?=- )", "\n" + section):
        if bullet.startswith("- **"):
            lead = bullet[len("- **") :].split("**", 1)[0]
            leads.append(" ".join(lead.split()))
    return leads


def test_every_pin_names_an_open_item() -> None:
    """FR-004: each pin's item is the bold lead of an open bullet of ``docs/architecture.md`` §12."""
    leads = open_item_leads()
    assert len(leads) > 20, leads
    missing = [pin.name for pin in PINS if " ".join(pin.item.split()) not in leads]
    assert not missing, missing


def test_with_the_pins_emptied_nothing_is_withheld_and_every_cell_is_scheduled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-004: the withheld set is computed from ``PINS`` alone."""
    monkeypatch.setattr(model_pins, "PINS", ())
    assert not any(withheld(cell) for cell in cells())
    assert scheduled() == cells()


def test_everything_withheld_repeats_a_pinned_operation_or_its_sibling() -> None:
    """FR-004: a withheld cell shares its place with a one-operation pinned cell and its operation."""
    for cell in cells():
        if not withheld(cell):
            continue
        assert pin_of(cell) is None
        place = (cell.personality, cell.site, cell.path)
        reasons = [
            pin
            for pin in PINS
            for held in pin.cells
            if len(held.script) == 1
            and (held.personality, held.site, held.path) == place
            and len(cell.script) > 1
            and (held.script[0] in cell.script or pin.sibling in cell.script)
        ]
        assert reasons, cell_id(cell)


def test_a_pins_one_operation_sibling_script_is_scheduled() -> None:
    """FR-004: the bare sibling runs, as the cell whose finite timeout spends its budget waiting."""
    expected = {
        Cell(held.personality, held.site, held.path, (pin.sibling,))
        for pin in PINS
        if pin.sibling is not None
        for held in pin.cells
        if len(held.script) == 1
    }
    assert {cell_id(cell) for cell in expected} == {
        "reentrant-emit-worker-flush",
        "reentrant-swap-emit-worker-flush",
        "factory-factory-worker-flush",
        "reentrant-close-orphan-shutdown",
        "reentrant-close-worker-shutdown",
        "reentrant-swap-close-orphan-shutdown",
        "reentrant-swap-close-worker-shutdown",
    }
    assert not [cell_id(cell) for cell in expected if withheld(cell)]
    assert expected <= set(scheduled())


def test_no_pin_expects_a_crash() -> None:
    """FR-002: a crash is never an expected failure."""
    assert not [pin.name for pin in PINS if any(s.startswith("ChildCrashed:") for s in pin.verdict)]


def test_a_pinned_cell_is_an_expected_failure_only_at_its_pins_verdict() -> None:
    """FR-004: the pure decision behind the strict mark."""
    pin = PINS[0]
    with pytest.raises(PinnedVerdict):
        judge(pin, pin.verdict, "")
    for other in (frozenset(), frozenset({"Stalled:info"}), pin.verdict | {"Stalled:info"}):
        with pytest.raises(AssertionError) as caught:
            judge(pin, other, "")
        assert not isinstance(caught.value, PinnedVerdict)
    judge(None, frozenset(), "")
    with pytest.raises(AssertionError):
        judge(None, frozenset({"Stalled:info"}), "")
    crashed = model_pins.Pin("p", (), frozenset({"ChildCrashed:SIGSEGV"}), "item")
    with pytest.raises(AssertionError) as caught:
        judge(crashed, crashed.verdict, "")
    assert not isinstance(caught.value, PinnedVerdict)


def test_every_scheduled_pinned_cell_carries_the_strict_mark() -> None:
    """FR-004: each pinned cell's parameter carries one strict ``xfail`` expecting the pin's raise."""
    for param in test_model_cells._params():
        (cell,) = param.values  # type: ignore[attr-defined]
        marks = list(param.marks)  # type: ignore[attr-defined]
        if pin_of(cell) is None:
            assert not marks, cell_id(cell)
            continue
        assert len(marks) == 1, cell_id(cell)
        assert marks[0].name == "xfail"
        assert marks[0].kwargs["strict"] is True
        assert marks[0].kwargs["raises"] is PinnedVerdict
        assert "run" not in marks[0].kwargs, cell_id(cell)


def test_every_reproduction_is_a_test_function_of_the_cells_file() -> None:
    """FR-004: a pin held by a reproduction names a strict test function of the cells file."""
    named = [pin for pin in PINS if pin.reproduction is not None]
    assert named
    for pin in named:
        assert not pin.cells, pin.name
        function = getattr(test_model_cells, str(pin.reproduction), None)
        assert callable(function), pin.reproduction
        assert str(pin.reproduction).startswith("test_")
        marks = [mark for mark in getattr(function, "pytestmark", []) if mark.name == "xfail"]
        assert len(marks) == 1, pin.reproduction
        assert marks[0].kwargs["strict"] is True
        assert marks[0].kwargs["raises"] is PinnedVerdict
        assert "run" not in marks[0].kwargs, pin.reproduction


def test_a_waiting_sink_holds_no_lock_of_its_own_while_it_waits() -> None:
    """FR-001: another thread reads a ``waiter``'s lock as free while its wait is in flight."""

    def body(run: ChildRun) -> None:
        sink = Waiter(WAITER_HOLD)
        run.call("configure", 5.0, lambda: install_sink(sink))
        _orphan(run)
        sink.armed = "emit"
        helper = threading.Thread(target=_orphan, args=(run,))
        helper.start()
        sink.entered.wait(5.0)
        run.prop("held", sink._lock.locked())
        run.call("shutdown", 3.0, lambda: log_foundry.shutdown(timeout=3.0))
        helper.join()

    outcome = run_body(body)
    assert outcome.verdict == frozenset(), outcome.text
    assert outcome.properties["held"] == "False"


def test_a_forker_forks_once_where_its_hooks_say_with_its_lock_free() -> None:
    """FR-001: a ``forker`` forks at the visit its hooks answer, not before, with its lock free.

    The child writes to the sink it inherited, which it could not do were the lock held at the
    fork, and its report passes the child oracle.
    """

    def body(run: ChildRun) -> None:
        hooks = _Asked(lambda site, nth: ("fork",) if site == "emit" and nth == 2 else None)
        sink = Forker(hooks)
        run.call("configure", 5.0, lambda: install_sink(sink))
        _orphan(run)
        _orphan(run)
        run.prop("forks", sink.forks)
        run.prop("held", sum(1 for _, _, held, _ in hooks.asked if held))

    outcome = run_body(body)
    assert outcome.verdict == frozenset(), outcome.text
    assert outcome.properties["forks"] == "1"
    assert outcome.properties["held"] == "0"
    assert "orphan_in" in outcome.text


def test_a_waiter_waits_on_the_event_it_holds_when_the_wait_begins() -> None:
    """FR-001: a signal assigned after the wait began does not cut it; the one it held does.

    Each event notes every wait made on it and what that wait returned, so which event the wait
    was on, and that setting it is what ended the wait, are read without timing anything.
    """

    class Noted(threading.Event):
        """An event that notes its name and the result of each wait made on it."""

        def __init__(self, name: str, noted: list[str]) -> None:
            """Builds an unset event that notes into a shared list."""
            super().__init__()
            self.name = name
            self.noted = noted

        def wait(self, timeout: float | None = None) -> bool:
            """Waits as an event does, then notes which event it was and what the wait returned."""
            result = super().wait(timeout)
            self.noted.append(f"{self.name}:{result}")
            return result

    def body(run: ChildRun) -> None:
        sink = Waiter(WAITER_HOLD)
        noted: list[str] = []
        held, later = Noted("held", noted), Noted("later", noted)
        sink.log_foundry_stop_signal = held
        sink.armed = "emit"
        helper = threading.Thread(target=sink.emit, args=([],))
        helper.start()
        sink.entered.wait(5.0)
        sink.log_foundry_stop_signal = later
        later.set()
        held.set()
        helper.join()
        run.prop("waited", " ".join(noted))
        run.prop("waits", len(sink.waits))

    outcome = run_body(body)
    assert outcome.verdict == frozenset(), outcome.text
    assert outcome.properties["waited"] == "held:True"
    assert outcome.properties["waits"] == "1"
