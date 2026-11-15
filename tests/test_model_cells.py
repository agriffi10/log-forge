"""SPEC-085's cells: each scheduled cell of the table, run once in a watched child and judged.

Every cell follows one procedure in its child (FR-001): install the personality's sink with
``configure``; reach it on the path; make the site's extra call, if it has one; call
``shutdown(timeout=5.0)``; apply the close, ledger and end-state oracles; run the exit drain,
which is what ``atexit`` calls; and apply the close oracles again. The time oracle is applied to
every call the cell and its scripts made, once nothing is in flight.

A cell that a pin names is an expected failure only at its pin's exact verdict, through a strict
``xfail`` that expects ``PinnedVerdict``: a pass, or any other verdict, fails the suite. A cell
that a pin withholds is not collected at all; it is the pinned defect again.

**A run that imported another tree's ``log_foundry`` is void.** This repo has been caught by an
editable install resolving a copied tree back to the original, with every mutant passing, so each
cell fails when the library it imported, in this process or in the child, is not under the
``src/`` beside this file's ``tests/``. That is what lets a run at an older commit, from a
``git archive`` export, be trusted.
"""

from __future__ import annotations

import functools
import json
import pathlib
import threading
import time
from typing import TYPE_CHECKING

import pytest

import log_foundry
import model_pins
import model_sinks
from log_foundry import _lifecycle
from model_oracles import (
    GATE_TIMEOUT,
    MARGIN,
    changes_overlapped,
    check_closed,
    check_end,
    check_ledger,
    check_released,
    check_repeat,
)
from model_pins import Cell, PinnedVerdict, cell_id, scheduled, settle
from model_sinks import (
    FINAL_SHUTDOWN_TIMEOUT,
    ChildRun,
    Outcome,
    Recorder,
    Reentrant,
    current,
    factory,
    install_built,
    install_sink,
    reach,
    read_end,
    read_ledger,
    run_body,
)

if TYPE_CHECKING:
    from collections.abc import Callable

_HERE = pathlib.Path(__file__).resolve().parent
_SRC = _HERE.parent / "src"

assert pathlib.Path(model_pins.__file__).resolve().parent == _HERE, model_pins.__file__
assert pathlib.Path(model_sinks.__file__).resolve().parent == _HERE, model_sinks.__file__


class _Quiet:
    """Hooks that never answer: the sink a script's ``configure`` installs runs no script."""

    def script(self, sink: object, site: str) -> tuple[str, ...] | None:
        """Answers nothing."""
        del sink, site
        return None

    def new_sink(self) -> object:
        """Supplies a plain recorder; never asked, since no script runs."""
        return Recorder()


class CellHooks:
    """A cell's two hooks: its script on the first visit to its site, and ``None`` ever after.

    The site is visited on the cell's first sink, except for a ``factory`` cell, whose sinks
    are built by the library and whose first build is the site. ``swap-close`` is that sink's
    ``close`` and ``swap-emit`` its ``emit``. At ``swap-emit`` the hook sets :attr:`entered`
    and waits until a swap token is open before it answers, so the script runs while a second
    ``configure(sink=…)`` is in flight; a gate that runs out is ``NotExercised:gate``.
    """

    def __init__(self, cell: Cell, *, gate: float = GATE_TIMEOUT) -> None:
        """Builds the hooks for a cell.

        Args:
          cell: The cell.
          gate: How long each ``swap-emit`` wait may take.
        """
        self.cell = cell
        self.gate = gate
        self.entered = threading.Event()
        self.first: object = None
        self._lock = threading.Lock()
        self._fired = False

    def script(self, sink: object, site: str) -> tuple[str, ...] | None:
        """Answers the cell's script at the first visit to its site, and ``None`` otherwise."""
        target = {"swap-close": "close", "swap-emit": "emit"}.get(self.cell.site, self.cell.site)
        if site != target:
            return None
        if self.cell.personality != "factory" and sink is not self.first:
            return None
        with self._lock:
            if self._fired:
                return None
            self._fired = True
        run = current()
        run.visited()
        if self.cell.site == "swap-emit":
            self.entered.set()
            deadline = time.monotonic() + self.gate
            while not _lifecycle._state._swaps_running and time.monotonic() < deadline:
                time.sleep(0.001)
            if not _lifecycle._state._swaps_running:
                run.find("NotExercised:gate", "no sink change opened a swap token")
                return None
        if self.cell.personality == "forker":
            return ("fork",)
        return self.cell.script

    def new_sink(self) -> object:
        """Supplies a sink of the cell's personality that runs no script."""
        return quiet_sink(self.cell.personality)


def quiet_sink(personality: str) -> object:
    """Returns a new sink of a personality that runs no script.

    Args:
      personality: The personality.

    Returns:
      The sink.
    """
    if personality == "factory":
        return Recorder()
    return Reentrant(_Quiet())


def installer(personality: str) -> Callable[[object], None]:
    """Returns how a personality's sink is installed.

    Args:
      personality: The personality.

    Returns:
      ``install_built`` for a ``factory``, ``install_sink`` otherwise.
    """
    return install_built if personality == "factory" else install_sink


def _install_first(run: ChildRun, cell: Cell, hooks: CellHooks) -> None:
    """Installs the cell's first sink with ``configure``, timed as a sink change.

    Args:
      run: The child's run.
      cell: The cell.
      hooks: Its hooks.
    """
    if cell.personality == "factory":
        build = factory(hooks)
        run.call(
            "configure",
            _lifecycle.DEFAULT_SWAP_TIMEOUT,
            lambda: log_foundry.configure(service="model", sink_factory=build),
        )
        return
    hooks.first = Reentrant(hooks)
    run.call("configure", _lifecycle.DEFAULT_SWAP_TIMEOUT, lambda: install_sink(hooks.first))


def _displace(run: ChildRun, cell: Cell) -> None:
    """Makes the second ``configure(sink=…)`` that ``swap-close`` and ``swap-emit`` need.

    Args:
      run: The child's run.
      cell: The cell.
    """
    second = quiet_sink(cell.personality)
    run.call("configure", _lifecycle.DEFAULT_SWAP_TIMEOUT, lambda: install_sink(second))


def _judge_after_shutdown(run: ChildRun) -> bool:
    """Applies the oracles due once the cell's ``shutdown()`` has returned.

    Args:
      run: The child's run.

    Returns:
      Whether two sink changes overlapped, the one case the repeat rule is not judged in.
    """
    exempt = changes_overlapped(run.calls())
    for sink in run.sinks:
        if isinstance(sink, Recorder):
            run.check(check_closed, sink.closes())
            if not exempt:
                run.check(check_repeat, sink.closes())
    run.check(check_ledger, read_ledger(run))
    run.check(check_end, *read_end())
    return exempt


def _judge_after_exit(run: ChildRun, exempt: bool) -> None:
    """Applies the oracles due once the exit drain has returned.

    Args:
      run: The child's run.
      exempt: Whether the repeat rule is not judged.
    """
    for sink in run.sinks:
        if isinstance(sink, Recorder):
            run.check(check_released, sink.closes())
            if not exempt:
                run.check(check_repeat, sink.closes())


def _tail(run: ChildRun) -> None:
    """The procedure's common end: the cell's shutdown, its oracles, the exit drain and its own.

    Args:
      run: The child's run.
    """
    run.call(
        "final-shutdown",
        FINAL_SHUTDOWN_TIMEOUT,
        lambda: log_foundry.shutdown(timeout=FINAL_SHUTDOWN_TIMEOUT),
    )
    run.settle()
    exempt = _judge_after_shutdown(run)
    run.call("exit", _lifecycle.DEFAULT_SHUTDOWN_TIMEOUT, _lifecycle._shutdown_worker)
    run.settle()
    _judge_after_exit(run, exempt)
    closes = [sink.closes() for sink in run.sinks if isinstance(sink, Recorder)]
    run.note(f"closes (writes, at entry, at return): {closes}")
    run.note(f"health: {log_foundry.health()}")


def cell_body(run: ChildRun, cell: Cell, *, gate: float = GATE_TIMEOUT) -> None:
    """The one procedure every cell follows, in its child.

    Args:
      run: The child's run.
      cell: The cell.
      gate: How long each ``swap-emit`` wait may take.
    """
    run.expect_site()
    hooks = CellHooks(cell, gate=gate)
    _install_first(run, cell, hooks)
    if cell.site == "swap-emit":
        run.count("span")
        run.call("span", 0.0, model_sinks._model_span)
        if hooks.entered.wait(gate):
            _displace(run, cell)
        else:
            run.find("NotExercised:gate", "the sink's first emit never entered")
    else:
        reach(cell.path)
        if cell.site == "swap-close":
            _displace(run, cell)
    _tail(run)


def run_cell(cell: Cell) -> Outcome:
    """Runs one cell in a watched child.

    Args:
      cell: The cell.

    Returns:
      What the parent read from the child.
    """
    return run_body(functools.partial(cell_body, cell=cell))


def _params() -> list[object]:
    """Returns one parameter per scheduled cell, a pinned one carrying the strict mark.

    Returns:
      The parameters.
    """
    params: list[object] = []
    for cell in scheduled():
        pin = model_pins.pin_of(cell)
        marks = (
            [
                pytest.mark.xfail(
                    strict=True, raises=PinnedVerdict, reason=f"pinned by {pin.name}: {pin.item}"
                )
            ]
            if pin is not None
            else []
        )
        params.append(pytest.param(cell, id=cell_id(cell), marks=marks))
    return params


def _assert_this_tree(outcome: Outcome) -> None:
    """Fails a void run: one whose library, here or in the child, is not under this tree's src.

    Args:
      outcome: The cell's outcome.
    """
    here = pathlib.Path(log_foundry.__file__).resolve()
    assert here.is_relative_to(_SRC), f"void run: this process imported {here}, not {_SRC}"
    if outcome.file is not None:
        there = pathlib.Path(outcome.file).resolve()
        assert there.is_relative_to(_SRC), f"void run: the child imported {there}, not {_SRC}"


@pytest.mark.parametrize("cell", _params())
def test_cell(cell: Cell, record_property: Callable[[str, object], None]) -> None:
    """One cell, judged against its pin; its verdict, tree and margin recorded on the report."""
    outcome = run_cell(cell)
    record_property("model_verdict", " ".join(sorted(outcome.verdict)) or "pass")
    record_property("model_file", outcome.file or "none")
    if outcome.margin is not None:
        record_property(MARGIN, f"{outcome.margin:.6f}")
    if outcome.stacks:
        record_property("model_stacks", json.dumps(outcome.stacks, sort_keys=True))
    for name, value in sorted(outcome.properties.items()):
        record_property(name, value)
    _assert_this_tree(outcome)
    settle(cell, outcome.verdict, outcome.text)
