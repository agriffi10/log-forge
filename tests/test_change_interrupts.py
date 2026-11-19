"""SPEC-097 FR-002: an interrupt anywhere in a sink change leaves no state a later call defers to.

Three review rounds each found a new way for a ``KeyboardInterrupt`` or a stop deferred during a
change to leave the deferral's state set: a slot one statement set and only a later statement
cleared. Each fix closed its route and opened the next. This corpus replaces the review loop.

**The population.** A child (``tests/_change_interrupt_child.py``) raises ``KeyboardInterrupt``
at one line event per run of eight functions, :data:`TRACED`: the change (``_change_sink``), its
publication and end (``open_command_swap``, ``end_command_swap``, ``close_command_swap``), its
swap (``_swap_sink``), and the deferral (``_merged_or_deferred``, ``_live_taker``,
``run_deferred_stops``). It does so for a non-re-entrant ``configure(sink=...)`` on the main
thread whose stop-signal setter defers a ``shutdown()``, and for the same change made from a
sink's ``emit`` on the drain thread. The rows are the line events a count run, which raises
nothing, reaches: they are derived from the code at run time, and the functions each mode must
reach are asserted, so a rename or a path that stops being taken fails here rather than
shrinking the corpus. ``_shutdown_worker``, the stop the deferral runs, is not traced: its
interruption is any ``shutdown()``'s, which this spec does not change.

**What no row reaches.** The rows are only the lines the count run executes, so a branch it never
takes is never interrupted here. Three are pinned by named tests in
``tests/test_deferred_stop_bound.py`` instead: ``_live_taker``'s branch that drops a taker whose
change has ended (``test_a_taker_whose_change_has_ended_reads_as_none_and_lets_its_frame_go``),
``end_command_swap``'s finding the operation by the frame when ``configure()`` never bound it
(``test_an_interrupt_before_the_change_binds_its_operation_leaves_nothing_published``), and
``open_command_swap``'s publishing the operation before adding its token
(``test_an_interrupt_between_publishing_and_adding_the_token_leaves_no_token``).

**The checks.** After each interrupt the interrupted thread owns no library lock,
``_sink_lock`` is not held and ``_sink_holder`` is not set, nothing deferred is left on a running
taker, a later unrelated ``configure(sink=...)`` retires nothing, a later ``shutdown()`` retires,
and by the exit every sink that was given events has been closed, none twice.

**The excused rows**, each by a reason derived from the code and asserted, never by a count:

- **A ``with`` whose ``__exit__`` the interrupt skipped.** An exception raised as a ``with``
  statement's exit runs, on the ``with`` line before ``__exit__`` is called, leaves the lock owned
  for good (CPython gh-74174). The row must have left a library lock owned and its line must be
  one of its function's ``with`` lines, read from the AST. The same rows hang the child at
  ``c9068a46``, whose locks are plain.
- **An instruction CPython's exception table leaves uncovered inside a ``with``.** An inlined
  comprehension's filter jump inside a ``with`` body is in no exception-table range on 3.13, so an
  exception raised there leaves the frame without running ``__exit__``. The row must have left a
  lock owned and its line must hold an instruction, inside a ``with`` statement's lines, that no
  entry of ``dis.Bytecode(...).exception_entries`` covers.
- **A displaced sink taken for a closer that never starts.** ``_swap_sink`` marks the sink it
  displaced as taken (``took``) and takes it off the owed record in the arguments of the
  ``release`` that starts its closer; an interrupt between the two loses that close. The code is
  ``c9068a46``'s, unchanged; that run never reaches it because its stop ran inline first. The span
  is derived from the AST, and the row must fail that check alone.
- **An interrupt on the statements that release ``_sink_lock`` or clear ``_sink_holder``.** Each is
  in a ``finally`` of its own, outside the end section, so nothing else leaves either; an
  interrupt on the statement itself is what no statement order closes. The row's line must be in
  ``_change_sink``'s ``finally`` blocks that release the lock or clear the holder, read from the
  AST, and it must fail only those two checks.

Serves invariants 3 and 5.
"""

from __future__ import annotations

import ast
import concurrent.futures
import dis
import inspect
import json
import pathlib
import subprocess
import sys
import textwrap
from typing import TYPE_CHECKING, Any

import pytest

from log_foundry import _lifecycle, config

if TYPE_CHECKING:
    from collections.abc import Callable

CHILD = pathlib.Path(__file__).with_name("_change_interrupt_child.py")

TRACED: dict[str, Callable[..., Any]] = {
    "_change_sink": config._change_sink,
    "open_command_swap": _lifecycle.open_command_swap,
    "end_command_swap": _lifecycle.end_command_swap,
    "close_command_swap": _lifecycle.close_command_swap,
    "_swap_sink": _lifecycle._swap_sink,
    "_merged_or_deferred": _lifecycle._merged_or_deferred,
    "_live_taker": _lifecycle._live_taker,
    "run_deferred_stops": _lifecycle.run_deferred_stops,
}
"""Every function the child traces, by name; the child must trace exactly these."""

REACHED = {
    "user": set(TRACED),
    "drain": set(TRACED) - {"open_command_swap", "close_command_swap"},
}
"""The functions each mode's count run reaches; a drain-thread change is re-entrant, publishing
nothing."""

OWNED = "the interrupt left a library lock owned by its thread"
SINK_LOCK = "_sink_lock was left held"
HOLDER = "_sink_holder was left set"


def _run(mode: str, k: int) -> dict[str, Any]:
    """One child run; its report, or the failure."""
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-B", str(CHILD), mode, str(k)],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    lines = [line for line in result.stdout.splitlines() if line.startswith("{")]
    if result.returncode != 0 or not lines:
        return {"mode": mode, "k": k, "failed": result.returncode, "stderr": result.stderr[-2000:]}
    report: dict[str, Any] = json.loads(lines[-1])
    return report


def failures(report: dict[str, Any]) -> list[str]:
    """Every check a run failed, by its reason.

    Args:
      report: The child's report.

    Returns:
      The failed checks' reasons, empty when the run passed every one.
    """
    if "failed" in report:
        return [f"the child failed: exit {report['failed']}: {report['stderr']}"]
    found = []
    if report["sink_lock_left"]:
        found.append(SINK_LOCK)
    if report["holder_left"]:
        found.append(HOLDER)
    if report.get("owned"):
        return [OWNED, *found]
    if report["live_taker_deferred"]:
        found.append("a deferral was left on a running taker")
    if report["later_configure_retired"]:
        found.append("a later unrelated configure(sink=...) retired the library")
    if report["later_shutdown_retired"] is not True:
        found.append("a later shutdown() did not retire")
    for name, (events, closes) in sorted(report["at_exit"].items()):
        if closes > 1:
            found.append(f"{name} was closed {closes} times")
        elif events and closes != 1:
            found.append(f"{name} was given events and not closed by the exit")
    return found


def _tree(function: Callable[..., Any]) -> tuple[ast.AST, int]:
    """A function's AST and the line its source starts on."""
    lines, first = inspect.getsourcelines(function)
    return ast.parse(textwrap.dedent("".join(lines))), first


def _with_lines(function: Callable[..., Any]) -> set[int]:
    """The lines of every ``with`` statement in a function."""
    tree, first = _tree(function)
    return {n.lineno + first - 1 for n in ast.walk(tree) if isinstance(n, ast.With)}


def _uncovered_lines(function: Callable[..., Any]) -> set[int]:
    """Lines inside a ``with`` holding an instruction that no exception-table entry covers."""
    tree, first = _tree(function)
    spans = [
        (n.lineno + first - 1, (n.end_lineno or n.lineno) + first - 1)
        for n in ast.walk(tree)
        if isinstance(n, ast.With)
    ]
    code = function.__code__
    entries = dis.Bytecode(code).exception_entries
    found: set[int] = set()
    for instruction in dis.get_instructions(code):
        line = instruction.positions.lineno if instruction.positions else None
        if line is None or not any(lo <= line <= hi for lo, hi in spans):
            continue
        if not any(e.start <= instruction.offset < e.end for e in entries):
            found.add(line)
    return found


def _release_lines() -> set[int]:
    """The lines of ``_change_sink``'s ``finally`` blocks that release the lock or clear the holder."""
    tree, first = _tree(config._change_sink)
    found: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Try):
            continue
        for statement in node.finalbody:
            releases = any(
                (
                    isinstance(n, ast.Call)
                    and isinstance(n.func, ast.Attribute)
                    and n.func.attr == "release"
                )
                or (
                    isinstance(n, ast.Assign)
                    and any(isinstance(t, ast.Name) and t.id == "_sink_holder" for t in n.targets)
                )
                for n in ast.walk(statement)
            )
            if releases:
                end = statement.end_lineno or statement.lineno
                found.update(range(statement.lineno + first - 1, end + first))
    return found


def _took_then_release_lines() -> set[int]:
    """The lines from a ``took`` to the ``release`` that starts the taken sink's closer.

    Derived from ``_swap_sink``'s source: every ``release(...)`` call whose ``closing`` argument
    takes from the owed record, with the nearest statement before it in the same block that
    calls ``took``.

    Returns:
      Absolute line numbers in ``_lifecycle.py``.
    """
    tree, first = _tree(_lifecycle._swap_sink)
    spans: set[int] = set()

    def calls(node: ast.AST, attr: str) -> bool:
        return any(
            isinstance(n, ast.Call)
            and (
                (isinstance(n.func, ast.Attribute) and n.func.attr == attr)
                or (isinstance(n.func, ast.Name) and n.func.id == attr)
            )
            for n in ast.walk(node)
        )

    for node in ast.walk(tree):
        for field in ("body", "orelse", "finalbody"):
            block = getattr(node, field, None)
            if not isinstance(block, list):
                continue
            for i, statement in enumerate(block):
                releases = [
                    n
                    for n in ast.walk(statement)
                    if isinstance(n, ast.Call)
                    and isinstance(n.func, ast.Name)
                    and n.func.id == "release"
                    and any(k.arg == "closing" and calls(k.value, "take") for k in n.keywords)
                ]
                if not releases:
                    continue
                took = next((s for s in reversed(block[:i]) if calls(s, "took")), None)
                if took is None:
                    continue
                end = statement.end_lineno or statement.lineno
                spans.update(range(took.lineno + first - 1, end + first))
    return spans


def excused(report: dict[str, Any]) -> str | None:
    """The category a failing run belongs to, its reason checked, or ``None``.

    Args:
      report: The child's report.

    Returns:
      The category's name, or ``None`` when no category's reason holds.
    """
    found = failures(report)
    function, _, line_text = (report.get("site") or "").partition(":")
    if function not in TRACED or not line_text.isdigit():
        return None
    line = int(line_text)
    if found and found[0] == OWNED:
        if line in _with_lines(TRACED[function]):
            return "a with statement's __exit__ skipped"
        if line in _uncovered_lines(TRACED[function]):
            return "an instruction no exception-table entry covers"
        return None
    if (
        function == "_change_sink"
        and line in _release_lines()
        and set(found) <= {SINK_LOCK, HOLDER}
    ):
        return "an interrupt on the statement that releases the lock or clears the holder"
    if (
        function == "_swap_sink"
        and line in _took_then_release_lines()
        and len(found) == 1
        and found[0].endswith("was given events and not closed by the exit")
    ):
        return "a displaced sink taken for a closer that never starts"
    return None


@pytest.mark.parametrize("mode", ["user", "drain"])
def test_an_interrupt_at_any_line_of_a_change_leaves_nothing_a_later_call_defers_to(
    mode: str,
) -> None:
    """FR-002: every line of a change, interrupted in turn; each run passes or is excused."""
    count = _run(mode, -1)
    assert failures(count) == [], f"the uninterrupted run failed: {count}"
    assert count["fired"], "the premise: the setter made its shutdown()"
    assert count["traced"] == sorted(TRACED), f"the child traces {count['traced']}"
    reached = {site.partition(":")[0] for site in count["sites"]}
    assert reached == REACHED[mode], f"the count run reached {sorted(reached)}"
    with concurrent.futures.ThreadPoolExecutor(8) as pool:
        rows = list(pool.map(lambda k: _run(mode, k), range(count["events"])))
    unexcused = [
        (row["k"], row.get("site"), failures(row))
        for row in rows
        if failures(row) and excused(row) is None
    ]
    assert unexcused == [], f"{len(unexcused)} of {len(rows)} rows failed: {unexcused}"
    interrupted = [row for row in rows if row.get("outcome") == "interrupted"]
    assert len(interrupted) == len(rows), "a row's interrupt never fired"
