"""No discharge of a close under ``_state._lock`` can owe a deferred stop-signal hand.

A worker build, or a retarget's hand of the worker's new sink, that meets a close of that sink in
flight during a stop marks the close's entry instead of handing the sink its signal
(``_CloseRegistry.defer_hand``), and whoever discharges the entry pays the hand through
``_lifecycle._hand_after_close`` — which takes ``_state._lock``. A discharge that runs with that
lock held and finds the mark would therefore deadlock the thread on its own lock, or, if it skipped
the payment, leave the late worker delivering into a sink on the shutdown's set signal. That cannot
happen today only because of a **shape**: every release made under the lock discharges an entry it
took in the same critical section, and the mark is written only in ``_get_worker``'s and
``_hand_retargeted``'s sections under the same lock, so nothing can mark it in between; and the one
discharge under the lock that can meet a mark, ``put_back``, reports it and lets ``_close_owed`` pay
after releasing the lock. This file holds that shape over the source.

Five checks, each a reason a finding names, and a roster of the sites the real module has, so a
rename or a lost ``with`` detection cannot make the audit pass by finding nothing. Four hold the
discharges under the lock; the fifth holds the premise they rest on, that every
``defer_hand`` call sits inside a ``with _state._lock:`` body. The corpus at the bottom asserts
each check's reason on a case written to trip it, and silence on the shapes that must pass.

**The scan is lexical.** It sees a call written inside a ``with _state._lock:`` body of the
function that holds the lock, and nothing else: a discharge reached through a helper that a
locked section calls, or a ``defer_hand`` reached that way, is invisible to it, and so is a lock
held through any spelling other than ``_state._lock``. A new helper on such a path needs this
module's reasoning checked by hand. Serves invariants 3 and 5.
"""

from __future__ import annotations

import ast
import pathlib
import textwrap

import pytest

import log_foundry

_SOURCE = pathlib.Path(log_foundry.__file__).resolve().parent / "_lifecycle.py"

RELEASE_NOT_TAKEN = "a release under the state lock discharges an entry it did not take there"
FINISH_NOT_TAKEN = "a finish under the state lock discharges an entry it did not take there"
PUT_BACK_DISCARDED = "a put_back under the state lock discards the deferred hand it reports"
PAID_UNDER_LOCK = "a deferred hand is paid under the state lock, which the payment takes itself"
MARKED_OUTSIDE_LOCK = (
    "a hand is deferred outside the state lock, so a locked discharge could meet it"
)

LOCK_HELD_SITES = {
    ("_swap_sink", "release"): 2,
    ("_close_owed", "put_back"): 1,
    ("_get_worker", "defer_hand"): 1,
    ("_hand_retargeted", "defer_hand"): 1,
}
"""Every discharge made under ``_state._lock`` in ``_lifecycle.py``, and every mark, by function.

``_swap_sink`` releases a stale sink and its displaced one detached, each taken in the same section;
``_close_owed`` puts back what an interrupt left untaken; ``_get_worker`` and ``_hand_retargeted``
mark a close in flight, the two writers of the mark — a worker build, and the hand
``Worker.retarget`` makes of its new sink. The ``defer_hand`` rows count every call, under the lock
or not, so moving a call is a roster change as well as a finding. A new site is a new row, after the
reasoning in this module's docstring has been checked for it.
"""


def _is_state_lock(node: ast.expr) -> bool:
    """Whether a ``with`` item is ``_state._lock``."""
    return (
        isinstance(node, ast.Attribute)
        and node.attr == "_lock"
        and isinstance(node.value, ast.Name)
        and node.value.id == "_state"
    )


def _called(node: ast.Call) -> str:
    """The name a call is made through: the bare name or the last attribute."""
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return ""


def _is_take(node: ast.expr | None, taken: set[str]) -> bool:
    """Whether an entry argument is a ``take(...)`` call, or a name bound from one in the block."""
    if isinstance(node, ast.Call):
        return _called(node) == "take"
    return isinstance(node, ast.Name) and node.id in taken


def _own_nodes(func: ast.FunctionDef | ast.AsyncFunctionDef) -> list[ast.AST]:
    """Every node of a function's body, without entering a definition nested in it."""
    nodes: list[ast.AST] = []
    stack: list[ast.AST] = list(func.body)
    while stack:
        node = stack.pop()
        nodes.append(node)
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            stack.extend(ast.iter_child_nodes(node))
    return nodes


def _locked_bodies(tree: ast.Module) -> list[tuple[str, list[ast.stmt]]]:
    """Every ``with _state._lock:`` body in the module, with the function that holds it."""
    return [
        (func.name, node.body)
        for func in ast.walk(tree)
        if isinstance(func, ast.FunctionDef | ast.AsyncFunctionDef)
        for node in _own_nodes(func)
        if isinstance(node, ast.With) and any(_is_state_lock(i.context_expr) for i in node.items)
    ]


def _calls_in(body: list[ast.stmt]) -> list[tuple[ast.Call, ast.AST]]:
    """Each call in a block with its parent, never descending into a nested definition."""
    out: list[tuple[ast.Call, ast.AST]] = []
    stack: list[ast.AST] = list(body)
    parents: dict[int, ast.AST] = {}
    while stack:
        node = stack.pop()
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda | ast.ClassDef):
            continue
        for child in ast.iter_child_nodes(node):
            parents[id(child)] = node
            stack.append(child)
        if isinstance(node, ast.Call):
            out.append((node, parents.get(id(node), node)))
    return out


def _bound_from_take(body: list[ast.stmt]) -> set[str]:
    """The names a block assigns from a ``take(...)`` call."""
    names: set[str] = set()
    for node in body:
        for sub in ast.walk(node):
            if isinstance(sub, ast.Assign) and _is_take(sub.value, set()):
                names |= {t.id for t in sub.targets if isinstance(t, ast.Name)}
    return names


def _defer_calls(tree: ast.Module) -> list[tuple[str, ast.Call]]:
    """Every ``defer_hand(...)`` call in the module, with the function it is written in."""
    return [
        (func.name, node)
        for func in ast.walk(tree)
        if isinstance(func, ast.FunctionDef | ast.AsyncFunctionDef)
        for node in _own_nodes(func)
        if isinstance(node, ast.Call) and _called(node) == "defer_hand"
    ]


def audit(source: str) -> tuple[list[str], dict[tuple[str, str], int]]:
    """Runs the five checks over one module's source.

    Returns the findings, each ``"<function>: <reason>"``, and the sites found, by function and
    call, for the roster: the lock-held discharges and every ``defer_hand`` call.
    """
    findings: list[str] = []
    sites: dict[tuple[str, str], int] = {}
    tree = ast.parse(source)
    locked_calls: set[int] = set()
    for function, body in _locked_bodies(tree):
        taken = _bound_from_take(body)
        for call, parent in _calls_in(body):
            locked_calls.add(id(call))
            name = _called(call)
            if name in {"release", "finish", "put_back"}:
                sites[(function, name)] = sites.get((function, name), 0) + 1
            if name == "release":
                closing = next((k.value for k in call.keywords if k.arg == "closing"), None)
                if not _is_take(closing, taken):
                    findings.append(f"{function}: {RELEASE_NOT_TAKEN}")
            elif name == "finish":
                entry = call.args[1] if len(call.args) > 1 else None
                if not _is_take(entry, taken):
                    findings.append(f"{function}: {FINISH_NOT_TAKEN}")
            elif name == "put_back":
                if isinstance(parent, ast.Expr):
                    findings.append(f"{function}: {PUT_BACK_DISCARDED}")
            elif name == "_hand_after_close":
                findings.append(f"{function}: {PAID_UNDER_LOCK}")
    for function, call in _defer_calls(tree):
        sites[(function, "defer_hand")] = sites.get((function, "defer_hand"), 0) + 1
        if id(call) not in locked_calls:
            findings.append(f"{function}: {MARKED_OUTSIDE_LOCK}")
    return findings, sites


def test_no_discharge_under_the_state_lock_can_owe_a_deferred_hand() -> None:
    """The shipped module keeps the shape, and its lock-held discharges are the recorded ones."""
    findings, sites = audit(_SOURCE.read_text())
    assert findings == [], findings
    assert sites == LOCK_HELD_SITES, (
        f"the discharges made under _state._lock moved: {sites}. Check each new site against this "
        "module's docstring before recording it"
    )


def _wrap(body: str) -> str:
    """Puts a corpus case inside a function, as every lock-held section in the module is."""
    return "def site():\n" + textwrap.indent(textwrap.dedent(body), "    ")


CORPUS: dict[str, tuple[str, list[str]]] = {
    "release-with-no-closing": (
        """
        with _state._lock:
            release(stale, detached=True)
        """,
        [f"site: {RELEASE_NOT_TAKEN}"],
    ),
    "release-closing-from-a-call-that-is-not-a-take": (
        """
        with _state._lock:
            release(stale, detached=True, closing=_state._closes._in_flight.get(id(stale)))
            release(other, detached=True, closing=_state._closes.register(other, _Rule.GRACED))
        """,
        [f"site: {RELEASE_NOT_TAKEN}", f"site: {RELEASE_NOT_TAKEN}"],
    ),
    "release-closing-from-elsewhere": (
        """
        entry = _state._closes.take(stale, rule)
        with _state._lock:
            release(stale, detached=True, closing=entry)
        """,
        [f"site: {RELEASE_NOT_TAKEN}"],
    ),
    "finish-of-an-entry-not-taken-here": (
        """
        with _state._lock:
            _state._closes.finish(sink, close)
        """,
        [f"site: {FINISH_NOT_TAKEN}"],
    ),
    "finish-of-an-entry-taken-in-the-same-block": (
        """
        with _state._lock:
            entry = _state._closes.take(sink, rule)
            _state._closes.finish(sink, entry)
            _state._closes.finish(other, _state._closes.take(other, rule))
        """,
        [],
    ),
    "put-back-discarded": (
        """
        with _state._lock:
            for sink, close in undischarged.values():
                _state._closes.put_back(sink, close)
        """,
        [f"site: {PUT_BACK_DISCARDED}"],
    ),
    "paid-under-the-lock": (
        """
        with _state._lock:
            if _state._closes.put_back(sink, close):
                _hand_after_close(sink)
        """,
        [f"site: {PAID_UNDER_LOCK}"],
    ),
    "the-shipped-shapes-pass": (
        """
        with _state._lock:
            closer = release(
                stale, detached=True, closing=_state._closes.take(stale, _Rule.GRACED)
            )
            entry = _state._closes.take(previous, _Rule.GRACED)
            release(previous, detached=True, closing=entry)
            if _state._closes.put_back(sink, close):
                owed.append(sink)
        for sink in owed:
            _hand_after_close(sink)
        release(sink, closing=close)
        """,
        [],
    ),
    "a-nested-definition-runs-later": (
        """
        with _state._lock:
            def later():
                release(sink)
                _hand_after_close(sink)
        """,
        [],
    ),
    "a-lock-held-in-a-nested-definition-is-its-own": (
        """
        def inner():
            with _state._lock:
                release(stale, detached=True)
        """,
        [f"inner: {RELEASE_NOT_TAKEN}"],
    ),
    "a-mark-outside-the-state-lock": (
        """
        with _state._lock:
            worker = _state.worker_exists()
        deferred = _state._closes.defer_hand(sink)
        """,
        [f"site: {MARKED_OUTSIDE_LOCK}"],
    ),
    "a-mark-under-another-lock-only": (
        """
        with _state._closes._lock:
            deferred = _state._closes.defer_hand(sink)
        """,
        [f"site: {MARKED_OUTSIDE_LOCK}"],
    ),
    "a-mark-inside-the-state-lock-passes": (
        """
        with _state._lock:
            sink = _ensure_sink()
            deferred = bool(_state._stops_running) and _state._closes.defer_hand(sink)
        """,
        [],
    ),
    "another-lock-is-not-the-state-lock": (
        """
        with _state._closes._lock:
            _state._closes.finish(sink, close)
        """,
        [],
    ),
}
"""Each case's source and the findings it must produce: a reason, or silence."""


@pytest.mark.parametrize("case", list(CORPUS))
def test_the_audit_names_the_reason_or_stays_silent(case: str) -> None:
    """Each check fires on the case written to trip it, and on nothing the shipped code does."""
    body, expected = CORPUS[case]
    findings, _ = audit(_wrap(body))
    assert findings == expected, findings
