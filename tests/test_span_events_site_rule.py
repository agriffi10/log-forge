"""SPEC-091 FR-001 AC-3 and the hold-site gate: who may write a span's buffer, and how.

The exactly-once argument rests on a **site rule**: every mutation of ``span.events`` other than
``append``, and every rebinding of the attribute, happens inside the sweep hold
(``with _sweep_hold:`` or ``with _sweep_hold.within(...)``) or in the deferred-close drain the hold
runs before its release, and ``_sweep_lock`` is acquired nowhere but inside the hold's own
classes. A rule about sites is only as good as the list of sites, so the sites are derived from
the AST of ``src/`` rather than written down: a later edit that adds a fifth writer, or takes the
lock some other way, fails here by name.

The third gate holds the hold's re-entry guard: every function that enters the hold must first
ask ``_sweep_held_here()``, because a collector callback inside a hold that entered it again would
either wait on its own thread or take a list a pass is part-way through. The set of such functions
is held to a table naming what each does on a thread that already holds the hold.

Each gate has a corpus that asserts the failure's **reason**, including the cases that must stay
silent, because a gate run only against the tree it guards proves the tree passes, not that the
gate works.
"""

from __future__ import annotations

import ast
import pathlib
from dataclasses import dataclass

import pytest

import log_foundry

_SRC = pathlib.Path(log_foundry.__file__).resolve().parent

MUTATORS = frozenset(
    {"extend", "insert", "pop", "remove", "clear", "sort", "reverse", "__setitem__", "__delitem__"}
)
"""``list`` methods that change a list in place; ``append`` is the one write allowed anywhere."""

HOLD_MACHINERY = frozenset({"_SweepHold", "_BoundedHold"})
"""The classes whose methods may acquire ``_sweep_lock``: the hold itself."""

DEFERRED_DRAIN = "_drain_deferred_closes"
"""The one function outside a ``with`` block that may swap a span's buffer: the hold's release
runs it while the lock is still held, and :func:`check_sites` proves nothing else calls it."""

EXCLUDED = {
    ("sinks/memory.py", "MemorySink"): (
        "a sink's own record of the batches it was handed, written only in its `__init__` and "
        "`emit`; never a span's buffer"
    ),
}
"""``.events`` attributes that are not a span's buffer, keyed by file and class, with the reason."""

HOLD_SITES = {
    "_sweep_registered": "takes nothing and returns None; a flush() that met it reports abandoned",
    "_sweep_open_spans": "takes nothing and returns False; its flush() reports abandoned",
    "_flush": "defers its swap and submit to the holder, which drains them before releasing",
    "_abandon": "marks the span and defers its take and its debit to the holder",
    "_adopt_unswept": "continue_trace() is refused with its own line",
}
"""Every function that enters the sweep hold, and what it does when its thread already holds it."""

SITE = "a span buffer is written outside the sweep hold"
ACQUIRE = "_sweep_lock is acquired outside the hold's own classes"
DRAIN_CALLER = "the deferred-close drain is called from outside the hold's release"
UNCHECKED = "a function enters the sweep hold without first asking _sweep_held_here()"
UNLISTED = "the functions entering the sweep hold differ from HOLD_SITES"


@dataclass(frozen=True)
class Finding:
    """One refusal: why, and where, as ``file:qualname``."""

    reason: str
    where: str


def _sources() -> dict[str, str]:
    return {
        str(path.relative_to(_SRC)): path.read_text(encoding="utf-8")
        for path in sorted(_SRC.rglob("*.py"))
    }


def _is_hold(item: ast.withitem) -> bool:
    expr = item.context_expr
    if isinstance(expr, ast.Name) and expr.id == "_sweep_hold":
        return True
    return (
        isinstance(expr, ast.Call)
        and isinstance(expr.func, ast.Attribute)
        and expr.func.attr == "within"
        and isinstance(expr.func.value, ast.Name)
        and expr.func.value.id == "_sweep_hold"
    )


def _names_lock(expr: ast.expr) -> bool:
    return (isinstance(expr, ast.Name) and expr.id == "_sweep_lock") or (
        isinstance(expr, ast.Attribute) and expr.attr == "_sweep_lock"
    )


class _Walker(ast.NodeVisitor):
    """Walks one module, tracking the enclosing class, function and hold depth."""

    def __init__(self, file: str) -> None:
        self.file = file
        self.scope: list[str] = []
        self.classes: list[str] = []
        self.hold = 0
        self.aliases: list[set[str]] = [set()]
        self.findings: list[Finding] = []
        self.hold_functions: dict[str, list[int]] = {}
        self.held_checks: dict[str, list[int]] = {}

    def where(self) -> str:
        return f"{self.file}:{'.'.join(self.scope) or '<module>'}"

    def function(self) -> str:
        return self.scope[-1] if self.scope else "<module>"

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.scope.append(node.name)
        self.classes.append(node.name)
        self.generic_visit(node)
        self.classes.pop()
        self.scope.pop()

    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        self.scope.append(node.name)
        self.aliases.append(set())
        saved, self.hold = self.hold, 0
        self.generic_visit(node)
        self.hold = saved
        self.aliases.pop()
        self.scope.pop()

    visit_FunctionDef = _visit_function
    visit_AsyncFunctionDef = _visit_function

    def _visit_with(self, node: ast.With | ast.AsyncWith) -> None:
        holds = any(_is_hold(item) for item in node.items)
        if holds:
            self.hold_functions.setdefault(self.function(), []).append(node.lineno)
        if any(_names_lock(item.context_expr) for item in node.items):
            self._acquired()
        self.hold += holds
        self.generic_visit(node)
        self.hold -= holds

    visit_With = _visit_with
    visit_AsyncWith = _visit_with

    def _acquired(self) -> None:
        if not (set(self.classes) & HOLD_MACHINERY):
            self.findings.append(Finding(ACQUIRE, self.where()))

    def _excluded(self) -> bool:
        return any((self.file, cls) in EXCLUDED for cls in self.classes)

    def _is_buffer(self, expr: ast.expr) -> bool:
        if isinstance(expr, ast.Attribute) and expr.attr == "events":
            return True
        return isinstance(expr, ast.Name) and expr.id in self.aliases[-1]

    def _write(self) -> None:
        if self.hold or self.function() == DEFERRED_DRAIN or self._excluded():
            return
        self.findings.append(Finding(SITE, self.where()))

    def _target(self, target: ast.expr) -> None:
        if isinstance(target, ast.Tuple | ast.List):
            for element in target.elts:
                self._target(element)
        elif (isinstance(target, ast.Attribute) and target.attr == "events") or (
            isinstance(target, ast.Subscript) and self._is_buffer(target.value)
        ):
            self._write()
        elif isinstance(target, ast.Starred):
            self._target(target.value)

    def visit_Assign(self, node: ast.Assign) -> None:
        for target in node.targets:
            self._target(target)
            if (
                isinstance(target, ast.Name)
                and isinstance(node.value, ast.Attribute)
                and node.value.attr == "events"
            ):
                self.aliases[-1].add(target.id)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        self._target(node.target)
        self.generic_visit(node)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        self._target(node.target)
        if isinstance(node.target, ast.Name) and node.target.id in self.aliases[-1]:
            self._write()
        self.generic_visit(node)

    def visit_Delete(self, node: ast.Delete) -> None:
        for target in node.targets:
            self._target(target)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        if isinstance(func, ast.Attribute):
            if func.attr in MUTATORS and self._is_buffer(func.value):
                self._write()
            if func.attr == "acquire" and _names_lock(func.value):
                self._acquired()
        if isinstance(func, ast.Name):
            if func.id == "_sweep_held_here":
                self.held_checks.setdefault(self.function(), []).append(node.lineno)
            if func.id == DEFERRED_DRAIN and self.function() != "_release_hold":
                self.findings.append(Finding(DRAIN_CALLER, self.where()))
        self.generic_visit(node)


def _walk(sources: dict[str, str]) -> list[_Walker]:
    walkers = []
    for file, text in sources.items():
        walker = _Walker(file)
        walker.visit(ast.parse(text))
        walkers.append(walker)
    return walkers


def check_sites(sources: dict[str, str]) -> list[Finding]:
    """The buffer writes outside the hold, lock acquisitions outside it, and stray drain calls."""
    return [finding for walker in _walk(sources) for finding in walker.findings]


def check_hold_sites(sources: dict[str, str], table: dict[str, str] = HOLD_SITES) -> list[Finding]:
    """Every function entering the hold asks ``_sweep_held_here()`` first, and is in ``table``."""
    findings: list[Finding] = []
    entered: set[str] = set()
    for walker in _walk(sources):
        for function, withs in walker.hold_functions.items():
            entered.add(function)
            checks = walker.held_checks.get(function, [])
            if not checks or min(checks) > min(withs):
                findings.append(Finding(UNCHECKED, f"{walker.file}:{function}"))
    if entered != set(table):
        findings.append(Finding(UNLISTED, f"{sorted(entered ^ set(table))}"))
    return findings


# -- the gates against the tree ------------------------------------------------------------


def test_every_span_buffer_write_is_an_append_or_inside_the_hold() -> None:
    """FR-001 AC-3: the site rule holds on ``src/``."""
    assert check_sites(_sources()) == []


def test_every_hold_site_checks_for_a_hold_on_its_own_thread() -> None:
    """FR-001 (added 2026-11-17): the five hold sites, each asking first."""
    assert check_hold_sites(_sources()) == []


# -- the corpus --------------------------------------------------------------------------------


def _with(file: str, old: str, new: str) -> dict[str, str]:
    sources = _sources()
    assert sources[file].count(old) == 1, f"the corpus anchor moved: {old!r}"
    sources[file] = sources[file].replace(old, new)
    return sources


def test_a_clear_added_to_api_log_is_named() -> None:
    """FR-001 AC-3's own case: ``span.events.clear()`` in ``api._log`` fails naming the site."""
    sources = _with(
        "api.py",
        "                span.events.append(event)\n",
        "                span.events.append(event)\n                span.events.clear()\n",
    )
    assert check_sites(sources) == [Finding(SITE, "api.py:_log")]


def test_a_sweep_lock_with_outside_the_hold_is_named() -> None:
    """FR-001 AC-3 (revised 2026-11-17): the lock taken by ``with`` outside ``_SweepHold``."""
    sources = _with(
        "decorator.py",
        "def _sweep_held_here() -> bool:\n",
        "def _bypass() -> None:\n    with _sweep_lock:\n        pass\n\n\n"
        "def _sweep_held_here() -> bool:\n",
    )
    assert check_sites(sources) == [Finding(ACQUIRE, "decorator.py:_bypass")]


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        (
            "def f(span):\n    events = span.events\n    del events[1:]\n",
            [Finding(SITE, "x.py:f")],
        ),
        (
            "def f(span):\n    span.events[0:1] = []\n",
            [Finding(SITE, "x.py:f")],
        ),
        (
            "def f(span):\n    span.events = []\n",
            [Finding(SITE, "x.py:f")],
        ),
        (
            "def f(span):\n    span.events += [1]\n",
            [Finding(SITE, "x.py:f")],
        ),
        (
            "def f(span):\n    taken, span.events = span.events, []\n",
            [Finding(SITE, "x.py:f")],
        ),
        (
            "def f(span):\n    first, *span.events = [1, 2]\n",
            [Finding(SITE, "x.py:f")],
        ),
        (
            "def f(span):\n    span.events: list = []\n",
            [Finding(SITE, "x.py:f")],
        ),
        (
            "def f(span):\n    span.events.pop()\n",
            [Finding(SITE, "x.py:f")],
        ),
        (
            "def f():\n    _sweep_lock.acquire()\n",
            [Finding(ACQUIRE, "x.py:f")],
        ),
        (
            "def f():\n    _drain_deferred_closes()\n",
            [Finding(DRAIN_CALLER, "x.py:f")],
        ),
        ("def f(span):\n    span.events.append(1)\n", []),
        ("def f(span):\n    with _sweep_hold:\n        span.events = []\n", []),
        (
            (
                "def f(span, w):\n    with _sweep_hold.within(w):\n"
                "        events = span.events\n        del events[1:]\n"
            ),
            [],
        ),
        ("def _drain_deferred_closes(span):\n    span.events = []\n", []),
        ("class _SweepHold:\n    def __enter__(self):\n        _sweep_lock.acquire()\n", []),
        ("def f(span):\n    taken = span.events[1:]\n    return taken\n", []),
    ],
    ids=[
        "alias-del",
        "slice-assign",
        "rebind",
        "augmented",
        "tuple-rebind",
        "starred-rebind",
        "annotated-rebind",
        "mutator",
        "acquire",
        "drain-caller",
        "append-silent",
        "rebind-in-hold-silent",
        "alias-del-in-bounded-hold-silent",
        "deferred-drain-silent",
        "hold-class-acquire-silent",
        "slice-read-silent",
    ],
)
def test_the_site_rule_reads_each_shape(source: str, expected: list[Finding]) -> None:
    """Each write shape is refused outside the hold, and each admitted shape stays silent."""
    assert check_sites({"x.py": source}) == expected


def test_the_memory_sinks_own_events_are_excluded_and_nothing_else_there() -> None:
    """The one exclusion is keyed by class: another class in the same file is still caught."""
    silent = {
        "sinks/memory.py": "class MemorySink:\n    def emit(self, b):\n        self.events.extend(b)\n"
    }
    assert check_sites(silent) == []
    caught = {"sinks/memory.py": "class Other:\n    def emit(self):\n        self.events.clear()\n"}
    assert check_sites(caught) == [Finding(SITE, "sinks/memory.py:Other.emit")]


def test_a_writer_in_lifecycle_is_caught() -> None:
    """The scan is all of ``src/``, so a module other than the span modules is held too."""
    sources = _with(
        "_lifecycle.py",
        "def _forget_open_spans_after_fork() -> None:\n",
        "def _sneak(span: object) -> None:\n    span.events.pop()  # type: ignore[attr-defined]\n\n\n"
        "def _forget_open_spans_after_fork() -> None:\n",
    )
    assert check_sites(sources) == [Finding(SITE, "_lifecycle.py:_sneak")]


def test_a_hold_site_without_its_check_is_named() -> None:
    """FR-001 (added 2026-11-17): with the check removed from ``_flush`` the gate names ``_flush``."""
    sources = _with(
        "decorator.py",
        "    worker = _lifecycle._get_worker()\n    if _sweep_held_here():\n"
        "        _deferred_closes.append((span, False))\n        return\n",
        "    worker = _lifecycle._get_worker()\n",
    )
    assert check_hold_sites(sources) == [Finding(UNCHECKED, "decorator.py:_flush")]


def test_a_new_hold_site_must_be_listed() -> None:
    """A sixth function entering the hold, even one that asks first, must join the table."""
    sources = _with(
        "decorator.py",
        "def _sweep_held_here() -> bool:\n",
        "def _sixth() -> None:\n    if _sweep_held_here():\n        return\n"
        "    with _sweep_hold:\n        pass\n\n\ndef _sweep_held_here() -> bool:\n",
    )
    assert check_hold_sites(sources) == [Finding(UNLISTED, "['_sixth']")]


def test_a_check_after_the_hold_does_not_count() -> None:
    """Asking only after the ``with`` is the same as not asking."""
    sources = {
        "x.py": "def _flush():\n    with _sweep_hold:\n        pass\n    _sweep_held_here()\n"
    }
    table = {"_flush": "test"}
    assert check_hold_sites(sources, table) == [Finding(UNCHECKED, "x.py:_flush")]
