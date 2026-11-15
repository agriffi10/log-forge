"""SPEC-088 FR-005 AC-3: no ledger counter is written from two threads outside a lock.

The rule that keeps every count exact is that a counter is written by one thread only, or under
a lock its site already holds. This derives every write of a ledger counter from ``src/`` and
classifies it as one of the shapes the rule admits. A write that is none of them fails by name, so
the next counter added to a shared path has to take a lock or a cell, or argue its case here.

**The admitted shapes.**

- *owner cell*: a write through ``_ledger.cell()``, or through a local bound from it in the same
  function, which only the calling thread's cell can be.
- *drain only*: a write on ``self`` inside one of :data:`DRAIN_ONLY`, ``Worker`` methods that run
  only on the drain thread; the gate also proves every call of one of them in ``src/`` comes from
  another of them, so a public method cannot reach one.
- *construction*: a write on ``self`` in the owning class's ``__init__``, or in
  ``Worker._reinit_after_fork``, which runs on a forked child's only thread; a module-level
  binding at import; and, in ``_ledger.py``, a write to a cell built in the same function and not
  yet published.
- *queue put*: ``self.entered`` inside ``_CountingQueue._put``, under the queue's mutex.
- *locked*: a write inside a ``with`` whose expression is a lock (:data:`LOCKS`).

Anything else with a ledger counter's name is refused unless :data:`NOT_LEDGER` names it with a
reason: the same name on an object that is not part of the ledger.
"""

from __future__ import annotations

import ast
import pathlib
from dataclasses import dataclass

import pytest

import log_foundry

_SRC = pathlib.Path(log_foundry.__file__).resolve().parent

CELL_COUNTERS = frozenset(
    {"span", "handed", "orphan", "orphan_open", "delivered", "skipped", "sink_lost"}
)
WORKER_COUNTERS = frozenset(
    {
        "_in_flight", "_taken", "_delivered", "_skipped", "_sink_lost", "_batch_lost",
        "_drain_lost", "_overflow_lost", "_handed_at_build",
    }
)
QUEUE_COUNTERS = frozenset({"entered"})
GLOBAL_COUNTERS = frozenset({"_orphan_lost", "_in_span_lost"})
DRAIN_ONLY = frozenset(
    {"_run", "_drain", "_final_drain", "_emit_pending", "_emit", "_terminal_failure", "_hold",
     "_settle"}
)
LOCKS = frozenset({"_lock", "_loss_lock", "lock", "mutex"})
CONSTRUCTION = frozenset({"__init__", "_reinit_after_fork"})

LEDGER_FILES = frozenset({"_ledger.py", "worker.py", "decorator.py", "api.py", "_lifecycle.py"})
"""The modules that hold or write a ledger counter. Every module is read for the worker's and the
two global counters' names, which are distinctive; the cells' short names (``span``,
``delivered``) and ``setattr`` are read only here, where a cell can be in reach, and the drain-only
calls only in ``worker.py``, whose ``Worker`` owns them. A sink's own ``self.skipped`` is not the
ledger's."""

NOT_LEDGER = {
    ("worker.py", "_FlushMarker.__init__", "self.delivered"): "a flush marker's verdict",
    ("worker.py", "Worker._drain", "item.delivered"): "a flush marker's verdict",
    ("worker.py", "Worker._final_drain", "marker.delivered"): "a flush marker's verdict",
    ("_lifecycle.py", "_CloseRegistry.__init__", "self._in_flight"): "the closes in flight",
}

UNADMITTED = "a ledger counter is written outside every admitted shape"
DRAIN_LEAK = "a drain-only method is called from outside the drain-only set"
STALE = "a NOT_LEDGER entry names no write"


@dataclass(frozen=True)
class Finding:
    """One refusal: its reason and the site."""

    reason: str
    site: tuple[str, str, str]


def _is_cell_call(node: ast.AST) -> bool:
    """Whether ``node`` is ``_ledger.cell()`` or a bare ``cell()``."""
    if not isinstance(node, ast.Call) or node.args or node.keywords:
        return False
    func = node.func
    if isinstance(func, ast.Attribute):
        return func.attr == "cell" and isinstance(func.value, ast.Name) and func.value.id == "_ledger"
    return isinstance(func, ast.Name) and func.id == "cell"


def _is_fresh_cell(node: ast.AST) -> bool:
    """Whether ``node`` is ``_Cell()``, a cell nothing has published yet."""
    return isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "_Cell"


def _lock_name(node: ast.AST) -> str | None:
    """The last name of a ``with`` expression, or ``None``."""
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Name):
        return node.id
    return None


class _Walker(ast.NodeVisitor):
    """Collects every ledger-counter write with what it needs to be classified."""

    def __init__(self, file: str) -> None:
        self.file = file
        self.scope: list[str] = []
        self.classes: list[str] = []
        self.locks = 0
        self.bound: list[dict[str, str]] = [{}]
        self.writes: list[tuple[str, str, str, str, bool, str]] = []
        self.calls: list[tuple[str, str]] = []

    def _qual(self) -> str:
        return ".".join(self.scope) or "<module>"

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.scope.append(node.name)
        self.classes.append(node.name)
        self.generic_visit(node)
        self.classes.pop()
        self.scope.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.scope.append(node.name)
        self.bound.append({})
        held, self.locks = self.locks, 0
        self.generic_visit(node)
        self.locks = held
        self.bound.pop()
        self.scope.pop()

    visit_AsyncFunctionDef = visit_FunctionDef  # type: ignore[assignment]

    def visit_With(self, node: ast.With) -> None:
        locked = any(_lock_name(item.context_expr) in LOCKS for item in node.items)
        self.locks += locked
        self.generic_visit(node)
        self.locks -= locked

    def _bind(self, target: ast.AST, value: ast.AST) -> None:
        if isinstance(target, ast.Name):
            if _is_cell_call(value):
                self.bound[-1][target.id] = "cell"
            elif _is_fresh_cell(value):
                self.bound[-1][target.id] = "fresh"

    def _record(self, target: ast.AST) -> None:
        if isinstance(target, ast.Attribute):
            name = target.attr
            receiver = target.value
            if _is_cell_call(receiver):
                kind = "cell"
            elif isinstance(receiver, ast.Name):
                kind = self.bound[-1].get(receiver.id, receiver.id)
            else:
                kind = "?"
            if name in CELL_COUNTERS | WORKER_COUNTERS | QUEUE_COUNTERS:
                self.writes.append(
                    (self._qual(), ast.unparse(target), name, kind, bool(self.locks),
                     self.classes[-1] if self.classes else "")
                )
        elif isinstance(target, ast.Name) and target.id in GLOBAL_COUNTERS:
            self.writes.append(
                (self._qual(), target.id, target.id, "global", bool(self.locks),
                 self.classes[-1] if self.classes else "")
            )
        elif isinstance(target, ast.Tuple):
            for element in target.elts:
                self._record(element)

    def visit_Assign(self, node: ast.Assign) -> None:
        for target in node.targets:
            self._bind(target, node.value)
            self._record(target)
        self.generic_visit(node)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        self._record(node.target)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if node.value is not None:
            self._bind(node.target, node.value)
            self._record(node.target)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        if isinstance(func, ast.Name) and func.id == "setattr" and node.args:
            receiver = node.args[0]
            kind = self.bound[-1].get(receiver.id, "?") if isinstance(receiver, ast.Name) else "?"
            self.writes.append(
                (self._qual(), ast.unparse(node), "setattr", kind, bool(self.locks),
                 self.classes[-1] if self.classes else "")
            )
        if (
            isinstance(func, ast.Attribute)
            and isinstance(func.value, ast.Name)
            and func.value.id == "self"
            and func.attr in DRAIN_ONLY
        ):
            self.calls.append((self._qual(), func.attr))
        self.generic_visit(node)


def _admitted(file: str, write: tuple[str, str, str, str, bool, str]) -> bool:
    """Whether one write is one of the admitted shapes."""
    qual, _text, name, kind, locked, owner = write
    method = qual.rsplit(".", 1)[-1]
    if locked:
        return True
    if kind == "cell":
        return True
    if kind == "fresh" and file == "_ledger.py":
        return True
    if qual == "<module>" and kind == "global":
        return True
    if kind == "self" and name in WORKER_COUNTERS and owner == "Worker":
        return method in DRAIN_ONLY or method in CONSTRUCTION
    if kind == "self" and name in QUEUE_COUNTERS and owner == "_CountingQueue":
        return method in {"_put", "__init__"}
    if kind == "self" and name in CELL_COUNTERS and owner == "_Cell" and file == "_ledger.py":
        return method == "__init__"
    return False


def check(sources: dict[str, str], exempt: dict | None = None) -> list[Finding]:
    """Classifies every ledger-counter write in ``sources`` and returns what the rule refuses.

    Args:
      sources: Source text keyed by its path under the package.
      exempt: The non-ledger writes to admit, :data:`NOT_LEDGER` unless given.

    Returns:
      One finding per refused write, drain-only leak or stale exemption.
    """
    exempt = NOT_LEDGER if exempt is None else exempt
    findings: list[Finding] = []
    seen_exempt: set[tuple[str, str, str]] = set()
    for file, text in sorted(sources.items()):
        walker = _Walker(file)
        walker.visit(ast.parse(text))
        ledger_file = file in LEDGER_FILES
        for write in walker.writes:
            if not ledger_file and write[2] not in WORKER_COUNTERS | GLOBAL_COUNTERS:
                continue
            site = (file, write[0], write[1])
            if site in exempt:
                seen_exempt.add(site)
                continue
            if not _admitted(file, write):
                findings.append(Finding(UNADMITTED, site))
        for qual, called in walker.calls if file == "worker.py" else ():
            caller = qual.rsplit(".", 1)[-1]
            if caller not in DRAIN_ONLY:
                findings.append(Finding(DRAIN_LEAK, (file, qual, called)))
    findings.extend(
        Finding(STALE, site)
        for site in exempt
        if site not in seen_exempt and site[0] in sources
    )
    return findings


def _live() -> dict[str, str]:
    return {
        str(path.relative_to(_SRC)): path.read_text(encoding="utf-8")
        for path in sorted(_SRC.rglob("*.py"))
    }


def test_every_ledger_write_is_single_writer_or_locked() -> None:
    """FR-005 AC-3 over the tree as it is."""
    findings = check(_live())
    assert not findings, "\n".join(f"{f.reason}: {f.site}" for f in findings)


def test_the_gate_reads_the_writes_it_exists_for() -> None:
    """The live tree has writes of every admitted shape, so the gate is not reading nothing."""
    kinds: set[str] = set()
    for file, text in _live().items():
        walker = _Walker(file)
        walker.visit(ast.parse(text))
        for qual, _text, _name, kind, locked, _owner in walker.writes:
            if locked:
                kinds.add("locked")
            elif kind in {"cell", "fresh", "global"}:
                kinds.add(kind)
            elif qual.rsplit(".", 1)[-1] in DRAIN_ONLY:
                kinds.add("drain")
            elif qual.endswith("._put"):
                kinds.add("put")
    assert kinds >= {"locked", "cell", "fresh", "global", "drain", "put"}, kinds


_WORKER = '''
class Worker:
    def __init__(self):
        self._taken = 0

    def _drain(self):
        self._hold()

    def _hold(self):
        self._taken += 1

    def submit(self):
        {body}
'''

REFUSED = {
    "a counter written in a public method": (
        {"worker.py": _WORKER.format(body="self._taken += 1")},
        UNADMITTED,
    ),
    "a drain-only method called from a public one": (
        {"worker.py": _WORKER.format(body="self._hold()")},
        DRAIN_LEAK,
    ),
    "a with on something that is not a lock": (
        {"worker.py": _WORKER.format(body="with self._sink:\n            self._taken += 1")},
        UNADMITTED,
    ),
    "a cell counter written through another object": (
        {"api.py": "def log(other):\n    other.span += 1\n"},
        UNADMITTED,
    ),
    "a global counter written in a function outside its lock": (
        {"decorator.py": "_in_span_lost = 0\ndef note():\n    global _in_span_lost\n    _in_span_lost += 1\n"},
        UNADMITTED,
    ),
    "a queue count written outside _put": (
        {"worker.py": "class _CountingQueue:\n    def get(self):\n        self.entered -= 1\n"},
        UNADMITTED,
    ),
    "a setattr on a published cell": (
        {"_ledger.py": "def fold(cells):\n    for one in cells:\n        setattr(one, 'span', 0)\n"},
        UNADMITTED,
    ),
}

_MARKER_SITE = ("worker.py", "_FlushMarker.__init__", "self.delivered")
_STALE_SOURCE = {"worker.py": "class _FlushMarker:\n    def __init__(self):\n        self.verdict = False\n"}

SILENT = {
    "a write through _ledger.cell()": {"api.py": "def log():\n    _ledger.cell().span += 1\n"},
    "a write through a local bound from cell()": {
        "api.py": "def log():\n    c = _ledger.cell()\n    c.orphan += 1\n    c.orphan_open -= 1\n"
    },
    "a drain-only write reached only from the drain": {"worker.py": _WORKER.format(body="pass")},
    "a write under a lock": {
        "worker.py": _WORKER.format(body="with self._lock:\n            self._overflow_lost += 1")
    },
    "a queue count in _put": {
        "worker.py": "class _CountingQueue:\n    def _put(self, item):\n        self.entered += 1\n"
    },
    "a fresh cell built in _ledger": {
        "_ledger.py": "def fold():\n    merged = _Cell()\n    setattr(merged, 'span', 1)\n    merged.handed = 2\n"
    },
    "a global under its lock": {
        "decorator.py": "_orphan_lost = 0\ndef note():\n    global _orphan_lost\n    with _loss_lock:\n        _orphan_lost += 1\n"
    },
}

_UNRELATED_SOURCE = {
    "worker.py": "class _FlushMarker:\n    def __init__(self):\n        self.delivered = False\n"
}


@pytest.mark.parametrize("case", sorted(REFUSED))
def test_each_check_refuses_its_shape(case: str) -> None:
    """Each check fires for its reason, and only that reason."""
    sources, reason = REFUSED[case]
    reasons = {finding.reason for finding in check(sources, {})}
    assert reasons == {reason}, (case, reasons)


def test_an_exemption_naming_no_write_is_stale() -> None:
    """An exemption whose write is gone fails, so a rename cannot leave one admitting nothing."""
    findings = check(_STALE_SOURCE, {_MARKER_SITE: "a flush marker's verdict"})
    assert [finding.reason for finding in findings] == [STALE]


def test_a_same_named_attribute_is_refused_until_it_is_exempted() -> None:
    """A marker's ``delivered`` is not a ledger count, and only its exemption admits it."""
    assert [finding.reason for finding in check(_UNRELATED_SOURCE, {})] == [UNADMITTED]
    assert check(_UNRELATED_SOURCE, {_MARKER_SITE: "a flush marker's verdict"}) == []


@pytest.mark.parametrize("case", sorted(SILENT))
def test_these_shapes_are_silent(case: str) -> None:
    """The admitted shapes pass, so a refusal above is the shape's and not the harness's."""
    assert check(SILENT[case], {}) == [], case
