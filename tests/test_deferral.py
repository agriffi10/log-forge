"""SPEC-058 — the one deferral helper, and the AST check that keeps it the only one.

The helper carries the deferral rule: a per-event drop decided during an ``emit`` is counted and
announced only when that ``emit`` returns normally. The behavioural tests below pin the helper's
own contract; each site's tests pin that the site uses it. The AST check
(:func:`deferral_findings`) is the instrument FR-002 names for "a second accumulate-then-commit is
refused", and it is a gate, so it carries a fixture corpus asserting its failure text — silent
cases included — beside the run over the real tree.
"""

from __future__ import annotations

import ast
import threading
from pathlib import Path
from typing import Self

import pytest

import log_foundry
from log_foundry.sinks._deferral import Deferral, wrapped_losses
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

_SRC = Path(log_foundry.__file__).parent


class RecordingLock:
    """A real lock that records, at each acquire and release, what the owner's counter held."""

    def __init__(self, owner: Owner) -> None:
        self._lock = threading.Lock()
        self._owner = owner
        self.events: list[tuple[str, int]] = []

    def __enter__(self) -> Self:
        self._lock.acquire()
        self.events.append(("acquire", self._owner.dropped))
        return self

    def __exit__(self, *exc: object) -> bool:
        self.events.append(("release", self._owner.dropped))
        self._lock.release()
        return False

    def locked(self) -> bool:
        return self._lock.locked()


class Owner:
    """The smallest owner the helper accepts: two counters and the lock guarding them."""

    def __init__(self) -> None:
        self.dropped = 0
        self.failed = 0
        self._counter_lock = RecordingLock(self)


# --- the helper's own contract ------------------------------------------------------------


def test_a_clean_exit_commits_counts_then_lines() -> None:
    """Counts land inside the owner's lock; lines run after it, in registration order."""
    owner = Owner()
    seen: list[tuple[str, int, bool]] = []

    with Deferral(owner) as pending:
        pending.count("dropped", 2)
        pending.count("dropped")
        pending.count("failed", 4)
        pending.announce(
            lambda: seen.append(("first", owner.dropped, owner._counter_lock.locked()))
        )
        pending.announce(
            lambda: seen.append(("second", owner.dropped, owner._counter_lock.locked()))
        )
        assert owner.dropped == 0, "nothing may land before the block exits"
        assert seen == [], "nothing may be announced before the block exits"

    assert (owner.dropped, owner.failed) == (3, 4)
    assert owner._counter_lock.events == [("acquire", 0), ("release", 3)], (
        "the counts must be written inside the lock: 0 when it was taken, 3 when it was released"
    )
    assert seen == [("first", 3, False), ("second", 3, False)], (
        "each line runs after the commit, outside the lock, in the order it was registered"
    )


@pytest.mark.parametrize(
    "error",
    [ValueError("boom"), SinkDeliveryError("none landed"), KeyboardInterrupt()],
    ids=["ValueError", "SinkDeliveryError", "KeyboardInterrupt"],
)
def test_a_raising_exit_commits_nothing_and_does_not_suppress(error: BaseException) -> None:
    """A raise of any kind discards the ledger and propagates by identity."""
    owner = Owner()
    seen: list[str] = []

    with pytest.raises(type(error)) as caught, Deferral(owner) as pending:
        pending.count("dropped", 5)
        pending.announce(lambda: seen.append("line"))
        raise error

    assert caught.value is error, "the exception must leave the block as the same object"
    assert (owner.dropped, owner.failed) == (0, 0), "a raising call counts nothing"
    assert seen == [], "and announces nothing"
    assert owner._counter_lock.events == [], "nor takes the owner's lock"


def test_an_early_return_inside_the_block_still_commits() -> None:
    """A ``return`` from inside the ``with`` is a clean exit, which is how two sites settle."""
    owner = Owner()

    def emit() -> str:
        with Deferral(owner) as pending:
            pending.count("dropped")
            return "returned"
        raise AssertionError("unreachable")  # pragma: no cover

    assert emit() == "returned"
    assert owner.dropped == 1


def test_nothing_pending_takes_no_lock() -> None:
    """A clean exit with no counts never touches the lock — a zero count included."""
    owner = Owner()
    seen: list[str] = []

    with Deferral(owner) as pending:
        pending.count("dropped", 0)
        pending.announce(lambda: seen.append("line"))

    assert owner._counter_lock.events == [], "nothing pending, so no acquisition"
    assert seen == ["line"], "a line registered without a count is still written"


def test_the_lock_is_read_from_the_owner_at_commit_time() -> None:
    """A lock rebound mid-call — what a fork repair does — is the one the commit takes."""
    owner = Owner()
    original = owner._counter_lock
    replacement = RecordingLock(owner)

    with Deferral(owner) as pending:
        pending.count("dropped")
        owner._counter_lock = replacement

    assert original.events == [], "the lock the owner held at construction must not be taken"
    assert replacement.events == [("acquire", 0), ("release", 1)]


def test_a_misspelt_counter_fails_loudly_on_the_first_clean_call() -> None:
    """The cost of naming counters by string, stated: a typo raises rather than counting nowhere."""
    owner = Owner()

    with pytest.raises(AttributeError, match="droped"), Deferral(owner) as pending:
        pending.count("droped")


@pytest.mark.parametrize(
    ("inner", "own", "expected"),
    [
        (None, 0, None),
        (None, 3, SinkLosses(dropped=3, failed=0)),
        (SinkLosses(dropped=2, failed=5), 3, SinkLosses(dropped=5, failed=5)),
        (SinkLosses(dropped=0, failed=0), 0, SinkLosses(dropped=0, failed=0)),
    ],
    ids=["none-0", "none-n", "ab-n", "zero-0"],
)
def test_wrapped_losses_rule(
    inner: SinkLosses | None, own: int, expected: SinkLosses | None
) -> None:
    """FR-003's four shapes: ``None`` only for an inner ``None`` and nothing of the wrapper's own."""
    assert wrapped_losses(inner, own) == expected


# --- the AST check: one helper, and no second accumulate-then-commit ----------------------

_HELPER_MODULE = "log_foundry.sinks._deferral"

DEFERRING_SITES: dict[str, dict[str, tuple[str, ...]]] = {
    "sinks/transform.py": {"TransformSink": ("emit",)},
    "sinks/filtering.py": {"FilteringSink": ("emit",)},
    "sinks/multi.py": {"MultiSink": ("emit",)},
    "sinks/sqs.py": {"SQSSink": ("emit", "_chunks")},
    "sinks/http.py": {"HTTPSink": ("emit", "_items")},
}
"""The functions in each site that decide a per-event drop, by module, class and name.

A class or function named here and missing from its module is a finding, not a skip: a rename
would otherwise shrink the guard as silently as a deletion.
"""

IMMEDIATE: dict[tuple[str, str], frozenset[str]] = {("MultiSink", "emit"): frozenset({"failed"})}
"""Stores the spec keeps immediate on purpose, and the only ones.

``MultiSink.failed`` counts child *calls* that raised, outside ``losses()``, and FR-007 keeps it
moving per call exactly as before. Nothing else in a deferring function may store to ``self``.
"""


class _Scope(ast.NodeVisitor):
    """Collects ``Deferral(...)`` calls, and which of them are ``with`` items inside an ``emit``."""

    def __init__(self) -> None:
        self.functions: list[str] = []
        self.calls: list[ast.Call] = []
        self.blessed: set[int] = set()

    def _function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        self.functions.append(node.name)
        self.generic_visit(node)
        self.functions.pop()

    visit_FunctionDef = _function
    visit_AsyncFunctionDef = _function

    def visit_With(self, node: ast.With) -> None:
        if self.functions and self.functions[-1] == "emit":
            for item in node.items:
                if _is_deferral_call(item.context_expr):
                    self.blessed.add(id(item.context_expr))
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        if _is_deferral_call(node):
            self.calls.append(node)
        self.generic_visit(node)


def _is_deferral_call(node: ast.AST) -> bool:
    """True for a call to ``Deferral`` however it is reached — a bare name or an attribute."""
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Name):
        return func.id == "Deferral"
    return isinstance(func, ast.Attribute) and func.attr == "Deferral"


def _imports_the_helper(tree: ast.Module) -> bool:
    """True when the module imports ``Deferral`` from the helper's own module."""
    return any(
        isinstance(node, ast.ImportFrom)
        and node.module == _HELPER_MODULE
        and any(alias.name == "Deferral" for alias in node.names)
        for node in ast.walk(tree)
    )


def _method(tree: ast.Module, cls_name: str, name: str) -> ast.ClassDef | ast.FunctionDef | None:
    """The class node, or its named method, or ``None`` when either is missing."""
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == cls_name:
            if not name:
                return node
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name == name:
                    return item
    return None


def _self_path(node: ast.expr) -> str | None:
    """``"a.b"`` for a target spelt ``self.a.b``, else ``None``."""
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name) and node.id == "self" and parts:
        return ".".join(reversed(parts))
    return None


def _flatten(target: ast.expr) -> list[ast.expr]:
    """A store target with any tuple, list or starred unpacking opened out."""
    if isinstance(target, ast.Tuple | ast.List):
        return [leaf for element in target.elts for leaf in _flatten(element)]
    if isinstance(target, ast.Starred):
        return _flatten(target.value)
    return [target]


def _self_stores(fn: ast.FunctionDef) -> list[tuple[str, int]]:
    """Every store to an attribute reached through ``self`` in ``fn``, with its line."""
    stores: list[tuple[str, int]] = []
    for node in ast.walk(fn):
        targets: list[ast.expr] = []
        if isinstance(node, ast.Assign):
            targets = node.targets
        elif isinstance(node, ast.AugAssign | ast.AnnAssign):
            targets = [node.target]
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "setattr"
            and node.args
            and isinstance(node.args[0], ast.Name)
            and node.args[0].id == "self"
        ):
            name = node.args[1] if len(node.args) > 1 else None
            attr = name.value if isinstance(name, ast.Constant) else "<dynamic>"
            stores.append((str(attr), node.lineno))
            continue
        for target in targets:
            for leaf in _flatten(target):
                path = _self_path(leaf)
                if path is not None:
                    stores.append((path, leaf.lineno))
    return stores


def deferral_findings(
    where: str,
    source: str,
    sites: dict[str, tuple[str, ...]] | None,
    immediate: dict[tuple[str, str], frozenset[str]],
) -> list[str]:
    """Every way one module breaks the one-helper rule, as lines a reader can act on.

    Four checks. Everywhere: a ``Deferral(...)`` is constructed only as a ``with`` item inside an
    ``emit``, which is what keeps one call's pending drops local to that call. In a named site
    only: the module imports ``Deferral`` from the helper's module; every named class and function
    exists; each named ``emit`` enters a ``Deferral``; and no named function stores to ``self``
    except the immediate stores ``immediate`` lists — a store there is a second ledger committed
    beside the helper's, which is the defect FR-002 refuses.

    Args:
      where: The module's path, for the message.
      source: The module's text.
      sites: The classes and functions this module must keep deferred, or ``None`` when it is
        not a deferring site.
      immediate: Stores permitted per ``(class, function)``.

    Returns:
      The findings, empty when the module complies.
    """
    tree = ast.parse(source)
    findings: list[str] = []
    scope = _Scope()
    scope.visit(tree)
    findings.extend(
        f"{where}:{call.lineno}: Deferral(...) built outside a `with` item in an emit; "
        f"one call's pending drops must stay local to that call"
        for call in scope.calls
        if id(call) not in scope.blessed
    )
    if sites is None:
        return findings
    if not _imports_the_helper(tree):
        findings.append(f"{where}: does not import Deferral from {_HELPER_MODULE}")
    for cls_name, functions in sites.items():
        if _method(tree, cls_name, "") is None:
            findings.append(f"{where}: class {cls_name} not found; the site list is stale")
            continue
        for name in functions:
            fn = _method(tree, cls_name, name)
            if not isinstance(fn, ast.FunctionDef):
                findings.append(f"{where}: {cls_name}.{name} not found; the site list is stale")
                continue
            if name == "emit" and not any(
                isinstance(node, ast.With)
                and any(_is_deferral_call(item.context_expr) for item in node.items)
                for node in ast.walk(fn)
            ):
                findings.append(f"{where}: {cls_name}.emit enters no Deferral")
            allowed = immediate.get((cls_name, name), frozenset())
            findings.extend(
                f"{where}:{line}: {cls_name}.{name} stores self.{attr} directly; "
                f"count it through the Deferral"
                for attr, line in _self_stores(fn)
                if attr not in allowed
            )
    return findings


_IMPORT = "from log_foundry.sinks._deferral import Deferral\n"

_DEFERRED_EMIT = """
class Sink:
    def emit(self, batch):
        with Deferral(self) as pending:
            for event in batch:
                pending.count("dropped")
"""

_BUILT_OUTSIDE = (
    "Deferral(...) built outside a `with` item in an emit; one call's pending drops must stay "
    "local to that call"
)
_STORES = "directly; count it through the Deferral"

_ONE_EMIT = {"Sink": ("emit",)}

_CORPUS: dict[str, tuple[str, dict[str, tuple[str, ...]] | None, list[str]]] = {
    "helper-only emit": (_IMPORT + _DEFERRED_EMIT, _ONE_EMIT, []),
    "inline +=": (
        _IMPORT
        + """
class Sink:
    def emit(self, batch):
        with Deferral(self) as pending:
            self.dropped += len(batch)
""",
        _ONE_EMIT,
        [f"m.py:6: Sink.emit stores self.dropped {_STORES}"],
    ),
    "a locked += is still a second ledger": (
        _IMPORT
        + """
class Sink:
    def emit(self, batch):
        with Deferral(self) as pending:
            pass
        with self._counter_lock:
            self.dropped_oversized += 1
""",
        _ONE_EMIT,
        [f"m.py:8: Sink.emit stores self.dropped_oversized {_STORES}"],
    ),
    "a tuple target and a setattr are stores too": (
        _IMPORT
        + """
class Sink:
    def emit(self, batch):
        with Deferral(self) as pending:
            self.a, (self.stats.b, *rest) = 1, (2, 3)
            setattr(self, "c", 4)
""",
        _ONE_EMIT,
        [
            f"m.py:6: Sink.emit stores self.a {_STORES}",
            f"m.py:6: Sink.emit stores self.stats.b {_STORES}",
            f"m.py:7: Sink.emit stores self.c {_STORES}",
        ],
    ),
    "MultiSink's call counter is immediate on purpose": (
        _IMPORT
        + """
class MultiSink:
    def emit(self, batch):
        with Deferral(self) as pending:
            with self._counter_lock:
                self.failed += 1
""",
        {"MultiSink": ("emit",)},
        [],
    ),
    "the exemption is MultiSink's alone": (
        _IMPORT
        + """
class Sink:
    def emit(self, batch):
        with Deferral(self) as pending:
            self.failed += 1
""",
        _ONE_EMIT,
        [f"m.py:6: Sink.emit stores self.failed {_STORES}"],
    ),
    "a helper the emit delegates to is checked by name": (
        _IMPORT
        + _DEFERRED_EMIT
        + """
    def _items(self, batch, pending):
        self.dropped_oversized += 1
""",
        {"Sink": ("emit", "_items")},
        [f"m.py:10: Sink._items stores self.dropped_oversized {_STORES}"],
    ),
    "missing import": (
        _DEFERRED_EMIT,
        _ONE_EMIT,
        ["m.py: does not import Deferral from log_foundry.sinks._deferral"],
    ),
    "an emit that enters no Deferral": (
        _IMPORT
        + """
class Sink:
    def emit(self, batch):
        for event in batch:
            pass
""",
        _ONE_EMIT,
        ["m.py: Sink.emit enters no Deferral"],
    ),
    "a stored Deferral": (
        _IMPORT
        + """
class Sink:
    def emit(self, batch):
        self._pending = Deferral(self)
        with Deferral(self) as pending:
            pass
""",
        _ONE_EMIT,
        [f"m.py:5: {_BUILT_OUTSIDE}", f"m.py:5: Sink.emit stores self._pending {_STORES}"],
    ),
    "a with item outside an emit": (
        _IMPORT
        + _DEFERRED_EMIT
        + """
    def flush(self):
        with Deferral(self):
            pass
""",
        _ONE_EMIT,
        [f"m.py:10: {_BUILT_OUTSIDE}"],
    ),
    "a stale site list": (
        _IMPORT + _DEFERRED_EMIT.replace("def emit", "def send"),
        {"Sink": ("emit", "_items")},
        [
            f"m.py:5: {_BUILT_OUTSIDE}",
            "m.py: Sink.emit not found; the site list is stale",
            "m.py: Sink._items not found; the site list is stale",
        ],
    ),
    "a renamed class": (
        _IMPORT + _DEFERRED_EMIT.replace("class Sink", "class Renamed"),
        _ONE_EMIT,
        ["m.py: class Sink not found; the site list is stale"],
    ),
    "a construction spelt through the module is still a construction": (
        _IMPORT
        + """
import log_foundry.sinks._deferral as deferral
class Sink:
    def emit(self, batch):
        with deferral.Deferral(self) as pending:
            pass
    def flush(self):
        deferral.Deferral(self)
""",
        _ONE_EMIT,
        [f"m.py:9: {_BUILT_OUTSIDE}"],
    ),
    "a module that is not a site is held only to the construction rule": (
        _IMPORT
        + """
class Other:
    def emit(self, batch):
        self.n += 1
        Deferral(self)
""",
        None,
        [f"m.py:6: {_BUILT_OUTSIDE}"],
    ),
}


@pytest.mark.parametrize("case", sorted(_CORPUS))
def test_the_deferral_lint_reports_what_it_claims_to(case: str) -> None:
    """Each check's failure text, and the shapes that must stay silent, on synthetic modules."""
    source, sites, expected = _CORPUS[case]
    assert deferral_findings("m.py", source, sites, IMMEDIATE) == expected


def test_every_deferring_site_takes_the_one_helper() -> None:
    """FR-002's instrument, over the real package: five sites, one helper, no second ledger.

    The construction rule runs over every module under ``src/``, so a sixth site that builds a
    ``Deferral`` is held to it before anyone adds it to :data:`DEFERRING_SITES`; the other three
    checks run on the five sites by name, and a site that has moved is a finding.
    """
    modules = {path.relative_to(_SRC).as_posix(): path for path in sorted(_SRC.rglob("*.py"))}
    assert set(DEFERRING_SITES) <= set(modules), (
        f"a deferring site's module is gone: {sorted(set(DEFERRING_SITES) - set(modules))}"
    )
    findings: list[str] = []
    for where, path in modules.items():
        source = path.read_text(encoding="utf-8")
        findings.extend(deferral_findings(where, source, DEFERRING_SITES.get(where), IMMEDIATE))
    assert findings == [], "\n".join(findings)
