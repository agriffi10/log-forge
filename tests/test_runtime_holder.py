"""SPEC-077 FR-001 and FR-005: one holder for the lifecycle bookkeeping, and nothing public moved.

The lock-site literal below was derived on the build's base commit, ``3d65cf32``, from the
unmodified ``_lifecycle.py`` and ``worker.py`` (the spec's FR-001 criterion: not the set at
``fda134a``, since SPEC-072 rewrote ``Worker._emit`` in between). A moved lock is written under its
new path, ``_state.<name>``. It is held as a **multiset** of (enclosing function, lock), which
implies the spec's set equality and also catches a second identical acquisition added to one
function — the shape the pinned lock counts in ``tests/test_runtime_state.py`` measure at runtime.
"""

from __future__ import annotations

import ast
import collections
import pathlib
import re

import pytest

import log_foundry
from log_foundry import _lifecycle
from test_lifecycle_inventory import _NESTED_BINDINGS, module_bindings, module_scope

_SRC = pathlib.Path(log_foundry.__file__).resolve().parent

MOVED = (
    "_closers",
    "_closers_lock",
    "_closing_now",
    "_closing_now_lock",
    "_owned",
    "_owned_lock",
    "_marking_failed",
)

LOCK_SITES_AT_BASE: dict[str, collections.Counter[tuple[str, str]]] = {
    "_lifecycle.py": collections.Counter(
        {
            ("_closing", "_state._closing_now_lock"): 1,
            ("_register_closing", "_state._closing_now_lock"): 1,
            ("_finish_closing", "_state._closing_now_lock"): 1,
            ("_clear_after_fork", "_state._closing_now_lock"): 1,
            ("_inheritance_roots", "_state._owned_lock"): 1,
            ("_mark_inherited", "_state._owned_lock"): 1,
            ("reclaim", "_state._owned_lock"): 1,
            ("stamp", "_state._owned_lock"): 1,
            ("releasable", "_state._owned_lock"): 1,
            ("_start_closer", "_state._closers_lock"): 1,
            ("join_closers", "_state._closers_lock"): 1,
            ("closing_count", "_state._closers_lock"): 1,
            ("_get_worker", "_state._lock"): 1,
            ("_note_orphan_emit", "_state._lock"): 1,
            ("_close_owed", "_state._lock"): 2,
            ("_close_owed", "_state._closing_now_lock"): 1,
            ("_shutdown_worker", "_state._lock"): 3,
            ("_swap_sink", "_state._lock"): 2,
            ("_flush_live_sink", "_state._lock"): 1,
        }
    ),
    "worker.py": collections.Counter(
        {
            ("Worker.submit", "self._lock"): 1,
            ("Worker._count_undeliverable", "self._lock"): 1,
            ("Worker.health", "self._lock"): 1,
            ("Worker.flush", "self._lock"): 1,
            ("Worker.retarget", "self._lock"): 3,
            ("Worker._record_incomplete_swap", "self._lock"): 1,
            ("Worker.claim_stop", "self._lock"): 1,
            ("Worker.stop", "self._lock"): 1,
            ("Worker._queued_submissions", "self._queue.mutex"): 1,
            ("Worker._take_marker", "self._lock"): 1,
            ("Worker._release_marker", "self._lock"): 1,
            ("Worker._release_waiters", "self._lock"): 1,
            ("Worker._release_waiters", "self._queue.mutex"): 1,
            ("Worker._terminal_failure", "self._lock"): 1,
            ("Worker._nothing_lost_since", "self._lock"): 1,
            ("Worker._emit", "self._lock"): 3,
        }
    ),
}


def _tree(name: str) -> ast.Module:
    """Parses one module of the package."""
    return ast.parse((_SRC / name).read_text(encoding="utf-8"))


def _with_sites(tree: ast.Module) -> collections.Counter[tuple[str, str]]:
    """Every ``with`` item in a module, as (qualified enclosing function, the lock it takes)."""
    found: collections.Counter[tuple[str, str]] = collections.Counter()

    def visit(node: ast.AST, qual: tuple[str, ...]) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
                visit(child, (*qual, child.name))
                continue
            if isinstance(child, ast.With | ast.AsyncWith):
                for item in child.items:
                    found[(".".join(qual) or "<module>", ast.unparse(item.context_expr))] += 1
            visit(child, qual)

    visit(tree, ())
    return found


def test_runtime_replaces_the_lifecycle_class_and_has_one_instance() -> None:
    """FR-001 AC-1: ``Runtime`` exists, ``_Lifecycle`` does not, and one instance is built."""
    assert hasattr(_lifecycle, "Runtime")
    assert not hasattr(_lifecycle, "_Lifecycle"), "the old name is an alias, a second home"
    assert type(_lifecycle._state) is _lifecycle.Runtime
    builds = [
        (path.name, node.lineno)
        for path in sorted(_SRC.rglob("*.py"))
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name | ast.Attribute)
        and (node.func.id if isinstance(node.func, ast.Name) else node.func.attr) == "Runtime"
    ]
    at_import = [
        node.lineno
        for node in _tree("_lifecycle.py").body
        if isinstance(node, ast.Assign)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Name)
        and node.value.func.id == "Runtime"
    ]
    assert len(builds) == 1 and builds[0][0] == "_lifecycle.py", (
        f"exactly one Runtime() is built in src/: {builds}"
    )
    assert at_import == [builds[0][1]], "and it is the module-level `_state = Runtime()`"


def test_no_moved_name_is_bound_at_module_level() -> None:
    """FR-001 AC-2: the seven names and ``_FORK_SKIP`` are gone from module scope, onto ``self``."""
    tree = _tree("_lifecycle.py")
    leftover = _module_bound(tree) & {*MOVED, "_FORK_SKIP"}
    assert not leftover, f"still bound at module level: {sorted(leftover)}"

    runtime = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "Runtime")
    init = next(n for n in runtime.body if isinstance(n, ast.FunctionDef) and n.name == "__init__")
    on_self = {
        target.attr
        for node in ast.walk(init)
        if isinstance(node, ast.Assign | ast.AnnAssign)
        for target in (node.targets if isinstance(node, ast.Assign) else [node.target])
        if isinstance(target, ast.Attribute)
        and isinstance(target.value, ast.Name)
        and target.value.id == "self"
    }
    missing = set(MOVED) - on_self
    assert not missing, f"not assigned on self in Runtime.__init__: {sorted(missing)}"


def _module_bound(tree: ast.Module) -> set[str]:
    """Every name bound at module scope, compound statements and imports included.

    Through ``test_lifecycle_inventory.module_scope``, the one walker both module-scope scans
    share: reading ``tree.body`` alone, a ``try:`` block re-binding ``_closing_now`` at module level
    survived the whole suite.
    """
    bound = {name for name, _ in module_bindings(tree)}
    for statement in module_scope(tree):
        if isinstance(statement, ast.Import | ast.ImportFrom):
            bound |= {(a.asname or a.name).split(".")[0] for a in statement.names}
    return bound


@pytest.mark.parametrize("kind", sorted(_NESTED_BINDINGS))
def test_the_module_scan_sees_a_binding_under_every_statement_kind(kind: str) -> None:
    """The scan above is not vacuous: a moved name bound under each compound statement is found."""
    source = _NESTED_BINDINGS[kind].replace("_cache", "_closing_now").replace("_i", "_closing_now")
    assert "_closing_now" in _module_bound(ast.parse(source)), source


def test_the_module_scan_sees_an_import_and_ignores_function_bodies() -> None:
    """An import binds at module scope; a function's or class's local does not."""
    assert "_owned" in _module_bound(ast.parse("from x import y as _owned\n"))
    assert not _module_bound(ast.parse("def f():\n    _owned = 1\nclass C:\n    _owned = 2\n")) & {
        "_owned"
    }


def test_the_fork_opt_out_lists_both_pinning_records() -> None:
    """FR-001 AC-3: one class-level declaration names both records that pin superseded sinks.

    The behavioural half — a child of ``configure(A)``, ``info()``, ``configure(B)`` runs no hook on
    A — is ``test_lifecycle_races.test_a_forked_child_does_not_hook_a_superseded_sink``.
    """
    assert _lifecycle.Runtime._FORK_SKIP == ("_owed", "_owned")


def test_the_lock_sites_are_the_base_commits() -> None:
    """FR-001 AC-4: every ``with <lock>:`` site, by function and lock, is the base commit's."""
    for name, expected in LOCK_SITES_AT_BASE.items():
        found = _with_sites(_tree(name))
        added = found - expected
        lost = expected - found
        assert not added and not lost, (
            f"{name}: the lock sites moved from those derived at 3d65cf32 — added "
            f"{dict(added)}, lost {dict(lost)}. Moving a lock changes the lock order, which is "
            "behaviour (SPEC-077 FR-001)"
        )


_STALE_DOC = re.compile(
    r"\b_Lifecycle\b"
    r"|_lifecycle\.(_closers|_closing_now|_owned|_marking_failed|_FORK_SKIP)\b"
    r"|:data:`(_closers|_closers_lock|_closing_now|_closing_now_lock|_owned|_owned_lock"
    r"|_marking_failed|_FORK_SKIP)`"
)


def test_no_docstring_names_a_removed_module_attribute() -> None:
    """FR-001 AC-5: the spec's grep, run as part of the suite, finds nothing in ``src/``."""
    hits = [
        f"{path.relative_to(_SRC)}:{number}: {line.strip()}"
        for path in sorted(_SRC.rglob("*.py"))
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if _STALE_DOC.search(line)
    ]
    assert not hits, "these lines name a module attribute that moved:\n  " + "\n  ".join(hits)


@pytest.mark.parametrize("name", ["Runtime", "State"])
def test_runtime_and_state_are_not_public(name: str) -> None:
    """FR-005 AC-3: neither name is importable from the package or listed in ``__all__``."""
    assert name not in log_foundry.__all__
    with pytest.raises(ImportError):
        exec(f"from log_foundry import {name}", {})  # noqa: S102
