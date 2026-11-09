"""SPEC-058 and SPEC-059 — the one deferral helper, and the AST checks that keep it the only one.

The helper carries the deferral rule: a per-event drop decided during an ``emit`` is counted and
announced only when that ``emit`` returns normally. The behavioural tests below pin the helper's
own contract; each site's tests pin that the site uses it. Two AST checks hold the rule over the
tree. :func:`deferral_findings` is the instrument SPEC-058 FR-002 names for "a second
accumulate-then-commit is refused": it holds each listed *site* to the helper. SPEC-059 FR-005's
:func:`dropped_store_findings` holds each *counter*: an attribute some ``losses()`` reports as
``dropped``, derived from the tree, is written only by a ``Deferral`` commit. Both are gates, so
each carries a fixture corpus asserting its failure text — silent cases included — beside the run
over the real tree.
"""

from __future__ import annotations

import ast
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Self

import pytest

import log_foundry
from log_foundry.sinks._deferral import Deferral, wrapped_losses
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

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


def test_count_for_commits_another_object_under_the_owners_one_acquisition() -> None:
    """SPEC-074 FR-001: a target's counter lands in the same acquisition, after the owner's own."""
    owner = Owner()

    class Target:
        dropped_oversized = 0

    target = Target()
    with Deferral(owner) as pending:
        pending.count("dropped", 1)
        pending.count_for(target, "dropped_oversized", 2)
        pending.count_for(target, "dropped_oversized", 0)
        assert target.dropped_oversized == 0, "nothing may land before the block exits"

    assert (owner.dropped, target.dropped_oversized) == (1, 2)
    assert owner._counter_lock.events == [("acquire", 0), ("release", 1)], "one acquisition"


def test_count_for_alone_still_takes_the_lock_and_a_raise_discards_it() -> None:
    owner = Owner()

    class Target:
        n = 0

    target = Target()
    with pytest.raises(ValueError), Deferral(owner) as pending:
        pending.count_for(target, "n", 3)
        raise ValueError
    assert target.n == 0 and owner._counter_lock.events == []
    with Deferral(owner) as pending:
        pending.count_for(target, "n", 3)
    assert target.n == 3 and len(owner._counter_lock.events) == 2


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

MODULE_LEVEL = "<module>"
"""The :data:`DEFERRING_SITES` key under which a module lists its module-level deferring functions.

SPEC-072's engine commits a whole delivery run in one module function, ``_engine._run``, rather
than in an ``emit``: it is shared by the worker, the orphan path and the inline adapter, so no one
class owns it. SPEC-073's run context adds ``_engine._commit``, which commits the counts a sink
registered on the run's context, one ``Deferral`` per owner, for ``_run`` and for a round run
outside any run. A function listed here is held to what a listed ``emit`` is, with one difference
that follows from having no ``self``: it may make **no** attribute store through any root, its
same-module callees included, since a second ledger there would be a store through the counters
object the ``Deferral`` already commits into.
"""

DEFERRING_SITES: dict[str, dict[str, tuple[str, ...]]] = {
    "sinks/transform.py": {"TransformSink": ("emit",)},
    "sinks/filtering.py": {"FilteringSink": ("emit",)},
    "sinks/multi.py": {"MultiSink": ("emit",)},
    "sinks/_socket.py": {"SocketTransport": ("send_all", "_sendable")},
    "sinks/syslog.py": {"SyslogSink": ("emit",)},
    "sinks/logstash.py": {"LogstashSink": ("emit",)},
    "sinks/kafka.py": {"KafkaSink": ("emit",)},
    "sinks/pubsub.py": {"GooglePubSubSink": ("emit",)},
    "sinks/stdout.py": {"StdoutSink": ("emit",)},
    "sinks/file.py": {"FileSink": ("emit",), "RotatingFileSink": ("emit",)},
    "sinks/sqlite.py": {"SQLiteSink": ("emit",)},
    "sinks/_engine.py": {MODULE_LEVEL: ("_run", "_commit")},
}
"""The functions in each site that decide a per-event drop, by module, class and name.

SPEC-058 listed the first five modules, SPEC-059 the next ten, and SPEC-069 the rest, each of
whose ``emit`` enters a ``Deferral`` for its encode step. SPEC-074 takes each sink it puts on the
delivery engine off the list, as SPEC-073 took ``sinks/http.py``: its drops register on the run
context ``SETTLED``, and its counts commit in ``_engine._run``'s one ``Deferral``. SPEC-073 took ``sinks/http.py`` off the
list: ``HTTPSink`` counts through the delivery engine's run context, whose one commit is
``_engine._commit``, listed below, and whose ``SETTLED`` rule for a dropped counter
:func:`run_context_count_findings` holds. Its two immediate ``failed`` stores went with it. ``SocketTransport`` is the one
entry without an ``emit``: the ledger is entered by ``SyslogSink.emit`` and ``LogstashSink.emit``
and handed to its ``send_all``, so the roster rule below, which starts from an ``emit``, cannot
require it and it is named by hand.

A class or function named here and missing from its module is a finding, not a skip: a rename
would otherwise shrink the guard as silently as a deletion. The other direction is held too: a
class anywhere under ``src/`` whose ``emit`` enters a ``Deferral`` and is not named here with
``emit`` is a finding, so converting a sink to the helper without listing it cannot leave its
old immediate stores unchecked.
"""

IMMEDIATE: dict[tuple[str, str], frozenset[str]] = {
    ("MultiSink", "emit"): frozenset({"failed"}),
    ("SocketTransport", "_send_one"): frozenset({"failed"}),
    ("SocketTransport", "_socket"): frozenset({"_sock"}),
    ("SocketTransport", "_reset"): frozenset({"_sock"}),
    ("GooglePubSubSink", "emit"): frozenset({"failed"}),
    ("GooglePubSubSink", "_resolve"): frozenset({"failed"}),
    ("GooglePubSubSink", "_drain_pending"): frozenset({"failed"}),
    ("GooglePubSubSink", "_reap"): frozenset({"_futures"}),
    ("GooglePubSubSink", "_await_overflow"): frozenset({"_futures"}),
    ("RotatingFileSink", "emit"): frozenset({"_size", "_failed"}),
    ("FileSink", "emit"): frozenset({"_failed"}),
    ("SQLiteSink", "emit"): frozenset({"_failed"}),
    ("RotatingFileSink", "_rotate_or_continue"): frozenset(
        {"_stream", "_size", "_next_rollover", "_rotation_failing"}
    ),
    ("RotatingFileSink", "_rotate"): frozenset({"_stream", "_size", "_next_rollover"}),
    ("RotatingFileSink", "_ensure_open"): frozenset({"_stream", "_size"}),
}
"""The stores the spec keeps immediate on purpose, keyed by the method making them; the only ones.

Derived by running the gate over the tree with this dict empty, not written from memory, and
held exact by :func:`stale_immediate`: an entry no deferring function reaches is a finding. None
may name a counter some ``losses()`` reports as ``dropped`` — that count is exact by promise, and
the tree test of :func:`dropped_store_findings` refuses the overlap — so every entry is ``failed``,
an upper bound, a counter that feeds it, or state that is not a count. Each entry, with why it
counts as it happens rather than through the ``Deferral``:

- ``MultiSink.emit`` / ``failed``: child *calls* that raised, outside ``losses()``; FR-007 keeps
  it moving per call exactly as before.
- ``SocketTransport._send_one`` / ``failed``: a message abandoned past the reconnect bound; the
  same upper bound.
- ``SocketTransport._socket`` and ``SocketTransport._reset`` / ``_sock``: the held socket, opened
  and dropped as the connection comes and goes; state, not a count.
- ``GooglePubSubSink.emit`` / ``failed``: a future orphaned by a close landing mid-batch; it was
  handed to the client and may land, so it is unconfirmed — the same upper bound.
- ``GooglePubSubSink._resolve`` / ``failed``: a future that resolved to an error; the same upper
  bound.
- ``GooglePubSubSink._drain_pending`` / ``failed``: futures still in flight when the close bound
  expired; the same upper bound.
- ``GooglePubSubSink._reap`` and ``GooglePubSubSink._await_overflow`` / ``_futures``: the
  pending list, rebound as futures settle or are put back; state, not a count.
- ``RotatingFileSink.emit``, ``_rotate_or_continue``, ``_rotate`` and ``_ensure_open`` / ``_size``, ``_stream``,
  ``_next_rollover``, ``_rotation_failing``: the active file's handle, its tracked size, the
  next time trigger and the once-per-outage flag; state, not a count.
- ``FileSink.emit``, ``RotatingFileSink.emit`` and ``SQLiteSink.emit`` / ``_failed``: a batch
  whose write, flush or insert raised a driver error, counted before the raise (SPEC-069 FR-004);
  an upper bound, the recorded exception of invariant 2, never a ``dropped`` counter.

``KafkaSink`` needs no entry: its delivery callback's ``failed`` is in ``_on_delivery``, passed to
``produce`` uncalled, which the gate does not follow (see :func:`deferral_findings`).
"""


class _Scope(ast.NodeVisitor):
    """Collects ``Deferral(...)`` calls, and which are ``with`` items in an ``emit`` and how bound.

    A ``with`` item inside an ``emit``, or directly inside a module-level function the site lists
    under :data:`MODULE_LEVEL`, is *blessed* when it binds nothing or a bare local name. One bound
    to anything else — ``as self._pending``, ``as box[0]`` — is recorded in ``bound`` with its
    target, because the target outlives the call and so shares the pending drops. A function of
    a listed name that is a method, or nested in another function, is not blessed.
    """

    def __init__(self, module_functions: tuple[str, ...] = ()) -> None:
        self.functions: list[str] = []
        self.nesting: list[str] = []
        self.module_functions = module_functions
        self.calls: list[ast.Call] = []
        self.blessed: set[int] = set()
        self.bound: dict[int, ast.expr] = {}

    def _function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        self.functions.append(node.name)
        self.nesting.append(node.name)
        self.generic_visit(node)
        self.nesting.pop()
        self.functions.pop()

    visit_FunctionDef = _function
    visit_AsyncFunctionDef = _function

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.nesting.append("<class>")
        self.generic_visit(node)
        self.nesting.pop()

    def _listed_here(self) -> bool:
        return len(self.nesting) == 1 and self.nesting[0] in self.module_functions

    def visit_With(self, node: ast.With) -> None:
        if self.functions and (self.functions[-1] == "emit" or self._listed_here()):
            for item in node.items:
                if not _is_deferral_call(item.context_expr):
                    continue
                target = item.optional_vars
                if target is None or isinstance(target, ast.Name):
                    self.blessed.add(id(item.context_expr))
                else:
                    self.bound[id(item.context_expr)] = target
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


def _class(tree: ast.Module, cls_name: str) -> ast.ClassDef | None:
    """The module-level class named ``cls_name``, or ``None`` when it is missing."""
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == cls_name:
            return node
    return None


def _methods(cls: ast.ClassDef) -> dict[str, ast.FunctionDef]:
    """The ``def`` methods written in the class's own body, by name."""
    return {item.name: item for item in cls.body if isinstance(item, ast.FunctionDef)}


def _enters_deferral(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """True when ``fn``, nested code included, has a ``with`` item that is a ``Deferral(...)``."""
    return any(
        isinstance(node, ast.With)
        and any(_is_deferral_call(item.context_expr) for item in node.items)
        for node in ast.walk(fn)
    )


def _unlisted_deferring_classes(
    where: str, tree: ast.Module, sites: dict[str, tuple[str, ...]] | None
) -> list[str]:
    """Every class whose ``emit`` enters a ``Deferral`` and is not listed, one line each.

    The roster is derived rather than trusted: without this, a class converted to the helper but
    left out of :data:`DEFERRING_SITES` would be held to nothing past the construction rule, so a
    store left behind in it — the half-converted sink — would pass in silence. Every class is
    visited, nested ones included, and an ``async def emit`` counts, so a shape the site list
    cannot name fails closed as "not found; the site list is stale" once listed, rather than
    passing. Reported in source order.
    """
    listed = sites or {}
    classes = sorted(
        (node for node in ast.walk(tree) if isinstance(node, ast.ClassDef)),
        key=lambda node: node.lineno,
    )
    return [
        f"{where}: {cls.name}.emit enters a Deferral but is not listed in DEFERRING_SITES"
        for cls in classes
        for item in cls.body
        if isinstance(item, ast.FunctionDef | ast.AsyncFunctionDef)
        and item.name == "emit"
        and _enters_deferral(item)
        and "emit" not in listed.get(cls.name, ())
    ]


def _self_path(node: ast.expr) -> str | None:
    """``"a.b"`` for a target spelt ``self.a.b``, ``self.a[k].b`` or ``self.a.b[k]``, else ``None``.

    A subscript on the way down is stepped through rather than named, so a store into an item
    of a container held on ``self`` is a store to that container.
    """
    parts: list[str] = []
    while isinstance(node, ast.Attribute | ast.Subscript):
        if isinstance(node, ast.Attribute):
            parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name) and node.id == "self" and parts:
        return ".".join(reversed(parts))
    return None


def _self_stores(
    fn: ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda,
    walk: Callable[[ast.AST], Iterator[ast.AST]] = ast.walk,
) -> list[tuple[str, int]]:
    """Every store to state reached through ``self`` in ``fn``, nested code included, by line.

    ``walk`` yields the nodes looked at: every node under ``fn`` by default, which is what
    :func:`deferral_findings` reads; :func:`dropped_store_findings` passes
    :func:`_outside_nested_methods`, so the functions of a class nested in ``fn``, which that
    class's own scan covers, are not scanned twice.

    A store is recognised by the context the parser gives its target, not by listing statements.
    The binding forms in ``ast``'s grammar whose target is an expression are ``Assign.targets``,
    ``AugAssign.target``, ``AnnAssign.target``, ``For.target``, ``AsyncFor.target``,
    ``withitem.optional_vars`` (``with`` and ``async with``) and ``comprehension.target`` (all
    four comprehensions), and an attribute or a subscript in any of them — inside any tuple, list
    or starred unpacking — parses with ``ctx=Store``, so one test covers them all. The forms left
    over cannot bind an attribute: ``NamedExpr``, ``except ... as``, ``import ... as``, a
    ``match`` capture and a type parameter take a bare name, and ``Delete`` parses with
    ``ctx=Del``, an unbinding that counts nothing. ``setattr(self, ...)`` is the one store the
    parser cannot mark, so it is matched by name. Sorted by position, so the report reads in
    source order whatever order ``ast.walk`` visits in.
    """
    stores: list[tuple[str, int, int]] = []
    for node in walk(fn):
        if isinstance(node, ast.Attribute | ast.Subscript) and isinstance(node.ctx, ast.Store):
            path = _self_path(node)
            if path is not None:
                stores.append((path, node.lineno, node.col_offset))
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
            stores.append((str(attr), node.lineno, node.col_offset))
    return [(path, line) for path, line, _ in sorted(stores, key=lambda s: (s[1], s[2]))]


def _self_calls(fn: ast.FunctionDef) -> list[str]:
    """The names ``fn`` calls as ``self.<name>(...)``, nested code included, in source order."""
    calls = [
        node
        for node in ast.walk(fn)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "self"
    ]
    calls.sort(key=lambda call: (call.lineno, call.col_offset))
    return [call.func.attr for call in calls if isinstance(call.func, ast.Attribute)]


def _reach(
    where: str,
    cls_name: str,
    fn: ast.FunctionDef,
    caller: str | None,
    methods: dict[str, ast.FunctionDef],
    seen: set[str],
    immediate: dict[tuple[str, str], frozenset[str]],
    used: set[tuple[str, str, str]] | None,
) -> list[str]:
    """``fn``'s own forbidden stores, then those of every method it reaches through ``self``.

    Depth first, each method once: ``seen`` is shared across one class's whole walk and starts
    holding every named function, so a helper two named functions both call is reported once,
    under the first route found, and a named function is reported under its own name rather
    than as somebody's callee. A cycle terminates for the same reason.
    """
    findings: list[str] = []
    allowed = immediate.get((cls_name, fn.name), frozenset())
    via = "" if caller is None else f", called from {cls_name}.{caller},"
    for attr, line in _self_stores(fn):
        if attr in allowed:
            if used is not None:
                used.add((cls_name, fn.name, attr))
            continue
        findings.append(
            f"{where}:{line}: {cls_name}.{fn.name}{via} stores self.{attr} directly; "
            f"count it through the Deferral, or, if it must count as it happens, add "
            f"({cls_name}, {fn.name}) to IMMEDIATE with the reason"
        )
    for name in _self_calls(fn):
        callee = methods.get(name)
        if callee is None or name in seen:
            continue
        seen.add(name)
        findings.extend(_reach(where, cls_name, callee, fn.name, methods, seen, immediate, used))
    return findings


def deferral_findings(
    where: str,
    source: str,
    sites: dict[str, tuple[str, ...]] | None,
    immediate: dict[tuple[str, str], frozenset[str]],
    used: set[tuple[str, str, str]] | None = None,
) -> list[str]:
    """Every way one module breaks the one-helper rule, as lines a reader can act on.

    **What it checks.** Everywhere: every ``Deferral(...)`` call, spelt as a bare name or as an
    attribute, is the context expression of a ``with`` item directly inside a function named
    ``emit``, binding nothing or a bare local name — built anywhere else, or bound to an
    attribute, a subscript or an unpacking, its pending drops outlive the call. Also everywhere:
    every class whose ``emit`` enters a ``Deferral`` is listed in ``sites`` with ``emit`` among
    its functions, so the site list is derived from the tree rather than trusted, and a class
    converted to the helper but never listed is a finding rather than a site the store checks
    below never visit. In a named site only: the module imports ``Deferral`` from the helper's
    module; every named class exists at module level and every named method in its body, so a
    rename is a finding rather than a smaller guard; each named ``emit`` enters a ``Deferral``;
    and no named method, nor any method of the same class it reaches through ``self.<name>(...)``
    calls — followed transitively, each once — stores to ``self`` (:func:`_self_stores` says
    which forms count) except where ``immediate`` allows that attribute in that method. A store
    there is a second ledger committed beside the helper's, which is the defect FR-002 refuses.

    **What it does not check.** It follows only a call spelt ``self.<name>(...)`` to a ``def`` in
    the same class body, so an inherited method, an override in a subclass (the HTTP platform
    sinks' ``_render``), ``super()``, a property, a ``getattr`` dispatch, a bound method passed
    uncalled and a call through another object are not followed. It sees a store only through a
    name spelt ``self``, so an alias (``me = self``) hides one, and it does not see a mutating
    method call (``self.counts.update(...)``). Outside a named site it does not see a blessed
    ``Deferral``'s local stored afterwards; inside one that is a store to ``self`` and a finding.
    A ``Deferral`` entered by an ``emit`` that is not a method — a module-level function — has
    no class to list, so it is held only to the construction rule; a module-level function that
    commits is listed under :data:`MODULE_LEVEL` instead and held as
    :func:`_module_function_findings` says.
    Nor does it check the block's extent: neither this check nor :func:`dropped_store_findings`
    verifies that an ``emit``'s ``with Deferral(...)`` encloses every statement that can raise
    after a drop is decided, so a send or a ``raise`` moved out of the block commits the drop
    before the raise abandons the batch, and both stay silent. Each converted site's down probe,
    the behaviour test that an abandoned batch counts nothing, is the guard for that.
    It reads syntax, not data flow: a store on a path ``emit`` never takes at run time is still a
    finding, which is why the allowlist names methods. That the allowlist itself is exact is
    :func:`stale_immediate`'s check, over the whole tree.

    Args:
      where: The module's path, for the message.
      source: The module's text.
      sites: The classes and functions this module must keep deferred, or ``None`` when it is
        not a deferring site.
      immediate: Stores permitted per ``(class, method)``, the method being the one that makes
        the store.
      used: When given, receives ``(class, method, attribute)`` for every store ``immediate``
        allowed, so :func:`stale_immediate` can report an entry nothing reached.

    Returns:
      The findings, empty when the module complies.
    """
    tree = ast.parse(source)
    findings: list[str] = []
    module_functions = (sites or {}).get(MODULE_LEVEL, ())
    scope = _Scope(module_functions)
    scope.visit(tree)
    for call in scope.calls:
        if id(call) in scope.blessed:
            continue
        target = scope.bound.get(id(call))
        if target is None:
            findings.append(
                f"{where}:{call.lineno}: Deferral(...) built outside a `with` item in an emit; "
                f"one call's pending drops must stay local to that call"
            )
        else:
            findings.append(
                f"{where}:{call.lineno}: Deferral(...) bound to {ast.unparse(target)}, not a "
                f"local name; one call's pending drops must stay local to that call"
            )
    findings.extend(_unlisted_deferring_classes(where, tree, sites))
    if sites is None:
        return findings
    if not _imports_the_helper(tree):
        findings.append(f"{where}: does not import Deferral from {_HELPER_MODULE}")
    findings.extend(_module_function_findings(where, tree, module_functions))
    for cls_name, functions in sites.items():
        if cls_name == MODULE_LEVEL:
            continue
        cls = _class(tree, cls_name)
        if cls is None:
            findings.append(f"{where}: class {cls_name} not found; the site list is stale")
            continue
        methods = _methods(cls)
        seen = {name for name in functions if name in methods}
        for name in functions:
            fn = methods.get(name)
            if fn is None:
                findings.append(f"{where}: {cls_name}.{name} not found; the site list is stale")
                continue
            if name == "emit" and not _enters_deferral(fn):
                findings.append(f"{where}: {cls_name}.emit enters no Deferral")
            findings.extend(_reach(where, cls_name, fn, None, methods, seen, immediate, used))
    return findings


def _attribute_stores(fn: ast.AST) -> list[tuple[str, int]]:
    """Every attribute store through any root in ``fn``, nested code included, by line.

    A store's target is an attribute, or a subscript whose chain passes through one, parsed with
    ``ctx=Store`` (:func:`_self_stores` says why that covers every binding form); a subscript of a
    bare local (``fate[i] = x``) stores nothing that outlives the call and is not one. A
    ``setattr(...)`` call is a store whatever its arguments.

    Args:
      fn: The function to scan.

    Returns:
      ``(rendered target, line)`` per store, in source order.

    Raises:
      None.
    """
    stores: list[tuple[str, int, int]] = []
    for node in ast.walk(fn):
        if isinstance(node, ast.Attribute | ast.Subscript) and isinstance(node.ctx, ast.Store):
            chain: ast.expr = node
            through_attribute = False
            while isinstance(chain, ast.Attribute | ast.Subscript):
                through_attribute = through_attribute or isinstance(chain, ast.Attribute)
                chain = chain.value
            if through_attribute:
                stores.append((ast.unparse(node), node.lineno, node.col_offset))
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "setattr"
        ):
            stores.append((ast.unparse(node), node.lineno, node.col_offset))
    return [(text, line) for text, line, _ in sorted(stores, key=lambda s: (s[1], s[2]))]


def _module_function_findings(where: str, tree: ast.Module, names: tuple[str, ...]) -> list[str]:
    """Holds each listed module-level function to the one-helper rule, one line per breach.

    Each listed name must be a function in the module's own body, so a rename is a finding; it
    must enter a ``Deferral``; and neither it nor any module-level function it reaches through a
    bare-name call — followed transitively, each once — may make an attribute store through any
    root (:func:`_attribute_stores`). A call through an attribute (``_diag.lost``), an imported
    name or a parameter (``deliver``) is not followed.

    Args:
      where: The module's path, for the message.
      tree: The parsed module.
      names: The functions :data:`DEFERRING_SITES` lists under :data:`MODULE_LEVEL`.

    Returns:
      The findings, empty when every listed function complies.

    Raises:
      None.
    """
    functions = {
        node.name: node
        for node in tree.body
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    }
    findings: list[str] = []
    seen = {name for name in names if name in functions}
    for name in names:
        fn = functions.get(name)
        if fn is None:
            findings.append(f"{where}: {name} not found; the site list is stale")
            continue
        if not _enters_deferral(fn):
            findings.append(f"{where}: {name} enters no Deferral")
        route: list[tuple[ast.FunctionDef | ast.AsyncFunctionDef, str]] = [(fn, "")]
        while route:
            current, via = route.pop(0)
            findings.extend(
                f"{where}:{line}: {current.name}{via} stores {target} directly; count it "
                f"through the Deferral"
                for target, line in _attribute_stores(current)
            )
            for call in ast.walk(current):
                if not (isinstance(call, ast.Call) and isinstance(call.func, ast.Name)):
                    continue
                callee = functions.get(call.func.id)
                if callee is None or callee.name in seen:
                    continue
                seen.add(callee.name)
                route.append((callee, f", called from {current.name},"))
    return findings


def stale_immediate(
    immediate: dict[tuple[str, str], frozenset[str]], used: set[tuple[str, str, str]]
) -> list[str]:
    """Every store ``immediate`` allows that no deferring function reached, one line each.

    An allowance nothing uses is a hole waiting for a method of that name to grow a store: the
    gate would wave it through with nobody having decided it should be immediate. Holding the
    allowlist equal to the stores it actually excuses keeps "and the only ones" true.

    Args:
      immediate: The allowlist, as :func:`deferral_findings` takes it.
      used: The ``(class, method, attribute)`` stores :func:`deferral_findings` allowed, gathered
        over every module it was run on.

    Returns:
      The stale entries, empty when every allowance excused a store.
    """
    return [
        f"IMMEDIATE allows {cls_name}.{method} to store self.{attr}, and no deferring function "
        f"reaches that store; the allowlist is stale"
        for (cls_name, method), attrs in sorted(immediate.items())
        for attr in sorted(attrs)
        if (cls_name, method, attr) not in used
    ]


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


def _stores(cls_name: str, method: str) -> str:
    """The tail of a store finding: both routes, the second naming the method that stores."""
    return (
        "directly; count it through the Deferral, or, if it must count as it happens, add "
        f"({cls_name}, {method}) to IMMEDIATE with the reason"
    )


_BOUND = "not a local name; one call's pending drops must stay local to that call"
_STALE_IMPORT = "m.py: does not import Deferral from log_foundry.sinks._deferral"

_ONE_EMIT = {"Sink": ("emit",)}

_CORPUS_IMMEDIATE: dict[tuple[str, str], frozenset[str]] = {
    ("MultiSink", "emit"): frozenset({"failed"}),
    ("HTTPSink", "_drop_refused"): frozenset({"dropped_oversized"}),
}
"""The corpus's own allowlist, fixed, so its cases test the allowlist mechanism and not the tree's.

The corpus read the shipped :data:`IMMEDIATE` until SPEC-059, and two of its cases lean on an entry
there: ``MultiSink.emit`` / ``failed``, and ``HTTPSink._drop_refused`` / ``dropped_oversized`` in
"an allowed store is silent in the method the allowlist names". SPEC-059 FR-004 removed the second
from the shipped list, which would have reddened a case about the mechanism over a change to an
entry. Every case's source and expected findings are what they were.
"""

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
        [f"m.py:6: Sink.emit stores self.dropped {_stores('Sink', 'emit')}"],
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
        [f"m.py:8: Sink.emit stores self.dropped_oversized {_stores('Sink', 'emit')}"],
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
            f"m.py:6: Sink.emit stores self.a {_stores('Sink', 'emit')}",
            f"m.py:6: Sink.emit stores self.stats.b {_stores('Sink', 'emit')}",
            f"m.py:7: Sink.emit stores self.c {_stores('Sink', 'emit')}",
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
        [f"m.py:6: Sink.emit stores self.failed {_stores('Sink', 'emit')}"],
    ),
    "a helper the emit delegates to is checked by name": (
        _IMPORT
        + _DEFERRED_EMIT
        + """
    def _items(self, batch, pending):
        self.dropped_oversized += 1
""",
        {"Sink": ("emit", "_items")},
        [f"m.py:10: Sink._items stores self.dropped_oversized {_stores('Sink', '_items')}"],
    ),
    "missing import": (_DEFERRED_EMIT, _ONE_EMIT, [_STALE_IMPORT]),
    "a Deferral imported from elsewhere is not the helper": (
        "from somewhere_else import Deferral\n" + _DEFERRED_EMIT,
        _ONE_EMIT,
        [_STALE_IMPORT],
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
        [
            f"m.py:5: {_BUILT_OUTSIDE}",
            f"m.py:5: Sink.emit stores self._pending {_stores('Sink', 'emit')}",
        ],
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
        [
            "m.py: Renamed.emit enters a Deferral but is not listed in DEFERRING_SITES",
            "m.py: class Sink not found; the site list is stale",
        ],
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
    "a module that is not a site is not held to the store rule": (
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
    "a class that enters a Deferral and is not listed is refused": (
        _IMPORT
        + """
class Sink:
    def emit(self, batch):
        with Deferral(self) as pending:
            records = self._records(batch)
    def _records(self, batch):
        with self._counter_lock:
            self.dropped_oversized += 1
""",
        None,
        ["m.py: Sink.emit enters a Deferral but is not listed in DEFERRING_SITES"],
    ),
    "an unlisted class beside a listed one is refused": (
        _IMPORT
        + _DEFERRED_EMIT
        + """
class Other:
    def emit(self, batch):
        with Deferral(self) as pending:
            pass
""",
        _ONE_EMIT,
        ["m.py: Other.emit enters a Deferral but is not listed in DEFERRING_SITES"],
    ),
    "a listed class whose emit is not among its functions is refused": (
        _IMPORT
        + _DEFERRED_EMIT
        + """
    def _items(self, batch):
        pass
""",
        {"Sink": ("_items",)},
        ["m.py: Sink.emit enters a Deferral but is not listed in DEFERRING_SITES"],
    ),
    "every listed class that enters a Deferral is silent": (
        _IMPORT
        + _DEFERRED_EMIT
        + """
class Other:
    def emit(self, batch):
        with Deferral(self) as pending:
            pass
""",
        {"Sink": ("emit",), "Other": ("emit",)},
        [],
    ),
    "a store in a method a named function calls is still a store": (
        _IMPORT
        + """
class Sink:
    def emit(self, batch):
        with Deferral(self) as pending:
            self._items(batch, pending)
    def _items(self, batch, pending):
        self._bump(len(batch))
    def _bump(self, n):
        with self._counter_lock:
            self.dropped_oversized += n
""",
        {"Sink": ("emit", "_items")},
        [
            "m.py:11: Sink._bump, called from Sink._items, stores self.dropped_oversized "
            + _stores("Sink", "_bump")
        ],
    ),
    "a store two calls below the emit is still a store": (
        _IMPORT
        + """
class Sink:
    def emit(self, batch):
        with Deferral(self) as pending:
            self._first(batch)
    def _first(self, batch):
        self._second(batch)
    def _second(self, batch):
        self.dropped += 1
""",
        _ONE_EMIT,
        [
            "m.py:10: Sink._second, called from Sink._first, stores self.dropped "
            + _stores("Sink", "_second")
        ],
    ),
    "a named function another one calls is reported under its own name": (
        _IMPORT
        + """
class Sink:
    def emit(self, batch):
        with Deferral(self) as pending:
            self._items(batch, pending)
    def _items(self, batch, pending):
        self.dropped += 1
""",
        {"Sink": ("emit", "_items")},
        [f"m.py:8: Sink._items stores self.dropped {_stores('Sink', '_items')}"],
    ),
    "a helper two callers share is reported once": (
        _IMPORT
        + """
class Sink:
    def emit(self, batch):
        with Deferral(self) as pending:
            self._a(batch)
            self._b(batch)
    def _a(self, batch):
        self._c(batch)
    def _b(self, batch):
        self._c(batch)
    def _c(self, batch):
        self.dropped += 1
""",
        _ONE_EMIT,
        [f"m.py:13: Sink._c, called from Sink._a, stores self.dropped {_stores('Sink', '_c')}"],
    ),
    "a recursive helper is walked once": (
        _IMPORT
        + """
class Sink:
    def emit(self, batch):
        with Deferral(self) as pending:
            self._walk(batch)
    def _walk(self, batch):
        if batch:
            self._walk(batch[1:])
        self.dropped += 1
""",
        _ONE_EMIT,
        [
            "m.py:10: Sink._walk, called from Sink.emit, stores self.dropped "
            + _stores("Sink", "_walk")
        ],
    ),
    "a call that reaches no store through self is silent": (
        _IMPORT
        + """
class Sink:
    def emit(self, batch):
        with Deferral(self) as pending:
            if self._ready(batch):
                self._inner.emit(batch)
                self._fn(batch)
                peer = self._peer
                peer._reset()
    def _ready(self, batch):
        return bool(batch) and self.enabled
    def _reset(self):
        self.dropped = 0
""",
        _ONE_EMIT,
        [],
    ),
    "a store to a local object is not a store to self": (
        _IMPORT
        + """
class Sink:
    def emit(self, batch):
        with Deferral(self) as pending:
            for event in batch:
                event.seen = True
                event["k"] = self.value
                items = [self.n]
                items[self.i] = 0
""",
        _ONE_EMIT,
        [],
    ),
    "an allowed store is silent in the method the allowlist names": (
        _IMPORT
        + """
class HTTPSink:
    def emit(self, batch):
        with Deferral(self) as pending:
            self._drop_refused(batch)
    def _drop_refused(self, chunk):
        with self._counter_lock:
            self.dropped_oversized += len(chunk)
""",
        {"HTTPSink": ("emit",)},
        [],
    ),
    "a store finding names the IMMEDIATE route for the method that stores": (
        _IMPORT
        + """
class HTTPSink:
    def emit(self, batch):
        with Deferral(self) as pending:
            self._give_up(batch)
    def _give_up(self, chunk):
        with self._counter_lock:
            self.failed += 1
""",
        {"HTTPSink": ("emit",)},
        [
            (
                "m.py:9: HTTPSink._give_up, called from HTTPSink.emit, stores self.failed "
                "directly; count it through the Deferral, or, if it must count as it happens, "
                "add (HTTPSink, _give_up) to IMMEDIATE with the reason"
            )
        ],
    ),
    "the allowance is the method's, not the class's": (
        _IMPORT
        + """
class HTTPSink:
    def emit(self, batch):
        with Deferral(self) as pending:
            self._items(batch)
    def _items(self, batch):
        self.dropped_oversized += 1
""",
        {"HTTPSink": ("emit",)},
        [
            (
                f"m.py:8: HTTPSink._items, called from HTTPSink.emit, stores "
                f"self.dropped_oversized {_stores('HTTPSink', '_items')}"
            )
        ],
    ),
    "a Deferral bound to self through the with target": (
        _IMPORT
        + """
class Sink:
    def emit(self, batch):
        with Deferral(self) as self._pending:
            self._pending.count("dropped")
""",
        _ONE_EMIT,
        [
            f"m.py:5: Deferral(...) bound to self._pending, {_BOUND}",
            f"m.py:5: Sink.emit stores self._pending {_stores('Sink', 'emit')}",
        ],
    ),
    "a Deferral bound to a subscript is refused outside a site too": (
        _IMPORT
        + """
class Other:
    def emit(self, batch):
        box = [None]
        with Deferral(self) as box[0]:
            pass
""",
        None,
        [
            f"m.py:6: Deferral(...) bound to box[0], {_BOUND}",
            "m.py: Other.emit enters a Deferral but is not listed in DEFERRING_SITES",
        ],
    ),
    "a for target on self is a store": (
        _IMPORT
        + """
class Sink:
    def emit(self, batch):
        with Deferral(self) as pending:
            for self.dropped in range(3):
                pass
""",
        _ONE_EMIT,
        [f"m.py:6: Sink.emit stores self.dropped {_stores('Sink', 'emit')}"],
    ),
    "every other binding form that can take an attribute is a store": (
        _IMPORT
        + """
class Sink:
    def emit(self, batch):
        with Deferral(self) as pending:
            self.a: int = 1
            with open(path) as self.b:
                pass
            [0 for self.c in batch]
            {0 for self.d in batch}
            {0: 0 for self.e in batch}
            list(0 for self.f in batch)
            self.g["k"] += 1
            async def inner():
                async for self.h in batch:
                    pass
                async with lock as self.i:
                    pass
            del self.j
""",
        _ONE_EMIT,
        [
            f"m.py:{line}: Sink.emit stores self.{attr} {_stores('Sink', 'emit')}"
            for attr, line in [
                ("a", 6),
                ("b", 7),
                ("c", 9),
                ("d", 10),
                ("e", 11),
                ("f", 12),
                ("g", 13),
                ("h", 15),
                ("i", 17),
            ]
        ],
    ),
}


_ENGINE_SITES: dict[str, tuple[str, ...]] = {MODULE_LEVEL: ("_run",)}

_ENGINE_RUN = """
def _run(counters, batch):
    total = _tally(batch)
    with Deferral(counters) as ledger:
        ledger.count("failed_events", total)
    return total
def _tally(batch):
    fate = [0] * len(batch)
    fate[0] = 1
    return sum(fate)
"""

_COMMIT_LINES = (
    '    with Deferral(counters) as ledger:\n        ledger.count("failed_events", total)\n'
)

_CORPUS.update(
    {
        "a listed module function's Deferral is blessed": (
            _IMPORT + _ENGINE_RUN,
            _ENGINE_SITES,
            [],
        ),
        "a module function with no Deferral is named": (
            _IMPORT + _ENGINE_RUN.replace(_COMMIT_LINES, ""),
            _ENGINE_SITES,
            ["m.py: _run enters no Deferral"],
        ),
        "a renamed module function is named": (
            _IMPORT + _ENGINE_RUN.replace("def _run(", "def _run_renamed("),
            _ENGINE_SITES,
            [f"m.py:5: {_BUILT_OUTSIDE}", "m.py: _run not found; the site list is stale"],
        ),
        "a store through the counters object is a second ledger": (
            _IMPORT
            + _ENGINE_RUN.replace(
                "    return total\n", "    counters.failed_events += total\n    return total\n"
            ),
            _ENGINE_SITES,
            ["m.py:7: _run stores counters.failed_events directly; count it through the Deferral"],
        ),
        "a store in a callee is named with its route": (
            _IMPORT
            + _ENGINE_RUN.replace(
                "    return sum(fate)\n",
                "    batch.owner.dropped_events = 1\n    return sum(fate)\n",
            ),
            _ENGINE_SITES,
            [
                (
                    "m.py:11: _tally, called from _run, stores batch.owner.dropped_events directly; "
                    "count it through the Deferral"
                )
            ],
        ),
        "a setattr in a listed function is a store": (
            _IMPORT
            + _ENGINE_RUN.replace(
                "    return total\n", "    setattr(counters, name, 1)\n    return total\n"
            ),
            _ENGINE_SITES,
            [
                (
                    "m.py:7: _run stores setattr(counters, name, 1) directly; count it through the "
                    "Deferral"
                )
            ],
        ),
        "an unlisted module function's Deferral is still refused": (
            _IMPORT + _ENGINE_RUN,
            None,
            [f"m.py:5: {_BUILT_OUTSIDE}"],
        ),
        "a method named like a listed function is not blessed": (
            _IMPORT
            + "class Engine:\n"
            + "".join(f"    {line}\n" for line in _ENGINE_RUN.strip().splitlines()),
            _ENGINE_SITES,
            [f"m.py:5: {_BUILT_OUTSIDE}", "m.py: _run not found; the site list is stale"],
        ),
    }
)


@pytest.mark.parametrize("case", sorted(_CORPUS))
def test_the_deferral_lint_reports_what_it_claims_to(case: str) -> None:
    """Each check's failure text, and the shapes that must stay silent, on synthetic modules."""
    source, sites, expected = _CORPUS[case]
    assert deferral_findings("m.py", source, sites, _CORPUS_IMMEDIATE) == expected


def test_an_allowance_nothing_reaches_is_reported_stale() -> None:
    """The allowlist is held exact: an entry no deferring function used is named, per attribute."""
    source = (
        _IMPORT
        + """
class Sink:
    def emit(self, batch):
        with Deferral(self) as pending:
            self.failed += 1
"""
    )
    immediate = {
        ("Sink", "emit"): frozenset({"failed", "dropped"}),
        ("Sink", "_gone"): frozenset({"failed"}),
    }
    used: set[tuple[str, str, str]] = set()

    assert deferral_findings("m.py", source, _ONE_EMIT, immediate, used) == []
    assert used == {("Sink", "emit", "failed")}
    stale = "and no deferring function reaches that store; the allowlist is stale"
    assert stale_immediate(immediate, used) == [
        f"IMMEDIATE allows Sink._gone to store self.failed, {stale}",
        f"IMMEDIATE allows Sink.emit to store self.dropped, {stale}",
    ]
    assert stale_immediate({("Sink", "emit"): frozenset({"failed"})}, used) == [], (
        "an allowlist every entry of which was used is silent"
    )


def test_every_deferring_site_takes_the_one_helper() -> None:
    """SPEC-058 FR-002's instrument over the real package: every site, one helper, one ledger.

    The construction rule and the roster rule run over every module under ``src/``, so a site not
    yet listed that builds a ``Deferral`` is held to the first, and one whose ``emit`` enters a
    ``Deferral`` is a finding until it is added to :data:`DEFERRING_SITES`; the other checks run
    on the listed sites by name, and a site that has moved is a finding. The allowlist is held
    exact over the same run: an :data:`IMMEDIATE` entry no deferring function reached is stale.
    """
    modules = {path.relative_to(_SRC).as_posix(): path for path in sorted(_SRC.rglob("*.py"))}
    assert set(DEFERRING_SITES) <= set(modules), (
        f"a deferring site's module is gone: {sorted(set(DEFERRING_SITES) - set(modules))}"
    )
    findings: list[str] = []
    used: set[tuple[str, str, str]] = set()
    for where, path in modules.items():
        source = path.read_text(encoding="utf-8")
        findings.extend(
            deferral_findings(where, source, DEFERRING_SITES.get(where), IMMEDIATE, used)
        )
    findings.extend(stale_immediate(IMMEDIATE, used))
    assert findings == [], "\n".join(findings)


# --- SPEC-059 FR-005: a dropped counter is written only by a Deferral commit --------------

DROPPED_FLOOR = frozenset(
    {
        "dropped_oversized",
        "_unrenderable",
        "rejected",
        "_dropped",
        "_unencodable",
        "dropped_events",
    }
)
"""Counters the derivation must find over the tree; a floor, not the set.

A new counter needs no edit here, because the set is derived. ``_id_failed`` left it with
SPEC-074, which deleted ``SQSSink``'s counter: an id-callable drop is a ``DROPPED`` outcome the
engine commits in ``dropped_events`` only when a run settles, which this floor still names and
:func:`run_context_count_findings` and the engine's own tests guard. What the floor catches is the
opposite drift: a refactor that blinds :func:`dropped_counters` — a renamed helper, a call it no
longer recognises — would otherwise shrink the guard as silently as a deletion.
"""

_REPORTED_CALLS = {"SinkLosses": ("dropped", 0), "wrapped_losses": ("own", 1)}
"""Each call that reports a drop count, with the argument that carries it: keyword, position."""


def _callee(call: ast.Call) -> str | None:
    """The called name, spelt bare (``SinkLosses``) or as an attribute (``base.SinkLosses``)."""
    func = call.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _argument(call: ast.Call, keyword: str, position: int) -> ast.expr | None:
    """The argument passed as ``keyword=``, else the one at ``position``, else ``None``.

    ``SinkLosses`` is keyword-only, so its positional form never constructs at run time; it is
    read anyway, as syntax, because reading it fails closed.
    """
    for item in call.keywords:
        if item.arg == keyword:
            return item.value
    if len(call.args) > position:
        return call.args[position]
    return None


def _self_rooted(node: ast.expr) -> bool:
    """True when ``node`` is a chain of attributes and subscripts that starts at ``self``."""
    while isinstance(node, ast.Attribute | ast.Subscript):
        node = node.value
    return isinstance(node, ast.Name) and node.id == "self"


def _reported_names(node: ast.AST) -> set[str]:
    """The **last** name of every ``self``-rooted chain read anywhere in ``node``.

    ``self.a.b`` yields ``b``, the attribute holding the count; ``a`` is an object on the way to
    it, and deriving it would make a legitimate rebinding of that object (a lazily opened client)
    a finding. A chain is taken whole at its outermost attribute and not descended into, so its
    inner names are never yielded.
    """
    if isinstance(node, ast.Attribute) and _self_rooted(node):
        return {node.attr}
    names: set[str] = set()
    for child in ast.iter_child_nodes(node):
        names |= _reported_names(child)
    return names


def _bound_names(target: ast.expr) -> list[str]:
    """The bare local names a target binds, through tuple, list and starred unpacking."""
    if isinstance(target, ast.Name):
        return [target.id]
    if isinstance(target, ast.Tuple | ast.List):
        return [name for item in target.elts for name in _bound_names(item)]
    if isinstance(target, ast.Starred):
        return _bound_names(target.value)
    return []


def _paired_values(target: ast.expr, value: ast.expr) -> list[tuple[str, ast.expr]]:
    """Each bare local name a target binds, with the part of ``value`` the name can hold.

    A tuple or list target over a tuple or list value of the same length, with no starred element
    on either side, pairs element-wise, recursively into a nested tuple or list, so
    ``dropped, failed = self.dropped_oversized, self.failed`` gives ``dropped`` the first element
    alone. Every other unpacking — unequal lengths, a starred element on either side, a value that
    is not a tuple or list display — maps each name to the whole value (:func:`_bound_names`):
    that can derive more than the name holds, never less, and so fails closed.
    """
    if (
        isinstance(target, ast.Tuple | ast.List)
        and isinstance(value, ast.Tuple | ast.List)
        and len(target.elts) == len(value.elts)
        and not any(isinstance(item, ast.Starred) for item in (*target.elts, *value.elts))
    ):
        return [
            pair
            for item, part in zip(target.elts, value.elts, strict=True)
            for pair in _paired_values(item, part)
        ]
    return [(name, value) for name in _bound_names(target)]


def _local_values(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> dict[str, list[ast.expr]]:
    """Every value ``fn`` assigns to each bare local name, nested functions included.

    Plain, annotated and augmented assignment and the walrus all count; a nested function's
    assignments are visible to the outer lookup, which is wider than Python's scoping and so fails
    closed. An unpacking assigns each name the part of the value :func:`_paired_values` pairs it
    with — one element of an equal-length tuple, else the whole value.
    """
    values: dict[str, list[ast.expr]] = {}
    for node in ast.walk(fn):
        if isinstance(node, ast.Assign):
            pairs = [(target, node.value) for target in node.targets]
        elif isinstance(node, ast.AnnAssign | ast.AugAssign | ast.NamedExpr) and node.value:
            pairs = [(node.target, node.value)]
        else:
            continue
        for target, value in pairs:
            for name, part in _paired_values(target, value):
                values.setdefault(name, []).append(part)
    return values


def dropped_counters(source: str) -> set[str]:
    """Every attribute one module's code reports as ``dropped``, derived from the syntax.

    **What it derives.** In every function at any depth — not only one named ``losses``, since a
    sink may build its report in a helper — every call to ``SinkLosses`` or ``wrapped_losses``,
    spelt bare or as an attribute. From ``SinkLosses`` it reads the ``dropped`` argument (keyword,
    else first positional); from ``wrapped_losses`` the ``own`` argument (keyword, else second
    positional). In that argument it takes the last name of every ``self``-rooted chain
    (:func:`_reported_names`), and, for every bare name it reads, the same from each value the
    function assigns to that name (:func:`_local_values`, which gives an unpacked name its own
    element where the two shapes match) — which is how ``TransformSink`` and ``FilteringSink``
    report ``self._dropped`` through a local. That step is **one hop**: a value's own names are
    not followed, so ``MultiSink.losses``' ``reported``, built from ``self._sinks``, contributes
    nothing, where a transitive step would derive ``_sinks``.

    Args:
      source: The module's text.

    Returns:
      The derived attribute names; the tree test unions them over every module.
    """
    counters: set[str] = set()
    for fn in ast.walk(ast.parse(source)):
        if not isinstance(fn, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        assigned = _local_values(fn)
        for call in ast.walk(fn):
            if not isinstance(call, ast.Call):
                continue
            spec = _REPORTED_CALLS.get(_callee(call) or "")
            if spec is None:
                continue
            argument = _argument(call, *spec)
            if argument is None:
                continue
            counters |= _reported_names(argument)
            for name in ast.walk(argument):
                if isinstance(name, ast.Name):
                    for value in assigned.get(name.id, []):
                        counters |= _reported_names(value)
    return counters


def _code_roots(node: ast.AST) -> list[ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda]:
    """The outermost functions and lambdas under ``node``, entering neither a class nor one of them.

    Every other child is descended through — a class body's ``if``, ``else``, ``try``, ``with``,
    loop and ``match`` blocks, the expression a lambda sits in — so a method is found wherever its
    ``def`` sits in the body's statement tree. A function or lambda reached is a root and is not
    entered, since a scan of it covers everything it nests; a class reached is skipped, since
    :func:`dropped_store_findings` scans every class as its own.
    """
    roots: list[ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda] = []
    for child in ast.iter_child_nodes(node):
        if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda):
            roots.append(child)
        elif not isinstance(child, ast.ClassDef):
            roots.extend(_code_roots(child))
    return roots


def _code_name(fn: ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda) -> str:
    """A function's own name, or ``<lambda>`` as ``__qualname__`` spells a lambda's."""
    return "<lambda>" if isinstance(fn, ast.Lambda) else fn.name


def _scan_roots(
    cls: ast.ClassDef,
) -> list[tuple[str, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda]]:
    """Each piece of ``cls``'s code that can run after construction, with the name a finding uses.

    That is every root :func:`_code_roots` finds in the class, named ``Sink.bump`` or
    ``Sink.<lambda>``, except ``__init__``: its own statements run during construction and are
    exempt. A function or lambda nested in ``__init__`` can run after it — a callback handed to a
    client is the realistic shape — so each outermost one is a root of its own, named as
    ``__qualname__`` names it: ``Sink.__init__.<locals>.refused``.
    """
    roots: list[tuple[str, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda]] = []
    for fn in _code_roots(cls):
        if _code_name(fn) != "__init__":
            roots.append((f"{cls.name}.{_code_name(fn)}", fn))
            continue
        roots.extend(
            (f"{cls.name}.__init__.<locals>.{_code_name(inner)}", inner)
            for inner in _code_roots(fn)
        )
    return roots


def _outside_nested_methods(node: ast.AST, in_class: bool = False) -> Iterator[ast.AST]:
    """``node`` and every node under it, except the functions and lambdas of a class nested in it.

    Those are roots of the nested class, which :func:`dropped_store_findings` scans as its own, so
    walking them here too would report each of their stores twice, the second time under the
    enclosing function's name. The rest of a nested class — the statements its body runs when the
    enclosing code defines it, which can close over that code's ``self`` — is walked, since no
    other scan reaches it.
    """
    yield node
    for child in ast.iter_child_nodes(node):
        if in_class and isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda):
            continue
        yield from _outside_nested_methods(child, in_class or isinstance(child, ast.ClassDef))


def dropped_store_findings(where: str, source: str, counters: set[str]) -> list[str]:
    """Every store to a derived counter after construction, as lines a reader can act on.

    **Where it looks.** Every class in the module, nested classes included, each scanned as its
    own. In a class it scans every function and lambda the body holds, with everything they nest:
    a method wherever its ``def`` sits in the body's statement tree — under an ``if``, ``else``,
    ``try``, ``with``, loop or ``match`` block as well as at the top — and a lambda in the body,
    named ``Sink.<lambda>``. ``__init__``'s own statements are exempt, since they run during
    construction. A function or lambda nested in ``__init__`` can run after it, so each is
    scanned, named as ``__qualname__`` names it — ``Sink.__init__.<locals>.refused``,
    ``Sink.__init__.<locals>.<lambda>``. Each store is reported once: the search for a class's
    functions enters neither a nested class nor another function (:func:`_code_roots`), and the
    scan of a function skips the functions of a class defined inside it
    (:func:`_outside_nested_methods`), each of which that class's own scan covers. Since SPEC-072
    it also scans every module-level function and lambda (:func:`_code_roots` over the module),
    named by its own name, because the engine writes ``SinkCounters`` from a module function.

    **What it checks.** Each store :func:`_self_stores` recognises whose path names a derived
    counter at **any** position — so ``self._socket.dropped_oversized += 1`` is a finding though
    its first name is ``_socket`` — and each ``setattr(self, <name>, ...)`` whose name is not a
    string literal, since nothing shows it is not a counter. The match is by attribute name across
    the tree, not within the reporting class, so it needs no inheritance resolution
    (``ElasticsearchSink.losses`` reads attributes ``HTTPSink`` writes); the cost is that an
    unrelated class storing a same-named attribute after construction is a finding too, and
    renames it. There is no allowlist: a ``dropped`` count is exact by promise (invariant 2), so a
    store that has to be immediate belongs to a counter no ``losses()`` reports as ``dropped``.
    And, since SPEC-072, each store whose chain from **any** other root names a derived counter
    — ``sink.log_foundry_counters.dropped_events += 1`` — and ``setattr(<any>, "<counter>", ...)``
    with the counter as a literal (:func:`_foreign_findings`); ``setattr(<not self>,
    <computed>, ...)`` stays silent, since it is the ``Deferral`` commit's form.

    **What it does not see.** It reads syntax. A store through an alias of ``self``
    (``me = self``), a mutating method call (``self.counts.update(...)``), a store through
    ``self.__dict__[...]`` (its path is ``__dict__``, which names no counter), a counter reported
    as ``SinkLosses(**mapping)`` (there is no ``dropped`` argument to read), a store in a
    module's top-level statements, ``__init__``'s own statements, the statements a class body runs as the class is
    defined — unless the class is defined inside scanned code, whose scan takes them as its own,
    since they can close over that code's ``self`` — and a counter reported through a call or a
    property rather than a plain ``self`` read — ``SyslogSink.losses`` returns
    ``self._socket.losses()``, which is that — are not findings. Syntax cannot say when code runs,
    either, so a function nested in ``__init__`` is a finding even when ``__init__`` calls it
    before returning, and so is a method only ``__init__`` calls; both fail closed.
    ``Deferral.__exit__`` writes through ``setattr(owner, ...)``, not ``self``, which is why the
    one permitted writer is invisible to a check that refuses every ``self`` store.

    Args:
      where: The module's path, for the message.
      source: The module's text.
      counters: The derived counter names, from :func:`dropped_counters` over the whole tree.

    Returns:
      The findings in source order, empty when the module complies.
    """
    findings: list[tuple[int, str]] = []
    tree = ast.parse(source)
    for cls in ast.walk(tree):
        if not isinstance(cls, ast.ClassDef):
            continue
        for name, fn in _scan_roots(cls):
            for path, line in _self_stores(fn, _outside_nested_methods):
                if path == "<dynamic>":
                    what = "stores through setattr(self, ...) with a computed name, which may be "
                    why = "a counter a losses() reports as dropped; name it, or count it"
                elif counters.intersection(path.split(".")):
                    what = f"stores self.{path}, which "
                    why = "a losses() reports as dropped; count it"
                else:
                    continue
                text = f"{where}:{line}: {name} {what}{why}{_THROUGH_THE_LEDGER}"
                findings.append((line, text))
            findings.extend(_foreign_findings(where, name, fn, counters, skip_self=True))
    for fn in _code_roots(tree):
        findings.extend(_foreign_findings(where, _code_name(fn), fn, counters, skip_self=False))
    return [text for _, text in sorted(findings, key=lambda finding: finding[0])]


def _foreign_findings(
    where: str,
    name: str,
    fn: ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda,
    counters: set[str],
    *,
    skip_self: bool,
) -> list[tuple[int, str]]:
    """Stores to a derived counter through a root other than ``self``, one finding each (SPEC-072).

    SPEC-072's ``SinkCounters`` is written through the sink's counters object, not ``self``, so a
    check that saw only ``self`` stores would let ``sink.log_foundry_counters.dropped_events += 1``
    pass anywhere. A store whose target chain names a derived counter at any position, from any
    root — a name, a call, a subscript — is a finding, as is ``setattr(<any>, "<counter>", ...)``
    with the counter as a string literal. ``setattr(<not self>, <computed>, ...)`` is silent: it is
    the ``Deferral`` commit's own form, and refusing it would refuse the one permitted writer. In a
    class the ``self``-rooted stores are :func:`_self_stores`' and are skipped here; at module
    level ``self`` is an ordinary name.

    Args:
      where: The module's path, for the message.
      name: The function's name, as a finding names it.
      fn: The function or lambda to scan, nested classes' functions excluded.
      counters: The derived counter names.
      skip_self: Whether ``self``-rooted stores are left to :func:`_self_stores`.

    Returns:
      ``(line, finding)`` pairs.

    Raises:
      None.
    """
    found: list[tuple[int, str]] = []
    for node in _outside_nested_methods(fn):
        target: str | None = None
        names: list[str] = []
        if isinstance(node, ast.Attribute | ast.Subscript) and isinstance(node.ctx, ast.Store):
            chain: ast.expr = node
            while isinstance(chain, ast.Attribute | ast.Subscript):
                if isinstance(chain, ast.Attribute):
                    names.append(chain.attr)
                chain = chain.value
            if skip_self and isinstance(chain, ast.Name) and chain.id == "self":
                continue
            target = ast.unparse(node)
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "setattr"
            and len(node.args) > 1
            and isinstance(node.args[1], ast.Constant)
            and isinstance(node.args[1].value, str)
        ):
            owner = node.args[0]
            if skip_self and isinstance(owner, ast.Name) and owner.id == "self":
                continue
            names = [node.args[1].value]
            target = f"{ast.unparse(owner)}.{node.args[1].value} through setattr"
        if target is None or not counters.intersection(names):
            continue
        text = (
            f"{where}:{node.lineno}: {name} stores {target}, which a losses() reports as dropped; "
            f"count it{_THROUGH_THE_LEDGER}"
        )
        found.append((node.lineno, text))
    return found


_THROUGH_THE_LEDGER = " through the calling emit's Deferral"
"""The close every store finding shares: where the count belongs instead."""


def _tree_sources() -> dict[str, str]:
    """Every module under ``src/log_foundry``, by its path relative to the package."""
    return {
        path.relative_to(_SRC).as_posix(): path.read_text(encoding="utf-8")
        for path in sorted(_SRC.rglob("*.py"))
    }


def _tree_counters(sources: dict[str, str]) -> set[str]:
    """The derived counters, unioned over the whole package."""
    counters: set[str] = set()
    for source in sources.values():
        counters |= dropped_counters(source)
    return counters


def test_every_dropped_counter_is_written_only_by_a_deferral_commit() -> None:
    """FR-005 part 3, over the real package: a ``dropped`` counter moves only in a commit.

    The roster and the allowlist hold a *site* to the helper, so a counter written in a method no
    roster entry reaches, or in a sink nobody listed, passed both. This holds the *counter*. The
    derived set must contain the floor, and no :data:`IMMEDIATE` entry may excuse a store to a
    derived counter, since that allowlist exists for counts that are not ``dropped``.
    """
    sources = _tree_sources()
    counters = _tree_counters(sources)
    assert counters >= DROPPED_FLOOR, (
        f"the derivation lost {sorted(DROPPED_FLOOR - counters)} — a refactor blinded the "
        f"derivation, or a counter was renamed (then rename it in DROPPED_FLOOR too)"
    )
    excused = {attr for attrs in IMMEDIATE.values() for attr in attrs} & counters
    assert not excused, f"IMMEDIATE excuses a store to a dropped counter: {sorted(excused)}"
    findings: list[str] = []
    for where, source in sources.items():
        findings.extend(dropped_store_findings(where, source, counters))
    assert findings == [], "\n".join(findings)


def _plant_in_on_delivery(source: str) -> tuple[str, int]:
    """``sinks/kafka.py`` with ``self.rejected += 1`` planted first in ``_on_delivery``'s body.

    The line goes after the docstring, at the body's own indentation, and its number is read
    from the parsed tree rather than written as a literal, so an edit above the method moves the
    expectation with it.
    """
    kafka = _class(ast.parse(source), "KafkaSink")
    assert kafka is not None, "KafkaSink is gone from sinks/kafka.py"
    method = _methods(kafka).get("_on_delivery")
    assert method is not None and len(method.body) > 1, "KafkaSink._on_delivery has moved"
    first = method.body[1]
    lines = source.splitlines(keepends=True)
    lines.insert(first.lineno - 1, " " * first.col_offset + "self.rejected += 1\n")
    return "".join(lines), first.lineno


def test_a_rejected_store_planted_in_an_unreached_kafka_method_is_named() -> None:
    """The gap this check closes: a store the roster never reaches is still a finding.

    ``_on_delivery`` is handed to ``produce`` uncalled, so the site check does not follow it and
    stays silent over the plant; the counter check names the module, the line, the class, the
    method and the stored path.
    """
    sources = _tree_sources()
    planted, line = _plant_in_on_delivery(sources["sinks/kafka.py"])
    named = (
        f"sinks/kafka.py:{line}: KafkaSink._on_delivery stores self.rejected, which a losses() "
        f"reports as dropped; count it through the calling emit's Deferral"
    )

    assert dropped_store_findings("sinks/kafka.py", planted, _tree_counters(sources)) == [named]
    assert (
        deferral_findings(
            "sinks/kafka.py", planted, DEFERRING_SITES["sinks/kafka.py"], IMMEDIATE, set()
        )
        == []
    ), "the premise: the site check does not reach the plant"


def _dropped_store(line: int, cls: str, method: str, path: str) -> str:
    """One expected store finding on the corpus module ``m.py``."""
    return (
        f"m.py:{line}: {cls}.{method} stores self.{path}, which a losses() reports as dropped; "
        f"count it through the calling emit's Deferral"
    )


_REPORTS = """
class Sink:
    def losses(self):
        return SinkLosses(dropped={dropped}, failed=self.failed)
    def bump(self):
        self.a += 1
        self.b += 1
"""

_DROPPED_CORPUS: dict[str, tuple[str, list[str]]] = {
    "dropped= keyword": (
        _REPORTS.format(dropped="self.a"),
        [_dropped_store(6, "Sink", "bump", "a")],
    ),
    "first positional": (
        """
class Sink:
    def losses(self):
        return SinkLosses(self.a, self.b)
    def bump(self):
        self.a += 1
        self.b += 1
""",
        [_dropped_store(6, "Sink", "bump", "a")],
    ),
    "callee spelt as an attribute": (
        """
class Sink:
    def losses(self):
        return base.SinkLosses(dropped=self.a, failed=0)
    def bump(self):
        self.a += 1
""",
        [_dropped_store(6, "Sink", "bump", "a")],
    ),
    "a sum of two attributes": (
        _REPORTS.format(dropped="self.a + self.b"),
        [_dropped_store(6, "Sink", "bump", "a"), _dropped_store(7, "Sink", "bump", "b")],
    ),
    "a local assigned from an attribute": (
        """
class Sink:
    def losses(self):
        with self._counter_lock:
            own = self.a
        return SinkLosses(dropped=own, failed=0)
    def bump(self):
        self.a += 1
""",
        [_dropped_store(8, "Sink", "bump", "a")],
    ),
    "a local assigned from a sum of two attributes": (
        """
class Sink:
    def losses(self):
        dropped = self.a + self.b
        return SinkLosses(dropped=dropped, failed=0)
    def bump(self):
        self.a += 1
        self.b += 1
""",
        [_dropped_store(7, "Sink", "bump", "a"), _dropped_store(8, "Sink", "bump", "b")],
    ),
    "a local from an annotated assignment": (
        """
class Sink:
    def losses(self):
        own: int = self.a
        return SinkLosses(dropped=own, failed=0)
    def bump(self):
        self.a += 1
""",
        [_dropped_store(7, "Sink", "bump", "a")],
    ),
    "a local from an augmented assignment": (
        """
class Sink:
    def losses(self):
        own = 0
        own += self.a
        return SinkLosses(dropped=own, failed=0)
    def bump(self):
        self.a += 1
""",
        [_dropped_store(8, "Sink", "bump", "a")],
    ),
    "a local bound by unpacking": (
        """
class Sink:
    def losses(self):
        first, *own = 0, self.a
        return SinkLosses(dropped=sum(own), failed=0)
    def bump(self):
        self.a += 1
""",
        [_dropped_store(7, "Sink", "bump", "a")],
    ),
    "a tuple-unpacked local pairs element-wise": (
        """
class Sink:
    def losses(self):
        dropped, failed = self.dropped_oversized, self.failed
        return SinkLosses(dropped=dropped, failed=failed)
    def emit(self, batch):
        self.dropped_oversized += 1
    def fail(self):
        self.failed += 1
""",
        [_dropped_store(7, "Sink", "emit", "dropped_oversized")],
    ),
    "a nested tuple-unpacked local pairs element-wise": (
        """
class Sink:
    def losses(self):
        queued, (dropped, failed) = self.queued, (self.a, self.failed)
        return SinkLosses(dropped=dropped, failed=failed)
    def bump(self):
        self.queued += 1
        self.a += 1
        self.failed += 1
""",
        [_dropped_store(8, "Sink", "bump", "a")],
    ),
    "an unequal unpacking maps each name to the whole value": (
        """
class Sink:
    def losses(self):
        first, *rest = self.x, self.y, self.z
        return SinkLosses(dropped=sum(rest), failed=0)
    def bump(self):
        self.x += 1
        self.y += 1
        self.z += 1
""",
        [
            _dropped_store(7, "Sink", "bump", "x"),
            _dropped_store(8, "Sink", "bump", "y"),
            _dropped_store(9, "Sink", "bump", "z"),
        ],
    ),
    "a starred value of equal length maps each name to the whole value": (
        """
class Sink:
    def losses(self):
        first, *rest = *self.pair, self.z
        return SinkLosses(dropped=sum(rest), failed=0)
    def bump(self):
        self.pair = ()
        self.z += 1
""",
        [_dropped_store(7, "Sink", "bump", "pair"), _dropped_store(8, "Sink", "bump", "z")],
    ),
    "a local from a walrus": (
        """
class Sink:
    def losses(self):
        if (own := self.a) >= 0:
            return SinkLosses(dropped=own, failed=0)
    def bump(self):
        self.a += 1
""",
        [_dropped_store(7, "Sink", "bump", "a")],
    ),
    "wrapped_losses with own=": (
        """
class Sink:
    def losses(self):
        return wrapped_losses(read_losses(self._inner), own=self.a)
    def bump(self):
        self.a += 1
""",
        [_dropped_store(6, "Sink", "bump", "a")],
    ),
    "wrapped_losses with the second positional": (
        """
class Sink:
    def losses(self):
        return wrapped_losses(read_losses(self._inner), self.a)
    def bump(self):
        self.a += 1
        self._inner = None
""",
        [_dropped_store(6, "Sink", "bump", "a")],
    ),
    "a counter reported by one class and stored by another": (
        """
class Reporter:
    def losses(self):
        return SinkLosses(dropped=self.a, failed=0)
class Writer:
    def bump(self):
        self.a += 1
""",
        [_dropped_store(7, "Writer", "bump", "a")],
    ),
    "every store form": (
        """
class Sink:
    def losses(self):
        return SinkLosses(dropped=self.a, failed=0)
    async def bump(self, batch):
        self.a = 1
        self.a += 1
        self.a: int = 1
        for self.a in batch:
            pass
        with open(path) as self.a:
            pass
        [0 for self.a in batch]
        {0 for self.a in batch}
        {0: 0 for self.a in batch}
        list(0 for self.a in batch)
        async for self.a in batch:
            pass
        async with lock as self.a:
            pass
        self.b, (self.a, *rest) = 1, (2, 3)
        self.a["k"] = 1
        setattr(self, "a", 1)
        del self.a
""",
        [
            _dropped_store(line, "Sink", "bump", "a")
            for line in (6, 7, 8, 9, 11, 13, 14, 15, 16, 17, 19, 21, 22, 23)
        ],
    ),
    "a store through an object self holds": (
        """
class Transport:
    def losses(self):
        return SinkLosses(dropped=self.dropped_oversized, failed=0)
class Sink:
    def emit(self, batch):
        self._socket.dropped_oversized += 1
        self._socket = None
""",
        [_dropped_store(7, "Sink", "emit", "_socket.dropped_oversized")],
    ),
    "a dynamic setattr(self, ...)": (
        """
class Sink:
    def losses(self):
        return SinkLosses(dropped=self.a, failed=0)
    def bump(self, name):
        setattr(self, name, 1)
        setattr(self, "b", 1)
""",
        [
            (
                "m.py:6: Sink.bump stores through setattr(self, ...) with a computed name, which "
                "may be a counter a losses() reports as dropped; name it, or count it through the "
                "calling emit's Deferral"
            )
        ],
    ),
    "a chained read derives its last name": (
        """
class Sink:
    def losses(self):
        return SinkLosses(dropped=self.a.n, failed=0)
    def bump(self):
        self.a = None
        self.a.n += 1
""",
        [_dropped_store(7, "Sink", "bump", "a.n")],
    ),
    "a chained read through a subscript derives its last name": (
        """
class Sink:
    def losses(self):
        return SinkLosses(dropped=self.by_kind["x"].n, failed=0)
    def bump(self):
        self.by_kind = {}
        self.by_kind["x"].n += 1
""",
        [_dropped_store(7, "Sink", "bump", "by_kind.n")],
    ),
    "a closure in __init__ is scanned, __init__'s own store is not": (
        """
class Sink:
    def __init__(self, producer):
        self.rejected = 0
        def refused(err):
            self.rejected += 1
        async def drained():
            self.rejected = 0
        producer.on_error = refused
    def losses(self):
        return SinkLosses(dropped=self.rejected, failed=0)
""",
        [
            _dropped_store(6, "Sink", "__init__.<locals>.refused", "rejected"),
            _dropped_store(8, "Sink", "__init__.<locals>.drained", "rejected"),
        ],
    ),
    "a lambda in __init__ is scanned": (
        """
class Sink:
    def __init__(self, producer):
        self.rejected = 0
        producer.on_error = lambda err: setattr(self, "rejected", self.rejected + 1)
    def losses(self):
        return SinkLosses(dropped=self.rejected, failed=0)
""",
        [_dropped_store(5, "Sink", "__init__.<locals>.<lambda>", "rejected")],
    ),
    "a method under a class-body if or else": (
        """
class Sink:
    def losses(self):
        return SinkLosses(dropped=self.a, failed=0)
    if True:
        def bump(self):
            self.a += 1
    else:
        def bump(self):
            self.a += 1
""",
        [_dropped_store(7, "Sink", "bump", "a"), _dropped_store(10, "Sink", "bump", "a")],
    ),
    "a method under every other class-body block": (
        """
class Sink:
    def losses(self):
        return SinkLosses(dropped=self.a, failed=0)
    try:
        def b1(self):
            self.a += 1
    except ImportError:
        def b2(self):
            self.a += 1
    finally:
        def b3(self):
            self.a += 1
    with lock:
        def b4(self):
            self.a += 1
    for _ in ():
        def b5(self):
            self.a += 1
    while False:
        def b6(self):
            self.a += 1
    match mode:
        case _:
            def b7(self):
                self.a += 1
""",
        [
            _dropped_store(line, "Sink", method, "a")
            for line, method in (
                (7, "b1"),
                (10, "b2"),
                (13, "b3"),
                (16, "b4"),
                (19, "b5"),
                (22, "b6"),
                (26, "b7"),
            )
        ],
    ),
    "a lambda in the class body": (
        """
class Sink:
    def losses(self):
        return SinkLosses(dropped=self.a, failed=0)
    bump = lambda self: setattr(self, "a", 1)
""",
        [_dropped_store(5, "Sink", "<lambda>", "a")],
    ),
    "a nested class is scanned once, as its own": (
        """
class Sink:
    def losses(self):
        return SinkLosses(dropped=self.a, failed=0)
    class Inner:
        def bump(self):
            self.a += 1
""",
        [_dropped_store(7, "Inner", "bump", "a")],
    ),
    "a closure in a method is reported once, as the method": (
        """
class Sink:
    def losses(self):
        return SinkLosses(dropped=self.a, failed=0)
    def start(self, client):
        def bump(err):
            self.a += 1
        client.on_error = bump
""",
        [_dropped_store(7, "Sink", "start", "a")],
    ),
    "a class defined in a method: its body with the method, its methods once": (
        """
class Sink:
    def losses(self):
        return SinkLosses(dropped=self.a, failed=0)
    def make(self):
        class Inner:
            self.a += 1
            def bump(self):
                self.a += 1
        return Inner
""",
        [_dropped_store(7, "Sink", "make", "a"), _dropped_store(9, "Inner", "bump", "a")],
    ),
    "a store in __init__ is silent": (
        """
class Sink:
    def __init__(self):
        self.a = 0
    def losses(self):
        return SinkLosses(dropped=self.a, failed=0)
""",
        [],
    ),
    "a store to failed is silent": (
        """
class Sink:
    def losses(self):
        return SinkLosses(dropped=self.a, failed=self.failed)
    def bump(self):
        self.failed += 1
""",
        [],
    ),
    "a count through the ledger is silent": (
        """
class Sink:
    def losses(self):
        return SinkLosses(dropped=self.rejected, failed=0)
    def emit(self, batch):
        with Deferral(self) as pending:
            pending.count("rejected")
""",
        [],
    ),
    "a class reporting dropped=0 is silent": (_REPORTS.format(dropped="0"), []),
    "a sum over children reads no self attribute": (
        """
class Fanout:
    def losses(self):
        children = [read_losses(sink) for sink in self._sinks]
        reported = [child for child in children if child is not None]
        return SinkLosses(dropped=sum(child.dropped for child in reported), failed=0)
    def replace(self, sinks):
        self._sinks = sinks
""",
        [],
    ),
}
"""The derived check's corpus: each derivation form and store form, and the shapes that are silent.

Each case is one synthetic module whose counters are derived from itself, so a case exercises the
derivation and the store scan together; the failure text is asserted whole. The cases on where a
store sits — a closure or lambda in ``__init__``, a method under a class-body block, a nested or
locally defined class — assert the whole list, so a store reported twice fails as surely as one
missed.
"""


_COUNTERS_CLASS = """
class SinkCounters:
    def __init__(self):
        self.dropped_events = 0
    def losses(self):
        dropped = self.dropped_events
        return SinkLosses(dropped=dropped, failed=0)
"""


def _foreign(line: int, name: str, target: str) -> str:
    """One expected store finding for a store through a root other than ``self``."""
    return (
        f"m.py:{line}: {name} stores {target}, which a losses() reports as dropped; count it "
        f"through the calling emit's Deferral"
    )


_DROPPED_CORPUS.update(
    {
        "a module function's chain store is named": (
            _COUNTERS_CLASS
            + "def _run(sink, n):\n    sink.log_foundry_counters.dropped_events += n\n",
            [_foreign(9, "_run", "sink.log_foundry_counters.dropped_events")],
        ),
        "a module function's literal setattr is named": (
            _COUNTERS_CLASS + 'def _run(c):\n    setattr(c, "dropped_events", 1)\n',
            [_foreign(9, "_run", "c.dropped_events through setattr")],
        ),
        "a call-rooted chain is named": (
            _COUNTERS_CLASS + "def _run(get):\n    get().dropped_events = 2\n",
            [_foreign(9, "_run", "get().dropped_events")],
        ),
        "a lambda at module level is scanned": (
            _COUNTERS_CLASS + "bump = lambda c: setattr(c, 'dropped_events', 1)\n",
            [_foreign(8, "<lambda>", "c.dropped_events through setattr")],
        ),
        "a method's store through another object is named": (
            _COUNTERS_CLASS
            + "class Engine:\n    def run(self, other):\n        other.dropped_events = 0\n",
            [_foreign(10, "Engine.run", "other.dropped_events")],
        ),
        "the Deferral commit's computed setattr is silent": (
            _COUNTERS_CLASS
            + "class Deferral:\n"
            + "    def __exit__(self, owner, name, n):\n"
            + "        setattr(owner, name, getattr(owner, name) + n)\n"
            + "def _commit(owner, name):\n    setattr(owner, name, 1)\n",
            [],
        ),
        "the counters' own construction is silent": (_COUNTERS_CLASS, []),
        "a name that only contains a counter is silent": (
            _COUNTERS_CLASS + "def _run(x):\n    x.dropped_eventsX = 1\n    x.n.events = 2\n",
            [],
        ),
    }
)


@pytest.mark.parametrize("case", sorted(_DROPPED_CORPUS))
def test_the_dropped_counter_check_reports_what_it_claims_to(case: str) -> None:
    """Each derivation and store form's finding text, and the shapes that must stay silent."""
    source, expected = _DROPPED_CORPUS[case]
    assert dropped_store_findings("m.py", source, dropped_counters(source)) == expected


# --- SPEC-073 FR-005: a dropped counter counted through a run context commits only if settled ----


def _is_when_settled(node: ast.expr | None) -> bool:
    """Whether ``when=`` is ``When.SETTLED``, spelt bare or through a module (``_engine.When``).

    A ``.SETTLED`` on anything not named ``When`` is not the run context's member, so it does
    not count: the gate reads the spelling, and a lookalike base fails closed.
    """
    if not (isinstance(node, ast.Attribute) and node.attr == "SETTLED"):
        return False
    base = node.value
    if isinstance(base, ast.Name):
        return base.id == "When"
    return isinstance(base, ast.Attribute) and base.attr == "When"


def run_context_count_findings(where: str, source: str, counters: set[str]) -> list[str]:
    """Every run-context ``count`` that could count a ``dropped`` counter in a run that failed.

    A sink on the delivery engine counts through ``RunContext.count(owner, name, n, *, when)``
    rather than storing, so :func:`dropped_store_findings` sees no store, and a ``when`` other than
    ``When.SETTLED`` would count a drop that a failed run's catcher already counts. The call is
    recognised by **shape**: an attribute call named ``count`` that passes ``when=`` (which
    ``RunContext.count`` requires, keyword-only), or names ``owner=`` or ``name=``, or takes
    three or more positional arguments. ``Deferral.count(counter, n)``, the context's ``drops``
    view and any ledger forwarding them take the counter first and at most two positionals with
    no ``when=``, so they are not this shape and stay silent: each is committed only by a settled
    run or a returning ``emit``.

    In that shape the counter's name is ``name=`` when given, else the second positional. A
    ``when=When.SETTLED`` (bare, or through a module's ``When``) is silent whatever the name.
    Otherwise a name that is missing or not a string literal is a finding, because the gate cannot
    tell which counter it is (it fails closed), and a literal naming one of ``counters`` is a
    finding too.

    Args:
      where: The module's path, for the message.
      source: The module's text.
      counters: The counters some ``losses()`` reports as ``dropped``.

    Returns:
      One line per finding, in source order.
    """
    findings: list[tuple[int, str]] = []
    for node in ast.walk(ast.parse(source)):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "count"
        ):
            continue
        keywords = {k.arg: k.value for k in node.keywords if k.arg is not None}
        if not (
            "when" in keywords or "owner" in keywords or "name" in keywords or len(node.args) >= 3
        ):
            continue
        when = keywords.get("when")
        if _is_when_settled(when):
            continue
        shown = ast.unparse(when) if when is not None else "no when"
        name = keywords.get("name", node.args[1] if len(node.args) >= 2 else None)
        if not (isinstance(name, ast.Constant) and isinstance(name.value, str)):
            spelt = ast.unparse(name) if name is not None else "nothing"
            findings.append(
                (
                    node.lineno,
                    (
                        f"{where}:{node.lineno}: a run-context count names its counter by "
                        f"{spelt}, which the gate cannot read, with {shown}; name it by a literal "
                        f"or pass when=When.SETTLED"
                    ),
                )
            )
        elif name.value in counters:
            findings.append(
                (
                    node.lineno,
                    (
                        f"{where}:{node.lineno}: the dropped counter {name.value} is counted "
                        f"through a run context with {shown}; a dropped count commits only when "
                        f"the run settles"
                    ),
                )
            )
    return [text for _, text in sorted(findings)]


def test_every_run_context_count_of_a_dropped_counter_is_settled() -> None:
    """FR-005: over the real package, a run-context ``count`` of a dropped counter is ``SETTLED``.

    The derivation of the dropped counters is :func:`_tree_counters`'s, so this holds the same
    population :func:`test_every_dropped_counter_is_written_only_by_a_deferral_commit` holds.
    """
    sources = _tree_sources()
    counters = _tree_counters(sources)
    assert counters >= DROPPED_FLOOR
    findings: list[str] = []
    for where, source in sources.items():
        findings.extend(run_context_count_findings(where, source, counters))
    assert findings == [], "\n".join(findings)


_RUN_CONTEXT_CORPUS: dict[str, tuple[str, list[str]]] = {
    "settled-literal": ('ctx.count(self, "dropped_oversized", 1, when=When.SETTLED)\n', []),
    "settled-non-literal": ("ctx.count(self, name, 1, when=When.SETTLED)\n", []),
    "not-a-dropped-counter": ('ctx.count(self, "failed", 1, when=When.ALWAYS)\n', []),
    "a-deferral-count-without-when": ('pending.count("dropped_oversized", 2)\n', []),
    "settled-through-a-module": (
        'ctx.count(self, "dropped_oversized", 1, when=_engine.When.SETTLED)\n',
        [],
    ),
    "a-forwarding-ledger": ("ledger.count(name, n)\n", []),
    "keyword-n": (
        'ctx.count(self, "dropped_oversized", n=1, when=When.ALWAYS)\n',
        [
            (
                "m.py:1: the dropped counter dropped_oversized is counted through a run context "
                "with When.ALWAYS; a dropped count commits only when the run settles"
            )
        ],
    ),
    "all-keywords": (
        'ctx.count(owner=self, name="dropped_oversized", n=1, when=When.ALWAYS)\n',
        [
            (
                "m.py:1: the dropped counter dropped_oversized is counted through a run context "
                "with When.ALWAYS; a dropped count commits only when the run settles"
            )
        ],
    ),
    "non-literal-name-keyword": (
        "ctx.count(owner=self, name=counter, n=1, when=When.ALWAYS)\n",
        [
            (
                "m.py:1: a run-context count names its counter by counter, which the gate cannot "
                "read, with When.ALWAYS; name it by a literal or pass when=When.SETTLED"
            )
        ],
    ),
    "a-settled-that-is-not-when": (
        'ctx.count(self, "dropped_oversized", 1, when=Other.SETTLED)\n',
        [
            (
                "m.py:1: the dropped counter dropped_oversized is counted through a run context "
                "with Other.SETTLED; a dropped count commits only when the run settles"
            )
        ],
    ),
    "always-literal": (
        'ctx.count(self, "dropped_oversized", 1, when=When.ALWAYS)\n',
        [
            (
                "m.py:1: the dropped counter dropped_oversized is counted through a run context with "
                "When.ALWAYS; a dropped count commits only when the run settles"
            )
        ],
    ),
    "last-literal": (
        'x = 1\nctx.count(self, "dropped_oversized", n, when=When.LAST)\n',
        [
            (
                "m.py:2: the dropped counter dropped_oversized is counted through a run context with "
                "When.LAST; a dropped count commits only when the run settles"
            )
        ],
    ),
    "no-when": (
        'ctx.count(self, "dropped_oversized", 1)\n',
        [
            (
                "m.py:1: the dropped counter dropped_oversized is counted through a run context with "
                "no when; a dropped count commits only when the run settles"
            )
        ],
    ),
    "non-literal-fails-closed": (
        "ctx.count(self, name, 1, when=When.ALWAYS)\n",
        [
            (
                "m.py:1: a run-context count names its counter by name, which the gate cannot read, "
                "with When.ALWAYS; name it by a literal or pass when=When.SETTLED"
            )
        ],
    ),
}
"""Each shape's finding, and the shapes that must stay silent, for the counter ``dropped_oversized``."""


@pytest.mark.parametrize("case", sorted(_RUN_CONTEXT_CORPUS))
def test_the_run_context_count_check_reports_what_it_claims_to(case: str) -> None:
    source, expected = _RUN_CONTEXT_CORPUS[case]
    assert run_context_count_findings("m.py", source, {"dropped_oversized"}) == expected
