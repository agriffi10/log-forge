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
    "sinks/kinesis.py": {"KinesisSink": ("emit", "_records")},
    "sinks/firehose.py": {"FirehoseSink": ("emit", "_records")},
    "sinks/sns.py": {"SNSSink": ("emit", "_bodies")},
    "sinks/mongodb.py": {"MongoDBSink": ("emit", "_documents")},
    "sinks/eventhubs.py": {"AzureEventHubsSink": ("emit", "_send_batch")},
    "sinks/_socket.py": {"SocketTransport": ("send_all", "_sendable")},
    "sinks/syslog.py": {"SyslogSink": ("emit",)},
    "sinks/logstash.py": {"LogstashSink": ("emit",)},
}
"""The functions in each site that decide a per-event drop, by module, class and name.

A class or function named here and missing from its module is a finding, not a skip: a rename
would otherwise shrink the guard as silently as a deletion. The other direction is held too: a
class anywhere under ``src/`` whose ``emit`` enters a ``Deferral`` and is not named here with
``emit`` is a finding, so converting a sink to the helper without listing it cannot leave its
old immediate stores unchecked.
"""

IMMEDIATE: dict[tuple[str, str], frozenset[str]] = {
    ("MultiSink", "emit"): frozenset({"failed"}),
    ("HTTPSink", "_abandon"): frozenset({"failed"}),
    ("HTTPSink", "_abandon_unreduced"): frozenset({"failed"}),
    ("HTTPSink", "_drop_refused"): frozenset({"dropped_oversized"}),
    ("SQSSink", "_send"): frozenset({"failed"}),
    ("KinesisSink", "_send"): frozenset({"failed", "dropped_unadjudicated"}),
    ("FirehoseSink", "_send"): frozenset({"failed", "dropped_unadjudicated"}),
    ("SNSSink", "_send"): frozenset({"failed"}),
    ("MongoDBSink", "emit"): frozenset({"failed"}),
    ("AzureEventHubsSink", "_send"): frozenset({"failed"}),
    ("SocketTransport", "_send_one"): frozenset({"failed"}),
    ("SocketTransport", "_socket"): frozenset({"_sock"}),
    ("SocketTransport", "_reset"): frozenset({"_sock"}),
}
"""The stores the spec keeps immediate on purpose, keyed by the method making them; the only ones.

Derived by running the gate over the tree with this dict empty, not written from memory, and
held exact by :func:`stale_immediate`: an entry no deferring function reaches is a finding. Each
entry, with why it counts as it happens rather than through the ``Deferral``:

- ``MultiSink.emit`` / ``failed``: child *calls* that raised, outside ``losses()``; FR-007 keeps
  it moving per call exactly as before.
- ``HTTPSink._abandon`` / ``failed``: a request abandoned past the retry bound, counted before
  the raise by design; ``failed`` is an upper bound SPEC-058 does not recount (*Out of Scope*).
- ``HTTPSink._abandon_unreduced`` / ``failed``: a chunk the reduction budget could not shrink;
  the same upper bound, the same *Out of Scope* clause.
- ``HTTPSink._drop_refused`` / ``dropped_oversized``: a ``413`` drop, counted as it happens; its
  conditional path is the drop-recount follow-up's (*Out of Scope*), not this spec's.
- ``SQSSink._send`` / ``failed``: entries failed past the bound and sender faults, counted as they
  happen, before ``emit`` can raise; the same upper bound, the same *Out of Scope* clause.
- ``KinesisSink._send`` and ``FirehoseSink._send`` / ``failed``, ``dropped_unadjudicated``:
  records failed past the bound, and records in a chunk whose response could not be paired to
  them, which ``losses()`` folds into ``failed``; an upper bound SPEC-059 does not recount.
- ``SNSSink._send`` / ``failed``: entries failed past the bound; the same upper bound.
- ``MongoDBSink.emit`` / ``failed``: rejected documents and connection errors past the bound; the
  same upper bound.
- ``AzureEventHubsSink._send`` / ``failed``: a batch abandoned past the bound; the same upper
  bound, its line still written under the emit lock as before (SPEC-059 *Out of Scope*).
- ``SocketTransport._send_one`` / ``failed``: a message abandoned past the reconnect bound; the
  same upper bound.
- ``SocketTransport._socket`` and ``SocketTransport._reset`` / ``_sock``: the held socket, opened
  and dropped as the connection comes and goes; state, not a count.
"""


class _Scope(ast.NodeVisitor):
    """Collects ``Deferral(...)`` calls, and which are ``with`` items in an ``emit`` and how bound.

    A ``with`` item inside an ``emit`` is *blessed* when it binds nothing or a bare local name.
    One bound to anything else — ``as self._pending``, ``as box[0]`` — is recorded in ``bound``
    with its target, because the target outlives the call and so shares the pending drops.
    """

    def __init__(self) -> None:
        self.functions: list[str] = []
        self.calls: list[ast.Call] = []
        self.blessed: set[int] = set()
        self.bound: dict[int, ast.expr] = {}

    def _function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        self.functions.append(node.name)
        self.generic_visit(node)
        self.functions.pop()

    visit_FunctionDef = _function
    visit_AsyncFunctionDef = _function

    def visit_With(self, node: ast.With) -> None:
        if self.functions and self.functions[-1] == "emit":
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


def _self_stores(fn: ast.FunctionDef) -> list[tuple[str, int]]:
    """Every store to state reached through ``self`` in ``fn``, nested code included, by line.

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
    for node in ast.walk(fn):
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
    no class to list, so it is held only to the construction rule.
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
    scope = _Scope()
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
    for cls_name, functions in sites.items():
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


@pytest.mark.parametrize("case", sorted(_CORPUS))
def test_the_deferral_lint_reports_what_it_claims_to(case: str) -> None:
    """Each check's failure text, and the shapes that must stay silent, on synthetic modules."""
    source, sites, expected = _CORPUS[case]
    assert deferral_findings("m.py", source, sites, IMMEDIATE) == expected


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
    """FR-002's instrument, over the real package: five sites, one helper, no second ledger.

    The construction rule and the roster rule run over every module under ``src/``, so a sixth
    site that builds a ``Deferral`` is held to the first, and one whose ``emit`` enters a
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
