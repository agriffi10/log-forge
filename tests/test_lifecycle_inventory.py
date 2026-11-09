"""SPEC-077 FR-003: the home of every lifecycle field and lock is declared, and the table is gated.

"Nothing else holds lifecycle state" is true only until the next field is added, so this derives
three populations by AST and requires each member to have a row in :data:`INVENTORY`, and each row
to match a member. A new lock or flag with no row fails by name; so does a row whose site is gone,
which is how a rename shrinks a roster silently.

**The populations.**

- *Constructions*: every ``Lock``, ``RLock``, ``Event``, ``Condition``, ``Semaphore``,
  ``BoundedSemaphore`` or ``Barrier`` built in the six files of :data:`FILES`, found however
  ``threading`` is imported (``test_fork_lifecycle._primitive_constructions``) and keyed
  ``(file, enclosing function or <module>, assignment target or <expr>, constructor)``. A site that
  stores nothing — ``_fork``'s type probes and its replacement factory — has home ``none``.
- *Fields*: every attribute assigned (plainly, augmented or annotated) on ``self`` in **any**
  method of ``Runtime`` or ``Worker``, and on ``_state``, ``_lifecycle._state`` or a module-level
  alias of either (``worker.py``'s ``_lifecycle_state``, derived rather than listed) anywhere.
- *Module state in* ``_lifecycle.py``: a module-level binding is a **constant** when its name with
  leading underscores removed is upper case, no function names it in ``global``, and its value is
  a literal, a numeric expression of literals, a ``tuple`` display, or a ``tuple``/``frozenset``
  call over literals, names and ``type(...)`` calls. A ``set`` display or call is state, since a
  set is mutable. Every other binding that is not an import, a function or a class is state.

**The child column** says what a forked child does with the entry: ``walk`` (a lock or event the
repair walk replaces), ``reset: <function>`` (a registered child handler, or a function or method
one of them reaches, writes it), ``inherited``, or ``none``; a tuple is a sequence, as in
``Runtime._stop``'s "walked, then replaced on resume". A ``walk`` entry must hold a lock or event:
built as one, or — one hop — assigned from a call whose function returns a field that is
(``Worker._stop`` is handed ``Runtime.refresh_stop_signal()``, which returns ``self._stop``).

**Callees are resolved by receiver, not by bare name.** ``self.m()`` is the enclosing class's
method; ``_state.m()`` and its aliases are ``Runtime``'s; a local assigned from a call whose
function is annotated to return one known class (``worker = _state.worker_exists()``, which
returns ``Worker | None``) is that class; ``_fork.f()`` is that module's function; a bare ``f()``
is resolved only in its own module. Resolving by bare name made ``worker._reinit_after_fork``
match both ``Worker._reinit_after_fork`` and ``_fork._reinit_after_fork``, and the closure from
``_rebuild_worker_after_fork`` then spanned two dozen functions writing about fifty names, so any
field the repair touched anywhere would have passed. **The remaining limit**: a write is matched
to its owner only when its receiver resolves the same way; a write through an unresolved receiver
(``other.x = ...`` on an untyped local) is credited to every class with a field of that name, so
the check can pass a reset that writes the right name on the wrong object.

**Left out of the code table**: the spec's Data Model also lists ``_fork._child_handlers``,
``_installed`` and ``reacquired_in_child``. They belong to no population here — they are
``_fork``'s own module state, and FR-003's module-state population is ``_lifecycle.py`` alone — so
a row for them would read as a stale entry. They stay in the Data Model as prose; the fork repair's
state is SPEC-079's.
"""

from __future__ import annotations

import ast
import pathlib
import re
from dataclasses import dataclass, field

import pytest

import log_foundry
from test_fork_lifecycle import _PRIMITIVES, _primitive_constructions, _threading_names

_SRC = pathlib.Path(log_foundry.__file__).resolve().parent

FILES = ("_lifecycle.py", "worker.py", "_fork.py", "config.py", "decorator.py", "console.py")
PAIR = ("_lifecycle.py", "worker.py")
FIELD_CLASSES = ("Runtime", "Worker")
STATE_CLASS = "Runtime"
MODULE_STATE_FILE = "_lifecycle.py"
WALKABLE = frozenset({"Lock", "RLock", "Event"})
TOKEN_SETS = ("_swaps_running", "_stops_running")
TOKEN_SETS_CHILD = ("reset: _lifecycle._clear_after_fork",)
REPAIR_ITSELF = ("_fork", "_reinit_after_fork")
CHILD_VALUE = re.compile(r"^(walk|inherited|none|reset: [A-Za-z_][\w]*\.[A-Za-z_]\w*)$")

PAIR_FLOOR = 11
OUTSIDE_FLOOR = 9
"""Construction floors, measured at ``3d65cf32``: 11 in the pair and 9 outside it.

The spec's Overview counts 8 outside with a line grep; ``_fork.py:422`` holds two constructions on
one line (``threading.RLock() if ... else threading.Lock()``), which an AST population counts as
two. Lower bounds, so a new lock raises the count without editing these; a moved lock is still one
construction, so the SPEC-077 moves did not lower either.
"""

UNDECLARED_CONSTRUCTION = "a lock or event is built with no inventory row"
UNDECLARED_FIELD = "a Runtime or Worker field is assigned with no inventory row"
UNDECLARED_MODULE_STATE = "a module-level binding in _lifecycle.py is state with no inventory row"
STALE_ENTRY = "an inventory row matches no construction, field or binding"
WRONG_HOME = "an inventory row names a home other than the one the source gives it"
BAD_CHILD = "an inventory row has no child value, or one that is not walk/reset/inherited/none"
WALK_NOT_PRIMITIVE = "a walk row holds something other than a lock or an event"
RESET_NOT_REGISTERED = "a reset row names a function no child handler is or reaches"
RESET_DOES_NOT_WRITE = "a reset row's function, and what it reaches, never writes the entry"
TOKEN_SETS_ROW = "the token sets must read reset: _lifecycle._clear_after_fork"  # noqa: S105

Key = tuple[str, ...]


@dataclass(frozen=True)
class Row:
    """One inventory entry: where the state lives, what a forked child does with it, and why."""

    home: str
    child: tuple[str, ...]
    reason: str


@dataclass(frozen=True)
class Finding:
    """One refusal, carrying its reason constant so a fixture can assert the reason, not a count."""

    reason: str
    key: Key


@dataclass
class _Ctx:
    """Where a node sits: module, class, and the classes the function's locals are known to hold."""

    module: str
    cls: str | None
    local_types: dict[str, str] = field(default_factory=dict)


@dataclass
class _Assigned:
    """One assignment of a field: the value written and where it was written."""

    value: ast.expr | None
    ctx: _Ctx


class _Index:
    """The parsed sources, their functions, and receiver-based call resolution."""

    def __init__(self, sources: dict[str, str]) -> None:
        self.trees = {name: ast.parse(text) for name, text in sources.items()}
        self.modules = {name.removesuffix(".py") for name in sources}
        self.functions: dict[Key, ast.FunctionDef | ast.AsyncFunctionDef] = {}
        self.ctx_of: dict[Key, _Ctx] = {}
        self.classes: set[str] = set()
        self.state_aliases = {"_state", "_lifecycle._state"}
        for name, tree in self.trees.items():
            module = name.removesuffix(".py")
            for node in tree.body:
                if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                    self.functions[(module, node.name)] = node
                    self.ctx_of[(module, node.name)] = _Ctx(module, None)
                elif isinstance(node, ast.ClassDef):
                    self.classes.add(node.name)
                    for item in node.body:
                        if isinstance(item, ast.FunctionDef | ast.AsyncFunctionDef):
                            self.functions[(node.name, item.name)] = item
                            self.ctx_of[(node.name, item.name)] = _Ctx(module, node.name)
                elif (
                    isinstance(node, ast.Assign)
                    and len(node.targets) == 1
                    and isinstance(node.targets[0], ast.Name)
                    and ast.unparse(node.value) in {"_state", "_lifecycle._state"}
                ):
                    self.state_aliases.add(node.targets[0].id)
        for key, function in self.functions.items():
            self._type_locals(function, self.ctx_of[key])

    def _returned_class(self, key: Key) -> str | None:
        """The one known class a function's return annotation names, if exactly one."""
        returns = self.functions[key].returns
        if returns is None:
            return None
        named = {n.id for n in ast.walk(returns) if isinstance(n, ast.Name)} & self.classes
        named |= {
            c for c in self.classes if isinstance(returns, ast.Constant) and c in str(returns.value)
        }
        return next(iter(named)) if len(named) == 1 else None

    def _type_locals(self, function: ast.AST, ctx: _Ctx) -> None:
        """Records the class a local holds when its assignment says so."""
        for node in ast.walk(function):
            if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                named = {n.id for n in ast.walk(node.annotation) if isinstance(n, ast.Name)}
                hits = named & self.classes
                if len(hits) == 1:
                    ctx.local_types[node.target.id] = next(iter(hits))
            elif (
                isinstance(node, ast.Assign)
                and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and isinstance(node.value, ast.Call)
            ):
                for callee in self.resolve(node.value, ctx):
                    returned = self._returned_class(callee)
                    if returned is not None:
                        ctx.local_types[node.targets[0].id] = returned

    def receiver(self, expr: ast.expr, ctx: _Ctx) -> str | None:
        """The class an attribute's receiver is known to be, or ``None``."""
        if isinstance(expr, ast.Name) and expr.id == "self":
            return ctx.cls
        if ast.unparse(expr) in self.state_aliases:
            return STATE_CLASS
        if isinstance(expr, ast.Name):
            return ctx.local_types.get(expr.id)
        return None

    def resolve(self, call: ast.Call, ctx: _Ctx) -> list[Key]:
        """The function a call reaches, by its receiver; empty when it cannot be said."""
        func = call.func
        if isinstance(func, ast.Name):
            key = (ctx.module, func.id)
            return [key] if key in self.functions else []
        if isinstance(func, ast.Attribute):
            cls = self.receiver(func.value, ctx)
            if cls is not None and (cls, func.attr) in self.functions:
                return [(cls, func.attr)]
            if isinstance(func.value, ast.Name) and func.value.id in self.modules:
                key = (func.value.id, func.attr)
                return [key] if key in self.functions else []
        return []

    def closure(self, start: Key) -> set[Key]:
        """Every function reachable from ``start`` through resolved calls, ``start`` included."""
        seen = {start}
        stack = [start]
        while stack:
            key = stack.pop()
            ctx = self.ctx_of[key]
            for node in ast.walk(self.functions[key]):
                if isinstance(node, ast.Call):
                    for callee in self.resolve(node, ctx):
                        if callee not in seen:
                            seen.add(callee)
                            stack.append(callee)
        return seen

    def registered(self) -> set[Key]:
        """Every function passed to ``register_child_handler`` anywhere in the sources."""
        found: set[Key] = set()
        for name, tree in self.trees.items():
            module = name.removesuffix(".py")
            for node in ast.walk(tree):
                if (
                    isinstance(node, ast.Call)
                    and ast.unparse(node.func).endswith("register_child_handler")
                    and node.args
                    and isinstance(node.args[0], ast.Name)
                ):
                    found.add((module, node.args[0].id))
        return found


def _qualname(parents: dict[int, ast.AST], node: ast.AST) -> tuple[str, str | None]:
    """A node's enclosing ``Class.function`` (or ``<module>``) and its enclosing class."""
    names: list[str] = []
    cls: str | None = None
    current = parents.get(id(node))
    while current is not None:
        if isinstance(current, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            names.append(current.name)
            if isinstance(current, ast.ClassDef) and cls is None:
                cls = current.name
        current = parents.get(id(current))
    return (".".join(reversed(names)) or "<module>"), cls


def _parents(tree: ast.AST) -> dict[int, ast.AST]:
    """Each node's parent, by id."""
    return {id(child): node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}


@dataclass
class Populations:
    """What the sources contain, derived, with the facts the checks need about each member."""

    homes: dict[Key, str] = field(default_factory=dict)
    constructor: dict[Key, str] = field(default_factory=dict)
    assigned: dict[Key, list[_Assigned]] = field(default_factory=dict)
    walkable_values: set[int] = field(default_factory=set)


def _constructions(index: _Index, pops: Populations) -> None:
    """Every primitive construction, keyed, with the home its assignment gives it."""
    for name, tree in index.trees.items():
        module = name.removesuffix(".py")
        parents = _parents(tree)
        globals_named = {n for g in ast.walk(tree) if isinstance(g, ast.Global) for n in g.names}
        _, direct = _threading_names(tree)
        for call in _primitive_constructions(tree, _PRIMITIVES):
            func = call.func
            ctor = func.attr if isinstance(func, ast.Attribute) else direct[ast.unparse(func)]
            qual, cls = _qualname(parents, call)
            stored = True
            node: ast.AST = call
            parent = parents.get(id(node))
            while isinstance(parent, ast.IfExp):
                node, parent = parent, parents.get(id(parent))
            statement: ast.AST | None = parent
            while statement is not None and not isinstance(statement, ast.stmt):
                stored = False
                statement = parents.get(id(statement))
            target_text = "<expr>"
            home = "none"
            if isinstance(statement, ast.Assign | ast.AnnAssign):
                target = (
                    statement.targets[0] if isinstance(statement, ast.Assign) else statement.target
                )
                target_text = ast.unparse(target)
                stored = stored and statement.value is node
                if (
                    stored
                    and isinstance(target, ast.Attribute)
                    and ast.unparse(target.value) == "self"
                ):
                    home = cls or "none"
                elif (
                    stored
                    and isinstance(target, ast.Name)
                    and (qual == "<module>" or target.id in globals_named)
                ):
                    home = module
            if ctor in WALKABLE:
                pops.walkable_values.add(id(call))
            key = ("construction", name, qual, target_text, ctor)
            pops.homes[key] = home
            pops.constructor[key] = ctor


def _fields(index: _Index, pops: Populations) -> None:
    """Every field of ``Runtime`` and ``Worker``, and every write to ``_state`` or an alias."""
    for name, tree in index.trees.items():
        module = name.removesuffix(".py")
        parents = _parents(tree)
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                targets, value = list(node.targets), node.value
            elif isinstance(node, ast.AnnAssign):
                targets, value = [node.target], node.value
            elif isinstance(node, ast.AugAssign):
                targets, value = [node.target], None
            else:
                continue
            qual, cls = _qualname(parents, node)
            fn_key: Key = (cls, qual.split(".")[-1]) if cls else (module, qual.split(".")[-1])
            ctx = index.ctx_of.get(fn_key, _Ctx(module, cls))
            for target in targets:
                if not isinstance(target, ast.Attribute):
                    continue
                receiver = ast.unparse(target.value)
                owner = None
                if receiver == "self" and cls in FIELD_CLASSES:
                    owner = cls
                elif receiver in index.state_aliases:
                    owner = STATE_CLASS
                if owner is None:
                    continue
                key = ("field", owner, target.attr)
                pops.homes[key] = owner
                pops.assigned.setdefault(key, []).append(_Assigned(value, ctx))


def _is_constant_value(node: ast.expr) -> bool:
    """A literal, a numeric expression of literals, a tuple display, or a tuple/frozenset call.

    A ``set`` is not a constant, display or call: it is mutable, so ``_PENDING = set()`` is state
    whatever its name says. A ``frozenset`` or ``tuple`` call may take a tuple or set display of
    literal parts, which is how ``_PLAIN_TYPES = frozenset({dict, ...})`` is written.
    """
    if isinstance(node, ast.Constant):
        return True
    if isinstance(node, ast.UnaryOp):
        return _is_constant_value(node.operand)
    if isinstance(node, ast.BinOp):
        return _is_constant_value(node.left) and _is_constant_value(node.right)
    if isinstance(node, ast.Tuple):
        return all(_is_constant_part(e) for e in node.elts)
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in {"tuple", "frozenset"}
        and not node.keywords
    ):
        return all(
            _is_constant_part(a)
            or (isinstance(a, ast.Tuple | ast.Set) and all(_is_constant_part(e) for e in a.elts))
            for a in node.args
        )
    return False


def _is_constant_part(node: ast.expr) -> bool:
    """An element a constant display may hold: a literal, a name, a ``type(...)`` call or a value."""
    if isinstance(node, ast.Constant | ast.Name):
        return True
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "type":
        return True
    return _is_constant_value(node)


def module_scope(tree: ast.Module) -> list[ast.stmt]:
    """Every statement that runs at module scope, inside compound statements included.

    The one walker both module-scope scans use — this file's module-state population and
    ``test_runtime_holder``'s check that no moved name is bound at module level. A scan of
    ``tree.body`` alone misses a binding under ``if``, ``try`` (its handlers, ``else`` and
    ``finally`` too), ``try``/``except*``, ``with``, ``for``, ``while`` or a ``match`` case, all
    of which bind at module scope: measured, ``try:\n    _closing_now = _state._closing_now`` at
    module level survived the whole suite. Function and class bodies are not module scope and
    are not entered.
    """
    found: list[ast.stmt] = []
    pending: list[ast.stmt] = list(tree.body)
    while pending:
        statement = pending.pop(0)
        found.append(statement)
        if isinstance(statement, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            continue
        children: list[ast.stmt] = []
        if isinstance(statement, ast.If | ast.While | ast.For | ast.AsyncFor):
            children = statement.body + statement.orelse
        elif isinstance(statement, ast.With | ast.AsyncWith):
            children = statement.body
        elif isinstance(statement, ast.Try | ast.TryStar):
            children = statement.body + statement.orelse + statement.finalbody
            for handler in statement.handlers:
                children += handler.body
        elif isinstance(statement, ast.Match):
            for case in statement.cases:
                children += case.body
        pending[:0] = children
    return found


def module_bindings(tree: ast.Module) -> list[tuple[str, ast.expr | None]]:
    """Every name bound at module scope with the value it is bound to (``None`` if not one value).

    Assignments of every form, ``for`` targets, ``with ... as`` targets, ``except ... as`` names
    and ``match`` captures; imports, functions and classes are left to their own rules.
    """
    bound: list[tuple[str, ast.expr | None]] = []

    def names(target: ast.AST) -> list[str]:
        if isinstance(target, ast.Name):
            return [target.id]
        if isinstance(target, ast.Tuple | ast.List):
            return [name for element in target.elts for name in names(element)]
        if isinstance(target, ast.Starred):
            return names(target.value)
        return []

    for statement in module_scope(tree):
        if isinstance(statement, ast.Assign):
            for target in statement.targets:
                value = statement.value if isinstance(target, ast.Name) else None
                bound += [(name, value) for name in names(target)]
        elif isinstance(statement, ast.AnnAssign):
            bound += [(name, statement.value) for name in names(statement.target)]
        elif isinstance(statement, ast.AugAssign | ast.For | ast.AsyncFor):
            bound += [(name, None) for name in names(statement.target)]
        elif isinstance(statement, ast.With | ast.AsyncWith):
            for item in statement.items:
                if item.optional_vars is not None:
                    bound += [(name, None) for name in names(item.optional_vars)]
        elif isinstance(statement, ast.Try | ast.TryStar):
            bound += [(h.name, None) for h in statement.handlers if h.name]
        elif isinstance(statement, ast.Match):
            for case in statement.cases:
                bound += [
                    (n.name, None)
                    for n in ast.walk(case.pattern)
                    if isinstance(n, ast.MatchAs | ast.MatchStar) and n.name
                ]
    return bound


def _module_state(index: _Index, pops: Populations) -> None:
    """Every module-level binding in ``_lifecycle.py`` the constant classifier calls state."""
    tree = index.trees.get(MODULE_STATE_FILE)
    if tree is None:
        return
    module = MODULE_STATE_FILE.removesuffix(".py")
    globals_named = {n for g in ast.walk(tree) if isinstance(g, ast.Global) for n in g.names}
    for name, value in module_bindings(tree):
        constant = (
            name.lstrip("_").isupper()
            and name not in globals_named
            and value is not None
            and _is_constant_value(value)
        )
        if not constant:
            key = ("module", MODULE_STATE_FILE, name)
            pops.homes[key] = module
            pops.assigned.setdefault(key, []).append(_Assigned(value, _Ctx(module, None)))


def derive(sources: dict[str, str]) -> tuple[_Index, Populations]:
    """Parses the sources and derives the three populations."""
    index = _Index(sources)
    pops = Populations()
    _constructions(index, pops)
    _fields(index, pops)
    _module_state(index, pops)
    return index, pops


# -- the checks: one function each, each returning its own reason ---------------------------


def check_undeclared_constructions(
    index: _Index, pops: Populations, table: dict[Key, Row]
) -> list[Finding]:
    """A construction with no row."""
    return [
        Finding(UNDECLARED_CONSTRUCTION, k)
        for k in pops.homes
        if k[0] == "construction" and k not in table
    ]


def check_undeclared_fields(
    index: _Index, pops: Populations, table: dict[Key, Row]
) -> list[Finding]:
    """A field with no row, wherever in the class it is first assigned."""
    return [Finding(UNDECLARED_FIELD, k) for k in pops.homes if k[0] == "field" and k not in table]


def check_undeclared_module_state(
    index: _Index, pops: Populations, table: dict[Key, Row]
) -> list[Finding]:
    """A module-level binding the classifier calls state, with no row."""
    return [
        Finding(UNDECLARED_MODULE_STATE, k)
        for k in pops.homes
        if k[0] == "module" and k not in table
    ]


def check_stale(index: _Index, pops: Populations, table: dict[Key, Row]) -> list[Finding]:
    """A row that matches no member of any population."""
    return [Finding(STALE_ENTRY, k) for k in table if k not in pops.homes]


def check_home(index: _Index, pops: Populations, table: dict[Key, Row]) -> list[Finding]:
    """A row whose home is not the one the source gives the entry."""
    return [
        Finding(WRONG_HOME, k)
        for k, row in table.items()
        if k in pops.homes and row.home != pops.homes[k]
    ]


def check_child_values(index: _Index, pops: Populations, table: dict[Key, Row]) -> list[Finding]:
    """A row with no child value, or one outside walk, ``reset: <fn>``, inherited and none."""
    return [
        Finding(BAD_CHILD, k)
        for k, row in table.items()
        if not row.child or not all(CHILD_VALUE.match(v) for v in row.child)
    ]


def _attr_of(key: Key) -> tuple[str | None, str]:
    """The owner and the attribute name a write must name, for any kind of key."""
    if key[0] == "field":
        return key[1], key[2]
    if key[0] == "module":
        return None, key[2]
    target = key[3]
    return None, target.rsplit(".", 1)[-1]


def _holds_primitive(index: _Index, pops: Populations, key: Key, hops: int = 1) -> bool:
    """Whether every write of an entry stores a lock or an event, following one returned field."""
    if key[0] == "construction":
        return pops.constructor.get(key) in WALKABLE
    writes = pops.assigned.get(key, [])
    if not writes:
        return False
    for write in writes:
        value = write.value
        if value is not None and id(value) in pops.walkable_values:
            continue
        if hops and isinstance(value, ast.Call):
            returned = [
                ret.value
                for callee in index.resolve(value, write.ctx)
                for ret in ast.walk(index.functions[callee])
                if isinstance(ret, ast.Return) and ret.value is not None
            ]
            callee_cls = next(iter({c[0] for c in index.resolve(value, write.ctx)}), None)
            if returned and all(
                isinstance(r, ast.Attribute)
                and ast.unparse(r.value) == "self"
                and _holds_primitive(index, pops, ("field", str(callee_cls), r.attr), hops - 1)
                for r in returned
            ):
                continue
        return False
    return True


def check_walk(index: _Index, pops: Populations, table: dict[Key, Row]) -> list[Finding]:
    """A ``walk`` row whose entry is not a lock or an event."""
    return [
        Finding(WALK_NOT_PRIMITIVE, k)
        for k, row in table.items()
        if "walk" in row.child and k in pops.homes and not _holds_primitive(index, pops, k)
    ]


def _handler_key(index: _Index, value: str) -> Key | None:
    """The function a ``reset: <module or class>.<name>`` value names, if it exists."""
    owner, _, name = value.removeprefix("reset: ").partition(".")
    key = (owner, name)
    return key if key in index.functions else None


def _allowed_handlers(index: _Index) -> set[Key]:
    """Registered handlers, everything they reach, and the repair itself."""
    allowed: set[Key] = set()
    for handler in index.registered():
        if handler in index.functions:
            allowed |= index.closure(handler)
    if REPAIR_ITSELF in index.functions:
        allowed.add(REPAIR_ITSELF)
    return allowed


def check_reset_registered(
    index: _Index, pops: Populations, table: dict[Key, Row]
) -> list[Finding]:
    """A ``reset`` row naming a function no child handler is or reaches."""
    allowed = _allowed_handlers(index)
    return [
        Finding(RESET_NOT_REGISTERED, k)
        for k, row in table.items()
        for value in row.child
        if value.startswith("reset: ") and _handler_key(index, value) not in allowed
    ]


def _writes(index: _Index, function: Key, owner: str | None, attr: str) -> bool:
    """Whether a function assigns the entry, or calls a method on it, on a matching receiver."""
    ctx = index.ctx_of[function]

    def matches(target: ast.expr) -> bool:
        if isinstance(target, ast.Name):
            return owner is None and target.id == attr
        if not isinstance(target, ast.Attribute) or target.attr != attr:
            return False
        receiver = index.receiver(target.value, ctx)
        return owner is None or receiver is None or receiver == owner

    for node in ast.walk(index.functions[function]):
        targets: list[ast.expr] = []
        if isinstance(node, ast.Assign):
            targets = list(node.targets)
        elif isinstance(node, ast.AnnAssign | ast.AugAssign):
            targets = [node.target]
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            targets = [node.func.value]
        for target in targets:
            base = target.value if isinstance(target, ast.Subscript) else target
            if matches(base):
                return True
    return False


def check_reset_writes(index: _Index, pops: Populations, table: dict[Key, Row]) -> list[Finding]:
    """A ``reset`` row whose function, and everything it reaches, never writes the entry."""
    findings: list[Finding] = []
    for k, row in table.items():
        for value in row.child:
            handler = _handler_key(index, value) if value.startswith("reset: ") else None
            if handler is None:
                continue
            owner, attr = _attr_of(k)
            if not any(_writes(index, fn, owner, attr) for fn in index.closure(handler)):
                findings.append(Finding(RESET_DOES_NOT_WRITE, k))
    return findings


def check_token_sets(index: _Index, pops: Populations, table: dict[Key, Row]) -> list[Finding]:
    """The two token sets are emptied by the handler that empties the close registry."""
    return [
        Finding(TOKEN_SETS_ROW, key)
        for name in TOKEN_SETS
        if (key := ("field", STATE_CLASS, name)) in pops.homes
        and key in table
        and table[key].child != TOKEN_SETS_CHILD
    ]


CHECKS = (
    check_undeclared_constructions,
    check_undeclared_fields,
    check_undeclared_module_state,
    check_stale,
    check_home,
    check_child_values,
    check_walk,
    check_reset_registered,
    check_reset_writes,
    check_token_sets,
)


def audit(sources: dict[str, str], table: dict[Key, Row]) -> list[Finding]:
    """Runs every check over the sources against the table."""
    index, pops = derive(sources)
    return [finding for check in CHECKS for finding in check(index, pops, table)]


# -- the inventory, in code: the spec's Data Model ---------------------------------------------

_W = ("walk",)
_INH = ("inherited",)
_NONE = ("none",)
_REBUILD = "reset: _lifecycle._rebuild_worker_after_fork"


def _c(file: str, qual: str, target: str, ctor: str) -> Key:
    return ("construction", file, qual, target, ctor)


def _rt(name: str) -> Key:
    return ("field", "Runtime", name)


def _wk(name: str) -> Key:
    return ("field", "Worker", name)


INVENTORY: dict[Key, Row] = {
    _c("_lifecycle.py", "Runtime.__init__", "self._lock", "Lock"): Row(
        "Runtime", _W, "the state lock, held across Worker(_ensure_sink())"
    ),
    _c("_lifecycle.py", "Runtime.__init__", "self._stop", "Event"): Row(
        "Runtime", _W, "the one stop signal, replaced and never cleared"
    ),
    _c("_lifecycle.py", "Runtime.refresh_stop_signal", "self._stop", "Event"): Row(
        "Runtime", _W, "the replacement for a set stop signal, for a later deliverer"
    ),
    _c("_lifecycle.py", "Runtime.__init__", "self._closers_lock", "Lock"): Row(
        "Runtime", _W, "guards the closer roster; moved off the module in SPEC-077"
    ),
    _c("_lifecycle.py", "Runtime.__init__", "self._closing_now_lock", "Lock"): Row(
        "Runtime", _W, "guards the in-flight close registry, a leaf; moved in SPEC-077"
    ),
    _c("_lifecycle.py", "Runtime.__init__", "self._owned_lock", "Lock"): Row(
        "Runtime", _W, "guards the ownership record, last in the order; moved in SPEC-077"
    ),
    _c("_lifecycle.py", "_Closing.__init__", "self.event", "Event"): Row(
        "_Closing", _W, "a per-close event owned by its registry entry"
    ),
    _c("worker.py", "_FlushMarker.__init__", "self.event", "Event"): Row(
        "_FlushMarker", _NONE, "one per flush() call; it lives in the queue a child replaces"
    ),
    _c("worker.py", "Worker.__init__", "self._drain_finished", "Event"): Row(
        "Worker", _W, "the drain loop's own event, read by draining and flush"
    ),
    _c("worker.py", "Worker.__init__", "self._drain_settled", "Event"): Row(
        "Worker", _W, "the drain loop's second event, ordered against the first"
    ),
    _c("worker.py", "Worker.__init__", "self._lock", "Lock"): Row(
        "Worker", _W, "guards the worker's counters, markers and _stopped"
    ),
    _c("config.py", "<module>", "_config_lock", "Lock"): Row(
        "config", _W, "no lifecycle state; guards the config rebinding"
    ),
    _c("console.py", "ConsoleWriter.__init__", "self._lock", "Lock"): Row(
        "ConsoleWriter", _W, "no lifecycle state; serialises console writes"
    ),
    _c("decorator.py", "<module>", "_sweep_lock", "Lock"): Row(
        "decorator", _W, "no lifecycle state; guards the open-span sweep"
    ),
    _c("decorator.py", "<module>", "_loss_lock", "Lock"): Row(
        "decorator", _W, "no lifecycle state; deliberately not the lifecycle lock"
    ),
    _c("_fork.py", "<module>", "_RLOCK_TYPE", "RLock"): Row(
        "none", _NONE, "a type probe at import, fed to type(); stores no primitive"
    ),
    _c("_fork.py", "<module>", "_LOCK_TYPES", "Lock"): Row(
        "none", _NONE, "a type probe at import, fed to type(); stores no primitive"
    ),
    _c("_fork.py", "_fresh_primitive", "fresh", "Event"): Row(
        "none", _NONE, "the repair's replacement factory, handed back and never kept"
    ),
    _c("_fork.py", "_fresh_primitive", "fresh", "RLock"): Row(
        "none", _NONE, "the repair's replacement factory, handed back and never kept"
    ),
    _c("_fork.py", "_fresh_primitive", "fresh", "Lock"): Row(
        "none", _NONE, "the repair's replacement factory, handed back and never kept"
    ),
    _rt("_lock"): Row("Runtime", _W, "the state lock"),
    _rt("_worker"): Row("Runtime", _INH, "the process worker; guarded by _lock, read unlocked"),
    _rt("_late_worker"): Row("Runtime", _INH, "a worker built inside a no-worker shutdown"),
    _rt("_owed"): Row("Runtime", _INH, "the one owed-close record; opted out of the walk"),
    _rt("retirements"): Row("Runtime", _INH, "the retirement count"),
    _rt("_atexit_registered"): Row("Runtime", _INH, "the exit handler is registered once"),
    _rt("_shutdown_running"): Row(
        "Runtime",
        _INH,
        "no-worker shutdown depth; a child inheriting it is a known defect left to SPEC-079",
    ),
    _rt("_stop"): Row(
        "Runtime",
        ("walk", _REBUILD),
        "walked, then replaced through refresh_stop_signal when a resumed child's worker asks",
    ),
    _rt("_closers"): Row("Runtime", _INH, "closer threads; a child's read dead and are pruned"),
    _rt("_closers_lock"): Row("Runtime", _W, "moved from the module in SPEC-077"),
    _rt("_closing_now"): Row(
        "Runtime", ("reset: _lifecycle._clear_after_fork",), "in-flight closes; moved in SPEC-077"
    ),
    _rt("_closing_now_lock"): Row("Runtime", _W, "moved from the module in SPEC-077"),
    _rt("_owned"): Row(
        "Runtime", ("reset: _lifecycle._mark_inherited",), "the ownership record; SPEC-077 moved it"
    ),
    _rt("_owned_lock"): Row("Runtime", _W, "moved from the module in SPEC-077"),
    _rt("_marking_failed"): Row(
        "Runtime",
        ("reset: _lifecycle._mark_inherited",),
        "set by the child's marking walk when it cannot finish; moved in SPEC-077",
    ),
    _rt("_swaps_running"): Row("Runtime", TOKEN_SETS_CHILD, "swap tokens, observer-only"),
    _rt("_stops_running"): Row("Runtime", TOKEN_SETS_CHILD, "stop tokens, observer-only"),
    _wk("sink"): Row("Worker", _INH, "the delivering sink; no Runtime.sink is added"),
    _wk("batch_size"): Row("Worker", _INH, "a tunable, set once"),
    _wk("flush_interval"): Row("Worker", _INH, "a tunable, set once"),
    _wk("max_retries"): Row("Worker", _INH, "a tunable, set once"),
    _wk("_max_queue"): Row("Worker", _INH, "a tunable, set once"),
    _wk("dropped"): Row("Worker", (_REBUILD,), "a Health counter, not lifecycle state"),
    _wk("failed_batches"): Row("Worker", (_REBUILD,), "a Health counter, not lifecycle state"),
    _wk("stopped_reason"): Row("Worker", (_REBUILD,), "a Health figure, not lifecycle state"),
    _wk("submitted_after_shutdown"): Row("Worker", (_REBUILD,), "a Health counter"),
    _wk("incomplete_swaps"): Row("Worker", (_REBUILD,), "a Health counter"),
    _wk("_queue"): Row("Worker", (_REBUILD,), "the drain's buffer, replaced as an object"),
    _wk("_stop"): Row(
        "Worker",
        ("walk", _REBUILD),
        "a reference to Runtime._stop taken at the build, refreshed when the child resumes",
    ),
    _wk("_epoch"): Row("Worker", _INH, "the retirement count at this worker's build"),
    _wk("_drain_finished"): Row("Worker", ("walk", _REBUILD), "the drain loop's own event"),
    _wk("_drain_settled"): Row("Worker", ("walk", _REBUILD), "the drain loop's own event"),
    _wk("_stopped"): Row("Worker", (_REBUILD,), "the once-only stop gate of one worker"),
    _wk("_exit_deadline"): Row("Worker", (_REBUILD,), "the drain's deadline (SPEC-062)"),
    _wk("_join_owed"): Row("Worker", (_REBUILD,), "the join a stop on the drain thread owes"),
    _wk("_join_taken_over"): Row("Worker", (_REBUILD,), "whether a later caller took it over"),
    _wk("_in_flight"): Row("Worker", (_REBUILD,), "the batch the drain thread is inside"),
    _wk("_taken_markers"): Row("Worker", (_REBUILD,), "flush() callers owed; in _FORK_SKIP"),
    _wk("_held"): Row("Worker", (_REBUILD,), "sinks the drain thread may be inside"),
    _wk("_lock"): Row("Worker", _W, "guards the worker's counters"),
    _wk("_thread"): Row("Worker", (_REBUILD,), "the drain thread"),
    ("module", "_lifecycle.py", "_state"): Row(
        "_lifecycle", _INH, "the one Runtime; walked as a holder, its fields repaired one by one"
    ),
}


def _live_sources() -> dict[str, str]:
    """The six files, as they are."""
    return {name: (_SRC / name).read_text(encoding="utf-8") for name in FILES}


def test_every_lifecycle_entry_has_a_home() -> None:
    """FR-003 AC-1 and AC-5 (invariants 6 and 12): the live sources against the inventory."""
    findings = audit(_live_sources(), INVENTORY)
    assert not findings, "\n".join(f"{f.reason}: {f.key}" for f in findings)


def test_the_construction_population_meets_its_floors() -> None:
    """FR-003 AC-1: at least eleven constructions in the pair and nine outside it."""
    _, pops = derive(_live_sources())
    in_pair = [k for k in pops.homes if k[0] == "construction" and k[1] in PAIR]
    outside = [k for k in pops.homes if k[0] == "construction" and k[1] not in PAIR]
    assert len(in_pair) >= PAIR_FLOOR, in_pair
    assert len(outside) >= OUTSIDE_FLOOR, outside


def test_the_module_state_population_is_the_one_runtime() -> None:
    """FR-003: after the moves, the classifier finds ``_state`` alone among the module's state."""
    _, pops = derive(_live_sources())
    assert [k for k in pops.homes if k[0] == "module"] == [("module", "_lifecycle.py", "_state")]


# -- the corpus: every check refuses its shape with its own reason, and is silent otherwise -----

_THREADING = "import threading\nfrom log_foundry import _fork\n"
_RUNTIME = (
    _THREADING
    + "class Runtime:\n"
    + "    def __init__(self):\n"
    + "        self._x = 0\n"
    + "_state = Runtime()\n"
)
_BASE_TABLE: dict[Key, Row] = {
    _rt("_x"): Row("Runtime", _INH, "fixture field"),
    ("module", "_lifecycle.py", "_state"): Row("_lifecycle", _INH, "fixture holder"),
}


def _with(**rows: Row) -> dict[Key, Row]:
    """The base table plus Runtime field rows named by keyword."""
    return {**_BASE_TABLE, **{_rt(name): row for name, row in rows.items()}}


_REGISTERED = (
    _THREADING
    + "class Runtime:\n"
    + "    def __init__(self):\n"
    + "        self._x = 0\n"
    + "        self._flag = False\n"
    + "_state = Runtime()\n"
    + "def _handler():\n"
    + "    _state._flag = False\n"
    + "def _other():\n"
    + "    _state._flag = False\n"
    + "_fork.register_child_handler(_handler)\n"
)

_ONE_HOP = (
    _THREADING
    + "class Runtime:\n"
    + "    def __init__(self):\n"
    + "        self._stop = threading.Event()\n"
    + "    def refresh(self):\n"
    + "        if self._stop.is_set():\n"
    + "            self._stop = threading.Event()\n"
    + "        return self._stop\n"
    + "_state = Runtime()\n"
)
_ONE_HOP_WORKER = (
    "from log_foundry import _lifecycle\n"
    "class Worker:\n"
    "    def __init__(self):\n"
    "        self._stop = _lifecycle._state.refresh()\n"
)
_ONE_HOP_TABLE: dict[Key, Row] = {
    _c("_lifecycle.py", "Runtime.__init__", "self._stop", "Event"): Row("Runtime", _W, "f"),
    _c("_lifecycle.py", "Runtime.refresh", "self._stop", "Event"): Row("Runtime", _W, "f"),
    _rt("_stop"): Row("Runtime", _W, "f"),
    _wk("_stop"): Row("Worker", _W, "one hop: handed the runtime's event"),
    ("module", "_lifecycle.py", "_state"): Row("_lifecycle", _INH, "f"),
}

_TWO_HOP = (
    _THREADING
    + "class Runtime:\n"
    + "    def __init__(self):\n"
    + "        self._stop = threading.Event()\n"
    + "        self._worker = None\n"
    + "    def worker_exists(self) -> Worker | None:\n"
    + "        return self._worker\n"
    + "    def refresh(self):\n"
    + "        self._stop = threading.Event()\n"
    + "        return self._stop\n"
    + "_state = Runtime()\n"
    + "def _rebuild():\n"
    + "    worker = _state.worker_exists()\n"
    + "    worker._reinit()\n"
    + "_fork.register_child_handler(_rebuild)\n"
)
_TWO_HOP_WORKER = (
    "from log_foundry import _lifecycle\n"
    "class Worker:\n"
    "    def __init__(self):\n"
    "        self._n = 0\n"
    "    def _reinit(self):\n"
    "        self._n = 0\n"
    "        _lifecycle._state.refresh()\n"
)
_TWO_HOP_TABLE: dict[Key, Row] = {
    _c("_lifecycle.py", "Runtime.__init__", "self._stop", "Event"): Row("Runtime", _W, "f"),
    _c("_lifecycle.py", "Runtime.refresh", "self._stop", "Event"): Row("Runtime", _W, "f"),
    _rt("_stop"): Row("Runtime", ("walk", "reset: _lifecycle._rebuild"), "two hops"),
    _rt("_worker"): Row("Runtime", _INH, "f"),
    _wk("_n"): Row("Worker", ("reset: _lifecycle._rebuild",), "one hop, through a typed local"),
    ("module", "_lifecycle.py", "_state"): Row("_lifecycle", _INH, "f"),
}

REFUSED: list[tuple[str, dict[str, str], dict[Key, Row], str]] = [
    (
        "an-undeclared-lock",
        {"_lifecycle.py": _RUNTIME + "_extra_lock = threading.Lock()\n"},
        {**_BASE_TABLE, ("module", "_lifecycle.py", "_extra_lock"): Row("_lifecycle", _W, "f")},
        UNDECLARED_CONSTRUCTION,
    ),
    (
        "an-undeclared-runtime-field",
        {"_lifecycle.py": _RUNTIME.replace("self._x = 0\n", "self._x = 0\n        self._y = 1\n")},
        _BASE_TABLE,
        UNDECLARED_FIELD,
    ),
    (
        "an-undeclared-worker-field-first-assigned-outside-init",
        {
            "_lifecycle.py": _RUNTIME,
            "worker.py": "class Worker:\n    def __init__(self):\n        self.a = 1\n"
            "    def later(self):\n        self._late = 2\n",
        },
        {**_BASE_TABLE, _wk("a"): Row("Worker", _INH, "f")},
        UNDECLARED_FIELD,
    ),
    (
        "an-undeclared-runtime-field-written-through-an-alias",
        {
            "_lifecycle.py": _RUNTIME,
            "worker.py": "from log_foundry import _lifecycle\n_rt = _lifecycle._state\n"
            "def f():\n    _rt._aliased = 1\n",
        },
        _BASE_TABLE,
        UNDECLARED_FIELD,
    ),
    (
        "an-undeclared-module-binding",
        {"_lifecycle.py": _RUNTIME + "_cache = {}\n"},
        _BASE_TABLE,
        UNDECLARED_MODULE_STATE,
    ),
    (
        "an-upper-case-name-a-function-rebinds",
        {"_lifecycle.py": _RUNTIME + "_LIMIT = 3\ndef f():\n    global _LIMIT\n    _LIMIT = 4\n"},
        _BASE_TABLE,
        UNDECLARED_MODULE_STATE,
    ),
    (
        "an-upper-case-name-holding-a-mutable-display",
        {"_lifecycle.py": _RUNTIME + "_TABLE = {}\n"},
        _BASE_TABLE,
        UNDECLARED_MODULE_STATE,
    ),
    (
        "a-stale-entry",
        {"_lifecycle.py": _RUNTIME},
        _with(_gone=Row("Runtime", _INH, "a field a rename removed")),
        STALE_ENTRY,
    ),
    (
        "a-field-declared-with-the-wrong-home",
        {"_lifecycle.py": _RUNTIME},
        {**_BASE_TABLE, _rt("_x"): Row("Worker", _INH, "declared on the wrong class")},
        WRONG_HOME,
    ),
    (
        "a-row-with-no-child-value",
        {"_lifecycle.py": _RUNTIME},
        {**_BASE_TABLE, _rt("_x"): Row("Runtime", (), "f")},
        BAD_CHILD,
    ),
    (
        "a-row-with-an-unknown-child-value",
        {"_lifecycle.py": _RUNTIME},
        {**_BASE_TABLE, _rt("_x"): Row("Runtime", ("repaired",), "f")},
        BAD_CHILD,
    ),
    (
        "a-walk-row-on-a-counter",
        {"_lifecycle.py": _RUNTIME},
        {**_BASE_TABLE, _rt("_x"): Row("Runtime", _W, "an int is not walked")},
        WALK_NOT_PRIMITIVE,
    ),
    (
        "a-walk-row-one-hop-from-a-counter",
        {
            "_lifecycle.py": _RUNTIME.replace(
                "        self._x = 0\n",
                "        self._x = 0\n    def get(self):\n        return self._x\n",
            ),
            "worker.py": "from log_foundry import _lifecycle\nclass Worker:\n"
            "    def __init__(self):\n        self._s = _lifecycle._state.get()\n",
        },
        {**_BASE_TABLE, _wk("_s"): Row("Worker", _W, "handed an int, not an event")},
        WALK_NOT_PRIMITIVE,
    ),
    (
        "a-reset-row-naming-an-unregistered-function",
        {"_lifecycle.py": _REGISTERED},
        _with(_flag=Row("Runtime", ("reset: _lifecycle._other",), "_other is never registered")),
        RESET_NOT_REGISTERED,
    ),
    (
        "a-reset-row-whose-handler-does-not-write-it",
        {"_lifecycle.py": _REGISTERED},
        {
            **_BASE_TABLE,
            _rt("_flag"): Row("Runtime", _INH, "f"),
            _rt("_x"): Row("Runtime", ("reset: _lifecycle._handler",), "_handler writes _flag"),
        },
        RESET_DOES_NOT_WRITE,
    ),
    (
        "a-reset-row-whose-handler-writes-the-name-on-another-class",
        {
            "_lifecycle.py": _TWO_HOP.replace(
                "        self._stop = threading.Event()\n        return self._stop\n",
                "        return self._stop\n",
            ),
            "worker.py": _TWO_HOP_WORKER.replace(
                "        self._n = 0\n        _lifecycle._state.refresh()\n",
                "        self._n = 0\n        self._stop = 0\n",
            ),
        },
        {
            **{k: v for k, v in _TWO_HOP_TABLE.items() if k[2:3] != ("Runtime.refresh",)},
            _wk("_stop"): Row("Worker", _INH, "the worker's own field of the same name"),
        },
        RESET_DOES_NOT_WRITE,
    ),
    (
        "a-token-set-row-that-is-not-the-registry-handlers",
        {
            "_lifecycle.py": _RUNTIME.replace(
                "self._x = 0\n", "self._x = 0\n        self._swaps_running = set()\n"
            )
        },
        _with(_swaps_running=Row("Runtime", _INH, "inherited by a child")),
        TOKEN_SETS_ROW,
    ),
]

_NESTED_BINDINGS = {
    "if": "if True:\n    _cache = {}\n",
    "else": "if False:\n    pass\nelse:\n    _cache = {}\n",
    "try-body": "try:\n    _cache = {}\nexcept Exception:\n    pass\n",
    "try-handler": "try:\n    pass\nexcept Exception:\n    _cache = {}\n",
    "try-else": "try:\n    pass\nexcept Exception:\n    pass\nelse:\n    _cache = {}\n",
    "try-finally": "try:\n    pass\nfinally:\n    _cache = {}\n",
    "try-star": "try:\n    pass\nexcept* ValueError:\n    _cache = {}\n",
    "with": "import contextlib\nwith contextlib.nullcontext():\n    _cache = {}\n",
    "with-as": "import contextlib\nwith contextlib.nullcontext() as _cache:\n    pass\n",
    "for": "for _i in ():\n    pass\n_ = None\n",
    "for-body": "for _ in ():\n    _cache = {}\n",
    "while": "while False:\n    _cache = {}\n",
    "match": "match 1:\n    case 1:\n        _cache = {}\n",
    "except-as": "try:\n    pass\nexcept Exception as _cache:\n    pass\n",
}
"""A module-scope binding under each compound statement kind, which the walker must reach."""

REFUSED += [
    (
        f"a-module-binding-under-{kind}",
        {"_lifecycle.py": _RUNTIME + source},
        _BASE_TABLE,
        UNDECLARED_MODULE_STATE,
    )
    for kind, source in _NESTED_BINDINGS.items()
    if kind != "for"
]
REFUSED += [
    (
        "a-module-binding-as-a-for-target",
        {"_lifecycle.py": _RUNTIME + _NESTED_BINDINGS["for"]},
        {**_BASE_TABLE, ("module", "_lifecycle.py", "_"): Row("_lifecycle", _INH, "f")},
        UNDECLARED_MODULE_STATE,
    ),
    (
        "an-upper-case-name-holding-a-set-call",
        {"_lifecycle.py": _RUNTIME + "_PENDING = set()\n"},
        _BASE_TABLE,
        UNDECLARED_MODULE_STATE,
    ),
    (
        "an-upper-case-name-holding-a-set-display",
        {"_lifecycle.py": _RUNTIME + "_SEEN = {1}\n"},
        _BASE_TABLE,
        UNDECLARED_MODULE_STATE,
    ),
]

SILENT: list[tuple[str, dict[str, str], dict[Key, Row]]] = [
    ("a-declared-field", {"_lifecycle.py": _RUNTIME}, _BASE_TABLE),
    (
        "the-constant-shapes-and-an-import",
        {
            "_lifecycle.py": "import os\nfrom typing import TYPE_CHECKING\n"
            "if TYPE_CHECKING:\n    from x import Y\n"
            + _RUNTIME
            + "DEFAULT_TIMEOUT = 30.0\n_FOREIGN = -1\n_CEILING = 100_000\n_SCALED = 2 * 1024\n"
            + "_SKIP = ('_a', '_b')\n_PLAIN = frozenset({dict, list, type(None)})\n"
            + "_PAIR = tuple((1, 2))\n_FROZEN = frozenset((1, 2))\n",
        },
        _BASE_TABLE,
    ),
    (
        "a-compound-child-value",
        {"_lifecycle.py": _TWO_HOP, "worker.py": _TWO_HOP_WORKER},
        _TWO_HOP_TABLE,
    ),
    (
        "a-walk-one-hop-through-a-returned-event",
        {"_lifecycle.py": _ONE_HOP, "worker.py": _ONE_HOP_WORKER},
        _ONE_HOP_TABLE,
    ),
    (
        "a-reset-two-hops-from-its-handler",
        {"_lifecycle.py": _TWO_HOP, "worker.py": _TWO_HOP_WORKER},
        _TWO_HOP_TABLE,
    ),
]


@pytest.mark.parametrize(
    ("sources", "table", "reason"), [r[1:] for r in REFUSED], ids=[r[0] for r in REFUSED]
)
def test_each_check_refuses_its_shape(
    sources: dict[str, str], table: dict[Key, Row], reason: str
) -> None:
    """FR-003 AC-2: each fixture trips exactly one check, and the finding names its reason."""
    reasons = {f.reason for f in audit(sources, table)}
    assert reasons == {reason}, f"expected only {reason!r}, got {reasons}"


@pytest.mark.parametrize(("sources", "table"), [s[1:] for s in SILENT], ids=[s[0] for s in SILENT])
def test_these_shapes_are_silent(sources: dict[str, str], table: dict[Key, Row]) -> None:
    """FR-003 AC-2: a declared field, every constant shape, an import, and the fork chains pass."""
    findings = audit(sources, table)
    assert not findings, [(f.reason, f.key) for f in findings]


def test_every_check_has_a_refusing_fixture() -> None:
    """No check is left without a row whose reason only it produces."""
    reasons = {
        UNDECLARED_CONSTRUCTION,
        UNDECLARED_FIELD,
        UNDECLARED_MODULE_STATE,
        STALE_ENTRY,
        WRONG_HOME,
        BAD_CHILD,
        WALK_NOT_PRIMITIVE,
        RESET_NOT_REGISTERED,
        RESET_DOES_NOT_WRITE,
        TOKEN_SETS_ROW,
    }
    assert len(reasons) == len(CHECKS), "one reason per check"
    assert {r[3] for r in REFUSED} == reasons
