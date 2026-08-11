"""Every sink close in `src/` goes through one release path (SPEC-042 FR-002 AC-1).

The guard that decides whether this process may release a sink has to live somewhere every
closer reaches. A first draft of SPEC-042 guarded the three sites the lifecycle owns and it was
measurably insufficient: a forked child wrapping an **inherited** sink in a `MultiSink` of its
own closed the parent's sink twice, because the wrapper the child built is itself releasable and
forwards the close to a child that is not. So the release is one helper and every library closer
calls it -- and "every" is a claim a lint has to keep true, not a sentence in a docstring.

**The discriminator, stated here rather than delegated.** Resolve each `.close()` receiver to an
annotation -- through a local alias to a module global, through an attribute to its `__init__`
parameter, and through an iteration to the container's -- and the site is in scope iff that
annotation names a sink type. Three of the eight carry no annotation at the call site, and so do
several driver closes, which is why the call expression alone cannot decide it.

**"Names a sink type" is `Sink` or a class in `test_sink_concurrency`'s roster that defines or
inherits an `emit`**, and that last clause is a correction to the criterion as written. That
roster's trigger set is `emit` *or* `send_all` *or* `close`, because the lock and post-close
decisions it enforces apply to a transport as much as to a sink -- so it contains
`SocketTransport`, which has `send_all` and `close` and no `emit`. Without the clause this lint
claims `LogstashSink`'s and `SyslogSink`'s socket closes, taking the roster to ten; both are
transports the sink built and owns outright, which no sink-ownership record describes and which
a forked child must still be able to release. Requiring an `emit` is what the word "sink" means
here, and it is derived from the same scan rather than carved out by name.
"""

from __future__ import annotations

import ast
import pathlib
from typing import TYPE_CHECKING

import log_foundry
from test_sink_concurrency import _base_names, _sink_classes_with_an_emit

if TYPE_CHECKING:
    from collections.abc import Iterator

_SRC = pathlib.Path(log_foundry.__file__).parent

_EXPECTED_CLOSERS = {
    ("_lifecycle", "_close_guarded"),
    ("decorator", "_close_orphan_sink"),
    ("worker", "Worker._close_sink"),
    ("sinks/multi", "MultiSink.close"),
    ("sinks/filtering", "FilteringSink.close"),
    ("sinks/transform", "TransformSink.close"),
    ("sinks/logstash", "LogstashSink.close"),
    ("sinks/sentry", "SentrySink.close"),
}
"""The eight sites that close a sink, named so a ninth is a decision somebody takes.

The first is the *thread body* of a detached close rather than the two callers that start one:
`decorator._swap_sink` and `Worker._close_swapped_out` request a close and hand the thread on to
a bounded join, and `_close_guarded` is where it is actually performed. Those two are
`_EXPECTED_REQUESTERS` -- listed separately because a requester is not a place a guard has to
sit, and folding them in here would make the roster read as ten closers when SPEC-042 counted
eight.
"""

_EXPECTED_REQUESTERS = {
    ("decorator", "_swap_sink"),
    ("worker", "Worker._close_swapped_out"),
}
"""The two sites that ask for a detached close and join the thread it returns.

They are held to the same roster because the helper's return value is theirs: SPEC-030 FR-003
gave each a bounded wait, and a helper that stopped returning the thread would take it away
silently (FR-002 AC-8).
"""

_CLOSER_FLOOR = 8
"""What the roster may not silently fall below.

SPEC-038 measured what a missing floor costs: moving five `emit` methods into a base dropped
five classes out of two lints in one commit, 34 to 29, with the suite green, and only the roster
that carried a floor noticed.
"""


def _modules() -> Iterator[tuple[str, ast.Module]]:
    """Yields every module of the installed package, keyed by a path-shaped stem.

    Args:
      None.

    Returns:
      One `(stem, parsed module)` pair per source file, `sinks/multi` rather than `multi` so a
      name colliding across the package and its `sinks/` subpackage stays distinguishable.

    Raises:
      None.
    """
    for path in sorted(_SRC.rglob("*.py")):
        stem = str(path.relative_to(_SRC).with_suffix(""))
        yield stem, ast.parse(path.read_text(encoding="utf-8"))


def _sink_type_names() -> set[str]:
    """Returns every type name that makes a `.close()` receiver a sink.

    A roster class qualifies only if it defines or inherits an `emit`. See the module docstring:
    the roster this reuses also admits `send_all`/`close`-only transports, and treating one of
    those as a sink would put two socket closes in a roster of eight.

    Args:
      None.

    Returns:
      `Sink` plus the qualifying roster class names.

    Raises:
      None.
    """
    classes = {name: node for _, node in _sink_classes_with_an_emit() for name in [node.name]}

    def _emits(node: ast.ClassDef, seen: set[str]) -> bool:
        if node.name in seen:
            return False
        seen.add(node.name)
        for child in ast.walk(node):
            if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef) and child.name == "emit":
                return True
        return any(base in classes and _emits(classes[base], seen) for base in _base_names(node))

    return {"Sink"} | {name for name, node in classes.items() if _emits(node, set())}


def _annotation_names(node: ast.expr | None) -> set[str]:
    """Returns every type name mentioned anywhere in one annotation.

    Flattening rather than parsing the shape is deliberate: `Sink`, `Sink | None`,
    `list[Sink]` and `tuple[Sink, ...]` must all answer "sink", and the iteration case reaches
    the element type for free because a container's annotation names it.

    Args:
      node: An annotation expression, or `None` for an unannotated target.

    Returns:
      The names, which may be empty.

    Raises:
      None.
    """
    if node is None:
        return set()
    if isinstance(node, ast.Name):
        return {node.id}
    if isinstance(node, ast.Attribute):
        return {node.attr}
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        try:
            return _annotation_names(ast.parse(node.value, mode="eval").body)
        except SyntaxError:
            return set()
    if isinstance(node, ast.BinOp):
        return _annotation_names(node.left) | _annotation_names(node.right)
    if isinstance(node, ast.Subscript):
        return _annotation_names(node.value) | _annotation_names(node.slice)
    if isinstance(node, ast.Tuple):
        return set().union(*(_annotation_names(item) for item in node.elts)) if node.elts else set()
    return set()


def _parameter_annotation(fn: ast.FunctionDef | ast.AsyncFunctionDef, name: str) -> set[str]:
    """Returns the annotation names of one parameter of one function.

    The `vararg` is included, which is what resolves `MultiSink`: its children arrive as
    `*sinks: Sink`, so the annotation names the element type directly.

    Args:
      fn: The function to look in.
      name: The parameter name.

    Returns:
      The names, empty when there is no such parameter or it is unannotated.

    Raises:
      None.
    """
    args = fn.args
    candidates = [*args.posonlyargs, *args.args, *args.kwonlyargs]
    candidates.extend(arg for arg in (args.vararg, args.kwarg) if arg is not None)
    for arg in candidates:
        if arg.arg == name:
            return _annotation_names(arg.annotation)
    return set()


def _self_attribute_names(cls: ast.ClassDef, attr: str) -> set[str]:
    """Resolves `self.<attr>` to an annotation, through the class body or `__init__`.

    Two routes, both used by the eight: an annotated assignment anywhere in the class
    (`self._http: HTTPSink | None = ...`), and a bare `self._inner = inner` whose right-hand
    side is a parameter of the enclosing method.

    Args:
      cls: The class the attribute belongs to.
      attr: The attribute name.

    Returns:
      The names, empty when neither route resolves.

    Raises:
      None.
    """
    names: set[str] = set()
    for fn in ast.walk(cls):
        if not isinstance(fn, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        for node in ast.walk(fn):
            if isinstance(node, ast.AnnAssign) and _is_self_attr(node.target, attr):
                names |= _annotation_names(node.annotation)
            elif (
                isinstance(node, ast.Assign)
                and isinstance(node.value, ast.Name)
                and any(_is_self_attr(target, attr) for target in node.targets)
            ):
                names |= _parameter_annotation(fn, node.value.id)
    for node in cls.body:
        if (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == attr
        ):
            names |= _annotation_names(node.annotation)
    return names


def _is_self_attr(node: ast.expr, attr: str) -> bool:
    """Whether one assignment target is `self.<attr>`.

    Args:
      node: The target expression.
      attr: The attribute name to match.

    Returns:
      Whether it matches.

    Raises:
      None.
    """
    return (
        isinstance(node, ast.Attribute)
        and node.attr == attr
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    )


def _module_globals(tree: ast.Module) -> dict[str, set[str]]:
    """Returns the annotation names of every module-level annotated global.

    This is the "local alias to a module global" route: `decorator._close_orphan_sink` reads
    `owed = _orphan_sink`, and only the global carries `Sink | None`.

    Args:
      tree: The parsed module.

    Returns:
      One entry per annotated global.

    Raises:
      None.
    """
    found: dict[str, set[str]] = {}
    for node in tree.body:
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            found[node.target.id] = _annotation_names(node.annotation)
    return found


def _resolve(
    receiver: ast.expr,
    fn: ast.FunctionDef | ast.AsyncFunctionDef | None,
    cls: ast.ClassDef | None,
    globals_: dict[str, set[str]],
    depth: int = 0,
) -> set[str]:
    """Resolves a `.close()` receiver expression to the type names it may hold.

    Recursion is bounded because the routes chain: a local name may alias a module global, and a
    `for` target may iterate an attribute that resolves through `__init__`. Two hops is what the
    eight need and the bound keeps a self-referential assignment from looping.

    Args:
      receiver: The expression a `close()` was called on.
      fn: The enclosing function, or `None` at module level.
      cls: The enclosing class, or `None`.
      globals_: The module's annotated globals.
      depth: How many alias hops have already been followed.

    Returns:
      The type names, empty when nothing resolves.

    Raises:
      None.
    """
    if depth > 2:
        return set()
    if isinstance(receiver, ast.Attribute):
        if cls is not None and isinstance(receiver.value, ast.Name) and receiver.value.id == "self":
            return _self_attribute_names(cls, receiver.attr)
        return set()
    if not isinstance(receiver, ast.Name):
        return set()
    name = receiver.id
    names: set[str] = set()
    if fn is not None:
        names |= _parameter_annotation(fn, name)
        for node in ast.walk(fn):
            if (
                isinstance(node, ast.AnnAssign)
                and isinstance(node.target, ast.Name)
                and node.target.id == name
            ):
                names |= _annotation_names(node.annotation)
            elif isinstance(node, ast.Assign) and any(
                isinstance(target, ast.Name) and target.id == name for target in node.targets
            ):
                names |= _resolve(node.value, fn, cls, globals_, depth + 1)
            elif (
                isinstance(node, ast.For)
                and isinstance(node.target, ast.Name)
                and node.target.id == name
            ):
                names |= _resolve(node.iter, fn, cls, globals_, depth + 1)
    return names | globals_.get(name, set())


def _walk_scopes(
    tree: ast.Module,
) -> Iterator[tuple[ast.AST, ast.FunctionDef | ast.AsyncFunctionDef | None, ast.ClassDef | None]]:
    """Yields every node of a module alongside the function and class enclosing it.

    Args:
      tree: The parsed module.

    Returns:
      One `(node, function, class)` triple per node.

    Raises:
      None.
    """

    def _descend(
        node: ast.AST,
        fn: ast.FunctionDef | ast.AsyncFunctionDef | None,
        cls: ast.ClassDef | None,
    ) -> Iterator[
        tuple[ast.AST, ast.FunctionDef | ast.AsyncFunctionDef | None, ast.ClassDef | None]
    ]:
        yield node, fn, cls
        for child in ast.iter_child_nodes(node):
            next_fn = child if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef) else fn
            next_cls = child if isinstance(child, ast.ClassDef) else cls
            yield from _descend(child, next_fn, next_cls)

    yield from _descend(tree, None, None)


def _qualified(fn: ast.FunctionDef | ast.AsyncFunctionDef | None, cls: ast.ClassDef | None) -> str:
    """Names the enclosing scope the way `_EXPECTED_CLOSERS` spells it.

    Args:
      fn: The enclosing function, or `None`.
      cls: The enclosing class, or `None`.

    Returns:
      `Class.method`, `function`, or `<module>`.

    Raises:
      None.
    """
    if fn is None:
        return "<module>"
    return f"{cls.name}.{fn.name}" if cls is not None else fn.name


def _sink_close_sites() -> list[tuple[str, str, int, frozenset[str]]]:
    """Returns every `.close()` in `src/` whose receiver resolves to a sink type.

    Args:
      None.

    Returns:
      One `(module stem, scope, line, resolved names)` tuple per in-scope site.

    Raises:
      None.
    """
    sink_names = _sink_type_names()
    sites: list[tuple[str, str, int, frozenset[str]]] = []
    for stem, tree in _modules():
        globals_ = _module_globals(tree)
        for node, fn, cls in _walk_scopes(tree):
            if not isinstance(node, ast.Call):
                continue
            if not (isinstance(node.func, ast.Attribute) and node.func.attr == "close"):
                continue
            resolved = _resolve(node.func.value, fn, cls, globals_)
            if resolved & sink_names:
                sites.append((stem, _qualified(fn, cls), node.lineno, frozenset(resolved)))
    return sites


def _release_call_sites() -> set[tuple[str, str]]:
    """Returns every scope in `src/` that calls the release helper.

    Both spellings count: `_lifecycle.release(...)` from the seven callers outside the module,
    and the bare `release(...)` the detached thread body makes inside it.

    Args:
      None.

    Returns:
      One `(module stem, scope)` pair per calling scope.

    Raises:
      None.
    """
    callers: set[tuple[str, str]] = set()
    for stem, tree in _modules():
        for node, fn, cls in _walk_scopes(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            qualified = (
                isinstance(func, ast.Attribute)
                and func.attr == "release"
                and isinstance(func.value, ast.Name)
                and func.value.id == "_lifecycle"
            )
            bare = stem == "_lifecycle" and isinstance(func, ast.Name) and func.id == "release"
            if qualified or bare:
                callers.add((stem, _qualified(fn, cls)))
    return callers


def test_the_only_sink_close_in_src_is_the_release_helper() -> None:
    """One expression in the package calls `close()` on a sink, and it is the guarded one.

    This is the property the whole spec rests on: a site that closes directly is a site the
    ownership guard cannot reach, which is exactly how the wrapper route survived a fix that
    guarded all three lifecycle sites.
    """
    sites = _sink_close_sites()
    assert [(stem, scope) for stem, scope, _, _ in sites] == [("_lifecycle", "release")], sites


def test_every_library_closer_goes_through_the_release_helper() -> None:
    """The eight are the eight, and the two detached requesters are the two."""
    assert _release_call_sites() == _EXPECTED_CLOSERS | _EXPECTED_REQUESTERS


def test_the_detached_requesters_still_receive_a_thread_to_join() -> None:
    """Both bounded waits survive the move (FR-002 AC-8).

    A helper that stopped returning the closer thread would cost each of them the bounded join
    SPEC-030 FR-003 built, and it would do so silently -- the call still compiles, the close
    still happens, and only the wait disappears. So the assertion is on the returned value being
    bound, not merely on the call being made.
    """
    bound: set[tuple[str, str]] = set()
    for stem, tree in _modules():
        for node, fn, cls in _walk_scopes(tree):
            if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Call):
                continue
            func = node.value.func
            if not (isinstance(func, ast.Attribute) and func.attr == "release"):
                continue
            if any(kw.arg == "detached" for kw in node.value.keywords):
                bound.add((stem, _qualified(fn, cls)))
    assert bound == _EXPECTED_REQUESTERS


def test_the_closer_roster_has_not_collapsed() -> None:
    """A floor, because a roster that silently empties passes every other test here.

    SPEC-038 measured the failure this guards: five classes left two lints in one commit with the
    suite green, and only the lint carrying a floor noticed.
    """
    assert len(_release_call_sites()) >= _CLOSER_FLOOR


def test_the_resolver_reaches_receivers_that_carry_no_annotation() -> None:
    """The discriminator earns its complexity: three of the eight resolve only indirectly.

    A resolver that answered only where the call site is annotated would be a simpler rule and a
    vacuous one -- it would miss `MultiSink`'s iterated child, `decorator`'s aliased global and
    `Worker`'s `__init__`-assigned attribute, which is three of the eight closers. Each is
    asserted to resolve, and asserted to resolve to `Sink` rather than to merely something.
    """
    indirect = {
        ("decorator", "_close_orphan_sink", ast.Name),
        ("worker", "Worker._close_sink", ast.Attribute),
        ("sinks/multi", "MultiSink.close", ast.Name),
    }
    resolved: set[tuple[str, str, type]] = set()
    for stem, tree in _modules():
        globals_ = _module_globals(tree)
        for node, fn, cls in _walk_scopes(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not (isinstance(func, ast.Attribute) and func.attr == "release"):
                continue
            if not (isinstance(func.value, ast.Name) and func.value.id == "_lifecycle"):
                continue
            if not node.args:
                continue
            receiver = node.args[0]
            if not _resolve(receiver, fn, cls, globals_) & {"Sink"}:
                continue
            resolved.add((stem, _qualified(fn, cls), type(receiver)))
    assert indirect <= resolved, indirect - resolved


def test_a_transport_that_is_not_a_sink_is_out_of_scope() -> None:
    """`SocketTransport` has `send_all` and `close` and no `emit`, so it is not a sink here.

    The reused roster admits it, and a discriminator that stopped at roster membership would
    claim `LogstashSink`'s and `SyslogSink`'s socket closes -- ten sites, not eight, and two of
    them transports the sink built and owns outright. The exclusion is derived from the same
    scan rather than carved out by name, so a future `SocketTransport` that grows an `emit`
    comes into scope on its own.
    """
    assert "SocketTransport" not in _sink_type_names()
    assert {"MultiSink", "HTTPSink", "FileSink", "Sink"} <= _sink_type_names()


def test_a_ninth_direct_close_is_caught(tmp_path: pathlib.Path) -> None:
    """The lint fails against the thing it forbids, rather than only passing today.

    Written as an end-to-end scan of a real module on disk: a synthetic AST node would exercise
    the resolver while proving nothing about the walk that feeds it.
    """
    module = tmp_path / "ninth.py"
    module.write_text(
        "from log_foundry.sinks.base import Sink\n"
        "\n"
        "\n"
        "def retire(sink: Sink) -> None:\n"
        "    sink.close()\n",
        encoding="utf-8",
    )
    tree = ast.parse(module.read_text(encoding="utf-8"))
    sink_names = _sink_type_names()
    globals_ = _module_globals(tree)
    caught = [
        node.lineno
        for node, fn, cls in _walk_scopes(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "close"
        and _resolve(node.func.value, fn, cls, globals_) & sink_names
    ]
    assert caught == [5]
