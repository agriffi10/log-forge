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

**That sentence was false when it was first written, and how it was false is the more useful
half.** As first shipped the resolver claimed only *one* of the two socket closes. `SyslogSink`
escaped because it holds its transport as `self._socket = SocketTransport(...)` -- a bare
assignment whose right-hand side is a *call* -- which `_self_attribute_names` could not read at
all, so the receiver resolved to nothing and the site was excluded for the wrong reason. The
resolver now reads that shape, which both makes the claim above reproduce and closes a real
hole: a ninth close written `self._x = HTTPSink(...)` would have been missed silently, and that
is the idiom this codebase actually uses.

**What the resolver still cannot see** is stated rather than left to be found, because Phase 2's
ownership guard is exactly as complete as this lint: a receiver reached through a comprehension,
a nested function's closure over an outer local, `getattr`, an unannotated module global, tuple
unpacking, a subscript, or a stored callable. Each would be a sink close this lint waves
through. None occurs in `src/` today -- verified by enumerating all sixteen `.close()` calls --
and `test_the_resolver_blind_spots_are_the_stated_ones` pins the list so it stays honest.
"""

from __future__ import annotations

import ast
import pathlib
import threading
import time
from typing import TYPE_CHECKING

import log_foundry
from log_foundry import _lifecycle
from log_foundry.worker import Worker
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


def _modules(root: pathlib.Path | None = None) -> Iterator[tuple[str, ast.Module]]:
    """Yields every module under one root, keyed by a path-shaped stem.

    The `root` parameter is what lets a test scan a temporary directory through the *real*
    collector rather than reimplementing its filter, which is how the sibling roster
    (`_sink_classes_with_an_emit`) already does it -- a duplicated predicate is one that drifts.

    Args:
      root: The directory to scan, defaulting to the installed package.

    Returns:
      One `(stem, parsed module)` pair per source file, `sinks/multi` rather than `multi` so a
      name colliding across the package and its `sinks/` subpackage stays distinguishable.

    Raises:
      None.
    """
    base = _SRC if root is None else root
    for path in sorted(base.rglob("*.py")):
        stem = str(path.relative_to(base).with_suffix(""))
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
    """Resolves `self.<attr>` to a type, through the class body or a method's assignments.

    Three routes. An annotated assignment anywhere in the class
    (`self._http: HTTPSink | None = ...`); a bare `self._inner = inner` whose right-hand side is
    a parameter of the enclosing method; and `self._socket = SocketTransport(...)`, a bare
    assignment whose right-hand side *constructs* the thing.

    That third route was missing when this lint was first written, and the omission is the
    reason to be explicit about it: `SyslogSink` holds its transport exactly that way, so its
    `close()` receiver resolved to nothing at all and the site was out of scope for the wrong
    reason -- not because a `SocketTransport` is not a sink, but because the resolver could not
    see what it was. A ninth *sink* close written `self._x = HTTPSink(...)` would have been
    missed the same way, silently, which is the failure mode this whole file exists to prevent.

    Args:
      cls: The class the attribute belongs to.
      attr: The attribute name.

    Returns:
      The names, empty when no route resolves.

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
            elif isinstance(node, ast.Assign) and any(
                _is_self_attr(target, attr) for target in node.targets
            ):
                if isinstance(node.value, ast.Name):
                    names |= _parameter_annotation(fn, node.value.id)
                elif isinstance(node.value, ast.Call):
                    names |= _annotation_names(node.value.func)
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


def _sink_close_sites(
    root: pathlib.Path | None = None,
) -> list[tuple[str, str, int, frozenset[str]]]:
    """Returns every `.close()` under one root whose receiver resolves to a sink type.

    Args:
      root: The directory to scan, defaulting to the installed package.

    Returns:
      One `(module stem, scope, line, resolved names)` tuple per in-scope site.

    Raises:
      None.
    """
    sink_names = _sink_type_names()
    sites: list[tuple[str, str, int, frozenset[str]]] = []
    for stem, tree in _modules(root):
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


def test_the_detached_requesters_still_bind_the_thread_they_are_handed() -> None:
    """Both callers still capture the return value (FR-002 AC-8), which is half the claim.

    This is an AST assertion about the **call sites**, and on its own it is worth less than it
    looks: a helper that stopped returning the thread leaves every call site byte-identical, so
    this test cannot see it -- measured, `release` mutated to `_start_closer(sink); return None`
    passed all seven lints here. The other half of AC-8 is therefore behavioural and lives in
    the two tests below, one per caller.
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


class _CountingSink:
    """Counts closes and can take a measurable time over one, so a skipped join is visible."""

    def __init__(self, close_seconds: float = 0.0) -> None:
        self.closed = 0
        self._close_seconds = close_seconds
        self.entered = threading.Event()
        self.may_finish = threading.Event()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Accepts a batch and keeps nothing; these tests assert on closes, not deliveries."""

    def close(self) -> None:
        """Records the close, optionally taking long enough for a missing join to show."""
        self.entered.set()
        if self._close_seconds:
            time.sleep(self._close_seconds)
        self.closed += 1


def test_a_detached_release_hands_back_the_thread_running_the_close() -> None:
    """The helper's half of AC-8: the return value is a live handle to an outstanding close.

    Asserted while the close is *provably* still running -- the sink is parked inside `close()`
    and `closed` is still zero -- so this cannot pass against a helper that closed inline and
    returned `None`, nor against one that returned a thread which had already finished.
    """
    sink = _CountingSink()
    sink.may_finish.clear()

    def _park() -> None:
        sink.entered.set()
        sink.may_finish.wait(10.0)
        sink.closed += 1

    sink.close = _park  # type: ignore[method-assign]
    closer = _lifecycle.release(sink, detached=True)

    assert closer is not None, "a detached release returns the thread it started"
    assert sink.entered.wait(5.0), "and that thread is actually running the close"
    assert sink.closed == 0, "which is still outstanding, so the release did not close inline"

    sink.may_finish.set()
    closer.join(5.0)
    assert sink.closed == 1, "and joining the returned thread is what waits for it"


def test_the_worker_waits_for_the_swapped_out_close_it_started() -> None:
    """`Worker._close_swapped_out`'s bounded wait, which no test covered (FR-002 AC-8).

    The AST lint above cannot see a helper that stops returning the thread, and when that
    mutation was run the only test in the whole suite that died covered
    `decorator._swap_sink` -- this caller's wait had nothing asserting it at all.

    The close takes measurable time on purpose: an instant one completes before the assertion
    whether or not anything joined it, so the elapsed floor is what distinguishes waiting from
    firing and forgetting. A floor rather than a window, so load can only make it more true.
    """
    close_seconds = 0.4
    old = _CountingSink(close_seconds=close_seconds)
    new = _CountingSink()
    worker = Worker(old, batch_size=1000, flush_interval=100.0)
    try:
        start = time.monotonic()
        assert worker.swap_sink(new, timeout=5.0) is True
        elapsed = time.monotonic() - start

        assert old.closed == 1, "a close that fits the budget has completed when the swap returns"
        assert elapsed >= close_seconds, "so the worker waited rather than firing and forgetting"
    finally:
        worker.shutdown(2.0)


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

    The reused roster admits it, and the exclusion is derived from the same scan rather than
    carved out by name, so a future `SocketTransport` that grows an `emit` comes into scope on
    its own.
    """
    assert "SocketTransport" not in _sink_type_names()
    assert {"MultiSink", "HTTPSink", "FileSink", "Sink"} <= _sink_type_names()


def test_the_unrefined_rule_really_would_claim_both_socket_closes() -> None:
    """The amendment's evidence, measured here rather than asserted in prose.

    A rationale for changing an acceptance criterion has to reproduce, and this one did not when
    it was first written: the resolver could not read `SyslogSink`'s `self._socket =
    SocketTransport(...)`, so only one of the two sites was actually claimed. Pinning the
    measurement is what stops the docstrings above drifting back into a story.
    """
    unrefined = {"Sink"} | {node.name for _, node in _sink_classes_with_an_emit()}
    sink_names = _sink_type_names()
    all_sites: list[tuple[str, str, frozenset[str]]] = []
    for stem, tree in _modules():
        globals_ = _module_globals(tree)
        for node, fn, cls in _walk_scopes(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "close"
            ):
                names = frozenset(_resolve(node.func.value, fn, cls, globals_))
                if names & unrefined:
                    all_sites.append((stem, _qualified(fn, cls), names))

    under_unrefined = {(stem, scope) for stem, scope, _ in all_sites}
    under_refined = {(stem, scope) for stem, scope, names in all_sites if names & sink_names}

    assert under_unrefined - under_refined == {
        ("sinks/logstash", "LogstashSink.close"),
        ("sinks/syslog", "SyslogSink.close"),
    }, "the amendment's evidence: both socket closes, and only they, are what the clause removes"
    assert under_refined == {("_lifecycle", "release")}
    assert under_refined == {(stem, scope) for stem, scope, _line, _names in _sink_close_sites()}, (
        "and the refined set is what the shipped collector actually returns"
    )


def test_a_ninth_direct_close_is_caught(tmp_path: pathlib.Path) -> None:
    """The lint fails against the thing it forbids, rather than only passing today.

    An end-to-end scan of real modules on disk through `_sink_close_sites` itself -- not a
    reimplementation of its filter, which is a second copy of the rule free to drift from the
    one that ships. Both the annotated-parameter shape and the `self._x = Concrete(...)` shape
    are here, the second because it is the one that was missed.
    """
    (tmp_path / "ninth.py").write_text(
        "from log_foundry.sinks.base import Sink\n\n\ndef retire(sink: Sink) -> None:\n"
        "    sink.close()\n",
        encoding="utf-8",
    )
    (tmp_path / "tenth.py").write_text(
        "from log_foundry.sinks.http import HTTPSink\n\n\nclass Wrapper:\n"
        "    def __init__(self, url: str) -> None:\n"
        "        self._inner = HTTPSink(url)\n\n"
        "    def close(self) -> None:\n"
        "        self._inner.close()\n",
        encoding="utf-8",
    )
    caught = {(stem, scope) for stem, scope, _line, _names in _sink_close_sites(tmp_path)}
    assert caught == {("ninth", "retire"), ("tenth", "Wrapper.close")}


def test_the_resolver_blind_spots_are_the_stated_ones(tmp_path: pathlib.Path) -> None:
    """What the lint waves through, pinned so the module docstring stays honest.

    Phase 2's ownership guard is exactly as complete as this resolver, so an unstated blind spot
    is a hole in the guard nobody knows about. None of these shapes occurs in `src/` today --
    all sixteen `.close()` calls were enumerated -- and this test is what turns "none today"
    into something that fails the day one appears, rather than the day it causes a defect.

    A shape that starts being *caught* fails here too, which is the point: that is a resolver
    improvement somebody should notice and move out of this list.
    """
    shapes = {
        "comprehension": "def f(sinks: list[Sink]) -> None:\n    [s.close() for s in sinks]\n",
        "closure": "def f(sink: Sink) -> None:\n    def inner() -> None:\n        sink.close()\n",
        "getattr": "def f(sink: Sink) -> None:\n    getattr(sink, 'close')()\n",
        "unannotated_global": "SINK = None\n\n\ndef f() -> None:\n    SINK.close()\n",
        "unpacking": "def f(pair: tuple[Sink, Sink]) -> None:\n    a, b = pair\n    a.close()\n",
        "subscript": "def f(sinks: list[Sink]) -> None:\n    sinks[0].close()\n",
    }
    for name, body in shapes.items():
        (tmp_path / f"{name}.py").write_text(
            f"from log_foundry.sinks.base import Sink\n\n\n{body}", encoding="utf-8"
        )
    assert _sink_close_sites(tmp_path) == [], (
        "a shape here is now caught -- move it out of the list"
    )
