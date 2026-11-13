"""The refusals of a sink argument that can never work: an unpatchable value, or a loop."""

from __future__ import annotations

import types
import weakref
from itertools import islice

from log_foundry import _diag, _fork
from log_foundry.sinks.base import _MRO, _NAMESPACE, _class_member, _instance_dict

_UNPATCHABLE: tuple[type, ...] = (
    object,
    str,
    bytes,
    bytearray,
    int,
    float,
    complex,
    bool,
    list,
    tuple,
    dict,
    set,
    frozenset,
    range,
)
"""The exact builtin types a sink argument is refused for: none can ever gain a callable ``emit``.

Each was probed at ``4fbfd60``: it is not a heap type and its ``__dictoffset__`` is ``0``, so
``setattr(x, "emit", f)`` raises; no class in its MRO has an ``emit``; and configured as
``sink=`` it lost every batch. **A closed list, not a rule.** "Not a heap type, and no instance
``__dict__``" was probed too and over-refuses: it holds for ``weakref.proxy(sink)`` and for
``super(Sub, inst)``, and both deliver, because a C type's own ``tp_getattro`` can forward
attribute lookup and Python cannot tell a forwarding one from the generic one. So another
unpatchable C type — ``memoryview``, an extension type — is accepted and loses its batches counted
and announced, as before; that trade is deliberate (Andrew's decisions of 2026-11-05).
"""


_LOOP_CEILING = 10_000
"""Objects :func:`loop_in` visits, and members it reads from one container, before it gives up.

A walk that gives up **accepts** the graph unexamined, which is the behaviour before the loop
refusal existed, so the bound trades a missed loop in a graph of more than ten thousand sinks for a
construction that never stalls. Ten thousand is far above any fan-out written by hand, and a walk
that reaches it was measured at about 27 ms at ``7ecfbad1``, so the headroom is deliberate in both
directions. Each wrapper constructor walks its own subgraph, so building a chain one wrapper at a
time costs quadratically in its depth; a chain deep enough for that to matter already hits
``RecursionError`` on its first ``emit``. Each ``MultiSink`` child is walked on its own, so the
same large subgraph handed as many children is walked once per child.
"""

_END = object()
"""The sentinel ``next`` returns when a node's forwarded sinks are exhausted."""

_THIS_CLASS = super.__dict__["__thisclass__"].__get__
"""A ``super`` object's ``__thisclass__``, read through ``super``'s own member, in C."""

_SELF_CLASS = super.__dict__["__self_class__"].__get__
"""A ``super`` object's ``__self_class__``, whose MRO its lookups search, read in C."""

_PROXY_TYPES: tuple[type, ...] = (weakref.ProxyType, weakref.CallableProxyType)
"""The exact types of a ``weakref.proxy``, which forwards every call to its referent."""


def unpatchable(value: object) -> bool:
    """Whether a value is exactly one of :data:`_UNPATCHABLE`'s types, so it can never be a sink.

    ``type(value) is t`` reads the type pointer and compares identities, so nothing of the value
    runs. Shared by :func:`require_sink` and by the fork walk's positional record, which skips a
    value ``require_sink`` would have refused even when one was assigned into a slot after
    construction.

    Args:
      value: Any object.

    Returns:
      Whether its exact type is one of the unpatchable builtin types.

    Raises:
      None.
    """
    return any(type(value) is kind for kind in _UNPATCHABLE)


def _forwards_to(node: object) -> list[object]:
    """Returns the sinks a node forwards every call to, read statically: its slots, or a target.

    Only two kinds of node forward: a ``super`` object whose ``emit`` resolves to a package
    wrapper's (:func:`_super_forwarded`), and a package wrapper, which forwards ``emit``,
    ``flush``, ``close`` and ``losses`` to the sinks in its ``_fork._CHILD_SINK_SLOTS`` positions. Anything else forwards
    nothing **as far as this walk knows**: a third-party sink holding another sink in an
    attribute may only keep a back-reference it never calls, so following its attributes would
    refuse graphs that work. The slot values are read from the instance ``__dict__`` through
    ``sinks.base._instance_dict`` and ``dict.items``, keys compared only when exactly ``str``, and a
    ``many`` slot's members through its builtin base's own iteration, so none of the node's code
    runs (SPEC-079 FR-002's rule for every walk over a handed-over graph). The one residue is
    ``sinks.base._class_member``'s: a key planted in a class namespace whose hash collides with a
    name looked up there (``__module__``) has its ``__eq__`` run, so the whole read sits inside the
    ``try`` and such a node costs only the check beneath it.

    Args:
      node: Any object the walk reached.

    Returns:
      The forwarded-to sinks, ``None`` dropped; empty for a node that forwards nothing, or whose
      storage could not be read.

    Raises:
      None. A node that cannot be read is announced and contributes nothing, so the walk accepts
        what lies beneath it, which is the behaviour before the loop refusal existed.
    """
    if type(node) is super:
        target = _super_forwarded(node)
        return [] if target is None else [target]
    try:
        slots = _fork._child_sink_slots(node)
        if not slots:
            return []
        own = _instance_dict(node)
        if own is None:
            return []
        held: list[object] = []
        for key, value in dict.items(own):
            kind = slots.get(key) if type(key) is str else None
            if kind == "one":
                held.append(value)
            elif kind == "many":
                for storage in _fork._storage_iterators(value):
                    held.extend(islice(storage, _LOOP_CEILING))
        return [sink for sink in held if sink is not None]
    except Exception as exc:
        _diag.absorbed(
            "reading a sink's children for a loop",
            exc,
            "the sinks beneath it are not checked for one",
        )
        return []


def _super_forwarded(node: object) -> object | None:
    """Returns the object a ``super`` node forwards into the package wrapper graph of, or ``None``.

    ``super(T, inst)`` searches ``inst``'s MRO **after** ``T``, so it forwards back into
    ``inst``'s wrapped sinks only when the first class after ``T`` that defines ``emit`` is a
    package wrapper with forwarding positions, or a subclass of one: a user ``Mid(MultiSink)``
    whose ``emit`` calls ``MultiSink.emit`` still inherits ``MultiSink.flush``, which forwards
    with no user code on the loop. A cooperative mixin — ``class Both(MultiSink,
    Recorder)`` handing ``super(MultiSink, self)`` as a child — resolves to ``Recorder.emit``,
    the caller's own code, and delivers; following every ``super`` to its bound object refused it.
    ``__thisclass__`` and ``__self_class__`` are read through ``super``'s own members, the MRO
    through ``sinks.base._MRO`` and each namespace through ``_NAMESPACE``, compared by identity,
    so nothing of the node runs; of its classes, only a namespace key planted to collide with
    ``emit`` or ``__module__`` can run its ``__eq__`` (``sinks.base._class_member``'s residue),
    which is caught here.

    Args:
      node: An object whose exact type is ``super``.

    Returns:
      Its bound object when the call it forwards lands in a package wrapper, else ``None``.

    Raises:
      None. A node whose classes cannot be read is announced and forwards nothing as far as the
        walk knows, which is the behaviour before the loop refusal existed.
    """
    target = _fork._super_target(node)
    if target is None:
        return None
    try:
        this = _THIS_CLASS(node)
        after = False
        for cls in _MRO(_SELF_CLASS(node)):
            if cls is this:
                after = True
            elif after and _NAMESPACE(cls).get("emit") is not None:
                wrapper = any(_fork._declared_slots(base) for base in _MRO(cls))
                return target if wrapper else None
    except Exception as exc:
        _diag.absorbed(
            "reading a super object's classes for a sink loop",
            exc,
            "the sinks beneath it are not checked for one",
        )
    return None


def _proxied_on_path(child: object, path: list[object]) -> object | None:
    """Returns the node on the current path a ``weakref.proxy`` child refers to, or ``None``.

    A proxy forwards every call to its referent, but the referent cannot be read from the proxy
    without calling into it. The other direction is static: ``weakref.getweakrefs`` lists, in C,
    the weak references and proxies to an object, so a proxy to a node on the path is found by
    identity among that node's. A proxy to a node **off** the path is not followed, so a loop
    that only closes beneath one is not found (``architecture.md`` §13).

    Args:
      child: A value whose exact type is a ``weakref`` proxy type.
      path: The nodes on the walk's current path, the ``back_to`` owner first when there is one.

    Returns:
      The node it refers to, or ``None``.

    Raises:
      None.
    """
    for node in path:
        if any(ref is child for ref in weakref.getweakrefs(node)):
            return node
    return None


def loop_in(root: object, *, back_to: object | None = None) -> object | None:
    """Returns a forwarding node the graph from ``root`` reaches again, or ``None`` when none does.

    A depth-first walk over :func:`_forwards_to`'s edges that keeps the nodes on the current path:
    an edge back to one of them is a loop, and every call into a sink on it — ``emit``, ``flush``,
    ``close``, a wrapper's ``deliver`` and stop-signal forward — recurses until Python's recursion
    limit. Measured before this refusal existed: one ``emit`` on a ``MultiSink`` holding itself and
    a ``MemorySink`` wrote the batch to the ``MemorySink`` 998 times, and its ``flush()`` raised
    ``RecursionError`` every time. ``back_to`` is treated as already on the path, which is how a
    wrapper under construction asks whether its new children reach it before it stores them.

    The walk is static (:func:`_forwards_to`), bounded by :data:`_LOOP_CEILING` visits, and
    follows only package forwarding positions, a ``super`` that resolves into one, and a
    ``weakref.proxy`` back to a node on the path (:func:`_proxied_on_path`). So four loops are
    **not** found, and are recorded in ``architecture.md`` §13: one that runs through a
    third-party sink's own code, one that closes only beneath a ``weakref.proxy`` to a node off
    the path, one formed after the last check ran — a slot assigned later, or two factory
    wrappers each building around the other at once — and one deeper than the ceiling.

    Args:
      root: The sink graph to examine.
      back_to: An object to treat as an ancestor of ``root``: the wrapper whose children these
        are, or ``None``.

    Returns:
      The first node reached twice on one path — ``back_to`` itself when the graph leads back to
      it — or ``None`` when no loop was found within the ceiling.

    Raises:
      None.
    """
    path: list[object] = [] if back_to is None else [back_to]
    on_path = {id(node) for node in path}
    if id(root) in on_path:
        return root
    if any(type(root) is kind for kind in _PROXY_TYPES):
        return _proxied_on_path(root, path)
    finished: set[int] = set()
    path.append(root)
    on_path.add(id(root))
    stack = [(root, iter(_forwards_to(root)))]
    visits = 1
    while stack:
        node, edges = stack[-1]
        child = next(edges, _END)
        if child is _END:
            stack.pop()
            path.pop()
            on_path.discard(id(node))
            finished.add(id(node))
            continue
        if id(child) in on_path:
            return child
        if id(child) in finished:
            continue
        if any(type(child) is kind for kind in _PROXY_TYPES):
            referent = _proxied_on_path(child, path)
            if referent is not None:
                return referent
        visits += 1
        if visits > _LOOP_CEILING:
            return None
        path.append(child)
        on_path.add(id(child))
        stack.append((child, iter(_forwards_to(child))))
    return None


def package_graph(root: object) -> frozenset[int] | None:
    """Returns the ids of the package's own sinks the graph from ``root`` forwards to, or ``None``.

    The receiver half of a final refusal's provenance (``sinks/_refusal.is_final_refusal``): the
    refusal is honoured only when the sink that refused is in this set, so only when the library
    itself forwarded the batch to it. A sink of the caller's own can choose where ``emit`` goes
    without leaving a frame in the refusal's traceback, and the diff reviews measured every shape
    below delivering on 1.x's resend through a fresh sink the caller had swapped in, where
    honouring the mark lost the batch: a ``__getattr__`` proxy, ``self.emit = inner.emit``
    rebound on a swap, a property returning a bound ``emit``, a subclass of a shipped sink that
    re-initialises itself, a ``CallbackSink`` whose callable dispatches through a property, a
    class that declares ``__module__ = "log_foundry..."``, ``FileSink.emit`` replaced on the
    class, and an ``emit`` planted in an instance ``__dict__`` under a ``str`` subclass key.

    So the walk answers ``None``, meaning "not the package's own", unless every node reached from
    ``root`` through the package wrappers' forwarding positions (:func:`_forwards_to`) has an
    exact type defined in this package (``_fork._defined_here``), whose ``emit``, found along its
    MRO's own namespaces, is exactly a plain function — not a property or other descriptor, which
    can route the call and return before it — and whose instance ``__dict__`` holds only exact
    ``str`` keys and none named ``emit``. Where that function was written is not checked: a plain
    function runs in a frame of its own, so one written outside the package is in the refusal's
    traceback and fails the traceback half; a filename test here was measured redundant (its
    mutant survived every case).
    A refuser need not be a forwarding node: ``SyslogSink`` and socket-mode ``LogstashSink`` hold
    the ``SocketTransport`` that refuses in an attribute, so each node's instance ``__dict__``
    values are in the set too — never walked further, since only a forwarding position forwards.
    A ``CallbackSink`` forwards nothing, so a sink its callable reaches is never in the set.

    The walk is static, as :func:`loop_in`'s is: no node's code runs. A ``super`` or a
    ``weakref.proxy`` is not a package type, a graph past :data:`_LOOP_CEILING` visits is not
    walked, and an unreadable node is doubt; each answers ``None``, which costs only the 1.x
    resend.

    Args:
      root: The sink whose ``emit`` the catcher called.

    Returns:
      The ids of every node and of every value its instance ``__dict__`` holds, or ``None`` when
      any node is not exactly the package's own, untouched.

    Raises:
      None.
    """
    try:
        held: set[int] = set()
        seen: set[int] = set()
        stack = [root]
        while stack:
            node = stack.pop()
            if id(node) in seen:
                continue
            seen.add(id(node))
            cls = type(node)
            if len(seen) > _LOOP_CEILING or not _fork._defined_here(cls):
                return None
            found = _class_member(cls, "emit")
            if found is None or type(found[0]) is not types.FunctionType:
                return None
            held.add(id(node))
            own = _instance_dict(node)
            if own is not None:
                for key, value in dict.items(own):
                    if type(key) is not str or key == "emit":
                        return None
                    held.add(id(value))
            stack.extend(_forwards_to(node))
        return frozenset(held)
    except Exception:
        return None


def require_sink(value: object, name: str, *, owner: object | None = None) -> None:
    """Refuses a sink argument that can never be a sink, or that would make a sink contain itself.

    The test is ``type(value) is t`` for each member of :data:`_UNPATCHABLE`: ``type()`` reads the
    object's type pointer and ``is`` compares identities, so neither a metaclass ``__eq__`` nor
    anything of the value runs. A value that lacks an ``emit`` today but could gain one — a class
    with ``emit = None`` whose ``start()`` binds it, a ``SimpleNamespace``, a module, a function —
    is accepted, since it delivers once it does; one that never does loses its batches counted
    and announced. The message names the type through its stored name, never the value.

    **A loop is refused after that**, through :func:`loop_in`: a graph that reaches ``owner`` —
    the wrapper about to store ``value`` — or that already contains a package wrapper reaching
    itself. No such graph works (:func:`loop_in` says what it did), so the refusal removes nothing
    a caller could use; it is a ``ValueError`` because the value is a sink of the right type
    placed where it closes a loop. The walk is static and follows only package forwarding
    positions, so a sink of the caller's own that merely keeps a reference to its parent is never
    refused.

    Args:
      value: The sink argument as the caller passed it.
      name: The argument's name for the message: ``sink``, ``FilteringSink inner``,
        ``TransformSink inner`` or ``MultiSink sinks[i]``.
      owner: The wrapper whose child ``value`` is about to become, or ``None`` at
        ``configure(sink=…)``.

    Returns:
      None.

    Raises:
      TypeError: When ``value`` is exactly one of the unpatchable builtin types.
      ValueError: When ``value`` leads back to ``owner``, or contains a wrapper that leads back
        to itself, so every call into it would recurse.
    """
    if unpatchable(value):
        raise TypeError(
            f"{name} must be a sink with an emit method, "
            f"got {_diag._STR_STR(_diag._STORED_NAME(type(value)))}"
        )
    looped = loop_in(value, back_to=owner)
    if looped is None:
        return
    kind = _diag._STR_STR(_diag._STORED_NAME(type(looped)))
    if owner is not None and looped is owner:
        raise ValueError(f"{name} leads back to this {kind}, so every call into it would recurse")
    raise ValueError(
        f"{name} holds a {kind} that leads back to itself, so every call would recurse"
    )
