"""The refusals of a sink argument that can never work: an unpatchable value, or a loop."""

from __future__ import annotations

from itertools import islice

from log_foundry import _diag, _fork
from log_foundry.sinks.base import _instance_dict

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
construction that never stalls. Ten thousand is far above any fan-out written by hand and still a
few milliseconds of static reads, so the headroom is deliberate in both directions.
"""

_END = object()
"""The sentinel ``next`` returns when a node's forwarded sinks are exhausted."""


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

    Only two kinds of node forward: a ``super`` object, which forwards every member to the object
    it is bound to, and a package wrapper, which forwards ``emit``, ``flush``, ``close`` and
    ``losses`` to the sinks in its ``_fork._CHILD_SINK_SLOTS`` positions. Anything else forwards
    nothing **as far as this walk knows**: a third-party sink holding another sink in an
    attribute may only keep a back-reference it never calls, so following its attributes would
    refuse graphs that work. The slot values are read from the instance ``__dict__`` through
    ``sinks.base._instance_dict`` and ``dict.items``, keys compared only when exactly ``str``, and a
    ``many`` slot's members through its builtin base's own iteration, so none of the node's code
    runs (SPEC-079 FR-002's rule for every walk over a handed-over graph).

    Args:
      node: Any object the walk reached.

    Returns:
      The forwarded-to sinks, ``None`` dropped; empty for a node that forwards nothing, or whose
      storage could not be read.

    Raises:
      None. A node that cannot be read is announced and contributes nothing, so the walk accepts
        what lies beneath it, which is the behaviour before the loop refusal existed.
    """
    target = _fork._super_target(node)
    if target is not None:
        return [target]
    slots = _fork._child_sink_slots(node)
    if not slots:
        return []
    try:
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
            "reading a wrapper sink's children for a loop",
            exc,
            "the sinks beneath it are not checked for one",
        )
        return []


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
    follows only package forwarding positions. So four loops are **not** found, and are recorded
    in ``architecture.md`` §13: one that runs through a third-party sink's own code, one closed
    through a ``weakref.proxy`` (whose referent no static read reaches), one formed by mutating a
    slot after the last check ran, and one deeper than the ceiling.

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
    on_path: set[int] = set() if back_to is None else {id(back_to)}
    if id(root) in on_path:
        return root
    finished: set[int] = set()
    on_path.add(id(root))
    stack = [(root, iter(_forwards_to(root)))]
    visits = 1
    while stack:
        node, edges = stack[-1]
        child = next(edges, _END)
        if child is _END:
            stack.pop()
            on_path.discard(id(node))
            finished.add(id(node))
            continue
        if id(child) in on_path:
            return child
        if id(child) in finished:
            continue
        visits += 1
        if visits > _LOOP_CEILING:
            return None
        on_path.add(id(child))
        stack.append((child, iter(_forwards_to(child))))
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
