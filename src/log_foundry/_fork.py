"""Repairing the library's own synchronization state in a forked child (SPEC-039, SPEC-079)."""

from __future__ import annotations

import collections
import os
import sys
import threading
import types
import weakref
from typing import TYPE_CHECKING, Any, cast

from log_foundry import _diag
from log_foundry.sinks.base import (
    _ABSENT,
    _MEMBER_GET,
    _MRO,
    _NAMESPACE,
    _class_member,
    _instance_dict,
    _static_value,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

_PACKAGE = __name__.rpartition(".")[0]
"""This package's import name, derived rather than written.

A literal is wrong under a vendored install — ``myapp._vendor.log_foundry`` — where both the
roster's resolution and the ownership test would miss every module and the handler would
silently repair nothing.
"""

_RLOCK_TYPE = type(threading.RLock())

_LOCK_TYPE = type(threading.Lock())

_CONTAINER_TYPES: tuple[type, ...] = (list, tuple, set, frozenset, dict, collections.deque)

_PLAIN_TYPE_IDS = frozenset(
    id(kind)
    for kind in (dict, list, tuple, set, frozenset, str, bytes, int, float, bool, type(None))
)
"""The ids of the builtin types that provably cannot be a sink, read by ``_lifecycle``.

A fact rather than a heuristic: ``Sink`` requires ``emit`` and ``close``, and no builtin has
either. Tested by **exact type**, so a ``class MySink(dict)`` is still asked, and by the type's
**id**, so the test hashes an ``int`` and never the type: a frozenset of the types themselves
hashed every value's type, which runs a metaclass ``__hash__``, and compared it on a collision,
which runs a metaclass ``__eq__`` (SPEC-079 FR-002). Each is a builtin type alive for the
process, so its id is stable. Defined here rather than in ``_lifecycle`` because the lifecycle
inventory reads a ``frozenset`` built from a generator there as module state.
"""

_child_handlers: list[Callable[[], None]] = []
"""What runs in the child once its locks are its own again, in registration order.

**An inverted registry, and that is what keeps the dependency arrow pointing one way**
(FR-006). The work a child needs done belongs to ``_lifecycle`` and the sinks,
and all three would have to import this module for it to reach them; instead they register with
it, so this module imports nothing that imports it and there is no cycle to break later.
"""

reacquired_in_child: list[Any] = []
"""Sinks whose re-acquisition hook returned normally in this child (SPEC-042 FR-005).

Published rather than acted on, because what it means is an *ownership* claim and ownership is
``_lifecycle``'s: a sink that returns from the hook holds a transport of its own, so a child may
release it.

Rebound — never mutated in place — **before the repair walk runs**, so a second fork cannot
inherit the first one's roster. The first version of this claimed the rebind happened "at the
start of every child repair" while doing it in the middle, after the walk; both halves were false
and nothing tested either. Until SPEC-079 a module-level ``_FORK_SKIP`` also kept the module-wide
walk out of this list; no module is walked now, so the rebind is the whole of the guard.

This module still imports nothing but ``_diag`` (SPEC-039 FR-006); the reader registers a
handler and reads this, which is the same inversion :data:`_child_handlers` exists for.
"""

_installed = False
"""Whether :func:`install` has already registered the child handler in this process.

A flag and a function rather than a bare module-scope call, so that registration is one
statement a reader can find and repeated calls cannot stack handlers (FR-006 AC-2). What it
does **not** cover is stated rather than implied: ``importlib.reload`` re-runs this body and
resets the flag, so a deliberate reload registers a second handler. That is harmless — the
repair is idempotent and a child would simply run it twice — and closing it would mean
recording the registration somewhere outside this module, which is a worse trade than saying so.
"""


def _defined_here(cls: type) -> bool:
    """Whether one class was defined in this package.

    The defining module is read from the class's own namespace through ``sinks.base._NAMESPACE``
    and accepted only as an exact ``str``, so no metaclass ``__module__`` property runs and no
    ``__eq__`` or ``startswith`` of a planted ``str`` subclass is called (SPEC-079 FR-002). A
    builtin type carries no ``__module__`` in its own namespace and is never this package's.

    Args:
      cls: The class to place.

    Returns:
      Whether its defining module is this package or a module inside it.

    Raises:
      None.
    """
    module = _NAMESPACE(cls).get("__module__")
    if type(module) is not str:
        return False
    return module == _PACKAGE or module.startswith(f"{_PACKAGE}.")


def _is_class(value: object) -> bool:
    """Whether a value is a class, decided from its real type's MRO and never from ``__class__``.

    ``isinstance(value, type)`` falls back to the value's ``__class__`` when its real type is not a
    subclass, which runs a ``__class__`` property or a ``__getattribute__`` the value defines.
    The real type's MRO, read through ``sinks.base._MRO`` and compared by identity, runs nothing.

    Args:
      value: Any object.

    Returns:
      Whether ``type`` is in the MRO of the value's real type.

    Raises:
      None.
    """
    return any(base is type for base in _MRO(type(value)))


def _is_module(value: object) -> bool:
    """Whether a value is a module, decided statically as :func:`_is_class` decides a class.

    Args:
      value: Any object.

    Returns:
      Whether ``types.ModuleType`` is in the MRO of the value's real type.

    Raises:
      None.
    """
    return any(base is types.ModuleType for base in _MRO(type(value)))


def _is_owned(value: object) -> bool:
    """Whether this object runs code this package defines, anywhere in its ancestry.

    The ownership test is what keeps the traversal off third-party state (FR-003 AC-2): a
    ``boto3`` session's locks, a ``librdkafka`` handle and a ``psycopg`` connection are not the
    library's to swap, and reaching into them would be a fork fix that breaks a driver.

    **The whole MRO is asked, not just the defining module**, because subclassing a shipped sink
    is a documented extension point — ``README`` offers ``Sink`` to subclass and SPEC-038 rebuilt
    ``HTTPSink.emit`` as a template method for exactly that. An instance of a user's
    ``class MySink(FileSink)`` reports ``__main__``, so a defining-module test walked straight
    past a ``_lock`` that ``FileSink.__init__`` built: measured, the child hung in ``info()``
    while a plain ``FileSink`` in the same probe returned. That is the hang this spec exists to
    remove, reached through the one door users are told to use.

    What that widening costs is recorded rather than left to be discovered: a class **mixing in**
    a third-party base alongside a library one has its foreign attributes replaced too, measured
    on ``class MySink(FileSink, ThirdPartyBase)``. A separately *held* client is still untouched,
    which is the boundary FR-005 states — the two cannot be told apart from the instance, and
    refusing the mixed case would mean refusing the sink's own lock with it.

    **Every read is static** (SPEC-079 FR-002): whether the value is a class, and the MRO asked,
    come from the value's real type through ``sinks.base._MRO``, so no ``__class__`` property,
    ``__getattribute__`` or metaclass ``__mro__`` of the value runs.

    Args:
      value: Any object, including a class.

    Returns:
      Whether it or any of its ancestors was defined in this package.

    Raises:
      None.
    """
    owner = value if _is_class(value) else type(value)
    return any(_defined_here(base) for base in _MRO(owner))


def _container_base(value: object) -> type | None:
    """Returns the builtin container type a value's real type derives from, or ``None``.

    Read from the real type's MRO and compared by identity, so neither ``__class__`` nor a
    metaclass ``__eq__`` runs. The builtin base is what the walk iterates a container through,
    never the subclass, so a subclass's own ``__iter__``, ``keys`` or ``values`` is never
    entered (SPEC-079 FR-002).

    Args:
      value: Any object.

    Returns:
      The member of :data:`_CONTAINER_TYPES` in the value's MRO, or ``None``. At most one can
      be: no class derives from two of them, since their layouts conflict.

    Raises:
      None.
    """
    chain = _MRO(type(value))
    for kind in _CONTAINER_TYPES:
        if any(base is kind for base in chain):
            return kind
    return None


def _storage_iterators(container: object) -> list[Iterator[object]]:
    """Returns iterators over a container's real storage, through its builtin base's own methods.

    ``dict.keys`` and ``dict.values`` for a mapping, and the builtin base's unbound ``__iter__``
    otherwise, so a subclass's own ``__iter__``, ``keys`` or ``values`` never runs (SPEC-079
    FR-002). The iterators are lazy: the caller bounds how far each is read.

    Args:
      container: Any object.

    Returns:
      Two iterators for a mapping (keys, then values), one for any other container, and none
      for a value that is not a container.

    Raises:
      None.
    """
    base = _container_base(container)
    if base is None:
        return []
    if base is dict:
        held = cast("dict[object, object]", container)
        return [iter(dict.keys(held)), iter(dict.values(held))]
    iterate = cast("Callable[[object], Iterator[object]]", _NAMESPACE(base)["__iter__"])
    return [iterate(container)]


def _is_container(value: object) -> bool:
    """Whether the walk reads this value's members as well as its attributes.

    A subclass test rather than an exact-type test, so a ``deque``, a ``defaultdict`` or any
    other ordinary subclass is entered: an exact tuple made membership depend on which concrete
    class a future sink happened to hold its children in, which is the guess FR-003 AC-3 exists
    to replace. Decided by :func:`_container_base` from the real type's MRO, so the question
    runs none of the value's code.

    Args:
      value: Any object.

    Returns:
      Whether it holds members the walk should read.

    Raises:
      None.
    """
    return _container_base(value) is not None


_CHILD_SINK_SLOTS: dict[tuple[str, str], dict[str, str]] = {
    ("config", "Config"): {"sink": "one"},
    ("worker", "Worker"): {"sink": "one"},
    ("worker", "SwapOutcome"): {"previous": "one"},
    ("sinks.multi", "MultiSink"): {"_sinks": "many"},
    ("sinks.filtering", "FilteringSink"): {"_inner": "one"},
    ("sinks.transform", "TransformSink"): {"_inner": "one"},
    ("sinks._factory", "_FactorySink"): {"_inner": "one"},
    ("sinks.logstash", "LogstashSink"): {"_http": "one"},
    ("sinks.sentry", "SentrySink"): {"_http": "one"},
}
"""Where a package class holds a sink it was handed, by ``(module, qualname)`` and slot name.

**Ownership is recorded by position, not by shape** (SPEC-079). A value in one of these slots was
handed to the library as a sink — ``require_sink`` admitted it, or a factory built it — so the
walks record it the way ``stamp`` records the root, unconditionally, whatever a static read says
it looks like. A ``weakref.proxy``, a ``super(Sub, inst)`` or a ``__getattr__`` proxy there reads
as not sink-shaped, because no static read follows a C type's own forwarding, and recording by
shape left it unrecorded: its wrapper then refused its close in the parent, and a forked child
could claim it and close the parent's transport. ``one`` is a slot holding one sink, ``many`` a
container of them. The shape test, ``_sink_shaped``, is kept only where position is unknown, in
holders this package does not define. ``tests/test_child_sink_slots.py`` holds the table complete
by behaviour — a class that stores a ``Sink``-annotated value on ``self`` however it is written,
calls ``require_sink`` or releases a sink with ``owner=self`` must have an entry — and checks each
entry at run time by building its class around a sentinel sink and requiring it recorded.

The same positions are the only edges ``_sink_arg.loop_in`` follows when it refuses a sink graph
that contains itself, so an entry here is also what makes a new wrapper's loop refusable
(``tests/test_sink_loops.py`` builds every entry around itself).
"""

_QUALNAME = type.__dict__["__qualname__"].__get__
"""``type.__qualname__``, unbound, so a metaclass property of that name never runs."""

_SUPER_SELF = super.__dict__["__self__"].__get__
"""A ``super`` object's ``__self__``, read through ``super``'s own member, in C."""


def _child_sink_slots(holder: object) -> dict[str, str]:
    """Returns the child-sink slots a package holder declares, through its MRO, read statically.

    Args:
      holder: Any object the walk reached.

    Returns:
      Slot name to ``one`` or ``many``; empty for anything that declares none.

    Raises:
      None.
    """
    slots: dict[str, str] = {}
    if _is_class(holder):
        return slots
    for base in _MRO(type(holder)):
        slots.update(_declared_slots(base))
    return slots


def _declared_slots(cls: type) -> dict[str, str]:
    """Returns the child-sink slots one class itself declares in the roster, read statically.

    Args:
      cls: A class from a holder's MRO.

    Returns:
      Its own ``_CHILD_SINK_SLOTS`` entry, or empty when it has none or is not this package's.

    Raises:
      None.
    """
    if not _defined_here(cls):
        return {}
    module = _NAMESPACE(cls).get("__module__")
    qualname = _QUALNAME(cls)
    if type(module) is not str or type(qualname) is not str:
        return {}
    return _CHILD_SINK_SLOTS.get((module.removeprefix(f"{_PACKAGE}."), qualname), {})


def _super_target(value: object) -> object | None:
    """Returns the object a ``super`` value is bound to, read in C, or ``None`` for anything else.

    A ``super(Sub, inst)`` configured as a sink forwards every member to ``inst``, so the walk
    treats it as transparent and reaches ``inst`` through it: a ``FileSink`` behind it is then
    repaired and re-acquired in a child like one configured directly (SPEC-079). The exact type is
    compared by identity and ``__self__`` read through ``super``'s own member, so nothing of the
    value runs. A ``weakref.proxy``'s referent cannot be reached without calling into it, so a
    sink behind one is not repaired (``architecture.md`` §13).

    Args:
      value: Any object.

    Returns:
      The bound object, or ``None``.

    Raises:
      None.
    """
    if type(value) is not super:
        return None
    try:
        target: object = _SUPER_SELF(value)
    except Exception:
        return None
    return target


_WEAK_PROXY_TYPE_IDS = frozenset({id(weakref.ProxyType), id(weakref.CallableProxyType)})
"""The ids of the two ``weakref`` proxy types, compared by identity so nothing of a value runs."""


def _is_weak_proxy(value: object) -> bool:
    """Whether a value is exactly a ``weakref`` proxy, which no static read sees through.

    The marking frontier, where position is unknown, records one as it records a sink-shaped
    object (SPEC-079): ``isinstance`` followed a proxy's forwarded ``__class__`` and recorded it,
    the static test cannot, and an unrecorded proxy a third-party holder kept was claimable by a
    child that re-configured it. Recording runs nothing; over-recording a proxy that is no sink
    only makes it unclaimable in that child.

    Args:
      value: Any object.

    Returns:
      Whether its exact type is ``weakref.ProxyType`` or ``weakref.CallableProxyType``.

    Raises:
      None.
    """
    return id(type(value)) in _WEAK_PROXY_TYPE_IDS


def _sink_shaped(value: object) -> bool:
    """Whether a value satisfies ``Sink``'s two members, read without running any of its code.

    The static twin of ``isinstance(value, Sink)`` on Python 3.12 and later, whose runtime
    protocol check reads ``emit`` and ``close`` with ``inspect.getattr_static`` and refuses a
    member that is ``None``. ``isinstance`` also asks the ABC machinery first, which reads the
    value's ``__class__`` and hashes and compares its type in the ABC cache, running a
    ``__class__`` property, a ``__getattribute__`` and a metaclass ``__hash__`` or ``__eq__``
    (SPEC-079 FR-002, measured through ``configure()``). Here each member is read with
    ``sinks.base._static_value``. A class is never sink-shaped, whatever functions it carries:
    the walk records instances it may close, and a class that ``isinstance`` would accept is a
    named divergence in ``tests/test_fork_static_reads.py``.

    Args:
      value: Any object.

    Returns:
      Whether both members resolve, neither to ``None``.

    Raises:
      None.
    """
    if _is_class(value):
        return False
    for name in ("emit", "close"):
        found = _static_value(value, name)
        if found is _ABSENT or found is None:
            return False
    return True


_MODULE_DICT: Callable[[types.ModuleType], dict[str, Any]] = _NAMESPACE(types.ModuleType)[
    "__dict__"
].__get__
"""A module's ``__dict__``, read through ``ModuleType``'s own member, so no hook of it runs."""

_SLOT_SEQUENCES: tuple[type, ...] = (tuple, list)
"""The exact types a ``__slots__`` declaration is read from; anything else declares nothing."""


def _slot_names(holder: object) -> list[tuple[type, str]]:
    """Returns every ``__slots__`` name declared across a holder's class hierarchy, with its class.

    A slotted instance keeps its attributes off ``__dict__``, so a walk reading only the instance
    dict would miss them. No shipped class needs this today and that is stated rather than dressed
    up: ``worker._FlushMarker`` is slotted and holds an ``Event``, but a marker lives inside a
    ``queue.Queue`` the walk never enters. It is here because the shape lint accepts a slotted
    ``self.<attr>``, so the walk has to be able to reach one.

    **Read statically** (SPEC-079 FR-002): the MRO and each class's namespace through
    ``sinks.base._MRO`` and ``_NAMESPACE``, and a declaration accepted only as an exact ``str``,
    or an exact ``tuple`` or ``list`` of exact ``str``, so no metaclass ``__mro__``, no iterator
    of a planted sequence and no ``__str__`` or ``__eq__`` of a planted name runs.

    Args:
      holder: Any object.

    Returns:
      ``(declaring class, name)`` for each declared slot, which may be empty.

    Raises:
      None.
    """
    names: list[tuple[type, str]] = []
    for cls in _MRO(type(holder)):
        declared = _NAMESPACE(cls).get("__slots__", ())
        if type(declared) is str:
            names.append((cls, declared))
        elif any(type(declared) is kind for kind in _SLOT_SEQUENCES):
            names.extend((cls, name) for name in declared if type(name) is str)
    return names


def _str_keyed(pairs: Any) -> list[tuple[str, Any]]:
    """Keeps the pairs whose key is an exact ``str``, deciding by type before anything hashes it.

    A key planted in an instance ``__dict__`` can be a ``str`` subclass with its own ``__hash__``
    or ``__eq__``, which every later set, frozenset or dict lookup on the name would run. No
    attribute the walk needs has such a name, so the pair is dropped unread.

    Args:
      pairs: ``(key, value)`` pairs from a builtin ``dict``'s own ``items``.

    Returns:
      The pairs with an exact-``str`` key.

    Raises:
      None.
    """
    return [(key, value) for key, value in pairs if type(key) is str]


def _namespace_items(holder: Any) -> list[tuple[str, Any]]:
    """Returns the ``(name, value)`` pairs a holder owns, running none of the holder's code.

    Values come from the instance ``__dict__``, read through ``sinks.base._instance_dict`` and
    copied with ``dict.items``, and from the slot member descriptors, rather than from ``vars()``
    or a blanket ``getattr`` over ``dir()``: ``vars()`` reads ``__dict__`` through the holder's
    ``__getattribute__`` or a ``__dict__`` property, ``dict()`` of a hostile mapping runs its
    ``keys`` and ``__getitem__``, and ``getattr`` evaluates every property, including ones that
    open a connection or take the very lock the walk is about to replace (SPEC-079 FR-002). A
    class's items are its own namespace through ``_NAMESPACE``, and a module's its ``__dict__``
    through ``ModuleType``'s member.

    Args:
      holder: A module, a class, or an instance.

    Returns:
      One pair per attribute the holder itself carries.

    Raises:
      None. A holder that cannot be read contributes nothing, announced, since a repair that
        stops at the first awkward object leaves the rest of the process holding dead locks.
    """
    try:
        if _is_class(holder):
            return _str_keyed(dict.items(_NAMESPACE(holder).copy()))
        if _is_module(holder):
            return _str_keyed(dict.items(_MODULE_DICT(holder)))
        own = _instance_dict(holder)
        items: list[tuple[str, Any]] = [] if own is None else _str_keyed(dict.items(own))
        present = {name for name, _value in items}
        for cls, name in _slot_names(holder):
            if name in present:
                continue
            value = _slot_value(holder, cls, name)
            if value is not None:
                items.append((name, value))
        return items
    except Exception as exc:
        _diag.absorbed("reading an object's attributes after a fork", exc, "it is not repaired")
        return []


def _slot_value(holder: object, cls: type, name: str) -> Any | None:
    """Reads one slot through its member descriptor, answering ``None`` when it is unset.

    Only an exact ``MemberDescriptorType`` in the declaring class's own namespace is read, through
    its builtin ``__get__``, so no property, ``__getattr__``, ``__getattribute__`` or descriptor
    of the holder runs (SPEC-079 FR-002). An unset slot, a name the class shadows with anything
    else, and a read that raises all answer ``None``, because each means "there is nothing here
    to replace": a lock is never ``None``, so nothing is lost by conflating them.

    Args:
      holder: The instance to read from.
      cls: The class whose ``__slots__`` declared the name.
      name: The slot name.

    Returns:
      The value, or ``None``.

    Raises:
      None.
    """
    member = _NAMESPACE(cls).get(name)
    if type(member) is not types.MemberDescriptorType:
        return None
    try:
        return _MEMBER_GET(member, holder, type(holder))
    except Exception:
        return None


def _set_only_dict(holder: object, name: str) -> dict[str, object] | None:
    """Returns the instance's own ``__dict__`` when its class member ``name`` is set-only.

    A set-only descriptor defines ``__set__`` and no ``__get__``, so CPython and the static
    reader read the instance's ``__dict__`` past it, and the ``__dict__`` entry is the value.
    Every lookup is static (``_class_member``, ``_instance_dict``), so no hook of the holder or
    its class runs.

    Args:
      holder: An instance the walk reached.
      name: The attribute about to be rebound.

    Returns:
      The instance ``__dict__``, or ``None`` when the member is anything else, or nothing, or
      cannot be read.

    Raises:
      None.
    """
    try:
        found = _class_member(type(holder), name)
        if found is None:
            return None
        kind = type(found[0])
        if _class_member(kind, "__get__") is not None or _class_member(kind, "__set__") is None:
            return None
        return _instance_dict(holder)
    except Exception:
        return None


def _assign(holder: Any, name: str, value: Any) -> None:
    """Puts a fresh primitive where the dead one was.

    Instances are written through ``object.__setattr__`` so a frozen dataclass or a custom
    ``__setattr__`` cannot refuse the repair; modules and classes take the ordinary path,
    which is the only one they have. One instance case is written straight into its own
    ``__dict__`` instead: a name whose class member is a set-only descriptor
    (:func:`_set_only_dict`), because ``object.__setattr__`` would run that descriptor's
    ``__set__`` — SPEC-075's wrappers forward their stop signal there, to a sink whose setter
    may take a lock this walk has not replaced yet — while the value it stores is the one in the
    ``__dict__``, which every read finds.

    Args:
      holder: The module, class or instance carrying the attribute.
      name: The attribute to rebind.
      value: The replacement primitive.

    Returns:
      None.

    Raises:
      None. A holder that refuses the write keeps a primitive no thread can ever release, which
        is announced rather than raised: this runs in a child that has not yet returned from
        ``fork``.
    """
    try:
        if _is_class(holder) or _is_module(holder):
            setattr(holder, name, value)
        elif (own := _set_only_dict(holder, name)) is not None:
            own[name] = value
        else:
            object.__setattr__(holder, name, value)
    except Exception as exc:
        _diag.absorbed(
            "re-initialising a lock after a fork",
            exc,
            f"{_diag._STR_STR(_diag._STORED_NAME(type(holder)))}.{name} may block the next "
            f"caller forever",
        )


def _fresh_primitive(value: Any, memo: dict[int, Any], keepalive: list[Any]) -> Any | None:
    """Returns the replacement for one lock or event, minting it at most once.

    **The memo is load-bearing, not tidiness.** A sink's ``log_foundry_stop_signal`` *is* the
    worker's ``_stop`` (SPEC-027), so two fresh events would leave the worker setting one and
    the sink waiting on the other — a shutdown that never cuts a backoff short. An ``Event``
    carries its set state across, which is also what makes replacing one safe at all.

    Args:
      value: The attribute value under inspection.
      memo: Replacements already minted, keyed by the id of what they replace.
      keepalive: Holds every replaced primitive, so no id in ``memo`` can be reused by a later
        object and hand back the wrong replacement.

    The type tests read the value's real type's MRO through ``sinks.base._MRO`` and compare by
    identity, so no ``__class__`` property of an attribute value runs (SPEC-079 FR-002). An
    ``Event``'s ``is_set`` is the one call made, and only on a real ``threading.Event``.

    Returns:
      The replacement, or ``None`` when this value is not a lock or an event.

    Raises:
      None.
    """
    existing = memo.get(id(value))
    if existing is not None:
        return existing
    fresh: Any
    kinds = _MRO(type(value))
    if any(kind is threading.Event for kind in kinds):
        fresh = threading.Event()
        if value.is_set():
            fresh.set()
    elif any(kind is _RLOCK_TYPE for kind in kinds):
        fresh = threading.RLock()
    elif any(kind is _LOCK_TYPE for kind in kinds):
        fresh = threading.Lock()
    else:
        return None
    memo[id(value)] = fresh
    keepalive.append(value)
    return fresh


_REACQUIRE_HOOK = "reacquire_after_fork"
"""The optional sink member a forked child asks to take its transport over.

Named for the larger half of what it does (SPEC-042 FR-005): stranding the parent's pending
bytes is one consequence, and re-acquiring the transport is the step — a sink that returns from
it has claimed the transport as this process's own, which is what makes releasing it safe.

Probed by name, as ``losses()`` and ``log_foundry_stop_signal`` are, so no existing sink stops
satisfying ``Sink`` by not having it. ``sinks/base.py`` states the contract; this is the only
place the name is read.
"""


def _offers_reacquire(holder: object) -> bool:
    """Whether this object carries FR-004's buffer hook, without letting the question raise.

    Read with ``sinks.base._static_value``, which raises nothing and runs none of the holder's
    code (SPEC-079 FR-002): an owned ``__getattr__`` or a user subclass's property raising
    anything would otherwise abort the walk, and what is lost then is not a buffer but the
    **lock repair** for everything the walk had not reached yet — the hang this module exists to
    remove, arrived at through the probe for a different hazard.

    Callability is part of the question, so an attribute of that name which is not a method is
    simply not the hook; a property reads as the property object, which is not callable.

    **A class or a module never offers it**, decided statically. ``_is_owned`` accepts a package
    class, so without this a wrapper holding ``self.kind = FileSink`` had every child call the
    unbound ``FileSink.reacquire_after_fork()``, absorb the ``TypeError`` and announce a
    re-acquisition failure that never happened.

    Args:
      holder: Any object the walk has entered.

    Returns:
      Whether it offers the hook.

    Raises:
      None.
    """
    if _is_class(holder) or _is_module(holder):
        return False
    return callable(_static_value(holder, _REACQUIRE_HOOK))


_SKIP_ATTRIBUTE = "_FORK_SKIP"
"""A holder's opt-out: attribute names the walk must not read or descend into.

**Bookkeeping that pins objects is not live state to repair**, and the difference is invisible to a
walk that only sees a container. ``_lifecycle._state._owned`` is the case: SPEC-042 FR-001 requires
it to hold a **strong** reference to every sink this process ever acquired, so without an opt-out
the walk reaches sinks the process abandoned several ``configure()`` calls ago — replacing their
locks, which is merely wasteful, and calling their fork hooks, which is not. Measured: a forked
child announced a buffer-discard failure for a sink that had been superseded, and a ``FileSink`` in
that state would have its file reopened on every fork for the life of the process.

Declared by the holder of the state — a module, or a class whose instance holds it, since
``sinks.base._static_value`` reads either — rather than listed here, which keeps this module's rule that it imports
nothing but ``_diag`` (SPEC-039 FR-006). Nothing is lost by skipping: a sink that is still *live* is
reached through the config and the worker, which are not opted out.
"""


_SKIP_SEQUENCES: tuple[type, ...] = (tuple, list, frozenset)
"""The exact types a ``_FORK_SKIP`` declaration is read from; anything else opts nothing out."""


def _skipped_names(holder: object) -> frozenset[str]:
    """Returns the attribute names a holder has opted out of the walk.

    The declaration is read with ``sinks.base._static_value`` and accepted only as an exact
    ``tuple``, ``list`` or ``frozenset``, keeping its exact ``str`` members, so no property,
    iterator or ``__eq__`` of a planted value runs (SPEC-079 FR-002).

    Args:
      holder: The module, class or instance the walk is about to read.

    Returns:
      The opted-out names, empty for anything that declares none.

    Raises:
      None. A malformed or unreadable declaration opts nothing out, so the walk does more work
        rather than less — the safe direction for a repair whose absence is a hang.
    """
    declared = _static_value(holder, _SKIP_ATTRIBUTE)
    if not any(type(declared) is kind for kind in _SKIP_SEQUENCES):
        return frozenset()
    return frozenset(name for name in declared if type(name) is str)  # type: ignore[attr-defined]


_GLOBAL_PRIMITIVES: tuple[tuple[str, str], ...] = (
    ("_lifecycle", "_state._lock"),
    ("_lifecycle", "_state._stop"),
    ("_lifecycle", "_state._owned_lock"),
    ("_lifecycle", "_state._closes._lock"),
    ("config", "_config_lock"),
    ("decorator", "_sweep_lock"),
    ("decorator", "_loss_lock"),
    ("api", "_console._lock"),
)
"""Every lock and event the package holds as module state, listed (SPEC-079 FR-001).

Each pair is a module of this package and a dotted attribute path from it. A lock held by the
package rather than by any sink — module-level state, or state on an object bound to a module
global — was found by a walk over every package module until SPEC-079; it is listed instead, and
``tests/test_fork_roster.py`` holds the list complete from both sides: a live sweep from every
module global, and an AST scan of every class that assigns a primitive to ``self``. A child
resolves each pair through ``sys.modules``, so a vendored install still resolves it and this
module imports nothing.
"""


class _Repair:
    """One fork's repair state, shared by the roster and the walk.

    Built once per fork by :func:`_reinit_after_fork`. The memo is load-bearing: a sink's
    ``log_foundry_stop_signal`` *is* the worker's ``_stop`` and ``_lifecycle._state._stop``
    (SPEC-027), and one memo shared by the roster and the walk is what hands all three the same
    replacement. ``seen`` and ``forwarded`` are the walk's own, carried here so several calls of
    ``_lifecycle._reachable_sinks`` share one ``seen`` set and the marking frontier starts from
    what the repair frontier forwarded; this module treats both as an opaque bag.

    Attributes:
      memo: Replacements already minted, keyed by the id of what they replace.
      keepalive: Every replaced primitive, so no id in ``memo`` can be reused by a later object.
      hooked: Holders offering ``reacquire_after_fork``, collected by the walk outside record
        seeds, in the order it reached them.
      seen: Ids of every object either frontier of the walk has visited.
      forwarded: What the repair frontier handed to the marking frontier, in order.
    """

    def __init__(self) -> None:
        """Starts every collection empty.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.memo: dict[int, Any] = {}
        self.keepalive: list[Any] = []
        self.hooked: list[Any] = []
        self.seen: set[int] = set()
        self.forwarded: list[Any] = []

    def repair(self, holder: object) -> list[tuple[str, Any]]:
        """Replaces a holder's own locks and events, and returns the rest of its namespace.

        Each holder's namespace is read once, here, so a walk that repairs and then descends
        reads nothing twice. A name in the holder's ``_FORK_SKIP`` is dropped entirely: it is
        neither replaced nor returned, so the walk neither enters, pushes nor forwards it.

        Args:
          holder: A package-owned object, class or module the walk reached.

        Returns:
          The ``(name, value)`` pairs that were neither skipped nor replaced.

        Raises:
          None.
        """
        skip = _skipped_names(holder)
        rest: list[tuple[str, Any]] = []
        for name, value in _namespace_items(holder):
            if name in skip:
                continue
            fresh = _fresh_primitive(value, self.memo, self.keepalive)
            if fresh is None:
                rest.append((name, value))
            else:
                _assign(holder, name, fresh)
        return rest


def _replace_roster(repair: _Repair) -> None:
    """Replaces each primitive :data:`_GLOBAL_PRIMITIVES` names, through the shared memo.

    A pair whose module is absent from ``sys.modules`` is skipped silently: the process never
    built that primitive. A pair whose module is present but whose path does not resolve is
    announced by type and the other pairs are still replaced. The first hop is read from the
    module's ``__dict__`` and every later one with ``sinks.base._static_value``, so nothing of
    the holder runs.

    Args:
      repair: The fork's shared repair state.

    Returns:
      None.

    Raises:
      None.
    """
    for module_name, path in _GLOBAL_PRIMITIVES:
        module = sys.modules.get(f"{_PACKAGE}.{module_name}")
        if module is None or not _is_module(module):
            continue
        try:
            holder, name, value = _resolve(module, path)
        except (KeyError, AttributeError) as exc:
            _diag.absorbed(
                "replacing a global lock after a fork",
                exc,
                f"{module_name}.{path} may block the next caller forever",
            )
            continue
        fresh = _fresh_primitive(value, repair.memo, repair.keepalive)
        if fresh is not None:
            _assign(holder, name, fresh)


def _resolve(module: types.ModuleType, path: str) -> tuple[object, str, object]:
    """Resolves one roster path to its holder, its last name and the value there.

    Args:
      module: The pair's module, from ``sys.modules``.
      path: The dotted attribute path from it.

    Returns:
      ``(holder, name, value)``: the object carrying the last hop, that hop, and its value.

    Raises:
      KeyError: When the module's ``__dict__`` has no first hop.
      AttributeError: When a later hop is missing, read with ``sinks.base._static_value``.
    """
    hops = path.split(".")
    holder: object = module
    value: object = _MODULE_DICT(module)[hops[0]]
    for hop in hops[1:]:
        holder = value
        value = _static_value(holder, hop)
        if value is _ABSENT:
            raise AttributeError(hop)
    return holder, hops[-1], value


_walk: Callable[[_Repair], None] | None = None
"""The one walk a child runs after the roster, set by :func:`register_walk`.

Registered rather than imported, the inversion :data:`_child_handlers` exists for: the walk is
``_lifecycle``'s, and this module imports nothing that imports it.
"""


def register_walk(fn: Callable[[_Repair], None]) -> None:
    """Sets the walk a forked child runs after the roster and before the hooks.

    A second registration replaces the first, which is the reload case :data:`_installed`
    records: a reloaded ``_lifecycle`` registers its new walk, and one walk runs per fork.

    Args:
      fn: Called in the child with the fork's ``_Repair``. A raise is absorbed and announced,
        and the hooks it collected before the raise still run.

    Returns:
      None.

    Raises:
      None.
    """
    global _walk
    _walk = fn


def _reacquire_transports(holders: list[Any]) -> list[Any]:
    """Asks each sink that owns a buffered stream to re-acquire it in this child (FR-004).

    A fork landing inside ``emit`` — after the write loop, before the flush — leaves the child
    holding the parent's unflushed bytes, which both processes then write: measured, the event
    at the fork point appeared on disk twice. Without a ``before`` handler there is nowhere to
    empty the buffer from (FR-001), so the child re-opens instead, which strands the copy.

    **Which hooks returned normally is published rather than acted on** (SPEC-042 FR-005). A
    sink that returns from the hook has claimed the transport as this process's own, which is an
    ownership fact — but ownership lives in ``_lifecycle``, and this module imports nothing but
    ``_diag``. So the roster is handed back and the handler ``_lifecycle`` registers re-stamps
    it.

    Args:
      holders: What the registered walk collected in ``_Repair.hooked``.

    Returns:
      The holders whose hook returned normally, in the order they were called.

    Raises:
      None. One sink's failure is absorbed separately from the rest, so a hook that cannot
        strand its buffer costs that sink a duplicated batch rather than costing every other
        sink its repair — and the consequence is named, because an absorbed failure is
        invisible apart from that line.
    """
    reacquired: list[Any] = []
    for holder in holders:
        try:
            getattr(holder, _REACQUIRE_HOOK)()
            reacquired.append(holder)
        except Exception as exc:
            _diag.absorbed(
                "re-acquiring a transport after a fork",
                exc,
                f"{_diag._STR_STR(_diag._STORED_NAME(type(holder)))} may write the parent's "
                f"pending bytes again, and this "
                f"child will not release it",
            )
    return reacquired


def register_child_handler(fn: Callable[[], None]) -> None:
    """Adds work to be done in a forked child, after its locks have been re-initialised.

    A handler may take any of the library's locks, which is what the ordering in
    :func:`_reinit_after_fork` buys it and why registering is the only way in. What it must not
    assume is that it is alone: handlers run in registration order and an earlier one may have
    started a thread — ``_lifecycle``'s worker rebuild does exactly that — so only the *first* runs in a
    genuinely single-threaded child. None of them may block, since nothing has returned from
    ``fork`` yet.

    Registering the same function twice is a no-op, compared by **identity**: ``in`` would ask
    a registered object's ``__eq__``, which is not this function's to trust and would raise into
    a caller documented as raising nothing. The exposure is the one :data:`_installed` records
    for the fork registration itself: a reload of a module whose body registers here would
    otherwise stack a second handler, and for the worker rebuild that means two drain threads,
    the first bound to a queue nothing writes to.

    Args:
      fn: Called with no arguments in the child. A failure is absorbed and announced, and the
        remaining handlers still run.

    Returns:
      None.

    Raises:
      None.
    """
    if not any(handler is fn for handler in _child_handlers):
        _child_handlers.append(fn)


def _reinit_after_fork() -> None:
    """Repairs the library in a child that has just returned from ``fork``.

    **The order of work here is the contract** (SPEC-039 FR-001 AC-2, SPEC-079 FR-002):

    1. rebind :data:`reacquired_in_child`, so this fork cannot inherit the last one's;
    2. replace the roster of global primitives, :data:`_GLOBAL_PRIMITIVES`;
    3. run the registered walk with the shared :class:`_Repair`, which repairs every sink and
       worker reached from the runtime's roots and marks what the child inherited;
    4. run the hooks it collected (:func:`_reacquire_transports`);
    5. run the registered handlers.

    A lock re-initialised *after* a handler that takes it is a handler that hangs, and it hangs
    on the child's only thread with nothing to interrupt it. Work that must happen before *any*
    handler stays inline rather than being registered — the buffer discard is the case, and
    registering it would put it after ``_lifecycle``'s worker rebuild, which has started a live
    drain thread by then, emitting into the very sink whose buffer is still the parent's.

    Args:
      None.

    Returns:
      None.

    Raises:
      None. A fork handler that raises has its exception printed by CPython with a full
        traceback, carrying the message arch §6 keeps out of anything the library says about
        itself — and it would leave the rest of the repair undone. So a walk that raises is
        absorbed and announced, the hooks it collected before the raise still run, and each
        handler's failure is absorbed separately from the rest: a child that cannot rebuild its
        worker should still have working locks. The handler list is iterated live rather than
        copied, which is safe because CPython's list iterator is index-based, so a handler that
        registers another simply causes it to run — and :func:`register_child_handler` is the
        only writer, appending only.
    """
    global reacquired_in_child
    reacquired_in_child = []
    repair = _Repair()
    try:
        _replace_roster(repair)
    except Exception as exc:
        _diag.absorbed("replacing the global locks after a fork", exc, "this child may block")
    walk = _walk
    if walk is not None:
        try:
            walk(repair)
        except Exception as exc:
            _diag.absorbed(
                "repairing the library after a fork", exc, "this child may block or lose"
            )
    reacquired_in_child = _reacquire_transports(repair.hooked)
    for handler in _child_handlers:
        try:
            handler()
        except Exception as exc:
            _diag.absorbed("running a fork handler", exc, "this child may not deliver")


def install() -> None:
    """Registers the child handler with ``os.register_at_fork``, once per process.

    Called from the package's ``__init__`` so registration happens at import of the package,
    and idempotent across repeated calls, which together are what make a double import register
    once (FR-006 AC-2 — see :data:`_installed` for the one case that is not covered).
    **Only** ``after_in_child`` is registered (FR-001 AC-1):
    ``before`` does not run for a C-level fork at all — uWSGI calls ``PyOS_AfterFork_Child``
    only — so the child handler has to be sufficient regardless, and a parent-side handler
    would buy a partial fix for a measured 1.20 s hold on the forking thread.

    A platform without ``os.register_at_fork`` — Windows — imports the package cleanly and
    registers nothing, which is what the guard is for. Nothing else in the library changes
    behaviour there, since only ``fork`` inherits the hazards this closes.

    Args:
      None.

    Returns:
      None.

    Raises:
      None.
    """
    global _installed
    if _installed or not hasattr(os, "register_at_fork"):
        return
    _installed = True
    os.register_at_fork(after_in_child=_reinit_after_fork)
