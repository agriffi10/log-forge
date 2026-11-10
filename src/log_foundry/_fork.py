"""Repairing the library's own synchronization state in a forked child (SPEC-039)."""

from __future__ import annotations

import collections
import os
import sys
import threading
import types
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
root selection and the ownership test would miss every module and the handler would silently
repair nothing.
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

_COUNTERS_ATTRIBUTE = "log_foundry_counters"
"""The one attribute the walk reads on a value it would not otherwise enter (SPEC-072 FR-003)."""

_SCALAR_TYPES: tuple[type, ...] = (
    str,
    int,
    float,
    bool,
    bytes,
    type(None),
    types.FunctionType,
    types.BuiltinFunctionType,
    types.ModuleType,
)
"""Exact types that are never an engine sink, skipped before the static read; classes are too.

The counters reach is asked of every value the walk declines to push. For a buffering sink most
of those are caller data — the strings and numbers inside the events a ``MemorySink`` holds — and
in an idle process most are the package's own functions, classes and imported modules, so each
would otherwise pay a static read for an attribute that can never matter: none of
these types defines ``deliver``, so no engine run ever counts into counters one of them holds.
Matched through :data:`_SKIPPED_TYPE_IDS`, by identity, never by ``in`` over a set of the types
themselves, so a hostile metaclass ``__eq__`` or ``__hash__`` never runs here.
"""

_SKIPPED_TYPE_IDS = frozenset(id(kind) for kind in _SCALAR_TYPES)
"""The ids of :data:`_SCALAR_TYPES`; each is a builtin type alive for the process, so stable."""

_child_handlers: list[Callable[[], None]] = []
"""What runs in the child once its locks are its own again, in registration order.

**An inverted registry, and that is what keeps the dependency arrow pointing one way**
(FR-006). The work a child needs done belongs to ``_lifecycle`` and the sinks,
and all three would have to import this module for it to reach them; instead they register with
it, so this module imports nothing that imports it and there is no cycle to break later.
"""

_FORK_SKIP = ("reacquired_in_child",)
"""This module's own opt-out from the walk below (:data:`_SKIP_ATTRIBUTE`).

:data:`reacquired_in_child` holds **sinks**, so without this the next fork's walk enters the
previous fork's roster and re-collects every sink in it — including ones ``configure()``
superseded in between. Measured: a child's walk collected a sink two ``configure()`` calls out
of date, its hook was called, and the reclaim that follows then stamped it for the grandchild's
pid, so a sink no descendant ever acquired read ``releasable``. A ratchet, not a one-off: once a
hook-implementing sink enters a roster, every descendant's walk finds it again.

Exactly the hazard ``_lifecycle._state._owned`` already declares an opt-out for, reached by adding a
second global that holds sinks. The rebind below is the belt to this brace.
"""

reacquired_in_child: list[Any] = []
"""Sinks whose re-acquisition hook returned normally in this child (SPEC-042 FR-005).

Published rather than acted on, because what it means is an *ownership* claim and ownership is
``_lifecycle``'s: a sink that returns from the hook holds a transport of its own, so a child may
release it.

Rebound — never mutated in place — **before the repair walk runs**, so a second fork cannot
inherit the first one's roster even if the opt-out above were removed. The first version of this
claimed the rebind happened "at the start of every child repair" while doing it in the middle,
after the walk; both halves were false and nothing tested either.

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


def _is_traversable(value: object) -> bool:
    """Whether the walk descends into this value.

    Three shapes and no others: a module of this package, an object running code this package
    defines, and a plain container, which is traversed because ``MultiSink._sinks`` is one and
    the sinks inside it hold the locks the child's first log call takes.

    Args:
      value: Any object.

    Returns:
      Whether it is a namespace or container the walk should enter.

    Raises:
      None.
    """
    if _is_module(value):
        name = _MODULE_DICT(cast("types.ModuleType", value)).get("__name__")
        return type(name) is str and (name == _PACKAGE or name.startswith(f"{_PACKAGE}."))
    return _is_container(value) or _is_owned(value)


def _container_children(container: Any) -> list[Any]:
    """Returns what a plain container holds, keys included for a mapping.

    Nothing is *replaced* inside a container: a primitive there would be unreachable in a
    tuple or a set, so a partial answer would read as coverage it does not have. The AST lint
    for FR-003 AC-3 forbids that shape outright instead, which is what makes descent-only
    correct here rather than merely convenient.

    Args:
      container: Any value :func:`_is_container` accepted.

    Returns:
      Its members, or both its keys and its values for a mapping.

    Raises:
      None. A container mutating under the walk would raise, and a child that cannot finish
        repairing itself must still repair what it reached. A foreign container subclass
        reachable from an owned attribute runs its own code here — ``keys``/``values`` for a
        mapping, ``__iter__`` for anything else — which is absorbed if it raises, but a
        *blocking* one is a child that never returns from ``fork`` and there is nothing to catch
        that with.
    """
    try:
        if isinstance(container, dict):
            return [*container.keys(), *container.values()]
        return list(container)
    except Exception as exc:
        _diag.absorbed("reading a container after a fork", exc, "what it holds is not repaired")
        return []


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
            return list(_NAMESPACE(holder).items())
        if _is_module(holder):
            return list(dict.items(_MODULE_DICT(holder)))
        own = _instance_dict(holder)
        items: list[tuple[str, Any]] = [] if own is None else list(dict.items(own))
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

    Args:
      holder: Any object the walk has entered.

    Returns:
      Whether it offers the hook.

    Raises:
      None.
    """
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


def _counters_type() -> type | None:
    """Returns ``SinkCounters`` when its module is already loaded, without importing anything.

    Read from ``sys.modules`` once per walk: this runs in a child that has not yet returned from
    ``fork``, where an import can deadlock on an import lock a parent thread held at the fork, and
    ``tests/test_fork_lifecycle.py`` holds this module to importing nothing from the package but
    ``_diag`` and ``sinks.base``. A process that never loaded ``sinks.verdict`` holds no
    ``SinkCounters`` to repair, so ``None`` costs nothing.

    Args:
      None.

    Returns:
      The class, or ``None`` when the module is not loaded or holds no such class.

    Raises:
      None.
    """
    try:
        module = sys.modules.get(f"{_PACKAGE}.sinks.verdict")
        if not isinstance(module, types.ModuleType):
            return None
        found = vars(module).get("SinkCounters")
    except Exception:
        return None
    return found if isinstance(found, type) else None


def _counters_reach(value: object, kind: type | None) -> Any | None:
    """Returns a value's ``log_foundry_counters`` when it is exactly ``kind``, read statically.

    The walk enters only this package's own objects, and the sinks SPEC-072's engine counts for
    are typically third-party ones it never enters, so their counters' lock would stay dead in a
    child. This reads the one attribute :data:`_COUNTERS_ATTRIBUTE` with
    ``sinks.base._static_value`` — no property, ``__getattr__``, ``__getattribute__``,
    descriptor ``__get__`` or metaclass hook (``__mro__``, ``__name__``, ``__eq__``,
    ``__hash__``) of the third-party object runs, the one residue being the ``__eq__`` of a
    namespace or instance-dict key whose hash collides with the name (see ``_class_member``;
    ``getattr`` has it too) — resolving a
    ``__slots__`` member through its builtin descriptor, and hands back the counters only when
    they are exactly ``SinkCounters``,
    which is this package's own class and so is then walked as any owned holder is. Nothing else of
    the value's namespace is read, so the walk's boundary (SPEC-039 FR-005) is otherwise unchanged.

    Args:
      value: A value the walk would not push.
      kind: ``SinkCounters``, from :func:`_counters_type`, or ``None``.

    Returns:
      The counters, or ``None``.

    Raises:
      None.
    """
    if kind is None:
        return None
    probed = type(value)
    if id(probed) in _SKIPPED_TYPE_IDS or issubclass(probed, type):
        return None
    found = _static_value(value, _COUNTERS_ATTRIBUTE)
    return found if type(found) is kind else None


def _container_push(child: object, kind: type | None) -> Any | None:
    """Returns what the walk pushes for one member of a container it entered, or ``None``.

    The member itself when the walk descends into it, else its counters through
    :func:`_counters_reach`. This branch is the only repair for an engine sink held in a
    container — ``MultiSink._sinks`` is a tuple — which is why it is a function of its own: a
    test can take this branch away alone and watch that child hang.

    Args:
      child: A member of the container.
      kind: ``SinkCounters``, from :func:`_counters_type`, or ``None``.

    Returns:
      The value to push, or ``None``.

    Raises:
      None.
    """
    return child if _is_traversable(child) else _counters_reach(child, kind)


def _reinit_primitives() -> list[Any]:
    """Replaces every lock and event this package owns, wherever the walk reaches one.

    An inherited ``Lock`` stays locked with no owner — measured, ``acquire(timeout=1)`` returns
    ``False`` — so a child's first log call blocks forever on the application's own thread. A
    lock that was *not* held is replaced too: asking whether one is held has no answer that is
    not itself a race (FR-003 AC-6).

    Being a container and being a namespace are **not** exclusive: an owned class that
    subclasses one holds both members and attributes, and treating the two as alternatives
    silently drops whichever branch lost. The cost is proportional to what the library's own
    containers hold, which for a buffering sink is caller data — a ``MemorySink`` holding 100k
    events measured 202 ms, against 0.45 ms idle. That is accepted rather than bounded: a cap
    would be a lock this cannot promise to find, and the alternative to finding it is a hang.

    **A value the walk would not push is asked for one attribute** (SPEC-072 FR-003): its
    ``log_foundry_counters``, read statically by :func:`_counters_reach` and pushed when it is
    exactly a ``SinkCounters``, so a third-party engine sink's counters' lock is replaced like an
    owned one. ``SinkCounters`` is looked up in ``sys.modules`` once per walk, not per value, and
    a scalar is skipped before the static read, so the reach adds one type test per value the
    walk declines and one static read per non-scalar one. Its boundary is the one
    level it reads: an engine sink held *inside* a third-party wrapper, or a sink that is itself
    a third-party container subclass, is not reached.

    **The sinks carrying FR-004's buffer hook are collected on this same walk** rather than by
    a second one, because the traversal is the expensive part and its cost is proportional to
    caller data — paying 202 ms twice on a process that forks is a worse trade than one function
    reporting what it passed. Collected, not called: a hook may take a lock, and the contract is
    that every lock is the child's own before any of them runs (FR-001 AC-2).

    Args:
      None.

    Returns:
      The owned instances carrying :data:`_REACQUIRE_HOOK`, in the order the walk reached them.

    Raises:
      None.
    """
    memo: dict[int, Any] = {}
    keepalive: list[Any] = []
    seen: set[int] = set()
    buffered: list[Any] = []
    counters_type = _counters_type()
    stack: list[Any] = [
        module
        for name, module in list(sys.modules.items())
        if module is not None and (name == _PACKAGE or name.startswith(f"{_PACKAGE}."))
    ]
    while stack:
        holder = stack.pop()
        if id(holder) in seen:
            continue
        seen.add(id(holder))
        if _is_container(holder):
            for child in _container_children(holder):
                pushed = _container_push(child, counters_type)
                if pushed is not None:
                    stack.append(pushed)
        if not isinstance(holder, types.ModuleType | type):
            if not _is_owned(holder):
                continue
            if _offers_reacquire(holder):
                buffered.append(holder)
        skip = _skipped_names(holder)
        for name, value in _namespace_items(holder):
            if name in skip:
                continue
            fresh = _fresh_primitive(value, memo, keepalive)
            if fresh is not None:
                _assign(holder, name, fresh)
            elif _is_traversable(value):
                stack.append(value)
            else:
                counters = _counters_reach(value, counters_type)
                if counters is not None:
                    stack.append(counters)
    return buffered


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
      holders: What :func:`_reinit_primitives` collected on its walk.

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
                f"{type(holder).__name__} may write the parent's pending bytes again, and this "
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

    **The order of work here is the contract** (FR-001 AC-2): locks and events first, then the
    buffer discard, then the registered handlers. A lock re-initialised *after* a handler that
    takes it is a handler that hangs, and it hangs on the child's only thread with nothing to
    interrupt it. Work that must happen before *any* handler stays inline between the two steps
    rather than being registered — the discard is the case, and registering it would put it
    after ``_lifecycle``'s worker rebuild, which has started a live drain thread by then, emitting into
    the very sink whose buffer is still the parent's.

    Args:
      None.

    Returns:
      None.

    Raises:
      None. A fork handler that raises has its exception printed by CPython with a full
        traceback, carrying the message arch §6 keeps out of anything the library says about
        itself — and it would leave the rest of the repair undone. One handler's failure is
        absorbed separately from the rest for the same reason: a child that cannot rebuild its
        worker should still have working locks. The list is iterated live rather than copied,
        which is safe because CPython's list iterator is index-based, so a handler that
        registers another simply causes it to run — and :func:`register_child_handler` is the
        only writer, appending only.
    """
    global reacquired_in_child
    reacquired_in_child = []
    buffered: list[Any] = []
    try:
        buffered = _reinit_primitives()
    except Exception as exc:
        _diag.absorbed("repairing the library after a fork", exc, "this child may block or lose")
    reacquired_in_child = _reacquire_transports(buffered)
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
