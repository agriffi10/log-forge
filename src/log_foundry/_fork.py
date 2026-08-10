"""Repairing the library's own synchronization state in a forked child (SPEC-039)."""

from __future__ import annotations

import collections
import os
import sys
import threading
import types
from typing import TYPE_CHECKING, Any

from log_foundry import _diag

if TYPE_CHECKING:
    from collections.abc import Callable

_PACKAGE = __name__.rpartition(".")[0]
"""This package's import name, derived rather than written.

A literal is wrong under a vendored install — ``myapp._vendor.log_foundry`` — where both the
root selection and the ownership test would miss every module and the handler would silently
repair nothing.
"""

_RLOCK_TYPE = type(threading.RLock())

_LOCK_TYPES: tuple[type, ...] = (type(threading.Lock()), _RLOCK_TYPE)

_CONTAINER_TYPES: tuple[type, ...] = (list, tuple, set, frozenset, dict, collections.deque)

_child_handlers: list[Callable[[], None]] = []
"""What runs in the child once its locks are its own again, in registration order.

**An inverted registry, and that is what keeps the dependency arrow pointing one way**
(FR-006). The work a child needs done belongs to ``decorator``, ``_lifecycle`` and the sinks,
and all three would have to import this module for it to reach them; instead they register with
it, so this module imports nothing that imports it and there is no cycle to break later.
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

    Args:
      cls: The class to place.

    Returns:
      Whether its defining module is this package or a module inside it.

    Raises:
      None.
    """
    module = getattr(cls, "__module__", "") or ""
    return module == _PACKAGE or module.startswith(f"{_PACKAGE}.")


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

    Args:
      value: Any object, including a class.

    Returns:
      Whether it or any of its ancestors was defined in this package.

    Raises:
      None.
    """
    owner = value if isinstance(value, type) else type(value)
    return any(_defined_here(base) for base in getattr(owner, "__mro__", (owner,)))


def _is_container(value: object) -> bool:
    """Whether the walk reads this value's members as well as its attributes.

    ``isinstance`` rather than an exact-type test, so a ``deque``, a ``defaultdict`` or any
    other ordinary subclass is entered: an exact tuple made membership depend on which concrete
    class a future sink happened to hold its children in, which is the guess FR-003 AC-3 exists
    to replace.

    Args:
      value: Any object.

    Returns:
      Whether it holds members the walk should read.

    Raises:
      None.
    """
    return isinstance(value, _CONTAINER_TYPES)


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
    if isinstance(value, types.ModuleType):
        name = getattr(value, "__name__", "") or ""
        return name == _PACKAGE or name.startswith(f"{_PACKAGE}.")
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


def _slot_names(holder: object) -> list[str]:
    """Returns every ``__slots__`` name declared across a holder's class hierarchy.

    A slotted instance keeps its attributes off ``__dict__``, so a walk reading only ``vars()``
    would miss them. No shipped class needs this today and that is stated rather than dressed
    up: ``worker._FlushMarker`` is slotted and holds an ``Event``, but a marker lives inside a
    ``queue.Queue`` the walk never enters. It is here because the shape lint accepts a slotted
    ``self.<attr>``, so the walk has to be able to reach one.

    Args:
      holder: Any object.

    Returns:
      The declared slot names, which may be empty.

    Raises:
      None.
    """
    names: list[str] = []
    for cls in type(holder).__mro__:
        declared = cls.__dict__.get("__slots__", ())
        if isinstance(declared, str):
            names.append(declared)
        else:
            names.extend(str(name) for name in declared)
    return names


def _namespace_items(holder: Any) -> list[tuple[str, Any]]:
    """Returns the ``(name, value)`` pairs a holder owns, without triggering its properties.

    Values come from the instance ``__dict__`` and the slot descriptors rather than from a
    blanket ``getattr`` over ``dir()``, which would evaluate every property — including ones
    that open a connection or take the very lock this is about to replace.

    Args:
      holder: A module, a class, or an instance.

    Returns:
      One pair per attribute the holder itself carries.

    Raises:
      None. An attribute that cannot be read is skipped, since a repair that stops at the first
        awkward object leaves the rest of the process holding dead locks.
    """
    items: list[tuple[str, Any]] = []
    try:
        own = dict(vars(holder))
    except TypeError:
        own = {}
    except Exception as exc:
        _diag.absorbed("reading an object's attributes after a fork", exc, "it is not repaired")
        return []
    items.extend(own.items())
    if isinstance(holder, types.ModuleType | type):
        return items
    for name in _slot_names(holder):
        if name in own:
            continue
        value = _slot_value(holder, name)
        if value is not None:
            items.append((name, value))
    return items


def _slot_value(holder: object, name: str) -> Any | None:
    """Reads one slot, answering ``None`` for a slot that is unset or refuses to be read.

    Both answers are deliberately the same, because both mean "there is nothing here to
    replace": a lock is never ``None``, so nothing is lost by conflating them, and an object
    that raises on attribute access must not end the repair for the rest of the process.

    Args:
      holder: The instance to read from.
      name: The slot name.

    Returns:
      The value, or ``None``.

    Raises:
      None.
    """
    try:
        return getattr(holder, name, None)
    except Exception:
        return None


def _assign(holder: Any, name: str, value: Any) -> None:
    """Puts a fresh primitive where the dead one was.

    Instances are written through ``object.__setattr__`` so a frozen dataclass or a custom
    ``__setattr__`` cannot refuse the repair; modules and classes take the ordinary path,
    which is the only one they have.

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
        if isinstance(holder, types.ModuleType | type):
            setattr(holder, name, value)
        else:
            object.__setattr__(holder, name, value)
    except Exception as exc:
        _diag.absorbed(
            "re-initialising a lock after a fork",
            exc,
            f"{type(holder).__name__}.{name} may block the next caller forever",
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

    Returns:
      The replacement, or ``None`` when this value is not a lock or an event.

    Raises:
      None.
    """
    existing = memo.get(id(value))
    if existing is not None:
        return existing
    fresh: Any
    if isinstance(value, threading.Event):
        fresh = threading.Event()
        if value.is_set():
            fresh.set()
    elif isinstance(value, _LOCK_TYPES):
        fresh = threading.RLock() if isinstance(value, _RLOCK_TYPE) else threading.Lock()
    else:
        return None
    memo[id(value)] = fresh
    keepalive.append(value)
    return fresh


def _reinit_primitives() -> None:
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

    Args:
      None.

    Returns:
      None.

    Raises:
      None.
    """
    memo: dict[int, Any] = {}
    keepalive: list[Any] = []
    seen: set[int] = set()
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
            stack.extend(child for child in _container_children(holder) if _is_traversable(child))
        if not isinstance(holder, types.ModuleType | type) and not _is_owned(holder):
            continue
        for name, value in _namespace_items(holder):
            fresh = _fresh_primitive(value, memo, keepalive)
            if fresh is not None:
                _assign(holder, name, fresh)
            elif _is_traversable(value):
                stack.append(value)


def register_child_handler(fn: Callable[[], None]) -> None:
    """Adds work to be done in a forked child, after its locks have been re-initialised.

    A handler may take any of the library's locks, which is what the ordering in
    :func:`_reinit_after_fork` buys it and why registering is the only way in. What it must not
    assume is that it is alone: handlers run in registration order and an earlier one may have
    started a thread — ``decorator``'s rebuild does exactly that — so only the *first* runs in a
    genuinely single-threaded child. None of them may block, since nothing has returned from
    ``fork`` yet.

    Registering the same function twice is a no-op. The exposure is the one :data:`_installed`
    records for the fork registration itself: a reload of a module whose body registers here
    would otherwise stack a second handler, and for the worker rebuild that means two drain
    threads, the first bound to a queue nothing writes to.

    Args:
      fn: Called with no arguments in the child. A failure is absorbed and announced, and the
        remaining handlers still run.

    Returns:
      None.

    Raises:
      None.
    """
    if fn not in _child_handlers:
        _child_handlers.append(fn)


def _reinit_after_fork() -> None:
    """Repairs the library in a child that has just returned from ``fork``.

    **The order of work here is the contract** (FR-001 AC-2): locks and events first, then the
    registered handlers. A lock re-initialised *after* a handler that takes it is a handler
    that hangs, and it hangs on the child's only thread with nothing to interrupt it.

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
    try:
        _reinit_primitives()
    except Exception as exc:
        _diag.absorbed("repairing the library after a fork", exc, "this child may block or lose")
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
