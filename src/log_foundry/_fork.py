"""Repairing the library's own synchronization state in a forked child (SPEC-039)."""

from __future__ import annotations

import os
import sys
import threading
import types
from typing import Any

from log_foundry import _diag

_PACKAGE = "log_foundry"

_RLOCK_TYPE = type(threading.RLock())

_LOCK_TYPES: tuple[type, ...] = (type(threading.Lock()), _RLOCK_TYPE)

_CONTAINER_TYPES: tuple[type, ...] = (list, tuple, set, frozenset, dict)

_installed = False
"""Whether :func:`install` has already registered the child handler in this process.

A flag and a function rather than a bare module-scope call, so that registration is one
statement a reader can find and repeated calls cannot stack handlers (FR-006 AC-2). What it
does **not** cover is stated rather than implied: ``importlib.reload`` re-runs this body and
resets the flag, so a deliberate reload registers a second handler. That is harmless — the
repair is idempotent and a child would simply run it twice — and closing it would mean
recording the registration somewhere outside this module, which is a worse trade than saying so.
"""


def _is_owned(value: object) -> bool:
    """Whether this object's type is defined by this package.

    The ownership test is what keeps the traversal off third-party state (FR-003 AC-2): a
    ``boto3`` session's locks, a ``librdkafka`` handle and a ``psycopg`` connection are not the
    library's to swap, and reaching into them would be a fork fix that breaks a driver.

    Args:
      value: Any object, including a class.

    Returns:
      Whether its type — or, for a class, the class itself — was defined in ``log_foundry``.

    Raises:
      None.
    """
    owner = value if isinstance(value, type) else type(value)
    module = getattr(owner, "__module__", "")
    return module == _PACKAGE or module.startswith(f"{_PACKAGE}.")


def _is_traversable(value: object) -> bool:
    """Whether the walk descends into this value.

    Three shapes and no others: a module of this package, an object this package's code
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
        name = getattr(value, "__name__", "")
        return name == _PACKAGE or name.startswith(f"{_PACKAGE}.")
    if type(value) in _CONTAINER_TYPES:
        return True
    return _is_owned(value)


def _container_children(container: Any) -> list[Any]:
    """Returns what a plain container holds, keys included for a mapping.

    Nothing is *replaced* inside a container: a primitive there would be unreachable in a
    tuple or a set, so a partial answer would read as coverage it does not have. The AST lint
    for FR-003 AC-3 forbids that shape outright instead, which is what makes descent-only
    correct here rather than merely convenient.

    Args:
      container: A list, tuple, set, frozenset or dict.

    Returns:
      Its members, or both its keys and its values for a mapping.

    Raises:
      None. A container mutating under the walk would raise, and a child that cannot finish
        repairing itself must still repair what it reached.
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
    would miss them — ``worker._FlushMarker`` is slotted and holds a ``threading.Event``.

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
        if type(holder) in _CONTAINER_TYPES:
            stack.extend(child for child in _container_children(holder) if _is_traversable(child))
            continue
        for name, value in _namespace_items(holder):
            fresh = _fresh_primitive(value, memo, keepalive)
            if fresh is not None:
                _assign(holder, name, fresh)
            elif _is_traversable(value):
                stack.append(value)


def _reinit_after_fork() -> None:
    """Repairs the library in a child that has just returned from ``fork``.

    **The order of work here is the contract** (FR-001 AC-2): locks and events first, because
    anything running afterwards may take one, and a lock re-initialised after a handler that
    takes it is a handler that hangs.

    Args:
      None.

    Returns:
      None.

    Raises:
      None. A fork handler that raises has its exception printed by CPython with a full
        traceback, carrying the message arch §6 keeps out of anything the library says about
        itself — and it would leave the rest of the repair undone.
    """
    try:
        _reinit_primitives()
    except Exception as exc:
        _diag.absorbed("repairing the library after a fork", exc, "this child may block or lose")


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
