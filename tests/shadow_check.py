"""Finds a library object whose instance dict shadows its class's method, the residue of a patch.

Restoring a method on an instance by assignment, ``obj.method = original`` where ``original``
was read as ``obj.method``, writes the *bound* method into the instance's ``__dict__``, where
nothing stood before. The instance then answers ``method`` from its own dict, so a later test that
patches the method on the *class* is silently bypassed for that object. ``MonkeyPatch``'s undo did
exactly this before pytest 9.2, which restores the instance dict instead (pytest #10644);
``test_fork_child_state.py`` patched ``_lifecycle._state._closes.arm`` that way under 9.1, and the
model soak's class-level wrapper of ``_CloseRegistry.arm`` then recorded no arm for the rest of the
process: four soak tests failed whenever they ran after that file in one process, and passed
under ``-n 12`` only because xdist rarely put the two files on one worker. A hand restore leaves
the same residue under any pytest, which is what the guard is still for.

The objects walked are derived, not listed: every global of every imported ``log_foundry`` module
that is an instance of a ``log_foundry`` class, and the ``log_foundry``-typed instances in those
objects' own dicts, one level down (which is how ``_state._closes`` is reached). It runs after
the conftest's reset has replaced the config and retired the worker, so a test's own sink is no
longer reachable and what is walked is state that outlives the test. A shadow is an
instance-dict key whose class-level value, found along the MRO, is a function, a staticmethod or a
classmethod. A data attribute is never reported, whatever its value: only a method-kind class
value can be shadowed in a way that hides a later class patch.

Two limits, both outside any shape the suite has today. The walk stops one level down and does not
enter containers, so a singleton nested deeper, or held in a dict, is not seen. And the check runs
at every test's teardown, so a module- or session-scoped fixture that patches a singleton's method
on the instance would read as a shadow while its patch is still live: the first test errors, and
the repair removes a patch later tests in that scope expected. Patch the class there too.
"""

from __future__ import annotations

import sys
import types
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

_PACKAGE = "log_foundry"
_METHODS = (types.FunctionType, staticmethod, classmethod)


def _owned(value: object) -> bool:
    """Whether ``value`` is an instance of a class defined in the library, not a class or module."""
    if isinstance(value, (type, types.ModuleType, types.FunctionType)):
        return False
    module = getattr(type(value), "__module__", "") or ""
    return module == _PACKAGE or module.startswith(_PACKAGE + ".")


def _instance_dict(value: object) -> dict[str, object] | None:
    """The object's own ``__dict__``, or ``None`` when it has none (a slotted class, say)."""
    try:
        return object.__getattribute__(value, "__dict__")
    except AttributeError:
        return None


def _roots() -> Iterator[tuple[str, object]]:
    """Yields every library-typed instance held by an imported library module, with its path."""
    for name, module in list(sys.modules.items()):
        if module is None or not (name == _PACKAGE or name.startswith(_PACKAGE + ".")):
            continue
        for key, value in list(vars(module).items()):
            if _owned(value):
                yield f"{name}.{key}", value


def _walked() -> Iterator[tuple[str, object]]:
    """Yields each root and the library-typed instances in its own dict, each object once."""
    seen: set[int] = set()
    for path, root in _roots():
        if id(root) in seen:
            continue
        seen.add(id(root))
        yield path, root
        own = _instance_dict(root) or {}
        for key, value in list(own.items()):
            if _owned(value) and id(value) not in seen:
                seen.add(id(value))
                yield f"{path}.{key}", value


def _class_value(cls: type, key: str) -> object:
    """The value ``key`` has on ``cls`` along its MRO, or ``None`` when no class defines it."""
    for klass in cls.__mro__:
        if key in vars(klass):
            return vars(klass)[key]
    return None


def shadows() -> list[tuple[str, object, str]]:
    """Every (path, object, name) whose instance dict holds a name its class defines as a method."""
    found: list[tuple[str, object, str]] = []
    for path, value in _walked():
        own = _instance_dict(value) or {}
        found.extend(
            (path, value, key)
            for key in list(own)
            if isinstance(_class_value(type(value), key), _METHODS)
        )
    return found


def check_and_repair() -> str | None:
    """Removes every shadow and returns the failure text naming them, or ``None`` when clean.

    The repair runs before the failure is reported, so one leaking test fails alone instead of
    reddening every later test that patches the same method on its class.
    """
    found = shadows()
    if not found:
        return None
    for _, value, key in found:
        _instance_dict(value).pop(key, None)  # type: ignore[union-attr]
    names = ", ".join(f"{path}.{key}" for path, _, key in found)
    return (
        f"this test left an instance attribute shadowing a class method on a library singleton: "
        f"{names}. A method assigned on an instance (a hand restore, a fake never undone, or a "
        f"monkeypatch undo before pytest 9.2) stays in its __dict__ and hides any later class "
        f"patch; patch the method on the class instead (see tests/shadow_check.py). Repaired."
    )
