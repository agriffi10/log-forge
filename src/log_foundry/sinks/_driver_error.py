"""A driver error raised as both ``SinkDeliveryError`` and the driver's own type (SPEC-069)."""

from __future__ import annotations

from typing import Any, cast

from log_foundry.sinks.base import SinkDeliveryError

__all__ = ["delivery_error"]

_PATH_ATTRIBUTES = ("filename", "filename2")
"""``OSError`` attributes ``args`` does not carry, copied only when the original set them.

Copying an unset one corrupts ``str()``: ``filename2=None`` renders ``... 'f' -> None``, and a
plain ``OSError("plain")`` given all of them renders ``[Errno None] None: None -> None``.
"""

_DRIVER_ATTRIBUTES = ("sqlite_errorcode", "sqlite_errorname")
"""Attributes ``sqlite3`` stores on the instance rather than in ``args``, copied when present."""


def delivery_error(err: Exception) -> Exception | None:
    """Builds the exception a sink raises for its own driver's error, typed as both.

    ``SQLiteSink`` and the two file sinks let ``sqlite3.Error`` and ``OSError`` escape, so
    callers wrote ``except sqlite3.Error`` and ``except OSError`` around them; the library's own
    word for "this sink delivered none of the batch" is ``SinkDeliveryError``. The instance built
    here is both, so every one of those ``except`` clauses still matches (SPEC-069 FR-004).

    It is an instance of a class derived at run time from ``SinkDeliveryError`` and the
    original's own class, one per original class (:func:`_dual`). The class keeps the original's
    ``__name__``, so a line naming the exception's type — the worker's abandon line — reads as it
    did. The instance is made from the original's ``args`` and given only the attributes the
    original actually set and ``args`` does not carry, so ``str()``, ``args``, ``errno``,
    ``filename`` and ``sqlite_errorcode`` equal the original's, and the original is its
    ``__cause__``.

    **This is dynamic inheritance**, which the Python rulebook asks us to avoid (§15), and the
    per-class cache is module state (§13). Both are the spec's mechanism: a static subclass per
    driver type cannot cover a type the driver adds, and the class is the only thing that makes
    an ``isinstance`` against both bases true. The class is in no ``__all__`` and has no
    documented import path — ``isinstance(exc, SinkDeliveryError)`` is how a caller asks.

    Args:
      err: The driver's exception, as the sink caught it.

    Returns:
      The dual-typed exception, or ``None`` when it cannot be built — an exception class whose
      constructor rejects its own ``args``, or one that cannot share a layout with
      ``SinkDeliveryError``. The sink then raises the original unchanged, because the library
      never fails the caller on its own fault (invariant 1).

    Raises:
      None.
    """
    try:
        built = _dual(type(err))(*err.args)
        for name, value in _set_attributes(err).items():
            setattr(built, name, value)
    except Exception:
        return None
    built.__cause__ = err
    return built


_CLASSES: dict[type, type[Exception]] = {}
"""The derived class for each original class, built on first use and reused after.

Module state, bounded by the number of distinct driver exception classes a process meets. Two
threads deriving one class at once both build it and ``setdefault`` keeps the first, so every
instance of one original class shares one derived class.
"""


def _dual(original: type[Exception]) -> type[Exception]:
    """Derives, once per original class, the class that is both it and ``SinkDeliveryError``.

    ``SinkDeliveryError`` comes first in the bases, so it is ``__bases__[1]`` that names the
    original, which :func:`_reduce` reads. ``__module__`` and ``__qualname__`` are the
    original's, so the class reads as the driver's in a traceback as well as in a line.

    Args:
      original: The driver exception's class.

    Returns:
      The derived class.

    Raises:
      TypeError: If the two bases cannot be combined; :func:`delivery_error` absorbs it.
    """
    found = _CLASSES.get(original)
    if found is not None:
        return found
    namespace: dict[str, Any] = {
        "__module__": original.__module__,
        "__qualname__": original.__qualname__,
        "__doc__": original.__doc__,
        "__reduce__": _reduce,
    }
    built = cast(
        "type[Exception]", type(original.__name__, (SinkDeliveryError, original), namespace)
    )
    return _CLASSES.setdefault(original, built)


def _set_attributes(err: BaseException) -> dict[str, object]:
    """The attributes of ``err`` that ``args`` does not carry and the original actually set.

    Args:
      err: The exception to read.

    Returns:
      Each attribute to copy, by name.

    Raises:
      None.
    """
    found: dict[str, object] = {}
    for name in _PATH_ATTRIBUTES:
        value = getattr(err, name, None)
        if value is not None:
            found[name] = value
    for name in _DRIVER_ATTRIBUTES:
        if hasattr(err, name):
            found[name] = getattr(err, name)
    return found


def _reduce(self: BaseException) -> tuple[Any, ...]:
    """Pickles a dual-typed exception as the driver's own, without the ``SinkDeliveryError`` base.

    The derived class is built at run time and is not importable under its name, so default
    pickling fails for every instance; rebuilding the original class from its ``args`` and the
    copied attributes is what a process on the far side of a pickle can reconstruct.

    Args:
      self: The dual-typed exception.

    Returns:
      The ``__reduce__`` tuple: :func:`_rebuild` and its arguments.

    Raises:
      None.
    """
    return _rebuild, (type(self).__bases__[1], self.args, _set_attributes(self))


def _rebuild(
    original: type[BaseException], args: tuple[object, ...], attrs: dict[str, object]
) -> BaseException:
    """Reconstructs the driver's own exception from what :func:`_reduce` recorded.

    Args:
      original: The driver exception's class.
      args: Its ``args``.
      attrs: The attributes copied beside them.

    Returns:
      An instance of ``original`` with the same ``args`` and attributes.

    Raises:
      Exception: Whatever ``original``'s constructor raises for those ``args``.
    """
    err = original(*args)
    for name, value in attrs.items():
        setattr(err, name, value)
    return err
