"""A driver error raised as both ``SinkDeliveryError`` and the driver's own type (SPEC-069)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from log_foundry import _diag
from log_foundry.sinks.base import SinkDeliveryError

if TYPE_CHECKING:
    from collections.abc import Iterable

__all__ = ["delivery_error"]

_PATH_ATTRIBUTES = ("filename", "filename2")
"""``OSError`` attributes ``args`` does not carry, copied only when the original set them.

Copying an unset one corrupts ``str()``: ``filename2=None`` renders ``... 'f' -> None``, and a
plain ``OSError("plain")`` given all of them renders ``[Errno None] None: None -> None``.
"""

_DRIVER_ATTRIBUTES = ("sqlite_errorcode", "sqlite_errorname")
"""Attributes ``sqlite3`` stores in the instance's ``__dict__`` rather than in ``args``."""

_ATTRIBUTES = _PATH_ATTRIBUTES + _DRIVER_ATTRIBUTES
"""Every attribute copied beside ``args``, wherever the original stores it."""

_MEMBER = type(OSError.__dict__["filename"])
"""The type of a slot's member descriptor, ``OSError``'s ``filename`` or a ``__slots__`` entry's."""

_ARGS = BaseException.__dict__["args"].__get__
_SET_CAUSE = BaseException.__dict__["__cause__"].__set__
_INSTANCE_DICT = BaseException.__dict__["__dict__"].__get__
_STORED_QUALNAME = type.__dict__["__qualname__"].__get__
_STORED_MODULE = type.__dict__["__module__"].__get__
_CLASS_DICT = type.__dict__["__dict__"].__get__
_STORED_MRO = type.__dict__["__mro__"].__get__
_FLAGS = type.__dict__["__flags__"].__get__
"""``BaseException``'s and ``type``'s own descriptors, unbound: how this module reads and writes.

**Every read of the driver's class or instance goes through one of these, through a slot's own
member descriptor (:func:`_slots`), or through a walk of a stored ``dict``'s entries that compares
only exact ``str`` keys** (:func:`_entries`). None of them is an attribute lookup, so a
metaclass property, a class ``__getattribute__``, ``__setattr__`` or data descriptor, or an
instance ``__dict__`` subclass is never consulted; none of them hashes or compares anything the
driver supplied, so a colliding key's ``__eq__`` and a metaclass ``__hash__`` never run. The
stored values copied onto the derived class are exact ``str`` or ``None``, so ``type()`` finds no
``__set_name__`` on them and a later traceback line compares no ``str`` subclass.

This replaced reading through attribute lookup one site at a time, after three review rounds
running each found another way in (``docs/architecture.md`` §12, resolved): the cache key's
``__hash__``, a ``__cause__`` setter and ``__setattr__``, an ``args`` property, attribute
properties, a stored ``__module__`` carrying ``__set_name__``, and ``type``'s ``__doc__``
descriptor, which calls a descriptor stored as the class's ``__doc__``. ``tests/
test_driver_error_corpus.py`` holds each of them, and their neighbours, as a hostile fixture.
"""

_HEAPTYPE = 1 << 9
"""``Py_TPFLAGS_HEAPTYPE``: a class built at run time, keeping ``__module__`` in its namespace."""


def delivery_error(err: Exception) -> Exception | None:
    """Builds the exception a sink raises for its own driver's error, typed as both.

    ``SQLiteSink`` and the two file sinks let ``sqlite3.Error`` and ``OSError`` escape, so
    callers wrote ``except sqlite3.Error`` and ``except OSError`` around them; the library's own
    word for "this sink delivered none of the batch" is ``SinkDeliveryError``. The instance built
    here is both, so every one of those ``except`` clauses still matches (SPEC-069 FR-004). The
    file sinks build it for whatever ``Exception`` their handle raised, a ``ValueError`` as much
    as an ``OSError``, and hand it to the delivery engine to raise from an ``emit`` whose run
    landed nothing (``RunContext.raise_on_failure``, SPEC-082).

    It is an instance of a class derived at run time from ``SinkDeliveryError`` and the
    original's own class, one per original class (:func:`_dual`). The class keeps the name the
    original stores, so a line naming the exception's type — the worker's abandon line — reads
    as it did. The instance is made from the ``args`` the original stores and given only the
    attributes the original actually stored and ``args`` does not carry, written where the
    original stored them, so ``str()``, ``args``, ``errno``, ``filename`` and ``sqlite_errorcode``
    equal the original's, and the original is its ``__cause__``.

    **Two zones.** Deriving from the original and calling the derived class run the driver's own
    code by Python's design — a metaclass, ``__init_subclass__``, ``__new__`` and ``__init__``,
    and the lookups subclassing makes in the original's namespace — so that work, and only that
    work, sits inside one guard, and a class that refuses there is raised unchanged. Everything
    else reads and writes through the descriptors at :data:`_ARGS`, which run none of the
    driver's code, so no attribute the original carries can stop the build and the cause is set
    outside the guard because it cannot raise.

    **This is dynamic inheritance**, which the Python rulebook asks us to avoid (§15), and the
    per-class cache is module state (§13). Both are the spec's mechanism: a static subclass per
    driver type cannot cover a type the driver adds, and the class is the only thing that makes
    an ``isinstance`` against both bases true. The class is in no ``__all__`` and has no
    documented import path — ``isinstance(exc, SinkDeliveryError)`` is how a caller asks.

    Args:
      err: The driver's exception, as the sink caught it.

    Returns:
      The dual-typed exception, or ``None`` when the original's class refuses it: a constructor
      that rejects its own ``args`` or returns anything but an instance of exactly the derived
      class, a class that cannot share a layout with ``SinkDeliveryError``, an
      ``__init_subclass__``, metaclass or namespace that refuses the derived class, or one that
      leaves it, through a metaclass ``mro`` or a rewrite of its bases, not deriving from both.
      The sink then raises the original unchanged, because the library never fails the caller on
      its own fault (invariant 1).

    Raises:
      None of its own. A ``BaseException`` that is not an ``Exception``, such as a
      ``KeyboardInterrupt`` the driver's constructor raises inside the guard, propagates
      unchanged, because no guard in the library catches one (invariant 1).
    """
    original = type(err)
    try:
        derived = _dual(original)
        if not _derives(derived, original):
            return None
        built = derived(*_ARGS(err))
        if type(built) is not derived:
            return None
        _write(built, _stored(err))
    except Exception:
        return None
    _SET_CAUSE(built, err)
    return built


_CLASSES: dict[int, tuple[type, type[Exception]]] = {}
"""The original and its derived class, by the original's ``id``, built on first use.

Keyed by ``id`` rather than by the class, because a class key is hashed and compared through its
metaclass. The entry holds the original itself, so the id cannot be reused while the entry stands.
Nothing else is trusted to: the derived class's bases and its ``__reduce__`` both held the original
too, but an ``__init_subclass__`` can rewrite the one and delete the other, after which the
original was collected and an unrelated class built at the same address was handed the derived
class of the first, an instance its own ``except`` misses. Module state, bounded by the number of
distinct driver exception classes a process meets. Two threads deriving one class at once both
build it and ``setdefault`` keeps the first, so every instance of one original class shares one
derived class.
"""


def _dual(original: type[Exception]) -> type[Exception]:
    """Derives, once per original class, the class that is both it and ``SinkDeliveryError``.

    Its name and qualified name are the ones the original stores, and so are its ``__module__``
    and ``__doc__`` when the original stores an exact or subclassed ``str`` there
    (:func:`_namespace_strings`), so the class reads as the driver's in a traceback as well as in
    a line. Each is copied as an exact ``str``. A ``__module__`` that is not a ``str`` is left out,
    and the class takes this module's, so a traceback line still has a module to name.

    Args:
      original: The driver exception's class.

    Returns:
      The derived class.

    Raises:
      Exception: Whatever ``type()`` raises while the original's metaclass, its
        ``__init_subclass__`` or a lookup in its namespace refuses the derived class, or a
        ``TypeError`` if the two bases cannot share a layout; :func:`delivery_error` absorbs it.
    """
    found = _CLASSES.get(id(original))
    if found is not None:
        return found[1]
    module, doc = _namespace_strings(original)
    namespace: dict[str, Any] = {
        "__qualname__": _diag._STR_STR(_STORED_QUALNAME(original)),
        "__doc__": doc,
        "__reduce__": _reducer(original),
    }
    if module is not None:
        namespace["__module__"] = module
    name = _diag._STR_STR(_diag._STORED_NAME(original))
    built = cast("type[Exception]", type(name, (SinkDeliveryError, original), namespace))
    return _CLASSES.setdefault(id(original), (original, built))[1]


def _derives(derived: type, original: type) -> bool:
    """Reports whether ``derived`` really is both ``original`` and ``SinkDeliveryError``.

    ``type()`` returns whatever the original's metaclass makes, a metaclass ``mro`` decides what
    an ``except`` clause matches, and an ``__init_subclass__`` or a caller holding the class can
    rewrite its bases afterwards, so the check reads the MRO the class stores, which is what an
    ``except`` reads, and compares by identity, which runs nothing. It runs on every call, cached
    class or not, because a rewrite can come after the class was cached.

    Args:
      derived: The class :func:`_dual` returned.
      original: The driver exception's class.

    Returns:
      Whether both appear in ``derived``'s stored MRO.

    Raises:
      None.
    """
    mro = _STORED_MRO(derived)
    return any(base is original for base in mro) and any(base is SinkDeliveryError for base in mro)


def _namespace_strings(original: type) -> tuple[str | None, str | None]:
    """Returns the ``__module__`` and ``__doc__`` a class stores, each an exact ``str`` or ``None``.

    Both are found by walking the class's own namespace (:func:`_entries`) rather than through
    ``type``'s descriptors, which look the name up in it: a lookup compares every key hashing as
    the name, so one planted with a raising ``__eq__`` ran, and ``type``'s ``__doc__`` calls the
    ``__get__`` of a descriptor stored as ``__doc__``. A class built in C stores no
    ``__module__`` and ``type`` derives it from the class's C name, which runs nothing, so that
    one is read through ``type``'s descriptor.

    Args:
      original: The driver exception's class.

    Returns:
      Its ``__module__`` and ``__doc__``, each copied with ``_diag._STR_STR`` when stored as a
      ``str``, and otherwise ``None``.

    Raises:
      None.
    """
    stored = _entries(_CLASS_DICT(original).items(), ("__module__", "__doc__"))
    module = stored.get("__module__")
    if "__module__" not in stored and not _FLAGS(original) & _HEAPTYPE:
        module = _STORED_MODULE(original)
    return _exact_str(module), _exact_str(stored.get("__doc__"))


def _exact_str(value: object) -> str | None:
    """Returns ``value`` as an exact ``str`` when it is one or a subclass, otherwise ``None``.

    ``issubclass`` against ``str`` reads the type's stored MRO, so it runs nothing the value's
    class defines, where ``isinstance`` falls back to the value's own ``__class__``.

    Args:
      value: A value the driver's class stores.

    Returns:
      The text, as an exact ``str``, or ``None``.

    Raises:
      None.
    """
    return _diag._STR_STR(value) if issubclass(type(value), str) else None


def _entries(pairs: Iterable[tuple[object, object]], names: tuple[str, ...]) -> dict[str, object]:
    """Returns the entries of a stored mapping whose key is exactly one of ``names``.

    It walks the entries rather than looking each name up, and keeps a key only when its type is
    exactly ``str`` and it equals one of ``names``, which are exact too, so no key the driver
    supplied is hashed or compared through its own code. An instance's ``__dict__`` is walked
    by ``dict.items`` itself, so a ``dict`` subclass's ``items`` and ``__iter__`` are never
    called; a class namespace arrives through its read-only proxy, whose ``items`` calls the
    namespace's own, an exact ``dict``.

    Args:
      pairs: The mapping's entries, as ``dict.items`` or the namespace proxy's ``items`` gives them.
      names: The keys wanted, each an exact ``str``.

    Returns:
      Each matching entry, by name.

    Raises:
      None.
    """
    return {key: value for key, value in pairs if type(key) is str and key in names}


def _slots(cls: type) -> dict[str, Any]:
    """Returns the member descriptor each copied attribute is stored in on ``cls``, if any.

    It walks the namespaces of the MRO ``cls`` stores (:func:`_entries`), nearest first, and keeps
    the first value under each name whose type is exactly a member descriptor: ``OSError``'s two
    path slots, or a ``__slots__`` entry. A property or plain value under the name above it is
    passed over, because a member descriptor is where the instance stores the value, and a
    property is the class's view of it, which would run the class's code.

    Args:
      cls: The class whose instances are read or written.

    Returns:
      Each name's member descriptor, by name.

    Raises:
      None.
    """
    found: dict[str, Any] = {}
    for klass in _STORED_MRO(cls):
        for name, value in _entries(_CLASS_DICT(klass).items(), _ATTRIBUTES).items():
            if name not in found and type(value) is _MEMBER:
                found[name] = value
    return found


def _stored(err: BaseException) -> dict[str, object]:
    """The attributes ``err`` stores that ``args`` does not carry and the original actually set.

    An attribute stored in a slot is read through that slot's member descriptor (:func:`_slots`);
    every other one lives in the instance's ``__dict__`` and is read from there by
    :func:`_entries`. A slot that was never set is skipped, as is one whose descriptor belongs to
    a class ``err`` is not, which a hostile class can store under the name; both raise from C,
    running nothing of the driver's. A path attribute is copied only when it is not ``None``.

    Args:
      err: The exception to read.

    Returns:
      Each attribute to copy, by name.

    Raises:
      None.
    """
    slots = _slots(type(err))
    in_dict = _entries(dict.items(_INSTANCE_DICT(err)), _ATTRIBUTES)
    found: dict[str, object] = {}
    for name in _ATTRIBUTES:
        if name in slots:
            try:
                value = slots[name].__get__(err)
            except (AttributeError, TypeError):
                continue
        elif name in in_dict:
            value = in_dict[name]
        else:
            continue
        if value is not None or name not in _PATH_ATTRIBUTES:
            found[name] = value
    return found


def _write(target: BaseException, attributes: dict[str, object]) -> None:
    """Writes each attribute where :func:`_stored` would read it: its slot, or the ``__dict__``.

    Args:
      target: The exception to write to.
      attributes: What :func:`_stored` returned.

    Returns:
      None.

    Raises:
      Exception: Whatever a key the constructor planted in ``target``'s ``__dict__`` raises when
        the insert compares it, or a slot's descriptor raises for a value it refuses;
        :func:`delivery_error` absorbs it.
    """
    slots = _slots(type(target))
    for name, value in attributes.items():
        if name in slots:
            slots[name].__set__(target, value)
        else:
            dict.__setitem__(_INSTANCE_DICT(target), name, value)


def _reducer(original: type[Exception]) -> Any:
    """Returns the ``__reduce__`` of the class derived from ``original``, which names it directly.

    The derived class is built at run time and is not importable under its name, so default
    pickling fails for every instance; rebuilding the original class from its ``args`` and the
    copied attributes is what a process on the far side of a pickle can reconstruct. The
    original is closed over here rather than read back from the class's bases, so a subclass of
    the derived class pickles as the original too and nothing about the class is read.

    Args:
      original: The driver exception's class.

    Returns:
      The ``__reduce__`` function for the derived class.

    Raises:
      None.
    """

    def reduce(self: BaseException) -> tuple[Any, ...]:
        """Pickles a dual-typed exception as the driver's own, with no ``SinkDeliveryError`` base.

        It reads ``args`` and the copied attributes again through the stored values, as
        :func:`delivery_error` did, so no property or ``__getattribute__`` on the original runs.

        Args:
          self: The dual-typed exception.

        Returns:
          The ``__reduce__`` tuple: :func:`_rebuild` and its arguments.

        Raises:
          None.
        """
        return _rebuild, (original, _ARGS(self), _stored(self))

    return reduce


def _rebuild(
    original: type[BaseException], args: tuple[object, ...], attrs: dict[str, object]
) -> BaseException:
    """Reconstructs the driver's own exception from what the derived ``__reduce__`` recorded.

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
    _write(err, attrs)
    return err
