"""SPEC-079 FR-002: every read the fork walk and the stamp walk make on an object runs none of its code.

The walk's predicates used to be ``isinstance``, ``getattr`` and ``vars``. Probed through
``configure(sink=h)`` at ``44db79a6``, a hostile sink's ``__class__`` getter and
``__getattribute__`` each ran twice, and its metaclass ``__hash__`` twice and ``__eq__`` once, all
from ``stamp``'s walk. The static predicates read the real type's MRO and namespaces through
``sinks.base``'s unbound accessors instead. Two kinds of test hold them:

- **A differential oracle** keeps each static predicate equal to the ``isinstance`` form it
  replaced over honest shapes, with every divergence named in the case and its reason, so a
  static predicate that drifts from what Python resolves fails by case rather than silently.
- **A hostile probe** counts every hook of a sink value, its class and its metaclass across
  ``stamp``, ``_reachable_sinks`` and a real forked child's repair, and requires zero.

Serves invariants 1 and 12.
"""

from __future__ import annotations

import collections
import types
import weakref
from typing import Any, ClassVar

import pytest

import log_foundry
from log_foundry import _fork, _lifecycle
from log_foundry.sinks.base import Sink
from log_foundry.sinks.file import FileSink
from log_foundry.sinks.filtering import FilteringSink
from log_foundry.sinks.memory import MemorySink
from log_foundry.sinks.multi import MultiSink
from test_fork_lifecycle import run_in_child


class Ran:
    """The shared record every hook in this file writes to while armed."""

    armed: ClassVar[bool] = False
    calls: ClassVar[collections.Counter[str]] = collections.Counter()


def _note(what: str) -> None:
    if Ran.armed:
        Ran.calls[what] += 1


class Meta(type):
    """A metaclass whose identity and name hooks record."""

    def __eq__(cls, other: object) -> bool:
        _note("meta __eq__")
        return cls is other

    def __hash__(cls) -> int:
        _note("meta __hash__")
        return id(cls)

    @property
    def __mro__(cls) -> tuple[type, ...]:  # type: ignore[override]
        _note("meta __mro__")
        return type.__dict__["__mro__"].__get__(cls)  # type: ignore[no-any-return]

    def __getattribute__(cls, name: str) -> Any:
        if name not in {"__dict__", "__name__", "__qualname__", "__module__"}:
            _note(f"meta __getattribute__ {name}")
        return type.__getattribute__(cls, name)


class Hostile(metaclass=Meta):
    """A sink every one of whose hooks records."""

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Accepts the batch."""

    def close(self) -> None:
        """Closes nothing."""

    @property  # type: ignore[misc]
    def __class__(self) -> type:  # type: ignore[override]
        _note("__class__ getter")
        return Hostile

    def __getattr__(self, name: str) -> Any:
        _note(f"__getattr__ {name}")
        raise AttributeError(name)

    def __getattribute__(self, name: str) -> Any:
        _note(f"__getattribute__ {name}")
        return object.__getattribute__(self, name)


class Plain:
    """A third-party object with no sink members."""


class Structural:
    """A sink in README.md's documented shape, subclassing nothing."""

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Accepts the batch."""

    def close(self) -> None:
        """Closes nothing."""


class Nominal(Sink):
    """A sink that subclasses the protocol."""

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Accepts the batch."""

    def close(self) -> None:
        """Closes nothing."""


class Slotted:
    """A slotted sink."""

    __slots__ = ("value",)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Accepts the batch."""

    def close(self) -> None:
        """Closes nothing."""


class EmitNone:
    """An object whose ``emit`` is ``None`` until something binds it."""

    emit = None

    def close(self) -> None:
        """Closes nothing."""


class Proxy:
    """A ``__getattr__`` proxy over a full sink."""

    def __init__(self, inner: object) -> None:
        object.__setattr__(self, "_inner", inner)

    def __getattr__(self, name: str) -> Any:
        return getattr(object.__getattribute__(self, "_inner"), name)


class Liar:
    """A non-sink whose ``__class__`` claims to be a sink class."""

    @property  # type: ignore[misc]
    def __class__(self) -> type:  # type: ignore[override]
        return Nominal


def _bound_later() -> object:
    sink = types.SimpleNamespace()
    sink.emit = lambda batch: None
    sink.close = lambda: None
    return sink


_SINK_CASES: dict[str, tuple[Any, str]] = {
    "shipped": (MemorySink(), ""),
    "structural": (Structural(), ""),
    "nominal": (Nominal(), ""),
    "slotted": (Slotted(), ""),
    "emit-none": (EmitNone(), ""),
    "proxy": (Proxy(MemorySink()), ""),
    "namespace-bound-later": (_bound_later(), ""),
    "plain": (Plain(), ""),
    "dict": ({}, ""),
    "multisink": (MultiSink(MemorySink()), ""),
    "weakref-proxy": (
        None,
        "a weakref proxy forwards `__class__` and its members in C, which no static read follows",
    ),
    "a sink class": (
        MemorySink,
        "a class carrying `emit` and `close` is not an instance the library could close",
    ),
    "lying __class__": (
        Liar(),
        "`isinstance` trusts the value's `__class__` property, which is the hook not run",
    ),
}

_KEEP = MemorySink()


@pytest.mark.parametrize("case", list(_SINK_CASES))
def test_the_static_sink_shape_agrees_with_isinstance(case: str) -> None:
    """Each case is `isinstance(value, Sink)` unless its reason says why the static test differs."""
    value, divergence = _SINK_CASES[case]
    if case == "weakref-proxy":
        value = weakref.proxy(_KEEP)
    expected = isinstance(value, Sink)
    if divergence:
        expected = not expected
    assert _fork._sink_shaped(value) is expected, divergence or "agrees with isinstance"


class _ListChild(list):  # type: ignore[type-arg]
    pass


class _DictChild(dict):  # type: ignore[type-arg]
    pass


_HONEST = [
    [],
    (),
    set(),
    frozenset(),
    {},
    collections.deque(),
    collections.OrderedDict(),
    collections.defaultdict(list),
    _ListChild(),
    _DictChild(),
    "s",
    b"b",
    3,
    None,
    object(),
    MemorySink(),
    MemorySink,
    MultiSink,
    Plain,
    Plain(),
    _fork,
    type,
    Meta,
]


@pytest.mark.parametrize("value", _HONEST, ids=lambda v: type(v).__name__)
def test_the_static_type_tests_agree_with_isinstance(value: object) -> None:
    """On every honest shape, the MRO identity tests answer what `isinstance` answers."""
    assert _fork._is_container(value) is isinstance(value, _fork._CONTAINER_TYPES)
    assert _fork._is_class(value) is isinstance(value, type)
    assert _fork._is_module(value) is isinstance(value, types.ModuleType)
    owner = value if isinstance(value, type) else type(value)
    by_getattr = any(
        (getattr(base, "__module__", "") or "").startswith(_fork._PACKAGE) for base in owner.__mro__
    )
    assert _fork._is_owned(value) is by_getattr


def _hostile_graph() -> tuple[Hostile, MultiSink]:
    """The hostile sink held two ways: a ``MultiSink`` child, and a ``FilteringSink``'s attribute.

    The attribute is what puts it in front of the repair's primitive test, which reads every
    value in a package holder's own namespace.
    """
    hostile = Hostile()
    return hostile, MultiSink(MemorySink(), hostile, FilteringSink(hostile))


def test_stamp_and_the_reachable_walk_run_no_hook_of_a_sink() -> None:
    """`configure()` and the stamp walk read the hostile sink statically: zero hook calls."""
    hostile, wrapper = _hostile_graph()
    Ran.calls.clear()
    Ran.armed = True
    try:
        found = _lifecycle._reachable_sinks(wrapper)
        _lifecycle.stamp(hostile)
        log_foundry.configure(service="static", sink=hostile)
    finally:
        Ran.armed = False
    assert dict(Ran.calls) == {}, "a hook of the sink, its class or its metaclass ran"
    assert any(sink is hostile for sink in found), "the walk still records the hostile sink"


def test_a_forked_child_repairs_without_running_a_hook_of_a_sink() -> None:
    """The child's repair reaches the hostile sink as a configured root and runs none of it."""
    hostile, wrapper = _hostile_graph()
    log_foundry.configure(service="static", sink=wrapper)
    Ran.calls.clear()
    Ran.armed = True
    try:
        child = run_in_child(lambda: repr(sorted(Ran.calls.items())))
    finally:
        Ran.armed = False
    assert child.finished, child.output
    assert child.output == "[]", f"the child's repair ran a hook: {child.output}"
    assert hostile is not None


def test_a_subclass_iterator_is_never_entered() -> None:
    """A `list` subclass's own non-terminating `__iter__` is never called, and its storage is read."""
    entered: list[int] = []
    first, second = object(), object()

    class Endless(list):  # type: ignore[type-arg]
        def __iter__(self):  # type: ignore[no-untyped-def]
            entered.append(1)
            while True:
                yield object()

    class Lying(dict):  # type: ignore[type-arg]
        def keys(self):  # type: ignore[no-untyped-def]
            entered.append(2)
            return iter(())

        def values(self):  # type: ignore[no-untyped-def]
            entered.append(3)
            return iter(())

    assert _lifecycle._bounded_children(Endless([first, second])) == [first, second]
    assert _lifecycle._bounded_children(Lying({first: second})) == [first, second]
    assert entered == [], "a subclass's own iteration ran"


def test_the_slot_and_skip_reads_run_no_hook() -> None:
    """A slotted holder's slot is read through its member, and `_FORK_SKIP` statically."""

    class Owned:
        __slots__ = ("_FORK_SKIP", "lock")

        def __getattribute__(self, name: str) -> Any:
            _note(f"slotted __getattribute__ {name}")
            return object.__getattribute__(self, name)

    holder = Owned()
    sentinel = object()
    object.__setattr__(holder, "lock", sentinel)
    object.__setattr__(holder, "_FORK_SKIP", ("lock",))
    Ran.calls.clear()
    Ran.armed = True
    try:
        items = dict(_fork._namespace_items(holder))
        skipped = _fork._skipped_names(holder)
        hooked = _fork._offers_reacquire(holder)
    finally:
        Ran.armed = False
    assert items["lock"] is sentinel
    assert skipped == frozenset({"lock"})
    assert hooked is False
    assert dict(Ran.calls) == {}


def test_a_file_sink_still_offers_its_hook() -> None:
    """The static hook read still finds a shipped sink's `reacquire_after_fork`."""
    assert _fork._offers_reacquire(FileSink.__new__(FileSink)) is True
    assert _fork._offers_reacquire(MemorySink()) is False


class _HostileKey(str):
    """A ``str`` subclass key whose ``__hash__`` and ``__eq__`` record while armed."""

    def __hash__(self) -> int:
        _note("key __hash__")
        return str.__hash__(self)

    def __eq__(self, other: object) -> bool:
        _note("key __eq__")
        return str.__eq__(self, other)


def test_a_str_subclass_key_in_an_instance_dict_runs_nothing_at_fork() -> None:
    """A planted non-``str`` key is dropped by type before anything hashes or compares it."""
    holder = FilteringSink(MemorySink())
    vars(holder)[_HostileKey("planted")] = MemorySink()
    log_foundry.configure(service="static", sink=holder)
    Ran.calls.clear()
    Ran.armed = True
    try:
        items = _fork._namespace_items(holder)
        child = run_in_child(lambda: repr(sorted(Ran.calls.items())))
    finally:
        Ran.armed = False
    assert all(type(name) is str for name, _value in items)
    assert dict(Ran.calls) == {}, "the parent's read ran the key's hooks"
    assert child.output == "[]", f"the child's walk ran the key's hooks: {child.output}"
