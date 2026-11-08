"""SPEC-072: the static reader and the ``deliver`` probe, held to real attribute resolution.

Three review frames each found the static reader or the probe disagreeing with what Python
itself resolves — a hook ran, an instance ``emit`` was ignored, a shadowed ``__dict__`` hid an
attribute — and each round closed one shape while the next found another. This corpus is the
structural answer: a differential fixture over the class shapes that decide attribute lookup.

- **The static reader.** For every shape, ``sinks.base._static_value(obj, name)`` must equal
  ``getattr(obj, name, _ABSENT)``. The only exceptions are written per case, as that case's own
  expected value and the reason: the shapes where ``getattr`` would run a hook the reader must
  not run, and the one recorded limit (an instance value shadowing a non-data descriptor behind
  a ``__dict__`` the class shadows).
- **The probe.** For every sink shape, ``read_deliver(sink) is not None`` exactly when the
  ``emit`` the worker would call — ``sink.emit`` resolved by Python, no hostile hook armed — is
  a bound plain function defined on or below the class that defines ``deliver``, and the sink's
  ``log_foundry_counters`` resolves to exactly a ``SinkCounters``. The expectation is derived
  from that resolution at collection time and shown in each case's id; nothing is hand-written.

Every class is built by a metaclass recording ``__eq__``, ``__hash__``, ``__mro__``,
``__name__`` and ``__getattribute__`` and carries instance ``__getattribute__``/``__getattr__``
hooks that record too; while armed, neither read may record anything. Serves invariants 6 and 13.
"""

from __future__ import annotations

import functools
import types
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar

import pytest

from log_foundry.sinks._engine import emit_via_deliver
from log_foundry.sinks.base import _ABSENT, _static_value
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict, read_deliver

if TYPE_CHECKING:
    from collections.abc import Callable


class Hooks:
    """The shared switch and record every hook in this file writes to."""

    armed: ClassVar[bool] = False
    calls: ClassVar[list[str]] = []


def _note(what: str) -> None:
    if Hooks.armed:
        Hooks.calls.append(what)


class Recording(type):
    """A metaclass whose identity, MRO, name and attribute hooks record while armed."""

    def __eq__(cls, other: object) -> bool:
        _note("meta.__eq__")
        return cls is other

    def __hash__(cls) -> int:
        _note("meta.__hash__")
        return id(cls)

    @property
    def __mro__(cls) -> tuple[type, ...]:  # type: ignore[override]
        _note("meta.__mro__")
        return type.__dict__["__mro__"].__get__(cls)  # type: ignore[no-any-return]

    @property
    def __name__(cls) -> str:  # type: ignore[override]
        _note("meta.__name__")
        return type.__dict__["__name__"].__get__(cls)  # type: ignore[no-any-return]

    def __getattribute__(cls, name: str) -> Any:
        _note(f"meta.__getattribute__({name})")
        return type.__getattribute__(cls, name)


def _recording_getattribute(self: object, name: str) -> Any:
    _note(f"__getattribute__({name})")
    return object.__getattribute__(self, name)


def _recording_getattr(self: object, name: str) -> Any:
    _note(f"__getattr__({name})")
    raise AttributeError(name)


_INSTANCE_HOOKS = {"__getattribute__": _recording_getattribute, "__getattr__": _recording_getattr}


def build(
    name: str,
    bases: tuple[type, ...] = (),
    namespace: dict[str, Any] | None = None,
    *,
    getattribute: bool = True,
) -> Any:
    """A class built by :class:`Recording`, with the recording instance hooks unless overridden.

    The probe's sinks take ``getattribute=False``: a Python-level ``__getattribute__`` anywhere in
    a sink's MRO refuses the offer by rule (it could hand the worker another ``emit``), so a
    recording one on every sink would make every probe case the same case. They keep the
    recording ``__getattr__`` and the recording metaclass.
    """
    hooks = dict(_INSTANCE_HOOKS) if getattribute else {"__getattr__": _recording_getattr}
    return Recording(name, bases, {**hooks, **(namespace or {})})


def _put(obj: object, name: str, value: object) -> object:
    object.__setattr__(obj, name, value)
    return obj


class NonData:
    """A non-data descriptor whose ``__get__`` records: ``getattr`` runs it, the reader must not."""

    def __get__(self, obj: object, cls: type | None = None) -> int:
        _note("NonData.__get__")
        return 7


class DeleteOnly:
    """A data descriptor by ``__delete__`` alone, so it beats the instance dict."""

    def __get__(self, obj: object, cls: type | None = None) -> int:
        _note("DeleteOnly.__get__")
        return 7

    def __delete__(self, obj: object) -> None:
        pass


class SetOnly:
    """A "validated field": ``__set__`` without ``__get__``, so reads go past it to the instance."""

    def __set_name__(self, owner: type, name: str) -> None:
        self.name = name

    def __set__(self, obj: object, value: object) -> None:
        object.__getattribute__(obj, "__dict__")[self.name] = value


class DeleteNoGet:
    """``__delete__`` without ``__get__``: not a data descriptor for reading either."""

    def __delete__(self, obj: object) -> None:
        pass


class HostileDict(dict[str, object]):
    """A ``dict`` subclass installed as ``__dict__``; its Python-level reads record."""

    def get(self, *args: Any) -> Any:  # type: ignore[override]
        _note("HostileDict.get")
        return dict.get(self, *args)

    def __getitem__(self, key: str) -> object:
        _note("HostileDict.__getitem__")
        return dict.__getitem__(self, key)


@dataclass(frozen=True)
class Divergent:
    """A case where the reader must not do what ``getattr`` does, with the reader's answer.

    ``hook`` says why: ``getattr`` runs a hook there (the test proves it does, so a stale entry
    for a shape whose hook went away reddens), or, when false, the case is the recorded limit
    and the test proves the two answers really differ.
    """

    expected: Callable[[Any], object]
    reason: str
    hook: bool = True


GETATTR = object()
"""The expectation of a case that must agree with ``getattr(obj, "x", _ABSENT)``."""


def _with_dict_entry(obj: Any, value: object) -> Any:
    obj.__dict__["x"] = value
    return obj


def _property_case() -> Any:
    cls = build("Prop", (), {"x": property(lambda self: _note("property") or 9)})
    return _with_dict_entry(cls(), 5)


def _nondata_no_instance() -> Any:
    return build("NonDataOnly", (), {"x": NonData()})()


def _shadowed_dict_property() -> Any:
    cls = build("ShadowDict", (), {"__dict__": property(lambda self: _note("__dict__") or {})})
    return _put(cls(), "x", 13)


def _foreign_getset() -> Any:
    borrowed = type("Unrelated", (), {}).__dict__["__dict__"]
    return _put(build("Foreign", (), {"__dict__": borrowed})(), "x", 8)


def _shadowed_dict_over_nondata() -> Any:
    cls = build(
        "ShadowOverNonData",
        (),
        {"__dict__": property(lambda self: _note("__dict__") or {}), "x": NonData()},
    )
    return _put(cls(), "x", 5)


def _many() -> Any:
    obj = build("Many")()
    for index in range(41):
        _put(obj, f"a{index}", index)
    return _put(obj, "x", 77)


def _getattribute_override() -> Any:
    def hooked(self: object, name: str) -> Any:
        _note("override.__getattribute__")
        return object.__getattribute__(self, name)

    return _put(build("Override", (), {"__getattribute__": hooked})(), "x", 6)


def _getattr_fallback() -> Any:
    def fallback(self: object, name: str) -> int:
        _note("override.__getattr__")
        return 5

    return build("Fallback", (), {"__getattr__": fallback})()


def _cached_absent() -> Any:
    computed = functools.cached_property(lambda self: _note("cached_property") or 1)
    return build("CachedAbsent", (), {"x": computed})()


def _with_dict_subclass() -> Any:
    obj = build("ReplacedDict")()
    object.__setattr__(obj, "__dict__", HostileDict(x=41))
    return obj


def _slotted(set_it: bool, base: tuple[type, ...] = ()) -> Any:
    cls = build("Slotted", base, {"__slots__": ("x",)})
    obj = cls()
    return _put(obj, "x", 11) if set_it else obj


_SLOT_BASE = build("SlotBase", (), {"__slots__": ("x",)})


_SET_ONLY_BARE = build("SetOnlyBare", (), {"x": SetOnly()})
"""One class for both builds of its case, so ``getattr``'s answer is the very descriptor read."""

STATIC_CASES: dict[str, tuple[Callable[[], Any], object]] = {
    "class-attr": (lambda: build("A", (), {"x": 1})(), GETATTR),
    "instance-over-class": (lambda: _put(build("B", (), {"x": 1})(), "x", 2), GETATTR),
    "property-beats-instance": (
        _property_case,
        Divergent(lambda o: type.__dict__["__dict__"].__get__(type(o))["x"], "getattr runs it"),
    ),
    "nondata-loses-to-instance": (
        lambda: _put(build("C", (), {"x": NonData()})(), "x", 3),
        GETATTR,
    ),
    "nondata-no-instance": (
        _nondata_no_instance,
        Divergent(
            lambda o: type.__dict__["__dict__"].__get__(type(o))["x"], "getattr runs __get__"
        ),
    ),
    "delete-only-data-descriptor": (
        lambda: _with_dict_entry(build("D", (), {"x": DeleteOnly()})(), 3),
        Divergent(
            lambda o: type.__dict__["__dict__"].__get__(type(o))["x"], "getattr runs __get__"
        ),
    ),
    "slot-set": (lambda: _slotted(True), GETATTR),
    "slot-unset": (lambda: _slotted(False), GETATTR),
    "inherited-slot": (lambda: _put(build("SlotChild", (_SLOT_BASE,))(), "x", 12), GETATTR),
    "slots-including-dict": (
        lambda: _put(build("SlotsDict", (), {"__slots__": ("__dict__", "y")})(), "x", 31),
        GETATTR,
    ),
    "managed-dict-over-40": (_many, GETATTR),
    "cached-property-in-dict": (
        lambda: _put(build("Cached", (), {"x": functools.cached_property(lambda s: 1)})(), "x", 21),
        GETATTR,
    ),
    "cached-property-absent": (
        _cached_absent,
        Divergent(lambda o: type.__dict__["__dict__"].__get__(type(o))["x"], "getattr computes it"),
    ),
    "int-subclass": (lambda: _put(build("I", (int,))(3), "x", 1), GETATTR),
    "dict-subclass": (lambda: _put(build("Dsub", (dict,))(), "x", 2), GETATTR),
    "exception-subclass": (lambda: _put(build("E", (Exception,))(), "x", 4), GETATTR),
    "dict-subclass-as-dict": (_with_dict_subclass, GETATTR),
    "metaclass-mro-property": (lambda: build("M", (), {"x": 4})(), GETATTR),
    "getattribute-override": (_getattribute_override, Divergent(lambda o: 6, "getattr runs it")),
    "getattr-fallback": (_getattr_fallback, Divergent(lambda o: _ABSENT, "getattr runs it")),
    "shadowed-dict-property": (_shadowed_dict_property, GETATTR),
    "foreign-getset": (_foreign_getset, GETATTR),
    "shadowed-dict-over-nondata": (
        _shadowed_dict_over_nondata,
        Divergent(
            lambda o: type.__dict__["__dict__"].__get__(type(o))["x"],
            "the recorded limit: an instance value over a non-data descriptor behind a shadowed "
            "__dict__ is not seen",
            hook=False,
        ),
    ),
    "missing": (lambda: build("Empty")(), GETATTR),
    "set-only-descriptor-with-instance": (
        lambda: _put(build("SetOnlyField", (), {"x": SetOnly()})(), "x", 42),
        GETATTR,
    ),
    "set-only-descriptor-no-instance": (
        lambda: _SET_ONLY_BARE(),
        GETATTR,
    ),
    "delete-only-no-get-with-instance": (
        lambda: _with_dict_entry(build("DeleteNoGetField", (), {"x": DeleteNoGet()})(), 43),
        GETATTR,
    ),
}


def _same(got: object, expected: object) -> bool:
    return got is expected or (type(got) is type(expected) and got == expected)


@pytest.mark.parametrize("case", sorted(STATIC_CASES))
def test_the_static_read_agrees_with_getattr(case: str) -> None:
    """``_static_value`` equals ``getattr`` but where this case says why not, and runs no hook."""
    make, expectation = STATIC_CASES[case]
    obj, probed = make(), make()
    Hooks.calls.clear()
    Hooks.armed = True
    try:
        real = getattr(obj, "x", _ABSENT)
    finally:
        Hooks.armed = False
    shared = ("__getattribute__(", "__getattr__(", "meta.")
    ran = [call for call in Hooks.calls if not call.startswith(shared)]
    if isinstance(expectation, Divergent):
        expected = expectation.expected(probed)
        if expectation.hook:
            assert ran, f"{case} is written as divergent because getattr runs a hook, and none ran"
        else:
            assert not _same(real, expected), f"{case}: the recorded limit no longer differs"
    else:
        assert not ran, f"{case}: getattr ran {ran}, so the case cannot be an agreement case"
        expected = real
    Hooks.calls.clear()
    Hooks.armed = True
    try:
        got = _static_value(probed, "x")
    finally:
        Hooks.armed = False
    assert Hooks.calls == [], f"{case}: the static read ran {Hooks.calls}"
    assert _same(got, expected), f"{case}: read {got!r}, expected {expected!r}"


# --- the probe ---------------------------------------------------------------------------


def _deliver(self: object, batch: list[dict[str, object]]) -> Verdict:
    return Verdict.uniform(Outcome.ACCEPTED, len(batch))


def _adapter(self: object, batch: list[dict[str, object]]) -> None:
    emit_via_deliver(self, batch)


def _close(self: object) -> None:
    pass


_ENGINE = {"deliver": _deliver, "emit": _adapter, "close": _close}


def _sink_class(
    name: str, bases: tuple[type, ...] = (), namespace: dict[str, Any] | None = None
) -> Any:
    return build(name, bases, namespace, getattribute=False)


def _engine(name: str, bases: tuple[type, ...] = (), extra: dict[str, Any] | None = None) -> Any:
    return _sink_class(name, bases, {**_ENGINE, **(extra or {})})


def _counted(obj: Any) -> Any:
    return _put(obj, "log_foundry_counters", SinkCounters())


def _proxy() -> Any:
    inner = _counted(_engine("Inner")())

    def delegate(self: object, name: str) -> Any:
        _note("proxy.__getattr__")
        return getattr(inner, name)

    return _sink_class("Proxy", (), {"__getattr__": delegate, "close": _close})()


def _emit_below() -> Any:
    base = _sink_class("EmitBase", (), {"emit": _adapter, "close": _close})
    return _counted(_sink_class("DeliverAbove", (base,), {"deliver": _deliver})())


def _instance_emit() -> Any:
    return _put(_counted(_engine("InstanceEmit")()), "emit", lambda batch: None)


def _stamping(self: object, batch: list[dict[str, object]]) -> None:
    _adapter(self, [{**event, "stamped": True} for event in batch])


def _many_sink() -> Any:
    sink = _counted(_engine("ManySink")())
    for index in range(41):
        _put(sink, f"a{index}", index)
    return sink


def _passing(self: object, name: str) -> Any:
    _note("override.__getattribute__")
    return object.__getattribute__(self, name)


def _scrubbing(self: object, name: str) -> Any:
    if name == "emit":
        inner = object.__getattribute__(self, "emit")
        return lambda batch: inner([{**event, "scrubbed": True} for event in batch])
    return object.__getattribute__(self, name)


def _supplies_emit(self: object, name: str) -> Any:
    if name == "emit":
        return types.MethodType(_adapter, self)
    raise AttributeError(name)


def _emit_bound_elsewhere() -> Any:
    other = _counted(_engine("Other")())
    sink = _counted(_engine("BoundElsewhere")())
    return _put(sink, "emit", types.MethodType(_adapter, other))


def _engine_slots() -> Any:
    return _counted(_engine("SlotSink", (), {"__slots__": ("log_foundry_counters",)})())


PROBE_CASES: dict[str, Callable[[], Any]] = {
    "plain": lambda: _counted(_engine("Plain")()),
    "no-counters": lambda: _engine("NoCounters")(),
    "counters-property": lambda: _engine(
        "CountersProp",
        (),
        {"log_foundry_counters": property(lambda s: _note("counters-property") or SinkCounters())},
    )(),
    "slots": _engine_slots,
    "slots-including-dict": lambda: _counted(_engine("SD", (), {"__slots__": ("__dict__",)})()),
    "managed-dict-over-40": lambda: _many_sink(),
    "int-subclass": lambda: _counted(_engine("IntSink", (int,))(3)),
    "dict-subclass": lambda: _counted(_engine("DictSink", (dict,))()),
    "exception-subclass": lambda: _counted(_engine("ExcSink", (Exception,))()),
    "metaclass-mro-property": lambda: _counted(_engine("MroSink")()),
    "getattribute-override": lambda: _counted(
        _engine("GaSink", (), {"__getattribute__": _passing})()
    ),
    "scrubbing-getattribute": lambda: _counted(
        _engine("ScrubSink", (), {"__getattribute__": _scrubbing})()
    ),
    "no-emit": lambda: _counted(
        _sink_class("NoEmit", (), {"deliver": _deliver, "close": _close})()
    ),
    "getattr-supplies-emit": lambda: _counted(
        _sink_class(
            "GetattrEmit",
            (),
            {"deliver": _deliver, "close": _close, "__getattr__": _supplies_emit},
        )()
    ),
    "instance-emit-bound-to-another-sink": _emit_bound_elsewhere,
    "set-only-counters-descriptor": lambda: _counted(
        _engine("SetOnlyCounters", (), {"log_foundry_counters": SetOnly()})()
    ),
    "shadowed-dict-property": lambda: _counted(
        _engine("ShadowSink", (), {"__dict__": property(lambda s: {})})()
    ),
    "foreign-getset": lambda: _counted(
        _engine("ForeignSink", (), {"__dict__": type("U", (), {}).__dict__["__dict__"]})()
    ),
    "instance-emit": _instance_emit,
    "class-emit-override": lambda: _counted(
        _sink_class("Overrides", (_engine("EngineBase"),), {"emit": _adapter})()
    ),
    "class-emit-override-own-function": lambda: _counted(
        _sink_class("Stamps", (_engine("EngineBase3"),), {"emit": _stamping})()
    ),
    "emit-defined-below-deliver": _emit_below,
    "emit-property": lambda: _counted(
        _engine("EmitProp", (), {"emit": property(lambda s: _adapter.__get__(s))})()
    ),
    "emit-staticmethod": lambda: _counted(
        _engine("EmitStatic", (), {"emit": staticmethod(lambda batch: None)})()
    ),
    "deliver-none": lambda: _counted(
        _sink_class("OptOut", (_engine("EngineBase2"),), {"deliver": None})()
    ),
    "proxy-getattr": _proxy,
}


def _offers_by_resolution(sink: Any) -> bool:
    """Whether real resolution makes the worker call the class adapter below ``deliver``'s class."""
    mro = type(sink).__mro__
    index = next((i for i, klass in enumerate(mro) if "deliver" in vars(klass)), None)
    if index is None or type(vars(mro[index])["deliver"]) is not types.FunctionType:
        return False
    if type(getattr(sink, "log_foundry_counters", None)) is not SinkCounters:
        return False
    bound = getattr(sink, "emit", None)
    if type(bound) is not types.MethodType or bound.__self__ is not sink:
        return False
    home = next((i for i, klass in enumerate(mro) if "emit" in vars(klass)), None)
    return home is not None and home >= index and vars(mro[home])["emit"] is bound.__func__


PROBE_DIVERGENT: dict[str, tuple[bool, str]] = {
    "counters-property": (
        False,
        "the counters resolve only by running a property, which the probe never runs",
    ),
    "getattribute-override": (
        False,
        (
            "a Python-level __getattribute__ may hand the worker another emit, and asking which "
            "means running it, so the probe refuses even one that passes through"
        ),
    ),
}
"""Cases where the probe must not follow resolution, with its answer and why.

The test proves each reason: resolving the case's members really runs a hook, so an entry whose
hook went away is a failure rather than a silent exemption.
"""

_DERIVED = {case: _offers_by_resolution(make()) for case, make in PROBE_CASES.items()}
_EXPECTED = {
    case: PROBE_DIVERGENT.get(case, (offered, ""))[0] for case, offered in _DERIVED.items()
}


@pytest.mark.parametrize(
    ("case", "offered"),
    sorted(_EXPECTED.items()),
    ids=[
        f"{case}-{'offered' if offered else 'refused'}"
        for case, offered in sorted(_EXPECTED.items())
    ],
)
def test_the_probe_offers_exactly_when_resolution_reaches_the_adapter(
    case: str, offered: bool
) -> None:
    """``read_deliver`` agrees with Python's own resolution of ``emit``, and runs no hook."""
    sink = PROBE_CASES[case]()
    Hooks.calls.clear()
    Hooks.armed = True
    try:
        derived = _offers_by_resolution(sink)
    finally:
        Hooks.armed = False
    ran = [
        c for c in Hooks.calls if not c.startswith(("__getattribute__(", "__getattr__(", "meta."))
    ]
    if case in PROBE_DIVERGENT:
        assert ran, f"{case} is divergent because resolution runs a hook, and none ran"
    else:
        assert not ran, f"{case}: resolution ran {ran}, so it cannot be the expectation"
        assert derived is offered, "the derivation is stable across builds"
    Hooks.calls.clear()
    Hooks.armed = True
    try:
        found = read_deliver(sink)
    finally:
        Hooks.armed = False
    assert Hooks.calls == [], f"{case}: the probe ran {Hooks.calls}"
    assert (found is not None) is offered, f"{case}: read_deliver gave {found!r}"


def test_the_corpus_offers_and_refuses_both() -> None:
    """A corpus whose derivation said one answer for every case could not tell a probe apart."""
    assert set(_EXPECTED.values()) == {True, False}, _EXPECTED
