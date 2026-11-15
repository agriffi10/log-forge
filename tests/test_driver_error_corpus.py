"""A hostile driver exception class can neither stop the dual type being built nor raise from it.

``sinks/_driver_error.delivery_error`` copies a driver's exception into a class derived from both
``SinkDeliveryError`` and the driver's own class. Review found the same class of defect in it three
rounds running: each fix closed one read that ran the driver class's own code, and the next frame
found another (``docs/architecture.md`` §12). This corpus holds the structural answer. Every
fixture is a driver exception whose class, metaclass or instance plants code where a read or write
could reach it; each hook records that it ran, in :data:`_RAN`, then raises. Each fixture carries
the failure it asserts never returns, which is the message of every assertion it fails.

Two kinds of fixture. A **read** fixture hides its code behind something the library copies —
the cache key, a stored attribute, the cause, a namespace value — and must still be built, with
its hook never run, by ``delivery_error``, by the dual type's ``__reduce__`` and by both file
sinks. A **refusal** fixture refuses the subclass or its instance through a hook Python's own
subclassing or instantiation runs, which no mechanism can skip while it derives from the class;
``delivery_error`` must return ``None`` without raising, and the file sinks raise the original.
Serves invariant 1.
"""

from __future__ import annotations

import gc
import pickle
import traceback
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pytest

from log_foundry.sinks._driver_error import delivery_error
from log_foundry.sinks.base import SinkDeliveryError
from sink_rigs import RIGS, ok

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

_RAN: list[str] = []
"""Every hostile hook that ran, by name; each one also raises."""

_ARGS = BaseException.__dict__["args"].__get__
_CAUSE = BaseException.__dict__["__cause__"].__get__
_FILENAME = OSError.__dict__["filename"].__get__
_INSTANCE_DICT = BaseException.__dict__["__dict__"].__get__
_STORED_NAME = type.__dict__["__name__"].__get__
"""Readers the assertions use, so a hostile class's own hooks never run in the test either."""


@pytest.fixture(autouse=True)
def _ran() -> list[str]:
    _RAN.clear()
    return _RAN


def _hook(name: str) -> RuntimeError:
    """Records that the hostile hook ``name`` ran and returns the error it raises."""
    _RAN.append(name)
    return RuntimeError(f"the driver class's {name} ran")


# --- read fixtures: module level, so pickle can find them -------------------------------------


class ArgsProperty(OSError):
    """An ``args`` property that raises: read through it, ``args`` never reaches the dual type."""

    @property
    def args(self) -> tuple[object, ...]:  # type: ignore[override]
        raise _hook("args property")


class CauseProperty(OSError):
    """A ``__cause__`` data descriptor whose setter raises, shadowing ``BaseException``'s."""

    @property
    def __cause__(self) -> BaseException | None:  # type: ignore[override]
        return None

    @__cause__.setter
    def __cause__(self, value: object) -> None:
        raise _hook("__cause__ setter")


class SetattrRaises(OSError):
    """A ``__setattr__`` that refuses every name, the cause and the copied attributes alike."""

    def __setattr__(self, name: str, value: object) -> None:
        raise _hook(f"__setattr__({name})")


class GetattributeRaises(OSError):
    """A ``__getattribute__`` that raises for every name the dual type copies or used to read."""

    _HIDDEN = frozenset(
        {"args", "filename", "filename2", "sqlite_errorcode", "sqlite_errorname", "__cause__"}
    )

    def __getattribute__(self, name: str) -> Any:
        if name in GetattributeRaises._HIDDEN:
            raise _hook(f"__getattribute__({name})")
        return super().__getattribute__(name)


class FilenameProperty(OSError):
    """A ``filename`` property that raises; the instance stores the path in ``OSError``'s slot."""

    @property
    def filename(self) -> object:  # type: ignore[override]
        raise _hook("filename property")

    @filename.setter
    def filename(self, value: object) -> None:
        OSError.__dict__["filename"].__set__(self, value)


class PlainSetattrRaises(Exception):
    """A non-``OSError`` whose ``__setattr__`` refuses, so a copied ``__dict__`` entry meets it."""

    def __setattr__(self, name: str, value: object) -> None:
        raise _hook(f"__setattr__({name})")


def _plain_setattr_raises() -> BaseException:
    err = PlainSetattrRaises("SENTINEL")
    _INSTANCE_DICT(err)["sqlite_errorcode"] = 5
    return err


class DriverAttributeProperty(Exception):
    """A ``sqlite_errorcode`` property raising what ``hasattr`` lets out, not ``AttributeError``."""

    @property
    def sqlite_errorcode(self) -> int:
        raise _hook("sqlite_errorcode property")


class Slotted(Exception):
    """Copied attributes kept in ``__slots__``, one set and one unset, none in ``__dict__``."""

    __slots__ = ("filename", "sqlite_errorcode")


def _slotted() -> BaseException:
    err = Slotted("SENTINEL")
    err.filename = "f.log"
    return err


class ForeignSlot(Exception):
    """A ``filename`` stored as ``OSError``'s member descriptor on a class that is not one."""

    filename = OSError.__dict__["filename"]


class DictSubclass(dict[str, object]):
    """An instance ``__dict__`` whose own lookups raise; the stored entries are what is copied."""

    def __getitem__(self, key: str) -> object:
        raise _hook("instance dict __getitem__")

    def get(self, key: str, default: object = None) -> object:  # type: ignore[override]
        raise _hook("instance dict get")

    def items(self) -> Any:
        raise _hook("instance dict items")

    def __contains__(self, key: object) -> bool:
        raise _hook("instance dict __contains__")

    def __iter__(self) -> Any:
        raise _hook("instance dict __iter__")


class CollidingKey:
    """A key hashing as ``target`` whose ``__eq__`` raises once armed, as a dict lookup runs."""

    def __init__(self, target: str) -> None:
        self.target = target
        self.armed = False

    def __hash__(self) -> int:
        return hash(self.target)

    def __eq__(self, other: object) -> bool:
        if self.armed:
            raise _hook(f"colliding key __eq__({self.target})")
        return False


_ABSENT = object()


def _plant(namespace: dict[Any, object], target: str) -> None:
    """Plants an armed key colliding with ``target`` ahead of it in a real namespace dict.

    The real ``target`` is taken out and put back after the planted key, so a lookup of
    ``target`` probes the planted key first and compares it, running its ``__eq__``; planted
    after the real key, the lookup would find the real one first and compare nothing.
    """
    key = CollidingKey(target)
    value = namespace.pop(target, _ABSENT)
    namespace[key] = "planted"
    if value is not _ABSENT:
        namespace[target] = value
    key.armed = True


# --- read fixtures: built per call, so collection never touches their metaclasses ------------


def _metaclass(name: str, body: dict[str, object]) -> type:
    return type(name, (type,), body)


def _raising_property(name: str) -> property:
    def get(cls: object) -> object:
        raise _hook(name)

    return property(get)


class _RaisingSetName:
    """A stored value whose ``__set_name__`` raises, which ``type()`` runs on a namespace value."""

    def __set_name__(self, owner: type, name: str) -> None:
        raise _hook(f"__set_name__ for {name}")


class _HostileStr(str):
    """A ``str`` subclass whose comparison raises, as a traceback line's module check would run."""

    __slots__ = ()

    def __eq__(self, other: object) -> bool:
        raise _hook("str subclass __eq__")

    def __hash__(self) -> int:
        raise _hook("str subclass __hash__")


class _ClassProperty:
    """A stored value whose ``__class__`` raises; ``isinstance`` reads it when its check fails."""

    @property
    def __class__(self) -> type:  # type: ignore[override]
        raise _hook("stored value __class__")


class _RaisingDoc:
    """A descriptor stored as the class's own ``__doc__``, which ``type``'s ``__doc__`` calls."""

    def __get__(self, obj: object, cls: type | None = None) -> str:
        raise _hook("__doc__ descriptor")


def _hash_raises() -> BaseException:
    def hash_(cls: object) -> int:
        raise _hook("metaclass __hash__")

    meta = _metaclass("HashMeta", {"__hash__": hash_})
    return meta("HashRaises", (OSError,), {})(5, "SENTINEL")  # type: ignore[no-any-return]


def _unhashable() -> BaseException:
    meta = _metaclass("UnhashableMeta", {"__hash__": None})
    return meta("Unhashable", (OSError,), {})(5, "SENTINEL")  # type: ignore[no-any-return]


def _eq_raises() -> BaseException:
    def eq(cls: object, other: object) -> bool:
        raise _hook("metaclass __eq__")

    meta = _metaclass("EqMeta", {"__hash__": lambda cls: 7, "__eq__": eq})
    first = meta("FirstColliding", (OSError,), {})
    delivery_error(first(5, "SENTINEL"))
    _RAN.clear()
    return meta("SecondColliding", (OSError,), {})(5, "SENTINEL")  # type: ignore[no-any-return]


def _with_class_attribute(name: str, value: object) -> Callable[[], BaseException]:
    def make() -> BaseException:
        error_class = type("Stored", (OSError,), {})
        setattr(error_class, name, value)
        return error_class(5, "SENTINEL")

    return make


def _module_deleted() -> BaseException:
    error_class = type("ModuleDeleted", (OSError,), {})
    del gc.get_referents(error_class.__dict__)[0]["__module__"]
    return error_class(5, "SENTINEL")


def _planted_class_key(target: str) -> Callable[[], BaseException]:
    def make() -> BaseException:
        error_class = type("Planted", (OSError,), {"__doc__": "planted"})
        _plant(gc.get_referents(error_class.__dict__)[0], target)
        return error_class(5, "SENTINEL")

    return make


def _metaclass_property(name: str) -> Callable[[], BaseException]:
    def make() -> BaseException:
        meta = _metaclass(f"Meta_{name}", {name: _raising_property(f"metaclass {name}")})
        return meta("MetaHidden", (OSError,), {})(5, "SENTINEL")  # type: ignore[no-any-return]

    return make


def _metaclass_getattribute() -> BaseException:
    names = {"__name__", "__qualname__", "__module__", "__doc__", "__bases__", "__mro__"}

    def getattribute(cls: type, name: str) -> object:
        if name in names:
            raise _hook(f"metaclass __getattribute__({name})")
        return type.__getattribute__(cls, name)

    meta = _metaclass("GetattributeMeta", {"__getattribute__": getattribute})
    return meta("MetaHidden", (OSError,), {})(5, "SENTINEL")  # type: ignore[no-any-return]


def _dict_subclass() -> BaseException:
    err = Exception("SENTINEL")
    err.__dict__ = DictSubclass(sqlite_errorcode=5, sqlite_errorname="SQLITE_ERROR")
    return err


def _planted_instance_key() -> BaseException:
    err = Exception("SENTINEL")
    err.sqlite_errorcode = 5  # type: ignore[attr-defined]
    _plant(_INSTANCE_DICT(err), "sqlite_errorcode")
    return err


# --- refusal fixtures ------------------------------------------------------------------------


class Picky(OSError):
    """A constructor that rejects its own ``args``, so no instance of the dual type can be made."""

    def __init__(self, code: int, *, why: str) -> None:
        super().__init__(code, why)


def _picky() -> BaseException:
    return Picky(28, why="full")


def _init_subclass_refuses() -> BaseException:
    def refuse(cls: type, **kwargs: object) -> None:
        raise _hook("__init_subclass__")

    return type("Final", (OSError,), {"__init_subclass__": classmethod(refuse)})(5, "SENTINEL")


def _metaclass_new_refuses() -> BaseException:
    def new(meta: type, name: str, bases: tuple[type, ...], ns: dict[str, object]) -> type:
        if len(bases) > 1:
            raise _hook("metaclass __new__")
        return type.__new__(meta, name, bases, ns)

    meta = _metaclass("SealedMeta", {"__new__": new})
    return meta("Sealed", (OSError,), {})(5, "SENTINEL")  # type: ignore[no-any-return]


def _metaclass_call_refuses() -> BaseException:
    def call(cls: type, *args: object) -> object:
        if len(type.__dict__["__bases__"].__get__(cls)) > 1:
            raise _hook("metaclass __call__")
        return type.__call__(cls, *args)

    meta = _metaclass("CallMeta", {"__call__": call})
    return meta("Uncallable", (OSError,), {})(5, "SENTINEL")  # type: ignore[no-any-return]


def _new_returns_another() -> BaseException:
    def new(cls: type, *args: object) -> object:
        if len(type.__dict__["__bases__"].__get__(cls)) > 1:
            return ValueError("not the dual type")
        return OSError.__new__(cls, *args)  # type: ignore[call-overload]

    return type("Swapping", (OSError,), {"__new__": new})(5, "SENTINEL")


def _mro_drops_original() -> BaseException:
    def mro(cls: type) -> list[type]:
        if len(type.__dict__["__bases__"].__get__(cls)) > 1:
            return [cls, SinkDeliveryError, Exception, BaseException, object]
        return type.mro(cls)

    meta = _metaclass("MroMeta", {"mro": mro})
    return meta("Dropped", (Exception,), {})("SENTINEL")  # type: ignore[no-any-return]


def _init_subclass_rebases() -> BaseException:
    def rebase(cls: type, **kwargs: object) -> None:
        cls.__bases__ = (SinkDeliveryError,)
        del cls.__reduce__  # type: ignore[attr-defined]

    return type("Rebased", (Exception,), {"__init_subclass__": classmethod(rebase)})("SENTINEL")


def _planted_init_subclass_key() -> BaseException:
    error_class = type("PlantedFinal", (OSError,), {})
    _plant(gc.get_referents(error_class.__dict__)[0], "__init_subclass__")
    return error_class(5, "SENTINEL")


@dataclass(frozen=True)
class Hostile:
    """One fixture: how to make the driver error, the failure it guards, and whether it builds.

    Attributes:
      make: Builds a fresh driver exception carrying the hostile hook.
      reason: The failure this fixture asserts never returns; every assertion's message.
      builds: ``True`` for a read fixture, which must be built; ``False`` for a refusal.
      pickles: Whether a full ``pickle`` round trip is asserted; ``pickle`` resolves the class by
        its own attribute reads, so only a module-level class with no metaclass hook qualifies.
    """

    make: Callable[[], BaseException]
    reason: str
    builds: bool = True
    pickles: bool = False


CORPUS: dict[str, Hostile] = {
    "metaclass-hash-raises": Hostile(
        _hash_raises, "the cache key ran the metaclass __hash__, so no dual type was built"
    ),
    "unhashable-class": Hostile(
        _unhashable, "an unhashable class could not be a cache key, so no dual type was built"
    ),
    "metaclass-eq-raises": Hostile(
        _eq_raises, "a colliding cache key ran the metaclass __eq__, so no dual type was built"
    ),
    "cause-setter": Hostile(
        lambda: CauseProperty(5, "SENTINEL"),
        "setting __cause__ ran the class's setter outside the guard, so delivery_error raised",
        pickles=True,
    ),
    "setattr-raises": Hostile(
        lambda: SetattrRaises(2, "SENTINEL", "a.log"),
        "a copied attribute or the cause went through the class's __setattr__, which refused it",
        pickles=True,
    ),
    "args-property": Hostile(
        lambda: ArgsProperty(5, "SENTINEL"),
        "args was read through the class's property, so no dual type was built and pickling raised",
        pickles=True,
    ),
    "getattribute-raises": Hostile(
        lambda: GetattributeRaises(2, "SENTINEL", "a.log"),
        "an attribute was read through the class's __getattribute__, so no dual type was built",
        pickles=True,
    ),
    "filename-property": Hostile(
        lambda: FilenameProperty(2, "SENTINEL", "a.log"),
        "the filename property ran, so one raising attribute cost the whole dual type",
        pickles=True,
    ),
    "driver-attribute-property": Hostile(
        lambda: DriverAttributeProperty("SENTINEL"),
        "hasattr ran the sqlite_errorcode property, so one raising attribute cost the dual type",
        pickles=True,
    ),
    "instance-dict-subclass": Hostile(
        _dict_subclass,
        "the instance's own __dict__ subclass ran its lookup, so no dual type was built",
    ),
    "planted-instance-key": Hostile(
        _planted_instance_key,
        "an attribute lookup compared a colliding instance-dict key, running its __eq__",
    ),
    "module-set-name": Hostile(
        _with_class_attribute("__module__", _RaisingSetName()),
        "the stored __module__ was copied into the namespace and type() ran its __set_name__",
    ),
    "module-str-subclass": Hostile(
        _with_class_attribute("__module__", _HostileStr("driver")),
        "a str subclass was copied as the dual's __module__, and its traceback line ran its __eq__",
    ),
    "module-not-a-str": Hostile(
        _with_class_attribute("__module__", 5),
        "a non-str __module__ was copied onto the dual type, so it had no module a line can name",
    ),
    "module-class-property": Hostile(
        _with_class_attribute("__module__", _ClassProperty()),
        "the stored __module__ was checked with isinstance, which read its own __class__",
    ),
    "name-str-subclass": Hostile(
        _with_class_attribute("__name__", _HostileStr("Stored")),
        "a str subclass stored as the name was copied as the dual type's name",
    ),
    "qualname-str-subclass": Hostile(
        _with_class_attribute("__qualname__", _HostileStr("Stored")),
        "a str subclass stored as the qualified name was copied as the dual type's",
    ),
    "plain-setattr-raises": Hostile(
        _plain_setattr_raises,
        "a copied __dict__ entry went through the class's __setattr__, which refused it",
        pickles=True,
    ),
    "slotted-attribute": Hostile(
        _slotted,
        "a copied attribute kept in __slots__ was looked for only in the __dict__, and was lost",
        pickles=True,
    ),
    "foreign-member-descriptor": Hostile(
        lambda: ForeignSlot("SENTINEL"),
        "a member descriptor of a class the instance is not raised TypeError from the read",
        pickles=True,
    ),
    "module-deleted": Hostile(
        _module_deleted, "a class whose namespace lost __module__ raised AttributeError on read"
    ),
    "planted-module-key": Hostile(
        _planted_class_key("__module__"),
        "reading the stored __module__ compared a colliding namespace key, running its __eq__",
    ),
    "planted-doc-key": Hostile(
        _planted_class_key("__doc__"),
        "reading the stored __doc__ compared a colliding namespace key, running its __eq__",
    ),
    "doc-descriptor": Hostile(
        _with_class_attribute("__doc__", _RaisingDoc()),
        "type's __doc__ descriptor called the descriptor stored as the class's __doc__",
    ),
    "doc-set-name": Hostile(
        _with_class_attribute("__doc__", _RaisingSetName()),
        "a non-str __doc__ was copied into the namespace and type() ran its __set_name__",
    ),
    "doc-str-subclass": Hostile(
        _with_class_attribute("__doc__", _HostileStr("documented")),
        "a str subclass was copied as the dual's __doc__ rather than an exact str",
    ),
    **{
        f"metaclass-{name.strip('_')}-property": Hostile(
            _metaclass_property(name), f"the metaclass {name} property ran on a copied read"
        )
        for name in ("__name__", "__module__", "__doc__", "__bases__", "__mro__")
    },
    "metaclass-getattribute": Hostile(
        _metaclass_getattribute, "the metaclass __getattribute__ ran on a copied read"
    ),
    "picky-constructor": Hostile(
        _picky, "a constructor rejecting its args escaped delivery_error", builds=False
    ),
    "init-subclass-refuses": Hostile(
        _init_subclass_refuses, "an __init_subclass__ refusal escaped delivery_error", builds=False
    ),
    "metaclass-new-refuses": Hostile(
        _metaclass_new_refuses, "a metaclass refusing the subclass escaped", builds=False
    ),
    "metaclass-call-refuses": Hostile(
        _metaclass_call_refuses, "a metaclass refusing the instance escaped", builds=False
    ),
    "new-returns-another": Hostile(
        _new_returns_another,
        "a constructor returning another object was raised as though it were the dual type",
        builds=False,
    ),
    "metaclass-mro-drops-original": Hostile(
        _mro_drops_original,
        "a metaclass mro left the original out, so an instance its own except misses was raised",
        builds=False,
    ),
    "init-subclass-rebases": Hostile(
        _init_subclass_rebases,
        "an __init_subclass__ rebased the derived class off the original, which was raised anyway",
        builds=False,
    ),
    "planted-init-subclass-key": Hostile(
        _planted_init_subclass_key,
        "subclassing compared a colliding namespace key and its refusal escaped",
        builds=False,
    ),
}
"""The hostile-class corpus, by id: every way in that review found, and its neighbours."""

BUILDS = [name for name, fixture in CORPUS.items() if fixture.builds]
REFUSES = [name for name, fixture in CORPUS.items() if not fixture.builds]
PICKLES = [name for name, fixture in CORPUS.items() if fixture.pickles]


def _build(name: str) -> tuple[BaseException, BaseException | None]:
    err = CORPUS[name].make()
    _RAN.clear()
    return err, delivery_error(err)  # type: ignore[arg-type]


@pytest.mark.parametrize("name", BUILDS)
def test_a_read_fixture_is_built_and_its_hook_never_runs(name: str) -> None:
    """The dual type is both classes, keeps the stored name, args,; its cause is the original."""
    reason = CORPUS[name].reason
    err, dual = _build(name)
    assert dual is not None, reason
    assert _RAN == [], reason
    assert isinstance(dual, SinkDeliveryError), reason
    assert issubclass(type(dual), type(err)), reason
    assert str.__str__(_STORED_NAME(type(dual))) == str.__str__(_STORED_NAME(type(err))), reason
    assert _ARGS(dual) == _ARGS(err), reason
    assert _CAUSE(dual) is err, reason


@pytest.mark.parametrize("name", BUILDS)
def test_a_read_fixtures_dual_type_copies_only_exact_strings(name: str) -> None:
    """Dual type ``__module__``, ``__qualname__`` and ``__doc__`` are ``str`` or absent."""
    reason = CORPUS[name].reason
    _, dual = _build(name)
    assert dual is not None, reason
    namespace = gc.get_referents(type.__dict__["__dict__"].__get__(type(dual)))[0]
    assert type(namespace["__module__"]) is str, reason
    assert type(_STORED_NAME(type(dual))) is str, reason
    assert type(type.__dict__["__qualname__"].__get__(type(dual))) is str, reason
    assert type(namespace["__doc__"]) in (str, type(None)), reason
    assert _RAN == [], reason


_TRACEBACK_READS = {
    "metaclass-module-property",
    "metaclass-mro-property",
    "metaclass-getattribute",
    "getattribute-raises",
}
"""Fixtures whose hook ``traceback`` itself reaches, through an attribute lookup of its own.

The metaclass ones guard the class's ``__module__``, which ``traceback`` reads as
``exc_type.__module__``; the dual type inherits that metaclass. ``getattribute-raises`` guards
``__cause__``, which it reads as ``exc.__cause__``; the dual type inherits that method.
"""

_SET_CAUSE = BaseException.__dict__["__cause__"].__set__
_SET_CONTEXT = BaseException.__dict__["__context__"].__set__


@pytest.mark.parametrize("name", BUILDS)
def test_a_read_fixtures_traceback_line_runs_nothing(name: str) -> None:
    """The line ``traceback`` prints for the dual type reads only what it was given.

    The cause is cleared first: ``traceback`` formats the cause too, and the cause is the
    original, whose class is the hostile one, so its line is the driver's to fail, not ours.
    """
    reason = CORPUS[name].reason
    _, dual = _build(name)
    assert dual is not None, reason
    if name in _TRACEBACK_READS:
        pytest.skip("traceback reads this class attribute itself, which the hook guards")
    _SET_CAUSE(dual, None)
    _SET_CONTEXT(dual, None)
    line = "".join(traceback.format_exception_only(dual))
    assert "SENTINEL" in line, reason
    assert _RAN == [], reason


@pytest.mark.parametrize("name", BUILDS)
def test_a_read_fixtures_reduce_never_raises(name: str) -> None:
    """``__reduce__`` is documented ``Raises: None``; it names the original and its stored args."""
    reason = CORPUS[name].reason
    err, dual = _build(name)
    assert dual is not None, reason
    namespace = gc.get_referents(type.__dict__["__dict__"].__get__(type(dual)))[0]
    _, arguments = namespace["__reduce__"](dual)
    assert arguments[0] is type(err), reason
    assert arguments[1] == _ARGS(err), reason
    assert _RAN == [], reason


@pytest.mark.parametrize("name", PICKLES)
def test_a_read_fixture_pickles_as_the_original(name: str) -> None:
    """The far side of a pickle gets the driver's own class, ``args`` and stored filename."""
    reason = CORPUS[name].reason
    err, dual = _build(name)
    assert dual is not None, reason
    back = pickle.loads(pickle.dumps(dual))  # noqa: S301
    assert type(back) is type(err), reason
    assert _ARGS(back) == _ARGS(err), reason
    if isinstance(err, OSError):
        assert _FILENAME(back) == _FILENAME(err), reason
    assert _RAN == [], reason


@pytest.mark.parametrize("name", REFUSES)
def test_a_refusal_fixture_returns_none_and_raises_nothing(name: str) -> None:
    """A class that refuses its subclass or instance leaves the original to be raised."""
    _, dual = _build(name)
    assert dual is None, CORPUS[name].reason


@pytest.mark.parametrize("name", list(CORPUS))
@pytest.mark.parametrize("sink", ["FileSink", "RotatingFileSink"])
def test_a_file_sink_raises_the_dual_type_or_the_original(
    sink: str, name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A read fixture is raised as both types with its hook never run; a refusal as the original."""
    fixture = CORPUS[name]
    err = fixture.make()
    rig = RIGS[sink](monkeypatch, tmp_path)
    rig.state.error = err
    _RAN.clear()
    caught: BaseException | None = None
    try:
        rig.sink.emit([ok("a")])
    except BaseException as raised:
        caught = raised
    if fixture.builds:
        assert isinstance(caught, SinkDeliveryError), fixture.reason
        assert issubclass(type(caught), type(err)), fixture.reason
        assert _RAN == [], fixture.reason
    else:
        assert caught is err, fixture.reason


def test_a_static_classs_dual_type_keeps_its_module() -> None:
    """``OSError`` stores no ``__module__``; its dual type's is ``type``'s own, ``builtins``."""
    dual = delivery_error(OSError(5, "SENTINEL"))
    assert dual is not None
    assert type.__dict__["__module__"].__get__(type(dual)) == "builtins"
    assert "".join(traceback.format_exception_only(dual)).startswith("OSError: ")


def test_a_driver_attribute_in_the_instance_dict_is_copied() -> None:
    """``sqlite_errorcode`` lives in the instance's ``__dict__``; the dual type's holds it too."""
    err = Exception("SENTINEL")
    err.sqlite_errorcode = 5  # type: ignore[attr-defined]
    err.sqlite_errorname = "SQLITE_ERROR"  # type: ignore[attr-defined]
    err.unrelated = "kept out"  # type: ignore[attr-defined]
    err.filename = None  # type: ignore[attr-defined]
    err.filename2 = "b.log"  # type: ignore[attr-defined]
    dual = delivery_error(err)
    assert dual is not None
    assert _INSTANCE_DICT(dual) == {
        "sqlite_errorcode": 5,
        "sqlite_errorname": "SQLITE_ERROR",
        "filename2": "b.log",
    }


def test_a_slotted_attribute_is_copied_into_its_slot_and_survives_the_pickle() -> None:
    """A ``__slots__`` attribute is read and written through its slot; an unset one stays unset."""
    dual = delivery_error(_slotted())  # type: ignore[arg-type]
    assert dual is not None
    assert dual.filename == "f.log"  # type: ignore[attr-defined]
    assert not hasattr(dual, "sqlite_errorcode")
    back = pickle.loads(pickle.dumps(dual))  # noqa: S301
    assert type(back) is Slotted
    assert back.filename == "f.log"


class ShadowingSlot(OSError):
    """An ``OSError`` subclass with its own ``filename`` slot, which attribute access reads."""

    __slots__ = ("filename",)


def test_the_nearest_slot_is_the_one_copied() -> None:
    """``filename`` lives in the subclass's slot, not ``OSError``'s, so that is what is copied."""
    err = ShadowingSlot(2, "SENTINEL")
    err.filename = "nearest.log"
    assert _FILENAME(err) is None
    dual = delivery_error(err)
    assert dual is not None
    assert dual.filename == "nearest.log"
    back = pickle.loads(pickle.dumps(dual))  # noqa: S301
    assert (type(back), back.filename) == (ShadowingSlot, "nearest.log")


def _collect_a_rebased_original() -> int:
    err = _init_subclass_rebases()
    assert delivery_error(err) is None  # type: ignore[arg-type]
    return id(type(err))


def test_a_class_built_where_a_rebased_original_was_is_given_its_own_dual_type() -> None:
    """The cache entry holds the original, so no later class can be handed its derived class.

    The rebased original's derived class neither derives from it nor holds it in a ``__reduce__``,
    so only the cache entry keeps it alive. Without that, it is collected, and an ordinary class
    built at its address — the very next one, in practice — was handed its derived class.
    """
    address = _collect_a_rebased_original()
    gc.collect()
    for index in range(500):
        ordinary = type(f"Ordinary{index}", (Exception,), {})
        dual = delivery_error(ordinary("SENTINEL"))
        reused = id(ordinary) == address
        assert isinstance(dual, ordinary), f"class {index} (reused address: {reused}) got another's"


def test_a_path_attribute_left_unset_is_not_copied() -> None:
    """An unset ``filename2`` stays unset, so ``str()`` does not render ``-> None``."""
    err = FileNotFoundError(2, "No such file or directory", "a.log")
    dual = delivery_error(err)
    assert dual is not None
    assert str(dual) == str(err)
    assert (dual.filename, dual.filename2) == ("a.log", None)


def test_the_corpus_ids_are_the_parametrize_ids() -> None:
    """Every fixture is either built or refused; a fixture in neither list would run nowhere."""
    assert sorted(BUILDS + REFUSES) == sorted(CORPUS)
    assert set(PICKLES) <= set(BUILDS)
