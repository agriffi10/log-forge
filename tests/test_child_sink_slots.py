"""SPEC-079: ownership is recorded by position where position is known, by shape only where not.

Three review frames each found a static shape predicate answering differently from
``isinstance`` and an ownership record changing with it. The mechanism that ends that class is
positional: a value the library was handed as a sink — the root of ``stamp``, a value in a package
wrapper's child-sink slot, a live delivery sink — is recorded whatever it looks like, because
recording runs none of its code. ``_fork._CHILD_SINK_SLOTS`` names the slots; the shape test,
``_fork._sink_shaped``, decides only inside holders this package does not define.

Two gates hold it:

- **The slot roster** derives every child-sink slot from the source and requires it in the
  table, both directions, with a fixture corpus asserting each reason.
- **A differential corpus** runs every recording site against every degenerate shape and
  asserts the record against base behaviour (``isinstance(x, Sink)``, as the walks asked it at
  ``9b238733``), each intended divergence named in the case.

Serves invariants 5 and 12.
"""

from __future__ import annotations

import ast
import functools
import pathlib
import re
import types
import weakref
from typing import Any

import pytest

import log_foundry
from log_foundry import _fork, _lifecycle, config
from log_foundry.sinks._factory import _FactorySink
from log_foundry.sinks.base import Sink
from log_foundry.sinks.filtering import FilteringSink
from log_foundry.sinks.memory import MemorySink
from log_foundry.sinks.multi import MultiSink
from log_foundry.sinks.transform import TransformSink

_SRC = pathlib.Path(log_foundry.__file__).resolve().parent

UNDECLARED_SLOT = "a package class holds a handed-over sink in a slot _CHILD_SINK_SLOTS lacks"
STALE_SLOT = "a _CHILD_SINK_SLOTS entry names no slot the source declares"

_ONE = re.compile(r"^Sink( \| None)?$")


def derived_slots(sources: dict[str, str]) -> dict[tuple[str, str], dict[str, str]]:
    """Every child-sink slot the sources declare, keyed as ``_fork._CHILD_SINK_SLOTS`` is.

    A slot is a ``self`` attribute assigned from an ``__init__`` parameter annotated ``Sink`` or
    ``Sink | None`` (``one``) or from a ``*args: Sink`` parameter (``many``), a ``self``
    attribute annotated ``Sink`` or ``Sink | None``, or a class-level field so annotated (a
    dataclass's). ``dict[int, Sink]`` and other containers of sinks are records, not slots.
    """
    found: dict[tuple[str, str], dict[str, str]] = {}
    for rel, source in sources.items():
        module = rel.removesuffix(".py").replace("/", ".")
        for cls in [n for n in ast.walk(ast.parse(source)) if isinstance(n, ast.ClassDef)]:
            slots: dict[str, str] = {}
            for node in cls.body:
                if (
                    isinstance(node, ast.AnnAssign)
                    and isinstance(node.target, ast.Name)
                    and _ONE.match(ast.unparse(node.annotation))
                ):
                    slots[node.target.id] = "one"
                if not isinstance(node, ast.FunctionDef):
                    continue
                params: dict[str, str] = {}
                if node.name == "__init__":
                    for arg in node.args.args + node.args.kwonlyargs:
                        if arg.annotation is not None and _ONE.match(ast.unparse(arg.annotation)):
                            params[arg.arg] = "one"
                    vararg = node.args.vararg
                    if (
                        vararg is not None
                        and vararg.annotation is not None
                        and ast.unparse(vararg.annotation) == "Sink"
                    ):
                        params[vararg.arg] = "many"
                for sub in ast.walk(node):
                    if (
                        isinstance(sub, ast.AnnAssign)
                        and isinstance(sub.target, ast.Attribute)
                        and ast.unparse(sub.target.value) == "self"
                        and _ONE.match(ast.unparse(sub.annotation))
                    ):
                        slots[sub.target.attr] = "one"
                    if isinstance(sub, ast.Assign | ast.AnnAssign) and isinstance(
                        sub.value, ast.Name
                    ):
                        targets = sub.targets if isinstance(sub, ast.Assign) else [sub.target]
                        for target in targets:
                            if (
                                isinstance(target, ast.Attribute)
                                and ast.unparse(target.value) == "self"
                                and sub.value.id in params
                            ):
                                slots[target.attr] = params[sub.value.id]
            if slots:
                found[(module, cls.name)] = slots
    return found


def slot_findings(
    sources: dict[str, str], table: dict[tuple[str, str], dict[str, str]]
) -> list[tuple[str, str]]:
    """The roster's refusals, each ``(reason, site)``."""
    derived = derived_slots(sources)
    findings: list[tuple[str, str]] = []
    for key, slots in sorted(derived.items()):
        for slot, kind in sorted(slots.items()):
            if table.get(key, {}).get(slot) != kind:
                findings.append((UNDECLARED_SLOT, f"{key[0]}.{key[1]}.{slot}:{kind}"))
    for key, slots in sorted(table.items()):
        for slot, kind in sorted(slots.items()):
            if derived.get(key, {}).get(slot) != kind:
                findings.append((STALE_SLOT, f"{key[0]}.{key[1]}.{slot}:{kind}"))
    return findings


def _src() -> dict[str, str]:
    return {
        str(p.relative_to(_SRC)): p.read_text(encoding="utf-8") for p in sorted(_SRC.rglob("*.py"))
    }


def test_the_slot_table_is_every_slot_the_source_declares() -> None:
    """Every child-sink slot in ``src/`` is in ``_CHILD_SINK_SLOTS`` with its kind, and no more."""
    assert slot_findings(_src(), _fork._CHILD_SINK_SLOTS) == []


_NEW_WRAPPER = (
    "class Mirror:\n"
    "    def __init__(self, inner: Sink, *, spare: Sink | None = None) -> None:\n"
    "        self._inner = inner\n"
    "        self._spare = spare\n"
)


@pytest.mark.parametrize(
    ("case", "sources", "table", "expected"),
    [
        (
            "a-new-wrapper",
            {"sinks/mirror.py": _NEW_WRAPPER},
            {("sinks.mirror", "Mirror"): {"_inner": "one"}},
            [(UNDECLARED_SLOT, "sinks.mirror.Mirror._spare:one")],
        ),
        (
            "a-stale-entry",
            {"sinks/mirror.py": _NEW_WRAPPER},
            {("sinks.mirror", "Mirror"): {"_inner": "one", "_spare": "one", "_gone": "one"}},
            [(STALE_SLOT, "sinks.mirror.Mirror._gone:one")],
        ),
        (
            "a-wrong-kind",
            {
                "sinks/fan.py": "class Fan:\n    def __init__(self, *sinks: Sink):\n        self._s = sinks\n"
            },
            {("sinks.fan", "Fan"): {"_s": "one"}},
            [
                (UNDECLARED_SLOT, "sinks.fan.Fan._s:many"),
                (STALE_SLOT, "sinks.fan.Fan._s:one"),
            ],
        ),
    ],
    ids=lambda v: v if isinstance(v, str) else "",
)
def test_the_slot_roster_refuses_its_shapes(
    case: str, sources: dict[str, str], table: Any, expected: list[tuple[str, str]]
) -> None:
    """Each fixture fails naming its slot and its reason."""
    assert slot_findings(sources, table) == expected


def test_the_slot_roster_is_silent_on_what_is_not_a_slot() -> None:
    """A ``Sink`` parameter not stored, a record of sinks, and a ``SinkLosses`` are not slots."""
    source = (
        "class Quiet:\n"
        "    owed: dict[int, Sink] = {}\n"
        "    def __init__(self, inner: Sink, losses: SinkLosses) -> None:\n"
        "        use(inner)\n"
        "        self.losses = losses\n"
    )
    assert slot_findings({"q.py": source}, {}) == []


# -- the differential corpus -----------------------------------------------------------------------


class _Rec:
    """A sink counting its closes."""

    def __init__(self) -> None:
        self.closes = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Accepts the batch."""

    def close(self) -> None:
        """Counts the close."""
        self.closes += 1


class _CallableRec(_Rec):
    def __call__(self) -> None:
        """Makes ``weakref.proxy`` build a ``CallableProxyType``."""


class _Sub(_Rec):
    pass


class _Slotted:
    __slots__ = ("closes",)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Accepts the batch."""

    def close(self) -> None:
        """Closes nothing."""


class _Getattr:
    def __init__(self, inner: object) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class _Liar:
    @property  # type: ignore[misc]
    def __class__(self) -> type:  # type: ignore[override]
        return _Rec


class _TP:
    """A third-party wrapper: the one place position is unknown."""

    def __init__(self, inner: object) -> None:
        self.inner = inner

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Accepts the batch."""

    def close(self) -> None:
        """Closes nothing."""


_KEEP: list[object] = []


def _kept(value: Any) -> Any:
    _KEEP.append(value)
    return value


def _module() -> types.ModuleType:
    module = types.ModuleType("a_module_sink")
    module.emit = lambda batch: None  # type: ignore[attr-defined]
    module.close = lambda: None  # type: ignore[attr-defined]
    return module


_SHAPES: dict[str, Any] = {
    "weakref.proxy": lambda: weakref.proxy(_kept(_Rec())),
    "CallableProxyType": lambda: weakref.proxy(_kept(_CallableRec())),
    "getattr-proxy": lambda: _Getattr(_Rec()),
    "super": lambda: super(_Sub, _kept(_Sub())),
    "slotted": _Slotted,
    "lying-class": _Liar,
    "class": lambda: _Rec,
    "module": _module,
    "bound-method": lambda: _kept(_Rec()).emit,
    "partial": lambda: functools.partial(print),
    "int": lambda: 123456789,
}

_MARKING_DIVERGES = {
    "class": "a class is never an instance the library could close (head records none)",
    "lying-class": "isinstance trusted the lying __class__; the object has no emit to close",
}
"""Where the third-party marking deliberately answers differently from base."""


def _base(value: object) -> bool:
    """What base's walks asked: ``isinstance(x, Sink)`` (the marking also skipped modules)."""
    return isinstance(value, Sink)


def _recorded(value: object) -> bool:
    with _lifecycle._state._owned_lock:
        return id(value) in _lifecycle._state._owned


@pytest.mark.parametrize("shape", list(_SHAPES))
def test_stamp_records_the_root_it_is_handed(shape: str) -> None:
    """Site: ``stamp``'s root. Recorded for every shape: being handed it declares it a sink.

    Base recorded it only when ``isinstance`` held; the divergence is the point (position).
    """
    value = _SHAPES[shape]()
    _lifecycle.stamp(value)
    assert _recorded(value)


_WRAPPERS: dict[str, Any] = {
    "MultiSink": lambda x: MultiSink(MemorySink(), x),
    "FilteringSink": FilteringSink,
    "TransformSink": lambda x: TransformSink(x, lambda e: e),
    "_FactorySink": None,
}


def _wrapped(kind: str, value: object) -> object:
    if kind == "_FactorySink":
        wrapper = _FactorySink(MemorySink)
        vars(wrapper)["_inner"] = value
        return wrapper
    return _WRAPPERS[kind](value)


@pytest.mark.parametrize("kind", list(_WRAPPERS))
@pytest.mark.parametrize("shape", list(_SHAPES))
def test_a_package_wrapper_slot_records_what_it_holds(kind: str, shape: str) -> None:
    """Site: a package wrapper's child-sink slot. Recorded for every shape it accepts.

    Base recorded a member only when ``isinstance`` held; by position every accepted member is
    recorded. An ``int`` is refused by ``require_sink`` before anything is stored, except in the
    factory wrapper's slot, which only the wrapper's own build fills.
    """
    value = _SHAPES[shape]()
    if shape == "int" and kind != "_FactorySink":
        with pytest.raises(TypeError, match="must be a sink with an emit method"):
            _wrapped(kind, value)
        return
    wrapper = _wrapped(kind, value)
    _lifecycle.stamp(wrapper)
    assert _recorded(value), f"{kind} did not record its {shape} member"


@pytest.mark.parametrize("shape", list(_SHAPES))
def test_the_live_sinks_include_the_configured_sink(shape: str) -> None:
    """Site: ``_live_sinks``. The configured sink is a live delivery sink for every shape."""
    value = _SHAPES[shape]()
    config._rebind(sink=value)
    assert any(sink is value for sink in _lifecycle._live_sinks())


@pytest.mark.parametrize("shape", list(_SHAPES))
def test_the_marking_records_by_shape_inside_a_third_party_holder(shape: str) -> None:
    """Site: the marking frontier inside a holder this package does not define. By shape.

    Expected: base's answer, ``isinstance(x, Sink)`` with modules skipped, except where named in
    ``_MARKING_DIVERGES``. A ``weakref`` proxy matches base because the marking records one by
    its exact type.
    """
    value = _SHAPES[shape]()
    config._rebind(sink=_TP(value))
    _lifecycle._walk_after_fork(_fork._Repair())
    expected = _base(value) and not isinstance(value, types.ModuleType)
    if shape in _MARKING_DIVERGES:
        expected = not expected
    assert _recorded(value) is expected, _MARKING_DIVERGES.get(shape, "agrees with base")


@pytest.mark.parametrize("shape", [shape for shape in _SHAPES if shape != "int"])
def test_the_marking_records_by_position_inside_a_package_wrapper_it_reaches(shape: str) -> None:
    """Site: a package wrapper the marking frontier reaches behind a third-party holder.

    Position is known again once the walk is inside a package wrapper, so its member is recorded
    for every shape it accepts, as on the repair frontier. Base recorded it only when
    ``isinstance`` held.
    """
    value = _SHAPES[shape]()
    config._rebind(sink=_TP(MultiSink(MemorySink(), value)))
    _lifecycle._walk_after_fork(_fork._Repair())
    assert _recorded(value), f"the marking did not record a {shape} member by position"


# -- the leaks the positional record closes ---------------------------------------------------------


@pytest.mark.parametrize("kind", ["MultiSink", "FilteringSink"])
@pytest.mark.parametrize("shape", ["weakref.proxy", "getattr-proxy", "super"])
def test_a_wrapper_closes_a_proxy_shaped_member_on_swap_and_shutdown(kind: str, shape: str) -> None:
    """A proxy-shaped member of a package wrapper is closed once by the process that configured it.

    At base the ``__getattr__`` proxy and ``super`` shapes were never closed (unrecorded, so the
    wrapper refused them), and at the first static-read build neither was the ``weakref`` proxy.
    """
    target = _Sub() if shape == "super" else _Rec()
    _KEEP.append(target)
    value: Any = {
        "weakref.proxy": lambda: weakref.proxy(target),
        "getattr-proxy": lambda: _Getattr(target),
        "super": lambda: super(_Sub, target),
    }[shape]()
    wrapper = MultiSink(value) if kind == "MultiSink" else FilteringSink(value)
    log_foundry.configure(service="slots", sink=wrapper)
    log_foundry.info("one")
    log_foundry.configure(sink=MemorySink())
    log_foundry.flush(timeout=5.0)
    log_foundry.shutdown(3.0)
    assert target.closes == 1
