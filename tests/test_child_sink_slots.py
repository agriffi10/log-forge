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

UNDECLARED_HOLDER = "a package class takes ownership of a sink but has no _CHILD_SINK_SLOTS entry"
UNDECLARED_SLOT = "a package class stores a handed-over sink in a slot its entry lacks"
STALE_ENTRY = "a _CHILD_SINK_SLOTS entry names a class that neither stores nor owns a sink"
STALE_EXEMPTION = "a _NOT_CHILD_SINK_SLOTS exemption names a slot the source no longer stores"
ADVICE = (
    "add {key!r}: {{{slot!r}: 'one' or 'many'}} to _fork._CHILD_SINK_SLOTS, a builder to "
    "_BUILDERS in tests/test_child_sink_slots.py, or an exemption with its reason to "
    "_NOT_CHILD_SINK_SLOTS"
)

_NOT_CHILD_SINK_SLOTS: dict[tuple[str, str], dict[str, str]] = {
    ("_lifecycle", "_CloseRegistry"): {
        "owed": "the owed-close record: a record of sinks, never a wrapper's child; it seeds "
        "the marking frontier and is never walked as a holder",
    },
    ("config", "Config"): {
        "sink_factory": "a callable that builds a sink, never a sink; its built sink is recorded "
        "from the factory wrapper's `_inner`",
    },
    ("sinks._factory", "_FactorySink"): {
        "_factory": "the caller's factory, a callable, never a sink",
    },
    ("sinks.filtering", "FilteringSink"): {
        "log_foundry_counters": "the inner sink's `SinkCounters`, derived from it, never a sink",
    },
    ("sinks.transform", "TransformSink"): {
        "log_foundry_counters": "the inner sink's `SinkCounters`, derived from it, never a sink",
    },
    ("worker", "Worker"): {
        "_held": "sinks the drain thread may still be inside, superseded ones among them; in "
        "Worker._FORK_SKIP so a superseded sink is never hooked",
    },
}
"""Slots that store sinks but are records, not a wrapper's children, each with its reason."""

_OWNERSHIP_CALLS = ("require_sink",)
"""Calls whose presence in a class's code means it takes a sink as its own child."""


def _mentions_sink(annotation: ast.expr | None) -> bool:
    """Whether an annotation names ``Sink`` anywhere: bare, qualified, quoted or inside a generic."""
    if annotation is None:
        return False
    for node in ast.walk(annotation):
        if isinstance(node, ast.Name) and node.id == "Sink":
            return True
        if isinstance(node, ast.Attribute) and node.attr == "Sink":
            return True
        if (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and re.search(r"\bSink\b", node.value)
        ):
            return True
    return False


def _module_key(rel: str) -> str:
    """The module a source file defines, relative to the package: a package's ``__init__`` is the package."""
    dotted = rel.removesuffix(".py").replace("/", ".")
    return dotted.removesuffix(".__init__") if dotted.endswith(".__init__") else dotted


def _classes(tree: ast.Module) -> list[tuple[str, ast.ClassDef]]:
    """Every class with its qualified name, nested classes as ``Outer.Inner``."""
    found: list[tuple[str, ast.ClassDef]] = []

    def visit(body: list[ast.stmt], prefix: str) -> None:
        for node in body:
            if isinstance(node, ast.ClassDef):
                qual = f"{prefix}{node.name}"
                found.append((qual, node))
                visit(node.body, f"{qual}.")

    visit(tree.body, "")
    return found


def _self_targets(node: ast.AST) -> list[tuple[str, ast.expr | None]]:
    """Each ``self`` attribute a statement stores, with the value expression stored into it."""
    stores: list[tuple[str, ast.expr | None]] = []
    if isinstance(node, ast.Assign | ast.AnnAssign):
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        for target in targets:
            if isinstance(target, ast.Tuple) and isinstance(node.value, ast.Tuple):
                pairs = list(zip(target.elts, node.value.elts, strict=False))
            else:
                pairs = [(target, node.value)]
            for single, value in pairs:
                if isinstance(single, ast.Attribute) and ast.unparse(single.value) == "self":
                    stores.append((single.attr, value))
    if (
        isinstance(node, ast.Call)
        and ast.unparse(node.func) in {"object.__setattr__", "setattr"}
        and len(node.args) == 3
        and ast.unparse(node.args[0]) == "self"
        and isinstance(node.args[1], ast.Constant)
        and isinstance(node.args[1].value, str)
    ):
        stores.append((node.args[1].value, node.args[2]))
    return stores


def derived_holders(sources: dict[str, str]) -> dict[tuple[str, str], tuple[set[str], bool]]:
    """Each package class that stores a handed-over sink or takes ownership of one.

    A slot is a ``self`` attribute — assigned, annotated, unpacked or set through
    ``object.__setattr__`` — whose value expression uses a parameter, of the method storing it,
    annotated with ``Sink`` anywhere in its annotation (``Sink``, ``Optional[Sink]``,
    ``Sequence[Sink]``, ``dict[str, Sink]``, ``'Sink'``, ``base.Sink``), or a ``self`` attribute or
    class-level field so annotated. The behaviour flag is whether the class's code calls
    ``require_sink`` or releases a sink with ``owner=self``.

    Returns:
      ``(module, qualname)`` to ``(slot names, takes ownership)``.
    """
    holders: dict[tuple[str, str], tuple[set[str], bool]] = {}
    for rel, source in sources.items():
        module = _module_key(rel)
        for qual, cls in _classes(ast.parse(source)):
            slots: set[str] = set()
            owns = False
            for node in cls.body:
                if (
                    isinstance(node, ast.AnnAssign)
                    and isinstance(node.target, ast.Name)
                    and _mentions_sink(node.annotation)
                ):
                    slots.add(node.target.id)
                if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                    continue
                every = node.args.posonlyargs + node.args.args + node.args.kwonlyargs
                params = {a.arg for a in every if _mentions_sink(a.annotation)}
                for extra in (node.args.vararg, node.args.kwarg):
                    if extra is not None and _mentions_sink(extra.annotation):
                        params.add(extra.arg)
                for sub in ast.walk(node):
                    if isinstance(sub, ast.Call):
                        name = ast.unparse(sub.func).rsplit(".", 1)[-1]
                        if name in _OWNERSHIP_CALLS or (
                            name == "release"
                            and any(
                                k.arg == "owner" and ast.unparse(k.value) == "self"
                                for k in sub.keywords
                            )
                        ):
                            owns = True
                    if (
                        isinstance(sub, ast.AnnAssign)
                        and isinstance(sub.target, ast.Attribute)
                        and ast.unparse(sub.target.value) == "self"
                        and _mentions_sink(sub.annotation)
                    ):
                        slots.add(sub.target.attr)
                    for attr, value in _self_targets(sub):
                        used = (
                            {n.id for n in ast.walk(value) if isinstance(n, ast.Name)}
                            if value
                            else set()
                        )
                        if used & params:
                            slots.add(attr)
            if slots or owns:
                holders[(module, qual)] = (slots, owns)
    return holders


def slot_findings(
    sources: dict[str, str],
    table: dict[tuple[str, str], dict[str, str]],
    exempt: dict[tuple[str, str], dict[str, str]] | None = None,
) -> list[tuple[str, str]]:
    """The static gate's refusals, each ``(reason, advice naming the class and slot)``."""
    exempt = _NOT_CHILD_SINK_SLOTS if exempt is None else exempt
    holders = derived_holders(sources)
    findings: list[tuple[str, str]] = []
    for key, (slots, owns) in sorted(holders.items()):
        open_slots = sorted(slots - set(exempt.get(key, {})))
        if key not in table:
            if open_slots or owns:
                slot = open_slots[0] if open_slots else "<the slot>"
                findings.append((UNDECLARED_HOLDER, ADVICE.format(key=key, slot=slot)))
            continue
        findings.extend(
            (UNDECLARED_SLOT, ADVICE.format(key=key, slot=slot))
            for slot in open_slots
            if slot not in table[key]
        )
    findings.extend(
        (STALE_ENTRY, f"{key!r}: remove it from _fork._CHILD_SINK_SLOTS")
        for key in sorted(table)
        if key not in holders
    )
    modules = {_module_key(rel) for rel in sources}
    for key, slots in sorted(exempt.items()):
        if key[0] not in modules:
            continue
        stored = holders.get(key, (set(), False))[0]
        findings.extend(
            (STALE_EXEMPTION, f"{key!r}.{slot}: remove it from _NOT_CHILD_SINK_SLOTS")
            for slot in sorted(slots)
            if slot not in stored
        )
    return findings


def _src() -> dict[str, str]:
    return {
        str(p.relative_to(_SRC)): p.read_text(encoding="utf-8") for p in sorted(_SRC.rglob("*.py"))
    }


def test_the_slot_table_covers_every_class_that_stores_or_owns_a_sink() -> None:
    """Every package class that stores a handed-over sink or takes ownership of one is declared."""
    assert slot_findings(_src(), _fork._CHILD_SINK_SLOTS) == []


def test_every_table_key_is_the_imported_class_module_and_qualname() -> None:
    """Each key names an importable class by its own ``__module__`` and ``__qualname__``."""
    import importlib

    for module, qualname in _fork._CHILD_SINK_SLOTS:
        imported = importlib.import_module(f"{log_foundry.__name__}.{module}")
        cls: Any = imported
        for part in qualname.split("."):
            cls = getattr(cls, part)
        assert cls.__module__ == f"{log_foundry.__name__}.{module}", (module, qualname)
        assert cls.__qualname__ == qualname, (module, qualname)


_HDR = "from collections.abc import Sequence\nfrom typing import Optional\n"

_MUST_FIRE: dict[str, tuple[str, str]] = {
    "init-inner": (
        "class Mirror:\n    def __init__(self, inner: Sink) -> None:\n        self._inner = inner\n",
        UNDECLARED_HOLDER,
    ),
    "varargs-list": (
        "class Fan:\n    def __init__(self, *targets: Sink) -> None:\n        self._t = list(targets)\n",
        UNDECLARED_HOLDER,
    ),
    "varargs-tuple": (
        "class Fan:\n    def __init__(self, *targets: Sink) -> None:\n        self._t = tuple(targets)\n",
        UNDECLARED_HOLDER,
    ),
    "annotated-self": (
        "class Lazy:\n    def __init__(self) -> None:\n        self._f: Sink | None = None\n",
        UNDECLARED_HOLDER,
    ),
    "optional": (
        "class Lazy:\n    def __init__(self, inner: Optional[Sink]) -> None:\n        self._inner = inner\n",
        UNDECLARED_HOLDER,
    ),
    "none-or-sink": (
        "class Lazy:\n    def __init__(self, inner: None | Sink) -> None:\n        self._inner = inner\n",
        UNDECLARED_HOLDER,
    ),
    "sequence": (
        "class Fan:\n    def __init__(self, sinks: Sequence[Sink]) -> None:\n        self._s = sinks\n",
        UNDECLARED_HOLDER,
    ),
    "sequence-tuple": (
        "class Fan:\n    def __init__(self, sinks: Sequence[Sink]) -> None:\n        self._s = tuple(sinks)\n",
        UNDECLARED_HOLDER,
    ),
    "list-annotation": (
        "class Fan:\n    def __init__(self, sinks: list[Sink]) -> None:\n        self._s: list[Sink] = sinks\n",
        UNDECLARED_HOLDER,
    ),
    "dict": (
        "class Router:\n    def __init__(self, routes: dict[str, Sink]) -> None:\n        self._r = routes\n",
        UNDECLARED_HOLDER,
    ),
    "require-sink-call": (
        "class Guard:\n    def __init__(self, inner):\n        self._inner = require_sink(inner, 'inner')\n",
        UNDECLARED_HOLDER,
    ),
    "release-owner-self": (
        "class Owner:\n    def close(self):\n        _lifecycle.release(self._t, owner=self)\n",
        UNDECLARED_HOLDER,
    ),
    "method-attach": (
        "class Late:\n    def attach(self, sink: Sink) -> None:\n        self._extra = sink\n",
        UNDECLARED_HOLDER,
    ),
    "object-setattr": (
        "class Frozen:\n    def __init__(self, inner: Sink) -> None:\n        object.__setattr__(self, '_inner', inner)\n",
        UNDECLARED_HOLDER,
    ),
    "nested-class": (
        "class Outer:\n    class Inner:\n        def __init__(self, inner: Sink) -> None:\n            self._inner = inner\n",
        UNDECLARED_HOLDER,
    ),
    "quoted": (
        "class Q:\n    def __init__(self, inner: 'Sink') -> None:\n        self._inner = inner\n",
        UNDECLARED_HOLDER,
    ),
    "qualified": (
        "class Q:\n    def __init__(self, inner: base.Sink) -> None:\n        self._inner = inner\n",
        UNDECLARED_HOLDER,
    ),
    "tuple-unpack": (
        "class Pair:\n    def __init__(self, a: Sink, b: Sink) -> None:\n        self._a, self._b = a, b\n",
        UNDECLARED_HOLDER,
    ),
    "class-field": ("class Rec:\n    sink: Sink | None = None\n", UNDECLARED_HOLDER),
}

_MUST_STAY_SILENT: dict[str, str] = {
    "non-sink-attribute": "class Plain:\n    def __init__(self, name: str) -> None:\n        self._name = name\n",
    "pass-through": "class Pass:\n    def __init__(self, inner: Sink) -> None:\n        use(inner)\n",
    "a-function": "def _helper2(inner: Sink) -> None:\n    pass\n",
    "non-self-target": "class L:\n    def __init__(self, inner: Sink) -> None:\n        other = object()\n        other.x = inner\n",
    "other-method-store": "class M:\n    def __init__(self, inner: Sink) -> None:\n        self._n = 0\n    def f(self, inner: int) -> None:\n        self._inner = inner\n",
    "release-without-owner": "class R:\n    def close(self):\n        _lifecycle.release(self._t)\n",
    "a-sink-losses": "class S:\n    def __init__(self, losses: SinkLosses) -> None:\n        self._l = losses\n",
}


@pytest.mark.parametrize("case", list(_MUST_FIRE))
def test_the_slot_gate_fires_on_every_natural_new_holder(case: str) -> None:
    """A new holder written any natural way fails, with advice naming what to add."""
    source, reason = _MUST_FIRE[case]
    findings = slot_findings({"sinks/zz_future.py": _HDR + source}, {}, {})
    assert [r for r, _ in findings] == [reason], findings
    assert "_CHILD_SINK_SLOTS" in findings[0][1]


@pytest.mark.parametrize("case", list(_MUST_STAY_SILENT))
def test_the_slot_gate_is_silent_on_what_holds_no_sink(case: str) -> None:
    """A non-sink attribute, a pass-through, a function, a non-self target: no finding."""
    assert slot_findings({"sinks/zz_future.py": _HDR + _MUST_STAY_SILENT[case]}, {}, {}) == []


_MIRROR = "class Mirror:\n    def __init__(self, inner: Sink, spare: Sink) -> None:\n        self._inner = inner\n        self._spare = spare\n"


@pytest.mark.parametrize(
    ("case", "sources", "table", "exempt", "expected"),
    [
        (
            "a-declared-class-gains-a-slot",
            {"sinks/mirror.py": _MIRROR},
            {("sinks.mirror", "Mirror"): {"_inner": "one"}},
            {},
            UNDECLARED_SLOT,
        ),
        (
            "an-entry-for-a-class-that-holds-nothing",
            {"sinks/mirror.py": "class Mirror:\n    pass\n"},
            {("sinks.mirror", "Mirror"): {"_inner": "one"}},
            {},
            STALE_ENTRY,
        ),
        (
            "an-exemption-for-a-slot-no-longer-stored",
            {"sinks/mirror.py": _MIRROR},
            {("sinks.mirror", "Mirror"): {"_inner": "one", "_spare": "one"}},
            {("sinks.mirror", "Mirror"): {"_gone": "reason"}},
            STALE_EXEMPTION,
        ),
        (
            "a-package-init-is-keyed-as-the-package",
            {"sinks/__init__.py": _MIRROR},
            {("sinks.__init__", "Mirror"): {"_inner": "one", "_spare": "one"}},
            {},
            UNDECLARED_HOLDER,
        ),
    ],
    ids=lambda v: v if isinstance(v, str) else "",
)
def test_each_slot_gate_check_refuses_its_shape(
    case: str, sources: Any, table: Any, exempt: Any, expected: str
) -> None:
    """Each static check fires on its own fixture, asserted by its reason."""
    assert expected in {r for r, _ in slot_findings(sources, table, exempt)}


def test_a_nested_class_and_a_package_init_are_keyed_as_they_import() -> None:
    """Keys follow the runtime: ``Outer.Inner`` for a nested class, the package for ``__init__``."""
    nested = (
        "class Outer:\n    class Inner:\n"
        "        def __init__(self, inner: Sink) -> None:\n            self._inner = inner\n"
    )
    assert (
        slot_findings({"sinks/x.py": nested}, {("sinks.x", "Outer.Inner"): {"_inner": "one"}}, {})
        == []
    )
    assert (
        slot_findings(
            {"sinks/__init__.py": _MIRROR},
            {("sinks", "Mirror"): {"_inner": "one", "_spare": "one"}},
            {},
        )
        == []
    )


def test_a_declared_and_exempted_holder_is_silent() -> None:
    """Silence for the declared shape, so each check above is seen to fire for its reason only."""
    sources = {"sinks/mirror.py": _MIRROR}
    table = {("sinks.mirror", "Mirror"): {"_inner": "one"}}
    exempt = {("sinks.mirror", "Mirror"): {"_spare": "a record"}}
    assert slot_findings(sources, table, exempt) == []


# -- the runtime differential: each declared slot records a sentinel, through the real constructor --


class _Sentinel:
    """A ``__getattr__`` proxy over a sink: no static read finds it sink-shaped."""

    def __init__(self) -> None:
        self._target = MemorySink()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._target, name)


def _build_factory(x: object) -> object:
    """The factory wrapper's build refuses a non-``Sink``, so its slot is filled as a build leaves it."""
    wrapper = _FactorySink(MemorySink)
    vars(wrapper)["_inner"] = x
    return wrapper


def _build_worker(x: object) -> object:
    from log_foundry.worker import Worker

    return Worker(x)  # type: ignore[arg-type]


def _build_swap_outcome(x: object) -> object:
    from log_foundry.worker import SwapOutcome

    fields = {f.name: None for f in __import__("dataclasses").fields(SwapOutcome)}
    fields["previous"] = x
    return SwapOutcome(**fields)


def _build_logstash(x: object) -> object:
    from test_sink_ownership import _logstash_wrapping

    return _logstash_wrapping(x)


def _build_sentry(x: object) -> object:
    from test_sink_ownership import _sentry_wrapping

    return _sentry_wrapping(x)


_BUILDERS: dict[tuple[str, str], Any] = {
    ("config", "Config"): lambda x: config.Config(sink=x),
    ("worker", "Worker"): _build_worker,
    ("worker", "SwapOutcome"): _build_swap_outcome,
    ("sinks.multi", "MultiSink"): lambda x: MultiSink(MemorySink(), x),
    ("sinks.filtering", "FilteringSink"): FilteringSink,
    ("sinks.transform", "TransformSink"): lambda x: TransformSink(x, lambda e: e),
    ("sinks._factory", "_FactorySink"): _build_factory,
    ("sinks.logstash", "LogstashSink"): _build_logstash,
    ("sinks.sentry", "SentrySink"): _build_sentry,
}
"""How to build each declared class around a sentinel sink, with its real constructor where it
takes one (the two that build their HTTP fallback themselves have it replaced, as the
ownership tests do)."""


def test_every_declared_class_has_a_builder() -> None:
    """A table entry with no builder is a slot nothing checks at run time."""
    assert set(_BUILDERS) == set(_fork._CHILD_SINK_SLOTS)


@pytest.mark.parametrize("key", sorted(_fork._CHILD_SINK_SLOTS), ids=lambda k: ".".join(k))
def test_each_declared_slot_records_a_sentinel(key: tuple[str, str]) -> None:
    """Building the class around a sentinel and stamping it records the sentinel: name and kind."""
    sentinel = _Sentinel()
    holder = _BUILDERS[key](sentinel)
    try:
        _lifecycle.stamp(holder)
        assert _recorded(sentinel), f"{key}: its declared slot did not record the sentinel"
    finally:
        stop = getattr(holder, "stop", None)
        if key == ("worker", "Worker") and stop is not None:
            stop(timeout=2.0)


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


class _MyMulti(MultiSink):
    """A user subclass: its slot is found through the MRO, not on the exact type."""


class _MyFilter(FilteringSink):
    """A user subclass of a single-child wrapper."""


_WRAPPERS: dict[str, Any] = {
    "MultiSink": lambda x: MultiSink(MemorySink(), x),
    "MyMulti": lambda x: _MyMulti(MemorySink(), x),
    "FilteringSink": FilteringSink,
    "MyFilter": _MyFilter,
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
    recorded. An ``int`` is refused by ``require_sink`` before anything is stored; planted in the
    factory wrapper's slot, which no constructor checks, it is skipped by the same test.
    """
    value = _SHAPES[shape]()
    if shape == "int" and kind != "_FactorySink":
        with pytest.raises(TypeError, match="must be a sink with an emit method"):
            _wrapped(kind, value)
        return
    wrapper = _wrapped(kind, value)
    _lifecycle.stamp(wrapper)
    if shape == "int":
        assert not _recorded(value), "a value require_sink would refuse is never recorded"
        return
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


def test_a_value_assigned_into_a_slot_that_can_never_be_a_sink_is_not_recorded_or_closed() -> None:
    """A builtin assigned into a slot after construction is skipped, as ``require_sink`` would.

    Recording ``5`` or ``7`` made ``shutdown()`` try to close it and write a close-failure line.
    """
    import io
    import sys

    a = _Rec()
    multi = MultiSink(a)
    multi._sinks = (a, 5)  # type: ignore[assignment]
    filtering = FilteringSink(_Rec())
    filtering._inner = 7  # type: ignore[assignment]
    log_foundry.configure(service="slots", sink=MultiSink(multi, filtering))
    assert not _recorded(5) and not _recorded(7)
    buffer = io.StringIO()
    saved = sys.stderr
    sys.stderr = buffer
    try:
        log_foundry.info("one")
        log_foundry.shutdown(3.0)
    finally:
        sys.stderr = saved
    assert "closing" not in buffer.getvalue(), buffer.getvalue()
    assert a.closes == 1


def test_a_multisink_closes_a_member_held_twice_once() -> None:
    """``MultiSink(a, a)`` closes ``a`` once, by identity (invariant 5)."""
    a = _Rec()
    proxied = weakref.proxy(_kept(_Rec()))
    log_foundry.configure(service="slots", sink=MultiSink(a, a, proxied, proxied))
    log_foundry.info("one")
    log_foundry.shutdown(3.0)
    assert a.closes == 1
    assert _KEEP[-1].closes == 1
