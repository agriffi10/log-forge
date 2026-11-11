"""A sink graph that contains itself is refused where it is formed, never recursed through.

Before this fix a ``MultiSink`` holding itself wrote one ``emit``'s batch to a sibling 998 times and
raised ``RecursionError`` from every ``flush()``; a ``FilteringSink`` or ``TransformSink`` holding
itself raised it from ``emit``, ``flush`` and ``close``. ``_sink_arg.loop_in`` finds such a loop
statically over the package wrappers' forwarding positions, and it is refused at the three places a
loop can be formed without touching a private attribute: a wrapper's ``__init__`` (re-run on an
existing wrapper, or a subclass passing ``self``), ``configure(sink=…)``, and ``_FactorySink``'s
build. Serves invariants 1 and 13 (no event duplicated or lost to the library's own recursion; a
sink argument that can never work is refused where it is written).
"""

from __future__ import annotations

import ast
import io
import pathlib
import sys
from typing import Any

import pytest

import log_foundry
from log_foundry import _fork, _sink_arg
from log_foundry.sinks._factory import _FactorySink
from log_foundry.sinks.base import SinkDeliveryError
from log_foundry.sinks.filtering import FilteringSink
from log_foundry.sinks.memory import MemorySink
from log_foundry.sinks.multi import MultiSink
from log_foundry.sinks.transform import TransformSink

_BATCH = [{"message": "m"}]


def _identity(event: dict[str, object]) -> dict[str, object]:
    return event


def _wrap(kind: str, inner: Any) -> Any:
    """Builds one package wrapper of ``kind`` around ``inner``."""
    if kind == "multi":
        return MultiSink(inner)
    if kind == "filtering":
        return FilteringSink(inner)
    return TransformSink(inner, _identity)


_CLASSES: dict[str, Any] = {
    "multi": MultiSink,
    "filtering": FilteringSink,
    "transform": TransformSink,
}


def _reinit(kind: str, wrapper: Any, inner: Any) -> None:
    """Runs ``kind``'s own ``__init__`` on ``wrapper`` with ``inner`` as its child: a public call."""
    if kind == "transform":
        TransformSink.__init__(wrapper, inner, _identity)
    else:
        _CLASSES[kind].__init__(wrapper, inner)


def _children(kind: str, wrapper: Any) -> Any:
    return wrapper._sinks if kind == "multi" else wrapper._inner


_NAMES = {
    "multi": r"MultiSink sinks\[0\]",
    "filtering": "FilteringSink inner",
    "transform": "TransformSink inner",
}
_KINDS = ["multi", "filtering", "transform"]


def _bare_multi(*children: Any) -> MultiSink:
    """A ``MultiSink`` holding ``children`` without its constructor, which would refuse a loop."""
    multi = MultiSink.__new__(MultiSink)
    MultiSink.__init__(multi)
    vars(multi)["_sinks"] = children
    return multi


# -- construction ----------------------------------------------------------------------------------


@pytest.mark.parametrize("kind", _KINDS)
def test_a_wrapper_refuses_itself_as_its_child_and_keeps_what_it_had(kind: str) -> None:
    """A re-run ``__init__`` handing the wrapper itself is refused before anything is stored."""
    mem = MemorySink()
    wrapper = _wrap(kind, mem)
    before = _children(kind, wrapper)
    with pytest.raises(ValueError, match=rf"^{_NAMES[kind]} leads back to this \w+Sink, so") as err:
        _reinit(kind, wrapper, wrapper)
    assert "recurse" in str(err.value)
    assert _children(kind, wrapper) is before, "the refused __init__ stored its child anyway"
    wrapper.emit(_BATCH)
    wrapper.flush()
    assert len(mem.events) == 1, "the refused wrapper no longer delivers exactly once"


@pytest.mark.parametrize("kind", _KINDS)
def test_a_subclass_passing_self_to_its_base_is_refused_by_its_own_name(kind: str) -> None:
    """``super().__init__(self, …)`` is the construction-time loop; the message names the subclass."""

    class Looping(_CLASSES[kind]):  # type: ignore[valid-type, misc]
        def __init__(self) -> None:
            _reinit(kind, self, self)

    with pytest.raises(ValueError, match=r"leads back to this Looping, so every call"):
        Looping()


@pytest.mark.parametrize("outer", _KINDS)
@pytest.mark.parametrize("middle", _KINDS)
def test_a_loop_through_another_wrapper_is_refused(outer: str, middle: str) -> None:
    """Every pairing of the three wrappers: outer re-initialised to hold a wrapper holding it."""
    mem = MemorySink()
    wrapper = _wrap(outer, mem)
    via = _wrap(middle, wrapper)
    with pytest.raises(ValueError, match=rf"^{_NAMES[outer]} leads back to this"):
        _reinit(outer, wrapper, via)


def test_a_loop_through_super_is_refused() -> None:
    """``super(Sub, inst)`` forwards every call to ``inst``, so the walk follows it."""

    class Sub(MultiSink):
        pass

    sub = Sub(MemorySink())
    with pytest.raises(ValueError, match=r"^MultiSink sinks\[0\] leads back to this Sub"):
        sub.__init__(super(Sub, sub))


def test_a_child_holding_a_loop_of_its_own_is_refused_by_the_looping_wrapper_name() -> None:
    """A child that already contains a loop is refused naming the wrapper on that loop."""
    looped = FilteringSink(MemorySink())
    vars(looped)["_inner"] = looped
    with pytest.raises(ValueError, match=r"^MultiSink sinks\[1\] holds a FilteringSink that leads"):
        MultiSink(MemorySink(), looped)


# -- configure(sink=…) -----------------------------------------------------------------------------


def test_configure_refuses_a_graph_containing_a_loop_and_keeps_its_config() -> None:
    """A loop formed by mutating a slot before ``configure()`` is refused there; nothing changes."""
    mem = MemorySink()
    multi = MultiSink(mem)
    multi._sinks = (multi, mem)  # type: ignore[assignment]
    log_foundry.configure(service="loops", sink=MemorySink())
    configured = log_foundry.get_config().sink
    with pytest.raises(ValueError, match=r"^sink holds a MultiSink that leads back to itself"):
        log_foundry.configure(sink=_bare_multi(MemorySink(), _bare_multi(multi)))
    with pytest.raises(ValueError, match=r"^sink holds a MultiSink that leads back to itself"):
        log_foundry.configure(sink=multi)
    assert log_foundry.get_config().sink is configured


# -- sink_factory ----------------------------------------------------------------------------------


def test_a_factory_wrapping_the_configured_sink_is_a_counted_failed_build() -> None:
    """``lambda: MultiSink(get_config().sink, …)`` would make every emit recurse; it is refused."""
    mem = MemorySink()
    log_foundry.configure(
        service="loops", sink_factory=lambda: MultiSink(log_foundry.get_config().sink, mem)
    )
    factory_sink = log_foundry.get_config().sink
    assert type(factory_sink) is _FactorySink
    buffer, saved = io.StringIO(), sys.stderr
    sys.stderr = buffer
    try:
        with pytest.raises(SinkDeliveryError, match="returned a sink that contains itself"):
            factory_sink.emit(_BATCH)
    finally:
        sys.stderr = saved
    assert factory_sink._inner is None, "the looped build was stored"
    assert mem.events == [], "the looped build was delivered to"
    assert "what it returned leads back to a _FactorySink" in buffer.getvalue()
    assert "1 build(s) failed so far" in buffer.getvalue()


def test_a_factory_returning_a_graph_with_its_own_loop_is_refused() -> None:
    """A built graph that loops without passing through the factory wrapper is refused too."""
    looped = TransformSink(MemorySink(), _identity)
    vars(looped)["_inner"] = looped
    factory_sink = _FactorySink(lambda: _bare_multi(looped))
    sys_err, sys.stderr = sys.stderr, io.StringIO()
    try:
        with pytest.raises(SinkDeliveryError, match="contains itself"):
            factory_sink.emit(_BATCH)
    finally:
        sys.stderr = sys_err
    assert factory_sink._inner is None


# -- what is not a loop ----------------------------------------------------------------------------


def test_a_diamond_and_a_repeated_child_are_not_loops() -> None:
    """A sink reached twice on *different* paths is a DAG, accepted and delivered to twice."""
    mem = MemorySink()
    diamond = MultiSink(mem, FilteringSink(mem), TransformSink(MultiSink(mem, mem), _identity))
    log_foundry.configure(service="loops", sink=diamond)
    diamond.emit(_BATCH)
    assert len(mem.events) == 4


def test_a_third_party_back_reference_is_never_refused() -> None:
    """A sink of the caller's own that keeps its parent, and never calls it, is not a loop."""

    class Parented:
        def __init__(self) -> None:
            self.events: list[dict[str, object]] = []
            self.parent: object = None

        def emit(self, batch: list[dict[str, object]]) -> None:
            self.events.extend(batch)

    child = Parented()
    parent = MultiSink(child)
    child.parent = parent
    log_foundry.configure(service="loops", sink=FilteringSink(parent))
    assert _sink_arg.loop_in(parent) is None
    parent.emit(_BATCH)
    assert child.events == _BATCH


# -- the walk's own bounds -------------------------------------------------------------------------


def test_the_walk_gives_up_at_its_ceiling_and_accepts(monkeypatch: pytest.MonkeyPatch) -> None:
    """Past ``_LOOP_CEILING`` visits the graph is accepted unexamined, which is the old behaviour."""
    bottom = FilteringSink(MemorySink())
    vars(bottom)["_inner"] = bottom
    top: Any = bottom
    for _ in range(4):
        above = MultiSink.__new__(MultiSink)
        vars(above)["_sinks"] = (top,)
        top = above
    assert _sink_arg.loop_in(top) is bottom
    monkeypatch.setattr(_sink_arg, "_LOOP_CEILING", 3)
    assert _sink_arg.loop_in(top) is None


def test_an_unreadable_wrapper_is_announced_and_its_children_accepted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A slot read that raises costs only the check beneath that node, announced on stderr."""
    looped = FilteringSink(MemorySink())
    vars(looped)["_inner"] = looped

    def boom(obj: object) -> None:
        raise RuntimeError("unreadable")

    monkeypatch.setattr(_sink_arg, "_instance_dict", boom)
    buffer, saved = io.StringIO(), sys.stderr
    sys.stderr = buffer
    try:
        assert _sink_arg.loop_in(looped) is None
    finally:
        sys.stderr = saved
    assert "reading a wrapper sink's children for a loop" in buffer.getvalue()
    assert "RuntimeError" in buffer.getvalue()


# -- every forwarding position, now and in a future wrapper -------------------------------------------


@pytest.mark.parametrize(
    ("key", "slot", "kind"),
    [
        (key, slot, kind)
        for key, slots in _fork._CHILD_SINK_SLOTS.items()
        if key[0].startswith("sinks.")
        for slot, kind in slots.items()
    ],
    ids=lambda value: value if isinstance(value, str) else ".".join(value),
)
def test_every_roster_slot_is_followed(key: tuple[str, str], slot: str, kind: str) -> None:
    """Each ``_CHILD_SINK_SLOTS`` sink position, set to hold its own wrapper, is found as a loop.

    The roster is held complete by ``test_child_sink_slots.py``, so a new package wrapper gains an
    entry there and is checked here without this file changing.
    """
    module = __import__(f"log_foundry.{key[0]}", fromlist=[key[1]])
    cls = getattr(module, key[1])
    holder = cls.__new__(cls)
    vars(holder)[slot] = holder if kind == "one" else (MemorySink(), holder)
    assert _sink_arg.loop_in(holder) is holder
    assert _sink_arg.loop_in(MemorySink(), back_to=holder) is None


def test_every_wrapper_constructor_passes_itself_as_the_owner() -> None:
    """Every ``require_sink`` call under ``sinks/`` names ``owner=self``; ``configure`` names none.

    Without ``owner`` a wrapper's constructor would still refuse a child that loops on its own, but
    not one that leads back to the wrapper being built — the re-run ``__init__`` and the subclass
    passing ``self``. Read from the source, so a new wrapper calling ``require_sink`` is held to it.
    """
    root = pathlib.Path(_sink_arg.__file__).parent
    calls: dict[str, list[ast.Call]] = {}
    for path in sorted(root.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "require_sink"
            ):
                calls.setdefault(str(path.relative_to(root)), []).append(node)
    wrappers = {name: found for name, found in calls.items() if name.startswith("sinks/")}
    assert set(wrappers) == {"sinks/multi.py", "sinks/filtering.py", "sinks/transform.py"}
    for name, found in wrappers.items():
        for call in found:
            owners = [kw.value for kw in call.keywords if kw.arg == "owner"]
            assert len(owners) == 1, f"{name}: require_sink without owner="
            assert isinstance(owners[0], ast.Name) and owners[0].id == "self", name
    assert [kw.arg for call in calls["config.py"] for kw in call.keywords] == []
