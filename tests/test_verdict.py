"""SPEC-072 FR-001: the ``Verdict`` contract, ``SinkCounters`` and the ``read_deliver`` probe.

Serves invariants 7, 10 and 13: ``deliver`` returning a ``Verdict`` is invariant 7's second form,
every public name added only grows the surface, and a malformed ``Verdict`` is refused where it is
built, its type named by its stored name and never by a value.
"""

from __future__ import annotations

import io
import sys
import threading
from typing import Any, ClassVar

import pytest

import log_foundry
from log_foundry import Outcome, Sink, SinkCounters, SinkLosses, Verdict, read_deliver
from log_foundry.sinks import verdict as verdict_mod

CANARY = "CANARY-secret-value"


def _claiming(claimed: str, calls: list[str], base: type = object) -> type:
    """A class built by a metaclass whose ``__name__`` property claims ``claimed`` and records."""

    def name(cls: type) -> str:
        calls.append("__name__")
        return claimed

    meta = type("_ClaimingMeta", (type,), {"__name__": property(name)})
    return meta("RealStoredName", (base,), {})


class _EngineSink:
    """The minimal sink that offers ``deliver``: a class-defined function and exact counters."""

    def __init__(self) -> None:
        self.log_foundry_counters = SinkCounters()
        self.delivered: list[list[dict[str, object]]] = []

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        self.delivered.append(batch)
        return Verdict.uniform(Outcome.ACCEPTED, len(batch))

    def emit(self, batch: list[dict[str, object]]) -> None:
        raise AssertionError("not reached by these tests")

    def close(self) -> None:
        pass


# --- construction ---------------------------------------------------------------------------


def test_a_verdict_constructs_by_keyword_and_stores_a_list_as_a_tuple() -> None:
    """FR-001 AC1: keyword construction, a list stored as the exact tuple of its members."""
    verdict = Verdict(outcomes=(Outcome.ACCEPTED, Outcome.RETRYABLE), retry_after=2.0)
    assert verdict.outcomes == (Outcome.ACCEPTED, Outcome.RETRYABLE)
    assert verdict.retry_after == 2.0
    listed = Verdict(outcomes=[Outcome.REJECTED, Outcome.DROPPED])  # type: ignore[arg-type]
    assert type(listed.outcomes) is tuple
    assert listed.outcomes == (Outcome.REJECTED, Outcome.DROPPED)
    assert Verdict(outcomes=()).outcomes == ()
    assert Verdict(outcomes=[]).outcomes == ()  # type: ignore[arg-type]


def test_uniform_builds_n_copies_of_one_outcome() -> None:
    """FR-001 AC1: ``uniform(REJECTED, 3)`` is three ``REJECTED``; ``True`` counts as one."""
    assert Verdict.uniform(Outcome.REJECTED, 3).outcomes == (Outcome.REJECTED,) * 3
    assert Verdict.uniform(Outcome.ACCEPTED, 0).outcomes == ()
    assert Verdict.uniform(Outcome.UNKNOWN, True).outcomes == (Outcome.UNKNOWN,)
    assert Verdict.uniform(Outcome.RETRYABLE, 2, retry_after=1.5).retry_after == 1.5


def test_positional_construction_is_refused_by_its_message() -> None:
    """FR-001 AC1, on the message: a keyword typo is the other ``TypeError``."""
    with pytest.raises(TypeError, match="positional argument"):
        Verdict((Outcome.ACCEPTED,))  # type: ignore[misc]
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        Verdict(outcome=(Outcome.ACCEPTED,))  # type: ignore[call-arg]


def test_a_field_cannot_be_assigned() -> None:
    """FR-001 AC1: the dataclass is frozen."""
    verdict = Verdict(outcomes=(Outcome.ACCEPTED,))
    with pytest.raises(AttributeError, match="cannot assign to field 'outcomes'"):
        verdict.outcomes = ()  # type: ignore[misc]
    with pytest.raises(AttributeError, match="cannot assign to field 'retry_after'"):
        verdict.retry_after = 1.0  # type: ignore[misc]


@pytest.mark.parametrize(
    ("element", "named"),
    [("accepted", "str"), (None, "NoneType"), (7, "int"), (CANARY, "str"), (Outcome, "EnumType")],
    ids=["str", "none", "int", "canary", "cls"],
)
def test_a_non_outcome_element_is_refused_naming_its_position(element: object, named: str) -> None:
    """FR-001 AC2: the position and the type are named, never the value."""
    with pytest.raises(TypeError) as caught:
        Verdict(outcomes=(Outcome.ACCEPTED, element))  # type: ignore[arg-type]
    assert str(caught.value) == f"Verdict outcomes[1] must be an Outcome, not {named}"


def _generator() -> Any:
    yield Outcome.ACCEPTED


@pytest.mark.parametrize(
    ("container", "named"),
    [
        ("ab", "str"),
        (_generator(), "generator"),
        ({Outcome.ACCEPTED}, "set"),
        ({Outcome.ACCEPTED: 1}, "dict"),
        (None, "NoneType"),
        (frozenset({Outcome.ACCEPTED}), "frozenset"),
    ],
    ids=["str", "generator", "set", "dict", "none", "frozenset"],
)
def test_a_non_sequence_container_is_refused_naming_its_type(container: object, named: str) -> None:
    """FR-001 AC2: any container other than a tuple or a list is refused by its type."""
    with pytest.raises(TypeError) as caught:
        Verdict(outcomes=container)  # type: ignore[arg-type]
    assert str(caught.value) == f"Verdict outcomes must be a tuple of Outcome members, not {named}"


def test_uniform_refuses_a_negative_count() -> None:
    """FR-001 AC2: a negative count is a ``ValueError`` naming the number."""
    with pytest.raises(ValueError, match=r"^Verdict.uniform count must not be negative, not -1$"):
        Verdict.uniform(Outcome.ACCEPTED, -1)
    with pytest.raises(TypeError, match=r"count must be an integer, not float"):
        Verdict.uniform(Outcome.ACCEPTED, 2.5)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match=r"count must be an integer, not str"):
        Verdict.uniform(Outcome.ACCEPTED, "3")  # type: ignore[arg-type]


def test_uniform_refuses_a_non_outcome_at_count_zero() -> None:
    """The outcome is validated before the count is used, so zero cannot hide a mistake."""
    with pytest.raises(TypeError, match=r"^Verdict outcomes\[0\] must be an Outcome, not str$"):
        Verdict.uniform("x", 0)  # type: ignore[arg-type]


@pytest.mark.parametrize("claimed", ["Outcome", "tuple"])
def test_a_metaclass_name_property_cannot_rename_a_refused_value(claimed: str) -> None:
    """FR-001 AC3: the stored name is written and the ``__name__`` property never runs."""
    calls: list[str] = []
    element = _claiming(claimed, calls)()
    with pytest.raises(TypeError) as by_element:
        Verdict(outcomes=(element,))
    assert str(by_element.value).endswith("not RealStoredName"), by_element.value

    container = _claiming(claimed, calls)()
    with pytest.raises(TypeError) as by_container:
        Verdict(outcomes=container)  # type: ignore[arg-type]
    assert str(by_container.value) == (
        "Verdict outcomes must be a tuple of Outcome members, not RealStoredName"
    )
    accepted = _claiming(claimed, calls, base=list)([Outcome.ACCEPTED])
    assert Verdict(outcomes=accepted).outcomes == (Outcome.ACCEPTED,)  # type: ignore[arg-type]
    assert calls == [], calls


def test_a_list_or_tuple_subclass_is_copied_through_the_base_iteration() -> None:
    """A subclass's ``__iter__`` never runs: the members are read from the base storage."""
    entered: ClassVar[list[str]] = []

    class Hostile(tuple):  # type: ignore[type-arg]
        def __iter__(self) -> Any:
            entered.append("__iter__")
            raise RuntimeError("iter-secret")

    class HostileList(list):  # type: ignore[type-arg]
        def __iter__(self) -> Any:
            entered.append("__iter__")
            raise RuntimeError("iter-secret")

    assert Verdict(outcomes=Hostile((Outcome.ACCEPTED,))).outcomes == (Outcome.ACCEPTED,)
    stored = Verdict(outcomes=HostileList([Outcome.DROPPED])).outcomes  # type: ignore[arg-type]
    assert type(stored) is tuple and stored == (Outcome.DROPPED,)
    assert entered == []


@pytest.mark.parametrize("shape", ["claims-outcome", "raises"])
def test_an_element_whose_class_claims_outcome_is_refused_unread(shape: str) -> None:
    """FR-001 AC4: ``__class__`` is never read; the real class's stored name is."""
    read: list[str] = []

    def spoof(self: object) -> type:
        read.append("__class__")
        if shape == "raises":
            raise RuntimeError("class-secret")
        return Outcome

    cls = type("Impostor", (), {"__class__": property(spoof)})
    with pytest.raises(TypeError) as caught:
        Verdict(outcomes=(Outcome.ACCEPTED, Outcome.ACCEPTED, cls()))
    assert str(caught.value) == "Verdict outcomes[2] must be an Outcome, not Impostor"
    assert read == []


def test_outcome_is_exactly_the_six_members_in_order() -> None:
    """FR-001 AC5: closed for 1.x, and an exhaustive ``match`` relies on the set."""
    assert [(member.name, member.value) for member in Outcome] == [
        ("ACCEPTED", "accepted"),
        ("RETRYABLE", "retryable"),
        ("REJECTED", "rejected"),
        ("DROPPED", "dropped"),
        ("UNKNOWN", "unknown"),
        ("SKIPPED", "skipped"),
    ]
    with pytest.raises(TypeError, match="cannot extend"):

        class More(Outcome):  # type: ignore[misc]
            EXTRA = "extra"


# --- SinkCounters ----------------------------------------------------------------------------


def test_sink_counters_start_at_zero_and_report_through_losses() -> None:
    """FR-003: the counters a sink owns, read under their own lock."""
    counters = SinkCounters()
    assert counters.losses() == SinkLosses(dropped=0, failed=0)
    counters.dropped_events = 2
    counters.failed_events = 5
    assert counters.losses() == SinkLosses(dropped=2, failed=5)
    assert type(counters._counter_lock) is type(threading.Lock())


# --- read_deliver ----------------------------------------------------------------------------


def test_read_deliver_returns_the_bound_method_of_an_engine_sink() -> None:
    """FR-001 AC6: the bound ``deliver``, calling through to the sink."""
    sink = _EngineSink()
    found = read_deliver(sink)
    assert found is not None
    assert found.__self__ is sink  # type: ignore[attr-defined]
    assert found.__func__ is _EngineSink.deliver  # type: ignore[attr-defined]
    assert found([{"a": 1}]).outcomes == (Outcome.ACCEPTED,)


class _OptOut(_EngineSink):
    deliver = None  # type: ignore[assignment]


class _StaticDeliver(_EngineSink):
    deliver = staticmethod(lambda batch: Verdict(outcomes=()))  # type: ignore[assignment]


class _PropertyDeliver(_EngineSink):
    @property
    def deliver(self) -> Any:  # type: ignore[override]
        raise AssertionError("a property deliver must never run")


class _BuiltinDeliver(_EngineSink):
    deliver = len  # type: ignore[assignment]


class _OverridesEmit(_EngineSink):
    def emit(self, batch: list[dict[str, object]]) -> None:
        pass


class _CountersProperty(_EngineSink):
    def __init__(self) -> None:
        self.delivered = []

    @property
    def log_foundry_counters(self) -> SinkCounters:  # type: ignore[override]
        raise AssertionError("a counters property must never run")


class _CountersSubclass(SinkCounters):  # type: ignore[misc]
    pass


class _SlottedCounters:
    __slots__ = ("log_foundry_counters",)

    def __init__(self, set_it: bool) -> None:
        if set_it:
            self.log_foundry_counters = SinkCounters()

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        return Verdict.uniform(Outcome.ACCEPTED, len(batch))

    def emit(self, batch: list[dict[str, object]]) -> None:
        pass

    def close(self) -> None:
        pass


class _RaisingGetattr(_EngineSink):
    entered: ClassVar[list[str]] = []

    def __getattr__(self, name: str) -> Any:
        type(self).entered.append(name)
        raise RuntimeError("getattr-secret")


class _DelegatingProxy:
    def __init__(self, inner: object) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def _instance_only() -> object:
    sink = _EngineSink()
    del sink.delivered
    plain = type("Plain", (), {"emit": lambda self, b: None, "close": lambda self: None})()
    plain.log_foundry_counters = SinkCounters()  # type: ignore[attr-defined]
    plain.deliver = sink.deliver  # type: ignore[attr-defined]
    return plain


class _NoCounters(_EngineSink):
    def __init__(self) -> None:
        self.delivered = []


def _with(counters: object) -> object:
    sink = _EngineSink()
    sink.log_foundry_counters = counters  # type: ignore[assignment]
    return sink


_REFUSED = {
    "no-deliver": lambda: type("P", (), {"log_foundry_counters": SinkCounters()})(),
    "deliver-none-opt-out": _OptOut,
    "deliver-not-a-function-staticmethod": _StaticDeliver,
    "deliver-property": _PropertyDeliver,
    "deliver-builtin": _BuiltinDeliver,
    "deliver-on-the-instance-only": _instance_only,
    "no-counters": lambda: _NoCounters(),
    "counters-of-another-type": lambda: _with(SinkLosses(dropped=0, failed=0)),
    "counters-subclass": lambda: _with(_CountersSubclass()),
    "counters-property": _CountersProperty,
    "unset-slot": lambda: _SlottedCounters(False),
    "proxy-delegating-to-an-engine-sink": lambda: _DelegatingProxy(_EngineSink()),
    "subclass-overriding-emit": _OverridesEmit,
    "a-class-passed-as-the-sink": lambda: _EngineSink,
    "none": lambda: None,
}


@pytest.mark.parametrize("shape", sorted(_REFUSED))
def test_read_deliver_refuses(shape: str, capfd: pytest.CaptureFixture[str]) -> None:
    """FR-001 AC6: ``None`` for every shape that does not offer, writing nothing to stderr."""
    sink = _REFUSED[shape]()
    assert read_deliver(sink) is None
    assert capfd.readouterr().err == ""


def test_a_slotted_counters_attribute_is_resolved_statically() -> None:
    """A ``__slots__`` counters member is read through its own descriptor, not ignored."""
    sink = _SlottedCounters(True)
    found = read_deliver(sink)
    assert found is not None and found.__self__ is sink  # type: ignore[attr-defined]


def test_read_deliver_writes_nothing_and_enters_no_getattr() -> None:
    """FR-001 AC6: a raising ``__getattr__`` is never entered and nothing is written."""
    _RaisingGetattr.entered.clear()
    plain = _RaisingGetattr()
    assert read_deliver(plain) is not None
    without = _RaisingGetattr()
    del without.log_foundry_counters
    before = dict(vars(without))
    stderr, sys.stderr = sys.stderr, io.StringIO()
    try:
        assert read_deliver(without) is None
        written = sys.stderr.getvalue()
    finally:
        sys.stderr = stderr
    assert _RaisingGetattr.entered == []
    assert written == ""
    assert vars(without) == before


def test_a_metaclass_mro_or_name_hook_never_runs_during_the_probe() -> None:
    """The MRO and the class namespaces are read through the unbound ``type`` accessors."""
    calls: list[str] = []

    def mro(cls: type) -> Any:
        calls.append("__mro__")
        raise RuntimeError("mro-secret")

    meta = type("_Meta", (type,), {"__mro__": property(mro)})
    cls = meta("Hooked", (_EngineSink,), {})
    assert read_deliver(cls()) is not None
    assert calls == []


# --- Sink protocol -----------------------------------------------------------------------------


def test_a_deliver_sink_is_still_a_sink() -> None:
    """FR-001 AC7: ``deliver`` is not on the Protocol, and adding it changes nothing there."""
    assert isinstance(_EngineSink(), Sink)
    assert "deliver" not in vars(Sink)
    assert "log_foundry_counters" not in vars(Sink)


def test_sink_gains_no_abstract_member() -> None:
    """FR-001 AC7: ``emit`` and ``close`` stay the only abstract members."""
    assert Sink.__abstractmethods__ == frozenset({"emit", "close"})

    class Incomplete(Sink):
        def deliver(self, batch: list[dict[str, object]]) -> Verdict:
            return Verdict(outcomes=())

        def close(self) -> None:
            pass

    with pytest.raises(TypeError, match="abstract"):
        Incomplete()  # type: ignore[abstract]


def test_the_sink_docstring_states_the_deliver_contract() -> None:
    """The members are probed by name, so the ``Sink`` docstring is their only contract."""
    documented = " ".join((Sink.__doc__ or "").split())
    for clause in (
        "deliver(batch) -> Verdict",
        "log_foundry_counters",
        "log_foundry_idempotent",
        "deliver = None",
        "must not call its own ``emit``",
        "emit_via_deliver",
        "read_deliver",
        "a single round with no memory of earlier calls",
        "FIFO hold is not released at the end",
        "sends held events in the run's last round",
    ):
        assert clause in documented, clause


def test_the_verdict_module_routes_on_the_real_type_only() -> None:
    """SPEC-071 FR-005 for the new module: no ``isinstance`` call in ``sinks/verdict.py``."""
    import ast
    import pathlib

    tree = ast.parse(pathlib.Path(verdict_mod.__file__).read_text(encoding="utf-8"))
    calls = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "isinstance"
    ]
    assert calls == [], f"sinks/verdict.py calls isinstance at lines {calls}"
    assert log_foundry.sinks.verdict is verdict_mod
