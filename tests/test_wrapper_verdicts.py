"""SPEC-075 FR-001 to FR-003 — the wrappers offer ``deliver`` exactly when what they wrap does.

``FilteringSink`` and ``TransformSink`` hold the inner sink's own counters, copy its budget, map
its verdict back to their own indices and count only their own decisions; ``LogstashSink`` is a
pure delegate to whichever backend it built. Most criteria run over both filtering wrappers, so
each twin claim is held on both. Serves invariants 2, 4, 6, 7, 8, 11 and 12.
"""

from __future__ import annotations

import ast
import errno
import threading
import time
from pathlib import Path
from typing import Any

import pytest

import log_foundry
import log_foundry.sinks._socket as socket_mod
import nats_engine_matrix_harness as harness
from conftest import install_worker
from log_foundry import _diag, _fork
from log_foundry.sinks import _engine, _retry
from log_foundry.sinks._socket import SocketTransport
from log_foundry.sinks.base import SinkLosses, _static_value
from log_foundry.sinks.filtering import FilteringSink
from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.logstash import LogstashSink
from log_foundry.sinks.memory import MemorySink
from log_foundry.sinks.multi import MultiSink
from log_foundry.sinks.nats import NATSSink
from log_foundry.sinks.sqs import SQSSink
from log_foundry.sinks.syslog import SyslogSink
from log_foundry.sinks.transform import TransformSink
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict, read_deliver
from log_foundry.worker import Worker
from test_fork_lifecycle import _held_by_a_parent_thread, run_in_child
from test_sinks_http import FakeOpener, FakeResponse
from test_sinks_sqs_fifo import FaultingClient, RecordingClient
from test_socket_engine import REFUSED, Socket, install
from test_transform_coercion import _one
from verdict_sinks import (
    MisspeltCounters,
    Proxy,
    Raw,
    Scripted,
    ThirdPartyWrapper,
    WaitRecorder,
    events,
    metaclass_named,
)

A, R, J, D, U, S = (
    Outcome.ACCEPTED,
    Outcome.RETRYABLE,
    Outcome.REJECTED,
    Outcome.DROPPED,
    Outcome.UNKNOWN,
    Outcome.SKIPPED,
)
KINDS = ("filtering", "transform")
_REAL_WAIT = _retry.wait
_SINKS = Path(__file__).resolve().parents[1] / "src" / "log_foundry" / "sinks"


@pytest.fixture(autouse=True)
def _no_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("log_foundry.sinks._retry.wait", lambda _delay, _stop=None: None)


class Caller:
    """The caller's per-event code: which events decline and which raise, counting each call."""

    def __init__(
        self,
        *,
        declines: frozenset[int] = frozenset(),
        fails: frozenset[int] = frozenset(),
        copies: bool = False,
    ) -> None:
        self.declines = declines
        self.fails = fails
        self.copies = copies
        self.errors = {i: ValueError(f"secret-{i}") for i in fails}
        self.calls: list[object] = []

    def predicate(self, event: dict[str, object]) -> bool:
        index = self._index(event)
        if index in self.fails:
            raise self.errors[index]  # type: ignore[index]
        return index not in self.declines

    def fn(self, event: dict[str, object]) -> dict[str, object] | None:
        index = self._index(event)
        if index in self.fails:
            raise self.errors[index]  # type: ignore[index]
        if index in self.declines:
            return None
        return dict(event) if self.copies else event

    def _index(self, event: dict[str, object]) -> object:
        """The event's ``i``, top level or in ``fields`` (a logged event), recorded as a call."""
        fields = event.get("fields")
        index = event.get("i", fields.get("i") if isinstance(fields, dict) else None)
        self.calls.append(index)
        return index


def wrap(kind: str, inner: Any, caller: Caller | None = None) -> Any:
    """A ``FilteringSink`` or ``TransformSink`` over ``inner`` running ``caller``'s code."""
    caller = caller if caller is not None else Caller()
    if kind == "filtering":
        return FilteringSink(inner, predicate=caller.predicate)
    return TransformSink(inner, caller.fn)


def run(sink: Any, batch: list[dict[str, object]], retries: int | None = 0) -> Any:
    """One engine run with no stop event, as the worker or the orphan path would make it."""
    return _engine.run_delivery(sink, batch, worker_retries=retries, stop=None)


def worked(sink: Any, batch: list[dict[str, object]], retries: int = 3) -> tuple[int, bool]:
    """Delivers ``batch`` through a real ``Worker``: ``(failed_batches, bool(flush()))``."""
    worker = Worker(sink, batch_size=1000, flush_interval=60.0, max_retries=retries)
    install_worker(worker)
    worker.submit(batch)
    flushed = bool(worker.flush(timeout=30.0))
    return worker.health().failed_batches, flushed


def orphaned(sink: Any, n: int = 1) -> int:
    """Logs ``n`` events outside any span with ``sink`` configured: the orphan losses they added."""
    log_foundry.configure(service="t", sink=sink)
    before = log_foundry.health().orphan_lost
    for i in range(n):
        log_foundry.info("orphan", i=i)
    return log_foundry.health().orphan_lost - before


def lines(err: str) -> list[str]:
    return [line for line in err.splitlines() if line.startswith("log-foundry:")]


# --- FR-001: the offer, the counters and the budget ---------------------------------------------


@pytest.mark.parametrize("kind", KINDS)
def test_the_offer_follows_the_inner_sink(kind: str) -> None:
    """AC1: offered over an engine sink, on its counters; refused over each non-offering shape."""
    inner = Scripted()
    sink = wrap(kind, inner)
    bound = read_deliver(sink)
    assert bound is not None and bound.__self__ is sink  # type: ignore[attr-defined]
    assert sink.log_foundry_counters is inner.log_foundry_counters
    for other in (
        MemorySink(),
        MisspeltCounters(),
        MultiSink(Scripted()),
        ThirdPartyWrapper(Scripted()),
        Proxy(Scripted()),
    ):
        refused = wrap(kind, other)
        assert read_deliver(refused) is None, type(other).__name__
        assert refused.log_foundry_counters is None


@pytest.mark.parametrize("kind", KINDS)
def test_a_subclass_overriding_emit_is_not_offered_but_its_direct_deliver_runs(kind: str) -> None:
    """AC1: the probe's override rule governs the worker and the orphan path, not a direct call."""
    base = FilteringSink if kind == "filtering" else TransformSink

    def emit(self: Any, batch: list[dict[str, object]]) -> None:
        base.emit(self, batch)

    sub = type("Stamping", (base,), {"emit": emit})
    inner = Scripted()
    sink = sub(inner, predicate=None) if kind == "filtering" else sub(inner, lambda e: e)
    assert read_deliver(sink) is None
    assert sink.log_foundry_counters is inner.log_foundry_counters
    batch = events(2)
    assert sink.deliver(batch).outcomes == (A, A)
    assert inner.calls == 1


@pytest.mark.parametrize("kind", KINDS)
def test_over_an_emit_only_inner_the_worker_calls_the_wrappers_emit(kind: str) -> None:
    """AC2: nothing is offered, so the worker's 1.x path delivers through ``emit``."""
    inner = MemorySink()
    sink = wrap(kind, inner, Caller(declines=frozenset({1})))
    batch = events(3)
    assert worked(sink, batch) == (0, True)
    assert inner.events == [batch[0], batch[2]]


class _BudgetProperty(Scripted):
    """An engine sink whose ``max_retries`` is a property: the engine reads it as none."""

    @property  # type: ignore[override]
    def max_retries(self) -> int:  # type: ignore[override]
        return 2


class _RaisingBudget(Scripted):
    """An engine sink whose ``max_retries`` read raises."""

    @property  # type: ignore[override]
    def max_retries(self) -> int:  # type: ignore[override]
        raise RuntimeError("budget-secret")


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize(
    ("make", "worker_calls", "orphan_calls"),
    [
        (lambda: Scripted([R], max_retries=2), 12, 3),
        (lambda: Scripted([R]), 4, 1),
        (lambda: _RaisingBudget([R]), 4, 1),
        (lambda: _BudgetProperty([R]), 4, 1),
    ],
    ids=["two", "none", "raising", "property"],
)
def test_a_wrapped_engine_sink_gets_the_rounds_it_gets_alone(
    kind: str, make: Any, worker_calls: int, orphan_calls: int
) -> None:
    """AC3: the worker's 1.x total ``(s + 1) × (w + 1)`` and the orphan's ``s + 1``, both ways.

    The property case discriminates the static copy from an ordinary read: an ordinary read would
    copy 2 and give the wrapped sink twelve calls where the bare sink, read statically, gets four.
    """
    for wrapped in (False, True):
        inner = make()
        sink = wrap(kind, inner) if wrapped else inner
        failed, _ = worked(sink, events(1))
        assert (inner.calls, failed) == (worker_calls, 1), "wrapped" if wrapped else "bare"
        log_foundry.shutdown(timeout=5.0)
    for wrapped in (False, True):
        inner = make()
        sink = wrap(kind, inner) if wrapped else inner
        assert orphaned(sink) == 1
        assert inner.calls == orphan_calls, "wrapped" if wrapped else "bare"
        log_foundry.shutdown(timeout=5.0)


@pytest.mark.parametrize("kind", KINDS)
def test_the_inner_ceiling_bounds_a_hint_and_its_idempotence_is_not_copied(
    kind: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC4: a 2.5 s hint against the inner sink's 1.0 s ceiling waits 1.0, wrapped and bare.

    An inner sink declaring ``log_foundry_idempotent`` resends an ``UNKNOWN`` alone, and through
    the wrapper, which declares none, it is called once.
    """
    monkeypatch.setattr(_retry, "wait", _REAL_WAIT)
    for wrapped in (False, True):
        inner = Scripted([Verdict(outcomes=(R,), retry_after=2.5), A], max_retries=1)
        inner.max_retry_after = 1.0  # type: ignore[attr-defined]
        stop = WaitRecorder()
        sink = wrap(kind, inner) if wrapped else inner
        _engine.run_delivery(sink, events(1), worker_retries=0, stop=stop)
        assert stop.waits == [1.0], "wrapped" if wrapped else "bare"
    monkeypatch.setattr(_retry, "wait", lambda _delay, _stop=None: None)
    calls = []
    for wrapped in (False, True):
        inner = Scripted([U], max_retries=3)
        inner.log_foundry_idempotent = True  # type: ignore[attr-defined]
        run(wrap(kind, inner) if wrapped else inner, events(1))
        calls.append(inner.calls)
    assert calls == [4, 1]


def _deliver_beside_a_1x_emit() -> set[str]:
    """Every class under ``sinks/`` defining ``deliver`` and an ``emit`` that never runs the adapter
    over the class itself — derived from the tree, so a fourth such class joins the population."""
    found: set[str] = set()
    for path in sorted(_SINKS.glob("*.py")):
        for node in ast.parse(path.read_text(encoding="utf-8")).body:
            if not isinstance(node, ast.ClassDef):
                continue
            methods = {m.name: m for m in node.body if isinstance(m, ast.FunctionDef)}
            if "deliver" not in methods or "emit" not in methods:
                continue
            adapts = any(
                isinstance(call, ast.Call)
                and ast.unparse(call.func).endswith("emit_via_deliver")
                and call.args
                and ast.unparse(call.args[0]) == "self"
                for call in ast.walk(methods["emit"])
            )
            if not adapts:
                found.add(node.name)
    return found


_BUILDERS: dict[str, Any] = {
    "FilteringSink": lambda inner: FilteringSink(inner),
    "TransformSink": lambda inner: TransformSink(inner, lambda e: e),
}


def test_the_copied_budget_is_read_only_on_every_class_that_copies_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AC5: assignment refuses on each derived class, and the static read finds the inner value.

    The population is derived (:func:`_deliver_beside_a_1x_emit`) and held to its three, so the
    derivation discriminates; each class is then built over a backend whose budget is known.
    """
    population = _deliver_beside_a_1x_emit()
    assert population == {"FilteringSink", "TransformSink", "LogstashSink"}
    install(monkeypatch, Socket())
    inner = Scripted(max_retries=2)
    inner.max_retry_after = 1.5  # type: ignore[attr-defined]
    built: list[tuple[Any, dict[str, object]]] = [
        (_BUILDERS[name](inner), {"max_retries": 2, "max_retry_after": 1.5})
        for name in sorted(population - {"LogstashSink"})
    ]
    http = LogstashSink(url="https://logstash.example/in", max_retries=4, max_retry_after=7.0)
    built.append((http, {"max_retries": 4, "max_retry_after": 7.0}))
    sock = LogstashSink(host="h", port=1, transport="udp", max_retries=5)
    built.append((sock, {"max_retries": 5, "max_retry_after": None}))
    assert {type(sink).__name__ for sink, _ in built} == population
    for sink, expected in built:
        source = "the backend" if type(sink) is LogstashSink else "the wrapped sink"
        for name, value in expected.items():
            with pytest.raises(
                AttributeError, match=f"{name} is read-only; it is copied from {source}"
            ):
                setattr(sink, name, 99)
            assert _static_value(sink, name) == value, (type(sink).__name__, name)
            assert getattr(sink, name) == value
    assert _static_value(http, "max_retries") == _static_value(http._http, "max_retries")
    assert _static_value(sock, "max_retries") == sock._socket.max_retries


def _subclass_assigning(name: str, attribute: str) -> Any:
    """A third-party subclass of the named class that assigns ``attribute`` in its ``__init__``."""
    base = {"FilteringSink": FilteringSink, "TransformSink": TransformSink}.get(name, LogstashSink)

    def init(self: Any, *args: Any, **kwargs: Any) -> None:
        base.__init__(self, *args, **kwargs)
        setattr(self, attribute, 5)

    return type(f"Assigning{name}", (base,), {"__init__": init})


_SUBCLASS_ARGS: dict[str, tuple[tuple[Any, ...], dict[str, Any]]] = {
    "FilteringSink": ((Scripted(),), {}),
    "TransformSink": ((Scripted(), lambda e: e), {}),
    "LogstashSink": ((), {"url": "https://logstash.example/in"}),
}


@pytest.mark.parametrize("attribute", ["max_retries", "max_retry_after"])
def test_a_subclass_assigning_the_copied_budget_is_refused_on_every_class(attribute: str) -> None:
    """Andrew's call (Q2): a third-party subclass that assigns either name now raises.

    The population is derived as the read-only test derives it, and held to the three, so a
    later change that loosens the descriptor on any one class reddens here by name.
    """
    population = _deliver_beside_a_1x_emit()
    assert population == set(_SUBCLASS_ARGS)
    for name in sorted(population):
        args, kwargs = _SUBCLASS_ARGS[name]
        with pytest.raises(AttributeError, match=f"{attribute} is read-only"):
            _subclass_assigning(name, attribute)(*args, **kwargs)


class _BoolBudget(Scripted):
    """An engine sink whose budget is a ``bool`` and a ``str``: the engine reads both as none."""

    max_retries = True
    max_retry_after = "5"


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("make", [_BudgetProperty, _BoolBudget], ids=["property", "bool"])
def test_a_budget_the_engine_reads_as_none_is_copied_as_none(kind: str, make: Any) -> None:
    """Frame-1 ruling: the wrapper stores what the engine reads off the inner sink, or ``None``.

    A property object or a ``bool`` is never stored, so the public read keeps its declared type,
    and the engine's rounds through the wrapper equal the bare sink's.
    """
    calls = []
    for wrapped in (False, True):
        inner = make([R])
        sink = wrap(kind, inner) if wrapped else inner
        if wrapped:
            assert sink.max_retries is None and sink.max_retry_after is None
        run(sink, events(1), retries=1)
        calls.append(inner.calls)
    assert calls == [2, 2]


@pytest.mark.parametrize("kind", KINDS)
def test_a_wrapper_that_offers_nothing_reads_none_for_its_budget(kind: str) -> None:
    """A budget not copied is ``None``, never the read-only descriptor object."""
    sink = wrap(kind, MemorySink())
    assert sink.max_retries is None and sink.max_retry_after is None
    inner = Scripted()
    offered = wrap(kind, inner)
    assert offered.max_retries is None and offered.max_retry_after is None
    assert vars(offered)["max_retries"] is None


@pytest.mark.parametrize("kind", KINDS)
def test_a_deliver_nothing_offers_or_that_stopped_offering_raises_naming_the_type(
    kind: str,
) -> None:
    """AC6: the wrapper's class for a non-offering wrapper; the inner type for replaced counters."""
    with pytest.raises(TypeError, match=type(wrap(kind, MemorySink())).__name__):
        wrap(kind, MemorySink()).deliver(events(1))
    for replacement in (None, SinkCounters()):
        inner = Scripted()
        sink = wrap(kind, inner)
        inner.log_foundry_counters = replacement  # type: ignore[assignment]
        with pytest.raises(TypeError, match="Scripted no longer offers deliver"):
            sink.deliver(events(1))
        delivery = run(sink, events(2))
        assert delivery.failed and delivery.raised == 2
        assert inner.calls == 0


@pytest.mark.parametrize("kind", KINDS)
def test_a_metaclass_name_property_cannot_rename_either_type(kind: str) -> None:
    """AC6: the stored name, for the inner sink's type and for the wrapper's own."""
    calls: list[str] = []
    meta = metaclass_named("Claimed", calls)
    inner_cls = meta("RealInner", (Scripted,), {})
    inner = inner_cls()
    sink = wrap(kind, inner)
    inner.log_foundry_counters = None
    with pytest.raises(TypeError) as raised:
        sink.deliver(events(1))
    assert "RealInner" in str(raised.value) and "Claimed" not in str(raised.value)
    assert calls == [], "the inner type was named without running its metaclass"
    base = FilteringSink if kind == "filtering" else TransformSink
    wrapper_cls = meta("RealWrapper", (base,), {})
    refused = (
        wrapper_cls(MemorySink()) if kind == "filtering" else wrapper_cls(MemorySink(), lambda e: e)
    )
    with pytest.raises(TypeError) as raised:
        refused.deliver(events(1))
    assert "RealWrapper" in str(raised.value) and "Claimed" not in str(raised.value)


SIGNAL_KINDS = (*KINDS, "logstash-http", "logstash-socket")


class _SignalRig:
    """A sink whose rounds succeed once and then fail retryably, and the backend it forwards to.

    The two wrappers stand over a scripted engine sink; ``LogstashSink`` over the backend it
    builds, an ``HTTPSink`` answering ``200`` then ``500`` or a socket transport whose peer starts
    refusing on :meth:`fail`. ``rounds`` counts what reached the destination either way.
    """

    def __init__(self, kind: str, monkeypatch: pytest.MonkeyPatch) -> None:
        self.socket: Socket | None = None
        self.opener: FakeOpener | None = None
        self.inner: Scripted | None = None
        if kind == "logstash-http":
            self.opener = FakeOpener([FakeResponse(200, b"{}"), FakeResponse(500, b"")])
            self.sink: Any = LogstashSink(
                url="http://logstash/in", opener=self.opener, max_retries=1
            )
            self.backend: Any = self.sink._http
        elif kind == "logstash-socket":
            self.socket = Socket()
            install(monkeypatch, self.socket)
            self.sink = _socket_logstash(max_retries=1)
            self.backend = self.sink._socket
        else:
            self.inner = Scripted([A, R], max_retries=1)
            self.sink = wrap(kind, self.inner)
            self.backend = self.inner

    def fail(self) -> None:
        """Makes every later round fail retryably; the scripted and HTTP rigs already do."""
        if self.socket is not None:
            self.socket.refuse = REFUSED

    @property
    def rounds(self) -> int:
        if self.opener is not None:
            return len(self.opener.calls)
        if self.socket is not None:
            return len(self.socket.attempts)
        assert self.inner is not None
        return self.inner.calls


@pytest.mark.parametrize("kind", SIGNAL_KINDS)
def test_the_stop_signal_is_read_statically_and_reaches_the_inner_sink(
    kind: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC7: stored where ``stop_signal_of`` reads it, and forwarded; a worker's offer lands both.

    Run over ``LogstashSink`` in both modes as well as the two wrappers: its signal moved off a
    property too, and nothing else would see it move back.
    """
    rig = _SignalRig(kind, monkeypatch)
    sink, backend = rig.sink, rig.backend
    assert hasattr(sink, "log_foundry_stop_signal") and sink.log_foundry_stop_signal is None
    event = threading.Event()
    sink.log_foundry_stop_signal = event
    assert _engine.stop_signal_of(sink) is event
    assert sink.log_foundry_stop_signal is event and backend.log_foundry_stop_signal is event
    worker = Worker(sink, batch_size=10, flush_interval=60.0)
    install_worker(worker)
    offered = _engine.stop_signal_of(sink)
    assert offered is not None and offered is not event
    assert backend.log_foundry_stop_signal is offered


@pytest.mark.parametrize("kind", SIGNAL_KINDS)
def test_setting_the_signal_cuts_a_wrapped_backoff_short_on_the_orphan_path(
    kind: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC7: the orphan path waits on ``stop_signal_of(sink)``, so a property would wait it out.

    Driven on the orphan path on purpose: the worker hands ``run_delivery`` its own event, so a
    worker-driven run would pass with the signal kept behind a property and prove nothing.
    """
    monkeypatch.setattr(_retry, "wait", _REAL_WAIT)
    monkeypatch.setattr(_engine, "BACKOFF_BASE", 30.0)
    rig = _SignalRig(kind, monkeypatch)
    sink = rig.sink
    log_foundry.configure(service="t", sink=sink)

    @log_foundry.trace
    def build_the_worker() -> None:
        log_foundry.info("inside")

    build_the_worker()
    log_foundry.flush()
    before = rig.rounds
    assert before >= 1, "the worker is built, and has offered its own signal, first"
    rig.fail()
    stop = threading.Event()
    sink.log_foundry_stop_signal = stop
    started = time.monotonic()
    timer = threading.Timer(0.2, stop.set)
    timer.start()
    try:
        log_foundry.info("orphan")
    finally:
        timer.cancel()
    assert time.monotonic() - started < 10.0
    assert rig.rounds - before == 2, "the cut wait still let the last round run"


@pytest.mark.parametrize("kind", KINDS)
def test_a_forked_child_runs_a_wrapper_while_the_parent_holds_the_shared_lock(kind: str) -> None:
    """AC8: the shared counters' lock and the wrapper's own, held at the fork, are the child's own.

    The child's batch carries one failure beside a good event, so the run commits into the
    wrapper's ``_dropped`` under its lock as well as into the shared counters, and the child also
    checks that the signal it reads statically is the one the inner sink holds.
    """
    inner = Scripted([[A, J]])
    sink = wrap(kind, inner, Caller(fails=frozenset({0})))
    log_foundry.configure(service="fork", version="0", env="test", sink=sink)
    released, holder = _held_by_a_parent_thread(inner.log_foundry_counters._counter_lock)
    own_released, own_holder = _held_by_a_parent_thread(sink._counter_lock)

    def deliver_once() -> str:
        delivery = run(sink, events(3))
        paired = _engine.stop_signal_of(sink) is inner.log_foundry_stop_signal
        return f"{delivery.failed} {sink.losses()} {paired}"

    try:
        child = run_in_child(deliver_once, timeout=5)
    finally:
        released.set()
        own_released.set()
        holder.join(5.0)
        own_holder.join(5.0)
    assert child.finished, f"the child {child.ending}, having written {child.output!r}"
    assert child.output == "False SinkLosses(dropped=1, failed=1) True", child.output


def test_the_fork_walk_writes_a_set_only_descriptor_without_running_it() -> None:
    """Plan review MINOR 1: the repair stores the fresh event; it never runs the forward.

    The forward hands the event to the inner sink, whose setter may take a lock the walk has not
    replaced yet, so the walk writes the instance ``__dict__`` past the descriptor. A read-only
    budget, whose ``__set__`` refuses, is written the same way rather than refused.
    """
    seen: list[object] = []

    class Recording(Scripted):
        @property  # type: ignore[override]
        def log_foundry_stop_signal(self) -> threading.Event | None:  # type: ignore[override]
            return None

        @log_foundry_stop_signal.setter
        def log_foundry_stop_signal(self, value: threading.Event | None) -> None:
            seen.append(value)

    sink = FilteringSink(Recording())
    fresh = threading.Event()
    _fork._assign(sink, "log_foundry_stop_signal", fresh)
    assert vars(sink)["log_foundry_stop_signal"] is fresh
    assert seen == []
    _fork._assign(sink, "max_retries", 7)
    assert sink.max_retries == 7


# --- FR-002: the mapping, the count and the refusal ---------------------------------------------


@pytest.mark.parametrize("kind", KINDS)
def test_a_declined_and_a_failed_event_are_skipped_and_only_the_failure_is_counted(
    kind: str,
) -> None:
    """AC1: ``[declined, kept, raising]`` maps to ``[S, A, S]``; ``_dropped`` alone counts it."""
    inner = Scripted()
    sink = wrap(kind, inner, Caller(declines=frozenset({0}), fails=frozenset({2})))
    batch = events(3)
    assert sink.deliver(batch).outcomes == (S, A, S)
    assert len(inner.handed[0]) == 1 and inner.handed[0][0] is batch[1]
    assert sink._dropped == 1
    assert inner.log_foundry_counters.dropped_events == 0
    assert sink.losses() == SinkLosses(dropped=1, failed=0)
    again = wrap(kind, Scripted(), Caller(declines=frozenset({0}), fails=frozenset({2})))
    delivery = run(again, events(3))
    assert not delivery.failed and again.losses() == SinkLosses(dropped=1, failed=0)


@pytest.mark.parametrize("kind", KINDS)
def test_an_empty_batch_is_an_empty_verdict_and_never_reaches_the_inner_sink(kind: str) -> None:
    inner = Scripted()
    assert wrap(kind, inner).deliver([]).outcomes == ()
    assert inner.calls == 0


@pytest.mark.parametrize("kind", KINDS)
def test_a_batch_of_nothing_but_failures_is_refused_once_and_counts_nothing(kind: str) -> None:
    """AC2: ``REJECTED`` with the first exception as the refusal; no sink counter moves."""
    caller = Caller(fails=frozenset({0, 1}))
    inner = Scripted()
    sink = wrap(kind, inner, caller)
    assert sink.deliver(events(2)).outcomes == (J, J)
    delivery = run(sink, events(2))
    assert delivery.failed and delivery.refused is caller.errors[0]
    assert inner.calls == 0
    assert sink._dropped == 0 and inner.losses() == SinkLosses(dropped=0, failed=0)

    caller = Caller(fails=frozenset({0, 1}))
    inner = Scripted()
    sink = wrap(kind, inner, caller)
    assert worked(sink, events(2), retries=3) == (1, False)
    assert caller.calls == [0, 1], "one deliver call: the caller's code ran once per event"
    assert sink._dropped == 0 and inner.losses() == SinkLosses(dropped=0, failed=0)
    log_foundry.shutdown(timeout=5.0)

    caller = Caller(fails=frozenset({0}))
    sink = wrap(kind, Scripted(), caller)
    assert orphaned(sink) == 1
    with pytest.raises(ValueError) as raised:
        sink.emit(events(1))
    assert raised.value is caller.errors[0]


@pytest.mark.parametrize("kind", KINDS)
def test_a_failure_beside_a_declined_or_a_rejected_event(kind: str) -> None:
    """AC3: settles with a declined sibling; never calls the inner sink when all declined; a
    rejected sibling fails the run and the drop is the catcher's."""
    sink = wrap(kind, Scripted(), Caller(declines=frozenset({0}), fails=frozenset({1})))
    assert not run(sink, events(2)).failed and sink._dropped == 1
    inner = Scripted()
    sink = wrap(kind, inner, Caller(declines=frozenset({0, 1})))
    assert sink.deliver(events(2)).outcomes == (S, S)
    assert inner.calls == 0 and sink.losses() == SinkLosses(dropped=0, failed=0)
    inner = Scripted([J])
    sink = wrap(kind, inner, Caller(fails=frozenset({1})))
    assert worked(sink, events(2), retries=0) == (1, False)
    assert sink._dropped == 0


def _coercible(event: dict[str, object]) -> dict[str, object] | None:
    """``test_transform_coercion``'s failing result for event 3; ``None``, a raise, a ``str``."""
    from datetime import UTC, datetime

    i = event["i"]
    if i == 0:
        return None
    if i == 1:
        raise RuntimeError("fn-secret")
    if i == 2:
        return "not a dict"  # type: ignore[return-value]
    return _one({"raw": b"caf\xc3\xa9", "nested": {"when": datetime(2026, 1, 2, tzinfo=UTC)}})


def test_a_transform_maps_its_three_failures_and_forwards_the_coerced_result() -> None:
    """AC4: ``[None, raising, non-dict, ok]`` → ``[S, S, S, inner's]``; the result is coerced."""
    inner = Scripted([A])
    sink = TransformSink(inner, _coercible)
    assert sink.deliver(events(4)).outcomes == (S, S, S, A)
    assert sink._dropped == 2
    assert inner.handed[0] == [
        _one({"raw": "café", "nested": {"when": "2026-01-02T00:00:00+00:00"}})
    ]


@pytest.mark.parametrize("kind", KINDS)
def test_indices_map_back_and_round_two_resends_only_the_retryable_event(kind: str) -> None:
    """AC5: ``[a declined, b, c raising, d]`` with ``[R, A]`` maps to ``[S, R, S, A]``.

    Round two hands the wrapper exactly ``[b]``. Neither the predicate nor the transform's
    function is asked again about ``b``, which round one forwarded, and the inner sink is handed
    the very object round one forwarded (Andrew's call, 2026-11-09; the predicate's from the
    frame-2 review).
    """
    sink = wrap(kind, Scripted([[R, A]]), Caller(declines=frozenset({0}), fails=frozenset({2})))
    assert sink.deliver(events(4)).outcomes == (S, R, S, A)
    caller = Caller(declines=frozenset({0}), fails=frozenset({2}), copies=True)
    inner = Scripted([[R, A], A], max_retries=1)
    sink = wrap(kind, inner, caller)
    batch = events(4)
    assert not run(sink, batch).failed
    assert [[e["i"] for e in r] for r in inner.rounds] == [[1, 3], [1]]
    assert caller.calls == [0, 1, 2, 3], "round two did not ask the caller's code again"
    if kind == "filtering":
        assert inner.handed[1][0] is batch[1]
    else:
        assert inner.handed[0][0] is not batch[1], "the premise: the function copies"
        assert inner.handed[1][0] is inner.handed[0][0], "the same object is forwarded"


def test_a_predicate_is_asked_once_per_event_per_run_and_afresh_on_the_next_run() -> None:
    """FR-002 (frame-2 review): a forwarded event is resent without asking the predicate again.

    Three rounds, two events: the predicate is asked twice. The remembered decisions die with the
    run, so the next run of the same events asks again, and so does a direct ``deliver``.
    """
    caller = Caller()
    inner = Scripted([R, R, A], max_retries=2)
    sink = FilteringSink(inner, predicate=caller.predicate)
    batch = events(2)
    assert not run(sink, batch).failed
    assert caller.calls == [0, 1]
    assert [[e["i"] for e in r] for r in inner.rounds] == [[0, 1]] * 3
    assert not run(sink, batch).failed
    assert caller.calls == [0, 1, 0, 1], "the next run asked the predicate afresh"
    sink.deliver(batch)
    assert caller.calls == [0, 1, 0, 1, 0, 1]


class _FirstN:
    """A rate-limit or sampling predicate: lets the first ``n`` calls through, then declines."""

    def __init__(self, n: int) -> None:
        self.n = n
        self.seen = 0

    def __call__(self, event: dict[str, object]) -> bool:
        self.seen += 1
        return self.seen <= self.n


class _Dedupe:
    """Forwards each message once: a common shape for a filter in front of a sink."""

    def __init__(self) -> None:
        self.seen: set[object] = set()

    def __call__(self, event: dict[str, object]) -> bool:
        key = event.get("message")
        if key in self.seen:
            return False
        self.seen.add(key)
        return True


@pytest.mark.parametrize("through_emit", [False, True], ids=["deliver", "emit"])
def test_a_stateful_predicate_cannot_drop_a_resend_and_the_paths_are_twins(
    through_emit: bool,
) -> None:
    """Frame-2 review (p1): a sampling predicate that has spent its budget does not see a resend.

    The inner sink accepts ``e0`` and answers ``RETRYABLE`` for ``e1``, then accepts everything.
    A predicate letting two calls through, asked again on the resend, would decline ``e1`` and
    lose it silently; both paths now deliver it, in the same rounds' events and the same ledger.
    """
    inner = Scripted([[A, R], A], max_retries=3)
    sink = FilteringSink(inner, predicate=_FirstN(2))
    target = ThirdPartyWrapper(sink) if through_emit else sink
    assert worked(target, events(2), retries=0) == (0, True)
    landed = {e["i"] for r in inner.rounds for e in r}
    assert landed == {0, 1}
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


@pytest.mark.parametrize("through_emit", [False, True], ids=["deliver", "emit"])
def test_a_dedupe_predicate_over_a_real_http_sink_resends_as_the_emit_path_does(
    through_emit: bool, capsys: pytest.CaptureFixture[str]
) -> None:
    """Frame-2 review (p7): ``503`` then ``200`` behind a dedupe predicate: two POSTs both ways.

    Asked again on the engine's resend, the dedupe predicate declined its own event: one POST,
    nothing counted, the message gone. The ``emit`` path's worker resend reached the inner
    sink's engine, which never asks the predicate, and so posted twice.
    """
    opener = FakeOpener([FakeResponse(503, b""), FakeResponse(200, b"{}")])
    inner = HTTPSink("http://collector/in", opener=opener, max_retries=3)
    sink = FilteringSink(inner, predicate=_Dedupe())
    target = ThirdPartyWrapper(sink) if through_emit else sink
    assert worked(target, [{"message": "payment captured", "level": "INFO"}]) == (0, True)
    assert len(opener.calls) == 2
    assert sink.losses() == SinkLosses(dropped=0, failed=0)
    assert not [line for line in lines(capsys.readouterr().err) if "lost" in line]


def test_a_predicate_cannot_fail_a_forwarded_event_on_its_resend(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Frame-2 review: a predicate that would raise on its second call is never called again.

    Asked again, it raised on the resend: one more ``absorbed`` line, a refusal "after others
    landed" and the event charged to the inner sink's ``failed_events``. Now the forwarded event
    is resent without asking and lands.
    """
    calls: list[object] = []

    def once(event: dict[str, object]) -> bool:
        calls.append(event["i"])
        if calls.count(event["i"]) > 1:
            raise ValueError("asked twice")
        return True

    inner = Scripted([[A, R], A], max_retries=1)
    sink = FilteringSink(inner, predicate=once)
    assert not run(sink, events(2)).failed
    assert calls == [0, 1]
    assert [[e["i"] for e in r] for r in inner.rounds] == [[0, 1], [1]], "e1 retried, landed"
    assert inner.losses() == SinkLosses(dropped=0, failed=0) and sink._dropped == 0
    assert lines(capsys.readouterr().err) == [], "no absorbed line, no refusal line"

    raising = FilteringSink(Scripted([A]), predicate=Caller(fails=frozenset({1})).predicate)
    assert not run(raising, events(2)).failed
    assert raising._dropped == 1, "a failure before any forward still costs only its event"
    written = lines(capsys.readouterr().err)
    assert len(written) == 1 and "absorbed a failure" in written[0], written


def test_a_transform_runs_once_per_event_per_run_and_afresh_on_the_next_run() -> None:
    """FR-002: every round of one run forwards one object per event; a new run makes new ones.

    Three rounds, two events: the function runs twice, and each round hands the inner sink the
    same two objects. The kept results die with the run, so delivering the same events again
    runs the function again and forwards new objects; so does a direct ``deliver``, which is a
    run of its own.
    """
    caller = Caller(copies=True)
    inner = Scripted([R, R, A], max_retries=2)
    sink = TransformSink(inner, caller.fn)
    batch = events(2)
    assert not run(sink, batch).failed
    assert caller.calls == [0, 1]
    first = inner.handed[0]
    assert all(r[i] is first[i] for r in inner.handed for i in range(2))
    assert not run(sink, batch).failed
    assert caller.calls == [0, 1, 0, 1], "the next run ran the function afresh"
    assert all(inner.handed[3][i] is not first[i] for i in range(2))
    sink.deliver(batch)
    assert caller.calls == [0, 1, 0, 1, 0, 1]


def test_a_declined_or_failed_result_is_not_kept() -> None:
    """Only a forwarded result is kept: a ``None`` or a raise leaves the event uncached.

    Both make the event ``SKIPPED``, which the engine never resends, so nothing asks again
    within the run; a fresh run asks the function again.
    """
    caller = Caller(declines=frozenset({0}), fails=frozenset({1}), copies=True)
    inner = Scripted([[R], A], max_retries=1)
    sink = TransformSink(inner, caller.fn)
    assert not run(sink, events(3)).failed
    assert caller.calls == [0, 1, 2]


def _http(responses: list[Any] | None, **kwargs: Any) -> tuple[HTTPSink, FakeOpener]:
    opener = FakeOpener(responses)
    return HTTPSink("http://collector/in", opener=opener, **kwargs), opener


@pytest.mark.parametrize("kind", KINDS)
def test_over_a_real_http_sink_the_wrapper_and_the_bare_sink_keep_one_ledger(kind: str) -> None:
    """AC6: an oversized event is one ``dropped`` both ways; an always-500 destination is
    retried exactly as often both ways, with equal ``.failed`` and ``failed_batches``."""
    oversized = [{"i": 0, "a": 1}, {"i": 1, "pad": "x" * 500}]
    seen = []
    for wrapped in (False, True):
        inner, _ = _http(None, max_batch_bytes=200)
        sink = wrap(kind, inner) if wrapped else inner
        assert not run(sink, oversized).failed
        seen.append(sink.losses())
    assert seen[0] == seen[1] and seen[0].dropped == 1
    out = []
    for wrapped in (False, True):
        inner, opener = _http([FakeResponse(500, b"")], max_retries=1)
        sink = wrap(kind, inner) if wrapped else inner
        failed, _ = worked(sink, events(2), retries=1)
        out.append((len(opener.calls), inner.failed, failed))
        log_foundry.shutdown(timeout=5.0)
    assert out[0] == out[1] and out[0][0] == 4


def _sqs(client: Any, **kwargs: Any) -> SQSSink:
    return SQSSink("https://sqs.example/q", client=client, max_retries=3, **kwargs)


@pytest.mark.parametrize("kind", KINDS)
def test_over_a_real_sqs_sink_the_wrapper_and_the_bare_sink_keep_one_ledger(kind: str) -> None:
    """AC7: an oversized message is one ``dropped`` both ways; an all-retryable batch makes the
    same client calls, with equal ``losses()`` and ``failed_batches``."""
    oversized = [{"i": 0}, {"i": 1, "pad": "x" * (SQSSink.MAX_BYTES + 1)}]
    seen = []
    for wrapped in (False, True):
        inner = _sqs(RecordingClient())
        sink = wrap(kind, inner) if wrapped else inner
        assert not run(sink, oversized).failed
        seen.append(sink.losses())
    assert seen[0] == seen[1] and seen[0].dropped == 1
    out = []
    for wrapped in (False, True):
        client = FaultingClient(sender_fault=False, code="InternalError")
        inner = _sqs(client)
        sink = wrap(kind, inner) if wrapped else inner
        failed, _ = worked(sink, events(2), retries=1)
        out.append((len(client.calls), sink.losses(), failed))
        log_foundry.shutdown(timeout=5.0)
    assert out[0] == out[1] and out[0][2] == 1


def _nats() -> NATSSink:
    return NATSSink(
        "logs", client=harness.Client(harness.Script([], default="raise")), publish_timeout=0.2
    )


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("family", ["sqs-sender-fault", "nats-sink-counted"])
def test_the_inner_rounds_marks_move_into_the_wrappers_positions(
    kind: str, family: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """AC8: a declined first event shifts every index; the marks move with it.

    Through each wrapper, over a real ``SQSSink`` whose client refuses every entry as a sender
    fault (``final_rejections``) and over a ``NATSSink`` whose every publish raises
    (``sink_counted``), the run ends exactly as the bare sink's given only the forwarded events.
    Without the rewrite the mark lands on the declined event's ``SKIPPED`` and the engine fails
    the round as an escaped exception.
    """
    batch = events(2)
    results = []
    for wrapped in (False, True):
        inner: Any = (
            _sqs(FaultingClient(sender_fault=True, code="InvalidParameterValue"))
            if family.startswith("sqs")
            else _nats()
        )
        try:
            sink = wrap(kind, inner, Caller(declines=frozenset({0}))) if wrapped else inner
            delivery = run(sink, batch if wrapped else batch[1:])
            results.append(
                (delivery.failed, delivery.raised, inner.failed, lines(capsys.readouterr().err))
            )
        finally:
            inner.close()
    assert results[0] == results[1], results
    assert results[0][1] == 0
    if family.startswith("sqs"):
        assert results[0][0] is False, "a batch lost only to final rejections settles"


def _marks_out_of_range(batch: list[dict[str, object]]) -> Verdict:
    """A round that marks index 2 of its two-event batch a final rejection: out of range."""
    _engine.run_context().final_rejections([2])
    return Verdict.uniform(J, len(batch))


@pytest.mark.parametrize("kind", KINDS)
def test_an_inner_mark_out_of_range_stays_invalid_through_the_wrapper(kind: str) -> None:
    """FR-002: a mark outside the forwarded list fails the round, as the bare sink's engine does.

    The wrapper declines event 0, so the inner sink's out-of-range index 2 would fall inside the
    wrapper's three-event batch if the rewrite moved it there, and fit the ``REJECTED`` there; it
    must stay out of range. Both runs fail as an escaped exception does, every event of the
    round counted raised — the wrapper's ``SKIPPED`` one too, SPEC-072's raising-round bound.
    """
    batch = events(3)
    bare = run(Scripted([_marks_out_of_range]), batch[1:])
    sink = wrap(kind, Scripted([_marks_out_of_range]), Caller(declines=frozenset({0})))
    wrapped = run(sink, batch)
    assert (bare.failed, bare.raised) == (True, 2)
    assert (wrapped.failed, wrapped.raised) == (True, 3)


@pytest.mark.parametrize("kind", KINDS)
def test_an_inner_exception_propagates_and_fails_the_run_as_bare(
    kind: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """AC9: by identity out of ``deliver``; through the engine, the bare sink's failed batches."""
    error = ConnectionRefusedError("secret")
    sink = wrap(kind, Scripted([error]))
    with pytest.raises(ConnectionRefusedError) as raised:
        sink.deliver(events(1))
    assert raised.value is error
    out = []
    for wrapped in (False, True):
        inner = Scripted([error])
        target = wrap(kind, inner) if wrapped else inner
        out.append(worked(target, events(2), retries=1))
        log_foundry.shutdown(timeout=5.0)
    assert out[0] == out[1] == (1, False)
    assert "secret" not in capsys.readouterr().err


class _Short(Verdict):
    """A ``Verdict`` subclass whose ``outcomes`` raises when read."""

    @property  # type: ignore[override]
    def outcomes(self) -> tuple[Outcome, ...]:  # type: ignore[override]
        raise RuntimeError("outcomes-secret")


class _Hint(Verdict):
    """A ``Verdict`` subclass whose ``retry_after`` raises from the ``n``-th read on."""

    reads: list[int]
    after: int

    @property  # type: ignore[override]
    def retry_after(self) -> float | None:  # type: ignore[override]
        self.reads.append(1)
        if len(self.reads) >= self.after:
            raise RuntimeError("hint-secret")
        return 2.0


def _bare(cls: type[Verdict], outcomes: tuple[object, ...], **attrs: object) -> Verdict:
    verdict = object.__new__(cls)
    if outcomes is not None and cls is not _Short:
        object.__setattr__(verdict, "outcomes", outcomes)
    for name, value in attrs.items():
        object.__setattr__(verdict, name, value)
    return verdict


def _string_outcome() -> Verdict:
    verdict = Verdict(outcomes=(A,))
    object.__setattr__(verdict, "outcomes", ("accepted",))
    return verdict


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize(
    "returned",
    [
        lambda: None,
        dict,
        lambda: Verdict(outcomes=()),
        _string_outcome,
        lambda: _bare(_Short, ()),
        lambda: _bare(_Hint, (A,), reads=[], after=1),
    ],
    ids=["none", "dict", "one-short", "bare-string", "raising-outcomes", "raising-hint"],
)
def test_a_malformed_inner_verdict_turns_the_forwarded_events_unknown(
    kind: str, returned: Any
) -> None:
    """AC10: the forwarded event is ``UNKNOWN``, the wrapper's own outcomes stand, the run settles."""
    caller = Caller(declines=frozenset({0}), fails=frozenset({2}))
    sink = wrap(kind, Scripted([lambda batch: returned()]), caller)
    verdict = sink.deliver(events(3))
    assert verdict.outcomes == (S, U, S) and verdict.retry_after is None
    sink = wrap(kind, Scripted([lambda batch: returned()]), Caller(declines=frozenset({0})))
    assert not run(sink, events(2)).failed


class _RegistersThenMalformed:
    """An engine sink whose round counts ``_x`` and announces a line, ``ALWAYS``, then returns
    ``None``: what it registered is the round the engine sets aside bare (PR 319)."""

    def __init__(self) -> None:
        self.log_foundry_counters = SinkCounters()
        self.max_retries = 0
        self._x = 0
        self._counter_lock = threading.Lock()

    def deliver(self, batch: list[dict[str, object]]) -> Any:
        context = _engine.run_context()
        context.count(self, "_x", len(batch), when=_engine.When.ALWAYS)
        context.announce(
            lambda: _diag.absorbed("an inner round", ValueError(), "inner line"),
            when=_engine.When.ALWAYS,
        )
        return None

    def emit(self, batch: list[dict[str, object]]) -> None:
        _engine.emit_via_deliver(self, batch)


@pytest.mark.parametrize("kind", ["bare", *KINDS])
def test_what_a_malformed_inner_round_registered_is_set_aside_as_bare(
    kind: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-002: the inner call's count and line are discarded, and the engine announces the events.

    Bare, PR 319's engine sets a round with an unreadable verdict aside, so the sink's own count
    and line made in it never commit, and the engine's settled line names the ``UNKNOWN``
    events. Through a wrapper the verdict the engine reads is the wrapper's, well formed, so
    ``RunContext.delegate`` sets the inner call aside itself.
    """
    inner = _RegistersThenMalformed()
    sink = inner if kind == "bare" else wrap(kind, inner)
    delivery = run(sink, events(2))
    assert not delivery.failed
    assert inner._x == 0
    assert inner.log_foundry_counters.losses() == SinkLosses(dropped=0, failed=2)
    name = type(sink).__name__
    assert lines(capsys.readouterr().err) == [
        (
            "log-foundry: lost 2 event(s); rejected 0, unresolved 0, raised 0, unknown 2, "
            f"dropped 0 after 1 delivery round(s) to {name}"
        )
    ]


@pytest.mark.parametrize("kind", ["bare", *KINDS])
def test_a_direct_deliver_keeps_what_a_malformed_inner_round_registered_as_bare(
    kind: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """Frame-2 review: outside a run nothing is set aside, bare or wrapped.

    A direct ``deliver`` is a detached round, and ``detached`` never sets an unreadable round
    aside, so the bare sink's count and line commit; through a wrapper they commit too.
    """
    inner = _RegistersThenMalformed()
    sink = inner if kind == "bare" else wrap(kind, inner)
    sink.deliver(events(2))
    assert inner._x == 2
    written = lines(capsys.readouterr().err)
    assert len(written) == 1 and "inner line" in written[0], written


@pytest.mark.parametrize("kind", KINDS)
def test_the_set_aside_keeps_the_wrappers_own_count(
    kind: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-002: the rewind reaches back only to just before the inner call.

    The wrapper registered its ``_dropped`` count for a caller-code failure before it called the
    inner sink, so the count survives the set-aside and commits with the settled run.
    """
    inner = _RegistersThenMalformed()
    sink = wrap(kind, inner, Caller(fails=frozenset({0})))
    assert not run(sink, events(3)).failed
    assert inner._x == 0 and sink._dropped == 1
    written = lines(capsys.readouterr().err)
    assert any("unknown 2" in line and type(sink).__name__ in line for line in written), written
    assert not any("inner line" in line for line in written), written


@pytest.mark.parametrize("kind", KINDS)
def test_a_hint_that_raises_on_the_second_read_maps_and_gives_no_hint(kind: str) -> None:
    """AC10: ``outcomes_of`` reads it once and accepts; the wrapper's guarded read gives ``None``."""
    verdict = _bare(_Hint, (A,), reads=[], after=2)
    sink = wrap(kind, Scripted([verdict]))
    mapped = sink.deliver(events(1))
    assert mapped.outcomes == (A,) and mapped.retry_after is None


@pytest.mark.parametrize("kind", KINDS)
def test_a_keyboard_interrupt_propagates_by_identity(kind: str) -> None:
    interrupt = KeyboardInterrupt()
    sink = wrap(kind, Scripted([interrupt]))
    with pytest.raises(KeyboardInterrupt) as raised:
        sink.deliver(events(1))
    assert raised.value is interrupt


@pytest.mark.parametrize("kind", KINDS)
def test_a_call_with_failures_writes_one_line_naming_types_never_the_message(
    kind: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """AC11: one ``absorbed`` line, the wrapper's class and the exception's type, no value."""
    sink = wrap(kind, Scripted(), Caller(fails=frozenset({1})))
    sink.deliver(events(2))
    written = lines(capsys.readouterr().err)
    assert len(written) == 1, written
    assert "absorbed a failure" in written[0]
    assert type(sink).__name__ in written[0] and "ValueError" in written[0]
    assert "secret" not in written[0]


def _reached(cls: ast.ClassDef) -> list[ast.FunctionDef]:
    """``deliver`` and every method of the class it reaches through ``self.<name>``."""
    methods = {m.name: m for m in cls.body if isinstance(m, ast.FunctionDef)}
    reached: list[ast.FunctionDef] = []
    stack, seen = ["deliver"], set()
    while stack:
        name = stack.pop()
        if name in seen or name not in methods:
            continue
        seen.add(name)
        reached.append(methods[name])
        stack.extend(
            node.attr
            for node in ast.walk(methods[name])
            if isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id == "self"
        )
    return reached


def deliver_findings(source: str, cls_name: str) -> list[str]:
    """Every way a wrapper's ``deliver`` writes something itself (SPEC-075 FR-002 AC12)."""
    cls = next(
        node
        for node in ast.parse(source).body
        if isinstance(node, ast.ClassDef) and node.name == cls_name
    )
    findings: list[str] = []
    counts = 0
    for fn in _reached(cls):
        for node in ast.walk(fn):
            where = f"{cls_name}.{fn.name}:{getattr(node, 'lineno', 0)}"
            if isinstance(node, ast.Call) and ast.unparse(node.func).endswith("Deferral"):
                findings.append(f"{where}: builds a Deferral")
            if isinstance(node, ast.With) and any(
                "lock" in ast.unparse(item.context_expr).lower() for item in node.items
            ):
                findings.append(f"{where}: enters a lock")
            targets: list[ast.expr] = []
            if isinstance(node, ast.Assign):
                targets = list(node.targets)
            elif isinstance(node, ast.AugAssign | ast.AnnAssign):
                targets = [node.target]
            for target in targets:
                root = target
                while isinstance(root, ast.Attribute | ast.Subscript):
                    if isinstance(root, ast.Attribute) and isinstance(root.value, ast.Name):
                        if root.value.id == "self":
                            findings.append(f"{where}: stores {ast.unparse(target)}")
                        break
                    root = root.value
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "count"
            ):
                named = len(node.args) >= 2 and ast.unparse(node.args[1]) == "'_dropped'"
                when = {k.arg: ast.unparse(k.value) for k in node.keywords}.get("when", "")
                if not (named and when.endswith("When.SETTLED")):
                    findings.append(f"{where}: a count other than _dropped, SETTLED")
                counts += 1
    if counts != 1:
        findings.append(f"{cls_name}: {counts} run-context counts, not exactly one")
    return findings


_WRAPPER_MODULES = {"FilteringSink": "filtering.py", "TransformSink": "transform.py"}

_NESTED = {"emit", "run_delivery", "emit_via_deliver"}


def nested_delivery_findings(source: str, cls_name: str) -> list[str]:
    """Every reference that could start a delivery of its own from inside a ``deliver``.

    A round that called an inner ``emit`` — its own engine run, with its own line — and also
    reported those events in its verdict would announce them twice (PR 319's hand-over item 3),
    and FR-001 says the wrapper never calls ``run_delivery`` or ``emit`` on its inner sink from
    ``deliver``. **What it checks:** in ``deliver`` and every method of the same class it reaches
    through ``self.<name>`` (:func:`_reached`), any load of an attribute named ``emit``,
    ``run_delivery`` or ``emit_via_deliver`` (called or not, so ``send = self._inner.emit``
    counts), any load of a bare name ``run_delivery`` or ``emit_via_deliver``, and any
    ``getattr`` call whose second argument is one of those names as a string constant. **What it
    does not:** a module-level helper, a method of another class, or a name assembled at run
    time; the walk is bounded to the class body on purpose.
    """
    cls = next(
        node
        for node in ast.parse(source).body
        if isinstance(node, ast.ClassDef) and node.name == cls_name
    )
    findings = []
    for fn in _reached(cls):
        for node in ast.walk(fn):
            where = f"{cls_name}.{fn.name}:{getattr(node, 'lineno', 0)}"
            if (
                isinstance(node, ast.Attribute)
                and isinstance(node.ctx, ast.Load)
                and node.attr in _NESTED
            ):
                findings.append(f"{where}: loads {ast.unparse(node)}")
            elif (
                isinstance(node, ast.Name)
                and isinstance(node.ctx, ast.Load)
                and node.id in _NESTED - {"emit"}
            ):
                findings.append(f"{where}: loads {node.id}")
            elif (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "getattr"
                and len(node.args) >= 2
                and isinstance(node.args[1], ast.Constant)
                and node.args[1].value in _NESTED
            ):
                findings.append(f"{where}: getattr names {node.args[1].value}")
    return findings


_NESTED_MODULES = {**_WRAPPER_MODULES, "LogstashSink": "logstash.py"}


@pytest.mark.parametrize("cls_name", sorted(_NESTED_MODULES))
def test_no_deliver_starts_a_delivery_of_its_own(cls_name: str) -> None:
    """Every ``emit`` and adapter call in the three classes sits in the 1.x ``emit``, never here."""
    source = (_SINKS / _NESTED_MODULES[cls_name]).read_text(encoding="utf-8")
    assert nested_delivery_findings(source, cls_name) == []


_NESTED_PLANTS = {
    "a direct inner emit": ("self._inner.emit(batch)", "loads self._inner.emit"),
    "an aliased inner emit": ("send = self._inner.emit\nsend(batch)", "loads self._inner.emit"),
    "a getattr emit": ('getattr(self._inner, "emit")(batch)', "getattr names emit"),
    "the adapter by module": (
        "_engine.emit_via_deliver(self._inner, batch)",
        "loads _engine.emit_via_deliver",
    ),
    "a bare run_delivery": (
        "run_delivery(self._inner, batch, worker_retries=0, stop=None)",
        "loads run_delivery",
    ),
}


@pytest.mark.parametrize("plant", sorted(_NESTED_PLANTS))
def test_the_nested_delivery_check_names_each_planted_reference(plant: str) -> None:
    """The check's own corpus: each plant in ``FilteringSink._round`` is named, by its reason."""
    source = (_SINKS / "filtering.py").read_text(encoding="utf-8")
    anchor = "        outcomes = [Outcome.SKIPPED] * len(batch)\n"
    assert source.count(anchor) == 1
    line, reason = _NESTED_PLANTS[plant]
    planted = "".join("        " + part + "\n" for part in line.split("\n"))
    findings = nested_delivery_findings(source.replace(anchor, planted + anchor), "FilteringSink")
    assert any(reason in f for f in findings), findings


@pytest.mark.parametrize("cls_name", sorted(_WRAPPER_MODULES))
def test_neither_deliver_writes_anything_itself(cls_name: str) -> None:
    """AC12: no ``Deferral``, no lock, no ``self`` store, one ``_dropped`` count ``SETTLED``."""
    source = (_SINKS / _WRAPPER_MODULES[cls_name]).read_text(encoding="utf-8")
    assert deliver_findings(source, cls_name) == []


_PLANTED = {
    "a store": ("self._dropped += 1", "stores self._dropped"),
    "a second count": (
        "_engine.run_context().count(self, '_dropped', 1, when=_engine.When.SETTLED)",
        "2 run-context counts, not exactly one",
    ),
    "a lock": ("with self._counter_lock:\n    pass", "enters a lock"),
    "an always count": (
        "_engine.run_context().count(self, '_dropped', 1, when=_engine.When.ALWAYS)",
        "a count other than _dropped, SETTLED",
    ),
}


@pytest.mark.parametrize("plant", sorted(_PLANTED))
def test_the_deliver_check_names_each_planted_write(plant: str) -> None:
    """The check's own corpus: each plant in ``FilteringSink._round`` is named, by its reason."""
    source = (_SINKS / "filtering.py").read_text(encoding="utf-8")
    anchor = "        outcomes = [Outcome.SKIPPED] * len(batch)\n"
    assert source.count(anchor) == 1
    line, reason = _PLANTED[plant]
    planted = "".join("        " + part + "\n" for part in line.split("\n"))
    findings = deliver_findings(source.replace(anchor, planted + anchor), "FilteringSink")
    assert any(reason in f for f in findings), findings


_ROWS = {
    "declined": (Caller(declines=frozenset({0})), [A]),
    "failed-beside-forwarded": (Caller(fails=frozenset({0})), [A]),
    "all-failed": (Caller(fails=frozenset({0, 1})), [A]),
    "forwarded": (Caller(), [A]),
    "forwarded-malformed": (Caller(), [Raw(None)]),
}


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("row", sorted(_ROWS))
def test_the_deliver_path_and_the_emit_path_are_twins(kind: str, row: str) -> None:
    """AC13: per table row, a worker through ``deliver`` and through ``emit`` keep one ledger.

    The ``emit`` path is a third-party pass-through around the same wrapper, so the worker calls
    the wrapper's 1.x ``emit``. ``failed`` is not compared: it is the upper bound, reached by
    different counts.
    """
    out = []
    for through_emit in (False, True):
        template, script = _ROWS[row]
        caller = Caller(declines=template.declines, fails=template.fails)
        inner = Scripted(script, max_retries=0)
        sink = wrap(kind, inner, caller)
        target = ThirdPartyWrapper(sink) if through_emit else sink
        failed, _ = worked(target, events(2), retries=0)
        losses = target.losses()
        out.append(([e["i"] for r in inner.rounds for e in r], losses.dropped, failed))
        log_foundry.shutdown(timeout=5.0)
    assert out[0] == out[1], out


@pytest.mark.parametrize("kind", KINDS)
def test_rounds_run_on_the_drain_thread_in_a_span_and_the_callers_outside(kind: str) -> None:
    """AC14: the worker's thread inside a span; the caller's own thread on the orphan path."""
    inner = Scripted()
    sink = wrap(kind, inner)
    log_foundry.configure(service="t", sink=sink)

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("inside")

    work()
    assert log_foundry.flush()
    caller = threading.current_thread().name
    assert inner.threads and all(name != caller for name in inner.threads)
    seen = len(inner.threads)
    log_foundry.info("outside")
    assert inner.threads[seen:] == [caller]


# --- a transform keeps one object per event across a run's rounds (Andrew's call, 2026-11-09) ----


class _FirstFailsOnce:
    """``a`` fails on the first two calls and ``b`` on the first; records the landing order."""

    def __init__(self) -> None:
        self.calls = 0
        self.landed: list[str] = []

    def send_message_batch(self, *, QueueUrl: str, Entries: list[dict[str, Any]]) -> dict[str, Any]:
        import json

        self.calls += 1
        ok, failed = [], []
        for entry in Entries:
            key = json.loads(entry["MessageBody"])["k"]
            if (key == "a" and self.calls <= 2) or (key == "b" and self.calls == 1):
                failed.append(
                    {"Id": entry["Id"], "Code": "ServiceUnavailable", "SenderFault": False}
                )
            else:
                ok.append({"Id": entry["Id"], "MessageId": "m"})
                self.landed.append(key)
        return {"Successful": ok, "Failed": failed}


def test_a_copying_transform_keeps_a_fifo_queues_order_as_bare() -> None:
    """A transform that copies before it returns lands a FIFO group in the bare sink's order.

    ``a`` fails twice and ``b`` once. Bare, ``SQSSink`` holds ``b`` behind ``a`` in its group and
    lands ``[a, b]``; through the wrapper it must recognise the forwarded object as the event it
    holds notes for, which a fresh copy per round defeated (``[b, a]`` in three calls).
    """
    out = []
    for wrapped in (False, True):
        client = _FirstFailsOnce()
        inner = SQSSink(
            "https://sqs.example/q.fifo",
            client=client,
            max_retries=3,
            fifo=True,
            message_group_id="g",
        )
        sink = TransformSink(inner, lambda e: dict(e)) if wrapped else inner
        run(sink, [{"k": "a", "i": 0}, {"k": "b", "i": 1}])
        out.append((client.landed, client.calls))
    assert out[0] == out[1], out
    assert out[1][0] == ["a", "b"]


@pytest.mark.parametrize("fifo", [False, True], ids=["standard", "fifo"])
def test_a_copying_transform_keeps_the_bare_sinks_attempt_count_line(
    fifo: bool, capsys: pytest.CaptureFixture[str]
) -> None:
    """The abandoned line counts the attempts a message made, through a transform as bare."""
    out = []
    for wrapped in (False, True):
        kwargs: dict[str, Any] = {"fifo": True, "message_group_id": "g"} if fifo else {}
        url = "https://sqs.example/q.fifo" if fifo else "https://sqs.example/q"
        inner = SQSSink(
            url,
            client=FaultingClient(sender_fault=False, code="InternalError"),
            max_retries=3,
            **kwargs,
        )
        sink = TransformSink(inner, lambda e: dict(e)) if wrapped else inner
        run(sink, events(2))
        out.append(lines(capsys.readouterr().err))
    assert out[0] == out[1], out
    if not fifo:
        assert out[1] == [
            "log-foundry: lost 2 message(s); SQSSink, still failing after 4 attempt(s); abandoned"
        ]


# --- FR-003: LogstashSink delegates ---------------------------------------------------------------


def _socket_logstash(**kwargs: Any) -> LogstashSink:
    kwargs.setdefault("max_retries", 3)
    return LogstashSink(host="loghost", port=5000, transport="udp", **kwargs)


def test_logstash_offers_on_its_backends_counters_and_returns_its_verdict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AC1: in each mode the bound method, the backend's counters, its verdict by identity."""
    sentinel = Verdict(outcomes=(A,))
    handed: list[object] = []

    def spy(self: Any, batch: list[dict[str, object]]) -> Verdict:
        handed.append((self, batch))
        return sentinel

    install(monkeypatch, Socket())
    monkeypatch.setattr(HTTPSink, "deliver", spy)
    monkeypatch.setattr(SocketTransport, "deliver", spy)
    for sink, backend in (
        (lst := LogstashSink(url="https://logstash.example/in"), lst._http),
        (sock := _socket_logstash(), sock._socket),
    ):
        bound = read_deliver(sink)
        assert bound is not None and bound.__self__ is sink  # type: ignore[attr-defined]
        assert sink.log_foundry_counters is backend.log_foundry_counters
        batch = events(1)
        assert sink.deliver(batch) is sentinel
        assert handed[-1] == (backend, batch) and handed[-1][1] is batch  # type: ignore[index]


def test_an_http_logstash_and_a_bare_http_sink_make_the_same_requests() -> None:
    """AC2: ``200`` then ``500`` always, through a worker: equal requests and ledgers."""
    out = []
    for through_logstash in (False, True):
        opener = FakeOpener([FakeResponse(200, b"{}"), FakeResponse(500, b"")])
        sink: Any = (
            LogstashSink(url="http://logstash/in", opener=opener, max_retries=1)
            if through_logstash
            else HTTPSink(
                "http://logstash/in",
                opener=opener,
                body_format="json_array",
                timeout=5.0,
                max_retries=1,
            )
        )
        worker = Worker(sink, batch_size=1000, flush_interval=60.0, max_retries=1)
        install_worker(worker)
        for _ in range(2):
            worker.submit(events(2))
            worker.flush(timeout=30.0)
        out.append((len(opener.calls), sink.losses(), sink.failed, worker.health().failed_batches))
        log_foundry.shutdown(timeout=5.0)
    assert out[0] == out[1], out
    assert out[0][3] == 1


def _syslog(**kwargs: Any) -> SyslogSink:
    kwargs.setdefault("max_retries", 3)
    return SyslogSink("loghost", 514, transport="udp", **kwargs)


def test_a_socket_logstash_and_a_syslog_sink_keep_the_same_ledger(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AC3: a refusing peer, then one that takes everything: equal ``losses()`` and batches."""
    out = []
    for make in (_syslog, _socket_logstash):
        sock = Socket(refuse=REFUSED)
        install(monkeypatch, sock)
        sink: Any = make(max_retries=1)
        worker = Worker(sink, batch_size=1000, flush_interval=60.0, max_retries=1)
        install_worker(worker)
        worker.submit(events(2))
        worker.flush(timeout=30.0)
        sock.refuse = None
        worker.submit(events(2))
        worker.flush(timeout=30.0)
        out.append((sink.losses(), worker.health().failed_batches))
        assert sink.losses() == sink._socket.losses()
        log_foundry.shutdown(timeout=5.0)
    assert out[0] == out[1], out
    assert out[0][1] == 1


def test_a_socket_logstash_counts_each_lost_event_once_per_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AC4: 1.x and SPEC-074 counted each event once per worker attempt — ``2 × 4``; now ``2``."""
    install(monkeypatch, Socket(refuse=REFUSED))
    sink = _socket_logstash()
    worker = Worker(sink, batch_size=1000, flush_interval=60.0)
    install_worker(worker)
    worker.submit(events(2))
    worker.flush(timeout=30.0)
    assert sink.failed == 2
    assert worker.health().failed_batches == 1


def test_a_socket_logstash_stays_inside_the_shutdown_budget_as_syslog_does(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AC5: a 0.5 s connect that refuses, under ``shutdown(timeout=0.9)``: SPEC-062's prediction
    allows the first round and declines the second, whose 0.5 s no longer fits, for both sinks.
    """
    rounds: list[str] = []
    original = SocketTransport._round

    def counted(self: SocketTransport, batch: list[dict[str, object]]) -> Any:
        rounds.append(self._sink_name)
        return original(self, batch)

    def slow_refusal(host: str, port: int, timeout: float) -> Any:
        time.sleep(0.5)
        raise ConnectionRefusedError(errno.ECONNREFUSED, "refused")

    monkeypatch.setattr(SocketTransport, "_round", counted)
    monkeypatch.setattr(socket_mod, "_make_tcp", slow_refusal)
    for make in (
        lambda: SyslogSink("loghost", 514, transport="tcp", timeout=0.5, max_retries=3),
        lambda: LogstashSink(host="loghost", port=5000, transport="tcp", timeout=0.5),
    ):
        sink: Any = make()
        worker = Worker(sink, batch_size=1000, flush_interval=60.0)
        install_worker(worker)
        worker.submit(events(1))
        log_foundry.shutdown(timeout=0.9)
    assert rounds == ["SyslogSink", "LogstashSink"], rounds


def test_an_http_backend_that_offers_nothing_leaves_logstash_offering_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A backend the probe refuses at construction: no offer, a ``TypeError`` naming the class."""
    monkeypatch.setattr(HTTPSink, "deliver", None)
    sink = LogstashSink(url="https://logstash.example/in")
    assert read_deliver(sink) is None and sink.log_foundry_counters is None
    assert sink.max_retries is None and sink.max_retry_after is None
    with pytest.raises(TypeError, match="LogstashSink offers no deliver"):
        sink.deliver(events(1))


def test_a_replaced_http_backend_counter_refuses_naming_its_type() -> None:
    sink = LogstashSink(url="https://logstash.example/in")
    assert sink._http is not None
    sink._http.log_foundry_counters = SinkCounters()
    with pytest.raises(TypeError, match="HTTPSink no longer offers deliver"):
        sink.deliver(events(1))
