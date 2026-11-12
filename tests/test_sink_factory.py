"""SPEC-079 FR-005 and FR-006: ``configure(sink_factory=…)``, and each process building its own sink.

The factory is never called by ``configure()``; the first emit in each process calls it, through
the private ``_FactorySink`` wrapper, and a forked child is handed a new wrapper by the fork
handler so it builds its own. Every promise that is a property of two processes is asserted in a
real child. Serves invariants 1, 2, 4, 5, 6, 10, 12 and 13.
"""

from __future__ import annotations

import io
import sys
import threading
import time
import types
import weakref
from typing import Any, ClassVar

import pytest

import log_foundry
from conftest import install_worker
from log_foundry import _lifecycle
from log_foundry._sink_arg import _UNPATCHABLE
from log_foundry.sinks._factory import _FactorySink
from log_foundry.sinks.base import Sink
from log_foundry.sinks.filtering import FilteringSink
from log_foundry.sinks.memory import MemorySink
from log_foundry.sinks.multi import MultiSink
from log_foundry.sinks.transform import TransformSink
from log_foundry.worker import Worker
from test_fork_lifecycle import run_in_child
from test_sink_ownership import RecordingSink


class _Factory:
    """A factory that records the thread of each call and returns a fresh ``RecordingSink``."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.built: list[RecordingSink] = []

    def __call__(self) -> RecordingSink:
        self.calls.append(threading.current_thread().name)
        sink = RecordingSink()
        self.built.append(sink)
        return sink


def _traced(value: int = 1) -> int:
    @log_foundry.trace
    def work() -> int:
        return value

    return work()


def _capture_stderr() -> tuple[io.StringIO, Any]:
    buffer = io.StringIO()
    saved = sys.stderr
    sys.stderr = buffer
    return buffer, saved


# -- FR-005: the surface and its refusals ----------------------------------------------------------


def test_configure_never_calls_the_factory() -> None:
    """FR-005: ``f`` is not called; ``sink_factory is f``; the configured sink is a ``Sink``."""
    factory = _Factory()
    log_foundry.configure(service="factory", sink_factory=factory)
    config = log_foundry.get_config()
    assert factory.calls == []
    assert config.sink_factory is factory
    assert isinstance(config.sink, Sink) and type(config.sink) is _FactorySink


@pytest.mark.parametrize("path", ["no-worker", "worker"])
def test_sink_and_sink_factory_replace_each_other(path: str) -> None:
    """FR-005: a later ``sink=`` clears the factory, a later factory replaces the sink, X closed once."""
    first = _Factory()
    log_foundry.configure(service="factory", sink_factory=first)
    if path == "worker":
        _traced()
    else:
        log_foundry.info("orphan")
    x = RecordingSink()
    log_foundry.configure(sink=x)
    config = log_foundry.get_config()
    assert config.sink_factory is None and config.sink is x
    log_foundry.info("to x")
    second = _Factory()
    log_foundry.configure(sink_factory=second)
    assert log_foundry.get_config().sink_factory is second
    deadline = time.monotonic() + 5.0
    while x.closed == 0 and time.monotonic() < deadline:
        time.sleep(0.01)
    assert x.closed == 1, f"X closed {x.closed} times on the {path} path"


def test_the_configured_factory_again_is_a_no_op() -> None:
    """FR-005: the same factory a second time performs no drain and no close."""
    factory = _Factory()
    log_foundry.configure(service="factory", sink_factory=factory)
    _traced()
    log_foundry.flush(timeout=5.0)
    wrapper = log_foundry.get_config().sink
    built = factory.built[0]
    log_foundry.configure(sink_factory=factory)
    assert log_foundry.get_config().sink is wrapper
    assert built.closed == 0
    assert log_foundry.health().incomplete_swaps == 0
    assert len(factory.calls) == 1


@pytest.mark.parametrize(
    ("build", "name"),
    [
        (lambda: FilteringSink(object()), "FilteringSink inner"),  # type: ignore[arg-type]
        (lambda: TransformSink(object(), lambda e: e), "TransformSink inner"),  # type: ignore[arg-type]
        (lambda: MultiSink(MemorySink(), object()), "MultiSink sinks[1]"),  # type: ignore[arg-type]
    ],
    ids=["FilteringSink", "TransformSink", "MultiSink"],
)
def test_the_wrappers_refuse_an_unpatchable_inner_sink(build: Any, name: str) -> None:
    """FR-005: each wrapper refuses an unpatchable inner sink, naming the argument and the type."""
    with pytest.raises(
        TypeError,
        match=rf"^{name.replace('[', r'\[').replace(']', r'\]')} must be a sink with an emit method, got object$",
    ):
        build()


class _EmitLater:
    """A class whose ``emit`` is ``None`` until ``start()`` binds it."""

    emit = None

    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []

    def start(self) -> None:
        self.emit = self.events.extend  # type: ignore[assignment]

    def close(self) -> None:
        """Releases nothing."""


class _Slotted:
    __slots__ = ("emit", "events")


class _NoClose:
    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []

    def emit(self, batch: list[dict[str, object]]) -> None:
        self.events.extend(batch)


class _Proxy:
    def __init__(self, inner: object) -> None:
        object.__setattr__(self, "_inner", inner)

    def __getattr__(self, name: str) -> Any:
        return getattr(object.__getattribute__(self, "_inner"), name)


class _Sub(MemorySink):
    pass


def _accepted(shape: str) -> tuple[Any, Any, Any]:
    """Builds one accepted shape: (the value to configure, a binder run after, the event store)."""
    store: list[dict[str, object]] = []
    if shape == "no-close":
        sink = _NoClose()
        return sink, lambda: None, sink.events
    if shape == "emit-none-then-start":
        sink = _EmitLater()
        return sink, sink.start, sink.events
    if shape == "namespace":
        ns = types.SimpleNamespace()
        return ns, lambda: setattr(ns, "emit", store.extend), store
    if shape == "slotted":
        slotted = _Slotted()
        return slotted, lambda: setattr(slotted, "emit", store.extend), store
    if shape == "function":

        def fn() -> None:
            """A function standing in for a sink."""

        return fn, lambda: setattr(fn, "emit", store.extend), store
    if shape == "module":
        module = types.ModuleType("a_sink_module")
        return module, lambda: setattr(module, "emit", store.extend), store
    inner = MemorySink()
    if shape == "proxy":
        return _Proxy(inner), lambda: None, inner.events
    if shape == "weakref-proxy":
        _KEEP.append(inner)
        return weakref.proxy(inner), lambda: None, inner.events
    sub = _Sub()
    _KEEP.append(sub)
    return super(_Sub, sub), lambda: None, sub.events


_KEEP: list[object] = []
_SHAPES = [
    "no-close",
    "emit-none-then-start",
    "namespace",
    "slotted",
    "function",
    "module",
    "proxy",
    "weakref-proxy",
    "super",
]


@pytest.mark.parametrize("shape", _SHAPES)
def test_every_patchable_sink_shape_is_accepted_and_delivers(shape: str) -> None:
    """FR-005: each shape is accepted, and delivers every event once it has an ``emit``."""
    value, bind, store = _accepted(shape)
    log_foundry.configure(service="shape", sink=value)
    bind()
    log_foundry.info("one")
    log_foundry.info("two")
    assert [event["message"] for event in store] == ["one", "two"]


class _NoEmit:
    """A user class that never gains an ``emit``: accepted, and its batches lost counted."""

    def close(self) -> None:
        """Releases nothing."""


@pytest.mark.parametrize("path", ["no-worker", "worker"])
def test_a_user_class_with_no_emit_is_lost_counted(path: str) -> None:
    """FR-005: accepted; counted in ``failed_batches`` or ``orphan_lost``, and announced."""
    log_foundry.configure(service="shape", sink=_NoEmit())  # type: ignore[arg-type]
    buffer, saved = _capture_stderr()
    try:
        if path == "worker":
            install_worker(
                Worker(
                    log_foundry.get_config().sink, max_retries=0, batch_size=1, flush_interval=0.01
                )
            )  # type: ignore[arg-type]
            _traced()
            log_foundry.flush(timeout=5.0)
        else:
            log_foundry.info("lost")
    finally:
        sys.stderr = saved
    health = log_foundry.health()
    if path == "worker":
        assert health.failed_batches == 1, health
    else:
        assert health.orphan_lost == 1, health
    assert "log-foundry:" in buffer.getvalue(), "the loss was announced"


def test_every_unpatchable_type_is_unpatchable() -> None:
    """FR-005: each member's flags, ``__dictoffset__``, ``setattr`` and MRO; proxy and super are not."""
    for kind in _UNPATCHABLE:
        assert not kind.__flags__ & (1 << 9), f"{kind.__name__} is a heap type"
        assert kind.__dictoffset__ == 0, kind
        instance = kind() if kind is not range else range(0)
        with pytest.raises((AttributeError, TypeError)):
            instance.emit = print
        assert all("emit" not in vars(base) for base in kind.__mro__), kind
    assert weakref.ProxyType not in _UNPATCHABLE
    assert super not in _UNPATCHABLE


class _Ran:
    calls: ClassVar[list[str]] = []
    armed: ClassVar[bool] = False


class _Meta(type):
    def __eq__(cls, other: object) -> bool:
        if _Ran.armed:
            _Ran.calls.append("meta __eq__")
        return cls is other

    def __hash__(cls) -> int:
        if _Ran.armed:
            _Ran.calls.append("meta __hash__")
        return id(cls)

    @property
    def __name__(cls) -> str:  # type: ignore[override]
        if _Ran.armed:
            _Ran.calls.append("meta __name__")
        return "Hostile"


class _Hostile(metaclass=_Meta):
    def emit(self, batch: list[dict[str, object]]) -> None:
        """Accepts the batch."""

    def close(self) -> None:
        """Closes nothing."""

    @property  # type: ignore[misc]
    def __class__(self) -> type:  # type: ignore[override]
        if _Ran.armed:
            _Ran.calls.append("__class__ getter")
        return _Hostile

    def __getattr__(self, name: str) -> Any:
        if _Ran.armed:
            _Ran.calls.append(f"__getattr__ {name}")
        raise AttributeError(name)

    def __getattribute__(self, name: str) -> Any:
        if _Ran.armed and name not in {"emit", "close"}:
            _Ran.calls.append(f"__getattribute__ {name}")
        return object.__getattribute__(self, name)


def _armed(action: Any) -> list[str]:
    _Ran.calls.clear()
    _Ran.armed = True
    try:
        action()
    finally:
        _Ran.armed = False
    return list(_Ran.calls)


def test_configuring_runs_no_hook_of_the_sink() -> None:
    """FR-005: no hook of the value runs in ``configure()`` or the three wrapper constructors.

    With no logging started, ``configure(sink=h)`` runs nothing of ``h``. On the swap path the
    one exception is the stop-signal offer, ``_lifecycle.offer_stop_signal``, which is delivery
    machinery and reads ``log_foundry_stop_signal`` by name; it is excepted by that name.
    """
    hostile = _Hostile()
    assert _armed(lambda: log_foundry.configure(service="hooks", sink=hostile)) == []
    for build in (
        lambda: FilteringSink(hostile),
        lambda: TransformSink(hostile, lambda e: e),
        lambda: MultiSink(hostile),
    ):
        assert _armed(build) == []
    log_foundry.configure(sink=MemorySink())
    _traced()
    swapped = _armed(lambda: log_foundry.configure(sink=_Hostile()))
    assert all("log_foundry_stop_signal" in call for call in swapped), swapped


# -- FR-006: when the factory runs -----------------------------------------------------------------


def test_the_factory_runs_on_the_first_emit_of_each_path() -> None:
    """FR-006: zero at configure; the orphan path calls it on the caller; ``@trace`` on the drain."""
    factory = _Factory()
    log_foundry.configure(service="factory", sink_factory=factory)
    assert factory.calls == []
    log_foundry.info("orphan")
    assert factory.calls == [threading.current_thread().name]

    gate = threading.Event()
    slow = _Factory()
    original = slow.__call__

    def blocking() -> RecordingSink:
        gate.wait(5.0)
        return original()

    log_foundry.configure(sink_factory=blocking)
    returned_first: list[bool] = []
    returned = _traced(7)
    returned_first.append(not slow.calls)
    gate.set()
    log_foundry.flush(timeout=5.0)
    assert returned == 7 and returned_first == [True], "the traced call returned before the build"
    assert slow.calls == ["log-foundry-worker"], slow.calls


def test_concurrent_first_emits_build_once() -> None:
    """FR-006: eight threads' first orphan ``info()`` call ``f`` once and all eight land."""
    factory = _Factory()
    log_foundry.configure(service="factory", sink_factory=factory)
    start = threading.Barrier(8)

    def one(n: int) -> None:
        start.wait(5.0)
        log_foundry.info(f"event-{n}")

    threads = [threading.Thread(target=one, args=(n,)) for n in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10.0)
    assert len(factory.calls) == 1
    assert sorted(e["message"] for e in factory.built[0].events) == sorted(
        f"event-{n}" for n in range(8)
    )


def test_an_emit_from_inside_the_factory_is_refused_not_deadlocked() -> None:
    """FR-006: a factory calling ``info()`` returns, is called once, and loses that one event."""
    built: list[RecordingSink] = []

    def reentrant() -> RecordingSink:
        log_foundry.info("from-inside")
        sink = RecordingSink()
        built.append(sink)
        return sink

    log_foundry.configure(service="factory", sink_factory=reentrant)
    _buffer, saved = _capture_stderr()
    worker = threading.Thread(target=log_foundry.info, args=("triggering",), daemon=True)
    try:
        worker.start()
        worker.join(5.0)
    finally:
        sys.stderr = saved
    assert not worker.is_alive(), "the re-entrant build deadlocked"
    assert len(built) == 1
    assert log_foundry.health().orphan_lost == 1
    assert [e["message"] for e in built[0].events] == ["triggering"]


def test_a_failing_factory_is_counted_announced_and_retried_on_the_orphan_path() -> None:
    """FR-006: two refused connections, then a build; two losses, one throttled line, two orphan lines."""
    refusals = [ConnectionRefusedError(61, "refused"), ConnectionRefusedError(61, "refused")]
    kind = type(refusals[0]).__name__
    built: list[RecordingSink] = []

    def flaky() -> RecordingSink:
        if refusals:
            raise refusals.pop(0)
        sink = RecordingSink()
        built.append(sink)
        return sink

    log_foundry.configure(service="factory", sink_factory=flaky)
    buffer, saved = _capture_stderr()
    try:
        for n in range(3):
            log_foundry.info(f"event-{n}")
    finally:
        sys.stderr = saved
    lines = buffer.getvalue().splitlines()
    assert log_foundry.health().orphan_lost == 2
    factory_lines = [line for line in lines if "sink_factory" in line]
    assert len(factory_lines) == 1 and f"({kind})" in factory_lines[0], lines
    assert len([line for line in lines if "SinkDeliveryError" in line]) == 2, lines
    assert [e["message"] for e in built[0].events] == ["event-2"]


@pytest.mark.parametrize("outcome", ["recovers", "always"])
def test_a_failing_factory_is_retried_by_the_worker(outcome: str) -> None:
    """FR-006: with ``max_retries=3`` two failures recover; one that always fails abandons once."""
    failures = [2 if outcome == "recovers" else 10**6]
    built: list[RecordingSink] = []

    def flaky() -> RecordingSink:
        if failures[0] > 0:
            failures[0] -= 1
            raise ConnectionRefusedError(61, "refused")
        sink = RecordingSink()
        built.append(sink)
        return sink

    log_foundry.configure(service="factory", sink_factory=flaky)
    sink = log_foundry.get_config().sink
    assert sink is not None
    install_worker(Worker(sink, max_retries=3, batch_size=1, flush_interval=0.01))
    buffer, saved = _capture_stderr()
    try:
        _traced()
        log_foundry.flush(timeout=10.0)
    finally:
        sys.stderr = saved
    health = log_foundry.health()
    if outcome == "recovers":
        assert health.failed_batches == 0, health
        assert built and built[0].events, "the batch was delivered"
    else:
        assert health.failed_batches == 1, health
        assert "abandon" in buffer.getvalue(), buffer.getvalue()


def test_a_factory_returning_a_non_sink_is_a_counted_loss() -> None:
    """FR-006: ``object()`` makes the emit a counted, announced loss naming ``object``."""
    log_foundry.configure(service="factory", sink_factory=object)  # type: ignore[arg-type]
    buffer, saved = _capture_stderr()
    try:
        log_foundry.info("lost")
    finally:
        sys.stderr = saved
    assert log_foundry.health().orphan_lost == 1
    line = next(line for line in buffer.getvalue().splitlines() if "sink_factory" in line)
    assert "returned object, not a sink" in line, line
    assert "AttributeError" not in buffer.getvalue()


def test_shutdown_closes_what_the_factory_built_once() -> None:
    """FR-006: the built sink is closed once and reads releasable; nothing built closes nothing."""
    factory = _Factory()
    log_foundry.configure(service="factory", sink_factory=factory)
    log_foundry.info("build it")
    built = factory.built[0]
    assert _lifecycle.releasable(built)
    log_foundry.shutdown(3.0)
    assert built.closed == 1

    unbuilt = _Factory()
    log_foundry.configure(sink_factory=unbuilt)
    log_foundry.shutdown(3.0)
    assert unbuilt.calls == [] and unbuilt.built == []


def test_shutdown_is_bounded_while_the_factory_is_blocked() -> None:
    """FR-006: ``shutdown(timeout=1.0)`` with the drain inside a 3 s factory returns within its bound."""
    gate = threading.Event()
    built: list[RecordingSink] = []

    def slow() -> RecordingSink:
        gate.wait(3.0)
        sink = RecordingSink()
        built.append(sink)
        return sink

    log_foundry.configure(service="factory", sink_factory=slow)
    _traced()
    deadline = time.monotonic() + 5.0
    while not _lifecycle._state._closes.owed and time.monotonic() < deadline:
        time.sleep(0.01)
    started = time.monotonic()
    log_foundry.shutdown(timeout=1.0)
    elapsed = time.monotonic() - started
    assert elapsed < 2.9, f"shutdown waited {elapsed:.2f}s on a factory blocked for 3s"
    gate.set()
    deadline = time.monotonic() + 5.0
    while not built and time.monotonic() < deadline:
        time.sleep(0.01)
    assert built, "the build completed after the bounded shutdown"
    log_foundry.shutdown(3.0)
    assert built[0].closed == 1


# -- FR-006: in a forked child ---------------------------------------------------------------------


@pytest.mark.parametrize("path", ["no-worker", "worker", "worker-inside-emit"])
def test_a_forked_child_builds_its_own_sink(path: str) -> None:
    """FR-006: the handler calls no factory; the child's events land in its own; I0 is untouched."""
    factory = _Factory()
    log_foundry.configure(service="factory", sink_factory=factory)
    release = threading.Event()
    if path == "no-worker":
        log_foundry.info("parent builds I0")
    else:
        _traced()
        log_foundry.flush(timeout=5.0)
    i0 = factory.built[0]
    if path == "worker-inside-emit":
        inside = threading.Event()
        original = i0.emit

        def parked_emit(batch: list[dict[str, object]]) -> None:
            inside.set()
            release.wait(5.0)
            original(batch)

        i0.emit = parked_emit  # type: ignore[method-assign]
        _traced()
        assert inside.wait(5.0), "the drain thread never entered I0.emit"
    calls_at_fork = len(factory.calls)
    closes_before = i0.closed

    def in_child() -> str:
        handler_calls = len(factory.calls) - calls_at_fork
        inherited = log_foundry.health().inherited_sink
        log_foundry.info("child-event")
        if path != "no-worker":
            _traced()
            log_foundry.flush(timeout=5.0)
        mine = factory.built[-1]
        events = [e["message"] for e in mine.events]
        log_foundry.shutdown(3.0)
        return (
            f"handler={handler_calls} own={mine is not i0} event={'child-event' in events} "
            f"closed={mine.closed} i0={i0.closed - closes_before} inherited={inherited}"
        )

    try:
        child = run_in_child(in_child)
    finally:
        release.set()
    assert child.output == ("handler=0 own=True event=True closed=1 i0=0 inherited=False"), (
        child.output
    )


def test_the_at_fork_handler_never_calls_the_factory() -> None:
    """FR-006: a factory that blocks forever does not delay the child's return from ``fork``."""
    built: list[int] = []
    never = threading.Event()

    def blocks_after_the_first() -> RecordingSink:
        if built:
            never.wait()
        built.append(1)
        return RecordingSink()

    log_foundry.configure(service="factory", sink_factory=blocks_after_the_first)
    log_foundry.info("parent builds")
    started = time.monotonic()
    child = run_in_child(lambda: f"{time.monotonic() - started < 3.0},{len(built)}", timeout=5)
    assert child.finished, child.ending
    assert child.output == "True,1", child.output


def test_a_grandchild_builds_its_own_and_a_childs_sink_is_inherited() -> None:
    """FR-006: a grandchild builds again; ``configure(sink=X)`` in a child is what its children get."""
    factory = _Factory()
    log_foundry.configure(service="factory", sink_factory=factory)
    log_foundry.info("parent")

    def in_child() -> str:
        log_foundry.info("child")
        child_sink = factory.built[-1]
        grandchild = run_in_child(
            lambda: f"{(log_foundry.info('g'), factory.built[-1] is not child_sink)[1]}"
        )
        x = RecordingSink()
        log_foundry.configure(sink=x)
        inherits = run_in_child(lambda: f"{log_foundry.get_config().sink is x}")
        return f"{grandchild.output},{inherits.output}"

    child = run_in_child(in_child, timeout=12)
    assert child.output == "True,True", child.output


class _SignalledSink:
    """A built sink declaring the stop-signal member, so the hand-off at build is observable."""

    log_foundry_stop_signal = None

    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []
        self.closed = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Keeps the batch."""
        self.events.extend(batch)

    def close(self) -> None:
        """Counts the close."""
        self.closed += 1


def _signalled_factory() -> tuple[Any, list[_SignalledSink]]:
    built: list[_SignalledSink] = []

    def factory() -> _SignalledSink:
        sink = _SignalledSink()
        built.append(sink)
        return sink

    return factory, built


def test_the_built_sink_is_handed_the_workers_stop_signal() -> None:
    """FR-006: what the factory built holds the worker's ``_stop`` after the first ``@trace``."""
    factory, built = _signalled_factory()
    log_foundry.configure(service="factory", sink_factory=factory)
    _traced()
    log_foundry.flush(timeout=5.0)
    worker = _lifecycle._state.worker_exists()
    assert worker is not None and built
    assert vars(built[0]).get("log_foundry_stop_signal") is worker._stop


def _child_of_a_traced_factory(monkeypatch: pytest.MonkeyPatch, *, ceiling: int | None) -> str:
    factory, built = _signalled_factory()
    if ceiling is not None:
        monkeypatch.setattr(_lifecycle, "_MARKING_CEILING", ceiling)
    log_foundry.configure(service="factory", sink_factory=factory)
    _traced()
    log_foundry.flush(timeout=5.0)

    def in_child() -> str:
        _traced()
        log_foundry.flush(timeout=5.0)
        mine = built[-1]
        adopted = log_foundry.get_config().sink
        owed = id(adopted) in _lifecycle._state._closes.owed
        log_foundry.shutdown(3.0)
        return f"owed={owed} closed={mine.closed}"

    return run_in_child(in_child).output


def test_a_trace_only_child_owes_and_closes_its_own_built_sink(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-006: the adopted wrapper is armed on the worker, so ``shutdown()`` closes the child's sink."""
    assert _child_of_a_traced_factory(monkeypatch, ceiling=None) == "owed=True closed=1"


def test_a_factory_child_owns_its_wrapper_when_the_marking_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-006: the child stamps its adopted wrapper, so even a failed marking leaves it releasable.

    With ``_MARKING_CEILING`` at zero every child's marking fails and refuses every sink it has
    no record of; only the adoption's own stamp makes the child's wrapper, and through it the sink
    the child built, the child's to close.
    """
    assert _child_of_a_traced_factory(monkeypatch, ceiling=0).endswith("closed=1")


def test_a_build_mark_inherited_across_a_fork_does_not_refuse_the_childs_thread() -> None:
    """The re-entry mark is keyed by process and thread, so an inherited mark matches no child thread.

    The fork is taken from inside the factory, on the parent's main thread, so the child inherits
    the wrapper mid-build: its claim naming the thread that called ``fork`` (once its build lock,
    held, which the walk replaced) and its re-entry mark naming
    the thread that called ``fork``, which is the child's own main thread. The caller wrapped the
    factory's sink itself, so the config carries no factory and the child keeps the inherited
    wrapper rather than adopting a new one. Keyed on the thread ident alone, every emit in that
    child was refused as a re-entry, forever.
    """
    outcome: list[str] = []

    def factory() -> RecordingSink:
        if not outcome:
            outcome.append("forking")
            child = run_in_child(
                lambda: (log_foundry.info("child"), f"lost={log_foundry.health().orphan_lost}")[1]
            )
            outcome.append(child.output)
        return RecordingSink()

    log_foundry.configure(service="factory", sink_factory=factory)
    inherited = log_foundry.get_config().sink
    assert inherited is not None
    log_foundry.configure(sink=FilteringSink(inherited))
    assert log_foundry.get_config().sink_factory is None
    log_foundry.info("parent")
    assert outcome == ["forking", "lost=0"], outcome
