"""SPEC-091 — an open span's events ship on the worker's interval.

Every open span is registered from its push to the start of its close, the drain thread takes
what each holds past its held ``span.start`` about once a flush interval, ``flush()`` reaches
every registered span, and the sweep lock is never re-entered on its own thread nor waited on
unboundedly by the drain.

The interleavings are forced, not raced: an owner parked inside ``list.append``, a sweep parked
between its membership read and its take, a close parked inside its backfill, a collector callback
run at a chosen point inside a hold, and a clock the test advances. Each forced case names the
event it would lose or misplace, so a regression reads as its reason rather than as a count.
"""

from __future__ import annotations

import asyncio
import contextvars
import gc
import os
import sys
import threading
import time
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry as lf
from conftest import install_worker, run_concurrently
from ledger_sinks import GatedSink
from log_foundry import _fork, _ledger, _lifecycle, context, decorator
from log_foundry import worker as worker_mod
from log_foundry.model import Span
from test_fork_lifecycle import report_from_child, run_in_child

if TYPE_CHECKING:
    from collections.abc import Callable

BOUND = 10.0
"""Seconds any forced handshake may take before the test fails rather than hangs."""

TRACEPARENT = "00-" + "c" * 32 + "-" + "d" * 16 + "-01"
OTHER_TRACEPARENT = "00-" + "e" * 32 + "-" + "f" * 16 + "-01"


class Recorder:
    """Records each batch the sink is handed, thread-safely."""

    def __init__(self) -> None:
        self.batches: list[list[dict[str, Any]]] = []
        self._lock = threading.Lock()

    def emit(self, batch: list[dict[str, Any]]) -> None:
        with self._lock:
            self.batches.append([dict(e) for e in batch])

    def close(self) -> None:
        return None

    @property
    def events(self) -> list[dict[str, Any]]:
        with self._lock:
            return [e for b in self.batches for e in b]

    def messages(self, function: str | None = None) -> list[object]:
        return [
            e["message"] for e in self.events if function is None or e.get("function") == function
        ]


class Parked:
    """A traced coroutine suspended inside its open span, in a context of its own.

    The span is a real one: ``_begin`` credits and registers it, its logs are real level calls,
    and :meth:`finish` resumes the coroutine so the span closes through ``_end`` as any call's
    does. Its context is its own, so the test thread's context holds no span.
    """

    def __init__(self, *messages: str, name: str = "parked") -> None:
        self.name = name
        self.loop = asyncio.new_event_loop()
        self.future: asyncio.Future[None] = self.loop.create_future()
        self.ctx = contextvars.copy_context()
        self.span: Span | None = None

        @lf.trace(name=name)
        async def body() -> None:
            self.span = context.current_span()
            for message in messages:
                lf.info(message)
            await self.future

        self.coro = body()
        self.ctx.run(self.coro.send, None)

    def log(self, message: str) -> None:
        self.ctx.run(lf.info, message)

    def set_baggage(self, **kv: object) -> None:
        self.ctx.run(lf.set_baggage, **kv)

    def finish(self) -> None:
        if not self.future.done():
            self.future.set_result(None)
            try:
                self.ctx.run(self.coro.send, None)
            except StopIteration:
                pass
        self.loop.close()


def _quiet(sink: object, **settings: Any) -> worker_mod.Worker:
    """Installs a sweeping worker whose drain never sweeps or emits on its own clock."""
    merged: dict[str, Any] = {"flush_interval": 3600.0, "sweep": decorator._sweep_registered}
    merged.update(settings)
    lf.configure(sink=sink)  # type: ignore[arg-type]
    worker = worker_mod.Worker(sink, **merged)  # type: ignore[arg-type]
    install_worker(worker)
    return worker


def _built(sink: object) -> worker_mod.Worker:
    """Builds the process worker the library's own way, so its drain sweeps."""
    lf.configure(sink=sink)  # type: ignore[arg-type]
    return _lifecycle._get_worker()


def _wait(condition: Callable[[], bool], what: str, bound: float = BOUND) -> None:
    deadline = time.monotonic() + bound
    while not condition():
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        time.sleep(0.005)


def _once(events: list[dict[str, Any]]) -> list[str]:
    """Each log id delivered more than once, or an empty list."""
    seen: dict[object, int] = {}
    for event in events:
        seen[event["log_id"]] = seen.get(event["log_id"], 0) + 1
    return [str(k) for k, n in seen.items() if n > 1]


def _traced(mode: str, body: Callable[[], None], name: str = "call") -> None:
    """Runs ``body`` inside one traced call, through the sync or the async wrapper (AC-9)."""
    if mode == "sync":

        @lf.trace(name=name)
        def call() -> None:
            body()

        call()
    else:

        @lf.trace(name=name)
        async def acall() -> None:
            body()

        asyncio.run(acall())


def _err_lines(capfd: pytest.CaptureFixture[str], needle: str) -> list[str]:
    return [line for line in capfd.readouterr().err.splitlines() if needle in line]


# ----------------------------------------------------------------------------- FR-001 AC-2


class _PausingList(list):  # type: ignore[type-arg]
    """A span buffer whose ``append`` parks once, before or after inserting, on a chosen event."""

    def __init__(self, items: list[dict[str, Any]], *, after: bool, message: str) -> None:
        super().__init__(items)
        self.after = after
        self.message = message
        self.parked = threading.Event()
        self.go = threading.Event()

    def append(self, item: dict[str, Any]) -> None:
        hit = item.get("message") == self.message and not self.parked.is_set()
        if hit and not self.after:
            self.parked.set()
            assert self.go.wait(BOUND)
        super().append(item)
        if hit and self.after:
            self.parked.set()
            assert self.go.wait(BOUND)


def test_an_append_parked_before_its_insert_is_taken_exactly_once() -> None:
    """FR-001 AC-2: the take deletes in place, so an append that read the list lands on it.

    A take that rebound ``span.events`` left the parked append inserting into the list the take
    had detached, so the event was on neither the span nor the queue: lost.
    """
    sink = Recorder()
    worker = _quiet(sink)
    parked = Parked("before")
    assert parked.span is not None
    buffer = _PausingList(parked.span.events, after=False, message="paused")
    parked.span.events = buffer
    owner = threading.Thread(daemon=True, target=parked.log, args=("paused",))
    owner.start()
    try:
        assert buffer.parked.wait(BOUND), "the owner never reached its append"
        result = decorator._sweep_registered(worker._hand_in)
        assert result is not None and result[0] == 1, result
    finally:
        buffer.go.set()
        owner.join(BOUND)
    parked.finish()
    lf.flush(BOUND)
    got = sink.messages("parked")
    assert got.count("paused") == 1, f"the event appended across the take was lost: {got}"
    assert sorted(got) == sorted(["span.start", "before", "paused", "span.end"]), got


class _CopyParkedRegistry(dict):  # type: ignore[type-arg]
    """``_open_spans`` whose ``copy`` parks the sweep once, just after the registry is copied."""

    def __init__(self, items: dict[int, Span], thread: list[int]) -> None:
        super().__init__(items)
        self.thread = thread
        self.parked = threading.Event()
        self.go = threading.Event()

    def copy(self) -> dict[int, Span]:
        copied = dict(self)
        if threading.get_ident() in self.thread and not self.parked.is_set():
            self.parked.set()
            assert self.go.wait(BOUND)
        return copied


def test_a_close_parked_after_appending_span_end_is_not_swept_early(monkeypatch) -> None:
    """FR-001 AC-2: the close leaves the registry first, and the sweep reads membership after length.

    The sweep copies the registry while the span is still in it, then the close removes the span,
    appends ``span.end`` and is parked there; the sweep's length now counts ``span.end``, and its
    membership read fails, so it takes nothing. Without that read, the take shipped ``span.end``
    with the logs, ahead of the close's own ``span.start``.
    """
    sink = Recorder()
    worker = _quiet(sink)
    parked = Parked("a", "b")
    assert parked.span is not None
    buffer = _PausingList(parked.span.events, after=True, message="span.end")
    parked.span.events = buffer
    sweeper: list[int] = []
    registry = _CopyParkedRegistry(dict(decorator._open_spans), sweeper)
    monkeypatch.setattr(decorator, "_open_spans", registry)
    result: list[object] = []

    def sweep() -> None:
        sweeper.append(threading.get_ident())
        result.append(decorator._sweep_registered(worker._hand_in))

    sweeping = threading.Thread(daemon=True, target=sweep)
    closer = threading.Thread(daemon=True, target=parked.finish)
    sweeping.start()
    try:
        assert registry.parked.wait(BOUND), "the sweep never copied the registry"
        closer.start()
        assert buffer.parked.wait(BOUND), "the close never appended span.end"
        registry.go.set()
        sweeping.join(BOUND)
        assert result and result[0] is not None, result
        assert result[0][0] == 0, f"a closing span was taken from: {result}"  # type: ignore[index]
    finally:
        registry.go.set()
        buffer.go.set()
        sweeping.join(BOUND)
        closer.join(BOUND)
    lf.flush(BOUND)
    got = sink.messages("parked")
    assert got[-1] == "span.end", f"span.end left before the span's other events: {got}"
    assert sorted(got) == sorted(["span.start", "a", "b", "span.end"]), got


class _PausingEvent(dict):  # type: ignore[type-arg]
    """A buffered event whose ``get("message")`` parks the close's backfill once."""

    def __init__(self, event: dict[str, Any], thread: list[int]) -> None:
        super().__init__(event)
        self.thread = thread
        self.parked = threading.Event()
        self.go = threading.Event()

    def get(self, key: object, default: object = None) -> object:
        if key == "message" and threading.get_ident() in self.thread and not self.parked.is_set():
            self.parked.set()
            assert self.go.wait(BOUND)
        return super().get(key, default)


class _PausingRegistry(dict):  # type: ignore[type-arg]
    """``_open_spans`` whose ``get`` parks the sweep once, just after the membership read."""

    def __init__(self, items: dict[int, Span], thread: list[int]) -> None:
        super().__init__(items)
        self.thread = thread
        self.parked = threading.Event()
        self.go = threading.Event()

    def get(self, key: object, default: object = None) -> object:
        found = super().get(key, default)
        if threading.get_ident() in self.thread and not self.parked.is_set():
            self.parked.set()
            assert self.go.wait(BOUND)
        return found


def test_a_backfill_parked_mid_iteration_keeps_every_boundary_baggage(monkeypatch) -> None:
    """FR-001 AC-2: the close's backfill iterates a snapshot, so a take cannot shift it.

    The sweep is parked between its membership read and its take, the close then starts its
    backfill and is parked on the second log, and the take deletes the logs while the backfill is
    part-way through. Over the live list the iterator stepped past ``span.end``, which then
    shipped without the span's baggage.
    """
    sink = Recorder()
    worker = _quiet(sink)
    parked = Parked("a", "b")
    parked.set_baggage(tenant="t1")
    assert parked.span is not None
    sweeper: list[int] = []
    closer_id: list[int] = []
    pausing_event = _PausingEvent(parked.span.events[2], closer_id)
    parked.span.events[2] = pausing_event
    registry = _PausingRegistry(dict(decorator._open_spans), sweeper)
    monkeypatch.setattr(decorator, "_open_spans", registry)

    def sweep() -> None:
        sweeper.append(threading.get_ident())
        decorator._sweep_registered(worker._hand_in)

    def close() -> None:
        closer_id.append(threading.get_ident())
        parked.finish()

    sweeping = threading.Thread(daemon=True, target=sweep)
    closing = threading.Thread(daemon=True, target=close)
    sweeping.start()
    try:
        assert registry.parked.wait(BOUND), "the sweep never read membership"
        closing.start()
        assert pausing_event.parked.wait(BOUND), "the close never reached its backfill"
        registry.go.set()
        sweeping.join(BOUND)
        assert not sweeping.is_alive()
    finally:
        registry.go.set()
        pausing_event.go.set()
        sweeping.join(BOUND)
        closing.join(BOUND)
    lf.flush(BOUND)
    boundary = {
        e["message"]: e.get("fields", {}).get("tenant")
        for e in sink.events
        if e.get("function") == "parked" and e["message"] in ("span.start", "span.end")
    }
    assert boundary == {"span.start": "t1", "span.end": "t1"}, (
        f"a boundary event lost its baggage to a take during the backfill: {boundary}"
    )


# ------------------------------------------------------------------------ FR-001 AC-4 to AC-9


def test_a_take_whose_hand_in_raises_is_counted_lost(capfd) -> None:
    """FR-001 AC-4: the faulted take is in ``in_span_lost``, one line says so, the rest ship."""
    sink = Recorder()
    worker = _quiet(sink)
    kept = Parked("x1", "x2", name="kept")
    faulted = Parked("boom", name="faulted")
    real = worker._hand_in

    def hand_in(events: list[dict[str, Any]]) -> tuple[int, int]:
        if any(e.get("message") == "boom" for e in events):
            raise RuntimeError("hand-in fault")
        return real(events)

    worker._hand_in = hand_in  # type: ignore[method-assign]
    before = lf.health().in_span_lost
    capfd.readouterr()
    assert worker._run_sweep() == 2
    assert lf.health().in_span_lost - before == 1, "exactly the faulted take's one event"
    assert len(_err_lines(capfd, "the events taken were lost")) == 1
    del worker._hand_in
    lf.flush(BOUND)
    assert sorted(sink.messages("kept")) == ["x1", "x2"], "the other span's take was delivered"
    kept.finish()
    faulted.finish()
    lf.flush(BOUND)
    assert "boom" not in sink.messages("faulted")


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_a_span_is_registered_from_its_push_until_its_close_begins(mode, monkeypatch) -> None:
    """FR-001 AC-5 and AC-9: in the registry inside the body, out of it once the close begins."""
    _quiet(Recorder())
    seen: dict[str, object] = {}
    real_end_event = decorator.end_event

    def end_event(span: Span, status: str, exc: BaseException | None) -> dict[str, object]:
        seen["at_close"] = decorator._open_spans.get(id(span)) is span
        return real_end_event(span, status, exc)

    monkeypatch.setattr(decorator, "end_event", end_event)

    def body() -> None:
        span = context.current_span()
        seen["span"] = span
        seen["inside"] = span is not None and decorator._open_spans.get(id(span)) is span

    _traced(mode, body)
    assert seen["inside"] is True, "registered between its push and its close"
    assert seen["at_close"] is False, "removed before span.end is built"
    assert id(seen["span"]) not in decorator._open_spans


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_a_worker_sweep_leaves_span_start_for_the_close(mode, monkeypatch) -> None:
    """FR-001 AC-6 and AC-9: ``span.start`` ships with ``span.end``, both with late baggage."""
    worker = _quiet(Recorder())
    submissions: list[list[dict[str, Any]]] = []
    real_put = worker._queue._put

    def put(item: object) -> None:
        if isinstance(item, list):
            submissions.append([dict(e) for e in item])
        real_put(item)

    monkeypatch.setattr(worker._queue, "_put", put)

    def body() -> None:
        for i in range(10):
            lf.info(f"log{i}")
        assert decorator._sweep_registered(worker._hand_in) is not None
        lf.set_baggage(tenant="late")

    _traced(mode, body)
    swept = [[e["message"] for e in s] for s in submissions if s[0]["message"] != "span.start"]
    assert swept == [[f"log{i}" for i in range(10)]], f"the sweep took span.start: {swept}"
    closing = [s for s in submissions if s[0]["message"] == "span.start"]
    assert len(closing) == 1 and [e["message"] for e in closing[0]] == ["span.start", "span.end"]
    assert [e["fields"].get("tenant") for e in closing[0]] == ["late", "late"], closing


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_an_in_span_flush_ships_span_start_and_releases_the_hold(mode) -> None:
    """FR-001 AC-7 and AC-9: the owner's flush ships span.start, and a later sweep takes from 0."""
    sink = Recorder()
    worker = _quiet(sink)
    seen: dict[str, object] = {}

    def body() -> None:
        lf.set_baggage(tenant="early")
        lf.info("x")
        lf.flush(BOUND)
        span = context.current_span()
        assert span is not None
        seen["held"] = span.held
        lf.info("y")
        seen["swept"] = decorator._sweep_registered(worker._hand_in)
        seen["left"] = list(span.events)

    _traced(mode, body)
    lf.flush(BOUND)
    assert seen["held"] == 0, "the in-span flush shipped span.start, so nothing is held"
    assert seen["swept"] is not None and seen["swept"][0] == 1  # type: ignore[index]
    assert seen["left"] == [], "the later sweep took from index 0"
    starts = [e for e in sink.events if e["message"] == "span.start"]
    assert len(starts) == 1 and starts[0]["fields"].get("tenant") == "early", starts


def test_continue_trace_after_a_worker_sweep_is_refused(capfd) -> None:
    """FR-001 AC-8: a sweep that took from the root span refuses a later adoption."""
    worker = _quiet(Recorder())
    seen: dict[str, object] = {}

    @lf.trace
    def root() -> None:
        lf.info("logged first")
        decorator._sweep_registered(worker._hand_in)
        seen["result"] = lf.continue_trace(TRACEPARENT)

    capfd.readouterr()
    root()
    result = seen["result"]
    assert not result and result.reason == "rejected", result  # type: ignore[attr-defined]
    lines = _err_lines(capfd, "trace context refused")
    assert len(lines) == 1 and "events have already been handed to the worker" in lines[0], lines


# ---------------------------------------------------------- FR-001: re-entry on the holding thread


class _Collecting(list):  # type: ignore[type-arg]
    """A span buffer whose slice read runs the collector once, as an allocation inside a take can."""

    def __init__(self, items: list[dict[str, Any]]) -> None:
        super().__init__(items)
        self.armed = True
        self.collecting = False

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, slice) and self.armed:
            self.armed = False
            self.collecting = True
            try:
                gc.collect()
            finally:
                self.collecting = False
        return list.__getitem__(self, key)


def test_a_collector_callback_that_closes_the_span_mid_take_defers_its_close() -> None:
    """FR-001 (added 2026-11-17): the sweeping thread never waits on its own hold.

    A collector callback runs between the take's slice and its ``del``, on the sweeping thread,
    and finalizes the very traced coroutine whose span the sweep is taking from. With a plain
    ``Lock`` the close took the lock again and the sweep never returned; with an ``RLock`` and no
    deferral the nested close swapped the list part-way through the take, so ``span.end`` shipped
    ahead of the logs and the ledger was 3 events short.
    """
    sink = Recorder()
    worker = _quiet(sink)
    parked = Parked("log0", "log1", "log2")
    assert parked.span is not None
    buffer = _Collecting(parked.span.events)
    parked.span.events = buffer
    fired: list[bool] = []

    def callback(phase: str, info: dict[str, Any]) -> None:
        if phase == "start" and buffer.collecting and not fired:
            fired.append(True)
            parked.ctx.run(parked.coro.close)

    result: list[object] = []
    gc.callbacks.append(callback)
    try:
        sweeping = threading.Thread(
            daemon=True, target=lambda: result.append(decorator._sweep_registered(worker._hand_in))
        )
        sweeping.start()
        sweeping.join(5.0)
    finally:
        gc.callbacks.remove(callback)
    assert not sweeping.is_alive(), "the sweeping thread waited on the hold it held"
    assert fired, "the callback never ran inside the take"
    lf.flush(BOUND)
    parked.loop.close()
    got = sink.messages("parked")
    assert sorted(got) == sorted(["log0", "log1", "log2", "span.start", "span.end"]), got
    assert got[-1] == "span.end", f"the nested close shipped ahead of the take: {got}"
    assert not _once(sink.events)
    assert lf.health().events.unaccounted == 0


class _WatchedLock:
    """``decorator._sweep_lock``, which runs the collector once when one thread next takes it."""

    def __init__(self, real: Any) -> None:
        self.real = real
        self.arm_for: int | None = None
        self.collecting: int | None = None
        self.attempts: list[tuple[int, bool]] = []

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        self.attempts.append((threading.get_ident(), timeout != -1))
        got = bool(self.real.acquire(blocking, timeout))
        if got and self.arm_for == threading.get_ident():
            self.arm_for = None
            self.collecting = threading.get_ident()
            try:
                gc.collect()
            finally:
                self.collecting = None
        return got

    def release(self) -> None:
        self.real.release()

    def _is_owned(self) -> bool:
        return bool(self.real._is_owned())


def _enter_sweep_registered(worker: worker_mod.Worker, arm: Callable[[], None]) -> None:
    parked = Parked("outer", name="outer")
    arm()
    decorator._sweep_registered(worker._hand_in)
    parked.finish()


def _enter_sweep_open_spans(worker: worker_mod.Worker, arm: Callable[[], None]) -> None:
    @lf.trace(name="outer")
    def outer() -> None:
        lf.info("outer")
        arm()
        lf.flush(BOUND)

    outer()


def _enter_flush(worker: worker_mod.Worker, arm: Callable[[], None]) -> None:
    @lf.trace(name="outer")
    def outer() -> None:
        lf.info("outer")
        arm()

    outer()


def _enter_abandon(worker: worker_mod.Worker, arm: Callable[[], None]) -> None:
    span, token, scope = decorator._begin("outer", None)
    assert span is not None
    lf.info("outer")
    arm()
    decorator._note_in_span_loss(decorator._abandon(span))
    if token is not None:
        context.pop_span(token)
    if scope is not None:
        context.pop_baggage_scope(scope)


def _enter_continue_trace(worker: worker_mod.Worker, arm: Callable[[], None]) -> None:
    @lf.trace(name="outer")
    def outer() -> None:
        lf.info("outer")
        arm()
        lf.continue_trace(TRACEPARENT)

    outer()


@pytest.mark.parametrize(
    "enter",
    [
        _enter_sweep_registered,
        _enter_sweep_open_spans,
        _enter_flush,
        _enter_abandon,
        _enter_continue_trace,
    ],
    ids=["_sweep_registered", "_sweep_open_spans", "_flush", "_abandon", "continue_trace"],
)
def test_each_hold_site_survives_nested_calls_from_a_collector_callback(enter, monkeypatch) -> None:
    """FR-001 (added 2026-11-17): at each hold site, on a thread other than the drain's.

    A collector callback runs inside the hold and makes a ``flush()``, a ``continue_trace()`` and
    a traced call. Each returns: the ``flush()`` reports ``"abandoned"``, the ``continue_trace()``
    is refused, and the traced call's close is deferred to the holder, so every event still
    reaches the sink exactly once.
    """
    sink = Recorder()
    worker = _quiet(sink)
    lock = _WatchedLock(decorator._sweep_lock)
    monkeypatch.setattr(decorator, "_sweep_lock", lock)
    nested: dict[str, object] = {}

    @lf.trace(name="nested")
    def inner() -> None:
        lf.info("nested")

    def callback(phase: str, info: dict[str, Any]) -> None:
        if phase == "start" and lock.collecting == threading.get_ident() and "flush" not in nested:
            nested["flush"] = lf.flush(0.5)
            nested["continue"] = lf.continue_trace(OTHER_TRACEPARENT)
            inner()

    def run() -> None:
        def arm() -> None:
            lock.arm_for = threading.get_ident()

        enter(worker, arm)

    gc.callbacks.append(callback)
    try:
        runner = threading.Thread(daemon=True, target=run)
        runner.start()
        runner.join(BOUND)
    finally:
        gc.callbacks.remove(callback)
    assert not runner.is_alive(), "a nested call waited on the hold its own thread held"
    assert "flush" in nested, "the collector never ran inside the hold"
    assert nested["flush"].reason == "abandoned", nested  # type: ignore[attr-defined]
    assert nested["continue"].reason == "rejected", nested  # type: ignore[attr-defined]
    lf.flush(BOUND)
    assert sink.messages("nested") == ["span.start", "nested", "span.end"], sink.messages("nested")
    if enter is not _enter_abandon:
        outer = sink.messages("outer")
        assert sorted(outer) == sorted(["span.start", "outer", "span.end"]), (
            f"the outer call's events were not each delivered once: {outer}"
        )
    assert not _once(sink.events)


def test_the_hold_reads_ownership_from_the_rlock_itself() -> None:
    """FR-001: ``_sweep_held_here`` relies on ``RLock._is_owned``, present on this interpreter."""
    assert isinstance(decorator._sweep_lock, type(threading.RLock()))
    assert callable(getattr(decorator._sweep_lock, "_is_owned", None)), (
        "this interpreter's RLock has no _is_owned, which every hold site asks"
    )
    assert decorator._sweep_held_here() is False
    with decorator._sweep_hold:
        assert decorator._sweep_held_here() is True
    assert decorator._sweep_held_here() is False


# ----------------------------------------------------------------------------- FR-002 AC-1


class _Clock:
    """The worker module's ``time``: ``monotonic`` reads the test's clock, the rest is real."""

    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now

    def __getattr__(self, name: str) -> Any:
        return getattr(time, name)


class _Ticks:
    """Steps the drain one pass per tick: a wait returns ``Empty`` on a tick or on a put."""

    def __init__(self) -> None:
        self.count = 0
        self.waiting = threading.Event()
        self.queue: Any = None

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        real_get = worker_mod._CountingQueue.get
        ticks = self

        def get(q: Any, block: bool = True, timeout: float | None = None) -> object:
            if not block or timeout is None:
                return real_get(q, block, timeout)
            ticks.queue = q
            with q.not_empty:
                seen = ticks.count
                deadline = time.monotonic() + BOUND
                while not q._qsize() and ticks.count == seen and timeout > 0:
                    ticks.waiting.set()
                    left = deadline - time.monotonic()
                    if left <= 0:
                        break
                    q.not_empty.wait(left)
            return real_get(q, False)

        monkeypatch.setattr(worker_mod._CountingQueue, "get", get)

    def tick(self) -> None:
        q = self.queue
        with q.not_empty:
            self.count += 1
            q.not_empty.notify_all()


@pytest.mark.parametrize("busy", [False, True], ids=["idle", "busy"])
def test_an_event_in_the_first_span_ships_within_one_interval(busy, monkeypatch) -> None:
    """FR-002 AC-1: one advance of a clock the test controls ships it, idle or busy.

    The event is logged inside the process's first open span, with no child span and no
    ``flush()``. The drain runs one pass per tick. Idle, the bottom-of-loop time trigger fires on
    the advance and resets ``last_flush``; busy, a span closes on every advance with
    ``batch_size`` 1, so the count trigger resets it. A sweep tied to the time trigger therefore
    never runs, and one that does not make the trigger due waits for a second advance.
    """
    clock = _Clock()
    ticks = _Ticks()
    sink = Recorder()
    released = threading.Event()
    parked = threading.Event()
    monkeypatch.setattr(worker_mod, "time", clock)
    ticks.install(monkeypatch)
    try:
        lf.configure(sink=sink)

        @lf.trace(name="first")
        def first() -> None:
            lf.info("first-log")
            parked.set()
            released.wait(BOUND)

        owner = threading.Thread(daemon=True, target=first)
        owner.start()
        assert parked.wait(BOUND)
        worker = _lifecycle._state.worker_exists()
        assert worker is not None and worker._sweep is not None, "the open built a sweeping worker"
        if busy:
            worker.batch_size = 1
        assert ticks.waiting.wait(BOUND), "the drain never reached its wait"
        ticks.waiting.clear()
        clock.now += worker.flush_interval
        if busy:

            @lf.trace(name="closer")
            def closer() -> None:
                lf.info("busy")

            other = threading.Thread(daemon=True, target=closer)
            other.start()
            other.join(BOUND)
        ticks.tick()
        deadline = time.monotonic() + 5.0
        while "first-log" not in sink.messages("first") and time.monotonic() < deadline:
            time.sleep(0.01)
        assert "first-log" in sink.messages("first"), (
            "the first span's event did not reach the sink within one interval of the clock"
        )
    finally:
        released.set()
        monkeypatch.undo()
    owner.join(BOUND)


# ------------------------------------------------------------------------ FR-002 AC-3 to AC-7


class _SettingSink(Recorder):
    """A sink whose stop-signal setter logs once, during the worker's build."""

    def __init__(self) -> None:
        super().__init__()
        self._signal: object = None
        self.during_build: list[object] = []

    @property
    def log_foundry_stop_signal(self) -> object:
        return self._signal

    @log_foundry_stop_signal.setter
    def log_foundry_stop_signal(self, value: object) -> None:
        self._signal = value
        if not self.during_build:
            self.during_build.append(context.current_span())
            lf.info("from-setter")


def test_the_first_traced_call_builds_the_worker_before_its_body() -> None:
    """FR-002 AC-3: the worker exists in the first body, and the setter logs on the orphan path."""
    sink = _SettingSink()
    lf.configure(sink=sink)
    seen: dict[str, object] = {}

    @lf.trace(name="first")
    def first() -> None:
        seen["worker"] = _lifecycle._state._worker
        seen["trace"] = context.current_span().trace_id  # type: ignore[union-attr]

    first()
    lf.flush(BOUND)
    assert seen["worker"] is not None, "the open built the worker before the body ran"
    assert sink.during_build == [None], "the setter ran with no span pushed"
    setter = [e for e in sink.events if e["message"] == "from-setter"]
    assert len(setter) == 1 and setter[0]["trace_id"] != seen["trace"], setter
    assert lf.health().orphan_lost == 0


def test_a_build_that_fails_at_the_open_is_retried_by_the_close(monkeypatch, capfd) -> None:
    """FR-002 AC-4: announced once, the call runs traced, and its close builds the worker."""
    sink = Recorder()
    lf.configure(sink=sink)
    real = _lifecycle._get_worker
    calls: list[int] = []

    def flaky() -> worker_mod.Worker:
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("no thread for you")
        return real()

    monkeypatch.setattr(_lifecycle, "_get_worker", flaky)
    capfd.readouterr()

    @lf.trace(name="call")
    def call() -> None:
        lf.info("inside")

    call()
    lf.flush(BOUND)
    assert len(_err_lines(capfd, "starting the worker for a span")) == 1
    assert sink.messages("call") == ["span.start", "inside", "span.end"]
    assert len(calls) >= 2, "the close retried the build"


def test_a_hand_in_fault_on_the_drain_is_announced_after_the_hold(monkeypatch) -> None:
    """FR-002 AC-5: the line is written with the hold free, and the drain keeps sweeping."""
    sink = Recorder()
    worker = _quiet(sink, flush_interval=0.05)
    free_at_line: list[bool] = []
    real_absorbed = decorator._diag.absorbed

    def absorbed(where: str, exc: BaseException, detail: str = "") -> None:
        if where == "handing in an open span's events":
            probe: list[bool] = []

            def try_it() -> None:
                got = decorator._sweep_lock.acquire(blocking=False)
                probe.append(got)
                if got:
                    decorator._sweep_lock.release()

            t = threading.Thread(daemon=True, target=try_it)
            t.start()
            t.join(BOUND)
            free_at_line.append(probe == [True])
        real_absorbed(where, exc, detail)

    monkeypatch.setattr(decorator._diag, "absorbed", absorbed)
    real_hand_in = worker._hand_in
    faults: list[int] = []

    def hand_in(events: list[dict[str, Any]]) -> tuple[int, int]:
        if not faults:
            faults.append(len(events))
            raise RuntimeError("hand-in fault")
        return real_hand_in(events)

    worker._hand_in = hand_in  # type: ignore[method-assign]
    parked = Parked("taken")
    _wait(lambda: free_at_line != [], "the drain's sweep to fault and announce")
    assert free_at_line == [True], "the fault's line was written while the hold was held"
    assert lf.health().in_span_lost == 1, "the taken event is counted lost"
    assert worker._thread.is_alive(), "the drain thread kept running"
    parked.log("later")
    _wait(lambda: "later" in sink.messages("parked"), "the next sweep to ship a later event")
    parked.finish()


def test_a_registry_that_cannot_be_copied_leaves_events_for_the_close(monkeypatch, capfd) -> None:
    """FR-002 AC-5: the sweep's own fault takes nothing, and the close ships everything."""
    sink = Recorder()
    worker = _quiet(sink)
    parked = Parked("kept")

    class Broken(dict):  # type: ignore[type-arg]
        def copy(self) -> dict[int, Span]:
            raise RuntimeError("copy fault")

    monkeypatch.setattr(decorator, "_open_spans", Broken(decorator._open_spans))
    capfd.readouterr()
    assert worker._run_sweep() == 0
    assert len(_err_lines(capfd, "sweeping open spans")) == 1
    assert parked.span is not None and len(parked.span.events) == 2, "nothing was taken"
    monkeypatch.undo()
    parked.finish()
    lf.flush(BOUND)
    assert sink.messages("parked") == ["span.start", "kept", "span.end"]


def test_an_idle_sweeping_worker_waits_an_interval(monkeypatch) -> None:
    """FR-002 AC-6: with a sweep set, an idle drain makes about one ``get()`` per interval."""
    calls = [0]
    real_get = worker_mod._CountingQueue.get

    def counting(q: Any, *args: Any, **kwargs: Any) -> object:
        calls[0] += 1
        return real_get(q, *args, **kwargs)

    monkeypatch.setattr(worker_mod._CountingQueue, "get", counting)
    interval = 0.05

    def nothing(hand_in: Any, *, wait: float | None = None) -> tuple[int, list[Any], list[Any]]:
        return 0, [], []

    worker = worker_mod.Worker(Recorder(), flush_interval=interval, sweep=nothing)
    install_worker(worker)
    began = time.monotonic()
    time.sleep(0.5)
    elapsed = time.monotonic() - began
    bound = int(elapsed / interval) + 3
    assert calls[0] <= bound, f"an idle sweeping drain spun: {calls[0]} get() calls in {elapsed}s"


def test_the_exit_drain_sweep_is_not_counted_after_shutdown(capfd) -> None:
    """FR-002 AC-7: the exit drain's own hand-in is delivered and never counted as late."""
    sink = Recorder()
    _built(sink)
    parked = Parked("x1", "x2")
    capfd.readouterr()
    lf.shutdown(BOUND)
    assert sorted(sink.messages("parked")) == ["x1", "x2"], "the exit drain swept and delivered"
    assert lf.health().submitted_after_shutdown == 0
    assert not _err_lines(capfd, "logged after shutdown()")
    parked.finish()


def test_the_drain_does_not_wait_on_a_thread_whose_hold_waits_on_the_drain(monkeypatch) -> None:
    """FR-002 (added 2026-11-17): the drain's wait for the hold is bounded.

    A thread holding the hold calls ``flush(5.0)`` once the drain has tried its sweep. Its own
    sweep takes nothing, so it reports ``"abandoned"``; the drain skips the sweep and answers its
    marker, so it returns at once. With the drain's wait unbounded it returned only at its 5 s
    deadline, since the drain waited on the holder that waited on the drain.
    """
    sink = Recorder()
    worker = _built(sink)
    lock = _WatchedLock(decorator._sweep_lock)
    monkeypatch.setattr(decorator, "_sweep_lock", lock)
    drain = worker._thread.ident
    outcome: dict[str, object] = {}

    def holder() -> None:
        with decorator._sweep_hold:
            start = len(lock.attempts)
            worker.flush_interval = 0.05
            deadline = time.monotonic() + BOUND
            while not any(ident == drain for ident, _ in lock.attempts[start:]):
                if time.monotonic() > deadline:
                    outcome["tried"] = False
                    return
                time.sleep(0.005)
            began = time.monotonic()
            outcome["result"] = lf.flush(5.0)
            outcome["elapsed"] = time.monotonic() - began

    thread = threading.Thread(daemon=True, target=holder)
    thread.start()
    thread.join(BOUND + 6)
    assert outcome.get("tried", True), "the drain never tried to take the hold"
    assert outcome["result"].reason == "abandoned", outcome  # type: ignore[attr-defined]
    assert outcome["elapsed"] < 1.0, f"the holder's flush waited on the drain: {outcome}"


# ----------------------------------------------------------------------------- FR-003


def test_a_flush_outside_every_span_ships_another_contexts_logs() -> None:
    """FR-003 AC-1: a flush from no span delivers another context's 5 logs, span.start held."""
    sink = Recorder()
    _quiet(sink)
    parked = Parked(*[f"log{i}" for i in range(5)])
    assert context.current_span() is None
    lf.flush(BOUND)
    assert sink.messages("parked") == [f"log{i}" for i in range(5)], sink.messages("parked")
    parked.finish()
    lf.flush(BOUND)
    assert sink.messages("parked")[-2:] == ["span.start", "span.end"]


def test_a_span_open_across_shutdown_is_counted_after_it() -> None:
    """FR-003 AC-3: its close after ``shutdown()`` adds nothing and is counted late."""
    sink = Recorder()
    _built(sink)
    parked = Parked("before")
    lf.shutdown(BOUND)
    delivered = len(sink.events)
    late = lf.health().submitted_after_shutdown
    parked.finish()
    assert len(sink.events) == delivered, "a span closing after shutdown reached the sink"
    assert lf.health().submitted_after_shutdown == late + 1


class _ReportingSink(Recorder):
    """In a forked child, reports each event it is handed down the child's pipe."""

    def __init__(self) -> None:
        super().__init__()
        self.parent = os.getpid()

    def emit(self, batch: list[dict[str, Any]]) -> None:
        if os.getpid() != self.parent:
            for event in batch:
                report_from_child(f"{event['message']}\n")
            return
        super().emit(batch)


def test_a_child_forked_while_a_sweep_is_due_writes_nothing_inherited() -> None:
    """FR-003 AC-4: the child forgets the parent's open spans before its drain resumes."""
    sink = _ReportingSink()
    worker = _quiet(sink)
    parked = Parked("parent-log")

    def work() -> str:
        live = _lifecycle._state.worker_exists()
        assert live is not None
        live.flush_interval = 0.05
        lf.flush(2.0)
        time.sleep(0.3)
        return ""

    child = run_in_child(work)
    assert "parent-log" not in child.output, f"the child shipped a parent's event: {child.output!r}"
    assert worker._thread.is_alive()
    parked.finish()


def _forget_order_problems(names: list[str]) -> list[str]:
    """Why a child-handler order would let the child sweep a parent's span, or nothing."""
    step = "_forget_open_spans_after_fork"
    if step not in names:
        return [f"{step} is not registered"]
    at = names.index(step)
    problems = []
    if "_reclaim_after_fork" in names and at < names.index("_reclaim_after_fork"):
        problems.append(f"{step} runs before _reclaim_after_fork, which must run first")
    if "_rebuild_worker_after_fork" in names and at > names.index("_rebuild_worker_after_fork"):
        problems.append(
            f"{step} runs after _rebuild_worker_after_fork, when the child's drain already sweeps"
        )
    return problems


_REAL_ORDER = [
    "_reclaim_after_fork",
    "_clear_after_fork",
    "_adopt_factory_after_fork",
    "_restart_ledger_after_fork",
    "_forget_open_spans_after_fork",
    "_rebuild_worker_after_fork",
]


@pytest.mark.parametrize(
    ("order", "reason"),
    [
        (_REAL_ORDER, None),
        (
            [n for n in _REAL_ORDER if n != "_forget_open_spans_after_fork"]
            + ["_forget_open_spans_after_fork"],
            "runs after _rebuild_worker_after_fork",
        ),
        (
            ["_forget_open_spans_after_fork"]
            + [n for n in _REAL_ORDER if n != "_forget_open_spans_after_fork"],
            "runs before _reclaim_after_fork",
        ),
    ],
    ids=["real", "registered-from-decorator", "registered-first"],
)
def test_the_order_check_names_each_wrong_place(order, reason) -> None:
    """FR-003 AC-5's corpus: the check is silent on the real order and names each wrong one."""
    problems = _forget_order_problems(order)
    if reason is None:
        assert problems == []
    else:
        assert len(problems) == 1 and reason in problems[0], problems


def test_the_forgetting_step_sits_between_reclaim_and_rebuild() -> None:
    """FR-003 AC-5: read from the registered handlers themselves."""
    names = [handler.__name__ for handler in _fork._child_handlers]
    assert _forget_order_problems(names) == [], names


def test_a_child_leaving_by_os_exit_delivers_nothing_of_the_inherited_span() -> None:
    """FR-003 AC-6: a child of a traced call logs, waits out several sweeps and leaves."""
    sink = _ReportingSink()
    _quiet(sink)
    outputs: list[str] = []

    @lf.trace(name="forking")
    def forking() -> None:
        lf.info("before-fork")

        def work() -> str:
            lf.info("child-log")
            live = _lifecycle._state.worker_exists()
            assert live is not None
            live.flush_interval = 0.05
            time.sleep(0.3)
            return ""

        outputs.append(run_in_child(work).output)

    forking()
    assert outputs == [""], f"the child delivered part of the inherited span: {outputs!r}"
    lf.flush(BOUND)
    assert sink.messages("forking") == ["span.start", "before-fork", "span.end"]


def test_a_sink_swap_during_sweeps_loses_and_duplicates_nothing() -> None:
    """FR-003 AC-7: across ``configure(sink=...)``, the sinks together get each event once."""
    sinks = [Recorder()]
    worker = _built(sinks[0])
    worker.flush_interval = 0.001
    done = threading.Event()

    def swapper() -> None:
        while not done.is_set():
            fresh = Recorder()
            sinks.append(fresh)
            lf.configure(sink=fresh)
            time.sleep(0.005)

    @lf.trace(name="work")
    def work_call() -> None:
        for i in range(3):
            lf.info(f"e{i}")
            time.sleep(0.001)

    swapping = threading.Thread(daemon=True, target=swapper)
    swapping.start()
    try:
        errors = run_concurrently(lambda t, i: work_call(), threads=4, per_thread=25)
    finally:
        done.set()
        swapping.join(BOUND)
    assert not errors, errors
    lf.flush(BOUND)
    events = [e for s in sinks for e in s.events if e.get("function") == "work"]
    assert not _once(events), "an event reached the sinks twice"
    assert len(events) == 4 * 25 * 5, f"events were lost across the swaps: {len(events)}"
    assert len(sinks) > 2, "the swapper never swapped"


# ----------------------------------------------------------------------------- FR-004


def test_the_ledger_balances_while_sweeps_run() -> None:
    """FR-004 AC-1: every snapshot balances while eight threads trace and a ninth flushes."""
    worker = _built(Recorder())
    worker.flush_interval = 0.001
    done = threading.Event()
    wrong: list[object] = []

    def flusher() -> None:
        while not done.is_set():
            lf.flush(1.0)

    def sampler() -> None:
        while not done.is_set():
            events = lf.health().events
            if events.unaccounted:
                wrong.append(events)

    @lf.trace(name="work")
    def work_call() -> None:
        for i in range(3):
            lf.info(f"e{i}")

    helpers = [
        threading.Thread(daemon=True, target=flusher),
        threading.Thread(daemon=True, target=sampler),
    ]
    for helper in helpers:
        helper.start()
    try:
        errors = run_concurrently(lambda t, i: work_call(), threads=8, per_thread=30)
    finally:
        done.set()
        for helper in helpers:
            helper.join(BOUND)
    assert not errors, errors
    assert not wrong, f"a snapshot taken while sweeps ran did not balance: {wrong[:2]}"
    lf.flush(BOUND)
    assert lf.health().events.pending == 0


def test_a_snapshot_held_inside_the_span_pass_reads_a_sweep_balanced() -> None:
    """FR-004 AC-2: hand-overs are read before span credits, so a sweep mid-read balances.

    The reading is held in its span pass just after the owner's cell, where it has read the
    owner's one credit, ``span.start``. The owner then logs 3 events and a sweep takes them and
    credits their hand-over in its own, later cell. With the passes swapped it read 1 - 3 as
    ``open_spans``, and ``unaccounted == 2``.
    """
    _quiet(Recorder())
    owner_go = threading.Event()
    owner_logged = threading.Event()
    owner_release = threading.Event()
    owner_ready = threading.Event()
    owner_cell: list[object] = []

    @lf.trace(name="owner")
    def owner() -> None:
        owner_cell.append(_ledger.cell())
        owner_ready.set()
        assert owner_go.wait(BOUND)
        for i in range(3):
            lf.info(f"held{i}")
        owner_logged.set()
        assert owner_release.wait(BOUND)

    owning = threading.Thread(daemon=True, target=owner)
    owning.start()
    assert owner_ready.wait(BOUND)
    later_done = threading.Event()
    later_ready = threading.Event()

    def later_cell() -> None:
        _ledger.cell()
        later_ready.set()
        later_done.wait(BOUND)

    later = threading.Thread(daemon=True, target=later_cell)
    later.start()
    assert later_ready.wait(BOUND)
    _, cells = _ledger.snapshot()
    index = next(i for i, c in enumerate(cells) if c is owner_cell[0])
    assert len(cells) > index + 1, "a cell after the owner's exists to hold the reading at"
    reading: list[object] = []

    def between(name: str, at: int) -> None:
        if name == "span" and at == index + 1 and not owner_go.is_set():
            owner_go.set()
            assert owner_logged.wait(BOUND)
            worker = _lifecycle._state.worker_exists()
            assert worker is not None
            decorator._sweep_registered(worker._hand_in)

    _ledger._between = between
    try:
        reader = threading.Thread(daemon=True, target=lambda: reading.append(lf.health().events))
        reader.start()
        reader.join(BOUND)
    finally:
        _ledger._between = None
        owner_release.set()
        later_done.set()
        owning.join(BOUND)
        later.join(BOUND)
    assert owner_go.is_set(), "the reading never reached the owner's cell"
    events = reading[0]
    assert events.unaccounted == 0 and events.open_spans >= 0, events  # type: ignore[attr-defined]


def _full_queue(gate: GatedSink) -> worker_mod.Worker:
    """A one-slot queue that is full behind a drain parked inside a gated emit."""
    worker = _quiet(gate, max_queue=1, batch_size=1)
    gate.gate.clear()

    @lf.trace(name="filler")
    def filler() -> None:
        lf.info("fill")

    filler()
    assert gate.entered.wait(BOUND), "the drain never entered the gated emit"
    filler()
    _wait(lambda: worker._queue.full(), "the queue to fill")
    return worker


def test_a_sweep_into_a_full_queue_leaves_every_take_on_its_span() -> None:
    """FR-004 AC-3 (revised 2026-11-17): a full queue drops nothing; the takes stay on their spans.

    ~~Two takes overflow: their events in overflow_lost, dropped by 2~~ — 12,000 open spans
    overflowed 6,000 submissions in one pass (``r_many_spans.py``). The first offer finds the
    queue full, its take goes back on its span at ``held``, and the pass stops: nothing is
    dropped, ``open_spans`` does not move, and every event is delivered at its span's close.
    """
    gate = GatedSink()
    worker = _full_queue(gate)
    first, second = Parked("a1", "a2"), Parked("b1", "b2", "b3")
    before, dropped = lf.health().events, worker.dropped
    try:
        result = decorator._sweep_registered(worker._hand_in)
        after = lf.health().events
        assert result is not None and result[0] == 0, result
        assert worker.dropped == dropped and after.overflow_lost == before.overflow_lost, (
            "a sweep dropped a take on a full queue"
        )
        assert after.open_spans == before.open_spans
        assert first.span is not None and second.span is not None
        assert [e["message"] for e in first.span.events] == ["span.start", "a1", "a2"]
        assert [e["message"] for e in second.span.events] == ["span.start", "b1", "b2", "b3"]
    finally:
        gate.gate.set()
    for parked in (first, second):
        _wait(lambda: worker._queue.qsize() == 0, "room for the close's submission")
        parked.finish()
    lf.flush(BOUND)
    got = sorted(str(e["message"]) for e in gate.events if e.get("function") == "parked")
    assert got == sorted(
        ["span.start", "a1", "a2", "span.end"] * 1 + ["span.start", "b1", "b2", "b3", "span.end"]
    )


def test_a_sweep_stops_at_capacity_and_the_rest_ship_at_their_close() -> None:
    """Diff review frame 2, MAJOR 1, scaled down: more open spans than the queue has room for.

    With five free slots and twelve open spans, the sweep hands in five takes, finds the queue
    full at the sixth, puts that take back and stops. Nothing is dropped, and every one of the
    twelve spans' events reaches the sink exactly once, the seven left behind at their close.
    """
    gate = GatedSink()
    worker = _quiet(gate, max_queue=5, batch_size=1)
    gate.gate.clear()

    @lf.trace(name="filler")
    def filler() -> None:
        lf.info("fill")

    filler()
    assert gate.entered.wait(BOUND), "the drain never entered the gated emit"
    spans = [Parked(f"s{i}-a", f"s{i}-b") for i in range(12)]
    try:
        result = decorator._sweep_registered(worker._hand_in)
        assert result is not None and result[0] == 10, f"five takes of two: {result}"
        assert worker.dropped == 0 and lf.health().events.overflow_lost == 0, (
            "the sweep dropped takes past the queue's capacity"
        )
        left = [p for p in spans if p.span is not None and len(p.span.events) == 3]
        assert len(left) == 7, "the seven spans past capacity still hold their logs"
    finally:
        gate.gate.set()
    for parked in spans:
        _wait(lambda: worker._queue.qsize() == 0, "room for the close's submission")
        parked.finish()
    lf.flush(BOUND)
    got = [e for e in gate.events if e.get("function") == "parked"]
    assert len(got) == 12 * 4 and not _once(got), f"{len(got)} of 48 parked events delivered"


def test_a_hand_in_fault_moves_only_in_span_lost() -> None:
    """FR-004 AC-4: the faulted take leaves open_spans for in_span_lost, and nothing else moves."""
    _quiet(Recorder())
    parked = Parked("a", "b")
    before = lf.health().events

    def raising(events: list[dict[str, Any]]) -> tuple[int, int]:
        raise RuntimeError("hand-in fault")

    result = decorator._sweep_registered(raising)
    after = lf.health().events
    assert result is not None and len(result[2]) == 1
    moved = {
        name: getattr(after, name) - getattr(before, name)
        for name in vars(before)
        if getattr(after, name) != getattr(before, name)
    }
    assert moved == {"in_span_lost": 2, "open_spans": -2}, moved
    parked.finish()


def test_a_flush_whose_registry_sweep_meets_a_full_queue_leaves_the_events() -> None:
    """FR-004 AC-5 (revised 2026-11-17): the registry sweep stops; nothing is lost or counted.

    ~~The overflow the registry sweep causes is in the call's counts~~. Another context's events
    stay on its span, the call counts no overflow and is not ``"abandoned"``, since nothing was
    lost, and the events are delivered at the span's close.
    """
    gate = GatedSink()
    worker = _full_queue(gate)
    parked = Parked("a", "b", "c")
    try:
        result = lf.flush(0.3)
        assert result.overflow_lost == 0 and result.reason != "abandoned", result
        assert parked.span is not None and len(parked.span.events) == 4, "still on the span"
    finally:
        gate.gate.set()
    _wait(lambda: worker._queue.qsize() == 0, "room for the close's submission")
    parked.finish()
    lf.flush(BOUND)
    got = sorted(str(e["message"]) for e in gate.events if e.get("function") == "parked")
    assert got == sorted(["span.start", "a", "b", "c", "span.end"]), got


def test_a_close_with_nothing_buffered_submits_nothing(monkeypatch) -> None:
    """FR-004 AC-8: an empty detach is not submitted, so it is never counted as queued."""
    worker = _quiet(Recorder())
    puts: list[object] = []
    real_put = worker._queue.put_nowait
    monkeypatch.setattr(
        worker._queue, "put_nowait", lambda item: (puts.append(item), real_put(item))
    )
    span = Span(trace_id="1" * 32, span_id="2" * 16, parent_span_id=None, name="n", start_ts=0.0)
    decorator._flush(span)
    assert puts == [], f"an empty batch was submitted: {puts}"


# ----------------------------------------------------------------------------- FR-005


def test_the_open_and_a_level_call_take_no_sweep_lock() -> None:
    """FR-005 AC-1: with another thread holding the hold, a body starts and logs; the close waits."""
    _quiet(Recorder())
    holding = threading.Event()
    release = threading.Event()
    started = threading.Event()
    logged = threading.Event()

    def holder() -> None:
        with decorator._sweep_hold:
            holding.set()
            release.wait(BOUND)

    @lf.trace(name="call")
    def call() -> None:
        started.set()
        lf.info("x")
        logged.set()

    hold = threading.Thread(daemon=True, target=holder)
    hold.start()
    assert holding.wait(BOUND)
    caller = threading.Thread(daemon=True, target=call)
    caller.start()
    try:
        assert started.wait(BOUND), "the open waited on the sweep hold"
        assert logged.wait(BOUND), "a level call waited on the sweep hold"
        caller.join(0.2)
        assert caller.is_alive(), "the close, which takes the hold, did not wait for it"
    finally:
        release.set()
        hold.join(BOUND)
        caller.join(BOUND)


class _BlockingStream:
    """A ``sys.stderr`` whose ``write`` blocks until released."""

    def __init__(self) -> None:
        self.writing = threading.Event()
        self.release = threading.Event()
        self.lines: list[str] = []

    def write(self, text: str) -> int:
        self.writing.set()
        self.release.wait(BOUND)
        self.lines.append(text)
        return len(text)

    def flush(self) -> None:
        return None


def test_a_wedged_console_does_not_hold_a_close() -> None:
    """FR-005 AC-3 (revised 2026-11-17): a sweep's line blocked on stderr holds no close.

    A sweep never overflows now, so the line it writes is the late-submission line: a ``flush()``
    after ``shutdown()`` sweeps another context's span into the retired worker, and counts and
    announces that submission once the hold is released. With that line blocked on stderr, a
    traced call on another thread closes; written under the hold, it did not.
    """
    _built(Recorder())
    lf.shutdown(BOUND)
    parked = Parked("x")
    stream = _BlockingStream()
    original = sys.stderr
    sys.stderr = stream  # type: ignore[assignment]
    closed = threading.Event()
    try:
        flushing = threading.Thread(daemon=True, target=lambda: lf.flush(1.0))
        flushing.start()
        assert stream.writing.wait(BOUND), "the sweep's late-submission line was never written"

        @lf.trace(name="closer")
        def closer() -> None:
            lf.info("y")

        def close() -> None:
            closer()
            closed.set()

        closing = threading.Thread(daemon=True, target=close)
        closing.start()
        assert closed.wait(5.0), "a close waited on a sweep whose line was blocked on stderr"
        assert not stream.release.is_set() and flushing.is_alive(), "the line was still pending"
    finally:
        stream.release.set()
        sys.stderr = original
    flushing.join(BOUND)
    closing.join(BOUND)
    parked.finish()


def test_the_open_calls_no_lifecycle_function_once_a_worker_exists(monkeypatch) -> None:
    """FR-005 AC-4: 1,000 traced calls call ``_get_worker`` 1,000 times, once per close."""
    _quiet(Recorder())
    real = _lifecycle._get_worker
    calls = [0]

    def counting() -> worker_mod.Worker:
        calls[0] += 1
        return real()

    monkeypatch.setattr(_lifecycle, "_get_worker", counting)

    @lf.trace(name="call")
    def call() -> None:
        return None

    for _ in range(1000):
        call()
    assert calls[0] == 1000, f"the open called _get_worker: {calls[0]} calls for 1,000 spans"


def test_an_abandoned_span_leaves_the_registry() -> None:
    """FR-001: a guard's abandon removes the span, so no later sweep copies it again."""
    _quiet(Recorder())
    span, token, scope = decorator._begin("abandoned", None)
    assert span is not None and decorator._open_spans.get(id(span)) is span
    decorator._note_in_span_loss(decorator._abandon(span))
    if token is not None:
        context.pop_span(token)
    if scope is not None:
        context.pop_baggage_scope(scope)
    assert id(span) not in decorator._open_spans, "an abandoned span stayed registered"


def test_an_abandon_inside_a_hold_is_debited_by_the_holder() -> None:
    """FR-001: on the holding thread the abandon defers its take, and the holder debits it."""
    _quiet(Recorder())
    span, token, scope = decorator._begin("abandoned", None)
    assert span is not None
    lf.info("lost")
    before = lf.health().in_span_lost
    with decorator._sweep_hold:
        assert decorator._abandon(span) == 0, "the take was deferred, not made under the hold"
        assert span.closed and span.abandoned
        assert lf.health().in_span_lost == before
    assert lf.health().in_span_lost - before == 2, "the holder debited span.start and the log"
    assert span.events == []
    if token is not None:
        context.pop_span(token)
    if scope is not None:
        context.pop_baggage_scope(scope)
    assert lf.health().events.unaccounted == 0


def test_a_close_inside_a_hold_ships_when_the_holder_releases() -> None:
    """FR-001: on the holding thread a close defers its swap and submit to the hold's release."""
    sink = Recorder()
    _quiet(sink)

    @lf.trace(name="nested")
    def nested() -> None:
        lf.info("inside")

    with decorator._sweep_hold:
        nested()
        assert [s for s, _ in decorator._deferred_closes], "the close was deferred"
    assert decorator._deferred_closes == [], "the release drained it"
    lf.flush(BOUND)
    assert sink.messages("nested") == ["span.start", "inside", "span.end"]


class _ReleaseWatch:
    """``decorator._sweep_lock``, recording how many closes are still deferred at each release."""

    def __init__(self, real: Any) -> None:
        self.real = real
        self.deferred_at_release: list[int] = []

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        return bool(self.real.acquire(blocking, timeout))

    def release(self) -> None:
        self.deferred_at_release.append(len(decorator._deferred_closes))
        self.real.release()

    def _is_owned(self) -> bool:
        return bool(self.real._is_owned())


def test_the_deferred_closes_drain_before_the_hold_is_released(monkeypatch) -> None:
    """FR-001: the holder ships a deferred close while it still holds the lock.

    Drained after the release, another thread's sweep could take the lock between the release
    and the drain and act on the deferred span's list before the deferred swap, which the
    ordering argument for ``span.end`` rests on not happening.
    """
    sink = Recorder()
    _quiet(sink)
    watch = _ReleaseWatch(decorator._sweep_lock)
    monkeypatch.setattr(decorator, "_sweep_lock", watch)

    @lf.trace(name="nested")
    def nested() -> None:
        lf.info("inside")

    with decorator._sweep_hold:
        nested()
    assert watch.deferred_at_release == [0], (
        f"the lock was released with closes still deferred: {watch.deferred_at_release}"
    )
    monkeypatch.undo()
    lf.flush(BOUND)
    assert sink.messages("nested") == ["span.start", "inside", "span.end"]


def test_a_flush_whose_registry_hand_in_fails_reports_abandoned(monkeypatch) -> None:
    """FR-003: a hand-in fault in ``flush()``'s registry sweep makes the call ``"abandoned"``."""
    sink = Recorder()
    worker = _quiet(sink)
    parked = Parked("boom")
    real_offer = worker._offer

    def offer(events: list[dict[str, Any]], *, count_retired: bool) -> int | None:
        if any(e.get("message") == "boom" for e in events):
            raise RuntimeError("hand-in fault")
        return real_offer(events, count_retired=count_retired)

    monkeypatch.setattr(worker, "_offer", offer)
    result = lf.flush(BOUND)
    assert not result and result.reason == "abandoned", (
        f"a flush whose registry sweep lost a take reported {result}"
    )
    assert lf.health().in_span_lost == 1
    monkeypatch.undo()
    parked.finish()
    lf.flush(BOUND)


def test_the_sweep_keeps_its_own_clock_through_a_burst_of_passes() -> None:
    """FR-002: a drain taking many submissions within an interval sweeps about once per interval.

    Each submission is one pass of the drain loop, so a sweep made on every pass would run once
    per submission; the sweep's own clock lets it run only once an interval has passed.
    """
    sweeps = [0]
    interval = 0.3

    def counting(hand_in: Any, *, wait: float | None = None) -> tuple[int, list[Any], list[Any]]:
        sweeps[0] += 1
        return 0, [], []

    worker = worker_mod.Worker(Recorder(), flush_interval=interval, sweep=counting)
    install_worker(worker)
    began = time.monotonic()
    submissions = 200
    for i in range(submissions):
        worker.submit([{"message": f"m{i}", "log_id": str(i)}])
    _wait(lambda: worker._queue.qsize() == 0, "the drain to take every submission")
    elapsed = time.monotonic() - began
    bound = int(elapsed / interval) + 2
    assert sweeps[0] <= bound, (
        f"the drain swept {sweeps[0]} times over {submissions} passes in {elapsed:.2f}s, "
        f"more than once an interval"
    )


def test_a_sweep_gives_up_on_a_queue_mutex_another_thread_holds(monkeypatch) -> None:
    """Diff review frame 2, MAJOR 3: the drain never waits unboundedly on the queue's mutex.

    Thread A holds the queue's mutex, standing in for a collector callback that runs inside
    another thread's ``put_nowait``, and takes the sweep hold there, as a close's swap does. The
    drain holds that hold while it offers a take; an offer that waited on the mutex unboundedly
    waited on A, and every close in the process hung. The offer gives up within its bound, the
    take goes back on its span, A gets the hold, and every event, a traced call's made after A
    lets go included, is delivered once with ``span.end`` last.

    A traced close made inside the mutex itself is a different hang, and not this spec's: its own
    submit waits on the mutex its thread already holds, since ``queue.Queue``'s mutex is a plain
    ``Lock``. It hangs on the base ``33a44e99`` too (``architecture.md`` §13).

    A takes the hold with a bounded wait of its own, so under an unbounded drain wait it gives up,
    lets go of the mutex, and the test fails on its assertion rather than hanging in teardown.
    """
    sink = Recorder()
    worker = _quiet(sink, flush_interval=0.05)
    in_hand_in = threading.Event()
    a_has_mutex = threading.Event()
    real_hand_in = worker._hand_in
    spied: list[bool] = []

    def hand_in(events: list[dict[str, Any]]) -> int | None:
        if not spied:
            spied.append(True)
            in_hand_in.set()
            assert a_has_mutex.wait(BOUND)
        return real_hand_in(events)

    monkeypatch.setattr(worker, "_hand_in", hand_in)
    parked = Parked("buffered")
    closed = threading.Event()

    @lf.trace(name="nested")
    def nested() -> None:
        lf.info("nested-close")

    def thread_a() -> None:
        assert in_hand_in.wait(BOUND)
        with worker._queue.mutex:
            a_has_mutex.set()
            try:
                with decorator._sweep_hold.within(3.0):
                    closed.set()
            except decorator._NotHeldError:
                pass

    a = threading.Thread(daemon=True, target=thread_a)
    a.start()
    try:
        assert closed.wait(5.0), "A waited on the hold while the drain waited on A's queue mutex"
    finally:
        a.join(BOUND)
    assert worker._thread.is_alive()
    monkeypatch.undo()
    nested()
    parked.finish()
    lf.flush(BOUND)
    for name, logs in (("parked", ["buffered"]), ("nested", ["nested-close"])):
        got = sink.messages(name)
        assert sorted(got) == sorted(["span.start", *logs, "span.end"]), (name, got)
        assert got[-1] == "span.end", (name, got)
    assert not _once(sink.events)


def test_a_skipped_sweep_does_not_slow_the_drain_behind_a_backlog() -> None:
    """Diff review frame 2, MAJOR 2: a sweep skipped for a held hold is retried an interval later.

    A thread holds the hold and calls ``flush()`` with 100 submissions queued ahead of its marker.
    Retrying the sweep on every pass made each pass wait out the sweep's bound, about twenty
    submissions a second, so the call took seconds; retried once an interval, it returns at once.
    """
    gate = GatedSink()
    worker = _built(gate)
    gate.gate.clear()
    worker.flush_interval = 0.05

    @lf.trace(name="one")
    def one() -> None:
        lf.info("x")

    one()
    assert gate.entered.wait(BOUND), "the drain never entered the gated emit"
    for _ in range(100):
        one()
    outcome: dict[str, object] = {}

    def holder() -> None:
        with decorator._sweep_hold:
            gate.gate.set()
            began = time.monotonic()
            outcome["result"] = lf.flush(10.0)
            outcome["elapsed"] = time.monotonic() - began

    thread = threading.Thread(daemon=True, target=holder)
    thread.start()
    thread.join(BOUND + 5)
    assert "elapsed" in outcome, "the in-hold flush never returned"
    assert outcome["elapsed"] < 1.0, f"the drain was slowed behind the held hold: {outcome}"


def test_a_flush_with_no_worker_builds_one_for_another_contexts_span(monkeypatch) -> None:
    """Diff review frame 2, MINOR 4: a failed open-time build leaves the flush to build it.

    The open-time build fails, so no worker exists while another context's span holds 5 logs.
    The caller's own sweep finds nothing to build one for; the registry sweep then builds it,
    as the own sweep does, and delivers the 5 logs, where it reported success over them.
    """
    sink = Recorder()
    lf.configure(sink=sink)
    real = _lifecycle._get_worker
    failures = [1]

    def flaky() -> worker_mod.Worker:
        if failures:
            failures.pop()
            raise RuntimeError("no thread for you")
        return real()

    monkeypatch.setattr(_lifecycle, "_get_worker", flaky)
    parked = Parked(*[f"log{i}" for i in range(5)])
    assert _lifecycle._state.worker_exists() is None, "the open-time build failed"
    result = lf.flush(BOUND)
    assert result, result
    assert sink.messages("parked") == [f"log{i}" for i in range(5)], sink.messages("parked")
    parked.finish()
    lf.flush(BOUND)


def test_an_own_sweep_that_cannot_take_the_queue_mutex_puts_the_buffer_back() -> None:
    """Diff review frame 2, MAJOR 3: the caller's own sweep is bounded on the mutex too.

    With another thread holding the queue's mutex, the offer gives up within its bound: the buffer
    goes back at the front of the span's list, before a log made afterwards, the sweep reports
    that it did not deliver, and the close ships everything in order.
    """
    sink = Recorder()
    worker = _quiet(sink)
    holding = threading.Event()
    release = threading.Event()

    def hold_mutex() -> None:
        with worker._queue.mutex:
            holding.set()
            release.wait(BOUND)

    holder = threading.Thread(daemon=True, target=hold_mutex)
    seen: dict[str, object] = {}

    @lf.trace(name="own")
    def own() -> None:
        lf.info("before")
        holder.start()
        assert holding.wait(BOUND)
        try:
            seen["swept"] = decorator._sweep_open_spans()
        finally:
            release.set()
            holder.join(BOUND)
        lf.info("after")
        span = context.current_span()
        assert span is not None
        seen["left"] = [e["message"] for e in span.events]

    own()
    lf.flush(BOUND)
    assert seen["swept"] is False, "a sweep that put nothing reported delivering"
    assert seen["left"] == ["span.start", "before", "after"], f"the buffer was lost: {seen}"
    assert sink.messages("own") == ["span.start", "before", "after", "span.end"]


def test_a_deferred_close_is_submitted_with_no_hold_held(monkeypatch) -> None:
    """Diff review frame 2, MAJOR 3: no lock is waited on inside the hold, a deferred close's either.

    The deferred close's buffer is swapped inside the hold, and submitted, which takes the queue's
    mutex, only once the hold is released.
    """
    sink = Recorder()
    worker = _quiet(sink)
    held_at_put: list[bool] = []
    real_put = worker._queue.put_nowait

    def put_nowait(item: object) -> None:
        held_at_put.append(decorator._sweep_held_here())
        real_put(item)

    monkeypatch.setattr(worker._queue, "put_nowait", put_nowait)

    @lf.trace(name="nested")
    def nested() -> None:
        lf.info("inside")

    with decorator._sweep_hold:
        nested()
    assert held_at_put == [False], (
        f"the deferred close was submitted inside the hold: {held_at_put}"
    )
    monkeypatch.undo()
    lf.flush(BOUND)
    assert sink.messages("nested") == ["span.start", "inside", "span.end"]


def test_the_exit_sweep_ships_an_open_span_past_a_full_queue() -> None:
    """Execution review: a full queue at the stop leaves no open span's events behind.

    The queue is full of submissions behind a drain parked in a gated emit when ``shutdown()``
    begins, and another context's span holds 5 logs and never closes, as a daemon thread's does.
    The exit drain's one sweep puts past ``max_queue``, since this drain is the queue's only
    consumer and reads its bound right after, so the 5 logs ship in the exit batches; refused on
    the full queue they stayed on a span nothing would close, delivered 0 of 5 and counted nowhere.
    """
    gate = GatedSink()
    worker = _quiet(gate, max_queue=4, batch_size=1)
    gate.gate.clear()

    @lf.trace(name="filler")
    def filler() -> None:
        lf.info("fill")

    filler()
    assert gate.entered.wait(BOUND), "the drain never entered the gated emit"
    for _ in range(4):
        filler()
    assert worker._queue.full()
    parked = Parked(*[f"d{i}" for i in range(5)])
    opener = threading.Timer(0.3, gate.gate.set)
    opener.start()
    try:
        lf.shutdown(BOUND)
    finally:
        opener.cancel()
        gate.gate.set()
    got = [e["message"] for e in gate.events if e.get("function") == "parked"]
    assert got == [f"d{i}" for i in range(5)], f"the exit drain left the open span's logs: {got}"
    parked.finish()


def test_a_refused_take_goes_back_before_what_the_owner_appended() -> None:
    """Execution review: a take the queue refuses is restored at ``held``, not at the end.

    While the drain's take is out, the owner logs ``late`` and closes, appending ``span.end``; the
    offer is then refused. Restored at ``held`` the take goes back ahead of both, and the close
    ships ``span.start, a, b, late, span.end``. Appended instead, ``a`` and ``b`` shipped after
    ``span.end``.
    """
    sink = Recorder()
    worker = _quiet(sink, flush_interval=0.05)
    holder: list[Span] = []
    ready = threading.Event()
    go_late = threading.Event()
    refused: list[bool] = []
    real = worker._hand_in

    def refusing(events: list[dict[str, Any]]) -> int | None:
        if refused:
            return real(events)
        refused.append(True)
        go_late.set()
        deadline = time.monotonic() + BOUND
        while not holder[0].closed and time.monotonic() < deadline:
            time.sleep(0.001)
        return None

    @lf.trace(name="owner")
    def owner() -> None:
        holder.append(context.current_span())  # type: ignore[arg-type]
        lf.info("a")
        lf.info("b")
        ready.set()
        assert go_late.wait(BOUND)
        lf.info("late")

    worker._hand_in = refusing  # type: ignore[method-assign]
    try:
        running = threading.Thread(daemon=True, target=owner)
        running.start()
        running.join(BOUND)
        assert refused, "the drain never swept the owner's span"
    finally:
        del worker._hand_in
    lf.flush(BOUND)
    got = sink.messages("owner")
    assert got == ["span.start", "a", "b", "late", "span.end"], (
        f"the take went back out of order: {got}"
    )


def test_an_own_sweep_puts_its_buffer_back_before_a_sharing_threads_append(monkeypatch) -> None:
    """Execution review: the caller's own sweep restores its buffer at the front of the span's list.

    The span's context is shared with another thread, as ``asyncio.to_thread`` shares it. While the
    own sweep's buffer is detached and its offer waits on a queue mutex another thread holds, the
    sharing thread logs ``late`` into the span's new list; the offer is refused, and the buffer
    goes back in front of it. Appended instead, ``late`` shipped before ``span.start``.
    """
    sink = Recorder()
    worker = _quiet(sink)
    holding = threading.Event()
    release = threading.Event()
    real_offer = worker._offer
    shared: list[contextvars.Context] = []

    def offer(events: list[dict[str, Any]], **kwargs: Any) -> int | None:
        sharer = threading.Thread(daemon=True, target=lambda: shared[0].run(lf.info, "late"))
        sharer.start()
        sharer.join(BOUND)
        return real_offer(events, **kwargs)

    def hold_mutex() -> None:
        with worker._queue.mutex:
            holding.set()
            release.wait(BOUND)

    result: list[object] = []

    @lf.trace(name="mine")
    def mine() -> None:
        lf.info("a")
        lf.info("b")
        shared.append(contextvars.copy_context())
        holder = threading.Thread(daemon=True, target=hold_mutex)
        holder.start()
        assert holding.wait(BOUND)
        monkeypatch.setattr(worker, "_offer", offer)
        try:
            result.append(decorator._sweep_open_spans())
        finally:
            monkeypatch.undo()
            release.set()
            holder.join(BOUND)

    mine()
    lf.flush(BOUND)
    assert result == [False], result
    got = sink.messages("mine")
    assert got == ["span.start", "a", "b", "late", "span.end"], (
        f"the buffer went back out of order: {got}"
    )


def test_a_refused_offer_stops_the_sweep_so_the_hold_is_soon_free(monkeypatch) -> None:
    """Execution review: one refused offer ends the pass rather than each span waiting its bound.

    With 20 open spans and the queue's mutex held by the test, a ``flush()``'s registry sweep is
    refused once and stops, so the hold is free again after about one ``_SWEEP_WAIT``. Continuing
    past a refusal waited the bound once per span, about a second with 20 spans, while every close
    in the process waited on the hold.
    """
    worker = _quiet(Recorder())
    spans = [Parked(f"s{i}") for i in range(20)]
    first_offer = threading.Event()
    real_offer = worker._offer

    def offer(events: list[dict[str, Any]], **kwargs: Any) -> int | None:
        first_offer.set()
        return real_offer(events, **kwargs)

    monkeypatch.setattr(worker, "_offer", offer)
    worker._queue.mutex.acquire()
    try:
        flusher = threading.Thread(daemon=True, target=lambda: lf.flush(BOUND))
        flusher.start()
        assert first_offer.wait(BOUND), "the flush's registry sweep never offered"
        began = time.monotonic()
        with decorator._sweep_hold:
            waited = time.monotonic() - began
    finally:
        worker._queue.mutex.release()
    flusher.join(BOUND)
    monkeypatch.undo()
    assert waited < 0.5, f"the sweep held the hold {waited:.3f}s past a refused offer"
    for parked in spans:
        parked.finish()
    lf.flush(BOUND)
