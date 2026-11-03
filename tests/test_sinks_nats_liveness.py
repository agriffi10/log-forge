"""SPEC-061 — NATSSink liveness: the loop thread, the probe, the confirmed batch, the bounded close.

Every test here reaches the sink through a fake ``nats`` module installed in ``sys.modules``
(SPEC-047 FR-002 AC-5's idiom), because the probe exists only for a client the sink connected
itself. The one fake, :class:`LiveFakeNATS`, models ``nats-py`` 2.16.0 where the spec cites it:
``_pongs`` is a real list of futures popped FIFO by an unguarded ``set_result`` (M5), ``flush``
returns without a PONG when the reader is dead or the client disconnected (M5, M9), and ``drain``
swallows a cancellation before a ``finally`` that flushes with its own 10 s default (M5, M7).
"""

from __future__ import annotations

import ast
import asyncio
import gc
import logging
import pathlib
import sys
import threading
import time
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry.sinks.nats as nats_sink_module
from conftest import run_concurrently
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.nats import (
    CALL_SLACK,
    DEFAULT_CLOSE_TIMEOUT,
    LOOP_JOIN_TIMEOUT,
    NATSSink,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

LOOP_THREAD_NAME = "log-foundry-nats-loop"
"""The name FR-001 gives the loop thread; pinned here rather than imported so a rename is seen."""

MARGIN = 1.0
"""Seconds of slack every timing assertion here adds to the bound it checks.

Generous on purpose: each of these tests proves something IS bounded, so the gap between the
bound and the unbounded behaviour carries the claim, and a tight margin only adds a flake under
twelve xdist workers.
"""


class FlushTimeoutError(TimeoutError):
    """The fake's ``nats.errors.FlushTimeoutError``, a ``TimeoutError`` as the driver's is."""


class LiveFakeJetStream:
    """The fake's ``jetstream()`` context; its publish awaits an ack the server may withhold."""

    def __init__(self, owner: LiveFakeNATS) -> None:
        self._owner = owner

    # The signature MIRRORS `JetStreamContext.publish`, so `timeout` is the driver's API (the same
    # reason `test_sinks_nats.py` gives for its ASYNC109 waiver).
    async def publish(self, subject: str, payload: bytes, *, timeout=None) -> None:  # noqa: ASYNC109
        self._owner.subs = 1
        self._owner.js_timeouts.append(timeout)
        await self._owner._enter_publish(payload)


class LiveFakeNATS:
    """A ``nats-py``-shaped client with the driver's PONG queue, reconnect counter and drain.

    Attributes:
      calls: Every method the sink called, in order: ``"flush"``, ``"publish"``, ``"close"``,
        ``"drain"``.
      threads: The thread each call ran on, parallel to ``calls``.
      flush_timeouts: The ``timeout`` each ``flush`` received, which FR-002 AC-8 asserts.
      answering: Whether a ``flush`` schedules its own PONG one loop turn later.
      reader_dead: Set when a PONG lands on a cancelled future, modelling the reader task ending
        while ``is_connected`` stays true (M3); ``flush`` then returns at once with no PONG.
      is_connected: The driver's flag, a plain attribute the test flips.
      stats: The driver's counters; ``reconnect()`` bumps ``"reconnects"`` and resets ``_pongs``.
      wait_override: When set, the seconds the fake's own ``wait_for`` uses instead of the
        ``timeout`` the sink passed, so an orphaned probe can expire inside a test.
      publish_gate: When set, every publish parks on it until it is set.
      close_gate: When set, ``close`` parks on it until it is set.
      close_raises: An exception type ``close`` raises, or ``None``.
      publish_raises: Raise from every publish when set, modelling "no responders".
      subs: The driver's subscription count; one after the first JetStream publish (M7).
      in_flight: Sink calls currently inside the fake; ``max_in_flight`` is its high-water mark.
    """

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.threads: list[threading.Thread] = []
        self.flush_timeouts: list[float] = []
        self.js_timeouts: list[float | None] = []
        self.published: list[bytes] = []
        self._pongs: list[asyncio.Future[bool]] = []
        self.answering = True
        self.reader_dead = False
        self.is_connected = True
        self.stats: dict[str, int] = {"reconnects": 0}
        self.wait_override: float | None = None
        self.publish_gate: asyncio.Event | None = None
        self.close_gate: asyncio.Event | None = None
        self.close_raises: type[BaseException] | None = None
        self.publish_raises = False
        self.publish_entered = threading.Event()
        self.subs = 0
        self.in_flight = 0
        self.max_in_flight = 0
        self.drain_timeout = 30.0
        self.drain_flush_timeout = 10.0
        self.connected_on: threading.Thread | None = None

    def _record(self, name: str) -> None:
        self.calls.append(name)
        self.threads.append(threading.current_thread())

    def _enter(self) -> None:
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)

    def _leave(self) -> None:
        self.in_flight -= 1

    async def _enter_publish(self, payload: bytes) -> None:
        self._record("publish")
        self._enter()
        self.publish_entered.set()
        try:
            if self.publish_gate is not None:
                await self.publish_gate.wait()
            else:
                await asyncio.sleep(0)
            if self.publish_raises:
                raise RuntimeError("no responders")
            self.published.append(payload)
        finally:
            self._leave()

    async def publish(self, subject: str, payload: bytes) -> None:
        await self._enter_publish(payload)

    def jetstream(self) -> LiveFakeJetStream:
        return LiveFakeJetStream(self)

    # The signature MIRRORS `Client.flush`, so `timeout` is the driver's API and the waiver is the
    # one `test_sinks_nats.py` records for the JetStream double.
    async def flush(self, timeout: float = 10) -> None:  # noqa: ASYNC109
        """``Client.flush`` at 2.16.0 L1251: a PING whose future is cancelled on timeout."""
        self._record("flush")
        self.flush_timeouts.append(timeout)
        if self.reader_dead or not self.is_connected:
            return
        self._enter()
        try:
            future: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
            self._pongs.append(future)
            if self.answering:
                asyncio.get_running_loop().call_soon(self.deliver_pong)
            try:
                wait = timeout if self.wait_override is None else self.wait_override
                await asyncio.wait_for(future, wait)
            except TimeoutError:
                future.cancel()
                raise FlushTimeoutError from None
        finally:
            self._leave()

    def deliver_pong(self) -> None:
        """``_process_pong`` at L1869: pops the oldest future and resolves it, unguarded."""
        if not self._pongs:
            return
        future = self._pongs.pop(0)
        try:
            future.set_result(True)
        except asyncio.InvalidStateError:
            self.reader_dead = True

    def reconnect(self) -> None:
        """What a completed reconnect leaves behind (L1736, L2332): a fresh PONG queue."""
        self._pongs = []
        self.stats["reconnects"] += 1
        self.is_connected = True

    async def close(self) -> None:
        self._record("close")
        self._enter()
        try:
            if self.close_gate is not None:
                await self.close_gate.wait()
            if self.close_raises is not None:
                raise self.close_raises("'NoneType' object is not callable")
            self.is_connected = False
        finally:
            self._leave()

    async def _subs_drained(self) -> None:
        if self.subs:
            await asyncio.Event().wait()

    async def drain(self) -> None:
        """``Client.drain`` at L877–L917: swallows a cancel, then flushes in a ``finally``."""
        self._record("drain")
        try:
            await asyncio.wait_for(self._subs_drained(), self.drain_timeout)
        except asyncio.CancelledError:
            pass
        except TimeoutError:
            pass
        finally:
            await self.flush(timeout=self.drain_flush_timeout)
            await self.close()


class FakeNatsModule:
    """A stand-in for the ``nats`` module whose ``connect`` returns the test's fake on the loop."""

    def __init__(self, factory: Callable[[], Any] = LiveFakeNATS) -> None:
        self.factory = factory
        self.calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        self.connected_on: threading.Thread | None = None
        self.fake: Any = None

    async def connect(self, *args: Any, **kwargs: Any) -> Any:
        self.calls.append((args, kwargs))
        self.connected_on = threading.current_thread()
        self.fake = self.factory()
        self.fake.connected_on = threading.current_thread()
        return self.fake


def _build(
    monkeypatch: pytest.MonkeyPatch,
    factory: Callable[[], Any] = LiveFakeNATS,
    **kwargs: Any,
) -> tuple[NATSSink, Any, FakeNatsModule]:
    module = FakeNatsModule(factory)
    monkeypatch.setitem(sys.modules, "nats", module)
    sink = NATSSink("logs", servers="nats://example:4222", **kwargs)
    return sink, module.fake, module


def _loop_threads() -> set[threading.Thread]:
    return {t for t in threading.enumerate() if t.name == LOOP_THREAD_NAME}


@pytest.fixture(autouse=True)
def _no_loop_thread_outlives_its_test() -> Iterator[None]:
    """Every loop thread a test starts is gone when the test ends, joined under a bound.

    A snapshot taken before the test, so another file's leaked sink is not charged to this one;
    a bounded join rather than a bare liveness check, because the wedged-loop test deliberately
    leaves a thread sleeping that ends a few seconds later on its own.
    """
    before = _loop_threads()
    yield
    started = [t for t in _loop_threads() if t not in before]
    for thread in started:
        thread.join(10.0)
    alive = [t for t in started if t.is_alive()]
    assert not alive, f"{len(alive)} loop thread(s) outlived the test"


def _on_loop(sink: NATSSink, coro: Any) -> Any:
    return asyncio.run_coroutine_threadsafe(coro, sink._loop).result(5.0)


def _pong(sink: NATSSink, fake: LiveFakeNATS) -> None:
    """Delivers one PONG on the loop thread and lets the probe task finish before returning."""

    async def deliver() -> None:
        fake.deliver_pong()
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    _on_loop(sink, deliver())


def _open(sink: NATSSink, gate: asyncio.Event) -> None:
    async def release() -> None:
        gate.set()

    _on_loop(sink, release())


# --- FR-001: the loop runs on a sink-owned thread for the sink's life --------------------------


def test_construction_starts_one_daemon_loop_thread_and_connects_on_it(monkeypatch) -> None:
    # FR-001 AC-1. Counted as a delta over a snapshot, because another test's unclosed sink in
    # the same process would otherwise be counted here.
    before = _loop_threads()
    sink, _, module = _build(monkeypatch)
    try:
        started = _loop_threads() - before
        assert len(started) == 1
        (thread,) = started
        assert thread.daemon
        assert thread is sink._thread
        assert module.connected_on is thread, "connect must run on the loop that drives the client"
    finally:
        sink.close()


def test_every_client_call_runs_on_the_loop_thread(monkeypatch) -> None:
    # FR-001 AC-2, and FR-001 AC-13's evidence that all three public calls take the one helper.
    sink, fake, _ = _build(monkeypatch)
    sink.emit([{"n": 1}])
    sink.flush()
    sink.close()

    assert {"publish", "flush", "close"} <= set(fake.calls)
    assert fake.threads, "the fake recorded nothing"
    assert all(t is sink._thread for t in fake.threads), {t.name for t in fake.threads}
    assert threading.current_thread() not in fake.threads


def test_a_refused_construction_starts_no_thread_and_builds_no_loop(monkeypatch) -> None:
    # FR-001 AC-3, first half.
    before = _loop_threads()
    loops: list[asyncio.AbstractEventLoop] = []
    real = asyncio.new_event_loop
    monkeypatch.setattr(asyncio, "new_event_loop", lambda: loops.append(real()) or loops[-1])
    with pytest.raises(ValueError, match="max_reconnect_attempts"):
        _build(monkeypatch, max_reconnect_attempts=0)

    assert _loop_threads() == before
    assert loops == []


@pytest.mark.parametrize("failure", ["raise", "import"])
def test_a_failed_connect_leaves_no_thread_and_a_closed_loop_and_reraises(
    monkeypatch, failure: str
) -> None:
    # FR-001 AC-3, second half, for a connect that raises and for a missing extra: either way
    # the thread already started must not outlive the constructor that failed.
    before = _loop_threads()
    loops: list[asyncio.AbstractEventLoop] = []
    real = asyncio.new_event_loop
    monkeypatch.setattr(asyncio, "new_event_loop", lambda: loops.append(real()) or loops[-1])

    class Exploding(FakeNatsModule):
        async def connect(self, *args: Any, **kwargs: Any) -> Any:
            raise OSError("nats: no servers available for connection")

    if failure == "raise":
        monkeypatch.setitem(sys.modules, "nats", Exploding())
        expected: type[BaseException] = OSError
    else:
        monkeypatch.setitem(sys.modules, "nats", None)
        expected = ImportError
    with pytest.raises(expected):
        NATSSink("logs", servers="nats://example:4222")
    for thread in _loop_threads() - before:
        thread.join(2.0)

    assert _loop_threads() == before, "the thread started for the connect must be gone"
    assert len(loops) == 1 and loops[0].is_closed()


class ReconnectingFakeNATS(LiveFakeNATS):
    """Starts disconnected and reconnects on a background task, as the driver's own does."""

    def __init__(self) -> None:
        super().__init__()
        self.is_connected = False
        self.reconnect_task = asyncio.get_running_loop().create_task(self._reconnect_later())

    async def _reconnect_later(self) -> None:
        await asyncio.sleep(0.3)
        self.is_connected = True


def test_the_drivers_background_tasks_run_between_calls(monkeypatch) -> None:
    """FR-001 AC-4: a reconnect that needs loop time with no call in progress still completes.

    Against the sink at ``71e42dd`` this emit is refused: the loop ran only inside
    ``run_until_complete``, so the reconnect task created in ``connect`` took its first step
    there, parked on its sleep, and never ran again — ``emit`` read ``is_connected`` false
    without touching the loop. Run once against that file in a scratch copy (SPEC-061 M2's
    shape): ``REFUSED: NATSSink published none of 1 event(s): the client is disconnected``.
    Recorded here because the premise of the whole spec is that the driver's tasks need CPU
    between calls.
    """
    sink, fake, _ = _build(monkeypatch, factory=ReconnectingFakeNATS)
    try:
        assert fake.is_connected is False
        time.sleep(0.5)
        sink.emit([{"n": 1}])
        assert fake.published
    finally:
        sink.close()


def test_concurrent_emit_flush_and_close_serialize_on_the_loop(monkeypatch) -> None:
    """FR-001 AC-5: ten threads through one submit-and-wait, with a preemption after the submit.

    The preemption point is a seam, not a reimplementation: ``_submit`` is wrapped so the first
    caller parks after its coroutine is on the loop and before it waits, holding the lock, while
    the other nine pile up behind it. The first caller's publish is parked on the fake's gate, so
    a lock released before the wait (mutant 3) would let a second emit's coroutine run on the
    loop beside the parked one, and the fake's ``max_in_flight`` would read two.
    """
    sink, fake, _ = _build(monkeypatch, publish_timeout=5.0)
    gate = asyncio.Event()
    fake.publish_gate = gate
    inside = threading.Event()
    go = threading.Event()
    parked_once = threading.Lock()
    real_submit = NATSSink._submit

    def preempting(self: NATSSink, coro: Any) -> Any:
        future = real_submit(self, coro)
        if parked_once.acquire(blocking=False):
            inside.set()
            go.wait(10.0)
        return future

    monkeypatch.setattr(NATSSink, "_submit", preempting)

    def release_when_parked() -> None:
        assert inside.wait(5.0), "no caller reached the seam"
        _open(sink, gate)
        go.set()

    releaser = threading.Thread(target=release_when_parked)
    releaser.start()

    def work(index: int, _iteration: int) -> None:
        if index == 9:
            sink.close()
        elif index == 8:
            sink.flush()
        else:
            sink.emit([{"n": index}])

    began = time.monotonic()
    errors = run_concurrently(work, 10)
    releaser.join(5.0)
    sink._thread.join(LOOP_JOIN_TIMEOUT + MARGIN)

    assert time.monotonic() - began < 15.0, "a thread took longer than the stated deadline"
    assert all(isinstance(e, SinkDeliveryError) for e in errors), errors
    assert fake.max_in_flight == 1, "two coroutines were inside the fake at once"
    assert not sink._thread.is_alive()


class ParkedJetStreamNATS(LiveFakeNATS):
    """A JetStream fake whose publish ignores the ack timeout the sink passes and never returns.

    JetStream mode is where the backstop is reachable through the driver's own shape: a core
    publish is wrapped in a task the sink bounds itself (SPEC-061 M-2), but a JetStream publish
    is awaited directly under the driver's ack timeout, so a driver that ignores that timeout is
    the coroutine that does not honour its deadline.
    """

    def __init__(self) -> None:
        super().__init__()
        self.publish_gate = asyncio.Event()


def test_the_backstop_closes_the_sink_when_a_publish_never_returns(monkeypatch) -> None:
    # FR-001 AC-6. Run on a helper thread with a bounded join, so a backstop that does not fire
    # fails the test rather than hanging the runner.
    sink, _, _ = _build(
        monkeypatch, factory=ParkedJetStreamNATS, jetstream=True, publish_timeout=0.2
    )
    outcome: list[BaseException | None] = []

    def emit() -> None:
        try:
            sink.emit([{"n": 1}])
        except BaseException as exc:
            outcome.append(exc)
        else:
            outcome.append(None)

    began = time.monotonic()
    caller = threading.Thread(target=emit)
    caller.start()
    caller.join(0.2 + CALL_SLACK + MARGIN + 5.0)
    elapsed = time.monotonic() - began

    assert not caller.is_alive(), "the backstop never fired"
    assert isinstance(outcome[0], SinkDeliveryError) and "wedged" in str(outcome[0])
    assert elapsed <= 0.2 + CALL_SLACK + LOOP_JOIN_TIMEOUT + MARGIN, f"{elapsed:.2f}s"
    with pytest.raises(SinkDeliveryError, match="closed"):
        sink.emit([{"n": 2}])
    sink._thread.join(LOOP_JOIN_TIMEOUT)
    assert not sink._thread.is_alive(), "the cancel-then-stop never ended the loop"
    sink.close()


def test_close_leaves_no_thread_no_open_loop_and_no_pending_task(monkeypatch, capsys) -> None:
    # FR-001 AC-7.
    records: list[logging.LogRecord] = []
    handler = logging.Handler()
    handler.emit = records.append  # type: ignore[method-assign]
    logging.getLogger("asyncio").addHandler(handler)
    try:
        sink, _, _ = _build(monkeypatch)
        sink.emit([{"n": 1}])
        sink.close()
        assert not sink._thread.is_alive()
        assert sink._loop.is_closed()
        assert asyncio.all_tasks(sink._loop) == set()
        del sink
        gc.collect()
    finally:
        logging.getLogger("asyncio").removeHandler(handler)

    assert "destroyed but it is pending" not in capsys.readouterr().err
    assert not [r for r in records if "destroyed but it is pending" in r.getMessage()]


def test_a_wedged_loop_makes_close_raise_timeout_and_abandon_the_thread(monkeypatch) -> None:
    # FR-001 AC-8. The wedge is a blocking call ON the loop thread (what a driver that blocks
    # inside a callback looks like), 5 s long so the close's whole bound expires inside it.
    sink, fake, _ = _build(monkeypatch, drain_timeout=0.2)
    wedged = threading.Event()

    def wedge() -> None:
        wedged.set()
        time.sleep(5.0)

    sink._loop.call_soon_threadsafe(wedge)
    assert wedged.wait(2.0)

    began = time.monotonic()
    with pytest.raises(TimeoutError):
        sink.close()
    elapsed = time.monotonic() - began

    assert elapsed <= 0.2 + CALL_SLACK + LOOP_JOIN_TIMEOUT + MARGIN, f"{elapsed:.2f}s"
    assert sink._closed
    assert not sink._loop.is_closed(), "a running loop cannot be closed; it is left unclosed"
    assert sink._thread.is_alive() and sink._thread.daemon
    closes_before = fake.calls.count("close")
    sink.close()
    assert fake.calls.count("close") == closes_before, "a second close must not touch the client"
    with pytest.raises(SinkDeliveryError, match="closed"):
        sink.emit([{"n": 1}])


def test_after_close_emit_and_flush_refuse_and_a_second_close_is_inert(monkeypatch) -> None:
    # FR-001 AC-9.
    sink, fake, _ = _build(monkeypatch)
    sink.emit([{"n": 1}])
    sink.close()
    before = sink.losses()

    with pytest.raises(SinkDeliveryError, match="closed"):
        sink.emit([{"n": 2}])
    with pytest.raises(SinkDeliveryError, match="closed"):
        sink.flush()
    sink.close()

    assert sink.losses() == before == SinkLosses(dropped=0, failed=0)
    assert fake.calls.count("close") == 1


def test_a_forked_child_refuses_without_touching_the_lock_loop_or_client(monkeypatch) -> None:
    """FR-001 AC-10, with a real ``os.fork`` while a caller holds ``self._lock``.

    The sink is unreachable from ``log_foundry``'s modules, so ``_fork._reinit_primitives`` does
    not replace its lock and the child inherits it **held**. A pid check made after the lock
    acquisition hangs the child on that lock and the alarm kills it; made before, the child
    refuses at once and touches nothing it did not acquire.
    """
    from test_fork_lifecycle import run_in_child

    sink, fake, _ = _build(monkeypatch, publish_timeout=20.0)
    gate = asyncio.Event()
    fake.publish_gate = gate
    parked = threading.Thread(target=lambda: sink.emit([{"n": 1}]))
    parked.start()
    assert fake.publish_entered.wait(5.0), "the caller never reached the fake's publish"
    assert sink._lock.locked()
    calls_at_fork = len(fake.calls)

    def work() -> str:
        notes: list[str] = []
        touched: list[str] = []
        sink._loop.call_soon_threadsafe = lambda *a, **k: touched.append("loop")  # type: ignore[method-assign]
        sink._thread.join = lambda *a, **k: touched.append("join")  # type: ignore[method-assign]
        for name, call in (("emit", lambda: sink.emit([{"n": 2}])), ("flush", sink.flush)):
            try:
                call()
            except SinkDeliveryError as exc:
                notes.append(f"{name}:{'forked child' in str(exc)}")
            else:
                notes.append(f"{name}:accepted")
        sink.close()
        notes.append(f"calls:{len(fake.calls) - calls_at_fork}")
        notes.append(f"touched:{','.join(touched)}")
        notes.append(f"losses:{sink.losses().failed}")
        return ";".join(notes)

    try:
        child = run_in_child(work, timeout=5)
    finally:
        _open(sink, gate)
        parked.join(5.0)

    assert child.output == "emit:True;flush:True;calls:0;touched:;losses:0", child.output
    assert child.status == 0, child.output
    sink.emit([{"n": 3}])
    sink.close()
    assert len(fake.published) == 2


class AnnouncingLock:
    """A lock that counts the threads waiting on it, so a test can see callers queued behind it."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.waiting = 0
        self._changed = threading.Condition()

    def __enter__(self) -> None:
        with self._changed:
            self.waiting += 1
            self._changed.notify_all()
        self._lock.acquire()
        with self._changed:
            self.waiting -= 1

    def __exit__(self, *exc: object) -> None:
        self._lock.release()

    def locked(self) -> bool:
        return self._lock.locked()

    def wait_for_waiters(self, count: int, timeout: float = 5.0) -> bool:
        with self._changed:
            return self._changed.wait_for(lambda: self.waiting >= count, timeout)


def test_a_closer_is_not_starved_by_queued_emits(monkeypatch) -> None:
    """FR-001 AC-12: ``close()`` behind one blocked emit and three queued ones is served next.

    The three queued emits took the lock's queue before ``close()`` set its flag, so only the
    post-acquire read refuses them (mutant 5); a fourth emit started after the flag is set is
    refused before it even queues, within about 50 ms, by the pre-lock read (mutant 4).
    """
    sink, fake, _ = _build(monkeypatch, publish_timeout=20.0)
    lock = AnnouncingLock()
    sink._lock = lock  # type: ignore[assignment]
    gate = asyncio.Event()
    fake.publish_gate = gate
    errors: dict[str, BaseException | None] = {}

    def emit(name: str) -> None:
        try:
            sink.emit([{"n": name}])
        except BaseException as exc:
            errors[name] = exc
        else:
            errors[name] = None

    first = threading.Thread(target=emit, args=("first",))
    first.start()
    assert fake.publish_entered.wait(5.0)
    queued = [threading.Thread(target=emit, args=(f"queued{i}",)) for i in range(3)]
    for thread in queued:
        thread.start()
    assert lock.wait_for_waiters(3), "the queued emits never reached the lock"

    closed_at: list[float] = []
    closer = threading.Thread(target=lambda: (sink.close(), closed_at.append(time.monotonic())))
    closer.start()
    assert lock.wait_for_waiters(4), "close() never reached the lock"
    assert sink._closing
    publishes_when_close_began = fake.calls.count("publish")

    began = time.monotonic()
    with pytest.raises(SinkDeliveryError, match="closing"):
        sink.emit([{"n": "fourth"}])
    assert time.monotonic() - began < 0.05, "the pre-lock read must refuse without queueing"

    released_at = time.monotonic()
    _open(sink, gate)
    first.join(5.0)
    for thread in queued:
        thread.join(5.0)
    closer.join(DEFAULT_CLOSE_TIMEOUT + LOOP_JOIN_TIMEOUT + MARGIN)

    assert errors["first"] is None
    for i in range(3):
        assert isinstance(errors[f"queued{i}"], SinkDeliveryError), errors
        assert "closing" in str(errors[f"queued{i}"])
    assert fake.calls.count("publish") == publishes_when_close_began
    assert (
        closed_at
        and closed_at[0] - released_at <= DEFAULT_CLOSE_TIMEOUT + LOOP_JOIN_TIMEOUT + MARGIN
    )


def test_every_public_call_reaches_the_loop_through_one_helper() -> None:
    # FR-001 AC-13, by AST: `emit`, `flush` and `close` each call `_run`, and the module hands
    # work to the loop in exactly one place, `_submit`.
    source = pathlib.Path(nats_sink_module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    sink_class = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "NATSSink")
    methods = {n.name: n for n in sink_class.body if isinstance(n, ast.FunctionDef)}

    def calls(node: ast.AST, attr: str) -> int:
        return sum(
            1
            for sub in ast.walk(node)
            if isinstance(sub, ast.Call)
            and isinstance(sub.func, ast.Attribute)
            and sub.func.attr == attr
        )

    for name in ("emit", "flush", "close"):
        assert calls(methods[name], "_run") == 1, f"{name} must reach the loop through _run"
    submitters = [name for name, node in methods.items() if calls(node, "run_coroutine_threadsafe")]
    assert submitters == ["_submit"], submitters
    assert source.count("run_coroutine_threadsafe") == 1
