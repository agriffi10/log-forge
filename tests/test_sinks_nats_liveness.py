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
import contextlib
import gc
import logging
import os
import pathlib
import sys
import threading
import time
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry.sinks.nats as nats_sink_module
from conftest import DelayArmedOnUse, run_concurrently
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.nats import (
    CALL_SLACK,
    DEFAULT_CLOSE_TIMEOUT,
    LOOP_JOIN_TIMEOUT,
    PROBE_FLUSH_TIMEOUT,
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

LOCK_HOLD = 5.0
"""Seconds a test's holder thread keeps the sink's lock: what a caller queued behind it waits."""

QUEUE_CEILING = LOCK_HOLD / 2
"""The ceiling on a call the latch must refuse without queueing on the held lock.

Half of :data:`LOCK_HOLD`, the wait a caller that queued instead would take, so a CI stall of a
few hundred milliseconds cannot fail an honest refusal while a queued caller still overshoots
the ceiling by 2.5 s.
"""

AT_ONCE_BUDGET = 1.0
"""The ``publish_timeout`` of a test whose guarded call settles at once and whose defect waits.

Each defect these tests guard spends the whole budget before the call returns: a probe waited
out rather than settled, a confirmation waited for where none is due, a reused PING waited on
instead of the latch refusing. A budget under 1.0 s left the defect's wait so short that a
ceiling below it sat within reach of a CI stall (123 ms has been seen).
"""

AT_ONCE_CEILING = AT_ONCE_BUDGET / 2
"""The ceiling on a call that must settle at once: half the budget its defect waits out."""


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
      timed_out: How many ``flush`` calls expired on the fake's own ``wait_for``.
      wait_override: When set, the seconds the fake's own ``wait_for`` uses instead of the
        ``timeout`` the sink passed, so an orphaned probe can expire inside a test.
      publish_gate: When set, every publish parks on it until it is set.
      close_gate: When set, ``close`` parks on it until it is set.
      close_raises: An exception type ``close`` raises, or ``None``.
      publish_raises: Raise from every publish when set, modelling "no responders".
      publish_delay: Seconds each publish sleeps on the loop, so a batch can spend its budget.
      subs: The driver's subscription count; one after the first JetStream publish (M7).
      in_flight: Sink calls currently inside the fake; ``max_in_flight`` is its high-water mark.
      pending_data_size: The driver's public counter of bytes queued for its flusher. A publish
        adds its payload and kicks the flusher; a PING adds ``len(b"PING\\r\\n")`` without
        queueing anything, as ``_send_ping`` does, and the flusher's next run resets it to zero.
      flusher_parked: While true the flusher never runs, as it is while the transport is past
        its high-water mark and the flusher sits in ``drain()``; the counter then stays non-zero.
      pubs_at_ping: How many publishes the flusher had written when each PING was sent, so a
        test can see a PING overtake the buffer.
      close_swallows_one_cancel: Whether ``close`` absorbs its first cancellation and keeps
        waiting, as the driver's ``_close`` does around its reconnection wait (client.py L820).
      close_ended: Set once ``close`` has returned or raised, so a test can see whether the close
        task ended inside the sink's close sequence or only when the loop was stopped.
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
        self.timed_out = 0
        self.wait_override: float | None = None
        self.publish_gate: asyncio.Event | None = None
        self.close_gate: asyncio.Event | None = None
        self.close_raises: type[BaseException] | None = None
        self.publish_raises = False
        self.publish_delay = 0.0
        self.publish_entered = threading.Event()
        self.subs = 0
        self.in_flight = 0
        self.max_in_flight = 0
        self.pending_data_size = 0
        self.flusher_parked = False
        self.written: list[bytes] = []
        self.pubs_at_ping: list[int] = []
        self.close_swallows_one_cancel = True
        self._cancels_swallowed = 0
        self.close_ended = False
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

    def _add_pending(self, size: int) -> None:
        self.pending_data_size += size

    def _kick_flusher(self) -> None:
        asyncio.get_running_loop().call_soon(self._run_flusher)

    def _run_flusher(self) -> None:
        """``Client._flusher`` L2348: writes the queue and resets the counter, unless parked."""
        if self.flusher_parked:
            return
        self.written.extend(self.published[len(self.written) :])
        self.pending_data_size = 0

    async def _enter_publish(self, payload: bytes) -> None:
        self._record("publish")
        self._enter()
        self.publish_entered.set()
        try:
            if self.publish_gate is not None:
                await self.publish_gate.wait()
            else:
                await asyncio.sleep(self.publish_delay)
            if self.publish_raises:
                raise RuntimeError("no responders")
            self.published.append(payload)
            self._add_pending(len(payload))
            self._kick_flusher()
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
            self.pubs_at_ping.append(len(self.written))
            self._add_pending(len(b"PING\r\n"))
            self._kick_flusher()
            if self.answering:
                asyncio.get_running_loop().call_soon(self.deliver_pong)
            try:
                wait = timeout if self.wait_override is None else self.wait_override
                await asyncio.wait_for(future, wait)
            except TimeoutError:
                future.cancel()
                self.timed_out += 1
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
            while self.close_gate is not None and not self.close_gate.is_set():
                try:
                    await self.close_gate.wait()
                except asyncio.CancelledError:
                    if not self.close_swallows_one_cancel or self._cancels_swallowed:
                        raise
                    self._cancels_swallowed += 1
            if self.close_raises is not None:
                raise self.close_raises("'NoneType' object is not callable")
            self.is_connected = False
        finally:
            self.close_ended = True
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


class BlockingPublishNATS(LiveFakeNATS):
    """A fake whose publish blocks the loop thread itself for 3 s, as a driver stuck in C would.

    Every await the sink makes is bounded by ``asyncio.wait`` (the probe, each publish on both
    paths, the drained wait, an injected flush), so a coroutine that merely never resolves can
    no longer reach the backstop; what can is a loop thread that does not return to the loop,
    which is the wedge the backstop exists for.
    """

    async def _enter_publish(self, payload: bytes) -> None:
        self._record("publish")
        # A deliberate block of the loop thread: the whole point of this fake.
        time.sleep(3.0)  # noqa: ASYNC251
        self.published.append(payload)


def test_the_backstop_closes_the_sink_when_a_publish_never_returns(monkeypatch) -> None:
    # FR-001 AC-6. Run on a helper thread with a bounded join, so a backstop that does not fire
    # fails the test rather than hanging the runner.
    sink, _, _ = _build(monkeypatch, factory=BlockingPublishNATS, publish_timeout=0.2)
    outcome: list[BaseException | None] = []

    def emit() -> None:
        try:
            sink.emit([{"n": 1}])
        except BaseException as exc:
            outcome.append(exc)
        else:
            outcome.append(None)

    began = time.monotonic()
    caller = threading.Thread(target=emit, daemon=True)
    caller.start()
    caller.join(0.2 + CALL_SLACK + MARGIN + 5.0)
    elapsed = time.monotonic() - began

    assert not caller.is_alive(), "the backstop never fired"
    assert isinstance(outcome[0], SinkDeliveryError) and "wedged" in str(outcome[0])
    assert elapsed <= 0.2 + CALL_SLACK + LOOP_JOIN_TIMEOUT + MARGIN, f"{elapsed:.2f}s"
    with pytest.raises(SinkDeliveryError, match="closed"):
        sink.emit([{"n": 2}])
    assert sink._thread.is_alive(), "the wedged thread is abandoned as a daemon, not joined"
    sink._thread.join(3.0 + LOOP_JOIN_TIMEOUT)
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
    """A first-come lock that counts the threads waiting on it, so a test fixes who is served next.

    ``threading.Lock`` is not FIFO, so a test that queues callers behind it cannot say which one
    the release wakes. Serving tickets in arrival order makes that a fact of the test rather than
    of the scheduler, and ``entered`` counts every caller that ever reached the lock.
    """

    def __init__(self) -> None:
        self._changed = threading.Condition()
        self._held = False
        self._next_ticket = 0
        self._serving = 0
        self.waiting = 0
        self.entered = 0

    def __enter__(self) -> None:
        with self._changed:
            ticket = self._next_ticket
            self._next_ticket += 1
            self.entered += 1
            self.waiting += 1
            self._changed.notify_all()
            self._changed.wait_for(lambda: not self._held and self._serving == ticket)
            self._held = True
            self._serving += 1
            self.waiting -= 1

    def __exit__(self, *exc: object) -> None:
        with self._changed:
            self._held = False
            self._changed.notify_all()

    def locked(self) -> bool:
        return self._held

    def wait_for_waiters(self, count: int, timeout: float = 5.0) -> bool:
        with self._changed:
            return self._changed.wait_for(lambda: self.waiting >= count, timeout)


def test_a_closer_is_not_starved_by_queued_emits(monkeypatch) -> None:
    """FR-001 AC-12: ``close()`` behind one blocked emit and three queued ones is served next.

    The three queued emits took the lock's queue before ``close()`` set its flag, so only the
    post-acquire read refuses them (mutant 5); a fourth emit started after the flag is set is
    refused without ever reaching the lock, by the pre-lock read (mutant 4).

    The lock is first-come, so the queued emits are served before the closer by construction. On
    the sink's own lock the release may wake the closer first instead, and the queued emits then
    find the sink closed rather than closing: refused just the same, but by the ``_closed`` read,
    an ordering in which mutant 5 survives and the message names a closed sink.
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
    entered_when_close_began = lock.entered

    fourth = threading.Thread(target=emit, args=("fourth",))
    fourth.start()
    fourth.join(5.0)
    assert not fourth.is_alive(), "the fourth emit queued on the lock instead of being refused"
    assert isinstance(errors["fourth"], SinkDeliveryError), errors
    assert "closing" in str(errors["fourth"])
    assert lock.entered == entered_when_close_began, (
        "the pre-lock read must refuse without queueing"
    )

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
    # FR-001 AC-13, by AST: `emit`'s round (`_round`, since SPEC-074 put `emit` on the engine),
    # `flush` and `close` each call `_run`, and the module hands work to the loop in exactly one
    # place, `_submit`.
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

    for name in ("_round", "flush", "close"):
        assert calls(methods[name], "_run") == 1, f"{name} must reach the loop through _run"
    submitters = [name for name, node in methods.items() if calls(node, "run_coroutine_threadsafe")]
    assert submitters == ["_submit"], submitters
    assert source.count("run_coroutine_threadsafe") == 1


# --- FR-002: an emit publishes only after the server answers a round trip ---------------------


class StalledFakeNATS(LiveFakeNATS):
    """A client whose server never answers a PING: the connection is open and silent (M1)."""

    def __init__(self) -> None:
        super().__init__()
        self.answering = False


def _timed(call: Callable[[], object]) -> tuple[float, BaseException | None]:
    began = time.monotonic()
    try:
        call()
    except BaseException as exc:
        return time.monotonic() - began, exc
    return time.monotonic() - began, None


@pytest.mark.parametrize("jetstream", [False, True], ids=["core", "jetstream"])
def test_emit_probes_once_before_its_first_publish(monkeypatch, jetstream: bool) -> None:
    # FR-002 AC-1.
    sink, fake, _ = _build(monkeypatch, jetstream=jetstream)
    sink.emit([{"n": 1}])
    sink.close()

    assert fake.calls[:2] == ["flush", "publish"], fake.calls
    assert fake.published


@pytest.mark.parametrize("jetstream", [False, True], ids=["core", "jetstream"])
def test_an_unanswered_probe_refuses_within_the_budget_and_counts_nothing(
    monkeypatch, capsys, jetstream: bool
) -> None:
    # FR-002 AC-2.
    sink, fake, _ = _build(
        monkeypatch, factory=StalledFakeNATS, jetstream=jetstream, publish_timeout=0.3
    )
    elapsed, error = _timed(lambda: sink.emit([{"n": 1}]))
    losses = sink.losses()
    with contextlib.suppress(TimeoutError):
        sink.close()

    assert isinstance(error, SinkDeliveryError) and "PONG" in str(error)
    assert 0.3 <= elapsed <= 0.3 + CALL_SLACK + MARGIN, f"{elapsed:.2f}s"
    assert "publish" not in fake.calls
    assert losses == SinkLosses(dropped=0, failed=0)
    assert "lost" not in capsys.readouterr().err


class RaisingFlushNATS(LiveFakeNATS):
    """A client whose ``flush`` raises ``FlushTimeoutError`` at once, before any PING is queued."""

    async def flush(self, timeout: float = 10) -> None:  # noqa: ASYNC109
        self._record("flush")
        self.flush_timeouts.append(timeout)
        raise FlushTimeoutError


def test_a_probe_that_raises_is_not_an_answer_and_the_next_call_sends_a_fresh_ping(
    monkeypatch,
) -> None:
    # FR-002 AC-2, the raising variant (plan finding M-4): condition 1 of the answer rule.
    sink, fake, _ = _build(monkeypatch, factory=RaisingFlushNATS, publish_timeout=AT_ONCE_BUDGET)
    elapsed, error = _timed(lambda: sink.emit([{"n": 1}]))
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"n": 2}])
    losses = sink.losses()
    with contextlib.suppress(Exception):
        sink.close()

    assert isinstance(error, SinkDeliveryError) and "PONG" in str(error)
    assert elapsed < AT_ONCE_CEILING, "a probe that raised is settled at once, not waited for"
    assert "publish" not in fake.calls
    assert losses == SinkLosses(dropped=0, failed=0)
    assert fake.calls[:2] == ["flush", "flush"], "a finished probe is discarded and re-sent"


def test_a_stall_sends_one_ping_across_five_refusals(monkeypatch) -> None:
    # FR-002 AC-3: the first refusal waits, the four after it are latched; one PING in total and
    # never two futures in the driver's queue.
    sink, fake, _ = _build(monkeypatch, factory=StalledFakeNATS, publish_timeout=0.2)
    pongs_seen: list[int] = []
    for _ in range(5):
        with pytest.raises(SinkDeliveryError):
            sink.emit([{"n": 1}])
        pongs_seen.append(len(fake._pongs))
    with contextlib.suppress(TimeoutError):
        sink.close()

    assert fake.calls.count("flush") == 1, fake.calls
    assert max(pongs_seen) <= 1, pongs_seen


def test_a_late_pong_does_not_kill_the_reader_and_a_second_stall_is_still_refused(
    monkeypatch,
) -> None:
    """FR-002 AC-4, the M3 regression.

    The driver's reader calls ``set_result`` on the oldest PONG future unguarded; a future the
    sink cancelled raises ``InvalidStateError`` there and the reader dies while ``is_connected``
    stays true, after which every ``flush`` returns at once with no round trip and a stall is
    accepted. The fake models exactly that, so a probe that cancels is caught by the second
    stall being accepted rather than refused.
    """
    sink, fake, _ = _build(monkeypatch, factory=StalledFakeNATS, publish_timeout=0.2)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"n": 1}])
    _pong(sink, fake)
    fake.answering = True
    sink.emit([{"n": 2}])
    fake.answering = False
    second_stall = _timed(lambda: sink.emit([{"n": 3}]))
    with contextlib.suppress(TimeoutError):
        sink.close()

    assert fake.reader_dead is False, "the sink cancelled a PONG wait the server answered"
    assert fake.published == [b'{"n": 2}']
    assert isinstance(second_stall[1], SinkDeliveryError)


def test_the_cancelling_variant_fails_that_regression(monkeypatch) -> None:
    # FR-002 AC-4's second half: the same test against a probe that cancels on its own deadline
    # (`await client.flush(timeout=remaining)`), which must fail it in the stated way. This is
    # the shape of the vacuity the real test refuses: the reader dies and the second stall is
    # accepted with nothing refused.
    async def cancelling(self: NATSSink, deadline: float) -> bool:
        try:
            await self._client.flush(timeout=max(0.01, deadline - time.monotonic()))
        except Exception:
            return False
        return True

    monkeypatch.setattr(NATSSink, "_await_probe", cancelling)
    sink, fake, _ = _build(monkeypatch, factory=StalledFakeNATS, publish_timeout=0.2)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"n": 1}])
    _pong(sink, fake)
    fake.answering = True
    sink.emit([{"n": 2}])
    fake.answering = False
    second_stall = _timed(lambda: sink.emit([{"n": 3}]))
    sink.close()

    assert fake.reader_dead is True, "the cancelling variant must kill the fake's reader"
    assert second_stall[1] is None, "and the second stall must then be accepted"
    assert len(fake.published) == 2


def test_a_pong_arriving_between_calls_is_not_credited(monkeypatch) -> None:
    # FR-002 AC-6.
    sink, fake, _ = _build(monkeypatch, factory=StalledFakeNATS, publish_timeout=0.2)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"n": 1}])
    _pong(sink, fake)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"n": 2}])
    with contextlib.suppress(TimeoutError):
        sink.close()

    assert fake.calls.count("flush") == 2, "the next emit must send its own PING"
    assert "publish" not in fake.calls


def test_a_reconnect_discards_the_orphaned_probe_and_its_outcome_is_retrieved(
    monkeypatch,
) -> None:
    # FR-002 AC-7. `wait_override` compresses the orphan's inner 600 s to 0.2 s so it expires
    # inside the test; its `FlushTimeoutError` must then be retrieved by the done callback, which
    # `gc.collect()` with a handler on the asyncio logger is the only way to observe.
    records: list[logging.LogRecord] = []
    handler = logging.Handler()
    handler.emit = records.append  # type: ignore[method-assign]
    logging.getLogger("asyncio").addHandler(handler)
    try:
        sink, fake, _ = _build(monkeypatch, factory=StalledFakeNATS, publish_timeout=0.2)
        fake.wait_override = 0.2
        with pytest.raises(SinkDeliveryError):
            sink.emit([{"n": 1}])
        assert sink._probe is not None
        orphan = sink._probe[0]
        _on_loop(sink, _run_sync(fake.reconnect))
        fake.answering = True
        sink.emit([{"n": 2}])
        _on_loop(sink, asyncio.wait({orphan}, timeout=2.0))
        # Asserted through the fake, never through `orphan.exception()`: reading the exception
        # here would itself mark it retrieved and hide a sink that never did (mutant 26 survived
        # exactly that version of this test).
        assert orphan.done() and not orphan.cancelled() and fake.timed_out == 1
        del orphan
        sink.close()
        gc.collect()
    finally:
        logging.getLogger("asyncio").removeHandler(handler)

    assert fake.calls[:3] == ["flush", "flush", "publish"], (
        "the reconnect must make the next emit send afresh rather than wait on the orphan"
    )
    assert fake.published == [b'{"n": 2}']
    assert not [r for r in records if "never retrieved" in r.getMessage()], records


async def _run_sync(fn: Callable[[], None]) -> None:
    fn()


def test_every_probe_carries_the_constant_timeout_and_no_private_name_is_used(
    monkeypatch,
) -> None:
    # FR-002 AC-8.
    sink, fake, _ = _build(monkeypatch)
    sink.emit([{"n": 1}])
    sink.flush()
    sink.close()
    source = pathlib.Path(nats_sink_module.__file__).read_text(encoding="utf-8")

    assert fake.flush_timeouts, "no probe was sent"
    assert fake.flush_timeouts == [PROBE_FLUSH_TIMEOUT] * len(fake.flush_timeouts)
    assert "_send_ping" not in source and "_pongs" not in source


def test_the_probe_timeout_exceeds_the_drivers_stale_threshold() -> None:
    # FR-002 AC-9, the idiom of `test_our_ack_ceiling_still_mirrors_the_drivers_own_default`.
    if os.environ.get("LOG_FOUNDRY_EXTRAS") != "1":
        pytest.importorskip("nats", reason="the `nats` extra is not installed")
    from nats.aio.client import DEFAULT_MAX_OUTSTANDING_PINGS, DEFAULT_PING_INTERVAL

    assert PROBE_FLUSH_TIMEOUT > DEFAULT_PING_INTERVAL * (DEFAULT_MAX_OUTSTANDING_PINGS + 1)
    source = pathlib.Path(nats_sink_module.__file__).read_text(encoding="utf-8")
    docstring = source.split("PROBE_FLUSH_TIMEOUT = ")[1].split('"""')[1]
    assert "ping_interval" in docstring and "max_outstanding_pings" in docstring


class FlushlessFakeNATS:
    """A sink-connected client that offers no ``flush``: published to, never probed."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.published: list[bytes] = []
        self.is_connected = True

    async def publish(self, subject: str, payload: bytes) -> None:
        self.calls.append("publish")
        self.published.append(payload)

    async def close(self) -> None:
        self.calls.append("close")


def test_a_sink_connected_client_without_flush_is_not_probed(monkeypatch) -> None:
    # FR-002 AC-10, first clause.
    sink, fake, _ = _build(monkeypatch, factory=FlushlessFakeNATS)
    sink.emit([{"n": 1}, {"n": 2}])
    sink.close()
    assert fake.calls == ["publish", "publish", "close"]


def test_a_disconnected_client_is_refused_before_any_ping(monkeypatch) -> None:
    # FR-002 AC-10, second clause.
    sink, fake, _ = _build(monkeypatch)
    fake.is_connected = False
    with pytest.raises(SinkDeliveryError, match="disconnected"):
        sink.emit([{"n": 1}])
    sink.close()
    assert "flush" not in fake.calls[:-1] and "publish" not in fake.calls


def test_an_empty_batch_sends_no_ping(monkeypatch) -> None:
    # FR-002 AC-10, third clause.
    sink, fake, _ = _build(monkeypatch)
    sink.emit([])
    assert fake.calls == []
    sink.close()


class DisconnectsInsideFlushNATS(LiveFakeNATS):
    """``flush`` flips the flag and returns without a PONG: the reconnecting fallback (M9)."""

    async def flush(self, timeout: float = 10) -> None:  # noqa: ASYNC109
        self._record("flush")
        self.flush_timeouts.append(timeout)
        self.is_connected = False


class AnswersThenDisconnectsNATS(LiveFakeNATS):
    """``flush`` is answered by a real PONG and the client is disconnected before it returns."""

    async def flush(self, timeout: float = 10) -> None:  # noqa: ASYNC109
        await super().flush(timeout)
        self.is_connected = False


class ReconnectsInsideFlushNATS(LiveFakeNATS):
    """``flush`` returns at once with the reconnect counter moved: a fresh queue, no PONG."""

    async def flush(self, timeout: float = 10) -> None:  # noqa: ASYNC109
        self._record("flush")
        self.flush_timeouts.append(timeout)
        self.stats["reconnects"] += 1


@pytest.mark.parametrize(
    "factory",
    [DisconnectsInsideFlushNATS, AnswersThenDisconnectsNATS, ReconnectsInsideFlushNATS],
    ids=["disconnected", "answers-then-disconnects", "reconnected"],
)
def test_a_flush_that_returns_without_a_pong_is_not_an_answer(monkeypatch, factory) -> None:
    # FR-002 AC-12: the three answer conditions, each flipped INSIDE the fake's flush (plan
    # finding M-5), so nothing but the condition itself can refuse the emit.
    sink, fake, _ = _build(monkeypatch, factory=factory, publish_timeout=0.3)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"n": 1}])
    with contextlib.suppress(TimeoutError):
        sink.close()
    assert "publish" not in fake.calls
    assert fake.calls.count("flush") >= 1


def test_the_latch_refuses_at_once_while_the_lock_is_held(monkeypatch) -> None:
    # FR-002 AC-13: the latch is read before the lock, so a holder of the lock cannot delay it.
    sink, fake, _ = _build(monkeypatch, factory=StalledFakeNATS, publish_timeout=0.2)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"n": 1}])
    holding = threading.Event()
    release = threading.Event()

    def hold() -> None:
        with sink._lock:
            holding.set()
            release.wait(LOCK_HOLD)

    holder = threading.Thread(target=hold)
    holder.start()
    assert holding.wait(2.0)
    try:
        timings = [_timed(lambda: sink.emit([{"n": 2}])), _timed(sink.flush)]
    finally:
        release.set()
        holder.join(5.0)
    with contextlib.suppress(TimeoutError):
        sink.close()

    for elapsed, error in timings:
        assert isinstance(error, SinkDeliveryError) and "PING" in str(error)
        assert elapsed < QUEUE_CEILING, f"the latch must refuse without queueing: {elapsed:.3f}s"
    assert fake.calls.count("flush") == 1


def test_a_healthy_call_after_a_cleared_stall_does_not_wait(monkeypatch) -> None:
    """FR-002 AC-14, first half, the timing: once a PONG clears the latch, a call is not held.

    Split from the test below so only this one pays a whole :data:`AT_ONCE_BUDGET` for its stall:
    the call it times must not wait, and a defect that made it wait would spend that budget.
    """
    sink, fake, _ = _build(monkeypatch, factory=StalledFakeNATS, publish_timeout=AT_ONCE_BUDGET)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"n": 1}])
    _pong(sink, fake)
    fake.answering = True
    sink.emit([{"n": 2}])
    elapsed, error = _timed(lambda: sink.emit([{"n": 3}]))
    sink.close()

    assert error is None, error
    assert elapsed < AT_ONCE_CEILING, (
        f"a healthy call after the stall must not wait: {elapsed:.3f}s"
    )
    assert fake.published == [b'{"n": 2}', b'{"n": 3}']


def test_the_latch_clears_on_a_pong_and_on_a_reconnect(monkeypatch) -> None:
    # FR-002 AC-14, first half; the healthy call's timing is the test above's.
    sink, fake, _ = _build(monkeypatch, factory=StalledFakeNATS, publish_timeout=0.2)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"n": 1}])
    _pong(sink, fake)
    fake.answering = True
    sink.emit([{"n": 2}])
    sink.emit([{"n": 3}])

    fake.answering = False
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"n": 4}])
    assert sink._latched()
    flushes_before = fake.calls.count("flush")
    _on_loop(sink, _run_sync(fake.reconnect))
    assert not sink._latched(), "a moved reconnect counter clears the latch"
    elapsed, error = _timed(lambda: sink.emit([{"n": 5}]))
    with contextlib.suppress(TimeoutError):
        sink.close()

    assert isinstance(error, SinkDeliveryError) and "PONG" in str(error)
    assert elapsed >= 0.2, "after a reconnect the call waits on a fresh PING, not the latch"
    assert fake.calls.count("flush") == flushes_before + 1
    assert fake.published == [b'{"n": 2}', b'{"n": 3}']


def test_a_reused_probe_wait_is_capped_by_the_probes_age(monkeypatch) -> None:
    # FR-002 AC-14, second half: a second caller joining a pending probe waits only what is left
    # of `publish_timeout` measured from the PING, not a full budget of its own. Timed from the
    # first caller's start: real, both are refused at about 1.0 s; a mutant giving the second
    # caller its own full budget refuses it at about 1.4 s.
    sink, fake, _ = _build(monkeypatch, factory=StalledFakeNATS, publish_timeout=1.0)
    results: dict[str, tuple[float, BaseException | None]] = {}
    started = time.monotonic()

    def first() -> None:
        results["first"] = _timed(lambda: sink.emit([{"n": 1}]))

    def second() -> None:
        time.sleep(0.4)
        results["second"] = _timed(lambda: sink.emit([{"n": 2}]))
        results["second_end"] = (time.monotonic() - started, None)

    threads = [threading.Thread(target=first), threading.Thread(target=second)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(5.0)
    with contextlib.suppress(TimeoutError):
        sink.close()

    assert isinstance(results["first"][1], SinkDeliveryError)
    assert isinstance(results["second"][1], SinkDeliveryError)
    assert results["second"][0] < 1.0, "the second caller must not wait a whole budget"
    assert results["second_end"][0] < 1.0 + 0.25, f"{results['second_end'][0]:.2f}s from the PING"
    assert fake.calls.count("flush") == 1


def test_an_injected_client_is_never_probed(monkeypatch) -> None:
    # FR-002 AC-15 (and FR-003 AC-11's no-confirmation half): the sink cannot drive a client it
    # did not connect on its own loop (M10), so an injected one keeps today's flag guard only.
    client = LiveFakeNATS()
    sink = NATSSink("logs", client=client)
    sink.emit([{"n": 1}, {"n": 2}])
    assert "flush" not in client.calls
    assert client.calls.count("publish") == 2
    client.is_connected = False
    with pytest.raises(SinkDeliveryError, match="disconnected"):
        sink.emit([{"n": 3}])
    sink.close()
    assert "flush" not in client.calls


# --- FR-003: a core batch is confirmed, or counted ---------------------------------------------


class StallsAfterFirstPublishNATS(LiveFakeNATS):
    """Answers the pre-publish PING, then the server pauses: no later PING is ever answered.

    The one batch that straddles the onset of a stall, which is the case FR-003 is about.
    """

    async def _enter_publish(self, payload: bytes) -> None:
        self.answering = False
        await super()._enter_publish(payload)


def test_a_core_batch_is_confirmed_by_a_second_ping_after_the_last_publish(monkeypatch) -> None:
    # FR-003 AC-1.
    sink, fake, _ = _build(monkeypatch)
    sink.emit([{"n": 1}, {"n": 2}, {"n": 3}])
    sink.close()
    assert fake.calls[:5] == ["flush", "publish", "publish", "publish", "flush"], fake.calls
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


def test_an_unconfirmed_core_batch_is_counted_and_announced_once(monkeypatch, capsys) -> None:
    # FR-003 AC-2.
    sink, _, _ = _build(monkeypatch, factory=StallsAfterFirstPublishNATS, publish_timeout=0.3)
    elapsed, error = _timed(lambda: sink.emit([{"n": 1}, {"n": 2}, {"n": 3}]))
    losses = sink.losses()
    with contextlib.suppress(TimeoutError):
        sink.close()
    err = capsys.readouterr().err

    assert error is None, "an unconfirmed batch returns, because the bytes may yet land"
    assert elapsed <= 0.3 + MARGIN
    assert losses == SinkLosses(dropped=0, failed=3)
    lines = [line for line in err.splitlines() if "lost" in line]
    assert len(lines) == 1, err
    assert "lost 3 event(s)" in lines[0] and "unconfirmed" in lines[0] and "PONG" in lines[0]


SPENT_BUDGET = 1.0
"""The ``publish_timeout`` of the spent-deadline test; its honest emit returns at about this."""

SPENT_CEILING = SPENT_BUDGET + min(SPENT_BUDGET, CALL_SLACK) / 2
"""The ceiling on an emit whose deadline is spent before its confirmation: 1.5 s.

The honest emit returns at the deadline, :data:`SPENT_BUDGET`. A defect that gave the
confirmation PING a fresh budget would wait a second one, cut short by the backstop at
``SPENT_BUDGET + CALL_SLACK``, so it returns at ``SPENT_BUDGET + min(SPENT_BUDGET, CALL_SLACK)``.
The ceiling sits midway, 0.5 s clear of each.
"""


class SlowStallingNATS(StallsAfterFirstPublishNATS):
    """Each publish costs real loop time, so the deadline is spent before the confirmation.

    Three quarters of :data:`SPENT_BUDGET` per publish: the first completes inside the budget and
    the second is abandoned at the deadline.
    """

    def __init__(self) -> None:
        super().__init__()
        self.publish_delay = 0.75 * SPENT_BUDGET


def test_a_spent_deadline_counts_without_waiting_and_leaves_the_probe_for_reuse(
    monkeypatch, capsys
) -> None:
    # FR-003 AC-3. Two publishes at 0.75 s each against a 1.0 s budget: the second is abandoned at
    # the deadline (plan finding M-2) and the confirmation has nothing left to wait, so the emit
    # returns at about the deadline with both counted; the PING it sent is still pending for the
    # next call, which reuses it rather than sending another. The timing is asserted before the
    # outcome because a confirmation waited for a fresh budget reaches the backstop, which closes
    # the sink and raises, and the wait is the defect this test names.
    sink, fake, _ = _build(monkeypatch, factory=SlowStallingNATS, publish_timeout=SPENT_BUDGET)
    elapsed, error = _timed(lambda: sink.emit([{"n": 1}, {"n": 2}, {"n": 3}]))
    assert elapsed < SPENT_CEILING, f"the spent deadline must not wait on the PING: {elapsed:.2f}s"
    assert error is None
    assert sink.losses() == SinkLosses(dropped=0, failed=3)
    assert sink._probe is not None and not sink._probe[0].done(), "the probe is left for reuse"
    flushes = fake.calls.count("flush")
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"n": 4}])
    with contextlib.suppress(TimeoutError):
        sink.close()

    assert fake.calls.count("flush") == flushes == 2, "the next call reused the pending PING"
    err = capsys.readouterr().err
    assert "unconfirmed" in err and "publish_timeout" in err


class SecondPublishRaisesNATS(StallsAfterFirstPublishNATS):
    """The second publish raises; the first and third land unconfirmed."""

    async def _enter_publish(self, payload: bytes) -> None:
        if self.calls.count("publish") == 1:
            self._record("publish")
            raise RuntimeError("no responders")
        await super()._enter_publish(payload)


def test_a_raised_publish_and_two_unconfirmed_events_count_three_not_four(monkeypatch) -> None:
    # FR-003 AC-4.
    sink, fake, _ = _build(monkeypatch, factory=SecondPublishRaisesNATS, publish_timeout=0.3)
    sink.emit([{"n": 1}, {"n": 2}, {"n": 3}])
    losses = sink.losses()
    with contextlib.suppress(TimeoutError):
        sink.close()
    assert losses == SinkLosses(dropped=0, failed=3)
    assert len(fake.published) == 2


def test_a_batch_where_every_publish_raised_raises_without_a_second_ping(monkeypatch) -> None:
    # FR-003 AC-5.
    sink, fake, _ = _build(monkeypatch)
    fake.publish_raises = True
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"n": 1}, {"n": 2}])
    losses = sink.losses()
    flushes = fake.calls.count("flush")
    sink.close()
    assert flushes == 1, "the pre-publish PING and no confirmation of a batch nothing left"
    assert fake.calls[:3] == ["flush", "publish", "publish"]
    assert losses == SinkLosses(dropped=0, failed=2)


def test_jetstream_mode_probes_once_only(monkeypatch) -> None:
    # FR-003 AC-6: each JetStream publish awaited its ack, so there is nothing to confirm.
    sink, fake, _ = _build(monkeypatch, jetstream=True)
    sink.emit([{"n": 1}, {"n": 2}, {"n": 3}])
    assert fake.calls.count("flush") == 1
    sink.close()


def test_the_emit_after_an_unconfirmed_batch_is_refused_while_the_stall_continues(
    monkeypatch,
) -> None:
    # FR-003 AC-7: the sustained stall is reported, not absorbed.
    sink, fake, _ = _build(monkeypatch, factory=StallsAfterFirstPublishNATS, publish_timeout=0.2)
    sink.emit([{"n": 1}])
    with pytest.raises(SinkDeliveryError, match=r"PING|PONG"):
        sink.emit([{"n": 2}])
    with contextlib.suppress(TimeoutError):
        sink.close()
    assert sink.losses() == SinkLosses(dropped=0, failed=1)
    assert fake.published == [b'{"n": 1}']


class DisconnectsAfterPublishNATS(LiveFakeNATS):
    """Answers the pre-publish PING, then the client reports itself disconnected."""

    async def _enter_publish(self, payload: bytes) -> None:
        await super()._enter_publish(payload)
        self.is_connected = False


class ConfirmReturnsWhileDisconnectedNATS(LiveFakeNATS):
    """The second ``flush`` flips the flag and returns at once, the reconnecting fallback (M9)."""

    async def flush(self, timeout: float = 10) -> None:  # noqa: ASYNC109
        if self.calls.count("flush") == 0:
            await super().flush(timeout)
            return
        self._record("flush")
        self.flush_timeouts.append(timeout)
        self.is_connected = False


def test_a_client_that_disconnects_after_the_publishes_is_counted_without_a_wait(
    monkeypatch,
) -> None:
    # FR-003 AC-10 (plan finding M-6): no second PING is sent to a client that reports itself
    # disconnected, since the driver's flush would return without one; and a confirmation flush
    # that does return at once while disconnected is not an answer.
    sink, fake, _ = _build(
        monkeypatch, factory=DisconnectsAfterPublishNATS, publish_timeout=AT_ONCE_BUDGET
    )
    elapsed, error = _timed(lambda: sink.emit([{"n": 1}, {"n": 2}]))
    losses = sink.losses()
    sink.close()
    assert elapsed < AT_ONCE_CEILING, f"{elapsed:.2f}s"
    assert error is None, error
    assert losses == SinkLosses(dropped=0, failed=2)
    assert fake.calls.count("flush") == 1, fake.calls
    assert fake._pongs == []

    sink, fake, _ = _build(
        monkeypatch, factory=ConfirmReturnsWhileDisconnectedNATS, publish_timeout=0.3
    )
    sink.emit([{"n": 1}])
    losses = sink.losses()
    sink.close()
    assert losses == SinkLosses(dropped=0, failed=1)


def test_an_injected_core_batch_has_no_second_round_trip_and_no_count() -> None:
    # FR-003 AC-11.
    client = LiveFakeNATS()
    client.answering = False
    sink = NATSSink("logs", client=client, publish_timeout=AT_ONCE_BUDGET)
    elapsed, error = _timed(lambda: sink.emit([{"n": 1}, {"n": 2}]))
    sink.close()
    assert elapsed < AT_ONCE_CEILING, f"{elapsed:.2f}s"
    assert error is None, error
    assert "flush" not in client.calls
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


def test_a_core_publish_that_blocks_is_abandoned_inside_the_deadline_and_the_sink_stays_open(
    monkeypatch, capsys
) -> None:
    """FR-003 AC-13 (plan finding M-2): the driver's unbounded force-flush cannot reach the backstop.

    ``Client.publish`` force-flushes once 2 MiB is pending, under a flush timeout that defaults to
    ``None``, so against a peer that stopped reading the await never returns. Bounded by the sink,
    the emit returns at its deadline with the blocked event counted as unconfirmed and the rest
    unattempted, the loop thread still alive and the sink still open; unbounded, the backstop
    would fire a second later and close the sink for good.
    """
    sink, fake, _ = _build(monkeypatch, publish_timeout=1.0)
    gate = asyncio.Event()
    fake.publish_gate = gate
    batch = [{"blob": "x" * (1024 * 1024)} for _ in range(3)]
    elapsed, error = _timed(lambda: sink.emit(batch))
    losses = sink.losses()
    alive = sink._thread.is_alive() and not sink._closed
    abandoned = len(sink._abandoned)
    _open(sink, gate)
    sink.close()
    err = capsys.readouterr().err

    assert error is None, error
    assert 1.0 <= elapsed <= 1.0 + MARGIN, f"{elapsed:.2f}s"
    assert losses == SinkLosses(dropped=0, failed=3)
    assert alive, "the backstop must not have fired"
    assert abandoned == 1
    assert "lost 1 event(s)" in err and "unconfirmed" in err
    assert "lost 2 event(s)" in err and "publish_timeout" in err
    assert sink._abandoned == set(), "the abandoned publish completed once the gate opened"


def test_deferral_gates_need_no_allowlist_entry_for_nats() -> None:
    # FR-003 AC-9: `nats.py` enters no `Deferral` and writes no `dropped`-named store, so the
    # tree-wide gates in `tests/test_deferral.py` pass unmodified; asserted here on the source so
    # the premise is checked rather than assumed.
    source = pathlib.Path(nats_sink_module.__file__).read_text(encoding="utf-8")
    assert "Deferral" not in source
    assert "self.dropped" not in source


# --- FR-004: close() and flush() are bounded by the sink, not by the driver's drain ------------


def test_close_never_drains_and_is_bounded_where_drain_is_not(monkeypatch) -> None:
    """FR-004 AC-1, both halves against the same fake shape.

    The sink's ``close()`` under a stall: a JetStream publish first, so the client holds its
    response-inbox subscription (M7's precondition), then the server stops answering. ``close()``
    raises ``TimeoutError`` inside ``D + LOOP_JOIN_TIMEOUT`` and never calls ``drain``. The
    driver's ``drain()`` under the same ``wait_for(D)`` on a throwaway loop runs its swallowed
    cancel and then its ``finally`` flush to that flush's own timeout, compressed here from the
    driver's 10 s to 2 s so the test costs 3 s rather than 11; M7 measured the real driver at
    12.00 s against a 2 s ``wait_for``.
    """
    sink, fake, _ = _build(monkeypatch, jetstream=True, drain_timeout=1.0)
    sink.emit([{"n": 1}])
    assert fake.subs == 1
    fake.answering = False
    began = time.monotonic()
    with pytest.raises(TimeoutError):
        sink.close()
    elapsed = time.monotonic() - began

    assert elapsed <= 1.0 + LOOP_JOIN_TIMEOUT + MARGIN, f"{elapsed:.2f}s"
    assert "drain" not in fake.calls and "close" in fake.calls
    assert not sink._thread.is_alive()

    stalled = LiveFakeNATS()
    stalled.subs = 1
    stalled.answering = False
    stalled.drain_flush_timeout = 2.0
    began = time.monotonic()
    with pytest.raises(FlushTimeoutError):
        asyncio.run(asyncio.wait_for(stalled.drain(), 1.0))
    drained = time.monotonic() - began
    assert drained >= 1.0 + 2.0 - 0.1, (
        f"drain's finally flush must run to its own bound: {drained:.2f}s"
    )
    assert stalled.calls == ["drain", "flush"], "the swallowed cancel let the finally flush run"


def test_close_raises_timeout_when_its_probe_is_unanswered(monkeypatch) -> None:
    # FR-004 AC-2.
    sink, fake, _ = _build(monkeypatch, factory=StalledFakeNATS)
    began = time.monotonic()
    with pytest.raises(TimeoutError, match="PING"):
        sink.close()
    elapsed = time.monotonic() - began

    assert elapsed <= DEFAULT_CLOSE_TIMEOUT + LOOP_JOIN_TIMEOUT + MARGIN, f"{elapsed:.2f}s"
    assert "close" in fake.calls
    assert not sink._thread.is_alive()


def test_a_client_close_that_never_returns_is_cancelled_at_the_deadline(monkeypatch) -> None:
    # FR-004 AC-3 (M8: `client.close()` still running at 8 s with 20 MB pending), and AC-15.
    #
    # The fake swallows its first cancellation as the driver's `_close` does, so the close task
    # ends inside the close sequence only if step two's cancel was DELIVERED before step three
    # cancelled again. Two `cancel()` calls made before the task next runs coalesce into one, so
    # a step two that dropped its cancel, or that cancelled without yielding, leaves the task
    # parked through step three's wait and ended only by the loop's stop; `ended_in_sequence`
    # reads the fake at the moment the close coroutine returns, which is what tells them apart.
    ended_in_sequence: list[bool] = []
    real_close_on_loop = NATSSink._close_on_loop

    async def spy(self: NATSSink) -> Any:
        outcome = await real_close_on_loop(self)
        ended_in_sequence.append(self._client.close_ended)
        return outcome

    monkeypatch.setattr(NATSSink, "_close_on_loop", spy)
    sink, fake, _ = _build(monkeypatch, drain_timeout=0.5)
    fake.close_gate = asyncio.Event()
    began = time.monotonic()
    sink.close()
    elapsed = time.monotonic() - began

    assert elapsed <= 0.5 + LOOP_JOIN_TIMEOUT + MARGIN, f"{elapsed:.2f}s"
    assert "close" in fake.calls
    assert not sink._thread.is_alive()
    assert sink._loop.is_closed()
    assert fake._cancels_swallowed == 1
    assert ended_in_sequence == [True], "step three's cancel merged into step two's"
    assert asyncio.all_tasks(sink._loop) == set(), "the parked close must not outlive the loop"


def test_a_failing_client_close_is_absorbed_and_the_loop_still_stops(monkeypatch, capsys) -> None:
    # FR-004 AC-4.
    sink, fake, _ = _build(monkeypatch)
    fake.close_raises = TypeError
    sink.close()
    err = capsys.readouterr().err

    assert not sink._thread.is_alive() and sink._loop.is_closed()
    lines = [line for line in err.splitlines() if "absorbed" in line]
    assert len(lines) == 1, err
    assert "TypeError" in lines[0] and "closing the NATS client" in lines[0]
    assert "NoneType" not in lines[0], "only the type is written, never the message"


@pytest.mark.parametrize("value", [0.3, None, 0, -1, float("inf"), float("nan")])
def test_drain_timeout_sets_the_close_deadline_and_is_still_forwarded(monkeypatch, value) -> None:
    # FR-004 AC-5.
    kwargs = {} if value is None else {"drain_timeout": value}
    sink, _, module = _build(monkeypatch, factory=StalledFakeNATS, **kwargs)
    _, connect_kwargs = module.calls[0]
    if value is None:
        assert "drain_timeout" not in connect_kwargs
    else:
        assert connect_kwargs["drain_timeout"] is value, "the raw value is forwarded unchanged"
    expected = 0.3 if value == 0.3 else DEFAULT_CLOSE_TIMEOUT
    assert sink._drain_timeout == expected

    began = time.monotonic()
    with pytest.raises(TimeoutError):
        sink.close()
    elapsed = time.monotonic() - began
    assert expected <= elapsed <= expected + LOOP_JOIN_TIMEOUT + MARGIN, f"{elapsed:.2f}s"


def test_the_close_bound_fits_the_closer_grace() -> None:
    # FR-004 AC-6.
    from log_foundry import _lifecycle

    assert DEFAULT_CLOSE_TIMEOUT + LOOP_JOIN_TIMEOUT <= _lifecycle.DEFAULT_CLOSER_GRACE


def test_flush_goes_through_the_probe(monkeypatch) -> None:
    # FR-004 AC-8, three fakes.
    sink, fake, _ = _build(monkeypatch)
    sink.flush()
    assert fake.calls == ["flush"] and fake.flush_timeouts == [PROBE_FLUSH_TIMEOUT]
    sink.close()

    sink, fake, _ = _build(monkeypatch, factory=StalledFakeNATS, publish_timeout=0.3)
    elapsed, error = _timed(sink.flush)
    losses = sink.losses()
    with contextlib.suppress(TimeoutError):
        sink.close()
    assert isinstance(error, SinkDeliveryError) and "PONG" in str(error)
    assert 0.3 <= elapsed <= 0.3 + CALL_SLACK + MARGIN, f"{elapsed:.2f}s"
    assert losses == SinkLosses(dropped=0, failed=0)

    sink, fake, _ = _build(monkeypatch)
    fake.is_connected = False
    with pytest.raises(SinkDeliveryError, match="disconnected"):
        sink.flush()
    assert "flush" not in fake.calls
    sink.close()


def test_a_late_pong_after_a_stalled_flush_leaves_the_reader_alive(monkeypatch) -> None:
    # FR-004 AC-9, the M3 regression for `flush()`.
    sink, fake, _ = _build(monkeypatch, factory=StalledFakeNATS, publish_timeout=0.2)
    with pytest.raises(SinkDeliveryError):
        sink.flush()
    _pong(sink, fake)
    fake.answering = True
    sink.flush()
    fake.answering = False
    second = _timed(sink.flush)
    with contextlib.suppress(TimeoutError):
        sink.close()

    assert fake.reader_dead is False
    assert isinstance(second[1], SinkDeliveryError), "a second stalled flush must be refused"


class CloselessFakeNATS:
    """A client with ``publish`` and ``flush`` but no ``close``."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.is_connected = True

    async def publish(self, subject: str, payload: bytes) -> None:
        self.calls.append("publish")

    async def flush(self, timeout: float = 10) -> None:  # noqa: ASYNC109
        self.calls.append("flush")


def test_an_injected_client_is_closed_under_the_deadline_and_never_flushed_or_drained() -> None:
    # FR-004 AC-10, first and third clauses.
    client = LiveFakeNATS()
    sink = NATSSink("logs", client=client)
    sink.flush()
    assert client.calls == ["flush"] and client.flush_timeouts == [10], "today's bare driver call"
    sink.close()
    assert client.calls == ["flush", "close"]

    client = LiveFakeNATS()
    client.close_gate = asyncio.Event()
    sink = NATSSink("logs", client=client)
    began = time.monotonic()
    sink.close()
    assert time.monotonic() - began <= DEFAULT_CLOSE_TIMEOUT + LOOP_JOIN_TIMEOUT + MARGIN
    assert client.calls == ["close"]


def test_a_client_without_close_does_not_make_close_raise(monkeypatch) -> None:
    # FR-004 AC-10, second clause, for a sink-connected client and an injected one.
    sink, fake, _ = _build(monkeypatch, factory=CloselessFakeNATS)
    sink.emit([{"n": 1}])
    sink.close()
    assert "close" not in fake.calls and not sink._thread.is_alive()

    sink = NATSSink("logs", client=CloselessFakeNATS())
    sink.close()
    assert not sink._thread.is_alive()


@pytest.mark.parametrize("reason", ["disconnected", "latched"])
def test_a_skipped_close_probe_raises_nothing(monkeypatch, reason: str) -> None:
    # FR-004 AC-12. Told apart from AC-2's `TimeoutError` by the fake's flush count: AC-2's probe
    # ran (one flush), these send none beyond what the latched emit already sent.
    sink, fake, _ = _build(monkeypatch, factory=StalledFakeNATS, publish_timeout=0.2)
    if reason == "disconnected":
        fake.is_connected = False
        flushes_before_close = 0
    else:
        with pytest.raises(SinkDeliveryError):
            sink.emit([{"n": 1}])
        assert sink._latched()
        flushes_before_close = 1
    began = time.monotonic()
    sink.close()
    elapsed = time.monotonic() - began

    assert fake.calls.count("flush") == flushes_before_close, "a skipped step one sends no PING"
    assert "close" in fake.calls
    assert not sink._thread.is_alive() and sink._loop.is_closed()
    assert elapsed <= DEFAULT_CLOSE_TIMEOUT + LOOP_JOIN_TIMEOUT + MARGIN


# --- Review finding 1: a PING must not overtake the driver's pending buffer -------------------


class ParkedFlusherNATS(LiveFakeNATS):
    """The transport is past its high-water mark: publishes queue, the flusher never runs."""

    def __init__(self) -> None:
        super().__init__()
        self.flusher_parked = True


class SlowFlusherNATS(LiveFakeNATS):
    """The flusher is parked until 0.2 s after the batch's first publish, then drains it.

    The timer starts at that publish, inside the emit the test times, and not when the fake is
    built. The sink connects in its constructor, so a timer started in ``__init__`` was already
    running before the test's clock started, and the test's ``0.2 <= elapsed`` read as low as
    0.1998 s on CI (Release run 42509770284 attempt 1, on ``458d79e6``). asyncio never fires a
    handle early by more than its clock resolution, so a window that opens after the clock does
    is a floor load cannot break. :class:`conftest.DelayArmedOnUse` holds the delay unarmed until
    that publish, and ``tests/test_timing_floors.py`` refuses a fake that arms one in ``__init__``.
    """

    def __init__(self) -> None:
        super().__init__()
        self.flusher_parked = True
        self.unpark = DelayArmedOnUse(0.2, self._unpark)

    async def _enter_publish(self, payload: bytes) -> None:
        self.unpark.arm()
        await super()._enter_publish(payload)

    def _unpark(self) -> None:
        self.flusher_parked = False
        self._run_flusher()


def test_a_ping_is_not_sent_while_publishes_are_still_queued_behind_the_flusher(
    monkeypatch, capsys
) -> None:
    """Review finding 1, red against 810b4c4.

    ``_send_ping`` writes to the transport directly while publishes wait in ``_pending`` for the
    flusher; with the flusher parked the PING overtakes them and the server answers it having
    processed none of the batch. Reproduced on CPython 3.12.12 against a raw-socket server: the
    confirming PONG arrived after 133 of 400 publishes. At 810b4c4 this fake's PING is answered
    and the batch reads confirmed with ``failed=0``; now the sink waits for the counter to read
    zero and counts the batch when the deadline passes first.

    Since frame-2 finding 2 the sink then leaves a liveness probe outstanding for the latch to
    age, and that PING does go out over the undrained buffer: the fake answers it, and the batch
    must still read unconfirmed, because a PING sent before the buffer drained is never a
    confirmation.
    """
    sink, fake, _ = _build(monkeypatch, factory=ParkedFlusherNATS, publish_timeout=0.3)
    elapsed, error = _timed(lambda: sink.emit([{"n": 1}, {"n": 2}, {"n": 3}]))
    losses = sink.losses()
    assert fake.pending_data_size > 0, "the fake's flusher must still be parked"
    with contextlib.suppress(TimeoutError):
        sink.close()

    assert error is None
    assert 0.3 <= elapsed <= 0.3 + MARGIN, f"{elapsed:.2f}s"
    assert losses == SinkLosses(dropped=0, failed=3), "the answered liveness PING confirmed it"
    assert fake.pubs_at_ping == [0, 0], "the probe, then one liveness PING over the full buffer"
    assert fake.timed_out == 0 and not fake._pongs, "and the server answered that PING"
    assert "unconfirmed" in capsys.readouterr().err


def test_the_confirmation_waits_for_the_flusher_and_then_confirms(monkeypatch) -> None:
    # The other half of finding 1: a flusher that drains inside the deadline is waited for, the
    # PING goes out after the last publish was written, and the batch is confirmed. A wait that
    # merely counted every parked batch would fail here.
    sink, fake, _ = _build(monkeypatch, factory=SlowFlusherNATS, publish_timeout=1.0)
    elapsed, error = _timed(lambda: sink.emit([{"n": 1}, {"n": 2}]))
    losses = sink.losses()
    sink.close()

    assert error is None and 0.2 <= elapsed < 1.0, f"{elapsed:.2f}s"
    assert losses == SinkLosses(dropped=0, failed=0)
    assert fake.pubs_at_ping[1] == 2, "the confirming PING must follow both written publishes"


class LateSecondPongNATS(LiveFakeNATS):
    """Answers every PING at once except the second, whose PONG arrives 0.8 s after it was sent."""

    async def flush(self, timeout: float = 10) -> None:  # noqa: ASYNC109
        if self.calls.count("flush") != 1:
            await super().flush(timeout)
            return
        self.answering = False
        asyncio.get_running_loop().call_later(0.8, self.deliver_pong)
        try:
            await super().flush(timeout)
        finally:
            self.answering = True


def test_a_reused_probes_ping_bytes_do_not_make_a_healthy_batch_unconfirmed(monkeypatch) -> None:
    """AC-14: an outstanding probe reused by the next emit leaves that emit's batch confirmed.

    ``_send_ping`` adds the PING's bytes to ``pending_data_size`` without queueing anything, and
    ``_flush_pending`` kicks the flusher in the same call, whose next run resets the counter
    whatever the queue held (``Client._flusher``). So the bump outlives a flusher run only while
    the flusher is parked in ``transport.drain()``, a peer that stopped reading, which is the batch
    that must be counted. Here the first emit's confirming PING (sent 0.4 s in, after a slow
    publish) is answered 0.8 s later, past that emit's 1.0 s deadline, so the first batch is
    counted and its PING is still outstanding when the second emit starts; the second reuses it,
    its PONG lands at about 1.2 s inside the reuse window (which ends at 1.4 s), and the second
    batch's own confirmation must then go out and be answered with nothing left over.
    """
    sink, fake, _ = _build(monkeypatch, factory=LateSecondPongNATS, publish_timeout=1.0)
    fake.publish_delay = 0.4
    sink.emit([{"n": 1}])
    first = sink.losses()
    fake.publish_delay = 0.0
    elapsed, error = _timed(lambda: sink.emit([{"n": 2}]))
    losses = sink.losses()
    pings = list(fake.pubs_at_ping)
    sink.close()

    assert first == SinkLosses(dropped=0, failed=1), "the late PONG must miss the first deadline"
    assert error is None and elapsed < 1.0, f"{elapsed:.2f}s"
    assert losses == first, "the batch sent after the reused probe was answered is confirmed"
    assert pings == [0, 1, 2], "probe, a confirmation the next emit reused, then its own"


def test_close_and_flush_wait_for_the_flusher_before_their_ping(monkeypatch) -> None:
    # FR-004's step one and `flush()` take the same wait: with the flusher parked, both report
    # the deadline rather than a false confirmation. Each of `emit` and `flush` leaves one
    # liveness PING for the latch when its wait fails (frame-2 finding 2), and the fake answers
    # both, so a sink that let such a PING confirm the buffer would not raise here. `close()`
    # arms none, since nothing follows it.
    #
    # Each call waits for the previous one's liveness PING to be answered first. That PING is
    # aged from the start of the call whose budget it spent (frame-3 finding 2), so a call made
    # before its PONG lands is refused by the latch without reaching the drained wait this test
    # is about;
    # `test_an_undrained_buffer_leaves_a_probe_for_the_latch_to_age` covers that refusal.
    sink, fake, _ = _build(monkeypatch, factory=ParkedFlusherNATS, publish_timeout=0.3)
    sink.emit([{"n": 1}])
    assert fake.calls.count("flush") == 2
    assert _wait_until(lambda: sink._probe is not None and sink._probe[0].done())
    with pytest.raises(SinkDeliveryError, match="PONG"):
        sink.flush()
    assert fake.calls.count("flush") == 3
    assert _wait_until(lambda: sink._probe is not None and sink._probe[0].done())
    with pytest.raises(TimeoutError):
        sink.close()
    assert fake.calls.count("flush") == 3, "close's step one sent no PING over a parked flusher"
    assert fake.pubs_at_ping == [0, 0, 0]


class CounterlessFakeNATS(LiveFakeNATS):
    """A client without ``pending_data_size``: treated as drained, confirmed as before."""

    def _add_pending(self, size: int) -> None:
        pass

    @property
    def pending_data_size(self) -> int:
        raise AttributeError("pending_data_size")

    @pending_data_size.setter
    def pending_data_size(self, value: int) -> None:
        pass


class HostileCounterNATS(CounterlessFakeNATS):
    """A client whose ``pending_data_size`` raises something ``getattr``'s default cannot absorb."""

    @property
    def pending_data_size(self) -> int:
        raise RuntimeError("the transport is gone")

    @pending_data_size.setter
    def pending_data_size(self, value: int) -> None:
        pass


@pytest.mark.parametrize(
    "factory", [CounterlessFakeNATS, HostileCounterNATS], ids=["absent", "raising"]
)
def test_a_client_without_the_counter_is_treated_as_drained(monkeypatch, factory) -> None:
    # An absent attribute takes `getattr`'s default and a raising one takes the `except`: two
    # separate branches, so each has its own case. Either way the sink invents no loss.
    sink, fake, _ = _build(monkeypatch, factory=factory, publish_timeout=0.5)
    sink.emit([{"n": 1}])
    losses = sink.losses()
    pings = fake.calls.count("flush")
    sink.close()
    assert losses == SinkLosses(dropped=0, failed=0)
    assert pings == 2, "the probe, then a confirmation sent at once rather than at the deadline"


# --- Review finding 2: an injected client's flush is bounded inside the coroutine -------------


class SlowInjectedNATS(LiveFakeNATS):
    """An injected client whose ``flush`` takes 2 s, longer than the sink's budget."""

    async def flush(self, timeout: float = 10) -> None:  # noqa: ASYNC109
        self._record("flush")
        await asyncio.sleep(2.0)
        self.calls.append("flush-returned")


def test_a_slow_injected_flush_is_refused_at_the_budget_and_the_sink_stays_open() -> None:
    # Red against 810b4c4: the injected flush reached the backstop at `publish_timeout +
    # CALL_SLACK` and closed the sink for good ("loop is wedged"); measured on a real client
    # connected on another loop, 2.01 s and every later emit refused.
    client = SlowInjectedNATS()
    sink = NATSSink("logs", client=client, publish_timeout=0.5)
    elapsed, error = _timed(sink.flush)
    abandoned = set(sink._abandoned)
    assert isinstance(error, SinkDeliveryError) and "wedged" not in str(error), error
    assert 0.5 <= elapsed < 0.5 + CALL_SLACK, f"{elapsed:.2f}s"
    assert not sink._closed and sink._thread.is_alive()
    sink.emit([{"n": 1}])
    assert client.published == [b'{"n": 1}']
    assert "flush-returned" not in client.calls, "the slow flush cannot have finished yet"
    if abandoned:
        _on_loop(sink, asyncio.wait(abandoned, timeout=3.0))
    assert "flush-returned" in client.calls, "the abandoned flush was cancelled, not left to run"
    sink.close()


# --- Review finding 3: a future already done when the bound expires returns its result --------


def test_a_future_done_at_the_bounds_expiry_is_returned_not_raised(monkeypatch) -> None:
    # The racing shape, forced: the future's bounded wait reports expiry although the coroutine
    # finished. `close()` must still mark the sink closed and stop the loop; a version that
    # re-raised escaped before either, so a second close() called the client again.
    real_submit = NATSSink._submit

    class ExpiredButDone:
        def __init__(self, inner: Any) -> None:
            self._inner = inner
            self.first = True

        def result(self, timeout: float | None = None) -> Any:
            if timeout is not None and self.first:
                self.first = False
                self._inner.result(timeout)
                raise TimeoutError("the bound expired on a finished future")
            return self._inner.result(timeout)

        def done(self) -> bool:
            return self._inner.done()

    monkeypatch.setattr(
        NATSSink, "_submit", lambda self, coro: ExpiredButDone(real_submit(self, coro))
    )
    sink, fake, _ = _build(monkeypatch)
    try:
        sink.close()
    except TimeoutError as err:
        pytest.fail(f"a close that finished was reported as an expired bound: {err}")
    assert sink._closed and not sink._thread.is_alive() and sink._loop.is_closed()
    sink.close()
    assert fake.calls.count("close") == 1


# --- Review finding 7: a JetStream publish that blocks is abandoned and counted ---------------


def test_a_blocking_jetstream_publish_is_abandoned_counted_and_the_sink_stays_open(
    monkeypatch, capsys
) -> None:
    """The force-flush fires on the cumulative pending size, so several JetStream publishes into
    a peer that stopped reading block as a core one does; the same per-publish task wait bounds
    them. At 810b4c4 the parked publish reached the backstop and closed the sink."""
    sink, fake, _ = _build(monkeypatch, jetstream=True, publish_timeout=0.5)
    gate = asyncio.Event()
    fake.publish_gate = gate
    elapsed, error = _timed(lambda: sink.emit([{"n": 1}, {"n": 2}, {"n": 3}]))
    losses = sink.losses()
    alive = sink._thread.is_alive() and not sink._closed
    _open(sink, gate)
    sink.close()
    err = capsys.readouterr().err

    assert error is None, error
    assert 0.5 <= elapsed <= 0.5 + MARGIN, f"{elapsed:.2f}s"
    assert alive, "the backstop must not have fired"
    assert losses == SinkLosses(dropped=0, failed=3)
    assert "lost 1 event(s)" in err and "JetStream publish abandoned" in err
    assert "lost 2 event(s)" in err and "publish_timeout" in err


# --- Frame-2 finding 1: an abandoned publish is counted on every twin, and latches ------------


class ForceFlushBlockedNATS(LiveFakeNATS):
    """A publish appends to the pending buffer, then blocks in the force-flush until the gate opens.

    ``Client._send_command`` appends the command and bumps ``pending_data_size`` before it
    force-flushes, so the bytes are already queued when the await stops returning; the base fake
    parks before it queues anything, which would let an owned client's confirmation PING be
    answered for a publish that never left.
    """

    async def _enter_publish(self, payload: bytes) -> None:
        self._record("publish")
        self.publish_entered.set()
        self._add_pending(len(payload))
        if self.publish_gate is not None:
            await self.publish_gate.wait()
        self.published.append(payload)
        self._kick_flusher()


@pytest.mark.parametrize("size", [1, 3])
def test_an_injected_core_publish_that_never_returns_is_counted_and_announced(
    capsys, size: int
) -> None:
    """Frame-2 finding 1: before this, ``failed`` stayed 0 for a batch of one, silently.

    An injected client has no confirmation round trip (M10), so nothing after the publish would
    ever judge the abandoned event; it is counted as unconfirmed where it is abandoned, exactly
    as the JetStream twin counts it, and the remainder as unattempted.
    """
    client = ForceFlushBlockedNATS()
    gate = asyncio.Event()
    client.publish_gate = gate
    sink = NATSSink("logs", client=client, publish_timeout=0.3)
    elapsed, error = _timed(lambda: sink.emit([{"n": i} for i in range(size)]))
    losses = sink.losses()
    _open(sink, gate)
    sink.close()
    err = capsys.readouterr().err

    assert error is None, error
    assert elapsed <= 0.3 + MARGIN, f"{elapsed:.2f}s"
    assert losses == SinkLosses(dropped=0, failed=size)
    assert "lost 1 event(s); NATSSink unconfirmed, publish abandoned" in err, err
    if size > 1:
        assert f"lost {size - 1} event(s)" in err and "publish_timeout" in err


def _wait_until(predicate: Callable[[], bool], within: float = 2.0) -> bool:
    end = time.monotonic() + within
    while time.monotonic() < end:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


@pytest.mark.parametrize("owned", [True, False], ids=["owned", "injected"])
def test_an_abandoned_publish_refuses_later_callers_at_once_until_it_finishes(
    monkeypatch, owned: bool
) -> None:
    """Frame-2 finding 1, the latch: a stall behind a blocked force-flush costs one caller.

    Callers already queued on the lock when the publish is abandoned are refused once they get
    it, without publishing (the check after the lock); a caller arriving while another thread
    holds the lock is refused without waiting for it (the check before the lock). When the
    abandoned publish finishes the latch clears and the next emit is accepted. Before this, p9
    measured eight such callers served one after another, 0.30 s to 2.41 s, every one of them
    publishing into the same blocked flush.
    """
    if owned:
        sink, fake, _ = _build(monkeypatch, factory=ForceFlushBlockedNATS, publish_timeout=0.3)
    else:
        fake = ForceFlushBlockedNATS()
        sink = NATSSink("logs", client=fake, publish_timeout=0.3)
    gate = asyncio.Event()
    fake.publish_gate = gate
    outcomes: dict[str, BaseException | None] = {}

    def call(tag: str, action: Callable[[], object]) -> None:
        outcomes[tag] = _timed(action)[1]

    first = threading.Thread(target=call, args=("first", lambda: sink.emit([{"n": 0}])))
    first.start()
    assert fake.publish_entered.wait(2.0)
    queued = [
        threading.Thread(target=call, args=(f"q{i}", lambda i=i: sink.emit([{"n": i + 1}])))
        for i in range(3)
    ]
    queued.append(threading.Thread(target=call, args=("flush", sink.flush)))
    for thread in queued:
        thread.start()
    for thread in [first, *queued]:
        thread.join(5.0)
    publishes = fake.calls.count("publish")

    holding = threading.Event()
    release = threading.Event()

    def hold() -> None:
        with sink._lock:
            holding.set()
            release.wait(LOCK_HOLD)

    holder = threading.Thread(target=hold)
    holder.start()
    assert holding.wait(2.0)
    try:
        late_elapsed, late_error = _timed(lambda: sink.emit([{"n": 9}]))
    finally:
        release.set()
        holder.join(5.0)

    _open(sink, gate)
    cleared = _wait_until(lambda: not sink._stuck)
    after_error = _timed(lambda: sink.emit([{"n": 10}]))[1]
    losses = sink.losses()
    sink.close()

    assert outcomes["first"] is None, outcomes["first"]
    for tag in ("q0", "q1", "q2", "flush"):
        error = outcomes[tag]
        assert isinstance(error, SinkDeliveryError) and "abandoned" in str(error), (tag, error)
    assert publishes == 1, "a queued caller published into the blocked flush"
    assert isinstance(late_error, SinkDeliveryError) and "abandoned" in str(late_error)
    assert late_elapsed < QUEUE_CEILING, (
        f"the latch must refuse without queueing: {late_elapsed:.3f}s"
    )
    assert cleared, "the latch did not clear when the abandoned publish finished"
    assert after_error is None, after_error
    assert losses == SinkLosses(dropped=0, failed=1), "the abandoned event, counted once"


# --- Frame-2 finding 2: a buffer that never drains still arms the latch -----------------------


class ParkedAndStalledNATS(StallsAfterFirstPublishNATS):
    """The flusher is parked and the server stops answering once the batch is published."""

    def __init__(self) -> None:
        super().__init__()
        self.flusher_parked = True


def test_an_undrained_buffer_leaves_a_probe_for_the_latch_to_age(monkeypatch) -> None:
    """Frame-2 finding 2: the call after an undrained batch is refused at once, not after a budget.

    The confirmation's drained wait failing used to short-circuit before any PING, so no probe
    existed for the latch to age, and the next caller sent one and waited a whole budget (p11:
    0.95 s for callers queued behind a 0.5 s emit). A probe is now left outstanding when the wait
    fails; once it has gone unanswered for ``publish_timeout`` the next call is refused before the
    lock, sending no PING of its own.

    The call is made at once. Frame-3 finding 2: the probe was dated when the drained wait
    failed, at the deadline, so the next caller waited a whole budget more (0.50 s on the fakes,
    1.002 s on the real driver at ``publish_timeout=1.0``); an earlier version of this test slept
    past ``publish_timeout`` first and so could not see it. The probe is now aged from the start
    of the call whose budget it spent. Dated when the drained wait began, as it briefly was, this
    test flaked one run in four: the next caller still waited the time the probe and the publish
    had taken.
    """
    sink, fake, _ = _build(
        monkeypatch, factory=ParkedAndStalledNATS, publish_timeout=AT_ONCE_BUDGET
    )
    sink.emit([{"n": 1}])
    first = sink.losses()
    pings = fake.calls.count("flush")
    elapsed, error = _timed(lambda: sink.emit([{"n": 2}]))
    later = fake.calls.count("flush")
    with contextlib.suppress(TimeoutError):
        sink.close()

    assert first == SinkLosses(dropped=0, failed=1)
    assert pings == 2, "the pre-publish probe, then the one left for the latch"
    assert elapsed < AT_ONCE_CEILING, f"the latch must refuse at once: {elapsed:.3f}s"
    assert isinstance(error, SinkDeliveryError) and "PING" in str(error), error
    assert later == pings, "the refused call sent no PING of its own"


def test_a_ping_sent_before_the_buffer_drained_never_confirms_it(monkeypatch) -> None:
    """Frame-2 finding 2's guard: a liveness probe is never a batch's confirmation.

    The PING left for the latch went out over an undrained buffer, so it may have overtaken the
    publishes. Here the buffer then drains and that PING's PONG arrives during a ``flush()``: the
    flush must take it as liveness only and send a fresh PING, after the drain, to confirm. A sink
    that reused the outstanding probe as the confirmation would return after two PINGs, the second
    sent with nothing written.

    The probe an undrained emit leaves behind is already ``publish_timeout`` old when the emit
    returns (it is aged from the start of the call whose budget it spent), so a flush cannot
    reach it before the latch does. The young outstanding probe this test needs is therefore built
    through the sink's own route on the loop: the left-behind probe is answered, and the sink's
    ``_send_probe`` sends a fresh PING, dated now, over the still-parked buffer. That is exactly
    the state a liveness probe sent before the drain leaves, without depending on how early in a
    budget the drained wait began.
    """
    sink, fake, _ = _build(monkeypatch, factory=ParkedAndStalledNATS, publish_timeout=0.5)
    sink.emit([{"n": 1}])
    assert fake.pubs_at_ping == [0, 0], "the probe, then the liveness PING over the full buffer"

    async def young_liveness_probe_over_the_full_buffer() -> None:
        assert sink._probe is not None
        fake.deliver_pong()
        await asyncio.wait({sink._probe[0]}, timeout=2.0)
        assert sink._outstanding_probe() is None
        sink._send_probe(silent_since=time.monotonic())

    _on_loop(sink, young_liveness_probe_over_the_full_buffer())
    assert fake.pubs_at_ping == [0, 0, 0], "a young PING sent over the undrained buffer"

    async def drain_and_answer_late() -> None:
        fake.flusher_parked = False
        fake._run_flusher()
        fake.answering = True
        asyncio.get_running_loop().call_later(0.1, fake.deliver_pong)

    _on_loop(sink, drain_and_answer_late())
    elapsed, error = _timed(sink.flush)
    pings = list(fake.pubs_at_ping)
    sink.close()

    assert error is None, error
    assert elapsed < 0.5, f"{elapsed:.2f}s"
    assert pings == [0, 0, 0, 1], "the confirmation must be a fresh PING sent after the drain"


SLOW_FIRST_PONG = 1.0
"""Seconds :class:`SlowFirstPongParkedNATS` holds its pre-publish PONG.

It is also the wait the defect in the test that uses it takes: a probe aged from when its drained
wait began is only ``budget - SLOW_FIRST_PONG`` old when the emit returns, so the next emit
reuses it and waits the remaining :data:`SLOW_FIRST_PONG` before refusing.
"""

SLOW_FIRST_PONG_BUDGET = 1.5 * SLOW_FIRST_PONG
"""The ``publish_timeout`` that test runs at, so the PONG takes two thirds of it."""


class SlowFirstPongParkedNATS(ParkedAndStalledNATS):
    """The pre-publish PING is answered after :data:`SLOW_FIRST_PONG`, then everything stalls."""

    async def flush(self, timeout: float = 10) -> None:  # noqa: ASYNC109
        if not self.calls.count("flush"):
            self.answering = False
            asyncio.get_running_loop().call_later(SLOW_FIRST_PONG, self.deliver_pong)
        await super().flush(timeout)


def test_the_latch_holds_after_an_undrained_batch_however_late_the_drained_wait_began(
    monkeypatch,
) -> None:
    """The left-behind probe is aged from the call's start, not from when its drained wait began.

    The pre-publish PONG takes 1.0 s of the 1.5 s budget, so the drained wait begins two thirds of
    the way through it and fails at the deadline. Aged from the call's start, the probe is a whole
    budget old when the emit returns, and the next emit is refused by the latch at once, sending
    no PING of its own. Aged from when the drained wait began, it was 0.5 s old: the next emit
    reused it and waited the remaining 1.0 s before refusing with "no PONG", which is the shape
    that made the latch test flake one parallel run in four. The ceiling is half that wait; at the
    earlier 0.2 s of a 0.3 s budget the wait was too short to bound above a CI stall.
    """
    sink, fake, _ = _build(
        monkeypatch, factory=SlowFirstPongParkedNATS, publish_timeout=SLOW_FIRST_PONG_BUDGET
    )
    sink.emit([{"n": 1}])
    first = sink.losses()
    pings = fake.calls.count("flush")
    elapsed, error = _timed(lambda: sink.emit([{"n": 2}]))
    later = fake.calls.count("flush")
    with contextlib.suppress(TimeoutError):
        sink.close()

    assert first == SinkLosses(dropped=0, failed=1)
    assert pings == 2, "the slow pre-publish probe, then the one left for the latch"
    assert elapsed < SLOW_FIRST_PONG / 2, f"the latch must refuse at once: {elapsed:.3f}s"
    assert isinstance(error, SinkDeliveryError), error
    assert "has not answered a PING" in str(error), (
        f"refused by a reused wait, not the latch: {error}"
    )
    assert later == pings, "the refused call sent no PING of its own"
