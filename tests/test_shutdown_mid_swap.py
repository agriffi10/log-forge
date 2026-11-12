"""A ``shutdown()`` or the exit drain landing mid-swap closes each sink once and leaves none open.

``configure(sink=B)`` with a live worker on A retargets the worker and then, in a second critical
section, arms B and releases A. A shutdown landing between the two found A owed and unheld, took
it and closed it inline; the swap's decision then popped A from the record with a default and
released it a second time on ``log-foundry-sink-close``. B, the worker's sink but not yet owed,
was drained into and never closed, so ``shutdown()`` returned with it open. Measured at
``624d817f`` in a fresh process per run: A closed twice in 20 of 20 forced runs, for
``shutdown()`` and the exit drain alike, and B left open in 5 of 5.

Two parts hold it. The swap releases its displaced sink only while the record still owes it
(``_CloseRegistry.owes``), which keeps A once on every path, and a shutdown waits for each swap in
flight to take its decision, within its closer grace, which closes B. The tests below hold each
part with a test of its own, and guard what the wait must not cost: a re-entrant shutdown that
would wait on itself, the drain charged to the grace, more than one grace per call, a backoff the
stop signal should cut short, and ``_shutdown_running`` left raised by an interrupt. Serves
invariants 5 and 3.
"""

from __future__ import annotations

import io
import re
import subprocess
import sys
import textwrap
import threading
import time
from typing import Any

import pytest

import log_foundry
from log_foundry import _lifecycle
from log_foundry import worker as worker_mod
from log_foundry.worker import Worker
from test_fork_lifecycle import run_in_child

pytestmark = pytest.mark.usefixtures("no_close_left_in_flight")

GATE_WAIT = 10.0
"""Seconds a test waits for a parked thread to arrive, or to leave once released."""

NOT_DECIDED = "shutdown() could not wait for a sink change in flight to take effect"
"""The start of the ``pending`` line a shutdown writes when a swap did not decide in time."""


class Sink:
    """Counts events and records each close with the event count and the thread that ran it.

    Two closes at the same event count are a double close: nothing was written between them, so
    the second had nothing to flush.
    """

    log_foundry_stop_signal: threading.Event | None = None

    def __init__(self, name: str) -> None:
        self.name = name
        self.events = 0
        self.at_close: list[tuple[int, str]] = []
        self.lock = threading.Lock()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Counts the batch."""
        with self.lock:
            self.events += len(batch)

    def close(self) -> None:
        """Records the close, the event count it saw and the closing thread."""
        with self.lock:
            self.at_close.append((self.events, threading.current_thread().name))

    @property
    def closes(self) -> int:
        """How many times it was closed."""
        return len(self.at_close)

    def __repr__(self) -> str:
        """Names the sink and its closes, so a failure reads without a debugger."""
        return f"<{self.name} events={self.events} closes={self.at_close}>"


@log_foundry.trace
def _traced() -> None:
    log_foundry.info("span")


def _worker_on(sink: Any) -> Worker:
    """Configures ``sink`` and builds a live worker on it through one span."""
    log_foundry.configure(service="shutdown-mid-swap", sink=sink)
    _traced()
    log_foundry.flush(timeout=5.0)
    worker = _lifecycle._state.live_worker()
    assert worker is not None and worker.sink is sink
    return worker


class _ParkAfterRetarget:
    """Parks the swap to ``target`` after its retarget returns, before it decides.

    That is the window: the worker already delivers to ``target``, and the swap has neither armed
    it nor released the sink it displaced.
    """

    def __init__(self, monkeypatch: pytest.MonkeyPatch, target: object) -> None:
        self.parked = threading.Event()
        self.release = threading.Event()
        real = Worker.retarget

        def parked_after(self_: Worker, new_sink: Any, deadline: float | None = None) -> Any:
            outcome = real(self_, new_sink, deadline)
            if new_sink is target and not self.parked.is_set():
                self.parked.set()
                self.release.wait(GATE_WAIT * 3)
            return outcome

        monkeypatch.setattr(Worker, "retarget", parked_after)


def _start(work: Any) -> threading.Thread:
    thread = threading.Thread(target=work, daemon=True)
    thread.start()
    return thread


def _finish(thread: threading.Thread) -> None:
    thread.join(GATE_WAIT)
    assert not thread.is_alive(), "a call did not return once released"


def _once(sink: Sink) -> bool:
    """Closed exactly once, so never twice at one event count."""
    return sink.closes == 1


# -- 1. the wait passes ---------------------------------------------------------------------------


def test_a_shutdown_in_the_window_waits_for_the_decision_and_closes_each_sink_once(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Invariant 5: the shutdown waits for the swap's decision, so B is owed when it takes.

    At ``624d817f`` the shutdown closed A inline and returned with B open, and the swap then
    released A again: ``A`` closed at ``[(3, MainThread), (3, log-foundry-sink-close)]``. Here the
    shutdown is still inside after 0.3 s, waiting; released, the swap arms B and releases A, and
    the shutdown closes B and waits for A's close as a bystander.
    """
    a, b = Sink("A"), Sink("B")
    _worker_on(a)
    _traced()
    gate = _ParkAfterRetarget(monkeypatch, b)
    swap = _start(lambda: log_foundry.configure(sink=b))
    assert gate.parked.wait(GATE_WAIT), "the swap reached its window"
    capsys.readouterr()
    returned: dict[str, object] = {}

    def stop() -> None:
        log_foundry.shutdown(timeout=5.0)
        returned["b_closes"] = b.closes

    shutdown = _start(stop)
    time.sleep(0.3)
    waiting = shutdown.is_alive()
    gate.release.set()
    _finish(swap)
    _finish(shutdown)
    err = capsys.readouterr().err
    assert _once(a), f"A was not closed exactly once: {a}"
    assert returned["b_closes"] == 1, f"shutdown() returned with B open: {b}"
    assert _once(b), f"B was not closed exactly once: {b}"
    assert waiting, "the shutdown did not wait for the swap in flight to decide"
    assert NOT_DECIDED not in err, err


# -- 2. the wait runs out: Part 1 alone keeps A once ----------------------------------------------


def test_a_shutdown_whose_wait_runs_out_still_closes_the_displaced_sink_once(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Invariant 5 on the fallback: the swap releases A only while the record still owes it.

    The closer grace is patched to 0.3 s and the swap stays parked past it, so the shutdown
    proceeds without the decision, closes A inline, announces what it could not settle, and
    returns having waited about the grace rather than its 5 s budget. The swap's decision then
    finds A no longer owed and leaves it to the closer that took it. At ``624d817f`` it took A
    anyway and released it a second time. B, armed by that late decision, is closed exactly once,
    by the next shutdown.
    """
    monkeypatch.setattr(_lifecycle, "DEFAULT_CLOSER_GRACE", 0.3)
    a, b = Sink("A"), Sink("B")
    _worker_on(a)
    gate = _ParkAfterRetarget(monkeypatch, b)
    swap = _start(lambda: log_foundry.configure(sink=b))
    assert gate.parked.wait(GATE_WAIT), "the swap reached its window"
    capsys.readouterr()
    start = time.monotonic()
    log_foundry.shutdown(timeout=5.0)
    elapsed = time.monotonic() - start
    still_parked = swap.is_alive()
    gate.release.set()
    _finish(swap)
    _lifecycle._state._closes.wait_graced(_lifecycle._Operation(_lifecycle._Op.STOP, 5.0), None)
    err = capsys.readouterr().err
    assert _once(a), f"A was not closed exactly once: {a}"
    assert still_parked, "the premise: the shutdown returned while the swap was still parked"
    assert 0.25 <= elapsed < 2.5, f"the wait spent {elapsed:.2f}s, not about the 0.3 s grace"
    assert NOT_DECIDED in err, err
    assert "shutdown timed out" not in err, f"the drain lost its budget to the wait: {err}"
    log_foundry.shutdown(timeout=5.0)
    assert _once(b), f"B was not closed exactly once by the next shutdown: {b}"


# -- 2b. the wait runs out, but the swap decides during the drain ---------------------------------


class _SlowEmit(Sink):
    """A sink whose ``emit`` holds the drain thread for ``seconds`` once armed."""

    def __init__(self, name: str, seconds: float) -> None:
        super().__init__(name)
        self.seconds = seconds
        self.armed = False
        self.entered = threading.Event()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Sleeps once if armed, then counts."""
        if self.armed:
            self.armed = False
            self.entered.set()
            time.sleep(self.seconds)
        super().emit(batch)


def test_a_swap_that_decides_during_the_drain_is_not_announced_as_left_open(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Invariant 5 and its line agree: a swap that decides before the take is closed, unannounced.

    A's ``emit`` holds the drain for 1.5 s, and the swap to B is stuck in its first flush behind
    it. The closer grace is 0.3 s, so the shutdown gives up waiting; but the drain then ends, the
    retarget declines, and the swap arms B before the call takes. The swap thread is joined at the
    take's entry so that order is forced rather than raced. B is then closed by this very call, so
    the line saying it may be left open must not be written. Measured on the frame-2 review's
    reproduction, the line was written at the wait, before the drain, and was false.
    """
    monkeypatch.setattr(_lifecycle, "DEFAULT_CLOSER_GRACE", 0.3)
    a, b = _SlowEmit("A", 1.5), Sink("B")
    _worker_on(a)
    swap_thread: list[threading.Thread] = []
    real_close_owed = _lifecycle._close_owed

    def after_the_swap(op: Any = None, **kwargs: Any) -> bool:
        if swap_thread:
            swap_thread[0].join(GATE_WAIT)
        return real_close_owed(op, **kwargs)

    monkeypatch.setattr(_lifecycle, "_close_owed", after_the_swap)
    a.armed = True
    _traced()
    swap = _start(lambda: log_foundry.configure(sink=b))
    assert a.entered.wait(GATE_WAIT), "the drain is inside A's slow emit"
    deadline = time.monotonic() + GATE_WAIT
    while not _lifecycle._state._swaps_running and time.monotonic() < deadline:
        time.sleep(0.01)
    assert _lifecycle._state._swaps_running, "the premise: the swap is in flight"
    capsys.readouterr()
    swap_thread.append(swap)
    log_foundry.shutdown(timeout=6.0)
    b_at_return = b.closes
    _finish(swap)
    err = capsys.readouterr().err
    assert b_at_return == 1, f"the call did not close the B the swap armed during its drain: {b}"
    assert NOT_DECIDED not in err, f"a false not-decided line: {err}"
    assert _once(a), f"A was not closed exactly once: {a}"
    log_foundry.shutdown(timeout=2.0)
    assert _once(b), f"B was not closed exactly once: {b}"


# -- 3. a re-entrant shutdown never waits on itself -----------------------------------------------


class _ShutdownFromSetter(Sink):
    """Calls ``shutdown(None)`` once from its stop-signal setter, which the retarget runs."""

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.fired = False

    @property
    def log_foundry_stop_signal(self) -> threading.Event | None:  # type: ignore[override]
        """Holds nothing; the setter is the hook."""
        return None

    @log_foundry_stop_signal.setter
    def log_foundry_stop_signal(self, value: threading.Event | None) -> None:
        if value is not None and not self.fired and _lifecycle._state._swaps_running:
            self.fired = True
            log_foundry.shutdown(timeout=None)


class _ShutdownFromEmit(Sink):
    """Calls ``shutdown(None)`` once from ``emit`` on the drain thread, while a swap is in flight."""

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.fired = False

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Counts, then shuts down once if a swap is in flight."""
        super().emit(batch)
        if not self.fired and _lifecycle._state._swaps_running:
            self.fired = True
            log_foundry.shutdown(timeout=None)


@pytest.mark.parametrize("where", ["setter", "emit"])
def test_a_reentrant_shutdown_during_a_swap_does_not_wait_on_it(
    where: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Invariant 3: a shutdown the swap is itself waiting on skips that swap.

    ``setter``: B's stop-signal setter, run by the retarget on the swap's own thread, calls
    ``shutdown(None)``; the shutdown skips its own thread's token. ``emit``: A's ``emit``, on the
    drain thread a retarget's flush is waiting for, calls ``shutdown(None)``; a shutdown on a
    ``log-foundry-`` thread waits on no swap. Either wait would last the whole closer grace, which
    is patched far past the child's bound, so a regression is a child killed rather than a slow
    pass; the swap budget is unbounded for the same reason. Run in a forked child because the
    regression is a hang.
    """
    monkeypatch.setattr(_lifecycle, "DEFAULT_CLOSER_GRACE", 120.0)
    monkeypatch.setattr(worker_mod, "DEFAULT_SWAP_TIMEOUT", None)

    def scenario() -> str:
        sys.stderr = io.StringIO()
        a = _ShutdownFromEmit("A") if where == "emit" else Sink("A")
        b = _ShutdownFromSetter("B") if where == "setter" else Sink("B")
        _worker_on(a)
        if where == "emit":
            real = Worker.retarget

            def with_a_span(self_: Worker, new_sink: Any, deadline: float | None = None) -> Any:
                _traced()
                return real(self_, new_sink, deadline)

            Worker.retarget = with_a_span  # type: ignore[method-assign]
        start = time.monotonic()
        log_foundry.configure(sink=b)
        elapsed = time.monotonic() - start
        fired = a.fired if isinstance(a, _ShutdownFromEmit) else b.fired  # type: ignore[union-attr]
        log_foundry.shutdown(timeout=2.0)
        return f"{elapsed:.3f}|{fired}|{a.at_close}|{b.closes}"

    child = run_in_child(scenario)
    assert child.finished, f"the child hung: it {child.ending}, {child.output!r}"
    elapsed, fired, a_closes, b_closes = child.output.split("|")
    assert fired == "True", "the premise: the re-entrant shutdown ran during the swap"
    assert float(elapsed) < 1.0, f"configure() stalled {elapsed}s on its own shutdown"
    assert a_closes.count("(") == 1, f"A was not closed exactly once: {a_closes}"
    assert b_closes == "1", f"B was not closed exactly once: {b_closes}"


# -- 4. the exit drain ----------------------------------------------------------------------------


def test_the_exit_drain_in_the_window_closes_each_sink_once() -> None:
    """Invariant 5 at exit: the library's ``atexit`` handler waits for the swap's decision.

    The swap is parked after its retarget on a daemon thread and released by a daemon timer
    about 0.3 s later; the main thread falls off the end, so the library's handler runs while the
    swap is parked. A probe handler registered before ``import log_foundry`` runs after it, and
    prints what each sink had seen by then. At ``624d817f`` A was closed twice, the second time
    on ``log-foundry-sink-close``, in 20 of 20 runs.
    """
    program = textwrap.dedent(
        """
        import atexit
        import threading

        a_closes = []
        b_closes = []
        atexit.register(lambda: print(f"PROBE A={a_closes} B={b_closes}", flush=True))

        import log_foundry
        from log_foundry.worker import Worker


        class Sink:
            log_foundry_stop_signal = None

            def __init__(self, closes):
                self.events = 0
                self.closes = closes

            def emit(self, batch):
                self.events += len(batch)

            def close(self):
                self.closes.append((self.events, threading.current_thread().name))


        a, b = Sink(a_closes), Sink(b_closes)
        parked, go = threading.Event(), threading.Event()
        real = Worker.retarget


        def parked_after(self, new_sink, deadline=None):
            outcome = real(self, new_sink, deadline)
            if new_sink is b:
                parked.set()
                go.wait(10)
            return outcome


        Worker.retarget = parked_after
        log_foundry.configure(service="exit-mid-swap", sink=a)


        @log_foundry.trace
        def traced():
            log_foundry.info("x")


        traced()
        threading.Thread(target=lambda: log_foundry.configure(sink=b), daemon=True).start()
        assert parked.wait(10)
        timer = threading.Timer(0.3, go.set)
        timer.daemon = True
        timer.start()
        print("EXITING", flush=True)
        """
    )
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", program], capture_output=True, text=True, timeout=90
    )
    assert "EXITING" in result.stdout, f"the child never got there: {result.stderr[-800:]}"
    probe = re.search(r"PROBE A=(\[.*?\]) B=(\[.*?\])", result.stdout)
    assert probe is not None, f"the probe handler did not run:\n{result.stdout}"
    a_closes, b_closes = probe.group(1), probe.group(2)
    assert a_closes.count("(") == 1, f"A was not closed exactly once at exit: {a_closes}"
    assert b_closes.count("(") == 1, f"B was not closed exactly once at exit: {b_closes}"
    assert NOT_DECIDED not in result.stderr, result.stderr[-800:]


# -- 6. the wait does not charge the drain to the grace, and spends one grace at most -------------


class _SlowFirstEmit(Sink):
    """A sink whose first ``emit`` holds the drain thread for ``seconds``."""

    def __init__(self, name: str, seconds: float) -> None:
        super().__init__(name)
        self.seconds = seconds
        self.slowed = False

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Sleeps the first time, then counts."""
        if not self.slowed:
            self.slowed = True
            time.sleep(self.seconds)
        super().emit(batch)


class _SlowCloseAfter(_SlowFirstEmit):
    """A slow-first-emit sink whose close also takes ``close_seconds``, recorded when it ends."""

    def __init__(self, name: str, seconds: float, close_seconds: float) -> None:
        super().__init__(name, seconds)
        self.close_seconds = close_seconds

    def close(self) -> None:
        """Takes its time, then records."""
        time.sleep(self.close_seconds)
        super().close()


def _strand_unfenced(monkeypatch: pytest.MonkeyPatch, y: _SlowFirstEmit, a: Sink) -> Worker:
    """Builds a worker on ``y``, then swaps to ``a`` with a budget ``y``'s first emit outlasts.

    The swap's drain is never confirmed, so ``y`` stays owed and among the sinks the worker may be
    inside: a later shutdown that finds the drain thread ended takes it as a graced close.
    """
    log_foundry.configure(service="shutdown-mid-swap", sink=y)
    _traced()
    monkeypatch.setattr(worker_mod, "DEFAULT_SWAP_TIMEOUT", 0.1)
    log_foundry.configure(sink=a)
    monkeypatch.setattr(worker_mod, "DEFAULT_SWAP_TIMEOUT", 5.0)
    worker = _lifecycle._state.live_worker()
    assert worker is not None and worker.sink is a
    assert worker.holds_unfenced(y), "the premise: y was swapped out without a confirmed fence"
    assert worker.health().incomplete_swaps == 1
    return worker


class _ParkFor:
    """Holds the swap to ``target`` for ``seconds`` after its retarget returns."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, target: object, seconds: float) -> None:
        self.parked = threading.Event()
        self.until = 0.0
        real = Worker.retarget

        def parked_after(self_: Worker, new_sink: Any, deadline: float | None = None) -> Any:
            outcome = real(self_, new_sink, deadline)
            if new_sink is target and not self.parked.is_set():
                self.until = time.monotonic() + seconds
                self.parked.set()
                time.sleep(seconds)
            return outcome

        monkeypatch.setattr(Worker, "retarget", parked_after)


def test_the_wait_does_not_charge_the_drain_to_the_grace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Invariant 3: the grace left after the wait is fixed after the drain, not before it.

    The shutdown waits 0.15 s for a swap's decision, then drains for 2.5 s into B, and only then
    takes the unfenced Y, whose detached close takes 0.5 s. The graced wait at the end must still
    cover Y: a grace opened at the wait would have ended during the drain, and the shutdown would
    have returned with Y's close still running (plan-review round 2, measured on the barrier it
    replaced).
    """
    y = _SlowCloseAfter("Y", 0.6, 0.5)
    a, b = Sink("A"), _SlowFirstEmit("B", 2.5)
    _strand_unfenced(monkeypatch, y, a)
    time.sleep(0.7)
    park = _ParkFor(monkeypatch, b, 0.15)
    swap = _start(lambda: log_foundry.configure(sink=b))
    assert park.parked.wait(GATE_WAIT), "the swap reached its window"
    _traced()
    log_foundry.shutdown(timeout=10.0)
    y_at_return = y.closes
    _finish(swap)
    _lifecycle._state._closes.wait_graced(_lifecycle._Operation(_lifecycle._Op.STOP, 5.0), None)
    assert b.events >= 1, "the premise: the drain delivered into B"
    assert y_at_return == 1, f"shutdown() returned before Y's graced close finished: {y}"


def test_a_shutdown_with_no_swap_in_flight_spends_nothing_on_the_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Invariant 3: an uncontended shutdown waits on nothing and charges nothing to its grace."""
    seen: list[float] = []
    real = _lifecycle._close_owed

    def recording(op: Any = None, **kwargs: Any) -> bool:
        if op is not None:
            seen.append(getattr(op, "grace_spent", 0.0))
        return real(op, **kwargs)

    monkeypatch.setattr(_lifecycle, "_close_owed", recording)
    _worker_on(Sink("A"))
    log_foundry.shutdown(timeout=5.0)
    assert seen, "the premise: the shutdown ran its closer"
    assert seen == [0.0] * len(seen), f"an uncontended shutdown recorded a wait: {seen}"


class _StuckClose(Sink):
    """A sink whose close parks until released, so its graced close is never done in time."""

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.release = threading.Event()

    def close(self) -> None:
        """Parks, then records."""
        self.release.wait(GATE_WAIT * 3)
        super().close()


def test_the_wait_and_the_graced_closes_share_one_grace(monkeypatch: pytest.MonkeyPatch) -> None:
    """Invariant 3, SPEC-078 FR-002: what the wait spent comes out of the call's one grace.

    The swap decides 1.5 s into a 2 s grace, and a detached close the shutdown then waits on never
    finishes. The call spends about the one grace in all, where a grace that ignored the wait
    would spend 1.5 s and then 2 s more.
    """
    y = _StuckClose("Y")
    a, b = Sink("A"), Sink("B")
    log_foundry.configure(service="shutdown-mid-swap", sink=y)
    _traced()
    log_foundry.flush(timeout=5.0)
    monkeypatch.setattr(worker_mod, "DEFAULT_SWAP_TIMEOUT", 0.2)
    log_foundry.configure(sink=a)
    monkeypatch.setattr(worker_mod, "DEFAULT_SWAP_TIMEOUT", 5.0)
    try:
        park = _ParkFor(monkeypatch, b, 1.5)
        swap = _start(lambda: log_foundry.configure(sink=b))
        assert park.parked.wait(GATE_WAIT), "the swap reached its window"
        assert _lifecycle._state._closes.graced_alive() == 1, "the premise: y's close is stuck"
        start = time.monotonic()
        log_foundry.shutdown(timeout=30.0)
        returned = time.monotonic()
        _finish(swap)
    finally:
        y.release.set()
    _lifecycle._state._closes.wait_graced(_lifecycle._Operation(_lifecycle._Op.STOP, 5.0), None)
    grace = _lifecycle.DEFAULT_CLOSER_GRACE
    elapsed = returned - start
    assert returned >= park.until, (
        f"the premise: the shutdown returned {park.until - returned:.2f}s before the swap decided"
    )
    assert elapsed < grace + 0.9, f"spent {elapsed:.2f}s, more than one {grace}s grace"


class _GatedSlowClose(Sink):
    """A sink whose close parks on an event, for up to ``seconds``, then records."""

    def __init__(self, name: str, seconds: float) -> None:
        super().__init__(name)
        self.seconds = seconds
        self.release = threading.Event()

    def close(self) -> None:
        """Parks, then records."""
        self.release.wait(self.seconds)
        super().close()


def test_a_swaps_slow_displaced_close_is_not_waited_out_before_the_drain(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Invariant 3: a swap publishes its decision before it joins the close it started.

    A's close takes longer than the 2 s grace. The swap decides 0.2 s after the shutdown starts
    waiting and releases A detached; had it set ``decided`` only after joining that close, the
    shutdown would spend its whole grace before the drain and write a false line saying the change
    had not taken effect. Here the wait records about 0.2 s and nothing is announced; A's close is
    then a bystander the rest of the grace covers.
    """
    spent: list[float] = []
    real_close_owed = _lifecycle._close_owed

    def recording(op: Any = None, **kwargs: Any) -> bool:
        if op is not None and kwargs.get("bystander_wait", True):
            spent.append(getattr(op, "grace_spent", 0.0))
        return real_close_owed(op, **kwargs)

    monkeypatch.setattr(_lifecycle, "_close_owed", recording)
    a, b = _GatedSlowClose("A", 4.0), Sink("B")
    _worker_on(a)
    gate = _ParkAfterRetarget(monkeypatch, b)
    swap = _start(lambda: log_foundry.configure(sink=b))
    try:
        assert gate.parked.wait(GATE_WAIT), "the swap reached its window"
        capsys.readouterr()
        shutdown = _start(lambda: log_foundry.shutdown(timeout=10.0))
        time.sleep(0.2)
        gate.release.set()
        _finish(shutdown)
        err = capsys.readouterr().err
    finally:
        gate.release.set()
        a.release.set()
    _finish(swap)
    _lifecycle._state._closes.wait_graced(_lifecycle._Operation(_lifecycle._Op.STOP, 5.0), None)
    grace = _lifecycle.DEFAULT_CLOSER_GRACE
    assert NOT_DECIDED not in err, f"a false not-decided line: {err}"
    assert spent and spent[0] < grace / 2, (
        f"the shutdown spent {spent} of its {grace}s grace waiting before the drain"
    )
    assert _once(a), f"A was not closed exactly once: {a}"
    assert _once(b), f"B was not closed exactly once: {b}"


def test_the_exit_closes_a_sink_the_swap_installed_after_a_shutdown_ran_out() -> None:
    """Invariant 5 on the fallback, at exit: what a late decision armed is closed by ``atexit``.

    The subprocess variant of the wait-runs-out test. The grace is 0.3 s and the swap stays parked
    past it, so the explicit ``shutdown()`` returns with B not yet owed; the swap is then released
    and arms B, and the main thread falls off the end. The library's ``atexit`` handler is the only
    thing left to close B, and the probe handler, registered before ``import log_foundry`` so it
    runs last, reports that it did, exactly once. This is the path the not-decided ``pending``
    line promises ("left open until the process exits").
    """
    program = textwrap.dedent(
        """
        import atexit
        import threading

        a_closes = []
        b_closes = []
        at_return = []
        atexit.register(
            lambda: print(f"PROBE A={a_closes} B={b_closes} AT_RETURN={at_return}", flush=True)
        )

        import log_foundry
        from log_foundry import _lifecycle
        from log_foundry.worker import Worker

        _lifecycle.DEFAULT_CLOSER_GRACE = 0.3


        class Sink:
            log_foundry_stop_signal = None

            def __init__(self, closes):
                self.events = 0
                self.closes = closes

            def emit(self, batch):
                self.events += len(batch)

            def close(self):
                self.closes.append((self.events, threading.current_thread().name))


        a, b = Sink(a_closes), Sink(b_closes)
        parked, go = threading.Event(), threading.Event()
        real = Worker.retarget


        def parked_after(self, new_sink, deadline=None):
            outcome = real(self, new_sink, deadline)
            if new_sink is b:
                parked.set()
                go.wait(10)
            return outcome


        Worker.retarget = parked_after
        log_foundry.configure(service="exit-after-fallback", sink=a)


        @log_foundry.trace
        def traced():
            log_foundry.info("x")


        traced()
        swapper = threading.Thread(target=lambda: log_foundry.configure(sink=b), daemon=True)
        swapper.start()
        assert parked.wait(10)
        log_foundry.shutdown(timeout=5.0)
        at_return.append(len(b_closes))
        go.set()
        swapper.join(10)
        print("EXITING", flush=True)
        """
    )
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", program], capture_output=True, text=True, timeout=90
    )
    assert "EXITING" in result.stdout, f"the child never got there: {result.stderr[-800:]}"
    probe = re.search(r"PROBE A=(\[.*?\]) B=(\[.*?\]) AT_RETURN=(\[.*?\])", result.stdout)
    assert probe is not None, f"the probe handler did not run:\n{result.stdout}"
    a_closes, b_closes, at_return = probe.group(1), probe.group(2), probe.group(3)
    assert at_return == "[0]", f"the premise: shutdown() returned with B already closed: {b_closes}"
    assert NOT_DECIDED in result.stderr, result.stderr[-800:]
    assert a_closes.count("(") == 1, f"A was not closed exactly once: {a_closes}"
    assert b_closes.count("(") == 1, f"B was not closed exactly once, by the exit: {b_closes}"


# -- 7. a backoff the stop signal cuts short is not held up ---------------------------------------


class _BackingOff(Sink):
    """A sink whose ``emit`` backs off on its stop signal for 4 s once armed."""

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.armed = False
        self.entered = threading.Event()
        self.signal: threading.Event | None = None

    @property
    def log_foundry_stop_signal(self) -> threading.Event | None:  # type: ignore[override]
        """The stop signal the library handed it."""
        return self.signal

    @log_foundry_stop_signal.setter
    def log_foundry_stop_signal(self, value: threading.Event | None) -> None:
        self.signal = value

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Backs off once, cut short by the stop signal, then counts."""
        if self.armed:
            self.armed = False
            self.entered.set()
            if self.signal is not None:
                self.signal.wait(4.0)
        super().emit(batch)


def test_a_shutdown_during_a_swap_held_by_a_backoff_cuts_it_short() -> None:
    """Invariant 3: the stop signal still reaches the backoff the swap's drain is waiting on.

    A backs off 4 s on its stop signal while ``configure(sink=B)``'s retarget waits on that
    drain. ``shutdown(2)`` sets the signal in its entry, before it waits for the swap, so the
    backoff ends at once, the swap's retarget declines, and the call returns well inside a second
    with A and B closed once each. The barrier this replaced measured 3.80 s and an expiry here.
    """
    a, b = _BackingOff("A"), Sink("B")
    _worker_on(a)
    a.armed = True
    _traced()
    swap = _start(lambda: log_foundry.configure(sink=b))
    assert a.entered.wait(GATE_WAIT), "the drain is inside A's backoff"
    time.sleep(0.1)
    start = time.monotonic()
    log_foundry.shutdown(timeout=2.0)
    elapsed = time.monotonic() - start
    _finish(swap)
    log_foundry.shutdown(timeout=2.0)
    assert elapsed < 1.0, f"shutdown(2) took {elapsed:.2f}s behind a backoff it should cut short"
    assert _once(a), f"A was not closed exactly once: {a}"
    assert _once(b), f"B was not closed exactly once: {b}"


# -- 9. an interrupt during the wait --------------------------------------------------------------


class _Interrupting:
    """An event stand-in whose ``wait`` raises ``KeyboardInterrupt``, as a signal handler would."""

    def wait(self, timeout: float | None = None) -> bool:
        """Raises."""
        raise KeyboardInterrupt

    def is_set(self) -> bool:
        """Never set."""
        return False

    def set(self) -> None:
        """Does nothing."""


def test_an_interrupt_during_the_wait_still_lowers_the_shutdown_count() -> None:
    """The no-worker branch's wait sits inside its ``try``, so its ``finally`` still runs.

    A no-worker swap decides in its first critical section, so a token undecided here cannot be
    produced by the library; one is planted, owned by another thread, whose wait raises.
    """
    token = _lifecycle._Operation(_lifecycle._Op.SWAP, 5.0)
    token.thread = -1
    token.decided = _Interrupting()  # type: ignore[assignment]
    _lifecycle._state._swaps_running.add(token)
    try:
        with pytest.raises(KeyboardInterrupt):
            _lifecycle._shutdown_worker(timeout=5.0)
    finally:
        _lifecycle._state._swaps_running.discard(token)
    assert _lifecycle._state._shutdown_running == 0, "an interrupted wait left the count raised"
    assert not _lifecycle._state._stops_running, "and its draining token in place"


# -- 10. a swap's stop-signal hand neither holds a shutdown nor mislabels its verdict -------------


class _ParkedSetter(Sink):
    """A sink whose stop-signal setter parks until ``release`` is set, recording what it stored."""

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.handing = threading.Event()
        self.release = threading.Event()
        self.stored: threading.Event | None = None

    @property
    def log_foundry_stop_signal(self) -> threading.Event | None:  # type: ignore[override]
        """The signal the setter last stored."""
        return self.stored

    @log_foundry_stop_signal.setter
    def log_foundry_stop_signal(self, value: threading.Event | None) -> None:
        self.handing.set()
        self.release.wait(GATE_WAIT)
        self.stored = value


def test_a_shutdown_does_not_wait_out_a_no_worker_swaps_slow_setter() -> None:
    """Invariant 3: a no-worker swap has decided before it hands its sink the signal.

    A sink P is owed through an orphan log, then ``configure(sink=S)`` runs a no-worker swap whose
    first section arms S and whose hand then parks in S's setter. A ``shutdown()`` on the main
    thread snapshots that swap; it must find it decided and go on, closing S while the setter is
    still parked, and the setter then stores the very event that shutdown set. At ``9f4d4dfd``
    the swap published ``decided`` only after the hand, so the shutdown waited out its whole
    2.00 s closer grace and wrote the not-decided ``pending`` line for a decision taken long
    before. Run in a forked child, so the parked setter cannot outlive the test.
    """

    def scenario() -> str:
        err = io.StringIO()
        sys.stderr = err
        p, s = Sink("P"), _ParkedSetter("S")
        log_foundry.configure(service="shutdown-mid-swap", sink=p)
        log_foundry.info("owes P")
        swap = _start(lambda: log_foundry.configure(sink=s))
        if not s.handing.wait(GATE_WAIT):
            return "the swap never reached the hand"
        start = time.monotonic()
        log_foundry.shutdown(timeout=5.0)
        elapsed = time.monotonic() - start
        closed_while_parked = s.closes
        s.release.set()
        swap.join(GATE_WAIT)
        stored = s.stored
        current = stored is _lifecycle._state._stop and stored is not None and stored.is_set()
        return (
            f"line={NOT_DECIDED in err.getvalue()} quick={elapsed < 1.0} "
            f"closed_while_parked={closed_while_parked} stored_the_set_signal={current}"
        )

    child = run_in_child(scenario)
    assert child.output == (
        "line=False quick=True closed_while_parked=1 stored_the_set_signal=True"
    ), child.output


UNFENCED = "could not be confirmed drained"
"""The fixed part of the line a swap writes when it counts an unconfirmed drain."""


def test_a_setter_shutdown_after_the_reassign_is_declined_not_unfenced() -> None:
    """A retarget whose new sink's setter shuts down reports ``declined``, counting nothing.

    The live worker on P is retargeted at A, and A's setter, run by the retarget after the
    reassign, calls ``shutdown()``. That shutdown stops the worker, drains into A and closes P, so
    the retarget's fence flush answers ``retired`` without trying. At ``9f4d4dfd`` the retarget
    read that as an unconfirmed drain: ``incomplete_swaps`` went to 1 and a line said P "could
    not be confirmed drained" and was "left open", though nothing timed out and P was closed.
    A is still owed, for the exit (invariant 5).
    """

    def scenario() -> str:
        err = io.StringIO()
        sys.stderr = err
        p, a = Sink("P"), _ShutdownFromSetter("A")
        worker = _worker_on(p)
        log_foundry.configure(sink=a)
        return (
            f"fired={a.fired} incomplete={worker.incomplete_swaps} "
            f"line={UNFENCED in err.getvalue()} a_owed={_lifecycle.owes_close(a)} "
            f"p_closes={p.closes}"
        )

    child = run_in_child(scenario)
    assert child.output == ("fired=True incomplete=0 line=False a_owed=True p_closes=1"), (
        child.output
    )


def test_a_shutdown_racing_the_fence_flush_is_declined_not_unfenced() -> None:
    """A ``shutdown()`` on another thread that retires the worker before the fence flush reads.

    The fence flush is wrapped: before it runs, a ``shutdown()`` is started on another thread
    and the wrapper waits until the retirement count has moved past the worker's epoch — the
    token alone is not enough, since the shutdown adds it before it moves the count. The real
    flush then answers ``retired``. That shutdown waits for this swap's decision, so it closes
    P and A once each after it. This is the case the fence flush's own verdict covers and a
    re-check of the retirement right after the stop-signal hand does not: the count had not
    moved yet when the hand returned. At ``9f4d4dfd`` the swap counted an incomplete swap and
    wrote the unfenced line here too.
    """

    def scenario() -> str:
        err = io.StringIO()
        sys.stderr = err
        p, a = Sink("P"), Sink("A")
        worker = _worker_on(p)
        real = Worker.flush
        calls: list[int] = []
        stopping: list[threading.Thread] = []
        retired_in_time: list[bool] = []

        def fenced_late(self_: Worker, timeout: float | None = 5.0) -> Any:
            calls.append(1)
            if len(calls) == 2:
                stopping.append(_start(lambda: log_foundry.shutdown(timeout=5.0)))
                deadline = time.monotonic() + GATE_WAIT
                while _lifecycle._state.retirements <= self_._epoch:
                    if time.monotonic() > deadline:
                        break
                    time.sleep(0.001)
                retired_in_time.append(_lifecycle._state.retirements > self_._epoch)
            return real(self_, timeout)

        Worker.flush = fenced_late  # type: ignore[method-assign]
        log_foundry.configure(sink=a)
        Worker.flush = real  # type: ignore[method-assign]
        for thread in stopping:
            thread.join(GATE_WAIT)
        return (
            f"raced={retired_in_time} incomplete={worker.incomplete_swaps} "
            f"line={UNFENCED in err.getvalue()} p_closes={p.closes} a_closes={a.closes}"
        )

    child = run_in_child(scenario)
    assert child.output == ("raced=[True] incomplete=0 line=False p_closes=1 a_closes=1"), (
        child.output
    )
