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
    still parked, and the setter then stores the very event that shutdown set: since SPEC-098,
    S's own event, created by the hand and set by the shutdown's entry. At ``9f4d4dfd``
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
        own = _lifecycle._state._signals[id(s)][1]
        current = stored is own and stored is not None and stored.is_set()
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
    """A setter's ``shutdown()`` during a swap counts nothing and closes the displaced sink once.

    The live worker on P is retargeted at A, and A's setter, run by the retarget after the
    reassign, calls ``shutdown()``. At ``9f4d4dfd`` the retarget read the stopped worker as an
    unconfirmed drain: ``incomplete_swaps`` went to 1 and a line said P "could not be confirmed
    drained" and was "left open", though nothing timed out and P was closed. Until SPEC-097 that
    shutdown ran inline under the change, so the retarget declined and A stayed owed for the exit.
    The setter runs on the changing thread, so since SPEC-097 FR-002 its ``shutdown()`` is
    deferred to the change's end: the swap completes, and the deferred stop closes P once and
    takes A too, so nothing is left owed (invariant 5).
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
    assert child.output == ("fired=True incomplete=0 line=False a_owed=False p_closes=1"), (
        child.output
    )


def test_a_shutdown_racing_the_fence_flush_with_the_drain_alive_stays_unfenced() -> None:
    """A ``shutdown()`` on another thread retires the worker before the fence flush, drain alive.

    The fence flush is wrapped: before it runs, a ``shutdown()`` is started on another thread
    and the wrapper waits until the retirement count has moved past the worker's epoch — the
    token alone is not enough, since the shutdown adds it before it moves the count. The real
    flush then answers ``retired``, but that shutdown is waiting for this swap's decision and
    has not stopped the drain thread, which is still alive. So the verdict stays ``unfenced``,
    with its count and line, as at ``9f4d4dfd``: only a drain thread that has ended says nothing
    of the drain can still be inside the displaced sink. The shutdown then closes P and A once
    each after the swap decides.
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
    assert child.output == ("raced=[True] incomplete=1 line=True p_closes=1 a_closes=1"), (
        child.output
    )


def test_a_retiring_shutdown_that_expires_inside_the_old_sink_leaves_the_swap_unfenced() -> None:
    """A fence flush that finds the worker retired, by a shutdown that expired, counts and says so.

    After the retarget's first flush the drain thread is sent into a 2 s ``emit`` on A. B's
    setter, run after the reassign, starts a ``shutdown(timeout=0.3)`` on another thread and
    waits until it has retired the worker, so the fence flush answers ``retired``; that shutdown
    then expires with the thread still inside A, and leaves A open. The swap is an unconfirmed
    one, and its line — A is "left open until a shutdown() finds the drain thread ended" — is
    true: the verdict must be ``unfenced``, counting one incomplete swap. Reading ``retired`` alone
    as ``declined`` wrote nothing and counted nothing here, for a sink really left open.
    """

    def scenario() -> str:
        err = io.StringIO()
        sys.stderr = err
        inside = threading.Event()

        class SlowA(Sink):
            slow = False

            def emit(self, batch: list[dict[str, object]]) -> None:
                if self.slow:
                    inside.set()
                    time.sleep(2.0)
                super().emit(batch)

        class StopsOthers(Sink):
            fired = False
            stopper: threading.Thread | None = None

            @property
            def log_foundry_stop_signal(self) -> threading.Event | None:  # type: ignore[override]
                return None

            @log_foundry_stop_signal.setter
            def log_foundry_stop_signal(self, value: threading.Event | None) -> None:
                if self.fired or threading.current_thread() is not threading.main_thread():
                    return
                self.fired = True
                live = _lifecycle._state.worker_exists()
                self.stopper = _start(lambda: log_foundry.shutdown(timeout=0.3))
                deadline = time.monotonic() + GATE_WAIT
                while live is not None and _lifecycle._state.retirements <= live._epoch:
                    if time.monotonic() > deadline:
                        break
                    time.sleep(0.001)

        a, b = SlowA("A"), StopsOthers("B")
        worker = _worker_on(a)
        real = Worker.flush
        calls: list[int] = []

        def slow_after_first(self_: Worker, timeout: float | None = 5.0) -> Any:
            calls.append(1)
            result = real(self_, timeout)
            if len(calls) == 1:
                a.slow = True
                _traced()
                inside.wait(GATE_WAIT)
            return result

        Worker.flush = slow_after_first  # type: ignore[method-assign]
        log_foundry.configure(sink=b)
        Worker.flush = real  # type: ignore[method-assign]
        if b.stopper is not None:
            b.stopper.join(GATE_WAIT)
        return (
            f"incomplete={worker.incomplete_swaps} line={UNFENCED in err.getvalue()} "
            f"a_closes={a.closes} a_held={_lifecycle._state.held(a)} "
            f"b_owed={_lifecycle.owes_close(b)}"
        )

    child = run_in_child(scenario)
    assert child.output == ("incomplete=1 line=True a_closes=0 a_held=True b_owed=True"), (
        child.output
    )


def test_a_setter_shutdown_that_expires_before_the_drain_leaves_stays_unfenced() -> None:
    """A setter's expiring ``shutdown()`` is deferred, so the swap completes and A is closed once.

    The drain thread is inside a 0.6 s ``emit`` on A when B's setter, run after the reassign,
    calls ``shutdown(timeout=0.3)`` itself and then works on for 0.5 s. Until SPEC-097 that
    shutdown ran inline, expired with the thread inside A and left A owed and open, and the
    verdict, which reads the owed record rather than the drain thread's liveness after the
    setter, was ``unfenced`` with one incomplete swap and its line. The setter runs on the changing
    thread, so since SPEC-097 FR-002 its ``shutdown()`` is deferred to the change's end: the swap
    completes, counting nothing, and the deferred stop closes A once (invariant 5).

    ``drain_ended`` is reported and not asserted. The deferred stop's deadline, the setter's
    0.3 s, has passed by the time the change ends, so it runs with no time to wait, and whether
    the drain thread has ended by the read just after ``configure()`` returns is the scheduler's:
    the spec measured ``False`` on its prototype, and this tree reads ``True``.
    """

    def scenario() -> str:
        err = io.StringIO()
        sys.stderr = err
        inside = threading.Event()

        class SlowA(Sink):
            slow = False

            def emit(self, batch: list[dict[str, object]]) -> None:
                if self.slow:
                    inside.set()
                    time.sleep(0.6)
                super().emit(batch)

        class ShutsDownThenWorks(Sink):
            fired = False

            @property
            def log_foundry_stop_signal(self) -> threading.Event | None:  # type: ignore[override]
                return None

            @log_foundry_stop_signal.setter
            def log_foundry_stop_signal(self, value: threading.Event | None) -> None:
                if self.fired or threading.current_thread() is not threading.main_thread():
                    return
                self.fired = True
                log_foundry.shutdown(timeout=0.3)
                time.sleep(0.5)

        a, b = SlowA("A"), ShutsDownThenWorks("B")
        worker = _worker_on(a)
        real = Worker.flush
        calls: list[int] = []

        def slow_after_first(self_: Worker, timeout: float | None = 5.0) -> Any:
            calls.append(1)
            result = real(self_, timeout)
            if len(calls) == 1:
                a.slow = True
                _traced()
                inside.wait(GATE_WAIT)
            return result

        Worker.flush = slow_after_first  # type: ignore[method-assign]
        log_foundry.configure(sink=b)
        Worker.flush = real  # type: ignore[method-assign]
        return (
            f"fired={b.fired} drain_ended={not worker._thread.is_alive()} "
            f"incomplete={worker.incomplete_swaps} line={UNFENCED in err.getvalue()} "
            f"a_owed={_lifecycle.owes_close(a)} a_closes={a.closes}"
        )

    child = run_in_child(scenario)
    output = re.sub(r" drain_ended=(True|False)", "", child.output or "")
    assert output == ("fired=True incomplete=0 line=False a_owed=False a_closes=1"), child.output


# -- SPEC-097 FR-001: the change is in flight from before its config write -----------------------


class _ParkAtSwapEntry:
    """Parks the change to ``target`` on its way into ``_swap_sink``: after its config write.

    ``config._swap_live_sink`` imports ``_swap_sink`` from ``_lifecycle`` at call time, so a patch
    of the module attribute intercepts the change after ``_rebind`` and before any section of the
    swap, which is the window SPEC-097 FR-001 publishes the change across.
    """

    def __init__(self, monkeypatch: pytest.MonkeyPatch, target: object, nested: Any = None) -> None:
        self.parked = threading.Event()
        self.release = threading.Event()
        real = _lifecycle._swap_sink

        def parked(new_sink: Any, timeout: float | None = 5.0) -> None:
            if new_sink is target and not self.parked.is_set():
                self.parked.set()
                self.release.wait(GATE_WAIT * 3)
                if nested is not None:
                    nested()
            real(new_sink, timeout)

        monkeypatch.setattr(_lifecycle, "_swap_sink", parked)


class _ParkStaleOrphan:
    """Parks one orphan log to ``target`` on the thread named ``stale`` at its owed check.

    The orphan log resolved ``target`` before the config write, so once released after a
    ``shutdown()`` it writes to ``target`` and arms it again: the one other sink owed when the
    swap decides, without which a no-worker swap returns early and arms nothing (p11's
    ``forced``).
    """

    def __init__(self, monkeypatch: pytest.MonkeyPatch, target: object) -> None:
        from log_foundry import api

        self.parked = threading.Event()
        self.release = threading.Event()
        real = api._note_orphan_emit

        def note(sink: Any) -> None:
            if sink is target and threading.current_thread().name == "stale":
                self.parked.set()
                self.release.wait(GATE_WAIT * 3)
            real(sink)

        monkeypatch.setattr(api, "_note_orphan_emit", note)


def test_a_no_worker_swap_does_not_arm_a_sink_a_concurrent_shutdown_closed_since_the_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-001, the no-worker branch: the installed sink is closed once, not twice at one count.

    ``lifecycle/p11_swap_arms_a_sink_a_shutdown_closed.py``'s ``forced`` variant in process. The
    change to S is parked after its config write; an orphan log arms S and writes once; a
    ``shutdown()`` on this thread takes S and closes it; the change is then released and, with
    something owed, reaches its no-worker arm. At ``c9068a46`` that arm made S owed again and the
    last ``shutdown()`` closed it a second time at write count 1. The change was published before
    its write, so the first ``shutdown()``'s take marked it withheld and the arm is skipped.
    """
    a, s = Sink("A"), Sink("S")
    log_foundry.configure(service="fr001", sink=a)
    log_foundry.info("a")
    stale_gate = _ParkStaleOrphan(monkeypatch, a)
    stale = threading.Thread(target=log_foundry.info, args=("late",), name="stale", daemon=True)
    stale.start()
    assert stale_gate.parked.wait(GATE_WAIT), "the stale orphan log reached its owed check"
    gate = _ParkAtSwapEntry(monkeypatch, s)
    changer = _start(lambda: log_foundry.configure(service="fr001", sink=s))
    assert gate.parked.wait(GATE_WAIT), "the change reached the window after its config write"
    log_foundry.info("s")
    log_foundry.shutdown(timeout=0.5)
    assert s.at_close == [(1, "MainThread")], f"the premise: the shutdown closed S once: {s}"
    stale_gate.release.set()
    _finish(stale)
    gate.release.set()
    _finish(changer)
    log_foundry.shutdown(timeout=2.0)
    assert s.at_close == [(1, "MainThread")], f"S was closed again with nothing written: {s}"
    assert [count for count, _ in a.at_close] == [1, 2], f"A's closes are p11's: {a}"


def test_a_nested_change_on_the_changing_thread_leaves_the_outer_change_its_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-001, frame-2 review m1: a nested ``configure(sink=X)`` does not take the outer change's op.

    The no-worker window above, with one more step: between its config write and its swap, the
    changing thread makes a nested ``configure(sink=X)``, as a signal handler there would. The
    nested swap used to read and clear the thread's published operation although it installs
    another sink, so the outer swap decided with a fresh operation the stop's take never marked,
    armed S again, and the last ``shutdown()`` closed it a second time at write count 1 (the
    frame-2 reviewer's ``p_steal.py nested-sink``, 5 of 5). The nested swap now leaves an operation
    that is not its own, and S is closed once.
    """
    a, s, x = Sink("A"), Sink("S"), Sink("X")
    log_foundry.configure(service="fr001", sink=a)
    log_foundry.info("a")
    stale_gate = _ParkStaleOrphan(monkeypatch, a)
    stale = threading.Thread(target=log_foundry.info, args=("late",), name="stale", daemon=True)
    stale.start()
    assert stale_gate.parked.wait(GATE_WAIT), "the stale orphan log reached its owed check"
    gate = _ParkAtSwapEntry(monkeypatch, s, nested=lambda: log_foundry.configure(sink=x))
    changer = _start(lambda: log_foundry.configure(service="fr001", sink=s))
    assert gate.parked.wait(GATE_WAIT), "the change reached the window after its config write"
    log_foundry.info("s")
    log_foundry.shutdown(timeout=0.5)
    stale_gate.release.set()
    _finish(stale)
    gate.release.set()
    _finish(changer)
    log_foundry.shutdown(timeout=2.0)
    assert [count for count, _ in s.at_close] == [1], (
        f"S was closed again with nothing written: {s}"
    )


def test_a_retarget_answering_declined_does_not_arm_a_sink_a_concurrent_shutdown_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-001, the ``declined`` branch: the installed sink is closed once.

    The change to B is inside its retarget's fence flush when an orphan log arms B and a
    ``shutdown()`` on another thread, whose wait for the change's decision runs out, stops the
    worker and takes B. The retarget then answers ``declined``; at ``c9068a46`` the change armed B
    anyway and the exit closed it a second time at one write count (SPEC-054's trade, reversed).
    """
    a, b = Sink("A"), Sink("B")
    _worker_on(a)
    verdicts: list[str] = []
    real_retarget = Worker.retarget
    real_flush = Worker.flush
    fired: list[bool] = []
    changing: list[bool] = []

    def flush_then_race(self_: Worker, timeout: float | None = 5.0) -> Any:
        result = real_flush(self_, timeout)
        if changing and not fired:
            fired.append(True)
            log_foundry.info("to b")
            stop = _start(lambda: log_foundry.shutdown(timeout=0.3))
            _finish(stop)
        return result

    def recording(self_: Worker, new_sink: Any, deadline: float | None = None) -> Any:
        outcome = real_retarget(self_, new_sink, deadline)
        if new_sink is b:
            verdicts.append(outcome.verdict)
        return outcome

    monkeypatch.setattr(Worker, "flush", flush_then_race)
    monkeypatch.setattr(Worker, "retarget", recording)
    changing.append(True)
    log_foundry.configure(sink=b)
    assert fired, "the premise: the race ran inside the retarget's first flush"
    assert verdicts == ["declined"], f"the premise: the retarget declined, read {verdicts}"
    _lifecycle._close_owed()
    assert _once(b), f"B was not closed exactly once: {b}"


def _force_fenced(monkeypatch: pytest.MonkeyPatch, a: Sink, b: Sink) -> list[str]:
    """Forces the ``fenced`` choreography: a stop whose wait runs out takes B mid-change.

    A live worker on A; the change to B is parked after its retarget answered; an orphan log
    writes to B and arms it; a ``shutdown()`` on another thread waits out its closer grace for the
    change, stops the worker, takes B and closes it; the change is then released. Shared with
    SPEC-099's ``tests/test_soak_known.py``, which records B's events under the model soak's
    wrappers and holds them to the fixed ``fenced`` record.

    Returns:
      Each verdict the retarget to B answered.
    """
    _worker_on(a)
    verdicts: list[str] = []
    real = Worker.retarget
    release = threading.Event()
    parked = threading.Event()

    def parked_after(self_: Worker, new_sink: Any, deadline: float | None = None) -> Any:
        outcome = real(self_, new_sink, deadline)
        if new_sink is b:
            verdicts.append(outcome.verdict)
            parked.set()
            release.wait(GATE_WAIT * 3)
        return outcome

    monkeypatch.setattr(Worker, "retarget", parked_after)
    swap = _start(lambda: log_foundry.configure(sink=b))
    assert parked.wait(GATE_WAIT), "the swap reached its window"
    log_foundry.info("to b")
    stop = _start(lambda: log_foundry.shutdown(timeout=6.0))
    _finish(stop)
    release.set()
    _finish(swap)
    return verdicts


def test_a_retarget_answering_fenced_keeps_its_arm_and_closes_the_sink_once_after_its_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-001, the ``fenced`` branch: a withheld change keeps its arm, so B is owed after its write.

    The change to B is parked after its retarget answered ``fenced``: the worker already delivers
    to B. An orphan log writes to B and arms it, and a concurrent ``shutdown()`` whose wait for the
    change's decision runs out after its two-second closer grace stops the worker, takes B and
    closes it, which marks the change withheld. Its timeout leaves the stop time to see the drain
    thread end, so B is no longer held when it takes. The change is released and still arms B,
    since on ``fenced`` that arm is the only one the worker's new sink gets: B is owed when
    ``configure()`` returns, and its last close comes after its last write.

    What this row does not claim: B is closed a second time at the same write count by the exit.
    That is the stop whose wait for a change in flight ran out (invariant 5's recorded exception),
    not the window FR-001 closes, and FR-001 keeps the ``fenced`` arm on purpose.
    """
    a, b = Sink("A"), Sink("B")
    verdicts = _force_fenced(monkeypatch, a, b)
    assert verdicts == ["fenced"], f"the premise: the retarget answered fenced, read {verdicts}"
    deadline = time.monotonic() + GATE_WAIT
    while b.closes == 0 and time.monotonic() < deadline:
        time.sleep(0.01)
    assert b.closes == 1, f"the premise: the stop's take closed B before the change decided: {b}"
    assert _lifecycle.owes_close(b), f"the withheld fenced change did not arm B: {b}"
    _lifecycle._close_owed()
    assert b.at_close[-1][0] == b.events, f"B was written after its last close: {b}"


def test_a_stale_take_by_a_second_change_does_not_withhold_the_first_changes_arm(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-001's first bound: only a ``_close_owed`` take withholds, never a swap's stale take.

    Two sink changes run unserialized on the worker path. The first, to B, is parked after its
    config write, holding ``config._sink_lock``, and an orphan log writes to B and arms it. The
    second, to C, gives up waiting for the lock and proceeds alongside: its stale loop takes B and
    closes it, then retargets the worker to C. The first change is released and retargets the
    worker to B. That take was a swap's, not a stop's, so the first change is not withheld and arms
    B; a traced call writes to B again and the exit closes it after that write, so B's closes
    enter at ``[1, 2]``. A withhold keyed on any take marked the change here: on ``fenced`` its arm
    stays regardless, so the mark itself is read, which is what fails for that mutant.
    """
    from log_foundry import worker as worker_module

    a, b, c = Sink("A"), Sink("B"), Sink("C")
    _worker_on(a)
    published: list[_lifecycle._Operation] = []
    real_open = _lifecycle.open_command_swap

    def recording(budget: float | None, installing: Any, change: Any) -> _lifecycle._Operation:
        op = real_open(budget, installing, change)
        if installing is b:
            published.append(op)
        return op

    monkeypatch.setattr(_lifecycle, "open_command_swap", recording)
    gate = _ParkAtSwapEntry(monkeypatch, b)
    first = _start(lambda: log_foundry.configure(sink=b))
    assert gate.parked.wait(GATE_WAIT), "the first change reached its window"
    log_foundry.info("b once")
    assert _lifecycle.owes_close(b), "the premise: the orphan log armed B"
    monkeypatch.setattr(worker_module, "DEFAULT_SWAP_TIMEOUT", 0.2)
    log_foundry.configure(sink=c)
    deadline = time.monotonic() + GATE_WAIT
    while not b.at_close and time.monotonic() < deadline:
        time.sleep(0.01)
    assert [count for count, _ in b.at_close] == [1], f"the premise: a stale take closed B: {b}"
    assert published and not published[0].withheld, "a swap's stale take withheld the change"
    gate.release.set()
    _finish(first)
    _traced()
    log_foundry.shutdown(timeout=5.0)
    assert b.events > 1, f"the premise: B was written again after its close: {b}"
    assert [count for count, _ in b.at_close] == [1, b.events], f"B's closes: {b}"


def test_a_shutdown_merged_into_a_running_stop_moves_no_counter() -> None:
    """SPEC-097 FR-002: a ``shutdown()`` from a sink's ``close()`` during a stop is merged.

    It returns at once and does nothing else: the running stop has already moved the retirement
    count, set the stop signal and will close what is owed. So ``retirements`` and every term of
    the event ledger read the same just after the inner call as just before it, which a merged
    call that re-ran any part of a stop would move (invariant 2).
    """

    def scenario() -> str:
        class ShutsDownInClose(Sink):
            reading: str = ""

            def close(self) -> None:
                before = (_lifecycle._state.retirements, repr(_lifecycle._ledger_snapshot()))
                started = time.monotonic()
                log_foundry.shutdown(timeout=None)
                took = time.monotonic() - started
                after = (_lifecycle._state.retirements, repr(_lifecycle._ledger_snapshot()))
                self.reading = f"same={before == after} quick={took < 0.5}"
                super().close()

        s = ShutsDownInClose("S")
        log_foundry.configure(service="merged", sink=s)
        log_foundry.info("one")
        log_foundry.shutdown(timeout=5.0)
        return f"{s.reading} closes={s.closes} retirements={_lifecycle._state.retirements}"

    child = run_in_child(scenario)
    assert child.output == "same=True quick=True closes=1 retirements=1", child.output


def test_every_published_change_drops_its_sink_once_its_configure_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-001: a change published before its write references no sink once ``configure()`` returns.

    A published operation that kept ``installing`` would hold every superseded sink alive past its
    change, which SPEC-081's bounded ownership record refuses; ``close_command_swap`` clears it.
    Every operation ``configure(sink=...)`` publishes is recorded, on both paths and across a
    re-entrant-free sequence of changes, and read once each call has returned.
    """
    published: list[_lifecycle._Operation] = []
    real = _lifecycle.open_command_swap

    def recording(budget: float | None, installing: Any, change: Any) -> _lifecycle._Operation:
        op = real(budget, installing, change)
        published.append(op)
        return op

    monkeypatch.setattr(_lifecycle, "open_command_swap", recording)
    first = Sink("first")
    log_foundry.configure(service="published", sink=first)
    log_foundry.info("orphan")
    log_foundry.configure(sink=Sink("second"))
    _traced()
    log_foundry.flush(timeout=5.0)
    log_foundry.configure(sink=Sink("third"))
    log_foundry.configure(sink=Sink("fourth"))
    assert len(published) == 4, published
    kept = [op for op in published if op.installing is not None]
    assert not kept, f"{len(kept)} published change(s) still reference a sink after returning"
    assert all(op.decided.is_set() for op in published)
    assert not _lifecycle._state._swaps_running
