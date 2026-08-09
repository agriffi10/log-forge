"""SPEC-035 — the shutdown paths SPEC-033 regressed, and the idempotent call that never waited."""

import ast
import inspect
import textwrap
import threading
import time

import pytest

log_foundry = pytest.importorskip("log_foundry")
decorator = pytest.importorskip("log_foundry.decorator")
worker_mod = pytest.importorskip("log_foundry.worker")


class CountingSink:
    """Holds events until closed, so a lost close is a lost event."""

    def __init__(self, name: str = "sink") -> None:
        self.name = name
        self.held: list[dict] = []
        self.delivered: list[dict] = []
        self.closed = 0
        self.stop_signal: threading.Event | None = None

    def emit(self, batch: list[dict]) -> None:
        self.held.extend(batch)

    def close(self) -> None:
        self.delivered.extend(self.held)
        self.held = []
        self.closed += 1


class BackoffSink(CountingSink):
    """Backs off inside ``emit``, re-reading ``stop_signal`` the way a retrying sink does.

    The re-read is the whole point. ``sinks/_retry.wait`` consults ``self.stop_signal`` once per
    attempt, so a signal swapped out *during* a backoff is the one the next attempt waits on — a
    sink that captured the event at entry could not observe FR-001 at all.
    """

    def __init__(self, backoff: float = 20.0) -> None:
        super().__init__()
        self.backoff = backoff
        self.in_emit = threading.Event()
        self.may_wait = threading.Event()
        self.cut_short: bool | None = None
        self.signal_when_waiting: threading.Event | None = None
        self._backing_off = False

    def emit(self, batch: list[dict]) -> None:
        super().emit(batch)
        if self._backing_off:
            return  # only the drain thread's first emit backs off; an orphan log is not gated
        self._backing_off = True
        self.in_emit.set()
        self.may_wait.wait(5.0)
        signal = self.stop_signal
        self.signal_when_waiting = signal
        self.cut_short = bool(signal is not None and signal.wait(self.backoff))


def _trace_once() -> None:
    @log_foundry.trace
    def work() -> None:
        return None

    work()


# --- FR-001: the stop signal is offered on ownership, not liveness ---------------------------


def test_the_offer_does_not_consult_liveness() -> None:
    """AC-1. ``_live_worker()`` returns None the instant ``retired`` latches, which is *entry* to
    ``shutdown``, so consulting it here un-skips the offer for the whole of the drain.

    Walked as an AST rather than grepped: both function names appear in this function's own
    docstring explaining the distinction, so a substring check over ``getsource`` asserts the
    prose and would fail against a correct implementation.
    """
    tree = ast.parse(textwrap.dedent(inspect.getsource(decorator._offer_orphan_signal)))
    called = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert "_live_worker" not in called, "the offer must key on the moment, not on liveness"


def test_an_orphan_log_during_the_drain_leaves_the_stop_signal_alone() -> None:
    """AC-2. The drain thread is about to wait on ``worker._stop``; an orphan log must not
    replace it with a fresh unset event."""
    sink = BackoffSink()
    log_foundry.configure(service="t", sink=sink)
    _trace_once()
    worker = decorator._worker
    assert worker is not None
    assert sink.stop_signal is worker._stop, "the worker owns this sink's signal to begin with"

    shutting_down = threading.Thread(target=lambda: log_foundry.shutdown(timeout=3.0))
    try:
        assert sink.in_emit.wait(5.0), "the drain thread never reached emit"
        shutting_down.start()
        while not worker.retired:  # the latch is entry to shutdown, not its completion
            time.sleep(0.001)

        log_foundry.info("an orphan log while the drain is in flight")

        assert sink.stop_signal is worker._stop, (
            "an orphan log replaced the signal the drain thread is about to wait on"
        )
    finally:
        sink.may_wait.set()
        shutting_down.join(10.0)


def test_an_orphan_log_during_the_drain_does_not_strand_the_backoff() -> None:
    """AC-3. End to end: the backoff is still cut short and the shutdown still completes.

    The backoff-to-budget **gap** is what is kept wide (20 s against 3 s), not the budget kept
    tight — a tight budget fails on its own setup under load rather than on the defect.
    """
    sink = BackoffSink(backoff=20.0)
    log_foundry.configure(service="t", sink=sink)
    _trace_once()
    worker = decorator._worker
    assert worker is not None

    outcome: list[bool] = []
    shutting_down = threading.Thread(
        target=lambda: outcome.append(log_foundry.shutdown(timeout=3.0) is not False)
    )
    try:
        assert sink.in_emit.wait(5.0)
        shutting_down.start()
        while not worker.retired:
            time.sleep(0.001)
        log_foundry.info("an orphan log while the drain is in flight")
    finally:
        sink.may_wait.set()
        shutting_down.join(30.0)

    assert not shutting_down.is_alive(), "shutdown never returned"
    assert sink.cut_short is True, "the sink's backoff ran to its full length"
    assert sink.signal_when_waiting is worker._stop, (
        "the sink waited on a signal the shutdown does not set"
    )
    assert log_foundry.health().stopped_reason is None, (
        "the shutdown expired, which is SPEC-027's global pause reintroduced"
    )


def test_a_sink_adopted_after_a_retired_worker_still_receives_a_signal() -> None:
    """AC-4. SPEC-033 FR-004 AC-4's second case: the ownership guard must not skip a sink the
    retired worker does *not* hold, or this fix re-breaks what that spec fixed."""
    first = CountingSink("A")
    log_foundry.configure(service="t", sink=first)
    _trace_once()
    log_foundry.shutdown(timeout=5.0)

    second = CountingSink("B")
    log_foundry.configure(service="t", sink=second)
    log_foundry.info("an orphan log against a sink no live worker owns")

    assert second.stop_signal is not None, (
        "a sink adopted after shutdown got no stop signal, so SPEC-027's guarantee is false here"
    )
    assert second.stop_signal is not first.stop_signal, "it must not inherit the retired signal"


# --- FR-004: the idempotent shutdown waits for the drain it found ----------------------------


class SlowEmitSink(CountingSink):
    """Takes a measurable time inside ``emit``, so a drain in flight is observable."""

    def __init__(self, emit_seconds: float = 2.0) -> None:
        super().__init__()
        self.emit_seconds = emit_seconds
        self.in_emit = threading.Event()

    def emit(self, batch: list[dict]) -> None:
        self.in_emit.set()
        time.sleep(self.emit_seconds)
        super().emit(batch)


class WedgedSink(CountingSink):
    """Blocks in ``emit`` until the test releases it, ignoring the stop signal entirely.

    A sink that honours the signal cannot make a shutdown expire once FR-001 is fixed — the
    backoff is cut and the drain finishes — so the expired path needs a sink that does not.
    """

    def __init__(self) -> None:
        super().__init__()
        self.in_emit = threading.Event()
        self.release = threading.Event()

    def emit(self, batch: list[dict]) -> None:
        self.in_emit.set()
        self.release.wait(30.0)
        super().emit(batch)


def test_a_second_shutdown_waits_for_the_drain_already_running() -> None:
    """AC-1. The common shape is not two user calls: it is a ``shutdown()`` on one thread and
    ``atexit`` on the main thread, which returned in under a millisecond and delivered nothing."""
    sink = SlowEmitSink(emit_seconds=1.5)
    log_foundry.configure(service="t", sink=sink)
    for _ in range(3):
        _trace_once()

    worker = decorator._worker
    assert worker is not None
    first = threading.Thread(target=lambda: log_foundry.shutdown(timeout=30.0))
    first.start()
    assert sink.in_emit.wait(5.0), "the first shutdown's drain never reached the sink"

    log_foundry.shutdown(timeout=30.0)  # the second caller: must not overtake the drain
    emitted_on_return = len(sink.held) + len(sink.delivered)  # close() moves held into delivered

    assert worker._drain_finished.is_set(), (
        "the second shutdown returned while the drain it found was still running"
    )
    assert emitted_on_return, (
        "and the sink's in-flight emit had not completed either — an observable independent of "
        "the flag the fix waits on, so this cannot pass by mirroring the implementation"
    )
    first.join(30.0)
    assert sink.closed == 1, "the sink must be closed exactly once"
    assert sink.delivered, "and everything submitted before the shutdown must have landed"


def test_a_second_shutdown_is_still_bounded_by_its_own_budget() -> None:
    """AC-2. The wait is the caller's budget, not the drain's — ``timeout=0`` returns promptly."""
    sink = SlowEmitSink(emit_seconds=3.0)
    log_foundry.configure(service="t", sink=sink)
    _trace_once()

    first = threading.Thread(target=lambda: log_foundry.shutdown(timeout=30.0))
    first.start()
    assert sink.in_emit.wait(5.0)

    start = time.monotonic()
    log_foundry.shutdown(timeout=0)
    elapsed = time.monotonic() - start

    assert elapsed < 1.0, f"a zero-budget second shutdown waited {elapsed:.2f}s for the drain"
    first.join(30.0)


def test_a_second_shutdown_after_the_first_completed_returns_immediately() -> None:
    """AC-3. Idempotency is not traded for correctness: a finished drain is waited on for zero."""
    sink = CountingSink()
    log_foundry.configure(service="t", sink=sink)
    _trace_once()
    log_foundry.shutdown(timeout=10.0)

    start = time.monotonic()
    log_foundry.shutdown(timeout=30.0)
    elapsed = time.monotonic() - start

    assert elapsed < 0.5, f"the idempotent path took {elapsed:.2f}s over a finished drain"
    assert sink.closed == 1, "and it did not close the sink a second time"


def test_an_expired_first_shutdown_does_not_make_the_second_wait() -> None:
    """AC-5. The drain thread is wedged and nothing will release it, so waiting on
    ``_drain_finished`` would spend the whole second budget before an exit that must happen."""
    sink = WedgedSink()
    log_foundry.configure(service="t", sink=sink)
    _trace_once()
    worker = decorator._worker
    assert worker is not None

    try:
        assert sink.in_emit.wait(5.0)
        log_foundry.shutdown(timeout=0.5)  # expires: the sink ignores the stop signal
        assert worker.stopped_reason == "ShutdownTimeout", "the first shutdown did not expire"

        start = time.monotonic()
        log_foundry.shutdown(timeout=30.0)
        elapsed = time.monotonic() - start

        assert elapsed < 1.0, (
            f"the second shutdown waited {elapsed:.2f}s on a drain the first one abandoned"
        )
    finally:
        sink.release.set()


def test_the_closer_grace_is_granted_exactly_once_across_both_calls(monkeypatch) -> None:
    """AC-4. Asserted by counting the joins, not by timing them — a gauge that has already
    unwound, or an elapsed time, is the observable that fails to hold here."""
    lifecycle = pytest.importorskip("log_foundry._lifecycle")
    joins: list[float | None] = []
    real = lifecycle.join_closers
    monkeypatch.setattr(
        lifecycle, "join_closers", lambda t: (joins.append(t), real(t))[1], raising=True
    )

    sink = SlowEmitSink(emit_seconds=1.0)
    log_foundry.configure(service="t", sink=sink)
    _trace_once()

    first = threading.Thread(target=lambda: log_foundry.shutdown(timeout=30.0))
    first.start()
    assert sink.in_emit.wait(5.0)
    log_foundry.shutdown(timeout=30.0)
    first.join(30.0)

    assert len(joins) == 2, (
        f"both callers must reach the grace exactly once each, got {len(joins)}"
    )
    assert all(t is None or t >= 0 for t in joins), "and each with a non-negative budget"


# --- FR-003: a swap racing a shutdown leaves its sink owned ----------------------------------


def _shutdown_inside_the_first_flush(worker) -> threading.Thread:
    """Opens the preemption point AC-3 requires, inside ``swap_sink``'s first ``flush()``.

    The window is a few instructions wide — between ``_swap_sink`` reading a live worker and
    ``Worker.swap_sink`` re-checking retirement after its drain — so it is opened deliberately
    rather than raced for. A **real** ``shutdown()`` runs in it rather than the latch alone:
    latching ``_shutdown_done`` by hand declines the swap identically, but leaves the drain
    thread alive, so ``_close_if_owed`` declines and the old sink is never closed by anyone —
    which would make AC-2 unobservable and read as a defect it is not.
    """
    real_flush = worker.flush
    shutting_down = threading.Thread(target=lambda: log_foundry.shutdown(timeout=30.0))

    def flush_then_shut_down(timeout=None):
        result = real_flush(timeout)
        worker.flush = real_flush
        shutting_down.start()
        while not worker.retired:
            time.sleep(0.001)
        return result

    worker.flush = flush_then_shut_down
    return shutting_down


def test_a_swap_declined_mid_shutdown_leaves_its_sink_owned() -> None:
    """AC-1. B is closed exactly once by the time the process exits.

    Counted against the sink rather than timed: a gauge that has already unwound, or an elapsed
    time, is the observable that does not hold for a close.
    """
    old = CountingSink("A")
    log_foundry.configure(service="t", sink=old)
    _trace_once()
    worker = decorator._worker
    assert worker is not None and worker.sink is old

    shutting_down = _shutdown_inside_the_first_flush(worker)
    new = CountingSink("B")
    log_foundry.configure(service="t", sink=new)  # the worker declines mid-swap
    shutting_down.join(60.0)

    assert worker.sink is old, "the swap really was declined, or this test proves nothing"

    decorator._close_orphan_sink()  # the exit path, and a no-op if the shutdown got there first
    assert new.closed == 1, f"B was closed {new.closed} times, not once"


def test_the_old_sink_is_not_closed_by_both_paths() -> None:
    """AC-2. A is still closed exactly once — the worker holds it, so the orphan path must not."""
    old = CountingSink("A")
    log_foundry.configure(service="t", sink=old)
    _trace_once()
    worker = decorator._worker
    assert worker is not None

    shutting_down = _shutdown_inside_the_first_flush(worker)
    new = CountingSink("B")
    log_foundry.configure(service="t", sink=new)
    shutting_down.join(60.0)

    decorator._close_orphan_sink()
    assert old.closed == 1, f"A was closed {old.closed} times, not once"
    assert new.closed == 1, f"B was closed {new.closed} times, not once"


def test_an_adopted_swap_still_reports_true_when_its_drain_is_unconfirmed() -> None:
    """The verdict is about **ownership**, not the quality of the drain (AC-4's boundary).

    An unconfirmed drain leaves the old sink open and counts ``incomplete_swaps``, but the swap
    itself happened — so returning False there would re-home a sink the worker is delivering to,
    which is the opposite of this FR.
    """
    old = CountingSink("A")
    log_foundry.configure(service="t", sink=old)
    _trace_once()
    worker = decorator._worker
    assert worker is not None

    worker.flush = lambda timeout=None: False  # the drain cannot be confirmed
    new = CountingSink("B")
    assert worker.swap_sink(new, 1.0) is True, "an unconfirmed drain is still an adopted swap"
    assert worker.sink is new
    assert worker.health().incomplete_swaps == 1
