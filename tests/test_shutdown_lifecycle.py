"""SPEC-035 — the shutdown paths SPEC-033 regressed, and the idempotent call that never waited."""

import ast
import inspect
import textwrap
import threading
import time

import pytest

import log_foundry
from log_foundry import _lifecycle


class CountingSink:
    """Holds events until closed, so a lost close is a lost event."""

    def __init__(self, name: str = "sink") -> None:
        self.name = name
        self.held: list[dict] = []
        self.delivered: list[dict] = []
        self.closed = 0
        self.log_foundry_stop_signal: threading.Event | None = None

    def emit(self, batch: list[dict]) -> None:
        self.held.extend(batch)

    def close(self) -> None:
        self.delivered.extend(self.held)
        self.held = []
        self.closed += 1


class BackoffSink(CountingSink):
    """Backs off inside ``emit``, re-reading ``log_foundry_stop_signal`` the way a retrying sink
    does.

    The re-read is the whole point. ``sinks/_retry.wait`` consults ``self.log_foundry_stop_signal``
    once per
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
        signal = self.log_foundry_stop_signal
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
    tree = ast.parse(textwrap.dedent(inspect.getsource(_lifecycle._offer_orphan_signal)))
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
    worker = _lifecycle._state._worker
    assert worker is not None
    assert sink.log_foundry_stop_signal is worker._stop, "the worker owns this sink's signal to begin with"

    shutting_down = threading.Thread(target=lambda: log_foundry.shutdown(timeout=3.0))
    try:
        assert sink.in_emit.wait(5.0), "the drain thread never reached emit"
        shutting_down.start()
        while not log_foundry.health().retired:  # entry to shutdown, not its completion
            time.sleep(0.001)

        log_foundry.info("an orphan log while the drain is in flight")

        assert sink.log_foundry_stop_signal is worker._stop, (
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
    worker = _lifecycle._state._worker
    assert worker is not None

    outcome: list[bool] = []
    shutting_down = threading.Thread(
        target=lambda: outcome.append(log_foundry.shutdown(timeout=3.0) is not False)
    )
    try:
        assert sink.in_emit.wait(5.0)
        shutting_down.start()
        while not log_foundry.health().retired:
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

    assert second.log_foundry_stop_signal is not None, (
        "a sink adopted after shutdown got no stop signal, so SPEC-027's guarantee is false here"
    )
    assert second.log_foundry_stop_signal is not first.log_foundry_stop_signal, "it must not inherit the retired signal"


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

    worker = _lifecycle._state._worker
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
    worker = _lifecycle._state._worker
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


def test_the_closer_grace_is_granted_exactly_once_per_call(monkeypatch) -> None:
    """AC-4. Asserted by counting the joins, not by timing them — a gauge that has already
    unwound, or an elapsed time, is the observable that fails to hold here."""
    from log_foundry import _lifecycle as lifecycle
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
    which would make AC-2 unobservable and read as a defect it is not. The wait reads
    ``health().retired``, the public observable, rather than a worker attribute: SPEC-054 FR-001
    moved the latch onto the lifecycle owner as a count, and entry to ``shutdown()`` is still
    what moves it.
    """
    real_flush = worker.flush
    shutting_down = threading.Thread(target=lambda: log_foundry.shutdown(timeout=30.0))

    def flush_then_shut_down(timeout=None):
        result = real_flush(timeout)
        worker.flush = real_flush
        shutting_down.start()
        while not log_foundry.health().retired:
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
    worker = _lifecycle._state._worker
    assert worker is not None and worker.sink is old

    shutting_down = _shutdown_inside_the_first_flush(worker)
    new = CountingSink("B")
    log_foundry.configure(service="t", sink=new)  # the worker declines mid-swap
    shutting_down.join(60.0)

    assert worker.sink is old, "the swap really was declined, or this test proves nothing"
    assert _lifecycle._state._owed.get(id(new)) is new or new.closed == 1, (
        "the declined sink is neither closed nor armed for the exit handler — owned by nobody"
    )


def test_the_old_sink_is_not_closed_by_both_paths() -> None:
    """AC-2. A is still closed exactly once — the worker holds it, so the orphan path must not."""
    old = CountingSink("A")
    log_foundry.configure(service="t", sink=old)
    _trace_once()
    worker = _lifecycle._state._worker
    assert worker is not None

    shutting_down = _shutdown_inside_the_first_flush(worker)
    new = CountingSink("B")
    log_foundry.configure(service="t", sink=new)
    shutting_down.join(60.0)

    _lifecycle._close_owed()
    assert old.closed == 1, f"A was closed {old.closed} times, not once"
    assert new.closed == 1, f"B was closed {new.closed} times, not once"


def test_an_adopted_swap_still_reports_true_when_its_drain_is_unconfirmed() -> None:
    """The verdict is about **ownership**, not the quality of the drain (AC-4's boundary).

    An unconfirmed drain leaves the old sink open *for now* — until a ``shutdown()`` that finds
    the drain thread ended closes it (SPEC-050 FR-004) — and counts ``incomplete_swaps``, but the
    swap itself happened, so returning False there would re-home a sink the worker is delivering
    to, which is the opposite of this FR.
    """
    old = CountingSink("A")
    log_foundry.configure(service="t", sink=old)
    _trace_once()
    worker = _lifecycle._state._worker
    assert worker is not None

    worker.flush = lambda timeout=None: False  # the drain cannot be confirmed
    new = CountingSink("B")
    assert worker.retarget(new, time.monotonic() + 1.0).verdict == "unfenced", (
        "an unconfirmed drain is still an adopted swap"
    )
    assert worker.sink is new
    assert worker.health().incomplete_swaps == 1


# --- found by independent review of this PR --------------------------------------------------


def test_a_declined_swap_re_arms_a_sink_already_closed_and_that_is_the_trade() -> None:
    """SPEC-054 FR-002 retires the closed-sink latch, and this is the one place it cost a close.

    ~~The re-arm guard `_note_orphan_emit` carries, which `_adopt_declined_swap` first
    omitted.~~ — struck (SPEC-021). The scenario is unchanged and all three actors are ordinary:
    an orphan log arms the sink while a `configure()` is inside the swap's first `flush()`, a
    `shutdown()` closes it, and the declining swap then arms it again.

    The latch refused that second arming. It is gone, so B is closed twice — and unlike the other
    two counts SPEC-054 raised, **nothing was written between the two closes**. It is the case
    FR-002 enumerates rather than a write-epoch double: *a live target is closed at exit whether
    or not anything was written since it was installed*, which is the worker path's rule.

    Not arming on a decline was tried, and it costs SPEC-035 FR-003's guarantee that a declined
    sink is owned by somebody — three tests in this file hold it. So the redundant close is the
    accepted trade, and `sinks/base.py` asks an implementation for an idempotent `close()` for
    exactly this.
    """
    old = CountingSink("A")
    log_foundry.configure(service="t", sink=old)
    _trace_once()
    worker = _lifecycle._state._worker
    assert worker is not None

    new = CountingSink("B")
    real_flush = worker.flush

    def flush_then_arm_and_shut_down(timeout=None):
        result = real_flush(timeout)
        worker.flush = real_flush
        log_foundry.info("an orphan log that arms B while the swap is in flight")
        assert _lifecycle._state._owed.get(id(new)) is new, (
            "the orphan log must have armed B"
        )
        log_foundry.shutdown(timeout=30.0)  # closes B and records it closed
        return result

    worker.flush = flush_then_arm_and_shut_down
    log_foundry.configure(service="t", sink=new)  # declines, then must not re-arm B

    _lifecycle._close_owed()
    assert new.closed == 2, (
        f"B was closed {new.closed} times: once by the racing shutdown, once as the live target "
        "the swap installed — the trade this test's docstring records"
    )


def test_an_abandoning_first_shutdown_releases_the_second_callers_wait() -> None:
    """FR-004 AC-5 in the **concurrent** ordering, which the serial test cannot reach.

    The abandonment is recorded by the first caller *after* its join expires, so a second caller
    that evaluated `draining` on entry has already committed to the wait. Measured before the
    fix: a second `shutdown(timeout=20)` returned after 20.01 s, and with `timeout=None` the
    process never exited.
    """
    sink = WedgedSink()
    log_foundry.configure(service="t", sink=sink)
    _trace_once()

    try:
        assert sink.in_emit.wait(5.0)
        first = threading.Thread(target=lambda: log_foundry.shutdown(timeout=1.0))
        first.start()

        start = time.monotonic()
        log_foundry.shutdown(timeout=20.0)  # entered while the first is still joining
        elapsed = time.monotonic() - start

        assert elapsed < 5.0, (
            f"the second shutdown waited {elapsed:.2f}s on a drain the first one abandoned "
            f"while it was already inside the wait"
        )
        first.join(30.0)
    finally:
        sink.release.set()


def test_a_declined_swaps_sink_is_closed_by_the_time_the_process_exits() -> None:
    """FR-003 AC-1's actual wording, which only a real interpreter exit can demonstrate.

    The in-process tests can show the sink is armed, but the arming and the close both run
    through functions the test could call itself. Here nothing is invoked by hand: the child
    exits normally and the library's own ``atexit`` handler is the only thing that could close B.
    """
    import subprocess
    import sys

    program = textwrap.dedent(
        """
        import sys, threading
        sys.path.insert(0, "src")
        import log_foundry
        from log_foundry import _lifecycle

        class S:
            def __init__(self, n): self.n = n; self.closed = 0; self.log_foundry_stop_signal = None
            def emit(self, b): pass
            def close(self): self.closed += 1; print(f"CLOSED {self.n}", flush=True)

        A, B = S("A"), S("B")
        log_foundry.configure(service="t", sink=A)

        @log_foundry.trace
        def w(): pass
        w()

        worker = _lifecycle._state._worker
        real = worker.flush
        def flush_then_shut_down(timeout=None):
            r = real(timeout)
            worker.flush = real
            t = threading.Thread(target=lambda: log_foundry.shutdown(timeout=30.0))
            t.start()
            while not log_foundry.health().retired:
                pass
            t.join(30.0)
            return r
        worker.flush = flush_then_shut_down

        log_foundry.configure(service="t", sink=B)   # declines mid-shutdown
        print("EXITING", flush=True)
        """
    )
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", program], capture_output=True, text=True, timeout=120
    )
    assert "EXITING" in result.stdout, f"the child never got there: {result.stderr[-800:]}"
    assert result.stdout.count("CLOSED B") == 1, (
        f"B was closed {result.stdout.count('CLOSED B')} times at exit, not once:\n"
        f"{result.stdout}\n{result.stderr[-800:]}"
    )
    assert result.stdout.count("CLOSED A") == 1, f"and A exactly once:\n{result.stdout}"


# -- SPEC-050 FR-002: the orphan path's half of the in-flight-close wait -----------------


def test_an_orphan_only_second_shutdown_waits_for_the_close_in_flight() -> None:
    """FR-002 AC-3. The orphan closer had the worker's shape, and the same residue.

    It empties `_owed` under `_state._lock` and *then* closes, so a second caller took the
    early return while the first was still inside an unbounded `close()`. In a process that only
    ever logged outside a span there is no worker, so this function is the *only* thing that
    closes the sink — which makes an `atexit` call returning through a running close total loss
    of a close-is-delivery sink's buffer.
    """
    class _CloseIsDelivery:
        """Buffers on emit; the close is the delivery, and it is slow enough to be raced."""

        def __init__(self) -> None:
            self.held: list[dict] = []
            self.wire: list[dict] = []
            self.closed = 0
            self.in_close = threading.Event()
            self.log_foundry_stop_signal: threading.Event | None = None

        def emit(self, batch: list[dict]) -> None:
            """Buffers the batch without delivering it."""
            self.held.extend(batch)

        def close(self) -> None:
            """Delivers, slowly, so a caller returning through it is measurable."""
            self.in_close.set()
            time.sleep(0.6)
            self.wire.extend(self.held)
            self.closed += 1

    sink = _CloseIsDelivery()
    log_foundry.configure(service="test", sink=sink)
    log_foundry.info("orphan")  # no span: no worker is ever built
    assert sink.held, "the premise: the orphan emit reached the sink"

    first = threading.Thread(target=lambda: log_foundry.shutdown(timeout=30.0))
    first.start()
    assert sink.in_close.wait(5.0), "the premise: the first caller is inside close()"

    log_foundry.shutdown(timeout=30.0)

    assert sink.closed == 1, "the second caller waited rather than closing again"
    assert sink.wire == sink.held, "and the close it waited for delivered the buffer"
    first.join(timeout=5)


def test_an_orphan_only_shutdown_with_no_close_in_flight_does_not_wait() -> None:
    """FR-002 AC-6, on the orphan path. `waiting` is captured before the slot is written.

    Read after the write instead, the caller that took the work would see its own event and wait
    out the whole closer grace on something it is itself responsible for setting — and since
    `_shutdown_worker` calls this on the *worker* path too, that is a stall on every ordinary
    shutdown, not just an orphan one.
    """
    class _Quiet:
        def __init__(self) -> None:
            self.closed = 0
            self.log_foundry_stop_signal: threading.Event | None = None

        def emit(self, batch: list[dict]) -> None:
            """Accepts a batch."""

        def close(self) -> None:
            """Releases nothing, instantly."""
            self.closed += 1

    sink = _Quiet()
    log_foundry.configure(service="test", sink=sink)
    log_foundry.info("orphan")

    start = time.monotonic()
    log_foundry.shutdown(timeout=30.0)
    log_foundry.shutdown(timeout=30.0)
    # A THIRD call is what catches an arming write made unconditionally: the second leaves a
    # permanently unset event behind it, and only a later caller reads it. Two shutdowns pass
    # against that bug.
    log_foundry.shutdown(timeout=30.0)
    elapsed = time.monotonic() - start

    assert sink.closed == 1
    assert elapsed < _lifecycle.DEFAULT_CLOSER_GRACE, (
        f"three orphan shutdowns took {elapsed:.2f}s with nothing to wait for"
    )


def test_a_second_closer_waits_for_a_close_it_did_not_start_even_while_taking_work() -> None:
    """SPEC-050 FR-002's property, strengthened by SPEC-054 FR-003's per-sink registrations.

    ~~The orphan record is a count and a gate, because it is not once-only. A single slot held
    one close's event, so a second orphan close overwrote it and its own completion then cleared
    it — a bystander arriving afterwards read nothing and waited for nothing.~~ — struck
    (SPEC-021). Measured on that shape: the bystander waited 1.005 s with one close and 0.000 s
    with a second completing in between, losing the first sink's whole buffer.

    That scenario can no longer be built, and that is the point: a close is registered against the
    **sink** it is closing, so a second caller cannot overwrite a first caller's record. What is
    asserted instead is the stronger property that makes it unbuildable — a caller waits on every
    registration that is not its own, **including one that also took work of its own**. So the
    caller closing the second sink does not return until the first sink's close has finished, and
    the first sink's buffer reaches the wire.
    """
    class _Slow:
        def __init__(self, seconds: float) -> None:
            self.seconds = seconds
            self.held = 0
            self.wire = 0
            self.closed = 0
            self.in_close = threading.Event()
            self.log_foundry_stop_signal: threading.Event | None = None

        def emit(self, batch: list[dict]) -> None:
            """Buffers; the close is the delivery."""
            self.held += len(batch)

        def close(self) -> None:
            """Delivers after a measurable delay."""
            self.in_close.set()
            time.sleep(self.seconds)
            self.wire, self.held = self.wire + self.held, 0
            self.closed += 1

    first, second = _Slow(1.0), _Slow(0.05)
    log_foundry.configure(service="test", sink=first)
    for _ in range(5):
        log_foundry.info("x")

    closer = threading.Thread(target=_lifecycle._close_owed, daemon=True)
    closer.start()
    assert first.in_close.wait(5.0), "the premise: the first close is running"

    log_foundry.configure(service="test", sink=second)
    log_foundry.info("y")
    start = time.monotonic()
    _lifecycle._close_owed()  # closes the second sink, and waits on the first
    waited = time.monotonic() - start

    assert second.closed == 1, "the premise: this caller had work of its own to do"
    assert waited > 0.3, (
        f"it returned in {waited:.3f}s, so it returned through a close it did not start"
    )
    assert first.wire == 5 and first.closed == 1, f"the first sink lost its buffer: {first.wire}"
    closer.join(5)


def test_a_bystander_does_not_release_the_gate_for_the_next_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-002. Only a caller that took work releases the count, so bystanders do not stack.

    A bystander that decremented on its way out would drive the count to zero and set the gate
    while the real close is still running, so the *second* bystander returns immediately. One
    bystander cannot see that, because its own release happens after its own wait; it takes two.

    The close has to outlast **both** waits or the test measures the grace rather than the guard:
    with a close shorter than one grace, the first bystander waits it out legitimately and the
    second one finds it finished, which is correct behaviour and indistinguishable from the bug.
    The grace is shortened rather than the close lengthened so this costs a second, not ten.
    """
    monkeypatch.setattr(_lifecycle, "DEFAULT_CLOSER_GRACE", 0.3)
    class _Slow:
        def __init__(self) -> None:
            self.closed = 0
            self.in_close = threading.Event()
            self.log_foundry_stop_signal: threading.Event | None = None

        def emit(self, batch: list[dict]) -> None:
            """Accepts a batch."""

        def close(self) -> None:
            """Runs longer than two shortened bystander waits put together."""
            self.in_close.set()
            time.sleep(1.5)
            self.closed += 1

    sink = _Slow()
    log_foundry.configure(service="test", sink=sink)
    log_foundry.info("orphan")

    closer = threading.Thread(target=_lifecycle._close_owed, daemon=True)
    closer.start()
    assert sink.in_close.wait(5.0), "the premise: the close is running"

    _lifecycle._close_owed()  # first bystander
    start = time.monotonic()
    _lifecycle._close_owed()  # second: must still find the close in flight
    waited = time.monotonic() - start

    assert waited > 0.1, (
        f"the second bystander returned in {waited:.3f}s, so the first released a count it "
        f"never took"
    )
    closer.join(5)


def test_an_interrupt_after_the_take_does_not_leak_a_close_registration() -> None:
    """FR-002. A leaked in-flight count is permanent, so the take is inside the `try`.

    `KeyboardInterrupt` is delivered asynchronously at a bytecode boundary, so one landing between
    the increment and the `try` left the count raised and the gate clear for the life of the
    process — every later caller then paying the whole grace. Measured on the unguarded shape: a
    real `SIGINT` storm leaked once every few hundred iterations. Simulated here at the point
    rather than raced for, because a storm test passes against the bug most of the time.

    `BaseException`, not `Exception`: SPEC-025 requires the interrupt to reach the caller, so the
    claim is that it reaches the caller *and* leaves the count clean.

    **What this cannot reach, stated rather than implied.** It injects at a call site, so it
    covers an interrupt anywhere in the body. The window that produced the finding is narrower —
    the few bytecodes between the increment and the `try:`, which contain no call at all — so
    moving the `try:` back below the lock survives this test. That mutant is only killable by a
    real `SIGINT` storm, which leaked once every few hundred iterations before the fix and not
    once in 21,000 after it. A storm is not in the suite because it cannot report through its own
    interrupts; the placement is held by review and by this test's weaker cousin.
    """
    class _Quiet:
        def __init__(self) -> None:
            self.closed = 0
            self.log_foundry_stop_signal: threading.Event | None = None

        def emit(self, batch: list[dict]) -> None:
            """Accepts a batch."""

        def close(self) -> None:
            """Releases nothing."""
            self.closed += 1

    log_foundry.configure(service="test", sink=_Quiet())
    log_foundry.info("orphan")

    real_choice = _lifecycle._inline_close_choice

    def interrupted(owed: list[object], worker: object) -> object:
        """Stands in for an async interrupt landing after the take, before any close."""
        raise KeyboardInterrupt

    _lifecycle._inline_close_choice = interrupted  # type: ignore[assignment]
    try:
        with pytest.raises(KeyboardInterrupt):
            _lifecycle._close_owed()
    finally:
        _lifecycle._inline_close_choice = real_choice  # type: ignore[assignment]

    assert not _lifecycle._closing_now, (
        f"a close registration leaked and never discharges: {_lifecycle._closing_now}"
    )

    start = time.monotonic()
    log_foundry.shutdown(timeout=30.0)
    assert time.monotonic() - start < _lifecycle.DEFAULT_CLOSER_GRACE, (
        "a later shutdown paid the grace for a close that never happened"
    )


def test_an_interrupt_after_the_take_leaves_the_sinks_owed_for_a_later_shutdown() -> None:
    """The other half of the discharge guarantee, found by SPEC-054's second diff review.

    Discharging the registration is not enough on its own. The take has already removed each sink
    from the owed record, so an interrupt between the take and the close left them **owed by
    nobody**: closed by nobody, and unreachable by a later `shutdown()` because nothing names
    them any more. Measured before the fix at three sinks, `closes=[0, 0, 0]`, record empty.

    The registry half is asserted by the sibling test above; this one asserts the record half and
    then drives the recovery, which is the point — a sink put back is only useful if the next
    call closes it.
    """
    first, second = CountingSink("A"), CountingSink("B")
    log_foundry.configure(service="t", sink=first)
    log_foundry.info("arms A")
    _lifecycle._note_orphan_emit(second)
    assert len(_lifecycle._state._owed) == 2, "the premise: two sinks are owed"

    real_choice = _lifecycle._inline_close_choice

    def interrupted(owed: list[object], worker: object) -> object:
        """Stands in for an async interrupt landing after the take, before any close."""
        raise KeyboardInterrupt

    _lifecycle._inline_close_choice = interrupted  # type: ignore[assignment]
    try:
        with pytest.raises(KeyboardInterrupt):
            _lifecycle._close_owed()
    finally:
        _lifecycle._inline_close_choice = real_choice  # type: ignore[assignment]

    assert (first.closed, second.closed) == (0, 0), "the premise: neither close ran"
    assert not _lifecycle._closing_now, "the registrations were discharged"
    assert {id(first), id(second)} <= set(_lifecycle._state._owed), (
        "and both sinks are owed again, so a later shutdown can still close them"
    )

    log_foundry.shutdown(timeout=5.0)
    assert (first.closed, second.closed) == (1, 1), (
        f"which it does — got A={first.closed} B={second.closed}"
    )


def test_a_forks_clear_inside_a_close_leaves_the_registry_consistent() -> None:
    """SPEC-050 FR-002's shape at SPEC-054 FR-003's registry, where the count could go negative.

    ~~A negative count never satisfies `not _orphan_closing`, so the gate never sets~~ — struck
    (SPEC-021): there is no count to drive negative. The reproduction it came from survives and
    still matters: forking from *inside* the inline close runs the child's handler, which clears
    the whole registry, and the forking thread's own discharge then arrives at an entry that is
    already gone. A count went to -1 there and never recovered; `_finish_closing` removes an
    entry only when it is still **the one it was handed**, so the late discharge finds nothing,
    removes nothing, and leaves the registry exactly as the clear left it.
    """

    class _Quiet:
        def __init__(self) -> None:
            self.log_foundry_stop_signal: threading.Event | None = None

        def emit(self, batch: list[dict]) -> None:
            """Accepts a batch."""

        def close(self) -> None:
            """Releases nothing, but empties the registry as a forked child's handler would."""
            _lifecycle._clear_after_fork()

    log_foundry.configure(service="test", sink=_Quiet())
    log_foundry.info("orphan")
    _lifecycle._close_owed()

    assert not _lifecycle._closing_now, (
        f"the clear and the late discharge left the registry inconsistent: "
        f"{_lifecycle._closing_now}"
    )


def test_an_orphan_only_second_shutdown_does_not_inherit_the_other_deadline() -> None:
    """FR-002 AC-5, on the orphan path — the half a flat cap got wrong.

    The worker path carves its wait from the caller's own deadline, and this took the flat
    `DEFAULT_CLOSER_GRACE` instead, so the same `shutdown(timeout=0)` returned in under half a
    second on one path and took the whole two seconds on the other. Both now use the same
    arithmetic, and the gap is what carries the claim: the close in flight lasts far longer than
    the cap, so a caller returning promptly can only have carved from its own budget.
    """
    class _Slow:
        def __init__(self) -> None:
            self.closed = 0
            self.in_close = threading.Event()
            self.log_foundry_stop_signal: threading.Event | None = None

        def emit(self, batch: list[dict]) -> None:
            """Accepts a batch."""

        def close(self) -> None:
            """Takes far longer than the closer grace, so a flat cap is visible."""
            self.in_close.set()
            time.sleep(5.0)
            self.closed += 1

    sink = _Slow()
    log_foundry.configure(service="test", sink=sink)
    log_foundry.info("orphan")

    first = threading.Thread(target=lambda: log_foundry.shutdown(timeout=30.0), daemon=True)
    first.start()
    assert sink.in_close.wait(5.0), "the premise: the first caller is inside close()"

    start = time.monotonic()
    log_foundry.shutdown(timeout=0)
    elapsed = time.monotonic() - start

    assert elapsed < _lifecycle.DEFAULT_CLOSER_GRACE, (
        f"shutdown(timeout=0) waited {elapsed:.2f}s on another caller's close"
    )


def test_a_daemon_thread_shutdown_finishes_its_close_before_the_process_exits() -> None:
    """FR-002 AC-2. The reproduction, and only a real interpreter exit can show it.

    Measured before the fix: `main exiting at 0.31s; closes started=1 finished=0 wire=0
    buffered=12`, because `atexit` found the close already claimed and returned, and the daemon
    performing it was killed where it stood. The observer registers **first**, so LIFO runs it
    last — after the library's own handler — since a probe registered last reports the state
    before the thing it is measuring has run.
    """
    import subprocess
    import sys

    program = textwrap.dedent(
        """
        import atexit, sys, threading, time
        sys.path.insert(0, "src")
        state = {"started": 0, "finished": 0, "wire": 0, "buffered": 0}
        atexit.register(lambda: print(
            "RESULT started=%(started)d finished=%(finished)d wire=%(wire)d "
            "buffered=%(buffered)d" % state, flush=True))

        import log_foundry

        class S:
            log_foundry_stop_signal = None
            def emit(self, b): state["buffered"] += len(b)
            def close(self):
                state["started"] += 1
                time.sleep(1.5)
                state["wire"] += state["buffered"]; state["buffered"] = 0
                state["finished"] += 1

        log_foundry.configure(service="t", sink=S())

        @log_foundry.trace
        def w(): log_foundry.info("x")
        for _ in range(3): w()

        started = threading.Event()
        threading.Thread(
            target=lambda: (started.set(), log_foundry.shutdown()), daemon=True
        ).start()
        started.wait(5); time.sleep(0.3)
        print("EXITING", flush=True)
        """
    )
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", program], capture_output=True, text=True, timeout=120
    )
    assert "EXITING" in result.stdout, f"the child never got there: {result.stderr[-800:]}"
    assert "RESULT started=1 finished=1 wire=9 buffered=0" in result.stdout, (
        "the atexit call returned through a running close and the buffer died with it:\n"
        f"{result.stdout}\n{result.stderr[-800:]}"
    )


def test_an_orphan_only_daemon_shutdown_finishes_its_close_before_the_process_exits() -> None:
    """FR-002 AC-3's other half — the orphan path at a real interpreter exit.

    The in-process test above settles the mechanism with an explicit second call. This settles
    what the criterion actually asks: a process that only ever logged outside a span, whose
    `shutdown()` began on a daemon thread, still delivers its close-is-delivery sink's buffer
    when the interpreter exits through `atexit`. There is no worker on this path, so
    `_close_owed` is the only thing that closes the sink at all.
    """
    import subprocess
    import sys

    program = textwrap.dedent(
        """
        import atexit, sys, threading, time
        sys.path.insert(0, "src")
        state = {"started": 0, "finished": 0, "wire": 0, "buffered": 0}
        atexit.register(lambda: print(
            "RESULT started=%(started)d finished=%(finished)d wire=%(wire)d "
            "buffered=%(buffered)d" % state, flush=True))

        import log_foundry

        class S:
            log_foundry_stop_signal = None
            def emit(self, b): state["buffered"] += len(b)
            def close(self):
                state["started"] += 1
                time.sleep(1.5)
                state["wire"] += state["buffered"]; state["buffered"] = 0
                state["finished"] += 1

        log_foundry.configure(service="t", sink=S())
        for i in range(3):
            log_foundry.info("orphan-%d" % i)   # no span, so no worker is ever built

        started = threading.Event()
        threading.Thread(
            target=lambda: (started.set(), log_foundry.shutdown()), daemon=True
        ).start()
        started.wait(5); time.sleep(0.3)
        print("EXITING", flush=True)
        """
    )
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", program], capture_output=True, text=True, timeout=120
    )
    assert "EXITING" in result.stdout, f"the child never got there: {result.stderr[-800:]}"
    assert "RESULT started=1 finished=1 wire=3 buffered=0" in result.stdout, (
        "the orphan path's atexit call returned through a running close:\n"
        f"{result.stdout}\n{result.stderr[-800:]}"
    )
