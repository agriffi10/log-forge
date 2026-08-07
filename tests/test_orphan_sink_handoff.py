"""SPEC-033 — the orphan-path sink handoff, where no worker exists to own the close."""

import threading
import time

import pytest

log_foundry = pytest.importorskip("log_foundry")
config = pytest.importorskip("log_foundry.config")
decorator = pytest.importorskip("log_foundry.decorator")
lifecycle = pytest.importorskip("log_foundry._lifecycle")
worker_mod = pytest.importorskip("log_foundry.worker")


class CountingSink:
    """Counts closes and holds events until closed, so a lost close is a lost event."""

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


class FailingEmitSink(CountingSink):
    """Raises out of ``emit`` — a total failure, which SPEC-026 FR-001 requires."""

    def emit(self, batch: list[dict]) -> None:
        raise RuntimeError("the destination is unreachable")


class SlowCloseSink(CountingSink):
    """Blocks inside ``close`` until released, so a bound on the wait is observable."""

    def __init__(self, name: str = "slow") -> None:
        super().__init__(name)
        self.in_close = threading.Event()
        self.release = threading.Event()

    def close(self) -> None:
        self.in_close.set()
        self.release.wait(30.0)
        super().close()


def _eventually(predicate, timeout: float = 5.0) -> bool:
    """Polls a predicate another thread satisfies rather than sleeping a fixed interval."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


# -- FR-001: the orphan path records which sink it wrote to, and which it has closed --------


def test_the_recorded_sink_is_the_one_emitted_to_not_the_one_configured() -> None:
    """AC-2. The property the boolean lacked, asserted directly.

    `configure()` assigns `_config.sink` before it calls the swap, so a close resolved through
    `_ensure_sink()` at close time would target whatever was configured last, not what was
    written to.
    """
    written, later = CountingSink("written"), CountingSink("later")
    log_foundry.configure(service="t", sink=written)
    log_foundry.info("reaches written")

    config.get_config().sink = later  # reassigned behind the library's back

    decorator._close_orphan_sink()

    assert written.closed == 1, "the sink the event reached is the one closed"
    assert later.closed == 0, "not whatever the config happens to name now"


def test_a_configured_but_never_written_sink_is_not_closed() -> None:
    """AC-3. `configure()` runs `_ensure_sink()` unconditionally; that must not arm a close."""
    sink = CountingSink()
    log_foundry.configure(service="t", sink=sink)

    decorator._close_orphan_sink()

    assert sink.closed == 0
    assert decorator._orphan_sink is None


def test_a_sink_whose_emit_raised_is_still_closed() -> None:
    """AC-4. SPEC-026 FR-001 makes a total failure raise, and a failed socket still needs releasing."""
    sink = FailingEmitSink()
    log_foundry.configure(service="t", sink=sink)

    log_foundry.info("this will raise inside the sink")  # absorbed by SPEC-025

    assert decorator._orphan_sink is sink, "armed before the emit, not after it"
    decorator._close_orphan_sink()
    assert sink.closed == 1


def test_a_latched_sink_is_not_rearmed_and_so_is_not_closed_twice() -> None:
    """AC-5. A post-`shutdown()` emit is refused at the closed sink; it must not re-arm it."""
    sink = CountingSink()
    log_foundry.configure(service="t", sink=sink)
    log_foundry.info("before")
    log_foundry.shutdown()
    assert sink.closed == 1

    log_foundry.info("after shutdown, against the closed sink")
    decorator._close_orphan_sink()

    assert sink.closed == 1, "a second close on a partially released sink is worse than none"


# -- FR-002: a late configure(sink=...) with no worker closes the old sink -------------------


def test_a_swap_with_no_worker_closes_the_previous_sink() -> None:
    """AC-1. The headline defect: A was left open with `incomplete_swaps` at zero."""
    old, new = CountingSink("old"), CountingSink("new")
    log_foundry.configure(service="t", sink=old)
    log_foundry.info("to old")

    log_foundry.configure(sink=new)

    assert old.closed == 1
    assert new.closed == 0


def test_after_the_swap_events_reach_the_new_sink(capsys) -> None:
    """AC-2."""
    old, new = CountingSink("old"), CountingSink("new")
    log_foundry.configure(service="t", sink=old)
    log_foundry.info("to old")
    log_foundry.configure(sink=new)

    log_foundry.info("to new")

    assert [e["message"] for e in new.held] == ["to new"]
    assert [e["message"] for e in old.held] == [], "old was closed, draining what it had"


def test_a_swap_then_shutdown_closes_both_sinks_with_no_second_emit() -> None:
    """AC-3. The case a clear-on-swap design regresses.

    Today (pre-SPEC-033) the boolean is path-agnostic and `_ensure_sink()` still names B at
    exit, so B *is* closed here. Re-pointing the record is what keeps that true while also
    closing A.
    """
    old, new = CountingSink("old"), CountingSink("new")
    log_foundry.configure(service="t", sink=old)
    log_foundry.info("to old")

    log_foundry.configure(sink=new)  # no second info()
    log_foundry.shutdown()

    assert (old.closed, new.closed) == (1, 1), "one leak must not be traded for another"


def test_a_swap_with_nothing_ever_emitted_closes_nothing() -> None:
    """AC-4."""
    old, new = CountingSink("old"), CountingSink("new")
    log_foundry.configure(service="t", sink=old)

    log_foundry.configure(sink=new)

    assert (old.closed, new.closed) == (0, 0)


def test_reconfiguring_the_same_sink_is_a_no_op() -> None:
    """AC-5. Mirrors `Worker.swap_sink`'s `self.sink is new_sink` guard."""
    sink = CountingSink()
    log_foundry.configure(service="t", sink=sink)
    log_foundry.info("to it")

    log_foundry.configure(sink=sink)

    assert sink.closed == 0, "no close"
    assert decorator._orphan_sink is sink, "and still armed for one at exit"


@pytest.mark.parametrize("orphan_first", [True, False])
def test_a_mixed_process_closes_the_old_sink_exactly_once(orphan_first: bool) -> None:
    """AC-6. Both orders. The worker owns the close; this path must not add a second."""
    old, new = CountingSink("old"), CountingSink("new")
    log_foundry.configure(service="t", sink=old)

    @log_foundry.trace
    def work() -> None:
        pass

    if orphan_first:
        log_foundry.info("orphan")
        work()
    else:
        work()
        log_foundry.info("orphan")

    log_foundry.configure(sink=new)

    assert old.closed == 1, f"exactly one close (orphan_first={orphan_first})"
    assert new.closed == 0


def test_a_close_that_raises_is_absorbed_and_the_swap_stands(capsys) -> None:
    """AC-7. The closer is joined before asserting, or the line may not be written yet."""

    class Exploding(CountingSink):
        def close(self) -> None:
            self.closed += 1
            raise RuntimeError("close failed")

    old, new = Exploding("old"), CountingSink("new")
    log_foundry.configure(service="t", sink=old)
    log_foundry.info("to old")

    log_foundry.configure(sink=new)  # joins the closer within the budget

    assert _eventually(lambda: old.closed == 1), "the close was attempted"
    assert config.get_config().sink is new, "and the swap still stands"
    assert decorator._orphan_sink is new
    assert "absorbed a failure while closing a swapped-out sink" in capsys.readouterr().err


def test_a_three_sink_chain_closes_each_exactly_once() -> None:
    """AC-9."""
    a, b, c = CountingSink("a"), CountingSink("b"), CountingSink("c")
    log_foundry.configure(service="t", sink=a)
    log_foundry.info("to a")
    log_foundry.configure(sink=b)
    log_foundry.configure(sink=c)
    log_foundry.info("to c")
    log_foundry.shutdown()

    assert (a.closed, b.closed, c.closed) == (1, 1, 1)


def test_a_swap_racing_a_first_trace_does_not_close_the_workers_sink() -> None:
    """AC-10. The race the unlocked `_worker` read would open.

    A first `@trace` resolves `_ensure_sink()` and then builds the worker under `_worker_lock`.
    An unlocked read in `_swap_sink` can see `None` in that window and close the sink the worker
    has already captured. The preemption point is injected, as SPEC-028 does for races that need
    one — the window is otherwise a few instructions wide.
    """
    old, new = CountingSink("old"), CountingSink("new")
    log_foundry.configure(service="t", sink=old)
    log_foundry.info("to old")

    building = threading.Event()
    real_worker = worker_mod.Worker

    class SlowToBuild(real_worker):  # type: ignore[misc, valid-type]
        def __init__(self, sink, **kw):
            building.set()
            time.sleep(0.3)  # the window an unlocked reader would exploit
            super().__init__(sink, **kw)

    decorator.Worker = SlowToBuild  # type: ignore[misc]
    try:
        tracer = threading.Thread(target=decorator._get_worker)
        tracer.start()
        assert building.wait(5.0), "the worker is mid-construction"

        log_foundry.configure(sink=new)  # must block on the lock, not close `old` underneath
        tracer.join(10.0)
    finally:
        decorator.Worker = real_worker  # type: ignore[misc]

    worker = decorator._worker
    assert worker is not None
    # The locked read makes the swap wait for construction and then take the *worker* path, so
    # the worker legitimately ends up on `new` and closes `old` itself. What must not happen is
    # the orphan branch closing `old` first: unlocked, that is two closes on one sink.
    assert worker.sink is new, "the swap went through the worker, having waited for it"
    assert old.closed == 1, "closed exactly once — an unlocked read closes it twice"


def test_a_concurrent_orphan_emit_is_not_blocked_for_the_swap_budget() -> None:
    """AC-11. The closer's join is outside `_worker_lock`, which every orphan emit takes."""
    hung, new = SlowCloseSink("hung"), CountingSink("new")
    log_foundry.configure(service="t", sink=hung)
    log_foundry.info("to hung")

    elapsed: list[float] = []

    def swapper() -> None:
        log_foundry.configure(sink=new)

    swap = threading.Thread(target=swapper)
    swap.start()
    try:
        assert hung.in_close.wait(5.0), "the close is running and will not return"

        start = time.monotonic()
        log_foundry.info("concurrent orphan emit")
        elapsed.append(time.monotonic() - start)
    finally:
        hung.release.set()
        swap.join(30.0)

    assert elapsed[0] < 1.0, (
        f"an orphan emit waited {elapsed[0]:.2f}s behind the swap's join — the lock is held "
        f"across it"
    )


def test_a_sink_adopted_after_a_retired_worker_is_still_closed() -> None:
    """AC-12 / FR-001 AC-6. `Worker.swap_sink` returns early once `_shutdown_done`.

    So a retired worker keeps its old sink forever while every orphan event goes to the new
    one — "a worker exists" stops answering "does a worker own this sink".
    """
    old, new = CountingSink("old"), CountingSink("new")
    log_foundry.configure(service="t", sink=old)

    @log_foundry.trace
    def work() -> None:
        pass

    work()
    log_foundry.shutdown()
    assert old.closed == 1

    log_foundry.configure(sink=new)
    log_foundry.info("to the sink adopted after shutdown")
    decorator._shutdown_worker()

    assert new.closed == 1, "closed by the orphan path, since the worker owns nothing further"
    assert new.delivered, "and its held events were delivered by that close"
    assert old.closed == 1, "the retired worker's own sink is not closed twice"


def test_an_expired_shutdown_still_declines_to_close() -> None:
    """AC-12's other half: the case the original `_worker is not None` guard existed for.

    An expired `Worker.shutdown` leaves the sink open because the drain thread may still be
    inside `emit`. The ownership form must not regress that into a close under a live writer.
    """

    class Wedged(CountingSink):
        def __init__(self) -> None:
            super().__init__("wedged")
            self.in_emit = threading.Event()
            self.release = threading.Event()

        def emit(self, batch: list[dict]) -> None:
            self.in_emit.set()
            self.release.wait(30.0)

    wedged = Wedged()
    log_foundry.configure(service="t", sink=wedged)
    log_foundry.info("orphan, arming the record")  # arms, and returns

    worker = worker_mod.Worker(wedged, batch_size=1, flush_interval=0.01)
    decorator._worker = worker
    try:
        worker.submit([{"message": "wedges the drain thread"}])
        assert wedged.in_emit.wait(5.0)

        decorator._shutdown_worker(timeout=0.2)  # expires with the thread inside emit

        assert wedged.closed == 0, "the drain thread may still be using it (SPEC-027 FR-004)"
        assert worker.health().stopped_reason == "ShutdownTimeout"
    finally:
        wedged.release.set()


# -- FR-003: the swapped-out close is bounded, abandonable, and granted the exit grace -------


def test_a_hanging_close_does_not_hold_configure_past_the_budget(monkeypatch) -> None:
    """AC-1. Gap kept wide: a 30 s close against a 1 s budget."""
    monkeypatch.setattr(worker_mod, "DEFAULT_SWAP_TIMEOUT", 1.0)
    hung, new = SlowCloseSink("hung"), CountingSink("new")
    log_foundry.configure(service="t", sink=hung)
    log_foundry.info("to hung")

    try:
        start = time.monotonic()
        log_foundry.configure(sink=new)
        elapsed = time.monotonic() - start

        assert elapsed < 10.0, f"configure() waited {elapsed:.2f}s on a close it must abandon"
        assert hung.closed == 0, "and the close is genuinely still running"
    finally:
        hung.release.set()


def test_an_expired_close_join_moves_no_counter_and_writes_no_line(capsys, monkeypatch) -> None:
    """AC-2 / FR-006 AC-1. A slow close and a stuck one are indistinguishable at that moment."""
    monkeypatch.setattr(worker_mod, "DEFAULT_SWAP_TIMEOUT", 0.2)
    hung, new = SlowCloseSink("hung"), CountingSink("new")
    log_foundry.configure(service="t", sink=hung)
    log_foundry.info("to hung")
    capsys.readouterr()

    try:
        log_foundry.configure(sink=new)

        health = log_foundry.health()
        assert health.closing_sinks == 1, "the scenario really occurred: a close is outstanding"
        assert health.incomplete_swaps == 0, "an expired close join is not an unconfirmed drain"
        assert health.failed_batches == 0
        assert capsys.readouterr().err == "", "and nothing is announced"
    finally:
        hung.release.set()


def test_closing_sinks_is_reported_with_no_worker(monkeypatch) -> None:
    """AC-3. The gauge must be reachable on a path where every other Health field is a zero."""
    monkeypatch.setattr(worker_mod, "DEFAULT_SWAP_TIMEOUT", 0.2)
    hung, new = SlowCloseSink("hung"), CountingSink("new")
    log_foundry.configure(service="t", sink=hung)
    log_foundry.info("to hung")

    assert decorator._worker is None, "no worker anywhere in this process"
    assert log_foundry.health().closing_sinks == 0, "nothing is closing yet"

    try:
        log_foundry.configure(sink=new)
        assert log_foundry.health().closing_sinks == 1, "the hung close is visible"
    finally:
        hung.release.set()

    assert _eventually(lambda: log_foundry.health().closing_sinks == 0), "and it falls again"


def test_shutdown_grants_the_grace_to_an_outstanding_orphan_closer(monkeypatch) -> None:
    """AC-4. A close that is slow but succeeding must not be killed at interpreter exit."""
    monkeypatch.setattr(worker_mod, "DEFAULT_SWAP_TIMEOUT", 0.1)
    slow, new = SlowCloseSink("slow"), CountingSink("new")
    log_foundry.configure(service="t", sink=slow)
    log_foundry.info("to slow")

    log_foundry.configure(sink=new)
    assert slow.in_close.wait(5.0)
    assert slow.closed == 0, "abandoned by the swap"

    threading.Timer(0.2, slow.release.set).start()
    log_foundry.shutdown(timeout=5.0)

    assert slow.closed == 1, "the grace let it finish"


def test_the_grace_runs_when_nothing_is_armed_and_on_a_second_shutdown(monkeypatch) -> None:
    """AC-5. Placed inside `_close_orphan_sink` it would be skipped in exactly these cases."""
    monkeypatch.setattr(worker_mod, "DEFAULT_SWAP_TIMEOUT", 0.1)
    slow, new = SlowCloseSink("slow"), CountingSink("new")
    log_foundry.configure(service="t", sink=slow)
    log_foundry.info("to slow")
    log_foundry.configure(sink=new)
    assert slow.in_close.wait(5.0)

    log_foundry.shutdown(timeout=0.05)  # first call: expires, closes `new`, grants ~nothing
    assert new.closed == 1

    # Nothing is armed now, and this is the idempotent second call — the `atexit` one.
    threading.Timer(0.2, slow.release.set).start()
    log_foundry.shutdown(timeout=5.0)

    assert slow.closed == 1, "the grace is granted with nothing armed and on a repeat call"


def test_the_grace_runs_after_the_live_sinks_own_close(monkeypatch) -> None:
    """AC-6. Order pinned, not just the outcome: it is what holds if the process is killed."""
    monkeypatch.setattr(worker_mod, "DEFAULT_SWAP_TIMEOUT", 0.1)
    order: list[str] = []
    slow, live = SlowCloseSink("slow"), CountingSink("live")

    def note_live_close() -> None:
        order.append("live")

    live.close = note_live_close  # type: ignore[method-assign]

    real_join = lifecycle.join_closers
    monkeypatch.setattr(
        lifecycle, "join_closers", lambda t: (order.append("grace"), real_join(t))[1]
    )

    log_foundry.configure(service="t", sink=slow)
    log_foundry.info("to slow")
    log_foundry.configure(sink=live)
    assert slow.in_close.wait(5.0)
    log_foundry.info("to live")

    slow.release.set()
    log_foundry.shutdown(timeout=5.0)

    assert order == ["live", "grace"], f"the live sink closes first — got {order}"


def test_the_live_orphan_sinks_close_stays_inline_and_unbounded() -> None:
    """AC-7. Unchanged from SPEC-031 FR-006, and asserted so a refactor cannot move it."""
    slow = SlowCloseSink("live")
    log_foundry.configure(service="t", sink=slow)
    log_foundry.info("to it")

    threading.Timer(0.4, slow.release.set).start()
    start = time.monotonic()
    log_foundry.shutdown(timeout=5.0)
    elapsed = time.monotonic() - start

    assert slow.closed == 1, "shutdown waited for it rather than detaching it"
    assert elapsed >= 0.4, "inline: it did not return before the close finished"


def test_a_closer_that_cannot_start_leaves_the_sink_open_and_says_so(capsys, monkeypatch) -> None:
    """AC-8. No inline fallback — that would reintroduce the unbounded wait under pressure."""
    old, new = CountingSink("old"), CountingSink("new")
    log_foundry.configure(service="t", sink=old)
    log_foundry.info("to old")
    capsys.readouterr()

    def refuse(*a, **kw):
        raise RuntimeError("can't start a thread")

    monkeypatch.setattr(lifecycle.threading.Thread, "start", refuse)

    log_foundry.configure(sink=new)

    assert old.closed == 0, "left open rather than closed inline"
    assert decorator._orphan_sink is new, "and the record still re-points (AC-8)"
    err = capsys.readouterr().err
    assert "starting the thread that closes a swapped-out sink" in err
    assert log_foundry.health().incomplete_swaps == 0


# -- FR-004: an orphan-only process hands its sink the stop signal ---------------------------


def test_an_orphan_emit_gives_the_sink_a_stop_signal() -> None:
    """AC-1. `Worker._offer_stop_signal` is the only other caller, and there is no worker."""
    sink = CountingSink()
    log_foundry.configure(service="t", sink=sink)

    log_foundry.info("to it")

    assert isinstance(sink.stop_signal, threading.Event)
    assert not sink.stop_signal.is_set()


def test_shutdown_sets_the_orphan_stop_signal() -> None:
    """AC-2. A sink parked in a backoff must be released rather than serving it in full."""
    retry = pytest.importorskip("log_foundry.sinks._retry")
    sink = CountingSink()
    log_foundry.configure(service="t", sink=sink)
    log_foundry.info("to it")
    signal = sink.stop_signal
    assert signal is not None

    threading.Timer(0.1, log_foundry.shutdown).start()
    start = time.monotonic()
    retry.wait(30.0, signal)  # a backoff far longer than the shutdown it must be cut short by
    elapsed = time.monotonic() - start

    assert elapsed < 10.0, f"the backoff ran {elapsed:.2f}s of 30 s — shutdown did not cut it"
    assert signal.is_set()


def test_a_sink_without_the_attribute_is_unaffected() -> None:
    """AC-3."""

    class Bare:
        def __init__(self) -> None:
            self.held: list[dict] = []

        def emit(self, batch: list[dict]) -> None:
            self.held.extend(batch)

        def close(self) -> None: ...

    sink = Bare()
    log_foundry.configure(service="t", sink=sink)

    log_foundry.info("to it")

    assert not hasattr(sink, "stop_signal"), "nothing was invented on it"
    assert len(sink.held) == 1, "and the emit went through"


def test_a_sink_that_rejects_the_signal_loses_interruptibility_not_the_emit(capsys) -> None:
    """AC-3's error path, matching `Worker._offer_stop_signal`."""

    class Hostile:
        """A read-only ``stop_signal`` — the case `Worker._offer_stop_signal` also absorbs."""

        def __init__(self) -> None:
            self.held: list[dict] = []

        @property
        def stop_signal(self):
            return None

        def emit(self, batch: list[dict]) -> None:
            self.held.extend(batch)

        def close(self) -> None: ...

    sink = Hostile()
    log_foundry.configure(service="t", sink=sink)
    capsys.readouterr()

    log_foundry.info("to it")

    assert len(sink.held) == 1, "the emit still happened"
    assert "handing the sink its stop signal" in capsys.readouterr().err


def test_the_worker_keeps_its_own_signal_on_the_sink_it_owns() -> None:
    """AC-4. Overwriting it would leave the drain thread serving a full backoff at shutdown."""
    sink = CountingSink()
    log_foundry.configure(service="t", sink=sink)

    @log_foundry.trace
    def work() -> None:
        pass

    work()
    worker = decorator._worker
    assert worker is not None

    log_foundry.info("an orphan emit against the sink the worker owns")

    assert sink.stop_signal is worker._stop, "the worker's event, not the orphan path's"


def test_a_sink_adopted_after_a_retired_worker_still_gets_a_signal() -> None:
    """AC-4's second half: skipping on a worker merely existing leaves this one uninterruptible."""
    old, new = CountingSink("old"), CountingSink("new")
    log_foundry.configure(service="t", sink=old)

    @log_foundry.trace
    def work() -> None:
        pass

    work()
    log_foundry.shutdown()
    log_foundry.configure(sink=new)
    log_foundry.info("to the newly adopted sink")

    worker = decorator._worker
    assert worker is not None and worker.sink is old, "the retired worker never swapped"
    assert isinstance(new.stop_signal, threading.Event), "yet the live sink has a signal"
    assert not new.stop_signal.is_set(), "and an unset one"


def test_a_sink_emitted_to_after_shutdown_still_backs_off() -> None:
    """AC-6. Both cases: a newly configured sink, and the same sink logged to again.

    An `Event` is set once and never cleared, and `_retry.wait` returns instantly on a set one,
    so handing a live sink the shutdown's event collapses every backoff to zero — a tight retry
    loop against exactly the rate-limited destination the backoff exists for. Keying the refresh
    on *arming* misses the second case, which `FR-001 AC-5` refuses to re-arm.
    """
    retry = pytest.importorskip("log_foundry.sinks._retry")
    same, fresh = CountingSink("same"), CountingSink("fresh")
    log_foundry.configure(service="t", sink=same)
    log_foundry.info("before")
    log_foundry.shutdown()

    log_foundry.info("same sink, after shutdown")  # latched: FR-001 AC-5 refuses to re-arm
    log_foundry.configure(sink=fresh)
    log_foundry.info("newly configured sink, after shutdown")

    for label, sink in (("the same sink", same), ("a newly configured sink", fresh)):
        signal = sink.stop_signal
        assert isinstance(signal, threading.Event), f"{label} has a signal"
        assert not signal.is_set(), f"{label}'s signal is unset"
        start = time.monotonic()
        retry.wait(0.3, signal)
        assert time.monotonic() - start >= 0.25, f"{label} still backs off"


def test_the_swapped_in_sink_is_offered_a_signal_without_waiting_for_an_emit() -> None:
    """AC-7."""
    old, new = CountingSink("old"), CountingSink("new")
    log_foundry.configure(service="t", sink=old)
    log_foundry.info("to old")

    log_foundry.configure(sink=new)

    assert isinstance(new.stop_signal, threading.Event)


# -- FR-006: incomplete_swaps keeps its worker-only meaning ----------------------------------


def test_the_worker_still_records_its_own_unconfirmed_drain() -> None:
    """AC-3. The exemption: this spec must not be read as forbidding the worker's own count."""

    class Wedged(CountingSink):
        def __init__(self) -> None:
            super().__init__("wedged")
            self.in_emit = threading.Event()
            self.release = threading.Event()

        def emit(self, batch: list[dict]) -> None:
            self.in_emit.set()
            self.release.wait(30.0)

    wedged, new = Wedged(), CountingSink("new")
    worker = worker_mod.Worker(wedged, batch_size=1, flush_interval=0.01)
    decorator._worker = worker
    try:
        worker.submit([{"message": "wedges the drain"}])
        assert wedged.in_emit.wait(5.0)

        log_foundry.configure(sink=new, service="t")

        assert worker.health().incomplete_swaps == 1, "the worker's drain genuinely failed"
    finally:
        wedged.release.set()


def test_health_gains_no_field() -> None:
    """AC-5."""
    assert worker_mod.Health._fields == (
        "queued",
        "dropped",
        "failed_batches",
        "stopped_reason",
        "sink",
        "retired",
        "submitted_after_shutdown",
        "incomplete_swaps",
        "closing_sinks",
    )
