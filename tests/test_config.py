"""Phase 1 — Config (arch §7). Global settings, set once at startup."""

import pytest

from log_foundry import _lifecycle, config


def test_configure_sets_identity_fields() -> None:
    config.configure(service="payments", version="2.14", env="prod")
    cfg = config.get_config()
    assert (cfg.service, cfg.version, cfg.env) == ("payments", "2.14", "prod")


def test_configure_patches_only_provided_fields() -> None:
    config.configure(service="payments", version="2.14", env="prod")
    config.configure(env="staging")  # a second call composes rather than resetting
    cfg = config.get_config()
    assert cfg.env == "staging"
    assert cfg.service == "payments"  # untouched by the second call


def test_configured_sink_is_stored() -> None:
    class FakeSink:
        def emit(self, batch: list[dict[str, object]]) -> None: ...
        def close(self) -> None: ...

    sink = FakeSink()
    config.configure(sink=sink)
    assert config.get_config().sink is sink


def test_defaults_default_to_empty_dict() -> None:
    # A fresh interpreter starts with no user defaults; setting them replaces the dict.
    config.configure(defaults={"team": "checkout"})
    assert config.get_config().defaults == {"team": "checkout"}


# -- SPEC-017 FR-006: payload ceilings ---------------------------------------------------


def test_ceilings_have_documented_defaults() -> None:
    cfg = config.get_config()
    assert cfg.max_value_bytes == 8192
    assert cfg.max_stack_bytes == 32768
    assert cfg.max_keys == 256
    assert cfg.max_depth == 8


def test_ceilings_are_configurable() -> None:
    config.configure(max_value_bytes=64, max_stack_bytes=128, max_keys=2, max_depth=2)
    cfg = config.get_config()
    assert (cfg.max_value_bytes, cfg.max_stack_bytes, cfg.max_keys, cfg.max_depth) == (
        64,
        128,
        2,
        2,
    )


@pytest.mark.parametrize(
    "name", ["max_value_bytes", "max_stack_bytes", "max_keys", "max_depth"]
)
@pytest.mark.parametrize("bad", [0, -1])
def test_a_non_positive_ceiling_is_rejected(name: str, bad: int) -> None:
    with pytest.raises(ValueError, match=name):
        config.configure(**{name: bad})


def test_a_rejected_call_leaves_the_config_untouched() -> None:
    """Validation runs before any assignment, so a bad ceiling cannot half-apply a call."""
    config.configure(service="before")
    with pytest.raises(ValueError):
        config.configure(service="after", max_keys=0)
    assert config.get_config().service == "before"


def test_max_keys_configured_through_configure_takes_effect() -> None:
    from log_foundry import model
    config.configure(max_keys=2)
    span = model.Span(
        trace_id="a" * 32, span_id="b" * 16, parent_span_id=None, name="fn", start_ts=0.0
    )
    event = model.build_event(
        span, "INFO", "m", fields={str(i): i for i in range(10)}, baggage={}
    )
    assert len(event["fields"]) == 2
    assert event["truncated"] is True


def test_max_depth_configured_through_configure_takes_effect() -> None:
    from log_foundry import model
    config.configure(max_depth=2)
    span = model.Span(
        trace_id="a" * 32, span_id="b" * 16, parent_span_id=None, name="fn", start_ts=0.0
    )
    event = model.build_event(
        span, "INFO", "m", fields={"a": {"b": {"c": {"d": 1}}}}, baggage={}
    )
    assert "<depth limit>" in str(event["fields"])
    assert event["truncated"] is True


def test_max_depth_of_one_still_keeps_scalar_field_values() -> None:
    """`fields` is the payload container, not a nesting level the caller chose — so the
    smallest legal max_depth must still emit scalar values rather than an empty event."""
    from log_foundry import model
    config.configure(max_depth=1)
    span = model.Span(
        trace_id="a" * 32, span_id="b" * 16, parent_span_id=None, name="fn", start_ts=0.0
    )
    event = model.build_event(span, "INFO", "m", fields={"a": 1, "b": "two"}, baggage={})
    assert event["fields"] == {"a": 1, "b": "two"}


# -- SPEC-030 FR-003: a late `configure(sink=...)` swaps the live delivery target ----------

import threading  # noqa: E402
import time  # noqa: E402

import log_foundry  # noqa: E402
from log_foundry import _lifecycle as lifecycle  # noqa: E402
from log_foundry import worker as worker_mod  # noqa: E402


def _span(msg) -> list[dict]:
    """One span's worth of events, as ``Worker.submit`` receives them."""
    return [{"message": msg}]


class SwapSink:
    """Records batches and counts closes, so a swap's ordering and cleanup are both visible."""

    def __init__(self) -> None:
        self.batches: list[list[dict]] = []
        self.closed = 0
        self._lock = threading.Lock()

    def emit(self, batch: list[dict]) -> None:
        with self._lock:
            self.batches.append(list(batch))

    def close(self) -> None:
        self.closed += 1

    @property
    def messages(self) -> list:
        with self._lock:
            return [e["message"] for b in self.batches for e in b]


class WedgedSink(SwapSink):
    """Blocks inside ``emit`` until released, so a drain provably cannot complete."""

    def __init__(self) -> None:
        super().__init__()
        self.in_emit = threading.Event()
        self.release = threading.Event()

    def emit(self, batch: list[dict]) -> None:
        self.in_emit.set()
        self.release.wait(10.0)
        super().emit(batch)


def _eventually(predicate, timeout: float = 5.0) -> bool:
    """Polls a predicate that another thread satisfies, rather than sleeping a fixed interval."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


def _worker_with(sink) -> "worker_mod.Worker":
    """Installs a process worker on ``sink`` whose batching triggers cannot fire unaided.

    A large ``batch_size`` and long ``flush_interval`` mean every delivery in these tests is one
    the swap or an explicit ``flush()`` caused, which is what makes the ordering assertions say
    something.
    """
    worker = worker_mod.Worker(sink, batch_size=1000, flush_interval=100.0)
    _lifecycle._state._worker = worker
    return worker


def test_a_late_sink_swap_routes_subsequent_events_to_the_new_sink() -> None:
    """The measured defect: sink A got 4 events, sink B got 0, and the config claimed B."""
    old, new = SwapSink(), SwapSink()
    worker = _worker_with(old)
    worker.submit(_span("before"))

    config.configure(sink=new)

    worker.submit(_span("after"))
    assert log_foundry.flush(timeout=5.0)

    assert new.messages == ["after"]
    assert config.get_config().sink is new
    assert worker.sink is new, "the config and the live target must agree"


def test_events_submitted_before_the_swap_are_drained_to_the_old_sink() -> None:
    old, new = SwapSink(), SwapSink()
    worker = _worker_with(old)
    worker.submit(_span("before"))

    config.configure(sink=new)

    assert old.messages == ["before"], "drained to the sink they were submitted for"
    assert new.messages == [], "and not carried over to the new one"


def test_the_previous_sink_is_closed_exactly_once_and_the_new_one_is_not() -> None:
    old, new = SwapSink(), SwapSink()
    _worker_with(old)

    config.configure(sink=new)

    assert old.closed == 1
    assert new.closed == 0
    assert log_foundry.health().incomplete_swaps == 0


def test_the_swap_drains_the_old_sink_then_fences_before_closing_it() -> None:
    """Two drains, and the order is the contract (structural, like SPEC-028's lock tests).

    The first carries the pre-swap events to the sink they were submitted for. The second runs
    *after* the reassignment and delivers nothing — it exists to prove the drain thread is not
    still inside the old sink's ``emit``, which is the one way ``close()`` could be called under
    a writer. That window needs a span finishing on another thread mid-swap to reach, so it is
    pinned by asserting the fence happens rather than by racing it.
    """
    old, new = SwapSink(), SwapSink()
    worker = _worker_with(old)
    real_flush, real_close, drains, close_budget = worker.flush, worker._close_swapped_out, [], []

    def recording_flush(timeout=None):
        drains.append((worker.sink, timeout))
        return real_flush(timeout)

    def recording_close(sink, timeout):
        close_budget.append(timeout)
        return real_close(sink, timeout)

    worker.flush = recording_flush
    worker._close_swapped_out = recording_close
    config.configure(sink=new)

    assert [sink for sink, _ in drains] == [old, new], "drain to the old sink, then fence after"
    assert old.closed == 1, "and only then close it"
    # One deadline covers all three waits, so a hung sink cannot cost a multiple of the budget.
    # Each step is a real queue round-trip, so the monotonic clock has moved between them; a
    # step handed a *fresh* budget would read equal to its predecessor rather than less.
    assert drains[1][1] < drains[0][1], "the fence gets what is left of the budget, not a fresh one"
    assert close_budget and close_budget[0] < drains[1][1], "and the close gets what is left of that"


def test_a_shutdown_landing_mid_swap_does_not_leak_the_new_sink() -> None:
    """The retirement check must be re-taken after the drain, not only before it.

    ``shutdown()`` closes whatever ``self.sink`` is at that moment and latches its once-only
    flag. A swap that reassigns afterwards installs a sink **nothing will ever close** — a second
    ``shutdown()`` returns early — and then reports an ``incomplete_swaps`` whose stderr line
    says the old sink was left open, when it was in fact closed. The race is injected rather
    than run, on the same principle as the fence test above.
    """
    old, new = SwapSink(), SwapSink()
    worker = _worker_with(old)
    real_flush = worker.flush

    def flush_then_retire(timeout=None):
        result = real_flush(timeout)
        worker.shutdown()  # atexit, or another thread, lands here
        return result

    worker.flush = flush_then_retire
    worker.swap_sink(new)

    assert worker.sink is old, "a retired worker must not be retargeted"
    assert old.closed == 1, "shutdown closed the live sink"
    assert new.closed == 0, "and the sink that was never installed is not orphaned"
    assert log_foundry.health().incomplete_swaps == 0, "nothing was swapped, so nothing to report"


def test_the_new_sink_is_given_the_workers_stop_signal() -> None:
    """SPEC-027 FR-002: a sink that cannot see the stop event backs off uninterruptibly."""

    class SignallingSink(SwapSink):
        log_foundry_stop_signal = None

    old, new = SwapSink(), SignallingSink()
    worker = _worker_with(old)

    config.configure(sink=new)

    assert new.log_foundry_stop_signal is worker._stop


def test_swapping_to_the_sink_that_is_already_live_is_a_no_op() -> None:
    sink = SwapSink()
    worker = _worker_with(sink)
    worker.submit(_span("queued"))

    config.configure(sink=sink)

    assert sink.closed == 0, "the live sink must not be closed out from under the worker"
    assert sink.messages == [], "and no drain was forced"


def test_configure_without_a_sink_never_rebuilds_anything() -> None:
    sink = SwapSink()
    worker = _worker_with(sink)
    worker.submit(_span("queued"))

    config.configure(service="payments", defaults={"team": "checkout"}, max_keys=32)

    assert worker.sink is sink
    assert sink.closed == 0
    assert sink.messages == [], "no drain was forced"
    assert config.get_config().service == "payments"


def test_configure_before_any_logging_creates_no_worker(capsys) -> None:
    """Pre-first-log behaviour is unchanged — there is no captured sink to disagree with."""
    _lifecycle._state._worker = None

    config.configure(sink=SwapSink())

    assert _lifecycle._state._worker is None, "a swap must not build a worker to have nothing to drain"
    assert capsys.readouterr().err == "", "and must not absorb a fault it caused itself"


def test_a_late_swap_after_shutdown_does_not_resurrect_the_worker() -> None:
    old, new = SwapSink(), SwapSink()
    worker = _worker_with(old)
    log_foundry.shutdown()
    assert old.closed == 1, "shutdown closed it; the swap must not close it again"

    config.configure(sink=new)

    assert _lifecycle._state._worker is worker, "no new worker"
    assert worker.sink is old, "a retired worker is not retargeted"
    assert old.closed == 1
    assert new.closed == 0
    assert config.get_config().sink is new, "the config still updates"

    worker.submit(_span("late"))
    h = log_foundry.health()
    assert (h.retired, h.submitted_after_shutdown) == (True, 1), "FR-001 still applies"


def test_an_unconfirmable_drain_is_bounded_counted_and_leaves_the_old_sink_open(
    monkeypatch, capsys
) -> None:
    """A hung sink must not make ``configure()`` hang, and must not be closed under its writer."""
    wedged, new = WedgedSink(), SwapSink()
    worker = worker_mod.Worker(wedged, batch_size=1, flush_interval=100.0)
    _lifecycle._state._worker = worker
    try:
        worker.submit(_span("stuck"))
        assert wedged.in_emit.wait(5.0), "the drain thread must be inside emit"
        monkeypatch.setattr(worker_mod, "DEFAULT_SWAP_TIMEOUT", 0.2)

        start = time.monotonic()
        config.configure(sink=new)
        elapsed = time.monotonic() - start

        assert elapsed < 2.0, f"configure() must stay bounded, took {elapsed:.2f}s"
        assert worker.sink is new, "the swap still stands — the caller asked for this sink"
        assert wedged.closed == 0, (
            "left open here: the drain thread may still be inside its emit. SPEC-050 FR-004 "
            "closes it at a later shutdown() that finds the thread ended, not at this swap"
        )
        assert log_foundry.health().incomplete_swaps == 1
        err = capsys.readouterr().err
        assert "could not be confirmed drained" in err
        assert "left open" in err
    finally:
        wedged.release.set()


def test_the_swap_budget_is_the_documented_default(monkeypatch) -> None:
    """``configure()`` must resolve the budget when it runs, not bind it at import.

    A default argument bound at definition time would leave the end-to-end bound unprovable —
    the bounded-swap test above would be pinning a number nothing reads.
    """
    assert worker_mod.DEFAULT_SWAP_TIMEOUT == 5.0

    seen: list[float | None] = []
    monkeypatch.setattr(worker_mod, "DEFAULT_SWAP_TIMEOUT", 1.25)
    monkeypatch.setattr(_lifecycle, "_swap_sink", lambda sink, timeout: seen.append(timeout))

    config.configure(sink=SwapSink())

    assert seen == [1.25]


def test_a_swap_that_raises_does_not_fail_configure(monkeypatch, capsys) -> None:
    """SPEC-025: a sink swap must never be the reason an application cannot start.

    This guards the whole ``swap_sink`` call, not the close inside it — a third-party sink can
    raise from ``emit`` during the drain, or from a ``log_foundry_stop_signal`` setter, and
    ``configure()``
    has never raised for anything but a rejected ceiling.
    """
    worker = _worker_with(SwapSink())

    def exploding_swap(new_sink, timeout=None):
        raise RuntimeError("swap failed")

    monkeypatch.setattr(worker, "swap_sink", exploding_swap)
    new = SwapSink()

    config.configure(sink=new)  # must not raise

    assert config.get_config().sink is new
    assert "swapping the log sink" in capsys.readouterr().err


class _WedgedEmitSink(SwapSink):
    """Blocks the drain thread inside ``emit``, so a ``shutdown`` join can be made to expire."""

    def __init__(self, release: threading.Event) -> None:
        super().__init__()
        self.release = release
        self.in_emit = threading.Event()

    def emit(self, batch: list[dict]) -> None:
        self.in_emit.set()
        self.release.wait(10.0)
        super().emit(batch)


class SlowCloseSink(SwapSink):
    """Blocks in ``close()`` until released, so a bounded close is observable both ways."""

    def __init__(self) -> None:
        super().__init__()
        self.in_close = threading.Event()
        self.release = threading.Event()

    def close(self) -> None:
        self.in_close.set()
        self.release.wait(10.0)
        super().close()


def test_the_swap_budget_bounds_the_previous_sinks_close() -> None:
    """The whole call is bounded: a sink that hangs in ``close()`` must not hang ``configure()``.

    The budget is generous and the close longer still, deliberately. An unconfirmed *drain*
    returns before the close is even reached, so a budget tight enough to race the scheduler
    would fail on setup rather than measuring anything — 10 ms failed 122 runs in 200 on a
    loaded machine while passing 25 for 25 idle. What must be tight is the gap between the
    budget and the close, not the budget.
    """
    budget = 0.3
    slow = SlowCloseSink()
    _worker_with(slow)
    try:
        start = time.monotonic()
        _lifecycle._swap_sink(SwapSink(), timeout=budget)
        elapsed = time.monotonic() - start

        assert slow.in_close.is_set(), "the close was started"
        assert slow.closed == 0, "and configure() returned while it was still running"
        # Against the budget, not against the 10 s close: a bare "did not take forever" passes
        # for a fresh full budget, a 10x join, and a fire-and-forget that never waits at all.
        assert elapsed < budget * 3, f"the wait must track the budget, took {elapsed:.2f}s"
    finally:
        slow.release.set()


def test_the_swap_waits_for_a_close_that_fits_inside_the_budget() -> None:
    """The other half of the bound: waiting is the default, and expiry is the exception.

    The close must take *measurable* time, or a fire-and-forget swap passes on luck — an
    instant close finishes before the assertion runs whether or not anything joined it. The
    elapsed check is what actually distinguishes the two, and it is a floor rather than a
    window, so load can only make it more true.
    """
    close_seconds = 0.2
    old = SwapSink()
    original_close = old.close

    def unhurried_close() -> None:
        time.sleep(close_seconds)
        original_close()

    old.close = unhurried_close  # type: ignore[method-assign]
    _worker_with(old)

    start = time.monotonic()
    _lifecycle._swap_sink(SwapSink(), timeout=5.0)
    elapsed = time.monotonic() - start

    assert old.closed == 1, "a close that fits in the budget has completed on return"
    assert elapsed >= close_seconds, "and configure() waited for it rather than firing and forgetting"


def test_shutdown_gives_an_outstanding_close_a_bounded_grace_to_finish() -> None:
    """A slow-but-succeeding close must not be killed at exit just for being slow.

    This is what makes the daemon closer safe rather than merely available, and it is the case
    the daemon loses on its own: measured, a `KafkaSink`-shaped sink whose ``close()`` *is* its
    delivery lost its buffer to the daemon and kept it under a non-daemon thread. The grace runs
    after the live sink is drained and closed, so it can never cost the sink still receiving
    events.
    """
    slow = SlowCloseSink()
    worker = _worker_with(slow)
    _lifecycle._swap_sink(SwapSink(), timeout=0.3)
    assert slow.in_close.wait(5.0), "the close is outstanding when shutdown begins"
    assert slow.closed == 0

    # Released *during* shutdown, not before it. Releasing first lets the daemon finish on its
    # own and the assertion below then passes whether or not anything joined it — which is how
    # the first version of this test passed against a shutdown that skipped the grace entirely.
    threading.Timer(0.2, slow.release.set).start()
    worker.shutdown(timeout=5.0)

    assert slow.closed == 1, "shutdown waited for the outstanding close rather than abandoning it"


def test_the_grace_is_capped_rather_than_taking_the_whole_shutdown_budget(monkeypatch) -> None:
    """A stuck close must not hold a process at exit for the full 30 s shutdown budget.

    The cap is what distinguishes this from "join with whatever is left": the close already had
    the swap's entire budget before ``shutdown`` was called, so one still running is far more
    likely stuck than slow.
    """
    monkeypatch.setattr(lifecycle, "DEFAULT_CLOSER_GRACE", 0.3)
    hung = SlowCloseSink()
    worker = _worker_with(hung)
    try:
        _lifecycle._swap_sink(SwapSink(), timeout=0.3)
        assert hung.in_close.wait(5.0)

        start = time.monotonic()
        worker.shutdown(timeout=30.0)  # generous budget; the cap is what must bound the wait
        elapsed = time.monotonic() - start

        # The patched cap must be the one that bound the wait, not the shipped 2.0 (SPEC-033
        # FR-005 AC-5). Pointed at a stale module this assertion is what fails: the test would
        # otherwise pass against the real grace under an unchanged name, which is invisible to a
        # `--collect-only` name diff.
        assert elapsed < 1.0, (
            f"the patched 0.3 s grace must be the bound, not the shipped "
            f"{worker_mod.DEFAULT_SHUTDOWN_TIMEOUT}s budget — took {elapsed:.2f}s"
        )
        assert elapsed < 5.0, f"the grace must be capped, not the whole budget — took {elapsed:.2f}s"
        assert hung.closed == 0, "and the hung close is abandoned"
    finally:
        hung.release.set()


def test_the_live_sink_is_closed_before_any_swapped_out_close_is_joined() -> None:
    """Defence in depth, pinned because measurement cannot distinguish it.

    Both orders deliver the live sink identically — the grace cap returns control long before
    anything is at risk, so swapping these two calls leaves the whole suite green. The order
    still matters for the case measurement does not cover: an external deadline killing the
    process *during* the grace, where the live sink would otherwise be the one left unclosed.
    """
    hung = SlowCloseSink()
    worker = _worker_with(hung)
    try:
        _lifecycle._swap_sink(SwapSink(), timeout=0.3)
        assert hung.in_close.wait(5.0)

        order: list[str] = []
        real_close_if_owed, real_join = worker._close_if_owed, worker._join_closers
        worker._close_if_owed = lambda *a: (order.append("live sink"), real_close_if_owed(*a))[1]
        worker._join_closers = lambda t: (order.append("swapped-out"), real_join(t))[1]

        worker.shutdown(timeout=0.5)

        assert order == ["live sink", "swapped-out"], "the live sink is never made to wait"
    finally:
        hung.release.set()


def test_an_expired_first_shutdown_still_grants_the_grace_on_the_next_call() -> None:
    """A wedged worker thread is no reason to abandon a healthy swapped-out close.

    The first ``shutdown`` expires on the drain thread and returns before the grace; the
    ``atexit`` call behind it took the idempotent path and used to return instantly, so a close
    that was moments from finishing got no grace at all — the loss the grace exists to prevent,
    reached through the one path that skipped it.
    """
    slow = SlowCloseSink()
    # batch_size=1 here rather than the shared helper's 1000: this test needs one submission to
    # reach the sink, because wedging the drain thread is how the first shutdown is made to expire.
    worker = worker_mod.Worker(slow, batch_size=1, flush_interval=100.0)
    _lifecycle._state._worker = worker
    wedge = threading.Event()
    wedged = _WedgedEmitSink(wedge)
    try:
        # First leave a close outstanding, then wedge the drain thread — in the other order the
        # swap's own drain either unwedges it or races the reassignment of ``worker.sink``.
        _lifecycle._swap_sink(wedged, timeout=0.3)
        assert slow.in_close.wait(5.0), "slow's close is outstanding"
        worker.submit([{"message": "stuck"}])
        assert wedged.in_emit.wait(5.0), "and the drain thread is now wedged"

        worker.shutdown(timeout=0.3)
        assert worker.health().stopped_reason == "ShutdownTimeout", "the first call expired"
        assert slow.closed == 0

        threading.Timer(0.2, slow.release.set).start()
        worker.shutdown(timeout=5.0)  # the atexit call behind it

        assert slow.closed == 1, "the idempotent path grants the grace too"
    finally:
        wedge.set()
        slow.release.set()


def test_the_grace_is_shared_across_every_outstanding_close() -> None:
    """N stuck closers must cost the grace once, not N times."""
    grace = 0.4
    hung = [SlowCloseSink() for _ in range(4)]
    worker = _worker_with(hung[0])
    try:
        for nxt in hung[1:]:
            _lifecycle._swap_sink(nxt, timeout=0.05)
        _lifecycle._swap_sink(SwapSink(), timeout=0.05)
        assert all(sink.in_close.wait(5.0) for sink in hung), "four closes are outstanding"

        start = time.monotonic()
        worker._join_closers(grace)
        elapsed = time.monotonic() - start

        assert elapsed < grace * 2, f"one shared deadline, not one each — took {elapsed:.2f}s"
    finally:
        for sink in hung:
            sink.release.set()


def test_an_unbounded_shutdown_still_caps_the_grace() -> None:
    """``shutdown(timeout=None)`` is a choice about draining events, not about a stuck close.

    ``None`` is public and documented as available on request, and it is the one input where
    "join with whatever is left of the budget" reads as "join forever". Both halves need
    pinning: a stuck close must not hang the exit, and the grace must not be skipped either.
    """
    hung = SlowCloseSink()
    worker = _worker_with(hung)
    elapsed: list[float] = []
    try:
        _lifecycle._swap_sink(SwapSink(), timeout=0.3)
        assert hung.in_close.wait(5.0)

        # On its own thread deliberately: a regression that joins forever here would otherwise
        # hang this test rather than fail it, and the `finally` that releases the sink would
        # never run. Off-thread, the bound below fails cleanly and teardown still happens.
        def timed_join() -> None:
            start = time.monotonic()
            worker._join_closers(None)
            elapsed.append(time.monotonic() - start)

        joiner = threading.Thread(target=timed_join)
        joiner.start()
        joiner.join(lifecycle.DEFAULT_CLOSER_GRACE * 3)

        assert elapsed, "an unbounded shutdown must still cap the grace, not join forever"
        assert elapsed[0] >= lifecycle.DEFAULT_CLOSER_GRACE, "and must not skip it either"
    finally:
        hung.release.set()


def test_health_does_not_block_behind_the_grace() -> None:
    """``health()`` takes the same lock the roster does; it must not wait on a 2 s join.

    SPEC-026 states ``health()`` is safe to call while delivery is in flight, and an operator
    reads it exactly when things are going wrong — which is when a close is hung.
    """
    hung = SlowCloseSink()
    worker = _worker_with(hung)
    try:
        _lifecycle._swap_sink(SwapSink(), timeout=0.3)
        assert hung.in_close.wait(5.0)

        joining = threading.Thread(target=worker._join_closers, args=(2.0,))
        joining.start()
        try:
            start = time.monotonic()
            worker.health()
            elapsed = time.monotonic() - start
        finally:
            hung.release.set()
            joining.join(10.0)

        assert elapsed < 1.0, f"health() waited on the grace for {elapsed:.2f}s"
    finally:
        hung.release.set()


def test_finished_closers_are_not_retained_between_swaps() -> None:
    """``health()`` prunes the roster, but a process that never calls it must not accumulate.

    A config-watcher reconfiguring on every file change is exactly the shape SPEC-030 exists
    for, and nothing obliges it to poll ``health()``.
    """
    _worker_with(SwapSink())  # the roster is process-global now, not this worker's (SPEC-033)

    for _ in range(50):
        _lifecycle._swap_sink(SwapSink(), timeout=5.0)

    assert len(lifecycle._closers) <= 2, (
        f"finished closers accumulated: {len(lifecycle._closers)} retained across 50 swaps"
    )


def test_an_expired_close_is_neither_abandoned_nor_reported(capsys) -> None:
    """The join decides who waits and nothing else — no counter, no line, no abandoned close.

    This is what makes the threaded close safe here where SPEC-028 reverted it for
    ``shutdown()``: that revert was because an expired join could not tell a slow-but-successful
    close from a stuck one and reported a loss for closes that had completed. Deriving no signal
    from the expiry dissolves the objection rather than arguing with it.
    """
    slow = SlowCloseSink()
    _worker_with(slow)
    try:
        capsys.readouterr()
        _lifecycle._swap_sink(SwapSink(), timeout=0.3)
        assert slow.in_close.wait(5.0)
        assert log_foundry.health().incomplete_swaps == 0, "a slow close is not a failed swap"
        assert capsys.readouterr().err == "", "and is not announced either — no line, no counter"

        slow.release.set()
        assert _eventually(lambda: slow.closed == 1), "the close ran to completion regardless"
    finally:
        slow.release.set()


def test_a_close_still_running_is_visible_in_health_while_it_runs() -> None:
    """The observable that replaces a signal derived from the expiry (review finding F3).

    A live gauge carries none of the ambiguity that made SPEC-028 revert a guessed one: read it
    non-zero and a close *is* running now. Read it non-zero every time and the destination is
    stuck, holding resources nothing will reclaim.
    """
    slow = SlowCloseSink()
    _worker_with(slow)
    try:
        assert log_foundry.health().closing_sinks == 0, "nothing is closing yet"

        _lifecycle._swap_sink(SwapSink(), timeout=0.3)
        assert slow.in_close.wait(5.0)
        assert log_foundry.health().closing_sinks == 1, "the hung close is visible"

        slow.release.set()
        assert _eventually(lambda: log_foundry.health().closing_sinks == 0), (
            "and the gauge falls again once it finishes — it is not a counter"
        )
    finally:
        slow.release.set()


def test_a_closer_thread_that_cannot_start_is_announced_not_run_inline(monkeypatch, capsys) -> None:
    """A process that cannot spawn a thread must not get the unbounded close back instead.

    Falling back to an inline close would reintroduce the wait this whole change removes, in
    the one situation where the process is already under resource pressure.
    """
    sink = SlowCloseSink()
    _worker_with(sink)

    def refuse(self) -> None:
        raise RuntimeError("can't start new thread")

    monkeypatch.setattr(threading.Thread, "start", refuse)

    start = time.monotonic()
    _lifecycle._swap_sink(SwapSink(), timeout=0.3)
    elapsed = time.monotonic() - start

    assert elapsed < 5.0, "the close must not have been run inline as a fallback"
    assert not sink.in_close.is_set(), "and must not have been attempted at all"
    assert "starting the thread that closes a swapped-out sink" in capsys.readouterr().err


def test_the_closer_thread_is_a_daemon(monkeypatch) -> None:
    """Structural, and load-bearing in the opposite direction to the first attempt here.

    A non-daemon closer was tried and is worse: CPython joins non-daemon threads *before*
    ``atexit``, so one hung close stops the exit drain from ever running. The subprocess test in
    ``test_worker.py`` measures that consequence; this one pins the flag it turns on.
    """
    started: list[threading.Thread] = []
    real_thread = threading.Thread

    class RecordingThread(real_thread):  # type: ignore[misc, valid-type]
        def start(self) -> None:
            started.append(self)
            super().start()

    monkeypatch.setattr(threading, "Thread", RecordingThread)
    _worker_with(SwapSink())
    _lifecycle._swap_sink(SwapSink())

    closers = [t for t in started if t.name == "log-foundry-sink-close"]
    assert closers, "the close ran on its own thread"
    assert all(t.daemon for t in closers), "a non-daemon closer holds the whole exit path hostage"


def test_a_sink_that_cannot_close_does_not_fail_configure(capsys) -> None:
    """SPEC-025 FR-004: a failing close is announced, never propagated."""

    class CloseFailsSink(SwapSink):
        def close(self) -> None:
            raise OSError("cannot close")

    _worker_with(CloseFailsSink())
    new = SwapSink()

    config.configure(sink=new)  # must not raise

    assert config.get_config().sink is new
    assert "closing a swapped-out sink" in capsys.readouterr().err


# -- SPEC-055 FR-001: the three stamps are refused at the door, since they bypass sanitize ------


@pytest.mark.parametrize("name", ["service", "version", "env"])
def test_a_stamp_that_cannot_encode_is_rejected(name: str) -> None:
    """A lone surrogate in a stamp would cost every batch on every column-binding sink."""
    with pytest.raises(ValueError, match=name):
        config.configure(**{name: "\udcff"})


@pytest.mark.parametrize("name", ["service", "version", "env"])
def test_a_non_str_stamp_is_rejected(name: str) -> None:
    with pytest.raises(TypeError, match=name):
        config.configure(**{name: 7})


def test_a_rejected_stamp_leaves_the_config_and_the_sink_unstamped() -> None:
    """The checks run before the ownership stamp, so a refused call records nothing."""
    from log_foundry import _lifecycle
    from log_foundry.sinks.null import NullSink

    config.configure(service="before")
    sink = NullSink()
    with pytest.raises(ValueError):
        config.configure(service="\udcff", sink=sink)
    assert config.get_config().service == "before"
    with _lifecycle._owned_lock:
        assert id(sink) not in _lifecycle._owned, "a refused call must not stamp the sink"


def test_a_str_enum_stamp_is_accepted_and_stored_plain() -> None:
    """`str.__str__` is what is stored: a StrEnum member stays acceptable, and never a subclass."""
    from enum import StrEnum

    class Env(StrEnum):
        PROD = "prod"

    config.configure(env=Env.PROD)
    assert config.get_config().env == "prod"
    assert type(config.get_config().env) is str
