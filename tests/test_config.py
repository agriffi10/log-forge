"""Phase 1 — Config (arch §7). Global settings, set once at startup."""

import pytest

config = pytest.importorskip("log_foundry.config")


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
    model = pytest.importorskip("log_foundry.model")
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
    model = pytest.importorskip("log_foundry.model")
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
    model = pytest.importorskip("log_foundry.model")
    config.configure(max_depth=1)
    span = model.Span(
        trace_id="a" * 32, span_id="b" * 16, parent_span_id=None, name="fn", start_ts=0.0
    )
    event = model.build_event(span, "INFO", "m", fields={"a": 1, "b": "two"}, baggage={})
    assert event["fields"] == {"a": 1, "b": "two"}


# -- SPEC-030 FR-003: a late `configure(sink=...)` swaps the live delivery target ----------

import threading  # noqa: E402
import time  # noqa: E402

log_foundry = pytest.importorskip("log_foundry")
decorator = pytest.importorskip("log_foundry.decorator")
worker_mod = pytest.importorskip("log_foundry.worker")


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


def _worker_with(sink) -> "worker_mod.Worker":
    """Installs a process worker on ``sink`` whose batching triggers cannot fire unaided.

    A large ``batch_size`` and long ``flush_interval`` mean every delivery in these tests is one
    the swap or an explicit ``flush()`` caused, which is what makes the ordering assertions say
    something.
    """
    worker = worker_mod.Worker(sink, batch_size=1000, flush_interval=100.0)
    decorator._worker = worker
    return worker


def test_a_late_sink_swap_routes_subsequent_events_to_the_new_sink() -> None:
    """The measured defect: sink A got 4 events, sink B got 0, and the config claimed B."""
    old, new = SwapSink(), SwapSink()
    worker = _worker_with(old)
    worker.submit(_span("before"))

    config.configure(sink=new)

    worker.submit(_span("after"))
    assert log_foundry.flush(timeout=5.0) is True

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
    real_flush, drains = worker.flush, []

    def recording_flush(timeout=None):
        drains.append((worker.sink, timeout))
        return real_flush(timeout)

    worker.flush = recording_flush
    config.configure(sink=new)

    assert [sink for sink, _ in drains] == [old, new], "drain to the old sink, then fence after"
    assert old.closed == 1, "and only then close it"
    # One deadline covers both, so a hung sink cannot cost twice the budget. The first drain is a
    # real queue round-trip, so the monotonic clock has moved by the time the fence is granted.
    assert drains[1][1] < drains[0][1], "the fence gets what is left of the budget, not a fresh one"


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
        stop_signal = None

    old, new = SwapSink(), SignallingSink()
    worker = _worker_with(old)

    config.configure(sink=new)

    assert new.stop_signal is worker._stop


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
    decorator._worker = None

    config.configure(sink=SwapSink())

    assert decorator._worker is None, "a swap must not build a worker to have nothing to drain"
    assert capsys.readouterr().err == "", "and must not absorb a fault it caused itself"


def test_a_late_swap_after_shutdown_does_not_resurrect_the_worker() -> None:
    old, new = SwapSink(), SwapSink()
    worker = _worker_with(old)
    log_foundry.shutdown()
    assert old.closed == 1, "shutdown closed it; the swap must not close it again"

    config.configure(sink=new)

    assert decorator._worker is worker, "no new worker"
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
    decorator._worker = worker
    try:
        worker.submit(_span("stuck"))
        assert wedged.in_emit.wait(5.0), "the drain thread must be inside emit"
        monkeypatch.setattr(worker_mod, "DEFAULT_SWAP_TIMEOUT", 0.2)

        start = time.monotonic()
        config.configure(sink=new)
        elapsed = time.monotonic() - start

        assert elapsed < 2.0, f"configure() must stay bounded, took {elapsed:.2f}s"
        assert worker.sink is new, "the swap still stands — the caller asked for this sink"
        assert wedged.closed == 0, "left open: the drain thread may still be inside its emit"
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
    monkeypatch.setattr(decorator, "_swap_sink", lambda sink, timeout: seen.append(timeout))

    config.configure(sink=SwapSink())

    assert seen == [1.25]


def test_a_swap_that_raises_does_not_fail_configure(monkeypatch, capsys) -> None:
    """SPEC-025: a sink swap must never be the reason an application cannot start.

    This guards the whole ``swap_sink`` call, not the close inside it — a third-party sink can
    raise from ``emit`` during the drain, or from a ``stop_signal`` setter, and ``configure()``
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


def test_the_swap_budget_does_not_bound_the_previous_sinks_close() -> None:
    """A known, recorded gap (arch §13) — pinned so it cannot be misread as a bounded call.

    ``Sink.close`` takes no timeout, so the swap's deadline covers the two drains and nothing
    after them. Bounding it needs an interruptible close, which is a change to the sink
    contract; SPEC-028 built and reverted the daemon-thread alternative.

    The budget is generous and the close is longer still, deliberately. An unconfirmed drain
    returns *before* the close, so a budget tight enough to race the scheduler would fail this
    test on its first assertion under load rather than measuring anything — 10 ms failed 122
    times in 200 on a loaded machine while passing 25 for 25 on an idle one. What must be tight
    is the gap between the budget and the close, not the budget itself.
    """
    budget, close_seconds, closed_for = 0.3, 0.5, []

    class SlowCloseSink(SwapSink):
        def close(self) -> None:
            time.sleep(close_seconds)
            closed_for.append(True)
            super().close()

    _worker_with(SlowCloseSink())

    start = time.monotonic()
    decorator._swap_sink(SwapSink(), timeout=budget)
    elapsed = time.monotonic() - start

    assert closed_for, "the drain was confirmed and the previous sink closed"
    assert elapsed >= close_seconds, "the close ran to completion, past the swap's own budget"


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
