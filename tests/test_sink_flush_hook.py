"""SPEC-036 FR-002 — ``Sink`` gains a flush hook, and ``flush()`` uses it.

``KafkaSink.emit`` hands to librdkafka's local buffer; ``GooglePubSubSink.emit`` appends an
unresolved future; ``SentrySink.emit`` hands to the SDK's background transport. For those sinks
``log_foundry.flush()`` structurally could not reach the data — measured against a stand-in with
that shape: ``flush() -> True``, on the wire 0, in the client buffer 3, ``health()`` all zeros.
"""

from __future__ import annotations

import log_foundry
from conftest import install_worker
from log_foundry import _lifecycle, decorator
from log_foundry.sinks.base import Sink, flush_sink
from log_foundry.worker import Worker


class Buffering:
    """A sink whose ``emit`` buffers in a client, as Kafka and Pub/Sub do."""

    def __init__(self, fail: bool = False) -> None:
        self.buffered: list[dict[str, object]] = []
        self.wire: list[dict[str, object]] = []
        self.fail = fail
        self.flushes = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        self.buffered.extend(batch)

    def flush(self) -> None:
        self.flushes += 1
        if self.fail:
            raise RuntimeError("the client could not be drained")
        self.wire.extend(self.buffered)
        self.buffered.clear()

    def close(self) -> None:
        self.flush()


class Plain:
    """A pre-SPEC-036 sink: ``emit`` and ``close`` and nothing else."""

    def __init__(self) -> None:
        self.got: list[dict[str, object]] = []

    def emit(self, batch: list[dict[str, object]]) -> None:
        self.got.extend(batch)

    def close(self) -> None:
        return None


def test_flush_reaches_the_client_buffer_after_draining_the_queue() -> None:
    """AC-1. The order matters: the queue's events must reach the buffer before it is emptied.

    The worker is installed with a large batch and a long interval, so nothing leaves the queue
    until this call's own marker drains it. A first version let the worker's ordinary triggers
    deliver first, which made the *order* unobservable: flushing the sink before the drain still
    passed, because the events were already in the client buffer by then.
    """
    sink = Buffering()
    log_foundry.configure(service="t", sink=sink)
    install_worker(Worker(sink, batch_size=1000, flush_interval=100.0))

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("in a span")

    work()
    assert sink.buffered == [] and sink.wire == [], (
        "precondition: nothing may have left the queue yet, or the ordering is unobservable"
    )

    assert log_foundry.flush(timeout=5.0)

    on_wire = [e.get("message") for e in sink.wire]
    assert "in a span" in on_wire, f"the client buffer was not reached: {on_wire}"
    assert sink.buffered == [], "and it was emptied, not merely copied"


def test_a_sink_without_the_method_is_unaffected() -> None:
    """AC-2. A pre-SPEC-036 sink still satisfies the protocol and still flushes fine."""
    sink = Plain()
    assert isinstance(sink, Sink)
    assert not hasattr(Sink, "flush"), "flush stays off the Protocol, like losses()"

    log_foundry.configure(service="t", sink=sink)

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("ordinary")

    work()
    assert log_foundry.flush(timeout=5.0)
    assert [e.get("message") for e in sink.got].count("ordinary") == 1


def test_a_failing_sink_flush_is_reported_as_sink_flush() -> None:
    """AC-4, AC-8. Total failure reaches the caller as a reason, never as an exception.

    The queue drained; the client buffer did not. That is a different thing from `"abandoned"`
    — the events are past this library and inside a driver — which is why it gets its own token.
    """
    sink = Buffering(fail=True)
    log_foundry.configure(service="t", sink=sink)

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("will sit in the client")

    work()
    result = log_foundry.flush(timeout=5.0)

    assert not result, "a client buffer that did not go out is not a successful flush"
    assert result.reason == "sink-flush", f"got {result.reason!r}"


def test_the_flush_probe_propagates_where_read_losses_swallows() -> None:
    """AC-4's mechanism, asserted directly: the two probes have opposite failure rules.

    `read_losses` swallows because a broken reporter must not take `health()` down. `flush_sink`
    must not, or a sink's failure is invisible and `flush()` reports success over it.
    """

    class Raises:
        def emit(self, batch: list[dict[str, object]]) -> None:
            return None

        def flush(self) -> None:
            raise RuntimeError("nope")

        def close(self) -> None:
            return None

    try:
        flush_sink(Raises())
    except RuntimeError:
        pass
    else:  # pragma: no cover - the assertion below reports it
        raise AssertionError("flush_sink must propagate, not swallow")

    assert flush_sink(Plain()) is False, "a sink with no flush reports that it had none"


def test_an_orphan_only_process_still_reaches_its_client_buffer() -> None:
    """AC-7. `_flush_worker` returned early on a null worker without touching the sink.

    An orphan-only process with a client-buffering sink would never reach its buffer — the same
    shape SPEC-031 FR-006 and SPEC-033 each found on the close path.
    """
    sink = Buffering()
    log_foundry.configure(service="t", sink=sink)
    log_foundry.info("outside any span")

    assert _lifecycle._state._worker is None, "precondition: no worker was ever built"
    assert log_foundry.flush(timeout=5.0)
    assert [e.get("message") for e in sink.wire] == ["outside any span"]


def test_a_flush_in_a_process_that_never_logged_touches_no_sink() -> None:
    """AC-7's other half, and what keeps FR-001 AC-6 true.

    `configure()` runs `_ensure_sink()` unconditionally, so a bare `configure(service=...)` has
    already built a sink nothing was written to. Gating on that would materialise a flush against
    it — the cost SPEC-031 FR-006 declined for the close path, for the same reason.
    """
    sink = Buffering()
    log_foundry.configure(service="t", sink=sink)

    assert log_foundry.flush(timeout=5.0)
    assert sink.flushes == 0, "nothing was ever logged, so there is nothing to flush"
    assert _lifecycle._state._worker is None


def test_a_closed_sink_refuses_a_flush() -> None:
    """AC-10. SPEC-032's rule reaches this member too: a released transport refuses."""
    from log_foundry.sinks.kafka import KafkaSink

    class FakeProducer:
        def __len__(self) -> int:
            return 0

        def flush(self, timeout: float | None = None) -> int:
            return 0

        def produce(self, *args: object, **kwargs: object) -> None:
            return None

        def poll(self, timeout: float = 0) -> int:
            return 0

    sink = KafkaSink(topic="t", producer=FakeProducer())
    sink.flush()  # open: fine
    sink.close()

    try:
        sink.flush()
    except Exception as exc:
        assert "closed" in str(exc), f"the refusal must say why: {exc}"
    else:  # pragma: no cover - the assertion below reports it
        raise AssertionError("a closed sink must refuse a flush, not touch its released driver")


# --- the wrappers, which hold nothing and must still forward ------------------------------


def test_the_wrapper_sinks_forward_the_flush_to_what_they_wrap() -> None:
    """A wrapper holding nothing is not a wrapper with nothing to do.

    Measured before the fix, with the composition the README itself shows: `MultiSink(Stdout,
    Kafka)` left every message in the client — `flush() -> True`, 0 on the wire, 3 in the buffer.
    Exactly the SPEC-027 finding about `log_foundry_stop_signal`: set on a wrapper it reaches
    nothing, which moves the defect rather than fixing it.
    """
    from log_foundry.sinks.filtering import FilteringSink
    from log_foundry.sinks.multi import MultiSink
    from log_foundry.sinks.transform import TransformSink

    for label, build in (
        ("multi", lambda inner: MultiSink(inner)),
        ("filtering", lambda inner: FilteringSink(inner, predicate=lambda event: True)),
        ("transform", lambda inner: TransformSink(inner, lambda event: event)),
    ):
        inner = Buffering()
        wrapper = build(inner)
        wrapper.emit([{"message": "e0"}])
        assert inner.wire == [], f"{label}: precondition, it is buffered not delivered"

        flush_sink(wrapper)

        assert [e.get("message") for e in inner.wire] == ["e0"], (
            f"{label} did not forward the flush, so its child's client was never reached"
        )


def test_a_multisink_flush_reaches_every_child_before_it_raises() -> None:
    """One failing child must not stop a healthy sibling being drained.

    The raise rule differs from `emit`'s on purpose: `emit` raises only on *total* failure because
    the worker retries a raised batch and a partial retry duplicates. Nothing retries a flush, and
    the caller asked whether everything is out — so any child that could not be drained makes the
    answer no.
    """
    from log_foundry.sinks.multi import MultiSink

    broken, healthy = Buffering(fail=True), Buffering()
    multi = MultiSink(broken, healthy)
    multi.emit([{"message": "e0"}])

    try:
        multi.flush()
    except RuntimeError:
        pass
    else:  # pragma: no cover - the assertion below reports it
        raise AssertionError("a child that could not be flushed must make the flush fail")

    assert [e.get("message") for e in healthy.wire] == ["e0"], (
        "the healthy sibling must still have been flushed, before anything was raised"
    )


# --- the post-close refusals, which no roster arm covers yet -------------------------------


def test_the_buffering_sinks_refuse_a_flush_after_close() -> None:
    """AC-10 per sink. A released transport refuses rather than being touched (SPEC-032).

    Asserted here per class rather than through `POST_CLOSE_BUILDERS`, whose exemption gate reads
    `emit`/`send_all` only and so cannot yet judge a `flush`. Recorded as owed: until that gate
    learns `flush`, these are what stands between a released driver and a call into it — each
    survived the whole suite as a mutant before this test existed.

    The assertion is on the exception **type**, not on its text. A first version accepted any
    exception whose message contained "closed", which a closed `asyncio` loop supplies for free
    (`RuntimeError: Event loop is closed`) — so it passed with NATS's refusal deleted, reporting
    a driver crash as if it were a refusal.
    """
    from log_foundry.sinks.base import SinkDeliveryError
    from log_foundry.sinks.nats import NATSSink
    from log_foundry.sinks.pubsub import GooglePubSubSink

    for label, build in (
        ("GooglePubSubSink", lambda: GooglePubSubSink("projects/p/topics/t", client=object())),
        ("NATSSink", lambda: NATSSink("s", client=object())),
    ):
        sink = build()
        sink.close()
        try:
            sink.flush()
        except SinkDeliveryError as exc:
            assert "closed" in str(exc), f"{label}: the refusal must say why: {exc}"
        except Exception as exc:  # pragma: no cover - reported below
            raise AssertionError(
                f"{label} reached its released driver and crashed ({type(exc).__name__}) "
                "instead of refusing"
            ) from exc
        else:  # pragma: no cover - reported below
            raise AssertionError(f"{label} must refuse a flush after close()")


def test_the_logging_sink_flushes_its_handler_chain() -> None:
    """The handler chain is this sink's transport, and a stdlib handler can buffer.

    A first pass filed `LoggingSink` alongside `MemorySink` and `NullSink` as having nothing
    underneath. `logging.Handler.flush` exists precisely because handlers buffer, and
    `MemoryHandler` does nothing else — measured, three events emitted and nothing on the stream
    until a flush.
    """
    import io
    import logging
    import logging.handlers

    from log_foundry.sinks.logging_sink import LoggingSink

    stream = io.StringIO()
    target = logging.StreamHandler(stream)
    buffering = logging.handlers.MemoryHandler(capacity=1000, target=target)
    logger = logging.getLogger("log_foundry.test.buffered")
    logger.handlers = [buffering]
    logger.propagate = False
    logger.setLevel(logging.INFO)

    sink = LoggingSink(logger=logger)
    sink.emit([{"message": "e0"}, {"message": "e1"}])
    assert stream.getvalue() == "", "precondition: the handler is buffering, not writing through"

    flush_sink(sink)

    assert stream.getvalue() != "", "the handler chain was never flushed"


def test_pubsub_raises_when_a_publish_is_still_in_flight() -> None:
    """AC-4 for the sink the FR names. This survived the whole suite as a mutant.

    An unresolved future is a client buffer that did not go out; returning normally over it is the
    exact "sink the worker believes" the FR exists to remove.
    """
    from log_foundry.sinks.pubsub import GooglePubSubSink

    class NeverSettles:
        def done(self) -> bool:
            return False

        def result(self, timeout: float | None = None) -> None:
            raise TimeoutError("still in flight")

    sink = GooglePubSubSink("projects/p/topics/t", client=object(), overflow_timeout=0.05)
    sink._futures.append(NeverSettles())

    try:
        sink.flush()
    except Exception as exc:
        assert "in flight" in str(exc), f"the failure must name what happened: {exc}"
    else:  # pragma: no cover - the assertion below reports it
        raise AssertionError("a publish still in flight must not read as a successful flush")

    assert sink._futures, "and the future is put back, not dropped — the sink is still open"


# --- behaviour that was load-bearing and unasserted, each found by a surviving mutant -------


def test_pubsub_keeps_an_unwaitable_future_instead_of_abandoning_the_list() -> None:
    """`_Unboundable` must be caught, or the whole swapped-out list is referenced by nothing.

    A future whose `result()` takes no `timeout` cannot be waited on within one — reachable
    through an injected `client=`. `flush()` has already swapped `pending` out of `self._futures`,
    so letting it escape abandons every future in the list: never resolved, never counted, never
    reported. Measured with the catch removed: 0 futures still held, against 2 with it.
    """
    from log_foundry.sinks.pubsub import GooglePubSubSink

    class Unwaitable:
        """`result()` takes no timeout, which is what `_Unboundable` exists to describe."""

        def done(self) -> bool:
            return False

        def result(self) -> None:
            return None

    sink = GooglePubSubSink("projects/p/topics/t", client=object(), overflow_timeout=0.05)
    sink._futures.extend([Unwaitable(), Unwaitable()])

    try:
        sink.flush()
    except Exception as exc:
        assert type(exc).__name__ == "SinkDeliveryError", (
            f"an unwaitable future must be reported, not propagated raw: {type(exc).__name__}"
        )
    else:  # pragma: no cover - reported below
        raise AssertionError("futures still in flight must not read as a successful flush")

    assert len(sink._futures) == 2, (
        "both futures must be put back — the list was swapped out, so nothing else holds them"
    )


def test_pubsub_bounds_the_whole_list_on_one_deadline() -> None:
    """The whole list is bounded by one `overflow_timeout`, not `n` of them.

    What this pins is the **bound**, not the line that implements it, and the distinction is
    worth writing down: moving `deadline` inside the loop does *not* break it, because the first
    future to exhaust the deadline extends the remainder into `unresolved` and breaks. Measured
    both ways at 0.21 s for ten futures. So the property survives that edit, and the test says so
    rather than claiming a mutant it does not kill.
    """
    import time

    from log_foundry.sinks.pubsub import GooglePubSubSink

    class NeverSettles:
        def done(self) -> bool:
            return False

        def result(self, timeout: float | None = None) -> None:
            raise TimeoutError("still in flight")

    sink = GooglePubSubSink("projects/p/topics/t", client=object(), overflow_timeout=0.2)
    sink._futures.extend(NeverSettles() for _ in range(10))

    began = time.monotonic()
    try:
        sink.flush()
    except Exception:
        pass
    elapsed = time.monotonic() - began

    assert elapsed < 1.0, (
        f"ten futures took {elapsed:.2f}s against a 0.2s bound — the deadline is per future, "
        "which is not a bound at all"
    )


def test_the_logging_sink_flushes_handlers_on_an_ancestor_logger() -> None:
    """The walk, not just the first logger — and this is the *default* sink's own path.

    `LoggingSink()` defaults to `logging.getLogger("log_foundry")`, which under `basicConfig()`
    has no handlers of its own: they are on root. Measured with the walk collapsed to one level,
    nothing was ever delivered.
    """
    import io
    import logging
    import logging.handlers

    from log_foundry.sinks.logging_sink import LoggingSink

    stream = io.StringIO()
    buffering = logging.handlers.MemoryHandler(
        capacity=1000, target=logging.StreamHandler(stream)
    )
    parent = logging.getLogger("log_foundry.test.ancestor")
    child = logging.getLogger("log_foundry.test.ancestor.child")
    saved = parent.handlers, child.handlers, parent.propagate
    parent.handlers, child.handlers, parent.propagate = [buffering], [], False
    child.setLevel(logging.INFO)
    try:
        sink = LoggingSink(logger=child)
        sink.emit([{"message": "e0"}])
        assert stream.getvalue() == "", "precondition: buffered on the ancestor's handler"

        flush_sink(sink)

        assert stream.getvalue() != "", "the ancestor's handler was never reached"
    finally:
        parent.handlers, child.handlers, parent.propagate = saved


def test_a_raising_handler_does_not_leave_the_rest_of_the_chain_unflushed() -> None:
    """MultiSink's rule, applied here: attempt every handler, then raise the first failure."""
    import io
    import logging
    import logging.handlers

    from log_foundry.sinks.logging_sink import LoggingSink

    class Broken(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            return None

        def flush(self) -> None:
            raise OSError("the stream is gone")

    stream = io.StringIO()
    healthy = logging.handlers.MemoryHandler(
        capacity=1000, target=logging.StreamHandler(stream)
    )
    logger = logging.getLogger("log_foundry.test.broken")
    saved = logger.handlers, logger.propagate
    logger.handlers, logger.propagate = [Broken(), healthy], False
    logger.setLevel(logging.INFO)
    try:
        sink = LoggingSink(logger=logger)
        sink.emit([{"message": "e0"}])
        try:
            sink.flush()
        except OSError:
            pass
        else:  # pragma: no cover - reported below
            raise AssertionError("a handler that could not flush must make the flush fail")

        assert stream.getvalue() != "", (
            "the healthy handler downstream of the broken one must still have been flushed"
        )
    finally:
        logger.handlers, logger.propagate = saved


def test_the_sentry_sink_flushes_its_client() -> None:
    """The third sink this file's docstring names, and the only one that had no test."""
    from log_foundry.sinks.sentry import SentrySink

    class FakeClient:
        def __init__(self) -> None:
            self.captured: list[object] = []
            self.flushes = 0

        def capture_event(self, event: object) -> None:
            self.captured.append(event)

        def flush(self) -> None:
            self.flushes += 1

    client = FakeClient()
    sink = SentrySink(client=client)
    sink.emit([{"message": "e0", "level": "ERROR"}])

    flush_sink(sink)

    assert client.flushes == 1, "the SDK's background transport was never pushed"


def test_the_sink_buffer_is_drained_even_when_the_sweep_failed() -> None:
    """A failed sweep must not skip the client-buffer drain.

    `worker.flush()` has already pushed the queue *into* that buffer by then, so returning early
    leaves exactly the events most worth saving before a freeze. The reason reported is still the
    most upstream failure, because that is the one to fix.
    """
    sink = Buffering()
    log_foundry.configure(service="t", sink=sink)

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("queued")

    work()

    real = decorator._sweep_open_spans

    def explode() -> None:
        raise RuntimeError("the sweep failed")

    decorator._sweep_open_spans = explode  # type: ignore[assignment]
    try:
        result = log_foundry.flush(timeout=5.0)
    finally:
        decorator._sweep_open_spans = real  # type: ignore[assignment]

    assert not result and result.reason == "abandoned", f"got {result.reason!r}"
    assert "queued" in [e.get("message") for e in sink.wire], (
        "the client buffer must still have been drained: the queue was pushed into it first"
    )
    assert sink.buffered == [], "and emptied, not left holding the span's events"
