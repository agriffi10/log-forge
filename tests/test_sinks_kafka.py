"""SPEC-010/032 — KafkaSink: produce-per-event, keying, close flush, delivery errors, post-close."""

from __future__ import annotations

import json
import sys

import pytest

from log_foundry import SinkLosses
from log_foundry.sinks.base import Sink, SinkDeliveryError
from log_foundry.sinks.kafka import DEFAULT_FLUSH_TIMEOUT, KafkaSink


class FakeProducer:
    """Records produce() calls and services the delivery callback (optionally with an error)."""

    def __init__(self, deliver_error: object = None) -> None:
        self.produced: list[tuple] = []
        self.polls = 0
        self.flushes = 0
        self.flush_timeouts: list[object] = []
        self.still_queued = 0
        self._deliver_error = deliver_error

    def produce(self, topic, value=None, key=None, callback=None) -> None:
        self.produced.append((topic, value, key))
        if callback is not None:
            callback(self._deliver_error, None)  # simulate async delivery result

    def poll(self, timeout) -> None:
        self.polls += 1

    def flush(self, *args) -> int:
        # The real Producer.flush returns the number of messages still queued, and takes a
        # timeout; the double records both so SPEC-038 FR-006 can be asserted on them.
        self.flushes += 1
        self.flush_timeouts.append(args[0] if args else None)
        return getattr(self, "still_queued", 0)


def test_is_a_sink() -> None:
    assert isinstance(KafkaSink("logs", producer=FakeProducer()), Sink)


def test_produces_one_message_per_event_with_key() -> None:
    producer = FakeProducer()
    KafkaSink("logs", producer=producer).emit([{"trace_id": "t1", "a": 1}])
    topic, value, key = producer.produced[0]
    assert topic == "logs"
    assert json.loads(value) == {"trace_id": "t1", "a": 1}
    assert key == b"t1"


def test_key_is_none_when_field_absent() -> None:
    producer = FakeProducer()
    KafkaSink("logs", producer=producer).emit([{"a": 1}])
    assert producer.produced[0][2] is None


def test_empty_key_field_disables_keying() -> None:
    producer = FakeProducer()
    KafkaSink("logs", producer=producer, key_field="").emit([{"trace_id": "t1"}])
    assert producer.produced[0][2] is None


def test_close_flushes() -> None:
    producer = FakeProducer()
    sink = KafkaSink("logs", producer=producer)
    sink.close()
    assert producer.flushes == 1


def test_delivery_errors_are_counted(capsys) -> None:
    producer = FakeProducer(deliver_error="broker down")
    sink = KafkaSink("logs", producer=producer)
    sink.emit([{"a": 1}, {"a": 2}])
    assert sink.failed == 2
    err = capsys.readouterr().err
    assert err.count("lost 1 message(s)") == 2, "one line per failed delivery"
    assert "broker down" not in err, "the driver's text is never written"


def test_requires_bootstrap_servers_without_producer() -> None:
    with pytest.raises(ValueError):
        KafkaSink("logs")


# -- SPEC-029 FR-002/FR-003: the delivery-error line -----------------------------------------


class FakeKafkaError:
    """A ``confluent_kafka.KafkaError``: a numeric ``code()`` and a leaky human ``str``.

    Not an exception, which is why the sink reports it with ``_diag.lost`` rather than
    ``_diag.absorbed`` — and why the code is the only part of it worth writing.
    """

    def __init__(self, code: int = -195) -> None:
        self._code = code

    def code(self) -> int:
        return self._code

    def __str__(self) -> str:
        return "Local: Broker transport failure — record {'user': 'user@example.com'}"

    __repr__ = __str__


def test_a_delivery_error_reports_the_code_not_the_message(capsys) -> None:
    sink = KafkaSink("logs", producer=FakeProducer(deliver_error=FakeKafkaError()))

    sink.emit([{"a": 1}])

    err = capsys.readouterr().err
    assert "user@example.com" not in err, "the driver's text can quote the record (arch §6)"
    assert "Broker transport failure" not in err
    assert "FakeKafkaError" in err, "the type"
    assert "code=-195" in err, "and librdkafka's own code, which is what makes it diagnosable"
    assert sink.failed == 1


class HostileCodeError(FakeKafkaError):
    """A ``code()`` returning an ``int`` subclass whose ``__int__`` raises.

    Contrived, but ``_code`` runs inside a delivery callback: an exception escaping it surfaces
    from ``emit``, and the worker would then count and retry the whole batch because a *diagnostic*
    failed. FR-003 says a diagnostic can never be the failure.
    """

    def code(self) -> object:  # type: ignore[override]
        class Hostile(int):
            def __int__(self) -> int:
                raise ValueError("driver quirk")

        return Hostile(7)


def test_a_hostile_code_cannot_reach_the_caller(capsys) -> None:
    sink = KafkaSink("logs", producer=FakeProducer(deliver_error=HostileCodeError()))

    sink.emit([{"a": 1}])  # must not raise

    err = capsys.readouterr().err
    assert "lost 1 message(s)" in err, "the loss is still announced"
    assert "code=" not in err, "the unusable code contributes nothing rather than failing"
    assert sink.failed == 1


def test_a_closed_sink_refuses_a_produce_without_moving_a_counter() -> None:
    """Refusing is a reported failure, not absorbed loss (SPEC-032 FR-001).

    The distinction matters to an operator reading ``health()``: a refused batch raises, so it
    reaches ``failed_batches`` through the worker. Counting it in ``losses()`` as well would
    report one lost batch twice, in two places meaning different things.
    """
    producer = FakeProducer()
    sink = KafkaSink("logs", producer=producer)
    sink.close()

    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}, {"a": 2}])

    assert sink.losses() == SinkLosses(dropped=0, failed=0), "a refusal moved a loss counter"
    assert producer.produced == []


def test_close_is_idempotent_and_flushes_once() -> None:
    """A second ``close()`` reaches the producer no further than the first.

    ``atexit`` racing user code is the documented case: ``shutdown()`` closes the sink and a
    caller with their own ``close()`` makes the second call. Flushing twice is not harmful in
    itself, but the flag guarding it is the same one ``emit`` reads, so a close that skipped it
    would leave the sink accepting after the first call.
    """
    producer = FakeProducer()
    sink = KafkaSink("logs", producer=producer)
    sink.close()
    sink.close()
    assert producer.flushes == 1


# --- SPEC-038 FR-006: close() is bounded and counts what it lost --------------------------


def test_close_passes_a_bounded_timeout_to_flush() -> None:
    """AC-1. `flush()` with no timeout waits `message.timeout.ms` — five minutes by default.

    `Worker.shutdown` closes the live sink inline and unbounded (arch §13), so an unreachable
    broker held process exit for those five minutes despite `shutdown(timeout=30)`.
    """
    producer = FakeProducer()
    KafkaSink("t", producer=producer, flush_timeout=7.5).close()
    assert producer.flush_timeouts == [7.5], "the wait must be capped, not open-ended"


def test_a_non_zero_remainder_is_counted_and_announced_once(capsys) -> None:
    """AC-2. `flush()` returns what is still queued — exactly the count lost at exit.

    The old call discarded that return value, so the loss was silent.
    """
    producer = FakeProducer()
    producer.still_queued = 42
    sink = KafkaSink("t", producer=producer)
    sink.close()
    assert sink.losses().failed == 42
    err = capsys.readouterr().err
    assert "lost 42 message(s)" in err, "one line carrying the count, not 42 lines"
    assert err.count("KafkaSink") == 1


def test_a_clean_flush_counts_nothing() -> None:
    producer = FakeProducer()
    sink = KafkaSink("t", producer=producer)
    sink.close()
    assert sink.losses().failed == 0


def test_the_exit_flush_gets_its_real_bound_when_driven_through_a_real_shutdown() -> None:
    """AC-3, amended by evidence. The stop signal must NOT shorten this flush.

    A revision cut the timeout to zero while a shutdown was in progress, on the reasoning that
    the events were lost either way. They were not: `produce()` is a local hand-off and
    `flush()` is the only thing that drains the producer's batch, so — since `Worker.shutdown`
    sets the stop event *before* the join, making it always set by the time `close()` runs —
    Kafka's exit delivery was switched off entirely. Measured: 9 buffered, `flush(0)`, zero
    delivered, all 11 booked as failed — 9 `info()` calls plus the span's start and end.

    This drives the real `log_foundry.shutdown()` rather than setting the flag by hand, because
    the hand-set version is exactly what made the defect look correct.
    """
    import log_foundry as lf

    class Draining:
        """Drains its whole batch when given any time at all, and none when given none.

        That is the contrast the defect turns on: `flush()` is the only thing that drains the
        producer, so a zero timeout delivers nothing however healthy the broker.
        """

        def __init__(self) -> None:
            self.buffered = 0
            self.delivered = 0
            self.saw_timeout: object = "never called"

        def produce(self, topic, value=None, key=None, callback=None) -> None:
            self.buffered += 1

        def poll(self, timeout) -> None:
            pass

        def flush(self, timeout=None) -> int:
            self.saw_timeout = timeout
            if timeout is None or timeout > 0:
                self.delivered += self.buffered
                self.buffered = 0
            return self.buffered

    producer = Draining()
    lf.configure(sink=KafkaSink("t", producer=producer, flush_timeout=10.0))

    @lf.trace
    def work() -> None:
        for i in range(9):
            lf.info("event", fields={"i": i})

    work()
    lf.shutdown()

    assert producer.saw_timeout == 10.0, (
        f"the exit flush must get its real bound, not a shutdown-shortened one; "
        f"got {producer.saw_timeout!r}"
    )
    assert producer.delivered == 11, (
        f"9 info() calls plus span.start and span.end; delivered {producer.delivered}"
    )
    assert producer.buffered == 0


def test_a_producer_returning_a_bool_does_not_book_a_phantom_loss() -> None:
    """`bool` is an `int`, so `isinstance(remaining, int)` accepted `True` as "one still queued".

    Reverting to `isinstance` passed the whole suite, because no double returned a bool.
    """
    producer = FakeProducer()
    producer.still_queued = True
    sink = KafkaSink("t", producer=producer)
    sink.close()
    assert sink.losses().failed == 0, "True is not a count of queued messages"


def test_a_flush_timeout_that_bounds_nothing_falls_back_to_the_default() -> None:
    """`0` reinstates the `flush(0)` that switched exit delivery off; `inf` the unbounded wait.

    Floored for the reason `Worker._emit` floors its retries (SPEC-021): a value that disables
    the guard must not arrive silently through the constructor.
    """
    producer = FakeProducer()
    for bad in (0, -1.0, float("inf"), float("nan")):
        sink = KafkaSink("t", producer=producer, flush_timeout=bad)
        assert sink.flush_timeout == DEFAULT_FLUSH_TIMEOUT, f"{bad!r} bounds nothing"
    assert KafkaSink("t", producer=producer, flush_timeout=2.5).flush_timeout == 2.5


# --- SPEC-047 FR-003: librdkafka's own delivery bound is reachable from the constructor --------


class FakeConfluentModule:
    """A stand-in for `confluent_kafka`, capturing the config the Producer was built with.

    Nothing in this suite drove the constructor's *build* path before SPEC-047 -- every test
    injects `producer=` -- so the config it assembles was unobservable.
    """

    def __init__(self) -> None:
        self.configs: list[dict] = []

    # Capitalised to mirror `confluent_kafka.Producer`, which is what the sink imports.
    def Producer(self, config: dict) -> FakeProducer:
        self.configs.append(dict(config))
        return FakeProducer()


def _build(monkeypatch, **kwargs) -> FakeConfluentModule:
    module = FakeConfluentModule()
    monkeypatch.setitem(sys.modules, "confluent_kafka", module)
    KafkaSink("logs", bootstrap_servers="broker:9092", **kwargs)
    return module


def test_producer_config_reaches_the_producer_this_sink_builds(monkeypatch) -> None:
    # FR-003 AC-1. This is the route to the bound that actually governs delivery here:
    # `produce()` is a local hand-off (0.0001 s measured against a dead broker), so librdkafka's
    # `message.timeout.ms` -- measured at a 300.18 s delivery callback by default -- is what
    # decides how long a message is retried, and nothing reached it before.
    module = _build(monkeypatch, producer_config={"message.timeout.ms": 1500, "retries": 2})
    assert module.configs[0]["message.timeout.ms"] == 1500
    assert module.configs[0]["retries"] == 2


def test_the_sinks_own_key_wins_over_producer_config(monkeypatch) -> None:
    # FR-003 AC-2, asserted on the resulting VALUE rather than on the key being present: a merge
    # in the other order leaves the key there too, holding the wrong broker.
    module = _build(
        monkeypatch, producer_config={"bootstrap.servers": "wrong:9092", "retries": 1}
    )
    assert module.configs[0]["bootstrap.servers"] == "broker:9092"
    assert module.configs[0]["retries"] == 1, "the caller's other keys still apply"


def test_omitting_producer_config_builds_exactly_todays_config(monkeypatch) -> None:
    # FR-003 AC-4, by equality: no existing caller's producer changes.
    module = _build(monkeypatch)
    assert module.configs[0] == {"bootstrap.servers": "broker:9092"}


def test_producer_config_alongside_an_injected_producer_is_refused() -> None:
    # FR-003 AC-3. It can only be applied to a producer this sink builds, and ignoring it would
    # silently discard the caller's bound (SPEC-043's rule that an argument no backend can
    # consume is an error, not an ignore).
    with pytest.raises(ValueError, match="producer_config"):
        KafkaSink("logs", producer=FakeProducer(), producer_config={"message.timeout.ms": 1500})
