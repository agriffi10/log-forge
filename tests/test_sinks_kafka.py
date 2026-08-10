"""SPEC-010/032 — KafkaSink: produce-per-event, keying, close flush, delivery errors, post-close."""

from __future__ import annotations

import json

import pytest

from log_foundry import SinkLosses

from log_foundry.sinks.base import Sink, SinkDeliveryError
from log_foundry.sinks.kafka import KafkaSink


class FakeProducer:
    """Records produce() calls and services the delivery callback (optionally with an error)."""

    def __init__(self, deliver_error: object = None) -> None:
        self.produced: list[tuple] = []
        self.polls = 0
        self.flushes = 0
        self._deliver_error = deliver_error

    def produce(self, topic, value=None, key=None, callback=None) -> None:
        self.produced.append((topic, value, key))
        if callback is not None:
            callback(self._deliver_error, None)  # simulate async delivery result

    def poll(self, timeout) -> None:
        self.polls += 1

    def flush(self, *args) -> None:
        self.flushes += 1


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
