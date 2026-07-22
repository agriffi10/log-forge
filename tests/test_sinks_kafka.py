"""SPEC-010 — KafkaSink: produce-per-event, keying, close flush, delivery errors (fake producer)."""

from __future__ import annotations

import json

import pytest

from log_foundry.sinks.base import Sink
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


def test_delivery_errors_are_counted() -> None:
    producer = FakeProducer(deliver_error="broker down")
    sink = KafkaSink("logs", producer=producer)
    sink.emit([{"a": 1}, {"a": 2}])
    assert sink.failed == 2


def test_requires_bootstrap_servers_without_producer() -> None:
    with pytest.raises(ValueError):
        KafkaSink("logs")
