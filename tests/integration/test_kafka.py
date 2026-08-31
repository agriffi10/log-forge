"""SPEC-041 FR-001 — KafkaSink against a real broker, consuming back what it produced."""

from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING

from confluent_kafka import Consumer

from log_foundry.sinks.kafka import KafkaSink

if TYPE_CHECKING:
    from integration.conftest import Endpoint


def consume(brokers: str, topic: str, expected: int) -> list[tuple[bytes | None, dict]]:
    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": uuid.uuid4().hex,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])
    got: list[tuple[bytes | None, dict]] = []
    try:
        for _ in range(120):
            message = consumer.poll(1.0)
            if message is None or message.error():
                continue
            got.append((message.key(), json.loads(message.value())))
            if len(got) == expected:
                break
    finally:
        consumer.close()
    return got


def test_a_batch_is_produced_and_can_be_consumed_back(
    services_are_up: dict[str, Endpoint],
) -> None:
    brokers = services_are_up["kafka"].url_host
    topic = f"lf-{uuid.uuid4().hex[:8]}"
    sink = KafkaSink(topic, bootstrap_servers=brokers)
    sink.emit([{"n": 1, "trace_id": "a" * 32}, {"n": 2, "trace_id": "a" * 32}])
    sink.close()

    assert sink.losses().failed == 0
    assert [payload["n"] for _, payload in consume(brokers, topic, 2)] == [1, 2]


def test_the_message_key_is_the_configured_field(services_are_up: dict[str, Endpoint]) -> None:
    brokers = services_are_up["kafka"].url_host
    topic = f"lf-{uuid.uuid4().hex[:8]}"
    sink = KafkaSink(topic, bootstrap_servers=brokers, key_field="trace_id")
    sink.emit([{"n": 1, "trace_id": "c" * 32}])
    sink.close()

    keys = [key for key, _ in consume(brokers, topic, 1)]
    assert keys == [b"c" * 32]
