"""SPEC-041 FR-001 — GooglePubSubSink against the Pub/Sub emulator."""

from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING

import pytest

from log_foundry.sinks.pubsub import GooglePubSubSink

if TYPE_CHECKING:
    from integration.conftest import Endpoint

PROJECT = "log-foundry-integration"


@pytest.fixture
def topic_and_subscription(services_are_up: dict[str, Endpoint], monkeypatch):
    # The client library selects the emulator from this variable at construction, so it has to be
    # set before the first client is built rather than passed as an argument.
    monkeypatch.setenv("PUBSUB_EMULATOR_HOST", services_are_up["pubsub"].url_host)
    from google.cloud import pubsub_v1

    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    name = f"lf-{uuid.uuid4().hex[:8]}"
    topic = publisher.topic_path(PROJECT, name)
    subscription = subscriber.subscription_path(PROJECT, name)
    publisher.create_topic(name=topic)
    subscriber.create_subscription(name=subscription, topic=topic)
    yield publisher, subscriber, topic, subscription
    subscriber.delete_subscription(subscription=subscription)
    publisher.delete_topic(topic=topic)


def pull(subscriber, subscription, expected: int) -> list[dict]:
    got: list[dict] = []
    for _ in range(20):
        response = subscriber.pull(
            subscription=subscription, max_messages=expected, return_immediately=True
        )
        for received in response.received_messages:
            got.append(json.loads(received.message.data))
            subscriber.acknowledge(subscription=subscription, ack_ids=[received.ack_id])
        if len(got) >= expected:
            break
    return got


def test_a_batch_is_published_and_can_be_pulled_back(topic_and_subscription) -> None:
    publisher, subscriber, topic, subscription = topic_and_subscription
    sink = GooglePubSubSink(topic, client=publisher)
    sink.emit([{"n": 1}, {"n": 2}, {"n": 3}])
    sink.flush()

    assert sorted(message["n"] for message in pull(subscriber, subscription, 3)) == [1, 2, 3]
    sink.close()
    assert sink.losses().failed == 0


def test_flush_resolves_the_pending_futures(topic_and_subscription) -> None:
    publisher, subscriber, topic, subscription = topic_and_subscription
    sink = GooglePubSubSink(topic, client=publisher)
    sink.emit([{"n": 1}])
    # SPEC-036 FR-002: `emit` appends an unresolved future, and before the flush hook existed
    # nothing but `close()` ever called `result()` on one.
    sink.flush()
    assert pull(subscriber, subscription, 1)
    sink.close()
