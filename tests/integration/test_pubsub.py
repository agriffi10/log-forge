"""SPEC-041 FR-001 — GooglePubSubSink against the Pub/Sub emulator."""

from __future__ import annotations

import json
import time
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
    # `max_pending` is pinned rather than left to the default, and it is not inert even though
    # it currently equals it: `_reap` trims the pending list down to this bound at the end of
    # every `emit`, so a default lowered below the batch size would drain the futures inside
    # `emit` and make the precondition below vacuous.
    sink = GooglePubSubSink(topic, client=publisher, max_pending=1000)

    # The property under test is that `flush()` RESOLVES the pending futures (SPEC-036 FR-002:
    # `emit` appends an unresolved future and, before the hook existed, nothing but `close()`
    # ever called `result()` on one). Asserting only that the messages arrive would stay green
    # with the flush deleted, since the client publishes on its own 10 ms batch latency anyway.
    # So the assertion is on the sink's pending list -- which `_reap` and `close` also empty,
    # but neither of those runs between the emit and the flush below.
    #
    # The precondition is retried rather than asserted once. Nothing here holds the futures
    # open: the client settles them on its own ~10 ms batch latency, so a stall between the
    # last `publish()` and the assertion could empty the list and fail a correct sink. Emitting
    # until some are outstanding makes the setup robust without weakening what is asserted.
    deadline = time.monotonic() + 30.0
    while not sink._futures and time.monotonic() < deadline:
        sink.emit([{"n": n} for n in range(20)])
    assert sink._futures, "precondition: emit must leave unresolved futures for flush to resolve"

    sink.flush()
    assert sink._futures == [], "flush returned with publishes still pending"

    assert pull(subscriber, subscription, 20)
    sink.close()
