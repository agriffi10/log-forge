"""SPEC-041 FR-001 — RabbitMQSink against a real broker.

The queue is declared before the sink publishes, and that is not setup noise. `RabbitMQSink`
publishes to the default exchange with a routing key and declares nothing itself, so AMQP
silently discards a message routed to a queue that does not exist -- a test that skipped the
declaration would pass while delivering nothing.
"""

from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING

import pika
import pytest

from log_foundry.sinks.base import SinkDeliveryError
from log_foundry.sinks.rabbitmq import RabbitMQSink

if TYPE_CHECKING:
    from integration.conftest import Endpoint


def test_a_batch_lands_on_the_declared_queue(services_are_up: dict[str, Endpoint]) -> None:
    url = f"amqp://guest:guest@{services_are_up['rabbitmq'].url_host}/"
    queue = f"lf-{uuid.uuid4().hex[:8]}"

    connection = pika.BlockingConnection(pika.URLParameters(url))
    connection.channel().queue_declare(queue=queue, durable=True)
    connection.close()

    sink = RabbitMQSink(exchange="", routing_key=queue, url=url)
    sink.emit([{"n": 1}, {"n": 2}, {"n": 3}])
    sink.close()

    connection = pika.BlockingConnection(pika.URLParameters(url))
    channel = connection.channel()
    bodies = []
    for _ in range(3):
        _, _, body = channel.basic_get(queue=queue, auto_ack=True)
        if body is not None:
            bodies.append(json.loads(body))
    channel.queue_delete(queue=queue)
    connection.close()

    assert [message["n"] for message in bodies] == [1, 2, 3]


def test_a_confirmed_sink_delivers_a_batch(services_are_up: dict[str, Endpoint]) -> None:
    """SPEC-093 FR-004: with publisher confirms on, every publish the broker acked lands."""
    url = f"amqp://guest:guest@{services_are_up['rabbitmq'].url_host}/"
    queue = f"lf-{uuid.uuid4().hex[:8]}"

    connection = pika.BlockingConnection(pika.URLParameters(url))
    connection.channel().queue_declare(queue=queue, durable=True)
    connection.close()

    sink = RabbitMQSink(exchange="", routing_key=queue, url=url, confirm=True)
    sink.emit([{"n": 1}, {"n": 2}, {"n": 3}])
    assert sink.losses().failed == 0
    sink.close()

    connection = pika.BlockingConnection(pika.URLParameters(url))
    channel = connection.channel()
    bodies = []
    for _ in range(3):
        _, _, body = channel.basic_get(queue=queue, auto_ack=True)
        if body is not None:
            bodies.append(json.loads(body))
    channel.queue_delete(queue=queue)
    connection.close()

    assert [message["n"] for message in bodies] == [1, 2, 3]


@pytest.mark.parametrize("confirm", [False, True], ids=["confirm-off", "confirm-on"])
def test_a_borrowed_connection_outlives_the_sink_that_published_on_it(
    services_are_up: dict[str, Endpoint], confirm: bool
) -> None:
    """SPEC-107 FR-003: ``close()`` ends the sink's channel and leaves the caller's connection.

    The batch is read back on the same connection after the close, which is what an application
    sharing its connection with the sink does next, and what a closed connection cannot do.
    """
    url = f"amqp://guest:guest@{services_are_up['rabbitmq'].url_host}/"
    queue = f"lf-{uuid.uuid4().hex[:8]}"

    connection = pika.BlockingConnection(pika.URLParameters(url))
    connection.channel().queue_declare(queue=queue, durable=True)
    connection.close()

    borrowed = pika.BlockingConnection(pika.URLParameters(url))
    try:
        sink = RabbitMQSink(exchange="", routing_key=queue, connection=borrowed, confirm=confirm)
        sink.emit([{"n": 1}, {"n": 2}, {"n": 3}])
        used = sink._channel
        assert used is not None, "the sink opened a channel, or its close proves nothing"
        sink.close()

        assert borrowed.is_open is True, "close() closed a connection the caller passed in"
        assert used.is_closed is True

        reader = borrowed.channel()
        bodies = []
        for _ in range(3):
            _, _, body = reader.basic_get(queue=queue, auto_ack=True)
            if body is not None:
                bodies.append(json.loads(body))
        reader.queue_delete(queue=queue)
    finally:
        if borrowed.is_open:
            borrowed.close()

    assert [message["n"] for message in bodies] == [1, 2, 3]


def test_an_unroutable_confirmed_publish_is_rejected_and_counted(
    services_are_up: dict[str, Endpoint], capfd: pytest.CaptureFixture[str]
) -> None:
    """SPEC-093 FR-004: ``mandatory`` makes a publish no queue takes ``REJECTED``, counted.

    The default exchange routes by queue name and no queue has this one, so the broker returns the
    message and then acks it, and pika raises ``UnroutableError``. Without ``mandatory`` the broker
    drops it in silence and the sink reads it as delivered, which this test's re-plant shows.
    """
    url = f"amqp://guest:guest@{services_are_up['rabbitmq'].url_host}/"
    routing_key = f"lf-nowhere-{uuid.uuid4().hex[:8]}"
    sink = RabbitMQSink(exchange="", routing_key=routing_key, url=url, confirm=True)
    capfd.readouterr()
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"n": 1}])
    sink.close()
    assert sink.losses().failed == 1
    lines = [line for line in capfd.readouterr().err.splitlines() if "lost" in line]
    assert any("RabbitMQSink, 1 attempt(s), UnroutableError" in line for line in lines), lines
