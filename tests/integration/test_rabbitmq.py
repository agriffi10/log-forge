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
