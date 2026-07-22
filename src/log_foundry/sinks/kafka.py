"""KafkaSink — produce events to a Kafka topic (arch §8, §9.1, SPEC-010).

A durable-buffer sink built on ``confluent-kafka`` (the optional ``kafka`` extra, imported lazily).
``produce()`` enqueues locally into the producer's internal batch and returns without blocking; the
delivery result arrives asynchronously on a callback serviced by ``poll()``/``flush()``. ``close()``
flushes so buffered messages are sent before exit. Delivery errors are counted and logged, never
raised out of ``emit``.
"""

from __future__ import annotations

import json
import sys
from typing import Any

__all__ = ["KafkaSink"]


class KafkaSink:
    """A :class:`~log_foundry.sinks.base.Sink` that produces events to a Kafka topic.

    Attributes:
        failed: Messages whose delivery callback reported an error.
    """

    def __init__(
        self,
        topic: str,
        *,
        producer: Any = None,
        bootstrap_servers: str | None = None,
        key_field: str = "trace_id",
    ) -> None:
        if producer is None:
            if bootstrap_servers is None:
                raise ValueError(
                    "KafkaSink requires bootstrap_servers when no producer is injected"
                )
            from confluent_kafka import Producer  # type: ignore[import-not-found]  # 'kafka' extra

            producer = Producer({"bootstrap.servers": bootstrap_servers})
        self.topic = topic
        self.producer = producer
        self.key_field = key_field
        self.failed = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Produce one message per event; serve delivery callbacks without blocking (FR-002)."""
        for event in batch:
            body = json.dumps(event).encode("utf-8")
            key = self._key(event)
            self.producer.produce(self.topic, value=body, key=key, callback=self._on_delivery)
            self.producer.poll(0)  # serve queued delivery callbacks, non-blocking

    def close(self) -> None:
        """Flush the producer so buffered messages are delivered before exit (FR-002)."""
        self.producer.flush()

    # -- internals ----------------------------------------------------------------------

    def _key(self, event: dict[str, object]) -> bytes | None:
        if not self.key_field:
            return None
        value = event.get(self.key_field)
        return str(value).encode("utf-8") if value is not None else None

    def _on_delivery(self, err: object, msg: object) -> None:
        """confluent-kafka delivery callback: count and log failures (FR-002)."""
        if err is not None:
            self.failed += 1
            sys.stderr.write(f"log-foundry: KafkaSink delivery failed: {err!r}\n")
