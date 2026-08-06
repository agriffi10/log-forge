"""KafkaSink — produce events to a Kafka topic (arch §8, §9.1, SPEC-010).

A durable-buffer sink built on ``confluent-kafka`` (the optional ``kafka`` extra, imported lazily).
``produce()`` enqueues locally into the producer's internal batch and returns without blocking; the
delivery result arrives asynchronously on a callback serviced by ``poll()``/``flush()``. ``close()``
flushes so buffered messages are sent before exit. Delivery errors are counted and logged, never
raised out of ``emit``.
"""

from __future__ import annotations

import json
from typing import Any

from log_foundry import _diag

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
        """confluent-kafka delivery callback: count and log failures (FR-002).

        ``err`` is a ``KafkaError``, not an exception, so it goes to :func:`~_diag.lost` rather
        than :func:`~_diag.absorbed`. Its ``str`` is a human message that can quote the record, so
        only the type and the numeric ``code()`` are written — the code is librdkafka's own
        enumeration and is what makes a delivery failure diagnosable (SPEC-029 FR-002).
        """
        if err is not None:
            self.failed += 1
            _diag.lost("message", 1, f"KafkaSink delivery, {type(err).__name__}{_code(err)}")


def _code(err: object) -> str:
    """Render a ``KafkaError``'s numeric code as ``" code=N"``, or ``""`` if it has none.

    ``code()`` is a method on confluent-kafka's ``KafkaError``, returns one of librdkafka's
    integer constants, and carries no caller data. Deliberately narrow: only an ``int`` is written,
    so a stub or a future version returning something else contributes nothing rather than
    smuggling text past the type-name rule.

    Total, and the ``int()`` is inside the guard for the same reason ``_diag.errno_of`` puts it
    there: ``isinstance(code, int)`` admits a *subclass*, whose ``__int__`` is Python that can
    raise — and this runs in a delivery callback, so an escaping exception would surface from
    ``emit`` and cost the whole batch. A diagnostic can never be the failure (SPEC-029 FR-003).
    """
    try:
        code = getattr(err, "code", None)
        value = code() if callable(code) else None
        return f" code={int(value)}" if isinstance(value, int) else ""
    except Exception:
        return ""
