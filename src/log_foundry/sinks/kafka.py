"""KafkaSink — produce events to a Kafka topic (arch §8, §9.1, SPEC-010)."""

from __future__ import annotations

import json
import threading
from typing import Any

from log_foundry import _diag
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

__all__ = ["KafkaSink"]


class KafkaSink:
    """A :class:`~log_foundry.sinks.base.Sink` that produces events to a Kafka topic.

    This is a durable-buffer sink built on ``confluent-kafka``, the optional ``kafka`` extra,
    imported lazily. ``produce()`` enqueues locally into the producer's internal batch and
    returns without blocking, while the delivery result arrives asynchronously on a callback
    serviced by ``poll()`` and ``flush()``.

    Attributes:
      failed: Messages whose delivery callback reported an error.
      rejected: Messages ``produce()`` itself refused — a full local queue, a serialization fault
        — which never reached the producer's batch at all.

    The driver requirement satisfied (SPEC-028 FR-002): this sink takes **no** transport
    lock. ``confluent-kafka`` documents its ``Producer`` as thread-safe, and this sink adds no state
    of its own to guard — ``produce()`` is a local hand-off into the client's internal queue.

    It refuses an emit after :meth:`close` (SPEC-032 FR-001), which is not the housekeeping it
    looks like: ``flush()`` is the only thing that drains the producer's local batch and services
    its delivery callbacks, so a message produced after it dies with the process — accepted,
    uncounted, and never the subject of a callback. ``emit`` returning normally is what made that
    silent, since the worker then never retried and ``flush()`` reported success.
    """

    def __init__(
        self,
        topic: str,
        *,
        producer: Any = None,
        bootstrap_servers: str | None = None,
        key_field: str = "trace_id",
    ) -> None:
        """Binds the sink to a topic and a producer.

        Args:
          topic: The topic to produce to.
          producer: A ``confluent-kafka``-shaped producer, or ``None`` to build one.
          bootstrap_servers: The broker list, required when no producer is injected.
          key_field: The event key used as the message key, or empty for no key.

        Returns:
          None.

        Raises:
          ValueError: If no producer is injected and no broker list was given.
          ImportError: If the ``kafka`` extra is not installed.
        """
        if producer is None:
            if bootstrap_servers is None:
                raise ValueError(
                    "KafkaSink requires bootstrap_servers when no producer is injected"
                )
            from confluent_kafka import Producer  # type: ignore[import-not-found]

            producer = Producer({"bootstrap.servers": bootstrap_servers})
        self.topic = topic
        self.producer = producer
        self.key_field = key_field
        self.failed = 0
        self.rejected = 0
        self._counter_lock = threading.Lock()
        self._closed = False
        self._close_lock = threading.Lock()

    def losses(self) -> SinkLosses:
        """Reports refused and undelivered messages (SPEC-026 FR-002).

        A callback failure never makes :meth:`emit` raise, and the reason is ownership rather
        than timing — ``poll(0)`` runs inside ``emit``, so a callback for a message produced
        earlier in the same batch can and does fire before it returns. Once the producer has
        accepted a message it owns delivery, including its own retries, and re-producing it from
        here would duplicate whatever the producer eventually lands.

        Args:
          None.

        Returns:
          The counters. Refusals are reported as ``dropped``, since nothing ever left the
          process, while ``failed`` is the delivery callback's verdict on a message the producer
          did accept.

        Raises:
          None.
        """
        with self._counter_lock:
            return SinkLosses(dropped=self.rejected, failed=self.failed)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Produces one message per event and serves delivery callbacks without blocking.

        ``produce()`` is a local hand-off, so what it refuses is the only failure this call can
        observe (FR-002). A partial refusal is counted and left alone, since the messages that
        were accepted are already on their way.

        A closed sink refuses the batch before touching the producer (SPEC-032 FR-001). Refusing
        is not a loss this sink absorbed, so it moves no counter here: it is a failure reported
        to the worker, which records it in ``health().failed_batches``. The check is an unlocked
        read of a write-once flag rather than a lock held across the produce loop, which would
        serialize a client ``confluent-kafka`` documents as thread-safe. A close landing between
        the check and the produce therefore still loses that message, exactly as ``MongoDBSink``
        documents for its own check; what the flag ends is the far larger case of a sink closed
        long before, which is every log written after ``shutdown()``.

        Args:
          batch: The events to produce.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When the sink is closed, or when every message was refused so the
            batch reached nothing — the total failure the worker's retry exists for
            (SPEC-026 FR-001).
        """
        if not batch:
            return
        if self._closed:
            raise SinkDeliveryError(
                f"KafkaSink produced none of {len(batch)} message(s): the sink is closed"
            )
        accepted = 0
        for event in batch:
            body = json.dumps(event).encode("utf-8")
            key = self._key(event)
            try:
                self.producer.produce(self.topic, value=body, key=key, callback=self._on_delivery)
            except Exception as err:
                with self._counter_lock:
                    self.rejected += 1
                _diag.lost("message", 1, f"KafkaSink produce, {type(err).__name__}")
                continue
            accepted += 1
            self.producer.poll(0)
        if batch and not accepted:
            raise SinkDeliveryError(f"KafkaSink produced none of {len(batch)} message(s)")

    def close(self) -> None:
        """Flushes the producer so buffered messages are delivered before exit (FR-002).

        Idempotent, with the flag set under a lock so two concurrent calls cannot both reach
        ``flush()`` — ``atexit`` racing user code is the documented case (SPEC-032 FR-001). The
        flag is set *before* the flush rather than after, so an emit arriving during a flush is
        refused rather than producing into a batch the flush has already walked past.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the producer raises on flush.
        """
        with self._close_lock:
            if self._closed:
                return
            self._closed = True
        self.producer.flush()

    def _key(self, event: dict[str, object]) -> bytes | None:
        """Derives one message's partition key from the configured field.

        Args:
          event: The event being produced.

        Returns:
          The encoded key, or ``None`` when there is no key field or no value.

        Raises:
          None.
        """
        if not self.key_field:
            return None
        value = event.get(self.key_field)
        return str(value).encode("utf-8") if value is not None else None

    def _on_delivery(self, err: object, msg: object) -> None:
        """Counts and logs a delivery failure, as ``confluent-kafka``'s callback (FR-002).

        The error is a ``KafkaError`` rather than an exception, so it goes to
        :func:`~log_foundry._diag.lost` rather than ``absorbed``. Its ``str`` is a human message
        that can quote the record, so only the type and the numeric code are written — the code
        is librdkafka's own enumeration and is what makes a delivery failure diagnosable
        (SPEC-029 FR-002).

        Args:
          err: The delivery error, or ``None`` on success.
          msg: The message the callback describes, unused.

        Returns:
          None.

        Raises:
          None.
        """
        if err is not None:
            with self._counter_lock:
                self.failed += 1
            _diag.lost("message", 1, f"KafkaSink delivery, {type(err).__name__}{_code(err)}")


def _code(err: object) -> str:
    """Renders a ``KafkaError``'s numeric code for a diagnostic.

    ``code()`` returns one of librdkafka's integer constants and carries no caller data. This is
    deliberately narrow: only an ``int`` is written, so a stub or a future version returning
    something else contributes nothing rather than smuggling text past the type-name rule. The
    ``int()`` sits inside the guard for the reason ``_diag.errno_of`` puts it there —
    ``isinstance(code, int)`` admits a subclass whose ``__int__`` is Python that can raise.

    Args:
      err: The delivery error.

    Returns:
      The rendered code, or an empty string when there is none.

    Raises:
      None. This runs in a delivery callback, so an escaping exception would surface from
        ``emit`` and cost the whole batch; a diagnostic can never be the failure (FR-003).
    """
    try:
        code = getattr(err, "code", None)
        value = code() if callable(code) else None
        return f" code={int(value)}" if isinstance(value, int) else ""
    except Exception:
        return ""
