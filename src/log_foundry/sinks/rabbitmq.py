"""RabbitMQSink — publish events to a RabbitMQ exchange (arch §8, §9.1, SPEC-010).

A durable-buffer sink on ``pika`` (the optional ``amqp`` extra, imported lazily). Each event is
published as a **persistent** message (delivery mode 2) to the configured exchange/routing key. A
dropped or closed connection is re-established within a bounded retry before the batch is abandoned
and counted; ``close()`` closes the channel and connection.
"""

from __future__ import annotations

import json
import time
from typing import Any

from log_foundry import _diag
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

__all__ = ["RabbitMQSink"]

_BACKOFF_BASE = 0.1
_PERSISTENT = 2  # pika delivery mode for persistent messages


class _PersistentProperties:
    """Fallback message properties (delivery_mode=persistent) used when ``pika`` is not installed.

    Real usage imports ``pika.BasicProperties``; this stand-in only exists so the sink is testable
    with an injected fake connection in an environment without the ``amqp`` extra.
    """

    delivery_mode = _PERSISTENT


class RabbitMQSink:
    """A :class:`~log_foundry.sinks.base.Sink` that publishes persistent messages to RabbitMQ."""

    def __init__(
        self,
        *,
        exchange: str,
        routing_key: str,
        connection: Any = None,
        url: str | None = None,
        max_retries: int = 3,
    ) -> None:
        self._exchange = exchange
        self._routing_key = routing_key
        self._url = url
        # Floored as ``Worker._emit`` floors its own (SPEC-021): a negative value abandoned
        # each message with no attempt made and no counter moved.
        self._max_retries = max(max_retries, 0)
        self._owns_connection = connection is None
        self._connection = connection if connection is not None else self._connect()
        self._channel: Any = None
        self._properties: Any = None
        self.failed = 0

    def losses(self) -> SinkLosses:
        """Messages abandoned past the reconnect-retry bound (SPEC-026 FR-002). Never raises."""
        return SinkLosses(dropped=0, failed=self.failed)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Publish one persistent message per event, reconnecting on error (FR-006).

        Raises when *no* message reached the broker (SPEC-026 FR-001) — a down broker is the
        case the worker's retry and ``failed_batches`` exist for. A partial publish is counted
        and left alone: retrying it would re-publish the messages already on the exchange.
        """
        published = 0
        for event in batch:
            if self._publish(json.dumps(event).encode("utf-8")):
                published += 1
        if batch and not published:
            raise SinkDeliveryError(f"RabbitMQSink published none of {len(batch)} message(s)")

    def close(self) -> None:
        """Close the channel and (owned) connection; idempotent (FR-006)."""
        if self._channel is not None:
            _safe_close(self._channel)
            self._channel = None
        if self._connection is not None:
            _safe_close(self._connection)
            self._connection = None

    # -- internals ----------------------------------------------------------------------

    def _publish(self, body: bytes) -> bool:
        """Publish one message within the retry bound; ``False`` once it is abandoned."""
        for attempt in range(self._max_retries + 1):
            try:
                self._active_channel().basic_publish(
                    exchange=self._exchange,
                    routing_key=self._routing_key,
                    body=body,
                    properties=self._persistent_properties(),
                )
                return True
            except Exception as err:  # isolation boundary: never crash the worker (FR-011)
                self._reset()
                if attempt < self._max_retries:
                    time.sleep(_BACKOFF_BASE * (2**attempt))
                    continue
                self.failed += 1
                _diag.lost(
                    "message",
                    1,
                    f"RabbitMQSink, {self._max_retries + 1} attempts, {type(err).__name__}",
                )
                return False
        return False  # unreachable: the loop returns on every path (mypy needs the exit)

    def _active_channel(self) -> Any:
        if self._connection is None:
            self._connection = self._connect()
        if self._channel is None:
            self._channel = self._connection.channel()
        return self._channel

    def _reset(self) -> None:
        """Drop the channel (and an owned connection) so the next attempt reconnects."""
        if self._channel is not None:
            _safe_close(self._channel)
            self._channel = None
        if self._owns_connection and self._connection is not None:
            _safe_close(self._connection)
            self._connection = None

    def _connect(self) -> Any:
        import pika  # type: ignore[import-not-found]  # optional 'amqp' extra

        params = pika.URLParameters(self._url) if self._url else pika.ConnectionParameters()
        return pika.BlockingConnection(params)

    def _persistent_properties(self) -> Any:
        if self._properties is None:
            try:
                import pika  # optional 'amqp' extra (type-ignored at the import in _connect)

                self._properties = pika.BasicProperties(delivery_mode=_PERSISTENT)
            except ImportError:
                self._properties = _PersistentProperties()
        return self._properties


def _safe_close(resource: Any) -> None:
    try:
        resource.close()
    except Exception:  # closing a broken channel/connection must not raise
        pass
