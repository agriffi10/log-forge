"""RabbitMQSink — publish events to a RabbitMQ exchange (arch §8, §9.1, SPEC-010).

A durable-buffer sink on ``pika`` (the optional ``amqp`` extra, imported lazily). Each event is
published as a **persistent** message (delivery mode 2) to the configured exchange/routing key. A
dropped or closed connection is re-established within a bounded retry before the batch is abandoned
and counted; ``close()`` closes the channel and connection.
"""

from __future__ import annotations

import json
import sys
import time
from typing import Any

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
    """A :class:`~log_forge.sinks.base.Sink` that publishes persistent messages to RabbitMQ."""

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
        self._max_retries = max_retries
        self._owns_connection = connection is None
        self._connection = connection if connection is not None else self._connect()
        self._channel: Any = None
        self._properties: Any = None
        self.failed = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Publish one persistent message per event, reconnecting on error (FR-006)."""
        for event in batch:
            self._publish(json.dumps(event).encode("utf-8"))

    def close(self) -> None:
        """Close the channel and (owned) connection; idempotent (FR-006)."""
        if self._channel is not None:
            _safe_close(self._channel)
            self._channel = None
        if self._connection is not None:
            _safe_close(self._connection)
            self._connection = None

    # -- internals ----------------------------------------------------------------------

    def _publish(self, body: bytes) -> None:
        for attempt in range(self._max_retries + 1):
            try:
                self._active_channel().basic_publish(
                    exchange=self._exchange,
                    routing_key=self._routing_key,
                    body=body,
                    properties=self._persistent_properties(),
                )
                return
            except Exception as err:  # isolation boundary: never crash the worker (FR-011)
                self._reset()
                if attempt < self._max_retries:
                    time.sleep(_BACKOFF_BASE * (2**attempt))
                    continue
                self.failed += 1
                sys.stderr.write(
                    f"log-forge: RabbitMQSink abandoned a message after "
                    f"{self._max_retries + 1} attempts ({err!r})\n"
                )
                return

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
