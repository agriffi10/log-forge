"""RabbitMQSink — publish events to a RabbitMQ exchange (arch §8, §9.1, SPEC-010)."""

from __future__ import annotations

import json
import threading
from typing import Any

from log_foundry import _diag
from log_foundry.sinks._retry import require_timeout, wait
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

__all__ = ["RabbitMQSink"]

_BACKOFF_BASE = 0.1
_PERSISTENT = 2

DEFAULT_BLOCKED_CONNECTION_TIMEOUT = 30.0
"""Seconds a broker may hold the connection blocked before ``pika`` gives up (SPEC-049 FR-005).

``pika``'s own default is ``None``: a broker under a memory or disk alarm sends
``Connection.Blocked`` and every publish then waits indefinitely, on the worker's single drain
thread. Applied only when the AMQP URL's query named no ``blocked_connection_timeout``.
"""


class _PersistentProperties:
    """Fallback persistent message properties, used when ``pika`` is not installed.

    Real usage imports ``pika.BasicProperties``; this stand-in exists only so the sink is
    testable with an injected fake connection in an environment without the ``amqp`` extra.
    """

    delivery_mode = _PERSISTENT


class RabbitMQSink:
    """A :class:`~log_foundry.sinks.base.Sink` publishing persistent messages to RabbitMQ.

    This is a durable-buffer sink on ``pika``, the optional ``amqp`` extra, imported lazily. Each
    event is published as a persistent message to the configured exchange and routing key, and a
    dropped or closed connection is re-established within a bounded retry. The worst-case delay
    (SPEC-027 FR-005) is ``max_retries`` interruptible waits per message, 0.7 s at the defaults.


    It keeps **no** client buffer (SPEC-036 FR-002): ``basic_publish`` writes the frame
    before it returns, so nothing is queued locally between emits. This sink does not enable
    publisher confirms, so "written" is not "acknowledged" — but that is a delivery-guarantee
    question, not a buffer a flush could empty.
    """

    def __init__(
        self,
        *,
        exchange: str,
        routing_key: str,
        connection: Any = None,
        url: str | None = None,
        max_retries: int = 3,
        blocked_connection_timeout: float | None = None,
        socket_timeout: float | None = None,
        stack_timeout: float | None = None,
    ) -> None:
        """Binds the sink to an exchange and connects if no connection was injected.

        Args:
          exchange: The exchange to publish to.
          routing_key: The routing key every message carries.
          connection: A ``pika``-shaped connection to borrow, or ``None`` to open one.
          url: The AMQP URL used when opening a connection.
          max_retries: Reconnect retries per message, floored at zero as ``Worker._emit`` floors
            its own (SPEC-021) — a negative value abandoned each message with no attempt made and
            no counter moved.
          blocked_connection_timeout: Seconds a blocked broker may hold the connection, or
            ``None`` for :data:`DEFAULT_BLOCKED_CONNECTION_TIMEOUT` unless the URL's query names
            ``blocked_connection_timeout`` itself (SPEC-049 FR-005) — the library's default never
            overrides a value the caller wrote, an explicit keyword does. ``pika``'s own default is
            unbounded. A caller who wants that injects their own connection.
          socket_timeout: Seconds for one socket operation, or ``None`` to keep the driver's
            finite 10 s default.
          stack_timeout: Seconds for the full connection handshake, or ``None`` to keep the
            driver's finite 15 s default.

        Returns:
          None.

        Raises:
          ValueError: If a bound cannot bound anything, or if one is passed alongside
            ``connection=``, which is already connected and cannot consume it — SPEC-043's rule
            that an argument no backend can use is an error rather than a silent ignore.
          ImportError: If the ``amqp`` extra is not installed.
          Exception: Whatever ``pika`` raises when connecting.
        """
        bounds = {
            "blocked_connection_timeout": blocked_connection_timeout,
            "socket_timeout": socket_timeout,
            "stack_timeout": stack_timeout,
        }
        supplied = sorted(name for name, value in bounds.items() if value is not None)
        if connection is not None and supplied:
            raise ValueError(
                "RabbitMQSink cannot apply "
                + ", ".join(supplied)
                + " to an injected connection, which is already connected; "
                "pass them where the connection is built, or drop connection="
            )
        self._bounds = {
            name: require_timeout(value, name, "RabbitMQSink")
            for name, value in bounds.items()
            if value is not None
        }
        self._exchange = exchange
        self._routing_key = routing_key
        self._url = url
        self._max_retries = max(max_retries, 0)
        self.log_foundry_stop_signal: threading.Event | None = None
        self._owns_connection = connection is None
        self._connection = connection if connection is not None else self._connect()
        self._channel: Any = None
        self._properties: Any = None
        self.failed = 0
        self._counter_lock = threading.Lock()
        self._lock = threading.Lock()
        self._closed = False

    def losses(self) -> SinkLosses:
        """Reports messages abandoned past the reconnect-retry bound (SPEC-026 FR-002).

        Args:
          None.

        Returns:
          The counters.

        Raises:
          None.
        """
        with self._counter_lock:
            return SinkLosses(dropped=0, failed=self.failed)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Publishes one persistent message per event, reconnecting on error (FR-006).

        A partial publish is counted and left alone: retrying it would re-publish the messages
        already on the exchange.

        The driver requirement satisfied (SPEC-028 FR-002): ``pika`` states that one connection
        must not be shared across threads, and this sink shares one connection, one channel, and
        *rebinds* both — ``_reset`` closes and nulls the channel another thread may be mid-publish
        on. It is structurally the same sink as ``SocketTransport``, one AMQP frame stream over
        one socket, so it takes the same lock.

        Args:
          batch: The events to publish.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When no message reached the broker (SPEC-026 FR-001) — a down broker
            is the case the worker's retry and ``failed_batches`` exist for. Also when the sink
            is already closed, which is not fussiness: ``_active_channel`` reopens a connection
            whenever it finds none, and ``close`` is now idempotent, so an emit arriving after a
            shutdown would open an AMQP connection that nothing will ever reap. One
            ``log_foundry.info()`` after ``shutdown()`` reaches this on the caller's own thread.
        """
        if not batch:
            return
        published = 0
        with self._lock:
            if self._closed:
                raise SinkDeliveryError(
                    f"RabbitMQSink published none of {len(batch)} message(s): the sink is closed"
                )
            for event in batch:
                if self._publish(json.dumps(event).encode("utf-8")):
                    published += 1
        if batch and not published:
            raise SinkDeliveryError(f"RabbitMQSink published none of {len(batch)} message(s)")

    def close(self) -> None:
        """Closes the channel and, when owned, the connection (FR-006).

        Idempotent.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
            if self._channel is not None:
                _safe_close(self._channel)
                self._channel = None
            if self._connection is not None:
                _safe_close(self._connection)
                self._connection = None

    def _publish(self, body: bytes) -> bool:
        """Publishes one message within the retry bound.

        Args:
          body: The serialized event.

        Returns:
          True when the broker took it, False once it is abandoned.

        Raises:
          None. This is an isolation boundary: a driver fault must never crash the worker
            (FR-011).
        """
        for attempt in range(self._max_retries + 1):
            try:
                self._active_channel().basic_publish(
                    exchange=self._exchange,
                    routing_key=self._routing_key,
                    body=body,
                    properties=self._persistent_properties(),
                )
                return True
            except Exception as err:
                self._reset()
                if attempt < self._max_retries:
                    wait(_BACKOFF_BASE * (2**attempt), self.log_foundry_stop_signal)
                    continue
                with self._counter_lock:
                    self.failed += 1
                _diag.lost(
                    "message",
                    1,
                    f"RabbitMQSink, {self._max_retries + 1} attempts, {type(err).__name__}",
                )
                return False
        return False

    def _active_channel(self) -> Any:
        """Returns the open channel, reconnecting and reopening as needed.

        Args:
          None.

        Returns:
          The channel.

        Raises:
          Exception: Whatever connecting or opening a channel raises.
        """
        if self._connection is None:
            self._connection = self._connect()
        if self._channel is None:
            self._channel = self._connection.channel()
        return self._channel

    def _reset(self) -> None:
        """Drops the channel, and an owned connection, so the next attempt reconnects.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        if self._channel is not None:
            _safe_close(self._channel)
            self._channel = None
        if self._owns_connection and self._connection is not None:
            _safe_close(self._connection)
            self._connection = None

    def _connect(self) -> Any:
        """Opens a blocking connection from the configured URL, or the defaults.

        The timeout bounds are applied here rather than in ``__init__`` because this has two
        callers — construction and :meth:`_active_channel`'s reconnect — and two parameter
        constructors, ``URLParameters`` and a bare ``ConnectionParameters``; a bound applied
        anywhere else would be lost on the first reconnect while every test at construction still
        passed (SPEC-049 FR-005). The library's blocked-connection default is set only when the
        parameters ``pika`` parsed from the URL carry ``None``, so ``?blocked_connection_timeout=``
        in the URL survives; an explicit keyword is set unconditionally and so wins.

        Args:
          None.

        Returns:
          The connection.

        Raises:
          ImportError: If the ``amqp`` extra is not installed.
          Exception: Whatever ``pika`` raises when connecting.
        """
        import pika  # type: ignore[import-not-found]

        params = pika.URLParameters(self._url) if self._url else pika.ConnectionParameters()
        for name, value in self._bounds.items():
            setattr(params, name, value)
        if params.blocked_connection_timeout is None:
            params.blocked_connection_timeout = DEFAULT_BLOCKED_CONNECTION_TIMEOUT
        return pika.BlockingConnection(params)

    def _persistent_properties(self) -> Any:
        """Returns the cached persistent message properties, building them on first use.

        Args:
          None.

        Returns:
          ``pika``'s properties, or the local stand-in when the extra is absent.

        Raises:
          None.
        """
        if self._properties is None:
            try:
                import pika

                self._properties = pika.BasicProperties(delivery_mode=_PERSISTENT)
            except ImportError:
                self._properties = _PersistentProperties()
        return self._properties


def _safe_close(resource: Any) -> None:
    """Closes a channel or connection, absorbing a failure.

    Args:
      resource: The channel or connection to close.

    Returns:
      None.

    Raises:
      None. Closing a broken channel or connection must not raise.
    """
    try:
        resource.close()
    except Exception:
        pass
