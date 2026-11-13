"""RabbitMQSink — publish events to a RabbitMQ exchange (arch §8, §9.1, SPEC-010)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from log_foundry.sinks import _engine
from log_foundry.sinks._args import ReadOnlyCount, refuse_with_injected, store_read_only
from log_foundry.sinks._classify import TRANSIENT_BUILTINS, driver_classes, is_retryable
from log_foundry.sinks._encode import encode_round
from log_foundry.sinks._engine import When, emit_via_deliver, run_context
from log_foundry.sinks._extras import import_extra
from log_foundry.sinks._guard import ClosedGuard, GuardMode
from log_foundry.sinks._refusal import final_refusal
from log_foundry.sinks._retry import floor_count, require_timeout
from log_foundry.sinks._rounds import Called, Lost, mark_sent, raised_line
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict

if TYPE_CHECKING:
    import threading

    from log_foundry.sinks.base import SinkLosses

__all__ = ["RabbitMQSink"]

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
    dropped or closed connection is re-established within a bounded retry. It delivers through
    SPEC-072's engine (SPEC-074 FR-004): each round publishes every remaining message once, in
    order, under the lock for that round only, and the engine resends only the messages that may
    land. The worst-case delay (SPEC-027 FR-005) is ``max_retries`` interruptible waits per run,
    0.7 s at the defaults, taken with no lock held, so a ``close()`` during one returns at once.

    It keeps **no** client buffer (SPEC-036 FR-002): ``basic_publish`` writes the frame
    before it returns, so nothing is queued locally between emits. This sink does not enable
    publisher confirms, so "written" is not "acknowledged" — but that is a delivery-guarantee
    question, not a buffer a flush could empty.

    Attributes:
      log_foundry_counters: The engine's event totals, which :meth:`losses` reports.
      max_retries: The floored constructor value, read-only (SPEC-074 FR-006).
    """

    max_retries: ReadOnlyCount[int] = ReadOnlyCount()

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
            no counter moved. Anything but an ``int`` is refused with ``TypeError`` (SPEC-066
            FR-001), before any connection is opened; a ``bool`` or ``int`` subclass is read as
            the ``int`` it holds.
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
          TypeError: If ``max_retries`` is not an ``int`` (SPEC-066).
          ValueError: If a bound cannot bound anything, or if ``url`` or a bound is passed
            alongside ``connection=``, which is already connected and cannot consume it —
            SPEC-043's rule that an argument no backend can use is an error rather than a silent
            ignore (``url`` since SPEC-071 FR-003).
          ImportError: If the ``amqp`` extra is not installed.
          Exception: Whatever ``pika`` raises when connecting.
        """
        bounds = {
            "blocked_connection_timeout": blocked_connection_timeout,
            "socket_timeout": socket_timeout,
            "stack_timeout": stack_timeout,
        }
        refuse_with_injected(
            "RabbitMQSink",
            "connection",
            connection,
            {
                "url": url,
                "blocked_connection_timeout": blocked_connection_timeout,
                "socket_timeout": socket_timeout,
                "stack_timeout": stack_timeout,
            },
        )
        self._bounds = {
            name: require_timeout(value, name, "RabbitMQSink")
            for name, value in bounds.items()
            if value is not None
        }
        self._exchange = exchange
        self._routing_key = routing_key
        self._url = url
        store_read_only(
            self, "max_retries", floor_count(max_retries, 0, "max_retries", "RabbitMQSink")
        )
        self.log_foundry_stop_signal: threading.Event | None = None
        self._owns_connection = connection is None
        self._connection = connection if connection is not None else self._connect()
        self._channel: Any = None
        self._properties: Any = None
        self.log_foundry_counters = SinkCounters()
        self._guard = ClosedGuard(GuardMode.SERIAL)

    @property
    def _closed(self) -> bool:
        """Whether a close has begun: the 1.x name, read-only, answered by the guard (SPEC-076).

        A 1.x subclass reading ``self._closed`` keeps working. It reads true from the moment a
        close begins, while that close still waits, which is the guard's ``closed``; assigning it
        raises ``AttributeError``, since the guard owns the state.

        Args:
          None.

        Returns:
          The guard's ``closed``.

        Raises:
          None.
        """
        return self._guard.closed

    @property
    def _lock(self) -> threading.Lock:
        """The guard's transport lock under its 1.x name, read-only (SPEC-076 FR-005).

        A 1.x subclass taking ``with self._lock:`` around its own work serialises with the
        guard's sections and its close, as it did with the 1.x lock; assigning it raises
        ``AttributeError``.

        Args:
          None.

        Returns:
          The lock each admitted section and the close hold.

        Raises:
          None.
        """
        return self._guard._lock

    @property
    def _counter_lock(self) -> threading.Lock:
        """The counters' lock, which every commit and ``losses()`` read takes.

        Assigning it replaces the counters' lock, so a test installing a counting lock here
        counts every acquisition the engine makes (SPEC-074 FR-001).

        Args:
          None.

        Returns:
          The lock.

        Raises:
          None.
        """
        return self.log_foundry_counters._counter_lock

    @_counter_lock.setter
    def _counter_lock(self, lock: threading.Lock) -> None:
        """Replaces the counters' lock.

        Args:
          lock: The new lock.

        Returns:
          None.

        Raises:
          None.
        """
        self.log_foundry_counters._counter_lock = lock

    @property
    def failed(self) -> int:
        """Events not confirmed, the engine's ``failed_events``; assignable, as 1.x's attribute was.

        Each lost event once per run (SPEC-074 FR-001), not once per worker attempt.

        Args:
          None.

        Returns:
          The count.

        Raises:
          None.
        """
        return self.log_foundry_counters.failed_events

    @failed.setter
    def failed(self, value: int) -> None:
        """Sets the count, under the counter lock, as assigning the 1.x attribute did.

        Args:
          value: The new count.

        Returns:
          None.

        Raises:
          None.
        """
        with self._counter_lock:
            self.log_foundry_counters.failed_events = value

    def losses(self) -> SinkLosses:
        """Reports the engine's totals: unencodable events, and messages never confirmed.

        ``dropped`` is the events the encode step could not encode (SPEC-069 FR-001), counted only
        by a run that settled; ``failed`` the messages still failing when the run's rounds ran
        out, or refused at their first publish (SPEC-026 FR-002, SPEC-070), each once per run.

        Args:
          None.

        Returns:
          The counters.

        Raises:
          None.
        """
        return self.log_foundry_counters.losses()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Delivers the batch through the engine inline, keeping the 1.x ``emit`` contract.

        A partial publish is counted and left alone: resending it would re-publish the messages
        already on the exchange.

        Args:
          batch: The events to publish.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When the run failed — no message reached the broker (SPEC-026
            FR-001) — with the engine's message. Also when the sink is already closed, by
            identity, with nothing counted: ``_active_channel`` reopens a connection whenever
            it finds none, so a publish after ``shutdown()`` would open one nothing would reap.
        """
        emit_via_deliver(self, batch)

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        """Runs one round and reports each event's fate by index (SPEC-074 FR-004).

        Args:
          batch: The events.

        Returns:
          The round's verdict.

        Raises:
          None. A ``BaseException`` from the driver propagates.
        """
        return _engine.one_round(self._round, batch)

    def _round(self, batch: list[dict[str, object]]) -> Verdict:
        """Publishes every remaining message once, in order, under the lock for this round.

        The driver requirement satisfied (SPEC-028 FR-002): ``pika`` states that one connection
        must not be shared across threads, and this sink shares one connection, one channel, and
        *rebinds* both — ``_reset`` closes and nulls the channel another thread may be
        mid-publish on — so the round takes the transport lock, as ``SocketTransport``'s does. A
        closed sink's round is a refusal carrying 1.x's closed error. Every event is encoded once
        per run (:func:`~log_foundry.sinks._encode.encode_round`), an unencodable one ``DROPPED``
        with SPEC-069's line. A publish that raises runs :meth:`_reset`; a retryable one makes
        that message ``RETRYABLE``, any other ``REJECTED``, and the round goes on to the next.

        Args:
          batch: The events this round is offered.

        Returns:
          A verdict with one outcome per event of ``batch``.

        Raises:
          None. A ``BaseException`` from the driver propagates.
        """
        with self._guard.enter() as section:
            if not section:
                run_context().refuse(
                    final_refusal(
                        f"RabbitMQSink published none of {len(batch)} message(s): "
                        "the sink is closed"
                    )
                )
                return Verdict.uniform(Outcome.REJECTED, len(batch))
            fates = [Outcome.DROPPED] * len(batch)
            lost = Lost(self)
            for position, body in encode_round(self, batch, self._encode, sink=type(self).__name__):
                mark_sent(self, [batch[position]])
                called = self._publish(body)
                if called.error is None:
                    fates[position] = Outcome.ACCEPTED
                    continue
                fates[position] = Outcome.RETRYABLE if called.retryable else Outcome.REJECTED
                kind = type(called.error).__name__
                lost.add(
                    [batch[position]],
                    when=When.LAST if called.retryable else When.ALWAYS,
                    key=kind,
                    line=raised_line("message", "RabbitMQSink", kind),
                )
            lost.register()
        return Verdict(outcomes=tuple(fates))

    def _encode(self, event: dict[str, object]) -> bytes:
        """Encodes one event as the UTF-8 JSON body of its message.

        Args:
          event: The event to encode.

        Returns:
          The message body.

        Raises:
          TypeError: If the event is not JSON-serializable, which ``encode_round`` turns into a
            drop of this one event. Neither source of events produces one: assembly coerces every
            value (``sanitize``), and a ``TransformSink`` checks and coerces what its function
            returns (SPEC-058 FR-001).
        """
        return json.dumps(event).encode("utf-8")

    def close(self) -> None:
        """Closes the channel and, when owned, the connection (FR-006).

        Idempotent through the guard (SPEC-076 FR-002): once a close begins every later round is
        refused at once, the one round inside, if any, is waited for and never released under,
        and the release runs once; a second close waits for the first's release, then returns.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self._guard.close(self._release)

    def _release(self) -> None:
        """Closes the channel, then the connection, dropping each reference.

        :meth:`close` hands this to the guard, which runs it once, with the lock held and
        the state already ``CLOSED``.

        Args:
          None.

        Returns:
          None.

        Raises:
          None. ``_safe_close`` absorbs what either close raises.
        """
        if self._channel is not None:
            _safe_close(self._channel)
            self._channel = None
        if self._connection is not None:
            _safe_close(self._connection)
            self._connection = None

    def _publish(self, body: bytes) -> Called[None]:
        """Publishes one message once (SPEC-074 FR-004); the engine resends it in a later round.

        Args:
          body: The serialized event.

        Returns:
          The call, with its exception when it raised, after :meth:`_reset`.

        Raises:
          None. This is an isolation boundary: a driver fault must never crash the worker
            (FR-011).
        """
        try:
            self._active_channel().basic_publish(
                exchange=self._exchange,
                routing_key=self._routing_key,
                body=body,
                properties=self._persistent_properties(),
            )
        except Exception as err:
            self._reset()
            return Called(None, error=err, retryable=is_retryable(self._retryable_error, err))
        return Called(None)

    def _retryable_error(self, err: Exception) -> bool:
        """Reports whether a failed publish is worth another attempt (SPEC-070 FR-002).

        ``AMQPConnectionError`` is retried, except the six subclasses a reconnect cannot fix —
        ``AuthenticationError``, ``ProbableAuthenticationError``, ``ProbableAccessDeniedError``,
        ``IncompatibleProtocolError``, ``ConnectionClosedByClient`` and
        ``ConnectionOpenAborted`` — and so is ``ChannelWrongStateError``, what a publish on the
        channel a dropped connection left behind raises; :meth:`_reset` has already dropped it, so
        the next attempt reconnects (``pika/exceptions.py``). ``ChannelClosedByBroker`` and
        ``ProtocolSyntaxError`` fail at once. ``NackError`` and ``UnroutableError`` are not
        classified: this sink enables neither ``confirm_delivery`` nor ``mandatory``, so pika
        cannot raise them here.

        Args:
          err: The exception the attempt raised.

        Returns:
          True when another attempt may succeed.

        Raises:
          None. :func:`~log_foundry.sinks._classify.is_retryable` reads a raise here as False.
        """
        excluded = driver_classes(
            "pika.exceptions",
            "AuthenticationError",
            "ProbableAuthenticationError",
            "ProbableAccessDeniedError",
            "IncompatibleProtocolError",
            "ConnectionClosedByClient",
            "ConnectionOpenAborted",
        )
        if isinstance(err, excluded):
            return False
        retried = (
            *TRANSIENT_BUILTINS,
            *driver_classes(
                "pika.exceptions",
                "AMQPConnectionError",
                "ChannelWrongStateError",
            ),
        )
        return isinstance(err, retried)

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
        pika = import_extra("pika", "amqp")
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
                pika = import_extra("pika", "amqp")
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
