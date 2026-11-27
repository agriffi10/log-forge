"""RabbitMQSink — publish events to a RabbitMQ exchange (arch §8, §9.1, SPEC-010)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from log_foundry import _diag
from log_foundry.sinks import _engine
from log_foundry.sinks._args import ReadOnlyCount, refuse_with_injected, store_read_only
from log_foundry.sinks._classify import (
    TRANSIENT_BUILTINS,
    driver_classes,
    is_retryable,
    may_have_landed,
)
from log_foundry.sinks._encode import encode_round
from log_foundry.sinks._engine import When, emit_via_deliver, run_context
from log_foundry.sinks._extras import import_extra
from log_foundry.sinks._guard import ClosedGuard, GuardMode
from log_foundry.sinks._refusal import final_refusal
from log_foundry.sinks._retry import floor_count, require_timeout
from log_foundry.sinks._rounds import Called, Lost, mark_may_have_landed, mark_sent, raised_line
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
    dropped or closed connection the sink opened is re-established within a bounded retry; on a
    borrowed connection only the channel is reopened, and one that is itself gone is never
    replaced, so every publish on it fails from then on. It delivers through SPEC-072's engine
    (SPEC-074 FR-004): each round publishes every remaining message once, in order, under the lock
    for that round only, and the engine resends only the messages that may land. The worst-case
    delay (SPEC-027 FR-005) is ``max_retries`` interruptible waits per run, 0.7 s at the defaults,
    taken with no lock held, so a ``close()`` during one returns at once.

    It keeps **no** client buffer (SPEC-036 FR-002): ``basic_publish`` writes the frame
    before it returns, so nothing is queued locally between emits.

    A connection passed as ``connection=`` is borrowed and stays the caller's (SPEC-107): it is
    never closed by the sink, at ``close()`` or after a failed publish, and the sink keeps its
    reference to it, so it never opens a connection of its own in its place. The channel is the
    sink's own in both modes, opened on whichever connection it holds, and ``close()`` closes it.
    A connection the sink opened from ``url=`` is closed with the sink.

    Publisher confirms are opt-in, ``confirm=True``, as Andrew chose on 2026-11-15 ("Off, opt
    in"; SPEC-093 FR-004). With `confirm=False`, the default, a publish that returned is all this
    sink can know: the frame was written, and nothing says a queue took it, so a message routed
    nowhere or written to a connection that died while idle reads as delivered. With confirms on,
    every channel the sink opens is put in confirm mode and each publish carries ``mandatory``,
    so a publish returns only once the broker acked it, a nack or an unroutable return is
    ``REJECTED``, and a connection lost before the confirm is ``UNKNOWN``, counted with a
    may-have-landed line and never sent again. Before a round's first publish an owned
    connection is asked to process its input once, so one that died while idle is replaced
    before any frame is written. A borrowed connection is not probed before a round, so a
    publish on one that died while idle reads as a lost confirm: probing it would run the
    caller's own consumer callbacks on the worker's thread. A confirm costs a broker round trip
    per message, since a blocking channel cannot pipeline them, and its wait is bounded only by
    the connection: its heartbeat, ``blocked_connection_timeout`` or a socket error.

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
        confirm: bool = False,
    ) -> None:
        """Binds the sink to an exchange and connects if no connection was injected.

        Args:
          exchange: The exchange to publish to.
          routing_key: The routing key every message carries.
          connection: A ``pika``-shaped connection to borrow, or ``None`` to open one. A
            connection passed in is never closed by the sink (SPEC-107); the channel the sink
            opens on it is, at ``close()``.
          url: The AMQP URL used when opening a connection.
          max_retries: Retries per message after a failed publish, each on a reopened channel,
            floored at zero as ``Worker._emit`` floors its own (SPEC-021) — a negative value
            abandoned each message with no attempt made and no counter moved. Anything but an
            ``int`` is refused with ``TypeError`` (SPEC-066 FR-001), before any connection is
            opened; a ``bool`` or ``int`` subclass is read as the ``int`` it holds.
          blocked_connection_timeout: Seconds a blocked broker may hold the connection, or
            ``None`` for :data:`DEFAULT_BLOCKED_CONNECTION_TIMEOUT` unless the URL's query names
            ``blocked_connection_timeout`` itself (SPEC-049 FR-005) — the library's default never
            overrides a value the caller wrote, an explicit keyword does. ``pika``'s own default is
            unbounded. A caller who wants that injects their own connection.
          socket_timeout: Seconds for one socket operation, or ``None`` to keep the driver's
            finite 10 s default.
          stack_timeout: Seconds for the full connection handshake, or ``None`` to keep the
            driver's finite 15 s default.
          confirm: Whether to ask the broker to confirm each publish (SPEC-093 FR-004), for an
            owned and a borrowed connection alike, since the sink opens its own channel and
            AMQP's ``confirm.select`` is scoped to it. ``False`` by default. A ``bool`` only: any
            other value is refused rather than read for its truth, since the argument is new and
            ``"false"`` would otherwise turn confirms on.

        Returns:
          None.

        Raises:
          TypeError: If ``max_retries`` is not an ``int`` (SPEC-066), or ``confirm`` is not a
            ``bool`` (SPEC-093 FR-004).
          ValueError: If a bound cannot bound anything, or if ``url`` or a bound is passed
            alongside ``connection=``, which is already connected and cannot consume it —
            SPEC-043's rule that an argument no backend can use is an error rather than a silent
            ignore (``url`` since SPEC-071 FR-003).
          ImportError: If the ``amqp`` extra is not installed.
          Exception: Whatever ``pika`` raises when connecting.
        """
        if not _diag.is_a(confirm, bool):
            raise TypeError(f"RabbitMQSink confirm must be a bool, not {_diag.type_name(confirm)}")
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
        self._confirming = False
        self._confirm = confirm
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
            identity, with nothing counted. The reconnect in ``_active_channel`` is reached only
            by a sink that opened its own connection, so for one a publish after ``shutdown()``
            would open a connection nothing would reap; on a borrowed connection, whose
            reference is kept (SPEC-107), it would publish on the caller's connection after
            the close.
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
        *rebinds* the channel, and with it a connection it opened — ``_reset`` closes and nulls
        the channel another thread may be mid-publish on — so the round takes the transport
        lock, as ``SocketTransport``'s does. A closed sink's round is a refusal carrying 1.x's
        closed error. Every event is encoded once per run
        (:func:`~log_foundry.sinks._encode.encode_round`), an unencodable one ``DROPPED``
        with SPEC-069's line. A publish that raises runs :meth:`_reset`; a retryable one makes
        that message ``RETRYABLE``, any other ``REJECTED``, and the round goes on to the next.

        With ``confirm=True`` (SPEC-093 FR-004) the round first probes an owned connection
        (:meth:`_probe_owned`). A confirming publish whose connection was lost before the broker
        answered is ``UNKNOWN``: it may have landed, so it is never sent again, and the round
        writes one ``ALWAYS`` line, ``lost N message(s); RabbitMQSink, no confirm before the
        connection was lost (<Type>); may have landed, not retried``.

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
                        self,
                        f"RabbitMQSink published none of {len(batch)} message(s): "
                        "the sink is closed"
                    )
                )
                return Verdict.uniform(Outcome.REJECTED, len(batch))
            self._probe_owned()
            fates = [Outcome.DROPPED] * len(batch)
            lost = Lost(self)
            unknown: list[str] = []
            name = _diag.type_name(self)
            for position, body in encode_round(self, batch, self._encode, sink=name):
                mark_sent(self, [batch[position]])
                called = self._publish(body)
                if called.error is None:
                    fates[position] = Outcome.ACCEPTED
                    continue
                kind = _diag.type_name(called.error)
                if called.result == "unknown":
                    fates[position] = Outcome.UNKNOWN
                    unknown.append(kind)
                    continue
                fates[position] = Outcome.RETRYABLE if called.retryable else Outcome.REJECTED
                mark_may_have_landed([position], called.error, called.retryable, called.reached)
                lost.add(
                    [batch[position]],
                    when=When.LAST if called.retryable else When.ALWAYS,
                    key=kind,
                    line=raised_line("message", "RabbitMQSink", kind),
                )
            lost.register()
            if unknown:
                count = len(unknown)
                first = unknown[0]
                run_context().announce(
                    lambda: _diag.lost(
                        "message",
                        count,
                        f"RabbitMQSink, no confirm before the connection was lost ({first}); "
                        "may have landed, not retried",
                    ),
                    when=When.ALWAYS,
                )
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
        """Closes the channel and, only when the sink opened it, the connection (SPEC-107 FR-001).

        A connection passed as ``connection=`` is the caller's: it is left open, and the sink's
        reference to it is kept. The channel is the sink's own in both modes and is closed.

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
        """Runs the sink's one release, :meth:`_reset`, as the close the guard makes once.

        The channel is closed and its reference dropped in both modes. The connection is closed
        and its reference dropped only when the sink opened it; a borrowed connection is left
        open and its reference kept (SPEC-107). The reference is kept because
        :meth:`_active_channel` connects whenever it finds none, and a borrowed sink has no URL
        to connect with, ``url=`` being refused beside ``connection=``: it would open a
        connection to ``pika``'s default broker that nothing would ever close, since the sink
        does not own it and this release has already run. Calling :meth:`_reset` rather than
        restating it keeps the ownership test in one place, so the two release sites cannot
        drift apart.

        :meth:`close` hands this to the guard, which runs it once, with the lock held and
        the state already ``CLOSED``.

        Args:
          None.

        Returns:
          None.

        Raises:
          None. ``_safe_close`` absorbs what either close raises.
        """
        self._reset()

    def _publish(self, body: bytes) -> Called[str]:
        """Publishes one message once (SPEC-074 FR-004); the engine resends it in a later round.

        Getting the channel and publishing are guarded apart, so a failure to get a channel,
        ``confirm_delivery`` raising among them, is read by the predicate as before. On a
        confirming channel (SPEC-093 FR-004) the publish carries ``mandatory=True`` and returns
        only once the broker acked it. ``UnroutableError`` and ``NackError`` are the broker's
        answer, keeping nothing, so they are refusals that keep the channel; a resend would earn
        the same answer. ``StreamLostError``, ``ConnectionClosedByBroker``,
        ``AMQPHeartbeatTimeout`` and ``ConnectionBlockedTimeout`` raised while confirming mean
        the connection went before the confirm, so the frame may have landed: the call is marked
        ``"unknown"`` and :meth:`_reset` run. Anything else goes to the predicate.

        Any other raise says whether the message may have landed (SPEC-095 FR-002), by class,
        since pika does not retry inside a publish: one of the errors that leave a publish
        unconfirmed — ``StreamLostError``, ``ConnectionClosedByBroker``, ``AMQPHeartbeatTimeout``
        or ``ConnectionBlockedTimeout`` — may have, and any other pika ``AMQPError``, such as a
        channel or connection it could not open, did not. An exception outside pika's tree may
        have, unless :func:`~log_foundry.sinks._classify.may_have_landed` clears it. Both
        guarded calls answer it, as the single guard before SPEC-093 did.

        Args:
          body: The serialized event.

        Returns:
          The call, with its exception when it raised, after :meth:`_reset`; its result is
          ``"unknown"`` for a confirming publish whose confirm was lost.

        Raises:
          None. This is an isolation boundary: a driver fault must never crash the worker
            (FR-011).
        """
        try:
            channel = self._active_channel()
        except Exception as err:
            self._reset()
            return Called(
                None,
                error=err,
                retryable=is_retryable(self._retryable_error, err),
                reached=self._may_have_landed(err),
            )
        try:
            channel.basic_publish(
                exchange=self._exchange,
                routing_key=self._routing_key,
                body=body,
                properties=self._persistent_properties(),
                **({"mandatory": True} if self._confirming else {}),
            )
        except Exception as err:
            refused = driver_classes("pika.exceptions", "UnroutableError", "NackError")
            if _diag.is_a(err, refused):
                return Called(None, error=err, retryable=False)
            self._reset()
            unconfirmed = driver_classes(
                "pika.exceptions",
                "StreamLostError",
                "ConnectionClosedByBroker",
                "AMQPHeartbeatTimeout",
                "ConnectionBlockedTimeout",
            )
            if self._confirming and _diag.is_a(err, unconfirmed):
                return Called("unknown", error=err)
            return Called(
                None,
                error=err,
                retryable=is_retryable(self._retryable_error, err),
                reached=self._may_have_landed(err),
            )
        return Called(None)

    def _may_have_landed(self, err: Exception) -> bool:
        """Whether a publish that raised ``err`` may have reached the broker (SPEC-095 FR-002).

        Args:
          err: The exception the publish raised.

        Returns:
          True for an unconfirmed-publish class of pika's, or for an exception outside pika's
          tree that :func:`~log_foundry.sinks._classify.may_have_landed` does not clear.

        Raises:
          None.
        """
        if not _diag.is_a(err, driver_classes("pika.exceptions", "AMQPError")):
            return may_have_landed(err)
        unconfirmed = driver_classes(
            "pika.exceptions",
            "StreamLostError",
            "ConnectionClosedByBroker",
            "AMQPHeartbeatTimeout",
            "ConnectionBlockedTimeout",
        )
        return _diag.is_a(err, unconfirmed)

    def _retryable_error(self, err: Exception) -> bool:
        """Reports whether a failed publish is worth another attempt (SPEC-070 FR-002).

        ``AMQPConnectionError`` is retried, except the six subclasses a reconnect cannot fix —
        ``AuthenticationError``, ``ProbableAuthenticationError``, ``ProbableAccessDeniedError``,
        ``IncompatibleProtocolError``, ``ConnectionClosedByClient`` and
        ``ConnectionOpenAborted`` — and so is ``ChannelWrongStateError``, what a publish on the
        channel a dropped connection left behind raises; :meth:`_reset` has already dropped it, so
        the next attempt opens a new channel, on a new connection when the sink opened its own
        (``pika/exceptions.py``). On a borrowed connection that is itself gone, opening the
        channel fails as well, with ``ConnectionWrongStateError``, an ``AMQPConnectionError`` this
        predicate retries: each retry fails the same way and the message is counted lost once
        the retries are spent, since the sink never replaces a connection it was handed.
        ``ChannelClosedByBroker`` and ``ProtocolSyntaxError`` fail at once. ``NackError`` and
        ``UnroutableError`` never reach this predicate: pika raises them only on a confirming
        channel (``confirm=True``), and :meth:`_publish` reads each as a broker refusal,
        ``REJECTED``, before asking it.

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
        if _diag.is_a(err, excluded):
            return False
        retried = (
            *TRANSIENT_BUILTINS,
            *driver_classes(
                "pika.exceptions",
                "AMQPConnectionError",
                "ChannelWrongStateError",
            ),
        )
        return _diag.is_a(err, retried)

    def _probe_owned(self) -> None:
        """Asks an owned connection to process its input once, so a dead one is replaced first.

        Only with ``confirm=True`` and an owned connection (SPEC-093 FR-004). A connection that
        died while idle is found here, before any frame is written, so its round's events stay
        ``RETRYABLE`` through a reconnect rather than reading as a lost confirm. The method is
        probed by name, and one that raises resets the connection. A borrowed connection is never
        probed: ``process_data_events`` dispatches every channel's pending consumer callbacks,
        which would run the caller's own code on the worker's thread.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        if not (self._confirm and self._owns_connection and self._connection is not None):
            return
        probe = getattr(self._connection, "process_data_events", None)
        if not callable(probe):
            return
        try:
            probe(time_limit=0)
        except Exception:
            self._reset()

    def _active_channel(self) -> Any:
        """Returns the open channel, reconnecting and reopening as needed.

        The reconnect is reached only by a sink that opened its own connection: no release drops
        a borrowed connection's reference (SPEC-107 FR-002), so on one only the channel is ever
        reopened, and when the borrowed connection is itself gone, opening the channel raises.

        With ``confirm=True`` each channel it opens is put in confirm mode before its first
        publish (SPEC-093 FR-004). ``confirm_delivery`` is probed by name, as ``SentrySink`` probes
        ``flush``, so a stand-in channel without it publishes unconfirmed, as in 1.x; one that
        raises closes the channel it was called on before the exception goes on, so none leaks.

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
            channel = self._connection.channel()
            confirm = getattr(channel, "confirm_delivery", None) if self._confirm else None
            if callable(confirm):
                try:
                    confirm()
                except BaseException:
                    _safe_close(channel)
                    raise
            self._confirming = callable(confirm)
            self._channel = channel
        return self._channel

    def _reset(self) -> None:
        """Releases the transport: the channel always, the connection only when the sink opened it.

        The sink's one release (SPEC-107 FR-001), and the one place the ownership test is
        written. It runs after a failed publish or a failed probe, so that the next attempt
        reopens what was dropped, and at close, through :meth:`_release`. A borrowed connection
        is never closed here and its reference is never dropped, so only its channel is ever
        reopened.

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
