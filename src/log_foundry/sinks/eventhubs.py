"""AzureEventHubsSink — send events to an Azure Event Hub (arch §8, §9.1, SPEC-010)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from log_foundry import _diag
from log_foundry.sinks import _engine
from log_foundry.sinks._args import refuse_with_injected
from log_foundry.sinks._classify import TRANSIENT_BUILTINS, driver_classes, is_retryable
from log_foundry.sinks._encode import encode_round
from log_foundry.sinks._engine import When, emit_via_deliver, run_context
from log_foundry.sinks._extras import import_extra
from log_foundry.sinks._guard import ClosedGuard, GuardMode
from log_foundry.sinks._refusal import final_refusal
from log_foundry.sinks._retry import floor_count
from log_foundry.sinks._rounds import Called, Lost, mark_sent, raised_line
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict

if TYPE_CHECKING:
    import threading

    from log_foundry.sinks._deferral import Deferral
    from log_foundry.sinks._engine import _Drops
    from log_foundry.sinks.base import SinkLosses

__all__ = ["AzureEventHubsSink"]


class AzureEventHubsSink:
    """A :class:`~log_foundry.sinks.base.Sink` that sends events to an Azure Event Hub.

    This is a durable-buffer sink on ``azure-eventhub``, the optional ``azure-eventhubs`` extra,
    imported lazily. Events are packed into one or more ``EventDataBatch`` objects respecting the
    1 MB per-batch limit, which the SDK signals by raising ``ValueError`` from ``add``. It delivers
    through SPEC-072's engine (SPEC-074 FR-004): each round packs the events still to send and
    sends every pack once, under the lock for that round only, so the worst-case delay (SPEC-027
    FR-005) is ``max_retries`` interruptible waits per run, 0.7 s at the defaults, taken with no
    lock held.


    It keeps **no** client buffer (SPEC-036 FR-002): the driver call returns only once the
    destination has the batch, so nothing is queued locally between emits.
    """

    def __init__(
        self,
        *,
        producer: Any = None,
        connection_str: str | None = None,
        eventhub: str | None = None,
        max_retries: int = 3,
    ) -> None:
        """Binds the sink to a producer, building one if none was injected.

        **The extra is required with an injected producer too** (SPEC-071 FR-002). Every batch is
        built from the extra's ``EventData`` class, so an injected producer without
        ``azure-eventhub`` installed used to construct and then raise ``ModuleNotFoundError`` out
        of every ``emit``. The class is resolved once here, after the argument checks and before
        anything is built, so that configuration is refused where it is written; ``emit`` still
        resolves it per batch through :func:`_event_data_cls`, the seam tests patch.

        Args:
          producer: An ``EventHubProducerClient``-shaped object, or ``None`` to build one.
          connection_str: The connection string, required when no producer is injected.
          eventhub: The hub name used when building a producer.
          max_retries: Retries per ``EventDataBatch``, floored at zero as ``Worker._emit`` floors
            its own (SPEC-021) — a negative value returned from ``_send`` having attempted
            nothing, and reported success. Anything but an ``int`` is refused with ``TypeError``
            (SPEC-066 FR-001), before any producer is built; a ``bool`` or ``int`` subclass is
            read as the ``int`` it holds.

        Returns:
          None.

        Raises:
          TypeError: If ``max_retries`` is not an ``int`` (SPEC-066).
          ValueError: If no producer is injected and no connection string was given, or if
            ``connection_str`` or ``eventhub`` is passed alongside ``producer=``, which is already
            built and would never read either (SPEC-071 FR-003).
          ImportError: If the ``azure-eventhubs`` extra is not installed, whether or not a producer
            was injected; its message names the extra (``MissingExtraError``).
        """
        self.max_retries = floor_count(max_retries, 0, "max_retries", "AzureEventHubsSink")
        refuse_with_injected(
            "AzureEventHubsSink",
            "producer",
            producer,
            {"connection_str": connection_str, "eventhub": eventhub},
        )
        if producer is None and connection_str is None:
            raise ValueError(
                "AzureEventHubsSink requires connection_str when no producer is injected"
            )
        _event_data_cls()
        if producer is None:
            client_cls = import_extra("azure.eventhub", "azure-eventhubs").EventHubProducerClient
            producer = client_cls.from_connection_string(connection_str, eventhub_name=eventhub)
        self.producer = producer
        self.log_foundry_stop_signal: threading.Event | None = None
        self.log_foundry_counters = SinkCounters()
        self.dropped_oversized = 0
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
        """Reports the engine's totals: oversized and unencodable drops, events never confirmed.

        ``dropped`` folds in the events the encode step could not encode (SPEC-069 FR-001), which
        ``dropped_oversized`` does not: that attribute still means oversize only.

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

        Args:
          batch: The events to send. An empty batch is a no-op.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When the run failed — every pack failed to send (SPEC-026 FR-001) —
            with the engine's message. An event too large for an empty batch is not a send
            failure, so a batch of nothing but oversized events returns, reported through
            :meth:`losses`; the drop is counted and announced only when ``emit`` returns — on the
            engine, only when the run settles. When the sink is closed, its 1.x error by identity,
            nothing counted.
        """
        emit_via_deliver(self, batch)

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        """Runs one round and reports each event's fate by index (SPEC-074 FR-004).

        Args:
          batch: The events.

        Returns:
          The round's verdict.

        Raises:
          None. A ``BaseException`` from the producer propagates.
        """
        return _engine.one_round(self._round, batch)

    def _round(self, batch: list[dict[str, object]]) -> Verdict:
        """Packs the round's events and sends every pack once, under the lock for this round.

        A closed sink's round is a refusal carrying 1.x's closed error. Every event is encoded
        once per run (:func:`~log_foundry.sinks._encode.encode_round`), an unencodable one
        ``DROPPED`` with SPEC-069's line; :meth:`_send_batch` packs and sends the rest.

        Args:
          batch: The events this round is offered.

        Returns:
          A verdict with one outcome per event of ``batch``.

        Raises:
          None. A ``BaseException`` from the producer propagates.
        """
        with self._guard.enter() as section:
            if not section:
                run_context().refuse(
                    final_refusal(
                        f"AzureEventHubsSink sent none of {len(batch)} event(s): the sink is closed"
                    )
                )
                return Verdict.uniform(Outcome.REJECTED, len(batch))
            context = run_context()
            fates = [Outcome.DROPPED] * len(batch)
            pairs = encode_round(self, batch, self._encode, sink=type(self).__name__)
            if pairs:
                self._send_batch(batch, pairs, fates, context.drops(self))
        return Verdict(outcomes=tuple(fates))

    def _encode(self, event: dict[str, object]) -> bytes:
        """Encodes one event as the UTF-8 JSON body of its ``EventData``.

        Args:
          event: The event to encode.

        Returns:
          The event's body.

        Raises:
          TypeError: If the event is not JSON-serializable, which ``encode_round`` turns into a
            drop of this one event. Neither source of events produces one: assembly coerces every
            value (``sanitize``), and a ``TransformSink`` checks and coerces what its function
            returns (SPEC-058 FR-001).
        """
        return json.dumps(event).encode("utf-8")

    def _send_batch(
        self,
        batch: list[dict[str, object]],
        pairs: list[tuple[int, bytes]],
        fates: list[Outcome],
        pending: Deferral | _Drops,
    ) -> None:
        """Packs the round's events and sends each pack once, with the lock already held.

        The driver requirement satisfied (SPEC-028 FR-002): Microsoft affirmatively states that
        the ``EventHubProducerClient`` is not thread-safe and recommends guarding it with a
        ``threading.Lock``; the ``EventDataBatch`` this builds up across the loop is single-owner
        state besides — two threads packing into batches from one producer interleave
        ``create_batch``/``add``/``send`` on it.

        An event too large for an empty batch is ``DROPPED`` and goes to ``pending`` rather than
        to the counter, so it is counted and announced only when ``emit`` returns (SPEC-059
        FR-001) — on the engine, ``pending`` is the run context's ``SETTLED`` view, so only when
        the run settles (SPEC-074) — and its line is written after the lock is released. A pack's
        failure makes its events ``RETRYABLE`` or ``REJECTED`` as ``_retryable_error`` answers,
        and the round goes on to the next pack.

        Args:
          batch: The round's events.
          pairs: Each encodable event's position with its body, known non-empty.
          fates: The round's outcomes, updated in place.
          pending: The run's ``SETTLED`` view, which holds every drop decided here until the run
            is known to settle.

        Returns:
          None.

        Raises:
          None. A ``BaseException`` from the producer propagates.
        """
        event_data_cls = _event_data_cls()
        lost = Lost(self)
        current = self.producer.create_batch()
        packed: list[int] = []
        for position, body in pairs:
            data = event_data_cls(body)
            if _try_add(current, data):
                packed.append(position)
                continue
            if len(current) > 0:
                self._settle(batch, packed, self._send(current), fates, lost)
            current = self.producer.create_batch()
            packed = []
            if _try_add(current, data):
                packed.append(position)
                continue
            pending.count("dropped_oversized")
            pending.announce(
                lambda: _diag.lost(
                    "event", 1, "AzureEventHubsSink, too large for an empty 1 MB batch"
                )
            )
        if len(current) > 0:
            self._settle(batch, packed, self._send(current), fates, lost)
        lost.register()

    def _settle(
        self,
        batch: list[dict[str, object]],
        packed: list[int],
        called: Called[None],
        fates: list[Outcome],
        lost: Lost,
    ) -> None:
        """Sets one pack's outcomes from its send.

        Args:
          batch: The round's events.
          packed: The pack's positions in the round's batch.
          called: The send.
          fates: The round's outcomes, updated in place.
          lost: The round's lost events.

        Returns:
          None.

        Raises:
          None.
        """
        carried = [batch[position] for position in packed]
        mark_sent(self, carried)
        if called.error is None:
            outcome = Outcome.ACCEPTED
        else:
            outcome = Outcome.RETRYABLE if called.retryable else Outcome.REJECTED
            kind = type(called.error).__name__
            lost.add(
                carried,
                when=When.LAST if called.retryable else When.ALWAYS,
                key=kind,
                line=raised_line("event", "AzureEventHubsSink, one batch", kind),
            )
        for position in packed:
            fates[position] = outcome

    def close(self) -> None:
        """Closes the producer (FR-009).

        Idempotent through the guard (SPEC-076 FR-002): once a close begins every later round is
        refused at once, the one round inside, if any, is waited for and never released under,
        and the release runs once; a second close waits for the first's release, then returns.
        The producer is never closed out from under an in-flight send (SPEC-028 FR-002).

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the producer raises on close.
        """
        self._guard.close(self._release)

    def _release(self) -> None:
        """Closes the producer.

        :meth:`close` hands this to the guard, which runs it once, with the lock held and
        the state already ``CLOSED``.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the producer raises on close.
        """
        self.producer.close()

    def _retryable_error(self, err: Exception) -> bool:
        """Reports whether a failed send is worth another attempt (SPEC-070 FR-002).

        ``ConnectionLostError`` ("in most cases the client will automatically retry"),
        ``ConnectError`` and ``OperationTimeoutError`` (``azure/eventhub/exceptions.py``) are
        retried; ``AuthenticationError``, a ``ConnectError`` subclass, is not, because a resend
        cannot fix a credential. ``EventDataError``, ``EventDataSendError`` (an AMQP rejection of
        the message), ``ClientClosedError`` and a bare ``EventHubError`` fail at once.

        Args:
          err: The exception the attempt raised.

        Returns:
          True when another attempt may succeed.

        Raises:
          None. :func:`~log_foundry.sinks._classify.is_retryable` reads a raise here as False.
        """
        if isinstance(err, driver_classes("azure.eventhub.exceptions", "AuthenticationError")):
            return False
        retried = (
            *TRANSIENT_BUILTINS,
            *driver_classes(
                "azure.eventhub.exceptions",
                "ConnectionLostError",
                "ConnectError",
                "OperationTimeoutError",
            ),
        )
        return isinstance(err, retried)

    def _send(self, event_batch: Any) -> Called[None]:
        """Sends one ``EventDataBatch`` once (SPEC-074 FR-004); the engine resends it later.

        Callers only reach this with a non-empty batch — both call sites check — because an empty
        one must never be sent: the SDK returns immediately without contacting the hub, which
        would score as a delivery nobody made.

        Args:
          event_batch: The packed batch to send.

        Returns:
          The call, with its exception when it raised.

        Raises:
          None. This is an isolation boundary: a driver fault must never crash the worker.
        """
        try:
            self.producer.send_batch(event_batch)
        except Exception as err:
            return Called(None, error=err, retryable=is_retryable(self._retryable_error, err))
        return Called(None)


def _try_add(event_batch: Any, data: Any) -> bool:
    """Adds one event to a batch.

    Args:
      event_batch: The batch being packed.
      data: The ``EventData`` to add.

    Returns:
      True when it fit, False when the batch signals it is full.

    Raises:
      None.
    """
    try:
        event_batch.add(data)
        return True
    except ValueError:
        return False


def _event_data_cls() -> Any:
    """Returns the ``EventData`` class.

    This is an indirection seam so tests can substitute a stand-in.

    Args:
      None.

    Returns:
      The class.

    Raises:
      ImportError: If the ``azure-eventhubs`` extra is not installed.
    """
    return import_extra("azure.eventhub", "azure-eventhubs").EventData
