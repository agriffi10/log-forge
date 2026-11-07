"""AzureEventHubsSink — send events to an Azure Event Hub (arch §8, §9.1, SPEC-010)."""

from __future__ import annotations

import json
import threading
from typing import Any

from log_foundry import _diag
from log_foundry.sinks._classify import TRANSIENT_BUILTINS, driver_classes, is_retryable
from log_foundry.sinks._deferral import Deferral
from log_foundry.sinks._encode import encode_events
from log_foundry.sinks._extras import import_extra
from log_foundry.sinks._retry import floor_count, wait
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

__all__ = ["AzureEventHubsSink"]

_BACKOFF_BASE = 0.1


class AzureEventHubsSink:
    """A :class:`~log_foundry.sinks.base.Sink` that sends events to an Azure Event Hub.

    This is a durable-buffer sink on ``azure-eventhub``, the optional ``azure-eventhubs`` extra,
    imported lazily. Events are packed into one or more ``EventDataBatch`` objects respecting the
    1 MB per-batch limit, which the SDK signals by raising ``ValueError`` from ``add``. The
    worst-case delay (SPEC-027 FR-005) is ``max_retries`` interruptible waits per batch, 0.7 s at
    the defaults.


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
          ValueError: If no producer is injected and no connection string was given.
          ImportError: If the ``azure-eventhubs`` extra is not installed, whether or not a producer
            was injected; its message names the extra (``MissingExtraError``).
        """
        self.max_retries = floor_count(max_retries, 0, "max_retries", "AzureEventHubsSink")
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
        self.failed = 0
        self.dropped_oversized = 0
        self._unencodable = 0
        self._counter_lock = threading.Lock()
        self._lock = threading.Lock()
        self._closed = False

    def losses(self) -> SinkLosses:
        """Reports oversized and unencodable drops, and events abandoned past the bound (FR-002).

        ``dropped`` folds in the events :meth:`emit` could not encode (SPEC-069 FR-001), which
        ``dropped_oversized`` does not: that attribute still means oversize only.

        Args:
          None.

        Returns:
          The counters.

        Raises:
          None.
        """
        with self._counter_lock:
            return SinkLosses(
                dropped=self.dropped_oversized + self._unencodable, failed=self.failed
            )

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Packs events into batches within the size limit and sends each (FR-009).

        A full batch is sent and a fresh one started, guarded on emptiness because the add also
        fails against a fresh batch when the event is oversized — and the SDK short-circuits an
        empty send and returns, a phantom success that counted as delivery and suppressed the
        raise for everything else in the emit.

        An oversize drop :meth:`_send_batch` decides is counted and announced only when
        ``emit`` returns, through the deferral helper (SPEC-059 FR-001): a send failure on every
        attempted batch, or ``create_batch`` raising, discards it, and the worker's retry
        decides it afresh. The ledger is entered **outside** the emit lock — the ``Deferral``
        first, ``_lock`` inside it — so the lock is released before the commit takes the counter
        lock and writes the oversize line, which therefore never runs under the transport lock
        (SPEC-028's two-lock decision). The failed-send count and line :meth:`_send` writes stay
        where they are, under the lock (SPEC-059 *Out of Scope*).

        Every event is encoded inside the ledger but **before** the emit lock is taken, and
        before the first ``create_batch`` (SPEC-069 FR-001). One that cannot be encoded is
        dropped, counted and announced only when this call returns; before, an
        ``EventDataBatch`` that filled ahead of it was already sent when its encode raised, and
        the worker's retry sent it again. A batch of nothing encodable touches the producer not
        at all and returns, once the closed check has passed (FR-002).

        Args:
          batch: The events to send. An empty batch is a no-op.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When every batch failed to send and at least one was attempted
            (SPEC-026 FR-001). An event dropped for being too large is not a send failure — it
            can never fit — so a batch of nothing but oversized events has nothing to retry and
            is reported through :meth:`losses` instead; a raise leaves none of them counted.
        """
        if not batch:
            return
        with Deferral(self) as pending:
            encoded = encode_events(batch, self._encode, pending, sink=type(self).__name__)
            with self._lock:
                if self._closed:
                    raise SinkDeliveryError(
                        f"AzureEventHubsSink sent none of {len(batch)} event(s): the sink is closed"
                    )
                if not encoded:
                    return
                self._send_batch([body for _, body in encoded], pending)

    def _encode(self, event: dict[str, object]) -> bytes:
        """Encodes one event as the UTF-8 JSON body of its ``EventData``.

        Args:
          event: The event to encode.

        Returns:
          The event's body.

        Raises:
          TypeError: If the event is not JSON-serializable, which ``encode_events`` turns into a
            drop of this one event. Neither source of events produces one: assembly coerces every
            value (``sanitize``), and a ``TransformSink`` checks and coerces what its function
            returns (SPEC-058 FR-001).
        """
        return json.dumps(event).encode("utf-8")

    def _send_batch(self, bodies: list[bytes], pending: Deferral) -> None:
        """Packs and sends the batch, with the emit lock already held.

        Split out so the lock's extent is one line in :meth:`emit`. The driver requirement
        satisfied (SPEC-028 FR-002): Microsoft affirmatively states that the
        ``EventHubProducerClient`` is not thread-safe and recommends guarding it with a
        ``threading.Lock``; the ``EventDataBatch`` this builds up across the loop is single-owner
        state besides — two threads packing into batches from one producer interleave
        ``create_batch``/``add``/``send`` on it.

        An event too large for an empty batch goes to ``pending`` rather than to the counter, so
        it is counted and announced only when ``emit`` returns (SPEC-059 FR-001), and its line is
        written after the emit lock is released rather than under it. The line names no
        per-event value, so each drop registers its own count and its own line, one per event as
        before.

        Args:
          bodies: The encoded events to send, known non-empty.
          pending: The calling ``emit``'s deferral, which holds every drop decided here until
            that ``emit`` is known not to raise.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When every attempted send failed.
        """
        event_data_cls = _event_data_cls()
        current = self.producer.create_batch()
        attempted = delivered = 0
        for body in bodies:
            data = event_data_cls(body)
            if _try_add(current, data):
                continue
            if len(current) > 0:
                attempted += 1
                delivered += self._send(current)
            current = self.producer.create_batch()
            if not _try_add(current, data):
                pending.count("dropped_oversized")
                pending.announce(
                    lambda: _diag.lost(
                        "event", 1, "AzureEventHubsSink, too large for an empty 1 MB batch"
                    )
                )
        if len(current) > 0:
            attempted += 1
            delivered += self._send(current)
        if attempted and not delivered:
            raise SinkDeliveryError(
                f"AzureEventHubsSink sent none of {attempted} EventDataBatch(es)"
            )

    def close(self) -> None:
        """Closes the producer (FR-009).

        Idempotent, and takes the emit lock so the producer is never closed out from under an
        in-flight send (SPEC-028 FR-002).

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the producer raises on close.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
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

    def _send(self, event_batch: Any) -> int:
        """Sends one ``EventDataBatch``, retrying failures (FR-009, FR-011).

        Callers only reach this with a non-empty batch — both call sites check — because an empty
        one must never be sent: the SDK returns immediately without contacting the hub, which
        would score as a delivery nobody made.

        Args:
          event_batch: The packed batch to send.

        Returns:
          1 when it landed, 0 once it is abandoned. The "did anything land" question in
          :meth:`emit` cannot be answered by a method that returns nothing.

        Raises:
          None. This is an isolation boundary: a driver fault must never crash the worker.
        """
        for attempt in range(self.max_retries + 1):
            try:
                self.producer.send_batch(event_batch)
                return 1
            except Exception as err:
                if attempt < self.max_retries and is_retryable(self._retryable_error, err):
                    wait(_BACKOFF_BASE * (2**attempt), self.log_foundry_stop_signal)
                    continue
                with self._counter_lock:
                    self.failed += len(event_batch)
                _diag.lost(
                    "event",
                    len(event_batch),
                    f"AzureEventHubsSink, one batch, {attempt + 1} attempt(s), "
                    f"{type(err).__name__}",
                )
                return 0
        return 0


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
