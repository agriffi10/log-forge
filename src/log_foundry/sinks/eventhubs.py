"""AzureEventHubsSink — send events to an Azure Event Hub (arch §8, §9.1, SPEC-010)."""

from __future__ import annotations

import json
import threading
from typing import Any

from log_foundry import _diag
from log_foundry.sinks._retry import wait
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

        Args:
          producer: An ``EventHubProducerClient``-shaped object, or ``None`` to build one.
          connection_str: The connection string, required when no producer is injected.
          eventhub: The hub name used when building a producer.
          max_retries: Retries per ``EventDataBatch``, floored at zero as ``Worker._emit`` floors
            its own (SPEC-021) — a negative value returned from ``_send`` having attempted
            nothing, and reported success.

        Returns:
          None.

        Raises:
          ValueError: If no producer is injected and no connection string was given.
          ImportError: If the ``azure-eventhubs`` extra is not installed.
        """
        if producer is None:
            if connection_str is None:
                raise ValueError(
                    "AzureEventHubsSink requires connection_str when no producer is injected"
                )
            from azure.eventhub import (  # type: ignore[import-not-found]
                EventHubProducerClient,
            )

            producer = EventHubProducerClient.from_connection_string(
                connection_str, eventhub_name=eventhub
            )
        self.producer = producer
        self.max_retries = max(max_retries, 0)
        self.log_foundry_stop_signal: threading.Event | None = None
        self.failed = 0
        self.dropped_oversized = 0
        self._counter_lock = threading.Lock()
        self._lock = threading.Lock()
        self._closed = False

    def losses(self) -> SinkLosses:
        """Reports oversized drops and events in a batch abandoned past the bound (FR-002).

        Args:
          None.

        Returns:
          The counters.

        Raises:
          None.
        """
        with self._counter_lock:
            return SinkLosses(dropped=self.dropped_oversized, failed=self.failed)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Packs events into batches within the size limit and sends each (FR-009).

        A full batch is sent and a fresh one started, guarded on emptiness because the add also
        fails against a fresh batch when the event is oversized — and the SDK short-circuits an
        empty send and returns, a phantom success that counted as delivery and suppressed the
        raise for everything else in the emit.

        Args:
          batch: The events to send. An empty batch is a no-op.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When every batch failed to send and at least one was attempted
            (SPEC-026 FR-001). An event dropped for being too large is not a send failure — it
            can never fit — so a batch of nothing but oversized events has nothing to retry and
            is reported through :meth:`losses` instead.
        """
        if not batch:
            return
        with self._lock:
            if self._closed:
                raise SinkDeliveryError(
                    f"AzureEventHubsSink sent none of {len(batch)} event(s): the sink is closed"
                )
            self._send_batch(batch)

    def _send_batch(self, batch: list[dict[str, object]]) -> None:
        """Packs and sends the batch, with the emit lock already held.

        Split out so the lock's extent is one line in :meth:`emit`. The driver requirement
        satisfied (SPEC-028 FR-002): Microsoft affirmatively states that the
        ``EventHubProducerClient`` is not thread-safe and recommends guarding it with a
        ``threading.Lock``; the ``EventDataBatch`` this builds up across the loop is single-owner
        state besides — two threads packing into batches from one producer interleave
        ``create_batch``/``add``/``send`` on it.

        Args:
          batch: The events to send, known non-empty.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When every attempted send failed.
        """
        event_data_cls = _event_data_cls()
        current = self.producer.create_batch()
        attempted = delivered = 0
        for event in batch:
            data = event_data_cls(json.dumps(event).encode("utf-8"))
            if _try_add(current, data):
                continue
            if len(current) > 0:
                attempted += 1
                delivered += self._send(current)
            current = self.producer.create_batch()
            if not _try_add(current, data):
                with self._counter_lock:
                    self.dropped_oversized += 1
                _diag.lost("event", 1, "AzureEventHubsSink, too large for an empty 1 MB batch")
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
                if attempt < self.max_retries:
                    wait(_BACKOFF_BASE * (2**attempt), self.log_foundry_stop_signal)
                    continue
                with self._counter_lock:
                    self.failed += len(event_batch)
                _diag.lost(
                    "event",
                    len(event_batch),
                    f"AzureEventHubsSink, one batch, {self.max_retries + 1} attempts, "
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
    from azure.eventhub import EventData

    return EventData
