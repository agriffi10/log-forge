"""AzureEventHubsSink — send events to an Azure Event Hub (arch §8, §9.1, SPEC-010).

A durable-buffer sink on ``azure-eventhub`` (the optional ``azure-eventhubs`` extra, imported
lazily). Events are packed into one or more ``EventDataBatch`` objects respecting the 1 MB per-batch
limit — the SDK signals a full batch by raising ``ValueError`` from ``batch.add`` — and each full
batch is sent. A single event too large for an even-empty batch is dropped with a counted warning;
send errors are retried within a bound. ``close()`` closes the producer.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import threading

from log_foundry import _diag
from log_foundry.sinks._retry import wait
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

__all__ = ["AzureEventHubsSink"]

_BACKOFF_BASE = 0.1


class AzureEventHubsSink:
    """A :class:`~log_foundry.sinks.base.Sink` that sends events to an Azure Event Hub.

    **Worst-case delay** (SPEC-027 FR-005): ``max_retries`` waits of ``0.1 * 2**n`` per EventDataBatch —
    0.7 s at the default 3. The waits are interruptible, so ``shutdown()`` cuts one short.
    """

    def __init__(
        self,
        *,
        producer: Any = None,
        connection_str: str | None = None,
        eventhub: str | None = None,
        max_retries: int = 3,
    ) -> None:
        if producer is None:
            if connection_str is None:
                raise ValueError(
                    "AzureEventHubsSink requires connection_str when no producer is injected"
                )
            from azure.eventhub import (  # type: ignore[import-not-found]  # 'azure-eventhubs'
                EventHubProducerClient,
            )

            producer = EventHubProducerClient.from_connection_string(
                connection_str, eventhub_name=eventhub
            )
        self.producer = producer
        # Floored as ``Worker._emit`` floors its own (SPEC-021): a negative value returned
        # from ``_send`` having attempted nothing, and reported success.
        self.max_retries = max(max_retries, 0)
        # Set by the worker when this sink is the configured one (SPEC-027 FR-002).
        self.stop_signal: threading.Event | None = None
        self.failed = 0
        self.dropped_oversized = 0

    def losses(self) -> SinkLosses:
        """Oversized drops and events in a batch abandoned past the retry bound (FR-002)."""
        return SinkLosses(dropped=self.dropped_oversized, failed=self.failed)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Pack events into ≤ 1 MB EventDataBatches and send each; drop oversized events (FR-009).

        Raises when every ``EventDataBatch`` failed to send and at least one was attempted
        (SPEC-026 FR-001). An event dropped for being too large is not a send failure — it can
        never fit, so a batch of nothing but oversized events has nothing to retry and does not
        raise; it is reported through ``losses().dropped`` instead.
        """
        if not batch:
            return
        event_data_cls = _event_data_cls()
        current = self.producer.create_batch()
        attempted = delivered = 0
        for event in batch:
            data = event_data_cls(json.dumps(event).encode("utf-8"))
            if _try_add(current, data):
                continue
            # current batch is full: send it and start a fresh one for this event. Guarded on
            # emptiness because ``_try_add`` also fails against a *fresh* batch when the event
            # is oversized, and the SDK short-circuits ``send_batch`` on an empty batch and
            # returns — a phantom success that counted as delivery and suppressed the raise for
            # everything else in the emit.
            if len(current) > 0:
                attempted += 1
                delivered += self._send(current)
            current = self.producer.create_batch()
            if not _try_add(current, data):
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
        """Close the producer (FR-009)."""
        self.producer.close()

    # -- internals ----------------------------------------------------------------------

    def _send(self, event_batch: Any) -> int:
        """Send one EventDataBatch, retrying failures; ``1`` if it landed (FR-009, FR-011).

        Callers only reach this with a non-empty batch — both call sites check — and count the
        result: the "did anything land" question in ``emit`` cannot be answered by a method that
        returns nothing. An empty one must never be sent: the SDK returns immediately without
        contacting the hub, which would score as a delivery nobody made.
        """
        for attempt in range(self.max_retries + 1):
            try:
                self.producer.send_batch(event_batch)
                return 1
            except Exception as err:  # isolation boundary: never crash the worker (FR-011)
                if attempt < self.max_retries:
                    wait(_BACKOFF_BASE * (2**attempt), self.stop_signal)
                    continue
                self.failed += len(event_batch)
                _diag.lost(
                    "event",
                    len(event_batch),
                    f"AzureEventHubsSink, one batch, {self.max_retries + 1} attempts, "
                    f"{type(err).__name__}",
                )
                return 0
        return 0  # unreachable: the loop returns on every path (mypy needs the exit)


def _try_add(event_batch: Any, data: Any) -> bool:
    """Add ``data`` to the batch; ``False`` when the batch signals it is full (``ValueError``)."""
    try:
        event_batch.add(data)
        return True
    except ValueError:
        return False


def _event_data_cls() -> Any:
    """Return ``azure.eventhub.EventData`` (indirection seam for tests)."""
    from azure.eventhub import EventData  # 'azure-eventhubs' extra (type-ignored in __init__)

    return EventData
