"""AzureEventHubsSink — send events to an Azure Event Hub (arch §8, §9.1, SPEC-010).

A durable-buffer sink on ``azure-eventhub`` (the optional ``azure-eventhubs`` extra, imported
lazily). Events are packed into one or more ``EventDataBatch`` objects respecting the 1 MB per-batch
limit — the SDK signals a full batch by raising ``ValueError`` from ``batch.add`` — and each full
batch is sent. A single event too large for an even-empty batch is dropped with a counted warning;
send errors are retried within a bound. ``close()`` closes the producer.
"""

from __future__ import annotations

import json
import sys
import time
from typing import Any

__all__ = ["AzureEventHubsSink"]

_BACKOFF_BASE = 0.1


class AzureEventHubsSink:
    """A :class:`~log_foundry.sinks.base.Sink` that sends events to an Azure Event Hub."""

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
        self.max_retries = max_retries
        self.failed = 0
        self.dropped_oversized = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Pack events into ≤ 1 MB EventDataBatches and send each; drop oversized events (FR-009)."""
        if not batch:
            return
        event_data_cls = _event_data_cls()
        current = self.producer.create_batch()
        for event in batch:
            data = event_data_cls(json.dumps(event).encode("utf-8"))
            if _try_add(current, data):
                continue
            # current batch is full: send it and start a fresh one for this event.
            self._send(current)
            current = self.producer.create_batch()
            if not _try_add(current, data):
                self.dropped_oversized += 1
                sys.stderr.write(
                    "log-foundry: AzureEventHubsSink dropped an event too large for an "
                    "empty 1 MB batch\n"
                )
        self._send(current)

    def close(self) -> None:
        """Close the producer (FR-009)."""
        self.producer.close()

    # -- internals ----------------------------------------------------------------------

    def _send(self, event_batch: Any) -> None:
        """Send one EventDataBatch (skipping an empty one), retrying failures (FR-009, FR-011)."""
        if len(event_batch) == 0:
            return
        for attempt in range(self.max_retries + 1):
            try:
                self.producer.send_batch(event_batch)
                return
            except Exception as err:  # isolation boundary: never crash the worker (FR-011)
                if attempt < self.max_retries:
                    time.sleep(_BACKOFF_BASE * (2**attempt))
                    continue
                self.failed += len(event_batch)
                sys.stderr.write(
                    f"log-foundry: AzureEventHubsSink abandoned a batch of {len(event_batch)} "
                    f"event(s) after {self.max_retries + 1} attempts ({err!r})\n"
                )
                return


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
