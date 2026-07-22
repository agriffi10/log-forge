"""SQSSink — ship batches to an SQS queue (arch §8, §9.1, guide Phase 10).

SQS is the headline production path: a durable buffer that decouples the app from ELK
availability — events accumulate safely in the queue during downstream spikes/outages instead
of being lost or back-pressuring the app. A separate consumer indexes them into ELK (out of
scope here). Like every sink this receives *already-built* event dicts and knows nothing about
spans.

``boto3`` is an **optional** dependency (the ``aws`` extra): it is imported lazily inside the
sink, never at module top, so ``import log_foundry.sinks.sqs`` — and the whole library — stays
dependency-free unless an ``SQSSink`` is actually instantiated without an injected client.

The worker (SPEC-004) batches by count and time, but SQS has hard per-request limits, so this
sink re-chunks every incoming batch on both dimensions: ≤ 10 messages **and** ≤ 256 KB per
``send_message_batch``. Partial failures (the response ``Failed`` list) are retried; a single
event too large to ever fit is dropped with a warning rather than crashing the batch.
"""

from __future__ import annotations

import json
import sys
from typing import Any

__all__ = ["SQSSink"]


class SQSSink:
    """A :class:`~log_foundry.sinks.base.Sink` that sends events to an SQS queue."""

    MAX_BATCH = 10  # SQS SendMessageBatch hard limit: entries per request
    MAX_BYTES = 256 * 1024  # SQS limit: 256 KB per request

    def __init__(self, queue_url: str, client: Any = None, *, max_retries: int = 3) -> None:
        if client is None:
            import boto3  # type: ignore[import-not-found]  # optional 'aws' extra

            client = boto3.client("sqs")
        self.queue_url = queue_url
        self.client = client
        self.max_retries = max_retries
        self.dropped_oversized = 0  # events too large to ever fit one message
        self.failed = 0  # entries still failing after the retry bound

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Re-chunk ``batch`` to SQS limits and send each chunk (FR-001, FR-002)."""
        for chunk in self._chunks(batch):
            self._send(chunk)

    def close(self) -> None:
        """No-op: the sink buffers nothing internally (FR-005)."""

    # -- internals ----------------------------------------------------------------------

    def _chunks(self, batch: list[dict[str, object]]) -> list[list[str]]:
        """Split ``batch`` into sends of ≤ ``MAX_BATCH`` bodies and ≤ ``MAX_BYTES`` each.

        Each event is serialized once with ``json.dumps``. An event whose serialized size
        exceeds ``MAX_BYTES`` on its own can never fit a message, so it is dropped with a
        counted warning (FR-004) instead of stalling the batch.
        """
        chunks: list[list[str]] = []
        current: list[str] = []
        current_bytes = 0
        for event in batch:
            body = json.dumps(event)
            size = len(body.encode("utf-8"))
            if size > self.MAX_BYTES:
                self.dropped_oversized += 1
                sys.stderr.write(
                    f"log-foundry: dropped an event of {size} bytes exceeding the "
                    f"{self.MAX_BYTES}-byte SQS message limit\n"
                )
                continue
            if current and (
                len(current) >= self.MAX_BATCH or current_bytes + size > self.MAX_BYTES
            ):
                chunks.append(current)
                current = []
                current_bytes = 0
            current.append(body)
            current_bytes += size
        if current:
            chunks.append(current)
        return chunks

    def _send(self, bodies: list[str]) -> None:
        """Send one valid chunk, retrying only the ``Failed`` entries with a bounded count.

        Successfully-sent entries are never re-sent; entries still failing past ``max_retries``
        are counted (``failed``) and logged, not silently dropped (FR-003).
        """
        entries = [{"Id": str(i), "MessageBody": body} for i, body in enumerate(bodies)]
        for attempt in range(self.max_retries + 1):
            response = self.client.send_message_batch(QueueUrl=self.queue_url, Entries=entries)
            failed = response.get("Failed", [])
            if not failed:
                return
            failed_ids = {entry["Id"] for entry in failed}
            entries = [entry for entry in entries if entry["Id"] in failed_ids]
            if attempt >= self.max_retries:
                self.failed += len(entries)
                sys.stderr.write(
                    f"log-foundry: {len(entries)} SQS message(s) still failing after "
                    f"{self.max_retries + 1} attempts; abandoned\n"
                )
                return
