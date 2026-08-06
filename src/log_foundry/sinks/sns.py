"""SNSSink — publish events to an SNS topic (arch §8, SPEC-010).

Mirrors the SPEC-005 ``SQSSink`` partial-failure policy on SNS's ``publish_batch`` (≤ 10 entries per
request, ≤ 256 KB total). ``boto3`` is the optional ``aws`` extra, imported lazily. The response
``Failed`` list is retried within a bounded count; entries still failing are counted and logged.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import threading

from log_foundry import _diag
from log_foundry.sinks._chunk import chunk_items
from log_foundry.sinks._retry import wait
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

__all__ = ["SNSSink"]

_BACKOFF_BASE = 0.1  # seconds; delay before retry attempt n is _BACKOFF_BASE * 2**n


class SNSSink:
    """A :class:`~log_foundry.sinks.base.Sink` that publishes events to an SNS topic.

    **Worst-case delay** (SPEC-027 FR-005): ``max_retries`` waits of ``0.1 * 2**n`` per chunk —
    0.7 s at the default 3. The waits are interruptible, so ``shutdown()`` cuts one short.
    """

    MAX_BATCH = 10  # publish_batch hard limit: entries per request
    MAX_BYTES = 256 * 1024  # 256 KB per request

    def __init__(self, topic_arn: str, *, client: Any = None, max_retries: int = 3) -> None:
        if client is None:
            import boto3  # type: ignore[import-not-found]  # optional 'aws' extra

            client = boto3.client("sns")
        self.topic_arn = topic_arn
        self.client = client
        # Floored as ``Worker._emit`` floors its own (SPEC-021): a negative value returned
        # from ``_send`` having published nothing, and reported success.
        self.max_retries = max(max_retries, 0)
        # Set by the worker when this sink is the configured one (SPEC-027 FR-002).
        self.stop_signal: threading.Event | None = None
        self.failed = 0
        self.dropped_oversized = 0

    def losses(self) -> SinkLosses:
        """Oversized drops and entries still failing past the retry bound (FR-002)."""
        return SinkLosses(dropped=self.dropped_oversized, failed=self.failed)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Re-chunk to publish_batch limits and send each chunk, retrying failures (FR-010).

        Raises when every chunk failed and at least one was sent (SPEC-026 FR-001). Events
        dropped before sending — too large to ever fit — are not a send failure and do not
        make a batch of nothing but oversized events raise; they can never be retried into
        existence, and are reported through ``losses().dropped``.
        """
        bodies = self._bodies(batch)
        chunks = delivered = 0
        for chunk in chunk_items(
            bodies, max_count=self.MAX_BATCH, max_bytes=self.MAX_BYTES, size_of=len
        ):
            chunks += 1
            delivered += self._send(chunk)
        if chunks and not delivered:
            raise SinkDeliveryError(f"SNSSink published none of {chunks} chunk(s)")

    def close(self) -> None:
        """No-op: the sink buffers nothing internally (FR-001)."""

    # -- internals ----------------------------------------------------------------------

    def _bodies(self, batch: list[dict[str, object]]) -> list[str]:
        """Serialize each event, dropping any single message too large to ever fit (FR-011)."""
        bodies: list[str] = []
        for event in batch:
            body = json.dumps(event)
            if len(body.encode("utf-8")) > self.MAX_BYTES:
                self.dropped_oversized += 1
                _diag.lost("event", 1, f"SNSSink, exceeds the {self.MAX_BYTES}-byte message limit")
                continue
            bodies.append(body)
        return bodies

    def _send(self, bodies: list[str]) -> int:
        """Publish one chunk, retrying only the ``Failed`` entries (bounded) (FR-010).

        Returns how many entries SNS accepted, so ``emit`` can tell "nothing landed" from a
        partial success. A chunk whose entries all failed contributes ``0``.
        """
        sent = len(bodies)
        entries = [{"Id": str(i), "Message": body} for i, body in enumerate(bodies)]
        for attempt in range(self.max_retries + 1):
            response = self.client.publish_batch(
                TopicArn=self.topic_arn, PublishBatchRequestEntries=entries
            )
            failed = response.get("Failed", [])
            if not failed:
                return sent
            failed_ids = {entry["Id"] for entry in failed}
            entries = [entry for entry in entries if entry["Id"] in failed_ids]
            if attempt < self.max_retries:
                # Before the next attempt, never before abandoning (SPEC-027 FR-003). This loop
                # re-sends the entries the destination flagged, and the canonical reason it
                # flags them is throttling — which an immediate re-send makes worse.
                wait(_BACKOFF_BASE * (2**attempt), self.stop_signal)
            if attempt >= self.max_retries:
                self.failed += len(entries)
                _diag.lost(
                    "message",
                    len(entries),
                    f"SNSSink, still failing after {self.max_retries + 1} attempts; abandoned",
                )
                return sent - len(entries)
        return 0  # unreachable: the loop returns on every path (mypy needs the exit)
