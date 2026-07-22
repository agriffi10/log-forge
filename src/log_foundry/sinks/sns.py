"""SNSSink — publish events to an SNS topic (arch §8, SPEC-010).

Mirrors the SPEC-005 ``SQSSink`` partial-failure policy on SNS's ``publish_batch`` (≤ 10 entries per
request, ≤ 256 KB total). ``boto3`` is the optional ``aws`` extra, imported lazily. The response
``Failed`` list is retried within a bounded count; entries still failing are counted and logged.
"""

from __future__ import annotations

import json
import sys
from typing import Any

from log_foundry.sinks._chunk import chunk_items

__all__ = ["SNSSink"]


class SNSSink:
    """A :class:`~log_foundry.sinks.base.Sink` that publishes events to an SNS topic."""

    MAX_BATCH = 10  # publish_batch hard limit: entries per request
    MAX_BYTES = 256 * 1024  # 256 KB per request

    def __init__(self, topic_arn: str, *, client: Any = None, max_retries: int = 3) -> None:
        if client is None:
            import boto3  # type: ignore[import-not-found]  # optional 'aws' extra

            client = boto3.client("sns")
        self.topic_arn = topic_arn
        self.client = client
        self.max_retries = max_retries
        self.failed = 0
        self.dropped_oversized = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Re-chunk to publish_batch limits and send each chunk, retrying failures (FR-010)."""
        bodies = self._bodies(batch)
        for chunk in chunk_items(
            bodies, max_count=self.MAX_BATCH, max_bytes=self.MAX_BYTES, size_of=len
        ):
            self._send(chunk)

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
                sys.stderr.write(
                    f"log-foundry: SNSSink dropped an event exceeding the "
                    f"{self.MAX_BYTES}-byte message limit\n"
                )
                continue
            bodies.append(body)
        return bodies

    def _send(self, bodies: list[str]) -> None:
        """Publish one chunk, retrying only the ``Failed`` entries (bounded) (FR-010)."""
        entries = [{"Id": str(i), "Message": body} for i, body in enumerate(bodies)]
        for attempt in range(self.max_retries + 1):
            response = self.client.publish_batch(
                TopicArn=self.topic_arn, PublishBatchRequestEntries=entries
            )
            failed = response.get("Failed", [])
            if not failed:
                return
            failed_ids = {entry["Id"] for entry in failed}
            entries = [entry for entry in entries if entry["Id"] in failed_ids]
            if attempt >= self.max_retries:
                self.failed += len(entries)
                sys.stderr.write(
                    f"log-foundry: {len(entries)} SNS message(s) still failing after "
                    f"{self.max_retries + 1} attempts; abandoned\n"
                )
                return
