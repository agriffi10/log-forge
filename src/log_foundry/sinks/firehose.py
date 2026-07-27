"""FirehoseSink — put event records to a Kinesis Data Firehose delivery stream (arch §8, SPEC-010).

The same durable-buffer shape as :class:`~log_foundry.sinks.kinesis.KinesisSink`, using Firehose's
``put_record_batch`` (≤ 500 records **and** ≤ 4 MB per request). ``boto3`` is the optional ``aws``
extra, imported lazily. Partial failures (the response ``RequestResponses``/``FailedPutCount``) are
retried within a bounded count.
"""

from __future__ import annotations

import json
import sys
from typing import Any

from log_foundry.sinks._chunk import chunk_items

__all__ = ["FirehoseSink"]


class FirehoseSink:
    """A :class:`~log_foundry.sinks.base.Sink` that writes events to a Firehose delivery stream."""

    MAX_RECORDS = 500  # put_record_batch hard limit: records per request
    MAX_REQUEST_BYTES = 4 * 1024 * 1024  # 4 MB per put_record_batch request
    MAX_RECORD_BYTES = 1024 * 1024  # 1 MB per record

    def __init__(self, delivery_stream: str, *, client: Any = None, max_retries: int = 3) -> None:
        if client is None:
            import boto3  # type: ignore[import-not-found]  # optional 'aws' extra

            client = boto3.client("firehose")
        self.delivery_stream = delivery_stream
        self.client = client
        self.max_retries = max_retries
        self.failed = 0
        self.dropped_oversized = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Re-chunk to put_record_batch limits and send each chunk, retrying failures (FR-004)."""
        records = self._records(batch)
        for chunk in chunk_items(
            records,
            max_count=self.MAX_RECORDS,
            max_bytes=self.MAX_REQUEST_BYTES,
            size_of=lambda record: len(record["Data"]),
        ):
            self._send(chunk)

    def close(self) -> None:
        """No-op: the sink buffers nothing internally (FR-001)."""

    # -- internals ----------------------------------------------------------------------

    def _records(self, batch: list[dict[str, object]]) -> list[dict[str, Any]]:
        """Build put_record_batch entries, dropping any single record too large (FR-011)."""
        records: list[dict[str, Any]] = []
        for event in batch:
            data = json.dumps(event).encode("utf-8")
            if len(data) > self.MAX_RECORD_BYTES:
                self.dropped_oversized += 1
                sys.stderr.write(
                    f"log-foundry: FirehoseSink dropped an event of {len(data)} bytes exceeding "
                    f"the {self.MAX_RECORD_BYTES}-byte per-record limit\n"
                )
                continue
            records.append({"Data": data})
        return records

    def _send(self, records: list[dict[str, Any]]) -> None:
        """Send one chunk, retrying only the entries the response flags as failed (FR-004)."""
        for attempt in range(self.max_retries + 1):
            response = self.client.put_record_batch(
                DeliveryStreamName=self.delivery_stream, Records=records
            )
            if not response.get("FailedPutCount"):
                return
            results = response.get("RequestResponses", [])
            records = [
                record
                # strict=False states today's behaviour. A short `results` would silently
                # truncate this retry list — see the note in kinesis.py.
                for record, result in zip(records, results, strict=False)
                if result.get("ErrorCode")
            ]
            if not records:
                return
            if attempt >= self.max_retries:
                self.failed += len(records)
                sys.stderr.write(
                    f"log-foundry: {len(records)} Firehose record(s) still failing after "
                    f"{self.max_retries + 1} attempts; abandoned\n"
                )
                return
