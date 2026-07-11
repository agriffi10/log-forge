"""KinesisSink — put event records to a Kinesis Data Stream (arch §8, §9.1, SPEC-010).

Extends the durable-buffer path ``SQSSink`` established: a queue/stream absorbs spikes and outages
while a separate consumer (out of scope) drains it into ELK. ``boto3`` is the optional ``aws`` extra,
imported lazily inside the sink (never at module top) so importing this module needs no ``boto3``
unless a sink is built without an injected client. Each incoming batch is re-chunked to Kinesis's
``put_records`` limits (≤ 500 records **and** ≤ 5 MB); partial failures are retried within a bound.
"""

from __future__ import annotations

import json
import sys
from typing import Any

from log_forge.sinks._chunk import chunk_items

__all__ = ["KinesisSink"]


class KinesisSink:
    """A :class:`~log_forge.sinks.base.Sink` that writes events to a Kinesis Data Stream."""

    MAX_RECORDS = 500  # put_records hard limit: records per request
    MAX_REQUEST_BYTES = 5 * 1024 * 1024  # 5 MB per put_records request
    MAX_RECORD_BYTES = 1024 * 1024  # 1 MB per record (Data)

    def __init__(
        self,
        stream_name: str,
        *,
        client: Any = None,
        partition_key_field: str = "trace_id",
        max_retries: int = 3,
    ) -> None:
        if client is None:
            import boto3  # type: ignore[import-not-found]  # optional 'aws' extra

            client = boto3.client("kinesis")
        self.stream_name = stream_name
        self.client = client
        self.partition_key_field = partition_key_field
        self.max_retries = max_retries
        self.failed = 0
        self.dropped_oversized = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Re-chunk to put_records limits and send each chunk, retrying failures (FR-003)."""
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
        """Build put_records entries, dropping any single record too large to ever fit (FR-011)."""
        records: list[dict[str, Any]] = []
        for event in batch:
            data = json.dumps(event).encode("utf-8")
            if len(data) > self.MAX_RECORD_BYTES:
                self.dropped_oversized += 1
                sys.stderr.write(
                    f"log-forge: KinesisSink dropped an event of {len(data)} bytes exceeding "
                    f"the {self.MAX_RECORD_BYTES}-byte per-record limit\n"
                )
                continue
            key = str(event.get(self.partition_key_field) or "log-forge")[:256]
            records.append({"Data": data, "PartitionKey": key})
        return records

    def _send(self, records: list[dict[str, Any]]) -> None:
        """Send one chunk, retrying only the records the response flags as failed (FR-003)."""
        for attempt in range(self.max_retries + 1):
            response = self.client.put_records(StreamName=self.stream_name, Records=records)
            if not response.get("FailedRecordCount"):
                return
            results = response.get("Records", [])
            records = [
                record
                for record, result in zip(records, results)
                if result.get("ErrorCode")
            ]
            if not records:
                return
            if attempt >= self.max_retries:
                self.failed += len(records)
                sys.stderr.write(
                    f"log-forge: {len(records)} Kinesis record(s) still failing after "
                    f"{self.max_retries + 1} attempts; abandoned\n"
                )
                return
