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

from log_foundry.sinks._batch import adjudicate_positional
from log_foundry.sinks._chunk import chunk_items

__all__ = ["FirehoseSink"]


class FirehoseSink:
    """A :class:`~log_foundry.sinks.base.Sink` that writes events to a Firehose delivery stream.

    Three counters report what was not delivered: ``failed`` (the delivery stream told us these
    failed, and they still failed after ``max_retries``), ``dropped_oversized`` (too large for the
    per-record limit to ever accept), and ``dropped_unadjudicated`` (a ``put_record_batch`` response
    whose ``RequestResponses`` did not describe the chunk that was sent, so no record in it could be
    paired to an outcome). A non-zero ``dropped_unadjudicated`` means those records were abandoned
    without the stream ever confirming them — treat it as loss, and as a sign the client is not
    AWS-shaped.
    """

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
        self.dropped_unadjudicated = 0

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
            verdict = adjudicate_positional(records, results)
            if verdict.unadjudicated:
                self.dropped_unadjudicated += verdict.unadjudicated
                sys.stderr.write(
                    f"log-foundry: FirehoseSink could not adjudicate a put_record_batch response "
                    f"({len(records)} record(s) sent, {len(results)} result(s) returned); "
                    f"{verdict.unadjudicated} record(s) abandoned\n"
                )
                return
            records = verdict.retry
            if not records:
                return
            if attempt >= self.max_retries:
                self.failed += len(records)
                sys.stderr.write(
                    f"log-foundry: {len(records)} Firehose record(s) still failing after "
                    f"{self.max_retries + 1} attempts; abandoned\n"
                )
                return
