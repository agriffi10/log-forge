"""FirehoseSink — put event records to a Kinesis Data Firehose delivery stream (arch §8, SPEC-010).

The same durable-buffer shape as :class:`~log_foundry.sinks.kinesis.KinesisSink`, using Firehose's
``put_record_batch`` (≤ 500 records **and** ≤ 4 MB per request). ``boto3`` is the optional ``aws``
extra, imported lazily. Partial failures (the response ``RequestResponses``/``FailedPutCount``) are
retried within a bounded count.
"""

from __future__ import annotations

import json
from typing import Any

from log_foundry import _diag
from log_foundry.sinks._batch import adjudicate_positional, usable_results
from log_foundry.sinks._chunk import chunk_items
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

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
        # Floored as ``Worker._emit`` floors its own (SPEC-021): a negative value returned
        # from ``_send`` having sent nothing, and reported success.
        self.max_retries = max(max_retries, 0)
        self.failed = 0
        self.dropped_oversized = 0
        self.dropped_unadjudicated = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Re-chunk to put_record_batch limits and send each chunk, retrying failures (FR-004).

        Raises when every chunk failed and at least one was sent (SPEC-026 FR-001). Records
        dropped before sending — too large to ever fit — are not a send failure and do not make
        a batch of nothing but oversized records raise; they can never be retried into
        existence, and are reported through ``losses().dropped``.

        An unadjudicable chunk is *unknown*, not *nothing*, and suppresses the raise. SPEC-018
        settled that such a chunk is abandoned rather than re-sent — the API reported a failure
        count, so some of it landed, and the worker's retry would duplicate that downstream
        forever. Raising on it would be exactly the re-send SPEC-018 refuses.

        That suppression is batch-wide, unlike ``SQSSink``'s. A sender fault is a rejection —
        the entries provably did not land, so re-sending them is futile rather than harmful —
        while "unadjudicable" means this sink *cannot tell* whether they landed. Once any chunk
        is in that state the emit can no longer prove nothing was delivered, so a raise would
        risk duplicating it, and the events in a plainly-failed sibling chunk stay a counted
        loss rather than a duplicated delivery. SPEC-018 chose that trade once already.
        """
        records = self._records(batch)
        chunks = delivered = 0
        unknown = False
        for chunk in chunk_items(
            records,
            max_count=self.MAX_RECORDS,
            max_bytes=self.MAX_REQUEST_BYTES,
            size_of=lambda record: len(record["Data"]),
        ):
            chunks += 1
            outcome = self._send(chunk)
            if outcome is None:
                unknown = True
            else:
                delivered += outcome
        if chunks and not delivered and not unknown:
            raise SinkDeliveryError(f"FirehoseSink delivered none of {chunks} chunk(s)")

    def losses(self) -> SinkLosses:
        """Oversized drops, abandoned records and unadjudicable chunks (FR-002). Never raises.

        ``failed`` sums ``failed`` and ``dropped_unadjudicated``: both are records the stream
        never confirmed. They stay apart on the instance, because "the stream said these failed"
        and "the response did not describe them" have different remedies (SPEC-018).
        """
        return SinkLosses(
            dropped=self.dropped_oversized,
            failed=self.failed + self.dropped_unadjudicated,
        )

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
                _diag.lost(
                    "event",
                    1,
                    f"FirehoseSink, {len(data)} bytes exceeds the "
                    f"{self.MAX_RECORD_BYTES}-byte per-record limit",
                )
                continue
            records.append({"Data": data})
        return records


    def _send(self, records: list[dict[str, Any]]) -> int | None:
        """Send one chunk, retrying only the entries the response flags as failed (FR-004).

        Returns how many records the stream accepted, so ``emit`` can tell "nothing landed"
        from a partial success — or ``None`` when the response could not be adjudicated, which
        is neither. "The stream did not say" must not be read as "the stream took nothing":
        that reading would make ``emit`` raise, and the worker's retry would re-send a chunk
        SPEC-018 settled must never be re-sent.
        """
        sent = len(records)
        for attempt in range(self.max_retries + 1):
            response = self.client.put_record_batch(
                DeliveryStreamName=self.delivery_stream, Records=records
            )
            if not response.get("FailedPutCount"):
                return sent
            results = usable_results(response.get("RequestResponses"))
            verdict = adjudicate_positional(records, results)
            if verdict.unadjudicated:
                self.dropped_unadjudicated += verdict.unadjudicated
                _diag.lost(
                    "record",
                    verdict.unadjudicated,
                    f"FirehoseSink could not adjudicate a put_record_batch response "
                    f"({len(records)} record(s) sent, {len(results)} result(s) returned); "
                    f"abandoned, not retried",
                )
                return None
            records = verdict.retry
            if not records:
                return sent
            if attempt >= self.max_retries:
                self.failed += len(records)
                _diag.lost(
                    "record",
                    len(records),
                    f"FirehoseSink, still failing after {self.max_retries + 1} attempts; abandoned",
                )
                return sent - len(records)
        return 0  # unreachable: the loop returns on every path (mypy needs the exit)
