"""KinesisSink — put event records to a Kinesis Data Stream (arch §8, §9.1, SPEC-010)."""

from __future__ import annotations

import json
import threading
from typing import Any

from log_foundry import _diag
from log_foundry.sinks._batch import adjudicate_positional, usable_results
from log_foundry.sinks._chunk import chunk_items
from log_foundry.sinks._retry import wait
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

__all__ = ["KinesisSink"]

_BACKOFF_BASE = 0.1


class KinesisSink:
    """A :class:`~log_foundry.sinks.base.Sink` that writes events to a Kinesis Data Stream.

    This extends the durable-buffer path ``SQSSink`` established: a stream absorbs spikes and
    outages while a separate consumer drains it into ELK. ``boto3`` is the optional ``aws``
    extra, imported lazily inside the sink. The worst-case delay (SPEC-027 FR-005) is
    ``max_retries`` interruptible waits per chunk, 0.7 s at the defaults.

    Attributes:
      failed: Records the stream said failed, which still failed after every retry.
      dropped_oversized: Records too large for the per-record limit to ever accept.
      dropped_unadjudicated: Records in a ``put_records`` response whose results array did not
        describe the chunk sent, so none could be paired to an outcome. A non-zero count means
        those records were abandoned without the stream ever confirming them — treat it as loss,
        and as a sign the client is not AWS-shaped.

    The driver requirement satisfied (SPEC-028 FR-002): this sink takes **no** transport
    lock. ``boto3`` clients are documented thread-safe, this one is built once in
    ``__init__``, and the sink rebinds nothing after construction.

    It also **accepts emit after close** (SPEC-032 FR-003): ``close()`` is a documented no-op,
    because the client is the caller's to release or the SDK's to reap, so a batch emitted
    afterwards still reaches the stream.
    """

    MAX_RECORDS = 500
    MAX_REQUEST_BYTES = 5 * 1024 * 1024
    MAX_RECORD_BYTES = 1024 * 1024

    def __init__(
        self,
        stream_name: str,
        *,
        client: Any = None,
        partition_key_field: str = "trace_id",
        max_retries: int = 3,
    ) -> None:
        """Binds the sink to a stream.

        Args:
          stream_name: The stream to write to.
          client: A boto3-shaped Kinesis client, or ``None`` to build one lazily.
          partition_key_field: The event key used as the record's partition key.
          max_retries: Retries for the failed records of a chunk, floored at zero as
            ``Worker._emit`` floors its own (SPEC-021) — a negative value returned from ``_send``
            having sent nothing, and reported success.

        Returns:
          None.

        Raises:
          ImportError: If ``boto3`` is needed and the ``aws`` extra is not installed.
        """
        if client is None:
            import boto3  # type: ignore[import-not-found]

            client = boto3.client("kinesis")
        self.stream_name = stream_name
        self.client = client
        self.partition_key_field = partition_key_field
        self.max_retries = max(max_retries, 0)
        self.stop_signal: threading.Event | None = None
        self.failed = 0
        self.dropped_oversized = 0
        self.dropped_unadjudicated = 0
        self._counter_lock = threading.Lock()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Re-chunks to the request limits and sends each chunk, retrying failures (FR-003).

        An unadjudicable chunk is unknown, not nothing, and suppresses the raise: SPEC-018
        settled that such a chunk is abandoned rather than re-sent, because the API reported a
        failure count, so some of it landed and the worker's retry would duplicate that
        downstream forever.

        That suppression is batch-wide, unlike ``SQSSink``'s. A sender fault is a rejection, so
        re-sending is futile rather than harmful, while "unadjudicable" means this sink cannot
        tell whether the records landed — once any chunk is in that state the emit can no longer
        prove nothing was delivered, so the events in a plainly-failed sibling chunk stay a
        counted loss rather than a duplicated delivery.

        Args:
          batch: The events to write.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When every chunk failed, at least one was sent, and none was
            unadjudicable (SPEC-026 FR-001). Records dropped before sending are not a send
            failure and are reported through :meth:`losses`.
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
            raise SinkDeliveryError(f"KinesisSink delivered none of {chunks} chunk(s)")

    def losses(self) -> SinkLosses:
        """Reports oversized drops, abandoned records and unadjudicable chunks (FR-002).

        Args:
          None.

        Returns:
          The counters. ``failed`` sums the abandoned and the unadjudicated, since both are
          records the stream never confirmed; they stay apart on the instance because "the
          stream said these failed" and "the response did not describe them" have different
          remedies (SPEC-018).

        Raises:
          None.
        """
        with self._counter_lock:
            return SinkLosses(
                dropped=self.dropped_oversized,
                failed=self.failed + self.dropped_unadjudicated,
            )

    def close(self) -> None:
        """Does nothing, since the sink buffers nothing internally (FR-001).

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """

    def _records(self, batch: list[dict[str, object]]) -> list[dict[str, Any]]:
        """Builds the request entries, dropping any record too large to ever fit (FR-011).

        Args:
          batch: The events to convert.

        Returns:
          The records that can be sent.

        Raises:
          None.
        """
        records: list[dict[str, Any]] = []
        for event in batch:
            data = json.dumps(event).encode("utf-8")
            if len(data) > self.MAX_RECORD_BYTES:
                with self._counter_lock:
                    self.dropped_oversized += 1
                _diag.lost(
                    "event",
                    1,
                    f"KinesisSink, {len(data)} bytes exceeds the "
                    f"{self.MAX_RECORD_BYTES}-byte per-record limit",
                )
                continue
            key = str(event.get(self.partition_key_field) or "log-foundry")[:256]
            records.append({"Data": data, "PartitionKey": key})
        return records

    def _send(self, records: list[dict[str, Any]]) -> int | None:
        """Sends one chunk, retrying only the records the response flags as failed (FR-003).

        The wait comes before the next attempt and never before abandoning (SPEC-027 FR-003):
        this loop re-sends the records the destination flagged, and the canonical reason it flags
        them is throttling, which an immediate re-send makes worse.

        Args:
          records: One chunk's request entries.

        Returns:
          How many records the stream accepted, so :meth:`emit` can tell "nothing landed" from a
          partial success, or ``None`` when the response could not be adjudicated, which is
          neither. "The stream did not say" must not be read as "the stream took nothing": that
          reading would make :meth:`emit` raise, and the worker's retry would re-send a chunk
          SPEC-018 settled must never be re-sent.

        Raises:
          Exception: Whatever the client raises.
        """
        sent = len(records)
        for attempt in range(self.max_retries + 1):
            response = self.client.put_records(StreamName=self.stream_name, Records=records)
            if not response.get("FailedRecordCount"):
                return sent
            results = usable_results(response.get("Records"))
            verdict = adjudicate_positional(records, results)
            if verdict.unadjudicated:
                with self._counter_lock:
                    self.dropped_unadjudicated += verdict.unadjudicated
                _diag.lost(
                    "record",
                    verdict.unadjudicated,
                    f"KinesisSink could not adjudicate a put_records response ({len(records)} "
                    f"record(s) sent, {len(results)} result(s) returned); abandoned, not retried",
                )
                return None
            records = verdict.retry
            if not records:
                return sent
            if attempt < self.max_retries:
                wait(_BACKOFF_BASE * (2**attempt), self.stop_signal)
            if attempt >= self.max_retries:
                with self._counter_lock:
                    self.failed += len(records)
                _diag.lost(
                    "record",
                    len(records),
                    f"KinesisSink, still failing after {self.max_retries + 1} attempts; abandoned",
                )
                return sent - len(records)
        return 0
