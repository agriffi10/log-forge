"""FirehoseSink — put event records to a Firehose delivery stream (arch §8, SPEC-010)."""

from __future__ import annotations

import json
import threading
from typing import TYPE_CHECKING, Any

from log_foundry import _diag
from log_foundry.sinks._batch import (
    ABSENT,
    adjudicate_positional,
    describe_count,
    results_rejected,
    usable_results,
)
from log_foundry.sinks._chunk import chunk_items
from log_foundry.sinks._classify import aws_retryable, is_retryable
from log_foundry.sinks._deferral import Deferral
from log_foundry.sinks._encode import encode_events
from log_foundry.sinks._extras import import_extra
from log_foundry.sinks._retry import floor_count, wait
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = ["FirehoseSink"]

_BACKOFF_BASE = 0.1


class FirehoseSink:
    """A :class:`~log_foundry.sinks.base.Sink` that writes events to a delivery stream.

    This is the same durable-buffer shape as
    :class:`~log_foundry.sinks.kinesis.KinesisSink`, using Firehose's ``put_record_batch``.
    ``boto3`` is the optional ``aws`` extra, imported lazily. The worst-case delay (SPEC-027
    FR-005) is ``max_retries`` interruptible waits per chunk, 0.7 s at the defaults.

    Attributes:
      failed: Records the delivery stream said failed, which still failed after every retry.
      dropped_oversized: Records too large for the per-record limit to ever accept.
      dropped_unadjudicated: Records in a response whose ``RequestResponses`` did not describe
        the chunk sent, so none could be paired to an outcome. A non-zero count means those
        records were abandoned without the stream ever confirming them — treat it as loss, and
        as a sign the client is not AWS-shaped.

    The driver requirement satisfied (SPEC-028 FR-002): this sink takes **no** transport
    lock. ``boto3`` clients are documented thread-safe, this one is built once in
    ``__init__``, and the sink rebinds nothing after construction.

    It also **adds no post-close guard** (SPEC-032 FR-003): ``close()`` is a documented no-op,
    because the client is the caller's to release or the SDK's to reap, so a batch emitted
    afterwards still reaches the delivery stream.


    It keeps **no** client buffer (SPEC-036 FR-002): the driver call returns only once the
    destination has the batch, so nothing is queued locally between emits.
    """

    MAX_RECORDS = 500
    MAX_REQUEST_BYTES = 4 * 1024 * 1024
    MAX_RECORD_BYTES = 1_024_000
    """Firehose's documented per-record ceiling: 1,000 KiB, which is **not** ``1024 * 1024``.

    The 24,576-byte gap between the two is a band of records this sink used to pass through and
    the service then rejected (SPEC-038 FR-009).
    """

    def __init__(self, delivery_stream: str, *, client: Any = None, max_retries: int = 3) -> None:
        """Binds the sink to a delivery stream.

        Args:
          delivery_stream: The stream to write to.
          client: A boto3-shaped Firehose client, or ``None`` to build one lazily.
          max_retries: Retries for the failed records of a chunk, floored at zero as
            ``Worker._emit`` floors its own (SPEC-021) — a negative value returned from ``_send``
            having sent nothing, and reported success. Anything but an ``int`` is refused
            with ``TypeError`` (SPEC-066 FR-001), before any client is built; a ``bool`` or
            ``int`` subclass is read as the ``int`` it holds.

        Returns:
          None.

        Raises:
          TypeError: If ``max_retries`` is not an ``int`` (SPEC-066).
          ImportError: If ``boto3`` is needed and the ``aws`` extra is not installed.
        """
        self.max_retries = floor_count(max_retries, 0, "max_retries", "FirehoseSink")
        if client is None:
            client = import_extra("boto3", "aws").client("firehose")
        self.delivery_stream = delivery_stream
        self.client = client
        self.log_foundry_stop_signal: threading.Event | None = None
        self.failed = 0
        self.dropped_oversized = 0
        self._unencodable = 0
        self.dropped_unadjudicated = 0
        self._counter_lock = threading.Lock()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Re-chunks to the request limits and sends each chunk, retrying failures (FR-004).

        An unadjudicable chunk is unknown, not nothing, and suppresses the raise: SPEC-018
        settled that such a chunk is abandoned rather than re-sent, because the API reported a
        failure count, so some of it landed and the worker's retry would duplicate that
        downstream forever.

        That suppression is batch-wide, unlike ``SQSSink``'s. A sender fault is a rejection, so
        re-sending is futile rather than harmful, while "unadjudicable" means this sink cannot
        tell whether the records landed — once any chunk is in that state the emit can no longer
        prove nothing was delivered, so the events in a plainly-failed sibling chunk stay a
        counted loss rather than a duplicated delivery.

        An oversize drop :meth:`_records` decides is counted and announced only when ``emit``
        returns, through the deferral helper whose ``with`` is the outermost block of the work
        (SPEC-059 FR-001). The drop is decided before any send, and this call can still raise
        afterwards — the total-failure raise below, a ``BaseException`` from the client — and be
        retried whole; counting it at once counted one oversized event once per attempt.

        Every event is encoded before any record is sized or sent (SPEC-069 FR-001), through
        :meth:`_encode` and the encode helper: one that cannot be encoded is dropped, counted and
        announced only when ``emit`` returns, through the same ledger, and a batch of nothing
        encodable sends nothing and returns (FR-002).

        Args:
          batch: The events to write.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When every chunk failed, at least one was sent, and none was
            unadjudicable (SPEC-026 FR-001). Records dropped before sending are not a send
            failure and are reported through :meth:`losses`; a raise leaves none of them
            counted.
        """
        chunks = delivered = 0
        unknown = False
        with Deferral(self) as pending:
            encoded = encode_events(batch, self._encode, pending, sink=type(self).__name__)
            records = self._records(encoded, pending)
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
        """Reports oversized and unencodable drops, abandoned records and unadjudicable chunks.

        ``dropped`` folds in the events :meth:`emit` could not encode (SPEC-069 FR-001);
        ``dropped_oversized`` still means oversize only.

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
                dropped=self.dropped_oversized + self._unencodable,
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

    def _encode(self, event: dict[str, object]) -> bytes:
        """Encodes one event as its newline-terminated record data.

        Args:
          event: The event to encode.

        Returns:
          The UTF-8 JSON data with its trailing newline.

        Raises:
          TypeError: If the event is not JSON-serializable, which ``encode_events`` turns into a
            drop of this one event. Neither source of events produces one: assembly coerces every
            value (``sanitize``), and a ``TransformSink`` checks and coerces what its function
            returns (SPEC-058 FR-001).
        """
        return json.dumps(event).encode("utf-8") + b"\n"

    def _records(
        self, encoded: list[tuple[dict[str, object], bytes]], pending: Deferral
    ) -> list[dict[str, Any]]:
        """Builds the request entries, dropping any record too large to ever fit (FR-011).

        It takes what :meth:`_encode` built (SPEC-069 FR-001), so sizing and the oversize drop
        run over encoded forms and nothing here can fail on an event.

        Each record ends with a newline (SPEC-038 FR-005). Firehose concatenates record payloads
        verbatim into the delivery buffer and the *producer* must supply the separator, so
        without it the S3 objects read ``{"a":1}{"b":2}`` — unparseable by Athena, Glue and
        OpenSearch ingest, and inconsistent with the NDJSON every other sink here emits. The
        newline is part of ``Data``, so it is charged to both the per-record limit checked below
        and the per-request budget :meth:`emit` chunks by.

        A drop goes to ``pending`` rather than to the counter, so it is counted and announced
        only when ``emit`` returns (SPEC-059 FR-001): a raise anywhere later in the same call
        discards it, and the worker's retry decides it afresh. Each dropped record's size is
        collected here, as the drop is decided, and the announcer writes one line per size —
        today's shape, each line naming its own record rather than the last one met.

        Args:
          encoded: Each encodable event with its newline-terminated data.
          pending: The calling ``emit``'s deferral, which holds every drop decided here until
            that ``emit`` is known not to raise.

        Returns:
          The records that can be sent.

        Raises:
          None.
        """
        records: list[dict[str, Any]] = []
        oversized: list[int] = []
        for _, data in encoded:
            if len(data) > self.MAX_RECORD_BYTES:
                oversized.append(len(data))
                continue
            records.append({"Data": data})
        if oversized:
            pending.count("dropped_oversized", len(oversized))
            pending.announce(lambda: self._announce_oversized(oversized))
        return records

    def _announce_oversized(self, sizes: list[int]) -> None:
        """Writes one line per record dropped for exceeding the per-record limit (FR-011).

        Called by the calling ``emit``'s deferral after the count is committed, outside the
        counter lock, and only when that ``emit`` returned normally (SPEC-059 FR-001).

        Args:
          sizes: The size of each dropped record, its trailing newline included, in the order
            they were met.

        Returns:
          None.

        Raises:
          None.
        """
        for size in sizes:
            _diag.lost(
                "event",
                1,
                f"FirehoseSink, {size} bytes exceeds the "
                f"{self.MAX_RECORD_BYTES}-byte per-record limit",
            )

    def _retryable_error(self, err: Exception) -> bool:
        """Reports whether a failed call is worth another attempt (SPEC-070 FR-002).

        botocore's transient and throttling failures, through the predicate the four AWS sinks
        share (:func:`~log_foundry.sinks._classify.aws_retryable`); anything else — a
        ``ParamValidationError``, an ``AccessDenied``, a ``TypeError`` from the caller's own
        code — fails at once, counted and announced on its first attempt with no backoff wait.

        Args:
          err: The exception the attempt raised.

        Returns:
          True when another attempt may succeed.

        Raises:
          None. :func:`~log_foundry.sinks._classify.is_retryable` reads a raise here as False.
        """
        return aws_retryable(err)

    def _retryable_entry(self, entry: Mapping[str, object]) -> bool:
        """Reports whether a resend of one failed record may succeed: always (SPEC-070 FR-003).

        Firehose ``PutRecordBatch`` documents a record's ``ErrorCode`` as one of
        ``ServiceUnavailableException`` and ``InternalFailure``, each transient, so the vendor's
        whole per-record list is retryable. A code the page does not list — or one that is not a
        ``str`` — is retried too, bounded by ``max_retries``: the permanent kinds of failure on
        this service are reported as whole-request errors (``InvalidArgumentException``, the KMS
        errors), so the per-record channel carries what the service failed to do; the page's only
        remedy for a failed record is to resend it; and the two mistakes are not symmetric — an
        undocumented permanent code costs at most ``max_retries`` bounded, idle waits, while an
        undocumented transient code read as permanent loses records the service would have
        taken.

        It is asked only about a record the response marks failed, by a truthy ``ErrorCode``, and
        :meth:`_send` does not ask it: with every answer True the branch for False would be dead
        code. It is declared for SPEC-072's engine, which takes it as an argument.

        Args:
          entry: The failed record's result entry.

        Returns:
          True.

        Raises:
          None.
        """
        return True

    def _send(self, records: list[dict[str, Any]]) -> int | None:
        """Sends one chunk, retrying only the entries the response flags as failed (FR-004).

        The wait comes before the next attempt and never before abandoning (SPEC-027 FR-003):
        this loop re-sends the records the destination flagged, and the canonical reason it flags
        them is throttling, which an immediate re-send makes worse.

        **A client exception costs this chunk, never the batch** (SPEC-048 FR-002). It used to
        propagate out of :meth:`emit`, so a failure on chunk N after chunks 1..N-1 had landed made
        the worker re-send the whole batch and duplicate everything already delivered — the exit
        drain, one large batch by construction, is exactly that shape. The guard sits around the
        client call *inside* this loop rather than around ``_send`` in ``emit``, so only the
        records still outstanding at the failing attempt are charged and the ones already accepted
        still count toward the return.

        A client exception is treated as **provable non-delivery** for the chunk, so a wholly
        failed batch still raises and ``unknown`` is untouched: SPEC-018's "unadjudicable" is a
        property of a *response*, and an exception is not a response. The cost is written down —
        a read timeout means the request went out and the reply was lost, so a re-send may
        duplicate — and taken because an unreachable endpoint is the common case and is exactly
        what the worker's retry exists for, while suppressing the raise would lose every event of
        every batch for a whole outage, silently.

        **The failed count and the coded records must agree** (SPEC-070 FR-003). This used to
        read the count first and return the chunk as sent when it was ``0`` or missing, so a
        reply saying ``0`` beside coded records, or no count beside them, counted nothing; and a
        positive count beside no coded record retried nothing and counted the chunk as sent. The
        reply now goes whole to ``adjudicate_positional`` with its count: only a ``0`` or absent
        count with no coded record is delivered, only a positive count matched by exactly that
        many coded records is retried, and every other combination is unadjudicated — counted on
        ``dropped_unadjudicated``, announced once with the count named, never resent — because
        the reply's two halves disagree about which records landed.

        Args:
          records: One chunk's request entries.

        Returns:
          How many records the stream accepted, so :meth:`emit` can tell "nothing landed" from a
          partial success, or ``None`` when the response could not be adjudicated, which is
          neither. "The stream did not say" must not be read as "the stream took nothing": that
          reading would make :meth:`emit` raise, and the worker's retry would re-send a chunk
          SPEC-018 settled must never be re-sent.

        Raises:
          None. ``Exception`` is caught rather than ``BaseException``, so a ``KeyboardInterrupt``
            or ``SystemExit`` still reaches the caller (SPEC-025 FR-004).
        """
        sent = len(records)
        for attempt in range(self.max_retries + 1):
            try:
                response = self.client.put_record_batch(
                    DeliveryStreamName=self.delivery_stream, Records=records
                )
            except Exception as err:
                if attempt < self.max_retries and is_retryable(self._retryable_error, err):
                    wait(_BACKOFF_BASE * (2**attempt), self.log_foundry_stop_signal)
                    continue
                with self._counter_lock:
                    self.failed += len(records)
                _diag.lost(
                    "record",
                    len(records),
                    f"FirehoseSink, {attempt + 1} attempt(s), {type(err).__name__}",
                )
                return sent - len(records)
            failed_count = response.get("FailedPutCount", ABSENT)
            raw = response.get("RequestResponses")
            results = usable_results(raw)
            rejected = results_rejected(raw)
            verdict = adjudicate_positional(
                records, results, failed_count=failed_count, rejected=rejected
            )
            if verdict.unadjudicated:
                coded = sum(1 for result in results if result.get("ErrorCode"))
                returned = (
                    "an unreadable result array"
                    if rejected
                    else f"{len(results)} result(s) returned"
                )
                with self._counter_lock:
                    self.dropped_unadjudicated += verdict.unadjudicated
                _diag.lost(
                    "record",
                    verdict.unadjudicated,
                    f"FirehoseSink could not adjudicate a put_record_batch response "
                    f"({len(records)} record(s) sent, {returned}, "
                    f"{coded} coded, "
                    f"{describe_count(failed_count)}); abandoned, not retried",
                )
                return None
            records = verdict.retry
            if not records:
                return sent
            if attempt < self.max_retries:
                wait(_BACKOFF_BASE * (2**attempt), self.log_foundry_stop_signal)
            if attempt >= self.max_retries:
                with self._counter_lock:
                    self.failed += len(records)
                _diag.lost(
                    "record",
                    len(records),
                    f"FirehoseSink, still failing after {self.max_retries + 1} attempt(s); "
                    "abandoned",
                )
                return sent - len(records)
        return 0
