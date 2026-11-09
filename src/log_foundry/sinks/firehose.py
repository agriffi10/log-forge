"""FirehoseSink — put event records to a Firehose delivery stream (arch §8, SPEC-010)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from log_foundry import _diag
from log_foundry.sinks import _engine
from log_foundry.sinks._chunk import chunk_items
from log_foundry.sinks._classify import aws_retryable, is_retryable
from log_foundry.sinks._encode import encode_round
from log_foundry.sinks._engine import emit_via_deliver, run_context
from log_foundry.sinks._extras import import_extra
from log_foundry.sinks._retry import floor_count
from log_foundry.sinks._rounds import Called, Lost, mark_sent, settle_positional
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict

if TYPE_CHECKING:
    import threading
    from collections.abc import Mapping

    from log_foundry.sinks._deferral import Deferral
    from log_foundry.sinks._engine import _Drops
    from log_foundry.sinks.base import SinkLosses

__all__ = ["FirehoseSink"]


class FirehoseSink:
    """A :class:`~log_foundry.sinks.base.Sink` that writes events to a delivery stream.

    This is the same durable-buffer shape as
    :class:`~log_foundry.sinks.kinesis.KinesisSink`, using Firehose's ``put_record_batch``.
    ``boto3`` is the optional ``aws`` extra, imported lazily. The worst-case delay (SPEC-027
    FR-005) is ``max_retries`` interruptible waits per run, 0.7 s at the defaults: it delivers through
    SPEC-072's engine (SPEC-074 FR-002), one call per chunk per round, the engine resending only
    the records the reply codes as failed.

    Attributes:
      log_foundry_counters: The engine's event totals, which :meth:`losses` reports.
      failed: Records the delivery stream said failed or never answered for, ``failed_events`` less
        ``dropped_unadjudicated``, as 1.x kept the two apart; assignable.
      dropped_oversized: Records too large for the per-record limit to ever accept, counted
        only by a run that settled.
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
          max_retries: Further rounds the engine gives the records still failing, floored at
            zero as ``Worker._emit`` floors its own (SPEC-021). Anything but an ``int`` is refused
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
        self.log_foundry_counters = SinkCounters()
        self.dropped_oversized = 0
        self.dropped_unadjudicated = 0

    @property
    def _counter_lock(self) -> threading.Lock:
        """The counters' lock, which every commit and ``losses()`` read takes.

        Assigning it replaces the counters' lock, so a test installing a counting lock here
        counts every acquisition the engine makes (SPEC-074 FR-001).

        Args:
          None.

        Returns:
          The lock.

        Raises:
          None.
        """
        return self.log_foundry_counters._counter_lock

    @_counter_lock.setter
    def _counter_lock(self, lock: threading.Lock) -> None:
        """Replaces the counters' lock.

        Args:
          lock: The new lock.

        Returns:
          None.

        Raises:
          None.
        """
        self.log_foundry_counters._counter_lock = lock

    @property
    def failed(self) -> int:
        """Records lost to an exhausted retry, a refused call or a raise, never the unadjudicated.

        The engine's ``failed_events`` less ``dropped_unadjudicated``: 1.x kept the two apart on
        the instance and summed them in ``losses().failed`` (SPEC-074 FR-001). Both are read
        under the counter lock, which the engine's one commit holds while it moves both, so a
        read never sees one moved and not the other.

        Args:
          None.

        Returns:
          The count.

        Raises:
          None.
        """
        with self._counter_lock:
            return self.log_foundry_counters.failed_events - self.dropped_unadjudicated

    @failed.setter
    def failed(self, value: int) -> None:
        """Sets the count under the counter lock, keeping the unadjudicated in ``losses()``.

        Args:
          value: The new count.

        Returns:
          None.

        Raises:
          None.
        """
        with self._counter_lock:
            self.log_foundry_counters.failed_events = value + self.dropped_unadjudicated

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Delivers the batch through the engine inline, keeping the 1.x ``emit`` contract.

        An unadjudicable chunk is unknown, not nothing, and suppresses the raise: SPEC-018
        settled that such a chunk is abandoned rather than resent, because some of it may have
        landed. Every drop — oversize, unencodable — is counted and announced only when ``emit``
        returns; on the engine, only when the run settles.

        Args:
          batch: The events to write.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When the run failed: nothing landed, something was still failing or
            refused, and nothing was unadjudicable (SPEC-026 FR-001).
        """
        emit_via_deliver(self, batch)

    def losses(self) -> SinkLosses:
        """Reports oversized and unencodable drops, abandoned records and unadjudicable chunks.

        The engine's totals (SPEC-074 FR-001): ``dropped`` folds in the events the encode step
        could not encode (SPEC-069 FR-001), and ``dropped_oversized`` still means oversize only.

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
        return self.log_foundry_counters.losses()

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        """Runs one round and reports each event's fate by index (SPEC-074 FR-002).

        Args:
          batch: The events.

        Returns:
          The round's verdict.

        Raises:
          None. A ``BaseException`` from the client propagates.
        """
        return _engine.one_round(self._round, batch)

    def _round(self, batch: list[dict[str, object]]) -> Verdict:
        """Sends every remaining chunk once and classifies every event of the round.

        Every event is encoded once per run (:func:`~log_foundry.sinks._encode.encode_round`),
        an unencodable one ``DROPPED`` with SPEC-069's line; :meth:`_records` drops an oversized
        one; the rest are chunked with ``chunk_items`` and each chunk sent once through
        :meth:`_send`, its reply settled positionally (:func:`settle_positional`).

        Args:
          batch: The events this round is offered.

        Returns:
          A verdict with one outcome per event of ``batch``.

        Raises:
          None. A ``BaseException`` from the client propagates.
        """
        context = run_context()
        fates = [Outcome.DROPPED] * len(batch)
        pairs = encode_round(self, batch, self._encode, sink=type(self).__name__)
        records = self._records(pairs, context.drops(self))
        lost = Lost(self)
        for chunk in chunk_items(
            records,
            max_count=self.MAX_RECORDS,
            max_bytes=self.MAX_REQUEST_BYTES,
            size_of=lambda pair: len(pair[1]["Data"]),
        ):
            positions = [position for position, _ in chunk]
            carried = [batch[position] for position in positions]
            sending = [record for _, record in chunk]
            mark_sent(self, carried)
            sent = self._send(sending)
            settle_positional(
                self,
                positions=positions,
                carried=carried,
                records=sending,
                error=sent.error,
                retryable=sent.retryable,
                response=sent.result,
                call="put_record_batch",
                name="FirehoseSink",
                count_key="FailedPutCount",
                results_key="RequestResponses",
                fates=fates,
                lost=lost,
            )
        lost.register()
        return Verdict(outcomes=tuple(fates))

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
          TypeError: If the event is not JSON-serializable, which ``encode_round`` turns into a
            drop of this one event. Neither source of events produces one: assembly coerces every
            value (``sanitize``), and a ``TransformSink`` checks and coerces what its function
            returns (SPEC-058 FR-001).
        """
        return json.dumps(event).encode("utf-8") + b"\n"

    def _records(
        self, encoded: list[tuple[int, bytes]], pending: Deferral | _Drops
    ) -> list[tuple[int, dict[str, Any]]]:
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
        only when ``emit`` returns (SPEC-059 FR-001) — on the engine, ``pending`` is the run
        context's ``SETTLED`` view, so only when the run settles (SPEC-074). Each dropped record's size is
        collected here, as the drop is decided, and the announcer writes one line per size —
        today's shape, each line naming its own record rather than the last one met.

        Args:
          encoded: Each encodable event's position in the round's batch with its newline-terminated data.
          pending: The run's ``SETTLED`` view, which holds every drop decided here until the run
            is known to settle.

        Returns:
          The position and request entry of each record that can be sent.

        Raises:
          None.
        """
        records: list[tuple[int, dict[str, Any]]] = []
        oversized: list[int] = []
        for position, data in encoded:
            if len(data) > self.MAX_RECORD_BYTES:
                oversized.append(len(data))
                continue
            records.append((position, {"Data": data}))
        if oversized:
            pending.count("dropped_oversized", len(oversized))
            pending.announce(lambda: self._announce_oversized(oversized))
        return records

    def _announce_oversized(self, sizes: list[int]) -> None:
        """Writes one line per record dropped for exceeding the per-record limit (FR-011).

        Called after the count is committed, outside the counter lock, and only by a run that
        settled (SPEC-059 FR-001, SPEC-074).

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
        code. SPEC-072's engine takes no entry predicate, so nothing calls it; it is declared so the
        rule is written down where the sink's other predicate is.

        Args:
          entry: The failed record's result entry.

        Returns:
          True.

        Raises:
          None.
        """
        return True

    def _send(self, records: list[dict[str, Any]]) -> Called[object]:
        """Sends one chunk once (SPEC-074 FR-002); the reply is read by the round.

        Nothing here waits or retries: the engine resends a record the reply codes as failed in a
        later round. **A client exception costs this chunk, never the batch** (SPEC-048 FR-002),
        and is treated as **provable non-delivery** for it: SPEC-018's "unadjudicable" is a
        property of a *response*, and an exception is not one. A read timeout means the request
        went out and the reply was lost, so a resend may duplicate — taken because an unreachable
        endpoint is the common case. **The failed count and the coded records must agree**
        (SPEC-070 FR-003): the round passes the reply whole to ``adjudicate_positional`` with its
        count, and every disagreement is unadjudicated, never resent.

        Args:
          records: One chunk's request entries.

        Returns:
          The call's reply or its exception.

        Raises:
          None. ``Exception`` is caught rather than ``BaseException``, so a ``KeyboardInterrupt``
            or ``SystemExit`` still reaches the caller (SPEC-025 FR-004).
        """
        try:
            response = self.client.put_record_batch(
                DeliveryStreamName=self.delivery_stream, Records=records
            )
        except Exception as err:
            return Called(None, error=err, retryable=is_retryable(self._retryable_error, err))
        return Called(response)
