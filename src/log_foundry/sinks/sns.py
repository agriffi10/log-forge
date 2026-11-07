"""SNSSink — publish events to an SNS topic (arch §8, SPEC-010)."""

from __future__ import annotations

import json
import threading
from typing import TYPE_CHECKING, Any

from log_foundry import _diag
from log_foundry.sinks._chunk import chunk_items
from log_foundry.sinks._classify import aws_retryable, is_retryable, sender_fault
from log_foundry.sinks._deferral import Deferral
from log_foundry.sinks._encode import encode_events
from log_foundry.sinks._retry import floor_count, wait
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = ["SNSSink"]

_BACKOFF_BASE = 0.1


class SNSSink:
    """A :class:`~log_foundry.sinks.base.Sink` that publishes events to an SNS topic.

    This mirrors the SPEC-005 ``SQSSink`` partial-failure policy on SNS's ``publish_batch``:
    the response's ``Failed`` list is retried within a bounded count, except a sender fault, which
    is counted at once and never retried (SPEC-070 FR-003), and entries still failing are counted
    and logged. ``boto3`` is the optional ``aws`` extra, imported lazily.

    The worst-case delay (SPEC-027 FR-005) is ``max_retries`` waits per chunk, 0.7 s at the
    defaults. The waits are interruptible, so ``shutdown()`` cuts one short.

    The driver requirement satisfied (SPEC-028 FR-002): this sink takes **no** transport
    lock. ``boto3`` clients are documented thread-safe, this one is built once in
    ``__init__``, and the sink rebinds nothing after construction.

    It also **adds no post-close guard** (SPEC-032 FR-003): ``close()`` is a documented no-op,
    because the client is the caller's to release or the SDK's to reap, so a batch emitted
    afterwards still reaches the topic.


    It keeps **no** client buffer (SPEC-036 FR-002): the driver call returns only once the
    destination has the batch, so nothing is queued locally between emits.
    """

    MAX_BATCH = 10
    MAX_BYTES = 256 * 1024

    def __init__(self, topic_arn: str, *, client: Any = None, max_retries: int = 3) -> None:
        """Binds the sink to a topic.

        Args:
          topic_arn: The topic to publish to.
          client: A boto3-shaped SNS client, or ``None`` to build one lazily.
          max_retries: Retries for the failed entries of a chunk, floored at zero as
            ``Worker._emit`` floors its own (SPEC-021) — a negative value returned from ``_send``
            having published nothing, and reported success. Anything but an ``int`` is refused
            with ``TypeError`` (SPEC-066 FR-001), before any client is built; a ``bool`` or
            ``int`` subclass is read as the ``int`` it holds.

        Returns:
          None.

        Raises:
          TypeError: If ``max_retries`` is not an ``int`` (SPEC-066).
          ImportError: If ``boto3`` is needed and the ``aws`` extra is not installed.
        """
        self.max_retries = floor_count(max_retries, 0, "max_retries", "SNSSink")
        if client is None:
            import boto3  # type: ignore[import-not-found]

            client = boto3.client("sns")
        self.topic_arn = topic_arn
        self.client = client
        self.log_foundry_stop_signal: threading.Event | None = None
        self.failed = 0
        self.dropped_oversized = 0
        self._unencodable = 0
        self._counter_lock = threading.Lock()

    def losses(self) -> SinkLosses:
        """Reports oversized drops and entries still failing past the retry bound (FR-002).

        ``dropped`` folds in the events :meth:`emit` could not encode (SPEC-069 FR-001);
        ``dropped_oversized`` still means oversize only.

        Args:
          None.

        Returns:
          The counters.

        Raises:
          None.
        """
        with self._counter_lock:
            return SinkLosses(
                dropped=self.dropped_oversized + self._unencodable, failed=self.failed
            )

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Re-chunks to the publish limits and sends each chunk, retrying failures (FR-010).

        An oversize drop :meth:`_bodies` decides is counted and announced only when ``emit``
        returns, through the deferral helper whose ``with`` is the outermost block of the work
        (SPEC-059 FR-001). The drop is decided before any publish, and this call can still raise
        afterwards — the total-failure raise below, a malformed ``Failed`` entry, a
        ``BaseException`` from the client — and be retried whole; counting it at once counted one
        oversized event once per attempt.

        Every event is encoded before any message is sized or published (SPEC-069 FR-001),
        through :meth:`_encode` and the encode helper: one that cannot be encoded is dropped,
        counted and announced only when ``emit`` returns, through the same ledger, and a batch of
        nothing encodable publishes nothing and returns (FR-002).

        Args:
          batch: The events to publish.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When every chunk failed and at least one was sent (SPEC-026
            FR-001), unless every entry that failed was a sender fault: a batch SNS rejected only
            as the caller's fault returns with the entries counted, as ``SQSSink``'s does,
            because a resend can only fail the same way (SPEC-070 FR-003). Events dropped
            before sending, being too large to ever fit, are not a send failure and do not make a
            batch of nothing but oversized events raise — they can never be retried into
            existence, and are reported through :meth:`losses`. A raise leaves none of them
            counted.
        """
        chunks = delivered = 0
        recoverable_loss = False
        with Deferral(self) as pending:
            encoded = encode_events(batch, self._encode, pending, sink=type(self).__name__)
            bodies = self._bodies(encoded, pending)
            for chunk in chunk_items(
                bodies, max_count=self.MAX_BATCH, max_bytes=self.MAX_BYTES, size_of=len
            ):
                chunks += 1
                accepted, retryable_lost = self._send(chunk)
                delivered += accepted
                recoverable_loss = recoverable_loss or retryable_lost
            if chunks and not delivered and recoverable_loss:
                raise SinkDeliveryError(f"SNSSink published none of {chunks} chunk(s)")

    def close(self) -> None:
        """Does nothing, since the sink buffers nothing internally (FR-001).

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """

    def _encode(self, event: dict[str, object]) -> str:
        """Encodes one event as its message body.

        Args:
          event: The event to encode.

        Returns:
          The JSON text.

        Raises:
          TypeError: If the event is not JSON-serializable, which ``encode_events`` turns into a
            drop of this one event. Neither source of events produces one: assembly coerces every
            value (``sanitize``), and a ``TransformSink`` checks and coerces what its function
            returns (SPEC-058 FR-001).
        """
        return json.dumps(event)

    def _bodies(
        self, encoded: list[tuple[dict[str, object], str]], pending: Deferral
    ) -> list[str]:
        """Keeps each encoded body, dropping any message too large to ever fit (FR-011).

        It takes what :meth:`_encode` built (SPEC-069 FR-001), so the size check runs over
        encoded forms and nothing here can fail on an event.

        A drop goes to ``pending`` rather than to the counter, so it is counted and announced
        only when ``emit`` returns (SPEC-059 FR-001): a raise anywhere later in the same call
        discards it, and the worker's retry decides it afresh. The line names no per-event value,
        so each drop registers its own count and its own line, one per event as before.

        Args:
          encoded: Each encodable event with its JSON body.
          pending: The calling ``emit``'s deferral, which holds every drop decided here until
            that ``emit`` is known not to raise.

        Returns:
          The serialized bodies that can be sent.

        Raises:
          None.
        """
        bodies: list[str] = []
        for _, body in encoded:
            if len(body.encode("utf-8")) > self.MAX_BYTES:
                pending.count("dropped_oversized")
                pending.announce(
                    lambda: _diag.lost(
                        "event", 1, f"SNSSink, exceeds the {self.MAX_BYTES}-byte message limit"
                    )
                )
                continue
            bodies.append(body)
        return bodies

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
        """Reports whether a resend of one rejected entry may succeed (SPEC-070 FR-003).

        A sender fault is the caller's: a byte-identical resend fails again, so it is counted and
        never retried. Read through :func:`~log_foundry.sinks._classify.sender_fault`, which SQS
        and SNS share, so the two services answer the same required field the same way.

        Args:
          entry: One item of the reply's ``Failed`` list.

        Returns:
          False for a sender fault, True otherwise.

        Raises:
          AttributeError: When the entry is not a mapping, as before.
        """
        return not sender_fault(entry)

    def _send(self, bodies: list[str]) -> tuple[int, bool]:
        """Publishes one chunk, retrying only the failed entries within the bound (FR-010).

        The wait comes before the next attempt and never before abandoning (SPEC-027 FR-003):
        this loop re-sends the entries the destination flagged, and the canonical reason it flags
        them is throttling, which an immediate re-send makes worse.

        **A client exception costs this chunk, never the batch** (SPEC-048 FR-002). It used to
        propagate out of :meth:`emit`, so a failure on chunk N after chunks 1..N-1 had landed made
        the worker re-send the whole batch and duplicate everything already delivered — the exit
        drain, one large batch by construction, is exactly that shape. The guard sits around the
        client call *inside* this loop rather than around ``_send`` in ``emit``, so only the
        messages still outstanding at the failing attempt are charged and the ones already accepted
        still count toward the return.

        A client exception is treated as **provable non-delivery** for the chunk, so a wholly
        failed batch still raises. The cost is written down —
        a read timeout means the request went out and the reply was lost, so a re-send may
        duplicate — and taken because an unreachable endpoint is the common case and is exactly
        what the worker's retry exists for, while suppressing the raise would lose every event of
        every batch for a whole outage, silently.

        **An entry SNS marks ``SenderFault`` is not retried** (SPEC-070 FR-003): SNS's
        ``BatchResultErrorEntry`` carries the same required field as SQS's, and a byte-identical
        resend of the caller's fault fails again. It is counted on ``failed`` when the reply
        arrives, announced with the first error code and ``not retried``, and dropped from the
        next round, exactly as ``SQSSink._send`` does; only :meth:`_retryable_entry`'s entries are
        resent. It used to retry every entry named in ``Failed``. Acceptance is counted per round
        by ``Id``, which is what keeps a dropped sender fault from reading as delivered.

        Args:
          bodies: One chunk's serialized events.

        Returns:
          How many entries SNS accepted, so :meth:`emit` can tell "nothing landed" from a partial
          success, and whether anything was given up on that a resend could plausibly recover. A
          chunk rejected wholly as sender faults reports False there.

        Raises:
          None. ``Exception`` is caught rather than ``BaseException``, so a ``KeyboardInterrupt``
            or ``SystemExit`` still reaches the caller (SPEC-025 FR-004).
        """
        entries = [{"Id": str(i), "Message": body} for i, body in enumerate(bodies)]
        accepted = 0
        for attempt in range(self.max_retries + 1):
            try:
                response = self.client.publish_batch(
                    TopicArn=self.topic_arn, PublishBatchRequestEntries=entries
                )
            except Exception as err:
                if attempt < self.max_retries and is_retryable(self._retryable_error, err):
                    wait(_BACKOFF_BASE * (2**attempt), self.log_foundry_stop_signal)
                    continue
                with self._counter_lock:
                    self.failed += len(entries)
                _diag.lost(
                    "message",
                    len(entries),
                    f"SNSSink, {attempt + 1} attempt(s), {type(err).__name__}",
                )
                return accepted, True
            failed = response.get("Failed", [])
            failed_ids = {entry["Id"] for entry in failed}
            accepted += sum(1 for entry in entries if entry["Id"] not in failed_ids)
            if not failed:
                return accepted, False
            sender_faults = [item for item in failed if not self._retryable_entry(item)]
            if sender_faults:
                with self._counter_lock:
                    self.failed += len(sender_faults)
                _diag.lost(
                    "message",
                    len(sender_faults),
                    f"SNSSink, rejected as invalid (first code: "
                    f"{sender_faults[0].get('Code', 'unknown')}); not retried",
                )
            retryable_ids = {item["Id"] for item in failed if self._retryable_entry(item)}
            if not retryable_ids:
                return accepted, False
            entries = [entry for entry in entries if entry["Id"] in retryable_ids]
            if attempt < self.max_retries:
                wait(_BACKOFF_BASE * (2**attempt), self.log_foundry_stop_signal)
            if attempt >= self.max_retries:
                with self._counter_lock:
                    self.failed += len(entries)
                _diag.lost(
                    "message",
                    len(entries),
                    f"SNSSink, still failing after {self.max_retries + 1} attempts; abandoned",
                )
                return accepted, True
        return accepted, False
