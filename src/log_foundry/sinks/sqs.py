"""SQSSink — ship batches to an SQS queue (arch §8, §9.1, guide Phase 10).

Ordering on a FIFO queue is best-effort across a retry boundary: an entry that fails while a
same-group entry ahead of it succeeded lands after it, because holding a whole group back on one
failure would trade log delivery for ordering the consumer can rebuild from ``timestamp``.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, NamedTuple

from log_foundry import _diag
from log_foundry.sinks._retry import wait
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

if TYPE_CHECKING:
    import threading

__all__ = ["SQSSink"]

_BACKOFF_BASE = 0.1

DEFAULT_GROUP_ID = "log-foundry"
"""Fallback group for an event carrying no usable ``trace_id`` — never send an empty group id."""

MAX_ID_LEN = 128
"""SQS maximum length for ``MessageGroupId`` and ``MessageDeduplicationId``."""

GroupIdSource = str | Callable[[dict[str, object]], str] | None
DedupIdSource = Callable[[dict[str, object]], str] | None


class _Prepared(NamedTuple):
    """One event serialized and costed, with its FIFO ids resolved.

    The ids are ``None`` on a standard queue, and are derived once here rather than again at
    send time: the deduplication fallback mints a fresh UUID, so deriving twice would bill the
    byte budget for one value and put a different one on the wire.

    Attributes:
      body: The serialized event.
      group_id: The resolved ``MessageGroupId``, or ``None``.
      dedup_id: The resolved ``MessageDeduplicationId``, or ``None``.
    """

    body: str
    group_id: str | None
    dedup_id: str | None


def _bounded(raw: str, fallback: str) -> str:
    """Normalizes a derived id, falling back when blank and truncating when over-long.

    Args:
      raw: The derived id.
      fallback: What a blank id becomes.

    Returns:
      The id, at most the SQS maximum length.

    Raises:
      None.
    """
    cleaned = raw.strip()
    if not cleaned:
        return fallback
    return cleaned[:MAX_ID_LEN]


class SQSSink:
    """A :class:`~log_foundry.sinks.base.Sink` that sends events to an SQS queue.

    SQS is the headline production path: a durable buffer that decouples the app from ELK
    availability, so events accumulate safely in the queue during downstream outages instead of
    being lost or back-pressuring the app. ``boto3`` is an optional dependency, imported lazily
    inside the sink, so the library stays dependency-free unless a sink is actually instantiated
    without an injected client.

    Both queue types are supported (SPEC-016). A ``.fifo`` queue URL selects FIFO behaviour,
    where every entry additionally carries a ``MessageGroupId``: SQS orders messages within a
    group, and the default group is the event's own ``trace_id``, which is exactly the unit
    whose events should stay ordered while keeping traces independent. Standard queues are
    untouched, their entries carrying ``Id`` and ``MessageBody`` and nothing else.

    The worst-case delay (SPEC-027 FR-005) is ``max_retries`` waits per chunk, 0.7 s at the
    defaults. The waits are interruptible, so ``shutdown()`` cuts one short.
    """

    MAX_BATCH = 10
    MAX_BYTES = 256 * 1024

    def __init__(
        self,
        queue_url: str,
        client: Any = None,
        *,
        max_retries: int = 3,
        fifo: bool | None = None,
        message_group_id: GroupIdSource = None,
        message_deduplication_id: DedupIdSource = None,
    ) -> None:
        """Binds the sink to a queue and decides its type once, not per emit.

        Args:
          queue_url: The queue to send to.
          client: A boto3-shaped SQS client, or ``None`` to build one lazily.
          max_retries: Retries for the failed entries of a chunk, floored at zero as
            ``Worker._emit`` floors its own (SPEC-021) — a negative value returned from ``_send``
            having sent nothing, and reported success.
          fifo: Overrides the queue type. AWS requires every FIFO queue name to end in
            ``.fifo``, so the suffix is a contract rather than a guess, but an explicit flag
            still wins.
          message_group_id: A constant or a callable deriving the group, or ``None`` to group by
            ``trace_id``.
          message_deduplication_id: A callable deriving the dedup id, or ``None`` to use the
            event's ``log_id``.

        Returns:
          None.

        Raises:
          ValueError: If a constant group id is blank.
          ImportError: If ``boto3`` is needed and the ``aws`` extra is not installed.
        """
        if isinstance(message_group_id, str) and not message_group_id.strip():
            raise ValueError(
                "message_group_id must be a non-empty string; pass None to group by trace_id"
            )
        if client is None:
            import boto3  # type: ignore[import-not-found]

            client = boto3.client("sqs")
        self.queue_url = queue_url
        self.client = client
        self.max_retries = max(max_retries, 0)
        self.stop_signal: threading.Event | None = None
        self.fifo = queue_url.endswith(".fifo") if fifo is None else fifo
        self.message_group_id = message_group_id
        self.message_deduplication_id = message_deduplication_id
        self.dropped_oversized = 0
        self.failed = 0

    def losses(self) -> SinkLosses:
        """Reports oversized drops and entries still failing past the retry bound (FR-002).

        Args:
          None.

        Returns:
          The counters. Sender faults land in ``failed`` alongside the retry-exhausted entries:
          SQS rejected the request itself, so the entry is as lost as one that timed out, and
          SPEC-016 settled that it must not be re-sent byte-identical.

        Raises:
          None.
        """
        return SinkLosses(dropped=self.dropped_oversized, failed=self.failed)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Re-chunks the batch to SQS limits and sends each chunk (FR-001, FR-002).

        Args:
          batch: The events to ship.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When every chunk failed, at least one was sent, and something was
            lost for a retryable reason (SPEC-026 FR-001). An event dropped for exceeding the
            message limit is not a send failure — it can never fit — so a batch of nothing but
            oversized events produces no chunks and does not raise. Nor does one whose chunks SQS
            rejected entirely as sender faults: those entries are lost, but re-sending them
            byte-identical can only fail the same way, and SPEC-016 FR-006 settled that they are
            abandoned rather than retried. That suppression is conditional rather than
            batch-wide, so a chunk lost to a throttle or an internal error still raises, at the
            cost of the sender-fault chunk being re-sent and re-rejected alongside it — futile
            but harmless, where silently dropping recoverable events would be the failure
            SPEC-026 exists to remove.
        """
        chunks = delivered = 0
        recoverable_loss = False
        for chunk in self._chunks(batch):
            chunks += 1
            accepted, retryable_lost = self._send(chunk)
            delivered += accepted
            recoverable_loss = recoverable_loss or retryable_lost
        if chunks and not delivered and recoverable_loss:
            raise SinkDeliveryError(f"SQSSink delivered none of {chunks} chunk(s)")

    def close(self) -> None:
        """Does nothing, since the sink buffers nothing internally (FR-005).

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """

    def _group_id(self, event: dict[str, object]) -> str:
        """Resolves one event's ``MessageGroupId`` (SPEC-016 FR-002).

        A constant is used as given, a callable is asked, and otherwise the event's own
        ``trace_id`` groups it. Every route is bounded, so a caller's callable returning an empty
        string cannot put an empty parameter on the wire.

        Args:
          event: The event being sent.

        Returns:
          The group id.

        Raises:
          Exception: Whatever a caller-supplied callable raises.
        """
        source = self.message_group_id
        if isinstance(source, str):
            raw = source
        elif source is not None:
            raw = source(event)
        else:
            raw = str(event.get("trace_id", ""))
        return _bounded(raw, DEFAULT_GROUP_ID)

    def _dedup_id(self, event: dict[str, object]) -> str:
        """Resolves one event's ``MessageDeduplicationId`` (SPEC-016 FR-003).

        This defaults to the event's ``log_id``, already a per-event UUID, so SQS's five-minute
        deduplication window can never collapse two genuinely distinct records. The fallback
        mints a fresh id for the same reason: a shared constant would make unrelated events
        deduplicate each other.

        Args:
          event: The event being sent.

        Returns:
          The deduplication id.

        Raises:
          Exception: Whatever a caller-supplied callable raises.
        """
        source = self.message_deduplication_id
        raw = source(event) if source is not None else str(event.get("log_id", ""))
        if raw.strip():
            return _bounded(raw, "")
        from log_foundry.ids import new_log_id

        return new_log_id()

    def _chunks(self, batch: list[dict[str, object]]) -> list[list[_Prepared]]:
        """Splits the batch into sends within both the entry-count and byte limits.

        Each event is serialized once, and on a FIFO queue its ids are resolved once here too
        (SPEC-016 FR-005), because they travel in the same request. An entry whose costed size
        exceeds the limit can never fit a message, so it is dropped with a counted warning
        (FR-004) instead of stalling the batch — and the check is made on everything that
        travels, not the body alone: a body just under the limit plus its FIFO ids would
        otherwise ship as a lone over-budget request that SQS rejects as a sender fault, losing
        the event as an opaque failure instead of a labelled oversize drop.

        Args:
          batch: The events to split.

        Returns:
          The chunks, each valid for one request.

        Raises:
          Exception: Whatever a caller-supplied id callable raises.
        """
        chunks: list[list[_Prepared]] = []
        current: list[_Prepared] = []
        current_bytes = 0
        for event in batch:
            body = json.dumps(event)
            size = len(body.encode("utf-8"))
            group_id: str | None = None
            dedup_id: str | None = None
            if self.fifo:
                group_id = self._group_id(event)
                dedup_id = self._dedup_id(event)
                size += len(group_id.encode("utf-8")) + len(dedup_id.encode("utf-8"))
            if size > self.MAX_BYTES:
                self.dropped_oversized += 1
                _diag.lost(
                    "event",
                    1,
                    f"SQSSink, {size} bytes exceeds the {self.MAX_BYTES}-byte message limit",
                )
                continue
            if current and (
                len(current) >= self.MAX_BATCH or current_bytes + size > self.MAX_BYTES
            ):
                chunks.append(current)
                current = []
                current_bytes = 0
            current.append(_Prepared(body, group_id, dedup_id))
            current_bytes += size
        if current:
            chunks.append(current)
        return chunks

    def _send(self, prepared: list[_Prepared]) -> tuple[int, bool]:
        """Sends one valid chunk, retrying only the failed entries within a bounded count.

        Successfully-sent entries are never re-sent, and entries still failing past the bound
        are counted and logged rather than silently dropped (FR-003). Acceptance is matched by
        ``Id`` rather than counted, so a ``Failed`` array carrying a duplicate or an unknown id
        cannot understate the total and turn a partial success into a false "nothing landed".
        The FIFO parameters are attached only on a FIFO queue (SPEC-016 FR-004).

        Entries SQS marks ``SenderFault`` are abandoned rather than retried (SPEC-016 FR-006),
        since a retry re-sends them byte-identical; a missing flag is treated as retryable, so an
        unfamiliar response shape degrades to the old behaviour rather than dropping. Attempts
        are separated by interruptible exponential backoff (SPEC-027 FR-003) — this sink was
        alone in re-sending immediately, while its own docstring named throttling as the
        retryable case, which is exactly what an instant retry makes worse.

        Args:
          prepared: One chunk's serialized, costed entries.

        Returns:
          How many entries were accepted, letting :meth:`emit` tell "nothing landed" from a
          partial success, and whether anything was given up on that a re-send could plausibly
          recover. A chunk SQS rejected wholesale as invalid reports False there.

        Raises:
          Exception: Whatever the client raises.
        """
        entries: list[dict[str, str]] = []
        for i, item in enumerate(prepared):
            entry = {"Id": str(i), "MessageBody": item.body}
            if item.group_id is not None:
                entry["MessageGroupId"] = item.group_id
            if item.dedup_id is not None:
                entry["MessageDeduplicationId"] = item.dedup_id
            entries.append(entry)
        accepted = 0
        for attempt in range(self.max_retries + 1):
            response = self.client.send_message_batch(QueueUrl=self.queue_url, Entries=entries)
            failed = response.get("Failed", [])
            failed_ids = {item.get("Id") for item in failed}
            accepted += sum(1 for entry in entries if entry["Id"] not in failed_ids)
            if not failed:
                return accepted, False
            sender_faults = [item for item in failed if item.get("SenderFault")]
            if sender_faults:
                self.failed += len(sender_faults)
                _diag.lost(
                    "message",
                    len(sender_faults),
                    f"SQSSink, rejected as invalid (first code: "
                    f"{sender_faults[0].get('Code', 'unknown')}); not retried",
                )

            retryable_ids = {item["Id"] for item in failed if not item.get("SenderFault")}
            if not retryable_ids:
                return accepted, False
            entries = [entry for entry in entries if entry["Id"] in retryable_ids]
            if attempt < self.max_retries:
                wait(_BACKOFF_BASE * (2**attempt), self.stop_signal)
            if attempt >= self.max_retries:
                self.failed += len(entries)
                _diag.lost(
                    "message",
                    len(entries),
                    f"SQSSink, still failing after {self.max_retries + 1} attempts; abandoned",
                )
                return accepted, True
        return accepted, False
