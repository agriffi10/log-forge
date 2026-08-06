"""SQSSink — ship batches to an SQS queue (arch §8, §9.1, guide Phase 10).

SQS is the headline production path: a durable buffer that decouples the app from ELK
availability — events accumulate safely in the queue during downstream spikes/outages instead
of being lost or back-pressuring the app. A separate consumer indexes them into ELK (out of
scope here). Like every sink this receives *already-built* event dicts and knows nothing about
spans.

``boto3`` is an **optional** dependency (the ``aws`` extra): it is imported lazily inside the
sink, never at module top, so ``import log_foundry.sinks.sqs`` — and the whole library — stays
dependency-free unless an ``SQSSink`` is actually instantiated without an injected client.

The worker (SPEC-004) batches by count and time, but SQS has hard per-request limits, so this
sink re-chunks every incoming batch on both dimensions: ≤ 10 messages **and** ≤ 256 KB per
``send_message_batch``. Partial failures (the response ``Failed`` list) are retried; a single
event too large to ever fit is dropped with a warning rather than crashing the batch.

Both queue types are supported (SPEC-016). A ``.fifo`` queue URL selects FIFO behaviour, where
every entry additionally carries a ``MessageGroupId`` — SQS orders messages *within* a group, and
the default group is the event's own ``trace_id``, which is exactly the unit whose events should
stay ordered. Per-trace groups also keep traces independent, so the queue delivers them in
parallel instead of serializing the whole process behind one group. Standard queues are
untouched: their entries carry ``Id`` and ``MessageBody`` and nothing else.

Ordering is best-effort across a retry boundary: if one entry fails while a same-group entry
ahead of it succeeded, the retried entry lands *after* it. Holding a whole group back on one
failure would trade log delivery for ordering the consumer can rebuild from ``timestamp``, so it
is not done.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any, NamedTuple

from log_foundry import _diag

__all__ = ["SQSSink"]

DEFAULT_GROUP_ID = "log-foundry"
"""Fallback group for an event carrying no usable ``trace_id`` — never send an empty group id."""

MAX_ID_LEN = 128
"""SQS maximum length for ``MessageGroupId`` and ``MessageDeduplicationId``."""

GroupIdSource = str | Callable[[dict[str, object]], str] | None
DedupIdSource = Callable[[dict[str, object]], str] | None


class _Prepared(NamedTuple):
    """One event serialized and costed, with its FIFO ids resolved (``None`` on a standard queue).

    The ids are derived **once**, here, rather than again at send time: the deduplication
    fallback mints a fresh UUID, so deriving twice would bill the byte budget for one value and
    put a different one on the wire.
    """

    body: str
    group_id: str | None
    dedup_id: str | None


def _bounded(raw: str, fallback: str) -> str:
    """Normalize a derived id: blank falls back, over-long truncates to the SQS maximum."""
    cleaned = raw.strip()
    if not cleaned:
        return fallback
    return cleaned[:MAX_ID_LEN]


class SQSSink:
    """A :class:`~log_foundry.sinks.base.Sink` that sends events to an SQS queue."""

    MAX_BATCH = 10  # SQS SendMessageBatch hard limit: entries per request
    MAX_BYTES = 256 * 1024  # SQS limit: 256 KB per request

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
        if isinstance(message_group_id, str) and not message_group_id.strip():
            raise ValueError(
                "message_group_id must be a non-empty string; pass None to group by trace_id"
            )
        if client is None:
            import boto3  # type: ignore[import-not-found]  # optional 'aws' extra

            client = boto3.client("sqs")
        self.queue_url = queue_url
        self.client = client
        self.max_retries = max_retries
        # AWS requires every FIFO queue name to end in '.fifo', so the suffix is a contract
        # rather than a guess — but an explicit flag still wins. Decided once, not per emit.
        self.fifo = queue_url.endswith(".fifo") if fifo is None else fifo
        self.message_group_id = message_group_id
        self.message_deduplication_id = message_deduplication_id
        self.dropped_oversized = 0  # events too large to ever fit one message
        self.failed = 0  # entries still failing after the retry bound

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Re-chunk ``batch`` to SQS limits and send each chunk (FR-001, FR-002)."""
        for chunk in self._chunks(batch):
            self._send(chunk)

    def close(self) -> None:
        """No-op: the sink buffers nothing internally (FR-005)."""

    # -- internals ----------------------------------------------------------------------

    def _group_id(self, event: dict[str, object]) -> str:
        """Resolve one event's ``MessageGroupId`` (SPEC-016 FR-002).

        A constant is used as given; a callable is asked; otherwise the event's own
        ``trace_id`` groups it. Every route is bounded by :func:`_bounded`, so a caller's
        callable returning ``""`` cannot put an empty parameter on the wire.
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
        """Resolve one event's ``MessageDeduplicationId`` (SPEC-016 FR-003).

        Defaults to the event's ``log_id`` — already a per-event UUID, so SQS's five-minute
        deduplication window can never collapse two genuinely distinct records. The fallback
        mints a fresh id for the same reason: a shared constant would make unrelated events
        deduplicate each other.
        """
        source = self.message_deduplication_id
        raw = source(event) if source is not None else str(event.get("log_id", ""))
        if raw.strip():
            return _bounded(raw, "")
        from log_foundry.ids import new_log_id

        return new_log_id()

    def _chunks(self, batch: list[dict[str, object]]) -> list[list[_Prepared]]:
        """Split ``batch`` into sends of ≤ ``MAX_BATCH`` entries and ≤ ``MAX_BYTES`` each.

        Each event is serialized once with ``json.dumps``, and on a FIFO queue its ids are
        resolved once here too (SPEC-016 FR-005) — they travel in the same request, so both the
        running budget and the single-entry check below cost them alongside the body.

        An entry whose costed size exceeds ``MAX_BYTES`` can never fit a message, so it is
        dropped with a counted warning (FR-004) instead of stalling the batch. The check is made
        on everything that travels, not the body alone: a body just under the limit plus up to
        256 bytes of FIFO ids would otherwise pass, then ship as a lone over-budget request that
        SQS rejects as a sender fault — which FR-006 (rightly) never retries, losing the event
        as an opaque failure instead of a labelled oversize drop.
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

    def _send(self, prepared: list[_Prepared]) -> None:
        """Send one valid chunk, retrying only the ``Failed`` entries with a bounded count.

        Successfully-sent entries are never re-sent; entries still failing past ``max_retries``
        are counted (``failed``) and logged, not silently dropped (FR-003). The FIFO parameters
        are attached only when the queue is FIFO, so a standard queue's entries are exactly
        ``Id`` + ``MessageBody`` (SPEC-016 FR-004).

        Entries SQS marks ``SenderFault`` are abandoned rather than retried (SPEC-016 FR-006):
        a retry re-sends them byte-identical, so a fault in the request itself can only fail
        the same way. Throttles and internal errors carry ``SenderFault: false`` and are still
        retried under the bound.
        """
        entries: list[dict[str, str]] = []
        for i, item in enumerate(prepared):
            entry = {"Id": str(i), "MessageBody": item.body}
            if item.group_id is not None:
                entry["MessageGroupId"] = item.group_id
            if item.dedup_id is not None:
                entry["MessageDeduplicationId"] = item.dedup_id
            entries.append(entry)
        for attempt in range(self.max_retries + 1):
            response = self.client.send_message_batch(QueueUrl=self.queue_url, Entries=entries)
            failed = response.get("Failed", [])
            if not failed:
                return
            # Abandon sender faults immediately and name the code: the retry would re-send the
            # entry byte-identical, and the code is the only thing that makes a rejection
            # diagnosable from the log line alone. A missing flag is treated as retryable, so
            # an unfamiliar response shape degrades to the old behaviour rather than dropping.
            sender_faults = [item for item in failed if item.get("SenderFault")]
            if sender_faults:
                self.failed += len(sender_faults)
                # The AWS error code is library-controlled in the sense that matters — it is an
                # enumerated API constant, not the event — and ``_diag`` bounds and escapes it
                # regardless, so a surprising response shape cannot forge a line (SPEC-029).
                _diag.lost(
                    "message",
                    len(sender_faults),
                    f"SQSSink, rejected as invalid (first code: "
                    f"{sender_faults[0].get('Code', 'unknown')}); not retried",
                )

            retryable_ids = {item["Id"] for item in failed if not item.get("SenderFault")}
            if not retryable_ids:
                return
            entries = [entry for entry in entries if entry["Id"] in retryable_ids]
            if attempt >= self.max_retries:
                self.failed += len(entries)
                _diag.lost(
                    "message",
                    len(entries),
                    f"SQSSink, still failing after {self.max_retries + 1} attempts; abandoned",
                )
                return
