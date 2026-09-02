"""SNSSink — publish events to an SNS topic (arch §8, SPEC-010)."""

from __future__ import annotations

import json
import threading
from typing import Any

from log_foundry import _diag
from log_foundry.sinks._chunk import chunk_items
from log_foundry.sinks._retry import wait
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

__all__ = ["SNSSink"]

_BACKOFF_BASE = 0.1


class SNSSink:
    """A :class:`~log_foundry.sinks.base.Sink` that publishes events to an SNS topic.

    This mirrors the SPEC-005 ``SQSSink`` partial-failure policy on SNS's ``publish_batch``:
    the response's ``Failed`` list is retried within a bounded count, and entries still failing
    are counted and logged. ``boto3`` is the optional ``aws`` extra, imported lazily.

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
            having published nothing, and reported success.

        Returns:
          None.

        Raises:
          ImportError: If ``boto3`` is needed and the ``aws`` extra is not installed.
        """
        if client is None:
            import boto3  # type: ignore[import-not-found]

            client = boto3.client("sns")
        self.topic_arn = topic_arn
        self.client = client
        self.max_retries = max(max_retries, 0)
        self.log_foundry_stop_signal: threading.Event | None = None
        self.failed = 0
        self.dropped_oversized = 0
        self._counter_lock = threading.Lock()

    def losses(self) -> SinkLosses:
        """Reports oversized drops and entries still failing past the retry bound (FR-002).

        Args:
          None.

        Returns:
          The counters.

        Raises:
          None.
        """
        with self._counter_lock:
            return SinkLosses(dropped=self.dropped_oversized, failed=self.failed)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Re-chunks to the publish limits and sends each chunk, retrying failures (FR-010).

        Args:
          batch: The events to publish.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When every chunk failed and at least one was sent (SPEC-026
            FR-001). Events dropped before sending, being too large to ever fit, are not a send
            failure and do not make a batch of nothing but oversized events raise — they can
            never be retried into existence, and are reported through :meth:`losses`.
        """
        bodies = self._bodies(batch)
        chunks = delivered = 0
        for chunk in chunk_items(
            bodies, max_count=self.MAX_BATCH, max_bytes=self.MAX_BYTES, size_of=len
        ):
            chunks += 1
            delivered += self._send(chunk)
        if chunks and not delivered:
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

    def _bodies(self, batch: list[dict[str, object]]) -> list[str]:
        """Serializes each event, dropping any message too large to ever fit (FR-011).

        Args:
          batch: The events to serialize.

        Returns:
          The serialized bodies that can be sent.

        Raises:
          None.
        """
        bodies: list[str] = []
        for event in batch:
            body = json.dumps(event)
            if len(body.encode("utf-8")) > self.MAX_BYTES:
                with self._counter_lock:
                    self.dropped_oversized += 1
                _diag.lost("event", 1, f"SNSSink, exceeds the {self.MAX_BYTES}-byte message limit")
                continue
            bodies.append(body)
        return bodies

    def _send(self, bodies: list[str]) -> int:
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

        Args:
          bodies: One chunk's serialized events.

        Returns:
          How many entries SNS accepted, so :meth:`emit` can tell "nothing landed" from a partial
          success. A chunk whose entries all failed contributes zero.

        Raises:
          None. ``Exception`` is caught rather than ``BaseException``, so a ``KeyboardInterrupt``
            or ``SystemExit`` still reaches the caller (SPEC-025 FR-004).
        """
        sent = len(bodies)
        entries = [{"Id": str(i), "Message": body} for i, body in enumerate(bodies)]
        for attempt in range(self.max_retries + 1):
            try:
                response = self.client.publish_batch(
                    TopicArn=self.topic_arn, PublishBatchRequestEntries=entries
                )
            except Exception as err:
                if attempt < self.max_retries:
                    wait(_BACKOFF_BASE * (2**attempt), self.log_foundry_stop_signal)
                    continue
                with self._counter_lock:
                    self.failed += len(entries)
                _diag.lost(
                    "message",
                    len(entries),
                    f"SNSSink, {self.max_retries + 1} attempt(s), {type(err).__name__}",
                )
                return sent - len(entries)
            failed = response.get("Failed", [])
            if not failed:
                return sent
            failed_ids = {entry["Id"] for entry in failed}
            entries = [entry for entry in entries if entry["Id"] in failed_ids]
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
                return sent - len(entries)
        return 0
