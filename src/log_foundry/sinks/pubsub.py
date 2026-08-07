"""GooglePubSubSink — publish events to a Google Cloud Pub/Sub topic (arch §8, SPEC-010)."""

from __future__ import annotations

import json
import threading
from typing import Any

from log_foundry import _diag
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

__all__ = ["GooglePubSubSink"]


class GooglePubSubSink:
    """A :class:`~log_foundry.sinks.base.Sink` that publishes events to a Pub/Sub topic.

    This is a durable-buffer sink on ``google-cloud-pubsub``, the optional ``gcp-pubsub`` extra,
    imported lazily. ``publish()`` returns a future that resolves asynchronously, so the sink
    accumulates the batch's futures and resolves them on :meth:`close`.
    """

    def __init__(self, topic: str, *, client: Any = None) -> None:
        """Binds the sink to a topic.

        Args:
          topic: The topic to publish to.
          client: A publisher client to borrow, or ``None`` to build one.

        Returns:
          None.

        Raises:
          ImportError: If the ``gcp-pubsub`` extra is not installed.
        """
        if client is None:
            from google.cloud import pubsub_v1  # type: ignore[import-not-found]

            client = pubsub_v1.PublisherClient()
        self.topic = topic
        self.client = client
        self.failed = 0
        self.rejected = 0
        self._counter_lock = threading.Lock()
        self._futures: list[Any] = []

    def losses(self) -> SinkLosses:
        """Reports refused publishes and futures that resolved to an error (FR-002).

        Args:
          None.

        Returns:
          The counters. ``failed`` only moves when the futures are resolved, which happens in
          :meth:`close`, so a long-lived process reads zero there until it shuts down — a
          property of the client's asynchronous publish, not of this accessor.

        Raises:
          None.
        """
        with self._counter_lock:
            return SinkLosses(dropped=self.rejected, failed=self.failed)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Publishes one message per event, retaining each future for flush on close (FR-008).

        ``publish()`` is a local hand-off returning a future, so a refusal is the only failure
        this call can observe. It is isolated per event, because letting the first refusal
        propagate would hand the worker a batch whose earlier events are already in flight, and
        the retry would duplicate them. The stderr line names which counter moved, since
        "refused" and "unconfirmed" mean different things.

        Args:
          batch: The events to publish.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When every event was refused (SPEC-026 FR-001).
        """
        published = 0
        for event in batch:
            try:
                future = self.client.publish(self.topic, data=json.dumps(event).encode("utf-8"))
            except Exception as err:
                with self._counter_lock:
                    self.rejected += 1
                _diag.lost("event", 1, f"GooglePubSubSink refused the publish, {type(err).__name__}")
                continue
            self._futures.append(future)
            published += 1
        if batch and not published:
            raise SinkDeliveryError(
                f"GooglePubSubSink published none of {len(batch)} event(s)"
            )

    def close(self) -> None:
        """Resolves all pending publish futures, counting and logging errors (FR-008).

        Idempotent.

        Args:
          None.

        Returns:
          None.

        Raises:
          None. This is an isolation boundary: an unresolved future must never crash the worker
            (FR-011).
        """
        for future in self._futures:
            try:
                future.result()
            except Exception as err:
                with self._counter_lock:
                    self.failed += 1
                _diag.lost(
                    "event", 1, f"GooglePubSubSink publish unconfirmed, {type(err).__name__}"
                )
        self._futures.clear()
