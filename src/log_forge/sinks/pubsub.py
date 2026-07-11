"""GooglePubSubSink — publish events to a Google Cloud Pub/Sub topic (arch §8, §9.1, SPEC-010).

A durable-buffer sink on ``google-cloud-pubsub`` (the optional ``gcp-pubsub`` extra, imported
lazily). ``publish()`` returns a future that resolves asynchronously; the sink accumulates the
batch's futures and resolves them on ``close()`` so buffered messages are flushed and publish errors
are counted and logged.
"""

from __future__ import annotations

import json
import sys
from typing import Any

__all__ = ["GooglePubSubSink"]


class GooglePubSubSink:
    """A :class:`~log_forge.sinks.base.Sink` that publishes events to a Pub/Sub topic."""

    def __init__(self, topic: str, *, client: Any = None) -> None:
        if client is None:
            from google.cloud import pubsub_v1  # type: ignore[import-not-found]  # 'gcp-pubsub'

            client = pubsub_v1.PublisherClient()
        self.topic = topic
        self.client = client
        self.failed = 0
        self._futures: list[Any] = []

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Publish one message per event, retaining each future for flush on close (FR-008)."""
        for event in batch:
            future = self.client.publish(self.topic, data=json.dumps(event).encode("utf-8"))
            self._futures.append(future)

    def close(self) -> None:
        """Resolve all pending publish futures, counting/logging errors; idempotent (FR-008)."""
        for future in self._futures:
            try:
                future.result()
            except Exception as err:  # isolation boundary: never crash the worker (FR-011)
                self.failed += 1
                sys.stderr.write(f"log-forge: GooglePubSubSink publish failed: {err!r}\n")
        self._futures.clear()
