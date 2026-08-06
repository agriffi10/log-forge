"""NATSSink — publish events to a NATS subject, optionally via JetStream (arch §8, SPEC-010)."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from log_foundry import _diag
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

__all__ = ["NATSSink"]


class NATSSink:
    """A :class:`~log_foundry.sinks.base.Sink` that publishes events to a NATS subject.

    This is a durable-buffer sink on ``nats-py``, the optional ``nats`` extra, imported lazily.
    The driver is async, so the sink owns a private event loop and drives each publish to
    completion from the synchronous ``emit``, which therefore returns only after the batch is
    handed off. With JetStream enabled it publishes through JetStream for durable
    acknowledgement.
    """

    def __init__(
        self,
        subject: str,
        *,
        client: Any = None,
        jetstream: bool = False,
        servers: str | None = None,
    ) -> None:
        """Binds the sink to a subject and connects if no client was injected.

        Args:
          subject: The subject to publish to.
          client: A ``nats-py``-shaped client to borrow, or ``None`` to connect one.
          jetstream: Whether to publish through JetStream.
          servers: The server URL used when connecting.

        Returns:
          None.

        Raises:
          ImportError: If the ``nats`` extra is not installed.
          Exception: Whatever the driver raises when connecting.
        """
        self._subject = subject
        self._jetstream = jetstream
        self._loop = asyncio.new_event_loop()
        self.failed = 0
        if client is None:
            import nats  # type: ignore[import-not-found]

            client = self._loop.run_until_complete(
                nats.connect(servers or "nats://localhost:4222")
            )
        self._client = client

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Drives the async publishes to completion on the managed loop (FR-007).

        Args:
          batch: The events to publish. An empty batch is a no-op.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When none of the events landed.
        """
        if not batch:
            return
        self._loop.run_until_complete(self._publish_all(batch))

    def losses(self) -> SinkLosses:
        """Reports events whose publish raised (SPEC-026 FR-002).

        Args:
          None.

        Returns:
          The counters.

        Raises:
          None.
        """
        return SinkLosses(dropped=0, failed=self.failed)

    def close(self) -> None:
        """Drains and closes the connection, then closes the managed loop (FR-007).

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever draining raises; the loop is closed regardless.
        """
        if self._loop.is_closed():
            return
        try:
            self._loop.run_until_complete(self._drain())
        finally:
            self._loop.close()

    async def _publish_all(self, batch: list[dict[str, object]]) -> None:
        """Publishes each event, isolating a per-event failure.

        Per-event isolation stays because a partial batch must not be retried wholesale: the
        events that published would be delivered twice.

        Args:
          batch: The events to publish.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When none of them landed (SPEC-026 FR-001).
        """
        target = self._client.jetstream() if self._jetstream else self._client
        published = 0
        for event in batch:
            try:
                await target.publish(self._subject, json.dumps(event).encode("utf-8"))
            except Exception as err:
                self.failed += 1
                _diag.lost("event", 1, f"NATSSink publish, {type(err).__name__}")
            else:
                published += 1
        if batch and not published:
            raise SinkDeliveryError(f"NATSSink published none of {len(batch)} event(s)")

    async def _drain(self) -> None:
        """Drains the client if the driver offers a drain.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the driver raises while draining.
        """
        drain = getattr(self._client, "drain", None)
        if drain is not None:
            await drain()
