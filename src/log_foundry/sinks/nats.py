"""NATSSink — publish events to a NATS subject, optionally via JetStream (arch §8, §9.1, SPEC-010).

A durable-buffer sink on ``nats-py`` (the optional ``nats`` extra, imported lazily). ``nats-py`` is
async, so the sink owns a private event loop and drives each publish to completion from the
synchronous ``emit`` (which therefore returns only after the batch is handed off). With
``jetstream=True`` it publishes through JetStream for durable acknowledgement. ``close()`` drains and
closes the connection.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from log_foundry import _diag

__all__ = ["NATSSink"]


class NATSSink:
    """A :class:`~log_foundry.sinks.base.Sink` that publishes events to a NATS subject."""

    def __init__(
        self,
        subject: str,
        *,
        client: Any = None,
        jetstream: bool = False,
        servers: str | None = None,
    ) -> None:
        self._subject = subject
        self._jetstream = jetstream
        self._loop = asyncio.new_event_loop()
        self.failed = 0
        if client is None:
            import nats  # type: ignore[import-not-found]  # optional 'nats' extra

            client = self._loop.run_until_complete(
                nats.connect(servers or "nats://localhost:4222")
            )
        self._client = client

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Drive the async publishes to completion on the managed loop (FR-007)."""
        if not batch:
            return
        self._loop.run_until_complete(self._publish_all(batch))

    def close(self) -> None:
        """Drain/flush and close the connection, then close the managed loop (FR-007)."""
        if self._loop.is_closed():
            return
        try:
            self._loop.run_until_complete(self._drain())
        finally:
            self._loop.close()

    # -- internals ----------------------------------------------------------------------

    async def _publish_all(self, batch: list[dict[str, object]]) -> None:
        target = self._client.jetstream() if self._jetstream else self._client
        for event in batch:
            try:
                await target.publish(self._subject, json.dumps(event).encode("utf-8"))
            except Exception as err:  # isolation boundary: never crash the worker (FR-011)
                self.failed += 1
                _diag.lost("event", 1, f"NATSSink publish, {type(err).__name__}")

    async def _drain(self) -> None:
        drain = getattr(self._client, "drain", None)
        if drain is not None:
            await drain()
