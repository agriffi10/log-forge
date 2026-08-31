"""NATSSink — publish events to a NATS subject, optionally via JetStream (arch §8, SPEC-010)."""

from __future__ import annotations

import asyncio
import json
import threading
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

    **Retry and the worst-case delay** (SPEC-041 FR-004). This sink adds no retry loop and needs
    none: a core ``publish()`` writes into the client's outbound buffer and returns without
    waiting — measured at 0.00 s for fifty publishes — so it never holds the worker's single
    drain thread and there is no backoff for a shutdown to cut short. SPEC-027's guarantee is met
    because there is no wait, not because a wait is bounded. Under JetStream ``publish()`` awaits
    an ack bounded by the driver's own timeout (5 s by default) and does not retry.

    **A disconnected client is reported, not absorbed** (FR-004 AC-5). That non-blocking publish
    is exactly what made this sink report success for events that had not left the process: with
    the server stopped, five successive emits each returned in 0.00 s with ``losses()`` reading
    all zeros, and when the client's reconnect budget (60 attempts × 2 s by default) ran out
    first, **one of six events reached the destination with every counter still at zero**. That
    is SPEC-026 FR-001's shape — a sink the worker believes, so its retry never engages and
    ``failed_batches`` never moves. ``emit`` now refuses a batch while the client reports itself
    disconnected.

    The limit is stated rather than overclaimed: ``is_connected`` does not flip the instant the
    server dies, so the first batch in the window before the client notices is still accepted and
    still buffered — measured, one of five emits landed in that window. It is the same
    check-then-act window ``MongoDBSink`` and ``KafkaSink`` already document for their own flags;
    what the guard ends is the far larger case of an outage that has been going on for any
    appreciable time. Refusing moves no ``losses()`` counter, per SPEC-032: it is a failure
    *reported* to the worker rather than one absorbed, and counting both would report one loss
    twice.
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
        self._counter_lock = threading.Lock()
        self._lock = threading.Lock()
        if client is None:
            import nats  # type: ignore[import-not-found]

            client = self._loop.run_until_complete(
                nats.connect(servers or "nats://localhost:4222")
            )
        self._client = client

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Drives the async publishes to completion on the managed loop (FR-007).

        The driver requirement satisfied (SPEC-028 FR-002): an ``asyncio`` event loop is
        single-entry. A second thread calling ``run_until_complete`` on a loop that is already
        running raises ``RuntimeError``, and can leave the loop's task machinery in a state where
        a thread never returns from ``emit`` at all — measured as a permanently hung application
        thread on the orphan path, which is the one outcome this library must never produce. The
        lock makes the loop what the sink already assumed it was: entered by one caller at a time.

        Args:
          batch: The events to publish. An empty batch is a no-op.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When none of the events landed.
        """
        if not batch:
            return
        with self._lock:
            if self._loop.is_closed():
                raise SinkDeliveryError(
                    f"NATSSink published none of {len(batch)} event(s): the sink is closed"
                )
            if not self._is_connected():
                raise SinkDeliveryError(
                    f"NATSSink published none of {len(batch)} event(s): "
                    "the client is disconnected"
                )
            self._loop.run_until_complete(self._publish_all(batch))

    def _is_connected(self) -> bool:
        """Reports whether the client can currently put anything on the wire (FR-004 AC-5).

        Probed by name, as ``drain`` and ``flush`` are, because this sink is written against a
        driver it does not own and accepts an injected ``client=``. A client that does not
        publish the attribute is assumed connected: the guard exists to convert a *known*
        non-delivery into a reported one, and inventing a refusal for a client that never
        claimed to be disconnected would fail batches that were going to succeed.

        Args:
          None.

        Returns:
          True when the client reports itself connected, or says nothing about it.

        Raises:
          None. A driver whose attribute access raises is treated as connected, so a probe can
            never be the reason a batch fails.
        """
        try:
            connected = getattr(self._client, "is_connected", True)
        except Exception:
            return True
        return bool(connected)

    def losses(self) -> SinkLosses:
        """Reports events whose publish raised (SPEC-026 FR-002).

        Args:
          None.

        Returns:
          The counters.

        Raises:
          None.
        """
        with self._counter_lock:
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
        with self._lock:
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
                with self._counter_lock:
                    self.failed += 1
                _diag.lost("event", 1, f"NATSSink publish, {type(err).__name__}")
            else:
                published += 1
        if batch and not published:
            raise SinkDeliveryError(f"NATSSink published none of {len(batch)} event(s)")

    def flush(self) -> None:
        """Pushes the client's outbound buffer onto the wire without closing (SPEC-036 FR-002).

        Core ``publish()`` writes into the client's own outbound buffer and returns; the network
        write happens on the driver's flusher task. That is why :meth:`close` drains, and why
        ``log_foundry.flush()`` could not reach a published-but-unwritten event before this hook.
        Under JetStream ``publish()`` awaits an ack, so there is nothing pending and this costs a
        round trip at worst.

        Takes the same lock :meth:`emit` does, for the reason recorded there: an ``asyncio`` loop
        is single-entry, and a second thread calling ``run_until_complete`` on a running loop can
        leave a thread never returning at all.

        Args:
          None.

        Returns:
          None.

        Raises:
          SinkDeliveryError: The sink is closed.
          Exception: Whatever the driver raises while flushing.
        """
        with self._lock:
            if self._loop.is_closed():
                raise SinkDeliveryError("NATSSink cannot flush: the sink is closed")
            self._loop.run_until_complete(self._flush_client())

    async def _flush_client(self) -> None:
        """Flushes the client if the driver offers one, mirroring :meth:`_drain`'s probe.

        Probed by name for the same reason ``drain`` is: the sink is written against a driver it
        does not own, and a client without the method has nothing buffered to push.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the driver raises while flushing.
        """
        flush = getattr(self._client, "flush", None)
        if flush is not None:
            await flush()

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
