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

    The driver requirement satisfied (SPEC-028 FR-002): this sink takes **no** transport lock —
    the publisher client owns its own batching and threading, and ``publish()`` is a local
    hand-off. What it does hold is the pending-futures list, which is genuinely shared between
    ``emit`` (appending) and ``close`` (resolving), so that list has its own small lock and
    ``close`` swaps it out rather than iterating and clearing. Without the swap, a future
    appended after the loop passed its index was dropped unresolved: an unconfirmed publish
    never counted in ``failed`` and never reported by :meth:`losses`, which is precisely the
    silent loss SPEC-026 exists to end.

    That same loss was reachable from *outside* ``close`` until SPEC-032 FR-001: nothing stopped
    a later ``emit`` appending to the fresh list, and nothing would ever call ``result()`` on
    it. The sink now refuses a batch once closed, and the append re-checks under the futures
    lock, so a publish cannot land on a list the swap has already taken.
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
        self._futures_lock = threading.Lock()
        self._futures: list[Any] = []
        self._closed = False

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

        A closed sink refuses the batch before touching the client (SPEC-032 FR-001), because
        nothing will resolve a future appended after :meth:`close` swapped the list out.
        Refusing moves no counter here: it is a failure reported to the worker, which records it
        in ``health().failed_batches``, not loss this sink absorbed. The flag is read twice on
        purpose — once for the batch, and again under ``_futures_lock`` for each append, which
        is the lock ``close`` swaps under. Without the second read a close landing mid-loop
        would leave exactly the orphaned future this guard exists to prevent, and the read costs
        nothing because the append already takes that lock.

        A close that lands mid-batch does **not** raise, even when it catches every event. Each
        one had ``publish()`` called on it and may well land, so raising would have the worker
        re-send them and duplicate whatever did — the SPEC-018 rule that only a *provable*
        non-delivery may be retried. They are counted as unconfirmed instead, which is exactly
        what they are. That is why the total-failure raise below tests the refusals rather than
        the successes: "nothing was published" and "nothing was confirmed" are different claims,
        and only the first is safe to retry.

        Args:
          batch: The events to publish.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When the sink was already closed on entry, or when every event was
            refused (SPEC-026 FR-001).
        """
        if not batch:
            return
        if self._closed:
            raise SinkDeliveryError(
                f"GooglePubSubSink published none of {len(batch)} event(s): the sink is closed"
            )
        refused = 0
        for event in batch:
            try:
                future = self.client.publish(self.topic, data=json.dumps(event).encode("utf-8"))
            except Exception as err:
                refused += 1
                with self._counter_lock:
                    self.rejected += 1
                _diag.lost("event", 1, f"GooglePubSubSink refused the publish, {type(err).__name__}")
                continue
            with self._futures_lock:
                if self._closed:
                    with self._counter_lock:
                        self.failed += 1
                    _diag.lost(
                        "event", 1, "GooglePubSubSink publish unconfirmed, the sink closed mid-batch"
                    )
                    continue
                self._futures.append(future)
        if refused == len(batch):
            raise SinkDeliveryError(
                f"GooglePubSubSink published none of {len(batch)} event(s)"
            )

    def close(self) -> None:
        """Resolves all pending publish futures, counting and logging errors (FR-008).

        Idempotent. The pending list is swapped out under a lock rather than iterated and then
        cleared (SPEC-028 FR-002): ``emit`` appends to it from any thread, so the old
        iterate-then-``clear()`` discarded any future appended after the loop passed its index —
        an unconfirmed publish whose ``result()`` was never called, never counted in ``failed``
        and never reported by ``losses()``. That is the silent loss SPEC-026 exists to end,
        reached through the one piece of shared state this sink has.

        The closed flag is set in the same critical section as the swap (SPEC-032 FR-001), which
        is what makes :meth:`emit`'s second check exact: an append winning the lock before this
        point is resolved by the loop below, and one arriving after sees the flag. The futures
        are resolved outside the lock, so a slow ``result()`` never blocks an emit that is about
        to be refused anyway.

        Args:
          None.

        Returns:
          None.

        Raises:
          None. This is an isolation boundary: an unresolved future must never crash the worker
            (FR-011).
        """
        with self._futures_lock:
            self._closed = True
            pending, self._futures = self._futures, []
        for future in pending:
            try:
                future.result()
            except Exception as err:
                with self._counter_lock:
                    self.failed += 1
                _diag.lost(
                    "event", 1, f"GooglePubSubSink publish unconfirmed, {type(err).__name__}"
                )
