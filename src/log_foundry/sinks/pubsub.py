"""GooglePubSubSink — publish events to a Google Cloud Pub/Sub topic (arch §8, SPEC-010)."""

from __future__ import annotations

import json
import threading
from typing import Any

from log_foundry import _diag
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

__all__ = ["GooglePubSubSink"]

DEFAULT_MAX_PENDING = 1000
"""Outstanding publish futures one sink will hold before it waits (FR-004 AC-2).

Roughly one drain interval's worth at the default batch size: large enough that ordinary
publishing never serializes on it, and small enough to bound memory at a constant rather than at
the number of events the process has ever logged.
"""


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

    The list was also append-only for the life of the process until SPEC-038 FR-004: memory grew
    with the total number of events logged, and because ``result()`` ran only at ``close``,
    ``failed`` stayed at zero and ``health()`` reported clean through an entire Pub/Sub outage.
    Each ``emit`` now reaps whatever has settled and holds at most ``max_pending`` outstanding.

    Attributes:
      max_pending: Outstanding futures held before ``emit`` waits on the oldest.
      failed: Publishes the client did not confirm.
      rejected: Publishes the client refused outright, before any future existed.
    """

    def __init__(self, topic: str, *, client: Any = None, max_pending: int | None = None) -> None:
        """Binds the sink to a topic.

        Args:
          topic: The topic to publish to.
          client: A publisher client to borrow, or ``None`` to build one.
          max_pending: Outstanding futures held before :meth:`emit` waits on the oldest, or
            ``None`` for :data:`DEFAULT_MAX_PENDING`. Floored at one, since a bound of zero would
            make every publish synchronous and defeat the client's own batching.

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
        self.max_pending = max(max_pending if max_pending is not None else DEFAULT_MAX_PENDING, 1)
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
          The counters. ``failed`` moves as futures settle, which :meth:`emit` reaps on every
          call (FR-004), so an outage is visible while it is happening rather than only at
          shutdown. It still lags the publish that caused it by however long the client takes to
          give up, which is a property of the client's asynchronous publish and not of this
          accessor.

        Raises:
          None.
        """
        with self._counter_lock:
            return SinkLosses(dropped=self.rejected, failed=self.failed)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Publishes one message per event, then reaps the futures that have settled (FR-008).

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
        nothing because the append already takes that lock. Only the read happens under it: the
        counter bump and the stderr line are taken *after* the lock is released, because
        ``close`` waits on the same lock and a blocked stderr would otherwise hold it — an I/O
        call inside a transport lock is what SPEC-028's two-lock decision exists to avoid.

        A close that lands mid-batch does **not** raise, even when it catches every event. Each
        one had ``publish()`` called on it and may well land, so raising would have the worker
        re-send them and duplicate whatever did — the SPEC-018 rule that only a *provable*
        non-delivery may be retried. They are counted as unconfirmed instead, which is exactly
        what they are. That is why the total-failure raise below tests the refusals rather than
        the successes: "nothing was published" and "nothing was confirmed" are different claims,
        and only the first is safe to retry.

        The reap runs *before* the total-failure test, so a batch the client refused outright
        still returns the pending list to its bound on the way out.

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
                orphaned = self._closed
                if not orphaned:
                    self._futures.append(future)
            if orphaned:
                with self._counter_lock:
                    self.failed += 1
                _diag.lost(
                    "event", 1, "GooglePubSubSink publish unconfirmed, the sink closed mid-batch"
                )
        self._reap()
        if refused == len(batch):
            raise SinkDeliveryError(
                f"GooglePubSubSink published none of {len(batch)} event(s)"
            )

    def _reap(self) -> None:
        """Resolves the futures that have settled, and waits on the oldest if over the bound.

        This is what makes an outage visible while it is happening (FR-004 AC-1) and what stops
        the pending list growing with the total number of events the process has ever logged
        (AC-4). Before it, ``result()`` was called only from :meth:`close`, so ``failed`` stayed
        at zero and ``health()`` read clean through an entire Pub/Sub outage.

        Futures are selected under the futures lock and resolved *outside* it, as :meth:`close`
        does: a settled future's ``result()`` is immediate, but an overflow future's is a wait,
        and holding the lock across it would block every concurrent publish behind it — the I/O
        inside a transport lock that SPEC-028's two-lock arrangement exists to avoid.

        Overflow is taken from the front, so the oldest publish is the one waited on. Waiting
        rather than discarding is deliberate (AC-2): the event has already been handed to the
        client and may yet be delivered, so dropping it here would invent a loss that the
        destination has not committed.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        with self._futures_lock:
            settled = [future for future in self._futures if _has_settled(future)]
            outstanding = [future for future in self._futures if not _has_settled(future)]
            excess = max(len(outstanding) - self.max_pending, 0)
            overflow, self._futures = outstanding[:excess], outstanding[excess:]
        for future in (*settled, *overflow):
            self._resolve(future)

    def _resolve(self, future: Any) -> None:
        """Waits for one publish to settle, counting and announcing a failure.

        Args:
          future: The publish future to resolve.

        Returns:
          None.

        Raises:
          None. An unresolved future must never crash the worker (FR-011), and this is called
            from ``close`` as well as from the emitting thread.
        """
        try:
            future.result()
        except Exception as err:
            with self._counter_lock:
                self.failed += 1
            _diag.lost("event", 1, f"GooglePubSubSink publish unconfirmed, {type(err).__name__}")

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
            self._resolve(future)


def _has_settled(future: Any) -> bool:
    """Reports whether a publish future has already resolved, without ever waiting on it.

    Total, and the two "cannot tell" cases are answered differently on purpose. A future with no
    ``done()`` at all is reported **outstanding**: it cannot be polled, but it is still held
    under ``max_pending``, so it is bounded without every publish being made synchronous — which
    is what reporting it settled would do, since the reap would then call ``result()`` on it
    immediately. A ``done()`` that *raises* is reported settled instead, because a future whose
    own state query fails is broken, and resolving it now counts it rather than leaving it to
    occupy the bound forever.

    Args:
      future: The publish future to inspect.

    Returns:
      True when the future has resolved, or when its state query failed.

    Raises:
      None.
    """
    done = getattr(future, "done", None)
    if not callable(done):
        return False
    try:
        return bool(done())
    except Exception:
        return True
