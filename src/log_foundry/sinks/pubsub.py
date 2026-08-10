"""GooglePubSubSink — publish events to a Google Cloud Pub/Sub topic (arch §8, SPEC-010)."""

from __future__ import annotations

import json
import threading
import time
from typing import Any

from log_foundry import _diag
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

__all__ = ["GooglePubSubSink"]

DEFAULT_OVERFLOW_TIMEOUT = 30.0
"""Seconds one ``emit`` waits on an over-bound publish before putting it back (FR-004 AC-2).

Thirty matches ``DEFAULT_MAX_RETRY_AFTER``: it is long enough that an ordinary publish settles
inside it, and short enough that the single drain thread is never held past what
``shutdown()``'s own budget allows. It bounds the **whole** overflow pass of one ``emit``, not
each future in it — per-future it would be thirty seconds times however many futures are over
the limit, which is not a bound at all.
"""

_POLL_INTERVAL = 0.05
"""Longest single wait on one future, so a shutdown is noticed within it (SPEC-027).

The deadline bounds the *total*, but a client honouring a 30-second timeout on one future
blocks for all thirty regardless — measured, a shutdown 0.05 s into an emit went unnoticed for
10 s. Waiting in slices makes the stop signal effective within one slice, which is the
"interruptible" half of SPEC-027's rule; the deadline is the "bounded" half.
"""

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
    Each ``emit`` now reaps whatever has settled and holds at most ``max_pending`` outstanding
    **between emits, while the client is resolving**. Two documented exceptions: the reap runs
    once at the end of a call, so one ``emit`` of 6,000 events peaks at 5,999 futures before it
    trims; and against a client resolving nothing at all the list grows by a batch per emit,
    because :meth:`_await_overflow` puts back what its bounded wait did not outlast rather than
    discarding it. What the bound rules out is growth with the total number of events a *healthy*
    process has logged, which is what FR-004 is about.

    Attributes:
      max_pending: Outstanding futures held between emits before ``emit`` waits on the oldest.
      overflow_timeout: Seconds one over-bound publish is waited on before being put back.
      failed: Publishes the client did not confirm.
      rejected: Publishes the client refused outright, before any future existed.
    """

    def __init__(
        self,
        topic: str,
        *,
        client: Any = None,
        max_pending: int | None = None,
        overflow_timeout: float = DEFAULT_OVERFLOW_TIMEOUT,
    ) -> None:
        """Binds the sink to a topic.

        Args:
          topic: The topic to publish to.
          client: A publisher client to borrow, or ``None`` to build one.
          max_pending: Outstanding futures held before :meth:`emit` waits on the oldest, or
            ``None`` for :data:`DEFAULT_MAX_PENDING`. Floored at one, since a bound of zero would
            make every publish synchronous and defeat the client's own batching.
          overflow_timeout: Seconds to wait on one over-bound publish before putting it back.
            The wait runs on the worker's single drain thread, so it is bounded by SPEC-027's
            rule that no sink wait may be unbounded or uninterruptible.

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
        self.overflow_timeout = overflow_timeout
        self.log_foundry_stop_signal: threading.Event | None = None
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

        The partition is **one pass**, and that is load-bearing rather than tidy. Two
        comprehensions query ``done()`` twice per future, and a future that settles between the
        two queries — which is what a client's commit thread does — lands in neither list: not
        resolved, and dropped by the reassignment below. Measured, five failed publishes vanished
        with ``losses()`` reading clean, and under a real thread race 41% of failures went
        uncounted. That is the silent loss this method exists to end, so the scan may not ask the
        same future twice.

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
            settled: list[Any] = []
            outstanding: list[Any] = []
            for future in self._futures:
                (settled if _has_settled(future) else outstanding).append(future)
            excess = max(len(outstanding) - self.max_pending, 0)
            overflow, self._futures = outstanding[:excess], outstanding[excess:]
        for future in settled:
            self._resolve(future)
        self._await_overflow(overflow)

    def _await_overflow(self, overflow: list[Any]) -> None:
        """Waits on the oldest futures once the pending list is over its bound (FR-004 AC-2).

        The wait is **bounded and interruptible**, per SPEC-027. This runs inside ``emit`` on the
        worker's single drain thread, so an unbounded ``result()`` here is a pause on all log
        delivery that ``shutdown()`` cannot cut short. Before FR-004 the only blocking
        ``result()`` was in ``close``, where such a wait belongs.

        **One deadline covers the whole list, and the stop signal is re-read each time round.**
        A per-*future* timeout is not a bound: with the shipped 30 s and ten futures over the
        limit, one ``emit`` blocked for five minutes, and a shutdown landing mid-loop was not
        noticed until every future had been waited on — measured at 5.04 s for a signal set after
        0.3 s. Both were regressions against a version of this sink that never blocked at all.

        A future the wait does not outlast is **put back**, not counted: it may still land, and
        ``close`` resolves whatever remains. That has a consequence worth stating rather than
        hiding, because it is a real limit: against a client that resolves *nothing*, the pending
        list grows by a batch per emit (measured 10 → 60 over six emits) and the ``max_pending``
        bound stops holding. Nothing better is available — the three properties "never invent
        loss", "never block delivery indefinitely" and "bound memory" cannot all hold when the
        destination has stopped answering, and the first two are the ones this library is for.
        The growth is a symptom of the outage, every event in it is still accounted for, and
        ``losses()`` reports what resolves.

        Args:
          overflow: The oldest outstanding futures, already removed from the pending list.

        Returns:
          None.

        Raises:
          None.
        """
        if not overflow:
            return
        deadline = time.monotonic() + self.overflow_timeout
        unresolved: list[Any] = []
        for index, future in enumerate(overflow):
            settled = False
            while not self._out_of_time(deadline):
                slice_ = min(deadline - time.monotonic(), _POLL_INTERVAL)
                if self._resolve(future, slice_):
                    settled = True
                    break
            if settled:
                continue
            unresolved.append(future)
            if self._out_of_time(deadline):
                unresolved.extend(overflow[index + 1 :])
                break
        if not unresolved:
            return
        with self._futures_lock:
            if not self._closed:
                self._futures[:0] = unresolved
                return
        for future in unresolved:
            self._resolve(future)

    def _out_of_time(self, deadline: float) -> bool:
        """Reports whether the overflow pass must stop, on the deadline or on a shutdown.

        The stop signal is read here rather than once before the loop: read once, a shutdown
        arriving mid-pass was not noticed until every future had been waited on.

        Args:
          deadline: The monotonic time the pass may not run past.

        Returns:
          True when no further waiting may happen.

        Raises:
          None.
        """
        if time.monotonic() >= deadline:
            return True
        stop = self.log_foundry_stop_signal
        return stop is not None and stop.is_set()

    def _resolve(self, future: Any, timeout: float | None = None) -> bool:
        """Waits for one publish to settle, counting and announcing a failure.

        Args:
          future: The publish future to resolve.
          timeout: Seconds to wait, or ``None`` to wait indefinitely — which is what ``close``
            does, and what a shutdown defers this to.

        Returns:
          True when the future settled, False when a *bounded* wait expired with it still in
          flight. An expired wait is not a failure and moves no counter: the publish is
          unfinished, not unconfirmed.

          A ``TypeError`` from a *bounded* call is read the same way, because a future whose
          ``result()`` takes no ``timeout`` cannot be waited on within one: counting it would
          invent loss on a publish that was going to succeed, which an injected ``client=`` makes
          reachable (measured: three of four healthy publishes reported ``failed`` with a
          ``TypeError`` line). It is put back and resolved unbounded at ``close`` instead. A
          genuine ``TypeError`` from the publish itself takes the same route and is counted
          there, so nothing is lost either way.

          A ``TimeoutError`` is only read that way when this call set a timeout. With none set —
          which is every call from ``close`` — it is the client reporting that the publish itself
          timed out, and is counted like any other failure. Conflating the two made a genuinely
          unconfirmed publish silently uncounted, which an existing ``losses()`` test caught. A
          server-side timeout arriving *during* a bounded wait is read as "still in flight" and
          the future is put back, so it is counted at ``close`` rather than here — later, never
          lost.

        Raises:
          None. An unresolved future must never crash the worker (FR-011), and this is called
            from ``close`` as well as from the emitting thread.
        """
        try:
            future.result() if timeout is None else future.result(timeout=timeout)
        except Exception as err:
            if timeout is not None and isinstance(err, TimeoutError | TypeError):
                return False
            with self._counter_lock:
                self.failed += 1
            _diag.lost("event", 1, f"GooglePubSubSink publish unconfirmed, {type(err).__name__}")
        return True

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
