"""GooglePubSubSink — publish events to a Google Cloud Pub/Sub topic (arch §8, SPEC-010)."""

from __future__ import annotations

import json
import threading
import time
from typing import Any

from log_foundry import _diag
from log_foundry.sinks._retry import usable_timeout, wait
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

__all__ = ["GooglePubSubSink"]


class _Unboundable(Exception):
    """A future whose ``result()`` takes no ``timeout``, so no bounded wait on it is possible.

    Raised rather than folded into :meth:`GooglePubSubSink._resolve`'s ``False`` because the two
    need different answers: ``False`` means "not settled yet, wait the rest of the slice and try
    again", while this means "trying again can only fail the same way". Conflating them spun the
    slice loop at full speed until the deadline — measured at 3.5 million ``result()`` calls and
    a pegged core for one second, which is thirty at the shipped default.
    """

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

    **Retry (SPEC-041 FR-004).** The client's own retry is bounded and runs on the client's
    threads, never the worker's drain thread: the generated ``publish`` carries
    ``Retry(initial=0.1, maximum=60.0, multiplier=4, deadline=600.0)`` with a 60 s per-call
    timeout, so a publish gives up after ten minutes at the outside. What this sink contributes
    is the only wait the drain thread ever takes — :meth:`_await_overflow`'s
    ``overflow_timeout``, bounded and interruptible per SPEC-027. Measured with the destination
    stopped and ``overflow_timeout=5.0``: every over-bound emit returned in 5.00 s exactly.

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
          overflow_timeout: Seconds to wait on one over-bound publish before putting it back,
            and since SPEC-048 the bound on ``close()`` too. The wait runs on the worker's single
            drain thread, so it is bounded by SPEC-027's rule that no sink wait may be unbounded
            or uninterruptible. **Floored** to :data:`DEFAULT_OVERFLOW_TIMEOUT` when it cannot
            bound anything (SPEC-049 FR-002) — ``nan`` made every deadline comparison ``False``,
            so the loop that bounds ``flush()``, ``close()`` and ``_await_overflow`` had no bound
            at all, and ``inf`` removed it outright. Floored rather than refused because it is an
            *existing* argument some of whose degenerate values deliver against a healthy client,
            exactly ``KafkaSink(flush_timeout=)``'s shape, and SPEC-047 floored every one of those
            through :func:`~log_foundry.sinks._retry.usable_timeout`; refusing ``inf`` here while
            flooring it there would be two rules for one value.

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
        self.overflow_timeout = usable_timeout(overflow_timeout, DEFAULT_OVERFLOW_TIMEOUT)
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
        expired, unboundable = self._resolve_within(
            overflow, time.monotonic() + self.overflow_timeout, heed_stop=True
        )
        unresolved = expired + unboundable
        if not unresolved:
            return
        with self._futures_lock:
            if not self._closed:
                self._futures[:0] = unresolved
                return
        self._drain_pending(unresolved)

    def _resolve_within(
        self, pending: list[Any], deadline: float, *, heed_stop: bool
    ) -> tuple[list[Any], list[Any]]:
        """Waits on each future until the deadline, splitting what did not settle in two.

        One deadline covers the whole list rather than a timeout per future: at the shipped
        ``max_pending`` a per-future wait is not a bound at all (SPEC-038's rule that a bound
        applied per item is ``n x timeout``). ``_futures_lock`` is never held across a
        ``result()``, because ``emit`` takes it per event and an application thread on the orphan
        path would block behind it.

        Args:
          pending: The futures to resolve, already removed from the pending list.
          deadline: A ``time.monotonic()`` reading the pass must not run past.
          heed_stop: Whether a set stop signal also ends the pass. **True only on the flush
            path.** ``Worker.shutdown`` sets that event *before* closing the sink inline, so a
            close that heeded it would abandon everything on every ordinary shutdown -- SPEC-038's
            rule that a shutdown shortens a *wait* and must never skip *work*, and the exit drain
            is the one path a serverless process has.

        Returns:
          The futures whose wait expired, and separately the ones that cannot be waited on within
          a timeout at all. They are split because they mean opposite things: an expired future is
          unconfirmed, while an *unboundable* one is a healthy publish this pass simply cannot
          poll, and counting the second as loss invents it (SPEC-036 measured three of four
          healthy publishes reported ``failed`` that way).

        Raises:
          None.
        """
        expired: list[Any] = []
        unboundable: list[Any] = []
        for index, future in enumerate(pending):
            settled = False
            unpollable = False
            while not self._past(deadline, heed_stop=heed_stop):
                began = time.monotonic()
                slice_ = min(deadline - began, _POLL_INTERVAL)
                try:
                    settled = self._resolve(future, slice_)
                except _Unboundable:
                    unpollable = True
                    break
                if settled:
                    break
                wait(slice_ - (time.monotonic() - began), self.log_foundry_stop_signal)
            else:
                self._classify_remainder(pending[index:], expired, unboundable)
                break
            if unpollable:
                unboundable.append(future)
        return expired, unboundable

    def _classify_remainder(
        self, remaining: list[Any], expired: list[Any], unboundable: list[Any]
    ) -> None:
        """Sorts the futures a deadline never reached into expired and unboundable.

        The pass gives up on the whole remainder when its one deadline expires, and the two
        outcomes must still be told apart: an expired future is unconfirmed and is counted, while
        an **unboundable** one is a healthy publish that simply cannot be polled within a timeout,
        and counting it invents loss. A blanket ``expired.extend(...)`` here charged three healthy
        publishes as lost behind one stalled future, which is SPEC-036's measured defect
        reintroduced by the fix that cites it.

        The probe is a zero-second wait, so it costs nothing against a deadline that has already
        gone: a future whose ``result()`` takes no ``timeout`` raises ``_Unboundable`` on the way
        in, and everything else either settles immediately or reports itself still in flight.

        Args:
          remaining: The futures the pass did not reach, the current one first.
          expired: The unconfirmed list, appended to in place.
          unboundable: The cannot-be-polled list, appended to in place.

        Returns:
          None.

        Raises:
          None.
        """
        for future in remaining:
            try:
                if not self._resolve(future, 0):
                    expired.append(future)
            except _Unboundable:
                unboundable.append(future)

    def _past(self, deadline: float, *, heed_stop: bool) -> bool:
        """Reports whether a resolution pass must stop.

        Args:
          deadline: A ``time.monotonic()`` reading.
          heed_stop: Whether a set stop signal also ends the pass.

        Returns:
          True on the deadline, or on the stop signal when this caller heeds it.

        Raises:
          None.
        """
        if time.monotonic() >= deadline:
            return True
        if not heed_stop:
            return False
        stop = self.log_foundry_stop_signal
        return stop is not None and stop.is_set()

    def _drain_pending(self, pending: list[Any]) -> None:
        """Resolves a swapped-out list within the close bound, counting what it abandons.

        The tail every close-race site shares: :meth:`close`, and the two branches where a close
        lands while another pass is mid-flight. Each used to resolve its leftovers with
        ``timeout=None``, which is the unbounded wait ``Worker.shutdown`` performs inline against
        a client publish deadline of 600 s. ``_await_overflow``'s copy is the one that mattered
        most: it runs on whichever thread called ``emit``, which on the orphan path is an
        application thread.

        Unboundable futures still get ``timeout=None``, because that is the only wait they accept
        and :meth:`_resolve` counts whatever they resolve to.

        Args:
          pending: The futures this caller owns and nothing else will resolve.

        Returns:
          None.

        Raises:
          None.
        """
        expired, unboundable = self._resolve_within(
            pending, time.monotonic() + self.overflow_timeout, heed_stop=False
        )
        for future in unboundable:
            self._resolve(future)
        if not expired:
            return
        with self._counter_lock:
            self.failed += len(expired)
        _diag.lost(
            "event",
            len(expired),
            f"GooglePubSubSink, {len(expired)} publish(es) still in flight when the "
            f"{self.overflow_timeout}s close bound expired",
        )

    def _resolve(self, future: Any, timeout: float | None = None) -> bool:
        """Waits for one publish to settle, counting and announcing a failure.

        Args:
          future: The publish future to resolve.
          timeout: Seconds to wait, or ``None`` to wait indefinitely. Since SPEC-048 FR-004 that
            is what an *unboundable* future gets, not what ``close`` does: ``close`` bounds itself
            on ``overflow_timeout`` through :meth:`_drain_pending`, because it runs inline inside
            ``Worker.shutdown`` against a client publish deadline of 600 s.

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
          _Unboundable: When a bounded call cannot be made at all, which only
            :meth:`_await_overflow` catches. An unresolved future must never crash the worker
            (FR-011), and this is called from ``close`` as well as from the emitting thread, so
            nothing else escapes.
        """
        try:
            future.result() if timeout is None else future.result(timeout=timeout)
        except Exception as err:
            if timeout is not None and isinstance(err, TypeError):
                raise _Unboundable from None
            if timeout is not None and isinstance(err, TimeoutError):
                return False
            with self._counter_lock:
                self.failed += 1
            _diag.lost("event", 1, f"GooglePubSubSink publish unconfirmed, {type(err).__name__}")
        return True

    def flush(self) -> None:
        """Resolves the outstanding publish futures without closing the sink (SPEC-036 FR-002).

        ``emit`` appends an unresolved future and returns; before this hook existed nothing but
        ``close()`` ever called ``result()`` on them, so ``log_foundry.flush()`` could not reach a
        single one — the call whose whole purpose is delivery before a freeze.

        **It is :meth:`_await_overflow` applied to the whole pending list, and the three rules it
        obeys are that method's, each earned by a measured defect.** One ``deadline`` covers the
        list rather than a timeout per future: at the shipped ``max_pending`` a per-future wait is
        not a bound at all, and a stalled destination would hold ``log_foundry.flush()`` for
        hours. ``_Unboundable`` is **caught**, because a future whose ``result()`` takes no
        ``timeout`` cannot be waited on within one — and letting it escape here would abandon the
        entire list, which has already been swapped out and is referenced by nothing else. And
        ``_futures_lock`` is **not** held across a ``result()``, because ``emit`` takes it per
        event and an application thread on the orphan path would block behind it.

        A future that did not settle is put back rather than dropped: the sink stays open, so it
        is unfinished, not unconfirmed, and the next flush or the close waits on it again. It
        **raises** when any remained, which is what makes ``log_foundry.flush()`` report
        ``reason="sink-flush"``. A future that settled *failed* is already counted by
        :meth:`_resolve` and reported through ``losses()``, per SPEC-026.

        Args:
          None.

        Returns:
          None.

        Raises:
          SinkDeliveryError: The sink is closed, or a publish was still in flight afterwards.
        """
        if self._closed:
            raise SinkDeliveryError("GooglePubSubSink cannot flush: the sink is closed")
        with self._futures_lock:
            pending, self._futures = self._futures, []
        if not pending:
            return

        expired, unboundable = self._resolve_within(
            pending, time.monotonic() + self.overflow_timeout, heed_stop=True
        )
        unresolved = expired + unboundable

        if not unresolved:
            return
        with self._futures_lock:
            closed = self._closed
            if not closed:
                self._futures[:0] = unresolved
        if closed:
            self._drain_pending(unresolved)
        raise SinkDeliveryError(
            f"GooglePubSubSink flushed with {len(unresolved)} publish(es) still in flight"
        )

    def close(self) -> None:
        """Resolves the pending publish futures within a bound, counting the rest (FR-008).

        **Bounded since SPEC-048 FR-004.** It used to wait ``timeout=None`` per future, and
        ``Worker.shutdown`` closes the live sink inline, so one unreachable destination held
        process exit for the client's 600 s publish deadline per future. The bound is
        ``overflow_timeout`` on the monotonic clock and deliberately **not** the stop signal;
        :meth:`_resolve_within` records why.

        **The bound does not cover a future that cannot be waited on within a timeout.** One whose
        ``result()`` takes no ``timeout`` argument is resolved unbounded, because that is the only
        wait it accepts and SPEC-036 measured that counting it instead invents loss on publishes
        that were going to succeed. So a client handing out unboundable futures that also do not
        settle still holds the close for its own deadline, once per future: measured at 27.0 s for
        nine futures against a 3 s stand-in, where the bounded path took 2.0 s for sixty.
        ``google-cloud-pubsub``'s own future accepts a timeout, so this is reachable only through
        an injected ``client=`` that does not — but ``client=`` is a frozen public parameter, which
        is why it is written down here rather than assumed away. Recorded in ``architecture.md``
        §12; it is strictly better than before, where **every** future took this path.

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
        self._drain_pending(pending)


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
