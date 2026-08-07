"""Background flush worker — non-blocking span delivery (arch §9, guide Phase 9)."""

from __future__ import annotations

import queue
import threading
import time
from typing import TYPE_CHECKING, NamedTuple, cast

from log_foundry import _diag

if TYPE_CHECKING:
    from log_foundry.sinks.base import Sink, SinkLosses

__all__ = ["Health", "Worker"]

_SHUTDOWN = object()

DEFAULT_SHUTDOWN_TIMEOUT = 30.0
"""Seconds :meth:`Worker.shutdown` will wait for the drain thread (SPEC-027 FR-004).

Generous, because the ordinary case is a fast drain and expiring early would abandon events
that were about to be delivered. Bounded at all, because ``shutdown()`` runs from ``atexit``
and an unbounded join there is a hung process.
"""

_DROP_WARN_EVERY = 1000

DEFAULT_CLOSER_GRACE = 2.0
"""Seconds :meth:`Worker.shutdown` gives an outstanding swapped-out close to finish.

Deliberately much smaller than the shutdown budget it is carved from. This is a last chance for
a close that is *nearly* done, not a second full attempt: it already had the swap's whole budget
(``DEFAULT_SWAP_TIMEOUT``) before ``shutdown`` was ever called, so one still running here is far
more likely stuck than slow, and every second spent on it is a second the process does not exit.
"""

DEFAULT_SWAP_TIMEOUT = 5.0
"""Seconds a late ``configure(sink=...)`` will spend draining the previous sink (FR-003).

Shorter than the shutdown budget on purpose: this runs on the caller's thread inside a
configuration call, where a long stall is a startup that appears to hang, and what is at risk
is a sink swap rather than the tail of the whole process.
"""


def _bounded_seconds(timeout: float | None) -> str:
    """Renders a shutdown timeout for a diagnostic without trusting its ``__str__``.

    The timeout is the caller's, so it is a value the library does not control — the rule
    ``_diag.errno_of`` follows for an ``errno``. A non-number renders as ``"?"`` rather than
    whatever its ``__repr__`` chose to say (SPEC-029 FR-002).

    Args:
      timeout: The caller's timeout, or ``None`` for an unbounded wait.

    Returns:
      The rendered seconds, ``"no timeout"``, or ``"?"``.

    Raises:
      None.
    """
    try:
        return f"{float(timeout):g}s" if timeout is not None else "no timeout"
    except Exception:
        return "?"


class Health(NamedTuple):
    """A point-in-time snapshot of the worker's delivery counters (SPEC-017 FR-005).

    ``stopped_reason``, ``sink``, and SPEC-030's three are defaulted and appended in that
    order, so the zeroed snapshot in ``decorator._worker_health`` — and any third-party
    construction — keeps working, and attribute and index access to every earlier field stays
    as it was.

    Attributes:
      queued: Submissions currently buffered. Approximate by nature: it is read without
        stopping the world, and briefly counts the internal flush/shutdown markers alongside
        real submissions.
      dropped: Submissions discarded because the queue was full (backpressure).
      failed_batches: Batches abandoned after the retry budget was spent.
      stopped_reason: The exception type name that terminated the drain thread, or ``None`` if
        it never died — which is also what a live worker and a process that never logged
        report. Non-``None`` is categorically worse than the two counters above: they measure
        loss the worker absorbed and kept running through, this one means the worker is gone
        (SPEC-019 FR-003). Also ``"ShutdownTimeout"`` when a bounded :meth:`Worker.shutdown`
        expired before the drain finished (SPEC-027 FR-004), the same thing to a reader.
      sink: The configured sink's own loss counters, or ``None`` when there is no worker or
        the sink reports nothing (SPEC-026 FR-003). Nested rather than folded into the
        integers above because they count different things: ``dropped`` here is backpressure
        at this queue, ``dropped`` on the sink is an event that never reached the wire.
      retired: Whether ``shutdown()`` has been called. It describes an action the
        caller took, not a failure the library detected, which is why it is a boolean where
        ``stopped_reason`` is a string — SPEC-019 rejected an ``alive`` flag because it would
        read ``False`` for a process that never logged, and that objection does not apply to a
        field which is simply ``False`` until someone calls ``shutdown()`` (SPEC-030 FR-001).
        On its own it is not a fault: a process that shuts down and stops logging is correct.
        It is the one field ``decorator._worker_health`` synthesizes rather than zeroing, so
        that a process which only ever logged outside a span — and therefore has no worker at
        all — still reports its own shutdown truthfully (SPEC-031 FR-006).
      submitted_after_shutdown: Submissions accepted after ``shutdown()`` and queued where
        nothing will drain them. Non-zero alongside ``retired`` is the signature of the
        serverless mistake — ``shutdown()`` called per invocation on a warm container, so the
        first invocation logs and every later one silently does not. The count starts at the
        moment ``shutdown()`` begins rather than when the drain thread ends, so a submission
        racing the final drain is counted even if that drain carried it; erring toward
        reporting is the right direction for a signal whose whole purpose is visibility.
      incomplete_swaps: Late ``configure(sink=...)`` calls whose drain of the previous sink
        did not complete (SPEC-030 FR-003). The swap still took effect, so the caller has the
        sink it asked for, but two things could not be guaranteed: events submitted before the
        call may have been carried to the new sink instead of the old one, and the old sink was
        left **open** rather than closed, because the drain thread may still be inside its
        ``emit`` — the reasoning SPEC-027 FR-004 applies to an expired ``shutdown()``.
      closing_sinks: Swapped-out sinks whose ``close()`` is running *at this instant* — a live
        gauge, not a counter, and the only field here that can fall as well as rise. A close is
        bounded only in how long ``configure()`` waits for it, so this is how a destination
        stuck in ``close()`` becomes visible at all. Reading it non-zero once means a swap just
        happened; reading it non-zero repeatedly means a close is not coming back, and that sink
        still holds its resources. It is deliberately a live read rather than a count of expired
        joins: a slow close and a stuck one are indistinguishable at the moment a join expires,
        and SPEC-028 reverted a design that guessed.
    """

    queued: int
    dropped: int
    failed_batches: int
    stopped_reason: str | None = None
    sink: SinkLosses | None = None
    retired: bool = False
    submitted_after_shutdown: int = 0
    incomplete_swaps: int = 0
    closing_sinks: int = 0


class _FlushMarker:
    """A drain request travelling the queue in FIFO order (SPEC-013 FR-002).

    The mechanism follows from the queue being FIFO: everything submitted before ``flush()``
    was called is necessarily ahead of the marker, so by the time the worker dequeues it those
    events are either already emitted or sitting in ``pending``. Like ``_SHUTDOWN`` it is never
    emitted, but unlike ``_SHUTDOWN`` it carries state, so it is a class rather than a bare
    sentinel.

    ``delivered`` carries the drain's outcome back to the waiter, not merely the fact that the
    marker was reached (SPEC-021 FR-001). It is written by the drain thread before
    ``event.set()`` and read by the waiter after ``event.wait()`` returns, so the ``Event``
    supplies the ordering and no further lock is needed. It starts ``False`` because every path
    that answers a marker assigns it explicitly, leaving the default to be read only when the
    drain thread died without computing an answer.

    The outcome is computed from ``seen_failures`` — ``Worker.failed_batches`` as it stood when
    the marker was created — against the same counter when the marker is answered, so every
    flush outstanding when a batch is abandoned reports it. A batch abandoned before the call
    is deliberately not in scope: that loss is already in ``failed_batches`` and on stderr, and
    folding it in would make every later empty flush report a failure it did not incur.
    """

    __slots__ = ("delivered", "event", "seen_failures")

    def __init__(self, seen_failures: int) -> None:
        """Stamps a marker with the failure count it will be judged against.

        Args:
          seen_failures: ``Worker.failed_batches`` as read on the caller's thread, before the
            marker joins the queue.

        Returns:
          None.

        Raises:
          None.
        """
        self.event = threading.Event()
        self.delivered = False
        self.seen_failures = seen_failures


class Worker:
    """Owns a bounded queue and daemon thread that batch and flush events to a sink.

    :meth:`submit` is a fast, in-process handoff, so a slow or down sink can never
    back-pressure the app: the queue is bounded and overflow is dropped-newest with a counter
    (arch §9). Two drains are deliberately distinct (SPEC-013) — :meth:`shutdown` is terminal
    and the worker never comes back, while :meth:`flush` drains on demand and leaves everything
    running, which a process that is frozen rather than exited needs. This class owns delivery
    mechanics only, and knows nothing about spans or context.
    """

    def __init__(
        self,
        sink: Sink,
        *,
        batch_size: int = 10,
        flush_interval: float = 1.0,
        max_queue: int = 10_000,
        max_retries: int = 3,
    ) -> None:
        """Starts the drain thread and offers the sink this worker's stop signal.

        The lock guards the ``dropped`` counter, incremented from any caller thread, and the
        shutdown once-only flag, since ``shutdown`` may be called concurrently by ``atexit``
        and user code.

        Args:
          sink: The destination every batch is emitted to.
          batch_size: How many submissions accumulate before an emit is triggered.
          flush_interval: Seconds before a partial batch is emitted anyway.
          max_queue: Ceiling on buffered submissions, past which the newest is dropped.
          max_retries: Retries after a failing emit, floored at zero by :meth:`_emit`.

        Returns:
          None.

        Raises:
          None.
        """
        self.sink = sink
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.max_retries = max_retries
        self.dropped = 0
        self.failed_batches = 0
        self.stopped_reason: str | None = None
        self.submitted_after_shutdown = 0
        self.incomplete_swaps = 0
        self._closers: list[threading.Thread] = []
        self._queue: queue.Queue[object] = queue.Queue(maxsize=max_queue)
        self._stop = threading.Event()
        self._shutdown_done = False
        self._sink_closed = False
        self._lock = threading.Lock()
        self._offer_stop_signal()
        self._thread = threading.Thread(
            target=self._run, name="log-foundry-worker", daemon=True
        )
        self._thread.start()

    def _offer_stop_signal(self) -> None:
        """Gives the sink this worker's shutdown event, if it advertises somewhere to put it.

        The dependency stays one-way (SPEC-027 FR-002): ``sinks`` must not import ``worker``,
        so the worker pushes rather than the sink pulling. It is probed with ``hasattr``, the
        same optional-protocol shape SPEC-026 uses for ``losses()`` — a sink without the
        attribute simply never gets one and backs off uninterruptibly, exactly as before.

        Args:
          None.

        Returns:
          None.

        Raises:
          None. A sink whose ``stop_signal`` is a read-only property, or whose
            ``__setattr__`` objects, loses interruptibility rather than preventing the worker
            from starting.
        """
        try:
            if hasattr(self.sink, "stop_signal"):
                self.sink.stop_signal = self._stop
        except Exception as exc:
            _diag.absorbed(
                "handing the sink its stop signal", exc, "its backoff stays uninterruptible"
            )

    def submit(self, events: list[dict[str, object]]) -> None:
        """Hands a finished span's events to the worker, without blocking.

        This enqueues via ``put_nowait`` and returns immediately without touching the sink.
        When the queue is full it drops this submission and counts it in ``dropped`` rather
        than blocking the caller (FR-001, FR-004), warning on a throttle since overflow is a
        high-rate condition and a line per drop would be its own outage (SPEC-017 FR-005).

        The warning is written outside the lock deliberately: stderr can block on a slow
        reader, and the lock also guards the once-only shutdown flag, so holding it across a
        blocking write would let a wedged console stall the drain path. Lines may therefore
        interleave out of order under concurrency, but the counts they carry are exact.

        A submission arriving after :meth:`shutdown` is still accepted, and counted where it
        can be seen (SPEC-030 FR-001): the worker is retired and nothing will drain the queue,
        so this is total silent loss until something reports it. The check is a single unlocked
        read of a flag that is only ever set, never cleared, which is what keeps the normal path
        free — taking the lock here would put every submission behind the counter's contention
        for a condition that is false in every correct program. SPEC-019's objection to a
        liveness check in ``submit`` does not reach it: that was about probing the thread, this
        is a boolean already in the object's dict.

        Args:
          events: The span's buffered events, submitted as one item.

        Returns:
          None.

        Raises:
          None.
        """
        if self._shutdown_done:
            self._count_undeliverable()
        try:
            self._queue.put_nowait(events)
        except queue.Full:
            with self._lock:
                self.dropped += 1
                total = self.dropped
            if total == 1 or total % _DROP_WARN_EVERY == 0:
                _diag.lost("submission", total, "log queue full; count is cumulative")

    def _count_undeliverable(self) -> None:
        """Counts a post-shutdown submission and warns on the same throttle as overflow.

        The two conditions warrant the same treatment for the same reason (FR-002): a caller
        making this mistake makes it on every invocation, so a line per submission would be its
        own outage, while total silence is how the mistake survives to production. The counter
        moves before the line is attempted, per ``_diag``'s record-first rule, and the write
        happens outside the lock so a wedged console cannot stall the drain path.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        with self._lock:
            self.submitted_after_shutdown += 1
            total = self.submitted_after_shutdown
        if total == 1 or total % _DROP_WARN_EVERY == 0:
            _diag.lost(
                "submission",
                total,
                "logged after shutdown(), which is terminal; nothing will drain these. Use "
                "flush() in a process that logs again. Count is cumulative",
            )

    def health(self) -> Health:
        """Snapshots the delivery counters (SPEC-017 FR-005, SPEC-019 FR-003).

        This stays valid after :meth:`shutdown`: the counters are plain integers that outlive
        the thread, and the final drain consumes the queue, so ``queued`` reads 0 rather than a
        stale marker. The same applies to ``stopped_reason``, since a caller finding a dead
        worker will usually call ``shutdown()`` next. Reading it after a shutdown is in fact
        the point of ``retired`` and ``submitted_after_shutdown`` (SPEC-030 FR-001), which
        report a state only a retired worker can be in.

        Args:
          None.

        Returns:
          The snapshot, including the sink's own losses when it reports any.

        Raises:
          None.
        """
        with self._lock:
            dropped, failed_batches = self.dropped, self.failed_batches
            stopped_reason = self.stopped_reason
            retired = self._shutdown_done
            submitted_after_shutdown = self.submitted_after_shutdown
            incomplete_swaps = self.incomplete_swaps
            self._closers = [closer for closer in self._closers if closer.is_alive()]
            closing_sinks = len(self._closers)
        return Health(
            queued=self._queue.qsize(),
            dropped=dropped,
            failed_batches=failed_batches,
            stopped_reason=stopped_reason,
            sink=self._sink_losses(),
            retired=retired,
            submitted_after_shutdown=submitted_after_shutdown,
            incomplete_swaps=incomplete_swaps,
            closing_sinks=closing_sinks,
        )

    def _sink_losses(self) -> SinkLosses | None:
        """Reads the configured sink's optional ``losses()`` (FR-003).

        The probe and its guarantees live in ``sinks.base.read_losses``, imported here rather
        than at module scope, which keeps ``worker`` free of a runtime dependency on ``sinks``
        the same way ``config`` does.

        Args:
          None.

        Returns:
          The sink's losses, or ``None`` when it reports none.

        Raises:
          None.
        """
        from log_foundry.sinks.base import read_losses

        return read_losses(self.sink)

    def flush(self, timeout: float | None = 5.0) -> bool:
        """Drains everything submitted before this call through the sink, without stopping.

        The precise claim is that nothing was abandoned while this call was outstanding: the
        batch this flush forces, and any batch another flush or a batching trigger emitted
        while its marker waited its turn. A batch abandoned before the call is not in scope —
        that loss is already in ``failed_batches`` and on stderr, and counting it here would
        make every later empty flush report a failure it did not incur.

        Unlike :meth:`shutdown` the thread keeps running, the sink is not closed, and the
        once-only shutdown flag is untouched, so logging continues normally afterwards
        (SPEC-013 FR-002). The put is blocking rather than ``put_nowait``, because on a full
        queue ``put_nowait`` would skip the flush and return as though it had succeeded, the
        one outcome a flush must never produce silently.

        Liveness is re-checked **after** the put, and that second look is what makes a
        ``timeout=None`` call safe. The checks above can both pass microseconds before the drain
        thread finishes, leaving this marker queued behind a thread that will never read it and
        past the sweep :meth:`shutdown` runs on its way out — a bounded caller then waits out
        its timeout, which SPEC-021 accepts as correct either way, but an unbounded one waits
        forever. Re-checking closes that: a marker put while the thread is still alive is queued
        before it can exit, so either the thread answers it or ``shutdown``'s sweep does.

        Args:
          timeout: Seconds bounding the whole call — one deadline shared by the put and the
            wait, so the two cannot add up to twice the timeout. ``None`` waits indefinitely.

        Returns:
          True once the worker has delivered them. False on timeout, on a worker already shut
          down or dead, on a queue too full to accept the marker, and when the drain carrying
          those events was abandoned after exhausting retries (SPEC-021 FR-001). That last
          case used to return True, a false success exactly where ``flush()`` matters most.

        Raises:
          None.
        """
        with self._lock:
            if self._shutdown_done:
                return False
        if not self._thread.is_alive():
            return False
        with self._lock:
            marker = _FlushMarker(self.failed_batches)
        deadline = None if timeout is None else time.monotonic() + timeout
        try:
            self._queue.put(marker, timeout=timeout)
        except queue.Full:
            return False
        if not self._thread.is_alive():
            return False
        remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
        if not marker.event.wait(remaining):
            return False
        return marker.delivered

    def swap_sink(self, new_sink: Sink, timeout: float | None = DEFAULT_SWAP_TIMEOUT) -> None:
        """Retargets delivery at a new sink, draining and closing the previous one (FR-003).

        This is what makes a late ``configure(sink=...)`` mean what it says. The sink was
        captured once when the worker was built, so before SPEC-030 a later call updated the
        config — which ``get_config().sink`` then reported — while every event continued to the
        old sink: the config and the behaviour disagreed, and nothing said so.

        The attribute is reassigned rather than the worker rebuilt, which keeps the queue, the
        thread, the counters and the ``atexit`` registration intact; rebuilding would drop
        whatever was queued and register a second drain. The order is the contract: drain first
        so everything submitted before the call reaches the sink it was submitted for, swap,
        then drain again before closing. That second drain is a fence rather than a delivery —
        it proves the drain thread is not still inside the old sink's ``emit``, which is the one
        way ``close()`` could be called under a writer.

        On a drain that cannot be confirmed the swap still stands, because the caller asked for
        the new sink and silently keeping the old one is the defect this method exists to fix.
        What changes is that the old sink is left **open** and ``incomplete_swaps`` records it:
        the drain thread may still be using it, and SPEC-027 FR-004 already settled that a
        leaked resource beats a close raced against a write.

        Both guards are re-taken after the first drain, which blocks and therefore cannot be
        trusted to return into the state it left. Retirement is the one that bites: ``shutdown``
        closes whatever ``self.sink`` was at that moment and latches its once-only flag, so a
        swap reassigning afterwards would install a sink nothing will ever close, and then
        report that the *old* one was left open when it had in fact just been closed.

        ``configure()`` remains a startup call and this does not make it thread-safe. A span
        finishing on another thread during the swap may land on either sink; what is guaranteed
        is that everything submitted before the call was drained to the old one.

        Args:
          new_sink: The sink every subsequent batch is emitted to.
          timeout: Seconds bounding the **whole** call — one deadline shared by both drains and
            the close of the old sink, so a destination that hangs in any of the three cannot
            hold ``configure()`` for a multiple of the budget. ``None`` waits indefinitely.

        Returns:
          None. The outcome is reported through ``health().incomplete_swaps`` and one stderr
          line rather than a return value, because the caller is ``configure()``, which has
          never had one and whose callers do not check.

        Raises:
          None on a sink fault. A close that fails is announced, as everywhere else
          (SPEC-025 FR-004).
        """
        with self._lock:
            if self._shutdown_done or self.sink is new_sink:
                return
        deadline = None if timeout is None else time.monotonic() + timeout
        drained = self.flush(timeout)
        with self._lock:
            if self._shutdown_done:
                return
            old = self.sink
            if old is new_sink:
                return
            self.sink = new_sink
        self._offer_stop_signal()
        remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
        if not (drained and self.flush(remaining)):
            self._record_incomplete_swap(timeout)
            return
        left = None if deadline is None else max(0.0, deadline - time.monotonic())
        self._close_swapped_out(old, left)

    def _record_incomplete_swap(self, timeout: float | None) -> None:
        """Counts a swap whose drain could not be confirmed, then announces it (FR-003).

        Two things are reported at once because they have one cause: queued items may have been
        carried to the new sink rather than the old one, and the old sink was left open. The
        count is queued *items* — one per submitted span plus any marker — which makes it a
        floor on the events involved, the useful direction for a reader deciding whether to care.

        Args:
          timeout: The budget the drain was given, rendered for the line.

        Returns:
          None.

        Raises:
          None.
        """
        with self._lock:
            self.incomplete_swaps += 1
        _diag.lost(
            "item",
            self._queued_or_unknown(),
            f"the previous sink could not be confirmed drained within "
            f"{_bounded_seconds(timeout)} of a configure(sink=...); it is left open, and "
            f"queued items may reach the new sink instead",
        )

    def _close_swapped_out(self, sink: Sink, timeout: float | None) -> None:
        """Closes a sink the worker no longer delivers to, waiting only for the budget.

        This is reached only once both drains have been confirmed, so the *drain thread* is
        provably out of this sink's ``emit``. An orphan-path emitter on an application thread
        is not covered: it resolves the sink through ``_ensure_sink`` before emitting, so one
        that read the old sink before ``configure()`` reassigned it can still be inside its
        ``emit`` — which is why ``sinks/base.py`` requires ``close()`` to tolerate exactly that
        (SPEC-028 FR-001), and why the sinks holding transport state take their lock in both.

        ``Sink.close`` takes no timeout, so the close is run on its own thread and joined for
        what is left of the swap's budget. **An expired join decides only who waits** — it moves
        no counter and writes no line, which is what dissolves SPEC-028's objection that an
        expired join cannot tell a slow-but-successful close from a stuck one and so reports a
        loss for closes that completed. What *is* observable is a live fact rather than an
        inference: ``health().closing_sinks`` counts the closes running at the moment it is read.

        The thread is a **daemon**, and it is :meth:`_join_closers` that makes that safe rather
        than merely available. A non-daemon thread was tried and is worse on its own: CPython
        joins non-daemon threads *before* running ``atexit``, so one hung close stops the exit
        drain from ever running and loses everything buffered in the **live** sink, along with
        the application's own exit handlers. A daemon alone is also worse on its own, in the
        opposite case: a close that is slow but *succeeding* is killed at exit, losing whatever
        it was flushing. ``shutdown`` therefore drains and closes the live sink first, then
        joins any outstanding closer for what is left of its budget — so a slow close finishes,
        a hung one costs only the grace, and neither can reach the live sink. What SPEC-028
        refused to abandon was the sink the worker was *still delivering to*; this one has been
        fenced out of the delivery path by two confirmed drains.

        It is deliberately not :meth:`_close_sink`, which answers a different question — that
        one closes the sink the worker still holds, exactly once, and only after the thread has
        ended.

        Args:
          sink: The sink that was swapped out.
          timeout: Seconds to wait for the close before returning and letting it finish on its
            own. ``None`` waits indefinitely.

        Returns:
          None.

        Raises:
          None. ``Thread.start`` raises when the platform will not give the process another
            thread, and a swap that cannot spawn one must leave the sink open and say so rather
            than fall back to an inline close — the fallback would reintroduce the unbounded
            wait this method exists to remove, in the one situation where the process is
            already under resource pressure.
        """
        closer = threading.Thread(
            target=self._close_detached,
            args=(sink,),
            name="log-foundry-sink-close",
            daemon=True,
        )
        try:
            closer.start()
        except Exception as exc:
            _diag.absorbed(
                "starting the thread that closes a swapped-out sink",
                exc,
                "it is left open and may still hold its resources",
            )
            return
        with self._lock:
            self._closers = [old for old in self._closers if old.is_alive()]
            self._closers.append(closer)
        closer.join(timeout)

    def _close_detached(self, sink: Sink) -> None:
        """Closes a swapped-out sink on its own thread, absorbing a failure.

        The guard is what makes the thread safe to leave unattended: an exception escaping here
        would reach CPython's thread bootstrap, which prints a full traceback carrying the
        exception's message — the user data arch §6 keeps out of anything the library says about
        itself, and the reason :meth:`shutdown` guards its own close.

        Args:
          sink: The sink to close.

        Returns:
          None.

        Raises:
          None.
        """
        try:
            sink.close()
        except Exception as exc:
            _diag.absorbed("closing a swapped-out sink", exc, "it may still hold its resources")

    def shutdown(self, timeout: float | None = DEFAULT_SHUTDOWN_TIMEOUT) -> None:
        """Stops the thread, drains and emits everything queued, then closes the sink.

        This is bounded (SPEC-027 FR-004). On expiry it returns having stopped what it could,
        records a ``stopped_reason`` of ``"ShutdownTimeout"`` and writes one line; it does not
        kill the thread, which Python cannot do and which would leave a sink mid-write if it
        could. An expired shutdown does not close the sink either, because the drain thread may
        still be inside ``emit`` — the cost is a leaked resource in a process that is exiting
        anyway, which is the cheaper of the two. That close is deferred rather than abandoned:
        a later call finds the thread finished and closes the sink then.

        The once-only flag deliberately stays ahead of the close. Re-running a drain is not
        safe, and a second ``shutdown()`` retrying a close that already failed would call
        ``close()`` twice on a sink that may have partially released its resources; what
        SPEC-025 FR-004 changed is that the failure is announced rather than swallowed.

        **The sentinel is queued before ``_stop`` is set, and that order is what makes it
        impossible to strand.** Both ways of leaving the drain loop — taking the sentinel, or
        seeing ``_stop`` — can only happen once it is already in the queue, so either that
        ``get`` consumes it or :meth:`_final_drain` does. The reverse order left a window in
        which the loop read ``_stop``, exited, and finished its final drain before the sentinel
        landed: measured stranding it once in roughly 4000 idle shutdowns and once in 52 under
        load, which never lost an event but left ``health().queued`` reading 1 for the life of
        the process. Paying for it needs :meth:`_drain` to break on the sentinel rather than
        loop, or a thread taking it before ``_stop`` was set would block for another
        ``flush_interval``.

        :meth:`_release_waiters` runs on the way out for the sibling case the ordering cannot
        reach: a ``flush()`` that passed its liveness check microseconds before the thread
        finished can still queue a marker nothing will answer, and with ``timeout=None`` that
        caller waits forever rather than merely too long.

        The worker does not come back, and :meth:`submit` keeps accepting afterwards — so a
        caller that logs again queues events nothing will drain. That is reported rather than
        prevented, through ``retired`` and ``submitted_after_shutdown`` (SPEC-030 FR-001) and
        one stderr line; refusing the submission or restarting the thread were both rejected,
        the second because a thread that resurrects itself fights a process trying to exit.

        Args:
          timeout: Seconds the join may take. ``None`` waits indefinitely, which is what this
            did unconditionally before and is still available on request.

        Returns:
          None.

        Raises:
          None. An unguarded close raised out of the ``atexit`` handler, where CPython printed
            a full traceback carrying the exception's message, which arch §6 keeps out of
            anything the library says about itself.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._lock:
            first = not self._shutdown_done
            self._shutdown_done = True
        if not first:
            self._close_if_owed()
            self._join_closers(None if deadline is None else max(0.0, deadline - time.monotonic()))
            return
        try:
            self._queue.put_nowait(_SHUTDOWN)
        except queue.Full:
            pass
        self._stop.set()
        self._thread.join(timeout)
        if self._thread.is_alive():
            queued = self._queued_or_unknown()
            with self._lock:
                if self.stopped_reason is None:
                    self.stopped_reason = "ShutdownTimeout"
            _diag.lost(
                "item",
                queued,
                f"shutdown timed out after {_bounded_seconds(timeout)}; the sink is left open "
                f"because the worker thread is still using it",
            )
            return
        self._release_waiters()
        self._close_if_owed()
        self._join_closers(None if deadline is None else max(0.0, deadline - time.monotonic()))

    def _join_closers(self, timeout: float | None) -> None:
        """Gives a swapped-out sink's close its last chance before the process exits.

        This is what makes the daemon closer of :meth:`_close_swapped_out` safe rather than
        merely available. A daemon is killed wherever it has reached at interpreter exit, so
        without this a close that was slow but *succeeding* — a ``KafkaSink`` flushing its
        producer, where the close is the delivery — would lose its buffer, which a non-daemon
        thread would not have. Measured both ways: daemon alone lost those events, non-daemon
        alone lost everything in the *live* sink instead, and this join is what takes neither
        loss.

        **The cap is the mechanism.** The wait is the smaller of ``DEFAULT_CLOSER_GRACE`` and
        what remains of ``shutdown``'s own budget: capped so a stuck close cannot hold a process
        at exit for the whole shutdown budget, and carved from that budget so it cannot extend it
        either. Running after :meth:`_close_if_owed` rather than before it is defence in depth
        rather than the guarantee — measured, the two orders deliver the live sink identically,
        because the cap returns control long before anything is at risk. It is still the right
        order, and pinned by a test: it is what holds if an external deadline kills the process
        *during* the grace, where the live sink would otherwise be the one left unclosed.

        It runs on the idempotent path too. A first ``shutdown`` that expired on a wedged drain
        thread returns before ever reaching here, and the ``atexit`` call that follows would
        otherwise return instantly — denying the grace to a swapped-out close that is healthy and
        moments from finishing, which is exactly the loss the grace exists to prevent. The closer
        is independent of the worker thread, so a wedged worker is no reason to abandon it. The
        expired path itself still skips it, and that costs nothing: the thread join consumed the
        budget, so the remainder is zero.

        Args:
          timeout: Seconds remaining in ``shutdown``'s budget, further capped by
            ``DEFAULT_CLOSER_GRACE`` and shared across every outstanding close. ``None`` takes
            the cap rather than waiting indefinitely — an unbounded ``shutdown`` is a caller's
            choice about draining events, not a licence for a stuck close to hold the exit.

        Returns:
          None.

        Raises:
          None. A join on a thread that has already finished is a no-op, and one that has not
            is abandoned at the deadline — which is the daemon's contract, not a failure.
        """
        with self._lock:
            closers = [closer for closer in self._closers if closer.is_alive()]
            self._closers = closers
        grace = DEFAULT_CLOSER_GRACE if timeout is None else min(timeout, DEFAULT_CLOSER_GRACE)
        deadline = time.monotonic() + grace
        for closer in closers:
            closer.join(max(0.0, deadline - time.monotonic()))

    def _close_if_owed(self) -> None:
        """Closes the sink exactly once, and only once the drain thread has ended.

        Every exit from :meth:`shutdown` that may close comes through here — the expired one
        deliberately does not, since the thread is still using the sink — so the decision is
        made in one place under one lock. Two concurrent ``shutdown()`` calls are what needs
        it, and ``atexit`` plus user code calling it at once is documented as normal.

        ``is_alive()`` is the safety condition rather than a heuristic: it reads ``False`` only
        after ``_run`` has returned, so the sink is provably out of use *by the worker*.

        The close runs to completion, inline, and is deliberately **not** bounded — which leaves
        one honest gap. SPEC-028 made ``close()`` take the sink's emit lock, so an application
        thread on the orphan path can hold that lock inside a driver call with no timeout of its
        own and delay this past ``shutdown``'s budget. Running the close on a joinable daemon
        thread was tried and reverted: at interpreter exit the daemon is killed wherever it has
        reached, which for ``SQLiteSink`` is between ``commit()`` and ``close()`` — turning the
        leaked handle SPEC-027 FR-004 accepts into the partial write it was avoiding. It also
        could not tell a slow-but-successful close from a stuck one, so it reported
        ``ShutdownTimeout`` and "left open" for closes that had in fact completed, latching
        SPEC-019's alert term on a healthy shutdown. A wrong signal is worse than a slow one.
        The residual delay is recorded in ``architecture.md`` §13 rather than papered over.

        The close runs outside the lock, because it can reach ``_diag`` and a wedged console must
        not stall a lock :meth:`submit` also takes.

        Args:
          None.

        Returns:
          None.

        Raises:
          BaseException: Whatever the sink's ``close`` raised that is not an ``Exception``.
            ``_close_sink`` absorbs ``Exception`` but lets a ``KeyboardInterrupt`` or
            ``SystemExit`` through to the caller (SPEC-025 FR-004).
        """
        with self._lock:
            if self._sink_closed or self._thread.is_alive():
                return
            self._sink_closed = True
        self._close_sink()

    def _close_sink(self) -> None:
        """Closes the sink, absorbing a failure.

        This runs after the join, so everything queued has already been drained and emitted:
        what is lost here is the sink's own cleanup, not events.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        try:
            self.sink.close()
        except Exception as exc:
            _diag.absorbed("closing the sink", exc, "it may still hold its resources")

    def _queued_or_unknown(self) -> int:
        """Returns queued items, or zero where the platform does not implement ``qsize``.

        Items, not events: the queue holds one entry per submitted span plus any
        flush/shutdown marker.

        Args:
          None.

        Returns:
          The queue size, or 0 when it cannot be read.

        Raises:
          None. A diagnostic must not be the reason the diagnosis is lost.
        """
        try:
            return self._queue.qsize()
        except Exception:
            return 0

    def _run(self) -> None:
        """Runs the drain loop, recording whatever terminates it (SPEC-019 FR-001).

        :meth:`_emit` already absorbs an ``Exception`` from the sink, so anything reaching this
        handler has ended the only thread that delivers — and CPython's thread bootstrap
        discards a ``SystemExit`` without even a traceback, which is why the catch is
        ``BaseException``. It records and exits; looping onward past a ``KeyboardInterrupt``
        would be a worse failure than the one this prevents.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        pending: list[list[dict[str, object]]] = []
        try:
            self._drain(pending)
        except BaseException as exc:
            self._terminal_failure(exc, len(pending))
            self._release_waiters()

    def _release_waiters(self) -> None:
        """Answers every ``flush()`` marker still queued, so no caller waits out its timeout.

        A ``BaseException`` from the main loop skips :meth:`_final_drain` entirely, which is
        where queued markers are normally answered, leaving a waiter to sit for its full
        timeout on a thread that is never coming back. The markers are read out of the queue
        rather than consumed, because the queued event-lists are the evidence
        ``health().queued`` and the terminal line report; each keeps its pessimistic
        ``delivered``, which is the truth here.

        It is called from two places. The terminal-failure path is the original one. The clean
        :meth:`shutdown` path was added because that same enqueue-after-the-drain race happens
        there too, and hurts more: measured stranding a marker in 13 of 400 shutdowns raced
        against a ``flush()`` under load, where the caller sat out its whole timeout — and
        ``flush(timeout=None)``, which the API documents as supported, waits forever rather
        than too long. The marker keeps its pessimistic ``delivered``, which is the honest
        answer: the drain that would have carried it is gone.

        One residual race, stated rather than papered over: a ``flush()`` that passed its
        liveness check microseconds before *this* sweep can still enqueue a marker after it,
        and that one waits out its timeout — then returns False, which is correct either way.
        A marker left queued is also still counted by ``health().queued``, which describes
        submissions; removing it would mean deleting a specific item, which ``Queue`` has no
        public way to do, and the read above is the access ``architecture.md`` §13 sanctions.

        The reliance on ``queue.Queue``'s private ``mutex`` and ``queue`` is deliberate and is
        recorded in ``architecture.md`` §13 Known Constraints (SPEC-031 FR-005): there is no
        public "inspect without consuming", and the draining alternative would destroy the
        evidence the terminal-failure line reports. A CPython change would surface as the test
        that exercises this against a mixed queue, rather than as waiters silently timing out.

        Args:
          None.

        Returns:
          None.

        Raises:
          None. This runs after the record and the stderr line, neither of which may be lost.
        """
        try:
            with self._queue.mutex:
                markers = [i for i in self._queue.queue if isinstance(i, _FlushMarker)]
            for marker in markers:
                marker.event.set()
        except Exception:
            pass

    def _terminal_failure(self, exc: BaseException, undrained: int) -> None:
        """Records the drain loop's terminal exit, then announces it (FR-001, FR-002).

        Recording precedes announcing: stderr may be closed or wedged, and unlike the overflow
        warning this line is written exactly once and cannot be re-emitted later. The
        exception's type is reported and its message is not — the rule ``_diag`` now applies to
        every line the library writes (SPEC-029), and the reason this site had it first.

        The announcement is an :func:`~log_foundry._diag.absorbed` rather than a fourth kind of
        line, since the thread's death is an exception this method caught and did not
        propagate. The count reports what was in hand and what was queued behind it (SPEC-021
        FR-002), because held alone under-reads the loss: nothing will drain the queue either.
        The queued figure is items rather than event-lists and says so, making it a floor on
        what was lost, which is the useful direction.

        Args:
          exc: The exception that ended the drain thread.
          undrained: How many event-lists the loop still held in hand.

        Returns:
          None.

        Raises:
          None.
        """
        with self._lock:
            self.stopped_reason = type(exc).__name__
        try:
            queued: object = self._queue.qsize()
        except Exception:
            queued = "?"
        _diag.absorbed(
            "draining the log queue",
            exc,
            f"worker thread stopped; {undrained} undrained event-list(s) held and {queued} "
            f"queued item(s) undelivered, nothing further will be delivered",
        )

    def _drain(self, pending: list[list[dict[str, object]]]) -> None:
        """Accumulates event-lists and emits a batch on the count or time trigger.

        A flush marker emits immediately, ignoring both triggers, because a caller who asked
        for a flush is not interested in the batching policy — and that branch returns to the
        top of the loop, since a marker falling through to the append below would be treated as
        a list of events and handed to ``sink.emit``, killing this thread. The window is
        advanced even when idle: otherwise the timeout collapses to zero and ``get`` busy-spins
        a core.

        The shutdown sentinel **breaks** rather than falling through to the loop condition.
        :meth:`shutdown` queues it before setting ``_stop``, so a thread that takes it may find
        ``_stop`` still clear; continuing would re-enter ``get`` and block for another
        ``flush_interval``, which is a slow shutdown rather than a prompt one. Leaving
        immediately is safe because the only thing after the loop is :meth:`_final_drain`, which
        collects whatever is still queued — the sentinel is a wake-up, never a fence.

        Args:
          pending: The accumulator owned by :meth:`_run`, which reports its size on a terminal
            failure. It is mutated in place rather than rebound, so that count is accurate.

        Returns:
          None.

        Raises:
          Exception: Whatever the queue or a final drain raises; :meth:`_run` is the guard.
        """
        last_flush = time.monotonic()
        while not self._stop.is_set():
            timeout = max(0.0, self.flush_interval - (time.monotonic() - last_flush))
            try:
                item = self._queue.get(timeout=timeout)
            except queue.Empty:
                item = None
            if isinstance(item, _FlushMarker):
                try:
                    self._emit_pending(pending)
                    item.delivered = self._nothing_lost_since(item)
                finally:
                    last_flush = time.monotonic()
                    item.event.set()
                continue
            if item is _SHUTDOWN:
                break
            if item is not None:
                pending.append(cast("list[dict[str, object]]", item))
            now = time.monotonic()
            if len(pending) >= self.batch_size or now - last_flush >= self.flush_interval:
                self._emit_pending(pending)
                last_flush = now
        self._final_drain(pending)

    def _emit_pending(self, pending: list[list[dict[str, object]]]) -> None:
        """Emits the pending event-lists if there are any, then clears them.

        The two always go together.

        Args:
          pending: The accumulated event-lists, cleared in place.

        Returns:
          None.

        Raises:
          Exception: Whatever :meth:`_emit` does not absorb.
        """
        if pending:
            self._emit(pending)
            pending.clear()

    def _nothing_lost_since(self, marker: _FlushMarker) -> bool:
        """Reports whether any batch was abandoned while a marker was outstanding (FR-001).

        ``failed_batches`` moves exactly once per abandoned batch, so comparing it against the
        marker's stamp answers "was anything lost while this flush was in flight", which is a
        stronger question than "did my emit succeed": a marker whose own emit found nothing
        pending still reports a loss another emit incurred while it waited its turn. It is
        deliberately not a running "has anything ever failed" flag, which would make every
        empty flush after a single bad batch report a failure it did not incur.

        Args:
          marker: The marker being answered.

        Returns:
          True when nothing was abandoned since the marker was stamped.

        Raises:
          None.
        """
        with self._lock:
            return self.failed_batches == marker.seen_failures

    def _final_drain(self, pending: list[list[dict[str, object]]]) -> None:
        """Pulls anything still queued on stop and emits the tail as one final batch.

        The marker guard is a second copy of :meth:`_drain`'s and needs the same exclusion.
        Markers are answered after the final emit, so a ``flush()`` that raced ``shutdown()``
        is answered by the drain that carried its events, with that drain's outcome. They are
        set in a ``finally`` so a ``BaseException`` from the final emit cannot strand a waiter
        for its whole timeout.

        Args:
          pending: The accumulated event-lists, extended with whatever the queue still holds.

        Returns:
          None.

        Raises:
          BaseException: Whatever the final emit raises; :meth:`_run` is the guard.
        """
        markers: list[_FlushMarker] = []
        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break
            if isinstance(item, _FlushMarker):
                markers.append(item)
                continue
            if item is not None and item is not _SHUTDOWN:
                pending.append(cast("list[dict[str, object]]", item))
        try:
            self._emit_pending(pending)
            for marker in markers:
                marker.delivered = self._nothing_lost_since(marker)
        finally:
            for marker in markers:
                marker.event.set()

    def _emit(self, event_lists: list[list[dict[str, object]]]) -> None:
        """Flattens queued per-span event-lists into one batch and emits it, retrying.

        A failing ``sink.emit`` is retried up to ``max_retries`` times; past that the batch is
        abandoned with a counted warning and draining continues, so a broken sink never crashes
        the worker thread or the app (FR-002, FR-003). The backoff waits on the stop event, so
        a failing sink cannot stall the drain past a few quick tries during shutdown, and the
        warning goes through ``_diag`` so a broken stderr cannot kill the thread and cost every
        batch after it (SPEC-029 FR-003).

        ``max_retries`` is floored at zero so the loop always makes at least one attempt: a
        negative value otherwise skipped the emit entirely and discarded the batch with no
        attempt, no counter and nothing on stderr — reachable only by misconfiguration, but
        reachable.

        Args:
          event_lists: The accumulated per-span event-lists to flatten and emit.

        Returns:
          None. The outcome is not returned because ``failed_batches`` already moves exactly
          once per abandoned batch, and a second channel for the same fact could only disagree
          with the counter a waiting ``flush()`` is compared against (SPEC-021 FR-001).

        Raises:
          None.
        """
        batch = [event for events in event_lists for event in events]
        if not batch:
            return
        retries = max(self.max_retries, 0)
        for attempt in range(retries + 1):
            try:
                self.sink.emit(batch)
                return
            except Exception:
                if attempt >= retries:
                    with self._lock:
                        self.failed_batches += 1
                    _diag.lost(
                        "event", len(batch), f"batch abandoned after {retries + 1} emit attempts"
                    )
                    return
                self._stop.wait(min(0.01 * (2**attempt), 0.5))
