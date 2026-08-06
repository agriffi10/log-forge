"""Background flush worker — non-blocking span delivery (arch §9, guide Phase 9).

A finished span used to flush inline, blocking the decorated function on ``sink.emit``. The
``Worker`` moves that off the hot path: :meth:`submit` is a fast, in-process handoff to a
bounded queue drained by a daemon thread that batches events (by count *and* time) into a
single ``sink.emit`` call. A slow or down sink can never back-pressure the app — the queue is
bounded and overflow is dropped-newest with a counter (arch §9). Emit failures are retried
with backoff; a graceful :meth:`shutdown` drains the queue, emits the tail, and closes the
sink so buffered events survive process exit.

Two drains, deliberately distinct (SPEC-013): :meth:`shutdown` is terminal — it stops the thread
and closes the sink, and the worker never comes back — while :meth:`flush` drains on demand and
leaves everything running. A process that is frozen rather than exited (a serverless handler
between invocations) needs the second, because it will be asked to log again.

This module owns *delivery mechanics only*; it receives already-built event dicts and knows
nothing about spans or context (the same dumbness that makes sinks swappable).
"""

from __future__ import annotations

import queue
import threading
import time
from typing import TYPE_CHECKING, NamedTuple, cast

from log_foundry import _diag

if TYPE_CHECKING:
    from log_foundry.sinks.base import Sink, SinkLosses

__all__ = ["Health", "Worker"]

# Sentinel enqueued by shutdown() to wake a worker blocked in queue.get() so it stops promptly
# instead of waiting out the flush_interval. It is never emitted.
_SHUTDOWN = object()

DEFAULT_SHUTDOWN_TIMEOUT = 30.0
"""Seconds :meth:`Worker.shutdown` will wait for the drain thread (SPEC-027 FR-004).

Generous, because the ordinary case is a fast drain and expiring early would abandon events that
were about to be delivered. Bounded at all, because ``shutdown()`` runs from ``atexit`` and an
unbounded join there is a hung process — a sink blocked in a network call with a generous socket
timeout can still hold the thread even after FR-002 removed the common cause.
"""

# Queue overflow is a high-rate condition by nature: a line per dropped submission would be its
# own outage. Warn on the first drop, then every this-many-th (SPEC-017 FR-005).
_DROP_WARN_EVERY = 1000


def _bounded_seconds(timeout: float | None) -> str:
    """Render a shutdown timeout for a diagnostic without trusting its ``__str__``.

    ``timeout`` is the caller's, so it is a value the library does not control — the rule
    ``_diag.errno_of`` follows for an ``errno``. A non-number renders as ``"?"`` rather than
    whatever its ``__repr__`` chose to say (SPEC-029 FR-002).
    """
    try:
        return f"{float(timeout):g}s" if timeout is not None else "no timeout"
    except Exception:
        return "?"


class Health(NamedTuple):
    """A point-in-time snapshot of the worker's delivery counters (SPEC-017 FR-005).

    Attributes:
        queued: Submissions currently buffered. Approximate by nature — it is read without
            stopping the world, and briefly counts the internal flush/shutdown markers
            alongside real submissions.
        dropped: Submissions discarded because the queue was full (backpressure).
        failed_batches: Batches abandoned after the retry budget was spent.
        stopped_reason: The exception type name that terminated the drain thread, or ``None``
            if it never died — which is also what a live worker and a process that never
            logged report. Non-``None`` is categorically worse than the two counters above:
            they measure loss the worker absorbed and kept running through, this one means
            the worker is gone and nothing further will be delivered (SPEC-019 FR-003). Also
            ``"ShutdownTimeout"`` when a bounded :meth:`Worker.shutdown` expired before the
            drain finished (SPEC-027 FR-004) — the same thing to a reader, which is why it
            extends this vocabulary rather than adding a field.
        sink: The configured sink's own loss counters, or ``None`` when there is no worker or
            the sink reports nothing (SPEC-026 FR-003). Nested rather than folded into the two
            integers above because they count different things: ``dropped`` here is backpressure
            at *this* queue, ``dropped`` on the sink is an event that never reached the wire,
            and one number would make the remedies indistinguishable.
    """

    queued: int
    dropped: int
    failed_batches: int
    # Defaulted so the zeroed snapshot in `decorator._worker_health` — and any third-party
    # construction — keeps working unchanged.
    stopped_reason: str | None = None
    # Appended for the same reason `stopped_reason` was: attribute and index access to every
    # field that came before stay exactly as they were.
    sink: SinkLosses | None = None


class _FlushMarker:
    """A drain request travelling the queue in FIFO order (SPEC-013 FR-002).

    The mechanism follows from the queue being FIFO: everything submitted *before* ``flush()``
    was called is necessarily ahead of the marker, so by the time the worker dequeues it those
    events are either already emitted or sitting in ``pending``. The worker emits ``pending``,
    then sets ``event``. No lock, no inspection of queue internals, and no coordination with the
    batching triggers.

    Like ``_SHUTDOWN`` it is never emitted — but unlike ``_SHUTDOWN`` it carries state, so it is
    a class rather than a bare sentinel object.

    ``delivered`` carries the drain's *outcome* back to the waiter, not merely the fact that the
    marker was reached (SPEC-021 FR-001). It is written by the drain thread before ``event.set()``
    and read by the waiter after ``event.wait()`` returns, so the ``Event`` supplies the ordering
    and no further lock is needed.

    The outcome is computed from ``seen_failures`` — ``Worker.failed_batches`` as it stood when
    this marker was created, on the *caller's* thread — against the same counter when the marker is
    answered. Equal means nothing was abandoned while this flush was outstanding, which covers both
    the batch the marker forces and any batch another flush or a batching trigger emitted in the
    meantime. So every flush *outstanding* when a batch is abandoned reports it, including the ones
    whose own emit found nothing left to do because another marker's emit had just cleared and lost
    it — that case was the original false success.

    Two limits, both deliberate. A batch abandoned *before* the call is not in scope: that loss is
    already in ``failed_batches`` and on stderr, and folding it in would make every later empty
    flush report a failure it did not incur. It follows that two *concurrent* flushes need not
    agree — one stamped before the abandonment and one after are the outstanding case and the
    already-lost case respectively, and the race between them decides which is which. Reporting a
    past loss and not letting a past loss stick are the same property read in opposite directions;
    FR-001's "an empty drain is a successful one" chooses. ``health().failed_batches`` is what
    reports a loss regardless of when anyone asked.

    ``delivered`` starts ``False``: every path that answers a marker assigns it explicitly, so the
    default is read only when the drain thread died without computing an answer, where "I could not
    establish that this was delivered" is the honest reading. (The spec's Data Model sketched the
    default as ``True``, before the concurrent-flush case moved the empty-drain answer off it.)
    """

    __slots__ = ("delivered", "event", "seen_failures")

    def __init__(self, seen_failures: int) -> None:
        self.event = threading.Event()
        self.delivered = False
        self.seen_failures = seen_failures


class Worker:
    """Owns a bounded queue + daemon thread that batches and flushes events to ``sink``."""

    def __init__(
        self,
        sink: Sink,
        *,
        batch_size: int = 10,
        flush_interval: float = 1.0,
        max_queue: int = 10_000,
        max_retries: int = 3,
    ) -> None:
        self.sink = sink
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.max_retries = max_retries
        self.dropped = 0  # submissions dropped because the queue was full (backpressure)
        self.failed_batches = 0  # batches abandoned after exhausting retries (worker-thread only)
        self.stopped_reason: str | None = None  # set once if the drain loop dies (SPEC-019)
        self._queue: queue.Queue[object] = queue.Queue(maxsize=max_queue)
        self._stop = threading.Event()
        self._shutdown_done = False
        self._sink_closed = False  # a shutdown that expired still owes the close (SPEC-027)
        # Guards the `dropped` counter (incremented from any caller thread) and the shutdown
        # once-only flag (shutdown may be called concurrently by atexit and user code).
        self._lock = threading.Lock()
        self._offer_stop_signal()
        self._thread = threading.Thread(
            target=self._run, name="log-foundry-worker", daemon=True
        )
        self._thread.start()

    def _offer_stop_signal(self) -> None:
        """Give the sink this worker's shutdown event, if it advertises somewhere to put it.

        The dependency stays one-way (SPEC-027 FR-002): ``sinks`` must not import ``worker``, so
        the worker pushes rather than the sink pulling. Probed with ``hasattr``, the same
        optional-protocol shape SPEC-026 uses for ``losses()`` — a sink without the attribute
        simply never gets one and backs off uninterruptibly, exactly as before this spec.

        Total: a sink whose ``stop_signal`` is a read-only property, or whose ``__setattr__``
        objects, loses interruptibility rather than preventing the worker from starting.
        """
        try:
            if hasattr(self.sink, "stop_signal"):
                self.sink.stop_signal = self._stop
        except Exception as exc:
            _diag.absorbed(
                "handing the sink its stop signal", exc, "its backoff stays uninterruptible"
            )

    def submit(self, events: list[dict[str, object]]) -> None:
        """Hand a finished span's events to the worker. Non-blocking.

        Enqueues via ``put_nowait`` and returns immediately without touching the sink. When the
        queue is full, drops this submission (drop-newest) and counts it in ``dropped`` rather
        than blocking the caller (FR-001, FR-004), warning on a throttle (SPEC-017 FR-005).
        """
        try:
            self._queue.put_nowait(events)
        except queue.Full:
            with self._lock:
                self.dropped += 1
                total = self.dropped  # read under the lock: every value is produced exactly once
            # Written outside the lock deliberately. stderr can block on a slow reader, and
            # ``_lock`` also guards flush()/shutdown()'s once-only flag — holding it across a
            # blocking write would let a wedged console stall the drain path, not just other
            # submitters. Lines may therefore interleave out of order under concurrency; the
            # counts they carry are still exact.
            if total == 1 or total % _DROP_WARN_EVERY == 0:
                _diag.lost("submission", total, "log queue full; count is cumulative")

    def health(self) -> Health:
        """Snapshot the delivery counters (SPEC-017 FR-005, SPEC-019 FR-003). Never raises.

        Valid after :meth:`shutdown` — the counters are plain integers that outlive the thread,
        and the final drain consumes the queue, so ``queued`` reads 0 rather than a stale marker.
        The same applies to ``stopped_reason``: a terminal failure stays readable afterwards,
        since a caller finding a dead worker will usually call ``shutdown()`` next.
        """
        with self._lock:
            dropped, failed_batches = self.dropped, self.failed_batches
            stopped_reason = self.stopped_reason
        return Health(
            queued=self._queue.qsize(),
            dropped=dropped,
            failed_batches=failed_batches,
            stopped_reason=stopped_reason,
            sink=self._sink_losses(),
        )

    def _sink_losses(self) -> SinkLosses | None:
        """Read the configured sink's optional ``losses()``; ``None`` when it has none (FR-003).

        The probe and its guarantees live in ``sinks.base.read_losses`` — imported here rather
        than at module scope, keeping ``worker`` free of a runtime dependency on ``sinks`` the
        same way ``config`` does (the ``Sink`` Protocol itself is ``TYPE_CHECKING``-only).
        """
        from log_foundry.sinks.base import read_losses

        return read_losses(self.sink)

    def flush(self, timeout: float | None = 5.0) -> bool:
        """Drain everything submitted before this call through the sink, without stopping.

        Returns ``True`` once the worker has *delivered* them — ``False`` on timeout, on a worker
        already shut down or dead, on a queue too full to accept the marker, and when the drain
        carrying those events was abandoned after exhausting retries (SPEC-021 FR-001). That last
        case used to return ``True``: the drain had run, so the marker was answered regardless of
        what came of the emit. It is a false success exactly where ``flush()`` matters most — a
        serverless handler draining before the environment freezes has the return value as its
        only evidence the tail of the queue survived.

        The precise claim is *nothing was abandoned while this call was outstanding*: the batch
        this flush forces, and any batch another flush or a batching trigger emitted while its
        marker waited its turn. A batch abandoned **before** the call is not in scope — that loss
        is already in ``failed_batches`` and on stderr, and counting it here would make every
        later empty flush report a failure it did not incur. ``health()`` is the cumulative
        record; this is a verdict on one drain.

        Unlike :meth:`shutdown` the thread keeps running, the sink is **not** closed, and the
        once-only shutdown flag is untouched, so logging continues normally afterwards
        (SPEC-013 FR-002).
        """
        with self._lock:
            if self._shutdown_done:
                # Nothing will ever consume a marker now, so report the failure immediately
                # rather than make the caller wait out `timeout` for a drain that cannot happen.
                # A caller with an execution deadline would pay that wait for nothing (FR-003).
                return False
        if not self._thread.is_alive():
            return False
        with self._lock:
            # Stamped here, on the caller's thread, *before* the marker joins the queue: it is the
            # baseline the drain thread compares against, so anything abandoned from this moment
            # on is attributed to this flush. Reading it after the put would race the very emit
            # the flush is about to force.
            marker = _FlushMarker(self.failed_batches)
        # One deadline shared by the put and the wait: a caller asked for a bound on the whole
        # call, not on each half of it, so the two cannot add up to 2 * timeout.
        deadline = None if timeout is None else time.monotonic() + timeout
        try:
            # A *blocking* put, never put_nowait: on a full queue put_nowait would skip the
            # flush and return as though it had succeeded, which is the one outcome a flush must
            # never produce silently. A put that times out is reported as False.
            self._queue.put(marker, timeout=timeout)
        except queue.Full:
            return False
        remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
        if not marker.event.wait(remaining):
            return False  # timed out: the drain never happened, so it delivered nothing.
        return marker.delivered

    def shutdown(self, timeout: float | None = DEFAULT_SHUTDOWN_TIMEOUT) -> None:
        """Stop the thread, drain + emit everything queued, then ``close()`` the sink.

        **Bounded** (SPEC-027 FR-004). ``timeout`` caps how long the join may take; ``None``
        waits indefinitely, which is what this did unconditionally and is still available on
        request. On expiry it returns having stopped what it could, records
        ``stopped_reason="ShutdownTimeout"`` and writes one line — it does not kill the thread,
        which Python cannot do and which would leave a sink mid-write if it could.

        An expired shutdown does **not** close the sink. The drain thread may still be inside
        ``emit``, and closing a transport out from under it would turn a slow shutdown into a
        corrupt one. The cost is a leaked resource in a process that is exiting anyway, which is
        the cheaper of the two.

        That close is *deferred*, not abandoned: the once-only flag still short-circuits a second
        drain, but a later call finds the thread finished and closes the sink then. Without that,
        one expiry meant the sink was never closed for the life of the process — a caller who
        waited and tried again had no way to complete what they started.

        Idempotent: a second call is a no-op (FR-005). Registered via ``atexit`` by the
        decorator's lazy worker so a program that logs and exits immediately still flushes.

        **Total** (SPEC-025 FR-004). The close was unguarded, so a sink whose ``close()`` failed
        raised out of ``shutdown()`` — and out of the ``atexit`` handler, where CPython printed
        "Exception ignored in atexit callback" with a full traceback carrying the exception's
        message, which arch §6 keeps out of anything the library says about itself.

        The once-only flag deliberately stays *ahead* of the close rather than moving after it.
        (The close it guards is now owned by :meth:`_close_if_owed`, which is what makes the
        deferred case safe without weakening this.)
        Re-running a drain is not safe, and a second ``shutdown()`` retrying a close that already
        failed would call ``close()`` twice on a sink that may have partially released its
        resources. Idempotence is preserved; what changes is that the failure is announced rather
        than swallowed by the flag.
        """
        with self._lock:
            first = not self._shutdown_done
            self._shutdown_done = True
        if not first:
            # A previous call already drained; nothing here re-runs it. But if that call expired
            # before it could close the sink, the close is still owed.
            self._close_if_owed()
            return
        self._stop.set()
        try:
            self._queue.put_nowait(_SHUTDOWN)  # wake a blocked get() for a prompt stop
        except queue.Full:
            pass
        self._thread.join(timeout)
        if self._thread.is_alive():
            queued = self._queued_or_unknown()
            # Recorded before the line, as SPEC-019's terminal failure is: stderr may be wedged,
            # and this is written exactly once.
            with self._lock:
                if self.stopped_reason is None:
                    self.stopped_reason = "ShutdownTimeout"
            # "item", not "event", and for ``_terminal_failure``'s reason: the queue holds
            # event-*lists* plus any internal marker, so this counts submissions and sentinels,
            # not events. It is also a floor rather than a total — what the wedged thread still
            # holds in hand is not in the queue, and it may yet deliver some of it.
            _diag.lost(
                "item",
                queued,
                f"shutdown timed out after {_bounded_seconds(timeout)}; the sink is left open "
                f"because the worker thread is still using it",
            )
            return
        self._close_if_owed()

    def _close_if_owed(self) -> None:
        """Close the sink exactly once, and only once the drain thread has ended.

        Both exits from :meth:`shutdown` come through here, so the decision is made in one place
        under one lock. Two concurrent ``shutdown()`` calls are the case that needs it — the
        docstring above names a double ``close()`` on a partially-released sink as the thing this
        design avoids, and ``atexit`` plus user code calling it at once is documented as normal.
        A success path that closed unconditionally could race a deferred call that had just
        observed the thread end.

        ``is_alive()`` is the safety condition, not a heuristic: it reads ``False`` only after
        ``_run`` has returned, so the sink is provably out of use.

        The close itself runs **outside** the lock, because it can reach ``_diag`` and a wedged
        console must not stall a lock ``submit()`` also takes.
        """
        with self._lock:
            if self._sink_closed or self._thread.is_alive():
                return
            self._sink_closed = True
        self._close_sink()

    def _close_sink(self) -> None:
        """Close the sink, absorbing a failure. Called once, after the thread has ended."""
        try:
            self.sink.close()
        except Exception as exc:
            # After the join, so everything queued has already been drained and emitted: what is
            # lost here is the sink's own cleanup, not events.
            _diag.absorbed("closing the sink", exc, "it may still hold its resources")

    def _queued_or_unknown(self) -> int:
        """Queued **items**, or ``0`` where the platform does not implement ``qsize``.

        Items, not events: the queue holds one entry per submitted span plus any flush/shutdown
        marker. Never raises — a diagnostic must not be the reason the diagnosis is lost.
        """
        try:
            return self._queue.qsize()
        except Exception:
            return 0

    # -- worker thread ------------------------------------------------------------------

    def _run(self) -> None:
        """Drain loop: accumulate event-lists and emit a batch on the count/time trigger.

        Guarded end to end (SPEC-019 FR-001). ``_emit`` already absorbs an ``Exception`` from
        the sink, so anything reaching this handler has ended the only thread that delivers —
        and CPython's thread bootstrap discards a ``SystemExit`` without even a traceback, which
        is why the catch is ``BaseException`` rather than ``Exception``. It records and exits;
        looping onward past a ``KeyboardInterrupt`` would be a worse failure than the one this
        prevents.
        """
        pending: list[list[dict[str, object]]] = []
        try:
            self._drain(pending)
        except BaseException as exc:  # deliberately broad — see the docstring
            self._terminal_failure(exc, len(pending))
            self._release_waiters()

    def _release_waiters(self) -> None:
        """Answer every ``flush()`` marker still queued, so no caller waits out its timeout.

        A ``BaseException`` from the main loop skips ``_final_drain`` entirely, which is where
        queued markers are normally answered — leaving a waiter to sit for its full ``timeout``
        on a thread that is never coming back. FR-001 requires the waiter to be released on every
        path, and this is the last one.

        The markers are *read* out of the queue rather than consumed: the queued event-lists are
        the evidence ``health().queued`` and the terminal line report, and answering a waiter must
        not erase it. Each keeps its pessimistic ``delivered``, which is the truth here.

        Residual race, stated rather than papered over: a ``flush()`` that passed its liveness
        check microseconds before the thread died can still enqueue a marker after this sweep, and
        that one waits out its timeout — and then returns ``False``, which is correct either way.
        """
        try:
            with self._queue.mutex:  # a snapshot, so a marker cannot be missed mid-iteration
                markers = [i for i in self._queue.queue if isinstance(i, _FlushMarker)]
            for marker in markers:
                marker.event.set()
        except Exception:  # this runs *after* the record and the stderr line; neither may be lost
            pass

    def _terminal_failure(self, exc: BaseException, undrained: int) -> None:
        """Record the drain loop's terminal exit, then announce it (FR-001, FR-002).

        Recording precedes announcing: stderr may be closed or wedged, and unlike the overflow
        warning this line is written exactly once and cannot be re-emitted later, so the record
        must not be able to ride on it. The exception's *type* is reported and its message is not
        — the rule ``_diag`` now applies to every line the library writes (SPEC-029), and the
        reason this site had it first.

        The announcement is an :func:`~log_foundry._diag.absorbed`, not a fourth kind of line: the
        thread's death *is* an exception this method caught and did not propagate, and what it
        cost belongs in the detail. The count reports what was *in hand* and what was still
        *queued behind it* (SPEC-021 FR-002). Held alone under-reads the loss: nothing will drain
        the queue either, so an operator reading "1 undrained event-list(s)" could conclude far
        less was lost than was.

        The queued figure is "items", not "event-lists", and says so: like ``Health.queued`` it is
        read without stopping the world, so it counts any internal flush/shutdown marker sitting
        alongside real submissions, and a producer thread can add to the queue between the death
        and the read. It is a floor on what was lost, which is the useful direction.
        """
        with self._lock:
            self.stopped_reason = type(exc).__name__
        try:
            # In its own guard, and after the record: ``qsize()`` is not guaranteed on every
            # platform's queue, and a diagnostic must not be the reason the diagnosis is lost.
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
        """The drain loop proper. ``pending`` is owned by :meth:`_run`, which reports its size.

        It is mutated in place rather than rebound so the terminal handler sees what was still
        in hand; a local ``pending = []`` here would leave that count reading zero.
        """
        last_flush = time.monotonic()
        while not self._stop.is_set():
            timeout = max(0.0, self.flush_interval - (time.monotonic() - last_flush))
            try:
                item = self._queue.get(timeout=timeout)
            except queue.Empty:
                item = None
            if isinstance(item, _FlushMarker):
                # Drain on demand: emit immediately, ignoring both the batch_size and the
                # flush_interval trigger — a caller who asked for a flush is not interested in
                # the batching policy. Note this branch *returns to the top of the loop*: the
                # marker must never fall through to the append below, where it would be treated
                # as a list of events and handed to sink.emit, killing this thread.
                try:
                    self._emit_pending(pending)
                    item.delivered = self._nothing_lost_since(item)
                finally:
                    # Signal even if the emit died, so a waiter is released rather than left to
                    # wait out its timeout on a thread that is no longer running. A failing flush
                    # therefore returns False promptly rather than at the caller's timeout, and
                    # an emit that died leaves the marker's pessimistic default standing.
                    last_flush = time.monotonic()
                    item.event.set()
                continue
            if item is not None and item is not _SHUTDOWN:
                pending.append(cast("list[dict[str, object]]", item))
            now = time.monotonic()
            if len(pending) >= self.batch_size or now - last_flush >= self.flush_interval:
                self._emit_pending(pending)
                # Advance the window even when idle (pending empty). Otherwise last_flush never
                # moves while the queue is empty, timeout collapses to 0.0, and get(timeout=0.0)
                # busy-spins a core. Resetting it lets the next get() block a full interval.
                last_flush = now
        self._final_drain(pending)

    def _emit_pending(self, pending: list[list[dict[str, object]]]) -> None:
        """Emit ``pending`` if there is any, then clear it. The two always go together."""
        if pending:
            self._emit(pending)
            pending.clear()

    def _nothing_lost_since(self, marker: _FlushMarker) -> bool:
        """Whether any batch was abandoned while ``marker`` was outstanding (SPEC-021 FR-001).

        ``failed_batches`` moves exactly once per abandoned batch, so comparing it against the
        marker's stamp answers "was anything lost while this flush was in flight" — which is the
        question ``flush()`` asks, and a stronger one than "did *my* emit succeed". A marker whose
        own emit found nothing pending still reports the loss if another flush's emit, or a
        batching trigger's, abandoned a batch while it waited its turn.

        Deliberately *not* a running "has anything ever failed" flag. That would make every empty
        flush after a single bad batch report a failure it did not incur, contradicting the rule
        that an empty drain is a successful one.

        Called on the drain thread; the lock is for the counter's other readers, and is held only
        for the read (``shutdown()`` releases it before ``join()``, so it cannot deadlock).
        """
        with self._lock:
            return self.failed_batches == marker.seen_failures

    def _final_drain(self, pending: list[list[dict[str, object]]]) -> None:
        """On stop, pull anything still queued and emit the tail as one final batch."""
        markers: list[_FlushMarker] = []
        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break
            if isinstance(item, _FlushMarker):
                # This guard is a second copy of _drain's and needs the same exclusion. Markers
                # are answered *after* the final emit below, so a flush() that raced shutdown()
                # is answered by the drain that carried its events — with that drain's outcome.
                markers.append(item)
                continue
            if item is not None and item is not _SHUTDOWN:
                pending.append(cast("list[dict[str, object]]", item))
        try:
            self._emit_pending(pending)
            for marker in markers:
                marker.delivered = self._nothing_lost_since(marker)
        finally:
            # In a ``finally`` so a ``BaseException`` from the final emit cannot strand a waiter
            # for its whole timeout — the markers are released carrying their pessimistic
            # default, which is the truth about a batch that died with this thread.
            for marker in markers:
                marker.event.set()

    def _emit(self, event_lists: list[list[dict[str, object]]]) -> None:
        """Flatten queued per-span event-lists into one batch and emit, retrying with backoff.

        A failing ``sink.emit`` is retried up to ``max_retries`` times; past that the batch is
        abandoned with a counted warning and draining continues, so a broken sink never crashes
        the worker thread or the app (FR-002, FR-003).

        The outcome is not returned: ``failed_batches`` already moves exactly once per abandoned
        batch, and that counter is what a waiting ``flush()`` is compared against (SPEC-021
        FR-001), so a second channel for the same fact could only disagree with it.

        ``max_retries`` is floored at zero so the loop always makes at least one attempt. A
        negative value otherwise skipped the emit entirely and discarded the batch with no
        attempt, no counter and nothing on stderr — a silent loss of exactly the kind this arc of
        specs exists to remove, reachable only by misconfiguration but reachable.
        """
        batch = [event for events in event_lists for event in events]
        if not batch:
            return
        retries = max(self.max_retries, 0)
        for attempt in range(retries + 1):
            try:
                self.sink.emit(batch)
                return
            except Exception:  # any sink failure must not kill the worker thread
                if attempt >= retries:
                    # Under the lock so a concurrent health() sees a coherent snapshot rather
                    # than a half-updated pair. No deadlock: shutdown() releases before join().
                    with self._lock:
                        self.failed_batches += 1
                    # Through ``_diag`` so the write is guarded: unguarded, a broken stderr raised
                    # out of here, through ``_drain``, into ``_run``'s handler, and the drain
                    # thread died for good — a diagnostic about one lost batch costing every batch
                    # after it (SPEC-029 FR-003).
                    _diag.lost(
                        "event", len(batch), f"batch abandoned after {retries + 1} emit attempts"
                    )
                    return
                # Backoff between attempts; _stop.wait returns at once during shutdown, so a
                # failing sink can't stall the drain past max_retries quick tries.
                self._stop.wait(min(0.01 * (2**attempt), 0.5))
