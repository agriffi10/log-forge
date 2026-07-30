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
import sys
import threading
import time
from typing import TYPE_CHECKING, NamedTuple, cast

if TYPE_CHECKING:
    from log_foundry.sinks.base import Sink

__all__ = ["Health", "Worker"]

# Sentinel enqueued by shutdown() to wake a worker blocked in queue.get() so it stops promptly
# instead of waiting out the flush_interval. It is never emitted.
_SHUTDOWN = object()

# Queue overflow is a high-rate condition by nature: a line per dropped submission would be its
# own outage. Warn on the first drop, then every this-many-th (SPEC-017 FR-005).
_DROP_WARN_EVERY = 1000


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
            the worker is gone and nothing further will be delivered (SPEC-019 FR-003).
    """

    queued: int
    dropped: int
    failed_batches: int
    # Defaulted so the zeroed snapshot in `decorator._worker_health` — and any third-party
    # construction — keeps working unchanged.
    stopped_reason: str | None = None


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

    It starts ``False``, not ``True``: every path that answers a marker assigns the outcome
    explicitly, so the default is only ever read when the drain thread died between releasing the
    waiter and computing an answer. "I could not establish that this was delivered" is the honest
    reading of that, and the empty-drain success it might otherwise stand in for is supplied by
    ``Worker._emit_pending`` instead. (The spec's Data Model sketched the default as ``True``,
    before the concurrent-flush case moved the empty-drain answer onto the worker.)
    """

    __slots__ = ("delivered", "event")

    def __init__(self) -> None:
        self.event = threading.Event()
        self.delivered = False


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
        # The outcome of the most recent emit, answering a flush() marker that finds nothing left
        # to drain (SPEC-021 FR-001). Drain-thread-only: written and read there, never elsewhere.
        self._last_delivered = True
        self._queue: queue.Queue[object] = queue.Queue(maxsize=max_queue)
        self._stop = threading.Event()
        self._shutdown_done = False
        # Guards the `dropped` counter (incremented from any caller thread) and the shutdown
        # once-only flag (shutdown may be called concurrently by atexit and user code).
        self._lock = threading.Lock()
        self._thread = threading.Thread(
            target=self._run, name="log-foundry-worker", daemon=True
        )
        self._thread.start()

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
                try:
                    sys.stderr.write(
                        f"log-foundry: log queue full, dropped {total} submission(s) so far\n"
                    )
                except Exception:  # submit() runs on the *caller's* thread, so an
                    # unwritable stderr (closed fd, broken pipe, daemonized process) would raise
                    # straight into the app. A diagnostic about dropped logs must never itself be
                    # the reason a decorated function fails. The counter is already recorded.
                    pass

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
        )

    def flush(self, timeout: float | None = 5.0) -> bool:
        """Drain everything submitted before this call through the sink, without stopping.

        Returns ``True`` once the worker has *delivered* them — ``False`` on timeout, on a worker
        already shut down or dead, on a queue too full to accept the marker, and when the drain
        carrying those events was abandoned after exhausting retries (SPEC-021 FR-001). That last
        case used to return ``True``: the drain had run, so the marker was answered regardless of
        what came of the emit. It is a false success exactly where ``flush()`` matters most — a
        serverless handler draining before the environment freezes has the return value as its
        only evidence the tail of the queue survived.

        "The drain carrying those events" is the precise claim: the answer comes from the emit
        that covered what was ahead of this marker, which may be one another flush() or a batching
        trigger already forced. It is not a verdict on every batch the worker has ever sent —
        ``health().failed_batches`` is the cumulative record.

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
        marker = _FlushMarker()
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

    def shutdown(self) -> None:
        """Stop the thread, drain + emit everything queued, then ``close()`` the sink.

        Idempotent: a second call is a no-op (FR-005). Registered via ``atexit`` by the
        decorator's lazy worker so a program that logs and exits immediately still flushes.
        """
        with self._lock:
            if self._shutdown_done:
                return
            self._shutdown_done = True
        self._stop.set()
        try:
            self._queue.put_nowait(_SHUTDOWN)  # wake a blocked get() for a prompt stop
        except queue.Full:
            pass
        self._thread.join()
        self.sink.close()

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
        must not be able to ride on it. The exception's *type* is reported and its message is
        not — a sink's exception text can carry event data, and arch §6 keeps caller data out of
        places it was not asked for (the same rule behind ``sanitize``'s type-name placeholder).
        """
        name = type(exc).__name__
        with self._lock:
            self.stopped_reason = name
        try:
            sys.stderr.write(
                f"log-foundry: worker thread stopped on {name}; {undrained} undrained "
                f"event-list(s), nothing further will be delivered\n"
            )
        except Exception:  # best-effort: the record above is what an operator reads.
            pass

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
                    item.delivered = self._emit_pending(pending)
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

    def _emit_pending(self, pending: list[list[dict[str, object]]]) -> bool:
        """Emit ``pending`` if there is any, and return the outcome a marker should report.

        The outcome is *recorded* (``_last_delivered``) rather than derived from this call alone,
        because a marker that arrives with ``pending`` already empty is not thereby successful —
        the events ahead of it in the FIFO were emitted by the most recent emit, so that emit's
        outcome is its answer (SPEC-021 FR-001). Without this, two concurrent ``flush()`` calls
        over the same events had the first emit-and-abandon them and the second, finding nothing
        left to do, report ``True``: the same false success this spec exists to remove, one
        interleaving further out. Found by the fresh-context review.

        Called only from the drain thread, which is also the only reader, so ``_last_delivered``
        needs no lock. It starts ``True``: a process that has never emitted has never lost
        anything, so its first flush is a successful empty drain.
        """
        if pending:
            # False *before* the call, not after: ``_emit`` absorbs an ``Exception``, but a
            # ``BaseException`` from a sink escapes it and would otherwise leave the previous
            # (possibly successful) outcome standing for a batch that died with this thread.
            self._last_delivered = False
            self._last_delivered = self._emit(pending)
            pending.clear()
        return self._last_delivered

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
            delivered = self._emit_pending(pending)
            for marker in markers:
                marker.delivered = delivered
        finally:
            # In a ``finally`` so a ``BaseException`` from the final emit cannot strand a waiter
            # for its whole timeout — the markers are released carrying their pessimistic
            # default, which is the truth about a batch that died with this thread.
            for marker in markers:
                marker.event.set()

    def _emit(self, event_lists: list[list[dict[str, object]]]) -> bool:
        """Flatten queued per-span event-lists into one batch and emit, retrying with backoff.

        A failing ``sink.emit`` is retried up to ``max_retries`` times; past that the batch is
        abandoned with a counted warning and draining continues, so a broken sink never crashes
        the worker thread or the app (FR-002, FR-003).

        Returns whether the batch reached the sink. ``False`` means the abandon path below — the
        same event that increments ``failed_batches`` — or, for a negative ``max_retries``, a loop
        that makes no attempt at all. :meth:`_emit_pending` records it for a caller-facing
        ``flush()`` (SPEC-021 FR-001); the batching triggers do nothing further with it, since a
        batch that failed its whole retry budget is gone.
        """
        batch = [event for events in event_lists for event in events]
        if not batch:
            return True  # an empty batch is delivered in the only sense available to it.
        for attempt in range(self.max_retries + 1):
            try:
                self.sink.emit(batch)
                return True
            except Exception:  # any sink failure must not kill the worker thread
                if attempt >= self.max_retries:
                    # Under the lock so a concurrent health() sees a coherent snapshot rather
                    # than a half-updated pair. No deadlock: shutdown() releases before join().
                    with self._lock:
                        self.failed_batches += 1
                    sys.stderr.write(
                        f"log-foundry: abandoned a batch of {len(batch)} event(s) after "
                        f"{self.max_retries + 1} failed emit attempts\n"
                    )
                    return False
                # Backoff between attempts; _stop.wait returns at once during shutdown, so a
                # failing sink can't stall the drain past max_retries quick tries.
                self._stop.wait(min(0.01 * (2**attempt), 0.5))
        # Reachable only for a negative ``max_retries``, where the loop makes no attempt at all.
        # The batch was not delivered, and saying so is more useful than asserting it can't happen.
        return False
