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
    """

    queued: int
    dropped: int
    failed_batches: int


class _FlushMarker:
    """A drain request travelling the queue in FIFO order (SPEC-013 FR-002).

    The mechanism follows from the queue being FIFO: everything submitted *before* ``flush()``
    was called is necessarily ahead of the marker, so by the time the worker dequeues it those
    events are either already emitted or sitting in ``pending``. The worker emits ``pending``,
    then sets ``event``. No lock, no inspection of queue internals, and no coordination with the
    batching triggers.

    Like ``_SHUTDOWN`` it is never emitted — but unlike ``_SHUTDOWN`` it carries state, so it is
    a class rather than a bare sentinel object.
    """

    __slots__ = ("event",)

    def __init__(self) -> None:
        self.event = threading.Event()


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
        """Snapshot the delivery counters (SPEC-017 FR-005). Never raises.

        Valid after :meth:`shutdown` — the counters are plain integers that outlive the thread,
        and the final drain consumes the queue, so ``queued`` reads 0 rather than a stale marker.
        """
        with self._lock:
            dropped, failed_batches = self.dropped, self.failed_batches
        return Health(
            queued=self._queue.qsize(), dropped=dropped, failed_batches=failed_batches
        )

    def flush(self, timeout: float | None = 5.0) -> bool:
        """Drain everything submitted before this call through the sink, without stopping.

        Returns ``True`` once the worker has emitted them, ``False`` on timeout or when the
        worker has already been shut down. Unlike :meth:`shutdown` the thread keeps running, the
        sink is **not** closed, and the once-only shutdown flag is untouched, so logging
        continues normally afterwards (SPEC-013 FR-002).
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
        return marker.event.wait(remaining)

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
        """Drain loop: accumulate event-lists and emit a batch on the count/time trigger."""
        pending: list[list[dict[str, object]]] = []
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
                    if pending:
                        self._emit(pending)
                        pending = []
                finally:
                    # Signal even if the emit died, so a waiter is released rather than left to
                    # wait out its timeout on a thread that is no longer running.
                    last_flush = time.monotonic()
                    item.event.set()
                continue
            if item is not None and item is not _SHUTDOWN:
                pending.append(cast("list[dict[str, object]]", item))
            now = time.monotonic()
            if len(pending) >= self.batch_size or now - last_flush >= self.flush_interval:
                if pending:
                    self._emit(pending)
                    pending = []
                # Advance the window even when idle (pending empty). Otherwise last_flush never
                # moves while the queue is empty, timeout collapses to 0.0, and get(timeout=0.0)
                # busy-spins a core. Resetting it lets the next get() block a full interval.
                last_flush = now
        self._final_drain(pending)

    def _final_drain(self, pending: list[list[dict[str, object]]]) -> None:
        """On stop, pull anything still queued and emit the tail as one final batch."""
        markers: list[_FlushMarker] = []
        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break
            if isinstance(item, _FlushMarker):
                # This guard is a second copy of _run's and needs the same exclusion. Markers
                # are answered *after* the final emit below, so a flush() that raced shutdown()
                # still returns True — and truthfully: its events really did reach the sink.
                markers.append(item)
                continue
            if item is not None and item is not _SHUTDOWN:
                pending.append(cast("list[dict[str, object]]", item))
        if pending:
            self._emit(pending)
        for marker in markers:
            marker.event.set()

    def _emit(self, event_lists: list[list[dict[str, object]]]) -> None:
        """Flatten queued per-span event-lists into one batch and emit, retrying with backoff.

        A failing ``sink.emit`` is retried up to ``max_retries`` times; past that the batch is
        abandoned with a counted warning and draining continues, so a broken sink never crashes
        the worker thread or the app (FR-002, FR-003).
        """
        batch = [event for events in event_lists for event in events]
        if not batch:
            return
        for attempt in range(self.max_retries + 1):
            try:
                self.sink.emit(batch)
                return
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
                    return
                # Backoff between attempts; _stop.wait returns at once during shutdown, so a
                # failing sink can't stall the drain past max_retries quick tries.
                self._stop.wait(min(0.01 * (2**attempt), 0.5))
