"""Background flush worker — non-blocking span delivery (arch §9, guide Phase 9).

A finished span used to flush inline, blocking the decorated function on ``sink.emit``. The
``Worker`` moves that off the hot path: :meth:`submit` is a fast, in-process handoff to a
bounded queue drained by a daemon thread that batches events (by count *and* time) into a
single ``sink.emit`` call. A slow or down sink can never back-pressure the app — the queue is
bounded and overflow is dropped-newest with a counter (arch §9). Emit failures are retried
with backoff; a graceful :meth:`shutdown` drains the queue, emits the tail, and closes the
sink so buffered events survive process exit.

This module owns *delivery mechanics only*; it receives already-built event dicts and knows
nothing about spans or context (the same dumbness that makes sinks swappable).
"""

from __future__ import annotations

import queue
import sys
import threading
import time
from typing import cast

from log_foundry.sinks.base import Sink

__all__ = ["Worker"]

# Sentinel enqueued by shutdown() to wake a worker blocked in queue.get() so it stops promptly
# instead of waiting out the flush_interval. It is never emitted.
_SHUTDOWN = object()


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
        than blocking the caller (FR-001, FR-004).
        """
        try:
            self._queue.put_nowait(events)
        except queue.Full:
            with self._lock:
                self.dropped += 1

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
        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break
            if item is not None and item is not _SHUTDOWN:
                pending.append(cast("list[dict[str, object]]", item))
        if pending:
            self._emit(pending)

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
            except Exception:  # noqa: BLE001 — any sink failure must not kill the worker thread
                if attempt >= self.max_retries:
                    self.failed_batches += 1
                    sys.stderr.write(
                        f"log-foundry: abandoned a batch of {len(batch)} event(s) after "
                        f"{self.max_retries + 1} failed emit attempts\n"
                    )
                    return
                # Backoff between attempts; _stop.wait returns at once during shutdown, so a
                # failing sink can't stall the drain past max_retries quick tries.
                self._stop.wait(min(0.01 * (2**attempt), 0.5))
