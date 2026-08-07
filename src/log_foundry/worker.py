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

    ``stopped_reason`` and ``sink`` are defaulted and appended in that order, so the zeroed
    snapshot in ``decorator._worker_health`` — and any third-party construction — keeps
    working, and attribute and index access to every earlier field stays as it was.

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
    """

    queued: int
    dropped: int
    failed_batches: int
    stopped_reason: str | None = None
    sink: SinkLosses | None = None


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

        Args:
          events: The span's buffered events, submitted as one item.

        Returns:
          None.

        Raises:
          None.
        """
        try:
            self._queue.put_nowait(events)
        except queue.Full:
            with self._lock:
                self.dropped += 1
                total = self.dropped
            if total == 1 or total % _DROP_WARN_EVERY == 0:
                _diag.lost("submission", total, "log queue full; count is cumulative")

    def health(self) -> Health:
        """Snapshots the delivery counters (SPEC-017 FR-005, SPEC-019 FR-003).

        This stays valid after :meth:`shutdown`: the counters are plain integers that outlive
        the thread, and the final drain consumes the queue, so ``queued`` reads 0 rather than a
        stale marker. The same applies to ``stopped_reason``, since a caller finding a dead
        worker will usually call ``shutdown()`` next.

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
        return Health(
            queued=self._queue.qsize(),
            dropped=dropped,
            failed_batches=failed_batches,
            stopped_reason=stopped_reason,
            sink=self._sink_losses(),
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
        remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
        if not marker.event.wait(remaining):
            return False
        return marker.delivered

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
        with self._lock:
            first = not self._shutdown_done
            self._shutdown_done = True
        if not first:
            self._close_if_owed()
            return
        self._stop.set()
        try:
            self._queue.put_nowait(_SHUTDOWN)
        except queue.Full:
            pass
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
        self._close_if_owed()

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

        One residual race, stated rather than papered over: a ``flush()`` that passed its
        liveness check microseconds before the thread died can still enqueue a marker after
        this sweep, and that one waits out its timeout — then returns False, which is correct
        either way.

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
            if item is not None and item is not _SHUTDOWN:
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
