"""log_foundry — consistent, structured logs per decorated function call."""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _dist_version

from log_foundry import _fork
from log_foundry.api import critical, debug, error, info, set_baggage, warning
from log_foundry.config import Config, configure, get_config
from log_foundry.context import (
    current_baggage_header,
    current_trace_context,
    current_traceparent,
    get_baggage,
    reset_context,
)
from log_foundry.decorator import continue_trace, trace
from log_foundry.results import ContinueResult, FlushResult
from log_foundry.sinks.base import Sink, SinkDeliveryError, SinkLosses, read_losses
from log_foundry.worker import DEFAULT_SHUTDOWN_TIMEOUT, Health

try:
    __version__ = _dist_version("log-foundry")
except PackageNotFoundError:
    __version__ = "0.0.0"

_fork.install()


def flush(timeout: float | None = 5.0) -> FlushResult:
    """Drains buffered events through the sink without closing it.

    This is the drain for a process that is frozen rather than exited — an AWS Lambda handler
    must drain before it returns, but will be invoked again on the same warm container. Call it
    in a ``finally``: the invocation worth logging is the one that failed. Unlike
    :func:`shutdown` the background worker stays alive and the sink stays open, so logging
    continues normally afterwards.

    Args:
      timeout: Seconds to wait for the drain. ``None`` waits indefinitely, which is unsafe in
        any environment with an execution deadline — it converts "some logs were lost" into
        "the invocation timed out".

    Returns:
      A :class:`FlushResult`. Truthy when the events submitted before this call reached the
      sink — so a truthy result means they were delivered, not merely that a drain took place.
      Falsy carries a ``reason`` naming which outcome occurred: ``"timed-out"``, ``"retired"``,
      ``"thread-died"``, ``"queue-full"``, ``"abandoned"`` or ``"sink-flush"`` — the last meaning
      the queue drained but the sink could not empty its own client buffer (SPEC-036 FR-002). It is falsy rather than ``False``:
      ``if flush():`` is unchanged, but ``flush() is True`` can no longer hold, which is why the
      type had to change before ``1.0.0`` rather than after (SPEC-034 FR-007). Events submitted
      concurrently by another thread may or may not be included, since the caller cannot have
      meant those, and a batch lost before the call belongs to :func:`health`.

    Raises:
      None.
    """
    from log_foundry.decorator import _flush_worker

    return _flush_worker(timeout)


def health() -> Health:
    """Snapshots the background worker's delivery counters (SPEC-017 FR-005).

    A non-zero ``dropped`` means the queue filled and submissions were discarded to keep your
    code non-blocking, and a non-zero ``failed_batches`` means a sink stayed broken through the
    whole retry budget — both are losses the library absorbs on purpose, and this is how you
    notice them. A non-``None`` ``stopped_reason`` is worse than either: the background thread
    died on that exception type, so nothing further will be delivered at all (SPEC-019). A
    ``retired`` worker still being handed events is the same total loss arrived at from the
    other direction — someone called :func:`shutdown` in a process that logs again (SPEC-030)::

        h = log_foundry.health()
        if h.dropped or h.failed_batches or h.stopped_reason or h.incomplete_swaps or (
            h.sink and (h.sink.dropped or h.sink.failed)
        ) or (h.retired and h.submitted_after_shutdown) or h.orphan_lost or h.in_span_lost:
            ...  # raise an alert; logs were silently lost

    ``orphan_lost`` and ``in_span_lost`` are the two terms that do **not** describe the worker
    (SPEC-036 FR-003). A level call made with no active span emits on the caller's own thread, and
    an event that cannot be *built* never reaches a queue at all — so every other field here
    describes machinery those two losses never touched, and a process that only logs outside a
    span read all zeros over total loss until they existed. They stay separate because one can
    mean the destination or the data and the other can only mean the data; their sum is a number
    nobody can act on.

    ``retired`` alone is not a fault — a process that shuts down and then stops logging is
    doing the right thing, which is why it is paired with the count rather than alerted on.
    ``closing_sinks`` is deliberately absent for the same kind of reason: it is briefly non-zero
    during a perfectly healthy sink swap, so alerting on a single reading would fire on correct
    use. It is a gauge to watch over time, not a term in this test.

    Args:
      None.

    Returns:
      The snapshot: ``queued``, ``dropped``, ``failed_batches``, ``stopped_reason``, ``sink``,
      ``retired``, ``submitted_after_shutdown`` and ``incomplete_swaps``. ``retired`` says
      :func:`shutdown` was called, and ``submitted_after_shutdown`` counts events accepted
      afterwards, which are queued where nothing will drain them — non-zero together, that is
      the ``shutdown()``-per-invocation mistake, and the remedy is :func:`flush`.
      ``incomplete_swaps`` counts late ``configure(sink=...)`` calls whose drain of the
      previous sink could not be confirmed, leaving that sink open. ``closing_sinks`` is the
      odd one out — a live gauge rather than a counter, reporting how many swapped-out sinks
      are inside ``close()`` right now, so a destination stuck there is visible at all; it
      falls back to zero on its own, and only a *persistently* non-zero reading is a fault.
      ``sink`` is the configured sink's own
      :class:`~log_foundry.sinks.base.SinkLosses` — loss the sink absorbed rather than the
      worker (SPEC-026) — and is ``None`` when no worker exists or the sink reports nothing.
      Note ``sink`` describes whichever sink is live now, so a swap takes the previous sink's
      absorbed losses out of the snapshot with it.
      Its ``dropped`` is not the worker's: the worker's is backpressure at the queue, the
      sink's is an event that never reached the wire, and the stderr line names which. Its
      ``failed`` is an upper bound on loss rather than a count of it, since a sink that raises
      on total failure counts the attempt and hands the batch back for the worker to retry. A
      process that has never logged has no worker, and asking does not create one — the
      snapshot is simply zeroed, except for ``retired``, which stays truthful even for a
      process that only ever logged outside a span and so built no worker at all (SPEC-031
      FR-006). Valid after :func:`shutdown`.

    Raises:
      None.
    """
    from log_foundry.decorator import _worker_health

    return _worker_health()


def shutdown(timeout: float | None = DEFAULT_SHUTDOWN_TIMEOUT) -> None:
    """Flushes buffered events and closes the sink, blocking until drained.

    This is terminal — the worker does not come back — and idempotent. Do not call it
    per-invocation in a serverless handler: the first invocation on a warm container would log
    and every later one would silently log nothing; use :func:`flush` there. It is also
    registered via ``atexit``, so call it explicitly only when you want to be certain the tail
    of the queue reached the sink before a fast process exit (SPEC-004 FR-005).

    Logging afterwards is **accepted and undeliverable**: those events are queued and nothing
    will drain them. It is not silent any more — :func:`health` then reports ``retired`` with a
    non-zero ``submitted_after_shutdown``, and the first such submission writes one stderr line
    (SPEC-030). That pair is the reading that catches the mistake above.

    A process that only ever logged **outside** a span built no worker, and this used to be a
    no-op there, leaving the sink open forever: every event lost on a sink whose ``close()`` is
    what delivers them, the flush and the resource on a synchronous one. It now closes that
    sink — exactly once, and without creating a worker to do it — and ``health().retired``
    reads ``True`` afterwards rather than staying vacuously ``False`` (SPEC-031 FR-006). A
    later level call reaches a closed sink, so a sink that guards its own post-close state
    refuses it and one stderr line is written; a stateless one such as ``StdoutSink`` still
    accepts it (SPEC-032).

    Args:
      timeout: Seconds bounding the wait for the background thread and, carved from the same
        budget, a short grace for any sink still closing after a late ``configure(sink=...)``
        (SPEC-027 FR-004, SPEC-030 FR-003). ``None``
        waits indefinitely, which is what this did unconditionally before and is still
        available on request, but is unsafe anywhere with an execution deadline — ``atexit`` is
        one such place, where a sink blocked in a network call would hold the process open. An
        expired shutdown reports a ``stopped_reason`` of ``"ShutdownTimeout"`` and leaves the
        sink open, since the drain thread may still be inside ``emit``.

    Returns:
      None.

    Raises:
      None.
    """
    from log_foundry.decorator import _shutdown_worker

    _shutdown_worker(timeout)


__all__ = [
    "DEFAULT_SHUTDOWN_TIMEOUT",
    "Config",
    "ContinueResult",
    "FlushResult",
    "Health",
    "Sink",
    "SinkDeliveryError",
    "SinkLosses",
    "__version__",
    "configure",
    "continue_trace",
    "critical",
    "current_baggage_header",
    "current_trace_context",
    "current_traceparent",
    "debug",
    "error",
    "flush",
    "get_baggage",
    "get_config",
    "health",
    "info",
    "read_losses",
    "reset_context",
    "set_baggage",
    "shutdown",
    "trace",
    "warning",
]
