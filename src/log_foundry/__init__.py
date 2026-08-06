"""log_foundry — consistent, structured logs per decorated function call.

Public façade (module-function shape). Exposes configuration, the ``@trace`` decorator, the
``debug/info/warning/error/critical`` emitters, ``set_baggage``, the cross-process propagation
pair (``continue_trace`` to adopt an inbound context; ``current_traceparent`` /
``current_trace_context`` / ``current_baggage_header`` to publish this one), ``reset_context``
(clear both for a caller who opens no span), ``flush`` (drain and keep logging) and ``shutdown``
(drain, close the sink, and stop).
"""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _dist_version

from log_foundry.api import critical, debug, error, info, set_baggage, warning
from log_foundry.config import configure, get_config
from log_foundry.context import (
    current_baggage_header,
    current_trace_context,
    current_traceparent,
    reset_context,
)
from log_foundry.decorator import continue_trace, trace
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.worker import DEFAULT_SHUTDOWN_TIMEOUT, Health

try:
    # Distribution name ("log-foundry") differs from the import name ("log_foundry").
    __version__ = _dist_version("log-foundry")
except PackageNotFoundError:  # running from a source tree that isn't installed
    __version__ = "0.0.0"


def flush(timeout: float | None = 5.0) -> bool:
    """Drain buffered events through the sink without closing it.

    ``True`` means the events submitted before this call reached the sink; ``False`` means the
    drain carrying them was abandoned, or never completed. (Events submitted concurrently by
    another thread may or may not be included — the caller cannot have meant those.) Unlike
    :func:`shutdown` the background worker stays alive and the sink stays open, so logging
    continues normally afterwards.

    This is the drain for a process that is *frozen* rather than exited — an AWS Lambda
    handler must drain before it returns, but will be invoked again on the same warm
    container. Call it in a ``finally``: the invocation worth logging is the one that failed.

    ``timeout=None`` waits indefinitely, which is unsafe in any environment with an execution
    deadline — it converts "some logs were lost" into "the invocation timed out".

    Returns ``False`` if the drain did not complete within ``timeout``, if the worker has
    already been shut down or has died, or if a batch was abandoned while this call was
    outstanding (SPEC-021 FR-001) — a ``True`` means the events were delivered, not merely that
    a drain took place. A batch lost *before* the call is outside this call's window and belongs
    to :func:`health`, whose ``failed_batches`` is the cumulative record. Never raises.
    """
    from log_foundry.decorator import _flush_worker

    return _flush_worker(timeout)


def health() -> Health:
    """Snapshot the background worker's delivery counters (SPEC-017 FR-005). Never raises.

    Returns ``queued`` / ``dropped`` / ``failed_batches`` / ``stopped_reason`` / ``sink``. A
    non-zero ``dropped`` means the queue filled and submissions were discarded to keep your code
    non-blocking; a non-zero ``failed_batches`` means a sink stayed broken through the whole
    retry budget. Both are losses the library absorbs on purpose, and this is how you notice
    them. A non-``None`` ``stopped_reason`` is worse than either: the background thread died
    on that exception type, so nothing further will be delivered at all (SPEC-019)::

        h = log_foundry.health()
        if h.dropped or h.failed_batches or h.stopped_reason or (
            h.sink and (h.sink.dropped or h.sink.failed)
        ):
            ...  # raise an alert; logs were silently lost

    ``sink`` is the configured sink's own :class:`~log_foundry.sinks.base.SinkLosses` — loss the
    *sink* absorbed rather than the worker, which the worker's counters cannot see (SPEC-026).
    It is ``None`` when no worker exists and when the sink reports nothing, since ``losses()`` is
    optional. Its ``dropped`` is not the worker's: the worker's is backpressure at the queue, the
    sink's is an event that never reached the wire — usually one too large to ever fit, and for the
    sinks whose client owns a local buffer (Kafka, Pub/Sub) also what that buffer refused. The
    stderr line names which. Its ``failed`` is an upper bound on loss, not a count of it: a sink
    that raises on total failure counts the attempt *and* hands the batch back, and the worker's
    retry may then deliver it.

    A process that has never logged has no worker, and asking after its health does not create
    one — the snapshot is simply zeroed. Valid after :func:`shutdown`, which leaves the final
    counters readable.
    """
    from log_foundry.decorator import _worker_health

    return _worker_health()


def shutdown(timeout: float | None = DEFAULT_SHUTDOWN_TIMEOUT) -> None:
    """Flush buffered events and close the sink, blocking until drained. Idempotent.

    Also registered via ``atexit``; call it explicitly before a fast process exit when you
    want to be certain the tail of the queue reached the sink (SPEC-004 FR-005).

    This is **terminal** — the worker does not come back. Do not call it per-invocation in a
    serverless handler: the first invocation on a warm container would log and every later one
    would silently log nothing. Use :func:`flush` there, which drains and keeps the worker.

    ``timeout`` bounds the wait for the background thread (SPEC-027 FR-004). ``None`` waits
    indefinitely, which is what this did unconditionally before and is still available on
    request — but it is unsafe anywhere with an execution deadline, and ``atexit`` is one such
    place: a sink blocked in a network call would hold the process open. An expired shutdown
    reports ``health().stopped_reason == "ShutdownTimeout"`` and leaves the sink **open**, since
    the drain thread may still be inside ``emit``. Never raises.
    """
    from log_foundry.decorator import _shutdown_worker

    _shutdown_worker(timeout)


__all__ = [
    "DEFAULT_SHUTDOWN_TIMEOUT",
    "Health",
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
    "get_config",
    "health",
    "info",
    "reset_context",
    "set_baggage",
    "shutdown",
    "trace",
    "warning",
]
