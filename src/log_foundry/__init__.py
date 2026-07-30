"""log_foundry — consistent, structured logs per decorated function call.

Public façade (module-function shape). Exposes configuration, the ``@trace`` decorator, the
``debug/info/warning/error/critical`` emitters, ``set_baggage``, the cross-process propagation
pair (``continue_trace`` to adopt an inbound context; ``current_traceparent`` /
``current_trace_context`` / ``current_baggage_header`` to publish this one), ``flush`` (drain and
keep logging) and ``shutdown`` (drain, close the sink, and stop).
"""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _dist_version

from log_foundry.api import critical, debug, error, info, set_baggage, warning
from log_foundry.config import configure, get_config
from log_foundry.context import (
    current_baggage_header,
    current_trace_context,
    current_traceparent,
)
from log_foundry.decorator import continue_trace, trace
from log_foundry.worker import Health

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
    already been shut down or has died, or if the drain carrying these events was abandoned
    after exhausting its retries (SPEC-021 FR-001) — a ``True`` means they were delivered, not
    merely that a drain took place. For the cumulative record across every batch the worker has
    sent, read :func:`health`. Never raises.
    """
    from log_foundry.decorator import _flush_worker

    return _flush_worker(timeout)


def health() -> Health:
    """Snapshot the background worker's delivery counters (SPEC-017 FR-005). Never raises.

    Returns ``queued`` / ``dropped`` / ``failed_batches`` / ``stopped_reason``. A non-zero
    ``dropped`` means the queue filled and submissions were discarded to keep your code
    non-blocking; a non-zero ``failed_batches`` means a sink stayed broken through the whole
    retry budget. Both are losses the library absorbs on purpose, and this is how you notice
    them. A non-``None`` ``stopped_reason`` is worse than either: the background thread died
    on that exception type, so nothing further will be delivered at all (SPEC-019)::

        h = log_foundry.health()
        if h.dropped or h.failed_batches or h.stopped_reason:
            ...  # raise an alert; logs were silently lost

    A process that has never logged has no worker, and asking after its health does not create
    one — the snapshot is simply zeroed. Valid after :func:`shutdown`, which leaves the final
    counters readable.
    """
    from log_foundry.decorator import _worker_health

    return _worker_health()


def shutdown() -> None:
    """Flush buffered events and close the sink, blocking until drained. Idempotent.

    Also registered via ``atexit``; call it explicitly before a fast process exit when you
    want to be certain the tail of the queue reached the sink (SPEC-004 FR-005).

    This is **terminal** — the worker does not come back. Do not call it per-invocation in a
    serverless handler: the first invocation on a warm container would log and every later one
    would silently log nothing. Use :func:`flush` there, which drains and keeps the worker.
    """
    from log_foundry.decorator import _shutdown_worker

    _shutdown_worker()


__all__ = [
    "Health",
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
    "set_baggage",
    "shutdown",
    "trace",
    "warning",
]
