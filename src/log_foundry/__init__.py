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

try:
    # Distribution name ("log-foundry") differs from the import name ("log_foundry").
    __version__ = _dist_version("log-foundry")
except PackageNotFoundError:  # running from a source tree that isn't installed
    __version__ = "0.0.0"


def flush(timeout: float | None = 5.0) -> bool:
    """Drain buffered events through the sink without closing it.

    Every event submitted before this call has been passed to ``sink.emit`` when it returns
    ``True``. (Events submitted concurrently by another thread may or may not be included —
    the caller cannot have meant those.) Unlike :func:`shutdown` the background worker stays
    alive and the sink stays open, so logging continues normally afterwards.

    This is the drain for a process that is *frozen* rather than exited — an AWS Lambda
    handler must drain before it returns, but will be invoked again on the same warm
    container. Call it in a ``finally``: the invocation worth logging is the one that failed.

    ``timeout=None`` waits indefinitely, which is unsafe in any environment with an execution
    deadline — it converts "some logs were lost" into "the invocation timed out".

    Returns ``False`` if the drain did not complete within ``timeout``, or if the worker has
    already been shut down. Never raises.
    """
    from log_foundry.decorator import _flush_worker

    return _flush_worker(timeout)


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
    "configure",
    "get_config",
    "trace",
    "debug",
    "info",
    "warning",
    "error",
    "critical",
    "set_baggage",
    "continue_trace",
    "current_traceparent",
    "current_trace_context",
    "current_baggage_header",
    "flush",
    "shutdown",
    "__version__",
]
