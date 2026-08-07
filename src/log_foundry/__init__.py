"""log_foundry — consistent, structured logs per decorated function call."""

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
    __version__ = _dist_version("log-foundry")
except PackageNotFoundError:
    __version__ = "0.0.0"


def flush(timeout: float | None = 5.0) -> bool:
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
      True when the events submitted before this call reached the sink. False if the drain did
      not complete within the timeout, if the worker has already been shut down or has died, or
      if a batch was abandoned while this call was outstanding (SPEC-021 FR-001) — so True means
      the events were delivered, not merely that a drain took place. Events submitted
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
    died on that exception type, so nothing further will be delivered at all (SPEC-019)::

        h = log_foundry.health()
        if h.dropped or h.failed_batches or h.stopped_reason or (
            h.sink and (h.sink.dropped or h.sink.failed)
        ):
            ...  # raise an alert; logs were silently lost

    Args:
      None.

    Returns:
      The snapshot: ``queued``, ``dropped``, ``failed_batches``, ``stopped_reason`` and
      ``sink``. The last is the configured sink's own
      :class:`~log_foundry.sinks.base.SinkLosses` — loss the sink absorbed rather than the
      worker (SPEC-026) — and is ``None`` when no worker exists or the sink reports nothing.
      Its ``dropped`` is not the worker's: the worker's is backpressure at the queue, the
      sink's is an event that never reached the wire, and the stderr line names which. Its
      ``failed`` is an upper bound on loss rather than a count of it, since a sink that raises
      on total failure counts the attempt and hands the batch back for the worker to retry. A
      process that has never logged has no worker, and asking does not create one — the
      snapshot is simply zeroed. Valid after :func:`shutdown`.

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

    Args:
      timeout: Seconds bounding the wait for the background thread (SPEC-027 FR-004). ``None``
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
