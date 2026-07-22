"""log_forge — consistent, structured logs per decorated function call.

Public façade (module-function shape). Exposes configuration, the ``@trace`` decorator, the
``debug/info/warning/error/critical`` emitters, ``set_baggage``, and ``shutdown`` (graceful
drain of the background worker).
"""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _dist_version

from log_forge.api import critical, debug, error, info, set_baggage, warning
from log_forge.config import configure, get_config
from log_forge.decorator import trace

try:
    __version__ = _dist_version("log-forge")
except PackageNotFoundError:  # running from a source tree that isn't installed
    __version__ = "0.0.0"


def shutdown() -> None:
    """Flush buffered events and close the sink, blocking until drained. Idempotent.

    Also registered via ``atexit``; call it explicitly before a fast process exit when you
    want to be certain the tail of the queue reached the sink (SPEC-004 FR-005).
    """
    from log_forge.decorator import _shutdown_worker

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
    "shutdown",
    "__version__",
]
