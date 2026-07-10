"""log_forge — consistent, structured logs per decorated function call.

Public façade (module-function shape). Later phases re-export ``shutdown`` here as it lands
(see docs/implementation-guide.md module map). This phase exposes configuration, the
``@trace`` decorator, the ``debug/info/warning/error/critical`` emitters, and ``set_baggage``.
"""

from log_forge.api import critical, debug, error, info, set_baggage, warning
from log_forge.config import configure, get_config
from log_forge.decorator import trace

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
]
