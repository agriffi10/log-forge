"""log_forge — consistent, structured logs per decorated function call.

Public façade (module-function shape). Later phases re-export the
``debug/info/warning/error/critical`` emitters, ``set_baggage`` and ``shutdown`` here as
they land (see docs/implementation-guide.md module map). This phase exposes configuration
and the ``@trace`` decorator.
"""

from log_forge.config import configure, get_config
from log_forge.decorator import trace

__all__ = ["configure", "get_config", "trace"]
