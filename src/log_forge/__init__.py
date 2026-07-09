"""log_forge — consistent, structured logs per decorated function call.

Public façade (module-function shape). Later phases re-export ``trace``, the
``debug/info/warning/error/critical`` emitters, ``set_baggage`` and ``shutdown`` here as
they land (see docs/implementation-guide.md module map). Phase 1 exposes configuration.
"""

from log_forge.config import configure, get_config

__all__ = ["configure", "get_config"]
