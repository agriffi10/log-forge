"""W3C Trace Context-compatible id generation (arch §3.1, guide Phase 2).

``trace_id`` and ``span_id`` use the W3C wire formats (not arbitrary UUIDs) so a future
"adopt the inbound request's trace" feature is just a header parse (arch §12) and the
emitted records stay compatible with standard distributed-tracing tooling. ``log_id`` is
internal-only, so a UUID is fine.
"""

from __future__ import annotations

import os
import uuid

__all__ = ["new_trace_id", "new_span_id", "new_log_id"]


def new_trace_id() -> str:
    """Return a fresh trace id: 32 lowercase hex chars (16 random bytes)."""
    return os.urandom(16).hex()


def new_span_id() -> str:
    """Return a fresh span id: 16 lowercase hex chars (8 random bytes)."""
    return os.urandom(8).hex()


def new_log_id() -> str:
    """Return a fresh log id: a UUID4 hex string (internal-only, format is our choice)."""
    return uuid.uuid4().hex
