"""Span model + log-event construction and serialization (arch §6, guide Phase 3).

This is the heart of "structured, never free-form": every event is assembled here into one
identical JSON shape, which is what makes logs queryable downstream. This module only
*builds* records — it deliberately imports neither ``context`` nor ``decorator`` and does
not know where the "current" span lives (arch §6 watch-out).
"""

from __future__ import annotations

import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone

__all__ = ["Span", "build_event", "start_event", "end_event"]

# Auto-generated span-boundary event messages. Tests assert on contract fields
# (``status``, ``trace_id``), never on this text — rename freely.
_START_MESSAGE = "span.start"
_END_MESSAGE = "span.end"


@dataclass
class Span:
    """One decorated function call: its identity, timing, and buffered events.

    ``start_ts`` is a ``time.monotonic()`` reading (not wall-clock) so ``duration_ms`` can
    never go negative across a clock change. ``defaults`` are per-decorator field overrides;
    ``events`` is the queue flushed together at span end.
    """

    trace_id: str
    span_id: str
    parent_span_id: str | None
    name: str
    start_ts: float
    defaults: dict[str, object] = field(default_factory=dict)
    events: list[dict[str, object]] = field(default_factory=list)


def _iso_now() -> str:
    """Return the current UTC time as ISO-8601 with millisecond precision and a 'Z'."""
    now = datetime.now(timezone.utc)
    return f"{now:%Y-%m-%dT%H:%M:%S}.{now.microsecond // 1000:03d}Z"


def build_event(
    span: Span,
    level: str,
    message: str,
    *,
    fields: dict[str, object],
    baggage: dict[str, object],
) -> dict[str, object]:
    """Assemble one log record in the arch §6 schema.

    Field precedence, lowest to highest: config ``defaults`` → ``span.defaults`` → ``baggage``
    → per-call ``fields`` (arch §5.1). Later sources win on a key conflict.
    """
    from log_forge.config import get_config
    from log_forge.ids import new_log_id

    cfg = get_config()
    merged: dict[str, object] = {**cfg.defaults, **span.defaults, **baggage, **fields}
    return {
        "timestamp": _iso_now(),
        "level": level,
        "message": message,
        "trace_id": span.trace_id,
        "span_id": span.span_id,
        "parent_span_id": span.parent_span_id,
        "log_id": new_log_id(),
        "function": span.name,
        "service": cfg.service,
        "version": cfg.version,
        "env": cfg.env,
        "fields": merged,
    }


def start_event(span: Span) -> dict[str, object]:
    """Build the span-start boundary event."""
    return build_event(span, "INFO", _START_MESSAGE, fields={}, baggage={})


def end_event(
    span: Span,
    status: str,
    exc: BaseException | None = None,
) -> dict[str, object]:
    """Build the span-end boundary event.

    Adds ``duration_ms`` (from a monotonic delta), ``status`` (``"ok"``/``"error"``), and on
    failure a nested ``error`` with the exception type and formatted stack (arch §6).
    """
    level = "INFO" if status == "ok" else "ERROR"
    event = build_event(span, level, _END_MESSAGE, fields={}, baggage={})
    event["duration_ms"] = (time.monotonic() - span.start_ts) * 1000.0
    event["status"] = status
    if exc is not None:
        event["error"] = {
            "type": type(exc).__name__,
            "stack": "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
        }
    return event
