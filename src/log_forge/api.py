"""User-facing log emitters + baggage re-export (SPEC-002, arch §6).

The level functions (``debug``/``info``/``warning``/``error``/``critical``) let user code emit
its own structured events *inside* a decorated call: each appends one event (built via the
SPEC-001 :func:`~log_forge.model.build_event`, stamped with current baggage) to the current
span's queue, so the whole call's logs flush together at span end. A level call made with no
active span is not dropped — it becomes a standalone one-event span with a fresh ``trace_id``,
flushed directly (FR-004). ``echo=True`` additionally surfaces a human-readable line to the
console synchronously (FR-002), without waiting for the async flush; the event still reaches
the sink via the normal path.
"""

from __future__ import annotations

from log_forge import context
from log_forge.config import _ensure_sink
from log_forge.console import ConsoleWriter
from log_forge.context import set_baggage
from log_forge.ids import new_span_id, new_trace_id
from log_forge.model import Span, build_event

__all__ = [
    "debug",
    "info",
    "warning",
    "error",
    "critical",
    "set_baggage",
]

# One console writer per process (default stream sys.stderr). Configurable overrides are
# deferred (SPEC-002 Out of Scope); only explicit per-call echo= ships here.
_console = ConsoleWriter()


def _log(level: str, message: str, echo: bool, fields: dict[str, object]) -> None:
    """Build one ``level`` event and route it, honoring ``echo``.

    Inside a span the event is appended to that span's queue (flushed together at span end).
    With no active span (orphan) it becomes a standalone one-event span — fresh ``trace_id``,
    ``parent_span_id=None`` — flushed directly so nothing is dropped (FR-004). ``echo`` writes
    the same event as a human-readable console line, additively.
    """
    baggage = context.get_baggage()
    span = context.current_span()
    if span is not None:
        event = build_event(span, level, message, fields=fields, baggage=baggage)
        span.events.append(event)
    else:
        # Orphan: no active span. Emit a standalone single-event span (fresh trace) so the
        # log is recorded, not silently dropped. Resolve the sink via _ensure_sink (as the
        # decorator does) so a zero-config orphan log falls back to StdoutSink instead of
        # crashing. SPEC-004's worker will later own this direct handoff.
        orphan = Span(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            parent_span_id=None,
            name=message,
            start_ts=0.0,
        )
        event = build_event(orphan, level, message, fields=fields, baggage=baggage)
        _ensure_sink().emit([event])
    if echo:
        _console.write(event)


def debug(message: str, *, echo: bool = False, **fields: object) -> None:
    """Emit a ``DEBUG`` event on the current span (or a standalone orphan span)."""
    _log("DEBUG", message, echo, fields)


def info(message: str, *, echo: bool = False, **fields: object) -> None:
    """Emit an ``INFO`` event on the current span (or a standalone orphan span)."""
    _log("INFO", message, echo, fields)


def warning(message: str, *, echo: bool = False, **fields: object) -> None:
    """Emit a ``WARNING`` event on the current span (or a standalone orphan span)."""
    _log("WARNING", message, echo, fields)


def error(message: str, *, echo: bool = False, **fields: object) -> None:
    """Emit an ``ERROR`` event on the current span (or a standalone orphan span)."""
    _log("ERROR", message, echo, fields)


def critical(message: str, *, echo: bool = False, **fields: object) -> None:
    """Emit a ``CRITICAL`` event on the current span (or a standalone orphan span)."""
    _log("CRITICAL", message, echo, fields)
