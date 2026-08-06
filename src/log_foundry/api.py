"""User-facing log emitters + baggage re-export (SPEC-002, arch §6).

The level functions (``debug``/``info``/``warning``/``error``/``critical``) let user code emit
its own structured events *inside* a decorated call: each appends one event (built via the
SPEC-001 :func:`~log_foundry.model.build_event`, stamped with current baggage) to the current
span's queue, so the whole call's logs flush together at span end. A level call made with no
active span is not dropped — it becomes a standalone one-event span with a fresh ``trace_id``,
flushed directly (FR-004). ``echo=True`` additionally surfaces a human-readable line to the
console synchronously (FR-002), without waiting for the async flush; the event still reaches
the sink via the normal path.
"""

from __future__ import annotations

from log_foundry import _diag, context
from log_foundry.config import _ensure_sink
from log_foundry.console import ConsoleWriter
from log_foundry.context import set_baggage
from log_foundry.ids import new_span_id, new_trace_id
from log_foundry.model import Span, build_event

__all__ = [
    "critical",
    "debug",
    "error",
    "info",
    "set_baggage",
    "warning",
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

    The orphan branch is the one that reaches the sink **on the caller's own thread**, with no
    worker between them to absorb a failure, so it is guarded end to end (SPEC-025 FR-003): a
    bare ``log_foundry.info(...)`` must not hand the application a ``ConnectionError`` from a
    destination the application never chose to talk to. The in-span branch is deliberately left
    untouched — it only appends to a list, and FR-003 requires no behaviour change there.
    """
    baggage = context.get_baggage()
    span = context.current_span()
    event: dict[str, object] | None = None
    if span is not None:
        event = build_event(span, level, message, fields=fields, baggage=baggage)
        span.events.append(event)
    else:
        # Orphan: no active span. Emit a standalone single-event span (fresh trace) so the
        # log is recorded, not silently dropped. Resolve the sink via _ensure_sink (as the
        # decorator does) so a zero-config orphan log falls back to StdoutSink instead of
        # crashing. SPEC-004's worker will later own this direct handoff.
        try:
            orphan = Span(
                trace_id=new_trace_id(),
                span_id=new_span_id(),
                parent_span_id=None,
                name=message,
                start_ts=0.0,
            )
            event = build_event(orphan, level, message, fields=fields, baggage=baggage)
            _ensure_sink().emit([event])
        except Exception as exc:
            # The whole branch, not just `emit`: `_ensure_sink` constructs the sink on first
            # use, so a sink that fails to *build* raises here too — and that is the shape the
            # spec's own motivating case takes.
            _diag.absorbed("emitting an orphan log", exc, "the event was lost")
    if echo and event is not None:
        try:
            _console.write(event)
        except Exception as exc:
            # An echo is a diagnostic convenience and must not outrank the caller: the stream
            # may be closed, redirected, or gone at interpreter shutdown. It runs *after* the
            # emit, so a broken console never costs the event itself.
            _diag.absorbed("echoing to the console", exc)


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
