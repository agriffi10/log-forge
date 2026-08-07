"""User-facing log emitters + baggage re-export (SPEC-002, arch §6)."""

from __future__ import annotations

from log_foundry import _diag, context
from log_foundry.config import _ensure_sink
from log_foundry.console import ConsoleWriter
from log_foundry.context import set_baggage
from log_foundry.decorator import _note_orphan_emit
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

_console = ConsoleWriter()


def _log(level: str, message: str, echo: bool, fields: dict[str, object]) -> None:
    """Builds one event at the given level and routes it, honoring ``echo``.

    Inside a span the event is appended to that span's queue and flushed together at span
    end. With no active span it becomes a standalone one-event span — fresh ``trace_id``,
    ``parent_span_id`` of ``None`` — flushed directly so nothing is dropped (FR-004), with
    the sink resolved through ``_ensure_sink`` so a zero-config orphan log falls back to
    ``StdoutSink`` rather than crashing.

    That direct handoff is **settled, not pending**: SPEC-004's worker took over the traced
    path and this branch was deliberately left synchronous, so a level call outside a span is
    never silently dropped for want of a worker (``architecture.md`` §12 Resolved, "Orphan
    logs"). It carried a comment saying the worker "will later" own it long after the
    decision was made (SPEC-031 FR-003).

    Because no worker is built here, nothing else in the library knows the sink was ever
    written to — so this branch records it (SPEC-031 FR-006). That is what arms the exit-time
    close for a process which only ever logs this way, and keying it on a sink this call is
    about to write to, rather than on a *configured* sink, is deliberate: ``configure()``
    materializes a ``StdoutSink`` whether or not anything is logged through it, and closing one
    nothing was ever written to is cost with no benefit.

    It is armed **before** the emit rather than after it, which matters for the sinks most
    likely to need the close. SPEC-026 FR-001 makes a total failure raise, so an orphan-only
    process against a dead syslog or HTTP destination raises on every call — and arming
    afterwards would leave the socket that failure came from open forever, which is the leak
    this FR exists to stop, in exactly the case that is leaking. A sink that raised is still a
    sink that was written to. ``_ensure_sink`` is resolved first, so a sink that fails to
    *construct* arms nothing: there is nothing to close.

    The orphan branch is the one that reaches the sink on the caller's own thread, with no
    worker between them to absorb a failure, so the whole branch is guarded (SPEC-025
    FR-003) — ``_ensure_sink`` constructs the sink on first use, so a sink that fails to
    build raises here too. The in-span branch is deliberately left untouched, since it only
    appends to a list. The echo runs after the emit, so a closed or redirected stream never
    costs the event itself.

    Args:
      level: The severity label to stamp on the event.
      message: The caller-supplied message text.
      echo: Whether to also write a human-readable console line.
      fields: Per-call fields merged into the event.

    Returns:
      None.

    Raises:
      None. A logging call must never hand the application an exception from a destination
        it never chose to talk to; absorbed faults are reported through ``_diag``.
    """
    baggage = context.get_baggage()
    span = context.current_span()
    event: dict[str, object] | None = None
    if span is not None:
        event = build_event(span, level, message, fields=fields, baggage=baggage)
        span.events.append(event)
    else:
        try:
            orphan = Span(
                trace_id=new_trace_id(),
                span_id=new_span_id(),
                parent_span_id=None,
                name=message,
                start_ts=0.0,
            )
            event = build_event(orphan, level, message, fields=fields, baggage=baggage)
            sink = _ensure_sink()
            _note_orphan_emit(sink)
            sink.emit([event])
        except Exception as exc:
            _diag.absorbed("emitting an orphan log", exc, "the event was lost")
    if echo and event is not None:
        try:
            _console.write(event)
        except Exception as exc:
            _diag.absorbed("echoing to the console", exc)


def debug(message: str, *, echo: bool = False, **fields: object) -> None:
    """Emits a ``DEBUG`` event on the current span, or a standalone orphan span.

    Args:
      message: The message text.
      echo: Whether to also write a human-readable console line.
      **fields: Per-call structured fields.

    Returns:
      None.

    Raises:
      None.
    """
    _log("DEBUG", message, echo, fields)


def info(message: str, *, echo: bool = False, **fields: object) -> None:
    """Emits an ``INFO`` event on the current span, or a standalone orphan span.

    Args:
      message: The message text.
      echo: Whether to also write a human-readable console line.
      **fields: Per-call structured fields.

    Returns:
      None.

    Raises:
      None.
    """
    _log("INFO", message, echo, fields)


def warning(message: str, *, echo: bool = False, **fields: object) -> None:
    """Emits a ``WARNING`` event on the current span, or a standalone orphan span.

    Args:
      message: The message text.
      echo: Whether to also write a human-readable console line.
      **fields: Per-call structured fields.

    Returns:
      None.

    Raises:
      None.
    """
    _log("WARNING", message, echo, fields)


def error(message: str, *, echo: bool = False, **fields: object) -> None:
    """Emits an ``ERROR`` event on the current span, or a standalone orphan span.

    Args:
      message: The message text.
      echo: Whether to also write a human-readable console line.
      **fields: Per-call structured fields.

    Returns:
      None.

    Raises:
      None.
    """
    _log("ERROR", message, echo, fields)


def critical(message: str, *, echo: bool = False, **fields: object) -> None:
    """Emits a ``CRITICAL`` event on the current span, or a standalone orphan span.

    Args:
      message: The message text.
      echo: Whether to also write a human-readable console line.
      **fields: Per-call structured fields.

    Returns:
      None.

    Raises:
      None.
    """
    _log("CRITICAL", message, echo, fields)
