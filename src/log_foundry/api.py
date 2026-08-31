"""User-facing log emitters + baggage re-export (SPEC-002, arch §6)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from log_foundry import _diag, context
from log_foundry._lifecycle import _note_orphan_emit
from log_foundry.config import _ensure_sink
from log_foundry.console import ConsoleWriter
from log_foundry.context import set_baggage
from log_foundry.decorator import _note_in_span_loss, _note_orphan_loss
from log_foundry.ids import new_span_id, new_trace_id
from log_foundry.model import Span, build_event

if TYPE_CHECKING:
    from collections.abc import Mapping

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

    **Both branches are guarded.** The orphan branch reaches the sink on the caller's own
    thread, with no worker between them to absorb a failure (SPEC-025 FR-003) — ``_ensure_sink``
    constructs the sink on first use, so a sink that fails to build raises here too.

    ~~The in-span branch is deliberately left untouched, since it only appends to a list.~~ —
    struck (SPEC-021), and it is why that branch shipped unguarded: it does not *only* append.
    It calls ``build_event``, which calls ``truncate_str``, which calls ``value.encode`` — so
    ``info(some_exception)``, a slip ``mypy`` catches only at typed call sites, returned normally
    on the orphan path and killed the caller's function inside a span. Worse than a crash: the
    decorator's own handler then recorded the span ``status=error`` with an ``error.type`` of
    ``AttributeError`` that the caller's code never raised — wrong data *and* a broken caller.
    A guard that reads "this cannot fail" is a guard that stops being true when the code under
    it changes, and this one had already stopped.

    **An append to a span that has already closed takes the orphan route** (SPEC-036 FR-004).
    ``contextvars`` copies the same ``Span`` object into every task created inside a span, so a
    fire-and-forget ``create_task`` can log after its parent returned — and since ``_flush``
    detaches at submit, that append lands in a buffer nothing will ever emit. This is the only
    place that can notice, because nothing in the library reads a span again after it closes; a
    post-hoc check of the buffer has no observer. A span that has closed is not a span, so the
    event becomes a fresh one-event span and inherits the orphan path's accounting entire —
    delivered on success, ``orphan_lost`` on failure. The flag is read before the event is built
    and the append happens after, so a thread *sharing* the span can still slip between the two and
    strand its event in the post-swap list; that window is recorded in ``architecture.md`` §13
    rather than closed, since closing it costs a per-span lock on the hottest path. That is why FR-004 adds no ``Health`` field
    of its own, and why the destination is named here rather than left implied.

    The cost is stated rather than hidden: the event gets a **fresh** ``trace_id``, so it leaves
    its trace, not merely its span. ``contextvars`` cannot deliver the alternative once the span
    is gone, and the choice is against losing the event outright.

    **Both losses are counted, and counted apart** (SPEC-036 FR-003). Each guard records before it
    announces, so the stderr line SPEC-025 already writes is unchanged and this adds a counter
    rather than a second announcement. The orphan increment sits in the ``except`` rather than
    after ``sink.emit``, which is what makes a sink that fails to *construct* count too. They stay
    two fields because the populations differ: this branch can fail at the destination, the
    in-span branch can only fail at the data.

    The echo runs after the emit, so a closed or redirected stream never costs the event itself.

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
    baggage = context._live_baggage()
    span = context.current_span()
    event: dict[str, object] | None = None
    if span is not None and not span.closed:
        try:
            event = build_event(span, level, message, fields=fields, baggage=baggage)
            span.events.append(event)
        except Exception as exc:
            _note_in_span_loss()
            _diag.absorbed("building an in-span log", exc, "the event was lost")
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
            _note_orphan_loss()
            _diag.absorbed("emitting an orphan log", exc, "the event was lost")
    if echo and event is not None:
        try:
            _console.write(event)
        except Exception as exc:
            _diag.absorbed("echoing to the console", exc)


def _merge(fields: Mapping[str, object] | None, kv: dict[str, object]) -> dict[str, object]:
    """Combines the explicit ``fields=`` mapping with the keyword form.

    ``echo`` and ``message`` were reserved words stolen from the caller's field namespace:
    ``info("x", echo="incoming payload echoed back")`` dropped a real field *and* turned on an
    unwanted console line, and ``message=`` raised ``TypeError`` (SPEC-034 FR-004). ``fields=``
    is the way round all three — including its own name, which is why the escape hatch can
    express every key including the ones this signature reserves — and the only way to pass a
    key that is not a Python identifier.

    The keyword form wins a collision. ``fields=`` is the bulk route, usually a mapping built
    somewhere else; ``**kv`` is what the caller wrote at this call site, and a literal
    overriding a base is what ``{**base, **overrides}`` already means in the language.

    A ``fields=`` that is not a mapping is **absorbed, not raised**. This helper runs in the
    emitter, before :func:`_log`, so it sits outside that function's orphan guard entirely: an
    unguarded ``{**fields, **kv}`` propagated a ``TypeError`` into the application on all four
    entry paths, including the orphan one where SPEC-025's promise holds today, and inside a span
    the decorator then recorded ``status=error`` with an ``error.type`` the caller's code never
    raised. It was also asymmetric in a way neither reading defends — ``fields=[]`` was silently
    ignored while ``fields=["x"]`` crashed. The library coerces rather than validates (SPEC-017),
    so the tolerant half is the one that matches, and the fault is announced by type.

    Args:
      fields: The explicit mapping, or ``None``.
      kv: The keyword-collected fields.

    Returns:
      One mapping, with ``kv`` taking precedence. The caller's ``fields`` is never mutated, and a
        ``fields=`` that could not be merged is dropped rather than costing the event.

    Raises:
      None.
    """
    if not fields:
        return kv
    try:
        return {**fields, **kv}
    except Exception as exc:
        _diag.absorbed("merging the fields= argument", exc, "the fields= argument was ignored")
        return kv


def debug(
    message: str,
    *,
    echo: bool = False,
    fields: Mapping[str, object] | None = None,
    **kv: object,
) -> None:
    """Emits a ``DEBUG`` event on the current span, or a standalone orphan span.

    Args:
      message: The message text.
      echo: Whether to also write a human-readable console line.
      fields: Per-call structured fields, for names ``**kv`` cannot express — the three
        reserved words ``message``, ``echo`` and ``fields`` itself, and any key that is not a
        Python identifier at all. Merged **under** ``**kv``, so a name given both ways takes the
        keyword's value.
      **kv: Per-call structured fields.

    Returns:
      None.

    Raises:
      None.
    """
    _log("DEBUG", message, echo, _merge(fields, kv))


def info(
    message: str,
    *,
    echo: bool = False,
    fields: Mapping[str, object] | None = None,
    **kv: object,
) -> None:
    """Emits an ``INFO`` event on the current span, or a standalone orphan span.

    Args:
      message: The message text.
      echo: Whether to also write a human-readable console line.
      fields: Per-call structured fields, for names ``**kv`` cannot express — the three
        reserved words ``message``, ``echo`` and ``fields`` itself, and any key that is not a
        Python identifier at all. Merged **under** ``**kv``, so a name given both ways takes the
        keyword's value.
      **kv: Per-call structured fields.

    Returns:
      None.

    Raises:
      None.
    """
    _log("INFO", message, echo, _merge(fields, kv))


def warning(
    message: str,
    *,
    echo: bool = False,
    fields: Mapping[str, object] | None = None,
    **kv: object,
) -> None:
    """Emits a ``WARNING`` event on the current span, or a standalone orphan span.

    Args:
      message: The message text.
      echo: Whether to also write a human-readable console line.
      fields: Per-call structured fields, for names ``**kv`` cannot express — the three
        reserved words ``message``, ``echo`` and ``fields`` itself, and any key that is not a
        Python identifier at all. Merged **under** ``**kv``, so a name given both ways takes the
        keyword's value.
      **kv: Per-call structured fields.

    Returns:
      None.

    Raises:
      None.
    """
    _log("WARNING", message, echo, _merge(fields, kv))


def error(
    message: str,
    *,
    echo: bool = False,
    fields: Mapping[str, object] | None = None,
    **kv: object,
) -> None:
    """Emits an ``ERROR`` event on the current span, or a standalone orphan span.

    Args:
      message: The message text.
      echo: Whether to also write a human-readable console line.
      fields: Per-call structured fields, for names ``**kv`` cannot express — the three
        reserved words ``message``, ``echo`` and ``fields`` itself, and any key that is not a
        Python identifier at all. Merged **under** ``**kv``, so a name given both ways takes the
        keyword's value.
      **kv: Per-call structured fields.

    Returns:
      None.

    Raises:
      None.
    """
    _log("ERROR", message, echo, _merge(fields, kv))


def critical(
    message: str,
    *,
    echo: bool = False,
    fields: Mapping[str, object] | None = None,
    **kv: object,
) -> None:
    """Emits a ``CRITICAL`` event on the current span, or a standalone orphan span.

    Args:
      message: The message text.
      echo: Whether to also write a human-readable console line.
      fields: Per-call structured fields, for names ``**kv`` cannot express — the three
        reserved words ``message``, ``echo`` and ``fields`` itself, and any key that is not a
        Python identifier at all. Merged **under** ``**kv``, so a name given both ways takes the
        keyword's value.
      **kv: Per-call structured fields.

    Returns:
      None.

    Raises:
      None.
    """
    _log("CRITICAL", message, echo, _merge(fields, kv))
