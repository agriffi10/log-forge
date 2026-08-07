"""Span model + log-event construction and serialization (arch §6, guide Phase 3)."""

from __future__ import annotations

import time
import traceback
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from log_foundry.config import get_config
from log_foundry.ids import new_log_id
from log_foundry.sanitize import sanitize_fields, truncate_str, truncate_tail

if TYPE_CHECKING:
    from log_foundry.config import Config

__all__ = ["Span", "backfill_baggage", "build_event", "end_event", "start_event"]

_START_MESSAGE = "span.start"
_END_MESSAGE = "span.end"

_TRUNCATED = "truncated"


@dataclass
class Span:
    """One decorated function call: its identity, timing, and buffered events.

    ``start_ts`` is a ``time.monotonic()`` reading rather than wall-clock, so
    ``duration_ms`` can never go negative across a clock change. ``defaults`` are per-
    decorator field overrides, and ``events`` is the queue flushed together at span end.
    """

    trace_id: str
    span_id: str
    parent_span_id: str | None
    name: str
    start_ts: float
    defaults: dict[str, object] = field(default_factory=dict)
    events: list[dict[str, object]] = field(default_factory=list)


def _iso_now() -> str:
    """Returns the current UTC time as ISO-8601 with millisecond precision and a ``Z``.

    Args:
      None.

    Returns:
      The formatted timestamp.

    Raises:
      None.
    """
    now = datetime.now(UTC)
    return f"{now:%Y-%m-%dT%H:%M:%S}.{now.microsecond // 1000:03d}Z"


def build_event(
    span: Span,
    level: str,
    message: str,
    *,
    fields: dict[str, object],
    baggage: dict[str, object],
) -> dict[str, object]:
    """Assembles one log record in the arch §6 schema.

    Field precedence runs lowest to highest: config ``defaults`` → ``span.defaults`` →
    ``baggage`` → per-call ``fields`` (arch §5.1). The merged mapping is coerced and
    size-bounded here (SPEC-017 FR-001/FR-002), which is what makes every sink's bare
    ``json.dumps`` safe without any of them changing. ``message`` and ``function`` are
    bounded too, since both are caller-supplied text and leaving either out would keep
    ``info(huge_string)`` unbounded.

    ``get_config`` and ``new_log_id`` are imported at module scope (SPEC-031 FR-004). They were
    function-local to avoid a cycle, but there is none to avoid: neither ``config`` nor ``ids``
    imports this module, and ``config``'s own back-references to ``decorator`` stay local for
    the reason its docstrings give. This is the hottest path in the library, so resolving them
    once at import is what ``sanitize`` already does with its one-time bindings.

    Args:
      span: The span the event belongs to, supplying identity and defaults.
      level: The severity label, such as ``"INFO"``.
      message: The caller-supplied message text.
      fields: Per-call fields, which win over every other source.
      baggage: Context baggage merged beneath the per-call fields.

    Returns:
      The assembled event, safe for any sink to serialize.

    Raises:
      None.
    """
    cfg = get_config()
    merged: dict[str, object] = {**cfg.defaults, **span.defaults, **baggage, **fields}
    safe, clipped = sanitize_fields(merged, cfg=cfg)
    bounded_message, message_clipped = truncate_str(message, cfg.max_value_bytes)
    bounded_function, function_clipped = truncate_str(span.name, cfg.max_value_bytes)
    event: dict[str, object] = {
        "timestamp": _iso_now(),
        "level": level,
        "message": bounded_message,
        "trace_id": span.trace_id,
        "span_id": span.span_id,
        "parent_span_id": span.parent_span_id,
        "log_id": new_log_id(),
        "function": bounded_function,
        "service": cfg.service,
        "version": cfg.version,
        "env": cfg.env,
    }
    if clipped or message_clipped or function_clipped:
        event[_TRUNCATED] = True
    event["fields"] = safe
    return event


def start_event(span: Span) -> dict[str, object]:
    """Builds the span-start boundary event.

    Args:
      span: The span being opened.

    Returns:
      The ``span.start`` event.

    Raises:
      None.
    """
    return build_event(span, "INFO", _START_MESSAGE, fields={}, baggage={})


def _exception_message(exc: BaseException) -> str:
    """Returns ``str(exc)``, or an empty string for an exception raised with no arguments.

    This is guarded because a user exception may define a ``__str__`` that itself raises.
    It runs inside the decorator's ``except`` block, so an exception escaping here would
    replace the one the user's code raised, demoting theirs to ``__context__`` — logging
    breaking the app, which is the thing SPEC-017 FR-003 exists to stop.

    Args:
      exc: The exception to render.

    Returns:
      The message text, or ``"<unprintable message>"`` if rendering failed.

    Raises:
      None.
    """
    try:
        return str(exc)
    except Exception:
        return "<unprintable message>"


def _exception_stack(exc: BaseException) -> str:
    """Returns the formatted traceback for an exception.

    ``traceback.format_exception`` already renders a failing ``__str__`` as
    ``<exception str() failed>`` rather than propagating, so this guard is for the rarer
    case of a frame that cannot be rendered at all.

    Args:
      exc: The exception whose traceback is formatted.

    Returns:
      The formatted traceback, or a placeholder naming the exception type.

    Raises:
      None.
    """
    try:
        return "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    except Exception:
        return f"<unformattable traceback: {type(exc).__name__}>"


def _error_fields(exc: BaseException, *, cfg: Config) -> tuple[dict[str, object], bool]:
    """Builds the bounded ``error`` sub-document and reports whether a ceiling fired.

    ``type`` keeps the bare class name consumers already index, and ``module`` is added
    alongside it so two same-named exception classes from different packages stay
    distinguishable rather than qualifying ``type`` in place and breaking every existing
    query (SPEC-017 FR-003). ``stack`` gets its own larger ceiling and is clipped from the
    head, because ``format_exception`` puts the exception and innermost frames last.

    Args:
      exc: The exception being recorded.
      cfg: The config supplying the ceilings.

    Returns:
      The ``error`` sub-document and whether any ceiling clipped a value.

    Raises:
      None.
    """
    type_name, t1 = truncate_str(type(exc).__name__, cfg.max_value_bytes)
    module, t2 = truncate_str(str(getattr(type(exc), "__module__", "")), cfg.max_value_bytes)
    message, t3 = truncate_str(_exception_message(exc), cfg.max_value_bytes)
    stack, t4 = truncate_tail(_exception_stack(exc), cfg.max_stack_bytes)
    error: dict[str, object] = {
        "type": type_name,
        "module": module,
        "message": message,
        "stack": stack,
    }
    return error, (t1 or t2 or t3 or t4)


def end_event(
    span: Span,
    status: str,
    exc: BaseException | None = None,
) -> dict[str, object]:
    """Builds the span-end boundary event.

    Adds ``duration_ms`` from a monotonic delta, ``status``, and on failure a nested
    ``error`` carrying the exception type, module, message and formatted stack (arch §6,
    SPEC-017 FR-003).

    Args:
      span: The span being closed.
      status: The span outcome, ``"ok"`` or ``"error"``.
      exc: The exception that ended the span, if it failed.

    Returns:
      The ``span.end`` event.

    Raises:
      None.
    """
    level = "INFO" if status == "ok" else "ERROR"
    event = build_event(span, level, _END_MESSAGE, fields={}, baggage={})
    event["duration_ms"] = (time.monotonic() - span.start_ts) * 1000.0
    event["status"] = status
    if exc is not None:
        error, clipped = _error_fields(exc, cfg=get_config())
        event["error"] = error
        if clipped:
            event[_TRUNCATED] = True
    return event


def backfill_baggage(span: Span, baggage: dict[str, object]) -> None:
    """Merges the span's final baggage into its buffered boundary events.

    ``@trace`` buffers ``span.start`` before the body runs, so baggage the body sets on its
    first line did not exist when that event was built, and :func:`build_event` snapshots
    ``fields`` into each event dict. Boundary events only: an ``info`` emitted before
    ``set_baggage`` genuinely did not carry it, and merging over mid-span events would let
    baggage override a per-call field, inverting :func:`build_event`'s precedence.

    The values bypass :func:`build_event`'s pass entirely, so they are coerced here once
    above the loop. The merge can push a mapping past ``max_keys``, which is deliberate:
    re-capping would drop the correlation keys SPEC-015 shipped to add (SPEC-017 FR-001).

    Args:
      span: The span whose buffered events are completed.
      baggage: The span's final baggage, passed in because this module does not know where
        the current span lives (arch §6).

    Returns:
      None.

    Raises:
      None.
    """
    if not baggage:
        return
    safe, clipped = sanitize_fields(baggage, cfg=get_config())
    for event in span.events:
        if event.get("message") in (_START_MESSAGE, _END_MESSAGE):
            fields = event.get("fields")
            if isinstance(fields, dict):
                event["fields"] = {**fields, **safe}
                if clipped:
                    event[_TRUNCATED] = True
