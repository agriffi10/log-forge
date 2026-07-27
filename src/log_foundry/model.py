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
from typing import TYPE_CHECKING

from log_foundry.sanitize import sanitize_fields, truncate_str, truncate_tail

if TYPE_CHECKING:
    from log_foundry.config import Config

__all__ = ["Span", "build_event", "start_event", "end_event", "backfill_baggage"]

# Auto-generated span-boundary event messages. Tests assert on contract fields
# (``status``, ``trace_id``), never on this text — rename freely.
_START_MESSAGE = "span.start"
_END_MESSAGE = "span.end"

# Marks an event that a ceiling clipped (SPEC-017 FR-002). Set **only ever to True**, never to
# False — that single invariant is what makes "OR with whatever an earlier stage set" and
# "absent, not false, on a clean event" both fall out with no read-modify-write anywhere.
_TRUNCATED = "truncated"


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

    The merged mapping is coerced and size-bounded here (SPEC-017 FR-001/FR-002), which is what
    makes every sink's bare ``json.dumps`` safe without any of them changing. ``message`` is
    bounded too: it is a base field, but unlike the other eleven it is caller-supplied free text.
    """
    from log_foundry.config import get_config
    from log_foundry.ids import new_log_id

    cfg = get_config()
    merged: dict[str, object] = {**cfg.defaults, **span.defaults, **baggage, **fields}
    safe, clipped = sanitize_fields(merged, cfg=cfg)
    bounded_message, message_clipped = truncate_str(message, cfg.max_value_bytes)
    event: dict[str, object] = {
        "timestamp": _iso_now(),
        "level": level,
        "message": bounded_message,
        "trace_id": span.trace_id,
        "span_id": span.span_id,
        "parent_span_id": span.parent_span_id,
        "log_id": new_log_id(),
        "function": span.name,
        "service": cfg.service,
        "version": cfg.version,
        "env": cfg.env,
    }
    # Before ``fields`` so it reads ahead of the payload blob in a rendered log line.
    if clipped or message_clipped:
        event[_TRUNCATED] = True
    event["fields"] = safe
    return event


def start_event(span: Span) -> dict[str, object]:
    """Build the span-start boundary event."""
    return build_event(span, "INFO", _START_MESSAGE, fields={}, baggage={})


def _exception_message(exc: BaseException) -> str:
    """``str(exc)`` — ``""`` for an exception raised with no arguments (SPEC-017 FR-003).

    Guarded because a user exception may define a ``__str__`` that itself raises. This runs
    inside the decorator's ``except`` block, so an exception escaping here would *replace* the
    one the user's code raised, demoting theirs to ``__context__`` — logging breaking the app,
    which is the thing this spec exists to stop.
    """
    try:
        return str(exc)
    except Exception:  # noqa: BLE001 — see above; a hostile __str__ must not escape.
        return "<unprintable message>"


def _exception_stack(exc: BaseException) -> str:
    """The formatted traceback.

    ``traceback.format_exception`` already renders a failing ``__str__`` as
    ``<exception str() failed>`` rather than propagating, so this guard is for the rarer case of
    a frame that cannot be rendered at all.
    """
    try:
        return "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    except Exception:  # noqa: BLE001 — same reasoning as _exception_message.
        return f"<unformattable traceback: {type(exc).__name__}>"


def _error_fields(exc: BaseException, *, cfg: Config) -> tuple[dict[str, object], bool]:
    """Build the bounded ``error`` sub-document; report whether a ceiling fired.

    ``type`` keeps the bare class name consumers already index; ``module`` is added alongside it
    so two same-named exception classes from different packages stay distinguishable, rather
    than qualifying ``type`` in place and breaking every existing query (SPEC-017 FR-003).

    ``stack`` gets its own, larger ceiling: a traceback is legitimately long and is the most
    valuable thing on the event. It is clipped from the *head*, because ``format_exception``
    puts the exception and the innermost frames last.
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
    """Build the span-end boundary event.

    Adds ``duration_ms`` (from a monotonic delta), ``status`` (``"ok"``/``"error"``), and on
    failure a nested ``error`` with the exception type, module, message and formatted stack
    (arch §6, SPEC-017 FR-003).
    """
    from log_foundry.config import get_config

    level = "INFO" if status == "ok" else "ERROR"
    event = build_event(span, level, _END_MESSAGE, fields={}, baggage={})
    event["duration_ms"] = (time.monotonic() - span.start_ts) * 1000.0
    event["status"] = status
    if exc is not None:
        error, clipped = _error_fields(exc, cfg=get_config())
        event["error"] = error
        if clipped:
            event[_TRUNCATED] = True  # only ever True — this *is* the OR with build_event's
    return event


def backfill_baggage(span: Span, baggage: dict[str, object]) -> None:
    """Merge the span's final baggage into its buffered **boundary** events.

    ``@trace`` buffers ``span.start`` before the body runs, so baggage the body sets on its first
    line did not exist when that event was built — and :func:`build_event` snapshots ``fields``
    into each event dict, so a later value has to be written back. Same structural reason as
    :func:`~log_foundry.decorator._reparent_current_span`. The whole span flushes as one batch at
    close, so this completes the events before any of them is emitted.

    Boundary events only. An ``info`` emitted before ``set_baggage`` genuinely did not carry it,
    and rewriting a record of a moment would be a lie; merging over mid-span events would also let
    baggage override a per-call field, inverting :func:`build_event`'s precedence. Boundary events
    describe the span as a whole and carry no per-call fields, so baggage correctly wins there over
    ``cfg.defaults`` and ``span.defaults``.

    The baggage is a parameter rather than read from ``context``: this module does not know where
    the current span lives (arch §6).

    These values bypass :func:`build_event`'s pass entirely — ``set_baggage`` accepts arbitrary
    objects — so they are coerced here, **once** above the loop rather than per event: the same
    mapping is merged into every boundary event, and ``fields`` is already sanitized. The merge
    can push a mapping past ``max_keys``; that is deliberate, because re-capping here would drop
    the correlation keys SPEC-015 shipped to add (SPEC-017 FR-001).
    """
    if not baggage:
        return
    from log_foundry.config import get_config

    safe, clipped = sanitize_fields(baggage, cfg=get_config())
    for event in span.events:
        # Matched on the message constants, not a position — that ``span.start`` is index 0 is an
        # implementation detail of when it happens to be appended.
        if event.get("message") in (_START_MESSAGE, _END_MESSAGE):
            fields = event.get("fields")
            if isinstance(fields, dict):
                event["fields"] = {**fields, **safe}
                if clipped:
                    event[_TRUNCATED] = True
