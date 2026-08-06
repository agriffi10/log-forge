"""Context propagation: span stack + baggage via contextvars (arch §5, guide Phase 4)."""

from __future__ import annotations

import contextvars
from typing import TYPE_CHECKING
from urllib.parse import quote, unquote

from log_foundry.ids import format_traceparent

if TYPE_CHECKING:
    from log_foundry.model import Span

__all__ = [
    "current_baggage_header",
    "current_span",
    "current_trace_context",
    "current_traceparent",
    "get_baggage",
    "pop_baggage_scope",
    "pop_span",
    "push_baggage_scope",
    "push_span",
    "reset_context",
    "set_baggage",
]

_span_stack: contextvars.ContextVar[tuple[Span, ...]] = contextvars.ContextVar(
    "log_foundry_span_stack", default=()
)
_baggage: contextvars.ContextVar[dict[str, object]] = contextvars.ContextVar(
    "log_foundry_baggage",
    default={},  # noqa: B039
)
_adopted: contextvars.ContextVar[tuple[str, str | None] | None] = contextvars.ContextVar(
    "log_foundry_adopted_context", default=None
)

BAGGAGE_MAX_BYTES = 8192


def current_span() -> Span | None:
    """Returns the innermost active span.

    Args:
      None.

    Returns:
      The current span, or ``None`` when no span is active.

    Raises:
      None.
    """
    stack = _span_stack.get()
    return stack[-1] if stack else None


def push_span(span: Span) -> contextvars.Token[tuple[Span, ...]]:
    """Pushes a span onto the stack.

    Args:
      span: The span being opened.

    Returns:
      A token to hand back to :func:`pop_span`.

    Raises:
      None.
    """
    return _span_stack.set((*_span_stack.get(), span))


def pop_span(token: contextvars.Token[tuple[Span, ...]]) -> None:
    """Restores the span stack to its state before the matching :func:`push_span`.

    This is total and never raises, on the same terms and for the same reason as
    :func:`pop_baggage_scope`: it runs in the decorator's ``finally``, where an exception
    would replace the one the caller's own function raised (arch §4, SPEC-025). ``reset``
    rejects a token minted in another context and one already used, so the fallback
    restores the captured stack directly, treating anything that is not a tuple as empty.

    Args:
      token: The token returned by :func:`push_span`.

    Returns:
      None.

    Raises:
      None.
    """
    try:
        _span_stack.reset(token)
    except Exception:
        old = getattr(token, "old_value", None)
        _span_stack.set(old if isinstance(old, tuple) else ())


def get_baggage() -> dict[str, object]:
    """Returns the current trace's baggage, which must not be mutated in place.

    The trace's own keys are discarded when the enclosing root span closes, restoring the
    baggage in effect before it, so a later trace sees only a process-level default set
    before any span opened. With no span open at all nothing releases them and they
    accumulate for the life of the context; :func:`reset_context` is the release there.

    Args:
      None.

    Returns:
      The baggage mapping, to be treated as read-only.

    Raises:
      None.
    """
    return _baggage.get()


def set_baggage(**kv: object) -> None:
    """Merges key/values into the current trace's baggage.

    The keys live until the enclosing root span closes, then the baggage in effect before
    that span is restored, so they ride every event at or below this point in the trace and
    none of the next trace's. Called with no span open they become a process-level default
    that later traces inherit and restore to, which ``configure(defaults=...)`` expresses
    better.

    Args:
      **kv: Fields to merge into the baggage.

    Returns:
      None.

    Raises:
      None.
    """
    _baggage.set({**_baggage.get(), **kv})


def push_baggage_scope() -> contextvars.Token[dict[str, object]]:
    """Opens a root span's baggage scope.

    Setting the variable to its own current value is what mints the token — there is no
    other way to say "restore whatever was here", including the case where nothing was ever
    set. It is safe because no baggage dict is ever mutated in place.

    Args:
      None.

    Returns:
      A token to hand back to :func:`pop_baggage_scope`.

    Raises:
      None.
    """
    return _baggage.set(_baggage.get())


def pop_baggage_scope(token: contextvars.Token[dict[str, object]]) -> None:
    """Closes a root span's scope: restores baggage, discards any adopted trace context.

    The two are deliberately asymmetric. Baggage is restored, so a process-level default set
    before any span outlives the traces that run under it while a request's own keys do not
    (SPEC-024 FR-001); the adopted context is cleared, because it is a one-shot handoff and
    restoring it would leave the next invocation joining the previous caller's trace
    (FR-002). The adopted context is cleared first, ahead of anything that could fail: a
    stale trace id puts wrong data in the log stream, while a missed baggage restore only
    leaves a stale field.

    Both writes land in the context the root span's ``finally`` runs in, so an adoption made
    outside a span that then runs in a child context — any ``asyncio.Task`` — is cleared in
    the copy rather than the parent. :func:`continue_trace` is documented to be called on the
    entry point's first line, which is inside the span and unaffected; a caller who adopts
    before dispatching across a task boundary clears it with :func:`reset_context`.

    Args:
      token: The token returned by :func:`push_baggage_scope`.

    Returns:
      None.

    Raises:
      None. A decorated function must not fail on the way out (arch §4), so a token minted
        in another context or already used falls back to setting the captured value.
    """
    _adopted.set(None)
    try:
        _baggage.reset(token)
    except Exception:
        old = getattr(token, "old_value", None)
        _baggage.set(old if isinstance(old, dict) else {})


def reset_context() -> None:
    """Clears baggage and any adopted trace context outright (SPEC-024 FR-003).

    This is for the caller who uses the emitters without ``@trace``: the orphan path opens no
    span, so there is no root-span exit to hang the release on. It is also the remedy for the
    one case that scope cannot reach, an adoption made outside a span whose root span then
    runs in a child context, and must be called in the context that made the adoption.

    Unlike the scope release this clears rather than restores, erasing a process-level
    baggage default too. Prefer calling it outside a span: inside one it does the obvious
    thing to the events that follow, but SPEC-015 backfills ``span.start`` and ``span.end``
    from the baggage live at close, so a mid-span reset also empties the two boundary events
    of baggage that was live for most of the span.

    Args:
      None.

    Returns:
      None.

    Raises:
      None.
    """
    _adopted.set(None)
    _baggage.set({})


def get_adopted_context() -> tuple[str, str | None] | None:
    """Returns the inbound trace context adopted for the next root span.

    Args:
      None.

    Returns:
      The adopted ``(trace_id, parent_span_id)``, or ``None`` if none was adopted.

    Raises:
      None.
    """
    return _adopted.get()


def set_adopted_context(trace_id: str, parent_span_id: str | None) -> None:
    """Records an inbound trace context for the next root span opened in this context.

    Args:
      trace_id: The inbound trace id to join.
      parent_span_id: The caller's span id, or ``None`` if the header carried none.

    Returns:
      None.

    Raises:
      None.
    """
    _adopted.set((trace_id, parent_span_id))


def current_traceparent() -> str | None:
    """Returns the current span as a W3C ``traceparent`` string.

    Args:
      None.

    Returns:
      The formatted header value, or ``None`` if no span is active.

    Raises:
      None.
    """
    span = current_span()
    return format_traceparent(span.trace_id, span.span_id) if span else None


def current_trace_context() -> tuple[str, str] | None:
    """Returns the current span's trace and span ids as a pair.

    This is for callers who would rather move two fields than a formatted string — a Step
    Functions payload, a queue message attribute. It exists so nobody is pushed into
    :func:`current_span`, which is internal and hands back a mutable :class:`~.model.Span`.

    Args:
      None.

    Returns:
      A ``(trace_id, span_id)`` tuple, or ``None`` if no span is active.

    Raises:
      None.
    """
    span = current_span()
    return (span.trace_id, span.span_id) if span else None


def current_baggage_header() -> str:
    """Returns the current baggage in W3C ``baggage`` header format.

    The baggage store holds arbitrary objects but the wire format is text, so non-string
    values are serialized with ``str()`` — put a dict in baggage and it arrives at the next
    process as its repr, not as a dict.

    Args:
      None.

    Returns:
      The formatted header value, empty when there is no baggage.

    Raises:
      None.
    """
    return format_baggage_header(get_baggage())


def format_baggage_header(baggage: dict[str, object]) -> str:
    """Serializes a baggage mapping to the W3C ``key1=value1,key2=value2`` format.

    Keys and values are percent-encoded with ``safe=""``, so the separators themselves are
    encoded rather than corrupting the framing — the whole reason a value containing a comma
    survives the hop and round-trips through :func:`parse_baggage_header`.

    Args:
      baggage: The mapping to serialize.

    Returns:
      The formatted header value.

    Raises:
      None.
    """
    return ",".join(f"{quote(k, safe='')}={quote(str(v), safe='')}" for k, v in baggage.items())


def parse_baggage_header(header: object) -> dict[str, object] | None:
    """Parses a W3C ``baggage`` header into a mapping.

    This is total, like the ``traceparent`` parser, because the header arrives from outside
    the process. W3C allows per-member properties after a ``;`` and nothing here consumes
    them, so they are dropped rather than treated as part of the value.

    Args:
      header: An inbound header value of any type.

    Returns:
      The parsed mapping, or ``None`` for a non-string, an empty header, one over
      :data:`BAGGAGE_MAX_BYTES`, or a malformed member — never a partial parse, which would
      silently drop correlating fields.

    Raises:
      None.
    """
    if not isinstance(header, str) or not header.strip():
        return None
    if len(header.encode("utf-8", "replace")) > BAGGAGE_MAX_BYTES:
        return None
    parsed: dict[str, object] = {}
    for member in header.split(","):
        entry = member.split(";", 1)[0].strip()
        if not entry:
            continue
        key, sep, value = entry.partition("=")
        if not sep:
            return None
        key = unquote(key.strip())
        if not key:
            return None
        parsed[key] = unquote(value.strip())
    return parsed or None
