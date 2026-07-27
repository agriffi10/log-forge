"""Context propagation: span stack + baggage via contextvars (arch §5, guide Phase 4).

``contextvars`` (not thread-locals) is correct under both threads and asyncio — each task
inherits its own copy — so ``log_foundry.info(...)`` can find "the span I'm inside" with no
manual passing. This module holds a *stack* of active spans (the top is the current span and
the parent of the next nested call) plus trace-scoped baggage.

Two footguns, both avoided below:
  * Never mutate a ContextVar's default mutable value — the ``()`` / ``{}`` defaults are shared
    across all contexts. Always ``.set()`` a new tuple/dict.
  * Use the token/``reset`` pattern, not a manual pop — ``reset(token)`` restores the exact
    prior state even when tasks branch.
"""

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
    "pop_span",
    "push_span",
    "set_baggage",
]

_span_stack: contextvars.ContextVar[tuple[Span, ...]] = contextvars.ContextVar(
    "log_foundry_span_stack", default=()
)
_baggage: contextvars.ContextVar[dict[str, object]] = contextvars.ContextVar(
    "log_foundry_baggage",
    default={},  # noqa: B039 - the never-mutate rule in this module's docstring is what
    # makes the shared default safe: `set_baggage` replaces the dict, `get_baggage` is not
    # exported, and both internal readers treat it read-only.
)
# An inbound trace context adopted via ``continue_trace`` (SPEC-014), applied by ``_open_span``
# to the next *root* span. A ContextVar for the same reason as the two above: it is then correct
# under threads and asyncio for free, and a task that adopts a context cannot leak it to siblings.
_adopted: contextvars.ContextVar[tuple[str, str | None] | None] = contextvars.ContextVar(
    "log_foundry_adopted_context", default=None
)

# W3C suggests 8192 bytes for the whole `baggage` header. Bounded on the way in so a hostile or
# runaway header cannot inflate every subsequent event emitted by this process.
BAGGAGE_MAX_BYTES = 8192


def current_span() -> Span | None:
    """Return the innermost active span, or ``None`` when no span is active."""
    stack = _span_stack.get()
    return stack[-1] if stack else None


def push_span(span: Span) -> contextvars.Token[tuple[Span, ...]]:
    """Push ``span`` onto the stack; return a token to hand back to :func:`pop_span`."""
    return _span_stack.set((*_span_stack.get(), span))


def pop_span(token: contextvars.Token[tuple[Span, ...]]) -> None:
    """Restore the span stack to its state before the matching :func:`push_span`."""
    _span_stack.reset(token)


def get_baggage() -> dict[str, object]:
    """Return the current trace's baggage (do not mutate the returned dict in place)."""
    return _baggage.get()


def set_baggage(**kv: object) -> None:
    """Merge key/values into the current trace's baggage (replaces with a new dict)."""
    _baggage.set({**_baggage.get(), **kv})


# -- adopted inbound context (SPEC-014) --------------------------------------------------


def get_adopted_context() -> tuple[str, str | None] | None:
    """Return the adopted ``(trace_id, parent_span_id)``, or ``None`` if none was adopted."""
    return _adopted.get()


def set_adopted_context(trace_id: str, parent_span_id: str | None) -> None:
    """Record an inbound trace context for the next root span opened in this context."""
    _adopted.set((trace_id, parent_span_id))


# -- the producer side: publish where this process is ------------------------------------


def current_traceparent() -> str | None:
    """Return the current span as a W3C ``traceparent`` string, or ``None`` if none is active."""
    span = current_span()
    return format_traceparent(span.trace_id, span.span_id) if span else None


def current_trace_context() -> tuple[str, str] | None:
    """Return ``(trace_id, span_id)`` for the current span, or ``None`` if none is active.

    For callers who would rather move two fields than a formatted string — a Step Functions
    payload, a queue message attribute. It exists so nobody is pushed into
    :func:`current_span`, which is internal and hands back a mutable :class:`~.model.Span`.
    """
    span = current_span()
    return (span.trace_id, span.span_id) if span else None


# -- the W3C `baggage` header codec ------------------------------------------------------


def current_baggage_header() -> str:
    """Return the current baggage in W3C ``baggage`` header format (``""`` when empty).

    The baggage store is ``dict[str, object]`` but the wire format is text, so **non-string
    values are serialized with ``str()``** — put a dict in baggage and it arrives at the next
    process as its repr, not as a dict. Keys and values are percent-encoded, so a value
    containing ``,``, ``=`` or non-ASCII round-trips through :func:`parse_baggage_header`.
    """
    return format_baggage_header(get_baggage())


def format_baggage_header(baggage: dict[str, object]) -> str:
    """Serialize a baggage mapping to the W3C ``key1=value1,key2=value2`` format."""
    # safe="" so the separators themselves (`,` `=` `;`) are encoded rather than corrupting the
    # framing — the whole reason a value containing a comma can survive the hop.
    return ",".join(f"{quote(k, safe='')}={quote(str(v), safe='')}" for k, v in baggage.items())


def parse_baggage_header(header: object) -> dict[str, object] | None:
    """Parse a W3C ``baggage`` header into a mapping, or ``None`` if it is unusable.

    Total, like the ``traceparent`` parser: the header arrives from outside the process. Returns
    ``None`` for a non-string, an empty header, one over :data:`BAGGAGE_MAX_BYTES`, or a
    malformed member — never a partial parse, which would silently drop correlating fields.
    """
    if not isinstance(header, str) or not header.strip():
        return None
    if len(header.encode("utf-8", "replace")) > BAGGAGE_MAX_BYTES:
        return None
    parsed: dict[str, object] = {}
    for member in header.split(","):
        # W3C allows per-member properties after a `;` (e.g. `k=v;metadata`). Nothing here
        # consumes them, so they are dropped rather than treated as part of the value.
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
