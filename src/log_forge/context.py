"""Context propagation: span stack + baggage via contextvars (arch §5, guide Phase 4).

``contextvars`` (not thread-locals) is correct under both threads and asyncio — each task
inherits its own copy — so ``log_forge.info(...)`` can find "the span I'm inside" with no
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

if TYPE_CHECKING:
    from log_forge.model import Span

__all__ = [
    "current_span",
    "push_span",
    "pop_span",
    "get_baggage",
    "set_baggage",
]

_span_stack: contextvars.ContextVar[tuple[Span, ...]] = contextvars.ContextVar(
    "log_forge_span_stack", default=()
)
_baggage: contextvars.ContextVar[dict[str, object]] = contextvars.ContextVar(
    "log_forge_baggage", default={}
)


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
