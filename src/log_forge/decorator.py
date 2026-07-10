"""The ``@trace`` decorator — synchronous and async (arch §4–5, guide Phases 6, 8).

Opens a span on enter and closes it on exit (success *or* exception), maintaining the
trace/parent hierarchy from the context stack, then flushes the finished span straight to
the configured sink. The decorator is **non-swallowing**: it records the failure and
re-raises the original exception unchanged (arch §4).

At decoration time :func:`asyncio.iscoroutinefunction` selects a sync or async wrapper. The
two are deliberate near-duplicates — the only difference is ``await fn(...)`` — because the
sync/async split is a hard boundary; ``contextvars`` already propagates the span stack and
baggage correctly across ``await`` points and concurrent tasks (arch §5), so the async span
opens when the coroutine actually runs and closes when it finishes, with no new machinery.

Flushing is synchronous here; the background worker (SPEC-004) later swaps only
:func:`_flush` for a non-blocking handoff — the lifecycle below is untouched.
"""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Callable
from time import monotonic
from typing import Any, TypeVar, cast, overload

from log_forge import context
from log_forge.config import _ensure_sink
from log_forge.ids import new_span_id, new_trace_id
from log_forge.model import Span, end_event, start_event

__all__ = ["trace"]

F = TypeVar("F", bound=Callable[..., Any])


def _open_span(name: str, defaults: dict[str, object] | None) -> Span:
    """Mint a span, inheriting the trace/parent from the current context (arch §3)."""
    parent = context.current_span()
    span = Span(
        trace_id=parent.trace_id if parent else new_trace_id(),
        span_id=new_span_id(),
        parent_span_id=parent.span_id if parent else None,
        name=name,
        start_ts=monotonic(),
        defaults=defaults or {},
    )
    span.events.append(start_event(span))
    return span


def _flush(span: Span) -> None:
    """Ship the finished span's buffered events to the configured sink.

    Resolves the sink via ``_ensure_sink`` so a zero-config ``@trace`` falls back to
    ``StdoutSink`` instead of crashing. SPEC-004 replaces this with ``worker.submit(...)``.
    """
    _ensure_sink().emit(span.events)


def _close_span(span: Span, status: str, exc: BaseException | None) -> None:
    """Append the end event (so the flushed queue is complete), then flush."""
    span.events.append(end_event(span, status, exc))
    _flush(span)


@overload
def trace(func: F) -> F: ...
@overload
def trace(
    *, name: str | None = ..., defaults: dict[str, object] | None = ...
) -> Callable[[F], F]: ...


def trace(
    func: F | None = None,
    *,
    name: str | None = None,
    defaults: dict[str, object] | None = None,
) -> F | Callable[[F], F]:
    """Trace a function call as a span. Usable bare (``@trace``) or with args.

    ``@trace(name=..., defaults=...)`` overrides the span name (default:
    ``func.__qualname__``) and adds per-decorator default fields.
    """

    def decorate(fn: F) -> F:
        if asyncio.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                span = _open_span(name or fn.__qualname__, defaults)
                token = context.push_span(span)
                try:
                    result = await fn(*args, **kwargs)
                    _close_span(span, "ok", None)
                    return result
                except BaseException as exc:
                    # Non-swallowing (arch §4): record the error, then re-raise unchanged.
                    # BaseException covers asyncio.CancelledError — a cancelled coroutine is
                    # recorded as an error end event, never left with an unclosed span.
                    _close_span(span, "error", exc)
                    raise
                finally:
                    context.pop_span(token)

            return cast(F, async_wrapper)

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            span = _open_span(name or fn.__qualname__, defaults)
            token = context.push_span(span)
            try:
                result = fn(*args, **kwargs)
                _close_span(span, "ok", None)
                return result
            except BaseException as exc:
                # Non-swallowing (arch §4): record the error, then re-raise unchanged.
                _close_span(span, "error", exc)
                raise
            finally:
                context.pop_span(token)

        return cast(F, wrapper)

    return decorate(func) if func is not None else decorate
