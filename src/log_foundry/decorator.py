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

:func:`continue_trace` (SPEC-014) lives here rather than in ``api`` because it is the other half
of :func:`_open_span`'s hierarchy rules: it decides which trace the *next* root span joins, and
re-parents an already-open one. The ``traceparent`` codec it validates with stays in ``ids``.
"""

from __future__ import annotations

import asyncio
import atexit
import functools
import sys
import threading
from collections.abc import Callable
from time import monotonic
from typing import Any, TypeVar, cast, overload

from log_foundry import context
from log_foundry.config import _ensure_sink
from log_foundry.ids import (
    is_valid_span_id,
    is_valid_trace_id,
    new_span_id,
    new_trace_id,
    parse_traceparent,
)
from log_foundry.model import Span, backfill_baggage, end_event, start_event
from log_foundry.worker import Health, Worker

__all__ = ["continue_trace", "trace"]

# Bound on how much of a rejected inbound value is echoed into a stderr warning.
_MAX_REJECTED_ECHO = 64

# One background worker per process (SPEC-004), created lazily from the configured sink on the
# first flush. The double-checked lock makes concurrent first-flushes create exactly one.
_worker: Worker | None = None
_worker_lock = threading.Lock()
_atexit_registered = False  # register the drain exactly once per process, not per worker

F = TypeVar("F", bound=Callable[..., Any])


def _open_span(name: str, defaults: dict[str, object] | None) -> Span:
    """Mint a span, inheriting the trace/parent from the current context (arch §3).

    An inbound context adopted via :func:`continue_trace` is consulted **only** when no span is
    open (SPEC-014 FR-001): a nested call still inherits from its in-process parent, because
    moving it to the inbound span would sever it from the parent it actually ran inside.
    """
    parent = context.current_span()
    trace_id: str
    parent_span_id: str | None
    if parent is not None:
        trace_id, parent_span_id = parent.trace_id, parent.span_id
    else:
        adopted = context.get_adopted_context()
        trace_id, parent_span_id = adopted or (new_trace_id(), None)
    span = Span(
        trace_id=trace_id,
        span_id=new_span_id(),
        parent_span_id=parent_span_id,
        name=name,
        start_ts=monotonic(),
        defaults=defaults or {},
    )
    span.events.append(start_event(span))
    return span


def _warn_rejected(reason: str, value: object) -> None:
    """Report a rejected inbound context on stderr, as ``worker`` and ``SQSSink`` do.

    The offending value is echoed only as a **bounded ``repr``**. Unbounded is a log-injection
    surface — the value is attacker-controllable, and `repr` additionally escapes newlines and
    control characters so it cannot forge a second log line in an operator's console.
    """
    shown = repr(value)
    if len(shown) > _MAX_REJECTED_ECHO:
        shown = shown[:_MAX_REJECTED_ECHO] + "…"
    sys.stderr.write(f"log-foundry: ignoring inbound trace context ({reason}): {shown}\n")


def continue_trace(
    traceparent: str | None = None,
    *,
    trace_id: str | None = None,
    parent_span_id: str | None = None,
    baggage: str | None = None,
) -> bool:
    """Adopt an inbound trace context so this process's spans join the caller's trace.

    Accepts a W3C ``traceparent`` string or the ids directly. **If a span is already open and
    it is a root** (``parent_span_id is None`` — the ``@trace``-decorated entry point calling
    this on its first line), that span is re-parented in place and its buffered events are
    rewritten to match. A span that is *not* a root is left alone; a call with no span open
    re-parents nothing. In every case the context applies to the next root span opened here.

    Call it on the **first line** of the entry point: a child span that already finished has
    been handed to the worker and can no longer be rewritten.

    The adoption is **consumed by that one root span** and does not survive it (SPEC-024): the
    next root span opened with no fresh call of its own starts a new trace, which is what stops
    a warm container from logging every later invocation into the first caller's trace. So a
    batch that fans out to several *sibling* root spans needs one call per item — or, better, a
    single ``@trace`` entry point so the items are nested spans of one trace.

    One constraint on that release: it happens in whichever context the root span's ``finally``
    runs in. Adopt here and then dispatch the span into a **child** context — any
    ``asyncio.Task``, including the one ``asyncio.run`` creates — and the clear lands in the
    copy while this context keeps the adoption. Calling this on the entry point's first line, as
    above, is inside the span and unaffected; a caller who adopts before dispatching should call
    :func:`~log_foundry.reset_context` when the work is done.

    ``baggage`` is a W3C ``baggage`` header merged into the current context. It succeeds or
    fails independently of the trace context.

    Returns ``True`` when a context was adopted, ``False`` when nothing valid was supplied and
    a fresh trace is in use. Never raises.

    Adopting a context grants **nothing** — it selects a correlation id and confers no
    authority. The validation below is about output integrity (never emitting a malformed id
    into the event stream), not authorization; this is not a trust boundary.
    """
    adopted: tuple[str, str | None] | None = None
    if traceparent is not None:
        if trace_id is not None or parent_span_id is not None:
            # A programming error, not bad input: the caller supplied the same thing twice.
            _warn_rejected("both traceparent and explicit ids given; traceparent wins", traceparent)
        parsed = parse_traceparent(traceparent)
        if parsed is None:
            _warn_rejected("unparseable traceparent", traceparent)
        else:
            adopted = parsed
    elif trace_id is not None:
        if not is_valid_trace_id(trace_id):
            _warn_rejected("invalid trace_id", trace_id)
        elif parent_span_id is not None and not is_valid_span_id(parent_span_id):
            # Drop just the parent and join as another root rather than reject the whole
            # context: being in the right trace without a parent beats being in a fresh one.
            _warn_rejected("invalid parent_span_id; joining as a root", parent_span_id)
            adopted = (trace_id, None)
        else:
            # parent_span_id may legitimately be omitted — a consumer that knows the trace but
            # not the specific parent span is better off in the right trace than a fresh one.
            adopted = (trace_id, parent_span_id)
    # Nothing supplied at all is a silent no-op, not a rejection: `event.get("traceparent")`
    # legitimately yields None whenever the caller did not propagate one, and warning on that
    # would write a line on every uninstrumented invocation.

    if adopted is not None:
        context.set_adopted_context(*adopted)
        _reparent_current_span(*adopted)

    if baggage is not None:
        parsed_baggage = context.parse_baggage_header(baggage)
        if parsed_baggage is None:
            # Deliberately independent of the trace context above: losing correlating fields is
            # bad, and losing the trace join because one field was malformed is worse.
            _warn_rejected("unusable baggage header", baggage)
        else:
            context.set_baggage(**parsed_baggage)

    return adopted is not None


def _reparent_current_span(trace_id: str, parent_span_id: str | None) -> None:
    """Move an already-open **root** span into the adopted trace, events included.

    ``@trace`` opened the entry point's span before its body ran, so by the time
    :func:`continue_trace` is called on the first line that span already exists *and* has its
    ``span.start`` event buffered. :func:`~log_foundry.model.build_event` **snapshots** the ids
    into each event dict, so re-parenting only the dataclass would leave the buffered start
    event on the old trace — one span emitting its start on trace A and its end on trace B. A
    split trace is worse than no continuation at all, because it looks like data rather than a
    bug.
    """
    span = context.current_span()
    if span is None or span.parent_span_id is not None:
        # Not a root: it already belongs to an in-process trace, and moving it would sever it
        # from its own parent. The adopted context still applies to the next root span.
        return
    span.trace_id = trace_id
    span.parent_span_id = parent_span_id
    for event in span.events:
        event["trace_id"] = trace_id
        # `span_id` is never overwritten — the adopting span keeps its own identity and takes
        # the inbound span as its parent. Overwriting would give two processes the same span id
        # and break parent/child reconstruction downstream.
        if event.get("span_id") == span.span_id:
            event["parent_span_id"] = parent_span_id


def _get_worker() -> Worker:
    """Return the process worker, creating it lazily from the configured sink (FR-006).

    Registers the graceful drain via ``atexit`` exactly once, on first creation, so a program
    that logs and exits immediately still flushes its buffered events.
    """
    global _worker, _atexit_registered
    if _worker is None:
        with _worker_lock:
            if _worker is None:
                if not _atexit_registered:
                    atexit.register(_shutdown_worker)
                    _atexit_registered = True
                _worker = Worker(_ensure_sink())
    return _worker


def _shutdown_worker() -> None:
    """Drain + close the process worker if one was created (idempotent). Backs ``shutdown()``."""
    if _worker is not None:
        _worker.shutdown()


def _flush_worker(timeout: float | None = 5.0) -> bool:
    """Drain the process worker without retiring it. Backs ``flush()`` (SPEC-013 FR-003).

    Returns ``True`` when no worker exists: a process that never logged has nothing to drain,
    and building one here — with the thread and ``atexit`` registration :func:`_get_worker`
    brings — in order to flush nothing would be pure cost. So this deliberately does *not* call
    :func:`_get_worker`.
    """
    worker = _worker
    if worker is None:
        return True
    try:
        return worker.flush(timeout)
    except Exception:  # a flush is the call most likely to be made in a
        # `finally`, so the library must never be the reason a caller's function fails. Any
        # failure is reported by the return value instead (FR-003).
        return False


def _worker_health() -> Health:
    """Snapshot the process worker's counters, or zeros if none was ever created.

    Backs :func:`log_foundry.health` (SPEC-017 FR-005). Like :func:`_flush_worker` this
    deliberately does *not* call :func:`_get_worker`: starting a thread and registering an
    ``atexit`` drain in order to report an empty snapshot would be pure cost. That snapshot
    reads ``stopped_reason=None`` — a worker that was never created has not died, which is why
    SPEC-019 reports the terminal failure as a reason rather than an ``alive`` flag.
    """
    worker = _worker
    if worker is None:
        return Health(queued=0, dropped=0, failed_batches=0)
    return worker.health()


def _flush(span: Span) -> None:
    """Hand the finished span's events to the background worker — non-blocking (FR-001).

    Resolves/creates the worker via :func:`_get_worker` (whose sink comes from ``_ensure_sink``,
    so a zero-config ``@trace`` still falls back to ``StdoutSink`` rather than crashing).
    """
    _get_worker().submit(span.events)


def _close_span(span: Span, status: str, exc: BaseException | None) -> None:
    """Append the end event, complete the boundary events' baggage, then flush.

    Every path — sync, async, and the error path — closes through here, which is what makes one
    backfill call enough to cover all three.
    """
    span.events.append(end_event(span, status, exc))
    # SPEC-015: the boundary events were built with the baggage known at their construction —
    # empty for `span.start`, which is buffered before the body runs. Complete them from the
    # baggage live *now*, while the batch is still ours.
    backfill_baggage(span, context.get_baggage())
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
                # Before _open_span, which is what makes the new span current (SPEC-024).
                is_root = context.current_span() is None
                span = _open_span(name or fn.__qualname__, defaults)
                token = context.push_span(span)
                scope = context.push_baggage_scope() if is_root else None
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
                    if scope is not None:
                        context.pop_baggage_scope(scope)

            return cast("F", async_wrapper)

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Before _open_span, which is what makes the new span current (SPEC-024).
            is_root = context.current_span() is None
            span = _open_span(name or fn.__qualname__, defaults)
            token = context.push_span(span)
            scope = context.push_baggage_scope() if is_root else None
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
                if scope is not None:
                    context.pop_baggage_scope(scope)

        return cast("F", wrapper)

    return decorate(func) if func is not None else decorate
