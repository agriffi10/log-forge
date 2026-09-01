"""The ``@trace`` decorator — synchronous and async (arch §4–5, guide Phases 6, 8)."""

from __future__ import annotations

import asyncio
import functools
import threading
from collections.abc import Callable
from time import monotonic
from typing import TYPE_CHECKING, Any, TypeVar, cast, overload

from log_foundry import _diag, _lifecycle, context
from log_foundry.ids import (
    is_valid_span_id,
    is_valid_trace_id,
    new_span_id,
    new_trace_id,
    parse_traceparent,
)
from log_foundry.model import Span, backfill_baggage, end_event, start_event
from log_foundry.results import ContinueResult

if TYPE_CHECKING:
    import contextvars


__all__ = ["continue_trace", "trace"]

_sweep_lock = threading.Lock()
"""Serializes the span sweep, so two threads cannot deliver one span's buffer twice.

The detach is a load and a store, and ``contextvars`` copies the same ``Span`` object into every
task and into any ``copy_context()`` thread, so the two-reader case is ordinary rather than exotic
(SPEC-036 FR-001 AC-10). A flush is not a hot path; a sink lock it is not competing with.
"""
_loss_lock = threading.Lock()
"""Guards the two loss counters, and deliberately not the lifecycle lock (SPEC-036 FR-003 AC-5).

SPEC-028's ordering rule: a counter takes its own lock, because the orphan path runs on arbitrary
application threads and ``_lifecycle._state._lock`` is held across ``Worker(_ensure_sink())``
in :func:`_lifecycle._get_worker` — a blocking build a counter increment must never queue behind. It cannot
deadlock here either: the increment sits in ``api._log``'s ``except``, where
:func:`_lifecycle._note_orphan_emit` has already released that lock and the propagating
exception has
released any sink lock.
"""
_orphan_lost = 0
_in_span_lost = 0

F = TypeVar("F", bound=Callable[..., Any])


def _open_span(name: str, defaults: dict[str, object] | None) -> Span:
    """Mints a span, inheriting the trace and parent from the current context (arch §3).

    An inbound context adopted via :func:`continue_trace` is consulted only when no span is
    open (SPEC-014 FR-001): a nested call still inherits from its in-process parent, because
    moving it to the inbound span would sever it from the parent it actually ran inside.

    Args:
      name: The span name, normally the decorated function's qualified name.
      defaults: Per-decorator default fields, or ``None``.

    Returns:
      The new span, with its ``span.start`` event already buffered.

    Raises:
      None.
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


def continue_trace(
    traceparent: str | None = None,
    *,
    trace_id: str | None = None,
    parent_span_id: str | None = None,
    baggage: str | None = None,
) -> ContinueResult:
    """Adopts an inbound trace context so this process's spans join the caller's trace.

    Call it on the first line of the entry point: if a span is already open and it is a root,
    that span is re-parented in place and its buffered events are rewritten to match, while a
    child span that already finished has been handed to the worker and can no longer be
    rewritten. A span that is not a root is left alone, and a call with no span open
    re-parents nothing; in every case the context applies to the next root span opened here.

    The adoption is consumed by that one root span and does not survive it (SPEC-024), which
    is what stops a warm container logging every later invocation into the first caller's
    trace — so a batch fanning out to sibling root spans needs one call per item, or a single
    ``@trace`` entry point. The release happens in whichever context the root span's
    ``finally`` runs in, so a caller who adopts here and then dispatches into a child context
    should call :func:`~log_foundry.reset_context` when the work is done.

    Adopting a context grants nothing — it selects a correlation id and confers no authority.
    The validation is about output integrity, never emitting a malformed id into the event
    stream, not authorization; this is not a trust boundary.

    Args:
      traceparent: A W3C ``traceparent`` string, which wins if ids are also supplied.
      trace_id: The trace to join, as an alternative to a ``traceparent``.
      parent_span_id: The caller's span id. It may legitimately be omitted, since a consumer
        that knows the trace but not the specific parent is better off in the right trace
        than a fresh one, and an invalid one drops just the parent rather than the context.
      baggage: A W3C ``baggage`` header merged into the current context. It succeeds or fails
        independently of the trace context, because losing correlating fields is bad and
        losing the trace join because one field was malformed is worse.

    Returns:
      A :class:`ContinueResult`. Truthy when a context was adopted; falsy with a ``reason`` of
      ``"nothing-supplied"`` when no argument carried one, or ``"rejected"`` when something was
      supplied and was malformed — two outcomes that read identically as ``False`` today, where
      the second is a caller bug and the first is often a deliberate "continue if there is one".
      Falsy also means a fresh
      trace is in use. Supplying nothing at all is a silent no-op rather than a rejection,
      since a caller who did not propagate a header would otherwise get a line per
      invocation.

      **The verdict describes the trace context and nothing else.** ``baggage=`` is merged
      independently of it (SPEC-014: losing correlating fields is bad, and losing the trace join
      because one field was malformed is worse), so ``continue_trace(baggage=...)`` alone applies
      the baggage and still reports falsy — there was no trace context to adopt. The reason then
      distinguishes the two honestly, because ``"rejected"`` means **exactly** that a rejection
      was announced through ``_diag``: a malformed ``baggage=`` is a rejection, a well-formed one
      is not, and each reads back the way the stderr line does. A first version keyed the reason
      on "was any argument supplied", which reported ``"nothing-supplied"`` for a malformed
      baggage header *while writing the rejection line for it* — the discrimination FR-007 AC-3
      exists to provide, stated backwards.

    Raises:
      None.
    """
    adopted: tuple[str, str | None] | None = None
    announced = False
    if traceparent is not None:
        if trace_id is not None or parent_span_id is not None:
            _diag.rejected("both traceparent and explicit ids given; traceparent wins", traceparent)
        parsed = parse_traceparent(traceparent)
        if parsed is None:
            _diag.rejected("unparseable traceparent", traceparent)
            announced = True
        else:
            adopted = parsed
    elif trace_id is not None:
        if not is_valid_trace_id(trace_id):
            _diag.rejected("invalid trace_id", trace_id)
            announced = True
        elif parent_span_id is not None and not is_valid_span_id(parent_span_id):
            _diag.rejected("invalid parent_span_id; joining as a root", parent_span_id)
            adopted = (trace_id, None)
        else:
            adopted = (trace_id, parent_span_id)
    elif parent_span_id is not None:
        _diag.rejected("parent_span_id given with no trace_id to join", parent_span_id)
        announced = True

    if adopted is not None and _current_span_was_swept():
        _diag.rejected(
            "the current span has already been flushed; trace context refused",
            traceparent if traceparent is not None else str(trace_id),
        )
        adopted = None
        announced = True

    if adopted is not None:
        context.set_adopted_context(*adopted)
        _reparent_current_span(*adopted)

    if baggage is not None:
        parsed_baggage = context.parse_baggage_header(baggage)
        if parsed_baggage is None:
            _diag.rejected("unusable baggage header", baggage)
            announced = True
        else:
            context.set_baggage(**parsed_baggage)

    if adopted is not None:
        return ContinueResult(ok=True)
    return ContinueResult(ok=False, reason="rejected" if announced else "nothing-supplied")


def _current_span_was_swept() -> bool:
    """Reports whether an in-span ``flush()`` has already shipped this span's events.

    Read from ``context.current_span()`` — what :func:`_reparent_current_span` itself reads —
    **and only when that span is a root**, which is the other half of that function's own guard.
    A swept *child* is not a reason to refuse: the re-parent would have returned early on it and
    rewritten nothing, so a refusal there prevents no corruption. It would still be wrong to
    refuse — the two guards must agree, or the refusal fires where the thing it guards does not
    run — though the adoption it spares reaches less than it appears to: SPEC-024 clears the
    adopted context at the **root** span's close, so one made inside a child does not survive to
    the next root span either. ``continue_trace``'s documented placement on the entry
    point's first line is untouched either way: nothing has been swept that early.

    Args:
      None.

    Returns:
      Whether the current span is a root that has been swept.

    Raises:
      None.
    """
    span = context.current_span()
    return span is not None and span.parent_span_id is None and span.swept


def _reparent_current_span(trace_id: str, parent_span_id: str | None) -> None:
    """Moves an already-open root span into the adopted trace, events included.

    ``@trace`` opened the entry point's span before its body ran, so by the time
    :func:`continue_trace` is called that span already exists and has its ``span.start``
    buffered, and :func:`~log_foundry.model.build_event` snapshots the ids into each event
    dict. Re-parenting only the dataclass would leave one span emitting its start on trace A
    and its end on trace B, and a split trace is worse than no continuation at all because it
    looks like data rather than a bug.

    A span that is not a root is left alone: it already belongs to an in-process trace, and
    moving it would sever it from its own parent. ``span_id`` is never overwritten either —
    the adopting span keeps its own identity and takes the inbound span as its parent, where
    overwriting would give two processes the same span id.

    Args:
      trace_id: The adopted trace id.
      parent_span_id: The adopted parent span id, or ``None``.

    Returns:
      None.

    Raises:
      None.
    """
    span = context.current_span()
    if span is None or span.parent_span_id is not None:
        return
    span.trace_id = trace_id
    span.parent_span_id = parent_span_id
    for event in span.events:
        event["trace_id"] = trace_id
        if event.get("span_id") == span.span_id:
            event["parent_span_id"] = parent_span_id





























def _sweep_open_spans() -> None:
    """Hands the worker every event buffered on an open span in this context (SPEC-036 FR-001).

    An in-span event lives on ``span.events`` until the span *closes*, and ``Worker.flush``
    drains the *queue* — so a ``flush()`` called inside a ``@trace``d function, which is where
    the README's serverless recipe put it, had by construction nothing to drain. Measured: zero
    of two events delivered, every counter clean, and ``FlushResult`` reporting ``reason=None``.

    The span stays **open**: its events go now and its ``span.end`` arrives later in its own
    batch. Closing and reopening was rejected — it would emit a ``span.end`` the function never
    reached, with a fabricated ``duration_ms`` and ``status``.

    Two things must happen before the events leave, and both are why this is not a one-liner.
    The boundary events are backfilled **first**, because SPEC-015 completes them at close by
    iterating ``span.events`` and a swept buffer would ship ``span.start`` with ``fields={}`` —
    the very defect that spec exists to fix, recreated by any in-span flush. They therefore carry
    the baggage as of the flush rather than as of the close, which is a real semantic change and
    the alternative is mutating an event the worker already owns (SPEC-028). And the buffer is
    **detached by swap**, never cleared: ``clear()`` empties the same list object the worker was
    handed.

    The worker is created when there is something to submit, and **resolved before the buffer is
    detached**. That ordering is the whole of the difference between a lost batch and a delivered
    one: ``_get_worker`` can raise — it ends in ``Thread.start()`` — and a detach that has already
    happened leaves the events in a discarded local while the span reads empty and ``flush()``
    reports success. Measured with the failure injected: 3 of 4 events destroyed, every counter
    zero, on a span that was still open and would have delivered them at its close.
    ``Worker.submit`` raises nothing, so once it is reached the batch is safe. Creating the worker
    at all narrows SPEC-013's refusal rather than contradicting it — that exists so an *empty*
    flush does not stand up a thread, and a sweep that found buffered events is not an empty
    flush. A cold-start Lambda flushing before it returns is exactly this case: the worker is
    built when the first span *closes*, so inside the first traced call there is none.

    Concurrent sweeps are serialized on ``_sweep_lock``. The detach is a load and a store with a
    real gap between them, and two threads sharing one ``Span`` — which ``contextvars`` makes
    ordinary — can both read the same buffer and deliver it twice: measured, all 8 events
    duplicated with the window held open, and 9 of 25 runs with only a GIL yield between them.
    Rarely preempted on today's build is not a guarantee, and the floor is ``>=3.12`` where a
    free-threading build removes even that. A flush is not a hot path, so a single lock is the
    right cost.

    **The detach stays one statement, and the two orderings above are not in tension.** A draft
    hoisted the load to the top of the loop so a test could park on it — which put
    ``_get_worker()``, and therefore ``Thread.start()``, *inside* the load-to-store gap: measured,
    a sweep racing a close then delivered the whole batch twice, two ``span.end`` events among
    them, in 67 of 100 unforced trials against 0 before.

    One statement makes that gap **narrow, not closed**, and the difference matters. It compiles
    to ``LOAD_ATTR … STORE_ATTR`` with no ``CALL`` between, so CPython's eval breaker never runs
    there and today's GIL cannot switch inside it — 0 of 500 unforced trials. Forced with an
    opcode-level preemption it reproduces 10 of 10, and a free-threaded build removes the
    accident entirely while ``requires-python`` has no upper bound. So :func:`_flush` takes this
    same lock rather than relying on the width of a window: that is the *detach-vs-detach* race,
    and a process-global lock is the right instrument for it. The **append** race
    (``api._log`` versus a detach) is a different window needing a per-span lock, and
    ``architecture.md`` §13 declines it on cost.

    It reaches only the calling context's spans. ``contextvars`` offers no way to enumerate
    another thread's or task's context, so a ``flush()`` in a handler that fanned out does not
    reach what those tasks buffered.

    Args:
      None.

    Returns:
      None.

    Raises:
      Exception: Whatever building the worker raises. :func:`_flush_worker` guards it, because a
        flush is the call most likely to be made in a ``finally``.
    """
    with _sweep_lock:
        for span in context._live_span_stack():
            if not span.events:
                span.swept = True
                continue
            worker = _lifecycle._get_worker()
            backfill_baggage(span, context._live_baggage())
            span.swept = True
            buffered, span.events = span.events, []
            worker.submit(buffered)






def _note_orphan_loss() -> None:
    """Counts one event lost on the synchronous path (SPEC-036 FR-003).

    Called from ``api._log``'s orphan ``except``, which wraps the span construction, the event
    build, ``_ensure_sink`` and the emit — so a sink that failed to *construct* is counted here
    too, which an increment placed after ``sink.emit`` would miss.

    Args:
      None.

    Returns:
      None.

    Raises:
      None.
    """
    global _orphan_lost
    with _loss_lock:
        _orphan_lost += 1


def _note_in_span_loss() -> None:
    """Counts one event lost while being built inside a span (SPEC-036 FR-003).

    Separate from :func:`_note_orphan_loss` because the two aggregate different failure
    populations: this path cannot fail at ``emit``, so a non-zero count here always means the
    data, never the destination.

    Args:
      None.

    Returns:
      None.

    Raises:
      None.
    """
    global _in_span_lost
    with _loss_lock:
        _in_span_lost += 1


def _read_losses() -> tuple[int, int]:
    """Reads both loss counters under the lock they are written under.

    Args:
      None.

    Returns:
      The orphan and in-span loss counts, in that order.

    Raises:
      None.
    """
    with _loss_lock:
        return _orphan_lost, _in_span_lost






def _flush(span: Span) -> None:
    """Hands the finished span's events to the background worker, without blocking (FR-001).

    The worker is resolved or created via :func:`_get_worker`, whose sink comes from
    ``_ensure_sink``, so a zero-config ``@trace`` still falls back to ``StdoutSink`` rather
    than crashing.

    **The buffer is detached, not handed over** (SPEC-036 FR-004). ``submit`` used to take the
    live list, so a task outliving its parent span appended to a list the worker already owned:
    under the flush interval the event was delivered but ordered after ``span.end``, and over it
    silently lost — a pure race on a timer. The swap gives the worker the old list and leaves the
    span a fresh one, so the outcome stops depending on timing. A copy would do the same and
    allocates a second list per span on the hottest path in the library; the swap is free.

    The late append is now landing in a buffer nothing will emit, which is *also* loss — that
    half is ``api._log``'s, keyed on :attr:`Span.closed`.

    It takes ``_sweep_lock`` for the detach, because :func:`_sweep_open_spans` performs the same
    detach on the same attribute and a span can be swept and closed concurrently — measured, the
    whole batch delivered twice with two ``span.end`` events among them. The hold covers one
    statement and not the submit; ``Worker.submit`` is a ``put_nowait`` that never blocks, and the
    cost of the lock on the traced path measured within noise (+0.4% single-threaded, +0.9% across
    eight threads, 20,000 spans each). The **append** window this does not close is a different
    one, needs a per-span lock, and is declined in ``architecture.md`` §13.

    Args:
      span: The finished span whose buffered events are submitted.

    Returns:
      None.

    Raises:
      Exception: Whatever creating the worker or submitting raises; :func:`_end` is the guard.
    """
    with _sweep_lock:
        events, span.events = span.events, []
    _lifecycle._get_worker().submit(events)


type _SpanScope = tuple[
    Span | None,
    contextvars.Token[tuple[Span, ...]] | None,
    contextvars.Token[dict[str, object]] | None,
]


def _begin(name: str, defaults: dict[str, object] | None) -> _SpanScope:
    """Opens a span and makes it current, degrading rather than failing.

    Everything here runs before the caller's function body, so a failure would mean the
    library prevented the application from doing its work at all — the worst reading of arch
    §4, and worse than the close-path faults (SPEC-025 FR-001) because nothing has run yet.
    Whatever succeeded is kept, and :func:`_end` releases exactly that much.

    The order of the three steps is what keeps every outcome coherent. The baggage scope is
    taken first: opened last, a root call that lost only its scope would keep the span and
    leak its baggage into the next request, which is the SPEC-024 defect reappearing through a
    failure path. A span that exists but was never pushed is still closed and flushed by
    :func:`_end` — it is not current, so nested calls will not parent to it, but that is more
    than discarding it would preserve. The root test reads
    :func:`~log_foundry.context.current_span` before :func:`_open_span`, which is what makes
    the new span current.

    Args:
      name: The span name, normally the decorated function's qualified name.
      defaults: Per-decorator default fields, or ``None``.

    Returns:
      The span, span-stack token and baggage-scope token, any of which may be ``None`` when
      that step did not happen or did not succeed.

    Raises:
      None.
    """
    span: Span | None = None
    token: contextvars.Token[tuple[Span, ...]] | None = None
    scope: contextvars.Token[dict[str, object]] | None = None
    try:
        if context.current_span() is None:
            scope = context.push_baggage_scope()
        span = _open_span(name, defaults)
        token = context.push_span(span)
    except Exception as exc:
        _diag.absorbed(
            "opening a span",
            exc,
            "this call runs untraced" if span is None else "this call is traced incompletely",
        )
    return span, token, scope


def _end(
    span: Span | None,
    token: contextvars.Token[tuple[Span, ...]] | None,
    scope: contextvars.Token[dict[str, object]] | None,
    status: str,
    error: BaseException | None,
) -> None:
    """Closes the span and releases the context.

    This is called from the wrappers' ``finally`` once per span, with the outcome the body
    actually had (SPEC-025 FR-002) — the previous shape closed once in the ``try`` and again
    in the ``except``, so a close that failed on the success path emitted a second,
    contradicting ``span.end`` for a call that had returned normally.

    One ordering constraint, and only one: the baggage scope is released after the close, so
    SPEC-015's backfill still reads the baggage that was live inside the span. Both releases
    are total in their own right (SPEC-024, SPEC-025), so neither needs a guard here. The
    ``is not None`` checks are load-bearing rather than defensive tidiness: ``pop_span(None)``
    would fall through that function's own guard to ``set(())`` and wipe the whole stack,
    detaching the parent of an untraced nested call and splitting its trace.

    Args:
      span: The span to close, or ``None`` if none was opened.
      token: The span-stack token to release, or ``None``.
      scope: The baggage-scope token to release, or ``None``.
      status: The span outcome, ``"ok"`` or ``"error"``.
      error: The exception the body raised, if any.

    Returns:
      None.

    Raises:
      None on a library fault. The catch is ``Exception``, never ``BaseException``: a
        ``KeyboardInterrupt`` or ``SystemExit`` arriving here is the operator's or the
        runtime's intent and must still reach the caller.
    """
    if span is not None:
        try:
            _close_span(span, status, error)
        except Exception as exc:
            _diag.absorbed("closing a span", exc, "the span's events were lost")
    if token is not None:
        context.pop_span(token)
    if scope is not None:
        context.pop_baggage_scope(scope)


def _close_span(span: Span, status: str, exc: BaseException | None) -> None:
    """Appends the end event, completes the boundary events' baggage, then flushes.

    Every path — sync, async, and the error path — closes through here, which is what makes
    one backfill call enough to cover all three. The boundary events were built with the
    baggage known at their construction, empty for ``span.start``, so SPEC-015 completes them
    from the baggage live now, while the batch is still ours.

    ``closed`` is set **before** the flush, not after (SPEC-036 FR-004). ``_flush`` can raise —
    building the worker, or the queue path — and :func:`_end`'s guard absorbs it, so setting the
    flag afterwards would leave it ``False`` on exactly the runs where a later append most needs
    the orphan route. ``sinks/base.py`` settles the general form of this: set it before releasing
    anything.

    Args:
      span: The span being closed.
      status: The span outcome, ``"ok"`` or ``"error"``.
      exc: The exception that ended the span, if it failed.

    Returns:
      None.

    Raises:
      Exception: Whatever building the end event or flushing raises; :func:`_end` is the
        guard.
    """
    span.events.append(end_event(span, status, exc))
    backfill_baggage(span, context._live_baggage())
    span.closed = True
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
    """Traces a function call as a span, usable bare as ``@trace`` or with arguments.

    Args:
      func: The function being decorated when used bare, otherwise ``None``.
      name: Overrides the span name, which defaults to ``func.__qualname__``.
      defaults: Per-decorator default fields added to every event on the span.

    Returns:
      The wrapped function, or a decorator when called with arguments.

    Raises:
      None.
    """

    def decorate(fn: F) -> F:
        """Wraps one function, selecting the sync or async wrapper at decoration time.

        :func:`asyncio.iscoroutinefunction` makes the choice once. The two wrappers are
        deliberate near-duplicates — the only difference is ``await fn(...)`` — because the
        sync/async split is a hard boundary, and ``contextvars`` already propagates the span
        stack and baggage across ``await`` points and concurrent tasks (arch §5), so the async
        span opens when the coroutine actually runs and closes when it finishes.

        Args:
          fn: The function to wrap.

        Returns:
          The wrapper, typed as the original function.

        Raises:
          None.
        """
        if asyncio.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                """Runs the coroutine inside a span, re-raising its exception unchanged.

                Non-swallowing per arch §4: the outcome is recorded and the original
                exception re-raised. The catch is ``BaseException`` so that
                ``asyncio.CancelledError`` is recorded as an error end event rather than
                leaving an unclosed span. ``status`` and ``error`` are deleted in a nested
                ``finally`` for the reason ``except ... as exc`` auto-deletes its target:
                ``error`` holds an exception whose traceback holds this frame, so keeping it
                past the handler builds a cycle only the collector can break, pinning the
                caller's own frames and locals with it.

                Args:
                  *args: Positional arguments forwarded to the wrapped coroutine.
                  **kwargs: Keyword arguments forwarded to the wrapped coroutine.

                Returns:
                  Whatever the wrapped coroutine returns.

                Raises:
                  BaseException: Whatever the wrapped coroutine raises, unchanged.
                """
                span, token, scope = _begin(name or fn.__qualname__, defaults)
                status, error = "ok", None
                try:
                    result = await fn(*args, **kwargs)
                except BaseException as exc:
                    status, error = "error", exc
                    raise
                finally:
                    try:
                        _end(span, token, scope, status, error)
                    finally:
                        del status, error
                return result

            return cast("F", async_wrapper)

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            """Runs the function inside a span, re-raising its exception unchanged.

            Non-swallowing per arch §4: the outcome is recorded and the original exception
            re-raised. ``status`` and ``error`` are deleted in a nested ``finally`` for the
            reason ``except ... as exc`` auto-deletes its target: ``error`` holds an exception
            whose traceback holds this frame, so keeping it past the handler builds a cycle
            only the collector can break, pinning the caller's own frames and locals with it.

            Args:
              *args: Positional arguments forwarded to the wrapped function.
              **kwargs: Keyword arguments forwarded to the wrapped function.

            Returns:
              Whatever the wrapped function returns.

            Raises:
              BaseException: Whatever the wrapped function raises, unchanged.
            """
            span, token, scope = _begin(name or fn.__qualname__, defaults)
            status, error = "ok", None
            try:
                result = fn(*args, **kwargs)
            except BaseException as exc:
                status, error = "error", exc
                raise
            finally:
                try:
                    _end(span, token, scope, status, error)
                finally:
                    del status, error
            return result

        return cast("F", wrapper)

    return decorate(func) if func is not None else decorate
