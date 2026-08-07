"""The ``@trace`` decorator — synchronous and async (arch §4–5, guide Phases 6, 8)."""

from __future__ import annotations

import asyncio
import atexit
import functools
import threading
from collections.abc import Callable
from time import monotonic
from typing import TYPE_CHECKING, Any, TypeVar, cast, overload

from log_foundry import _diag, context
from log_foundry.config import _ensure_sink
from log_foundry.ids import (
    is_valid_span_id,
    is_valid_trace_id,
    new_span_id,
    new_trace_id,
    parse_traceparent,
)
from log_foundry.model import Span, backfill_baggage, end_event, start_event
from log_foundry.worker import DEFAULT_SHUTDOWN_TIMEOUT, DEFAULT_SWAP_TIMEOUT, Health, Worker

if TYPE_CHECKING:
    import contextvars

    from log_foundry.sinks.base import Sink

__all__ = ["continue_trace", "trace"]

_worker: Worker | None = None
_worker_lock = threading.Lock()
_atexit_registered = False
_orphan_close_owed = False
_orphan_sink_closed = False
_orphan_retired = False

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
) -> bool:
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
      True when a context was adopted, False when nothing valid was supplied and a fresh
      trace is in use. Supplying nothing at all is a silent no-op rather than a rejection,
      since a caller who did not propagate a header would otherwise get a line per
      invocation.

    Raises:
      None.
    """
    adopted: tuple[str, str | None] | None = None
    if traceparent is not None:
        if trace_id is not None or parent_span_id is not None:
            _diag.rejected("both traceparent and explicit ids given; traceparent wins", traceparent)
        parsed = parse_traceparent(traceparent)
        if parsed is None:
            _diag.rejected("unparseable traceparent", traceparent)
        else:
            adopted = parsed
    elif trace_id is not None:
        if not is_valid_trace_id(trace_id):
            _diag.rejected("invalid trace_id", trace_id)
        elif parent_span_id is not None and not is_valid_span_id(parent_span_id):
            _diag.rejected("invalid parent_span_id; joining as a root", parent_span_id)
            adopted = (trace_id, None)
        else:
            adopted = (trace_id, parent_span_id)

    if adopted is not None:
        context.set_adopted_context(*adopted)
        _reparent_current_span(*adopted)

    if baggage is not None:
        parsed_baggage = context.parse_baggage_header(baggage)
        if parsed_baggage is None:
            _diag.rejected("unusable baggage header", baggage)
        else:
            context.set_baggage(**parsed_baggage)

    return adopted is not None


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


def _get_worker() -> Worker:
    """Returns the process worker, creating it lazily from the configured sink (FR-006).

    The graceful drain is registered via ``atexit`` exactly once, on first creation, so a
    program that logs and exits immediately still flushes its buffered events. The
    double-checked lock makes concurrent first-flushes create exactly one worker.

    Args:
      None.

    Returns:
      The process-wide worker.

    Raises:
      Exception: Whatever constructing the sink or worker raises.
    """
    global _worker
    if _worker is None:
        with _worker_lock:
            if _worker is None:
                _register_exit_handler()
                _worker = Worker(_ensure_sink())
    return _worker


def _register_exit_handler() -> None:
    """Registers the one ``atexit`` handler that covers both delivery paths (SPEC-031 FR-006).

    One registration, not two, and one flag guarding it. :func:`_shutdown_worker` handles the
    worker path *and* the orphan path, so an orphan log arming this does not cost a later
    ``@trace`` its exit drain — which reusing a worker-only registration flag would. Two
    handlers would be worse still: ``atexit`` runs LIFO, so the second would close a sink the
    first had already closed. What is made once-only is the *close*, not the registration.

    Callers hold ``_worker_lock``.

    Args:
      None.

    Returns:
      None.

    Raises:
      None.
    """
    global _atexit_registered
    if not _atexit_registered:
        atexit.register(_shutdown_worker)
        _atexit_registered = True


def _note_orphan_emit() -> None:
    """Records that a level call with no span reached the sink (SPEC-031 FR-006).

    This is what arms the exit-time close, and it is deliberately keyed on an event having
    *landed* rather than on a sink existing: ``configure()`` runs ``_ensure_sink()``
    unconditionally, so a bare ``configure(service=…)`` has already built a ``StdoutSink``,
    and keying on that would close a sink nothing was ever written to.

    The unlocked read is the fast path on a per-call route — the flag is written once and
    never cleared, so a racing reader either sees it set or takes the lock and finds it set
    there.

    Args:
      None.

    Returns:
      None.

    Raises:
      None.
    """
    global _orphan_close_owed
    if _orphan_close_owed:
        return
    with _worker_lock:
        if not _orphan_close_owed:
            _register_exit_handler()
            _orphan_close_owed = True


def _close_orphan_sink() -> None:
    """Closes a sink only the orphan path ever wrote to, once (SPEC-031 FR-006).

    A process that never opens a span builds no worker, so nothing owned the sink's close and
    nothing performed it: on a locally-buffering sink every event died in the client's batch,
    on a synchronous one the flush and the resource were lost, and ``health()`` read all-clear
    because every field it carries describes a worker that does not exist.

    A live worker owns the close instead, and this returns — that is what makes a mixed
    process exactly one ``close()`` in either order. It also inherits the worker's reasons for
    *not* closing: an expired :meth:`Worker.shutdown` leaves the sink open because the drain
    thread may still be inside ``emit``.

    The once-only flag is set ahead of the close, as ``Worker.shutdown``'s is: a second
    ``close()`` on a sink that partially released its resources is worse than an unclosed one.

    Args:
      None.

    Returns:
      None.

    Raises:
      None. This runs from ``atexit``, where an escaping exception makes CPython print a
        traceback carrying the message arch §6 keeps out of anything the library says about
        itself. ``Exception``, never ``BaseException`` (SPEC-025 FR-004).
    """
    global _orphan_sink_closed
    if _worker is not None:
        return
    with _worker_lock:
        if not _orphan_close_owed or _orphan_sink_closed:
            return
        _orphan_sink_closed = True
    try:
        _ensure_sink().close()
    except Exception as exc:
        _diag.absorbed("closing the sink", exc, "it may still hold its resources")


def _shutdown_worker(timeout: float | None = DEFAULT_SHUTDOWN_TIMEOUT) -> None:
    """Drains and closes the process worker, or closes an orphan-only sink, backing ``shutdown()``.

    The ``atexit`` registration binds this function, so the exit path gets the bounded form
    and its default (SPEC-027 FR-004) — an unbounded join in an ``atexit`` handler is a
    process that will not exit. Idempotent on both paths.

    ``_orphan_retired`` is set unconditionally and read only when there is no worker, which is
    what makes ``health().retired`` truthful for a process that shut down without ever
    building one (SPEC-031 FR-006). No worker is created here to answer it: standing up a
    thread at exit to prove there is nothing to drain is pure cost, the same refusal
    :func:`_swap_sink` and :func:`_flush_worker` already make.

    Args:
      timeout: Seconds to wait for the drain, or ``None`` to wait indefinitely.

    Returns:
      None.

    Raises:
      None.
    """
    global _orphan_retired
    _orphan_retired = True
    if _worker is not None:
        _worker.shutdown(timeout)
        return
    _close_orphan_sink()


def _swap_sink(new_sink: Sink, timeout: float | None = DEFAULT_SWAP_TIMEOUT) -> None:
    """Retargets the process worker at a new sink, backing a late ``configure(sink=...)``.

    Like :func:`_flush_worker` this deliberately does not call :func:`_get_worker`: a process
    that has not logged has captured no sink, so there is nothing to swap and building a thread
    to prove it would be pure cost — that is also the case where the old behaviour was already
    correct (SPEC-030 FR-003).

    Args:
      new_sink: The sink already written to the config, to be made the live delivery target.
      timeout: Seconds bounding the whole swap — both drains and the close of the previous
        sink share it as one deadline.

    Returns:
      None.

    Raises:
      None. This runs inside ``configure()``, which has never raised for anything but a
        rejected ceiling, and a sink swap that fails must not become the reason an application
        cannot start.
    """
    worker = _worker
    if worker is None:
        return
    try:
        worker.swap_sink(new_sink, timeout)
    except Exception as exc:
        _diag.absorbed(
            "swapping the log sink", exc, "events may still be delivered to the previous sink"
        )


def _flush_worker(timeout: float | None = 5.0) -> bool:
    """Drains the process worker without retiring it, backing ``flush()`` (SPEC-013 FR-003).

    This deliberately does not call :func:`_get_worker`: a process that never logged has
    nothing to drain, and building a worker — with the thread and ``atexit`` registration
    that brings — in order to flush nothing would be pure cost.

    Args:
      timeout: Seconds to wait for the drain, or ``None`` to wait indefinitely.

    Returns:
      Whether everything outstanding was delivered, and True when no worker exists.

    Raises:
      None. A flush is the call most likely to be made in a ``finally``, so the library must
        never be the reason a caller's function fails; a failure is reported by the return
        value instead (FR-003).
    """
    worker = _worker
    if worker is None:
        return True
    try:
        return worker.flush(timeout)
    except Exception:
        return False


def _worker_health() -> Health:
    """Snapshots the process worker's counters, or zeros if none was ever created.

    Like :func:`_flush_worker` this deliberately does not call :func:`_get_worker`: starting a
    thread and registering an ``atexit`` drain in order to report an empty snapshot would be
    pure cost. That snapshot reads a ``stopped_reason`` of ``None`` — a worker that was never
    created has not died, which is why SPEC-019 reports the terminal failure as a reason
    rather than an ``alive`` flag.

    ``retired`` is the one field synthesized rather than zeroed (SPEC-031 FR-006). It records
    an action the caller took, not a state of the worker, so it stays true in a process that
    called ``shutdown()`` without ever building one — where it was previously vacuous, and the
    whole snapshot read all-clear over a sink that had just been closed.
    ``submitted_after_shutdown`` is deliberately **not** synthesized alongside it: SPEC-030
    defines that count as submissions queued where nothing will drain them, and a later orphan
    log is refused at the closed sink and announced instead. The two are not the same claim.

    Args:
      None.

    Returns:
      The worker's health snapshot, backing :func:`log_foundry.health` (SPEC-017 FR-005).

    Raises:
      None.
    """
    worker = _worker
    if worker is None:
        return Health(queued=0, dropped=0, failed_batches=0, retired=_orphan_retired)
    return worker.health()


def _flush(span: Span) -> None:
    """Hands the finished span's events to the background worker, without blocking (FR-001).

    The worker is resolved or created via :func:`_get_worker`, whose sink comes from
    ``_ensure_sink``, so a zero-config ``@trace`` still falls back to ``StdoutSink`` rather
    than crashing.

    Args:
      span: The finished span whose buffered events are submitted.

    Returns:
      None.

    Raises:
      Exception: Whatever creating the worker or submitting raises; :func:`_end` is the guard.
    """
    _get_worker().submit(span.events)


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
