"""The ``@trace`` decorator — synchronous and async (arch §4–5, guide Phases 6, 8)."""

from __future__ import annotations

import asyncio
import atexit
import functools
import threading
from dataclasses import replace
from collections.abc import Callable
from time import monotonic
from typing import TYPE_CHECKING, Any, TypeVar, cast, overload

from log_foundry import _diag, _lifecycle, context
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
_orphan_sink: Sink | None = None
"""The sink the orphan path owns the close of, or ``None`` when nothing is owed (SPEC-033 FR-001).

Written only under ``_worker_lock``; read without it on the emit hot path, where the read is
**stale, never invalid** — a reference read is atomic, a stale mismatch self-corrects under the
lock, and a stale match is reachable only when an emit races a close, which is the lifecycle error
SPEC-030 documents rather than a new one.
"""
_orphan_closed_sink: Sink | None = None
"""The most recently closed orphan-owned sink, refused re-arming (SPEC-033 FR-001).

An identity rather than SPEC-031's boolean, which made the close once per *process* and so left a
sink configured after ``shutdown()`` unclosed forever. It is a single slot: handing back a sink
already swapped out re-admits it, which arch §13 records rather than fixes, since tracking every
sink ever closed would pin them all against collection to fix what the worker path does not fix
either.
"""
_orphan_stop = threading.Event()
"""The stop signal handed to a sink no live worker owns (SPEC-033 FR-004).

Replaced with a fresh event whenever it is already set, because an ``Event`` is set once and never
cleared and ``sinks/_retry.wait`` returns immediately on a set one — a sink still holding the
shutdown's event has every backoff collapsed to zero.
"""
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
    global _worker, _orphan_sink
    if _worker is None:
        with _worker_lock:
            if _worker is None:
                _register_exit_handler()
                _worker = Worker(_ensure_sink())
                _orphan_sink = None
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


def _note_orphan_emit(sink: Sink) -> None:
    """Records which sink a level call with no span reached (SPEC-031 FR-006, SPEC-033 FR-001).

    This arms the exit-time close, and it is deliberately keyed on an event having *landed*
    rather than on a sink existing: ``configure()`` runs ``_ensure_sink()`` unconditionally, so a
    bare ``configure(service=…)`` has already built a ``StdoutSink``, and keying on that would
    close a sink nothing was ever written to.

    It records the sink **object**, not a flag. ``configure()`` assigns ``_config.sink`` before
    it calls the swap, so by the time anything could close the previous sink the config no longer
    names it — a boolean cannot say which one is owed. A sink already recorded as closed is
    refused re-arming, which is what stops a post-``shutdown()`` emit against a closed sink
    causing a second ``close()`` on it.

    The unlocked fast path is a stale read, never an invalid one: a reference read is atomic, a
    stale mismatch simply takes the lock and re-checks, and a stale match is reachable only when
    an emit races a close — the lifecycle error SPEC-030 documents rather than one introduced
    here.

    The stop-signal offer is keyed on the *sink* rather than on the record (SPEC-033 FR-004), so
    it also reaches a sink that is latched closed and still being emitted to; an arming-keyed
    offer would leave that one holding the shutdown's set event and backing off not at all.

    Args:
      sink: The sink this call is about to emit to.

    Returns:
      None.

    Raises:
      None.
    """
    global _orphan_sink
    if (sink is _orphan_sink or sink is _orphan_closed_sink) and not _orphan_stop.is_set():
        return
    with _worker_lock:
        _offer_orphan_signal(sink)
        if sink is _orphan_sink or sink is _orphan_closed_sink:
            return
        _register_exit_handler()
        _orphan_sink = sink


def _live_worker() -> Worker | None:
    """Returns the process worker only while it is still delivering (SPEC-033 FR-002).

    Three guards ask who owns a sink, and two of them mean a *live* owner. A retired worker holds
    its sink forever — :meth:`Worker.swap_sink` returns early once shut down — so keying on a
    worker merely existing hands the swap to something that will do nothing with it, and the sink
    adopted afterwards is closed by no one: measured, ``configure(A)`` → ``@trace`` →
    ``shutdown()`` → ``configure(B)`` → ``info()`` → ``configure(C)`` left B unclosed with its
    event undelivered and every counter clean, which is the failure shape this spec exists to
    close.

    :func:`_close_orphan_sink` deliberately does **not** use this: there a retired worker's
    ownership is exactly what must make it decline, since an expired shutdown leaves the drain
    thread possibly still inside that sink's ``emit``.

    Args:
      None.

    Returns:
      The worker while it is still delivering, or ``None`` when there is none or it has retired.

    Raises:
      None.
    """
    worker = _worker
    return None if worker is None or worker.retired else worker


def _offer_orphan_signal(sink: Sink) -> None:
    """Gives a sink no live worker owns an unset stop signal (SPEC-033 FR-004).

    An orphan-only process never receives one otherwise — ``Worker._offer_stop_signal`` is the
    only caller and there is no worker — so SPEC-027's guarantee that a shutdown cuts a backoff
    short is false on this path, and the inline close at exit can sit behind an uninterruptible
    wait held by another orphan writer.

    The skip is keyed on **ownership**, not on a worker merely existing. A retired worker keeps
    its old sink forever (``Worker.swap_sink`` returns early once ``_shutdown_done``) while every
    orphan event goes to a newly configured one, so skipping on existence would leave that live
    sink uninterruptible for the rest of the process. Where a worker does own the sink, its own
    ``_stop`` is already there and is the event its drain loop waits on; overwriting it would
    leave the drain thread serving a full backoff across ``Worker.shutdown``'s join, which is the
    global pause SPEC-027 exists to remove.

    Ownership alone is not the whole predicate either, and this site is the one place in the
    module where neither of the two categories fits (SPEC-035 FR-001). :func:`_live_worker` was
    wrong: ``retired`` latches on **entry** to ``Worker.shutdown``, so for the whole of the drain
    the skip stopped applying and an orphan log handed the sink a fresh unset event — precisely
    the one the drain thread was about to wait on. Measured, a 20 s backoff against
    ``shutdown(timeout=3)`` was then still outstanding when the shutdown expired at 3.01 s with
    the sink left open — the wait was never cut, not merely slow. But bare ``_worker.sink is
    sink`` is wrong in the opposite direction: it skips for a worker whose shutdown has
    **finished**, leaving a sink still being written to holding a set event that can never clear,
    which is SPEC-033 FR-004's tight retry loop and is covered by a test that spec shipped. The
    predicate is therefore the ownership term **conjoined with** ``Worker.draining`` — the
    moment as well as the identity, since without the identity an orphan log to sink Y would be
    skipped merely because a live worker is draining into sink X. That property's docstring
    carries the rest of the reasoning.

    A fresh event replaces one that is already set, because an ``Event`` never clears and
    ``sinks/_retry.wait`` returns immediately on a set one — a sink handed the shutdown's event
    would have every subsequent backoff collapsed to zero, which against a rate-limited
    destination is a tight retry loop. SPEC-027's contract is "cut short by a shutdown", not
    "never wait again".

    Callers hold ``_worker_lock``.

    Args:
      sink: The sink to offer a signal to.

    Returns:
      None.

    Raises:
      None.
    """
    global _orphan_stop
    worker = _worker
    if worker is not None and worker.sink is sink and worker.draining:
        return
    if _orphan_stop.is_set():
        _orphan_stop = threading.Event()
    _lifecycle.offer_stop_signal(sink, _orphan_stop)


def _close_orphan_sink() -> None:
    """Closes a sink only the orphan path ever wrote to, once (SPEC-031 FR-006).

    A process that never opens a span builds no worker, so nothing owned the sink's close and
    nothing performed it: on a locally-buffering sink every event died in the client's batch,
    on a synchronous one the flush and the resource were lost, and ``health()`` read all-clear
    because every field it carries describes a worker that does not exist.

    A worker that owns *this* sink closes it instead, and this returns — that is what makes a
    mixed process exactly one ``close()`` in either order. It also inherits that worker's reasons
    for *not* closing: an expired :meth:`Worker.shutdown` leaves the sink open because the drain
    thread may still be inside ``emit``, and there ``_worker.sink is owed`` still holds.

    The guard is **ownership**, not a worker merely existing (SPEC-033 FR-002). The two stop
    being the same question the moment the worker is retired: ``Worker.swap_sink`` returns early
    once ``_shutdown_done``, so a retired worker keeps its old sink forever while every orphan
    event goes to a newly configured one — measured, a sink configured after ``shutdown()`` was
    then closed by nothing at all, losing a locally-buffering sink's whole batch while
    ``health()`` read ``retired=True, submitted_after_shutdown=0, failed_batches=0``.

    That check is read **under** ``_worker_lock``, not ahead of it, because :func:`_get_worker`
    assigns ``_worker`` while holding that same lock. Unlocked, a ``shutdown()`` racing a first
    ``@trace`` could read ``None``, block behind the worker's construction, and then close the
    sink underneath the worker that had just captured it — reproduced with an injected
    preemption point, the way SPEC-028 demonstrates the races that need one.

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
    global _orphan_sink, _orphan_closed_sink
    with _worker_lock:
        owed = _orphan_sink
        if owed is None:
            return
        if _worker is not None and _worker.sink is owed:
            return
        _orphan_sink = None
        _orphan_closed_sink = owed
    try:
        owed.close()
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

    The worker branch runs :func:`_close_orphan_sink` before returning (SPEC-033 FR-002). A
    retired worker owns nothing further, so a sink adopted after its shutdown is the orphan
    path's to close; that function's ownership guard is what keeps this from double-closing the
    sink the worker just closed itself.

    ``_orphan_stop`` is set **before** delegating, so a sink parked in a backoff is released
    while :meth:`Worker.shutdown` is still draining rather than after it has given up waiting.

    The closer grace is granted **once**, by whichever path owns this call.
    :meth:`Worker.shutdown` already grants it — on its successful path and on its idempotent one,
    which is what covers a first shutdown that expired before reaching it — so joining again here
    would charge a second full ``DEFAULT_CLOSER_GRACE`` against the same exit: measured 4.01 s
    against a 2 s grace. The orphan branch grants it instead, where nothing else will, and gets it
    on every path including the one where nothing was armed and the idempotent second call.

    Args:
      timeout: Seconds to wait for the drain, or ``None`` to wait indefinitely.

    Returns:
      None.

    Raises:
      None.
    """
    global _orphan_retired
    _orphan_retired = True
    _orphan_stop.set()
    deadline = None if timeout is None else monotonic() + timeout
    if _worker is not None:
        _worker.shutdown(timeout)
        _close_orphan_sink()
        return
    _close_orphan_sink()
    _lifecycle.join_closers(None if deadline is None else max(0.0, deadline - monotonic()))


def _swap_sink(new_sink: Sink, timeout: float | None = DEFAULT_SWAP_TIMEOUT) -> None:
    """Retargets delivery at a new sink, backing a late ``configure(sink=...)``.

    Like :func:`_flush_worker` this deliberately does not call :func:`_get_worker`: a process
    that has not logged has captured no sink, so there is nothing to swap and building a thread
    to prove it would be pure cost (SPEC-030 FR-003).

    **Both delivery paths are handled here** (SPEC-033 FR-002). A worker delegates to
    :meth:`Worker.swap_sink`, which owns the drains. With no worker there is nothing buffered to
    drain — an orphan emit is synchronous and has returned before ``configure()`` was entered —
    so the handoff is the close alone. Returning early there, as this did, left the previous sink
    open forever with ``incomplete_swaps`` at zero, since every field of ``Health`` describes a
    worker that does not exist.

    The record is **re-pointed** at the new sink rather than cleared. Clearing would leave nothing
    armed until the next orphan emit, so a process that swaps and then exits without logging again
    would leak the *new* sink — measured, that case closes correctly today, so clearing would trade
    one leak for another. Re-pointing is also what the worker path does: :meth:`Worker.shutdown`
    closes ``self.sink`` whether or not anything was emitted to it since the swap.

    No fence, either. The only writer that could still be inside the old sink's ``emit`` is an
    orphan emitter on another application thread, and that is exactly the writer
    :meth:`Worker._close_swapped_out` documents itself as not covering — which is why
    ``sinks/base.py`` requires ``close()`` to tolerate a concurrent ``emit`` (SPEC-028 FR-001).
    This inherits that contract rather than weakening it.

    **Who performs the swap, who owns the old sink's close, and who owns the new one when the
    swap is declined are three questions** — the third added by SPEC-035 FR-003 and answered by
    :func:`_adopt_declined_swap`, off ``Worker.swap_sink``'s return value rather than a predicate
    here, because only the worker knows whether it got as far as reassigning. The first is
    liveness — a retired worker performs nothing, since :meth:`Worker.swap_sink` returns early
    once shut down, so routing the swap to it loses the handoff entirely. The second is
    ownership, exactly as in :func:`_close_orphan_sink`: a worker that *holds* ``old`` has either
    closed it already or deliberately left it open because its drain thread may still be inside
    that sink's ``emit`` (SPEC-027 FR-004). Answering the second with liveness closes it a second
    time on a clean shutdown, and closes it **under a live writer** on an expired one — both
    measured, both introduced by the first version of this split, and the second is the outcome
    ``sinks/base.py`` and SPEC-028 exist to prevent. So the record is re-pointed either way, and
    the close is performed only when no worker holds it.

    ``_worker`` is read **under** ``_worker_lock``. Unlocked it was harmless, because the
    no-worker branch did nothing; once that branch closes a sink it is the race
    :func:`_close_orphan_sink` was built against — a first ``@trace`` on another thread can be
    inside ``Worker(...)`` with its sink already resolved while this thread reads ``None`` and
    closes the sink that worker is about to deliver to. The closer's **join is outside** the
    lock: it waits up to the swap's whole budget, and holding the process-wide lock across that
    would park every concurrent emit behind it.

    ``incomplete_swaps`` is deliberately not touched on the orphan path. It records a *drain*
    that could not be confirmed (SPEC-030), and there is no drain here; an expired close join
    reports nothing at all, by the decision that made the bounded close available.

    Args:
      new_sink: The sink already written to the config, to be made the live delivery target.
      timeout: Seconds bounding the whole swap — the drains, where there are any, and the close
        of the previous sink share it as one deadline.

    Returns:
      None.

    Raises:
      None. This runs inside ``configure()``, which has never raised for anything but a
        rejected ceiling, and a sink swap that fails must not become the reason an application
        cannot start.
    """
    global _orphan_sink, _orphan_closed_sink
    closer = None
    with _worker_lock:
        worker = _live_worker()
        if worker is not None:
            _orphan_sink = None
        else:
            old = _orphan_sink
            if old is None or old is new_sink:
                return
            _orphan_sink = new_sink
            _offer_orphan_signal(new_sink)
            if _worker is None or _worker.sink is not old:
                _orphan_closed_sink = old
                closer = _lifecycle.close_detached(old)
    if worker is not None:
        try:
            worker_holds_sink = worker.swap_sink(new_sink, timeout)
        except Exception as exc:
            _diag.absorbed(
                "swapping the log sink", exc, "events may still be delivered to the previous sink"
            )
            return
        if not worker_holds_sink:
            _adopt_declined_swap(new_sink)
        return
    if closer is not None:
        closer.join(timeout)


def _adopt_declined_swap(new_sink: Sink) -> None:
    """Takes ownership of a sink a worker refused mid-swap (SPEC-035 FR-003).

    ``Worker.swap_sink`` re-checks retirement after its first ``flush()`` and returns early once
    ``_shutdown_done`` latched, but :func:`_swap_sink` had already cleared ``_orphan_sink`` on the
    strength of a worker being live a few instructions earlier. The new sink then sat in the
    config, installed nowhere and recorded nowhere: measured, ``config.sink is B`` was ``True``,
    ``B`` was never closed, and ``health()`` read entirely clean — for a sink whose ``close()``
    *is* its delivery, a ``KafkaSink`` flushing its producer, a silently lost buffer.

    Only the new sink is re-homed. ``Worker.swap_sink`` declines before reassigning anything, so
    whatever the worker held it still holds, and the worker closes it through its own
    ``_close_if_owed``; closing it here would be the double-close :func:`_close_orphan_sink`
    exists to avoid. The one case with no "old" sink at all is a retired worker that already
    holds ``new_sink`` — the entry check tests ``_shutdown_done`` before identity, so it declines
    a swap that was already satisfied — and there the same ownership guard makes the worker
    perform the single close.

    **A sink already recorded as closed is refused re-arming**, the guard
    :func:`_note_orphan_emit` carries and for its reason. Without it, an orphan log arming
    ``_orphan_sink`` while this thread is inside ``swap_sink``'s first drain, followed by a
    ``shutdown()`` that closes it, lets this re-arm a closed sink for a second ``close()`` at
    exit — reproduced, and ``Sink.close`` promises no idempotency.

    ``incomplete_swaps`` is deliberately not moved. It counts an unconfirmed *drain*, and this
    swap had no drain to confirm — the worker declined before reassigning anything — so counting
    it would stop telling an operator whether events were misrouted or a close was merely slow.

    Args:
      new_sink: The sink the worker refused to adopt.

    Returns:
      None.

    Raises:
      None.
    """
    global _orphan_sink
    with _worker_lock:
        _offer_orphan_signal(new_sink)
        if new_sink is _orphan_sink or new_sink is _orphan_closed_sink:
            return
        _register_exit_handler()
        _orphan_sink = new_sink


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

    The synthesis also survives a worker built *after* that shutdown, which is why it is an
    ``or`` rather than a fallback. An orphan-only ``shutdown()`` leaves ``_worker`` unset, so a
    later ``@trace`` constructs a fresh worker whose own ``retired`` is ``False`` — and reading
    that alone would say the process was never shut down, contradicting this function's own
    guarantee one call earlier. The events that worker carries are not lost silently: against a
    sink that guards its post-close state they raise and land in ``failed_batches`` (measured),
    and against one that releases nothing on ``close()`` they genuinely still deliver. So the
    detection is ``failed_batches`` there rather than SPEC-030's ``retired`` +
    ``submitted_after_shutdown`` pair, which stays the signal for the path it was built for.

    Args:
      None.

    Returns:
      The worker's health snapshot, backing :func:`log_foundry.health` (SPEC-017 FR-005).

    Raises:
      None.
    """
    worker = _worker
    if worker is None:
        return Health(
            queued=0,
            dropped=0,
            failed_batches=0,
            retired=_orphan_retired,
            closing_sinks=_lifecycle.closing_count(),
        )
    health = worker.health()
    if _orphan_retired and not health.retired:
        return replace(health, retired=True)
    return health


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
    backfill_baggage(span, context._live_baggage())
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
