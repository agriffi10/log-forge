"""The ``@trace`` decorator — synchronous and async (arch §4–5, guide Phases 6, 8)."""

from __future__ import annotations

import asyncio
import functools
import inspect
import threading
from collections.abc import Callable
from time import monotonic
from typing import TYPE_CHECKING, Any, TypeVar, cast, overload

from log_foundry import _diag, _ledger, _lifecycle, context
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
    from collections.abc import Mapping

    from log_foundry.worker import Worker


__all__ = ["continue_trace", "trace"]

_sweep_lock = threading.RLock()
"""Serializes every non-append write to a span's buffer, so no event is handed over twice.

The detach is a load and a store, and ``contextvars`` copies the same ``Span`` object into every
task and into any ``copy_context()`` thread, so the two-reader case is ordinary rather than exotic
(SPEC-036 FR-001 AC-10). A flush is not a hot path; a sink lock it is not competing with.

**Taken only through** :data:`_sweep_hold` (SPEC-091 FR-001), and an ``RLock`` rather than a
``Lock``. A collector callback or a finalizer runs on whichever thread allocates, and a hold
allocates, so one can run inside a hold and close a traced span there, or call ``flush()``: with a
plain ``Lock`` that close took the lock again on the thread already holding it, and the sweeping
thread never returned. An ``RLock`` alone is not enough either, because the nested close would swap
the list a sweep is part-way through taking; so every site first asks :func:`_sweep_held_here` and,
on the holding thread, takes nothing or defers to the holder. The owner an ``RLock`` records is
set and cleared inside its C ``acquire`` and ``release``, so there is no moment between taking the
lock and recording the holder in which a callback could run.
"""
_deferred_closes: list[tuple[Span, bool]] = []
"""Closes and abandons a nested call left to the hold's holder (SPEC-091 FR-001).

Each entry is a span and whether a guard abandoned it rather than closed it. Only the thread
holding :data:`_sweep_hold` appends, and only that thread drains, in :func:`_drain_deferred_closes`
before the hold is released, so the list needs no lock of its own. It holds spans, never a worker,
so no new module state holds a sink. A forked child clears it (:func:`_forget_open_spans_in_child`).
"""
_open_spans: dict[int, Span] = {}
"""Every open span, keyed by ``id(span)``, from its push to the start of its close (SPEC-091 FR-001).

Written without a lock: :func:`_begin` registers a span with one ``__setitem__`` after its push,
and :func:`_close_span` removes it with one ``pop`` as its first statement, before ``span.end`` is
built. Each is one C call under the GIL. This is module state relied on across threads, which
``best-practices/python`` §13 asks code to avoid; it is accepted deliberately, because a lock here
would sit on every traced call's open and close, and the take rule in :func:`_sweep_registered`
is what keeps the readers exact. A free-threaded build is not argued for (``architecture.md`` §13).
"""

type _Totals = tuple[int, int]
"""The ``dropped`` and ``submitted_after_shutdown`` totals one enqueue moved, each 0 if unmoved."""
type _HandIn = Callable[[list[dict[str, object]]], _Totals]
"""Enqueues one span's take and returns the totals it moved, writing nothing to stderr."""
type _SweepResult = tuple[int, list[_Totals], list[Exception]]
"""The events a sweep handed in, each hand-in's totals, and each hand-in's fault."""


class _NotHeld(Exception):
    """Raised by a bounded hold's entry when the sweep lock was not free within its wait."""


class _BoundedHold:
    """One entry into the sweep hold, whose acquire waits at most ``wait`` seconds.

    ``with _sweep_hold.within(wait):`` raises :class:`_NotHeld` from ``__enter__`` when the wait
    expires, so its body never runs without the lock; ``wait=None`` blocks, as ``with
    _sweep_hold:`` does. The exit is the hold's own (:func:`_release_hold`).

    Attributes:
      None.
    """

    __slots__ = ("_wait",)

    def __init__(self, wait: float | None) -> None:
        """Records the wait.

        Args:
          wait: The longest wait for the lock in seconds, or ``None`` to block.

        Returns:
          None.

        Raises:
          None.
        """
        self._wait = wait

    def __enter__(self) -> None:
        """Takes the sweep lock, or raises when it was not free within the wait.

        Args:
          None.

        Returns:
          None.

        Raises:
          _NotHeld: When the wait expired.
        """
        if self._wait is None:
            _sweep_lock.acquire()
        elif not _sweep_lock.acquire(timeout=self._wait):
            raise _NotHeld

    def __exit__(self, *exc_info: object) -> None:
        """Drains the deferred closes, releases the lock, then announces what they handed in.

        Args:
          *exc_info: The exception the body raised, if any, which propagates.

        Returns:
          None.

        Raises:
          None of its own.
        """
        _release_hold()


class _SweepHold:
    """The one way to take :data:`_sweep_lock` (SPEC-091 FR-001).

    ``with _sweep_hold:`` blocks for the lock, and ``with _sweep_hold.within(wait):`` waits at
    most ``wait``. Either exit drains :data:`_deferred_closes` while the lock is still held, so a
    close a collector callback deferred ships after the holder's own in-place work, then releases
    the lock, then writes the deferred closes' lines with no lock held. No site enters a hold its
    own thread already holds: each asks :func:`_sweep_held_here` first, and
    ``tests/test_open_span_sweep.py`` derives the sites and holds them to that.

    Attributes:
      None.
    """

    __slots__ = ()

    def __enter__(self) -> None:
        """Takes the sweep lock, blocking.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        _sweep_lock.acquire()

    def __exit__(self, *exc_info: object) -> None:
        """Drains the deferred closes, releases the lock, then announces what they handed in.

        Args:
          *exc_info: The exception the body raised, if any, which propagates.

        Returns:
          None.

        Raises:
          None of its own.
        """
        _release_hold()

    def within(self, wait: float | None) -> _BoundedHold:
        """Returns an entry into the hold whose acquire waits at most ``wait`` seconds.

        Args:
          wait: The longest wait, or ``None`` to block.

        Returns:
          The bounded entry.

        Raises:
          None.
        """
        return _BoundedHold(wait)


_sweep_hold = _SweepHold()
"""The sweep hold every non-append write to a span's buffer is made inside (SPEC-091 FR-001)."""


def _sweep_held_here() -> bool:
    """Reports whether the calling thread already holds the sweep hold (SPEC-091 FR-001).

    It reads the owner the ``RLock`` itself records, through ``_is_owned``, the method
    ``threading.Condition`` uses on a lock that has it. It exists on CPython's C ``RLock`` on every
    supported version but is not in the type stubs, hence the directive.

    Args:
      None.

    Returns:
      Whether this thread is inside a hold, as a collector callback or finalizer run inside one is.

    Raises:
      None.
    """
    return bool(_sweep_lock._is_owned())  # type: ignore[attr-defined]


def _release_hold() -> None:
    """Ends a hold: drains the deferred closes, releases the lock, announces their totals.

    The drain runs before the release, so a pass's in-place work on a span's list comes before the
    deferred swap of that list, and ``span.end`` ships last. A fault in the drain still releases
    the lock, and whatever was not drained stays deferred for the next hold on this thread.

    Args:
      None.

    Returns:
      None.

    Raises:
      None of its own; a ``BaseException`` from the drain propagates after the release.
    """
    try:
        moved = _drain_deferred_closes()
    finally:
        _sweep_lock.release()
    for worker, totals in moved:
        worker._announce(totals)


def _drain_deferred_closes() -> list[tuple[Worker, _Totals]]:
    """Ships every close a nested call deferred to this hold, while the lock is still held.

    Only :func:`_release_hold` calls it. Each deferred close swaps its span's buffer as
    :func:`_flush` would, hands a non-empty one to the process worker with the retirement read
    kept, and credits the hand-over in this thread's cell once the enqueue returns; a deferred
    abandon swaps the buffer and debits it to ``in_span_lost``, as :func:`_abandon` would. The
    worker always exists for a deferred close, which resolved it before deferring; one that does
    not, which no path reaches, is counted lost rather than dropped silently. It loops until the
    list is empty, because its own allocations can run a callback that defers another.

    Args:
      None.

    Returns:
      Each worker handed a buffer and the totals its enqueue moved, announced after the release.

    Raises:
      None.
    """
    moved: list[tuple[Worker, _Totals]] = []
    while _deferred_closes:
        span, abandoned = _deferred_closes.pop(0)
        events, span.events = span.events, []
        if abandoned:
            _note_in_span_loss(len(events))
            continue
        if not events:
            continue
        worker = _lifecycle._state.worker_exists()
        if worker is None:
            _note_in_span_loss(len(events))
            continue
        moved.append((worker, worker._enqueue(events, count_retired=True)))
        _ledger.cell().handed += len(events)
    return moved


def _forget_open_spans_in_child() -> None:
    """Forgets every registered span and deferred close, in a forked child (SPEC-091 FR-003).

    The child's sweep must never ship what the parent had pending, which is *A value the child
    inherits is stranded, never merely detached*. The spans stay on the child's copy of their
    contexts, so the forking thread's own spans still ship at their close as they did before
    SPEC-091; only the sweep forgets them. Called from ``_lifecycle``'s child handler, ahead of the
    worker's rebuild.

    Args:
      None.

    Returns:
      None.

    Raises:
      None.
    """
    _open_spans.clear()
    _deferred_closes.clear()


def _sweep_registered(hand_in: _HandIn, *, wait: float | None = None) -> _SweepResult | None:
    """Hands in every open span's buffered events but its held ``span.start`` (SPEC-091 FR-001).

    The worker's sweep, run by the drain thread about once a flush interval with the worker's
    :meth:`~log_foundry.worker.Worker._hand_in`, and a ``flush()``'s registry sweep, run with an
    enqueue that keeps the retirement read. It takes one pass over a copy of the registry inside
    the hold, and for each span:

    - reads ``n = len(span.events)`` **first**, then skips the span if it is no longer the
      registered value for its id or is ``closed``. The close leaves the registry before it
      appends ``span.end``, so a length that counts ``span.end`` is always followed by a failed
      membership read, and a take never ships ``span.end`` early;
    - takes ``span.events[held:n]`` and deletes that slice **in place**. The list is never rebound,
      so an append that read the attribute before the take lands on the same list after ``n`` and
      is taken by the next sweep or the close; a rebinding take lost one event in 36,015 to that;
    - sets ``swept``, hands the take in as **its own submission**, and credits the hand-over in
      this thread's cell once ``hand_in`` returns, so every event the credit counts is already in
      the queue's taken-in count or in ``overflow_lost`` (SPEC-088). One submission per span keeps
      SPEC-090's cap meaningful.

    The slice and the ``del`` are outside the ``try``, so a fault there leaves the events where they
    were. A fault after the ``del``, in ``hand_in`` or in setting ``swept``, means the taken events
    are on neither the span nor the queue: they are debited to ``in_span_lost`` at once and the
    fault is kept for the caller's line. ``hand_in`` raises before its put or not at all
    (``Worker._enqueue``), so a debit is never also queued, except for an asynchronous
    ``BaseException`` landing after a put, which is debited and re-raised, counted twice, and
    recorded in ``architecture.md`` §13. Nothing is written to stderr here; the caller announces
    every total and fault once the hold is released.

    The cell is bound **before** the hold is taken. A thread's first credit registers its cell
    under the ledger's registration lock, and a collector callback inside that lock's section may
    call ``flush()``, which takes this hold; binding first keeps the two from nesting here.

    Args:
      hand_in: Enqueues one span's take and returns the totals it moved.
      wait: The longest wait for the hold, or ``None`` to block. The drain passes
        ``worker._SWEEP_WAIT``; every other caller blocks.

    Returns:
      The events handed in, each hand-in's totals and each hand-in's fault; or ``None``, taking
      nothing, when this thread already holds the hold, which a collector callback inside one
      does, or when the wait expired.

    Raises:
      BaseException: Whatever ``hand_in`` raises that is not an ``Exception``, after its take is
        debited.
    """
    if _sweep_held_here():
        return None
    cell = _ledger.cell()
    handed = 0
    totals: list[_Totals] = []
    faults: list[Exception] = []
    try:
        with _sweep_hold.within(wait):
            for span in _open_spans.copy().values():
                events = span.events
                n = len(events)
                if _open_spans.get(id(span)) is not span or span.closed:
                    continue
                first = span.held
                if n <= first:
                    continue
                take = events[first:n]
                del events[first:n]
                try:
                    span.swept = True
                    totals.append(hand_in(take))
                except Exception as exc:
                    _note_in_span_loss(len(take))
                    faults.append(exc)
                    continue
                except BaseException:
                    _note_in_span_loss(len(take))
                    raise
                cell.handed += len(take)
                handed += len(take)
    except _NotHeld:
        return None
    return handed, totals, faults


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
    span.held = 1
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

    Each argument is normalised once, on entry, by :func:`_exact` (SPEC-067 FR-004): a ``str``
    subclass becomes the exact ``str`` it holds, through ``str.__str__``, and anything else is
    passed on as it is, to be refused and announced by type name. No code of an inbound value
    runs anywhere in this call, so a valid header in a ``str`` subclass is still adopted, and
    what is stored, put on events and formatted later by :func:`current_traceparent` is the exact
    ``str`` — a subclass kept as it was would raise its own exception from that formatting. The
    four parameters are reassigned through ``cast`` rather than rebound to new names, so no later
    line can read an argument the normalisation skipped; the ``cast`` states the same thing the
    annotation already does, which a caller not running mypy is free to ignore.

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

      ``"rejected"`` follows the announcement, not its delivery. The stderr write is best-effort,
      so a stream that faults still yields ``"rejected"``: the refusal was announced, and the
      line was lost to the stream rather than to the library.

    Raises:
      None. A ``BaseException`` raised by the stderr write, such as a ``KeyboardInterrupt``, still
        propagates (SPEC-029).
    """
    traceparent = cast("str | None", _exact(traceparent))
    trace_id = cast("str | None", _exact(trace_id))
    parent_span_id = cast("str | None", _exact(parent_span_id))
    baggage = cast("str | None", _exact(baggage))
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

    if adopted is not None:
        refusal = _adopt_unswept(*adopted)
        if refusal is not None:
            _diag.rejected(refusal, traceparent if traceparent is not None else str(trace_id))
            adopted = None
            announced = True

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


def _exact(value: object) -> object:
    """Returns an inbound ``str`` subclass as the exact ``str`` it holds, anything else as is.

    The test is ``issubclass(type(value), str)``, a check over the real type's MRO that consults
    ``str``'s metaclass and never the value's; ``isinstance`` would read the value's own
    ``__class__``, a property free to raise. The copy is ``str.__str__(value)``, which is
    C-level: on a subclass it returns the stored characters without calling the subclass's
    ``__str__``, ``__len__`` or anything else. A value of any other type is not touched at all,
    so the parsers refuse it and ``_diag.rejected`` names it by type.

    The copy is linear in the subclass's length, so a subclass too large to duplicate raises
    ``MemoryError`` from it. That ``Exception`` returns the value as it is, and the parsers then
    refuse it as not an exact ``str`` and ``_diag.rejected`` echoes it through a bounded slice,
    which copies nothing, so the call stays total rather than failing the caller.

    Args:
      value: One of :func:`continue_trace`'s inbound arguments, of any type.

    Returns:
      An exact ``str`` for a ``str`` or ``str`` subclass; otherwise, or when the copy fails,
      ``value`` itself.

    Raises:
      None.
    """
    if issubclass(type(value), str):
        try:
            return str.__str__(cast("str", value))
        except Exception:
            return value
    return value


def _stored_name(cls: type) -> str:
    """Returns the name a type stores, as an exact ``str``, running no code of the type (SPEC-080).

    The name is read through ``type``'s own ``__name__`` descriptor (``_diag._STORED_NAME``),
    not ``cls.__name__``, so a metaclass ``__name__`` property or ``__getattribute__`` is never
    consulted and cannot divert, raise or lie. ``type.__name__``'s setter accepts a ``str``
    subclass and the descriptor hands it back as stored, so the result goes through
    ``_diag._STR_STR``, ``str.__str__`` unbound, which copies the held text without running a
    method of the subclass.

    This is the decoration-time sibling of ``sanitize._type_name``, which is not reused: that
    one swallows a failure to ``"?"`` because assembly must never raise, where at decoration
    raising is allowed (invariant 13) and ``"?"`` would be a wrong span name.

    Args:
      cls: The type to name, normally ``type(value)`` for a value ``@trace`` was handed.

    Returns:
      The type's stored name, as an exact ``str``.

    Raises:
      None.
    """
    return _diag._STR_STR(_diag._STORED_NAME(cls))


def _adopt_unswept(trace_id: str, parent_span_id: str | None) -> str | None:
    """Adopts a context and re-parents the current root span unless a sweep took from it.

    The check and the re-parent happen inside one hold (SPEC-091 FR-001), so no sweep, the
    worker's or another thread's ``flush()``, can take from the root span between them: with a
    50 ms pause forced inside the re-parent and a 1 ms interval, 19 of 20 root spans reached the
    sink with two trace ids without the hold and none with it. That closes ``architecture.md``
    §13's "``Span.swept`` is not a synchronization primitive". The refusal's line is written by the
    caller, after the hold is released.

    On a thread that already holds the hold, which a collector callback inside one does, the
    adoption is refused outright: the sweep part-way through on this thread may be taking from this
    very span, and the hold cannot be taken again to wait for it.

    Args:
      trace_id: The adopted trace id.
      parent_span_id: The adopted parent span id, or ``None``.

    Returns:
      ``None`` when the context was adopted; otherwise the reason for the rejection line.

    Raises:
      None.
    """
    if _sweep_held_here():
        return "a sweep is in progress on this thread; trace context refused"
    with _sweep_hold:
        if _current_span_was_swept():
            return (
                "the current span's events have already been handed to the worker; trace context "
                "refused"
            )
        context.set_adopted_context(trace_id, parent_span_id)
        _reparent_current_span(trace_id, parent_span_id)
    return None


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





























def _sweep_open_spans() -> bool:
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

    ~~It reaches only the calling context's spans. ``contextvars`` offers no way to enumerate
    another thread's or task's context, so a ``flush()`` in a handler that fanned out does not
    reach what those tasks buffered.~~ — superseded by SPEC-091 FR-003: this still walks only the
    calling context, which ships its spans whole with ``span.start`` backfilled, and
    :func:`_sweep_registered` then reaches every other registered span from
    ``_lifecycle._flush_worker``. A span this ships has ``held`` set to 0, so a later worker sweep
    takes from index 0.

    **Nothing is written to stderr inside the hold** (SPEC-091 FR-001). Each buffer goes through
    ``Worker._enqueue`` under the hold, and the totals are announced once it is released: a line
    written under the hold stalled every span close in the process behind a wedged console. On a
    thread that already holds the hold, which a collector callback inside one does, it takes
    nothing and returns ``False``, so the ``flush()`` reports ``"abandoned"``.

    **The worker is resolved outside** ``_sweep_lock``, as :func:`_flush` resolves it, and still
    before any detach. Building it hands the sink its stop signal, which runs the sink's setter,
    and a setter that calls ``flush()`` or finishes a ``@trace`` span takes this lock: under it,
    that setter waited on its own thread. So a pass that meets a buffer with no worker resolved
    stops before detaching anything, releases the lock, resolves the worker and sweeps again; a
    flush that finds nothing buffered still builds nothing.

    **Each take is a hand-over in the event ledger** (SPEC-088 FR-001), credited in the calling
    thread's cell after ``submit`` returns, so every event the credit counts is already in the
    queue's taken-in count or in ``overflow_lost``. The length is read under the lock, at the
    swap, so a racing append cannot change what is counted. The cell is bound before the hold is
    taken, for the reason :func:`_sweep_registered` gives.

    Args:
      None.

    Returns:
      ``False`` when it took nothing because this thread already held the hold, else ``True``.

    Raises:
      Exception: Whatever building the worker raises. :func:`_flush_worker` guards it, because a
        flush is the call most likely to be made in a ``finally``.
    """
    if _sweep_held_here():
        return False
    cell = _ledger.cell()
    worker: Worker | None = None
    totals: list[_Totals] = []
    while True:
        with _sweep_hold:
            needs_worker = False
            for span in context._live_span_stack():
                if not span.events:
                    span.swept = True
                    continue
                if worker is None:
                    needs_worker = True
                    break
                backfill_baggage(span, context._live_baggage())
                span.swept = True
                span.held = 0
                buffered, span.events = span.events, []
                count = len(buffered)
                totals.append(worker._enqueue(buffered, count_retired=True))
                cell.handed += count
        if not needs_worker:
            break
        worker = _lifecycle._get_worker()
    if worker is not None:
        for moved in totals:
            worker._announce(moved)
    return True






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


def _note_in_span_loss(count: int = 1) -> None:
    """Counts events lost while being built or handed over inside a span (SPEC-036 FR-003).

    Separate from :func:`_note_orphan_loss` because the two aggregate different failure
    populations: this path cannot fail at a sink's ``emit``, which is ``failed_batches``. It has
    **two** causes, not one (SPEC-050 FR-003): a value that could not be built into an event, and
    a process that could not give the library a thread to deliver through at all — where
    ``Worker.__init__`` cannot start the drain thread, so the span's whole buffer is lost with no
    worker in existence to record anything. The count is what the span was holding, so the second
    cause moves it by more than one.

    Args:
      count: How many events were lost, defaulting to the single event the build path loses.

    Returns:
      None.

    Raises:
      None.
    """
    global _in_span_lost
    with _loss_lock:
        _in_span_lost += count


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

    **The worker is resolved before the buffer is detached** (SPEC-050 FR-003), which is what
    lets :func:`_end` count what was lost. Resolving second, the events were already in a local
    when :func:`~log_foundry._lifecycle._get_worker` raised, so they died with the exception and
    ``len(span.events)`` read zero on exactly the path the count is for. Nothing between the
    detach and the hand-off can fail: :meth:`~log_foundry.worker.Worker.submit` is total on every
    path, so the events cannot be stranded in the local by this order. It also **narrows** the
    SPEC-036 FR-004 window rather than widening it — the gap between detaching and submitting
    falls from the whole of ``_get_worker()`` to a lock release.

    **The hand-over is credited after** ``submit`` **returns** (SPEC-088 FR-001), in the calling
    thread's cell, by the length read at the detach. Credited before, a submission ``submit`` lost
    without a count would read as balanced; after, it leaves the meeting of the caller's
    hand-overs with the queue's taken-in count positive, which the shutdown check reports.

    **An empty detach submits nothing** (SPEC-091 FR-004). A worker sweep, or a ``flush()``, can
    leave a closing span's list empty, and an empty submission after a ``shutdown()`` was counted
    as queued, counted as submitted after shutdown and announced as a lost submission, the
    ``empty-submission`` pin of SPEC-085 that ``architecture.md`` §12 now lists as resolved.

    **A close on a thread already inside the hold is deferred to its holder** (SPEC-091 FR-001).
    A collector callback or finalizer inside a hold can finish a traced call, or finalize a
    traced coroutine, on the holding thread; that close has already left the registry, appended
    ``span.end`` and backfilled, and its swap and submit are appended to ``_deferred_closes``,
    which the holder drains before it releases the hold. So the holder's in-place take acts on
    the list first and ``span.end`` still ships last, where taking the hold again here waited on
    this thread for ever.

    Args:
      span: The finished span whose buffered events are submitted.

    Returns:
      None.

    Raises:
      Exception: Whatever creating the worker or submitting raises; :func:`_end` is the guard.
    """
    worker = _lifecycle._get_worker()
    if _sweep_held_here():
        _deferred_closes.append((span, False))
        return
    with _sweep_hold:
        events, span.events = span.events, []
        count = len(events)
    if events:
        worker.submit(events)
        _ledger.cell().handed += count


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

    **The call's** ``span.start`` **is credited first, before the** ``try`` (SPEC-088 FR-001):
    accepted means asked for, so a guard that forgets its debit leaves a remainder a check can
    see, where a credit after the build would hide exactly the events a failed open lost.

    **The process worker is built here, ahead of that credit** (SPEC-091 FR-002), when none
    exists yet, so the drain thread can sweep the process's first span while it is still open:
    inside the first traced call there was no worker, and nothing could. It is read as one
    attribute load, so once a worker exists the open calls no ``_lifecycle`` function. The build
    runs before the span is pushed, so the sink's stop-signal setter, which the build runs, logs
    on the orphan route: built after the push, a setter's ``info()`` became an in-span event and
    two SPEC-085 cells went red as ``LedgerBroken:orphans-1``. A build that fails is announced by
    type and the call runs traced; its close retries the build, as before. The guard catches only
    ``Exception``, so a ``BaseException`` from the setter leaves before anything is credited.

    **The span is registered after its push** (SPEC-091 FR-001), in ``_open_spans``, with one dict
    write and no lock; a span whose push failed is never registered and is still closed as before.

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
    if _lifecycle._state._worker is None:
        try:
            _lifecycle._get_worker()
        except Exception as exc:
            _diag.absorbed("starting the worker for a span", exc, "its events wait for its close")
    _ledger.cell().span += 1
    try:
        if context.current_span() is None:
            scope = context.push_baggage_scope()
        span = _open_span(name, defaults)
        token = context.push_span(span)
        _open_spans[id(span)] = span
    except Exception as exc:
        if span is None:
            _ledger.cell().span += 1
            _note_in_span_loss(2)
        _diag.absorbed(
            "opening a span",
            exc,
            "this call runs untraced" if span is None else "this call is traced incompletely",
        )
    except BaseException:
        _note_in_span_loss(1 if span is None else _abandon(span))
        raise
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

    **The absorbed close is counted, not merely announced** (SPEC-050 FR-003). It used to write
    one stderr line and move no field, so a process that could not start a drain thread lost every
    span's events under ``Health(stopped_reason=None, in_span_lost=0)`` — all zeros over total
    loss, which is the reading SPEC-036 FR-003 added the counter to remove. The count is
    ``len(span.events)``, which :func:`_flush` leaves intact until there is a worker to take them,
    and it is recorded **before** the announcement for the reason every other site here is:
    stderr may be wedged, and the counter must not be lost to it.

    **A span's** ``span.end`` **is credited before the close is tried** (SPEC-088 FR-001), for the
    reason :func:`_begin` credits ``span.start`` before its ``try``.

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
        _ledger.cell().span += 1
        try:
            _close_span(span, status, error)
        except Exception as exc:
            _note_in_span_loss(_abandon(span) + (0 if span.ended else 1))
            _diag.absorbed("closing a span", exc, "the span's events were lost")
        except BaseException:
            _note_in_span_loss(_abandon(span) + (0 if span.ended else 1))
            raise
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

    **The span leaves the registry first** (SPEC-091 FR-001), before ``span.end`` is built or
    appended. A sweep reads a span's length before its membership, so a length that counts
    ``span.end`` is always followed by a membership read that fails, and no sweep ships
    ``span.end`` ahead of the span's logs: without that read, 25 and 29 spans a run ended early.

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
    _open_spans.pop(id(span), None)
    event = end_event(span, status, exc)
    span.events.append(event)
    span.ended = True
    backfill_baggage(span, context._live_baggage())
    span.closed = True
    _flush(span)


def _abandon(span: Span) -> int:
    """Takes a span's buffer and closes it, for a guard that gave up on it (SPEC-088 FR-002).

    The guard counts what it took as lost, so the take is what keeps those events from a second
    term: left on the span, a context still holding it could ``flush()`` them after they were
    counted, and in the review's unforced race 3 events were both counted and delivered. The take
    is the detach :func:`_flush` and the sweep make, under the same lock. The span is marked
    closed, so a later append takes the orphan route, and abandoned, so that orphan keeps this
    span's ids (:func:`log_foundry.api._log`). Popping the span from its context is out of scope
    (``architecture.md`` §12), so it stays current there.

    The span also leaves the registry (SPEC-091), so no sweep copies it again. On a thread that
    already holds the sweep hold, which a collector callback inside one does, the take is deferred
    to the holder, which debits it to ``in_span_lost`` before it releases the hold; this then
    returns 0, so the guard counts only the ``span.end`` it adds itself.

    Args:
      span: The span the guard gave up on.

    Returns:
      How many events it took, 0 when the take was deferred.

    Raises:
      None.
    """
    _open_spans.pop(id(span), None)
    if _sweep_held_here():
        span.closed = True
        span.abandoned = True
        _deferred_closes.append((span, True))
        return 0
    with _sweep_hold:
        taken, span.events = span.events, []
    span.closed = True
    span.abandoned = True
    return len(taken)


def _refuse_unusable(fn: object) -> None:
    """Refuses, at decoration, a callable ``@trace`` could not trace (SPEC-055 FR-002).

    Invariant 13: a bad argument is refused where it is written, never on the first call. A
    ``classmethod`` or ``staticmethod`` object is a decorator applied in the wrong order —
    ``classmethod`` is not callable, so the wrapper would fail every call, and ``staticmethod``
    *is* callable but the wrapper replaces the descriptor, so an instance call would hand
    ``self`` to a function declared without one. A ``str`` is the slip ``@trace("checkout")``,
    which meant ``name=``. Anything else that is not callable is refused by type.

    A generator function or an async generator function is refused too (SPEC-055 FR-003): its
    body runs after the wrapper has returned the generator object, so the span would open and
    close before a single line of it ran, and every event the body logged would be an orphan on
    a fresh trace — measured, ``['span.start', 'span.end']`` before the first ``next()``. The
    check is the code flags, on ``fn``, on whatever ``fn`` advertises through ``__wrapped__``
    — so ``@trace`` above ``@contextmanager``, ``@lru_cache`` or any ``functools.wraps`` wrapper
    of a generator is refused too, each measured recording the same orphaned ``inside`` event —
    and on the type's ``__call__`` for a callable instance; all of them see through a
    ``functools.partial``. A plain function that merely *returns* a generator object without
    advertising it is indistinguishable here and is not detected, which is a stated limit
    rather than a gap to close at call time, where invariant 13 does not refuse. Wrapping the
    iteration instead — a span per generator, pushed around every resumption — was considered
    and deferred: a refusal can be lifted into a wrap later without breaking anyone, while a
    wrap shipped first freezes its semantics at 1.0.

    The checks run in a fixed order, so that only a callable runs its own code here (SPEC-080
    FR-003, FR-004). Each of the first three routes on ``fn``'s **real** type, which asks
    nothing of ``fn`` itself: ``isinstance`` would read ``fn``'s own ``__class__``, a property
    free to raise or to lie.

    1. ``issubclass(type(fn), str)`` comes first, so no other check reads a ``str`` subclass
       before it is refused. The hint is rendered by ``_diag._echo``, which takes the first 64
       characters through ``str``'s own slots, quotes, escapes and cuts them with ``…``, running
       no method of a subclass; for ``"checkout"`` the message is the one it always was. A
       callable that merely *claims* ``str`` through ``__class__`` no longer matches and is
       traced like any callable instance.
    2. ``issubclass(type(fn), (classmethod, staticmethod))``: the misordered-descriptor message,
       naming the type's stored name (:func:`_stored_name`), so a real subclass is told its own
       decorator.
    3. ``not callable(fn)``, which reads the type's ``tp_call`` slot and runs no code of the
       value; the type is named by its stored name.
    4. Only then ``isinstance(fn, (classmethod, staticmethod))``, the one check that **keeps**
       ``isinstance``, on purpose. A callable proxy that forwards ``__class__`` — a
       ``wrapt.FunctionWrapper`` around a ``classmethod``, say — is a descriptor whose binding a
       plain-function wrapper would lose, so refusing it is right, and it is told the kind it
       claims. By this step the value is a callable, so its ``__class__`` raising here is the
       caller's own code raising at decoration, never a per-call failure. The order has one
       accepted trade-off: a *non-callable* proxy forwarding ``__class__`` to a ``classmethod``
       is told it needs a callable rather than given the descriptor advice, still a refusal at
       decoration naming the argument.

    Both descriptor messages are built by :func:`_misordered`, which reads ``__func__`` and its
    ``__qualname__`` only through ``getattr`` and keeps the latter only if its real type is a
    ``str``.

    Args:
      fn: Whatever was handed to the decorator.

    Returns:
      None.

    Raises:
      TypeError: If ``fn`` is a ``str`` by real type, a ``classmethod`` or ``staticmethod`` by
        real type or by the ``__class__`` a callable claims, not callable at all, or a generator
        function or async generator function.
      Exception: Whatever a callable's own attribute access raises — a ``__class__`` property,
        say, or a ``classmethod`` or ``staticmethod``'s ``__func__`` — and whatever computing the
        ``str`` hint's echo raises, such as a ``MemoryError``, at decoration, in the caller's
        frame, which is invariant 13's moment.
    """
    if issubclass(type(fn), str):
        raise TypeError(
            f"@trace needs a callable, got str; pass a span name as @trace(name={_diag._echo(fn)})"
        )
    if issubclass(type(fn), (classmethod, staticmethod)):
        raise TypeError(_misordered(_stored_name(type(fn)), fn))
    if not callable(fn):
        raise TypeError(f"@trace needs a callable, got {_stored_name(type(fn))}")
    if isinstance(fn, (classmethod, staticmethod)):
        kind = "classmethod" if isinstance(fn, classmethod) else "staticmethod"
        raise TypeError(_misordered(kind, fn))
    if _is_generator_function(fn):
        raise TypeError(
            f"@trace cannot trace the iteration of the generator function {_span_name(fn)}: its "
            f"body runs after the wrapper has returned, so the span would close before it "
            f"starts. Trace the consumer instead: the function that iterates it"
        )


def _misordered(kind: str, fn: object) -> str:
    """Builds the misordered-descriptor message, reading nothing it has not checked (SPEC-080).

    ``kind`` is the decorator to move above ``@trace``: the real type's stored name for a
    ``classmethod`` or ``staticmethod`` by real type, or the kind a callable proxy claims.
    The wrapped function is named by ``fn.__func__.__qualname__``, each read through
    ``getattr`` with a default, so a proxy with no ``__func__`` is described rather than
    raising ``AttributeError``. The name is kept only if its real type is a ``str``, copied
    with ``str.__str__`` so no method of a subclass runs; anything else — ``5``, or an object
    whose ``__class__`` property claims ``str`` — becomes "the function" without being read.

    A builder rather than a raiser, so both ``raise TypeError(...)`` sites stay visible in
    :func:`_refuse_unusable`. The ``getattr`` defaults catch only ``AttributeError``: a callable
    proxy's raising ``__func__`` property, or a raising ``__func__``/``__qualname__`` behind a
    real ``classmethod`` or ``staticmethod``, is the caller's code and raises at decoration.

    Args:
      kind: The decorator name the message tells the caller to move.
      fn: The descriptor or descriptor proxy that was handed to ``@trace``.

    Returns:
      The ``TypeError`` message.

    Raises:
      Exception: Whatever reading ``__func__`` or its ``__qualname__`` raises other than
        ``AttributeError``, at decoration, in the caller's frame.
    """
    func_qualname = getattr(getattr(fn, "__func__", None), "__qualname__", None)
    inner = "the function"
    if issubclass(type(func_qualname), str):
        inner = str.__str__(cast("str", func_qualname))
    return (
        f"@trace cannot wrap the {kind} object around {inner}; put @{kind} above @trace, "
        f"so that @trace decorates the plain function"
    )


def _is_generator_function(fn: object) -> bool:
    """Reports whether a callable's body is a generator or async generator (SPEC-055 FR-003).

    The same shape as :func:`_is_async`: the code flags of ``fn`` itself, which see through a
    ``functools.partial``; then of whatever ``fn`` advertises through ``__wrapped__``, which is
    how ``@contextmanager``, ``@lru_cache`` and every ``functools.wraps`` wrapper point back at
    the generator they wrap; and for anything that is not a plain function the type's
    ``__call__`` as well, so a callable instance whose ``__call__`` yields is caught too. The
    unwrap is guarded because :func:`inspect.unwrap` raises ``ValueError`` on a ``__wrapped__``
    cycle, and a cycle is not a generator.

    Args:
      fn: The callable being decorated.

    Returns:
      Whether ``fn`` would return a generator or async generator object when called.

    Raises:
      Exception: Whatever the callable's own ``__class__``, ``__call__`` or ``__wrapped__``
        lookup raises, at decoration, in the caller's frame.
    """
    if _yields(fn):
        return True
    try:
        target = inspect.unwrap(cast("Callable[..., Any]", fn))
    except ValueError:
        target = fn
    if target is not fn and _yields(target):
        return True
    if inspect.isfunction(fn):
        return False
    return _yields(type(fn).__call__)


def _yields(fn: object) -> bool:
    """Reads a callable's code flags for a generator or async generator body.

    Args:
      fn: The callable to inspect.

    Returns:
      Whether its body is a generator or an async generator.

    Raises:
      None.
    """
    return inspect.isgeneratorfunction(fn) or inspect.isasyncgenfunction(fn)


def _span_name(fn: object) -> str:
    """Resolves the span name once, at decoration, from any callable (SPEC-055 FR-002).

    ``fn.__qualname__`` where the callable has one, else its type's name — so a
    ``functools.partial`` traces as ``partial`` and a callable instance as its class, where
    reading the attribute per call inside the wrapper raised ``AttributeError`` into the
    application on every call.

    Both routes are taken on real types and both return an exact ``str`` (SPEC-080 FR-002). The
    ``__qualname__`` is kept if ``issubclass(type(qualname), str)``, copied with
    ``str.__str__`` so a subclass's methods never run and the span name is never the subclass
    itself. Otherwise the type's stored name is used, read by :func:`_stored_name` through
    ``type``'s own ``__name__`` descriptor, which a metaclass ``__name__`` property cannot
    divert: through ``type(fn).__name__``, a property returning ``1`` decorated cleanly and then
    absorbed every call's span, and one that raised raised out of decoration. A ``__qualname__``
    that exists but is not a ``str`` needs no clause: ``functools.wraps`` copies it onto the
    wrapper and Python refuses a non-``str`` there before any wrapper exists; the route above
    no longer reads that value's ``__class__`` on the way.

    Args:
      fn: The callable being decorated.

    Returns:
      The span name, as an exact ``str``.

    Raises:
      Exception: Whatever the callable's own ``__qualname__`` lookup raises other than
        ``AttributeError``, at decoration, in the caller's frame.
    """
    qualname = getattr(fn, "__qualname__", None)
    if issubclass(type(qualname), str):
        return str.__str__(cast("str", qualname))
    return _stored_name(type(fn))


def _is_async(fn: object) -> bool:
    """Decides the async wrapper, consulting a callable instance's ``__call__`` (SPEC-055 FR-002).

    ``asyncio.iscoroutinefunction`` sees through a ``functools.partial`` but reads a callable
    instance whose ``__call__`` is ``async def`` as synchronous, so accepting instances at all
    (FR-002) would have handed one the sync wrapper and closed the span before the coroutine
    ran — the generator finding's shape again. The type's ``__call__`` is asked too, for
    anything that is not a plain function; a ``partial``'s or a class's is a slot wrapper and
    reads as synchronous, which is correct.

    Args:
      fn: The callable being decorated.

    Returns:
      Whether the async wrapper applies.

    Raises:
      Exception: Whatever the callable's own ``__class__`` or ``__call__`` lookup raises, at
        decoration, in the caller's frame.
    """
    if asyncio.iscoroutinefunction(fn):
        return True
    if inspect.isfunction(fn):
        return False
    return asyncio.iscoroutinefunction(type(fn).__call__)


@overload
def trace(func: F) -> F: ...
@overload
def trace(
    *, name: str | None = ..., defaults: Mapping[str, object] | None = ...
) -> Callable[[F], F]: ...


def trace(
    func: F | None = None,
    *,
    name: str | None = None,
    defaults: Mapping[str, object] | None = None,
) -> F | Callable[[F], F]:
    """Traces a function call as a span, usable bare as ``@trace`` or with arguments.

    Args:
      func: The function being decorated when used bare, otherwise ``None``.
      name: Overrides the span name, which defaults to ``func.__qualname__`` where the callable
        has one and to its type's name otherwise — ``partial`` for a ``functools.partial``, the
        class name for a callable instance (SPEC-055 FR-002). It is routed by its **real type
        at this call** (SPEC-080 FR-001), ``issubclass(type(name), str)``, which never asks the
        value: a ``str`` subclass is kept as the exact ``str`` it holds, copied with
        ``str.__str__`` so no method of the subclass runs, then or on any later call; anything
        else not ``None`` is raised here, before any decorator is applied, named by its type's
        stored name. The copy is not guarded, so a ``MemoryError`` from copying an enormous
        subclass propagates here too. An empty name still falls back to the default.
      defaults: Per-decorator default fields added to every event on the span. Any
        ``Mapping`` is accepted, and a copy is taken **once here**, at decoration — not per
        call. Before SPEC-051 FR-002 the caller's own object was bound to every span and read
        on the per-event path, so editing it after decoration changed later spans; that is
        tolerable for a ``dict`` and is not for a ``Mapping`` whose ``keys()`` is user code.

    Returns:
      The wrapped function, or a decorator when called with arguments.

    Raises:
      TypeError: At this call, for a ``name=`` that is not a ``str`` by real type (SPEC-055
        FR-002, SPEC-080 FR-001). At decoration, for a ``classmethod`` or ``staticmethod``
        object (the decorators are in the wrong order), a ``str`` (``name=`` was meant),
        anything not callable, or a generator function, whose iteration cannot be traced by a
        wrapper around the call (SPEC-055 FR-003) — invariant 13. Because ``name=`` is judged
        before the decorated object, a bad ``name=`` beside a bad object reports ``name=``:
        ``trace("checkout", name=1)`` and ``trace(fn, name=1)`` both raise ``name= must be a
        str, got int``. ``defaults=`` is copied before either, so a bad ``defaults=`` is
        reported first.
      Exception: At this call, whatever ``dict(defaults)`` raises from the mapping's own code,
        or a ``MemoryError`` from copying an enormous ``str`` subclass ``name=``; at
        decoration, whatever the callable's own attributes raise while they are read — all in
        the caller's frame, which is invariant 13's moment.
    """
    span_defaults = None if defaults is None else dict(defaults)
    exact_name: str | None
    if issubclass(type(name), str):
        exact_name = str.__str__(cast("str", name))
    elif name is None:
        exact_name = None
    else:
        raise TypeError(f"@trace name= must be a str, got {_stored_name(type(name))}")

    def decorate(fn: F) -> F:
        """Wraps one function, selecting the sync or async wrapper at decoration time.

        :func:`_is_async` makes the choice once. The two wrappers are
        deliberate near-duplicates — the only difference is ``await fn(...)`` — because the
        sync/async split is a hard boundary, and ``contextvars`` already propagates the span
        stack and baggage across ``await`` points and concurrent tasks (arch §5), so the async
        span opens when the coroutine actually runs and closes when it finishes. The span name
        is resolved here, once, rather than read from ``fn`` on every call (SPEC-055 FR-002).
        It reads the exact name :func:`trace` already resolved from ``name=`` (SPEC-080
        FR-001), so the truthiness test that sends an empty name to :func:`_span_name` runs on
        an exact ``str`` and never on a subclass's ``__bool__`` or ``__len__``.

        Args:
          fn: The function to wrap.

        Returns:
          The wrapper, typed as the original function.

        Raises:
          TypeError: From :func:`_refuse_unusable`, at decoration.
          Exception: Whatever the callable's own attributes raise while they are read, at
            decoration, in the caller's frame.
        """
        _refuse_unusable(fn)
        span_name = exact_name or _span_name(fn)
        if _is_async(fn):

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
                span, token, scope = _begin(span_name, span_defaults)
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
            span, token, scope = _begin(span_name, span_defaults)
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
