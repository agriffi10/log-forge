"""Background flush worker — non-blocking span delivery (arch §9, guide Phase 9)."""

from __future__ import annotations

import os
import queue
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Protocol, cast

from log_foundry import _diag, _ledger, _lifecycle
from log_foundry.results import FlushResult
from log_foundry.sinks import _engine
from log_foundry.sinks._refusal import is_final_refusal
from log_foundry.sinks._retry import floor_count

if TYPE_CHECKING:
    from log_foundry.decorator import _HandIn, _SweepResult
    from log_foundry.sinks.base import Sink, SinkLosses

__all__ = ["EventLedger", "Health", "Worker"]

_SHUTDOWN = object()


_lifecycle_state = _lifecycle._state
"""The lifecycle owner, bound once at import rather than reached through the module per read.

SPEC-054 FR-001 moved the retirement latch onto the owner, so :meth:`Worker.submit` asks
``retirements > self._epoch`` twice per call where it read one ``self`` attribute before.
Measured on this machine over 5M iterations in one process, best of 7: the ``self`` attribute
was 3.80 ns, and ``_lifecycle._state.retirements > self._epoch`` — a global load, two attribute
hops and a compare — is 14.86 ns, which is +22 ns across ``submit``'s two reads and outside
FR-001 AC-5's 10 ns budget. Bound here it is 7.55 ns per read, +7.5 ns per ``submit``, inside
it. The spec predicted under 2 ns per read from a 4.1 / 5.9 ns pair; that pair measured
``state.retirements`` with ``state`` already a local, which is not the expression it went on to
prescribe.

**The isolated read is the measurement that decides, because the whole-``submit`` one cannot see
this.** A 1M-call harness over ``submit`` spreads ~20 ns across repeated runs of the *same* tree
— more than the effect — so it resolves neither this choice nor the change itself; six
alternating rounds put the two spellings inside each other's range. An earlier version of that
harness also let one queue grow to a million entries inside the timer and reported +14 ns for a
change that is +3 ns, which was allocator behaviour rather than the read.

Binding the **object** is safe: ``_lifecycle._state`` is assigned once at that module's import
and never rebound — the suite mutates its fields rather than replacing it — and a fork copies the
object, so this names the same owner in the child. It is a second *name* for one object and not
a second copy of any state, which is what would make it the twin this spec exists to delete.
"""

DEFAULT_SHUTDOWN_TIMEOUT = _lifecycle.DEFAULT_SHUTDOWN_TIMEOUT
"""Re-exported from ``_lifecycle``, which owns it (SPEC-040 FR-001).

It moved because the lifecycle owner binds it as a **def-time** default and cannot import this
module at module scope. Re-exported rather than relocated outright so that
``log_foundry.worker.DEFAULT_SHUTDOWN_TIMEOUT`` — which ``__init__`` re-exports publicly and
SPEC-034 froze — keeps naming the same object.
"""


_PUT_POLL_SECONDS = 0.05
"""How long a full-queue ``flush()`` waits before re-asking whether the drain was abandoned.

Small enough that an unbounded caller notices promptly, large enough that a full queue does not
become a spin. It bounds only the *slice*, never the caller's own deadline (SPEC-050 FR-001).
"""

DEFAULT_SWAP_TIMEOUT = _lifecycle.DEFAULT_SWAP_TIMEOUT
"""Re-exported from ``_lifecycle``, which owns it (SPEC-040 FR-001).

Moved and re-exported for the reason :data:`DEFAULT_SHUTDOWN_TIMEOUT` was.
"""

_JOIN_MARGIN: float = 0.1
"""Seconds the drain thread needs after its last attempt returns to be seen to end (SPEC-062).

After ``sink.emit`` returns from the last attempt the drain thread still counts the batch,
writes the ``_diag`` line, answers the flush markers and leaves :meth:`Worker._run`, and
``Thread.join`` must observe that end inside the caller's timeout. :meth:`Worker._emit` adds
this to its prediction of when a retry would end, and :meth:`Worker._emit_pending` to its
prediction of when a later exit batch's first attempt would end (SPEC-090 FR-003), both through
:meth:`Worker._retry_declined`, so an attempt permitted by the rule leaves the join time to
return. It is that window and nothing more: it reserves no time for ``close()``,
which the lifecycle owner runs inline after the join whatever the budget, and it is a module
constant rather than a parameter because a knob would let a caller recreate the overrun the
budget exists to remove. ``_retry_declined`` reads it at each decision rather than binding it
at definition, so a test can patch it to prove it is added.
"""

_BACKLOG_BATCH_CAP: int = 1_000
"""The most events a batch grows to while a backlog is waiting (SPEC-090 FR-001).

In events, not submissions. A batch holds more than ``batch_size`` submissions only while its
events stay within the size target, and the target never exceeds this, so no batch exceeds it
unless it holds at most ``batch_size`` submissions, which is a batch the count trigger alone
would also send. 1,000 is the largest default per-request count of any shipped sink: the HTTP
family's ``DEFAULT_MAX_BATCH_COUNT`` and the ``chunk_size`` of ``ClickHouseSink`` and
``PostgresSink``. Past it a chunking sink makes more requests per call and saves no call, and a
sink that fails after running fast abandons its batch whole, so the cap bounds that loss. It is
a module constant, not a ``Worker`` argument, because capacity is not a setting (Andrew,
2026-11-15), and it is read at each use, as ``_JOIN_MARGIN`` is, so a test can patch it.
"""


QUEUE_FULL = -1
"""What :meth:`Worker._offer` returns when the queue had no room and nothing was put."""

type _Totals = tuple[int, int]
"""The ``dropped`` and ``submitted_after_shutdown`` totals one enqueue moved, each 0 if unmoved."""

_SWEEP_WAIT: float = 0.05
"""Seconds the drain thread waits for the sweep hold before it skips a sweep (SPEC-091 FR-002).

Every other caller of the hold blocks; the drain alone is bounded, because it is the one thread
the others wait on, so a thread holding the hold that then waits on the drain would otherwise wait
on a drain waiting on it. On expiry the drain skips the sweep and tries again on its next pass, so
a holder that keeps the hold costs the drain at most twenty idle waits a second rather than a
spin. The longest hold measured while this was chosen, one sweep of 1,000 spans of ten events each
(``spec091_hot_path.py hold``, 555.8 µs under a load average near 30), is about a ninetieth of it,
so an ordinary hold never makes the drain skip. Shorter skips sweeps a busy process needs; longer
holds a ``flush()`` made inside a hold that much longer.
"""


class _Sweep(Protocol):
    """The type of ``Worker``'s ``sweep=``: ``decorator._sweep_registered`` (SPEC-091 FR-002).

    A protocol rather than a ``Callable`` alias because the drain passes ``wait`` by keyword. The
    shapes it names live in ``decorator``, imported for type checking only, so this module still
    knows nothing of spans at runtime.
    """

    def __call__(self, hand_in: _HandIn, *, wait: float | None = None) -> _SweepResult | None:
        """Hands in every open span's take, or takes nothing and returns ``None``.

        Args:
          hand_in: Offers one span's take, returning ``None`` when it put nothing.
          wait: The longest wait for the sweep hold, or ``None`` to block.

        Returns:
          The events handed in, the late submissions left to count, and the hand-in faults, or
          ``None`` when the hold was not taken.

        Raises:
          None.
        """


def _bounded_seconds(timeout: float | None) -> str:
    """Renders a shutdown timeout for a diagnostic without trusting its ``__str__``.

    The timeout is the caller's, so it is a value the library does not control — the rule
    ``_diag.errno_of`` follows for an ``errno``. A non-number renders as ``"?"`` rather than
    whatever its ``__repr__`` chose to say (SPEC-029 FR-002).

    Args:
      timeout: The caller's timeout, or ``None`` for an unbounded wait.

    Returns:
      The rendered seconds, ``"no timeout"``, or ``"?"``.

    Raises:
      None.
    """
    try:
        return f"{float(timeout):g}s" if timeout is not None else "no timeout"
    except Exception:
        return "?"


@dataclass(frozen=True, kw_only=True)
class EventLedger:
    """Every accepted event, in the one term it is in now, counted in events (SPEC-088 FR-001).

    An event is credited to ``accepted`` at the call that asks for it, before the library tries
    to build it, and from then on it is in exactly one other term: still on its way (the three
    gauges), out through an exit (``delivered``, ``skipped``), or lost (the six ``*_lost`` terms
    and ``stranded``). So ``accepted == pending + delivered + skipped + lost`` once no library call
    is in progress. A gauge can fall; every other term only rises in a process, which is what
    lets the events lost between two points be read as the fieldwise difference of two readings.

    ``unaccounted`` is not a term. It is how far the terms are from balancing, the events the
    ledger cannot place, and it is zero in a correct library. A gauge that would read below zero
    is published as zero and its deficit is counted there instead, so a reader never meets a
    negative count.

    Attributes:
      accepted: Events asked for: a traced call's ``span.start`` and ``span.end``, and each level
        call, whether or not the event could then be built.
      open_spans: Events buffered on a span and not yet handed to the worker. A remainder, so it
        also holds the events the recorded append window strands on a closed span
        (``architecture.md`` §13).
      queued: Events in the worker's queue while its drain thread runs. Events, where
        ``Health.queued`` counts submissions and markers.
      in_flight: Events the drain thread has taken and not finished with, plus orphan logs whose
        call is still running.
      delivered: Events whose ``emit`` returned, or that a ``deliver`` sink answered
        ``ACCEPTED``.
      skipped: Events a ``deliver`` sink answered ``SKIPPED``.
      in_span_lost: Events that could not be built or handed over inside a span; the same value
        as ``Health.in_span_lost``.
      orphan_lost: Orphan logs that could not be built, or whose run failed or raised; the same
        value as ``Health.orphan_lost``.
      overflow_lost: Events whose submission met a full queue. ``Health.dropped`` counts the
        same submissions.
      batch_lost: Events of a batch abandoned after its retries, or whose retry was declined,
        or an exit batch declined unattempted because it was predicted to end after the
        ``shutdown()`` deadline (SPEC-090 FR-003).
      sink_lost: Events of a settled run that ended neither ``ACCEPTED`` nor ``SKIPPED``.
      drain_lost: Events the drain thread held when it died.
      stranded: Events in the queue of a worker whose drain has ended: its thread has ended, or
        it has debited the last of what it held and will take nothing more (SPEC-089).
      unaccounted: Events the terms cannot place; zero when they balance.
    """

    accepted: int = 0
    open_spans: int = 0
    queued: int = 0
    in_flight: int = 0
    delivered: int = 0
    skipped: int = 0
    in_span_lost: int = 0
    orphan_lost: int = 0
    overflow_lost: int = 0
    batch_lost: int = 0
    sink_lost: int = 0
    drain_lost: int = 0
    stranded: int = 0
    unaccounted: int = 0

    @property
    def pending(self) -> int:
        """Events still on their way: ``open_spans + queued + in_flight``.

        Args:
          None.

        Returns:
          The sum of the three gauges.

        Raises:
          None.
        """
        return self.open_spans + self.queued + self.in_flight

    @property
    def lost(self) -> int:
        """Events lost: the six ``*_lost`` terms and ``stranded``.

        Args:
          None.

        Returns:
          The sum of the seven loss terms.

        Raises:
          None.
        """
        return (
            self.in_span_lost
            + self.orphan_lost
            + self.overflow_lost
            + self.batch_lost
            + self.sink_lost
            + self.drain_lost
            + self.stranded
        )


class _CountingQueue(queue.Queue[object]):
    """The worker's queue, counting the events of each submission it takes in (SPEC-088 FR-001).

    ``_put`` runs under the mutex ``put`` and ``put_nowait`` already hold, so the count is exact
    and :meth:`Worker.submit` writes no counter of its own on the accept path. Flush markers and
    the shutdown sentinel are not submissions and count nothing.

    Attributes:
      entered: Events of the submissions this queue has taken in.
    """

    def __init__(self, maxsize: int = 0) -> None:
        """Builds the queue with nothing taken in.

        Args:
          maxsize: The bound, as ``queue.Queue`` takes it.

        Returns:
          None.

        Raises:
          None.
        """
        self.entered = 0
        super().__init__(maxsize)

    def _put(self, item: object) -> None:
        """Counts a submission's events, then queues it.

        **A submission is classified as the drain classifies it**: anything that is neither the
        shutdown sentinel, by identity, nor a flush marker, by its exact type, which
        :meth:`Worker._final_drain` and :meth:`Worker._drain` then take and count through
        :meth:`Worker._take`, whose first call is :meth:`Worker._hold`. So the taken-in count and
        the taking count agree by construction, and the test reads the item's real type, never a
        class hook the item could answer with.
        ~~``isinstance(item, list)``~~ — replaced after the rebase onto ``bb6656ff``, whose
        ``tests/test_class_check_roster.py`` refuses an ``isinstance`` that runs a value's class
        hook; it also asked a different question from the drain's. ``_FlushMarker`` has no
        subclass, so the exact type and the drain's ``_diag.is_a`` agree, and no call is added.

        It appends to ``self.queue`` itself rather than calling ``super()._put``, which is that one
        append: the zero-argument ``super()`` lookup cost more than the count, and measured, the
        counting put cost 48 to 55 ns over a plain one with it, against FR-005's bound of 50, and
        measures 31.2 ns (3.13.1) and 26.9 ns (3.12.12) with the classification above. The deque
        is the attribute :meth:`Worker._queued_submissions` already reads (``architecture.md`` §13
        records the reliance).

        Args:
          item: A submission, a flush marker or the shutdown sentinel.

        Returns:
          None.

        Raises:
          None.
        """
        if item is not _SHUTDOWN and type(item) is not _FlushMarker:
            self.entered += len(item)  # type: ignore[arg-type]
        self.queue.append(item)


@dataclass(frozen=True, kw_only=True)
class Health:
    """A point-in-time snapshot of the worker's delivery counters (SPEC-017 FR-005).

    Construction is **keyword-only** (SPEC-051 FR-001), which is what makes appending a field
    safe: order is not part of the contract, so a new counter can go anywhere and no third-party
    construction can bind to a position. It was previously ordered — every field after
    ``failed_batches`` is defaulted and was appended in the order the specs landed. The claim
    that a caller could also subscript this, carried here from the ``NamedTuple`` SPEC-034
    replaced, was false from the moment it became a dataclass: ``len(health())`` raises
    ``TypeError``, and ``README.md`` said the opposite correctly throughout.

    Attributes:
      queued: Submissions currently buffered. Approximate by nature: it is read without
        stopping the world, and counts the internal flush/shutdown markers alongside real
        submissions — normally only in passing, but a ``flush()`` marker stranded by racing
        ``shutdown()`` is answered and then counted for the life of the process, since
        ``Queue`` offers no way to remove one specific item. A ``_SHUTDOWN`` sentinel put after
        the final drain fixed the bound on what it takes, when it began (SPEC-090 FR-002), is
        counted the same way, a recorded exception (``architecture.md`` §12).
      dropped: Submissions discarded because the queue was full (backpressure).
      failed_batches: Batches abandoned after the retry budget was spent, or whose retry the
        exit drain declined because it was predicted to end after the ``shutdown()``
        deadline (SPEC-062), or that the exit drain declined unattempted for the same
        prediction once an earlier exit batch had made its first attempt (SPEC-090 FR-003).
      stopped_reason: The exception type name that terminated the drain thread, or ``None`` if
        it never died — which is also what a live worker and a process that never logged
        report. Non-``None`` is categorically worse than the two counters above: they measure
        loss the worker absorbed and kept running through, this one means the worker is gone
        (SPEC-019 FR-003). Also ``"ShutdownTimeout"`` when a bounded :meth:`Worker.stop`
        expired before the drain finished (SPEC-027 FR-004), and the type name of whatever
        stopped a forked child's rebuild from starting a thread at all (SPEC-039 FR-002) — a
        case where the drain thread never ran rather than died. All three are the same thing to
        a reader, which is why they share the field: nothing is delivering.
      sink: The configured sink's own loss counters, or ``None`` when the sink reports
        nothing (SPEC-026 FR-003). Nested rather than folded into the
        integers above because they count different things: ``dropped`` here is backpressure
        at this queue, ``dropped`` on the sink is an event that never reached the wire.
        ~~or ``None`` when there is no worker~~ — struck (SPEC-021): that was the two-owner
        shape describing itself as the contract. Measured against a ``MultiSink`` with one
        raising child, an ``info()`` outside a span reported ``None`` here while the sink's own
        ``losses()`` read ``failed=1``, which contradicts ``docs/invariants.md`` §2. SPEC-054
        FR-005 fills it from the configured sink on every path.
      retired: Whether ``shutdown()`` has been called. It describes an action the
        caller took, not a failure the library detected, which is why it is a boolean where
        ``stopped_reason`` is a string — SPEC-019 rejected an ``alive`` flag because it would
        read ``False`` for a process that never logged, and that objection does not apply to a
        field which is simply ``False`` until someone calls ``shutdown()`` (SPEC-030 FR-001).
        On its own it is not a fault: a process that shuts down and stops logging is correct.
        It is the one field ``_lifecycle._worker_health`` synthesizes rather than zeroing, so
        that a process which only ever logged outside a span — and therefore has no worker at
        all — still reports its own shutdown truthfully (SPEC-031 FR-006).
      submitted_after_shutdown: Submissions accepted after ``shutdown()`` and queued where
        nothing will drain them. Non-zero alongside ``retired`` is the signature of the
        serverless mistake — ``shutdown()`` called per invocation on a warm container, so the
        first invocation logs and every later one silently does not. The count starts at the
        moment ``shutdown()`` begins rather than when the drain thread ends, so a submission
        racing the final drain is counted even if that drain carried it; erring toward
        reporting is the right direction for a signal whose whole purpose is visibility. A
        submission counts only once its put has succeeded (SPEC-088 FR-002): one that met a full
        queue is in ``dropped`` alone, so this reads 0 when every late submission overflowed.
      incomplete_swaps: Late ``configure(sink=...)`` calls whose drain of the previous sink
        did not complete (SPEC-030 FR-003). The swap still took effect, so the caller has the
        sink it asked for, but two things could not be guaranteed: events submitted before the
        call may have been carried to the new sink instead of the old one, and the old sink was
        left open **for now** rather than closed, because the drain thread may still be inside its
        ``emit`` — the reasoning SPEC-027 FR-004 applies to an expired ``shutdown()``.
        ~~left **open** rather than closed~~ — struck (SPEC-050 FR-004): that objection is about
        the *instant* of the swap and expires when the drain thread does, so the sink is recorded
        and closed by the first ``shutdown()`` that finds the thread ended. A non-zero count here
        still means events may have been misrouted; it no longer means a sink leaks.
        It describes the **worker's drain** and nothing else. A swap on the orphan path has no
        queue and no drain, so there is nothing to confirm and this stays zero there (SPEC-033
        FR-006); an expired *close* join reports nothing on either path, by the decision that
        made the bounded close available at all. A non-zero value therefore always means events
        may have been misrouted, never merely that a close was slow — ``closing_sinks`` is the
        field for that.
      closing_sinks: Swapped-out sinks whose ``close()`` is running *at this instant* — a live
        gauge rather than a counter, so it falls as well as rises. ~~the only field here that
        can fall~~ — struck (SPEC-034 AC-2c): ``queued`` falls on every drain. Those two are the
        only gauges and every other integer here is monotonic, which is the distinction an
        operator alerting on "any non-zero" needs and which no name encodes. Stated as the rule
        rather than as a count: it said "the other five" until SPEC-051, having gone stale when
        SPEC-036 appended ``orphan_lost`` and ``in_span_lost`` and made it six. A close is
        bounded only in how long ``configure()`` waits for it, so this is how a destination
        stuck in ``close()`` becomes visible at all. Reading it non-zero once means a swap just
        happened; reading it non-zero repeatedly means a close is not coming back, and that sink
        still holds its resources. It is deliberately a live read rather than a count of expired
        joins: a slow close and a stuck one are indistinguishable at the moment a join expires,
        and SPEC-028 reverted a design that guessed.
      inherited_sink: Whether the sink this process **last installed for delivery** is one it
        may not release — one it inherited across a ``fork`` (SPEC-042 FR-004). "Last
        installed", not "would deliver to now": after ``shutdown()`` the process delivers
        nowhere, and reporting ``True`` there is the point rather than a wrinkle, since an
        inherited sink left open at exit is exactly what this explains. **The referent is one
        object, named here because SPEC-033 measured three candidates disagreeing** — the
        worker's sink if a worker exists, else the sink the orphan path recorded, else the
        configured one. That contest is over: SPEC-054 FR-005 answers this and ``sink`` from
        the **config**, so the three candidates are history rather than a live fallback chain.
        It describes that object and *not* the graph beneath it, so a child that wraps an
        inherited sink in a ``MultiSink`` of its own reads ``False`` here while the wrapper's
        child is still refused — stated because the opposite reading is the natural one.
        It is a **state, not a fault**, and deliberately not a term in the documented alert
        idiom, which is the call ``closing_sinks`` got. It explains a handle still open after
        ``shutdown()``, and it is the signal that a deployment shares a sink across a fork at
        all. ``True`` for a shared ``StdoutSink`` too, whose ``close()`` only flushes — so a
        ``True`` is not by itself evidence that anything is held.
      orphan_lost: Events lost on the **synchronous** path — a level call made with no active
        span, which emits on the caller's own thread with no worker between it and the sink
        (SPEC-036 FR-003). Until this field existed that loss was counted nowhere: ``health()``
        describes a worker, and this path has none, so a process logging only this way read all
        zeros over total loss. Not ``failed_batches``, which means batches a worker abandoned
        after spending a retry budget and kept running past; there is no batch, no retry and no
        worker here. Not ``SinkLosses`` either — the sink did not absorb anything, it raised,
        which is what SPEC-026 requires of it. It covers everything inside the orphan guard, a
        sink that failed to *construct* included, so it climbing means **the destination or the
        data**.
      in_span_lost: Events lost while being built or handed over *inside* a span (SPEC-037
        AC-5c, deferred to SPEC-036 FR-003 so the pair was designed together). The in-span path
        cannot lose an event at ``emit`` — that is ``failed_batches`` — so this has exactly two
        causes: **the data**, a value that could not be built into an event; and, since SPEC-050
        FR-003, **no destination at all**, where the process could not give the library a thread
        to deliver through and the span's whole buffer was lost with no worker in existence to
        report anything. ~~this climbing means **the data**, always~~ — struck (SPEC-050
        FR-003): it was true only while that second cause was counted nowhere. ~~The two are told
        apart by the count, which is one event for the first and the span's whole buffer for the
        second~~ — struck (SPEC-088 FR-004): a span that fails to open now counts its two boundary
        events, a close that fails counts its buffer and its ``span.end``, and an interrupted
        build counts one, so the count no longer tells the causes apart. They are told apart by
        the stderr line, which names the site. Two fields rather than one
        because they aggregate different failure populations and so fail SPEC-026's test,
        *would one number hide which fix applies*. Their sum is deliberately not reported: with
        different populations it is a number nobody can act on.
      events: The event ledger (SPEC-088 FR-004): every accepted event in the one term it is in
        now, counted in **events**, with ``unaccounted`` the events the terms cannot place. Its
        terms are ``accepted``, the gauges ``open_spans``, ``queued`` and ``in_flight``, the exits
        ``delivered`` and ``skipped``, the losses ``in_span_lost``, ``orphan_lost``,
        ``overflow_lost``, ``batch_lost``, ``sink_lost``, ``drain_lost`` and ``stranded``, and the
        properties ``pending`` and ``lost``. ``events.queued`` counts events where ``queued``
        above counts submissions and markers, and ``events.overflow_lost`` counts the events of
        the submissions ``dropped`` counts; ``events.in_span_lost`` and ``events.orphan_lost`` are
        the two fields above, read once for both. Defaults to an all-zero ledger, so a ``Health``
        built by hand needs no argument for it and a reader never tests it for ``None``.
    """

    queued: int
    dropped: int
    failed_batches: int
    stopped_reason: str | None = None
    sink: SinkLosses | None = None
    retired: bool = False
    submitted_after_shutdown: int = 0
    incomplete_swaps: int = 0
    closing_sinks: int = 0
    inherited_sink: bool = False
    orphan_lost: int = 0
    in_span_lost: int = 0
    events: EventLedger = EventLedger()


@dataclass(frozen=True, kw_only=True)
class SwapOutcome:
    """What :meth:`Worker.retarget` did, and the sink it displaced (SPEC-054 FR-003).

    Internal, and deliberately not in ``__all__``: SPEC-034 froze the public surface and this is
    a hand-off between two modules of the library. Reporting through a value rather than a
    predicate is SPEC-035 FR-003's decision — the decline is taken between two lock acquisitions
    and nothing outside the worker can observe it.

    Attributes:
      verdict: ``declined`` when this worker retired mid-swap — before the reassign, keeping its
        sink, or after it, when the fence flush found it retired and the shutdown that retired it
        had taken the previous sink,
        ``fenced`` when a drain after the reassign confirmed nothing can still be inside the
        previous sink, ``unfenced`` when that confirmation did not arrive within the budget.
      previous: The sink it displaced, which is the new sink itself when there was nothing to do.
    """

    verdict: Literal["declined", "fenced", "unfenced"]
    previous: Sink | None


class _FlushMarker:
    """A drain request travelling the queue in FIFO order (SPEC-013 FR-002).

    The mechanism follows from the queue being FIFO: everything submitted before ``flush()``
    was called is necessarily ahead of the marker, so by the time the worker dequeues it those
    events are either already emitted or sitting in ``pending``. Like ``_SHUTDOWN`` it is never
    emitted, but unlike ``_SHUTDOWN`` it carries state, so it is a class rather than a bare
    sentinel.

    ``delivered`` carries the drain's outcome back to the waiter, not merely the fact that the
    marker was reached (SPEC-021 FR-001). It is written by the drain thread before
    ``event.set()`` and read by the waiter after ``event.wait()`` returns, so the ``Event``
    supplies the ordering and no further lock is needed. It starts ``False`` because every path
    that answers a marker assigns it explicitly, leaving the default to be read only when the
    drain thread died without computing an answer.

    The outcome is computed from ``seen_failures`` — ``Worker.failed_batches`` as it stood when
    the marker was created — against the same counter when the marker is answered, so every
    flush outstanding when a batch is abandoned reports it. A batch abandoned before the call
    is deliberately not in scope: that loss is already in ``failed_batches`` and on stderr, and
    folding it in would make every later empty flush report a failure it did not incur.

    **The three SPEC-089 fields carry the end of a counted flush's window back the same way.**
    ``counted`` asks the answering drain for a ledger snapshot; only ``log_foundry.flush()``
    asks, so :meth:`Worker.retarget`'s internal flushes pay nothing. ``ledger`` is that
    snapshot, written before ``event.set()`` like ``delivered``, and ``ledger_pid`` the pid the
    drain read it in, stamped beside it so a fork between the read and the caller's wake cannot
    misdate it. ``unwinding`` says the marker was answered while its drain was unwinding an
    exception, so the snapshot would have been taken before ``_terminal_failure`` debited
    ``drain_lost`` and before the queue counted as ``stranded``; the caller waits on
    ``Worker._drain_accounted`` and reads for itself instead.
    """

    __slots__ = (
        "counted",
        "delivered",
        "event",
        "ledger",
        "ledger_pid",
        "seen_failures",
        "unwinding",
    )

    def __init__(self, seen_failures: int, *, counted: bool = False) -> None:
        """Stamps a marker with the failure count it will be judged against.

        Args:
          seen_failures: ``Worker.failed_batches`` as read on the caller's thread, before the
            marker joins the queue.
          counted: Whether the drain answering it should snapshot the event ledger (SPEC-089).

        Returns:
          None.

        Raises:
          None.
        """
        self.event = threading.Event()
        self.delivered = False
        self.seen_failures = seen_failures
        self.counted = counted
        self.ledger: EventLedger | None = None
        self.ledger_pid: int | None = None
        self.unwinding = False


class _Reading:
    """One reading of the event ledger, with the residuals the published terms leave out.

    The published ``EventLedger`` is what a caller reads. The residuals are kept beside it for the
    test suite's balance fixture (SPEC-088 FR-006), which reads at rest and so can hold each one
    to zero where the shutdown check, which may read while calls run, counts only the positive
    side.

    Attributes:
      ledger: The published terms.
      orphan_residual: Orphan credits less the orphan gauge, exits and ``orphan_lost``.
      meeting_residual: Hand-overs since the process worker was built less what its queue took
        in and what overflowed, or ``None`` with no process worker.
      drain_residual: The process worker's taken events less those held and settled.
    """

    __slots__ = ("drain_residual", "ledger", "meeting_residual", "orphan_residual")

    def __init__(
        self,
        ledger: EventLedger,
        orphan_residual: int,
        meeting_residual: int | None,
        drain_residual: int,
    ) -> None:
        """Records one reading.

        Args:
          ledger: The published terms.
          orphan_residual: The orphan residual.
          meeting_residual: The meeting residual, or ``None``.
          drain_residual: The drain residual.

        Returns:
          None.

        Raises:
          None.
        """
        self.ledger = ledger
        self.orphan_residual = orphan_residual
        self.meeting_residual = meeting_residual
        self.drain_residual = drain_residual


def _read_ledger(worker: Worker | None) -> _Reading:
    """Reads the event ledger once, in the order that keeps every reading sound (SPEC-088 FR-001).

    A reading is several loads, not one, and calls run while it is taken, so the order is what
    stops a call that is balancing correctly from reading as an imbalance. **The order**, built as
    the plan review checked it (M2): the process worker (read by the caller), then whether its
    drain is still running (``Worker._drain_terms``), its taking count, held events and exits;
    ``in_span_lost``; every cell's hand-overs (pass 1); the queue's taken-in count and
    ``_overflow_lost``; every cell's span credits (pass 2); the orphan pass, cell by cell;
    ``orphan_lost``. Each property rests on one rule, that a count is read before any count
    written earlier in the life of the same events:

    - ``accepted`` is never below the worker path's exits and losses: every one of them was
      written after its events' credits, and is read before them.
    - ``open_spans`` never reads below zero: a hand-over pass 1 reads was written after the take,
      the take after the append, the append after the credit, whichever thread's cell holds it;
      pass 2 runs later, from a fresh snapshot, and reads it. Read cell by cell instead, a span
      shared through a copied context (``asyncio.to_thread``) can read its hand-over without its
      credit, so the shutdown check would fire on a call that was balancing.
    - ``queued`` never reads below zero: the taking count is read before the taken-in count.
    - The meeting residual never reads above zero while hand-overs run: a hand-over is credited
      after ``Worker.submit`` returns, so pass 1 reads it only after the queue counted it or it
      overflowed, and both are read after pass 1.
    - The drain and orphan residuals never read above zero: each is read in the reverse of the
      order its writer writes it, the gauge raised before the credit and lowered after the exit.
    - ``queued`` and ``stranded`` split the queue on whether the drain is still running, read
      first: it reads ended once ``Worker._drain_accounted`` is set, which follows the drain's
      last take and last debit (SPEC-089 FR-003), or once the thread has ended. A reading that
      sees it ended therefore sees every debit, so ``held`` is then settled and nothing it holds
      is counted twice or missed.

    It takes the registration lock once per pass and no other lock: not ``Runtime._lock``,
    ``Worker._lock``, the queue's mutex or ``decorator._loss_lock``, whose counters are each read
    as one load. So it waits behind no ``submit``, drain or lifecycle call, and it may run on any
    thread, the drain thread included.

    Args:
      worker: The process worker, read by the caller before anything here, or ``None``.

    Returns:
      The published terms, with the three residuals.

    Raises:
      None.
    """
    from log_foundry import decorator

    if worker is None:
        at_build = 0
        running, taken, held = False, 0, 0
        w_delivered = w_skipped = w_sink_lost = batch_lost = drain_lost = 0
    else:
        at_build = worker._handed_at_build
        (
            running,
            taken,
            held,
            w_delivered,
            w_skipped,
            w_sink_lost,
            batch_lost,
            drain_lost,
        ) = worker._drain_terms()
    in_span_lost = decorator._in_span_lost
    handed = _ledger.handed()
    entered = 0 if worker is None else worker._queue.entered
    overflow_lost = 0 if worker is None else worker._overflow_lost
    span = _ledger.spans()
    orphan, orphan_open, o_delivered, o_skipped, o_sink_lost = _ledger.orphans()
    orphan_lost = decorator._orphan_lost
    in_queue = entered - taken
    open_spans = span - in_span_lost - handed
    drain_residual = (
        taken - held - (w_delivered + w_skipped + w_sink_lost + batch_lost + drain_lost)
    )
    orphan_residual = orphan - orphan_open - (o_delivered + o_skipped + o_sink_lost) - orphan_lost
    meeting = None if worker is None else handed - at_build - entered - overflow_lost
    unaccounted = (
        (0 if running else max(0, held))
        + max(0, orphan_residual)
        + max(0, meeting or 0)
        + max(0, drain_residual)
        + max(0, -open_spans)
        + max(0, -in_queue)
        + max(0, -held)
        + max(0, -orphan_open)
    )
    ledger = EventLedger(
        accepted=span + orphan,
        open_spans=max(0, open_spans),
        queued=max(0, in_queue) if running else 0,
        in_flight=max(0, held) + max(0, orphan_open),
        delivered=w_delivered + o_delivered,
        skipped=w_skipped + o_skipped,
        in_span_lost=in_span_lost,
        orphan_lost=orphan_lost,
        overflow_lost=overflow_lost,
        batch_lost=batch_lost,
        sink_lost=w_sink_lost + o_sink_lost,
        drain_lost=drain_lost,
        stranded=0 if running else max(0, in_queue),
        unaccounted=unaccounted,
    )
    return _Reading(ledger, orphan_residual, meeting, drain_residual)


class Worker:
    """Owns a bounded queue and daemon thread that batch and flush events to a sink.

    :meth:`submit` is a fast, in-process handoff, so a slow or down sink can never
    back-pressure the app: the queue is bounded and overflow is dropped-newest with a counter
    (arch §9). Two drains are deliberately distinct (SPEC-013) — :meth:`stop` retires this worker
    and it never comes back, while :meth:`flush` drains on demand and leaves everything running,
    which a process that is frozen rather than exited needs. This class owns delivery mechanics
    only, and knows nothing about spans or context.
    """

    _FORK_SKIP = ("_held", "_taken_markers", "_queue", "_thread")
    """Attribute names ``_fork``'s repair walk must not read or descend into (SPEC-050 FR-004).

    ``_queue`` and ``_thread`` joined at SPEC-079, when the worker became a root of the walk.
    Neither holds a primitive the walk could replace and neither holds anything the marking needs:
    the queue is replaced whole by :meth:`_reinit_after_fork` with its mutex and conditions, and
    the thread did not survive the fork.

    ``_taken_markers`` names ``flush()`` callers **in the parent**, whose ``Event`` the walk would
    otherwise replace on an object nothing in the child will ever wait on. The skip stops that
    work and :meth:`_reinit_after_fork` drops the references, because a skip alone leaves them
    held — measured, a child inherited the parent's in-flight marker permanently and kept it
    through fifty further flushes of its own.

    ``_held`` names the sinks this worker's thread may still be inside — its live sink, plus any
    it swapped out without confirming a fence (SPEC-054 FR-003). The superseded half is exactly
    the shape ``_fork._SKIP_ATTRIBUTE`` describes: bookkeeping that pins objects is not live state
    to repair. Without the opt-out the walk reaches a superseded sink, replaces its locks — merely
    wasteful — and runs its fork hooks, which is not: ``_lifecycle.reclaim`` then overwrites the
    foreign-pid record the child holds for it — the parent's own stamp, or the ``_FOREIGN``
    ``_walk_after_fork`` writes where the parent recorded nothing — leaving a child able
    to release a transport it never acquired. Nothing is lost by skipping it, for the reason
    ``_lifecycle._state._owned``'s entry gives: a sink that is still *live* is reached through
    ``self.sink`` and the config, neither of which is opted out — which is what makes skipping the
    whole list safe even though the live sink is in it.
    """

    def __init__(
        self,
        sink: Sink,
        *,
        batch_size: int = 10,
        flush_interval: float = 1.0,
        max_queue: int = 10_000,
        max_retries: int = 3,
        offer_signal: bool = True,
        sweep: _Sweep | None = None,
    ) -> None:
        """Starts the drain thread and, unless told otherwise, offers the sink its stop signal.

        The lock guards the ``dropped`` counter, incremented from any caller thread, and the
        shutdown once-only flag, since ``shutdown`` may be called concurrently by ``atexit``
        and user code.

        **The exit drain fits the caller's budget** (SPEC-062 FR-001). ``_exit_deadline`` is
        the ``shutdown()`` caller's deadline as a ``time.monotonic()`` instant, ``None`` for an
        unbounded one, and it is written by :meth:`claim_stop` in the same decision that wins
        the once-only gate — so the caller whose timeout the join uses is always the caller
        whose deadline the drain declines against — and cleared only by
        :meth:`_reinit_after_fork` where that method starts a drain thread. Until a stop is
        claimed it is ``None``, which declines nothing, so ordinary draining, ``flush()`` and a
        swap retry exactly as before. It has one other writer: a caller that takes over a join the
        drain thread owed replaces the drain-thread caller's deadline with its own (SPEC-068
        FR-006). ``_join_owed`` records that debt — ``True`` only between a stop claimed **on**
        the drain thread, which cannot join itself, and the next caller off it, which takes the
        gate over. ``_join_taken_over`` records that the debt was paid, so the caller that took
        it over queues no second sentinel. Both are written under ``_lock`` by :meth:`claim_stop`
        and reset with ``_stopped`` where :meth:`_reinit_after_fork` starts a drain thread.
        ~~``_in_flight`` is the
        size of the batch :meth:`_emit` is holding, from before its first attempt until it returns~~
        — superseded by SPEC-088: ``_in_flight`` counts every event the drain thread has taken
        from the queue and not yet settled, which under this drain is the batch inside
        :meth:`_emit` whenever an emit is running, so an expired join can still say how many events
        are inside the emit it gave up waiting on (SPEC-062 FR-005), and it is the ledger's
        ``in_flight`` too, one counter the stopper reads once. It is written by the drain thread
        only, through :meth:`_hold` and :meth:`_settle`, and read without the lock by the stopper,
        as :attr:`draining` is read.

        **The ledger's worker terms** (SPEC-088 FR-001). ``_taken`` counts the events the drain
        has taken, ``_delivered``, ``_skipped``, ``_sink_lost``, ``_batch_lost`` and
        ``_drain_lost`` the exits it has written, all on the drain thread only; ``_overflow_lost``
        counts the events of overflowed submissions under ``_lock``, beside ``dropped``; and the
        queue counts what it took in. ``_handed_at_build`` is the process's hand-over count when
        this worker was built, so the meeting of the caller's hand-overs with this queue counts
        only the hand-overs it could have received.

        **The backlog batching state** (SPEC-090 FR-001). ``_pending_events`` is the events in
        the batch being built, ``_target`` the size target in events that the last emitted batch
        set (``0`` means the count trigger's batch), and ``_target_generation`` the sink
        generation that target was measured under; all three are written by the drain thread
        only. ``_sink_generation`` counts the sink swaps :meth:`retarget` made, under ``_lock``,
        so a measurement that straddled a swap is discarded without holding a reference to the
        sink it replaced, which keeps ``_FORK_SKIP`` unchanged. **The exit budget's state**
        (FR-003): ``_exit_batches`` counts the exit batches whose first attempt was made,
        ``_exit_longest`` is the longest attempt any of them made, ``_exit_declining`` records
        that an exit batch declined a retry or that one was declined, and
        ``_exit_abandoned_seen`` that the first abandoned exit batch has written its own line,
        all drain thread only; ``_abandoned_tally`` and ``_declined_tally`` are the
        ``(events, batches)`` not yet announced, written under ``_lock`` because :meth:`stop`
        takes them too.

        **``_drain_accounted`` is the third drain event** (SPEC-089 FR-003). It is set once the
        drain has debited everything it will ever debit: by :meth:`_terminal_failure` right after
        it debits what a dying thread held, before the line it writes, and by :meth:`_run`'s
        outermost ``finally`` on every exit. From then on nothing takes a queued event or debits
        a held one, so the ledger counts the queue as ``stranded`` as it does once the thread has
        ended, and a ``flush()`` waiting out a dying drain waits on this rather than on the
        thread: a ``join`` also waited out that line, which can block on a stderr the waiter
        itself holds.

        ``sink_released=`` used to sit here and is retired (SPEC-054 FR-002). It made a worker
        built during a ``shutdown()`` inherit a discharged close for a sink that shutdown's orphan
        branch had just closed. Under one owed-close record the flag has no work to do: this
        worker's build arms its sink, and the closer's second pass closes it *after* this worker's
        drain — which is one close per write-epoch (SPEC-045 FR-002) rather than the double it
        was added to prevent.

        The worker's build **arms** its sink and releases nothing. Which sinks are owed a close is
        the lifecycle owner's record now, and this constructor's only part in it is that the owner
        adds ``sink`` to that record in the same critical section it builds this worker in.

        Args:
          sink: The destination every batch is emitted to.
          batch_size: How many submissions accumulate before an emit is triggered while
            nothing more is waiting, and the fewest a batch holds under a backlog, where it
            grows by whole submissions toward the size target, up to ``_BACKLOG_BATCH_CAP``
            events (SPEC-090 FR-001).
          flush_interval: Seconds before a partial batch is emitted anyway.
          max_queue: Ceiling on buffered submissions, past which the newest is dropped.
          max_retries: Retries after a failing emit, an ``int`` floored at zero here (SPEC-066
            FR-001); anything else is refused with ``TypeError``, and a ``bool`` or ``int``
            subclass is stored as the exact ``int`` it holds. On the exit drain a retry is
            declined when it is predicted to end after the ``shutdown()`` deadline (SPEC-062),
            so the retries a failing batch sees there are bounded by the
            caller's budget as well as by this count. ~~the first attempt is always made~~ —
            refined by SPEC-090 FR-003: the first attempt of the first exit batch is always
            made, and a later exit batch is declined unattempted once one is predicted to end
            after the deadline.
          offer_signal: ``False`` when the owner hands the sink this worker's ``_stop`` itself,
            after releasing its lock. The library's own build does: the offer runs the sink's
            setter, and a setter that logs or calls ``configure()`` waited on the lock the build
            held (``_lifecycle._get_worker``).
          sweep: The sweep of open spans the drain runs about once a flush interval, or ``None``
            (SPEC-091 FR-002). The library's own build passes ``decorator._sweep_registered`` and
            nothing else passes one, so a worker a test builds by hand sweeps nothing and this
            module still knows nothing of spans.

        Returns:
          None.

        Raises:
          TypeError: If ``max_retries`` is not an ``int`` (SPEC-066), before any thread starts or
            stop signal is taken.
        """
        self.sink = sink
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.max_retries = floor_count(max_retries, 0, "max_retries", "Worker")
        self.dropped = 0
        self.failed_batches = 0
        self.stopped_reason: str | None = None
        self.submitted_after_shutdown = 0
        self.incomplete_swaps = 0
        self._max_queue = max_queue
        self._queue = _CountingQueue(maxsize=max_queue)
        self._stop = _lifecycle._state.refresh_stop_signal()
        self._epoch = _lifecycle._state.retirements
        self._drain_finished = threading.Event()
        self._drain_settled = threading.Event()
        self._drain_accounted = threading.Event()
        self._stopped = False
        self._exit_deadline: float | None = None
        self._join_owed = False
        self._join_taken_over = False
        self._in_flight = 0
        self._taken = 0
        self._delivered = 0
        self._skipped = 0
        self._sink_lost = 0
        self._batch_lost = 0
        self._drain_lost = 0
        self._overflow_lost = 0
        self._handed_at_build = _ledger.handed_unlocked()
        self._taken_markers: list[_FlushMarker] = []
        self._held: list[Sink] = [sink]
        self._lock = threading.Lock()
        self._pending_events = 0
        self._target = 0.0
        self._target_generation = 0
        self._sink_generation = 0
        self._exit_batches = 0
        self._exit_longest = 0.0
        self._exit_declining = False
        self._exit_abandoned_seen = False
        self._abandoned_tally = (0, 0)
        self._declined_tally = (0, 0)
        self._sweep = sweep
        if offer_signal:
            _lifecycle.offer_stop_signal(sink, self._stop)
        self._thread = threading.Thread(target=self._run, name="log-foundry-worker", daemon=True)
        self._thread.start()

    def _reinit_after_fork(self, *, resume: bool) -> None:
        """Rebuilds this worker **in place** for a child that has just forked (SPEC-039 FR-002).

        The child inherits a ``Worker`` whose thread does not exist, so ``submit`` goes on
        enqueueing and nothing drains: measured, six events never delivered, ``atexit`` closing
        the sink without a drain, and ``health()`` reading ``queued=2`` with every other field
        clean — the documented alert idiom blind. Rebuilding rather than retiring is the design:
        a prefork server's child is a working process, and a child that silently stops logging
        is the failure this arc exists to remove.

        **In place, never a new object** (FR-002 AC-5). The ownership guards keyed on
        ``_worker.sink is X`` and ``_lifecycle``'s registry are identity comparisons, so
        replacing the worker would leave every one of them answering about something else.

        **The queue is replaced, not drained.** Emptying it would keep ``queue.Queue``'s own
        mutex and its three ``Condition``s, which the fork walk cannot reach — it enters no
        standard-library container — and a fork landing inside that mutex leaves the child's
        very next ``submit`` blocked on the application's thread. That is why the replacement
        happens even when nothing is resumed: a retired worker still *accepts* submissions
        (SPEC-030 FR-001), so a retired child would block on the same mutex. Starting empty is
        also what stops the parent's undelivered backlog being sent twice (AC-2).

        ``stopped_reason`` is cleared rather than set to ``"Forked"`` (AC-6). SPEC-019 defines
        that field as "the drain thread died", and this child's drain thread is about to be
        running — the alternative reads as the more honest one and is not.

        ``_in_flight`` is reset on both branches beside the other per-drain fields, because a
        child inherits the parent's value for a drain thread that does not exist in it
        (SPEC-062 FR-005). The ledger's worker terms are reset with it, and the queue is a new
        counting queue: the child's ledger restarts (SPEC-088 FR-005), and its hand-over count
        restarts at zero with it, so ``_handed_at_build`` is zero too. ``_exit_deadline`` is reset only where a drain thread is started: a
        worker whose ``stop()`` was called directly rather than through ``shutdown()`` never
        moved the retirement count, so :func:`_lifecycle._rebuild_worker_after_fork` still
        resumes it, and without the reset every retry in that child would be declined against
        a deadline the parent's caller set, forever. A retired parent forks a retired child, and
        that child's record is left as it stands.

        **The backlog batching and exit budget fields are reset on both branches** (SPEC-090
        FR-001 rule 6), beside ``_in_flight`` and for its reason: they describe a drain thread
        that does not exist in the child. ``_pending_events`` goes to zero because the child's
        drain thread starts with an empty batch, and ``_target`` to zero so the child's first
        batch under a backlog is the count trigger's, measured afresh against the child's sink.
        The exit fields and both tallies go to their unstarted values, so a child neither
        declines against nor announces an exit the parent was making. ``_sink_generation`` is
        **not** reset: the child inherits it harmlessly, as it inherits ``sink``, and
        ``_target_generation`` is set to it, so the zeroed target is the current one.
        ``_lifecycle._adopt_factory_after_fork`` then reassigns ``sink`` without a bump, which
        is harmless for the same reason: the target is already zero, and the child's drain
        cannot emit before the fork handlers return.

        **The resume branch also resets the stop gate and replaces a set stop event**
        (SPEC-068 FR-005). Such a worker's child inherits ``_stopped`` true, which would make its
        first ``shutdown()`` a losing caller that records no deadline, and inherits ``_stop``
        **set**, because ``_fork._fresh_primitive`` carries an event's set state across — and
        :meth:`_drain` tests ``_stop`` first, so the resumed drain loop exited at once. Measured:
        the child's drain thread had ended before its first submission, which was never
        delivered and never counted. Resetting ``_stopped`` alone was measured too and is not
        enough: the deadline is then recorded, but the drain thread is still dead. So the branch
        clears ``_stopped``, ``_join_owed`` and ``_join_taken_over`` and takes its event from
        :meth:`~log_foundry._lifecycle.Runtime.refresh_stop_signal`, the call ``__init__``
        makes: the owner's current event when that is unset, so a worker never stopped keeps the
        event it had and the identity with the owner and the sink is unchanged, and a fresh one
        shared with the owner when it is set. It runs before the stop signal is offered, so the
        sink is handed the new event. The owner's lock is not taken, for the reason
        :func:`_lifecycle._rebuild_worker_after_fork` gives. The retired branch keeps all three
        as inherited — a fork does not undo a ``shutdown()`` — and an owed join inherited there is
        harmless: the next ``shutdown()`` in that child takes it over, finds ``_drain_finished``
        set and joins the inherited thread, which is dead in the child and returns at once.

        ``_held`` is reset to the live sink alone. A child stranded nothing, and ``releasable()``
        refuses the parent's sinks on the pid stamp anyway — so keeping a superseded one buys no
        close and costs a strong reference, plus a pointless refused release at every child exit.
        Emptying is what "a child inherits no promise" means for the record as well as for the
        close. ``_taken_markers`` goes with it: a child inherits no ``flush()`` caller to answer.

        The in-flight-close slot this method also cleared is gone (SPEC-054 FR-006). A close in
        flight is registered per sink in the close registry's in-flight half now
        (``_lifecycle._state._closes``, SPEC-078), and the child drops that whole half in
        ``_lifecycle._clear_after_fork`` for the reason this slot was cleared
        here: it names closes running on threads that did not survive the fork, and
        ``_fork._fresh_primitive`` carries an ``Event``'s set state across, so an inherited entry
        that was already **set** would answer the child's *own* later close instantly — its first
        background ``shutdown()`` claims a close, ``atexit`` reads "already finished" and returns,
        and the process exits through a running close.

        The drain events are **set or cleared to match what this child will actually do**,
        never simply inherited. Resuming clears them, so ``draining`` and ``flush``'s gate
        describe the thread starting here rather than the one that did not survive the fork;
        retiring **sets** them, because no thread will ever set them and a child forked while a
        ``shutdown()`` was mid-join otherwise inherits them unset with nothing to settle them.
        Two consequences, both measured: that child paid the whole 30 s budget at exit and with
        ``shutdown(timeout=None)`` would never exit at all, and — reaching further —
        ``_lifecycle._will_hand`` reads it as still draining and skips, leaving the sink holding
        a **set** stop event, so every later backoff returns instantly against a destination
        that is already refusing (SPEC-033 FR-004). ``_drain_finished`` has no reader on a
        retired worker and is set for the invariant rather than for an observable: settled with
        finished clear is what :attr:`draining` defines as an *abandoned* drain, and a child
        reporting no ``stopped_reason`` must not read that way. ``_drain_accounted`` (SPEC-089)
        follows the other two: cleared for the thread starting here, so a ``flush()`` waiting out
        a dying drain waits on that thread, and set where no thread will run, since nothing is
        left to debit. The stop signal is re-offered for
        the sink the fork walk cannot reach: a **third-party** sink is outside its ownership
        boundary, so it would keep pointing at the pre-fork event while this worker sets a new
        one — SPEC-027's guarantee broken by the repair meant to preserve it. **Except a sink the
        forking thread is being offered** (SPEC-083 FR-004), which :func:`_lifecycle.handing_here`
        reports: this handler runs in the child nested inside that sink's setter call, so offering
        it here ran the setter inside itself, and a setter holding its own lock hung the child
        before its first line, on the worker build's hand and on ``retarget``'s (measured at
        ``53b68203``). That sink keeps the event it holds, which the child's fork handler kept
        and its ``shutdown()`` sets (``_lifecycle._clear_after_fork``, FR-003).

        **The new thread is only installed once it has started**, which ``__init__`` never had
        to consider: a constructor whose ``start`` raises lets no ``Worker`` escape, while here
        the worker is already the process's. Assigning first would leave an unstarted ``Thread``
        on a live worker reading ``draining`` forever, and the next ``shutdown()`` would take a
        ``RuntimeError`` from ``join`` straight out of a public call documented to raise nothing.
        The inherited thread object is kept instead, which is safe because CPython repairs it
        across the fork — measured, it reports dead and both a bounded and an unbounded ``join``
        return in 0.0000 s — and the failure is recorded as a ``stopped_reason``, which is
        exactly SPEC-019's vocabulary for "nothing is delivering". Setting the drain events is
        what keeps :meth:`shutdown` from queueing a sentinel into a queue no thread will read.

        The success path assigns ``self._thread`` *after* the start, so for an instant a live
        drain thread coexists with the inherited dead one in that attribute. ~~That is safe only
        because the drain thread never reads it~~ — struck (SPEC-068 FR-006): :meth:`claim_stop`
        and :meth:`stop` now ask ``threading.current_thread() is self._thread``, and they run on
        the drain thread when a sink's ``emit`` calls ``shutdown()``. Inside that instant the
        comparison would read false, the call would join the dead thread, which returns at once,
        and :meth:`may_be_inside` would read the sink as free under an emit in flight. It cannot
        happen: the new thread reaches ``sink.emit`` only by taking a submission or a marker from
        the queue, the queue was replaced empty above, and nothing can fill it before the
        assignment, because this handler runs on the child's only thread and nothing else in the
        child runs until it returns. That is an argument this note states, not something any
        lock enforces.

        Args:
          resume: Whether to start a drain thread. ``False`` for a retired parent, which forks
            a retired child (AC-4): a fork does not undo a ``shutdown()``, and reviving a worker
            the caller terminated would be the library overruling them.

        Returns:
          None.

        Raises:
          None.
        """
        self._queue = _CountingQueue(maxsize=self._max_queue)
        self.dropped = 0
        self.failed_batches = 0
        self.submitted_after_shutdown = 0
        self.incomplete_swaps = 0
        self.stopped_reason = None
        self._in_flight = 0
        self._taken = 0
        self._delivered = 0
        self._skipped = 0
        self._sink_lost = 0
        self._batch_lost = 0
        self._drain_lost = 0
        self._overflow_lost = 0
        self._handed_at_build = 0
        self._taken_markers = []
        self._held = [self.sink]
        self._pending_events = 0
        self._target = 0.0
        self._target_generation = self._sink_generation
        self._exit_batches = 0
        self._exit_longest = 0.0
        self._exit_declining = False
        self._exit_abandoned_seen = False
        self._abandoned_tally = (0, 0)
        self._declined_tally = (0, 0)
        if not resume:
            self._drain_finished.set()
            self._drain_settled.set()
            self._drain_accounted.set()
            return
        self._exit_deadline = None
        self._stopped = False
        self._join_owed = False
        self._join_taken_over = False
        self._stop = _lifecycle._state.refresh_stop_signal()
        self._drain_finished.clear()
        self._drain_settled.clear()
        self._drain_accounted.clear()
        if not _lifecycle.handing_here(self.sink):
            _lifecycle.offer_stop_signal(self.sink, self._stop)
        thread = threading.Thread(target=self._run, name="log-foundry-worker", daemon=True)
        try:
            thread.start()
        except Exception as exc:
            self.stopped_reason = _diag.type_name(exc)
            self._drain_finished.set()
            self._drain_settled.set()
            self._drain_accounted.set()
            _diag.absorbed("starting this child's drain thread", exc, "it will deliver nothing")
            return
        self._thread = thread

    def submit(self, events: list[dict[str, object]]) -> None:
        """Hands a submission of span events to the worker, without blocking.

        ~~a finished span's events~~ — widened by SPEC-091: a submission is a span's buffer handed
        over at its close or by an in-span ``flush()``, and the worker's own sweep and a
        ``flush()``'s registry sweep hand in what open spans hold through :meth:`_enqueue`.

        **It is** :meth:`_enqueue` **then** :meth:`_announce` (SPEC-091 FR-001), unchanged to its
        callers. The split exists for the callers that hand in while holding
        ``decorator._sweep_hold``: they enqueue under the hold and announce once it is released,
        because a line written under the hold stalls every span close in the process behind a
        wedged console, where this method's own lines were only ever kept outside ``_lock``.

        This enqueues via ``put_nowait`` and returns immediately without touching the sink.
        When the queue is full it drops this submission and counts it in ``dropped`` rather
        than blocking the caller (FR-001, FR-004), warning on a throttle since overflow is a
        high-rate condition and a line per drop would be its own outage (SPEC-017 FR-005).

        The warning is written outside the lock deliberately: stderr can block on a slow
        reader, and the lock also guards the once-only shutdown flag, so holding it across a
        blocking write would let a wedged console stall the drain path. Lines may therefore
        interleave out of order under concurrency, but the counts they carry are exact.

        A submission arriving after :meth:`shutdown` is still accepted, and counted where it
        can be seen (SPEC-030 FR-001): the worker is retired and nothing will drain the queue,
        so this is total silent loss until something reports it. The check is a single unlocked
        read of a flag that is only ever set, never cleared, which is what keeps the normal path
        free — taking the lock here would put every submission behind the counter's contention
        for a condition that is false in every correct program. SPEC-019's objection to a
        liveness check in ``submit`` does not reach it: that was about probing the thread, this
        is a boolean already in the object's dict.

        **The flag is read again after the put** (SPEC-050 FR-005), which stops the count missing
        a submission — not, despite the temptation to say so, which makes it *exact*.
        ``Health.submitted_after_shutdown`` documents the count as deliberately generous, starting
        when ``shutdown()`` begins rather than when the drain ends, so a submission the final drain
        in fact carried is counted anyway; this read widens that population in the direction that
        field calls correct, and the stderr line's "nothing will drain these" is over-strong for
        exactly the same reason and for exactly as long as it has been. A caller preempted between
        the first read and the ``put_nowait``
        queues its item after the final drain has already run, where nothing will read it — and
        the counter built for exactly that case stayed at zero, so the documented
        ``retired`` + ``submitted_after_shutdown`` pair could not fire. It **closes** the window
        rather than narrowing it: the retirement count moves under the owner's lock at the top of
        :meth:`shutdown`, strictly before the sentinel and before ``_stop`` is set, therefore
        strictly before :meth:`_final_drain`. Any submission that can be stranded has the flag
        already latched by the time this read runs, and a read that still sees ``False`` proves
        the shutdown had not begun when the item was queued — which is not the same as proving the
        drain missed it, and is the whole of the over-report above. This is :meth:`flush`'s post-put
        ``_drain_finished`` check, in the same place for the same race. A submission the queue
        **dropped** returns before it: an item that never joined the queue cannot be stranded in
        it, and counting it in both fields would double-report one loss.

        **That last sentence was not true of the code until SPEC-088 FR-002 item 1** (finding 16):
        the flag was also read *before* the put, and a retired submission was counted there,
        then counted again in ``dropped`` when the put met a full queue — measured, ``p03`` read a
        ``submitted_after_shutdown`` above ``queued`` plus ``dropped``. The flag is now read once,
        after a put that succeeded, which is the read the paragraph above argues closes the
        window; the read before the put added nothing to that argument but the double count. A
        caller can see one consequence: ``submitted_after_shutdown`` reads 0 when every late
        submission overflowed, since none of them was queued. In the event ledger an overflowed
        event is in ``overflow_lost`` and a queued one in ``stranded``, so the two cannot overlap.

        Args:
          events: The span's buffered events, submitted as one item.

        Returns:
          None.

        Raises:
          None.
        """
        self._announce(self._enqueue(events, count_retired=True))

    def _enqueue(self, events: list[dict[str, object]], *, count_retired: bool) -> _Totals:
        """Puts one submission on the queue and counts its overflow or retirement, writing nothing.

        :meth:`submit`'s counting half (SPEC-091 FR-001), in the order :meth:`submit` documents:
        the put; on a full queue, ``dropped`` and ``_overflow_lost`` under ``_lock``; otherwise,
        when ``count_retired`` is set, the single post-put retirement read and its count. Nothing
        after a put that succeeded can raise an ``Exception``: an attribute load, an ``int``
        comparison and a locked increment. So a hand-in that raises raised before its events were
        queued, which is what lets a sweep debit a raising hand-in's take to ``in_span_lost``; an
        asynchronous ``BaseException`` landing after the put is the one residual, counted twice,
        and recorded in ``architecture.md`` §13.

        Args:
          events: The submission, handed over whole.
          count_retired: Whether a put after ``shutdown()`` began is counted in
            ``submitted_after_shutdown``. The drain thread's own hand-in passes ``False``
            (:meth:`_hand_in`).

        Returns:
          The ``dropped`` total this call moved, or 0, and the ``submitted_after_shutdown`` total
          it moved, or 0, for :meth:`_announce`.

        Raises:
          None.
        """
        try:
            self._queue.put_nowait(events)
        except queue.Full:
            with self._lock:
                self.dropped += 1
                self._overflow_lost += len(events)
                total = self.dropped
            return total, 0
        if count_retired and _lifecycle_state.retirements > self._epoch:
            return 0, self._count_undeliverable()
        return 0, 0

    def _announce(self, totals: _Totals) -> None:
        """Writes the throttled lines for the totals :meth:`_enqueue` moved (SPEC-091 FR-001).

        :meth:`submit`'s writing half. Its callers hold no library lock: a hand-in made inside
        ``decorator._sweep_hold`` is announced by its caller once the hold is released. The lines
        and their throttle are the ones :meth:`submit` always wrote: a line at the first count and
        every ``WARN_EVERY`` after, since a caller making either mistake makes it on every call.

        Args:
          totals: The ``dropped`` and ``submitted_after_shutdown`` totals one enqueue moved, each
            0 when it moved nothing.

        Returns:
          None.

        Raises:
          None.
        """
        dropped, late = totals
        if dropped and (dropped == 1 or dropped % _diag.WARN_EVERY == 0):
            _diag.lost("submission", dropped, "log queue full; count is cumulative")
        if late and (late == 1 or late % _diag.WARN_EVERY == 0):
            _diag.lost(
                "submission",
                late,
                "logged after shutdown(), which is terminal; nothing will drain these. Use "
                "flush() in a process that logs again. Count is cumulative",
            )

    def _offer(
        self,
        events: list[dict[str, object]],
        *,
        count_retired: bool,
        wait: float = _SWEEP_WAIT,
        past_capacity: bool = False,
    ) -> int | None:
        """Puts a sweep's take on the queue if there is room, never dropping and never blocking.

        The hand-in every sweep makes inside the sweep hold (SPEC-091, diff review frame 2). It is
        ``queue.Queue.put(block=False)``'s own body, taken apart so that the queue's mutex is
        acquired with a wait of at most ``wait`` seconds: under the mutex it refuses a full queue,
        puts through :meth:`_CountingQueue._put`, so the taken-in count still moves, raises
        ``unfinished_tasks`` and notifies ``not_empty``, exactly as ``put`` does. The mutex is a
        plain ``Lock``, so it cannot be taken first and ``put_nowait`` called inside it. Two
        things make it differ from :meth:`_enqueue`, and each closes a defect frame 2 measured:

        - **It never drops.** A full queue returns :data:`QUEUE_FULL` with nothing put, and the
          drain's and a ``flush()``'s registry sweep put the take back on its span, where the close
          will ship it. Overflowing instead dropped 6,000
          of 60,000 events with 12,000 spans open (``r_many_spans.py``).
        - **It never waits unboundedly for a lock**, because the caller holds the sweep hold. A
          thread inside the queue's section that then takes the hold, as ``_abandon`` or
          ``continue_trace()`` would from a collector callback there, waited on a sweep that waited
          on it; the bound lets the sweep give up and release the hold, and keeps the drain from
          waiting for ever. It does not rescue a span **closed** inside the queue's section: that
          close's own submit then waits on the mutex its thread holds, a plain ``Lock``, as it did
          before SPEC-091 (``r_mutex_cycle.py``, ``architecture.md`` §13). The worker's ``_lock``
          is not taken at all: there is no overflow to count, and the retirement read's count is
          returned for the caller to make through :meth:`_settle_late` once the hold is released.

        **At exit the capacity check is skipped** (diff review, execution frame). The exit drain's
        one sweep passes ``past_capacity=True``: the drain thread is the queue's only consumer, and
        it fixes its bound on what it takes right after this sweep, so a put past ``max_queue``
        there costs at most the open spans' takes once. Refused on a full queue, those takes stayed
        on their spans, and a span the process never closes, a daemon thread's, lost them
        uncounted: 0 of 5 with the queue full at ``shutdown()`` (``exit_full.py``).

        Args:
          events: One span's take, offered whole as one submission.
          count_retired: Whether a put after ``shutdown()`` began is to be counted as late.
          wait: The longest wait for the queue's mutex, in seconds.
          past_capacity: Whether to put whatever the queue holds, which only the exit drain's
            sweep asks for.

        Returns:
          ``None`` when the mutex was not free within ``wait`` and :data:`QUEUE_FULL` when the
          queue had no room, each having put nothing; otherwise 1 when the put is late and
          ``count_retired`` is set, for the caller to count after the hold, and 0 otherwise.

        Raises:
          None.
        """
        q = self._queue
        if not q.mutex.acquire(timeout=wait):
            return None
        try:
            if not past_capacity and 0 < q.maxsize <= q._qsize():
                return QUEUE_FULL
            q._put(events)
            q.unfinished_tasks += 1
            q.not_empty.notify()
        finally:
            q.mutex.release()
        return 1 if count_retired and _lifecycle_state.retirements > self._epoch else 0

    def _count_overflow(self, events: int) -> None:
        """Counts one submission the caller's own ``flush()`` sweep could not queue, after the hold.

        The caller's own sweep keeps SPEC-089's overflow (FR-003 AC-3): its buffer is dropped and
        counted, in ``dropped`` and ``overflow_lost``, as :meth:`_enqueue` counts one. It is counted
        here, with no sweep hold held, so ``_lock`` is never taken under it.

        Args:
          events: The events of the dropped buffer.

        Returns:
          None.

        Raises:
          None.
        """
        with self._lock:
            self.dropped += 1
            self._overflow_lost += events
            total = self.dropped
        self._announce((total, 0))

    def _settle_late(self, late: int) -> None:
        """Counts and announces the late submissions a sweep's offers left, with no hold held.

        Args:
          late: How many of the sweep's puts landed after ``shutdown()`` began.

        Returns:
          None.

        Raises:
          None.
        """
        for _ in range(late):
            self._announce((0, self._count_undeliverable()))

    def _hand_in(self, events: list[dict[str, object]]) -> int | None:
        """Offers a submission the drain thread's own sweep took, with no retirement read.

        The drain thread that hands this submission in is the thread whose exit drain takes it,
        so a ``shutdown()`` never strands it (SPEC-091 FR-002): the exit drain sweeps before it
        fixes its bound. A drain that dies, or an exit that expires first, leaves it in SPEC-088's
        ``stranded`` or ``in_flight``, as any queued submission. Counting it in
        ``submitted_after_shutdown`` would print "nothing will drain these" over events the exit
        drain then delivers.

        Args:
          events: One open span's take.

        Returns:
          What :meth:`_offer` returned: ``None`` when nothing was put, else 0.

        Raises:
          None.
        """
        return self._offer(events, count_retired=False)

    def _hand_in_at_exit(self, events: list[dict[str, object]]) -> int | None:
        """Offers a take the exit drain's one sweep made, past the queue's capacity.

        The exit drain is about to take what the queue holds and nothing reads it afterwards, so a
        full queue there must not leave the open spans' events where no close may come for them
        (:meth:`_offer`). The mutex is still taken with a bounded wait.

        Args:
          events: One open span's take.

        Returns:
          What :meth:`_offer` returned: ``None`` when the mutex was not free in time, else 0.

        Raises:
          None.
        """
        return self._offer(events, count_retired=False, past_capacity=True)

    def _run_sweep(self, *, at_exit: bool = False) -> int | None:
        """Sweeps every open span into this queue from the drain thread, absorbing a fault.

        SPEC-091 FR-002. The sweep is the callable the owner passed as ``sweep=``,
        ``decorator._sweep_registered``, called with :meth:`_hand_in` and a bounded wait for the
        sweep hold, :data:`_SWEEP_WAIT`: the drain is the one thread every other waits on, so a
        thread that holds the hold and then waits on the drain (a collector callback inside a hold
        calling ``flush()``, ``shutdown()`` or a sink swap) would otherwise wait on a drain that
        waits on it. Every late submission and fault the sweep hands back is counted and announced
        here, after the hold is released, never under it. An ``Exception`` from the sweep itself is
        absorbed and announced by type; the events it had not yet taken stay on their spans.

        Args:
          at_exit: Whether this is the exit drain's sweep, which hands in past the queue's
            capacity (:meth:`_hand_in_at_exit`).

        Returns:
          The events handed in, 0 when there is no sweep or it failed, and ``None`` when the
          sweep took nothing because the hold was not free within the wait (or this thread
          already held it).

        Raises:
          None.
        """
        sweep = self._sweep
        if sweep is None:
            return 0
        try:
            hand_in = self._hand_in_at_exit if at_exit else self._hand_in
            result = sweep(hand_in, wait=_SWEEP_WAIT)
        except Exception as exc:
            _diag.absorbed("sweeping open spans", exc, "events not yet taken stay on their spans")
            return 0
        if result is None:
            return None
        handed, late, faults = result
        self._settle_late(late)
        for fault in faults:
            _diag.absorbed("handing in an open span's events", fault, "the events taken were lost")
        return handed

    def _count_undeliverable(self) -> int:
        """Counts a post-shutdown submission under the lock, writing nothing.

        The count's line is :meth:`_announce`'s now (SPEC-091 FR-001): it moved out so that a
        hand-in made inside the sweep hold writes nothing there. It keeps the throttle it had,
        and the counter still moves before the line is attempted, per ``_diag``'s record-first
        rule.

        Args:
          None.

        Returns:
          The new ``submitted_after_shutdown`` total.

        Raises:
          None.
        """
        with self._lock:
            self.submitted_after_shutdown += 1
            return self.submitted_after_shutdown

    @property
    def draining(self) -> bool:
        """Whether this worker's own ``_stop`` is still the sink's route to a cut-short backoff.

        Neither ``retired`` nor a bare ``_worker is not None`` answers this, and the two specs
        that got it wrong disagreed in opposite directions (SPEC-035 FR-001). SPEC-033 requires a
        sink still being written to *after* a shutdown to be handed a **fresh** event, because
        ``_stop`` is set forever and an ``Event`` never clears, so every later backoff would
        collapse to zero. SPEC-035 requires a sink whose drain is **in flight** to keep the event
        that drain is about to wait on, or the shutdown serves a full backoff and expires. Both
        are the same sink held by the same worker; only the moment differs, so the predicate has
        to be the moment.

        An abandoned drain counts as not draining. The thread is wedged and the shutdown has
        already given up on it (:meth:`~log_foundry._lifecycle.Runtime.held` and SPEC-027
        FR-004 leave the sink open
        for that reason), so nothing further will cut its backoff — where SPEC-033's tight retry
        loop would go on costing the still-running application every emit.

        It reads **one** event, ``_drain_settled``, set both where the loop stops and where a
        shutdown gives up on it — not a conjunction of the two facts. A reader that tested them
        separately would be correct only at the instant it looked: :meth:`shutdown`'s idempotent
        path evaluates this and *then* waits, so an abandonment landing in between has to release
        that wait, and only an event already being waited on can do that. Measured before the
        change, with the first caller expiring while the second was inside its wait: a second
        ``shutdown(timeout=20)`` returned after 20.01 s, and with ``timeout=None`` the process
        never exited at all.

        ``_drain_finished`` is deliberately left alone rather than widened: :meth:`flush` and
        :meth:`shutdown`'s sentinel gate both read it as "the loop stopped reading the queue",
        which an abandoned — and still running — drain has not done. The two events stay
        distinguishable without a third flag, which is why no ``_drain_abandoned`` is kept:
        abandoned is ``_drain_settled`` set with ``_drain_finished`` unset, and the operator-facing
        form of the same fact is ``stopped_reason == "ShutdownTimeout"``.

        Read without the lock, as the retirement count is: the event is set once and never cleared, so a
        racing reader sees one of two answers and both are momentarily true.

        Args:
          None.

        Returns:
          Whether the drain loop is still running and has not been abandoned.

        Raises:
          None.
        """
        return not self._drain_settled.is_set()

    def health(self) -> Health:
        """Snapshots this worker's own counters, and nothing else (SPEC-054 FR-005).

        The fields a *process* answers — ``retired``, ``sink``, ``closing_sinks``,
        ``inherited_sink`` and the two loss counters — are left at their defaults and filled by
        ``_lifecycle._worker_health``, which is the one place a ``Health`` is assembled. Two
        assemblers is what let ``sink`` be filled on the worker path only.

        This stays valid after :meth:`shutdown`: the counters are plain integers that outlive
        the thread, and the final drain consumes the queue. ``queued`` therefore reads 0 for a
        worker nothing logged to afterwards — but not always: submissions accepted after the
        shutdown stay queued on purpose (SPEC-030), and a ``flush()`` marker stranded by
        racing it is answered and then counted, as ``Health.queued`` records. The same applies
        to ``stopped_reason``, since a caller finding a dead
        worker will usually call ``shutdown()`` next. Reading it after a shutdown is in fact
        the point of ``retired`` and ``submitted_after_shutdown`` (SPEC-030 FR-001), which
        report a state only a retired worker can be in.

        Args:
          None.

        Returns:
          The counter half of a snapshot; the rest is the lifecycle owner's to fill.

        Raises:
          None.
        """
        with self._lock:
            dropped, failed_batches = self.dropped, self.failed_batches
            stopped_reason = self.stopped_reason
            submitted_after_shutdown = self.submitted_after_shutdown
            incomplete_swaps = self.incomplete_swaps
        return Health(
            queued=self._queue.qsize(),
            dropped=dropped,
            failed_batches=failed_batches,
            stopped_reason=stopped_reason,
            submitted_after_shutdown=submitted_after_shutdown,
            incomplete_swaps=incomplete_swaps,
        )

    def _sink_losses(self) -> SinkLosses | None:
        """Reads the configured sink's optional ``losses()`` (FR-003).

        The probe and its guarantees live in ``sinks.base.read_losses``, imported here rather
        than at module scope, which keeps ``worker`` free of a runtime dependency on ``sinks``
        the same way ``config`` does.

        Args:
          None.

        Returns:
          The sink's losses, or ``None`` when it reports none.

        Raises:
          None.
        """
        from log_foundry.sinks.base import read_losses

        return read_losses(self.sink)

    def flush(self, timeout: float | None = 5.0) -> FlushResult:
        """Drains everything submitted before this call through the sink, without stopping.

        The verdict alone, from a marker that asks the drain for no ledger snapshot (SPEC-089
        FR-003). :meth:`retarget` calls this, so a swap neither snapshots nor waits on a dying
        drain; ``log_foundry.flush()`` calls :meth:`_flush_marked` directly for its counts.
        :meth:`_flush_marked` carries the mechanism and its reasoning.

        Args:
          timeout: Seconds bounding the whole call. ``None`` waits indefinitely.

        Returns:
          The :class:`FlushResult` :meth:`_flush_marked` reaches, which carries no counts.

        Raises:
          None.
        """
        return self._flush_marked(timeout, counted=False)[0]

    def _flush_marked(
        self, timeout: float | None, *, counted: bool
    ) -> tuple[FlushResult, _FlushMarker | None]:
        """Drains everything submitted before this call, returning the verdict and its marker.

        The precise claim is that nothing was abandoned while this call was outstanding: the
        batch this flush forces, and any batch another flush or a batching trigger emitted
        while its marker waited its turn. A batch abandoned before the call is not in scope —
        that loss is already in ``failed_batches`` and on stderr, and counting it here would
        make every later empty flush report a failure it did not incur.

        Unlike :meth:`shutdown` the thread keeps running, the sink is not closed, and the
        once-only shutdown flag is untouched, so logging continues normally afterwards
        (SPEC-013 FR-002). The put is blocking rather than ``put_nowait``, because on a full
        queue ``put_nowait`` would skip the flush and return as though it had succeeded, the
        one outcome a flush must never produce silently.

        The drain's completion is re-checked **after** the put, and that second look is what
        makes a ``timeout=None`` call safe. The checks above can both pass microseconds before
        the drain finishes, leaving this marker queued behind something that will never read it
        — a bounded caller then waits out its timeout, which SPEC-021 accepts as correct either
        way, but an unbounded one waits forever.

        It tests ``_drain_finished`` and not only ``is_alive()``, because the two are not the
        same instant and the gap between them is where the hang survives: the terminal-failure
        path sweeps for markers and *then* returns, so a marker queued after that sweep sits
        behind a thread still reading as alive. The flag is set **before** the sweep, and a
        ``put`` and the sweep's snapshot both take the queue's own mutex, so a marker either
        lands before the snapshot and is answered, or lands after it and finds the flag set.

        **``_drain_settled`` is the third flag, and an expired ``shutdown()`` sets only that
        one** (SPEC-050 FR-001). On that path the drain is still alive and still inside ``emit``,
        so ``_drain_finished`` is clear and ``is_alive()`` is true — a marker landing after that
        sweep found every existing condition false and waited on a drain the process had already
        given up on. Measured: the flusher still waiting three seconds later, released only by
        re-running the sweep by hand. ``timeout=None`` makes it permanent, and in an application
        that is a non-daemon thread the interpreter joins at exit, so the process does not exit.
        ``_drain_settled`` set with ``_drain_finished`` clear is *uniquely* that branch — every
        other setter sets both, and :meth:`_run` sets ``_drain_finished`` **first**, so a true
        read of settled implies finished was already set — which is what lets this report
        ``"abandoned"`` rather than ``"thread-died"`` for a thread that is demonstrably alive.
        Inverting that pair in :meth:`_run` survives the whole suite and would produce the
        mislabel; it is held by :meth:`_run`'s docstring naming the order rather than by a test,
        because the consequence is a wrong *reason* on two falsy results and not a wrong verdict.

        **The delivered test comes first, and a test holds it there.** A marker the drain answered
        ``delivered=True`` between the sweep and ``_drain_finished`` being set must report
        ``ok=True``, not ``"abandoned"`` — the drain adjudicated it, and a false negative here is
        what makes :meth:`retarget` count an ``incomplete_swaps`` and write a loss line for a swap
        that completed. A wrong verdict, not a wrong reason. An earlier draft of this docstring
        called the window unreachable without interposing inside this method; that was wrong twice
        over — parking after the put reaches it, and holding ``_release_marker`` widens it — and a
        reviewer built both.

        Reporting is by the **marker**, never by the check alone. A drain that answered this
        marker and then exited has delivered, and saying otherwise would be a false failure —
        one ``retarget`` reads as an unconfirmed drain, counting ``incomplete_swaps``, leaving
        the previous sink open and writing a loss line for a swap that in fact completed.

        **The marker is returned beside the verdict** (SPEC-089 FR-003), so the caller of a
        counted flush can take the end of its window from it: the ledger the drain wrote when it
        answered, or, when it carries none or was answered while unwinding, a read of its own.
        The eleven verdict constructions below carry no counts; ``_lifecycle._with_counts``
        attaches them, on the calling thread, so this method never waits on a dying drain and a
        swap's timing is unchanged.

        Args:
          timeout: Seconds bounding the whole call — one deadline shared by the put and the
            wait, so the two cannot add up to twice the timeout. ``None`` waits indefinitely.
          counted: Whether the marker asks its answering drain for a ledger snapshot.

        Returns:
          The verdict and the marker this call queued, or ``None`` in place of the marker when it
          returned before queueing one. The verdict is a :class:`FlushResult`, truthy once the
          drain that carried the events finished and abandoned no batch while the call was
          outstanding, and otherwise falsy with a ``reason``: ``"timed-out"``, ``"retired"``,
          ``"thread-died"``, ``"queue-full"``, or ``"abandoned"``. That last one has **four** producers, not one:
          the drain carrying those events gave up after exhausting retries (SPEC-021 FR-001,
          the original — and it used to return True, a false success exactly where ``flush()``
          matters most); an expiring ``shutdown()`` answered the marker pessimistically rather
          than leave it on a drain it had abandoned (SPEC-050 FR-001); this call's own marker
          arrived after that had already happened; or it never reached the queue, because that
          was full and the drain had already been given up on. Only the first says the drain
          adjudicated the batch, so ``"abandoned"`` means "not confirmed delivered", never
          "confirmed lost". ``ok=True`` means the drain answered and no batch was abandoned in
          the window: ``delivered`` starts ``False`` and is written only by the drain, only after
          its emit returned. A settled run's lost events do not make it falsy (SPEC-089); they
          are counted on the result ``log_foundry.flush()`` returns. **The inner call carries the
          type too, not only the public ``log_foundry.flush``** (SPEC-034 FR-007 AC-1b): the
          five outcomes are distinguishable only here, so a public wrapper over a bare ``bool``
          could name none of them without guessing.

        Raises:
          None.
        """
        if _lifecycle_state.retirements > self._epoch:
            return FlushResult(ok=False, reason="retired"), None
        if not self._thread.is_alive():
            return FlushResult(ok=False, reason="thread-died"), None
        with self._lock:
            marker = _FlushMarker(self.failed_batches, counted=counted)
        deadline = None if timeout is None else time.monotonic() + timeout
        if not self._put_marker(marker, deadline):
            if self._given_up():
                return FlushResult(ok=False, reason="abandoned"), None
            if not self._thread.is_alive() or self._drain_finished.is_set():
                return FlushResult(ok=False, reason="thread-died"), None
            return FlushResult(ok=False, reason="queue-full"), None
        given_up = self._given_up()
        if self._settled():
            answered = marker.event.is_set()
            if answered and marker.delivered:
                return FlushResult(ok=True), marker
            if answered or given_up:
                return FlushResult(ok=False, reason="abandoned"), marker
            return FlushResult(ok=False, reason="thread-died"), marker
        remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
        if not marker.event.wait(remaining):
            return FlushResult(ok=False, reason="timed-out"), marker
        if not marker.delivered:
            return FlushResult(ok=False, reason="abandoned"), marker
        return FlushResult(ok=True), marker

    def _given_up(self) -> bool:
        """Whether a bounded ``shutdown()`` expired on a drain that is still running.

        ``_drain_settled`` set with ``_drain_finished`` clear is **uniquely** that branch: every
        other setter sets both, and :meth:`_run` sets ``_drain_finished`` first, so a true read of
        settled implies finished was already set. That ordering is what makes this predicate
        precise rather than merely suggestive, and :meth:`_run` names it in turn.

        Args:
          None.

        Returns:
          Whether the drain has been abandoned while still alive.

        Raises:
          None.
        """
        return self._drain_settled.is_set() and not self._drain_finished.is_set()

    def _settled(self) -> bool:
        """Whether nothing will drain this queue again, for any of the three reasons.

        The disjunction :meth:`flush`'s post-put re-check tests, named once so the pre-put wait
        and the post-put check cannot drift apart: the drain finished, a bounded ``shutdown()``
        gave up on it, or the thread is gone. :meth:`_given_up` is the narrower question of *which*
        of those it was, which only the reason mapping needs.

        Args:
          None.

        Returns:
          Whether the drain has stopped, been abandoned, or died.

        Raises:
          None.
        """
        return self._drain_finished.is_set() or self._given_up() or not self._thread.is_alive()

    def _put_marker(self, marker: _FlushMarker, deadline: float | None) -> bool:
        """Queues a flush marker, giving up if the drain is abandoned while the queue is full.

        A plain ``put`` with ``timeout=None`` blocks until the queue has room, and on a full queue
        behind a permanently wedged sink there is never any — so the caller waited forever one
        line *before* the post-put re-check that exists to prevent exactly that. Measured at
        process level: a non-daemon flusher parked in ``Queue.put`` for the whole run, and an
        interpreter that could not exit because it joins that thread. The re-check cannot help,
        because ``_release_waiters`` can only answer a marker that is already in the queue.

        So the wait is taken in slices and :meth:`_settled` is consulted between them — the
        re-check's own disjunction, not merely :meth:`_given_up`, because a *terminally dead*
        drain sets both flags and would leave an unbounded caller parked forever on the narrower
        test. The deadline is consulted **after** a put has been attempted, never before: a
        ``flush(timeout=0)`` is "enqueue and do not wait", and testing the deadline first turned
        it into a call that never enqueued at all and reported backpressure that did not exist —
        which is the one outcome SPEC-034 FR-007 named ``reason`` to tell apart. A bounded caller
        is otherwise unaffected: the deadline still ends it, with the same ``"queue-full"``.
        The slice is a polling granularity on a queue that is *already* full, which is a degraded
        state the caller is being told about either way, and it bounds only the slice — a negative
        or zero remainder clamps to an immediate attempt rather than raising, where ``Queue.put``
        rejects a negative timeout before it even looks at capacity. That makes the **put** total,
        which is what ``Raises: None`` said all along. That raise was never *public*, on two separate
        counts: ``Worker`` is not exported, so this is not a public call, and
        ``_lifecycle._flush_worker``'s bare ``except Exception`` caught that ``ValueError`` and reported
        ``"thread-died"`` on a live thread with ``stopped_reason`` at ``None``, so what a caller
        sees change is the token, not an exception. The *wait* below is unchanged and a timeout
        large enough to overflow ``time_t`` still raises out of **this** method, on this tree and
        every earlier one — and is caught by that same ``except Exception`` before a public caller
        sees it, exactly as the ``ValueError`` was. The distinction drawn above holds there too.

        Args:
          marker: The marker to enqueue.
          deadline: The caller's monotonic deadline, or ``None`` for an unbounded caller.

        Returns:
          Whether the marker reached the queue.

        Raises:
          None.
        """
        while True:
            slice_seconds = _PUT_POLL_SECONDS
            if deadline is not None:
                slice_seconds = min(slice_seconds, max(0.0, deadline - time.monotonic()))
            try:
                self._queue.put(marker, timeout=slice_seconds)
            except queue.Full:
                if self._settled() or (deadline is not None and time.monotonic() >= deadline):
                    return False
            else:
                return True

    def retarget(self, new_sink: Sink, deadline: float | None = None) -> SwapOutcome:
        """Drains into the current sink, reassigns, and fences the drain (SPEC-054 FR-003).

        What ``swap_sink`` was, minus the close: the owner performs every close now, from one
        record (FR-002), so this reports what it did and hands the previous sink back rather than
        releasing it itself. Reporting through a **return value** rather than a predicate is
        SPEC-035 FR-003's decision, kept: the decline is taken between two lock acquisitions and
        nothing outside can observe it.

        **Three verdicts, and the owner does something different with each.** ``declined`` — this
        worker retired mid-swap, so it keeps its sink forever (SPEC-033 FR-002) and the owner owes
        both a close; or it retired after the reassign, holding ``new_sink``, which the shutdown
        that retired it drained into. ``fenced`` — a ``flush()`` after the reassign confirmed the
        drain reached the new sink, so nothing can still be inside the old one and the owner may
        release it.
        ``unfenced`` — that confirmation did not arrive within the budget, so the old sink stays
        among the ones this thread may be inside and a later ``shutdown()`` that finds the thread
        ended closes it (SPEC-050 FR-004).

        The retirement check is re-taken **after** the drain as well as before it. A ``shutdown()``
        landing in between would otherwise install a sink nothing will ever close, and report an
        ``incomplete_swaps`` whose line says the old sink was left open when it was in fact closed.

        **And a third time, after the reassign, through the fence flush's own verdict and the
        owed record.** A ``shutdown()`` that retires this worker once it holds ``new_sink`` — its
        setter's own ``shutdown()``, run by the offer above, or one on another thread landing
        before the fence flush reads the count — makes that flush answer ``retired`` at its first
        line, without trying. The verdict then turns on the fact the unconfirmed-swap line
        reports: whether the previous sink is **still owed**, read after that flush. Not owed, the
        retiring shutdown took it — a take happens only once nothing of the drain can be inside
        the sink, and the taker performs the close — so the verdict is ``declined``, with no
        ``incomplete_swaps`` and no line. The sink asked about is this worker's own, which no
        swap cedes (a cede is of a kept sink in the section that arms its holder, after this
        returns) and which the closer takes only once the drain thread has ended; its guard
        passes a take over, and puts the sink back, only for a sink lying beneath one still held
        (SPEC-087). Read as ``unfenced``, as it was until this fix, a setter
        calling ``shutdown()`` counted an incomplete swap and wrote that the previous sink "could
        not be confirmed drained" and was "left open", though the shutdown had already closed it.
        **Still owed, it stays** ``unfenced``, line and count as before: the retiring shutdown has
        not taken it yet (one on another thread waits for this swap's decision first), or it
        expired with the drain thread inside the sink and left it open, which a later
        ``shutdown()`` closes once it finds the thread ended — the line is true there. The drain
        thread's liveness was the first rule and is the wrong fact: read after the setter
        returns, it found a thread that had left the old sink only after the shutdown that should
        have closed it had expired, and reported ``declined`` for a sink left owed and open. The
        reason is read rather than a sixth ``retirements > self._epoch``, which
        :meth:`~log_foundry._lifecycle.Runtime.live_worker` counts; ``flush`` returns ``retired``
        from that comparison and nowhere else. ``old`` stays among the sinks this thread may be
        inside on every verdict but ``fenced``.

        **The new sink is handed this worker's signal after the reassign, through**
        :func:`_lifecycle._hand_retargeted`, **which defers into a stop's close.** A swap during
        a ``shutdown()`` can retarget a late or tail worker onto a sink that stop is closing, and
        that close is already waiting on the signal the stop set; handing it this worker's unset
        ``_stop`` made it wait out the sink's whole backoff — measured at ``a8e91fac``, 4.01 s for
        a ``shutdown(timeout=1)``. So while a stop is running and a close of the sink is in
        flight, the hand marks that close and is paid when it ends, as a worker build's is.
        Outside a stop the hand is made at once, here, with no library lock held but
        ``config._sink_lock``, as it always was.

        **A first drain that failed stays** ``unfenced`` **even when that shutdown lands after the
        reassign**, deliberately. That drain really was unconfirmed, and the verdict reports the
        drain. Its line then says the previous sink is "left open" although a shutdown already
        run may have closed it, which is a false clause left in place; reading the retirement
        there too would bury the failed drain under the shutdown's verdict.

        The unconfirmed-swap diagnostic reports the budget the drain was **given**, the remainder
        of the deadline when this method read it, not what was left after the flush: once that
        flush has timed out the remainder is always ~0, which would say nothing about what the
        drain was allowed. The budget given is the caller's budget less the time between the
        deadline being set and this method reading the clock, whatever spent it. The line reads
        "within 0s", truthfully, whenever nothing was left by that read: the caller's budget was
        already spent upstream (a ``configure()`` whose wait for ``config._sink_lock`` used it up
        passes ``0.0``), that time used up the rest, or the caller passed none on purpose, as a
        ``configure()`` re-entered from a sink's own code does. ``config._change_sink`` writes a
        line of its own when it gave up waiting for that lock and when it is re-entered.

        **The reassign bumps** ``_sink_generation`` **in the same locked section** (SPEC-090
        FR-001 rule 5). :meth:`_emit` reads ``self.sink`` for the engine and again on every 1.x
        attempt, so a batch can straddle the swap; :meth:`_emit_pending` reads the generation
        before and after its emit and discards a measurement taken across a bump, and
        :meth:`_size_target` reads a target measured under an older generation as zero, so the
        first batch a new sink is handed under a backlog is the count trigger's. An ``int``
        rather than the sink, so nothing pins a sink this swap replaced.

        Args:
          new_sink: The sink to deliver to from now on.
          deadline: The ``time.monotonic()`` instant the caller's budget expires, or ``None``.

        Returns:
          What it did, and the sink it displaced.

        Raises:
          BaseException: A ``KeyboardInterrupt`` or ``SystemExit`` from the new sink's own
            ``log_foundry_stop_signal`` setter, while it is handed this worker's signal or while a
            mark that hand made is paid (``_lifecycle._hand_retargeted``);
            ``_lifecycle.offer_stop_signal`` absorbs an ``Exception`` from it.
        """
        with self._lock:
            if _lifecycle_state.retirements > self._epoch:
                return SwapOutcome(verdict="declined", previous=self.sink)
            if self.sink is new_sink:
                return SwapOutcome(verdict="fenced", previous=new_sink)
        timeout = None if deadline is None else max(0.0, deadline - time.monotonic())
        drained = self.flush(timeout)
        with self._lock:
            if _lifecycle_state.retirements > self._epoch:
                return SwapOutcome(verdict="declined", previous=self.sink)
            old = self.sink
            if old is new_sink:
                return SwapOutcome(verdict="fenced", previous=new_sink)
            self.sink = new_sink
            self._sink_generation += 1
            self._held = [held for held in self._held if held is not new_sink] + [new_sink]
        _lifecycle._hand_retargeted(new_sink, self._stop)
        remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
        if not drained:
            self._record_incomplete_swap(timeout, old)
            return SwapOutcome(verdict="unfenced", previous=old)
        fence = self.flush(remaining)
        if not fence and fence.reason == "retired" and not _lifecycle.owes_close(old):
            return SwapOutcome(verdict="declined", previous=old)
        if not fence:
            self._record_incomplete_swap(timeout, old)
            return SwapOutcome(verdict="unfenced", previous=old)
        with self._lock:
            self._held = [held for held in self._held if held is not old]
        return SwapOutcome(verdict="fenced", previous=old)

    def may_be_inside(self, sink: Sink) -> bool:
        """Whether this worker's thread may still be inside that sink (SPEC-054 FR-003, FR-004).

        The worker's half of :meth:`~log_foundry._lifecycle.Runtime.held`, and one of the two
        answers about its own thread that only it can give. True while the thread is **alive** and
        the sink is one it holds: its live sink, or a sink swapped out without a confirmed fence.

        **Liveness, not** ``draining``. After an expired ``shutdown()`` the drain is marked settled
        while the thread is still inside ``emit``, and the closer must treat that as still inside
        so the sink is left open (SPEC-027 FR-004) — the opposite answer from the one the signal
        refresh needs in the same state, which is why the moment is two predicates rather than one.

        Takes no lock, which is arch §9.2's rule for a question. ``_held`` is **rebound** rather
        than mutated in place, so a read is one atomic reference load and the list this iterates
        cannot change under it — the same property the deleted ``worker_owns_now`` relied on.

        Args:
          sink: The sink the closer is deciding whether to take.

        Returns:
          Whether a live thread of this worker's may be inside it.

        Raises:
          None.
        """
        if not self._thread.is_alive():
            return False
        return any(held is sink for held in self._held)

    def holds_unfenced(self, sink: Sink) -> bool:
        """Whether this sink was swapped out of this worker without a confirmed fence (FR-003).

        Asked at the closer's take, about a sink it is *about* to close, so the thread has already
        ended — :meth:`may_be_inside` would have kept it out otherwise. What it decides is
        **how**: such a sink is released detached and granted only the closer grace, keeping
        SPEC-050 FR-004's judgement that it already had the swap's whole budget and is far more
        likely stuck than slow, so joining it would let one stuck sink hold the exit.

        Takes no lock, for :meth:`may_be_inside`'s reason.

        Args:
          sink: The sink the closer took.

        Returns:
          Whether this worker holds it as an unfenced swap rather than as its live sink.

        Raises:
          None.
        """
        return sink is not self.sink and any(held is sink for held in self._held)

    def _record_incomplete_swap(self, timeout: float | None, sink: Sink) -> None:
        """Records a swap whose drain could not be confirmed, then announces it (FR-003).

        Two things are reported at once because they have one cause: queued submissions may be
        carried to the new sink rather than the old one, and the old sink is not closed here. The
        line is a :func:`~log_foundry._diag.pending` one, never a loss (SPEC-068 FR-002): those
        submissions sit on a **live** drain, may well be delivered to the new sink, and are
        counted by :meth:`_emit` if they are later abandoned — so a ``lost`` line here announced
        them a second time. The count is :meth:`_queued_submissions`, which leaves out this
        swap's own unanswered flush marker, and the clause is dropped when it is zero. It counts
        the queue and not the previous sink's buffer deliberately: the queued submissions are
        the ones that may be misrouted. A probe read it as zero while nine events sat in that
        sink's client buffer, and both numbers were right about different things.

        The wording is the shortest that still says both things, because the whole line must fit
        ``_diag``'s detail bound. The line it replaced was 214 characters at a 4-character budget
        and shipped cut off mid-word; this one fits at a 14-character budget, the longest
        ``_bounded_seconds`` renders for a finite float, with a six-digit count. The substrings
        ``could not be confirmed drained`` and ``left open`` are kept, which ``configure()``'s
        tests read.

        **The sink is also recorded, not merely announced** (SPEC-050 FR-004). It is owed a close
        that cannot be performed now — the drain thread may still be inside its ``emit``, which is
        SPEC-027 FR-004's reasoning — but that objection expires when the thread does, so
        ``_lifecycle._close_owed`` performs it once ``is_alive()`` reads ``False``. Recorded by
        identity, at most once. That dedup is defensive rather than reached: the same object can
        only be ``old`` twice if it was re-adopted as ``self.sink`` in between, and every
        re-adoption prunes it — so under the documented single-threaded ``configure()`` contract
        it owes one close for one other reason as well.

        Args:
          timeout: The budget the drain was given, rendered for the line.
          sink: The previous sink, left open now and owed a close at shutdown.

        Returns:
          None.

        Raises:
          None.
        """
        with self._lock:
            self.incomplete_swaps += 1
        queued = self._queued_submissions()
        detail = (
            f"the previous sink could not be confirmed drained within "
            f"{_bounded_seconds(timeout)}; it is left open until a shutdown() finds the drain "
            f"thread ended"
        )
        if queued:
            detail += f"; {queued} submission(s) still queued, which may reach the new sink"
        _diag.pending(detail)

    def claim_stop(self, deadline: float | None) -> bool:
        """Wins or loses the once-only stop gate, recording the winner's deadline (SPEC-062).

        The gate and the record are **one decision under the lock**, and that is the whole
        point of the method. A first draft recorded the deadline in one critical section and
        decided ``_stopped`` in another, and under two concurrent ``shutdown()`` calls with
        timeouts 0.9 and 1.8 the drain was measured declining against 1.8 while the 0.9 caller
        did the join: that join expired, the 1.8 caller returned as a second caller, and the
        sink was never closed. Here whoever sets ``_stopped`` is whoever's deadline is written,
        so the drain always declines against the deadline the joining caller is holding, and a
        later caller's deadline is refused along with its claim, which is what makes the record
        write-once across concurrent stoppers — save for the one handover below, which
        ``_join_owed`` exists to allow. Only :meth:`_reinit_after_fork` writes it otherwise,
        clearing it for a child that resumes.

        **One exception: a join is owed** (SPEC-068 FR-006). A sink's ``emit`` can call
        ``shutdown()``, and then the caller is the drain thread, which cannot join itself. When
        that caller wins, the same critical section records ``_join_owed``, and :meth:`stop`
        returns without joining. The next caller **not** on the drain thread then takes the gate
        over here: it clears ``_join_owed``, records **its own** deadline in place of the
        drain-thread caller's, and returns ``True``, so it does the first caller's whole work —
        the join, and on expiry the line. That keeps SPEC-062's rule that the caller whose timeout
        the join uses is the caller whose deadline the drain declines against, because the
        drain-thread caller never joins. It does not re-open a retry a joining caller's deadline
        declined, because no join has run yet; and it happens once, because the flag is cleared
        in the decision that takes it, so every later caller loses as before. Without it, an emit
        that wedged after the drain-thread call was announced by nobody, and every later caller,
        ``atexit`` included, spent its whole budget waiting in silence — measured as a 30.12 s
        exit with no line and no close. A losing caller on the drain thread never takes the owed
        join over: it is the one thread that cannot perform it.

        **The verdict stays a** ``bool``, because both callers — :meth:`stop` and
        :func:`_lifecycle._shutdown_worker`, which passes it on as ``claimed`` — only need "do
        the first caller's work". The one difference in that work is recorded beside it instead:
        the takeover sets ``_join_taken_over`` in the same critical section, and :meth:`stop`
        reads it to skip the sentinel, which the drain-thread winner already queued. A second one
        is not harmless. Taken over while :meth:`_final_drain`'s emit is running, it lands after
        the bound that drain fixed when it began (SPEC-090 FR-002), is never consumed, and
        ``health().queued`` reads 1 for the life of the process with ``submitted_after_shutdown``
        at 0 — measured in 33 of 300 unforced runs. The drain needs no wake-up from it either:
        the drain-thread winner has set the stop event, and the drain loop tests that as soon as
        the emit it is inside returns.

        Whether this caller is the drain thread is read **before** the lock, so the lock is still
        held across no call. In a forked child, ``self._thread`` names the inherited dead thread
        for an instant after the new one starts; :meth:`_reinit_after_fork` says why no caller
        can be on the new thread inside that instant.

        It takes only ``_lock`` and calls nothing while holding it, so it is a leaf in the lock
        order: :func:`_lifecycle._shutdown_worker` calls it under ``_state._lock`` and the
        nesting ``_state._lock`` then ``Worker._lock`` is the order ``Runtime``'s docstring
        states, with this lock and the close registry's leaf lock both leaves.

        Args:
          deadline: The caller's deadline as a ``time.monotonic()`` instant, or ``None`` for
            an unbounded stop.

        Returns:
          Whether this caller won the gate or took an owed join over. ``True`` means it must do
          the first caller's work in :meth:`stop`; ``False`` means a stop is already under way
          and its deadline stands.

        Raises:
          None.
        """
        on_drain_thread = threading.current_thread() is self._thread
        with self._lock:
            if self._stopped:
                if not self._join_owed or on_drain_thread:
                    return False
                self._join_owed = False
                self._join_taken_over = True
                self._exit_deadline = deadline
                return True
            self._stopped = True
            self._exit_deadline = deadline
            self._join_owed = on_drain_thread
            return True

    def stop(
        self, timeout: float | None = DEFAULT_SHUTDOWN_TIMEOUT, *, claimed: bool | None = None
    ) -> None:
        """Ends this worker's drain thread, and nothing else (SPEC-054 FR-003).

        What remains of ``shutdown()`` once the closes moved to the lifecycle owner: set the event
        it holds, queue the sentinel, join, and on expiry record ``ShutdownTimeout`` and release
        whoever was waiting on a flush marker. Whether the process is **retired** is not this
        worker's to say and never was on both paths at once — the owner moved the count before it
        called this (FR-001).

        **It sets the event it holds, which is not always the owner's current one.** A worker built
        during a ``shutdown()`` refreshed the signal at its build, for the reason a sink adopted
        after one does, so the owner's ``set`` at the top of the call never reaches it. Measured
        without this: a 3 s backoff bounded the stop at 3.01 s, against 0.06 s with it.
        :func:`_lifecycle._shutdown_worker` now sets that event itself, in the section that
        retires such a worker, before its take; this ``set`` is then a no-op there and still the
        one a direct caller relies on.

        **The once-only gate is** ``_stopped``, **decided under the lock by** :meth:`claim_stop`,
        which records the winner's deadline in the same decision (SPEC-062 FR-001). A second
        caller waits for the drain to settle within its own budget rather than joining again:
        today's shape, kept, and it is not the retirement latch FR-001 deleted — "this worker's
        stop has been entered" is a fact about one thread, with no counterpart on the orphan path,
        which is why it stays here beside :attr:`draining` and :meth:`may_be_inside`. Gating on
        ``_drain_settled`` instead was tried and is wrong: two concurrent first callers both see it
        clear, both run the expiry branch, and a wedged sink produces two "shutdown timed out"
        lines with ``queued`` counted twice.

        **On the drain thread itself it never joins or waits on that thread** (SPEC-068 FR-006).
        A bad ``timeout`` never reaches this far: the public ``shutdown()`` refuses it at entry
        (SPEC-066), before anything is claimed, on the drain thread as anywhere — invariant 13's
        refusal where the argument is written. A sink's ``emit`` that calls ``shutdown()`` makes the
        drain thread the caller, and at ``fda134a`` a first caller there raised ``RuntimeError`` out
        of ``Thread.join`` while a losing one waited on ``_drain_settled``, which only this same
        thread's exit or another caller's expiry can set. As the first caller it queues the sentinel
        unless the drain has finished, sets the stop event and returns: :meth:`claim_stop` has
        recorded that a join is owed, and the next caller off this thread takes it over — doing the
        first caller's work except the sentinel, which is already queued and a second copy of which
        can strand behind the final drain (``_join_taken_over``, :meth:`claim_stop`). It writes no
        expiry line and sets no ``stopped_reason``, because nothing has timed out, and it calls no
        :meth:`_release_waiters`, because this thread's own exit sweeps the markers in
        :meth:`_run`'s ``finally``. As a losing caller it returns without waiting on
        ``_drain_settled``. What the rest of ``shutdown()`` does on that thread — the closer and its
        grace — is the lifecycle owner's and unchanged. The drain ends once the current emit
        returns, through :meth:`_drain`'s stop check and :meth:`_final_drain`, as for any stop.

        **The claim is made before any stop event this worker holds is set**, on both routes.
        :func:`_lifecycle._shutdown_worker` claims for a live worker inside the critical section
        that moves the retirement count, before it sets ``_state._stop`` — which is this worker's
        own event — and passes the verdict in as ``claimed``; it claims a worker built since its
        entry the same way, in the section that retires that worker, before setting the event the
        worker holds. A direct caller passes nothing and claims here, before ``self._stop.set()``.
        Either way a backoff cut
        short by the stop wakes to find the deadline already recorded. Each caller joins or waits
        against **its own** budget, never another caller's: a second caller is bounded by the
        budget it asked for whichever deadline the drain is declining against.

        **The caller who won the claim joins until the deadline the claim recorded**
        (SPEC-078 FR-005), ``_exit_deadline``, rather than until a deadline computed here from
        ``timeout``. For :func:`_lifecycle._shutdown_worker` that recorded deadline is its
        operation's, fixed once when the call began, so every slow step of a shutdown reads what
        remains of one instant and the join cannot end later than the deadline the drain declines
        against. ``timeout`` is then only what the expiry line renders — the budget the caller
        asked for, as given, so the line still reads ``shutdown timed out after 0.5s`` rather than
        whatever remainder was left. A direct caller passes nothing, claims here with ``now +
        timeout``, and joins until that, as before; a losing caller waits its own ``now +
        timeout``, since the recorded deadline is the winner's.

        An expired join leaves the sink **open**, because the thread is still inside it
        (SPEC-027 FR-004). The closer sees that through :meth:`may_be_inside` and leaves the sink
        in the owner's record for a later call. The expiry line reports what is **pending**, never
        a loss (SPEC-068 FR-001): how many events are inside the emit the thread is still running
        (SPEC-062 FR-005) and how many submissions are still queued behind it, each clause only
        when non-zero. Neither is lost yet. The emit may still deliver, and :meth:`_emit` counts
        its batch when it returns; until then the events are in ``health().events.in_flight``,
        which the line names (SPEC-088 FR-002 item 4, where it promised a count "when the emit
        returns" that never comes if the process exits first); the queued submissions are counted by whichever ``_emit`` line
        settles them, one per batch, so each event is announced lost at most once. The queued
        clause promises no later count on purpose: ~~when the wedged emit is
        :meth:`_final_drain`'s own, what is queued arrived after that drain's last read and will
        never be taken~~ — refined by SPEC-090 FR-002: when the wedged emit is one of
        :meth:`_final_drain`'s batches, what is queued is either inside the bound that drain
        fixed when it began, and taken only if the emit returns, or arrived after it and is
        never taken. It counts submissions only, never the sentinel just queued or a flush
        marker (:meth:`_queued_submissions`).

        **The in-flight figure is every event the drain thread holds** (SPEC-090 FR-002): the
        batch inside the emit, the batch being built and at most one carried submission, which
        is ``health().events.in_flight``, one read of a counter only the drain thread writes.
        **The exit tallies are written first** (FR-003): :meth:`_write_exit_tallies` takes and
        zeroes them under the lock, and a batch is added to one only after :meth:`_settle` has
        debited it from ``_in_flight``, so taking them before reading the figure leaves no batch
        named both in a tally line and in this line's in-flight figure, and the tally lines
        precede the expiry line. What the drain thread adds after this take it writes at its
        own end.

        Args:
          timeout: The caller's budget in seconds, or ``None`` to wait indefinitely. It bounds the
            wait of a caller that claims here or loses the claim, and it is what the expiry line
            renders; a caller that already won the claim joins until the deadline it recorded.
          claimed: The verdict of a :meth:`claim_stop` the caller already made under its own
            lock, or ``None`` to claim here.

        Returns:
          None.

        Raises:
          None.
        """
        own = None if timeout is None else time.monotonic() + timeout
        on_drain_thread = threading.current_thread() is self._thread
        first = self.claim_stop(own) if claimed is None else claimed
        deadline = self._exit_deadline if first else own
        if not first:
            if on_drain_thread:
                return
            if self.draining:
                self._drain_settled.wait(
                    None if deadline is None else max(0.0, deadline - time.monotonic())
                )
            return
        took_over = self._join_taken_over and not on_drain_thread
        if not self._drain_finished.is_set() and not took_over:
            try:
                self._queue.put_nowait(_SHUTDOWN)
            except queue.Full:
                pass
        self._stop.set()
        if on_drain_thread:
            return
        self._thread.join(None if deadline is None else max(0.0, deadline - time.monotonic()))
        if self._thread.is_alive():
            self._write_exit_tallies()
            queued = self._queued_submissions()
            in_flight = self._in_flight
            with self._lock:
                if self.stopped_reason is None:
                    self.stopped_reason = "ShutdownTimeout"
            self._drain_settled.set()
            detail = (
                f"shutdown timed out after {_bounded_seconds(timeout)}; the sink is left open "
                f"because the worker thread is still using it"
            )
            if in_flight:
                detail += f"; {in_flight} event(s) in flight (health().events.in_flight)"
            if queued:
                detail += f"; {queued} submission(s) still queued"
            _diag.pending(detail)
            self._release_waiters()
            return
        self._release_waiters()

    def _queued_submissions(self) -> int:
        """Counts the submissions still queued, leaving out the shutdown sentinel and flush markers.

        Submissions, not events and not entries (SPEC-068 FR-001): the queue holds one entry per
        submitted span plus any ``_SHUTDOWN`` sentinel and ``_FlushMarker``, and ``qsize()``
        counts all three. The expiry line used that and so announced the sentinel ``stop()`` had
        just queued as a lost item, and the swap line counted its own unanswered marker. The
        classification is :meth:`_final_drain`'s — an entry that is neither of the two is a
        submission — so the line counts exactly what a drain would take.

        It reads ``Queue``'s private ``mutex`` and ``queue``, the access :meth:`_release_waiters`
        already makes and ``architecture.md`` §13 records: there is no public way to inspect a
        queue without consuming it. A CPython change there would make this read 0, dropping the
        queued clause from a line, rather than raise.

        Args:
          None.

        Returns:
          The number of queued submissions, or 0 when the queue cannot be read.

        Raises:
          None. A diagnostic must not be the reason the diagnosis is lost.
        """
        try:
            with self._queue.mutex:
                return sum(
                    1
                    for item in self._queue.queue
                    if item is not _SHUTDOWN and not _diag.is_a(item, _FlushMarker)
                )
        except Exception:
            return 0

    def _run(self) -> None:
        """Runs the drain loop, recording whatever terminates it (SPEC-019 FR-001).

        :meth:`_emit` already absorbs an ``Exception`` from the sink, so anything reaching this
        handler has ended the only thread that delivers — and CPython's thread bootstrap
        discards a ``SystemExit`` without even a traceback, which is why the catch is
        ``BaseException``. It records and exits; looping onward past a ``KeyboardInterrupt``
        would be a worse failure than the one this prevents.

        The two ``finally`` blocks are nested rather than merged, and the order they impose is
        load-bearing three times over. ``_drain_finished`` is set **before** ``_drain_settled``,
        and :meth:`_given_up` reads the pair in the opposite order to conclude that a shutdown
        expired on a live drain — so inverting these two lines would make an ordinary terminal
        exit indistinguishable from an abandoned one for a moment, and a ``flush()`` landing there
        would be told ``"abandoned"`` where ``"thread-died"`` is the truth. Both are falsy, so the
        cost is a wrong reason rather than a wrong verdict, which is why it is recorded here
        rather than pinned by a test. ``_drain_finished`` is set the instant the loop stops reading
        the queue — *before* :meth:`_terminal_failure`, which writes to stderr and can block on
        a slow reader — so a ``shutdown()`` arriving during that window sees a drain that is
        already finished and declines to queue a sentinel nothing would consume. And it is set
        before :meth:`_release_waiters`, so a marker either lands ahead of that sweep's snapshot
        and is answered by it, or lands behind it and finds the flag set; both take the queue's
        own mutex, which is what leaves no gap between the two. Sweeping here rather than only
        in :meth:`Worker.stop` is what covers the paths ``stop`` never reaches — a
        terminal failure, and a bounded shutdown that expired while this thread was still
        inside an emit.

        **The outer** ``finally`` **also writes the exit tallies** (SPEC-090 FR-003), after the
        waiters are released. ~~so no ``flush()`` caller waits behind a stderr write~~ That
        ordering holds only for the path this copy exists for: on every normal exit
        :meth:`_final_drain`'s own ``finally`` has already written and zeroed the tallies before
        the waiters here are released, and this call finds nothing (diff review, 2026-11-16). A
        drain that dies in :meth:`_drain` after its exit batches began never reaches
        :meth:`_final_drain`'s ``finally``, so this is the drain thread's own end for that
        path; when nothing is tallied it writes nothing.

        ``_drain_accounted`` is set right after the sweep, in a ``finally`` of its own, so it is
        set on every exit even when the sweep raises, and before the exit tallies' line, so no
        ``flush()`` waiting on it waits behind a stderr write (SPEC-089 FR-003): on a clean exit
        nothing set it earlier, and a terminal one has already set it in :meth:`_terminal_failure`.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        pending: list[list[dict[str, object]]] = []
        try:
            try:
                self._drain(pending)
            finally:
                self._drain_finished.set()
                self._drain_settled.set()
        except BaseException as exc:
            self._terminal_failure(exc, len(pending))
        finally:
            try:
                self._release_waiters()
            finally:
                self._drain_accounted.set()
            self._write_exit_tallies()

    def _take_marker(self, marker: _FlushMarker) -> None:
        """Records a marker the drain thread has taken out of the queue (SPEC-050 FR-001).

        :meth:`_release_waiters` answers markers by reading ``self._queue.queue``, so it can only
        reach one that is still *in* the queue. A marker the drain has already dequeued is held in
        that thread's local while it emits — and if the sink's ``emit`` never returns, nothing
        answers it and a ``flush(timeout=None)`` waits forever, which is the defect FR-001 exists
        to remove. Registering it here is what puts it back within reach.

        Args:
          marker: The marker this thread is about to work on.

        Returns:
          None.

        Raises:
          None.
        """
        with self._lock:
            self._taken_markers.append(marker)
            settled = self._drain_settled.is_set()
        if settled:
            marker.event.set()

    def _release_marker(self, marker: _FlushMarker) -> None:
        """Drops a marker the drain thread has finished with, before it is answered.

        **It runs after ``event.set()``, and that order is load-bearing** — the reverse survives
        the suite, which is this repo's evidence that nothing covers it rather than that it is
        safe. An earlier draft of this docstring read the green as proof of equivalence and said
        so; it is not. Deregistering first leaves a window in which the marker is in neither the
        queue nor the record, so an async ``BaseException`` landing there — the one ``_run``
        catches, on this thread — strands a ``flush(timeout=None)`` that ``_run``'s own closing
        sweep can no longer reach. Answering first inverts the failure into a leaked list entry on
        a worker that is already dead. This is the reasoning :meth:`_answer_marker`, which both
        drains answer a marker through (SPEC-090 FR-002), applies to its own ``finally``.

        Args:
          marker: The marker this thread has finished with.

        Returns:
          None.

        Raises:
          None.
        """
        with self._lock:
            self._taken_markers = [m for m in self._taken_markers if m is not marker]

    def _seal_marker(self, marker: _FlushMarker, *, unwinding: bool) -> None:
        """Closes a counted marker's window on the drain thread, just before it is answered.

        :meth:`_answer_marker` calls it, the one answerer :meth:`_drain` and :meth:`_final_drain`
        share (SPEC-090), in the ``finally`` that sets the marker's event and immediately before
        that ``set()``, so the
        snapshot is taken after every event ahead of the marker has settled and the waiter reads
        it after ``event.wait()`` returns, the same ordering ``delivered`` relies on (SPEC-089
        FR-002).

        **A marker answered while unwinding gets no snapshot, only the mark.** The drain is dying
        then, and ``_terminal_failure`` has not yet debited what it held to ``drain_lost`` nor
        set ``_drain_accounted``, so the queue does not yet read as ``stranded``; a snapshot
        taken here would miss both. The caller sees ``unwinding`` and waits on
        ``_drain_accounted`` before reading for itself (FR-003).

        **The snapshot never waits on the ledger's registration lock**
        (:func:`_lifecycle._ledger_snapshot_if_free`). Code running inside that lock's section,
        a ``__del__`` or a signal handler, may be the very ``flush()`` or ``shutdown()`` waiting
        on this marker, so when another thread holds the lock the marker is answered with no
        snapshot and the caller reads the end itself.

        **A fault in the snapshot is absorbed by type, and the marker is answered without one.**
        This runs on the only thread that delivers, and an exception escaping a ``finally`` here
        would end it; the caller then reads the end itself. Only ``Exception`` is caught, since a
        ``BaseException`` is ``_run``'s to record. It writes no ledger counter.

        Args:
          marker: The marker about to be answered.
          unwinding: Whether the answerer is unwinding an exception rather than settled.

        Returns:
          None.

        Raises:
          None.
        """
        if unwinding:
            marker.unwinding = True
            return
        if not marker.counted:
            return
        try:
            ledger = _lifecycle._ledger_snapshot_if_free()
            if ledger is None:
                return
            marker.ledger_pid = os.getpid()
            marker.ledger = ledger
        except Exception as exc:
            _diag.absorbed(
                "reading the event ledger for a flush",
                exc,
                "the flush reads its counts on the calling thread instead",
            )

    def _release_waiters(self) -> None:
        """Answers the ``flush()`` markers outstanding at the moment it runs.

        **Two populations, and the second is not optional** (SPEC-050 FR-001). A marker still in
        the queue is read from it; a marker the drain thread has already **taken** is read from
        :attr:`_taken_markers`, because between dequeuing one and returning from ``sink.emit`` the
        drain holds it in a local where a queue read cannot see it. Answering only the first is
        what the audit prescribed and it does not cover the audit's own probe. The taken markers
        are answered **before** the queue read and outside its ``try``: that read reaches into
        ``Queue``'s privates, a risk ``architecture.md`` §13 accepts on the understanding that a
        CPython change costs *timed-out* waiters, and letting the new mechanism inherit it would
        upgrade that cost to waiters that never return at all.

        A ``BaseException`` from the main loop skips :meth:`_final_drain` entirely, which is
        where queued markers are normally answered, leaving a waiter to sit for its full
        timeout on a thread that is never coming back. The markers are read out of the queue
        rather than consumed, because the queued event-lists are the evidence
        ``health().queued`` and the terminal line report; each keeps its pessimistic
        ``delivered``, which is the truth here.

        It is called from three places. The terminal-failure path is the original one. The clean
        :meth:`shutdown` path was added because that same enqueue-after-the-drain race happens
        there too, and hurts more: measured stranding a marker in 13 of 400 shutdowns raced
        against a ``flush()`` under load, where the caller sat out its whole timeout — and
        ``flush(timeout=None)``, which the API documents as supported, waits forever rather
        than too long. The marker keeps its pessimistic ``delivered``, which is the honest
        answer: the drain that would have carried it is gone.

        **It answers a snapshot, so "every" would be the wrong word** and a caller arriving
        after it is not covered here. A ``flush()`` that passed its guards microseconds before
        this sweep can still enqueue its marker after it. That caller is answered by
        :meth:`flush`'s own post-put re-check instead, which consults the same
        ``_drain_settled`` this path sets — before SPEC-050 FR-001 added it there, such a caller
        waited out its timeout, and a ``timeout=None`` one waited forever.
        A marker left queued is also still counted by ``health().queued``, which describes
        submissions; removing it would mean deleting a specific item, which ``Queue`` has no
        public way to do, and the read above is the access ``architecture.md`` §13 sanctions.

        The reliance on ``queue.Queue``'s private ``mutex`` and ``queue`` is deliberate and is
        recorded in ``architecture.md`` §13 Known Constraints (SPEC-031 FR-005): there is no
        public "inspect without consuming", and the draining alternative would destroy the
        evidence the terminal-failure line reports. A CPython change would surface as the test
        that exercises this against a mixed queue, rather than as waiters silently timing out.

        Args:
          None.

        Returns:
          None.

        Raises:
          None. This runs after the record and the stderr line, neither of which may be lost.
        """
        with self._lock:
            taken = list(self._taken_markers)
        for marker in taken:
            marker.event.set()
        try:
            with self._queue.mutex:
                queued = [i for i in self._queue.queue if _diag.is_a(i, _FlushMarker)]
            for marker in queued:
                marker.event.set()
        except Exception:
            pass

    def _terminal_failure(self, exc: BaseException, undrained: int) -> None:
        """Records the drain loop's terminal exit, then announces it (FR-001, FR-002).

        Recording precedes announcing: stderr may be closed or wedged, and unlike the overflow
        warning this line is written exactly once and cannot be re-emitted later. The
        exception's type is reported and its message is not — the rule ``_diag`` now applies to
        every line the library writes (SPEC-029), and the reason this site had it first.

        The announcement is an :func:`~log_foundry._diag.absorbed` rather than a fourth kind of
        line, since the thread's death is an exception this method caught and did not
        propagate. The count reports what was in hand and what was queued behind it (SPEC-021
        FR-002), because held alone under-reads the loss: nothing will drain the queue either.
        The queued figure is items rather than event-lists and says so, making it a floor on
        what was lost, which is the useful direction.

        **The events the thread held are debited to** ``drain_lost`` (SPEC-088 FR-002 item 3),
        before the line, for the record-first reason above. They were in no counter at all: the
        thread had taken them from the queue, so ``queued`` no longer held them, and nothing
        would ever emit them. What is queued behind the thread is the ledger's ``stranded``, read
        rather than counted, from the moment ``_drain_accounted`` is set here, right after that
        debit and **before the line** (SPEC-089 FR-003): the drain takes nothing after it, and a
        ``flush()`` waiting out this death must not also wait out a write that can block on a
        stderr the waiting thread itself holds, which a ``join`` on the thread did.

        Args:
          exc: The exception that ended the drain thread.
          undrained: How many event-lists the loop still held in hand.

        Returns:
          None.

        Raises:
          None.
        """
        with self._lock:
            self.stopped_reason = _diag.type_name(exc)
        held = self._in_flight
        self._settle(held, drain_lost=held)
        self._drain_accounted.set()
        try:
            queued: object = self._queue.qsize()
        except Exception:
            queued = "?"
        _diag.absorbed(
            "draining the log queue",
            exc,
            f"worker thread stopped; lost {held} event(s) in {undrained} undrained event-list(s) "
            f"it held and {queued} queued item(s) undelivered, nothing further will be delivered",
        )

    def _drain(self, pending: list[list[dict[str, object]]]) -> None:
        """Accumulates event-lists and emits a batch on the count, size or time trigger.

        A flush marker emits immediately, ignoring every trigger, because a caller who asked
        for a flush is not interested in the batching policy — and that branch returns to the
        top of the loop, since a marker falling through to the take below would be treated as
        a list of events and handed to ``sink.emit``, killing this thread. The marker is
        answered through :meth:`_answer_marker`, the one answerer :meth:`_final_drain` calls
        too (SPEC-090 FR-002), so a marker's verdict cannot be decided one way on one drain and
        another way on the other. The window is advanced even when idle: otherwise the timeout
        collapses to zero and ``get`` busy-spins a core.

        **A submission goes through** :meth:`_take` **and the batch is judged by**
        :meth:`_complete` (SPEC-090 FR-001), the two methods both drains call. With nothing
        waiting, a batch of ``batch_size`` submissions is emitted, as before the rule; while an
        item is waiting at the count trigger it grows toward the size target. ``waiting`` is
        read only once the batch holds ``batch_size`` submissions, so the queue's mutex is not
        taken again on every take. The time trigger and ``last_flush`` stay here, local to this
        loop, the one interval a later sweep of open spans can follow. **A carry emit restarts
        the interval** (plan review, 2026-11-16): when :meth:`_take` emitted the batch ahead
        of the submission it took, ``last_flush`` becomes the clock read just before that take,
        as the count trigger's emit sets it to the clock read before its emit. Without it a
        carry emit as long as the interval left the time trigger to fire on the very next take,
        and a per-event sink under a backlog was handed single-span batches.

        **The sweep keeps a clock of its own** (SPEC-091 FR-002). With ``sweep=`` set, the top of
        each pass runs :meth:`_run_sweep` once ``flush_interval`` has passed since ``last_sweep``,
        and a sweep that handed anything in sets ``last_flush`` back by one interval, so the time
        trigger fires on the take that follows and the swept submission leaves with it. The wait
        is the smaller of the two remainders, and reads ``last_sweep`` only when ``sweep=`` is set,
        so a worker built by hand waits as it always did. Two alternatives were measured and
        rejected: a sweep tied to the time trigger never ran while the count trigger kept firing,
        and one that did not make the trigger due waited for the next, up to two intervals. A
        sweep that could not take the hold within :data:`_SWEEP_WAIT` leaves ``last_sweep`` as it
        was and is tried again on the next pass. Under a backlog a carry right after a sweep
        restarts ``last_flush``, so the swept submission waits behind the backlog like any queued
        submission; the one-interval bound is a drain's without a backlog. **A skipped sweep waits
        an interval too** (diff review, frame 2): one that could not take the hold within
        :data:`_SWEEP_WAIT` sets ``last_sweep`` to the clock read after the attempt, so it is tried
        again an interval later, not on every pass. Retrying every pass made each pass wait out the
        bound while another thread held the hold, which slowed the drain to about twenty
        submissions a second: a ``flush()`` made inside a hold with 200 submissions queued ahead of
        its marker returned after 11.46 s (``r_hold_backlog.py``). A sweep that stopped because the
        queue was full or its lock busy has run, and waits an interval like any other.

        The shutdown sentinel **breaks** rather than falling through to the loop condition.
        :meth:`shutdown` queues it before setting ``_stop``, so a thread that takes it may find
        ``_stop`` still clear; continuing would re-enter ``get`` and block for another
        ``flush_interval``, which is a slow shutdown rather than a prompt one. Leaving
        immediately is safe because the only thing after the loop is :meth:`_final_drain`, which
        continues the batch this loop was building and takes what was queued when it began —
        the sentinel is a wake-up, never a fence.

        Args:
          pending: The accumulator owned by :meth:`_run`, which reports its size on a terminal
            failure. It is mutated in place rather than rebound, so that count is accurate.

        Returns:
          None.

        Raises:
          Exception: Whatever the queue or a final drain raises; :meth:`_run` is the guard.
        """
        last_flush = last_sweep = time.monotonic()
        while not self._stop.is_set():
            now = time.monotonic()
            if self._sweep is not None and now - last_sweep >= self.flush_interval:
                handed = self._run_sweep()
                last_sweep = now if handed is not None else time.monotonic()
                if handed:
                    last_flush = now - self.flush_interval
            now = time.monotonic()
            timeout = self.flush_interval - (now - last_flush)
            if self._sweep is not None:
                timeout = min(timeout, self.flush_interval - (now - last_sweep))
            timeout = max(0.0, timeout)
            try:
                item = self._queue.get(timeout=timeout)
            except queue.Empty:
                item = None
            if _diag.is_a(item, _FlushMarker):
                try:
                    self._answer_marker(pending, item)
                finally:
                    last_flush = time.monotonic()
                continue
            if item is _SHUTDOWN:
                break
            if item is not None:
                taken_at = time.monotonic()
                if self._take(pending, cast("list[dict[str, object]]", item)):
                    last_flush = taken_at
            waiting = len(pending) >= self.batch_size and not self._queue.empty()
            now = time.monotonic()
            if self._complete(pending, waiting=waiting) or now - last_flush >= self.flush_interval:
                self._emit_pending(pending)
                last_flush = now
        self._final_drain(pending)

    def _emit_pending(self, pending: list[list[dict[str, object]]]) -> None:
        """Emits the pending event-lists if there are any, measures the emit, then clears them.

        **The size target is set here, from this batch alone** (SPEC-090 FR-001 rule 4).
        ``elapsed`` is ``time.monotonic()`` read around :meth:`_emit`, so its retries and the
        engine's waits lower the measured rate, and :meth:`_set_target` turns the batch's
        events and that time into the target the next batch reads. The sink generation is read
        before the emit and compared after it, and ``failed_batches`` likewise, so a batch that
        straddled a swap, or was abandoned on the steady drain, sets the target to zero. A batch
        that holds no events is not handed to the sink, as :meth:`_emit` never handed it one,
        and sets the target to zero, which rule 4 names, before the exit budget below is asked
        anything: it is no attempt, and declining it would count a failed batch of nothing.

        **An abandoned exit batch leaves the target as it was** (SPEC-090 FR-001 rule 4, diff
        review, 2026-11-16), as a declined one does. On the steady drain a failure resets the
        target so a failing sink is retried with small batches while it may recover; at exit
        nothing waits for it to recover, and resetting there cut the rest of a dead sink's
        backlog into ``batch_size`` submissions each. Every such batch costs the sink's own
        calls and lines, which the exit tallies cannot aggregate: the engine's per-run line and
        a sink's own per-batch line, such as ``HTTPSink``'s. Measured with a refusing
        ``HTTPSink`` and 10,000 spans at exit: 15,217 opener calls and 953 lines with the
        reset, 321 calls and 22 lines without it, against 321 and 21 before SPEC-090.

        **An exit batch is declined unattempted when it is predicted to overrun** (FR-003). An
        exit batch is one whose first attempt begins after :meth:`claim_stop` set ``_stopped``,
        read without the lock as ``_exit_deadline`` is. The first exit batch's first attempt is
        always made. A later one is declined, through :meth:`_decline_exit_batch`, when an
        exit batch has declined a retry or a batch was declined (``_exit_declining``), or when
        :meth:`_retry_declined` predicts from the longest attempt any exit batch has made that
        this one would end past the deadline. A deadline of ``None`` declines nothing, latch or
        not: it is read first, because a caller off the drain thread that takes over an owed
        join records its own deadline (:meth:`claim_stop`), which can be ``None`` after a batch
        was declined, and the decline line would then claim a deadline there is not (diff
        review, 2026-11-16). A declined batch was not emitted and leaves the target as it was,
        so the batches the rest of the backlog is cut into are the rule's. The size is summed
        from the list rather than read from ``_pending_events``, so the measurement holds on any
        path that appends without :meth:`_take`.

        Args:
          pending: The accumulated event-lists, cleared in place.

        Returns:
          None.

        Raises:
          Exception: Whatever :meth:`_emit` does not absorb.
        """
        if not pending:
            return
        size = sum(len(events) for events in pending)
        if size == 0:
            pending.clear()
            self._pending_events = 0
            self._set_target(0, 0.0, failed=False, generation=self._sink_generation)
            return
        exit_batch = self._stopped
        if (
            exit_batch
            and self._exit_batches
            and self._exit_deadline is not None
            and (self._exit_declining or self._retry_declined(self._exit_longest))
        ):
            self._decline_exit_batch(size)
            pending.clear()
            self._pending_events = 0
            return
        if exit_batch:
            self._exit_batches += 1
        generation = self._sink_generation
        before = self.failed_batches
        started = time.monotonic()
        self._emit(pending, exit_batch=exit_batch)
        elapsed = time.monotonic() - started
        pending.clear()
        self._pending_events = 0
        failed = self.failed_batches != before
        if exit_batch and failed:
            return
        self._set_target(size, elapsed, failed=failed, generation=generation)

    def _size_target(self) -> float:
        """The size target a batch reads now, in events (SPEC-090 FR-001 rule 4).

        A target measured under an older sink generation reads as zero, so after a swap the
        first batch under a backlog is the count trigger's until the new sink is measured.

        Args:
          None.

        Returns:
          ``_target`` when it was measured under the current sink generation, otherwise zero.

        Raises:
          None.
        """
        return self._target if self._target_generation == self._sink_generation else 0.0

    def _take(self, pending: list[list[dict[str, object]]], item: list[dict[str, object]]) -> bool:
        """Takes one submission into the batch, emitting the batch first when it would overshoot.

        **The take is counted first** (SPEC-090 FR-001, added 2026-11-16): :meth:`_hold` is the
        first call, before the join-or-carry decision and before any emit, so a carried
        submission is in ``in_flight`` from its take. One counted only after the previous batch
        emitted would leave the window from take to :meth:`_hold` open across a sink call, and
        ``health().events.queued`` would over-read for that long.

        **The carry** (FR-001 rule 2): when the batch already holds ``batch_size`` submissions
        and this submission's events would take it past the size target, the batch is emitted
        without it and it begins the next batch. Otherwise it joins. So a batch holds more than
        ``batch_size`` submissions only while its events stay within the target, and the target
        never exceeds ``_BACKLOG_BATCH_CAP``, which is what makes the cap a ceiling. The append
        is in a ``finally`` so that a ``BaseException`` escaping the carry's emit still leaves
        the submission in ``pending``, where :meth:`_terminal_failure` counts the event-lists
        the thread held.

        Args:
          pending: The batch being built, mutated in place.
          item: The submission just taken from the queue.

        Returns:
          Whether a carry emitted the batch ahead of ``item``, so :meth:`_drain` can restart its
          interval.

        Raises:
          BaseException: Whatever the carry's :meth:`_emit_pending` raises; :meth:`_run` is the
            guard.
        """
        self._hold(item)
        carried = False
        try:
            if (
                len(pending) >= self.batch_size
                and self._pending_events + len(item) > self._size_target()
            ):
                carried = True
                self._emit_pending(pending)
        finally:
            pending.append(item)
            self._pending_events += len(item)
        return carried

    def _complete(self, pending: list[list[dict[str, object]]], *, waiting: bool) -> bool:
        """Whether the batch being built is ready to emit (SPEC-090 FR-001 rule 2).

        A batch is complete once it holds ``batch_size`` submissions and either nothing more is
        waiting, which is the count trigger exactly as it was before the rule, or its events
        have reached the size target. With a target of zero the second clause always holds, so
        a new worker, a forked child and a batch after a failure or a swap send the count
        trigger's batch whether anything is waiting or not.

        Args:
          pending: The batch being built.
          waiting: Whether an item is waiting in the queue; :meth:`_final_drain` passes ``True``.

        Returns:
          True when the batch should be emitted now.

        Raises:
          None.
        """
        if len(pending) < self.batch_size:
            return False
        return not waiting or self._pending_events >= self._size_target()

    def _set_target(self, events: int, elapsed: float, *, failed: bool, generation: int) -> None:
        """Sets the size target from the batch just emitted (SPEC-090 FR-001 rule 4).

        Zero when the batch was counted in ``failed_batches``, when the sink generation moved
        while it was emitted, or when it held no events: the next batch is then the count
        trigger's. An exit batch counted in ``failed_batches`` never reaches here:
        :meth:`_emit_pending` leaves the target as it was, as for a declined one. The cap when
        ``elapsed`` is zero or negative: an emit the clock cannot see took less than its
        resolution, which is the limit of an unbounded rate, and the cap is
        what bounds that limit; keeping the previous target instead would let a frozen clock pin
        whatever it last measured, and dividing by zero on this thread would end it. Otherwise
        ``min(cap, events / elapsed * flush_interval / 2)``: half an interval of the sink's own
        rate, so an emit stays shorter than the time trigger's interval and a per-event sink
        settles near half an interval's worth rather than growing its calls.

        Args:
          events: The events the batch held.
          elapsed: The seconds its emit took, retries and the engine's waits included.
          failed: Whether the batch was counted in ``failed_batches``.
          generation: The sink generation read before the emit.

        Returns:
          None.

        Raises:
          None.
        """
        cap = float(_BACKLOG_BATCH_CAP)
        if failed or events == 0 or generation != self._sink_generation:
            self._target = 0.0
        elif elapsed <= 0:
            self._target = cap
        else:
            self._target = min(cap, events / elapsed * self.flush_interval / 2)
        self._target_generation = generation

    def _answer_marker(self, pending: list[list[dict[str, object]]], marker: _FlushMarker) -> None:
        """Emits the batch ahead of a flush marker, then answers it (SPEC-090 FR-002).

        The one answerer: :meth:`_drain` and :meth:`_final_drain` both call it, so each takes
        the marker, computes ``delivered`` and sets its event in one place. The marker is
        registered through :meth:`_take_marker` before the emit, so a wedged emit leaves it
        within :meth:`_release_waiters`' reach, and it is answered in a ``finally``, then
        deregistered, so a ``BaseException`` from the emit cannot strand its waiter; the order
        of those two is :meth:`_release_marker`'s. Before the event is set the marker is
        sealed through :meth:`_seal_marker` (SPEC-089): a counted marker answered once its
        batch settled carries this drain's ledger, and one answered while the emit unwinds is
        marked so its caller waits for the drain to account before reading.

        Args:
          pending: The batch being built, emitted and cleared in place.
          marker: The flush marker just taken from the queue.

        Returns:
          None.

        Raises:
          BaseException: Whatever :meth:`_emit_pending` raises; :meth:`_run` is the guard.
        """
        self._take_marker(marker)
        settled = False
        try:
            self._emit_pending(pending)
            marker.delivered = self._nothing_lost_since(marker)
            settled = True
        finally:
            self._seal_marker(marker, unwinding=not settled)
            marker.event.set()
            self._release_marker(marker)

    def _decline_exit_batch(self, size: int) -> None:
        """Counts an exit batch declined unattempted, and marks every later one declined.

        SPEC-090 FR-003. The batch moves ``failed_batches`` by one and its events go to
        ``batch_lost``, as a declined retry's do, and it is added to the declined tally, which
        :meth:`_write_exit_tallies` announces in one line. :meth:`_settle` runs **before** the
        locked count and tally, so a batch :meth:`stop` finds in a tally it took has already
        left ``_in_flight``, which it reads after the take. No line is written here. The mark
        declines a later batch only while a deadline stands: :meth:`_emit_pending` reads a
        deadline of ``None`` first, and declines nothing under it.

        Args:
          size: The events the batch held.

        Returns:
          None.

        Raises:
          None.
        """
        self._exit_declining = True
        self._settle(size, batch_lost=size)
        with self._lock:
            self.failed_batches += 1
            events, batches = self._declined_tally
            self._declined_tally = (events + size, batches + 1)

    def _write_exit_tallies(self) -> None:
        """Takes both exit tallies and writes at most one line for each (SPEC-090 FR-003).

        Taking zeroes them in the same locked section, so no batch is announced twice whoever
        takes them: :meth:`_final_drain` and :meth:`_run` at the drain thread's end, or
        :meth:`stop` when its join expires, before its expiry line. A drain thread that goes on
        after that take writes what it adds afterwards at its own end. Every counter the lines
        name has already moved, by ``_diag``'s record-first rule. Not drain-thread only, because
        :meth:`stop` calls it.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        with self._lock:
            abandoned, declined = self._abandoned_tally, self._declined_tally
            self._abandoned_tally = (0, 0)
            self._declined_tally = (0, 0)
        if abandoned[1]:
            _diag.lost("event", abandoned[0], f"{abandoned[1]} more exit batch(es) abandoned")
        if declined[1]:
            _diag.lost(
                "event",
                declined[0],
                f"{declined[1]} exit batch(es) declined unattempted; each was predicted to end "
                f"after the shutdown() deadline",
            )

    def _hold(self, events: list[dict[str, object]]) -> None:
        """Counts a submission the drain thread has just taken from the queue (SPEC-088 FR-001).

        The gauge rises before the taking count, as an orphan call raises its gauge before it
        credits, so a reading that has counted the take finds the events either still held or
        already in an exit: the drain residual, taken less held less exits, never reads above
        zero for a drain that is balancing (FR-003). Drain thread only, and the one place a take is
        counted, so a later drain that takes differently calls this rather than writing a counter.

        Args:
          events: The submission taken.

        Returns:
          None.

        Raises:
          None.
        """
        count = len(events)
        self._in_flight += count
        self._taken += count

    def _settle(
        self,
        size: int,
        *,
        delivered: int = 0,
        skipped: int = 0,
        sink_lost: int = 0,
        batch_lost: int = 0,
        drain_lost: int = 0,
    ) -> None:
        """Debits events the drain thread has finished with to their exits (SPEC-088 FR-001).

        The exits are written before the gauge falls, for the reason :meth:`_hold` raises the gauge
        first. Drain thread only, and the one place an exit is written: every route out of
        :meth:`_emit` calls it, and so does :meth:`_terminal_failure` for what a dying thread
        held.

        Args:
          size: The events settled, which leave ``in_flight``.
          delivered: Of those, the ones whose ``emit`` returned or that ended ``ACCEPTED``.
          skipped: The ones a ``deliver`` sink answered ``SKIPPED``.
          sink_lost: The ones a settled run ended neither ``ACCEPTED`` nor ``SKIPPED``.
          batch_lost: The ones of a batch abandoned or whose retry was declined.
          drain_lost: The ones the drain thread held when it died.

        Returns:
          None.

        Raises:
          None.
        """
        self._delivered += delivered
        self._skipped += skipped
        self._sink_lost += sink_lost
        self._batch_lost += batch_lost
        self._drain_lost += drain_lost
        self._in_flight -= size

    def _drain_terms(self) -> tuple[bool, int, int, int, int, int, int, int]:
        """Reads this worker's drain-side terms, without a lock, in the order a reading needs.

        Whether the drain is still running is read first: once it reads ended, nothing will take
        a queued event or debit a held one, and reading it after the counts lets a debit land
        between the two (SPEC-088 FR-001). It reads ended once ``_drain_accounted`` is set or the
        thread has ended, whichever is seen first (SPEC-089 FR-003): the event is set only after
        the drain's last debit, so a dying drain's queue reads as ``stranded`` before its stderr
        line is written rather than only once the thread is gone. ``_drain_finished`` is not the
        test, because :meth:`_run` sets it before :meth:`_terminal_failure` debits what the
        thread held. The taking count is read
        before the gauge and the gauge before the exits, the reverse of the order the drain
        writes them in. The queue's taken-in count and ``_overflow_lost`` are left to the caller,
        which reads them after the caller-side hand-overs. Each read is one attribute load, so
        this waits behind nothing, the drain thread included.

        Args:
          None.

        Returns:
          Whether the drain is still running, then the taken, held, delivered, skipped,
          sink-lost, batch-lost and drain-lost counts.

        Raises:
          None.
        """
        running = not self._drain_accounted.is_set() and self._thread.is_alive()
        taken = self._taken
        held = self._in_flight
        return (
            running,
            taken,
            held,
            self._delivered,
            self._skipped,
            self._sink_lost,
            self._batch_lost,
            self._drain_lost,
        )

    def _nothing_lost_since(self, marker: _FlushMarker) -> bool:
        """Reports whether any batch was abandoned while a marker was outstanding (FR-001).

        ``failed_batches`` moves exactly once per abandoned batch, so comparing it against the
        marker's stamp answers "was anything lost while this flush was in flight", which is a
        stronger question than "did my emit succeed": a marker whose own emit found nothing
        pending still reports a loss another emit incurred while it waited its turn. It is
        deliberately not a running "has anything ever failed" flag, which would make every
        empty flush after a single bad batch report a failure it did not incur.

        Args:
          marker: The marker being answered.

        Returns:
          True when nothing was abandoned since the marker was stamped.

        Raises:
          None.
        """
        with self._lock:
            return self.failed_batches == marker.seen_failures

    def _final_drain(self, pending: list[list[dict[str, object]]]) -> None:
        """Continues the batch on stop and sends what was queued when it began, by the rule.

        ~~Pulls anything still queued on stop and emits the tail as one final batch~~ —
        superseded by SPEC-090 FR-002. It continues the batch :meth:`_drain` handed it, with its
        ``_pending_events``, takes items without waiting and passes each submission through
        :meth:`_take`, emitting whenever :meth:`_complete` reports the batch complete with the
        queue counted as waiting, so a batch boundary is decided here exactly as the steady
        drain decides it; the remainder goes when the loop ends. Handing the sink the whole
        backlog in one call left a ``shutdown()`` that ran out of time with nearly 20,000 events
        inside one call whose outcome nobody would learn.

        **It sweeps open spans first** (SPEC-091 FR-003), with one :meth:`_run_sweep` before the
        bound is read, so the events open spans hold when the drain stops are inside the bound and
        ship in the exit batches by SPEC-090's rule and under its budget. ``span.start`` stays
        held, so a span still open then ships its boundary events later, counted after shutdown,
        or not at all if the process ends first. The attempt waits for the sweep hold no longer
        than the steady drain does; a span whose events it could not take keeps them. It hands in
        **past ``max_queue``** (:meth:`_hand_in_at_exit`): this drain is the queue's only consumer
        and reads its bound right after, so a full queue at the stop no longer leaves a daemon
        thread's span with its events and no close to ship them, which delivered 0 of 5, uncounted
        (diff review, execution frame).

        **It takes at most the entries the queue held when it began**, read ~~in its first
        statement~~ right after that sweep, so a caller that keeps logging after ``shutdown()``
        cannot keep it running: what arrives later stays queued, in ``stranded`` and
        ``submitted_after_shutdown``. A sentinel put after that read is never taken, so the window
        a late sentinel strands in now opens when this drain begins rather than where its one pull
        loop used to end, which widens it by that loop's duration, sub-millisecond to a few
        milliseconds (``architecture.md`` §12).

        **A marker is answered where it is met**, through :meth:`_answer_marker`, after the
        batch ahead of it is emitted, so a ``flush()`` that raced ``shutdown()`` is answered by
        the batches carrying its events and never waits for the batches behind it. A
        ``BaseException`` from a batch leaves the markers already answered as they were, the
        one being answered set by that method's ``finally``, and every later one in the queue
        for :meth:`_run`'s :meth:`_release_waiters`. The exit budget lives in
        :meth:`_emit_pending`, which every batch here goes through, and the tallies it keeps are
        written in this method's ``finally`` (FR-003). Sealing a counted marker with this drain's
        ledger, or marking it answered while unwinding, happens inside :meth:`_answer_marker`
        through :meth:`_seal_marker`, as it does for :meth:`_drain` (SPEC-089, invariant 6).

        Args:
          pending: The batch :meth:`_drain` was building, continued and emitted in place.

        Returns:
          None.

        Raises:
          BaseException: Whatever an exit batch raises; :meth:`_run` is the guard.
        """
        if self._sweep is not None:
            self._run_sweep(at_exit=True)
        budget = self._queue.qsize()
        try:
            while budget > 0:
                budget -= 1
                try:
                    item = self._queue.get_nowait()
                except queue.Empty:
                    break
                if _diag.is_a(item, _FlushMarker):
                    self._answer_marker(pending, item)
                    continue
                if item is not None and item is not _SHUTDOWN:
                    self._take(pending, cast("list[dict[str, object]]", item))
                    if self._complete(pending, waiting=True):
                        self._emit_pending(pending)
            self._emit_pending(pending)
        finally:
            self._write_exit_tallies()

    def _retry_declined(self, longest: float) -> bool:
        """Whether another attempt is predicted to end past the ``shutdown()`` deadline (SPEC-062).

        The rule is ``now + longest + _JOIN_MARGIN > _exit_deadline``, written here once so the
        1.x ``emit`` loop and the delivery engine (SPEC-072) ask the same question: the engine
        is handed this bound method as its decline predicate. :meth:`_emit_pending` asks it too,
        before a later exit batch's first attempt, with the longest attempt any exit batch has
        made (SPEC-090 FR-003). A deadline of ``None`` declines nothing. ``_JOIN_MARGIN`` is
        read at each call, so a test can patch it to prove it is added; the deadline is read
        without the lock, as :meth:`_emit` documents.

        Args:
          longest: The longest attempt of the current batch so far, in seconds, or, before a
            later exit batch, the longest attempt any exit batch has made.

        Returns:
          True when the next attempt should not be made.

        Raises:
          None.
        """
        deadline = self._exit_deadline
        return deadline is not None and time.monotonic() + longest + _JOIN_MARGIN > deadline

    def _tally_exit_abandon(self, size: int, *, exit_batch: bool) -> bool:
        """Adds an abandoned exit batch to the abandon tally unless it is the first (SPEC-090).

        FR-003: the first abandoned exit batch writes the abandon line itself, and records that
        it has; every later one is added to ``_abandoned_tally`` instead, which
        :meth:`_write_exit_tallies` announces in one line. The caller holds ``_lock``, the lock
        the tally is written under, in the section that moves ``failed_batches``, and calls
        this after :meth:`_settle`. A batch that is not an exit batch is never tallied.

        Args:
          size: The events of the abandoned batch.
          exit_batch: Whether it is an exit batch whose abandonment may be tallied.

        Returns:
          True when the batch was tallied and the caller writes no line for it.

        Raises:
          None.
        """
        if not exit_batch:
            return False
        if not self._exit_abandoned_seen:
            self._exit_abandoned_seen = True
            return False
        events, batches = self._abandoned_tally
        self._abandoned_tally = (events + size, batches + 1)
        return True

    def _emit(
        self, event_lists: list[list[dict[str, object]]], *, exit_batch: bool = False
    ) -> None:
        """Flattens queued per-span event-lists into one batch and emits it, retrying.

        **A sink that offers ``deliver`` is run through the delivery engine first** (SPEC-072
        FR-005): ``run_delivery`` probes the sink once per batch, so a swap to or from an engine
        sink takes effect on the next batch, and returns ``None`` for a sink that does not offer
        it, which then takes the ``emit`` loop below unchanged. The engine's budget already holds
        the worker's share — ``(sink + 1) * (worker + 1)`` rounds — so no whole batch is resent
        around it; it waits on this worker's ``_stop`` and declines through
        :meth:`_retry_declined`. A failed run moves ``failed_batches`` once and writes one line, the
        declined wording with ``k`` the ``deliver`` calls made, or 1.x's abandoned wording with the
        cycles the run started; a settled run adds nothing, as a partial outcome never did. The
        line counts the batch as it was before the run, so a ``deliver`` that mutates the list it
        was handed cannot change it.

        A failing ``sink.emit`` is retried up to ``max_retries`` times; past that the batch is
        abandoned with a counted warning and draining continues, so a broken sink never crashes
        the worker thread or the app (FR-002, FR-003). The backoff waits on the stop event, so
        a failing sink cannot stall the drain past a few quick tries during shutdown, and the
        warning goes through ``_diag`` so a broken stderr cannot kill the thread and cost every
        batch after it (SPEC-029 FR-003).

        **A refusal no retry can cure is abandoned at the attempt that raised it.** A shipped sink
        that is closed or closing, or a forked child's ``SQLiteSink`` or ``NATSSink``, raises the
        error :func:`~log_foundry.sinks._refusal.final_refusal` built, and resending it cannot
        succeed: it only cost the batch ``max_retries`` backoff waits, measured at 17.3 s for a
        forked child's ``flush()`` over 2000 ``@trace`` calls, 4 ``emit`` calls a batch, at
        ``2ff9de9e`` (#342). Such a
        batch moves ``failed_batches`` once and writes the abandoned line with the attempts it
        actually made, as an exhausted batch does and as an engine sink's ``REJECTED`` run
        already did. Any other exception, a third-party sink's ``SinkDeliveryError`` included,
        keeps the 1.x resend, and so does a shipped sink's final refusal that reached the worker
        through a sink of the caller's own, whether that sink re-raised it (its frame is in the
        traceback) or only forwarded ``emit`` to the shipped sink (it is in the sink graph): the
        mark is private and is honoured only for the package's own sinks, so the ``emit`` contract
        for a sink of the caller's own is unchanged.

        ``max_retries`` is floored at zero so the loop always makes at least one attempt: a
        negative value otherwise skipped the emit entirely and discarded the batch with no
        attempt, no counter and nothing on stderr — reachable only by misconfiguration, but
        reachable.

        **On the exit drain a retry fits the caller's budget or is not made** (SPEC-062 FR-001).
        Each attempt is timed, ``time.monotonic()`` around ``sink.emit`` only and never the
        backoff, and immediately before each retry, after its backoff wait has returned, the
        retry is declined when ``now + longest attempt of this batch + _JOIN_MARGIN`` is past
        ``_exit_deadline``. ~~The first attempt of a batch is never declined~~ — refined by
        SPEC-090 FR-003: the first attempt of the first exit batch is never declined, and a
        later exit batch's is declined by :meth:`_emit_pending` before this method is called,
        once an exit batch has declined a retry or the longest attempt any exit batch has made
        predicts it would overrun. A deadline of
        ``None`` declines nothing, and "longest" is per batch. The estimator is the longest
        attempt rather than the last or the mean because a sink whose attempts vary — 0.1 s,
        0.8 s, 0.3 s — can run the next for as long as it ever has, and a prediction from a
        shorter one lets the join expire with the sink left open, which is the outcome the
        budget exists to prevent. The deadline is read without the lock, as :attr:`draining` is
        read: it is written by :meth:`claim_stop` in the decision that wins the stop gate or
        takes an owed join over (SPEC-068 FR-006), and a stale read costs at most one attempt, which
        invariant 3's observable already allows. Measured at ``71e42dd``, before the rule, a 0.5 s
        failing sink under ``shutdown(timeout=1.2)`` made 3 attempts, the join expired and
        ``close()`` ran 0 times; with it, 2 attempts and one close.

        A declined retry is counted once in ``failed_batches`` and announced once, with a reason
        distinct from the exhausted wording so a reader can tell "the sink stayed broken" from
        "the shutdown ran out of time" (FR-002). The abandon line is not written as well.

        **An exit batch feeds the exit budget and has its abandon line aggregated** (SPEC-090
        FR-003). ``exit_batch`` is :meth:`_emit_pending`'s verdict that this batch's first
        attempt begins after the stop was claimed. Every 1.x attempt of such a batch is timed,
        on success as on failure, and an engine run is timed whole and counted as one attempt,
        which overstates it and so declines no later than the true attempts would; the longest
        is kept in ``_exit_longest`` for the decision before the next exit batch. A declined
        retry of an exit batch, through the 1.x loop or an engine run that reports
        ``declined``, sets ``_exit_declining``, so every later exit batch is declined
        unattempted while a deadline stands; a deadline of ``None`` declines nothing, as
        :meth:`_emit_pending` says. The first abandoned exit batch writes the abandon line as
        any batch does; every later one is added to the abandon tally under the lock, beside
        ``failed_batches``, and announced by :meth:`_write_exit_tallies`, so a dead sink at exit
        writes two of the worker's lines where a line per batch would write a thousand. The
        tallies do not reach a line the engine or the sink itself writes per batch, the engine's
        ``absorbed`` line or ``HTTPSink``'s ``lost 1 request(s)``: those follow the number of exit
        batches, which :meth:`_emit_pending` keeps near one per target's worth of events, which
        is ``_BACKLOG_BATCH_CAP`` when it was measured there, by leaving the target as it was
        after an abandoned exit batch, except when the target was already zero at the stop
        (``architecture.md`` §12). On every route that can tally,
        :meth:`_settle` runs before the locked count, so a batch :meth:`stop` finds in a tally it
        took has already left ``_in_flight``.

        ~~``_in_flight`` holds the batch size from before the first attempt until this method
        returns by any route~~ — superseded by SPEC-088: ``_in_flight`` already holds the batch,
        because :meth:`_hold` counted each submission as the drain took it, and every route out
        that settles the batch — delivered, abandoned, declined, or a settled engine run — debits
        it through :meth:`_settle` to one exit. A ``BaseException`` escaping the sink settles
        nothing here: the events stay held, and :meth:`_terminal_failure` debits them to
        ``drain_lost`` when the thread dies, which the ``finally`` that zeroed the gauge used to
        hide.

        Args:
          event_lists: The accumulated per-span event-lists to flatten and emit.
          exit_batch: Whether this batch's first attempt begins after the stop was claimed
            (SPEC-090 FR-003); ``False``, the default, for every other batch and for a caller
            that hands this a batch directly.

        Returns:
          None. The outcome is not returned because ``failed_batches`` already moves exactly
          once per abandoned batch, and a second channel for the same fact could only disagree
          with the counter a waiting ``flush()`` is compared against (SPEC-021 FR-001).

        Raises:
          BaseException: Whatever ``sink.emit`` raises that is not an ``Exception``;
            :meth:`_run` is the guard.
        """
        batch = [event for events in event_lists for event in events]
        if not batch:
            return
        retries = max(self.max_retries, 0)
        longest = 0.0
        size = len(batch)
        run_started = time.monotonic()
        delivery = _engine.run_delivery(
            self.sink,
            batch,
            worker_retries=self.max_retries,
            stop=self._stop,
            decline=self._retry_declined,
        )
        if delivery is not None:
            if exit_batch:
                self._exit_longest = max(self._exit_longest, time.monotonic() - run_started)
                if delivery.declined:
                    self._exit_declining = True
            if delivery.failed:
                self._settle(size, batch_lost=size)
                with self._lock:
                    self.failed_batches += 1
                    tallied = self._tally_exit_abandon(
                        size, exit_batch=exit_batch and not delivery.declined
                    )
                if tallied:
                    return
                if delivery.declined:
                    _diag.lost(
                        "event",
                        size,
                        f"retry declined after {delivery.attempts} emit attempt(s); the next "
                        f"was predicted to end after the shutdown() deadline",
                    )
                else:
                    _diag.lost(
                        "event",
                        size,
                        f"batch abandoned after {delivery.cycles} emit attempts",
                    )
                return
            self._settle(
                size,
                delivered=delivery.accepted,
                skipped=delivery.skipped,
                sink_lost=size - delivery.accepted - delivery.skipped,
            )
            return
        for attempt in range(retries + 1):
            started = time.monotonic()
            sink = self.sink
            try:
                sink.emit(batch)
            except Exception as err:
                longest = max(longest, time.monotonic() - started)
                if exit_batch:
                    self._exit_longest = max(self._exit_longest, longest)
                if attempt >= retries or is_final_refusal(err, sink):
                    self._settle(size, batch_lost=size)
                    with self._lock:
                        self.failed_batches += 1
                        tallied = self._tally_exit_abandon(size, exit_batch=exit_batch)
                    if tallied:
                        return
                    _diag.lost(
                        "event",
                        size,
                        f"batch abandoned after {attempt + 1} emit attempts",
                    )
                    return
            else:
                if exit_batch:
                    self._exit_longest = max(self._exit_longest, time.monotonic() - started)
                self._settle(size, delivered=size)
                return
            self._stop.wait(_engine.cycle_backoff(attempt))
            if self._retry_declined(longest):
                if exit_batch:
                    self._exit_declining = True
                with self._lock:
                    self.failed_batches += 1
                self._settle(size, batch_lost=size)
                _diag.lost(
                    "event",
                    size,
                    f"retry declined after {attempt + 1} emit attempt(s); the next was "
                    f"predicted to end after the shutdown() deadline",
                )
                return
