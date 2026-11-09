"""Background flush worker — non-blocking span delivery (arch §9, guide Phase 9)."""

from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

from log_foundry import _diag, _lifecycle
from log_foundry.results import FlushResult
from log_foundry.sinks import _engine
from log_foundry.sinks._retry import floor_count

if TYPE_CHECKING:
    from log_foundry.sinks.base import Sink, SinkLosses

__all__ = ["Health", "Worker"]

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
this to its prediction of when a retry would end, so a retry permitted by the rule leaves the
join time to return. It is that window and nothing more: it reserves no time for ``close()``,
which the lifecycle owner runs inline after the join whatever the budget, and it is a module
constant rather than a parameter because a knob would let a caller recreate the overrun the
budget exists to remove. ``_emit`` reads it at each decision rather than binding it at
definition, so a test can patch it to prove it is added.
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
        the final drain's last read is counted the same way, a recorded exception
        (``architecture.md`` §12).
      dropped: Submissions discarded because the queue was full (backpressure).
      failed_batches: Batches abandoned after the retry budget was spent, or whose retry the
        exit drain declined because it was predicted to end after the ``shutdown()``
        deadline (SPEC-062).
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
        reporting is the right direction for a signal whose whole purpose is visibility.
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
        FR-003): it was true only while that second cause was counted nowhere. The two are told
        apart by the count, which is one event for the first and the span's whole buffer for the
        second, and by the stderr line, which names the site. Two fields rather than one
        because they aggregate different failure populations and so fail SPEC-026's test,
        *would one number hide which fix applies*. Their sum is deliberately not reported: with
        different populations it is a number nobody can act on.
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


@dataclass(frozen=True, kw_only=True)
class SwapOutcome:
    """What :meth:`Worker.retarget` did, and the sink it displaced (SPEC-054 FR-003).

    Internal, and deliberately not in ``__all__``: SPEC-034 froze the public surface and this is
    a hand-off between two modules of the library. Reporting through a value rather than a
    predicate is SPEC-035 FR-003's decision — the decline is taken between two lock acquisitions
    and nothing outside the worker can observe it.

    Attributes:
      verdict: ``declined`` when this worker retired mid-swap and kept its sink,
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
    """

    __slots__ = ("delivered", "event", "seen_failures")

    def __init__(self, seen_failures: int) -> None:
        """Stamps a marker with the failure count it will be judged against.

        Args:
          seen_failures: ``Worker.failed_batches`` as read on the caller's thread, before the
            marker joins the queue.

        Returns:
          None.

        Raises:
          None.
        """
        self.event = threading.Event()
        self.delivered = False
        self.seen_failures = seen_failures


class Worker:
    """Owns a bounded queue and daemon thread that batch and flush events to a sink.

    :meth:`submit` is a fast, in-process handoff, so a slow or down sink can never
    back-pressure the app: the queue is bounded and overflow is dropped-newest with a counter
    (arch §9). Two drains are deliberately distinct (SPEC-013) — :meth:`shutdown` is terminal
    and the worker never comes back, while :meth:`flush` drains on demand and leaves everything
    running, which a process that is frozen rather than exited needs. This class owns delivery
    mechanics only, and knows nothing about spans or context.
    """

    _FORK_SKIP = ("_held", "_taken_markers")
    """Attribute names ``_fork``'s repair walk must not read or descend into (SPEC-050 FR-004).

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
    ``_mark_inherited`` ``setdefault``s where the parent recorded nothing — leaving a child able
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
    ) -> None:
        """Starts the drain thread and offers the sink this worker's stop signal.

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
        ``_in_flight`` is the
        size of the batch :meth:`_emit` is holding, from before its first attempt until it returns,
        so an expired join can say how many events are inside the emit it gave up waiting on
        (FR-005). It is written by the drain thread and read without the lock by the stopper, as
        :attr:`draining` is read: a single assignment, so a reader sees one of two answers, both
        true at the instant.

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
          batch_size: How many submissions accumulate before an emit is triggered.
          flush_interval: Seconds before a partial batch is emitted anyway.
          max_queue: Ceiling on buffered submissions, past which the newest is dropped.
          max_retries: Retries after a failing emit, an ``int`` floored at zero here (SPEC-066
            FR-001); anything else is refused with ``TypeError``, and a ``bool`` or ``int``
            subclass is stored as the exact ``int`` it holds. On the exit drain a retry is
            declined when it is predicted to end after the ``shutdown()`` deadline (SPEC-062),
            so the retries a failing batch sees there are bounded by the
            caller's budget as well as by this count; the first attempt is always made.

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
        self._queue: queue.Queue[object] = queue.Queue(maxsize=max_queue)
        self._stop = _lifecycle._state.refresh_stop_signal()
        self._epoch = _lifecycle._state.retirements
        self._drain_finished = threading.Event()
        self._drain_settled = threading.Event()
        self._stopped = False
        self._exit_deadline: float | None = None
        self._join_owed = False
        self._join_taken_over = False
        self._in_flight = 0
        self._taken_markers: list[_FlushMarker] = []
        self._held: list[Sink] = [sink]
        self._lock = threading.Lock()
        _lifecycle.offer_stop_signal(sink, self._stop)
        self._thread = threading.Thread(
            target=self._run, name="log-foundry-worker", daemon=True
        )
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
        (SPEC-062 FR-005). ``_exit_deadline`` is reset only where a drain thread is started: a
        worker whose ``stop()`` was called directly rather than through ``shutdown()`` never
        moved the retirement count, so :func:`_lifecycle._rebuild_worker_after_fork` still
        resumes it, and without the reset every retry in that child would be declined against
        a deadline the parent's caller set, forever. A retired parent forks a retired child, and
        that child's record is left as it stands.

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
        flight is registered per sink in ``_lifecycle._state._closing_now`` now, and the child drops
        the whole registry in ``_lifecycle._clear_after_fork`` for the reason this slot was cleared
        here: it names closes running on threads that did not survive the fork, and
        ``_fork._fresh_primitive`` carries an ``Event``'s set state across, so an inherited entry
        that was already **set** would answer the child's *own* later close instantly — its first
        background ``shutdown()`` claims a close, ``atexit`` reads "already finished" and returns,
        and the process exits through a running close.

        The two drain events are **set or cleared to match what this child will actually do**,
        never simply inherited. Resuming clears them, so ``draining`` and ``flush``'s gate
        describe the thread starting here rather than the one that did not survive the fork;
        retiring **sets** them, because no thread will ever set them and a child forked while a
        ``shutdown()`` was mid-join otherwise inherits them unset with nothing to settle them.
        Two consequences, both measured: that child paid the whole 30 s budget at exit and with
        ``shutdown(timeout=None)`` would never exit at all, and — reaching further —
        ``_offer_orphan_signal`` reads it as still draining and skips, leaving the sink holding
        a **set** stop event, so every later backoff returns instantly against a destination
        that is already refusing (SPEC-033 FR-004). ``_drain_finished`` has no reader on a
        retired worker and is set for the invariant rather than for an observable: settled with
        finished clear is what :attr:`draining` defines as an *abandoned* drain, and a child
        reporting no ``stopped_reason`` must not read that way. The stop signal is re-offered for
        the sink the fork walk cannot reach: a **third-party** sink is outside its ownership
        boundary, so it would keep pointing at the pre-fork event while this worker sets a new
        one — SPEC-027's guarantee broken by the repair meant to preserve it.

        **The new thread is only installed once it has started**, which ``__init__`` never had
        to consider: a constructor whose ``start`` raises lets no ``Worker`` escape, while here
        the worker is already the process's. Assigning first would leave an unstarted ``Thread``
        on a live worker reading ``draining`` forever, and the next ``shutdown()`` would take a
        ``RuntimeError`` from ``join`` straight out of a public call documented to raise nothing.
        The inherited thread object is kept instead, which is safe because CPython repairs it
        across the fork — measured, it reports dead and both a bounded and an unbounded ``join``
        return in 0.0000 s — and the failure is recorded as a ``stopped_reason``, which is
        exactly SPEC-019's vocabulary for "nothing is delivering". Setting both drain events is
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
        self._queue = queue.Queue(maxsize=self._max_queue)
        self.dropped = 0
        self.failed_batches = 0
        self.submitted_after_shutdown = 0
        self.incomplete_swaps = 0
        self.stopped_reason = None
        self._in_flight = 0
        self._taken_markers = []
        self._held = [self.sink]
        if not resume:
            self._drain_finished.set()
            self._drain_settled.set()
            return
        self._exit_deadline = None
        self._stopped = False
        self._join_owed = False
        self._join_taken_over = False
        self._stop = _lifecycle._state.refresh_stop_signal()
        self._drain_finished.clear()
        self._drain_settled.clear()
        _lifecycle.offer_stop_signal(self.sink, self._stop)
        thread = threading.Thread(target=self._run, name="log-foundry-worker", daemon=True)
        try:
            thread.start()
        except Exception as exc:
            self.stopped_reason = type(exc).__name__
            self._drain_finished.set()
            self._drain_settled.set()
            _diag.absorbed("starting this child's drain thread", exc, "it will deliver nothing")
            return
        self._thread = thread

    def submit(self, events: list[dict[str, object]]) -> None:
        """Hands a finished span's events to the worker, without blocking.

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

        Args:
          events: The span's buffered events, submitted as one item.

        Returns:
          None.

        Raises:
          None.
        """
        retired = _lifecycle_state.retirements > self._epoch
        if retired:
            self._count_undeliverable()
        try:
            self._queue.put_nowait(events)
        except queue.Full:
            with self._lock:
                self.dropped += 1
                total = self.dropped
            if total == 1 or total % _diag.WARN_EVERY == 0:
                _diag.lost("submission", total, "log queue full; count is cumulative")
            return
        if not retired and _lifecycle_state.retirements > self._epoch:
            self._count_undeliverable()

    def _count_undeliverable(self) -> None:
        """Counts a post-shutdown submission and warns on the same throttle as overflow.

        The two conditions warrant the same treatment for the same reason (FR-002): a caller
        making this mistake makes it on every invocation, so a line per submission would be its
        own outage, while total silence is how the mistake survives to production. The counter
        moves before the line is attempted, per ``_diag``'s record-first rule, and the write
        happens outside the lock so a wedged console cannot stall the drain path.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        with self._lock:
            self.submitted_after_shutdown += 1
            total = self.submitted_after_shutdown
        if total == 1 or total % _diag.WARN_EVERY == 0:
            _diag.lost(
                "submission",
                total,
                "logged after shutdown(), which is terminal; nothing will drain these. Use "
                "flush() in a process that logs again. Count is cumulative",
            )

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

        Args:
          timeout: Seconds bounding the whole call — one deadline shared by the put and the
            wait, so the two cannot add up to twice the timeout. ``None`` waits indefinitely.

        Returns:
          A :class:`FlushResult`, truthy once the worker has delivered them and otherwise
          falsy with a ``reason``: ``"timed-out"``, ``"retired"``, ``"thread-died"``,
          ``"queue-full"``, or ``"abandoned"``. That last one has **four** producers, not one:
          the drain carrying those events gave up after exhausting retries (SPEC-021 FR-001,
          the original — and it used to return True, a false success exactly where ``flush()``
          matters most); an expiring ``shutdown()`` answered the marker pessimistically rather
          than leave it on a drain it had abandoned (SPEC-050 FR-001); this call's own marker
          arrived after that had already happened; or it never reached the queue, because that
          was full and the drain had already been given up on. Only the first says the drain
          adjudicated the batch, so ``"abandoned"`` means "not confirmed delivered", never
          "confirmed lost". ``ok=True`` is unaffected and still means the sink took them:
          ``delivered`` starts ``False`` and is written only by the drain, only after its emit
          returned. **The inner call carries the
          type too, not only the public ``log_foundry.flush``** (SPEC-034 FR-007 AC-1b): the
          five outcomes are distinguishable only here, so a public wrapper over a bare ``bool``
          could name none of them without guessing.

        Raises:
          None.
        """
        if _lifecycle_state.retirements > self._epoch:
            return FlushResult(ok=False, reason="retired")
        if not self._thread.is_alive():
            return FlushResult(ok=False, reason="thread-died")
        with self._lock:
            marker = _FlushMarker(self.failed_batches)
        deadline = None if timeout is None else time.monotonic() + timeout
        if not self._put_marker(marker, deadline):
            if self._given_up():
                return FlushResult(ok=False, reason="abandoned")
            if not self._thread.is_alive() or self._drain_finished.is_set():
                return FlushResult(ok=False, reason="thread-died")
            return FlushResult(ok=False, reason="queue-full")
        given_up = self._given_up()
        if self._settled():
            answered = marker.event.is_set()
            if answered and marker.delivered:
                return FlushResult(ok=True)
            if answered or given_up:
                return FlushResult(ok=False, reason="abandoned")
            return FlushResult(ok=False, reason="thread-died")
        remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
        if not marker.event.wait(remaining):
            return FlushResult(ok=False, reason="timed-out")
        if not marker.delivered:
            return FlushResult(ok=False, reason="abandoned")
        return FlushResult(ok=True)

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
        both a close. ``fenced`` — a ``flush()`` after the reassign confirmed the drain reached
        the new sink, so nothing can still be inside the old one and the owner may release it.
        ``unfenced`` — that confirmation did not arrive within the budget, so the old sink stays
        among the ones this thread may be inside and a later ``shutdown()`` that finds the thread
        ended closes it (SPEC-050 FR-004).

        The retirement check is re-taken **after** the drain as well as before it. A ``shutdown()``
        landing in between would otherwise install a sink nothing will ever close, and report an
        ``incomplete_swaps`` whose line says the old sink was left open when it was in fact closed.

        The unconfirmed-swap diagnostic reports the budget as it stood at **entry**, not what was
        left of it by then: by that point the remainder is ~0 and the line would read "could not
        be confirmed drained within 0s", which is true of the last attempt and false about what
        the caller allowed.

        Args:
          new_sink: The sink to deliver to from now on.
          deadline: The ``time.monotonic()`` instant the caller's budget expires, or ``None``.

        Returns:
          What it did, and the sink it displaced.

        Raises:
          None.
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
            self._held = [held for held in self._held if held is not new_sink] + [new_sink]
        _lifecycle.offer_stop_signal(new_sink, self._stop)
        remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
        if not (drained and self.flush(remaining)):
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
        that drain's last read, is never consumed, and ``health().queued`` reads 1 for the life
        of the process with ``submitted_after_shutdown`` at 0 — measured in 33 of 300 unforced
        runs. The drain needs no wake-up from it either: the drain-thread winner has set the stop
        event, and the drain loop tests that as soon as the emit it is inside returns.

        Whether this caller is the drain thread is read **before** the lock, so the lock is still
        held across no call. In a forked child, ``self._thread`` names the inherited dead thread
        for an instant after the new one starts; :meth:`_reinit_after_fork` says why no caller
        can be on the new thread inside that instant.

        It takes only ``_lock`` and calls nothing while holding it, so it is a leaf in the lock
        order: :func:`_lifecycle._shutdown_worker` calls it under ``_state._lock`` and the
        nesting ``_state._lock`` then ``Worker._lock`` is the order the closer already
        documents at ``_closing_now_lock``.

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
        own event — and passes the verdict in as ``claimed``. A direct caller, and the late-worker
        route, pass nothing and claim here, before ``self._stop.set()``. Either way a backoff cut
        short by the stop wakes to find the deadline already recorded. Each caller joins or waits
        against **its own** timeout, never another caller's: a second caller is bounded by the
        budget it asked for whichever deadline the drain is declining against.

        An expired join leaves the sink **open**, because the thread is still inside it
        (SPEC-027 FR-004). The closer sees that through :meth:`may_be_inside` and leaves the sink
        in the owner's record for a later call. The expiry line reports what is **pending**, never
        a loss (SPEC-068 FR-001): how many events are inside the emit the thread is still running
        (SPEC-062 FR-005) and how many submissions are still queued behind it, each clause only
        when non-zero. Neither is lost yet. The emit may still deliver, and :meth:`_emit` counts
        its batch when it returns; the queued submissions are counted by whichever ``_emit`` line
        settles them, one per batch, so each event is announced lost at most once. The queued
        clause promises no later count on purpose: when the wedged emit is :meth:`_final_drain`'s
        own, what is queued arrived after that drain's last read and will never be taken. It
        counts submissions only, never the sentinel just queued or a flush marker
        (:meth:`_queued_submissions`).

        Args:
          timeout: Seconds to wait for the drain thread, or ``None`` to wait indefinitely.
          claimed: The verdict of a :meth:`claim_stop` the caller already made under its own
            lock, or ``None`` to claim here.

        Returns:
          None.

        Raises:
          None.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        on_drain_thread = threading.current_thread() is self._thread
        first = self.claim_stop(deadline) if claimed is None else claimed
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
        self._thread.join(timeout)
        if self._thread.is_alive():
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
                detail += f"; {in_flight} event(s) in flight, counted when the emit returns"
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
                    if item is not _SHUTDOWN and not isinstance(item, _FlushMarker)
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
            self._release_waiters()

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
        a worker that is already dead. This is the reasoning :meth:`_final_drain` already applies
        to its own ``finally``.

        Args:
          marker: The marker this thread has finished with.

        Returns:
          None.

        Raises:
          None.
        """
        with self._lock:
            self._taken_markers = [m for m in self._taken_markers if m is not marker]

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
                queued = [i for i in self._queue.queue if isinstance(i, _FlushMarker)]
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

        Args:
          exc: The exception that ended the drain thread.
          undrained: How many event-lists the loop still held in hand.

        Returns:
          None.

        Raises:
          None.
        """
        with self._lock:
            self.stopped_reason = type(exc).__name__
        try:
            queued: object = self._queue.qsize()
        except Exception:
            queued = "?"
        _diag.absorbed(
            "draining the log queue",
            exc,
            f"worker thread stopped; {undrained} undrained event-list(s) held and {queued} "
            f"queued item(s) undelivered, nothing further will be delivered",
        )

    def _drain(self, pending: list[list[dict[str, object]]]) -> None:
        """Accumulates event-lists and emits a batch on the count or time trigger.

        A flush marker emits immediately, ignoring both triggers, because a caller who asked
        for a flush is not interested in the batching policy — and that branch returns to the
        top of the loop, since a marker falling through to the append below would be treated as
        a list of events and handed to ``sink.emit``, killing this thread. The window is
        advanced even when idle: otherwise the timeout collapses to zero and ``get`` busy-spins
        a core.

        The shutdown sentinel **breaks** rather than falling through to the loop condition.
        :meth:`shutdown` queues it before setting ``_stop``, so a thread that takes it may find
        ``_stop`` still clear; continuing would re-enter ``get`` and block for another
        ``flush_interval``, which is a slow shutdown rather than a prompt one. Leaving
        immediately is safe because the only thing after the loop is :meth:`_final_drain`, which
        collects whatever is still queued — the sentinel is a wake-up, never a fence.

        Args:
          pending: The accumulator owned by :meth:`_run`, which reports its size on a terminal
            failure. It is mutated in place rather than rebound, so that count is accurate.

        Returns:
          None.

        Raises:
          Exception: Whatever the queue or a final drain raises; :meth:`_run` is the guard.
        """
        last_flush = time.monotonic()
        while not self._stop.is_set():
            timeout = max(0.0, self.flush_interval - (time.monotonic() - last_flush))
            try:
                item = self._queue.get(timeout=timeout)
            except queue.Empty:
                item = None
            if isinstance(item, _FlushMarker):
                self._take_marker(item)
                try:
                    self._emit_pending(pending)
                    item.delivered = self._nothing_lost_since(item)
                finally:
                    last_flush = time.monotonic()
                    item.event.set()
                    self._release_marker(item)
                continue
            if item is _SHUTDOWN:
                break
            if item is not None:
                pending.append(cast("list[dict[str, object]]", item))
            now = time.monotonic()
            if len(pending) >= self.batch_size or now - last_flush >= self.flush_interval:
                self._emit_pending(pending)
                last_flush = now
        self._final_drain(pending)

    def _emit_pending(self, pending: list[list[dict[str, object]]]) -> None:
        """Emits the pending event-lists if there are any, then clears them.

        The two always go together.

        Args:
          pending: The accumulated event-lists, cleared in place.

        Returns:
          None.

        Raises:
          Exception: Whatever :meth:`_emit` does not absorb.
        """
        if pending:
            self._emit(pending)
            pending.clear()

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
        """Pulls anything still queued on stop and emits the tail as one final batch.

        The marker guard is a second copy of :meth:`_drain`'s and needs the same exclusion.
        Markers are answered after the final emit, so a ``flush()`` that raced ``shutdown()``
        is answered by the drain that carried its events, with that drain's outcome. They are
        set in a ``finally`` so a ``BaseException`` from the final emit cannot strand a waiter
        for its whole timeout.

        Args:
          pending: The accumulated event-lists, extended with whatever the queue still holds.

        Returns:
          None.

        Raises:
          BaseException: Whatever the final emit raises; :meth:`_run` is the guard.
        """
        markers: list[_FlushMarker] = []
        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break
            if isinstance(item, _FlushMarker):
                markers.append(item)
                self._take_marker(item)
                continue
            if item is not None and item is not _SHUTDOWN:
                pending.append(cast("list[dict[str, object]]", item))
        try:
            self._emit_pending(pending)
            for marker in markers:
                marker.delivered = self._nothing_lost_since(marker)
        finally:
            for marker in markers:
                marker.event.set()
                self._release_marker(marker)

    def _retry_declined(self, longest: float) -> bool:
        """Whether another attempt is predicted to end past the ``shutdown()`` deadline (SPEC-062).

        The rule is ``now + longest + _JOIN_MARGIN > _exit_deadline``, written here once so the
        1.x ``emit`` loop and the delivery engine (SPEC-072) ask the same question: the engine
        is handed this bound method as its decline predicate. A deadline of ``None`` declines
        nothing. ``_JOIN_MARGIN`` is read at each call, so a test can patch it to prove it is
        added; the deadline is read without the lock, as :meth:`_emit` documents.

        Args:
          longest: The longest attempt of the current batch so far, in seconds.

        Returns:
          True when the next attempt should not be made.

        Raises:
          None.
        """
        deadline = self._exit_deadline
        return deadline is not None and time.monotonic() + longest + _JOIN_MARGIN > deadline

    def _emit(self, event_lists: list[list[dict[str, object]]]) -> None:
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

        ``max_retries`` is floored at zero so the loop always makes at least one attempt: a
        negative value otherwise skipped the emit entirely and discarded the batch with no
        attempt, no counter and nothing on stderr — reachable only by misconfiguration, but
        reachable.

        **On the exit drain a retry fits the caller's budget or is not made** (SPEC-062 FR-001).
        Each attempt is timed, ``time.monotonic()`` around ``sink.emit`` only and never the
        backoff, and immediately before each retry, after its backoff wait has returned, the
        retry is declined when ``now + longest attempt of this batch + _JOIN_MARGIN`` is past
        ``_exit_deadline``. The first attempt of a batch is never declined, a deadline of
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

        ``_in_flight`` holds the batch size from before the first attempt until this method
        returns by any route — delivered, abandoned, declined, or a ``BaseException`` escaping
        the sink — so the stopper's expiry line can name the events inside an emit it gave up
        waiting on (FR-005). It is reset in a ``finally`` for exactly the escaping case.

        Args:
          event_lists: The accumulated per-span event-lists to flatten and emit.

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
        self._in_flight = size
        try:
            delivery = _engine.run_delivery(
                self.sink,
                batch,
                worker_retries=self.max_retries,
                stop=self._stop,
                decline=self._retry_declined,
            )
            if delivery is not None:
                if delivery.failed:
                    with self._lock:
                        self.failed_batches += 1
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
            for attempt in range(retries + 1):
                started = time.monotonic()
                try:
                    self.sink.emit(batch)
                    return
                except Exception:
                    longest = max(longest, time.monotonic() - started)
                    if attempt >= retries:
                        with self._lock:
                            self.failed_batches += 1
                        _diag.lost(
                            "event",
                            len(batch),
                            f"batch abandoned after {retries + 1} emit attempts",
                        )
                        return
                self._stop.wait(_engine.cycle_backoff(attempt))
                if self._retry_declined(longest):
                    with self._lock:
                        self.failed_batches += 1
                    _diag.lost(
                        "event",
                        len(batch),
                        f"retry declined after {attempt + 1} emit attempt(s); the next was "
                        f"predicted to end after the shutdown() deadline",
                    )
                    return
        finally:
            self._in_flight = 0
