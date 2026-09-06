"""Background flush worker — non-blocking span delivery (arch §9, guide Phase 9)."""

from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from log_foundry import _diag, _lifecycle
from log_foundry.results import FlushResult

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


def _closer_grace(deadline: float | None) -> float:
    """Returns how long a caller may wait on a close it does not own (SPEC-050 FR-002).

    The same arithmetic :func:`~log_foundry._lifecycle.join_closers` applies, and for the same
    reason: this is an *exit* waiting on a close, so it is capped at ``_lifecycle.DEFAULT_CLOSER_GRACE`` and
    carved from whatever budget the caller brought. Waiting the whole shutdown budget instead
    would make a stuck close cost thirty seconds at exit where it costs none today, which is the
    trade that constant's own docstring already refuses.

    Args:
      deadline: The caller's own monotonic deadline, or ``None`` for an unbounded caller — which
        takes the cap rather than waiting indefinitely, since an unbounded ``shutdown()`` is a
        choice about draining events, not a licence for a stuck close to hold the exit.

    Returns:
      Seconds to wait, never negative and never above the cap.

    Raises:
      None.
    """
    if deadline is None:
        return _lifecycle.DEFAULT_CLOSER_GRACE
    return max(0.0, min(_lifecycle.DEFAULT_CLOSER_GRACE, deadline - time.monotonic()))


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
        ``Queue`` offers no way to remove one specific item.
      dropped: Submissions discarded because the queue was full (backpressure).
      failed_batches: Batches abandoned after the retry budget was spent.
      stopped_reason: The exception type name that terminated the drain thread, or ``None`` if
        it never died — which is also what a live worker and a process that never logged
        report. Non-``None`` is categorically worse than the two counters above: they measure
        loss the worker absorbed and kept running through, this one means the worker is gone
        (SPEC-019 FR-003). Also ``"ShutdownTimeout"`` when a bounded :meth:`Worker.shutdown`
        expired before the drain finished (SPEC-027 FR-004), and the type name of whatever
        stopped a forked child's rebuild from starting a thread at all (SPEC-039 FR-002) — a
        case where the drain thread never ran rather than died. All three are the same thing to
        a reader, which is why they share the field: nothing is delivering.
      sink: The configured sink's own loss counters, or ``None`` when there is no worker or
        the sink reports nothing (SPEC-026 FR-003). Nested rather than folded into the
        integers above because they count different things: ``dropped`` here is backpressure
        at this queue, ``dropped`` on the sink is an event that never reached the wire.
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
        object, named
        here because SPEC-033 measured three candidates disagreeing**: the worker's sink if a
        worker exists, else the sink the orphan path recorded, else the configured one. It
        describes that object and *not* the graph beneath it, so a child that wraps an
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

    _FORK_SKIP = ("_unclosed_swaps", "_taken_markers")
    """Attribute names ``_fork``'s repair walk must not read or descend into (SPEC-050 FR-004).

    ``_taken_markers`` names ``flush()`` callers **in the parent**, whose ``Event`` the walk would
    otherwise replace on an object nothing in the child will ever wait on. The skip stops that
    work and :meth:`_reinit_after_fork` drops the references, because a skip alone leaves them
    held — measured, a child inherited the parent's in-flight marker permanently and kept it
    through fifty further flushes of its own.

    ``_unclosed_swaps`` holds sinks this process has **stopped** delivering to, which is exactly
    the shape ``_fork._SKIP_ATTRIBUTE`` describes: bookkeeping that pins objects is not live state
    to repair. Without the opt-out the walk reaches a superseded sink, replaces its locks — merely
    wasteful — and runs its fork hooks, which is not: ``_lifecycle.reclaim`` then overwrites the
    foreign-pid record the child holds for it — the parent's own stamp, or the ``_FOREIGN``
    ``_mark_inherited`` ``setdefault``s where the parent recorded nothing — leaving a child able
    to release a transport it never acquired. Nothing is lost by skipping it, for the reason
    ``_lifecycle._owned``'s entry gives: a sink that is still *live* is reached through
    ``self.sink`` and the config, neither of which is opted out.
    """

    def __init__(
        self,
        sink: Sink,
        *,
        batch_size: int = 10,
        flush_interval: float = 1.0,
        max_queue: int = 10_000,
        max_retries: int = 3,
        sink_released: bool = False,
    ) -> None:
        """Starts the drain thread and offers the sink this worker's stop signal.

        The lock guards the ``dropped`` counter, incremented from any caller thread, and the
        shutdown once-only flag, since ``shutdown`` may be called concurrently by ``atexit``
        and user code.

        ``sink_released`` says the close is **already discharged** by whoever released this sink,
        never that the sink is unusable (SPEC-044 FR-001). Only ``_lifecycle._get_worker`` passes
        it, and only for a worker built while a ``shutdown()`` was mid-flight over the very sink
        that shutdown's orphan branch had just closed — without it the exit close performs a
        second ``close()``. ``sinks/base.py`` requires an implementation to make its release
        idempotent, but the library must not *rely* on that: it cannot enforce what a
        third-party sink does, and a half-released transport is the failure SPEC-032 exists to
        prevent. So the library performs one close and does not test whether a sink survived
        two. This
        worker still emits to that sink: one that guards its post-close state refuses and the
        batch lands in ``failed_batches``, which is the documented signal on this path. The flag
        describes **one** sink, so :meth:`swap_sink` clears it when it adopts another — otherwise
        the claim would transfer to every sink this worker later held, and the next one would be
        closed by nobody.

        Args:
          sink: The destination every batch is emitted to.
          batch_size: How many submissions accumulate before an emit is triggered.
          flush_interval: Seconds before a partial batch is emitted anyway.
          max_queue: Ceiling on buffered submissions, past which the newest is dropped.
          max_retries: Retries after a failing emit, floored at zero by :meth:`_emit`.
          sink_released: Whether ``sink``'s close has already been performed elsewhere.

        Returns:
          None.

        Raises:
          None.
        """
        self.sink = sink
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.max_retries = max_retries
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
        self._sink_closed = sink_released
        self._closing: threading.Event | None = None
        self._taken_markers: list[_FlushMarker] = []
        self._unclosed_swaps: list[Sink] = []
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

        ``_unclosed_swaps`` is emptied alongside it. A child stranded nothing, and
        ``releasable()`` refuses the parent's sinks on the pid stamp anyway — so keeping them buys
        no close and costs a strong reference to every superseded sink, plus a pointless refused
        release at every child exit. Emptying is what "a child inherits no promise" means for the
        record as well as for the close.

        ``_taken_markers`` and ``_unclosed_swaps`` are emptied with it: a child inherits no
        ``flush()`` caller to answer and stranded no sink, so both are references it can only hold.

        ``_closing`` is emptied unconditionally, on both branches (SPEC-050 FR-002). It names a
        close running on a thread that did not survive the fork, so the child inherits a promise
        nothing can keep: ``_fork._fresh_primitive`` carries an ``Event``'s set state across, so
        an inherited event that was already **set** would answer the child's *own* later close
        instantly — the child's first background ``shutdown()`` claims a close, ``atexit`` reads
        "already finished" and returns, and the process exits through a running close. That is
        the defect FR-002 closes, made permanent in every child. Emptying the slot needs no
        set-or-clear reasoning at all: ``None`` means no close is running *here*, which is true
        of a child on both branches.

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
        drain thread coexists with the inherited dead one in that attribute. That is safe only
        because the drain thread never reads it — ``_run``, ``_drain``, ``_terminal_failure``
        and ``_release_waiters`` do not, and the three readers are all on caller threads — which
        is an invariant this note states rather than one anything enforces.

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
        self._closing = None
        self._taken_markers = []
        self._unclosed_swaps = []
        if not resume:
            self._drain_finished.set()
            self._drain_settled.set()
            return
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
        already given up on it (``_close_orphan_sink`` and SPEC-027 FR-004 leave the sink open
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
        """Snapshots the delivery counters (SPEC-017 FR-005, SPEC-019 FR-003).

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
          The snapshot, including the sink's own losses when it reports any.

        Raises:
          None.
        """
        retired = _lifecycle_state.retirements > self._epoch
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
            sink=self._sink_losses(),
            retired=retired,
            submitted_after_shutdown=submitted_after_shutdown,
            incomplete_swaps=incomplete_swaps,
            closing_sinks=_lifecycle.closing_count(),
            inherited_sink=not _lifecycle.releasable(self.sink),
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
        what makes :meth:`swap_sink` count an ``incomplete_swaps`` and write a loss line for a swap
        that completed. A wrong verdict, not a wrong reason. An earlier draft of this docstring
        called the window unreachable without interposing inside this method; that was wrong twice
        over — parking after the put reaches it, and holding ``_release_marker`` widens it — and a
        reviewer built both.

        Reporting is by the **marker**, never by the check alone. A drain that answered this
        marker and then exited has delivered, and saying otherwise would be a false failure —
        one ``swap_sink`` reads as an unconfirmed drain, counting ``incomplete_swaps``, leaving
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

    def swap_sink(self, new_sink: Sink, timeout: float | None = DEFAULT_SWAP_TIMEOUT) -> bool:
        """Retargets delivery at a new sink, draining and closing the previous one (FR-003).

        This is what makes a late ``configure(sink=...)`` mean what it says. The sink was
        captured once when the worker was built, so before SPEC-030 a later call updated the
        config — which ``get_config().sink`` then reported — while every event continued to the
        old sink: the config and the behaviour disagreed, and nothing said so.

        The attribute is reassigned rather than the worker rebuilt, which keeps the queue, the
        thread, the counters and the ``atexit`` registration intact; rebuilding would drop
        whatever was queued and register a second drain. The order is the contract: drain first
        so everything submitted before the call reaches the sink it was submitted for, swap,
        then drain again before closing. That second drain is a fence rather than a delivery —
        it proves the drain thread is not still inside the old sink's ``emit``, which is the one
        way ``close()`` could be called under a writer.

        On a drain that cannot be confirmed the swap still stands, because the caller asked for
        the new sink and silently keeping the old one is the defect this method exists to fix.
        What changes is that the old sink is left open **for now** and ``incomplete_swaps``
        records it: the drain thread may still be using it, and SPEC-027 FR-004 already settled
        that a leaked resource beats a close raced against a write. ~~left **open**~~ — struck
        (SPEC-050 FR-004): it is recorded in :attr:`_unclosed_swaps` and closed by
        :meth:`_close_if_owed` once the drain thread has ended, which is when that objection
        stops applying.

        Both guards are re-taken after the first drain, which blocks and therefore cannot be
        trusted to return into the state it left. Retirement is the one that bites: ``shutdown``
        closes whatever ``self.sink`` was at that moment and latches its once-only flag, so a
        swap reassigning afterwards would install a sink nothing will ever close, and then
        report that the *old* one was left open when it had in fact just been closed.

        ``configure()`` remains a startup call and this does not make it thread-safe. A span
        finishing on another thread during the swap may land on either sink; what is guaranteed
        is that everything submitted before the call was drained to the old one.

        Args:
          new_sink: The sink every subsequent batch is emitted to.
          timeout: Seconds bounding the **whole** call — one deadline shared by both drains and
            the close of the old sink, so a destination that hangs in any of the three cannot
            hold ``configure()`` for a multiple of the budget. ``None`` waits indefinitely.

        Returns:
          Whether this worker now holds ``new_sink``, and therefore owns its close. ``False``
          means **declined**, which happens only when the count moved past this worker's epoch — and the
          caller must then own the handoff itself, because a declined swap leaves the new sink
          installed nowhere (SPEC-035 FR-003). ``True`` covers the sink already being this
          worker's, which owes nothing further. The *quality* of the swap is still reported
          through ``health().incomplete_swaps`` and one stderr line rather than here: an
          unconfirmed drain is a swap that happened, so it returns ``True``, and conflating the
          two would make ``_swap_sink`` re-home a sink this worker is delivering to.

        Raises:
          None on a sink fault. A close that fails is announced, as everywhere else
          (SPEC-025 FR-004).
        """
        with self._lock:
            if _lifecycle_state.retirements > self._epoch:
                return False
            if self.sink is new_sink:
                return True
        deadline = None if timeout is None else time.monotonic() + timeout
        drained = self.flush(timeout)
        with self._lock:
            if _lifecycle_state.retirements > self._epoch:
                return False
            old = self.sink
            if old is new_sink:
                return True
            self.sink = new_sink
            self._sink_closed = False
            self._discard_owed_swap(new_sink)
        _lifecycle.offer_stop_signal(new_sink, self._stop)
        remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
        if not (drained and self.flush(remaining)):
            self._record_incomplete_swap(timeout, old)
            return True
        left = None if deadline is None else max(0.0, deadline - time.monotonic())
        self._close_swapped_out(old, left)
        return True

    def _record_incomplete_swap(self, timeout: float | None, sink: Sink) -> None:
        """Records a swap whose drain could not be confirmed, then announces it (FR-003).

        Two things are reported at once because they have one cause: queued items may have been
        carried to the new sink rather than the old one, and the old sink is not closed here. The
        count is queued *items* — one per submitted span plus any marker — which makes it a
        floor on the events involved, the useful direction for a reader deciding whether to care.
        It counts the queue and not the previous sink's buffer deliberately: the queued items are
        the ones that may be misrouted. A probe read it as zero while nine events sat in that
        sink's client buffer, and both numbers were right about different things.

        **The sink is also recorded, not merely announced** (SPEC-050 FR-004). It is owed a close
        that cannot be performed now — the drain thread may still be inside its ``emit``, which is
        SPEC-027 FR-004's reasoning — but that objection expires when the thread does, so
        :meth:`_close_if_owed` performs it once ``is_alive()`` reads ``False``. Recorded by
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
            if not any(owed is sink for owed in self._unclosed_swaps):
                self._unclosed_swaps.append(sink)
        _diag.lost(
            "item",
            self._queued_or_unknown(),
            f"the previous sink could not be confirmed drained within "
            f"{_bounded_seconds(timeout)} of a configure(sink=...); it is left open until a "
            f"shutdown() that finds the drain thread ended closes it, and queued items may "
            f"reach the new sink instead",
        )

    def _discard_owed_swap(self, sink: Sink) -> None:
        """Drops a stranded sink from the owed record, because something else will close it.

        The record is what makes :meth:`_close_if_owed`'s close once-only (SPEC-050 FR-004), so
        every route by which a recorded sink acquires a *different* closer has to prune it, or
        that sink is closed twice — and ``_lifecycle.release`` guards process ownership only and
        latches nothing about "already closed", so nothing downstream would catch it.

        Two routes, and the count is two rather than three because of an invariant worth stating:
        **a sink in the record is never this worker's live sink.** It is recorded as ``old``,
        after ``self.sink`` has already been reassigned, and it is pruned again the moment it is
        re-adopted — so the confirmed-swap branch of :meth:`swap_sink` can never see a recorded
        sink and a prune there would be unreachable. It was written, mutation-tested, found to
        survive every mutant for exactly that reason, and removed.

        So: a stranded sink **re-adopted** as this worker's live sink is closed by
        :meth:`_close_if_owed`'s live-sink branch, which is the prune in :meth:`swap_sink`; and
        one the orphan record hands over in ``_lifecycle._swap_sink`` is closed there, which
        matters because that function's re-arm guard is a single slot a later swap overwrites,
        the shape SPEC-044 FR-004 measured as ``A.closed == 2``.

        Callers hold :attr:`_lock`, ``_lifecycle._swap_sink`` included — it holds the process-wide
        lock and takes this one under it, the same nesting :meth:`swap_sink` already performs.

        Args:
          sink: The sink to drop, matched by identity. Absent is the ordinary case and a no-op.

        Returns:
          None.

        Raises:
          None.
        """
        self._unclosed_swaps = [owed for owed in self._unclosed_swaps if owed is not sink]

    def _close_swapped_out(self, sink: Sink, timeout: float | None) -> None:
        """Closes a sink the worker no longer delivers to, waiting only for the budget.

        This is reached only once both drains have been confirmed, so the *drain thread* is
        provably out of this sink's ``emit``. An orphan-path emitter on an application thread
        is not covered: it resolves the sink through ``_ensure_sink`` before emitting, so one
        that read the old sink before ``configure()`` reassigned it can still be inside its
        ``emit`` — which is why ``sinks/base.py`` requires ``close()`` to tolerate exactly that
        (SPEC-028 FR-001), and why the sinks holding transport state take their lock in both.

        **It records nothing in the closed-sink latch** (SPEC-044 FR-004): its caller is reached
        through ``_lifecycle._swap_sink``, which latches this same sink before the swap begins,
        and latching again here would only overwrite a record with itself.

        ``Sink.close`` takes no timeout, so the close is run on its own thread and joined for
        what is left of the swap's budget. **An expired join decides only who waits** — it moves
        no counter and writes no line, which is what dissolves SPEC-028's objection that an
        expired join cannot tell a slow-but-successful close from a stuck one and so reports a
        loss for closes that completed. What *is* observable is a live fact rather than an
        inference: ``health().closing_sinks`` counts the closes running at the moment it is read.

        The thread is a **daemon**, and it is :meth:`_join_closers` that makes that safe rather
        than merely available. A non-daemon thread was tried and is worse on its own: CPython
        joins non-daemon threads *before* running ``atexit``, so one hung close stops the exit
        drain from ever running and loses everything buffered in the **live** sink, along with
        the application's own exit handlers. A daemon alone is also worse on its own, in the
        opposite case: a close that is slow but *succeeding* is killed at exit, losing whatever
        it was flushing. ``shutdown`` therefore drains and closes the live sink first, then
        joins any outstanding closer for what is left of its budget — so a slow close finishes,
        a hung one costs only the grace, and neither can reach the live sink. What SPEC-028
        refused to abandon was the sink the worker was *still delivering to*; this one has been
        fenced out of the delivery path by two confirmed drains.

        It is deliberately not :meth:`_close_sink`, which answers a different question — that
        one closes the sink the worker still holds, exactly once, and only after the thread has
        ended.

        Args:
          sink: The sink that was swapped out.
          timeout: Seconds to wait for the close before returning and letting it finish on its
            own. ``None`` waits indefinitely.

        Returns:
          None.

        Raises:
          None. ``Thread.start`` raises when the platform will not give the process another
            thread, and a swap that cannot spawn one must leave the sink open and say so rather
            than fall back to an inline close — the fallback would reintroduce the unbounded
            wait this method exists to remove, in the one situation where the process is
            already under resource pressure.
        """
        closer = _lifecycle.release(sink, detached=True)
        if closer is not None:
            closer.join(timeout)

    def shutdown(self, timeout: float | None = DEFAULT_SHUTDOWN_TIMEOUT) -> None:
        """Stops the thread, drains and emits everything queued, then closes the sink.

        This is bounded (SPEC-027 FR-004). On expiry it returns having stopped what it could,
        records a ``stopped_reason`` of ``"ShutdownTimeout"`` and writes one line; it does not
        kill the thread, which Python cannot do and which would leave a sink mid-write if it
        could. An expired shutdown does not close the sink either, because the drain thread may
        still be inside ``emit`` — the cost is a leaked resource in a process that is exiting
        anyway, which is the cheaper of the two. That close is deferred rather than abandoned:
        a later call finds the thread finished and closes the sink then.

        The once-only flag deliberately stays ahead of the close. Re-running a drain is not
        safe, and a second ``shutdown()`` retrying a close that already failed would call
        ``close()`` twice on a sink that may have partially released its resources; what
        SPEC-025 FR-004 changed is that the failure is announced rather than swallowed.

        **The sentinel is queued before ``_stop`` is set, and while the drain loop is running
        that order is what makes it impossible to strand.** Both ways of leaving the loop —
        taking the sentinel, or seeing ``_stop`` — can only happen once it is already in the
        queue, so either that ``get`` consumes it or :meth:`_final_drain` does. The reverse
        order left a window in which the loop read ``_stop``, exited, and finished its final
        drain before the sentinel landed. It never lost an event, but left ``health().queued``
        reading 1 for the life of the process. The rate is load-dependent and not worth
        quoting as a property — rare when idle, and repeatedly reproduced between roughly one
        shutdown in 14 and one in 50 with spinner threads and a tightened switch interval.

        Paying for that order needs :meth:`_drain` to break on the sentinel rather than loop, or
        a thread taking it before ``_stop`` was set would block for another ``flush_interval``
        — measured stalling low single-digit percentages of shutdowns under load for the entire
        budget, latching a ``stopped_reason`` of ``"ShutdownTimeout"``, which is far worse than
        the cosmetic problem being fixed.

        The premise is the loop, so the put is skipped once the drain has stopped reading. A
        drain that died terminally (SPEC-019) is not coming back for a wake-up, and queueing one
        for it would strand it permanently — reintroducing the symptom on the one path the
        ordering cannot reach. The gate is ``_drain_finished`` rather than ``is_alive()``,
        because they are not the same instant: the thread is still alive throughout
        :meth:`_terminal_failure`, which writes to stderr and can block on a slow reader, and a
        liveness test would queue a sentinel through that whole window. The flag is set before
        that call, so this one does not.

        **The idempotent path waits for the drain it found running** (SPEC-035 FR-004).
        ``_stopped`` latches on *entry*, so a second caller used to take that branch and
        return in under a millisecond over a drain that had barely started. The shape is rarely
        two user calls: it is a ``shutdown()`` on one thread and ``atexit`` on the main thread,
        and measured with a 2 s-emit sink and three traced calls it delivered **nothing**, never
        closed the sink, and left the process gone in 0.39 s with nothing on stderr. The
        wait is on ``_drain_settled`` and bounded by *this* call's own budget, so
        ``shutdown(timeout=0)`` still returns promptly and a caller never inherits the other
        call's deadline. It is skipped where :attr:`draining` is already false, and **released**
        where the drain is abandoned after the wait began — the first caller sets the same event
        when its join expires, which is what stops a wedged thread costing the second caller its
        whole budget in front of an exit that has to happen.

        :meth:`_release_waiters` runs on the way out for the sibling case the ordering cannot
        reach: a ``flush()`` that passed its liveness check microseconds before the thread
        finished can still queue a marker nothing will answer, and with ``timeout=None`` that
        caller waits forever rather than merely too long.

        The worker does not come back, and :meth:`submit` keeps accepting afterwards — so a
        caller that logs again queues events nothing will drain. That is reported rather than
        prevented, through ``retired`` and ``submitted_after_shutdown`` (SPEC-030 FR-001) and
        one stderr line; refusing the submission or restarting the thread were both rejected,
        the second because a thread that resurrects itself fights a process trying to exit.

        Args:
          timeout: Seconds the join may take. ``None`` waits indefinitely, which is what this
            did unconditionally before and is still available on request.

        Returns:
          None.

        Raises:
          None. An unguarded close raised out of the ``atexit`` handler, where CPython printed
            a full traceback carrying the exception's message, which arch §6 keeps out of
            anything the library says about itself.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._lock:
            first = not self._stopped
            self._stopped = True
        if not first:
            if self.draining:
                self._drain_settled.wait(
                    None if deadline is None else max(0.0, deadline - time.monotonic())
                )
            self._close_if_owed(deadline)
            self._join_closers(None if deadline is None else max(0.0, deadline - time.monotonic()))
            return
        if not self._drain_finished.is_set():
            try:
                self._queue.put_nowait(_SHUTDOWN)
            except queue.Full:
                pass
        self._stop.set()
        self._thread.join(timeout)
        if self._thread.is_alive():
            queued = self._queued_or_unknown()
            with self._lock:
                if self.stopped_reason is None:
                    self.stopped_reason = "ShutdownTimeout"
            self._drain_settled.set()
            _diag.lost(
                "item",
                queued,
                f"shutdown timed out after {_bounded_seconds(timeout)}; the sink is left open "
                f"because the worker thread is still using it",
            )
            self._release_waiters()
            return
        self._release_waiters()
        self._close_if_owed(deadline)
        self._join_closers(None if deadline is None else max(0.0, deadline - time.monotonic()))

    def _join_closers(self, timeout: float | None) -> None:
        """Gives a swapped-out sink's close its last chance before the process exits.

        This is what makes the daemon closer of :meth:`_close_swapped_out` safe rather than
        merely available. A daemon is killed wherever it has reached at interpreter exit, so
        without this a close that was slow but *succeeding* — a ``KafkaSink`` flushing its
        producer, where the close is the delivery — would lose its buffer, which a non-daemon
        thread would not have. Measured both ways: daemon alone lost those events, non-daemon
        alone lost everything in the *live* sink instead, and this join is what takes neither
        loss.

        **The cap is the mechanism.** The wait is the smaller of ``DEFAULT_CLOSER_GRACE`` and
        what remains of ``shutdown``'s own budget: capped so a stuck close cannot hold a process
        at exit for the whole shutdown budget, and carved from that budget so it cannot extend it
        either. Running after :meth:`_close_if_owed` rather than before it is defence in depth
        rather than the guarantee — measured, the two orders deliver the live sink identically,
        because the cap returns control long before anything is at risk. It is still the right
        order, and pinned by a test: it is what holds if an external deadline kills the process
        *during* the grace, where the live sink would otherwise be the one left unclosed.

        It runs on the idempotent path too. A first ``shutdown`` that expired on a wedged drain
        thread returns before ever reaching here, and the ``atexit`` call that follows would
        otherwise return instantly — denying the grace to a swapped-out close that is healthy and
        moments from finishing, which is exactly the loss the grace exists to prevent. The closer
        is independent of the worker thread, so a wedged worker is no reason to abandon it. The
        expired path itself still skips it, and that costs nothing: the thread join consumed the
        budget, so the remainder is zero.

        Args:
          timeout: Seconds remaining in ``shutdown``'s budget, further capped by
            ``DEFAULT_CLOSER_GRACE`` and shared across every outstanding close. ``None`` takes
            the cap rather than waiting indefinitely — an unbounded ``shutdown`` is a caller's
            choice about draining events, not a licence for a stuck close to hold the exit.

        Returns:
          None.

        Raises:
          None. A join on a thread that has already finished is a no-op, and one that has not
            is abandoned at the deadline — which is the daemon's contract, not a failure.
        """
        _lifecycle.join_closers(timeout)

    def _close_if_owed(self, deadline: float | None = None) -> None:
        """Closes the sink exactly once, and only once the drain thread has ended.

        Every exit from :meth:`shutdown` that may close comes through here — the expired one
        deliberately does not, since the thread is still using the sink — so the decision is
        made in one place under one lock. Two concurrent ``shutdown()`` calls are what needs
        it, and ``atexit`` plus user code calling it at once is documented as normal.

        ``is_alive()`` is the safety condition rather than a heuristic: it reads ``False`` only
        after ``_run`` has returned, so the sink is provably out of use *by the worker*.

        **It records nothing in the closed-sink latch, and does not need to** (SPEC-044 FR-004):
        ``_lifecycle._orphan_owed`` still names this sink where anything named it, and
        ``worker_owns`` answers ``True``, so ``_close_orphan_sink`` declines rather than re-arming
        it. The latch exists for a sink this worker has *stopped* holding.

        The close runs to completion, inline, and is deliberately **not** bounded — which leaves
        one honest gap. SPEC-028 made ``close()`` take the sink's emit lock, so an application
        thread on the orphan path can hold that lock inside a driver call with no timeout of its
        own and delay this past ``shutdown``'s budget. Running the close on a joinable daemon
        thread was tried and reverted: at interpreter exit the daemon is killed wherever it has
        reached, which for ``SQLiteSink`` is between ``commit()`` and ``close()`` — turning the
        leaked handle SPEC-027 FR-004 accepts into the partial write it was avoiding. It also
        could not tell a slow-but-successful close from a stuck one, so it reported
        ``ShutdownTimeout`` and "left open" for closes that had in fact completed, latching
        SPEC-019's alert term on a healthy shutdown. A wrong signal is worse than a slow one.
        The residual delay is recorded in ``architecture.md`` §13 rather than papered over.

        The close runs outside the lock, because it can reach ``_diag`` and a wedged console must
        not stall a lock :meth:`submit` also takes.

        **A caller that does not claim the close waits for the one that did** (SPEC-050 FR-002).
        This is the open half of the previous audit's C3: the drain half was fixed by having the
        idempotent :meth:`shutdown` wait on ``_drain_settled``, and the close needed the same
        shape. Without it, a ``shutdown()`` first called on a background thread returned to
        ``atexit`` while its close was still running and the interpreter exited through it —
        measured against a close-is-delivery sink, twelve events died in the sink's own buffer at
        0.31 s with nothing on stderr. The wait is capped by :func:`_closer_grace` rather than
        taking the caller's whole budget, so a *stuck* close costs the exit the same grace a
        swapped-out one already costs it, and a slow-but-succeeding close finishes.

        The in-flight close is an ``Event`` in a **slot**, not a flag beside a permanent event.
        A waiter reads the slot under the lock and waits on whatever object it found, so there is
        no clear-versus-set question to get wrong: a slot holding ``None`` means no close is
        running *here*, which is also exactly what a forked child needs. The alternative — one
        event set forever — rests on the close being once-only, which is an argument rather than
        a mechanism, since :meth:`swap_sink` resets ``_sink_closed``.

        **It is also where a sink stranded by an unconfirmed swap is closed** (SPEC-050 FR-004).
        Those closes are decided by the same ``is_alive()`` question and taken out of the record
        under the same lock, which is what makes them once-only; they run **detached**, so
        :meth:`_join_closers` bounds the wait and this method cannot grow ``shutdown``'s budget,
        and they are started before the live sink's inline close so the two overlap rather than
        serialise. Living here rather than on :meth:`shutdown`'s success branch is what gives a
        stranded sink the same second chance the live one has: a first ``shutdown()`` that
        expired on a wedged drain thread leaves the record intact, and the ``atexit`` call that
        follows finds the thread finished and closes it then.

        Args:
          deadline: The calling ``shutdown``'s own monotonic deadline, or ``None``. It bounds
            only the wait for another caller's close, never a close performed here.

        Returns:
          None.

        Raises:
          BaseException: Whatever the sink's ``close`` raised that is not an ``Exception``.
            ``_close_sink`` absorbs ``Exception`` but lets a ``KeyboardInterrupt`` or
            ``SystemExit`` through to the caller (SPEC-025 FR-004).
        """
        with _lifecycle._state._lock, self._lock:
            alive = self._thread.is_alive()
            if alive:
                owed: list[Sink] = []
            else:
                owed, self._unclosed_swaps = self._unclosed_swaps, []
                for stale in owed:
                    _lifecycle.discharge_owed(stale)
            if self._sink_closed or alive:
                claimed, closing = False, self._closing
            else:
                self._sink_closed = True
                self._closing = threading.Event()
                claimed, closing = True, self._closing
        for stale in owed:
            _lifecycle.release(stale, detached=True)
        if not claimed:
            if closing is not None:
                closing.wait(_closer_grace(deadline))
            return
        try:
            self._close_sink()
        finally:
            with self._lock:
                self._closing = None
            if closing is not None:
                closing.set()

    def _close_sink(self) -> None:
        """Closes the sink, absorbing a failure.

        This runs after the join, so everything queued has already been drained and emitted:
        what is lost here is the sink's own cleanup, not events.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        try:
            _lifecycle.release(self.sink)
        except Exception as exc:
            _diag.absorbed("closing the sink", exc, "it may still hold its resources")

    def _queued_or_unknown(self) -> int:
        """Returns queued items, or zero where the platform does not implement ``qsize``.

        Items, not events: the queue holds one entry per submitted span plus any
        flush/shutdown marker.

        Args:
          None.

        Returns:
          The queue size, or 0 when it cannot be read.

        Raises:
          None. A diagnostic must not be the reason the diagnosis is lost.
        """
        try:
            return self._queue.qsize()
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
        in :meth:`Worker.shutdown` is what covers the paths ``shutdown`` never reaches — a
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

    def _emit(self, event_lists: list[list[dict[str, object]]]) -> None:
        """Flattens queued per-span event-lists into one batch and emits it, retrying.

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

        Args:
          event_lists: The accumulated per-span event-lists to flatten and emit.

        Returns:
          None. The outcome is not returned because ``failed_batches`` already moves exactly
          once per abandoned batch, and a second channel for the same fact could only disagree
          with the counter a waiting ``flush()`` is compared against (SPEC-021 FR-001).

        Raises:
          None.
        """
        batch = [event for events in event_lists for event in events]
        if not batch:
            return
        retries = max(self.max_retries, 0)
        for attempt in range(retries + 1):
            try:
                self.sink.emit(batch)
                return
            except Exception:
                if attempt >= retries:
                    with self._lock:
                        self.failed_batches += 1
                    _diag.lost(
                        "event", len(batch), f"batch abandoned after {retries + 1} emit attempts"
                    )
                    return
                self._stop.wait(min(0.01 * (2**attempt), 0.5))
