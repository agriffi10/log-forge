"""Sink-lifecycle facilities shared by both delivery paths (SPEC-033 FR-005)."""

from __future__ import annotations

import atexit
import collections
import contextlib
import enum
import gc
import os
import sys
import threading
import weakref
from itertools import islice
from time import monotonic
from typing import TYPE_CHECKING

from log_foundry import _diag, _fork, _sink_arg

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from log_foundry.results import FlushResult
    from log_foundry.sinks.base import Sink
    from log_foundry.worker import Health, Worker

DEFAULT_SHUTDOWN_TIMEOUT = 30.0
"""Seconds :meth:`Worker.stop` will wait for the drain thread (SPEC-027 FR-004).

Generous, because the ordinary case is a fast drain and expiring early would abandon events
that were about to be delivered. Bounded at all, because ``shutdown()`` runs from ``atexit``
and an unbounded join there is a hung process.

It lives here rather than in ``worker.py`` because the lifecycle owner binds it as a **def-time**
default (SPEC-040 FR-001), and a def-time default cannot come from the function-local import this
module uses to stay off ``worker``'s import path. ``worker.py`` re-exports it, so
``log_foundry.worker.DEFAULT_SHUTDOWN_TIMEOUT`` and the public ``log_foundry`` export are
unchanged.
"""

DEFAULT_SWAP_TIMEOUT = 5.0
"""Seconds a late ``configure(sink=...)`` will spend draining the previous sink (SPEC-030 FR-003).

Shorter than the shutdown budget on purpose: this runs on the caller's thread inside a
configuration call, where a long stall is a startup that appears to hang, and what is at risk
is a sink swap rather than the tail of the whole process.

Here rather than in ``worker.py`` for the reason :data:`DEFAULT_SHUTDOWN_TIMEOUT` is, and
re-exported the same way.
"""

DEFAULT_CLOSER_GRACE = 2.0
"""Seconds a shutdown gives an outstanding swapped-out close to finish.

Deliberately much smaller than the shutdown budget it is carved from. This is a last chance for
a close that is *nearly* done, not a second full attempt: it already had the swap's whole budget
(``DEFAULT_SWAP_TIMEOUT``) before ``shutdown`` was ever called, so one still running here is far
more likely stuck than slow, and every second spent on it is a second the process does not exit.
"""


class State(enum.Enum):
    """The process's lifecycle state, stored by the runtime and checked by every call (SPEC-078).

    SPEC-077 introduced it as an observer, derived from the flags and deciding nothing. SPEC-078
    makes it the authority: the settled part is stored in :attr:`Runtime._current`, written only
    by :meth:`Runtime._move` inside critical sections the calls already take, and the lifecycle
    calls take the answer the transition table gives for the column they find on entry
    (:meth:`Runtime._column`). The two transient states are read off the token sets, which is
    what keeps both writes free of a new lock acquisition.

    **``CLOSED`` keeps three arrows out, by decision** (SPEC-078 FR-004). After a ``shutdown()``
    that built no worker, a ``@trace`` call builds a live worker, delivers and reads ``RUNNING``
    (SPEC-044 FR-001 refused a permanent retirement fence). After a ``shutdown()`` that stopped a
    worker, the same call is counted in ``submitted_after_shutdown`` and the state stays
    ``CLOSED`` (SPEC-019, SPEC-030: reported, not restarted). An orphan log after either reaches
    the configured sink and arms it again (SPEC-032). Making ``CLOSED`` terminal would turn the
    first case's delivered events into ``submitted_after_shutdown``, which a minor release may not
    do. ``SWAPPING`` is entered from ``CLOSED`` as well as ``RUNNING``, when something is owed.

    The values are ``enum.auto()`` because nothing reads them, and a literal ``"draining"`` would
    be filed by ``tests/test_worker_predicate_roster.py`` as a worker question, its sentinel
    vocabulary including that word.

    Attributes:
      UNCONFIGURED: Nothing owed, built or retired. Stored, it is also the dispatch column of a
        process whose configured sink has resolved, which :attr:`Runtime.state` reports as
        ``RUNNING``.
      RUNNING: A live worker, or a sink owed a close, and not retired.
      SWAPPING: ``_swap_sink`` is inside its work.
      DRAINING: ``_shutdown_worker`` is inside its body, ``shutdown()`` and the exit drain alike.
      CLOSED: Retired, with no live worker.
    """

    UNCONFIGURED = enum.auto()
    RUNNING = enum.auto()
    SWAPPING = enum.auto()
    DRAINING = enum.auto()
    CLOSED = enum.auto()


class _Op(enum.Enum):
    """The six lifecycle calls the transition table has a row for (SPEC-078 FR-004).

    The values are ``enum.auto()`` for :class:`State`'s reason: a literal would be filed by the
    predicate roster if it ever matched one of its sentinel words.

    Attributes:
      BUILD: A span needs a worker and none exists (``_get_worker``'s inner check).
      ARM: An orphan log reaches a sink not owed (``_note_orphan_emit``'s locked branch).
      SWAP: ``configure(sink=...)``, through :func:`_swap_sink`.
      FLUSH: ``flush()``, through :func:`_flush_worker`.
      STOP: ``shutdown()`` or the exit drain, through :func:`_shutdown_worker`.
      FORK: A forked child's entry, :func:`_clear_after_fork`.
    """

    BUILD = enum.auto()
    ARM = enum.auto()
    SWAP = enum.auto()
    FLUSH = enum.auto()
    STOP = enum.auto()
    FORK = enum.auto()


class _Rule(enum.Enum):
    """How one close in flight is run and waited for (SPEC-078 FR-002).

    Today's three rules, gathered rather than changed: SPEC-030 keeps the live sink's close on the
    calling thread, SPEC-046 joins a shutdown's fan-out in full because a daemon that exit kills
    loses what it was flushing, and SPEC-030 FR-003 detaches a swapped-out close and grants it
    only the closer grace, since it already had the swap's whole budget.

    Attributes:
      INLINE: Closed on the calling thread.
      JOINED: A shutdown's fan-out thread, joined in full.
      GRACED: Detached; joined to the swap's budget by the swap, then to the shutdown's one grace.
    """

    INLINE = enum.auto()
    JOINED = enum.auto()
    GRACED = enum.auto()


class _Operation:
    """The slow half of a ``SWAP`` or a ``STOP``, and the token that holds its state (SPEC-078).

    Built once per call, before that call's first critical section, and added to
    :attr:`Runtime._swaps_running` or :attr:`Runtime._stops_running` inside it; discarded in the
    call's ``finally`` by one lock-free ``set.discard`` (SPEC-077 FR-002). Its ``deadline`` is
    computed here and nowhere else, and every slow step of the call reads what remains of it.

    It is built **before** the lock rather than inside it so that a call blocked behind
    ``Worker(_ensure_sink())``, which runs under ``_state._lock``, does not have its budget start
    only after that wait: the deadline is the instant the caller asked, as it was when the call
    computed a local.

    ``budget`` is the caller's timeout as given, kept only so :meth:`Worker.stop` can render it in
    its expiry line. The stop's join does not read it: it ends at the deadline
    :meth:`Worker.claim_stop` recorded from this operation.

    ``grace_end`` is written only by :meth:`_CloseRegistry._open_grace`, at the first graced wait
    of a ``STOP``, and every later graced wait of the same call ends at it, which is what makes
    the closer grace spent once per call (FR-002).

    ``drained`` records that a ``STOP`` has made its **last take** of the owed record, and it is
    what bounds the one changed cell of the transition table: a swap during a drain with no live
    worker declines only while some drain still has a take ahead of it, because only then does
    the drain own every owed close. It is written under ``_state._lock``, in the critical section
    where the last take becomes known — the second pass's take; the first pass's, when no
    bystander was waited on and no late worker can still be registered; or the section that
    reads the late worker, when there is none and nothing was waited on — and read by
    :meth:`Runtime._take_ahead` under the same lock. ``late_possible`` is whether a worker built
    now would still be registered as this call's late worker: true from a no-worker shutdown's
    entry until the section that lowers ``_shutdown_running``.

    ``decided`` is what a ``STOP`` waits on for a ``SWAP`` in flight (the fix after SPEC-079 for a
    shutdown landing mid-swap). A swap with a live worker retargets it and then, in a second
    critical section, arms the sink it installed and releases the one it displaced; a shutdown
    taking the owed record between the two closed the displaced sink and left the installed one
    unarmed, so ``shutdown()`` returned with it open. :func:`_swap_sink` sets the event once its
    last critical section has run, before it joins its closers, and again in its ``finally``, so
    no exit leaves it unset. ``thread`` is the ``threading.get_ident()`` of the call that built
    the token, which lets a shutdown on the swap's own thread skip its own token rather than
    wait on itself. ``grace_spent`` is how long a ``STOP`` waited on those events, which
    :meth:`_CloseRegistry._open_grace` deducts so the call still spends one closer grace at most.

    Attributes:
      kind: ``_Op.SWAP`` or ``_Op.STOP``.
      deadline: The ``time.monotonic()`` instant the caller's budget expires, or ``None``.
      budget: The caller's timeout as given, for a diagnostic only.
      grace_end: The instant every graced wait of this call ends, once one has started.
      drained: Whether this ``STOP`` has made its last take of the owed record.
      late_possible: Whether a worker built now would be this ``STOP``'s late worker.
      decided: Set once a ``SWAP`` has taken its decision, so a ``STOP`` may take after it.
      thread: The ident of the thread that built this token.
      grace_spent: Seconds this ``STOP`` spent waiting on swaps to decide, carved from its grace.
      unsettled: The swaps this ``STOP`` gave up waiting on and had not seen decide by its last
        take, for the one line that says so.
    """

    def __init__(self, kind: _Op, timeout: float | None) -> None:
        """Fixes the call's deadline from the caller's timeout, once.

        Args:
          kind: Which call this is the slow half of.
          timeout: The caller's timeout in seconds, or ``None`` for an unbounded call.

        Returns:
          None.

        Raises:
          None.
        """
        self.kind = kind
        self.budget = timeout
        self.deadline: float | None = None if timeout is None else monotonic() + timeout
        self.grace_end: float | None = None
        self.drained = False
        self.late_possible = False
        self.decided = threading.Event()
        self.thread = threading.get_ident()
        self.grace_spent = 0.0
        self.unsettled: list[_Operation] = []


class _Close:
    """One close in flight: the event a bystander waits on, its rule and its thread (SPEC-078).

    It holds **no sink**, as ``_Closing`` did not: an owned object holding a sink would hand any
    route by which the fork walk reached it superseded sinks to run fork hooks on. Since SPEC-079
    the walk does not reach :data:`_state` at all, and the child's :func:`_clear_after_fork`
    drops every in-flight entry, this event with it, so nothing in a child repairs or waits on it.
    The thread is not this package's, so holding it is safe.

    **It counts its participants.** A release that finds a close of the same sink already in
    flight joins that entry rather than starting a second (a wrapper forwarding the close of a
    child a swap is closing detached, say). The entry leaves the registry, and its event is set,
    only when the last participant finishes. Discharging it at the first finish would drop a
    still-running detached thread out of the shutdown's graced wait and out of
    ``health().closing_sinks`` while that close was still going on.

    Attributes:
      event: Set when the last participant in this close has finished, however it finished.
      rule: Which rule runs and waits for it.
      threads: Every thread performing it, one per detached participant, in the order they
        started; empty for an inline close and until one starts. A list rather than one slot so
        that a second participant's thread never overwrites the first's: the full join, the
        graced wait and ``health().closing_sinks`` each see all of them.
      participants: How many releases are performing this close; guarded by the registry's lock.
      hand_owed: Whether a worker build declined to hand this sink its stop signal while this
        close was in flight, so the participant that discharges the entry hands it instead
        (:meth:`_CloseRegistry.defer_hand`); guarded by the registry's lock.
    """

    def __init__(self, rule: _Rule) -> None:
        """Builds an unset event for one close.

        Args:
          rule: Which rule runs and waits for this close.

        Returns:
          None.

        Raises:
          None.
        """
        self.event = threading.Event()
        self.rule = rule
        self.threads: list[threading.Thread] = []
        self.participants = 1
        self.hand_owed = False


class _CloseRegistry:
    """Every close the library owes and every close in flight, with the moves between them.

    SPEC-078 FR-001. A close passed through three facts held in four places: **owed**
    (``Runtime._owed``), **in flight** (``_closing_now`` under its own leaf lock) and, for a
    detached close, **its thread** (``_closers`` under another), plus the fan-out threads
    :func:`_close_owed` kept in a local list. The moves between them were written out at each
    site, and the rule all of them keep — no instant at which a sink is neither owed nor in flight
    (SPEC-054 FR-003) — was restated at each. Here :meth:`take` removes a sink from ``owed`` and
    records its entry in one call made under ``_state._lock``, so the rule has one home.

    **Two locks guard it, by half.** ``owed`` is guarded by ``Runtime._lock``, which every caller
    that arms, takes or puts back already holds. The in-flight half is guarded by :attr:`_lock`, a
    leaf: :meth:`Runtime.held` and :meth:`Runtime.in_flight` are asked with ``_state._lock`` held
    and read it, and closer threads discharge it without ``_state._lock``. That leaf replaces two
    (``_closing_now_lock`` and ``_closers_lock``), which were both leaves under ``_state._lock``
    and never nested with each other, so no acquisition order changes. It is held across a dict
    read or a single mutation, never across a ``close()`` or a join.

    **An entry nothing discharges is permanent**, and three questions suffer from it: the sink is
    skipped by the signal refresh forever, the closer never takes it because :meth:`Runtime.held`
    stays true, and every later bystander waits out the whole grace on an event nobody will set.
    So every exit discharges, including a refused release and a closer thread that never started.
    A fork breaks the bracket — the child inherits entries whose ``finally`` no thread will run —
    so :func:`_clear_after_fork` empties the in-flight half in the child.

    Attributes:
      owed: Every sink the library owes a close, by ``id``; guarded by ``_state._lock``.
    """

    _FORK_SKIP = ("owed",)
    """Keeps the owed record, which pins superseded sinks, out of the repair walk's descent.

    It strongly references every sink still owed a close (SPEC-044 FR-005, SPEC-054 FR-002),
    including sinks the process abandoned several ``configure()`` calls ago. Without the opt-out the
    walk reaches them and runs their fork hooks: measured, a child of ``configure(A)`` → ``info()``
    → ``configure(B)`` ran ``reacquire_after_fork()`` on both, and a ``FileSink`` in that place would
    have its file re-opened on every fork. A live target is still reached through the config and
    through ``worker.sink``. ``_fork._skipped_names`` reads the opt-out off the holder of the
    attribute, which is this object since SPEC-078 moved the record here from
    :attr:`Runtime._FORK_SKIP`; a class attribute, for the reason that one is. Since SPEC-079 the
    walk starts from the runtime's live roots and :data:`_state` is not one, so this guards a
    route the walk does not take; the owed sinks seed the marking frontier instead, never hooked.
    """

    def __init__(self) -> None:
        """Builds an empty registry.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.owed: dict[int, Sink] = {}
        self._in_flight: dict[int, _Close] = {}
        self._lock = threading.Lock()

    def arm(self, sink: Sink) -> None:
        """Records that the library owes *sink* a close; the caller holds ``_state._lock``.

        Args:
          sink: The sink something has delivered to, or will.

        Returns:
          None.

        Raises:
          None.
        """
        self.owed[id(sink)] = sink

    def owes(self, sink: Sink) -> bool:
        """Whether the record owes *sink* a close right now; the caller holds ``_state._lock``.

        Identity rather than ``in`` or equality: the record is keyed by ``id``, and an ``id`` is
        reusable once its object dies, so only the stored object being this one says it is this
        sink that is owed.

        Args:
          sink: The sink a caller is about to take.

        Returns:
          Whether the record holds this very object.

        Raises:
          None.
        """
        return self.owed.get(id(sink)) is sink

    def take(self, sink: Sink, rule: _Rule) -> _Close:
        """Moves *sink* from owed to in flight in one call; the caller holds ``_state._lock``.

        Every caller takes only a sink it found owed, under the same lock: :func:`_close_owed`
        and the swap's stale loop take from the record's own values, and the swap's fenced
        branch asks :meth:`owes` first. A sink the record no longer owes was taken by somebody
        else, and its close is theirs. Taking one anyway is how a ``shutdown()`` landing between
        a swap's retarget and its decision had the displaced sink closed twice, once inline by
        the shutdown and once detached by the swap.

        Args:
          sink: The sink whose close the caller has decided to perform.
          rule: How that close is run and waited for.

        Returns:
          The in-flight entry the close discharges.

        Raises:
          None.
        """
        self.owed.pop(id(sink), None)
        return self._enter(sink, rule)

    def put_back(self, sink: Sink, close: _Close) -> bool:
        """Re-arms a sink whose close was never handed off, and discharges its entry.

        ``setdefault`` rather than assignment, so a re-arm that landed meanwhile keeps its own
        entry. The caller holds ``_state._lock``, so it must not pay a deferred hand here: it
        collects the answer and pays after releasing the lock (:func:`_close_owed`).

        Args:
          sink: The sink a take removed and no closer received.
          close: The entry that take made.

        Returns:
          What :meth:`finish` returned: whether a deferred hand is owed to the sink now.

        Raises:
          None.
        """
        self.owed.setdefault(id(sink), sink)
        return self.finish(sink, close)

    def register(self, sink: Sink, rule: _Rule) -> _Close:
        """Records a close with no take, which is how a wrapper's forwarded release reaches here.

        Args:
          sink: The sink about to be closed.
          rule: How that close is run and waited for.

        Returns:
          The in-flight entry the close discharges.

        Raises:
          None.
        """
        return self._enter(sink, rule)

    def _enter(self, sink: Sink, rule: _Rule) -> _Close:
        """Adds an in-flight entry, or joins the one already there as a further participant.

        A close already registered keeps its entry rather than gaining a second: two deciders
        cannot take one sink, because a take is under ``_state._lock`` and asks
        :meth:`Runtime.held` first, but a wrapper's forwarded release of a child can arrive while
        that child's own close is in flight. That release becomes a participant, so the entry
        outlives it while the first close is still running.

        Args:
          sink: The sink whose close is starting.
          rule: The rule for a new entry; an existing entry keeps its own.

        Returns:
          The entry this close discharges.

        Raises:
          None.
        """
        with self._lock:
            existing = self._in_flight.get(id(sink))
            if existing is not None:
                existing.participants += 1
                return existing
            close = _Close(rule)
            self._in_flight[id(sink)] = close
            return close

    def finish(self, sink: Sink, close: _Close | None) -> bool:
        """Discharges one participant in a close, and the entry with the last of them.

        The entry is removed only when it is still the one this caller was handed, so a sink
        re-armed and re-closed inside the window cannot have the wrong close discharged for it,
        and a forked child's clear followed by the forking thread's own late discharge removes
        nothing. Setting the event outside the lock is deliberate: a waiter woken by it goes on to
        take ``_state._lock``, and holding the leaf across that would invert the lock order.

        **It reports a hand a worker build declined** (:meth:`defer_hand`), read under the same
        leaf lock that discharged the entry, and only to the last participant, so exactly one
        caller pays it: the build either marked the entry before this removed it, and this reads
        the mark, or found no entry and handed the sink itself.

        Args:
          sink: The sink whose close has ended, however it ended.
          close: The entry this participant was handed, or ``None`` for a caller with none.

        Returns:
          Whether this call discharged the entry and the entry carried a declined hand, which the
          caller then pays with :func:`_hand_after_close` once it holds no lifecycle lock.

        Raises:
          None.
        """
        if close is None:
            return False
        with self._lock:
            close.participants -= 1
            done = close.participants <= 0
            if done and self._in_flight.get(id(sink)) is close:
                del self._in_flight[id(sink)]
            owed_hand = done and close.hand_owed
        if done:
            close.event.set()
        return owed_hand

    def defer_hand(self, sink: Sink) -> bool:
        """Marks a close of *sink* in flight as owing the sink its stop signal, if there is one.

        Asked by the worker build with ``_state._lock`` held, the order :meth:`in_flight` already
        uses, and the check and the mark are one acquisition of the leaf: an entry the build
        marks is discharged later by a :meth:`finish` that reads the mark, and an entry already
        discharged is not found, so the build hands the sink itself. No hand is lost between them.

        Args:
          sink: The sink the build is about to start a worker on.

        Returns:
          Whether a close of it was in flight, so the build hands nothing now.

        Raises:
          None.
        """
        with self._lock:
            close = self._in_flight.get(id(sink))
            if close is None:
                return False
            close.hand_owed = True
            return True

    def marked(self) -> bool:
        """Whether any close in flight carries a hand a worker build declined on it.

        The cheap gate :func:`_pay_marks_after_stop` asks before it takes ``_state._lock``, so a
        ``shutdown()`` that left no mark behind — every one but a build in a stop's tail — adds
        no acquisition of that lock to its pinned count. One acquisition of the leaf.

        Args:
          None.

        Returns:
          Whether some in-flight entry has :attr:`_Close.hand_owed` set.

        Raises:
          None.
        """
        with self._lock:
            return any(close.hand_owed for close in self._in_flight.values())

    def unmark(self, sink: Sink) -> bool:
        """Clears the declined-hand mark on the close of *sink* in flight, if it carries one.

        The caller pays the hand itself, so the discharge that ends that close later finds the
        mark gone and pays nothing: under the leaf lock, a mark is consumed exactly once, by this
        or by :meth:`finish`.

        Args:
          sink: The sink whose close in flight may carry a mark.

        Returns:
          Whether a mark was there and is now cleared.

        Raises:
          None.
        """
        with self._lock:
            close = self._in_flight.get(id(sink))
            if close is None or not close.hand_owed:
                return False
            close.hand_owed = False
            return True

    def in_flight(self, sink: Sink) -> bool:
        """Whether a close of *sink* is in flight on some thread right now.

        Args:
          sink: The sink whose close is in question.

        Returns:
          Whether an entry for it is registered.

        Raises:
          None.
        """
        with self._lock:
            return id(sink) in self._in_flight

    def in_flight_ids(self) -> set[int]:
        """The ids of every sink whose close is in flight, for ``_repin``'s strong set (SPEC-081).

        One acquisition of the leaf, taken with no other library lock held, so the re-pin never
        nests it under the record lock.

        Args:
          None.

        Returns:
          A snapshot of the in-flight ids.

        Raises:
          None.
        """
        with self._lock:
            return set(self._in_flight)

    def attach(self, close: _Close, thread: threading.Thread) -> None:
        """Records a thread performing a close, once it has started, beside any already there.

        Appended, never assigned: a detached release that joins an entry another participant is
        already performing on a thread of its own must not overwrite that thread, or the full
        join, the graced wait and ``health().closing_sinks`` would stop seeing a close that is
        still running.

        Args:
          close: The entry the thread discharges.
          thread: The started thread.

        Returns:
          None.

        Raises:
          None.
        """
        with self._lock:
            close.threads = [*close.threads, thread]

    def bystanders(self, mine: set[int]) -> list[_Close]:
        """Every close in flight that the caller did not take, for its bystander wait.

        Captured at the take, in the caller's critical section, rather than after its own closes:
        SPEC-050 FR-002's rule is that a caller waits for a close somebody else had in flight when
        it arrived, and a re-read later would also wait on closes that started afterwards.

        Args:
          mine: The ids of the sinks the caller took.

        Returns:
          The entries the caller must wait for.

        Raises:
          None.
        """
        with self._lock:
            return [close for key, close in self._in_flight.items() if key not in mine]

    def join_joined(self, closes: list[_Close]) -> None:
        """Joins every ``JOINED`` close's thread **in full** (SPEC-046).

        Not capped by the grace: SPEC-046 built the capped version and measured it losing data —
        four owed 2-second closes against ``shutdown(timeout=1.0)`` completed one — so a
        shutdown's own closes cost the slowest of them and none is abandoned to a daemon that exit
        kills. ``architecture.md`` §13 records the unbounded wait.

        Args:
          closes: The entries whose threads the caller started.

        Returns:
          None.

        Raises:
          None.
        """
        for close in closes:
            for thread in close.threads:
                thread.join()

    def _open_grace(self, op: _Operation) -> float:
        """Fixes the instant every graced wait of *op* ends, at its first graced wait.

        The one writer of :attr:`_Operation.grace_end`. At ``fda134a`` two waits computed the grace
        independently — the bystander wait and ``join_closers`` — and a swapped-out close the
        shutdown did not take was waited on by both: ``shutdown(timeout=30)`` measured 4.00 s
        against a 2.0 s grace. Fixed once per call, the second wait gets only what is left.

        **What the call already spent waiting on swaps is deducted**, :attr:`_Operation.grace_spent`
        (see :func:`_await_swap_decisions`). That wait runs before the drain and comes out of
        the same grace, so a call that waited on a swap and then on a detached close still spends
        one grace at most (SPEC-078 FR-002). It is deducted here rather than by opening the grace
        before the drain, because an end fixed then would charge the drain's own time to it and
        starve the graced waits that follow; a call that waited on no swap deducts nothing.

        Args:
          op: The ``STOP`` whose graced waits share one end.

        Returns:
          That end, as a ``time.monotonic()`` instant.

        Raises:
          None.
        """
        if op.grace_end is None:
            left = max(0.0, closer_grace(op.deadline) - op.grace_spent)
            op.grace_end = monotonic() + left
        return op.grace_end

    def wait_graced(self, op: _Operation, closes: list[_Close] | None) -> None:
        """Waits on closes the caller did not perform, all ending at the call's one grace.

        Two callers. :func:`_close_owed` passes the bystander entries it captured at its take —
        closes somebody else had in flight, whatever their rule. :func:`_shutdown_worker` passes
        ``None`` at its end, which means every ``GRACED`` close in flight with a thread still
        alive; that is the last chance a detached close gets before the process exits. A list with
        nothing in it spends no grace and sets no ``grace_end``.

        **Every wait is on the entry's event, never on a thread.** The event is set when the
        **last** participant finishes, so a close a forwarded release is still performing after
        the detached thread has ended is still waited for; joining the thread returned through
        it.

        Args:
          op: The ``STOP`` this wait belongs to.
          closes: The entries to wait on, or ``None`` for every live ``GRACED`` close.

        Returns:
          None.

        Raises:
          None. A wait that expires abandons the close to its daemon, which is the contract of a
            detached close, not a failure.
        """
        if closes is None:
            with self._lock:
                closes = [
                    close
                    for close in self._in_flight.values()
                    if close.rule is _Rule.GRACED
                    and any(thread.is_alive() for thread in close.threads)
                ]
        if not closes:
            return
        end = self._open_grace(op)
        for close in closes:
            close.event.wait(max(0.0, end - monotonic()))

    def graced_alive(self) -> int:
        """Counts the detached closes running right now, backing ``Health.closing_sinks``.

        A live fact rather than an inference from a timeout: an expired join reports nothing, since
        a slow close and a stuck one cannot be told apart at that moment, so this gauge is what an
        operator reads instead. ``JOINED`` fan-out closes are not counted, as they never were.

        Args:
          None.

        Returns:
          The number of ``GRACED`` entries with a thread still alive.

        Raises:
          None.
        """
        with self._lock:
            return sum(
                1
                for close in self._in_flight.values()
                if close.rule is _Rule.GRACED and any(thread.is_alive() for thread in close.threads)
            )


def _remaining(deadline: float | None) -> float | None:
    """What is left of a deadline, as a timeout, or ``None`` for an unbounded one.

    Args:
      deadline: A ``time.monotonic()`` instant, or ``None``.

    Returns:
      The seconds until it, never negative, or ``None``.

    Raises:
      None.
    """
    return None if deadline is None else max(0.0, deadline - monotonic())


class _Entry:
    """One ownership-record entry: who acquired a sink, and how the record holds it (SPEC-081).

    Written under :attr:`Runtime._owned_lock` and read through a snapshot. The entry holds its sink
    **strongly** only while the library still holds the sink — the sink is reachable from a live
    delivery root, owed a close, or being closed — and **weakly** afterwards, so a superseded sink
    is collected once the application drops it (FR-001). An entry naming another process, which in
    a child is every inherited one, is held strongly for the child's life (FR-004).

    **Its shape never tears.** ``weak`` is created once, when the entry is written, carries no
    callback (a callback runs inside the collector, on whatever thread allocates), and is kept for
    the entry's life; only ``strong`` toggles between the sink and ``None``. A fork landing in the
    middle of a write can therefore see a stale ``strong``, never an entry with neither reference.
    A sink whose type cannot be weakly referenced — no ``__weakref__`` slot, a ``weakref.proxy``
    among them — has ``weak`` ``None`` and is held strongly for life; creating the reference runs
    none of the sink's code either way.

    **A dead entry answers for nothing** (FR-003): an ``id`` is free for reuse the moment its
    object dies, so an entry whose :meth:`referent` is gone, or is not the object asked about, is
    read as absent by every reader and overwritten by every writer, and pruned by ``_repin``.

    Attributes:
      pid: ``os.getpid()`` at the write, or ``_FOREIGN``.
      strong: The sink while the record pins it, otherwise ``None``.
      weak: A weak reference to the sink, or ``None`` when it cannot be weakly referenced.
      closed: Whether a close this process performed on the sink has finished, returned or raised.
      captured: Whether the capture before a demotion wrote this entry, rather than a stamp, a
        reclaim or the fork walk.
      epoch: ``Runtime._owned_clock`` at this entry's last stamp or reclaim.
    """

    __slots__ = ("captured", "closed", "epoch", "pid", "strong", "weak")

    pid: int
    strong: object | None
    weak: weakref.ReferenceType[object] | None
    closed: bool
    captured: bool
    epoch: int

    def __init__(
        self, sink: object, pid: int, epoch: int, *, strong: bool, captured: bool = False
    ) -> None:
        """Records one sink, weakly where its type allows and strongly when asked or forced to.

        Args:
          sink: The sink being recorded.
          pid: The acquiring process, or ``_FOREIGN``.
          epoch: The record's epoch at this write.
          strong: Whether the record pins the sink now; a sink that cannot be weakly referenced is
            pinned whatever this says.
          captured: Whether the capture before a demotion is writing it.

        Returns:
          None.

        Raises:
          None.
        """
        weak: weakref.ReferenceType[object] | None
        try:
            weak = weakref.ref(sink)
        except TypeError:
            weak = None
        self.weak = weak
        self.strong = sink if strong or weak is None else None
        self.pid = pid
        self.epoch = epoch
        self.closed = False
        self.captured = captured

    def referent(self) -> object | None:
        """Returns the recorded sink while it is alive, the strong reference first.

        Args:
          None.

        Returns:
          The sink, or ``None`` once a weakly held sink has died.

        Raises:
          None.
        """
        if self.strong is not None:
            return self.strong
        weak = self.weak
        return None if weak is None else weak()


class Runtime:
    """The process's delivery lifecycle: one owner, one state, three locks (SPEC-040, SPEC-078).

    The state below lived as seven loose module globals in ``decorator.py``, and seven pieces of
    shipped work came out of that one fact — SPEC-030, SPEC-031 FR-006, SPEC-033, two SPEC-035
    regressions, SPEC-035 FR-002's roster, and SPEC-039's forked-child rebuild. Every one asked
    the same thing at a different site: *who owns the worker or the sink at this instant, and what
    may this path therefore do?*

    **The states.** A process is in exactly one :class:`State`, and the fields that decide it are
    named. SPEC-040's four ownership states are the settled three of these, by the same fields:

    - ``UNCONFIGURED`` (SPEC-040's **cold**) — ``_worker is None``, nothing owed and nothing
      retired. Nothing has been logged, no thread exists, no sink is owed a close. ``configure()``
      alone does not leave it as far as the runtime is concerned: it runs ``_ensure_sink()``
      unconditionally, so a resolved sink is not evidence anything was written to it (SPEC-031
      FR-006). :attr:`state` reports such a process as ``RUNNING``, because the config has a sink
      to deliver to; the stored value and the dispatch column stay ``UNCONFIGURED``, because the
      config is not the runtime's to read under its lock.
    - ``RUNNING`` — either **orphan-only** (the owed record names every sink a level call with no
      span reached, and ``_worker`` is still ``None``; the ``atexit`` handler is armed by the emit
      that landed rather than by the sink existing), or **worker-backed** (``_worker`` holds a
      live process worker, which owns the drain; the sink's close and the stop signal are this
      owner's on both paths, SPEC-054). A mixed process passes through orphan-only first, and one
      record is what keeps that exactly one ``close()``.
    - ``CLOSED`` (SPEC-040's **retired**) — ``retirements`` is above zero and no worker is live:
      every worker built before the last ``shutdown()`` reads as no longer live with it. A retired
      worker never restarts: a later log through it is accepted, reported and refused at the
      closed sink rather than prevented (SPEC-030). Retirement is not terminal for the process,
      though: a ``@trace`` after a ``shutdown()`` that found no worker builds a live worker
      (SPEC-044 FR-001), so ``CLOSED`` returns to ``RUNNING`` — the decision :class:`State`
      records.
    - ``SWAPPING`` and ``DRAINING`` — a :func:`_swap_sink` with work, or :func:`_shutdown_worker`,
      is inside its body; read off the token sets.

    **The transitions** are the table SPEC-078 FR-004 states, six calls by five states.
    ``_get_worker`` builds once, under the lock, from any settled state. ``_note_orphan_emit``
    arms a sink. ``_swap_sink`` retargets within whichever state holds and re-points the record
    rather than clearing it, because clearing leaks the new sink in a process that swaps and
    exits; during a drain with no live worker it declines, because the drain owns every owed close.
    ``_shutdown_worker`` takes any state to ``CLOSED`` and is idempotent on both paths. A forked
    child re-enters in the state its parent held — a retired parent forks a retired child.
    :attr:`_current` is moved by :meth:`_move` alone, at the end of the four critical sections
    that can change it.

    **The four questions** (``architecture.md`` §9.2) are the methods below, one each, and a
    caller *selects* one rather than composing a predicate. Answering with the wrong one is this
    codebase's most repeated defect: three reviewers each named a different site, each was fixed,
    and a fourth shipped broken.

    **None of the questions takes the lock**, and that is load-bearing rather than an omission.
    :func:`_get_worker`'s inner check, :func:`_close_owed`, :func:`_swap_sink` and
    :func:`_will_hand`'s callers all hold ``_lock`` when they ask, so a non-reentrant
    acquire inside a question would deadlock them; and :func:`_get_worker`'s **outer** check is
    deliberately unlocked on the ``@trace`` hot path, where a lock would serialize every span
    flush in the process. Each read is a single reference load, which is atomic — a caller
    needing consistency across two reads takes ``_lock`` itself. :attr:`state` is a reader under
    the same rule; :meth:`_column` and :meth:`_move` are asked with ``_lock`` held.

    **Three locks, not one.** :attr:`_lock` guards the worker, the owed record, the count and
    the stop event; the close registry's leaf lock guards the closes in flight, and
    :attr:`_owned_lock` the ownership record. SPEC-078 merged two leaves into the registry's one
    (``_closing_now_lock`` and ``_closers_lock`` never nested with each other), which changed no
    acquisition order. Folding the leaf into ``_lock`` would deadlock :meth:`in_flight` and
    :meth:`held`, which are asked with ``_lock`` held and take that leaf by design, and closer
    threads discharge it without ``_lock``. The order is ``_lock`` then ``_config_lock`` then
    ``_owned_lock``, and ``_lock`` then ``Worker._lock`` and the registry's lock as leaves, with
    nothing acquiring ``_lock`` beneath any of them. ``config._sink_lock`` sits above them all: a
    ``configure()`` that changes the sink holds it from the config write to the end of
    :func:`_swap_sink`, and nothing holding any of these takes it.

    **The record's lock is re-entrant, the only one of the three that is** (SPEC-081 FR-003).
    Since an entry holds a sink the library no longer holds only weakly, a collection can run a
    sink's finalizer on a thread inside a record section, and a finalizer that forwards a close
    into a library wrapper reaches :func:`releasable` and :func:`release` on that same thread. Each
    section under it iterates a snapshot, so a re-entrant write is safe. That path is the one place
    the order can invert: such a close whose entry carries a hand a worker build declined reaches
    :func:`_hand_after_close`, which takes ``_lock`` with ``_owned_lock`` held (``architecture.md``
    §13, a recorded residual).

    **No sink's stop-signal setter runs under** :attr:`_lock`. A setter is the sink's own code
    and may call back into the library on the same thread, which a non-reentrant lock turns into
    a self-deadlock: measured at ``624d817f``, a setter calling ``configure()``, ``info()``,
    ``flush()`` or ``shutdown()`` hung the process on every path that handed a signal. So each
    such path decides under the lock and hands after releasing it (:func:`_hand_out`), with a
    hand token (:attr:`_hands_running`) covering what the deciding section published.

    Args:
      None.

    Returns:
      None.

    Raises:
      None.
    """

    _FORK_SKIP = ("_owned",)
    """Keeps the ownership record, which can pin superseded sinks, out of the repair walk's descent.

    ``_owned`` references every sink this process acquired that is still alive (SPEC-042 FR-001,
    SPEC-081), strongly while the library holds it and weakly after, and in a forked child it
    holds every inherited one strongly, including sinks the parent abandoned several
    ``configure()`` calls before the fork. Without the opt-out the walk
    reaches them and calls their fork hooks — measured, a child of ``configure(A)`` → ``info()`` →
    ``configure(B)`` ran ``reacquire_after_fork()`` on both, a child announced a buffer discard for
    a superseded sink, and a ``FileSink`` in that place would have its file re-opened on every fork
    forever. A **live** target is still reached by the walk through the config and through
    ``worker.sink``, so nothing the child must repair is hidden by this. The owed record, the
    other one that pins such sinks, moved to the close registry in SPEC-078 and carries its own
    opt-out, :attr:`_CloseRegistry._FORK_SKIP`, because ``_fork._skipped_names`` reads the opt-out
    off **the holder of the attribute**, statically, so it must be declared where the record
    lives.

    **Since SPEC-079 it guards a route the walk does not take.** The child's one walk starts
    from the runtime's live roots, and :data:`_state` is not one: its primitives are on
    ``_fork._GLOBAL_PRIMITIVES``. It stays as the guard for any other route by which the walk
    might reach :data:`_state`. A **class** attribute deliberately, read through
    ``sinks.base._static_value``, which finds a class attribute as ``getattr`` would.

    Marking is not narrowed: :func:`_record_seeds` reads the ownership and owed records directly,
    so the marking frontier still reaches an inherited superseded sink that is still a seed, never
    hooked, and it is still refused (SPEC-042 FR-001) — on the stamp ``configure()`` left, since
    :func:`_walk_after_fork` leaves an existing live record alone rather than writing ``_FOREIGN``
    over it — or on ``_FOREIGN``, for a sink held inside it that the bounded stamp walk never
    reached.
    """

    def __init__(self) -> None:
        """Builds the process's one lifecycle owner, in the cold state.

        Every primitive is assigned to ``self`` rather than built at module scope, which is what
        lets a forked child replace it (SPEC-039 FR-002): since SPEC-079 the four this instance
        holds are on ``_fork._GLOBAL_PRIMITIVES``, which writes back at an instance attribute,
        and an AST lint enforces that shape.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self._worker: Worker | None = None
        self._lock = threading.Lock()
        self._atexit_registered = False
        self._stop = threading.Event()
        self.retirements = 0
        self._shutdown_running = 0
        self._late_worker: Worker | None = None
        self._current = State.UNCONFIGURED
        """The settled state the runtime owns: ``RUNNING``, ``CLOSED`` or ``UNCONFIGURED``.

        SPEC-077's rules 3 to 6 without the configured-sink term, written only by :meth:`_move`
        under :attr:`_lock`. A forked child inherits it as the parent held it, which is exact:
        the fields it is a function of are inherited too.
        """
        self._closes = _CloseRegistry()
        """Every close the library owes and every close in flight (SPEC-078 FR-001).

        Replaces ``_owed``, ``_closing_now``, ``_closing_now_lock``, ``_closers`` and
        ``_closers_lock``. Walked by the fork repair as an owned holder: its leaf lock and every
        entry's event are replaced, and its owed record is opted out by
        :attr:`_CloseRegistry._FORK_SKIP`.
        """
        self._signal_owed = False
        """Whether an owed sink may hold a stop signal other than the process's own.

        Set by :func:`_clear_after_fork`, because the roster gives a child a fresh ``_stop`` while a
        sink no live worker delivers to still holds the parent's copy, and by
        :func:`_settle_signal` when a hand could not settle within :data:`_SETTLE_ROUNDS`. Settled
        by :func:`_owed_candidates`, which carries the reasoning, on the next orphan log or
        ``flush()``: the hand is deferred to a call that can run a sink's code. It is cleared in
        the deciding section, after any token that section opens, so a thread that forks from
        inside a setter leaves its child's flag, which the child's handler set, alone.
        """
        self._hands_running: set[object] = set()
        """One token per stop-signal hand whose deciding section published something.

        Non-empty sends every orphan emit to the locked branch of :func:`_note_orphan_emit`, which
        hands the sink its signal itself before it emits, so no thread reaches a sink that a
        section has just armed, or whose signal it has just replaced, before the sink holds the
        signal. A token is added under :attr:`_lock` **before** the section publishes the arm, the
        refreshed ``_stop`` or the cleared :attr:`_signal_owed`, and the fast path reads it
        **after** those fields, so a reader that sees a publication sees the token too. It is
        discarded without the lock, by one ``set.discard``, in the hand's ``finally``: the
        mechanism :attr:`_swaps_running` documents, with the same ``python.md`` §13 exception. A
        section that publishes nothing opens none: a token per hand made every emit that met one
        hand again under a token of its own. Measured by
        ``tests/test_reentrant_stop_signal.py``'s concurrency case (8 threads of 300 logs, a
        0.5 ms setter), a token per hand ran the setter 2399 times; opening one only on a
        publication, 8 or 9. A forked child empties it in :func:`_clear_after_fork`.
        """
        self._handing_here = threading.local()
        """Which sinks this thread is handing a stop signal to, as ``ids``: ``id`` to depth.

        Read by :func:`handing_here`, the one test of it, so a setter that logs, or builds the
        worker, on the thread already handing it that sink is not handed it again, which would
        recurse. Per thread, so a forked child keeps the forking thread's entries, which that
        thread's own ``finally`` removes. A ``threading.local`` rather than the ``contextvars``
        ``python.md`` §13 prefers, because what it records is a call on this thread's stack: a
        hand is synchronous and never crosses an ``await`` or a context copy, so the question is
        "is this thread inside that call right now", not context propagation. A context variable
        would also be inherited by a task or thread started from inside the setter, which is not
        inside the hand.
        """
        self._releasing_here = threading.local()
        """How many performed releases this thread is inside, as ``depth`` (SPEC-081 FR-001).

        Read by :func:`release` so that only the outermost one on a thread re-pins the ownership
        record: a release a wrapper forwards from inside another release's ``close()`` leaves the
        re-pin to it. Per thread, for :attr:`_handing_here`'s reason: what it records is a call on
        this thread's stack. A forked child starts at zero (:func:`_clear_after_fork`): a child
        forked from inside a ``close()`` — a fork-context ``multiprocessing.Process`` started there
        — may leave through ``os._exit`` without unwinding, and an inherited depth would then keep
        it from ever re-pinning. A forking thread that does unwind in the child restores each
        level's saved depth on the way out, so it still ends at zero.
        """
        self._freeze_floor = gc.get_freeze_count() if sys.version_info < (3, 13) else 0
        """The freeze count of a process that never called ``gc.freeze()`` (SPEC-081 FR-005).

        :func:`_mark_heap` reads a heap as frozen only above it. On CPython 3.12 the collector moves
        immortal objects into the permanent generation, so ``gc.get_freeze_count()`` reads a few
        hundred with no freeze anywhere — measured 375 tuples, unchanged by later imports, fifty
        collections, allocation churn and collections on eight threads, back to the same value
        after ``gc.unfreeze()`` and one collection, and about 27,000 after a real ``gc.freeze()``.
        So 3.12 reads its floor once, when this module is imported. On 3.13 and later the count is
        zero without a freeze, so the floor is zero and every freeze is seen. What 3.12 cannot see
        is a freeze made **before** this package was imported, which the floor then absorbs: a
        tripped child of such a process scans a heap that hides the frozen objects (a recorded
        residual, ``architecture.md`` §13). Inherited by a child as it stands.
        """
        self._building_here = threading.local()
        """The sink-factory builds whose factory is running on this thread, as ``yielders``.

        Each entry is a factory wrapper's way to release the threads waiting on that build
        (:func:`building`, :func:`yielding`). Per thread, for the reason :attr:`_handing_here`
        gives: what it records is a call on this thread's stack, the factory call, and a thread or
        task the factory starts is not inside it. A forked child keeps the forking thread's
        entries, which that thread's own ``finally`` removes.
        """
        self._marking_failed = False
        """Whether this child cannot tell what it inherited, so it trusts no unrecorded sink.

        Set when the child's walk raised, or when its marking tripped :data:`_MARKING_CEILING` and
        the heap scan that falls back for it failed or met a frozen heap (SPEC-081 FR-005, FR-006).
        An unrecorded sink in such a child may be one it missed rather than one it built, so
        :func:`releasable` refuses an unrecorded sink, and :func:`stamp` records a sink it is
        handed that has no record as ``_FOREIGN``, unless it is a root the library built: a leaked handle rather than a closed
        transport, the direction SPEC-042 FR-001 requires a gap to fail in. Inherited by a
        grandchild, whose walk then scans its heap and clears the flag when that scan succeeds.
        """
        self._owned: dict[int, _Entry] = {}
        """Which sinks this process acquired, keyed by ``id``, with the pid that acquired each.

        **No record means refused** (FR-001), and that default is the whole mechanism: a sink the
        library was never handed was never its to release, so every gap fails toward a leaked handle
        rather than toward closing a transport another process is still using.

        **An entry pins its sink only while the library still holds it** (SPEC-081 FR-001): while
        the sink is reachable from a live delivery root, owed a close or being closed. After a
        performed close, or a supersession with nothing owed, the entry keeps only a weak reference,
        so a superseded sink is collected once the application drops it; before it goes weak, the
        sinks beneath it are recorded (``_capture``). The two reasons the reference used to be
        strong for life are kept another way. An ``id`` is reusable the moment its object dies, so
        an entry whose sink is dead, or is not the object asked about, answers for nothing
        (:func:`_entry_for`) and is pruned by ``_repin``. A collected sink can close itself from a
        finalizer, which in a forked child would be a destructive close, so a child pins every
        inherited entry whose sink is alive before anything else runs (``_pin_inherited``).
        """
        self._owned_lock = threading.RLock()
        """Guards :attr:`Runtime._owned`, and is the **last** lock in the process's order.

        ``_state._lock`` → ``_config_lock`` → this, never the reverse in any pair (FR-001 AC-12).
        The three-term form is the real one: ``_get_worker`` calls ``config._ensure_sink()`` while
        holding ``_state._lock``, and ``_ensure_sink``'s construction branch takes ``_config_lock``
        before it can stamp. The criterion states the two-term version, which is true but skips the
        middle term.

        **Re-entrant** (SPEC-081 FR-003): a sink the record holds only weakly can be collected
        while a thread is inside a section, and a finalizer forwarding a close into a library
        wrapper then reaches :func:`releasable` and :func:`release` on that thread; with a plain
        ``Lock`` that thread hung. Every section iterates a snapshot of :attr:`_owned`, so a
        re-entrant write during it is safe. On the roster, where ``_fork._fresh_primitive`` mints
        an ``RLock`` for it.

        The orphan logging path is the opposite constraint and takes **no** lock at all: it reaches
        ``_ensure_sink``'s fast-path return once per event, which must never stamp (AC-10).
        """
        self._owned_clock = 0
        """Bumped by every :func:`stamp` and :func:`reclaim`, and written on the entries they touch.

        ``_repin`` reads it when it begins and demotes only an entry whose epoch is no later, so a
        sink handed over while a re-pin is walking is never demoted by it; :func:`release` compares
        an entry's epoch across a close, so a sink handed over again during its own close is left
        unclosed in the record (SPEC-081 FR-001). Inherited by a child as it stands.
        """
        self._stamp_refusal_announced = False
        """Whether the line for a ``sink=`` a last-resort child refused was written (FR-006).

        Once per process: reset by :func:`_walk_after_fork` in each child.
        """
        self._capture_announced = False
        """Whether the line for a capture that stopped short was written (SPEC-081 FR-002).

        Once per process, for a trip and a raise together: reset by :func:`_walk_after_fork`.
        """
        self._repin_announced = False
        """Whether the line for a re-pin that raised was written (SPEC-081 FR-001).

        Once per process: reset by :func:`_walk_after_fork` in each child.
        """
        self._swaps_running: set[_Operation] = set()
        """One token per ``_swap_sink`` call inside its work; non-empty reads ``SWAPPING``.

        A set of tokens rather than a counter or a flag, for ``_shutdown_running``'s reason: two
        swaps can still be inside at once, and a boolean cleared by the first to finish would
        misreport the second. ``config._sink_lock`` serializes two ``configure(sink=...)``
        calls, but a call whose wait for it outlasted its budget proceeds alongside the
        holder. A token is the call's
        :class:`_Operation` (SPEC-078; a bare ``object()`` before it), **added**
        under ``_lock`` in a critical section the call already has and **discarded without the
        lock** in its ``finally`` — one ``set.discard``, which CPython performs atomically, where an
        ``int -= 1`` outside the lock is a read-modify-write that can lose a decrement. That is
        what keeps the observer from adding a lock acquisition to any call (SPEC-077 FR-002).
        ``python.md`` §13 says not to rely on a builtin's atomicity across threads; this is the
        deliberate exception. Since SPEC-078 a library call does read the stop tokens — the swap's
        decline asks :meth:`_take_ahead` under ``_lock`` — and a discard that lagged would make it
        see a drain that had already made its last take, which ``drained`` already answers. A
        forked child empties it in :func:`_clear_after_fork`.
        """
        self._stops_running: set[_Operation] = set()
        """One token per ``_shutdown_worker`` call inside its body; non-empty reads ``DRAINING``.

        The same mechanism as :attr:`_swaps_running`, for two concurrent ``shutdown()`` calls; the
        exit drain is the same function with the same default budget, so it is the same state.
        """

    def worker_exists(self) -> Worker | None:
        """Existence — is there a worker at all, and therefore anything to do (arch §9.2).

        The weakest of the four: it says only that this process built a worker, never that the
        worker still delivers or that it owns any particular sink. A retired worker is still the
        process worker, which is why rebuilding one would fight a process trying to exit
        (SPEC-019) and why :func:`_shutdown_worker`, :func:`_flush_worker` and
        :func:`_worker_health` all answer from it rather than from :meth:`live_worker`.

        Takes no lock; see the class docstring for why that is a requirement rather than a
        default.

        Its own definition is the one of the four the roster does not file, because the body is a
        bare ``return self._worker`` and ``_boolean_positions`` excludes a plain return by design
        — the value is handed to a caller who asks their own question. That is not a hole: every
        rewrite that would change this question's category introduces a boolean the walker does
        see, an ``IfExp`` test or a call to :meth:`live_worker`.

        Args:
          None.

        Returns:
          The process worker if one was ever built, retired or not, otherwise ``None``.

        Raises:
          None.
        """
        return self._worker

    def live_worker(self) -> Worker | None:
        """Liveness — who *performs* an action, and a retired worker performs nothing (arch §9.2).

        Reading the retirement count in order to *report* it, as :func:`_worker_health` does, is
        this same question asked for a different purpose rather than a fifth category.

        A worker is live while :attr:`retirements` has not moved past its ``_epoch`` — the count
        as it stood at that worker's build (SPEC-054 FR-001). Spelled ``>`` here and at the five
        reads in ``worker.py``, deliberately one way: ``!=`` is equivalent **only** because the
        count is monotonic, and the only thing making it monotonic is that this module contains
        one ``= 0`` at init and two ``+= 1`` in :func:`_shutdown_worker`. That is a property of
        the current source rather than of the design, so the comparison does not rest on it. A **count** rather than a latch, because a
        latch changes what ``submitted_after_shutdown`` means: a worker built after a
        ``shutdown()`` returned still delivers (SPEC-044 FR-001), and against a latched boolean
        every event it delivered would be counted as queued where nothing will drain it. Against
        the count, that worker's epoch equals the count until the *next* ``shutdown()`` moves it,
        which is exactly when its submissions start being stranded.

        A retired worker holds its sink forever — :meth:`Worker.retarget` declines once
        shut down — so keying on a worker merely existing hands the swap to something that will
        do nothing with it, and the sink adopted afterwards is closed by no one: measured,
        ``configure(A)`` → ``@trace`` → ``shutdown()`` → ``configure(B)`` → ``info()`` →
        ``configure(C)`` left B unclosed with its event undelivered and every counter clean
        (SPEC-033 FR-002).

        :func:`_close_owed` deliberately does **not** use this: there a retired worker's thread
        being *alive* is exactly what must make the take decline, since an expired shutdown leaves
        it possibly still inside that sink's ``emit``. That is :meth:`held`, not liveness.

        Takes no lock. :func:`_swap_sink` asks it under ``_lock``; :func:`_flush_live_sink` asks
        it under no lock at all, which is sound because it consumes the answer immediately and
        holds nothing across it.

        Args:
          None.

        Returns:
          The worker while it is still delivering, or ``None`` when there is none or it retired.

        Raises:
          None.
        """
        worker = self._worker
        return None if worker is None or self.retirements > worker._epoch else worker

    def refresh_stop_signal(self) -> threading.Event:
        """Returns the process's stop signal, replacing it first if it is already set.

        An ``Event`` is set once and never cleared, and ``sinks/_retry.wait`` returns
        immediately on a set one — so a sink handed the shutdown's event would have every
        later backoff collapsed to zero, which against a rate-limited destination is a tight
        retry loop. SPEC-027's contract is "cut short by a shutdown", not "never wait again".

        The signal is one object for the whole process (SPEC-054 FR-001): the worker's drain
        loop, its retry waits and every sink's backoff wait on the same event, and a worker is
        handed it at its build rather than building one of its own. A worker built after a
        ``shutdown()`` returned is handed a **fresh** one for the same reason a sink adopted then
        is — it delivers, and a set event would collapse every backoff to zero. A worker already
        holding the set event keeps it, since a retired worker is never restarted (SPEC-019), so
        the replacement can only ever reach a later deliverer. **One worker holding a set event
        is restarted** (SPEC-068 FR-005): a worker stopped by a direct ``Worker.stop()`` never
        moved the retirement count, so a child forked from it is resumed, and its
        ``_reinit_after_fork`` takes its event from here exactly as a freshly built worker does.
        Without that, the resumed drain loop found the inherited event set and exited at once.

        It is a method rather than a rebind at the call site because the replacement must be a
        ``self`` attribute assignment: the child's repair, which since SPEC-079 reaches this
        event through ``_fork._GLOBAL_PRIMITIVES``, writes back at a module global or an instance
        attribute, and ``tests/test_fork_lifecycle.py``'s shape lint enforces that the source
        says so (SPEC-039 FR-002).

        Callers hold ``_lock``, with one exception: ``Worker._reinit_after_fork`` calls it from the
        fork handler without it, for the reason :func:`_rebuild_worker_after_fork` gives for not
        taking it — the child's only thread, its locks re-initialised moments earlier.

        Args:
          None.

        Returns:
          A stop signal that is not set, for handing to a sink.

        Raises:
          None.
        """
        if self._stop.is_set():
            self._stop = threading.Event()
        return self._stop

    def in_flight(self, sink: Sink) -> bool:
        """Moment, for the signal refresh — is anything using this sink right now (arch §9.2).

        One of **two** predicates in the moment category, and the pair is stated here rather
        than left to be rediscovered at a site, because an abandoned drain answers them
        oppositely (SPEC-054 FR-004). This one asks whether the worker is *draining* into the
        sink, so an abandoned drain counts as **over** and the sink gets a fresh event rather
        than SPEC-033 FR-004's tight retry loop; :meth:`held` asks whether the worker's thread
        is *alive* and may be inside the sink, so the same state counts as **still inside** and
        the sink is left open (SPEC-027 FR-004).

        It replaces ``worker_owns_now``, which was a conjunction over ownership and the moment.
        With one owed-close record there is no ownership question left for a call site to get
        wrong (FR-002), so what remains is the moment alone — and the identity term is what
        still stops an orphan log to sink Y being skipped because a live worker is draining
        into sink X.

        Takes no ``_lock``, which is the class docstring's rule and is what lets
        :func:`_will_hand`'s callers hold it. It does take the close registry's leaf
        lock through :meth:`_CloseRegistry.in_flight`, which is the order the shipped code already
        establishes — :func:`_will_hand` reaches that lock under ``_lock`` — and it is a leaf,
        so nothing acquires ``_lock`` beneath it.

        Args:
          sink: The sink about to be offered a stop signal.

        Returns:
          Whether the worker is draining into that sink, or a close of it is registered.

        Raises:
          None.
        """
        worker = self._worker
        if worker is not None and worker.sink is sink and worker.draining:
            return True
        return self._closes.in_flight(sink)

    def held(self, sink: Sink) -> bool:
        """Moment, for the close — may anything still be inside this sink (arch §9.2).

        The other half of the moment category, and the difference from :meth:`in_flight` is
        thread **liveness** rather than ``draining``. After an expired ``shutdown()`` the drain
        thread is alive inside ``emit`` with the drain marked settled, and the two answers must
        diverge there: the signal refresh must treat that as over so the sink gets an event it
        can be woken by, while the closer must treat it as still inside so the sink is left open
        for a later call (SPEC-027 FR-004). Both were measured — SPEC-033 FR-002 closing under a
        live writer, SPEC-035 FR-001 the fresh event never arriving.

        The worker's own answer is :meth:`Worker.may_be_inside`, which covers its live sink and
        any sink swapped out without a confirmed fence (SPEC-050 FR-004). A registered release
        counts too, so a second caller finding a close already running leaves the sink alone
        rather than closing it twice.

        Takes no ``_lock``; see :meth:`in_flight` for the leaf lock and its order.

        Args:
          sink: The sink the closer is deciding whether to take.

        Returns:
          Whether a live drain thread may be inside it, or a close of it is registered.

        Raises:
          None.
        """
        worker = self._worker
        if worker is not None and worker.may_be_inside(sink):
            return True
        return self._closes.in_flight(sink)

    def _move(self) -> None:
        """Re-settles :attr:`_current` from the worker, the count and the owed record (SPEC-078).

        The one writer of the stored state outside ``__init__``, and the caller holds ``_lock``.
        It evaluates SPEC-077's rules 3 to 6 **without** rule 5's configured-sink term: a live
        worker is ``RUNNING``; otherwise a retirement is ``CLOSED``; otherwise something owed is
        ``RUNNING``; otherwise ``UNCONFIGURED``. The config is left out because it is not the
        runtime's: ``configure()`` writes it without entering the runtime and must not take this
        lock, so a stored value that read it could go stale the instant ``configure()`` returned.

        **Where it is called is decided by whether removing the call would be observable.** It is
        the last statement of the four critical sections that can change the value — the build in
        :func:`_get_worker` (a worker, and the record armed from empty), the locked branch of
        :func:`_note_orphan_emit` (the record armed from empty), and :func:`_shutdown_worker`'s
        entry and late-worker sections (the retirement count). No other section calls it, because
        none can change the value, and a call there would be an equivalent mutant:
        :func:`_close_owed`'s take and put-back run only after the shutdown has moved the count,
        after which rule 3 or 4 decides and the record is not read; :func:`_swap_sink` arms a sink
        only when one is already owed or a worker is live; and :func:`_clear_after_fork` changes
        none of the fields it reads.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        live = self.live_worker()
        retirements = self.retirements
        if live is not None:
            self._current = State.RUNNING
        elif retirements > 0:
            self._current = State.CLOSED
        elif self._closes.owed:
            self._current = State.RUNNING
        else:
            self._current = State.UNCONFIGURED

    def _column(self) -> State:
        """The column of the transition table a lifecycle call takes its answer from (SPEC-078).

        The token sets first, then the stored state: ``DRAINING`` while a ``shutdown()`` or the
        exit drain is inside its body, else ``SWAPPING`` while a swap is inside its work, else
        :attr:`_current`. It never reads the config, so a ``SWAP`` or an ``ARM`` from cold is in
        the ``UNCONFIGURED`` column although :attr:`state` already reports ``RUNNING`` there.
        The caller holds ``_lock``; the reads are each one reference load or one ``bool`` of a
        builtin set, so the answer is exact for the section that asks.

        **Only** :func:`_swap_sink` **consults it**, and that is the table rather than an omission.
        Five of its six rows give the same code in every column: ``BUILD`` and ``ARM`` build and
        arm wherever they are; ``BUILD`` × ``DRAINING`` is split by ``_shutdown_running``, which is
        kept on purpose because its window is narrower than the state's (a worker built after the
        drain lowered it must not be registered where nothing stops it); ``FLUSH`` moves nothing;
        ``STOP`` × ``DRAINING`` is the second caller ``Worker.claim_stop`` already answers; and the
        ``FORK`` entry only empties the token sets. A read whose value decided nothing at those
        entries would be a dead binding. ``SWAP`` × ``DRAINING`` with no live worker is the one
        cell whose answer differs — it declines — and that is where this is read.

        Args:
          None.

        Returns:
          The column the calling entry is in.

        Raises:
          None.
        """
        if self._stops_running:
            return State.DRAINING
        if self._swaps_running:
            return State.SWAPPING
        return self._current

    def _take_ahead(self) -> bool:
        """Whether some drain in progress still has a take of the owed record ahead of it.

        The bound of the one changed cell (SPEC-078 FR-004): a swap during a drain with no live
        worker declines only while this is true, because only then does the drain own every owed
        close — it will take what is owed when it gets there. After a drain's last take, a sink
        armed since (an orphan log re-arming it) is taken by nothing in that call, and declining a
        swap then left it owed and open after the shutdown returned, where the swap's own
        no-worker branch releases it. The caller holds ``_lock``; each flag is written under it.

        The token set is copied into a tuple before it is read: its discard runs without the
        lock, and the copy is one C-level pass that runs no Python code, so it cannot see the set
        change size under it.

        Args:
          None.

        Returns:
          Whether any ``STOP`` in :attr:`_stops_running` has not yet made its last take.

        Raises:
          None.
        """
        return any(not op.drained for op in tuple(self._stops_running))

    @property
    def state(self) -> State:
        """The lifecycle state: the token sets, then the stored state; takes no lock (SPEC-078).

        Each input is read once into a local before any rule is tested — the two token sets, the
        stored state and whether the configured sink has resolved — and the rules are then tested
        in this order, the first that holds deciding:

        1. a token in :attr:`_stops_running` is ``DRAINING`` — ``shutdown()`` or the exit drain is
           inside :func:`_shutdown_worker`;
        2. a token in :attr:`_swaps_running` is ``SWAPPING``;
        3. a stored ``UNCONFIGURED`` with a configured sink resolved is ``RUNNING``: the one input
           the runtime does not own, read through the config because it is the recorded authority
           for the installed sink (``architecture.md`` §12), and what makes ``configure()`` leave
           ``UNCONFIGURED`` as a caller sees it;
        4. otherwise the stored :attr:`_current`.

        **Lock-free**, beside the four questions and for their reason: it must answer while
        another thread holds ``_lock``, and several guards hold that lock, which a non-reentrant
        acquire here would deadlock. The answer is a snapshot. Nothing in ``src/`` reads it: the
        library's own calls read :meth:`_column` under the lock they already hold, so this is the
        reporting view, read by tests.

        Args:
          None.

        Returns:
          The state the process is in.

        Raises:
          None.
        """
        stopping = bool(self._stops_running)
        swapping = bool(self._swaps_running)
        current = self._current
        resolved = _live_config_sink() is not None
        if stopping:
            return State.DRAINING
        if swapping:
            return State.SWAPPING
        if current is State.UNCONFIGURED and resolved:
            return State.RUNNING
        return current


_state = Runtime()
"""The process's one lifecycle owner.

A module global rather than a lazily-built singleton, so a forked child's repair resolves it
without having to know it might not exist yet. It is **not** a root of the child's one walk
(SPEC-079): its four primitives — the state lock, the stop event, the record lock and the close
registry's lock — are listed in ``_fork._GLOBAL_PRIMITIVES`` and replaced first, and its records
are not live state. The owed and ownership records seed the marking frontier instead, never
hooked, and :attr:`Runtime._FORK_SKIP` and :attr:`_CloseRegistry._FORK_SKIP` stay as the guard
for any other route by which the walk might reach it.
"""


def _clear_after_fork() -> None:
    """Drops what no thread will finish in a child, and re-settles its state (SPEC-078, SPEC-079).

    An entry is discharged by a ``finally`` on the thread performing the close — and a
    forked child has only the thread that called ``fork()``, so every inherited entry is one
    nothing will ever clear. Left in place it is not a missed refresh but a permanent one: once
    the child sets its own ``_stop``, that sink is handed the set event and every backoff
    collapses to zero, which is SPEC-033 FR-004's tight retry loop. A bystander in the child
    would also wait out the grace for a close that can never finish.

    Registered with ``_fork`` rather than reached for by it, the inversion SPEC-039 FR-006
    requires so that ``_fork`` imports nothing but ``_diag``. It takes the registry's own leaf
    lock, which the roster re-initialised moments earlier, and writes the in-flight half
    directly rather than through a method, so the inventory gate's reset check sees the write in
    the registered handler itself. A detached close's thread lived in that half since SPEC-078
    (it was ``_closers``, read as dead in a child and pruned), so it is dropped with its entry.
    The owed record is not touched: the child inherits it, as it always has.

    It runs **after** :func:`_reclaim_after_fork` and before :func:`_rebuild_worker_after_fork`, and
    the placement is free rather than load-bearing: the in-flight half holds ``int`` ids, and no
    handler reads it.

    The process-wide orphan-close count and its idle gate were reset here too and are gone
    (SPEC-054 FR-006): with a per-sink registration there is no second thing that can be left
    inconsistent, and the in-flight events are dropped here with their entries.

    **It is the transition table's ``FORK`` entry** (SPEC-078 FR-003), and since SPEC-079 it is
    the fifth site that calls :meth:`Runtime._move`. The fields :attr:`Runtime._current` is a
    function of are inherited with it, which made the stored state exact — unless the fork landed
    inside one of the four sections, between a field write and that section's ``_move``. A parent
    thread holds ``_state._lock`` there, the roster gives the child a fresh one, and the child
    sees the fields written up to the fork point and the state from before them. So the handler
    re-settles: it takes ``_state._lock``, which the roster replaced, and calls ``_move`` over
    the fields as they are. That is not an equivalent mutant of SPEC-078's "inherited, so exact":
    measured, a child forked inside ``_get_worker``'s section read a stored state that differed
    from the derived one.

    **The same fork inside ``_get_worker`` left a live worker's sink unowed**, between
    ``_state._worker = worker`` and the arm. A re-acquired ``FileSink`` the child then owned was
    never closed by the child's ``shutdown()``, which closes only owed sinks. So the handler arms
    the live worker's sink when the owed record lacks it, which in an unbroken fork it never does.

    **It resets the no-worker shutdown count and the late worker** (SPEC-079 FR-004).
    ``_shutdown_running`` counts the no-worker ``shutdown()`` calls inside their body, and no such
    call survived the fork; inherited, every worker the child built was registered as
    ``_late_worker`` for a shutdown that would never come (SPEC-077's measurement).
    ``_late_worker`` is the count's pair. The inherited worker keeps the liveness its epoch gives
    it, so a child forked inside the ``_shutdown_running`` window inherits that worker as its live
    worker either way.

    **It also empties the three token sets** (SPEC-077 FR-002), for the same reason as the
    registry: a token is discarded by a ``finally`` in the ``_swap_sink`` or ``_shutdown_worker``
    call that added it, or in the stop-signal hand that opened it, and that call's thread did not
    survive the fork, so an inherited token would leave the child reading ``SWAPPING`` or
    ``DRAINING`` forever, or sending every orphan emit to the locked branch. A thread that forks
    from inside a hand continues in the child, and its own ``discard`` then finds nothing. Under the registry's lock
    only because this is the critical section the handler already has; the sets themselves need
    none.

    **It zeroes the release depth** (SPEC-081): :attr:`Runtime._releasing_here` counts the
    performed releases a thread is inside, and a child forked from inside one's ``close()`` that
    leaves through ``os._exit`` never unwinds them, so an inherited depth would stop that child
    from ever re-pinning its ownership record.

    **It marks the child's stop signal as owed to its sinks**, through
    :attr:`Runtime._signal_owed`, which :func:`_owed_candidates` settles on the child's
    first orphan log or ``flush()``. It writes only the flag, so no sink's code runs here. It is
    set unconditionally, including in a grandchild and when a worker exists, because the offer
    skips a sink a live worker is draining into and the cost is one locked branch per fork.

    Args:
      None.

    Returns:
      None.

    Raises:
      None.
    """
    with _state._lock:
        with _state._closes._lock:
            _state._closes._in_flight.clear()
            _state._swaps_running.clear()
            _state._stops_running.clear()
            _state._hands_running.clear()
        _state._shutdown_running = 0
        _state._late_worker = None
        _state._signal_owed = True
        _state._releasing_here = threading.local()
        worker = _state.live_worker()
        if worker is not None and id(worker.sink) not in _state._closes.owed:
            _state._closes.arm(worker.sink)
        _state._move()


_FOREIGN = -1
"""The pid a record carries when the sink belongs to some earlier process.

Never a real pid, so it can never match :func:`os.getpid`, which is what gives "this process did
not acquire it" a **terminal** state. A forked child lays it over each inherited sink its walk
reaches that the parent never recorded; **when** that happens is :func:`_walk_after_fork`'s to
state, and this docstring defers rather than restating it (SPEC-053 FR-003). The restatement that
stood here was one of the twelve sites PR #218 had to correct, and it drifted out of step with a
function in its own module.

Without it the record protects nothing where it is empty: ``stamp`` is write-once, and write-once
defends only a record that already exists, so a child could ``configure()`` its way into *owning*
a sink the parent never recorded and then close it entirely legitimately. Measured before this
existed — a child claimed a connection sink held behind a third-party wrapper and closed it,
destroying the parent's transport. Unrecorded has to be unclaimable, not merely unreleasable.
"""

_MARKING_CEILING = 100_000
"""Objects the child's marking walk may visit before it stops and marks the heap instead.

**A cap is right here where SPEC-039 rejected one, and the difference is the fallback.** That
spec declined to bound its repair walk because an unfound lock is a child that hangs with no
safe degradation — a cap would trade a certain hazard for an uncertain one. This walk has one:
a trip marks the sink-shaped objects on the child's inherited heap instead (:func:`_mark_heap`,
SPEC-081 FR-005), and where that scan cannot run, :attr:`Runtime._marking_failed` refuses every
unrecorded sink, a leaked handle rather than a closed transport, the direction FR-001 requires.
Past a trip the walk neither repairs nor hooks what it did not reach; the heap scan only marks.

The exposure is also larger than the repair frontier's. That frontier enters containers only
from owned holders and one level deep; the marking frontier enters every container reachable
through arbitrary third-party objects, and a ``list`` subclass with a non-terminating
``__iter__`` once took a child to **5.7 GB RSS in nine minutes**, unkillable by its parent
because the parent was being starved. Since SPEC-079 a subclass's own iteration is never run,
but a genuinely large graph still exists, and exhausting memory in a child that has not returned
from ``fork`` is worse than the leak the cap causes.

It counts the marking frontier's own visits, across both of its groups, bounds every
container read in either frontier, and bounds the objects ``_capture`` enters beneath the sinks
one re-pin demotes (SPEC-081 FR-002). Set far above any real graph — a sink's is on the order of
hundreds — and above the ~24,000 a module escape once reached before the marking stopped
descending into modules. Before SPEC-081 the record pinned every sink ever handed over and seeded
the marking with all of them, so a process that reconfigured about 500 times tripped it in every
child; the record now seeds only what is alive and not a closed shipped sink (:func:`_record_seeds`).
"""


def _may_be_a_sink(value: object) -> bool:
    """Whether a value is worth asking the structural question about at all.

    Tested against ``_fork._PLAIN_TYPE_IDS`` by the exact type's id, so a ``class MySink(dict)``
    is still asked and no metaclass ``__hash__`` or ``__eq__`` of the value's type runs. Skipping
    the plain builtins is the single largest term in the walk's cost, because a buffering sink's
    contents are almost entirely these: it took a ``MemorySink`` holding 100k events from
    1,109 ms to 279 ms before the descent was bounded at all (SPEC-042 FR-001 AC-11).

    Args:
      value: Any object reached by the walk.

    Returns:
      Whether its exact type is something other than a plain builtin.

    Raises:
      None.
    """
    return id(type(value)) not in _fork._PLAIN_TYPE_IDS


def _bounded_children(container: object) -> list[object]:
    """Reads at most :data:`_MARKING_CEILING` members of one container, without materialising it.

    **The per-object ceiling in the walk cannot save a walk stuck inside one call**, which is
    where the module-wide walk SPEC-079 deleted put it: its helper did ``list(container)``, and a
    ``list`` subclass with a non-terminating ``__iter__`` never returns from that. Measured — a
    child reached 5.7 GB RSS in nine minutes and its parent's own timeout could not kill it,
    because the parent was being starved. So the bound has to be on the *read*, not only on the
    loop around it.

    **The members are read through the builtin base type's own iteration**, never the
    subclass's (SPEC-079 FR-002): ``dict.keys`` and ``dict.values`` for a mapping, and the base's
    unbound ``__iter__`` otherwise, so a subclass's own ``__iter__``, ``keys`` or ``values`` never
    runs and a non-terminating one is never entered. What is read is the container's real storage.

    Args:
      container: Any value ``_fork._is_container`` accepted.

    Returns:
      Its members, keys included for a mapping, truncated at the ceiling.

    Raises:
      None. A container that raises while being read contributes nothing, as it does in
        ``_fork``; what it holds is then unmarked. It keeps whatever ``stamp`` recorded, and
        where nothing did it is unrecorded — refused through a recorded wrapper, and §13 item
        7's residual otherwise, since absorbing here leaves :attr:`Runtime._marking_failed` clear.
    """
    try:
        return _read_members(container)
    except Exception as exc:
        _diag.absorbed(
            "reading a container while marking a forked child's sinks",
            exc,
            "what it holds is not marked by this child",
        )
        return []


def _read_members(container: object) -> list[object]:
    """Reads at most :data:`_MARKING_CEILING` members of one container, letting a fault propagate.

    The read :func:`_bounded_children` guards, through the builtin base type's own iteration so no
    code of a subclass runs. ``_capture`` calls it directly, so a container that raises in the
    parent is announced under the capture's own line rather than the marking's (SPEC-081 FR-002).

    Args:
      container: Any value ``_fork._is_container`` accepted.

    Returns:
      Its members, keys included for a mapping, truncated at the ceiling.

    Raises:
      Exception: Whatever reading the container's storage raises.
    """
    members: list[object] = []
    for storage in _fork._storage_iterators(container):
        members.extend(islice(storage, _MARKING_CEILING))
    return members


def _reachable_sinks(root: object, *, after_fork: _fork._Repair | None = None) -> list[object]:
    """Returns every sink reachable from one object, and in a forked child repairs the way there.

    A wrapper is handed over *with* its children, so the same act acquires them and the record
    has to reach the whole graph — a first draft stamped only what ``configure()`` was given, and
    a structural sink inside a ``MultiSink`` was then neither stamped nor reachable by
    ``_fork``'s mark, which is the object a forked child closed twice.

    **Reaching the graph is a descent question, not an ownership one**, and that distinction is
    what keeps this inside SPEC-039's boundary. The walk enters library objects and plain
    containers as that module's predicates already define, and *records* any sink-shaped member
    it meets even where it will not descend into it. Recording runs nothing of the object: an
    ``id``, a reference, and ``_fork._sink_shaped``'s static read of ``emit`` and ``close``, which
    like every predicate here reads the real type's MRO and namespaces and never calls a hook of
    the value, its class or its metaclass (SPEC-079 FR-002).
    "Do not reach into third-party state" (SPEC-039 FR-003 AC-2) forbids mutating and traversing
    a foreign object, not noticing one.

    **A container is scanned one level and never recursed into**, which is a bound chosen with a
    measurement in hand (FR-001 AC-11). A sink is never inside *caller data*: it is an owned
    object's attribute, or a member of a container an owned object holds directly, which is what
    ``MultiSink._sinks`` is. Unbounded descent enters every event dict a buffering sink holds and
    measured 279 ms on a ``MemorySink`` with 100k events against 2 ms for this; both return an
    identical set for every shape the library ships or ``README.md`` documents. What the bound
    gives up is a sink two container hops below an owned holder, which is then unrecorded,
    therefore refused, therefore **leaked rather than destructively closed** — the one direction
    FR-001 permits a gap to fail in.

    **Sink-shaped is tested before container-shaped**, and the order is load-bearing. A sink
    whose class subclasses a builtin container — ``class MySink(dict)``, or anything built on a
    ``NamedTuple`` — satisfies both tests, and with the container branch first it was read as a
    bag of members and never recorded. Held as a bare attribute that is exactly
    ``FilteringSink._inner``, so ``FilteringSink(MySink()).close()`` silently closed nothing:
    measured, a regression against the unguarded release this replaced, needing no fork at all.
    ``MultiSink`` escaped only by accident of position, its children arriving through the
    container branch. A plain ``tuple`` or ``list`` is not sink-shaped, so the wrapper case still
    takes the container branch as it must.

    **The order is a trade, not a free win.** A value satisfying *both* tests is pushed as a
    holder, and for ``stamp`` this walk's holder loop has no container branch, so an owned
    non-sink container subclass and a container-subclass sink's own members both lose reach.
    Neither is a destructive close — the marking frontier of :func:`_walk_after_fork` reads them
    in a child — and no shipped sink holds either shape; the residual is an own-process leak,
    which is the direction FR-001 permits, taken in exchange for closing a live silent one.

    **With ``after_fork``, the same walk is the forked child's repair frontier** (SPEC-079
    FR-002). It shares the ``_Repair``'s ``seen`` set with every other call and with the marking
    frontier, so no object is visited twice in one fork. On each package-owned holder it
    replaces the holder's own locks and events through ``_Repair.repair`` (minus its
    ``_FORK_SKIP`` names, which are neither replaced, entered, pushed nor forwarded), collects
    the holder for the ``reacquire_after_fork`` step when it offers the hook, and, when the holder
    is also a container, reads its members too. It forwards to the marking frontier every value
    it neither pushes nor drops as plain: each container, after its one-level read, and each
    object that is neither a sink nor a package object. On a sink-shaped holder this package does
    not own it replaces nothing and forwards that holder's own namespace and members, because
    the holder is already in the shared ``seen`` set when the marking frontier reaches it, so
    nothing beneath it would otherwise be visited. The repair frontier has no visit cap, for
    SPEC-039's reason: a cap there would be a lock it could not promise to find.

    Args:
      root: The object ``configure()`` or ``_ensure_sink()`` was handed, or one of the runtime's
        live roots in a forked child.
      after_fork: The fork's shared repair state, or ``None`` for ``stamp``'s read-only walk.

    Returns:
      The sinks reached, each once, in the order the walk met them.

    Raises:
      None. The reads are the ones ``_fork`` already guards, and a graph that cannot be walked
        fully leaves the unreached sinks unrecorded — refused, which is the safe default.
    """
    seen: set[int] = set() if after_fork is None else after_fork.seen
    found: list[object] = []
    stack: list[object] = [root]
    while stack:
        holder = stack.pop()
        if id(holder) in seen or not _may_be_a_sink(holder):
            continue
        seen.add(id(holder))
        if _fork._sink_shaped(holder):
            found.append(holder)
        if not _fork._is_owned(holder):
            target = _fork._super_target(holder)
            if target is not None:
                stack.append(target)
            elif after_fork is not None:
                _forward_foreign(holder, after_fork)
            continue
        if after_fork is None:
            items = _fork._namespace_items(holder)
        else:
            items = after_fork.repair(holder)
            if _fork._offers_reacquire(holder):
                after_fork.hooked.append(holder)
            if _fork._is_container(holder):
                _push_members(holder, stack)
        positional = _positional_sinks(holder, items)
        found.extend(positional)
        stack.extend(positional)
        for _name, value in items:
            if _is_candidate(value):
                stack.append(value)
            elif _fork._is_container(value):
                _push_members(value, stack)
                if after_fork is not None:
                    after_fork.forwarded.append(value)
            elif after_fork is not None and _may_be_a_sink(value):
                after_fork.forwarded.append(value)
    return found


def _positional_sinks(
    holder: object,
    items: list[tuple[str, object]],
    read: Callable[[object], list[object]] = _bounded_children,
) -> list[object]:
    """Returns the sinks a package holder holds in its declared child-sink slots, by position.

    Read from ``_fork._CHILD_SINK_SLOTS``: a value in such a slot was handed to the library as a
    sink, so it is recorded unconditionally, as ``stamp`` records its root, and pushed so its own
    graph is walked (SPEC-079). Nothing of the value runs: a ``one`` slot contributes its value, a
    ``many`` slot the members of its container, read through ``_bounded_children``.

    Args:
      holder: A package-owned object the walk entered.
      items: Its namespace pairs, as the walk read them.
      read: How a ``many`` slot's container is read: :func:`_bounded_children` for the walks,
        :func:`_read_members` for ``_capture``, which announces a fault under its own line.

    Returns:
      The sinks in its declared slots, ``None`` dropped.

    Raises:
      Exception: Only what ``read`` lets through; the walks' default raises nothing.
    """
    slots = _fork._child_sink_slots(holder)
    if not slots:
        return []
    sinks: list[object] = []
    for name, value in items:
        kind = slots.get(name)
        if kind == "one":
            sinks.append(value)
        elif kind == "many":
            sinks.extend(read(value))
    return [sink for sink in sinks if sink is not None and not _sink_arg.unpatchable(sink)]


def _push_members(container: object, stack: list[object]) -> None:
    """Pushes a container's candidate members, one level, onto the walk's stack.

    Members are pushed the same way in both modes of :func:`_reachable_sinks`; what the repair
    frontier forwards is the container itself, which the marking frontier reads again for what
    this level did not push.

    Args:
      container: A value ``_fork._is_container`` accepted.
      stack: The walk's stack.

    Returns:
      None.

    Raises:
      None.
    """
    stack.extend(member for member in _bounded_children(container) if _is_candidate(member))


def _forward_foreign(holder: object, after_fork: _fork._Repair) -> None:
    """Hands what a foreign sink-shaped holder holds to the marking frontier, replacing nothing.

    The holder is in the shared ``seen`` set once the repair frontier has visited it, so the
    marking frontier would never enter it; without this, nothing beneath a third-party wrapper is
    repaired, hooked or marked (SPEC-079 FR-002).

    Args:
      holder: A sink-shaped object this package does not own, on the repair frontier.
      after_fork: The fork's shared repair state.

    Returns:
      None.

    Raises:
      None.
    """
    after_fork.forwarded.extend(_holder_members(holder))


def _holder_members(holder: object) -> list[object]:
    """Returns what a foreign holder holds: its namespace values and, for a container, its members.

    Read with ``_fork._namespace_items`` and :func:`_bounded_children`, so none of the holder's
    code runs, and filtered to what the marking frontier would visit at all.

    Args:
      holder: A sink-shaped object this package does not own, or any object the marking
        frontier entered.

    Returns:
      The values worth visiting, in namespace order, then the container members.

    Raises:
      None.
    """
    target = _fork._super_target(holder)
    if target is not None:
        return [target]
    skip = _fork._skipped_names(holder)
    members = [
        value
        for name, value in _fork._namespace_items(holder)
        if name not in skip and _worth_visiting(value)
    ]
    if _fork._is_container(holder):
        members.extend(member for member in _bounded_children(holder) if _worth_visiting(member))
    return members


def _worth_visiting(value: object) -> bool:
    """Whether the marking frontier visits a value at all: a container, or not a plain builtin.

    A plain scalar can hold nothing, so it is never queued and never counts as a visit.

    Args:
      value: A member or attribute just read.

    Returns:
      Whether it is a container or something other than a plain builtin.

    Raises:
      None.
    """
    return _fork._is_container(value) or _may_be_a_sink(value)


def _is_candidate(value: object) -> bool:
    """Whether the walk pushes this value onto its stack.

    Args:
      value: A member or attribute the walk has just read.

    Returns:
      Whether it is either sink-shaped or an object this package defines.

    Raises:
      None.
    """
    return _may_be_a_sink(value) and (_fork._sink_shaped(value) or _fork._is_owned(value))


def _live_roots() -> list[object]:
    """Returns the runtime's live delivery targets, the roots of a forked child's repair frontier.

    The config's sink, the live worker and the late worker, each when it exists. A worker is a
    package-owned holder, so its sink and its own locks are reached through it. ``_state`` is not
    a root: its primitives are on ``_fork._GLOBAL_PRIMITIVES``, and its records are not live
    state (SPEC-079 FR-002). A patchable seam rather than inline, so a test can make the roots
    raise and watch the walk fail safe.

    **A sink reachable from none of these is not thereby safe**, and saying so was worse than
    saying nothing. A parent that builds a connection sink in application state and never hands
    it to the library — ``sink = SocketSink(...)`` at import in a gunicorn master — leaves
    nothing for the walk to find, and a child whose ``post_fork`` calls
    ``configure(sink=that_object)`` is the *first* process to hand it over, so it acquires it
    legitimately and closes the parent's transport at shutdown. It is recorded as a constraint in
    ``architecture.md`` §13, and ``sink_factory=`` is the library's way to build the sink in the
    worker process instead.

    Args:
      None.

    Returns:
      The roots, ``None`` dropped.

    Raises:
      Exception: Whatever resolving the config raises; :func:`_walk_after_fork` absorbs it and
        fails safe.
    """
    from log_foundry import config

    worker = _state.worker_exists()
    candidates = (config._live_config().sink, worker, _state._late_worker)
    return [found for found in candidates if found is not None]


def _live_sinks() -> list[object]:
    """Returns the sinks the runtime delivers to, recorded by the walk whatever they look like.

    The configured sink, and the live and the late worker's ``sink``. A ``sink_factory`` wrapper's
    built sink is not listed here: the walk records it by position, from the wrapper's declared
    ``_inner`` slot. Recording one runs none of its code — an ``id`` and a
    reference — so each is recorded unconditionally, beside what the walk's static sink-shape test
    finds (SPEC-079). Without it a ``weakref.proxy``, a ``super(Sub, inst)`` or a ``__getattr__``
    proxy configured as the sink, which no static read finds sink-shaped, could stay unrecorded in
    a child whose walk succeeded, and the child would then close the parent's transport.

    Args:
      None.

    Returns:
      The live delivery sinks, ``None`` dropped; a sink may appear twice.

    Raises:
      Exception: Whatever resolving the config raises; :func:`_walk_after_fork` absorbs it.
    """
    from log_foundry import config

    configured = config._live_config().sink
    sinks: list[object] = [configured]
    workers = (_state.worker_exists(), _state._late_worker)
    sinks.extend(worker.sink for worker in workers if worker is not None)
    return [sink for sink in sinks if sink is not None]


def _record_seeds() -> list[object]:
    """Returns the sinks the record holds, the marking frontier's second group of seeds.

    Every sink in ``_state._closes.owed``, then the sink of every entry in ``_state._owned``
    whose sink is alive, which are the only handles the library holds that the live roots may not
    reach. ``_owned`` is the load-bearing entry: it is the only one that reaches a sink held inside
    a **superseded** wrapper — one ``configure()`` replaced, so it is no live target, while the
    transport beneath it is still the parent's. Dropping it is a destructive close.

    **A closed sink of a shipped class exactly is left out** (SPEC-081 FR-004). It released its
    transport through the closed guard (SPEC-076), and what it held was stamped when it was handed
    over, so walking it buys nothing — and walking every closed, superseded ``MemorySink``'s events
    was the whole cost of a long-lived process's children tripping :data:`_MARKING_CEILING`. The
    test is the exact class (``_fork._defined_here``), never ``_fork._is_owned``: a caller's
    ``class Pool(Sink)`` or a subclass of a shipped sink satisfies that, while its ``close()`` is
    the caller's code and may have closed nothing beneath it, so such a sink, closed and kept, is
    still a seed — as a closed third-party sink is, whose transport may sit two containers down,
    deeper than ``_capture`` reads. A sink the capture recorded was never closed, so it is a seed
    while it lives.

    Args:
      None.

    Returns:
      The seeds, owed sinks first.

    Raises:
      None.
    """
    seeds: list[object] = list(_state._closes.owed.values())
    with _state._owned_lock:
        alive = [(entry.referent(), entry.closed) for entry in list(_state._owned.values())]
    seeds.extend(
        referent
        for referent, closed in alive
        if referent is not None and not (closed and _fork._defined_here(type(referent)))
    )
    return seeds


class _CeilingTripped(Exception):
    """The marking frontier passed :data:`_MARKING_CEILING` visits."""


def _mark_frontier(
    queue: collections.deque[object],
    repair: _fork._Repair,
    found: list[object],
    *,
    hook: bool,
    visits: int,
) -> int:
    """Visits one group of the marking frontier's seeds, first in first out, within the ceiling.

    **First in, first out** (SPEC-079 FR-003), so every seed of the group is visited before any
    of their members: a forwarded ``FileSink`` behind a third-party wrapper cannot lose its
    repair and its hook to the caller data of a large container forwarded beside it. It skips
    modules and reads as the marking always read: a container through :func:`_bounded_children`,
    a container subclass both ways, and any other object that is not a plain builtin, third-party
    ones included, through ``_fork._namespace_items``. On a package-owned holder it replaces the
    primitives as the repair frontier does, and collects ``reacquire_after_fork`` only when
    ``hook`` is set: from forwarded seeds, which are part of a live target's graph and must be
    stranded (the stranding decision), never from record seeds, whose superseded sinks must not
    be re-acquired (SPEC-044 FR-005). Names in a holder's ``_FORK_SKIP`` are not visited.

    Args:
      queue: The group's seeds; members are appended as the frontier reads them.
      repair: The fork's shared repair state; its ``seen`` set is the repair frontier's.
      found: Where each sink-shaped object visited is recorded.
      hook: Whether a holder reached here is collected for the hook step.
      visits: The visits both groups have made so far.

    Returns:
      The visits made so far, this group's included.

    Raises:
      _CeilingTripped: When the next visit would pass :data:`_MARKING_CEILING`; the queue still
        holds what was left.
    """
    while queue:
        if visits >= _MARKING_CEILING:
            raise _CeilingTripped
        holder = queue.popleft()
        if id(holder) in repair.seen or _fork._is_module(holder):
            continue
        repair.seen.add(id(holder))
        visits += 1
        if not _may_be_a_sink(holder):
            queue.extend(member for member in _bounded_children(holder) if _worth_visiting(member))
            continue
        if _fork._sink_shaped(holder) or _fork._is_weak_proxy(holder):
            found.append(holder)
        if not _fork._is_owned(holder):
            queue.extend(_holder_members(holder))
            continue
        if _fork._is_container(holder):
            queue.extend(member for member in _bounded_children(holder) if _worth_visiting(member))
        if hook and _fork._offers_reacquire(holder):
            repair.hooked.append(holder)
        items = repair.repair(holder)
        positional = _positional_sinks(holder, items)
        found.extend(positional)
        queue.extend(positional)
        queue.extend(value for _name, value in items if _worth_visiting(value))
    return visits


def _repair_live_counters(repair: _fork._Repair) -> None:
    """Repairs every live ``SinkCounters``, read from its registry without an import.

    ``sinks.verdict._LIVE_COUNTERS`` holds each counters object the process built that is still
    alive, so a third-party engine sink's counters lock is repaired whatever holds it — a sink
    the walk never enters, or a wrapper that keeps only that sink's bound ``emit``, which no
    reading walk reaches (SPEC-079 FR-002). The module comes from ``sys.modules``: a process that
    never loaded it built no counters, and a child must not import. A member already in ``seen``
    was repaired by the walk, so each is repaired at most once.

    Args:
      repair: The fork's shared repair state.

    Returns:
      None.

    Raises:
      None.
    """
    module = sys.modules.get(f"{_fork._PACKAGE}.sinks.verdict")
    if module is None or not _fork._is_module(module):
        return
    registry = _fork._MODULE_DICT(module).get("_LIVE_COUNTERS")
    if type(registry) is not weakref.WeakSet:
        return
    for counters in list(registry):
        if id(counters) in repair.seen:
            continue
        repair.seen.add(id(counters))
        repair.repair(counters)


_LAST_RESORT = (
    "this child cannot tell what it inherited and will close only a sink the library built "
    "itself; configure sink_factory= to build one"
)
"""The detail a child writes when it cannot tell what it inherited (SPEC-081 FR-006)."""


def _walk_after_fork(repair: _fork._Repair) -> None:
    """The one walk a forked child runs: repair from the live roots, then mark (SPEC-079).

    Registered with ``_fork.register_walk`` and run by ``_fork._reinit_after_fork`` after the
    inherited record is pinned and the roster of global primitives is replaced, and before any
    handler, with the ``_Repair`` that carries the roster's memo, so a sink's
    ``log_foundry_stop_signal`` and the worker's ``_stop`` stay the one ``Event`` the roster put
    at ``_state._stop``.

    1. **The repair frontier**: :func:`_reachable_sinks` from each of :func:`_live_roots`, with
       ``after_fork``, then every live ``SinkCounters`` from its registry.
    2. **The marking frontier**, visited only once the repair frontier is empty, bounded by
       :data:`_MARKING_CEILING` visits across both of its groups and by the same per-read bound on
       every container: first the forwarded seeds, whose package holders are hooked, then
       :func:`_record_seeds`, whose holders never are. The groups share the repair frontier's
       ``seen`` set, so an object reachable from both is visited through the first.
    3. **The heap fallback** (SPEC-081 FR-005), when the marking frontier tripped the ceiling or
       this child inherited :attr:`Runtime._marking_failed` from an ancestor whose own marking
       failed: :func:`_mark_heap` adds the sink-shaped objects on the inherited heap to what the
       frontiers recorded. It runs inside the at-fork walk, before any handler and before the
       application resumes, so whatever it finds was inherited.
    4. **The record write**: each object recorded above that has no live entry
       (:func:`_entry_for`) gets a strong ``_FOREIGN`` one, so a sink the parent stamped keeps the
       parent's pid, a dead entry on a reused ``id`` is overwritten, and a recorded sink with no
       record becomes unclaimable. What no frontier and no scan recorded stays unrecorded, and is
       refused unless this child can tell what it inherited.
    5. **The verdict.** A scan that succeeded clears :attr:`Runtime._marking_failed` — a trip
       writes one ``pending`` line saying the heap was marked instead, while a scan run only for
       an inherited flag writes nothing. A scan that raised, or met a frozen heap
       (``gc.freeze``), sets it and writes the last-resort line (FR-006).

    It first resets the three once-only announcement flags, so each child announces for itself.

    **Marking is what makes "no record" terminal in a child** (SPEC-042 FR-001). Without it, a
    child that re-wraps an inherited object the parent never recorded stamps it with its own pid
    and then closes the parent's transport; the marking frontier keeps the reach that prevents it
    for a sink behind a third-party wrapper and for one two containers down, and the heap scan
    keeps it past a trip.

    Args:
      repair: The fork's shared repair state.

    Returns:
      None.

    Raises:
      None. An exception escaping the walk itself sets :attr:`Runtime._marking_failed` with no
        heap scan and writes the last-resort line, after which this child refuses an unrecorded
        sink rather than trusting it, and records a ``sink=`` it is handed as ``_FOREIGN``.
        Every read below is static, so that guard is for a fault in resolving the roots or the
        seeds. The hooks collected before a raise still run, and ``_reclaim_after_fork`` runs
        either way.
    """
    inherited_failed = _state._marking_failed
    _state._capture_announced = False
    _state._stamp_refusal_announced = False
    _state._repin_announced = False
    scanned: bool | Exception | None = None
    tripped = False
    try:
        found: list[object] = _live_sinks()
        for root in _live_roots():
            found.extend(_reachable_sinks(root, after_fork=repair))
        _repair_live_counters(repair)
        queue: collections.deque[object] = collections.deque(repair.forwarded)
        try:
            visits = _mark_frontier(queue, repair, found, hook=True, visits=0)
            queue = collections.deque(_record_seeds())
            _mark_frontier(queue, repair, found, hook=False, visits=visits)
        except _CeilingTripped:
            tripped = True
        if tripped or inherited_failed:
            try:
                scanned = _mark_heap(found)
            except Exception as exc:
                scanned = exc
        with _state._owned_lock:
            epoch = _state._owned_clock
            for inherited in found:
                if _entry_for(inherited) is None:
                    _state._owned[id(inherited)] = _Entry(inherited, _FOREIGN, epoch, strong=True)
    except Exception as exc:
        _state._marking_failed = True
        _diag.absorbed("marking the sinks a forked child inherited", exc, _LAST_RESORT)
        return
    if isinstance(scanned, Exception):
        _state._marking_failed = True
        _diag.absorbed("marking the sinks a forked child inherited", scanned, _LAST_RESORT)
    elif scanned is False:
        _state._marking_failed = True
        _diag.pending(f"the heap is frozen (gc.freeze); {_LAST_RESORT}")
    elif scanned is True:
        _state._marking_failed = False
        if tripped:
            _diag.pending(
                f"the walk marking a forked child's inherited sinks passed {_MARKING_CEILING} "
                f"objects; this child marked the unrecorded sinks on its heap instead and closes "
                f"none of them, even if configured, unless re-acquired"
            )


def _heap_objects() -> list[object]:
    """Returns every object the collector tracks, the seam a test makes raise (SPEC-081 FR-006).

    Args:
      None.

    Returns:
      ``gc.get_objects()``.

    Raises:
      Exception: Whatever the collector raises; :func:`_walk_after_fork` absorbs it.
    """
    return gc.get_objects()


def _mark_heap(found: list[object]) -> bool:
    """Adds the sink-shaped objects on a forked child's heap to what its walk found (FR-005).

    Andrew's decision of 2026-11-12, for a child whose marking tripped :data:`_MARKING_CEILING`:
    rather than trust nothing, it marks what it can see. Each object :func:`_heap_objects` returns
    is judged only by the static predicates the walk already uses — skipped when its exact type is
    a plain builtin, kept when ``_fork._sink_shaped`` or ``_fork._is_weak_proxy`` says so — so no
    object's code runs, and a class is never kept. Every instance of a Python class is tracked by
    the collector, so what it misses is a C-extension sink type without GC support, as the walk
    does.

    **A frozen heap is a failed scan.** After ``gc.freeze()`` the permanent generation is out of
    ``gc.get_objects()``'s reach, so a scan there would miss every frozen sink; it reads nothing
    and reports failure, sending the child to the last resort. "Frozen" is a freeze count above
    :attr:`Runtime._freeze_floor`, not above zero: CPython 3.12 keeps a few hundred immortal tuples
    in the permanent generation with no ``gc.freeze()`` at all, so a zero test sent every tripped
    child on 3.12 to the last resort. Unfreezing to scan was rejected: it
    would change the application's collector state behind its back.

    The cost is paid once, in a scanning child only: measured about 2 ms to list 71k objects and
    52 ms for 2.5M, then about 0.65 s per million to classify them, and a copy of every page holding
    an inherited tracked object, since taking a reference writes its count.

    Args:
      found: What the walk recorded; each object kept is appended.

    Returns:
      ``True`` when the heap was read, ``False`` when it was frozen and nothing was read.

    Raises:
      Exception: Whatever :func:`_heap_objects` raises.
    """
    if gc.get_freeze_count() > _state._freeze_floor:
        return False
    for candidate in _heap_objects():
        if not _may_be_a_sink(candidate):
            continue
        if _fork._sink_shaped(candidate) or _fork._is_weak_proxy(candidate):
            found.append(candidate)
    return True


def _reclaim_after_fork() -> None:
    """Records each sink whose re-acquisition hook returned, the first handler in a child.

    The half of the old marking handler that was not a walk (SPEC-042 FR-005): a sink that
    returned from ``reacquire_after_fork`` provably holds its own transport, so :func:`reclaim`
    overrides whatever the walk wrote. It runs after the marks are written and
    before any other handler, and it runs whether or not the walk failed.

    Args:
      None.

    Returns:
      None.

    Raises:
      None.
    """
    for reacquired in _fork.reacquired_in_child:
        reclaim(reacquired)


def reclaim(sink: object) -> None:
    """Records that a sink re-acquired its transport in this process (SPEC-042 FR-005).

    The one write that **overrides** an existing record, and it has to be: an inherited sink
    **the marking walk reached** carries another process's pid by the time the hook roster is
    read — the parent's own stamp, or the ``_FOREIGN`` :func:`_walk_after_fork` writes where the
    parent recorded nothing — so a write-once here would leave a sink that provably
    holds its own descriptor refused forever. Where the walk did *not* reach it the record is
    empty and this write is the one that fills it — a walk can miss a sink with no exception at
    all, and the loop calling this sits outside that walk's ``try`` so it also runs when the walk
    ended badly.

    **It re-stamps the sink that re-acquired, and nothing above it** (FR-005 AC-8). A child
    inheriting ``MultiSink(FileSink, FileSink)`` re-stamps the two children — only they implement
    the hook — while the wrapper keeps the parent's mark and stays refused, which leaves the
    re-acquired children reachable only through a wrapper nothing will release. That is a leak
    and loses nothing, since ``FileSink.emit`` flushes at the end of every batch, but it is
    stated rather than discovered.

    It writes a fresh strong entry naming this process and bumps :attr:`Runtime._owned_clock`
    onto it, as :func:`stamp` does, so a re-pin already walking cannot demote it (SPEC-081).

    Args:
      sink: The sink whose hook returned normally.

    Returns:
      None.

    Raises:
      None.
    """
    pid = os.getpid()
    with _state._owned_lock:
        _state._owned_clock += 1
        _state._owned[id(sink)] = _Entry(sink, pid, _state._owned_clock, strong=True)


_STAMP_REFUSAL = (
    "configure(sink=...) in a forked child that could not tell what it inherited: this child "
    "will not close that sink; configure sink_factory= so each process builds a sink of its own"
)
"""The detail of a last-resort child's first refused ``sink=`` (SPEC-081 FR-006)."""


def stamp(sink: object, *, built: bool = False) -> None:
    """Records that this process acquired a sink, and everything reachable from it (FR-001).

    Called at the one moment ownership is knowable — when the library is *handed* a sink, by
    ``configure(sink=…)`` or by ``_ensure_sink()`` building the lazy default — and never on
    ``_ensure_sink``'s fast-path return, which runs once per orphan event (AC-10).

    **Write-once per object.** A stamp naming another process is never overwritten, so a forked
    child cannot claim an inherited sink by configuring its way back to it, and the answer
    survives a second fork. Overwriting on every ``configure()`` satisfies every other criterion
    in FR-001 and fails AC-4. "Per object" is checked through :func:`_entry_for`, so an entry whose
    sink is dead, left on an ``id`` a new sink now occupies, is overwritten rather than kept
    (SPEC-081 FR-003). An entry this process already holds for the sink is made strong again, its
    ``closed`` cleared and its epoch bumped: the sink is handed over afresh, possibly while its
    earlier close is still running, and :func:`release` then leaves it unclosed in the record.

    The walk runs **outside** the lock and only the record write takes it, so an arbitrary
    object graph is never traversed while holding a process-wide lock.

    **The root is recorded whether or not it reads as sink-shaped** (SPEC-079). Being handed it
    is what declares it a sink, and recording it runs none of its code: an ``id`` and a reference.
    The walk below it is static, so a sink whose members only a C type's own lookup can reach — a
    ``weakref.proxy``, a ``super(Sub, inst)``, a ``__getattr__`` proxy — does not read as
    sink-shaped there; left unrecorded, a forked child found it unrecorded with its walk
    succeeded and closed the parent's transport at its ``shutdown()``.

    **In a child that cannot tell what it inherited** (:attr:`Runtime._marking_failed`), every
    sink it finds that has no live entry is recorded ``_FOREIGN`` (SPEC-081 FR-006): such a sink
    may be one the walk missed rather than one this process made, and recording it as this
    process's would let the child close a parent's transport. The one exception is a root the
    library built itself — ``built=True``, passed only by ``config._ensure_sink``,
    ``config._change_sink`` for a ``sink_factory=`` wrapper, ``_FactorySink._checked`` and
    :func:`_adopt_factory_after_fork` — which is recorded as this process's, so the lazy default
    and a factory's sink are still closed. The first ``configure(sink=…)`` whose root is refused
    this way writes one ``pending`` line per process. Where the flag is clear, ``built`` changes
    nothing.

    Args:
      sink: The sink being installed, whose reachable graph is acquired with it.
      built: Whether the library constructed this root, which a last-resort child still owns.

    Returns:
      None.

    Raises:
      None. This runs inside ``configure()``, which must not fail an application's startup over
        a bookkeeping step; an unrecorded sink is a refused one, which leaks rather than closes.
    """
    reachable: list[object] = [sink]
    try:
        reachable.extend(_reachable_sinks(sink))
    except Exception as exc:
        _diag.absorbed("recording which sinks this process owns", exc, "they will not be closed")
    pid = os.getpid()
    refused = False
    with _state._owned_lock:
        failed = _state._marking_failed
        _state._owned_clock += 1
        epoch = _state._owned_clock
        for index, found in enumerate(reachable):
            entry = _entry_for(found)
            if entry is None:
                own = not failed or (built and index == 0)
                _state._owned[id(found)] = _Entry(
                    found, pid if own else _FOREIGN, epoch, strong=True
                )
                refused = refused or (index == 0 and not own)
            elif entry.pid == pid:
                entry.strong = found
                entry.closed = False
                entry.captured = False
                entry.epoch = epoch
    if refused:
        announced, _state._stamp_refusal_announced = _state._stamp_refusal_announced, True
        if not announced:
            _diag.pending(_STAMP_REFUSAL)


def releasable(sink: object, *, owner: object = None) -> bool:
    """Whether this process may close a sink (FR-001).

    A recorded sink answers for itself: releasable exactly when the record names this process.
    That is the whole mechanism for the defect — in a child, an inherited sink's record does not
    name this process, so the child refuses the object it inherited. What it names instead
    differs: the parent's own stamp where ``configure()`` left one, and ``_FOREIGN`` where
    :func:`_walk_after_fork` wrote it, which is no real pid. The one sanctioned exception is
    :func:`reclaim`, which re-stamps a sink whose ``reacquire_after_fork()`` returned for *this*
    process — measured, such a sink reads releasable in the child while one without the hook
    reads refused beside it.

    **An *unrecorded* sink inherits the answer from whatever is releasing it**, and that is a
    correction to FR-001's flat "no record means refused". Every lifecycle path stamps: a sink
    reaches the worker or the orphan record only through ``config._ensure_sink()``, which the
    two acquisition points cover. So "no record" never occurs on a path the fork defect travels
    — it occurs when a **user** calls ``close()`` on a wrapper the library was never handed, and
    refusing there turns a documented public API into a silent no-op, which is the failure mode
    this whole arc exists to remove. ``FilteringSink(inner).close()`` must still close ``inner``.

    The wrapper is what makes the two distinguishable, so it is asked:

    - Neither recorded — a graph nothing in the record reaches. The caller owns it; honour the
      close.
    - The child recorded elsewhere — the inherited sink, or one :func:`_walk_after_fork` marked
      ``_FOREIGN``. Refused however it was reached, which is what closes the wrapper route
      (FR-002 AC-3).
    - The wrapper recorded, the child not — the sink added to a wrapper *after* ``configure()``
      walked it. Refused, per FR-001 AC-6: the library holds this graph, so a member it has no
      record of is one it must not assume is this process's. The consequence is a leak, recorded
      in §13.

    **"Unrecorded is the caller's" is only sound because a fork makes it false first.** In a
    child, :func:`_walk_after_fork` records every inherited sink **its walk reaches** that the
    parent did not record as ``_FOREIGN``, *before* any other handler runs — one the parent *did*
    record keeps that stamp and is refused on it. What the walk reached is therefore stamped or
    ``_FOREIGN``; what it missed stays unrecorded and is the §13 item 7 residual, not a case this
    closes — measured, a sink built before the fork behind a container that raises is unrecorded
    in the child, releasable, and closed. A walk that tripped :data:`_MARKING_CEILING` marks the
    sink-shaped objects on the child's heap instead (SPEC-081 FR-005). If the walk could not
    finish *at all*, or the heap scan failed, :attr:`Runtime._marking_failed` withdraws the
    assumption entirely and an unrecorded sink is refused.

    **An entry whose sink is dead answers for nothing** (SPEC-081 FR-003), for the sink and for
    its ``owner`` alike: an ``id`` is free for reuse once its object dies, and a new object of the
    same type lands on it almost every time (measured 99.93%), so a dead entry read as a record
    would refuse a fresh sink or lend it a dead sink's verdict. :func:`_entry_for` reads it as
    absent. A closed sink the caller still holds keeps its entry, weakly, so it answers exactly as
    before: recorded, and releasable again by the process that closed it.

    Identity is still re-checked against a strong reference rather than trusting the ``id``. While
    the reference stands an id collision is impossible, so this can only fail if that invariant
    breaks — and answering ``False`` there is the safe direction.

    Args:
      sink: The sink a caller is about to close.
      owner: The wrapper forwarding the close, when one is. ``None`` from the three lifecycle
        sites, which hold the sink directly.

    Returns:
      Whether this process may close it.

    Raises:
      None.
    """
    pid = os.getpid()
    with _state._owned_lock:
        at_id = _state._owned.get(id(sink))
        record = _entry_for(sink)
        owner_record = None if owner is None else _entry_for(owner)
    if at_id is not None and at_id.strong is not None and at_id.strong is not sink:
        return False
    if record is not None:
        return record.pid == pid
    return owner_record is None and not _state._marking_failed


def _entry_for(sink: object) -> _Entry | None:
    """Returns the record's live entry for this very object; the caller holds the record lock.

    The one reader of an entry (SPEC-081 FR-003). An entry whose sink has died, or whose sink is
    not this object, is read as absent: an ``id`` is free for reuse once its object dies, so such
    an entry describes some other object entirely. Every writer overwrites what this reads as
    absent, and ``_repin`` prunes it. Nothing here runs an object's code: a dictionary lookup by an
    ``int`` and a weak-reference dereference.

    Args:
      sink: The object asked about.

    Returns:
      The entry, or ``None`` when there is none, its sink is dead, or its sink is another object.

    Raises:
      None.
    """
    entry = _state._owned.get(id(sink))
    if entry is None or entry.referent() is not sink:
        return None
    return entry


def release(
    sink: Sink,
    *,
    detached: bool = False,
    owner: object = None,
    closing: _Close | None = None,
) -> threading.Thread | None:
    """Closes a sink on the library's behalf — the one path by which it ever does (SPEC-042 FR-002).

    Eight sites closed a sink directly before this existed, and guarding only the three the
    lifecycle owns was measurably insufficient: a forked child that wraps an **inherited** sink in
    a ``MultiSink`` of its own reaches the inner sink through the wrapper, so the parent's
    structural sink was closed twice with all three lifecycle sites guarded. Routing every
    library closer through one function is what gives the ownership question one home.

    **The guard moves here; the error handling does not.** This propagates whatever ``close()``
    raises, because the callers do not agree today and must not be made to: four absorb, under
    three distinct ``_diag`` texts naming the site (SPEC-029), ``MultiSink`` also increments its
    ``failed`` counter, and ``FilteringSink``/``TransformSink``/``LogstashSink`` propagate under
    a documented ``Raises:``. Folding the ``try/except`` in here would drop absorbed close
    failures out of ``Health.sink.failed`` — a SPEC-026 regression — and falsify those three.

    **A sink this process did not acquire is skipped, not failed** (FR-001, FR-002). A forked
    child inherits the parent's sink object, and closing it sends a real protocol goodbye on a
    connection the parent is still using — measured, the parent's next write failed with
    ``ECONNRESET``. Refusing here is a **skip**: nothing is counted as lost, nothing is retried,
    and every caller's control flow is unchanged, so ``MultiSink.close`` still isolates and
    continues, ``shutdown()`` still returns, and a swap still installs its new sink. The sink is
    left **open**, which is the trade SPEC-027 FR-004 and SPEC-030 already made twice: a leaked
    resource in an exiting process beats a corrupt write.

    Only the *release* is refused. Every **drain** is untouched (FR-003), so a child still gets
    its own events out through the sink it inherited.

    **The participant that discharges an entry pays the hand a worker build declined on it**
    (:meth:`_CloseRegistry.defer_hand`), on every exit, through :func:`_hand_after_close`, which
    takes ``_state._lock`` and so must never run under it. No caller that holds that lock can be
    handed a marked entry: ``_swap_sink`` takes, releases and discharges in **one** critical
    section, and the mark is written only in :func:`_get_worker`'s section under the same lock, so
    it cannot land in between; ``_close_owed``'s ``put_back``, the one discharge that does hold the
    lock with a mark possible, pays after releasing it. ``tests/test_lock_held_discharges.py``
    holds that shape. The inline exit pays inside its ``finally``, so a ``close()`` that raised
    still hands the sink the worker's signal.

    **A performed close settles the record** (SPEC-081 FR-001), after the entry is discharged and
    any deferred hand paid, so no lifecycle lock is held, and whether ``close()`` returned or
    raised: the sink's entry is marked ``closed`` — unless the sink was handed over again while
    this close ran, which bumped the entry's epoch past the one read just before the close — and
    ``_repin`` runs, demoting what the library no longer holds, once per outermost release on the
    thread (:func:`_settle_close`). Neither raises, so neither can
    replace what ``close()`` raised. A refused release and a detached one perform no close here;
    the detached one's thread comes back through this inline path.

    Args:
      sink: The sink to close.
      detached: Whether to close on a daemon thread rather than inline. Detached is for a sink
        the caller has stopped delivering to and must not block on (SPEC-030 FR-003).
      owner: The wrapper forwarding this close, when one is. The five shipped wrappers pass
        themselves; the lifecycle sites hold their sink directly and pass nothing. See
        :func:`releasable` for what the distinction decides.
      closing: The in-flight entry this close discharges, from :meth:`_CloseRegistry.take`, for
        a caller that took the sink before deciding who performs the close (SPEC-054 FR-003). It
        is discharged on **every** exit — a refused release and a closer thread that never
        started included, since either would otherwise leave an event nobody sets. ``None`` for a
        caller that did not take one, in which case one is registered here: ``GRACED`` before a
        detached thread starts, so the thread is attached to it and counted, and ``INLINE``
        around an inline close, which is how a wrapper's forwarded release reaches the registry.

    Returns:
      The started closer thread for a detached release, or ``None`` — for a refused release, for
      a completed inline close, and when the platform would not give the process another thread.
      A caller needing to tell those apart consults :func:`releasable`, which it already has.

    Raises:
      Exception: Whatever an inline ``close()`` raised. A refused release raises nothing, and
        neither does a detached one: its thread body absorbs, since there is no caller left to
        hand it to.
      BaseException: Whatever the sink's stop-signal setter raised that is not an ``Exception``,
        when a deferred hand is paid here. :func:`offer_stop_signal` absorbs an ``Exception``; a
        ``KeyboardInterrupt`` or ``SystemExit`` from the setter reaches the caller, and on the
        inline exit it replaces any exception ``close()`` raised, since it is raised in that
        ``finally``.
    """
    if not releasable(sink, owner=owner):
        if _state._closes.finish(sink, closing):
            _hand_after_close(sink)
        return None
    if detached:
        entry = closing if closing is not None else _state._closes.register(sink, _Rule.GRACED)
        closer = _start_closer(sink, entry)
        if closer is None and _state._closes.finish(sink, entry):
            _hand_after_close(sink)
        return closer
    epoch = _close_clock(sink)
    depth = _release_depth()
    registration = closing if closing is not None else _state._closes.register(sink, _Rule.INLINE)
    try:
        _state._releasing_here.depth = depth + 1
        sink.close()
    finally:
        _state._releasing_here.depth = depth
        try:
            if _state._closes.finish(sink, registration):
                _hand_after_close(sink)
        finally:
            _settle_close(sink, epoch, repin=depth == 0)
    return None


def _release_depth() -> int:
    """How many performed releases this thread is inside right now, for :func:`_settle_close`.

    Args:
      None.

    Returns:
      The depth, zero outside every one.

    Raises:
      None.
    """
    depth: int = getattr(_state._releasing_here, "depth", 0)
    return depth


def _close_clock(sink: object) -> int | None:
    """Reads the epoch of a sink's entry just before its close, for :func:`_settle_close`.

    Args:
      sink: The sink about to be closed.

    Returns:
      Its live entry's epoch, or ``None`` when it has no live entry.

    Raises:
      None.
    """
    with _state._owned_lock:
        entry = _entry_for(sink)
        return None if entry is None else entry.epoch


def _settle_close(sink: object, epoch: int | None, *, repin: bool) -> None:
    """Marks a sink this process closed as closed, then re-pins the record (SPEC-081 FR-001).

    The entry is marked only when it is still the live entry naming this process **at the epoch
    read before the close**. A ``configure(sink=s)`` landing while ``s``'s earlier close ran has
    made the entry strong again and bumped its epoch, and marking it then would leave a sink the
    library holds again looking finished — so it is left as that stamp made it.

    **Only the outermost performed release on a thread re-pins.** A wrapper's ``close()`` forwards
    a release for each of its children, and a re-pin walks the live graph, whose cost grows with
    what the live sink holds, so one re-pin per child multiplied a wrapper's close by its member
    count (``tests/test_ownership_record.py`` records the measurement). A release nested inside
    another's ``close()`` on the same thread therefore marks its sink and leaves the re-pin to the
    release that encloses it, which runs after every child has settled, so a library-driven
    wrapper close re-pins once. A wrapper the caller closes directly has no enclosing release, so
    each child's release re-pins, as before.

    Args:
      sink: The sink whose close has just ended, returned or raised.
      epoch: What :func:`_close_clock` read before the close.
      repin: Whether this release is the outermost on its thread, and so re-pins.

    Returns:
      None.

    Raises:
      None. ``_repin`` absorbs its own faults, so the exception ``close()`` raised is never
        replaced from here.
    """
    pid = os.getpid()
    with _state._owned_lock:
        entry = _entry_for(sink)
        if entry is not None and entry.pid == pid and entry.epoch == epoch:
            entry.closed = True
    if repin:
        _repin()


def _repin() -> None:
    """Holds strongly what the library still holds, and weakly the rest (SPEC-081 FR-001).

    Run on every exit of :func:`_swap_sink` and at the end of every :func:`release` that performed
    a close and is not nested inside another on its thread (:func:`_settle_close`). It works out which sinks the library still holds — reachable by the stamp walk from a
    live delivery root (:func:`_delivering_sinks`), owed a close, or being closed — promotes this
    process's entries for them, demotes this process's other entries to weak after recording the
    sinks beneath them (``_capture``), and prunes every entry whose sink has died. Entries naming
    another process are never promoted or demoted: in a child those are what it inherited.

    **It never raises.** A fault is absorbed and announced once per process
    (:attr:`Runtime._repin_announced`): a swap must still install its sink, and a release must
    still raise what ``close()`` raised.

    Args:
      None.

    Returns:
      None.

    Raises:
      None.
    """
    try:
        _repin_record()
    except Exception as exc:
        announced, _state._repin_announced = _state._repin_announced, True
        if not announced:
            _diag.absorbed(
                "re-pinning the ownership record",
                exc,
                "sinks this process no longer holds may stay in memory until a later swap or close",
            )


def _repin_record() -> None:
    """The work of :func:`_repin`: three short sections under the record lock, the walks outside.

    1. The epoch is read under the lock, so an entry written at or before it is a whole write.
    2. With no lock held, the sinks the library still holds are worked out: the stamp walk from
       each of :func:`_delivering_sinks`, the owed record's snapshot (read unlocked, as
       :func:`_record_seeds` reads it) and the ids of the closes in flight. A take landing
       between those two reads can be missed by both; that is benign, because the closing
       thread holds the sink alive and its own release re-pins after the close.
    3. Under the lock, over a snapshot: a dead entry is pruned (no weak-reference callback does
       it, since a callback runs inside the collector on any thread); this process's entry for a
       held sink is made strong; this process's strong entry for any other sink, when its sink
       can be weakly referenced and its epoch is no later than the one read in step 1, is put on
       a local list that holds the sink. The epoch test here only saves a capture beneath a sink
       stamped while the walks ran; step 5's is the binding one.
    4. With no lock held, ``_capture`` records what lies beneath the sinks on that list.
    5. Under the lock, the captured sinks with no live entry are written weakly, naming this
       process — or ``_FOREIGN`` in a child that cannot tell what it inherited — and each listed
       entry is demoted only while its epoch is still no later than step 1's: a sink handed over
       again during the capture has had its entry made strong and its epoch bumped by that stamp,
       and demoting it would leave a sink the library holds again weakly recorded. The list goes out of scope only after that section,
       so the last strong reference to a demoted sink drops with no library lock held and any
       finalizer it runs meets none of them.

    A finalizer that re-enters here on a thread already inside a section (FR-003) runs the whole
    body re-entrantly, its capture included; the record lock is an ``RLock`` and each section
    iterates its own snapshot.

    Args:
      None.

    Returns:
      None.

    Raises:
      Exception: Whatever resolving the roots or reading the registry raises; :func:`_repin`
        absorbs it.
    """
    pid = os.getpid()
    with _state._owned_lock:
        start = _state._owned_clock
    pinned: set[int] = set()
    for root in _delivering_sinks():
        pinned.add(id(root))
        pinned.update(id(found) for found in _reachable_sinks(root))
    owed = list(_state._closes.owed.values())
    pinned.update(id(sink) for sink in owed)
    pinned.update(_state._closes.in_flight_ids())
    demoted: list[tuple[int, _Entry, object]] = []
    with _state._owned_lock:
        for key, entry in list(_state._owned.items()):
            referent = entry.referent()
            if referent is None:
                if _state._owned.get(key) is entry:
                    del _state._owned[key]
            elif entry.pid != pid:
                continue
            elif key in pinned:
                entry.strong = referent
            elif entry.strong is not None and entry.weak is not None and entry.epoch <= start:
                demoted.append((key, entry, referent))
    captured, _visits = _capture([sink for _key, _entry, sink in demoted])
    writer = _FOREIGN if _state._marking_failed else pid
    with _state._owned_lock:
        epoch = _state._owned_clock
        for sink in captured:
            if _entry_for(sink) is None:
                _state._owned[id(sink)] = _Entry(sink, writer, epoch, strong=False, captured=True)
        for key, entry, sink in demoted:
            if _state._owned.get(key) is entry and entry.epoch <= start and entry.strong is sink:
                entry.strong = None


def _delivering_sinks() -> list[object]:
    """Returns the sinks the library delivers to now: the roots of the re-pin's walk.

    The configured sink, the **live** worker's sink and the late worker's sink, each when it
    exists. Not :func:`_live_sinks`, which counts a retired worker's sink too — a retired worker's
    closed sink is no longer held — and not :func:`_live_roots`, the fork walk's seam.

    Args:
      None.

    Returns:
      The sinks, ``None`` dropped.

    Raises:
      Exception: Whatever resolving the config raises; :func:`_repin` absorbs it.
    """
    from log_foundry import config

    sinks: list[object] = [config._live_config().sink]
    worker = _state.live_worker()
    if worker is not None:
        sinks.append(worker.sink)
    late_worker = _state._late_worker
    if late_worker is not None:
        sinks.append(late_worker.sink)
    return [sink for sink in sinks if sink is not None]


_CAPTURE_SHORT = (
    "recording the sinks beneath a superseded sink stopped short; a forked child may close one "
    "of them if it configures that sink itself"
)
"""The detail of the capture's once-only line (SPEC-081 FR-002)."""


def _capture(demoted: list[object]) -> tuple[list[object], int]:
    """Records what lies beneath sinks about to be demoted, so a child can still refuse it (FR-002).

    A strong entry is what kept a superseded wrapper alive, and a forked child's marking reached
    the sink beneath it from there. Once the wrapper can be collected, that sink needs an entry of
    its own, so before ``_repin`` demotes an entry this walks beneath its sink and returns every
    sink-shaped object and every ``weakref`` proxy it meets, the demoted sinks themselves left
    out; ``_repin`` records those that have no live entry.

    **Its reach** is the stamp walk's, extended into third-party holders. A holder this package
    owns (``_fork._is_owned``, which a ``Sink`` subclass satisfies) is entered as the stamp walk
    enters it: its namespace and its declared child-sink slots, a container read one level and
    only candidates pushed — so a ``MemorySink``'s plain event dicts are skipped. A third-party
    holder is read as the marking reads one: its namespace and its own container members, and one
    level into each plain container there; every object found that is not a plain builtin is
    entered in turn. A plain container is never entered, so a sink two containers below a
    third-party holder is not reached (the record keeps that holder as a seed while it lives).
    Every read is static; no code of a sink runs.

    **Its bound** is :data:`_MARKING_CEILING` objects entered; a plain member read and skipped is
    not one. A trip or a raise stops it with what it had found, and is announced once per process
    (:attr:`Runtime._capture_announced`), a trip through ``_diag.pending`` and a raise through
    ``_diag.absorbed``. It takes no lock: it runs between ``_repin``'s sections.

    Args:
      demoted: The sinks whose entries are about to go weak.

    Returns:
      The objects to record, and how many objects it entered.

    Raises:
      None.
    """
    roots = {id(sink) for sink in demoted}
    seen: set[int] = set()
    stack: list[object] = list(demoted)
    found: list[object] = []
    visits = 0
    tripped = False
    try:
        while stack:
            holder = stack.pop()
            if id(holder) in seen or not _may_be_a_sink(holder) or _fork._is_module(holder):
                continue
            if visits >= _MARKING_CEILING:
                tripped = True
                break
            seen.add(id(holder))
            visits += 1
            if id(holder) not in roots and (
                _fork._sink_shaped(holder) or _fork._is_weak_proxy(holder)
            ):
                found.append(holder)
            stack.extend(_beneath(holder, found))
    except Exception as exc:
        _announce_capture(exc)
        return found, visits
    if tripped:
        _announce_capture(None)
    return found, visits


def _beneath(holder: object, found: list[object]) -> list[object]:
    """Returns what ``_capture`` enters next beneath one holder, recording its positional sinks.

    Args:
      holder: An object ``_capture`` has just entered.
      found: Where a package holder's declared child-sink slots are recorded, by position.

    Returns:
      The members to enter.

    Raises:
      Exception: Whatever reading a container raises; ``_capture`` announces it.
    """
    target = _fork._super_target(holder)
    if target is not None:
        return [target]
    if _fork._is_owned(holder):
        items = _fork._namespace_items(holder)
        positional = _positional_sinks(holder, items, _read_members)
        found.extend(positional)
        members: list[object] = list(positional)
        for _name, value in items:
            if _is_candidate(value):
                members.append(value)
            elif _fork._is_container(value):
                members.extend(child for child in _read_members(value) if _is_candidate(child))
        return members
    skip = _fork._skipped_names(holder)
    direct = [value for name, value in _fork._namespace_items(holder) if name not in skip]
    if _fork._is_container(holder):
        direct.extend(_read_members(holder))
    members = []
    for value in direct:
        if _may_be_a_sink(value):
            members.append(value)
        elif _fork._is_container(value):
            members.extend(child for child in _read_members(value) if _may_be_a_sink(child))
    return members


def _announce_capture(exc: Exception | None) -> None:
    """Writes the capture's line once per process: a trip as ``pending``, a raise as ``absorbed``.

    Args:
      exc: What the capture raised, or ``None`` for a trip of the ceiling.

    Returns:
      None.

    Raises:
      None.
    """
    announced, _state._capture_announced = _state._capture_announced, True
    if announced:
        return
    if exc is None:
        _diag.pending(_CAPTURE_SHORT)
    else:
        _diag.absorbed("recording the sinks beneath a superseded sink", exc, _CAPTURE_SHORT)


def _pin_inherited() -> None:
    """Holds strongly every entry a forked child inherited whose sink is alive (SPEC-081 FR-004).

    Registered with ``_fork.register_pin`` and run **first** in the child's handler, before the
    roster replaces any lock and before anything else can drop a reference, so no sink the
    application stops referencing in the child is collected there and no finalizer sends a
    goodbye on the parent's transport: the second reason the record once held every sink strongly
    for life, kept for exactly what was alive at the fork. It takes no lock — the child is
    single-threaded until a handler starts a thread, and the record lock may still be a parent
    thread's — and reads a snapshot.

    Args:
      None.

    Returns:
      None.

    Raises:
      None.
    """
    for entry in list(_state._owned.values()):
        if entry.strong is None:
            entry.strong = entry.referent()


def _hand_after_close(sink: Sink) -> None:
    """Pays a stop-signal hand a worker build declined while a close of *sink* was in flight.

    A worker built during a ``shutdown()``'s close of its sink used to hand that sink a fresh
    signal at once: the build refreshes a set ``_stop``, and the close, already waiting on the
    sink's signal, then waited on that unset event for the sink's whole backoff — measured, a
    ``@trace`` landing in the inline close made ``shutdown(timeout=1)`` take 4.00 s. So the build
    declines and marks the entry (:meth:`_CloseRegistry.defer_hand`), and whoever discharges the
    entry hands the sink the signal here, once the close it would have cut short is over.
    Declining alone was not enough: the late worker then delivered into a sink still holding the
    shutdown's set event, the tight retry loop SPEC-033 FR-004 exists to prevent.

    **The current signal, never a refreshed one.** It reads ``_state._stop`` as it is and asks
    :func:`_will_hand` first, under ``_state._lock``, then hands with no lock held through
    :func:`_hand_out`, whose settle keeps the same rule; so it can deliver only the event the next
    ``shutdown()`` sets, never an unset one into a close in flight. It publishes nothing, so it
    opens no hand token. It is not paid through :attr:`Runtime._signal_owed`: a debt is settled by
    the next orphan log or ``flush()``, which **refreshes**, and measured, a ``flush()`` between
    the late worker's stop and the shutdown's second take then handed an unset event into that
    take's close, 4.00 s against 0.00 s; nor would a debt ever reach the late worker's sink in a
    process that only traces.

    Args:
      sink: The sink whose close has just been discharged.

    Returns:
      None.

    Raises:
      BaseException: What the sink's setter raised that :func:`offer_stop_signal` does not absorb.
    """
    with _state._lock:
        if not _will_hand(sink):
            return
        event = _state._stop
    _hand_out([sink], event)


def _pay_marks_after_stop() -> None:
    """Pays a hand the build declined in a stop, once no stop is left, if its close outlives it.

    The build defers while any stop is running (:attr:`Runtime._stops_running`), which reaches
    past the window where its worker is that stop's late worker: a build in a no-worker stop's
    tail, after ``_shutdown_running`` has dropped, is deferred too, and the worker it built is
    live and stopped by nobody. Deferring there is still right while the stop runs, because the
    stop's second pass may yet close the sink inline — measured, gating the deferral on
    ``_shutdown_running`` instead let a build inside that second-pass close hand its fresh signal
    into it, and the ``shutdown()`` took 4.92 s against 0.91 s. But when the close the build
    marked is not one this stop performs — a swap's displaced close still running after it —
    the live worker delivered into a sink still on the shutdown's set signal until that close
    ended: measured, three 0.3 s backoffs during it waited 0.00 s each, against 0.30 s each
    before the deferral existed.

    So the **last** stop out — run by :func:`_shutdown_worker` after it discards its token, and
    only when it finds no other stop running under ``_state._lock`` — hands the live worker's
    sink the worker's own signal and clears the mark (:meth:`_CloseRegistry.unmark`), so the
    close's own discharge later pays nothing. Two stops finishing together each ask under the
    lock, and the mark is consumed once, so only one hands. The locked re-read of the stop tokens
    is a defensive guard, unreachable as the code stands: a stop's entry retires the one worker
    in the same section that adds its token, so any stop running when this section reads them
    leaves :meth:`Runtime.live_worker` already ``None`` here. With no stop left, nothing awaits
    that close on the signal, which is the moment the build would have handed it before the
    deferral; so this skips only :func:`_will_hand`'s refusal of a sink whose close is in flight,
    and keeps every other term: the worker is live and draining into the sink, its ``_stop`` is
    the current signal and unset, and this thread is not already handing the sink. It **never
    refreshes**: it hands the event the next ``shutdown()`` sets. A mark the terms refuse is left
    for the close's discharge to pay. A fork while this hand runs leaves the child to its own
    repair, as for any hand: the child drops every in-flight entry with its mark, owes its sinks
    its own signal, and a continuing settle re-reads the child's signal under the child's lock.

    A ``shutdown()`` with no mark left behind — every one but this case — asks only a lock-free
    read of the stop tokens and one acquisition of the registry's leaf
    (:meth:`_CloseRegistry.marked`), so its pinned count of ``_state._lock`` acquisitions does not
    move.

    Args:
      None.

    Returns:
      None.

    Raises:
      BaseException: What the sink's setter raised that :func:`offer_stop_signal` does not absorb.
    """
    if _state._stops_running or not _state._closes.marked():
        return
    with _state._lock:
        if _state._stops_running:
            return
        worker = _state.live_worker()
        if worker is None:
            return
        sink = worker.sink
        stop = _state._stop
        if (
            worker._stop is not stop
            or stop.is_set()
            or not worker.draining
            or handing_here(sink)
            or not _state._closes.unmark(sink)
        ):
            return
    _hand_out([sink], stop)


def _start_closer(sink: Sink, closing: _Close) -> threading.Thread | None:
    """Starts a daemon close of a sink no longer being delivered to (SPEC-030 FR-003).

    The thread is returned rather than joined, so a caller holding a lock can start under it and
    wait after releasing it — ``_swap_sink`` mutates its records under the process-wide
    ``_state._lock`` and must not hold that across a wait of the swap's whole budget (SPEC-033
    FR-002). Callers that hold no lock join it immediately and are equivalent to the single call
    this replaced.

    The thread is a **daemon**, and it is the shutdown's graced wait
    (:meth:`_CloseRegistry.wait_graced`) that makes that safe rather than merely available. A non-daemon thread was tried and is worse on its own: CPython joins
    non-daemon threads *before* running ``atexit``, so one hung close stops the exit drain from
    ever running and loses everything buffered in the **live** sink. A daemon alone is worse in
    the opposite case: a close that is slow but *succeeding* is killed at exit, losing whatever
    it was flushing.

    The started thread is attached to the close's entry, which is what the graced wait joins and
    ``health().closing_sinks`` counts.

    Args:
      sink: The sink that was swapped out.
      closing: The in-flight entry the thread discharges.

    Returns:
      The started thread, or ``None`` when the platform would not give the process another one.

    Raises:
      None. ``Thread.start`` raises when the process is out of threads, and a swap that cannot
        spawn one must leave the sink open and say so rather than fall back to an inline close —
        the fallback would reintroduce the unbounded wait this exists to remove, in the one
        situation where the process is already under resource pressure.
    """
    closer = threading.Thread(
        target=_close_guarded,
        args=(sink, closing),
        name="log-foundry-sink-close",
        daemon=True,
    )
    try:
        closer.start()
    except Exception as exc:
        _diag.absorbed(
            "starting the thread that closes a swapped-out sink",
            exc,
            "it is left open and may still hold its resources",
        )
        return None
    _state._closes.attach(closing, closer)
    return closer


def _close_guarded(sink: Sink, closing: _Close) -> None:
    """Closes a swapped-out sink on its own thread, absorbing a failure.

    The guard is what makes the thread safe to leave unattended: an exception escaping here
    would reach CPython's thread bootstrap, which prints a full traceback carrying the
    exception's message — the user data arch §6 keeps out of anything the library says about
    itself. It goes back through :func:`release` rather than calling ``close()`` itself, so the
    thread body is one of the eight callers rather than a ninth close (SPEC-042 FR-002).

    Args:
      sink: The sink to close.
      closing: The in-flight entry the close discharges.

    Returns:
      None.

    Raises:
      None.
    """
    try:
        release(sink, closing=closing)
    except Exception as exc:
        _diag.absorbed("closing a swapped-out sink", exc, "it may still hold its resources")


def closer_grace(deadline: float | None) -> float:
    """The one grace arithmetic: the smaller of the cap and what is left of the budget (FR-003).

    **The cap is the mechanism.** A wait on somebody else's close is the smaller of
    :data:`DEFAULT_CLOSER_GRACE` and what remains of the shutdown's own budget: capped so a stuck
    close cannot hold a process at exit for the whole shutdown budget, and carved from that budget
    so it cannot extend it either.

    Three copies of this stood in the tree before SPEC-054 FR-003 — ``worker._closer_grace``,
    ``_lifecycle._bystander_grace`` and an inline one inside ``join_closers`` — which is one
    arithmetic asked in three places about the same budget. Since SPEC-078 it is asked once per
    ``shutdown()`` call, by :meth:`_CloseRegistry._open_grace`, and every graced wait of that call
    ends at the instant it fixed.

    Args:
      deadline: The ``time.monotonic()`` instant the caller's budget expires, or ``None`` for an
        unbounded shutdown. ``None`` takes the cap rather than waiting indefinitely: an unbounded
        shutdown is a caller's choice about draining events, not a licence for a stuck close to
        hold the exit.

    Returns:
      Seconds to wait, never negative.

    Raises:
      None.
    """
    if deadline is None:
        return DEFAULT_CLOSER_GRACE
    return max(0.0, min(DEFAULT_CLOSER_GRACE, deadline - monotonic()))


def offer_stop_signal(sink: Sink, stop: threading.Event) -> None:
    """Gives a sink an interruptible-wait signal, if it advertises somewhere to put one.

    The dependency stays one-way (SPEC-027 FR-002): ``sinks`` must not import ``worker``, so the
    holder of the event pushes rather than the sink pulling. It is probed with ``hasattr``, the
    same optional-protocol shape SPEC-026 uses for ``losses()`` — a sink without the attribute
    simply never gets one and backs off uninterruptibly, exactly as before.

    Args:
      sink: The sink to offer the signal to.
      stop: The event that is set when delivery should stop waiting.

    Returns:
      None.

    Raises:
      None. A sink whose ``log_foundry_stop_signal`` is a read-only property, or whose
      ``__setattr__``
        objects, loses interruptibility rather than preventing the caller from proceeding.
    """
    try:
        if hasattr(sink, "log_foundry_stop_signal"):
            sink.log_foundry_stop_signal = stop
    except Exception as exc:
        _diag.absorbed("handing the sink its stop signal", exc, "its backoff stays uninterruptible")


def _get_worker() -> Worker:
    """Returns the process worker, creating it lazily from the configured sink (FR-006).

    The graceful drain is registered via ``atexit`` exactly once, on first creation, so a
    program that logs and exits immediately still flushes its buffered events. The
    double-checked lock makes concurrent first-flushes create exactly one worker.

    The two imports are function-local because ``config`` and ``worker`` both reach this module,
    and they sit **inside** the outer existence check rather than at the top of the function: at
    the top they run on every call, including the fast path this takes on every span close —
    measured 164-179 ns there against 22-24 ns here. Neither of *these two* runs under the lock,
    since both precede the ``with``; ``_ensure_sink()`` inside it may still cold-import a sink
    module, which is unchanged from before the move and is not what this placement is about.

    The fast path is ~11 ns slower than the bare global read it replaced, which is the cost of
    asking :meth:`Runtime.worker_exists` rather than reading a module attribute. Against an
    ~18.5 µs traced call that is 0.06%, and end-to-end timing is identical either side.

    **The orphan record is not discarded without deciding who closes it** (SPEC-044 FR-002).
    ``configure()`` writes ``_config.sink`` *before* it takes this lock, so a ``configure(sink=B)``
    blocked here leaves ``_ensure_sink()`` returning B while the record still names A — which has
    events and has never been closed. Clearing unconditionally lost A's close entirely, with
    ``incomplete_swaps`` at zero because every field of ``Health`` describes a worker and the
    worker was fine (natural rate 6/400). ~~Where the new worker did not adopt the recorded sink,
    this transition owns that close: latch it and release it detached.~~ — struck (SPEC-054
    FR-002). **The build arms its sink and releases nothing.** A sink this build did not adopt
    simply stays owed, and is released by the ``configure()`` that superseded it — a swap always
    follows the config write that made the build see a different sink — or at exit. That is a
    behaviour that disappeared, recorded here rather than left to be discovered.

    **A worker built while a ``shutdown()`` is running is registered for it** (SPEC-044 FR-001).
    ``_shutdown_worker`` raises a depth counter and reads the worker in one critical section, so a
    worker built after that read is registered here and drained by that same call rather than
    left running with nothing to stop it. ~~``sink_released`` covers the ordering where the
    orphan branch already closed this sink.~~ — struck (SPEC-054 FR-002): the flag is retired,
    because this build arms its sink and :func:`_close_owed`'s second pass closes it **after**
    that worker's drain. Where the worker registers before the first pass takes anything, that is
    the sink's only close; where it is built *during* the first pass's close, the sink is closed
    a second time, after the events that worker delivered into it — one close per write-epoch
    (SPEC-045 FR-002) rather than a double.
    The registration is gated on the counter, which is what keeps this off the **sequential**
    ``shutdown()`` → ``@trace`` path, where a fresh worker still delivers by the decision
    :func:`_worker_health` records.

    **The sink is handed its stop signal after the lock is released**, not by ``Worker.__init__``
    inside it: a setter is the sink's own code, and one that logs or calls ``configure()`` on
    this thread waited on the lock its own thread held. The build opens a hand token before it
    publishes the worker, so another thread's orphan log to this sink hands the signal itself
    before it emits (:attr:`Runtime._hands_running`). A build on a thread already handing this
    sink — a setter that runs a ``@trace`` — leaves the hand to the outer one, whose re-check
    compares against the event this build's ``Worker.__init__`` took. The ``try`` covers the
    whole section, so a ``Thread.start()`` that raises out of the build does not strand the
    token.

    **While a stop is running and a close of the sink is in flight, the build hands nothing and
    opens no token**: it marks that close's entry (:meth:`_CloseRegistry.defer_hand`), and the
    participant that discharges the entry pays the hand (:func:`_hand_after_close`). The close is
    a ``shutdown()``'s, already waiting on the sink's signal, which the shutdown set; ``Worker``'s
    build refreshes that set signal, and handing the fresh one made the close wait out the sink's
    whole backoff — measured, a ``@trace`` landing in a ``shutdown(timeout=1)``'s inline close
    took it to 4.00 s, against 0.00 s for an ``info()`` there, which :func:`_will_hand` already
    refused. This is the one section that publishes — the worker, the arm and the refreshed
    ``_stop`` — with no token. A token would send other threads' orphan logs to the locked
    branch, where :func:`_will_hand` refuses a sink whose close is in flight, so those threads
    would hand it nothing either. What is left is the moment between the discharge deleting the
    entry and the payment's section: an orphan log on another thread can take the fast path
    there and emit with the sink still on the shutdown's set event. Limited to a stop in progress
    (:attr:`Runtime._stops_running`, read here under the lock): outside one, a close in flight is
    a swap's or a wrapper's that no shutdown cuts short, and the build hands at once, as before.
    The gate is the whole stop rather than ``_shutdown_running``, because the stop's second pass
    still closes the sink inline after that count drops; so a build in the stop's tail, whose
    worker that stop never stops, is deferred too, and if the marked close outlives the stop the
    last stop out pays the hand (:func:`_pay_marks_after_stop`) rather than leave the live worker
    delivering into a sink on the set event until that close ends.
    The mark is asked before :func:`handing_here`, because a build inside a setter's hand to this
    sink would otherwise leave the hand to an outer settle that refuses on that close and leaves
    nobody to pay.

    Args:
      None.

    Returns:
      The process-wide worker.

    Raises:
      Exception: Whatever constructing the sink or worker raises.
    """
    worker = _state.worker_exists()
    if worker is None:
        from log_foundry.config import _ensure_sink
        from log_foundry.worker import Worker

        token: object | None = None
        sink: Sink | None = None
        try:
            with _state._lock:
                worker = _state.worker_exists()
                if worker is None:
                    _register_exit_handler()
                    sink = _ensure_sink()
                    deferred = bool(_state._stops_running) and _state._closes.defer_hand(sink)
                    if not deferred and not handing_here(sink):
                        token = _open_hand()
                    worker = Worker(sink, offer_signal=False)
                    _state._worker = worker
                    if _state._shutdown_running > 0:
                        _state._late_worker = worker
                    _state._closes.arm(sink)
                    _state._move()
            if token is not None and sink is not None:
                _hand_out([sink], worker._stop)
        finally:
            if token is not None:
                _state._hands_running.discard(token)
    return worker


def _register_exit_handler() -> None:
    """Registers the one ``atexit`` handler that covers both delivery paths (SPEC-031 FR-006).

    One registration, not two, and one flag guarding it. :func:`_shutdown_worker` handles the
    worker path *and* the orphan path, so an orphan log arming this does not cost a later
    ``@trace`` its exit drain — which reusing a worker-only registration flag would. Two
    handlers would be worse still: ``atexit`` runs LIFO, so the second would close a sink the
    first had already closed. What is made once-only is the *close*, not the registration.

    Callers hold ``_state._lock``.

    Args:
      None.

    Returns:
      None.

    Raises:
      None.
    """
    if not _state._atexit_registered:
        atexit.register(_shutdown_worker)
        _state._atexit_registered = True


def _note_orphan_emit(sink: Sink) -> None:
    """Records which sink a level call with no span reached (SPEC-031 FR-006, SPEC-033 FR-001).

    This arms the exit-time close, and it is deliberately keyed on an event having *landed*
    rather than on a sink existing: ``configure()`` runs ``_ensure_sink()`` unconditionally, so a
    bare ``configure(service=…)`` has already built a ``StdoutSink``, and keying on that would
    close a sink nothing was ever written to.

    It records the sink **object**, not a flag. ``configure()`` assigns ``_config.sink`` before
    it calls the swap, so by the time anything could close the previous sink the config no longer
    names it — a boolean cannot say which one is owed. A sink written to after its close is
    armed again, and closed again (SPEC-045, invariant 5).

    The unlocked fast path is a stale read, never an invalid one: a reference read is atomic, a
    stale mismatch simply takes the lock and re-checks, and a stale match is reachable only when
    an emit races a close — the lifecycle error SPEC-030 documents rather than one introduced
    here. It also declines while any hand token is open (:attr:`Runtime._hands_running`), reading
    the token **after** the fields a deciding section publishes, so a thread that sees a sink
    armed or a signal replaced takes this branch and hands the sink its signal before emitting.
    That read is what the fast path adds; the locked branch lives in :func:`_arm_and_hand`, so its
    locals and ``try`` cost the fast path nothing. Measured with ``timeit`` on an armed sink, best of nine
    runs of a million calls, twelve runs each, interleaved, the fast path took 50.6-55.9 ns at
    ``624d817f`` and 60.5-62.5 ns at ``30138ef6``, against about 7.5 µs for a whole orphan
    ``info()`` to a sink that does nothing. Folding :attr:`Runtime._signal_owed` into the token
    set would save one more attribute read and was not taken: the flag has its own fork, cap and
    reset semantics that a sentinel token would have to restate.

    The stop-signal offer is keyed on the *sink* rather than on the record (SPEC-033 FR-004), so
    it also reaches a sink that is latched closed and still being emitted to; an arming-keyed
    offer would leave that one holding the shutdown's set event and backing off not at all.

    **The section decides, the hand runs after it.** A setter is the sink's own code, and one that
    logs, flushes, shuts down or calls ``configure()`` on this thread waited on the lock its own
    thread held: measured at ``624d817f``, every such call hung. So the section chooses the sinks
    and the event (:func:`_will_hand`, :func:`_owed_candidates`), arms, and opens a hand token only
    when it published something — an arm, a replaced ``_stop`` or a cleared debt — and
    :func:`_hand_out` runs with no library lock held. An emit taking this branch only because
    another thread's token is open publishes nothing and hands without a token, which is what
    keeps one hand from begetting another under concurrent logging.

    **A sink a setter released is armed again.** A setter that swaps the sink or shuts down can
    close the very sink this call is about to write to, and that write makes it owed a second
    close (invariant 5). :func:`rearm_if_released` reads the record without the lock and takes it
    only on a miss, so the ordinary hand costs no acquisition.

    **A forked child's first orphan log also takes the locked branch**, through
    :attr:`Runtime._signal_owed`. The child's ``_stop`` is fresh and unset while the inherited
    sink is owed, so the fast path would return and leave the sink holding the parent's copy of
    the event; :func:`_owed_candidates` says why, and what the branch then hands.

    Args:
      sink: The sink this call is about to emit to.

    Returns:
      None.

    Raises:
      None.
    """
    if (
        id(sink) not in _state._closes.owed
        or _state._stop.is_set()
        or _state._signal_owed
        or _state._hands_running
    ):
        _arm_and_hand(sink)


def _arm_and_hand(sink: Sink) -> None:
    """The locked branch of :func:`_note_orphan_emit`: decide under the lock, hand after it.

    A function of its own so the orphan fast path is its condition and nothing else, with no
    locals to set up on the call every orphan log makes; :func:`_note_orphan_emit` carries the
    reasoning for both halves.

    Args:
      sink: The sink this call is about to emit to.

    Returns:
      None.

    Raises:
      None.
    """
    token: object | None = None
    event: threading.Event | None = None
    try:
        with _state._lock:
            armed = id(sink) in _state._closes.owed
            debt = _state._signal_owed
            hands = _owed_candidates()
            if _will_hand(sink) and not any(owed is sink for owed in hands):
                hands.append(sink)
            if hands:
                if not armed or debt or _state._stop.is_set():
                    token = _open_hand()
                event = _state.refresh_stop_signal()
            _state._signal_owed = False
            if not armed:
                _register_exit_handler()
                _state._closes.arm(sink)
                _state._move()
        if event is not None:
            _hand_out(hands, event)
            rearm_if_released(sink)
    finally:
        if token is not None:
            _state._hands_running.discard(token)


def _adopt_factory_after_fork() -> None:
    """Gives a forked child a sink factory wrapper of its own, so it builds its own sink.

    Registered after :func:`_clear_after_fork` and before :func:`_rebuild_worker_after_fork`
    (SPEC-079 FR-006). When the inherited config carries a ``sink_factory``, it builds a new
    ``_FactorySink`` for it — which calls no factory — stamps it for this child, rebinds the
    child's config to it, and when a worker exists points the worker at it and arms it in the owed
    record, under ``_state._lock``, which the roster replaced. The worker rebuild that follows
    offers it the child's stop signal. The child's first emit then builds the child's own sink,
    which the child owns and closes; the parent's wrapper and whatever it built stay inherited,
    owed, refused their close and sent nothing.

    It calls no factory, does no I/O and waits on nothing, so it cannot delay ``fork``. With a
    worker present the stored state follows the worker, so the arm changes no state and no
    ``_move`` is needed; without one, nothing is armed until the child's first orphan emit. The
    wrapper class is read from ``sys.modules``: it is loaded whenever a factory was configured,
    and a child must not import.

    Args:
      None.

    Returns:
      None.

    Raises:
      None. A failure is absorbed by ``_fork`` like any handler's.
    """
    from log_foundry import config

    factory = config._live_config().sink_factory
    if factory is None:
        return
    module = sys.modules.get(f"{_fork._PACKAGE}.sinks._factory")
    if module is None or not _fork._is_module(module):
        return
    adopted = _fork._MODULE_DICT(module)["_FactorySink"](factory)
    stamp(adopted, built=True)
    config._rebind(sink=adopted)
    worker = _state.worker_exists()
    if worker is not None:
        with _state._lock:
            worker.sink = adopted
            _state._closes.arm(adopted)


def _rebuild_worker_after_fork() -> None:
    """Gives a forked child a drain thread of its own, or a retired worker (SPEC-039 FR-002).

    Registered with ``_fork`` at import rather than reached for by it, which is what keeps that
    module free of an import of this one (FR-006). It runs on the child's only thread, after the
    locks are its own again, so it may take them.

    The predicate is deliberately **not** ``_state.live_worker()``, even though the two agree today.
    What is being asked is whether to start a thread, and the answer must survive a later
    reading of liveness — so it is hoisted into ``resume`` and passed, where the roster files
    it as its own decision rather than as a call whose category a reader has to chase.

    ``_state._lock`` is deliberately **not** taken. There is one thread here by construction,
    and the lock was re-initialised moments ago: a child that took it would be taking a lock no
    other thread can contend for, at the one moment in the process's life when that is provably
    true.

    Args:
      None.

    Returns:
      None.

    Raises:
      None. ``_fork`` absorbs and announces a handler's failure, so a child whose worker cannot
        be rebuilt still has working locks.
    """
    worker = _state.worker_exists()
    if worker is None:
        return
    resume = _state.live_worker() is not None
    worker._reinit_after_fork(resume=resume)


_SETTLE_ROUNDS = 4
"""How many times one stop-signal hand offers before it leaves the rest to the next call.

A hand offers again while the process signal moved under it (:func:`_settle_signal`), and that
ends as soon as nothing replaces the signal between an offer and the read after it. Only
something replacing it on every round keeps the loop going — a setter that itself sets the
signal and causes a refresh, say — so the rounds are bounded, and the rest is handed by the next
orphan log or ``flush()`` through :attr:`Runtime._signal_owed` rather than by spinning here. Four
leaves room for a shutdown racing one hand more than once, which no ordinary process does.
"""


def _mid_hand() -> dict[int, int]:
    """Returns this thread's record of the sinks it is handing a stop signal to, by ``id``.

    The value is a depth, not a flag, so a hand nested inside another to the same sink cannot
    clear the outer one's mark; :func:`handing_here` keeps that from happening, and the depth
    keeps it harmless if it ever does.

    Args:
      None.

    Returns:
      The live mapping of ``id(sink)`` to how many hands to it this thread is inside.

    Raises:
      None.
    """
    local = _state._handing_here
    ids: dict[int, int] | None = getattr(local, "ids", None)
    if ids is None:
        ids = {}
        local.ids = ids
    return ids


def handing_here(sink: object) -> bool:
    """Whether this thread is inside a stop-signal hand to *sink* right now.

    The one test of it. A setter that logs to its own sink, builds the worker on it, or emits to
    a factory wrapper whose built sink it is, reaches a decision about the sink it is being
    handed; deciding to hand it again would run the setter again, recursively, so every decision
    asks this first — :func:`_will_hand`, :func:`_get_worker` and the factory wrapper's build.

    Args:
      sink: The sink a hand is being decided for.

    Returns:
      Whether a hand to that sink is already on this thread's stack.

    Raises:
      None.
    """
    return id(sink) in _mid_hand()


@contextlib.contextmanager
def handing(sink: object) -> Iterator[None]:
    """Marks this thread as handing *sink* its stop signal for the length of the block.

    Args:
      sink: The sink being handed a signal.

    Returns:
      A context manager; the mark is removed when the block exits, however it exits.

    Raises:
      None.
    """
    ids = _mid_hand()
    key = id(sink)
    ids[key] = ids.get(key, 0) + 1
    try:
        yield
    finally:
        left = ids[key] - 1
        if left:
            ids[key] = left
        else:
            del ids[key]


def _builds_here() -> list[tuple[Callable[[], None], Callable[[], None]]]:
    """Returns this thread's stack of factory builds in progress, as their yield callbacks.

    Args:
      None.

    Returns:
      The live list of ``(start, end)`` pairs, innermost build last.

    Raises:
      None.
    """
    local = _state._building_here
    stack: list[tuple[Callable[[], None], Callable[[], None]]] | None = getattr(
        local, "yielders", None
    )
    if stack is None:
        stack = []
        local.yielders = stack
    return stack


@contextlib.contextmanager
def building(start: Callable[[], None], end: Callable[[], None]) -> Iterator[None]:
    """Marks this thread as running a sink factory for the length of the block.

    The factory wrapper runs its factory inside this, with no lock held, so a call the factory
    makes back into the library can find the build and release the threads waiting on it
    (:func:`yielding`).

    Args:
      start: Called by :func:`yielding` as a yield begins: threads waiting on this build stop
        waiting and refuse their emit, and threads arriving at it refuse too.
      end: Called as that yield ends, so threads arriving afterwards wait for the build again.

    Returns:
      A context manager; the mark is removed when the block exits, however it exits.

    Raises:
      None.
    """
    stack = _builds_here()
    entry = (start, end)
    stack.append(entry)
    try:
        yield
    finally:
        stack.remove(entry)


@contextlib.contextmanager
def yielding() -> Iterator[None]:
    """Releases the threads waiting on any factory build this thread is running, for the block.

    ``flush()``, ``shutdown()`` and a ``configure()`` that changes the sink wrap the part of
    themselves that can wait on the drain thread in this. Called from inside a sink factory, that
    wait may be on a drain thread that is itself waiting for the factory to finish, which is a
    cycle no lock order breaks: the factory must not finish before the call returns, and the drain
    thread cannot run before the factory finishes. So for the length of the call every thread
    waiting on this thread's builds, and every thread arriving at one, refuses its emit as a
    delivery failure instead of waiting; the worker retries the batch under ``max_retries`` and
    the orphan path counts it lost. When the call returns, threads arriving at the build wait for
    it again. Outside a factory the stack is empty and this is one thread-local read.

    Each build's ``end`` runs for exactly the builds whose ``start`` ran, innermost first, however
    the block exits.

    Args:
      None.

    Returns:
      A context manager.

    Raises:
      None.
    """
    stack = getattr(_state._building_here, "yielders", None)
    if not stack:
        yield
        return
    started: list[Callable[[], None]] = []
    try:
        for start, end in list(stack):
            start()
            started.append(end)
        yield
    finally:
        for end in reversed(started):
            end()


def _will_hand(sink: Sink) -> bool:
    """Whether a deciding section hands *sink* the process's stop signal (SPEC-033 FR-004).

    An orphan-only process never received one before SPEC-033 FR-004 — the worker offered the
    signal to its own sink and there is no worker here — so SPEC-027's guarantee that a shutdown
    cuts a backoff short was false on this path, and the inline close at exit can sit behind an
    uninterruptible wait held by another orphan writer.

    The skip is keyed on the **moment**, not on a worker merely existing. A retired worker keeps
    its old sink forever (``Worker.retarget`` declines once retired) while every
    orphan event goes to a newly configured one, so skipping on existence would leave that live
    sink uninterruptible for the rest of the process. Where a worker is draining into the sink,
    the owner's ``_stop`` is already there and is the event its drain loop waits on; replacing it
    would leave the drain thread serving a full backoff across ``Worker.stop``'s join, which is
    the global pause SPEC-027 exists to remove.

    Liveness alone is not the predicate either, which is why the moment is its own category
    (SPEC-035 FR-001, SPEC-054 FR-004). :func:`live_worker` was wrong: the count moves on
    **entry** to ``_shutdown_worker``, so for the whole of the drain the skip stopped applying and
    an orphan log handed the sink a fresh unset event — precisely the one the drain thread was
    about to wait on. Measured, a 20 s backoff against ``shutdown(timeout=3)`` was then still
    outstanding when the shutdown expired at 3.01 s with the sink left open — the wait was never
    cut, not merely slow. Identity alone is wrong in the opposite direction: it skips for a worker
    whose shutdown has **finished**, leaving a sink still being written to holding a set event
    that can never clear, which is SPEC-033 FR-004's tight retry loop and is covered by a test
    that spec shipped. The predicate is therefore :meth:`Runtime.in_flight` — the identity
    **conjoined with** ``Worker.draining``, plus a close registered against the sink — since
    without the identity an orphan log to sink Y would be skipped merely because a live worker is
    draining into sink X. That method's docstring carries the rest of the reasoning, including
    why the close needs the *other* moment predicate.

    A fresh event replaces one that is already set, because an ``Event`` never clears and
    ``sinks/_retry.wait`` returns immediately on a set one. **Except while a release of this sink
    is in flight** (SPEC-044 FR-003), where the replacement cancels the signal the close is
    waiting on: measured, an ``info()`` landing inside the close made ``shutdown()`` serve an 8 s
    backoff in full, against 0.00 s with no racing log, on both delivery paths. Returning rather
    than handing the set event is the direct expression of "the signal it holds is not replaced".
    The worker build does not ask this; during a stop it declines for a close in flight by
    marking that close, which pays the hand once it is over (:func:`_hand_after_close`). A refusal
    here needs no payment, because the orphan log's next emit meets the set signal in the fast
    path and repairs its sink; a worker delivers without passing through here, so its sink does.

    **And except while this thread is already handing it one** (:func:`handing_here`), where a
    second hand would run the setter inside itself. The hand that is already running settles the
    sink on the current event, so skipping costs nothing.

    **A sink a live worker drains into is handed the worker's own event when that is the current,
    unset signal.** That hand replaces nothing — it is the object the drain already waits on — and
    it is needed: the worker build hands its sink the signal after its section, so a log on
    another thread that meets the build's token would otherwise skip, as the moment predicate
    says, and emit to a sink still holding no signal. The caller's ``refresh_stop_signal()``
    returns that same unset object. A set signal, a worker holding another event, or a close in
    flight keeps the skip, which is the case SPEC-035 FR-001 and SPEC-044 FR-003 measured.

    The moment half is :func:`_may_replace`, which :func:`_settle_signal` asks too.

    Callers hold ``_state._lock``. It decides only: the caller hands after releasing the lock.

    Args:
      sink: The sink a hand is being decided for.

    Returns:
      Whether to hand it the process's signal.

    Raises:
      None.
    """
    return not handing_here(sink) and _may_replace(sink)


def _may_replace(sink: Sink) -> bool:
    """Whether the moment allows handing *sink* the process's current signal (SPEC-035 FR-001).

    The moment half of :func:`_will_hand`, without its this-thread clause, and the one test
    :func:`_settle_signal`'s re-check asks before it hands again. One test for both, because the
    re-check bailing out on a bare :meth:`Runtime.in_flight` left a set event on a live sink: a
    late hand stored a shutdown's set event after a fresh worker's build had handed the sink the
    new unset one, the re-check saw the new worker draining into the sink and returned, and with
    the sink armed and the signal unset the fast path never repaired it — measured, a 0.5 s
    backoff then waited 0.000 s (SPEC-033 FR-004's tight retry loop). Handing that worker's own
    current, unset event replaces nothing, so it is allowed here as in the decision.

    A sink nothing is using may be handed the signal. A sink a live worker drains into may be
    handed that worker's own event when it is the current, unset signal and no close of the sink
    is in flight: that is the object the drain already waits on. Anything else in flight keeps
    the signal it holds (SPEC-044 FR-003).

    Callers hold ``_state._lock``.

    Args:
      sink: The sink a hand is being decided for.

    Returns:
      Whether handing it the current signal replaces nothing something in flight relies on.

    Raises:
      None.
    """
    if not _state.in_flight(sink):
        return True
    owner = _state.worker_exists()
    stop = _state._stop
    return (
        owner is not None
        and owner.sink is sink
        and owner._stop is stop
        and not stop.is_set()
        and not _state._closes.in_flight(sink)
    )


def _owed_candidates() -> list[Sink]:
    """The owed sinks a signal debt hands the process's signal to (invariant 3).

    The roster gives a forked child a fresh ``_stop``. A sink of the package's own classes is
    repaired by the at-fork walk, whose memo hands it that same fresh event, but a sink of the
    caller's own class that no live worker delivers to still holds the parent's copy, which the
    child's ``shutdown()`` never sets. Measured with no live worker, a thread inside such a sink's
    backoff waited it out in full after the child's ``shutdown()``, where an unforked process is
    cut short at once. That held for a child forked after the parent's worker had been shut down
    too, and for a sink inside ``MultiSink``, ``FilteringSink`` or ``TransformSink``. A hand that
    could not settle within :data:`_SETTLE_ROUNDS` leaves the same debt.

    The hand is not made at fork, because handing a signal runs the sink's own code. Nothing
    waits on such a sink's signal before the child's first call that delivers to it: an orphan
    log, through :func:`_note_orphan_emit`, or a ``flush()``, through :func:`_flush_live_sink`,
    which calls each owed sink's own ``flush``. A ``shutdown()`` reaches owed sinks' ``close``
    without settling first, and is safe for a different reason: a sink of the caller's own class
    inherited from the parent is refused its close in the child, and the only inherited sinks a
    child may close are the package's own, which already hold the child's event. The log and the
    flush both decide this in their one section before reaching any sink. They are twins
    (invariant 6): settling the debt in the log alone left a ``flush()`` before the first log
    waiting on the parent's event.

    It names every owed sink, not only the one a log is about to reach, because a sink is owed for
    as long as the child may still deliver to it, filtered by :func:`_will_hand`, so a sink a
    resumed worker is draining into keeps the event the worker's rebuild handed it, and a sink
    whose close is in flight keeps the one the close waits on. The caller clears the flag in the
    same section, after opening its hand token, so the locked branch is paid once per debt.

    Callers hold ``_state._lock``.

    Args:
      None.

    Returns:
      The sinks to hand the process's signal, empty when nothing is owed.

    Raises:
      None.
    """
    if not _state._signal_owed:
        return []
    return [sink for sink in list(_state._closes.owed.values()) if _will_hand(sink)]


def _open_hand() -> object:
    """Opens a hand token, before the deciding section publishes anything.

    Called only by a section that publishes: an arm, a replaced ``_stop``, a cleared
    :attr:`Runtime._signal_owed` or a worker. Its caller discards the token in a ``finally`` once
    the hand is over; :attr:`Runtime._hands_running` carries the ordering argument.

    Callers hold ``_state._lock``.

    Args:
      None.

    Returns:
      The token, for the caller to discard.

    Raises:
      None.
    """
    token = object()
    _state._hands_running.add(token)
    return token


def _hand_out(sinks: list[Sink], event: threading.Event) -> None:
    """Hands each sink the stop signal its deciding section chose, holding no lifecycle lock.

    The only library lock a caller may still hold across it is ``config._sink_lock``, which a
    setter's own ``configure()`` never waits on, and it reaches here held by three routes: the
    no-worker swap's ``configure()``, handing its new sink; a deferred hand paid at a close's
    discharge (:func:`_hand_after_close`) by a ``shutdown()`` that a sink's own code ran on a
    swap's thread, which holds that lock across the whole swap; and a stop-end payment
    (:func:`_pay_marks_after_stop`) by such a ``shutdown()``, run inside a swap's setter. Each sink is marked as being handed on this
    thread (:func:`handing`) for the length of its hand, so a setter that logs to it, or builds
    the worker on it, does not hand it again. Every caller has already asked
    :func:`handing_here`, through :func:`_will_hand` or directly.

    Args:
      sinks: The sinks to hand it to.
      event: The process's stop signal as the section left it.

    Returns:
      None.

    Raises:
      None.
    """
    for sink in sinks:
        with handing(sink):
            _settle_signal(sink, event)


def _settle_signal(sink: Sink, event: threading.Event) -> None:
    """Offers *sink* the process's current stop signal until it holds it, for bounded rounds.

    Deciding under the lock and handing after it opens a gap the atomic offer did not have: two
    hands to one sink can land in either order. One thread decides on ``E``, a shutdown sets
    ``E``, another refreshes to ``F`` and hands it, and the first thread's late hand leaves the
    sink holding the set ``E`` while the fast path, seeing ``F`` unset, never refreshes it —
    SPEC-033 FR-004's tight retry loop. So after each offer it reads ``_state._stop`` without the
    lock; if the signal is still the one it handed, the sink holds the current signal, because
    whichever thread wrote it last reads after its own write. Otherwise it takes the lock, leaves
    alone a sink :func:`_may_replace` says something in flight relies on, and offers the current
    signal again.

    **The rule: a settle only ever hands the current signal, and never creates one.** It reads
    ``_state._stop`` as it is, set or not, and never calls ``refresh_stop_signal``, so it cannot
    publish an event the rest of the runtime has not seen. A round that refreshed did exactly
    that inside a shutdown: between ``_shutdown_worker`` setting the signal and its close taking
    the sink, no moment predicate can see the close coming, and a late hand's round replaced the
    set signal with a fresh unset one the sink then held into its close — measured, a
    ``shutdown(timeout=2)`` took 8.00 s against a sink's 8 s backoff, against 0.00 s with no late
    hand. Handing the current signal when it is set leaves the sink exactly where the atomic offer
    left it, and the next orphan log or ``flush()`` refreshes it, since the fast path declines on
    a set signal. So **a late hand never delivers an unset event into a shutdown's close**
    (SPEC-044 FR-003): every event it hands is the one a shutdown sets.

    The rounds are capped at :data:`_SETTLE_ROUNDS`, which only something replacing the signal on
    every round can reach; past them it sets :attr:`Runtime._signal_owed` in the last round's
    section, so the next orphan log or ``flush()`` hands every owed sink the current signal. The
    lock is read as ``_state._lock`` on each round, never held in a local across an offer, so a
    setter that forks leaves the continuation taking the child's lock and handing the child's own
    event.

    Args:
      sink: The sink being handed a signal.
      event: The signal its deciding section chose.

    Returns:
      None.

    Raises:
      None.
    """
    for remaining in range(_SETTLE_ROUNDS - 1, -1, -1):
        offer_stop_signal(sink, event)
        if _state._stop is event:
            return
        with _state._lock:
            if not _may_replace(sink):
                return
            if not remaining:
                _state._signal_owed = True
                return
            event = _state._stop


def owes_close(sink: object) -> bool:
    """Whether the owed-close record names *sink* right now, read without the lock.

    One atomic dict lookup, so a stale answer is the most it can be; the factory wrapper's build
    reads it at entry to know whether a release during its hand is one it must answer.

    Args:
      sink: The sink to look up.

    Returns:
      Whether the library owes it a close at this instant.

    Raises:
      None.
    """
    return id(sink) in _state._closes.owed


def rearm_if_released(sink: Sink) -> None:
    """Arms *sink* again when a stop-signal setter released it before this call writes to it.

    A setter that swaps the configured sink or shuts down can close the sink its own hand is for,
    and the emit that follows the hand then writes to a sink already closed, which invariant 5
    makes owed a second close (SPEC-045). The record is read without the lock, so a hand that
    released nothing costs no acquisition; only a miss takes the lock and arms.

    It does not call :meth:`Runtime._move`, because the arm cannot change the stored state: a
    setter releases an owed sink only through a swap, which arms the sink it installs, or through
    a shutdown, which moves the retirement count, so the record is never armed here from empty in
    a process with no retirement and no live worker. A call would be the equivalent mutant
    :meth:`Runtime._move`'s docstring excludes.

    Args:
      sink: The sink about to be written to.

    Returns:
      None.

    Raises:
      None.
    """
    if id(sink) in _state._closes.owed:
        return
    with _state._lock:
        if id(sink) in _state._closes.owed:
            return
        _register_exit_handler()
        _state._closes.arm(sink)


def _close_taken(sink: Sink, closing: _Close) -> None:
    """Closes one sink the closer took, absorbing whatever the close raised (SPEC-046).

    Factored out so the inline close and the threaded ones are provably the same call, and
    because a thread body must not raise: an exception escaping one reaches CPython's bootstrap,
    which prints a traceback carrying the message arch §6 keeps out of anything the library says
    about itself. It is the same guard :func:`_close_guarded` applies for a swapped-out sink, with
    the ``_diag`` text of the site it actually is (SPEC-029).

    It records nothing of its own, and needs to: :func:`_close_owed` takes the sink out of the
    owed record and records its close in flight **in one call**, :meth:`_CloseRegistry.take`,
    under ``_state._lock``, which is what stops two callers performing the same close (SPEC-054
    FR-002, FR-003). Handing the entry down rather than making one here is what lets a caller take
    a sink before deciding who performs the close, detached closes included.

    Args:
      sink: The sink to close.
      closing: The in-flight entry to discharge, whatever the close does.

    Returns:
      None.

    Raises:
      None. ``Exception``, never ``BaseException`` — the SPEC-025 line. On the calling thread that
        keeps a ``KeyboardInterrupt`` or ``SystemExit`` reaching the caller as that decision
        requires; on a fan-out thread it cannot, because there is no caller to reach. CPython's
        ``threading.excepthook`` announces an interrupt itself and discards a ``SystemExit``, so
        one raised by a threaded close is reported by the runtime rather than by this library and
        does not propagate. That is :func:`_close_guarded`'s shipped behaviour too; it is stated
        here rather than left to be discovered.
    """
    try:
        release(sink, closing=closing)
    except Exception as exc:
        _diag.absorbed("closing the sink", exc, "it may still hold its resources")


def _live_config_sink() -> Sink | None:
    """Returns the configured sink, for callers that must not import config at module scope.

    Args:
      None.

    Returns:
      The sink the config names, or None when none is set.

    Raises:
      None.
    """
    from log_foundry.config import _live_config

    return _live_config().sink


def _inline_close_choice(owed: list[Sink], worker: Worker | None) -> Sink:
    """Picks the owed sink whose close stays on the calling thread (SPEC-046 FR-001).

    Ordered: the **worker's own sink** where a worker holds one of these, then the **configured**
    sink where it is among those owed, then the most recently armed. Keeping the live sink inline
    is what preserves SPEC-030's decision that ``shutdown()``'s own close runs on the thread that
    called it, and the worker's sink comes first because the two can differ — after a declined
    swap the config names B while the worker still delivers to A (SPEC-035 FR-003), and putting
    A's close on a fan-out thread would expose it to the SPEC-028 objection: a bystander returning
    after the grace lets the interpreter exit through a daemon mid-``commit()``. The last
    fallback is what keeps the single-owed-sink case free of a thread it does not need.

    Membership is by **identity**, never ``in``. ``list.__contains__`` is ``x is e or x == e``, so
    a sink with a value ``__eq__`` — a dataclass, say, which ``Sink`` permits and no shipped sink
    happens to be — would match an object the record never armed: the inline close would then run
    against a sink that was never latched, and every genuinely owed sink would go to a thread.

    It shares :func:`_delivering_to_an_inherited_sink`'s **config** term and adds one ahead of
    it, which is the whole difference between the two questions: that function asks which sink is
    *installed*, and this one asks which close must stay on the calling thread. After a declined
    swap those differ — the config names the new sink while the worker still delivers to the old
    one — and it is the worker's that must not go to a fan-out thread (SPEC-028's objection).

    Args:
      owed: The sinks this closer took, in arming order and never empty.
      worker: The process worker, or ``None`` where none was built.

    Returns:
      The one to close on the calling thread.

    Raises:
      None.
    """
    live = None if worker is None else worker.sink
    for sink in owed:
        if sink is live:
            return sink
    configured = _live_config_sink()
    for sink in owed:
        if sink is configured:
            return sink
    return owed[-1]


def _close_owed(op: _Operation | None = None, *, bystander_wait: bool = True) -> bool:
    """Closes every owed sink nothing is holding — the one closer, for every exit (FR-003).

    It replaces four functions that each closed sinks on one path: ``Worker._close_if_owed``,
    ``Worker._close_sink``, ``_close_orphan_sink`` and the per-sink ``_close_owed``. There is one
    owed-close record now (FR-002), so there is one thing to take from and one place that decides
    who performs a close.

    **The take, under** ``_state._lock``. Every owed sink that is not :meth:`Runtime.held` goes
    through :meth:`_CloseRegistry.take`, which removes it from the record and records its close in
    flight **in one call**, so there is no instant at which a sink is neither owed nor in flight —
    a gap a preempted orphan emit re-arms and a racing ``shutdown()`` then closes alongside
    (SPEC-054 FR-003). Each take names the entry's rule (SPEC-078 FR-002): ``GRACED`` for a sink
    the worker holds unfenced whose thread has since ended (SPEC-050 FR-004), ``JOINED`` for every
    other. A sink left behind is either inside a live drain thread or already being closed by
    somebody else, and the two are answered differently below.

    **The closes, with the lock released.** SPEC-046's shape: :func:`_inline_close_choice` runs on
    this thread, its entry's rule rebound to ``INLINE`` just before the close, and the rest on
    threads joined in full in a ``finally``, so a ``KeyboardInterrupt`` delivered mid-fan-out
    reaches the caller with every started close joined rather than four abandoned mid-write. The
    choice is made **after** the take, inside the ``try``, so an interrupt there is an interrupt
    between a take and its close and is put back below. A ``GRACED`` entry is released detached
    rather than joined: it already had the swap's whole budget, so it is far more likely stuck
    than slow, and joining it would let one stuck swapped-out sink hold the exit where the graced
    wait at the end of :func:`_shutdown_worker` costs it only what is left of the grace.

    **The wait, and why it is on the in-flight half rather than on the record.** A caller waits on
    every close that was in flight when it took and that is not **its own**
    (:meth:`_CloseRegistry.bystanders`) — SPEC-050's rule that a second caller waits for a close
    rather than returning through it. The trigger is deliberately not "an owed sink was left
    behind": by the time a bystander arrives, the caller inside the close has already
    **discharged** that sink from the record, so the record is empty and the wait would never fire.
    Measured on the shipped reproduction —
    ``tests/test_worker.py::test_a_second_shutdown_waits_for_an_inline_close_it_did_not_claim``,
    with a 0.6 s close — the record-keyed version returned through it at ``closed == 0``, which
    is the defect SPEC-050 FR-002 exists to prevent, re-introduced.

    The wait ends at the call's **one** grace, :attr:`_Operation.grace_end`, which this wait fixes
    if it is the call's first graced wait (SPEC-078 FR-002). The graced join at the end of
    :func:`_shutdown_worker` then gets only what is left of it rather than a second grace:
    measured at ``fda134a``, two independently computed waits made ``shutdown(timeout=30)`` take
    4.00 s against a 2.0 s grace. A sink held only by a *drain thread* — an expired ``shutdown()``,
    or a late worker mid-drain — costs no grace, because it has no entry to wait on. Run
    unconditionally, a second pass defeats SPEC-050 FR-002's two-caller criterion: the first
    caller, just out of its own inline close, re-takes the re-armed sink before the bystander
    returns from its wait.

    **A sink this call did not hand off is put back and its entry discharged**, however the call
    leaves (:meth:`_CloseRegistry.put_back`). A ``KeyboardInterrupt`` delivered between the take
    and a close would otherwise leave an entry nobody sets — that sink is then skipped by the
    signal refresh forever, never taken by a later closer, and every later bystander waits out the
    whole grace on it. Discharging alone is not enough: the take has already removed the sink from
    the record, so an interrupt there left it **owed by nobody**. Measured with an interrupt
    injected at :func:`_inline_close_choice`: three sinks, ``closes=[0, 0, 0]``, the record empty.

    Each entry is dropped from that set immediately **before** its close is handed off, and the
    ``try`` opens on the line after the take rather than after the inline choice — an interrupt
    inside :func:`_inline_close_choice` leaked every entry the take had just made. The residual
    window is the few bytecodes between the drop and the call — the same narrow shape SPEC-050
    FR-002 accepted for the count this replaced, and for the same reason: it contains no call, so
    nothing but a real signal storm can land in it.

    **A put-back entry can carry a hand a worker build declined** while the take held it
    (:meth:`_CloseRegistry.defer_hand`): the interrupt left the sink unclosed, still on the
    shutdown's set signal, with a late worker about to deliver into it. ``put_back`` runs under
    ``_state._lock``, so it only reports the mark; the sinks it names are paid
    (:func:`_hand_after_close`) once the lock is released, and **after** the fan-out closes are
    joined, in a ``finally`` of their own, so a setter that is slow or raises cannot cut that
    join short.

    **It records the call's last take** (SPEC-078 FR-004): in the take's own critical section,
    ``op.drained`` is set when no further take of this call can follow — the second pass, or a
    first pass that waits on nobody once no late worker can still be registered for the call.
    That is what bounds the declining swap. In the same section it drops from
    :attr:`_Operation.unsettled` every swap the call gave up waiting on that has decided since:
    that swap's arm landed before this take, so the sink it installed is closed here, and
    :func:`_announce_unsettled` must not say it may be left open.

    Args:
      op: The ``STOP`` this closer runs for, whose deadline bounds the grace and which carries the
        call's one ``grace_end``. ``None`` only for a direct call from a test, which gets an
        unbounded operation of its own, as the old ``deadline=None`` default gave it.
      bystander_wait: Whether to wait on closes somebody else had in flight. ``False`` for
        :func:`_shutdown_worker`'s second pass, which runs only when there is something for it to
        do and must never wait a second time.

    Returns:
      Whether it waited on a close it did not perform, which is what tells its caller a second
      pass has something to look at.

    Raises:
      BaseException: Whatever the inline close raised, after every fan-out close it started has
        been joined. ``Exception`` from a close is absorbed by :func:`_close_taken`; what reaches
        here is the SPEC-025 pair a caller is owed. The same pair raised by a sink's stop-signal
        setter during a deferred hand paid here or in a release (:func:`_hand_after_close`)
        reaches the caller too, and replaces an exception already on its way out.
    """
    operation = _Operation(_Op.STOP, None) if op is None else op
    joined: list[_Close] = []
    entries: list[tuple[Sink, _Close]] = []
    with _state._lock:
        worker = _state.worker_exists()
        taken = [sink for sink in _state._closes.owed.values() if not _state.held(sink)]
        for sink in taken:
            rule = _Rule.JOINED
            if worker is not None and worker.holds_unfenced(sink):
                rule = _Rule.GRACED
            entries.append((sink, _state._closes.take(sink, rule)))
        mine = {id(sink) for sink in taken}
        operation.unsettled = [swap for swap in operation.unsettled if not swap.decided.is_set()]
        waiting = _state._closes.bystanders(mine) if bystander_wait else []
        if not bystander_wait or not (waiting or operation.late_possible):
            operation.drained = True
    undischarged = {id(sink): (sink, close) for sink, close in entries}
    try:
        inline = _inline_close_choice(taken, worker) if taken else None
        for sink, close in entries:
            if sink is inline:
                continue
            del undischarged[id(sink)]
            if close.rule is _Rule.GRACED:
                release(sink, detached=True, closing=close)
                continue
            closer = threading.Thread(
                target=_close_taken,
                args=(sink, close),
                name="log-foundry-owed-close",
                daemon=True,
            )
            try:
                closer.start()
            except Exception as exc:
                _diag.absorbed(
                    "starting the thread that closes an owed sink",
                    exc,
                    "it is closed inline instead",
                )
                _close_taken(sink, close)
            else:
                _state._closes.attach(close, closer)
                joined.append(close)
        for sink, close in entries:
            if sink is inline:
                close.rule = _Rule.INLINE
                del undischarged[id(sink)]
                _close_taken(sink, close)
    finally:
        owed_hands: list[Sink] = []
        if undischarged:
            with _state._lock:
                for sink, close in undischarged.values():
                    if _state._closes.put_back(sink, close):
                        owed_hands.append(sink)
        try:
            _state._closes.join_joined(joined)
        finally:
            for sink in owed_hands:
                _hand_after_close(sink)
    if not waiting:
        return False
    _state._closes.wait_graced(operation, waiting)
    return True


def _swaps_to_await() -> list[_Operation]:
    """Every swap in flight a shutdown entering now must let decide; the caller holds ``_lock``.

    Read in the shutdown's entry critical section, after the retirement count has moved and the
    stop signal is set, and that placement is what makes the snapshot complete. A swap that is not
    in it reads the worker in a later section, after the retirement, so it finds no live worker
    and meets the drain at its own entry (SPEC-078 FR-004); a swap that is in it read a live
    worker, and its retarget, which re-checks the count after its drain, declines unless it had
    already reassigned. Either way the snapshot holds every swap whose decision may still be
    ahead of it. No lock acquisition is added: this runs inside a section the call
    already takes, so the pinned counts hold.

    **A re-entrant shutdown waits on nothing it could be holding up.** On the drain thread — a
    sink's ``emit``, which a retarget's flush may be waiting on — every swap is skipped, since an
    undecided swap may be waiting on this very thread. On the swap's own thread — a stop-signal
    setter run by the retarget, or a signal handler interrupting ``configure()`` — its own token
    is skipped by ``thread``. Either wait would last the whole closer grace, or until the process
    is killed with a large one. The rule is written as ``config._change_sink``'s prefix,
    ``log-foundry-``, so it also covers a closer thread; that half is harmless rather than needed,
    because a swap joins its closers only after it has set ``decided``, and a decided swap is
    never in the snapshot. **The cost**: a re-entrant shutdown landing between a swap's retarget
    and its decision drains and returns with the installed sink not yet owed, so that sink is
    left open until a later ``shutdown()`` or the exit closes it, and no line says so; the
    displaced one is still closed once, because the swap releases only what the record owes.

    The token set is copied into a tuple first, for :meth:`Runtime._take_ahead`'s reason: its
    discard runs without the lock.

    Args:
      None.

    Returns:
      The tokens of the swaps whose decisions the shutdown waits for, possibly none.

    Raises:
      None.
    """
    if threading.current_thread().name.startswith("log-foundry-"):
        return []
    me = threading.get_ident()
    return [
        swap
        for swap in tuple(_state._swaps_running)
        if swap.thread != me and not swap.decided.is_set()
    ]


def _await_swap_decisions(op: _Operation, swaps: list[_Operation]) -> None:
    """Waits for each swap in flight to take its decision, all ending within the closer grace.

    The fix after SPEC-079 for a shutdown landing between a swap's retarget and its decision: the
    sink the swap installed is the worker's but not yet owed there, so the shutdown drained into
    it and returned with it open, and invariant 5 failed (measured at ``624d817f``, 5 of 5
    forced runs). Waiting here, before the drain and the take, means the installed sink is owed
    by the time the record is read, and the displaced one has been released or left owed by the
    swap's own rules.

    **Bounded by the closer grace, computed now**, because a swap whose retarget is slow to
    notice the stop is somebody else's slow step, which is what the grace exists to cap: an
    unbounded ``shutdown(None)`` must not hang on a stuck swap. What it actually waited is
    recorded as :attr:`_Operation.grace_spent`, in a ``finally`` so an interrupt records it too,
    and :meth:`_CloseRegistry._open_grace` deducts it: one grace per call at most (SPEC-078
    FR-002), without charging the drain to it. A swap not decided in time is proceeded past; the
    displaced sink is still closed once, because the swap releases only a sink the record still
    owes. The swaps given up on are recorded as :attr:`_Operation.unsettled` rather than
    announced here: one may still decide during the drain, and its arm then lands before this
    call's take, which closes the sink it installed — measured with a swap stuck in its first
    flush behind a 3 s ``emit``, whose decline armed B in time for the call to close it while a
    line written at this point said B might be left open. :func:`_close_owed` narrows the record
    at each take and :func:`_announce_unsettled` writes the line after the last. An empty list
    waits on nothing and records nothing, so an uncontended shutdown is unchanged.

    **Not the** ``config._sink_lock`` **barrier.** Holding that lock for the shutdown broke six
    shipped tests and regressed a backoff cut short by the stop signal; passing it as a barrier
    waited out the swap's slow displaced closes, since the lock is held through the swap's closer
    join. The shutdown needs only that no swap is between its retarget and its decision when it
    takes, so it waits on exactly that.

    Args:
      op: The ``STOP`` doing the waiting, whose deadline caps the grace.
      swaps: The tokens :func:`_swaps_to_await` read at the call's entry.

    Returns:
      None.

    Raises:
      None.
    """
    if not swaps:
        return
    started = monotonic()
    end = started + closer_grace(op.deadline)
    try:
        for swap in swaps:
            swap.decided.wait(max(0.0, end - monotonic()))
    finally:
        op.grace_spent = monotonic() - started
    op.unsettled = [swap for swap in swaps if not swap.decided.is_set()]


def _announce_unsettled(op: _Operation) -> None:
    """Writes the not-decided line, once, if a swap given up on had not decided by the last take.

    Called by :func:`_shutdown_worker` after its last take of the owed record. What is left in
    :attr:`_Operation.unsettled` is a swap that had still not decided when the call last took, in
    that take's own critical section, so whatever it installs is armed after this call has
    finished taking and is left for a later ``shutdown()`` or the exit. A swap that decided in
    time for a take was dropped from the record there, and its sink was closed by this call, so
    no line is written for it. The residual window runs from the end of a swap's decision
    section to its ``decided.set()``, a few bytecodes with no call in them, and a swap caught
    there writes a line that was not needed, which is the safe direction.

    Args:
      op: The ``STOP`` whose wait gave up on some swaps, or on none.

    Returns:
      None.

    Raises:
      None.
    """
    if op.unsettled:
        _diag.pending(
            "shutdown() could not wait for a sink change in flight to take effect; the sink it "
            "installs may be left open until the process exits"
        )


def _shutdown_worker(timeout: float | None = DEFAULT_SHUTDOWN_TIMEOUT) -> None:
    """Drains and closes every sink the library owes, backing ``shutdown()`` on both paths.

    The ``atexit`` registration binds this function, so the exit path gets the bounded form
    and its default (SPEC-027 FR-004) — an unbounded join in an ``atexit`` handler is a
    process that will not exit. Idempotent on both paths.

    **One shape for both branches now** (SPEC-054 FR-003): move the count, stop whichever worker
    there is, run the closer, and grant the closer grace once at the end. The orphan branch and
    the worker branch used to reach two different closers and arrange the grace between them by
    hand, which is what let a worker-path ``shutdown()`` pay for an orphan close of a sink it
    never touched.

    ``_state.retirements`` is moved unconditionally and is the only latch, which is
    what makes ``health().retired`` truthful for a process that shut down without ever
    building a worker (SPEC-031 FR-006). No worker is created here to answer it: standing up a
    thread at exit to prove there is nothing to drain is pure cost, the same refusal
    :func:`_swap_sink` and :func:`_flush_worker` already make.

    ``_state._stop`` is set **before** delegating, so a sink parked in a backoff is released
    while :meth:`Worker.stop` is still draining rather than after it has given up waiting. A
    **late** worker holds a different event — its build refreshed the signal, for the reason a
    sink adopted after a ``shutdown()`` does — so ``stop`` sets the event it actually holds.
    Measured without that: a 3 s backoff bounded the stop at 3.01 s, against 0.06 s with it.

    **The count and the worker read are one critical section** (SPEC-044 FR-001).
    Unlocked, a worker built between them sent this down the no-worker branch: the drain thread
    was never stopped and its sink never closed, while ``health()`` reported ``retired=True`` and
    later logs were delivered by a live worker with ``submitted_after_shutdown`` at zero.
    ``atexit`` recovered it in a process that exits; a frozen serverless container never does,
    which is the deployment this call exists for. Closing the read window alone only narrows it,
    so ``_shutdown_running`` stays raised for the whole of the no-worker branch and
    :func:`_get_worker` registers what it builds under the same lock — read and lowered in the
    last critical section, so there is no gap between the two.

    It is a **depth counter, not a flag**. Two concurrent ``shutdown()`` calls are documented as
    normal, and with a boolean the first to finish lowers it while the second is still running —
    measured, a worker built at that instant was registered nowhere and the second call returned
    having stopped nothing, which is the original defect verbatim.

    **Every wait of the worker's stop ends at this call's deadline** (SPEC-078 FR-005). A caller
    that won the claim passes its ``budget`` only for the expiry line; :meth:`Worker.stop` joins
    until the deadline :meth:`Worker.claim_stop` recorded. A caller that lost the claim passes
    what remains of its own deadline, which is what bounds its wait for the winner's drain, and
    a losing caller writes no expiry line, so no rendered text depends on it.

    **The stored state moves with the count** (SPEC-078 FR-003): :meth:`Runtime._move` is the
    last statement of the entry's critical section and of the late worker's, the two sections of
    this call that change the retirement count.

    **The count moves a second time before a late worker is stopped**, under the same lock
    (FR-001). That worker recorded the already-incremented count at its build, so without the
    second move the call that stops it leaves it reading as *live*: delivering nothing, its next
    submission uncounted, and a later swap waiting a whole budget for a fence it cannot confirm.

    **The second pass runs only when there is something for it to do** — the first waited on a
    close somebody else held, or a late worker was stopped — and it never waits itself. Run
    unconditionally it defeats FR-002's two-caller criterion: the first caller, just out of its
    own inline close, re-takes the re-armed sink before the bystander returns from its wait.
    Nothing runs a third.

    **The closer grace is spent once per call** (SPEC-078 FR-002). Every graced wait of the call —
    the first pass's bystander wait and the graced join at the end — ends at the operation's one
    ``grace_end``, fixed by the first of them, after the inline close so SPEC-030's order holds.
    Two independently computed waits measured 4.00 s and 4.01 s against a 2 s grace at
    ``fda134a``. The graced join at the end runs on every path, as ``join_closers`` did, and spends
    nothing when no detached close is alive.

    **A live worker's stop is claimed here, before** ``_state._stop`` **is set** (SPEC-062
    FR-001). That event is the worker's own, and setting it is what cuts a backoff short, so the
    drain thread may wake and decide its next retry at once; :meth:`Worker.claim_stop` records
    this caller's deadline in the same decision that wins the once-only gate, inside this same
    critical section, so the wake finds the deadline and the join that follows belongs to the
    caller who recorded it. The verdict is passed to :meth:`Worker.stop`, which then joins or
    waits against this caller's own timeout. When the winner was the drain thread itself — a
    sink's ``emit`` calling ``shutdown()`` — it cannot join, so the next call from another thread
    takes the gate over in ``claim_stop`` with its own deadline and joins instead (SPEC-068
    FR-006). Nothing here changes for it: the closer leaves the sink a live drain thread may
    still be inside, which on that call is the caller's own thread, and the closer grace still
    applies, so that call can wait on a swapped-out sink's close though it never waits on its own
    thread. A late worker is stopped through ``stop`` without a verdict
    and claims for itself, before it sets the different event it holds.

    **The draining token** (SPEC-077 FR-002) is this call's :class:`_Operation`, added to
    :attr:`Runtime._stops_running` in the first critical section, before ``_stop`` is set, and
    discarded in the outer ``finally`` once the graced join has returned, so
    :attr:`Runtime.state` reads ``DRAINING`` for the whole body on both branches, the late
    worker's stop and the second ``_close_owed`` pass included — and is never left there by an
    exception. The discard takes no lock: it is one ``set.discard``, so the token adds no
    acquisition to this call's pinned count. After it, the last stop out pays any hand a worker
    build declined during the stop on a close that outlives it (:func:`_pay_marks_after_stop`),
    which takes ``_state._lock`` only when such a mark exists.

    **Before it drains, it waits for every swap in flight to take its decision**, within the
    closer grace (:func:`_await_swap_decisions`), from a snapshot read in the entry's critical
    section (:func:`_swaps_to_await`). A swap between its retarget and its decision has installed
    a sink it has not yet armed, and a drain there returned with that sink open. The wait sits
    at the head of each branch — on the worker branch before ``worker.stop``, on the no-worker
    branch as the first statement inside its ``try``, so an interrupt during the wait still
    lowers ``_shutdown_running``. A swap that starts after the entry finds the worker retired and
    meets the drain at its own entry (SPEC-078 FR-004); the one not covered is a swap that starts
    after the entry and retargets a late worker built during a no-worker shutdown, where the
    displaced sink is still closed once and the installed one may be left for the exit. A swap
    the wait gave up on is announced only after the call's last take, by
    :func:`_announce_unsettled`, and only if it had still not decided by then: one that decides
    during the drain has its sink closed by this call.

    Args:
      timeout: Seconds to wait for the drain, or ``None`` to wait indefinitely.

    Returns:
      None.

    Raises:
      BaseException: What a sink's stop-signal setter raised that :func:`offer_stop_signal` does
        not absorb, during a deferred hand this call pays — at a close's discharge
        (:func:`_hand_after_close`) or at the stop's end (:func:`_pay_marks_after_stop`). Both
        run inside a ``finally``, so such a ``KeyboardInterrupt`` or ``SystemExit`` replaces an
        exception already unwinding. The same pair raised by an inline ``close()`` escapes too
        (:func:`_close_owed`).
    """
    op = _Operation(_Op.STOP, timeout)
    claimed = False
    try:
        with _state._lock:
            _state._stops_running.add(op)
            _state.retirements += 1
            worker = _state.worker_exists()
            if worker is None:
                _state._shutdown_running += 1
                op.late_possible = True
            else:
                claimed = worker.claim_stop(op.deadline)
            _state._stop.set()
            swaps = _swaps_to_await()
            _state._move()
        late_worker: Worker | None = None
        waited = False
        if worker is not None:
            _await_swap_decisions(op, swaps)
            worker.stop(op.budget if claimed else _remaining(op.deadline), claimed=claimed)
            waited = _close_owed(op)
        else:
            try:
                _await_swap_decisions(op, swaps)
                waited = _close_owed(op)
            finally:
                with _state._lock:
                    late_worker = _state._late_worker
                    _state._late_worker = None
                    _state._shutdown_running -= 1
                    op.late_possible = False
                    if late_worker is None and not waited:
                        op.drained = True
            if late_worker is not None:
                with _state._lock:
                    _state.retirements += 1
                    _state._move()
                late_worker.stop(_remaining(op.deadline))
        if waited or late_worker is not None:
            _close_owed(op, bystander_wait=False)
        _announce_unsettled(op)
        _state._closes.wait_graced(op, None)
    finally:
        _state._stops_running.discard(op)
        _pay_marks_after_stop()


def _swap_sink(new_sink: Sink, timeout: float | None = DEFAULT_SWAP_TIMEOUT) -> None:
    """Retargets delivery at a new sink, backing a late ``configure(sink=...)``.

    **One function over one record** (SPEC-054 FR-003). It was two branches keeping two records
    in step, plus ``_adopt_declined_swap`` to re-home a sink the worker refused mid-swap; with
    one record the branches differ only in whether there is a drain to fence.

    With a live worker it calls :meth:`Worker.retarget`, which drains, reassigns and fences, and
    reports what it did. The owner then arms ``new_sink`` **whether or not the worker adopted
    it** — which is what ``_adopt_declined_swap`` was for, with nothing left to adopt — and
    decides the old sink:

    - **fenced** — the worker confirmed its drain reached the new sink, so nothing *of the
      worker's* can still be inside the old one. Where :meth:`Runtime.held` also says nothing
      else is, it leaves the record and is released detached, joined to this call's budget.

      **The ``held`` term is not redundant with the fence**, and leaving it out is a second close
      running *concurrently* with a first. ``retarget`` takes the old sink out of the worker's
      own held set when it confirms the fence, and this branch then re-takes ``_lock``; in that
      gap the sink is owed and no longer held, so a concurrent ``shutdown()`` takes it, registers
      it and starts closing it. The registry hands this branch back that *same*
      registration — it is one per sink by design — so the detached release starts a second
      closer into a ``close()`` already in flight. Measured with an injected preemption point at
      two overlapping closes, and at 13 of 120 seeds unforced against a 2 ms ``close()``;
      ``sinks/base.py`` documents tolerance for a *sequential* second close, never two at once.

      **The** ``owes`` **term is the same gap's other half.** A shutdown in that gap whose take
      has already *finished* — it found the old sink owed and unheld, took it and closed it
      inline — leaves nothing in flight for ``held`` to see, and the take this branch used to
      make popped the absent sink with a default and released it a second time on
      ``log-foundry-sink-close`` (measured at ``624d817f``, 20 of 20 forced runs, and the same
      for the exit drain). So the old sink is released only while the record still owes it
      (:meth:`_CloseRegistry.owes`). Skipping the release cannot leave it owed by nobody: every
      route out of the record is a take whose taker performs the close, and the worker's sink is
      always armed — the build arms it, this function arms what it installs, a forked child
      re-arms ``worker.sink`` — so "not owed" here means "taken", never "never armed".
    - **unfenced** — the drain could not be confirmed within the budget, so the old sink stays in
      the record and the worker keeps it among the sinks it may be inside. A later ``shutdown()``
      that finds the drain thread ended closes it (SPEC-050 FR-004); ``incomplete_swaps`` counts
      it, as before.
    - **declined** — the worker retired mid-swap and keeps its old sink forever (SPEC-033 FR-002),
      or retired after it had reassigned, when the shutdown that retired it had already taken
      the displaced sink from the record (:meth:`Worker.retarget`); a retirement after the
      reassign that left the displaced sink owed is reported ``unfenced``. Either way the old sink
      is left to the record — owed for the exit, or already taken by that shutdown — and the new
      one is armed anyway, because it is what ``configure()`` installed and the orphan path will
      deliver to it — SPEC-035 FR-003's guarantee that a declined sink is owned by *somebody*.

      Not arming it was tried and is wrong. It looks like the uniform rule — a sink is armed when
      something takes responsibility for delivering to it — and it satisfies FR-002's "never two
      for one write-epoch" at one reachable site: an orphan log arms the new sink while the swap
      is in flight, a racing ``shutdown()`` closes it, and arming it again on the decline buys a
      second close with nothing written in between. But it costs the guarantee outright, which
      three shipped tests hold: a declined sink that nothing arms is closed by nobody. So the
      redundant close stands, and it is the case FR-002 already enumerates — *a live target is
      closed at exit whether or not anything was written since it was installed*, which is the
      worker path's rule and the reason ``sinks/base.py`` asks for an idempotent ``close()``.

    Every **other** owed sink — neither the old one nor the new one, and not one
    :meth:`Runtime.held` says something may still be inside — is released detached and joined
    to the budget, on both branches. That is the swap superseding sinks nothing is delivering to
    any more.

    The ``held`` term is load-bearing and was not in the first draft of this function. Under one
    record a sink the worker swapped out **without a confirmed fence** is still owed *and* still
    among the sinks its thread may be inside, so a later ``configure()`` would find it "neither
    the old nor the new one" and release it under a live drain thread — which is SPEC-033
    FR-002's measured defect arriving at a new site. Two records hid it, because the stranded
    sink lived on the worker and this loop only ever saw the orphan one. Reproduced by
    ``tests/test_owed_closes.py::test_a_stranded_sink_re_armed_on_the_orphan_path_is_closed_once``
    at ``A.closes == 2``.

    With **no** worker there is no drain to fence, and ``new_sink`` is armed **only when something
    was owed**. A ``configure(A)`` then ``configure(B)`` with nothing ever written must arm
    nothing, which is FR-002's arming rule: a configured sink nothing wrote to is never owed a
    close. On the worker branch it is a worker's *adoption* that arms a sink, and there is no
    adoption here.

    Every discharge from the record and the registration that replaces it happen in **one**
    critical section under ``_state._lock``, so a preempted orphan emit cannot re-arm a sink in a
    gap where it is neither owed nor in flight (FR-003).

    **During a drain with no live worker it declines at entry, while the drain still has a take
    ahead of it** (SPEC-078 FR-004, :meth:`Runtime._take_ahead`): no retarget, no close and no
    arm, from its first critical section. Until its last take the drain in progress owns every
    owed close, and at ``fda134a`` a swap landing there — after the worker's stop had retired it —
    released the worker's sink detached once its drain thread had ended (measured: A's close ran
    on ``log-foundry-sink-close``, a daemon joined only for the grace and killed at exit wherever
    it has reached, the SPEC-028 objection SPEC-030 and SPEC-046 keep the live sink's close inline
    to avoid) and armed the new sink, which the drain then closed though nothing had been written
    to it. The config already names the new sink, and the first event that reaches it arms it as
    any orphan log does. **A live worker in the drain still swaps**: a late worker the drain will
    stop, or one built in the drain's tail that it will not, which delivers (SPEC-044 FR-001);
    declining there would leave a live worker on the old sink with no ``incomplete_swaps`` and no
    line — the lost configure this function's ``Raises:`` disowns. **After the drain's last take
    the swap runs its no-worker branch as before**: a sink armed since — an orphan log re-arming
    the drained sink, say — is taken by nothing left in that call, so declining there left it owed
    and open after the shutdown returned, where this branch releases it detached and arms the new
    one. A swap that entered before the drain is past this check and finishes by the rules above,
    where its retarget declines unless it had already reassigned, and the drain waits for its
    decision before it takes (see the paragraph on ``decided`` below).

    **It is correct for one swap at a time, and its one caller holds ``config._sink_lock``** so
    that two are never interleaved. The decision after the retarget — arm the new sink, release
    the one displaced — is taken on what this call's retarget saw, and a second swap retargeting
    in between makes that stale: the worker went A to C and then C to B, B's swap closed C as the
    sink it displaced, and C's swap then re-armed C, which the exit closed again with nothing
    written in between (measured in 10 of 50 runs at ``641dfc86``, and 0 of 100 with the lock).
    The stale-sink loop above is the second route: B's swap found A, displaced and no longer
    held, still owed and released it, and C's swap then released A as the sink it displaced.
    Re-checking that decision here was rejected: an unfenced second swap leaves its displaced sink
    for a later ``shutdown()``, so a first swap that skipped arming a sink no longer current would
    leave that sink owed by nobody, and each further guard of that kind answers one interleaving.
    The lock makes the pair the sequential case this function already gets right. A call whose
    wait for it outlasted the budget still runs here alongside the holder, announced. **The**
    ``owes`` **term on the fenced branch is not that re-check**: skipping an *arm* can orphan a
    sink, because nothing else may arm it, while skipping a *release* of a sink the record no
    longer owes cannot, because every route out of the record is a take whose taker closes it.
    On the unserialized pair it makes the second route close A once, and a second swap that
    retargets B to C before B's swap has armed B finds B not owed and skips it; B's own swap then
    arms B, so B is closed once, later, instead of twice.

    **A shutdown waits for this call's decision** (:func:`_await_swap_decisions`). Between the
    retarget and the decision the sink installed is the worker's but not yet owed, so a
    ``shutdown()`` taking the record there drained into it and returned with it open (measured at
    ``624d817f``: six events delivered, never closed). The decision is published on the token's
    ``decided`` event, set right after the last critical section — the decision on the worker
    branch, or the absorbed retarget exception; the first section on the no-worker branch — and
    before the closers are joined. Set after the join instead, a shutdown would wait out this
    call's slow detached closes before it drains: the grace would be spent ahead of the drain, a
    close slower than the grace would make it write a false ``pending`` line saying the change
    had not taken effect, and the drain and the take would start that much later. The
    ``finally`` sets it again, so no exit leaves a shutdown waiting on it.

    **With no worker, the new sink is handed its stop signal after the first section**, with no
    lifecycle lock held, and before the closers are joined. Its setter is the sink's own code, and
    a setter calling ``configure()`` back waited on the lock its own thread held. The caller's
    ``config._sink_lock`` is still held across the hand, so that ``configure()`` is re-entrant in
    ``config._change_sink``'s sense: it tries the lock once, swaps unconfirmed and says so. Handing after
    the section, rather than before it, gives that re-entrant ``configure()`` the state this call
    has fully landed: it releases ``new_sink`` as stale and arms its own, which is what the two
    calls made in sequence do. Handed before, this call's section would then find the re-entrant
    call's sink owed and release it as stale under a config that names it. The section opens a
    hand token before it arms, so another thread's orphan log to ``new_sink`` hands the signal
    itself before it emits (:attr:`Runtime._hands_running`). The worker branch is unchanged:
    :meth:`Worker.retarget` already hands outside every lock but ``config._sink_lock``, which a
    re-entrant ``configure()`` never waits on.

    **``decided`` is set before that hand, not after it.** The no-worker decision is whole when
    the first section ends — the stale sinks taken, ``new_sink`` armed, the signal chosen — so a
    shutdown on another thread has nothing left to wait for. Set after the hand, it waited out
    the setter: measured, a ``shutdown(timeout=5)`` behind a setter parked for 3 s took the whole
    2.00 s closer grace and wrote the not-decided ``pending`` line for a decision that had long
    been taken. The shutdown that now proceeds may close ``new_sink`` while its setter is still
    running here, which is the window the stop-signal decision records for a close that starts
    before a pending hand lands: the event this hand delivers is the one that shutdown set at its
    entry, so the setter stores a set signal and the close is not held by it.

    **The swapping token** (SPEC-077 FR-002) is this call's :class:`_Operation`, added to
    :attr:`Runtime._swaps_running` in the first critical section — after the early return for no
    live worker and nothing owed, so a swap with no work never enters ``SWAPPING``, and before any
    stale sink is released, so every close this swap starts runs inside the state. It is
    discarded in the outer ``finally``, after the closers are joined, with no lock: one
    ``set.discard``, so the token adds no acquisition to this call's pinned count.

    **The outer ``finally`` ends with ``_repin``** (SPEC-081 FR-001), on every exit, both early
    returns included: a ``configure(sink=…)`` with nothing ever logged returns early and still
    supersedes a sink, which must go weak in the record or the record grows without bound. It
    runs last, after ``decided`` is set, so a waiting ``shutdown()`` is not held by its walk, and
    with no lifecycle lock held — only ``config._sink_lock``, where the caller took it.

    Args:
      new_sink: The sink to deliver to from now on.
      timeout: Seconds for the drain and the detached closes, or ``None``.

    Returns:
      None.

    Raises:
      None. A retarget that raises is absorbed and announced: the previous sink keeps receiving,
        which is a reported degradation rather than a lost configure.
    """
    op = _Operation(_Op.SWAP, timeout)
    closers: list[threading.Thread] = []
    token: object | None = None
    event: threading.Event | None = None
    try:
        with _state._lock:
            worker = _state.live_worker()
            old = None if worker is None else worker.sink
            if worker is None and not _state._closes.owed:
                return
            if worker is None and _state._column() is State.DRAINING and _state._take_ahead():
                return
            _state._swaps_running.add(op)
            for stale in list(_state._closes.owed.values()):
                if stale is new_sink or stale is old or _state.held(stale):
                    continue
                closer = release(
                    stale, detached=True, closing=_state._closes.take(stale, _Rule.GRACED)
                )
                if closer is not None:
                    closers.append(closer)
            if worker is None:
                hand = _will_hand(new_sink)
                if hand:
                    token = _open_hand()
                _register_exit_handler()
                _state._closes.arm(new_sink)
                if hand:
                    event = _state.refresh_stop_signal()
        if event is not None:
            op.decided.set()
            _hand_out([new_sink], event)
        if worker is not None:
            try:
                outcome = worker.retarget(new_sink, op.deadline)
            except Exception as exc:
                _diag.absorbed(
                    "swapping the log sink",
                    exc,
                    "events may still be delivered to the previous sink",
                )
            else:
                with _state._lock:
                    _register_exit_handler()
                    _state._closes.arm(new_sink)
                    previous = outcome.previous
                    fenced_out = (
                        outcome.verdict == "fenced"
                        and previous is not None
                        and previous is not new_sink
                        and not _state.held(previous)
                        and _state._closes.owes(previous)
                    )
                    if fenced_out and previous is not None:
                        closer = release(
                            previous,
                            detached=True,
                            closing=_state._closes.take(previous, _Rule.GRACED),
                        )
                        if closer is not None:
                            closers.append(closer)
        op.decided.set()
        for closer in closers:
            closer.join(_remaining(op.deadline))
    finally:
        op.decided.set()
        _state._swaps_running.discard(op)
        if token is not None:
            _state._hands_running.discard(token)
        _repin()


def _flush_live_sink() -> bool:
    """Drains whatever the delivering sink holds in its own client (SPEC-036 FR-002).

    Called **after** the queue drain, because the queue's events have to reach the client buffer
    before it is emptied. A sink with no ``flush`` of its own is unaffected, which is what keeps
    every pre-SPEC-036 sink satisfying the protocol.

    Which sinks are asked is the **owed-close record**, with no branch on which path armed them
    (SPEC-054 FR-005). It was a live worker's sink where one existed and the orphan record's
    otherwise, which is two readers of one question; the record names every sink something has
    delivered to and still owes a close, which is exactly the set that may be holding events in a
    client buffer. Not "a sink has been resolved" — ``configure()`` runs ``_ensure_sink()`` unconditionally, so a
    bare ``configure(service=...)`` has already built a ``StdoutSink`` that nothing was ever
    written to, and materialising a flush against it is the cost SPEC-031 FR-006 declined for the
    close path for the same reason. So a ``flush()`` in a process that has never logged touches
    no sink, which is what FR-001 AC-6 needs to stay true.

    **No owed sink is flushed before it holds its stop signal**, which is the orphan log's rule
    held on its twin (invariant 6). A signal debt is decided here as there
    (:func:`_owed_candidates`), and while another thread's hand token is open, each owed sink
    :func:`_will_hand` accepts is handed the signal here too, since that thread may be mid-hand
    on it — including a post-fork debt another thread cleared at its decision. The hands run
    after the section, with no library lock held, as the log's do; the section opens a token only
    when it published something, a cleared debt or a replaced ``_stop``.

    Args:
      None.

    Returns:
      Whether the sink's own flush succeeded. ``True`` also when there was no sink to ask, or
      when it holds nothing of its own.

    Raises:
      None. A failure is reported as a ``FlushResult`` reason by the caller, never raised: a
        flush is the call most likely to be made in a ``finally``.
    """
    from log_foundry.sinks.base import flush_sink

    token: object | None = None
    event: threading.Event | None = None
    try:
        with _state._lock:
            debt = _state._signal_owed
            hands = _owed_candidates()
            pending = list(_state._closes.owed.values())
            if _state._hands_running:
                hands += [
                    sink
                    for sink in pending
                    if not any(handed is sink for handed in hands) and _will_hand(sink)
                ]
            if hands:
                if debt or _state._stop.is_set():
                    token = _open_hand()
                event = _state.refresh_stop_signal()
            _state._signal_owed = False
        if event is not None:
            _hand_out(hands, event)
    finally:
        if token is not None:
            _state._hands_running.discard(token)
    if not pending:
        return True
    drained = True
    for sink in pending:
        try:
            flush_sink(sink)
        except Exception as exc:
            _diag.absorbed("flushing the sink's own buffer", exc, "its client still holds events")
            drained = False
    return drained


def _flush_worker(timeout: float | None = 5.0) -> FlushResult:
    """Drains the process worker without retiring it, backing ``flush()`` (SPEC-013 FR-003).

    ~~This deliberately does not call :func:`_get_worker`~~ — narrowed by SPEC-036 FR-001. The
    refusal still holds for an *empty* flush: a process that never logged has nothing to drain,
    and building a worker — with the thread and ``atexit`` registration that brings — in order to
    flush nothing would be pure cost. What changed is that :func:`_sweep_open_spans`, which runs
    first, does build one when it finds buffered events on an open span, because submitting them
    into a worker that does not exist delivers nothing and still reports success.

    Args:
      timeout: Seconds to wait for the drain, or ``None`` to wait indefinitely.

    Returns:
      A :class:`FlushResult`, truthy when everything outstanding was delivered and when no
      worker exists — a process that never logged has nothing to drain, so it has lost nothing.
      A sweep that could not hand its buffers over reports ``"abandoned"``, the existing token
      for "this call did not deliver them" (SPEC-036 FR-001): the events are still on their open
      spans and their close may yet carry them, but the caller asked *now*, and on the
      cold-start path this exists for there may be no close — reporting success there is the
      exact shape the spec was written to remove. The drain still runs, so whatever was
      submitted before the failure is not held back by it.

    The sink's own buffer is drained **whichever way the earlier steps went**, and the failure
    reasons are decided afterwards. A draft returned early on a failed sweep or a dead drain
    thread, which skipped it — and by then ``worker.flush`` had already pushed the queue *into*
    that buffer, so the events most worth saving before a freeze were the ones left there. The
    reason reported is the most upstream failure, because that is the one to fix.

    Raises:
      None. A flush is the call most likely to be made in a ``finally``, so the library must
        never be the reason a caller's function fails; a failure is reported by the return
        value instead (FR-003).
    """
    from log_foundry.decorator import _sweep_open_spans
    from log_foundry.results import FlushResult

    swept = True
    try:
        _sweep_open_spans()
    except Exception as exc:
        _diag.absorbed("sweeping open spans for a flush", exc, "buffered events were not swept")
        swept = False
    worker = _state.worker_exists()
    drained: FlushResult = FlushResult(ok=True)
    thread_died = False
    if worker is not None:
        try:
            drained = worker.flush(timeout)
        except Exception:
            thread_died = True
    sink_drained = _flush_live_sink()
    if not swept:
        return FlushResult(ok=False, reason="abandoned")
    if thread_died:
        return FlushResult(ok=False, reason="thread-died")
    if not sink_drained:
        return FlushResult(ok=False, reason="sink-flush")
    return drained


def _delivering_to_an_inherited_sink() -> bool:
    """Whether the sink this process delivers to was handed over by another one (SPEC-042).

    ``health().inherited_sink``. A forked child that inherits its parent's sink object refuses to
    close it, and this is what says so before the close would have happened: a `True` here in a
    process that did not `configure()` after forking is the deployment `architecture.md` §9 warns
    about, and the answer a reader needs is about the **one** sink being delivered to.

    **Answered from the config** (SPEC-054 FR-005), which closes ``architecture.md`` §12's open
    item. It read the owed record's last entry, and arming order does not make a sink the
    installed one: ``_swap_sink`` inserts the new sink and a preempted emit then appends the
    *superseded* one, so the order can be ``[live, superseded]`` and the field could name a sink
    this process had stopped delivering to. Neither end of the record is authoritative for
    "installed" — arming order is emit order, which is a different question — and §12 already
    names the config as the authority.

    It is the same authority :func:`_worker_health` uses for ``sink``, deliberately: both fields
    describe the destination, and answering them from two places is what let them disagree.

    With no sink resolved at all there is nothing installed and nothing inherited, so the answer
    is ``False`` rather than a guess.

    Args:
      None.

    Returns:
      Whether the configured sink carries another process's ownership record.

    Raises:
      None. ``health()`` is a diagnostic and must not be the reason a caller fails; an
        unanswerable question reports ``False``, the same direction as a process that never
        forked.
    """
    from log_foundry.config import _live_config

    try:
        sink = _live_config().sink
        return sink is not None and not releasable(sink)
    except Exception:
        return False


def _worker_health() -> Health:
    """Assembles the process's health snapshot once, from one record (SPEC-054 FR-005).

    Like :func:`_flush_worker` this deliberately does not call :func:`_get_worker`: starting a
    thread and registering an ``atexit`` drain in order to report an empty snapshot would be
    pure cost. That snapshot reads a ``stopped_reason`` of ``None`` — a worker that was never
    created has not died, which is why SPEC-019 reports the terminal failure as a reason
    rather than an ``alive`` flag.

    **One construction site, and every field has one authority.** The worker contributes its own
    counters and nothing else; ``retired`` is the owner's count; ``sink`` is ``read_losses`` over
    the **configured** sink; ``closing_sinks`` is the closer registry; ``inherited_sink`` is the
    config; and the two loss counters are ``decorator``'s. Two branches assembling eight fields
    apiece is what let ``sink`` be filled on one path only — measured against a ``MultiSink``
    with one raising child, ``health().sink`` read ``SinkLosses(dropped=0, failed=3)`` inside a
    span and ``None`` outside one while the sink's own ``losses()`` read ``failed=1``, which
    contradicts ``docs/invariants.md`` §2's observable that loss a sink absorbed is in
    ``health().sink``.

    ``sink`` reads the **configured** sink rather than ``worker.sink``, and the two differ in
    three states: briefly inside a swap, permanently after a declined swap (SPEC-035 FR-003, the
    worker retired mid-swap and keeps A while B is delivered to), and permanently after any
    ``configure(sink=…)`` on a retired worker (SPEC-033 FR-002). In the last two the worker path
    used to report A's losses while every event went to B. The config is what
    ``architecture.md`` §12 already names the authority for "installed", and B is the sink being
    delivered to, so that is an observable change taken deliberately.

    ``retired`` records an action the caller took, not a state of the worker, so it stays true in
    a process that called ``shutdown()`` without ever building one — where it was previously
    vacuous, and the whole snapshot read all-clear over a sink that had just been closed. It also
    survives a worker built *after* that shutdown: the count is the process's, and that worker's
    epoch merely tells it apart from a stranded one. The events such a worker carries are not
    lost silently — against a sink that guards its post-close state they raise and land in
    ``failed_batches`` (measured), and against one that releases nothing on ``close()`` they
    genuinely still deliver — so the detection there is ``failed_batches`` rather than SPEC-030's
    ``retired`` + ``submitted_after_shutdown`` pair, which stays the signal for the path it was
    built for.

    ``submitted_after_shutdown`` is deliberately **not** synthesized where no worker exists:
    SPEC-030 defines that count as submissions queued where nothing will drain them, and a later
    orphan log is delivered or refused at the sink and announced instead. The two are not the
    same claim.

    The ``retired`` binding is load-bearing rather than style: the roster's ``_boolean_positions``
    does not file a keyword argument, so writing it inline in the one ``Health`` construction
    would drop
    this module's only retirement guard out of ``tests/test_worker_predicate_roster.py`` and take
    its count from 45 to 44 with nothing red.

    Args:
      None.

    Returns:
      The process's health snapshot, backing :func:`log_foundry.health` (SPEC-017 FR-005).

    Raises:
      None.
    """
    from log_foundry.config import _live_config
    from log_foundry.decorator import _read_losses
    from log_foundry.sinks.base import read_losses
    from log_foundry.worker import Health

    worker = _state.worker_exists()
    counters = None if worker is None else worker.health()
    orphan_lost, in_span_lost = _read_losses()
    retired = _state.retirements > 0
    configured = _live_config().sink
    return Health(
        queued=0 if counters is None else counters.queued,
        dropped=0 if counters is None else counters.dropped,
        failed_batches=0 if counters is None else counters.failed_batches,
        stopped_reason=None if counters is None else counters.stopped_reason,
        sink=None if configured is None else read_losses(configured),
        retired=retired,
        submitted_after_shutdown=(0 if counters is None else counters.submitted_after_shutdown),
        incomplete_swaps=0 if counters is None else counters.incomplete_swaps,
        closing_sinks=_state._closes.graced_alive(),
        inherited_sink=_delivering_to_an_inherited_sink(),
        orphan_lost=orphan_lost,
        in_span_lost=in_span_lost,
    )


_fork.register_pin(_pin_inherited)
_fork.register_walk(_walk_after_fork)
_fork.register_child_handler(_reclaim_after_fork)
_fork.register_child_handler(_clear_after_fork)
_fork.register_child_handler(_adopt_factory_after_fork)
_fork.register_child_handler(_rebuild_worker_after_fork)
