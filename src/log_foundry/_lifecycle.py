"""Sink-lifecycle facilities shared by both delivery paths (SPEC-033 FR-005)."""

from __future__ import annotations

import atexit
import os
import threading
import time
import types
from dataclasses import replace
from itertools import islice
from time import monotonic
from typing import TYPE_CHECKING

from log_foundry import _diag, _fork
from log_foundry.sinks.base import Sink

if TYPE_CHECKING:
    from log_foundry.results import FlushResult
    from log_foundry.worker import Health, Worker

DEFAULT_SHUTDOWN_TIMEOUT = 30.0
"""Seconds :meth:`Worker.shutdown` will wait for the drain thread (SPEC-027 FR-004).

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

class _Lifecycle:
    """The process's delivery lifecycle: one owner, one lock, four questions (SPEC-040 FR-001).

    The state below lived as seven loose module globals in ``decorator.py``, and seven pieces of
    shipped work came out of that one fact — SPEC-030, SPEC-031 FR-006, SPEC-033, two SPEC-035
    regressions, SPEC-035 FR-002's roster, and SPEC-039's forked-child rebuild. Every one asked
    the same thing at a different site: *who owns the worker or the sink at this instant, and what
    may this path therefore do?*

    **The states.** A process is in exactly one of these, and the field that decides is named:

    - **cold** — ``_worker is None`` and ``_orphan_owed`` is empty. Nothing has been logged, no
      thread exists, no sink is owed a close. ``configure()`` alone does not leave this state:
      it runs ``_ensure_sink()`` unconditionally, so a resolved sink is not evidence anything
      was written to it (SPEC-031 FR-006).
    - **orphan-only** — ``_orphan_owed`` names every sink a level call with no span actually
      reached, and ``_worker`` is still ``None``. The close is owed to this path, and the
      ``atexit`` handler is armed by the emit that landed rather than by the sink existing.
    - **worker-backed** — ``_worker`` holds the process worker. It owns the drain, the sink's
      close, and the stop signal. A mixed process passes through orphan-only first, and
      :func:`_close_orphan_sink`'s ownership guard is what keeps that exactly one ``close()``.
    - **retired** — ``_orphan_retired`` is set, and ``_worker.retired`` with it where a worker
      exists. ``shutdown()`` is terminal by design (SPEC-013): nothing restarts, and a later
      log is accepted, reported, and refused at the closed sink rather than prevented
      (SPEC-030).

    **The transitions.** ``_get_worker`` takes cold or orphan-only to worker-backed, once, under
    the lock. ``_note_orphan_emit`` takes cold to orphan-only. ``_swap_sink`` retargets within
    whichever state holds, and re-points the record rather than clearing it, because clearing
    leaks the new sink in a process that swaps and exits. ``_shutdown_worker`` takes any state to
    retired and is idempotent on both paths. ``_rebuild_worker_after_fork`` re-enters the child
    in the state its parent held — a retired parent forks a retired child.

    **The four questions** (``architecture.md`` §9.2) are the methods below, one each, and a
    caller *selects* one rather than composing a predicate. Answering with the wrong one is this
    codebase's most repeated defect: three reviewers each named a different site, each was fixed,
    and a fourth shipped broken.

    **None of the four takes the lock**, and that is load-bearing rather than an omission.
    :func:`_get_worker`'s inner check, :func:`_close_orphan_sink`, :func:`_swap_sink` and
    :func:`_offer_orphan_signal`'s callers all hold ``_lock`` when they ask, so a non-reentrant
    acquire inside a question would deadlock them; and :func:`_get_worker`'s **outer** check is
    deliberately unlocked on the ``@trace`` hot path, where a lock would serialize every span
    flush in the process. Each read is a single reference load, which is atomic — a caller
    needing consistency across two reads takes ``_lock`` itself.

    Args:
      None.

    Returns:
      None.

    Raises:
      None.
    """

    _FORK_SKIP = ("_orphan_closed_sink",)
    """Keeps the superseded sink out of ``_fork``'s repair walk (SPEC-044 FR-005).

    The same hazard the module-level :data:`_FORK_SKIP` declares for ``_owned``, at a slot that
    declaration cannot reach. ``_fork._skipped_names`` reads the opt-out off **the holder of the
    attribute** — a plain ``getattr`` — so a module global is consulted only for the module's own
    namespace, while ``_orphan_closed_sink`` lives on this instance. Measured before the fix: a
    child of ``configure(A)`` → ``info()`` → ``configure(B)`` ran ``reacquire_after_fork()`` on
    both, and a ``FileSink`` in A's place would have its file re-opened on every fork forever.

    A **class** attribute deliberately. ``_fork._namespace_items`` reads ``vars(holder)``, the
    instance ``__dict__`` only, so a plain ``getattr`` finds this while the walk over
    :data:`_state` does not see it. The walk does reach it once, through the class itself, and
    harmlessly: it is a tuple of strings, which holds no primitive to replace and no sink to
    hook. The module-level tuple is unchanged and still needed; neither is the whole rule.

    Marking is not narrowed: :func:`_inheritance_roots` reads the slot directly, so the walk
    still reaches an inherited superseded sink and it is still refused (SPEC-042 FR-001) — on the
    stamp ``configure()`` left, since :func:`_mark_inherited` ``setdefault``s and leaves an
    existing record alone rather than writing ``_FOREIGN`` over it — or on ``_FOREIGN``, for a
    sink held inside it that the bounded stamp walk never reached.
    """

    def __init__(self) -> None:
        """Builds the process's one lifecycle owner, in the cold state.

        Every primitive is assigned to ``self`` rather than built at module scope, which is what
        lets ``_fork``'s repair walk find and replace it in a child (SPEC-039 FR-002): the walk
        writes back at a module global or an instance attribute, and an AST lint enforces that
        shape.

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
        self._orphan_owed: dict[int, Sink] = {}
        self._orphan_closed_sink: Sink | None = None
        self._orphan_stop = threading.Event()
        self._orphan_retired = False
        self._shutdown_running = 0
        self._late_worker: Worker | None = None

    def take_orphan_owed(self) -> list[Sink]:
        """Empties the owed-close record and returns what it held, in arming order.

        A **transition**, not one of the four questions (arch §9.2): every caller that clears the
        record is deciding who performs the closes it was holding, and reading-and-clearing in
        one step is what stops two of them deciding the same sink is theirs.

        Callers hold ``_lock``.

        Args:
          None.

        Returns:
          The sinks that were owed a close, oldest first.

        Raises:
          None.
        """
        owed = list(self._orphan_owed.values())
        self._orphan_owed.clear()
        return owed

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

        Reading ``retired`` in order to *report* it, as :func:`_worker_health` does, is this same
        question asked for a different purpose rather than a fifth category.

        A retired worker holds its sink forever — :meth:`Worker.swap_sink` returns early once
        shut down — so keying on a worker merely existing hands the swap to something that will
        do nothing with it, and the sink adopted afterwards is closed by no one: measured,
        ``configure(A)`` → ``@trace`` → ``shutdown()`` → ``configure(B)`` → ``info()`` →
        ``configure(C)`` left B unclosed with its event undelivered and every counter clean
        (SPEC-033 FR-002).

        :func:`_close_orphan_sink` deliberately does **not** use this: there a retired worker's
        ownership is exactly what must make it decline, since an expired shutdown leaves the
        drain thread possibly still inside that sink's ``emit``.

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
        return None if worker is None or worker.retired else worker

    def refresh_stop_signal(self) -> threading.Event:
        """Returns the orphan stop signal, replacing it first if it is already set.

        An ``Event`` is set once and never cleared, and ``sinks/_retry.wait`` returns
        immediately on a set one — so a sink handed the shutdown's event would have every
        later backoff collapsed to zero, which against a rate-limited destination is a tight
        retry loop. SPEC-027's contract is "cut short by a shutdown", not "never wait again".

        It is a method rather than a rebind at the call site because the replacement must be a
        ``self`` attribute assignment: ``_fork``'s repair walk writes back at a module global or
        an instance attribute, and ``tests/test_fork_lifecycle.py``'s shape lint enforces that
        the source says so (SPEC-039 FR-002).

        Callers hold ``_lock``.

        Args:
          None.

        Returns:
          A stop signal that is not set, for handing to a sink.

        Raises:
          None.
        """
        if self._orphan_stop.is_set():
            self._orphan_stop = threading.Event()
        return self._orphan_stop

    def worker_owns(self, sink: Sink) -> bool:
        """Ownership — who *owns* a close, which a retired worker still does (arch §9.2).

        The question three reviewers each named a different site for. It is not liveness:
        :meth:`Worker.swap_sink` returns early once ``_shutdown_done``, so a retired worker keeps
        its old sink forever, and answering "who closes this" with :meth:`live_worker` closes it
        a second time on a clean shutdown and closes it **under a live writer** on an expired one
        — both measured (SPEC-033 FR-002).

        Takes no lock; :func:`_close_orphan_sink` and :func:`_swap_sink` both ask it under
        ``_lock``, which is where the consistency they need comes from.

        Args:
          sink: The sink whose owner is in question.

        Returns:
          Whether the process worker holds that sink, retired or not.

        Raises:
          None.
        """
        worker = self._worker
        return worker is not None and worker.sink is sink

    def worker_owns_now(self, sink: Sink) -> bool:
        """Ownership ∧ moment — whose stop event the sink should be holding *now* (arch §9.2).

        A **conjunction** rather than a new subject, which is why it is named for both terms. It
        exists because neither half is right alone at :func:`_offer_orphan_signal`, the one site
        that asks it: bare ownership skips the offer for a worker whose shutdown has *finished*,
        leaving a live sink holding a set event that can never clear, while liveness alone
        un-skips for the whole drain and hands the drain thread a fresh event nobody will set —
        ``retired`` latches on **entry** to :meth:`Worker.shutdown`, not at its completion. Both
        were measured (SPEC-035 FR-001), and the identity term is what stops an orphan log to
        sink Y being skipped merely because a live worker is draining into sink X.

        Takes no lock. Its one caller is :func:`_offer_orphan_signal`, which does not acquire
        ``_lock`` itself — all three of *its* callers hold it, and that is the obligation a
        fourth caller would have to satisfy.

        Args:
          sink: The sink about to be offered a stop signal.

        Returns:
          Whether the process worker holds that sink *and* is still draining into it.

        Raises:
          None.
        """
        worker = self._worker
        return worker is not None and worker.sink is sink and worker.draining


_state = _Lifecycle()
"""The process's one lifecycle owner.

A module global rather than a lazily-built singleton, so ``_fork``'s repair walk reaches it in a
forked child without having to know it might not exist yet. It is deliberately **not** in
:data:`_FORK_SKIP`: unlike :data:`_owned`, which pins superseded sinks and must stay out of the
walk, everything here is live state a child needs repaired — the lock and the stop event above
all.
"""


_closers: list[threading.Thread] = []
_closers_lock = threading.Lock()

_closing_now: set[int] = set()
"""Ids of the sinks a :func:`release` is running against **right now** (SPEC-044 FR-003).

The *moment* term of an ownership-and-moment question (arch §9.2), applied to the close rather
than to the worker. :func:`_offer_orphan_signal` replaces a stop event that is already set, so a
sink is never left holding one that collapses every later backoff to zero — right, and pinned by
SPEC-033 FR-004 for a sink adopted **after** ``shutdown()``. What it must not do is cancel the
signal a close is *currently waiting on*: measured, an ``info()`` landing inside the close made
``shutdown()`` serve an 8 s backoff in full, against 0.00 s with no racing log, on both delivery
paths.

Retirement is the wrong discriminator — it would break SPEC-033 FR-004's two tests — and this is
the right one, because :func:`release` is the single path by which the library ever closes a sink
(SPEC-042 FR-002), so one registration there covers the orphan close, ``Worker._close_if_owed``,
the swap's detached closer and every wrapper.

It holds ``int`` ids, not sinks, so it pins nothing against garbage collection and needs no
:data:`_FORK_SKIP` entry; the registration brackets ``close()`` inside :func:`release`, so an id
cannot be reused while it is registered. **A fork breaks that bracket** — the child inherits a
registration whose ``finally`` no thread will ever run — so :func:`_clear_closing_after_fork`
empties it in the child. Measured before that handler existed: the id survived, and once the
child set its own ``_orphan_stop`` that sink was handed the **set** event and backed off not at
all, permanently.
"""
_closing_now_lock = threading.Lock()
"""Guards :data:`_closing_now`, and sits **last** in the lock order.

``_state._lock`` -> ``_config_lock`` -> ``_owned_lock`` is the order :data:`_owned_lock` states;
this one is taken under ``_state._lock`` (through :func:`_offer_orphan_signal`) and is never
nested with either of the other two, so there is no cycle. It is held only across a set
membership test or a single mutation, never across a ``close()``.
"""

_orphan_closing = 0
"""How many orphan closes are running right now, written under ``_state._lock`` (SPEC-050 FR-002).

The orphan path's half of the C3 residue :meth:`~log_foundry.worker.Worker._close_if_owed` fixes
for the worker: :func:`_close_orphan_sink` empties ``_orphan_owed`` under that lock and *then*
closes, so a second caller found nothing owed and returned instantly — and an ``atexit`` call that
returns while a background thread is still inside a close-is-delivery sink's ``close()`` exits
through it and kills it.

**A count and a gate, not one close's event.** A single slot was tried and is wrong, because the
orphan close is not once-only: ``_orphan_owed`` is repopulated by :func:`_note_orphan_emit` and
:func:`_adopt_declined_swap`, so a second close overwrote the first's event and its own completion
then cleared the slot — a bystander arriving after that read nothing, waited for nothing, and the
interpreter exited through the *first* close, still running. Measured against a one-second close:
the bystander waited 1.005 s alone and 0.000 s with a second close completing in between, losing
the first sink's whole buffer. It is the correction SPEC-045 made to the owed-close *record*,
arriving here for the same reason.

The predicate a bystander needs is "no orphan close is in flight", so that is what is published:
:data:`_orphan_idle` is cleared while the count is non-zero and set when it returns to zero. One
``Event``, built once at module scope, which is also what keeps it where ``_fork``'s repair walk
can replace it.

**A leaked count is permanent, so the increment is inside the ``try`` and the decrement is
clamped.** Two ways it leaked, both reproduced: a ``KeyboardInterrupt`` delivered at a bytecode
boundary between the increment and the ``try`` — measured leaking once in a few hundred iterations
under a real ``SIGINT`` storm — and a ``fork()`` from *inside* the inline close, where the child's
handler zeroes the count and the forking thread's own ``finally`` then takes it to ``-1``, which
``if not _orphan_closing`` never satisfies again. Either leaves the gate clear forever and every
later caller paying the whole grace, in a process that survives rather than exits. The count is
therefore taken and released under one ``try``, guarded by a flag set in the same critical section
as the increment, and floored at zero.

**The gate is process-wide, not per-sink**, so a worker-path ``shutdown()`` can pay the grace for
an orphan close it has nothing to do with — measured at 2.007 s wall, 0.000 s CPU, with the live
sink still drained and closed. That is the price of not exiting through a running close, and it is
bounded by the grace either way.
"""

_orphan_idle = threading.Event()
"""Set exactly when :data:`_orphan_closing` is zero — what a bystander waits on (SPEC-050 FR-002).

Starts set: a process with no orphan close in flight must not make the first bystander wait.
Not in :data:`_FORK_SKIP` — an ``Event`` is what ``_fork``'s repair walk exists to replace. What
the walk cannot know is that a child is idle whatever the parent was doing, since the threads that
would have finished those closes did not survive the fork; :func:`_clear_closing_after_fork` says
so.
"""
_orphan_idle.set()


def _closing(sink: Sink) -> bool:
    """Whether a release of this sink is in flight on some thread right now (FR-003).

    Args:
      sink: The sink about to be offered a stop signal.

    Returns:
      Whether :func:`release` is inside that sink's ``close()``.

    Raises:
      None.
    """
    with _closing_now_lock:
        return id(sink) in _closing_now


def _clear_closing_after_fork() -> None:
    """Drops the in-flight close registrations a child inherited (FR-003).

    The registration is removed by a ``finally`` in :func:`release`, on the thread performing the
    close — and a forked child has only the thread that called ``fork()``, so every inherited
    entry is one nothing will ever clear. Left in place it is not a missed refresh but a
    permanent one: once the child sets its own ``_orphan_stop``, that sink is handed the set
    event and every backoff collapses to zero, which is SPEC-033 FR-004's tight retry loop.

    Registered with ``_fork`` rather than reached for by it, the inversion SPEC-039 FR-006
    requires so that ``_fork`` imports nothing but ``_diag``. It takes the registry's own lock,
    which the repair walk re-initialised moments earlier.

    It runs **after** :func:`_mark_inherited` and before :func:`_rebuild_worker_after_fork`, and
    the placement is free rather than load-bearing: the registry holds ``int`` ids and no handler
    reads it.

    :data:`_orphan_closing` is zeroed here for the same reason and by the same argument
    (SPEC-050 FR-002). It counts closes running on threads that did not survive the fork, so a
    child inheriting a non-zero count would make its next bystander wait out the whole closer
    grace for closes that can never finish. The fork walk replaces the ``Event`` but not the count
    that keeps it clear, which is the distinction this handler exists for.

    Args:
      None.

    Returns:
      None.

    Raises:
      None.
    """
    global _orphan_closing
    with _closing_now_lock:
        _closing_now.clear()
    with _state._lock:
        _orphan_closing = 0
        _orphan_idle.set()


_FORK_SKIP = ("_owned",)
"""Keeps the ownership record out of ``_fork``'s repair walk (``_fork._SKIP_ATTRIBUTE``).

The record strongly references every sink this process ever acquired, so the walk would
otherwise reach ones the process abandoned several ``configure()`` calls ago and call their fork
hooks — measured, a child announced a buffer discard for a superseded sink, and a ``FileSink``
there would be reopened on every fork forever. A sink that is still live is reached through the
config and the worker, so nothing the repair needs is lost.

This covers only **this module's own namespace**. ``_orphan_closed_sink`` pins a superseded sink
for the same reason and is an attribute of :data:`_state`, which ``_fork._skipped_names`` asks
separately — so :attr:`_Lifecycle._FORK_SKIP` declares it there, and the two together are the
rule (SPEC-044 FR-005).
"""

_FOREIGN = -1
"""The pid a record carries when the sink belongs to some earlier process.

Never a real pid, so it can never match :func:`os.getpid`. Laid down by
:func:`_mark_inherited` in a forked child over each inherited sink its walk reaches that the
parent never recorded —
it ``setdefault``s, so a sink already carrying the parent's own pid keeps it and is refused by
that — which is what gives "this process did not acquire it" a **terminal** state.

Without it the record protects nothing where it is empty: ``stamp`` is write-once, and write-once
defends only a record that already exists, so a child could ``configure()`` its way into *owning*
a sink the parent never recorded and then close it entirely legitimately. Measured before this
existed — a child claimed a connection sink held behind a third-party wrapper and closed it,
destroying the parent's transport. Unrecorded has to be unclaimable, not merely unreleasable.
"""

_MARKING_CEILING = 100_000
"""Objects the child's marking walk may visit before it gives up and refuses the unrecorded.

**A cap is right here where SPEC-039 rejected one, and the difference is the fallback.** That
spec declined to bound its repair walk because an unfound lock is a child that hangs with no
safe degradation — a cap would trade a certain hazard for an uncertain one. This walk has
:data:`_marking_failed`, built for exactly "it did not finish, so trust nothing unrecorded", so
tripping the cap degrades to a leaked handle: the direction FR-001 requires.

The exposure is also larger than that walk's. ``_reinit_primitives`` enters containers only from
owned holders; this one enters every container reachable through arbitrary third-party objects,
and a ``list`` subclass with a non-terminating ``__iter__`` took a child to **5.7 GB RSS in nine
minutes**, unkillable by its parent because the parent was being starved. Exhausting memory in a
child that has not returned from ``fork`` is worse than the leak the cap causes.

Set far above any real graph — a sink's is on the order of hundreds — and above the ~24,000 the
module escape used to reach before :func:`_mark_inherited` stopped descending into modules.
"""

_marking_failed = False
"""Whether the child's marking walk could not finish, so nothing unrecorded may be trusted.

The walk is what makes an inherited sink recorded; if it did not complete, an unrecorded sink in
this child may be one it missed rather than one this process built. Refusing every unrecorded
sink then costs a leaked handle, which is the direction FR-001 requires a gap to fail in.
"""

_owned: dict[int, tuple[int, object]] = {}
"""Which sinks this process acquired, keyed by ``id`` and holding the pid that acquired them.

**No record means refused** (FR-001), and that default is the whole mechanism: a sink the library
was never handed was never its to release, so every gap fails toward a leaked handle rather than
toward closing a transport another process is still using.

The value holds a **strong reference beside the pid**, which is load-bearing twice. An ``id`` is
reusable the moment its object dies, so a bare pid could be handed to an unrelated later object
that happened to land on the address; and a garbage-collected sink closes itself, which is the
same destructive close by another route. ``_fork._fresh_primitive`` already pairs an id with a
keepalive for the first of those reasons.

It therefore grows by one entry per sink ever handed to the library and never shrinks. That is
accepted rather than bounded: ``configure()`` is a startup call, so the count is startup-scale,
and evicting an entry is exactly the "no record" state that makes a sink unreleasable.
"""

_owned_lock = threading.Lock()
"""Guards :data:`_owned`, and is the **last** lock in the process's order (FR-001 AC-12).

``_state._lock`` → ``_config_lock`` → this, never the reverse in any pair. The three-term form
is the real one: ``_get_worker`` calls ``config._ensure_sink()`` while holding
``_state._lock``, and ``_ensure_sink``'s construction branch takes ``_config_lock`` before it can
stamp. The criterion states the two-term version, which is true but skips the middle term.

The orphan logging path is the opposite constraint and takes **no** lock at all: it reaches
``_ensure_sink``'s fast-path return once per event, which must never stamp (AC-10).
"""

_PLAIN_TYPES = frozenset(
    {dict, list, tuple, set, frozenset, str, bytes, int, float, bool, type(None)}
)
"""Builtin types that provably cannot be a sink, skipped before any structural test.

A fact rather than a heuristic: ``Sink`` requires ``emit`` and ``close``, and no builtin has
either. Tested by **exact type, never ``isinstance``**, so a ``class MySink(dict)`` is still
asked — measured, the exact form answers ``True`` for a dict-subclass sink and ``False`` for a
plain dict.

It is the single largest term in the walk's cost, because a buffering sink's contents are almost
entirely these: skipping them took a ``MemorySink`` holding 100k events from 1,109 ms to 279 ms
before the descent was bounded at all (FR-001 AC-11).
"""


def _may_be_a_sink(value: object) -> bool:
    """Whether a value is worth asking the structural question about at all.

    Args:
      value: Any object reached by the walk.

    Returns:
      Whether its exact type is something other than a plain builtin.

    Raises:
      None.
    """
    return type(value) not in _PLAIN_TYPES


def _bounded_children(container: object) -> list[object]:
    """Reads at most :data:`_MARKING_CEILING` members of one container, without materialising it.

    **The per-object ceiling in the walk cannot save a walk stuck inside one call**, which is
    where ``_fork._container_children`` puts it: that helper does ``list(container)``, and a
    ``list`` subclass with a non-terminating ``__iter__`` never returns from it. Measured — a
    child reached 5.7 GB RSS in nine minutes and its parent's own timeout could not kill it,
    because the parent was being starved. So the bound has to be on the *read*, not only on the
    loop around it.

    Args:
      container: Any value ``_fork._is_container`` accepted.

    Returns:
      Its members, keys included for a mapping, truncated at the ceiling.

    Raises:
      None. A container that raises while being read contributes nothing, as it does in
        ``_fork``; what it holds is then unmarked. It keeps whatever ``stamp`` recorded, and
        where nothing did it is unrecorded — refused through a recorded wrapper, and §13 item
        7's residual otherwise, since absorbing here leaves :data:`_marking_failed` clear.
    """
    try:
        if isinstance(container, dict):
            return [
                *islice(container.keys(), _MARKING_CEILING),
                *islice(container.values(), _MARKING_CEILING),
            ]
        return list(islice(container, _MARKING_CEILING))  # type: ignore[call-overload]
    except Exception as exc:
        _diag.absorbed(
            "reading a container while marking a forked child's sinks",
            exc,
            "what it holds is not marked by this child",
        )
        return []


def _reachable_sinks(root: object) -> list[object]:
    """Returns every sink reachable from one object handed to the library (FR-001).

    A wrapper is handed over *with* its children, so the same act acquires them and the record
    has to reach the whole graph — a first draft stamped only what ``configure()`` was given, and
    a structural sink inside a ``MultiSink`` was then neither stamped nor reachable by
    ``_fork``'s mark, which is the object a forked child closed twice.

    **Reaching the graph is a descent question, not an ownership one**, and that distinction is
    what keeps this inside SPEC-039's boundary. The walk enters library objects and plain
    containers as that module's predicates already define, and *records* any sink-shaped member
    it meets even where it will not descend into it. Recording reads nothing from the object: an
    ``id``, a reference, and the two attribute lookups a runtime-checkable Protocol performs.
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

    **The order is a trade, not a free win.** A value satisfying *both* tests is now pushed as a
    holder, and this walk's holder loop has no container branch, so an owned non-sink container
    subclass and a container-subclass sink's own members both lose reach. Neither is a
    destructive close — ``_mark_inherited`` descends unboundedly and compensates in the child —
    and no shipped sink holds either shape; the residual is an own-process leak, which is the
    direction FR-001 permits, taken in exchange for closing a live silent one.

    Args:
      root: The object ``configure()`` or ``_ensure_sink()`` was handed.

    Returns:
      The sinks reached, each once, in the order the walk met them.

    Raises:
      None. The reads are the ones ``_fork`` already guards, and a graph that cannot be walked
        fully leaves the unreached sinks unrecorded — refused, which is the safe default.
    """
    seen: set[int] = set()
    found: list[object] = []
    stack: list[object] = [root]
    while stack:
        holder = stack.pop()
        if id(holder) in seen or not _may_be_a_sink(holder):
            continue
        seen.add(id(holder))
        if isinstance(holder, Sink):
            found.append(holder)
        if not _fork._is_owned(holder):
            continue
        for _name, value in _fork._namespace_items(holder):
            if _is_candidate(value):
                stack.append(value)
            elif _fork._is_container(value):
                stack.extend(
                    member for member in _bounded_children(value) if _is_candidate(member)
                )
    return found


def _is_candidate(value: object) -> bool:
    """Whether the walk pushes this value onto its stack.

    Args:
      value: A member or attribute the walk has just read.

    Returns:
      Whether it is either sink-shaped or an object this package defines.

    Raises:
      None.
    """
    return _may_be_a_sink(value) and (isinstance(value, Sink) or _fork._is_owned(value))


def _inheritance_roots() -> list[object]:
    """Returns everything a forked child may have inherited a sink through.

    The live delivery targets and every sink already recorded, which together are the only
    handles the library itself holds at fork time.

    **A sink reachable from none of them is not thereby safe**, and saying so was worse than
    saying nothing. The residual is real and measured: a parent that builds a connection sink in
    application state and never hands it to the library — `sink = SocketSink(...)` at import in
    a gunicorn master — leaves nothing for this walk to find, and a child whose ``post_fork``
    calls ``configure(sink=that_object)`` is the *first* process to hand it over, so it acquires
    it legitimately and closes the parent's transport at shutdown. That cannot be decided here:
    FR-001's rule is that the library may release what it was handed, FR-001 AC-3 requires a
    child's ``configure()``d sink to be releasable, and nothing distinguishes the two without
    marking the whole heap. It is recorded as a constraint in ``architecture.md`` §13 rather
    than asserted away, and ``README.md``'s "build a connection-holding sink in the worker
    process" is exactly the deployment advice that avoids it.

    ``_owned.values()`` is the load-bearing entry, not the four live handles. It is the only one
    that reaches a sink held inside a **superseded** wrapper — one ``configure()`` replaced, so
    it is no live target, while the transport beneath it is still the parent's. Dropping it is a
    destructive close; dropping any of the other four changes nothing, since each is itself
    stamped and therefore already in the record.

    Args:
      None.

    Returns:
      The objects :func:`_mark_inherited` starts its walk from.

    Raises:
      None. A root that cannot be read is skipped; a partial roster marks less and therefore
        refuses more, which is the safe direction.
    """
    from log_foundry import config

    worker = _state.worker_exists()
    candidates = (
        config._live_config().sink,
        None if worker is None else worker.sink,
        *_state._orphan_owed.values(),
        _state._orphan_closed_sink,
    )
    roots: list[object] = [found for found in candidates if found is not None]
    with _owned_lock:
        roots.extend([reference for _pid, reference in _owned.values()])
    return roots


def _mark_inherited() -> None:
    """Records the sinks this child inherited, before any other fork handler runs (FR-001).

    Registered with ``_fork`` and run in the child, which is why it may take the library's locks:
    they were re-initialised moments earlier. It must run **before** any handler that could reach
    a release path, which registration order provides and a test pins.

    **This walk descends further than :func:`_reachable_sinks` deliberately, including into
    third-party objects.** That is the whole point: a connection sink held inside a wrapper the
    library does not own is invisible to the bounded stamp walk, so the parent never recorded it
    — and a child that re-wraps that same object in a ``MultiSink`` of its own then reaches it,
    claims it, and closes the parent's transport. Reproduced. The read is
    ``_fork._namespace_items``, the same one the fork repair uses: instance ``__dict__`` and slot
    descriptors only, so no property is triggered, nothing is mutated, and nothing is called.
    SPEC-039 FR-003 AC-2 forbids mutating and traversing foreign state to *repair* it; noticing
    what a wrapper holds so as to leave it alone is the opposite obligation, and FR-001 already
    draws that line for recording.

    The cost is one-off per fork and of the same order as the repair walk beside it: measured in
    the child at 0.0 ms idle, 0.3 ms for a 50-deep ``MultiSink`` and **117 ms** for a
    ``MemorySink`` holding 100k events, against SPEC-039's 202 ms for the repair walk. Fork cost
    roughly doubles rather than changing character. The pre-filter gates the structural test and
    the descent, not the push, so this is not :func:`_reachable_sinks`' number and the two must
    not be quoted for each other.

    Args:
      None.

    Returns:
      None.

    Raises:
      None. An escaping exception sets :data:`_marking_failed`, after which every unrecorded
        sink in this child is refused rather than trusted — a leaked handle instead of a
        destructive close. **That covers less than it appears to**: every read the walk makes is
        already absorbed one level down in ``_fork``, which returns empty and announces, so the
        common failure is a *partial* walk that raises nothing and leaves the flag clear. Sinks
        it did not reach are then unrecorded rather than marked. **The reclaim below runs
        either way**: a sink that returned from its hook provably holds its own transport, and a
        walk that failed is no reason to refuse it forever — which is the outcome
        :func:`reclaim`'s own docstring says a ``setdefault`` would wrongly cause. The ceiling
        path already reached it; the exception path did not, and the two differed silently.
        The outer guard is for a fault
        in this function's own frame — resolving the roots, or an object whose ``__class__``
        property raises, which makes ``isinstance`` raise here. Not a hostile *metaclass*: a
        value's ``__instancecheck__`` is never consulted, since ``Sink``'s own ``_ProtocolMeta``
        runs. Tripping :data:`_MARKING_CEILING` sets the flag too, by the same route and for the
        same reason. The partial-walk residual is recorded in §13.
    """
    global _marking_failed
    try:
        seen: set[int] = set()
        found: list[object] = []
        stack: list[object] = _inheritance_roots()
        while stack:
            if len(seen) >= _MARKING_CEILING:
                _marking_failed = True
                _diag.lost(
                    "object",
                    len(stack),
                    f"the walk marking a forked child's inherited sinks passed "
                    f"{_MARKING_CEILING} objects and stopped; this child will refuse to close "
                    f"any sink it has no record of",
                )
                break
            holder = stack.pop()
            if id(holder) in seen or isinstance(holder, types.ModuleType):
                continue
            seen.add(id(holder))
            if _may_be_a_sink(holder) and isinstance(holder, Sink):
                found.append(holder)
            if _fork._is_container(holder):
                stack.extend(_bounded_children(holder))
                continue
            if not _may_be_a_sink(holder):
                continue
            for _name, value in _fork._namespace_items(holder):
                if _fork._is_container(value) or _may_be_a_sink(value):
                    stack.append(value)
        with _owned_lock:
            for inherited in found:
                _owned.setdefault(id(inherited), (_FOREIGN, inherited))
    except Exception as exc:
        _marking_failed = True
        _diag.absorbed(
            "marking the sinks a forked child inherited",
            exc,
            "this child will refuse to close any sink it has no record of",
        )
    for reacquired in _fork.reacquired_in_child:
        reclaim(reacquired)


def reclaim(sink: object) -> None:
    """Records that a sink re-acquired its transport in this process (SPEC-042 FR-005).

    The one write that **overrides** an existing record, and it has to be: an inherited sink
    **the marking walk reached** carries another process's pid by the time the hook roster is
    read — the parent's own stamp, or the ``_FOREIGN`` :func:`_mark_inherited` ``setdefault``s
    where the parent recorded nothing — so a ``setdefault`` here would leave a sink that provably
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

    Args:
      sink: The sink whose hook returned normally.

    Returns:
      None.

    Raises:
      None.
    """
    pid = os.getpid()
    with _owned_lock:
        _owned[id(sink)] = (pid, sink)


def stamp(sink: object) -> None:
    """Records that this process acquired a sink, and everything reachable from it (FR-001).

    Called at the one moment ownership is knowable — when the library is *handed* a sink, by
    ``configure(sink=…)`` or by ``_ensure_sink()`` building the lazy default — and never on
    ``_ensure_sink``'s fast-path return, which runs once per orphan event (AC-10).

    **Write-once per object.** A stamp naming another process is never overwritten, so a forked
    child cannot claim an inherited sink by configuring its way back to it, and the answer
    survives a second fork. Overwriting on every ``configure()`` satisfies every other criterion
    in FR-001 and fails AC-4.

    The walk runs **outside** the lock and only the record write takes it, so an arbitrary
    object graph is never traversed while holding a process-wide lock.

    Args:
      sink: The sink being installed, whose reachable graph is acquired with it.

    Returns:
      None.

    Raises:
      None. This runs inside ``configure()``, which must not fail an application's startup over
        a bookkeeping step; an unrecorded sink is a refused one, which leaks rather than closes.
    """
    try:
        reachable = _reachable_sinks(sink)
    except Exception as exc:
        _diag.absorbed("recording which sinks this process owns", exc, "they will not be closed")
        return
    pid = os.getpid()
    with _owned_lock:
        for found in reachable:
            _owned.setdefault(id(found), (pid, found))


def releasable(sink: object, *, owner: object = None) -> bool:
    """Whether this process may close a sink (FR-001).

    A recorded sink answers for itself: releasable exactly when the record names this process.
    That is the whole mechanism for the defect — after a fork every stamp names the parent, so a
    child refuses the object it inherited.

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
    - The child recorded elsewhere — the inherited sink, or one :func:`_mark_inherited` marked
      ``_FOREIGN``. Refused however it was reached, which is what closes the wrapper route
      (FR-002 AC-3).
    - The wrapper recorded, the child not — the sink added to a wrapper *after* ``configure()``
      walked it. Refused, per FR-001 AC-6: the library holds this graph, so a member it has no
      record of is one it must not assume is this process's. The consequence is a leak, recorded
      in §13.

    **"Unrecorded is the caller's" is only sound because a fork makes it false first.** In a
    child, :func:`_mark_inherited` records every inherited sink **its walk reaches** that the
    parent did not record as ``_FOREIGN``, *before* any other handler runs — one the parent *did*
    record keeps that stamp and is refused on it. What the walk reached is therefore stamped or
    ``_FOREIGN``; what it missed stays unrecorded and is the §13 item 7 residual, not a case this
    closes — measured, a sink built before the fork behind a container that raises is unrecorded
    in the child, releasable, and closed. If the walk could not finish *at all*,
    :data:`_marking_failed` withdraws the assumption entirely and every unrecorded sink is
    refused.

    Identity is re-checked against the strong reference rather than trusting the ``id``. The
    reference is what makes an id collision impossible while a record stands, so this can only
    fail if that invariant breaks — and answering ``False`` there is the safe direction.

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
    with _owned_lock:
        record = _owned.get(id(sink))
        owner_record = None if owner is None else _owned.get(id(owner))
    if record is not None:
        return record[1] is sink and record[0] == pid
    return owner_record is None and not _marking_failed


def release(sink: Sink, *, detached: bool = False, owner: object = None) -> threading.Thread | None:
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

    Args:
      sink: The sink to close.
      detached: Whether to close on a daemon thread rather than inline. Detached is for a sink
        the caller has stopped delivering to and must not block on (SPEC-030 FR-003).
      owner: The wrapper forwarding this close, when one is. The five shipped wrappers pass
        themselves; the three lifecycle sites hold their sink directly and pass nothing. See
        :func:`releasable` for what the distinction decides.

    Returns:
      The started closer thread for a detached release, or ``None`` — for a refused release, for
      a completed inline close, and when the platform would not give the process another thread.
      A caller needing to tell those apart consults :func:`releasable`, which it already has.

    Raises:
      Exception: Whatever an inline ``close()`` raised. A refused release raises nothing, and
        neither does a detached one: its thread body absorbs, since there is no caller left to
        hand it to.
    """
    if not releasable(sink, owner=owner):
        return None
    if detached:
        return _start_closer(sink)
    with _closing_now_lock:
        _closing_now.add(id(sink))
    try:
        sink.close()
    finally:
        with _closing_now_lock:
            _closing_now.discard(id(sink))
    return None


def _start_closer(sink: Sink) -> threading.Thread | None:
    """Starts a daemon close of a sink no longer being delivered to (SPEC-030 FR-003).

    The thread is returned rather than joined, so a caller holding a lock can start under it and
    wait after releasing it — ``_swap_sink`` mutates its records under the process-wide
    ``_state._lock`` and must not hold that across a wait of the swap's whole budget (SPEC-033
    FR-002). Callers that hold no lock join it immediately and are equivalent to the single call
    this replaced.

    The thread is a **daemon**, and it is :func:`join_closers` that makes that safe rather than
    merely available. A non-daemon thread was tried and is worse on its own: CPython joins
    non-daemon threads *before* running ``atexit``, so one hung close stops the exit drain from
    ever running and loses everything buffered in the **live** sink. A daemon alone is worse in
    the opposite case: a close that is slow but *succeeding* is killed at exit, losing whatever
    it was flushing.

    Args:
      sink: The sink that was swapped out.

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
        args=(sink,),
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
    with _closers_lock:
        _closers[:] = [old for old in _closers if old.is_alive()]
        _closers.append(closer)
    return closer


def _close_guarded(sink: Sink) -> None:
    """Closes a swapped-out sink on its own thread, absorbing a failure.

    The guard is what makes the thread safe to leave unattended: an exception escaping here
    would reach CPython's thread bootstrap, which prints a full traceback carrying the
    exception's message — the user data arch §6 keeps out of anything the library says about
    itself. It goes back through :func:`release` rather than calling ``close()`` itself, so the
    thread body is one of the eight callers rather than a ninth close (SPEC-042 FR-002).

    Args:
      sink: The sink to close.

    Returns:
      None.

    Raises:
      None.
    """
    try:
        release(sink)
    except Exception as exc:
        _diag.absorbed("closing a swapped-out sink", exc, "it may still hold its resources")


def join_closers(timeout: float | None) -> None:
    """Gives outstanding swapped-out closes their last chance before the process exits.

    **The cap is the mechanism.** The wait is the smaller of :data:`DEFAULT_CLOSER_GRACE` and
    what remains of the shutdown's own budget: capped so a stuck close cannot hold a process at
    exit for the whole shutdown budget, and carved from that budget so it cannot extend it either.

    The registry is process-global rather than per-worker because a close started before any
    worker existed must still be counted and still be granted this grace (SPEC-033 FR-005).

    Args:
      timeout: Seconds remaining in the shutdown's budget, further capped by
        :data:`DEFAULT_CLOSER_GRACE` and shared across every outstanding close. ``None`` takes
        the cap rather than waiting indefinitely — an unbounded shutdown is a caller's choice
        about draining events, not a licence for a stuck close to hold the exit.

    Returns:
      None.

    Raises:
      None. A join on a thread that has already finished is a no-op, and one that has not is
        abandoned at the deadline — which is the daemon's contract, not a failure.
    """
    with _closers_lock:
        closers = [closer for closer in _closers if closer.is_alive()]
        _closers[:] = closers
    grace = DEFAULT_CLOSER_GRACE if timeout is None else min(timeout, DEFAULT_CLOSER_GRACE)
    deadline = time.monotonic() + grace
    for closer in closers:
        closer.join(max(0.0, deadline - time.monotonic()))


def closing_count() -> int:
    """Counts the swapped-out closes running at this instant, backing ``Health.closing_sinks``.

    A live fact rather than an inference from a timeout: an expired join reports nothing, since
    a slow close and a stuck one cannot be told apart at that moment, so this gauge is what an
    operator reads instead. It falls as well as rises.

    Args:
      None.

    Returns:
      The number of closer threads still alive.

    Raises:
      None.
    """
    with _closers_lock:
        _closers[:] = [closer for closer in _closers if closer.is_alive()]
        return len(_closers)


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
    asking :meth:`_Lifecycle.worker_exists` rather than reading a module attribute. Against an
    ~18.5 µs traced call that is 0.06%, and end-to-end timing is identical either side.

    **The orphan record is not discarded without deciding who closes it** (SPEC-044 FR-002).
    ``configure()`` writes ``_config.sink`` *before* it takes this lock, so a ``configure(sink=B)``
    blocked here leaves ``_ensure_sink()`` returning B while the record still names A — which has
    events and has never been closed. Clearing unconditionally lost A's close entirely, with
    ``incomplete_swaps`` at zero because every field of ``Health`` describes a worker and the
    worker was fine (natural rate 6/400). Where the new worker did not adopt the recorded sink,
    this transition owns that close: latch it and release it detached, the shape
    :func:`_swap_sink`'s no-worker branch already uses. The closer is **not** joined — this branch
    runs at most once per process, ``join_closers`` grants it the exit grace and
    ``health().closing_sinks`` reports it live, and joining would put a sink's whole close on the
    first traced call in the process.

    **A worker built while a ``shutdown()`` is running is registered for it** (SPEC-044 FR-001).
    ``_shutdown_worker`` raises a depth counter and reads the worker in one critical section, so a
    worker built after that read is registered here and drained by that same call rather than
    left running with nothing to stop it. ``sink_released`` covers the ordering where the orphan
    branch already closed this sink: the worker inherits a discharged close instead of performing
    a second one. In the other ordering the worker is registered first and
    :func:`_close_orphan_sink`'s ownership guard declines, so the worker performs the only close.
    Both are gated on the counter, which is what keeps this off the **sequential**
    ``shutdown()`` → ``@trace`` path, where a fresh worker still delivers by the decision
    :func:`_worker_health` records.

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

        with _state._lock:
            worker = _state.worker_exists()
            if worker is None:
                _register_exit_handler()
                sink = _ensure_sink()
                worker = Worker(
                    sink,
                    sink_released=_state._shutdown_running > 0
                    and _state._orphan_closed_sink is sink,
                )
                _state._worker = worker
                if _state._shutdown_running > 0:
                    _state._late_worker = worker
                owed = _state.take_orphan_owed()
                for stale in owed:
                    if stale is not worker.sink:
                        _state._orphan_closed_sink = stale
                        release(stale, detached=True)
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
    if (
        id(sink) in _state._orphan_owed or sink is _state._orphan_closed_sink
    ) and not _state._orphan_stop.is_set():
        return
    with _state._lock:
        _offer_orphan_signal(sink)
        if id(sink) in _state._orphan_owed or sink is _state._orphan_closed_sink:
            return
        _register_exit_handler()
        _state._orphan_owed[id(sink)] = sink
def _rebuild_worker_after_fork() -> None:
    """Gives a forked child a drain thread of its own, or a retired worker (SPEC-039 FR-002).

    Registered with ``_fork`` at import rather than reached for by it, which is what keeps that
    module free of an import of this one (FR-006). It runs on the child's only thread, after the
    locks are its own again, so it may take them.

    The predicate is deliberately **not** ``_state.live_worker()``, even though the two agree today.
    What is being asked is whether to start a thread, and the answer must survive a later
    reading of ``retired`` — so it is hoisted into ``resume`` and passed, where the roster files
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
    resume = not worker.retired
    worker._reinit_after_fork(resume=resume)
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
    the sink left open — the wait was never cut, not merely slow. But bare :meth:`worker_owns` is wrong in the opposite direction: it skips for a worker whose shutdown has
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

    **Except while a release of this sink is in flight** (SPEC-044 FR-003), where the replacement
    cancels the signal the close is waiting on: measured, an ``info()`` landing inside the close
    made ``shutdown()`` serve an 8 s backoff in full, against 0.00 s with no racing log, on both
    delivery paths. The discriminator is the **moment**, not retirement — retirement would
    un-refresh a sink adopted after ``shutdown()``, which SPEC-033 FR-004 measured and pinned.
    Returning rather than offering the set event is the direct expression of "the signal it holds
    is not replaced", and depends on nothing about which event ``_orphan_stop`` currently is.

    Callers hold ``_state._lock``.

    Args:
      sink: The sink to offer a signal to.

    Returns:
      None.

    Raises:
      None.
    """
    if _state.worker_owns_now(sink):
        return
    if _closing(sink):
        return
    offer_stop_signal(sink, _state.refresh_stop_signal())
def _close_owed(sink: Sink) -> None:
    """Closes one sink the orphan path owed, absorbing whatever the close raised (SPEC-046).

    Factored out so the inline close and the threaded ones are provably the same call, and
    because a thread body must not raise: an exception escaping one reaches CPython's bootstrap,
    which prints a traceback carrying the message arch §6 keeps out of anything the library says
    about itself. It is the same guard :func:`_close_guarded` applies for a swapped-out sink, with
    the ``_diag`` text of the site it actually is (SPEC-029).

    It records nothing in the closed-sink latch. :func:`_close_orphan_sink` **empties the owed
    record** under ``_state._lock`` before any close starts, which is what stops two callers
    performing the same close. The closed-sink latch is a single slot and holds only the last of
    them — unchanged by SPEC-046, and why a racing emit can still re-arm one of the others is
    recorded in ``architecture.md`` §13.

    Args:
      sink: The sink to close.

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
        release(sink)
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


def _inline_close_choice(owed: list[Sink]) -> Sink:
    """Picks the owed sink whose close stays on the calling thread (SPEC-046 FR-001).

    The **configured** sink where it is among those owed, and otherwise the most recently armed.
    The config is the authority for which sink is being delivered to, and keeping that one inline
    is what preserves SPEC-030's decision that ``shutdown()``'s own close stays inline. The
    fallback is what keeps the single-owed-sink case free of a thread it does not need.

    Membership is by **identity**, never ``in``. ``list.__contains__`` is ``x is e or x == e``, so
    a sink with a value ``__eq__`` — a dataclass, say, which ``Sink`` permits and no shipped sink
    happens to be — would match an object the record never armed: the inline close would then run
    against a sink that was never latched, and every genuinely owed sink would go to a thread.

    This deliberately does not copy :func:`_delivering_to_an_inherited_sink`, which takes the
    record's last entry and whose own docstring says neither end of the record is authoritative
    for "installed".

    Args:
      owed: The sinks owed a close, in arming order and never empty.

    Returns:
      The one to close on the calling thread.

    Raises:
      None.
    """
    configured = _live_config_sink()
    for sink in owed:
        if sink is configured:
            return sink
    return owed[-1]


def _bystander_grace(deadline: float | None) -> float:
    """Returns how long a caller may wait on a close another caller is performing (SPEC-050 FR-002).

    ``join_closers``'s arithmetic, and :func:`~log_foundry.worker._closer_grace` is its twin on the
    worker path: capped at :data:`DEFAULT_CLOSER_GRACE` because this is an exit waiting on a close
    it does not own, and carved from the caller's own budget so a ``shutdown(timeout=0)`` does not
    inherit somebody else's. The flat cap this replaced made the two paths disagree — the worker
    half returned in under half a second on a ``timeout=0`` call while this one took the whole two
    seconds.

    Args:
      deadline: The calling ``shutdown``'s monotonic deadline, or ``None`` for an unbounded caller,
        which takes the cap rather than waiting indefinitely.

    Returns:
      Seconds to wait, never negative and never above the cap.

    Raises:
      None.
    """
    if deadline is None:
        return DEFAULT_CLOSER_GRACE
    return max(0.0, min(DEFAULT_CLOSER_GRACE, deadline - monotonic()))


def discharge_owed(sink: Sink) -> None:
    """Records that a sink's owed close is being performed elsewhere (SPEC-050 FR-004).

    The orphan record and :attr:`~log_foundry.worker.Worker._unclosed_swaps` can name the **same**
    sink: an unconfirmed swap strands it in the worker's record, and an orphan emit that resolved
    it before the swap and resumed after the re-arm slot had moved on puts it back in
    ``_orphan_owed``. Both then close it — measured ``A.closes == 2`` with a preemption point at
    ``_ensure_sink``, which is SPEC-044 FR-004's shape at a record that did not exist then.

    Called with ``_state._lock`` held, by the worker taking its own record under it, so the take
    and the discharge are one critical section: split, a concurrent ``_close_orphan_sink`` can read
    the sink out of ``_orphan_owed`` in the gap and close it alongside.

    ``_orphan_closed_sink`` is latched as well as the entry removed, for the reason
    :func:`_get_worker` latches it: removal stops *this* close being performed twice, and the latch
    stops a later orphan emit re-arming a sink whose close is already under way.

    Args:
      sink: The sink whose close the caller is about to perform.

    Returns:
      None.

    Raises:
      None.
    """
    _state._orphan_owed.pop(id(sink), None)
    _state._orphan_closed_sink = sink


def _close_orphan_sink(deadline: float | None = None) -> None:
    """Closes a sink only the orphan path ever wrote to, once (SPEC-031 FR-006).

    A process that never opens a span builds no worker, so nothing owned the sink's close and
    nothing performed it: on a locally-buffering sink every event died in the client's batch,
    on a synchronous one the flush and the resource were lost, and ``health()`` read all-clear
    because every field it carries describes a worker that does not exist.

    **A caller that finds nothing owed waits for the one that is closing** (SPEC-050 FR-002).
    The record is emptied under ``_state._lock`` before any close begins, so a second caller took
    the early return below while the first was still inside an unbounded ``close()`` — and where
    that first caller is a background thread and the second is ``atexit``, the interpreter exits
    through a running close and kills it. For a sink whose ``close()`` *is* the delivery that is
    total loss of its buffer.

    **A separate caller cannot wait on itself, and the guard for that is the `if owed:` on the
    write.** (A sink whose own ``close()`` calls ``shutdown()`` re-enters on the closing thread and
    does wait out its grace — bounded, no deadlock, and pathological usage rather than a case this
    guards.)
    :data:`_orphan_closing` is installed only where ``owed`` is non-empty, so a caller that takes
    the work never reaches the read below and a bystander never installs anything. Capturing
    ``waiting`` before the write is *not* what makes this safe — both happen under
    ``_state._lock``, so reading the global there and reading the captured value are the same
    read, and mutating one into the other is an equivalent mutant. The conditional is the live
    guard: made unconditional, a caller with nothing owed leaves a permanently unset event behind
    it, and every call after the next one pays the whole grace on an event nothing will set —
    measured at the *third* successive orphan ``shutdown()``, which is why a test doing two of
    them cannot see it. The local exists so a reader can tell which of the two a caller is
    without re-deriving it.

    The wait is :func:`_bystander_grace` — capped at :data:`DEFAULT_CLOSER_GRACE` and carved from
    the caller's own deadline, the same arithmetic the worker path uses, so a
    ``shutdown(timeout=0)`` does not inherit another caller's budget on one path and not the other.
    It took a flat cap first, which made the two paths disagree by two seconds on that call.

    A worker that owns *this* sink closes it instead, and this returns — that is what makes a
    mixed process exactly one ``close()`` in either order. It also inherits that worker's reasons
    for *not* closing: an expired :meth:`Worker.shutdown` leaves the sink open because the drain
    thread may still be inside ``emit``, and there ``_state._worker.sink is owed`` still holds.

    The guard is **ownership**, not a worker merely existing (SPEC-033 FR-002). The two stop
    being the same question the moment the worker is retired: ``Worker.swap_sink`` returns early
    once ``_shutdown_done``, so a retired worker keeps its old sink forever while every orphan
    event goes to a newly configured one — measured, a sink configured after ``shutdown()`` was
    then closed by nothing at all, losing a locally-buffering sink's whole batch while
    ``health()`` read ``retired=True, submitted_after_shutdown=0, failed_batches=0``.

    That check is read **under** ``_state._lock``, not ahead of it, because :func:`_get_worker`
    assigns ``_state._worker`` while holding that same lock. Unlocked, a ``shutdown()`` racing a first
    ``@trace`` could read ``None``, block behind the worker's construction, and then close the
    sink underneath the worker that had just captured it — reproduced with an injected
    preemption point, the way SPEC-028 demonstrates the races that need one.

    The once-only flag is set ahead of the close, as ``Worker.shutdown``'s is: a second
    ``close()`` on a sink that partially released its resources is worse than an unclosed one.

    **The owed closes run concurrently and every one is joined** (SPEC-046). Draining the record
    in sequence made this cost one slow close *times* the number owed — measured against
    ``shutdown(timeout=1.0)`` with 2-second closes, one owed sink 2.00 s and four 8.02 s — which
    is a multiplication SPEC-045 introduced when it made the record a set. One sink closes on the
    calling thread (:func:`_inline_close_choice` picks which, and why) and the rest get a thread
    each, so the cost is the slowest rather than the sum.

    **Joined, deliberately not detached.** Routing them through :func:`_start_closer` and
    :func:`join_closers` is the obvious reuse and it loses data, in two independent ways. The
    grace is what remains of the shutdown's budget, which a slow inline close can exhaust
    entirely — measured completing **1 of 4** against a 1.0 s budget. And it caps at
    :data:`DEFAULT_CLOSER_GRACE` regardless, so even inside a generous budget a sink whose
    ``close()`` outlasts two seconds is abandoned and killed at interpreter exit — measured, a
    3-second close delivered nothing where it delivers today. It also recharges the double grace SPEC-044 measured, and runs into
    the §13 entry recording that a daemon close of *this* sink was built and reverted because
    exit can kill it inside ``SQLiteSink.commit()``. Joining every close avoids all three, and is
    strictly better than the sequential drain on both axes: the cost falls and the loss stays
    zero. The threads are daemons only so that one which somehow outlives the join cannot keep the
    interpreter alive; the join, not the flag, is what guarantees each close completes.

    **The join is in a ``finally``**, so a ``BaseException`` — the ``KeyboardInterrupt`` SPEC-025
    requires to reach the caller — waits for the started closes before it propagates. Without it,
    a Ctrl-C during ``shutdown()`` returned with every fan-out close abandoned *mid-write*, where
    the sequential drain abandoned one and had merely not started the rest: measured 4 killed
    mid-write against 1, which trades a leaked resource for a corrupt one and is the wrong side
    of the ordering §13 records for exactly this hazard. The interrupt still reaches the caller;
    it is delayed by the closes already in flight, which is the same wait the inline close has
    always imposed.

    A thread that will not start is closed **inline** instead, which is the opposite of
    :func:`_start_closer`'s refusal and deliberately so: that helper is spending a caller's
    bounded budget, and falling back to an inline close there would reintroduce the unbounded
    wait it exists to remove. This path has no budget left to protect — it is the exit — so the
    choice is between closing inline and never closing at all.

    Args:
      deadline: The calling ``shutdown``'s monotonic deadline, or ``None``. It bounds only
        the wait for another caller's close, never a close performed here.

    Returns:
      None.

    Raises:
      None. This runs from ``atexit``, where an escaping exception makes CPython print a
        traceback carrying the message arch §6 keeps out of anything the library says about
        itself. ``Exception``, never ``BaseException`` (SPEC-025 FR-004).
    """
    global _orphan_closing
    took = False
    started: list[threading.Thread] = []
    try:
        with _state._lock:
            owed: list[Sink] = [
                sink for sink in _state._orphan_owed.values() if not _state.worker_owns(sink)
            ]
            for sink in owed:
                del _state._orphan_owed[id(sink)]
                _state._orphan_closed_sink = sink
            if owed:
                _orphan_closing += 1
                _orphan_idle.clear()
                took = True
        if not owed:
            _orphan_idle.wait(_bystander_grace(deadline))
            return
        inline = _inline_close_choice(owed)
        for sink in owed:
            if sink is inline:
                continue
            closer = threading.Thread(
                target=_close_owed, args=(sink,), name="log-foundry-owed-close", daemon=True
            )
            try:
                closer.start()
            except Exception as exc:
                _diag.absorbed(
                    "starting the thread that closes an owed sink",
                    exc,
                    "it is closed inline instead",
                )
                _close_owed(sink)
            else:
                started.append(closer)
        _close_owed(inline)
    finally:
        for closer in started:
            closer.join()
        if took:
            with _state._lock:
                _orphan_closing = max(0, _orphan_closing - 1)
                if not _orphan_closing:
                    _orphan_idle.set()
def _shutdown_worker(timeout: float | None = DEFAULT_SHUTDOWN_TIMEOUT) -> None:
    """Drains and closes the process worker, or closes an orphan-only sink, backing ``shutdown()``.

    The ``atexit`` registration binds this function, so the exit path gets the bounded form
    and its default (SPEC-027 FR-004) — an unbounded join in an ``atexit`` handler is a
    process that will not exit. Idempotent on both paths.

    ``_state._orphan_retired`` is set unconditionally and read only when there is no worker, which is
    what makes ``health().retired`` truthful for a process that shut down without ever
    building one (SPEC-031 FR-006). No worker is created here to answer it: standing up a
    thread at exit to prove there is nothing to drain is pure cost, the same refusal
    :func:`_swap_sink` and :func:`_flush_worker` already make.

    The worker branch runs :func:`_close_orphan_sink` before returning (SPEC-033 FR-002). A
    retired worker owns nothing further, so a sink adopted after its shutdown is the orphan
    path's to close; that function's ownership guard is what keeps this from double-closing the
    sink the worker just closed itself.

    ``_state._orphan_stop`` is set **before** delegating, so a sink parked in a backoff is released
    while :meth:`Worker.shutdown` is still draining rather than after it has given up waiting.

    **The retirement latch and the worker read are one critical section** (SPEC-044 FR-001).
    Unlocked, a worker built between them sent this down the no-worker branch: the drain thread
    was never stopped and its sink never closed, while ``health()`` reported ``retired=True`` and
    later logs were delivered by a live worker with ``submitted_after_shutdown`` at zero.
    ``atexit`` recovered it in a process that exits; a frozen serverless container never does,
    which is the deployment this call exists for. Closing the read window alone only narrows it,
    so ``_shutdown_running`` stays raised for the whole of the no-worker branch and
    :func:`_get_worker` registers what it builds under the same lock — read and lowered in the
    last critical section, so there is no gap between the two.

    It is a **depth counter, not a flag**. Two concurrent ``shutdown()`` calls are documented as
    normal (``Worker._close_if_owed``), and with a boolean the first to finish lowers it while the
    second is still running — measured, a worker built at that instant was registered nowhere and
    the second call returned having stopped nothing, which is the original defect verbatim.

    The late worker's drain is charged against **this call's** deadline, and :func:`join_closers`
    is not also called on that path: :meth:`Worker.shutdown` grants the closer grace itself, and
    granting it twice measured 4.01 s against a 2 s grace — the same double charge this function's
    branches were already arranged to avoid.

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
    deadline = None if timeout is None else monotonic() + timeout
    with _state._lock:
        _state._orphan_retired = True
        _state._orphan_stop.set()
        worker = _state.worker_exists()
        if worker is None:
            _state._shutdown_running += 1
    if worker is not None:
        worker.shutdown(timeout)
        _close_orphan_sink(deadline)
        return
    try:
        _close_orphan_sink(deadline)
    finally:
        with _state._lock:
            late_worker = _state._late_worker
            _state._late_worker = None
            _state._shutdown_running -= 1
    if late_worker is not None:
        late_worker.shutdown(None if deadline is None else max(0.0, deadline - monotonic()))
        return
    join_closers(None if deadline is None else max(0.0, deadline - monotonic()))
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

    ``_state._worker`` is read **under** ``_state._lock``. Unlocked it was harmless, because the
    no-worker branch did nothing; once that branch closes a sink it is the race
    :func:`_close_orphan_sink` was built against — a first ``@trace`` on another thread can be
    inside ``Worker(...)`` with its sink already resolved while this thread reads ``None`` and
    closes the sink that worker is about to deliver to. The closer's **join is outside** the
    lock: it waits up to the swap's whole budget, and holding the process-wide lock across that
    would park every concurrent emit behind it.

    ``incomplete_swaps`` is deliberately not touched on the orphan path. It records a *drain*
    that could not be confirmed (SPEC-030), and there is no drain here; an expired close join
    reports nothing at all, by the decision that made the bounded close available.

    **The worker branch latches what it hands over, and decides the close before clearing**
    (SPEC-044 FR-004, FR-002). It used to clear the orphan record and record nothing: an
    orphan emit that resolved the old sink before the swap and resumed after it then re-armed a
    sink :meth:`Worker.swap_sink` had already closed, and the exit close performed a second
    ``close()`` on it — measured ``A.closed == 2`` with a preemption point injected at
    ``_ensure_sink``. ``sinks/base.py`` asks an implementation to make its release idempotent,
    but the library does not *rely* on that — it cannot enforce what a third-party sink does — so
    it performs one close (SPEC-032).

    **A sink this branch closes is dropped from the worker's owed-swap record** (SPEC-050
    FR-004). That record is what makes :meth:`~log_foundry.worker.Worker._close_if_owed`'s close
    of a stranded sink once-only, and this is the one route by which a sink in it can acquire a
    different closer: the re-arm guard next to this line is a single slot, so a second
    unconfirmed swap overwrites the first sink's protection and an orphan emit could put it back
    in the owed record. Defensive rather than reproduced — no reachable sequence for it was found
    — and one call, taken under the worker's own lock beneath this one, the same nesting
    :meth:`~log_foundry.worker.Worker.swap_sink` already performs.

    The latch is keyed on ``worker.sink``, **not** on the orphan record. The sink
    ``Worker.swap_sink`` is about to close is the one the worker holds, and in the reproduced case
    the record is ``None`` — the worker cleared it when it was built — so keying on the record
    latched nothing and left the defect exactly where it was. It is set even where the drain
    cannot be confirmed and the swap therefore leaves that sink **open**: the reason there is not
    "it was closed" but that a racing orphan emit must not re-arm a sink whose drain thread may
    still be inside ``emit``.

    Where the record names a **third** sink — neither the worker's nor the new one, reachable
    only by a preempted emit re-arming across an earlier swap — nothing else would close it, so
    this branch owns it and releases it detached, exactly as the no-worker branch does. That
    closer is not joined: the worker branch returns through :meth:`Worker.swap_sink`'s own
    deadline, ``join_closers`` grants the exit grace, and ``health().closing_sinks`` reports it
    live. The slot is single, so the worker's sink is latched **last** and wins: it is the case
    that reproduces without a compound race. The bound it had — the single slot's own — is gone:
    SPEC-045 replaced that slot with a record of every owed sink, so ``architecture.md`` §13 no
    longer states one, and the reasoning is in that spec's delivery doc.

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
    closers: list[threading.Thread] = []
    deadline = None if timeout is None else monotonic() + timeout
    with _state._lock:
        worker = _state.live_worker()
        if worker is not None:
            for stale in _state.take_orphan_owed():
                if stale is not new_sink and stale is not worker.sink:
                    _state._orphan_closed_sink = stale
                    with worker._lock:
                        worker._discard_owed_swap(stale)
                    closer = release(stale, detached=True)
                    if closer is not None:
                        closers.append(closer)
            if worker.sink is not new_sink:
                _state._orphan_closed_sink = worker.sink
        else:
            if not _state._orphan_owed:
                return
            superseded = [
                sink for sink in _state.take_orphan_owed() if sink is not new_sink
            ]
            _state._orphan_owed[id(new_sink)] = new_sink
            _offer_orphan_signal(new_sink)
            for stale in superseded:
                if not _state.worker_owns(stale):
                    _state._orphan_closed_sink = stale
                    closer = release(stale, detached=True)
                    if closer is not None:
                        closers.append(closer)
    if worker is not None:
        try:
            worker_holds_sink = worker.swap_sink(new_sink, timeout)
        except Exception as exc:
            _diag.absorbed(
                "swapping the log sink", exc, "events may still be delivered to the previous sink"
            )
        else:
            if not worker_holds_sink:
                _adopt_declined_swap(new_sink)
    for closer in closers:
        closer.join(None if deadline is None else max(0.0, deadline - monotonic()))
def _adopt_declined_swap(new_sink: Sink) -> None:
    """Takes ownership of a sink a worker refused mid-swap (SPEC-035 FR-003).

    ``Worker.swap_sink`` re-checks retirement after its first ``flush()`` and returns early once
    ``_shutdown_done`` latched, but :func:`_swap_sink` had already taken the orphan record on the
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
    ``_state._orphan_owed`` while this thread is inside ``swap_sink``'s first drain, followed by a
    ``shutdown()`` that closes it, lets this re-arm a closed sink for a second ``close()`` at
    exit — reproduced. ``sinks/base.py`` asks an implementation to make its release idempotent,
    but the library does not rely on it, for the reason :func:`_swap_sink` states.

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
    with _state._lock:
        _offer_orphan_signal(new_sink)
        if id(new_sink) in _state._orphan_owed or new_sink is _state._orphan_closed_sink:
            return
        _register_exit_handler()
        _state._orphan_owed[id(new_sink)] = new_sink
def _flush_live_sink() -> bool:
    """Drains whatever the delivering sink holds in its own client (SPEC-036 FR-002).

    Called **after** the queue drain, because the queue's events have to reach the client buffer
    before it is emptied. A sink with no ``flush`` of its own is unaffected, which is what keeps
    every pre-SPEC-036 sink satisfying the protocol.

    Which sink is asked follows the ownership rule the rest of this module uses (SPEC-033): a
    live worker's sink if there is one, otherwise the sink an orphan emit actually **reached**.
    Not "a sink has been resolved" — ``configure()`` runs ``_ensure_sink()`` unconditionally, so a
    bare ``configure(service=...)`` has already built a ``StdoutSink`` that nothing was ever
    written to, and materialising a flush against it is the cost SPEC-031 FR-006 declined for the
    close path for the same reason. So a ``flush()`` in a process that has never logged touches
    no sink, which is what FR-001 AC-6 needs to stay true.

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

    worker = _state.live_worker()
    if worker is not None:
        pending = [worker.sink]
    else:
        with _state._lock:
            pending = list(_state._orphan_owed.values())
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
    """Whether the sink this process last installed for delivery is one it may not release.

    Answerable with **no worker**, which is what makes it truthful in a process that only ever
    logs outside a span — the same refusal :func:`_worker_health` already makes for ``retired``,
    and for the same reason: standing up a thread to answer ``health()`` is forbidden.

    The three candidates are asked in delivery order: the worker's sink if a worker exists,
    else the sink an orphan emit reached most recently, else the configured one. SPEC-033's
    measured disagreement is worker-versus-config, which the **first** term already covers.

    The middle term takes the **last** entry of ``_state._orphan_owed``, and that is continuity
    with the single slot it replaced rather than a claim that the last entry is the sink being
    delivered to. It is not: ``_swap_sink`` inserts the new sink into a freshly emptied record and
    a preempted emit then appends the *superseded* one, so in this spec's own primary scenario the
    order is ``[live, superseded]``. Neither end of the record is authoritative for "installed" —
    arming order is emit order, which is a different question — and the config is. The answer is
    therefore unchanged from before SPEC-045 and can still name a superseded sink; that limit is
    an open item in ``architecture.md`` §12 rather than quietly fixed here, because correcting it
    changes a documented ``Health`` field on a path that spec did not otherwise touch.

    With no sink resolved at all there is nothing installed and nothing inherited, so the answer
    is ``False`` rather than a guess.

    Args:
      None.

    Returns:
      Whether that one sink carries another process's ownership record.

    Raises:
      None. ``health()`` is a diagnostic and must not be the reason a caller fails; an
        unanswerable question reports ``False``, the same direction as a process that never
        forked.
    """
    from log_foundry.config import _live_config

    try:
        worker = _state.worker_exists()
        owed = next(reversed(_state._orphan_owed.values()), None)
        sink = worker.sink if worker is not None else (owed or _live_config().sink)
        return sink is not None and not releasable(sink)
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

    The two loss counters are synthesized on **both** branches, for the reason ``retired`` is on
    one: they describe the caller's own path, not the worker's, and ``Worker`` cannot report them
    because it does not know they exist — ``worker.py`` imports nothing from this module, and the
    reverse read would be a cycle. A process that only ever logged outside a span has no worker
    and is exactly the process whose loss they exist to show (SPEC-036 FR-003 AC-7).

    The synthesis also survives a worker built *after* that shutdown, which is why it is an
    ``or`` rather than a fallback. An orphan-only ``shutdown()`` leaves ``_state._worker`` unset, so a
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
    from log_foundry.decorator import _read_losses
    from log_foundry.worker import Health

    worker = _state.worker_exists()
    orphan_lost, in_span_lost = _read_losses()
    if worker is None:
        return Health(
            queued=0,
            dropped=0,
            failed_batches=0,
            retired=_state._orphan_retired,
            closing_sinks=closing_count(),
            inherited_sink=_delivering_to_an_inherited_sink(),
            orphan_lost=orphan_lost,
            in_span_lost=in_span_lost,
        )
    health = worker.health()
    retired = _state._orphan_retired or health.retired
    return replace(
        health, retired=retired, orphan_lost=orphan_lost, in_span_lost=in_span_lost
    )


_fork.register_child_handler(_mark_inherited)
_fork.register_child_handler(_clear_closing_after_fork)
_fork.register_child_handler(_rebuild_worker_after_fork)
