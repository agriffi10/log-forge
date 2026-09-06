"""Sink-lifecycle facilities shared by both delivery paths (SPEC-033 FR-005)."""

from __future__ import annotations

import atexit
import os
import threading
import types
from itertools import islice
from time import monotonic
from typing import TYPE_CHECKING

from log_foundry import _diag, _fork
from log_foundry.sinks.base import Sink

if TYPE_CHECKING:
    from log_foundry.results import FlushResult
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

class _Lifecycle:
    """The process's delivery lifecycle: one owner, one lock, four questions (SPEC-040 FR-001).

    The state below lived as seven loose module globals in ``decorator.py``, and seven pieces of
    shipped work came out of that one fact — SPEC-030, SPEC-031 FR-006, SPEC-033, two SPEC-035
    regressions, SPEC-035 FR-002's roster, and SPEC-039's forked-child rebuild. Every one asked
    the same thing at a different site: *who owns the worker or the sink at this instant, and what
    may this path therefore do?*

    **The states.** A process is in exactly one of these, and the field that decides is named:

    - **cold** — ``_worker is None`` and ``_owed`` is empty. Nothing has been logged, no
      thread exists, no sink is owed a close. ``configure()`` alone does not leave this state:
      it runs ``_ensure_sink()`` unconditionally, so a resolved sink is not evidence anything
      was written to it (SPEC-031 FR-006).
    - **orphan-only** — ``_owed`` names every sink a level call with no span actually
      reached, and ``_worker`` is still ``None``. The close is owed to this path, and the
      ``atexit`` handler is armed by the emit that landed rather than by the sink existing.
    - **worker-backed** — ``_worker`` holds the process worker. It owns the drain; the sink's
      close and the stop signal are this owner's on both paths (SPEC-054). A mixed process passes
      through orphan-only first, and one record is what keeps that exactly one ``close()``.
    - **retired** — ``retirements`` is above zero, and every worker built before the last
      ``shutdown()`` reads as no longer live with it. ``shutdown()`` is terminal by design (SPEC-013): nothing restarts, and a later
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

    **None of the three takes the lock**, and that is load-bearing rather than an omission.
    :func:`_get_worker`'s inner check, :func:`_close_owed`, :func:`_swap_sink` and
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

    _FORK_SKIP = ("_owed",)
    """Keeps superseded sinks out of ``_fork``'s repair walk (SPEC-044 FR-005, SPEC-054 FR-002).

    The same hazard the module-level :data:`_FORK_SKIP` declares for ``_owned``, at a record that
    declaration cannot reach. ``_fork._skipped_names`` reads the opt-out off **the holder of the
    attribute** — a plain ``getattr`` — so a module global is consulted only for the module's own
    namespace, while ``_owed`` lives on this instance. Measured before the fix at the slot this
    replaced: a child of ``configure(A)`` → ``info()`` → ``configure(B)`` ran
    ``reacquire_after_fork()`` on both, and a ``FileSink`` in A's place would have its file
    re-opened on every fork forever.

    The record pins every sink still owed a close, superseded ones included, which is why it
    needs the opt-out where the single slot it replaced did. A **live** target is still reached
    by the walk through the config and through ``worker.sink``, so nothing the child must repair
    is hidden by this.

    A **class** attribute deliberately. ``_fork._namespace_items`` reads ``vars(holder)``, the
    instance ``__dict__`` only, so a plain ``getattr`` finds this while the walk over
    :data:`_state` does not see it. The walk does reach it once, through the class itself, and
    harmlessly: it is a tuple of strings, which holds no primitive to replace and no sink to
    hook. The module-level tuple is unchanged and still needed; neither is the whole rule.

    Marking is not narrowed: :func:`_inheritance_roots` reads the record directly, so the walk
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
        self._owed: dict[int, Sink] = {}
        self._stop = threading.Event()
        self.retirements = 0
        self._shutdown_running = 0
        self._late_worker: Worker | None = None

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

        A worker is live while its ``_epoch`` — :attr:`retirements` as it stood at that worker's
        build — is still current (SPEC-054 FR-001). A **count** rather than a latch, because a
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
        return None if worker is None or worker._epoch != self.retirements else worker

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
        the replacement can only ever reach a later deliverer.

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
        :func:`_offer_orphan_signal`'s callers hold it. It does take the leaf
        ``_closing_now_lock`` through :func:`_closing`, which is the order the shipped code
        already establishes — ``_offer_orphan_signal`` reaches that lock under ``_lock`` today —
        and it is a leaf, so nothing acquires ``_lock`` beneath it.

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
        return _closing(sink)

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
        return _closing(sink)


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

class _Closing:
    """One close in flight, holding the event a bystander waits on (SPEC-054 FR-003).

    A wrapper rather than a bare ``threading.Event`` in :data:`_closing_now`, and the reason is
    the fork walk. ``_fork._reinit_primitives`` replaces a primitive by **assigning it back on a
    holder by name**, so an ``Event`` sitting as a *value* in a module-level dict is reached by
    the traversal and replaced by nothing — there is no name to assign. Held as an attribute of
    an object this package owns, it is found by ``_fork._namespace_items`` and written back like
    any other, which is the shape ``tests/test_fork_lifecycle.py``'s primitive lint enforces
    (SPEC-039 FR-002).

    That the child empties the whole registry moments later (:func:`_clear_after_fork`) is not
    the argument: a primitive built where the walk cannot repair it is refused by shape here,
    deliberately, so a later use of one does not have to re-derive whether it happens to be safe.

    Attributes:
      event: Set when the close this registration brackets has ended, however it ended.
    """

    def __init__(self) -> None:
        """Builds an unset event for one close.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.event = threading.Event()


_closing_now: dict[int, _Closing] = {}
"""Sinks a close is running against **right now**, by id, each with the event it will set.

The *moment* term of arch §9.2's questions, applied to the close rather than to the worker, and
one of the two things :meth:`_Lifecycle.held` consults. It answers two callers at once
(SPEC-044 FR-003, SPEC-050 FR-002, SPEC-054 FR-003):

- :func:`_offer_orphan_signal` replaces a stop event that is already set, so a sink is never left
  holding one that collapses every later backoff to zero — right, and pinned by SPEC-033 FR-004
  for a sink adopted **after** ``shutdown()``. What it must not do is cancel the signal a close is
  *currently waiting on*: measured, an ``info()`` landing inside the close made ``shutdown()``
  serve an 8 s backoff in full, against 0.00 s with no racing log, on both delivery paths.
- A second ``shutdown()`` must **wait for** a close already running rather than returning through
  it, so each entry carries an ``Event`` the performer sets on its way out. A process-wide count
  and one idle gate stood here before SPEC-054 and cost a worker-path ``shutdown()`` the whole
  grace for an orphan close it had nothing to do with — measured at 2.007 s wall, 0.000 s CPU.
  Per sink, a caller waits only on closes that are actually in flight.

**A registration is made by the thread that decides the close, in the same critical section that
takes the sink out of** :attr:`_Lifecycle._owed` (FR-003). Registering inside :func:`release`
instead leaves a gap for a detached close — the daemon thread registers after the caller has
dropped ``_lock`` — and with the closed-sink latch retired that gap is a sink neither owed nor in
flight, which a preempted orphan emit re-arms and a racing ``shutdown()`` then closes alongside.
Every exit discharges it, including a :func:`releasable` that refuses and a closer thread that
never starts. An entry nothing discharges is permanent, and it is not ``health().closing_sinks``
that suffers — that counts live closer *threads*, through :func:`closing_count` — but the two
questions this registry answers: the sink is skipped by the signal refresh forever, the closer
never takes it because :meth:`_Lifecycle.held` stays true, and every later bystander waits out
the whole grace on an event nobody will set.

It is keyed by ``int`` id and the value is an ``Event``, so it pins no sink against garbage
collection and needs no :data:`_FORK_SKIP` entry; the registration brackets ``close()``, so an id
cannot be reused while it is registered. **A fork breaks that bracket** — the child inherits a
registration whose ``finally`` no thread will ever run — so :func:`_clear_after_fork` empties it
in the child. Measured before that handler existed: the id survived, and once the child set its
own ``_stop`` that sink was handed the **set** event and backed off not at all, permanently.
"""
_closing_now_lock = threading.Lock()
"""Guards :data:`_closing_now`, and sits **last** in the lock order.

``_state._lock`` -> ``worker._lock`` -> this one is the order the closer takes, and
``_state._lock`` -> ``_config_lock`` -> ``_owned_lock`` is the order :data:`_owned_lock` states.
This lock is never nested with either of the last two, so there is no cycle. It is held only
across a dict lookup or a single mutation, never across a ``close()``.
"""


def _closing(sink: Sink) -> bool:
    """Whether a release of this sink is in flight on some thread right now (FR-003).

    Args:
      sink: The sink whose close is in question.

    Returns:
      Whether a close of that sink is registered.

    Raises:
      None.
    """
    with _closing_now_lock:
        return id(sink) in _closing_now


def _register_closing(sink: Sink) -> _Closing:
    """Registers a close of *sink* as in flight and returns the event a bystander waits on.

    Called by the thread that **decides** the close, in the critical section that takes the sink
    out of the owed record, so there is no instant at which the sink is neither owed nor in
    flight (FR-003). A close already registered keeps its event rather than gaining a second: two
    deciders cannot reach here for one sink, because the take is under ``_state._lock``, but a
    detached close re-enters :func:`release` on its own thread and must find the event its
    requester registered.

    Args:
      sink: The sink whose close is about to start.

    Returns:
      The registration, whose event is set when that close finishes.

    Raises:
      None.
    """
    with _closing_now_lock:
        existing = _closing_now.get(id(sink))
        if existing is not None:
            return existing
        registration = _Closing()
        _closing_now[id(sink)] = registration
        return registration


def _finish_closing(sink: Sink, closing: _Closing | None) -> None:
    """Discharges a close registration and releases whoever was waiting on it.

    The entry is removed only when it is still the one this caller was handed, so a sink re-armed
    and re-closed inside the window cannot have the wrong close discharged for it. Setting the
    event outside the lock is deliberate: a waiter woken by it goes on to take ``_state._lock``,
    and holding the leaf lock across that would invert the order the module's docstrings state.

    Args:
      sink: The sink whose close has ended, however it ended.
      closing: The registration :func:`_register_closing` returned, or ``None`` for a caller
        that never made one.

    Returns:
      None.

    Raises:
      None.
    """
    if closing is None:
        return
    with _closing_now_lock:
        if _closing_now.get(id(sink)) is closing:
            del _closing_now[id(sink)]
    closing.event.set()


def _clear_after_fork() -> None:
    """Drops the in-flight close registrations a child inherited (FR-003, FR-006).

    The registration is discharged by a ``finally`` on the thread performing the close — and a
    forked child has only the thread that called ``fork()``, so every inherited entry is one
    nothing will ever clear. Left in place it is not a missed refresh but a permanent one: once
    the child sets its own ``_stop``, that sink is handed the set event and every backoff
    collapses to zero, which is SPEC-033 FR-004's tight retry loop. A bystander in the child
    would also wait out the grace for a close that can never finish.

    Registered with ``_fork`` rather than reached for by it, the inversion SPEC-039 FR-006
    requires so that ``_fork`` imports nothing but ``_diag``. It takes the registry's own lock,
    which the repair walk re-initialised moments earlier.

    It runs **after** :func:`_mark_inherited` and before :func:`_rebuild_worker_after_fork`, and
    the placement is free rather than load-bearing: the registry holds ``int`` ids and no handler
    reads it.

    This is the whole of the residue now (SPEC-054 FR-006). The process-wide orphan-close count
    and its idle gate were reset here too and are gone: with a per-sink registration there is no
    second thing that can be left inconsistent, and the events themselves are replaced by the
    repair walk before any handler runs.

    Args:
      None.

    Returns:
      None.

    Raises:
      None.
    """
    with _closing_now_lock:
        _closing_now.clear()


_FORK_SKIP = ("_owned",)
"""Keeps the ownership record out of ``_fork``'s repair walk (``_fork._SKIP_ATTRIBUTE``).

The record strongly references every sink this process ever acquired, so the walk would
otherwise reach ones the process abandoned several ``configure()`` calls ago and call their fork
hooks — measured, a child announced a buffer discard for a superseded sink, and a ``FileSink``
there would be reopened on every fork forever. A sink that is still live is reached through the
config and the worker, so nothing the repair needs is lost.

This covers only **this module's own namespace**. ``_owed`` pins superseded sinks
for the same reason and is an attribute of :data:`_state`, which ``_fork._skipped_names`` asks
separately — so :attr:`_Lifecycle._FORK_SKIP` declares it there, and the two together are the
rule (SPEC-044 FR-005, SPEC-054 FR-002).
"""

_FOREIGN = -1
"""The pid a record carries when the sink belongs to some earlier process.

Never a real pid, so it can never match :func:`os.getpid`, which is what gives "this process did
not acquire it" a **terminal** state. A forked child lays it over each inherited sink its walk
reaches that the parent never recorded; **when** that happens is :func:`_mark_inherited`'s to
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
        *_state._owed.values(),
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
    That is the whole mechanism for the defect — in a child, an inherited sink's record does not
    name this process, so the child refuses the object it inherited. What it names instead
    differs: the parent's own stamp where ``configure()`` left one, and ``_FOREIGN`` where
    :func:`_mark_inherited` wrote it, which is no real pid. The one sanctioned exception is
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


def release(
    sink: Sink,
    *,
    detached: bool = False,
    owner: object = None,
    closing: _Closing | None = None,
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

    Args:
      sink: The sink to close.
      detached: Whether to close on a daemon thread rather than inline. Detached is for a sink
        the caller has stopped delivering to and must not block on (SPEC-030 FR-003).
      owner: The wrapper forwarding this close, when one is. The five shipped wrappers pass
        themselves; the lifecycle sites hold their sink directly and pass nothing. See
        :func:`releasable` for what the distinction decides.
      closing: The in-flight registration this close is discharging, from
        :func:`_register_closing`, for a caller that registered before deciding who performs the
        close (FR-003). It is discharged on **every** exit — a refused release and a closer
        thread that never started included, since either would otherwise leave an event nobody
        sets. ``None`` for a caller that did not register, in which case one is made and
        bracketed here as it always was.

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
        _finish_closing(sink, closing)
        return None
    if detached:
        closer = _start_closer(sink, closing)
        if closer is None:
            _finish_closing(sink, closing)
        return closer
    registration = closing if closing is not None else _register_closing(sink)
    try:
        sink.close()
    finally:
        _finish_closing(sink, registration)
    return None


def _start_closer(sink: Sink, closing: _Closing | None = None) -> threading.Thread | None:
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
    with _closers_lock:
        _closers[:] = [old for old in _closers if old.is_alive()]
        _closers.append(closer)
    return closer


def _close_guarded(sink: Sink, closing: _Closing | None = None) -> None:
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
    ``_lifecycle._bystander_grace`` and an inline one inside :func:`join_closers` — which is one
    arithmetic asked in three places about the same budget.

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


def join_closers(deadline: float | None) -> None:
    """Gives outstanding detached closes their last chance before the process exits.

    The registry is process-global rather than per-worker because a close started before any
    worker existed must still be counted and still be granted this grace (SPEC-033 FR-005).
    Granted **once**, at the end of :func:`_shutdown_worker`, on every path (FR-003) — two
    functions arranged that by hand before.

    Args:
      deadline: The ``time.monotonic()`` instant the shutdown's budget expires, or ``None``.
        :func:`closer_grace` caps it, and the result is shared across every outstanding close.

    Returns:
      None.

    Raises:
      None. A join on a thread that has already finished is a no-op, and one that has not is
        abandoned at the deadline — which is the daemon's contract, not a failure.
    """
    with _closers_lock:
        closers = [closer for closer in _closers if closer.is_alive()]
        _closers[:] = closers
    end = monotonic() + closer_grace(deadline)
    for closer in closers:
        closer.join(max(0.0, end - monotonic()))


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
                worker = Worker(sink)
                _state._worker = worker
                if _state._shutdown_running > 0:
                    _state._late_worker = worker
                _state._owed[id(sink)] = sink
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
    if id(sink) in _state._owed and not _state._stop.is_set():
        return
    with _state._lock:
        _offer_orphan_signal(sink)
        if id(sink) in _state._owed:
            return
        _register_exit_handler()
        _state._owed[id(sink)] = sink
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
def _offer_orphan_signal(sink: Sink) -> None:
    """Gives a sink no live worker owns an unset stop signal (SPEC-033 FR-004).

    An orphan-only process never received one before SPEC-033 FR-004 — the worker offered the
    signal to its own sink and there is no worker here — so SPEC-027's guarantee that a shutdown
    cuts a backoff short was false on this path, and the inline close at exit can sit behind an uninterruptible
    wait held by another orphan writer.

    The skip is keyed on the **moment**, not on a worker merely existing. A retired worker keeps
    its old sink forever (``Worker.retarget`` declines once retired) while every
    orphan event goes to a newly configured one, so skipping on existence would leave that live
    sink uninterruptible for the rest of the process. Where a worker is draining into the sink,
    the owner's ``_stop`` is already there and is the event its drain loop waits on; replacing it
    would leave the drain thread serving a full backoff across ``Worker.stop``'s join, which is
    the global pause SPEC-027 exists to remove.

    Liveness alone is not the predicate either, which is why the moment is its own category
    (SPEC-035 FR-001, SPEC-054 FR-004). :func:`live_worker` was wrong: the count moves on **entry** to ``_shutdown_worker``, so for the whole of the drain
    the skip stopped applying and an orphan log handed the sink a fresh unset event — precisely
    the one the drain thread was about to wait on. Measured, a 20 s backoff against
    ``shutdown(timeout=3)`` was then still outstanding when the shutdown expired at 3.01 s with
    the sink left open — the wait was never cut, not merely slow. Identity alone is wrong in the
    opposite direction: it skips for a worker whose shutdown has **finished**, leaving a sink
    still being written to holding a set event that can never clear, which is SPEC-033 FR-004's
    tight retry loop and is covered by a test that spec shipped. The predicate is therefore
    :meth:`_Lifecycle.in_flight` — the identity **conjoined with** ``Worker.draining``, plus a
    close registered against the sink — since without the identity an orphan log to sink Y would
    be skipped merely because a live worker is draining into sink X. That method's docstring
    carries the rest of the reasoning, including why the close needs the *other* moment
    predicate.

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
    is not replaced", and depends on nothing about which event ``_stop`` currently is.

    Callers hold ``_state._lock``.

    Args:
      sink: The sink to offer a signal to.

    Returns:
      None.

    Raises:
      None.
    """
    if _state.in_flight(sink):
        return
    offer_stop_signal(sink, _state.refresh_stop_signal())
def _close_taken(sink: Sink, closing: _Closing) -> None:
    """Closes one sink the closer took, absorbing whatever the close raised (SPEC-046).

    Factored out so the inline close and the threaded ones are provably the same call, and
    because a thread body must not raise: an exception escaping one reaches CPython's bootstrap,
    which prints a traceback carrying the message arch §6 keeps out of anything the library says
    about itself. It is the same guard :func:`_close_guarded` applies for a swapped-out sink, with
    the ``_diag`` text of the site it actually is (SPEC-029).

    It records nothing of its own, and needs to: :func:`_close_owed` takes the sink out of the
    owed record and registers its close **in one critical section** under ``_state._lock``, which
    is what stops two callers performing the same close (SPEC-054 FR-002, FR-003). Handing the
    registration down rather than making one here is what lets a caller register before deciding
    who performs the close, detached closes included.

    Args:
      sink: The sink to close.
      closing: The in-flight registration to discharge, whatever the close does.

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


def _close_owed(deadline: float | None = None, *, bystander_wait: bool = True) -> bool:
    """Closes every owed sink nothing is holding — the one closer, for every exit (FR-003).

    It replaces four functions that each closed sinks on one path: ``Worker._close_if_owed``,
    ``Worker._close_sink``, ``_close_orphan_sink`` and the per-sink ``_close_owed``. There is one
    owed-close record now (FR-002), so there is one thing to take from and one place that decides
    who performs a close.

    **The take, under** ``_state._lock``. Every owed sink that is not :meth:`_Lifecycle.held` is
    removed from the record and registered in :data:`_closing_now` **in the same critical
    section**, so there is no instant at which a sink is neither owed nor in flight — a gap a
    preempted orphan emit re-arms and a racing ``shutdown()`` then closes alongside. A sink left
    behind is either inside a live drain thread or already being closed by somebody else, and the
    two are answered differently below.

    **The closes, with the lock released.** SPEC-046's shape: :func:`_inline_close_choice` runs on
    this thread and the rest on threads joined in a ``finally``, so a ``KeyboardInterrupt``
    delivered mid-fan-out reaches the caller with every started close joined rather than four
    abandoned mid-write. One exception is **detached** rather than joined — a sink the worker
    holds unfenced whose thread has since ended (SPEC-050 FR-004): it already had the swap's whole
    budget, so it is far more likely stuck than slow, and joining it would let one stuck
    swapped-out sink hold the exit where :func:`join_closers` costs it only the grace.

    **The wait, and why it is on the registry rather than on the record.** A caller waits on every
    close registered in :data:`_closing_now` that is not **its own**, for :func:`closer_grace` —
    SPEC-050's rule that a second caller waits for a close rather than returning through it. The
    trigger is deliberately not "an owed sink was left behind": by the time a bystander arrives,
    the caller inside the close has already **discharged** that sink from the record, so the
    record is empty and the wait would never fire. Measured on the shipped reproduction —
    ``tests/test_worker.py::test_a_second_shutdown_waits_for_an_inline_close_it_did_not_claim``,
    with a 0.6 s close — the record-keyed version returned through it at ``closed == 0``, which
    is the defect SPEC-050 FR-002 exists to prevent, re-introduced.

    A sink held only by a *drain thread* — an expired ``shutdown()``, or a late worker mid-drain
    — still costs no grace, because it carries no registration to wait on. Excluding its own
    registrations is what stops a caller spending the grace twice on the detached close
    :func:`join_closers` already bounds. Run unconditionally, a second pass defeats FR-002's
    two-caller criterion: the first caller, just out of its own inline close,
    re-takes the re-armed sink before the bystander returns from its wait.

    **A registration this call did not hand off is discharged on the way out**, however it
    leaves. A ``KeyboardInterrupt`` delivered between the take and a close would otherwise leave
    an entry nobody sets: that sink is then skipped by the signal refresh forever, never taken by
    a later closer, and every later bystander waits out the whole grace on it. Each entry is
    dropped from that set immediately **before** its close is handed off, and the ``try`` opens
    on the line after the take rather than after the inline choice — a ``KeyboardInterrupt``
    landing inside :func:`_inline_close_choice` leaked every registration the take had just made.
    The residual window is the few bytecodes between the drop and the call — the same narrow shape SPEC-050 FR-002
    accepted for the count this replaced, and for the same reason: it contains no call, so
    nothing but a real signal storm can land in it.

    Args:
      deadline: The ``time.monotonic()`` instant the caller's budget expires, or ``None``.
      bystander_wait: Whether to wait on closes somebody else registered. ``False`` for
        :func:`_shutdown_worker`'s second pass, which runs only when there is something for it to
        do and must never wait a second time.

    Returns:
      Whether it waited on a close it did not perform, which is what tells its caller a second
      pass has something to look at.

    Raises:
      BaseException: Whatever the inline close raised, after every fan-out close it started has
        been joined. ``Exception`` from a close is absorbed by :func:`_close_taken`; what reaches
        here is the SPEC-025 pair a caller is owed.
    """
    started: list[threading.Thread] = []
    with _state._lock:
        worker = _state.worker_exists()
        taken = [sink for sink in _state._owed.values() if not _state.held(sink)]
        for sink in taken:
            del _state._owed[id(sink)]
        registrations = [(sink, _register_closing(sink)) for sink in taken]
        detach = {
            id(sink)
            for sink in taken
            if worker is not None and worker.holds_unfenced(sink)
        }
        mine = {id(sink) for sink in taken}
        with _closing_now_lock:
            waiting = (
                [entry for key, entry in _closing_now.items() if key not in mine]
                if bystander_wait
                else []
            )
    undischarged = {id(sink): (sink, closing) for sink, closing in registrations}
    try:
        inline = _inline_close_choice(taken, worker) if taken else None
        for sink, closing in registrations:
            if sink is inline:
                continue
            del undischarged[id(sink)]
            if id(sink) in detach:
                release(sink, detached=True, closing=closing)
                continue
            closer = threading.Thread(
                target=_close_taken,
                args=(sink, closing),
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
                _close_taken(sink, closing)
            else:
                started.append(closer)
        for sink, closing in registrations:
            if sink is inline:
                del undischarged[id(sink)]
                _close_taken(sink, closing)
    finally:
        for sink, closing in undischarged.values():
            _finish_closing(sink, closing)
        for closer in started:
            closer.join()
    if not waiting:
        return False
    end = monotonic() + closer_grace(deadline)
    for entry in waiting:
        entry.event.wait(max(0.0, end - monotonic()))
    return True


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

    **The count moves a second time before a late worker is stopped**, under the same lock
    (FR-001). That worker recorded the already-incremented count at its build, so without the
    second move the call that stops it leaves it reading as *live*: delivering nothing, its next
    submission uncounted, and a later swap waiting a whole budget for a fence it cannot confirm.

    **The second pass runs only when there is something for it to do** — the first waited on a
    close somebody else held, or a late worker was stopped — and it never waits itself. Run
    unconditionally it defeats FR-002's two-caller criterion: the first caller, just out of its
    own inline close, re-takes the re-armed sink before the bystander returns from its wait.
    Nothing runs a third.

    The closer grace is granted **once**, at the end, on every path. Two functions arranged that
    between them before, and granting it twice measured 4.01 s against a 2 s grace.

    Args:
      timeout: Seconds to wait for the drain, or ``None`` to wait indefinitely.

    Returns:
      None.

    Raises:
      None.
    """
    deadline = None if timeout is None else monotonic() + timeout
    with _state._lock:
        _state.retirements += 1
        _state._stop.set()
        worker = _state.worker_exists()
        if worker is None:
            _state._shutdown_running += 1
    late_worker: Worker | None = None
    if worker is not None:
        worker.stop(timeout)
        waited = _close_owed(deadline)
    else:
        try:
            waited = _close_owed(deadline)
        finally:
            with _state._lock:
                late_worker = _state._late_worker
                _state._late_worker = None
                _state._shutdown_running -= 1
        if late_worker is not None:
            with _state._lock:
                _state.retirements += 1
            late_worker.stop(None if deadline is None else max(0.0, deadline - monotonic()))
    if waited or late_worker is not None:
        _close_owed(deadline, bystander_wait=False)
    join_closers(deadline)


def _swap_sink(new_sink: Sink, timeout: float | None = DEFAULT_SWAP_TIMEOUT) -> None:
    """Retargets delivery at a new sink, backing a late ``configure(sink=...)``.

    **One function over one record** (SPEC-054 FR-003). It was two branches keeping two records
    in step, plus ``_adopt_declined_swap`` to re-home a sink the worker refused mid-swap; with
    one record the branches differ only in whether there is a drain to fence.

    With a live worker it calls :meth:`Worker.retarget`, which drains, reassigns and fences, and
    reports what it did. The owner then arms ``new_sink`` **whether or not the worker adopted
    it** — which is what ``_adopt_declined_swap`` was for, with nothing left to adopt — and
    decides the old sink:

    - **fenced** — the worker confirmed its drain reached the new sink, so nothing can still be
      inside the old one. It leaves the record and is released detached, joined to this call's
      budget.
    - **unfenced** — the drain could not be confirmed within the budget, so the old sink stays in
      the record and the worker keeps it among the sinks it may be inside. A later ``shutdown()``
      that finds the drain thread ended closes it (SPEC-050 FR-004); ``incomplete_swaps`` counts
      it, as before.
    - **declined** — the worker retired mid-swap and keeps its old sink forever (SPEC-033 FR-002).
      The old sink stays owed for the exit, and the new one is armed anyway, because it is what
      ``configure()`` installed and the orphan path will deliver to it — SPEC-035 FR-003's
      guarantee that a declined sink is owned by *somebody*.

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
    :meth:`_Lifecycle.held` says something may still be inside — is released detached and joined
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

    Args:
      new_sink: The sink to deliver to from now on.
      timeout: Seconds for the drain and the detached closes, or ``None``.

    Returns:
      None.

    Raises:
      None. A retarget that raises is absorbed and announced: the previous sink keeps receiving,
        which is a reported degradation rather than a lost configure.
    """
    deadline = None if timeout is None else monotonic() + timeout
    closers: list[threading.Thread] = []
    with _state._lock:
        worker = _state.live_worker()
        old = None if worker is None else worker.sink
        if worker is None and not _state._owed:
            return
        for stale in list(_state._owed.values()):
            if stale is new_sink or stale is old or _state.held(stale):
                continue
            del _state._owed[id(stale)]
            closer = release(stale, detached=True, closing=_register_closing(stale))
            if closer is not None:
                closers.append(closer)
        if worker is None:
            _register_exit_handler()
            _state._owed[id(new_sink)] = new_sink
            _offer_orphan_signal(new_sink)
    if worker is not None:
        try:
            outcome = worker.retarget(new_sink, deadline)
        except Exception as exc:
            _diag.absorbed(
                "swapping the log sink", exc, "events may still be delivered to the previous sink"
            )
        else:
            with _state._lock:
                _register_exit_handler()
                _state._owed[id(new_sink)] = new_sink
                previous = outcome.previous
                if outcome.verdict == "fenced" and previous is not None and previous is not new_sink:
                    _state._owed.pop(id(previous), None)
                    closer = release(
                        previous, detached=True, closing=_register_closing(previous)
                    )
                    if closer is not None:
                        closers.append(closer)
    for closer in closers:
        closer.join(None if deadline is None else max(0.0, deadline - monotonic()))


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

    with _state._lock:
        pending = list(_state._owed.values())
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
        submitted_after_shutdown=(
            0 if counters is None else counters.submitted_after_shutdown
        ),
        incomplete_swaps=0 if counters is None else counters.incomplete_swaps,
        closing_sinks=closing_count(),
        inherited_sink=_delivering_to_an_inherited_sink(),
        orphan_lost=orphan_lost,
        in_span_lost=in_span_lost,
    )

_fork.register_child_handler(_mark_inherited)
_fork.register_child_handler(_clear_after_fork)
_fork.register_child_handler(_rebuild_worker_after_fork)
