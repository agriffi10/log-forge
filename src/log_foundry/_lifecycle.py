"""Sink-lifecycle facilities shared by both delivery paths (SPEC-033 FR-005)."""

from __future__ import annotations

import os
import threading
import time
import types
from itertools import islice

from log_foundry import _diag, _fork
from log_foundry.sinks.base import Sink

DEFAULT_CLOSER_GRACE = 2.0
"""Seconds a shutdown gives an outstanding swapped-out close to finish.

Deliberately much smaller than the shutdown budget it is carved from. This is a last chance for
a close that is *nearly* done, not a second full attempt: it already had the swap's whole budget
(``DEFAULT_SWAP_TIMEOUT``) before ``shutdown`` was ever called, so one still running here is far
more likely stuck than slow, and every second spent on it is a second the process does not exit.
"""

_closers: list[threading.Thread] = []
_closers_lock = threading.Lock()

_FORK_SKIP = ("_owned",)
"""Keeps the ownership record out of ``_fork``'s repair walk (``_fork._SKIP_ATTRIBUTE``).

The record strongly references every sink this process ever acquired, so the walk would
otherwise reach ones the process abandoned several ``configure()`` calls ago and call their fork
hooks — measured, a child announced a buffer discard for a superseded sink, and a ``FileSink``
there would be reopened on every fork forever. A sink that is still live is reached through the
config and the worker, so nothing the repair needs is lost.
"""

_FOREIGN = -1
"""The pid a record carries when the sink belongs to some earlier process.

Never a real pid, so it can never match :func:`os.getpid`. Laid down by
:func:`_mark_inherited` in a forked child over everything the child inherited, which is what
gives "this process did not acquire it" a **terminal** state.

Without it the record protects nothing where it is empty: ``stamp`` is write-once, and write-once
defends only a record that already exists, so a child could ``configure()`` its way into *owning*
a sink the parent never recorded and then close it entirely legitimately. Measured before this
existed — a child claimed a connection sink held behind a third-party wrapper and closed it,
destroying the parent's transport. Unrecorded has to be unclaimable, not merely unreleasable.
"""

_MARKING_CEILING = 100_000
"""Objects the child's marking walk may visit before it gives up and refuses everything.

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

``_worker_lock`` → ``_config_lock`` → this, never the reverse in any pair. The three-term form
is the real one: ``decorator._get_worker`` calls ``config._ensure_sink()`` while holding
``_worker_lock``, and ``_ensure_sink``'s construction branch takes ``_config_lock`` before it can
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
        ``_fork``; what it holds is then unmarked, therefore unrecorded, therefore refused.
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
            "what it holds is not marked, so this child will refuse to close it",
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
    from log_foundry import config, decorator

    worker = decorator._worker
    candidates = (
        config._live_config().sink,
        None if worker is None else worker.sink,
        decorator._orphan_sink,
        decorator._orphan_closed_sink,
    )
    roots: list[object] = [found for found in candidates if found is not None]
    with _owned_lock:
        roots.extend([reference for _pid, reference in _owned.values()])
    return roots


def _mark_inherited() -> None:
    """Marks every sink this child inherited as foreign, before anything can claim it (FR-001).

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
        it did not reach are then unrecorded rather than marked. The outer guard is for a fault
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
    except Exception as exc:
        _marking_failed = True
        _diag.absorbed(
            "marking the sinks a forked child inherited",
            exc,
            "this child will refuse to close any sink it has no record of",
        )
        return
    with _owned_lock:
        for inherited in found:
            _owned.setdefault(id(inherited), (_FOREIGN, inherited))
    for reacquired in _fork.reacquired_in_child:
        reclaim(reacquired)


def reclaim(sink: object) -> None:
    """Records that a sink re-acquired its transport in this process (SPEC-042 FR-005).

    The one write that **overrides** an existing record, and it has to be: :func:`_mark_inherited`
    has already stamped everything inherited ``_FOREIGN`` by the time the hook roster is read, so
    a ``setdefault`` here would leave a sink that provably holds its own descriptor refused
    forever.

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

    - Neither recorded — a graph the library never saw. The caller owns it; honour the close.
    - The child recorded elsewhere — the inherited sink, or one :func:`_mark_inherited` marked
      ``_FOREIGN``. Refused however it was reached, which is what closes the wrapper route
      (FR-002 AC-3).
    - The wrapper recorded, the child not — the sink added to a wrapper *after* ``configure()``
      walked it. Refused, per FR-001 AC-6: the library holds this graph, so a member it has no
      record of is one it must not assume is this process's. The consequence is a leak, recorded
      in §13.

    **"Unrecorded is the caller's" is only sound because a fork makes it false first.** In a
    child, :func:`_mark_inherited` records everything inherited as ``_FOREIGN`` *before* any
    handler runs, so an unrecorded sink there is one built after the fork. If that walk could
    not finish, :data:`_marking_failed` withdraws the assumption entirely and every unrecorded
    sink is refused.

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
    sink.close()
    return None


def _start_closer(sink: Sink) -> threading.Thread | None:
    """Starts a daemon close of a sink no longer being delivered to (SPEC-030 FR-003).

    The thread is returned rather than joined, so a caller holding a lock can start under it and
    wait after releasing it — ``decorator._swap_sink`` mutates its records under the process-wide
    ``_worker_lock`` and must not hold that across a wait of the swap's whole budget (SPEC-033
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


_fork.register_child_handler(_mark_inherited)
