"""The private wrapper behind ``configure(sink_factory=…)``: one sink per process (SPEC-079)."""

from __future__ import annotations

import functools
import os
import threading
from typing import TYPE_CHECKING

from log_foundry import _diag, _lifecycle, _sink_arg
from log_foundry.sinks._wrapper import ForwardedStopSignal
from log_foundry.sinks.base import Sink, SinkDeliveryError, SinkLosses, flush_sink, read_losses

if TYPE_CHECKING:
    from collections.abc import Callable


class _Build:
    """One claim on a factory wrapper's build: who runs the factory, and how its waiters wake.

    A token per build, rather than fields on the wrapper, so whatever settles or yields a build
    touches only its own: a builder whose claim was taken over records nothing over its
    successor's, and a yield never carries into the next build.

    Attributes:
      owner: ``(pid, thread ident)`` of the thread running the factory.
      wake: Set when the build settles or starts yielding; waiters wait on it with no lock held.
        Cleared when the last yield ends, so a waiter arriving after it waits again; a waiter the
        set already woke has returned from its wait, and the clear does not put it back.
      yields: How many calls on the owner's thread are yielding this build right now.
      yielded: How many yields this build has started, ever; a waiter that sees it move while
        parked was woken by a yield, and refuses even if that yield has already ended.
    """

    def __init__(self, owner: tuple[int, int]) -> None:
        """Records the claimant; nothing is waiting yet.

        Args:
          owner: ``(pid, thread ident)`` of the claiming thread.

        Returns:
          None.

        Raises:
          None.
        """
        self.owner = owner
        self.wake = threading.Event()
        self.yields = 0
        self.yielded = 0


class _FactorySink:
    """Builds this process's sink on its first emit, and delivers to it after (SPEC-079 FR-006).

    ``configure(sink_factory=f)`` installs one of these as the configured sink and never calls
    ``f``; the first ``emit`` in each process does, on whichever thread emits — the drain thread
    for span events, the caller's thread on the orphan path — so the caller's thread inside a
    span carries no sink construction (invariant 4), and a prefork master that never logs never
    builds one. Lazy in every process is Andrew's decision of 2026-11-04. A forked child is given
    a new one by ``_lifecycle._adopt_factory_after_fork``, so the child builds its own.

    **The build.** One thread claims the build under :attr:`_build_lock`, then calls the factory
    once with **no lock held**; its result is checked with ``isinstance(result, Sink)``, stamped
    for this process (``_lifecycle.stamp``) and recorded as :attr:`_pending` under the lock again.
    Concurrent first emits wait on the build with no lock held, and the factory runs once between
    them. The factory is the caller's code and may call back into the library: held under the
    lock, a factory that ran a ``@trace``d function and then called ``flush(None)`` or
    ``shutdown(None)`` hung, because the drain thread it waited on was blocked on that lock
    (measured at ``9f4d4dfd``). An event instead of a lock does not break that cycle by itself, so
    while the factory is inside ``flush()``, ``shutdown()`` or a sink-changing ``configure()``,
    which can wait on the drain thread, every emit waiting on the build refuses instead
    (:meth:`_yield_start`). The lock is never held across a delivery.

    **The hand runs after the lock is released**, and the built sink is published as
    :attr:`_inner` only after it holds the stop signal this wrapper was offered. The hand runs the
    built sink's setter, its own code: held under the build lock, a setter that logged waited on
    its own thread, and one that called ``shutdown()`` or ``flush()`` with no timeout joined a
    drain thread blocked on that lock, forever. So every thread that finds a sink pending but not
    yet published hands it the signal itself before using it — duplicate hands of one signal are
    harmless — except the thread already handing it (``_lifecycle.handing_here``), which is a
    setter emitting to its own sink and is delivered to the pending sink as is. Later emits read
    :attr:`_inner` without the lock. After publishing, the hand reads the wrapper's signal again
    and hands the built sink any newer one, because a signal forwarded while :attr:`_inner` was
    still unpublished reached nothing; the forwarder stores before it reads :attr:`_inner` and the
    hand publishes before it reads the signal, so one of the two sees the other. The re-reads
    stop after ``_lifecycle._SETTLE_ROUNDS``, which only a setter that changes the wrapper's
    signal on every hand can reach; the next signal handed to the wrapper is forwarded to the
    published sink as usual. Anything that released the wrapper before the hand ended — a setter
    swapping the sink or shutting down, or a ``configure(sink=...)`` on another thread while the
    factory was still running, when there was nothing built yet to release — leaves the wrapper
    armed again (``_lifecycle.rearm_if_released``), because the emit that follows writes to the
    built sink (invariant 5). Before, that sink was written to and closed by nobody. **What is
    answered is a release that landed during the build or the hand, and nothing else.** A wrapper
    the library owed a close when the emit began is re-armed when it is no longer owed. A wrapper
    the library does not owe — held inside a ``MultiSink`` or ``FilteringSink`` the caller
    configured, which the library closes and which closes this wrapper in turn — is re-armed only
    when :meth:`close` ran during the build or the hand, which :attr:`_releases` counts: arming it
    on every build made the library close it once itself and once more through the outer wrapper,
    so the built sink was closed twice (measured: ``configure(sink_factory=f)``,
    ``configure(sink=MultiSink(get_config().sink))``, ``info()``, ``shutdown()`` closed it twice,
    against once at ``624d817f``), and not arming it on a release left the built sink, written to
    after the outer wrapper's close, closed by nobody, which predates this fix. A wrapper a caller
    emits to directly, outside the library's delivery, is never closed during its own build and so
    never armed.

    **A failed build** — the factory raising ``Exception``, or returning something that is not a
    ``Sink`` — is announced here through ``_diag.absorbed``, naming the exception's type or the
    returned object's type and never a value (invariant 11), on the first failure and every
    ``WARN_EVERY``-th after it with the running total, because a factory that keeps failing fails
    once per emit. **A built sink that contains itself is a failed build too**: one leading back
    to this wrapper — the factory wrapped ``get_config().sink`` — or holding a wrapper that leads
    back to itself, found by ``_sink_arg.loop_in``, because storing it would make every emit
    recurse. It then raises ``SinkDeliveryError`` chained from the cause, which the path
    that made the call counts and announces as any sink's failure: the worker retries under its
    ``max_retries``, the orphan path counts ``orphan_lost``. The next emit calls the factory
    again. A non-sink result is dropped unclosed, since it has no ``close`` the library can
    trust, and so is a looped one, since the library cannot tell which of its sinks the caller
    still holds elsewhere. A ``BaseException`` from the factory propagates unchanged (SPEC-025).

    **An emit from inside the factory**, on the same thread, raises ``SinkDeliveryError`` at once
    rather than calling the factory again or waiting on the build its own thread runs; there is
    no sink yet to deliver to. The claim is keyed by process and thread ident, because a wrapper a
    forked child inherited mid-build carries the parent's claim, which the child claims over
    rather than waiting on a build no thread of its own will finish, and a child thread reusing
    that ident would otherwise be refused forever.

    It takes **no** transport lock (SPEC-028 FR-002): it holds no transport of its own, and the
    built sink serialises its own delivery. It **adds no post-close guard** (SPEC-032 FR-003):
    after a close, an emit with a built sink is delegated to it and the built sink decides
    (invariant 7), and an emit with nothing built builds one, since nothing was released.

    It defines no ``deliver`` and no ``reacquire_after_fork``: the built sink's own engine runs
    inside its ``emit`` (SPEC-072, SPEC-075), and the built sink's own hook is reached by the fork
    walk through :attr:`_inner`.

    Attributes:
      log_foundry_stop_signal: The worker's shutdown event, stored where the engine reads it and
        forwarded to the built sink once there is one.
    """

    log_foundry_stop_signal = ForwardedStopSignal()

    def __init__(self, factory: Callable[[], Sink]) -> None:
        """Holds the factory; builds nothing.

        Args:
          factory: Called with no arguments, at most once per successful build, to build this
            process's sink.

        Returns:
          None.

        Raises:
          None.
        """
        self._factory = factory
        self._build_lock = threading.Lock()
        self._inner: Sink | None = None
        self._pending: Sink | None = None
        self._releases = 0
        self._claim: _Build | None = None
        self._failures = 0
        vars(self)["log_foundry_stop_signal"] = None

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Delivers the batch to this process's sink, building it first if there is none yet.

        Args:
          batch: The events to deliver.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When the build fails, or is re-entered from inside the factory.
          Exception: Whatever the built sink's ``emit`` raises, unchanged.
        """
        inner = self._inner
        if inner is None:
            inner = self._build()
        inner.emit(batch)

    def _build(self) -> Sink:
        """Returns this process's sink, building it once and handing it its stop signal.

        Args:
          None.

        Returns:
          The built sink, or the one a concurrent first emit built.

        Raises:
          SinkDeliveryError: When this thread is already inside the factory, when the build it
            would wait on is yielding (:meth:`_yield_start`), or the factory raises
            ``Exception``, returns something that is not a ``Sink``, or returns a sink that leads
            back to this wrapper or to a wrapper of its own (``_sink_arg.loop_in``).
        """
        owed = _lifecycle.owes_close(self)
        seen = self._releases
        built = self._pending
        if built is None:
            built = self._make()
        if not _lifecycle.handing_here(built):
            self._hand(built, owed=owed, seen=seen)
        return built

    def _make(self) -> Sink:
        """Claims the build, calls the factory once with no lock held, and records what it built.

        :attr:`_build_lock` is held only to claim a build, to read a claim, and to record a
        result, never across the factory call. A thread finding another thread of this process
        running the factory waits on that build's ``wake`` with no lock held, then looks again:
        it returns the recorded sink, claims a build the last one left unrecorded (the factory
        failed, so it is called again, as the next emit always retried), or refuses while the
        builder is yielding (:meth:`_yield_start`), or when a yield started while it was parked,
        even one that has already ended: the yield's own call may be waiting on this very thread.
        A claim another process made — a child forked mid-build, reaching this inherited wrapper
        through a caller's own wrapper — is nobody's here, and is claimed over.

        The claim is cleared, and the result recorded, in one ``finally`` under the lock, then
        ``wake`` is set: a ``BaseException`` from the factory or the checks leaves no claim
        behind, which would refuse every later emit on this thread as re-entrant and spin every
        waiter on a set event. ``_announce`` runs before that ``finally``, while the claim is
        still held, so its counter has one writer. A builder whose claim was taken over records
        nothing, releases what it built, which nothing has written to, and looks again — unless
        what it built is the very sink its successor recorded, as a factory returning one shared
        sink does: releasing that closed the live sink, which was then written to after its close
        (measured by the second diff review, in a forked child logging from inside the factory).

        Args:
          None.

        Returns:
          The sink built here, or the one a concurrent first emit built while this one waited.

        Raises:
          SinkDeliveryError: As :meth:`_build`.
        """
        me = (os.getpid(), threading.get_ident())
        lock = self._build_lock
        parked: tuple[_Build, int] | None = None
        while True:
            with lock:
                built = self._pending
                if built is not None:
                    return built
                running = self._claim
                if running is None or running.owner[0] != me[0]:
                    claim = self._claim = _Build(me)
                    break
                if running.owner == me:
                    raise SinkDeliveryError(
                        "sink_factory emitted from inside itself; the event is lost"
                    )
                if running.yields or (
                    parked is not None and parked[0] is running and parked[1] != running.yielded
                ):
                    raise SinkDeliveryError(
                        "sink_factory called flush(), shutdown() or configure() while this emit "
                        "would have waited on it; the event is lost"
                    )
                parked = (running, running.yielded)
                wake = running.wake
            wake.wait()
        result: Sink | None = None
        recorded = False
        try:
            with _lifecycle.building(
                functools.partial(self._yield_start, claim),
                functools.partial(self._yield_end, claim),
            ):
                try:
                    made = self._factory()
                except Exception as exc:
                    self._announce(exc, "")
                    raise SinkDeliveryError("sink_factory raised; the next emit retries") from exc
            result = self._checked(made)
        finally:
            with lock:
                if self._claim is claim:
                    self._claim = None
                    if result is not None:
                        self._pending = result
                        recorded = True
                claim.wake.set()
        if result is not None and not recorded:
            if result is not self._pending and result is not self._inner:
                self._discard(result)
            return self._make()
        return result

    def _checked(self, result: object) -> Sink:
        """Refuses what the factory returned unless it is a sink with no loop; stamps it if so.

        Args:
          result: What the factory returned.

        Returns:
          The result, stamped for this process.

        Raises:
          SinkDeliveryError: When it is not a ``Sink``, or leads back to this wrapper or to a
            wrapper of its own (``_sink_arg.loop_in``).
        """
        if not isinstance(result, Sink):
            stored = _diag._STR_STR(_diag._STORED_NAME(type(result)))
            refusal = SinkDeliveryError("sink_factory did not return a sink")
            self._announce(refusal, f"it returned {stored}, not a sink")
            raise refusal
        looped = _sink_arg.loop_in(result, back_to=self)
        if looped is not None:
            stored = _diag._STR_STR(_diag._STORED_NAME(type(looped)))
            where = (
                "leads back to the sink_factory= wrapper"
                if looped is self
                else f"holds a {stored} that leads back to itself"
            )
            refusal = SinkDeliveryError("sink_factory returned a sink that contains itself")
            self._announce(refusal, f"what it returned {where}")
            raise refusal
        _lifecycle.stamp(result)
        return result

    def _discard(self, built: Sink) -> None:
        """Releases a sink built under a claim another thread took over, absorbing its failure.

        Reached only in a forked child whose factory resumed after a thread of the child claimed
        over the parent's build. Nothing has written to the sink, so it is closed once here.

        Args:
          built: The sink nobody recorded.

        Returns:
          None.

        Raises:
          None.
        """
        try:
            _lifecycle.release(built, owner=self)
        except Exception as exc:
            _diag.absorbed(
                "closing a sink_factory sink built under a claim another thread took over",
                exc,
                "it may be left open",
            )

    def _yield_start(self, claim: _Build) -> None:
        """Makes threads waiting on *claim*, and arriving at it, refuse until :meth:`_yield_end`.

        Run by ``_lifecycle.yielding`` on the builder's own thread, when the factory calls
        ``flush()``, ``shutdown()`` or a sink-changing ``configure()``: each can wait on the drain
        thread, which may be waiting on this build. ``wake`` is set so a waiter already parked
        refuses at once, and ``yielded`` moves so a waiter woken by it refuses even if the yield
        has ended by the time it looks.

        Args:
          claim: The build this thread is running.

        Returns:
          None.

        Raises:
          None.
        """
        lock = self._build_lock
        with lock:
            claim.yields += 1
            claim.yielded += 1
            claim.wake.set()

    def _yield_end(self, claim: _Build) -> None:
        """Ends one yield of *claim*; after the last, threads arriving at the build wait again.

        ``wake`` is cleared when the last yield ends, so an emit on another thread is refused only
        while the factory is inside one of those calls, never for the rest of a slow build.

        Args:
          claim: The build this thread is running.

        Returns:
          None.

        Raises:
          None.
        """
        lock = self._build_lock
        with lock:
            claim.yields -= 1
            if not claim.yields and self._claim is claim:
                claim.wake.clear()

    def _hand(self, built: Sink, *, owed: bool, seen: int) -> None:
        """Hands the built sink this wrapper's stop signal, with no lock held, then publishes it.

        Args:
          built: The pending sink.
          owed: Whether the library owed this wrapper a close when the emit began, so a release
            landing during the build or the hand is one the emit that follows must answer.
          seen: :attr:`_releases` as the emit began; a different count now means :meth:`close`
            ran during the build or the hand, through whatever holds this wrapper.

        Returns:
          None.

        Raises:
          None.
        """
        with _lifecycle.handing(built):
            handed = vars(self).get("log_foundry_stop_signal")
            if handed is not None:
                _lifecycle.offer_stop_signal(built, handed)
            self._inner = built
            for _ in range(_lifecycle._SETTLE_ROUNDS - 1):
                now = vars(self).get("log_foundry_stop_signal")
                if now is handed or now is None:
                    break
                _lifecycle.offer_stop_signal(built, now)
                handed = now
        if owed or self._releases != seen:
            _lifecycle.rearm_if_released(self)

    def _announce(self, exc: BaseException, what: str) -> None:
        """Counts one failed build; writes the line on the first and every ``WARN_EVERY``-th.

        Args:
          exc: The factory's exception, or the refusal of what it returned.
          what: What the factory returned, by type, when it returned a non-sink; else empty.

        Returns:
          None.

        Raises:
          None.
        """
        self._failures += 1
        if self._failures == 1 or self._failures % _diag.WARN_EVERY == 0:
            lead = f"{what}; " if what else ""
            _diag.absorbed(
                "building this process's sink with sink_factory",
                exc,
                f"{lead}{self._failures} build(s) failed so far; the next emit retries",
            )

    def flush(self) -> None:
        """Forwards the flush to the built sink, and does nothing when none is built.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the built sink's flush raises, so the failure reaches the caller as
            a ``FlushResult`` reason.
        """
        inner = self._inner
        if inner is not None:
            flush_sink(inner)

    def losses(self) -> SinkLosses | None:
        """Reports the built sink's losses, and zeros when none is built; counts nothing itself.

        Args:
          None.

        Returns:
          The built sink's losses (``None`` when it reports none), or zeros.

        Raises:
          None.
        """
        inner = self._inner
        return SinkLosses(dropped=0, failed=0) if inner is None else read_losses(inner)

    def close(self) -> None:
        """Releases the built sink as the shipped wrappers release theirs; nothing when none.

        Routed through ``_lifecycle.release`` with this wrapper as the owner, so a sink this
        process may not release is refused here as at the lifecycle's own closers (SPEC-042).
        It releases the **pending** sink when there is one, not only a published one: a setter on
        the built sink that swaps the configured sink closes this wrapper before the hand has
        published, and reading only the published sink left that sink, written to a moment later,
        closed by nobody (measured at ``624d817f``). It counts itself first, in ``_releases``, so
        a build it overlapped re-arms this wrapper even where the library does not owe it a close.
        After a close, an emit with a built sink is delegated to it and the built sink decides
        (invariant 7); an emit with nothing built builds one, since nothing was released
        (SPEC-032).

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the built sink's ``close`` raises.
        """
        self._releases += 1
        inner = self._pending
        if inner is None:
            inner = self._inner
        if inner is not None:
            _lifecycle.release(inner, owner=self)

    def _forward_stop_signal(self, signal: threading.Event | None) -> None:
        """Hands the worker's stop signal to the built sink, when there is one.

        Called by :class:`~log_foundry.sinks._wrapper.ForwardedStopSignal` after it stored the
        signal in this wrapper's ``__dict__``; a sink built later is handed it at its build.

        Args:
          signal: The worker's shutdown event, or ``None``.

        Returns:
          None.

        Raises:
          None.
        """
        inner = self._inner
        if inner is not None and signal is not None:
            _lifecycle.offer_stop_signal(inner, signal)
