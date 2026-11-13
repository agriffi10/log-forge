"""SPEC-083: a child forked inside a stop-signal setter is cut short by its own ``shutdown()``.

A sink's ``log_foundry_stop_signal`` setter is the sink's own code, and it can fork, for example by
starting a fork-context ``multiprocessing.Process``. A child that leaves through ``os._exit`` is
still inside that setter call for its whole life. The library records each offer on the offering
thread (FR-001). The child's fork handler keeps the forking thread's offered events, re-initialised
(FR-002), and the child's ``shutdown()`` sets them (FR-003).

Every sink here deliberately does **not** subclass ``Sink``. A sink that does is given the child's
own signal by the repair walk, so it never showed the defect. Every hang guard runs in a forked
child with a bounded wait, through ``test_fork_lifecycle.run_in_child``.

Serves invariants 3, 6 and 12.
"""

from __future__ import annotations

import gc
import os
import select
import signal
import threading
import time
from typing import TYPE_CHECKING

import pytest

import log_foundry
from log_foundry import _lifecycle
from log_foundry.sinks._factory import _FactorySink
from log_foundry.sinks.filtering import FilteringSink
from log_foundry.sinks.memory import MemorySink
from test_fork_lifecycle import report_from_child, run_in_child

if TYPE_CHECKING:
    from collections.abc import Callable

ENTERED = "entered;"
"""What a child reports before it calls anything that could re-enter the setter."""

GATE = 5.0
"""Seconds a racing thread, a nested child or a parent's lock acquisition is given."""

BACKOFF = 5.0
"""The backoff a thread in the child waits on the event the sink holds."""

CUT = 1.0
"""How soon after ``shutdown()`` returns that backoff must have ended."""


class Plain:
    """A sink that accepts every batch, with no stop signal."""

    def emit(self, batch: list[dict[str, object]]) -> None:
        pass

    def close(self) -> None:
        pass


class Holder(Plain):
    """A sink that stores the signal it is offered and runs ``act`` once, inside its setter.

    It counts the calls made in a process other than the one that built it. With ``locked``, it
    holds its own ``threading.Lock`` across the setter's body, as reasonable sink code may. In a
    child the acquisition blocks, so a re-entered setter hangs the child on its own lock and the
    child fails on its bound. In the test process the acquisition is bounded by ``GATE`` and a
    timeout is counted instead, so a re-entry there fails the test rather than the xdist worker.
    """

    def __init__(self, *, locked: bool = False) -> None:
        self._signal: threading.Event | None = None
        self._lock = threading.Lock()
        self.locked = locked
        self.home = os.getpid()
        self.calls = 0
        self.child_calls = 0
        self.depth = 0
        self.max_depth = 0
        self.timed_out = 0
        self.act: Callable[[threading.Event], None] | None = None

    @property
    def log_foundry_stop_signal(self) -> threading.Event | None:
        return self._signal

    @log_foundry_stop_signal.setter
    def log_foundry_stop_signal(self, event: threading.Event) -> None:
        self.calls += 1
        in_child = os.getpid() != self.home
        if in_child:
            self.child_calls += 1
        got = False
        if self.locked:
            got = self._lock.acquire() if in_child else self._lock.acquire(timeout=GATE)
            if not got:
                self.timed_out += 1
        self.depth += 1
        self.max_depth = max(self.max_depth, self.depth)
        try:
            self._signal = event
            act, self.act = self.act, None
            if act is not None:
                act(event)
        finally:
            self.depth -= 1
            if got:
                self._lock.release()


class Snapshotting(Plain):
    """A sink whose setter records this thread's offer record on every call."""

    def __init__(self) -> None:
        self.seen: list[tuple[threading.Event, list[tuple[int, threading.Event]]]] = []

    @property
    def log_foundry_stop_signal(self) -> None:
        return None

    @log_foundry_stop_signal.setter
    def log_foundry_stop_signal(self, event: threading.Event) -> None:
        self.seen.append((event, list(_lifecycle._offers_here())))


@log_foundry.trace
def _traced() -> None:
    """A traced call, whose span close builds the worker."""


def _hand(path: str, holder: Holder) -> None:
    """Configures ``holder`` and makes the library hand it a signal on one hand path.

    Each path's first hand to the holder is the one named, and ``configure()`` hands it nothing:
    ``orphan`` and ``filtering`` through ``_hand_out`` from an orphan log, ``build`` through
    ``_hand_out`` from the worker build, and ``retarget`` through ``_hand_retargeted``.
    """
    if path == "orphan":
        log_foundry.configure(service="t", sink=holder)
        log_foundry.info("hands it")
    elif path == "filtering":
        log_foundry.configure(service="t", sink=FilteringSink(holder))
        log_foundry.info("hands it")
    elif path == "build":
        log_foundry.configure(service="t", sink=holder)
        _traced()
    else:
        log_foundry.configure(service="t", sink=Plain())
        _traced()
        log_foundry.flush()
        log_foundry.configure(service="t", sink=holder)


def _describe(stale: list[threading.Event], event: threading.Event) -> str:
    """Renders a stale-signal list, naming ``event`` by identity."""
    return "[" + ",".join("E" if kept is event else "other" for kept in stale) + "]"


def _waits_cut_short(holder: Holder) -> str:
    """In a child: whether a wait on the event the sink holds ends at the child's shutdown."""
    held = holder.log_foundry_stop_signal
    assert held is not None
    own = held is _lifecycle._state._stop
    finished: list[tuple[bool, float]] = []
    waiter = threading.Thread(
        target=lambda: finished.append((held.wait(BACKOFF), time.monotonic())), daemon=True
    )
    waiter.start()
    log_foundry.info("child")
    log_foundry.flush(timeout=1)
    started = time.monotonic()
    log_foundry.shutdown(timeout=1)
    returned = time.monotonic()
    waiter.join(BACKOFF + 1)
    log_foundry.shutdown(timeout=1)
    ok, at = finished[0] if finished else (None, 0.0)
    cut = ok is True and started <= at < returned + CUT
    return f"own={own} ok={ok} cut={cut} second=ok"


# -- FR-001: every offer is recorded on the offering thread for its length -----------------------


def test_an_offer_is_on_this_threads_record_for_its_length() -> None:
    """AC1: inside the setter the record ends with this offer, and another thread's is empty."""
    holder = Holder()
    event = threading.Event()
    seen: dict[str, object] = {}

    def act(_offered: threading.Event) -> None:
        seen["last"] = _lifecycle._offers_here()[-1]
        other: list[list[tuple[int, threading.Event]]] = []
        thread = threading.Thread(target=lambda: other.append(list(_lifecycle._offers_here())))
        thread.start()
        thread.join(GATE)
        seen["other"] = other

    holder.act = act
    _lifecycle.offer_stop_signal(holder, event)
    assert seen == {"last": (id(holder), event), "other": [[]]}, seen


def test_a_nested_offer_sees_the_outer_entry_first() -> None:
    """AC2: the factory wrapper's setter offers its built sink, nested inside its own offer."""
    inner = Snapshotting()
    wrapper = _FactorySink(MemorySink)
    vars(wrapper)["_inner"] = inner
    event = threading.Event()
    _lifecycle.offer_stop_signal(wrapper, event)
    assert inner.seen == [(event, [(id(wrapper), event), (id(inner), event)])], inner.seen


@pytest.mark.parametrize("leaves", ["returns", "raises", "interrupted"])
def test_the_record_is_restored_however_the_setter_leaves(leaves: str) -> None:
    """AC3: an absorbed ``Exception`` and a propagating ``KeyboardInterrupt`` both pop the entry."""
    before = list(_lifecycle._offers_here())
    holder = Holder()

    def act(_offered: threading.Event) -> None:
        if leaves == "raises":
            raise RuntimeError("the setter fails")
        if leaves == "interrupted":
            raise KeyboardInterrupt

    holder.act = act
    if leaves == "interrupted":
        with pytest.raises(KeyboardInterrupt):
            _lifecycle.offer_stop_signal(holder, threading.Event())
    else:
        _lifecycle.offer_stop_signal(holder, threading.Event())
    assert holder.calls == 1
    assert _lifecycle._offers_here() == before


# -- FR-002: a child keeps the forking thread's offered events, safe to set -----------------------


def test_a_child_forked_inside_a_setter_keeps_the_offered_event() -> None:
    """AC1: the child holds the offered event itself, by identity, and nothing else."""
    holder = Holder()
    event = threading.Event()
    outputs: list[str] = []
    holder.act = lambda offered: outputs.append(
        run_in_child(lambda: _describe(_lifecycle._state._stale_signals, offered)).output
    )
    _lifecycle.offer_stop_signal(holder, event)
    assert outputs == ["[E]"], outputs


def test_a_child_forked_outside_any_offer_keeps_nothing() -> None:
    """AC2: a child forked by a thread inside no offer starts with no stale signal."""
    child = run_in_child(lambda: repr(_lifecycle._state._stale_signals))
    assert child.output == "[]", child.output


@pytest.mark.parametrize(("forker", "expected"), [("other-thread", "[]"), ("setter-thread", "[E]")])
def test_a_grandchild_keeps_only_its_forking_threads_offers(forker: str, expected: str) -> None:
    """AC3: the record is per thread and replaced on every fork, never extended.

    The child is forked inside the setter for ``E``. A grandchild forked from another of its
    threads finds that thread's empty record. One forked by the thread still inside the setter
    finds ``E`` on its record again.
    """
    holder = Holder()
    event = threading.Event()
    outputs: list[str] = []

    def grandchild() -> str:
        return _describe(_lifecycle._state._stale_signals, event)

    def child() -> str:
        if forker == "setter-thread":
            return run_in_child(grandchild).output
        out: list[str] = []
        thread = threading.Thread(target=lambda: out.append(run_in_child(grandchild).output))
        thread.start()
        thread.join(GATE * 3)
        return out[0] if out else "no grandchild"

    holder.act = lambda _offered: outputs.append(run_in_child(child, timeout=30).output)
    _lifecycle.offer_stop_signal(holder, event)
    assert outputs == [expected], outputs


def test_an_event_subclass_is_recorded_but_not_kept() -> None:
    """AC4: the record holds whatever is offered; the child keeps exact ``threading.Event`` only."""

    class Sub(threading.Event):
        pass

    holder = Holder()
    event = Sub()
    seen: list[str] = []

    def act(offered: threading.Event) -> None:
        recorded = _lifecycle._offers_here()[-1] == (id(holder), offered)
        stale = run_in_child(lambda: repr(_lifecycle._state._stale_signals)).output
        seen.append(f"recorded={recorded} stale={stale}")

    holder.act = act
    _lifecycle.offer_stop_signal(holder, event)
    assert seen == ["recorded=True stale=[]"], seen


def test_a_child_sets_an_offered_event_whose_condition_a_parent_thread_held() -> None:
    """AC5: the event is re-initialised at the fork, so the child's shutdown sets it and returns.

    A parent thread holds the offered event's condition across the fork. Without the
    re-initialisation the child's ``shutdown()`` waits forever on that condition, and the child
    reports only how far it got before the parent's bound kills it.
    """
    holder = Holder()
    event = threading.Event()
    outputs: list[str] = []

    def child() -> str:
        report_from_child(ENTERED)
        kept = any(stale is event for stale in _lifecycle._state._stale_signals)
        report_from_child(f"kept={kept};")
        log_foundry.shutdown(timeout=1)
        return f"returned set={event.is_set()}"

    def act(offered: threading.Event) -> None:
        held, release = threading.Event(), threading.Event()

        def hold() -> None:
            with offered._cond:  # type: ignore[attr-defined]
                held.set()
                release.wait(GATE * 3)

        threading.Thread(target=hold, daemon=True).start()
        held.wait(GATE)
        try:
            child_run = run_in_child(child, timeout=5)
            outputs.append(f"{child_run.output}|{child_run.ending}")
        finally:
            release.set()

    holder.act = act
    _lifecycle.offer_stop_signal(holder, event)
    assert outputs == ["entered;kept=True;returned set=True|exited 0"], outputs


# -- FR-003: the child's shutdown sets the signals it was being offered -------------------------


@pytest.mark.parametrize("path", ["orphan", "filtering", "build", "retarget"])
def test_a_childs_shutdown_cuts_short_a_wait_on_the_offered_event(path: str) -> None:
    """AC1, AC2, AC6, AC7: a wait in the child on the event the sink holds ends at its shutdown.

    The setter stores the event and forks. In the child a thread waits ``BACKOFF`` seconds on the
    event the sink holds, which is not the child's own signal, so only the set this spec adds can
    end it. The wait must end because the event was set, after ``shutdown()`` began and within
    ``CUT`` of its return; a second ``shutdown()`` returns too. The parent's event stays unset.
    At ``53b68203`` the orphan rows waited the full 5 s. The ``build`` and ``retarget`` rows read
    ``own=True`` there, because the resumed worker's hand re-ran the setter in the fork handler;
    once FR-004 skips that hand, this set is the only thing that ends their wait (AC4).
    """
    holder = Holder()
    outputs: list[str] = []
    offered: list[threading.Event] = []

    def act(event: threading.Event) -> None:
        offered.append(event)
        outputs.append(run_in_child(lambda: _waits_cut_short(holder)).output)

    holder.act = act
    _hand(path, holder)
    assert outputs == ["own=False ok=True cut=True second=ok"], outputs
    assert not offered[0].is_set(), "the parent's event was set by the child"


def _unwound(holder: Holder) -> str:
    """In a child that unwound out of the setter: the held event, the flush's hand and the cut."""
    held = holder.log_foundry_stop_signal
    assert held is not None
    before = held is _lifecycle._state._stop
    finished: list[tuple[bool, float]] = []
    waiter = threading.Thread(
        target=lambda: finished.append((held.wait(BACKOFF), time.monotonic())), daemon=True
    )
    waiter.start()
    log_foundry.flush(timeout=1)
    own = holder.log_foundry_stop_signal is _lifecycle._state._stop
    started = time.monotonic()
    log_foundry.shutdown(timeout=1)
    returned = time.monotonic()
    waiter.join(BACKOFF + 1)
    ok, at = finished[0] if finished else (None, 0.0)
    cut = ok is True and started <= at < returned + CUT
    return f"before={before} own={own} cut={cut}"


def test_a_child_that_unwinds_out_of_a_retarget_hand_is_cut_short_and_then_handed_its_own() -> None:
    """AC5: a bare ``os.fork()`` in the setter ``Worker.retarget`` runs, whose child returns.

    The child unwinds out of the setter, so its own ``finally`` blocks pop the offer record. It
    still holds the parent's event, because the resumed worker skipped the sink being offered and
    ``_hand_retargeted`` has no settle loop. A thread waits on that event, and the child's
    ``flush()`` hands the sink its own signal, settling the fork's debt. Only the set of the kept
    event can end the wait, at the child's ``shutdown()``.
    """

    def work() -> str:
        read_fd, write_fd = os.pipe()
        parent = os.getpid()
        holder = Holder()
        forked: list[int] = []

        def fork(_offered: threading.Event) -> None:
            gc.collect()
            pid = os.fork()
            if pid == 0:
                signal.alarm(int(GATE * 3))
            forked.append(pid)

        holder.act = fork
        _hand("retarget", holder)
        if os.getpid() != parent:
            try:
                line = _unwound(holder)
            except BaseException as exc:
                line = f"!{type(exc).__name__}: {exc}"
            os.write(write_fd, line.encode())
            os._exit(0)
        os.close(write_fd)
        ready = select.select([read_fd], [], [], GATE * 3)[0]
        line = os.read(read_fd, 256).decode() if ready else "silent"
        if not ready:
            os.kill(forked[0], signal.SIGKILL)
        os.waitpid(forked[0], 0)
        return line

    child = run_in_child(work, timeout=30)
    assert child.output == "before=False own=True cut=True", child.output


# -- FR-004: a child never runs a sink's setter inside the setter call that forked it ------------


@pytest.mark.parametrize("path", ["orphan", "build", "retarget"])
def test_a_lock_holding_setter_that_forks_is_never_reentered_in_the_child(path: str) -> None:
    """AC1 to AC3: the child logs, flushes and shuts down, and its setter never runs again.

    The setter holds its own lock across a body that forks. In the child that lock stays held by
    the setter's frame, so a nested call hangs the child on it: in the fork handler (the worker
    paths' resumed hand, before the child's first line) or on the child's first log. At
    ``53b68203`` the ``build`` and ``retarget`` rows hung. The orphan row keeps the child's
    handing mark, which is what a reset in the handler would remove.
    """
    holder = Holder(locked=True)
    outputs: list[str] = []

    def child() -> str:
        report_from_child(ENTERED)
        log_foundry.info("child")
        log_foundry.flush(timeout=1)
        log_foundry.shutdown(timeout=1)
        return f"calls={holder.child_calls} handing={_lifecycle.handing_here(holder)}"

    def act(_offered: threading.Event) -> None:
        run = run_in_child(child, timeout=6)
        outputs.append(f"{run.output}|{run.ending}")

    holder.act = act
    _hand(path, holder)
    assert outputs == ["entered;calls=0 handing=True|exited 0"], outputs
    assert holder.timed_out == 0, "the parent's setter was re-entered"


def test_a_child_forked_outside_any_offer_is_handed_its_own_signal_by_the_resumed_worker() -> None:
    """AC4: the resumed worker skips only a sink being offered, so this one is handed its own.

    The fork roster gives the child a fresh ``_stop``, so a sink still holding the parent's event
    fails this, which is what a skip that never offers would leave.
    """
    holder = Holder()
    _hand("build", holder)
    log_foundry.flush()
    child = run_in_child(
        lambda: (
            f"own={holder.log_foundry_stop_signal is _lifecycle._state._stop} "
            f"calls={holder.child_calls}"
        )
    )
    assert child.output == "own=True calls=1", child.output


def test_a_setter_a_retarget_runs_that_logs_is_not_handed_again() -> None:
    """In the test process: a setter ``Worker.retarget`` runs that logs is not nested inside itself.

    ``_hand_retargeted`` offers with no ``handing`` mark, so before SPEC-083 the log inside the
    setter found ``handing_here`` false and handed the sink again from inside its own setter: two
    calls, depth two, and a setter holding its own lock deadlocked (measured at ``53b68203``).
    The offer record makes the log skip it, and a later ``flush()`` leaves the sink on the
    process's current signal.
    """
    holder = Holder(locked=True)
    holder.act = lambda _offered: log_foundry.info("from the setter")
    _hand("retarget", holder)
    during = f"calls={holder.calls} depth={holder.max_depth} timed_out={holder.timed_out}"
    log_foundry.flush(timeout=GATE)
    assert during == "calls=1 depth=1 timed_out=0", during
    assert holder.log_foundry_stop_signal is _lifecycle._state._stop
