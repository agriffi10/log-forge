"""SPEC-083: a child forked inside a stop-signal setter is cut short by its own ``shutdown()``.

A sink's ``log_foundry_stop_signal`` setter is the sink's own code, and it can fork, for example by
starting a fork-context ``multiprocessing.Process``. A child that leaves through ``os._exit`` is
still inside that setter call for its whole life. The library records each offer on the offering
thread (FR-001). Since SPEC-098 every sink holds one event of its own, which a child keeps,
re-initialised in place by the seed before its repair walk (FR-002), and sets at its ``shutdown()``
(FR-003); the seed also marks handed a sink the forking thread was being offered, so no thread of
the child runs that setter again (FR-004). SPEC-098 deleted the per-thread stale-signal list and
the offer record the child shared between its threads, and the tests of those two mechanisms.

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

    It counts the calls made in a process other than the one that built it, and records every
    event it was offered in ``offered``. With ``locked``, it
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
        self.offered: list[threading.Event] = []
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
            self.offered.append(event)
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


def _deferred_now() -> int:
    """How many stops are deferred to the registered sink change now (SPEC-097 FR-002)."""
    taker = _lifecycle._state._deferred_taker
    return 0 if taker is None else len(taker.deferred)


@log_foundry.trace
def _traced() -> None:
    """A traced call, whose span close builds the worker."""


def _hand(path: str, holder: Holder) -> None:
    """Configures ``holder`` and makes the library hand it a signal on one hand path.

    Each path's first hand to the holder is the one named, and ``configure()`` hands it nothing:
    ``orphan`` and ``filtering`` through ``offer_once`` from an orphan log, ``build`` from the
    worker build, and ``retarget`` from ``Worker.retarget``.
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
    """AC1: inside the setter the record ends with this offer, and another thread's is empty.

    The event recorded is the one the setter is given, the sink's own (SPEC-098), not the one the
    caller passed.
    """
    holder = Holder()
    seen: dict[str, object] = {}

    def act(offered: threading.Event) -> None:
        seen["last"] = _lifecycle._offers_here()[-1] == (id(holder), offered)
        other: list[list[tuple[int, threading.Event]]] = []
        thread = threading.Thread(target=lambda: other.append(list(_lifecycle._offers_here())))
        thread.start()
        thread.join(GATE)
        seen["other"] = other

    holder.act = act
    _lifecycle.offer_stop_signal(holder, threading.Event())
    assert seen == {"last": True, "other": [[]]}, seen


def test_a_nested_offer_sees_the_outer_entry_first() -> None:
    """AC2: the factory wrapper's setter offers its built sink, nested inside its own offer.

    Each entry carries that sink's own event (SPEC-098): the wrapper's, then the built sink's.
    """
    inner = Snapshotting()
    wrapper = _FactorySink(MemorySink)
    vars(wrapper)["_inner"] = inner
    _lifecycle.offer_stop_signal(wrapper, threading.Event())
    own = _lifecycle._state._signals
    outer, mine = own[id(wrapper)][1], own[id(inner)][1]
    assert outer is not mine
    assert inner.seen == [(mine, [(id(wrapper), outer), (id(inner), mine)])], inner.seen


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
    """AC1: the child holds the offered event itself, by identity, and marks the sink handed.

    SPEC-098: the event is the sink's own, kept in place by the child's seed, and the seed marks
    the sink handed because the forking thread is inside its offer, so no later delivery in the
    child runs the setter. In the parent the setter has not returned, so the sink is not handed.
    """
    holder = Holder()
    outputs: list[str] = []

    def child() -> str:
        entry = _lifecycle._state._signals[id(holder)]
        return f"same={entry[1] is holder.offered[0]} handed={entry[2]}"

    def act(_offered: threading.Event) -> None:
        handed = _lifecycle._state._signals[id(holder)][2]
        outputs.append(f"{run_in_child(child).output} parent_handed={handed}")

    holder.act = act
    _lifecycle.offer_stop_signal(holder, threading.Event())
    assert outputs == ["same=True handed=True parent_handed=False"], outputs


def test_a_child_sets_an_offered_event_whose_condition_a_parent_thread_held() -> None:
    """AC5: the event is re-initialised at the fork, so the child's shutdown sets it and returns.

    SPEC-098 FR-003 AC-4, rewritten rather than deleted. A parent thread holds the sink's own
    event's condition across the fork. The child's seed re-initialises that event in place, so
    the child's ``shutdown()`` sets it and returns, and the sink still holds that same object.
    Without the re-initialisation the child's ``shutdown()`` waits forever on that condition, and
    the child reports only how far it got before the parent's bound kills it. The reviewer's
    ``heldcond.py`` read ``returned in 0.00s set=True same=True`` on the prototype.
    """
    holder = Holder()
    outputs: list[str] = []

    def child() -> str:
        report_from_child(ENTERED)
        event = holder.offered[0]
        kept = _lifecycle._state._signals[id(holder)][1] is event
        report_from_child(f"kept={kept};")
        started = time.monotonic()
        log_foundry.shutdown(timeout=1)
        quick = time.monotonic() - started < 1.0
        same = holder.log_foundry_stop_signal is event
        return f"returned quick={quick} set={event.is_set()} same={same}"

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
    _lifecycle.offer_stop_signal(holder, threading.Event())
    assert outputs == ["entered;kept=True;returned quick=True set=True same=True|exited 0"], outputs


# -- FR-003: the child's shutdown sets the signals it was being offered -------------------------


@pytest.mark.parametrize("path", ["orphan", "filtering", "build", "retarget"])
def test_a_childs_shutdown_cuts_short_a_wait_on_the_offered_event(path: str) -> None:
    """AC1, AC2, AC6, AC7: a wait in the child on the event the sink holds ends at its shutdown.

    The setter stores the event and forks. In the child a thread waits ``BACKOFF`` seconds on the
    event the sink holds, the sink's own, which is not the child's worker signal, so only the set
    of every sink's event at the child's stop can end it. The wait must end because the event was
    set, after ``shutdown()`` began and within ``CUT`` of its return; a second ``shutdown()``
    returns too. The parent's event stays unset. At ``53b68203`` the orphan rows waited the full
    5 s. The ``build`` and ``retarget`` rows read ``own=True`` there, because the resumed worker's
    hand re-ran the setter in the fork handler; since FR-004 no hand reaches that sink in the
    child, and since SPEC-098 the child's stop sets it as it sets every event the library made.
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
    """In a child that unwound out of the setter: the held event, the setter's calls and the cut."""
    held = holder.log_foundry_stop_signal
    assert held is not None
    own = _lifecycle._state._signals[id(holder)][1] is held
    finished: list[tuple[bool, float]] = []
    waiter = threading.Thread(
        target=lambda: finished.append((held.wait(BACKOFF), time.monotonic())), daemon=True
    )
    waiter.start()
    log_foundry.flush(timeout=1)
    kept = holder.log_foundry_stop_signal is held
    started = time.monotonic()
    log_foundry.shutdown(timeout=1)
    returned = time.monotonic()
    waiter.join(BACKOFF + 1)
    ok, at = finished[0] if finished else (None, 0.0)
    cut = ok is True and started <= at < returned + CUT
    return f"own={own} kept={kept} calls={holder.child_calls} cut={cut}"


@pytest.mark.parametrize("payer", ["none", "another-thread"])
def test_a_child_that_unwinds_out_of_a_retarget_hand_is_cut_short_and_then_handed_its_own(
    payer: str,
) -> None:
    """AC5: a bare ``os.fork()`` in the setter ``Worker.retarget`` runs, whose child returns.

    The child unwinds out of the setter, so its own ``finally`` blocks pop the offer record. The
    sink holds its own event, the object the parent made, which the child's seed kept in place and
    marked handed (SPEC-098), so neither the resumed worker, the child's ``flush()`` nor another
    thread's ``flush()`` made while the forking thread is still inside the setter runs the setter
    again, and the child's ``shutdown()`` sets that event, ending a wait on it. Before SPEC-098
    the sink kept the parent's copy of the worker's event and the child's ``flush()`` paid the
    fork's debt by handing it the child's; at ``471eef5e`` nothing did after another thread paid
    the debt first.
    """

    def work() -> str:
        read_fd, write_fd = os.pipe()
        parent = os.getpid()
        holder = Holder()
        forked: list[int] = []
        paid: list[str] = []

        def fork(_offered: threading.Event) -> None:
            gc.collect()
            pid = os.fork()
            if pid == 0:
                signal.alarm(int(GATE * 3))
                if payer == "another-thread":
                    thread = threading.Thread(target=lambda: log_foundry.flush(timeout=1))
                    thread.start()
                    thread.join(GATE)
                    paid.append(f"alive={thread.is_alive()} ")
            forked.append(pid)

        holder.act = fork
        _hand("retarget", holder)
        if os.getpid() != parent:
            try:
                line = "".join(paid) + _unwound(holder)
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
    paid = "alive=False " if payer == "another-thread" else ""
    assert child.output == f"{paid}own=True kept=True calls=0 cut=True", child.output


# -- FR-004: a child never runs a sink's setter inside the setter call that forked it ------------


@pytest.mark.parametrize("first", ["main", "thread"])
@pytest.mark.parametrize("path", ["orphan", "build", "retarget"])
def test_a_lock_holding_setter_that_forks_is_never_reentered_in_the_child(
    path: str, first: str
) -> None:
    """AC1 to AC3: the child logs, flushes and shuts down, and its setter never runs again.

    The setter holds its own lock across a body that forks. In the child that lock stays held by
    the setter's frame, so a nested call hangs the child on it: in the fork handler (the worker
    paths' resumed hand, before the child's first line) or on the child's first log. At
    ``53b68203`` the ``build`` and ``retarget`` rows hung.

    With ``first=thread`` the child's first log is made by **another** thread of the child, which
    holds no offer record of the forking thread's. At ``d3950848`` the orphan row's thread hung on
    the setter's lock, handing the sink the child's debt. Since SPEC-098 the child's seed marks
    the sink handed, because the forking thread is inside its offer (SPEC-098 plan, D-4), so no
    thread of the child offers it: those rows are what goes red without that mark. The child
    reports whether the sink's entry reads handed after its calls.
    """
    holder = Holder(locked=True)
    outputs: list[str] = []

    def child() -> str:
        report_from_child(ENTERED)
        alive = False
        if first == "thread":
            thread = threading.Thread(target=lambda: log_foundry.info("from a thread"), daemon=True)
            thread.start()
            thread.join(GATE)
            alive = thread.is_alive()
        report_from_child(f"alive={alive};")
        log_foundry.info("child")
        log_foundry.flush(timeout=1)
        log_foundry.shutdown(timeout=1)
        return f"calls={holder.child_calls} handed={_lifecycle._state._signals[id(holder)][2]}"

    def act(_offered: threading.Event) -> None:
        run = run_in_child(child, timeout=12)
        outputs.append(f"{run.output}|{run.ending}")

    holder.act = act
    _hand(path, holder)
    assert outputs == ["entered;alive=False;calls=0 handed=True|exited 0"], outputs
    assert holder.timed_out == 0, "the parent's setter was re-entered"


def test_a_child_forked_outside_any_offer_is_handed_its_own_signal_by_the_resumed_worker() -> None:
    """AC4, since SPEC-098: the resumed worker runs no setter, and the sink keeps its own event.

    The sink was handed in the parent, so its entry is inherited handed and the child's resume
    assigns nothing (SPEC-098 FR-003): the setter runs 0 times in the child, the sink holds the
    very object the parent made, re-initialised, and the child's ``shutdown()`` sets it. Before
    SPEC-098 the resume handed the sink the child's fresh worker event and the setter ran once.
    """
    holder = Holder()
    _hand("build", holder)
    log_foundry.flush()
    event = holder.offered[0]

    def child() -> str:
        same = holder.log_foundry_stop_signal is event
        log_foundry.shutdown(timeout=1)
        return f"same={same} calls={holder.child_calls} set={event.is_set()}"

    child_run = run_in_child(child)
    assert child_run.output == "same=True calls=0 set=True", child_run.output
    assert not event.is_set(), "the child's shutdown set the parent's event"


def test_a_setter_a_retarget_runs_that_logs_is_not_handed_again() -> None:
    """In the test process: a setter ``Worker.retarget`` runs that logs is not nested inside itself.

    The retarget's offer had no ``handing`` mark, so before SPEC-083 the log inside the setter
    handed the sink again from inside its own setter: two calls, depth two, and a setter holding
    its own lock deadlocked (measured at ``53b68203``). The offer record makes the log skip it,
    and a later ``flush()`` leaves the sink on its own event (SPEC-098), assigning nothing.
    """
    holder = Holder(locked=True)
    holder.act = lambda _offered: log_foundry.info("from the setter")
    _hand("retarget", holder)
    during = f"calls={holder.calls} depth={holder.max_depth} timed_out={holder.timed_out}"
    log_foundry.flush(timeout=GATE)
    assert during == "calls=1 depth=1 timed_out=0", during
    assert holder.calls == 1
    assert holder.log_foundry_stop_signal is _lifecycle._state._signals[id(holder)][1]


class Waiting(Holder):
    """A holder whose ``emit`` in a child backs off ``BACKOFF`` seconds on the event it holds."""

    def __init__(self, *, locked: bool = False) -> None:
        super().__init__(locked=locked)
        self.emits = 0
        self.waits: list[bool] = []

    def emit(self, batch: list[dict[str, object]]) -> None:
        if os.getpid() == self.home:
            return
        self.emits += 1
        held = self._signal
        self.waits.append(held.wait(BACKOFF) if held is not None else False)


def test_a_resumed_drain_through_a_factory_wrapper_never_reenters_the_setter() -> None:
    """The child's drain thread reaches the built sink through the wrapper and does not hand it.

    The parent's drain thread builds the sink through ``sink_factory=`` and forks inside its
    setter, before the wrapper records what it built. In the child the resumed worker's drain
    thread, another thread, meets the wrapper with nothing recorded, finds the sink pending, and
    must see it as being offered rather than hand it again. At ``d3950848`` it handed it, blocked
    on the setter's own lock, delivered nothing, and the child's ``shutdown()`` waited out its
    whole timeout. The delivered batch's backoff ends at the child's shutdown, through the kept
    event.
    """
    holder = Waiting(locked=True)
    outputs: list[str] = []

    def child() -> str:
        _traced()
        deadline = time.monotonic() + GATE
        while holder.emits == 0 and time.monotonic() < deadline:
            time.sleep(0.01)
        started = time.monotonic()
        log_foundry.shutdown(timeout=1)
        took = time.monotonic() - started
        return (
            f"emits={holder.emits} calls={holder.child_calls} cut={holder.waits == [True]} "
            f"fast={took < 0.5}"
        )

    def act(_offered: threading.Event) -> None:
        run = run_in_child(child, timeout=12)
        outputs.append(f"{run.output}|{run.ending}")

    holder.act = act
    log_foundry.configure(service="t", sink_factory=lambda: holder)
    _traced()
    log_foundry.flush(timeout=GATE * 4)
    assert outputs == ["emits=1 calls=0 cut=True fast=True|exited 0"], outputs


# -- SPEC-097 FR-006: a forked child inherits no command ----------------------------------------


def test_a_child_forked_from_a_swaps_closer_inherits_no_sink_change_and_shuts_down() -> None:
    """FR-006: a fork from the displaced sink's ``close()`` on the swap's closer thread.

    The fork lands while the sink change still holds ``config._sink_lock``: the swap joins its
    closer before it returns. ``config._sink_holder`` names the parent's changing thread, and a
    ``shutdown()`` from a ``log-foundry-`` thread reads a holder as a change in flight and is
    deferred to it, so with the holder inherited the child's ``shutdown()`` was deferred to a
    change whose other half is not in the child, and the child ended ``RUNNING`` with its span
    undelivered. The closer thread itself calls ``shutdown()`` just before it forks, which the
    parent defers to the change, so the parent has one deferred stop at the fork. In the child the
    holder is ``None``, no stop is deferred, and a traced call then ``shutdown()`` leave it
    ``CLOSED`` with the span delivered.
    """

    def work() -> str:
        read_fd, write_fd = os.pipe()
        parent = os.getpid()
        forked: list[int] = []

        class ForkOnClose:
            def __init__(self) -> None:
                self.messages: list[object] = []

            def emit(self, batch: list[dict[str, object]]) -> None:
                self.messages.extend(event.get("message") for event in batch)

            def close(self) -> None:
                if forked or threading.current_thread().name != "log-foundry-sink-close":
                    return
                holder = config._sink_holder
                log_foundry.shutdown(timeout=1.0)
                before = _deferred_now()
                gc.collect()
                pid = os.fork()
                forked.append(pid)
                if pid != 0:
                    return
                signal.alarm(int(GATE * 3))
                try:
                    line = (
                        f"parent_held={holder is not None} parent_deferred={before} "
                        f"holder={config._sink_holder} "
                        f"deferred={_deferred_now()} "
                    )

                    @log_foundry.trace
                    def child_span() -> None:
                        log_foundry.info("in the child")

                    child_span()
                    log_foundry.shutdown(timeout=GATE)
                    line += (
                        f"state={_lifecycle._state.state.name} "
                        f"delivered={'in the child' in installed.messages}"
                    )
                except BaseException as exc:
                    line = f"!{type(exc).__name__}: {exc}"
                os.write(write_fd, line.encode())
                os._exit(0)

        from log_foundry import config

        displaced, installed = ForkOnClose(), ForkOnClose()
        log_foundry.configure(service="fork-from-closer", sink=displaced)  # type: ignore[arg-type]

        @log_foundry.trace
        def span() -> None:
            log_foundry.info("x")

        span()
        log_foundry.flush(timeout=GATE)
        log_foundry.configure(sink=installed)  # type: ignore[arg-type]
        if os.getpid() != parent:
            os._exit(1)
        os.close(write_fd)
        if not forked:
            return "never forked"
        ready = select.select([read_fd], [], [], GATE * 3)[0]
        line = os.read(read_fd, 512).decode() if ready else "silent"
        if not ready:
            os.kill(forked[0], signal.SIGKILL)
        os.waitpid(forked[0], 0)
        return line

    child = run_in_child(work, timeout=30)
    assert child.output == (
        "parent_held=True parent_deferred=1 holder=None deferred=0 state=CLOSED delivered=True"
    ), child.output


def test_a_child_forked_on_the_changing_thread_drops_its_published_change() -> None:
    """FR-006: a fork on the changing thread, after the change is published, leaves no change.

    The fork is made from the change's config write (``config._rebind``), which runs after the
    change has published its operation and while it holds ``config._sink_lock``, and after a
    ``log-foundry-`` thread's ``shutdown()`` has been deferred to it. So at the fork the forking
    thread has a published change, the process has a deferred stop, and the holder is this thread.
    In the child all three are gone, and a traced call then ``shutdown()`` leave it ``CLOSED``
    with the span delivered to the sink the child's config names.
    """

    def work() -> str:
        read_fd, write_fd = os.pipe()
        parent = os.getpid()
        forked: list[int] = []

        class Recording:
            def __init__(self) -> None:
                self.messages: list[object] = []

            def emit(self, batch: list[dict[str, object]]) -> None:
                self.messages.extend(event.get("message") for event in batch)

            def close(self) -> None:
                return None

        from log_foundry import config

        old, new = Recording(), Recording()
        log_foundry.configure(service="fork-from-rebind", sink=old)  # type: ignore[arg-type]
        log_foundry.info("owed")
        real_rebind = config._rebind

        def rebind_then_fork(**changed: object) -> None:
            real_rebind(**changed)
            if forked or changed.get("sink") is not new:
                return
            deferring = threading.Thread(
                target=lambda: log_foundry.shutdown(timeout=1.0), name="log-foundry-test"
            )
            deferring.start()
            deferring.join(GATE)
            premise = (
                f"parent_published={_lifecycle._state._calls_here.published is not None} "
                f"parent_deferred={_deferred_now()} "
                f"parent_holder={config._sink_holder is not None} "
            )
            gc.collect()
            pid = os.fork()
            forked.append(pid)
            if pid != 0:
                return
            signal.alarm(int(GATE * 3))
            try:
                line = premise + (
                    f"published={_lifecycle._state._calls_here.published} "
                    f"deferred={_deferred_now()} "
                    f"holder={config._sink_holder} "
                )

                @log_foundry.trace
                def child_span() -> None:
                    log_foundry.info("in the child")

                child_span()
                log_foundry.shutdown(timeout=GATE)
                line += (
                    f"state={_lifecycle._state.state.name} "
                    f"delivered={'in the child' in new.messages}"
                )
            except BaseException as exc:
                line = f"!{type(exc).__name__}: {exc}"
            os.write(write_fd, line.encode())
            os._exit(0)

        config._rebind = rebind_then_fork  # type: ignore[assignment]
        try:
            log_foundry.configure(sink=new)  # type: ignore[arg-type]
        finally:
            config._rebind = real_rebind  # type: ignore[assignment]
        if os.getpid() != parent:
            os._exit(1)
        os.close(write_fd)
        if not forked:
            return "never forked"
        ready = select.select([read_fd], [], [], GATE * 3)[0]
        line = os.read(read_fd, 512).decode() if ready else "silent"
        if not ready:
            os.kill(forked[0], signal.SIGKILL)
        os.waitpid(forked[0], 0)
        return line

    child = run_in_child(work, timeout=30)
    assert child.output == (
        "parent_published=True parent_deferred=1 parent_holder=True "
        "published=None deferred=0 holder=None state=CLOSED delivered=True"
    ), child.output
