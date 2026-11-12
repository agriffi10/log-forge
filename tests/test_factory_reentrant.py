"""A ``sink_factory`` that calls back into the library never waits on a build it is running.

``_FactorySink`` called the factory while holding its build lock. A factory that ran a ``@trace``d
function built a worker whose drain thread emitted to the wrapper and blocked on that lock; the
factory then called ``flush()``, ``shutdown()`` or ``configure(sink=...)``, each of which waits on
that drain thread. At ``9f4d4dfd``, with the factory on the caller's thread, ``flush(None)`` and
``shutdown(None)`` hung, ``configure(sink=B)`` stalled for its 5 s swap budget and ``flush(2)``
and ``shutdown(2)`` for their 2 s. Now the factory runs under no lock, and for the length of
those three calls a thread waiting on the build refuses its emit instead of waiting.

Every guard against a hang runs in a forked child with a bounded wait, as in
``test_reentrant_stop_signal.py``; the child writes :data:`ENTERED` just before the call that hung,
so a hang reads back as that marker alone.

Serves invariants 2, 3 and 5.
"""

from __future__ import annotations

import os
import threading
import time
from typing import TYPE_CHECKING

import pytest

import log_foundry
from conftest import cpu_window
from log_foundry.sinks._factory import _FactorySink
from log_foundry.sinks.base import SinkDeliveryError
from test_fork_lifecycle import report_from_child, run_in_child

if TYPE_CHECKING:
    from collections.abc import Callable

ENTERED = "entered;"
"""What a child reports just before the call that hung at ``9f4d4dfd``."""

GATE = 5.0
"""Seconds a parked factory or a racing thread is given, inside a child, before giving up."""

FAST = 1.0
"""The budget a call that stalled at ``9f4d4dfd`` must now return within.

Half the shortest stall measured there (``flush(timeout=2)``, 2.00 s) and well above what the
fixed call takes (the drain's refused retries end in about 0.07 s), so a slow machine does not
flake it and the stall cannot pass it.
"""

YIELDED = "called flush(), shutdown() or configure() while this emit would have waited on it"
"""The fixed part of the refusal an emit meets while the factory is inside one of those calls."""


class Plain:
    """A sink that records its events and counts its closes."""

    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []
        self.closes = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        self.events.extend(batch)

    def close(self) -> None:
        self.closes += 1


def _traced() -> Callable[[], None]:
    @log_foundry.trace
    def work() -> None:
        pass

    return work


def _blocked(thread: threading.Thread) -> bool:
    """Whether *thread* is still running a moment after it started: it is waiting on something."""
    thread.join(0.2)
    return thread.is_alive()


_CALLS: dict[str, Callable[[Plain], object]] = {
    "flush-none": lambda b: log_foundry.flush(timeout=None),
    "shutdown-none": lambda b: log_foundry.shutdown(timeout=None),
    "configure": lambda b: log_foundry.configure(sink=b),
    "flush-2": lambda b: log_foundry.flush(timeout=2.0),
    "shutdown-2": lambda b: log_foundry.shutdown(timeout=2.0),
}


@pytest.mark.parametrize("call", sorted(_CALLS))
def test_a_factory_that_traces_then_waits_on_the_drain_returns(call: str) -> None:
    """The factory runs a ``@trace``d function, then a call that waits on the drain thread.

    The drain thread meets the build in progress and, because the factory is inside that call,
    refuses rather than waiting, so the call returns and the build lands. At ``9f4d4dfd`` the
    ``None`` rows hung and the others stalled for their whole budget. The drain thread's refusal
    is asserted by its reason: the span batch is abandoned after its emit attempts, and nothing
    else reports a lost batch.
    """

    def work() -> str:
        a, b = Plain(), Plain()
        traced = _traced()
        calls = 0

        def factory() -> Plain:
            nonlocal calls
            calls += 1
            if calls == 1:
                traced()
                _CALLS[call](b)
            return a

        log_foundry.configure(service="t", sink_factory=factory)
        report_from_child(ENTERED)
        started = time.monotonic()
        log_foundry.info("x")
        elapsed = time.monotonic() - started
        failed = log_foundry.health().failed_batches
        return f"fast={elapsed < FAST} calls={calls} failed={failed}"

    child = run_in_child(work)
    assert child.output != ENTERED, f"{call}: hung inside the factory's call into the library"
    assert child.output.startswith(ENTERED), child.output
    report = child.output[len(ENTERED) :]
    assert report == "fast=True calls=1 failed=1", report


def test_a_drain_meeting_a_slow_build_waits_for_it_and_delivers() -> None:
    """With no call back, a drain thread that meets a build in progress waits and delivers.

    The refusal is only for the length of a call that can wait on the drain thread. A factory
    that just takes its time must not cost the span batch that met it. The factory holds until
    the drain thread is inside the wrapper's ``emit``, then 0.2 s more, longer than the worker's
    whole backoff of about 0.07 s, so a drain that refused instead of waiting would abandon the
    batch.
    """

    def work() -> str:
        a = Plain()
        traced = _traced()
        entered, drain_in = threading.Event(), threading.Event()

        def factory() -> Plain:
            entered.set()
            drain_in.wait(GATE)
            time.sleep(0.2)
            return a

        log_foundry.configure(service="t", sink_factory=factory)
        wrapper = log_foundry.get_config().sink
        assert isinstance(wrapper, _FactorySink)

        def emit(batch: list[dict[str, object]]) -> None:
            if threading.current_thread().name.startswith("log-foundry-"):
                drain_in.set()
            _FactorySink.emit(wrapper, batch)

        wrapper.emit = emit  # type: ignore[method-assign]
        first = threading.Thread(target=lambda: log_foundry.info("first"), daemon=True)
        first.start()
        if not entered.wait(GATE):
            return "the factory never ran"
        traced()
        delivered = log_foundry.flush(timeout=GATE)
        first.join(GATE)
        failed = log_foundry.health().failed_batches
        return (
            f"drain_met_build={drain_in.is_set()} delivered={bool(delivered)} failed={failed} "
            f"events={len(a.events)}"
        )

    child = run_in_child(work)
    assert child.output == "drain_met_build=True delivered=True failed=0 events=3", child.output


def test_a_yield_refuses_a_parked_waiter_and_ends_with_its_call() -> None:
    """A thread already waiting is refused when the factory calls in; one arriving after waits.

    The first waiter parks on the build. The factory calls ``flush(timeout=0)``: the waiter wakes
    and is refused with the yielded reason, not left parked. After that call returns, a second
    emit arrives while the factory still runs; it waits, idly, and is delivered to what was
    built, so the refusal lasts only as long as the call that yielded, and the wake event the
    yield set does not leave the late waiter spinning (invariant 3).
    """

    def work() -> str:
        a = Plain()
        wrapper = _FactorySink(lambda: a)
        parked, resume, called = threading.Event(), threading.Event(), threading.Event()
        outcomes: dict[str, str] = {}

        def factory() -> Plain:
            parked.wait(GATE)
            log_foundry.flush(timeout=0)
            called.set()
            resume.wait(GATE)
            return a

        wrapper._factory = factory

        cpu: dict[str, float] = {}

        def emit(name: str) -> None:
            with cpu_window() as window:
                try:
                    wrapper.emit([{"n": name}])
                    outcomes[name] = "delivered"
                except SinkDeliveryError as exc:
                    outcomes[name] = "yielded" if YIELDED in str(exc) else f"refused: {exc}"
            cpu[name] = window.spent

        builder = threading.Thread(target=emit, args=("builder",), daemon=True)
        builder.start()
        while wrapper._claim is None and builder.is_alive():
            time.sleep(0.001)
        waiter = threading.Thread(target=emit, args=("waiter",), daemon=True)
        waiter.start()
        was_parked = _blocked(waiter)
        parked.set()
        waiter.join(GATE)
        if not called.wait(GATE):
            return "the factory never called flush()"
        late = threading.Thread(target=emit, args=("late",), daemon=True)
        late.start()
        late_waited = _blocked(late)
        resume.set()
        builder.join(GATE)
        late.join(GATE)
        return (
            f"parked={was_parked} late_waited={late_waited} "
            f"late_idle={cpu.get('late', 1.0) < 0.05} "
            + " ".join(f"{k}={v}" for k, v in sorted(outcomes.items()))
            + f" events={[e['n'] for e in a.events]}"
        )

    child = run_in_child(work)
    head = "parked=True late_waited=True late_idle=True builder=delivered late=delivered"
    assert child.output in (
        f"{head} waiter=yielded events=['builder', 'late']",
        f"{head} waiter=yielded events=['late', 'builder']",
    ), child.output


def test_a_claim_another_process_made_is_claimed_over() -> None:
    """A wrapper inherited mid-build carries the parent's claim; this process builds anyway.

    Nothing in this process will ever settle the parent's claim, so waiting on it would hang.
    """

    from log_foundry.sinks._factory import _Build

    def work() -> str:
        a = Plain()
        wrapper = _FactorySink(lambda: a)
        wrapper._claim = _Build((os.getpid() + 1, threading.get_ident()))
        wrapper.emit([{"n": 1}])
        return f"events={len(a.events)} claim={wrapper._claim}"

    child = run_in_child(work)
    assert child.output == "events=1 claim=None", child.output


@pytest.mark.parametrize("shared", [False, True], ids=["own", "shared"])
def test_a_builder_whose_claim_was_taken_over_releases_what_it_built(shared: bool) -> None:
    """A build whose claim another thread took over closes its own result and uses the winner's.

    Only a forked child resuming its factory reaches it: the child's thread claims over the
    parent's claim, builds B and delivers to it, and when the first factory call then returns A,
    A — recorded by nobody and written by nobody — is released once, and that emit goes to B.
    Simulated here by giving the parked build's claim another process's pid. A factory returning
    one shared sink returns the recorded sink itself the second time, and that is never released:
    it is the live sink.
    """

    def work() -> str:
        a, b = Plain(), Plain()
        entered, go = threading.Event(), threading.Event()
        calls = 0

        def factory() -> Plain:
            nonlocal calls
            calls += 1
            if calls == 1:
                entered.set()
                go.wait(GATE)
                return b if shared else a
            return b

        wrapper = _FactorySink(factory)
        first = threading.Thread(target=lambda: wrapper.emit([{"n": "first"}]), daemon=True)
        first.start()
        if not entered.wait(GATE):
            return "the factory never ran"
        claim = wrapper._claim
        assert claim is not None
        claim.owner = (os.getpid() + 1, claim.owner[1])
        wrapper.emit([{"n": "main"}])
        go.set()
        first.join(GATE)
        return (
            f"calls={calls} a={len(a.events)}/{a.closes} "
            f"b={sorted(str(e['n']) for e in b.events)}/{b.closes}"
        )

    child = run_in_child(work)
    released = "0" if shared else "1"
    assert child.output == f"calls=2 a=0/{released} b=['first', 'main']/0", child.output


def test_a_factory_raising_base_exception_leaves_no_claim() -> None:
    """A ``BaseException`` out of the factory clears the claim, so the next emit builds.

    A claim left behind would refuse every later emit on this thread as re-entrant, and spin a
    waiter on another thread against a wake event already set.
    """

    def work() -> str:
        a = Plain()
        calls = 0

        def factory() -> Plain:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise KeyboardInterrupt
            return a

        wrapper = _FactorySink(factory)
        try:
            wrapper.emit([{"n": 1}])
        except KeyboardInterrupt:
            pass
        wrapper.emit([{"n": 2}])
        return f"calls={calls} events={[e['n'] for e in a.events]} claim={wrapper._claim}"

    child = run_in_child(work)
    assert child.output == "calls=2 events=[2] claim=None", child.output


def test_a_failed_build_wakes_its_waiter_which_calls_the_factory_again() -> None:
    """A waiter on a build that fails claims the next one and calls the factory itself.

    That is what a thread waiting on the build lock did before: the next emit retries.
    """

    def work() -> str:
        a = Plain()
        calls = 0
        entered, fail = threading.Event(), threading.Event()

        def factory() -> Plain:
            nonlocal calls
            calls += 1
            if calls == 1:
                entered.set()
                fail.wait(GATE)
                raise ValueError("first build fails")
            return a

        wrapper = _FactorySink(factory)
        outcomes: dict[str, str] = {}

        def emit(name: str) -> None:
            try:
                wrapper.emit([{"n": name}])
                outcomes[name] = "delivered"
            except SinkDeliveryError:
                outcomes[name] = "failed"

        first = threading.Thread(target=emit, args=("first",), daemon=True)
        first.start()
        if not entered.wait(GATE):
            return "the factory never ran"
        waiter = threading.Thread(target=emit, args=("waiter",), daemon=True)
        waiter.start()
        was_parked = _blocked(waiter)
        fail.set()
        first.join(GATE)
        waiter.join(GATE)
        return f"parked={was_parked} calls={calls} " + " ".join(
            f"{k}={v}" for k, v in sorted(outcomes.items())
        )

    child = run_in_child(work)
    assert child.output == "parked=True calls=2 first=failed waiter=delivered", child.output


@pytest.mark.parametrize("path", ["orphan", "worker"])
def test_a_shutdown_landing_while_the_factory_runs_closes_the_built_sink_once(path: str) -> None:
    """A ``shutdown()`` on another thread during the factory call leaves A closed exactly once.

    The shutdown closes the wrapper while nothing is built; the build then writes to A and
    re-arms the wrapper, so the next ``shutdown()`` (standing in for the exit) closes A once.
    It passed at ``9f4d4dfd`` too; it pins the close path this change leaves alone.
    """

    def work() -> str:
        a = Plain()
        traced = _traced()
        entered, go = threading.Event(), threading.Event()

        def factory() -> Plain:
            entered.set()
            go.wait(GATE)
            return a

        log_foundry.configure(service="t", sink_factory=factory)
        first: threading.Thread | None = None
        if path == "orphan":
            first = threading.Thread(target=lambda: log_foundry.info("x"), daemon=True)
            first.start()
        else:
            traced()
        if not entered.wait(GATE):
            return "the factory never ran"
        log_foundry.shutdown(timeout=0.3)
        go.set()
        if first is not None:
            first.join(GATE)
        deadline = time.monotonic() + GATE
        while not a.events and time.monotonic() < deadline:
            time.sleep(0.01)
        before = a.closes
        log_foundry.shutdown(timeout=GATE)
        return f"written={bool(a.events)} before={before} after={a.closes}"

    child = run_in_child(work)
    assert child.output == "written=True before=0 after=1", child.output
