"""SPEC-097 FR-002: a stop deferred to a sink change's end keeps ``configure()`` inside its budget.

A ``shutdown()`` made from a sink's ``emit`` on the drain thread during ``configure(sink=...)``
is deferred: it records its deadline, returns at once, and the changing thread runs one stop for it
once the change has released ``config._sink_lock``, before ``configure()`` returns. A stop's
timeout bounds its drain and its waits, never a ``close()``, so the spec reviewer's
``fr002_bound.py`` measured a deferred stop that closed a slow sink inline taking ``configure()``
to 10.01 s and 15.02 s against a 5 s budget. Every close the deferred stop takes is now detached,
and it waits for them only within what is left of the change's budget.

Each row runs in a fresh interpreter, because the exit drain is ``atexit``'s: the displaced sink's
``close()`` blocks 10 s, the installed sink's, or both. The child registers its own ``atexit``
probe before the library registers its handler, so the probe runs after the exit drain (``atexit``
is last in, first out) and reads which closes were entered by then. Serves invariants 3 and 5.
"""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
import threading
import time
from typing import Any

import pytest

import log_foundry
from log_foundry import _lifecycle

_CHILD = textwrap.dedent(
    """
    import atexit, json, sys, threading, time

    ROW = sys.argv[1]
    report = {}

    def probe():
        report["entered_at_exit"] = {s.name: s.entered for s in (old, new)}
        sys.stdout.write(json.dumps(report) + "\\n")
        sys.stdout.flush()

    atexit.register(probe)

    import log_foundry
    from log_foundry import worker as worker_module

    class Sink:
        def __init__(self, name, slow):
            self.name, self.slow = name, slow
            self.entered = 0
            self.finished = 0
            self.shut_down = False
            self.changing = False

        def emit(self, batch):
            if self.changing and not self.shut_down:
                self.shut_down = True
                log_foundry.shutdown(timeout=None)

        def close(self):
            self.entered += 1
            if self.slow:
                time.sleep(10.0)
            self.finished += 1

    old = Sink("displaced", ROW in ("displaced", "both"))
    new = Sink("installed", ROW in ("installed", "both"))
    log_foundry.configure(service="bound", sink=old)

    @log_foundry.trace
    def work():
        log_foundry.info("x")

    work()
    log_foundry.flush(timeout=5.0)
    assert worker_module.DEFAULT_SWAP_TIMEOUT == 5.0
    work()
    old.changing = True
    wall, cpu = time.monotonic(), time.process_time()
    log_foundry.configure(sink=new)
    report["wall"] = time.monotonic() - wall
    report["cpu"] = time.process_time() - cpu
    report["closing_sinks"] = log_foundry.health().closing_sinks
    report["running"] = sum(1 for s in (old, new) if s.entered and not s.finished)
    report["shut_down"] = old.shut_down
    """
)


@pytest.mark.parametrize("row", ["displaced", "installed", "both"])
def test_a_deferred_stop_keeps_configure_within_its_budget_with_a_slow_close(row: str) -> None:
    """FR-002: ``configure()`` returns within 6.0 s and under 1 s of CPU, whichever close is slow.

    The sink's ``emit`` calls ``shutdown(timeout=None)`` during the change, on the drain thread.
    By the time the exit drain returns each sink's ``close()`` was entered once, and
    ``health().closing_sinks`` read just after ``configure()`` returns counts each close still
    running then, which the child reads from the sinks themselves.
    """
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", _CHILD, row],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    lines = [line for line in result.stdout.splitlines() if line.startswith("{")]
    assert result.returncode == 0 and lines, result.stdout[-2000:] + result.stderr[-2000:]
    report = json.loads(lines[-1])
    assert report["shut_down"], f"the premise: the emit's shutdown() ran: {report}"
    assert report["wall"] < 6.0, f"configure() took {report['wall']:.2f} s: {report}"
    assert report["cpu"] < 1.0, f"configure() spent {report['cpu']:.2f} s of CPU: {report}"
    assert report["entered_at_exit"] == {"displaced": 1, "installed": 1}, report
    assert report["closing_sinks"] == report["running"], report
    assert report["running"] >= 1, f"the premise: a slow close was still running: {report}"


# -- no deferred stop is left behind (frame-1 diff review, MAJOR 1) ---------------------------


class _Plain:
    def __init__(self) -> None:
        self.closes = 0
        self.events = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        self.events += len(batch)

    def close(self) -> None:
        self.closes += 1


class _StopsThenInterrupts(_Plain):
    """A sink whose stop-signal setter defers a ``shutdown()`` and then raises."""

    fired = False

    @property
    def log_foundry_stop_signal(self) -> threading.Event | None:
        return None

    @log_foundry_stop_signal.setter
    def log_foundry_stop_signal(self, value: threading.Event | None) -> None:
        if self.fired or threading.current_thread() is not threading.main_thread():
            return
        self.fired = True
        log_foundry.shutdown(timeout=1.0)
        raise KeyboardInterrupt("the setter is interrupted")


def test_a_change_that_raises_still_runs_the_stop_deferred_during_it() -> None:
    """MAJOR 1, the exceptional exit: the deferred stop runs as the change unwinds.

    A stop-signal setter defers ``shutdown(timeout=1)`` and then raises ``KeyboardInterrupt``,
    which ``configure()`` propagates. The deferred stop runs before the exception reaches the
    caller, and the exception is the setter's. Run only on a normal exit, it was left queued, and
    the next, unrelated ``configure(sink=C)`` retired the library at a timeout of zero.
    """
    log_foundry.configure(service="leak", sink=_Plain())  # type: ignore[arg-type]
    log_foundry.info("one")
    b = _StopsThenInterrupts()
    with pytest.raises(KeyboardInterrupt, match="setter is interrupted"):
        log_foundry.configure(sink=b)  # type: ignore[arg-type]
    assert b.fired, "the premise: the setter ran and deferred its shutdown()"
    assert _lifecycle._state._deferred_taker is None, "a deferred stop outlived its change"
    assert _lifecycle._state.retirements == 1, "the deferred stop did not run"
    c = _Plain()
    log_foundry.configure(sink=c)  # type: ignore[arg-type]
    assert _lifecycle._state.retirements == 1, "a later change ran a stale stop"


class _TakerCheckedList(list):  # type: ignore[type-arg]
    """A change's deferred list, recording an append made while it is not the running taker."""

    def __init__(self, op: _lifecycle._Operation, orphaned: list[float | None]) -> None:
        super().__init__()
        self.op, self.orphaned = op, orphaned

    def append(self, deadline: float | None) -> None:  # type: ignore[override]
        op = self.op
        if _lifecycle._state._deferred_taker is not op or not _lifecycle._on_stack(
            op.change, op.thread
        ):
            self.orphaned.append(deadline)
        super().append(deadline)


def _checked_operations(monkeypatch: pytest.MonkeyPatch) -> tuple[list[Any], list[Any]]:
    """Gives every operation built from here a checked deferred list; returns the ops, orphans."""
    built: list[_lifecycle._Operation] = []
    orphaned: list[float | None] = []
    real = _lifecycle._Operation.__init__

    def init(self: _lifecycle._Operation, kind: _lifecycle._Op, timeout: float | None) -> None:
        real(self, kind, timeout)
        self.deferred = _TakerCheckedList(self, orphaned)
        built.append(self)

    monkeypatch.setattr(_lifecycle._Operation, "__init__", init)
    return built, orphaned


class _SetterStops(_Plain):
    """A sink whose stop-signal setter calls ``shutdown(timeout=1)`` once, on any thread."""

    def __init__(self, then_raise: bool = False) -> None:
        super().__init__()
        self.fired = False
        self.then_raise = then_raise

    @property
    def log_foundry_stop_signal(self) -> threading.Event | None:
        return None

    @log_foundry_stop_signal.setter
    def log_foundry_stop_signal(self, value: threading.Event | None) -> None:
        if self.fired or value is None:
            return
        self.fired = True
        log_foundry.shutdown(timeout=1.0)
        if self.then_raise:
            raise KeyboardInterrupt("the setter is interrupted")


class _SetterStartsAStoppingThread(_SetterStops):
    """A setter that has another ``log-foundry-`` thread call ``shutdown()`` and waits for it."""

    @property
    def log_foundry_stop_signal(self) -> threading.Event | None:
        return None

    @log_foundry_stop_signal.setter
    def log_foundry_stop_signal(self, value: threading.Event | None) -> None:
        if self.fired or value is None:
            return
        self.fired = True
        other = threading.Thread(
            target=lambda: log_foundry.shutdown(timeout=1.0), name="log-foundry-other"
        )
        other.start()
        other.join(GATE)


class _ChangesOnEmit(_Plain):
    """A sink whose first ``emit`` changes the sink to ``target``: on the drain thread."""

    def __init__(self, target: object) -> None:
        super().__init__()
        self.target = target
        self.changed = False

    def emit(self, batch: list[dict[str, object]]) -> None:
        super().emit(batch)
        if not self.changed:
            self.changed = True
            log_foundry.configure(sink=self.target)  # type: ignore[arg-type]


class _ChangesOnClose(_Plain):
    """A sink whose ``close()`` on a closer thread changes the sink to ``target``."""

    def __init__(self, target: object) -> None:
        super().__init__()
        self.target = target

    def close(self) -> None:
        super().close()
        if threading.current_thread().name.startswith("log-foundry-") and self.target is not None:
            target, self.target = self.target, None
            log_foundry.configure(sink=target)  # type: ignore[arg-type]


_ROUTES = [
    "user-thread-change",
    "raising-change",
    "drain-thread-change",
    "closer-thread-change",
    "orphan-library-thread-change",
    "library-thread-holding-the-sink-lock",
]


@pytest.mark.parametrize("route", _ROUTES)
def test_every_deferral_is_taken_by_the_change_it_was_deferred_to(
    route: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FR-002: a stop defers only to a registered taker, which takes it; otherwise it runs.

    The structural rule behind every route a deferral took: a ``shutdown()`` defers only to the
    sink change registered, under ``Runtime._deferred_lock``, as the one that will take deferred
    stops, and only while that change is on its thread's stack; only that change takes them. Every
    operation's deferred list is replaced by one that records an append made while it is not the
    running taker, and every route a deferral can take is driven: a
    stop-signal setter on the changing user thread, the same in a change that then raises, a
    change made on the drain thread from a sink's ``emit``, one made on a closer thread from a
    displaced sink's ``close()``, one made on an orphan ``log-foundry-`` thread, and a
    ``shutdown()`` from another ``log-foundry-`` thread while a library thread holds
    ``config._sink_lock``. In each, no append is made without a taker, nothing is left deferred or
    registered, the stop ran (the process retired), and a later, unrelated ``configure()`` retires
    nothing. Deferring to whoever held ``config._sink_lock`` stranded the stops of the re-entrant
    routes, which no change takes, until that later ``configure()`` ran them.
    """
    built, orphaned = _checked_operations(monkeypatch)
    if route in ("user-thread-change", "raising-change"):
        log_foundry.configure(service="routes", sink=_Plain())  # type: ignore[arg-type]
        log_foundry.info("owed")
        b = _SetterStops(then_raise=route == "raising-change")
        if route == "raising-change":
            with pytest.raises(KeyboardInterrupt):
                log_foundry.configure(sink=b)  # type: ignore[arg-type]
        else:
            log_foundry.configure(sink=b)  # type: ignore[arg-type]
    elif route == "drain-thread-change":
        b = _SetterStops()
        log_foundry.configure(service="routes", sink=_ChangesOnEmit(b))  # type: ignore[arg-type]

        @log_foundry.trace
        def work() -> None:
            log_foundry.info("in span")

        work()
        log_foundry.flush(timeout=3.0)
    elif route == "closer-thread-change":
        b = _SetterStops()
        a = _ChangesOnClose(b)
        log_foundry.configure(service="routes", sink=a)  # type: ignore[arg-type]

        @log_foundry.trace
        def work() -> None:
            log_foundry.info("in span")

        work()
        log_foundry.flush(timeout=3.0)
        log_foundry.configure(sink=_Plain())  # type: ignore[arg-type]
    else:
        log_foundry.configure(service="routes", sink=_Plain())  # type: ignore[arg-type]
        log_foundry.info("owed")
        b = (
            _SetterStops()
            if route == "orphan-library-thread-change"
            else _SetterStartsAStoppingThread()
        )
        changer = threading.Thread(
            target=lambda: log_foundry.configure(sink=b),  # type: ignore[arg-type]
            name="log-foundry-probe",
        )
        changer.start()
        changer.join(GATE)
        assert not changer.is_alive()
    assert b.fired, f"the premise: the {route} route's shutdown() was made"
    assert orphaned == [], "a stop was deferred to a change that was not the running taker"
    assert [list(op.deferred) for op in built if op.deferred] == [], "a deferral was left behind"
    assert _lifecycle._state._deferred_taker is None, "a change stayed registered as the taker"
    retired = _lifecycle._state.retirements
    assert retired >= 1, "the shutdown() never ran"
    log_foundry.configure(sink=_Plain())  # type: ignore[arg-type]
    log_foundry.info("later")
    assert _lifecycle._state.retirements == retired, "a later configure() ran a stale stop"


def test_the_teardown_guard_names_a_sink_holder_left_behind() -> None:
    """FR-002, plan of frame 2 m3: a ``config._sink_holder`` left set is named, and reset.

    A holder left set makes every later ``shutdown()`` on that thread read a change in flight, so
    the guard every test's teardown runs names it, and the reset clears it for the next test.
    """
    import conftest
    from log_foundry import config

    config._sink_holder = threading.get_ident()
    try:
        message = conftest._left_commands()
    finally:
        config._sink_holder = None
    assert message is not None and "left config._sink_holder set" in message, message
    taker = _lifecycle._Operation(_lifecycle._Op.SWAP, None)
    _lifecycle._state._deferred_taker = taker
    try:
        message = conftest._left_commands()
    finally:
        _lifecycle._state._deferred_taker = None
    assert message is not None and "registered as the taker" in message, message
    taker.deferred.append(None)
    _lifecycle._state._deferred_taker = taker
    try:
        message = conftest._left_commands()
    finally:
        _lifecycle._state._deferred_taker = None
    assert message is not None and "1 stop(s) deferred to a sink change" in message, message
    assert conftest._left_commands() is None


class _GatedList(list):  # type: ignore[type-arg]
    """The deferred list, whose first iteration releases a paused deferral and waits for it."""

    def __init__(self, release: threading.Event, appended: threading.Event) -> None:
        super().__init__()
        self.release, self.appended = release, appended

    def __iter__(self):  # type: ignore[no-untyped-def]
        self.release.set()
        self.appended.wait(GATE)
        return super().__iter__()


GATE = 5.0
"""Seconds a test waits for a paused thread."""


@pytest.mark.parametrize("resume", ["after-the-change-ends", "between-copy-and-clear"])
def test_a_log_foundry_thread_that_decides_to_defer_is_never_left_behind(
    resume: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """MAJOR 1, the cross-thread routes: a deferral decided during a change is always run.

    During a sink change a ``log-foundry-`` thread calls ``shutdown()``. It reads the change in
    flight and decides to defer, and is paused just before its append. ``after-the-change-ends``
    resumes it once the change could have taken the list; ``between-copy-and-clear`` resumes it
    from the change's own read of the list. With the decision and the append outside a lock, and
    the list copied then cleared, both left the stop unrun: appended after the take, or appended
    between the copy and the clear and cleared. Now the decision and the append are one section of
    the lock the change's end takes, so the change waits for the append and runs the stop.
    """
    import inspect

    log_foundry.configure(service="routes", sink=_Plain())  # type: ignore[arg-type]
    log_foundry.info("owed")
    lines, first = inspect.getsourcelines(_lifecycle._merged_or_deferred)
    at = first + next(
        i for i, line in enumerate(lines) if "taker.deferred.append(deadline)" in line
    )
    code = _lifecycle._merged_or_deferred.__code__
    paused, release, appended = threading.Event(), threading.Event(), threading.Event()

    def deferring() -> None:
        def tracer(frame, event, arg):  # type: ignore[no-untyped-def]
            if frame.f_code is code:

                def local(frame, event, arg):  # type: ignore[no-untyped-def]
                    if event == "line" and frame.f_lineno == at and not paused.is_set():
                        paused.set()
                        release.wait(GATE)
                    return local

                return local
            return None

        sys.settrace(tracer)
        try:
            log_foundry.shutdown(timeout=1.0)
        finally:
            sys.settrace(None)
            appended.set()

    class StartsTheDeferral(_Plain):
        def __init__(self) -> None:
            super().__init__()
            self.started: list[threading.Thread] = []

        @property
        def log_foundry_stop_signal(self) -> threading.Event | None:
            return None

        @log_foundry_stop_signal.setter
        def log_foundry_stop_signal(self, value: threading.Event | None) -> None:
            if self.started or threading.current_thread() is not threading.main_thread():
                return
            thread = threading.Thread(target=deferring, name="log-foundry-probe", daemon=True)
            self.started.append(thread)
            thread.start()
            assert paused.wait(GATE), "the log-foundry- thread never reached its append"

    if resume == "between-copy-and-clear":
        real_init = _lifecycle._Operation.__init__

        def init(self: _lifecycle._Operation, kind: _lifecycle._Op, timeout: float | None) -> None:
            real_init(self, kind, timeout)
            self.deferred = _GatedList(release, appended)

        monkeypatch.setattr(_lifecycle._Operation, "__init__", init)
    timer = threading.Timer(0.5, release.set)
    timer.start()
    try:
        b = StartsTheDeferral()
        log_foundry.configure(sink=b)  # type: ignore[arg-type]
        b.started[0].join(GATE)
    finally:
        release.set()
        timer.cancel()
    assert not b.started[0].is_alive()
    assert _lifecycle._state._deferred_taker is None, "a deferral outlived its change"
    assert _lifecycle._state.retirements == 1, "the deferred stop was never run"


@pytest.mark.parametrize(
    ("deadlines", "until", "expected"),
    [
        ((1.0, 3.0), None, 3.0),
        ((3.0, 1.0), None, 3.0),
        ((1.0, None), None, None),
        ((None,), 2.0, 2.0),
        ((1.0, 3.0), 2.0, 2.0),
    ],
    ids=["latest", "latest-first", "an-unbounded-call", "cut-by-the-budget", "both-cut"],
)
def test_the_deferred_stop_takes_the_latest_deadline_or_none_cut_at_the_budget(
    deadlines: tuple[float | None, ...],
    until: float | None,
    expected: float | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-002: the latest deadline among the deferred calls, none if any was unbounded, then cut.

    Offsets are seconds from now; the stop is captured rather than run. ``min`` in place of ``max``
    reads the earliest, and dropping the unbounded call bounds a stop a caller asked to wait for.
    """
    seen: list[tuple[float | None, bool]] = []
    monkeypatch.setattr(
        _lifecycle, "_shutdown_worker", lambda timeout, *, detach: seen.append((timeout, detach))
    )
    now = time.monotonic()
    absolute = [None if d is None else now + d for d in deadlines]
    _lifecycle.run_deferred_stops(absolute, None if until is None else now + until)
    assert len(seen) == 1 and seen[0][1] is True, seen
    timeout = seen[0][0]
    if expected is None:
        assert timeout is None, seen
    else:
        assert timeout is not None and expected - 0.25 < timeout <= expected, seen


def test_an_interrupt_before_the_change_binds_its_operation_leaves_nothing_published(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-001/FR-002: the change's end finds what it published by its frame, not by its local.

    A ``KeyboardInterrupt`` landing as ``open_command_swap`` returns, before ``configure()`` binds
    the operation to its local, used to leave the operation published on the thread and its token
    in ``_swaps_running``, undecided, and the change registered as the taker (the execution
    review's ``p4_sweep.py``, rows 27 to 32). The end now finds the operation by the change's frame.
    """
    log_foundry.configure(service="unbound", sink=_Plain())  # type: ignore[arg-type]
    log_foundry.info("owed")
    real = _lifecycle.open_command_swap

    def interrupted(*args: Any) -> _lifecycle._Operation:
        real(*args)
        raise KeyboardInterrupt("as the operation is returned")

    monkeypatch.setattr(_lifecycle, "open_command_swap", interrupted)
    with pytest.raises(KeyboardInterrupt, match="as the operation is returned"):
        log_foundry.configure(sink=_Plain())  # type: ignore[arg-type]
    assert _lifecycle._state._calls_here.published is None, "the operation stayed published"
    assert not _lifecycle._state._swaps_running, "its token stayed in flight, undecided"
    assert _lifecycle._state._deferred_taker is None, "it stayed registered as the taker"


def test_only_the_change_registered_first_takes_a_stop_deferred_to_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-002: a second, unserialized change neither takes the registration nor its stops.

    The first change is parked between its registration and its swap, holding
    ``config._sink_lock``. A second ``configure(sink=...)`` gives up waiting for the lock, changes
    the sink alongside it and returns. The first then swaps, and its sink's stop-signal setter
    calls ``shutdown()``: that call is deferred to the first change, which is still running and
    registered, so it has not retired the library when the setter returns, and the first change
    runs it as it ends. A second change that registered over the first, or that took the first's
    registration as it ended, left the setter's ``shutdown()`` no taker, and it ran inline in the
    middle of the first change's swap (the execution review's surviving ``M2-always-register``
    and ``M5-take-without-identity``).
    """
    from log_foundry import worker as worker_module

    monkeypatch.setattr(worker_module, "DEFAULT_SWAP_TIMEOUT", 0.3)
    log_foundry.configure(service="second", sink=_Plain())  # type: ignore[arg-type]
    log_foundry.info("owed")
    parked, release = threading.Event(), threading.Event()
    seen: list[int] = []

    class Setter(_Plain):
        @property
        def log_foundry_stop_signal(self) -> threading.Event | None:
            return None

        @log_foundry_stop_signal.setter
        def log_foundry_stop_signal(self, value: threading.Event | None) -> None:
            if seen or value is None or threading.current_thread() is not first:
                return
            log_foundry.shutdown(timeout=1.0)
            seen.append(_lifecycle._state.retirements)

    b = Setter()
    real = _lifecycle._swap_sink

    def parking(new_sink: Any, timeout: float | None = 5.0) -> None:
        if new_sink is b and not parked.is_set():
            parked.set()
            release.wait(GATE)
        real(new_sink, timeout)

    monkeypatch.setattr(_lifecycle, "_swap_sink", parking)
    first = threading.Thread(target=lambda: log_foundry.configure(sink=b), name="first")  # type: ignore[arg-type]
    first.start()
    try:
        assert parked.wait(GATE), "the first change reached its swap"
        assert _lifecycle._state._deferred_taker is not None, "the premise: it registered"
        log_foundry.configure(sink=_Plain())  # type: ignore[arg-type]
    finally:
        release.set()
    first.join(GATE)
    assert not first.is_alive()
    assert seen == [0], f"the setter's shutdown() ran inside the first change: {seen}"
    assert _lifecycle._state.retirements == 1, "the first change did not run the deferred stop"


def test_a_finished_change_leaves_no_cycle_that_keeps_a_superseded_sink_alive() -> None:
    """MAJOR 1 of the final execution review: a change's frame is never a local of itself.

    ``configure(sink=...)`` bound its own frame to a local, to name the change, which made a
    reference cycle on every call: the frame held itself, and through ``f_back`` and its locals
    ``configure()``'s frame, its caller's and every sink they named, until a garbage collection.
    With the collector off, as a Lambda handler may run, the superseded sinks stayed alive
    (``p_cycle.py``). The frame is now passed inline and each operation drops it at the change's
    end, so with the collector disabled the two superseded sinks die as their changes return.
    """
    import gc
    import weakref

    def install() -> weakref.ref[_Plain]:
        sink = _Plain()
        log_foundry.configure(service="cycle", sink=sink)  # type: ignore[arg-type]
        return weakref.ref(sink)

    enabled = gc.isenabled()
    gc.disable()
    try:
        first, second, third = install(), install(), install()
        alive = [first() is not None, second() is not None, third() is not None]
    finally:
        if enabled:
            gc.enable()
    assert alive == [False, False, True], f"a superseded sink outlived its change: {alive}"


def test_a_taker_whose_change_has_ended_reads_as_none_and_lets_its_frame_go() -> None:
    """FR-002: a registered taker whose frame has left the stack is dropped, frame and deadlines.

    The branch of ``_live_taker`` no corpus row interrupts, since the count run never takes it:
    a change an interrupt unwound before its end deregistered it. A ``shutdown()`` made after it
    runs here rather than deferring to it, the slot is cleared, and the operation lets go of the
    frame and of the deadlines deferred to it.
    """
    log_foundry.configure(service="dead", sink=_Plain())  # type: ignore[arg-type]
    log_foundry.info("owed")

    def ended() -> Any:
        return sys._getframe()

    op = _lifecycle._Operation(_lifecycle._Op.SWAP, None)
    op.change = ended()
    op.deferred.append(None)
    _lifecycle._state._deferred_taker = op
    log_foundry.shutdown(timeout=1.0)
    assert _lifecycle._state.retirements == 1, "the shutdown() deferred to a change that ended"
    assert _lifecycle._state._deferred_taker is None, "the dead taker stayed registered"
    assert op.change is None, "the dead taker kept its change's frame alive"
    assert op.deferred == [], "the dead taker kept its deadlines"


def test_an_interrupt_between_publishing_and_adding_the_token_leaves_no_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-001: ``open_command_swap`` publishes the operation before it adds the token.

    The change's end finds an operation ``configure()`` never bound through the thread's
    published slot, so the publication must come first: a token added before it, then
    interrupted, was in flight and undecided for good, and every later ``shutdown()`` from
    another thread waited its closer grace on it. The interrupt lands on whichever of the two
    statements comes second, read from the source.
    """
    import inspect

    lines, first = inspect.getsourcelines(_lifecycle.open_command_swap)
    published = next(i for i, s in enumerate(lines) if "_calls_here.published = op" in s)
    added = next(i for i, s in enumerate(lines) if "_swaps_running.add(op)" in s)
    at = first + max(published, added)
    code = _lifecycle.open_command_swap.__code__
    log_foundry.configure(service="order", sink=_Plain())  # type: ignore[arg-type]
    log_foundry.info("owed")

    def tracer(frame, event, arg):  # type: ignore[no-untyped-def]
        if frame.f_code is not code:
            return None

        def local(frame, event, arg):  # type: ignore[no-untyped-def]
            if event == "line" and frame.f_lineno == at:
                raise KeyboardInterrupt("between publishing and the token")
            return local

        return local

    sys.settrace(tracer)
    try:
        with pytest.raises(KeyboardInterrupt, match="between publishing and the token"):
            log_foundry.configure(sink=_Plain())  # type: ignore[arg-type]
    finally:
        sys.settrace(None)
    assert not _lifecycle._state._swaps_running, "a token stayed in flight, undecided"
    assert _lifecycle._state._calls_here.published is None, "the operation stayed published"
