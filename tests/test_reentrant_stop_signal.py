"""A sink's stop-signal setter that calls back into the library never deadlocks it.

A ``log_foundry_stop_signal`` setter is the sink's own code. Until this fix the library ran it
while holding ``_lifecycle._state._lock`` (the orphan log, the post-fork debt, ``flush()``, the
no-worker swap and the worker build), ``decorator._sweep_lock`` (a worker built by a sweep) or the
factory wrapper's build lock, all non-reentrant, so a setter calling ``configure()``, ``info()``,
``flush()`` or ``shutdown()`` on the same thread hung the process: at ``624d817f`` fifteen of the
twenty rows in the table below hung; the rest, and the tests after the table, say what they did
there in their own docstrings. The library now decides under the lock
and hands after releasing it.

**Every guard against a hang runs in a forked child** with a bounded wait, through
``test_fork_lifecycle.run_in_child``. Asserting ``is_alive()`` on a thread in-process still hung
the test process in teardown. The child writes a marker just before the call that hung, so a hang
reads back as that marker alone rather than as silence.

The close counts follow invariant 5: a sink written to after its close is owed a second close. A
setter that swaps the sink or shuts down during a log's hand closes the sink that log then writes
to, so that sink is closed twice, once by the setter's call and once at the final ``shutdown()``.

Serves invariants 1, 3, 5 and 6.
"""

from __future__ import annotations

import gc
import io
import os
import select
import sys
import threading
import time
from typing import TYPE_CHECKING

import pytest

import log_foundry
from log_foundry import _lifecycle
from log_foundry.sinks._factory import _FactorySink
from test_fork_lifecycle import report_from_child, run_in_child

if TYPE_CHECKING:
    from collections.abc import Callable

ENTERED = "entered;"
"""What a child reports just before the call that hung at ``624d817f``."""

GATE = 5.0
"""Seconds a parked setter or a racing thread is given, inside a child, before the test gives up."""


class Plain:
    """A sink that records its events and counts its closes, with no stop signal."""

    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []
        self.closes = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        self.events.extend(batch)

    def close(self) -> None:
        self.closes += 1


class Hook(Plain):
    """A sink whose stop-signal setter runs ``act`` once, the first time it is handed a signal."""

    def __init__(self) -> None:
        super().__init__()
        self.act: Callable[[], None] = lambda: None
        self.fired = False
        self.calls = 0
        self._signal: threading.Event | None = None

    @property
    def log_foundry_stop_signal(self) -> threading.Event | None:
        return self._signal

    @log_foundry_stop_signal.setter
    def log_foundry_stop_signal(self, event: threading.Event | None) -> None:
        self._signal = event
        self.calls += 1
        if not self.fired:
            self.fired = True
            self.act()


def _traced() -> Callable[[], None]:
    @log_foundry.trace
    def work() -> None:
        pass

    return work


REENTRANT_LINE = "was called by a sink's own code, from inside the library"
"""The fixed part of the ``pending`` line a re-entrant ``configure(sink=...)`` writes."""


def _stderr() -> io.StringIO:
    """Replaces the child's ``sys.stderr``, so a test can read what the library wrote there."""
    captured = io.StringIO()
    sys.stderr = captured
    return captured


def _flush_quietly() -> None:
    log_foundry.flush(timeout=GATE)


def _summary(**sinks: Plain) -> str:
    """One line naming each sink's event and close count, read after a final ``shutdown()``."""
    return " ".join(f"{name}={len(s.events)}/{s.closes}" for name, s in sorted(sinks.items()))


# -- A: every path that hands a signal returns when the setter re-enters ------------------------


def _orphan(act: str) -> Callable[[], str]:
    """``configure(A)``, then a first ``info()`` whose hand runs the setter."""

    def work() -> str:
        a, b = Hook(), Plain()
        log_foundry.configure(service="t", sink=a)
        flushed: list[bool] = []
        a.act = {
            "configure": lambda: log_foundry.configure(sink=b),
            "info": lambda: log_foundry.info("from the setter"),
            "flush": lambda: flushed.append(bool(log_foundry.flush(timeout=GATE))),
            "shutdown": lambda: log_foundry.shutdown(timeout=1.0),
            "trace": _traced(),
        }[act]
        report_from_child(ENTERED)
        log_foundry.info("x")
        log_foundry.info("after")
        named_b = log_foundry.get_config().sink is b
        log_foundry.shutdown(timeout=GATE)
        return f"b_named={named_b} flushed={flushed} calls={a.calls} {_summary(a=a, b=b)}"

    return work


def _worker(act: str) -> Callable[[], str]:
    """``configure(A)``, then a first ``@trace`` whose worker build hands A its signal."""

    def work() -> str:
        a, b = Hook(), Plain()
        log_foundry.configure(service="t", sink=a)
        flushed: list[bool] = []
        a.act = {
            "configure": lambda: log_foundry.configure(sink=b),
            "info": lambda: log_foundry.info("from the setter"),
            "flush": lambda: flushed.append(bool(log_foundry.flush(timeout=GATE))),
            "shutdown": lambda: log_foundry.shutdown(timeout=1.0),
        }[act]
        traced = _traced()
        report_from_child(ENTERED)
        traced()
        traced()
        log_foundry.flush(timeout=GATE)
        named_b = log_foundry.get_config().sink is b
        log_foundry.shutdown(timeout=GATE)
        return f"b_named={named_b} flushed={flushed} {_summary(a=a, b=b)}"

    return work


def _swap(act: str) -> Callable[[], str]:
    """``configure(P)`` and a log, so P is owed, then a no-worker ``configure(A)``."""

    def work() -> str:
        p, a, b = Plain(), Hook(), Plain()
        captured = _stderr()
        log_foundry.configure(service="t", sink=p)
        log_foundry.info("first")
        flushed: list[bool] = []
        a.act = {
            "configure": lambda: log_foundry.configure(sink=b),
            "info": lambda: log_foundry.info("from the setter"),
            "flush": lambda: flushed.append(bool(log_foundry.flush(timeout=GATE))),
            "shutdown": lambda: log_foundry.shutdown(timeout=1.0),
        }[act]
        report_from_child(ENTERED)
        log_foundry.configure(sink=a)
        log_foundry.info("after")
        named_b = log_foundry.get_config().sink is b
        log_foundry.shutdown(timeout=GATE)
        reentrant = REENTRANT_LINE in captured.getvalue()
        return (
            f"b_named={named_b} flushed={flushed} reentrant={reentrant} {_summary(p=p, a=a, b=b)}"
        )

    return work


def _factory(act: str) -> Callable[[], str]:
    """``configure(sink_factory=...)``; the first log builds A, and A's hand runs the setter."""

    def work() -> str:
        a, b = Hook(), Plain()
        log_foundry.configure(service="t", sink_factory=lambda: a)
        a.act = {
            "configure": lambda: log_foundry.configure(sink=b),
            "info": lambda: log_foundry.info("from the setter"),
        }[act]
        report_from_child(ENTERED)
        log_foundry.info("x")
        log_foundry.info("after")
        named_b = log_foundry.get_config().sink is b
        log_foundry.shutdown(timeout=GATE)
        return f"b_named={named_b} {_summary(a=a, b=b)}"

    return work


def _sweep(act: str) -> Callable[[], str]:
    """A ``flush()`` inside a traced call sweeps its buffer and builds the worker there."""

    def work() -> str:
        a = Hook()
        log_foundry.configure(service="t", sink=a)
        inner = _traced()
        a.act = {
            "flush": _flush_quietly,
            "trace": inner,
        }[act]

        @log_foundry.trace
        def outer() -> None:
            log_foundry.info("in the span")
            report_from_child(ENTERED)
            log_foundry.flush(timeout=GATE)

        outer()
        log_foundry.shutdown(timeout=GATE)
        return _summary(a=a)

    return work


def _retarget(act: str) -> Callable[[], str]:
    """A live worker on P, then ``configure(A)``: ``Worker.retarget`` hands A its signal."""

    def work() -> str:
        p, a, b = Plain(), Hook(), Plain()
        log_foundry.configure(service="t", sink=p)
        traced = _traced()
        traced()
        a.act = {
            "configure": lambda: log_foundry.configure(sink=b),
            "info": lambda: log_foundry.info("from the setter"),
            "flush": _flush_quietly,
        }[act]
        report_from_child(ENTERED)
        log_foundry.configure(sink=a)
        traced()
        named_b = log_foundry.get_config().sink is b
        log_foundry.flush(timeout=GATE)
        return f"b_named={named_b} a_events={len(a.events)} b_events={len(b.events)}"

    return work


def _acts(b: Plain) -> dict[str, Callable[[], None]]:
    """The calls a setter makes back into the library, by name, the swap one targeting ``b``."""
    return {
        "configure": lambda: log_foundry.configure(sink=b),
        "info": lambda: log_foundry.info("from the setter"),
        "flush": _flush_quietly,
        "shutdown": lambda: log_foundry.shutdown(timeout=1.0),
    }


def _fork_rebuild(act: str) -> Callable[[], str]:
    """A live worker on A, then a fork: the child's worker rebuild hands A the child's signal."""

    def work() -> str:
        a = Hook()
        log_foundry.configure(service="t", sink=a)
        _flush_quietly()
        _traced()()
        a.fired = False
        a.act = _acts(Plain())[act]
        read_fd, write_fd = os.pipe()
        report_from_child(ENTERED)
        gc.collect()
        pid = os.fork()
        if pid == 0:
            os.write(write_fd, b"returned")
            os._exit(0)
        os.close(write_fd)
        ready = select.select([read_fd], [], [], GATE)[0]
        line = os.read(read_fd, 64).decode() if ready else "silent"
        if not ready:
            os.kill(pid, 9)
        os.waitpid(pid, 0)
        return line

    return work


def _factory_forward(act: str) -> Callable[[], str]:
    """A built factory sink A, then a fresh signal: the wrapper forwards it to A's setter."""

    def work() -> str:
        a = Hook()
        log_foundry.configure(service="t", sink_factory=lambda: a)
        log_foundry.info("builds A")
        a.fired = False
        a.act = _acts(Plain())[act]
        _lifecycle._state._stop.set()
        report_from_child(ENTERED)
        log_foundry.info("hands the wrapper a fresh signal")
        return "returned"

    return work


class _SwappedOut(Plain):
    """A sink whose first close — a swap's detached one — parks on ``go``; records its signals.

    ``emit`` backs off up to 0.3 s on the signal it holds and records how long it waited, so a
    set signal shows as a wait of nothing. With ``build_in_second_close`` its second close builds
    the worker on another thread first, then backs off up to 4 s on its signal. A second close
    records ``_shutdown_running`` as it begins, so a test relying on that close running after the
    count dropped can assert it rather than trust a sleep.
    """

    def __init__(self, *, build_in_second_close: bool = False) -> None:
        super().__init__()
        self.build_in_second_close = build_in_second_close
        self.in_close = threading.Event()
        self.go = threading.Event()
        self.waits: list[float] = []
        self.second_close_held_set: list[bool] = []
        self.second_close_shutdown_running: list[int] = []
        self.offers: list[threading.Event | None] = []
        self.act: Callable[[], None] | None = None
        self.armed = False
        self._signal: threading.Event | None = None

    @property
    def log_foundry_stop_signal(self) -> threading.Event | None:
        return self._signal

    @log_foundry_stop_signal.setter
    def log_foundry_stop_signal(self, event: threading.Event | None) -> None:
        self.offers.append(event)
        self._signal = event
        if self.armed and self.act is not None:
            self.armed = False
            self.act()

    def emit(self, batch: list[dict[str, object]]) -> None:
        from log_foundry.sinks import _retry

        started = time.monotonic()
        _retry.wait(0.3, self._signal)
        self.waits.append(round(time.monotonic() - started, 1))
        super().emit(batch)

    def close(self) -> None:
        if not self.closes:
            self.in_close.set()
            self.go.wait(GATE * 2)
        elif self.build_in_second_close:
            from log_foundry.sinks import _retry

            self.second_close_shutdown_running.append(_lifecycle._state._shutdown_running)
            tracer = threading.Thread(target=lambda: _traced()(), daemon=True)
            tracer.start()
            tracer.join(GATE)
            held = self._signal
            self.second_close_held_set.append(held is not None and held.is_set())
            _retry.wait(4.0, held)
        super().close()


def _swapped_out_and_back(sink: _SwappedOut) -> None:
    """Arms ``sink``, swaps it out — its close parks on a detached thread — and configures it again."""
    from log_foundry import worker as worker_mod

    worker_mod.DEFAULT_SWAP_TIMEOUT = 0.2
    log_foundry.configure(service="t", sink=sink)
    log_foundry.info("arms it")
    log_foundry.configure(sink=Plain())
    if not sink.in_close.wait(GATE):
        raise AssertionError("the swap never started closing the sink")
    log_foundry.configure(sink=sink)


_ROWS: dict[str, tuple[Callable[[], str], str]] = {
    "orphan-configure": (
        _orphan("configure"),
        "b_named=True flushed=[] calls=1 a=1/2 b=1/1",
    ),
    "orphan-info": (_orphan("info"), "b_named=False flushed=[] calls=1 a=3/1 b=0/0"),
    "orphan-flush": (_orphan("flush"), "b_named=False flushed=[True] calls=1 a=2/1 b=0/0"),
    "orphan-shutdown": (_orphan("shutdown"), "b_named=False flushed=[] calls=1 a=2/2 b=0/0"),
    "orphan-trace": (_orphan("trace"), "b_named=False flushed=[] calls=1 a=4/1 b=0/0"),
    "worker-configure": (_worker("configure"), "b_named=True flushed=[] a=0/1 b=4/1"),
    "worker-info": (_worker("info"), "b_named=False flushed=[] a=5/1 b=0/0"),
    "worker-flush": (_worker("flush"), "b_named=False flushed=[True] a=4/1 b=0/0"),
    "worker-shutdown": (_worker("shutdown"), "b_named=False flushed=[] a=0/1 b=0/0"),
    "swap-noworker-configure": (
        _swap("configure"),
        "b_named=True flushed=[] reentrant=True a=0/1 b=1/1 p=1/1",
    ),
    "swap-noworker-info": (
        _swap("info"),
        "b_named=False flushed=[] reentrant=False a=2/1 b=0/0 p=1/1",
    ),
    "swap-noworker-flush": (
        _swap("flush"),
        "b_named=False flushed=[True] reentrant=False a=1/1 b=0/0 p=1/1",
    ),
    "swap-noworker-shutdown": (
        _swap("shutdown"),
        "b_named=False flushed=[] reentrant=False a=1/2 b=0/0 p=1/1",
    ),
    "factory-orphan-info": (_factory("info"), "b_named=False a=3/1 b=0/0"),
    "factory-orphan-configure": (_factory("configure"), "b_named=True a=1/2 b=1/1"),
    "sweep-flush": (_sweep("flush"), "a=3/1"),
    "sweep-trace": (_sweep("trace"), "a=5/1"),
    "retarget-configure": (_retarget("configure"), "b_named=True a_events=0 b_events=2"),
    "retarget-info": (_retarget("info"), "b_named=False a_events=3 b_events=0"),
    "retarget-flush": (_retarget("flush"), "b_named=False a_events=2 b_events=0"),
    **{f"fork-rebuild-{act}": (_fork_rebuild(act), "returned") for act in _acts(Plain())},
    **{f"factory-forward-{act}": (_factory_forward(act), "returned") for act in _acts(Plain())},
}
"""Each path that hands a signal, by the call its setter makes, and what the child reports.

The counts are ``events/closes`` per sink after the final ``shutdown()``. Where the setter swaps the
sink or shuts down during a log's hand, the log then writes to the sink the setter closed, so that
sink is closed again at the end (invariant 5): ``orphan-configure``, ``orphan-shutdown``,
``factory-orphan-configure`` and ``swap-noworker-shutdown`` (whose closing ``info("after")`` writes
to A). The worker rows count ``span.start`` and ``span.end`` for each of two traced calls. The
orphan rows count the setter's calls: one hand each, since a sink is handed its own event once
per installation (SPEC-098), so the shutdown row's later log hands A nothing where it used to hand
it the refreshed signal a second time; ``orphan-trace`` builds the worker on A inside A's own hand,
and that build finds A on this thread's offer record, so A's setter runs once, not twice. The swap
rows say whether the setter's ``configure()`` wrote the re-entrant ``pending`` line: the no-worker
swap hands its sink inside ``config._sink_lock``, so a ``configure()`` from that setter is the
re-entrant kind, which tries the lock once, swaps unconfirmed and announces it. SPEC-098 deleted
three hand sites and their rows: the last stop's payment of a hand a build in its tail declined
(``stop-end``), a close's discharge paying one a build declined inside a ``shutdown()``'s inline
close (``close-end``), and a writer paying a mark it made after the last stop's end looked
(``writer-pays``); none of those hands exists, so no setter runs there. The ``fork-rebuild`` rows
fork after A was handed: since SPEC-098 the child's worker rebuild finds A handed and runs no
setter (FR-003), and the ``factory-forward`` rows set the worker's event and log, which since
SPEC-098 hands the wrapper nothing new; each only has to return. The retarget rows returned at
``624d817f`` too: ``Worker.retarget`` already handed outside
every lock a re-entrant ``configure()`` waits on, and they pin the precedent the fix generalises.
"""


@pytest.mark.parametrize("row", list(_ROWS))
def test_a_setter_reentering_the_library_returns(row: str) -> None:
    """Every path that hands a stop signal returns when the setter calls back into the library.

    At ``624d817f`` every row hung except four. The three retarget rows returned, and so did
    ``swap-noworker-info``, because that swap armed A before handing it its signal, so the
    setter's log took the lock-free fast path. ``factory-orphan-configure`` returned too, with A
    written to and closed by nobody: the swap closed the factory wrapper before it had published
    what it built.
    """
    work, expected = _ROWS[row]
    child = run_in_child(work)
    assert child.output != ENTERED, f"{row}: hung inside the setter's call into the library"
    assert child.output == ENTERED + expected, row


# -- B1: one hand does not beget another under concurrent logging ------------------------------


THREADS = 8
PER_THREAD = 300


def test_concurrent_logs_do_not_keep_handing_each_other_the_signal() -> None:
    """Concurrent first logs hand the sink its event a handful of times, never once per log.

    Before SPEC-098 a section opened a hand token only when it published something, because a
    token per hand made every log that met one hand again: this case ran the setter 2399 times for
    its 2400 logs, against 8 or 9 with the rule. Since SPEC-098 a sink is handed once per
    installation, marked handed when a setter returns, so only the logs that reach the sink
    before its first setter returns run it, at most one per thread here. The bound, a quarter of
    the logs, still sits far from the cascade.
    """

    def work() -> str:
        class Slow(Plain):
            calls = 0
            _signal: threading.Event | None = None

            @property
            def log_foundry_stop_signal(self) -> threading.Event | None:
                return self._signal

            @log_foundry_stop_signal.setter
            def log_foundry_stop_signal(self, event: threading.Event | None) -> None:
                Slow.calls += 1
                time.sleep(0.0005)
                self._signal = event

        sink = Slow()
        log_foundry.configure(service="t", sink=sink)
        start = threading.Barrier(THREADS)

        def logs() -> None:
            start.wait()
            for _ in range(PER_THREAD):
                log_foundry.info("x")

        threads = [threading.Thread(target=logs) for _ in range(THREADS)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(GATE * 4)
        return f"calls={Slow.calls} events={len(sink.events)}"

    child = run_in_child(work, timeout=30)
    calls, events = (int(part.split("=")[1]) for part in child.output.split())
    assert events == THREADS * PER_THREAD, child.output
    assert calls < THREADS * PER_THREAD // 4, (
        f"{child.output}: a log that hands under another's token opened a token of its own"
    )


# -- M1: the worker build cannot strand its token ----------------------------------------------


# -- ordering, the token, the twin, recursion, the cap, a fork ---------------------------------


def _unset(signal: threading.Event | None) -> bool:
    """Whether a sink holds a signal a shutdown can still cut a backoff short with."""
    return signal is not None and not signal.is_set()


class Parked(Plain):
    """A sink whose first hand parks before it stores the signal, until ``release`` is set.

    Records, at each ``emit`` and ``flush``, whether it held an unset signal then.
    """

    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()
        self.first = True
        self.held_at_emit: list[bool] = []
        self.held_at_flush: list[bool] = []
        self._signal: threading.Event | None = None

    @property
    def log_foundry_stop_signal(self) -> threading.Event | None:
        return self._signal

    @log_foundry_stop_signal.setter
    def log_foundry_stop_signal(self, event: threading.Event | None) -> None:
        if self.first and threading.current_thread() is not threading.main_thread():
            self.first = False
            self.entered.set()
            self.release.wait(GATE + 2.0)
        self._signal = event

    def emit(self, batch: list[dict[str, object]]) -> None:
        self.held_at_emit.append(_unset(self._signal))
        super().emit(batch)

    def flush(self) -> None:
        self.held_at_flush.append(_unset(self._signal))


def _parked_in(start: Callable[[], None], sink: Parked) -> threading.Thread:
    """Starts ``start`` on a thread and returns once that thread is parked in the sink's setter."""
    thread = threading.Thread(target=start, daemon=True)
    thread.start()
    if not sink.entered.wait(GATE):
        raise AssertionError("the first hand never reached the setter")
    return thread


def _finished(call: Callable[[], None]) -> bool:
    """Runs ``call`` on a thread and says whether it returned within the gate."""
    thread = threading.Thread(target=call, daemon=True)
    thread.start()
    thread.join(GATE)
    return not thread.is_alive()


@pytest.mark.parametrize("path", ["orphan", "swap", "worker"])
def test_another_thread_hands_the_signal_before_it_emits(path: str) -> None:
    """A sink whose first hand is still in its setter is handed by the next thread to reach it.

    SPEC-098 FR-001 AC-3. The first hand is parked in the setter after its section armed the
    sink: a first log, a no-worker swap, or the worker build. A log on another thread finds the
    sink not yet handed, because the mark is made only when a setter **returns**, so it offers
    the sink its own event itself, and the sink holds it when that log's event reaches it. Marked
    handed before the setter returned, that log emitted to a sink holding none (measured on the
    prototype, ``held=[False]`` on all three rows). Before SPEC-098 a hand token did this work. At
    ``624d817f`` the second log waited on the lock the parked setter held (``orphan``,
    ``worker``), or, after the swap, took the fast path to a sink that held no signal yet
    (``swap``), since that swap armed the sink before it offered.
    """

    def work() -> str:
        sink = Parked()
        if path == "swap":
            log_foundry.configure(service="t", sink=Plain())
            log_foundry.info("so something is owed")
            first = _parked_in(lambda: log_foundry.configure(sink=sink), sink)
        else:
            log_foundry.configure(service="t", sink=sink)
            _flush_quietly()
            start = _traced() if path == "worker" else lambda: log_foundry.info("first")
            first = _parked_in(start, sink)
        sink.held_at_emit.clear()
        raced = _finished(lambda: log_foundry.info("racing"))
        sink.release.set()
        first.join(GATE)
        return f"raced={raced} held={sink.held_at_emit[:1]}"

    child = run_in_child(work)
    assert child.output == "raced=True held=[True]", child.output


def test_a_flush_hands_a_sink_another_thread_is_still_handing() -> None:
    """``flush()`` is the orphan log's twin: no owed sink is flushed before it holds its signal.

    One thread is parked in the first hand to S. A ``flush()`` on another thread finds S not yet
    handed and offers it its own event before calling S's own ``flush`` (invariant 6; SPEC-098's
    plan, D-3a). Without that offer the flush reached S holding no signal. At ``624d817f`` the
    flush waited on the lock the parked setter held.
    """

    def work() -> str:
        sink = Parked()
        log_foundry.configure(service="t", sink=sink)
        first = _parked_in(lambda: log_foundry.info("first"), sink)
        raced = _finished(_flush_quietly)
        sink.release.set()
        first.join(GATE)
        return f"raced={raced} held={sink.held_at_flush[:1]}"

    child = run_in_child(work)
    assert child.output == "raced=True held=[True]", child.output


@pytest.mark.parametrize("nest", ["top", "multi", "filter"])
@pytest.mark.parametrize("scenario", ["configure", "shutdown", "concurrent"])
def test_a_built_sink_written_after_a_release_is_closed_again(scenario: str, nest: str) -> None:
    """Every factory-built sink ends closed after its last event, nested or not (invariant 5).

    The wrapper is released during its first build — by the built sink's setter swapping the sink
    or shutting down, or by a ``configure(sink=...)`` on another thread while the factory runs —
    and the build's emit then writes to the built sink. Configured directly, the library owed the
    wrapper and re-arms it; inside a ``MultiSink`` or ``FilteringSink`` it owes the outer wrapper
    instead, and the wrapper's own release count is what re-arms it. Either way the final
    ``shutdown()`` closes the built sink after that event. Nested, at ``624d817f`` and before the
    count, the event after the release was closed by nobody.
    """
    from log_foundry.sinks.filtering import FilteringSink
    from log_foundry.sinks.multi import MultiSink

    def work() -> str:
        timeline: list[str] = []
        in_factory, release = threading.Event(), threading.Event()

        class Built:
            def __init__(self, action: str | None) -> None:
                self.action = action
                self._signal: threading.Event | None = None

            def emit(self, batch: list[dict[str, object]]) -> None:
                timeline.append("emit")

            def close(self) -> None:
                timeline.append("close")

            @property
            def log_foundry_stop_signal(self) -> threading.Event | None:
                return self._signal

            @log_foundry_stop_signal.setter
            def log_foundry_stop_signal(self, event: threading.Event | None) -> None:
                self._signal = event
                action, self.action = self.action, None
                if action == "configure":
                    log_foundry.configure(sink=Plain())
                elif action == "shutdown":
                    log_foundry.shutdown(timeout=2.0)

        def factory() -> Built:
            if scenario == "concurrent":
                in_factory.set()
                release.wait(GATE)
                return Built(None)
            return Built(scenario)

        log_foundry.configure(service="t", sink_factory=factory)
        wrapper = log_foundry.get_config().sink
        if nest == "multi":
            log_foundry.configure(sink=MultiSink(wrapper))
        elif nest == "filter":
            log_foundry.configure(sink=FilteringSink(wrapper, predicate=lambda event: True))
        if scenario == "concurrent":
            first = threading.Thread(target=lambda: log_foundry.info("x"), daemon=True)
            first.start()
            if not in_factory.wait(GATE):
                return "the factory never ran"
            log_foundry.configure(sink=Plain())
            release.set()
            first.join(GATE)
        else:
            log_foundry.info("x")
        log_foundry.shutdown(timeout=GATE)
        return " ".join(timeline)

    expected = "emit close" if scenario == "concurrent" else "close emit close"
    child = run_in_child(work)
    assert child.output == expected, child.output


def test_a_late_hand_after_a_fresh_worker_leaves_the_sink_its_own_set_event() -> None:
    """A late delivery after a shutdown finds the sink's own event set, until a ``configure()``.

    SPEC-098 FR-002, rewritten from ``..._still_leaves_the_current_signal``. T1's first log parks in
    S's setter holding E, S's own event; ``shutdown()`` sets E; a ``@trace`` builds a fresh worker
    on S, a delivery after the stop with no ``configure()`` between, whose build offers S the same
    E. When T1's setter stores E, S holds E, set: the late delivery's waits collapse, which is the
    cost FR-002 accepts. Before SPEC-098 the build handed S the worker's fresh unset event and a
    settle re-check kept it there. A ``configure()`` that installs S again then clears E in place,
    so S's backoffs wait again. At ``624d817f`` the shutdown waited on the lock the parked setter
    held.
    """

    def work() -> str:
        sink = Parked()
        log_foundry.configure(service="t", sink=sink)
        _flush_quietly()
        first = _parked_in(lambda: log_foundry.info("first"), sink)
        own = _lifecycle._state._signals[id(sink)][1]
        if not _finished(lambda: log_foundry.shutdown(timeout=2.0)):
            return "the shutdown waited on the parked setter"
        _traced()()
        sink.release.set()
        first.join(GATE)
        log_foundry.info("after")
        held = sink.log_foundry_stop_signal
        late = f"own={held is own} set={held is not None and held.is_set()}"
        log_foundry.configure(sink=sink)
        return f"{late} reconfigured_unset={_unset(sink.log_foundry_stop_signal)}"

    child = run_in_child(work)
    assert child.output == "own=True set=True reconfigured_unset=True", child.output


def test_a_factory_wrapper_inside_a_multisink_closes_what_it_built_once() -> None:
    """A wrapper the library does not owe is not re-armed by its own build.

    ``configure(sink=MultiSink(get_config().sink))`` puts the factory wrapper inside a sink the
    caller configured, so the library owes the ``MultiSink`` a close and reaches the wrapper
    through it. The wrapper's build re-arms only a wrapper the library owed when the emit began;
    re-arming this one made the library close it itself as well, so the built sink was closed
    twice, against once at ``624d817f``.
    """
    from log_foundry.sinks.multi import MultiSink

    def work() -> str:
        built: list[Plain] = []

        def factory() -> Plain:
            built.append(Plain())
            return built[-1]

        log_foundry.configure(service="t", sink_factory=factory)
        log_foundry.configure(sink=MultiSink(log_foundry.get_config().sink))
        log_foundry.info("x")
        log_foundry.shutdown(timeout=GATE)
        return " ".join(f"{len(b.events)}/{b.closes}" for b in built)

    child = run_in_child(work)
    assert child.output == "1/1", child.output


@pytest.mark.parametrize("built", [False, True], ids=["configured", "sink_factory"])
def test_a_setter_that_logs_on_every_hand_is_handed_once(built: bool) -> None:
    """A setter that logs to its own sink is not handed the signal again inside itself.

    Configured directly, the log inside the setter reaches ``offer_stop_signal``, which skips the
    sink this thread is already offering (its offer record; SPEC-098 deleted the ``handing_here``
    mark that used to do this). Built by ``sink_factory=``, the log reaches the wrapper, which
    finds the built sink pending and, through the same record, delivers to it without handing it
    again. Without either
    skip the setter recursed until ``RecursionError``, which the offer absorbed with a line on
    stderr: with the factory's skip removed, the setter ran 138 times in one run of this test,
    as deep as the recursion limit let it go.
    """

    def work() -> str:
        captured = _stderr()

        class Chatty(Plain):
            calls = 0
            _signal: threading.Event | None = None

            @property
            def log_foundry_stop_signal(self) -> threading.Event | None:
                return self._signal

            @log_foundry_stop_signal.setter
            def log_foundry_stop_signal(self, event: threading.Event | None) -> None:
                Chatty.calls += 1
                self._signal = event
                log_foundry.info("from the setter")

        sink = Chatty()
        if built:
            log_foundry.configure(service="t", sink_factory=lambda: sink)
        else:
            log_foundry.configure(service="t", sink=sink)
        log_foundry.info("x")
        absorbed = "log-foundry:" in captured.getvalue()
        return f"calls={Chatty.calls} events={len(sink.events)} absorbed={absorbed}"

    child = run_in_child(work)
    assert child.output == "calls=1 events=2 absorbed=False", child.output


# -- the factory wrapper --------------------------------------------------------------------------


def test_a_concurrent_first_emit_hands_the_built_sink_before_using_it() -> None:
    """A thread finding the built sink pending hands it the signal itself before emitting to it.

    The first emit builds A and parks in A's setter, with no lock held. A log on another thread
    finds A pending, not yet published, hands it the signal and only then emits. Publishing A
    before its hand let that log emit to a sink holding no signal; at ``624d817f`` it waited on the
    build lock instead.
    """

    def work() -> str:
        sink = Parked()
        log_foundry.configure(service="t", sink_factory=lambda: sink)
        first = _parked_in(lambda: log_foundry.info("first"), sink)
        raced = _finished(lambda: log_foundry.info("racing"))
        sink.release.set()
        first.join(GATE)
        return f"raced={raced} held={sink.held_at_emit[:1]}"

    child = run_in_child(work)
    assert child.output == "raced=True held=[True]", child.output


def test_closing_the_wrapper_before_its_hand_publishes_closes_the_built_sink() -> None:
    """``_FactorySink.close`` releases the pending sink, not only a published one."""
    built = Plain()
    wrapper = _FactorySink(lambda: built)
    _lifecycle.stamp(wrapper)
    _lifecycle.stamp(built)
    vars(wrapper)["_pending"] = built
    wrapper.close()
    assert built.closes == 1, "a close before the hand published left the built sink open"


def test_a_wrapper_replaced_while_its_factory_runs_still_closes_what_it_built() -> None:
    """A ``configure(sink=...)`` racing the factory call leaves the built sink closed at exit.

    The swap closes the wrapper while the factory is still running, when there is nothing built to
    release; the first log then builds A and writes to it. The wrapper's hand re-arms the wrapper,
    so the final ``shutdown()`` closes it and, through it, A (invariant 5). At ``624d817f`` A was
    written to and closed by nobody.
    """

    def work() -> str:
        a, b = Plain(), Plain()
        entered, release = threading.Event(), threading.Event()

        def factory() -> Plain:
            entered.set()
            release.wait(GATE)
            return a

        log_foundry.configure(service="t", sink_factory=factory)
        first = threading.Thread(target=lambda: log_foundry.info("x"), daemon=True)
        first.start()
        if not entered.wait(GATE):
            return "the factory never ran"
        log_foundry.configure(sink=b)
        release.set()
        first.join(GATE)
        log_foundry.shutdown(timeout=GATE)
        return _summary(a=a, b=b)

    child = run_in_child(work)
    assert child.output == "a=1/1 b=0/1", child.output


def test_the_worker_build_hands_the_sink_it_built_the_worker_on() -> None:
    """The build hands the sink its section adopted, even if the worker is retargeted first.

    The hand runs after the build's section releases the lock, so a retarget can land between
    them. Here one does, planted at the release itself: the state lock is wrapped so that the
    first release after the worker is published points that worker at B before the build goes
    on. The build must hand A, the sink it built the worker on and armed, A's own event
    (SPEC-098); re-reading ``worker.sink`` after the lock handed B instead and left A, owed and
    written to, with no signal. The seam is the release rather than the offer, because the
    build's argument to its offer is bound before any replacement of it runs.
    """

    def work() -> str:
        a, b = Hook(), Hook()
        log_foundry.configure(service="t", sink=a)
        _flush_quietly()
        state = _lifecycle._state
        real = state._lock
        verdicts: list[str] = []

        class RetargetOnRelease:
            def __enter__(self) -> bool:
                return bool(real.__enter__())

            def __exit__(self, *exc: object) -> None:
                real.__exit__(*exc)
                worker = state.worker_exists()
                if worker is not None and not verdicts:
                    verdicts.append(worker.retarget(b, None).verdict)  # type: ignore[arg-type]

        state._lock = RetargetOnRelease()  # type: ignore[assignment]
        try:
            _traced()()
        finally:
            state._lock = real
        own = state._signals.get(id(a))
        held = a.log_foundry_stop_signal
        return f"verdicts={verdicts} a_holds={own is not None and held is own[1]}"

    child = run_in_child(work)
    assert child.output == "verdicts=['fenced'] a_holds=True", child.output


# -- a worker built while a close of its sink is in flight -------------------------------------


class _Recording(Plain):
    """Records every offer and every close as ``(kind, event, set?, thread)``, in order.

    The first close parks on ``go`` once it has said so on ``in_close``, so a test can build the
    worker strictly inside it. With ``backoff`` each close then waits up to 4 s on the signal it
    holds, as a sink's close cut short by the stop signal does, and records whether that wait was
    cut short.
    """

    def __init__(self, *, backoff: bool) -> None:
        super().__init__()
        self.backoff = backoff
        self.in_close = threading.Event()
        self.go = threading.Event()
        self.log: list[tuple[str, object, bool, str]] = []
        self._signal: threading.Event | None = None

    @property
    def log_foundry_stop_signal(self) -> threading.Event | None:
        return self._signal

    @log_foundry_stop_signal.setter
    def log_foundry_stop_signal(self, event: threading.Event | None) -> None:
        assert event is not None
        self.log.append(("offer", event, event.is_set(), threading.current_thread().name))
        self._signal = event

    def close(self) -> None:
        if not self.closes:
            self.in_close.set()
            self.go.wait(GATE)
        held = self._signal
        assert held is not None
        was_set = held.is_set()
        started = time.monotonic()
        if self.backoff:
            from log_foundry.sinks import _retry

            _retry.wait(4.0, held)
        kind = "close" if time.monotonic() - started < 1.0 else "close-long"
        self.log.append((kind, held, was_set, threading.current_thread().name))
        super().close()

    def render(self) -> str:
        """The log as ``kind:event:set|unset:thread``, naming the first event E0, the worker's W1."""
        names: dict[int, str] = {}
        if self.log:
            names[id(self.log[0][1])] = "E0"
        worker = _lifecycle._state.worker_exists()
        if worker is not None:
            names.setdefault(id(worker._stop), "W1")
        return " ".join(
            f"raised:{getattr(event, '__name__', event)}:{thread}"
            if kind == "raised"
            else f"{kind}:{names.get(id(event), 'X')}:{'set' if is_set else 'unset'}:{thread}"
            for kind, event, is_set, thread in self.log
        )


def _built_during_a_close(site: str) -> str:
    """Builds the worker while a close of its sink is in flight, and reports what the sink saw.

    ``inline``: a no-worker ``shutdown(timeout=1.0)`` on the ``stopper`` thread closes the sink
    inline and a ``@trace`` builds the late worker inside that close. ``put_back``: that
    shutdown's take is interrupted, after the build, by a ``KeyboardInterrupt`` at the inline
    choice, so the entry is put back. ``refused``: the release is refused once, after the build,
    so the entry is discharged without a close. ``no_stop``: the close is a bare release on the
    ``closer`` thread with no ``shutdown()`` running.
    """
    sink = _Recording(backoff=site != "no_stop")
    log_foundry.configure(service="t", sink=sink)
    log_foundry.info("arms it and hands it E0")
    if site == "put_back":

        def interrupted(*args: object, **kwargs: object) -> object:
            sink.in_close.set()
            sink.go.wait(GATE)
            raise KeyboardInterrupt

        _lifecycle._inline_close_choice = interrupted  # type: ignore[assignment]
    elif site == "refused":
        real_releasable = _lifecycle.releasable
        refused: list[bool] = []

        def refuse_once(target: object, *, owner: object = None) -> bool:
            if target is sink and not refused:
                refused.append(True)
                sink.in_close.set()
                sink.go.wait(GATE)
                return False
            return real_releasable(target, owner=owner)

        _lifecycle.releasable = refuse_once  # type: ignore[assignment]
    took: list[float] = []

    def stop() -> None:
        started = time.monotonic()
        try:
            log_foundry.shutdown(timeout=1.0)
        except BaseException as exc:
            sink.log.append(("raised", type(exc), False, threading.current_thread().name))
        took.append(time.monotonic() - started)

    if site == "no_stop":
        closer = threading.Thread(target=lambda: _lifecycle.release(sink), name="closer")
    else:
        closer = threading.Thread(target=stop, name="stopper")
    closer.daemon = True
    closer.start()
    if not sink.in_close.wait(GATE):
        return "the close never started"
    _traced()()
    sink.go.set()
    closer.join(GATE * 2)
    quick = "" if not took else f" quick={took[0] < 1.0}"
    return sink.render() + quick


def test_a_worker_built_during_a_close_leaves_that_close_the_shutdowns_signal() -> None:
    """Invariant 3: a ``@trace`` landing in a ``shutdown()``'s close does not hold that close.

    The shutdown set E0, the signal the sink holds, and is closing the sink when a ``@trace``
    builds the late worker, whose ``Worker.__init__`` refreshes the set E0 to an unset W1. The
    build must not hand the sink W1 there: the close is already waiting on the sink's signal. At
    ``9f4d4dfd`` it did, on the main thread, and the close then waited out its whole 4 s backoff on
    W1, so ``shutdown(timeout=1)`` took 4.00 s, against 0.00 s for an ``info()`` in the same place.
    Since SPEC-098 E0 is the sink's own event and the build's offer finds the sink handed, so no
    hand can reach the close at all.
    """
    child = run_in_child(lambda: _built_during_a_close("inline"), timeout=30)
    first_close = child.output.split(" ")[1]
    assert first_close == "close:E0:set:stopper", child.output
    assert child.output.endswith("quick=True"), child.output


@pytest.mark.parametrize(
    ("site", "expected"),
    [
        (
            "inline",
            "offer:E0:unset:MainThread close:E0:set:stopper close:E0:set:stopper quick=True",
        ),
        ("put_back", "offer:E0:unset:MainThread raised:KeyboardInterrupt:stopper quick=True"),
        ("refused", "offer:E0:unset:MainThread close:E0:set:stopper quick=True"),
        ("no_stop", "offer:E0:unset:MainThread close:E0:unset:closer"),
    ],
    ids=["inline", "put_back", "refused", "no_stop"],
)
def test_a_build_during_a_close_hands_the_sink_nothing_again(site: str, expected: str) -> None:
    """A worker built while a close of its sink is in flight assigns the sink nothing (SPEC-098).

    Rewritten from four tests of the declined hand and its payment: the build used to hand the
    sink the worker's fresh event, so during a stop it declined, marked the close, and the close's
    discharge, a put-back or a refused release paid the hand afterwards; outside a stop it handed
    at once. The sink now holds its own event E0, handed at its first log, so the build's offer
    finds it handed and assigns nothing on every route: ``inline``, the stop's two closes both run
    on E0, which the stop's entry set; ``put_back``, the interrupted take; ``refused``, the release
    refused once before the second pass closes the sink; and ``no_stop``, a bare release with no
    stop running, where E0 stays unset. Each of the four used to read a second offer.
    """
    child = run_in_child(lambda: _built_during_a_close(site), timeout=30)
    assert child.output == expected, child.output


def test_a_hand_during_a_setters_own_shutdown_never_runs_the_setter_inside_itself() -> None:
    """A setter whose ``shutdown()`` closes its own sink is never run again on its own thread.

    Rewritten from ``test_a_paid_hand_never_runs_a_setter_inside_itself`` (SPEC-098). S's setter,
    on its first hand, calls ``shutdown()``, which closes S inline on the same thread while that
    hand is still running; a ``@trace`` on another thread builds the late worker inside the close.
    Before SPEC-098 the close's discharge paid the build's declined hand on the thread still inside
    S's setter, which must not nest the setter. No payment exists now, and the build's offer runs
    on the builder thread: S is not handed until the first setter returns, so that offer may run
    the setter concurrently, on another thread, which FR-001 allows. What this holds is the
    nesting: the depth is counted per thread and never exceeds one, and S ends on its own event,
    which the shutdown set.
    """

    def work() -> str:
        in_close, go = threading.Event(), threading.Event()
        local = threading.local()
        depth = [0]

        class Nesting(Plain):
            def __init__(self) -> None:
                super().__init__()
                self._signal: threading.Event | None = None
                self.fired = False

            @property
            def log_foundry_stop_signal(self) -> threading.Event | None:
                return self._signal

            @log_foundry_stop_signal.setter
            def log_foundry_stop_signal(self, event: threading.Event | None) -> None:
                local.depth = getattr(local, "depth", 0) + 1
                depth[0] = max(depth[0], local.depth)
                try:
                    self._signal = event
                    if not self.fired:
                        self.fired = True
                        log_foundry.shutdown(timeout=1.0)
                finally:
                    local.depth -= 1

            def close(self) -> None:
                if not self.closes:
                    in_close.set()
                    go.wait(GATE)
                super().close()

        sink = Nesting()
        log_foundry.configure(service="t", sink=sink)

        def build() -> None:
            in_close.wait(GATE)
            _traced()()
            go.set()

        builder = threading.Thread(target=build, daemon=True)
        builder.start()
        log_foundry.info("hands S, whose setter shuts down")
        builder.join(GATE)
        held = sink.log_foundry_stop_signal
        own = held is _lifecycle._state._signals[id(sink)][1]
        return f"deepest_on_a_thread={depth[0]} own={own} set={held is not None and held.is_set()}"

    child = run_in_child(work, timeout=30)
    assert child.output == "deepest_on_a_thread=1 own=True set=True", child.output


def test_a_build_inside_a_setters_hand_leaves_the_sink_its_own_set_event() -> None:
    """A late worker built inside S's own hand during a ``shutdown()`` delivers on S's set event.

    SPEC-098 FR-002, rewritten from ``..._still_marks_the_close``. A log's hand to S is running
    on the main thread and its setter is parked when a ``shutdown()`` on another thread takes S
    and starts closing it. The setter then runs a ``@trace``, which builds the late worker on S
    on the thread already offering S, so the build's offer is skipped and nothing is nested. S
    holds its own event, which the shutdown set: the late worker's delivery after that stop, with
    no ``configure()`` between, finds it set, the cost FR-002 accepts. Before SPEC-098 the build
    marked S's close and its discharge handed S the late worker's unset event.
    """

    def work() -> str:
        in_close, go, settled, entered = (threading.Event() for _ in range(4))
        first = [True]

        class Inside(Plain):
            def __init__(self) -> None:
                super().__init__()
                self._signal: threading.Event | None = None

            @property
            def log_foundry_stop_signal(self) -> threading.Event | None:
                return self._signal

            @log_foundry_stop_signal.setter
            def log_foundry_stop_signal(self, event: threading.Event | None) -> None:
                self._signal = event
                if first[0] and threading.current_thread() is threading.main_thread():
                    first[0] = False
                    entered.set()
                    in_close.wait(GATE)
                    _traced()()
                    go.set()

            def close(self) -> None:
                if not self.closes:
                    in_close.set()
                    go.wait(GATE)
                    settled.wait(GATE)
                super().close()

        sink = Inside()
        log_foundry.configure(service="t", sink=sink)

        def stop() -> None:
            entered.wait(GATE)
            log_foundry.shutdown(timeout=GATE)

        stopper = threading.Thread(target=stop, daemon=True)
        stopper.start()
        log_foundry.info("hands S; its setter builds inside a shutdown's close")
        settled.set()
        stopper.join(GATE * 2)
        held = sink.log_foundry_stop_signal
        own = held is _lifecycle._state._signals[id(sink)][1]
        return f"own={own} set={held is not None and held.is_set()}"

    child = run_in_child(work, timeout=30)
    assert child.output == "own=True set=True", child.output


def test_a_worker_built_in_a_stops_tail_delivers_on_the_set_event_until_a_configure() -> None:
    """A live worker built in a stop's tail delivers on S's set event; ``configure(S)`` clears it.

    SPEC-098 FR-002, rewritten from ``..._is_handed_its_signal_when_the_stop_ends``, and the one
    test that now holds the outcome six deleted tests of the declined-hand payment pinned
    (``waited=[0.3]``). S's close is a swap's, parked on its own thread. A ``shutdown()`` runs; in
    its tail, after it has lowered ``_shutdown_running``, a ``@trace`` builds a worker on S, live,
    which no stop will stop. S holds its own event, which the shutdown set, so that worker's
    deliveries after the stop, with no ``configure()`` between, back off 0.0 s: the cost FR-002
    accepts, where before SPEC-098 the last stop out handed S the live worker's unset event and
    the backoff waited 0.3 s. A ``configure()`` that installs S again, with no stop running, clears
    S's event in place and hands it again, and the next backoff waits 0.3 s.
    """

    def work() -> str:
        sink = _SwappedOut()
        _swapped_out_and_back(sink)
        real = _lifecycle._announce_unsettled

        def build_in_the_tail(op: object) -> None:
            _traced()()
            real(op)  # type: ignore[arg-type]

        _lifecycle._announce_unsettled = build_in_the_tail  # type: ignore[assignment]
        log_foundry.shutdown(timeout=1.0)
        _lifecycle._announce_unsettled = real
        live = _lifecycle._state.live_worker()
        held = sink.log_foundry_stop_signal
        own = held is _lifecycle._state._signals[id(sink)][1]
        late = f"live={live is not None and live.sink is sink} own={own}"
        in_flight = _lifecycle._state._closes.in_flight(sink)
        sink.waits.clear()
        _traced()()
        log_foundry.flush(timeout=GATE)
        after_stop = sink.waits[:1]
        log_foundry.configure(sink=sink)
        sink.waits.clear()
        _traced()()
        log_foundry.flush(timeout=GATE)
        sink.go.set()
        return (
            f"in_flight={in_flight} {late} waited_late={after_stop} "
            f"waited_reconfigured={sink.waits[:1]}"
        )

    child = run_in_child(work, timeout=40)
    assert child.output == (
        "in_flight=True live=True own=True waited_late=[0.0] waited_reconfigured=[0.3]"
    ), child.output


def test_a_build_inside_a_stops_second_pass_close_keeps_that_close_short() -> None:
    """A build inside a no-worker shutdown's second-pass close is deferred like one in its first.

    S's first close is a swap's, parked on its own thread, so the shutdown's first pass takes
    nothing and waits on it as a bystander; once it is released, the second pass — after
    ``_shutdown_running`` has dropped — takes S and closes it inline. A ``@trace`` inside that
    close builds a worker on S, and the build must still leave the close the shutdown's set
    signal, since a stop is running and is closing S. Gating the deferral on
    ``_shutdown_running`` instead handed the close the worker's fresh signal and it served its
    whole 4 s backoff: measured 4.92 s against 0.91 s.
    """

    def work() -> str:
        sink = _SwappedOut(build_in_second_close=True)
        _swapped_out_and_back(sink)

        def release_first_close() -> None:
            deadline = time.monotonic() + GATE
            while not _lifecycle._state._stops_running and time.monotonic() < deadline:
                time.sleep(0.005)
            time.sleep(0.2)
            sink.go.set()

        releaser = threading.Thread(target=release_first_close, daemon=True)
        releaser.start()
        started = time.monotonic()
        log_foundry.shutdown(timeout=6.0)
        elapsed = time.monotonic() - started
        releaser.join(GATE)
        return (
            f"shutdown_running={sink.second_close_shutdown_running} "
            f"second_close_held_set={sink.second_close_held_set} quick={elapsed < 2.0}"
        )

    child = run_in_child(work, timeout=40)
    expected = "shutdown_running=[0] second_close_held_set=[True] quick=True"
    assert child.output == expected, child.output


# -- a hand landing as a shutdown begins, before its take or into its close ---------------------


class _ParkedSetter(_Recording):
    """A ``_Recording`` whose setter parks on its ``park_on``-th call (0-based) until ``release``.

    The parked hand is made on a thread other than the test's; since SPEC-098 the sink is marked
    handed only once that setter returns. ``park_on`` defaults to the first call.
    """

    def __init__(self, *, park_on: int = 0) -> None:
        super().__init__(backoff=True)
        self.park_on = park_on
        self.parked = threading.Event()
        self.release = threading.Event()

    @property
    def log_foundry_stop_signal(self) -> threading.Event | None:
        return self._signal

    @log_foundry_stop_signal.setter
    def log_foundry_stop_signal(self, event: threading.Event | None) -> None:
        first = sum(entry[0] == "offer" for entry in self.log) == self.park_on
        assert event is not None
        self.log.append(("offer", event, event.is_set(), threading.current_thread().name))
        self._signal = event
        if first:
            self.parked.set()
            self.release.wait(GATE)


def _park(name: str, call: int) -> tuple[threading.Event, threading.Event]:
    """Parks the ``call``-th (0-based) call of ``_lifecycle.<name>``, after it runs or before.

    ``_await_swap_decisions`` parks after it returns, the moment a stop has set the signal and
    is about to take; ``_close_owed`` and ``_announce_unsettled`` park before they run.
    """
    entered, go = threading.Event(), threading.Event()
    real = getattr(_lifecycle, name)
    count = [0]

    def parked(*args: object, **kwargs: object) -> object:
        mine = count[0]
        count[0] += 1
        if name == "_await_swap_decisions":
            result = real(*args, **kwargs)
            if mine == call:
                entered.set()
                go.wait(GATE)
            return result
        if mine == call:
            entered.set()
            go.wait(GATE)
        return real(*args, **kwargs)

    setattr(_lifecycle, name, parked)
    return entered, go


def _handed_before_the_take(row: str) -> str:
    """Hands S its signal between a ``shutdown()`` setting the signal and its take of S.

    ``orphan``: a no-worker ``shutdown(timeout=1.0)`` on the ``stopper`` thread is parked after
    it has set the signal and awaited the swaps, before its first take, and an ``info()`` to S
    lands there. ``orphan-worker``: S is the worker's sink; the stop is parked after
    ``worker.stop``, before its first take. ``orphan-late``: a ``@trace`` builds a late worker on
    S while the stop is parked before its first take, which then skips S; the ``info()`` lands
    after that late worker's stop, before the second pass's take. ``flush-token``: another
    thread's hand to S is parked in S's setter, so its token is open, and a ``flush()`` lands
    before the first take. S's close waits up to 4 s on the signal it holds.
    """
    sink = _ParkedSetter() if row == "flush-token" else _Recording(backoff=True)
    sink.in_close.set()
    sink.go.set()
    log_foundry.configure(service="t", sink=sink)
    holder: threading.Thread | None = None
    if row == "orphan-worker":
        _traced()()
        log_foundry.flush(timeout=GATE)
        log_foundry.info("an orphan log with the worker live")
    elif row == "flush-token":
        holder = threading.Thread(
            target=lambda: log_foundry.info("arms it; its hand parks"), name="holder", daemon=True
        )
        holder.start()
        if not isinstance(sink, _ParkedSetter) or not sink.parked.wait(GATE):
            return "the first hand never parked"
    else:
        log_foundry.info("arms it and hands it E0")
    if row == "orphan-worker":
        entered, go = _park("_close_owed", 0)
    else:
        entered, go = _park("_await_swap_decisions", 0)
    took: list[float] = []

    def stop() -> None:
        started = time.monotonic()
        log_foundry.shutdown(timeout=1.0)
        took.append(time.monotonic() - started)

    stopper = threading.Thread(target=stop, name="stopper", daemon=True)
    stopper.start()
    if not entered.wait(GATE):
        return "the stop never parked"
    if row == "orphan-late":
        second, go_second = _park("_close_owed", 1)
        _traced()()
        go.set()
        if not second.wait(GATE):
            return "the second take never parked"
        go = go_second
    if row == "flush-token":
        log_foundry.flush(timeout=1.0)
    else:
        log_foundry.info("lands before the take")
    go.set()
    if isinstance(sink, _ParkedSetter):
        sink.release.set()
    if holder is not None:
        holder.join(GATE)
    stopper.join(GATE * 2)
    closes = [entry for entry in sink.render().split(" ") if entry.startswith("close")]
    return f"{closes[:1]} quick={bool(took) and took[0] < 1.0}"


_BEFORE_THE_TAKE = {
    "orphan": "['close:E0:set:stopper'] quick=True",
    "orphan-worker": "['close:E0:set:stopper'] quick=True",
    "orphan-late": "['close:E0:set:stopper'] quick=True",
    "flush-token": "['close:E0:set:stopper'] quick=True",
}
"""What S's first close held, by where the hand landed: always the signal the stop set.

``E0`` is the first event S was handed, S's own (SPEC-098), which the stop's entry sets. In the
``orphan-late`` row it used to be ``W1``, the late worker's own signal, handed to S by the log
after that worker's stop; since SPEC-098 no hand gives S another event, so S closes on E0.
"""


@pytest.mark.parametrize("row", list(_BEFORE_THE_TAKE))
def test_a_hand_before_the_take_leaves_the_close_the_shutdowns_signal(row: str) -> None:
    """Invariant 3: a log or ``flush()`` landing as a ``shutdown()`` begins does not hold its close.

    The stop sets the signal in its entry section and takes S later. A hand in between must leave
    S on the signal the stop set, not a fresh one: at ``a8e91fac`` the section refreshed the set
    signal and handed S the fresh unset event, and the take's close then waited its whole 4 s
    backoff on it. Since SPEC-098 S holds one event of its own, which the stop's entry sets, and
    a later hand assigns nothing.
    """
    child = run_in_child(lambda: _handed_before_the_take(row), timeout=30)
    assert child.output == _BEFORE_THE_TAKE[row], child.output


def test_an_orphan_log_after_the_last_take_finds_the_sinks_own_set_event() -> None:
    """After a stop's last take, an orphan log re-arms S on its own set event, until a configure.

    SPEC-098 FR-002, rewritten from ``..._is_handed_a_fresh_signal``. The no-worker
    ``shutdown()`` has closed S and made its last take, and is parked in its tail; an ``info()``
    re-arms S there. Before SPEC-098 that log refreshed the signal and handed S a fresh unset one.
    Now S keeps its own event, which the stop set, and the log assigns nothing: a delivery after a
    stop with no ``configure()`` between finds the event set, the cost FR-002 accepts (it
    supersedes SPEC-033 FR-004 AC-6's "the same sink logged to again"). A ``configure()`` that
    installs S again once the stop has returned clears it.
    """

    def work() -> str:
        sink = _Recording(backoff=False)
        sink.in_close.set()
        sink.go.set()
        log_foundry.configure(service="t", sink=sink)
        log_foundry.info("arms it and hands it E0")
        e0 = _lifecycle._state._signals[id(sink)][1]
        entered, go = _park("_announce_unsettled", 0)
        stopper = threading.Thread(target=lambda: log_foundry.shutdown(timeout=1.0), daemon=True)
        stopper.start()
        if not entered.wait(GATE):
            return "the stop never reached its tail"
        running = bool(_lifecycle._state._stops_running)
        log_foundry.info("re-arms it in the stop's tail")
        held = sink.log_foundry_stop_signal
        own = held is e0
        late_set = held is not None and held.is_set()
        go.set()
        stopper.join(GATE * 2)
        log_foundry.configure(sink=sink)
        cleared = not e0.is_set()
        return (
            f"closed={sink.closes} stop_running={running} own={own} set={late_set} "
            f"reconfigured_unset={cleared}"
        )

    child = run_in_child(work, timeout=30)
    assert child.output == (
        "closed=1 stop_running=True own=True set=True reconfigured_unset=True"
    ), child.output


def _stop_on(name: str, took: list[float]) -> threading.Thread:
    """Starts ``shutdown(timeout=1.0)`` on a thread called ``name``, recording how long it took."""

    def stop() -> None:
        started = time.monotonic()
        log_foundry.shutdown(timeout=1.0)
        took.append(time.monotonic() - started)

    thread = threading.Thread(target=stop, name=name, daemon=True)
    thread.start()
    return thread


def _closes_of(sink: _Recording, *, threads: bool = True) -> list[str]:
    """``sink``'s closes as :meth:`_Recording.render` names them, with or without the thread."""
    closes = [entry for entry in sink.render().split(" ") if entry.startswith("close")]
    return closes if threads else [entry.rsplit(":", 1)[0] for entry in closes]


def _late_build(row: str) -> str:
    """Builds a late worker while R is being handed, before a no-worker ``shutdown()`` takes R.

    Before SPEC-098 each route handed R the late worker's unset W1, as described below; since then
    R holds its own E0, which the stop set, and neither the settle nor the ``flush()`` hand exists.

    ``late-settle``: thread A's orphan log hands R its signal and R's setter parks on that first
    offer, of E0. A ``shutdown(timeout=1.0)`` on ``stopper`` sets E0 and is parked before its
    first take; ``configure(sink=S2)`` declines, since a take is ahead; a ``@trace`` builds the
    late worker on S2, whose build publishes its own unset event W1 as the current signal.
    Releasing R's setter, A's settle re-reads the signal and hands R W1. ``late-flush``: an
    ``info()`` hands R E0 before the stop; the ``@trace`` runs on thread B and S2's setter parks
    in the build's hand, so B's hand token is open, and a ``flush()`` hands owed R the current
    signal, W1. Either way R's close waits up to 4 s on the signal it holds.
    """
    r = _ParkedSetter() if row == "late-settle" else _Recording(backoff=True)
    s2 = _ParkedSetter() if row == "late-flush" else Plain()
    for sink in (r, s2):
        if isinstance(sink, _Recording):
            sink.in_close.set()
            sink.go.set()
    log_foundry.configure(service="t", sink=r)
    helper: threading.Thread | None = None
    if row == "late-settle":
        helper = threading.Thread(
            target=lambda: log_foundry.info("hands R E0; its setter parks"), name="A", daemon=True
        )
        helper.start()
        if not isinstance(r, _ParkedSetter) or not r.parked.wait(GATE):
            return "R's setter never parked"
    else:
        log_foundry.info("arms R and hands it E0")
    entered, go = _park("_await_swap_decisions", 0)
    took: list[float] = []
    stopper = _stop_on("stopper", took)
    if not entered.wait(GATE):
        return "the stop never parked"
    log_foundry.configure(sink=s2)
    if row == "late-settle":
        _traced()()
        if isinstance(r, _ParkedSetter):
            r.release.set()
    else:
        helper = threading.Thread(target=lambda: _traced()(), name="B", daemon=True)
        helper.start()
        if not isinstance(s2, _ParkedSetter) or not s2.parked.wait(GATE):
            return "the build's hand never parked"
        log_foundry.flush(timeout=1.0)
        s2.release.set()
    helper.join(GATE)
    late = _lifecycle._state._late_worker is not None
    go.set()
    stopper.join(GATE * 2)
    return f"late={late} {_closes_of(r)} quick={bool(took) and took[0] < 1.0}"


def _tail_build() -> str:
    """Builds a worker after a no-worker stop's registration window and before its final take.

    X is armed and swapped out for R, and X's close parks on the swap's detached thread, so R is
    owed and holds E0. A ``shutdown(timeout=1.0)`` on ``stopper`` closes R in its first pass and
    waits on X's close as a bystander; there, thread A's orphan log re-arms R and R's setter parks
    in that hand, and ``configure(sink=C)`` declines, a take being ahead. X's close is released,
    the first pass returns, the stop lowers ``_shutdown_running``, and it is parked before its
    final pass's take. A ``@trace`` there builds T on C: registered as nobody's late worker, so
    live, and its build publishes T's unset event W1. Before SPEC-098 R's setter parked on that
    re-arming hand and A's settle then handed R W1; since SPEC-098 the re-arm assigns R nothing, so
    R's second close runs on its own E0, set by the stop. R's second close waits up to 4 s on the
    signal it holds.
    """
    from log_foundry import worker as worker_mod

    worker_mod.DEFAULT_SWAP_TIMEOUT = 0.2
    x, r, c = _SwappedOut(), _ParkedSetter(park_on=1), Plain()
    r.in_close.set()
    r.go.set()
    log_foundry.configure(service="t", sink=x)
    log_foundry.info("arms X")
    log_foundry.configure(sink=r)
    if not x.in_close.wait(GATE):
        return "X's displaced close never parked"
    registry = _lifecycle._state._closes
    real_wait = registry.wait_graced
    waiting = threading.Event()

    def wait_graced(op: object, closes: object) -> None:
        if closes:
            waiting.set()
        real_wait(op, closes)  # type: ignore[arg-type]

    registry.wait_graced = wait_graced  # type: ignore[method-assign]
    final, go_final = _park("_close_owed", 1)
    took: list[float] = []
    stopper = _stop_on("stopper", took)
    if not waiting.wait(GATE):
        return "the first pass never waited on X"
    holder = threading.Thread(
        target=lambda: log_foundry.info("re-arms R; its setter parks"), name="A", daemon=True
    )
    holder.start()
    holder.join(GATE)
    log_foundry.configure(sink=c)
    x.go.set()
    if not final.wait(GATE):
        return "the final take never parked"
    running = _lifecycle._state._shutdown_running
    _traced()()
    tail = _lifecycle._state.worker_exists()
    late = _lifecycle._state._late_worker is not None
    r.release.set()
    holder.join(GATE)
    released = time.monotonic()
    go_final.set()
    stopper.join(GATE * 2)
    quick = bool(took) and time.monotonic() - released < 1.0
    stopped = (
        tail is not None and _lifecycle._state.live_worker() is None and not tail._thread.is_alive()
    )
    return (
        f"running={running} late={late} {_closes_of(r)} quick={quick} tail_worker_stopped={stopped}"
    )


def _two_stops() -> str:
    """Two no-worker stops meet one late worker, and both takes decline before either sets it.

    R's setter parks on thread A's first offer, of E0. Two ``shutdown(timeout=1.0)`` calls, on
    ``one`` and ``two``, set E0 and are parked before their first takes; ``configure(sink=S2)``
    declines and a ``@trace`` builds the late worker L on S2, whose unset event W1 becomes the
    current signal; before SPEC-098 A's settle then handed R W1, and since then R keeps its own E0,
    which both stops set. Both stops are released and each is parked again as
    its first ``_close_owed`` returns, so both have taken — or declined — before either reaches
    the section that sets W1. Then both go on.
    """
    r, s2 = _ParkedSetter(), Plain()
    r.in_close.set()
    r.go.set()
    log_foundry.configure(service="t", sink=r)
    helper = threading.Thread(
        target=lambda: log_foundry.info("hands R E0; its setter parks"), name="A", daemon=True
    )
    helper.start()
    if not r.parked.wait(GATE):
        return "R's setter never parked"
    names = ("one", "two")
    before = {name: threading.Event() for name in names}
    after = {name: threading.Event() for name in names}
    go_before, go_after = threading.Event(), threading.Event()
    real_await, real_close = _lifecycle._await_swap_decisions, _lifecycle._close_owed
    first_done: set[str] = set()

    def await_swaps(op: object, swaps: object) -> None:
        real_await(op, swaps)  # type: ignore[arg-type]
        before[threading.current_thread().name].set()
        go_before.wait(GATE)

    def close_owed(*args: object, **kwargs: object) -> bool:
        result = real_close(*args, **kwargs)  # type: ignore[arg-type]
        name = threading.current_thread().name
        if name in after and name not in first_done:
            first_done.add(name)
            after[name].set()
            go_after.wait(GATE)
        return result

    _lifecycle._await_swap_decisions = await_swaps  # type: ignore[assignment]
    _lifecycle._close_owed = close_owed  # type: ignore[assignment]
    took: list[float] = []
    stoppers = [_stop_on(name, took) for name in names]
    if not all(before[name].wait(GATE) for name in names):
        return "a stop never parked before its take"
    log_foundry.configure(sink=s2)
    _traced()()
    r.release.set()
    helper.join(GATE)
    go_before.set()
    if not all(after[name].wait(GATE) for name in names):
        return "a stop never returned from its first take"
    go_after.set()
    for stopper in stoppers:
        stopper.join(GATE * 2)
    quick = len(took) == 2 and max(took) < 1.0
    return (
        f"{_closes_of(r, threads=False)} quick={quick} retirements={_lifecycle._state.retirements}"
    )


_BUILT_SINCE_THE_ENTRY: dict[str, tuple[Callable[[], str], str]] = {
    "late-settle": (
        lambda: _late_build("late-settle"),
        "late=True ['close:E0:set:stopper'] quick=True",
    ),
    "late-flush": (
        lambda: _late_build("late-flush"),
        "late=True ['close:E0:set:stopper'] quick=True",
    ),
    "tail": (
        _tail_build,
        (
            "running=0 late=False ['close:E0:set:stopper', 'close:E0:set:stopper'] quick=True "
            "tail_worker_stopped=True"
        ),
    ),
    "two-stops": (_two_stops, "['close:E0:set'] quick=True retirements=3"),
}
"""What R's close held at its entry, by how the worker built since the stop's entry reached it.

``E0`` is the first event R was handed, R's own (SPEC-098), which the stop's entry set; each close
reads ``set``. Before SPEC-098 every row closed R on ``W1``, the worker's own event, which its build
published as the current signal and a hand delivered to R before the stop took R, and the take's
decline was what kept that close from waiting on W1 unset. No hand delivers W1 to any sink now,
and the take still declines, so the worker built since the entry is still stopped by the stop:
the tail row's ``tail_worker_stopped=True``. At ``864d71e5`` every row closed R on W1 unset and
waited out the 4 s backoff (``close-long:W1:unset``); the tail row's worker also stayed live, and
each stop in the two-stops row waited until the other set W1. The tail row's R setter used to park
on a second hand, which no longer runs.
"""


@pytest.mark.parametrize("row", list(_BUILT_SINCE_THE_ENTRY))
def test_a_stop_never_closes_on_the_unset_event_of_a_worker_built_since_its_entry(
    row: str,
) -> None:
    """Invariant 3: a worker built during a ``shutdown()`` never leaves that call's close waiting.

    A worker built after a stop's entry publishes its own unset event as the current signal.
    Before SPEC-098 any hand of the current signal — a settle still running across the stop's
    take, a ``flush()`` under the build's token — could deliver it to a sink that stop then
    closes; now each sink holds its own event, which the stop's entry set. The stop's take still
    declines while the worker's event is unset, sets it in the section that retires the worker,
    and takes then. What each row asserts is the event R's close held when it began and that it
    was set; the time is a second check. Measured at ``864d71e5``, each
    row held W1 unset and ``shutdown(timeout=1.0)`` took 4.0 s against R's 4 s backoff; in the
    two-stops row each stop took 1.0 s instead, R's close waiting until the other stop set W1 at
    its own deadline. The two-stops row also pins the retirement count at one per stop plus one
    for the late worker, though two stops now stop it.
    """
    work, expected = _BUILT_SINCE_THE_ENTRY[row]
    child = run_in_child(work, timeout=40)
    assert child.output == expected, child.output


def test_a_declined_take_closes_the_owed_sinks_while_the_worker_drains() -> None:
    """The stop takes again as soon as it has set the worker's event, before it joins the worker.

    R is owed and holds E0; a no-worker ``shutdown(timeout=1.0)`` is parked before its first
    take while a ``@trace`` builds the late worker on S2. The take declines on the worker's unset
    event, and the stop must close R while that worker drains rather than after its join: S2's
    ``emit``, which the late worker's final drain makes, waits up to 0.5 s for R's close and
    records whether it saw it. Joining before the retake made that emit wait the whole 0.5 s,
    and in the plan review's measurement put a 0.5 s close of R after the call's deadline. A
    mutant guard, green at ``864d71e5`` too, where the first pass took R before stopping the late
    worker at all.
    """

    def work() -> str:
        r = _Recording(backoff=False)
        r.in_close.set()
        r.go.set()
        closed, drained = threading.Event(), threading.Event()
        saw: list[bool] = []
        real_close = r.close

        def close() -> None:
            real_close()
            closed.set()

        r.close = close  # type: ignore[method-assign]

        class WaitsForR(Plain):
            def emit(self, batch: list[dict[str, object]]) -> None:
                drained.wait(GATE)
                saw.append(closed.wait(0.5))
                super().emit(batch)

        log_foundry.configure(service="t", sink=r)
        log_foundry.info("arms R and hands it E0")
        entered, go = _park("_await_swap_decisions", 0)
        took: list[float] = []
        stopper = _stop_on("stopper", took)
        if not entered.wait(GATE):
            return "the stop never parked"
        log_foundry.configure(sink=WaitsForR())
        _traced()()
        drained.set()
        go.set()
        stopper.join(GATE * 2)
        return f"saw_r_closed={saw[:1]} {_closes_of(r)} quick={bool(took) and took[0] < 1.0}"

    child = run_in_child(work, timeout=30)
    assert child.output == "saw_r_closed=[True] ['close:E0:set:stopper'] quick=True", child.output


def test_a_stop_keeps_its_take_ahead_through_the_join_of_a_worker_its_take_declined_for() -> None:
    """The retake after a decline is not the stop's last take, so the hands still refuse to refresh.

    R is owed and holds E0; a no-worker ``shutdown(timeout=1.0)`` is parked before its first
    take while a ``@trace`` builds the late worker on S2, so the take declines. The stop sets the
    worker's event, retakes R, and joins the worker, whose final drain is held in S2's ``emit``.
    There, ``configure(sink=Q)`` and an ``info()`` to Q land: the stop's final pass still takes Q,
    so Q must be handed the stop's set signal, not a refreshed one: ``E0`` names the first event Q
    was handed, which must be the current signal as the stop left it. The plan's prototype marked
    the call drained at the retake: during the join the swap went ahead and refreshed the signal,
    Q was handed the fresh unset event, and the final pass closed Q on it, a 4 s backoff waited
    in full and ``shutdown()`` taking 4.3 s, against 0.3 s at ``864d71e5``, where a late worker's
    join kept the take ahead. A mutant guard, green at ``864d71e5`` too. Since SPEC-098 Q's first
    hand during the stop creates Q's own event already set, so ``E0`` here is that set event and
    no refresh exists to race it; the row still pins the take-ahead bookkeeping.
    """

    def work() -> str:
        r, q = _Recording(backoff=False), _Recording(backoff=True)
        for sink in (r, q):
            sink.in_close.set()
            sink.go.set()
        inside, release = threading.Event(), threading.Event()

        class HeldInEmit(Plain):
            def emit(self, batch: list[dict[str, object]]) -> None:
                inside.set()
                release.wait(GATE)
                super().emit(batch)

        log_foundry.configure(service="t", sink=r)
        log_foundry.info("arms R and hands it E0")
        entered, go = _park("_await_swap_decisions", 0)
        took: list[float] = []
        stopper = _stop_on("stopper", took)
        if not entered.wait(GATE):
            return "the stop never parked"
        log_foundry.configure(sink=HeldInEmit())
        _traced()()
        go.set()
        if not inside.wait(GATE):
            return "the worker never drained"
        with _lifecycle._state._lock:
            ahead = _lifecycle._state._take_ahead()
        log_foundry.configure(sink=q)
        log_foundry.info("arms Q during the join")
        release.set()
        stopper.join(GATE * 2)
        closes = _closes_of(q, threads=False)
        return f"take_ahead={ahead} {closes} quick={bool(took) and took[0] < 1.0}"

    child = run_in_child(work, timeout=30)
    assert child.output == "take_ahead=True ['close:E0:set'] quick=True", child.output


def test_a_declined_stops_retake_waits_for_a_close_another_stop_started() -> None:
    """The retake after a decline waits on a close in flight, as a first pass does (invariant 5).

    R is owed and its ``close()`` takes 1 s. A no-worker ``shutdown(timeout=3.0)`` on ``one`` is
    parked before its first take while ``configure(sink=S2)`` and a ``@trace`` build the late
    worker; released, its take declines and it is parked again as that first ``_close_owed``
    returns. A second ``shutdown(timeout=3.0)`` on ``two`` then enters on the worker branch and
    starts closing R. ``one`` goes on: its retake finds R's close in flight, and must wait for
    it rather than return through it with R still open (SPEC-050 FR-002). At ``1e559923`` the
    retake did not wait: ``one`` returned before R's close finished. Green at ``864d71e5``, where
    ``one`` took R in its first pass, before any decline existed.
    """

    def work() -> str:
        closed_at: list[float] = []
        in_close = threading.Event()

        class SlowClose(Plain):
            def close(self) -> None:
                in_close.set()
                time.sleep(1.0)
                super().close()
                closed_at.append(time.monotonic())

        r = SlowClose()
        log_foundry.configure(service="t", sink=r)
        log_foundry.info("arms R")
        entered, go = _park("_await_swap_decisions", 0)
        real_close = _lifecycle._close_owed
        after, go_after = threading.Event(), threading.Event()
        first: list[bool] = []

        def close_owed(*args: object, **kwargs: object) -> bool:
            result = real_close(*args, **kwargs)  # type: ignore[arg-type]
            if threading.current_thread().name == "one" and not first:
                first.append(True)
                after.set()
                go_after.wait(GATE)
            return result

        _lifecycle._close_owed = close_owed  # type: ignore[assignment]
        returned: dict[str, float] = {}

        def stop(name: str) -> None:
            log_foundry.shutdown(timeout=3.0)
            returned[name] = time.monotonic()

        one = threading.Thread(target=stop, args=("one",), name="one", daemon=True)
        one.start()
        if not entered.wait(GATE):
            return "one never parked before its take"
        log_foundry.configure(sink=Plain())
        _traced()()
        go.set()
        if not after.wait(GATE):
            return "one's first take never returned"
        declined = _lifecycle._state._take_ahead()
        two = threading.Thread(target=stop, args=("two",), name="two", daemon=True)
        two.start()
        if not in_close.wait(GATE):
            return "two never started closing R"
        go_after.set()
        one.join(GATE * 2)
        two.join(GATE * 2)
        waited = bool(closed_at) and "one" in returned and returned["one"] >= closed_at[0]
        return (
            f"take_ahead_after_decline={declined} one_returned_after_r_closed={waited} "
            f"r_closes={r.closes}"
        )

    child = run_in_child(work, timeout=30)
    assert child.output == (
        "take_ahead_after_decline=True one_returned_after_r_closed=True r_closes=1"
    ), child.output


def test_a_late_workers_expiry_line_names_the_timeout_the_caller_gave() -> None:
    """A late worker's join that expires writes the caller's ``timeout``, as the worker path does.

    A no-worker ``shutdown(timeout=1.0)`` takes and closes the configured sink in its first pass;
    as that pass returns, ``configure(sink=S)`` and a ``@trace`` build the late worker on S, whose
    ``emit`` sleeps 3 s, so the stop's join of that worker expires at the call's deadline. The
    stop claims that worker in the section that retires it and joins with ``op.budget``, so the
    line reads ``after 1s``. At ``864d71e5`` the late worker claimed inside ``Worker.stop`` with
    what was left of the deadline, and the line named that remainder — ``after 0.998545s`` here,
    ``after 0.490264s`` in a probe that built the worker half a second later — which a caller
    cannot match to the timeout it passed; joining with ``_remaining(op.deadline)`` again reddens
    this (``after 0.998934s``).
    """

    def work() -> str:
        import re

        class Slow(Plain):
            def emit(self, batch: list[dict[str, object]]) -> None:
                time.sleep(3.0)
                super().emit(batch)

        log_foundry.configure(service="t", sink=Plain())
        log_foundry.info("arms the first sink")
        real_close = _lifecycle._close_owed
        built: list[bool] = []

        def close_owed(*args: object, **kwargs: object) -> bool:
            result = real_close(*args, **kwargs)  # type: ignore[arg-type]
            if not built:
                built.append(True)
                log_foundry.configure(sink=Slow())
                _traced()()
            return result

        _lifecycle._close_owed = close_owed  # type: ignore[assignment]
        captured = _stderr()
        log_foundry.shutdown(timeout=1.0)
        sys.stderr = sys.__stderr__
        found = re.findall(r"shutdown timed out after (\S+?);", captured.getvalue())
        return f"built={bool(built)} expiry={found}"

    child = run_in_child(work, timeout=30)
    assert child.output == "built=True expiry=['1s']", child.output


def test_a_stop_that_declined_keeps_its_take_ahead_after_the_stop_that_popped_its_worker_ends() -> (
    None
):
    """A declined stop with no late worker of its own is not drained when its first pass ends.

    Two no-worker ``shutdown(timeout=1.0)`` calls, on ``one`` and ``two``, are parked before
    their first takes while a ``@trace`` builds the late worker L; both are released and parked
    again as their first ``_close_owed`` returns, so both declined. ``one`` goes on alone: it pops
    L as its late worker, sets L's event, closes what is owed, joins L and ends. ``two`` then goes
    on and is parked before its retake: its own late worker was popped by ``one``, it waited on
    nothing, and its take declined, so a retake and a final pass are still ahead of it. There,
    ``configure(sink=Q)`` and an ``info()`` to Q land, and ``two``'s retake takes Q, so Q must
    hold the set signal: ``E0`` names the first event Q was handed. Marking ``two`` drained in its
    first section because it popped no late worker let the swap and the log refresh the signal,
    and the retake closed Q on that unset event, a 4 s backoff waited in full. A mutant guard
    for the decline's own bookkeeping, so it has no reading at ``864d71e5``: neither stop declined
    there, ``two`` made no second take to park before, and the row reports exactly that. Since
    SPEC-098 Q's own event is created set while a stop runs, so the refresh this guarded is gone
    and the row pins the decline's bookkeeping alone.
    """

    def work() -> str:
        q = _Recording(backoff=True)
        q.in_close.set()
        q.go.set()
        names = ("one", "two")
        before = {name: threading.Event() for name in names}
        after = {name: threading.Event() for name in names}
        go_before = threading.Event()
        go_after = {name: threading.Event() for name in names}
        retaking, go_retake = threading.Event(), threading.Event()
        real_await, real_close = _lifecycle._await_swap_decisions, _lifecycle._close_owed
        calls: dict[str, int] = {}

        def await_swaps(op: object, swaps: object) -> None:
            real_await(op, swaps)  # type: ignore[arg-type]
            before[threading.current_thread().name].set()
            go_before.wait(GATE)

        def close_owed(*args: object, **kwargs: object) -> bool:
            name = threading.current_thread().name
            calls[name] = calls.get(name, 0) + 1
            if name == "two" and calls[name] == 2:
                retaking.set()
                go_retake.wait(GATE)
            result = real_close(*args, **kwargs)  # type: ignore[arg-type]
            if name in after and calls[name] == 1:
                after[name].set()
                go_after[name].wait(GATE)
            return result

        log_foundry.configure(service="t", sink=Plain())
        log_foundry.info("arms the first sink")
        _lifecycle._await_swap_decisions = await_swaps  # type: ignore[assignment]
        _lifecycle._close_owed = close_owed  # type: ignore[assignment]
        took: dict[str, list[float]] = {name: [] for name in names}
        stoppers = {name: _stop_on(name, took[name]) for name in names}
        if not all(before[name].wait(GATE) for name in names):
            return "a stop never parked before its take"
        log_foundry.configure(sink=Plain())
        _traced()()
        go_before.set()
        if not all(after[name].wait(GATE) for name in names):
            return "a stop never returned from its first take"
        go_after["one"].set()
        stoppers["one"].join(GATE * 2)
        go_after["two"].set()
        if not retaking.wait(GATE):
            return "the second stop never reached its retake"
        with _lifecycle._state._lock:
            ahead = _lifecycle._state._take_ahead()
        log_foundry.configure(sink=q)
        log_foundry.info("arms Q before the retake")
        go_retake.set()
        stoppers["two"].join(GATE * 2)
        quick = bool(took["two"]) and took["two"][0] < 1.0
        return f"take_ahead={ahead} {_closes_of(q, threads=False)} quick={quick}"

    child = run_in_child(work, timeout=30)
    assert child.output == "take_ahead=True ['close:E0:set'] quick=True", child.output


def _retargeted_into_a_close(route: str) -> str:
    """Retargets a late worker onto S while a no-worker ``shutdown()`` is closing S inline.

    The ``stopper`` thread's ``shutdown(timeout=1.0)`` parks inside S's close. ``built-on-it``: a
    ``@trace`` builds the late worker on S, which marks the close (the build's own deferral), and
    ``configure(sink=T)`` then ``configure(sink=S)`` retargets it away and back.
    ``built-elsewhere``: ``configure(sink=T)`` first, so the ``@trace`` builds the late worker on T
    and marks nothing, and ``configure(sink=S)`` retargets it onto S, so only the retarget's hand
    can mark the close.
    The report is S's record (``_Recording.render``) and whether the shutdown took under 1 s.
    """
    sink, other = _Recording(backoff=True), Plain()
    log_foundry.configure(service="t", sink=sink)
    log_foundry.info("arms it and hands it E0")
    took: list[float] = []

    def stop() -> None:
        started = time.monotonic()
        log_foundry.shutdown(timeout=1.0)
        took.append(time.monotonic() - started)

    stopper = threading.Thread(target=stop, name="stopper", daemon=True)
    stopper.start()
    if not sink.in_close.wait(GATE):
        return "the close never started"
    if route == "built-on-it":
        _traced()()
        log_foundry.configure(sink=other)
    else:
        log_foundry.configure(sink=other)
        _traced()()
    log_foundry.configure(sink=sink)
    sink.go.set()
    stopper.join(GATE * 2)
    return f"{sink.render()} quick={bool(took) and took[0] < 1.0}"


@pytest.mark.parametrize("route", ["built-on-it", "built-elsewhere"])
def test_a_retarget_onto_a_sink_a_shutdown_is_closing_leaves_that_close_the_shutdowns_signal(
    route: str,
) -> None:
    """Invariant 3: a retarget onto S inside a ``shutdown()``'s close of S does not hold that close.

    The late worker is retargeted onto S while the stop is closing S, already waiting on S's
    signal E0, which the stop set. ``Worker.retarget`` must not hand S the worker's unset W1 there.
    At ``a8e91fac`` it did, on the main thread inside the close, and the close waited its whole
    4 s backoff on W1.
    """
    child = run_in_child(lambda: _retargeted_into_a_close(route), timeout=30)
    first_close = [entry for entry in child.output.split(" ") if entry.startswith("close")][:1]
    assert first_close == ["close:E0:set:stopper"], child.output
    assert child.output.endswith("quick=True"), child.output


def test_a_retarget_onto_a_sink_a_shutdown_is_closing_hands_it_nothing_again() -> None:
    """A retarget onto S inside a ``shutdown()``'s close of S assigns S nothing (SPEC-098).

    Rewritten from ``test_a_retarget_hand_declined_for_a_close_is_paid_when_it_finishes``. The
    late worker is built on T and ``configure(sink=S)`` retargets it onto S while the stop is
    closing S. That ``configure()`` runs while a stop runs, so it clears nothing, and S, handed
    its own E0 at its first log, is assigned nothing by the retarget's offer: both closes run on
    E0, which the stop set. Before SPEC-098 the retarget declined, marked S's close, and its
    discharge handed S the late worker's W1, on which the second pass then closed it.
    """
    child = run_in_child(lambda: _retargeted_into_a_close("built-elsewhere"), timeout=30)
    assert child.output == (
        "offer:E0:unset:MainThread close:E0:set:stopper close:E0:set:stopper quick=True"
    ), child.output


def test_a_retarget_with_no_stop_running_hands_at_once_during_a_close() -> None:
    """A retarget onto a sink whose bare release is in flight hands it its own event at once.

    A mutant guard, green at ``a8e91fac`` too. A bare release of S is parked on the ``closer``
    thread with no ``shutdown()`` running, so nothing will set S's signal and its close waits for
    none. ``configure(sink=S)`` retargets the live worker onto S and must hand S its signal at
    once, on the main thread, while that close is still parked: since SPEC-098, S's own event.
    Deferring outside a stop left S holding nothing until the close ended.
    """

    def work() -> str:
        sink = _Recording(backoff=False)
        log_foundry.configure(service="t", sink=Plain())
        _traced()()
        closer = threading.Thread(target=lambda: _lifecycle.release(sink), name="closer")
        closer.daemon = True
        closer.start()
        if not sink.in_close.wait(GATE):
            return "the close never started"
        log_foundry.configure(sink=sink)
        own = _lifecycle._state._signals.get(id(sink))
        holds = own is not None and sink.log_foundry_stop_signal is own[1]
        handed_on = [thread for kind, _, _, thread in sink.log if kind == "offer"]
        in_flight = _lifecycle._state._closes.in_flight(sink)
        sink.go.set()
        closer.join(GATE)
        return f"in_flight={in_flight} holds_now={holds} handed_on={handed_on}"

    child = run_in_child(work, timeout=30)
    assert child.output == "in_flight=True holds_now=True handed_on=['MainThread']", child.output


class _RetargetedInSecondClose(_SwappedOut):
    """A ``_SwappedOut`` whose second close has a tail worker retargeted onto it first.

    On another thread, inside the close: ``configure(sink=T)``, a ``@trace`` that builds a worker
    on T, then ``configure(sink=S)``, which retargets that worker onto this sink. The close then
    records whether the signal it holds is set and backs off up to 4 s on it.
    """

    def close(self) -> None:
        if not self.closes:
            self.in_close.set()
            self.go.wait(GATE * 2)
        else:
            from log_foundry.sinks import _retry

            self.second_close_shutdown_running.append(_lifecycle._state._shutdown_running)

            def retarget_onto_me() -> None:
                log_foundry.configure(sink=Plain())
                _traced()()
                log_foundry.configure(sink=self)

            mover = threading.Thread(target=retarget_onto_me, daemon=True)
            mover.start()
            mover.join(GATE)
            held = self._signal
            self.second_close_held_set.append(held is not None and held.is_set())
            _retry.wait(4.0, held)
        Plain.close(self)


def test_a_retarget_inside_a_stops_second_pass_close_keeps_that_close_short() -> None:
    """A retarget onto S inside a no-worker stop's second-pass close defers as in its first.

    S's first close is a swap's, parked on its own thread, so the shutdown's first pass takes
    nothing and waits on it; once it is released, the second pass, after ``_shutdown_running``
    has dropped, takes S and closes it inline. Inside that close a worker built on T in the
    stop's tail is retargeted onto S, and the hand must still leave the close the stop's set
    signal, since a stop is running and is closing S. At ``a8e91fac`` the retarget handed S the
    worker's fresh signal and the close served its whole 4 s backoff; gating the deferral on
    ``_shutdown_running`` rather than on the whole stop does the same.
    """

    def work() -> str:
        sink = _RetargetedInSecondClose()
        _swapped_out_and_back(sink)

        def release_first_close() -> None:
            deadline = time.monotonic() + GATE
            while not _lifecycle._state._stops_running and time.monotonic() < deadline:
                time.sleep(0.005)
            time.sleep(0.2)
            sink.go.set()

        releaser = threading.Thread(target=release_first_close, daemon=True)
        releaser.start()
        started = time.monotonic()
        log_foundry.shutdown(timeout=6.0)
        elapsed = time.monotonic() - started
        releaser.join(GATE)
        return (
            f"shutdown_running={sink.second_close_shutdown_running} "
            f"second_close_held_set={sink.second_close_held_set} quick={elapsed < 2.0}"
        )

    child = run_in_child(work, timeout=40)
    expected = "shutdown_running=[0] second_close_held_set=[True] quick=True"
    assert child.output == expected, child.output
