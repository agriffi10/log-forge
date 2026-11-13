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
    the worker on another thread first, then backs off up to 4 s on its signal.
    """

    def __init__(self, *, build_in_second_close: bool = False) -> None:
        super().__init__()
        self.build_in_second_close = build_in_second_close
        self.in_close = threading.Event()
        self.go = threading.Event()
        self.waits: list[float] = []
        self.second_close_held_set: list[bool] = []
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


def _build_in_the_tail(sink: _SwappedOut) -> None:
    """Runs a ``shutdown()`` whose tail builds a worker on ``sink``, whose swapped-out close runs on."""
    real = _lifecycle._announce_unsettled

    def build_in_the_tail(op: object) -> None:
        _traced()()
        real(op)  # type: ignore[arg-type]

    _lifecycle._announce_unsettled = build_in_the_tail  # type: ignore[assignment]
    try:
        log_foundry.shutdown(timeout=1.0)
    finally:
        _lifecycle._announce_unsettled = real


def _stop_end(act: str) -> Callable[[], str]:
    """A worker built in a stop's tail on S, whose close outlives the stop: the stop's end hands S.

    S's setter makes the call ``act`` names on that hand, which runs on the ``shutdown()``
    caller's thread after the stop's graced wait, with no lock held.
    """

    def work() -> str:
        sink, other = _SwappedOut(), Plain()
        _swapped_out_and_back(sink)
        sink.act = {**_acts(other), "trace": _traced()}[act]
        sink.armed = True
        report_from_child(ENTERED)
        _build_in_the_tail(sink)
        acted = not sink.armed
        sink.go.set()
        log_foundry.shutdown(timeout=GATE)
        return f"acted={acted} other={len(other.events)}/{other.closes}"

    return work


def _close_end(act: str) -> Callable[[], str]:
    """A worker built inside a ``shutdown()``'s inline close of S: that close's discharge hands S.

    S's setter makes the call ``act`` names on that hand, which runs on the ``shutdown()``'s own
    thread, inside that call, once the close is over.
    """

    def work() -> str:
        sink, other = _SwappedOut(), Plain()
        log_foundry.configure(service="t", sink=sink)
        log_foundry.info("arms it")
        stopper = threading.Thread(target=lambda: log_foundry.shutdown(timeout=GATE), daemon=True)
        stopper.start()
        if not sink.in_close.wait(GATE):
            return "the close never started"
        _traced()()
        sink.act = {**_acts(other), "trace": _traced()}[act]
        sink.armed = True
        report_from_child(ENTERED)
        sink.go.set()
        stopper.join(GATE * 2)
        acted = not sink.armed and not stopper.is_alive()
        log_foundry.shutdown(timeout=GATE)
        return f"acted={acted} other={len(other.events)}/{other.closes}"

    return work


_ROWS: dict[str, tuple[Callable[[], str], str]] = {
    "orphan-configure": (
        _orphan("configure"),
        "b_named=True flushed=[] calls=1 a=1/2 b=1/1",
    ),
    "orphan-info": (_orphan("info"), "b_named=False flushed=[] calls=1 a=3/1 b=0/0"),
    "orphan-flush": (_orphan("flush"), "b_named=False flushed=[True] calls=1 a=2/1 b=0/0"),
    "orphan-shutdown": (_orphan("shutdown"), "b_named=False flushed=[] calls=2 a=2/2 b=0/0"),
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
    "stop-end-configure": (_stop_end("configure"), "acted=True other=0/1"),
    "stop-end-info": (_stop_end("info"), "acted=True other=0/0"),
    "stop-end-flush": (_stop_end("flush"), "acted=True other=0/0"),
    "stop-end-shutdown": (_stop_end("shutdown"), "acted=True other=0/0"),
    "stop-end-trace": (_stop_end("trace"), "acted=True other=0/0"),
    "close-end-configure": (_close_end("configure"), "acted=True other=0/1"),
    "close-end-info": (_close_end("info"), "acted=True other=0/0"),
    "close-end-flush": (_close_end("flush"), "acted=True other=0/0"),
    "close-end-shutdown": (_close_end("shutdown"), "acted=True other=0/0"),
    "close-end-trace": (_close_end("trace"), "acted=True other=0/0"),
}
"""Each path that hands a signal, by the call its setter makes, and what the child reports.

The counts are ``events/closes`` per sink after the final ``shutdown()``. Where the setter swaps
the sink or shuts down during a log's hand, the log then writes to the sink the setter closed, so
that sink is closed again at the end (invariant 5): ``orphan-configure``, ``orphan-shutdown``,
``factory-orphan-configure`` and ``swap-noworker-shutdown`` (whose closing ``info("after")``
writes to A). The worker rows count ``span.start`` and ``span.end`` for each of two traced calls.
The orphan rows count the setter's calls: one hand each, the shutdown row's later log being handed
the refreshed signal a second time; ``orphan-trace`` builds the worker on A inside A's own hand,
and that build leaves the hand to the one already running, so A's setter runs once, not twice.
The swap rows say whether the setter's ``configure()`` wrote the re-entrant ``pending`` line: the
no-worker swap hands its sink inside ``config._sink_lock``, so a ``configure()`` from that setter
is the re-entrant kind, which tries the lock once, swaps unconfirmed and announces it.
The ``stop-end`` rows hand it in the last stop's payment of a hand a worker build in that stop's
tail declined, on the ``shutdown()`` caller's thread after its graced wait; the ``close-end`` rows
in a close's discharge paying one a build declined inside a ``shutdown()``'s inline close, on that
shutdown's own thread. In both, the setter's ``configure()`` closes the sink it installs at the
final ``shutdown()``, with nothing written to it. The ``fork-rebuild`` rows hand the signal in a forked child's worker rebuild, which runs in the
fork handler with the repaired locks free, and the ``factory-forward`` rows hand it through the
factory wrapper's forward to its published sink, from the orphan log's hand; each only has to
return. The retarget rows returned at ``624d817f`` too: ``Worker.retarget`` already handed outside every
lock a re-entrant ``configure()`` waits on, and they pin the precedent the fix generalises.
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
    """A section opens a hand token only when it published something, so hands do not cascade.

    While a token is open every orphan log takes the locked branch and hands its sink the signal
    before emitting. If that hand opened a token of its own, the next log would meet it and hand
    again, and so on for as long as logging continued. With a token per hand this case ran the
    setter 2399 times for its 2400 logs; with tokens opened only on a publication, 8 or 9 times in
    five runs. The bound, a quarter of the logs, sits far from both. ``624d817f`` had no tokens,
    so this guards the mechanism rather than the old defect.
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


def test_a_worker_build_that_fails_leaves_no_hand_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """The build's ``try`` covers its whole section, so a ``Thread.start()`` that raises discards.

    A token left open sends every orphan log in the process to the locked branch for good. The
    assertion is made before the test ends, so the suite's own reset cannot be what passes it.
    """
    real = threading.Thread.start

    def refuse(self: threading.Thread) -> None:
        if self.name == "log-foundry-worker":
            raise RuntimeError("can't start new thread")
        real(self)

    log_foundry.configure(service="t", sink=Hook())
    monkeypatch.setattr(threading.Thread, "start", refuse)
    _traced()()
    assert _lifecycle._state.worker_exists() is None, "the build was meant to fail"
    assert not _lifecycle._state._hands_running, "a failed build left its hand token open"


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


def test_a_late_hand_never_leaves_a_sink_on_a_stale_event() -> None:
    """A hand that lands after a newer one is settled on the process's current signal.

    One thread decides on E and parks in the setter; the signal is set, as a shutdown sets it;
    another log refreshes to F and hands it. When the first hand finally stores the set E, its
    re-check finds the process signal moved and hands F again. Without the re-check the sink holds
    the set E while the fast path, seeing F unset, never refreshes it: SPEC-033 FR-004's tight
    retry loop. At ``624d817f`` the second log could not run at all while the first was parked.
    """

    def work() -> str:
        sink = Parked()
        log_foundry.configure(service="t", sink=sink)
        first = _parked_in(lambda: log_foundry.info("first"), sink)
        _lifecycle._state._stop.set()
        raced = _finished(lambda: log_foundry.info("second"))
        sink.release.set()
        first.join(GATE)
        held = sink.log_foundry_stop_signal
        current = _lifecycle._state._stop
        return f"raced={raced} current={held is current} set={held is not None and held.is_set()}"

    child = run_in_child(work)
    assert child.output == "raced=True current=True set=False", child.output


@pytest.mark.parametrize("path", ["orphan", "swap", "worker"])
def test_another_thread_hands_the_signal_before_it_emits(path: str) -> None:
    """A section that armed a sink opens a token first, so no thread emits to it unsignalled.

    The first hand is parked in the setter after its section armed the sink: a first log, a
    no-worker swap, or the worker build. A log on another thread meets the open token, takes the
    locked branch and hands the sink its signal itself, so the sink holds one when that log's
    event reaches it. Without the token in the fast path, that log emitted to a sink holding none.
    At ``624d817f`` the second log waited on the lock the parked setter held (``orphan``,
    ``worker``), or, after the swap, took the fast path to a sink that held no signal yet
    (``swap``), since that swap armed the sink before it offered.

    The child's fork handler leaves it a signal debt, which also sends the racing log to the
    locked branch; a ``flush()`` settles it first, or the ``worker`` row would pass with the
    token's read removed from the fast path.
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

    One thread is parked in the first hand to S. A ``flush()`` on another thread meets the open
    token and hands S the signal before calling S's own ``flush`` (invariant 6). Without that
    step the flush reached S holding no signal. At ``624d817f`` the flush waited on the lock the
    parked setter held.
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


def test_a_flush_settling_a_debt_holds_others_off_until_its_hand_lands() -> None:
    """A ``flush()`` whose section clears a signal debt opens a token before it hands.

    S holds a set signal and a debt is owed, as after a fork. A ``flush()`` on one thread replaces
    the signal, clears the debt and parks in S's setter before S stores the new one. A log on
    another thread then sees a fresh unset signal and no debt; only the flush's token sends it to
    the locked branch, where it hands S the signal itself. Without that token it took the fast path
    and emitted to S still holding the set signal. At ``624d817f`` the log waited on the lock the
    parked flush held.
    """

    def work() -> str:
        sink = Parked()
        log_foundry.configure(service="t", sink=sink)
        _flush_quietly()
        log_foundry.info("arms it")
        _lifecycle._state._stop.set()
        _lifecycle._state._signal_owed = True
        sink.held_at_emit.clear()
        first = _parked_in(_flush_quietly, sink)
        raced = _finished(lambda: log_foundry.info("racing"))
        sink.release.set()
        first.join(GATE)
        return f"raced={raced} held={sink.held_at_emit[:1]}"

    child = run_in_child(work)
    assert child.output == "raced=True held=[True]", child.output


def test_a_settle_hands_the_current_signal_and_never_creates_one() -> None:
    """A re-check that finds the signal moved hands the current one, set or not, and makes none.

    S's setter, on its first hand, sets the process signal and refreshes it to a new one, E1,
    which it sets too, as a shutdown racing the hand would leave it. The re-check finds the
    signal moved and hands S the current E1, set; it does not refresh it into a fresh event of
    its own, which is what gave a sink an unset event inside a shutdown's close. The next log,
    meeting the set signal, refreshes it through the locked branch and S ends on an unset one.
    It reaches into the signal the setter moves, so at ``624d817f``, where the offer ran under the
    lock the setter takes, it deadlocked.
    """

    def work() -> str:
        state = _lifecycle._state
        moved: list[threading.Event] = []

        class Mover(Plain):
            _signal: threading.Event | None = None

            @property
            def log_foundry_stop_signal(self) -> threading.Event | None:
                return self._signal

            @log_foundry_stop_signal.setter
            def log_foundry_stop_signal(self, event: threading.Event | None) -> None:
                if not moved:
                    with state._lock:
                        state._stop.set()
                        moved.append(state.refresh_stop_signal())
                        moved[0].set()
                self._signal = event

        sink = Mover()
        log_foundry.configure(service="t", sink=sink)
        _flush_quietly()
        log_foundry.info("first")
        made_none = state._stop is moved[0]
        holds_current = sink.log_foundry_stop_signal is moved[0]
        log_foundry.info("next")
        settled = sink.log_foundry_stop_signal is state._stop and _unset(
            sink.log_foundry_stop_signal
        )
        return f"made_none={made_none} holds_current={holds_current} next_log_settles={settled}"

    child = run_in_child(work)
    assert child.output == "made_none=True holds_current=True next_log_settles=True", child.output


def test_a_late_hand_inside_a_shutdown_does_not_hold_up_its_close() -> None:
    """A late hand landing between a shutdown's set and its close leaves the close cut short.

    A thread's hand is parked in S's setter holding the then-current F. Later logs move the
    process to G. A ``shutdown()`` sets G and, before its close takes S, the parked hand resumes
    and stores F; its re-check finds the signal moved and hands S the current G, which is set, so
    S's close, which backs off 8 s on its signal, returns at once. A re-check that refreshed
    handed S a fresh unset event there instead, and the ``shutdown(timeout=2)`` took 8.00 s. At
    ``624d817f`` the parked hand held the lock the shutdowns need.
    """

    def work() -> str:
        from log_foundry.sinks import _retry

        reached, go, a_done = threading.Event(), threading.Event(), threading.Event()

        class Slow:
            def __init__(self) -> None:
                self._signal: threading.Event | None = None
                self.park = False

            def emit(self, batch: list[dict[str, object]]) -> None:
                pass

            def close(self) -> None:
                _retry.wait(8.0, self._signal)

            @property
            def log_foundry_stop_signal(self) -> threading.Event | None:
                return self._signal

            @log_foundry_stop_signal.setter
            def log_foundry_stop_signal(self, event: threading.Event | None) -> None:
                if self.park and threading.current_thread().name == "late":
                    self.park = False
                    reached.set()
                    go.wait(GATE * 2)
                self._signal = event

        sink = Slow()
        log_foundry.configure(service="t", sink=sink)
        log_foundry.info("one")
        log_foundry.shutdown(timeout=GATE)
        sink.park = True
        late = threading.Thread(
            target=lambda: (log_foundry.info("late"), a_done.set()), name="late", daemon=True
        )
        late.start()
        if not reached.wait(GATE):
            return "the late hand never parked"
        log_foundry.shutdown(timeout=GATE)
        log_foundry.info("re-arms with a fresh signal")
        real = _lifecycle._close_owed

        def resume_first(*args: object, **kwargs: object) -> bool:
            go.set()
            a_done.wait(GATE)
            return real(*args, **kwargs)  # type: ignore[arg-type]

        _lifecycle._close_owed = resume_first  # type: ignore[assignment]
        started = time.monotonic()
        log_foundry.shutdown(timeout=2.0)
        elapsed = time.monotonic() - started
        _lifecycle._close_owed = real
        return f"within_budget={elapsed < 3.0}"

    child = run_in_child(work, timeout=30)
    assert child.output == "within_budget=True", child.output


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


def test_a_late_hand_after_a_fresh_worker_still_leaves_the_current_signal() -> None:
    """A late orphan hand landing after a shutdown and a new worker's hand is settled too.

    T1's first log parks in S's setter holding E; ``shutdown()`` sets E; a ``@trace`` builds a
    fresh worker on S, whose hand gives S the new, unset F. When T1 stores the set E, its re-check
    finds the signal moved, and the new worker draining into S is no reason to stop: it hands S
    F, the worker's own current event. A bare ``in_flight`` bail-out returned there and left S on
    E for good, since S is armed and F unset, so the fast path never repaired it — measured, a
    0.5 s backoff on S's signal then waited 0.000 s (SPEC-033 FR-004). At ``624d817f`` the shutdown
    waited on the lock the parked setter held.
    """

    def work() -> str:
        sink = Parked()
        log_foundry.configure(service="t", sink=sink)
        _flush_quietly()
        first = _parked_in(lambda: log_foundry.info("first"), sink)
        if not _finished(lambda: log_foundry.shutdown(timeout=2.0)):
            return "the shutdown waited on the parked setter"
        _traced()()
        sink.release.set()
        first.join(GATE)
        log_foundry.info("after")
        held = sink.log_foundry_stop_signal
        current = _lifecycle._state._stop
        return f"current={held is current} unset={_unset(held)}"

    child = run_in_child(work)
    assert child.output == "current=True unset=True", child.output


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

    Configured directly, the log inside the setter meets this thread's own open token and takes
    the locked branch, where ``_lifecycle.handing_here`` skips the sink this thread is already
    handing. Built by ``sink_factory=``, the log reaches the wrapper, which finds the built sink
    pending and, through the same test, delivers to it without handing it again. Without either
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


def test_a_setter_that_keeps_moving_the_signal_is_deferred_not_looped() -> None:
    """A hand that cannot settle stops after ``_SETTLE_ROUNDS`` and leaves the rest owed.

    The setter sets and refreshes the process signal on every call, so each offer is stale by
    the time it is checked. The hand gives up after the cap with the debt flag set, and the next
    log hands the sink the current, unset signal. Without the cap the child never returns.
    """

    def work() -> str:
        class Mover(Plain):
            calls = 0
            moving = True
            _signal: threading.Event | None = None

            @property
            def log_foundry_stop_signal(self) -> threading.Event | None:
                return self._signal

            @log_foundry_stop_signal.setter
            def log_foundry_stop_signal(self, event: threading.Event | None) -> None:
                self._signal = event
                if Mover.moving:
                    Mover.calls += 1
                    with _lifecycle._state._lock:
                        _lifecycle._state._stop.set()
                        _lifecycle._state.refresh_stop_signal()

        sink = Mover()
        log_foundry.configure(service="t", sink=sink)
        log_foundry.info("x")
        owed = _lifecycle._state._signal_owed
        Mover.moving = False
        log_foundry.info("y")
        held = sink.log_foundry_stop_signal
        settled = held is _lifecycle._state._stop and held is not None and not held.is_set()
        return f"calls={Mover.calls} owed={owed} settled={settled}"

    child = run_in_child(work)
    assert child.output == f"calls={_lifecycle._SETTLE_ROUNDS} owed=True settled=True", child.output


def test_a_setter_that_forks_leaves_the_child_its_own_signal() -> None:
    """A setter forking during a debt's hand leaves the grandchild owing, and handing, its own.

    The debt flag is cleared in the deciding section, before the fork, so the grandchild's own
    fork handler sets it and nothing on the continuing thread clears it again; the continuing
    hand finds the process signal is the grandchild's fresh one and hands that. At ``624d817f``
    the continuation cleared the grandchild's flag after the setter returned, so the debt the
    grandchild's own fork handler had just recorded was dropped.
    """

    def work() -> str:
        read_fd, write_fd = os.pipe()
        parent = os.getpid()

        class Forker(Plain):
            arm = False
            _signal: threading.Event | None = None

            @property
            def log_foundry_stop_signal(self) -> threading.Event | None:
                return self._signal

            @log_foundry_stop_signal.setter
            def log_foundry_stop_signal(self, event: threading.Event | None) -> None:
                self._signal = event
                if Forker.arm:
                    Forker.arm = False
                    gc.collect()
                    os.fork()

        sink = Forker()
        log_foundry.configure(service="t", sink=sink)
        log_foundry.info("arms it")
        Forker.arm = True
        _lifecycle._state._signal_owed = True
        log_foundry.info("the debt's hand forks")
        if os.getpid() != parent:
            held = sink.log_foundry_stop_signal
            line = f"owed={_lifecycle._state._signal_owed} own={held is _lifecycle._state._stop}"
            os.write(write_fd, line.encode())
            os._exit(0)
        os.close(write_fd)
        ready = select.select([read_fd], [], [], GATE)[0]
        line = os.read(read_fd, 256).decode() if ready else "silent"
        os.wait()
        return line

    child = run_in_child(work)
    assert child.output == "owed=True own=True", child.output


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


def test_a_signal_handed_to_the_wrapper_mid_build_reaches_the_built_sink() -> None:
    """A signal the wrapper is handed while the built sink is unpublished still reaches it.

    The first build's hand parks holding E; the signal is set and another log hands the wrapper
    a fresh F, which forwards to nothing because nothing is published yet. When the first hand
    resumes and stores the set E, it reads the wrapper's signal again and hands F. At
    ``624d817f`` the built sink kept the set E.
    """

    def work() -> str:
        sink = Parked()
        log_foundry.configure(service="t", sink_factory=lambda: sink)
        first = _parked_in(lambda: log_foundry.info("first"), sink)
        _lifecycle._state._stop.set()
        _finished(lambda: log_foundry.info("second"))
        sink.release.set()
        first.join(GATE)
        held = sink.log_foundry_stop_signal
        current = _lifecycle._state._stop
        return f"current={held is current} set={held is not None and held.is_set()}"

    child = run_in_child(work)
    assert child.output == "current=True set=False", child.output


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
    on. The build must hand A, the sink it built the worker on and armed; re-reading
    ``worker.sink`` after the lock handed B instead and left A, owed and written to, with no
    signal. The seam is the release rather than ``_hand_out``, because the build's arguments to
    ``_hand_out`` are evaluated before any replacement of it runs.
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
        worker = state.worker_exists()
        stop = None if worker is None else worker._stop
        return f"verdicts={verdicts} a_holds={a.log_foundry_stop_signal is stop}"

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
    """
    child = run_in_child(lambda: _built_during_a_close("inline"), timeout=30)
    first_close = child.output.split(" ")[1]
    assert first_close == "close:E0:set:stopper", child.output
    assert child.output.endswith("quick=True"), child.output


def test_the_hand_a_build_declined_is_paid_when_the_close_finishes() -> None:
    """The close that the build declined for hands the sink the late worker's signal, after it.

    Declining alone left the late worker delivering into a sink on the shutdown's set E0, whose
    backoff then collapses to nothing (SPEC-033 FR-004). So the discharge of that close hands the
    sink W1, the current signal and unset, on the closing thread once the close is over; the
    shutdown's second pass then stops the late worker, setting W1, and closes the sink on it. At
    ``9f4d4dfd`` the build handed W1 itself, on the main thread and inside the close.
    """
    child = run_in_child(lambda: _built_during_a_close("inline"), timeout=30)
    assert child.output == (
        "offer:E0:unset:MainThread close:E0:set:stopper offer:W1:unset:stopper "
        "close:W1:set:stopper quick=True"
    ), child.output


def test_a_put_back_pays_the_hand_its_entry_carried() -> None:
    """An interrupted take puts the entry back, and pays the hand the build declined on it.

    The interrupt lands after the build, so the sink is never closed and is still on the set E0;
    the late worker is never stopped either, since the interrupt leaves the call. ``put_back``
    runs under the state lock, so the payment waits for the lock to be released and the fan-out
    joined, and then hands W1 on the stopper thread, before the interrupt reaches the caller. At
    ``9f4d4dfd`` the build handed W1 on the main thread instead.
    """
    child = run_in_child(lambda: _built_during_a_close("put_back"), timeout=30)
    assert child.output == (
        "offer:E0:unset:MainThread offer:W1:unset:stopper raised:KeyboardInterrupt:stopper quick=True"
    ), child.output


def test_a_refused_release_pays_the_hand_its_entry_carried() -> None:
    """A release refused after the build discharges the entry with no close, and still pays.

    The refusal is planted once, as for a sink a forked child did not acquire; the payment hands
    W1 on the stopper thread, and the second pass, released normally, closes the sink on W1 once
    the late worker's stop has set it. At ``9f4d4dfd`` the build handed W1 on the main thread.
    """
    child = run_in_child(lambda: _built_during_a_close("refused"), timeout=30)
    assert child.output == (
        "offer:E0:unset:MainThread offer:W1:unset:stopper close:W1:set:stopper quick=True"
    ), child.output


def test_a_build_with_no_stop_running_hands_at_once_during_a_close() -> None:
    """The decline is limited to a stop in progress; any other close in flight keeps the old hand.

    A bare release of the sink is in flight on the ``closer`` thread with no ``shutdown()``
    running, so nothing will set the sink's signal and the close is not waiting for one. The
    build hands the sink its signal at once, on the main thread, inside the close, exactly as at
    ``9f4d4dfd``. The worker's signal is E0 itself here, since nothing set it.
    """
    child = run_in_child(lambda: _built_during_a_close("no_stop"), timeout=30)
    assert child.output == (
        "offer:E0:unset:MainThread offer:E0:unset:MainThread close:E0:unset:closer"
    ), child.output


def test_a_paid_hand_never_runs_a_setter_inside_itself() -> None:
    """The payment asks ``handing_here`` like every decision, so a setter is never nested.

    S's setter, on its first hand, calls ``shutdown()``, which closes S inline on the same thread
    while that hand is still running; a ``@trace`` on another thread builds the late worker inside
    the close, and the build declines. When the close is discharged, the payment runs on the
    thread still inside S's setter, so it must leave the hand to the settle already running there
    rather than run the setter again inside itself. That settle then finds the signal moved and
    hands S the late worker's W1. Without the test the payment re-entered the setter one level
    deeper.
    """

    def work() -> str:
        in_close, go = threading.Event(), threading.Event()
        depth = [0, 0]

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
                depth[0] += 1
                depth[1] = max(depth[1], depth[0])
                try:
                    self._signal = event
                    if not self.fired:
                        self.fired = True
                        log_foundry.shutdown(timeout=1.0)
                finally:
                    depth[0] -= 1

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
        worker = _lifecycle._state.worker_exists()
        holds = worker is not None and sink.log_foundry_stop_signal is worker._stop
        return f"deepest={depth[1]} holds_late={holds}"

    child = run_in_child(work, timeout=30)
    assert child.output == "deepest=1 holds_late=True", child.output


def test_a_paid_hand_delivers_the_current_signal_and_never_refreshes_it() -> None:
    """The payment hands ``_state._stop`` as it is, set or not, and never replaces it.

    Driven at the discharge itself: S holds E0, a close of S is registered and marked as owing a
    hand, as a build during a stop marks it, and E0 is set, as a second ``shutdown()`` entering
    while that close runs sets it. Discharging the close through ``release`` pays the hand. It
    must hand S that very set E0 and leave the process signal alone. A payment that refreshed
    would hand S a fresh unset event; on the route where a second shutdown then closes S, that
    close serves the sink's whole backoff, which is the class SPEC-044 FR-003 closed.
    """

    def work() -> str:
        sink = _Recording(backoff=False)
        sink.in_close.set()
        sink.go.set()
        log_foundry.configure(service="t", sink=sink)
        log_foundry.info("arms it and hands it E0")
        state = _lifecycle._state
        e0 = state._stop
        entry = state._closes.register(sink, _lifecycle._Rule.INLINE)
        marked = state._closes.defer_hand(sink)
        e0.set()
        _lifecycle.release(sink, closing=entry)
        held = sink.log_foundry_stop_signal
        return (
            f"marked={marked} holds_e0={held is e0} set={held is not None and held.is_set()} "
            f"signal_unchanged={state._stop is e0}"
        )

    child = run_in_child(work)
    assert child.output == "marked=True holds_e0=True set=True signal_unchanged=True", child.output


def test_a_build_inside_a_setters_hand_still_marks_the_close() -> None:
    """The mark is asked before ``handing_here``, so a build inside S's own hand still marks.

    A log's hand to S is running on the main thread and its setter is parked when a
    ``shutdown()`` on another thread takes S and starts closing it. The setter then runs a
    ``@trace``, which builds the late worker on S on the thread already handing S. The hand
    already running is no help here: its settle finds the signal moved and S's close in flight,
    and leaves S alone, and the close is held until it has. So only the mark the build makes
    gets S the late worker's signal, paid when the close ends. Asked after ``handing_here``, the
    build marked nothing and S was left on the shutdown's set signal.
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
        worker = _lifecycle._state.worker_exists()
        holds = worker is not None and sink.log_foundry_stop_signal is worker._stop
        return f"holds_late={holds}"

    child = run_in_child(work, timeout=30)
    assert child.output == "holds_late=True", child.output


def test_a_worker_built_in_a_stops_tail_is_handed_its_signal_when_the_stop_ends() -> None:
    """A build deferred in a stop's tail is paid by the last stop out, not by a close it outlives.

    S's close is a swap's, parked on its own thread. A ``shutdown()`` runs; in its tail, after it
    has lowered ``_shutdown_running``, a ``@trace`` builds a worker on S. That worker is live, and
    no stop will stop it. The build defers, since a stop is running and S's close is in flight;
    but that close outlives the stop, so the hand cannot wait for it. When the stop ends it hands
    S the live worker's own unset signal, while S's close is still parked, and S's backoffs wait
    again. Without that, S stayed on the shutdown's set signal until the close ended, and three
    0.3 s backoffs waited nothing — against 0.3 s each before the deferral existed.
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
        holds = live is not None and held is live._stop and not held.is_set()
        in_flight = _lifecycle._state._closes.in_flight(sink)
        sink.waits.clear()
        _traced()()
        log_foundry.flush(timeout=GATE)
        sink.go.set()
        return f"in_flight={in_flight} holds_live_unset={holds} waited={sink.waits[:1]}"

    child = run_in_child(work, timeout=40)
    assert child.output == "in_flight=True holds_live_unset=True waited=[0.3]", child.output


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
        return f"second_close_held_set={sink.second_close_held_set} quick={elapsed < 2.0}"

    child = run_in_child(work, timeout=40)
    assert child.output == "second_close_held_set=[True] quick=True", child.output


def test_a_mark_paid_at_the_stops_end_is_not_paid_again_by_the_close() -> None:
    """The stop's end consumes the mark, so the close's own discharge later hands nothing.

    The close of S that the tail build marked is a swap's, still parked when the stop ends; the
    stop's end hands S the live worker's signal and clears the mark. When the close is released
    and discharges its entry, it must find no mark and leave S alone, so S's setter sees that
    signal exactly once. A stop-end payment that left the mark set had the discharge hand it a
    second time, running the setter again for nothing.
    """

    def work() -> str:
        sink = _SwappedOut()
        _swapped_out_and_back(sink)
        _build_in_the_tail(sink)
        live = _lifecycle._state.live_worker()
        sink.go.set()
        deadline = time.monotonic() + GATE
        while _lifecycle._state._closes.in_flight(sink) and time.monotonic() < deadline:
            time.sleep(0.005)
        time.sleep(0.1)
        handed = 0 if live is None else sum(1 for event in sink.offers if event is live._stop)
        return f"closed={sink.closes} late_signal_handed={handed}"

    child = run_in_child(work, timeout=40)
    assert child.output == "closed=1 late_signal_handed=1", child.output
