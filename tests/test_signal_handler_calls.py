"""SPEC-097 FR-004: a lifecycle call made on a thread inside a library lock section is held.

A signal handler runs on the main thread between two bytecodes, and a finalizer wherever the
collector runs. When either interrupted the library inside one of its lock sections and called
``shutdown()``, the call waited on a lock its own thread held, for good: 5, 29 and 32 of 200
fresh processes in ``lifecycle/p1b_run.py``'s three modes at ``ead40798``. Now a ``shutdown()`` or
``configure()`` made there is held, recorded on the thread, and run at the end of the public call
the thread was in, before any later stop; a ``flush()`` returns ``"re-entrant"``.

Every row runs in a fresh interpreter, injecting ``SIGTERM`` with ``sys.settrace`` at the first
line run inside a library section, as the audit's probes do, and is bounded by a watchdog, so a
regression fails one row rather than hanging the suite. Serves invariants 1, 2, 3 and 5.
"""

from __future__ import annotations

import os
import pathlib
import re
import subprocess
import sys
import textwrap

import pytest

import log_foundry
from log_foundry import _lifecycle, decorator

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_SRC = pathlib.Path(log_foundry.__file__).resolve().parent.parent
_PROBE = _ROOT / "docs/audits/2026-11-14-probes/core-pipeline/spec097_signal_in_section.py"


def _run(args: list[str], *, timeout: float = 60) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "PYTHONPATH": str(_SRC)}
    return subprocess.run(  # noqa: S603
        [sys.executable, "-B", *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
        check=False,
    )


STALL_ALLOWANCE = 0.5
"""The suite's stall allowance (``tests/test_timing_floors.py``): a handler's call that returns at
once is held under it, since a ceiling below it fails on a CI runner's scheduling delay alone."""

_HELD_STOP = "retired=True sink_closes=1"

ROWS = {
    "flush-section/shutdown": _HELD_STOP,
    "orphan-arm/shutdown": _HELD_STOP,
    "submit/shutdown": _HELD_STOP,
    "rebind/shutdown": _HELD_STOP,
    "swap/shutdown": _HELD_STOP,
    "health/shutdown": _HELD_STOP,
    "flush-section/flush": "handler_took=[-2.0, ",
    "flush-section/configure": "configured_other=True",
    "orphan-arm/info": "",
    "submit/span": "",
    "flush-section/info": "",
}
"""``spec097_signal_in_section.py``'s eleven rows and what each must read once its call returned."""


@pytest.mark.parametrize("row", list(ROWS))
def test_a_handlers_call_inside_a_library_section_returns_and_runs_when_the_call_ends(
    row: str,
) -> None:
    """FR-004: the port of ``spec097_signal_in_section.py``, one row per case, all eleven.

    The handler's call returns at once (``handler_took`` under :data:`STALL_ALLOWANCE`, so a loaded
    CI runner's scheduling delay is not read as a wait, while the 6 s watchdog a waiting handler
    hits is far above it), the process exits 0 rather than to that watchdog, and, read once the
    interrupted call has returned with no further library call, a held ``shutdown()`` has retired
    the process and closed the sink once, a ``flush()`` returned falsy (``-2.0``), a held
    ``configure()`` installed its sink, and the event ledger balances.
    """
    result = _run([str(_PROBE), row], timeout=30)
    lines = result.stdout.strip().splitlines()
    assert result.returncode == 0 and lines, f"{row}: exit {result.returncode}\n{result.stderr}"
    line = lines[-1]
    assert "fired=True" in line, f"{row}: the signal never fired: {line}"
    assert "unaccounted=0" in line, line
    took = re.search(r"handler_took=\[([^\]]*)\]", line)
    assert took is not None and float(took.group(1).split(", ")[-1]) < STALL_ALLOWANCE, line
    assert ROWS[row] in line, line


_ENSURE_SINK = textwrap.dedent(
    """
    import faulthandler, os, signal, sys
    faulthandler.dump_traceback_later(6, exit=True, file=open(os.devnull, "w"))
    import log_foundry
    from log_foundry import _lifecycle, config
    from log_foundry.sinks.stdout import StdoutSink

    took = []

    def on_term(signum, frame):
        log_foundry.configure(service="from the handler")
        took.append("returned")

    signal.signal(signal.SIGTERM, on_term)
    import inspect
    code = config._ensure_sink.__code__
    lines, first = inspect.getsourcelines(config._ensure_sink)
    at = first + next(
        i for i, line in enumerate(lines) if "_config = replace(_config, sink=default)" in line
    )
    fired = []

    def tracer(frame, event, arg):
        if frame.f_code is code:
            def local(frame, event, arg):
                if event == "line" and not fired and frame.f_lineno == at:
                    fired.append(frame.f_lineno)
                    signal.raise_signal(signal.SIGTERM)
                return local
            return local
        return None

    sys.settrace(tracer)
    log_foundry.info("first")
    sys.settrace(None)
    built = [
        entry.referent()
        for entry in _lifecycle._state._owned.values()
        if type(entry.referent()) is StdoutSink
    ]
    live = config._live_config()
    sys.stdout.write(
        f"fired={bool(fired)} handler={took} defaults={len(built)} "
        f"used={len(built) == 1 and live.sink is built[0]} service={live.service}\\n"
    )
    """
)


def test_a_handlers_configure_inside_the_default_sinks_build_builds_one_default() -> None:
    """FR-004, plan review M2: a handler's ``configure(service=...)`` inside ``_ensure_sink``.

    The first log builds the default sink under ``config._config_lock``. The signal is raised at
    the section's write of the default it has just built and stamped. A handler there calling
    ``configure()`` re-entered the lock: its own ``_ensure_sink`` found no sink yet, built and
    stamped a second default and installed it, which the interrupted write then replaced, leaving a
    stamped sink nothing used. ``configure()`` is held at its entry now, after validation, and runs
    once the interrupted ``info()`` returns: one default sink, the one in use, and the handler's
    service.
    """
    result = _run(["-c", _ENSURE_SINK], timeout=30)
    lines = [line for line in result.stdout.splitlines() if line.startswith("fired=")]
    assert result.returncode == 0 and lines, result.stdout[-2000:] + result.stderr[-2000:]
    assert lines[-1] == (
        "fired=True handler=['returned'] defaults=1 used=True service=from the handler"
    ), lines[-1]


_MARKER_WAIT = textwrap.dedent(
    """
    import faulthandler, os, signal, sys, threading
    faulthandler.dump_traceback_later(6, exit=True, file=open(os.devnull, "w"))
    import log_foundry
    from log_foundry import _lifecycle

    class Sink:
        def __init__(self):
            self.closes = 0
            self.gate = threading.Event()
            self.gate.set()
        def emit(self, batch):
            self.gate.wait(2)
        def close(self):
            self.closes += 1

    sink = Sink()
    log_foundry.configure(service="marker-wait", sink=sink)

    @log_foundry.trace
    def work():
        log_foundry.info("x")

    work()
    log_foundry.flush(timeout=2)
    took = []

    def on_term(signum, frame):
        import time
        started = time.monotonic()
        log_foundry.shutdown(timeout=2)
        took.append(round(time.monotonic() - started, 1))

    signal.signal(signal.SIGTERM, on_term)
    fired = []

    def tracer(frame, event, arg):
        if frame.f_code.co_filename.endswith("threading.py") and frame.f_code.co_name == "wait":
            def local(frame, event, arg):
                if event == "line" and not fired and _lifecycle._state._calls_here.flushing:
                    fired.append(frame.f_lineno)
                    signal.raise_signal(signal.SIGTERM)
                return local
            return local
        return None

    sink.gate.clear()
    work()
    threading.Timer(0.3, sink.gate.set).start()
    sys.settrace(tracer)
    log_foundry.flush(timeout=2)
    sys.settrace(None)
    sys.stdout.write(
        f"fired={bool(fired)} took={took} retired={_lifecycle._state.retirements > 0} "
        f"closes={sink.closes}\\n"
    )
    """
)


def test_a_handlers_shutdown_inside_a_flushs_marker_wait_is_held_until_the_flush_returns() -> None:
    """FR-004: the marker wait is marked, so a handler interrupting it holds its ``shutdown()``.

    ``flush()`` waits on an ``Event`` for its marker, and a handler that interrupts that wait runs
    inside ``Event.wait``'s condition section; the handler's stop sets that very event, so it waited
    on its own thread for good, which the prototype found beyond the four library locks. Only that
    wait is marked (``_CallsHere.flushing``). The signal is raised at the first line run inside a
    ``threading`` ``wait`` while the mark is up: the handler's call returns at once, and once the
    flush has returned, read with no further library call, the held stop has retired the process
    and closed the sink once.
    """
    result = _run(["-c", _MARKER_WAIT], timeout=30)
    lines = [line for line in result.stdout.splitlines() if line.startswith("fired=")]
    assert result.returncode == 0 and lines, f"exit {result.returncode}\n{result.stderr[-2000:]}"
    took = re.search(r"took=\[([^\]]*)\]", lines[-1])
    assert took is not None and float(took.group(1)) < STALL_ALLOWANCE, lines[-1]
    assert lines[-1].startswith("fired=True took=["), lines[-1]
    assert lines[-1].endswith("] retired=True closes=1"), lines[-1]


def test_a_stop_runs_a_call_held_before_it_first() -> None:
    """FR-004: a stop runs every call held on its thread before it begins.

    A call can still be held when a stop starts: a held call that fails while a call unwinds is
    absorbed, and the calls held after it stay held for the thread's next public call. Here a
    failing call and ``configure(sink=other)`` are held, and a traced call that raises runs the
    first and leaves the second. The next call is ``shutdown()``, which runs the held change first,
    while the worker is still live: the change retargets the worker and arms ``other``, and the stop
    then closes both sinks once. Run after the stop instead, the change would find a retired worker
    and nothing owed, and ``other`` would never be closed.
    """
    import functools

    sink, other = _QuietSink(), _QuietSink()
    log_foundry.configure(service="stop-first", sink=sink)  # type: ignore[arg-type]

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("x")

    @log_foundry.trace
    def fails() -> None:
        _hold_a_faulting_call([])
        _lifecycle.hold(functools.partial(log_foundry.configure, sink=other))  # type: ignore[arg-type]
        raise _Unwinding("the traced function fails")

    work()
    log_foundry.flush(timeout=2)
    with pytest.raises(_Unwinding):
        fails()
    assert len(_lifecycle._state._calls_here.waiting or []) == 1, "the premise: one call left held"
    log_foundry.shutdown(timeout=2)
    assert not _lifecycle._state._calls_here.waiting
    assert log_foundry.get_config().sink is other
    assert (sink.closes, other.closes) == (1, 1), (sink.closes, other.closes)


_SIG_EXIT = textwrap.dedent(
    """
    import atexit, faulthandler, os, signal, sys
    faulthandler.dump_traceback_later(8, exit=True, file=open(os.devnull, "w"))
    WHERE = sys.argv[1]

    def probe():
        from log_foundry import _lifecycle, config
        sys.stdout.write(
            f"held={held} config_is_other={config._live_config().sink is other} "
            f"other_closes={other.closes} state={_lifecycle._state.state.name}\\n"
        )
        sys.stdout.flush()

    atexit.register(probe)
    import log_foundry
    from log_foundry import _lifecycle, worker

    class Sink:
        def __init__(self):
            self.closes = 0
        def emit(self, batch):
            return None
        def close(self):
            self.closes += 1

    sink, other = Sink(), Sink()
    log_foundry.configure(service="sig-exit", sink=sink)

    @log_foundry.trace
    def work():
        log_foundry.info("x")

    work()
    log_foundry.flush(timeout=2)
    held = []

    def on_term(signum, frame):
        log_foundry.configure(sink=other)
        held.append(config_now() is not other)

    def config_now():
        from log_foundry import config
        return config._live_config().sink

    signal.signal(signal.SIGTERM, on_term)
    if WHERE == "health":
        code, lock = worker.Worker.health.__code__, lambda: _lifecycle._state._worker._lock
    else:
        code, lock = _lifecycle._close_owed.__code__, lambda: _lifecycle._state._lock
    fired = []

    def tracer(frame, event, arg):
        if frame.f_code is code:
            def local(frame, event, arg):
                if event == "line" and not fired and lock()._is_owned():
                    fired.append(frame.f_lineno)
                    signal.raise_signal(signal.SIGTERM)
                return local
            return local
        return None

    sys.settrace(tracer)
    if WHERE == "health":
        log_foundry.health()
    else:
        log_foundry.shutdown(timeout=2)
    sys.settrace(None)
    held = held and fired and held[0]
    """
)


@pytest.mark.parametrize(
    ("where", "closes"),
    [("health", 1), ("stop", 0)],
)
def test_a_held_configure_is_never_left_after_the_drain_it_would_race(
    where: str, closes: int
) -> None:
    """FR-004: the port of the spec reviewer's ``sig_exit.py``, from its description.

    A handler holds ``configure(sink=other)``, interrupting ``health()`` or a ``shutdown()``'s
    owed-close section, and the process then exits. A held call runs at the end of the public
    call it interrupted, and the calls held during a stop run once it is over, so at the end of the
    exit the config names ``other`` and the state is ``CLOSED``: the held call is never lost and
    never left after the drain. Held during ``health()``, the change retargets the live worker and
    arms ``other``, and the exit closes it once; the first prototype left ``other`` named and
    unclosed.

    Held during a stop, ``other`` is named but never closed, because nothing was written to it:
    the change runs after the stop retired the worker and took what was owed, so it finds no live
    worker and nothing owed and arms nothing. That is the existing no-worker arming rule: at
    ``9ee07a88`` a plain ``configure(sink=other)`` made after ``shutdown()``, with no writes, leaves
    ``other`` unclosed at exit too (measured ``other_closes=0 other_writes=0``). The spec's
    criterion reads ``other_closes=0`` for this row since the build (Andrew's decision, 2026-11-18).
    """
    result = _run(["-c", _SIG_EXIT, where], timeout=30)
    lines = [line for line in result.stdout.splitlines() if line.startswith("held=")]
    assert result.returncode == 0 and lines, result.stdout[-2000:] + result.stderr[-2000:]
    assert lines[-1] == (
        f"held=True config_is_other=True other_closes={closes} state=CLOSED"
    ), lines[-1]


_IN_FLUSH = textwrap.dedent(
    """
    import faulthandler, os, sys
    faulthandler.dump_traceback_later(8, exit=True, file=open(os.devnull, "w"))
    PATH, INNER = sys.argv[1], sys.argv[2]
    import log_foundry
    from log_foundry import _lifecycle, config

    class Sink:
        def __init__(self):
            self.armed = False
            self.result = None
        def emit(self, batch):
            return None
        def flush(self):
            if not self.armed:
                return
            self.armed = False
            if INNER == "flush":
                self.result = bool(log_foundry.flush(timeout=2))
            elif INNER == "shutdown":
                log_foundry.shutdown(timeout=2)
                self.result = _lifecycle._state.state.name
            else:
                log_foundry.configure(sink=other)
                self.result = config._live_config().sink is other
        def close(self):
            return None

    sink, other = Sink(), Sink()
    log_foundry.configure(service="in-flush", sink=sink)
    if PATH == "worker":
        @log_foundry.trace
        def work():
            log_foundry.info("x")
        work()
    else:
        log_foundry.info("x")
    log_foundry.flush(timeout=2)
    sink.armed = True
    log_foundry.flush(timeout=2)
    sys.stdout.write(f"inner={sink.result} state={_lifecycle._state.state.name}\\n")
    """
)


@pytest.mark.parametrize(
    ("path", "inner", "expected"),
    [
        (path, inner, expected)
        for path in ("orphan", "worker")
        for inner, expected in (
            ("flush", "inner=True state=RUNNING"),
            ("shutdown", "inner=CLOSED state=CLOSED"),
            ("configure", "inner=True state=RUNNING"),
        )
    ],
)
def test_a_sinks_own_flush_hook_makes_its_lifecycle_calls_as_before(
    path: str, inner: str, expected: str
) -> None:
    """FR-004: the port of the spec reviewer's ``inflush.py``, its six rows, from its description.

    The library calls a sink's own ``flush()`` hook on the caller's thread holding no library lock,
    so the hook's lifecycle calls are not held or refused: only ``Worker._flush_marked``'s wait on
    its marker is marked, never the whole of ``flush()``. The first prototype marked the whole call,
    and an inner ``flush()`` read ``re-entrant`` and an inner ``shutdown()`` left ``RUNNING``. Each
    row reads as it does at ``c9068a46``: the inner ``flush()`` truthy, the inner ``shutdown()``
    leaving ``CLOSED``, the inner ``configure()`` installing its sink.
    """
    result = _run(["-c", _IN_FLUSH, path, inner], timeout=30)
    lines = [line for line in result.stdout.splitlines() if line.startswith("inner=")]
    assert result.returncode == 0 and lines, result.stdout[-2000:] + result.stderr[-2000:]
    assert lines[-1] == expected, lines[-1]


_HELD_SWEEP = _ROOT / "docs/audits/2026-11-14-probes/core-pipeline/spec097_held_sweep.py"
_SWEEP_ROW = re.compile(r"^(\S+)\s+exit=(\S+) (.*)$")


def test_every_public_call_that_takes_a_library_lock_runs_the_calls_held_inside_it() -> None:
    """FR-004: the port of ``spec097_held_sweep.py``; the end-runners are derived, not listed.

    The probe runs every callable in ``log_foundry.__all__``, plus a first orphan log and a sink
    change, each in a fresh interpreter under a watchdog, and prints ``NO RECIPE`` for a callable
    it has no recipe for, so a new public call cannot be left out unseen. Under ``sys.settrace`` it
    raises ``SIGTERM`` at the first library line run while the thread holds a library lock, and the
    handler calls ``shutdown(timeout=2)``. Once the public call has returned, with no further
    library call, a row reads whether the process retired and how many calls are still held. Every
    row that fired must read ``retired=True held=0``: a public call that can take a library lock
    and does not run its thread's held calls at its end leaves the handler's stop unrun. The calls
    known to take a lock must be among those that fired, a floor that keeps the test from passing
    on a tracer that fires nowhere.
    """
    result = _run([str(_HELD_SWEEP)], timeout=300)
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-2000:]
    rows = [m.groups() for m in map(_SWEEP_ROW.match, result.stdout.splitlines()) if m]
    names = {name for name, _, _ in rows}
    assert {"configure", "flush", "health", "shutdown", "trace", "info"} <= names, names
    assert not [row for row in rows if "NO RECIPE" in row[2]], "a public call has no recipe"
    assert not [row for row in rows if row[1] != "0"], [row for row in rows if row[1] != "0"]
    fired = {name: line for name, _, line in rows if not line.startswith("fired=[]")}
    assert {"configure", "configure-sink", "flush", "health", "shutdown", "trace", "info-first"} <= (
        set(fired)
    ), sorted(fired)
    unrun = {name: line for name, line in fired.items() if "retired=True held=0" not in line}
    assert not unrun, f"a public call left its handler's held stop unrun: {unrun}"


_SYSTEM_EXIT = textwrap.dedent(
    """
    import atexit, faulthandler, os, signal, sys
    faulthandler.dump_traceback_later(8, exit=True, file=open(os.devnull, "w"))
    WHERE = sys.argv[1]
    report = {}

    def probe():
        from log_foundry import _lifecycle
        events = _lifecycle._ledger_snapshot()
        sys.stdout.write(
            f"fired={bool(report.get('fired'))} exit_seen={report.get('exit_seen')} "
            f"unwound={report.get('unwound')} "
            f"retirements={_lifecycle._state.retirements} closes={sink.closes} "
            f"held_left={len(_lifecycle._state._calls_here.waiting or [])} "
            f"unaccounted={events.unaccounted}\\n"
        )
        sys.stdout.flush()

    atexit.register(probe)
    import log_foundry
    from log_foundry import _lifecycle

    class Sink:
        def __init__(self):
            self.closes = 0
        def emit(self, batch):
            return None
        def close(self):
            self.closes += 1

    sink = Sink()
    log_foundry.configure(service="system-exit", sink=sink)

    @log_foundry.trace
    def work():
        log_foundry.info("x")

    @log_foundry.trace
    async def awork():
        log_foundry.info("x")

    def run_async():
        import asyncio
        asyncio.run(awork())

    if WHERE not in ("level", "trace-first", "trace-first-async"):
        work()
        log_foundry.flush(timeout=2)

    def on_term(signum, frame):
        log_foundry.shutdown(timeout=2)
        sys.exit(0)

    signal.signal(signal.SIGTERM, on_term)

    def tracer(frame, event, arg):
        if not frame.f_globals.get("__name__", "").startswith("log_foundry"):
            return None
        def local(frame, event, arg):
            if event == "line" and not report.get("fired") and _lifecycle.in_section():
                report["fired"] = True
                sys.settrace(None)
                signal.raise_signal(signal.SIGTERM)
            return local
        return local

    sys.settrace(tracer)
    try:
        {"level": lambda: log_foundry.info("first"), "trace": work, "trace-first": work,
         "trace-async": run_async, "trace-first-async": run_async,
         "health": log_foundry.health}[WHERE]()
    except SystemExit:
        report["exit_seen"] = True
        held = len(_lifecycle._state._calls_here.waiting or [])
        report["unwound"] = f"{_lifecycle._state.retirements}/{held}"
        raise
    finally:
        sys.settrace(None)
    """
)


@pytest.mark.parametrize(
    ("where", "expected"),
    [
        ("trace", "unwound=1/0 retirements=2 closes=1 held_left=0"),
        ("trace-async", "unwound=1/0 retirements=2 closes=1 held_left=0"),
        ("health", "unwound=1/0 retirements=2 closes=1 held_left=0"),
        ("level", "unwound=1/0 retirements=1 closes=0 held_left=0"),
        ("trace-first", "unwound=1/0 retirements=1 closes=0 held_left=0"),
        ("trace-first-async", "unwound=1/0 retirements=1 closes=0 held_left=0"),
    ],
)
def test_a_handler_that_raises_system_exit_has_its_held_stop_run(where: str, expected: str) -> None:
    """FR-004: a handler that calls ``shutdown()`` then ``sys.exit()`` inside a library section.

    The ``shutdown()`` is held and the ``SystemExit`` unwinds the interrupted call, which runs the
    held stop as it unwinds: every public call that can take a library lock runs its thread's held
    calls however it ends. ``unwound`` is read in the caller's ``except SystemExit``, before the
    exit drain, whose own start runs held calls too and would otherwise hide a call that did not:
    the held stop has run (one retirement) and nothing is left held. Inside a traced call's close,
    sync or async, and inside ``health()``, the exit drain is then the second stop: two
    retirements, the sink closed once.

    The ``level`` and ``trace-first`` rows land inside a process's first orphan log, in the section
    that arms the sink, and its first traced call (sync or async), in the worker's build, before
    the library has
    registered its exit drain. The held stop runs as that call unwinds, so the process retires, and
    nothing is owed a close, since nothing was written to the sink. Before the stop ran on the
    unwinding path these two rows read ``retirements=0 held_left=1``: nothing ran the held stop at
    all. The ledger balances in every row; the orphan log's accepted event is counted lost.
    """
    result = _run(["-c", _SYSTEM_EXIT, where], timeout=30)
    lines = [line for line in result.stdout.splitlines() if line.startswith("fired=")]
    assert lines, f"exit {result.returncode}\n{result.stdout[-2000:]}\n{result.stderr[-2000:]}"
    assert lines[-1] == f"fired=True exit_seen=True {expected} unaccounted=0", lines[-1]


_IN_EXIT_DRAIN = textwrap.dedent(
    """
    import atexit, faulthandler, os, signal, sys
    faulthandler.dump_traceback_later(8, exit=True, file=open(os.devnull, "w"))
    seen = {}

    def probe():
        from log_foundry import _lifecycle, config
        sys.stdout.write(
            f"fired={bool(seen.get('fired'))} service={config._live_config().service} "
            f"held_left={len(_lifecycle._state._calls_here.waiting or [])} "
            f"closes={sink.closes}\\n"
        )
        sys.stdout.flush()

    atexit.register(probe)
    import log_foundry
    from log_foundry import _lifecycle

    class Sink:
        def __init__(self):
            self.closes = 0
        def emit(self, batch):
            return None
        def close(self):
            self.closes += 1

    sink = Sink()
    log_foundry.configure(service="before", sink=sink)
    log_foundry.info("owed")

    def on_term(signum, frame):
        log_foundry.configure(service="from the handler")

    signal.signal(signal.SIGTERM, on_term)
    code = _lifecycle._close_owed.__code__
    armed = []

    def tracer(frame, event, arg):
        if frame.f_code is code and armed:
            def local(frame, event, arg):
                if event == "line" and not seen.get("fired") and _lifecycle._state._lock._is_owned():
                    seen["fired"] = True
                    signal.raise_signal(signal.SIGTERM)
                return local
            return local
        return None

    def arm():
        armed.append(True)
        sys.settrace(tracer)

    atexit.register(arm)
    """
)


def test_a_call_held_during_the_exit_drain_runs_before_the_process_ends() -> None:
    """FR-004: the exit drain runs the calls held during it once it is over.

    The exit drain binds the stop directly, not through ``shutdown()``, so the stop's own
    ``finally`` is the only place its held calls run. A handler holds ``configure(service=...)``
    inside the exit drain's owed-close section; the probe, registered first with ``atexit`` and so
    run last, reads the handler's service and nothing left held.
    """
    result = _run(["-c", _IN_EXIT_DRAIN], timeout=30)
    lines = [line for line in result.stdout.splitlines() if line.startswith("fired=")]
    assert lines, f"exit {result.returncode}\n{result.stdout[-2000:]}\n{result.stderr[-2000:]}"
    assert lines[-1] == "fired=True service=from the handler held_left=0 closes=1", lines[-1]


class _Unwinding(BaseException):
    """The exception a call is unwinding with, which must reach its caller unchanged."""


class _HeldFault(RuntimeError):
    """What a held call raises."""


def _hold_a_faulting_call(ran: list[str]) -> None:
    def faulting() -> None:
        ran.append("held")
        raise _HeldFault("the held call fails")

    _lifecycle.hold(faulting)


class _QuietSink:
    def __init__(self) -> None:
        self.closes = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        return None

    def close(self) -> None:
        self.closes += 1


class _UnwindingSink(_QuietSink):
    def emit(self, batch: list[dict[str, object]]) -> None:
        raise _Unwinding("the orphan delivery unwinds")


_UNWINDING_CALLS = [
    "trace-body",
    "trace-body-async",
    "trace-open",
    "trace-open-async",
    "trace-close",
    "trace-close-async",
    "level",
    "configure",
]


@pytest.mark.parametrize("call", _UNWINDING_CALLS)
def test_a_held_call_run_while_a_call_unwinds_never_replaces_its_exception(
    call: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-004: held calls run on the exceptional path, and the unwinding exception still wins.

    A call that fails when it runs is held during the public call, as a handler's call would be,
    and the public call then ends in an exception of its own: a traced function's, the traced
    open's, the traced close's (each sync and async), a level call's delivery's, and
    ``configure()``'s argument check's. The held call runs as the call unwinds, and its failure is
    absorbed with one line, so the caller receives the call's own exception, unchanged. Nothing is
    left held.
    """
    import asyncio

    ran: list[str] = []
    log_foundry.configure(service="unwinding", sink=_QuietSink())  # type: ignore[arg-type]

    @log_foundry.trace
    def fails() -> None:
        _hold_a_faulting_call(ran)
        raise _Unwinding("the traced function fails")

    @log_foundry.trace
    async def afails() -> None:
        _hold_a_faulting_call(ran)
        raise _Unwinding("the traced function fails")

    @log_foundry.trace
    def returns() -> None:
        _hold_a_faulting_call(ran)

    @log_foundry.trace
    async def areturns() -> None:
        _hold_a_faulting_call(ran)

    def open_fails(name: str, defaults: object) -> object:
        _hold_a_faulting_call(ran)
        raise _Unwinding("the open")

    real_begin, real_end = decorator._begin, decorator._end

    def close_fails(*args: object) -> None:
        real_end(*args)  # type: ignore[arg-type]
        raise _Unwinding("the close")

    if call.startswith("trace-open"):
        decorator._begin = open_fails  # type: ignore[assignment]
    if call.startswith("trace-close"):
        decorator._end = close_fails  # type: ignore[assignment]
    try:
        if call in ("trace-body", "trace-open"):
            with pytest.raises(_Unwinding, match=r"traced function|the open"):
                fails()
        elif call in ("trace-body-async", "trace-open-async"):
            with pytest.raises(_Unwinding, match=r"traced function|the open"):
                asyncio.run(afails())
        elif call == "trace-close":
            with pytest.raises(_Unwinding, match="the close"):
                returns()
        elif call == "trace-close-async":
            with pytest.raises(_Unwinding, match="the close"):
                asyncio.run(areturns())
        elif call == "level":

            class HoldsThenUnwinds(_QuietSink):
                def emit(self, batch: list[dict[str, object]]) -> None:
                    _hold_a_faulting_call(ran)
                    raise _Unwinding("the orphan delivery unwinds")

            log_foundry.configure(sink=HoldsThenUnwinds())  # type: ignore[arg-type]
            with pytest.raises(_Unwinding, match="orphan delivery"):
                log_foundry.info("x")
        else:
            _hold_a_faulting_call(ran)
            with pytest.raises(TypeError):
                log_foundry.configure(service=3)  # type: ignore[arg-type]
    finally:
        if call.startswith("trace-open"):
            decorator._begin = real_begin  # type: ignore[assignment]
        if call.startswith("trace-close"):
            decorator._end = real_end  # type: ignore[assignment]
    assert ran == ["held"], ran
    assert not _lifecycle._state._calls_here.waiting, "a held call was left held"
    err = capsys.readouterr().err
    assert "running a held lifecycle call while the call it interrupted unwound" in err, err
    assert "_HeldFault" in err, err


@pytest.mark.parametrize("flavour", ["sync", "async"])
def test_a_call_held_in_the_open_runs_before_the_function_body(flavour: str) -> None:
    """FR-004: a call held while a traced call opens runs once the open returns, before the body.

    A held stop landing in a process's first traced call, in the worker's build inside the open,
    used to run only at the end of the call, after the whole function body, so a handler's
    ``shutdown()`` waited out a function that could run for minutes. It now runs as soon as the
    open returns, sync and async alike.
    """
    import asyncio

    order: list[str] = []
    log_foundry.configure(service="open-first", sink=_QuietSink())  # type: ignore[arg-type]
    real_begin = decorator._begin

    def begin_then_hold(name: str, defaults: object) -> object:
        scope = real_begin(name, defaults)  # type: ignore[arg-type]
        _lifecycle.hold(lambda: order.append("held"))
        return scope

    @log_foundry.trace
    def work() -> None:
        order.append("body")

    @log_foundry.trace
    async def awork() -> None:
        order.append("body")

    decorator._begin = begin_then_hold  # type: ignore[assignment]
    try:
        if flavour == "sync":
            work()
        else:
            asyncio.run(awork())
    finally:
        decorator._begin = real_begin  # type: ignore[assignment]
    assert order == ["held", "body"], order


@pytest.mark.parametrize("flavour", ["sync", "async"])
def test_a_held_calls_own_exception_reaches_the_caller_when_the_call_returns(
    flavour: str,
) -> None:
    """FR-004: on a call that returns, a held call's failure is the call's own and propagates.

    Only an exception already unwinding is protected from replacement; with none, the held call is
    the last thing the traced call does, after its close, and what it raises is what the caller
    gets. The call is held inside the function body, so it runs at the call's return, sync and
    async alike.
    """
    import asyncio

    ran: list[str] = []
    log_foundry.configure(service="returning", sink=_QuietSink())  # type: ignore[arg-type]

    @log_foundry.trace
    def returns() -> int:
        _hold_a_faulting_call(ran)
        return 1

    @log_foundry.trace
    async def areturns() -> int:
        _hold_a_faulting_call(ran)
        return 1

    with pytest.raises(_HeldFault):
        if flavour == "sync":
            returns()
        else:
            asyncio.run(areturns())
    assert ran == ["held"]
    assert not _lifecycle._state._calls_here.waiting
