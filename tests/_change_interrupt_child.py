"""One run of the change-interrupt corpus: a KeyboardInterrupt at one line event of a sink change.

Run as ``python _change_interrupt_child.py <mode> <k>``. ``mode`` is ``user``, a non-re-entrant
``configure(sink=b)`` on the main thread whose sink's stop-signal setter defers a ``shutdown()``,
or ``drain``, the same change made from a sink's ``emit`` on the drain thread. The interrupt is
raised at the ``k``-th line event inside the change's functions (:data:`CHANGE_FUNCTIONS`) on that
thread; ``k == -1`` raises none and reports every event's site instead, which is how the corpus
derives its rows from the code as it is. After the change, the child makes a later unrelated
``configure(sink=c)``, then a ``shutdown()``, then exits, and prints one JSON line from an
``atexit`` probe registered before the library's own handler, so it runs after the exit drain.
A run whose interrupt left one of the library's locks owned by the interrupted thread, an
interrupt that skipped a ``with`` statement's ``__exit__``, reports that and exits at once: every
later call on that lock would wait or be held for good, on the base as on the build.
"""

from __future__ import annotations

import atexit
import faulthandler
import json
import os
import sys
import threading

MODE = sys.argv[1]
K = int(sys.argv[2])
report: dict[str, object] = {"mode": MODE, "k": K}
sinks: dict[str, Plain] = {}


def _probe() -> None:
    report["at_exit"] = {name: [s.events, s.closes] for name, s in sinks.items()}
    sys.stdout.write(json.dumps(report) + "\n")
    sys.stdout.flush()


atexit.register(_probe)
faulthandler.dump_traceback_later(15, exit=True)

import log_foundry  # noqa: E402
from log_foundry import _lifecycle, config, worker  # noqa: E402

worker.DEFAULT_SWAP_TIMEOUT = 0.5

CHANGE_FUNCTIONS = {
    getattr(module, name).__code__: name
    for module, name in [
        (config, "_change_sink"),
        (_lifecycle, "open_command_swap"),
        (_lifecycle, "end_command_swap"),
        (_lifecycle, "close_command_swap"),
        (_lifecycle, "_swap_sink"),
        (_lifecycle, "_merged_or_deferred"),
        (_lifecycle, "_live_taker"),
        (_lifecycle, "run_deferred_stops"),
    ]
    if hasattr(module, name)
}
"""The functions a sink change and its deferral run; each line the count run reaches is a row.

``_shutdown_worker``, the stop a deferral runs, is not among them: its interruption is any
``shutdown()``'s, which this spec does not change, and its own suites cover it.
"""


class Plain:
    """A sink that counts what it is given and its closes."""

    def __init__(self, name: str) -> None:
        self.name, self.events, self.closes = name, 0, 0
        sinks[name] = self

    def emit(self, batch: list[dict[str, object]]) -> None:
        self.events += len(batch)

    def close(self) -> None:
        self.closes += 1


class Stops(Plain):
    """A sink whose stop-signal setter calls ``shutdown(timeout=1)`` once, on the changing thread."""

    def __init__(self, name: str, thread: list[threading.Thread]) -> None:
        super().__init__(name)
        self.fired = False
        self.thread = thread

    @property
    def log_foundry_stop_signal(self) -> threading.Event | None:
        return None

    @log_foundry_stop_signal.setter
    def log_foundry_stop_signal(self, value: threading.Event | None) -> None:
        if self.fired or value is None or threading.current_thread() not in self.thread:
            return
        self.fired = True
        log_foundry.shutdown(timeout=1.0)


class Changes(Plain):
    """A sink whose first ``emit``, on the drain thread, changes the sink to ``target``."""

    def __init__(self, name: str, target: Plain) -> None:
        super().__init__(name)
        self.target, self.changed = target, False

    def emit(self, batch: list[dict[str, object]]) -> None:
        super().emit(batch)
        if not self.changed:
            self.changed = True
            changing.append(threading.current_thread())
            try:
                log_foundry.configure(sink=self.target)  # type: ignore[arg-type]
            finally:
                owned.extend(_owned())
                changed.set()


def _owned() -> list[int]:
    """The library locks the calling thread still owns: an interrupt that skipped a ``__exit__``."""
    locks = getattr(_lifecycle, "_library_locks", list)()
    return [i for i, lock in enumerate(locks) if lock._is_owned()]


changing: list[threading.Thread] = []
changed = threading.Event()
owned: list[int] = []
sites: list[str] = []
interrupted: list[str] = []


def _tracer(frame, event, arg):  # type: ignore[no-untyped-def]
    if frame.f_code not in CHANGE_FUNCTIONS or threading.current_thread() not in changing:
        return None

    def local(frame, event, arg):  # type: ignore[no-untyped-def]
        if event == "line":
            site = f"{CHANGE_FUNCTIONS[frame.f_code]}:{frame.f_lineno}"
            index = len(sites)
            sites.append(site)
            if index == K:
                interrupted.append(site)
                raise KeyboardInterrupt(site)
        return local

    return local


@log_foundry.trace
def work(message: str) -> None:
    log_foundry.info(message)


if MODE == "drain":
    threading.settrace(_tracer)
a = Plain("a")
log_foundry.configure(service="interrupts", sink=a)  # type: ignore[arg-type]
work("one")
log_foundry.flush(timeout=2.0)
work("owed")
b = Stops("b", changing)
outcome = "returned"
if MODE == "user":
    changing.append(threading.current_thread())
    sys.settrace(_tracer)
    try:
        log_foundry.configure(sink=b)  # type: ignore[arg-type]
    except KeyboardInterrupt:
        outcome = "interrupted"
    finally:
        sys.settrace(None)
        owned.extend(_owned())
else:
    log_foundry.configure(sink=Changes("e", b))  # type: ignore[arg-type]
    work("to e")
    changed.wait(10.0)
    threading.settrace(None)  # type: ignore[arg-type]
    outcome = "interrupted" if interrupted else "returned"
report.update(
    owned=owned,
    site=interrupted[0] if interrupted else None,
    outcome=outcome,
    traced=sorted(CHANGE_FUNCTIONS.values()),
    sink_lock_left=config._sink_lock.locked(),
    holder_left=config._sink_holder is not None,
)
if owned:
    sys.stdout.write(json.dumps(report) + "\n")
    sys.stdout.flush()
    os._exit(0)
if MODE == "drain":
    log_foundry.flush(timeout=2.0)
state = _lifecycle._state
taker = getattr(state, "_deferred_taker", None)
report.update(
    events=len(sites),
    sites=sites if K == -1 else None,
    fired=b.fired,
    live_taker_deferred=(
        len(taker.deferred)
        if taker is not None and _lifecycle._on_stack(taker.change, taker.thread)
        else 0
    ),
    retired_after_change=state.retirements,
)
before = state.retirements
c = Plain("c")
log_foundry.configure(sink=c)  # type: ignore[arg-type]
work("to c")
report["later_configure_retired"] = state.retirements - before
log_foundry.shutdown(timeout=1.0)
report["later_shutdown_retired"] = log_foundry.health().retired
