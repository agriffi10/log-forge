"""SPEC-097, written while authoring it against ead40798: a signal handler that calls a lifecycle
method while the main thread holds one of the library's locks.

Usage: spec097_signal_in_section.py [row]   (no argument runs every row, each in a fresh process)

Run with ``PYTHONPATH=<tree>/src``. Each row injects SIGTERM with ``sys.settrace`` at the first line
executed inside one library function while the main thread holds the lock that function's section
takes, as ``lifecycle/p1a_signal_shutdown_injected.py`` does, and patches no library code. A
held lock is read through ``_is_owned`` where the lock has one (an ``RLock``) and ``locked`` where it
does not, so the same file runs against a tree whose locks are plain and one whose locks are not.
The handler calls the row's lifecycle method, and a watchdog exits 1 after 6 s. A row prints how
long the handler's call took, and, once the interrupted call has returned, whether the process is
retired and how many times the sink was closed, both read directly with no further library call,
so a public call that runs held calls at its end cannot be confused with a later one that did.
The rows ``rebind``, ``swap`` and ``health`` were added after SPEC-097's spec review, from the
reviewer's ``sig_configure.py`` and ``sig_any.py``. Exit 0 means the handler returned.
"""

import faulthandler
import os
import signal
import subprocess
import sys
import time

ROWS = {
    "flush-section/shutdown": (
        "_lifecycle",
        "_flush_live_sink",
        "_state._lock",
        "flush",
        "shutdown",
    ),
    "orphan-arm/shutdown": ("_lifecycle", "_arm_and_hand", "_state._lock", "info", "shutdown"),
    "submit/shutdown": ("worker", "_put", "mutex", "span", "shutdown"),
    "flush-section/flush": ("_lifecycle", "_flush_live_sink", "_state._lock", "flush", "flush"),
    "flush-section/configure": (
        "_lifecycle",
        "_flush_live_sink",
        "_state._lock",
        "flush",
        "configure",
    ),
    "orphan-arm/info": ("_lifecycle", "_arm_and_hand", "_state._lock", "info", "info"),
    "submit/span": ("worker", "_put", "mutex", "span", "span"),
    "flush-section/info": ("_lifecycle", "_flush_live_sink", "_state._lock", "flush", "info"),
    "rebind/shutdown": ("config", "_rebind", "config", "configure-service", "shutdown"),
    "swap/shutdown": ("_lifecycle", "_swap_sink", "_state._lock", "configure-sink", "shutdown"),
    "health/shutdown": ("worker.Worker", "health", "worker", "health", "shutdown"),
}

if len(sys.argv) < 2:
    for row in ROWS:
        started = time.monotonic()
        r = subprocess.run(
            [sys.executable, "-B", __file__, row], capture_output=True, text=True, timeout=30
        )
        last = (r.stdout.strip().splitlines() or ["(nothing printed)"])[-1]
        print(f"{row:26s} exit={r.returncode} {last}  [{time.monotonic() - started:.2f}s]")
    sys.exit(0)

faulthandler.dump_traceback_later(6, exit=True, file=open(os.devnull, "w"))
import log_foundry  # noqa: E402
from log_foundry import _lifecycle, config, worker  # noqa: E402

module_name, function, lock_path, call, handler_call = ROWS[sys.argv[1]]


class Sink:
    def __init__(self) -> None:
        self.events = 0
        self.closes = 0

    def emit(self, batch: list[dict]) -> None:
        self.events += len(batch)

    def close(self) -> None:
        self.closes += 1


sink, other = Sink(), Sink()
log_foundry.configure(service="spec097", sink=sink)


@log_foundry.trace
def work() -> None:
    log_foundry.info("in")


if call != "info":
    work()
    log_foundry.flush(timeout=2)
    log_foundry.info("armed")


def held(lock: object) -> bool:
    owned = getattr(lock, "_is_owned", None)
    return bool(owned()) if owned is not None else bool(lock.locked())


def lock_now() -> object:
    if lock_path == "mutex":
        return _lifecycle._state._worker._queue.mutex
    if lock_path == "config":
        return config._config_lock
    if lock_path == "worker":
        return _lifecycle._state._worker._lock
    return _lifecycle._state._lock


took: list[float] = []


def on_term(signum: int, frame: object) -> None:
    started = time.monotonic()
    if handler_call == "shutdown":
        log_foundry.shutdown(timeout=2)
    elif handler_call == "info":
        log_foundry.info("from the handler")
    elif handler_call == "span":
        work()
    elif handler_call == "flush":
        took.append(-1.0 if log_foundry.flush(timeout=2) else -2.0)
    else:
        log_foundry.configure(sink=other)
    took.append(time.monotonic() - started)


signal.signal(signal.SIGTERM, on_term)
holders = {"worker": worker._CountingQueue, "worker.Worker": worker.Worker}
holder = holders.get(module_name) or sys.modules[f"log_foundry.{module_name}"]
if function == "_arm_and_hand" and not hasattr(holder, function):
    function = "_note_orphan_emit"  # SPEC-098's deletion moved the orphan arm's section here
target = getattr(holder, function)
code = target.__code__
fired: list[int] = []


def tracer(frame, event, arg):  # type: ignore[no-untyped-def]
    if frame.f_code is code:

        def local(frame, event, arg):  # type: ignore[no-untyped-def]
            if event == "line" and not fired and held(lock_now()):
                fired.append(frame.f_lineno)
                signal.raise_signal(signal.SIGTERM)
            return local

        return local
    return None


sys.settrace(tracer)
if call == "flush":
    log_foundry.flush(timeout=1)
elif call == "info":
    log_foundry.info("first")
elif call == "configure-service":
    log_foundry.configure(service="renamed")
elif call == "configure-sink":
    log_foundry.configure(sink=other)
elif call == "health":
    log_foundry.health()
else:
    work()
sys.settrace(None)
time.sleep(0.2)
retired = _lifecycle._state.retirements > 0
closes = getattr(sink, "closes", "?")
configured_other = config._live_config().sink is other
log_foundry.shutdown(timeout=2)
ledger = log_foundry.health().events
print(
    f"fired={bool(fired)} handler_took={[round(t, 2) for t in took]} retired={retired} "
    f"sink_closes={closes} configured_other={configured_other} accepted={ledger.accepted} "
    f"delivered={ledger.delivered} unaccounted={ledger.unaccounted}"
)
