"""SPEC-097 and SPEC-098, written while authoring them against ead40798, for SPEC-098: which stop
signal a sink holds, how often its setter runs, and whether a stop cuts its wait short, in a parent
and in a forked child.

Run with ``PYTHONPATH=<tree>/src`` against two trees and compare. Each row is a fresh process. A
sink records every assignment of ``log_foundry_stop_signal`` and the distinct events it was handed.
``sequence`` makes ``configure(S)``, an orphan log, a traced call, ``flush()``, ``configure(T)``,
``configure(S)``, an orphan log, ``shutdown()``, then an orphan log after the shutdown, and prints
the assignments, the distinct events, whether S's current signal is set after the shutdown, and
how long a 2 s wait on it took then. ``swap-orphan``, ``swap-worker`` and ``swap-multi`` (added after SPEC-098's spec review) reach S
on the orphan path or the worker path and then ``configure()`` T, or a ``MultiSink`` of T and a
second sink, and print whether each installed sink holds an event when ``configure()`` returns,
before anything is delivered to it. ``cut`` puts a thread in a 4 s wait on S's signal and calls
``shutdown()``; it prints how long after the call the wait ended. ``child-orphan`` and
``child-worker`` fork after S is reached on that path; in the child a thread waits 4 s on the
event S holds and the child calls ``shutdown()``: the row prints the setter's calls in the child,
whether the child's S holds the parent's event object, when the child's wait ended, and whether the
parent's event was set by the child. Every child is bounded and leaves through ``os._exit``.
"""

import os
import select
import subprocess
import sys
import threading
import time

ROWS = (
    "sequence",
    "cut",
    "child-orphan",
    "child-worker",
    "swap-orphan",
    "swap-worker",
    "swap-multi",
)

if len(sys.argv) < 2:
    for row in ROWS:
        r = subprocess.run(
            [sys.executable, "-B", __file__, row], capture_output=True, text=True, timeout=60
        )
        print(
            f"{row:13s} exit={r.returncode} {(r.stdout.strip().splitlines() or ['(nothing)'])[-1]}"
        )
    sys.exit(0)

import log_foundry  # noqa: E402


class Holder:
    def __init__(self) -> None:
        self._signal: threading.Event | None = None
        self.assigned = 0
        self.seen: list[int] = []

    @property
    def log_foundry_stop_signal(self) -> threading.Event | None:
        return self._signal

    @log_foundry_stop_signal.setter
    def log_foundry_stop_signal(self, signal: threading.Event | None) -> None:
        self.assigned += 1
        if signal is not None and id(signal) not in self.seen:
            self.seen.append(id(signal))
        self._signal = signal

    def emit(self, batch: list[dict]) -> None:
        pass

    def close(self) -> None:
        pass


@log_foundry.trace
def traced() -> None:
    log_foundry.info("in")


def timed_wait(event: threading.Event, seconds: float, out: list[float]) -> None:
    started = time.monotonic()
    event.wait(seconds)
    out.append(time.monotonic())


row = sys.argv[1]
S, T = Holder(), Holder()
log_foundry.configure(service="spec098", sink=S)
if row == "sequence":
    log_foundry.info("a")
    traced()
    log_foundry.flush(timeout=5)
    log_foundry.configure(sink=T)
    log_foundry.configure(sink=S)
    log_foundry.info("b")
    log_foundry.shutdown(timeout=5)
    log_foundry.info("after")
    held = S.log_foundry_stop_signal
    started = time.monotonic()
    if held is not None:
        held.wait(2.0)
    print(
        f"assignments={S.assigned} distinct_events={len(S.seen)} set_after_shutdown={held is not None and held.is_set()} wait_after_shutdown={time.monotonic() - started:.2f}s"
    )
elif row.startswith("swap-"):
    if row == "swap-worker":
        traced()
        log_foundry.flush(timeout=5)
    else:
        log_foundry.info("a")
    if row == "swap-multi":
        from log_foundry.sinks.multi import MultiSink

        T2 = Holder()
        log_foundry.configure(sink=MultiSink(T, T2))
        held = [T.log_foundry_stop_signal, T2.log_foundry_stop_signal]
    else:
        log_foundry.configure(sink=T)
        held = [T.log_foundry_stop_signal]
    print(f"held_when_configure_returned={[e is not None for e in held]} assignments={T.assigned}")
elif row == "cut":
    log_foundry.info("a")
    ended: list[float] = []
    waiter = threading.Thread(target=timed_wait, args=(S.log_foundry_stop_signal, 4.0, ended))
    waiter.start()
    time.sleep(0.1)
    called = time.monotonic()
    log_foundry.shutdown(timeout=5)
    waiter.join(6)
    print(f"wait ended {ended[0] - called:.2f}s after shutdown() was called")
else:
    if row == "child-orphan":
        log_foundry.info("a")
    else:
        traced()
        log_foundry.flush(timeout=5)
    parent_event = S.log_foundry_stop_signal
    before = S.assigned
    read, write = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(read)
        try:
            log_foundry.info("child")
            held = S.log_foundry_stop_signal
            ended: list[float] = []
            waiter = threading.Thread(target=timed_wait, args=(held, 4.0, ended), daemon=True)
            waiter.start()
            time.sleep(0.1)
            called = time.monotonic()
            log_foundry.shutdown(timeout=3)
            waiter.join(5)
            cut = f"{ended[0] - called:.2f}s" if ended else "never"
            line = f"child: setter_calls={S.assigned - before} same_event_object={held is parent_event} wait_ended={cut} after its shutdown()"
        except BaseException as exc:  # noqa: BLE001
            line = f"child raised {type(exc).__name__}"
        os.write(write, line.encode())
        os._exit(0)
    os.close(write)
    ready, _, _ = select.select([read], [], [], 20)
    text = os.read(read, 4096).decode() if ready else "child printed nothing in 20 s"
    try:
        os.kill(pid, 9)
    except ProcessLookupError:
        pass
    os.waitpid(pid, 0)
    print(f"{text}; parent_event_set_by_child={parent_event is not None and parent_event.is_set()}")
