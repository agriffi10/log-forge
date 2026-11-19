"""SPEC-097, written while revising it after its spec review: a handler's level call and traced
call landing inside every library lock section a workload's main thread enters.

Usage: spec097_reentry_sweep.py                    enumerate, then one child per section
       spec097_reentry_sweep.py enumerate <w> [u]  print workload w's sections (u: and unreached)
       spec097_reentry_sweep.py fire <w> <k>       one child: interrupt workload w's k-th section

Run with ``PYTHONPATH=<tree>/src`` against a tree carrying the command path: it reads
``_lifecycle.in_section()``. A **section** is either the first statement inside a ``with`` on one
of the lifecycle locks, read from the AST of ``src/log_foundry`` (``_state._lock``, ``self._lock``,
the close registry's lock, ``_config_lock``, the sweep hold and its bounded entry, ``_loss_lock``,
the worker queue's ``mutex``, and the two ``RLock``s that stay out of the ownership oracle by
design, the ledger's ``registry.lock`` and ``_state._owned_lock``), or a line of the library,
``queue.py`` or ``threading.py`` run holding one of those locks right after a line that held none,
which is how a bare ``acquire()`` site is found. The sections a workload reaches are found by
running it under ``sys.settrace``, not listed.

Revised while building SPEC-097 on ``c9068a46`` (SPEC-089 and SPEC-091 built): the matcher reads
SPEC-091's ``_sweep_hold`` sites and the queue mutex, the two by-design ``RLock``s were added, and
the work is two workloads, because a no-worker ``shutdown()`` with a late worker can only be the
first stop of a process that has built no worker. ``main``: ``configure(sink=A)``, an orphan log, a
traced call, ``flush()``, a change to B, a factory-built sink and a sink that takes a stop signal,
an echo into a console stream that fails, an in-span log whose build fails, a close that fails, a
``continue_trace()`` adoption, a ``flush()`` that sweeps another thread's open span, a gated sink
whose queue fills, a change whose drain cannot be confirmed and a stop that expires, a sink that
raises, a ``shutdown()``, and a traced call and a log after it. ``late``: the default sink's build,
then a no-worker ``shutdown()`` during which another thread's traced call builds the late worker.

For each section a fresh child runs that workload again, raises ``SIGTERM`` at the first entry of
that section, and the handler makes an orphan log, a traced call that logs once, and a second
orphan log, re-entering the section's lock where it needs it. The child passes when the workload
ends with the ledger balanced (``unaccounted == 0``), each sink closed as often as in a control
child of the same workload that fires nothing, no span current, and the handler's three calls
made. A hang is the 15 s watchdog's exit 1. ``enumerate`` with ``u`` also prints, after a ``--``
line, every AST section the workload did not reach.
"""

import concurrent.futures
import faulthandler
import os
import subprocess
import sys

MODE = sys.argv[1] if len(sys.argv) > 1 else "all"
WORKLOADS = ("main", "late", "direct")

if MODE == "all":

    def run(*args: str, timeout: float = 90) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-B", __file__, *args], capture_output=True, text=True, timeout=timeout
        )

    rows = []
    reached_everywhere: set[str] = set()
    unreached_by: list[set[str]] = []
    controls: dict[str, str] = {}
    for workload in WORKLOADS:
        out = run("enumerate", workload).stdout.splitlines()
        listing = [line[5:] for line in out if line.startswith("SITE ")]
        out = run("enumerate", workload, "u").stdout.splitlines()
        unreached_by.append({line[7:] for line in out if line.startswith("UNSEEN ")})
        reached_everywhere.update(listing)
        control = run("fire", workload, "-1").stdout.split()
        controls[workload] = next(word for word in control if word.startswith("closes="))
        print(f"control {workload} (nothing fired): {' '.join(control)}")

        def one(k: int, workload: str = workload, listing: list[str] = listing) -> tuple:
            try:
                r = run("fire", workload, str(k), timeout=40)
            except subprocess.TimeoutExpired:
                return workload, listing[k], "timeout", ""
            last = (r.stdout.strip().splitlines() or ["(nothing)"])[-1]
            return workload, listing[k], r.returncode, last

        with concurrent.futures.ThreadPoolExecutor(8) as pool:
            rows += list(pool.map(one, range(len(listing))))
    bad = 0
    for workload, site, code, line in rows:
        ok = code == 0 and line.startswith("ok") and controls[workload] in line.split()
        bad += not ok
        print(f"{'ok  ' if ok else 'FAIL'} {workload:4s} {site:60s} exit={code} {line[:150]}")
    unreached = sorted(set.intersection(*unreached_by) - reached_everywhere)
    print(f"sections={len(rows)} failed={bad} ast_sections_not_reached={len(unreached)}")
    for site in unreached:
        print(f"not reached {site}")
    sys.exit(0)

WORKLOAD = sys.argv[2]
faulthandler.dump_traceback_later(15, exit=True, file=open(os.devnull, "w"))
import contextvars  # noqa: E402
import errno  # noqa: E402
import signal  # noqa: E402
import threading  # noqa: E402

import log_foundry  # noqa: E402
from log_foundry import _ledger, _lifecycle, api, decorator, worker  # noqa: E402

TARGET = int(sys.argv[3]) if MODE == "fire" else -1


class Sink:
    log_foundry_stop_signal = None

    def __init__(self) -> None:
        self.events = 0
        self.closes = 0

    def emit(self, batch: list[dict]) -> None:
        self.events += len(batch)

    def close(self) -> None:
        self.closes += 1


class Gated(Sink):
    def __init__(self) -> None:
        super().__init__()
        self.open = threading.Event()

    def emit(self, batch: list[dict]) -> None:
        self.open.wait(5)
        super().emit(batch)


class Raising(Sink):
    def emit(self, batch: list[dict]) -> None:
        raise RuntimeError("down")


class FailingStream:
    def write(self, text: str) -> int:
        raise OSError(errno.EAGAIN, "try again")

    def flush(self) -> None:
        return None


A, B, F, G, H, C = Sink(), Sink(), Sink(), Gated(), Sink(), Raising()
SINKS = (A, B, F, G, H, C)


@log_foundry.trace
def work() -> None:
    log_foundry.info("in")


def once(owner: object, name: str, fault: BaseException) -> None:
    real = getattr(owner, name)
    fired: list[bool] = []

    def faulty(*args: object, **kwargs: object) -> object:
        if not fired:
            fired.append(True)
            raise fault
        return real(*args, **kwargs)

    setattr(owner, name, faulty)


def main_workload() -> None:
    log_foundry.configure(service="sweep", sink=A)
    log_foundry.info("a")
    work()
    log_foundry.flush(timeout=2)
    log_foundry.configure(sink=B)
    log_foundry.info("b")
    work()
    log_foundry.flush(timeout=2)
    log_foundry.health()
    log_foundry.configure(sink_factory=lambda: F)
    log_foundry.info("f")
    work()
    log_foundry.flush(timeout=2)
    real_stream = api._console._stream
    api._console._stream = FailingStream()
    log_foundry.info("echoed", echo=True)
    api._console._stream = real_stream
    real_build = api.build_event

    @log_foundry.trace
    def in_span_fault() -> None:
        log_foundry.info("lost in the span")

    once(api, "build_event", RuntimeError("an in-span build fails"))
    try:
        in_span_fault()
    finally:
        api.build_event = real_build
    real_close = decorator._close_span
    once(decorator, "_close_span", RuntimeError("a close fails"))
    try:
        work()
    finally:
        decorator._close_span = real_close
    context = contextvars.copy_context()
    context.run(log_foundry.continue_trace, "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")
    release, entered = threading.Event(), threading.Event()

    @log_foundry.trace
    def parked() -> None:
        log_foundry.info("parked in another thread's span")
        entered.set()
        release.wait(5)

    other = threading.Thread(target=parked)
    other.start()
    entered.wait(5)
    log_foundry.flush(timeout=2)
    release.set()
    other.join(5)
    log_foundry.read_losses(B)
    worker.DEFAULT_SWAP_TIMEOUT = 0.2
    log_foundry.configure(sink=G)
    for _ in range(12):
        work()
    log_foundry.configure(sink=H)
    log_foundry.configure(sink=C)
    log_foundry.info("c")
    work()
    gated = _lifecycle._state._worker
    log_foundry.shutdown(timeout=0.2)
    G.open.set()
    if gated is not None:
        gated._thread.join(5)
    work()
    log_foundry.info("after the shutdown")
    log_foundry.health()
    log_foundry.shutdown(timeout=1)


def late_workload() -> None:
    log_foundry.info("the default sink's build")
    log_foundry.configure(service="sweep", sink=A)
    log_foundry.info("a")
    real_close_owed = _lifecycle._close_owed
    built: list[bool] = []

    def close_owed(*args: object, **kwargs: object) -> bool:
        if not built:
            built.append(True)
            builder = threading.Thread(target=work)
            builder.start()
            builder.join(5)
        return real_close_owed(*args, **kwargs)

    _lifecycle._close_owed = close_owed
    try:
        log_foundry.shutdown(timeout=2)
    finally:
        _lifecycle._close_owed = real_close_owed
    log_foundry.info("after the shutdown")
    log_foundry.shutdown(timeout=1)


def direct_workload() -> None:
    """Drives the main-thread sections no public sequence here reaches, each in a valid state.

    They are the hand machinery SPEC-098 deletes, which runs on the main thread until it does,
    ``_close_owed``'s put-back after a fault, the overflow count a sweep's hand-in makes, and the
    loss read nothing calls. Each is reached by calling it as the library does, so the question the
    sweep asks of it, whether a handler's calls re-entering its lock leave the process sound, is
    the same as on a public path.
    """
    from log_foundry._lifecycle import _Op, _Operation, _Rule

    log_foundry.configure(service="sweep", sink=A)
    work()
    log_foundry.flush(timeout=2)
    state = _lifecycle._state
    live = state._worker
    stop = _Operation(_Op.STOP, 1.0)
    state._stops_running.add(stop)
    try:
        _lifecycle._hand_retargeted(B, state._stop)
    finally:
        state._stops_running.discard(stop)
    closing = state._closes.register(B, _Rule.GRACED)
    state._closes.defer_hand(B)
    state._closes.unmark(B)
    state._closes.defer_hand(B)
    _lifecycle._pay_marks_after_stop()
    state._closes.finish(B, closing)
    _lifecycle._hand_after_close(B)
    _lifecycle._settle_signal(B, threading.Event())
    _lifecycle.rearm_if_released(B)
    if live is not None:
        live._count_overflow(0)
    decorator._read_losses()
    log_foundry.info("a, owed again")
    real_choice = _lifecycle._inline_close_choice
    once(_lifecycle, "_inline_close_choice", RuntimeError("the inline choice fails"))
    try:
        _lifecycle._close_owed(None, bystander_wait=False)
    except RuntimeError as exc:
        assert "inline choice" in str(exc), exc
    finally:
        _lifecycle._inline_close_choice = real_choice
    log_foundry.shutdown(timeout=2)


handled: list[str] = []


def on_term(signum: int, frame: object) -> None:
    log_foundry.info("from the handler")
    work()
    log_foundry.info("from the handler again")
    handled.append("done")


signal.signal(signal.SIGTERM, on_term)
worker.Worker.__init__.__kwdefaults__["max_queue"] = 4


def ast_sections() -> dict[tuple[str, int], str]:
    import ast
    import pathlib
    import re

    locks = re.compile(
        r"^(_state\._lock|self\._lock|_state\._closes\._lock|self\._closes\._lock|_config_lock"
        r"|_sweep_lock|_sweep_hold|_sweep_hold\.within\(.*\)|_loss_lock|decorator\._loss_lock"
        r"|self\._queue\.mutex|registry\.lock|_ledger\._registry\.lock|_state\._owned_lock)$"
    )
    found: dict[tuple[str, int], str] = {}
    root = pathlib.Path(_lifecycle.__file__).parent
    for path in sorted(root.glob("*.py")):
        tree = ast.parse(path.read_text())
        for fn in ast.walk(tree):
            if not isinstance(fn, ast.FunctionDef):
                continue
            for node in ast.walk(fn):
                if isinstance(node, ast.With) and any(
                    locks.match(ast.unparse(item.context_expr)) for item in node.items
                ):
                    first = node.body[0].lineno
                    found[(path.name, first)] = f"{path.name}:{fn.name}:{first}"
    return found


SECTIONS = ast_sections()
sites: list[str] = []
state = {"inside": False, "fired": False}


def watched(filename: str) -> bool:
    return "log_foundry" in filename or filename.endswith(("queue.py", "threading.py"))


def holds_a_lock() -> bool:
    if _lifecycle.in_section():
        return True
    return bool(
        _ledger._registry.lock._is_owned()
        or _lifecycle._state._owned_lock._is_owned()
        or api._console._lock.locked()
    )


def tracer(frame, event, arg):  # type: ignore[no-untyped-def]
    if not watched(frame.f_code.co_filename):
        return None

    def local(frame, event, arg):  # type: ignore[no-untyped-def]
        if event != "line" or state["fired"]:
            return local
        inside = holds_a_lock()
        name = os.path.basename(frame.f_code.co_filename)
        key = SECTIONS.get((name, frame.f_lineno))
        if key is None and inside and not state["inside"]:
            key = f"{name}:{frame.f_code.co_name}:{frame.f_lineno}"
        if key is not None and inside:
            if key not in sites:
                sites.append(key)
                if len(sites) - 1 == TARGET:
                    state["fired"] = True
                    sys.settrace(None)
                    signal.raise_signal(signal.SIGTERM)
        state["inside"] = inside
        return local

    return local


sys.settrace(tracer)
{"main": main_workload, "late": late_workload, "direct": direct_workload}[WORKLOAD]()
sys.settrace(None)
if MODE == "enumerate":
    print("\n".join("SITE " + site for site in sites))
    if len(sys.argv) > 3:
        print("\n".join("UNSEEN " + site for site in sorted(set(SECTIONS.values()) - set(sites))))
    sys.exit(0)
ledger = log_foundry.health().events
problems = []
if ledger.unaccounted:
    problems.append(f"unaccounted={ledger.unaccounted}")
closes = "closes=" + ",".join(str(sink.closes) for sink in SINKS)
if log_foundry.current_trace_context() is not None:
    problems.append("a span is still current")
if TARGET >= 0 and handled != ["done"]:
    problems.append(f"handler={handled} fired={state['fired']}")
print(
    "ok" if not problems else "BROKEN " + "; ".join(problems),
    closes,
    f"accepted={ledger.accepted} delivered={ledger.delivered} stranded={ledger.stranded}",
)
