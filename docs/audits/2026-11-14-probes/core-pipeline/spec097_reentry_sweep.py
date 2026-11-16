"""SPEC-097, written while revising it after its spec review: a handler's level call and traced
call landing inside every library lock section a workload's main thread enters.

Usage: spec097_reentry_sweep.py            enumerate the sections, then run one child per section
       spec097_reentry_sweep.py enumerate  print the sections only
       spec097_reentry_sweep.py fire <k>   one child: interrupt the k-th section

Run with ``PYTHONPATH=<tree>/src`` against a tree carrying the prototype: it reads
``_lifecycle.in_section()``, as the spec reviewer's ``sig_any.py`` does. The workload, on the main
thread, is ``configure(sink=A)``, a first orphan log, a traced call, ``flush()``,
``configure(sink=B)``, an orphan log, a traced call, ``flush()``, ``health()`` and ``shutdown()``.
A **section** is either the first statement inside a ``with`` on one of the lifecycle locks
(``_state._lock``, ``self._lock``, the close registry's lock, ``_config_lock``, ``_sweep_lock`` and
``_loss_lock``), read from the AST of ``src/log_foundry``, or a line in ``queue.py`` or
``threading.py`` run holding a library lock right after a line that held none; the sections a
workload reaches are found by running it under ``sys.settrace``, not listed. ``enumerate`` also
prints, after a ``--`` line, every AST section the workload did not reach. The workload goes on to
a sink that raises, an orphan log and a traced call into it, and a traced call and a log after the
shutdown, so the loss paths and the after-shutdown counting are among them. For each one a
fresh child runs the workload again, raises SIGTERM at the first entry of that section, and the
handler makes an orphan log, a traced call that logs once, and a second orphan log, re-entering the
section's lock where it needs it. The child passes when the workload ends with the ledger
balanced (``unaccounted == 0``), each sink closed as often as in a control child that fires
nothing, no span current, and the handler's three calls made. A row also prints how many events
were delivered, since a handler interrupting the stop logs after it and those events are counted,
not delivered. A hang is the 10 s watchdog's exit 1. Every child is bounded, and the parent prints
the control, one line per section, a total, and the AST sections the workload did not reach.
"""

import concurrent.futures
import faulthandler
import os
import subprocess
import sys

MODE = sys.argv[1] if len(sys.argv) > 1 else "all"

if MODE == "all":
    listing = subprocess.run(
        [sys.executable, "-B", __file__, "enumerate"], capture_output=True, text=True, timeout=60
    ).stdout.split()
    unreached = (
        subprocess.run(
            [sys.executable, "-B", __file__, "enumerate", "unreached"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        .stdout.split("--")[-1]
        .split()
    )

    def one(k: int) -> tuple[str, int | str, str]:
        try:
            r = subprocess.run(
                [sys.executable, "-B", __file__, "fire", str(k)],
                capture_output=True,
                text=True,
                timeout=30,
            )
        except subprocess.TimeoutExpired:
            return listing[k], "timeout", ""
        return listing[k], r.returncode, (r.stdout.strip().splitlines() or ["(nothing)"])[-1]

    control = subprocess.run(
        [sys.executable, "-B", __file__, "fire", "-1"], capture_output=True, text=True, timeout=60
    ).stdout.split()
    control_closes = next(word for word in control if word.startswith("closes="))
    print(f"control (nothing fired): {' '.join(control)}")
    with concurrent.futures.ThreadPoolExecutor(8) as pool:
        rows = list(pool.map(one, range(len(listing))))
    bad = 0
    for site, code, line in rows:
        ok = code == 0 and line.startswith("ok") and control_closes in line.split()
        bad += not ok
        print(f"{'ok  ' if ok else 'FAIL'} {site:60s} exit={code} {line[:160]}")
    print(f"sections={len(rows)} failed={bad} ast_sections_not_reached={len(unreached)}")
    for site in unreached:
        print(f"not reached {site}")
    sys.exit(0)

faulthandler.dump_traceback_later(10, exit=True, file=open(os.devnull, "w"))
import signal  # noqa: E402

import log_foundry  # noqa: E402
from log_foundry import _lifecycle  # noqa: E402

TARGET = int(sys.argv[2]) if MODE == "fire" else -1


class Sink:
    def __init__(self) -> None:
        self.events = 0
        self.closes = 0

    def emit(self, batch: list[dict]) -> None:
        self.events += len(batch)

    def close(self) -> None:
        self.closes += 1


A, B = Sink(), Sink()


@log_foundry.trace
def work() -> None:
    log_foundry.info("in")


class Raising(Sink):
    def emit(self, batch: list[dict]) -> None:
        raise RuntimeError("down")


C = Raising()


def workload() -> None:
    log_foundry.configure(service="sweep", sink=A)
    log_foundry.info("a")
    work()
    log_foundry.flush(timeout=2)
    log_foundry.configure(sink=B)
    log_foundry.info("b")
    work()
    log_foundry.flush(timeout=2)
    log_foundry.health()
    log_foundry.configure(sink=C)
    log_foundry.info("c")
    work()
    log_foundry.flush(timeout=0.5)
    log_foundry.health()
    log_foundry.shutdown(timeout=3)
    work()
    log_foundry.info("after the shutdown")
    log_foundry.health()
    log_foundry.shutdown(timeout=1)


handled: list[str] = []


def on_term(signum: int, frame: object) -> None:
    log_foundry.info("from the handler")
    work()
    log_foundry.info("from the handler again")
    handled.append("done")


signal.signal(signal.SIGTERM, on_term)


def ast_sections() -> dict[tuple[str, int], str]:
    import ast
    import pathlib
    import re

    locks = re.compile(
        r"^(_state\._lock|self\._lock|_state\._closes\._lock|self\._closes\._lock|_config_lock"
        r"|_sweep_lock|_loss_lock)$"
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


def tracer(frame, event, arg):  # type: ignore[no-untyped-def]
    if not watched(frame.f_code.co_filename):
        return None

    def local(frame, event, arg):  # type: ignore[no-untyped-def]
        if event != "line" or state["fired"]:
            return local
        inside = _lifecycle.in_section()
        name = os.path.basename(frame.f_code.co_filename)
        key = SECTIONS.get((name, frame.f_lineno))
        if key is None and inside and not state["inside"] and name in ("queue.py", "threading.py"):
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
workload()
sys.settrace(None)
if MODE == "enumerate":
    print("\n".join(sites))
    if len(sys.argv) > 2:
        print("--")
        print("\n".join(sorted(set(SECTIONS.values()) - set(sites))))
    sys.exit(0)
ledger = log_foundry.health().events
problems = []
if ledger.unaccounted:
    problems.append(f"unaccounted={ledger.unaccounted}")
closes = f"closes={A.closes},{B.closes},{C.closes}"
if log_foundry.current_trace_context() is not None:
    problems.append("a span is still current")
if TARGET >= 0 and handled != ["done"]:
    problems.append(f"handler={handled} fired={state['fired']}")
print(
    "ok" if not problems else "BROKEN " + "; ".join(problems),
    closes,
    f"accepted={ledger.accepted} delivered={ledger.delivered} stranded={ledger.stranded}",
)
