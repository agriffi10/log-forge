"""SPEC-097, written while revising it against ead40798 after its spec review: every public call of
``log_foundry.__all__`` interrupted inside a library lock section by a handler that calls
``shutdown(timeout=2)``.

Usage: spec097_held_sweep.py [call]   (no argument runs every call, each in a fresh process)

Run with ``PYTHONPATH=<tree>/src`` against a tree carrying the prototype: it reads
``_lifecycle.in_section()``. Adapted from the spec reviewer's ``sig_any.py``. The calls are derived
from ``log_foundry.__all__``: every callable there needs a recipe below, and one without a recipe is
printed as ``NO RECIPE``, so a new public call cannot be left out unseen. A recipe runs under
``sys.settrace``; at the first line of any library frame run while the thread holds a library lock
(``in_section()``), SIGTERM is raised and the handler calls ``shutdown(timeout=2)``. Once the public
call has returned, and with no further library call, a row reads whether the process retired and
whether a stop is still held on the thread. A call that never enters a section reads ``fired=[]``.
"""

import faulthandler
import subprocess
import sys
import time

if len(sys.argv) < 2:
    import log_foundry

    names = [
        n
        for n in log_foundry.__all__
        if callable(getattr(log_foundry, n)) and not isinstance(getattr(log_foundry, n), type)
    ]
    names += ["info-first", "configure-sink"]
    for name in names:
        r = subprocess.run(
            [sys.executable, "-B", __file__, name], capture_output=True, text=True, timeout=30
        )
        last = (r.stdout.strip().splitlines() or ["(nothing printed)"])[-1]
        print(f"{name:24s} exit={r.returncode} {last}")
    sys.exit(0)

faulthandler.dump_traceback_later(8, exit=True)
import signal  # noqa: E402

import log_foundry  # noqa: E402
from log_foundry import _lifecycle  # noqa: E402

call = sys.argv[1]


class Sink:
    closes = 0

    def emit(self, batch):
        pass

    def close(self):
        self.closes += 1

    def flush(self):
        pass


sink, other = Sink(), Sink()
log_foundry.configure(service="probe", sink=sink)


@log_foundry.trace
def work():
    log_foundry.info("in")


if call != "info-first":
    work()
    log_foundry.flush(timeout=2)
TP = "00-" + "1" * 32 + "-" + "2" * 16 + "-01"
RECIPES = {
    "configure": lambda: log_foundry.configure(service="x"),
    "configure-sink": lambda: log_foundry.configure(sink=other),
    "continue_trace": lambda: log_foundry.continue_trace(TP),
    "critical": lambda: log_foundry.critical("x"),
    "current_baggage_header": log_foundry.current_baggage_header,
    "current_trace_context": log_foundry.current_trace_context,
    "current_traceparent": log_foundry.current_traceparent,
    "debug": lambda: log_foundry.debug("x"),
    "emit_via_deliver": lambda: log_foundry.emit_via_deliver(sink, []),
    "error": lambda: log_foundry.error("x"),
    "flush": lambda: log_foundry.flush(timeout=1),
    "flush_sink": lambda: log_foundry.flush_sink(sink),
    "get_baggage": log_foundry.get_baggage,
    "get_config": log_foundry.get_config,
    "health": log_foundry.health,
    "info": lambda: log_foundry.info("x"),
    "info-first": lambda: log_foundry.info("first"),
    "read_deliver": lambda: log_foundry.read_deliver(sink),
    "read_losses": lambda: log_foundry.read_losses(sink),
    "reset_context": log_foundry.reset_context,
    "set_baggage": lambda: log_foundry.set_baggage(k="v"),
    "shutdown": lambda: log_foundry.shutdown(timeout=1),
    "trace": work,
    "warning": lambda: log_foundry.warning("x"),
}
if call not in RECIPES:
    print("NO RECIPE")
    sys.exit(2)
took = []


def on_term(signum, frame):
    t = time.monotonic()
    log_foundry.shutdown(timeout=2)
    took.append(round(time.monotonic() - t, 2))


signal.signal(signal.SIGTERM, on_term)
fired = []


def tracer(frame, event, arg):
    if not frame.f_globals.get("__name__", "").startswith("log_foundry"):
        return None

    def local(frame, event, arg):
        if event == "line" and not fired and _lifecycle.in_section():
            fired.append(f"{frame.f_code.co_name}:{frame.f_lineno}")
            signal.raise_signal(signal.SIGTERM)
        return local

    return local


sys.settrace(tracer)
RECIPES[call]()
sys.settrace(None)
waiting = getattr(_lifecycle._sections, "waiting", None) or []
print(f"fired={fired} took={took} retired={_lifecycle._state.retirements > 0} held={len(waiting)}")
