"""Written for SPEC-101 on 2026-11-17, adapted from its spec reviewer's probe; not the audit's.

What FR-002's order costs when a holder's close is slow. An unconfirmed configure(A), then
configure(H) with H holding A, so one take of shutdown(timeout=0.5) takes both H and A. H is a
caller's wrapper whose close leaves A alone and takes 3 s (slow), one whose close never returns
(hung: it waits on an event nothing sets, bounded at 20 s), or a shipped MultiSink(A, Z) whose
other member Z takes 3 s to close (multi). Prints, in seconds from the shutdown() call, when it
returned and when A was closed, and A's closes 4 s later. Run against SPEC-097's prototype tree
and SPEC-101's, never -I:

  PYTHONPATH=<tree>/src python -B docs/audits/2026-11-14-probes/lifecycle/spec101_slow_holder.py
"""
import faulthandler
import subprocess
import sys
import threading
import time

if len(sys.argv) == 1:
    for mode in ("slow", "hung", "multi"):
        r = subprocess.run([sys.executable, "-B", __file__, mode], capture_output=True, text=True,
                           timeout=60)
        print(f"{mode:5s} exit={r.returncode} {r.stdout.strip()}")
    sys.exit(0)

import warnings  # noqa: E402

warnings.filterwarnings("ignore")
faulthandler.dump_traceback_later(40, exit=True)
import log_foundry  # noqa: E402
from log_foundry import worker as worker_mod  # noqa: E402
from log_foundry.sinks.multi import MultiSink  # noqa: E402

START = [0.0]


class Rec:
    def __init__(self, close_s=0.0):
        self.inside = 0
        self.slow_emit = 0.0
        self.close_s = close_s
        self.closed_at: list[float] = []

    def emit(self, batch):
        self.inside += 1
        time.sleep(self.slow_emit)
        self.inside -= 1

    def close(self):
        time.sleep(self.close_s)
        self.closed_at.append(round(time.monotonic() - START[0], 2))


class Holder:
    def __init__(self, m, hang):
        self.m = m
        self.hang = hang

    def emit(self, batch):
        self.m.emit(batch)

    def close(self):
        if self.hang:
            threading.Event().wait(20)
        else:
            time.sleep(3)


@log_foundry.trace
def span():
    pass


mode = sys.argv[1]
a = Rec()
log_foundry.configure(service="slow-holder", sink=a)
span()
log_foundry.flush(timeout=5)
a.slow_emit = 0.5
span()
worker_mod.DEFAULT_SWAP_TIMEOUT = 0.1
H = MultiSink(a, Rec(close_s=3)) if mode == "multi" else Holder(a, hang=mode == "hung")
log_foundry.configure(sink=H)
worker_mod.DEFAULT_SWAP_TIMEOUT = 5.0
unconfirmed = log_foundry.health().incomplete_swaps
while a.inside:
    time.sleep(0.01)
a.slow_emit = 0.0
span()
log_foundry.flush(timeout=5)
START[0] = time.monotonic()
log_foundry.shutdown(timeout=0.5)
returned = round(time.monotonic() - START[0], 2)
at_return = list(a.closed_at)
time.sleep(4)
print(f"incomplete_swaps={unconfirmed} shutdown returned at {returned}; A closed at return: "
      f"{at_return}; A closed 4 s later: {a.closed_at}")
