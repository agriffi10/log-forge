"""Written for SPEC-101 on 2026-11-17 against a38bd80c, not one of the audit's probes.

Whether the record SPEC-101's prototype adds stays bounded, which SPEC-087 measured a deferred
release not to: 200 rounds of configure(A), one log, configure(W(A)), one log, with no span, for
W a wrapper that leaves its member alone (Leaves) and one that closes it (Mine), then shutdown().
Prints A's closes, overlapping closes and refused events, the owed record's size, and the sizes of
the prototype's two records, the after-holder record and the record of sinks the library wrote to
(_lifecycle._AFTER and _ARMED; "absent" on a tree without them), at the last round and after
shutdown(). An entry of either is dropped once its sink is collected, so the configured holder,
still alive, keeps one. Run from the repository root, never -I:

  PYTHONPATH=<tree>/src python -B docs/audits/2026-11-14-probes/lifecycle/spec101_rounds.py
"""
import faulthandler
import subprocess
import sys
import threading
import time

if len(sys.argv) == 1:
    for wrapper in ("Leaves", "Mine"):
        r = subprocess.run([sys.executable, "-B", __file__, wrapper], capture_output=True,
                           text=True, timeout=120)
        print(f"{wrapper:6s} exit={r.returncode} {r.stdout.strip()} {r.stderr.strip()[:200]}")
    sys.exit(0)

import log_foundry
from log_foundry import _lifecycle

faulthandler.dump_traceback_later(100, exit=True)


class Rec:
    def __init__(self):
        self.lock = threading.Lock()
        self.closes = self.closing = self.overlaps = self.refused = self.events = 0
        self.closed = False

    def emit(self, batch):
        with self.lock:
            if self.closed:
                self.refused += len(batch)
                raise RuntimeError("closed")
            self.events += len(batch)

    def close(self):
        with self.lock:
            self.closing += 1
            self.overlaps += self.closing > 1
        time.sleep(0.001)
        with self.lock:
            self.closing -= 1
            self.closes += 1
            self.closed = True


class Leaves:
    def __init__(self, inner):
        self.inner = inner

    def emit(self, batch):
        self.inner.emit(batch)

    def close(self):
        pass


class Mine(Leaves):
    def close(self):
        self.inner.close()


def after():
    record = getattr(_lifecycle, "_AFTER", None)
    armed = getattr(_lifecycle, "_ARMED", None)
    if record is None:
        return "absent"
    return f"{len(record)} armed_record={'absent' if armed is None else len(armed)}"


W = {"Leaves": Leaves, "Mine": Mine}[sys.argv[1]]
a = Rec()
log_foundry.configure(service="rounds")
for _ in range(200):
    log_foundry.configure(sink=a)
    log_foundry.info("x")
    log_foundry.configure(sink=W(a))
    log_foundry.info("y")
mid = (a.closes, len(_lifecycle._state._closes.owed), after())
log_foundry.shutdown(timeout=5)
time.sleep(0.2)
print(
    f"after 200 rounds: A.closes={mid[0]} owed={mid[1]} after_record={mid[2]} | after shutdown: "
    f"A.closes={a.closes} overlaps={a.overlaps} refused={a.refused} events={a.events}/400 "
    f"owed={len(_lifecycle._state._closes.owed)} after_record={after()}"
)
