"""SPEC-090, written on 2026-11-15 against 3e27a0a6 and not one of the audit's own probes: p13's burst, timed on the caller.
The sink's cost per call is the first argument (0 for an instant sink). It shows what a drain that keeps pace with a burst
costs the application's thread under the GIL: today an instant sink's burst takes longer than a 20 ms sink's, which drops
two thirds of it instead. Wall-clock figures move with the machine's load; compare interleaved runs only.
Usage: python spec090_burst_cost.py <seconds per call> [spans]
"""

import sys, time
import log_foundry as lf

COST = float(sys.argv[1])
TOTAL = int(sys.argv[2]) if len(sys.argv) > 2 else 30_000


class S:
    def __init__(self):
        self.n = 0
        self.calls = 0

    def emit(self, b):
        if COST:
            time.sleep(COST)
        self.n += len(b)
        self.calls += 1

    def close(self):
        pass


s = S()
lf.configure(sink=s)


@lf.trace
def work():
    pass


t0 = time.monotonic()
for _ in range(TOTAL):
    work()
burst = time.monotonic() - t0
lf.flush(60)
h = lf.health()
print(
    f"cost={COST} burst={burst:.2f}s dropped={h.dropped} calls={s.calls} mean={s.n / max(s.calls, 1):.0f}"
)
