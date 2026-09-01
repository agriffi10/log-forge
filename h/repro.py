"""SPEC-046 reproduction: the exit close runs the owed sinks in sequence.

  python repro.py <n_slow_sinks> <seconds_each>
"""
import sys, threading, time
import log_foundry
from log_foundry import _lifecycle

n = int(sys.argv[1]); secs = float(sys.argv[2])


class SlowClose:
    log_foundry_stop_signal = None

    def __init__(self, name, seconds):
        self.name, self.seconds = name, seconds
        self.closed_at = None
        self.started_at = None

    def emit(self, batch):
        pass

    def close(self):
        self.started_at = time.monotonic()
        time.sleep(self.seconds)
        self.closed_at = time.monotonic()


sinks = [SlowClose(f"s{i}", secs) for i in range(n)]
log_foundry.configure(service="t", sink=sinks[0])
log_foundry.info("arm the first")
for s in sinks[1:]:
    _lifecycle._note_orphan_emit(s)

owed = len(_lifecycle._state._orphan_owed)
t0 = time.monotonic()
log_foundry.shutdown(timeout=1.0)
elapsed = time.monotonic() - t0

print(f"owed sinks     : {owed}")
print(f"per-close cost : {secs}s each")
print(f"shutdown budget: 1.0s")
print(f"shutdown took  : {elapsed:.2f}s")
starts = sorted(s.started_at for s in sinks if s.started_at)
overlap = "CONCURRENT" if len(starts) > 1 and (starts[-1] - starts[0]) < secs else "SEQUENTIAL"
print(f"closes ran     : {overlap}")
print(f"expected if sequential: ~{n * secs:.1f}s   if concurrent: ~{secs:.1f}s")
