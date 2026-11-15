"""SPEC-090, written on 2026-11-15 against 3e27a0a6 and not one of the audit's own probes: p13's burst made deterministic.
The sink costs 20 ms a call and its first emit waits on a gate, so the drain holds one batch while 30,000 spans are
submitted: the queue fills to its 10,000 and the rest is dropped whatever the machine's speed. The gate then opens and the
probe counts the emit calls and batch sizes that carry the backlog. Today: 1,000 calls of 20 events. Under SPEC-090's rule:
a few calls while the size target is measured, then calls of 1,000 events.
Usage: python spec090_burst_gated.py
"""
import collections, threading, time
import log_foundry as lf

class Gated:
    def __init__(self):
        self.gate = threading.Event(); self.sizes = []; self.n = 0
    def emit(self, batch):
        if not self.sizes and not self.gate.is_set():
            self.gate.wait()
        time.sleep(0.02)
        self.sizes.append(len(batch)); self.n += len(batch)
    def close(self): pass

sink = Gated()
lf.configure(sink=sink)
@lf.trace
def work(): pass
for _ in range(30_000):
    work()
h = lf.health()
t0 = time.monotonic(); sink.gate.set(); r = lf.flush(120); took = time.monotonic() - t0
after = sink.sizes[1:]
print(f"dropped={h.dropped} queued_at_release={h.queued}; after the release: {len(after)} emit call(s), "
      f"largest {max(after)} events, sizes {dict(collections.Counter(after))}; drained in {took:.2f}s; flush={r.ok}")
