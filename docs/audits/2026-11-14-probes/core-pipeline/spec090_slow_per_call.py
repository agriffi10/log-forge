"""SPEC-090, written on 2026-11-16 from the second diff review's probe: a per-call sink slower than half the flush interval.
The sink sleeps a fixed time per call, whatever the batch's size. The size target after a batch of e events is
e / cost * interval / 2, so it multiplies by interval / (2 * cost) each batch: below half an interval it climbs to the cap,
above it it never grows past the count trigger's batch. 3,000 two-event spans are queued behind a gated first call.
Usage: python spec090_slow_per_call.py <seconds per call>
"""

import sys, threading, time
from log_foundry.worker import Worker

COST = float(sys.argv[1])
handed = []
gate = threading.Event()


class PerCall:
    def emit(self, batch):
        handed.append(len(batch))
        if len(handed) == 1:
            gate.wait(30)
        time.sleep(COST)

    def close(self):
        pass


w = Worker(PerCall(), batch_size=10, flush_interval=1.0, max_queue=100_000)
for _ in range(3000):
    w.submit([{"a": 1}, {"a": 2}])
time.sleep(0.1)
gate.set()
time.sleep(8)
print(f"cost={COST}s first 14 batches: {handed[:14]} target={w._target:.1f}")
w._stop.set()
