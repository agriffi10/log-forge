"""SPEC-090, written on 2026-11-15 against 3e27a0a6 and not one of the audit's own probes: what a full queue holds in memory.
10,000 two-event spans are queued behind a gated sink and tracemalloc reads what they hold. SPEC-090 keeps the queue at
10,000 submissions for this reason: the queue is the library's memory bound, and a batch only references what it holds.
Usage: python spec090_queue_memory.py
"""

import threading, time, tracemalloc, gc
import log_foundry as lf


class Gate:
    def __init__(self):
        self.g = threading.Event()

    def emit(self, b):
        self.g.wait()

    def close(self):
        pass


s = Gate()
lf.configure(sink=s, service="svc")


@lf.trace
def work():
    pass


work()
time.sleep(1.2)  # the time trigger takes the first span into the gated emit
gc.collect()
tracemalloc.start()
base = tracemalloc.get_traced_memory()[0]
for _ in range(10_000):
    work()
gc.collect()
held = tracemalloc.get_traced_memory()[0] - base
h = lf.health()
print(
    f"queued={h.queued} dropped={h.dropped} held={held / 1e6:.1f} MB, {held / max(h.queued, 1):.0f} B per queued span"
)
s.g.set()
lf.flush(30)
