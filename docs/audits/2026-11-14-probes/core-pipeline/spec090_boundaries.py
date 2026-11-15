"""SPEC-090, written on 2026-11-15 against 3e27a0a6 and not one of the audit's own probes: the size target's resets.
"abandon": under a backlog, the sink's second call raises; prints the batch sizes delivered around it. Under SPEC-090 the
batch after the failed one is back to ten spans. "swap": a Worker.retarget to a second sink under a backlog; under SPEC-090
the new sink's first batch is ten spans, because a target applies only to the sink it was measured on. Today every batch
is ten spans in both.
Usage: python spec090_boundaries.py abandon|swap
"""

import sys, threading, time
from log_foundry.worker import Worker


def ev():
    return [{"i": 0}, {"i": 1}]


class Rec:
    def __init__(self, fail_on=()):
        self.sizes = []
        self.fail_on = set(fail_on)
        self.calls = 0
        self.gate = threading.Event()
        self.gate.set()

    def emit(self, batch):
        self.gate.wait()
        self.calls += 1
        if self.calls in self.fail_on:
            raise RuntimeError("down")
        self.sizes.append(len(batch))

    def close(self):
        pass


if sys.argv[1] == "abandon":
    s = Rec(fail_on={2})
    w = Worker(s, max_retries=0)
    s.gate.clear()
    for _ in range(3000):
        w.submit(ev())
    s.gate.set()
    time.sleep(1.0)
    print("abandon: delivered batch sizes", s.sizes[:6], "failed_batches", w.failed_batches)
else:
    a = Rec()
    w = Worker(a)
    a.gate.clear()
    for _ in range(3000):
        w.submit(ev())
    a.gate.set()
    time.sleep(0.5)
    b = Rec()
    b.gate.clear()
    for _ in range(3000):
        w.submit(ev())
    t = threading.Thread(target=w.retarget, args=(b, time.monotonic() + 5))
    t.start()
    time.sleep(0.3)
    b.gate.set()
    t.join()
    for _ in range(3000):
        w.submit(ev())
    time.sleep(0.5)
    print("swap: first sink's batches", a.sizes[:4], "second sink's batches", b.sizes[:4])
w.stop(5.0)
