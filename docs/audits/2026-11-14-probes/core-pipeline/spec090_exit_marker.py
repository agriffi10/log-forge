"""SPEC-090, written on 2026-11-15 against 3e27a0a6 and not one of the audit's own probes: exit batches and a flush marker.
A Worker with batch_size=1 and a 60 s interval measures one batch (its first, held 0.2 s by a gate), then holds at exit 30
two-event submissions with a flush marker after the fifth. The sink logs each batch's size and whether the marker had been
answered when it arrived. Today: the 60 events go in one call, the marker after it. Under SPEC-090, with the cap patched to
10 (worker._BACKLOG_BATCH_CAP, when the tree has it): six batches of 10, the marker answered after the first.
Usage: python spec090_exit_marker.py
"""

import threading, time
import log_foundry.worker as worker
from log_foundry.worker import Worker, _FlushMarker

if hasattr(worker, "_BACKLOG_BATCH_CAP"):
    worker._BACKLOG_BATCH_CAP = 10
M = []


class Rec:
    def __init__(self):
        self.log = []
        self.gate = threading.Event()

    def emit(self, batch):
        self.gate.wait()
        self.log.append((len(batch), bool(M) and M[0].event.is_set()))

    def close(self):
        pass


s = Rec()
w = Worker(s, flush_interval=60.0, batch_size=1)
w.submit([{"i": 0}, {"i": 1}])
while not w._in_flight:
    time.sleep(0.01)
for _ in range(5):
    w.submit([{"i": 0}, {"i": 1}])
m = _FlushMarker(w.failed_batches)
M.append(m)
w._queue.put_nowait(m)
for _ in range(25):
    w.submit([{"i": 0}, {"i": 1}])
st = threading.Thread(target=w.stop, args=(10.0,))
st.start()
time.sleep(0.2)
s.gate.set()
st.join()
print("(batch events, marker already answered):", s.log, "delivered:", m.delivered)
