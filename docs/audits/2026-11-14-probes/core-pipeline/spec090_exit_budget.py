"""SPEC-090, adapted on 2026-11-15 from its spec review's probe, against 3e27a0a6: a slow-failing sink on a batched exit drain.
The sink's first emit waits on a gate; every later one sleeps 0.5 s and raises. One span goes into the gated emit, 30 more are
queued, and shutdown(timeout=1.2) runs through the public API, so close() is counted. "tree" runs the library as it is.
"batched" replaces Worker._final_drain with a per-batch exit drain that makes every batch's first attempt, the shape SPEC-090
first specified: the join expires and close() does not run. SPEC-062's case is today's 2 attempts and one close.
Usage: python spec090_exit_budget.py tree|batched
"""

import queue, sys, threading, time
from typing import cast
import log_foundry as lf
from log_foundry import worker
from log_foundry.worker import _FlushMarker, _SHUTDOWN


class SlowFailing:
    def __init__(self):
        self.gate = threading.Event()
        self.attempts = 0
        self.first = True
        self.closes = 0

    def emit(self, batch):
        if self.first:
            self.first = False
            self.gate.wait()
            return
        self.attempts += 1
        time.sleep(0.5)
        raise RuntimeError("down")

    def close(self):
        self.closes += 1


def batched_final_drain(self, pending):
    while True:
        try:
            item = self._queue.get_nowait()
        except queue.Empty:
            break
        if isinstance(item, _FlushMarker) or item is None or item is _SHUTDOWN:
            continue
        pending.append(cast("list[dict[str, object]]", item))
        if len(pending) >= self.batch_size:
            self._emit_pending(pending)
    self._emit_pending(pending)


if sys.argv[1] == "batched":
    worker.Worker._final_drain = batched_final_drain
sink = SlowFailing()
lf.configure(sink=sink)


@lf.trace
def work():
    pass


work()
time.sleep(1.2)  # the time trigger takes the first span into the gated emit
for _ in range(30):
    work()
threading.Timer(0.05, sink.gate.set).start()
t0 = time.monotonic()
lf.shutdown(timeout=1.2)
took = time.monotonic() - t0
h = lf.health()
print(
    f"{sys.argv[1]}: shutdown took {took:.2f}s; join expired={h.stopped_reason == 'ShutdownTimeout'}; "
    f"attempts={sink.attempts}; close() ran {sink.closes} time(s); failed_batches={h.failed_batches}"
)
