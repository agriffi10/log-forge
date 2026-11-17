"""SPEC-103, written on 2026-11-17: the last steady batch fails, so the size target is 0 at the stop, and the sink then recovers.
At exit it is healthy but costs MS milliseconds per event (or, with a fifth argument `dead`, sleeps that long and raises). The
steady batch carrying the first span waits on a gate and fails every attempt; the spans are queued behind it and `Worker.stop`
runs the exit drain under TIMEOUT. It prints the time, the batch sizes handed to the sink (one entry per attempt), and the lines.
A closure that read a zero target as the cap at exit hands this sink 1,000 events in its first exit batch, which no budget can
decline, so the join expires and the sink is left open; today the first exit batch is ten spans and the budget sizes the rest.
The stop is claimed only once the gated first batch is inside the sink, so that batch is never an exit batch
(revised 2026-11-17: before, a stop claimed first made it one, and 3 of 18 runs differed).
Usage: python spec103_recovered_slow_exit.py <ms_per_event> <spans> <timeout> [dead]
"""

import io, sys, threading, time
from log_foundry.worker import Worker

MS, SPANS, TIMEOUT = float(sys.argv[1]) / 1000, int(sys.argv[2]), float(sys.argv[3])
DEAD = len(sys.argv) > 4 and sys.argv[4] == "dead"
gate = threading.Event()
entered = threading.Event()
sizes = []


class Sink:
    def emit(self, events):
        if any("first" in e for e in events):
            entered.set()
            gate.wait(30)
            raise ConnectionError("down")
        sizes.append(len(events))
        time.sleep(MS * len(events))
        if DEAD:
            raise TimeoutError("each event timed out")

    def close(self):
        pass


w = Worker(Sink(), batch_size=10, flush_interval=1.0, max_queue=100_000)
w.submit([{"first": 1}, {"a": 2}])
time.sleep(0.1)
for _ in range(SPANS):
    w.submit([{"a": 1}, {"a": 2}])
buf = io.StringIO()
old = sys.stderr
sys.stderr = buf
entered.wait(5)
threading.Timer(0.2, gate.set).start()
t0 = time.monotonic()
w.stop(TIMEOUT)
took = time.monotonic() - t0
sys.stderr = old
err = buf.getvalue()
lines = [l for l in err.splitlines() if l.startswith("log-foundry:")]
print(
    f"{'dead' if DEAD else 'recovered'} {MS * 1000:g}ms/event spans={SPANS} timeout={TIMEOUT}: took={took:.2f}s "
    f"join expired={'timed out' in err} attempts={len(sizes)} first={sizes[:4]} largest={max(sizes or [0])} "
    f"events handed={sum(sizes)} lines={len(lines)}"
)
for l in lines:
    print("  ", l[:170])
