"""SPEC-103, from its spec review on 2026-11-17: a sink that fails slowly at exit, with the worker's own retries chosen.
spec103_recovered_slow_exit.py's shape with the flush interval and max_retries as arguments. The steady batch carrying the first
span waits on a gate and fails, so the size target is 0 at the stop; the stop is claimed only once that batch is inside the sink.
At exit each call sleeps MS milliseconds per event and then raises (`dead`) or returns (`ok`). It prints whether the join expired,
the batch sizes handed over and each call's duration. With max_retries=0 and a 1.0 s interval, a probe that doubles a failed
batch without bounding it by the longest attempt seen hands the sink an attempt longer than SPEC-090 FR-003's predictor
allows for, and the join can expire.
Usage: python spec103_slow_dead_exit_budget.py <ms_per_event> <spans> <timeout> <interval> <retries> [dead|ok]
"""
import io, sys, threading, time
from log_foundry.worker import Worker

MS, SPANS, TIMEOUT = float(sys.argv[1]) / 1000, int(sys.argv[2]), float(sys.argv[3])
IV, RET = float(sys.argv[4]), int(sys.argv[5])
DEAD = len(sys.argv) > 6 and sys.argv[6] == "dead"
gate = threading.Event()
entered = threading.Event()
sizes = []
durs = []
closes = [0]


class Sink:
    def emit(self, events):
        if any("first" in e for e in events):
            entered.set()
            gate.wait(30)
            raise ConnectionError("down")
        sizes.append(len(events))
        t = time.monotonic()
        time.sleep(MS * len(events))
        durs.append(round(time.monotonic() - t, 3))
        if DEAD:
            raise TimeoutError("each event timed out")

    def close(self):
        closes[0] += 1


w = Worker(Sink(), batch_size=10, flush_interval=IV, max_queue=100_000, max_retries=RET)
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
print(
    f"timeout={TIMEOUT} iv={IV} ret={RET} {'dead' if DEAD else 'ok'}: took={took:.2f} "
    f"expired={'timed out' in err} attempts={len(sizes)} sizes={sizes[:12]} durs={durs[:12]}"
)
