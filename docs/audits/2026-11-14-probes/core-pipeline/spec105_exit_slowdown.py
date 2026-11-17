"""SPEC-105 groundwork, written on 2026-11-17 against 1e9ab67e: a request-shaped sink that slows during the exit drain.
Each emit costs ceil(n / CHUNK) requests; a request costs FAST_R s until SLOW_AFTER emits have been made after the stop,
then SLOW s. The start is either `unmeasured` (nothing is measured before the stop, so the size target is 0; the steady
batch carrying the first span is held behind a gate and fails 0.2 s after `stop` begins, which keeps the target at 0 and
leaves the exit drain the timeout less about 0.2 s: SPEC-105's shape) or `measured` (the worker delivers normally first,
so SPEC-090's rate rule has sized the target before the stop). `healthy` sets SLOW equal to FAST_R. Prints how long `Worker.stop` took, whether
the join expired, the batch sizes handed to the sink after the stop, and what it had delivered when `stop` returned.
Usage: python spec105_exit_slowdown.py <unmeasured|measured> <slowdown|healthy> <spans> <timeout> [chunk] [fast_r] [slow] [slow_after]
"""

import io
import math
import sys
import threading
import time

from log_foundry.worker import Worker

START, SHAPE, SPANS, TIMEOUT = sys.argv[1], sys.argv[2], int(sys.argv[3]), float(sys.argv[4])
CHUNK = int(sys.argv[5]) if len(sys.argv) > 5 else 10
FAST_R = float(sys.argv[6]) if len(sys.argv) > 6 else 0.0005
SLOW = float(sys.argv[7]) if len(sys.argv) > 7 else 0.3
SLOW_AFTER = int(sys.argv[8]) if len(sys.argv) > 8 else 2
if SHAPE == "healthy":
    SLOW = FAST_R
gate, entered, stopping = threading.Event(), threading.Event(), threading.Event()
after_stop, delivered = [], [0]


class Sink:
    def emit(self, events):
        if any("first" in e for e in events):
            entered.set()
            gate.wait(30)
            raise ConnectionError("down")
        per = FAST_R
        if stopping.is_set():
            after_stop.append(len(events))
            if len(after_stop) > SLOW_AFTER:
                per = SLOW
        time.sleep(math.ceil(len(events) / CHUNK) * per)
        delivered[0] += len(events)

    def close(self):
        pass


w = Worker(Sink(), batch_size=10, flush_interval=1.0, max_queue=100_000)
if START == "unmeasured":
    w.submit([{"first": 1}, {"a": 2}])
    time.sleep(0.1)
    for _ in range(SPANS):
        w.submit([{"a": 1}, {"a": 2}])
    entered.wait(5)
    threading.Timer(0.2, gate.set).start()
else:
    for _ in range(SPANS):
        w.submit([{"a": 1}, {"a": 2}])
    time.sleep(0.02)
buf = io.StringIO()
old, sys.stderr = sys.stderr, buf
stopping.set()
t0 = time.monotonic()
w.stop(TIMEOUT)
took = time.monotonic() - t0
at_return = delivered[0]
sys.stderr = old
err = buf.getvalue()
print(f"{START} {SHAPE} chunk={CHUNK} t={TIMEOUT}: took={took:.2f}s expired={'timed out' in err} "
      f"calls_after_stop={len(after_stop)} first_after_stop={after_stop[:4]} largest={max(after_stop or [0])} "
      f"delivered_at_return={at_return} of {2 * SPANS}")
