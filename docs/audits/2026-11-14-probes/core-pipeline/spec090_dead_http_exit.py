"""SPEC-090, written on 2026-11-16 from the second diff review's probe: a refusing HTTPSink at exit, its own lines counted.
HTTPSink writes a line per failed request, which the exit drain's tallies cannot aggregate, so the lines at exit follow the
number of exit batches. The opener's first call waits on a gate; in `cap` mode it then answers 200, so the target is the cap
when the stop lands, and in `zero` mode it refuses too, so the target is 0. Every later call refuses. The spans are queued
behind it and `Worker.stop(30)` runs the exit drain. It prints the opener calls, the `log-foundry:` lines and failed_batches.
Usage: python spec090_dead_http_exit.py cap|zero <spans> <max_retries>
"""

import io, sys, threading, time
from log_foundry.sinks.http import HTTPSink
from log_foundry.worker import Worker

MODE = sys.argv[1]
SPANS = int(sys.argv[2])
RETRIES = int(sys.argv[3])
gate = threading.Event()
calls = [0]


class Ok:
    status = 200
    headers = {}

    def read(self, n=-1):
        return b""

    def getcode(self):
        return 200

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def opener(req, timeout=None):
    calls[0] += 1
    if calls[0] == 1:
        gate.wait(30)
        if MODE == "cap":
            return Ok()
    raise ConnectionRefusedError(61, "refused")


sink = HTTPSink("http://127.0.0.1:1/x", opener=opener, max_retries=RETRIES)
w = Worker(sink, batch_size=10, flush_interval=3600, max_queue=100_000)
for _ in range(10):
    w.submit([{"a": 1}, {"a": 2}])
time.sleep(0.2)
for _ in range(SPANS):
    w.submit([{"a": 1}, {"a": 2}])
buf = io.StringIO()
old = sys.stderr
sys.stderr = buf
threading.Timer(0.3, gate.set).start()
t0 = time.monotonic()
w.stop(30)
took = time.monotonic() - t0
sys.stderr = old
lines = [l for l in buf.getvalue().splitlines() if l.startswith("log-foundry:")]
print(f"mode={MODE} spans={SPANS} retries={RETRIES}: opener calls={calls[0]} lines={len(lines)} took={took:.2f}s failed_batches={w.failed_batches}")
for l in lines[:3]:
    print("  ", l[:200])
if len(lines) > 3:
    print("   ...", lines[-1][:200])
