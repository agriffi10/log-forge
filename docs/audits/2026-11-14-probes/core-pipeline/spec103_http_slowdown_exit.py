"""SPEC-103, written on 2026-11-17: a refusing HTTPSink whose requests turn into 0.3 s timeouts mid-exit, on the real clock.
An HTTPSink(max_batch_count=10, max_retries=0) whose opener holds the in-flight batch's request until the stop lands and refuses
it, then refuses every request at once until <quick> seconds (10 ms by default) after that gate opened, and from then on
waits 0.3 s per request and times out. Worker batch_size=10, max_retries=0, 2,000 one-event spans queued at exit,
log_foundry.shutdown(<budget>). It prints how long shutdown() took, whether the join completed (stopped_reason), close()
calls, the requests made, and how long the drain went on running after shutdown() returned. The last diff review's numbers: on the probe that grew exit batches (7f400f51), at
budgets of 1.0 and 3.0 s, ShutdownTimeout, close() 0 times, and the drain running 20.9 and 18.9 s after the return; on
e7b437f9, one close and nothing after the return. Runs against whatever worker.py is importable.
How much of the backlog goes in the quick window depends on the machine's speed, so <quick> is an argument.
Usage: PYTHONPATH=src python spec103_http_slowdown_exit.py <budget> [quick]
"""

import io, sys, threading, time

import log_foundry
from log_foundry import _lifecycle
from log_foundry.sinks.http import HTTPSink
from log_foundry.worker import Worker

BUDGET = float(sys.argv[1])
QUICK = float(sys.argv[2]) if len(sys.argv) > 2 else 0.01
SPANS = 2000
stop = []
opened = []
requests = [0]


def opener(request, timeout=None):
    requests[0] += 1
    if requests[0] == 1:
        stop[0].wait(30)
        opened.append(time.monotonic())
        raise ConnectionRefusedError(61, "refused")
    if time.monotonic() < opened[0] + QUICK:
        raise ConnectionRefusedError(61, "refused")
    threading.Event().wait(0.3)
    raise TimeoutError("timed out")


sink = HTTPSink("http://127.0.0.1:1/x", opener=opener, max_batch_count=10, max_retries=0)
closes = [0]
sink.close = lambda: closes.__setitem__(0, closes[0] + 1)
worker = Worker(sink, batch_size=10, flush_interval=1.0, max_retries=0)
stop.append(worker._stop)
with _lifecycle._state._lock:
    _lifecycle._state._worker = worker
    _lifecycle._state._closes.arm(sink)
    _lifecycle._state._move()
for seq in range(10):
    worker.submit([{"message": "m", "seq": seq}])
for _ in range(5000):
    if requests[0]:
        break
    threading.Event().wait(0.001)
for seq in range(10, 10 + SPANS):
    worker.submit([{"message": "m", "seq": seq}])
buf = io.StringIO()
old = sys.stderr
sys.stderr = buf
began = time.monotonic()
log_foundry.shutdown(BUDGET)
returned = time.monotonic()
closes_at_return = closes[0]
worker._drain_finished.wait(120)
ended = time.monotonic()
sys.stderr = old
print(
    f"http budget={BUDGET} quick={QUICK}: shutdown took {returned - began:.2f}s; "
    f"stopped_reason={worker.stopped_reason} close()={closes_at_return} requests={requests[0] - 1}; "
    f"the drain ran {ended - returned:.2f}s after the return"
)
