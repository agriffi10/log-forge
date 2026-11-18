"""SPEC-104 diff review, frame 2, run against SPEC-104's tree and against 1dc174fb: log_foundry.flush(timeout=T) on the application thread while the worker's round meets a stall.

Usage: python appflush.py <factor> <T> [orphan]
  factor: the stalled publish's length in publish_timeouts (P = 1.0)
  T: the timeout handed to log_foundry.flush
  orphan: if given, no worker; the stall is armed by an orphan level call on a second thread.
  The third argument is read as P too, so pass a number: `15 0.1 10` is the orphan case at
  P = 10, and `15 0.1` the worker case at P = 1.
Prints the flush's wall time and result, measured from the abandonment.
"""
import asyncio
import json
import sys
import threading
import time

import log_foundry
from log_foundry import _lifecycle
from log_foundry.sinks.nats import NATSSink
from log_foundry.worker import Worker

P = float(sys.argv[3]) if len(sys.argv) > 3 else 1.0


class JS:
    def __init__(self, o):
        self.o = o

    async def publish(self, subject, payload, *, timeout=None):  # noqa: ASYNC109
        await self.o._publish(payload)


class Client:
    def __init__(self, factor):
        self.factor = factor
        self.stalls = []
        self.armed = threading.Event()
        self.is_connected = True

    def jetstream(self):
        return JS(self)

    async def _publish(self, payload):
        if self.armed.is_set() and not self.stalls:
            e = [time.monotonic(), None]
            self.stalls.append(e)
            await asyncio.sleep(self.factor * P)
            e[1] = time.monotonic()
        else:
            await asyncio.sleep(0.001)

    async def flush(self):
        return None

    async def close(self):
        return None


def main():
    factor, T = float(sys.argv[1]), float(sys.argv[2])
    orphan = len(sys.argv) > 3
    client = Client(factor)
    sink = NATSSink("logs", client=client, jetstream=True, publish_timeout=P)
    log_foundry.configure(service="x", sink=sink)

    @log_foundry.trace
    def traced():
        return None

    client.armed.set()
    if orphan:
        threading.Thread(target=lambda: log_foundry.info("arm"), daemon=True).start()
    else:
        worker = Worker(sink, batch_size=50, flush_interval=0.02)
        with _lifecycle._state._lock:
            _lifecycle._state._worker = worker
            _lifecycle._state._closes.arm(worker.sink)
            _lifecycle._state._move()
        for _ in range(5):
            traced()
    while not client.stalls:
        time.sleep(0.001)
    # wait until the abandoning call has returned: abandonment at stall start + P
    abandoned = client.stalls[0][0] + P
    while time.monotonic() < abandoned + 0.05:
        time.sleep(0.002)
    if not orphan:
        for _ in range(3):
            traced()
    t0 = time.monotonic()
    r = log_foundry.flush(timeout=T)
    took = time.monotonic() - t0
    print(json.dumps({"factor": factor, "T": T, "orphan": orphan,
                      "flush_took": round(took, 3), "result": bool(r),
                      "reason": getattr(r, "reason", None),
                      "since_abandon": round(t0 - abandoned, 3)}))
    log_foundry.shutdown(timeout=1)


if __name__ == "__main__":
    main()
