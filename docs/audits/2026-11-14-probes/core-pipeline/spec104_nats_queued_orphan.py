"""SPEC-104 diff review, frame 2, run against SPEC-104's tree and against 1dc174fb: An orphan level call on the application thread queued behind the worker's abandoning round.

Usage: python queued_orphan.py <factor> [P]
The worker's round stalls on publish 0 (factor*P); log_foundry.info() is called 0.1 s after the
stall began, so it queues on the sink lock behind that round. Reports the info() wall time.
"""
import asyncio
import json
import sys
import threading
import time

import log_foundry
from log_foundry import _lifecycle
from log_foundry.sinks.nats import CALL_SLACK, NATSSink
from log_foundry.worker import Worker

P = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0


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
    factor = float(sys.argv[1])
    client = Client(factor)
    sink = NATSSink("logs", client=client, jetstream=True, publish_timeout=P)
    log_foundry.configure(service="x", sink=sink)

    @log_foundry.trace
    def traced():
        return None

    worker = Worker(sink, batch_size=50, flush_interval=0.02)
    with _lifecycle._state._lock:
        _lifecycle._state._worker = worker
        _lifecycle._state._closes.arm(worker.sink)
        _lifecycle._state._move()
    client.armed.set()
    for _ in range(5):
        traced()
    while not client.stalls:
        time.sleep(0.001)
    while time.monotonic() < client.stalls[0][0] + 0.1:
        time.sleep(0.002)
    t0 = time.monotonic()
    log_foundry.info("queued orphan")
    took = time.monotonic() - t0
    log_foundry.shutdown(timeout=1)
    ev = log_foundry.health().events
    print(json.dumps({"factor": factor, "P": P, "info_took": round(took, 3),
                      "two_P_plus_slack": 2 * P + CALL_SLACK, "unaccounted": ev.unaccounted,
                      "orphan_lost": ev.orphan_lost}))


if __name__ == "__main__":
    main()
