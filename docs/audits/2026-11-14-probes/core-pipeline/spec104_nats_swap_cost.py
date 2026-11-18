"""SPEC-104 diff review, frame 2, run against SPEC-104's tree and against 1dc174fb: configure(sink=new) on the application thread while the worker's NATSSink round meets a stall.

Usage: python swap.py <factor> <offset>
P = 1.0. The worker's round abandons publish 0 (stall factor*P); configure(sink=ListSink()) is
called offset seconds after the abandonment. Reports configure's wall time and the ledger.
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


class ListSink:
    def __init__(self):
        self.got = []

    def emit(self, batch):
        self.got.extend(batch)


def main():
    factor, offset = float(sys.argv[1]), float(sys.argv[2])
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
    abandoned = client.stalls[0][0] + P
    while time.monotonic() < abandoned + offset:
        time.sleep(0.002)
    for _ in range(5):
        traced()
    t0 = time.monotonic()
    log_foundry.configure(sink=ListSink())
    took = time.monotonic() - t0
    log_foundry.shutdown(timeout=2)
    ev = log_foundry.health().events
    print(json.dumps({"factor": factor, "offset": offset, "configure": round(took, 3),
                      "unaccounted": ev.unaccounted, "batch_lost": ev.batch_lost,
                      "sink_lost": ev.sink_lost, "delivered": ev.delivered}))


if __name__ == "__main__":
    main()
