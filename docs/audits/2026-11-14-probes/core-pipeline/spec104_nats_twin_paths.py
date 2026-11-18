"""SPEC-104 diff review, frame 2, run against SPEC-104's tree and against 1dc174fb: drive each twin path through the library with a JetStream stall double.

Usage: python paths.py <wrap> <phase> <stall>
  wrap:  bare | filter | transform | multi
  phase: exit | steady | orphan
  stall: young (1.5 P) | long (15 P)
Prints JSON with health terms, timings and whether any publish/flush started while the stalled
publish was pending.
"""
from __future__ import annotations

import asyncio
import json
import sys
import threading
import time

import log_foundry
from log_foundry import _lifecycle
from log_foundry.sinks.filtering import FilteringSink
from log_foundry.sinks.multi import MultiSink
from log_foundry.sinks.nats import NATSSink
from log_foundry.sinks.transform import TransformSink
from log_foundry.worker import Worker

P = 0.4


class JS:
    def __init__(self, owner):
        self.o = owner

    async def publish(self, subject, payload, *, timeout=None):  # noqa: ASYNC109
        await self.o._publish(payload)


class Client:
    def __init__(self, factor):
        self.factor = factor
        self.trigger = lambda: False
        self.stalls = []
        self.received = []
        self.log = []
        self.is_connected = True
        self.n = 0

    def jetstream(self):
        return JS(self)

    async def _publish(self, payload):
        n = self.n
        self.n += 1
        self.log.append(("ps", n, time.monotonic()))
        if self.trigger() and not self.stalls:
            e = [time.monotonic(), None]
            self.stalls.append(e)
            await asyncio.sleep(self.factor * P)
            e[1] = time.monotonic()
        else:
            await asyncio.sleep(0.001)
        self.received.append(payload)
        self.log.append(("pe", n, time.monotonic()))

    async def flush(self):
        self.log.append(("fs", -1, time.monotonic()))

    async def close(self):
        return None


class ListSink:
    def __init__(self):
        self.got = []

    def emit(self, batch):
        self.got.extend(batch)


def main():
    wrap, phase, stall = sys.argv[1:4]
    client = Client({"young": 1.5, "long": 15}.get(stall) or float(stall))
    nats = NATSSink("logs", client=client, jetstream=True, publish_timeout=P)
    sink = {
        "bare": lambda: nats,
        "filter": lambda: FilteringSink(nats, min_level="DEBUG"),
        "transform": lambda: TransformSink(nats, lambda e: e),
        "multi": lambda: MultiSink(nats, ListSink()),
    }[wrap]()
    log_foundry.configure(service="x", sink=sink)

    @log_foundry.trace
    def traced():
        return None

    out = {"wrap": wrap, "phase": phase, "stall": stall}
    if phase == "orphan":
        # level calls with no span go through the orphan path on this thread
        client.trigger = lambda: True
        times = []
        for i in range(6):
            t0 = time.monotonic()
            try:
                log_foundry.info(f"orphan {i}")
                err = None
            except BaseException as exc:  # noqa: BLE001
                err = repr(exc)
            times.append((round(time.monotonic() - t0, 3), err))
        out["orphan_times"] = times
        t0 = time.monotonic()
        log_foundry.shutdown(timeout=5)
        out["shutdown"] = round(time.monotonic() - t0, 3)
    else:
        worker = Worker(sink, batch_size=50, flush_interval=0.05 if phase == "steady" else 3600.0)
        with _lifecycle._state._lock:
            _lifecycle._state._worker = worker
            _lifecycle._state._closes.arm(worker.sink)
            _lifecycle._state._move()
        if phase == "exit":
            client.trigger = worker._stop.is_set
            for _ in range(100):
                traced()
            t0 = time.monotonic()
            log_foundry.shutdown(timeout=10)
            out["shutdown"] = round(time.monotonic() - t0, 3)
        else:
            trig = threading.Event()
            client.trigger = trig.is_set
            trig.set()
            for _ in range(25):
                traced()
            deadline = time.monotonic() + 5
            while not client.stalls and time.monotonic() < deadline:
                time.sleep(0.005)
            for _ in range(75):
                traced()
            t0 = time.monotonic()
            r = log_foundry.flush(timeout=10)
            out["flush"] = (round(time.monotonic() - t0, 3), bool(r))
            t0 = time.monotonic()
            log_foundry.shutdown(timeout=10)
            out["shutdown"] = round(time.monotonic() - t0, 3)
    h = log_foundry.health()
    ev = h.events
    out["events"] = {k: getattr(ev, k) for k in ("accepted", "delivered", "batch_lost", "sink_lost",
                                                  "orphan_lost", "unaccounted")
                     if hasattr(ev, k)}
    out["failed_batches"] = h.failed_batches
    out["received"] = len(client.received)
    out["distinct"] = len(set(client.received))
    out["stalls"] = [[round(a - client.stalls[0][0], 3), None if b is None else round(b - client.stalls[0][0], 3)]
                     for a, b in client.stalls]
    # any publish or flush started while the stalled publish was pending?
    if client.stalls:
        a, b = client.stalls[0]
        b = b if b is not None else float("inf")
        inside = [(k, n, round(t - a, 3)) for k, n, t in client.log if a < t < b and k in ("ps", "fs")]
        out["started_inside_stall"] = inside[:5]
    print(json.dumps(out))


if __name__ == "__main__":
    main()
