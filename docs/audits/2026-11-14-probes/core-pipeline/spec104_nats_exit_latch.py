"""SPEC-104 groundwork, written on 2026-11-17 against 03dbf502: a JetStream NATSSink's rounds against its own latch.
A real NATSSink over an injected client, so `_probing()` is false and only the abandoned-publish latch is exercised; the
probe latch is not. Needs no `nats` extra. Each round is wrapped to record whether the worker's stop event was set when
it began, and every ack is credited to the round that made it, so `exit_acked=` counts only what rounds begun after the
stop delivered: a steady round still in flight at the stop is not an exit round, and its tail is not counted as one.
The misbehaviour is armed by the first round that begins after the trigger, never by a round already in flight:
  transient  that round's first publish takes 1.5 x publish_timeout, ignoring the driver timeout it is handed (the
             shape of the driver's force-flush past 2 MiB into a peer that stopped reading, SPEC-061 FR-001); every
             other publish acks in 1 ms
  slow       from that round on, every publish takes 1.5 x publish_timeout, ignoring its timeout
  reconnect  from that round on, the client reports itself disconnected until 1 s after the trigger
  dead       from that round on, the client reports itself disconnected
The trigger is `exit` (the stop: shutdown(timeout) is called once the backlog is queued) or `steady` (a flag set while
the worker drains the backlog; no shutdown for 6 s, then health() is read, then shutdown(timeout)).
Usage: python spec104_nats_exit_latch.py <mode> <exit|steady> <spans> <publish_timeout> <shutdown timeout>
"""
import asyncio
import contextlib
import io
import sys
import time

import log_foundry as lf
from log_foundry import _lifecycle
from log_foundry.sinks.nats import NATSSink

MODE, WHEN, SPANS = sys.argv[1], sys.argv[2], int(sys.argv[3])
TIMEOUT, SHUT = float(sys.argv[4]), float(sys.argv[5])
TRIGGER = [None]
ARMED = [None]
CURRENT = ["steady"]
ACKS = {"steady": 0, "exit": 0}


class JS:
    def __init__(self, c):
        self.c = c

    async def publish(self, subject, payload, *, timeout=None):
        kind = CURRENT[0]
        stall = ARMED[0] is not None and (MODE == "slow" or (MODE == "transient" and not self.c.stalled))
        if stall:
            self.c.stalled = True
            await asyncio.sleep(1.5 * TIMEOUT)
        else:
            await asyncio.sleep(0.001)
        ACKS[kind] += 1


class Client:
    def __init__(self):
        self.pending_data_size = 0
        self.stats = {"reconnects": 0}
        self.stalled = False

    @property
    def is_connected(self):
        if ARMED[0] is None or MODE not in ("reconnect", "dead"):
            return True
        return MODE == "reconnect" and time.monotonic() - TRIGGER[0] > 1.0

    async def publish(self, subject, payload):
        ACKS[CURRENT[0]] += 1

    def jetstream(self):
        return JS(self)

    async def flush(self, timeout=None):
        return None

    async def close(self):
        return None

    async def drain(self):
        return None


c = Client()
sink = NATSSink("s", client=c, jetstream=True, publish_timeout=TIMEOUT)
_round = sink._round


def round_(batch):
    worker = _lifecycle._state._worker
    stopped = worker is not None and worker._stop.is_set()
    CURRENT[0] = "exit" if stopped else "steady"
    triggered = stopped if WHEN == "exit" else TRIGGER[0] is not None
    if triggered and ARMED[0] is None:
        ARMED[0] = time.monotonic()
    return _round(batch)


sink._round = round_
lf.configure(sink=sink)


@lf.trace
def work():
    pass


for _ in range(SPANS):
    work()
err = io.StringIO()
with contextlib.redirect_stderr(err):
    if WHEN == "steady":
        TRIGGER[0] = time.monotonic()
        time.sleep(6)
        h = lf.health()
        print(f"{MODE} steady, 6 s after the trigger: steady_acked={ACKS['steady']} of {2 * SPANS} "
              f"batch_lost={h.events.batch_lost} failed_batches={h.failed_batches}")
    else:
        TRIGGER[0] = time.monotonic()
    t0 = time.monotonic()
    lf.shutdown(timeout=SHUT)
    took = time.monotonic() - t0
h = lf.health()
lines = sum(1 for line in err.getvalue().splitlines() if line.startswith("log-foundry"))
print(f"{MODE} {WHEN}: steady_acked={ACKS['steady']} exit_acked={ACKS['exit']} of {2 * SPANS} "
      f"batch_lost={h.events.batch_lost} failed_batches={h.failed_batches} took={took:.2f}s lines={lines}")
