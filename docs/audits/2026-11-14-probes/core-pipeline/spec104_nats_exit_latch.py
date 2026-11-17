"""SPEC-104 groundwork, written on 2026-11-17 against 03dbf502: a JetStream NATSSink's exit drain against its own latches.
A real NATSSink over an injected client; the driver's misbehaviour starts only once shutdown() is called, so what the
worker delivered before it is reported apart (`before=`) from what the exit drain delivered (`at_exit=`). Needs no
`nats` extra. Modes:
  transient  the first publish after shutdown() starts stalls 1.5 x publish_timeout, ignoring the driver timeout it is
             handed (SPEC-061 M3/M6's force-flush, or a tie below twice DRIVER_DEADLINE_MARGIN); every other acks in 1 ms
  slow       every publish after shutdown() starts takes 1.5 x publish_timeout, ignoring its timeout
  reconnect  the client reports itself disconnected from shutdown() until 1 s after it, then connected
  dead       the client reports itself disconnected from shutdown() on
Usage: python spec104_nats_exit_latch.py <mode> <spans> <publish_timeout> <shutdown timeout>
"""
import asyncio
import contextlib
import io
import sys
import time
import log_foundry as lf
from log_foundry.sinks.nats import NATSSink

MODE, SPANS, TIMEOUT, SHUT = sys.argv[1], int(sys.argv[2]), float(sys.argv[3]), float(sys.argv[4])
T0 = [None]


class JS:
    def __init__(self, c):
        self.c = c

    async def publish(self, subject, payload, *, timeout=None):
        if MODE == "slow" or (MODE == "transient" and T0[0] is not None and not self.c.stalled):
            self.c.stalled = True
            await asyncio.sleep(1.5 * TIMEOUT)
        else:
            await asyncio.sleep(0.001)
        self.c.acked += 1


class Client:
    def __init__(self):
        self.pending_data_size = 0
        self.stats = {"reconnects": 0}
        self.acked = 0
        self.stalled = False

    @property
    def is_connected(self):
        if MODE == "dead" and T0[0] is not None:
            return False
        if MODE != "reconnect" or T0[0] is None:
            return True
        return time.monotonic() - T0[0] > 1.0

    async def publish(self, subject, payload):
        self.acked += 1

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
lf.configure(sink=sink)


@lf.trace
def work():
    pass


for _ in range(SPANS):
    work()
err = io.StringIO()
before = c.acked
T0[0] = time.monotonic()
with contextlib.redirect_stderr(err):
    lf.shutdown(timeout=SHUT)
took = time.monotonic() - T0[0]
h = lf.health()
print(f"{MODE}: before={before} at_exit={c.acked - before} of {2 * SPANS} batch_lost={h.events.batch_lost} "
      f"failed_batches={h.failed_batches} took={took:.2f}s lines={sum(1 for l in err.getvalue().splitlines() if l.startswith('log-foundry'))}")
