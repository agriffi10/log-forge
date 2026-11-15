"""SPEC-090, written on 2026-11-15 against 3e27a0a6 and not one of the audit's own probes: a backlog into a JetStream NATSSink.
A real NATSSink over an injected client whose JetStream publish sleeps the ack time, so the sink's one publish_timeout covers a
whole batch's acks. A fixed 1,000-event batch outlasts the default 10 s budget at a 20 ms ack and loses its tail; SPEC-090's
size target keeps each emit near half a second. Set LF_TRACE=1 against a build that prints its emits to see their durations.
Needs no `nats` extra: the client is injected.
Usage: python spec090_nats_ack_backlog.py <ack seconds> <spans> <publish_timeout>
"""

import asyncio, sys, time
import log_foundry as lf
from log_foundry.sinks.nats import NATSSink

ACK = float(sys.argv[1])
SPANS = int(sys.argv[2])
TIMEOUT = float(sys.argv[3])
durations = []


class JS:
    def __init__(self, c):
        self.c = c

    async def publish(self, subject, payload, *, timeout=None):
        await asyncio.sleep(ACK)
        self.c.acked += 1


class Client:
    def __init__(self):
        self.is_connected = True
        self.pending_data_size = 0
        self.stats = {"reconnects": 0}
        self.acked = 0

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


class Timed(NATSSink):
    def deliver(self, batch):
        t = time.monotonic()
        try:
            return super().deliver(batch)
        finally:
            durations.append((len(batch), time.monotonic() - t))


c = Client()
sink = Timed("s", client=c, jetstream=True, publish_timeout=TIMEOUT)
lf.configure(sink=sink)


@lf.trace
def work():
    pass


t0 = time.monotonic()
for _ in range(SPANS):
    work()
r = lf.flush(300)
h = lf.health()
print(
    f"ack={ACK}s spans={SPANS} events={2 * SPANS}: acked={c.acked} sink.failed={h.sink.failed if h.sink else None} "
    f"failed_batches={h.failed_batches} flush={r} took={time.monotonic() - t0:.1f}s"
)
if durations:
    print(
        f"deliver calls={len(durations)} largest batch={max(n for n, _ in durations)} "
        f"longest deliver={max(d for _, d in durations):.3f}s"
    )
