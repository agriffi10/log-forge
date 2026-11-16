"""SPEC-094 probe: one message stuck in the producer while every other report succeeds.

Adapted on 2026-11-16 from SPEC-094's reviewer's `stuck_one_kafka.py`, against 97a03f45; not one of
the audit's probes. One partition has no leader, so the producer never reports one message, and
librdkafka's `flush` waits on the producer's whole backlog. Each later `emit` should return within
one slice of its own messages being reported, since the stand-in's `flush` holds its timeout while
the stuck message is queued, as librdkafka's does. Run against a tree:
`PYTHONPATH=<tree>/src python -B <this file>`.
"""

import time

from log_foundry.sinks.kafka import KafkaSink


class OneStuck:
    """The first message is never reported; every later one is reported at the next poll."""

    def __init__(self) -> None:
        self.pending = []
        self.stuck = 0

    def produce(self, topic, value=None, key=None, callback=None):
        if self.stuck == 0:
            self.stuck = 1
            return
        self.pending.append(callback)

    def poll(self, t):
        served, self.pending = self.pending, []
        for cb in served:
            cb(None, None)
        return len(served)

    def flush(self, timeout=None):
        self.poll(0)
        if self.stuck and timeout:
            time.sleep(timeout)
        return self.stuck


sink = KafkaSink("t", producer=OneStuck(), flush_timeout=0.5)
for i in range(4):
    t = time.monotonic()
    sink.emit([{"message": f"m{i}-{j}", "trace_id": "t"} for j in range(10)])
    print(f"emit {i}: {time.monotonic() - t:.2f}s losses={sink.losses()}")
