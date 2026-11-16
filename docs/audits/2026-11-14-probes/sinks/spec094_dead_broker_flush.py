"""SPEC-094 probe: a KafkaSink over a producer that never reports (a dead broker).

Adapted on 2026-11-16 from SPEC-094's reviewer's `dead_kafka_flush.py`, against 97a03f45; not one
of the audit's probes. It prints what `log_foundry.flush()` returns and how long it takes, the
losses, and how long an orphan `info()` holds the caller's thread. Run against a tree:
`PYTHONPATH=<tree>/src python -B <this file>`.
"""

import contextlib
import io
import time

import log_foundry
from log_foundry.sinks.kafka import KafkaSink

print("log_foundry from", log_foundry.__file__)


class DeadProducer:
    """Takes every message and reports none."""

    def __init__(self):
        self.q = 0

    def produce(self, topic, value=None, key=None, callback=None):
        self.q += 1

    def poll(self, t):
        return 0

    def flush(self, timeout=None):
        if timeout:
            time.sleep(timeout)
        return self.q


sink = KafkaSink("t", producer=DeadProducer(), flush_timeout=0.5)
log_foundry.configure(sink=sink)


@log_foundry.trace
def work():
    log_foundry.info("hello")


err = io.StringIO()
with contextlib.redirect_stderr(err):
    work()
    t = time.monotonic()
    r = log_foundry.flush(timeout=5)
    el = time.monotonic() - t
    t = time.monotonic()
    log_foundry.info("orphan")
    orphan = time.monotonic() - t
print("flush:", r, f"elapsed={el:.2f}s")
print("losses:", sink.losses(), "failed_batches:", log_foundry.health().failed_batches)
print(f"orphan info() took {orphan:.2f}s")
print("lines:", [ln for ln in err.getvalue().splitlines() if ln.startswith("log-foundry")])
log_foundry.shutdown(timeout=1)
