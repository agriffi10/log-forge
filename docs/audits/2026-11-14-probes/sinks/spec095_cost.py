"""SPEC-095 probe: what one engine run costs, clean and with one lost reply.

Written on 2026-11-16 while authoring SPEC-095, against 97a03f45; not one of the audit's probes.
Run it once against each tree and compare the medians:

    PYTHONPATH=<tree>/src python -B spec095_cost.py 2>/dev/null

It times `_engine._run` over a `deliver` that accepts everything, and over one that answers
`RETRYABLE` for every event once, marking them as possibly landed where the tree has the mark,
then accepts them. The stop event is set, so no backoff sleeps. The lost-reply case on a tree
with SPEC-095 includes the resend line, which `2>/dev/null` discards.
"""

import statistics
import threading
import time

from log_foundry.sinks import _engine
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict


def clean(batch):
    return Verdict(outcomes=(Outcome.ACCEPTED,) * len(batch))


class Once:
    def __init__(self):
        self.n = 0

    def __call__(self, batch):
        self.n += 1
        if self.n == 1:
            mark = getattr(_engine.run_context(), "may_have_landed", None)
            if mark is not None:
                mark(range(len(batch)))
            return Verdict(outcomes=(Outcome.RETRYABLE,) * len(batch))
        return Verdict(outcomes=(Outcome.ACCEPTED,) * len(batch))


def run(deliver, size):
    batch = [{"log_id": str(i)} for i in range(size)]
    stop = threading.Event()
    stop.set()
    t = time.perf_counter()
    _engine._run(
        object(),
        deliver,
        SinkCounters(),
        batch,
        sink_retries=3,
        worker_retries=0,
        stop=stop,
        decline=None,
    )
    return time.perf_counter() - t


for size in (1, 100, 1000):
    for label, make in (("clean", lambda: clean), ("reply-lost-once", Once)):
        samples = []
        for _ in range(2000 if size < 1000 else 300):
            samples.append(run(make(), size))
        print(f"{label:16} {size:5} median {statistics.median(samples) * 1e6:9.1f} us")
