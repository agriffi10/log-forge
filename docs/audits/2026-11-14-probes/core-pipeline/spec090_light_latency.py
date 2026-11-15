"""SPEC-090, written on 2026-11-15 against 3e27a0a6 and not one of the audit's own probes: no backlog, so nothing changes.
One span closes every GAP seconds into a sink costing COST a call; the sink records each span.end's delay from the close to
the emit that carries it, and the batch sizes. Today and under SPEC-090 the batches should be ten spans (20 events).
Usage: python spec090_light_latency.py <gap seconds> <spans> [sink seconds per call]
"""

import sys, time, statistics, collections
import log_foundry as lf
from log_foundry import context

GAP = float(sys.argv[1])
N = int(sys.argv[2])
COST = float(sys.argv[3]) if len(sys.argv) > 3 else 0.0
closed = {}
delays = []
sizes = collections.Counter()


class Rec:
    def emit(self, batch):
        if COST:
            time.sleep(COST)
        now = time.monotonic()
        sizes[len(batch)] += 1
        for e in batch:
            sid = e.get("span_id")
            if "duration_ms" in e and sid in closed:
                delays.append(now - closed[sid])

    def close(self):
        pass


lf.configure(sink=Rec())
for _ in range(N):
    ids = []

    @lf.trace
    def inner():
        ids.append(context.current_span().span_id)

    inner()
    closed[ids[0]] = time.monotonic()
    time.sleep(GAP)
lf.flush(30)
d = sorted(delays)
print(
    f"gap={GAP}s n={N} cost={COST}: matched={len(d)} median={statistics.median(d) * 1000:.0f}ms "
    f"max={d[-1] * 1000:.0f}ms sizes={dict(sizes)}"
)
