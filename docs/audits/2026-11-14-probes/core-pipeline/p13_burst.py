"""P13: a burst against a sink with a fixed 20 ms cost per emit call, whatever the batch size."""
import threading, time, collections
import log_foundry as lf
from fakes import StderrCapture, show_health

class PerCallSink:
    def __init__(self):
        self.n = 0
        self.sizes = collections.Counter()
    def emit(self, batch):
        time.sleep(0.02)                 # one request per call: the cost does not depend on len(batch)
        self.n += len(batch)
        self.sizes[len(batch)] += 1
    def close(self): pass

sink = PerCallSink()
lf.configure(sink=sink)

@lf.trace
def work(): pass

TOTAL = 30_000
with StderrCapture() as err:
    t0 = time.monotonic()
    for _ in range(TOTAL):
        work()
    produced_s = time.monotonic() - t0
    h = show_health(lf, "burst produced")
    r = lf.flush(60)
    drained_s = time.monotonic() - t0
    h = show_health(lf, "after flush")
print(f"accepted spans={TOTAL} in {produced_s:.2f}s ({TOTAL/produced_s:,.0f} spans/s); delivered events={sink.n} (spans={sink.n//2}); dropped={h.dropped}")
print(f"IDENTITY spans: {TOTAL} == {sink.n//2} + {h.dropped} -> {TOTAL == sink.n//2 + h.dropped}")
print(f"emit calls by batch size (events): {dict(sink.sizes)}; drain of the backlog took {drained_s - produced_s:.1f}s; flush={r}")

# P14: a level call outside a span after shutdown(), against a sink that keeps no closed state
lf.shutdown()
before = sink.n
lf.info("after-shutdown-orphan")
h = show_health(lf, "orphan log after shutdown")
print(f"orphan log after shutdown: sink received +{sink.n - before}")
