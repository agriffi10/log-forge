"""P8: the drain thread dies holding a batch (the sink raises a BaseException)."""
import log_foundry as lf
from fakes import CountingSink, StderrCapture, show_health

sink = CountingSink()
sink.raise_when = lambda call, batch: SystemExit(1) if call == 2 else None
lf.configure(sink=sink)

@lf.trace
def work(i):
    pass

with StderrCapture() as err:
    for i in range(10): work(i)
    r1 = lf.flush(10)                      # batch 1: delivered
    for i in range(10): work(i)
    r2 = lf.flush(10)                      # batch 2: kills the drain thread
    w = lf._lifecycle._state.worker_exists()
    w._thread.join(5)
    for i in range(30): work(i)            # accepted after the death
    r3 = lf.flush(1)
    h = show_health(lf, "after death")
    accepted = 50
    delivered = sink.count("span.end")
    print(f"flushes: {r1} | {r2} | {r3}; thread alive={w._thread.is_alive()}")
    print(f"accepted spans={accepted} delivered={delivered} queued={h.queued} dropped={h.dropped} failed_batches={h.failed_batches}")
    print(f"IDENTITY spans: {accepted} == {delivered} + {h.queued} + {h.dropped} -> {accepted == delivered + h.queued + h.dropped} "
          f"(gap {accepted - delivered - h.queued - h.dropped} = the batch the thread died holding)")
    lf.shutdown(2)
    h = show_health(lf, "after shutdown")
    print(f"delivered after shutdown={sink.count('span.end')} sink.closed={sink.closed}")
for l in err.lines():
    print("  stderr:", l)
