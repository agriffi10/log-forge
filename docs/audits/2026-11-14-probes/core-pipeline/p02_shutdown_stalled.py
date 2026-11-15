"""P2: shutdown(timeout) against a sink stalled inside emit; then the sink recovers."""
import threading, time
import log_foundry as lf
from fakes import CountingSink, StderrCapture, show_health

sink = CountingSink()
lf.configure(sink=sink)

@lf.trace
def work(i):
    pass

N = 500
with StderrCapture() as err:
    sink.gate.clear()
    for i in range(N):
        work(i)
    sink.entered.wait(5)          # the worker is inside emit with its first batch
    t0 = time.monotonic()
    lf.shutdown(timeout=0.5)
    print(f"shutdown returned after {time.monotonic()-t0:.2f}s; sink.closed={sink.closed}")
    h = show_health(lf, "after expired shutdown")
    print(f"delivered so far spans={sink.count('span.end')}")
    print(f"IDENTITY (spans) at this instant: {N} == delivered {sink.count('span.end')} + queued {h.queued} + dropped {h.dropped}"
          f" -> {N == sink.count('span.end') + h.queued + h.dropped}  (gap = batch in flight, in no counter)")
    r = lf.flush(1)
    print("flush after expired shutdown:", r)
    sink.gate.set()               # the destination recovers
    w = lf._lifecycle._state.worker_exists()
    w._thread.join(5)
    h = show_health(lf, "after sink recovered")
    print(f"delivered spans={sink.count('span.end')} of {N}; sink.closed={sink.closed}; thread alive={w._thread.is_alive()}")
for l in err.lines():
    print("  stderr:", l)
