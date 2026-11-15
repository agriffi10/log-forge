"""P1: buffer overflow against a stalled sink. 3 events per span."""
import log_foundry as lf
from fakes import CountingSink, StderrCapture, show_health

sink = CountingSink()
lf.configure(sink=sink)
sink.gate.clear()

@lf.trace
def work(i):
    lf.info("in-span", i=i)

N = 12_000
with StderrCapture() as err:
    for i in range(N):
        work(i)
    h = show_health(lf, "stalled")
    sink.gate.set()
    r = lf.flush(30)
    h = show_health(lf, "after flush")
print("flush:", r)
ends = sink.count("span.end")
print(f"accepted spans={N} events={3*N}")
print(f"delivered spans={ends} events={sink.count()}")
print(f"health.dropped (submissions)={h.dropped}; events actually missing={3*N - sink.count()}")
print(f"IDENTITY spans: {N} == {ends} + {h.dropped} -> {N == ends + h.dropped}")
print(f"IDENTITY events using health alone: {3*N} == {sink.count()} + {h.dropped} -> {3*N == sink.count() + h.dropped}")
for l in err.lines():
    print("  stderr:", l)
lf.shutdown()
