"""P5: an emit-only sink that fails every 3rd batch for good; then one that fails transiently."""
import log_foundry as lf
from fakes import CountingSink, StderrCapture, show_health

sink = CountingSink()
lf.configure(sink=sink)

@lf.trace
def work(i):
    lf.info("in-span", i=i)

# a "batch" here is the logical batch: the worker re-sends the same list object on retry
state = {"batch_no": 0, "seen": None, "fail_batches": 0, "attempts": 0}
def every_third(call, batch):
    if state["seen"] is not batch:
        state["seen"] = batch
        state["batch_no"] += 1
    if state["batch_no"] % 3 == 0:
        state["attempts"] += 1
        return ConnectionError("down")
    return None

N = 300
with StderrCapture() as err:
    sink.raise_when = every_third
    for i in range(N):
        work(i)
        if i % 10 == 9:
            r = lf.flush(10)       # make batches deterministic: 10 spans (30 events) each
    h = show_health(lf, "every 3rd batch fails for good")
    print(f"accepted events={3*N}; delivered={sink.count()}; failed_batches={h.failed_batches}; "
          f"sum of 'lost N event(s)' stderr lines={err.lost_events()}; emit attempts on failing batches={state['attempts']}")
    print(f"IDENTITY events via stderr: {3*N} == {sink.count()} + {err.lost_events()} -> {3*N == sink.count() + err.lost_events()}")
    print(f"IDENTITY events via health() alone: no event-count term for an abandoned batch (failed_batches={h.failed_batches} batches, sink={h.sink})")
    print("last flush():", r)
    sample = [l for l in err.lines()][:2]
for l in sample:
    print("  stderr:", l)

# transient: every batch fails on its first attempt only
sink2 = CountingSink()
seen = {}
def first_attempt_fails(call, batch):
    seen[id(batch)] = seen.get(id(batch), 0) + 1
    return ConnectionError("blip") if seen[id(batch)] == 1 else None
sink2.raise_when = first_attempt_fails
lf.configure(sink=sink2)
with StderrCapture() as err:
    for i in range(100):
        work(i)
    r = lf.flush(10)
    h = show_health(lf, "transient (first attempt of each batch fails)")
    print(f"accepted events=300; delivered={sink2.count()}; duplicates={len(sink2.log_ids())-len(set(sink2.log_ids()))}; flush={r}; stderr lines={len(err.lines())}")

# orphan path twin: one transient failure, no retry
sink3 = CountingSink()
n = {"c": 0}
def fail_first_call(call, batch):
    return ConnectionError("blip") if call == 1 else None
sink3.raise_when = fail_first_call
lf.configure(sink=sink3)
with StderrCapture() as err:
    lf.info("orphan-1")
    lf.info("orphan-2")
    h = show_health(lf, "orphan path, first emit fails once")
    print(f"orphan accepted=2 delivered={sink3.count()} emit calls={sink3.calls} orphan_lost={h.orphan_lost}")
    for l in err.lines(): print("  stderr:", l)
lf.shutdown()
