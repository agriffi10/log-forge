"""P9: is flush() a durability barrier? A deliver-contract sink with partial outcomes."""
import threading
import log_foundry as lf
from log_foundry import Outcome, Verdict, SinkCounters, emit_via_deliver
from fakes import StderrCapture, show_health


class DeliverSink:
    """A sink on the verdict contract; `plan(batch) -> list[Outcome]` decides each event's fate."""
    max_retries = 0
    def __init__(self, plan):
        self.log_foundry_counters = SinkCounters()
        self.plan = plan
        self.accepted = []
        self.calls = 0
    def deliver(self, batch):
        self.calls += 1
        outcomes = self.plan(batch)
        self.accepted += [e for e, o in zip(batch, outcomes) if o is Outcome.ACCEPTED]
        return Verdict(outcomes=tuple(outcomes))
    def emit(self, batch):
        emit_via_deliver(self, batch)
    def losses(self):
        return self.log_foundry_counters.losses()
    def close(self):
        pass

@lf.trace
def work(i):
    lf.info("in-span", i=i)

def run(label, plan, n=10):
    sink = DeliverSink(plan)
    lf.configure(sink=sink)
    before = lf.health()
    with StderrCapture() as err:
        for i in range(n):
            work(i)
        r = lf.flush(10)
        h = lf.health()
    lost = 3 * n - len(sink.accepted)
    print(f"[{label}] accepted events={3*n} sink took={len(sink.accepted)} lost={lost} deliver calls={sink.calls}")
    print(f"    flush()={r}  failed_batches +{h.failed_batches - before.failed_batches}  health.sink={h.sink}")
    for l in err.lines():
        print("    stderr:", l)

A, R, J, D, U = Outcome.ACCEPTED, Outcome.RETRYABLE, Outcome.REJECTED, Outcome.DROPPED, Outcome.UNKNOWN
run("1 accepted, 29 never land (RETRYABLE every round)", lambda b: [A] + [R] * (len(b) - 1) if len(b) == 30 else [R] * len(b))
run("1 accepted, 29 REJECTED", lambda b: [A] + [J] * (len(b) - 1))
run("all 30 DROPPED by the sink", lambda b: [D] * len(b))
run("all 30 UNKNOWN", lambda b: [U] * len(b))
run("all 30 REJECTED", lambda b: [J] * len(b))

# (b) emit-only sink: a batch abandoned by the worker's own trigger BEFORE flush() is called
from fakes import CountingSink
sink = CountingSink()
sink.raise_when = lambda call, batch: ConnectionError("down") if call <= 4 else None
lf.configure(sink=sink)
before = lf.health()
with StderrCapture() as err:
    for i in range(10):          # 10 submissions == batch_size: the worker emits without a flush
        work(i)
    w = lf._lifecycle._state.worker_exists()
    while lf.health().failed_batches == before.failed_batches:   # wait for the abandon, by state
        pass
    work(99)
    r = lf.flush(10)
    h = lf.health()
print(f"[earlier batch abandoned, then flush] accepted events=33 delivered={sink.count()} flush()={r} failed_batches +{h.failed_batches - before.failed_batches}")
for l in err.lines():
    print("    stderr:", l)
lf.shutdown()
