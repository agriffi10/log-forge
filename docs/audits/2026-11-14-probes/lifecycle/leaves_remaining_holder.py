"""Written for SPEC-087 on 2026-11-15, not one of the audit's probes: a wrapper that leaves its
member alone, as the holder still in the record on whose account the library does not close it.
configure(Leaves(A)) -> a span -> a second span parks the drain thread inside A.emit ->
shutdown(timeout=0.2) -> configure(MultiSink(A, B)) -> a log with no span -> shutdown(timeout=0.3)
-> the emit released -> shutdown(). Prints A's close count at the end, and whether A was closed
while the drain thread was inside it. FR-004's fourth shape: 5cb0f449 closes A once, under that
thread; SPEC-087 closes it 0 times and loses nothing; the close is the caller's to make."""
import faulthandler
import threading
import time

import log_foundry
from log_foundry.sinks.multi import MultiSink

faulthandler.dump_traceback_later(25, exit=True)


class Rec:
    def __init__(self, name):
        self.name = name
        self.closes = 0
        self.events = 0
        self.refused = 0
        self.closed = False
        self.inside = 0
        self.closed_while_inside = 0
        self.park = None
        self.parked = threading.Event()

    def emit(self, batch):
        if self.closed:
            self.refused += len(batch)
            raise RuntimeError("closed")
        self.inside += 1
        try:
            if self.park is not None and threading.current_thread().name == "log-foundry-worker":
                self.parked.set()
                self.park.wait(10)
            self.events += len(batch)
        finally:
            self.inside -= 1

    def close(self):
        if self.inside:
            self.closed_while_inside += 1
        self.closes += 1
        self.closed = True


class Leaves:
    def __init__(self, inner):
        self.inner = inner

    def emit(self, batch):
        self.inner.emit(batch)

    def close(self):
        pass


@log_foundry.trace
def work():
    pass


a, b = Rec("A"), Rec("B")
log_foundry.configure(service="leaves", sink=Leaves(a))
work()
log_foundry.flush(timeout=3)
a.park = threading.Event()
work()
a.parked.wait(5)
log_foundry.shutdown(timeout=0.2)
log_foundry.configure(sink=MultiSink(a, b))
log_foundry.info("orphan")
log_foundry.shutdown(timeout=0.3)
closes_while_parked = a.closes
a.park.set()
while a.inside:
    time.sleep(0.01)
log_foundry.shutdown()
print(
    f"A.closes={a.closes} A.closed_while_inside={a.closed_while_inside} "
    f"A.closes_while_parked={closes_while_parked} A.refused={a.refused} A.events={a.events} "
    f"B.closes={b.closes}"
)
