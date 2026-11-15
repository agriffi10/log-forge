"""P4: swap between two sink graphs that share a child (no concurrency needed).
configure(sink=MultiSink([X, Y])) -> log -> configure(sink=X) -> log -> shutdown.
Does the swap close X (through the displaced MultiSink) while X is the live sink, and how many
times is X closed in total? Run for the orphan path and the worker path, and for FilteringSink."""
import sys, subprocess
if len(sys.argv) == 1:
    for wrap in ("multi", "filter"):
        for path in ("orphan", "worker"):
            r = subprocess.run([sys.executable, "-B", __file__, wrap, path], capture_output=True, text=True, timeout=60)
            print(f"{wrap:6s} {path:6s} exit={r.returncode} {r.stdout.strip()}  stderr: {r.stderr.strip()[:300]}")
    sys.exit(0)
import faulthandler, time
import log_foundry
from log_foundry.sinks.multi import MultiSink
from log_foundry.sinks.filtering import FilteringSink
faulthandler.dump_traceback_later(25, exit=True)
wrap, path = sys.argv[1], sys.argv[2]
log = []
class Rec:
    def __init__(self, name): self.name = name; self.closed = False; self.closes = 0; self.after_close = 0; self.n = 0
    def emit(self, batch):
        self.n += len(batch)
        if self.closed: self.after_close += len(batch)
    def close(self): self.closes += 1; self.closed = True; log.append(f"{self.name}.close#{self.closes}@{stage}")
@log_foundry.trace
def work(): pass
def write():
    if path == "worker": work(); log_foundry.flush(timeout=2)
    else: log_foundry.info("x")
x, y = Rec("X"), Rec("Y")
stage = "start"
holder = MultiSink(x, y) if wrap == "multi" else FilteringSink(x, predicate=lambda e: True)
log_foundry.configure(service="p4", sink=holder); write()
stage = "swap"; log_foundry.configure(sink=x); time.sleep(0.3)
stage = "live-on-X"; write(); write()
stage = "shutdown"; log_foundry.shutdown(timeout=5)
print(f"X.closes={x.closes} X.events={x.n} X.events_after_its_close={x.after_close} order={log}")
