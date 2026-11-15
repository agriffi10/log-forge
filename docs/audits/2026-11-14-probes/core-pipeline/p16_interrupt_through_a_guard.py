"""P16, written while authoring SPEC-088 and not one of the audit's own probes: a KeyboardInterrupt passing through each guard.
The guards catch Exception and let a BaseException through (invariant 1). Row 1 raises it from the sink during an orphan log,
row 2 while a span opens (start_event), row 3 while a span closes (end_event, after one in-span log). Each row prints what
reached the caller, what the sink received, and health(). Rows 2 and 3 patch the one function named; nothing else is patched."""
import log_foundry as lf
from log_foundry import decorator

class Sink:
    def __init__(self, interrupt=False): self.got, self.interrupt = [], interrupt
    def emit(self, batch):
        if self.interrupt: raise KeyboardInterrupt
        self.got.extend(e["message"] for e in batch)
    def close(self): pass

def report(label, sink, reached):
    lf.flush()
    h = lf.health()
    print(f"[{label}] caller got {reached}; sink got {sink.got}; orphan_lost={h.orphan_lost} in_span_lost={h.in_span_lost} "
          f"dropped={h.dropped} failed_batches={h.failed_batches} events={getattr(h, 'events', 'no such field')}")

def interrupt(*a, **k): raise KeyboardInterrupt

sink = Sink(interrupt=True); lf.configure(sink=sink)
try: lf.info("orphan"); reached = "nothing"
except KeyboardInterrupt: reached = "KeyboardInterrupt"
report("the sink interrupts an orphan log", sink, reached)

sink = Sink(); lf.configure(sink=sink)
@lf.trace
def work(): lf.info("in-span")
real = decorator.start_event; decorator.start_event = interrupt
try: work(); reached = "nothing"
except KeyboardInterrupt: reached = "KeyboardInterrupt"
finally: decorator.start_event = real
report("an interrupt while the span opens", sink, reached)

real = decorator.end_event; decorator.end_event = interrupt
try: work(); reached = "nothing"
except KeyboardInterrupt: reached = "KeyboardInterrupt"
finally: decorator.end_event = real
report("an interrupt while the span closes", sink, reached)
