"""P15, written while authoring SPEC-088 and not one of the audit's own probes: what a forked child's health() starts as.
The parent loses one orphan log (its sink refuses it), delivers five spans, then forks from inside an open span that has
buffered one log. The child reports health() at once and again after its copy of the span has closed and a flush(); the
parent reports after its own. No library code is patched."""
import contextlib, io, os, sys, warnings
warnings.simplefilter("ignore")
import log_foundry as lf

class Sink:
    def emit(self, batch):
        if any(e["message"] == "refuse me" for e in batch): raise ConnectionError("down")
    def close(self): pass

def show(who, when):
    h = lf.health()
    print(f"{who} {when}: orphan_lost={h.orphan_lost} in_span_lost={h.in_span_lost} dropped={h.dropped} "
          f"failed_batches={h.failed_batches} queued={h.queued} events={getattr(h, 'events', 'no such field')}")
    sys.stdout.flush()

lf.configure(sink=Sink())
@lf.trace
def closed(): lf.info("x")
for _ in range(5): closed()
with contextlib.redirect_stderr(io.StringIO()):
    lf.info("refuse me")
lf.flush()
show("parent", "before the fork")

@lf.trace
def outer():
    lf.info("buffered before the fork")
    pid = os.fork()
    if pid == 0:
        show("child ", "at the fork")
        return "child "
    os.waitpid(pid, 0)
    return "parent"
who = outer()
lf.flush()
show(who, "after its span closed and a flush()")
if who == "child ":
    os._exit(0)
