"""SPEC-090, written on 2026-11-15 against 3e27a0a6 and not one of the audit's own probes: the lines a dead sink writes at exit.
The sink's first emit waits on a gate and every later one raises at once, so each batch is abandoned in microseconds. 10,000
spans are queued and shutdown() runs the exit drain. Today that is one batch and one abandon line. A per-batch exit drain
abandons about 1,000 batches; the probe counts the lines and the events they name, which should stay a handful.
Usage: python spec090_dead_sink_exit_lines.py
"""
import io, re, sys, threading, time
import log_foundry as lf

class Dead:
    def __init__(self): self.gate = threading.Event(); self.first = True
    def emit(self, b):
        if self.first:
            self.first = False; self.gate.wait(); raise RuntimeError("down")
        raise RuntimeError("down")
    def close(self): pass
s = Dead(); lf.configure(sink=s)
@lf.trace
def work(): pass
work()
time.sleep(1.2)
for _ in range(10_000): work()
real, buf = sys.stderr, io.StringIO(); sys.stderr = buf
threading.Timer(0.05, s.gate.set).start()
t0 = time.monotonic(); lf.shutdown(timeout=30); took = time.monotonic() - t0
sys.stderr = real
lines = [l for l in buf.getvalue().splitlines() if l.startswith("log-foundry")]
named = sum(int(m.group(1)) for l in lines if (m := re.match(r"log-foundry: lost (\d+) event", l)))
h = lf.health()
print(f"shutdown took {took:.2f}s; {len(lines)} line(s) naming {named} lost event(s) of 20,002; "
      f"failed_batches={h.failed_batches}")
for l in lines[:6]:
    print("  ", l)
