"""SPEC-090, written on 2026-11-15 against 3e27a0a6 and not one of the audit's own probes: a backlog under a frozen clock.
time.monotonic is patched to a constant, as tests/test_sinks_file.py does, so every emit measures zero elapsed time. A size
target computed as events over elapsed divides by zero on the drain thread, which is terminal. The probe reports whether
the drain survived and delivered the backlog.
Usage: python spec090_frozen_clock.py
"""
import threading, time
import log_foundry as lf

class Gated:
    def __init__(self): self.gate = threading.Event(); self.n = 0; self.sizes = []
    def emit(self, b):
        self.gate.wait(); self.n += len(b); self.sizes.append(len(b))
    def close(self): pass
s = Gated(); lf.configure(sink=s)
@lf.trace
def work(): pass
for _ in range(10): work()
real = time.monotonic
frozen = real()
time.monotonic = lambda: frozen
for _ in range(2000): work()
s.gate.set()
deadline = real() + 10
while s.n < 2 * 2010 and real() < deadline:
    time.sleep(0.05)
h = lf.health()
time.monotonic = real
print(f"delivered {s.n} of {2 * 2010} events in {len(s.sizes)} emit(s), largest {max(s.sizes)}; "
      f"stopped_reason={h.stopped_reason}")
