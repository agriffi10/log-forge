"""SPEC-103, written on 2026-11-17 for its mechanism replacement: Recovered sink: the pre-stop batch fails (target 0), the first exit batch fails after X s, then every batch is accepted
at 1 ms an event; max_retries=0. Sweeps budgets T0..T1 and prints total events delivered.
Usage: band.py <X> <t0> <t1> <step>"""
import io, sys, threading, time
from log_foundry.worker import Worker
X, T0, T1, ST = float(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3]), float(sys.argv[4])
total = 0; expired = 0; t = T0; zero_runs = 0
while t <= T1 + 1e-9:
    gate = threading.Event(); entered = threading.Event(); got = [0]; n = [0]
    class Sink:
        def emit(self, events):
            if any("first" in e for e in events):
                entered.set(); gate.wait(30); raise ConnectionError("down")
            n[0] += 1
            if n[0] == 1:
                time.sleep(X); raise ConnectionError("still down")
            time.sleep(0.001 * len(events)); got[0] += len(events)
        def close(self): pass
    w = Worker(Sink(), batch_size=10, flush_interval=1.0, max_queue=100_000, max_retries=0)
    w.submit([{"first": 1}]); time.sleep(0.05)
    for _ in range(2000): w.submit([{"a": 1}])
    entered.wait(5); buf = io.StringIO(); old = sys.stderr; sys.stderr = buf; gate.set()
    w.stop(t); sys.stderr = old
    total += got[0]; zero_runs += got[0] == 0; expired += "timed out" in buf.getvalue(); t += ST
print(f"X={X}: delivered total={total} runs delivering 0={zero_runs} expired={expired}")
