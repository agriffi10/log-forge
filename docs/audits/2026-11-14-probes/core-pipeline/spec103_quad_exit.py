"""SPEC-103, written on 2026-11-17 for its mechanism replacement: A dead sink whose failing attempt costs A*n**2 seconds, max_retries=0; budget sweep as step_exit.py.
Usage: quad_exit.py <A> <spans> <t0> <t1> <step>"""
import io, sys, threading, time
from log_foundry.worker import Worker
A, SP, T0, T1, ST = float(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3]), float(sys.argv[4]), float(sys.argv[5])
late = expired = runs = 0; seen = set(); t = T0
while t <= T1 + 1e-9:
    gate = threading.Event(); entered = threading.Event(); sizes = []
    class Sink:
        def emit(self, events):
            if any("first" in e for e in events):
                entered.set(); gate.wait(30); raise ConnectionError("down")
            sizes.append(len(events)); time.sleep(A * len(events) ** 2); raise ConnectionError("dead")
        def close(self): pass
    w = Worker(Sink(), batch_size=10, flush_interval=1.0, max_queue=100_000, max_retries=0)
    w.submit([{"first": 1}]); time.sleep(0.05)
    for _ in range(SP): w.submit([{"a": 1}])
    entered.wait(5); buf = io.StringIO(); old = sys.stderr; sys.stderr = buf; gate.set()
    t0 = time.monotonic(); w.stop(t); took = time.monotonic() - t0; sys.stderr = old
    runs += 1; expired += "timed out" in buf.getvalue(); late += took > t; seen.add(max(sizes or [0])); t += ST
print(f"quad A={A}: runs={runs} expired={expired} over-budget={late} largest={sorted(seen)}")
