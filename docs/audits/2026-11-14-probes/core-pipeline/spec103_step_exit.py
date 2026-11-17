"""SPEC-103, written on 2026-11-17 for its mechanism replacement: A dead sink whose failing attempt costs ceil(n/CHUNK) * PER seconds (a chunked request loop), max_retries=0.
The pre-stop batch fails, so the target is 0. Sweeps the shutdown budget; reports late drains and expired joins.
Usage: step_exit.py <chunk> <per_s> <spans> <t0> <t1> <step>"""
import io, math, sys, threading, time
from log_foundry.worker import Worker
CH, PER, SP, T0, T1, ST = int(sys.argv[1]), float(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4]), float(sys.argv[5]), float(sys.argv[6])
late = expired = 0; runs = 0; sizes_seen = set()
t = T0
while t <= T1 + 1e-9:
    gate = threading.Event(); entered = threading.Event(); sizes = []
    class Sink:
        def emit(self, events):
            if any("first" in e for e in events):
                entered.set(); gate.wait(30); raise ConnectionError("down")
            sizes.append(len(events)); time.sleep(math.ceil(len(events) / CH) * PER); raise ConnectionError("dead")
        def close(self): pass
    w = Worker(Sink(), batch_size=10, flush_interval=1.0, max_queue=100_000, max_retries=0)
    w.submit([{"first": 1}]); time.sleep(0.05)
    for _ in range(SP): w.submit([{"a": 1}])
    entered.wait(5)
    buf = io.StringIO(); old = sys.stderr; sys.stderr = buf
    gate.set()
    t0 = time.monotonic(); w.stop(t); took = time.monotonic() - t0
    sys.stderr = old
    runs += 1
    if "timed out" in buf.getvalue(): expired += 1
    if took > t: late += 1
    sizes_seen.add(max(sizes or [0]))
    t += ST
print(f"chunk={CH} per={PER}: runs={runs} expired={expired} over-budget={late} largest batches seen={sorted(sizes_seen)}")
