"""P8, written while authoring SPEC-086 and not one of the audit's own probes: a sink closed twice with nothing written between the two closes, traced to show it is not a double close.
Usage: p8_second_close_after_a_rearm.py [seed [runs]]   (defaults: seed 1653, 300 runs)
Each run is a fresh process. It draws exactly what tests/test_invariants_model.py draws for that seed in its `emit` form
(spans, orphan logs, flush(timeout=2.0) and configure(sink=new); the only shutdown() is the last call), counts every
delivery call at its first statement, and wraps the close registry's `arm` and `take` to record them. For a sink whose
two closes entered at one write count it prints the trace: an orphan log arms the sink, a sink change takes it, the same
log arms it again (rearm_if_released) and writes, the first close runs, and a second sink change takes and closes it.
One take per close with a writer's arm between the takes is invariant 5's SPEC-045 exception, a sink written to after
its close was taken being owed a second one. A rule that reads only the sink sees two closes at one count."""
import io, json, random, subprocess, sys, threading, time
SEED = int(sys.argv[1]) if len(sys.argv) > 1 else 1653
if len(sys.argv) < 4:
    runs = int(sys.argv[2]) if len(sys.argv) > 2 else 300
    hits, shown = 0, 0
    for _ in range(runs):
        r = subprocess.run([sys.executable, "-B", __file__, str(SEED), "1", "child"], capture_output=True, text=True, timeout=120)
        report = json.loads(r.stdout.strip().splitlines()[-1])
        if report["repeat"]:
            hits += 1
            if shown < 3:
                shown += 1
                print(json.dumps(report))
    print(f"seed={SEED}: {hits} of {runs} runs closed a sink twice at one write count")
    sys.exit(0)

import log_foundry
captured = io.StringIO()
sys.stderr = captured
THREADS, OPS, CLOSE_SECONDS = 4, 60, 0.002
sinks = []

from log_foundry import _lifecycle as _lc
trace = []
def _names(depth=6):
    f = sys._getframe(2); out = []
    while f is not None and len(out) < depth:
        out.append(f.f_code.co_name); f = f.f_back
    return "<".join(out)
_arm, _take = _lc._CloseRegistry.arm, _lc._CloseRegistry.take
def arm(self, sink):
    trace.append(("arm", id(sink), getattr(sink, "entered", None), threading.current_thread().name, time.monotonic(), _names()))
    return _arm(self, sink)
def take(self, sink, rule):
    trace.append(("take", id(sink), getattr(sink, "entered", None), threading.current_thread().name, time.monotonic(), _names()))
    return _take(self, sink, rule)
_lc._CloseRegistry.arm, _lc._CloseRegistry.take = arm, take

class Recorder:
    log_foundry_stop_signal = None
    def __init__(self, emit_delay):
        self.emit_delay, self.entered, self.closes, self._lock = emit_delay, 0, [], threading.Lock()
        sinks.append(self)
    def emit(self, batch):
        with self._lock:
            self.entered += 1
            trace.append(("write", id(self), self.entered, threading.current_thread().name, time.monotonic(), _names(4)))
        if self.emit_delay:
            time.sleep(self.emit_delay)
    def close(self):
        with self._lock:
            self.closes.append((self.entered, threading.current_thread().name, time.monotonic()))
            trace.append(("close", id(self), self.entered, threading.current_thread().name, time.monotonic(), ""))
        time.sleep(CLOSE_SECONDS)

@log_foundry.trace
def inner():
    log_foundry.info("in")

@log_foundry.trace
def outer(nested):
    log_foundry.info("in")
    if nested:
        inner()

def actor(rng):
    for _ in range(OPS):
        roll = rng.random()
        if roll < 0.55 or roll >= 0.95:
            outer(rng.random() < 0.3)
        elif roll < 0.75:
            log_foundry.info("orphan")
        elif roll < 0.85:
            log_foundry.flush(timeout=2.0)
        else:
            log_foundry.configure(service="model", sink=Recorder(rng.choice([0.0, 0.0, 0.002])))

log_foundry.configure(service="model", sink=Recorder(random.Random(SEED).choice([0.0, 0.0, 0.002])))
barrier = threading.Barrier(THREADS)
def run(index):
    barrier.wait()
    actor(random.Random(SEED * 10 + index))
pool = [threading.Thread(target=run, args=(i,)) for i in range(THREADS)]
interval = sys.getswitchinterval()
sys.setswitchinterval(1e-6)
for t in pool: t.start()
for t in pool: t.join(60)
sys.setswitchinterval(interval)
log_foundry.shutdown(timeout=5.0)
repeat = []
for index, sink in enumerate(sinks):
    for (a, ta, wa), (b, tb, wb) in zip(sink.closes, sink.closes[1:]):
        if a == b:
            repeat.append({"sink": index, "writes_at_both_closes": a, "writes_in_all": sink.entered,
                           "threads": [ta, tb], "ms_apart": round((wb - wa) * 1000, 1)})
lines = [line for line in captured.getvalue().splitlines() if line.startswith("log-foundry:")]
tr = []
for r in repeat:
    sid = id(sinks[r["sink"]]); t0 = None
    for op, i, n, th, t, st in trace:
        if i == sid:
            t0 = t if t0 is None else t0
            tr.append([op, n, th, round((t - t0) * 1000, 2), st])
sys.stdout.write(json.dumps({"trace": tr, "repeat": repeat, "sinks": len(sinks), "stderr_lines": lines[:4], "file": log_foundry.__file__}) + "\n")
