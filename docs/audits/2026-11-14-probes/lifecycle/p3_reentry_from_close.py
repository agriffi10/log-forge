"""P3: a sink whose close() calls back into the library (shutdown / flush / configure / info / a span).
Each row runs in a fresh process (argv[1] = call, argv[2] = path), with a watchdog. Reports how long the
outer shutdown(timeout=10) took, the close counts, and the stderr lines."""
import faulthandler, subprocess, sys, time
ROWS = ["shutdown", "shutdown-none", "flush", "configure", "info", "span"]
if len(sys.argv) == 1:
    for path in ("orphan", "worker", "two-sinks"):
        for call in ROWS:
            r = subprocess.run([sys.executable, "-B", __file__, call, path], capture_output=True, text=True, timeout=60)
            err = " | ".join(l[:150] for l in r.stderr.strip().splitlines() if "Thread 0x" not in l)[:420]
            print(f"{path:9s} {call:13s} exit={r.returncode} {r.stdout.strip()}  stderr: {err}")
    sys.exit(0)

import log_foundry
call, path = sys.argv[1], sys.argv[2]
faulthandler.dump_traceback_later(25, exit=True)

class Plain:
    def __init__(self, name): self.name = name; self.events = []; self.closes = 0
    def emit(self, batch): self.events.extend(batch)
    def close(self): self.closes += 1

other = Plain("other")
@log_foundry.trace
def work(): pass

class CallsBack(Plain):
    inner_seconds = None
    def close(self):
        self.closes += 1
        if self.closes > 1: return
        started = time.monotonic()
        if call == "shutdown": log_foundry.shutdown(timeout=10)
        elif call == "shutdown-none": log_foundry.shutdown(timeout=None)
        elif call == "flush": log_foundry.flush(timeout=10)
        elif call == "configure": log_foundry.configure(sink=other)
        elif call == "info": log_foundry.info("from close")
        elif call == "span": work()
        CallsBack.inner_seconds = time.monotonic() - started

s = CallsBack("S")
log_foundry.configure(service="p3", sink=s)
if path == "two-sinks":          # S superseded but still owed alongside a second owed sink
    log_foundry.info("arm S")
    t = Plain("T"); log_foundry.configure(sink=t); log_foundry.info("arm T")
elif path == "worker": work(); log_foundry.flush(timeout=2)
else: log_foundry.info("arm S")
started = time.monotonic()
log_foundry.shutdown(timeout=10)
outer = time.monotonic() - started
h = log_foundry.health()
print(f"outer={outer:.2f}s inner={CallsBack.inner_seconds if CallsBack.inner_seconds is None else round(CallsBack.inner_seconds,2)}s S.closes={s.closes} other.closes={other.closes} other.events={len(other.events)} closing_sinks={h.closing_sinks} state={log_foundry._lifecycle._state.state.name}")
