"""P2: fork x configure(sink=...) in flight x the child's own configure(sink=...).
A parent thread T1 is parked inside configure(sink=B) (in B's stop-signal setter, so it holds
config._sink_lock and config._sink_holder == T1's ident). Another thread forks. The child reports
what it inherited, then starts threads and looks for one whose ident equals the stale holder; if it
finds one, it runs configure(sink=C) on it and reports whether that call was treated as re-entrant."""
import faulthandler, io, os, sys, threading, contextlib
import log_foundry
from log_foundry import config

faulthandler.dump_traceback_later(20, exit=True)

class Plain:
    def __init__(self, name): self.name = name; self.events = []; self.closes = 0
    def emit(self, batch): self.events.extend(batch)
    def close(self): self.closes += 1

class Parking(Plain):
    entered = threading.Event(); release = threading.Event()
    _signal = None
    @property
    def log_foundry_stop_signal(self): return self._signal
    @log_foundry_stop_signal.setter
    def log_foundry_stop_signal(self, value):
        self._signal = value
        if os.getpid() == PARENT:
            Parking.entered.set(); Parking.release.wait(10)

PARENT = os.getpid()
a, b = Plain("A"), Parking("B")
log_foundry.configure(service="p2", sink=a)
log_foundry.info("arm A")
t1 = threading.Thread(target=lambda: log_foundry.configure(sink=b), name="T1")
t1.start()
assert Parking.entered.wait(5), "T1 never reached the setter"
print(f"parent: T1 ident={t1.ident} holder={config._sink_holder} lock_held={config._sink_lock.locked()}", flush=True)

r, w = os.pipe()
pid = os.fork()
if pid == 0:
    os.close(r)
    out = [f"child: _sink_lock.locked()={config._sink_lock.locked()} _sink_holder={config._sink_holder} (main ident={threading.get_ident()})"]
    stale = config._sink_holder
    hit = None
    for i in range(300):
        seen = {}
        def body():
            seen["ident"] = threading.get_ident()
            if seen["ident"] == stale:
                err = io.StringIO()
                with contextlib.redirect_stderr(err):
                    log_foundry.info("child arms B"); log_foundry.configure(sink=Plain("C"))
                seen["stderr"] = err.getvalue()
        th = threading.Thread(target=body, name=f"user-{i}"); th.start(); th.join()
        if seen["ident"] == stale:
            hit = (i, seen["stderr"]); break
    if hit is None:
        out.append("child: no thread of 300 reused the stale ident on this platform")
    else:
        out.append(f"child: thread #{hit[0]} reused the stale ident; its configure(sink=C) wrote: {hit[1].strip()[:400]!r}")
        out.append(f"child: incomplete_swaps={log_foundry.health().incomplete_swaps}")
    os.write(w, "\n".join(out).encode()); os._exit(0)
os.close(w)
print(os.read(r, 65536).decode()); os.waitpid(pid, 0)
Parking.release.set(); t1.join(5)
log_foundry.shutdown(timeout=2)
