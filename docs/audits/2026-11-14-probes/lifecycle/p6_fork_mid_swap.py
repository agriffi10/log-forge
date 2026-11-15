"""P6: fork x configure(sink=B) in flight (live worker on A) x the child's own logging.
The parent's drain thread is parked inside A.emit, so T1's configure(sink=B) has written the
config and is waiting in Worker.retarget's first flush (the worker still delivers to A). Another
thread forks there. The child then logs one span and one orphan log, flushes, and reports where
each landed; the parent finishes its swap and reports the same for itself. No library code is patched."""
import faulthandler, os, sys, threading, time, warnings
import log_foundry
from log_foundry import _lifecycle, config
warnings.simplefilter("ignore")
faulthandler.dump_traceback_later(30, exit=True)
PARENT = os.getpid()
class Rec:
    park = threading.Event(); inside = threading.Event(); release = threading.Event()
    def __init__(self, name): self.name = name; self.msgs = []; self.closes = 0
    def emit(self, batch):
        if os.getpid() == PARENT and self.name == "A" and Rec.park.is_set():
            Rec.inside.set(); Rec.release.wait(20)
        self.msgs += [(e.get("message"), e.get("span_name") or e.get("name") or e.get("fn") or e.get("function")) for e in batch]; Rec.keys = sorted(batch[0])
    def close(self): self.closes += 1
    def kinds(self): return [m for m in self.msgs if any(str(x).split("-")[0] in ("child", "parent") for x in m)]
def traced(name):
    @log_foundry.trace(name=name)
    def f(): pass
    return f
a, b = Rec("A"), Rec("B")
log_foundry.configure(service="p6", sink=a)
traced("warm")(); assert log_foundry.flush(timeout=3)
Rec.park.set(); traced("parks-the-drain")()          # the drain thread is now inside A.emit
log_foundry.flush(timeout=0.01)
assert Rec.inside.wait(5), "the drain thread never reached A.emit"
t1 = threading.Thread(target=lambda: log_foundry.configure(sink=b), name="T1"); t1.start()
deadline = time.monotonic() + 5
while config._live_config().sink is not b and time.monotonic() < deadline: time.sleep(0.005)
time.sleep(0.2)                                       # T1 is now waiting in retarget's first flush
w = _lifecycle._state.worker_exists()
print(f"parent at the fork: config.sink={config._live_config().sink.name} worker.sink={w.sink.name} swaps_running={len(_lifecycle._state._swaps_running)}", flush=True)
r, wfd = os.pipe()
pid = os.fork()
if pid == 0:
    os.close(r)
    cw = _lifecycle._state.worker_exists()
    out = [f"child after repair: config.sink={config._live_config().sink.name} worker.sink={cw.sink.name} state={_lifecycle._state.state.name} swaps_running={len(_lifecycle._state._swaps_running)}"]
    a.msgs.clear(); b.msgs.clear()
    traced("child-span")(); log_foundry.info("child-orphan")
    ok = log_foundry.flush(timeout=3)
    out.append(f"child flush={bool(ok)} A got {a.kinds()} B got {b.kinds()}")
    log_foundry.shutdown(timeout=3)
    h = log_foundry.health()
    out.append(f"child after shutdown: A.closes={a.closes} B.closes={b.closes} inherited_sink={h.inherited_sink} queued={h.queued}")
    os.write(wfd, "\n".join(out).encode()); os._exit(0)
os.close(wfd); print(os.read(r, 65536).decode(), flush=True); os.waitpid(pid, 0)
Rec.park.clear(); Rec.release.set(); t1.join(10)
a.msgs.clear(); b.msgs.clear()
traced("parent-span")(); log_foundry.info("parent-orphan"); log_foundry.flush(timeout=3)
pw = _lifecycle._state.worker_exists()
print(f"parent after its swap: config.sink={config._live_config().sink.name} worker.sink={pw.sink.name} A got {a.kinds()} B got {b.kinds()}")
log_foundry.shutdown(timeout=3)
