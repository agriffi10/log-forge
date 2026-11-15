"""P7: fork from inside sink.close(), which a no-worker shutdown() runs on the caller's thread.
The child continues inside _shutdown_worker with the counters _clear_after_fork reset. It reports
Runtime._shutdown_running, then runs the scenario that counter exists for: a span whose worker is
built while a later no-worker shutdown() is inside a (parked) close. 'control' runs the same
scenario in a process that never forked."""
import faulthandler, os, sys, threading, time, warnings, subprocess
if len(sys.argv) == 1:
    for row in ("control", "fork-inside-close"):
        r = subprocess.run([sys.executable, "-B", __file__, row], capture_output=True, text=True, timeout=90)
        print(f"{row:18s} exit={r.returncode} {r.stdout.strip()}  stderr: {r.stderr.strip()[:300]}")
    sys.exit(0)
import log_foundry
from log_foundry import _lifecycle
warnings.simplefilter("ignore"); faulthandler.dump_traceback_later(30, exit=True)
row = sys.argv[1]
class Rec:
    def __init__(self, name, fork=False, park=False):
        self.name = name; self.n = 0; self.closes = 0; self.fork = fork; self.park = park
        self.entered = threading.Event(); self.release = threading.Event()
    def emit(self, batch): self.n += len(batch)
    def close(self):
        self.closes += 1
        if self.park: self.entered.set(); self.release.wait(10)
        if self.fork:
            self.fork = False
            r, w = os.pipe(); pid = os.fork()
            if pid: os.close(w); print(os.read(r, 65536).decode(), flush=True); os.waitpid(pid, 0); os._exit(0)
            os.close(r); Rec.out = w            # the child returns from close() into _shutdown_worker
@log_foundry.trace
def work(): pass
def scenario():
    c = Rec("C", park=True)
    log_foundry.configure(sink=c); log_foundry.info("arm C")
    stopper = threading.Thread(target=lambda: log_foundry.shutdown(timeout=5)); stopper.start()
    assert c.entered.wait(5)
    before = _lifecycle._state._shutdown_running
    work()                                    # builds the worker while that shutdown is inside
    c.release.set(); stopper.join(10)
    h = log_foundry.health(); w = _lifecycle._state.worker_exists()
    return (f"_shutdown_running during the shutdown={before}, after={_lifecycle._state._shutdown_running}; "
            f"after shutdown() returned: state={_lifecycle._state.state.name} worker_live={_lifecycle._state.live_worker() is not None} "
            f"drain_thread_alive={w._thread.is_alive()} C.events={c.n} C.closes={c.closes} queued={h.queued}")
if row == "control":
    log_foundry.configure(service="p7", sink=Rec("S")); log_foundry.info("arm S"); log_foundry.shutdown(timeout=5)
    print(scenario())
else:
    Rec.out = None
    log_foundry.configure(service="p7", sink=Rec("S", fork=True)); log_foundry.info("arm S")
    log_foundry.shutdown(timeout=5)            # S.close() forks; only the child comes back here
    head = f"child: _shutdown_running={_lifecycle._state._shutdown_running}; "
    os.write(Rec.out, (head + scenario()).encode()); os._exit(0)
