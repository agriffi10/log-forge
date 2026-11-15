"""P5: the atexit drain through a real interpreter exit. Rows (argv[1]):
  two-owed      two sinks owed a close at exit (the exit drain fans closes out on threads)
  factory-thread a sink_factory whose sink starts a helper thread on its first emit, first reached
                 in the exit drain (a short script: one traced call, then the interpreter exits)
  late-span     another atexit handler, registered earlier (so it runs later), runs a traced call
Each row prints what its sinks saw from a final atexit handler registered FIRST (it runs last)."""
import atexit, subprocess, sys, threading
if len(sys.argv) == 1:
    for row in ("two-owed", "factory-thread", "late-span"):
        r = subprocess.run([sys.executable, "-B", __file__, row], capture_output=True, text=True, timeout=90)
        print(f"{row:15s} exit={r.returncode} {r.stdout.strip()}\n   stderr: {' | '.join(l[:200] for l in r.stderr.strip().splitlines())[:600]}")
    sys.exit(0)
import log_foundry
row = sys.argv[1]
class Plain:
    def __init__(self, name): self.name = name; self.n = 0; self.closes = 0; self.close_threads = []
    def emit(self, batch): self.n += len(batch)
    def close(self): self.closes += 1; self.close_threads.append(threading.current_thread().name)
class Threaded(Plain):
    """Starts a helper thread on its first emit, as a client with a background sender does."""
    helper = None; error = None
    def emit(self, batch):
        if self.helper is None:
            try:
                self.helper = threading.Thread(target=lambda: None, daemon=True); self.helper.start()
            except BaseException as exc:
                Threaded.error = repr(exc); raise
        super().emit(batch)
sinks = []
def report():
    h = log_foundry.health()
    print(" ".join(f"{s.name}: events={s.n} closes={s.closes} on={s.close_threads}" for s in sinks),
          f"| helper_error={Threaded.error} failed_batches={h.failed_batches} queued={h.queued} after_shutdown={h.submitted_after_shutdown}", flush=True)
atexit.register(report)               # registered first, runs last
@log_foundry.trace
def work(): pass
if row == "two-owed":
    a, b = Plain("A"), Plain("B"); sinks += [a, b]
    log_foundry.configure(service="p5", sink=a); log_foundry.info("a")
    # leave A owed and unheld while B becomes owed too: a swap would close A, so arm B through a worker
    work()                                  # worker on A
    import log_foundry._lifecycle as L
    L._note_orphan_emit(b); b.emit([{}])    # the orphan path's own arm call, for a second owed sink
elif row == "factory-thread":
    t = Threaded("T"); sinks.append(t)
    log_foundry.configure(service="p5", sink_factory=lambda: t)
    work()                                  # queued; the first emit happens in the exit drain
elif row == "late-span":
    a = Plain("A"); sinks.append(a)
    atexit.register(work)                   # registered before the library's handler: runs after it
    log_foundry.configure(service="p5", sink=a); work()
