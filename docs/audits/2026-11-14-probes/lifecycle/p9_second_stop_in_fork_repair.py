"""P9, written while authoring SPEC-086 and not one of the audit's own probes: a stop-signal setter that calls shutdown() during a forked child's own fork repair.
The parent configures a sink, makes one traced call so a worker is live on it, and forks. The child's fork repair hands the
inherited sink its stop signal before the child's drain thread exists; the sink's setter, in the child only, makes the calls
of one row. The parent waits DEADLINE seconds, asks a child that is still alive for its stacks (a faulthandler handler on
SIGUSR1, registered before the fork, so the child inherits it), then kills it. The `outside` rows make the same calls from
a setter in a process that never forked. No library code is patched."""
import faulthandler, os, signal, subprocess, sys, tempfile, time
ROWS = ["one-stop", "two-stops-second-none", "two-stops-second-finite", "outside-two-stops"]
DEADLINE = 6.0
if len(sys.argv) == 1:
    for row in ROWS:
        r = subprocess.run([sys.executable, "-B", __file__, row], capture_output=True, text=True, timeout=60)
        print(f"{row:24s} {r.stdout.strip()}")
    sys.exit(0)

import log_foundry
row = sys.argv[1]
PARENT = os.getpid()
took = []

class Sink:
    _signal = None
    def __init__(self): self.fired = False
    @property
    def log_foundry_stop_signal(self): return self._signal
    @log_foundry_stop_signal.setter
    def log_foundry_stop_signal(self, value):
        self._signal = value
        inside_repair = os.getpid() != PARENT
        if self.fired or (not inside_repair and not row.startswith("outside")):
            return
        self.fired = True
        began = time.monotonic()
        if row == "one-stop":
            log_foundry.shutdown(timeout=None)
        elif row == "two-stops-second-finite":
            log_foundry.shutdown(timeout=3.0); log_foundry.shutdown(timeout=3.0)
        else:
            log_foundry.shutdown(timeout=3.0); log_foundry.shutdown(timeout=None)
        took.append(round(time.monotonic() - began, 2))
    def emit(self, batch): pass
    def close(self): pass

@log_foundry.trace
def work(): log_foundry.info("in")

if row.startswith("outside"):
    faulthandler.dump_traceback_later(20, exit=True)
    log_foundry.configure(service="p9", sink=Sink()); work(); log_foundry.shutdown(timeout=3.0)
    print(f"setter's calls took {took} s; no fork")
    sys.exit(0)

log_foundry.configure(service="p9", sink=Sink())
work()
stacks = tempfile.TemporaryFile(mode="w+")
faulthandler.register(signal.SIGUSR1, file=stacks, all_threads=True)
read_end, write_end = os.pipe()
began = time.monotonic()
pid = os.fork()
if pid == 0:
    os.close(read_end)
    os.write(write_end, f"repair returned; setter's calls took {took} s".encode())
    os._exit(0)
os.close(write_end)
status = None
while time.monotonic() - began < DEADLINE:
    done, status = os.waitpid(pid, os.WNOHANG)
    if done:
        break
    status = None
    time.sleep(0.05)
if status is None:
    os.kill(pid, signal.SIGUSR1); time.sleep(0.5); os.kill(pid, signal.SIGKILL); os.waitpid(pid, 0)
    stacks.seek(0)
    frames = [line.split(" in ")[-1] for line in stacks.read().splitlines() if "log_foundry/" in line or __file__ in line]
    print(f"child never left its fork repair in {DEADLINE} s; its stack, innermost first: {' < '.join(frames)}")
else:
    print(f"child after {time.monotonic() - began:.2f} s: {os.read(read_end, 4096).decode()}")
log_foundry.shutdown(timeout=3.0)
