"""P10, written while authoring SPEC-086 and not one of the audit's own probes: a child forked while another thread of the parent is writing to sys.stderr, whose first loss the library then announces.
Each row forks FORKS children from the main thread. In `library` and `plain` a second thread of the parent writes to
sys.stderr without pause, so a fork can land while that thread holds the stream's lock. The `library` child shuts down
and makes one traced call, which the library announces with `_diag.lost` ("logged after shutdown()"); the `plain` child
calls no library code and writes one line to sys.stderr itself; `quiet` is `library` with no writer thread. Every child
arms a 2 s alarm first, and one the alarm kills is counted as hung. stderr is the null device, opened as a buffered
file, which is what a test runner's capture gives a process too."""
import os, signal, subprocess, sys, threading
ROWS, FORKS = ["library", "plain", "quiet"], 100
if len(sys.argv) == 1:
    for row in ROWS:
        r = subprocess.run([sys.executable, "-B", __file__, row], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, timeout=900)
        print(f"{row:8s} {r.stdout.strip()}")
    sys.exit(0)

import log_foundry
row = sys.argv[1]

@log_foundry.trace
def work(): log_foundry.info("in")

class Sink:
    def emit(self, batch): pass
    def close(self): pass

log_foundry.configure(service="p10", sink=Sink())
work(); log_foundry.flush(timeout=2.0)
stop = threading.Event()
def writer():
    line = "x" * 512 + "\n"
    while not stop.is_set():
        sys.stderr.write(line)
if row != "quiet":
    threading.Thread(target=writer, daemon=True).start()
hung = other = 0
for _ in range(FORKS):
    pid = os.fork()
    if pid == 0:
        signal.alarm(2)
        if row == "plain":
            sys.stderr.write("child\n")
        else:
            log_foundry.shutdown(timeout=1.0)
            work()
        os._exit(0)
    _, status = os.waitpid(pid, 0)
    if os.WIFSIGNALED(status) and os.WTERMSIG(status) == signal.SIGALRM:
        hung += 1
    elif not (os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0):
        other += 1
stop.set()
print(f"{hung} of {FORKS} children hung until their alarm; {other} ended any other way; stderr is {type(sys.stderr).__name__} over {type(sys.stderr.buffer).__name__}")
