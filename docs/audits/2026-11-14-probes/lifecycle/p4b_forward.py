"""P4b in the other direction: the outgoing sink becomes a child of the incoming graph.
configure(sink=FileSink) -> 1 event -> configure(sink=MultiSink(that FileSink, MemorySink)) -> 3 events
-> shutdown. Counts the lines that reached the file and what health() says about the rest.
Not a frame's probe: written while the audit was brought into the repository, as
p4b_shared_child_shipped_sink.py with its two configure lines exchanged, because architecture.md
section 12 leads with this direction and the lifecycle frame probed only the other."""
import os, sys, tempfile, time, subprocess
if len(sys.argv) == 1:
    for path in ("orphan", "worker"):
        r = subprocess.run([sys.executable, "-B", __file__, path], capture_output=True, text=True, timeout=60)
        print(f"{path:6s} exit={r.returncode} {r.stdout.strip()}\n       stderr: {' | '.join(l[:170] for l in r.stderr.strip().splitlines())[:700]}")
    sys.exit(0)
import faulthandler
import log_foundry
from log_foundry.sinks.file import FileSink
from log_foundry.sinks.memory import MemorySink
from log_foundry.sinks.multi import MultiSink
faulthandler.dump_traceback_later(25, exit=True)
path = sys.argv[1]
target = os.path.join(tempfile.mkdtemp(prefix="p4b-"), "out.jsonl")
f, m = FileSink(target), MemorySink()
@log_foundry.trace
def work(): pass
def write(n):
    for _ in range(n):
        if path == "worker": work()
        else: log_foundry.info("x")
    if path == "worker": log_foundry.flush(timeout=3)
log_foundry.configure(service="p4b", sink=f); write(1)
log_foundry.configure(sink=MultiSink(f, m)); time.sleep(0.3)
write(3)
log_foundry.shutdown(timeout=5)
h = log_foundry.health()
lines = sum(1 for _ in open(target))
per = 2 if path == "worker" else 1     # a span is a start and an end event
print(f"events written by the caller={4*per} lines in the file={lines} | orphan_lost={h.orphan_lost} failed_batches={h.failed_batches} queued={h.queued} sink={h.sink}")
