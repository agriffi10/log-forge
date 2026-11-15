"""P7: fork with a non-empty queue in the parent and an open span holding buffered events."""
import json, os, sys, threading, collections
import log_foundry as lf

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "p07_out.jsonl")
if os.path.exists(OUT):
    os.remove(OUT)

class FileSink:
    def __init__(self):
        self.owner = os.getpid()
        self.gate = threading.Event()
        self.gate.set()
        self.entered = threading.Event()
    def emit(self, batch):
        if os.getpid() == self.owner:
            self.entered.set()
            self.gate.wait()
        fd = os.open(OUT, os.O_WRONLY | os.O_APPEND | os.O_CREAT)
        try:
            for e in batch:
                os.write(fd, (json.dumps({"pid": os.getpid(), "log_id": e["log_id"], "message": e["message"],
                                          "function": e["function"]}) + "\n").encode())
        finally:
            os.close(fd)
    def close(self):
        pass

sink = FileSink()
lf.configure(sink=sink)

@lf.trace
def queued_before_fork(i):
    pass

@lf.trace
def outer():
    lf.info("buffered-in-open-span-before-fork")
    pid = os.fork()
    if pid == 0:
        lf.info("child-after-fork")
    else:
        lf.info("parent-after-fork")
    return pid

sink.gate.clear()
for i in range(25):
    queued_before_fork(i)          # 25 submissions: 10 go in flight, 15 stay queued
sink.entered.wait(5)
h = lf.health()
print(f"parent before fork: queued={h.queued}", flush=True)
pid = outer()
if pid == 0:
    r = lf.flush(5)
    h = lf.health()
    os.write(1, f"child:  flush={r} queued={h.queued} dropped={h.dropped} failed_batches={h.failed_batches} stopped_reason={h.stopped_reason}\n".encode())
    os._exit(0)
os.waitpid(pid, 0)
sink.gate.set()
r = lf.flush(5)
h = lf.health()
print(f"parent: flush={r} queued={h.queued} dropped={h.dropped} failed_batches={h.failed_batches}")
lf.shutdown()
rows = [json.loads(l) for l in open(OUT)]
me = os.getpid()
by = collections.Counter(("parent" if r["pid"] == me else "child", r["message"] if r["function"] != "queued_before_fork" else "queued_before_fork:" + r["message"]) for r in rows)
for k in sorted(by): print("  ", k, by[k])
ids = collections.Counter(r["log_id"] for r in rows)
dups = [i for i, c in ids.items() if c > 1]
print(f"rows={len(rows)} distinct log_ids={len(ids)} log_ids delivered twice={len(dups)}")
for d in dups:
    print("   duplicated:", [ (("parent" if r['pid']==me else 'child'), r["message"]) for r in rows if r["log_id"] == d])
pre = sum(1 for r in rows if r["function"] == "queued_before_fork")
print(f"pre-fork queue: accepted events=50 delivered rows={pre}")
