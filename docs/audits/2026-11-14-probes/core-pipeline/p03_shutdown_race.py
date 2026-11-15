"""P3: shutdown() racing emits from 8 threads; then P10: keep submitting after shutdown."""
import sys, threading
import log_foundry as lf
from fakes import CountingSink, StderrCapture, show_health

sink = CountingSink()
lf.configure(sink=sink)

@lf.trace
def work():
    pass

stop = threading.Event()
go = threading.Event()
counts = [0] * 8
LIMIT = int(sys.argv[1]) if len(sys.argv) > 1 else 400   # spans per thread after which a thread stops by itself

def loop(k):
    go.wait()
    while not stop.is_set() and counts[k] < LIMIT:
        work()
        counts[k] += 1

threads = [threading.Thread(target=loop, args=(k,)) for k in range(8)]
with StderrCapture() as err:
    for t in threads: t.start()
    go.set()
    while sum(counts) < 8 * LIMIT // 4:   # shutdown once a quarter of the work is accepted
        pass
    lf.shutdown()
    at_shutdown = sum(counts)
    for t in threads: t.join()
    stop.set()
    accepted = sum(counts)
    h = show_health(lf, "after")
delivered = sink.count("span.end")
starts = sink.count("span.start")
print(f"accepted spans={accepted} (about {at_shutdown} when shutdown() returned); delivered span.end={delivered} span.start={starts}; sink.closed={sink.closed}")
print(f"IDENTITY spans: accepted {accepted} == delivered {delivered} + queued {h.queued} + dropped {h.dropped} -> {accepted == delivered + h.queued + h.dropped}")
print(f"undelivered = {accepted - delivered}; submitted_after_shutdown = {h.submitted_after_shutdown}; "
      f"over-count = {h.submitted_after_shutdown - (accepted - delivered)}")
print(f"sum of loss terms queued+dropped+submitted_after_shutdown = {h.queued + h.dropped + h.submitted_after_shutdown} vs undelivered {accepted - delivered}")
print(f"duplicates delivered: {len(sink.log_ids()) - len(set(sink.log_ids()))}")
for l in err.lines():
    print("  stderr:", l)
