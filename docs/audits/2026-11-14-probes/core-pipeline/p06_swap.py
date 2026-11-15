"""P6: configure(sink=B) mid-stream, with A healthy and then with A stalled."""
import threading, time
import log_foundry as lf
from fakes import CountingSink, StderrCapture, show_health

@lf.trace
def work():
    pass

def run(label, stall_old):
    a, b = CountingSink("A"), CountingSink("B")
    lf.configure(sink=a)
    lf.flush(5)
    counts = [0] * 4
    LIMIT = 1500
    def loop(k):
        while counts[k] < LIMIT:
            work(); counts[k] += 1
    base = lf.health()
    with StderrCapture() as err:
        ts = [threading.Thread(target=loop, args=(k,)) for k in range(4)]
        if stall_old:
            a.gate.clear()
        for t in ts: t.start()
        while sum(counts) < 2000: pass
        t0 = time.monotonic()
        lf.configure(sink=b)
        swap_s = time.monotonic() - t0
        for t in ts: t.join()
        r1 = lf.flush(2)
        a.gate.set()
        r2 = lf.flush(10)
        w = lf._lifecycle._state.worker_exists()
        h = lf.health()
    accepted = sum(counts)
    ea, eb = a.count("span.end"), b.count("span.end")
    ids = a.log_ids() + b.log_ids()
    print(f"[{label}] accepted spans={accepted} A={ea} B={eb} A+B={ea+eb} dup={len(ids)-len(set(ids))} "
          f"dropped +{h.dropped-base.dropped} failed_batches +{h.failed_batches-base.failed_batches} "
          f"incomplete_swaps +{h.incomplete_swaps-base.incomplete_swaps} swap took {swap_s:.2f}s A.closed={a.closed}")
    print(f"    flush while A stalled={r1}; flush after A recovered={r2}")
    print(f"    IDENTITY: {accepted} == {ea+eb} + {h.dropped-base.dropped} -> {accepted == ea+eb+h.dropped-base.dropped}")
    for l in err.lines(): print("    stderr:", l)

run("old sink healthy", False)
run("old sink stalled in emit", True)
lf.shutdown()
