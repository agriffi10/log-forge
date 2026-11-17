"""SPEC-103, written on 2026-11-17: a custom sink that refuses any batch above LIMIT events, and fails some exit batches transiently.
It costs MS milliseconds per event, so its measured size target sits below LIMIT. The first steady batch is delivered behind a
gate, or, with `zero`, fails every attempt so the target is 0 at the stop. The exit batches numbered FAIL (1-based, counting
batches, not attempts; `3` or a range `1-5`) fail every attempt at once, for a reason other than their size; a batch past
LIMIT is refused at once too. A batch is known by the sequence number its first span carries, never by an object's id(),
which a freed event can hand to a later one. It prints the events delivered of those accepted and the batch sizes handed over.
A closure that let an abandoned exit batch raise a non-zero target to its own rate would hand every later batch more than LIMIT
events after one transient failure.
The stop is claimed only once the gated first batch is inside the sink, so that batch is never an exit batch
(revised 2026-11-17: before, a stop claimed first made it one, and 3 of 18 runs differed).
Usage: python spec103_size_limited_exit.py <ms_per_event> <limit> <fail> <spans> [zero]
"""

import io, sys, threading, time
from log_foundry.worker import Worker

MS, LIMIT, SPANS = float(sys.argv[1]) / 1000, int(sys.argv[2]), int(sys.argv[4])
lo, _, hi = sys.argv[3].partition("-")
FAIL = range(int(lo), int(hi or lo) + 1)
ZERO = len(sys.argv) > 5 and sys.argv[5] == "zero"
gate = threading.Event()
entered = threading.Event()
sizes = []
delivered = [0]
batches = []


class Sink:
    def emit(self, events):
        if any("first" in e for e in events):
            entered.set()
            gate.wait(30)
            if ZERO:
                raise ConnectionError("down")
            delivered[0] += len(events)
            return
        key = events[0]["seq"]
        if key not in batches:
            batches.append(key)
        sizes.append(len(events))
        if batches.index(key) + 1 in FAIL:
            raise ConnectionError("transient, refused at once")
        if len(events) > LIMIT:
            raise ValueError("413 too large, refused at once")
        time.sleep(MS * len(events))
        delivered[0] += len(events)

    def close(self):
        pass


w = Worker(Sink(), batch_size=10, flush_interval=1.0, max_queue=100_000)
w.submit([{"first": 1}, {"a": 2}])
time.sleep(0.1)
for i in range(SPANS):
    w.submit([{"seq": i}, {"a": 2}])
buf = io.StringIO()
old = sys.stderr
sys.stderr = buf
entered.wait(5)
threading.Timer(0.2, gate.set).start()
w.stop(60)
sys.stderr = old
print(
    f"limit={LIMIT} {MS * 1000:g}ms/event fail={sys.argv[3]} {'zero' if ZERO else 'measured'}: delivered={delivered[0]} of "
    f"{2 * (SPANS + 1)} attempts={len(sizes)} sizes={sizes[:10]}{'...' if len(sizes) > 10 else ''} failed_batches={w.failed_batches}"
)
