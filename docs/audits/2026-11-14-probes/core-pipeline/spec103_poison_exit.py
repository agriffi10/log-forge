"""SPEC-103, written on 2026-11-17: one span's event is refused for its content on every attempt, a 400-style poison, at exit.
Every other batch is accepted. The first steady batch waits on a gate while the backlog queues and `Worker.stop(30)` is
called, so the poison span, the 101st of the backlog, lands in an exit batch. It prints the events delivered of those accepted.
A closure that declined every exit batch after the first abandoned one would lose every deliverable event behind the poison,
under a line naming the shutdown deadline as the reason; today only the batch holding the poison is lost.
The stop is claimed only once the gated first batch is inside the sink, so that batch is never an exit batch
(revised 2026-11-17: before, a stop claimed first made it one, and 3 of 18 runs differed).
Usage: python spec103_poison_exit.py <spans>
"""

import io, sys, threading, time
from log_foundry.worker import Worker

SPANS = int(sys.argv[1])
gate = threading.Event()
entered = threading.Event()
calls = [0]
delivered = [0]


class Sink:
    def emit(self, events):
        calls[0] += 1
        if calls[0] == 1:
            entered.set()
            gate.wait(30)
        if any("poison" in e for e in events):
            raise ValueError("400 bad request: one malformed event")
        delivered[0] += len(events)

    def close(self):
        pass


w = Worker(Sink(), batch_size=10, flush_interval=1.0, max_queue=100_000)
w.submit([{"a": 1}, {"a": 2}])
time.sleep(0.1)
for i in range(SPANS):
    w.submit([{"poison": 1}, {"a": 2}] if i == 100 else [{"a": 1}, {"a": 2}])
buf = io.StringIO()
old = sys.stderr
sys.stderr = buf
entered.wait(5)
threading.Timer(0.2, gate.set).start()
w.stop(30)
sys.stderr = old
print(f"poison spans={SPANS}: emits={calls[0]} delivered={delivered[0]} of {2 * (SPANS + 1)} failed_batches={w.failed_batches}")
for l in buf.getvalue().splitlines()[:4]:
    print("  ", l[:170])
