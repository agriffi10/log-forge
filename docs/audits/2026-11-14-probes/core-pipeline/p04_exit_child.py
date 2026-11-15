"""P4 child: fill the queue behind a gated sink, open the gate, and exit at once (atexit drains)."""
import os, sys, threading, time
import log_foundry as lf

OUT = sys.argv[1]
PER_CHUNK_S = float(sys.argv[2])      # latency of one 10-event send, the SQS SendMessageBatch shape
SPANS = int(sys.argv[3])

class SlowSink:
    def __init__(self):
        self.gate = threading.Event()
        self.sent = 0
        self.calls = 0
        self.biggest = 0
    def emit(self, batch):
        self.gate.wait()
        self.calls += 1
        self.biggest = max(self.biggest, len(batch))
        for i in range(0, len(batch), 10):
            time.sleep(PER_CHUNK_S)
            self.sent += len(batch[i:i + 10])
            with open(OUT, "w") as f:
                f.write(f"{self.sent} {self.calls} {self.biggest}")
    def close(self):
        with open(OUT + ".closed", "w") as f:
            f.write("closed")

sink = SlowSink()
lf.configure(sink=sink)

@lf.trace
def work():
    pass

for _ in range(SPANS):
    work()
h = lf.health()
print(f"child: accepted spans={SPANS} events={2*SPANS} queued={h.queued} dropped={h.dropped}", flush=True)
sink.gate.set()
sys.exit(0)
