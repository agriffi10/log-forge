"""SPEC-090, written on 2026-11-15 against 3e27a0a6 and not one of the audit's own probes: a backlog into a per-event sink.
The sink sleeps the first argument times the batch's length, the shape of SQS at ten entries a request or a JetStream
NATSSink at one ack per event. A bigger batch only makes its call longer, so SPEC-090's size target settles near half a
flush interval of the sink's own work. It prints the batch sizes and the longest emit.
Usage: python spec090_per_event_sink.py <seconds per event> <spans>
"""

import sys, time, collections
import log_foundry as lf

PER = float(sys.argv[1])
SPANS = int(sys.argv[2])
sizes = collections.Counter()
longest = [0.0]
got = [0]


class PerEvent:
    def emit(self, batch):
        t = time.monotonic()
        time.sleep(PER * len(batch))
        sizes[len(batch)] += 1
        got[0] += len(batch)
        longest[0] = max(longest[0], time.monotonic() - t)

    def close(self):
        pass


lf.configure(sink=PerEvent())


@lf.trace
def work():
    pass


t0 = time.monotonic()
for _ in range(SPANS):
    work()
r = lf.flush(600)
print(
    f"per-event={PER}s spans={SPANS}: received={got[0]} of {2 * SPANS} emits={sum(sizes.values())} largest={max(sizes)} "
    f"longest emit={longest[0]:.2f}s drain={time.monotonic() - t0:.1f}s flush={r.ok} sizes={dict(sorted(sizes.items()))}"
)
