"""SPEC-094 probe: the first slice under a set stop, the pause between asks, a resolved TimeoutError.

Written on 2026-11-16 while revising SPEC-094 after the execution-frame review, against 97a03f45;
not one of the audit's probes. Each case drives one `deliver` of `KafkaSink` or `GooglePubSubSink`
with a stand-in client and prints its wall time, its CPU time where the case is about spinning,
how often it asked the client, and its outcomes:

- under a stop signal set before the round, reports that arrive within the first 50 ms, which a
  first slice measured from the wait's start takes and a first slice of one pass does not;
- a client that answers at once and never reports, with the stop unset and set, where the pause
  between asks is what keeps CPU time far below wall time;
- a Pub/Sub future already resolved with a `TimeoutError`, which reads `UNKNOWN` at once rather
  than after the whole timeout.

    PYTHONPATH=<tree>/src python -B spec094_first_slice_and_pause.py [label]
"""

import contextlib
import io
import resource
import sys
import threading
import time

from log_foundry.sinks.kafka import KafkaSink
from log_foundry.sinks.pubsub import GooglePubSubSink


def cpu():
    r = resource.getrusage(resource.RUSAGE_SELF)
    return r.ru_utime + r.ru_stime


class Partitioned:
    """Message i on partition i % 4; partition p's reports due at 10 + 8p ms; flush serves one
    partition's reports and returns, as a flush that is woken by one served op might."""

    def __init__(self):
        self.cbs = []
        self.t0 = None

    def produce(self, topic, value=None, key=None, callback=None):
        if self.t0 is None:
            self.t0 = time.monotonic()
        self.cbs.append((len(self.cbs) % 4, callback))

    def poll(self, timeout=None):
        return 0

    def flush(self, timeout=None):
        end = time.monotonic() + (timeout or 0)
        while True:
            now = time.monotonic() - self.t0
            due = [p for p in range(4) if now >= 0.010 + 0.008 * p]
            ready = [(p, cb) for p, cb in self.cbs if p in due]
            if ready:
                first = ready[0][0]
                served = [(p, cb) for p, cb in ready if p == first]
                self.cbs = [x for x in self.cbs if x not in served]
                for _, cb in served:
                    cb(None, None)
                return len(self.cbs)
            if time.monotonic() >= end:
                return len(self.cbs)
            time.sleep(0.001)


class Immediate:
    """flush returns at once, never serving, with messages still queued."""

    def __init__(self):
        self.calls = 0

    def produce(self, topic, value=None, key=None, callback=None):
        pass

    def poll(self, timeout=None):
        return 0

    def flush(self, timeout=None):
        self.calls += 1
        return 1


class Fut:
    """A future whose result() raises TimeoutError at once, never waiting."""

    def __init__(self):
        self.calls = 0

    def result(self, timeout=None):
        self.calls += 1
        raise TimeoutError

    def done(self):
        return False


class FastClient:
    def __init__(self):
        self.futures = []

    def publish(self, topic, data=None, **kw):
        f = Fut()
        self.futures.append(f)
        return f


class Staggered:
    """Resolves future i at 10 + 0.02 i ms, all within 30 ms."""

    def __init__(self):
        from concurrent.futures import Future

        self.Future = Future
        self.futures = []

    def publish(self, topic, data=None, **kw):
        f = self.Future()
        threading.Timer(0.010 + 0.00002 * len(self.futures), f.set_result, args=("id",)).start()
        self.futures.append(f)
        return f


def stopped(sink):
    sink.log_foundry_stop_signal = threading.Event()
    sink.log_foundry_stop_signal.set()
    return sink


def unknown(v):
    return sum(o.name == "UNKNOWN" for o in v.outcomes)


with contextlib.redirect_stderr(io.StringIO()):
    s = stopped(KafkaSink("t", producer=Partitioned(), flush_timeout=5))
    t0 = time.monotonic()
    v = s.deliver([{"m": i} for i in range(40)])
    out = [
        (
            f"kafka stop set, reports by 34 ms: wall {1000 * (time.monotonic() - t0):.1f} ms, "
            f"UNKNOWN {unknown(v)} of 40"
        )
    ]
    for stop in (False, True):
        p = Immediate()
        s = KafkaSink("t", producer=p, flush_timeout=0.5)
        if stop:
            stopped(s)
        c0, t0 = cpu(), time.monotonic()
        v = s.deliver([{"m": 1}])
        out.append(
            f"kafka flush returns at once, stop={stop}: wall {time.monotonic() - t0:.3f} s, "
            f"cpu {cpu() - c0:.3f} s, flush calls {p.calls}, {[o.name for o in v.outcomes]}"
        )
    c = Staggered()
    s = stopped(GooglePubSubSink(client=c, topic="projects/p/topics/t", overflow_timeout=5))
    t0 = time.monotonic()
    v = s.deliver([{"m": i} for i in range(1000)])
    out.append(
        f"pubsub stop set, futures by 30 ms: wall {1000 * (time.monotonic() - t0):.1f} ms, "
        f"UNKNOWN {unknown(v)} of 1000"
    )
    for stop in (False, True):
        c = FastClient()
        s = GooglePubSubSink(client=c, topic="projects/p/topics/t", overflow_timeout=0.5)
        if stop:
            stopped(s)
        c0, t0 = cpu(), time.monotonic()
        v = s.deliver([{"m": 1}])
        out.append(
            f"pubsub result() raises at once, stop={stop}: wall {time.monotonic() - t0:.3f} s, "
            f"cpu {cpu() - c0:.3f} s, result calls {c.futures[0].calls}, "
            f"{[o.name for o in v.outcomes]}"
        )
    from concurrent.futures import Future

    class OneDone:
        def publish(self, topic, data=None, **kw):
            f = Future()
            f.set_exception(TimeoutError("resolved with a timeout"))
            return f

    s = GooglePubSubSink(client=OneDone(), topic="projects/p/topics/t", overflow_timeout=5)
    t0 = time.monotonic()
    v = s.deliver([{"m": 1}])
    out.append(
        f"pubsub future done with TimeoutError, overflow_timeout 5: wall "
        f"{time.monotonic() - t0:.3f} s, {[o.name for o in v.outcomes]}"
    )
print(sys.argv[1] if len(sys.argv) > 1 else "")
print("\n".join(out))
