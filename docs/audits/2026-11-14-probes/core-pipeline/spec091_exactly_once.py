"""SPEC-091: every event of an open span is delivered exactly once while the worker sweeps it.

Eight threads each run traced calls that nest a child span, log inside both, set baggage after
the first log, and close, while the worker's interval is cut to 1 ms so the drain thread sweeps
as often as it can, under a switch interval of 1e-6 s. A ninth thread calls ``flush()`` in a loop, sweeping from outside every span.
Afterwards every event must have arrived exactly once, each span's ``span.start`` and ``span.end``
must carry the span's final baggage, and no ``span.end`` may arrive before its own span's logs.
Prints one line; ``dup``, ``missing``, ``bad_boundary`` and ``end_not_last`` must all be 0, and
``shipped_before_their_close`` counts events that left in an earlier batch than their span's end,
which is 0 on a tree that does not sweep.
With ``swap``, the ninth thread also moves the process between two sinks with ``configure(sink=…)``
after every flush, and the two sinks' events are counted together (batch order is then per sink,
so ``end_not_last`` and ``shipped_before_their_close`` do not apply in that mode).
Run: PYTHONPATH=<src> python spec091_exactly_once.py [swap], once per process (shutdown is terminal).
"""

import collections
import sys
import threading

import log_foundry as lf
from log_foundry import _lifecycle

THREADS, CALLS, LOGS = 8, 300, 5


class Collect:
    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []
        self.batch_of: dict[object, int] = {}
        self.batches = 0
        self.lock = threading.Lock()

    def emit(self, batch: list[dict[str, object]]) -> None:
        with self.lock:
            self.batches += 1
            for e in batch:
                self.batch_of.setdefault(e["log_id"], self.batches)
            self.events.extend(batch)

    def close(self) -> None:
        pass


def run(swap: bool) -> str:
    sink = Collect()
    other = Collect()
    lf.configure(sink=sink)

    @lf.trace
    def child(t: int, c: int) -> None:
        for i in range(LOGS):
            lf.info("child-log", t=t, c=c, i=i)

    @lf.trace
    def parent(t: int, c: int) -> None:
        lf.info("parent-log", t=t, c=c, i=-1)
        lf.set_baggage(req=f"{t}-{c}")
        for i in range(LOGS):
            lf.info("parent-log", t=t, c=c, i=i)
        child(t, c)

    parent(-1, -1)
    worker = _lifecycle._state.worker_exists()
    assert worker is not None
    worker.flush_interval = 0.001
    stop = threading.Event()

    def flusher() -> None:
        turn = 0
        while not stop.is_set():
            lf.flush(timeout=5.0)
            if swap:
                turn += 1
                lf.configure(sink=other if turn % 2 else sink)

    def body(t: int) -> None:
        for c in range(CALLS):
            parent(t, c)

    f = threading.Thread(target=flusher)
    f.start()
    ts = [threading.Thread(target=body, args=(t,)) for t in range(THREADS)]
    for th in ts:
        th.start()
    for th in ts:
        th.join()
    stop.set()
    f.join()
    lf.flush(timeout=10.0)
    lf.shutdown(timeout=10.0)
    for e in other.events:
        sink.batch_of.setdefault(e["log_id"], other.batch_of[e["log_id"]])
    sink.events.extend(other.events)

    seen = collections.Counter(e["log_id"] for e in sink.events)
    dup = sum(n - 1 for n in seen.values() if n > 1)
    calls = THREADS * CALLS + 1
    expected = calls * (2 + 2 + 1 + 2 * LOGS)
    by_span: dict[object, list[dict[str, object]]] = collections.defaultdict(list)
    for e in sink.events:
        by_span[e["span_id"]].append(e)
    bad_boundary = 0
    end_not_last = 0
    early = 0
    for span_events in by_span.values():
        for e in span_events:
            fields = e.get("fields")
            if e["message"] in ("span.start", "span.end") and (
                not isinstance(fields, dict) or "req" not in fields
            ):
                bad_boundary += 1
        ends = [sink.batch_of[e["log_id"]] for e in span_events if e["message"] == "span.end"]
        early += sum(1 for e in span_events if ends and sink.batch_of[e["log_id"]] < ends[0])
        msgs = [e["message"] for e in span_events]
        if "span.end" in msgs and msgs.index("span.end") != len(msgs) - 1:
            end_not_last += 1
    return (
        f"delivered={len(sink.events)} expected={expected} dup={dup} "
        f"missing={expected - len(seen)} bad_boundary={bad_boundary} "
        f"end_not_last={end_not_last} shipped_before_their_close={early}"
    )


if __name__ == "__main__":
    sys.setswitchinterval(1e-6)
    print(run("swap" in sys.argv[1:]), flush=True)
