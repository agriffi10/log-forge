"""SPEC-091: a worker sweep cannot split a root span across two trace ids during continue_trace().

A root span logs three events, then calls ``continue_trace()`` with a pause of 50 ms injected at
the start of ``_reparent_current_span``, while the worker's interval is cut to 1 ms so the drain
thread sweeps inside that pause if nothing stops it. Prints, over 20 trials, how many spans
reached the sink carrying more than one ``trace_id`` and how many adoptions were refused. A split
span is the defect; a refusal is the guard working.
Run: PYTHONPATH=<src> python spec091_continue_trace.py
"""

import collections
import threading
import time

import log_foundry as lf
from log_foundry import _lifecycle, decorator

TRIALS = 20
INBOUND = "00-" + "ab" * 16 + "-" + "cd" * 8 + "-01"


class Collect:
    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []
        self.lock = threading.Lock()

    def emit(self, batch: list[dict[str, object]]) -> None:
        with self.lock:
            self.events.extend(batch)

    def close(self) -> None:
        pass


def main() -> None:
    sink = Collect()
    lf.configure(sink=sink)
    original = decorator._reparent_current_span

    def paused(trace_id: str, parent_span_id: str | None) -> None:
        time.sleep(0.05)
        original(trace_id, parent_span_id)

    decorator._reparent_current_span = paused  # type: ignore[assignment]
    refused = 0

    @lf.trace
    def handler() -> None:
        nonlocal refused
        for i in range(3):
            lf.info("before", i=i)
        if not lf.continue_trace(INBOUND):
            refused += 1
        lf.info("after")

    handler()
    worker = _lifecycle._state.worker_exists()
    assert worker is not None
    worker.flush_interval = 0.001
    for _ in range(TRIALS):
        handler()
    lf.flush(timeout=5.0)
    traces: dict[object, set[object]] = collections.defaultdict(set)
    for e in sink.events:
        traces[e["span_id"]].add(e["trace_id"])
    split = sum(1 for ids in traces.values() if len(ids) > 1)
    print(f"trials={TRIALS} split_spans={split} refused={refused}")


if __name__ == "__main__":
    main()
