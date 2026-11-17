"""SPEC-091: what registering an open span costs the traced call, measured in isolation and whole.

``isolated`` times the three statements the prototype adds to a traced call: the registry write
at open, the read of the process worker at open, and the registry pop at close, each over 2M
iterations, best of 7, against an empty loop. ``whole`` times a traced call with one in-span log
into a sink that does nothing, 20,000 calls, best of 7; run it on both trees and compare, and expect
the difference to be within the noise of two runs of one tree. ``isolated`` also times the
snapshot the close's backfill takes of a three-event buffer. ``hold`` (prototype only, see
``spec091_prototype.py``) times one sweep of 10, 100 and 1,000 registered spans holding ten
events each past their ``span.start``, each handed into a plain ``queue.Queue`` as the worker's
``put_nowait`` does: how long the sweep holds the lock a closing span waits on.
``close`` (added by the build, 2026-11-17) times what the re-entry guard puts around the close's
swap: a bare ``Lock`` taken and released, an ``RLock``, the check ``_sweep_held_here()`` plus an
``RLock``, and the check plus ``with _sweep_hold:``, each over 2M iterations, best of 7, net of an
empty loop. The last is what ``decorator._flush`` now does.
Run: PYTHONPATH=<src> python spec091_hot_path.py isolated|whole|hold|close
"""

import queue
import sys
import time
import timeit

import log_foundry as lf
from log_foundry import _lifecycle
from log_foundry.model import Span


class Null:
    def emit(self, batch: list[dict[str, object]]) -> None:
        pass

    def close(self) -> None:
        pass


def isolated() -> None:
    registry: dict[int, Span] = {}
    span = Span(trace_id="0" * 32, span_id="0" * 16, parent_span_id=None, name="x", start_ts=0.0)
    state = _lifecycle._state
    n = 2_000_000
    env = {"registry": registry, "span": span, "state": state}
    base = min(timeit.repeat("pass", number=n, repeat=7, globals=env)) / n
    put = min(timeit.repeat("registry[id(span)] = span", number=n, repeat=7, globals=env)) / n
    read = min(timeit.repeat("state._worker is None", number=n, repeat=7, globals=env)) / n
    events = [{} for _ in range(3)]
    env["events"] = events
    walk = min(timeit.repeat("for e in events: pass", number=n, repeat=7, globals=env)) / n
    snap = min(timeit.repeat("for e in list(events): pass", number=n, repeat=7, globals=env)) / n
    both = (
        min(
            timeit.repeat(
                "registry[id(span)] = span; registry.pop(id(span), None)",
                number=n,
                repeat=7,
                globals=env,
            )
        )
        / n
    )
    print(
        f"register={1e9 * (put - base):.1f}ns worker_read={1e9 * (read - base):.1f}ns "
        f"register+pop={1e9 * (both - base):.1f}ns backfill_snapshot_3={1e9 * (snap - walk):.1f}ns"
    )


def hold() -> None:
    from log_foundry import decorator

    for spans in (10, 100, 1000):
        best = float("inf")
        for _ in range(7):
            decorator._open_spans.clear()
            for i in range(spans):
                span = Span(
                    trace_id="0" * 32,
                    span_id=f"{i:016x}",
                    parent_span_id=None,
                    name="x",
                    start_ts=0.0,
                )
                span.events = [{"message": "span.start"}] + [{"i": j} for j in range(10)]
                span.held = 1
                decorator._open_spans[id(span)] = span
            sink_queue: queue.Queue[object] = queue.Queue()

            def hand_in(batch: list[dict[str, object]]) -> int:
                sink_queue.put_nowait(batch)
                return 0

            t0 = time.perf_counter()
            decorator._sweep_registered(hand_in)
            best = min(best, time.perf_counter() - t0)
        decorator._open_spans.clear()
        print(f"open_spans={spans} events_each=10 sweep_holds_lock={1e6 * best:.1f}us")


def whole() -> None:
    lf.configure(sink=Null())

    @lf.trace
    def call() -> None:
        lf.info("x")

    call()
    worker = _lifecycle._state.worker_exists()
    assert worker is not None
    calls = 20_000
    best = float("inf")
    for _ in range(7):
        t0 = time.perf_counter()
        for _ in range(calls):
            call()
        best = min(best, (time.perf_counter() - t0) / calls)
        lf.flush(timeout=30.0)
    print(f"traced_call_with_one_log={1e6 * best:.2f}us")


def close() -> None:
    import threading

    from log_foundry import decorator

    n = 2_000_000
    names = {
        "lock": threading.Lock(),
        "rlock": threading.RLock(),
        "held_here": decorator._sweep_held_here,
        "hold": decorator._sweep_hold,
    }
    statements = {
        "bare_lock": "with lock:\n    pass",
        "rlock": "with rlock:\n    pass",
        "check_and_rlock": "held_here()\nwith rlock:\n    pass",
        "check_and_hold": "held_here()\nwith hold:\n    pass",
    }
    base = min(timeit.repeat("pass", number=n, repeat=7)) / n
    parts = []
    for name, statement in statements.items():
        best = min(timeit.repeat(statement, globals=names, number=n, repeat=7)) / n
        parts.append(f"{name}={1e9 * (best - base):.1f}ns")
    print(" ".join(parts))


if __name__ == "__main__":
    {"isolated": isolated, "whole": whole, "hold": hold, "close": close}[sys.argv[1]]()
