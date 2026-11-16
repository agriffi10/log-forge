"""SPEC-097 and SPEC-098, written while authoring them against ead40798: what the prototype costs.

Run once per tree with ``PYTHONPATH=<tree>/src`` and compare the two outputs. ``calls`` times, on
the thread's CPU clock, an orphan ``info()`` into a sink that does nothing, and a traced call that
logs once inside, each as the minimum over 60 runs of 2,000 calls, per call. ``locks`` times
``with lock:`` around nothing for a ``threading.Lock`` and a ``threading.RLock``, the minimum over
200 runs of 100,000, per acquisition, with the empty loop subtracted. ``commands`` times on the
wall clock, median of 21, a ``configure(sink=...)`` that swaps a live worker and a ``shutdown()``
of a process with one worker and nothing queued, each in a fresh child.
``wrapper`` times, against a tree carrying the prototype, the work the prototype adds to each
orphan log and to each traced call's open and close: one call of a function that returns at once
wrapped in ``try``/``finally`` around ``_lifecycle._run_waiting()`` with nothing waiting, against
the same function called bare, the minimum over 200 runs of 100,000, per call.
``orphan-note`` (added after SPEC-098's spec review) times ``_lifecycle._note_orphan_emit`` on a
sink already armed and handed, the orphan log's per-call lifecycle step, which SPEC-098's deletion
reduces to an owed check and a lock-free table read: the minimum over 200 runs of 100,000, per call,
with the empty loop subtracted.
Run: PYTHONPATH=<src> python spec097_cost.py calls|locks|commands|wrapper|orphan-note
"""

import statistics
import subprocess
import sys
import threading
import time

MODE = sys.argv[1]


class Quiet:
    log_foundry_stop_signal = None

    def emit(self, batch: list[dict]) -> None:
        pass

    def close(self) -> None:
        pass


def per_call(fn, n: int, runs: int) -> float:  # type: ignore[no-untyped-def]
    best = float("inf")
    for _ in range(runs):
        started = time.thread_time()
        for _ in range(n):
            fn()
        best = min(best, (time.thread_time() - started) / n)
    return best


if MODE == "calls":
    import log_foundry

    log_foundry.configure(service="cost", sink=Quiet())

    @log_foundry.trace
    def traced() -> None:
        log_foundry.info("in")

    log_foundry.info("warm")
    traced()
    log_foundry.flush(timeout=5)
    orphan = per_call(lambda: log_foundry.info("x"), 2000, 60)
    span = per_call(traced, 2000, 60)
    log_foundry.flush(timeout=30)
    print(f"orphan info(): {orphan * 1e6:.2f} us per call")
    print(f"traced call with one info(): {span * 1e6:.2f} us per call")
elif MODE == "locks":
    for name, lock in (("Lock", threading.Lock()), ("RLock", threading.RLock())):

        def held() -> None:
            for _ in range(100_000):
                with lock:
                    pass

        def empty() -> None:
            for _ in range(100_000):
                pass

        best = min(per_call(held, 1, 1) for _ in range(200)) - min(
            per_call(empty, 1, 1) for _ in range(200)
        )
        print(f"with {name}: {best / 100_000 * 1e9:.1f} ns per acquisition")
elif MODE == "wrapper":
    from log_foundry import _lifecycle

    def body() -> None:
        pass

    def wrapped() -> None:
        try:
            return body()
        finally:
            _lifecycle._run_waiting()

    def bare_loop() -> None:
        for _ in range(100_000):
            body()

    def wrapped_loop() -> None:
        for _ in range(100_000):
            wrapped()

    bare = min(per_call(bare_loop, 1, 1) for _ in range(200))
    added = min(per_call(wrapped_loop, 1, 1) for _ in range(200))
    print(f"added per wrapped call: {(added - bare) / 100_000 * 1e9:.1f} ns")
elif MODE == "orphan-note":
    import log_foundry
    from log_foundry import _lifecycle

    sink = Quiet()
    log_foundry.configure(service="cost", sink=sink)
    log_foundry.info("arm and hand")
    note = _lifecycle._note_orphan_emit

    def noted() -> None:
        for _ in range(100_000):
            note(sink)

    def empty_loop() -> None:
        for _ in range(100_000):
            pass

    best = min(per_call(noted, 1, 1) for _ in range(200))
    floor = min(per_call(empty_loop, 1, 1) for _ in range(200))
    print(f"_note_orphan_emit on an armed, handed sink: {(best - floor) / 100_000 * 1e9:.1f} ns")
elif MODE == "commands":
    child = """
import sys, time, log_foundry
class Quiet:
    log_foundry_stop_signal = None
    def emit(self, batch): pass
    def close(self): pass
log_foundry.configure(service="cost", sink=Quiet())
@log_foundry.trace
def traced(): pass
traced(); log_foundry.flush(timeout=5)
started = time.monotonic(); log_foundry.configure(sink=Quiet()); swap = time.monotonic() - started
started = time.monotonic(); log_foundry.shutdown(timeout=5); stop = time.monotonic() - started
print(swap, stop)
"""
    swaps, stops = [], []
    for _ in range(21):
        out = subprocess.run(
            [sys.executable, "-B", "-c", child], capture_output=True, text=True, timeout=60
        )
        swap, stop = (float(x) for x in out.stdout.split())
        swaps.append(swap)
        stops.append(stop)
    print(f"configure(sink=...) swap: median {statistics.median(swaps) * 1e3:.2f} ms")
    print(f"shutdown(): median {statistics.median(stops) * 1e3:.2f} ms")
