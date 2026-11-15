"""SPEC-088: what the event ledger costs, measured in isolation, by the spec's Evidence method.

``isolated`` times one statement in a loop of 100,000, the minimum of 200 runs with the candidates
interleaved, on the thread's CPU clock, with the empty loop subtracted: a bare ``+=`` on a global,
one credit through ``_ledger.cell()``, the counting ``_put`` over a plain one (a ``put_nowait`` and
``get_nowait`` pair against the same pair on a plain ``queue.Queue``, so nothing grows inside the
timed loop), and ``with lock:`` around an increment. It prints a traced call's ledger writes as two
credits, one hand-over and one counting put. ``snapshot`` times ``_lifecycle._ledger_snapshot()``
with 1, 9 and 50 live cells, each the minimum of 300 runs of 1,000 calls, per call.
Run: PYTHONPATH=<src> python spec088_cost.py isolated|snapshot
"""

import queue
import sys
import threading
import time

from log_foundry import _ledger, _lifecycle
from log_foundry.worker import _CountingQueue

LOOP = 100_000
RUNS = 200
COUNTER = 0
LOCK = threading.Lock()


def empty() -> None:
    for _ in range(LOOP):
        pass


def bare() -> None:
    global COUNTER
    for _ in range(LOOP):
        COUNTER += 1


def credit() -> None:
    cell = _ledger.cell
    for _ in range(LOOP):
        cell().span += 1


def locked() -> None:
    global COUNTER
    for _ in range(LOOP):
        with LOCK:
            COUNTER += 1


def pair(q) -> None:
    put, get, item = q.put_nowait, q.get_nowait, [1, 2, 3]
    for _ in range(LOOP):
        put(item)
        get()


def counting() -> None:
    pair(_CountingQueue(maxsize=10))


def plain() -> None:
    pair(queue.Queue(maxsize=10))


def timed(fn) -> int:
    start = time.thread_time_ns()
    fn()
    return time.thread_time_ns() - start


def isolated() -> None:
    _ledger.cell()
    candidates = {"empty": empty, "bare": bare, "credit": credit, "locked": locked,
                  "counting": counting, "plain": plain}
    best = {name: float("inf") for name in candidates}
    for _ in range(RUNS):
        for name, fn in candidates.items():
            best[name] = min(best[name], timed(fn))
    per = {name: value / LOOP for name, value in best.items()}
    rows = {
        "bare += on a global": per["bare"] - per["empty"],
        "one credit through _ledger.cell()": per["credit"] - per["empty"],
        "the counting _put, over a plain one": per["counting"] - per["plain"],
        "with lock: around the increment": per["locked"] - per["empty"],
    }
    rows["a traced call's writes (two credits, one hand-over, one counting put)"] = (
        3 * rows["one credit through _ledger.cell()"] + rows["the counting _put, over a plain one"]
    )
    print(sys.version.split()[0])
    for name, value in rows.items():
        print(f"{name}: {value:.1f} ns")


def snapshot() -> None:
    print(sys.version.split()[0])
    for live in (1, 9, 50):
        _ledger.restart()
        _ledger.cell().span += 1
        release = threading.Event()
        ready = threading.Barrier(live)

        def hold() -> None:
            _ledger.cell().span += 1
            ready.wait()
            release.wait()

        threads = [threading.Thread(target=hold) for _ in range(live - 1)]
        for thread in threads:
            thread.start()
        if live > 1:
            ready.wait()
        cells = len(_ledger.snapshot()[1])
        best = float("inf")
        for _ in range(300):
            start = time.thread_time_ns()
            for _ in range(1_000):
                _lifecycle._ledger_snapshot()
            best = min(best, (time.thread_time_ns() - start) / 1_000)
        release.set()
        for thread in threads:
            thread.join()
        print(f"one snapshot, {live} live cell(s) ({cells} registered): {best / 1000:.2f} us")


{"isolated": isolated, "snapshot": snapshot}[sys.argv[1]]()
