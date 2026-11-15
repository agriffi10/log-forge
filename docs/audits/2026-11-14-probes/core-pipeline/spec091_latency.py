"""SPEC-091: how long an open span's event waits before the sink has it, with no flush() called.

One root span, with no child span and so no worker built by a close, logs one event at a time at
staggered offsets and blocks on the sink's receipt of that event, the way a handler blocks on a
slow downstream call. Prints, per sample, the seconds from the level call to the sink's ``emit``,
and the maximum. With ``busy``, another thread closes a traced call every 2 ms, so the count
trigger keeps the worker emitting and its time trigger never fires. On a tree that does not
sweep, every sample times out at the cap.
Run: PYTHONPATH=<src> python spec091_latency.py [samples] [busy]
"""

import sys
import threading
import time

import log_foundry as lf

CAP = 5.0


class Signal:
    def __init__(self) -> None:
        self.got: dict[object, float] = {}
        self.cond = threading.Condition()

    def emit(self, batch: list[dict[str, object]]) -> None:
        now = time.monotonic()
        with self.cond:
            for e in batch:
                self.got[(e.get("fields") or {}).get("n")] = now  # type: ignore[union-attr]
            self.cond.notify_all()

    def close(self) -> None:
        pass


def main(samples: int, busy: bool) -> None:
    sink = Signal()
    lf.configure(sink=sink)
    waits: list[float] = []
    stop = threading.Event()

    @lf.trace
    def other() -> None:
        lf.info("other")

    def churn() -> None:
        while not stop.is_set():
            other()
            time.sleep(0.002)

    churner = threading.Thread(target=churn)
    if busy:
        churner.start()

    @lf.trace
    def handler() -> None:
        for n in range(samples):
            time.sleep(0.137 * n % 0.9)
            t0 = time.monotonic()
            lf.info("in-span", n=n)
            with sink.cond:
                sink.cond.wait_for(lambda n=n: n in sink.got, timeout=CAP)
            waits.append(sink.got.get(n, t0 + CAP) - t0)

    handler()
    stop.set()
    if busy:
        churner.join()
    print(" ".join(f"{w:.3f}" for w in waits))
    print(f"busy={busy} samples={len(waits)} max={max(waits):.3f}s cap={CAP}s")
    lf.shutdown(timeout=5.0)


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 8, "busy" in sys.argv[2:])
