"""SPEC-094 probe: what waiting for the delivery report costs `KafkaSink` and `GooglePubSubSink`.

Written on 2026-11-15 while authoring SPEC-094, against 33582484; not one of the audit's probes.
A fake client acknowledges each message a set time after it was handed over, on a thread of its
own, as a broker's round trip would. Each case emits batches of a given size through the sink's
`emit` and prints the median wall time of one `emit` and the events per second that implies; a
dead client never acknowledges, and its row shows that one `emit` is bounded by the sink's
timeout. Revised on 2026-11-16: the fake's `poll` blocks until it serves a report or its timeout
passes, as librdkafka's does, because SPEC-094's round now waits by polling. Run it against two
trees and compare:

    PYTHONPATH=<tree>/src python -B spec094_report_wait_cost.py
"""

from __future__ import annotations

import collections
import statistics
import sys
import threading
import time
from typing import Any

import log_foundry
from log_foundry.sinks.kafka import KafkaSink
from log_foundry.sinks.pubsub import GooglePubSubSink


class Producer:
    """A confluent-kafka-shaped producer: reports run in `poll` and `flush` once acknowledged."""

    def __init__(self, latency: float | None) -> None:
        self.latency = latency
        self.queue: collections.deque[tuple[float, Any]] = collections.deque()
        self.lock = threading.Lock()

    def produce(self, topic: str, value: Any = None, key: Any = None, callback: Any = None) -> None:
        with self.lock:
            self.queue.append((time.monotonic(), callback))

    def _serve(self) -> int:
        now = time.monotonic()
        ready = []
        with self.lock:
            while (
                self.queue and self.latency is not None and now - self.queue[0][0] >= self.latency
            ):
                ready.append(self.queue.popleft())
        for _, callback in ready:
            if callback is not None:
                callback(None, None)
        return len(ready)

    def poll(self, timeout: float = 0) -> int:
        """Blocks until it serves a report or the timeout passes, as librdkafka's poll does."""
        deadline = time.monotonic() + timeout
        while True:
            served = self._serve()
            if served or time.monotonic() >= deadline:
                return served
            time.sleep(0.0002)

    def flush(self, timeout: float | None = None) -> int:
        deadline = time.monotonic() + (timeout if timeout is not None else 3600)
        while True:
            self._serve()
            with self.lock:
                left = len(self.queue)
            if not left or time.monotonic() >= deadline:
                return left
            time.sleep(0.0002)

    def __len__(self) -> int:
        return len(self.queue)


class Future:
    """A Pub/Sub future that resolves `latency` seconds after its publish, or never."""

    def __init__(self, latency: float | None) -> None:
        self.at = None if latency is None else time.monotonic() + latency

    def done(self) -> bool:
        return self.at is not None and time.monotonic() >= self.at

    def result(self, timeout: float | None = None) -> str:
        if self.at is None:
            if timeout is not None:
                time.sleep(timeout)
            raise TimeoutError
        wait = self.at - time.monotonic()
        if wait > 0:
            if timeout is not None and wait > timeout:
                time.sleep(timeout)
                raise TimeoutError
            time.sleep(wait)
        return "id"


class Publisher:
    def __init__(self, latency: float | None) -> None:
        self.latency = latency

    def publish(self, topic: str, data: bytes | None = None, **attrs: Any) -> Future:
        return Future(self.latency)


def batch(n: int) -> list[dict[str, object]]:
    return [{"message": "m", "level": "INFO", "i": i} for i in range(n)]


def measure(make: Any, size: int, repeats: int) -> float:
    sink = make()
    times = []
    for _ in range(repeats):
        events = batch(size)
        started = time.perf_counter()
        sink.emit(events)
        times.append(time.perf_counter() - started)
    return statistics.median(times)


def main() -> int:
    print("log_foundry", log_foundry.__file__, "python", sys.version.split()[0])
    sinks = {
        "KafkaSink": lambda latency, bound: KafkaSink(
            "t", producer=Producer(latency), flush_timeout=bound
        ),
        "GooglePubSubSink": lambda latency, bound: GooglePubSubSink(
            "projects/p/topics/t", client=Publisher(latency), overflow_timeout=bound
        ),
    }
    for name, make in sinks.items():
        for latency in (0.001, 0.005, 0.020):
            for size in (10, 100, 1000):
                took = measure(lambda: make(latency, 10.0), size, 7)
                print(
                    f"{name:<17} ack {latency * 1000:>4.0f} ms  batch {size:>4}  "
                    f"emit {took * 1000:8.2f} ms  {size / took:>10.0f} events/s"
                )
        took = measure(lambda: make(None, 0.5), 10, 3)
        print(f"{name:<17} no ack ever, timeout 0.5 s  batch   10  emit {took * 1000:8.2f} ms")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
