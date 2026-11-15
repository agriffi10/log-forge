"""SPEC-093 probe: what publisher confirms cost `RabbitMQSink`, one round trip per message.

Written on 2026-11-15 while authoring SPEC-093, against 33582484; not one of the audit's probes.
pika's `BlockingChannel.basic_publish` in confirm mode waits for each message's `Basic.Ack`
before it returns (`pika/adapters/blocking_connection.py`, `basic_publish`), so the fake channel
here sleeps a set round trip inside each publish once `confirm_delivery()` was called, and
returns at once otherwise. It prints the median wall time of one `emit` per batch size and round
trip. Run it against two trees and compare:

    PYTHONPATH=<tree>/src python -B spec093_rabbitmq_confirm_cost.py
"""

from __future__ import annotations

import statistics
import sys
import time
from typing import Any

import log_foundry
from log_foundry.sinks.rabbitmq import RabbitMQSink


class Channel:
    def __init__(self, rtt: float) -> None:
        self.rtt = rtt
        self.confirming = False

    def confirm_delivery(self) -> None:
        time.sleep(self.rtt)
        self.confirming = True

    def basic_publish(self, **kwargs: Any) -> None:
        if self.confirming:
            time.sleep(self.rtt)

    def close(self) -> None:
        pass


class Connection:
    def __init__(self, rtt: float) -> None:
        self.rtt = rtt
        self.is_open = True

    def channel(self) -> Channel:
        return Channel(self.rtt)

    def close(self) -> None:
        pass


def main() -> int:
    print("log_foundry", log_foundry.__file__, "python", sys.version.split()[0])
    for rtt in (0.0002, 0.001):
        for size in (10, 100, 1000):
            sink = RabbitMQSink(exchange="x", routing_key="k", connection=Connection(rtt))
            times = []
            for _ in range(5):
                events = [{"message": "m", "i": i} for i in range(size)]
                started = time.perf_counter()
                sink.emit(events)
                times.append(time.perf_counter() - started)
            took = statistics.median(times)
            print(
                f"RabbitMQSink  round trip {rtt * 1000:4.1f} ms  batch {size:>4}  "
                f"emit {took * 1000:9.2f} ms  {size / took:>9.0f} events/s"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
