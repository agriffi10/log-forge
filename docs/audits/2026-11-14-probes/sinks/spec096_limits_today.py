"""SPEC-096 probe: how SQSSink and SNSSink apply their byte limit today.

Written on 2026-11-16 while authoring SPEC-096, against 10ba21ee; not one of the audit's probes.
A recording fake client takes every entry; nothing is refused by the "service". It prints, for
each case, the requests made, each request's summed costed size, what was dropped and the lines
written. Run from the repository root:

    PYTHONPATH=src python -B docs/audits/2026-11-14-probes/sinks/spec096_limits_today.py

Cases:
  A  ten 100 KiB events to SQS: is the request's byte sum bounded, and by what?
  B  ten 30 KiB events to SQS: is the sum bounded when ten entries would fit 1 MiB?
  C  one event over 256 KiB beside two small ones: dropped, counted, and the line.
  D  an instance assignment `sink.MAX_BYTES = 2048` and a subclass override: honoured?
  E  a FIFO entry whose body alone fits but body plus ids does not.
  F  the same as A, C and D for SNSSink (F4, the subclass, added on 2026-11-16 at ead40798).
"""

from __future__ import annotations

import contextlib
import io
import json
import threading

from log_foundry.sinks.sns import SNSSink
from log_foundry.sinks.sqs import SQSSink

KIB = 1024


class Recorder:
    """A boto3-shaped SQS and SNS client that takes every entry and records each request."""

    def __init__(self) -> None:
        self.requests: list[list[dict[str, str]]] = []

    def send_message_batch(self, QueueUrl: str, Entries: list[dict[str, str]]) -> dict:
        self.requests.append(Entries)
        return {"Successful": [{"Id": e["Id"]} for e in Entries], "Failed": []}

    def publish_batch(
        self, TopicArn: str, PublishBatchRequestEntries: list[dict[str, str]]
    ) -> dict:
        self.requests.append(PublishBatchRequestEntries)
        return {"Successful": [{"Id": e["Id"]} for e in PublishBatchRequestEntries], "Failed": []}


def costed(entry: dict[str, str]) -> int:
    """The size SQSSink costs an entry: the UTF-8 body plus the FIFO ids, as `_prepare` does."""
    total = 0
    for key in ("MessageBody", "Message", "MessageGroupId", "MessageDeduplicationId"):
        if key in entry:
            total += len(entry[key].encode("utf-8"))
    return total


def event(i: int, pad: int) -> dict[str, object]:
    return {"log_id": f"l{i:04d}", "trace_id": "t" * 32, "n": i, "pad": "x" * pad}


def run(name: str, sink, batch) -> None:
    stop = threading.Event()
    stop.set()
    sink.log_foundry_stop_signal = stop
    err = io.StringIO()
    raised = None
    with contextlib.redirect_stderr(err):
        try:
            sink.emit(batch)
        except Exception as exc:
            raised = type(exc).__name__
    client = sink.client
    sums = [sum(costed(e) for e in req) for req in client.requests]
    counts = [len(req) for req in client.requests]
    held = sum(counts)
    losses = sink.losses()
    print(f"\n== {name}")
    print(f"   requests={len(client.requests)} entries_per_request={counts}")
    print(f"   costed_sum_per_request={sums} max={max(sums) if sums else 0}")
    print(
        f"   held={held} of {len(batch)} dropped_oversized={sink.dropped_oversized} "
        f"losses={losses} raised={raised}"
    )
    for line in [x for x in err.getvalue().splitlines() if x.strip()][:3]:
        print(f"     | {line[:160]}")


def main() -> None:
    print(f"SQSSink.MAX_BYTES={SQSSink.MAX_BYTES} SNSSink.MAX_BYTES={SNSSink.MAX_BYTES}")
    print(f"10 x 256 KiB = {10 * 256 * KIB} bytes; 1 MiB = {1024 * KIB} bytes")

    run(
        "A SQS ten 100 KiB events",
        SQSSink("https://q", client=Recorder()),
        [event(i, 100 * KIB) for i in range(10)],
    )
    run(
        "B SQS ten 30 KiB events",
        SQSSink("https://q", client=Recorder()),
        [event(i, 30 * KIB) for i in range(10)],
    )
    run(
        "C SQS one 300 KiB event beside two small",
        SQSSink("https://q", client=Recorder()),
        [event(0, 10), event(1, 300 * KIB), event(2, 10)],
    )

    sink = SQSSink("https://q", client=Recorder())
    sink.MAX_BYTES = 2048
    run(
        "D1 SQS instance MAX_BYTES=2048, a 3 KiB event beside a small one",
        sink,
        [event(0, 10), event(1, 3 * KIB)],
    )

    class Small(SQSSink):
        MAX_BYTES = 2048

    run(
        "D2 SQS subclass MAX_BYTES=2048, a 3 KiB event beside a small one",
        Small("https://q", client=Recorder()),
        [event(0, 10), event(1, 3 * KIB)],
    )

    body = json.dumps(event(0, 0))
    pad = SQSSink.MAX_BYTES - len(body.encode()) - 10
    big = event(0, pad)
    body_len = len(json.dumps(big).encode())
    print(f"\n   E body alone = {body_len} bytes, {SQSSink.MAX_BYTES - body_len} under MAX_BYTES")
    run(
        "E SQS FIFO body fits alone, body+ids does not",
        SQSSink("https://q.fifo", client=Recorder()),
        [big],
    )

    run(
        "F1 SNS ten 100 KiB events",
        SNSSink("arn:t", client=Recorder()),
        [event(i, 100 * KIB) for i in range(10)],
    )
    run(
        "F2 SNS one 300 KiB event beside two small",
        SNSSink("arn:t", client=Recorder()),
        [event(0, 10), event(1, 300 * KIB), event(2, 10)],
    )
    sns = SNSSink("arn:t", client=Recorder())
    sns.MAX_BYTES = 2048
    run(
        "F3 SNS instance MAX_BYTES=2048, a 3 KiB event beside a small one",
        sns,
        [event(0, 10), event(1, 3 * KIB)],
    )

    class SmallTopic(SNSSink):
        MAX_BYTES = 2048

    run(
        "F4 SNS subclass MAX_BYTES=2048, a 3 KiB event beside a small one",
        SmallTopic("arn:t", client=Recorder()),
        [event(0, 10), event(1, 3 * KIB)],
    )


if __name__ == "__main__":
    main()
