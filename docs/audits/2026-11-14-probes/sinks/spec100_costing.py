"""SPEC-100 probe: does a FIFO entry's cost include its two ids, on SQSSink and on SNSSink?

Written on 2026-11-17 while authoring SPEC-100, against 03dbf502; not one of the audit's probes.
A recording fake client takes every entry. Each case sends one event whose JSON body alone is a
few bytes under the sink's limit, so it fits only if the ids are not counted, beside one small
event, and prints what was sent, what was dropped and the lines. `SQSSink` runs on any tree, as
`spec096_limits_today.py` case E does; `SNSSink` on a FIFO topic runs when the tree's `SNSSink`
takes `fifo` (the SPEC-100 prototype). No AWS library is needed. Run from the repository root:

    PYTHONPATH=src python -B docs/audits/2026-11-14-probes/sinks/spec100_costing.py

Cases:
  A  a FIFO destination: the body is 40 bytes under the limit and the ids cost 64.
  B  the same body to a standard destination, as the control: it must be sent.
  C  the same as A with a 1,000-byte limit set on the instance, so the measurement does not
     depend on the default.
"""

from __future__ import annotations

import contextlib
import inspect
import io
import json

from log_foundry.sinks.sns import SNSSink
from log_foundry.sinks.sqs import SQSSink


class Recorder:
    """A boto3-shaped SQS and SNS client that takes every entry."""

    def __init__(self) -> None:
        self.requests: list[list[dict[str, str]]] = []

    def send_message_batch(self, QueueUrl: str, Entries: list[dict[str, str]]) -> dict:
        self.requests.append(Entries)
        return {"Successful": [{"Id": e["Id"]} for e in Entries], "Failed": []}

    def publish_batch(self, TopicArn: str, PublishBatchRequestEntries: list[dict[str, str]]) -> dict:
        self.requests.append(PublishBatchRequestEntries)
        return {"Successful": [{"Id": e["Id"]} for e in PublishBatchRequestEntries], "Failed": []}


def sized(limit: int, under: int) -> dict[str, object]:
    """An event whose `json.dumps` is exactly `limit - under` bytes."""
    event: dict[str, object] = {"log_id": "l" * 32, "trace_id": "t" * 32, "pad": ""}
    event["pad"] = "x" * (limit - under - len(json.dumps(event).encode("utf-8")))
    assert len(json.dumps(event).encode("utf-8")) == limit - under
    return event


def run(label: str, sink, client: Recorder, limit: int) -> None:
    big = sized(limit, 40)
    small = {"log_id": "s" * 32, "trace_id": "t" * 32}
    err = io.StringIO()
    with contextlib.redirect_stderr(err):
        sink.emit([big, small])
    sent = [len(e.get("MessageBody", e.get("Message", ""))) for r in client.requests for e in r]
    print(
        f"   {label}: limit {limit}, body {limit - 40} bytes; bodies sent {sent}; "
        f"dropped_oversized={sink.dropped_oversized}"
    )
    for line in err.getvalue().splitlines():
        print(f"      stderr: {line}")


def builds(fifo: bool):
    yield "SQSSink", lambda c: SQSSink("https://sqs/q.fifo" if fifo else "https://sqs/q", client=c)
    if "fifo" in inspect.signature(SNSSink.__init__).parameters or not fifo:
        arn = "arn:aws:sns:r:1:t.fifo" if fifo else "arn:aws:sns:r:1:t"
        yield "SNSSink", lambda c: SNSSink(arn, client=c)


def case(title: str, fifo: bool, limit: int | None = None) -> None:
    print(title)
    for name, make in builds(fifo):
        client = Recorder()
        sink = make(client)
        if limit is not None:
            sink.MAX_BYTES = limit
        run(name, sink, client, sink.MAX_BYTES)


if __name__ == "__main__":
    case("A  FIFO, body 40 bytes under the default limit, ids 64 bytes", fifo=True)
    case("B  standard, the same body (control)", fifo=False)
    case("C  FIFO, a 1,000-byte limit on the instance", fifo=True, limit=1000)
