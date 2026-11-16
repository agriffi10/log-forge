"""SPEC-096 probe: what SQSSink and SNSSink count when the destination's limit is below the sink's.

Written on 2026-11-16 while authoring SPEC-096, against 10ba21ee; not one of the audit's probes.
The fake service enforces a queue's or topic's `MaximumMessageSize` and the 1 MiB batch total in
one of two ways, because the AWS pages do not say which a real `SendMessageBatch` uses for an entry
over a queue's own `MaximumMessageSize`:

  request  the whole call fails with a 400 `ClientError`, `InvalidParameterValue` (an entry over
           the destination's limit) or `BatchRequestTooLong` (the sum over the batch limit), and
           stores nothing. This is how moto's SQS emulation behaves (`moto/sqs/models.py`,
           `send_message_batch` validates every entry before sending any).
  entry    an entry over the destination's limit is named in `Failed` with `SenderFault: true` and
           the rest are stored; the batch total still fails the whole call.

A sink limit above today's 256 KiB is simulated by assigning `MAX_BYTES` on the instance, which
`_prepare`, `_chunks` and `_bodies` read per round at 10ba21ee (see `spec096_limits_today.py`
case D), so it is what a `max_message_bytes=` argument would do. `botocore` is not installed: a
stand-in `botocore.exceptions` module with a `ClientError` is put in `sys.modules`, which is where
`_classify.driver_classes` looks. Run from the repository root:

    PYTHONPATH=src python -B docs/audits/2026-11-14-probes/sinks/spec096_destination_below_limit.py
"""

from __future__ import annotations

import contextlib
import io
import sys
import threading
import types

KIB = 1024
MIB = 1024 * KIB


class ClientError(Exception):
    """botocore's shape: `.response` carries `Error.Code` and `ResponseMetadata.HTTPStatusCode`."""

    def __init__(self, code: str, status: int = 400) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}


_exc = types.ModuleType("botocore.exceptions")
_exc.ClientError = ClientError  # type: ignore[attr-defined]
sys.modules.setdefault("botocore", types.ModuleType("botocore"))
sys.modules["botocore.exceptions"] = _exc

from log_foundry.sinks.sns import SNSSink  # noqa: E402
from log_foundry.sinks.sqs import SQSSink  # noqa: E402


class Service:
    """A queue or topic with its own `MaximumMessageSize`, in one refusal mode."""

    def __init__(self, maximum: int, mode: str, batch_limit: int) -> None:
        self.maximum = maximum
        self.mode = mode
        self.batch_limit = batch_limit
        self.held = 0
        self.calls = 0

    def _take(self, entries: list[dict[str, str]], body_key: str) -> dict:
        self.calls += 1
        sizes = [len(e[body_key].encode("utf-8")) for e in entries]
        if sum(sizes) > self.batch_limit:
            raise ClientError("BatchRequestTooLong")
        over = {e["Id"] for e, s in zip(entries, sizes, strict=True) if s > self.maximum}
        if over and self.mode == "request":
            raise ClientError("InvalidParameterValue")
        self.held += len(entries) - len(over)
        return {
            "Successful": [{"Id": e["Id"]} for e in entries if e["Id"] not in over],
            "Failed": [
                {"Id": i, "SenderFault": True, "Code": "InvalidParameterValue"} for i in over
            ],
        }

    def send_message_batch(self, QueueUrl: str, Entries: list[dict[str, str]]) -> dict:
        return self._take(Entries, "MessageBody")

    def publish_batch(
        self, TopicArn: str, PublishBatchRequestEntries: list[dict[str, str]]
    ) -> dict:
        return self._take(PublishBatchRequestEntries, "Message")


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
    service = sink.client
    print(f"\n== {name}")
    print(
        f"   calls={service.calls} held={service.held} of {len(batch)} "
        f"dropped_oversized={sink.dropped_oversized} failed={sink.failed} raised={raised}"
    )
    for line in [x for x in err.getvalue().splitlines() if x.strip()][:3]:
        print(f"     | {line[:170]}")


def sqs(limit: int | None, service: Service) -> SQSSink:
    sink = SQSSink("https://q", client=service, max_retries=3)
    if limit is not None:
        sink.MAX_BYTES = limit
    return sink


def sns(limit: int | None, service: Service) -> SNSSink:
    sink = SNSSink("arn:t", client=service, max_retries=3)
    if limit is not None:
        sink.MAX_BYTES = limit
    return sink


def main() -> None:
    mixed = (
        [event(i, 10) for i in range(4)]
        + [event(4, 400 * KIB)]
        + [event(i, 10) for i in range(5, 10)]
    )
    print("batch: nine ~100-byte events and one 400 KiB event, ten entries, one request's worth")
    for mode in ("request", "entry"):
        run(
            f"S1 [{mode}] SQS at 256 KiB (today, and the compatible default), queue at 262,144",
            sqs(None, Service(262144, mode, MIB)),
            mixed,
        )
        run(
            f"S2 [{mode}] SQS at 1 MiB (the alternative default), queue at 262,144",
            sqs(MIB, Service(262144, mode, MIB)),
            mixed,
        )
        run(
            f"S3 [{mode}] SQS at 1 MiB, queue at 1,048,576 (the service default)",
            sqs(MIB, Service(MIB, mode, MIB)),
            mixed,
        )
        run(
            f"S4 [{mode}] SQS at 256 KiB, queue at 65,536 (configured lower than the sink)",
            sqs(None, Service(64 * KIB, mode, MIB)),
            [event(0, 10), event(1, 100 * KIB), event(2, 10)],
        )

    over = [event(i, 300 * KIB) for i in range(6)]
    run(
        "S5 [request] SQS MAX_BYTES=2 MiB (above the service), six 300 KiB events, queue at 1 MiB",
        sqs(2 * MIB, Service(MIB, "request", MIB)),
        over,
    )

    two = [event(0, 200 * KIB), event(1, 200 * KIB)]
    run(
        "T1 [entry] SNS at 1 MiB, topic at its default 262,144: two 200 KiB messages, one request",
        sns(MIB, Service(262144, "entry", 262144)),
        two,
    )
    run(
        "T2 [entry] SNS at 256 KiB (today), topic at 262,144: the same two messages",
        sns(None, Service(262144, "entry", 262144)),
        two,
    )
    run(
        "T3 [entry] SNS at 1 MiB, topic raised to 1,048,576: one 600 KiB message",
        sns(MIB, Service(MIB, "entry", MIB)),
        [event(0, 600 * KIB)],
    )
    run(
        "T4 [entry] SNS at 256 KiB (today), topic raised to 1,048,576: the same 600 KiB message",
        sns(None, Service(MIB, "entry", MIB)),
        [event(0, 600 * KIB)],
    )


if __name__ == "__main__":
    main()
