"""SPEC-100 probe: what each FIFO argument combination does at construction, on both AWS sinks.

Written on 2026-11-17 while revising SPEC-100, against 03dbf502; not one of the audit's probes.
Each case builds `SQSSink` and, when the tree's `SNSSink` takes `fifo` (the SPEC-100 prototype),
`SNSSink`, with an injected client so nothing is imported or built, and prints the exception's
type and message, or `accepted` with the sink's `fifo`. Case H emits one event whose
de-duplication callable raises and one oversized event to a FIFO sink and prints what came back.
No AWS library is needed. Run from the repository root:

    PYTHONPATH=src python -B docs/audits/2026-11-14-probes/sinks/spec100_refusals.py

Cases:
  A  `fifo` of the wrong type: `1`, `"yes"`, and a value whose `__class__` claims `bool`.
  B  `fifo=True` on a name not ending in `.fifo`.
  C  `fifo=False` on a name ending in `.fifo`, and `fifo=True` on one.
  D  an id argument on a sink that is not FIFO, by its name and by `fifo=False`.
  E  two faults in one call: which refusal wins.
  F  a blank constant group on a standard name.
  G  the base's `_REFUSED` shapes: a group of `5` and a dedup of `"x"`.
  H  a FIFO batch of one id failure and one oversized event.
"""

from __future__ import annotations

import contextlib
import inspect
import io
from typing import TYPE_CHECKING, Any

from log_foundry.sinks.sns import SNSSink
from log_foundry.sinks.sqs import SQSSink

if TYPE_CHECKING:
    from collections.abc import Callable


class Client:
    """A boto3-shaped SQS and SNS client that takes every entry and records each call."""

    def __init__(self) -> None:
        self.calls: list[list[dict[str, str]]] = []

    def _reply(self, entries: list[dict[str, str]]) -> dict:
        self.calls.append(entries)
        return {"Successful": [{"Id": e["Id"], "MessageId": "m"} for e in entries], "Failed": []}

    def send_message_batch(self, *, QueueUrl: str, Entries: list[dict[str, str]]) -> dict:
        return self._reply(Entries)

    def publish_batch(self, *, TopicArn: str, PublishBatchRequestEntries: list[dict[str, str]]) -> dict:
        return self._reply(PublishBatchRequestEntries)


class Claims:
    """A value whose `__class__` claims `bool`."""

    @property
    def __class__(self):  # type: ignore[override]
        return bool


SNS_FIFO = "fifo" in inspect.signature(SNSSink.__init__).parameters
NAMES = {"SQSSink": ("https://sqs/q", "https://sqs/q.fifo"), "SNSSink": ("arn:aws:sns:r:1:t", "arn:aws:sns:r:1:t.fifo")}


def builders() -> list[tuple[str, type, str, str]]:
    out = [("SQSSink", SQSSink, *NAMES["SQSSink"])]
    if SNS_FIFO:
        out.append(("SNSSink", SNSSink, *NAMES["SNSSink"]))
    return out


def attempt(label: str, build: Callable[[], Any]) -> None:
    try:
        sink = build()
    except Exception as exc:
        print(f"   {label}: {type(exc).__name__}: {exc}")
    else:
        print(f"   {label}: accepted, fifo={sink.fifo!r}")


def case(title: str, kwargs_by_label: dict[str, tuple[bool, dict[str, object]]]) -> None:
    print(title)
    for name, cls, std, fifo_name in builders():
        for label, (on_fifo, kwargs) in kwargs_by_label.items():
            target = fifo_name if on_fifo else std
            attempt(
                f"{name} {label}",
                lambda cls=cls, target=target, kwargs=kwargs: cls(target, client=Client(), **kwargs),
            )


def case_h() -> None:
    print("H  a FIFO batch of one id failure and one oversized event")
    for name, cls, _std, fifo_name in builders():

        def dedup(event: dict[str, object]) -> str:
            if event["n"] == 0:
                raise KeyError("n")
            return str(event["log_id"])

        client = Client()
        sink = cls(fifo_name, client=client, message_deduplication_id=dedup)
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            try:
                result = f"returned {sink.emit([{'log_id': 'L0', 'trace_id': 'T', 'n': 0}, {'log_id': 'L1', 'trace_id': 'T', 'n': 1, 'pad': 'x' * 300_000}])!r}"
            except Exception as exc:
                result = f"raised {type(exc).__name__}"
        print(f"   {name}: emit {result}; calls {len(client.calls)}; losses()={sink.losses()}")
        for line in err.getvalue().splitlines():
            print(f"      stderr: {line}")


def main() -> None:
    if not SNS_FIFO:
        print("SNSSink cases skipped: this tree's SNSSink takes no `fifo` (run on the SPEC-100 prototype)")
    case("A  fifo of the wrong type", {
        "fifo=1": (True, {"fifo": 1}),
        "fifo='yes'": (True, {"fifo": "yes"}),
        "fifo=Claims()": (True, {"fifo": Claims()}),
    })
    case("B  fifo=True on a standard name", {"fifo=True": (False, {"fifo": True})})
    case("C  fifo given on a .fifo name", {
        "fifo=False": (True, {"fifo": False}),
        "fifo=True": (True, {"fifo": True}),
    })
    case("D  an id argument on a sink that is not FIFO", {
        "standard, group": (False, {"message_group_id": "g"}),
        "standard, dedup": (False, {"message_deduplication_id": lambda e: "d"}),
        "standard, both": (False, {"message_group_id": "g", "message_deduplication_id": lambda e: "d"}),
        ".fifo with fifo=False, group": (True, {"fifo": False, "message_group_id": "g"}),
    })
    case("E  two faults in one call", {
        "max_retries=2.5, fifo='x'": (False, {"max_retries": 2.5, "fifo": "x"}),
        "fifo='x', group=5": (False, {"fifo": "x", "message_group_id": 5}),
    })
    case("F  a blank constant group on a standard name", {"group='  '": (False, {"message_group_id": "  "})})
    case("G  the _REFUSED shapes", {
        "FIFO, group=5": (True, {"message_group_id": 5}),
        "standard, group=5": (False, {"message_group_id": 5}),
        "FIFO, dedup='x'": (True, {"message_deduplication_id": "x"}),
        "standard, dedup='x'": (False, {"message_deduplication_id": "x"}),
    })
    case_h()


if __name__ == "__main__":
    main()
