"""SPEC-100 probe: is a FIFO entry's dedup id the same on every resend, and what is it by default?

Written on 2026-11-17 while authoring SPEC-100, against 03dbf502; not one of the audit's probes.
A recording fake client answers each call's first entry as a retryable failure on the first call
and accepts everything after, so the engine resends that entry in a second round. Each case prints
the `MessageDeduplicationId` and `MessageGroupId` of every send, and how often each id callable
ran. It runs against `SQSSink` on any tree, and against `SNSSink` too when the tree's `SNSSink`
takes `fifo` (the SPEC-100 prototype). No AWS library is needed. Run from the repository root:

    PYTHONPATH=src python -B docs/audits/2026-11-14-probes/sinks/spec100_ids_once.py

Cases:
  A  the default ids: is the dedup id the event's `log_id`, and the group its `trace_id`?
  B  callables that return a new value on every call: is the resend's id the first send's?
  C  the same event objects emitted again (a second run): does the id change?
  D  an event with no `log_id`: the minted fallback, within one run and across two.
  E  callables returning 200 characters that share their first 128: what goes on the wire?
  F  callables returning garbage: a blank group, a blank dedup id, a dedup id of None, and a
     dedup callable returning None for every event: can any send lack an id?
"""

from __future__ import annotations

import contextlib
import inspect
import io
import itertools

from log_foundry.sinks.sns import SNSSink
from log_foundry.sinks.sqs import SQSSink


class FailFirstEntryOnce:
    """A boto3-shaped SQS and SNS client: the first call's first entry fails retryably."""

    def __init__(self) -> None:
        self.calls: list[list[dict[str, str]]] = []

    def _reply(self, entries: list[dict[str, str]]) -> dict:
        self.calls.append(entries)
        if len(self.calls) == 1:
            failed = [{"Id": entries[0]["Id"], "SenderFault": False, "Code": "InternalError"}]
            ok = [{"Id": e["Id"], "MessageId": "m"} for e in entries[1:]]
            return {"Successful": ok, "Failed": failed}
        return {"Successful": [{"Id": e["Id"], "MessageId": "m"} for e in entries], "Failed": []}

    def send_message_batch(self, QueueUrl: str, Entries: list[dict[str, str]]) -> dict:
        return self._reply(Entries)

    def publish_batch(self, TopicArn: str, PublishBatchRequestEntries: list[dict[str, str]]) -> dict:
        return self._reply(PublishBatchRequestEntries)


def sinks(**kwargs):
    """Each FIFO sink this tree can build, with its client."""
    built = []
    client = FailFirstEntryOnce()
    built.append(("SQSSink", SQSSink("https://sqs/q.fifo", client=client, **kwargs), client))
    if "fifo" in inspect.signature(SNSSink.__init__).parameters:
        client = FailFirstEntryOnce()
        built.append(("SNSSink", SNSSink("arn:aws:sns:r:1:t.fifo", client=client, **kwargs), client))
    return built


def show(label: str, client: FailFirstEntryOnce) -> None:
    for number, call in enumerate(client.calls, 1):
        pairs = [(e.get("MessageDeduplicationId"), e.get("MessageGroupId")) for e in call]
        print(f"   {label} call {number}: (dedup, group) = {pairs}")


def quiet(fn) -> None:
    with contextlib.redirect_stderr(io.StringIO()):
        fn()


def case_a() -> None:
    print("A  default ids")
    events = [{"log_id": "L0", "trace_id": "T0"}, {"log_id": "L1", "trace_id": "T1"}]
    for name, sink, client in sinks():
        quiet(lambda: sink.emit(events))
        show(name, client)


def case_b() -> None:
    print("B  callables returning a new value per call")
    for name, _, _ in sinks():
        counter = itertools.count()
        asked: list[str] = []

        def dedup(event: dict[str, object]) -> str:
            asked.append("dedup")
            return f"d{next(counter)}"

        def group(event: dict[str, object]) -> str:
            asked.append("group")
            return f"g{next(counter)}"

        sink, client = next(
            (s, c)
            for n, s, c in sinks(message_deduplication_id=dedup, message_group_id=group)
            if n == name
        )
        quiet(lambda: sink.emit([{"log_id": "L0"}, {"log_id": "L1"}]))
        show(name, client)
        print(f"   {name} callables ran {len(asked)} time(s) for 2 events over 2 rounds")


def case_c() -> None:
    print("C  the same event objects in a second run")
    for name, _, _ in sinks():
        counter = itertools.count()
        sink, client = next(
            (s, c)
            for n, s, c in sinks(message_deduplication_id=lambda e: f"d{next(counter)}")
            if n == name
        )
        events = [{"log_id": "L0"}]
        quiet(lambda: sink.emit(events))
        quiet(lambda: sink.emit(events))
        show(f"{name} callable", client)
        sink, client = next((s, c) for n, s, c in sinks() if n == name)
        quiet(lambda: sink.emit(events))
        quiet(lambda: sink.emit(events))
        show(f"{name} default", client)


def case_d() -> None:
    print("D  no log_id: the minted fallback")
    for name, sink, client in sinks():
        events = [{"trace_id": "T0"}, {"trace_id": "T1"}]
        quiet(lambda: sink.emit(events))
        quiet(lambda: sink.emit(events))
        show(name, client)


def case_e() -> None:
    print("E  200-character ids sharing their first 128")
    for name, _, _ in sinks():
        sink, client = next(
            (s, c)
            for n, s, c in sinks(
                message_deduplication_id=lambda e: "p" * 128 + str(e["n"]) * 72,
                message_group_id=lambda e: "q" * 128 + str(e["n"]) * 72,
            )
            if n == name
        )
        quiet(lambda: sink.emit([{"log_id": "L0", "n": 0}, {"log_id": "L1", "n": 1}]))
        wire = [e["MessageDeduplicationId"] for e in client.calls[-1]]
        first = client.calls[0]
        print(
            f"   {name}: dedup id lengths on the wire {[len(e['MessageDeduplicationId']) for e in first]}, "
            f"the two events' dedup ids equal: {first[0]['MessageDeduplicationId'] == first[1]['MessageDeduplicationId']}, "
            f"group ids equal: {first[0]['MessageGroupId'] == first[1]['MessageGroupId']}; last call {len(wire)} entry(ies)"
        )


def case_f() -> None:
    print("F  callables returning garbage")
    answers = {
        "blank group": {"message_group_id": lambda e: "  "},
        "blank dedup": {"message_deduplication_id": lambda e: ""},
        "None dedup for event 1": {
            "message_deduplication_id": lambda e: None if e["log_id"] == "L1" else "ok"
        },
        "None dedup for every event": {"message_deduplication_id": lambda e: None},
    }
    for label, kwargs in answers.items():
        for name, sink, client in sinks(**kwargs):
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                try:
                    sink.emit([{"log_id": "L0", "trace_id": "T0"}, {"log_id": "L1", "trace_id": "T1"}])
                    outcome = "returned"
                except Exception as exc:
                    outcome = f"raised {type(exc).__name__}"
            sent = [
                (len(e.get("MessageDeduplicationId", "")), e.get("MessageGroupId"))
                for call in client.calls
                for e in call
            ]
            missing = sum(
                1
                for call in client.calls
                for e in call
                if "MessageGroupId" not in e or "MessageDeduplicationId" not in e
            )
            print(
                f"   {name} {label}: emit {outcome}; (dedup length, group) sent {sent}; "
                f"entries missing an id {missing}; losses()={sink.losses()}"
            )
            for line in err.getvalue().splitlines():
                print(f"      stderr: {line}")


if __name__ == "__main__":
    case_a()
    case_b()
    case_c()
    case_d()
    case_e()
    case_f()
