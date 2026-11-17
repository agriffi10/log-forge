"""SPEC-100 probe: what order a FIFO group's messages land in when one of them fails once.

Written on 2026-11-17 while authoring SPEC-100, against 03dbf502; not one of the audit's probes.
A fake service keeps the order in which it accepted each message. It fails one chosen message
retryably (a `Failed` entry with `SenderFault: false`) for its first `n` sends and accepts
everything else. Each case emits five events of one trace (one group by default) and two of
another, through `emit`, which runs the engine's `max_retries + 1` rounds. It runs against
`SQSSink` on any tree; when the tree's `SNSSink` takes `fifo` (the SPEC-100 prototype) it also
runs `SNSSink`, with the group hold and with the hold switched off. No AWS library is needed. Run
from the repository root:

    PYTHONPATH=src python -B docs/audits/2026-11-14-probes/sinks/spec100_ordering.py

Cases:
  A  event a0 fails once.
  B  event a0 fails on every send (`max_retries + 1` of them).
  C  the first call's reply names every entry failed, then every later call is accepted.
  D  twelve events of one group, more than one call's ten, and a00 fails once.
  E  the same twelve, and a00 fails on every send.
"""

from __future__ import annotations

import contextlib
import inspect
import io

from log_foundry.sinks.sns import SNSSink
from log_foundry.sinks.sqs import SQSSink


class Service:
    """A boto3-shaped client failing `victim` for its first `times` sends, or a whole first call."""

    def __init__(self, victim: str | None, times: int, *, first_call_fails: bool = False) -> None:
        self.victim = victim
        self.times = times
        self.first_call_fails = first_call_fails
        self.sent: dict[str, int] = {}
        self.landed: list[str] = []
        self.calls = 0

    def _reply(self, entries: list[dict[str, str]], body: str) -> dict:
        self.calls += 1
        failed, ok = [], []
        for entry in entries:
            name = entry[body].split('"n": "')[1].split('"')[0]
            self.sent[name] = self.sent.get(name, 0) + 1
            if (self.first_call_fails and self.calls == 1) or (
                name == self.victim and self.sent[name] <= self.times
            ):
                failed.append({"Id": entry["Id"], "SenderFault": False, "Code": "InternalError"})
            else:
                self.landed.append(name)
                ok.append({"Id": entry["Id"], "MessageId": "m"})
        return {"Successful": ok, "Failed": failed}

    def send_message_batch(self, QueueUrl: str, Entries: list[dict[str, str]]) -> dict:
        return self._reply(Entries, "MessageBody")

    def publish_batch(self, TopicArn: str, PublishBatchRequestEntries: list[dict[str, str]]) -> dict:
        return self._reply(PublishBatchRequestEntries, "Message")


def batch(twelve: bool = False) -> list[dict[str, object]]:
    if twelve:
        return [{"log_id": f"la{i}", "trace_id": "A" * 32, "n": f"a{i:02d}"} for i in range(12)]
    a = [{"log_id": f"la{i}", "trace_id": "A" * 32, "n": f"a{i}"} for i in range(5)]
    b = [{"log_id": f"lb{i}", "trace_id": "B" * 32, "n": f"b{i}"} for i in range(2)]
    return [a[0], b[0], a[1], a[2], b[1], a[3], a[4]]


def run(label: str, make, service: Service, twelve: bool = False) -> None:
    sink = make(service)
    err = io.StringIO()
    with contextlib.redirect_stderr(err):
        try:
            sink.emit(batch(twelve))
            outcome = "returned"
        except Exception as exc:
            outcome = f"raised {type(exc).__name__}"
    group_a = [n for n in service.landed if n.startswith("a")]
    print(
        f"   {label}: {service.calls} call(s); landed order {service.landed}; group A landed sorted: "
        f"{group_a == sorted(group_a)}; emit {outcome}; failed={sink.losses().failed}"
    )
    for line in err.getvalue().splitlines():
        print(f"      stderr: {line}")


def makers():
    out = [("SQSSink", lambda c: SQSSink("https://sqs/q.fifo", client=c))]
    if "fifo" in inspect.signature(SNSSink.__init__).parameters:
        out.append(("SNSSink", lambda c: SNSSink("arn:aws:sns:r:1:t.fifo", client=c)))
    return out


@contextlib.contextmanager
def hold_off():
    """Switches the shared hold off, so a held message is sent at once (the alternative)."""
    from log_foundry.sinks import _fifo

    saved = (_fifo.hold, _fifo.block)
    _fifo.hold = lambda chunk, blocked, sends, limit: [False] * len(chunk)
    _fifo.block = lambda *args: None
    try:
        yield
    finally:
        _fifo.hold, _fifo.block = saved


def case(title: str, twelve: bool = False, **service_kwargs) -> None:
    print(title)
    for name, make in makers():
        run(name, make, Service(**service_kwargs), twelve)
        if name == "SNSSink":
            with hold_off():
                run("SNSSink, hold off", make, Service(**service_kwargs), twelve)


if __name__ == "__main__":
    case("A  a0 fails once", victim="a0", times=1)
    case("B  a0 fails on every send", victim="a0", times=99)
    case("C  the first call's reply names every entry failed", victim=None, times=0, first_call_fails=True)
    case("D  twelve events of one group, a00 fails once", twelve=True, victim="a00", times=1)
    case("E  twelve events of one group, a00 fails on every send", twelve=True, victim="a00", times=99)
