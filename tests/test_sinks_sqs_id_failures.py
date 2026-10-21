"""SPEC-058 FR-004 — ``SQSSink``'s FIFO id callables cost only their event; oversize counts once.

On a FIFO queue ``_chunks`` asks the caller's ``message_group_id`` and
``message_deduplication_id`` callables for every event, and one raise used to cost the batch. It
now drops that event under FR-002's ledger rule, and — in the same function — the existing
oversize drop gets the same deferral: both are counted and announced only when ``emit`` returns
normally, because a raising ``emit`` is retried whole and would otherwise count the same event once
per attempt (Probe D: ``dropped == 4`` for one event).
"""

from __future__ import annotations

import pytest

import log_foundry
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.memory import MemorySink
from log_foundry.sinks.multi import MultiSink
from log_foundry.sinks.sqs import SQSSink

FIFO_URL = "https://sqs.example/q.fifo"
STD_URL = "https://sqs.example/q"
SENTINEL = "s3cr3t-sentinel"
ID_LINE = "log-foundry: lost 1 event(s); SQSSink, a FIFO id callable raised ValueError"


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Neutralize the SQS backoff so retry tests run instantly."""
    monkeypatch.setattr("log_foundry.sinks.sqs.wait", lambda _delay, _stop=None: None)


class Accepting:
    """Accepts everything and records the entries of every call."""

    def __init__(self) -> None:
        self.calls: list[list[dict]] = []

    def send_message_batch(self, *, QueueUrl: str, Entries: list[dict]) -> dict:
        self.calls.append([dict(entry) for entry in Entries])
        return {"Successful": [{"Id": entry["Id"]} for entry in Entries], "Failed": []}

    @property
    def entries(self) -> list[dict]:
        return [entry for call in self.calls for entry in call]


class Down(Accepting):
    """Records every call and raises on each, as an unreachable endpoint does."""

    def send_message_batch(self, *, QueueUrl: str, Entries: list[dict]) -> dict:
        self.calls.append([dict(entry) for entry in Entries])
        raise ConnectionError("unreachable")


class DownOnce(Accepting):
    """Raises on its first call and accepts every one after."""

    def send_message_batch(self, *, QueueUrl: str, Entries: list[dict]) -> dict:
        if not self.calls:
            self.calls.append([dict(entry) for entry in Entries])
            raise ConnectionError("first attempt")
        return super().send_message_batch(QueueUrl=QueueUrl, Entries=Entries)


class Refuses:
    """A client the test must never reach."""

    def send_message_batch(self, **kwargs: object) -> dict:
        raise AssertionError("the client must not be called")


def _ev(i: int, **extra: object) -> dict[str, object]:
    return {"i": i, "message": "m", "trace_id": "a" * 32, "log_id": f"log-{i}", **extra}


def _fails_for(i: int, message: str = "id failed"):
    def callable_(event: dict[str, object]) -> str:
        if event["i"] == i:
            raise ValueError(message)
        return "group"

    return callable_


def _id_lines(err: str) -> list[str]:
    return [line for line in err.splitlines() if "id callable" in line]


def _oversize_lines(err: str) -> list[str]:
    return [line for line in err.splitlines() if "message limit" in line]


@pytest.mark.parametrize("which", ["group", "dedup"])
def test_a_raising_id_callable_costs_only_its_event(which: str, capsys) -> None:
    client = Accepting()
    key = "message_group_id" if which == "group" else "message_deduplication_id"
    sink = SQSSink(FIFO_URL, client=client, **{key: _fails_for(2)})

    sink.emit([_ev(0), _ev(1), _ev(2), _ev(3)])

    assert [entry["MessageBody"] for entry in client.entries] == [
        f'{{"i": {i}, "message": "m", "trace_id": "{"a" * 32}", "log_id": "log-{i}"}}'
        for i in (0, 1, 3)
    ]
    assert sink.losses() == SinkLosses(dropped=1, failed=0)
    assert _id_lines(capsys.readouterr().err) == [ID_LINE]


def test_every_id_failing_reraises_the_first_exception(capsys) -> None:
    raised: list[BaseException] = []

    def group(event: dict[str, object]) -> str:
        raised.append(ValueError("every one fails"))
        raise raised[-1]

    sink = SQSSink(FIFO_URL, client=Refuses(), message_group_id=group)

    with pytest.raises(ValueError) as caught:
        sink.emit([_ev(0), _ev(1)])

    assert caught.value is raised[0]
    assert sink.losses() == SinkLosses(dropped=0, failed=0)
    assert capsys.readouterr().err == ""


def test_an_id_failure_and_an_oversized_event_settle_without_a_send() -> None:
    sink = SQSSink(FIFO_URL, client=Refuses(), message_group_id=_fails_for(0))
    sink.MAX_BYTES = 300

    sink.emit([_ev(0), _ev(1, pad="x" * 400)])

    assert sink.losses() == SinkLosses(dropped=2, failed=0)
    assert sink.dropped_oversized == 1


def test_a_retried_batch_counts_the_id_failure_only_on_the_attempt_that_returned(capsys) -> None:
    sink = SQSSink(FIFO_URL, client=DownOnce(), message_group_id=_fails_for(1), max_retries=0)
    batch = [_ev(0), _ev(1), _ev(2)]

    with pytest.raises(SinkDeliveryError):
        sink.emit(batch)
    assert sink.losses().dropped == 0
    assert _id_lines(capsys.readouterr().err) == []

    sink.emit(batch)
    assert sink.losses().dropped == 1
    assert _id_lines(capsys.readouterr().err) == [ID_LINE]


@pytest.mark.parametrize("client", ["down", "down-once"])
def test_probe_d_rerun_the_oversized_event_is_counted_once_or_never(client: str, capsys) -> None:
    """Probe D through the worker: abandoned, the event is counted nowhere but the abandon line.

    With an always-failing client the worker abandons the batch after its retries, and the
    oversized event's count and line are gone with every attempt that raised. With a client that
    fails once, the retry returns normally and the event is counted, and announced, once.
    """
    sink = SQSSink(STD_URL, client=Down() if client == "down" else DownOnce(), max_retries=0)
    log_foundry.configure(service="t", sink=sink, max_value_bytes=500_000)

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("hello", big="x" * 300_000)

    work()
    result = log_foundry.flush()

    err = capsys.readouterr().err
    if client == "down":
        assert not result.ok and log_foundry.health().failed_batches == 1
        assert sink.dropped_oversized == 0
        assert sink.losses().dropped == 0
        assert _oversize_lines(err) == []
    else:
        assert result.ok and log_foundry.health().failed_batches == 0
        assert sink.dropped_oversized == 1
        assert sink.losses().dropped == 1
        assert len(_oversize_lines(err)) == 1


def test_an_id_failure_does_not_move_dropped_oversized() -> None:
    sink = SQSSink(FIFO_URL, client=Accepting(), message_deduplication_id=_fails_for(0))

    sink.emit([_ev(0), _ev(1)])

    assert sink.dropped_oversized == 0
    assert sink.losses() == SinkLosses(dropped=1, failed=0)


def test_a_standard_queue_never_consults_the_callables() -> None:
    def never(event: dict[str, object]) -> str:
        raise AssertionError("a standard queue must not ask for an id")

    client = DownOnce()
    sink = SQSSink(
        STD_URL,
        client=client,
        message_group_id=never,
        message_deduplication_id=never,
        max_retries=0,
    )
    sink.MAX_BYTES = 300
    batch = [_ev(0), _ev(1, pad="x" * 400)]

    with pytest.raises(SinkDeliveryError):
        sink.emit(batch)
    assert sink.dropped_oversized == 0, "the attempt raised, so its oversize drop is not counted"

    sink.emit(batch)
    assert sink.dropped_oversized == 1
    assert [sorted(entry) for entry in client.calls[-1]] == [["Id", "MessageBody"]], (
        "the one sendable event went out with no FIFO ids"
    )


def test_no_sentinel_reaches_stderr(capsys) -> None:
    sink = SQSSink(FIFO_URL, client=Accepting(), message_group_id=_fails_for(0, f"bad {SENTINEL}"))

    sink.emit([_ev(0, secret=SENTINEL), _ev(1, secret=SENTINEL)])

    err = capsys.readouterr().err
    assert SENTINEL not in err
    assert _id_lines(err) == [ID_LINE]


def test_an_orphan_id_failure_is_absorbed_once() -> None:
    sink = SQSSink(FIFO_URL, client=Refuses(), message_group_id=_fails_for_every)
    log_foundry.configure(service="t", sink=sink)

    log_foundry.info("outside a span")

    assert log_foundry.health().orphan_lost == 1
    assert sink.losses().dropped == 0


def _fails_for_every(event: dict[str, object]) -> str:
    raise ValueError("fails")


def test_under_a_fan_out_the_id_failed_event_is_absorbed_once() -> None:
    sqs = SQSSink(FIFO_URL, client=Down(), message_group_id=_fails_for(1), max_retries=0)
    multi = MultiSink(sqs, MemorySink())

    multi.emit([_ev(0), _ev(1), _ev(2), _ev(3)])

    assert sqs.losses().dropped == 0, "its emit raised, so it counted no id failure"
    assert sqs.failed == 3, "the premise: the three sendable entries were lost at the client"
    assert multi.losses() == SinkLosses(dropped=0, failed=sqs.failed + 4)
