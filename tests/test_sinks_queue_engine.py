"""SPEC-074 FR-002 — the queue services deliver per entry on the engine, and keyed results are read once.

``SQSSink``, ``SNSSink``, ``KinesisSink`` and ``FirehoseSink`` each make one call per chunk per
round; the engine resends only what may land, within 1.x's total budget. Every client here is a
scripted double: ``reply(entries, call)`` returns the response for one call (or raises), and each
call's entries are recorded with their decoded events, so a test reads what reached the client
by event, never by recomputing the sink's rule. Serves invariants 2, 3, 6 and 7.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import pytest

import log_foundry
from conftest import install_worker
from log_foundry.sinks import _engine
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.firehose import FirehoseSink
from log_foundry.sinks.kinesis import KinesisSink
from log_foundry.sinks.sns import SNSSink
from log_foundry.sinks.sqs import SQSSink
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict
from log_foundry.worker import Worker

Reply = Callable[[list[dict[str, Any]], int], Any]

QUEUES = ("SQSSink", "SNSSink")
STREAMS = ("KinesisSink", "FirehoseSink")
ALL = QUEUES + STREAMS
FIFO = "https://sqs/q.fifo"


@pytest.fixture(autouse=True)
def _no_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("log_foundry.sinks._retry.wait", lambda _delay, _stop=None: None)


def accept(entries: list[dict[str, Any]], call: int) -> dict[str, Any]:
    return {"Failed": []}


class Client:
    """A double of all four AWS clients, each call answered by ``reply``.

    Attributes:
      calls: Per call, the decoded event of each entry, in order.
      entries: Per call, the raw entries sent.
    """

    def __init__(self, reply: Reply = accept) -> None:
        self.reply = reply
        self.calls: list[list[dict[str, Any]]] = []
        self.entries: list[list[dict[str, Any]]] = []

    def _go(self, entries: list[dict[str, Any]], body: Callable[[dict[str, Any]], Any]) -> Any:
        self.entries.append([dict(e) for e in entries])
        self.calls.append([json.loads(body(e)) for e in entries])
        return self.reply(entries, len(self.calls))

    def send_message_batch(self, *, QueueUrl: str, Entries: list[dict[str, Any]]) -> Any:
        return self._go(Entries, lambda e: e["MessageBody"])

    def publish_batch(
        self, *, TopicArn: str, PublishBatchRequestEntries: list[dict[str, Any]]
    ) -> Any:
        return self._go(PublishBatchRequestEntries, lambda e: e["Message"])

    def put_records(self, *, StreamName: str, Records: list[dict[str, Any]]) -> Any:
        return self._go(Records, lambda e: e["Data"])

    def put_record_batch(self, *, DeliveryStreamName: str, Records: list[dict[str, Any]]) -> Any:
        return self._go(Records, lambda e: e["Data"])

    def sent(self, i: int) -> int:
        """How many calls carried event ``i``."""
        return sum(1 for call in self.calls for event in call if event.get("i") == i)


def build(kind: str, reply: Reply = accept, **kwargs: Any) -> tuple[Any, Client]:
    client = Client(reply)
    chunk = kwargs.pop("chunk", None)
    if kind == "SQSSink":
        sink: Any = SQSSink(kwargs.pop("url", "https://sqs/q"), client=client, **kwargs)
    elif kind == "SNSSink":
        sink = SNSSink("arn", client=client, **kwargs)
    elif kind == "KinesisSink":
        sink = KinesisSink("s", client=client, **kwargs)
    else:
        sink = FirehoseSink("s", client=client, **kwargs)
    if chunk is not None:
        if kind in QUEUES:
            sink.MAX_BATCH = chunk
        else:
            sink.MAX_RECORDS = chunk
    return sink, client


def events(n: int, **extra: Any) -> list[dict[str, object]]:
    return [{"i": i, "log_id": f"l{i}", "trace_id": "t", **extra} for i in range(n)]


def raising(
    error: Callable[[], Exception], when: Callable[[list[dict[str, Any]], int], bool]
) -> Reply:
    def reply(entries: list[dict[str, Any]], call: int) -> Any:
        if when(entries, call):
            raise error()
        return ok_reply(entries)

    return reply


def ok_reply(entries: list[dict[str, Any]]) -> dict[str, Any]:
    """An all-accepted reply in whichever shape the entries' service uses."""
    if entries and "Data" in entries[0]:
        results = [{"SequenceNumber": "1"} for _ in entries]
        return {
            "FailedRecordCount": 0,
            "Records": results,
            "FailedPutCount": 0,
            "RequestResponses": results,
        }
    return {"Failed": []}


def failing(kind: str, failed: Callable[[dict[str, Any]], bool], *, sender: bool = False) -> Reply:
    """A reply coding each entry whose decoded event ``failed`` says as a per-entry failure."""

    def reply(entries: list[dict[str, Any]], call: int) -> Any:
        if kind in STREAMS:
            results = [
                {"ErrorCode": "InternalFailure"}
                if failed(json.loads(e["Data"]))
                else {"SequenceNumber": "1"}
                for e in entries
            ]
            count = sum(1 for r in results if "ErrorCode" in r)
            key = (
                ("FailedRecordCount", "Records")
                if kind == "KinesisSink"
                else (
                    "FailedPutCount",
                    "RequestResponses",
                )
            )
            return {key[0]: count, key[1]: results}
        body = "MessageBody" if kind == "SQSSink" else "Message"
        return {
            "Failed": [
                {"Id": e["Id"], "SenderFault": sender, "Code": "X"}
                for e in entries
                if failed(json.loads(e[body]))
            ]
        }

    return reply


def lost(err: str) -> list[str]:
    return [line for line in err.splitlines() if line.startswith("log-foundry: lost")]


def _worker(sink: object, **kwargs: Any) -> Worker:
    kwargs.setdefault("batch_size", 1000)
    kwargs.setdefault("flush_interval", 60.0)
    worker = Worker(sink, **kwargs)  # type: ignore[arg-type]
    install_worker(worker)
    return worker


def through_worker(sink: object, batch: list[dict[str, object]], retries: int = 2) -> Worker:
    worker = _worker(sink, max_retries=retries)
    worker.submit(batch)
    worker.flush(timeout=30.0)
    return worker


# --- one bad entry costs only itself ----------------------------------------------------------


def test_one_bad_entry_costs_only_itself() -> None:
    sink, client = build("SQSSink", failing("SQSSink", lambda e: e["i"] == 37), max_retries=3)
    sink.emit(events(100))
    landed = [e["i"] for call in client.calls for e in call if e["i"] != 37]
    assert sorted(landed) == [i for i in range(100) if i != 37], "99 land, each once"
    assert sink.failed == 1
    assert client.sent(37) == 4


# --- sender faults ----------------------------------------------------------------------------


@pytest.mark.parametrize("kind", QUEUES)
def test_a_sender_fault_beside_a_retryable_entry(
    kind: str, capsys: pytest.CaptureFixture[str]
) -> None:
    body = "MessageBody" if kind == "SQSSink" else "Message"

    def reply(entries: list[dict[str, Any]], call: int) -> Any:
        if call > 1:
            return {"Failed": []}
        failed = []
        for e in entries:
            i = json.loads(e[body])["i"]
            if i in (0, 1):
                failed.append({"Id": e["Id"], "SenderFault": i == 0, "Code": f"C{i}"})
        return {"Failed": failed}

    sink, client = build(kind, reply, max_retries=3)
    sink.emit(events(3))
    assert [[e["i"] for e in call] for call in client.calls] == [[0, 1, 2], [1]]
    assert sink.failed == 1
    lines = lost(capsys.readouterr().err)
    assert lines == [
        f"log-foundry: lost 1 message(s); {kind}, rejected as invalid (first code: C0); not retried"
    ]


@pytest.mark.parametrize("kind", QUEUES)
def test_a_batch_lost_only_to_sender_faults_returns(kind: str) -> None:
    sink, _ = build(kind, failing(kind, lambda e: True, sender=True), max_retries=3)
    sink.emit(events(2))
    assert sink.failed == 2
    sink2, _ = build(kind, failing(kind, lambda e: True, sender=True), max_retries=3)
    worker = _worker(sink2, max_retries=2)
    worker.submit(events(2))
    assert worker.flush(timeout=30.0)
    assert worker.failed_batches == 0


@pytest.mark.parametrize("kind", QUEUES)
def test_a_sender_fault_beside_an_exhausted_chunk_raises(kind: str) -> None:
    def reply(entries: list[dict[str, Any]], call: int) -> Any:
        body = "MessageBody" if kind == "SQSSink" else "Message"
        return {
            "Failed": [
                {"Id": e["Id"], "SenderFault": json.loads(e[body])["i"] == 0, "Code": "X"}
                for e in entries
            ]
        }

    sink, _ = build(kind, reply, max_retries=1, chunk=1)
    with pytest.raises(SinkDeliveryError):
        sink.emit(events(2))
    sink2, _ = build(kind, reply, max_retries=1, chunk=1)
    worker = through_worker(sink2, events(2))
    assert worker.failed_batches == 1


@pytest.mark.parametrize(
    "failed",
    [
        {"Id": "1"},
        ["1"],
        [{"Id": "7"}],
        [{"Id": "1"}, {"Id": "1"}],
        [{"Code": "X"}],
        [{"Id": True}],
    ],
    ids=["dict", "str-item", "unknown-id", "duplicate", "no-id", "bool-id"],
)
@pytest.mark.parametrize("kind", QUEUES)
def test_a_malformed_failed_is_counted_once_and_never_resent(
    kind: str, failed: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    sink, client = build(kind, lambda entries, call: {"Failed": failed}, max_retries=3)
    sink.emit(events(3))
    assert len(client.calls) == 1, "never resent"
    assert sink.failed == 3, "the chunk counted once"
    lines = lost(capsys.readouterr().err)
    assert len(lines) == 1 and "could not adjudicate" in lines[0], lines


# --- SQS FIFO keeps message-group order -------------------------------------------------------


def _grouped(groups: str) -> list[dict[str, object]]:
    return [{"i": i, "log_id": f"l{i}", "g": g} for i, g in enumerate(groups)]


def _fifo(reply: Reply, **kwargs: Any) -> tuple[SQSSink, Client]:
    return build(  # type: ignore[return-value]
        "SQSSink",
        reply,
        url=FIFO,
        message_group_id=lambda e: str(e["g"]),
        **kwargs,
    )


def _order(client: Client) -> list[list[int]]:
    return [[e["i"] for e in call] for call in client.calls]


def test_a_one_group_fifo_batch_on_a_healthy_client_is_one_call() -> None:
    sink, client = _fifo(accept, max_retries=3)
    sink.emit(_grouped("AAAAA"))
    assert _order(client) == [[0, 1, 2, 3, 4]]


def test_a_fifo_group_keeps_its_order_across_chunks() -> None:
    def reply(entries: list[dict[str, Any]], call: int) -> Any:
        if call == 1:
            return {"Failed": [{"Id": entries[0]["Id"], "SenderFault": False, "Code": "T"}]}
        return {"Failed": []}

    sink, client = _fifo(reply, max_retries=3, chunk=2)
    sink.emit(_grouped("ACAB"))
    assert _order(client) == [[0, 1], [3], [0], [2]], (
        "A2 waits until A0 lands; B is sent in round one; no A overtakes an earlier A"
    )


def test_a_standard_queue_holds_nothing() -> None:
    def reply(entries: list[dict[str, Any]], call: int) -> Any:
        if call == 1:
            return {"Failed": [{"Id": entries[0]["Id"], "SenderFault": False, "Code": "T"}]}
        return {"Failed": []}

    sink, client = build("SQSSink", reply, max_retries=3, chunk=2)
    sink.emit(_grouped("ACAB"))
    assert _order(client) == [[0, 1], [2, 3], [0]]


def test_an_always_failing_event_frees_its_group_on_its_last_send() -> None:
    sink, client = _fifo(failing("SQSSink", lambda e: e["i"] == 0), max_retries=3, chunk=2)
    sink.emit(_grouped("ACAB"))
    assert _order(client) == [[0, 1], [3], [0], [0], [0, 2]], (
        "A2 is first sent in the round of A0's fourth send, after A0, and lands"
    )
    assert sink.failed == 1


def _rounds(client: Client, sink: SQSSink) -> list[tuple[int, int]]:
    """Wraps the client so each entry sent is recorded as ``(round, i)``, in send order."""
    seen: list[tuple[int, int]] = []
    send = client.send_message_batch

    def recording(*, QueueUrl: str, Entries: list[dict[str, Any]]) -> Any:
        number = _engine.run_context().round
        seen.extend((number, json.loads(e["MessageBody"])["i"]) for e in Entries)
        return send(QueueUrl=QueueUrl, Entries=Entries)

    sink.client = type("C", (), {"send_message_batch": staticmethod(recording)})()
    return seen


def test_the_last_round_sends_a_held_event_after_its_predecessors_final_send(
    capsys: pytest.CaptureFixture[str],
) -> None:
    sink, client = _fifo(failing("SQSSink", lambda e: e["i"] in (0, 1)), max_retries=3, chunk=1)
    seen = _rounds(client, sink)
    sink.emit(_grouped("AAA"))
    assert seen == [(1, 0), (2, 0), (3, 0), (4, 0), (4, 1), (4, 2)], (
        "A1 is first sent on A0's last send; A2, held behind A1 until then, follows A1's final "
        "send in the run's last round"
    )
    assert sink.failed == 2, "A2's verdict is its own send's, which landed"
    err = lost(capsys.readouterr().err)
    assert not any("held behind" in line for line in err), err


@pytest.mark.parametrize("path", ["worker", "run_delivery"])
def test_no_round_before_the_last_sends_a_held_event(path: str) -> None:
    sink, client = _fifo(failing("SQSSink", lambda e: e["i"] in (0, 1, 2)), max_retries=3, chunk=1)
    seen = _rounds(client, sink)
    if path == "worker":
        through_worker(sink, _grouped("AAAA"), retries=1)
    else:
        _engine.run_delivery(sink, _grouped("AAAA"), worker_retries=1, stop=None)
    first = {i: min(r for r, j in seen if j == i) for i in range(4)}
    assert first == {0: 1, 1: 4, 2: 7, 3: 8}, (
        "each successor waits for its predecessor's fourth send, and A3 for the run's last "
        "round (8 = 4 x 2), never a cycle's last round"
    )
    assert [j for r, j in seen if r == 8] == [0, 1, 2, 3], "the last round keeps batch order"


def test_a_run_ending_before_its_last_round_counts_a_held_event_with_its_own_line(
    capsys: pytest.CaptureFixture[str],
) -> None:
    sink, client = _fifo(failing("SQSSink", lambda e: e["i"] == 0), max_retries=3, chunk=1)
    _engine.run_delivery(
        sink, _grouped("AAA"), worker_retries=0, stop=None, decline=lambda _longest: True
    )
    assert _order(client) == [[0]], "the decline ends the run after round one"
    assert client.sent(1) == client.sent(2) == 0
    assert sink.failed == 3
    assert (
        "log-foundry: lost 2 message(s); SQSSink, 0 attempt(s), held behind an earlier message of its FIFO group"
        in lost(capsys.readouterr().err)
    )


def test_only_an_engine_runs_last_round_is_last() -> None:
    seen: list[tuple[int, bool]] = []

    class Probe:
        max_retries = 2

        def __init__(self) -> None:
            self.log_foundry_counters = SinkCounters()

        def deliver(self, batch: list[dict[str, object]]) -> Verdict:
            context = _engine.run_context()
            seen.append((context.round, context.last))
            return Verdict.uniform(Outcome.RETRYABLE, len(batch))

        def emit(self, batch: list[dict[str, object]]) -> None:
            _engine.emit_via_deliver(self, batch)

    _engine.run_delivery(Probe(), [{"i": 0}], worker_retries=1, stop=None)
    assert seen == [(1, False), (2, False), (3, False), (4, False), (5, False), (6, True)]
    seen.clear()
    _engine.one_round(Probe().deliver, [{"i": 0}])
    assert seen == [(1, False)], "a detached round's caller owns any later round"
    assert _engine.run_context().last is False


# --- Kinesis and Firehose: an unreadable reply ------------------------------------------------


@pytest.mark.parametrize("kind", STREAMS)
def test_an_unadjudicable_response_is_counted_once(
    kind: str, capsys: pytest.CaptureFixture[str]
) -> None:
    key = (
        ("FailedRecordCount", "Records")
        if kind == "KinesisSink"
        else ("FailedPutCount", "RequestResponses")
    )
    sink, client = build(kind, lambda entries, call: {key[0]: 1, key[1]: [{}]}, max_retries=3)
    sink.emit(events(2))
    assert len(client.calls) == 1
    assert sink.dropped_unadjudicated == 2
    assert sink.losses().failed == 2
    assert sink.failed == 0
    call = "put_records" if kind == "KinesisSink" else "put_record_batch"
    assert lost(capsys.readouterr().err) == [
        (
            f"log-foundry: lost 2 record(s); {kind} could not adjudicate a {call} response "
            "(2 record(s) sent, 1 result(s) returned, 0 coded, failed count 1); abandoned, not "
            "retried"
        )
    ]


# --- the budget ---------------------------------------------------------------------------------


def _unit(kind: str) -> str:
    return "message" if kind in QUEUES else "record"


@pytest.mark.parametrize("kind", ALL)
def test_an_always_retryable_client_spends_the_whole_budget_through_emit(
    kind: str, capsys: pytest.CaptureFixture[str]
) -> None:
    sink, client = build(
        kind, raising(lambda: ConnectionError("down"), lambda e, c: True), max_retries=3, chunk=1
    )
    with pytest.raises(SinkDeliveryError):
        sink.emit(events(3))
    assert len(client.calls) == 12
    assert sink.failed == 3
    assert lost(capsys.readouterr().err) == [
        f"log-foundry: lost 3 {_unit(kind)}(s); {kind}, 4 attempt(s), ConnectionError"
    ]


@pytest.mark.parametrize("kind", ALL)
def test_an_always_retryable_client_spends_the_whole_budget_through_the_worker(
    kind: str, capsys: pytest.CaptureFixture[str]
) -> None:
    sink, client = build(
        kind, raising(lambda: ConnectionError("down"), lambda e, c: True), max_retries=3, chunk=1
    )
    worker = through_worker(sink, events(3))
    assert len(client.calls) == 36
    assert worker.failed_batches == 1
    assert sink.failed == 3
    assert lost(capsys.readouterr().err) == [
        f"log-foundry: lost 3 {_unit(kind)}(s); {kind}, 12 attempt(s), ConnectionError",
        "log-foundry: lost 3 event(s); batch abandoned after 3 emit attempts",
    ]


@pytest.mark.parametrize("kind", ALL)
def test_a_refused_exception_costs_its_chunk_only(
    kind: str, capsys: pytest.CaptureFixture[str]
) -> None:
    first = raising(lambda: TypeError("caller"), lambda entries, call: call == 1)
    sink, client = build(kind, first, max_retries=3, chunk=1)
    sink.emit(events(3))
    assert [[e["i"] for e in call] for call in client.calls] == [[0], [1], [2]]
    assert sink.failed == 1
    assert lost(capsys.readouterr().err) == [
        f"log-foundry: lost 1 {_unit(kind)}(s); {kind}, 1 attempt(s), TypeError"
    ]


# --- each event is encoded once per run ---------------------------------------------------------


def _flaky(entries: list[dict[str, Any]], call: int) -> Any:
    if call <= 2:
        raise ConnectionError("down")
    return ok_reply(entries)


def _encode_run(
    kind: str, monkeypatch: pytest.MonkeyPatch | None = None
) -> tuple[int, Any, list[str]]:
    sink, _ = build(kind, _flaky, max_retries=3)
    real = sink._encode
    calls: list[object] = []

    def counted(event: dict[str, object]) -> Any:
        calls.append(event)
        return real(event)

    sink._encode = counted
    sink.emit([*events(3), {"i": 9, "blob": b"raw"}])
    return len(calls), sink, []


@pytest.mark.parametrize("kind", ALL)
def test_each_event_is_encoded_once_per_run(kind: str, capsys: pytest.CaptureFixture[str]) -> None:
    calls, sink, _ = _encode_run(kind)
    assert calls == 4, "encode is called len(batch) times however many rounds run"
    lines = [line for line in capsys.readouterr().err.splitlines() if "encoding raised" in line]
    assert lines == [f"log-foundry: lost 1 event(s); {kind}, encoding raised TypeError"]
    assert sink.losses().dropped == 1


def test_the_encode_cache_mutant_is_caught(monkeypatch: pytest.MonkeyPatch) -> None:
    """Dropping the per-run cache — a fresh notes dict on every call — reddens the count."""
    real = _engine.RunContext.notes

    def forgetful(self: _engine.RunContext, owner: object) -> dict[str, object]:
        notes = real(self, owner)
        notes.pop("encoded", None)
        return notes

    monkeypatch.setattr(_engine.RunContext, "notes", forgetful)
    calls, _, _ = _encode_run("SNSSink")
    assert calls > 4


# --- SQS FIFO id callables --------------------------------------------------------------------


def test_an_id_failure_and_an_unencodable_event_alone_settle() -> None:
    def group(event: dict[str, object]) -> str:
        if event["i"] == 0:
            raise KeyError("g")
        return "g"

    sink, client = build("SQSSink", url=FIFO, message_group_id=group)
    batch: list[dict[str, object]] = [{"i": 0}, {"i": 1, "blob": b"raw"}]
    sink.emit(batch)
    assert client.calls == []
    assert sink.losses() == SinkLosses(dropped=2, failed=0)
    sink2, _ = build("SQSSink", url=FIFO, message_group_id=group)
    worker = through_worker(sink2, [{"i": 0}, {"i": 1, "blob": b"raw"}])
    assert worker.failed_batches == 0


def _counting_ids() -> tuple[dict[str, list[object]], Callable[..., str], Callable[..., str]]:
    seen: dict[str, list[object]] = {"group": [], "dedup": []}

    def group(event: dict[str, object]) -> str:
        seen["group"].append(event["i"])
        return f"g{event['i']}"

    def dedup(event: dict[str, object]) -> str:
        seen["dedup"].append(event["i"])
        return "" if event["i"] == 1 else f"d{event['i']}"

    return seen, group, dedup


def _ids_run() -> tuple[dict[str, list[object]], Client]:
    seen, group, dedup = _counting_ids()

    def reply(entries: list[dict[str, Any]], call: int) -> Any:
        if call <= 2:
            failed = [e for e in entries if json.loads(e["MessageBody"])["i"] == 1]
            return {"Failed": [{"Id": e["Id"], "SenderFault": False, "Code": "T"} for e in failed]}
        return {"Failed": []}

    sink, client = build(
        "SQSSink",
        reply,
        url=FIFO,
        message_group_id=group,
        message_deduplication_id=dedup,
        max_retries=3,
    )
    sink.emit(events(3))
    return seen, client


def _carried_ids(client: Client, i: int) -> list[tuple[str, str]]:
    return [
        (entry["MessageGroupId"], entry["MessageDeduplicationId"])
        for call in client.entries
        for entry in call
        if json.loads(entry["MessageBody"])["i"] == i
    ]


def test_fifo_ids_are_resolved_once_per_run() -> None:
    seen, client = _ids_run()
    assert sorted(seen["group"]) == [0, 1, 2] and sorted(seen["dedup"]) == [0, 1, 2]
    resent = _carried_ids(client, 1)
    assert len(resent) == 3 and len(set(resent)) == 1, "every resend carries the first send's ids"


def test_the_per_round_id_mutant_is_caught(monkeypatch: pytest.MonkeyPatch) -> None:
    real = SQSSink._chunks

    def per_round(self: SQSSink, batch: list[dict[str, object]]) -> Any:
        _engine.run_context().notes(self).pop("prepared", None)
        return real(self, batch)

    monkeypatch.setattr(SQSSink, "_chunks", per_round)
    seen, client = _ids_run()
    assert len(seen["dedup"]) > 3
    assert len(set(_carried_ids(client, 1))) > 1


def test_a_repeated_event_never_resends_a_landed_occurrences_ids() -> None:
    """M1: the same event twice; its first occurrence lands, the second is resent with its own."""
    event: dict[str, object] = {"i": 0, "log_id": "same"}

    def reply(entries: list[dict[str, Any]], call: int) -> Any:
        if call == 1:
            return {"Failed": [{"Id": entries[1]["Id"], "SenderFault": False, "Code": "T"}]}
        return {"Failed": []}

    sink, client = build(
        "SQSSink", reply, url=FIFO, message_group_id="g", message_deduplication_id=lambda e: ""
    )
    sink.emit([event, event])
    first, second = client.entries[0]
    [resent] = client.entries[1]
    assert resent["MessageDeduplicationId"] == second["MessageDeduplicationId"]
    assert resent["MessageDeduplicationId"] != first["MessageDeduplicationId"]


# --- drops count once, on a settled run ---------------------------------------------------------


@pytest.mark.parametrize("kind", ALL)
def test_oversize_and_unencodable_drops_count_on_a_settled_run_only(kind: str) -> None:
    big = "x" * (2 * 1024 * 1024)
    batch: list[dict[str, object]] = [{"i": 0}, {"i": 1, "pad": big}, {"i": 2, "blob": b"raw"}]
    sink, _ = build(kind)
    sink.emit(batch)
    assert (sink.dropped_oversized, sink.losses().dropped) == (1, 2)
    failed, _ = build(
        kind, raising(lambda: ConnectionError("down"), lambda e, c: True), max_retries=0
    )
    with pytest.raises(SinkDeliveryError):
        failed.emit(batch)
    assert (failed.dropped_oversized, failed.losses().dropped) == (0, 0)


# --- every id callable raising is a refusal -----------------------------------------------------


def _all_raise() -> tuple[list[KeyError], Callable[[dict[str, object]], str]]:
    raised: list[KeyError] = []

    def group(event: dict[str, object]) -> str:
        error = KeyError("g")
        raised.append(error)
        raise error

    return raised, group


def test_every_id_callable_raising_is_a_refusal_through_emit() -> None:
    raised, group = _all_raise()
    sink, client = build("SQSSink", url=FIFO, message_group_id=group, max_retries=3)
    with pytest.raises(KeyError) as info:
        sink.emit(events(3))
    assert info.value is raised[0]
    assert len(raised) == 3
    assert client.calls == []
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


def test_every_id_callable_raising_is_one_deliver_and_one_failed_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _raised, group = _all_raise()
    sink, _ = build("SQSSink", url=FIFO, message_group_id=group, max_retries=3)
    rounds: list[int] = []
    real = SQSSink.deliver
    monkeypatch.setattr(
        SQSSink, "deliver", lambda self, batch: (rounds.append(1), real(self, batch))[1]
    )
    worker = through_worker(sink, events(3))
    assert len(rounds) == 1
    assert worker.failed_batches == 1
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


def test_every_id_callable_raising_is_one_orphan_loss() -> None:
    raised, group = _all_raise()
    sink, _ = build("SQSSink", url=FIFO, message_group_id=group, max_retries=3)
    log_foundry.configure(service="t", sink=sink)
    before = log_foundry.health().orphan_lost
    log_foundry.info("orphan")
    assert len(raised) == 1
    assert log_foundry.health().orphan_lost == before + 1


# --- the worker and orphan twins ----------------------------------------------------------------


def _orphan(sink: object) -> int:
    log_foundry.configure(service="t", sink=sink)
    before = log_foundry.health().orphan_lost
    log_foundry.info("orphan")
    return log_foundry.health().orphan_lost - before


@pytest.mark.parametrize("kind", QUEUES)
def test_a_one_event_sender_fault_keeps_one_ledger_on_both_paths(kind: str) -> None:
    orphan, _ = build(kind, failing(kind, lambda e: True, sender=True))
    assert _orphan(orphan) == 0
    worked, _ = build(kind, failing(kind, lambda e: True, sender=True))
    worker = through_worker(worked, events(1))
    assert worker.failed_batches == 0
    assert orphan.failed == worked.failed == 1


@pytest.mark.parametrize("kind", ALL)
def test_a_one_event_retryable_refusal_keeps_one_ledger_on_both_paths(kind: str) -> None:
    orphan, _ = build(kind, failing(kind, lambda e: True), max_retries=1)
    assert _orphan(orphan) == 1
    worked, _ = build(kind, failing(kind, lambda e: True), max_retries=1)
    worker = through_worker(worked, events(1))
    assert worker.failed_batches == 1
    assert orphan.failed == worked.failed == 1


@pytest.mark.parametrize("kind", STREAMS)
def test_failed_is_read_under_the_counter_lock_so_a_commit_never_tears_it(kind: str) -> None:
    """``.failed`` is ``failed_events - dropped_unadjudicated``, read under the lock that moves both.

    Read outside it, a commit in progress showed one moved and not the other (frame-1 review).
    """
    import threading

    sink, _ = build(kind)
    read: list[int] = []
    with sink._counter_lock:
        sink.log_foundry_counters.failed_events += 2
        reader = threading.Thread(target=lambda: read.append(sink.failed))
        reader.start()
        reader.join(0.2)
        assert reader.is_alive(), "the read waits for the commit in progress"
        sink.dropped_unadjudicated += 2
    reader.join(10.0)
    assert read == [0]
