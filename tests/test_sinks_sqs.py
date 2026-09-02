"""SPEC-005 — SQSSink: protocol conformance, count/byte chunking, Failed retry, oversized drop.

All tests inject a fake SQS client (no boto3 / AWS / network). The one boto3-creation path is
covered by injecting a fake ``boto3`` module into ``sys.modules``.
"""

import json
import sys

import pytest

from log_foundry.sinks.base import SinkDeliveryError


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Neutralize backoff sleeps (SPEC-027 FR-003 gave SQSSink one) so retry tests run instantly."""
    # ``wait`` is bound into each sink at import, and its Event branch never reaches
    # ``time.sleep`` — patching either centrally would leave this fixture inert.
    monkeypatch.setattr("log_foundry.sinks.sqs.wait", lambda _delay, _stop=None: None)

sqs_mod = pytest.importorskip("log_foundry.sinks.sqs")
SQSSink = sqs_mod.SQSSink


class FakeSQSClient:
    """Records send_message_batch calls; can fail chosen entry Ids once or always."""

    def __init__(self) -> None:
        self.calls: list[list[dict]] = []
        self.fail_once_ids: set[str] = set()
        self.always_fail_ids: set[str] = set()
        self._failed_once: set[str] = set()

    def send_message_batch(self, *, QueueUrl: str, Entries: list[dict]) -> dict:
        self.calls.append([dict(e) for e in Entries])
        successful, failed = [], []
        for entry in Entries:
            eid = entry["Id"]
            if eid in self.always_fail_ids:
                failed.append({"Id": eid, "SenderFault": False, "Code": "InternalError"})
            elif eid in self.fail_once_ids and eid not in self._failed_once:
                self._failed_once.add(eid)
                failed.append({"Id": eid, "SenderFault": False, "Code": "Throttled"})
            else:
                successful.append({"Id": eid, "MessageId": f"m-{eid}"})
        return {"Successful": successful, "Failed": failed}

    @property
    def sent_bodies(self) -> list[str]:
        """Bodies from Successful sends (each entry counted once, across all calls)."""
        seen: list[str] = []
        failed_once_left = set(self.fail_once_ids)
        for call in self.calls:
            for entry in call:
                eid = entry["Id"]
                if eid in self.always_fail_ids:
                    continue
                if eid in failed_once_left:
                    failed_once_left.discard(eid)
                    continue
                seen.append(entry["MessageBody"])
        return seen


def _events(n: int, pad: int = 0) -> list[dict]:
    return [{"i": i, "pad": "x" * pad} for i in range(n)]


# -- FR-001: Sink protocol conformance --------------------------------------------------


def test_conforms_to_sink_protocol() -> None:
    from log_foundry.sinks.base import Sink

    sink = SQSSink("https://sqs.example/q", client=FakeSQSClient())
    assert isinstance(sink, Sink)


def test_injected_client_avoids_boto3() -> None:
    fake = FakeSQSClient()
    sink = SQSSink("https://sqs.example/q", client=fake)
    assert sink.client is fake
    assert "boto3" not in sys.modules, "an injected client must not import boto3"


def test_creates_boto3_client_when_none(monkeypatch) -> None:
    created = {}

    class FakeBoto3:
        @staticmethod
        def client(name: str) -> str:
            created["name"] = name
            return "FAKE_SQS_CLIENT"

    monkeypatch.setitem(sys.modules, "boto3", FakeBoto3)
    sink = SQSSink("https://sqs.example/q")
    assert created["name"] == "sqs"
    assert sink.client == "FAKE_SQS_CLIENT"


# -- FR-002: count- and byte-aware chunking ---------------------------------------------


def test_chunks_by_count_max_ten() -> None:
    client = FakeSQSClient()
    SQSSink("q", client=client).emit(_events(25))

    assert [len(call) for call in client.calls] == [10, 10, 5]
    assert all(len(call) <= SQSSink.MAX_BATCH for call in client.calls)


def test_chunks_by_bytes_under_limit() -> None:
    client = FakeSQSClient()
    # ~100 KB each: at most two fit one 256 KB request.
    SQSSink("q", client=client).emit(_events(3, pad=100 * 1024))

    for call in client.calls:
        total = sum(len(entry["MessageBody"].encode("utf-8")) for entry in call)
        assert total <= SQSSink.MAX_BYTES
    assert len(client.calls) >= 2, "300 KB of events must split across requests"


def test_full_coverage_no_loss_no_duplication() -> None:
    client = FakeSQSClient()
    events = _events(23)
    SQSSink("q", client=client).emit(events)

    sent = [json.loads(b) for call in client.calls for entry in call for b in [entry["MessageBody"]]]
    assert sent == events, "every event sent exactly once, in order"


def test_each_body_is_json_dumps_of_event() -> None:
    client = FakeSQSClient()
    events = _events(3)
    SQSSink("q", client=client).emit(events)

    bodies = [entry["MessageBody"] for call in client.calls for entry in call]
    assert bodies == [json.dumps(e) for e in events]


# -- FR-003: partial-failure handling ---------------------------------------------------


def test_partial_failure_retries_only_failed_entries() -> None:
    client = FakeSQSClient()
    client.fail_once_ids = {"1"}  # second entry fails on first attempt, then succeeds
    sink = SQSSink("q", client=client, max_retries=3)
    sink.emit(_events(3))

    assert len(client.calls) == 2, "one initial send + one retry"
    assert [e["Id"] for e in client.calls[1]] == ["1"], "retry carries only the failed entry"
    assert sink.failed == 0


def test_persistent_failure_is_counted_not_raised(capsys) -> None:
    client = FakeSQSClient()
    client.always_fail_ids = {"0"}
    sink = SQSSink("q", client=client, max_retries=2)
    sink.emit(_events(3))  # entries 1,2 succeed; entry 0 never does

    assert sink.failed >= 1
    assert len(client.calls) == 3, "initial + max_retries attempts for the failing entry"
    assert f"lost {sink.failed} message(s)" in capsys.readouterr().err
    # the successfully-sent entries were not discarded
    assert {json.loads(b)["i"] for b in client.sent_bodies} == {1, 2}


# -- FR-004: oversized-event handling ---------------------------------------------------


def test_oversized_event_dropped_rest_sent(capsys) -> None:
    client = FakeSQSClient()
    sink = SQSSink("q", client=client)
    batch = [{"i": 0}, {"i": 1, "big": "x" * (SQSSink.MAX_BYTES + 10)}, {"i": 2}]
    sink.emit(batch)

    assert sink.dropped_oversized == 1
    assert "lost 1 event(s)" in capsys.readouterr().err
    sent = {json.loads(b)["i"] for call in client.calls for entry in call for b in [entry["MessageBody"]]}
    assert sent == {0, 2}, "the oversized event is dropped; the rest are still sent"


# -- FR-005: close ----------------------------------------------------------------------


def test_close_is_noop() -> None:
    sink = SQSSink("q", client=FakeSQSClient())
    assert sink.close() is None


class _Boom(Exception):
    """A client fault shaped like `botocore`'s ClientError / EndpointConnectionError."""


class _Raising:
    """A SQS client that raises on the chosen call numbers, and records what it accepted."""

    def __init__(self, fail_on: set[int]) -> None:
        self.calls = 0
        self.accepted: list[str] = []
        self.fail_on = fail_on

    def _go(self, items: list[str]) -> None:
        self.calls += 1
        if self.calls in self.fail_on:
            raise _Boom("the endpoint could not be reached")
        self.accepted.extend(items)

    def send_message_batch(self, *, QueueUrl, Entries):
        self._go([e["MessageBody"] for e in Entries])
        return {}


def test_a_client_failure_costs_its_chunk_not_the_batch(capsys) -> None:
    """SPEC-048 FR-002. The client call was unguarded, so a fault mid-batch duplicated the rest.

    A `ClientError` on chunk N propagated out of `emit` after chunks 1..N-1 had landed, and the
    worker retries whole batches -- so the exit drain, which is one large batch by construction,
    re-sent everything already delivered. Measured before the fix: 25 events in 3 chunks, `delivered=35 duplicates=10 losses=(0, 0)`.

    The criterion that binds is that `emit` **returns**: that is what stops the worker's retry at
    its source, so the duplication cannot happen rather than being cleaned up afterwards.
    """
    client = _Raising(fail_on={2})
    sink = SQSSink("https://q", client=client, max_retries=0)
    sink.emit([{"i": i, "trace_id": f"t{i}"} for i in range(25)])
    assert len(client.accepted) == len(set(client.accepted)) == 15, (
        "the chunks that landed are delivered exactly once"
    )
    assert sink.losses().failed == 10, "and the failed chunk is counted, not silent"
    assert "_Boom" in capsys.readouterr().err, "and announced by exception type"


def test_a_total_client_failure_still_raises() -> None:
    """A wholly-failed batch must still reach the worker's retry -- nothing landed to duplicate.

    **This is the mutation-sensitive one.** With the guard added and the failure not fed back
    into the total-failure test, `emit` returns normally having lost the entire batch with
    `losses()` reading zero -- which is precisely the "a sink that absorbs a total failure is a
    sink the worker believes" shape SPEC-026 exists to end.
    """
    client = _Raising(fail_on=set(range(1, 500)))
    sink = SQSSink("https://q", client=client, max_retries=0)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"i": i, "trace_id": f"t{i}"} for i in range(25)])
    assert client.accepted == [], "nothing landed"
    assert sink.losses().failed > 0, "and every abandoned item is counted"


def test_a_keyboard_interrupt_is_not_absorbed_by_the_chunk_guard() -> None:
    """The guard catches `Exception`, never `BaseException` (SPEC-025 FR-004).

    An operator's Ctrl-C and the runtime's `SystemExit` are intent, and must reach the caller.
    """

    class _Interrupting(_Raising):
        def _go(self, items: list[str]) -> None:
            raise KeyboardInterrupt

    sink = SQSSink("https://q", client=_Interrupting(fail_on=set()), max_retries=0)
    with pytest.raises(KeyboardInterrupt):
        sink.emit([{"i": 1, "trace_id": "t"}])
