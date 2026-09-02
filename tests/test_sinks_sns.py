"""SPEC-010 — SNSSink: 10-entry publish_batch chunks + Failed-list retry (fake client)."""

from __future__ import annotations

import json

import pytest

from log_foundry.sinks.base import Sink, SinkDeliveryError
from log_foundry.sinks.sns import SNSSink


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch):
    """Neutralize the retry backoff SPEC-027 FR-003 added, so retry tests run instantly.

    ``wait`` is bound into each sink at import, and its Event branch never reaches ``time.sleep``
    — patching either centrally would leave this fixture inert.
    """
    monkeypatch.setattr("log_foundry.sinks.sns.wait", lambda _delay, _stop=None: None)


class FakeSNS:
    """Records publish_batch calls; fails entry Ids in ``fail_once``/``always_fail``."""

    def __init__(self, fail_once: set[str] | None = None, always_fail: set[str] | None = None):
        self.calls: list[list[dict]] = []
        self._fail_once = set(fail_once or set())
        self._always_fail = set(always_fail or set())
        self._failed_once: set[str] = set()

    def publish_batch(self, *, TopicArn: str, PublishBatchRequestEntries: list[dict]) -> dict:
        self.calls.append([dict(e) for e in PublishBatchRequestEntries])
        successful, failed = [], []
        for entry in PublishBatchRequestEntries:
            eid = entry["Id"]
            if eid in self._always_fail:
                failed.append({"Id": eid, "Code": "InternalError"})
            elif eid in self._fail_once and eid not in self._failed_once:
                self._failed_once.add(eid)
                failed.append({"Id": eid, "Code": "Throttled"})
            else:
                successful.append({"Id": eid})
        return {"Successful": successful, "Failed": failed}


def test_is_a_sink() -> None:
    assert isinstance(SNSSink("arn", client=FakeSNS()), Sink)


def test_one_message_per_event() -> None:
    client = FakeSNS()
    SNSSink("arn", client=client).emit([{"a": 1}, {"a": 2}])
    entries = client.calls[0]
    assert [e["Id"] for e in entries] == ["0", "1"]
    assert json.loads(entries[0]["Message"]) == {"a": 1}


def test_chunks_by_ten_entries() -> None:
    client = FakeSNS()
    SNSSink("arn", client=client).emit([{"i": i} for i in range(25)])
    assert [len(call) for call in client.calls] == [10, 10, 5]


def test_oversized_message_is_dropped(capsys) -> None:
    client = FakeSNS()
    sink = SNSSink("arn", client=client)
    sink.emit([{"pad": "x" * (256 * 1024 + 50)}, {"ok": 1}])
    assert sink.dropped_oversized == 1
    assert "lost 1 event(s)" in capsys.readouterr().err
    assert len(client.calls[0]) == 1


def test_failed_entries_retried_then_succeed() -> None:
    client = FakeSNS(fail_once={"0"})
    sink = SNSSink("arn", client=client)
    sink.emit([{"a": 1}])
    assert len(client.calls) == 2
    assert sink.failed == 0


def test_persistent_failures_counted(capsys) -> None:
    client = FakeSNS(always_fail={"0"})
    sink = SNSSink("arn", client=client, max_retries=2)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}])  # the only entry in the only chunk failed (SPEC-026 FR-001)
    assert len(client.calls) == 3
    assert sink.failed == 1
    assert "lost 1 message(s)" in capsys.readouterr().err


class _Boom(Exception):
    """A client fault shaped like `botocore`'s ClientError / EndpointConnectionError."""


class _Raising:
    """A SNS client that raises on the chosen call numbers, and records what it accepted."""

    def __init__(self, fail_on: set[int]) -> None:
        self.calls = 0
        self.accepted: list[str] = []
        self.fail_on = fail_on

    def _go(self, items: list[str]) -> None:
        self.calls += 1
        if self.calls in self.fail_on:
            raise _Boom("the endpoint could not be reached")
        self.accepted.extend(items)

    def publish_batch(self, *, TopicArn, PublishBatchRequestEntries):
        self._go([e["Message"] for e in PublishBatchRequestEntries])
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
    sink = SNSSink("arn:topic", client=client, max_retries=0)
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
    sink = SNSSink("arn:topic", client=client, max_retries=0)
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

    sink = SNSSink("arn:topic", client=_Interrupting(fail_on=set()), max_retries=0)
    with pytest.raises(KeyboardInterrupt):
        sink.emit([{"i": 1, "trace_id": "t"}])
