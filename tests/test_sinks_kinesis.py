"""SPEC-010 — KinesisSink: 500/5MB chunking, partition keys, FailedRecordCount retry (fake client)."""

from __future__ import annotations

import json

from log_forge.sinks.base import Sink
from log_forge.sinks.kinesis import KinesisSink


class FakeKinesis:
    """Records put_records calls; fails records whose Data is in ``fail_once``/``always_fail``."""

    def __init__(self, fail_once: set[bytes] | None = None, always_fail: set[bytes] | None = None):
        self.calls: list[list[dict]] = []
        self._fail_once = set(fail_once or set())
        self._always_fail = set(always_fail or set())

    def put_records(self, *, StreamName: str, Records: list[dict]) -> dict:
        self.calls.append([dict(r) for r in Records])
        results, failed = [], 0
        for record in Records:
            data = record["Data"]
            if data in self._always_fail:
                results.append({"ErrorCode": "InternalFailure"})
                failed += 1
            elif data in self._fail_once:
                self._fail_once.discard(data)
                results.append({"ErrorCode": "ProvisionedThroughputExceededException"})
                failed += 1
            else:
                results.append({"SequenceNumber": "1", "ShardId": "shard-0"})
        return {"FailedRecordCount": failed, "Records": results}


def test_is_a_sink() -> None:
    assert isinstance(KinesisSink("stream", client=FakeKinesis()), Sink)


def test_one_record_per_event_with_partition_key() -> None:
    client = FakeKinesis()
    KinesisSink("stream", client=client).emit(
        [{"trace_id": "t1", "a": 1}, {"trace_id": "t2", "a": 2}]
    )
    records = client.calls[0]
    assert len(records) == 2
    assert json.loads(records[0]["Data"]) == {"trace_id": "t1", "a": 1}
    assert records[0]["PartitionKey"] == "t1"


def test_partition_key_falls_back_when_field_absent() -> None:
    client = FakeKinesis()
    KinesisSink("stream", client=client).emit([{"a": 1}])
    assert client.calls[0][0]["PartitionKey"] == "log-forge"


def test_chunks_by_record_count() -> None:
    client = FakeKinesis()
    KinesisSink("stream", client=client).emit([{"i": i} for i in range(1001)])
    assert [len(call) for call in client.calls] == [500, 500, 1]


def test_oversized_record_is_dropped() -> None:
    client = FakeKinesis()
    sink = KinesisSink("stream", client=client)
    sink.emit([{"pad": "x" * (1024 * 1024 + 50)}, {"ok": 1}])
    assert sink.dropped_oversized == 1
    assert len(client.calls[0]) == 1  # only the in-limit record was sent


def test_failed_records_are_retried_then_succeed() -> None:
    body = json.dumps({"trace_id": "t1", "a": 1}).encode("utf-8")
    client = FakeKinesis(fail_once={body})
    sink = KinesisSink("stream", client=client)
    sink.emit([{"trace_id": "t1", "a": 1}])
    assert len(client.calls) == 2  # initial + one retry of the failed record
    assert sink.failed == 0


def test_persistent_failures_are_counted() -> None:
    body = json.dumps({"trace_id": "t1", "a": 1}).encode("utf-8")
    client = FakeKinesis(always_fail={body})
    sink = KinesisSink("stream", client=client, max_retries=2)
    sink.emit([{"trace_id": "t1", "a": 1}])
    assert len(client.calls) == 3  # initial + 2 retries
    assert sink.failed == 1
