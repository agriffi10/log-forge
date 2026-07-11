"""SPEC-010 — FirehoseSink: 500/4MB chunking, FailedPutCount retry (fake client)."""

from __future__ import annotations

import json

from log_forge.sinks.base import Sink
from log_forge.sinks.firehose import FirehoseSink


class FakeFirehose:
    """Records put_record_batch calls; fails records whose Data is flagged."""

    def __init__(self, fail_once: set[bytes] | None = None, always_fail: set[bytes] | None = None):
        self.calls: list[list[dict]] = []
        self._fail_once = set(fail_once or set())
        self._always_fail = set(always_fail or set())

    def put_record_batch(self, *, DeliveryStreamName: str, Records: list[dict]) -> dict:
        self.calls.append([dict(r) for r in Records])
        responses, failed = [], 0
        for record in Records:
            data = record["Data"]
            if data in self._always_fail:
                responses.append({"ErrorCode": "ServiceUnavailableException"})
                failed += 1
            elif data in self._fail_once:
                self._fail_once.discard(data)
                responses.append({"ErrorCode": "ServiceUnavailableException"})
                failed += 1
            else:
                responses.append({"RecordId": "r-1"})
        return {"FailedPutCount": failed, "RequestResponses": responses}


def test_is_a_sink() -> None:
    assert isinstance(FirehoseSink("stream", client=FakeFirehose()), Sink)


def test_one_record_per_event_no_partition_key() -> None:
    client = FakeFirehose()
    FirehoseSink("stream", client=client).emit([{"a": 1}, {"a": 2}])
    records = client.calls[0]
    assert len(records) == 2
    assert json.loads(records[0]["Data"]) == {"a": 1}
    assert "PartitionKey" not in records[0]


def test_chunks_by_record_count() -> None:
    client = FakeFirehose()
    FirehoseSink("stream", client=client).emit([{"i": i} for i in range(1001)])
    assert [len(call) for call in client.calls] == [500, 500, 1]


def test_oversized_record_is_dropped() -> None:
    client = FakeFirehose()
    sink = FirehoseSink("stream", client=client)
    sink.emit([{"pad": "x" * (1024 * 1024 + 50)}, {"ok": 1}])
    assert sink.dropped_oversized == 1
    assert len(client.calls[0]) == 1


def test_failed_entries_are_retried_then_succeed() -> None:
    body = json.dumps({"a": 1}).encode("utf-8")
    client = FakeFirehose(fail_once={body})
    sink = FirehoseSink("stream", client=client)
    sink.emit([{"a": 1}])
    assert len(client.calls) == 2
    assert sink.failed == 0


def test_persistent_failures_are_counted() -> None:
    body = json.dumps({"a": 1}).encode("utf-8")
    client = FakeFirehose(always_fail={body})
    sink = FirehoseSink("stream", client=client, max_retries=1)
    sink.emit([{"a": 1}])
    assert len(client.calls) == 2
    assert sink.failed == 1
