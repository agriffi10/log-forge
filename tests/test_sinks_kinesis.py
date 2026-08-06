"""SPEC-010 — KinesisSink: 500/5MB chunking, partition keys, FailedRecordCount retry (fake client).

SPEC-018 adds the mismatched-response cases: a results array that does not describe the chunk.
"""

from __future__ import annotations

import json

import pytest

from log_foundry.sinks.base import Sink, SinkDeliveryError
from log_foundry.sinks.kinesis import KinesisSink


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
    assert client.calls[0][0]["PartitionKey"] == "log-foundry"


def test_chunks_by_record_count() -> None:
    client = FakeKinesis()
    KinesisSink("stream", client=client).emit([{"i": i} for i in range(1001)])
    assert [len(call) for call in client.calls] == [500, 500, 1]


def test_oversized_record_is_dropped(capsys) -> None:
    client = FakeKinesis()
    sink = KinesisSink("stream", client=client)
    sink.emit([{"pad": "x" * (1024 * 1024 + 50)}, {"ok": 1}])
    assert sink.dropped_oversized == 1
    assert "lost 1 event(s)" in capsys.readouterr().err
    assert len(client.calls[0]) == 1  # only the in-limit record was sent


def test_failed_records_are_retried_then_succeed() -> None:
    body = json.dumps({"trace_id": "t1", "a": 1}).encode("utf-8")
    client = FakeKinesis(fail_once={body})
    sink = KinesisSink("stream", client=client)
    sink.emit([{"trace_id": "t1", "a": 1}])
    assert len(client.calls) == 2  # initial + one retry of the failed record
    assert sink.failed == 0


def test_persistent_failures_are_counted(capsys) -> None:
    body = json.dumps({"trace_id": "t1", "a": 1}).encode("utf-8")
    client = FakeKinesis(always_fail={body})
    sink = KinesisSink("stream", client=client, max_retries=2)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"trace_id": "t1", "a": 1}])  # the only record in the only chunk failed (SPEC-026 FR-001)
    assert len(client.calls) == 3  # initial + 2 retries
    assert sink.failed == 1
    assert "lost 1 record(s)" in capsys.readouterr().err


# -- SPEC-018: responses that don't describe the chunk ------------------------------------


class MalformedKinesis:
    """Reports failures but returns a Records array of a length that does not match the request."""

    def __init__(self, results: list[dict] | None) -> None:
        self.calls: list[list[dict]] = []
        self._results = results

    def put_records(self, *, StreamName: str, Records: list[dict]) -> dict:
        self.calls.append([dict(r) for r in Records])
        response: dict = {"FailedRecordCount": 1}
        if self._results is not None:
            response["Records"] = self._results
        return response


def test_short_response_abandons_the_whole_chunk(capsys: pytest.CaptureFixture[str]) -> None:
    client = MalformedKinesis([{"ErrorCode": "InternalFailure"}])
    sink = KinesisSink("stream", client=client)
    sink.emit([{"a": 1}, {"a": 2}, {"a": 3}])
    assert sink.dropped_unadjudicated == 3  # every record sent, not just the unpaired tail
    assert sink.failed == 0  # `failed` still means "the stream told us these failed"
    assert len(client.calls) == 1  # the retry loop stopped; nothing was re-sent
    err = capsys.readouterr().err
    assert err.count("\n") == 1
    assert err.startswith("log-foundry: lost 3 record(s)"), "the count leads the line"
    assert "KinesisSink" in err
    assert "3 record(s) sent, 1 result(s) returned" in err
    assert "abandoned, not retried" in err


def test_long_response_abandons_the_whole_chunk(capsys: pytest.CaptureFixture[str]) -> None:
    client = MalformedKinesis([{"ErrorCode": "InternalFailure"}] * 3)
    sink = KinesisSink("stream", client=client)
    sink.emit([{"a": 1}])
    assert sink.dropped_unadjudicated == 1
    assert len(client.calls) == 1
    assert "1 record(s) sent, 3 result(s) returned" in capsys.readouterr().err


def test_absent_results_abandon_the_whole_chunk(capsys: pytest.CaptureFixture[str]) -> None:
    client = MalformedKinesis(None)
    sink = KinesisSink("stream", client=client)
    sink.emit([{"a": 1}, {"a": 2}])
    assert sink.dropped_unadjudicated == 2
    assert len(client.calls) == 1
    assert "2 record(s) sent, 0 result(s) returned" in capsys.readouterr().err


def test_an_unusable_results_field_abandons_and_counts(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A Records field that isn't a list of mappings is counted like a mismatch, never raised."""

    class UnusableKinesis(MalformedKinesis):
        def put_records(self, *, StreamName: str, Records: list[dict]) -> dict:
            super().put_records(StreamName=StreamName, Records=Records)
            return {"FailedRecordCount": 1, "Records": None}  # present, but not a list

    client = UnusableKinesis(None)
    sink = KinesisSink("stream", client=client)
    sink.emit([{"a": 1}, {"a": 2}])
    assert sink.dropped_unadjudicated == 2
    assert sink.failed == 0
    assert len(client.calls) == 1
    assert "2 record(s) sent, 0 result(s) returned" in capsys.readouterr().err


def test_no_failures_reported_never_consults_the_results_array(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A fake client that reports zero failures stays silent however its Records array looks."""

    class SilentKinesis(MalformedKinesis):
        def put_records(self, *, StreamName: str, Records: list[dict]) -> dict:
            super().put_records(StreamName=StreamName, Records=Records)
            return {"FailedRecordCount": 0}  # no Records key at all

    sink = KinesisSink("stream", client=SilentKinesis(None))
    sink.emit([{"a": 1}, {"a": 2}])
    assert sink.dropped_unadjudicated == 0
    assert capsys.readouterr().err == ""


def test_emit_does_not_raise_on_an_unadjudicated_chunk() -> None:
    KinesisSink("stream", client=MalformedKinesis(None)).emit([{"a": 1}])  # no exception


def test_well_formed_responses_count_nothing_and_say_nothing(
    capsys: pytest.CaptureFixture[str],
) -> None:
    body = json.dumps({"trace_id": "t1", "a": 1}).encode("utf-8")
    sink = KinesisSink("stream", client=FakeKinesis(fail_once={body}))
    sink.emit([{"trace_id": "t1", "a": 1}, {"trace_id": "t2", "a": 2}])
    assert sink.dropped_unadjudicated == 0
    assert capsys.readouterr().err == ""
