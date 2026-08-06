"""SPEC-010 — FirehoseSink: 500/4MB chunking, FailedPutCount retry (fake client).

SPEC-018 adds the mismatched-response cases: a RequestResponses array that does not describe the
chunk.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from log_foundry.sinks.base import Sink
from log_foundry.sinks.firehose import FirehoseSink

if TYPE_CHECKING:
    import pytest


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


def test_oversized_record_is_dropped(capsys) -> None:
    client = FakeFirehose()
    sink = FirehoseSink("stream", client=client)
    sink.emit([{"pad": "x" * (1024 * 1024 + 50)}, {"ok": 1}])
    assert sink.dropped_oversized == 1
    assert "lost 1 event(s)" in capsys.readouterr().err
    assert len(client.calls[0]) == 1


def test_failed_entries_are_retried_then_succeed() -> None:
    body = json.dumps({"a": 1}).encode("utf-8")
    client = FakeFirehose(fail_once={body})
    sink = FirehoseSink("stream", client=client)
    sink.emit([{"a": 1}])
    assert len(client.calls) == 2
    assert sink.failed == 0


def test_persistent_failures_are_counted(capsys) -> None:
    body = json.dumps({"a": 1}).encode("utf-8")
    client = FakeFirehose(always_fail={body})
    sink = FirehoseSink("stream", client=client, max_retries=1)
    sink.emit([{"a": 1}])
    assert len(client.calls) == 2
    assert sink.failed == 1
    assert "lost 1 record(s)" in capsys.readouterr().err


# -- SPEC-018: responses that don't describe the chunk ------------------------------------


class MalformedFirehose:
    """Reports failures but returns RequestResponses of a length that does not match the request."""

    def __init__(self, responses: list[dict] | None) -> None:
        self.calls: list[list[dict]] = []
        self._responses = responses

    def put_record_batch(self, *, DeliveryStreamName: str, Records: list[dict]) -> dict:
        self.calls.append([dict(r) for r in Records])
        response: dict = {"FailedPutCount": 1}
        if self._responses is not None:
            response["RequestResponses"] = self._responses
        return response


def test_short_response_abandons_the_whole_chunk(capsys: pytest.CaptureFixture[str]) -> None:
    client = MalformedFirehose([{"ErrorCode": "ServiceUnavailableException"}])
    sink = FirehoseSink("stream", client=client)
    sink.emit([{"a": 1}, {"a": 2}, {"a": 3}])
    assert sink.dropped_unadjudicated == 3
    assert sink.failed == 0
    assert len(client.calls) == 1  # the retry loop stopped; nothing was re-sent
    err = capsys.readouterr().err
    assert err.count("\n") == 1
    assert err.startswith("log-foundry: lost 3 record(s)"), "the count leads the line"
    assert "FirehoseSink" in err
    assert "3 record(s) sent, 1 result(s) returned" in err
    assert "abandoned, not retried" in err


def test_long_response_abandons_the_whole_chunk(capsys: pytest.CaptureFixture[str]) -> None:
    client = MalformedFirehose([{"ErrorCode": "ServiceUnavailableException"}] * 3)
    sink = FirehoseSink("stream", client=client)
    sink.emit([{"a": 1}])
    assert sink.dropped_unadjudicated == 1
    assert len(client.calls) == 1
    assert "1 record(s) sent, 3 result(s) returned" in capsys.readouterr().err


def test_absent_responses_abandon_the_whole_chunk(capsys: pytest.CaptureFixture[str]) -> None:
    client = MalformedFirehose(None)
    sink = FirehoseSink("stream", client=client)
    sink.emit([{"a": 1}, {"a": 2}])
    assert sink.dropped_unadjudicated == 2
    assert len(client.calls) == 1
    assert "2 record(s) sent, 0 result(s) returned" in capsys.readouterr().err


def test_an_unusable_responses_field_abandons_and_counts(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A RequestResponses field that isn't a list of mappings is counted, never raised."""

    class UnusableFirehose(MalformedFirehose):
        def put_record_batch(self, *, DeliveryStreamName: str, Records: list[dict]) -> dict:
            super().put_record_batch(DeliveryStreamName=DeliveryStreamName, Records=Records)
            return {"FailedPutCount": 1, "RequestResponses": None}  # present, but not a list

    client = UnusableFirehose(None)
    sink = FirehoseSink("stream", client=client)
    sink.emit([{"a": 1}, {"a": 2}])
    assert sink.dropped_unadjudicated == 2
    assert sink.failed == 0
    assert len(client.calls) == 1
    assert "2 record(s) sent, 0 result(s) returned" in capsys.readouterr().err


def test_no_failures_reported_never_consults_the_responses_array(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A fake client that reports zero failures stays silent however its responses array looks."""

    class SilentFirehose(MalformedFirehose):
        def put_record_batch(self, *, DeliveryStreamName: str, Records: list[dict]) -> dict:
            super().put_record_batch(DeliveryStreamName=DeliveryStreamName, Records=Records)
            return {"FailedPutCount": 0}  # no RequestResponses key at all

    sink = FirehoseSink("stream", client=SilentFirehose(None))
    sink.emit([{"a": 1}, {"a": 2}])
    assert sink.dropped_unadjudicated == 0
    assert capsys.readouterr().err == ""


def test_emit_does_not_raise_on_an_unadjudicated_chunk() -> None:
    FirehoseSink("stream", client=MalformedFirehose(None)).emit([{"a": 1}])  # no exception


def test_well_formed_responses_count_nothing_and_say_nothing(
    capsys: pytest.CaptureFixture[str],
) -> None:
    body = json.dumps({"a": 1}).encode("utf-8")
    sink = FirehoseSink("stream", client=FakeFirehose(fail_once={body}))
    sink.emit([{"a": 1}, {"a": 2}])
    assert sink.dropped_unadjudicated == 0
    assert capsys.readouterr().err == ""
