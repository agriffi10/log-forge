"""SPEC-010 — FirehoseSink: 500/4MB chunking, FailedPutCount retry (fake client).

SPEC-018 adds the mismatched-response cases: a RequestResponses array that does not describe the
chunk.
"""

from __future__ import annotations

import json

import pytest

from log_foundry.sinks.base import Sink, SinkDeliveryError
from log_foundry.sinks.firehose import FirehoseSink


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch):
    """Neutralize the retry backoff SPEC-027 FR-003 added, so retry tests run instantly.

    ``wait`` is bound into each sink at import, and its Event branch never reaches ``time.sleep``
    — patching either centrally would leave this fixture inert.
    """
    monkeypatch.setattr("log_foundry.sinks.firehose.wait", lambda _delay, _stop=None: None)


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
    body = json.dumps({"a": 1}).encode("utf-8") + b"\n"   # FR-005 delimiter
    client = FakeFirehose(fail_once={body})
    sink = FirehoseSink("stream", client=client)
    sink.emit([{"a": 1}])
    assert len(client.calls) == 2
    assert sink.failed == 0


def test_persistent_failures_are_counted(capsys) -> None:
    body = json.dumps({"a": 1}).encode("utf-8") + b"\n"   # FR-005 delimiter
    client = FakeFirehose(always_fail={body})
    sink = FirehoseSink("stream", client=client, max_retries=1)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}])  # the only record in the only chunk failed (SPEC-026 FR-001)
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
    body = json.dumps({"a": 1}).encode("utf-8") + b"\n"   # FR-005 delimiter
    sink = FirehoseSink("stream", client=FakeFirehose(fail_once={body}))
    sink.emit([{"a": 1}, {"a": 2}])
    assert sink.dropped_unadjudicated == 0
    assert capsys.readouterr().err == ""


# --- SPEC-038 FR-005 / FR-009: the delimiter and the real per-record ceiling --------------


def test_every_record_ends_with_a_newline_so_the_object_parses_as_ndjson() -> None:
    """AC-1 + AC-3. Firehose concatenates payloads verbatim; the producer supplies the separator.

    Without it an S3 object reads `{"a":1}{"b":2}` — unparseable by Athena, Glue and OpenSearch
    ingest, and unlike the NDJSON every other sink here emits.
    """
    client = FakeFirehose()
    FirehoseSink("stream", client=client).emit([{"a": 1}, {"b": 2}, {"c": 3}])
    chunk = client.calls[0]
    assert all(record["Data"].endswith(b"\n") for record in chunk)

    concatenated = b"".join(record["Data"] for record in chunk)
    parsed = [json.loads(line) for line in concatenated.splitlines()]
    assert parsed == [{"a": 1}, {"b": 2}, {"c": 3}], "the delivered object parses as NDJSON"


def test_the_newline_is_charged_to_the_per_record_limit() -> None:
    """AC-2. An event one byte under the ceiling is over it once delimited."""
    sink = FirehoseSink("stream", client=FakeFirehose())
    padding = FirehoseSink.MAX_RECORD_BYTES - len(json.dumps({"pad": ""}).encode("utf-8")) - 1
    exactly_full = {"pad": "x" * padding}
    assert len(json.dumps(exactly_full).encode("utf-8")) == FirehoseSink.MAX_RECORD_BYTES - 1

    sink.emit([exactly_full])
    assert sink.dropped_oversized == 0, "one byte of headroom is enough for the newline"

    over = FirehoseSink("stream", client=FakeFirehose())
    over.emit([{"pad": "x" * (padding + 1)}])
    assert over.dropped_oversized == 1, "without headroom the delimiter pushes it over"


def test_the_newline_is_charged_to_the_per_request_budget() -> None:
    """AC-2, the per-request half — and the record size is what decides whether it can fail.

    Two independent ways this test can be vacuous, and it has been both:

    1. `MAX_RECORDS` (500) and `MAX_REQUEST_BYTES` (4 MiB) both bound a chunk, so with small
       records the *count* binds first and the byte budget never engages. The first version used
       ~1,012-byte records and passed with the delimiter removed entirely.
    2. Above 4 MiB / 500 = 8,389 bytes the byte budget binds — but the chunk size is a *floor
       division*, so one byte per record only changes it at the sizes where the division tips.
       The second version used 9,000-byte records: 4194304 // 9012 == 4194304 // 9011 == 465, so
       the delimiter still made no difference and un-charging it from `size_of` left the whole
       file green.

    8,989 is chosen so the record is 9,001 bytes: 465 per chunk with the delimiter charged, 466
    without. Only 359 of the sizes between 8,400 and 30,000 are sensitive that way, which is why
    picking a round number twice landed on an insensitive one.
    """
    client = FakeFirehose()
    sink = FirehoseSink("stream", client=client)
    payload = 8_989  # 9,001 bytes per record: see the docstring
    count = 900
    sink.emit([{"pad": "x" * payload} for _ in range(count)])

    assert all(len(chunk) < FirehoseSink.MAX_RECORDS for chunk in client.calls), (
        "the byte budget must be what bounds these chunks, or the delimiter never matters"
    )
    for chunk in client.calls:
        assert sum(len(r["Data"]) for r in chunk) <= FirehoseSink.MAX_REQUEST_BYTES

    # One record's true wire size, delimiter included, decides how many fit. Computed from the
    # event rather than read back from the request, so an uncharged delimiter shifts the answer.
    one = len(json.dumps({"pad": "x" * payload}).encode("utf-8")) + 1
    assert FirehoseSink.MAX_REQUEST_BYTES // one != FirehoseSink.MAX_REQUEST_BYTES // (one - 1), (
        "the fixture must sit where one byte per record moves the chunk boundary, or this "
        "assertion cannot fail"
    )
    assert len(client.calls[0]) == FirehoseSink.MAX_REQUEST_BYTES // one, (
        "the first chunk holds exactly as many delimited records as the budget allows"
    )


def test_the_per_record_ceiling_is_the_documented_1000_kib_not_1_mib() -> None:
    """FR-009 AC-1. The 24,576-byte gap was a band the service rejected and this sink passed."""
    assert FirehoseSink.MAX_RECORD_BYTES == 1_024_000
    assert FirehoseSink.MAX_RECORD_BYTES != 1024 * 1024


def test_a_record_between_the_two_ceilings_is_dropped_rather_than_sent_to_be_rejected() -> None:
    """FR-009 AC-3, at the boundary that mattered: one byte under and one byte over."""
    under = FirehoseSink("stream", client=(ok := FakeFirehose()))
    pad = FirehoseSink.MAX_RECORD_BYTES - len(json.dumps({"pad": ""}).encode("utf-8")) - 1
    under.emit([{"pad": "x" * pad}])
    assert under.dropped_oversized == 0 and len(ok.calls) == 1

    over = FirehoseSink("stream", client=(none := FakeFirehose()))
    over.emit([{"pad": "x" * (pad + 1)}])
    assert over.dropped_oversized == 1, "one byte over the real ceiling never reaches the wire"
    assert none.calls == []


class _Boom(Exception):
    """A client fault shaped like `botocore`'s ClientError / EndpointConnectionError."""


class _Raising:
    """A Firehose client that raises on the chosen call numbers, and records what it accepted."""

    def __init__(self, fail_on: set[int]) -> None:
        self.calls = 0
        self.accepted: list[str] = []
        self.fail_on = fail_on

    def _go(self, items: list[str]) -> None:
        self.calls += 1
        if self.calls in self.fail_on:
            raise _Boom("the endpoint could not be reached")
        self.accepted.extend(items)

    def put_record_batch(self, *, DeliveryStreamName, Records):
        self._go([bytes(r["Data"]).decode() for r in Records])
        return {"FailedPutCount": 0, "RequestResponses": [{} for _ in Records]}


def test_a_client_failure_costs_its_chunk_not_the_batch(capsys) -> None:
    """SPEC-048 FR-002. The client call was unguarded, so a fault mid-batch duplicated the rest.

    A `ClientError` on chunk N propagated out of `emit` after chunks 1..N-1 had landed, and the
    worker retries whole batches -- so the exit drain, which is one large batch by construction,
    re-sent everything already delivered. Measured before the fix: 1,000 records in 2 chunks, `duplicates=500 losses=(0, 0)`.

    The criterion that binds is that `emit` **returns**: that is what stops the worker's retry at
    its source, so the duplication cannot happen rather than being cleaned up afterwards.
    """
    client = _Raising(fail_on={2})
    sink = FirehoseSink("ds", client=client, max_retries=0)
    sink.emit([{"i": i, "trace_id": f"t{i}"} for i in range(1000)])
    assert len(client.accepted) == len(set(client.accepted)) == 500, (
        "the chunks that landed are delivered exactly once"
    )
    assert sink.losses().failed == 500, "and the failed chunk is counted, not silent"
    assert "_Boom" in capsys.readouterr().err, "and announced by exception type"


def test_a_total_client_failure_still_raises() -> None:
    """A wholly-failed batch must still reach the worker's retry -- nothing landed to duplicate.

    **This is the mutation-sensitive one.** With the guard added and the failure not fed back
    into the total-failure test, `emit` returns normally having lost the entire batch with
    `losses()` reading zero -- which is precisely the "a sink that absorbs a total failure is a
    sink the worker believes" shape SPEC-026 exists to end.
    """
    client = _Raising(fail_on=set(range(1, 500)))
    sink = FirehoseSink("ds", client=client, max_retries=0)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"i": i, "trace_id": f"t{i}"} for i in range(1000)])
    assert client.accepted == [], "nothing landed"
    assert sink.losses().failed > 0, "and every abandoned item is counted"


def test_a_keyboard_interrupt_is_not_absorbed_by_the_chunk_guard() -> None:
    """The guard catches `Exception`, never `BaseException` (SPEC-025 FR-004).

    An operator's Ctrl-C and the runtime's `SystemExit` are intent, and must reach the caller.
    """

    class _Interrupting(_Raising):
        def _go(self, items: list[str]) -> None:
            raise KeyboardInterrupt

    sink = FirehoseSink("ds", client=_Interrupting(fail_on=set()), max_retries=0)
    with pytest.raises(KeyboardInterrupt):
        sink.emit([{"i": 1, "trace_id": "t"}])
