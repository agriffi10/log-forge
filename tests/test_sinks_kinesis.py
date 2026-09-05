"""SPEC-010 — KinesisSink: 500/5MB chunking, partition keys, FailedRecordCount retry (fake client).

SPEC-018 adds the mismatched-response cases: a results array that does not describe the chunk.
"""

from __future__ import annotations

import json

import pytest

from log_foundry.sinks.base import Sink, SinkDeliveryError
from log_foundry.sinks.kinesis import KinesisSink


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch):
    """Neutralize the retry backoff SPEC-027 FR-003 added, so retry tests run instantly.

    ``wait`` is bound into each sink at import, and its Event branch never reaches ``time.sleep``
    — patching either centrally would leave this fixture inert.
    """
    monkeypatch.setattr("log_foundry.sinks.kinesis.wait", lambda _delay, _stop=None: None)


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


# --- SPEC-038 FR-009: the request budget must charge the partition key --------------------


def test_the_request_budget_charges_the_partition_key_it_sends() -> None:
    """AC-2. `PutRecords` charges the key against the 5 MiB limit; ignoring it understates.

    The record size matters to whether this test can fail at all. `MAX_RECORDS` (500) and
    `MAX_REQUEST_BYTES` both bound a chunk, and with small records the *count* binds first — so
    the key is invisible and the test passes against the defect. At 20,000 bytes the byte limit
    binds at 262 records, whose keys add 67 KB against only 2,880 bytes of slack. A first version
    of this test used 9,000-byte records, where the count caps the chunk at 500 and the assertion
    could never fail.
    """
    client = FakeKinesis()
    sink = KinesisSink("stream", client=client, partition_key_field="pk")
    sink.emit([{"pk": "k" * 256, "pad": "x" * 20_000} for _ in range(1500)])

    assert all(len(chunk) < KinesisSink.MAX_RECORDS for chunk in client.calls), (
        "the byte limit must be what bounds these chunks, or the key never enters the sum"
    )
    for chunk in client.calls:
        charged = sum(len(r["Data"]) + len(r["PartitionKey"]) for r in chunk)
        assert charged <= KinesisSink.MAX_REQUEST_BYTES, (
            f"a chunk exceeded the real budget once its keys are counted: {charged}"
        )


def test_the_chunks_stay_packed_once_the_key_is_charged() -> None:
    """AC-3. The fix must not be a blanket over-reservation that halves throughput."""
    client = FakeKinesis()
    sink = KinesisSink("stream", client=client, partition_key_field="pk")
    sink.emit([{"pk": "k" * 256, "pad": "x" * 20_000} for _ in range(1500)])
    worst = max(
        sum(len(r["Data"]) + len(r["PartitionKey"]) for r in chunk) for chunk in client.calls
    )
    assert worst <= KinesisSink.MAX_REQUEST_BYTES
    assert worst > KinesisSink.MAX_REQUEST_BYTES - 40_000, (
        f"chunks are packed to within one record of the budget, not split conservatively: {worst}"
    )


def test_firehose_has_no_partition_key_to_charge() -> None:
    """AC-2's other half: this applies to Kinesis only, so Firehose's sizing is unchanged."""
    from log_foundry.sinks.firehose import FirehoseSink
    from test_sinks_firehose import FakeFirehose

    client = FakeFirehose()
    FirehoseSink("stream", client=client).emit([{"a": 1}])
    assert set(client.calls[0][0]) == {"Data"}, "a Firehose record carries no PartitionKey"


class _Boom(Exception):
    """A client fault shaped like `botocore`'s ClientError / EndpointConnectionError."""


class _Raising:
    """A Kinesis client that raises on the chosen call numbers, and records what it accepted."""

    def __init__(self, fail_on: set[int]) -> None:
        self.calls = 0
        self.accepted: list[str] = []
        self.fail_on = fail_on

    def _go(self, items: list[str]) -> None:
        self.calls += 1
        if self.calls in self.fail_on:
            raise _Boom("the endpoint could not be reached")
        self.accepted.extend(items)

    def put_records(self, *, StreamName, Records):
        self._go([bytes(r["Data"]).decode() for r in Records])
        return {"FailedRecordCount": 0, "Records": [{} for _ in Records]}


def test_a_client_failure_costs_its_chunk_not_the_batch(capsys) -> None:
    """SPEC-048 FR-002. The client call was unguarded, so a fault mid-batch duplicated the rest.

    A `ClientError` on chunk N propagated out of `emit` after chunks 1..N-1 had landed, and the
    worker retries whole batches -- so the exit drain, which is one large batch by construction,
    re-sent everything already delivered. Measured before the fix: 1,000 records in 2 chunks, `duplicates=500 losses=(0, 0)`.

    The criterion that binds is that `emit` **returns**: that is what stops the worker's retry at
    its source, so the duplication cannot happen rather than being cleaned up afterwards.
    """
    client = _Raising(fail_on={2})
    sink = KinesisSink("stream", client=client, max_retries=0)
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
    sink = KinesisSink("stream", client=client, max_retries=0)
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

    sink = KinesisSink("stream", client=_Interrupting(fail_on=set()), max_retries=0)
    with pytest.raises(KeyboardInterrupt):
        sink.emit([{"i": 1, "trace_id": "t"}])


def test_the_per_record_ceiling_charges_the_partition_key(capsys) -> None:
    """SPEC-048 FR-003. `PutRecords` bills the key against the per-record limit; the check did not.

    Measured before the fix: `data=1048576 + key=200` passed the sink's check and went to the
    wire, where the service rejects the whole `PutRecords` call -- which, via the unguarded
    client call FR-002 closes, then duplicated every earlier chunk.
    """
    client = FakeKinesis()
    sink = KinesisSink("stream", client=client, partition_key_field="trace_id")
    key = "k" * 200
    probe = {"trace_id": key, "pad": ""}
    overhead = len(json.dumps(probe).encode())
    event = {"trace_id": key, "pad": "x" * (sink.MAX_RECORD_BYTES - overhead)}

    sink.emit([event])
    assert client.calls == [], "the record is dropped before any client call"
    assert sink.losses().dropped == 1
    err = capsys.readouterr().err
    assert "1048776 bytes with its partition key" in err, (
        "the diagnostic reports the total charged, not the data length alone"
    )


def test_a_record_exactly_at_the_ceiling_including_its_key_is_sent() -> None:
    """The boundary is inclusive: data + key == the limit still goes."""
    client = FakeKinesis()
    sink = KinesisSink("stream", client=client, partition_key_field="trace_id")
    key = "k" * 200
    probe = {"trace_id": key, "pad": ""}
    overhead = len(json.dumps(probe).encode())
    event = {"trace_id": key, "pad": "x" * (sink.MAX_RECORD_BYTES - overhead - len(key))}

    sink.emit([event])
    assert len(client.calls) == 1, "a record exactly at the ceiling is sent"
    assert sink.losses().dropped == 0


def test_a_partition_key_is_charged_and_bounded_in_utf8_bytes() -> None:
    """Bytes, not characters. The two differ for any non-ASCII key and the service bills bytes."""
    from log_foundry.sinks.kinesis import MAX_PARTITION_KEY_BYTES, _partition_key, _record_size

    multibyte = "é" * 200  # 200 characters, 400 UTF-8 bytes
    assert len(multibyte) == 200 and len(multibyte.encode()) == 400

    bounded = _partition_key(multibyte)
    encoded = bounded.encode("utf-8")
    assert len(encoded) <= MAX_PARTITION_KEY_BYTES, "bounded on bytes, not on characters"
    assert bounded == encoded.decode("utf-8"), "and the cut leaves valid UTF-8"

    record = {"Data": b"x" * 10, "PartitionKey": bounded}
    assert _record_size(record) == 10 + len(encoded), (
        "the request budget charges the key's byte length too"
    )
    assert _record_size(record) != 10 + len(bounded), "which differs from its character length"


def test_a_lone_surrogate_in_the_partition_key_does_not_raise_out_of_emit() -> None:
    """A bare encode on a lone surrogate raises, and the key is built after assembly's guarantee.

    Assembly replaces a lone surrogate (SPEC-055 FR-001), but this event dict never went through
    it -- a `TransformSink` or a caller's own sink can hand `emit` a rewritten batch -- and a
    `UnicodeEncodeError` escaping `emit` is precisely the raw, uncounted failure SPEC-048
    exists to remove. Both encodes carry `errors=` for this.
    """
    client = FakeKinesis()
    sink = KinesisSink("stream", client=client, partition_key_field="trace_id")
    sink.emit([{"trace_id": "\udcff-bad", "i": 1}])
    assert len(client.calls) == 1, "the record still goes"
