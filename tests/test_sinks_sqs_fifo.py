"""SPEC-016 — SQSSink FIFO support: detection, group/dedup ids, byte budget.

Companion to `test_sinks_sqs.py`, which stays untouched as the FR-004 regression guard: every
test there runs on a standard queue and must keep passing byte-for-byte.
"""

from __future__ import annotations

import json

import pytest

from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.sqs import DEFAULT_GROUP_ID, MAX_ID_LEN, SQSSink


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Neutralize backoff sleeps (SPEC-027 FR-003 gave SQSSink one) so retry tests run instantly."""
    # ``wait`` is bound into each sink at import, and its Event branch never reaches
    # ``time.sleep`` — patching either centrally would leave this fixture inert.
    monkeypatch.setattr("log_foundry.sinks.sqs.wait", lambda _delay, _stop=None: None)

FIFO_URL = "https://sqs.example/q.fifo"
STD_URL = "https://sqs.example/q"


class RecordingClient:
    """Accepts everything; records the entries of every send_message_batch call."""

    def __init__(self) -> None:
        self.calls: list[list[dict]] = []

    def send_message_batch(self, *, QueueUrl: str, Entries: list[dict]) -> dict:
        self.calls.append([dict(e) for e in Entries])
        return {"Successful": [{"Id": e["Id"]} for e in Entries], "Failed": []}

    @property
    def entries(self) -> list[dict]:
        """Every entry across every call, flattened."""
        return [entry for call in self.calls for entry in call]


def _event(trace_id: str = "a" * 32, log_id: str = "log-1", **extra: object) -> dict:
    return {"message": "span.end", "trace_id": trace_id, "log_id": log_id, **extra}


def _sized_event(target: int, trace_id: str, log_id: str) -> dict:
    """An event whose json.dumps is exactly `target` bytes, padded to order."""
    base = {"trace_id": trace_id, "log_id": log_id, "pad": ""}
    return {**base, "pad": "x" * (target - len(json.dumps(base)))}


# -- FR-001: FIFO detection with an explicit override -----------------------------------


def test_fifo_url_suffix_detected() -> None:
    assert SQSSink(FIFO_URL, client=RecordingClient()).fifo is True


def test_standard_url_is_not_fifo() -> None:
    assert SQSSink(STD_URL, client=RecordingClient()).fifo is False


def test_explicit_fifo_true_forces_it_on_a_plain_url() -> None:
    assert SQSSink(STD_URL, client=RecordingClient(), fifo=True).fifo is True


def test_explicit_fifo_false_forces_it_off_on_a_fifo_url() -> None:
    assert SQSSink(FIFO_URL, client=RecordingClient(), fifo=False).fifo is False


def test_detection_is_case_sensitive() -> None:
    # AWS documents the suffix as lowercase '.fifo'; anything else is not a FIFO queue.
    assert SQSSink("https://sqs.example/q.FIFO", client=RecordingClient()).fifo is False


def test_detection_happens_once_at_construction() -> None:
    sink = SQSSink(STD_URL, client=RecordingClient())
    sink.queue_url = FIFO_URL  # mutating the URL afterwards must not re-decide
    assert sink.fifo is False


# -- FR-002: MessageGroupId per entry, defaulting to trace_id ---------------------------


def test_every_fifo_entry_carries_a_non_empty_group_id() -> None:
    client = RecordingClient()
    SQSSink(FIFO_URL, client=client).emit([_event() for _ in range(3)])
    assert all(e["MessageGroupId"] for e in client.entries)


def test_group_id_defaults_to_the_events_own_trace_id() -> None:
    client = RecordingClient()
    SQSSink(FIFO_URL, client=client).emit([_event(trace_id="b" * 32)])
    assert client.entries[0]["MessageGroupId"] == "b" * 32


def test_three_traces_yield_three_groups_in_one_request() -> None:
    client = RecordingClient()
    events = [_event(trace_id=t * 32, log_id=f"log-{t}") for t in ("a", "b", "c")]
    SQSSink(FIFO_URL, client=client).emit(events)

    assert len(client.calls) == 1
    pairs = {(e["MessageGroupId"], json.loads(e["MessageBody"])["trace_id"]) for e in client.entries}
    # Each group id is paired with the body it was derived from — no cross-wiring.
    assert pairs == {("a" * 32, "a" * 32), ("b" * 32, "b" * 32), ("c" * 32, "c" * 32)}


def test_constant_group_id_overrides_trace_id() -> None:
    client = RecordingClient()
    events = [_event(trace_id=t * 32, log_id=f"log-{t}") for t in ("a", "b")]
    SQSSink(FIFO_URL, client=client, message_group_id="constant").emit(events)
    assert [e["MessageGroupId"] for e in client.entries] == ["constant", "constant"]


def test_callable_group_id_is_called_once_per_event() -> None:
    client = RecordingClient()
    seen: list[str] = []

    def group_of(event: dict) -> str:
        seen.append(str(event["log_id"]))
        return f"g-{event['log_id']}"

    events = [_event(log_id=f"log-{i}") for i in range(3)]
    SQSSink(FIFO_URL, client=client, message_group_id=group_of).emit(events)

    assert seen == ["log-0", "log-1", "log-2"]
    assert [e["MessageGroupId"] for e in client.entries] == ["g-log-0", "g-log-1", "g-log-2"]


def test_callable_may_group_by_baggage_carried_in_fields() -> None:
    # The documented per-span recipe: set_baggage() lands in `fields` (model.py), and since
    # SPEC-015 the boundary events carry it too, so a whole span shares one group.
    client = RecordingClient()
    sink = SQSSink(
        FIFO_URL,
        client=client,
        message_group_id=lambda e: str(e["fields"].get("group") or e["trace_id"]),  # type: ignore[union-attr]
    )
    sink.emit([_event(fields={"group": "tenant-42"}), _event(fields={})])
    assert [e["MessageGroupId"] for e in client.entries] == ["tenant-42", "a" * 32]


@pytest.mark.parametrize("trace_id", ["", "   ", None])
def test_missing_or_blank_trace_id_falls_back(trace_id: object) -> None:
    client = RecordingClient()
    event = {"message": "m", "log_id": "log-1"}
    if trace_id is not None:
        event["trace_id"] = str(trace_id)
    SQSSink(FIFO_URL, client=client).emit([event])
    assert client.entries[0]["MessageGroupId"] == DEFAULT_GROUP_ID


def test_callable_returning_blank_falls_back_rather_than_sending_empty() -> None:
    client = RecordingClient()
    SQSSink(FIFO_URL, client=client, message_group_id=lambda e: "  ").emit([_event()])
    assert client.entries[0]["MessageGroupId"] == DEFAULT_GROUP_ID


def test_over_long_group_id_is_truncated() -> None:
    client = RecordingClient()
    SQSSink(FIFO_URL, client=client, message_group_id="g" * 200).emit([_event()])
    assert client.entries[0]["MessageGroupId"] == "g" * MAX_ID_LEN


def test_blank_constant_group_id_is_rejected_at_construction() -> None:
    # A deterministic config error the caller can fix — fail fast beats a silent substitution
    # surfacing as a mystery group in their queue.
    with pytest.raises(ValueError, match="non-empty"):
        SQSSink(FIFO_URL, client=RecordingClient(), message_group_id="   ")


def test_retry_reordering_limitation_is_documented() -> None:
    from log_foundry.sinks import sqs

    assert sqs.__doc__ is not None
    assert "best-effort" in sqs.__doc__ and "retry" in sqs.__doc__


# -- FR-003: MessageDeduplicationId per entry, defaulting to log_id ---------------------


def test_dedup_id_defaults_to_log_id() -> None:
    client = RecordingClient()
    SQSSink(FIFO_URL, client=client).emit([_event(log_id="log-xyz")])
    assert client.entries[0]["MessageDeduplicationId"] == "log-xyz"


def test_events_differing_only_by_log_id_get_distinct_dedup_ids() -> None:
    client = RecordingClient()
    SQSSink(FIFO_URL, client=client).emit([_event(log_id="log-1"), _event(log_id="log-2")])
    ids = [e["MessageDeduplicationId"] for e in client.entries]
    assert ids == ["log-1", "log-2"]
    assert len(set(ids)) == 2


def test_callable_dedup_id_overrides_the_default() -> None:
    client = RecordingClient()
    calls: list[str] = []

    def dedup_of(event: dict) -> str:
        calls.append(str(event["log_id"]))
        return f"d-{event['log_id']}"

    SQSSink(FIFO_URL, client=client, message_deduplication_id=dedup_of).emit(
        [_event(log_id="log-1"), _event(log_id="log-2")]
    )
    assert calls == ["log-1", "log-2"]
    assert [e["MessageDeduplicationId"] for e in client.entries] == ["d-log-1", "d-log-2"]


def test_missing_log_id_mints_a_fresh_unshared_dedup_id() -> None:
    client = RecordingClient()
    SQSSink(FIFO_URL, client=client).emit([{"message": "m", "trace_id": "a" * 32}] * 2)
    ids = [e["MessageDeduplicationId"] for e in client.entries]
    assert all(ids), "never an empty parameter"
    assert len(set(ids)) == 2, "never a value shared with another entry"


def test_over_long_dedup_id_is_truncated() -> None:
    client = RecordingClient()
    SQSSink(FIFO_URL, client=client).emit([_event(log_id="d" * 200)])
    assert client.entries[0]["MessageDeduplicationId"] == "d" * MAX_ID_LEN


# -- FR-004: standard queues are unaffected ---------------------------------------------


def test_standard_queue_entries_carry_no_fifo_parameters() -> None:
    client = RecordingClient()
    SQSSink(STD_URL, client=client).emit([_event() for _ in range(3)])
    assert all(set(e) == {"Id", "MessageBody"} for e in client.entries)


def test_forced_non_fifo_on_a_fifo_url_carries_no_fifo_parameters() -> None:
    client = RecordingClient()
    SQSSink(FIFO_URL, client=client, fifo=False).emit([_event()])
    assert set(client.entries[0]) == {"Id", "MessageBody"}


def test_standard_chunk_boundaries_are_unchanged() -> None:
    client = RecordingClient()
    SQSSink(STD_URL, client=client).emit([_event(log_id=f"log-{i}") for i in range(25)])
    assert [len(call) for call in client.calls] == [10, 10, 5]


# -- FR-005: FIFO parameters count toward the request byte budget -----------------------


def test_fifo_parameters_are_costed_against_the_256kb_limit() -> None:
    # Eight bodies that fit one standard request exactly, but not once the group and dedup
    # ids that travel with them are counted.
    trace_id, log_id = "a" * 32, "b" * 36
    events = [_sized_event(32760, trace_id, f"{log_id[:-1]}{i}") for i in range(8)]
    assert sum(len(json.dumps(e).encode()) for e in events) <= SQSSink.MAX_BYTES

    std_client = RecordingClient()
    SQSSink(STD_URL, client=std_client).emit(events)
    assert len(std_client.calls) == 1, "standard queue: one request"

    fifo_client = RecordingClient()
    SQSSink(FIFO_URL, client=fifo_client).emit(events)
    assert len(fifo_client.calls) > 1, "FIFO: the added ids push it over, so it splits"


def test_no_fifo_request_exceeds_the_byte_limit() -> None:
    client = RecordingClient()
    events = [_sized_event(32760, "a" * 32, f"log-{i}") for i in range(8)]
    SQSSink(FIFO_URL, client=client).emit(events)

    for call in client.calls:
        billed = sum(
            len(e["MessageBody"].encode())
            + len(e["MessageGroupId"].encode())
            + len(e["MessageDeduplicationId"].encode())
            for e in call
        )
        assert billed <= SQSSink.MAX_BYTES


def test_oversized_drop_is_judged_on_everything_that_travels() -> None:
    # A body over the limit on its own is undeliverable on either queue type.
    big = _sized_event(SQSSink.MAX_BYTES + 1, "a" * 32, "log-1")

    std = SQSSink(STD_URL, client=RecordingClient())
    std.emit([big, _event()])
    fifo = SQSSink(FIFO_URL, client=RecordingClient())
    fifo.emit([big, _event()])

    assert std.dropped_oversized == fifo.dropped_oversized == 1


def test_fifo_narrow_band_event_is_dropped_not_shipped_over_budget() -> None:
    # A body at exactly the limit fits a standard message, but not once the FIFO ids that
    # travel with it are added. Dropping it labels the loss; shipping it would earn a
    # SenderFault, which FR-006 never retries — losing the event as an opaque failure.
    event = _sized_event(SQSSink.MAX_BYTES, "a" * 32, "b" * 36)

    std_client = RecordingClient()
    std = SQSSink(STD_URL, client=std_client)
    std.emit([event])
    assert std.dropped_oversized == 0, "standard queue: the body alone fits"
    assert len(std_client.calls) == 1

    fifo_client = RecordingClient()
    fifo = SQSSink(FIFO_URL, client=fifo_client)
    fifo.emit([event])
    assert fifo.dropped_oversized == 1, "FIFO: body + ids exceed the limit, so it is dropped"
    assert fifo_client.calls == [], "nothing is sent"


def test_no_fifo_request_exceeds_the_limit_with_a_narrow_band_event_mixed_in() -> None:
    # The budget invariant must hold for a batch that mixes ordinary and near-limit events.
    events = [
        _event(log_id="small-1"),
        _sized_event(SQSSink.MAX_BYTES, "a" * 32, "b" * 36),
        _event(log_id="small-2"),
    ]
    client = RecordingClient()
    SQSSink(FIFO_URL, client=client).emit(events)

    for call in client.calls:
        billed = sum(
            len(e["MessageBody"].encode())
            + len(e["MessageGroupId"].encode())
            + len(e["MessageDeduplicationId"].encode())
            for e in call
        )
        assert billed <= SQSSink.MAX_BYTES


# -- FR-006: sender-fault entries are not retried ---------------------------------------


class FaultingClient:
    """Fails chosen entry Ids every time, with a caller-chosen SenderFault flag and code."""

    def __init__(self, *, sender_fault: bool, code: str = "MissingParameter") -> None:
        self.sender_fault = sender_fault
        self.code = code
        self.calls: list[list[dict]] = []

    def send_message_batch(self, *, QueueUrl: str, Entries: list[dict]) -> dict:
        self.calls.append([dict(e) for e in Entries])
        return {
            "Successful": [],
            "Failed": [
                {"Id": e["Id"], "SenderFault": self.sender_fault, "Code": self.code}
                for e in Entries
            ],
        }


class MixedFaultClient:
    """Entry '0' is a permanent sender fault; the rest are retryable internal errors."""

    def __init__(self) -> None:
        self.calls: list[list[dict]] = []

    def send_message_batch(self, *, QueueUrl: str, Entries: list[dict]) -> dict:
        self.calls.append([dict(e) for e in Entries])
        failed = []
        for e in Entries:
            fault = e["Id"] == "0"
            failed.append(
                {
                    "Id": e["Id"],
                    "SenderFault": fault,
                    "Code": "InvalidParameterValue" if fault else "InternalError",
                }
            )
        return {"Successful": [], "Failed": failed}


def test_sender_fault_entries_are_not_resent() -> None:
    client = FaultingClient(sender_fault=True)
    sink = SQSSink(STD_URL, client=client, max_retries=3)
    sink.emit([_event(log_id=f"log-{i}") for i in range(2)])

    assert len(client.calls) == 1, "a deterministic rejection is never worth re-sending"
    assert sink.failed == 2
    assert sink.losses() == SinkLosses(dropped=0, failed=2), (
        "lost and reported — but not raised, or the worker's retry would re-send them "
        "byte-identical, which is what FR-006 refuses (SPEC-026 FR-001)"
    )


def test_non_sender_faults_are_still_retried_under_the_bound() -> None:
    client = FaultingClient(sender_fault=False, code="InternalError")
    sink = SQSSink(STD_URL, client=client, max_retries=3)
    with pytest.raises(SinkDeliveryError):
        # Retryable and still failing at the bound: a genuine total failure, so the worker
        # gets its signal (SPEC-026 FR-001). A sender fault would not raise — see below.
        sink.emit([_event()])

    assert len(client.calls) == 4, "1 attempt + 3 retries"
    assert sink.failed == 1


def test_mixed_response_retries_only_the_retryable_entries() -> None:
    client = MixedFaultClient()
    sink = SQSSink(STD_URL, client=client, max_retries=2)
    with pytest.raises(SinkDeliveryError):
        sink.emit([_event(log_id=f"log-{i}") for i in range(3)])  # nothing landed

    assert [len(call) for call in client.calls] == [3, 2, 2], "entry '0' drops out after call 1"
    assert all(e["Id"] != "0" for call in client.calls[1:] for e in call)
    assert sink.failed == 3, "1 sender fault + 2 exhausted retryables"


def test_abandoned_sender_faults_name_the_sqs_code(capsys) -> None:
    client = FaultingClient(sender_fault=True, code="MissingParameter")
    SQSSink(STD_URL, client=client).emit([_event()])

    err = capsys.readouterr().err
    assert "lost 1 message(s)" in err, "the line carries the count"
    assert "MissingParameter" in err, "the code is what makes the cause diagnosable"
    assert "not retried" in err


def test_a_missing_sender_fault_flag_degrades_to_retrying() -> None:
    # An unfamiliar response shape must not silently drop messages.
    class NoFlagClient:
        def __init__(self) -> None:
            self.calls: list[list[dict]] = []

        def send_message_batch(self, *, QueueUrl: str, Entries: list[dict]) -> dict:
            self.calls.append([dict(e) for e in Entries])
            return {"Successful": [], "Failed": [{"Id": e["Id"], "Code": "Weird"} for e in Entries]}

    client = NoFlagClient()
    with pytest.raises(SinkDeliveryError):
        SQSSink(STD_URL, client=client, max_retries=2).emit([_event()])
    assert len(client.calls) == 3


def test_fifo_misconfiguration_costs_one_call_per_chunk_not_four() -> None:
    # The scenario FR-006 exists for: fifo=False forced onto a .fifo queue, so entries go out
    # without a MessageGroupId and SQS rejects every one as a sender fault.
    client = FaultingClient(sender_fault=True, code="MissingParameter")
    sink = SQSSink(FIFO_URL, client=client, fifo=False, max_retries=3)
    sink.emit([_event(log_id=f"log-{i}") for i in range(12)])

    assert [len(call) for call in client.calls] == [10, 2], "one call per chunk, no retries"
    assert sink.failed == 12
