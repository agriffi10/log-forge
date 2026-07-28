"""SPEC-018 — adjudicate_positional: the length precondition behind Kinesis/Firehose retries."""

from __future__ import annotations

from log_foundry.sinks._batch import adjudicate_positional


def test_equal_lengths_select_the_flagged_records() -> None:
    records = [{"Data": b"a"}, {"Data": b"b"}, {"Data": b"c"}]
    results = [{"ErrorCode": "InternalFailure"}, {"SequenceNumber": "1"}, {"ErrorCode": "Throttle"}]
    verdict = adjudicate_positional(records, results)
    assert verdict.retry == [{"Data": b"a"}, {"Data": b"c"}]
    assert verdict.unadjudicated == 0


def test_equal_lengths_with_no_failures_select_nothing() -> None:
    records = [{"Data": b"a"}, {"Data": b"b"}]
    verdict = adjudicate_positional(records, [{"SequenceNumber": "1"}, {"SequenceNumber": "2"}])
    assert verdict.retry == []
    assert verdict.unadjudicated == 0


def test_a_falsy_error_code_is_not_a_failure() -> None:
    verdict = adjudicate_positional([{"Data": b"a"}], [{"ErrorCode": ""}])
    assert verdict.retry == []
    assert verdict.unadjudicated == 0


def test_short_results_adjudicate_nothing() -> None:
    records = [{"Data": b"a"}, {"Data": b"b"}, {"Data": b"c"}]
    verdict = adjudicate_positional(records, [{"ErrorCode": "InternalFailure"}])
    assert verdict.retry == []
    assert verdict.unadjudicated == 3  # the whole chunk, not just the unpaired tail


def test_long_results_adjudicate_nothing() -> None:
    records = [{"Data": b"a"}]
    results = [{"ErrorCode": "InternalFailure"}, {"ErrorCode": "InternalFailure"}]
    verdict = adjudicate_positional(records, results)
    assert verdict.retry == []
    assert verdict.unadjudicated == 1  # counted in records sent, not results returned


def test_absent_results_adjudicate_nothing() -> None:
    verdict = adjudicate_positional([{"Data": b"a"}, {"Data": b"b"}], [])
    assert verdict.retry == []
    assert verdict.unadjudicated == 2


def test_empty_records_are_adjudicated_trivially() -> None:
    verdict = adjudicate_positional([], [])
    assert verdict.retry == []
    assert verdict.unadjudicated == 0


def test_error_key_is_a_parameter() -> None:
    records = [{"Data": b"a"}, {"Data": b"b"}]
    results = [{"status": "failed"}, {"ErrorCode": "InternalFailure"}]
    verdict = adjudicate_positional(records, results, error_key="status")
    assert verdict.retry == [{"Data": b"a"}]  # the ErrorCode entry is not consulted
    assert verdict.unadjudicated == 0
