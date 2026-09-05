"""SPEC-018 — adjudicate_positional: the length precondition behind Kinesis/Firehose retries."""

from __future__ import annotations

import pytest

from log_foundry.sinks._batch import adjudicate_positional, usable_results


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


# -- usable_results: the response field is whatever the client chose to return -------------


def test_a_list_of_mappings_passes_through_unchanged() -> None:
    results = [{"ErrorCode": "InternalFailure"}, {"SequenceNumber": "1"}]
    assert usable_results(results) is results


def test_an_empty_list_is_usable() -> None:
    assert usable_results([]) == []


def test_a_none_field_describes_nothing() -> None:
    assert usable_results(None) == []


def test_a_scalar_field_describes_nothing() -> None:
    assert usable_results("InternalFailure") == []
    assert usable_results(3) == []


def test_a_list_of_non_mappings_describes_nothing() -> None:
    assert usable_results(["InternalFailure", "ok"]) == []
    assert usable_results([{"ErrorCode": "InternalFailure"}, None]) == []


def test_an_unusable_field_routes_to_unadjudicated_not_an_exception() -> None:
    records = [{"Data": b"a"}, {"Data": b"b"}]
    verdict = adjudicate_positional(records, usable_results(None))
    assert verdict.retry == []
    assert verdict.unadjudicated == 2


@pytest.mark.parametrize("bad", [0, -1, -5])
def test_chunk_list_refuses_a_non_positive_size(bad: int) -> None:
    """SPEC-049 FR-002, making `chunk_list`'s own `Raises:` true.

    Two things make this test's shape non-obvious, and both are the reason it asserts the
    **message** rather than the type. `chunk_list` is a generator, so the guard raises at the
    first `next()` and not at the call -- `pytest.raises` around the bare call would not fire, so
    the list() is load-bearing. And `chunk_list(items, 0)` *already* raised `ValueError` today,
    from `range()`, so removing the new guard leaves a type-only assertion green: this is the one
    guard in this spec that is provably vacuous without a message assertion.
    """
    from log_foundry.sinks._chunk import chunk_list

    with pytest.raises(ValueError, match="size must be a positive integer"):
        list(chunk_list([1, 2, 3], bad))


def test_chunk_list_still_chunks_a_positive_size() -> None:
    from log_foundry.sinks._chunk import chunk_list

    assert list(chunk_list([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]]
