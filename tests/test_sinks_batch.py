"""SPEC-018 — adjudicate_positional: the length precondition behind Kinesis/Firehose retries."""

from __future__ import annotations

import pytest

from log_foundry.sinks._batch import (
    ABSENT,
    adjudicate_positional,
    describe_count,
    results_rejected,
    usable_results,
)


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


# -- SPEC-070 FR-003: the failed count and the coded entries must agree ------------------------

_A, _B = {"Data": b"a"}, {"Data": b"b"}
_CODED = {"ErrorCode": "InternalFailure"}


@pytest.mark.parametrize(
    ("count", "results", "retry", "unadjudicated"),
    [
        (0, [{}, {}], [], 0),
        (ABSENT, [{}, {}], [], 0),
        (0, [], [], 0),
        (ABSENT, [], [], 0),
        (0, [{}], [], 0),
        (1, [{}, _CODED], [_B], 0),
        (2, [_CODED, _CODED], [_A, _B], 0),
        (0, [_CODED, _CODED], [], 2),
        (ABSENT, [_CODED, {}], [], 2),
        (2, [{}, {}], [], 2),
        (1, [_CODED, _CODED], [], 2),
        (3, [_CODED, _CODED], [], 2),
        (1, [_CODED], [], 2),
        (True, [{}, _CODED], [], 2),
        (False, [{}, {}], [], 2),
        ("1", [{}, _CODED], [], 2),
        (1.0, [{}, _CODED], [], 2),
        (-1, [{}, {}], [], 2),
        (None, [{}, {}], [], 2),
    ],
    ids=[
        "zero", "absent", "zero-no-array", "absent-no-array", "zero-short-uncoded",
        "one-of-two", "two-of-two", "zero-beside-coded", "absent-beside-coded",
        "two-beside-none", "one-beside-two", "three-beside-two", "short-array", "bool-true",
        "bool-false", "str", "float", "negative", "explicit-none",
    ],
)
def test_a_passed_count_is_read_three_ways(count, results, retry, unadjudicated) -> None:
    """Delivered only on ``0``/absent with nothing coded; retried only on agreement."""
    verdict = adjudicate_positional([_A, _B], results, failed_count=count)
    assert (verdict.retry, verdict.unadjudicated) == (retry, unadjudicated)


def test_without_a_count_the_rule_before_spec_070_holds() -> None:
    verdict = adjudicate_positional([_A, _B], [{}, _CODED])
    assert (verdict.retry, verdict.unadjudicated) == ([_B], 0)
    assert adjudicate_positional([_A, _B], [_CODED]).unadjudicated == 2


def test_empty_records_with_a_positive_count_leave_nothing_to_count() -> None:
    assert adjudicate_positional([], [], failed_count=2) == ([], 0)


@pytest.mark.parametrize(
    ("count", "text"),
    [(ABSENT, "no failed count"), (0, "failed count 0"), (2, "failed count 2"),
     ("1", "a failed count of type str"), (None, "a failed count of type NoneType"),
     (True, "a failed count of type bool")],
)
def test_describe_count_names_a_type_never_a_value(count, text) -> None:
    assert describe_count(count) == text


@pytest.mark.parametrize(
    ("raw", "rejected"),
    [(None, False), ([], False), ([{}], False), ([{}, None], True), ("x", True), ({}, True)],
    ids=["absent", "empty", "usable", "not-a-mapping", "str", "dict"],
)
def test_results_rejected_means_present_and_unusable(raw, rejected) -> None:
    assert results_rejected(raw) is rejected


@pytest.mark.parametrize("count", [0, ABSENT, 2])
def test_a_rejected_array_is_unadjudicated_in_counted_mode_only(count) -> None:
    """A present array nobody can read cannot show that nothing is coded (rule 3, not rule 1)."""
    assert adjudicate_positional([_A, _B], [], failed_count=count, rejected=True) == ([], 2)
    assert adjudicate_positional([_A, _B], [], rejected=True) == ([], 2), "today's rule: mismatch"
    assert adjudicate_positional([_A, _B], [], failed_count=0) == ([], 0), "no array: delivered"
