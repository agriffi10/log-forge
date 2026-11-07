"""Shared adjudication of a positional batch response (SPEC-018)."""

from __future__ import annotations

from typing import Any, NamedTuple

__all__ = [
    "ABSENT",
    "Adjudication",
    "adjudicate_positional",
    "describe_count",
    "usable_results",
]


class _Absent:
    """The type of :data:`ABSENT` and :data:`_NOT_PASSED`; two instances, never equal."""

    __slots__ = ()


ABSENT: object = _Absent()
"""Passed as ``failed_count`` when the response carried no failed count at all (SPEC-070 FR-003).

Distinct from ``None``, which is a count the response *did* carry and which is not an ``int``.
"""

_NOT_PASSED: object = _Absent()
"""The default for ``failed_count``: the caller has no count to offer, and today's rule applies."""


class Adjudication[T](NamedTuple):
    """The outcome of pairing a positional batch response against the records it describes.

    ``retry`` is non-empty only when ``unadjudicated`` is ``0``: the response either describes
    the chunk or it does not, and the two are never both non-zero.

    Attributes:
      retry: Records the response explicitly flagged as failed.
      unadjudicated: Records whose outcome the response did not describe, ``0`` on a
        well-formed response.
    """

    retry: list[T]
    unadjudicated: int


def usable_results(results: Any) -> list[dict[str, Any]]:
    """Returns the results if they are a list of mappings, else an empty list.

    The response comes off a client the sink does not control, so the field may be any shape at
    all — ``None``, a scalar, a list of non-mappings. None of those carry per-record outcomes
    that can be read, so each is treated as describing nothing, which routes it to the same
    counted, audible abandonment as a length mismatch. Raising instead would put the
    malformed-client case back on the path this module exists to take it off.

    Args:
      results: The response's per-record array, of any shape.

    Returns:
      The results when usable, otherwise an empty list.

    Raises:
      None.
    """
    if isinstance(results, list) and all(isinstance(result, dict) for result in results):
        return results
    return []


def adjudicate_positional[T](
    records: list[T],
    results: list[dict[str, Any]],
    *,
    error_key: str = "ErrorCode",
    failed_count: object = _NOT_PASSED,
) -> Adjudication[T]:
    """Pairs a positional batch response against the records it should describe.

    Some batch APIs report per-record outcomes positionally, with no identifiers, so entry *i*
    describes record *i*. That correlation holds only while the two arrays are the same length,
    and a disagreement is evidence they are not aligned rather than an invitation to use the
    overlapping prefix — pairing them anyway truncates silently, which reads downstream as
    "everything landed" for records the destination never confirmed.

    Args:
      records: The records that were sent.
      results: The response's per-record array, including one the caller defaulted to empty
        because the response omitted it.
      error_key: The key a failed entry carries, a parameter so a future positional response
        naming its error differently needs no fork of this rule.
      failed_count: The response's own failed count (``FailedRecordCount``,
        ``FailedPutCount``), or :data:`ABSENT` when the response carried none. Each reply states
        its failures twice, as a count and as coded entries, and when the two disagree they
        disagree about which records landed (SPEC-070 FR-003). Passed, the rule is three-way:
        the chunk is delivered only when the count is the ``int`` ``0`` or absent **and** no
        usable result carries ``error_key``; the coded records are retried only when the count is
        a positive ``int`` (a ``bool`` is not one), the arrays are the same length and exactly
        that many entries are coded; every other combination — a ``0`` or absent count beside
        coded entries, a positive count beside more or fewer coded entries, a count that is not
        an ``int`` (``None`` included), and a length mismatch — is unadjudicated, because a
        resend of the coded records could duplicate what the count says was taken. Not passed,
        the rule is the one before SPEC-070, for a caller with no count to offer.

    Returns:
      The records to retry, or the count abandoned as unadjudicated when the arrays disagree.

    Raises:
      None.
    """
    if failed_count is not _NOT_PASSED:
        return _adjudicate_counted(records, results, error_key, failed_count)
    if len(results) != len(records):
        return Adjudication([], len(records))
    return Adjudication(
        [record for record, result in zip(records, results, strict=True) if result.get(error_key)],
        0,
    )


def _adjudicate_counted[T](
    records: list[T],
    results: list[dict[str, Any]],
    error_key: str,
    failed_count: object,
) -> Adjudication[T]:
    """Applies SPEC-070 FR-003's three-way rule; :func:`adjudicate_positional` says why.

    Args:
      records: The records that were sent.
      results: The response's usable per-record array.
      error_key: The key a failed entry carries.
      failed_count: The response's failed count, or :data:`ABSENT`.

    Returns:
      Nothing to retry and nothing unadjudicated for a delivered chunk, the coded records for a
      consistent failure, or the whole chunk unadjudicated.

    Raises:
      None.
    """
    coded = [index for index, result in enumerate(results) if result.get(error_key)]
    if failed_count is ABSENT or (type(failed_count) is int and failed_count == 0):
        return Adjudication([], len(records) if coded else 0)
    if (
        type(failed_count) is int
        and failed_count > 0
        and len(results) == len(records)
        and len(coded) == failed_count
    ):
        return Adjudication([records[index] for index in coded], 0)
    return Adjudication([], len(records))


def describe_count(failed_count: object) -> str:
    """Renders a response's failed count for an unadjudicated line, naming a type, never a value.

    The count comes off the client, so only an ``int`` is written as a number; anything else is
    named by its type, which keeps a server's text out of the line (invariant 11).

    Args:
      failed_count: The response's failed count, or :data:`ABSENT`.

    Returns:
      ``"no failed count"``, ``"failed count N"`` or ``"a failed count of type T"``.

    Raises:
      None.
    """
    if failed_count is ABSENT:
        return "no failed count"
    if type(failed_count) is int:
        return f"failed count {failed_count}"
    return f"a failed count of type {type(failed_count).__name__}"
