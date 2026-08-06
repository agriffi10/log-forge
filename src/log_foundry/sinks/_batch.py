"""Shared adjudication of a positional batch response (SPEC-018)."""

from __future__ import annotations

from typing import Any, NamedTuple

__all__ = ["Adjudication", "adjudicate_positional", "usable_results"]


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

    Returns:
      The records to retry, or the count abandoned as unadjudicated when the arrays disagree.

    Raises:
      None.
    """
    if len(results) != len(records):
        return Adjudication([], len(records))
    return Adjudication(
        [record for record, result in zip(records, results, strict=True) if result.get(error_key)],
        0,
    )
