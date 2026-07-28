"""Shared adjudication of a positional batch response (SPEC-018).

Some batch APIs report per-record outcomes **positionally**: the response carries a parallel array
with no identifiers, so entry *i* describes record *i*. That correlation holds only while the two
arrays are the same length, and a disagreement is evidence they are not aligned — not an
invitation to use the overlapping prefix. Pairing them anyway truncates silently, which reads
downstream as "everything landed" for records the destination never confirmed.

This module makes the precondition explicit: either the response describes the whole chunk and the
caller acts on it, or it does not and the caller abandons the chunk audibly. Id-keyed responses
(``SQSSink``, ``SNSSink``) select by id and cannot truncate, so they do not come through here.
"""

from __future__ import annotations

from typing import Any, NamedTuple

__all__ = ["Adjudication", "adjudicate_positional"]


class Adjudication[T](NamedTuple):
    """The outcome of pairing a positional batch response against the records it should describe.

    ``retry`` is non-empty only when ``unadjudicated`` is ``0``: the response either describes the
    chunk or it does not. The two are never both non-zero.

    Attributes:
        retry: Records the response explicitly flagged as failed.
        unadjudicated: Records whose outcome the response did not describe — ``0`` on a
            well-formed response.
    """

    retry: list[T]
    unadjudicated: int


def adjudicate_positional[T](
    records: list[T],
    results: list[dict[str, Any]],
    *,
    error_key: str = "ErrorCode",
) -> Adjudication[T]:
    """Pair a positional batch response against the records it should describe.

    Returns the subset of ``records`` whose paired ``results`` entry carries a truthy ``error_key``.
    If the arrays disagree in length — including a ``results`` the caller defaulted to empty
    because the response omitted it — nothing is selected and the chunk is reported unadjudicated.
    ``error_key`` is a parameter so a future positional response naming its error differently needs
    no fork of this rule.
    """
    if len(results) != len(records):
        return Adjudication([], len(records))
    return Adjudication(
        [record for record, result in zip(records, results, strict=True) if result.get(error_key)],
        0,
    )
