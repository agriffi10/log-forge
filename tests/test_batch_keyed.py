"""SPEC-074 FR-002 — the keyed reader: a keyed response adjudicates all of a chunk or none of it.

``adjudicate_keyed`` pairs a ``Failed`` list (SQS, SNS) or ``writeErrors`` (MongoDB) with the
positions whose identifier each item names. The cases are the criterion's, plus a hostile item
whose lookup or comparison raises: the reader never raises, and a ``BaseException`` still
propagates. Serves invariants 2 and 7.
"""

from __future__ import annotations

from typing import Any

import pytest

from log_foundry.sinks._batch import KeyedAdjudication, adjudicate_keyed

_KEYS = ["0", "1", "2"]


def test_one_failed_entry_pairs_its_position() -> None:
    entry = {"Id": "1", "Code": "X"}
    assert adjudicate_keyed(_KEYS, [entry], key="Id") == KeyedAdjudication([None, entry, None], 0)


def test_a_missing_failed_pairs_none() -> None:
    assert adjudicate_keyed(_KEYS, [], key="Id") == KeyedAdjudication([None, None, None], 0)


@pytest.mark.parametrize(
    ("keys", "failed", "key"),
    [
        (_KEYS, {"Id": "1"}, "Id"),
        (_KEYS, ["1"], "Id"),
        (_KEYS, [{"Id": "7"}], "Id"),
        (_KEYS, [{"Id": "1"}, {"Id": "1"}], "Id"),
        (_KEYS, [{"Code": "X"}], "Id"),
        ([0, 1], [{"index": True}], "index"),
        ([0, 1], [{"index": 1.0}], "index"),
        (_KEYS, None, "Id"),
    ],
    ids=["dict", "str-item", "unknown-id", "duplicate", "no-id", "bool-for-int", "float", "none"],
)
def test_an_unreadable_response_adjudicates_none(keys: list[object], failed: Any, key: str) -> None:
    result = adjudicate_keyed(keys, failed, key=key)
    assert result == KeyedAdjudication([None] * len(keys), len(keys))


class _HostileGet(dict):  # type: ignore[type-arg]
    def __contains__(self, key: object) -> bool:
        raise RuntimeError("hostile")


class _HostileEq(str):
    def __eq__(self, other: object) -> bool:
        raise RuntimeError("hostile")

    __hash__ = str.__hash__


class _Interrupting(dict):  # type: ignore[type-arg]
    def __contains__(self, key: object) -> bool:
        raise KeyboardInterrupt


class _HostileList(list):  # type: ignore[type-arg]
    def __iter__(self) -> Any:
        raise RuntimeError("never iterated through the subclass hook")


@pytest.mark.parametrize(
    "failed",
    [[_HostileGet(Id="1")], [{"Id": _HostileEq("1")}], _HostileList([{"Id": "1"}])],
    ids=["contains-raises", "eq-raises", "list-subclass"],
)
def test_it_never_raises_on_a_hostile_response(failed: Any) -> None:
    result = adjudicate_keyed(_KEYS, failed, key="Id")
    assert result.unadjudicated in (0, 3)
    if result.unadjudicated == 0:
        assert result.failed[1] is not None


def test_a_base_exception_from_an_item_propagates() -> None:
    with pytest.raises(KeyboardInterrupt):
        adjudicate_keyed(_KEYS, [_Interrupting(Id="1")], key="Id")
