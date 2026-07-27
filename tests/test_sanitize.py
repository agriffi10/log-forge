"""SPEC-017 — value coercion and size ceilings (FR-001, FR-002).

Pure unit tests: every case builds its own ``Config`` inline rather than going through
``configure()``, so nothing here touches the global singleton and a ceiling under test can be set
to an absurd value without the validation ``configure()`` (rightly) applies.
"""

from __future__ import annotations

import enum
import json
from datetime import UTC, date, datetime, time
from decimal import Decimal
from uuid import UUID

import pytest

from log_foundry.config import Config
from log_foundry.sanitize import (
    TRUNCATION_MARKER,
    coerce,
    sanitize_fields,
    truncate_str,
    truncate_tail,
)

CFG = Config()


def _one(value: object, **overrides: int) -> object:
    """Coerce a single value through the full ``sanitize_fields`` path."""
    cfg = Config(**overrides) if overrides else CFG
    fields, _ = sanitize_fields({"k": value}, cfg=cfg)
    return fields["k"]


def _truncated(value: object, **overrides: int) -> bool:
    cfg = Config(**overrides) if overrides else CFG
    _, flag = sanitize_fields({"k": value}, cfg=cfg)
    return flag


# -- FR-001: the coercion table ---------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("plain", "plain"),
        (7, 7),
        (1.5, 1.5),
        (True, True),
        (None, None),
        (datetime(2026, 1, 1, 12, 30, tzinfo=UTC), "2026-01-01T12:30:00+00:00"),
        (date(2026, 1, 1), "2026-01-01"),
        (time(12, 30), "12:30:00"),
        (UUID("12345678-1234-5678-1234-567812345678"), "12345678-1234-5678-1234-567812345678"),
        (Decimal("1.10"), "1.10"),
        (b"bytes", "bytes"),
        (bytearray(b"barray"), "barray"),
        (memoryview(b"mview"), "mview"),
        ((1, 2), [1, 2]),
        ([1, "a"], [1, "a"]),
        ({"n": 1}, {"n": 1}),
    ],
)
def test_coercion_table(value: object, expected: object) -> None:
    assert _one(value) == expected


def test_decimal_becomes_a_string_so_precision_survives() -> None:
    # The point of the str(): float(Decimal("0.1")) is not 0.1.
    assert _one(Decimal("0.1")) == "0.1"


def test_sets_become_lists() -> None:
    assert sorted(_one({"b", "a"})) == ["a", "b"]  # type: ignore[arg-type]
    assert sorted(_one(frozenset({1, 2}))) == [1, 2]  # type: ignore[arg-type]


def test_memoryview_is_text_not_a_byte_list() -> None:
    # memoryview is a registered Sequence, so without explicit handling this renders as [120].
    assert _one(memoryview(b"x")) == "x"


def test_every_coerced_event_is_json_serializable() -> None:
    class Custom:
        pass

    fields, _ = sanitize_fields(
        {
            "when": datetime(2026, 1, 1, tzinfo=UTC),
            "oid": UUID("12345678-1234-5678-1234-567812345678"),
            "amount": Decimal("1.10"),
            "raw": b"\xff\xfe",
            "tags": {"a", "b"},
            "obj": Custom(),
        },
        cfg=CFG,
    )
    json.dumps(fields)  # must not raise


# -- FR-001: the unserializable fallback ------------------------------------------------


def test_unknown_type_becomes_a_named_placeholder() -> None:
    class MyClass:
        pass

    assert _one(MyClass()) == "<unserializable: MyClass>"


def test_other_fields_survive_an_unserializable_neighbour() -> None:
    class MyClass:
        pass

    fields, _ = sanitize_fields({"bad": MyClass(), "good": "kept", "n": 3}, cfg=CFG)
    assert fields == {"bad": "<unserializable: MyClass>", "good": "kept", "n": 3}


def test_placeholder_does_not_disclose_the_repr() -> None:
    """The whole reason the fallback is a type name and not ``repr(value)`` (arch §6)."""

    class Client:
        def __repr__(self) -> str:
            return "Client(token='s3cret')"

    fields, _ = sanitize_fields({"client": Client()}, cfg=CFG)
    assert "s3cret" not in json.dumps(fields)


def test_a_hostile_value_cannot_escape() -> None:
    class Hostile:
        def __str__(self) -> str:
            raise RuntimeError("nope")

        def __repr__(self) -> str:
            raise RuntimeError("nope")

        def __iter__(self) -> object:
            raise RuntimeError("nope")

    assert _one(Hostile()) == "<unserializable: Hostile>"


def test_a_raising_isoformat_degrades_rather_than_raising() -> None:
    class BadDate(datetime):
        def isoformat(self, *a: object, **k: object) -> str:
            raise RuntimeError("nope")

    assert _one(BadDate(2026, 1, 1)) == "<unserializable: BadDate>"


# -- FR-001: enums, keys, cycles ---------------------------------------------------------


def test_int_and_str_enums_degrade_to_their_plain_value() -> None:
    class Colour(enum.IntEnum):
        RED = 1

    class Name(enum.StrEnum):
        A = "a"

    # Both members *are* int/str instances; passing the member itself through would hand a
    # non-JSON sink (postgres, sqlite) an Enum where a plain value was promised.
    assert _one(Colour.RED) == 1
    assert type(_one(Colour.RED)) is int
    assert _one(Name.A) == "a"
    assert type(_one(Name.A)) is str


def test_enum_with_a_structured_value_renders_as_the_member() -> None:
    class Pair(enum.Enum):
        A = (1, 2)

    assert _one(Pair.A) == "Pair.A"


def test_non_string_mapping_keys_are_coerced() -> None:
    assert _one({1: "a"}) == {"1": "a"}
    assert _one({None: "a"}) == {"None": "a"}


def test_self_referencing_mapping_is_marked_circular() -> None:
    d: dict[str, object] = {}
    d["self"] = d
    assert _one(d) == {"self": "<circular>"}


def test_self_referencing_list_is_marked_circular() -> None:
    lst: list[object] = []
    lst.append(lst)
    assert _one(lst) == ["<circular>"]


def test_a_shared_sibling_is_not_a_cycle() -> None:
    shared = {"n": 1}
    assert _one({"x": shared, "y": shared}) == {"x": {"n": 1}, "y": {"n": 1}}


def test_a_cycle_does_not_set_the_truncated_flag() -> None:
    """``<circular>`` is a coercion outcome, not a ceiling — only ceilings flag."""
    d: dict[str, object] = {}
    d["self"] = d
    assert _truncated(d) is False


def test_an_unserializable_value_does_not_set_the_truncated_flag() -> None:
    class MyClass:
        pass

    assert _truncated(MyClass()) is False


# -- FR-002: the four ceilings -----------------------------------------------------------


def test_long_string_is_truncated_and_flags() -> None:
    value = "x" * 20_000
    out = _one(value)
    assert isinstance(out, str)
    assert out.endswith(TRUNCATION_MARKER)
    assert len(out.encode("utf-8")) <= CFG.max_value_bytes
    assert _truncated(value) is True


def test_truncation_cuts_on_a_character_boundary() -> None:
    # Every char is 2 bytes, so an odd budget forces a cut mid-sequence.
    value = "é" * 100
    out = _one(value, max_value_bytes=45)
    assert isinstance(out, str)
    out.encode("utf-8")  # must not raise — the payload decodes cleanly
    assert len(out.encode("utf-8")) <= 45


def test_a_value_within_every_ceiling_does_not_flag() -> None:
    assert _truncated({"a": "short", "b": [1, 2, 3]}) is False


def test_max_keys_caps_a_mapping_and_flags() -> None:
    out = _one({str(i): i for i in range(300)}, max_keys=2)
    assert isinstance(out, dict)
    assert len(out) == 2
    assert _truncated({str(i): i for i in range(300)}, max_keys=2) is True


def test_max_keys_caps_a_sequence() -> None:
    out = _one(list(range(300)), max_keys=3)
    assert out == [0, 1, 2]


def test_max_depth_replaces_deeper_nesting_and_flags() -> None:
    deep: object = "bottom"
    for _ in range(12):
        deep = {"n": deep}
    fields, flag = sanitize_fields({"k": deep}, cfg=Config(max_depth=3))
    assert "<depth limit>" in json.dumps(fields)
    assert flag is True


def test_truncation_marker_is_bounded_by_the_ceiling_not_added_to_it() -> None:
    """A ceiling that can be exceeded is not a ceiling — the total stays within max_bytes."""
    out, flag = truncate_str("x" * 500, 100)
    assert flag is True
    assert len(out.encode("utf-8")) <= 100


# -- FR-002: the two truncators ----------------------------------------------------------


_LONG = "HEAD" + "x" * 100 + "TAIL"


def test_truncate_str_keeps_the_head() -> None:
    out, flag = truncate_str(_LONG, 30)
    assert flag is True
    assert out.startswith("HEAD")
    assert "TAIL" not in out
    assert out.endswith(TRUNCATION_MARKER)


def test_truncate_tail_keeps_the_tail() -> None:
    out, flag = truncate_tail(_LONG, 30)
    assert flag is True
    assert out.endswith("TAIL")
    assert "HEAD" not in out
    assert out.startswith(TRUNCATION_MARKER)


def test_truncate_tail_within_budget_is_untouched() -> None:
    assert truncate_tail("short", 1000) == ("short", False)


def test_truncate_str_within_budget_is_untouched() -> None:
    assert truncate_str("short", 1000) == ("short", False)


@pytest.mark.parametrize("ceiling", [1, 5, len(TRUNCATION_MARKER.encode("utf-8"))])
def test_truncate_tail_with_a_ceiling_smaller_than_the_marker(ceiling: int) -> None:
    """``raw[-0:]`` returns the *whole* string, so this needs a real guard, not a slice."""
    out, flag = truncate_tail("x" * 500, ceiling)
    assert flag is True
    assert out == TRUNCATION_MARKER
    assert "x" not in out


@pytest.mark.parametrize("ceiling", [1, 5, len(TRUNCATION_MARKER.encode("utf-8"))])
def test_truncate_str_with_a_ceiling_smaller_than_the_marker(ceiling: int) -> None:
    out, flag = truncate_str("x" * 500, ceiling)
    assert flag is True
    assert out == TRUNCATION_MARKER


def test_lone_surrogate_does_not_raise() -> None:
    """A str from surrogateescape raises on a bare .encode("utf-8") — inside a total function."""
    value = "\ud800" * 10
    assert truncate_str(value, 4)[1] is True
    assert _one(value) is not None  # no exception


# -- public helpers ----------------------------------------------------------------------


def test_coerce_handles_a_bare_value() -> None:
    assert coerce(Decimal("2.5"), cfg=CFG) == "2.5"


def test_sanitize_fields_returns_a_new_mapping() -> None:
    original = {"a": 1}
    out, _ = sanitize_fields(original, cfg=CFG)
    assert out == original
    assert out is not original
