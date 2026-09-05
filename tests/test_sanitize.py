"""SPEC-017 — value coercion and size ceilings (FR-001, FR-002).

Pure unit tests: every case builds its own ``Config`` inline rather than going through
``configure()``, so nothing here touches the global singleton and a ceiling under test can be set
to an absurd value without the validation ``configure()`` (rightly) applies.
"""

from __future__ import annotations

import enum
import json
import os
import sys
from datetime import UTC, date, datetime, time
from decimal import Decimal
from time import perf_counter
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


def test_a_lone_surrogate_is_replaced_not_passed_through() -> None:
    """SPEC-054 FR-001 AC-1/AC-2: one U+FFFD per surrogate, an exact str, strictly encodable.

    Before SPEC-054 this test asserted only that the call returned; the surrogate left assembly
    intact and cost `SQLiteSink` the whole batch.
    """
    bad = os.fsdecode(b"file-\xff.txt")
    out, altered = truncate_str(bad, 8192)
    assert (out, altered) == ("file-\ufffd.txt", True)
    assert type(out) is str
    out.encode("utf-8")

    ten = "\ud800" * 10
    assert truncate_str(ten, 4) == (TRUNCATION_MARKER, True)
    clipped, flag = truncate_str("a\ud800" + "x" * 30, 20)
    assert (clipped, flag) == ("a\ufffdxx" + TRUNCATION_MARKER, True), "6 bytes kept beside the marker"
    clipped.encode("utf-8")


# -- public helpers ----------------------------------------------------------------------


def test_coerce_handles_a_bare_value() -> None:
    assert coerce(Decimal("2.5"), cfg=CFG) == "2.5"


def test_sanitize_fields_returns_a_new_mapping() -> None:
    original = {"a": 1}
    out, _ = sanitize_fields(original, cfg=CFG)
    assert out == original
    assert out is not original


# -- SPEC-020: integer bounds -----------------------------------------------------------

# Past CPython's default `sys.get_int_max_str_digits()` (4300), where `str()` itself raises.
_HUGE = 10**5000


def test_an_ordinary_int_is_unchanged_and_still_an_int() -> None:
    cfg = Config()
    for n in (0, 1, -1, 4200, -4200, 2**62, -(2**62)):
        out = coerce(n, cfg=cfg)
        assert out == n
        assert type(out) is int, "the common case must not become a string or lose precision"


def test_an_over_long_int_is_replaced_by_a_placeholder() -> None:
    out = coerce(_HUGE, cfg=Config())
    assert isinstance(out, str)
    assert out.startswith("<int: ~")
    assert out.endswith(" digits>")


def test_negatives_are_bounded_identically_to_positives() -> None:
    cfg = Config()
    assert coerce(-_HUGE, cfg=cfg) == coerce(_HUGE, cfg=cfg), "the sign must not shift the bound"


def test_bool_is_untouched() -> None:
    cfg = Config()
    assert coerce(True, cfg=cfg) is True, "bool is an int subclass but must stay a bool"
    assert coerce(False, cfg=cfg) is False


def test_a_float_is_untouched() -> None:
    assert coerce(1.5e308, cfg=Config()) == 1.5e308  # IEEE-754 bounds it already


def test_the_replacement_sets_the_truncated_flag() -> None:
    fields, truncated = sanitize_fields({"n": _HUGE}, cfg=Config())
    assert truncated is True
    assert isinstance(fields["n"], str)

    _, untruncated = sanitize_fields({"n": 4200}, cfg=Config())
    assert untruncated is False


def test_an_over_long_int_is_bounded_wherever_it_is_nested() -> None:
    cfg = Config()
    fields, _ = sanitize_fields(
        {
            "mapping": {"k": _HUGE},
            "list": [_HUGE],
            "tuple": (_HUGE,),
            "set": {_HUGE},
            "deep": {"a": {"b": [{"c": _HUGE}]}},
        },
        cfg=cfg,
    )
    assert isinstance(fields["mapping"]["k"], str)
    assert isinstance(fields["list"][0], str)
    assert isinstance(fields["tuple"][0], str)
    assert isinstance(next(iter(fields["set"])), str)
    assert isinstance(fields["deep"]["a"]["b"][0]["c"], str)


def test_an_int_valued_enum_member_is_bounded() -> None:
    class Big(enum.Enum):
        HUGE = _HUGE

    class BigInt(enum.IntEnum):
        SMALL = 3

    out = coerce(Big.HUGE, cfg=Config())
    assert isinstance(out, str)
    assert out.startswith("<int: ~")
    assert coerce(BigInt.SMALL, cfg=Config()) == 3, "an in-range IntEnum still degrades to its value"


def test_an_int_subclass_is_bounded() -> None:
    class Weight(int):
        pass

    out = coerce(Weight(_HUGE), cfg=Config())
    assert isinstance(out, str)
    assert coerce(Weight(7), cfg=Config()) == 7


def test_coercion_is_total_at_absurd_magnitudes() -> None:
    cfg = Config()
    for n in (10**100000, -(10**100000), 2**1000000):
        assert isinstance(coerce(n, cfg=cfg), str)  # no ValueError escapes


def test_the_result_is_always_json_serializable() -> None:
    """The guarantee SPEC-017 stated and this closes: json.dumps never refuses an event."""
    fields, _ = sanitize_fields({"n": _HUGE, "ok": 1, "deep": [{"x": 2**100000}]}, cfg=Config())
    json.dumps(fields)  # would raise ValueError on an unbounded int


def test_the_ceiling_never_admits_an_int_str_would_refuse() -> None:
    """FR-002: where the bit_length arithmetic is inexact it must err toward replacing."""
    import sys

    limit = sys.get_int_max_str_digits()
    cfg = Config()
    admitted = 0
    # Both ends of each digit band: 10**(d-1) is the smallest d-digit number, 10**d - 1 the
    # largest — the dangerous end, where an under-estimating bound would let one slip through.
    for digits in range(limit - 3, limit + 4):
        for n in (10 ** (digits - 1), 10**digits - 1):
            out = coerce(n, cfg=cfg)
            if type(out) is int:
                str(out)  # must not raise — that is the whole contract
                admitted += 1
    assert admitted, "the walk must admit some values, or it proves nothing about over-replacing"


def test_a_configured_ceiling_below_the_interpreter_limit_wins() -> None:
    cfg = Config(max_value_bytes=10)
    assert isinstance(coerce(10**20, cfg=cfg), str), "20 digits exceeds a 10-byte ceiling"
    assert coerce(123, cfg=cfg) == 123


def test_an_over_long_int_key_does_not_destroy_its_mapping() -> None:
    """A bare ``str(key)`` raised here, and the failure took every sibling key with it."""
    fields, truncated = sanitize_fields({"d": {"ok": 1, _HUGE: 2}}, cfg=Config())
    assert fields["d"]["ok"] == 1, "the sibling key must survive the hostile one"
    assert any(k.startswith("<int: ~") for k in fields["d"]), "the elided key is named, not dropped"
    assert truncated is True
    json.dumps(fields)


def test_an_ordinary_int_key_still_renders_as_its_digits() -> None:
    fields, _ = sanitize_fields({"d": {7: "seven", True: "yes"}}, cfg=Config())
    assert fields["d"]["7"] == "seven"
    assert fields["d"]["True"] == "yes", "a bool key is its name, not 1"


# -- SPEC-021 FR-003: the ceiling counts the minus sign ----------------------------------


def test_a_negative_int_is_measured_with_its_sign() -> None:
    """`Config(max_value_bytes=10)` used to admit -10**9, which renders as eleven bytes."""
    cfg = Config(max_value_bytes=10)
    assert coerce(10**9, cfg=cfg) == 10**9, "ten digits fits a ten-byte ceiling"
    out = coerce(-(10**9), cfg=cfg)
    assert isinstance(out, str), "eleven rendered bytes does not"
    assert out.startswith("<int: ~10 digits>"), "the sign is not a digit, and is not counted as one"


def test_the_sign_only_matters_at_the_boundary() -> None:
    """One rendered byte, not a halved ceiling: a negative one digit shorter still fits."""
    cfg = Config(max_value_bytes=10)
    assert coerce(-(10**8), cfg=cfg) == -(10**8), "nine digits plus a sign is ten bytes"
    for n in (-1, -4200, -(2**62)):
        out = coerce(n, cfg=Config())
        assert out == n
        assert type(out) is int, "the common path must not shift at all"


def test_a_negative_ints_replacement_still_renders_and_serializes() -> None:
    cfg = Config(max_value_bytes=10)
    fields, truncated = sanitize_fields({"n": -(10**9)}, cfg=cfg)
    assert truncated is True
    json.dumps(fields)


def test_the_ceiling_never_admits_a_negative_int_str_would_refuse() -> None:
    """SPEC-020 FR-002 still holds on the side the sign now shifts."""
    import sys

    limit = sys.get_int_max_str_digits()
    cfg = Config()
    admitted = 0
    for digits in range(limit - 3, limit + 4):
        for n in (-(10 ** (digits - 1)), -(10**digits - 1)):
            out = coerce(n, cfg=cfg)
            if type(out) is int:
                str(out)  # must not raise
                admitted += 1
    assert admitted, "the walk must admit some values, or it proves nothing about over-replacing"


def test_a_negative_key_is_bounded_with_its_sign_too() -> None:
    # A 30-byte ceiling, not the 10-byte one above: a mapping key goes through `text()` after
    # `integer()`, so at a ceiling shorter than the placeholder the placeholder is itself clipped
    # to the truncation marker. That is SPEC-017 behaviour and not what this test is about.
    cfg = Config(max_value_bytes=30)
    payload = {"d": {-(10**29): "big", 10**29: "ok", -1: "small"}}
    fields, truncated = sanitize_fields(payload, cfg=cfg)
    assert fields["d"]["-1"] == "small", "an ordinary negative key still renders as its digits"
    assert fields["d"][str(10**29)] == "ok", "30 digits unsigned still fits the same ceiling"
    assert "<int: ~30 digits>" in fields["d"], "31 rendered bytes does not, and is named"
    assert truncated is True


def test_the_sign_test_cannot_be_diverted_by_an_int_subclass() -> None:
    """`value < 0` would dispatch to user code; one hostile key would take its siblings."""

    class Hostile(int):
        def __lt__(self, other: object) -> bool:
            raise RuntimeError("boom")

    fields, _ = sanitize_fields({"d": {Hostile(-5): "v", "ok": 1}}, cfg=Config())
    assert fields["d"] == {"-5": "v", "ok": 1}, "the sibling key must survive the hostile one"

    class Liar(int):
        def __lt__(self, other: object) -> bool:
            return False  # claims to be non-negative, to buy a byte past the ceiling

    assert isinstance(coerce(Liar(-(10**9)), cfg=Config(max_value_bytes=10)), str)


# -- SPEC-031 FR-004: the interpreter's integer limit is read once per pass ------------------


def test_the_interpreter_limit_is_read_once_per_pass_not_once_per_integer(monkeypatch) -> None:
    """It cannot change during a pass, and it sat on a hot path measured per value."""
    import sys as sys_mod

    real = sys_mod.get_int_max_str_digits
    calls: list[int] = []

    def counting() -> int:
        calls.append(1)
        return real()

    monkeypatch.setattr(sys_mod, "get_int_max_str_digits", counting)
    fields, _ = sanitize_fields(
        {"a": list(range(200)), "b": {str(i): i for i in range(200)}}, cfg=Config(max_keys=500)
    )

    assert fields["a"][7] == 7, "the pass really did coerce integers"
    assert len(calls) <= 1, f"read the interpreter limit {len(calls)} times in one pass"


def test_the_boundary_behaviour_of_spec_020_is_unchanged_by_the_cached_ceiling() -> None:
    """The same integers are replaced and the same placeholders produced, sign included."""
    import sys as sys_mod

    limit = sys_mod.get_int_max_str_digits()
    at_limit = 10 ** (limit - 1)  # exactly `limit` digits
    past_limit = 10**limit  # one digit too many

    cfg = Config(max_value_bytes=limit + 10)  # the interpreter's limit is the lower of the two
    fields, truncated = sanitize_fields(
        {"at": at_limit, "past": past_limit, "neg_at": -at_limit}, cfg=cfg
    )

    assert fields["at"] == at_limit
    assert fields["past"] == f"<int: ~{limit + 1} digits>"
    assert fields["neg_at"] == f"<int: ~{limit} digits>", (
        "the sign pushes a negative at the limit one over — SPEC-021 FR-003, unchanged"
    )
    assert truncated is True


# -- SPEC-037 FR-003: NaN and Infinity are replaced, not passed through ----------------------


@pytest.mark.parametrize(
    ("value", "marker"),
    [
        (float("nan"), "<float: nan>"),
        (float("inf"), "<float: inf>"),
        (float("-inf"), "<float: -inf>"),
    ],
)
def test_each_non_finite_float_gets_a_distinguishable_marker(value: float, marker: str) -> None:
    """FR-003 AC-2. A reader must be able to tell which of the three it was.

    Collapsing all three to one token — or to `None` — would answer "there was a number here"
    and destroy the only information the field still carried.
    """
    safe, clipped = sanitize_fields({"v": value}, cfg=Config())
    assert safe == {"v": marker}
    assert clipped is True


def test_the_substitution_sets_the_truncated_marker() -> None:
    """FR-003 AC-3. Every other substitution `sanitize` makes sets it; a silent one is a lie."""
    _, clipped = sanitize_fields({"v": float("nan")}, cfg=Config())
    assert clipped is True


@pytest.mark.parametrize(
    "value",
    [0.0, -0.0, 1.5, -1.5, 1e308, -1e308, 5e-324, sys.float_info.max, sys.float_info.min],
)
def test_ordinary_floats_are_untouched(value: float) -> None:
    """FR-003 AC-4. The fix must not over-reach.

    `-0.0` and the subnormal `5e-324` are the two a naive `if not value:` or `if value != value or
    abs(value) > BIG:` gets wrong; `sys.float_info.max` is finite and must survive.
    """
    safe, clipped = sanitize_fields({"v": value}, cfg=Config())
    assert safe == {"v": value}
    assert repr(safe["v"]) == repr(value), "negative zero must not become positive zero"
    assert clipped is False


def test_non_finite_floats_are_replaced_everywhere_a_value_can_sit() -> None:
    """FR-003 AC-5. Nested, in a sequence, and as a mapping key.

    The key path needed its own branch: a float key went through `str()` and rendered as the
    bare string `"nan"` — valid JSON, so no sink would complain, but it lost that the key was a
    float and set no marker. SPEC-020 had to handle keys separately for the same reason.
    """
    safe, clipped = sanitize_fields(
        {
            "nested": {"x": float("nan")},
            "seq": [float("inf"), 2.0],
            "keyed": {float("-inf"): "v"},
        },
        cfg=Config(),
    )
    assert safe == {
        "nested": {"x": "<float: nan>"},
        "seq": ["<float: inf>", 2.0],
        "keyed": {"<float: -inf>": "v"},
    }
    assert clipped is True


def test_a_hostile_float_subclass_cannot_choose_what_the_library_prints() -> None:
    """The placeholder is computed by the library, never by the value (`_FLOAT_REPR`).

    `f"<float: {value}>"` calls `format(value, "")`, which is the value's own `__str__` — so a
    `float` subclass chose the text. Measured before the fix: a subclass naming its source
    emitted `<float: inf from probe-7 (token=sk-live-...)>`, one returning two megabytes emitted
    all of it against a `max_value_bytes` of 8192, and one that raised took every sibling key
    with it. Three separate failures from one interpolation, and `integer()` beside it already
    reads `<` through an unbound `int.__lt__` on exactly this reasoning.
    """
    cfg = Config()

    class Leaky(float):
        def __str__(self) -> str:
            return "inf from probe-7 (token=sk-live-DEADBEEF)"

    class Huge(float):
        def __str__(self) -> str:
            return "x" * 2_000_000

    class Boom(float):
        def __str__(self) -> str:
            raise RuntimeError("a __str__ the library must not depend on")

    safe, clipped = sanitize_fields({"leak": Leaky("inf"), "huge": Huge("nan")}, cfg=cfg)
    assert safe == {"leak": "<float: inf>", "huge": "<float: nan>"}
    assert clipped is True

    # The same subclass in key position. Review found that dropping `key()`'s `text()` wrap
    # survived the whole suite -- true of the *pre-fix* code, where it was the only thing bounding
    # a two-megabyte marker. It is not true any more: once the library computes the marker,
    # `real()` cannot return a long string, so the wrap is defence in depth and **no test can
    # distinguish it**. Recorded rather than pinned with an assertion that cannot fail.
    keyed, _ = sanitize_fields({"k": {Huge("inf"): 1}}, cfg=cfg)
    assert keyed["k"] == {"<float: inf>": 1}

    # And a raising `__str__` must not take its siblings down with it.
    survived, _ = sanitize_fields({"k": {Boom("nan"): 1, "survivor": 2}}, cfg=cfg)
    assert survived["k"] == {"<float: nan>": 1, "survivor": 2}


def test_a_float_subclass_and_a_float_enum_are_covered_too() -> None:
    """`float` was in `_PLAIN_SCALARS`, which the exact-type *and* subclass paths both read.

    Removing it from that set without giving the `Enum` branch its own float rule would have sent
    a float-valued enum member to `str()`, which is a different bug in the same edit.
    """

    class Ratio(float):
        pass

    class Level(enum.Enum):
        BROKEN = float("nan")
        SUBCLASSED = Ratio("inf")

    safe, clipped = sanitize_fields(
        {"sub": Ratio("nan"), "enum": Level.BROKEN, "enum_sub": Level.SUBCLASSED}, cfg=Config()
    )
    assert safe == {
        "sub": "<float: nan>",
        "enum": "<float: nan>",
        # An Enum member that is a float *subclass* matched neither `type(member) is float` nor
        # the int arm, and fell through to `str(value)` -- JSON-safe, so nothing complained, but
        # the marker and `truncated` were both lost. The arm tests `isinstance` now.
        "enum_sub": "<float: inf>",
    }
    assert clipped is True


def test_the_finite_path_is_not_measurably_more_expensive() -> None:
    """FR-003 AC-7, measured against a neighbour rather than against the clock.

    An absolute wall-clock budget is a race with the machine — this repo has been bitten by
    timing tests failing on their own setup. The honest comparison is the sibling rule: `integer`
    does `bit_length()`, two multiplications, a division and an unbound `int.__lt__`, where this
    does one `math.isfinite`. Floats must not cost more than that, with a wide margin so the
    assertion is about the algorithm and not about scheduling noise.

    **Measured sensitivity:** it catches an order-of-magnitude regression, not a small one — a
    calibration run survived 5 extra `str()` calls per value and failed at 10. Stated rather than
    left for "wide margin" to imply a tight bound. Note also that `Config().max_keys` is 256, so
    only the first 256 of the 2000 keys are coerced per call.
    """
    cfg = Config()
    floats = {f"k{i}": float(i) + 0.5 for i in range(2000)}
    ints = {f"k{i}": i for i in range(2000)}

    start = perf_counter()
    for _ in range(20):
        sanitize_fields(floats, cfg=cfg)
    float_cost = perf_counter() - start

    start = perf_counter()
    for _ in range(20):
        sanitize_fields(ints, cfg=cfg)
    int_cost = perf_counter() - start

    assert float_cost < int_cost * 3, (
        f"the float path cost {float_cost:.4f}s against the integer path's {int_cost:.4f}s; "
        "a finite float should take one isfinite() call, not a rendering-size computation"
    )


# -- SPEC-054 FR-001 / FR-004: surrogates, hostile str subclasses, hostile keys ---------------


class _BadEncode(str):
    """A str subclass whose own text operations raise — the measurement must not consult them."""

    def encode(self, *args: object, **kwargs: object) -> bytes:
        raise RuntimeError("encode boom")

    def __str__(self) -> str:
        raise RuntimeError("str boom")


class _KeyBoom:
    def __str__(self) -> str:
        raise RuntimeError("keyboom")

    def __hash__(self) -> int:
        return 1

    def __eq__(self, other: object) -> bool:
        return False


class _BadInt(int):
    def __str__(self) -> str:
        raise RuntimeError("no digits for you")


def test_truncate_tail_replaces_a_surrogate() -> None:
    """FR-001 AC-3, the unit half: the tail clipper gives the same answers as the head clipper."""
    bad = os.fsdecode(b"file-\xff.txt")
    assert truncate_tail(bad, 8192) == ("file-\ufffd.txt", True)
    out, flag = truncate_tail("y" * 30 + "\ud800a", 20)
    assert (out, flag) == (TRUNCATION_MARKER + "yy\ufffda", True), "6 bytes kept beside the marker"
    out.encode("utf-8")


def test_a_str_subclass_cannot_divert_the_measurement() -> None:
    """FR-001 AC-5: `str.__str__` is unbound, so a hostile `encode`/`__str__` never runs."""
    out, altered = truncate_str(_BadEncode("hello"), 100)
    assert (out, altered) == ("hello", False)
    assert type(out) is str
    assert type(truncate_tail(_BadEncode("hello"), 100)[0]) is str


def test_a_str_subclass_value_whose_str_raises_is_plain_text() -> None:
    """FR-001: the value branch no longer calls `str(value)`, which ran the subclass's `__str__`."""
    assert _one(_BadEncode("val")) == "val"
    assert type(_one(_BadEncode("val"))) is str
    assert _truncated(_BadEncode("val")) is False


def test_the_replacement_sets_truncated() -> None:
    """FR-001 AC-6, the unit half: a substitution nobody can see is a change to the data."""
    assert _truncated("\udcff") is True
    assert _truncated("plain") is False
    assert _one("a\udcffb") == "a\ufffdb"


def test_undecodable_bytes_are_replaced_and_marked() -> None:
    """FR-001: the same byte is marked whether it arrives as str or as bytes."""
    assert _one(b"\xff\xfe\x80") == "\ufffd\ufffd\ufffd"
    assert _truncated(b"\xff\xfe\x80") is True
    assert _truncated(bytearray(b"\xff")) is True
    assert _truncated(memoryview(b"\xff")) is True


def test_plain_bytes_are_not_marked() -> None:
    assert _one(b"plain") == "plain"
    assert _truncated(b"plain") is False


def test_a_hostile_key_costs_only_itself() -> None:
    """FR-004 AC-1: the audit's `{'boom': '<unserializable: dict>'}` becomes a key placeholder."""
    fields, flag = sanitize_fields({"boom": {"sib": 1, _KeyBoom(): 2}}, cfg=CFG)
    assert fields == {"boom": {"sib": 1, "<unserializable key: _KeyBoom>": 2}}
    assert flag is True


def test_an_int_subclass_key_whose_str_raises_gets_the_key_placeholder() -> None:
    """FR-004 AC-2: the integer branch's own `str()` is inside the guard too."""
    fields, flag = sanitize_fields({"m": {"sib": 1, _BadInt(5): 2}}, cfg=CFG)
    assert fields == {"m": {"sib": 1, "<unserializable key: _BadInt>": 2}}
    assert flag is True


def test_a_str_subclass_key_renders_as_plain_text() -> None:
    """FR-004 AC-2, the other half: FR-001's `str.__str__` reaches the key before the guard does."""
    fields, flag = sanitize_fields({"m": {_BadEncode("k"): 1}}, cfg=CFG)
    assert fields == {"m": {"k": 1}}
    assert flag is False


def test_a_hostile_key_is_isolated_at_any_depth() -> None:
    """FR-004 AC-3: at the top level of `fields=` and three levels down."""
    top, top_flag = sanitize_fields({"sib": 1, _KeyBoom(): 2}, cfg=CFG)
    assert top == {"sib": 1, "<unserializable key: _KeyBoom>": 2} and top_flag is True
    deep, deep_flag = sanitize_fields({"a": {"b": {"c": {"sib": 1, _KeyBoom(): 2}}}}, cfg=CFG)
    assert deep == {"a": {"b": {"c": {"sib": 1, "<unserializable key: _KeyBoom>": 2}}}}
    assert deep_flag is True


def test_two_hostile_keys_of_one_type_collide_and_the_collision_is_marked() -> None:
    """The accepted limit, pinned so it is a decision: the later wins, and `truncated` is set."""
    fields, flag = sanitize_fields({"m": {"sib": 1, _KeyBoom(): 2, _KeyBoom(): 3}}, cfg=CFG)
    assert fields == {"m": {"sib": 1, "<unserializable key: _KeyBoom>": 3}}
    assert flag is True


def test_a_placeholder_honours_the_ceiling_on_both_forms() -> None:
    """FR-004 AC-4: `type.__name__` is writable, so the placeholder goes through `text()` too."""
    huge = type("X" * 9000, (), {})
    hostile = type("Y" * 9000, (), {"__str__": lambda self: (_ for _ in ()).throw(RuntimeError())})
    cfg = Config(max_value_bytes=64)
    fields, flag = sanitize_fields({"v": huge(), "k": {hostile(): 1}}, cfg=cfg)
    value_form = fields["v"]
    assert isinstance(value_form, str)
    assert len(value_form.encode("utf-8")) <= 64 and value_form.endswith(TRUNCATION_MARKER)
    (key_form,) = fields["k"]  # type: ignore[misc]
    assert len(key_form.encode("utf-8")) <= 64 and key_form.endswith(TRUNCATION_MARKER)
    assert flag is True
