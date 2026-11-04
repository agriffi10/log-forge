"""SPEC-055 FR-006 — the adversarial assembly corpus, asserting invariant 8 value by value.

Every row is driven through both delivery paths (invariant 6) — `lf.info` inside a `@trace`,
delivered by the worker, and `lf.info` with no span open, emitted synchronously — and every row
is held to the whole of invariant 8's observable, not the one clause it is about. The silence
rows (`truncated` expected absent) are what keep the walker honest: a corpus of only-failures
cannot see a false positive.

The assertion order in `_assert_safe` is deliberate: the strict-encode check runs first so that
reverting FR-001's replacement is reported as an encode failure rather than as a mismatched pin.
Failure messages render values through `ascii()`, because a lone surrogate in a plain `repr`
crashes the xdist worker that tries to report it (`execnet` cannot serialise the message), and
an `INTERNALERROR` is not a readable failure.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, NamedTuple
from uuid import UUID

import pytest

import log_foundry as lf
from log_foundry.sanitize import TRUNCATION_MARKER
from log_foundry.sinks.memory import MemorySink

if TYPE_CHECKING:
    from log_foundry.config import Config

# -- the hostile types -------------------------------------------------------------------------


class _BadIter(dict):  # type: ignore[type-arg]
    def __iter__(self) -> Iterator[object]:
        raise RuntimeError("iter boom")

    def items(self) -> object:  # type: ignore[override]
        raise RuntimeError("items boom")


class _LyingLen(dict):  # type: ignore[type-arg]
    def __len__(self) -> int:
        return 10**9


class _BadStr:
    def __str__(self) -> str:
        raise RuntimeError("str boom")

    def __repr__(self) -> str:
        raise RuntimeError("repr boom")


class _BadEncode(str):
    def encode(self, *args: object, **kwargs: object) -> bytes:
        raise RuntimeError("encode boom")

    def __str__(self) -> str:
        raise RuntimeError("str boom")


class _Color(Enum):
    RED = "red"


class _Weird(Enum):
    OBJ = object()


@dataclass
class _DC:
    a: int = 1
    note: str = "an attribute a repr would print"


class _RaisingMapping(Mapping[object, object]):
    def __getitem__(self, key: object) -> object:
        raise KeyError(key)

    def __iter__(self) -> Iterator[object]:
        raise RuntimeError("map iter")

    def __len__(self) -> int:
        return 1


class _KeyBoom:
    def __str__(self) -> str:
        raise RuntimeError("keyboom")

    def __hash__(self) -> int:
        return 1

    def __eq__(self, other: object) -> bool:
        return False


_HUGE_NAME = type("N" * 20_000, (), {})
_HUGE_NAME_HOSTILE = type(
    "H" * 20_000, (), {"__str__": lambda self: (_ for _ in ()).throw(RuntimeError("no"))}
)


def _raise(*args: object) -> str:
    raise RuntimeError("hostile")


# -- SPEC-063 FR-002: objects that claim a type they are not, and their real-type twins ---------


class _ClaimsBool:
    @property  # type: ignore[misc]
    def __class__(self) -> type:  # type: ignore[override]
        return bool


class _ClaimsFloat:
    @property  # type: ignore[misc]
    def __class__(self) -> type:  # type: ignore[override]
        return float

    def __float__(self) -> float:
        return 1.0


class _ClaimsStr:
    @property  # type: ignore[misc]
    def __class__(self) -> type:  # type: ignore[override]
        return str


class _IntLikeMeta(type):
    """A metaclass that hashes and compares equal to ``int``, which a type-set lookup believes."""

    def __hash__(cls) -> int:
        return hash(int)

    def __eq__(cls, other: object) -> bool:
        return True


class _MetaInt(metaclass=_IntLikeMeta):
    pass


class _EnumFloatImpostor(Enum):
    M = _ClaimsFloat()


class _EnumMetaImpostor(Enum):
    M = _MetaInt()


class _EnumBoolImpostor(Enum):
    M = _ClaimsBool()


class _EnumStrClaim(Enum):
    M = _ClaimsStr()


class _LyingFloat(float):
    def __float__(self) -> float:
        return 9.0

    def __repr__(self) -> str:
        return "LIE"

    def __str__(self) -> str:
        raise RuntimeError("str")


class _LyingInt(int):
    def __int__(self) -> int:
        return 9

    def __index__(self) -> int:
        return 9

    def __repr__(self) -> str:
        return "LIE"

    def __str__(self) -> str:
        raise RuntimeError("str")


class _EnumFloatSubclass(Enum):
    R = _LyingFloat(1.5)


class _StrLyingFloat(float):
    def __str__(self) -> str:
        return "LIE"


class _IntColor(int, Enum):
    RED = 1


class _PInt(int):
    def __str__(self) -> str:
        return "P"


class _BoolEnum(Enum):
    T = True
    F = False


def _stored_named(metaclass_name: property) -> type:
    """A class stored as ``Stored`` whose metaclass answers ``__name__`` with ``metaclass_name``."""
    meta = type("_Meta", (type,), {"__name__": metaclass_name})
    return meta("Stored", (), {"__str__": _raise})  # type: ignore[no-any-return]


_STORED_NAME_RAISES = _stored_named(property(_raise))
_STORED_NAME_42 = _stored_named(property(lambda cls: 42))
_STORED_NAME_RENAMED = _stored_named(property(lambda cls: "renamed"))


class _HijackName(str):
    """A ``str`` subclass for a stored ``__name__``: ``str()`` raises and formatting renames."""

    def __str__(self) -> str:
        raise RuntimeError("str")

    def __format__(self, spec: str) -> str:
        return "HIJACK"


_STORED_HIJACK = type("Stored", (), {"__str__": _raise})
_STORED_HIJACK.__name__ = _HijackName("Stored")


class _DictProxy:
    """A transparent proxy of a ``dict``: it claims the type and renders through ``items()``."""

    @property  # type: ignore[misc]
    def __class__(self) -> type:  # type: ignore[override]
        return dict

    def items(self) -> object:
        return {"a": 1}.items()


class _DatetimeProxy:
    """A transparent proxy of a ``datetime``, rendered through its ``isoformat()``."""

    @property  # type: ignore[misc]
    def __class__(self) -> type:  # type: ignore[override]
        return datetime

    def isoformat(self) -> str:
        return "2026-01-01T00:00:00"


class _ProxiedInt:
    @property  # type: ignore[misc]
    def __class__(self) -> type:  # type: ignore[override]
        return int


class _TupleClaimsUUID(tuple):  # type: ignore[type-arg]
    """A real ``tuple`` subclass claiming ``UUID``: walked by its real type, never rendered."""

    @property  # type: ignore[misc]
    def __class__(self) -> type:  # type: ignore[override]
        return UUID


class _TupleClaimsDecimal(tuple):  # type: ignore[type-arg]
    @property  # type: ignore[misc]
    def __class__(self) -> type:  # type: ignore[override]
        return Decimal


class _BytesClaimsDecimal(bytes):
    @property  # type: ignore[misc]
    def __class__(self) -> type:  # type: ignore[override]
        return Decimal


class _ClaimsEnumWithValue:
    """Mixed into a real builtin: claims ``Enum`` and answers ``.value`` with a bare object."""

    @property  # type: ignore[misc]
    def __class__(self) -> type:  # type: ignore[override]
        return Enum

    @property
    def value(self) -> object:
        return object()


class _TupleClaimsEnum(_ClaimsEnumWithValue, tuple):  # type: ignore[misc, type-arg]
    pass


class _DictClaimsEnum(_ClaimsEnumWithValue, dict):  # type: ignore[misc, type-arg]
    pass


class _CorpusSecret:
    def __repr__(self) -> str:
        return "SECRET"


def _exact_bools(expected: list[bool]) -> Callable[[object], bool]:
    """Pins a list of booleans by type as well as value, since ``1 == True``."""
    return _pinned(
        lambda v: v == expected and isinstance(v, list) and all(type(x) is bool for x in v)
    )


_CYCLE_DICT: dict[str, object] = {}
_CYCLE_DICT["self"] = _CYCLE_DICT
_CYCLE_LIST: list[object] = []
_CYCLE_LIST.append(_CYCLE_LIST)

_NINE_MILLION_DIGITS = Decimal("1" * 9_000_000)
_TEN_MILLION_X = "x" * 10_000_000


def _deep(levels: int) -> dict[str, object]:
    root: dict[str, object] = {}
    cur = root
    for _ in range(levels):
        nxt: dict[str, object] = {}
        cur["d"] = nxt
        cur = nxt
    return root


_BAD = os.fsdecode(b"file-\xff.txt")
_MAX_VALUE_BYTES = 8192
_MAX_KEYS = 256
_MAX_DEPTH = 8
_MAX_STACK_BYTES = 32768


# -- the table ---------------------------------------------------------------------------------


class Row(NamedTuple):
    """One corpus case: where the value goes, what it is, and what must come out."""

    id: str
    slot: str
    value: object
    truncated: bool
    expect: object


def _pinned(predicate: Callable[[object], bool]) -> Callable[[object], bool]:
    """Marks an expectation as a predicate, for values too large to pin literally."""
    return predicate


def _is_placeholder(kind: str) -> Callable[[object], bool]:
    return _pinned(lambda v: isinstance(v, str) and v.startswith(f"<{kind}"))


ROWS: list[Row] = [
    # the round-two audit's values, as fields={"v": value}
    Row("cycle_dict", "value", _CYCLE_DICT, False, {"self": "<circular>"}),
    Row("cycle_list", "value", _CYCLE_LIST, False, ["<circular>"]),
    Row("bad_iter", "value", _BadIter(a=1), False, "<unserializable: _BadIter>"),
    Row("lying_len", "value", _LyingLen(a=1), False, {"a": 1}),
    Row("raising_mapping", "value", _RaisingMapping(), False, "<unserializable: _RaisingMapping>"),
    Row("bad_str", "value", _BadStr(), False, "<unserializable: _BadStr>"),
    Row("bad_encode_str_subclass", "value", _BadEncode("x"), False, "x"),
    Row("bad_bytes", "value", b"\xff\xfe\x80", True, "���"),
    Row("surrogate", "value", "\ud800", True, "�"),
    Row("surrogate_key", "key", "\ud800", True, {"�": 1}),
    Row("big_str", "value", "x" * 10_000_000, True, _pinned(lambda v: isinstance(v, str))),
    Row(
        "big_dict",
        "value",
        {str(i): i for i in range(100_000)},
        True,
        _pinned(lambda v: isinstance(v, dict) and len(v) == _MAX_KEYS),
    ),
    Row("deep", "value", _deep(1000), True, _pinned(lambda v: isinstance(v, dict))),
    Row("decimal", "value", Decimal("0.1"), False, "0.1"),
    Row("dt_tz", "value", datetime(2026, 1, 1, tzinfo=UTC), False, "2026-01-01T00:00:00+00:00"),
    Row("dt_naive", "value", datetime(2026, 1, 1), False, "2026-01-01T00:00:00"),
    Row("enum", "value", _Color.RED, False, "red"),
    Row("weird_enum", "value", _Weird.OBJ, False, "_Weird.OBJ"),
    Row("dataclass", "value", _DC(), False, "<unserializable: _DC>"),
    Row("set", "value", {1}, False, [1]),
    Row("frozenset", "value", frozenset({3}), False, [3]),
    Row("tuple", "value", (1, "a"), False, [1, "a"]),
    Row("bool_int", "value", [True, 1, False, 0], False, [True, 1, False, 0]),
    Row("neg0", "value", -0.0, False, -0.0),
    Row("e400", "value", 1e400, True, "<float: inf>"),
    Row("nan", "value", float("nan"), True, "<float: nan>"),
    Row("bigint", "value", 10**100_000, True, _is_placeholder("int")),
    Row("negbig", "value", -(10**100_000), True, _is_placeholder("int")),
    Row(
        "mixed_keys",
        "value",
        {1: "a", (1, 2): "b", None: "c", 2.5: "d", _KeyBoom(): "f"},
        True,
        {
            "1": "a",
            "[1, 2]": "b",
            "None": "c",
            "2.5": "d",
            "<unserializable key: _KeyBoom>": "f",
        },
    ),
    Row("bool_key", "value", {True: "e"}, False, {"True": "e"}),
    Row("nfc", "value", "é", False, "é"),
    Row("nfd", "value", "é", False, "é"),
    Row("bytearray", "value", bytearray(b"ab"), False, "ab"),
    Row("memoryview", "value", memoryview(b"cd"), False, "cd"),
    Row("int_max_digits", "value", 10**5000, True, _is_placeholder("int")),
    Row("int_4999_over_interpreter_limit", "value", 10**4999, True, _is_placeholder("int")),
    Row("int_4000_renders", "value", 10**4000, False, 10**4000),
    Row("big_key_str", "key", "k" * 20_000, True, _pinned(lambda v: isinstance(v, dict))),
    # SPEC-055's additions
    Row("fsdecode_value", "value", _BAD, True, "file-�.txt"),
    Row("fsdecode_key", "key", _BAD, True, {"file-�.txt": 1}),
    Row("fsdecode_message", "message", _BAD, True, "file-�.txt"),
    Row("fsdecode_span", "span", _BAD, True, "file-�.txt"),
    Row("fsdecode_in_bytes", "value", b"file-\xff.txt", True, "file-�.txt"),
    Row("bad_encode_message", "message", _BadEncode("hello"), False, "hello"),
    Row("bad_encode_key", "key", _BadEncode("k"), False, {"k": 1}),
    Row(
        "hostile_key_beside_sibling",
        "value",
        {"sib": 1, _KeyBoom(): 2},
        True,
        {"sib": 1, "<unserializable key: _KeyBoom>": 2},
    ),
    Row(
        "huge_type_name_value",
        "value",
        _HUGE_NAME(),
        True,
        _pinned(lambda v: isinstance(v, str) and v.startswith("<unserializable: NNN")),
    ),
    Row(
        "huge_type_name_key",
        "key",
        _HUGE_NAME_HOSTILE(),
        True,
        _pinned(lambda v: isinstance(v, dict) and next(iter(v)).startswith("<unserializable key")),
    ),
    # SPEC-057's additions: a pass visits at most 131,072 values and charges at most 16 MiB
    Row(
        "node_budget_over",
        "value",
        [[[0, 0]] * 256] * 256,
        True,
        _pinned(lambda v: "<node limit>" in set(_walk(v))),
    ),
    Row("node_budget_silence_256x256", "value", [[0] * 256] * 256, False, [[0] * 256] * 256),
    Row(
        "size_budget_decimal_twice",
        "value",
        [_NINE_MILLION_DIGITS, _NINE_MILLION_DIGITS],
        True,
        ["1" * 8178 + TRUNCATION_MARKER, "<size limit>"],
    ),
    Row(
        "size_budget_clipped_str_control",
        "value",
        [_TEN_MILLION_X] * 100,
        True,
        ["x" * 8178 + TRUNCATION_MARKER] * 100,
    ),
    Row("size_budget_silence_100x8192", "value", ["x" * 8192] * 100, False, ["x" * 8192] * 100),
    # controls: ordinary values must leave `truncated` absent
    Row("plain_text", "value", "hello", False, "hello"),
    Row("plain_message", "message", "hello", False, "hello"),
    Row("plain_span", "span", "work", False, "work"),
    Row("plain_int", "value", 7, False, 7),
    Row("plain_nested", "value", {"a": [1, {"b": "c"}]}, False, {"a": [1, {"b": "c"}]}),
    # SPEC-063 FR-002: a route that returns the caller's object admits it by exact type
    Row("impostor_bool", "value", _ClaimsBool(), False, "<unserializable: _ClaimsBool>"),
    Row("impostor_float", "value", _ClaimsFloat(), False, "<unserializable: _ClaimsFloat>"),
    Row("impostor_metaclass_int", "value", _MetaInt(), False, "<unserializable: _MetaInt>"),
    Row(
        "enum_float_impostor",
        "value",
        _EnumFloatImpostor.M,
        False,
        "<unserializable: _EnumFloatImpostor>",
    ),
    Row("enum_metaclass_impostor", "value", _EnumMetaImpostor.M, False, "_EnumMetaImpostor.M"),
    Row("enum_bool_impostor", "value", _EnumBoolImpostor.M, False, "_EnumBoolImpostor.M"),
    Row("enum_str_claim", "value", _EnumStrClaim.M, False, "<unserializable: _EnumStrClaim>"),
    Row("float_subclass_lying", "value", _LyingFloat(1.5), False, 1.5),
    Row("int_subclass_lying", "value", _LyingInt(7), False, 7),
    Row("float_subclass_nan", "value", _LyingFloat("nan"), True, "<float: nan>"),
    Row("enum_float_subclass", "value", _EnumFloatSubclass.R, False, 1.5),
    Row(
        "exact_bools_and_bool_enum_values",
        "value",
        [True, False, _BoolEnum.T, _BoolEnum.F],
        False,
        _exact_bools([True, False, True, False]),
    ),
    Row(
        "float_impostor_key",
        "value",
        {_ClaimsFloat(): 1, "sib": 2},
        True,
        {"<unserializable key: _ClaimsFloat>": 1, "sib": 2},
    ),
    Row("float_subclass_key_lying_str", "key", _StrLyingFloat(1.5), False, {"1.5": 1}),
    Row("int_enum_key", "key", _IntColor.RED, False, {"_IntColor.RED": 1}),
    Row("int_subclass_key_own_str", "key", _PInt(3), False, {"P": 1}),
    Row(
        "bool_impostor_key",
        "value",
        {_ClaimsBool(): 1, "sib": 2},
        True,
        {"<unserializable key: _ClaimsBool>": 1, "sib": 2},
    ),
    Row(
        "stored_name_raises_value",
        "value",
        _STORED_NAME_RAISES(),
        False,
        "<unserializable: Stored>",
    ),
    Row(
        "stored_name_raises_key",
        "key",
        _STORED_NAME_RAISES(),
        True,
        {"<unserializable key: Stored>": 1},
    ),
    Row("stored_name_42_value", "value", _STORED_NAME_42(), False, "<unserializable: Stored>"),
    Row("stored_name_42_key", "key", _STORED_NAME_42(), True, {"<unserializable key: Stored>": 1}),
    Row(
        "stored_name_renamed_value",
        "value",
        _STORED_NAME_RENAMED(),
        False,
        "<unserializable: Stored>",
    ),
    Row(
        "stored_name_renamed_key",
        "key",
        _STORED_NAME_RENAMED(),
        True,
        {"<unserializable key: Stored>": 1},
    ),
    Row(
        "stored_name_str_subclass_value",
        "value",
        {"sib": 1, "x": _STORED_HIJACK(), "z": 2},
        False,
        {"sib": 1, "x": "<unserializable: Stored>", "z": 2},
    ),
    Row(
        "stored_name_str_subclass_key",
        "value",
        {_STORED_HIJACK(): 1, "sib": 2},
        True,
        {"<unserializable key: Stored>": 1, "sib": 2},
    ),
    Row("proxy_dict", "value", _DictProxy(), False, {"a": 1}),
    Row("proxy_datetime", "value", _DatetimeProxy(), False, "2026-01-01T00:00:00"),
    Row("proxied_int", "value", _ProxiedInt(), False, "<unserializable: _ProxiedInt>"),
    Row("real_tuple_claiming_uuid", "value", _TupleClaimsUUID((1, "a")), False, [1, "a"]),
    Row("real_tuple_claiming_decimal", "value", _TupleClaimsDecimal((1, "a")), False, [1, "a"]),
    Row("real_bytes_claiming_decimal", "value", _BytesClaimsDecimal(b"ab"), False, "ab"),
    Row(
        "real_tuple_claiming_enum",
        "value",
        _TupleClaimsEnum((1, _CorpusSecret())),
        False,
        [1, "<unserializable: _CorpusSecret>"],
    ),
    Row(
        "real_dict_claiming_enum",
        "value",
        _DictClaimsEnum(a=_CorpusSecret()),
        False,
        {"a": "<unserializable: _CorpusSecret>"},
    ),
]

_ROUTES = ("in_span", "orphan")


# -- the walker and the one assertion function ----------------------------------------------


def _walk(obj: object) -> Iterator[str]:
    """Yields every str in an event — keys and values, at every depth."""
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, dict):
        for key, value in obj.items():
            yield from _walk(key)
            yield from _walk(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from _walk(item)


def _ceiling_for(path: str) -> int:
    return _MAX_STACK_BYTES if path == "error.stack" else _MAX_VALUE_BYTES


def _strings_with_paths(obj: object, path: str = "") -> Iterator[tuple[str, str]]:
    if isinstance(obj, str):
        yield path, obj
    elif isinstance(obj, dict):
        for key, value in obj.items():
            yield from _strings_with_paths(key, f"{path}<key>")
            yield from _strings_with_paths(value, f"{path}.{key}" if path else str(key))
    elif isinstance(obj, list):
        for item in obj:
            yield from _strings_with_paths(item, f"{path}[]")


def _assert_safe(
    event: dict[str, object],
    *,
    control_keys: set[str],
    row: Row,
) -> None:
    """The whole of invariant 8's observable, in the order that names the right clause first."""
    for text in _walk(event):
        try:
            str.encode(text, "utf-8")
        except UnicodeEncodeError:
            pytest.fail(f"{row.id}: a string left assembly unencodable: {ascii(text)[:80]}")
    for text in _walk(event):
        assert type(text) is str, f"{row.id}: a str subclass reached the event: {type(text)}"
    _assert_exact_types(event, row=row)
    json.dumps(event, allow_nan=False)
    for path, text in _strings_with_paths(event):
        size = len(text.encode("utf-8"))
        assert size <= _ceiling_for(path), f"{row.id}: {path} is {size} bytes"
    assert set(event) - {"truncated"} == control_keys - {"truncated"}, (
        f"{row.id}: top-level keys changed: {sorted(set(event) ^ control_keys)}"
    )
    if row.truncated:
        assert event.get("truncated") is True, f"{row.id}: truncated should be set"
    else:
        assert "truncated" not in event, f"{row.id}: truncated should be absent"
    actual = _actual(event, row.slot)
    if callable(row.expect):
        assert row.expect(actual), f"{row.id}: pin failed on {ascii(actual)[:120]}"
    else:
        assert actual == row.expect, f"{row.id}: {ascii(actual)[:120]} != {ascii(row.expect)[:120]}"


_EXACT_LEAVES = (str, int, float, bool, type(None))


def _assert_exact_types(obj: object, *, row: Row, path: str = "") -> None:
    """SPEC-063 FR-002: every leaf is an exact JSON scalar, every container an exact dict or list.

    Exact, by ``type() is``, because a subclass instance or an impostor that reached the event is
    what this observable rules out, and ``isinstance`` would admit both. Identity, never ``in``: a
    class whose metaclass compares equal to everything is ``in`` any tuple of types, which is the
    very impostor this walks for. The message names the path and the type's qualified name, never
    the leaf itself, whose ``repr`` may be hostile.
    """
    kind = type(obj)
    if kind is dict:
        for key, value in obj.items():  # type: ignore[attr-defined]
            assert type(key) is str, f"{row.id}: {path}<key> is a {type(key).__qualname__}"
            _assert_exact_types(value, row=row, path=f"{path}.{key}" if path else key)
    elif kind is list:
        for item in obj:  # type: ignore[attr-defined]
            _assert_exact_types(item, row=row, path=f"{path}[]")
    else:
        exact = any(kind is leaf for leaf in _EXACT_LEAVES)
        assert exact, f"{row.id}: {path} is a {kind.__qualname__}"


def _actual(event: dict[str, object], slot: str) -> object:
    if slot == "message":
        return event["message"]
    if slot == "span":
        return event["function"]
    fields = event["fields"]
    assert isinstance(fields, dict)
    return fields["v"]


# -- driving both routes ---------------------------------------------------------------------


def _emit(row: Row, route: str, sink: MemorySink) -> dict[str, object]:
    """Runs one row through one route and returns the event it produced."""

    def call() -> None:
        if row.slot == "value":
            lf.info("m", fields={"v": row.value})
        elif row.slot == "key":
            lf.info("m", fields={"v": {row.value: 1}})
        elif row.slot == "message":
            lf.info(row.value)  # type: ignore[arg-type]
        else:
            lf.info("m")

    if route == "orphan":
        call()
    else:
        name = row.value if row.slot == "span" else "work"
        lf.trace(name=name)(call)()  # type: ignore[arg-type]
        assert lf.flush(timeout=10)
    if row.slot == "message":
        candidates = [e for e in sink.events if e["message"] not in ("span.start", "span.end")]
    elif row.slot == "span":
        candidates = [e for e in sink.events if e["message"] == "span.start"]
    else:
        candidates = [e for e in sink.events if e["message"] == "m"]
    assert len(candidates) == 1, f"{row.id}/{route}: expected one event, got {len(candidates)}"
    return candidates[0]


@pytest.fixture
def sink() -> MemorySink:
    sink = MemorySink()
    lf.configure(
        service="corpus",
        sink=sink,
        max_value_bytes=_MAX_VALUE_BYTES,
        max_keys=_MAX_KEYS,
        max_depth=_MAX_DEPTH,
        max_stack_bytes=_MAX_STACK_BYTES,
    )
    return sink


def _control_keys(route: str, slot: str, sink: MemorySink) -> set[str]:
    control = Row("control", slot, "control", False, "control")
    keys = set(_emit(control, route, sink))
    sink.events.clear()
    return keys


@pytest.mark.parametrize("route", _ROUTES)
@pytest.mark.parametrize("row", ROWS, ids=[r.id for r in ROWS])
def test_every_row_keeps_invariant_8_on_both_paths(row: Row, route: str, sink: MemorySink) -> None:
    if row.slot == "span" and route == "orphan":
        pytest.skip("a span name has no orphan route: the orphan span is named after its message")
    before = lf.health()
    control_keys = _control_keys(route, row.slot, sink)
    event = _emit(row, route, sink)
    _assert_safe(event, control_keys=control_keys, row=row)
    after = lf.health()
    assert (after.in_span_lost, after.orphan_lost) == (before.in_span_lost, before.orphan_lost), (
        f"{row.id}/{route}: the event was absorbed and lost rather than built"
    )


# -- guards on the guard ----------------------------------------------------------------------


def test_the_table_is_as_large_as_the_spec_requires() -> None:
    """FR-006 AC-1: at least 45 rows, at least ten expecting silence, every id unique."""
    assert len(ROWS) >= 45
    assert sum(1 for r in ROWS if not r.truncated) >= 10
    assert len({r.id for r in ROWS}) == len(ROWS)


def test_the_walker_reaches_keys() -> None:
    """FR-006 AC-3: a walker that skipped keys would pass a surrogate key silently."""
    assert "\ud800" in set(_walk({"a": {"\ud800": [1, {"k": "v"}]}}))
    assert list(_walk({"k": ["v", {"x": "y"}]})) == ["k", "v", "x", "y"]


def test_the_assertion_function_rejects_an_unsafe_event() -> None:
    """The one assertion function must itself be able to fail, on each clause it checks."""
    keys = {"message", "fields", "level"}
    good: dict[str, object] = {"message": "m", "fields": {"v": "hello"}, "level": "INFO"}
    _assert_safe(good, control_keys=keys, row=Row("ok", "value", "hello", False, "hello"))
    with pytest.raises(pytest.fail.Exception):
        bad = {**good, "fields": {"v": "\udcff"}}
        _assert_safe(bad, control_keys=keys, row=Row("sur", "value", "x", False, "\udcff"))
    with pytest.raises(AssertionError, match="subclass"):
        sub = {**good, "fields": {"v": _BadEncode("hello")}}
        _assert_safe(sub, control_keys=keys, row=Row("sub", "value", "x", False, "hello"))
    with pytest.raises(AssertionError, match="truncated should be absent"):
        marked = {**good, "truncated": True}
        _assert_safe(marked, control_keys=keys, row=Row("t", "value", "hello", False, "hello"))
    with pytest.raises(AssertionError, match="pin failed"):
        _assert_safe(good, control_keys=keys, row=Row("p", "value", "x", False, lambda v: False))
    with pytest.raises(AssertionError, match=r"\.v is a _LyingFloat"):
        sub_float = {**good, "fields": {"v": _LyingFloat(1.5)}}
        _assert_safe(sub_float, control_keys=keys, row=Row("f", "value", 1.5, False, 1.5))
    with pytest.raises(AssertionError, match=r"\.v is a tuple"):
        tupled = {**good, "fields": {"v": (1,)}}
        _assert_safe(tupled, control_keys=keys, row=Row("t", "value", (1,), False, (1,)))


def test_the_control_row_reflects_config_defaults(sink: MemorySink) -> None:
    """The ceilings the table assumes are the ones the fixture configured."""
    cfg: Config = lf.get_config()
    assert (cfg.max_value_bytes, cfg.max_keys, cfg.max_depth, cfg.max_stack_bytes) == (
        _MAX_VALUE_BYTES,
        _MAX_KEYS,
        _MAX_DEPTH,
        _MAX_STACK_BYTES,
    )
