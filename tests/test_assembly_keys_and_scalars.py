"""SPEC-063 FR-001 and FR-002 — container keys under the pass budgets, and exact-type routing.

Invariant 8 is the promise held here: the event is safe by construction at assembly, every cut
is marked, and no route hands a sink the caller's own object. Invariant 1 is the hang tests: a
key that never returned is a call that fails the caller. Invariant 6 is why every test that
builds an event runs on both delivery paths. Every name imported here exists before SPEC-063, so
the module collects against the old ``sanitize.py`` and each test there fails for its own stated
reason rather than at import.

The hang-shaped tests are guarded by ``test_assembly_budget``'s SIGALRM deadline, which raises a
``BaseException`` the coercer's ``except Exception`` cannot swallow, and assert on CPU time
(``time.process_time()``) with a generous budget. No failure message renders a hostile key or
value: they name a route, a type or a number.
"""

from __future__ import annotations

import ast
import json
import pathlib
import sys
import time
import tracemalloc
from collections.abc import Iterator, Mapping
from decimal import Decimal
from enum import Enum, StrEnum
from typing import NamedTuple
from uuid import UUID

import pytest

import log_foundry as lf
from log_foundry import sanitize
from log_foundry.config import Config
from log_foundry.sanitize import (
    TRUNCATION_MARKER,
    _coerce_unbounded,
    _Coercer,
    sanitize_fields,
)
from test_assembly_budget import _ROUTES, _configure, _deadline, _emit

_NODE = "<node limit>"
_SIZE = "<size limit>"
_DEPTH = "<depth limit>"
_SANITIZE = pathlib.Path(sanitize.__file__)


class _Pairs(Mapping[object, object]):
    """A caller ``Mapping`` yielding the given pairs, for keys a ``dict`` cannot hold or build."""

    def __init__(self, pairs: list[tuple[object, object]]) -> None:
        self.pairs = pairs

    def __getitem__(self, key: object) -> object:
        for candidate, value in self.pairs:
            if candidate is key:
                return value
        raise KeyError("absent")

    def __iter__(self) -> Iterator[object]:
        return iter([candidate for candidate, _ in self.pairs])

    def __len__(self) -> int:
        return len(self.pairs)


def _dag(levels: int) -> frozenset[object]:
    """``f = frozenset({f, (f, i)})`` applied ``levels`` times: its ``repr`` doubles per level."""
    f: frozenset[object] = frozenset()
    for i in range(levels):
        f = frozenset({f, (f, i)})
    return f


_DAG_40 = _dag(40)


# -- FR-001: a container key is the JSON of its coerced form ----------------------------------


class _P(NamedTuple):
    x: int
    y: int


class _HashableDict(dict):  # type: ignore[type-arg]
    def __hash__(self) -> int:  # type: ignore[override]
        return 1


class _HashableList(list):  # type: ignore[type-arg]
    def __hash__(self) -> int:  # type: ignore[override]
        return 2


class _TupleEnum(tuple, Enum):  # type: ignore[type-arg]
    A = (1, 2)


@pytest.mark.parametrize(
    ("key", "text"),
    [
        ((1, 2), "[1, 2]"),
        (("a",), '["a"]'),
        (frozenset({1}), "[1]"),
        (((1, 2), (3,)), "[[1, 2], [3]]"),
        (_P(x=1, y=2), "[1, 2]"),
        ((None, True, 1.5, "é"), '[null, true, 1.5, "é"]'),
        ((), "[]"),
        (_HashableDict(a=1), '{"a": 1}'),
    ],
    ids=[
        "pair",
        "one-str",
        "frozenset",
        "nested",
        "namedtuple",
        "scalars",
        "empty",
        "dict-subclass",
    ],
)
def test_container_keys_are_written_as_the_json_of_their_coerced_form(
    key: object, text: str
) -> None:
    """FR-001 AC1: each key in its own mapping, since ``(1, 2)``, ``_P(1, 2)`` compare equal."""
    fields, flag = sanitize_fields({"k": _Pairs([(key, 1)])}, cfg=Config())
    assert fields == {"k": {text: 1}}
    assert flag is False


def test_distinct_container_keys_stay_distinct() -> None:
    """FR-001 AC1: ``(1, "2")`` and ``(1, 2)`` give two keys; a type-name key would give one."""
    fields, flag = sanitize_fields({"k": {(1, "2"): "a", (1, 2): "b"}}, cfg=Config())
    assert fields == {"k": {'[1, "2"]': "a", "[1, 2]": "b"}}
    assert flag is False


def test_an_enum_container_mixin_key_keeps_its_str() -> None:
    """FR-001 AC1 (silence): a ``(tuple, Enum)`` member is no container key; it keeps ``str()``."""
    fields, flag = sanitize_fields({"k": {_TupleEnum.A: 1}}, cfg=Config())
    assert fields == {"k": {"_TupleEnum.A": 1}}
    assert flag is False


def test_a_container_key_is_clipped_to_max_value_bytes_and_marked() -> None:
    """FR-001 AC2: the key's text is clipped to the ceiling with the marker, and marked."""
    fields, flag = sanitize_fields({"k": {("x" * 100,): 1}}, cfg=Config(max_value_bytes=40))
    assert fields == {"k": {'["' + "x" * 24 + TRUNCATION_MARKER: 1}}
    assert flag is True


def test_a_container_key_clipped_only_as_text_is_marked() -> None:
    """The clip's own mark: five 8-byte members fit 40 bytes each, their 60-byte text does not.

    No member is clipped, so nothing but the key text's clip can set ``truncated``. The clipped
    shape of the previous test cannot tell, because its member is clipped as well.
    """
    key = tuple("x" * 8 for _ in range(5))
    text = json.dumps(list(key))
    assert len(text) == 60, "the premise: the text exceeds the 40-byte ceiling"
    fields, flag = sanitize_fields({"k": {key: 1}}, cfg=Config(max_value_bytes=40))
    assert fields == {"k": {text[:26] + TRUNCATION_MARKER: 1}}
    assert flag is True


def test_a_container_key_obeys_max_keys() -> None:
    """FR-001 AC2: a 300-member key is capped at the default 256 members, and marked."""
    fields, flag = sanitize_fields({"k": {tuple(range(300)): 1}}, cfg=Config())
    (text,) = fields["k"]
    assert text.startswith("["), "the key is not the JSON of its members"
    assert json.loads(text) == list(range(256))
    assert flag is True


class _Secret:
    def __repr__(self) -> str:
        return "SECRET"

    def __str__(self) -> str:
        return "SECRET"


def test_a_member_repr_is_never_written_inside_a_container_key() -> None:
    """FR-001 AC3: a member is a value, so it is its type-name placeholder, never its ``repr``."""
    fields, _ = sanitize_fields({"k": {(_Secret(),): 1}}, cfg=Config())
    assert fields == {"k": {'["<unserializable: _Secret>"]': 1}}
    assert "SECRET" not in json.dumps(fields)


def _circular() -> _Pairs:
    mapping = _Pairs([])
    mapping.pairs.append((_HashableList([mapping]), 1))
    return mapping


@pytest.mark.parametrize(
    ("max_depth", "value", "expected", "flag"),
    [
        (2, {(1,): 0}, {'["<depth limit>"]': 0}, True),
        (1, {(1,): 0}, {'"<depth limit>"': _DEPTH}, True),
        (8, None, {'["<circular>"]': 1}, False),
    ],
    ids=["member-past-the-depth", "key-past-the-depth", "circular"],
)
def test_a_container_key_obeys_max_depth_and_the_cycle_check(
    max_depth: int, value: object, expected: object, flag: bool
) -> None:
    """FR-001 AC4: the key's members sit at the entry's value depth; a cycle is ``<circular>``."""
    inner = _circular() if value is None else value
    fields, truncated = sanitize_fields({"k": inner}, cfg=Config(max_depth=max_depth))
    assert fields == {"k": expected}
    assert truncated is flag


@pytest.mark.parametrize(
    ("budget", "value", "expected", "flag"),
    [
        (5, {(1, 2): 0}, {"[1, 2]": 0}, False),
        (4, {(1, 2): 0}, {"[1, 2]": _NODE}, True),
        (3, {(1, 2): 0}, {_NODE: _NODE}, True),
        (2, {(1, 2): 0}, {_NODE: _NODE}, True),
        (1, {(1, 2): 0}, {_NODE: _NODE}, True),
        (1, {(): 0}, {_NODE: _NODE}, True),
    ],
    ids=["5", "4", "3", "2", "1", "empty-key-at-1"],
)
def test_a_container_key_spends_the_node_budget(
    monkeypatch: pytest.MonkeyPatch, budget: int, value: object, expected: object, flag: bool
) -> None:
    """FR-001 AC5: the inner dict is node 1, the key 2, its members 3 and 4, the value 5.

    The empty key has no member to spend a node, so only the key's own node stops the pass at a
    budget of 1; without it the key would be ``"[]"`` and its value ``<node limit>``.
    """
    monkeypatch.setattr(sanitize, "_MAX_NODES", budget)
    fields, truncated = sanitize_fields({"a": value}, cfg=Config())
    assert fields == {"a": expected}
    assert truncated is flag


@pytest.mark.parametrize(
    ("member", "value", "expected", "flag"),
    [
        ("x" * 9, "y", {'["xxxxxxxxx"]': "y"}, False),
        ("x" * 10, "y", {'["xxxxxxxxxx"]': _SIZE}, True),
        ("x" * 11, None, {_SIZE: _SIZE}, True),
    ],
    ids=["9-whole", "10-value-cut", "11-key-cut"],
)
def test_a_container_key_spends_the_byte_budget_and_its_text_is_not_charged_again(
    monkeypatch: pytest.MonkeyPatch, member: str, value: object, expected: object, flag: bool
) -> None:
    """FR-001 AC6: at a budget of 10 the member is charged, its 13-character text is not."""
    monkeypatch.setattr(sanitize, "_MAX_RENDERED_BYTES", 10)
    fields, truncated = sanitize_fields({(member,): value}, cfg=Config())
    assert fields == expected
    assert truncated is flag


class _BadIterList(list):  # type: ignore[type-arg]
    def __hash__(self) -> int:  # type: ignore[override]
        return 3

    def __iter__(self) -> Iterator[object]:
        raise RuntimeError("iter boom")


@pytest.mark.parametrize("route", _ROUTES)
def test_a_container_key_whose_iteration_raises_costs_no_sibling(route: str) -> None:
    """FR-001 AC7: the key's own failure is the key placeholder, marked; the sibling is kept."""
    sink = _configure()
    pairs = _Pairs([(_BadIterList([1]), 1), ("sib", 2)])
    event = _emit(route, {"v": pairs}, sink)
    assert event["fields"] == {"v": {"<unserializable key: _BadIterList>": 1, "sib": 2}}
    assert event.get("truncated") is True


class _PosesAsTuple:
    """Claims ``tuple`` through ``__class__``, with no usable ``__iter__``."""

    @property  # type: ignore[misc]
    def __class__(self) -> type:  # type: ignore[override]
        return tuple

    def __str__(self) -> str:
        return "S"


def test_only_a_real_container_type_is_a_container_key() -> None:
    """FR-001 AC8 (silence): a ``__class__`` claim is not a container key; it keeps ``str()``."""
    fields, flag = sanitize_fields({"k": {_PosesAsTuple(): 1}}, cfg=Config())
    assert fields == {"k": {"S": 1}}
    assert flag is False


def test_other_keys_render_as_before() -> None:
    """FR-001 AC8 (silence): every key that is not a container keeps its text."""
    keys = {b"ab": 1, None: 2, True: 3, Decimal("1.5"): 4, 2.5: 5, 7: 6}
    fields, flag = sanitize_fields({"k": keys}, cfg=Config())
    assert fields == {"k": {"b'ab'": 1, "None": 2, "True": 3, "1.5": 4, "2.5": 5, "7": 6}}
    assert flag is False


class _RefusingBytes(bytes):
    """A ``bytes`` key whose rendering raises and whose own length lies: priced, never rendered."""

    def __str__(self) -> str:
        raise RuntimeError("rendered")

    def __len__(self) -> int:
        return 0


class _RefusingBytearray(bytearray):
    def __hash__(self) -> int:  # type: ignore[override]
        return 4

    def __str__(self) -> str:
        raise RuntimeError("rendered")

    def __len__(self) -> int:
        return 0


class _HashableBytearray(bytearray):
    def __hash__(self) -> int:  # type: ignore[override]
        return 5


@pytest.mark.parametrize("kind", [_RefusingBytes, _RefusingBytearray], ids=["bytes", "bytearray"])
def test_a_bytes_like_key_is_priced_through_its_unbound_length(
    monkeypatch: pytest.MonkeyPatch, kind: type
) -> None:
    """FR-001 AC9: 25 bytes price at 103, over a budget of 100, whatever ``__len__`` says.

    ``__str__`` raises, not ``__repr__``: ``str()`` of a bytes-like subclass calls the C repr and
    never the subclass's ``__repr__``, so a raising ``__repr__`` renders fine and crosses the
    budget anyway. A rendered key here would be the key placeholder.
    """
    monkeypatch.setattr(sanitize, "_MAX_RENDERED_BYTES", 100)
    pairs = _Pairs([(kind(b"\x00" * 25), 1)])
    fields, flag = sanitize_fields(pairs, cfg=Config())  # type: ignore[arg-type]
    assert fields == {_SIZE: _SIZE}
    assert flag is True


def test_a_bytes_key_priced_exactly_at_the_budget_is_rendered_and_charged_its_repr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-001 AC9: 24 zero bytes price at 99 and fit 99; the charge is the repr's actual 99."""
    monkeypatch.setattr(sanitize, "_MAX_RENDERED_BYTES", 99)
    key = b"\x00" * 24
    assert len(str(key)) == 99, "the premise: four characters a byte plus b''"
    coercer = _Coercer(Config())
    out = coercer.mapping({key: None}, -1)
    assert out == {str(key): None}
    assert coercer._charged == 99
    assert coercer.truncated is False


def test_a_short_bytes_key_whose_repr_would_fit_is_refused_by_its_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-001 AC9: ``b"a" * 25`` renders in 28 characters but prices at 103, so it is refused."""
    monkeypatch.setattr(sanitize, "_MAX_RENDERED_BYTES", 100)
    assert len(str(b"a" * 25)) == 28, "the premise: its repr would fit"
    fields, flag = sanitize_fields({b"a" * 25: None}, cfg=Config())
    assert fields == {_SIZE: _SIZE}
    assert flag is True


@pytest.mark.parametrize(
    ("key", "budget", "expected"),
    [
        (_RefusingBytes(b"\x00" * 30), 100, {_SIZE: _SIZE}),
        (b"ab", 11, {"b'ab'": None}),
        (b"ab", 10, {_SIZE: _SIZE}),
    ],
    ids=["30-bytes-refused-at-100", "2-bytes-fit-at-11", "2-bytes-refused-at-10"],
)
def test_a_bytes_like_key_is_priced_at_its_repr_upper_bound(
    monkeypatch: pytest.MonkeyPatch, key: bytes, budget: int, expected: object
) -> None:
    """A bytes key is priced at four characters per byte plus three, the most its repr can be.

    Thirty bytes price at 123, over a budget of 100, so the key is refused unrendered, where a
    lower-bound price of 30 would render it and its ``__str__`` would raise. ``b"ab"`` prices at
    exactly 11: it fits a budget of 11, is rendered and charged its actual five characters, and is
    refused at 10.
    """
    monkeypatch.setattr(sanitize, "_MAX_RENDERED_BYTES", budget)
    fields, _ = sanitize_fields({key: None}, cfg=Config())
    assert fields == expected


@pytest.mark.parametrize("size", [64 << 20, 16 << 20], ids=["64MiB", "16MiB"])
@pytest.mark.parametrize("kind", [bytes, _HashableBytearray], ids=["bytes", "bytearray"])
def test_a_huge_bytes_like_key_is_never_rendered(kind: type, size: int) -> None:
    """FR-001 AC9: 64 or 16 MiB of zero bytes as a key is refused unrendered (repr: four times)."""
    key = kind(b"\x00" * size)
    tracemalloc.start()
    started = time.process_time()
    try:
        fields, flag = sanitize_fields({key: 1}, cfg=Config())
        cpu = time.process_time() - started
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert fields == {_SIZE: _SIZE}
    assert flag is True
    assert cpu < 2, f"{cpu:.2f} s of CPU"
    assert peak < 8 << 20, f"peak {peak >> 20} MiB"


def test_a_transforms_coercion_renders_container_keys_by_the_same_rule() -> None:
    """FR-001 AC10: the types-only pass writes the same text, and never clips it."""
    result, _ = _coerce_unbounded({"fields": {(1, 2): 3}})
    assert result == {"fields": {"[1, 2]": 3}}
    result, _ = _coerce_unbounded({"fields": {("x" * 100_000,): 3}})
    assert result == {"fields": {'["' + "x" * 100_000 + '"]': 3}}


@pytest.mark.parametrize(
    ("max_depth", "expected"), [(8, None), (64, {_NODE: _NODE})], ids=["depth-cut", "node-cut"]
)
@pytest.mark.parametrize("route", _ROUTES)
def test_a_frozenset_dag_key_returns_on_both_paths(
    route: str, max_depth: int, expected: object
) -> None:
    """FR-001 AC11: forty levels of a shared DAG as one key return, cut and marked.

    At the default depth of 8 the depth limit cuts the key before the node budget; at 64 the
    node budget does, and the key's mapping is the stop placeholder. Before SPEC-063 the key was
    rendered by its builtin ``repr``, doubling per level, and never returned.
    """
    sink = _configure(max_depth=max_depth)
    started = time.process_time()
    event = _emit(route, {"v": {_DAG_40: 1}}, sink, deadline=5)
    cpu = time.process_time() - started
    assert cpu < 2, f"{route}: {cpu:.2f} s of CPU"
    assert event.get("truncated") is True, route
    fields = event["fields"]
    assert isinstance(fields, dict)
    if expected is not None:
        assert fields["v"] == expected, route
    else:
        (text,) = fields["v"]
        assert text.startswith("["), route


def test_a_tuple_key_of_shared_megabyte_strings_is_bounded() -> None:
    """FR-001 AC12: 64 references to one 1 MiB string charge their clips, never a 64 MiB repr."""
    shared = "x" * (1 << 20)
    key = tuple([shared] * 64)
    tracemalloc.start()
    started = time.process_time()
    try:
        fields, flag = sanitize_fields({"k": {key: 1}}, cfg=Config())
        cpu = time.process_time() - started
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert fields == {"k": {'["' + "x" * 8176 + TRUNCATION_MARKER: 1}}
    assert flag is True
    assert cpu < 2, f"{cpu:.2f} s of CPU"
    assert peak < 8 << 20, f"peak {peak >> 20} MiB"


def test_a_tuple_key_of_shared_tuples_stops_at_the_byte_budget() -> None:
    """FR-001 AC12: 64 × 64 leaves of 8,192 bytes: 2,048 fit the budget exactly, the next stops."""
    shared = "x" * (1 << 20)
    inner = tuple([shared] * 64)
    key = tuple([inner] * 64)
    coercer = _Coercer(Config())
    started = time.process_time()
    with _deadline(5, "the 64 x 64 key"):
        out = coercer.mapping({key: 1}, -1)
    cpu = time.process_time() - started
    assert out == {_SIZE: _SIZE}
    assert coercer.truncated is True
    assert coercer._charged == sanitize._MAX_RENDERED_BYTES == 2048 * 8192
    assert cpu < 2, f"{cpu:.2f} s of CPU"


class _TupleClaimsUUID(tuple):  # type: ignore[type-arg]
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


_RENDERED_CLAIMS = [_TupleClaimsUUID, _TupleClaimsDecimal]


@pytest.mark.parametrize("kind", _RENDERED_CLAIMS, ids=["uuid", "decimal"])
@pytest.mark.parametrize("route", _ROUTES)
def test_a_real_container_claiming_a_rendered_type_is_walked(route: str, kind: type) -> None:
    """FR-002: a real tuple subclass claiming ``UUID`` or ``Decimal`` is walked, never ``str()``-ed.

    As a value the ``isinstance`` branches for those types called ``str()``, the builtin ``repr``:
    it never returned around the DAG and it wrote a member's ``repr``.
    """
    sink = _configure()
    started = time.process_time()
    event = _emit(route, {"v": kind((_DAG_40,))}, sink, deadline=5)
    assert time.process_time() - started < 2, route
    assert event.get("truncated") is True, route
    secret = _emit(route, {"v": kind((_Secret(),))}, sink)
    assert secret["fields"] == {"v": ["<unserializable: _Secret>"]}
    assert "SECRET" not in json.dumps(secret)


@pytest.mark.parametrize("kind", _RENDERED_CLAIMS, ids=["uuid", "decimal"])
@pytest.mark.parametrize("route", _ROUTES)
def test_a_container_key_claiming_a_rendered_type_is_still_walked(route: str, kind: type) -> None:
    """FR-001: the key is walked by its real type, never through the value route's ``str()``."""
    sink = _configure()
    started = time.process_time()
    event = _emit(route, {"v": {kind((_DAG_40,)): 1}}, sink, deadline=5)
    assert time.process_time() - started < 2, route
    assert event.get("truncated") is True, route
    fields = event["fields"]
    assert isinstance(fields, dict)
    (text,) = fields["v"]
    assert text.startswith("[") and "<depth limit>" in text, f"{route}: not the walked text"
    secret = _emit(route, {"v": {kind((_Secret(),)): 1}}, sink)
    assert secret["fields"] == {"v": {'["<unserializable: _Secret>"]': 1}}
    assert "SECRET" not in json.dumps(secret)


def test_a_real_bytes_subclass_claiming_decimal_is_read_to_the_ceiling() -> None:
    """FR-002: a real ``bytes`` subclass claiming ``Decimal`` is read to its ceiling only."""
    value = _BytesClaimsDecimal(b"\x00" * (64 << 20))
    tracemalloc.start()
    try:
        fields, flag = sanitize_fields({"v": value}, cfg=Config())
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert fields == {"v": "\x00" * 8178 + TRUNCATION_MARKER}
    assert flag is True
    assert peak < 8 << 20, f"peak {peak >> 20} MiB"


# -- FR-002: a route that returns the caller's object admits it by exact type ------------------


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
    def __hash__(cls) -> int:
        return hash(int)

    def __eq__(cls, other: object) -> bool:
        return True


class _MetaInt(metaclass=_IntLikeMeta):
    pass


@pytest.mark.parametrize(
    "kind", [_ClaimsBool, _ClaimsFloat, _MetaInt], ids=["bool", "float", "metaclass"]
)
@pytest.mark.parametrize("route", _ROUTES)
def test_an_impostor_scalar_value_is_a_type_placeholder_on_both_paths(
    route: str, kind: type
) -> None:
    """FR-002 AC1: each is ``<unserializable: T>`` naming its real class, unmarked, and dumps."""
    sink = _configure()
    event = _emit(route, {"v": kind()}, sink)
    assert event["fields"] == {"v": f"<unserializable: {kind.__qualname__}>"}
    json.dumps(event, allow_nan=False)
    assert "truncated" not in event


class _EnumFloat(Enum):
    M = _ClaimsFloat()


class _EnumMeta(Enum):
    M = _MetaInt()


class _EnumBool(Enum):
    M = _ClaimsBool()


class _EnumStr(Enum):
    M = _ClaimsStr()


@pytest.mark.parametrize(
    ("member", "expected"),
    [
        (_EnumFloat.M, "<unserializable: _EnumFloat>"),
        (_EnumMeta.M, "_EnumMeta.M"),
        (_EnumBool.M, "_EnumBool.M"),
        (_EnumStr.M, "<unserializable: _EnumStr>"),
    ],
    ids=["float-impostor", "metaclass", "bool-impostor", "str-claim"],
)
def test_an_enum_member_admits_its_value_by_exact_type(member: Enum, expected: str) -> None:
    """FR-002 AC2: a claimed ``float`` or ``str`` names the Enum; any other claim is its text."""
    fields, _ = sanitize_fields({"v": member}, cfg=Config())
    assert fields == {"v": expected}


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


@pytest.mark.parametrize("route", _ROUTES)
def test_a_scalar_subclass_is_written_as_its_exact_base_type(route: str) -> None:
    """FR-002 AC3: the stored value, as an exact ``float`` or ``int``, whatever the dunders say."""
    sink = _configure()
    event = _emit(
        route,
        {
            "f": _LyingFloat(1.5),
            "i": _LyingInt(7),
            "e": _EnumFloatSubclass.R,
            "n": _LyingFloat("nan"),
        },
        sink,
    )
    fields = event["fields"]
    assert isinstance(fields, dict)
    assert (type(fields["f"]), fields["f"]) == (float, 1.5)
    assert (type(fields["i"]), fields["i"]) == (int, 7)
    assert (type(fields["e"]), fields["e"]) == (float, 1.5)
    assert fields["n"] == "<float: nan>"
    assert event.get("truncated") is True, "the nan is a marked substitution"
    unmarked = _emit(route, {"f": _LyingFloat(1.5), "i": _LyingInt(7)}, sink)
    assert "truncated" not in unmarked


class _BoolEnum(Enum):
    T = True
    F = False


@pytest.mark.parametrize("route", _ROUTES)
def test_an_exact_bool_stays_a_bool(route: str) -> None:
    """FR-002: ``True`` and ``False``, as values and as an Enum's values, stay exact ``bool``s.

    The exact-``bool`` branch is otherwise silent to remove: ``int.__int__(True)`` is ``1``,
    ``1 == True``, and the corpus walker admits an ``int``. The JSON text tells them apart.
    """
    sink = _configure()
    event = _emit(route, {"t": True, "f": False, "et": _BoolEnum.T, "ef": _BoolEnum.F}, sink)
    fields = event["fields"]
    assert isinstance(fields, dict)
    assert [type(fields[k]) for k in ("t", "f", "et", "ef")] == [bool] * 4
    assert json.dumps(fields) == '{"t": true, "f": false, "et": true, "ef": false}'


class _StrLyingFloat(float):
    def __str__(self) -> str:
        return "LIE"


class _IntColor(int, Enum):
    RED = 1


class _PInt(int):
    def __str__(self) -> str:
        return "P"


def test_float_keys_are_converted_and_int_subclass_keys_keep_their_str() -> None:
    """FR-002 AC4: a float key is its exact float or the placeholder; an int subclass keeps str().

    An object claiming ``bool`` as a key is the key placeholder: the integer branch admits by
    ``type(key) is not bool``, so the claim reaches the unbound reads, which refuse it.
    """
    fields, flag = sanitize_fields(
        {"k": {_ClaimsFloat(): 1, _StrLyingFloat(1.5): 2, _IntColor.RED: 3, _PInt(4): 4, "sib": 5}},
        cfg=Config(),
    )
    assert fields == {
        "k": {
            "<unserializable key: _ClaimsFloat>": 1,
            "1.5": 2,
            "_IntColor.RED": 3,
            "P": 4,
            "sib": 5,
        }
    }
    assert flag is True
    claimed, flag = sanitize_fields({"k": {_ClaimsBool(): 1, "sib": 2}}, cfg=Config())
    assert claimed == {"k": {"<unserializable key: _ClaimsBool>": 1, "sib": 2}}
    assert flag is True


class _IntClaimsBool(int):
    """A real ``int`` subclass claiming ``bool``, which ``not isinstance(key, bool)`` believed."""

    @property  # type: ignore[misc]
    def __class__(self) -> type:  # type: ignore[override]
        return bool

    __hash__ = int.__hash__


def test_an_int_subclass_key_claiming_bool_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    """FR-002: three million digits under no interpreter limit are a placeholder, not rendered."""
    previous = sys.get_int_max_str_digits()
    sys.set_int_max_str_digits(0)
    try:
        key = _IntClaimsBool(10**3_000_000)
        started = time.process_time()
        with _deadline(5, "the int key claiming bool"):
            fields, flag = sanitize_fields({"k": {key: 1}}, cfg=Config())
        cpu = time.process_time() - started
    finally:
        sys.set_int_max_str_digits(previous)
    (text,) = fields["k"]
    assert text.startswith("<int: ~"), text[:40]
    assert flag is True
    assert cpu < 2, f"{cpu:.2f} s of CPU"


def _stored(metaclass_name: property) -> type:
    meta = type("_Meta", (type,), {"__name__": metaclass_name})

    def _raise(self: object) -> str:
        raise RuntimeError("str")

    return meta("Stored", (), {"__str__": _raise})  # type: ignore[no-any-return]


def _raising(cls: type) -> str:
    raise RuntimeError("name")


@pytest.mark.parametrize(
    "metaclass_name",
    [property(_raising), property(lambda cls: 42), property(lambda cls: "renamed")],
    ids=["raises", "42", "renamed"],
)
def test_a_placeholder_reads_the_stored_type_name(metaclass_name: property) -> None:
    """FR-002 AC5: the stored name, ``Stored``, whatever the metaclass's ``__name__`` says."""
    cls = _stored(metaclass_name)
    fields, _ = sanitize_fields({"v": cls(), "k": {cls(): 1}}, cfg=Config())
    assert fields == {"v": "<unserializable: Stored>", "k": {"<unserializable key: Stored>": 1}}


class _HijackName(str):
    def __str__(self) -> str:
        raise RuntimeError("str")

    def __format__(self, spec: str) -> str:
        return "HIJACK"


def test_a_str_subclass_stored_name_is_read_as_exact_text() -> None:
    """FR-002 AC6: a ``str`` subclass stored as the name neither raises nor renames anything."""

    def _raise(self: object) -> str:
        raise RuntimeError("str")

    cls = type("Stored", (), {"__str__": _raise})
    cls.__name__ = _HijackName("Stored")
    fields, _ = sanitize_fields({"sib": 1, "v": cls(), "k": {cls(): 1, "x": 2}}, cfg=Config())
    assert fields == {
        "sib": 1,
        "v": "<unserializable: Stored>",
        "k": {"<unserializable key: Stored>": 1, "x": 2},
    }


def _spoofable_memberships(source: str) -> list[str]:
    """Every ``in``/``not in`` test of ``type(...)`` or ``kind``, and each ``_PLAIN_SCALARS``."""
    found = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Compare) and any(
            isinstance(op, (ast.In, ast.NotIn)) for op in node.ops
        ):
            left = node.left
            if isinstance(left, ast.Name) and left.id == "kind":
                found.append(f"line {node.lineno}: kind in ...")
            elif (
                isinstance(left, ast.Call)
                and isinstance(left.func, ast.Name)
                and left.func.id == "type"
            ):
                found.append(f"line {node.lineno}: type(...) in ...")
        if isinstance(node, ast.Name) and node.id == "_PLAIN_SCALARS":
            found.append(f"line {node.lineno}: _PLAIN_SCALARS")
    return found


def test_sanitize_routes_no_type_through_a_spoofable_membership() -> None:
    """FR-002 AC9: a type-set lookup consults the metaclass's ``__hash__`` and ``__eq__``."""
    assert _spoofable_memberships(_SANITIZE.read_text(encoding="utf-8")) == []


def test_the_membership_scan_reddens_on_the_old_shape() -> None:
    """FR-002 AC9: the scan finds each old shape and stays silent on an identity or an id lookup."""
    old = (
        "_PLAIN_SCALARS = frozenset({int, bool})\n"
        "if kind in _PLAIN_SCALARS:\n    pass\n"
        "if type(member) in S or member is None:\n    pass\n"
        "if type(x) not in S:\n    pass\n"
    )
    found = _spoofable_memberships(old)
    assert sorted(found) == [
        "line 1: _PLAIN_SCALARS",
        "line 2: _PLAIN_SCALARS",
        "line 2: kind in ...",
        "line 4: type(...) in ...",
        "line 6: type(...) in ...",
    ]
    assert (
        _spoofable_memberships("if ident in self._parents:\n    pass\nif kind is bool:\n    pass\n")
        == []
    )


def test_the_module_imports_only_names_older_than_spec_063() -> None:
    """The baseline run's premise: this module collects against the pre-SPEC-063 ``sanitize``."""
    for name in ("TRUNCATION_MARKER", "_coerce_unbounded", "_Coercer", "sanitize_fields"):
        assert hasattr(sanitize, name), name
    assert lf.info is not None


class _TupleClaimsEnum(tuple):  # type: ignore[type-arg]
    """A real ``tuple`` subclass claiming ``Enum``, with no ``.value`` of its own."""

    @property  # type: ignore[misc]
    def __class__(self) -> type:  # type: ignore[override]
        return Enum


def test_a_container_key_claiming_enum_is_still_walked() -> None:
    """FR-001: the key is walked by its real type, even under an ``Enum`` claim (silence-shaped).

    ``_dispatch`` enters its ``Enum`` branch only for a real ``Enum`` or a non-builtin, so a route
    through it would walk this key too; it no longer tells the two routings apart. The ``int``
    and ``float`` claims of ``test_a_container_key_claiming_a_scalar_is_still_walked`` do.
    """
    fields, flag = sanitize_fields({"k": {_TupleClaimsEnum((1, 2)): 1}}, cfg=Config())
    assert fields == {"k": {"[1, 2]": 1}}
    assert flag is False


class _BytearrayClaimsBytes(bytearray):
    @property  # type: ignore[misc]
    def __class__(self) -> type:  # type: ignore[override]
        return bytes


def test_a_real_bytearray_claiming_bytes_is_read_through_its_own_slice() -> None:
    """FR-002: the bytes route reads by real type, so the ``bytes`` slice never refuses it."""
    fields, flag = sanitize_fields({"v": _BytearrayClaimsBytes(b"ab")}, cfg=Config())
    assert fields == {"v": "ab"}
    assert flag is False


class _ClaimsAs(tuple):  # type: ignore[type-arg]
    """A real ``tuple`` subclass whose ``__class__`` is set per subclass by ``claimed``."""

    claimed: type = object

    @property  # type: ignore[misc]
    def __class__(self) -> type:  # type: ignore[override]
        return self.claimed


@pytest.mark.parametrize("claimed", [str, int, float], ids=["str", "int", "float"])
def test_a_container_key_claiming_a_scalar_is_still_walked(claimed: type) -> None:
    """FR-001: the container rule runs ahead of ``_key``'s claim-based ``str``, ``int``, ``float``.

    Behind them, ``isinstance`` believed the claim and the claimed type's unbound read refused the
    tuple, so the key was the key placeholder rather than the JSON of its members. The ``int`` and
    ``float`` claims are also what tells this route from one through ``_dispatch``, whose ``int``
    and ``float`` branches refuse the claim the same way.
    """
    kind = type(f"_TupleClaims{claimed.__name__}", (_ClaimsAs,), {"claimed": claimed})
    fields, flag = sanitize_fields({"k": {kind((1, 2)): 1}}, cfg=Config())
    assert fields == {"k": {"[1, 2]": 1}}
    assert flag is False


class _StrEnum(StrEnum):
    A = "a"


class _FloatEnum(float, Enum):
    A = 1.5


class _IntEnumKey(int, Enum):
    A = 1


def test_enum_keys_keep_their_text() -> None:
    """FR-002: a ``StrEnum`` key is its value; a ``(float, Enum)`` or ``(int, Enum)`` key its name.

    ``(float, Enum)`` is excluded from the float-key conversion, so it reads ``_FloatEnum.A`` as at
    ``71e42dd`` rather than ``1.5``, exactly as ``(int, Enum)`` keeps ``_IntEnumKey.A``.
    """
    keys = {_StrEnum.A: 1, _FloatEnum.A: 2, _IntEnumKey.A: 3}
    fields, flag = sanitize_fields({"k": keys}, cfg=Config())
    assert fields == {"k": {"a": 1, "_FloatEnum.A": 2, "_IntEnumKey.A": 3}}
    assert flag is False


class _ClaimsEnumWithValue:
    """Mixed into a real builtin: claims ``Enum`` and answers ``.value`` with a bare object."""

    @property  # type: ignore[misc]
    def __class__(self) -> type:  # type: ignore[override]
        return Enum

    @property
    def value(self) -> object:
        return object()


class _TupleClaimsEnumValue(_ClaimsEnumWithValue, tuple):  # type: ignore[misc, type-arg]
    pass


class _DictClaimsEnumValue(_ClaimsEnumWithValue, dict):  # type: ignore[misc, type-arg]
    pass


class _BytesClaimsEnumValue(_ClaimsEnumWithValue, bytes):  # type: ignore[misc]
    pass


class _IntClaimsEnumValue(_ClaimsEnumWithValue, int):  # type: ignore[misc]
    pass


class _FloatClaimsEnumValue(_ClaimsEnumWithValue, float):  # type: ignore[misc]
    pass


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (_TupleClaimsEnumValue((_Secret(),)), ["<unserializable: _Secret>"]),
        (_DictClaimsEnumValue(a=_Secret()), {"a": "<unserializable: _Secret>"}),
        (_IntClaimsEnumValue(7), 7),
        (_FloatClaimsEnumValue(1.5), 1.5),
    ],
    ids=["tuple", "dict", "int", "float"],
)
@pytest.mark.parametrize("route", _ROUTES)
def test_a_real_builtin_claiming_enum_is_routed_by_its_real_type(
    route: str, value: object, expected: object
) -> None:
    """FR-002: the ``Enum`` branch's ``str()`` fallback never sees a real builtin container.

    The branch admits a real ``Enum`` or a non-builtin only; before that test a real ``tuple``
    claiming ``Enum`` was written ``(SECRET,)`` and a ``dict`` ``{'a': SECRET}``. A real ``int`` or
    ``float`` subclass so claiming is its exact number, from its own branch, never its ``str()``.
    """
    sink = _configure()
    event = _emit(route, {"v": value}, sink)
    assert event["fields"] == {"v": expected}
    fields = event["fields"]
    assert isinstance(fields, dict)
    assert type(fields["v"]) is type(expected)
    assert "SECRET" not in json.dumps(event)


@pytest.mark.parametrize("route", _ROUTES)
def test_a_real_tuple_claiming_enum_around_a_dag_returns(route: str) -> None:
    """FR-002: walked by its real type, a shared DAG inside it is cut, where ``str()`` hung."""
    sink = _configure()
    started = time.process_time()
    event = _emit(route, {"v": _TupleClaimsEnumValue((_DAG_40,))}, sink, deadline=5)
    cpu = time.process_time() - started
    assert cpu < 2, f"{route}: {cpu:.2f} s of CPU"
    assert event.get("truncated") is True, route


def test_a_real_bytes_claiming_enum_is_read_to_the_ceiling() -> None:
    """FR-002: 16 MiB of bytes claiming ``Enum`` is read to its ceiling, never rendered by repr."""
    value = _BytesClaimsEnumValue(b"\x00" * (16 << 20))
    tracemalloc.start()
    try:
        fields, flag = sanitize_fields({"v": value}, cfg=Config())
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert fields == {"v": "\x00" * 8178 + TRUNCATION_MARKER}
    assert flag is True
    assert peak < 8 << 20, f"peak {peak >> 20} MiB"
