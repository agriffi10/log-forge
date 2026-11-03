"""SPEC-056 FR-001 — ``_otel.log_record``: one event to one OTLP/JSON LogRecord, totally.

Every record this file builds is collected in ``CORPUS`` so ``test_otel_envelope.py`` can run it
through the generated-class round trip, which is FR-001's last criterion and closes there.
"""

from __future__ import annotations

import ast
import copy
import sys
import time
from collections.abc import Iterator, Mapping
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType
from typing import Any, NamedTuple

import pytest

from log_foundry import _otel, sanitize
from log_foundry._otel import MAX_NEST, MAX_NODES, log_record
from log_foundry.sinks._time import epoch_nanos


class _Point(NamedTuple):
    x: int
    y: int


STAMP = "2026-10-05T12:34:56.789Z"
SENTINEL = "s3cr3t-sentinel"

CORPUS: list[tuple[str, dict[str, object]]] = []
"""Every record a test below built, named, for ``test_otel_envelope.py``.

Filled as the tests run, so it is read by a test that first *calls* every test in this module in
its own process (``xdist`` collects per worker, where a list filled by other workers is empty), and
then round-trips everything that was built.
"""


def _record(name: str, event: object) -> dict[str, object]:
    """Maps an event and keeps the record for the round-trip corpus."""
    record = log_record(event)  # type: ignore[arg-type]
    CORPUS.append((name, record))
    return record


def _event(**over: object) -> dict[str, object]:
    """A span.end-shaped event with every library key, overridable per test."""
    event: dict[str, object] = {
        "timestamp": STAMP,
        "level": "ERROR",
        "message": "span.end",
        "trace_id": "0af7651916cd43dd8448eb211c80319c",
        "span_id": "b7ad6b7169203331",
        "parent_span_id": "00f067aa0ba902b7",
        "log_id": "log-1",
        "function": "pkg.work",
        "service": "svc",
        "version": "1",
        "env": "test",
        "truncated": True,
        "duration_ms": 12,
        "status": "error",
        "error": {"type": "Boom", "module": "mypkg.errors", "message": "bang", "stack": "tb"},
        "fields": {"k": 1},
    }
    event.update(over)
    return event


def _attrs(record: dict[str, object]) -> dict[str, Any]:
    """Attribute key to AnyValue, asserting the keys are unique on the way."""
    pairs = [(a["key"], a["value"]) for a in record.get("attributes", [])]  # type: ignore[union-attr]
    keys = [key for key, _ in pairs]
    assert len(keys) == len(set(keys)), f"repeated attribute key: {keys}"
    return dict(pairs)


def _keys(record: dict[str, object]) -> list[str]:
    return [a["key"] for a in record.get("attributes", [])]  # type: ignore[union-attr]


def _marked(record: dict[str, object]) -> bool:
    return _attrs(record).get("log_foundry.truncated") == {"boolValue": True}


def _values(value: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """Every AnyValue inside one, itself first — the node count a test asserts."""
    yield value
    for pair in value.get("kvlistValue", {}).get("values", []):
        yield from _values(pair["value"])
    for item in value.get("arrayValue", {}).get("values", []):
        yield from _values(item)


def _record_values(record: dict[str, object]) -> list[dict[str, Any]]:
    """Every value the record holds, body and attributes, the uncounted mark excluded."""
    found: list[dict[str, Any]] = []
    if "body" in record:
        found.extend(_values(record["body"]))  # type: ignore[arg-type]
    for key, value in _attrs(record).items():
        if key != "log_foundry.truncated":
            found.extend(_values(value))
    return found


def _strings(record: dict[str, object]) -> list[str]:
    return [v["stringValue"] for v in _record_values(record) if "stringValue" in v]


def _nested(depth: int, leaf: object = 1) -> object:
    value = leaf
    for _ in range(depth):
        value = [value]
    return value


def _innermost(value: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    """Descends an arrayValue chain, returning the container count and the leaf."""
    containers = 0
    while "arrayValue" in value:
        containers += 1
        value = value["arrayValue"]["values"][0]
    return containers, value


def _shared(levels: int) -> list[object]:
    y: list[object] = []
    for _ in range(levels):
        y = [y, y]
    return y


# --- structure ----------------------------------------------------------------------------


def test_the_module_is_a_leaf() -> None:
    """A leaf by design: ``_otel`` imports nothing from its own package, checked on its AST.

    A ``sys.modules`` probe cannot show this, because importing any submodule runs
    ``log_foundry/__init__.py`` and its sibling imports first.
    """
    tree = ast.parse(Path(_otel.__file__).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            assert not any(a.name.startswith("log_foundry") for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            assert not (node.module or "").startswith("log_foundry")
            assert node.level == 0, "a relative import is an intra-package import"


def test_the_node_budget_and_placeholder_equal_sanitizes() -> None:
    """AC-19. SPEC-057's assembly budget and this one cannot drift apart unseen."""
    assert _otel.MAX_NODES == sanitize._MAX_NODES
    record = _record("node-limit-text", {"fields": {"y": _shared(24)}})
    assert sanitize._NODE_LIMIT in _strings(record)
    assert _otel._NODE_LIMIT == sanitize._NODE_LIMIT


def test_nothing_in_otel_is_exported() -> None:
    assert not hasattr(_otel, "__all__")


# --- timestamp ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "stamp",
    [
        STAMP,
        "1970-01-01T00:00:00.001Z",
        "2000-02-29T23:59:59.999Z",
        "2026-01-01T00:00:00.000Z",
        "2038-01-19T03:14:08.000Z",
    ],
)
def test_a_library_timestamp_is_exact_and_agrees_with_epoch_nanos(stamp: str) -> None:
    """AC-1. Two exact conversions, held to agree; the spec's two values pinned by hand."""
    record = _record(f"ts-{stamp}", _event(timestamp=stamp))
    assert record["timeUnixNano"] == str(epoch_nanos(stamp))
    assert record["observedTimeUnixNano"] == record["timeUnixNano"]
    expected = {STAMP: "1791203696789000000", "1970-01-01T00:00:00.001Z": "1000000"}
    if stamp in expected:
        assert record["timeUnixNano"] == expected[stamp]


@pytest.mark.parametrize(
    "stamp",
    [
        None,
        5,
        "2026-10-05T12:34:56.789",
        "2026-10-05T12:34:56Z",
        "2026-10-05 12:34:56.789Z",
        "1970-01-01T00:00:00.000Z",
        "1969-12-31T23:59:59.999Z",
        "2600-01-01T00:00:00.000Z",
        "2026-02-30T00:00:00.000Z",
        "2026-13-01T00:00:00.000Z",
        "2026-10-05T24:00:00.000Z",
        "",
        "x",
    ],
)
def test_an_unusable_timestamp_omits_both_fields_and_reads_no_clock(
    stamp: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC-2. ``epoch_nanos`` reads the clock on the impossible date; this mapping never does."""

    def no_clock() -> int:
        raise AssertionError("the clock was read")

    monkeypatch.setattr(time, "time_ns", no_clock)
    monkeypatch.setattr(time, "time", no_clock)
    event = _event(timestamp=stamp) if stamp is not None else _event()
    if stamp is None:
        del event["timestamp"]
    record = _record(f"bad-ts-{stamp!r}", event)
    assert "timeUnixNano" not in record
    assert "observedTimeUnixNano" not in record
    if stamp == "2026-02-30T00:00:00.000Z":
        with pytest.raises(AssertionError, match="clock"):
            epoch_nanos(stamp)


# --- level, ids, event name ---------------------------------------------------------------


def test_the_five_levels_map_and_others_carry_text_only() -> None:
    """AC-3."""
    for level, number in (
        ("DEBUG", 5),
        ("INFO", 9),
        ("WARNING", 13),
        ("ERROR", 17),
        ("CRITICAL", 21),
    ):
        record = _record(f"level-{level}", _event(level=level))
        assert record["severityNumber"] == number
        assert record["severityText"] == level
    for level in ("TRACE", "info"):
        record = _record(f"level-{level}", _event(level=level))
        assert "severityNumber" not in record
        assert record["severityText"] == level
    for level in ("", 5, None, {"a": 1}):
        record = _record(f"level-{level!r}", _event(level=level))
        assert "severityNumber" not in record
        assert "severityText" not in record
    absent = _event()
    del absent["level"]
    record = _record("level-absent", absent)
    assert "severityNumber" not in record
    assert "severityText" not in record


def test_ids_pass_through_lowercased_with_the_sampled_flag() -> None:
    """AC-4."""
    record = _record("ids", _event())
    assert record["traceId"] == "0af7651916cd43dd8448eb211c80319c"
    assert record["spanId"] == "b7ad6b7169203331"
    assert record["flags"] == 1
    upper = _record(
        "ids-upper", _event(trace_id="0AF7651916CD43DD8448EB211C80319C", span_id="B7AD6B7169203331")
    )
    assert upper["traceId"] == "0af7651916cd43dd8448eb211c80319c"
    assert upper["spanId"] == "b7ad6b7169203331"
    for bad in ("zz" * 16, "0" * 32, "0af7", 5, None, "0af7651916cd43dd8448eb211c80319cd"):
        record = _record(f"bad-trace-{bad!r}", _event(trace_id=bad))
        assert "traceId" not in record and "flags" not in record
        assert record["spanId"] == "b7ad6b7169203331"
    for bad in ("zz" * 8, "0" * 16, "b7ad", 5, None):
        record = _record(f"bad-span-{bad!r}", _event(span_id=bad))
        assert "spanId" not in record
        assert record["traceId"] == "0af7651916cd43dd8448eb211c80319c" and record["flags"] == 1


def test_event_name_is_the_boundary_message_only() -> None:
    """AC-5. A caller logging those exact words gets the label too, as backfill does."""
    for message in ("span.start", "span.end"):
        record = _record(f"name-{message}", _event(message=message))
        assert record["eventName"] == message
        assert record["body"] == {"stringValue": message}
    for message in ("hello", "span.ends", 5, None):
        assert "eventName" not in _record(f"noname-{message!r}", _event(message=message))


# --- library attributes -------------------------------------------------------------------


def test_span_end_attributes_come_in_table_order() -> None:
    """AC-6."""
    record = _record("span-end-full", _event())
    assert _keys(record) == [
        "code.function.name",
        "log.record.uid",
        "log_foundry.parent_span_id",
        "log_foundry.truncated",
        "log_foundry.duration_ms",
        "log_foundry.status",
        "exception.type",
        "exception.message",
        "exception.stacktrace",
        "k",
    ]
    without = _event(parent_span_id=None)
    del without["truncated"]
    record = _record("span-end-no-parent", without)
    assert "log_foundry.parent_span_id" not in _keys(record)
    assert "log_foundry.truncated" not in _keys(record)


def test_duration_ms_is_an_int_or_a_double_or_absent() -> None:
    """AC-7."""
    assert _attrs(_record("dur-int", _event(duration_ms=12)))["log_foundry.duration_ms"] == {
        "intValue": "12"
    }
    assert _attrs(_record("dur-float", _event(duration_ms=1.5)))["log_foundry.duration_ms"] == {
        "doubleValue": 1.5
    }
    for bad in (True, "12", None):
        assert "log_foundry.duration_ms" not in _attrs(
            _record(f"dur-{bad!r}", _event(duration_ms=bad))
        )


def test_exception_type_is_module_qualified_except_builtins() -> None:
    """AC-8."""
    cases = [
        ({"type": "OSError", "module": "builtins"}, "OSError"),
        ({"type": "Boom", "module": "mypkg.errors"}, "mypkg.errors.Boom"),
        ({"type": "Boom", "module": ""}, "Boom"),
        ({"type": "Boom"}, "Boom"),
        ({"type": "Boom", "module": 5}, "Boom"),
    ]
    for error, expected in cases:
        record = _record(f"exc-{expected}-{error.get('module')!r}", _event(error=error))
        assert _attrs(record)["exception.type"] == {"stringValue": expected}
    for error in ("OSError", 5, None, ["Boom"]):
        record = _record(f"exc-notmap-{error!r}", _event(error=error))
        assert not any(key.startswith("exception.") for key in _keys(record))


def test_wrong_type_library_keys_are_omitted_and_message_takes_any_value() -> None:
    """AC-11."""
    record = _record(
        "wrong-types",
        _event(
            function=5,
            log_id={},
            parent_span_id=5,
            status=5,
            duration_ms="x",
            truncated="yes",
            error={"type": 5, "message": {"a": 1}},
        ),
    )
    assert _keys(record) == ["k"]
    assert _record("body-kvlist", _event(message={"a": 1}))["body"] == {
        "kvlistValue": {"values": [{"key": "a", "value": {"intValue": "1"}}]}
    }
    assert _record("body-int", _event(message=5))["body"] == {"intValue": "5"}
    absent = _event()
    del absent["message"]
    assert "body" not in _record("body-absent", absent)
    for value in _record_values(record):
        if "stringValue" in value:
            assert isinstance(value["stringValue"], str)
    assert isinstance(record.get("severityText", ""), str)


# --- values -------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, {}),
        (True, {"boolValue": True}),
        (False, {"boolValue": False}),
        (5, {"intValue": "5"}),
        (2**63 - 1, {"intValue": "9223372036854775807"}),
        (-(2**63), {"intValue": "-9223372036854775808"}),
        (2**63, {"stringValue": "9223372036854775808"}),
        (-(2**63) - 1, {"stringValue": "-9223372036854775809"}),
        (10**100, {"stringValue": str(10**100)}),
        (1.5, {"doubleValue": 1.5}),
        (float("nan"), {"doubleValue": "NaN"}),
        (float("inf"), {"doubleValue": "Infinity"}),
        (float("-inf"), {"doubleValue": "-Infinity"}),
        ("s", {"stringValue": "s"}),
        ({}, {"kvlistValue": {}}),
        ([], {"arrayValue": {}}),
        (
            (1, (2,)),
            {
                "arrayValue": {
                    "values": [{"intValue": "1"}, {"arrayValue": {"values": [{"intValue": "2"}]}}]
                }
            },
        ),
        (_Point(1, 2), {"arrayValue": {"values": [{"intValue": "1"}, {"intValue": "2"}]}}),
        (
            {"a": [1, {"b": None}]},
            {
                "kvlistValue": {
                    "values": [
                        {
                            "key": "a",
                            "value": {
                                "arrayValue": {
                                    "values": [
                                        {"intValue": "1"},
                                        {"kvlistValue": {"values": [{"key": "b", "value": {}}]}},
                                    ]
                                }
                            },
                        }
                    ]
                }
            },
        ),
    ],
    ids=repr,
)
def test_field_values_encode_as_any_values(value: object, expected: dict[str, object]) -> None:
    """AC-9."""
    record = _record(f"value-{value!r}"[:60], _event(fields={"v": value}, truncated=False))
    assert _attrs(record)["v"] == expected
    assert not _marked(record), "none of these marks"


def test_an_integer_too_long_to_render_is_a_marked_placeholder() -> None:
    """AC-10."""
    for value in (10**5000, -(10**5000)):
        record = _record(f"bigint-{value > 0}", _event(fields={"v": value}, truncated=False))
        assert _attrs(record)["v"] == {"stringValue": "<int: ~5001 digits>"}
        assert _marked(record)


def test_a_lone_surrogate_is_replaced_everywhere_and_marked() -> None:
    """AC-12."""
    bad = "a\ud800b"
    record = _record(
        "surrogates",
        _event(
            message=bad,
            fields={"v": bad, bad: 1},
            error={"type": "E", "message": bad},
            function=bad,
            truncated=False,
        ),
    )
    attrs = _attrs(record)
    assert record["body"] == {"stringValue": "a�b"}
    assert attrs["v"] == {"stringValue": "a�b"}
    assert attrs["a�b"] == {"intValue": "1"}
    assert attrs["exception.message"] == {"stringValue": "a�b"}
    assert attrs["code.function.name"] == {"stringValue": "a�b"}
    assert _marked(record)
    for level in ("ERROR\ud800", "x\udfff"):
        record = _record(f"level-surrogate-{level!r}", _event(level=level, truncated=False))
        assert record["severityText"] == level.replace("\ud800", "�").replace("\udfff", "�")
        assert _marked(record)


class Lying(int):
    def __str__(self) -> str:
        return SENTINEL

    def __repr__(self) -> str:
        return SENTINEL


class LyingStr(str):
    __slots__ = ()

    def __str__(self) -> str:
        return SENTINEL

    def __eq__(self, other: object) -> bool:
        return True

    def __hash__(self) -> int:
        raise RuntimeError("hash")


class LyingFloat(float):
    def __float__(self) -> float:
        return 99.0


class RaisingItems(dict):  # type: ignore[type-arg]
    def items(self) -> Any:
        raise RuntimeError("items")


class LyingTuple(tuple):  # type: ignore[type-arg]
    def __iter__(self) -> Any:
        return iter([SENTINEL])


class RaisingMapping(Mapping):  # type: ignore[type-arg]
    """A ``Mapping`` that is not a ``dict``, whose ``items`` raises."""

    def __getitem__(self, key: object) -> object:
        return 1

    def __iter__(self) -> Iterator[object]:
        return iter(["a"])

    def __len__(self) -> int:
        return 1

    def items(self) -> Any:
        raise RuntimeError("items")


def test_a_subclass_cannot_steer_its_own_mapping() -> None:
    """AC-13. Dispatch is on the builtin base through its unbound methods."""
    record = _record(
        "lying",
        _event(
            fields={
                "i": Lying(7),
                "s": LyingStr("plain"),
                "f": LyingFloat(1.5),
                "d": RaisingItems(a=1),
                "t": LyingTuple((1, 2)),
            },
            truncated=False,
        ),
    )
    attrs = _attrs(record)
    assert attrs["i"] == {"intValue": "7"}
    assert attrs["s"] == {"stringValue": "plain"}
    assert attrs["f"] == {"doubleValue": 1.5}
    assert attrs["d"] == {"kvlistValue": {"values": [{"key": "a", "value": {"intValue": "1"}}]}}
    assert attrs["t"] == {"arrayValue": {"values": [{"intValue": "1"}, {"intValue": "2"}]}}
    assert SENTINEL not in str(record) and "99.0" not in str(record)
    assert not _marked(record)
    proxy = _record("mappingproxy", _event(fields={"m": MappingProxyType({"a": 1})}))
    assert _attrs(proxy)["m"] == {"stringValue": "<unserializable: mappingproxy>"}
    raising = _record("raising-mapping", _event(fields={"m": RaisingMapping()}))
    assert _attrs(raising)["m"] == {"stringValue": "<unserializable: RaisingMapping>"}


class Claim:
    """Passes ``isinstance(_, str)`` and fails ``str.__str__``."""

    __class__ = property(lambda self: str)  # type: ignore[assignment]


class IntClaim:
    __class__ = property(lambda self: int)  # type: ignore[assignment]


class _RaisingName(type):
    @property
    def __name__(cls) -> str:  # type: ignore[override]
        raise RuntimeError("name")


class Unnamed(metaclass=_RaisingName):
    pass


def test_an_impostor_is_a_placeholder_named_for_what_it_is() -> None:
    """AC-14. ``isinstance`` is not the dispatch; the unbound read is."""
    assert isinstance(Claim(), str) and isinstance(IntClaim(), int)
    record = _record(
        "claims",
        _event(
            message=Claim(),
            fields={"c": Claim(), "i": IntClaim(), Claim(): 1, IntClaim(): 2},
            error={"type": "E", "message": Claim()},
            truncated=False,
        ),
    )
    attrs = _attrs(record)
    assert record["body"] == {"stringValue": "<unserializable: Claim>"}
    assert attrs["c"] == {"stringValue": "<unserializable: Claim>"}
    assert attrs["i"] == {"stringValue": "<unserializable: IntClaim>"}
    assert "exception.message" not in attrs
    assert attrs["<unserializable key: Claim>"] == {"intValue": "1"}
    assert attrs["<unserializable key: IntClaim>"] == {"intValue": "2"}
    assert _marked(record)
    record = _record("unnamed", _event(fields={"u": Unnamed(), Unnamed(): 1}, truncated=False))
    assert _attrs(record)["u"] == {"stringValue": "<unserializable: ?>"}
    assert _attrs(record)["<unserializable key: ?>"] == {"intValue": "1"}
    assert _marked(record)


# --- bounds -------------------------------------------------------------------------------


def test_nesting_renders_to_the_cap_and_cuts_the_next_level() -> None:
    """AC-15."""
    for depth in (23, 24):
        record = _record(f"deep-{depth}", _event(fields={"v": _nested(depth)}, truncated=False))
        containers, leaf = _innermost(_attrs(record)["v"])
        assert (containers, leaf) == (depth, {"intValue": "1"})
        assert not _marked(record)
    for depth in (25, 2000):
        record = _record(f"deep-{depth}", _event(fields={"v": _nested(depth)}, truncated=False))
        containers, leaf = _innermost(_attrs(record)["v"])
        assert (containers, leaf) == (MAX_NEST, {"stringValue": "<depth limit>"})
        assert _marked(record)


def test_a_cycle_is_found_by_path_and_is_not_a_mark() -> None:
    """AC-16."""
    x: list[object] = []
    x.append(x)
    x.append(x)
    started = time.perf_counter()
    record = _record("cycle", _event(fields={"x": x}, truncated=False))
    assert time.perf_counter() - started < 1.0
    assert _attrs(record)["x"] == {
        "arrayValue": {"values": [{"stringValue": "<circular>"}, {"stringValue": "<circular>"}]}
    }
    assert not _marked(record)


def test_a_shared_subtree_spends_the_node_budget_exactly_once() -> None:
    """AC-17."""
    started = time.perf_counter()
    record = _record("shared-24", _event(fields={"y": _shared(24)}, truncated=False))
    assert time.perf_counter() - started < 10.0
    values = _record_values(record)
    assert len(values) == MAX_NODES + 1
    assert _strings(record).count("<node limit>") == 1
    assert _marked(record)
    within = _record("shared-20", _event(fields={"y": _shared(20)}, truncated=False))
    assert "<depth limit>" not in _strings(within), "21 containers deep: the depth cap never fires"
    assert _strings(within).count("<node limit>") == 1
    assert len(_record_values(within)) == MAX_NODES + 1
    assert _marked(within), "the node cut alone marks the record"
    wide = {"m": [list(range(256)) for _ in range(256)]}
    record = _record("wide-256", _event(fields=wide, truncated=False))
    assert len(list(_values(_attrs(record)["m"]))) == 1 + 256 + 256 * 256
    assert "<node limit>" not in _strings(record)
    assert not _marked(record)


def test_the_walk_charges_library_attributes_before_the_body() -> None:
    """AC-18. A hostile message cannot remove a library attribute, and the mark is uncounted."""
    started = time.perf_counter()
    record = _record("walk-order", _event(message=_shared(24), fields={"k": 1}, truncated=False))
    assert time.perf_counter() - started < 10.0
    assert "<node limit>" in [v.get("stringValue") for v in _values(record["body"])]  # type: ignore[arg-type]
    attrs = _attrs(record)
    assert attrs["code.function.name"] == {"stringValue": "pkg.work"}
    assert attrs["log.record.uid"] == {"stringValue": "log-1"}
    assert attrs["log_foundry.status"] == {"stringValue": "error"}
    assert attrs["exception.type"] == {"stringValue": "mypkg.errors.Boom"}
    assert attrs["exception.message"] == {"stringValue": "bang"}
    assert attrs["exception.stacktrace"] == {"stringValue": "tb"}
    assert "k" not in attrs
    assert "droppedAttributesCount" not in record
    assert _marked(record)
    assert len(_record_values(record)) == MAX_NODES + 1


# --- keys ---------------------------------------------------------------------------------


class RaisingKey:
    def __str__(self) -> str:
        raise RuntimeError("str")


def test_keys_render_as_sanitize_renders_them() -> None:
    """AC-20, the direct half; the transform half is in ``test_sinks_otlp.py``."""
    record = _record("int-keys", _event(fields={1: "a", 2: "b"}, truncated=False))
    assert _attrs(record)["1"] == {"stringValue": "a"} and _attrs(record)["2"] == {
        "stringValue": "b"
    }
    assert "droppedAttributesCount" not in record and not _marked(record)
    record = _record("scalar-keys", _event(fields={True: 1, None: 2, 1.5: 3}, truncated=False))
    assert set(_attrs(record)) >= {"True", "None", "1.5"} and not _marked(record)
    record = _record("nan-key", _event(fields={float("nan"): 1}, truncated=False))
    assert _attrs(record)["<float: nan>"] == {"intValue": "1"} and _marked(record)
    record = _record("bigint-key", _event(fields={10**5000: 1}, truncated=False))
    assert _attrs(record)["<int: ~5001 digits>"] == {"intValue": "1"} and _marked(record)
    record = _record("lying-int-key", _event(fields={Lying(7): 1}, truncated=False))
    assert _attrs(record)["7"] == {"intValue": "1"} and SENTINEL not in str(record)
    for key, name in (
        ((1, 2), "tuple"),
        (Decimal("1.5"), "Decimal"),
        (RaisingKey(), "RaisingKey"),
        (Claim(), "Claim"),
        (Unnamed(), "?"),
    ):
        record = _record(f"bad-key-{name}", _event(fields={key: 1}, truncated=False))
        assert _attrs(record)[f"<unserializable key: {name}>"] == {"intValue": "1"}, name
        assert _marked(record), name


def test_repeated_empty_and_unrenderable_keys_are_dropped_and_counted() -> None:
    """AC-20, the tail: the first stands, the drop is counted, nested levels too."""
    record = _record("dup-1", _event(fields={1: "a", "1": "b"}, truncated=False))
    assert _attrs(record)["1"] == {"stringValue": "a"}
    assert record["droppedAttributesCount"] == 1 and not _marked(record)
    record = _record(
        "dup-raising",
        _event(fields={RaisingKey(): "first", RaisingKey(): "second"}, truncated=False),
    )
    assert _attrs(record)["<unserializable key: RaisingKey>"] == {"stringValue": "first"}
    assert record["droppedAttributesCount"] == 1 and _marked(record)
    record = _record("empty-key", _event(fields={"": 1}, truncated=False))
    assert "k" not in _attrs(record) and "" not in _attrs(record)
    assert record["droppedAttributesCount"] == 1 and not _marked(record)
    record = _record(
        "nested-keys",
        _event(
            fields={"a": {"": 1, "b": 2, 1: "x", "1": "y", RaisingKey(): 1, RaisingKey(): 2}},
            truncated=False,
        ),
    )
    inner = _attrs(record)["a"]["kvlistValue"]["values"]
    assert [pair["key"] for pair in inner] == ["b", "1", "<unserializable key: RaisingKey>"]
    assert record["droppedAttributesCount"] == 3 and _marked(record)


def test_collisions_and_the_reserved_prefix_are_dropped_and_counted() -> None:
    """AC-21."""
    hostile = {"log_foundry.status": "ok", "code.function.name": "evil", "exception.type": "X"}
    record = _record("collide-span-end", _event(fields=hostile, truncated=False))
    attrs = _attrs(record)
    assert attrs["log_foundry.status"] == {"stringValue": "error"}
    assert attrs["code.function.name"] == {"stringValue": "pkg.work"}
    assert attrs["exception.type"] == {"stringValue": "mypkg.errors.Boom"}
    assert record["droppedAttributesCount"] == 3 and not _marked(record)
    info = _event(level="INFO", message="m", fields=hostile, truncated=False)
    for name in ("duration_ms", "status", "error", "parent_span_id"):
        del info[name]
    record = _record("collide-info", info)
    attrs = _attrs(record)
    assert attrs["code.function.name"] == {"stringValue": "pkg.work"}
    assert "log_foundry.status" not in attrs
    assert attrs["exception.type"] == {"stringValue": "X"}
    assert record["droppedAttributesCount"] == 2
    for event in (
        _event(fields={"log_foundry.custom": 1}, truncated=False),
        dict(info, fields={"log_foundry.custom": 1}),
    ):
        record = _record("reserved-custom", event)
        assert "log_foundry.custom" not in _attrs(record)
        assert record["droppedAttributesCount"] == 1
    assert "droppedAttributesCount" not in _record("no-collision", _event(truncated=False))


def test_the_mark_is_exactly_the_listed_set() -> None:
    """AC-22, AC-25. One record per case, both halves."""
    x: list[object] = []
    x.append(x)
    unmarked: dict[str, dict[str, object]] = {
        "circular": {"fields": {"x": x}},
        "unserializable-value": {"fields": {"o": object()}},
        "int-beyond-int64": {"fields": {"i": 2**63}},
        "nan-value": {"fields": {"f": float("nan")}},
        "collision": {"function": "f", "fields": {"code.function.name": 1}},
        "reserved": {"fields": {"log_foundry.x": 1}},
        "empty-key": {"fields": {"": 1}},
        "int-key": {"fields": {1: 1}},
    }
    for name, event in unmarked.items():
        assert not _marked(_record(f"unmarked-{name}", event)), name
    marked: dict[str, dict[str, object]] = {
        "depth": {"fields": {"v": _nested(25)}},
        "node": {"fields": {"y": _shared(20)}},
        "bigint": {"fields": {"i": 10**5000}},
        "surrogate": {"message": "a\ud800b"},
        "bad-key": {"fields": {(1, 2): 1}},
        "assembly": {"truncated": True},
    }
    for name, event in marked.items():
        assert _marked(_record(f"marked-{name}", event)), name


# --- purity -------------------------------------------------------------------------------


def test_five_events_map_independently_and_none_is_mutated() -> None:
    """AC-24. A mutation would change what a ``MultiSink`` sibling delivers (invariant 2)."""
    events: list[dict[str, object]] = [
        _event(),
        _event(level="INFO", message="m", fields={"a": [1, 2], "b": {"c": None}}),
        _event(fields={10**5000: 1, "": 2, Lying(3): 4, "log_foundry.x": 5, "k": "plain"}),
        _event(message={"deep": _nested(30)}),
        _event(trace_id="0" * 32, fields={}),
    ]
    before = copy.deepcopy(events)
    solo = [log_record(copy.deepcopy(event)) for event in events]
    batch = [log_record(event) for event in events]
    assert len(batch) == 5
    assert batch == solo
    assert events == before
    for name, record in zip(("e0", "e1", "e2-hostile", "e3", "e4"), batch, strict=True):
        CORPUS.append((name, record))


def test_an_assembly_mark_survives() -> None:
    """AC-25."""
    assert _marked(_record("assembly-mark", _event(truncated=True, fields={})))
    assert not _marked(_record("assembly-unmarked", _event(truncated=False, fields={})))


def test_a_non_dict_event_is_an_empty_record() -> None:
    """Rule 1's last sentence: still one record per event."""
    for event in ("x", 5, None, [1], MappingProxyType({"message": "m"})):
        assert log_record(event) == {}  # type: ignore[arg-type]


def test_the_placeholder_text_assumes_the_default_digit_limit() -> None:
    """AC-10 names ``<int: ~5001 digits>``, which is the interpreter default of 4300 at work."""
    assert sys.get_int_max_str_digits() == 4300


class DictClaim:
    """Passes ``isinstance(_, dict)`` and fails every unbound ``dict`` read."""

    __class__ = property(lambda self: dict)  # type: ignore[assignment]


@pytest.mark.parametrize("slot", ["fields", "error"])
def test_a_dict_impostor_in_a_slot_costs_only_that_slot(slot: str) -> None:
    """Rules 1 and 2 at the slot: a ``fields`` or ``error`` claiming ``dict`` is omitted alone.

    Without a guard on the slot's own unbound read, the raise reached ``log_record``'s outer
    guard and the whole record collapsed to ``{}`` (pre-push review, MINOR-1).
    """
    assert isinstance(DictClaim(), dict)
    record = _record(f"dict-claim-{slot}", _event(**{slot: DictClaim()}, truncated=False))
    assert record["timeUnixNano"] == "1791203696789000000"
    assert record["body"] == {"stringValue": "span.end"}
    assert record["severityText"] == "ERROR" and record["severityNumber"] == 17
    attrs = _attrs(record)
    assert attrs["code.function.name"] == {"stringValue": "pkg.work"}
    assert attrs["log.record.uid"] == {"stringValue": "log-1"}
    if slot == "fields":
        assert "k" not in attrs
        assert attrs["exception.type"] == {"stringValue": "mypkg.errors.Boom"}
    else:
        assert not any(key.startswith("exception.") for key in attrs)
        assert attrs["k"] == {"intValue": "1"}
    assert "droppedAttributesCount" not in record and not _marked(record)
