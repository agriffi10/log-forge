"""SPEC-096 — SQSSink and SNSSink take their message limit as an argument, never past 1 MiB.

Every sink here is built with an injected client, so the module runs the same with or without
the ``aws`` extra installed (``integration.yml`` reruns the unit suite with every extra). Every
expected number is written out, never read from the sink under test: the limits are the AWS
ranges the spec quotes, and the 1.x requests are the recording in
``tests/data/spec096_requests_10ba21ee.json``, made against a ``git archive`` of ``10ba21ee``.
"""

from __future__ import annotations

import decimal
import inspect
import json
import pathlib
import re
from typing import Any

import pytest

import message_limit_cases as cases
from log_foundry.sinks.sns import SNSSink
from log_foundry.sinks.sqs import SQSSink
from message_limit_cases import Recorder, event
from test_numeric_door_roster import _PKG, scan, scan_tree, without_helper

KIB = 1024
MIB = 1024 * 1024
FIXTURE = pathlib.Path(__file__).parent / "data" / "spec096_requests_10ba21ee.json"
NAMED = re.compile(r"the (\d+)-byte message limit")

_CLASSES = [SQSSink, SNSSink]
_IDS = ["SQSSink", "SNSSink"]


def _build(cls: type, client: Any = None, **kw: Any) -> Any:
    """Builds either sink against a recording client, never boto3."""
    target = "https://q" if cls is SQSSink or issubclass(cls, SQSSink) else "arn:t"
    return cls(target, client=Recorder() if client is None else client, **kw)


def _sized(i: int, body_bytes: int) -> dict[str, object]:
    """An ``event(i, pad)`` whose JSON body is exactly ``body_bytes`` long (``i`` below 10)."""
    bare = len(json.dumps(event(i, 0)).encode("utf-8"))
    built = event(i, body_bytes - bare)
    assert len(json.dumps(built).encode("utf-8")) == body_bytes, "the sizing premise"
    return built


def _body(entry: dict[str, str]) -> str:
    return entry["MessageBody"] if "MessageBody" in entry else entry["Message"]


def _ns(sink: Any) -> list[list[int]]:
    """Each request's events, by the ``n`` each body carries."""
    return [
        [json.loads(_body(entry))["n"] for entry in request] for request in sink.client.requests
    ]


def _sums(sink: Any) -> list[int]:
    return [sum(cases.costed(entry) for entry in request) for request in sink.client.requests]


def _named(lines: list[str]) -> list[int]:
    return [int(match.group(1)) for line in lines if (match := NAMED.search(line))]


class Ceiling(Recorder):
    """A recorder that refuses, as SQS and SNS do, any request summing past 1 MiB."""

    def _check(self, entries: list[dict[str, str]]) -> None:
        if sum(cases.costed(entry) for entry in entries) > MIB:
            raise RuntimeError("BatchRequestTooLong: past the service's 1 MiB")

    def send_message_batch(self, QueueUrl: str, Entries: list[dict[str, str]]) -> dict[str, Any]:
        self._check(Entries)
        return super().send_message_batch(QueueUrl, Entries)

    def publish_batch(
        self, TopicArn: str, PublishBatchRequestEntries: list[dict[str, str]]
    ) -> dict[str, Any]:
        self._check(PublishBatchRequestEntries)
        return super().publish_batch(TopicArn, PublishBatchRequestEntries)


class OutOfRange(int):
    """An int subclass in range whose comparisons all say it is out of range."""

    def __lt__(self, other: object) -> bool:
        return True

    def __gt__(self, other: object) -> bool:
        return True

    def __le__(self, other: object) -> bool:
        return False

    def __ge__(self, other: object) -> bool:
        return False


class InRange(int):
    """An int subclass whose comparisons say 1,024 to 1,048,576 and whose text says 262144."""

    def __lt__(self, other: object) -> bool:
        return False

    def __gt__(self, other: object) -> bool:
        return False

    def __le__(self, other: object) -> bool:
        return True

    def __ge__(self, other: object) -> bool:
        return True

    def __repr__(self) -> str:
        return "262144"

    def __str__(self) -> str:
        return "262144"

    def __format__(self, spec: str) -> str:
        return "262144"


# --- FR-001: the argument ---------------------------------------------------------------------

_EMPTY = inspect.Parameter.empty
_POK = inspect.Parameter.POSITIONAL_OR_KEYWORD
_KW = inspect.Parameter.KEYWORD_ONLY
_ONE_X = {
    SQSSink: [
        ("self", _POK, _EMPTY),
        ("queue_url", _POK, _EMPTY),
        ("client", _KW, None),
        ("max_retries", _KW, 3),
        ("fifo", _KW, None),
        ("message_group_id", _KW, None),
        ("message_deduplication_id", _KW, None),
    ],
    SNSSink: [
        ("self", _POK, _EMPTY),
        ("topic_arn", _POK, _EMPTY),
        ("client", _KW, None),
        ("max_retries", _KW, 3),
    ],
}
"""Each constructor's parameters in 1.2.0, written out: name, kind and default."""


@pytest.mark.parametrize("cls", _CLASSES, ids=_IDS)
def test_each_signature_ends_with_max_message_bytes_and_keeps_the_1x_parameters(cls: type) -> None:
    """FR-001 AC1 / invariant 10: appended last, keyword-only, default None; 1.x unchanged."""
    params = [
        (p.name, p.kind, p.default) for p in inspect.signature(cls.__init__).parameters.values()
    ]
    assert params[:-1] == _ONE_X[cls]
    assert params[-1] == ("max_message_bytes", _KW, None)


@pytest.mark.parametrize("value", [1024, 1048576, 262144, None])
@pytest.mark.parametrize("cls", _CLASSES, ids=_IDS)
def test_a_limit_in_range_is_stored(cls: type, value: int | None) -> None:
    """FR-001 AC2: both ends of AWS's range, the default's value, and None construct."""
    sink = _build(cls, max_message_bytes=value)
    assert sink.max_message_bytes == value
    assert type(sink.max_message_bytes) is type(value)


@pytest.mark.parametrize("cls", _CLASSES, ids=_IDS)
def test_an_int_subclass_in_range_is_stored_as_the_exact_int(cls: type) -> None:
    """FR-001 AC2: a 2048 whose comparisons say out of range is judged by the 2048 it holds."""
    sink = _build(cls, max_message_bytes=OutOfRange(2048))
    assert sink.max_message_bytes == 2048
    assert type(sink.max_message_bytes) is int


@pytest.mark.parametrize("held", [512, 1048577])
@pytest.mark.parametrize("cls", _CLASSES, ids=_IDS)
def test_a_lying_subclass_out_of_range_is_refused_naming_its_own_value(
    cls: type, held: int
) -> None:
    """FR-001 AC3: the range check runs on the exact copy, and the message renders that copy.

    A build comparing the caller's object constructs here, and one rendering it through
    ``repr`` or ``format`` names 262144.
    """
    with pytest.raises(ValueError) as info:
        _build(cls, max_message_bytes=InRange(held))
    assert type(info.value) is ValueError
    assert str(info.value) == (
        f"{cls.__name__} max_message_bytes must be from 1024 to 1048576 bytes, got {held}"
    )
    assert "262144" not in str(info.value)


@pytest.mark.parametrize(
    ("value", "named"), [(1023, "1023"), (1048577, "1048577"), (0, "0"), (-1, "-1"), (True, "1")]
)
@pytest.mark.parametrize("cls", _CLASSES, ids=_IDS)
def test_an_out_of_range_int_is_refused_naming_class_argument_value_and_bounds(
    cls: type, value: int, named: str
) -> None:
    """FR-001 AC4: refused, never floored, since a new argument has no configuration to keep."""
    with pytest.raises(ValueError) as info:
        _build(cls, max_message_bytes=value)
    assert type(info.value) is ValueError
    assert str(info.value) == (
        f"{cls.__name__} max_message_bytes must be from 1024 to 1048576 bytes, got {named}"
    )


@pytest.mark.parametrize(
    ("value", "type_name"),
    [
        (1.5e6, "float"),
        (262144.0, "float"),
        ("262144", "str"),
        (decimal.Decimal(262144), "Decimal"),
    ],
    ids=["1.5e6", "262144.0", "str", "Decimal"],
)
@pytest.mark.parametrize("cls", _CLASSES, ids=_IDS)
def test_a_non_int_limit_is_refused_with_a_type_error(
    cls: type, value: object, type_name: str
) -> None:
    """FR-001 AC4: exact_int's refusal, naming the class, the argument and the type."""
    with pytest.raises(TypeError) as info:
        _build(cls, max_message_bytes=value)
    assert type(info.value) is TypeError
    assert (
        str(info.value) == f"{cls.__name__} max_message_bytes must be an integer, not {type_name}"
    )


_EARLIER = [
    (SQSSink, {"max_retries": 2.5}),
    (SNSSink, {"max_retries": 2.5}),
    (SQSSink, {"message_group_id": 5}),
    (SQSSink, {"message_group_id": "  "}),
]


@pytest.mark.parametrize(
    ("cls", "earlier"),
    _EARLIER,
    ids=["SQSSink-max_retries", "SNSSink-max_retries", "SQSSink-group-type", "SQSSink-group-blank"],
)
def test_an_earlier_refusal_keeps_its_exact_error(cls: type, earlier: dict[str, Any]) -> None:
    """FR-001 AC6: the limit is judged after every refusal a 1.x call could already meet."""
    with pytest.raises(Exception) as without:
        _build(cls, **earlier)
    with pytest.raises(Exception) as beside:
        _build(cls, max_message_bytes=1.5, **earlier)
    assert type(beside.value) is type(without.value)
    assert str(beside.value) == str(without.value)
    assert "max_message_bytes" not in str(beside.value)


@pytest.mark.parametrize("cls", _CLASSES, ids=_IDS)
def test_a_limit_beside_an_injected_client_constructs(cls: type) -> None:
    """FR-001 AC7: the limit is not a client option, so SPEC-071's injected-client rule skips it."""
    client = Recorder()
    sink = cls("https://q" if cls is SQSSink else "arn:t", client=client, max_message_bytes=262144)
    assert sink.client is client
    assert sink.max_message_bytes == 262144


def test_max_message_bytes_is_judged_and_swept_on_both_sinks() -> None:
    """FR-001 AC8: the roster sees both judgements, and removing either is reported by site."""
    sites = {"sinks/sqs.py:SQSSink.max_message_bytes", "sinks/sns.py:SNSSink.max_message_bytes"}
    tree = scan_tree(_PKG)
    assert sites <= tree.judged
    calls = [call for call in tree.helper_calls if call.site in sites]
    assert sorted((call.site, call.kind) for call in calls) == sorted(
        (site, "exact_int") for site in sites
    )
    for call in calls:
        source = (_PKG / call.file).read_text(encoding="utf-8")
        mutated = scan(without_helper(source, call), call.file)
        assert f"{call.site}: compare" in mutated.findings, mutated.findings


# --- FR-004: MAX_BYTES keeps its value and its reach ------------------------------------------


@pytest.mark.parametrize("cls", _CLASSES, ids=_IDS)
def test_max_bytes_keeps_its_value_on_the_class(cls: type) -> None:
    """FR-004 AC1: a class attribute holding 262,144, never copied onto the instance."""
    sink = _build(cls)
    assert cls.MAX_BYTES == 262144
    assert "MAX_BYTES" in vars(cls)
    assert "MAX_BYTES" not in vars(sink)
    assert type(sink).MAX_BYTES == 262144


def _small_limit(cls: type, how: str) -> Any:
    if how == "instance":
        sink = _build(cls)
        sink.MAX_BYTES = 2048
        return sink
    small = type(f"Small{cls.__name__}", (cls,), {"MAX_BYTES": 2048})
    return _build(small)


@pytest.mark.parametrize("how", ["instance", "subclass"])
@pytest.mark.parametrize("cls", _CLASSES, ids=_IDS)
def test_max_bytes_still_sets_the_limit_when_max_message_bytes_is_none(cls: type, how: str) -> None:
    """FR-004 AC3: cases D1, D2, F3 and F4 — an instance assignment and a subclass both bind."""
    sink = _small_limit(cls, how)
    result = cases.run(sink, [event(0, 10), event(1, 3 * KIB)])
    assert _ns(sink) == [[0]]
    assert sink.dropped_oversized == 1
    assert _named(result["lines"]) == [2048], result["lines"]


@pytest.mark.parametrize("cls", _CLASSES, ids=_IDS)
def test_max_message_bytes_wins_over_max_bytes(cls: type) -> None:
    """FR-004 AC4: a passed limit beats MAX_BYTES, in the round and in the line written later."""
    sink = _build(cls, max_message_bytes=65536)
    sink.MAX_BYTES = 2048
    result = cases.run(sink, [event(0, 3 * KIB), event(1, 100 * KIB)])
    assert _ns(sink) == [[0]], "the 3 KiB event lands under 65536 though MAX_BYTES says 2048"
    assert sink.dropped_oversized == 1
    assert _named(result["lines"]) == [65536], result["lines"]


# --- FR-002 / FR-003 AC1: the default sends what 1.x sent -------------------------------------


def _recording() -> dict[str, Any]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def test_the_recording_matches_the_specs_evidence_table() -> None:
    """The fixture is what the spec measured at 10ba21ee, so it is not a recording of anything."""
    rec = _recording()
    assert sorted(rec) == ["A", "B", "C", "E", "F1", "F2"]

    def shape(name: str) -> tuple[list[int], list[int], int, int, str | None]:
        requests = rec[name]["requests"]
        return (
            [len(request) for request in requests],
            [sum(entry["cost"] for entry in request) for request in requests],
            rec[name]["dropped_oversized"],
            rec[name]["failed"],
            rec[name]["raised"],
        )

    assert shape("A") == ([2] * 5, [204972] * 5, 0, 0, None)
    assert shape("F1") == ([2] * 5, [204972] * 5, 0, 0, None)
    assert shape("B") == ([8, 2], [246448, 61612], 0, 0, None)
    assert shape("C") == ([2], [192], 1, 0, None)
    assert shape("F2") == ([2], [192], 1, 0, None)
    assert shape("E") == ([], [], 1, 0, None)
    assert rec["C"]["lines"] == [
        "log-foundry: lost 1 event(s); SQSSink, 307286 bytes exceeds the 262144-byte message limit"
    ]
    assert rec["F2"]["lines"] == [
        "log-foundry: lost 1 event(s); SNSSink, exceeds the 262144-byte message limit"
    ]
    assert rec["E"]["lines"] == [
        "log-foundry: lost 1 event(s); SQSSink, 262171 bytes exceeds the 262144-byte message limit"
    ]
    keys = {name: {tuple(e["keys"]) for r in rec[name]["requests"] for e in r} for name in rec}
    assert keys["A"] == keys["B"] == keys["C"] == {("MessageBody",)}
    assert keys["F1"] == keys["F2"] == {("Message",)}


def test_the_recording_comparison_sees_one_byte_and_one_boundary() -> None:
    """The comparison is of requests, not counts: one byte or one moved entry fails it."""
    factory, batch = cases.cases()["B"]
    sink = factory()
    cases.run(sink, batch)
    raw = sink.client.requests
    expected = _recording()["B"]["requests"]
    assert cases.request_records(raw) == expected, "the premise: a matching run passes"
    flipped = [[dict(entry) for entry in request] for request in raw]
    body = flipped[0][3]["MessageBody"]
    flipped[0][3]["MessageBody"] = body[:-3] + ("y" if body[-3] != "y" else "z") + body[-2:]
    assert cases.request_records(flipped) != expected
    swapped = [list(request) for request in raw]
    swapped[0][-1], swapped[1][0] = swapped[1][0], swapped[0][-1]
    assert [len(r) for r in swapped] == [len(r) for r in expected], "counts alone still agree"
    assert cases.request_records(swapped) != expected


@pytest.mark.parametrize("name", ["A", "B", "C", "E"])
def test_sqs_default_requests_match_the_10ba21ee_recording(name: str) -> None:
    """FR-002 AC1 / invariants 2 and 7: entry for entry, byte for byte but the Id, and the lines."""
    factory, batch = cases.cases()[name]
    assert cases.run(factory(), batch) == _recording()[name]


@pytest.mark.parametrize("name", ["F1", "F2"])
def test_sns_default_requests_match_the_10ba21ee_recording(name: str) -> None:
    """FR-003 AC1 / invariants 2 and 6: SNS's twin of the recording."""
    factory, batch = cases.cases()[name]
    assert cases.run(factory(), batch) == _recording()[name]


# --- FR-002: SQSSink applies the limit --------------------------------------------------------


def test_sqs_at_one_mib_sends_the_largest_message_and_drops_one_byte_more() -> None:
    """FR-002 AC2: 1,048,576 bytes is sent alone and lands; one byte more is dropped."""
    sink = _build(SQSSink, client=Ceiling(), max_message_bytes=MIB)
    result = cases.run(sink, [_sized(0, MIB), _sized(1, MIB + 1)])
    assert _ns(sink) == [[0]]
    assert len(sink.client.requests[0][0]["MessageBody"].encode("utf-8")) == MIB
    assert sink.dropped_oversized == 1
    assert sink.losses().failed == 0
    assert result["lines"] == [
        "log-foundry: lost 1 event(s); SQSSink, 1048577 bytes exceeds the 1048576-byte message limit"
    ]


def test_sqs_fifo_at_one_mib_costs_the_ids() -> None:
    """FR-002 AC3: the ids count toward the limit, so a body under 1 MiB can still be dropped."""
    ids = len("t" * 32) + len("l0000")
    sink = SQSSink("https://q.fifo", client=Ceiling(), max_message_bytes=MIB)
    fits, over = _sized(0, MIB - ids), _sized(1, MIB + 1 - ids)
    assert len(json.dumps(over).encode("utf-8")) < MIB, "the premise: its body alone fits"
    result = cases.run(sink, [fits, over])
    assert _ns(sink) == [[0]]
    sent = sink.client.requests[0][0]
    assert sent["MessageGroupId"] == "t" * 32
    assert sent["MessageDeduplicationId"] == "l0000"
    assert cases.costed(sent) == MIB
    assert sink.dropped_oversized == 1
    assert result["lines"] == [
        "log-foundry: lost 1 event(s); SQSSink, 1048577 bytes exceeds the 1048576-byte message limit"
    ]


def test_sqs_at_64_kib_drops_the_100_kib_event_and_sends_the_rest_together() -> None:
    """FR-002 AC4: a queue set below 256 KiB gets nothing it would refuse."""
    sink = _build(SQSSink, max_message_bytes=65536)
    result = cases.run(sink, [event(0, 100), event(1, 100 * KIB), event(2, 100)])
    assert _ns(sink) == [[0, 2]]
    assert sink.dropped_oversized == 1
    assert _named(result["lines"]) == [65536]


def test_sqs_at_one_mib_packs_three_300_kib_events_per_request() -> None:
    """FR-002 AC5: the request's sum is held to the limit, the service's own batch ceiling."""
    sink = _build(SQSSink, client=Ceiling(), max_message_bytes=MIB)
    result = cases.run(sink, [event(i, 300 * KIB) for i in range(10)])
    assert _ns(sink) == [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
    assert all(total <= MIB for total in _sums(sink)), _sums(sink)
    assert sink.losses().failed == 0
    assert result["raised"] is None


@pytest.mark.parametrize("how", ["instance", "subclass"])
@pytest.mark.parametrize("cls", _CLASSES, ids=_IDS)
def test_a_max_bytes_above_one_mib_is_capped(cls: type, how: str) -> None:
    """FR-002 AC6 and FR-003 AC5: a MAX_BYTES of 2 MiB is held to the service's 1 MiB."""
    if how == "instance":
        sink = _build(cls, client=Ceiling())
        sink.MAX_BYTES = 2 * MIB
    else:
        big = type(f"Big{cls.__name__}", (cls,), {"MAX_BYTES": 2 * MIB})
        sink = _build(big, client=Ceiling())
    batch = [event(0, MIB + MIB // 2), *(event(i, 300 * KIB) for i in range(1, 7))]
    result = cases.run(sink, batch)
    assert sink.dropped_oversized == 1
    assert _named(result["lines"]) == [MIB], result["lines"]
    assert _ns(sink) == [[1, 2, 3], [4, 5, 6]]
    assert all(total <= MIB for total in _sums(sink)), _sums(sink)
    assert sink.losses().failed == 0
    assert result["raised"] is None


@pytest.mark.parametrize("cls", _CLASSES, ids=_IDS)
def test_an_all_oversized_batch_makes_no_request(cls: type) -> None:
    """FR-002 AC7: every event dropped, counted and announced once, and emit returns."""
    sink = _build(cls, max_message_bytes=1024)
    result = cases.run(sink, [event(i, 2 * KIB) for i in range(3)])
    assert sink.client.requests == []
    assert result["raised"] is None
    assert sink.dropped_oversized == 3
    assert _named(result["lines"]) == [1024, 1024, 1024]
    assert len(result["lines"]) == 3


# --- FR-003: SNSSink applies the limit, in UTF-8 bytes ----------------------------------------


class Utf8SNS(SNSSink):
    """An SNSSink whose bodies carry UTF-8 rather than ``\\u`` escapes."""

    def _encode(self, event: dict[str, object]) -> str:
        return json.dumps(event, ensure_ascii=False)


def test_sns_costs_a_request_in_utf8_bytes() -> None:
    """FR-003 AC2: characters undercount UTF-8, so counting them packed requests past the limit."""
    sink = Utf8SNS("arn:t", client=Recorder(), max_message_bytes=1024)
    wide = {"n": 0, "pad": "é" * 600}
    assert len(json.dumps(wide, ensure_ascii=False)) < 1024
    assert len(json.dumps(wide, ensure_ascii=False).encode("utf-8")) > 1024
    pair = [{"n": 1, "pad": "é" * 400}, {"n": 2, "pad": "é" * 400}]
    for item in pair:
        body = json.dumps(item, ensure_ascii=False)
        assert len(body) < 512
        assert 700 <= len(body.encode("utf-8")) <= 1000
    result = cases.run(sink, [wide, *pair])
    assert sink.dropped_oversized == 1
    assert _named(result["lines"]) == [1024]
    assert _ns(sink) == [[1], [2]], "two requests: together they are over 1,024 bytes"
    assert all(total <= 1024 for total in _sums(sink)), _sums(sink)


def test_sns_at_one_mib_publishes_600_kib_and_drops_one_byte_past() -> None:
    """FR-003 AC3: T3 and T4 — 600 KiB lands at 1 MiB and is dropped at the default."""
    at_default = _build(SNSSink)
    cases.run(at_default, [event(0, 600 * KIB)])
    assert at_default.client.requests == []
    assert at_default.dropped_oversized == 1
    sink = _build(SNSSink, client=Ceiling(), max_message_bytes=MIB)
    result = cases.run(sink, [event(0, 600 * KIB), _sized(1, MIB + 1)])
    assert _ns(sink) == [[0]]
    assert sink.losses().failed == 0
    assert sink.dropped_oversized == 1
    assert result["lines"] == [
        "log-foundry: lost 1 event(s); SNSSink, exceeds the 1048576-byte message limit"
    ]


def test_sns_at_256_kib_sends_two_200_kib_messages_apart() -> None:
    """FR-003 AC4: T2 — the topic's default holds the request's sum, so two calls, both land."""
    sink = _build(SNSSink, max_message_bytes=262144)
    cases.run(sink, [event(0, 200 * KIB), event(1, 200 * KIB)])
    assert _ns(sink) == [[0], [1]]
    assert all(total <= 262144 for total in _sums(sink)), _sums(sink)
    assert sink.losses().failed == 0


@pytest.mark.parametrize("limit", [1024, 65536, 262144, MIB])
def test_sqs_and_sns_agree_at_every_limit(limit: int) -> None:
    """FR-003 AC6 / invariant 6: one input, one limit, the same drops, boundaries and line."""
    sizes = [limit // 2, limit // 2, limit + 1, limit // 3, limit]
    batch = [_sized(i, size) for i, size in enumerate(sizes)]
    seen = {}
    for cls in _CLASSES:
        sink = _build(cls, max_message_bytes=limit)
        result = cases.run(sink, batch)
        seen[cls.__name__] = (sink.dropped_oversized, _ns(sink), _named(result["lines"]))
        assert sink.dropped_oversized >= 1
        assert len(sink.client.requests) >= 2
        assert _named(result["lines"]) == [limit] * sink.dropped_oversized
    assert seen["SQSSink"] == seen["SNSSink"]
    assert seen["SQSSink"] == (1, [[0, 1], [3], [4]], [limit])
