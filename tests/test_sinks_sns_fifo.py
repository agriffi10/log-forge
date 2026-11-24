"""SPEC-100 — ``SNSSink`` writes an SNS FIFO topic through the mechanism ``SQSSink`` uses.

FR-001's arguments and their judgement on both sinks, FR-002's ids (resolved once per run, resent
unchanged, costed), FR-003's digest of an over-long de-duplication id, FR-004's group hold and
FR-005's reading of every FIFO refusal. Where a rule binds both FIFO sinks the test runs on each,
since they share one module (``sinks/_fifo.py``) and invariant 6 asks every twin path for the same
verdict. Every client is a scripted double from ``test_sinks_queue_engine`` (``reply(entries,
call)``), so a test reads what reached the client by event and never recomputes the sink's rule.
Serves invariants 2, 6, 7, 10, 11 and 13.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import re
import sys
import types
from typing import TYPE_CHECKING, Any

import pytest

import driver_standins as standins
from log_foundry.sinks import _engine, _fifo, sns, sqs
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.sns import SNSSink
from log_foundry.sinks.sqs import SQSSink
from test_sinks_queue_engine import Client, Reply, accept, events, lost

if TYPE_CHECKING:
    from collections.abc import Callable

FIFO_ARN = "arn:aws:sns:r:1:t.fifo"
STD_ARN = "arn:aws:sns:r:1:t"
FIFO_URL = "https://sqs.example/q.fifo"
STD_URL = "https://sqs.example/q"
SINKS = ("SQSSink", "SNSSink")
_HEX32 = re.compile(r"^[0-9a-f]{32}$")


@pytest.fixture(autouse=True)
def _no_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    """Neutralizes the engine's backoff, so a multi-round test spends no real time waiting."""
    monkeypatch.setattr("log_foundry.sinks._retry.wait", lambda _delay, _stop=None: None)


@pytest.fixture
def boto3(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """A ``boto3`` stand-in that records every client built, so a refusal can prove it built none."""
    built: list[str] = []
    monkeypatch.setitem(sys.modules, "boto3", types.SimpleNamespace(client=built.append))
    return built


def _body_key(kind: str) -> str:
    return "MessageBody" if kind == "SQSSink" else "Message"


def fifo_sink(kind: str, reply: Reply = accept, **kwargs: Any) -> tuple[Any, Client]:
    """A FIFO sink of ``kind`` over a scripted client."""
    client = Client(reply)
    if kind == "SQSSink":
        return SQSSink(FIFO_URL, client=client, **kwargs), client
    return SNSSink(FIFO_ARN, client=client, **kwargs), client


def std_sink(kind: str, reply: Reply = accept, **kwargs: Any) -> tuple[Any, Client]:
    """A standard-destination sink of ``kind`` over a scripted client."""
    client = Client(reply)
    if kind == "SQSSink":
        return SQSSink(STD_URL, client=client, **kwargs), client
    return SNSSink(STD_ARN, client=client, **kwargs), client


def _ids_of(client: Client, kind: str, i: int) -> list[tuple[str | None, str | None]]:
    """Every ``(MessageGroupId, MessageDeduplicationId)`` that carried event ``i``, in send order."""
    key = _body_key(kind)
    return [
        (entry.get("MessageGroupId"), entry.get("MessageDeduplicationId"))
        for call in client.entries
        for entry in call
        if json.loads(entry[key])["i"] == i
    ]


def _fail_first_entry_of_first_call(entries: list[dict[str, Any]], call: int) -> Any:
    if call == 1:
        return {"Failed": [{"Id": entries[0]["Id"], "SenderFault": False, "Code": "T"}]}
    return {"Failed": []}


def _counting() -> tuple[list[str], Callable[..., str], Callable[..., str]]:
    """Group and de-duplication callables answering a new string on every call, counting each."""
    asked: list[str] = []

    def group(event: dict[str, object]) -> str:
        asked.append(f"g{event['i']}")
        return f"g{event['i']}-{len(asked)}"

    def dedup(event: dict[str, object]) -> str:
        asked.append(f"d{event['i']}")
        return f"d{event['i']}-{len(asked)}"

    return asked, group, dedup


# --- FR-001: the arguments and their judgement ------------------------------------------------


def test_the_signature_appends_the_three_fifo_arguments() -> None:
    """FR-001 AC1 / invariant 10: after ``max_message_bytes``, keyword-only, default ``None``."""
    params = list(inspect.signature(SNSSink.__init__).parameters.values())
    assert [(p.name, p.kind, p.default) for p in params[-4:]] == [
        (name, inspect.Parameter.KEYWORD_ONLY, None)
        for name in ("max_message_bytes", "fifo", "message_group_id", "message_deduplication_id")
    ]
    sink = SNSSink("arn", client=Client(), max_retries=2)
    assert sink.max_retries == 2 and sink.fifo is False


def test_a_fifo_arn_selects_fifo_and_a_standard_arn_does_not() -> None:
    """FR-001 AC2: the ``.fifo`` suffix decides, as the queue URL does on ``SQSSink``."""
    assert SNSSink(FIFO_ARN, client=Client()).fifo is True
    assert SNSSink(STD_ARN, client=Client()).fifo is False


@pytest.mark.parametrize("kind", SINKS)
def test_fifo_true_and_false_on_a_fifo_name_construct(kind: str) -> None:
    """FR-001 AC2 / invariant 13: ``fifo=False`` overrides a ``.fifo`` name, the recorded exception."""
    assert fifo_sink(kind, fifo=True)[0].fifo is True
    assert fifo_sink(kind, fifo=False)[0].fifo is False


@pytest.mark.parametrize("kind", SINKS)
def test_fifo_true_without_the_suffix_is_refused_before_any_client(
    boto3: list[str], kind: str
) -> None:
    """FR-001 AC3: AWS requires the suffix, so such a sink could never deliver."""
    with pytest.raises(ValueError) as info:
        if kind == "SQSSink":
            SQSSink(STD_URL, fifo=True)
        else:
            SNSSink(STD_ARN, fifo=True)
    noun, word = ("queue_url", "queue") if kind == "SQSSink" else ("topic_arn", "topic")
    assert str(info.value) == (
        f"{kind} fifo=True needs a {noun} ending in .fifo: AWS requires every FIFO {word} name "
        "to end so"
    )
    assert boto3 == []


_IDS: list[tuple[str, dict[str, Any], str]] = [
    ("group-constant", {"message_group_id": "g"}, "message_group_id"),
    ("group-callable", {"message_group_id": lambda e: "g"}, "message_group_id"),
    ("dedup-callable", {"message_deduplication_id": lambda e: "d"}, "message_deduplication_id"),
    (
        "both",
        {"message_group_id": "g", "message_deduplication_id": lambda e: "d"},
        "message_group_id, message_deduplication_id",
    ),
]


@pytest.mark.parametrize("route", ["by-name", "fifo-false"])
@pytest.mark.parametrize(("case", "kwargs", "names"), _IDS, ids=[c[0] for c in _IDS])
@pytest.mark.parametrize("kind", SINKS)
def test_an_id_argument_on_a_standard_destination_is_refused_naming_it(
    boto3: list[str], kind: str, case: str, kwargs: dict[str, Any], names: str, route: str
) -> None:
    """FR-001 AC5 / invariant 13: a standard destination never reads an id, so it is refused."""
    if route == "by-name":
        destination = STD_URL if kind == "SQSSink" else STD_ARN
    else:
        destination = FIFO_URL if kind == "SQSSink" else FIFO_ARN
        kwargs = {**kwargs, "fifo": False}
    cls = SQSSink if kind == "SQSSink" else SNSSink
    with pytest.raises(ValueError) as info:
        cls(destination, **kwargs)
    noun, word = ("queue_url", "queue") if kind == "SQSSink" else ("topic_arn", "topic")
    assert str(info.value) == (
        f"{kind} cannot apply {names} to a standard {word}; drop it, or use a {noun} ending in .fifo"
    )
    assert boto3 == []


_BASE_REFUSALS: list[tuple[str, Callable[[], object], type[Exception], str]] = [
    (
        "sqs-max_retries",
        lambda: SQSSink(STD_URL, max_retries=2.5, fifo="x"),  # type: ignore[arg-type]
        TypeError,
        "SQSSink max_retries must be an integer, not float",
    ),
    (
        "sqs-max_message_bytes",
        lambda: SQSSink(STD_URL, max_message_bytes=5, fifo="x"),  # type: ignore[arg-type]
        ValueError,
        "SQSSink max_message_bytes must be from 1024 to 1048576 bytes, got 5",
    ),
    (
        "sns-max_message_bytes",
        lambda: SNSSink(STD_ARN, max_message_bytes=5, fifo="x"),  # type: ignore[arg-type]
        ValueError,
        "SNSSink max_message_bytes must be from 1024 to 1048576 bytes, got 5",
    ),
    (
        "sqs-group",
        lambda: SQSSink(STD_URL, fifo="x", message_group_id=5),  # type: ignore[arg-type]
        TypeError,
        "SQSSink message_group_id must be a str, a callable or None, not int",
    ),
    (
        "sns-group",
        lambda: SNSSink(STD_ARN, fifo="x", message_group_id=5),  # type: ignore[arg-type]
        TypeError,
        "SNSSink message_group_id must be a str, a callable or None, not int",
    ),
]


@pytest.mark.parametrize(
    ("case", "build", "kind", "text"), _BASE_REFUSALS, ids=[c[0] for c in _BASE_REFUSALS]
)
def test_a_call_refused_at_the_base_keeps_its_exact_error(
    boto3: list[str], case: str, build: Callable[[], object], kind: type[Exception], text: str
) -> None:
    """FR-001 AC7: ``judge`` runs after every earlier refusal, so a two-fault call reads as before."""
    with pytest.raises(kind) as info:
        build()
    assert type(info.value) is kind
    assert str(info.value) == text
    assert boto3 == []


def test_a_str_subclass_group_is_sent_as_its_str_without_its_own_strip() -> None:
    """FR-001 AC8: the constant is read through ``str.__str__``, as on ``SQSSink``."""
    stripped: list[str] = []

    class Group(str):
        def strip(self, chars: str | None = None) -> str:
            stripped.append(str.__str__(self))
            return str.strip(self, chars)

    sink, client = fifo_sink("SNSSink", message_group_id=Group(" g "))
    sink.emit(events(2))
    assert [e["MessageGroupId"] for call in client.entries for e in call] == ["g", "g"]
    assert stripped == []


def test_the_aliases_are_the_shared_objects() -> None:
    """FR-001 AC9: one definition, exported from both sinks' modules."""
    assert sns.GroupIdSource is sqs.GroupIdSource is _fifo.GroupIdSource
    assert sns.DedupIdSource is sqs.DedupIdSource is _fifo.DedupIdSource
    assert {"GroupIdSource", "DedupIdSource"} <= set(sns.__all__)
    assert sqs.DEFAULT_GROUP_ID is _fifo.DEFAULT_GROUP_ID
    assert sqs.MAX_ID_LEN is _fifo.MAX_ID_LEN
    moved = {
        "_Prepared": _fifo.Prepared,
        "_Plan": _fifo.Plan,
        "_Entry": _fifo.Entry,
        "_bounded": _fifo.bounded,
        "_hold": _fifo.hold,
        "_block": _fifo.block,
    }
    assert {name: getattr(sqs, name) for name in moved} == moved


# --- FR-002: the ids, once per run, resent unchanged and costed -------------------------------


def test_a_fifo_topic_sends_trace_id_and_log_id_and_a_standard_topic_neither() -> None:
    """FR-002 AC1: the defaults are the event's own ``trace_id`` and ``log_id``."""
    batch = [{"i": i, "log_id": f"L{i}", "trace_id": f"T{i}"} for i in range(2)]
    sink, client = fifo_sink("SNSSink")
    sink.emit(batch)
    assert [(e["MessageGroupId"], e["MessageDeduplicationId"]) for e in client.entries[0]] == [
        ("T0", "L0"),
        ("T1", "L1"),
    ]
    standard, plain = std_sink("SNSSink")
    standard.emit(batch)
    assert [sorted(e) for e in plain.entries[0]] == [["Id", "Message"], ["Id", "Message"]]


@pytest.mark.parametrize("kind", SINKS)
def test_a_resend_carries_the_first_sends_ids(kind: str) -> None:
    """FR-002 AC2 / invariant 2: the resend's ids are the first send's, each callable asked once."""
    asked, group, dedup = _counting()
    sink, client = fifo_sink(
        kind,
        _fail_first_entry_of_first_call,
        message_group_id=group,
        message_deduplication_id=dedup,
    )
    sink.emit(events(2))
    assert len(client.entries) == 2, "the failed entry is resent in a second round"
    first, resend = _ids_of(client, kind, 0)
    assert resend == first
    assert sorted(asked) == ["d0", "d1", "g0", "g1"], "once per event, across both rounds"


@pytest.mark.parametrize("kind", SINKS)
def test_an_id_callable_raising_for_one_event_costs_only_it(
    kind: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-002 AC4 / invariant 2: the other event is sent, the raising one dropped and named."""

    def group(event: dict[str, object]) -> str:
        if event["i"] == 0:
            raise ValueError("no group")
        return "g"

    sink, client = fifo_sink(kind, message_group_id=group)
    sink.emit(events(2))
    assert [json.loads(e[_body_key(kind)])["i"] for e in client.entries[0]] == [1]
    assert sink.losses() == SinkLosses(dropped=1, failed=0)
    assert lost(capsys.readouterr().err) == [
        f"log-foundry: lost 1 event(s); {kind}, a FIFO id callable raised ValueError"
    ]


@pytest.mark.parametrize("kind", SINKS)
def test_an_id_callable_raising_for_every_event_reraises_by_identity(kind: str) -> None:
    """FR-002 AC4 / invariant 7: a batch of nothing but id failures re-raises the first one."""
    raised: list[Exception] = []

    def group(event: dict[str, object]) -> str:
        error = ValueError(f"no group {event['i']}")
        raised.append(error)
        raise error

    sink, client = fifo_sink(kind, message_group_id=group)
    with pytest.raises(ValueError) as info:
        sink.emit(events(2))
    assert info.value is raised[0]
    assert client.entries == []
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


@pytest.mark.parametrize("kind", SINKS)
def test_an_id_failure_and_an_oversized_event_settle_with_no_call(
    kind: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-002 AC4 / invariant 7: a batch of drops only returns, each drop counted and lined."""

    def group(event: dict[str, object]) -> str:
        if event["i"] == 0:
            raise ValueError("no group")
        return "g"

    sink, client = fifo_sink(kind, message_group_id=group, max_message_bytes=1024)
    sink.emit([{"i": 0, "log_id": "a"}, {"i": 1, "log_id": "b", "pad": "x" * 2000}])
    assert client.entries == []
    assert sink.losses() == SinkLosses(dropped=2, failed=0)
    assert sink.dropped_oversized == 1
    lines = lost(capsys.readouterr().err)
    assert len(lines) == 2, lines
    assert f"log-foundry: lost 1 event(s); {kind}, a FIFO id callable raised ValueError" in lines
    assert any(
        line.startswith(f"log-foundry: lost 1 event(s); {kind}, ")
        and line.endswith("exceeds the 1024-byte message limit")
        for line in lines
    ), lines


_UNCOSTABLE = [
    ("bytes", b"abc", "AttributeError"),
    ("lone-surrogate", "ok\ud800", "UnicodeEncodeError"),
]


@pytest.mark.parametrize("which", ["group", "dedup"])
@pytest.mark.parametrize(("case", "answer", "error"), _UNCOSTABLE, ids=[c[0] for c in _UNCOSTABLE])
@pytest.mark.parametrize("kind", SINKS)
def test_an_id_answer_that_cannot_be_costed_drops_only_its_event(
    kind: str,
    case: str,
    answer: object,
    error: str,
    which: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """FR-002 AC5 / invariant 2: it raised at the costing, outside the ``try``, and lost the round."""

    def source(event: dict[str, object]) -> Any:
        return answer if event["i"] == 0 else f"ok{event['i']}"

    key = "message_group_id" if which == "group" else "message_deduplication_id"
    sink, client = fifo_sink(kind, **{key: source})
    sink.emit(events(2))
    assert len(client.entries) == 1 and len(client.entries[0]) == 1
    sent = client.entries[0][0]
    assert json.loads(sent[_body_key(kind)])["i"] == 1
    assert {"MessageGroupId", "MessageDeduplicationId"} <= set(sent)
    assert sink.losses() == SinkLosses(dropped=1, failed=0)
    assert lost(capsys.readouterr().err) == [
        f"log-foundry: lost 1 event(s); {kind}, a FIFO id callable raised {error}"
    ]


@pytest.mark.parametrize("kind", SINKS)
def test_blank_answers_fall_back_and_the_minted_id_survives_the_resend(kind: str) -> None:
    """FR-002 AC6: a blank group is ``log-foundry``; a blank dedup a minted id, kept on resend."""
    sink, client = fifo_sink(
        kind,
        _fail_first_entry_of_first_call,
        message_group_id=lambda e: "  ",
        message_deduplication_id=lambda e: "",
    )
    sink.emit(events(1))
    first, resend = _ids_of(client, kind, 0)
    assert first[0] == "log-foundry"
    assert first[1] is not None and _HEX32.match(first[1]), first
    assert resend == first


def test_the_ids_count_against_the_message_limit(capsys: pytest.CaptureFixture[str]) -> None:
    """FR-002 AC7: 40 bytes under the limit, with ids costing 64, is over it on a FIFO topic."""
    limit = SNSSink.MAX_BYTES
    base: dict[str, object] = {"i": 0, "log_id": "a" * 32, "trace_id": "b" * 32, "pad": ""}
    event = {**base, "pad": "x" * (limit - 40 - len(json.dumps(base)))}
    assert len(json.dumps(event).encode("utf-8")) == limit - 40
    sink, client = fifo_sink("SNSSink")
    sink.emit([event])
    assert client.entries == []
    assert sink.dropped_oversized == 1
    assert sink.losses() == SinkLosses(dropped=1, failed=0)
    assert lost(capsys.readouterr().err) == [
        "log-foundry: lost 1 event(s); SNSSink, exceeds the 262144-byte message limit"
    ]
    standard, plain = std_sink("SNSSink")
    standard.emit([event])
    assert len(plain.entries) == 1 and standard.dropped_oversized == 0


class _DedupTopic:
    """A FIFO topic stand-in that keeps one copy per de-duplication id, as the SPEC-092 double does.

    Its first ``publish_batch`` takes every entry and then raises ``ReadTimeoutError``, the reply
    lost after the write; every later call takes its entries and answers them all.

    Attributes:
      held: Each held message's body, one per de-duplication id.
      calls: Every call's entries.
    """

    def __init__(self, timeout: type[Exception]) -> None:
        self.timeout = timeout
        self.seen: set[str] = set()
        self.held: list[str] = []
        self.calls: list[list[dict[str, Any]]] = []

    def publish_batch(
        self, *, TopicArn: str, PublishBatchRequestEntries: list[dict[str, Any]]
    ) -> Any:
        self.calls.append([dict(e) for e in PublishBatchRequestEntries])
        for entry in PublishBatchRequestEntries:
            key = entry["MessageDeduplicationId"]
            if key not in self.seen:
                self.seen.add(key)
                self.held.append(entry["Message"])
        if len(self.calls) == 1:
            raise self.timeout("read timed out")
        return {
            "Successful": [{"Id": e["Id"]} for e in PublishBatchRequestEntries],
            "Failed": [],
        }


def _dedup_run(monkeypatch: pytest.MonkeyPatch) -> _DedupTopic:
    botocore = standins.install(monkeypatch, standins.BOTOCORE)
    topic = _DedupTopic(botocore.ReadTimeoutError)
    _asked, group, dedup = _counting()
    sink = SNSSink(FIFO_ARN, client=topic, message_group_id=group, message_deduplication_id=dedup)
    sink.emit(events(3))
    return topic


def test_a_deduplicating_topic_holds_each_event_once_after_a_lost_reply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-002 AC8 / invariant 2: the resend carries the first ids, so the topic absorbs it."""
    topic = _dedup_run(monkeypatch)
    assert len(topic.calls) == 2, "the lost reply is resent once"
    assert sorted(json.loads(body)["i"] for body in topic.held) == [0, 1, 2]
    per_event = [
        {
            e["MessageDeduplicationId"]
            for call in topic.calls
            for e in call
            if json.loads(e["Message"])["i"] == i
        }
        for i in range(3)
    ]
    assert [len(ids) for ids in per_event] == [1, 1, 1], per_event


def test_a_deduplicating_topic_without_the_cache_holds_more(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-002 AC8: with ids resolved per round, the resend is a new id, and the topic holds both."""
    real = SNSSink._round

    def per_round(self: SNSSink, batch: list[dict[str, object]]) -> Any:
        _engine.run_context().notes(self).pop("prepared", None)
        return real(self, batch)

    monkeypatch.setattr(SNSSink, "_round", per_round)
    topic = _dedup_run(monkeypatch)
    assert len(topic.held) > 3


# --- FR-003: an over-long de-duplication id goes out as its digest ----------------------------


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@pytest.mark.parametrize("kind", SINKS)
def test_two_long_dedup_answers_sharing_a_prefix_get_two_digests(kind: str) -> None:
    """FR-003 AC1 / invariant 2: a 128-character prefix let two events share one id."""
    answers = {0: "p" * 128 + "a" * 72, 1: "p" * 128 + "b" * 72}
    sink, client = fifo_sink(kind, message_deduplication_id=lambda e: answers[int(e["i"])])
    sink.emit(events(2))
    sent = [e["MessageDeduplicationId"] for e in client.entries[0]]
    assert sent == [_digest(answers[0]), _digest(answers[1])]
    assert sent[0] != sent[1] and all(len(s) == 64 for s in sent)


_BOUNDARIES: list[tuple[str, str, Callable[[str], bool]]] = [
    ("exactly-128", "e" * 128, lambda sent: sent == "e" * 128),
    ("128-in-spaces", " " + "s" * 128 + " ", lambda sent: sent == "s" * 128),
    ("129-then-space", "n" * 129 + " ", lambda sent: sent == _digest("n" * 129)),
    ("blank", "   ", lambda sent: bool(_HEX32.match(sent))),
]


@pytest.mark.parametrize("kind", SINKS)
@pytest.mark.parametrize(("case", "answer", "check"), _BOUNDARIES, ids=[c[0] for c in _BOUNDARIES])
def test_the_digest_boundaries(
    kind: str, case: str, answer: str, check: Callable[[str], bool]
) -> None:
    """FR-003 AC3: 128 is sent as it is, 129 is digested, after the strip; blank is minted."""
    sink, client = fifo_sink(kind, message_deduplication_id=lambda e: answer)
    sink.emit(events(1))
    sent = client.entries[0][0]["MessageDeduplicationId"]
    assert check(sent), (case, sent)


@pytest.mark.parametrize("kind", SINKS)
def test_the_resend_of_a_digested_id_carries_the_same_digest(kind: str) -> None:
    """FR-003 AC3: the digest is part of the entry the run keeps, so a resend carries it."""
    sink, client = fifo_sink(
        kind, _fail_first_entry_of_first_call, message_deduplication_id=lambda e: "r" * 200
    )
    sink.emit(events(1))
    first, resend = _ids_of(client, kind, 0)
    assert first[1] == resend[1] == _digest("r" * 200)


def test_sns_fifo_raises_a_sink_delivery_error_when_nothing_lands() -> None:
    """A FIFO topic that is unreachable raises, as a standard topic does (SPEC-026 FR-001)."""

    def down(entries: list[dict[str, Any]], call: int) -> Any:
        raise ConnectionError("unreachable")

    sink, _client = fifo_sink("SNSSink", down, max_retries=0)
    with pytest.raises(SinkDeliveryError):
        sink.emit(events(2))
    assert sink.losses() == SinkLosses(dropped=0, failed=2)
