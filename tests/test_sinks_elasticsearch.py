"""SPEC-009 — Elasticsearch/OpenSearch _bulk framing + items[] error parsing (fake opener)."""

from __future__ import annotations

import json

import pytest

from log_foundry.sinks.base import Sink, SinkDeliveryError, SinkLosses
from log_foundry.sinks.elasticsearch import ElasticsearchSink, OpenSearchSink
from test_sinks_http import FakeOpener, FakeResponse


def test_is_a_sink() -> None:
    assert isinstance(ElasticsearchSink("http://es:9200", index="logs"), Sink)


def test_bulk_framing_action_and_source_lines() -> None:
    opener = FakeOpener()
    ElasticsearchSink("http://es:9200/", index="logs", opener=opener).emit([{"a": 1}, {"b": 2}])
    call = opener.calls[0]
    assert call["url"] == "http://es:9200/_bulk"
    assert call["headers"]["content-type"] == "application/x-ndjson"
    assert call["body"].decode("utf-8") == (
        '{"index": {"_index": "logs"}}\n{"a": 1}\n{"index": {"_index": "logs"}}\n{"b": 2}\n'
    )


def test_bulk_item_errors_are_counted(capsys) -> None:
    response = FakeResponse(
        200,
        json.dumps(
            {
                "errors": True,
                "items": [
                    {"index": {"status": 201}},
                    {"index": {"status": 400, "error": {"type": "mapper_parsing_exception"}}},
                ],
            }
        ).encode("utf-8"),
    )
    opener = FakeOpener([response])
    sink = ElasticsearchSink("http://es:9200", index="logs", opener=opener)
    sink.emit([{"a": 1}, {"b": 2}])
    assert sink.item_errors == 1  # only the errored item counts; the indexed one is retained
    assert "lost 1 bulk item(s)" in capsys.readouterr().err


def test_no_errors_flag_skips_item_parsing() -> None:
    response = FakeResponse(200, json.dumps({"errors": False, "items": []}).encode("utf-8"))
    sink = ElasticsearchSink("http://es:9200", index="logs", opener=FakeOpener([response]))
    sink.emit([{"a": 1}])
    assert sink.item_errors == 0


def test_basic_auth_applied() -> None:
    opener = FakeOpener()
    ElasticsearchSink("http://es", index="logs", auth=("elastic", "pw"), opener=opener).emit(
        [{"a": 1}]
    )
    assert opener.calls[0]["headers"]["authorization"].startswith("Basic ")


def test_opensearch_reuses_bulk_logic() -> None:
    opener = FakeOpener()
    sink = OpenSearchSink("http://os:9200", index="logs", opener=opener)
    assert isinstance(sink, Sink)
    sink.emit([{"a": 1}])
    assert opener.calls[0]["url"] == "http://os:9200/_bulk"


# --- SPEC-038 FR-001: chunking, and per-chunk adjudication ------------------------------


def _ok(count: int) -> bytes:
    return json.dumps({"errors": False, "items": [{"index": {}} for _ in range(count)]}).encode()


def _all_failed(count: int) -> bytes:
    return json.dumps(
        {"errors": True, "items": [{"index": {"error": "nope"}} for _ in range(count)]}
    ).encode()


def test_a_large_batch_is_split_and_every_action_pair_stays_intact() -> None:
    """An action line and its source line are one item, so a split can never separate them."""
    opener = FakeOpener([FakeResponse(200, _ok(50))])
    sink = ElasticsearchSink("http://es:9200", index="logs", max_batch_count=50, opener=opener)
    sink.emit([{"n": i} for i in range(120)])
    assert [call["body"].decode().count("\n") for call in opener.calls] == [100, 100, 40]
    indexed: list[int] = []
    for call in opener.calls:
        lines = call["body"].decode().rstrip("\n").split("\n")
        assert len(lines) % 2 == 0, "a chunk ended between an action line and its source"
        assert all(json.loads(line) == {"index": {"_index": "logs"}} for line in lines[::2])
        indexed.extend(json.loads(line)["n"] for line in lines[1::2])
    assert indexed == list(range(120)), "every event indexed once, in order, across the chunks"


def test_one_chunk_rejecting_every_item_does_not_fail_a_batch_that_indexed_others() -> None:
    """AC-3, at the sink whose 200 can still mean nothing was indexed."""
    opener = FakeOpener([FakeResponse(200, _all_failed(2)), FakeResponse(200, _ok(2))])
    sink = ElasticsearchSink("http://es:9200", index="logs", max_batch_count=2, opener=opener)
    sink.emit([{"n": i} for i in range(4)])
    assert len(opener.calls) == 2
    assert sink.item_errors == 2, "the rejected chunk is counted"
    assert sink.losses().failed == 2


def test_every_chunk_rejecting_every_item_is_a_total_failure() -> None:
    opener = FakeOpener([FakeResponse(200, _all_failed(2))])
    sink = ElasticsearchSink("http://es:9200", index="logs", max_batch_count=2, opener=opener)
    with pytest.raises(SinkDeliveryError, match="delivered none of 4 event"):
        sink.emit([{"n": i} for i in range(4)])
    assert sink.item_errors == 4


# --- A 2xx body with no readable verdict: counted and announced, never raised or re-sent ------


_TOO_DEEP = b"[" * 1_000_000
_REJECTED_THEN_TOO_DEEP = _all_failed(2)[:-1] + b', "tail": ' + _TOO_DEEP
_NO_VERDICT = {
    "empty": b"",
    "syntax": b"not json",
    "encoding": b"\xff\xfe\xff",
    "depth": _TOO_DEEP,
    "rejected-then-too-deep": _REJECTED_THEN_TOO_DEEP,
    "array": b"[]",
    "number": b"5",
    "string": b'"x"',
    "null": b"null",
}
_NO_VERDICT_LINE = (
    "lost 2 event(s); {name} could not adjudicate a _bulk response "
    "(2 event(s) sent, 0 readable item(s), 0 error(s)); they may have landed; not retried"
)


@pytest.mark.parametrize("body", [_TOO_DEEP, _REJECTED_THEN_TOO_DEEP], ids=["open", "tail"])
def test_the_too_deep_bodies_really_are_recursion_errors_not_decoding_ones(body) -> None:
    """The premise of the tests below: without it `depth` would just repeat `syntax`."""
    with pytest.raises(RecursionError):
        json.loads(body)


@pytest.mark.parametrize("sink_type", [ElasticsearchSink, OpenSearchSink])
@pytest.mark.parametrize("body", _NO_VERDICT.values(), ids=_NO_VERDICT.keys())
def test_a_2xx_body_with_no_readable_verdict_is_counted_once(sink_type, body, capsys) -> None:
    """Undecodable and non-object bodies are charged whole, and the request is not re-sent."""
    opener = FakeOpener([FakeResponse(200, body)])
    sink = sink_type("http://es:9200", index="logs", opener=opener)
    sink.emit([{"a": 1}, {"b": 2}])
    assert len(opener.calls) == 1, "the request succeeded, so it is not sent again"
    assert sink.dropped_unadjudicated == 2
    assert sink.item_errors == 0
    assert sink.failed == 0
    assert sink.losses() == SinkLosses(dropped=0, failed=2)
    lines = capsys.readouterr().err.splitlines()
    assert len(lines) == 1
    assert _NO_VERDICT_LINE.format(name=sink_type.__name__) in lines[0]


def test_a_decoder_memory_error_is_counted_rather_than_escaping_emit(monkeypatch, capsys) -> None:
    """`MemoryError` is no decoding error, so only the wide catch keeps it inside the sink."""

    def out_of_memory(payload: bytes) -> object:
        raise MemoryError

    monkeypatch.setattr("log_foundry.sinks.elasticsearch.json.loads", out_of_memory)
    opener = FakeOpener([FakeResponse(200, _ok(2))])
    sink = ElasticsearchSink("http://es:9200", index="logs", opener=opener)
    sink.emit([{"a": 1}, {"b": 2}])
    assert len(opener.calls) == 1
    assert sink.dropped_unadjudicated == 2
    assert sink.losses() == SinkLosses(dropped=0, failed=2)
    assert _NO_VERDICT_LINE.format(name="ElasticsearchSink") in capsys.readouterr().err


@pytest.mark.parametrize("raised", [KeyboardInterrupt, SystemExit])
def test_a_base_exception_from_the_decoder_propagates_with_nothing_counted(
    monkeypatch, capsys, raised
) -> None:
    """Invariant 1 catches `Exception` and never `BaseException`, so these reach the caller."""

    def interrupted(payload: bytes) -> object:
        raise raised

    monkeypatch.setattr("log_foundry.sinks.elasticsearch.json.loads", interrupted)
    sink = ElasticsearchSink(
        "http://es:9200", index="logs", opener=FakeOpener([FakeResponse(200, _ok(2))])
    )
    with pytest.raises(raised):
        sink.emit([{"a": 1}, {"b": 2}])
    assert sink.dropped_unadjudicated == 0
    assert sink.losses() == SinkLosses(dropped=0, failed=0)
    assert capsys.readouterr().err == ""


_UNVERIFIED = {
    "empty-object": b"{}",
    "errors-false": b'{"errors": false}',
    "errors-null": b'{"errors": null}',
    "errors-zero": b'{"errors": 0}',
    "empty-items": b'{"items": []}',
    "error-document": b'{"error": {"type": "auth"}, "status": 500}',
    "items-all-errored": b'{"items": [{"index": {"error": "x"}}, {"index": {"error": "x"}}]}',
}


@pytest.mark.parametrize("body", _UNVERIFIED.values(), ids=_UNVERIFIED.keys())
def test_an_object_without_truthy_errors_is_read_as_success_unverified(body, capsys) -> None:
    """Pins what the code does, not that it is right: architecture.md §12 holds the exposure.

    The last two bodies report failure and still count nothing. A change that starts charging
    them has to decide what an endpoint that omits `errors` costs, with the Astra profile.
    """
    sink = ElasticsearchSink(
        "http://es:9200", index="logs", opener=FakeOpener([FakeResponse(200, body)])
    )
    sink.emit([{"a": 1}, {"b": 2}])
    assert sink.dropped_unadjudicated == 0
    assert sink.losses() == SinkLosses(dropped=0, failed=0)
    assert capsys.readouterr().err == ""


def test_a_short_items_array_and_an_unreadable_body_write_the_same_shape_of_line(capsys) -> None:
    """One route: the line differs only in the counts the readable array supplies."""
    short = json.dumps({"errors": True, "items": [{"index": {"error": "nope"}}]}).encode()
    sink = ElasticsearchSink(
        "http://es:9200", index="logs", opener=FakeOpener([FakeResponse(200, short)])
    )
    sink.emit([{"a": 1}, {"b": 2}])
    assert sink.dropped_unadjudicated == 2
    assert (
        "lost 2 event(s); ElasticsearchSink could not adjudicate a _bulk response "
        "(2 event(s) sent, 1 readable item(s), 1 error(s)); they may have landed; not retried"
    ) in capsys.readouterr().err


@pytest.mark.parametrize(
    "responses",
    [(_TOO_DEEP, _all_failed(2)), (_all_failed(2), _TOO_DEEP)],
    ids=["too-deep-first", "too-deep-last"],
)
def test_a_too_deep_chunk_leaves_its_siblings_outcome_standing(responses, capsys) -> None:
    """The later chunk is still sent, and each chunk keeps its own outcome, in either order."""
    opener = FakeOpener([FakeResponse(200, body) for body in responses])
    sink = ElasticsearchSink("http://es:9200", index="logs", max_batch_count=2, opener=opener)
    sink.emit([{"n": i} for i in range(4)])
    assert len(opener.calls) == 2, "both chunks sent once, none skipped and none re-sent"
    assert sink.item_errors == 2, "only the chunk the server rejected is counted as rejected"
    assert sink.dropped_unadjudicated == 2, "only the too-deep chunk is counted as unadjudicated"
    assert sink.losses() == SinkLosses(dropped=0, failed=4)
    err = capsys.readouterr().err
    assert "lost 2 bulk item(s)" in err
    assert _NO_VERDICT_LINE.format(name="ElasticsearchSink") in err


def test_a_too_deep_chunk_beside_a_delivered_one_charges_only_itself() -> None:
    opener = FakeOpener([FakeResponse(200, _ok(2)), FakeResponse(200, _TOO_DEEP)])
    sink = ElasticsearchSink("http://es:9200", index="logs", max_batch_count=2, opener=opener)
    sink.emit([{"n": i} for i in range(4)])
    assert len(opener.calls) == 2
    assert sink.dropped_unadjudicated == 2
    assert sink.losses() == SinkLosses(dropped=0, failed=2)


@pytest.mark.parametrize("bad", ["my index", "a\tb", "a\nb", ""])
def test_a_whitespace_or_empty_index_is_refused(bad: str) -> None:
    """SPEC-049 FR-004, and the mechanism is NOT the one the audit recorded.

    The audit called this frame corruption; it is not. `json.dumps` escapes the `_bulk` action
    line correctly, so the NDJSON frame stays intact, and an index name the cluster rejects
    already arrives as a counted per-item error. It is refused because a name the cluster can
    never accept makes every batch fail for the life of the process, and a startup error is the
    honest place to say so.
    """
    with pytest.raises(ValueError, match="index"):
        ElasticsearchSink("http://h", index=bad)
