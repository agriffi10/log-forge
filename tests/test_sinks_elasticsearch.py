"""SPEC-009 — Elasticsearch/OpenSearch _bulk framing + items[] error parsing (fake opener)."""

from __future__ import annotations

import json

import pytest

from log_foundry.sinks.base import Sink, SinkDeliveryError
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
    with pytest.raises(SinkDeliveryError, match="delivered none of 2 chunk"):
        sink.emit([{"n": i} for i in range(4)])
    assert sink.item_errors == 4


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
