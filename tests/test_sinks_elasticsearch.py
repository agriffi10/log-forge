"""SPEC-009 — Elasticsearch/OpenSearch _bulk framing + items[] error parsing (fake opener)."""

from __future__ import annotations

import json

from log_forge.sinks.base import Sink
from log_forge.sinks.elasticsearch import ElasticsearchSink, OpenSearchSink
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
        '{"index": {"_index": "logs"}}\n{"a": 1}\n'
        '{"index": {"_index": "logs"}}\n{"b": 2}\n'
    )


def test_bulk_item_errors_are_counted() -> None:
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
