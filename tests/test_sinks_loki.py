"""SPEC-009 — LokiSink push payload, label grouping, and nanosecond timestamps (fake opener)."""

from __future__ import annotations

import json
from datetime import UTC, datetime

from log_foundry.sinks.base import Sink
from log_foundry.sinks.loki import LokiSink
from test_sinks_http import FakeOpener


def test_is_a_sink() -> None:
    assert isinstance(LokiSink("http://loki:3100"), Sink)


def test_push_payload_labels_and_ns_timestamp() -> None:
    opener = FakeOpener()
    LokiSink("http://loki:3100/", opener=opener).emit(
        [
            {
                "service": "api",
                "env": "prod",
                "level": "INFO",
                "timestamp": "2026-07-11T00:00:00.000Z",
                "message": "hello",
            }
        ]
    )
    call = opener.calls[0]
    assert call["url"] == "http://loki:3100/loki/api/v1/push"
    assert call["headers"]["content-type"] == "application/json"
    payload = json.loads(call["body"])
    assert len(payload["streams"]) == 1
    stream = payload["streams"][0]
    assert stream["stream"] == {"service": "api", "env": "prod", "level": "INFO"}
    ns, line = stream["values"][0]
    expected_ns = str(int(datetime(2026, 7, 11, tzinfo=UTC).timestamp() * 1_000_000_000))
    assert ns == expected_ns
    assert json.loads(line)["message"] == "hello"


def test_events_group_into_streams_by_label_set() -> None:
    opener = FakeOpener()
    LokiSink("http://loki:3100", labels=("level",), opener=opener).emit(
        [{"level": "INFO"}, {"level": "ERROR"}, {"level": "INFO"}]
    )
    payload = json.loads(opener.calls[0]["body"])
    by_level = {tuple(s["stream"].items()): len(s["values"]) for s in payload["streams"]}
    assert by_level == {(("level", "INFO"),): 2, (("level", "ERROR"),): 1}


def test_timestamp_fallback_is_numeric_when_absent() -> None:
    opener = FakeOpener()
    LokiSink("http://loki:3100", labels=("service",), opener=opener).emit([{"service": "api"}])
    ns = json.loads(opener.calls[0]["body"])["streams"][0]["values"][0][0]
    assert ns.isdigit()


# --- SPEC-038 FR-001: chunking, and grouping that survives it ----------------------------


def test_a_large_batch_is_split_and_every_line_survives() -> None:
    opener = FakeOpener()
    sink = LokiSink("http://loki:3100", max_batch_bytes=20_000, opener=opener)
    sink.emit([{"service": "api", "level": "INFO", "n": i, "pad": "x" * 500} for i in range(200)])
    assert len(opener.calls) > 1
    assert all(len(call["body"]) <= 20_000 for call in opener.calls)
    values = [
        value
        for call in opener.calls
        for stream in json.loads(call["body"])["streams"]
        for value in stream["values"]
    ]
    assert len(values) == 200
    assert sorted(json.loads(line)["n"] for _ts, line in values) == list(range(200))


def test_streams_are_regrouped_inside_each_chunk() -> None:
    """Grouping is per request, so a chunk carries only the labels its own events have."""
    opener = FakeOpener()
    sink = LokiSink("http://loki:3100", max_batch_count=2, opener=opener)
    sink.emit(
        [
            {"service": "api", "n": 0},
            {"service": "api", "n": 1},
            {"service": "worker", "n": 2},
            {"service": "worker", "n": 3},
        ]
    )
    assert len(opener.calls) == 2
    first, second = (json.loads(call["body"])["streams"] for call in opener.calls)
    assert len(first) == 1 and first[0]["stream"] == {"service": "api"}
    assert len(second) == 1 and second[0]["stream"] == {"service": "worker"}
    assert len(first[0]["values"]) == 2 and len(second[0]["values"]) == 2


def test_a_chunk_holding_two_label_sets_still_carries_two_streams() -> None:
    opener = FakeOpener()
    LokiSink("http://loki:3100", opener=opener).emit(
        [{"service": "api", "n": 0}, {"service": "worker", "n": 1}]
    )
    streams = json.loads(opener.calls[0]["body"])["streams"]
    assert sorted(stream["stream"]["service"] for stream in streams) == ["api", "worker"]
