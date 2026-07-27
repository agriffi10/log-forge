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
