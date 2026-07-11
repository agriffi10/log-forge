"""SPEC-009 — SaaS intake sinks: Datadog, Splunk HEC, New Relic, Honeycomb, Sentry (fakes)."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from log_forge.sinks.base import Sink
from log_forge.sinks.datadog import DatadogSink
from log_forge.sinks.honeycomb import HoneycombSink
from log_forge.sinks.newrelic import NewRelicSink
from log_forge.sinks.sentry import SentrySink
from log_forge.sinks.splunk import SplunkHECSink
from test_sinks_http import FakeOpener


# --- FR-007: Datadog --------------------------------------------------------------------


def test_datadog_intake_url_header_and_enrichment() -> None:
    opener = FakeOpener()
    DatadogSink(
        "key123", site="datadoghq.eu", service="payments", ddtags="env:prod", opener=opener
    ).emit([{"message": "m", "level": "INFO"}])
    call = opener.calls[0]
    assert call["url"] == "https://http-intake.logs.datadoghq.eu/api/v2/logs"
    assert call["headers"]["dd-api-key"] == "key123"
    entry = json.loads(call["body"])[0]
    assert entry["ddsource"] == "log-forge"
    assert entry["service"] == "payments"
    assert entry["ddtags"] == "env:prod"


def test_datadog_default_site() -> None:
    opener = FakeOpener()
    DatadogSink("k", opener=opener).emit([{"a": 1}])
    assert "datadoghq.com" in opener.calls[0]["url"]


# --- FR-008: Splunk HEC -----------------------------------------------------------------


def test_splunk_hec_envelope_auth_and_time() -> None:
    opener = FakeOpener()
    SplunkHECSink(
        "https://splunk:8088/services/collector", "tok", host="h1", opener=opener
    ).emit(
        [
            {"message": "m", "timestamp": "2026-07-11T00:00:00.000Z"},
            {"message": "n", "timestamp": "2026-07-11T00:00:00.000Z"},
        ]
    )
    call = opener.calls[0]
    assert call["headers"]["authorization"] == "Splunk tok"
    body = call["body"].decode("utf-8")
    decoder = json.JSONDecoder()
    first, index = decoder.raw_decode(body)  # HEC = concatenated JSON objects, not an array
    second, _ = decoder.raw_decode(body, index)
    assert first["event"]["message"] == "m"
    assert first["host"] == "h1"
    assert first["time"] == datetime(2026, 7, 11, tzinfo=timezone.utc).timestamp()
    assert second["event"]["message"] == "n"


# --- FR-009: New Relic ------------------------------------------------------------------


def test_newrelic_endpoint_key_and_array_body() -> None:
    opener = FakeOpener()
    NewRelicSink("nrkey", region="EU", opener=opener).emit([{"a": 1}])
    call = opener.calls[0]
    assert call["url"] == "https://log-api.eu.newrelic.com/log/v1"
    assert call["headers"]["api-key"] == "nrkey"
    assert json.loads(call["body"]) == [{"a": 1}]


def test_newrelic_invalid_region_raises() -> None:
    with pytest.raises(ValueError):
        NewRelicSink("k", region="MARS")


# --- FR-010: Honeycomb ------------------------------------------------------------------


def test_honeycomb_batch_shape_and_header() -> None:
    opener = FakeOpener()
    HoneycombSink("hckey", "mydataset", opener=opener).emit([{"a": 1}, {"b": 2}])
    call = opener.calls[0]
    assert call["url"] == "https://api.honeycomb.io/1/batch/mydataset"
    assert call["headers"]["x-honeycomb-team"] == "hckey"
    assert json.loads(call["body"]) == [{"data": {"a": 1}}, {"data": {"b": 2}}]


# --- FR-011: Sentry ---------------------------------------------------------------------


class FakeSentrySDK:
    """A stand-in for ``sentry_sdk`` recording captured events."""

    def __init__(self) -> None:
        self.events: list = []

    def capture_event(self, event: dict) -> None:
        self.events.append(event)


def test_sentry_sdk_path_with_level_gating() -> None:
    sdk = FakeSentrySDK()
    sink = SentrySink(sdk=sdk, min_level="ERROR")
    assert isinstance(sink, Sink)
    sink.emit(
        [
            {"level": "INFO", "message": "skip me"},
            {"level": "ERROR", "message": "boom"},
            {"level": "CRITICAL", "message": "worse"},
        ]
    )
    assert sink.sent == 2
    assert sink.skipped == 1
    assert [e["level"] for e in sdk.events] == ["error", "fatal"]
    assert sdk.events[0]["extra"]["message"] == "boom"


def test_sentry_http_envelope_fallback_when_sdk_absent(monkeypatch) -> None:
    monkeypatch.setattr("log_forge.sinks.sentry._import_sdk", lambda: None)
    opener = FakeOpener()
    sink = SentrySink(dsn="https://pubkey@o123.ingest.sentry.io/456", opener=opener)
    assert sink._sdk is None
    sink.emit([{"level": "ERROR", "message": "boom"}, {"level": "DEBUG", "message": "skip"}])
    assert sink.sent == 1
    assert sink.skipped == 1
    call = opener.calls[0]
    assert call["url"] == "https://o123.ingest.sentry.io/api/456/envelope/"
    assert call["headers"]["x-sentry-auth"].startswith("Sentry sentry_key=pubkey")
    lines = call["body"].decode("utf-8").strip().split("\n")
    assert len(lines) == 3  # envelope header, item header, payload
    assert json.loads(lines[1]) == {"type": "event"}
    assert json.loads(lines[2])["level"] == "error"


def test_sentry_without_sdk_or_dsn_raises(monkeypatch) -> None:
    monkeypatch.setattr("log_forge.sinks.sentry._import_sdk", lambda: None)
    with pytest.raises(ValueError):
        SentrySink()
