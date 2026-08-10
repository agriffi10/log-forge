"""SPEC-009 — SaaS intake sinks: Datadog, Splunk HEC, New Relic, Honeycomb, Sentry (fakes)."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from log_foundry.sinks.base import Sink
from log_foundry.sinks.datadog import DatadogSink
from log_foundry.sinks.honeycomb import HoneycombSink
from log_foundry.sinks.newrelic import NewRelicSink
from log_foundry.sinks.sentry import SentrySink
from log_foundry.sinks.splunk import SplunkHECSink
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
    assert entry["ddsource"] == "log-foundry"
    assert entry["service"] == "payments"
    assert entry["ddtags"] == "env:prod"


def test_datadog_default_site() -> None:
    opener = FakeOpener()
    DatadogSink("k", opener=opener).emit([{"a": 1}])
    # Assert the whole URL, not a substring of it. `"datadoghq.com" in url` passes for
    # `https://evil.example/?x=datadoghq.com` too, which is why CodeQL flags the substring form
    # (py/incomplete-url-substring-sanitization) — and the exact string is what this test means.
    assert opener.calls[0]["url"] == "https://http-intake.logs.datadoghq.com/api/v2/logs"


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
    assert first["time"] == datetime(2026, 7, 11, tzinfo=UTC).timestamp()
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
    fake = FakeSentrySDK()
    sink = SentrySink(client=fake, min_level="ERROR")
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
    assert [e["level"] for e in fake.events] == ["error", "fatal"]
    assert fake.events[0]["extra"]["message"] == "boom"


def test_sentry_http_envelope_fallback_when_sdk_absent(monkeypatch) -> None:
    monkeypatch.setattr("log_foundry.sinks.sentry._import_sdk", lambda: None)
    opener = FakeOpener()
    sink = SentrySink(dsn="https://pubkey@o123.ingest.sentry.io/456", opener=opener)
    assert sink.client is None
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
    monkeypatch.setattr("log_foundry.sinks.sentry._import_sdk", lambda: None)
    with pytest.raises(ValueError):
        SentrySink()


# --- SPEC-038 FR-001: every platform sink chunks through HTTPSink.emit -------------------


def test_datadog_carries_its_documented_intake_limits() -> None:
    """AC-1/AC-5. Datadog publishes both figures, so both are the vendor's rather than ours."""
    assert (DatadogSink.MAX_BATCH_COUNT, DatadogSink.MAX_BATCH_BYTES) == (1000, 5_000_000)


def test_datadog_splits_a_batch_over_its_array_limit() -> None:
    """Before FR-001 this went as one array of 2,500 and the intake rejected all of it."""
    opener = FakeOpener()
    DatadogSink("key", opener=opener).emit([{"n": i} for i in range(2500)])
    counts = [len(json.loads(call["body"])) for call in opener.calls]
    assert counts == [1000, 1000, 500]
    assert all(entry["ddsource"] == "log-foundry" for entry in json.loads(opener.calls[0]["body"]))


def test_newrelic_splits_a_batch_over_its_one_megabyte_post_limit() -> None:
    opener = FakeOpener()
    NewRelicSink("key", opener=opener).emit([{"pad": "x" * 2000} for _ in range(1000)])
    assert len(opener.calls) > 1
    assert all(len(call["body"]) <= NewRelicSink.MAX_BATCH_BYTES for call in opener.calls)
    assert sum(len(json.loads(call["body"])) for call in opener.calls) == 1000


def test_honeycomb_splits_a_batch_and_keeps_its_data_envelope() -> None:
    opener = FakeOpener()
    HoneycombSink("key", "dataset", opener=opener).emit([{"pad": "x" * 2000} for _ in range(1000)])
    assert len(opener.calls) > 1
    assert all(len(call["body"]) <= HoneycombSink.MAX_BATCH_BYTES for call in opener.calls)
    entries = [entry for call in opener.calls for entry in json.loads(call["body"])]
    assert len(entries) == 1000
    assert all(set(entry) == {"data"} for entry in entries)


def test_splunk_splits_a_batch_and_keeps_concatenated_hec_envelopes() -> None:
    opener = FakeOpener()
    SplunkHECSink("http://splunk:8088", "tok", opener=opener).emit(
        [{"pad": "x" * 2000} for _ in range(1000)]
    )
    assert len(opener.calls) > 1
    decoder = json.JSONDecoder()
    total = 0
    for call in opener.calls:
        body = call["body"].decode("utf-8")
        index = 0
        while index < len(body):
            envelope, index = decoder.raw_decode(body, index)
            assert set(envelope) >= {"event", "time", "source"}
            total += 1
    assert total == 1000, "HEC stays concatenated JSON objects, one per event, across chunks"


def test_every_platform_sink_carries_the_limits_its_docstring_cites() -> None:
    """AC-1/AC-5. The constants are the contract; without this only Datadog's were pinned.

    Mutating `ElasticsearchSink.MAX_BATCH_BYTES` from 10 MB to 100 MB, `LokiSink`'s from 4 MB to
    400 MB, or `NewRelicSink`'s down to 100 KB all left the suite green, because the chunking
    tests pass explicit `max_batch_*` arguments and never exercise the class attributes. The
    literals here are transcribed from each docstring's cited figure, not read back from the
    class, so a constant that drifts from its citation fails.
    """
    from log_foundry.sinks.elasticsearch import ElasticsearchSink, OpenSearchSink
    from log_foundry.sinks.loki import LokiSink

    assert (DatadogSink.MAX_BATCH_COUNT, DatadogSink.MAX_BATCH_BYTES) == (1000, 5_000_000)
    assert DatadogSink.MAX_EVENT_BYTES == 1_000_000, "its documented single-log cap"
    assert (NewRelicSink.MAX_BATCH_COUNT, NewRelicSink.MAX_BATCH_BYTES) == (1000, 1_000_000)
    assert (HoneycombSink.MAX_BATCH_COUNT, HoneycombSink.MAX_BATCH_BYTES) == (1000, 1_000_000)
    assert (SplunkHECSink.MAX_BATCH_COUNT, SplunkHECSink.MAX_BATCH_BYTES) == (1000, 1_000_000)
    assert (LokiSink.MAX_BATCH_COUNT, LokiSink.MAX_BATCH_BYTES) == (1000, 4_000_000)
    assert (ElasticsearchSink.MAX_BATCH_COUNT, ElasticsearchSink.MAX_BATCH_BYTES) == (
        1000,
        10_000_000,
    )
    assert OpenSearchSink.MAX_BATCH_BYTES == ElasticsearchSink.MAX_BATCH_BYTES, (
        "OpenSearch reuses the bulk protocol verbatim, limits included"
    )


def test_datadog_drops_an_event_over_its_single_log_cap_though_the_payload_would_fit() -> None:
    """The one sink whose per-event limit is stricter than its request limit.

    A 2 MB event sits inside the 5 MB payload budget and is rejected by the 1 MB per-log cap —
    a limit the request budget cannot see, so before `MAX_EVENT_BYTES` it went on the wire.
    """
    opener = FakeOpener()
    sink = DatadogSink("key", opener=opener)
    sink.emit([{"a": 1}, {"pad": "x" * 2_000_000}, {"b": 2}])
    assert sink.dropped_oversized == 1
    entries = [entry for call in opener.calls for entry in json.loads(call["body"])]
    assert len(entries) == 2, "the two ordinary events still ship"
