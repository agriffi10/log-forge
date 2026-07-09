"""SPEC-001 FR-003 — model unit coverage without the `lf` fixture.

The pre-written `test_model.py` uses the shared `lf` fixture, which gates on `log_forge.info`
(a SPEC-002 feature), so it stays skipped through SPEC-001. These tests exercise `model.py`
directly against a `FakeSink`-configured process so the schema, precedence, and end-event
fields are verified now.
"""

import re

import pytest

config = pytest.importorskip("log_forge.config")
model = pytest.importorskip("log_forge.model")

BASE_FIELDS = (
    "timestamp", "level", "message", "trace_id", "span_id", "parent_span_id",
    "log_id", "function", "service", "version", "env", "fields",
)
ISO_MS_Z = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")


def _span():
    return model.Span(
        trace_id="a" * 32, span_id="b" * 16, parent_span_id=None, name="fn", start_ts=0.0
    )


def test_build_event_has_all_base_fields_and_iso_timestamp(fake_sink) -> None:
    config.configure(service="pay", version="1.2", env="prod", sink=fake_sink)
    event = model.build_event(_span(), "INFO", "hi", fields={"user_id": 7}, baggage={})
    for key in BASE_FIELDS:
        assert key in event, f"missing base field: {key}"
    assert event["service"] == "pay"
    assert event["version"] == "1.2"
    assert event["env"] == "prod"
    assert ISO_MS_Z.match(event["timestamp"]), event["timestamp"]
    assert event["fields"]["user_id"] == 7


def test_field_precedence_defaults_then_baggage_then_fields(fake_sink) -> None:
    config.configure(service="t", sink=fake_sink, defaults={"k": "from_config", "team": "x"})
    span = _span()
    span.defaults = {"k": "from_span"}
    event = model.build_event(
        span, "INFO", "m", fields={"k": "from_fields"}, baggage={"k": "from_baggage"}
    )
    assert event["fields"]["k"] == "from_fields"   # per-call fields win
    assert event["fields"]["team"] == "x"          # lower-precedence config default survives


def test_end_event_carries_status_duration_and_error(fake_sink) -> None:
    config.configure(service="t", sink=fake_sink)
    ok = model.end_event(_span(), "ok")
    assert ok["status"] == "ok"
    assert isinstance(ok["duration_ms"], float)
    assert "error" not in ok

    try:
        raise ValueError("boom")
    except ValueError as exc:
        err = model.end_event(_span(), "error", exc)
    assert err["status"] == "error"
    assert err["error"]["type"] == "ValueError"
    assert "ValueError: boom" in err["error"]["stack"]


def test_duration_ms_is_never_negative(fake_sink) -> None:
    import time

    config.configure(service="t", sink=fake_sink)
    span = model.Span(
        trace_id="a" * 32, span_id="b" * 16, parent_span_id=None,
        name="fn", start_ts=time.monotonic(),
    )
    assert model.end_event(span, "ok")["duration_ms"] >= 0.0
