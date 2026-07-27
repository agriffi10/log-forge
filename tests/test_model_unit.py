"""SPEC-001 FR-003 — model unit coverage without the `lf` fixture.

The pre-written `test_model.py` uses the shared `lf` fixture, which gates on `log_foundry.info`
(a SPEC-002 feature), so it stays skipped through SPEC-001. These tests exercise `model.py`
directly against a `FakeSink`-configured process so the schema, precedence, and end-event
fields are verified now.
"""

import re

import pytest

config = pytest.importorskip("log_foundry.config")
model = pytest.importorskip("log_foundry.model")

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


# -- SPEC-017 FR-003: the exception message as a queryable field -------------------------


def _end_error(exc: BaseException) -> dict:
    """The `error` sub-document from an end event built for `exc`."""
    return model.end_event(_span(), "error", exc)["error"]


def _raised(exc_type, *args):
    """Raise and catch `exc_type(*args)` so the exception carries a real traceback."""
    try:
        raise exc_type(*args)
    except BaseException as caught:  # noqa: BLE001 — the point is to capture it
        return caught


def test_error_carries_a_queryable_message() -> None:
    err = _end_error(_raised(ValueError, "bad input"))
    assert err["message"] == "bad input"
    assert err["type"] == "ValueError"


def test_error_carries_the_defining_module() -> None:
    class Custom(Exception):
        pass

    assert _end_error(_raised(ValueError, "x"))["module"] == "builtins"
    assert _end_error(_raised(Custom, "x"))["module"] == Custom.__module__


def test_error_message_is_empty_string_when_raised_with_no_arguments() -> None:
    err = _end_error(_raised(ValueError))
    assert err["message"] == ""  # not missing, not None


def test_error_message_survives_a_raising_dunder_str() -> None:
    class Hostile(Exception):
        def __str__(self) -> str:
            raise RuntimeError("nope")

    err = _end_error(_raised(Hostile))
    assert err["message"] == "<unprintable message>"
    assert err["type"] == "Hostile"


def test_error_type_and_stack_are_unchanged_for_an_untruncated_exception() -> None:
    """Regression guard: existing consumers indexing `type`/`stack` must see the same bytes."""
    import traceback

    exc = _raised(ValueError, "bad input")
    expected = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    err = _end_error(exc)
    assert err["type"] == "ValueError"
    assert err["stack"] == expected


def test_stack_uses_its_own_larger_ceiling() -> None:
    config.configure(max_value_bytes=64, max_stack_bytes=32768)
    exc = _raised(ValueError, "x" * 500)
    err = _end_error(exc)
    # The stack comfortably exceeds max_value_bytes and must NOT be clipped to it.
    assert len(err["stack"].encode()) > 64
    # ...while the message, an ordinary str value, is bounded.
    assert len(err["message"].encode()) <= 64


def test_stack_truncation_keeps_the_tail() -> None:
    config.configure(max_stack_bytes=120)
    exc = _raised(ValueError, "distinctive-tail-marker")
    err = _end_error(exc)
    assert len(err["stack"].encode()) <= 120
    assert err["stack"].rstrip().endswith("distinctive-tail-marker")


# -- SPEC-017 FR-002: the truncated marker ------------------------------------------------


def test_truncated_marker_absent_on_a_clean_event() -> None:
    event = model.build_event(_span(), "INFO", "m", fields={"a": 1}, baggage={})
    assert "truncated" not in event  # absent, not False


def test_truncated_marker_set_when_a_field_is_clipped() -> None:
    config.configure(max_value_bytes=16)
    event = model.build_event(_span(), "INFO", "m", fields={"a": "x" * 500}, baggage={})
    assert event["truncated"] is True


def test_truncated_marker_set_when_the_message_is_clipped() -> None:
    """`message` is caller-supplied free text, so it is bounded like any other string."""
    config.configure(max_value_bytes=32)
    event = model.build_event(_span(), "INFO", "y" * 500, fields={}, baggage={})
    assert event["truncated"] is True
    assert len(event["message"].encode()) <= 32


def test_library_generated_base_fields_are_never_truncated() -> None:
    config.configure(max_value_bytes=1)
    event = model.build_event(_span(), "INFO", "m", fields={}, baggage={})
    assert event["trace_id"] == "a" * 32
    assert event["span_id"] == "b" * 16
    assert ISO_MS_Z.match(event["timestamp"])
    assert event["level"] == "INFO"


def test_base_field_set_is_unchanged_apart_from_the_optional_marker() -> None:
    event = model.build_event(_span(), "INFO", "m", fields={}, baggage={})
    assert tuple(event) == BASE_FIELDS


def test_function_is_bounded_like_message() -> None:
    """`function` is caller-supplied too — via @trace(name=...) and, on the orphan path, from
    the message itself — so leaving it out kept info(huge_string) unbounded."""
    config.configure(max_value_bytes=32)
    span = model.Span(
        trace_id="a" * 32, span_id="b" * 16, parent_span_id=None, name="f" * 500, start_ts=0.0
    )
    event = model.build_event(span, "INFO", "m", fields={}, baggage={})
    assert len(event["function"].encode()) <= 32
    assert event["truncated"] is True
