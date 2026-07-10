"""SPEC-002 — level logging API: append-to-span, precedence, baggage, orphan path.

These assert the contract (level, fields, trace/span linkage), not the auto-generated
span-boundary wording. The `lf` fixture configures a FakeSink; where a test needs the
emitted events without a decorated call (orphan path), it asserts on the sink directly.
"""

import contextvars

import pytest

api = pytest.importorskip("log_forge.api")

LEVELS = ["debug", "info", "warning", "error", "critical"]


def test_level_appends_one_event_with_uppercase_level(lf, fake_sink) -> None:
    @lf.trace(name="work")
    def work() -> None:
        for name in LEVELS:
            getattr(lf, name)(f"{name} line")

    contextvars.copy_context().run(work)

    events = fake_sink.events
    for name in LEVELS:
        matches = [e for e in events if e["message"] == f"{name} line"]
        assert len(matches) == 1, f"exactly one {name} event expected"
        assert matches[0]["level"] == name.upper()


def test_event_carries_span_identity_and_unique_log_id(lf, fake_sink) -> None:
    @lf.trace(name="work")
    def work() -> None:
        lf.info("a")
        lf.info("b")

    contextvars.copy_context().run(work)

    user_events = [e for e in fake_sink.events if e["message"] in ("a", "b")]
    assert len({e["trace_id"] for e in user_events}) == 1
    assert len({e["span_id"] for e in user_events}) == 1
    # each event gets its own log_id
    assert len({e["log_id"] for e in user_events}) == 2


def test_field_precedence_call_fields_win_over_baggage_and_defaults(lf, fake_sink) -> None:
    lf.configure(defaults={"k": "config"})

    @lf.trace(name="work", defaults={"k": "span"})
    def work() -> None:
        lf.set_baggage(k="baggage")
        lf.info("precedence", k="call")

    contextvars.copy_context().run(work)

    event = next(e for e in fake_sink.events if e["message"] == "precedence")
    assert event["fields"]["k"] == "call"


def test_baggage_rides_every_subsequent_event(lf, fake_sink) -> None:
    @lf.trace(name="work")
    def work() -> None:
        lf.set_baggage(request_id="req-123")
        lf.info("first")
        lf.info("second")

    contextvars.copy_context().run(work)

    user_events = [e for e in fake_sink.events if e["message"] in ("first", "second")]
    assert user_events
    assert all(e["fields"]["request_id"] == "req-123" for e in user_events)


def test_orphan_log_emits_standalone_span_flushed_directly(lf, fake_sink) -> None:
    # No active span: a level call must still be recorded, not dropped (FR-004).
    lf.warning("orphaned", code=7)

    events = fake_sink.events
    assert len(events) == 1
    event = events[0]
    assert event["level"] == "WARNING"
    assert event["message"] == "orphaned"
    assert event["parent_span_id"] is None
    assert event["fields"]["code"] == 7
    assert event["trace_id"] and event["span_id"]


def test_orphan_logs_get_distinct_traces(lf, fake_sink) -> None:
    lf.info("one")
    lf.info("two")

    traces = {e["trace_id"] for e in fake_sink.events}
    assert len(traces) == 2, "each orphan log starts its own trace"
