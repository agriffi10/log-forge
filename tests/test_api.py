"""SPEC-002 — level logging API: append-to-span, precedence, baggage, orphan path.

These assert the contract (level, fields, trace/span linkage), not the auto-generated
span-boundary wording. The `lf` fixture configures a FakeSink; where a test needs the
emitted events without a decorated call (orphan path), it asserts on the sink directly.
"""

import contextvars

import pytest

api = pytest.importorskip("log_foundry.api")

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


# -- SPEC-017 FR-001: the orphan path must not raise into the caller ----------------------


def test_orphan_log_with_an_unserializable_field_does_not_raise(lf, fake_sink) -> None:
    """The headline criterion: `api._log` emits synchronously on the caller's own thread when
    no span is active, so a serialization failure there lands in the user's stack frame."""
    from datetime import datetime

    lf.info("m", when=datetime(2026, 1, 1))  # no active span — must return normally

    event = fake_sink.events[-1]
    assert event["fields"]["when"] == "2026-01-01T00:00:00"


def test_orphan_log_coerces_the_documented_types(lf, fake_sink) -> None:
    from decimal import Decimal
    from uuid import UUID

    lf.info(
        "m",
        oid=UUID("12345678-1234-5678-1234-567812345678"),
        amount=Decimal("1.10"),
        raw=b"\xff",
        tags={"b", "a"},
    )

    fields = fake_sink.events[-1]["fields"]
    assert fields["oid"] == "12345678-1234-5678-1234-567812345678"
    assert fields["amount"] == "1.10"
    assert isinstance(fields["raw"], str)
    assert sorted(fields["tags"]) == ["a", "b"]


def test_orphan_log_with_a_domain_object_keeps_its_other_fields(lf, fake_sink) -> None:
    class MyClass:
        pass

    lf.info("m", bad=MyClass(), good="kept")

    fields = fake_sink.events[-1]["fields"]
    assert fields["bad"] == "<unserializable: MyClass>"
    assert fields["good"] == "kept"


def test_every_emitted_event_is_json_serializable(lf, fake_sink) -> None:
    import json
    from datetime import datetime

    @lf.trace(name="work")
    def work() -> None:
        lf.info("inside", at=datetime(2026, 1, 1))

    work()
    for event in fake_sink.events:
        json.dumps(event)  # must not raise for any event the pipeline produced


# -- SPEC-020 FR-004: an over-long int must not raise into the caller either --------------

# Past CPython's default int->str conversion limit, where json.dumps refuses to render.
_HUGE = 10**5000


def test_orphan_log_with_an_over_long_int_does_not_raise(lf, fake_sink) -> None:
    """The same failure SPEC-017 fixed for unserializable objects, reached through a number.

    Before SPEC-020 this raised ValueError in the caller's own stack frame: the orphan path
    emits synchronously, and json.dumps refuses an int past sys.get_int_max_str_digits().
    """
    import json

    lf.info("m", n=_HUGE, ok=7)  # no active span — must return normally

    event = fake_sink.batches[-1][-1]
    json.dumps(event)  # the guarantee: every sink can serialize what it is handed
    assert event["fields"]["ok"] == 7, "the sound fields survive alongside the elided one"
    assert event["fields"]["n"].startswith("<int: ~")
    assert event["truncated"] is True


def test_an_over_long_int_inside_a_span_does_not_destroy_its_batch(lf, fake_sink) -> None:
    """Inside a span the value reached the sink, json.dumps raised, and the retry loop
    abandoned the whole flattened batch — taking co-batched events from other spans."""
    import json

    @lf.trace(name="poisoned")
    def poisoned() -> None:
        lf.info("big", n=_HUGE)

    @lf.trace(name="innocent")
    def innocent() -> None:
        lf.info("small", n=1)

    contextvars.copy_context().run(poisoned)
    contextvars.copy_context().run(innocent)

    batch = [event for events in fake_sink.batches for event in events]
    json.dumps(batch)  # the flattened batch the worker builds — must serialize whole

    assert any(e["message"] == "small" for e in batch), "the unrelated span survives intact"
    assert any(e["message"] == "big" for e in batch)
