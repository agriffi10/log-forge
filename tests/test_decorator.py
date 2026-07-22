"""Phases 6-7 — @trace (sync) + logging API (arch §4, §6).

These assert behavior, not exact wording of the auto-generated span boundary events, so
they won't break if you rename "span.start"/"span.end". The end event is identified by
its `status` field (arch §6), which is part of the contract.
"""

import contextvars

import pytest


def test_single_span_records_status_ok_and_user_log(lf, fake_sink) -> None:
    @lf.trace(name="work")
    def work() -> int:
        lf.info("doing work", item=1)
        return 42

    assert contextvars.copy_context().run(work) == 42

    events = fake_sink.events
    assert any(e["message"] == "doing work" for e in events)
    # exactly one end event, marked ok
    assert sum(1 for e in events if e.get("status") == "ok") == 1
    # everything in this call shares one trace and one span
    assert len({e["trace_id"] for e in events}) == 1
    assert len({e["span_id"] for e in events}) == 1


def test_exception_is_recorded_then_reraised(lf, fake_sink) -> None:
    @lf.trace(name="boom")
    def boom() -> None:
        raise ValueError("nope")

    with pytest.raises(ValueError, match="nope"):
        contextvars.copy_context().run(boom)

    assert any(e.get("status") == "error" for e in fake_sink.events)


def test_nested_calls_share_trace_and_link_parent(lf, fake_sink) -> None:
    @lf.trace(name="child")
    def child() -> None:
        lf.info("child work")

    @lf.trace(name="parent")
    def parent() -> None:
        lf.info("parent work")
        child()

    contextvars.copy_context().run(parent)

    events = fake_sink.events
    assert len({e["trace_id"] for e in events}) == 1, "nested calls must share one trace"

    parent_span_id = next(e["span_id"] for e in events if e["function"] == "parent")
    child_events = [e for e in events if e["function"] == "child"]
    assert child_events
    assert all(e["parent_span_id"] == parent_span_id for e in child_events)
    # the root span has no parent
    assert all(e["parent_span_id"] is None
               for e in events if e["function"] == "parent")


def test_baggage_flows_to_descendant_logs(lf, fake_sink) -> None:
    @lf.trace(name="child")
    def child() -> None:
        lf.info("child work")

    @lf.trace(name="parent")
    def parent() -> None:
        lf.set_baggage(request_id="r-123")
        child()

    if not hasattr(lf, "set_baggage"):
        pytest.skip("log_foundry.set_baggage not implemented yet")

    contextvars.copy_context().run(parent)

    # Baggage rides user *log* events (SPEC-002 FR-003), not the decorator's span.start/end
    # boundary events — SPEC-002 leaves those unchanged (SPEC-001, boundary baggage is empty).
    child_logs = [
        e for e in fake_sink.events if e["function"] == "child" and e["message"] == "child work"
    ]
    assert child_logs
    assert all(e["fields"].get("request_id") == "r-123" for e in child_logs)
