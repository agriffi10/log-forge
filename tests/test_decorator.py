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

    contextvars.copy_context().run(parent)

    # Baggage rides user *log* events (SPEC-002 FR-003), not the decorator's span.start/end
    # boundary events — SPEC-002 leaves those unchanged (SPEC-001, boundary baggage is empty).
    child_logs = [
        e for e in fake_sink.events if e["function"] == "child" and e["message"] == "child work"
    ]
    assert child_logs
    assert all(e["fields"].get("request_id") == "r-123" for e in child_logs)


# -- SPEC-024 FR-001: baggage is scoped to the root span --------------------------------
#
# These deliberately run two root spans inside *one* `copy_context()`, which is the case the
# suite never covered before: a long-lived process serving sequential requests on one thread.


def _events_for(fake_sink, function: str) -> list[dict]:
    return [e for e in fake_sink.events if e["function"] == function]


def test_baggage_does_not_leak_into_a_later_root_span(lf, fake_sink) -> None:
    @lf.trace(name="alice")
    def alice() -> None:
        lf.set_baggage(user_id="alice")
        lf.info("serving")

    @lf.trace(name="bob")
    def bob() -> None:
        lf.info("serving")

    def body() -> None:
        alice()
        bob()

    contextvars.copy_context().run(body)

    alice_events = _events_for(fake_sink, "alice")
    bob_events = _events_for(fake_sink, "bob")
    assert alice_events and bob_events
    assert any(e["fields"].get("user_id") == "alice" for e in alice_events)
    assert all("user_id" not in e["fields"] for e in bob_events), (
        "one request's baggage must not appear on the next request's events"
    )


def test_baggage_from_a_nested_call_reaches_the_parent_and_later_siblings(lf, fake_sink) -> None:
    @lf.trace(name="setter")
    def setter() -> None:
        lf.set_baggage(tenant="acme")

    @lf.trace(name="sibling")
    def sibling() -> None:
        lf.info("after")

    @lf.trace(name="parent")
    def parent() -> None:
        setter()
        sibling()

    contextvars.copy_context().run(parent)

    sibling_logs = [e for e in _events_for(fake_sink, "sibling") if e["message"] == "after"]
    assert sibling_logs
    assert all(e["fields"].get("tenant") == "acme" for e in sibling_logs), (
        "a nested span must not reset — 'at or below' is the whole point of baggage"
    )
    parent_end = next(e for e in _events_for(fake_sink, "parent") if e.get("status") == "ok")
    assert parent_end["fields"].get("tenant") == "acme"


def test_baggage_set_before_any_span_survives_the_span(lf, fake_sink) -> None:
    from log_foundry import context as context_mod

    @lf.trace(name="work")
    def work() -> None:
        lf.set_baggage(request="r1")
        lf.info("inside")

    def body() -> None:
        lf.set_baggage(process="p1")
        work()
        # the scope restores the process-level default rather than erasing it
        assert context_mod.get_baggage() == {"process": "p1"}

    contextvars.copy_context().run(body)

    inside = [e for e in fake_sink.events if e["message"] == "inside"]
    assert inside and all(e["fields"].get("process") == "p1" for e in inside)


def test_baggage_is_restored_when_the_root_span_raises(lf, fake_sink) -> None:
    @lf.trace(name="boom")
    def boom() -> None:
        lf.set_baggage(user_id="alice")
        raise ValueError("nope")

    @lf.trace(name="after")
    def after() -> None:
        lf.info("clean")

    def body() -> None:
        with pytest.raises(ValueError, match="nope"):
            boom()
        after()

    contextvars.copy_context().run(body)

    after_events = _events_for(fake_sink, "after")
    assert after_events
    assert all("user_id" not in e["fields"] for e in after_events)


def test_the_boundary_backfill_still_sees_the_spans_final_baggage(lf, fake_sink) -> None:
    """SPEC-015: `_close_span` runs inside the `try`, so the reset cannot pre-empt it."""

    @lf.trace(name="work")
    def work() -> None:
        lf.set_baggage(tenant="acme")

    contextvars.copy_context().run(work)

    boundary = [e for e in _events_for(fake_sink, "work") if e["message"].startswith("span.")]
    assert len(boundary) == 2
    assert all(e["fields"].get("tenant") == "acme" for e in boundary)
