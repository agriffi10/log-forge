"""SPEC-001 FR-006/FR-007 — sync @trace lifecycle without the logging API.

The pre-written `test_decorator.py` uses `lf.info`/`lf.set_baggage` (SPEC-002), so it stays
skipped through SPEC-001. These tests drive the decorator against a `FakeSink` directly and
assert on the span-boundary events, exercising FR-006 (lifecycle, hierarchy, non-swallowing)
and FR-007 (the `trace` façade export) now.
"""

import contextvars

import pytest

log_foundry = pytest.importorskip("log_foundry")

pytestmark = pytest.mark.skipif(
    not (hasattr(log_foundry, "trace") and hasattr(log_foundry, "configure")),
    reason="log_foundry.trace / configure not implemented yet",
)


def test_single_span_records_start_end_ok(fake_sink) -> None:
    log_foundry.configure(service="t", sink=fake_sink)

    @log_foundry.trace(name="work")
    def work() -> int:
        return 42

    assert contextvars.copy_context().run(work) == 42
    log_foundry.shutdown()  # drain the background worker before asserting (SPEC-004)
    events = fake_sink.events
    assert any(e["function"] == "work" for e in events)
    assert sum(1 for e in events if e.get("status") == "ok") == 1
    assert len({e["trace_id"] for e in events}) == 1
    assert len({e["span_id"] for e in events}) == 1


def test_exception_is_recorded_then_reraised(fake_sink) -> None:
    log_foundry.configure(service="t", sink=fake_sink)

    @log_foundry.trace
    def boom() -> None:
        raise ValueError("nope")

    with pytest.raises(ValueError, match="nope"):
        contextvars.copy_context().run(boom)

    log_foundry.shutdown()  # drain the background worker before asserting (SPEC-004)
    errors = [e for e in fake_sink.events if e.get("status") == "error"]
    assert errors, "an error end event must be recorded"
    assert errors[0]["error"]["type"] == "ValueError"


def test_bare_and_parameterized_forms_both_work(fake_sink) -> None:
    log_foundry.configure(service="t", sink=fake_sink)

    @log_foundry.trace
    def bare() -> str:
        return "b"

    @log_foundry.trace(name="custom")
    def param() -> str:
        return "p"

    contextvars.copy_context().run(bare)
    contextvars.copy_context().run(param)
    log_foundry.shutdown()  # drain the background worker before asserting (SPEC-004)
    funcs = {e["function"] for e in fake_sink.events}
    assert "custom" in funcs  # name= override
    assert any(f.endswith("bare") for f in funcs)  # __qualname__ default


def test_nested_calls_share_trace_and_link_parent(fake_sink) -> None:
    log_foundry.configure(service="t", sink=fake_sink)

    @log_foundry.trace(name="child")
    def child() -> None:
        return None

    @log_foundry.trace(name="parent")
    def parent() -> None:
        child()

    contextvars.copy_context().run(parent)
    log_foundry.shutdown()  # drain the background worker before asserting (SPEC-004)
    events = fake_sink.events
    assert len({e["trace_id"] for e in events}) == 1, "nested calls share one trace"

    parent_span_id = next(e["span_id"] for e in events if e["function"] == "parent")
    child_events = [e for e in events if e["function"] == "child"]
    assert child_events
    assert all(e["parent_span_id"] == parent_span_id for e in child_events)
    assert all(e["parent_span_id"] is None for e in events if e["function"] == "parent")


def test_end_event_appended_before_flush(fake_sink) -> None:
    """The flushed batch must be complete — start and end both present (FR-006)."""
    log_foundry.configure(service="t", sink=fake_sink)

    @log_foundry.trace(name="w")
    def w() -> int:
        return 1

    contextvars.copy_context().run(w)
    log_foundry.shutdown()  # drain: the one span's two events flush together in one batch
    assert len(fake_sink.batches) == 1
    assert len(fake_sink.batches[0]) == 2
    assert fake_sink.batches[0][-1].get("status") == "ok"
