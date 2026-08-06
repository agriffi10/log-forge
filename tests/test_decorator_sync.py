"""SPEC-001 FR-006/FR-007 — sync @trace lifecycle without the logging API.

The pre-written `test_decorator.py` uses `lf.info`/`lf.set_baggage` (SPEC-002), so it stays
skipped through SPEC-001. These tests drive the decorator against a `FakeSink` directly and
assert on the span-boundary events, exercising FR-006 (lifecycle, hierarchy, non-swallowing)
and FR-007 (the `trace` façade export) now.
"""

import contextvars
import time

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


# -- SPEC-025 FR-001/FR-002: a logging fault never becomes the caller's --------------------
#
# These drive the decorator with a *broken close* rather than a broken sink, because the sink
# is reached through the worker thread, which already absorbs its own failures (SPEC-019). The
# faults this spec closes are the ones on the caller's own thread: `_close_span` itself.


@pytest.fixture
def broken_close(monkeypatch):
    """Make `_close_span` raise, and hand back the list of what it was called with."""
    decorator = pytest.importorskip("log_foundry.decorator")
    calls: list[tuple[str, object]] = []

    def _raise(span, status, exc, _real=decorator._close_span):
        calls.append((status, exc))
        raise RuntimeError("closing blew up")

    monkeypatch.setattr(decorator, "_close_span", _raise)
    return calls


def test_a_successful_call_survives_a_broken_close(broken_close, capsys) -> None:
    log_foundry.configure(service="t")

    @log_foundry.trace(name="work")
    def work() -> int:
        return 42

    assert contextvars.copy_context().run(work) == 42, "the caller's own result must survive"
    assert broken_close == [("ok", None)], "closed exactly once, with the real outcome"
    err = capsys.readouterr().err
    assert "log-foundry: absorbed a failure while closing a span (RuntimeError)" in err
    assert "the span's events were lost" in err
    assert "closing blew up" not in err, "the message is never written (arch §6)"


def test_a_failing_call_propagates_its_own_exception(broken_close) -> None:
    log_foundry.configure(service="t")

    @log_foundry.trace(name="boom")
    def boom() -> None:
        raise ValueError("the caller's own error")

    with pytest.raises(ValueError, match="the caller's own error") as caught:
        contextvars.copy_context().run(boom)

    assert caught.value.__cause__ is None, "the close fault must not be chained onto it"
    assert caught.value.__context__ is None
    assert broken_close == [("error", caught.value)]


def test_the_close_runs_exactly_once_per_span(fake_sink) -> None:
    """FR-002: the old shape closed in the `try` *and* the `except`, so a close that failed on
    the success path emitted a second, contradicting `span.end` for a call that had returned."""
    log_foundry.configure(service="t", sink=fake_sink)
    decorator = pytest.importorskip("log_foundry.decorator")
    flushed: list[list[str]] = []

    def flush_then_fail(span):
        flushed.append([e.get("message") for e in span.events])
        raise RuntimeError("flush exploded")

    decorator_flush = decorator._flush
    decorator._flush = flush_then_fail
    try:
        @log_foundry.trace(name="work")
        def work() -> int:
            return 42

        assert contextvars.copy_context().run(work) == 42
    finally:
        decorator._flush = decorator_flush

    assert len(flushed) == 1, "one close attempt, not a second from the except branch"
    assert flushed[0].count("span.end") == 1, "and one end event in it"


def test_one_end_event_on_each_path(fake_sink) -> None:
    log_foundry.configure(service="t", sink=fake_sink)

    @log_foundry.trace(name="ok_call")
    def ok_call() -> int:
        return 1

    @log_foundry.trace(name="bad_call")
    def bad_call() -> None:
        raise ValueError("no")

    contextvars.copy_context().run(ok_call)
    with pytest.raises(ValueError):
        contextvars.copy_context().run(bad_call)
    log_foundry.shutdown()

    ends = [e for e in fake_sink.events if e["message"] == "span.end"]
    by_fn = {e["function"]: e for e in ends}
    assert len(ends) == 2, "exactly one end event per span"
    assert by_fn["ok_call"]["status"] == "ok"
    assert "error" not in by_fn["ok_call"]
    assert by_fn["bad_call"]["status"] == "error"
    assert by_fn["bad_call"]["error"]["type"] == "ValueError"


def test_a_keyboardinterrupt_from_the_close_still_reaches_the_caller(monkeypatch) -> None:
    """FR-001: the guard catches Exception, never BaseException."""
    log_foundry.configure(service="t")
    decorator = pytest.importorskip("log_foundry.decorator")

    def _interrupt(span, status, exc):
        raise KeyboardInterrupt

    monkeypatch.setattr(decorator, "_close_span", _interrupt)

    @log_foundry.trace(name="work")
    def work() -> int:
        return 42

    with pytest.raises(KeyboardInterrupt):
        contextvars.copy_context().run(work)


def test_a_call_still_runs_when_the_span_cannot_be_opened(monkeypatch, capsys) -> None:
    """Hardening the pre-body setup: a fault there would stop the app doing its work at all."""
    log_foundry.configure(service="t")
    decorator = pytest.importorskip("log_foundry.decorator")

    def _no_entropy(name, defaults):
        raise OSError("no entropy available")

    monkeypatch.setattr(decorator, "_open_span", _no_entropy)

    @log_foundry.trace(name="work")
    def work() -> int:
        return 42

    assert contextvars.copy_context().run(work) == 42
    err = capsys.readouterr().err
    assert "absorbed a failure while opening a span (OSError)" in err
    assert "this call runs untraced" in err


def test_an_untraced_call_still_propagates_its_own_exception(monkeypatch) -> None:
    log_foundry.configure(service="t")
    decorator = pytest.importorskip("log_foundry.decorator")
    monkeypatch.setattr(
        decorator, "_open_span", lambda name, defaults: (_ for _ in ()).throw(OSError("nope"))
    )

    @log_foundry.trace(name="boom")
    def boom() -> None:
        raise ValueError("mine")

    with pytest.raises(ValueError, match="mine"):
        contextvars.copy_context().run(boom)


def test_the_span_stack_is_left_clean_after_a_broken_close(broken_close) -> None:
    """`pop_span` runs after the guarded close, so the stack unwinds even when the close fails."""
    log_foundry.configure(service="t")
    context = pytest.importorskip("log_foundry.context")

    @log_foundry.trace(name="work")
    def work() -> int:
        return 42

    def body() -> None:
        work()
        assert context.current_span() is None, "the stack must not leak a span"

    contextvars.copy_context().run(body)


def test_duration_is_measured_before_the_flush(fake_sink, monkeypatch) -> None:
    """FR-002: `duration_ms` covers the function body, not the flush that follows it."""
    log_foundry.configure(service="t", sink=fake_sink)
    decorator = pytest.importorskip("log_foundry.decorator")
    real_flush = decorator._flush

    def slow_flush(span):
        time.sleep(0.05)
        real_flush(span)

    monkeypatch.setattr(decorator, "_flush", slow_flush)

    @log_foundry.trace(name="quick")
    def quick() -> None:
        return None

    contextvars.copy_context().run(quick)
    log_foundry.shutdown()

    end = next(e for e in fake_sink.events if e["message"] == "span.end")
    assert end["duration_ms"] < 50, "the 50 ms flush must not be inside the measurement"
