"""SPEC-001 FR-006/FR-007 — sync @trace lifecycle without the logging API.

These tests drive the decorator against a `FakeSink` directly and assert on the span-boundary
events, exercising FR-006 (lifecycle, hierarchy, non-swallowing) and FR-007 (the `trace`
façade export).
"""

import contextvars
import gc
import threading
import time

import pytest

import log_foundry
from log_foundry.sinks import stdout as stdout_sink


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
    from log_foundry import decorator
    calls: list[tuple[str, object]] = []

    def _raise(span, status, exc):
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


def test_the_close_runs_exactly_once_per_span(fake_sink, monkeypatch) -> None:
    """FR-002: the old shape closed in the `try` *and* the `except`, so a close that failed on
    the success path emitted a second, contradicting `span.end` for a call that had returned."""
    log_foundry.configure(service="t", sink=fake_sink)
    from log_foundry import decorator
    flushed: list[list[str]] = []

    def flush_then_fail(span):
        flushed.append([e.get("message") for e in span.events])
        raise RuntimeError("flush exploded")

    monkeypatch.setattr(decorator, "_flush", flush_then_fail)

    @log_foundry.trace(name="work")
    def work() -> int:
        return 42

    assert contextvars.copy_context().run(work) == 42
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
    from log_foundry import decorator

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
    from log_foundry import decorator

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
    from log_foundry import decorator
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
    from log_foundry import context

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
    from log_foundry import decorator
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


def test_the_setup_guard_lets_a_keyboardinterrupt_through(monkeypatch) -> None:
    """FR-001: `_begin` catches Exception, never BaseException — as `_end` does."""
    log_foundry.configure(service="t")
    from log_foundry import decorator

    def _interrupt(name, defaults):
        raise KeyboardInterrupt

    monkeypatch.setattr(decorator, "_open_span", _interrupt)

    @log_foundry.trace(name="work")
    def work() -> int:
        return 42

    with pytest.raises(KeyboardInterrupt):
        contextvars.copy_context().run(work)


def test_a_span_that_could_not_be_pushed_is_still_closed_and_flushed(
    fake_sink, monkeypatch, capsys
) -> None:
    """Partial setup is kept, not unwound — the span exists, so its events are worth having."""
    log_foundry.configure(service="t", sink=fake_sink)
    from log_foundry import context
    monkeypatch.setattr(
        context, "push_span", lambda span: (_ for _ in ()).throw(RuntimeError("no push"))
    )

    @log_foundry.trace(name="work")
    def work() -> int:
        return 42

    assert contextvars.copy_context().run(work) == 42
    log_foundry.shutdown()
    assert [e["message"] for e in fake_sink.events] == ["span.start", "span.end"]
    assert "this call is traced incompletely" in capsys.readouterr().err


def test_an_untraced_root_call_still_releases_its_baggage_scope(fake_sink, monkeypatch) -> None:
    """Why the scope is taken *first*: it outlives a span that could not be opened.

    Taken last, this state would be "span kept, scope lost" — a traced call that leaks its
    baggage into the next request, which is the SPEC-024 defect reappearing through a failure
    path. Taken first, the scope is still there to release even though nothing was traced.
    """
    log_foundry.configure(service="t", sink=fake_sink)
    from log_foundry import context, decorator
    monkeypatch.setattr(
        decorator, "_open_span", lambda name, defaults: (_ for _ in ()).throw(OSError("nope"))
    )

    @log_foundry.trace(name="work")
    def work() -> None:
        context.set_baggage(request_id="r1")

    def body() -> None:
        work()
        assert context.get_baggage() == {}, "the scope survived the failed span and released"

    contextvars.copy_context().run(body)


def test_a_root_whose_scope_fails_is_not_traced_at_all(fake_sink, monkeypatch, capsys) -> None:
    """The other half of the same trade: no scope means no span, rather than span-without-scope.

    Nothing can restore baggage once the scope itself is unavailable, so the choice is between a
    traced call that leaks and an untraced one that leaks. Untraced is the honest one — it does
    not also claim, in the log stream, to be a well-formed span.
    """
    log_foundry.configure(service="t", sink=fake_sink)
    from log_foundry import context
    monkeypatch.setattr(
        context, "push_baggage_scope", lambda: (_ for _ in ()).throw(RuntimeError("no scope"))
    )

    @log_foundry.trace(name="work")
    def work() -> int:
        return 42

    assert contextvars.copy_context().run(work) == 42
    log_foundry.shutdown()
    assert fake_sink.events == [], "no half-formed span reaches the sink"
    assert "this call runs untraced" in capsys.readouterr().err


def test_an_untraced_nested_call_does_not_detach_its_parent(fake_sink, monkeypatch) -> None:
    """`_end`'s `token is not None` guard: `pop_span(None)` would wipe the whole stack."""
    log_foundry.configure(service="t", sink=fake_sink)
    from log_foundry import context, decorator
    real_open = decorator._open_span
    seen: list[object] = []

    def open_but_fail_for_child(name, defaults):
        if name == "child":
            raise OSError("no entropy")
        return real_open(name, defaults)

    monkeypatch.setattr(decorator, "_open_span", open_but_fail_for_child)

    @log_foundry.trace(name="child")
    def child() -> None:
        return None

    @log_foundry.trace(name="parent")
    def parent() -> None:
        before = context.current_span()
        child()
        seen.append(context.current_span() is before)

    contextvars.copy_context().run(parent)
    assert seen == [True], "the parent must still be current after an untraced nested call"


def test_a_normal_close_writes_nothing_to_stderr(fake_sink, capsys) -> None:
    log_foundry.configure(service="t", sink=fake_sink)

    @log_foundry.trace(name="work")
    def work() -> int:
        return 42

    contextvars.copy_context().run(work)
    log_foundry.shutdown()
    assert capsys.readouterr().err == ""


def test_events_are_flushed_before_the_wrapper_returns(fake_sink) -> None:
    """FR-002: a caller that flushes immediately afterwards observes the span."""
    log_foundry.configure(service="t", sink=fake_sink)

    @log_foundry.trace(name="work")
    def work() -> int:
        return 42

    def body() -> None:
        work()
        assert log_foundry.flush()
        assert any(e["function"] == "work" for e in fake_sink.events)

    contextvars.copy_context().run(body)


def test_a_sink_that_fails_to_construct_does_not_fail_the_caller(monkeypatch, capsys) -> None:
    """FR-001 as worded: the motivating case is a sink whose *construction* raises."""
    log_foundry.configure(service="t")
    from log_foundry import config
    monkeypatch.setattr(
        config, "_ensure_sink", lambda: (_ for _ in ()).throw(RuntimeError("no sink"))
    )

    @log_foundry.trace(name="work")
    def work() -> int:
        return 42

    @log_foundry.trace(name="boom")
    def boom() -> None:
        raise ValueError("mine")

    assert contextvars.copy_context().run(work) == 42
    with pytest.raises(ValueError, match="mine") as caught:
        contextvars.copy_context().run(boom)
    assert caught.value.__cause__ is None
    assert "absorbed a failure while closing a span (RuntimeError)" in capsys.readouterr().err


def test_a_raising_traced_call_leaves_no_reference_cycle(monkeypatch) -> None:
    """The outcome is held in a local past the `except`, which `except ... as` would have freed.

    Without an explicit `del`, the frame holds the exception whose traceback holds the frame —
    a cycle only the collector can break, which also pins the *caller's* frames and locals.
    """
    log_foundry.configure(service="t")
    from log_foundry import decorator
    monkeypatch.setattr(decorator, "_flush", lambda span: None)

    @log_foundry.trace(name="boom")
    def boom() -> None:
        raise ValueError("x")

    gc.collect()
    gc.disable()
    try:
        before = len(gc.get_objects())
        for _ in range(50):
            try:
                boom()
            except ValueError:
                pass
        growth = len(gc.get_objects()) - before
    finally:
        gc.enable()

    # ~13 objects per call are retained when the cycle is present (650 for 50 calls).
    assert growth < 100, f"retained {growth} objects across 50 raising calls"


# -- SPEC-050 FR-003: a span whose events cannot reach a worker counts them --------------


class _RefusingThread:
    """Makes `Thread.start` refuse for the drain thread only, as an out-of-threads process does."""

    def __init__(self, monkeypatch) -> None:
        self._real = threading.Thread.start

        def refuse(thread: threading.Thread) -> None:
            """Refuses the library's drain thread and lets pytest's own threads through."""
            if thread.name == "log-foundry-worker":
                raise RuntimeError("can't start new thread")
            self._real(thread)

        monkeypatch.setattr(threading.Thread, "start", refuse)


def test_a_span_that_cannot_reach_a_worker_counts_every_event_it_held(capsys) -> None:
    """FR-003 AC-1, AC-2. The reproduction: total loss under an all-clear `health()`.

    Measured before the fix: two traced calls, two stderr lines, and
    `Health(stopped_reason=None, in_span_lost=0)` with nothing delivered. The count is the span's
    whole buffer, so three events per span rather than one — a per-span increment would read 2
    for six lost events and understate the loss by a factor of the span's size.
    """
    sink = stdout_sink.StdoutSink()
    log_foundry.configure(service="test", sink=sink)

    with pytest.MonkeyPatch.context() as patch:
        _RefusingThread(patch)

        @log_foundry.trace
        def work() -> None:
            log_foundry.info("inside")

        work()
        work()

    err = capsys.readouterr().err
    health = log_foundry.health()

    assert health.in_span_lost == 6, "three events per span, both spans"
    assert err.count("absorbed a failure while closing a span (RuntimeError)") == 2, (
        "the existing line is unchanged and still one per span"
    )
    assert health.orphan_lost == 0, "this is not the synchronous path"


def test_a_span_failing_before_its_end_event_counts_only_what_it_held(capsys) -> None:
    """FR-003 AC-4. Both failure populations reach the count, with different totals.

    A fault in `end_event` leaves the span holding what it had *before* the close began, and a
    fault in `_flush` leaves it holding the end event too. Counting `len(span.events)` is what
    makes one site cover both; a constant would be wrong for at least one of them.
    """
    from log_foundry import decorator
    sink = stdout_sink.StdoutSink()
    log_foundry.configure(service="test", sink=sink)

    with pytest.MonkeyPatch.context() as patch:
        def boom(*args: object, **kwargs: object) -> None:
            """Fails where the end event is built, before it can be appended."""
            raise RuntimeError("no end event")

        patch.setattr(decorator, "end_event", boom)

        @log_foundry.trace
        def work() -> None:
            log_foundry.info("inside")

        work()

    capsys.readouterr()
    assert log_foundry.health().in_span_lost == 2, (
        "span.start and the info() call — the end event was never appended"
    )


def test_the_reordered_flush_preserves_event_order_and_counts_nothing(fake_sink, capsys) -> None:
    """FR-003 AC-3. The regression guard for reordering the hottest path in the library.

    `_flush` now resolves the worker before it detaches the buffer. The ordinary path must be
    untouched: every event delivered, in order, with the loss counter still at zero.
    """
    log_foundry.configure(service="test", sink=fake_sink)

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("one")
        log_foundry.info("two")

    work()
    assert bool(log_foundry.flush(timeout=5.0)), "the premise: the batch was delivered"
    capsys.readouterr()

    names = [e.get("message") or e.get("event") for e in fake_sink.events]
    assert names == ["span.start", "one", "two", "span.end"], f"order changed: {names}"
    assert log_foundry.health().in_span_lost == 0, "the ordinary path loses nothing"


def test_a_swept_span_cannot_be_double_counted_by_the_reordered_flush() -> None:
    """FR-003 AC-5, which the reorder makes reachable and the code answers by construction.

    `_sweep_open_spans` performs the same detach on the same attribute, so a span swept while
    `_get_worker()` is blocked hands its buffer over once and `_flush`'s later detach finds an
    empty list. The double-count half is structural rather than timed: `_sweep_open_spans` never
    calls `_note_in_span_loss`, so there is no second site to count from — asserted here as a
    property of the module rather than raced for, because a race test for an invariant that holds
    by construction can only ever pass.
    """
    import inspect

    from log_foundry import decorator

    source = inspect.getsource(decorator._sweep_open_spans)
    assert "_note_in_span_loss" not in source, (
        "the sweep gained a loss count, so a span it and _flush both touch can now be counted twice"
    )


# -- SPEC-054 FR-001 AC-3: the error sub-document is assembled through the clippers too ---------


def test_a_surrogate_in_the_exception_is_replaced_in_the_end_event(fake_sink) -> None:
    """A message from `surrogateescape` reaches `error.message` and `error.stack` as U+FFFD.

    `_error_fields` routes all four strings through `truncate_str`/`truncate_tail`, so the fix
    in the clippers reaches the span.end event without any change here; this pins that it does.
    """
    import os

    log_foundry.configure(service="t", sink=fake_sink)
    bad = os.fsdecode(b"file-\xff.txt")

    @log_foundry.trace
    def boom() -> None:
        raise RuntimeError(bad)

    with pytest.raises(RuntimeError):
        contextvars.copy_context().run(boom)
    log_foundry.shutdown()

    end = [e for e in fake_sink.events if e.get("status") == "error"][-1]
    error = end["error"]
    assert error["message"] == "file-�.txt"
    assert "file-�.txt" in error["stack"] and "\udcff" not in error["stack"]
    for text in (error["message"], error["stack"], end["function"]):
        text.encode("utf-8")
    assert end["truncated"] is True


# -- SPEC-054 FR-002: the span name is resolved once, and a misordered descriptor is refused ----


def _spans_named(fake_sink) -> set[str]:
    return {e["function"] for e in fake_sink.events if e["message"] == "span.start"}


def test_a_partial_is_traced_under_its_type_name(fake_sink) -> None:
    """`functools.partial` has no `__qualname__`; the wrapper used to raise AttributeError."""
    import functools

    log_foundry.configure(service="t", sink=fake_sink)

    def add(a: int, b: int = 2) -> int:
        return a + b

    assert log_foundry.trace(functools.partial(add, 1))() == 3
    log_foundry.shutdown()
    assert _spans_named(fake_sink) == {"partial"}


def test_a_callable_instance_is_traced_under_its_class(fake_sink) -> None:
    log_foundry.configure(service="t", sink=fake_sink)

    class Handler:
        def __call__(self) -> str:
            return "called"

    assert log_foundry.trace(Handler())() == "called"
    log_foundry.shutdown()
    assert _spans_named(fake_sink) == {"Handler"}


def test_the_name_is_resolved_once_at_decoration(fake_sink) -> None:
    """A `__qualname__` given to the instance after decoration is never read by the wrapper."""
    log_foundry.configure(service="t", sink=fake_sink)

    class Handler:
        def __call__(self) -> int:
            return 1

    instance = Handler()
    traced = log_foundry.trace(instance)
    instance.__qualname__ = "renamed.after.decoration"  # type: ignore[attr-defined]
    assert traced() == 1
    log_foundry.shutdown()
    assert _spans_named(fake_sink) == {"Handler"}


def test_an_explicit_name_still_wins_over_the_fallback(fake_sink) -> None:
    import functools

    log_foundry.configure(service="t", sink=fake_sink)
    log_foundry.trace(name="explicit")(functools.partial(lambda: 1))()
    log_foundry.shutdown()
    assert _spans_named(fake_sink) == {"explicit"}


def test_a_non_str_qualname_is_refused_by_wraps_at_decoration() -> None:
    """The fallthrough that a non-str `__qualname__` would need never runs: `wraps` refuses first."""

    class Handler:
        def __call__(self) -> None:
            pass

    instance = Handler()
    instance.__qualname__ = 5  # type: ignore[attr-defined]
    with pytest.raises(TypeError, match="__qualname__"):
        log_foundry.trace(instance)


def test_trace_above_classmethod_is_refused_at_decoration() -> None:
    with pytest.raises(TypeError, match=r"classmethod.*K\.m.*above @trace"):

        class K:
            @log_foundry.trace
            @classmethod
            def m(cls) -> None:
                pass


def test_trace_above_staticmethod_is_refused_at_decoration() -> None:
    """Refused even though a staticmethod object is callable: the wrapper replaces the descriptor."""
    with pytest.raises(TypeError, match=r"staticmethod.*K\.s.*above @trace"):

        class K:
            @log_foundry.trace
            @staticmethod
            def s() -> None:
                pass


def test_classmethod_above_trace_still_works(fake_sink) -> None:
    log_foundry.configure(service="t", sink=fake_sink)

    class K:
        @classmethod
        @log_foundry.trace
        def m(cls) -> str:
            return cls.__name__

    assert K.m() == "K"
    log_foundry.shutdown()
    assert _spans_named(fake_sink) == {
        "test_classmethod_above_trace_still_works.<locals>.K.m"
    }


def test_staticmethod_above_trace_still_works(fake_sink) -> None:
    log_foundry.configure(service="t", sink=fake_sink)

    class K:
        @staticmethod
        @log_foundry.trace
        def s() -> str:
            return "s"

    assert K().s() == "s"
    log_foundry.shutdown()
    assert _spans_named(fake_sink) == {
        "test_staticmethod_above_trace_still_works.<locals>.K.s"
    }


def test_a_non_callable_is_refused_naming_its_type() -> None:
    with pytest.raises(TypeError, match="got object"):
        log_foundry.trace(object())  # type: ignore[type-var]


def test_a_non_str_name_is_refused_at_decoration() -> None:
    """`@trace(name=1)` used to decorate happily and then run every call untraced, forever."""
    with pytest.raises(TypeError, match="name= must be a str, got int"):
        log_foundry.trace(name=1)(lambda: None)  # type: ignore[arg-type]


def test_a_string_is_refused_with_the_name_hint() -> None:
    """`@trace("checkout")` is the slip; the message says what was meant."""
    with pytest.raises(TypeError, match=r"name='checkout'"):
        log_foundry.trace("checkout")  # type: ignore[type-var]
