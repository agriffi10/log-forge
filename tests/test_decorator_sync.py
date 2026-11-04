"""SPEC-001 FR-006/FR-007 — sync @trace lifecycle without the logging API.

These tests drive the decorator against a `FakeSink` directly and assert on the span-boundary
events, exercising FR-006 (lifecycle, hierarchy, non-swallowing) and FR-007 (the `trace`
façade export).
"""

import contextvars
import gc
import re
import threading
import time
import types

import pytest

import log_foundry
from conftest import Hostility, armed, object_probe, str_probe
from log_foundry import _diag, decorator
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


# -- SPEC-055 FR-001 AC-3: the error sub-document is assembled through the clippers too ---------


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


# -- SPEC-055 FR-002: the span name is resolved once, and a misordered descriptor is refused ----


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


# -- SPEC-055 FR-003: a generator function is refused at decoration -----------------------------


def test_a_generator_function_is_refused_at_decoration() -> None:
    """The span would open and close before the body ran; every event inside would be an orphan."""
    with pytest.raises(TypeError, match=r"generator function .*gen\b.*Trace the consumer"):

        @log_foundry.trace
        def gen():  # type: ignore[no-untyped-def]
            yield 1


def test_a_partial_of_a_generator_function_is_refused() -> None:
    import functools

    def gen(n: int):  # type: ignore[no-untyped-def]
        yield n

    with pytest.raises(TypeError, match="generator function"):
        log_foundry.trace(functools.partial(gen, 1))


def test_an_instance_whose_call_is_a_generator_is_refused() -> None:
    class Producer:
        def __call__(self):  # type: ignore[no-untyped-def]
            yield 1

    with pytest.raises(TypeError, match="generator function"):
        log_foundry.trace(Producer())


def test_trace_above_contextmanager_is_refused() -> None:
    """`@contextmanager` advertises the generator through `__wrapped__`; measured, the orphan ledger."""
    import contextlib

    with pytest.raises(TypeError, match="generator function"):

        @log_foundry.trace
        @contextlib.contextmanager
        def scope():  # type: ignore[no-untyped-def]
            yield


def test_trace_above_a_wraps_wrapper_or_lru_cache_of_a_generator_is_refused() -> None:
    import functools

    def gen():  # type: ignore[no-untyped-def]
        yield 1

    @functools.wraps(gen)
    def wrapper():  # type: ignore[no-untyped-def]
        return gen()

    with pytest.raises(TypeError, match="generator function"):
        log_foundry.trace(wrapper)
    with pytest.raises(TypeError, match="generator function"):
        log_foundry.trace(functools.lru_cache(gen))


def test_a_wrapped_cycle_is_not_a_generator(fake_sink) -> None:
    """`inspect.unwrap` raises on a `__wrapped__` cycle; the guard treats it as not a generator."""
    log_foundry.configure(service="t", sink=fake_sink)

    def plain() -> int:
        return 1

    plain.__wrapped__ = plain  # type: ignore[attr-defined]
    assert log_foundry.trace(plain)() == 1


def test_a_function_that_returns_a_generator_object_is_accepted(fake_sink) -> None:
    """The stated limit: a plain function's code flags say nothing about what it returns."""
    log_foundry.configure(service="t", sink=fake_sink)

    def gen():  # type: ignore[no-untyped-def]
        yield 1

    @log_foundry.trace
    def make():  # type: ignore[no-untyped-def]
        return gen()

    assert list(make()) == [1]


# -- SPEC-080: @trace routes what it is handed on its real type ---------------------------------
#
# Every hostile value here records into `conftest.Hostility` while `armed()` is open and behaves
# as its base type otherwise, because pytest reads these values when it reports a failure: its
# `saferepr` reads `cls.__name__`, and an always-raising metaclass turns a failure into an
# INTERNALERROR. Every armed body is one `with armed() as calls:`; assertions run after it closes.

_SLIP = "@trace needs a callable, got str; pass a span name as @trace(name='checkout')"


def _class_says(answer: object) -> property:
    """A ``__class__`` property that records and answers ``answer`` (or raises) while armed."""

    def read(self):  # type: ignore[no-untyped-def]
        if Hostility.armed:
            Hostility.calls.append("__class__")
            if answer == "raise":
                raise RuntimeError("__class__-secret")
            return answer
        return type(self)

    return property(read)


def _always_class(answer: type) -> property:
    """A ``__class__`` property that always answers ``answer`` and records nothing.

    For the callable proxies whose ``__class__`` the kept ``isinstance`` is allowed to read
    (FR-004), so those reads do not land in a spy another criterion asserts empty.
    """
    return property(lambda self: answer)


def _name_meta(answer: object) -> type:
    """A fresh metaclass whose ``__name__`` property records and answers (or raises) while armed."""

    def read(cls):  # type: ignore[no-untyped-def]
        if Hostility.armed:
            Hostility.calls.append("meta.__name__")
            if answer == "raise":
                raise RuntimeError("meta-name-secret")
            return answer
        return type.__dict__["__name__"].__get__(cls)

    return type("_NameMeta", (type,), {"__name__": property(read)})


def _str_with_bool(text: str) -> str:
    """A ``str`` subclass holding ``text`` whose every method raises while armed, ``__bool__`` too.

    ``str`` has no ``__bool__``, so ``str_probe`` cannot hook it; the truthiness of a subclass
    is exactly what the old ``name or _span_name(fn)`` ran.
    """

    def boolean(self) -> bool:  # type: ignore[no-untyped-def]
        if Hostility.armed:
            Hostility.calls.append("__bool__")
            raise RuntimeError("__bool__-secret")
        return str.__len__(self) > 0

    base = type(str_probe(text, raising="all"))
    return type("_B", (base,), {"__bool__": boolean})(text)


def _span_starts(fake_sink) -> list[tuple[object, type]]:
    """Each ``span.start`` event's ``function`` and its exact type, in emission order."""
    return [
        (e["function"], type(e["function"]))
        for e in fake_sink.events
        if e["message"] == "span.start"
    ]


# FR-001: name= is routed on its real type at the trace(...) call, kept as an exact str.


def test_a_name_claiming_str_is_refused_at_the_trace_call(capsys) -> None:
    """At base the call returned a decorator, and every call it wrapped ran untraced."""
    value = object_probe("_ClaimsStr", class_property="str")

    with (
        armed() as calls,
        pytest.raises(TypeError, match=r"name= must be a str, got _ClaimsStr$"),
    ):
        log_foundry.trace(name=value)

    assert calls == []
    assert capsys.readouterr().err == ""


def test_a_name_whose_class_raises_is_refused_at_the_trace_call() -> None:
    value = object_probe("_ClassRaises", class_property="raise")

    with (
        armed() as calls,
        pytest.raises(TypeError, match=r"name= must be a str, got _ClassRaises$"),
    ):
        log_foundry.trace(name=value)

    assert calls == []


def test_a_name_whose_type_says_str_is_refused_by_its_stored_name() -> None:
    value = _name_meta("str")("_NameLies", (), {})()

    with (
        armed() as calls,
        pytest.raises(TypeError, match=r"name= must be a str, got _NameLies$"),
    ):
        log_foundry.trace(name=value)

    assert calls == []


async def test_a_hostile_str_subclass_name_traces_as_its_exact_str(fake_sink, capsys) -> None:
    """No method of the subclass runs, at decoration or per call, on the sync or async wrapper."""
    log_foundry.configure(service="t", sink=fake_sink)
    name = _str_with_bool("checkout")

    with armed() as calls:

        @log_foundry.trace(name=name)
        def work() -> int:
            return 1

        @log_foundry.trace(name=name)
        async def awork() -> int:
            return 2

        results = [work(), work(), await awork(), await awork()]

    log_foundry.shutdown()
    assert results == [1, 1, 2, 2]
    assert _span_starts(fake_sink) == [("checkout", str)] * 4
    assert calls == []
    assert capsys.readouterr().err == ""


def test_an_empty_str_subclass_name_falls_back_to_the_qualified_name(fake_sink) -> None:
    log_foundry.configure(service="t", sink=fake_sink)
    name = _str_with_bool("")

    def work() -> int:
        return 1

    with armed() as calls:
        result = log_foundry.trace(name=name)(work)()

    log_foundry.shutdown()
    assert result == 1
    assert _spans_named(fake_sink) == {
        "test_an_empty_str_subclass_name_falls_back_to_the_qualified_name.<locals>.work"
    }
    assert calls == []


def test_a_non_str_name_is_refused_before_any_decorator_is_applied() -> None:
    with pytest.raises(TypeError, match=r"name= must be a str, got int$"):
        log_foundry.trace(name=1)


def test_a_bad_name_is_reported_before_a_str_slip() -> None:
    """Two bad arguments: ``name=`` is judged at the call, so it is the one reported."""
    with pytest.raises(TypeError, match=r"name= must be a str, got int$"):
        log_foundry.trace("checkout", name=1)


def test_name_none_traces_under_the_qualified_name(fake_sink) -> None:
    log_foundry.configure(service="t", sink=fake_sink)

    def work() -> int:
        return 1

    assert log_foundry.trace(name=None)(work)() == 1
    log_foundry.shutdown()
    assert _spans_named(fake_sink) == {
        "test_name_none_traces_under_the_qualified_name.<locals>.work"
    }


# FR-002: the span-name fallback routes on real type and reads the stored type name.


def test_a_callable_whose_type_name_is_not_a_str_traces_under_its_stored_name(
    fake_sink, capsys
) -> None:
    """At base the span name was ``1``, and every call absorbed a failure opening its span."""
    log_foundry.configure(service="t", sink=fake_sink)
    cls = _name_meta(1)("_IntNamed", (), {"__call__": lambda self: 7})

    with armed() as calls:
        traced = log_foundry.trace(cls())
        results = [traced(), traced()]

    log_foundry.shutdown()
    assert results == [7, 7]
    assert _span_starts(fake_sink) == [("_IntNamed", str)] * 2
    assert "absorbed a failure while opening a span" not in capsys.readouterr().err
    assert calls == []


async def test_a_callable_whose_type_name_raises_traces_under_its_stored_name(fake_sink) -> None:
    log_foundry.configure(service="t", sink=fake_sink)
    meta = _name_meta("raise")

    async def acall(self) -> int:  # type: ignore[no-untyped-def]
        return 2

    sync_cls = meta("_Nameless", (), {"__call__": lambda self: 1})
    async_cls = meta("_ANameless", (), {"__call__": acall})

    with armed() as calls:
        traced = log_foundry.trace(sync_cls())
        atraced = log_foundry.trace(async_cls())
        results = [traced(), await atraced()]

    log_foundry.shutdown()
    assert results == [1, 2]
    assert sorted(_span_starts(fake_sink)) == [("_ANameless", str), ("_Nameless", str)]
    assert calls == []


def test_a_generator_callable_whose_type_name_raises_is_refused_by_its_stored_name() -> None:
    """The generator refusal's message is ``_span_name``'s second caller."""

    def produce(self):  # type: ignore[no-untyped-def]
        yield 1

    instance = _name_meta("raise")("_GenNameless", (), {"__call__": produce})()

    with (
        armed() as calls,
        pytest.raises(TypeError, match=r"generator function _GenNameless:"),
    ):
        log_foundry.trace(instance)

    assert calls == []


def test_span_name_copies_a_str_subclass_type_name() -> None:
    class _Plain:
        def __call__(self) -> None:
            pass

    _Plain.__name__ = str_probe("Held", raising="all")
    instance = _Plain()

    with armed() as calls:
        got = decorator._span_name(instance)

    assert type(got) is str
    assert got == "Held"
    assert calls == []


def test_span_name_copies_a_str_subclass_qualname() -> None:
    def f() -> None:
        pass

    f.__qualname__ = str_probe("Held.q", raising="all")

    with armed() as calls:
        got = decorator._span_name(f)

    assert type(got) is str
    assert got == "Held.q"
    assert calls == []


def test_a_qualname_claiming_str_is_refused_without_reading_its_class() -> None:
    """``functools.wraps`` still refuses it; the route no longer runs its ``__class__`` first."""

    class Handler:
        def __call__(self) -> None:
            pass

    instance = Handler()
    instance.__qualname__ = object_probe("_QualLies", class_property="str")

    with armed() as calls, pytest.raises(TypeError, match="__qualname__"):
        log_foundry.trace(instance)

    assert calls == []


# FR-003: the str slip is refused first, by real type, with a bounded hint.


def test_the_str_slip_message_is_unchanged() -> None:
    with pytest.raises(TypeError) as info:
        log_foundry.trace("checkout")

    assert str(info.value) == _SLIP


def test_a_str_subclass_slip_is_refused_without_running_its_code() -> None:
    value = str_probe("checkout", raising={"__repr__", "__getitem__", "__len__"})

    with armed() as calls, pytest.raises(TypeError) as info:
        log_foundry.trace(value)

    assert str(info.value) == _SLIP
    assert calls == []


@pytest.mark.parametrize(
    "make",
    [
        lambda: type("_CS", (str,), {"__class__": _class_says("raise")})("checkout"),
        lambda: str_probe("checkout", raising={"__getattribute__"}),
    ],
    ids=["class-raises", "getattribute-raises"],
)
def test_a_str_subclass_slip_whose_class_or_attributes_raise_is_refused(make) -> None:
    value = make()

    with armed() as calls, pytest.raises(TypeError) as info:
        log_foundry.trace(value)

    assert str(info.value) == _SLIP
    assert calls == []


def test_a_str_subclass_slip_claiming_classmethod_is_refused_as_a_str() -> None:
    value = type("_CM", (str,), {"__class__": _class_says(classmethod)})("checkout")

    with armed() as calls, pytest.raises(TypeError) as info:
        log_foundry.trace(value)

    assert str(info.value) == _SLIP
    assert calls == []


def test_a_long_str_slip_is_echoed_bounded() -> None:
    with pytest.raises(TypeError) as info:
        log_foundry.trace("x" * 10_000)

    assert str(info.value) == (
        "@trace needs a callable, got str; pass a span name as @trace(name='" + "x" * 63 + "…)"
    )


def test_a_callable_claiming_str_is_traced(fake_sink, capsys) -> None:
    """Its ``__class__`` still runs at decoration, the caller's own code; never per call."""
    log_foundry.configure(service="t", sink=fake_sink)
    reads: list[str] = []

    class _CallableClaimsStr:
        @property
        def __class__(self) -> type:
            reads.append("__class__")
            return str

        def __call__(self) -> int:
            return 5

    traced = log_foundry.trace(_CallableClaimsStr())
    reads.clear()
    results = [traced(), traced()]

    log_foundry.shutdown()
    assert results == [5, 5]
    assert reads == []
    assert _span_starts(fake_sink) == [("_CallableClaimsStr", str)] * 2
    assert capsys.readouterr().err == ""


def test_an_echo_failure_at_decoration_propagates(monkeypatch) -> None:
    """``_diag._echo``'s second caller lets a failure out, where ``rejected`` writes ``<?>``."""

    def failing(value: object) -> str:
        raise MemoryError("echo")

    monkeypatch.setattr(_diag, "_echo", failing)
    with pytest.raises(MemoryError, match="echo"):
        log_foundry.trace("checkout")


# FR-004: the descriptor and non-callable refusals read nothing unchecked, isinstance last.


def test_a_callable_claiming_classmethod_without_func_is_told_classmethod(capsys) -> None:
    class _ClaimsCM:
        __class__ = _always_class(classmethod)

        def __call__(self) -> None:
            pass

    message = (
        "cannot wrap the classmethod object around the function; put @classmethod above @trace"
    )
    with pytest.raises(TypeError, match=re.escape(message)):
        log_foundry.trace(_ClaimsCM())

    assert capsys.readouterr().err == ""


def test_a_callable_claiming_staticmethod_is_told_staticmethod_and_its_func() -> None:
    class _ClaimsSM:
        __class__ = _always_class(staticmethod)

        def __call__(self) -> None:
            pass

    def g() -> None:
        pass

    g.__qualname__ = "g"
    instance = _ClaimsSM()
    instance.__func__ = g

    message = "cannot wrap the staticmethod object around g; put @staticmethod above @trace"
    with pytest.raises(TypeError, match=re.escape(message)):
        log_foundry.trace(instance)


def test_a_claimed_classmethod_whose_func_qualname_is_not_a_str_says_the_function() -> None:
    class _ClaimsCM:
        __class__ = _always_class(classmethod)

        def __call__(self) -> None:
            pass

    instance = _ClaimsCM()
    instance.__func__ = types.SimpleNamespace(__qualname__=5)

    with pytest.raises(TypeError, match="around the function;"):
        log_foundry.trace(instance)


def test_a_claimed_classmethod_whose_func_qualname_claims_str_says_the_function() -> None:
    class _ClaimsCM:
        __class__ = _always_class(classmethod)

        def __call__(self) -> None:
            pass

    instance = _ClaimsCM()
    instance.__func__ = types.SimpleNamespace(
        __qualname__=object_probe("_QQ", class_property="str")
    )

    with armed() as calls, pytest.raises(TypeError, match="around the function;"):
        log_foundry.trace(instance)

    assert calls == []


def test_a_real_classmethod_with_a_str_subclass_qualname_names_the_held_text() -> None:
    def m(cls) -> None:
        pass

    m.__qualname__ = str_probe("K.m", raising={"__format__", "__str__"})
    descriptor = classmethod(m)

    with (
        armed() as calls,
        pytest.raises(TypeError, match=re.escape("around K.m; put @classmethod above @trace")),
    ):
        log_foundry.trace(descriptor)

    assert calls == []


def test_a_classmethod_subclass_is_told_its_own_name() -> None:
    class MyCM(classmethod):
        pass

    def m(cls) -> None:
        pass

    with pytest.raises(TypeError, match=re.escape("put @MyCM above @trace")):
        log_foundry.trace(MyCM(m))


def test_a_classmethod_subclass_whose_type_name_raises_is_told_its_stored_name() -> None:
    def m(cls) -> None:
        pass

    descriptor = _name_meta("raise")("_NamelessCM", (classmethod,), {})(m)

    with (
        armed() as calls,
        pytest.raises(TypeError, match=re.escape("put @_NamelessCM above @trace")),
    ):
        log_foundry.trace(descriptor)

    assert calls == []


def test_a_non_callable_claiming_classmethod_is_told_it_needs_a_callable() -> None:
    """The accepted trade-off: a non-callable proxy is not given the descriptor advice."""

    class _NCProxy:
        __class__ = _always_class(classmethod)

    def g() -> None:
        pass

    instance = _NCProxy()
    instance.__func__ = g

    with pytest.raises(TypeError, match=r"needs a callable, got _NCProxy$"):
        log_foundry.trace(instance)


def test_a_non_callable_whose_class_raises_is_told_it_needs_a_callable() -> None:
    value = object_probe("_ClassRaises", class_property="raise")

    with (
        armed() as calls,
        pytest.raises(TypeError, match=r"needs a callable, got _ClassRaises$"),
    ):
        log_foundry.trace(value)

    assert calls == []


def test_a_non_callable_whose_type_name_raises_is_told_its_stored_name() -> None:
    value = object_probe("_Nameless", meta=True)

    with (
        armed() as calls,
        pytest.raises(TypeError, match=r"needs a callable, got _Nameless$"),
    ):
        log_foundry.trace(value)

    assert calls == []
