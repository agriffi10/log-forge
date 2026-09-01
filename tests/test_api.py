"""SPEC-002 — level logging API: append-to-span, precedence, baggage, orphan path.

These assert the contract (level, fields, trace/span linkage), not the auto-generated
span-boundary wording. The `lf` fixture configures a FakeSink; where a test needs the
emitted events without a decorated call (orphan path), it asserts on the sink directly.
"""

import contextvars
import io
import sys

import pytest

from log_foundry import _lifecycle

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
    """Inside a span the value reached the sink, json.dumps raised, and the retry loop abandoned
    the whole flattened batch — taking co-batched events from unrelated spans with it.

    Driven through a real ``Worker`` rather than the fixture's inline flush, so the co-batching
    and the abandon path are the real ones: ``batch_size=2`` against a long interval makes the
    second submission flush both spans as exactly one batch, and the sink serializes as any real
    sink does. Before SPEC-020 that ``json.dumps`` raised and ``failed_batches`` went to 1.
    """
    import json

    from log_foundry.worker import Worker

    @lf.trace(name="poisoned")
    def poisoned() -> None:
        lf.info("big", n=_HUGE)

    @lf.trace(name="innocent")
    def innocent() -> None:
        lf.info("small", n=1)

    contextvars.copy_context().run(poisoned)
    contextvars.copy_context().run(innocent)
    poisoned_events, innocent_events = fake_sink.batches[0], fake_sink.batches[1]

    class JsonSink:
        def __init__(self) -> None:
            self.batches: list[list[dict]] = []

        def emit(self, batch: list[dict]) -> None:
            json.dumps(batch)
            self.batches.append(list(batch))

        def close(self) -> None:
            pass

    sink = JsonSink()
    worker = Worker(sink, batch_size=2, flush_interval=60.0)
    worker.submit(poisoned_events)
    worker.submit(innocent_events)
    worker.shutdown()

    assert len(sink.batches) == 1, "both spans must land in one flattened batch"
    messages = [event["message"] for event in sink.batches[0]]
    assert "big" in messages
    assert "small" in messages, "the unrelated span survives intact"
    assert worker.health().failed_batches == 0, "the batch was delivered, not abandoned"


# -- SPEC-024 FR-003: the orphan path is what `reset_context` exists for -----------------


def test_reset_context_clears_baggage_for_a_later_orphan_log(lf, fake_sink) -> None:
    """The case with no root span to hang the release on — the emitters used without @trace."""

    def body() -> None:
        lf.set_baggage(user_id="alice")
        lf.info("serving alice")
        lf.reset_context()
        lf.info("serving bob")

    contextvars.copy_context().run(body)

    alice, bob = fake_sink.events
    assert alice["fields"]["user_id"] == "alice"
    assert "user_id" not in bob["fields"]


def test_reset_context_inside_a_real_root_span_also_empties_the_boundary_events(
    lf, fake_sink
) -> None:
    """FR-003's "safe inside an open span", end to end — including the part that surprises.

    SPEC-015 backfills the boundary events from the baggage live at *close*, so a reset mid-span
    leaves `span.start` and `span.end` — the events carrying `duration_ms` and `status` — with
    none of it, even though it was live for most of the span. That follows from SPEC-015's
    "boundary events take the span's final baggage" decision rather than contradicting it, but
    it is why the docs say to call this outside a span.
    """

    @lf.trace(name="work")
    def work() -> None:
        lf.set_baggage(user_id="alice")
        lf.info("before")
        lf.reset_context()
        lf.info("after")

    def body() -> None:
        work()

    contextvars.copy_context().run(body)

    by_message = {e["message"]: e for e in fake_sink.events}
    assert by_message["before"]["fields"]["user_id"] == "alice"
    assert "user_id" not in by_message["after"]["fields"]
    boundary = [e for e in fake_sink.events if e["message"].startswith("span.")]
    assert len(boundary) == 2
    assert all("user_id" not in e["fields"] for e in boundary)


# -- SPEC-025 FR-003: the orphan path cannot raise into the caller -------------------------
#
# The orphan branch is the only place a level call reaches the sink on the caller's own
# thread, with no worker between them to absorb a failure.


class _BrokenSink:
    """A sink that fails the way a real one does — at emit time, not construction."""

    def __init__(self, exc: BaseException | None = None) -> None:
        self.exc = exc or ConnectionError("sink is down")

    def emit(self, batch: list[dict]) -> None:
        raise self.exc

    def close(self) -> None:
        pass


class _BrokenStream:
    """A stream that is closed, redirected, or gone — the shape `ConsoleWriter` cannot survive."""

    def __init__(self, exc: BaseException | None = None) -> None:
        self.exc = exc or OSError("stream closed")

    def write(self, s: str) -> int:
        raise self.exc

    def flush(self) -> None:
        raise self.exc


@pytest.mark.parametrize("level", LEVELS)
def test_an_orphan_log_survives_a_broken_sink(level, capsys) -> None:
    lf = pytest.importorskip("log_foundry")
    lf.configure(service="t", sink=_BrokenSink())

    def body() -> None:
        getattr(lf, level)("bare line", code=7)

    contextvars.copy_context().run(body)  # must not raise

    err = capsys.readouterr().err
    assert "absorbed a failure while emitting an orphan log (ConnectionError)" in err
    assert "the event was lost" in err
    assert "sink is down" not in err, "the message is never written (arch §6)"


def test_an_in_span_log_is_unaffected_by_a_broken_sink() -> None:
    """FR-003 requires no behaviour change in-span: it buffers and never touches the sink."""
    lf = pytest.importorskip("log_foundry")
    lf.configure(service="t", sink=_BrokenSink())
    context = pytest.importorskip("log_foundry.context")
    seen: list[int] = []

    def body() -> None:
        span = api.Span(
            trace_id="a" * 32, span_id="b" * 16, parent_span_id=None, name="x", start_ts=0.0
        )
        token = context.push_span(span)
        try:
            lf.info("in-span line")
            seen.append(len(span.events))
        finally:
            context.pop_span(token)

    contextvars.copy_context().run(body)
    assert seen == [1], "the event was appended to the span's buffer, not emitted"


def _break_console(monkeypatch, exc: BaseException | None = None) -> None:
    """Replace the console writer wholesale, as `test_console_echo.py` does.

    Never poke `_console._stream` and restore it by hand: under `capsys` the "original" is
    pytest's per-test `CaptureIO`, which is closed at teardown, so the restore leaves the
    process-global writer permanently broken — and this spec's own echo guard would then
    absorb the breakage silently for the rest of the session.
    """
    console_mod = pytest.importorskip("log_foundry.console")
    monkeypatch.setattr(
        api, "_console", console_mod.ConsoleWriter(stream=_BrokenStream(exc))
    )


def test_echo_survives_a_broken_console_and_the_event_still_reaches_the_sink(
    fake_sink, monkeypatch, capsys
) -> None:
    lf = pytest.importorskip("log_foundry")
    lf.configure(service="t", sink=fake_sink)
    _break_console(monkeypatch)

    def body() -> None:
        lf.info("echoed", echo=True)

    contextvars.copy_context().run(body)

    assert [e["message"] for e in fake_sink.events] == ["echoed"]
    assert "absorbed a failure while echoing to the console (OSError)" in capsys.readouterr().err


def test_a_broken_sink_still_lets_the_echo_through(monkeypatch, capsys) -> None:
    """Why the echo is gated on the event existing rather than skipped when the emit fails.

    The event was built before `emit` was reached, so the operator still sees the line on the
    console even though the sink lost it — which is the more useful of the two outcomes.
    """
    lf = pytest.importorskip("log_foundry")
    lf.configure(service="t", sink=_BrokenSink())
    console_mod = pytest.importorskip("log_foundry.console")
    stream = io.StringIO()
    monkeypatch.setattr(api, "_console", console_mod.ConsoleWriter(stream=stream))

    def body() -> None:
        lf.info("echoed", echo=True)

    contextvars.copy_context().run(body)

    assert "echoed" in stream.getvalue(), "a lost event is still worth echoing"
    err = capsys.readouterr().err
    assert err.count("absorbed a failure") == 1, "the emit failed; the echo did not"
    assert "echoing to the console" not in err


def test_a_keyboardinterrupt_from_the_console_still_propagates(fake_sink, monkeypatch) -> None:
    """The echo guard draws the same line as the sink guard: Exception, never BaseException."""
    lf = pytest.importorskip("log_foundry")
    lf.configure(service="t", sink=fake_sink)
    _break_console(monkeypatch, KeyboardInterrupt())

    def body() -> None:
        lf.info("echoed", echo=True)

    with pytest.raises(KeyboardInterrupt):
        contextvars.copy_context().run(body)


def test_the_guard_covers_building_the_orphan_event_not_only_the_emit(monkeypatch, capsys) -> None:
    """The whole branch is wrapped: `build_event` is inside it too, not just `emit`.

    `echo=True` here also pins the ``event is not None`` gate — with no event built there is
    nothing to echo, and echoing ``None`` would raise a second, unrelated failure inside the
    guard that exists to keep this call quiet.
    """
    lf = pytest.importorskip("log_foundry")
    lf.configure(service="t")
    monkeypatch.setattr(
        api, "build_event", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("cannot build"))
    )

    def body() -> None:
        lf.info("bare line", echo=True)

    contextvars.copy_context().run(body)
    err = capsys.readouterr().err
    assert "absorbed a failure while emitting an orphan log (RuntimeError)" in err
    assert err.count("absorbed a failure") == 1, "no second failure from echoing a missing event"


def test_an_orphan_log_returns_even_when_stderr_is_broken_too(monkeypatch) -> None:
    """The channel of last resort has no fallback: losing the line beats raising."""
    lf = pytest.importorskip("log_foundry")
    lf.configure(service="t", sink=_BrokenSink())
    monkeypatch.setattr(sys, "stderr", _BrokenStream())

    def body() -> None:
        lf.info("nowhere to report this")

    contextvars.copy_context().run(body)  # must not raise


def test_a_keyboardinterrupt_from_the_sink_still_propagates() -> None:
    """FR-003, as FR-001: the guard catches Exception, never BaseException."""
    lf = pytest.importorskip("log_foundry")
    lf.configure(service="t", sink=_BrokenSink(KeyboardInterrupt()))

    def body() -> None:
        lf.info("interrupted")

    with pytest.raises(KeyboardInterrupt):
        contextvars.copy_context().run(body)


def test_a_sink_that_fails_to_construct_does_not_fail_an_orphan_log(monkeypatch, capsys) -> None:
    """`_ensure_sink` builds the sink on first use, so the whole branch is guarded, not `emit`."""
    lf = pytest.importorskip("log_foundry")
    lf.configure(service="t")
    monkeypatch.setattr(
        api, "_ensure_sink", lambda: (_ for _ in ()).throw(RuntimeError("cannot build a sink"))
    )

    def body() -> None:
        lf.info("bare line")

    contextvars.copy_context().run(body)
    err = capsys.readouterr().err
    assert "absorbed a failure while emitting an orphan log (RuntimeError)" in err


# -- SPEC-031 FR-006: a process that never created a worker still closes its sink ------------


class _CountingSink:
    """Records the events it took and how many times it was closed."""

    def __init__(self) -> None:
        self.events: list[dict] = []
        self.closes = 0

    def emit(self, batch: list[dict]) -> None:
        self.events.extend(batch)

    def close(self) -> None:
        self.closes += 1


def test_an_orphan_only_process_closes_its_sink_exactly_once_on_shutdown() -> None:
    """The defect: shutdown() returned early on a null worker, so close() never happened."""
    lf = pytest.importorskip("log_foundry")
    sink = _CountingSink()
    lf.configure(service="t", version="0", env="t", sink=sink)

    lf.info("no span anywhere")
    assert sink.closes == 0, "nothing closes while the process is still logging"

    lf.shutdown()

    assert sink.closes == 1
    assert len(sink.events) == 1, "the event landed synchronously, as it always did"


def test_shutdown_stays_idempotent_on_the_orphan_path() -> None:
    lf = pytest.importorskip("log_foundry")
    sink = _CountingSink()
    lf.configure(service="t", sink=sink)
    lf.info("x")

    lf.shutdown()
    lf.shutdown()
    lf.shutdown()

    assert sink.closes == 1, "the once-only guard sits ahead of the close, as Worker's does"


def test_a_close_that_raises_does_not_reach_the_caller(capsys) -> None:
    """SPEC-025: this runs from atexit, where an escape makes CPython print the message."""
    lf = pytest.importorskip("log_foundry")

    class _Exploding(_CountingSink):
        def close(self) -> None:
            super().close()
            raise RuntimeError("the destination went away")

    sink = _Exploding()
    lf.configure(service="t", sink=sink)
    lf.info("x")

    lf.shutdown()  # must not raise

    assert sink.closes == 1
    err = capsys.readouterr().err
    assert "absorbed a failure while closing the sink (RuntimeError)" in err
    assert "the destination went away" not in err, "the message is never written (arch §6)"

    lf.shutdown()
    assert sink.closes == 1, "a failed close is announced, not retried"


def test_configure_without_ever_logging_closes_nothing_and_creates_nothing() -> None:
    """AC-8. Not because the sink was never built — configure() always builds one."""
    lf = pytest.importorskip("log_foundry")

    sink = _CountingSink()
    lf.configure(service="t", sink=sink)

    lf.shutdown()

    assert sink.closes == 0, "no event ever reached it, so closing it is cost with no benefit"
    assert _lifecycle._state._worker is None


def test_no_worker_thread_is_created_by_the_orphan_lifecycle() -> None:
    """AC-7. Out of Scope bans standing up a thread at exit to prove there is nothing to drain."""
    import threading

    lf = pytest.importorskip("log_foundry")

    before = threading.active_count()
    sink = _CountingSink()
    lf.configure(service="t", sink=sink)
    lf.info("one")
    lf.info("two")
    lf.shutdown()

    assert threading.active_count() == before
    assert _lifecycle._state._worker is None
    assert sink.closes == 1


def test_health_reports_retired_after_an_orphan_only_shutdown() -> None:
    """AC-4/AC-5: `retired` stops being vacuous; `submitted_after_shutdown` keeps its meaning."""
    lf = pytest.importorskip("log_foundry")

    sink = _CountingSink()
    lf.configure(service="t", sink=sink)
    lf.info("x")
    assert lf.health().retired is False

    lf.shutdown()

    health = lf.health()
    assert health.retired is True
    assert health.submitted_after_shutdown == 0, (
        "SPEC-030 defines that as queued-where-nothing-drains; this is refused-and-announced"
    )
    assert health.stopped_reason is None, "a clean shutdown is not a terminal failure (SPEC-019)"
    assert _lifecycle._state._worker is None, "health() must not create a worker to answer this"


def test_a_guarded_sink_refuses_the_log_that_follows_the_close(tmp_path, capsys) -> None:
    """AC-6, against a sink that actually carries a post-close guard (SPEC-032)."""
    lf = pytest.importorskip("log_foundry")
    sqlite_mod = pytest.importorskip("log_foundry.sinks.sqlite")

    sink = sqlite_mod.SQLiteSink(str(tmp_path / "events.db"))
    lf.configure(service="t", sink=sink)
    lf.info("before")
    lf.shutdown()
    capsys.readouterr()

    def body() -> None:
        lf.info("after the close")

    contextvars.copy_context().run(body)

    err = capsys.readouterr().err
    assert err.count("absorbed a failure while emitting an orphan log (SinkDeliveryError)") == 1, (
        "refused at the closed sink and announced once, not silently buffered — and named, so "
        "an unrelated fault in this branch cannot satisfy the assertion"
    )


def test_the_default_stdout_sink_is_not_expected_to_refuse(capsys) -> None:
    """The counterpart to AC-6: 19 of 34 sink classes add no post-close guard, by design."""
    lf = pytest.importorskip("log_foundry")
    stream = io.StringIO()
    from log_foundry.sinks.stdout import StdoutSink

    lf.configure(service="t", sink=StdoutSink(stream=stream))
    lf.info("before")
    lf.shutdown()

    def body() -> None:
        lf.info("after the close")

    contextvars.copy_context().run(body)

    assert "after the close" in stream.getvalue(), (
        "StdoutSink's close() only flushes; an implementer testing AC-6 here misreads the fix"
    )
    assert "absorbed a failure" not in capsys.readouterr().err


def test_a_mixed_process_closes_once_and_keeps_the_worker_drain_orphan_first() -> None:
    """AC-3, first order. Both failure modes the FR names are green without this test.

    A reused ``_atexit_registered`` would have the orphan log consume the worker's
    registration, costing a mixed process its exit drain (SPEC-004 FR-005); a second ``atexit``
    handler would double-close, since ``atexit`` runs LIFO. Neither shows up in a span-only or
    an orphan-only test.
    """
    lf = pytest.importorskip("log_foundry")

    sink = _CountingSink()
    lf.configure(service="t", version="0", env="t", sink=sink)

    lf.info("orphan first")

    @lf.trace
    def traced() -> None:
        lf.info("inside the span")

    traced()
    assert _lifecycle._state._worker is not None, "the span built a worker"

    lf.shutdown()

    assert sink.closes == 1, "the worker owns the close; the orphan path must defer to it"
    messages = [event["message"] for event in sink.events]
    assert "orphan first" in messages
    assert "inside the span" in messages, "the worker's drain still ran"
    assert "span.end" in messages


def test_a_mixed_process_closes_once_and_keeps_the_worker_drain_span_first() -> None:
    """AC-3, the other order — the worker exists before anything arms the orphan close."""
    lf = pytest.importorskip("log_foundry")

    sink = _CountingSink()
    lf.configure(service="t", version="0", env="t", sink=sink)

    @lf.trace
    def traced() -> None:
        lf.info("inside the span")

    traced()

    def body() -> None:
        lf.info("orphan second")

    contextvars.copy_context().run(body)
    assert _lifecycle._state._worker is not None

    lf.shutdown()

    assert sink.closes == 1
    messages = [event["message"] for event in sink.events]
    assert "inside the span" in messages
    assert "orphan second" in messages
    assert lf.health().retired is True


def test_a_span_only_process_still_closes_exactly_once() -> None:
    """The regression guard: nothing about FR-006 may add a close to the path that worked."""
    lf = pytest.importorskip("log_foundry")

    sink = _CountingSink()
    lf.configure(service="t", version="0", env="t", sink=sink)

    @lf.trace
    def traced() -> None:
        return None

    traced()
    lf.shutdown()

    assert sink.closes == 1


_ORPHAN_EXIT_PROGRAM = """
import log_foundry as lf

class CountingSink:
    def __init__(self):
        self.closes = 0
    def emit(self, batch):
        pass
    def close(self):
        self.closes += 1
        print("CLOSES=%d" % self.closes, flush=True)

sink = CountingSink()
lf.configure(service="t", env="t", sink=sink)
lf.info("no span anywhere")
{shutdown}
print("END OF MAIN", flush=True)
"""


@pytest.mark.parametrize(
    ("shutdown", "label"),
    [("", "atexit alone"), ("lf.shutdown()", "explicit shutdown, then atexit")],
)
def test_interpreter_exit_closes_an_orphan_only_sink_exactly_once(
    shutdown, label, tmp_path
) -> None:
    """AC-2. A real subprocess, and the *count* is what is asserted.

    ``atexit`` handlers do not run inside pytest, and ``atexit._run_exitfuncs()`` would fire the
    whole registry and corrupt suite state. The count rather than the mere fact of a close is
    what distinguishes the fix from a second ``atexit`` handler, which closes twice — that
    variant passes a "did it close?" assertion and is the trap the FR names.
    """
    import os
    import pathlib
    import subprocess
    import sys as sys_mod

    lf = pytest.importorskip("log_foundry")
    src = pathlib.Path(lf.__file__).resolve().parent.parent
    script = tmp_path / f"orphan_exit_{len(shutdown)}.py"
    script.write_text(_ORPHAN_EXIT_PROGRAM.format(shutdown=shutdown))

    # Suppressed for the reason the sibling atexit test in test_worker.py gives: this
    # interpreter plus a script written into pytest's own tmp_path. No shell, no outside input.
    result = subprocess.run(  # noqa: S603
        [sys_mod.executable, str(script)],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": str(src)},
        timeout=60,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "END OF MAIN" in result.stdout
    assert result.stdout.count("CLOSES=") == 1, (
        f"{label}: expected exactly one close, got {result.stdout.count('CLOSES=')}"
    )
    assert "CLOSES=1" in result.stdout
    assert "Traceback" not in result.stderr


_MIXED_EXIT_PROGRAM = """
import log_foundry as lf

class CountingSink:
    def __init__(self):
        self.closes = 0
        self.delivered = []
    def emit(self, batch):
        self.delivered.extend(e["message"] for e in batch)
    def close(self):
        self.closes += 1
        print("CLOSES=%d DELIVERED=%s" % (self.closes, sorted(set(self.delivered))), flush=True)

sink = CountingSink()
lf.configure(service="t", env="t", sink=sink)

@lf.trace(name="work")
def work():
    lf.info("inside the span")

{first}
{second}
print("END OF MAIN", flush=True)
"""


@pytest.mark.parametrize(
    ("first", "second", "order"),
    [
        ('lf.info("orphan")', "work()", "orphan-then-span"),
        ("work()", 'lf.info("orphan")', "span-then-orphan"),
    ],
)
def test_a_mixed_process_at_interpreter_exit_closes_once_and_still_drains(
    first, second, order, tmp_path
) -> None:
    """AC-3's other half, and the only test that can see it.

    The in-process siblings call ``shutdown()`` explicitly, so the ``atexit`` registration is
    never exercised there — and that is precisely where the FR's first trap lives. Reusing
    ``_atexit_registered`` to arm the orphan close makes ``_get_worker`` skip
    ``atexit.register``, so a mixed process silently loses its exit drain (SPEC-004 FR-005) with
    every in-process test still green. The second trap, a separate handler, shows up here as
    ``CLOSES=2``.
    """
    import os
    import pathlib
    import subprocess
    import sys as sys_mod

    lf = pytest.importorskip("log_foundry")
    src = pathlib.Path(lf.__file__).resolve().parent.parent
    script = tmp_path / f"mixed_exit_{order}.py"
    script.write_text(_MIXED_EXIT_PROGRAM.format(first=first, second=second))

    # Suppressed for the reason the sibling atexit test in test_worker.py gives.
    result = subprocess.run(  # noqa: S603
        [sys_mod.executable, str(script)],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": str(src)},
        timeout=60,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "END OF MAIN" in result.stdout
    assert result.stdout.count("CLOSES=") == 1, f"{order}: expected exactly one close"
    assert "CLOSES=1" in result.stdout
    assert "inside the span" in result.stdout, (
        f"{order}: the worker's atexit drain must still run — the span's events reached the sink"
    )
    assert "orphan" in result.stdout
    assert "span.end" in result.stdout, "the whole span, not just the level call inside it"


# -- SPEC-031 FR-006: the guards a first review found untested ------------------------------


def test_the_orphan_close_defers_to_a_live_worker_even_when_called_directly() -> None:
    """The `_worker is not None` guard, exercised in isolation.

    A first review found it dead under test: removing it alone left the whole suite green,
    because `_shutdown_worker` returns before reaching here whenever a worker exists. It is
    still the guard that keeps a `shutdown()` racing a first `@trace` from closing the sink the
    new worker just captured, so it is asserted directly rather than only through that caller.
    """
    lf = pytest.importorskip("log_foundry")

    sink = _CountingSink()
    lf.configure(service="t", version="0", env="t", sink=sink)
    lf.info("arms the close")

    @lf.trace
    def traced() -> None:
        return None

    traced()
    assert _lifecycle._state._worker is not None

    _lifecycle._close_orphan_sink()

    assert sink.closes == 0, "the worker owns this sink; the orphan close must stand down"


def test_the_worker_check_is_read_under_the_lock_that_publishes_the_worker() -> None:
    """A shutdown() racing a first @trace must not close the sink the worker just captured.

    The race needs an injected preemption point — the repo's own SPEC-028 precedent. The lock
    wrapper creates the worker inside `__enter__`, i.e. exactly while a thread is blocked
    acquiring `_worker_lock`, which is the interleaving a bare unlocked read admits.
    """
    lf = pytest.importorskip("log_foundry")
    from log_foundry import worker as worker_mod

    sink = _CountingSink()
    lf.configure(service="t", version="0", env="t", sink=sink)
    lf.info("arms the close")

    real_lock = _lifecycle._state._lock

    class _PreemptingLock:
        """Publishes a worker while a caller is mid-acquire, as a real thread could."""

        def __init__(self) -> None:
            self.fired = False

        def __enter__(self):
            real_lock.acquire()
            if not self.fired:
                self.fired = True
                _lifecycle._state._worker = worker_mod.Worker(sink)
            return self

        def __exit__(self, *exc: object) -> None:
            real_lock.release()

    _lifecycle._state._lock = _PreemptingLock()  # type: ignore[assignment]
    try:
        _lifecycle._close_orphan_sink()
    finally:
        _lifecycle._state._lock = real_lock

    assert sink.closes == 0, (
        "the worker was published under the lock, so the close must observe it and stand down"
    )


def test_a_sink_that_raises_on_emit_is_still_closed() -> None:
    """SPEC-026 FR-001 makes total failure raise, so this is the case most likely to leak.

    An orphan-only process against a dead destination raises on every call. Arming after the
    emit would leave the socket that failure came from open forever — the exact leak FR-006
    exists to stop. A sink that raised is still a sink that was written to.
    """
    lf = pytest.importorskip("log_foundry")
    from log_foundry.sinks.base import SinkDeliveryError

    class _DeadDestination(_CountingSink):
        def emit(self, batch: list[dict]) -> None:
            raise SinkDeliveryError("delivered none of 1 event(s)")

    sink = _DeadDestination()
    lf.configure(service="t", version="0", env="t", sink=sink)

    def body() -> None:
        lf.info("never lands")

    contextvars.copy_context().run(body)
    lf.shutdown()

    assert sink.closes == 1, "the resource behind the failure is released, not leaked"


def test_a_sink_that_fails_to_construct_arms_nothing() -> None:
    """The other side of it: there is no sink, so there is nothing to close."""
    lf = pytest.importorskip("log_foundry")

    lf.configure(service="t")
    original = api._ensure_sink

    def _explode():
        raise RuntimeError("cannot build a sink")

    api._ensure_sink = _explode  # type: ignore[assignment]
    try:

        def body() -> None:
            lf.info("no sink to write to")

        contextvars.copy_context().run(body)
    finally:
        api._ensure_sink = original  # type: ignore[assignment]

    assert not _lifecycle._state._orphan_owed


def test_retired_survives_a_worker_built_after_an_orphan_only_shutdown() -> None:
    """A first review's finding: `retired` reverted to False and contradicted this API.

    An orphan-only `shutdown()` leaves `_worker` unset, so a later `@trace` builds a fresh
    worker whose own `retired` is False. Reading that alone says the process was never shut
    down — false, and it contradicts what `health()` reported one call earlier.
    """
    lf = pytest.importorskip("log_foundry")

    sink = _CountingSink()
    lf.configure(service="t", version="0", env="t", sink=sink)
    lf.info("orphan")
    lf.shutdown()
    assert lf.health().retired is True

    @lf.trace
    def traced() -> None:
        return None

    traced()
    assert _lifecycle._state._worker is not None, "a fresh worker really was built"

    assert lf.health().retired is True, "shutdown() happened; a new worker cannot un-happen it"


# -- SPEC-037 FR-001/FR-002: no info() call can fail the caller, in a span or out of one ------


def test_a_bad_value_is_absorbed_inside_a_span(lf, fake_sink, capsys) -> None:
    """FR-001 AC-1 and AC-4, and FR-002 AC-1 — the two halves of audit A2 together.

    `info(exc)` is an ordinary slip that `mypy` catches only at typed call sites. It returned
    normally on the orphan path and killed the decorated function inside a span, which is the
    same call with opposite outcomes. And the decorator's handler then recorded the span
    `status=error` with an `error.type` of `AttributeError` the caller never raised.
    """

    @lf.trace
    def work() -> str:
        lf.info(ValueError("not a string"))
        return "the function completed"

    assert work() == "the function completed", "the caller's own return value survives"

    ends = [e for e in fake_sink.events if e.get("status") is not None]
    assert ends, "the span still closed"
    assert ends[-1]["status"] == "ok", "the function returned; the span did not fail"
    assert "error" not in ends[-1], "a library-internal failure is not the caller's error"
    assert "AttributeError" in capsys.readouterr().err, "and it is still announced"


async def test_a_bad_value_is_absorbed_inside_an_async_span(lf, fake_sink) -> None:
    """FR-001 AC-1's third path. The async wrapper is a separate code path from the sync one."""

    @lf.trace
    async def work() -> str:
        lf.info(ValueError("not a string"))
        return "the coroutine completed"

    assert await work() == "the coroutine completed"


def test_a_bad_value_is_absorbed_outside_a_span(lf, fake_sink) -> None:
    """FR-001 AC-1's orphan path — already true before this FR, and it must stay true."""
    lf.info(ValueError("not a string"))


def test_the_absorbed_failure_is_announced_once_by_type_only(lf, fake_sink, capsys) -> None:
    """FR-001 AC-2 and FR-002 AC-3. Moved out of the `error` field, not hidden.

    Type only, never the message: an exception's text routinely carries the value that provoked
    it, which is the rule `arch §6` states and `_diag` exists to apply once.
    """

    @lf.trace
    def work() -> None:
        lf.info(ValueError("a secret the message would leak"))

    work()
    err = capsys.readouterr().err
    assert err.count("absorbed a failure while building an in-span log") == 1
    assert "AttributeError" in err
    assert "a secret the message would leak" not in err


@pytest.mark.parametrize("escape", [KeyboardInterrupt, SystemExit])
def test_the_operators_intent_still_reaches_the_caller(lf, fake_sink, monkeypatch, escape) -> None:
    """FR-001 AC-3. `Exception`, never `BaseException` — SPEC-025 FR-004 settled it.

    A `KeyboardInterrupt` or `SystemExit` is the operator's or the runtime's intent and must not
    be swallowed by a logging call.
    """

    def explode(*args: object, **kwargs: object) -> None:
        raise escape("the operator's intent")

    monkeypatch.setattr("log_foundry.api.build_event", explode)

    @lf.trace
    def work() -> None:
        lf.info("this will raise a BaseException from build_event")

    with pytest.raises(escape):
        work()


def test_a_span_whose_function_genuinely_raised_is_unchanged(lf, fake_sink) -> None:
    """FR-002 AC-2 and AC-4, asserted beside the ok case.

    The risk of a fix like this is that every span starts reading `ok`. SPEC-001's contract is
    that a function which raises records `status=error` with the caller's *own* exception type.
    """

    @lf.trace
    def work() -> None:
        raise RuntimeError("the caller's own failure")

    with pytest.raises(RuntimeError):
        work()

    ends = [e for e in fake_sink.events if e.get("status") is not None]
    assert ends[-1]["status"] == "error"
    assert ends[-1]["error"]["type"] == "RuntimeError", "the caller's type, not the library's"
