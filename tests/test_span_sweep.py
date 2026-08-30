"""SPEC-036 FR-001 — ``flush()`` drains open spans.

An in-span event lives on ``span.events`` until the span *closes*; ``Worker.flush`` drains the
*queue*. So a ``flush()`` called inside a ``@trace``d function — which is where the README's
serverless recipe put it — had by construction nothing to drain. Measured on `690d2a5`: zero of
two events delivered, every counter clean, and ``FlushResult`` reporting ``reason=None``.

These tests assert against what the **sink** received. Asserting a truthy ``flush()`` instead is
vacuous: ``Worker.flush`` answers its marker from ``_nothing_lost_since`` and an empty batch
short-circuits in ``_emit``, so zero swept events still reports delivered.
"""

from __future__ import annotations

import asyncio
import collections
import contextvars
import sys
import threading

import log_foundry
from conftest import run_concurrently
from log_foundry import context, decorator
from log_foundry.model import Span


class Recorder:
    """Records what actually reached the sink."""

    def __init__(self) -> None:
        self.got: list[dict[str, object]] = []
        self._lock = threading.Lock()

    def emit(self, batch: list[dict[str, object]]) -> None:
        with self._lock:
            self.got.extend(batch)

    def close(self) -> None:
        return None

    def messages(self) -> list[object]:
        with self._lock:
            return [e.get("message") for e in self.got]


def test_the_readme_serverless_recipe_delivers_before_the_handler_returns() -> None:
    """AC-1. The headline case, in the in-span form, which is what a caller writes anyway.

    It stays a test after AC-1a moved the published example outside the span, because the shape
    is the one people reach for and it must work.
    """
    sink = Recorder()
    log_foundry.configure(service="billing-api", sink=sink)
    seen: dict[str, list[object]] = {}

    @log_foundry.trace
    def handler() -> str:
        log_foundry.info("received", records=2)
        try:
            return "ok"
        finally:
            log_foundry.flush(timeout=5.0)
            seen["delivered"] = sink.messages()

    handler()

    assert "received" in seen["delivered"], (
        f"the in-span flush delivered nothing: {seen['delivered']}"
    )


def test_a_nested_flush_drains_every_open_span_in_the_stack() -> None:
    """AC-2. Not only the innermost — the outer span's buffer is just as unreachable."""
    sink = Recorder()
    log_foundry.configure(service="t", sink=sink)
    seen: dict[str, list[object]] = {}

    @log_foundry.trace
    def inner() -> None:
        log_foundry.info("from-inner")
        log_foundry.flush(timeout=5.0)
        seen["delivered"] = sink.messages()

    @log_foundry.trace
    def outer() -> None:
        log_foundry.info("from-outer")
        inner()

    outer()

    assert "from-outer" in seen["delivered"], "the outer span was not swept"
    assert "from-inner" in seen["delivered"], "the inner span was not swept"


def test_the_span_stays_open_and_usable_after_a_sweep() -> None:
    """AC-3. Events emitted after the flush still land, and span.end is real, not fabricated."""
    sink = Recorder()
    log_foundry.configure(service="t", sink=sink)

    seen: dict[str, list[object]] = {}

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("before")
        log_foundry.flush(timeout=5.0)
        seen["at_flush"] = sink.messages()
        log_foundry.info("after")

    work()
    log_foundry.flush(timeout=5.0)

    assert "before" in seen["at_flush"], (
        "asserted inside the span: after the close, the ordinary close path delivers this "
        "whether or not a sweep ever ran"
    )
    messages = sink.messages()
    assert "before" in messages and "after" in messages
    ends = [e for e in sink.got if e.get("message") == "span.end"]
    assert len(ends) == 1, "exactly one span.end, not one per flush"
    assert ends[0].get("status") == "ok", "the real outcome, not a fabrication"
    assert isinstance(ends[0].get("duration_ms"), (int, float)), "and the real duration"


def test_no_event_is_delivered_twice_and_none_is_destroyed() -> None:
    """AC-4. The criterion a wrong implementation passes the rest of the ACs without.

    `span.events.clear()` empties the *same list object* `Worker.submit` was handed, so the
    natural reading of "clear the buffer" destroys the swept events while `flush()` still reports
    success. Measured on a nested span before the fix: 4 of 6 events gone.
    """
    sink = Recorder()
    log_foundry.configure(service="t", sink=sink)

    seen: dict[str, list[object]] = {}

    @log_foundry.trace
    def inner() -> None:
        log_foundry.info("i1")
        log_foundry.info("i2")
        log_foundry.flush(timeout=5.0)
        seen["at_flush"] = sink.messages()

    @log_foundry.trace
    def outer() -> None:
        log_foundry.info("o1")
        log_foundry.info("o2")
        inner()

    outer()
    log_foundry.flush(timeout=5.0)

    assert set(seen["at_flush"]) >= {"o1", "o2", "i1", "i2"}, (
        f"the sweep delivered nothing: {seen['at_flush']}. Asserting only after the close makes "
        "this pass against a sweep that does nothing at all, which is what it exists to catch"
    )
    messages = sink.messages()
    for name in ("o1", "o2", "i1", "i2"):
        assert messages.count(name) == 1, (
            f"{name} appears {messages.count(name)} times, not once: {messages}"
        )


def test_a_swept_span_start_keeps_its_baggage_backfill() -> None:
    """AC-5. Where SPEC-015 regresses under any in-span flush.

    That spec completes `span.start`/`span.end` at *close*, by iterating `span.events` — so a
    sweep that emptied the buffer ships `span.start` with `fields={}`. Measured: control
    `fields={'user_id': 'u42'}`, with an unbackfilled sweep `fields=None`.
    """
    sink = Recorder()
    log_foundry.configure(service="t", sink=sink)

    seen: dict[str, list[dict[str, object]]] = {}

    @log_foundry.trace
    def work() -> None:
        log_foundry.set_baggage(user_id="u42")
        log_foundry.flush(timeout=5.0)
        seen["at_flush"] = [dict(e) for e in sink.got]

    work()
    log_foundry.flush(timeout=5.0)

    swept_starts = [e for e in seen["at_flush"] if e.get("message") == "span.start"]
    assert len(swept_starts) == 1, (
        f"span.start must reach the sink at the sweep, not at the close: {seen['at_flush']}"
    )
    assert swept_starts[0].get("fields") == {"user_id": "u42"}, (
        f"the sweep cost span.start its baggage backfill: {swept_starts[0].get('fields')}"
    )
    starts = [e for e in sink.got if e.get("message") == "span.start"]
    assert len(starts) == 1, "and it is not delivered a second time at the close"


def test_a_flush_with_no_span_open_creates_no_worker() -> None:
    """AC-6. SPEC-013's refusal is narrowed, not removed: an empty flush stands up no thread."""
    log_foundry.configure(service="t", sink=Recorder())

    assert log_foundry.flush(timeout=5.0), "a process that never logged has lost nothing"
    assert decorator._worker is None, "and no worker was built to tell us so"


def test_a_sweep_that_finds_events_creates_the_worker() -> None:
    """AC-7. The cold-start path AC-1 exercises, and without it AC-1 cannot pass.

    Inside the *first* traced call `decorator._worker is None` — the worker is built when a span
    *closes* — so a sweep that submits into a worker that does not exist delivers nothing and
    still reports success.
    """
    sink = Recorder()
    log_foundry.configure(service="t", sink=sink)
    seen: dict[str, object] = {}

    @log_foundry.trace
    def handler() -> None:
        seen["worker_before"] = decorator._worker
        log_foundry.info("cold start")
        log_foundry.flush(timeout=5.0)
        seen["delivered"] = sink.messages()

    handler()

    assert seen["worker_before"] is None, "precondition: no worker exists inside the first call"
    assert "cold start" in seen["delivered"]  # type: ignore[operator]


def test_two_threads_sweeping_one_shared_span_deliver_each_event_once() -> None:
    """AC-10. Forced, not raced.

    An unforced race passes against CPython's current bytecode granularity and proves nothing,
    which is the property SPEC-028 says a test must not rest on. `contextvars` copies the same
    `Span` object into a task; a bare thread starts with a fresh context, so the shared span is
    reached here with `copy_context().run`, which is how it is reachable in an application too.

    Measured before `_sweep_lock`: unforced, 0 duplicates in 600 trials; with the window held
    open, **all 8 events delivered twice**. The detach is a load and a store, so "rarely
    preempted on today's GIL build" was the only thing making it look safe — and the floor is
    3.12, where a free-threading build removes even that.
    """
    import contextvars

    sink = Recorder()
    log_foundry.configure(service="t", sink=sink)
    span = Span(trace_id="t" * 32, span_id="s" * 16, parent_span_id=None, name="n", start_ts=0.0)
    span.events.extend({"message": f"e{i}"} for i in range(8))

    # The window is FORCED, not raced. Unforced it does not reproduce (measured 0 of 600
    # trials) — a test that races it passes on CPython's current bytecode granularity and proves
    # nothing, which is the property SPEC-028 says a test must not rest on.
    #
    # The park sits on the **swap's** load, which is each thread's *second* read of `.events`
    # (the emptiness check is the first), and holds the first thread there until the second has
    # loaded the same list. A first version parked on read 0 — before the swap, so the parked
    # thread woke and re-read an already-emptied buffer and submitted nothing — and a second
    # released on a timer rather than on the other thread, killing the no-lock mutant in only 19
    # of 20 runs.
    loads = threading.local()
    parked = threading.Event()
    second_loaded = threading.Event()
    first = threading.Event()

    class Parking(Span):
        """Parks the first thread between the swap's load and its store."""

        @property  # type: ignore[misc]
        def events(self) -> list[dict[str, object]]:
            buffered = self.__dict__["events"]
            loads.n = getattr(loads, "n", 0) + 1
            if loads.n == 2:
                if not first.is_set():
                    first.set()
                    parked.set()
                    second_loaded.wait(0.3)
                else:
                    second_loaded.set()
            return buffered

        @events.setter
        def events(self, value: list[dict[str, object]]) -> None:
            self.__dict__["events"] = value

    span.__class__ = Parking
    token = context.push_span(span)
    # One copy per thread: a Context cannot be entered by two threads at once. Both copies hold
    # the *same* Span object, which is the sharing this test is about.
    copies = [contextvars.copy_context() for _ in range(2)]
    context.pop_span(token)

    errors = run_concurrently(
        lambda t, _i: copies[t].run(decorator._sweep_open_spans), threads=2
    )
    assert parked.is_set(), "precondition: the window was never actually opened"
    log_foundry.flush(timeout=5.0)

    assert errors == [], f"the sweep raised: {errors}"
    messages = sink.messages()
    for i in range(8):
        assert messages.count(f"e{i}") == 1, (
            f"e{i} appears {messages.count(f'e{i}')} times — the swap must be all-or-nothing"
        )


async def test_the_sweep_reaches_only_the_calling_context() -> None:
    """AC-9. The honest bound, pinned so it cannot silently widen or narrow.

    `contextvars` gives no way to enumerate another task's context, so a `flush()` in a handler
    that fanned out does not reach what those tasks buffered.
    """
    sink = Recorder()
    log_foundry.configure(service="t", sink=sink)
    buffered = asyncio.Event()
    release = asyncio.Event()

    @log_foundry.trace
    async def other_task() -> None:
        log_foundry.info("in-another-task")
        buffered.set()
        await release.wait()

    task = asyncio.get_running_loop().create_task(other_task())
    await buffered.wait()

    seen: dict[str, list[object]] = {}

    @log_foundry.trace
    async def here() -> None:
        log_foundry.info("in-this-context")
        log_foundry.flush(timeout=5.0)
        seen["at_flush"] = sink.messages()

    await here()

    assert "in-this-context" in seen["at_flush"], (
        "positive control: without it the negative below passes against a sweep that does "
        "nothing at all, which pins no bound"
    )
    assert "in-another-task" not in seen["at_flush"], (
        "the bound widened: a flush reached another task's open span"
    )

    release.set()
    await task
    log_foundry.flush(timeout=5.0)
    assert "in-another-task" in sink.messages(), "and it still arrives when that span closes"


def test_a_sweep_racing_a_span_close_delivers_each_event_once() -> None:
    """`_sweep_lock` serializes sweeps against sweeps; only a tight detach holds against a close.

    `_close_span` detaches the same attribute and does **not** take that lock, so whatever sits
    between the sweep's load and its store is exposed to it. A draft hoisted the load to the top
    of the loop so a test could park on it, which put `_get_worker()` — and therefore
    `Thread.start()` — inside the gap: measured, the whole batch delivered twice, two `span.end`
    events among them, in 28 of 100 unforced trials here and 67 of 100 on the reviewing machine.
    With the detach back to one statement: 0 of 100 in both places.

    Unforced on purpose. Forcing this window needs a preemption inside a single bytecode pair,
    and the honest instrument is repetition at a tightened switch interval — which is what
    reproduced the defect in the first place.
    """
    def one_trial() -> Recorder:
        """One span whose close races a sweep on a thread sharing the very same Span object."""
        decorator._worker = None
        sink = Recorder()
        log_foundry.configure(service="t", sink=sink)
        barrier = threading.Barrier(2)
        holder: dict[str, contextvars.Context] = {}

        def sweep() -> None:
            barrier.wait()
            holder["ctx"].run(decorator._sweep_open_spans)

        @log_foundry.trace
        def work() -> threading.Thread:
            for i in range(4):
                log_foundry.info(f"e{i}")
            # Copied *inside* the span, so the sweeping thread sees it on its stack.
            holder["ctx"] = contextvars.copy_context()
            sweeper = threading.Thread(target=sweep)
            sweeper.start()
            barrier.wait()  # the close and the sweep are released together
            return sweeper

        work().join(5.0)
        log_foundry.flush(timeout=5.0)
        return sink

    original = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        for _ in range(30):
            counts = collections.Counter(e.get("message") for e in one_trial().got)
            duplicated = {k: v for k, v in counts.items() if v > 1}
            assert not duplicated, (
                f"a sweep racing the close delivered events twice: {duplicated}"
            )
    finally:
        sys.setswitchinterval(original)


def test_the_close_path_detaches_under_the_same_lock_as_the_sweep() -> None:
    """Both detaches rebind `span.events`, so both must take `_sweep_lock`.

    One statement makes the gap narrow, not closed: it compiles to LOAD_ATTR ... STORE_ATTR with
    no CALL between, so today's GIL cannot switch inside it — measured 0 of 500 unforced trials,
    and 10 of 10 with an opcode-level preemption injected. `requires-python` has no upper bound,
    so a free-threaded build removes that accident entirely. The lock measured within noise on
    the traced path (+0.4% on one thread, +0.9% across eight, 20,000 spans each), which is why
    relying on the width of the window was the wrong trade.

    Asserted structurally rather than by racing it, because the race is exactly what a test
    cannot force without an opcode-level instrument, and a test that merely races it passes on
    today's build whether or not the lock is there.
    """
    taken: list[str] = []
    real = decorator._sweep_lock

    class Watching:
        def __enter__(self) -> None:
            real.acquire()
            taken.append("held")

        def __exit__(self, *exc: object) -> None:
            real.release()

    sink = Recorder()
    log_foundry.configure(service="t", sink=sink)
    decorator._sweep_lock = Watching()  # type: ignore[assignment]
    try:

        @log_foundry.trace
        def work() -> None:
            log_foundry.info("e0")

        work()
    finally:
        decorator._sweep_lock = real

    assert taken == ["held"], (
        "the close path's detach must hold the lock the sweep holds, or a span swept and closed "
        "concurrently delivers its whole batch twice"
    )


def test_a_sweep_whose_worker_cannot_be_built_destroys_nothing() -> None:
    """The worker is resolved *before* the buffer is detached, and the order is the guarantee.

    `_get_worker()` can raise — it ends in `Thread.start()` — and a detach that already happened
    leaves the events in a discarded local while the span reads empty and `flush()` reports
    success. Measured with the failure injected and the detach first: **3 of 4 events destroyed**,
    every counter zero, on a span that was still open and would have delivered them at its close.
    That is the SPEC-017/026 shape inside the spec built to remove it.

    `Worker.submit` documents `Raises: None.` and swallows `queue.Full`, so once it is reached
    the batch is safe. Everything that can fail is therefore in front of the detach.
    """
    sink = Recorder()
    log_foundry.configure(service="t", sink=sink)
    span = Span(trace_id="t" * 32, span_id="s" * 16, parent_span_id=None, name="n", start_ts=0.0)
    span.events.extend({"message": f"e{i}"} for i in range(4))

    def refuse() -> object:
        raise RuntimeError("can't start new thread")

    real = decorator._get_worker
    decorator._get_worker = refuse  # type: ignore[assignment]
    token = context.push_span(span)
    try:
        result = log_foundry.flush(timeout=5.0)
    finally:
        context.pop_span(token)
        decorator._get_worker = real  # type: ignore[assignment]

    assert [e["message"] for e in span.events] == ["e0", "e1", "e2", "e3"], (
        f"the span must still hold its events, so its close can deliver them: {span.events}"
    )
    assert not result, (
        "and the caller is told: reporting success over a sweep that handed nothing over is the "
        "exact shape this spec exists to remove, and on the cold-start path there may be no "
        "close to carry them"
    )
    assert result.reason == "abandoned", f"an existing token, not a new one: {result.reason}"


def test_a_span_with_an_empty_buffer_is_still_marked_swept() -> None:
    """The branch that submits nothing still has to record that a flush passed over it.

    Otherwise a `continue_trace()` after a flush that happened to find the buffer empty would
    adopt and re-parent — and the events already delivered by an *earlier* sweep of the same
    span would keep the old trace id. Dropping the flag on this branch changed no other test.
    """
    log_foundry.configure(service="t", sink=Recorder())
    span = Span(trace_id="t" * 32, span_id="s" * 16, parent_span_id=None, name="n", start_ts=0.0)

    token = context.push_span(span)
    try:
        decorator._sweep_open_spans()
    finally:
        context.pop_span(token)

    assert span.swept, "a flush passed over this span, even though it had nothing to hand over"


def test_continue_trace_is_not_refused_inside_a_swept_child_span() -> None:
    """The refusal is keyed on a swept **root**, which is what the re-parent acts on.

    The child here *is* swept — the flush sweeps the whole stack — and that is the point: the
    guard must agree with `_reparent_current_span`, which returns early on any span with a
    parent and would therefore have rewritten nothing. Refusing there fires where the thing it
    guards does not run.

    An earlier name said "unswept", which is the case this does not cover and cannot: a flush
    inside a child sweeps that child too. This is the only test that kills a revert of the
    `parent_span_id is None` half of the guard.
    """
    log_foundry.configure(service="t", sink=Recorder())
    inbound = "00-" + "e" * 32 + "-" + "f" * 16 + "-01"
    seen: dict[str, object] = {}

    @log_foundry.trace
    def child() -> None:
        log_foundry.info("in-child")
        log_foundry.flush(timeout=5.0)
        seen["result"] = log_foundry.continue_trace(inbound)

    @log_foundry.trace
    def root() -> None:
        child()

    root()

    assert seen["result"], (
        "a swept child must not refuse: the re-parent declines on a non-root anyway, so the "
        "refusal would cost an adoption and prevent no corruption"
    )


def test_continue_trace_refuses_the_context_after_a_sweep_but_keeps_the_baggage() -> None:
    """AC-11a, AC-11b. One span must not carry two trace ids.

    `_reparent_current_span` adopts a context by rewriting the events still *buffered* on the
    open root span. Swept events have left that buffer, so an adoption after a sweep leaves the
    swept events on the old trace and everything after on the inbound one — what that function's
    docstring calls "worse than no continuation at all because it looks like data rather than a
    bug", and the SPEC-024 category of wrong data rather than lost data.

    The baggage merge still runs, per SPEC-014: losing correlating fields is bad, and losing the
    trace join because one field was malformed is worse.
    """
    sink = Recorder()
    log_foundry.configure(service="t", sink=sink)
    inbound = "00-" + "a" * 32 + "-" + "b" * 16 + "-01"
    seen: dict[str, object] = {}

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("before-sweep")
        log_foundry.flush(timeout=5.0)
        seen["result"] = log_foundry.continue_trace(inbound, baggage="tenant=acme")
        log_foundry.info("after-refusal")

    work()
    log_foundry.flush(timeout=5.0)

    result = seen["result"]
    assert not result, "the trace context is refused"
    assert result.reason == "rejected", f"and announced as one, not silently: {result.reason}"  # type: ignore[union-attr]

    trace_ids = {e.get("trace_id") for e in sink.got}
    assert len(trace_ids) == 1, f"one span, one trace id — got {trace_ids}"
    assert "a" * 32 not in trace_ids, "the inbound trace was not adopted"

    after = [e for e in sink.got if e.get("message") == "after-refusal"]
    assert after and after[0].get("fields") == {"tenant": "acme"}, (
        f"the baggage merge must still run: {after[0].get('fields') if after else None}"
    )


def test_continue_trace_on_the_first_line_is_unaffected() -> None:
    """AC-11a's other order: the documented placement adopts normally, nothing having been swept."""
    sink = Recorder()
    log_foundry.configure(service="t", sink=sink)
    inbound = "00-" + "c" * 32 + "-" + "d" * 16 + "-01"
    seen: dict[str, object] = {}

    @log_foundry.trace
    def work() -> None:
        seen["result"] = log_foundry.continue_trace(inbound)
        log_foundry.info("adopted")
        log_foundry.flush(timeout=5.0)

    work()
    log_foundry.flush(timeout=5.0)

    assert seen["result"], "nothing had been swept, so the adoption stands"
    assert {e.get("trace_id") for e in sink.got} == {"c" * 32}, "every event joined the inbound trace"
