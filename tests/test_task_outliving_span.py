"""SPEC-036 FR-004 — an event from a task that outlives its span is not lost.

``contextvars`` copies the *same* ``Span`` object into every task created inside a span, and
``submit`` handed the live list to the queue. A fire-and-forget ``create_task`` that logs after
its parent span closed therefore appended to a list that had already been emitted: under the
worker's flush interval the event was delivered but ordered after ``span.end``, and over the
interval it was silently lost. A pure race on a timer.

These tests deliberately do **not** use the ``lf`` fixture: `tests/conftest.py` monkeypatches
``decorator._flush`` with a synchronous stand-in that calls ``_ensure_sink().emit(span.events)``
and never detaches, so a detach test written on that fixture cannot fail.
"""

from __future__ import annotations

import asyncio

import pytest

import log_foundry
from log_foundry import _lifecycle, decorator
from log_foundry.model import Span
from log_foundry.worker import Worker


def _install_worker(sink: Recorder, interval: float) -> Worker:
    """Installs the process worker directly, since `flush_interval` is a Worker argument.

    `configure()` does not expose it, and the interval is the whole variable AC-1 sweeps.
    """
    log_foundry.configure(service="t", sink=sink)
    worker = Worker(sink, batch_size=1000, flush_interval=interval)
    _lifecycle._state._worker = worker
    return worker


class Recorder:
    """Records everything it is given, so delivery is counted at the sink."""

    def __init__(self) -> None:
        self.got: list[dict[str, object]] = []

    def emit(self, batch: list[dict[str, object]]) -> None:
        self.got.extend(batch)

    def close(self) -> None:
        return None


@pytest.mark.parametrize("interval", [0.01, 10.0])
async def test_the_outcome_no_longer_depends_on_the_flush_interval(interval: float) -> None:
    """AC-1. The same code at both intervals produces the same result.

    Before the fix this was the whole defect: at 0.01 s the late event was delivered (misordered
    inside its own span), at 10 s it was gone. Nothing about the caller's code decided which.

    **The parent's batch must be emitted before the late task logs, and that is what makes this
    test say anything.** A first draft closed the span, let the task run, and only then called
    `flush()` — which passed against the unfixed library at *both* intervals, because the worker
    still held the span's live list and the forced drain emitted it *after* the late append, so
    the event rode along on the very list the defect is about. The drain has to happen in
    between, which is what the interval decides in production and what the explicit `flush()`
    below makes deterministic here.
    """
    sink = Recorder()
    _install_worker(sink, interval)
    released = asyncio.Event()

    @log_foundry.trace
    async def parent() -> None:
        async def late() -> None:
            await released.wait()
            log_foundry.info("logged after the parent span closed")

        asyncio.get_running_loop().create_task(late())

    await parent()
    log_foundry.flush(timeout=5.0)  # the parent's batch reaches the sink and is done with
    assert any(e.get("message") == "span.end" for e in sink.got), (
        "precondition: the span's batch must already be emitted, or this proves nothing"
    )

    released.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    log_foundry.flush(timeout=5.0)

    messages = [e.get("message") for e in sink.got]
    assert "logged after the parent span closed" in messages, (
        f"the late event was lost at interval={interval}: {messages}"
    )


async def test_a_late_append_is_not_silently_dropped_and_takes_the_orphan_route() -> None:
    """AC-2. Decided in `api._log` at append time against the closed flag, not after the fact.

    A post-hoc check of the buffer cannot satisfy this: nothing in the library reads a span again
    after `_close_span` submits and returns. The event becomes a fresh one-event span, so it
    carries a **different trace_id** than the span it was logically part of — the cost FR-004
    states rather than hides.
    """
    sink = Recorder()
    _install_worker(sink, 10.0)
    captured: dict[str, str] = {}
    released = asyncio.Event()

    @log_foundry.trace
    async def parent() -> None:
        captured["trace_id"] = str(log_foundry.current_trace_context()[0])

        async def late() -> None:
            await released.wait()
            log_foundry.info("late")

        asyncio.get_running_loop().create_task(late())

    await parent()
    released.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    log_foundry.flush(timeout=5.0)

    late = [e for e in sink.got if e.get("message") == "late"]
    assert len(late) == 1, f"delivered exactly once, not lost and not duplicated: {sink.got}"
    assert late[0]["trace_id"] != captured["trace_id"], (
        "it takes the orphan route, so it gets a fresh trace_id — stated, not hidden"
    )


async def test_an_event_from_a_task_inside_the_span_still_lands_in_it() -> None:
    """AC-3. The ordinary case is untouched: a task logging within the span's lifetime."""
    sink = Recorder()
    _install_worker(sink, 10.0)
    captured: dict[str, str] = {}

    @log_foundry.trace
    async def parent() -> None:
        captured["trace_id"] = str(log_foundry.current_trace_context()[0])

        async def inner() -> None:
            log_foundry.info("inside")

        await asyncio.get_running_loop().create_task(inner())

    await parent()
    log_foundry.flush(timeout=5.0)

    inside = [e for e in sink.got if e.get("message") == "inside"]
    assert len(inside) == 1
    assert inside[0]["trace_id"] == captured["trace_id"], "still in its parent's trace"


def test_the_detach_duplicates_nothing() -> None:
    """AC-4. Every event delivered exactly once across a normal span close.

    A **control**: it passes against every FR-004 mutant, because with the closed-flag routing in
    place nothing observable depends on the detach here. That is not a reason to drop it — AC-4's
    risk is a future change that destroys events, and this is what would catch it. The detach's
    own guard is `test_the_span_is_left_a_fresh_list_rather_than_a_cleared_one`, and its real
    justification arrives with FR-001's sweep.
    """
    sink = Recorder()
    _install_worker(sink, 10.0)

    @log_foundry.trace
    def work() -> None:
        for i in range(4):
            log_foundry.info(f"event-{i}")

    work()
    log_foundry.flush(timeout=5.0)

    messages = [e.get("message") for e in sink.got]
    for i in range(4):
        assert messages.count(f"event-{i}") == 1, f"event-{i} appears {messages.count(f'event-{i}')} times"


def test_the_span_is_left_a_fresh_list_rather_than_a_cleared_one() -> None:
    """AC-4, at the mechanism. The swap is what makes the detach free and correct.

    `submit(span.events)` then `span.events.clear()` empties the *same list object* the worker was
    handed, which destroys the batch. The swap hands over the old list and rebinds the attribute,
    so what the worker holds can never be reached through the span again.
    """
    submitted: list[list[dict[str, object]]] = []

    class Spy:
        def submit(self, events: list[dict[str, object]]) -> None:
            submitted.append(events)

    span = Span(
        trace_id="t", span_id="s", parent_span_id=None, name="n", start_ts=0.0,
        events=[{"message": "one"}],
    )
    original = span.events
    real_get_worker = _lifecycle._get_worker
    _lifecycle._get_worker = lambda: Spy()  # type: ignore[assignment]
    try:
        decorator._flush(span)
    finally:
        _lifecycle._get_worker = real_get_worker  # type: ignore[assignment]

    assert submitted == [[{"message": "one"}]], "the worker got the events"
    assert submitted[0] is original, "by reference — the old list itself"
    assert span.events == [], "and the span was left an empty one"
    assert span.events is not original, "a *fresh* list, not the one the worker now owns"


def test_closed_is_set_before_the_flush_so_a_failing_flush_still_routes() -> None:
    """AC-2, the failure path. `_flush` can raise and `_end` absorbs it.

    Setting the flag after the flush would leave it False on exactly the runs where a later
    append most needs the orphan route — the event would land in a dead buffer with no counter
    moving. `sinks/base.py` settles the general form: set it before releasing anything.
    """
    seen: dict[str, bool] = {}

    def exploding_flush(span: Span) -> None:
        seen["closed_when_flush_ran"] = span.closed
        raise RuntimeError("the worker could not be built")

    sink = Recorder()
    log_foundry.configure(service="t", sink=sink)
    real_flush = decorator._flush
    decorator._flush = exploding_flush  # type: ignore[assignment]
    try:

        @log_foundry.trace
        def work() -> None:
            log_foundry.info("in the span")

        work()  # the decorator absorbs the flush failure; the caller is not failed
    finally:
        decorator._flush = real_flush  # type: ignore[assignment]

    assert seen["closed_when_flush_ran"] is True, (
        "closed must already be set when _flush runs, or a raising flush leaves it False"
    )
