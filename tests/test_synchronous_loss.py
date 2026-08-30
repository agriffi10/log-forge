"""SPEC-036 FR-003 — the synchronous path reports its own loss.

A level call with no active span emits on the caller's thread. SPEC-025 guards it so a broken
destination cannot fail the caller, and until this spec nothing recorded that the event was lost:
``health()`` describes a worker, and this path has none. A process logging only this way read
``queued=0 dropped=0 failed_batches=0 stopped_reason=None`` over total, permanent loss.
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

import log_foundry
from conftest import run_concurrently
from log_foundry import decorator

if TYPE_CHECKING:
    from log_foundry.sinks.base import Sink


class Exploding:
    """A sink whose ``emit`` always raises, standing in for a dead destination."""

    def emit(self, batch: list[dict[str, object]]) -> None:
        raise RuntimeError("the destination is gone")

    def close(self) -> None:
        return None


class Recorder:
    """A sink that takes everything, for the success path."""

    def __init__(self) -> None:
        self.got: list[dict[str, object]] = []

    def emit(self, batch: list[dict[str, object]]) -> None:
        self.got.extend(batch)

    def close(self) -> None:
        return None


def test_orphan_losses_are_counted() -> None:
    """AC-1. Five level calls with no span against a raising sink leave orphan_lost == 5."""
    log_foundry.configure(service="t", sink=Exploding())
    for _ in range(5):
        log_foundry.info("this will be lost")

    assert log_foundry.health().orphan_lost == 5


def test_in_span_losses_are_counted_and_the_two_do_not_absorb_each_other() -> None:
    """AC-1a, AC-4. The counters are asserted separately, never as a sum.

    With different failure populations their total is a number nobody can act on, and pinning it
    would teach a later reader they are two halves of one counter. `info(ValueError(...))` is the
    in-span failure: `build_event` reaches `truncate_str`, which calls `value.encode` (SPEC-037).
    """
    log_foundry.configure(service="t", sink=Recorder())

    @log_foundry.trace
    def work() -> None:
        for _ in range(5):
            log_foundry.info(ValueError("not a string"))  # type: ignore[arg-type]

    work()

    health = log_foundry.health()
    assert health.in_span_lost == 5, "the in-span failures are counted"
    assert health.orphan_lost == 0, "and none of them leaked into the orphan counter"


def test_a_mixed_process_reports_each_path_separately() -> None:
    """AC-4. Orphan loss in orphan_lost, worker loss in failed_batches, neither absorbing."""
    log_foundry.configure(service="t", sink=Exploding())
    log_foundry.info("lost outside a span")

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("lost inside one, at the sink")

    work()
    log_foundry.flush(timeout=5.0)

    health = log_foundry.health()
    assert health.orphan_lost == 1, "the synchronous emit"
    assert health.failed_batches >= 1, "and the worker's abandoned batch, separately"
    assert health.in_span_lost == 0, "the in-span event was built fine; it died at the sink"


def test_a_sink_that_fails_to_construct_is_counted() -> None:
    """AC-6. An increment placed after `sink.emit` would pass AC-1 and fail this.

    The orphan guard wraps the span construction, the event build, `_ensure_sink` and the emit,
    so every way the event can be lost is inside it. A sink that cannot be built loses the event
    exactly as a sink that raises does.
    """
    log_foundry.configure(service="t", sink=Recorder())

    def explode() -> Sink:
        raise RuntimeError("the sink cannot be built")

    original = log_foundry.api._ensure_sink
    log_foundry.api._ensure_sink = explode  # type: ignore[assignment]
    try:
        log_foundry.info("lost before any sink existed")
    finally:
        log_foundry.api._ensure_sink = original  # type: ignore[assignment]

    assert log_foundry.health().orphan_lost == 1


def test_a_successful_orphan_emit_moves_nothing() -> None:
    """AC-8. The counter must not move on the success path."""
    sink = Recorder()
    log_foundry.configure(service="t", sink=sink)
    log_foundry.info("this one lands")

    assert len(sink.got) == 1
    health = log_foundry.health()
    assert health.orphan_lost == 0
    assert health.in_span_lost == 0


def test_health_reports_the_counters_with_and_without_a_worker() -> None:
    """AC-7. `health()` still creates no worker, and both branches carry the fields."""
    log_foundry.configure(service="t", sink=Exploding())
    log_foundry.info("lost, and no worker has ever been built")

    assert decorator._worker is None, "reading health must not stand up a thread"
    assert log_foundry.health().orphan_lost == 1, "the no-worker branch reports it"

    @log_foundry.trace
    def work() -> None:
        return None

    work()  # builds the worker
    assert decorator._worker is not None
    assert log_foundry.health().orphan_lost == 1, "and the worker branch still reports it"


def test_flush_is_unchanged_on_the_orphan_path() -> None:
    """AC-2. SPEC-021 settled `flush()`'s window: it reports the drain that carried the events.

    A process whose only loss was synchronous has nothing outstanding to drain, so `flush()` is
    truthy and *should* be. Cumulative loss is `health()`'s job, which is why FR-003 added a
    counter rather than changing this. Pinned so a later reader does not "fix" it.

    Qualified by SPEC-036 FR-002, which is the one thing that can now make this call falsy on the
    orphan path: a sink with a client buffer of its own is flushed here too, and if *that* fails
    the result carries `reason="sink-flush"`. `Exploding` has no `flush`, so the claim above still
    holds exactly as written for it — the qualifier is about which sink, not about the loss.
    """
    log_foundry.configure(service="t", sink=Exploding())
    log_foundry.info("lost synchronously")

    assert log_foundry.flush(timeout=2.0), "the drain carried everything it was given: nothing"
    assert log_foundry.health().orphan_lost == 1, "and the loss is visible over there instead"


def test_the_counter_lock_is_dedicated_and_not_the_worker_lock() -> None:
    """AC-5. SPEC-028's rule: a counter takes its own lock.

    `_worker_lock` is held across `Worker(_ensure_sink())` in `_get_worker`, a blocking build a
    counter increment on an arbitrary application thread must never queue behind.
    """
    assert decorator._loss_lock is not decorator._worker_lock
    assert isinstance(decorator._loss_lock, type(threading.Lock()))


def test_the_increment_happens_under_the_lock() -> None:
    """AC-5, the property that survives free-threading.

    The race is not reproducible without injecting a preemption point (SPEC-028 measured a bare
    `+=` losing zero across 1.6M concurrent increments), so what is asserted is that the counter
    is written while the lock is held — not that a bare increment happens to be safe today.

    It compares the counter across the lock's own scope. A first draft only recorded *that*
    `__exit__` ran, which passed with the increment moved outside the `with` entirely — proving
    the lock was entered and nothing about what happened inside it.
    """
    seen: dict[str, int] = {}
    real_lock = decorator._loss_lock

    class WatchingLock:
        def __enter__(self) -> None:
            real_lock.acquire()
            seen["on_enter"] = decorator._orphan_lost

        def __exit__(self, *exc: object) -> None:
            seen["on_exit"] = decorator._orphan_lost
            real_lock.release()

    decorator._loss_lock = WatchingLock()  # type: ignore[assignment]
    try:
        decorator._note_orphan_loss()
    finally:
        decorator._loss_lock = real_lock

    assert seen["on_exit"] == seen["on_enter"] + 1, (
        "the counter must change *inside* the lock's scope, not merely alongside it"
    )


def test_concurrent_orphan_losses_are_all_counted() -> None:
    """AC-5, under real contention rather than a substituted lock.

    SPEC-028's finding stands — a bare `+=` loses nothing across millions of increments on this
    interpreter — so this is not expected to catch a lost update today. It is here because the
    guarantee is about the lock, not about CPython's current bytecode granularity, and a
    free-threading build removes the property the unlocked version accidentally has.
    """
    log_foundry.configure(service="t", sink=Exploding())
    errors = run_concurrently(lambda _t, _i: log_foundry.info("lost"), threads=64)

    assert errors == [], "SPEC-025: a broken destination never reaches the caller"
    assert log_foundry.health().orphan_lost == 64


def test_the_lock_is_a_module_global_the_fork_walk_can_reach() -> None:
    """AC-5a. SPEC-039's derived fork roster picks a lock up only where the walk can write it.

    `tests/test_fork_lifecycle.py::test_every_lock_is_assigned_where_the_walk_can_reach_it`
    already names this spec's counter lock by anticipation, and it is what actually enforces
    this. What is asserted here is the single shape that lint depends on: a lock reachable as a
    module attribute, so the child's walk can rebind it. A lock built into a tuple, held in a
    closure, or created per call satisfies none of that and would be re-inherited held.

    A first draft parametrized this over the two counter names and asserted they were `int` —
    which varied nothing with the parameter and said nothing about the lock.
    """
    real = decorator._loss_lock
    used: list[str] = []

    class Marker:
        def __enter__(self) -> None:
            real.acquire()
            used.append("entered")

        def __exit__(self, *exc: object) -> None:
            real.release()

    decorator._loss_lock = Marker()  # type: ignore[assignment]
    try:
        decorator._note_orphan_loss()
        decorator._note_in_span_loss()
    finally:
        decorator._loss_lock = real

    assert used == ["entered", "entered"], (
        "both recorders must take the lock through the module global, not a private one behind "
        "it — a lock the walk can rebind but the code does not consult is not a lock at all"
    )
