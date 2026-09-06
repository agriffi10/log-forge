"""SPEC-044 — the five lifecycle races, each pinned by the reproduction that found it.

Every test here was first run against the tree *before* its fix and observed to fail. The
harnesses that found them (`h17`, `h4`, `h2`, `h2b`, `h1`, `h11`) were scratch files, named in
`architecture.md` §13 until SPEC-045 compressed that entry to its closure record; committing the
reproductions here is what stops the next reader measuring a criterion against whatever they
rebuild from its prose, and is why the entry no longer needs to name them.

Four of the five need an **injected preemption point**, because the window is a few instructions
wide: the idiom is `tests/test_orphan_sink_handoff.py`'s — patch a function the racing path calls
while it holds the lifecycle lock, and park it on an `Event`. A `sleep` would not do, and neither
would an unforced rate: race 4 measured 0/120 without a preemption point.
"""

from __future__ import annotations

import ast
import gc
import os
import pathlib
import threading
import time
from typing import TYPE_CHECKING

import pytest

import log_foundry
from log_foundry import _fork, _lifecycle

if TYPE_CHECKING:
    from collections.abc import Iterator

from log_foundry import api
from log_foundry.sinks import _retry as retry
from log_foundry.sinks import stdout as stdout_sink

_LIFECYCLE_SRC = pathlib.Path(_lifecycle.__file__)
_DRAIN_NAME = "log-foundry-worker"


class CountingSink:
    """Counts closes and emits, records the event count at each close, and can park inside one.

    `events_at_close` is what distinguishes a second close that **follows** the delivery it
    discharges — SPEC-045 FR-002's one close per write-epoch — from the double it would otherwise
    be indistinguishable from, since both read `closes == 2` (SPEC-054 FR-002).

    It declares `log_foundry_stop_signal` because `offer_stop_signal` probes for it with
    `hasattr` (SPEC-027 FR-002) — a sink without the attribute simply never receives one, so a
    double that omits it makes every signal assertion vacuously unreachable rather than false.
    """

    log_foundry_stop_signal: threading.Event | None = None

    def __init__(self, name: str = "sink", close_seconds: float = 0.0) -> None:
        self.name = name
        self.closes = 0
        self.events = 0
        self.events_at_close: list[int] = []
        self.close_seconds = close_seconds
        self.in_close = threading.Event()
        self.may_finish: threading.Event | None = None
        self._lock = threading.Lock()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Counts the batch and keeps nothing; these tests assert on closes, not deliveries."""
        with self._lock:
            self.events += len(batch)

    def close(self) -> None:
        """Records the close, optionally parking so a concurrent call lands inside it.

        Only the **first** close parks. A test that holds one close open while driving a swap
        gets a second close of the same sink from `_close_swapped_out`, and parking that one too
        would deadlock the swap against the gate the test has not opened yet.
        """
        with self._lock:
            self.closes += 1
            self.events_at_close.append(self.events)
            first = self.closes == 1
        self.in_close.set()
        if self.may_finish is not None and first:
            self.may_finish.wait(10.0)
        if self.close_seconds:
            time.sleep(self.close_seconds)

    def __repr__(self) -> str:
        """Names the sink and its counts, so an assertion failure reads without a debugger."""
        return f"<{self.name} closes={self.closes} events={self.events}>"


class _PreemptInGetWorker:
    """Parks the first `@trace` inside `_get_worker`'s critical section, holding the lock.

    `_register_exit_handler` is called under `_state._lock` and only from there, which is what
    makes it the right place to stand: the patch is inside the window every one of these races
    needs, and it does not have to simulate the window.
    """

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.inside = threading.Event()
        self.go = threading.Event()
        real = _lifecycle._register_exit_handler

        def preempting() -> None:
            real()
            self.inside.set()
            self.go.wait(10.0)

        monkeypatch.setattr(_lifecycle, "_register_exit_handler", preempting)
        self._monkeypatch = monkeypatch
        self._real = real

    def wait_until_held(self) -> None:
        """Blocks until the racing thread is inside the lock, then unpatches."""
        assert self.inside.wait(5.0), "the first @trace never reached _get_worker's lock"
        self._monkeypatch.setattr(_lifecycle, "_register_exit_handler", self._real)

    def release(self) -> None:
        """Lets the parked thread finish building its worker."""
        self.go.set()


_PRE_EXISTING_DRAINS: set[int] = set()


@pytest.fixture(autouse=True)
def _ignore_drain_threads_this_file_did_not_start() -> Iterator[None]:
    """Snapshots the drain threads already running, so this file's census counts only its own.

    A process-global `threading.enumerate()` census is a census of the whole session, not of the
    worker the test built — and another file leaking a worker makes these assertions fail with a
    message pointing at the library. Measured: `test_span_sweep.py` discarded thirty workers
    without shutting them down, so the three FR-001 tests passed only because file names sort
    `l` before `s`. That leak is fixed at source too, but a test that depends on another file's
    hygiene is passing for the wrong reason either way.
    """
    _PRE_EXISTING_DRAINS.clear()
    _PRE_EXISTING_DRAINS.update(
        id(thread) for thread in threading.enumerate() if thread.name == _DRAIN_NAME
    )
    yield


def _drain_threads() -> list[str]:
    """Returns the live drain threads this test started, so a leak of its own is legible."""
    return [
        thread.name
        for thread in threading.enumerate()
        if thread.name == _DRAIN_NAME and id(thread) not in _PRE_EXISTING_DRAINS
    ]


# --------------------------------------------------------------------------- FR-001


def test_a_shutdown_racing_a_first_trace_stops_the_worker_it_built(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-001 AC-1/AC-2. `h17`: the worst of the six, and confirmed independently.

    `_shutdown_worker` read the existence question unlocked, so a worker built after that read
    sent it down the no-worker branch: the drain thread was never stopped and its sink never
    closed, while `health()` reported `retired=True`. `atexit` recovered it in a process that
    exits — a frozen serverless container never does, which is the deployment `shutdown()`
    exists for. Measured on the pre-fix tree: `drain thread alive=True`.
    """
    sink = CountingSink("A")
    log_foundry.configure(service="t", sink=sink)
    log_foundry.info("arm the orphan record")

    preempt = _PreemptInGetWorker(monkeypatch)

    @log_foundry.trace
    def work() -> int:
        return 1

    tracer = threading.Thread(target=work)
    tracer.start()
    preempt.wait_until_held()

    shutdown = threading.Thread(target=lambda: log_foundry.shutdown(timeout=5.0))
    shutdown.start()
    shutdown.join(0.5)
    assert shutdown.is_alive(), "the shutdown must be queued on the lifecycle lock, not done"
    preempt.release()
    tracer.join(10.0)
    shutdown.join(20.0)

    assert not _drain_threads(), "shutdown() left a live drain thread nothing will stop"
    assert log_foundry.health().retired, "and health() must still report the shutdown"


def test_the_racing_shutdown_closes_the_sink_once_when_the_worker_registers_first(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-001 AC-3, the ordering where `_get_worker` reaches the lock **before** the shutdown.

    The shutdown then finds a worker and takes the worker branch, so the sink is the worker's to
    close and `_close_orphan_sink`'s ownership guard declines. Nothing new is needed for this
    ordering — it is here because AC-3 names both, and because a fix for the other one must not
    turn this into a second close.
    """
    sink = CountingSink("A")
    log_foundry.configure(service="t", sink=sink)
    log_foundry.info("arm the orphan record")

    preempt = _PreemptInGetWorker(monkeypatch)

    @log_foundry.trace
    def work() -> int:
        return 1

    tracer = threading.Thread(target=work)
    tracer.start()
    preempt.wait_until_held()
    shutdown = threading.Thread(target=lambda: log_foundry.shutdown(timeout=5.0))
    shutdown.start()
    shutdown.join(0.5)
    assert shutdown.is_alive(), "the shutdown must be queued on the lifecycle lock, not done"
    preempt.release()
    tracer.join(10.0)
    shutdown.join(20.0)

    assert _lifecycle._state.worker_exists() is not None, "the race built one"
    log_foundry.shutdown(timeout=5.0)  # the exit path, run explicitly
    assert sink.closes == 1, f"closed {sink.closes} times, not once: {sink}"


def test_the_racing_shutdown_closes_the_sink_twice_when_the_orphan_branch_closes_first() -> None:
    """SPEC-054 FR-002 AC-4 re-states this from one close to two, and says why.

    ~~Without `sink_released` the late worker owns a close of its own and performs a **second**
    `close()`~~ — struck (SPEC-021). The ordering is unchanged: the shutdown wins the first
    critical section, finds no worker, raises the counter and closes the sink; the `@trace`
    builds its worker only once that close is already underway, over the very sink just
    released. What changed is the verdict on the second close. `Worker(sink_released=)` made the
    late worker inherit a discharged close so there would be exactly one; under one owed-close
    record that flag has no work to do, because the late worker's **build arms its sink** and the
    closer's second pass closes it *after* that worker's drain.

    Two closes, and the second is not a double: it follows a delivery the first close could not
    have carried, which is SPEC-045 FR-002's one-close-per-write-epoch rule applied to the site
    that still contradicted it. The event count at each close is what says so, and it is asserted
    rather than the bare count — a second close that landed *before* the late worker's events
    would be the defect this replaced, wearing the same number.

    The preemption point is the sink's own `close()`, which is where the shutdown provably is
    after its first critical section — no patching required, and it cannot drift out of the
    window the way a patched helper can.
    """
    sink = CountingSink("A")
    sink.may_finish = threading.Event()
    log_foundry.configure(service="t", sink=sink)
    log_foundry.info("arm the orphan record")

    shutdown = threading.Thread(target=lambda: log_foundry.shutdown(timeout=5.0))
    shutdown.start()
    assert sink.in_close.wait(5.0), "the shutdown reached the orphan close having found no worker"
    assert id(sink) not in _lifecycle._state._owed, "and discharged it before closing"

    @log_foundry.trace
    def work() -> int:
        log_foundry.info("delivered by the late worker")

    work()  # builds a worker over the sink that is being closed right now
    worker = _lifecycle._state.worker_exists()
    assert worker is not None, "the late worker was built inside the shutdown's window"
    assert _lifecycle._state._late_worker is worker, "and registered for the running shutdown"
    assert id(sink) in _lifecycle._state._owed, "and its build re-armed the sink"

    sink.may_finish.set()
    shutdown.join(20.0)

    assert not _drain_threads(), "the shutdown drained the worker it did not know it would build"
    assert sink.closes == 2, (
        f"the first shutdown's second pass closes it again after the late worker's drain: {sink}"
    )
    assert sink.events_at_close[1] > sink.events_at_close[0], (
        "and the second close follows the delivery it discharges, which is what makes it one "
        f"close per write-epoch rather than a double: {sink.events_at_close}"
    )

    log_foundry.shutdown(timeout=5.0)  # the exit path
    assert sink.closes == 2, f"and nothing runs a third: {sink}"


def test_a_late_worker_that_swaps_still_closes_the_sink_it_adopted() -> None:
    """SPEC-044 FR-001 AC-3's other half, kept after SPEC-054 FR-002 removed its mechanism.

    ~~`Worker._sink_closed` is a worker-lifetime flag, so a worker born with a discharged close
    keeps that claim across every later swap unless it is reset~~ — struck (SPEC-021): the flag
    is retired, because under one owed-close record a worker's build arms its sink and a swap
    arms the next one, so there is no claim to carry or reset. The **observable** is what this
    keeps: the sink a late worker adopts mid-shutdown is closed by somebody. For a sink whose
    `close()` *is* its delivery, a `KafkaSink` flushing its producer, the alternative is a
    silently lost buffer with `health()` reading clean.

    Reaching it needs all three of a shutdown in flight, a worker born inside that window, and a
    swap while that worker is still live — which is why the shutdown is parked inside the orphan
    sink's own `close()` for the whole sequence. Measured against the unreset flag:
    `B.closes == 0`, and nothing else in the suite saw it.
    """
    a, b = CountingSink("A"), CountingSink("B")
    a.may_finish = threading.Event()
    log_foundry.configure(service="t", sink=a)
    log_foundry.info("arm the orphan record")

    shutdown = threading.Thread(target=lambda: log_foundry.shutdown(timeout=None))
    shutdown.start()
    assert a.in_close.wait(5.0), "the shutdown is inside the orphan close"

    @log_foundry.trace
    def work() -> int:
        return 1

    work()  # the late worker, born over a sink already released
    worker = _lifecycle._state.worker_exists()
    assert worker is not None, "the late worker was built inside the shutdown's window"

    log_foundry.configure(sink=b)  # it is still live, so this is a real swap
    assert worker.sink is b, "the live late worker adopted B"

    a.may_finish.set()
    shutdown.join(20.0)
    _lifecycle.join_closers(time.monotonic() + 5.0)

    assert b.closes == 1, (
        f"the sink adopted after the discharged-close claim must still be closed: {b}"
    )


def test_a_worker_built_after_the_shutdown_returned_owns_its_sinks_close() -> None:
    """SPEC-044 FR-001 AC-5's rule, asserted on the record now that the flag is gone.

    ~~The discharged-close claim is gated on the shutdown window~~ — struck (SPEC-021):
    SPEC-054 FR-002 retired the claim, so there is no gate left to test. The rule it protected
    stands and is what is asserted here: a worker built **after** a shutdown returned owns its
    sink's close, so its build arms that sink and a later `shutdown()` performs it.

    That is now the same statement on both paths, which is the point of one record — the
    sequential case and the racing one differ in *when* the arming happens and in nothing else.
    """
    sink = CountingSink("A")
    log_foundry.configure(service="t", sink=sink)
    log_foundry.info("before the shutdown")
    log_foundry.shutdown()
    assert sink.closes == 1, "the orphan branch closed it"
    assert id(sink) not in _lifecycle._state._owed, "and discharged it"
    assert _lifecycle._state._shutdown_running == 0, "and the shutdown has returned"

    @log_foundry.trace
    def work() -> int:
        log_foundry.info("delivered after the shutdown returned")

    work()
    worker = _lifecycle._state.worker_exists()
    assert worker is not None, "the sequential case still builds a live worker"
    assert id(worker.sink) in _lifecycle._state._owed, (
        "a worker built outside a running shutdown owns its sink's close, so its build arms it"
    )

    log_foundry.shutdown(timeout=5.0)
    assert sink.closes == 2, f"and the next shutdown performs that close: {sink}"
    assert sink.events_at_close[1] > sink.events_at_close[0], (
        "one close per write-epoch: the second follows the events the late worker delivered"
    )


def test_the_racing_shutdown_returns_within_its_own_timeout() -> None:
    """FR-001 AC-6. The late worker's drain is charged against this call's deadline.

    It has to be driven the way the orphan-branch-closes-first test is. Parking inside
    `_get_worker` leaves the shutdown queued on the lifecycle lock, so it finds a worker and
    takes the **worker** branch — the late-worker drain never runs and the deadline arithmetic
    is never exercised. Measured: with that shape, replacing the late drain with a `raise` left
    the test green, and making it fully unbounded left the whole suite green.

    Here the shutdown is parked inside the orphan sink's own `close()`, past its first critical
    section, and the late worker is given a sink whose `emit` outlasts the budget — so a drain
    charged against a fresh deadline would visibly overrun.
    """
    budget = 1.0
    blocking = 6.0
    let_emit_finish = threading.Event()

    class SlowEmitSink:
        """Takes longer over one emit than the shutdown's whole budget.

        It blocks on an `Event` rather than sleeping a fixed span so the test can release it at
        the end: an expired `shutdown()` leaves the drain thread running by design, and a thread
        still inside a six-second sleep outlives this test and breaks the next one's
        `_drain_threads()` assertion. Found exactly that way.
        """

        log_foundry_stop_signal: threading.Event | None = None

        def __init__(self) -> None:
            self.closes = 0

        def emit(self, batch: list[dict[str, object]]) -> None:
            """Blocks past the budget, so an uncharged drain cannot hide."""
            let_emit_finish.wait(blocking)

        def close(self) -> None:
            """Counts the close; the assertion here is on elapsed time."""
            self.closes += 1

    orphan = CountingSink("orphan")
    orphan.may_finish = threading.Event()
    log_foundry.configure(service="t", sink=orphan)
    log_foundry.info("arm the orphan record")

    elapsed: list[float] = []

    def timed_shutdown() -> None:
        start = time.monotonic()
        log_foundry.shutdown(timeout=budget)
        elapsed.append(time.monotonic() - start)

    shutdown = threading.Thread(target=timed_shutdown)
    shutdown.start()
    assert orphan.in_close.wait(5.0), "the shutdown is inside the orphan close, past its latch"

    log_foundry.configure(sink=SlowEmitSink())

    @log_foundry.trace
    def work() -> int:
        return 1

    work()  # the late worker, registered for the running shutdown, on the slow sink
    assert _lifecycle._state._late_worker is not None, "the late worker was registered"

    orphan.may_finish.set()
    shutdown.join(30.0)

    try:
        assert elapsed, "the shutdown returned"
        assert elapsed[0] < blocking, (
            f"shutdown(timeout={budget}) took {elapsed[0]:.2f}s against a {blocking}s emit — the "
            "late worker's drain must be charged against this call's deadline, not a fresh one"
        )
    finally:
        let_emit_finish.set()  # the expired shutdown left that drain thread running


def test_two_concurrent_shutdowns_both_fence_the_worker_they_race() -> None:
    """FR-001: the depth counter, reproduced rather than asserted in prose.

    A boolean is not nestable. Two threads in `_shutdown_worker` both raise it, and the first to
    reach the last critical section lowers it while the second is still running — so a worker
    built at that instant is registered nowhere and the second call returns having stopped
    nothing, which is the original defect verbatim. Two concurrent `shutdown()` calls are
    documented as normal: `Worker._close_if_owed`'s docstring names `atexit` plus a caller's own
    cleanup as the case it exists for.

    Both shutdowns are parked inside the orphan sink's `close()` — the first close parks, the
    second passes straight through — and the worker is built while both are outstanding.
    """
    orphan = CountingSink("orphan")
    orphan.may_finish = threading.Event()
    log_foundry.configure(service="t", sink=orphan)
    log_foundry.info("arm the orphan record")

    first = threading.Thread(target=lambda: log_foundry.shutdown(timeout=10.0))
    first.start()
    assert orphan.in_close.wait(5.0), "the first shutdown is inside the orphan close"
    assert _lifecycle._state._shutdown_running >= 1, "and has raised the counter"

    second = threading.Thread(target=lambda: log_foundry.shutdown(timeout=10.0))
    second.start()
    second.join(0.5)

    @log_foundry.trace
    def work() -> int:
        return 1

    work()  # built while at least one shutdown is still outstanding
    assert _lifecycle._state._late_worker is not None, (
        "a worker built while any shutdown is running must be registered for it — with a "
        "boolean the first call to finish lowers the flag and this one is registered nowhere"
    )

    orphan.may_finish.set()
    first.join(20.0)
    second.join(20.0)

    assert _lifecycle._state._shutdown_running == 0, "both calls lowered what they raised"
    assert not _drain_threads(), "and neither returned leaving a live drain thread"


def test_a_worker_built_after_shutdown_returned_still_delivers() -> None:
    """FR-001 AC-5 — the Out of Scope guard, and the reason the fence is not permanent.

    A sequential `orphan → shutdown() → @trace` builds a fresh live worker that genuinely
    delivers against a permissive sink; `_worker_health`'s docstring settles that, and the
    detection there is `failed_batches` rather than SPEC-030's pair. A permanent retirement
    fence would have silently reversed it, which is the finding that redesigned FR-001.
    """
    sink = CountingSink("A")
    log_foundry.configure(service="t", sink=sink)
    log_foundry.info("before the shutdown")
    log_foundry.shutdown()
    delivered_before = sink.events

    @log_foundry.trace
    def work() -> int:
        return 1

    work()
    log_foundry.flush(timeout=5.0)

    assert sink.events > delivered_before, "the worker built afterwards must still deliver"
    assert log_foundry.health().submitted_after_shutdown == 0, (
        "and its submissions are not the pair SPEC-030 defines, which needs a retired worker"
    )

    late = _lifecycle._state.worker_exists()
    assert late is not None and _lifecycle._state.live_worker() is late, (
        "the premise for the second half: the worker built afterwards reads as live"
    )
    log_foundry.shutdown(timeout=5.0)
    assert not late.draining, "SPEC-054 FR-001 AC-4: the second shutdown's drain finished"
    assert _lifecycle._state.live_worker() is None, "and it stopped reading as live"

    delivered = sink.events
    log_foundry.info("after the second shutdown")
    assert log_foundry.health().submitted_after_shutdown == 0, (
        "an orphan log outside a span is delivered, not stranded (SPEC-030's fence, kept)"
    )
    assert sink.events > delivered, "and it really did land"


def test_a_second_shutdown_strands_and_counts_the_late_workers_next_submission() -> None:
    """FR-001 AC-2. The count, not a latch, is what makes both halves of this true.

    A worker built after a `shutdown()` returned records the already-incremented count as its
    epoch, so its own submissions are **not** stranded — that is the half above. This is the
    other: the *next* `shutdown()` moves the count past that epoch, and from that instant the
    same worker's submissions are queued where nothing will drain them, which is exactly what
    `submitted_after_shutdown` counts (SPEC-030 FR-001).

    Against a latched boolean the two halves cannot both hold: a worker whose flag is set at
    build counts everything it ever delivered, and one whose flag is clear never starts counting.
    """
    sink = CountingSink("A")
    log_foundry.configure(service="t", sink=sink)
    log_foundry.info("an orphan log, so the first shutdown has something to retire")
    log_foundry.shutdown(timeout=5.0)

    @log_foundry.trace
    def work() -> int:
        return 1

    work()
    log_foundry.flush(timeout=5.0)
    assert log_foundry.health().submitted_after_shutdown == 0, (
        "the premise: the late worker's own submissions are not stranded"
    )

    log_foundry.shutdown(timeout=5.0)
    work()

    assert log_foundry.health().submitted_after_shutdown == 1, (
        "the submission after the second shutdown is stranded and counted"
    )


_UNDER_LOCK_OFFENDERS = frozenset({"_close_orphan_sink", "join_closers", "_close_owed"})
"""Calls that must never appear inside a `with _state._lock` body.

Every one performs or waits on a sink `close()`. A name is added here whenever a helper starts
wrapping one, because this lint matches on the name and cannot see through a rename.
"""


def test_no_close_or_drain_is_performed_under_the_lifecycle_lock() -> None:
    """FR-001 AC-4 and FR-002 AC-4, as a lint rather than as prose.

    Both are claims about *where the lock is held*, and a sentence in a docstring is not one a
    later edit has to satisfy. An inline `release()`, a `Worker.shutdown` or an
    `_close_orphan_sink` inside a `with _state._lock` body parks every concurrent emit and every
    first `@trace` in the process behind a destination. A **detached** release started under the
    lock is the existing shape (SPEC-033 FR-002) and is allowed: it only starts a thread.

    The offender set is a **name** list, so a helper that wraps an inline release is invisible to
    it until it is named — which is how a refactor shrinks a derived guard as silently as a
    deletion. `_close_owed` is the case: SPEC-046 moved `_close_orphan_sink`'s inline
    `release()` into it, and without the entry below this lint would pass against that call
    reappearing under the lock.
    """
    tree = ast.parse(_LIFECYCLE_SRC.read_text())
    offenders: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.With):
            continue
        if not any(
            isinstance(item.context_expr, ast.Attribute) and item.context_expr.attr == "_lock"
            for item in node.items
        ):
            continue
        for inner in ast.walk(node):
            if not isinstance(inner, ast.Call):
                continue
            name = ast.unparse(inner.func)
            if name in _UNDER_LOCK_OFFENDERS or name.endswith(".shutdown"):
                offenders.append(name)
            if name == "release" and not any(kw.arg == "detached" for kw in inner.keywords):
                offenders.append("inline release()")
    assert not offenders, f"these run under _state._lock and must not: {sorted(set(offenders))}"


# --------------------------------------------------------------------------- FR-002


def test_a_worker_that_did_not_adopt_the_recorded_sink_does_not_discard_its_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-002 AC-1/AC-2. `h4`, natural rate 6/400.

    `configure()` writes `_config.sink` **before** taking the lifecycle lock, so a
    `configure(sink=B)` blocked behind a first `@trace` leaves `_ensure_sink()` returning B while
    the orphan record still names A. Clearing the record unconditionally lost A's close outright:
    measured `A.closes == 0` with events on it and `incomplete_swaps` at zero.
    """
    a, b = CountingSink("A"), CountingSink("B")
    log_foundry.configure(service="t", sink=a)
    log_foundry.info("to A")
    assert _lifecycle._state._owed.get(id(a)) is a, (
        "the record is armed by the emit that landed"
    )

    preempt = _PreemptInGetWorker(monkeypatch)

    @log_foundry.trace
    def work() -> int:
        return 1

    tracer = threading.Thread(target=work)
    tracer.start()
    preempt.wait_until_held()

    reconfigure = threading.Thread(target=lambda: log_foundry.configure(sink=b))
    reconfigure.start()
    reconfigure.join(0.5)
    assert reconfigure.is_alive(), "configure() must be blocked on the lifecycle lock"

    preempt.release()
    tracer.join(10.0)
    reconfigure.join(10.0)

    worker = _lifecycle._state.worker_exists()
    assert worker is not None and worker.sink is b, "the worker captured the newly configured B"
    log_foundry.shutdown(timeout=5.0)
    _lifecycle.join_closers(time.monotonic() + 5.0)
    assert a.events and a.closes == 1, f"A had events and must be closed exactly once: {a}"


def test_a_worker_that_did_adopt_the_recorded_sink_leaves_one_close() -> None:
    """FR-002 AC-3 — the mixed-process guarantee SPEC-031 FR-006 shipped, unchanged.

    The criterion FR-002's fix could most plausibly have broken: where the worker *did* adopt the
    recorded sink, the close is still the worker's alone and still happens exactly once.
    """
    sink = CountingSink("A")
    log_foundry.configure(service="t", sink=sink)
    log_foundry.info("an orphan emit, which arms the record")

    @log_foundry.trace
    def work() -> int:
        return 1

    work()
    log_foundry.shutdown(timeout=5.0)
    _lifecycle.join_closers(time.monotonic() + 5.0)
    assert sink.closes == 1, f"a mixed process closes once, in either order: {sink}"


def _orphan_record_sites() -> dict[tuple[str, str], ast.FunctionDef]:
    """Every site that assigns `_state._orphan_sink`, keyed by (function, assigned expression).

    One walker, used by both properties asserted over this population — the disposition table
    below and SPEC-045's released-record consultation. Two walkers over one population drift, as
    SPEC-038 FR-001 AC-1a/AC-1b had to write a cross-check to catch.

    Args:
      None.

    Returns:
      A mapping from `(enclosing function, assigned expression)` to that function's AST node.

    Raises:
      None.
    """
    tree = ast.parse(_LIFECYCLE_SRC.read_text())
    found: dict[tuple[str, str], ast.FunctionDef] = {}
    for scope in ast.walk(tree):
        if not isinstance(scope, ast.FunctionDef):
            continue
        for node in ast.walk(scope):
            if isinstance(node, (ast.Assign, ast.Delete)):
                writes = [ast.unparse(target) for target in node.targets]
            elif isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
                writes = [ast.unparse(node.value.func)]
            else:
                continue
            for write in writes:
                if "_owed" in write:
                    value = (
                        ast.unparse(node.value)
                        if isinstance(node, ast.Assign)
                        else write
                    )
                    found[(scope.name, value)] = scope
    return found


def test_every_site_that_clears_the_orphan_record_declares_its_disposition() -> None:
    """FR-002 AC-5. Derived, because a hand-written list of sites rots (SPEC-028, SPEC-035).

    A transition may drop a sink from the owed-close record only after deciding who performs the
    close it was holding, and this enumerates the sites that arm or drop one so that a new one is
    a decision somebody takes rather than a default.

    SPEC-045 made the record a `dict` rather than a single slot, which collapsed three separate
    clears into one transition. SPEC-054 FR-002 then merged four records into it and FR-003 gave
    it one closer, so what remains is the armings and the per-sink removals that pair with a
    close — one for each thing that can perform one, and **no wholesale clear at all**.

    The set equality **is** the floor: an exact comparison against a hand-written table cannot
    shrink unnoticed, so a separate `>= n` assertion beside it would read as a second safety net
    while being strictly implied. Stated rather than asserted twice, as the sibling roster does.
    """
    dispositions = {
        ("_close_owed", "_state._owed[id(sink)]"): (
            "removed per sink — the one closer performs that sink's close, and it removes and "
            "registers the close in the **same** critical section, so there is no instant at "
            "which the sink is neither owed nor in flight (SPEC-054 FR-003)"
        ),
        ("_swap_sink", "_state._owed[id(stale)]"): (
            "removed per sink — the swap releases a superseded sink itself, detached, and "
            "registers that close beside the removal under _state._lock for the same reason"
        ),
        ("_swap_sink", "_state._owed.pop"): (
            "removed per sink — the previous sink, once the worker confirmed its fence, so "
            "nothing can still be inside it; released detached and joined to the swap's budget"
        ),
        ("_swap_sink", "new_sink"): "armed — the sink configure() just installed is owed a close",
        ("_note_orphan_emit", "sink"): "armed — the emit that landed owns the close",
        ("_get_worker", "sink"): (
            "armed — a worker built on a sink owes it a close whether or not anything is emitted "
            "afterwards, which is the worker path's rule (SPEC-054 FR-002)"
        ),
        ("_shutdown_worker", "_close_owed"): (
            "neither — it is the call to the closer, which the walker sees because the callee's "
            "name carries the record's. Listed rather than filtered out, because a filter narrow "
            "enough to drop it would drop a real site under a similar name"
        ),
    }
    found = set(_orphan_record_sites())
    assert found == set(dispositions), (
        "a site writing _state._orphan_sink is a close-ownership decision — add it to the table "
        f"with its disposition. missing: {sorted(set(dispositions) - found)}; "
        f"undeclared: {sorted(found - set(dispositions))}"
    )


def test_the_owed_close_record_is_only_ever_mutated_in_place() -> None:
    """SPEC-045. A rebind of the record is the single-slot defect, reintroduced.

    The record was one slot, so arming a second sink discarded the first and its close went to
    nobody — measured, the live sink closed zero times while a stale one was closed twice.
    Making it a `dict` fixes that only while every site mutates it **in place**: any
    `_owed = {...}` silently drops whatever another thread armed between the read and the
    write, which is the same defect with a wider window.

    There is no sanctioned wholesale emptying any more (SPEC-054 FR-003): every removal is per
    sink, under the lock, in the same critical section that registers that sink's close. The
    assertion below is therefore that **nothing** clears the record — a `clear()` reappearing
    would be a caller taking sinks whose closes it has not decided.

    The collector gathers `ast.Assign` only, so `__init__`'s annotated declaration is not a
    rebind it can see — an earlier draft carried a disjunct exempting that line, which read as
    covering a case the walk never reaches.
    """
    tree = ast.parse(_LIFECYCLE_SRC.read_text())
    rebinds = [
        ast.unparse(node)
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Attribute) and target.attr == "_owed"
    ]
    assert not rebinds, (
        "the owed-close record is rebound rather than mutated, which drops whatever another "
        f"thread armed in between: {rebinds}"
    )
    clears = {
        scope.name
        for scope in ast.walk(tree)
        if isinstance(scope, ast.FunctionDef)
        for node in ast.walk(scope)
        if isinstance(node, ast.Call) and ast.unparse(node.func).endswith("_owed.clear")
    }
    assert clears == set(), (
        "the record is emptied per sink, in the critical section that decides each close, so a "
        f"wholesale clear is a caller taking sinks it has not decided: {sorted(clears)}"
    )


# --------------------------------------------------------------------------- FR-003


@pytest.mark.parametrize("build_worker", [False, True], ids=["orphan", "worker"])
def test_a_close_in_flight_keeps_the_stop_signal_it_was_given(build_worker: bool) -> None:
    """FR-003 AC-1/AC-2. `h2` and `h2b`: SPEC-027's guarantee failing on a race, on both paths.

    `_offer_orphan_signal` replaced a stop event that is already set with a fresh unset one, so
    an `info()` landing *inside* the close handed the sink an unset event and the close then
    served its whole backoff. Measured on the pre-fix tree: 8.01 s against an 8 s backoff, versus
    0.00 s with no racing log.
    """
    backoff = 8.0
    waited: list[float] = []
    emit_landed = threading.Event()

    class BackingOffSink:
        """A sink whose `close()` performs one interruptible backoff — the SPEC-027 shape."""

        log_foundry_stop_signal: threading.Event | None = None

        def __init__(self) -> None:
            self.in_close = threading.Event()

        def emit(self, batch: list[dict[str, object]]) -> None:
            """Accepts and keeps nothing; this test asserts on the close's wait."""

        def close(self) -> None:
            """Signals that the close has begun, lets the racing emit land, then backs off."""
            self.in_close.set()
            emit_landed.wait(10.0)
            start = time.monotonic()
            retry.wait(backoff, self.log_foundry_stop_signal)
            waited.append(time.monotonic() - start)

    sink = BackingOffSink()
    log_foundry.configure(service="t", sink=sink)
    if build_worker:

        @log_foundry.trace
        def work() -> int:
            return 1

        work()
    log_foundry.info("arm the orphan path")

    def racer() -> None:
        sink.in_close.wait(10.0)
        log_foundry.info("racing the shutdown")
        emit_landed.set()

    threading.Thread(target=racer, daemon=True).start()
    log_foundry.shutdown()

    assert waited, "the close ran"
    assert waited[0] < 1.0, (
        f"the close waited {waited[0]:.2f}s of a {backoff}s backoff — a racing log call "
        "replaced the shutdown's signal with a fresh unset one"
    )


def test_a_sink_adopted_after_the_shutdown_returned_still_backs_off() -> None:
    """FR-003 AC-3, restated here so this file carries the property it must not break.

    SPEC-033 FR-004 measured and pinned it: after `shutdown()` has **returned**, a sink still
    receives an unset event and still backs off, because an `Event` never clears and a set one
    collapses every later backoff to zero. The two tests that own this criterion live in
    `tests/test_orphan_sink_handoff.py` and are deliberately not edited; this is the reason the
    FR-003 discriminator is the *moment* and not retirement.
    """
    same, fresh = CountingSink("same"), CountingSink("fresh")
    log_foundry.configure(service="t", sink=same)
    log_foundry.info("before")
    log_foundry.shutdown()
    log_foundry.configure(sink=fresh)
    log_foundry.info("after the shutdown returned")

    signal = fresh.log_foundry_stop_signal
    assert isinstance(signal, threading.Event), "the newly adopted sink has a signal"
    assert not signal.is_set(), "and an unset one — nothing is in flight"
    start = time.monotonic()
    retry.wait(0.3, signal)
    assert time.monotonic() - start >= 0.25, "so it still backs off"


def test_a_release_that_raises_still_drops_its_in_flight_registration() -> None:
    """FR-003 AC-4's other half — the path a `try/finally` exists for.

    `release()` propagates whatever `close()` raised, deliberately: the callers do not agree on
    error handling and folding a `try/except` in would drop absorbed failures out of
    `Health.sink.failed` (SPEC-042 FR-002). `FilteringSink`, `TransformSink` and `LogstashSink`
    all propagate, so this path is reached by shipped code.

    A leaked id is permanent and silent: every later `_offer_orphan_signal` for that object
    returns early, so the sink keeps a stale — possibly set — stop event and backs off not at
    all, which is the SPEC-033 FR-004 failure the in-flight discriminator was chosen to avoid.
    """

    class RaisingSink:
        """Refuses its own close, the way a wrapper forwarding a child's failure does."""

        log_foundry_stop_signal: threading.Event | None = None

        def emit(self, batch: list[dict[str, object]]) -> None:
            """Keeps nothing; this test asserts on the registration, not on delivery."""

        def close(self) -> None:
            """Raises, so `release` propagates and the `finally` is the only way back."""
            raise RuntimeError("the transport refused to close")

    sink = RaisingSink()
    _lifecycle.stamp(sink)
    with pytest.raises(RuntimeError):
        _lifecycle.release(sink)
    with _lifecycle._closing_now_lock:
        registered = id(sink) in _lifecycle._closing_now
    assert not registered, (
        "a close that raised left its id registered — the sink is now permanently exempt from "
        "the stop-signal refresh"
    )


def test_a_forked_child_drops_the_in_flight_close_registrations_it_inherited() -> None:
    """FR-003 AC-4's sibling: the registration is removed by a `finally` on the closing thread.

    A forked child has only the thread that called `fork()`, so an inherited entry is one
    nothing will ever clear — and left in place it is not a missed refresh but a permanent one:
    once the child sets its own `_orphan_stop`, that sink is handed the set event and backs off
    not at all. Measured before `_clear_closing_after_fork` existed.
    """
    with _lifecycle._closing_now_lock:
        _lifecycle._closing_now[0xDEADBEEF] = threading.Event()
    try:
        read_fd, write_fd = os.pipe()
        # See `run_in_child` in test_fork_lifecycle.py: a child must not be left anything
        # fork-unsafe to finalize. `test_every_fork_collects_first` derives this rule.
        gc.collect()
        pid = os.fork()
        if pid == 0:  # pragma: no cover - the child never returns to pytest
            os.close(read_fd)
            with _lifecycle._closing_now_lock:
                remaining = len(_lifecycle._closing_now)
            os.write(write_fd, str(remaining).encode())
            os._exit(0)
        os.close(write_fd)
        os.waitpid(pid, 0)
        assert os.read(read_fd, 32).decode() == "0", (
            "the child inherited a registration whose finally will never run"
        )
        os.close(read_fd)
    finally:
        with _lifecycle._closing_now_lock:
            _lifecycle._closing_now.pop(0xDEADBEEF, None)


# --------------------------------------------------------------------------- FR-004


def test_a_swap_that_hands_a_sink_to_the_worker_owes_a_second_close_for_the_late_emit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SPEC-054 FR-002 AC-3 re-states SPEC-044 FR-004 AC-1 from one close to two.

    `h1`: 0/120 without an injected preemption point, hence the injection. An orphan emit
    resolves sink A before the swap and resumes after it, so the emit lands on a sink the swap
    has already closed.

    ~~and the exit close then performed a second `close()`~~ — struck (SPEC-021). The **route**
    is unchanged and the verdict on its second close is not. SPEC-044 FR-004 answered it with a
    closed-sink latch that refused the re-arm, and SPEC-054 FR-002 retires that latch: an emit
    that lands re-arms, always, and a sink written to after its close is owed another
    (SPEC-045 FR-002). What the latch protected against — a close performed against a sink the
    drain thread may still be inside — is answered at close time by `held`, not by refusing the
    arming.

    So A is closed **twice**, and the second close follows the event that earned it. The event
    count at each close is what says so: a second close landing *before* the late event would be
    the double the latch existed to prevent, wearing the same number.
    """
    a, b = CountingSink("A"), CountingSink("B")
    log_foundry.configure(service="t", sink=a)

    @log_foundry.trace
    def work() -> int:
        return 1

    work()  # builds the worker on A
    assert id(a) in _lifecycle._state._owed, (
        "the build arms its sink rather than clearing the record (SPEC-054 FR-002)"
    )

    resolved = threading.Event()
    release_it = threading.Event()
    real_ensure = api._ensure_sink

    def preempting_ensure_sink() -> object:
        sink = real_ensure()
        resolved.set()
        release_it.wait(10.0)
        return sink

    monkeypatch.setattr(api, "_ensure_sink", preempting_ensure_sink)
    emitter = threading.Thread(target=lambda: log_foundry.info("orphan racing the swap"))
    emitter.start()
    assert resolved.wait(5.0), "the emit resolved A and is parked holding it"
    monkeypatch.setattr(api, "_ensure_sink", real_ensure)

    log_foundry.configure(sink=b)  # the worker-path swap: Worker.swap_sink closes A
    release_it.set()
    emitter.join(10.0)

    log_foundry.shutdown(timeout=5.0)
    _lifecycle.join_closers(time.monotonic() + 5.0)
    assert a.closes == 2, (
        f"the emit that landed after the swap's close re-armed A, so it is owed another: {a}"
    )
    assert a.events_at_close[1] > a.events_at_close[0], (
        "and that second close follows the event it discharges, which is one close per "
        f"write-epoch rather than a double: {a.events_at_close}"
    )


def test_a_swap_releases_a_third_sink_the_record_named(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-002's second site, and the branch a mutation sweep found had no behavioural cover.

    `_swap_sink`'s worker branch clears the orphan record too, so the FR-002 rule applies there
    as well: where the record names a sink that is neither the worker's nor the one being
    installed, nothing else would ever close it. Reaching that state needs a preempted orphan
    emit to re-arm across an earlier swap, which is why the branch had survived every test —
    deleting it outright left `tests/test_lifecycle_races.py` fully green.

    What it does **not** pin is the closer join's budget arithmetic — that the join is against
    the swap's remaining deadline rather than a fresh `timeout`. Reverting that survives this
    test, and separating the two costs a drain slow enough to consume most of the budget and a
    close slower still, which makes the assertion a load-sensitive 0.6-vs-1.0 second margin. A
    flaky bound is worse than an unpinned one-line expression, so it is flagged here rather than
    tested. The bound below is the honest weaker claim: the swap returns bounded at all.
    """
    third, first, second = CountingSink("third"), CountingSink("first"), CountingSink("second")
    log_foundry.configure(service="t", sink=first)

    @log_foundry.trace
    def work() -> int:
        return 1

    work()  # a worker on `first`
    worker = _lifecycle._state.worker_exists()
    assert worker is not None and worker.sink is first

    # Re-arm the record on a sink the worker does not hold, the way a preempted emit would.
    with _lifecycle._state._lock:
        _lifecycle._state._owed[id(third)] = third
    _lifecycle.stamp(third)

    started = time.monotonic()
    log_foundry.configure(sink=second)
    swap_seconds = time.monotonic() - started
    _lifecycle.join_closers(time.monotonic() + 5.0)

    assert worker.sink is second, "the worker adopted the newly configured sink"
    assert third.closes == 1, (
        f"the sink the record named is neither the worker's nor the new one, so this swap owns "
        f"its close and nothing else would perform it: {third}"
    )
    assert swap_seconds < _lifecycle.DEFAULT_SWAP_TIMEOUT, (
        "a swap that starts a detached close still returns inside its own budget"
    )


# --------------------------------------------------------------------------- FR-005


class _HookSink(stdout_sink.StdoutSink):
    """Subclasses a shipped sink so `_fork._is_owned` is true and the walk reaches it."""

    def __init__(self, name: str) -> None:
        super().__init__()
        self.name = name
        self.closes = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Keeps nothing; this test asserts on which sinks the child's walk reaches."""

    def close(self) -> None:
        """Counts the close, so the superseded sink is provably released before the fork."""
        self.closes += 1

    def reacquire_after_fork(self) -> None:
        """Records that the child's repair walk reached this sink."""
        _HOOKED.append(self.name)


_HOOKED: list[str] = []


def test_a_forked_child_does_not_hook_a_superseded_sink() -> None:
    """FR-005 AC-1. `h11`: the exact hazard `_fork._SKIP_ATTRIBUTE`'s own docstring describes.

    The owed-close record pins a sink a swap already released, and the repair walk reached it,
    so a child called `reacquire_after_fork()` on a closed sink — a `FileSink` there would have
    its file re-opened on every fork for the life of the process. The module-level `_FORK_SKIP`
    could not reach it: `_owned` is a module global while this slot is an attribute of `_state`,
    and `_fork._skipped_names` asks the holder. Measured pre-fix: the child hooked both.
    """
    _HOOKED.clear()
    superseded, live = _HookSink("superseded"), _HookSink("live")
    log_foundry.configure(service="t", sink=superseded)
    log_foundry.info("to the sink that is about to be superseded")
    log_foundry.configure(sink=live)
    log_foundry.info("to the live sink")
    _lifecycle.join_closers(time.monotonic() + 5.0)
    assert superseded.closes == 1 and id(superseded) not in _lifecycle._state._owed, (
        "the premise: the swap released it and it left the record, so what could still reach it "
        "is a walk that descends into a record it should not"
    )

    read_fd, write_fd = os.pipe()
    # See `run_in_child` in test_fork_lifecycle.py: a child must not be left anything
    # fork-unsafe to finalize. `test_every_fork_collects_first` derives this rule.
    gc.collect()
    pid = os.fork()
    if pid == 0:  # pragma: no cover - the child never returns to pytest
        os.close(read_fd)
        os.write(write_fd, ",".join(sorted(_HOOKED)).encode())
        os._exit(0)
    os.close(write_fd)
    os.waitpid(pid, 0)
    hooked = os.read(read_fd, 4096).decode()
    os.close(read_fd)
    assert hooked == "live", f"the child's walk reached {hooked!r}, not the live sink alone"


def test_a_child_still_refuses_to_close_an_inherited_superseded_sink() -> None:
    """FR-005 AC-2. Narrowing the repair walk must not cost the child its refusal.

    It does not, and the reason is worth stating because it is not the obvious one. The refusal
    is grounded in `_owned`, where `configure()` stamped the sink with the **parent's** pid;
    `_mark_inherited` uses `setdefault`, so it leaves that stamp alone, and `releasable` answers
    on `record[0] == pid`, which the parent's stamp fails. `_inheritance_roots`' own docstring
    says the same from the other side — `_owned.values()` is its load-bearing entry, and dropping
    any of the four live handles "changes nothing, since each is itself stamped".

    No edit to FR-005's opt-out can reach that: `_FORK_SKIP` is read only by
    `_fork._skipped_names` for the repair walk, and the marking path never consults it. So the
    two terms the child reports carry different work, and the split is not the obvious one. The
    **verdict** is the term that sees `releasable` itself weakened while the record stands:
    drop its `record[0] == pid` and the child answers `releasable,the-parent-s`. A change that
    *deletes* the record is caught twice over, `releasable,unrecorded` — and only once, on the
    pid, if the marking walk fails in the same edit, because `_marking_failed` then refuses the
    unrecorded sink anyway and the verdict stays `refused`.

    The **pid** is the only term that sees a record merely downgraded. A verdict cannot tell
    `setdefault` from an overwrite: in a process that has just returned from `fork` no record
    can name the child, so the parent's stamp and an overwriting `_FOREIGN` refuse alike, and
    the record's only other field — the strong reference `releasable` re-checks identity
    against — holds the same object under both spellings. Measured at `01fd73c`, where this
    docstring claimed to catch that very mutant: replacing the `setdefault` with
    `_owned[id(inherited)] = (_FOREIGN, inherited)` left the whole suite green. Reporting which
    pid was found, rather than a verdict about it, is what makes a downgraded record redden.
    """
    superseded, live = CountingSink("superseded"), CountingSink("live")
    log_foundry.configure(service="t", sink=superseded)
    log_foundry.info("to the sink that is about to be superseded")
    log_foundry.configure(sink=live)
    _lifecycle.join_closers(time.monotonic() + 5.0)
    assert superseded.closes == 1, "the premise: the swap released it before the fork"

    parent_pid = os.getpid()
    read_fd, write_fd = os.pipe()
    # See `run_in_child` in test_fork_lifecycle.py: a child must not be left anything
    # fork-unsafe to finalize. `test_every_fork_collects_first` derives this rule.
    gc.collect()
    pid = os.fork()
    if pid == 0:  # pragma: no cover - the child never returns to pytest
        os.close(read_fd)
        with _lifecycle._owned_lock:
            record = _lifecycle._owned.get(id(superseded))
        if record is None:
            stamped = "unrecorded"
        elif record[0] == os.getpid():
            stamped = "the-child-s"
        elif record[0] == parent_pid:
            stamped = "the-parent-s"
        elif record[0] == _lifecycle._FOREIGN:
            stamped = "foreign"
        else:
            stamped = "another-process"
        released = "releasable" if _lifecycle.releasable(superseded) else "refused"
        os.write(write_fd, f"{released},{stamped}".encode())
        os._exit(0)
    os.close(write_fd)
    os.waitpid(pid, 0)
    verdict = os.read(read_fd, 64).decode()
    os.close(read_fd)
    assert verdict == "refused,the-parent-s", (
        f"the child answered {verdict!r}: it must refuse the inherited superseded sink, and "
        "refuse it on the stamp `configure()` left in the parent — a record a later edit "
        "downgrades to `_FOREIGN` refuses too, so only the pid tells the two apart"
    )


def test_the_fork_opt_out_is_declared_where_the_walk_reads_it() -> None:
    """FR-005 AC-3. The stale-detector: moving the slot must not silently un-skip it.

    `_fork._skipped_names` reads the declaration off the **holder** of the attribute, so this
    asserts against the real reader rather than against `_Lifecycle._FORK_SKIP` directly — a
    declaration that stops being found is exactly the failure the behavioural test above would
    also catch, and exactly the one a `getattr` on the class would not.
    """
    assert "_owed" in _fork._skipped_names(_lifecycle._state), (
        "the record pinning superseded sinks must be opted out on the object that holds it"
    )
    assert "_owned" in _fork._skipped_names(_lifecycle), (
        "and the module-level declaration stays — the two together are the rule"
    )


# --------------------------------------------------------------------------- FR-006


@pytest.mark.parametrize("build_worker", [False, True], ids=["orphan", "worker"])
def test_a_slow_close_outlasts_the_shutdown_timeout(build_worker: bool) -> None:
    """FR-006 AC-3. Pins the documented limit, on both delivery paths.

    `shutdown(timeout=…)` bounds the drain thread's join and the swapped-out-sink closer grace,
    and **not** the live sink's `close()`, which runs inline. That is deliberate: a daemon closer
    for this close was built and reverted twice (`Worker._close_if_owed`, SPEC-030), and bounding
    it properly needs an interruptible `Sink.close` — a change to the published sink contract.
    So this asserts the limit rather than the fix, and reddens if the close is ever bounded,
    which is what stops the documentation drifting away from the behaviour again.

    Measured with a 6-second close at 6.01 s against `timeout=2.0`; the ratio is what matters,
    so the pin uses a fraction of a second.
    """
    slow = 0.6
    sink = CountingSink("slow", close_seconds=slow)
    log_foundry.configure(service="t", sink=sink)
    if build_worker:

        @log_foundry.trace
        def work() -> int:
            return 1

        work()
    else:
        log_foundry.info("arm the orphan path")

    start = time.monotonic()
    log_foundry.shutdown(timeout=0.05)
    elapsed = time.monotonic() - start

    assert sink.closes == 1, "the close ran inline"
    assert elapsed >= slow, (
        f"shutdown(timeout=0.05) returned in {elapsed:.2f}s against a {slow}s close — if the "
        "live sink's close is now bounded, update shutdown()'s docstring and architecture.md §13"
    )
