"""The suite's ledger balance check (SPEC-088 FR-006). A helper module, not collected.

`tests/conftest.py`'s autouse `_ledger_balance` fixture calls :func:`begin` at each test's setup
and :func:`verdict` at its teardown, after the test's body and before the suite's own reset, and
fails the test with the reasons :func:`verdict` returns. It stops nothing: every check is a read.
The logic lives here rather than in `conftest.py` so the fixture stays a thin caller.

**It reads at rest for the caller's side, and lets a live drain settle for the worker's side.**
The test's own calls have returned by its teardown, so the process terms are read once. A
worker's drain thread may still be running, though, and between `queue.get` returning and
`Worker._hold` counting the take, or inside `Worker._settle` between the exits and the gauge, its
own terms read out of balance for a few bytecodes: `main` went red once that way, at `97a03f45`,
on `test_the_snapshot_returns_while_each_lock_is_held[queue]`, whose drain took its submission
only when the test let go of the queue's mutex. So a worker whose drain is alive and whose
reading fails is read again, every :data:`_PAUSE`, until a reading passes or :data:`SETTLE` has
elapsed, and the last reading's reasons are the ones reported. A reading that passes the first
time is not repeated, and neither is one of an ended drain, which nothing will move again: a
stalled sink balances through ``in_flight`` and ``queued`` on its first reading, so it still adds
no wait (FR-006 AC-7). The order in `src/` is not the thing to change: `_hold` raises the gauge
before the taking count, outside the queue's mutex, so that ``health()`` and the ``shutdown()``
check never read a false ``unaccounted``, and `tests/test_ledger_write_sites.py` holds it there.

**What fails a test.**

- ``unaccounted`` is not zero: the library's own count of events it cannot place.
- The orphan residual is not zero, with either sign. The shutdown check counts only a positive
  one, because a call in progress reads negative; this reads at rest, after the test's body, so a
  negative one is a debit made twice.
- ``open_spans`` differs from the events buffered on the spans still open in this context, which
  is zero for a test that closed what it opened.
- For any `Worker` the test built, ``queued + stranded`` differs from the events in its queue,
  counted under its mutex, or its own terms place events the ledger cannot: events in flight on an
  ended drain, a drain residual of either sign, or a gauge below zero. ~~a positive drain
  residual~~ — widened by diff review frame 2: at rest a negative one is an exit counted twice,
  the orphan residual's reason, and with only the positive side checked a `_settle` that doubled
  `delivered` passed `test_worker.py`, `test_shutdown_lifecycle.py` and `test_api.py` silent.

**What it cannot see.** A negative meeting residual is not flagged: hand-overs below what the
queue took in and what overflowed. `overflow_lost` counted twice reads that way, and so does
every test that hands a worker events directly, which no hand-over credits; flagging it errored
334 tests (diff review frame 2). The positive side is in ``unaccounted`` and is checked.

A test using the `lf` fixture emits each span directly from `decorator._flush`; that fixture
credits the hand-over as `_flush` does and records the events here, which no queue takes in, so
the meeting residual may be positive by at most that count.

**The marker.** `ledger_exempt(reason)` names a test that works below the accept sites, and it is
strict: a marked test whose process balances fails, so an exemption that is no longer needed
cannot stay.
"""

from __future__ import annotations

import functools
import time
import weakref

_tracked: weakref.WeakSet = weakref.WeakSet()
"""Every `Worker` built since the current test began and still alive at its teardown.

Weakly held, so a worker the test dropped before its teardown is not checked: a known limit of
the check, kept on purpose. Measured in the all-extras environment, holding every worker strongly
until the next test began failed exactly the 32 worker-path cases of
`test_sinks_nats_engine_matrix.py::test_the_engine_matches_1x` and 2 of
`test_a_planted_verdict_is_caught`, whose harness hands `Worker._emit` a batch no queue took in, as
the exempt `test_the_workers_emit_loop_survives_a_huge_max_retries` does; a function-level marker
would wrongly fail those tests' balancing `emit`-path cases. Every other test balanced."""

_direct = [0]
"""Events the `lf` fixture emitted directly since the current test began."""

MARKER = "ledger_exempt"

SETTLE = 2.0
"""Seconds one teardown lets live drains settle, shared by every worker it reads.

The window it covers is a few bytecodes long, so the bound is spent only by a drain that is truly
out of balance, which then fails its test this much later. It is shared rather than per worker
so a defect that every worker shows costs one bound, not one per worker."""

_PAUSE = 0.005
"""Seconds between two readings of a live drain that has not settled."""


def _track_workers() -> None:
    """Records every `Worker` built from now on in :data:`_tracked`, installed once per process."""
    from log_foundry import worker as worker_mod

    original = worker_mod.Worker.__init__
    if getattr(original, "_ledger_tracked", False):
        return

    @functools.wraps(original)
    def tracking(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        original(self, *args, **kwargs)
        _tracked.add(self)

    tracking._ledger_tracked = True  # type: ignore[attr-defined]
    worker_mod.Worker.__init__ = tracking  # type: ignore[method-assign]


def begin() -> None:
    """Starts a test on an empty ledger, with no worker tracked and no direct emit recorded.

    A test that plants an imbalance on purpose, to show the shutdown check sees it, calls this
    again once it has asserted, so the imbalance it made is its own and does not need a marker.
    """
    from log_foundry import _ledger, _lifecycle, decorator

    _ledger.restart(0)
    with decorator._loss_lock:
        decorator._orphan_lost = 0
        decorator._in_span_lost = 0
    _lifecycle._state._ledger_announced = 0
    _track_workers()
    _tracked.clear()
    _direct[0] = 0


def emitted_directly(count: int) -> None:
    """Records events the `lf` fixture emitted with no queue between it and the sink."""
    _direct[0] += count


def _queued_events(worker) -> int:  # type: ignore[no-untyped-def]
    with worker._queue.mutex:
        return sum(len(item) for item in worker._queue.queue if isinstance(item, list))


def worker_problems(worker, settle: float = SETTLE) -> list[str]:  # type: ignore[no-untyped-def]
    """What one worker's own terms place that the ledger cannot, once a live drain has settled.

    A failing reading of a live drain is repeated until one passes or ``settle`` seconds have
    passed; a passing reading, or any reading of an ended drain, is final at once.
    """
    deadline = time.monotonic() + settle
    alive, found = _read_worker(worker)
    while found and alive and time.monotonic() < deadline:
        time.sleep(_PAUSE)
        alive, found = _read_worker(worker)
    return found


def _read_worker(worker) -> tuple[bool, list[str]]:  # type: ignore[no-untyped-def]
    """One reading of a worker's own terms: whether its drain was alive, and what they misplace."""
    alive, taken, held, *exits = worker._drain_terms()
    in_queue = worker._queue.entered - taken
    found: list[str] = []
    events = _queued_events(worker)
    if in_queue != events:
        found.append(f"a worker's queued+stranded reads {in_queue} with {events} events in its queue")
    if not alive and held > 0:
        found.append(f"a worker's ended drain holds {held} events in flight")
    residual = taken - held - sum(exits)
    if residual != 0:
        found.append(f"a worker's drain residual is {residual}")
    if held < 0:
        found.append(f"a worker's in_flight reads {held}")
    return alive, found


def problems() -> list[str]:
    """Every reason the process does not balance at rest, read now."""
    from log_foundry import _lifecycle, context

    seen = _lifecycle._ledger_reading()
    e = seen.ledger
    found: list[str] = []
    unaccounted = e.unaccounted
    meeting = seen.meeting_residual
    if meeting is not None and meeting > 0 and _direct[0]:
        unaccounted -= min(meeting, _direct[0])
        if meeting > _direct[0]:
            found.append(f"the meeting residual {meeting} exceeds the lf fixture's {_direct[0]}")
    if unaccounted:
        found.append(f"unaccounted={unaccounted}")
    if seen.orphan_residual:
        found.append(f"orphan residual={seen.orphan_residual}")
    left_open = [span for span in context._live_span_stack() if not span.closed]
    own = sum(len(span.events) for span in left_open)
    if e.open_spans != own:
        found.append(f"open_spans={e.open_spans} with {own} buffered on this context's open spans")
    if left_open:
        found.append(f"open_spans: the test left {len(left_open)} span(s) open on its own context")
    deadline = time.monotonic() + SETTLE
    for worker in list(_tracked):
        found.extend(worker_problems(worker, max(0.0, deadline - time.monotonic())))
    return found


def judge(found: list[str], marked: bool) -> str | None:
    """The failure message for one test's teardown, or ``None`` when it passes the check."""
    if marked:
        if found:
            return None
        return f"marked {MARKER} but the event ledger balances: remove the marker"
    if found:
        return "the event ledger does not balance: " + "; ".join(found)
    return None


def verdict(node) -> str | None:  # type: ignore[no-untyped-def]
    """Judges the test ``node`` at its teardown; ``None`` when it passes the check."""
    return judge(problems(), node.get_closest_marker(MARKER) is not None)
