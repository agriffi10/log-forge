# Spec: Lifecycle Residue — Stranded Waiters, Unfinished Closes and Uncounted Loss

**ID:** SPEC-050  
**Status:** Draft  
**Last Updated:** 2026-09-02  
**Depends On:** SPEC-013, SPEC-021, SPEC-027, SPEC-030, SPEC-031, SPEC-036, SPEC-045, SPEC-046

## Overview

Five places where the shutdown path stops short of what it already knows how to do. A `flush()`
waiting on a stuck sink is abandoned without being told; a `shutdown()` that begins on a
background thread returns to `atexit` while its close is still running, and the process exits
through it; a span whose events cannot reach a worker at all is announced but counted nowhere; a
sink stranded by an overrunning `configure(sink=...)` is never closed even once the drain thread
that might have been inside it has been joined; and a submission that lands a microsecond after
the final drain is queued where nothing will read it, with the counter built for exactly that
case reading zero. Each is small, each loses or hides something an operator would want, and each
is fixed with machinery this library already has.

## Scope

### In Scope

- The five findings above, from the 2026-09-02 pre-1.0 audit (R3, R4, R6, R11, R13), every one
  reproduced by running it against `eb80099` before this spec was written.
- Correcting the incomplete-swap announcement, whose "it is left open" clause FR-004 makes false.
- One housekeeping item in the suite this spec already edits: the wall-clock budget at
  `tests/test_worker.py::test_submit_returns_before_emit_completes` asserts `< 0.1` against a
  0.3 s emit. A test proving an operation is *not* bounded needs a wide budget-vs-operation gap,
  not a tight budget; the tight one fails on its own setup under load.

### Out of Scope

- **R12** — `health().retired` reading `True` for a worker built after an orphan-only
  `shutdown()`. Investigated and rejected as not a defect: `Health.retired` is documented as
  "whether `shutdown()` has been called", which is true there; `_worker_health` already reasons
  about this exact case in writing and chooses the `or` deliberately; the documented alert idiom
  is the *pair* `retired and submitted_after_shutdown`, which correctly stays silent. Measured:
  the fresh worker's events are not lost silently either — against a guarding sink they land in
  `failed_batches` with a line and `flush()` returns `abandoned`. The reasoning is recorded in
  `docs/decisions.md` so the next audit stops here rather than re-finding it.
- Bounding `Sink.close()`. It is unbounded on both paths by architecture §13, and FR-002 and
  FR-004 make a caller *wait* for a close within a budget without making the close itself
  bounded.
- Any change to `sinks/`, the public dataclasses' shape, or the README — all owned elsewhere.
- A fourth `_diag` verb. FR-004's line is imprecise, not unwriteable, and widening the
  three-verb surface SPEC-029 settled is out of proportion to one sentence.

---

## Functional Requirements

### FR-001: A shutdown that expires releases the waiters it stranded

#### Description:

`Worker.shutdown`'s expiry branch records `stopped_reason`, sets `_drain_settled`, writes its
line and returns — before `_release_waiters()`, whose own docstring says it exists for this
caller. A `flush(timeout=None)` parked behind a stuck sink therefore waits forever on a drain
that has been given up on. Reproduced: `flusher still waiting after shutdown gave up: True`.

The marker keeps `delivered=False`, so the flush reports `"abandoned"` — the honest answer,
since the drain that would have carried those events is gone.

#### Acceptance Criteria:

- [ ] With the drain thread blocked inside `sink.emit` and a `flush(timeout=None)` outstanding on
      another thread, a `shutdown(timeout=T)` that expires answers that flush before returning:
      the flushing thread is not alive one second after `shutdown` returns.
- [ ] That flush returns `FlushResult(ok=False, reason="abandoned")` — not `ok=True`, and not
      `"timed-out"`.
- [ ] The expiry branch still latches `stopped_reason="ShutdownTimeout"` and writes exactly one
      `lost` line, with the same text as before.
- [ ] A `shutdown()` whose join succeeds is unchanged, and answers each outstanding marker
      exactly once.
- [ ] Deleting the new call makes the first criterion fail (mutation-tested, not asserted).

### FR-002: The idempotent shutdown path waits for an in-flight inline close

#### Description:

`shutdown()` called first on a background thread reaches `_close_if_owed`, which latches
`_sink_closed` and then runs an unbounded inline `close()`. The `atexit` call that follows finds
the close already claimed and returns instantly, so the interpreter exits through a close that is
still running and kills it. For a sink whose `close()` *is* the delivery this is total loss of
whatever it had buffered. Reproduced with a close-is-delivery sink:
`main exiting at 0.31s; closes started=1 finished=0 wire=0 buffered=12`, and nothing on stderr.

This is the open half of the 2026-08-07 audit's C3: the drain half was fixed by having the
idempotent path wait on `_drain_settled`, and the close half needs the same shape — an event set
when the inline close finishes, waited on with what remains of *this* caller's own budget.

#### Acceptance Criteria:

- [ ] With `shutdown()` parked inside the sink's `close()` on another thread, a second
      `shutdown(timeout=T)` does not return until that close has finished, for a close shorter
      than T.
- [ ] Against a close-is-delivery sink, a first `shutdown()` on a daemon thread followed by
      process exit leaves the sink's buffer delivered — measured by an `atexit` probe registered
      *before* the library's own handler, so LIFO runs it last.
- [ ] The second call stays bounded: against a `close()` that never returns,
      `shutdown(timeout=T)` returns within `T + DEFAULT_CLOSER_GRACE`, and a `close()` slower
      than T does not extend it.
- [ ] `shutdown(timeout=0)` on the idempotent path returns promptly and never inherits the first
      caller's deadline.
- [ ] A second `shutdown()` with no close in flight returns without waiting.
- [ ] The waiting caller neither performs a second `close()` nor reports one.

### FR-003: A span whose events cannot reach a worker counts them

#### Description:

When the process cannot give the library another thread, `Worker.__init__`'s `Thread.start`
raises, `_get_worker` propagates it, and `decorator._end` absorbs it with one stderr line — and
counts nothing. Every span's events are lost with `Health(stopped_reason=None, in_span_lost=0)`.
`_reinit_after_fork` sets `stopped_reason` for the identical failure; this path is silent to
every field. Reproduced: two traced calls, stderr twice, `in_span_lost=0`, delivered 0.

`in_span_lost` is the field for it — the events were lost while the span was being closed, which
is the population SPEC-036 FR-003 defined it over. The count is the events the span was actually
holding, not one per span.

#### Acceptance Criteria:

- [ ] With `Thread.start` refusing, two traced calls each carrying one `info()` leave
      `health().in_span_lost == 6` — three events per span, not 2 and not 0.
- [ ] The existing `absorbed` line is unchanged and still written once per span.
- [ ] A span that closes normally leaves `in_span_lost` at 0.
- [ ] A span that fails after the end event was appended counts that event too; one that fails
      before it counts only what the span held.
- [ ] `orphan_lost` is untouched by this path.

### FR-004: A sink stranded by an unconfirmed swap is closed once the drain thread is joined

#### Description:

A `configure(sink=B)` whose drain of A cannot be confirmed within the swap budget installs B,
counts `incomplete_swaps`, and leaves A **open forever**. The reason is sound at that instant —
the drain thread may still be inside `A.emit`, and closing under a live writer is the failure
SPEC-027 and SPEC-028 exist to prevent. It stops being sound once `shutdown()` has joined the
drain thread: A is then provably out of use, which is the same condition `_close_swapped_out`
already treats as sufficient. Reproduced: after an unconfirmed swap and a clean `shutdown()`,
`A.closes=0` with nine events still in A's client buffer — for a close-is-delivery sink that is
silent loss, not a leaked handle.

The close is performed with the existing detached closer, so `_join_closers` bounds the wait and
`shutdown`'s budget cannot grow. Only the path where the join **succeeded** may do it; an expired
`shutdown()` leaves A open exactly as today, because the drain thread is still live.

This supersedes the constraint recorded at `architecture.md` §7 ("the previous sink is left
**open**") for the clean-shutdown path. The same claim appears in `README.md`, which another
session owns; the correction is flagged rather than made here.

#### Acceptance Criteria:

- [ ] After a `configure(sink=B)` whose drain of A was not confirmed, a `shutdown()` whose join
      succeeds closes A exactly once, and a client-buffering A has delivered its buffer when
      `shutdown()` returns.
- [ ] A `shutdown()` that expires with the drain thread still alive leaves A open and closes
      nothing extra.
- [ ] A is never closed twice: an A swapped out unconfirmed and later swapped back in as the live
      sink is closed once, by the live-sink path only.
- [ ] Two successive unconfirmed swaps A→B→C leave both A and B closed after `shutdown()`.
- [ ] A `close()` that never returns cannot extend the budget: `shutdown(timeout=T)` returns
      within `T + DEFAULT_CLOSER_GRACE`.
- [ ] A *confirmed* swap records nothing new — the existing `_close_swapped_out` remains the only
      closer on that path, and no sink is closed twice.
- [ ] The incomplete-swap stderr line no longer claims the sink is left open unconditionally, and
      names `shutdown()` as what closes it.
- [ ] `health().incomplete_swaps` still increments exactly once per unconfirmed swap, and a
      confirmed swap writes no line.

### FR-005: A submission that lands after the final drain is counted

#### Description:

`Worker.submit` reads `_shutdown_done` unlocked and then puts. A caller preempted between the two
queues its item after the final drain has run, where nothing will read it — and
`submitted_after_shutdown` stays zero, so the documented alert idiom
`retired and submitted_after_shutdown` cannot fire. `Health.submitted_after_shutdown`'s own
docstring says the count "starts at the moment `shutdown()` begins rather than when the drain
thread ends … erring toward reporting is the right direction for a signal whose whole purpose is
visibility"; this window errs the other way. Reproduced deterministically with a preemption point
at the put: `queued=1 submitted_after_shutdown=0`, no line.

The unlocked fast path stays unlocked. The fix is a second read *after* the put, the same
double-check `flush()` already performs against `_drain_finished` for the same race.

#### Acceptance Criteria:

- [ ] With a submission parked at its `put_nowait` and released after `shutdown()` has joined the
      drain thread, `health().submitted_after_shutdown == 1` and one line is written.
- [ ] A submission that read the flag as already latched is counted exactly once, not twice.
- [ ] A submission entirely before `shutdown()` is not counted.
- [ ] The uncontended path takes no lock it did not take before.
- [ ] `health().queued` still reports the stranded item, unchanged.

---

## Data Model

```python
# src/log_foundry/worker.py — Worker, new private state only. No public type changes.

_close_finished: threading.Event   # set when an inline _close_sink() returns, however it returned
_close_running: bool               # guarded by _lock; True between claiming the close and finishing it
_unclosed_swaps: list[Sink]        # sinks swapped out on an unconfirmed drain, owed a close at shutdown
```

`Health` is unchanged: FR-003 and FR-005 move counters that already exist.

---

## API / Interface Contract

No public signature changes. The observable differences are:

```
flush(timeout=None)            # answered, not stranded, when a bounded shutdown expires  (FR-001)
shutdown(timeout=T)            # second caller waits out an in-flight close, bounded by T  (FR-002)
health().in_span_lost          # counts a span whose events could not reach a worker       (FR-003)
health().submitted_after_shutdown  # counts a submission that raced the final drain        (FR-005)
configure(sink=B) + shutdown() # closes an unconfirmed-swap sink once the drain is joined  (FR-004)
```

## Configuration / Environment

None. No new knobs, constants or extras.

## File & Folder Structure

```
src/log_foundry/
├── worker.py        # FR-001, FR-002, FR-004, FR-005
└── decorator.py     # FR-003
docs/
├── architecture.md  # §7 swap paragraph and §9 shutdown, for FR-004 and FR-002
├── decisions.md     # pipeline/lifecycle area: FR-004's reversal, and R12's rejection
└── specs/SPEC-050-lifecycle-residue.md
tests/
├── test_worker.py       # FR-001, FR-002, FR-004, FR-005 + the budget housekeeping
├── test_lifecycle.py    # FR-002's atexit/exit-path coverage
└── test_decorator_sync.py  # FR-003
```

## Implementation Phases

### Phase 1: The two counters

- FR-003: count the span's events into `in_span_lost` where `_end` absorbs a close failure.
- FR-005: re-read `_shutdown_done` after `submit`'s put, counting exactly once.
- Both are additive to existing counters and touch no lifecycle ordering.

### Phase 2: The shutdown waits

- FR-001: release waiters on the expiry branch.
- FR-002: record an in-flight inline close and have the idempotent path wait on it with the
  remaining deadline, alongside its existing `_drain_settled` wait.

### Phase 3: The stranded sink

- FR-004: record a sink swapped out on an unconfirmed drain; release it detached on the
  successful-join path of `shutdown()`, ahead of `_join_closers`; correct the announcement.
- Update `architecture.md` §7, and the pipeline/lifecycle area of `docs/decisions.md`.

### Phase 4: Housekeeping and the ritual

- Widen the budget-vs-operation gap at `test_worker.py::test_submit_returns_before_emit_completes`.
- Record R12's rejection in `docs/decisions.md`; delivery doc; INDEX row; CLAUDE.md digest line.
