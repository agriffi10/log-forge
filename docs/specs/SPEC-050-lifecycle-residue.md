# Spec: Lifecycle Residue — Stranded Waiters, Unfinished Closes and Uncounted Loss

**ID:** SPEC-050  
**Status:** Completed  
**Last Updated:** 2026-09-02  
**Depends On:** SPEC-013, SPEC-021, SPEC-027, SPEC-030, SPEC-031, SPEC-036, SPEC-039, SPEC-042, SPEC-045, SPEC-046

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

The findings are R3, R4, R6, R11 and R13 of `docs/audits/2026-09-02-pre-1.0-audit.md`, which
landed on `main` while this spec was being built — an earlier draft said the audit was not a
document in this repo and that its identifiers resolved to nothing, which was true when it was
written and is not now. Each finding is still restated in full in the FR that owns it, so this
spec stands alone; the identifiers are there so a reader can find the original evidence.

## Scope

### In Scope

- The five findings above, every one reproduced by running it against `eb80099` before this spec
  was written.
- Correcting every site that states the now-false "the previous sink is left open" claim FR-004
  supersedes, *inside this repo's source and `docs/`*.
- One housekeeping item in the suite this spec already edits: the wall-clock budget at
  `tests/test_worker.py::test_submit_returns_before_emit_completes` asserts `< 0.1` against a
  0.3 s emit. A test proving an operation is *not* bounded needs a wide budget-vs-operation gap,
  not a tight budget; the tight one fails on its own setup under load.
- **Added by the spec review, said out loud rather than absorbed quietly:** FR-002 was widened
  from the worker path to *both* delivery paths. `_lifecycle._close_orphan_sink` empties
  `_orphan_owed` under the lock and then closes inline, so a second caller finds nothing owed and
  returns instantly — the identical defect, in a process that only ever logged outside a span.
  Fixing one path and not the other is the shape SPEC-033 had to widen once already.

### Out of Scope

- **`health().retired` reading `True` for a worker built after an orphan-only `shutdown()`.**
  Investigated and rejected as not a defect: `Health.retired` is documented as "whether
  `shutdown()` has been called", which is true there; `_worker_health` already reasons about this
  exact case in writing and chooses the `or` deliberately; the documented alert idiom is the
  *pair* `retired and submitted_after_shutdown`, which correctly stays silent. Measured: the fresh
  worker's events are not lost silently either — against a guarding sink they land in
  `failed_batches` with a line, and `flush()` returns `abandoned`. The reasoning goes into
  `docs/decisions.md` so a later reader stops here rather than re-finding it.
- Bounding `Sink.close()`. It is unbounded on both paths by architecture §13. FR-002 and FR-004
  make a *caller* wait for a close within a budget; neither bounds the close.
- Any change to `sinks/`, the public dataclasses' shape, `pyproject.toml`, or `README.md` — all
  owned elsewhere. `README.md:253` and `README.md:1009` state the claim FR-004 supersedes; the
  correction is **owed by the delivery doc and named in the PR body**, not performed here.
- A fourth `_diag` verb. FR-004's line is imprecise, not unwriteable, and widening the three-verb
  surface SPEC-029 settled is out of proportion to one sentence.

---

## Functional Requirements

### FR-001: A shutdown that expires releases the waiters it stranded

#### Description:

`Worker.shutdown`'s expiry branch records `stopped_reason`, sets `_drain_settled`, writes its
line and returns — before `_release_waiters()`, whose own docstring says it exists for this
caller.

**Three arrival orderings, and the audit's prescribed remedy covered one.** A `flush()`'s marker
can be queued when the sweep runs, held by the drain thread, or enqueued after the sweep has
already run — and each needed its own answer. The first two shipped in successive attempts; the
third was found by a reviewer on the release-notes session and closed in the same way.

**The audit's prescribed remedy did not cover the audit's own probe, and the first attempt
shipped it anyway.** Calling `_release_waiters()` there answers markers still *in* the queue.
Which markers those are is a race the caller does not control: a `flush()` whose marker arrives
while the drain is already inside `emit` stays queued and is swept, but one the drain dequeues
*before* blocking is held in that thread's local and reachable by nothing. Reproduced on merged
`main` by a peer session and confirmed here — `items in queue: 0, markers visible: 0`, flusher
still alive after `shutdown` returned. The drain thread now registers each marker it takes, and
the sweep answers those as well as the queued ones.

Reproduced before either half: `flusher still waiting after shutdown gave up: True`, a
`flush(timeout=None)` waiting forever on a drain this very call had given up on.

**The trade this makes, stated rather than assumed.** On the two existing call sites the drain
thread is finished or terminally dead, so `delivered=False` is simply true. Here it is not: the
branch is entered *because* the thread is still alive, and if its `emit` later returns,
`_final_drain` answers the same marker again with the real outcome. The waiter has already woken
and reported `"abandoned"` for a batch that may yet land. That is a false negative against
"`flush()` answers from the drain that carried the events", and it is accepted deliberately: the
alternative is an unbounded wait on a drain nothing will settle, and a pessimistic answer a
caller can act on beats a correct one it never receives. The events are unaffected either way —
`_final_drain` still carries them to the sink — so what is traded is a *verdict*, not delivery.

**The population that can receive that pessimistic verdict is wider than the sweep**, and the
widening is part of the trade rather than a side effect of it. It now covers a marker the drain
thread was *holding* when the sweep ran, and a marker taken *after* the sweep, which answers
itself. In none of these does a caller see `ok=True` over lost events: `delivered` starts `False`
and is only ever written by the owning drain, so every added path can produce `abandoned` and
nothing else. The direction is one-way by construction, which is what makes the trade acceptable
against `flush()` answering "from the drain that carried the events".

**It supersedes a settled decision, which is recorded in a test rather than in the register.**
`test_an_expired_shutdown_leaves_the_sentinel_for_the_live_thread` asserts the opposite and gives
the reason. That test is superseded **in place**, struck through with this spec named, not
deleted; its sentinel half is unchanged and still asserted.

#### Acceptance Criteria:

- [ ] With the drain thread blocked inside `sink.emit` and a `flush(timeout=None)` outstanding on
      another thread, a `shutdown(timeout=T)` that expires answers that flush before returning:
      the flushing thread is not alive one second after `shutdown` returns.
- [ ] That flush returns `FlushResult(ok=False, reason="abandoned")` — not `ok=True`, and not
      `"timed-out"`.
- [ ] The expiry branch still latches `stopped_reason="ShutdownTimeout"` and writes exactly one
      `lost` line, with the same text as before.
- [ ] A marker re-answered by a later `_final_drain` corrupts nothing: the sink still receives its
      batch exactly once, and no second `FlushResult` is produced.
- [ ] The same holds for the ordering the first fix missed: with the drain thread holding the
      marker rather than the queue, the flush is still answered — asserted separately, because the
      test written from the audit's probe passes with that half reverted.
- [ ] A marker taken by `_final_drain` rather than by the loop is answered too.
- [ ] A `flush()` whose marker is enqueued *after* the sweep has run is answered too: its post-put
      re-check consults `_drain_settled`, which is the only flag the expiry branch sets. It
      reports `abandoned` rather than `thread-died`, because the thread is alive.
- [ ] That condition does not relabel a drain that genuinely died, which still reports
      `thread-died` — asserted through the post-put branch, not the early liveness guard the
      existing test returns at.
- [ ] A marker taken *after* the sweep has already run answers itself: with `shutdown(timeout=0)`
      and the drain holding a marker it has not yet recorded, the `flush(timeout=None)` still
      returns `abandoned` rather than waiting on a drain that will never reach its own sweep.
- [ ] The record of taken markers returns to empty, over an emit that returns and one that raises.
      A deregistration that did nothing would leak one marker and one `Event` per `flush()` for
      the life of the process, and no other assertion here would notice.
- [ ] A forked child holds none of the parent's in-flight markers, and the repair walk does not
      reach them — asserted against the walk with a control, since the reset would mask the skip.
- [ ] Deleting the new call makes the first criterion fail (mutation-tested, not asserted).

### FR-002: A second shutdown waits for an in-flight inline close, on both delivery paths

#### Description:

`shutdown()` called first on a background thread reaches `_close_if_owed`, which latches
`_sink_closed` and then runs an unbounded inline `close()`. The `atexit` call that follows finds
the close already claimed and returns instantly, so the interpreter exits through a close that is
still running and kills it. For a sink whose `close()` *is* the delivery this is total loss of
whatever it had buffered. Reproduced with a close-is-delivery sink:
`main exiting at 0.31s; closes started=1 finished=0 wire=0 buffered=12`, and nothing on stderr.

`_lifecycle._close_orphan_sink` has the same shape: it removes every owed sink from
`_orphan_owed` under `_state._lock`, then closes, and a second caller takes its `if not owed:
return` immediately. Both paths need the fix.

This is the open half of the 2026-08-07 audit's C3: the drain half was fixed by having the
idempotent path wait on `_drain_settled`, and the close half needs the same shape.

**The wait is capped at `DEFAULT_CLOSER_GRACE`, not at the caller's whole budget.** Waiting the
full `DEFAULT_SHUTDOWN_TIMEOUT` would make a *stuck* close cost 30 s at exit where it costs ~0 s
today. `_join_closers` already answers this exact question — how long may an exit wait on a close
it does not own — with `min(DEFAULT_CLOSER_GRACE, remaining)`, and that constant's own docstring
argues the case ("every second spent on it is a second the process does not exit"). Using the
same rule makes this cost identical to the sibling mechanism rather than fifteen times it.

**Forked-child state.** The new event and flag join the six attributes `Worker._reinit_after_fork`
resets rather than inherits. `_fork._fresh_primitive` carries an `Event`'s set state across a
fork, so a child forked while the parent was inside the inline close would otherwise inherit
"a close is running" with nothing alive to finish it, and pay the full wait at every later
`shutdown()` — the measured defect that docstring already records for `_drain_settled`.

#### Acceptance Criteria:

- [ ] With `shutdown()` parked inside the sink's `close()` on another thread, a second
      `shutdown(timeout=T)` does not return until that close has finished, for any close shorter
      than both `T` and `DEFAULT_CLOSER_GRACE`.
- [ ] Against a close-is-delivery sink, a first `shutdown()` on a daemon thread followed by
      process exit leaves the sink's buffer delivered — measured by an `atexit` probe registered
      *before* the library's own handler, so LIFO runs it last.
- [ ] The same holds on the orphan-only path: a process that only ever logged outside a span, with
      `shutdown()` first called on a daemon thread, delivers its close-is-delivery sink's buffer.
- [ ] The second call stays bounded: against a `close()` that never returns, `shutdown(timeout=T)`
      returns within `min(T, DEFAULT_CLOSER_GRACE)` of reaching the wait, and never later than
      `T` plus the grace `_join_closers` already takes.
- [ ] `shutdown(timeout=0)` on the idempotent path returns promptly and never inherits the first
      caller's deadline.
- [ ] A second `shutdown()` with no close in flight returns without waiting.
- [ ] The waiting caller neither performs a second `close()` nor reports one.
- [ ] A child forked while the parent is inside the inline close does not wait for it: the child's
      own `shutdown()` returns without paying the grace.

### FR-003: A span whose events cannot reach a worker counts them

#### Description:

When the process cannot give the library another thread, `Worker.__init__`'s `Thread.start`
raises, `_get_worker` propagates it, and `decorator._end` absorbs it with one stderr line — and
counts nothing. Every span's events are lost with `Health(stopped_reason=None, in_span_lost=0)`.
`_reinit_after_fork` sets `stopped_reason` for the identical failure; this path is silent to
every field. Reproduced: two traced calls, stderr twice, `in_span_lost=0`, delivered 0.

**The count needs a number `_end` does not currently have.** `decorator._flush` detaches the
span's buffer into a local *before* it resolves the worker, so on the failing path the events die
with the exception and `len(span.events)` at `_end` is 0. Rather than add a second count site
inside `_flush`, the detach moves to *after* `_get_worker()` returns: the events then stay on the
span until there is a worker to take them, one site counts them, and both failure populations —
a fault before the end event was appended and one after — are visible to it. Moving the detach
later narrows the SPEC-036 FR-004 window it exists to close rather than widening it, and
`Worker.submit` is documented `Raises: None`, so nothing between the detach and the hand-off can
fail.

`in_span_lost` is the field: these events were lost while the span was being closed, which is the
population SPEC-036 FR-003 defined it over. Surfacing a `stopped_reason` instead was the audit's
other suggestion and is rejected: there is no worker to carry it, so it would need module state
synthesized the way `retired` is, for a signal that names a cause where the caller needs a count.

**It falsifies two docstrings, and they are part of this FR.** `Health.in_span_lost` (public) and
`decorator._note_in_span_loss` both say the path "cannot fail at `emit`", so a non-zero count
"always means **the data**, never the destination". After this it can also mean the process could
not give the library a thread to deliver through. Both are corrected here.

#### Acceptance Criteria:

- [ ] With `Thread.start` refusing, two traced calls each carrying one `info()` leave
      `health().in_span_lost == 6` — three events per span, not 2 and not 0.
- [ ] The existing `absorbed` line is unchanged and still written once per span.
- [ ] A span that closes normally leaves `in_span_lost` at 0, and its events are delivered in the
      same order as before the detach moved.
- [ ] A span that fails *after* the end event was appended counts that event too; one that fails
      before it counts only what the span held.
- [ ] A span swept by `_sweep_open_spans` while `_get_worker()` is blocked is not double-counted
      and its events are not delivered twice.
- [ ] `orphan_lost` is untouched by this path.
- [ ] `Health.in_span_lost` and `_note_in_span_loss` no longer claim the count always means the
      data, and the new cause is named in both.

### FR-004: A sink stranded by an unconfirmed swap is closed once the drain thread has ended

#### Description:

A `configure(sink=B)` whose drain of A cannot be confirmed within the swap budget installs B,
counts `incomplete_swaps`, and leaves A **open forever**. The reason is sound at that instant —
the drain thread may still be inside `A.emit`, and closing under a live writer is the failure
SPEC-027 and SPEC-028 exist to prevent. It stops being sound once the drain thread has ended: A
is then provably out of use *by the drain thread*, which is the same condition
`_close_swapped_out` already treats as sufficient. (An orphan emitter on an application thread is
not covered by that condition and never was — `sinks/base.py` requires `close()` to tolerate
exactly that concurrent `emit`, and this inherits the contract rather than weakening it.)
Reproduced: after an unconfirmed swap and a clean `shutdown()`, `A.closes=0` with nine events
still in A's client buffer — for a close-is-delivery sink that is silent loss, not a leaked
handle.

**Where the close is performed decides how much it covers.** `_close_if_owed` already asks "has
the drain thread ended?" under `_lock`, and `shutdown`'s own docstring says of the live sink that
an expired first call defers rather than abandons its close, because "a later call finds the
thread finished and closes the sink then". Draining the record there rather than on `shutdown`'s
success branch gives the stranded sink that same second chance, and taking the record out under
the lock *is* the once-only latch — `_lifecycle.release()` guards only process ownership and
latches nothing about "already closed", so every double-close route in this codebase is closed by
construction at its call site.

**The record must be pruned wherever a close is decided elsewhere**, or two routes close A twice:
a sink swapped out unconfirmed, re-adopted, then swapped out again on a *confirmed* drain, where
`_close_swapped_out` closes it; and the orphan path, whose re-arm guard `_orphan_closed_sink` is a
single slot that a second unconfirmed swap overwrites.

**The record holds sinks the process has stopped delivering to, so it must not be walked after a
fork.** That is what `_FORK_SKIP` exists for: `_fork`'s repair walk would otherwise re-enter
superseded sinks, replace their locks and re-run their fork hooks, and `_lifecycle.reclaim` would
overwrite the `_FOREIGN` stamp `_mark_inherited` set — leaving a child able to close a sink it
never acquired. `Worker` declares no `_FORK_SKIP` today and will need one.

This supersedes the "left open" claim at `architecture.md` §7, `docs/decisions.md`'s SPEC-030
entry (under *The sink contract: waiting, concurrency and shutdown*, **not** the pipeline area),
and four `worker.py` docstrings including the public `Health.incomplete_swaps`. It does **not**
touch SPEC-027's claim about an *expired* `shutdown()` leaving the live sink open, which stays
true.

#### Acceptance Criteria:

- [ ] After a `configure(sink=B)` whose drain of A was not confirmed, a `shutdown()` whose join
      succeeds closes A exactly once, and a client-buffering A has delivered its buffer when
      `shutdown()` returns.
- [ ] A `shutdown()` that expires with the drain thread still alive leaves A open; a *later*
      `shutdown()` that finds the thread finished closes it then.
- [ ] A is never closed twice by any route: swapped out unconfirmed then re-adopted as the live
      sink, and swapped out unconfirmed, re-adopted, then swapped out again on a *confirmed*
      drain. A third route — an orphan re-arm after a second unconfirmed swap overwrites the
      single `_orphan_closed_sink` slot — is guarded by the same prune and is **not** asserted:
      no reachable call sequence for it was found, and a test for an unconstructable scenario
      passes vacuously.
- [ ] Two successive unconfirmed swaps A→B→C leave both A and B closed exactly once after
      `shutdown()`.
- [ ] A *stranded* sink whose `close()` never returns cannot extend the budget: `shutdown(timeout=T)`
      returns within `T` plus the grace `_join_closers` already takes, with the live sink's own
      close fast.
- [ ] A *confirmed* swap records nothing: `_close_swapped_out` remains the only closer on that
      path, and the record is empty at `shutdown()`.
- [ ] `Worker` declares `_unclosed_swaps` in a `_FORK_SKIP`, and a fork with a stranded sink
      recorded does not reach that sink: it is not relocked, its fork hooks do not run, and it
      stays refused by `releasable()` in the child.
- [ ] The incomplete-swap stderr line no longer says the sink is left open unconditionally; it
      says a later `shutdown()` closes it once the drain thread has ended.
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

The unlocked fast path stays unlocked. The fix is a second read *after* the put — the same
double-check `flush()` already performs against `_drain_finished` for the same race — and it
closes the window rather than narrowing it: `_shutdown_done` latches under `_lock` at the top of
`shutdown`, strictly before the sentinel and before `_stop.set()`, therefore strictly before
`_final_drain`. Any submission that *can* be stranded has the flag already latched when the
post-put read runs.

#### Acceptance Criteria:

- [ ] With a submission parked at its `put_nowait` and released after `shutdown()` has joined the
      drain thread, `health().submitted_after_shutdown == 1` and one line is written.
- [ ] A submission that read the flag as already latched is counted exactly once, not twice.
- [ ] A submission entirely before `shutdown()` is not counted.
- [ ] The uncontended path takes no lock it did not take before.
- [ ] `health().queued` still reports the stranded item, unchanged.
- [ ] A submission dropped by a full queue is not counted as stranded.

---

## Data Model

```python
# src/log_foundry/worker.py — Worker, new private state only. No public type changes.

_FORK_SKIP = ("_unclosed_swaps",)   # a record of sinks the process stopped delivering to
_close_finished: threading.Event    # set when an inline _close_sink() returns, however it returned
_close_running: bool                # under _lock; True between claiming the close and finishing it
_unclosed_swaps: list[Sink]         # swapped out on an unconfirmed drain, owed a close at shutdown

# src/log_foundry/_lifecycle.py — module state for the orphan path's half of FR-002.

_orphan_close_finished: threading.Event
_orphan_close_running: bool         # under _state._lock
```

`Health` is unchanged: FR-003 and FR-005 move counters that already exist.

---

## API / Interface Contract

No public signature changes. The observable differences are:

```
flush(timeout=None)            # answered, not stranded, when a bounded shutdown expires  (FR-001)
shutdown(timeout=T)            # second caller waits out an in-flight close, capped        (FR-002)
health().in_span_lost          # counts a span whose events could not reach a worker       (FR-003)
health().submitted_after_shutdown  # counts a submission that raced the final drain        (FR-005)
configure(sink=B) + shutdown() # closes an unconfirmed-swap sink once the drain has ended  (FR-004)
```

## Configuration / Environment

None. No new knobs, constants or extras — FR-002 reuses `DEFAULT_CLOSER_GRACE`.

## File & Folder Structure

```
src/log_foundry/
├── worker.py        # FR-001, FR-002, FR-004, FR-005 (+ four superseded docstrings)
├── _lifecycle.py    # FR-002's orphan-path half
└── decorator.py     # FR-003
docs/
├── architecture.md  # §7 swap paragraph, §9 shutdown
├── decisions.md     # the sink-contract area (FR-004's reversal) + the rejection above
└── specs/SPEC-050-lifecycle-residue.md
tests/
├── test_worker.py             # FR-001, FR-002, FR-004, FR-005 + the budget housekeeping
├── test_shutdown_lifecycle.py # FR-002's orphan path and exit-path coverage
├── test_fork_lifecycle.py     # FR-002's and FR-004's forked-child criteria
├── test_worker_predicate_roster.py  # FR-003's new worker-question site, classified
├── test_config.py             # a monkeypatch that mirrors _close_if_owed's signature
└── test_decorator_sync.py     # FR-003
```

## Implementation Phases

### Phase 1: The two counters

- FR-003: move `_flush`'s detach after `_get_worker()`, and count `len(span.events)` where `_end`
  absorbs a close failure.
- FR-005: re-read `_shutdown_done` after `submit`'s put, counting exactly once.
- Neither touches lifecycle ordering.

### Phase 2: The shutdown waits

- FR-001: release waiters on the expiry branch.
- FR-002: record an in-flight inline close on both paths; have the idempotent path wait on it for
  `min(DEFAULT_CLOSER_GRACE, remaining)`; reset the new state in `Worker._reinit_after_fork`.

### Phase 3: The stranded sink

- FR-004: record a sink swapped out on an unconfirmed drain, with `Worker._FORK_SKIP` and the
  prune rule; drain the record from `_close_if_owed` once the drain thread has ended; correct the
  announcement and the four superseded `worker.py` docstrings.
- Update `architecture.md` §7 and the sink-contract area of `docs/decisions.md`.

### Phase 4: Housekeeping and the ritual

- Widen the budget-vs-operation gap at `test_worker.py::test_submit_returns_before_emit_completes`.
- Record the rejection above in `docs/decisions.md`; delivery doc; INDEX row; CLAUDE.md digest
  line; name the two owed `README.md` corrections in the delivery doc and the PR body.
