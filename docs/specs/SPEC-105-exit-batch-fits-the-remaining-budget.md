---
type: Spec
id: SPEC-105
title: "Once an Exit Batch Is Delivered, Each Later One Fits the Remaining Shutdown Budget"
description: "Records the rate of the exit drain's most recent delivered batch and, after every exit batch from then on, delivered or failed, holds the next batch to what that rate fits into the time left before the shutdown() deadline less the join margin, whenever the exit budget's prediction would let that batch start, so a sink healthy or flapping at exit is never handed a batch the prediction did not allow for, and a tight shutdown() completes its join and closes the sink, with no setting, line, tally or Health field added."
spec_status: Completed
depends_on: [SPEC-062, SPEC-090]
---

## Overview

At `shutdown()`, the worker sends what is queued in batches and holds them to the caller's
timeout: the first exit batch is always attempted, and a later one is declined when the longest
attempt any exit batch has made, plus a 0.1 s join margin, predicts it would end past the deadline.
Between batches, each delivered batch sets the next batch's size to half a flush interval of the
sink's measured rate. The two rules do not see each other. A sink healthy at exit whose first exit
batch is small (ten spans, because the last batch before the stop failed) delivers it quickly, and
the rate rule then makes the next batch more than ten times longer than any attempt the prediction
has seen. With 2 ms an event, 20 events in 0.04 s lead to a second batch of 220 to 250 events,
about 0.5 s of work, and the prediction, which allows for 0.04 s plus the margin, lets it start.
When the failing pre-stop batch returns 0.2 s into the budget, leaving the exit drain the timeout
less about 0.2 s, a `shutdown()` budget of 0.45 to 0.65 s sees that batch end past the deadline:
the join expires, and the sink is left open. That is the outcome the exit budget exists to
prevent (invariants 3 and 5). A sink that flaps at exit reaches the same outcome by another route:
a target set after a delivered batch is kept through the failed batches after it, while the time
they spend shrinks the budget.

This spec closes both. The worker records the rate of the most recent delivered exit batch, and
after every exit batch from then on, delivered or failed, the next batch is held to what that rate
fits into the time left before the deadline, less the join margin, whenever the prediction would
let that batch start. With an ample budget this never binds, and the batches are what they are
today; with a tight one the sink is handed fewer events, the join completes and the sink is closed.
Nothing a caller configures changes, and no line, tally or `Health` field is added.

**One band changes outcome, by Andrew's choice.** When the rate rule's next batch would end inside
the last `_JOIN_MARGIN` before the deadline, today's code starts it, the join completes, and every
event is delivered: by luck, since the margin is what the join needs to see the drain end. With the
bound that batch is trimmed to end by the deadline less the margin, and SPEC-062's prediction
declines the tail, which is counted and announced. Andrew chose this on 2026-11-17: the 0.1 s
margin is the rule everywhere at exit, the same trade SPEC-062 made for retries. The design was
prototyped and measured, and re-measured on `main` on 2026-11-24 (*Evidence*); the alternatives
considered are under *Revision history* and in FR-006.

This spec closes the first shape of the `docs/architecture.md` §12 item "The exit drain sizes a
batch from a rate it measured earlier…", a sink that recovers by exit; that item's other shapes
stay open (*Out of Scope*).

## Scope

### In Scope

- One new drain-thread field, `_exit_rate`, the rate of the most recent delivered exit batch,
  reset with the other exit fields and listed in the lifecycle inventory (FR-001).
- After a delivered exit batch, the next size target is at most what that rate fits into the time
  left before the deadline less `_JOIN_MARGIN` (FR-001).
- After a failed exit batch whose emit did not straddle a sink change, once a rate has been
  recorded, the same bound, lowering the target that `main` keeps through that failure and never
  raising it (FR-002).
- The bound applies only when the time left less the margin is at least the longest exit attempt,
  which is when SPEC-062's prediction would let the next batch start; and a declined exit batch,
  once a rate is recorded, restores the rate rule's target the bound lowered (FR-003).
- The end-to-end outcome at a tight budget: the join completes, `close()` runs once, and every
  event is delivered or counted in an existing line, on the 1.x and the engine path (FR-004).
- Where the budget does not bind, the batches are what they are today; in the last margin before
  the deadline, the batch is trimmed and the tail declined (FR-005).
- The record: the two decisions, the docstrings, the inventory, the §12 item's first shape, a
  qualification at each site the item lists as shown false, SPEC-103's marker, the README and the
  pending release notes (FR-006).

**The twin paths** (invariant 6). The bound sits in `Worker._emit_pending`, which every batch of
both drains goes through, above both branches of `_emit`: the 1.x `emit` loop and the delivery
engine's run. A carry from `_take`, the batch ahead of a flush marker (`_answer_marker`) and the
exit drain's last batch all reach it. It acts only on an exit batch, so `_drain` and `_final_drain`
keep sharing every rule but this one, which, like the exit budget it serves, exists only at exit.

### Out of Scope

- **The target kept through a failed exit batch.** On `main` an exit batch counted in
  `failed_batches` leaves the target as it was (SPEC-090 FR-001 rule 4, refined in its diff review
  on 2026-11-16), as a declined one does. This spec leaves that rule as it is and only lowers the
  kept target afterwards (FR-002). Before any exit batch has been delivered no rate is recorded,
  and the kept target stands as on `main`.
- **The §12 item's other shapes.** A sink that slows during the exit, a retry predicted from a fast
  failure, and a sink slow from its very first exit call are not closed here. The bound trusts the
  recorded rate, which a slowdown falsifies; it sizes batches and leaves `_retry_declined`'s
  prediction of a retry as it is; and before any exit batch has been delivered it records no rate.
  The §12 item stays open for those three, with no closer named.
- **A failed exit batch whose emit straddled a sink change.** It keeps its target, as `main` has
  it, and the bound is not applied after it.
- **Keying the rate by sink generation.** No `retarget` can reassign during a `shutdown()`'s exit
  drain: `_shutdown_worker` bumps `retirements` and claims the stop in one `_state._lock` section,
  and `retarget` checks `retirements > self._epoch` under `Worker._lock` before its reassign. So
  the sink-change handling on the exit path is defensive. Were the sink changed between exit
  batches, the rate recorded from the old sink would stand until the new sink delivers an exit
  batch, which records its own, and a bound applied in between, after the new sink's failed batch,
  could lower only a target stamped with the old generation, which `_size_target` already reads as
  zero, so it would change no batch; keying the rate by generation would need a second attribute
  for no effect (FR-002 pins this through a stand-in for `retarget`'s locked reassign).
- **A bound after a declined exit batch.** A declined batch is not emitted, records nothing and is
  followed only by declined batches while the deadline stands, so no bound is applied after it;
  once a rate is recorded it restores the rate rule's target instead (FR-003).
- **An exit batch whose emit the clock cannot see.** FR-001 records no rate when `elapsed` is zero
  or below, and `_set_target` reads that as the cap. On a monotonic clock coarser than a fast small
  exit batch's emit, that batch reads zero elapsed, records no rate, and the next batch goes out at
  the cap with no bound: the §12 item's first shape again, for a fast sink. How coarse the clock is
  depends on the platform (`time.get_clock_info('monotonic').resolution` reports it). Keeping the
  previous rate when `elapsed <= 0` would close it for every exit batch but the first, which would
  stay unmeasured. It is recorded as a remainder of the first shape in the §12 item, with no closer
  named, by diff review frame 2.
- **The first exit batch.** Its size is whatever the target read at the stop, and its first attempt
  is always made (SPEC-090 FR-003). A target measured before the stop is not capped, so a first
  exit batch can still be longer than the budget; that is the first-attempt rule's own trade.
- **A sink whose per-event cost rises within the exit drain**, the §12 item's second shape above.
  The bound assumes the next batch runs at the recorded rate, as SPEC-062's prediction assumes the
  next attempt is no longer than the longest seen.
- **A batch the bound cannot shrink.** A batch is never fewer than `batch_size` submissions, so
  when those spans carry more events than the bound allows, the batch is longer than the bound.
  That limit exists today for every size target.
- **The steady drain**, whose batches the bound never reads, including a batch whose first attempt
  began before the stop was claimed.
- **`NATSSink`'s abandoned-publish latch at exit** (SPEC-104).
- **Any setting, argument, environment variable, diagnostic line, tally, `failed_batches` meaning
  or `Health` field**, which Andrew's answer of 2026-11-15 ("Larger batches, no setting") rules out
  for every batch size (invariant 10).

---

## Functional Requirements

### FR-001: After a delivered exit batch, its rate is recorded and the next is held to what it fits in the time left

#### Description:

**Today.** `_emit_pending` times each batch's `_emit` and, for a delivered batch, calls
`_set_target`, which sets the target to `min(cap, events / elapsed × flush_interval / 2)`. An exit
batch counted in `failed_batches` returns before `_set_target`, keeping the target as it was.
Before a later exit batch, `_retry_declined` declines it when `now + _exit_longest + _JOIN_MARGIN`
passes the deadline. The prediction assumes the next attempt is no longer than the longest seen,
and the rate rule makes no such promise: after a short delivered exit batch, the next can be many
times longer.

**The rate.** A delivered exit batch whose emit did not straddle a sink-generation change and whose
`elapsed` is above zero sets `Worker._exit_rate` to `size / elapsed`, in events a second. Any other
delivered exit batch sets it to `0.0`, meaning none recorded: an `elapsed` of zero or below is a
clock that cannot see the emit, whose rate is unbounded, and `_set_target` already gives that batch
the cap; a straddling batch measured a sink no longer installed. A failed exit batch leaves the rate
as it was. `_exit_rate` is a drain-thread `float`, set to `0.0` in `Worker.__init__` beside
`_exit_longest` and in `_reinit_after_fork` beside it, before the `resume` split, so one reset
covers both branches; it is listed in `tests/test_lifecycle_inventory.py`'s Worker table with
`_REBUILD`, as `_exit_longest` is.

**The bound.** After `_set_target` has run for a delivered exit batch, while the deadline is not
`None` and a rate is recorded, one private method, `_fit_exit_target`, computes
`room = deadline − now − _JOIN_MARGIN`, where `now` is `time.monotonic()` read then and
`_JOIN_MARGIN` and the deadline are read at that use. When FR-003's guard passes it lowers the
target to at most `_exit_rate × room`. At the recorded rate, the next batch's attempt then ends by
the deadline less the margin: a batch the prediction lets start fits the budget by its size, rather
than by the prediction's assumption about its length, as far as its `batch_size` submissions allow
(Out of Scope). The bound only ever lowers the target.

`elapsed` is the whole `_emit`, retries and the engine's waits included, so a batch delivered
after a retry records a lower rate and gets a smaller bound.

**FR-001's clock.** The fake-clock criteria here and in FR-002, FR-003 and FR-005 share one
premise: the worker's clock is one only the sink advances, starting at 0.0; the pre-stop call costs
nothing on it, so the first exit batch begins at 0.0, and every exit call costs 0.00390625 s an
event; `_JOIN_MARGIN` is patched to 0.0390625 s; the interval is 1.0 s; `max_retries` is 0;
`batch_size` is 10 and every span carries two events; the pre-stop batch fails, so the target is
zero at the stop; and the deadline is set on that clock while the first exit batch is in the sink.
Every value below is exact in binary unless it says otherwise.

#### Acceptance Criteria:

- [ ] On FR-001's clock (the pre-stop call charged nothing) with the deadline at 0.328125 and 500
      spans queued, the first two exit batches are 20 and 54 events, and the declined line names
      926 events in 8 batches. Without the bound they are 20 and 128 (invariant 3).
- [ ] The engine twin, a `deliver` sink with the same per-event cost, `max_retries=0` at both
      levels and the same clock and deadline, is handed the same 20 and 54 (invariant 6).
- [ ] On FR-001's clock (the pre-stop call charged nothing) with a first exit batch that costs no
      time, the deadline at 0.328125 and 500 spans queued, `_exit_rate` reads 0.0 when the second
      exit batch reaches the sink, and that batch is the whole remaining backlog, 980 events, as
      the cap allows (invariant 3).
- [ ] `_reinit_after_fork` sets `_exit_rate` to 0.0 with `resume=True` and with `resume=False`, and
      `tests/test_lifecycle_inventory.py` lists it with its child column (invariant 12).
- [ ] Serves invariants 3, 6 and 12.

### FR-002: After a failed exit batch, the recorded rate holds the next to the time left as well

#### Description:

On `main`, an exit batch counted in `failed_batches` returns from `_emit_pending` before
`_set_target`, so the target is kept as it was (SPEC-090 FR-001 rule 4, refined in its diff review
on 2026-11-16). So the bound FR-001 applied after a delivered batch, computed against the time left
then, stands through the failed batches after it, while the time they spend shrinks what is left.
A sink that delivers, fails a few times quickly, then delivers again is handed a batch sized for a
budget that has gone.

So after every failed exit batch whose emit did not straddle a sink-generation change,
`_fit_exit_target` applies FR-001's bound, before `_emit_pending` returns, with the rate recorded
from the most recent delivered exit batch, against the time left now. Before any exit batch has
been delivered, no rate is recorded and the kept target stands, as on `main`. The bound takes the
minimum, so it never raises the kept target; a target that reads zero stays zero.

#### Acceptance Criteria:

- [ ] On FR-001's clock (the pre-stop call charged nothing) with the deadline at 0.578125 and 500
      spans queued, take a sink that delivers the first exit batch, fails the next three after
      0.0625 s each, and delivers the fifth. The exit batches are 20, 118, 102, 86 and 70 events,
      and the clock reads 0.5390625, the deadline less the margin, when the fifth returns
      (invariants 3 and 5).
- [ ] Removing the bound from the failed path makes that test fail: the exit batches are 20, 118,
      118, 118 and 118, and the fifth ends at 0.7265625, past the deadline. This is mutation-tested
      (invariants 3 and 5).
- [ ] An exit batch that fails across a sink change, made through a stand-in for `retarget`'s
      locked reassign (`_reassign` in the tests, since `retarget` itself refuses during a
      `shutdown()`'s exit drain), with a rate recorded and a deadline under which the bound would
      lower the target, leaves `_target` and `_target_generation` as they were before it. This pins the state rather than an outcome (a stale-generation target reads 0.0
      either way, Out of Scope), so a mutant that drops the generation check is visible
      (invariant 6).
- [ ] On FR-001's clock (the pre-stop call charged nothing), after a sink change between two exit
      batches through that stand-in, with a rate recorded from the old sink at 0.00390625 s an event, a new sink at
      0.0078125 s an event, and the new sink's first exit batch failing, `_size_target()` reads 0.0
      before the next exit batch, and after the new sink's first delivered exit batch `_exit_rate`
      reads 128.0, the new sink's rate, not the old sink's 256.0 (invariant 6).
- [ ] A dead sink at exit with no exit batch delivered is handed exactly what `main` hands it, the
      target kept through each failed exit batch, and `_exit_rate` reads 0.0 throughout;
      `test_a_dead_engine_sink_at_exit_is_handed_batches_at_the_cap` and
      `test_a_refused_http_sink_at_exit_is_handed_batches_at_the_cap` pass unchanged (invariant 2).
- [ ] Serves invariants 2, 3, 5 and 6.

### FR-003: The bound applies only when the prediction would let the next batch start, and a decline restores the rate rule's target

#### Description:

`_fit_exit_target` lowers the target only when `room >= _exit_longest`, which is when SPEC-062's
prediction, asked at that moment, would let the next exit batch start. Otherwise it leaves the
target as it is. Below that line the next batch is declined unattempted whatever its size, so a
smaller target buys nothing, and it would cut the declined backlog into more batches: once the
deadline has passed, `room` is negative, and a bound applied there drove the target to zero or
below, so the declined backlog went out in `batch_size`-submission batches and the declined line
named dozens where it names one. Because `room` is then at least `_exit_longest`, the bound is
positive whenever `_exit_longest` is, so it never sets a target to zero, which `_set_target`'s rule
reads as unmeasured: the next batch is then the count trigger's.

**A declined exit batch restores the rate rule's target** (diff review frame 2, 2026-11-24). The
guard is asked when an exit batch returns; the prediction is asked again before the next batch,
after its items were taken. When the room at the fit is at least `_exit_longest` but less than
`_exit_longest` plus the time from the fit to the next prediction, which can include the take, a
flush marker's answer or a wait for the GIL, the guard lowers the target, the next batch is
declined anyway, and the whole declined backlog was cut at the lowered target: 980 events in 49 batches where `main` names 8, on
both twins. So when an exit batch is declined and a rate is recorded, `_emit_pending` sets the
target to `min(cap, _exit_rate × flush_interval / 2)`, what `_set_target` gave the batch that
recorded the rate, with no new attribute. The first declined batch was already taken at the
lowered target, so the cost is at most one declined batch more than the rate rule's target alone
cuts the backlog into, and only when the room falls in that window, from the fit to the next
prediction, whose size this spec does not measure: an accepted trade (980 events in 9 batches).
With no rate recorded, a decline leaves the target as it was, as SPEC-090 has it. The restore can
also set a target the bound did not lower, read from the code, and each such case changes only
how a backlog already declined is cut: an empty exit batch after a rate is recorded zeroes the
target and a later decline restores the rate rule's, which cuts the declined backlog into fewer
batches; a `flush_interval` a caller changed since the rate was recorded, which the restore reads
where `_set_target` read the old one; and a forked child's stale rate, which `_reinit_after_fork`
describes. None delivers an event differently. The cut can differ after a takeover that replaces
the deadline with `None` following a decline: the rest of the backlog is then delivered, cut at
the restored target, as `main` cuts it (`[20, 128 × 7, 64]`, where without the restore it is
`[20, 20, 128 × 7, 44]`).

**The guard's failure is nearly silent.** Since the restore, the guard decides only the size of
the first declined batch. Without it, or with it weakened to `room > 0`, no event is lost or
delivered differently and no line appears or disappears; the count of declined batches in the
existing line moves by at most one. The two mutants are killed by different criteria. Removing the guard
fails `tests/test_event_ledger_defects.py::test_an_expired_shutdown_leaves_the_batch_in_flight`
with `lost 960 event(s); 2 exit batch(es) declined unattempted` where it pins 1. The guard weakened
to `room > 0` passes that test, because `room` is negative once the deadline has passed and the
weakened guard refuses there too, and it passes the 0.6 s, 200-span criterion, which gives 258
events in 3 batches either way; the 0.6 s, 199-span criterion kills it.

#### Acceptance Criteria:

- [ ] `tests/test_event_ledger_defects.py::test_an_expired_shutdown_leaves_the_batch_in_flight`
      passes unchanged, with its `lost 960 event(s); 1 exit batch(es) declined unattempted`.
      Removing the guard makes it fail with 2 declined batches; this is mutation-tested
      (invariants 2 and 11).
- [ ] On FR-001's clock (the pre-stop call charged nothing) with the deadline at 0.09765625 and 500
      spans queued, so the time left is below the margin after the first exit batch, no later exit
      batch is handed to the sink, the target reads 128.0, and the declined line names 980 events
      in 8 batches. Without the guard it names 9 (invariants 2 and 3).
- [ ] On FR-001's clock (the pre-stop call charged nothing) with the deadline at 0.6 and 200 spans
      queued, the exit batches handed are 20 and 122, and the declined line names 258 events in 3
      batches (invariants 2 and 3).
- [ ] On FR-001's clock (the pre-stop call charged nothing) with the deadline at 0.6 and 199 spans
      queued, the exit batches handed are 20 and 122, and the declined line names 256 events in 2
      batches. With the guard weakened to `room > 0`, or removed, it names 3; this is
      mutation-tested, and is the criterion that kills the weakened guard (invariants 2 and 3).
- [ ] On FR-001's clock (the pre-stop call charged nothing) with the deadline at 0.1953125, 500
      spans queued and the clock moved on 2**-20 s after the first exit batch returns, no later
      exit batch is handed to the sink, the declined line names 980 events in 9 batches, and the
      target reads 128.0 after the join, on the 1.x and the engine path. Without the restore it
      names 49; this is mutation-tested (invariants 2 and 6).
- [ ] On FR-001's clock (the pre-stop call charged nothing) with a first exit batch that costs no
      time, the deadline at 0.03125 and 1,000 spans queued, no later exit batch is handed to the
      sink, the declined line names 1,980 events in 2 batches, and the target reads 1000.0. A
      restore made without a recorded rate names 50; this is mutation-tested (invariant 2).
- [ ] On FR-001's clock (the pre-stop call charged nothing) but at 2**-12 s an event, 4,096 events
      a second, so the rate rule's 2,048 is above the 1,000-event cap, with the deadline at 0.045
      and 5,000 spans queued, no later exit batch is handed to the sink, the declined line names
      9,980 events in 10 batches, and the target reads 1000.0. A restore without the cap names 6;
      this is mutation-tested (invariants 2 and 3).
- [ ] On FR-001's clock (the pre-stop call charged nothing) with the deadline at 0.1953125, 500
      spans queued and no time between the fit and the prediction, the room equals the longest
      attempt: the exit batches handed are 20 and 20, the second returning at 0.15625, and the
      declined line names 960 events in 8 batches. A guard of `room > _exit_longest` hands 20 and
      128; this is mutation-tested (invariant 3).
- [ ] In FR-001's first criterion, after the 54-event batch the time left less the margin is
      exactly 0.0, and `worker._target` reads 128.0 after the join, never 0.0 (invariant 3).
- [ ] Serves invariants 2, 3, 6 and 11.

### FR-004: At a tight budget the join completes, the sink is closed, and every event is accounted

#### Description:

The end-to-end outcome on the real clock, with `tests/test_exit_drain_batches.py`'s fixtures: a
1.0 s interval, `max_retries=0`, a sink healthy at exit at 2 ms an event, and 2,000 two-event spans
queued. The pre-stop call, released by the stop, idles 0.2 s and then fails, so the target is zero
at the stop and the exit drain has the timeout less about 0.2 s. That idle is what makes the defect
reproduce: a pre-stop call that returns the instant the stop is claimed, as the file's other tests
have it, leaves the exit drain nearly the whole timeout, and with the delivered-path bound removed
the join then still completed in 2 of 10 runs at 0.45 s and 6 of 6 at 0.5 to 0.65 s in the spec
review; with the idle, that mutant expired 10 of 10 at 0.45 s on both twins. The committed probe
`docs/audits/2026-11-14-probes/core-pipeline/spec103_recovered_slow_exit.py` (cited, not copied)
gives this shape from a script, its pre-stop batch failing about 0.2 s into the budget as the §12
item describes.

An expired join still returns within its budget, so elapsed time alone cannot tell this design
from a tree without the bound. `stopped_reason` and the count of `close()` calls at the return are
the assertions that do.

#### Acceptance Criteria:

- [ ] In the shape above, the pre-stop call idling 0.2 s, under `shutdown(timeout=0.45)` the call
      returns within 0.45 s plus the file's `_SLACK`, `stopped_reason` is `None`, no expiry line
      is written, and `close()` has run once when it returns (invariants 3 and 5).
- [ ] In that run, the events the sink returned from, the pre-stop batch's 20 in its abandoned line
      and the count in the declined line add to 4,020, and `ledger_check.worker_problems(worker)`
      is empty (invariant 2).
- [ ] Removing the bound from the delivered path makes the first criterion fail on `stopped_reason`
      and the `close()` count: the second exit batch is the rate rule's, and the join expires. This
      is mutation-tested and recorded in the delivery doc (invariants 3 and 5).
- [ ] The engine twin, a `deliver` sink at 2 ms an event with `max_retries=0` at both levels, under
      the same budget, ends with `stopped_reason` `None`, `close()` run once at the return, and the
      ledger empty of problems (invariant 6).
- [ ] `spec103_recovered_slow_exit.py 2 2000 <timeout>` completes the join at timeouts of 0.45,
      0.5, 0.6 and 0.65 s on 3.12 and 3.13, in two runs each (invariants 3 and 5).
- [ ] Serves invariants 2, 3, 5 and 6.

### FR-005: Where the budget does not bind nothing changes, and in the last margin the batch is trimmed

#### Description:

The bound is relative to the time left, so it binds only when the budget is what limits the next
batch; the rate rule, and the target kept through a failed exit batch, govern otherwise. A `None`
deadline and every steady batch never read it. These criteria hold the healthy exit drain, the
failed path's kept target and the first-attempt rule to today's behaviour.

There is one band where the outcome changes, by Andrew's decision of 2026-11-17 (Overview): when
the rate rule's next batch would end inside the last `_JOIN_MARGIN` before the deadline. Today it is
started and, by luck, delivered whole; with the bound it is trimmed to end by the deadline less the
margin, and SPEC-062's prediction declines the tail, which is counted and announced. The 0.1 s
margin is the rule everywhere at exit.

#### Acceptance Criteria:

- [ ] On FR-001's clock (the pre-stop call charged nothing) with the deadline at 64.0 and 2,000
      spans queued, the sizes handed to the sink equal those of the same run under
      `retire_process(worker, None)`, and every exit batch after the first and before the last is
      128 events (invariant 3).
- [ ] On FR-001's clock (the pre-stop call charged nothing) with the deadline at 0.6 and 74 spans
      queued, the exit batches handed are 20 and 122, and the declined line names 6 events. Without
      the bound they are 20 and 128, ending at 0.578125, inside the last margin, and nothing is
      declined (invariants 2 and 3).
- [ ] `spec103_recovered_slow_exit.py 2 2000 30` hands the sink 3,982 events on 3.12 and 3.13, and
      `… 2 2000 2.0` completes its join, as today (invariants 2 and 3).
- [ ] A pre-stop batch delivered while the stop is claimed, with a deadline set on the worker's
      clock during it tight enough that the bound would lower the target, sets the target by the
      rate rule alone and leaves `_exit_rate` at 0.0 (invariant 6).
- [ ] Every existing test in `tests/test_exit_drain_batches.py`, among them
      `test_a_deadline_already_past_still_makes_the_first_exit_batchs_first_attempt` and every
      SPEC-090 FR-003 and SPEC-103 test there, and every existing test in
      `tests/test_worker_exit_budget.py` and `tests/test_backlog_batching.py`, passes unchanged
      (invariants 3 and 6).
- [ ] The diff adds one `Worker` attribute, `_exit_rate`, with its one row in
      `tests/test_lifecycle_inventory.py`, and no `_diag` call, no constructor parameter, and no
      `SinkCounters` or `Health` field (invariants 10, 11 and 12).
- [ ] Serves invariants 2, 3, 6, 10, 11 and 12.

### FR-006: The record states the bound and its trades, and the item's first shape is closed

#### Description:

The completion ritual (`docs/process/completion-ritual.md`, step 5) refines in place, with a dated
History entry, the decision whose rule this changes. That is *The exit drain declines a retry, or
a later batch, predicted to end past the shutdown deadline*
(`docs/decisions/sink-lifecycle/the-exit-drain-declines-only-a-retry-predicted-to-end-past-the-shutdown-deadline.md`),
since the bound exists to keep its prediction honest. It is a refinement, not a reversal: a batch
predicted to end after the deadline is still not work the process gets, and the bound makes the
batch it lets start one that fits. It carries two trades. At a tight budget the sink is handed
fewer events than the rate rule would, and the join completes; that is the decision's own trade.
In the last margin before the deadline, a batch today's code delivered by luck is trimmed and its
tail declined, which Andrew chose on 2026-11-17. It also records that a sink change between exit
batches costs nothing, since the old sink's rate can lower only a target `_size_target` already
reads as zero (Out of Scope), so the next reader does not file it.

The decision's **Rejected** list, which names none of these today, gains four alternatives, each
on its own terms. **A growth-tied
bound**, which holds the next batch to `events × (_exit_longest + _JOIN_MARGIN) / _exit_longest`,
lets each delivered batch's attempt run at most one margin, 0.1 s, longer than the longest before
it, whatever the budget: at 2 ms an event a healthy exit drain would go 20, 70, 120, 170 and 220
events before reaching the rate rule's 248, and at an interval of 4 s or more about twenty batches
before the 1,000-event cap, each a sink call, where this bound leaves an ample budget exactly as
it is. It ties growth to the margin, a quantity unrelated to how much time is left. **No growth at
exit**, no exit batch above the target in force at the stop, which the §12 item measured: it slows
a healthy exit that starts from a zero target, most for a sink that sends one request a batch, and
from a measured target it leaves the 1,000 in force at the stop, so it does not close the
slowdown there. **A floor of one event** and **`max(0, …)`**, each with its reason (*Revision
history*, the spec review of 2026-11-17).

The §12 item is not struck whole, because two of its shapes stay open (Out of Scope). For the same
reason, each sentence the item lists as shown false gains a qualification in this change, since on
`main` none carries one: it states what can still overrun the budget (a sink that slows during the
exit, a retry after a call that failed at once, and a first exit batch longer than the budget) and
points at the §12 item.

#### Acceptance Criteria:

- [ ] The sink-lifecycle decision's fence and body state the bound, after a delivered and after a
      failed exit batch, its guard, and why it holds the prediction to the budget; its trade
      paragraph gains the two trades above with this spec's numbers and Andrew's decision, the
      restore's one-batch trade (FR-003), and the `retarget` note; its **Rejected** list gains the growth-tied bound with the arithmetic above,
      no growth at exit with the §12 item's measured reason, and the floor of one event and
      `max(0, …)` with their reasons; and its History gains a dated SPEC-105 entry at the top
      (serves no invariant: the register's record, judged by the completion ritual).
- [ ] The pipeline decision *A backlog drains in larger batches, and capacity is not a setting*
      names the exit bound in its size-target bullet, pointing at the sink-lifecycle decision, and
      gains a dated History entry; its fence is unchanged, since the bound only lowers a target
      (serves no invariant: the register's record).
- [ ] The docstrings of `_emit_pending`, `_set_target`, `_retry_declined`, `Worker.__init__`'s exit
      fields and `_reinit_after_fork`, the new `_fit_exit_target`, the module text on
      `_JOIN_MARGIN`, and the `Worker` row of `docs/component-inventory.md` state the bound and its
      guard (serves no invariant: prose hygiene).
- [ ] Each sentence the §12 item lists as shown false gains a qualification naming what can still
      overrun (a sink that slows during the exit, a retry after a call that failed at once, a first
      exit batch longer than the budget) and pointing at the §12 item: `Worker.__init__`'s
      docstring's "The exit drain fits the caller's budget"; the `_JOIN_MARGIN` docstring's "so an
      attempt permitted by the rule leaves the join time to return"; `_set_target`'s "so an emit
      stays shorter than the time trigger's interval"; `_emit`'s "On the exit drain a retry fits the
      caller's budget or is not made"; `src/log_foundry/sinks/nats.py`'s module headline "The exit
      drain fits the join", though not the arithmetic under it; `docs/invariants.md` section 3's
      *Guarded* clause "the exit drain's retries fit the `shutdown()` budget"; the pipeline
      decision's "so an emit stays shorter than the time trigger's interval"; and the
      sink-lifecycle decision's trade paragraph, which names only a prediction that runs long
      (serves no invariant: prose hygiene).
- [ ] In `docs/architecture.md` §12, the item "The exit drain sizes a batch from a rate it measured
      earlier…" stays open: its first shape, *A sink that recovers by exit*, is marked closed by
      SPEC-105 in place with this spec's before and after rows; its **Closed by** sentence names
      SPEC-105 as the first shape's landed closer, without the paused-Draft and old-title text, and
      the second and third shapes and a sink slow from its first exit call as having no closer;
      and a *Resolved* entry records the first shape's closure (serves no invariant: the record of
      a closed item).
- [ ] SPEC-103's Out of Scope bullet ending "SPEC-105 is its spec." gains a one-line marker that
      SPEC-105 closes the first shape only (serves no invariant: the record of a closed item).
- [ ] The README's *When the worker flushes* and the pending `docs/release-notes/v1.2.1.md` bullet
      on an exit drain predicted to overrun narrow the caveat "a batch the exit has grown since":
      once a batch of the exit is delivered, each later batch is held to what fits in the time
      left less the margin, so a healthy sink's grown batch no longer overruns, while a sink that
      slows during the exit, a retry after a call that failed at once, and a first batch that
      alone takes longer than the budget still can (serves no invariant: the release artifact no
      gate holds to the code).
- [ ] `sh scripts/docs-lint.sh`, `sh scripts/spec-lint.sh` and
      `poetry run python scripts/docstring-lint.py` exit 0 (serves no invariant: the gates).

---

## Data Model

```python
# src/log_foundry/worker.py, private
class Worker:
    ...  # constructor unchanged
    # SPEC-105 FR-001; drain thread only; 0.0 in __init__ and in _reinit_after_fork, before its
    # resume split, so both branches reset it
    _exit_rate: float  # events/s of the most recent delivered exit batch; 0.0 when none recorded
```

## API / Interface Contract

No public surface changes. The private shape, in `Worker._emit_pending` on `main`
(`src/log_foundry/worker.py`), whose failed-exit branch today is `if exit_batch and failed: return`:

```python
if exit_batch and self._exit_batches and self._exit_deadline is not None and (...):
    self._decline_exit_batch(size)
    if self._exit_rate > 0:                                       # FR-003's restore
        self._target = min(float(_BACKLOG_BATCH_CAP), self._exit_rate * self.flush_interval / 2)
    ...
    return
...
failed = self.failed_batches != before
if exit_batch and failed:
    if generation == self._sink_generation:                       # FR-002
        self._fit_exit_target()
    return                                                        # the target is kept, as on main
self._set_target(size, elapsed, failed=failed, generation=generation)
if exit_batch:                                                    # FR-001
    measured = elapsed > 0 and generation == self._sink_generation
    self._exit_rate = size / elapsed if measured else 0.0
    self._fit_exit_target()

def _fit_exit_target(self) -> None:
    deadline = self._exit_deadline
    if deadline is None or self._exit_rate <= 0:
        return
    room = deadline - time.monotonic() - _JOIN_MARGIN
    if room >= self._exit_longest:                                # FR-003's guard
        self._target = min(self._target, self._exit_rate * room)
```

## File & Folder Structure

```
src/log_foundry/worker.py               # FR-001 to FR-003; docstrings for FR-006
tests/test_exit_drain_batches.py        # FR-001 to FR-005
tests/test_backlog_batching.py          # the fork reset of _exit_rate
tests/test_lifecycle_inventory.py       # the _exit_rate row
docs/decisions/sink-lifecycle/the-exit-drain-declines-only-a-retry-predicted-to-end-past-the-shutdown-deadline.md
docs/decisions/pipeline/a-backlog-drains-in-larger-batches-and-capacity-is-not-a-setting.md
docs/architecture.md, docs/component-inventory.md                             # FR-006
src/log_foundry/sinks/nats.py, docs/invariants.md                             # FR-006's qualifications
docs/specs/SPEC-103-repeated-sink-lines-at-exit-are-folded.md                 # FR-006's marker
README.md, docs/release-notes/v1.2.1.md                                       # FR-006
```

## Implementation Phases

### Phase 1: The rate, the bound and its guard (FR-001 to FR-005)

- Add `_exit_rate`, its resets and its inventory row; record it after a delivered exit batch; add
  `_fit_exit_target` with its guard and call it after every emitted exit batch that did not
  straddle a sink change.
- Restore the rate rule's target when an exit batch is declined and a rate is recorded.
- Write the clock, flapping, retarget, guard, restore, boundary, fork-reset, tight-budget,
  engine-twin, margin-band and unchanged-path tests; mutation-test the guard (removed, against the
  ledger test and the 0.09765625 s criterion; weakened to `room > 0`, against the 0.6 s, 199-span
  criterion; and `>=` turned to `>`), the restore (removed, and without its rate check), the
  delivered-path bound and the failed-path bound; run `spec103_recovered_slow_exit.py` at the
  FR-004 and FR-005 timeouts on 3.12 and 3.13 for the record.

### Phase 2: The record (FR-006)

- Refine the two decisions, rewrite the docstrings and the inventory row, mark the §12 item's first
  shape closed and add its *Resolved* entry, mark SPEC-103's bullet, and narrow the README's and
  the pending release notes' caveat.
- Add the qualification at each site the §12 item lists as shown false: the four `worker.py`
  docstrings, `src/log_foundry/sinks/nats.py`'s module headline, section 3's *Guarded* clause in
  `docs/invariants.md`, and the two decisions.

## Evidence

### Re-measured on `main`, 2026-11-24 (current)

The design as this spec states it (the recorded rate, the bound after a delivered exit batch and
after a failed one that did not straddle a generation change, FR-003's guard, no floor) was
prototyped on the `src/` of `main` at `d9f2f097`, with toggles for the guard's variants and for
the failed-path bound, and driven on FR-001's clock.

| Case on FR-001's clock | With the bound | `main` (no bound) |
|---|---|---|
| deadline 0.328125, 500 spans | exit batches 20, 54; they return at 0.078125 and 0.2890625; target 128.0 after; 926 declined in 8 | 20, 128 (ends 0.578125); 852 in 7 |
| same, engine twin | 20, 54 | — |
| same, first exit batch costs nothing | 20, 980; `_exit_rate` 0.0 when the second reaches the sink | — |
| deadline 0.578125, 500 spans, exit batches 2 to 4 fail at 0.0625 s | 20, 118, 102, 86, 70; the fifth returns at 0.5390625; 604 in 5 declined | 20, 128, 128, 128, 128, ending 0.765625 |
| same, failed-path bound removed | 20, 118, 118, 118, 118; the fifth ends 0.7265625 | |
| deadline 0.09765625, 500 spans | exit batch 20 only; target 128.0; 980 declined in 8 | 980 in 8 |
| same, guard removed | 980 in 49; target −5.0 | |
| deadline 0.6, 200 spans | 20, 122 (ends 0.5546875); 258 declined in 3 | 20, 128; 252 in 2 |
| same, guard `room > 0` (and removed) | 258 in 13; target about 1.6 | |
| deadline 0.6, 74 spans | 20, 122; 6 declined in 1 | 20, 128, ending 0.578125, inside the margin; none declined |
| deadline 64.0, 2,000 spans | sizes equal `main`'s and the `None` deadline's; 33 exit batches, every interior one 128 | |

Those figures predate the restore FR-003 gained in diff review frame 2, which changes the
guard-removed rows: with it the 0.09765625 s case names 980 events in 9 batches, the ledger test 2
declined batches, and the 0.6 s, 200-span case 258 in 3 under the weakened guard (FR-003).
`test_an_expired_shutdown_leaves_the_batch_in_flight` with the guard removed fails with
`lost 960 event(s); 48 exit batch(es) declined unattempted`; with the guard weakened to `room > 0`
it passes. With the full prototype, `tests/test_event_ledger_defects.py`,
`tests/test_exit_drain_batches.py`, `tests/test_worker_exit_budget.py` and
`tests/test_backlog_batching.py` pass. So every fake-clock figure in FR-001, FR-002, FR-003 and
FR-005 re-derives exactly on `main`, and the margin band and Andrew's 2026-11-17 decision read as
they did: today's 20 and 128 end at 0.578125, inside the last margin before 0.6, with nothing
declined, and the bound gives 20 and 122 with 6 declined.

The real clock, `spec103_recovered_slow_exit.py 2 2000 <timeout>` on 3.13.1, two runs a cell
(figures are observed ranges; the 3.12 runs FR-004 and FR-005 name are the build's):

| `shutdown` timeout | `main` at `d9f2f097` | The prototype |
|---|---|---|
| 0.45 s | join expired; second exit batch 222 to 236 events | completes in 0.34 s; second batch 44 to 46 |
| 0.5 s | join expired; 224 to 248 | completes in 0.40 s; 74 |
| 0.6 s | join expired; 224 to 232 | completes in 0.48 s; 112 to 114 |
| 0.65 s | join expired; 228 to 240 | completes in 0.52 s; 136 to 138 |
| 2.0 s | 1.69 to 1.72 s; 738 to 750 events handed | 1.69 s; 736 |
| 30 s | 8.23 s; 3,982 handed | 8.22 to 8.23 s; 3,982 |

A healthy exit drain with an ample budget is not slowed: the 2 s and 30 s rows match.

### History: the 2026-11-17 evidence, on SPEC-103's growth branch

These figures were measured on SPEC-103's growth branch, whose design did not land; they are kept
as the record of how the design was chosen.

**The defect and the first prototype.** `spec103_recovered_slow_exit.py 2 2000 <timeout>`: the
pre-stop batch fails, so the target is zero at the stop; at exit the sink is healthy at 2 ms an
event; 2,000 two-event spans; a 1.0 s interval. The first prototype bounded the target after a
delivered exit batch with that batch's own rate, with no guard and a floor of one event, applied to
the `src/` of SPEC-103's branch at `68f8695a`. Measured on 3.13.1 in at least two runs a cell, and
on 3.12.12 in two runs each at 0.45, 0.6, 2.0 and 30 s; figures are observed ranges.

| `shutdown` timeout | SPEC-103's tree | The first prototype |
|---|---|---|
| 0.45 s | join expired; second exit batch 238 to 246 events | completes in 0.35 s; second batch 50 to 56 |
| 0.5 s | join expired; 248 | completes in 0.39 s; 68 |
| 0.6 s | join expired; 234 to 248 | completes in 0.50 s; 124 to 128 |
| 0.65 s | join expired; 248 | completes in 0.55 s; 154 |
| 2.0 s | 1.72 to 1.73 s; 750 to 764 events handed | 1.69 to 1.72 s; 738 to 758 |
| 30 s | 8.18 to 8.20 s; 3,982 handed | 8.18 to 8.19 s; 3,982 |

`main` at `61b942c0` also expires its join at 0.45 to 0.65 s: the exposure predates SPEC-103, which
does not touch a delivered batch.

**The design as specified.** The spec reviewer built the recorded rate, the failed-path bound and
FR-003's guard, with the floor, on SPEC-103's tree. That build passes
`test_an_expired_shutdown_leaves_the_batch_in_flight`, all of `tests/test_exit_drain_batches.py`,
FR-001's 20 and 54 on both twins, FR-002's flapping sequence and FR-004's join on both twins. The
same build with the floor removed, the design this spec states, passes the ledger test and
`tests/test_exit_drain_batches.py` and gives every fake-clock figure in FR-001, FR-002, FR-003 and
FR-005. Removing the guard there fails the ledger test with 48 declined batches, and gives 49 at the
0.09765625 deadline and 13 at the 0.6 deadline with 200 spans; weakening it to `room > 0` gives 13
at the latter. The flapping case was derived from the rule and confirmed on that build, not on the
first prototype.

**The margin band**, `spec103_recovered_slow_exit.py 2 74 <timeout>`, from the spec review: at
0.55 s on 3.13.1, two runs, today's tree hands the sink 130 events and completes at 0.47 s, and the
design hands 100 to 110 and declines 20 to 30; at 0.5 s on 3.12.12, today hands 130 and the design
76.

## Revision history

- **2026-11-17, drafted.** From the defect SPEC-103's spec review found ~~and SPEC-103 files in
  §12~~ — filed in §12 by another session's docs change, not by SPEC-103 — and from the prototype
  measured above.
  - **Rejected, not built: SPEC-103's third bound applied to the delivered path**, which holds the
    next batch to `events × (_exit_longest + _JOIN_MARGIN) / _exit_longest`. The arithmetic is
    enough: it lets each delivered batch's attempt run at most one margin, 0.1 s, longer than the
    longest before it, whatever the budget. At 2 ms an event a healthy exit drain would go 20, 70,
    120, 170 and 220 events before reaching the rate rule's 248, and at an interval of 4 s or more
    about twenty batches before the 1,000-event cap, each a sink call, where this spec's bound
    leaves an ample budget exactly as it is. It ties growth to the margin, a quantity unrelated to
    how much time is left, so it would change every healthy exit drain to fix one that runs out of
    time. (SPEC-103's bounds did not land; FR-006 now records this as the growth-tied bound on its
    own terms.)
- **Author's revision, 2026-11-17.** The orchestrating session answered the three questions the
  draft returned.
  - A target kept through a failed exit batch is closed here rather than left out: the worker
    records the most recent delivered exit batch's rate in `_exit_rate`, and the bound is applied
    after every exit batch once a rate is recorded, ~~after SPEC-103's sizing on the failed path~~
    — on `main`, on the target kept through the failure, since SPEC-103's sizing did not land
    (FR-002, with a flapping-sink criterion). FR-005 names the one new attribute and its inventory
    row, and the case leaves Out of Scope.
  - ~~The floor after a takeover that replaces the deadline with `None` is an accepted trade~~ —
    the floor was dropped in the spec review below.
  - No arc, matching SPEC-104.
- **Spec review, 2026-11-17.**
  - Blocker: the floor drove the target to one event once the deadline had passed, so the declined
    backlog went out in `batch_size`-submission batches and
    `test_an_expired_shutdown_leaves_the_batch_in_flight` read 48 declined batches where it pins 1.
    The bound now applies only when `room >= _exit_longest` (FR-003, the silent guard,
    mutation-tested against that test).
  - The floor and its FR are dropped: while a deadline stood its effect could never be observed,
    since the next batch was declined either way, and after a takeover to a `None` deadline it cost
    a trade. `max(0, …)` was the unrecorded alternative; the guard makes both unnecessary, since
    `room` is then at least `_exit_longest`. The takeover trade leaves Out of Scope.
  - FRs renumbered: the guard is FR-003, the tight budget FR-004, the unchanged paths FR-005 and
    the record FR-006.
  - The margin band is stated in the Overview, FR-005 and FR-006, with Andrew's decision of
    2026-11-17 to cap at the margin, a fake-clock criterion (20, 122, 6 declined) and the review's
    real-clock runs in Evidence.
  - The fake-clock premise is stated once as FR-001's clock and named in each criterion that uses
    it: the pre-stop call charges nothing, chosen over re-deriving the figures with SPEC-103's
    cost, which charges every call and gives 20 and 34, and 20, 98, 82, 66 and 50.
  - A failed exit batch that straddled a sink change keeps its target, with a criterion; a
    `retarget` between exit batches is an accepted trade in Out of Scope.
  - "Every exit batch after the first is 128" now says "after the first and before the last".
  - "Fits the budget by its size" is qualified by the `batch_size` minimum, which is in Out of
    Scope.
  - ~~The §12 title quoted is SPEC-103's actual one.~~ — the item was since retitled "The exit
    drain sizes a batch from a rate it measured earlier…", which this spec now quotes.
  - The real-clock criteria name `stopped_reason` and the `close()` count as the assertions that
    tell the design from the delivered-path mutant.
- **Held, 2026-11-17: ~~do not plan or build from this text~~ — lifted 2026-11-24, below.** Andrew paused SPEC-105 until SPEC-103's rewrite settles. SPEC-103 no longer grows exit batches (it folds repeated exit lines instead), so every passage here that cites SPEC-103's exit probe, `_exit_probing`, its bounds or its sequence tests, or builds on its code, describes a design that will not land. On resume, this Draft is revised against the folded SPEC-103 and goes through a spec review before any plan. The §12 item it is pointed at, "The exit drain sizes a batch from a rate it measured earlier…", names it as the closer of that item's first shape only (a sink that recovers by exit); the slowdown and the fast-failure retry shapes stay open, so on resume either this spec's Out of Scope is revisited or its record strikes only the first shape, never the whole item.
- **Held again, 2026-11-18.** SPEC-103 has landed in its folded form (#409), which ends the first hold's
  reason. Under the scope freeze Andrew adopted that day, this spec now waits until SPEC-097 lands,
  because both change the shutdown path in `worker.py`. On resume it is revised against `main` as it
  then stands and goes through a spec review before any plan (*An arc closes at the specs it names,
  and only its orchestrator adds work*, `docs/decisions/working-rules/`).
- **Resumed and revised against `main`, 2026-11-24.** Andrew lifted the hold ("Start both", for
  SPEC-099 and SPEC-105). The design is unchanged. The spec was checked against `main` at
  `d9f2f097`, after SPEC-103's folded form (#409), SPEC-097 (#413) and SPEC-098 (#415), by reading
  `_emit_pending`, `_set_target`, `_size_target`, `_retry_declined`, the exit fields' resets and the
  §12 item, and by running a prototype of this design on that `src/`: every fake-clock figure in
  FR-001, FR-002, FR-003 and FR-005 re-derived exactly, and the real clock was re-measured on
  3.13.1 (*Evidence*, *Re-measured on `main`*). The changes:
  - The Held banner and the Overview's *Sequencing* paragraph are gone; the Overview names the §12
    item whose first shape this spec closes.
  - Every passage built on SPEC-103's growth code (its probe, `_exit_probing`, its three bounds,
    its sequence tests) now builds on `main`, where a failed exit batch keeps its target (SPEC-090
    FR-001 rule 4, refined 2026-11-16): In Scope's FR-002 bullet, Out of Scope's first bullet,
    FR-002's Description and its dead-sink criterion, FR-005's Description and its unchanged-tests
    criterion, the Data Model, the API contract, File structure and Phase 1, which no longer
    rebases or adds SPEC-103 to `depends_on`.
  - FR-001's clock drops its comparison with a SPEC-103 test that does not exist on `main`, and
    states `batch_size` 10 and two-event spans; the `_reinit_after_fork` reset sits before the
    `resume` split, as `_exit_longest`'s does.
  - FR-003 records that the guard weakened to `room > 0` passes the ledger test, so the 0.6 s,
    200-span criterion is what kills it; removing the guard still fails the ledger test with 48.
    The zero target it never sets is now described by what `main` does with one.
  - The `retarget` trade is no longer a trade: read from `main`, only `_set_target` stamps
    `_target_generation`, so after a `retarget` the old sink's rate can lower only a target
    `_size_target` already reads as zero, and the new sink's first delivered exit batch records
    its own rate. Out of Scope says so, FR-002 gains a criterion holding it, and FR-006 carries two
    trades and a note where it carried three.
  - Out of Scope names the §12 item's other shapes (a sink that slows during the exit, a retry
    predicted from a fast failure) and a sink slow from its very first exit call as not closed
    here, with the item staying open for them.
  - FR-006 no longer strikes the §12 item whole: it marks the first shape closed in place, updates
    its **Closed by**, ~~keeps the sentences it lists as shown false qualified~~ (on `main` none
    carries a qualification; the spec review below has FR-006 add one at each), and adds a
    *Resolved* entry. SPEC-103's marker goes on its Out of Scope bullet ending "SPEC-105 is its spec." alone,
    since SPEC-103 has no FR-003 paragraph filing the item. The README and release-notes caveat
    "a batch the exit has grown since" is narrowed rather than dropped. The decision's **Rejected**
    list describes the growth-tied bound on its own terms and gains no growth at exit.
  - *Evidence* leads with the 2026-11-24 re-measurement on `main`; the 2026-11-17 figures on
    SPEC-103's branch are kept as history.
  - **Spec review, 2026-11-24.** Buildable as written; every figure confirmed on a build. The
    findings and what each changed:
    - Major: FR-004's shape, with the pre-stop call returning the instant the stop is claimed, did
      not reproduce the defect: with the delivered-path bound removed the join still completed in
      2 of 10 runs at 0.45 s and 6 of 6 at 0.5 to 0.65 s, so the recorded kill was a coin-flip.
      FR-004's Description now has the pre-stop call, released by the stop, idle 0.2 s and then
      fail, leaving the exit drain the timeout less about 0.2 s (that mutant then expired 10 of 10
      at 0.45 s on both twins, and the build passed all four timeouts); its first criterion names
      that shape; the probe is said to give this shape, as the §12 item describes it; and the
      Overview states the 0.2 s premise beside its 0.45 to 0.65 s budgets.
    - Should-fix: on `main` none of the code-side sentences the §12 item lists as shown false
      carries a qualification, so "keeps its qualification" had nothing to keep. The orchestrator
      decided SPEC-105 adds one at each site in the same change; FR-006 gains a criterion listing
      every site, each quoted sentence checked against `main`, and File structure and Phase 2 add
      `src/log_foundry/sinks/nats.py` and `docs/invariants.md`.
    - Nit: the sink-lifecycle decision's **Rejected** list names neither the floor nor
      `max(0, …)`, so FR-006's Description now says the list gains them, as its criterion does.
    - Nit: FR-002's `retarget`-between criterion passed vacuously when both sinks cost the same;
      the new sink now costs 0.0078125 s an event and the criterion asserts `_exit_rate` reads
      128.0, the new sink's rate, not 256.0.
    - Nit: FR-002's straddle criterion now says it pins the state rather than guarding an outcome,
      so a mutant without the generation check is visible.
- **Build, 2026-11-24.** Two spec defects found by the build, corrected in place:
  - FR-006 named "the `Worker` class docstring" as the home of "The exit drain fits the caller's
    budget"; the sentence is in `Worker.__init__`'s docstring, and the criterion now says so (the
    §12 item's own reference is corrected with it).
  - FR-002's two `retarget` criteria could not be built as written: no `retarget` can reassign
    during a `shutdown()`'s exit drain, since `_shutdown_worker` bumps `retirements` and claims
    the stop in one `_state._lock` section and `retarget` checks `retirements > self._epoch` under
    `Worker._lock` before its reassign. Both criteria now pin the state through a stand-in for
    `retarget`'s locked reassign (`_reassign` in the tests), and Out of Scope's *Keying the rate by
    sink generation* says the sink-change handling on the exit path is therefore defensive.
- **Diff review frame 2, 2026-11-24.** Four findings:
  - Fixed: the guard is asked when an exit batch returns and the prediction again before the next
    batch, after its items were taken, so a room in `[_exit_longest, _exit_longest + the take's
    time)` lowered the target, the batch was declined anyway, and the whole declined backlog was
    cut at the lowered target: 980 events in 49 batches against `main`'s 8, on both twins. A
    declined exit batch now restores the rate rule's target once a rate is recorded (FR-003), and
    the repro names 9. FR-003's Description and criteria are rewritten for it: removing the guard
    now fails the ledger test with 2 declined batches, not 48, and gives 9 at the 0.09765625 s
    deadline; the weakened guard is no longer killed by the 0.6 s, 200-span criterion but by a
    199-span one; the guard's failure moves the declined count by at most one batch, not dozens; and
    criteria are added for the restore on both twins and for a decline with no rate recorded.
    FR-001's first criterion gains its declined line, 926 events in 8 batches.
  - Fixed: the guard's boundary, `room == _exit_longest`, had no test; FR-003 gains one, and the
    `>` mutant hands 20 and 128.
  - Fixed: `_reinit_after_fork`'s docstring said a child never bounds a batch by a parent's exit
    rate; a fork from inside an exit batch's `emit` on the drain thread can, after the reset, and
    the docstring now says so and why it cannot make the child overrun a budget.
  - Flagged, not built: an exit batch whose emit the clock cannot see records no rate, and the
    next batch goes out at the cap. Recorded in Out of Scope and in the §12 item as a remainder of
    the first shape, with a candidate closure and no closer named.
- **Fix review, 2026-11-24.** A review aimed at the restore found it sound and closed its prose:
  - FR-003 gains a criterion for the restore's cap (2**-12 s an event, deadline 0.045, 5,000
    spans: 9,980 events in 10 declined batches, target 1000.0; 6 without the cap).
  - "Moves by one" became "by at most one", since the 0.6 s, 200-span case gives 258 in 3 either
    way.
  - "A window narrower than one take" was unmeasured; the window is now stated as the time from
    the fit to the next prediction, which can include a take, a marker's answer or a GIL wait,
    with no size claimed.
  - "Delivers nothing differently" became "delivers no event differently", with the cut a takeover
    to a `None` deadline can see after a decline.
  - The targets the restore can set that the bound did not lower now match `_emit_pending`'s
    docstring at `c749b2bc`: an empty exit batch, a changed `flush_interval` and a forked child's
    stale rate.
