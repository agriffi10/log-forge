---
type: Spec
id: SPEC-105
title: "Once an Exit Batch Is Delivered, Each Later One Fits the Remaining Shutdown Budget"
description: "Records the rate of the exit drain's most recent delivered batch and, after every exit batch from then on, delivered or failed, holds the next batch to what that rate fits into the time left before the shutdown() deadline less the join margin, so a sink healthy or flapping at exit is never handed a batch the exit budget's prediction did not allow for, and a tight shutdown() completes its join and closes the sink, with no setting, line, tally or Health field added."
spec_status: Draft
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
has seen. With 2 ms an event, 20 events in 0.04 s lead to a batch of about 248 events, about 0.5 s
of work, and the prediction, which allows for 0.04 s plus the margin, lets it start. With a
`shutdown()` budget of 0.45 to 0.65 s that batch ends past the deadline, the join expires, and the
sink is left open. That is the outcome the exit budget exists to prevent (invariants 3 and 5). A
sink that flaps at exit reaches the same outcome by another route: a target set after a delivered
batch is kept through the failed batches after it, while the time they spend shrinks the budget.

This spec closes both. The worker records the rate of the most recent delivered exit batch, and
after every exit batch from then on, delivered or failed, the next batch is held to what that rate
fits into the time left before the deadline, less the join margin. With an ample budget this never
binds, and the batches are what they are today; with a tight one the sink is handed fewer events,
the join completes and the sink is closed. Nothing a caller configures changes, and no line, tally
or `Health` field is added. The design was prototyped and measured (*Evidence*); the alternative
considered is under *Revision history*.

**Sequencing.** SPEC-103, being built on branch `spec-103-exit-zero-target`, changes the same
lines of `Worker._emit_pending`: it sizes the next exit batch after a *failed* one, and it files
this defect in `docs/architecture.md` §12 as an open item ("A recovered sink at exit whose next
batch outruns the exit budget's prediction", SPEC-103 FR-003). This spec builds on SPEC-103's code
and closes that item. SPEC-103 is absent from `depends_on` only because `spec-lint.sh` refuses a
dependency on a spec not yet in `docs/specs/`; Phase 1's first step adds it.

## Scope

### In Scope

- One new drain-thread field, `_exit_rate`, the rate of the most recent delivered exit batch,
  reset with the other exit fields and listed in the lifecycle inventory (FR-001).
- After a delivered exit batch, the next size target is at most what that rate fits into the time
  left before the deadline less `_JOIN_MARGIN` (FR-001).
- After a failed exit batch, once a rate has been recorded, the same bound, applied after
  SPEC-103's own sizing and only ever lowering it (FR-002).
- That bound never sets the target below one event (FR-003).
- The end-to-end outcome at a tight budget: the join completes, `close()` runs once, and every
  event is delivered or counted in an existing line, on the 1.x and the engine path (FR-004).
- Everywhere the budget does not bind, the batches are what they are today (FR-005).
- The record: the two decisions, the docstrings, the inventory, the §12 item, SPEC-103's markers,
  the README and the pending release notes (FR-006).

**The twin paths** (invariant 6). The bound sits in `Worker._emit_pending`, which every batch of
both drains goes through, above both branches of `_emit`: the 1.x `emit` loop and the delivery
engine's run. A carry from `_take`, the batch ahead of a flush marker (`_answer_marker`) and the
exit drain's last batch all reach it. It acts only on an exit batch, so `_drain` and `_final_drain`
keep sharing every rule but this one, which, like the exit budget it serves, exists only at exit.

### Out of Scope

- **SPEC-103's sizing after a failed exit batch**, its probe and its three bounds, which this spec
  leaves as they are and only lowers afterwards (FR-002). Before any exit batch has been delivered
  no rate is recorded, and SPEC-103's longest-attempt bound is what holds a probed batch.
- **A declined exit batch.** It is not emitted, records nothing and is followed only by declined
  batches while the deadline stands, so no bound is applied after it.
- **The first exit batch.** Its size is whatever the target read at the stop, and its first attempt
  is always made (SPEC-090 FR-003). A target measured before the stop is not capped, so a first
  exit batch can still be longer than the budget; that is the first-attempt rule's own trade.
- **A sink whose per-event cost rises within the exit drain.** The bound assumes the next batch
  runs at the recorded rate, as SPEC-062's prediction assumes the next attempt is no longer than
  the longest seen.
- **A floored target after a takeover replaces the deadline with `None`** (SPEC-068), an accepted
  trade. The target of one event then stands, so a dead sink is handed `batch_size`-submission
  batches for the rest of that exit rather than SPEC-103's growing ones. It is accepted because a
  takeover after the floor has bound is rare, and the floor exists to keep a zero target meaning
  *unmeasured* while a deadline stands (FR-003). A takeover that records a later deadline takes
  effect after the next exit batch.
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
`_set_target`, which sets the target to `min(cap, events / elapsed × flush_interval / 2)`. Before a
later exit batch, `_retry_declined` declines it when `now + _exit_longest + _JOIN_MARGIN` passes
the deadline. The prediction assumes the next attempt is no longer than the longest seen, and the
rate rule makes no such promise: after a short delivered exit batch, the next can be many times
longer.

**The rate.** A delivered exit batch whose emit did not straddle a sink-generation change and whose
`elapsed` is above zero sets `Worker._exit_rate` to `size / elapsed`, in events a second. Any other
delivered exit batch sets it to `0.0`, meaning none recorded: an `elapsed` of zero or below is a
clock that cannot see the emit, whose rate is unbounded, and `_set_target` already gives that batch
the cap; a straddling batch measured a sink no longer installed. A failed exit batch leaves the rate
as it was. `_exit_rate` is a drain-thread `float`, set to `0.0` in `Worker.__init__` beside
`_exit_longest` and in `_reinit_after_fork` on both branches, and listed in
`tests/test_lifecycle_inventory.py`'s Worker table with `_REBUILD`, as `_exit_longest` is.

**The bound.** After `_set_target` has run for a delivered exit batch, while the deadline is not
`None` and a rate is recorded, the target is lowered to at most

    max(1, _exit_rate × (deadline − now − _JOIN_MARGIN))

where `now` is `time.monotonic()` read then, and `_JOIN_MARGIN` and the deadline are read at that
use. One private method, `_fit_exit_target`, applies it on this path and on FR-002's. At the
recorded rate, the next batch's attempt then ends by the deadline less the margin. SPEC-062's
prediction still runs before that batch and can still decline it; the bound only ever lowers the
target. So a batch the prediction lets start fits the budget by its size, rather than by the
prediction's assumption about its length.

`elapsed` is the whole `_emit`, retries and the engine's waits included, so a batch delivered
after a retry records a lower rate and gets a smaller bound.

#### Acceptance Criteria:

- [ ] On a clock only the sink advances, starting at 0.0, with `_JOIN_MARGIN` patched to 0.0390625
      s, a 1.0 s interval, `max_retries=0`, the pre-stop batch failed so the target is zero at the
      stop, a sink costing 0.00390625 s an event, and the exit deadline set to 0.328125 on that
      clock while the first exit batch is in the sink: the first two exit batches are 20 and 54
      events. Without the bound they are 20 and 128; every value is exact in binary (invariant 3).
- [ ] The engine twin, a `deliver` sink with the same per-event cost, `max_retries=0` at both
      levels and the same deadline, is handed the same 20 and 54 (invariant 6).
- [ ] A first exit batch whose `elapsed` reads 0.0 on that clock, under the same deadline, leaves
      `_exit_rate` at 0.0 and the target at the cap (invariant 3).
- [ ] `_reinit_after_fork` sets `_exit_rate` to 0.0 with `resume=True` and with `resume=False`, and
      `tests/test_lifecycle_inventory.py` lists it with its child column (invariant 12).
- [ ] Serves invariants 3, 6 and 12.

### FR-002: After a failed exit batch, the recorded rate holds the next to the time left as well

#### Description:

SPEC-090 keeps a target a delivered batch measured through a fast exit failure, and SPEC-103 keeps
that rule. So the bound FR-001 applied after a delivered batch, computed against the time left
then, stands through the failed batches after it, while the time they spend shrinks what is left.
A sink that delivers, fails a few times quickly, then delivers again is handed a batch sized for a
budget that has gone.

So after every failed exit batch, once SPEC-103's own sizing has run, `_fit_exit_target` applies
FR-001's bound with the rate recorded from the most recent delivered exit batch, against the time
left now. Before any exit batch has been delivered, no rate is recorded and nothing changes. The
bound takes the minimum, so it never raises a target SPEC-103 set and never undoes its probe; a
target that reads zero stays zero.

#### Acceptance Criteria:

- [ ] On FR-001's clock, margin and per-event cost, with the deadline set to 0.578125, take a sink
      that delivers the first exit batch, fails the next three after 0.0625 s each, and delivers
      the fifth. The exit batches are 20, 118, 102, 86 and 70 events, and the clock reads
      0.5390625, the deadline less the margin, when the fifth returns (invariants 3 and 5).
- [ ] Removing the bound from the failed path makes that test fail: the exit batches are 20, 118,
      118, 118 and 118, and the fifth ends at 0.7265625, past the deadline. This is mutation-tested
      (invariants 3 and 5).
- [ ] A dead sink with no exit batch delivered, as in SPEC-103's sequence tests, is handed exactly
      SPEC-103's sequence, and `_exit_rate` reads 0.0 throughout (invariant 2).
- [ ] Serves invariants 2, 3 and 5.

### FR-003: The bound never sets the target below one event

#### Description:

A target of zero means *unmeasured*: SPEC-103's probe sizes the next batch afresh after a failure
when it reads one, and the count trigger reads it as "no size yet". The time left less the margin
can be zero or negative after an exit batch that ended close to the deadline, so the bare product
would be zero or below. The floor of one event makes the next batch the count trigger's minimum,
`batch_size` submissions, without reading as unmeasured, and SPEC-062's prediction then declines it
when even that will not fit: when the longest exit attempt exceeds the time left less the margin,
the next batch is declined unattempted, and its events, with every later exit batch's, are counted
in the existing declined line.

**This floor is the guard whose failure would be silent.** Without it the target reads zero or
negative, which builds the same `batch_size`-submission batch as a target of one event does, so no
batch size, count or line changes until a later exit batch fails. SPEC-103's probe then reads a
zero as no measurement and resizes the next batch from the failure, and reads a negative target as
neither. Nothing announces the difference. It is mutation-tested.

#### Acceptance Criteria:

- [ ] In FR-001's first criterion, after the 54-event batch the time left less the margin is
      exactly 0.0, and `worker._target` reads 1.0 after the join. Replacing `max(1, …)` with the
      bare product makes it read 0.0; this guard is mutation-tested (invariants 2 and 3).
- [ ] With the deadline set to 0.09765625 instead, so that after the first exit batch the time
      left is less than the margin, `worker._target` reads 1.0 where the bare product is -5.0, and
      no later exit batch is handed to the sink (invariants 2 and 3).
- [ ] In FR-001's first criterion, no exit batch after the 54-event one is handed to the sink, and
      the declined line names every queued event not handed and not in the pre-stop batch
      (invariant 2).
- [ ] Serves invariants 2 and 3.

### FR-004: At a tight budget the join completes, the sink is closed, and every event is accounted

#### Description:

The end-to-end outcome on the real clock, in `tests/test_exit_drain_batches.py`'s style: a 1.0 s
interval, `max_retries=0`, a pre-stop batch that fails so the target is zero at the stop, a sink
healthy at exit at 2 ms an event, and 2,000 two-event spans queued. The probe SPEC-103 committed,
`docs/audits/2026-11-14-probes/core-pipeline/spec103_recovered_slow_exit.py` (cited, not copied),
gives the same shape from a script.

#### Acceptance Criteria:

- [ ] Under `shutdown(timeout=0.45)`, the call returns within 0.45 s plus the file's `_SLACK`,
      `stopped_reason` is `None`, no expiry line is written, and `close()` has run once when it
      returns (invariants 3 and 5).
- [ ] In that run, the events the sink returned from, the pre-stop batch's 20 in its abandoned line
      and the count in the declined line add to 4,020, and `ledger_check.worker_problems(worker)`
      is empty (invariant 2).
- [ ] Removing the bound from the delivered path makes the first criterion fail on the join: the
      second exit batch is the rate rule's, and the join expires. This is mutation-tested and
      recorded in the delivery doc (invariants 3 and 5).
- [ ] The engine twin, a `deliver` sink at 2 ms an event with `max_retries=0` at both levels, under
      the same budget, completes the join with `close()` run once and the ledger empty of problems
      (invariant 6).
- [ ] `spec103_recovered_slow_exit.py 2 2000 <timeout>` completes the join at timeouts of 0.45,
      0.5, 0.6 and 0.65 s on 3.12 and 3.13, in two runs each (invariants 3 and 5).
- [ ] Serves invariants 2, 3, 5 and 6.

### FR-005: Where the budget does not bind, nothing changes

#### Description:

The bound is relative to the time left, so it binds only when the budget is what limits the next
batch; the rate rule, and SPEC-103's sizing after a failure, govern otherwise. A `None` deadline
and every steady batch never read it. These criteria hold the healthy exit drain, the failed path
SPEC-103 owns and the first-attempt rule to today's behaviour.

#### Acceptance Criteria:

- [ ] On FR-001's clock and sink with the deadline 64.0 on that clock, the sizes handed to the sink
      equal those of the same run under `retire_process(worker, None)`, and every exit batch after
      the first is 128 events (invariant 3).
- [ ] `spec103_recovered_slow_exit.py 2 2000 30` hands the sink 3,982 events on 3.12 and 3.13, and
      `… 2 2000 2.0` completes its join, as today (invariants 2 and 3).
- [ ] A pre-stop batch delivered while the stop is claimed, with a deadline set on the worker's
      clock during it tight enough that the bound would lower the target, sets the target by the
      rate rule alone and leaves `_exit_rate` at 0.0 (invariant 6).
- [ ] Every SPEC-103 test in `tests/test_exit_drain_batches.py`, every SPEC-090 FR-003 test there and
      `test_a_deadline_already_past_still_makes_the_first_exit_batchs_first_attempt` pass unchanged
      (invariants 3 and 6).
- [ ] The diff adds one `Worker` attribute, `_exit_rate`, with its one row in
      `tests/test_lifecycle_inventory.py`, and no `_diag` call, no constructor parameter, and no
      `SinkCounters` or `Health` field (invariants 10, 11 and 12).
- [ ] Serves invariants 2, 3, 6, 10, 11 and 12.

### FR-006: The record states the bound and its trades, and the item it closes is struck

#### Description:

The completion ritual (`docs/process/completion-ritual.md`, step 5) refines in place, with a dated
History entry, the decision whose rule this changes. That is *The exit drain declines a retry, or
a later batch, predicted to end past the shutdown deadline*
(`docs/decisions/sink-lifecycle/the-exit-drain-declines-only-a-retry-predicted-to-end-past-the-shutdown-deadline.md`),
since the bound exists to keep its prediction honest. It is a refinement, not a reversal: a batch
predicted to end after the deadline is still not work the process gets, and the bound makes the
batch it lets start one that fits. It carries two trades. At a tight budget the sink is handed
fewer events than the rate rule would, and the join completes; that is the decision's own trade.
After a takeover replaces the deadline with `None`, a floored target stands, which is the trade in
Out of Scope, and the decision's body records it with its reason.

#### Acceptance Criteria:

- [ ] The sink-lifecycle decision's fence and body state the bound, after a delivered and after a
      failed exit batch, and why it holds the prediction to the budget; its trade paragraph gains
      the tight-budget trade with this spec's numbers and the takeover trade with its reason; its
      **Rejected** list gains SPEC-103's third bound on the delivered path with the arithmetic below;
      and its History gains a dated SPEC-105 entry at the top (serves no invariant: the register's
      record, judged by the completion ritual).
- [ ] The pipeline decision *A backlog drains in larger batches, and capacity is not a setting*
      names the exit bound in its size-target bullet, pointing at the sink-lifecycle decision, and
      gains a dated History entry; its fence is unchanged, since the bound only lowers a target
      (serves no invariant: the register's record).
- [ ] The docstrings of `_emit_pending`, `_set_target`, `_retry_declined`, `Worker.__init__`'s exit
      fields and `_reinit_after_fork`, the new `_fit_exit_target`, the module text on
      `_JOIN_MARGIN`, and the `Worker` row of `docs/component-inventory.md` state the bound (serves
      no invariant: prose hygiene).
- [ ] The §12 item SPEC-103 FR-003 files is struck in place to one line naming SPEC-105 and moved
      under *Resolved* with this spec's before and after rows; SPEC-103's Out of Scope bullet and
      its FR-003 paragraph filing the item each gain a one-line marker naming SPEC-105 (serves no
      invariant: the record of a closed item).
- [ ] The README's *When the worker flushes* and the pending `docs/release-notes/v1.2.1.md` bullet
      on an exit drain predicted to overrun say that, at `shutdown()`, once a batch is delivered,
      each later batch is also held to what fits in the time left, so a tight budget hands the sink
      fewer events and closes it (serves no invariant: the release artifact no gate holds to the
      code).
- [ ] `sh scripts/docs-lint.sh`, `sh scripts/spec-lint.sh` and
      `poetry run python scripts/docstring-lint.py` exit 0 (serves no invariant: the gates).

---

## Data Model

```python
# src/log_foundry/worker.py, private
class Worker:
    ...  # constructor unchanged
    # SPEC-105 FR-001; drain thread only; 0.0 in __init__ and in _reinit_after_fork on both branches
    _exit_rate: float  # events/s of the most recent delivered exit batch; 0.0 when none recorded
```

## API / Interface Contract

No public surface changes. The private shape, in `Worker._emit_pending` on SPEC-103's code
(`src/log_foundry/worker.py`):

```python
if exit_batch and failed:
    ...  # SPEC-103's probe, unchanged
    self._fit_exit_target()                                       # FR-002
    return
self._exit_probing = False
self._set_target(size, elapsed, failed=failed, generation=generation)
if exit_batch:                                                    # FR-001
    measured = elapsed > 0 and generation == self._sink_generation
    self._exit_rate = size / elapsed if measured else 0.0
    self._fit_exit_target()

def _fit_exit_target(self) -> None:
    deadline = self._exit_deadline
    if deadline is not None and self._exit_rate > 0:
        room = deadline - time.monotonic() - _JOIN_MARGIN
        self._target = min(self._target, max(1.0, self._exit_rate * room))   # FR-003's floor
```

## File & Folder Structure

```
src/log_foundry/worker.py               # FR-001 to FR-003; docstrings for FR-006
tests/test_exit_drain_batches.py        # FR-001 to FR-005
tests/test_backlog_batching.py          # the fork reset of _exit_rate
tests/test_lifecycle_inventory.py       # the _exit_rate row
docs/decisions/sink-lifecycle/the-exit-drain-declines-only-a-retry-predicted-to-end-past-the-shutdown-deadline.md
docs/decisions/pipeline/a-backlog-drains-in-larger-batches-and-capacity-is-not-a-setting.md
docs/architecture.md, docs/component-inventory.md, docs/specs/SPEC-103-*.md   # FR-006
README.md, docs/release-notes/v1.2.1.md                                       # FR-006
```

## Implementation Phases

### Phase 1: The rate and the bound (FR-001 to FR-005)

- Rebase onto `main` with SPEC-103 landed, and add SPEC-103 to `depends_on`.
- Add `_exit_rate`, its resets and its inventory row; record it after a delivered exit batch; add
  `_fit_exit_target` and call it after every emitted exit batch.
- Write the clock, flapping, floor, fork-reset, tight-budget, engine-twin and unchanged-path tests;
  mutation-test the floor, the delivered-path bound and the failed-path bound; run
  `spec103_recovered_slow_exit.py` at the FR-004 and FR-005 timeouts on 3.12 and 3.13 for the
  record.

### Phase 2: The record (FR-006)

- Refine the two decisions, rewrite the docstrings and the inventory row, strike the §12 item and
  mark SPEC-103, and top up the README and the pending release notes.

## Evidence

`spec103_recovered_slow_exit.py 2 2000 <timeout>`: the pre-stop batch fails, so the target is zero
at the stop; at exit the sink is healthy at 2 ms an event; 2,000 two-event spans; a 1.0 s
interval. The prototype bounded the target after a delivered exit batch with that batch's own rate
(FR-001 without the recorded field and without FR-002, which no delivered-only run exercises),
applied to the `src/` of SPEC-103's branch at `68f8695a`. Measured on 3.13.1 in at least two runs
a cell, and on 3.12.12 in two runs each at 0.45, 0.6, 2.0 and 30 s; figures are observed ranges.

| `shutdown` timeout | SPEC-103's tree | This spec's design (prototype) |
|---|---|---|
| 0.45 s | join expired; second exit batch 238 to 246 events | completes in 0.35 s; second batch 50 to 56 |
| 0.5 s | join expired; 248 | completes in 0.39 s; 68 |
| 0.6 s | join expired; 234 to 248 | completes in 0.50 s; 124 to 128 |
| 0.65 s | join expired; 248 | completes in 0.55 s; 154 |
| 2.0 s | 1.72 to 1.73 s; 750 to 764 events handed | 1.69 to 1.72 s; 738 to 758 |
| 30 s | 8.18 to 8.20 s; 3,982 handed | 8.18 to 8.19 s; 3,982 |

`main` at `61b942c0` also expires its join at 0.45 to 0.65 s: the exposure predates SPEC-103, which
does not touch a delivered batch. A healthy exit drain with an ample budget is not slowed: the 2 s
and 30 s rows match. At a tight budget the sink is handed fewer events, and the join completes with
`close()` run. The flapping case (FR-002) was derived on paper from the rule, not prototyped; its
criterion's figures are the rule's exact arithmetic on a fake clock.

## Revision history

- **2026-11-17, drafted.** From the defect SPEC-103's spec review found and SPEC-103 files in §12,
  and from the prototype measured above.
  - **Rejected, not built: SPEC-103's third bound applied to the delivered path**, which holds the
    next batch to `events × (_exit_longest + _JOIN_MARGIN) / _exit_longest`. The arithmetic is
    enough: it lets each delivered batch's attempt run at most one margin, 0.1 s, longer than the
    longest before it, whatever the budget. At 2 ms an event a healthy exit drain would go 20, 70,
    120, 170 and 220 events before reaching the rate rule's 248, and at an interval of 4 s or more
    about twenty batches before the 1,000-event cap, each a sink call, where this spec's bound
    leaves an ample budget exactly as it is. It ties growth to the margin, a quantity unrelated to
    how much time is left, so it would change every healthy exit drain to fix one that runs out of
    time.
- **Author's revision, 2026-11-17.** The orchestrating session answered the three questions the
  draft returned.
  - A target kept through a failed exit batch is closed here rather than left out: the worker
    records the most recent delivered exit batch's rate in `_exit_rate`, and the bound is applied
    after every exit batch once a rate is recorded, after SPEC-103's sizing on the failed path
    (FR-002, with a flapping-sink criterion). FR-005 names the one new attribute and its inventory
    row, and the case leaves Out of Scope.
  - The floor after a takeover that replaces the deadline with `None` is an accepted trade,
    recorded in Out of Scope with its cost and reason, and in the decision's body (FR-006).
  - No arc, matching SPEC-104.
