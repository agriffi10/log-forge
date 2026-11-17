---
type: Spec
id: SPEC-105
title: "Once an Exit Batch Is Delivered, Each Later One Fits the Remaining Shutdown Budget"
description: "Records the rate of the exit drain's most recent delivered batch and, after every exit batch from then on, delivered or failed, holds the next batch to what that rate fits into the time left before the shutdown() deadline less the join margin, whenever the exit budget's prediction would let that batch start, so a sink healthy or flapping at exit is never handed a batch the prediction did not allow for, and a tight shutdown() completes its join and closes the sink, with no setting, line, tally or Health field added."
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
prototyped and measured (*Evidence*); the alternatives considered are under *Revision history*.

**Sequencing.** SPEC-103, being built on branch `spec-103-exit-zero-target`, changes the same
lines of `Worker._emit_pending`: it sizes the next exit batch after a *failed* one, and it files
this defect in `docs/architecture.md` §12 as an open item ("A sink that recovers by exit can be
handed a second exit batch whose attempt the exit budget did not predict"). This spec builds on
SPEC-103's code and closes that item. SPEC-103 is absent from `depends_on` only because
`spec-lint.sh` refuses a dependency on a spec not yet in `docs/specs/`; Phase 1's first step adds
it.

## Scope

### In Scope

- One new drain-thread field, `_exit_rate`, the rate of the most recent delivered exit batch,
  reset with the other exit fields and listed in the lifecycle inventory (FR-001).
- After a delivered exit batch, the next size target is at most what that rate fits into the time
  left before the deadline less `_JOIN_MARGIN` (FR-001).
- After a failed exit batch whose emit did not straddle a sink change, once a rate has been
  recorded, the same bound, applied after SPEC-103's own sizing and only ever lowering it (FR-002).
- The bound applies only when the time left less the margin is at least the longest exit attempt,
  which is when SPEC-062's prediction would let the next batch start (FR-003).
- The end-to-end outcome at a tight budget: the join completes, `close()` runs once, and every
  event is delivered or counted in an existing line, on the 1.x and the engine path (FR-004).
- Where the budget does not bind, the batches are what they are today; in the last margin before
  the deadline, the batch is trimmed and the tail declined (FR-005).
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
- **A failed exit batch whose emit straddled a sink change.** It keeps its target, as SPEC-103
  FR-001 has it, and the bound is not applied after it.
- **A `retarget` that lands between exit batches**, an accepted trade. The rate recorded from the
  old sink can then lower the new sink's first probed target. It only ever lowers a target, the
  case is rare, and keying the rate by sink generation would need a second attribute.
- **A declined exit batch.** It is not emitted, records nothing and is followed only by declined
  batches while the deadline stands, so no bound is applied after it.
- **The first exit batch.** Its size is whatever the target read at the stop, and its first attempt
  is always made (SPEC-090 FR-003). A target measured before the stop is not capped, so a first
  exit batch can still be longer than the budget; that is the first-attempt rule's own trade.
- **A sink whose per-event cost rises within the exit drain.** The bound assumes the next batch
  runs at the recorded rate, as SPEC-062's prediction assumes the next attempt is no longer than
  the longest seen.
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
premise, chosen over SPEC-103's: the worker's clock is one only the sink advances, starting at 0.0;
the pre-stop call costs nothing on it, so the first exit batch begins at 0.0, and every exit call
costs 0.00390625 s an event; `_JOIN_MARGIN` is patched to 0.0390625 s; the interval is 1.0 s;
`max_retries` is 0; the pre-stop batch fails, so the target is zero at the stop; and the deadline is
set on that clock while the first exit batch is in the sink. SPEC-103's
`test_the_longest_exit_attempt_bounds_the_next_batch` charges the pre-stop call too, which would
give 20 and 34 below rather than 20 and 54. Every value below is exact in binary unless it says
otherwise.

#### Acceptance Criteria:

- [ ] On FR-001's clock (the pre-stop call charged nothing) with the deadline at 0.328125 and 500
      spans queued, the first two exit batches are 20 and 54 events. Without the bound they are 20
      and 128 (invariant 3).
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

SPEC-090 keeps a target a delivered batch measured through a fast exit failure, and SPEC-103 keeps
that rule. So the bound FR-001 applied after a delivered batch, computed against the time left
then, stands through the failed batches after it, while the time they spend shrinks what is left.
A sink that delivers, fails a few times quickly, then delivers again is handed a batch sized for a
budget that has gone.

So after every failed exit batch whose emit did not straddle a sink-generation change, once
SPEC-103's own sizing has run, `_fit_exit_target` applies FR-001's bound with the rate recorded
from the most recent delivered exit batch, against the time left now. Before any exit batch has
been delivered, no rate is recorded and nothing changes. The bound takes the minimum, so it never
raises a target SPEC-103 set and never undoes its probe; a target that reads zero stays zero.

#### Acceptance Criteria:

- [ ] On FR-001's clock (the pre-stop call charged nothing) with the deadline at 0.578125 and 500
      spans queued, take a sink that delivers the first exit batch, fails the next three after
      0.0625 s each, and delivers the fifth. The exit batches are 20, 118, 102, 86 and 70 events,
      and the clock reads 0.5390625, the deadline less the margin, when the fifth returns
      (invariants 3 and 5).
- [ ] Removing the bound from the failed path makes that test fail: the exit batches are 20, 118,
      118, 118 and 118, and the fifth ends at 0.7265625, past the deadline. This is mutation-tested
      (invariants 3 and 5).
- [ ] An exit batch that fails across a `retarget` leaves `_target` as SPEC-103 left it, with a rate
      recorded and a deadline under which the bound would lower it (invariant 6).
- [ ] A dead sink with no exit batch delivered, as in SPEC-103's sequence tests, is handed exactly
      SPEC-103's sequence, and `_exit_rate` reads 0.0 throughout (invariant 2).
- [ ] Serves invariants 2, 3, 5 and 6.

### FR-003: The bound applies only when the prediction would let the next batch start

#### Description:

`_fit_exit_target` lowers the target only when `room >= _exit_longest`, which is when SPEC-062's
prediction, asked at that moment, would let the next exit batch start. Otherwise it leaves the
target as it is. Below that line the next batch is declined unattempted whatever its size, so a
smaller target buys nothing, and it would cut the declined backlog into more batches: once the
deadline has passed, `room` is negative, and a bound applied there drove the target to zero or
below, so the declined backlog went out in `batch_size`-submission batches and the declined line
named dozens where it names one. Because `room` is then at least `_exit_longest`, the bound is
positive whenever `_exit_longest` is, so it never sets a target to zero, which SPEC-103's probe
reads as unmeasured.

**This guard is the one whose failure would be silent.** Without it, or with it weakened to
`room > 0`, no event is lost or delivered differently and no line appears or disappears; only the
count of declined batches in the existing declined line changes, and a zero or negative target
waits for a later failure to mislead SPEC-103's probe. It is mutation-tested against
`tests/test_event_ledger_defects.py::test_an_expired_shutdown_leaves_the_batch_in_flight` and the
fake-clock criteria below.

#### Acceptance Criteria:

- [ ] `tests/test_event_ledger_defects.py::test_an_expired_shutdown_leaves_the_batch_in_flight`
      passes unchanged, with its `lost 960 event(s); 1 exit batch(es) declined unattempted`.
      Removing the guard makes it fail with 48 declined batches; this is mutation-tested
      (invariants 2 and 11).
- [ ] On FR-001's clock (the pre-stop call charged nothing) with the deadline at 0.09765625 and 500
      spans queued, so the time left is below the margin after the first exit batch, no later exit
      batch is handed to the sink, the target reads 128.0, and the declined line names 980 events
      in 8 batches. Without the guard it names 49 batches (invariants 2 and 3).
- [ ] On FR-001's clock (the pre-stop call charged nothing) with the deadline at 0.6 and 200 spans
      queued, the exit batches handed are 20 and 122, and the declined line names 258 events in 3
      batches. With the guard weakened to `room > 0` it names 13; this is mutation-tested
      (invariants 2 and 3).
- [ ] In FR-001's first criterion, after the 54-event batch the time left less the margin is
      exactly 0.0, and `worker._target` reads 128.0 after the join, never 0.0 (invariant 3).
- [ ] Serves invariants 2, 3 and 11.

### FR-004: At a tight budget the join completes, the sink is closed, and every event is accounted

#### Description:

The end-to-end outcome on the real clock, in `tests/test_exit_drain_batches.py`'s style: a 1.0 s
interval, `max_retries=0`, a pre-stop batch that fails so the target is zero at the stop, a sink
healthy at exit at 2 ms an event, and 2,000 two-event spans queued. The probe SPEC-103 committed,
`docs/audits/2026-11-14-probes/core-pipeline/spec103_recovered_slow_exit.py` (cited, not copied),
gives the same shape from a script.

An expired join still returns within its budget, so elapsed time alone cannot tell this design
from a tree without the bound. `stopped_reason` and the count of `close()` calls at the return are
the assertions that do.

#### Acceptance Criteria:

- [ ] Under `shutdown(timeout=0.45)`, the call returns within 0.45 s plus the file's `_SLACK`,
      `stopped_reason` is `None`, no expiry line is written, and `close()` has run once when it
      returns (invariants 3 and 5).
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
batch; the rate rule, and SPEC-103's sizing after a failure, govern otherwise. A `None` deadline
and every steady batch never read it. These criteria hold the healthy exit drain, the failed path
SPEC-103 owns and the first-attempt rule to today's behaviour.

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
batch it lets start one that fits. It carries three trades. At a tight budget the sink is handed
fewer events than the rate rule would, and the join completes; that is the decision's own trade.
In the last margin before the deadline, a batch today's code delivered by luck is trimmed and its
tail declined, which Andrew chose on 2026-11-17. A `retarget` between exit batches can let the old
sink's rate lower the new sink's first probed target (Out of Scope).

#### Acceptance Criteria:

- [ ] The sink-lifecycle decision's fence and body state the bound, after a delivered and after a
      failed exit batch, its guard, and why it holds the prediction to the budget; its trade
      paragraph gains the three trades above with this spec's numbers and Andrew's decision; its
      **Rejected** list gains SPEC-103's third bound on the delivered path with the arithmetic below,
      and the floor and `max(0, …)` with their reasons; and its History gains a dated SPEC-105
      entry at the top (serves no invariant: the register's record, judged by the completion
      ritual).
- [ ] The pipeline decision *A backlog drains in larger batches, and capacity is not a setting*
      names the exit bound in its size-target bullet, pointing at the sink-lifecycle decision, and
      gains a dated History entry; its fence is unchanged, since the bound only lowers a target
      (serves no invariant: the register's record).
- [ ] The docstrings of `_emit_pending`, `_set_target`, `_retry_declined`, `Worker.__init__`'s exit
      fields and `_reinit_after_fork`, the new `_fit_exit_target`, the module text on
      `_JOIN_MARGIN`, and the `Worker` row of `docs/component-inventory.md` state the bound and its
      guard (serves no invariant: prose hygiene).
- [ ] The §12 item "A sink that recovers by exit can be handed a second exit batch whose attempt the
      exit budget did not predict" is struck in place to one line naming SPEC-105 and moved under
      *Resolved* with this spec's before and after rows; SPEC-103's Out of Scope bullet and its
      FR-003 paragraph filing the item each gain a one-line marker naming SPEC-105 (serves no
      invariant: the record of a closed item).
- [ ] The README's *When the worker flushes* and the pending `docs/release-notes/v1.2.1.md` bullet
      on an exit drain predicted to overrun say that, at `shutdown()`, once a batch is delivered,
      each later batch is also held to what fits in the time left less the margin, so a tight
      budget hands the sink fewer events and closes it (serves no invariant: the release artifact no
      gate holds to the code).
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
    if generation == self._sink_generation:
        self._fit_exit_target()                                   # FR-002
    return
self._exit_probing = False
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
docs/architecture.md, docs/component-inventory.md, docs/specs/SPEC-103-*.md   # FR-006
README.md, docs/release-notes/v1.2.1.md                                       # FR-006
```

## Implementation Phases

### Phase 1: The rate, the bound and its guard (FR-001 to FR-005)

- Rebase onto `main` with SPEC-103 landed, and add SPEC-103 to `depends_on`.
- Add `_exit_rate`, its resets and its inventory row; record it after a delivered exit batch; add
  `_fit_exit_target` with its guard and call it after every emitted exit batch that did not
  straddle a sink change.
- Write the clock, flapping, guard, fork-reset, tight-budget, engine-twin, margin-band and
  unchanged-path tests; mutation-test the guard (removed, and weakened to `room > 0`), the
  delivered-path bound and the failed-path bound; run `spec103_recovered_slow_exit.py` at the
  FR-004 and FR-005 timeouts on 3.12 and 3.13 for the record.

### Phase 2: The record (FR-006)

- Refine the two decisions, rewrite the docstrings and the inventory row, strike the §12 item and
  mark SPEC-103, and top up the README and the pending release notes.

## Evidence

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
does not touch a delivered batch. A healthy exit drain with an ample budget is not slowed: the 2 s
and 30 s rows match.

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
  - The §12 title quoted is SPEC-103's actual one.
  - The real-clock criteria name `stopped_reason` and the `close()` count as the assertions that
    tell the design from the delivered-path mutant.
