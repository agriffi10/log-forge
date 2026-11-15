---
type: Spec
id: SPEC-085
title: "Adversarial Sink Cells: Sinks That Wait, Call Back and Fork, One Deterministic Cell at a Time"
description: "Adds a table of 637 deterministic test cells in which a sink waits on its stop signal, calls back into the library, is built by a factory or forks, each judged by tight time, close and ledger oracles in a watched child, with every cell that is red on v1.2.0 either pinned to an open item or withheld with the pin it repeats, so a lifecycle defect of these shapes fails a test and not the review of the next change."
spec_status: Draft
depends_on: [SPEC-077, SPEC-079]
arc: audit-2026-11-14
arc_order: 1
---

## Overview

The audit of `v1.2.0` (`docs/audits/2026-11-14-post-1.2.0-audit.md`) found that the repo's tests
drive only passive sinks: none waits on the stop signal the library hands it, calls back into the
library from its own code, or forks. Eight of the twelve lifecycle and sink fixes that followed
the 1.2.0 arc needed a sink that does one of those, and none of the twelve credits an existing
test. This spec adds those sinks as four **personalities** and runs each one through a fixed
table of small, deterministic scenarios called **cells**: one sink, one place its code runs, one
or two calls made from there. Every cell is judged the same way. A cell that is red on `v1.2.0`
is either pinned to the open item that records its defect or withheld because it repeats a
pinned one, so the suite stays green and turns red the day a defect is fixed or a new one
appears. It changes no library code. The arc's next
specs build on it: the generative model driven by the same personalities, then the lifecycle
changes the pins point at.

## Scope

### In Scope

- Four sink personalities, `waiter`, `reentrant`, `factory` and `forker`, and the table of 637
  cells they are run through (FR-001).
- A child process per cell with a watchdog that turns a hang into a failure carrying stacks
  (FR-002).
- The oracles a cell is judged by: time, a wait cut short, closes, the ledger, the end state and
  a forked child's report (FR-003).
- The pin table, fixed here from measurement: 20 pinned cells, 175 withheld, 442 that must pass,
  and one pinned reproduction; four new open items and one correction in `docs/architecture.md`
  §12 (FR-004).
- Proof that the cells bite: they fail, for the stated reason, at the parents of three fixed
  defects (FR-005).

### Out of Scope

- **Any change under `src/`.** A defect is pinned, never fixed here.
- **The generative model.** `tests/test_invariants_model.py` is not changed by this spec; driving
  its two halves with these personalities is the arc's next spec.
- **Deriving the table.** This table is hand-declared and deliberately provisional. A later spec
  of this arc derives the callback-by-public-call matrix from the tree, with a sink's own
  `flush`, transform and filter callables, a signal handler and a fork as calls, and replaces it.
- **Controlled-schedule or exhaustive interleaving exploration**, a sink conformance corpus over
  the shipped sinks, a CI job, any change under `.github/`, and any new dependency.
- **Sink graphs.** No cell wraps a personality in `MultiSink`, `FilteringSink` or
  `TransformSink`, or mixes two personalities.
- **A sink that calls back while holding a lock of its own.** That is §12's "A `shutdown()`
  called inside an orphan-path sink's `emit` closes that sink from inside its own emit", a
  different personality.
- **A forked child that returns into the library call it was forked inside.** A cell's child
  runs its script and leaves through `os._exit` without unwinding. The audit's finding 19 (a fork
  from `close()` during a no-worker `shutdown()`) needs the child to unwind that `shutdown()`, so
  no cell reaches it: the `forker` cell at site `close` on the orphan path is green at
  `a0f44b5f`. It belongs with fork-as-a-call in the matrix spec.
- **The CPU half of invariant 3**, and the async decorator body.

---

## Functional Requirements

*Evidence* at the end of this spec holds every measurement the requirements rest on. Each was
taken at `a0f44b5f` unless a commit is named, with a prototype of this spec kept at the path
given there.

### FR-001: Four personalities and a fixed table of 637 cells

#### Description:

**A personality** is a test-double sink with one adversarial behaviour. All four are built on one
recorder that counts the delivery calls entering it and records that count at each `close`'s
entry and return (FR-003).

| Personality | Behaviour |
|---|---|
| `reentrant` | Declares `log_foundry_stop_signal` as a property. Its setter, its `emit` and its `close` can run a **script** on the thread that reached them. |
| `factory` | A plain recorder handed to the library with `configure(sink_factory=…)`. The factory callable can run a script before it returns the recorder. |
| `waiter` | Keeps the signal it is handed in a plain attribute. When the cell has **armed** it for `emit` or for `close`, the next such call waits on the event it holds then, with `Event.wait`, for at most `WAITER_HOLD`; holding none, it sleeps that long. |
| `forker` | At one site, once, calls `run_in_child` (`tests/test_fork_lifecycle.py:319`) from inside the sink code and reaps the child there. |

**A script** is one or two **operations**, made in order: `info` (a level call outside a span),
`span` (one traced call that logs once), `flush` (`timeout=2.0`), `flush-none` (`timeout=None`),
`shutdown` (`timeout=3.0`), `shutdown-none` (`timeout=None`), `configure` (a sink change to a new
sink of the same personality that runs no script). That is 7 one-operation and 49 two-operation
scripts, 56 in all. Two operations are needed: the defect #345 fixed appears only when a factory
makes a traced call and then waits.

**Where sink code runs.** A script runs after the recorder's own bookkeeping: in the setter after
the value is stored, in `emit` after the write is counted and the events kept, in `close` after
the entry count is recorded and before the return count. The setter is called on every
assignment, `None` included; a cell's script runs on the first visit to the cell's site and
never again. A thread inside a script starts no second script. No personality holds a lock of
its own while a script, a wait or a fork runs.

**Who decides what runs at a visit.** A personality does not hold a script. It is built with
two **hooks** and asks them: `script(sink, site)` at every visit to a site, which returns the
script to run there or `None`, and `new_sink()` for the sink a script's `configure` installs. A
`forker` asks the same `script` hook and forks when it answers `("fork",)`; a `factory`'s
callable asks it at every build; a `waiter` takes its hold as a constructor argument and is
armed by its driver. A cell's hooks answer with the cell's script on the first visit to the
cell's site and `None` ever after. This spec owns the hooks: the generative model that follows
supplies hooks that draw a script per visit, and changes nothing in the personalities.

**A cell** is `(personality, site, path, script)`. The **path** is how the sink is first reached:
`orphan` is one `info()`; `worker` is one traced call followed by `flush(timeout=REACH_TIMEOUT)`,
long enough that the cell's own flush never races a finite wait inside a script. Every cell runs
this procedure, in a child (FR-002): install the personality's sink with `configure`; reach it on
the path; make the site's extra call, if it has one; call `shutdown(timeout=5.0)`; apply the
oracles; run the exit drain (`_lifecycle._shutdown_worker()`, what `atexit` calls); apply the
close oracles again.

| Site | Reached by |
|---|---|
| `setter`, `emit` | the path's first call |
| `close` | the cell's `shutdown` |
| `swap-close` | a second `configure(sink=…)` after the path, which displaces the sink and closes it |
| `swap-emit` (worker path only) | a traced call whose batch the drain thread delivers while a second `configure(sink=…)` is in flight: the sink's first `emit` sets an event and waits until a swap token is open (`_lifecycle._state._swaps_running`), the cell waits on that event and then calls `configure`; no reaching flush. Each of the two waits is bounded by `GATE_TIMEOUT`, and one that runs out makes the cell `NotExercised:gate` |
| `factory` | the path's first delivery, which builds the sink |

| Personality | Site and path combinations | Scripts | Cells |
|---|---|---|---|
| `reentrant` | `setter`, `emit`, `close`, `swap-close` on both paths; `swap-emit` on `worker` (9) | all 56 | 504 |
| `factory` | `factory` on both paths (2) | all 56 | 112 |
| `forker` | the same 9 as `reentrant` | `fork` | 9 |
| `waiter` | `emit` on both paths with `shutdown`, `exit`, `child-shutdown`, `child-exit`; `close` on both paths with `shutdown`, `exit` | the stop named | 12 |

**The waiter's cells.** After the path, the cell arms the sink and puts one call inside the
wait: for `emit` on the orphan path a helper thread's `info()`, on the worker path a traced
call's batch on the drain thread; for `close` the stop's own close. It waits on an event the sink
sets on entering the wait, records the time, and makes the **stop**: `shutdown(timeout=3.0)` or
the exit drain. In the two `child-` forms the cell forks first, through `run_in_child`, and the
child does the arming, the call and the stop on the sink it inherited. A child never closes an
inherited sink, so there is no `child-` form for `close`.

**The forker's child** logs once outside a span, makes one traced call, calls
`flush(timeout=2.0)` and `shutdown(timeout=3.0)`, applies the time oracle to its four calls, and
reports where its two events landed, what it closed and its state.

#### Acceptance Criteria:

- [ ] The four personalities exist in `tests/model_sinks.py` with the behaviours above, and each
      script, wait and fork runs with no lock of the personality's own held.
- [ ] A personality asks its `script` hook at every visit to a site and its `new_sink` hook for
      every sink a script's `configure` installs; a test drives one `reentrant` sink with hooks
      that answer on the second visit and asserts the script ran then and not on the first.
- [ ] The cell table is generated from the tables above and holds exactly 637 cells: 504, 112, 9
      and 12; a test asserts those four numbers as literals.
- [ ] Every cell is one test item whose id is `personality-site-path-script`, with a two-operation
      script joined by `+`, and can be selected by that id and run alone.
- [ ] Every cell follows the one procedure above, on the orphan path and on the worker path, so
      one script is judged on both delivery paths (invariant 6).
- [ ] A cell whose site was never reached fails as `NotExercised`, so a cell cannot pass by doing
      nothing.
- [ ] `git diff --stat origin/main -- src/ .github/ tests/test_invariants_model.py` is empty for
      the branch.

### FR-002: A cell runs in a watched child and reports a verdict

#### Description:

A personality can deadlock the library: at `b2528383`, the parent of #341, every one of the 112
`reentrant` cells at site `setter` never returns. So each cell runs in a child made by
`run_in_child`, which already bounds a child twice, by its own alarm and by the parent's
deadline. Those two say only that a child died. This FR adds what says why.

**The watchdog.** Inside the child every call the cell or a script makes is registered while it
is in flight. A watchdog thread ends the child when a call's **own time** (FR-003) passes its
budget plus `EPSILON` plus `HANG_AFTER`. It writes one line naming the call and its thread, then
every thread's stack with `faulthandler.dump_traceback`, to the child's pipe (the descriptor in
`test_fork_lifecycle._child_pipe`, read as a module attribute at the time of use, since the
child assigns it), and leaves through `os._exit`. The deadline is on own time, not elapsed time,
so a call waiting on sink code that is still running elsewhere is not killed for it. A cell whose
procedure ends with a call still in flight waits for that call before it judges.

**The child's own timeout** is `CELL_TIMEOUT`, 60 s, passed to `run_in_child`, whose default of
10 s is shorter than the watchdog's deadlines: 11 s for a `configure` or the cell's `shutdown`,
36 s for the exit drain. Sixty clears the longest deadline plus every finite budget a cell can
spend before it (10 s reaching, 5 s shutting down, 8 s in a script). With the default, two of
the 112 cells of FR-005's second row die of the alarm before the watchdog can report.

**A child of a child.** Threads do not survive a fork, so a `forker`'s child and a `waiter`'s
`child-` form start a watchdog of their own on their own pipe. The cell that reaped one adds
`ChildBroken:hung` when it died of a watchdog, its alarm or the deadline, and
`ChildBroken:crashed` when it ended any other way without a report.

**The verdict.** The oracles run in the child, on the child's own records, and all of them run;
none stops at the first failure. The child reports a **verdict**: the set of `class:key`
signatures of everything that failed, empty for a pass. The parent adds one of two when the child
did not report: `HangFound:<call>@<module>.<function>`, from the watchdog's line and the
innermost `log_foundry` frame of the hung call's thread (`@none` when that thread has no such
frame), or `HangFound:silent` when a child died
of its alarm or the parent's deadline with no watchdog line; and `ChildCrashed` when it ended any
other way, by another signal, a non-zero exit or an exception out of the cell's own code.

| Class | Keys | Raised when |
|---|---|---|
| `HangFound` | `<call>@<module>.<function>`, `silent` | above |
| `ChildCrashed` | `<how it ended>` | above; never an expected failure |
| `NotExercised` | `site`, `gate` | the cell's sink code never ran, or a `swap-emit` gate ran out |
| `CallRaised` | `<call>` | an exception escaped a library call (invariant 1) |
| `BoundExceeded` | `<call>` | a call with a finite timeout overran it (FR-003) |
| `Stalled` | `<call>` | a call given no timeout did not return at once (FR-003) |
| `WaitNotCut` | `wait` | a wait outlived the stop that should cut it |
| `CloseBroken` | `repeat@<count>`, `never-closed@<writes>`, `unreleased@<count at the last close>`, `inherited-closed` | a close rule broke |
| `LedgerBroken` | `spans<signed excess>`, `orphans<signed excess>`, as in `spans+1` | an identity does not balance, by that many |
| `StateBroken` | `end` | the cell did not end `CLOSED` with no token held |
| `ChildBroken` | `two-sinks`, `uncounted`, `closed-inherited`, `state`, `hung`, `crashed` | a forked child's report, or its ending |

A key says which oracle broke, on which call, and by how much where that is an exact number.
`Stalled` carries no number because a duration is not a stable key; a second defect that stalls
the same call in a pinned cell would hide behind the pin until the first is fixed, and the
failure text, which carries the measured time and the stacks, is where it would show.

#### Acceptance Criteria:

- [ ] Every cell runs in a child made by `run_in_child` with `timeout=CELL_TIMEOUT`, and every
      fork in the model files goes through `run_in_child`.
- [ ] A call whose own time passes its budget plus `EPSILON` plus `HANG_AFTER` ends the child,
      and the parent's verdict is `HangFound:<call>@<module>.<function>` with every thread's
      stack in the failure text (invariant 3).
- [ ] A child that dies of its alarm or the parent's deadline with no watchdog line gives
      `HangFound:silent`; one that ends any other way without a report gives `ChildCrashed`.
- [ ] Every oracle of FR-003 runs in every cell whatever the others found, and the verdict is
      the set of all their signatures.
- [ ] A fixture corpus drives the child runner and asserts each verdict: a body that parks on a
      lock inside a model call (`HangFound:<call>@none`), one that raises (`ChildCrashed`), one
      that kills itself with `SIGSEGV` (`ChildCrashed`), one that returns (empty), one whose site
      is never reached (`NotExercised:site`), one whose library call raises (`CallRaised`), and
      one whose forked child parks (`ChildBroken:hung`).

### FR-003: The oracles a cell is judged by

#### Description:

**Time.** Each call has a **budget**: its timeout when that is a number, `DEFAULT_SWAP_TIMEOUT`
for a sink-changing `configure`, `DEFAULT_SHUTDOWN_TIMEOUT` for the exit drain, and zero for a
call given none (`info`, `span`, `flush-none`, `shutdown-none`). Its **own time** is its elapsed
time minus the time during which sink code was running that it could have been waiting on: the
union of every stretch of model sink code (a script, an armed wait, a fork and its reaping) on
any thread that overlaps the call, except a stretch on the call's own thread that began before
the call and ended after it, which is the code the call was made from. This is invariant 3's
observable, "elapsed wall time ≤ the timeout plus the sink's one in-flight call", with the sink's
share measured. A close runs on a closer thread and a delivery on the drain thread, so the
subtraction has to cross threads: subtracting only the caller's thread flags a `shutdown()` that
is correctly waiting for a slow close.

The oracle is `own time ≤ budget + EPSILON`. A call with a finite timeout that breaks it is
`BoundExceeded`, which is invariant 3's bound. A call with no timeout that breaks it is
`Stalled`: it had nothing to wait for and waited. That is the model's own, stricter question,
and it is how a self-wait becomes visible. `EPSILON` is 1.0 s: over twice the slowest return
measured under load, twice `tests/test_timing_floors.py`'s `STALL_ALLOWANCE`, and half the
shortest self-wait these cells must see, the 2.0 s closer grace.

**What the time oracle cannot judge.** A self-wait that ends inside a finite budget passes: a
nested `shutdown(timeout=3.0)` that waits 2.0 s on itself is inside its budget, which is why
`flush-none` and `shutdown-none` are operations. And a call is not judged for any time it
overlaps sink code on another thread, whether or not it was waiting on it. In the cells whose
sink code runs on a library thread (`emit` and `swap-emit` on the worker path, `swap-close`, the
worker-path `factory` and `waiter` cells) that covers the cell's own reaching flush, displacing
`configure` and `shutdown`. The calls a script makes are always judged, because the script is
the stretch they are exempt from subtracting.

**A wait cut short.** A `waiter`'s wait in flight when its stop is entered must end within
`EPSILON` of that entry (invariant 3: "a shutdown cuts it short"). Nothing is asserted about how
long a wait was served otherwise: no lower bound appears in the model files.

**Closes.** A write is counted when the library's call enters the sink, at the first statement
of the method the library calls. Three rules:

- **Repeat close** (after the cell's `shutdown`, and again after the exit drain): no close of a
  sink enters at the same count as the close before it.
- **Closed** (after the cell's `shutdown`): every sink that was written to has had a close. This
  is invariant 5's first observable, and the check the model test already makes.
- **Released** (after the exit drain): no write entered a sink after its last close entered.

The third rule waits for the exit drain because invariant 5 records that "a sink written to
after its close is owed a second close" (SPEC-045), and owed is not closed: 24 cells write to a
sink from inside its own close, the library records it owed, and the exit drain closes it.
Asserted straight after `shutdown`, the rule called those 24 red for behaviour the record
settles; 16 of the 24 are among the cells that run.

**The one exemption from repeat close.** The rule is not judged in a cell where two of the
model's own sink-changing `configure` calls overlapped in time, on any threads: one began
before the other ended. That is what
`docs/decisions/public-api/concurrent-sink-changes-are-serialized-by-one-lock-whose-wait-is-spent-from-the-swap-budget.md`
permits: a sink change re-entered by a sink's own code runs unserialized, and of two changes
running alongside each other it records that "a sink may be closed twice". The oracle reads the
overlap from the calls it timed, not from the library's line: `config.py` writes "swapped
without waiting or confirming its drain" for every re-entrant `configure`, including one with no
other change in flight, and an exemption keyed on the line let a planted double close through
(FR-004). Of the cells that run, 30 are exempt: every one at `swap-close` and `swap-emit` whose
script holds `configure`.

**The ledger.** Spans: `span.end` events delivered, plus `health().queued` less flush markers,
plus `dropped`, plus the `span.end` events in batches the worker abandoned, equals the traced
calls made. Orphans: delivered plus `orphan_lost` equals `info` calls made. The abandoned term
exists because a batch refused while a factory is inside its own `flush()` or `shutdown()` is a
counted failure by the fence of
`docs/decisions/sink-lifecycle/a-sink-factory-runs-under-no-lock-and-a-build-that-calls-back-releases-its-waiters.md`,
not a defect. It is observed by wrapping `Worker._emit`: when `failed_batches` rose across a
call, the `span.end` events in the event lists that call was handed are abandoned.
`failed_batches` alone will not do, since one batch can hold several traced calls.

**End state.** After the cell's `shutdown` the runtime reads `CLOSED` and holds no swap or stop
token.

**A forked child.** It closes no sink it inherited; its span and its orphan log land in at most
one sink between them; each is delivered or moves a counter; and it reads `CLOSED` after its own
`shutdown()`.

#### Acceptance Criteria:

- [ ] Every call a cell, a script or a forked child makes is held to `own time ≤ budget +
      EPSILON` with own time as defined above, failing as `BoundExceeded` for a finite timeout
      (invariant 3) and as `Stalled` for none; an exception escaping any of them is `CallRaised`
      (invariant 1).
- [ ] A `waiter`'s wait in flight at its stop ends within `EPSILON` of the stop's entry, in the
      parent and in a forked child, for a wait in `emit` and in `close` (invariant 3), on both
      paths (invariant 6).
- [ ] The three close rules are asserted at the two points named, with writes counted at entry
      (invariant 5), and the repeat-close rule is skipped only in a cell where two of the
      model's sink-changing `configure` calls overlapped in time.
- [ ] The two ledger identities hold in every cell, with every call a script made counted
      (invariant 2), and the cell ends `CLOSED` with no token held.
- [ ] A forked child's report is held to the four checks above (invariant 12).
- [ ] Each oracle, the ledger, end-state and child oracles included, is a pure function over
      recorded data with a fixture corpus asserting silence and failure, by signature,
      including: a finite call that used its whole budget (silent); a
      call that overlaps another thread's sink code for its whole length (silent); a no-timeout
      call over `EPSILON` (`Stalled`); a sink written during its own close and closed by the exit
      drain (silent); two closes at one count (`repeat`); a write after the last close with no
      later close (`unreleased`).
- [ ] No assertion in the model files is a lower bound on a duration, and
      `tests/test_timing_floors.py` passes over them unchanged.
- [ ] The pytest session prints one line naming the worst `own time − budget` over the cells
      whose verdict is empty, with its test id, under `-n 12` as under `-n 0`; a pinned cell's
      margin is not in it, since four of them sit 2.00 s over by design.
- [ ] **Owed until the pull request's green CI run:** the delivery doc records the worst margin
      over passing cells that run printed on each Python leg, and each is at most
      `MARGIN_REDERIVE` (0.5 s). The
      bound was measured on macOS only, so the pull request body names this criterion as owed,
      and a leg above 0.5 s stops the merge: `EPSILON` then changes only by a revision of this
      spec, since a value above 1.0 s gives up the 2.0 s self-wait.

### FR-004: The red cells are pinned, the pins are fixed here, and a pin cannot go quiet

#### Description:

At `a0f44b5f`, 105 of the 637 cells are red, the same 105 with the same verdicts in four runs
and, for the cells that run, three more under a load average of 50. This spec fixes that set. A
cell that is red at build time and not listed here, or listed and green, is a finding the build
reports before it changes anything.

**A pin** names cells, the exact verdict each has, and its **item**: the bold lead of a bullet
under `### Open` in `docs/architecture.md` §12. A pinned cell runs, and is reported as an
expected failure only when its verdict equals its pin's; a pass, or any other verdict, fails the
suite. A pin on a one-operation script also **withholds** every longer script that contains it
at the same personality, site and path, and may name one sibling operation, the same defect in a
form the time oracle cannot see, whose scripts are withheld too. A withheld cell is not run: it
is the pinned defect again, and each costs seconds of waiting. Everything a pin withholds is
computed from the pin, so deleting the pin brings it back.

| Pin | Pinned cells | Verdict | Withheld | Item |
|---|---|---|---|---|
| `drain-flush` | `flush-none` at `reentrant` `emit`/`worker`, `reentrant` `swap-emit`/`worker`, `factory` `factory`/`worker` (3) | `HangFound:flush-none@worker.flush` | at those three: every longer script with `flush-none`, and every script with the sibling `flush` (75) | existing, corrected by this spec: see below |
| `nested-shutdown` | `shutdown-none` at `reentrant` `close` and `swap-close`, both paths (4) | `Stalled:shutdown-none` | at those four: every longer script with `shutdown-none`, and every script with the sibling `shutdown` (100) | "`shutdown()` called from a sink's own `close()` waits out the closer grace on that same close" (audit finding 20) |
| `empty-submission` | `reentrant` `setter`/`worker` with `flush` or `flush-none` beside `shutdown` or `shutdown-none`, in either order (8) | `LedgerBroken:spans+1` | none | new, (a) below |
| `orphan-arm-in-close` | `reentrant` `close`, both paths, `configure+info` (2) | `CloseBroken:never-closed@1` | none | new, (b) below |
| `installed-closed-twice` | `reentrant` `swap-emit`/`worker` with `info+shutdown` and `info+shutdown-none` (2) | `CloseBroken:repeat@1` | none | new, (c) below |
| `fork-mid-swap` | `forker` `swap-emit`/`worker` (1) | `ChildBroken:two-sinks` | none | "A child forked while a sink change is in flight sends its spans to the old sink and its orphan logs to the new one" (audit finding 6) |
| `orphan-write-after-close` | none: a reproduction test | `CloseBroken:unreleased@1` | none | new, (d) below |

That is 20 pinned cells, 175 withheld and 442 that run and must pass. Of the 105 red cells, 20
are the pinned ones and 85 are among the withheld. The other 90 withheld cells are green: 84
hold only a sibling operation, and 6 hold `flush-none` after a nested shutdown has already
retired the worker.

**The reproduction.** One defect was found by the generative model, needs two threads, and has
no cell: an orphan log that has passed its owed check when a `shutdown()` takes its sink writes
after the close, and the sink is owed nothing. The test
`test_an_orphan_log_parked_after_its_owed_check_writes_after_the_close`, in a child, installs a
recorder, logs once outside a span, patches `log_foundry.api._note_orphan_emit` (the name
`api.py` imports and calls) to call the real function and then park on an event, starts a thread
whose `info()` parks there, calls `shutdown()`, releases and joins the thread, runs the exit
drain and applies the released rule. It is the pin's strict expected failure.

**A mutant the close oracle must kill.** The repeat-close rule guards a fixed class of defect,
so it is run against one. In `_lifecycle._swap_sink`, in the section that decides what a swap
releases, directly before `if fenced_out and previous is not None:`, the mutant adds:

```python
if (
    not fenced_out
    and previous is not None
    and previous is not new_sink
    and threading.current_thread().name.startswith("log-foundry-")
):
    previous.close()
```

A swap running on a library thread then closes the sink it displaced once more. Against a tree
exported from the branch head with that one change, the eight `reentrant` `emit`/`worker` cells
that run and hold `configure` in their script (`configure`, `configure+configure`,
`configure+info`, `configure+shutdown`, `configure+shutdown-none`, `configure+span`,
`info+configure`, `span+configure`) must each have a `CloseBroken:repeat@<count>` signature in
their verdict; measured, the count is 1 in six of them, 2 in `info+configure`, and both 1 and 0
in `configure+configure`. The mutant also closes a sink twice in 18 cells at `swap-close` and
`swap-emit`; those are exempt, because two sink changes are in flight there and the decision
permits exactly that outcome.

**The four items this spec's build adds to §12**, each one bullet in §12's own form: the bold
lead below, tagged `(SPEC-085; invariant N)`, then the measurement under *Evidence* for its pin,
then what closes it. Nothing else is said in them.

- **(a)** "An empty batch submitted after a `shutdown()` is counted as queued, counted as
  submitted after shutdown and announced as a lost submission" (invariant 2). **Closed by** not
  submitting, or not counting, a batch with no events.
- **(b)** "A sink first written by an orphan log made inside a `shutdown()`'s close is not closed
  by that `shutdown()`" (invariant 5, first observable). **Closed by** the shutdown's final pass
  taking what was armed during its own closes, as it does for a worker built there.
- **(c)** "A sink a swap installs is closed twice with nothing written between when an orphan log
  armed it and a re-entrant `shutdown()` closed it before the swap decided" (invariant 5). **Closed
  by** the one lifecycle command path, or by the swap not arming a sink a shutdown has closed
  since the swap began.
- **(d)** "An orphan log that passed its owed check before a `shutdown()` took its sink writes
  after the close, and the sink is owed nothing" (invariant 5). It also says the measurement used
  a recorder that keeps accepting after `close()` and that a shipped sink behind the closed guard
  was not measured. **Closed by** a mark around the orphan path's delivery call that a closer
  waits for or that re-arms the sink once the call returns, or by one lifecycle command path.

**The one correction.** §12's item "`flush()` and `configure(sink=…)` called on the drain thread
wait on themselves" records "a 5.007 s stranded swap". At `a0f44b5f` a `configure(sink=…)` made
from `emit` on the drain thread returns in 0.00 s with two announced lines. The build corrects
the item in place: the `configure` half is struck through with the measurement and its commit
beside it, not deleted; the `flush()` half is restated as measured (2.00 s for
`flush(timeout=2.0)`, no return for `flush(timeout=None)`, and a later `shutdown(timeout=5.0)`
that expires); and the lead becomes "`flush()` called on the drain thread waits on itself",
which is the `drain-flush` pin's item.

#### Acceptance Criteria:

- [ ] `PINS` holds the seven pins above, and a test asserts as literals that the table has 637
      cells, 20 pinned, 175 withheld and 442 run-and-must-pass.
- [ ] A pinned cell is reported as an expected failure only when its verdict equals its pin's
      verdict exactly; a pinned cell that passes, or fails with any other verdict, fails the
      suite.
- [ ] Every one of the 442 cells passes on the branch, and the six local gates are green.
- [ ] A test asserts every pinned cell is a member of the generated table, every pin's
      reproduction is a test function of the cells file, and every pin's item is the bold lead
      of a bullet between `### Open` and `### Resolved` in `docs/architecture.md`.
- [ ] The withheld set is computed from `PINS` alone; a test asserts that with `PINS` emptied
      nothing is withheld and all 637 cells are scheduled.
- [ ] The delivery doc records three mutations and the test each reddened: one cell removed from
      the generated table, one pinned cell removed from a pin, and one pin's item misspelled.
- [ ] Against a tree exported from the branch head with the `_swap_sink` mutant above as its only
      change, each of the eight named `reentrant` `emit`/`worker` cells has a
      `CloseBroken:repeat@<count>` signature in its verdict;
      against the unmutated export the same run reads 442 passed and 20 expected failures; the
      delivery doc records both counts and that `log_foundry.__file__` was under each export.
- [ ] The reproduction test fails as `CloseBroken:unreleased@1` on every run at the branch head,
      and the same body without the park passes.
- [ ] `docs/architecture.md` §12 gains items (a) to (d) with the leads above, and the drain-thread
      item is corrected as described; the file's diff against `origin/main` touches nothing else.
- [ ] A test reads the model files' source and fails on `skip`, `skipif`, `importorskip` or a
      non-strict `xfail`.
- [ ] A cell whose verdict at build time differs from this spec's (red and unlisted, or listed
      and green) is recorded in the delivery doc with its verdict and reported to the owner
      before a pin is added, changed or removed.
- [ ] This FR serves no invariant: it is the register of where the cells are red and the gate on
      that register; the promises are FR-003's.

### FR-005: The cells fail, for the stated reason, at the parents of three fixed defects

#### Description:

Running a gate against the tree it guards shows the tree passes, not that the gate works. The
corpora of FR-002 and FR-003 show each piece fires. This FR shows the whole does, where it is
known a defect was: the parents of three fixes the audit said this harness should have caught.
A class alone is not a reason, since any hang is a `HangFound`, so each row names where.

| Fix | Parent | Cells | Verdict there | And |
|---|---|---|---|---|
| #337, `95945c0f` | `641dfc86` | `waiter-emit-orphan-child-shutdown`, `waiter-emit-orphan-child-exit` | `WaitNotCut:wait`, the wait served for `WAITER_HOLD` | the two worker-path `child-` cells pass there |
| #341, `9f4d4dfd` | `b2528383` | all 112 `reentrant` cells at site `setter` | `HangFound:<first operation>@_lifecycle.<function>`, the function being the one that takes the lock that operation needs: `_note_orphan_emit` for `info`, `_get_worker` for `span`, `_shutdown_worker` for `shutdown` and `shutdown-none`, `_swap_sink` for `configure`, and for `flush` and `flush-none` `_flush_live_sink` on the orphan path and `_get_worker` on the worker path | the setter was run under the lock its call then needs; at the branch head the same calls return |
| #345, `83550f01` | `144f87c8` | `factory-factory-orphan-span+flush-none`, `factory-factory-orphan-span+shutdown-none` | `HangFound:flush-none@worker.flush`, `HangFound:shutdown-none@worker.stop` | a `log-foundry-worker` thread's stack is inside `sinks/_factory.py`: the drain thread is waiting on the build the factory is running |

**How, without rewriting history.** `git archive <parent> | tar -x -C <scratch>` exports the
parent's whole tree; the model files (*File & Folder Structure*) are copied over its `tests/`;
the named cells are run from that directory. This repo has been caught by an editable install
resolving a copied tree back to the original and every mutant "passing", so every cell's report
carries `log_foundry.__file__` and a run is void unless that path is under the scratch
directory. The model files import from `tests/` only `run_in_child` and `_child_pipe`, which
exist at `641dfc86`.

**A row that does not fail as stated is a finding, not a criterion to loosen.** The delivery
doc records what was seen, the spec does not move to `Completed`, and the miss goes to the owner
with the measurement.

#### Acceptance Criteria:

- [ ] At `641dfc86` the two named `waiter` cells have verdict `WaitNotCut:wait` and the two
      worker-path `child-` cells pass (invariant 3); at the branch head all twelve `waiter`
      cells pass.
- [ ] At `b2528383` each of the 112 `reentrant` `setter` cells has the verdict
      `HangFound:<first operation>@_lifecycle.<function>` with the function the table gives for
      that operation and path; at the branch head none of the 112 hangs.
- [ ] At `144f87c8` the two named `factory` cells have the two verdicts in the table and a drain
      thread inside `sinks/_factory.py` (invariant 3); at the branch head both pass.
- [ ] Every run at a parent reports `log_foundry.__file__` under that parent's scratch directory.
- [ ] The delivery doc has one row per fix with the command, the cell ids, the verdicts and the
      counts; a row that did not fail as stated is recorded as not caught and the spec stays
      `In Progress`.

---

## Data Model

```python
# tests/model_oracles.py
EPSILON = 1.0            # seconds over a call's budget
MARGIN_REDERIVE = 0.5    # a worst CI margin above this stops the merge (FR-003)
HANG_AFTER = 5.0         # own-time seconds past budget + EPSILON before the watchdog ends a child
WAITER_HOLD = 4.0        # an armed wait
REACH_TIMEOUT = 10.0     # the worker path's reaching flush; above any finite wait in a script
GATE_TIMEOUT = 5.0       # each of swap-emit's two waits
CELL_TIMEOUT = 60        # run_in_child's timeout for a cell; above the longest watchdog deadline

class ModelFailure(AssertionError): ...
# HangFound, ChildCrashed, NotExercised, CallRaised, BoundExceeded, Stalled, WaitNotCut,
# CloseBroken, LedgerBroken, StateBroken, ChildBroken: one subclass each (FR-002's table)

@dataclass(frozen=True)
class Interval:           # one stretch of model sink code
    thread: int
    began: float
    ended: float

@dataclass(frozen=True)
class Call:
    name: str             # an operation, or "configure", "reach-flush", "final-shutdown", "exit"
    thread: int
    budget: float         # 0.0 for a call given no timeout
    began: float
    ended: float

@dataclass(frozen=True)
class Wait:
    began: float
    ended: float
    stop_entered: float

@dataclass(frozen=True)
class Closes:
    writes: int
    at_entry: tuple[int, ...]    # the write count as each close entered
    at_return: tuple[int, ...]

def own_time(call: Call, intervals: Sequence[Interval]) -> float: ...
def check_call(call: Call, intervals: Sequence[Interval]) -> None: ...   # BoundExceeded | Stalled
def check_wait(wait: Wait) -> None: ...                                  # WaitNotCut
def check_repeat(closes: Closes) -> None: ...                            # CloseBroken "repeat"
def check_closed(closes: Closes) -> None: ...                            # CloseBroken "never-closed"
def check_released(closes: Closes) -> None: ...                          # CloseBroken "unreleased"

@dataclass(frozen=True)
class Ledger:
    spans: int            # traced calls made
    span_ends: int        # span.end events delivered
    queued: int           # health().queued less flush markers
    dropped: int
    abandoned: int        # span.end events in batches the worker abandoned
    orphans: int          # info calls made
    orphans_delivered: int
    orphan_lost: int

@dataclass(frozen=True)
class ChildReport:
    ending: str                       # "reported" | "hung" | "crashed"
    orphan_in: tuple[int, ...]        # indices of the sinks its orphan log landed in
    span_in: tuple[int, ...]
    counters_moved: bool              # queued, dropped, failed_batches or orphan_lost rose
    closed_inherited: tuple[int, ...]
    state: str

def check_ledger(ledger: Ledger) -> None: ...                            # LedgerBroken
def check_end(state: str, tokens: int) -> None: ...                      # StateBroken
def check_child(report: ChildReport) -> None: ...                        # ChildBroken

Verdict = frozenset[str]         # "Class:key" signatures; empty is a pass

# tests/model_pins.py
@dataclass(frozen=True)
class Cell:
    personality: str             # "reentrant" | "factory" | "waiter" | "forker"
    site: str                    # "setter" | "emit" | "close" | "swap-close" | "swap-emit" | "factory"
    path: str                    # "orphan" | "worker"
    script: tuple[str, ...]      # one or two operations; ("fork",); or the waiter's stop

@dataclass(frozen=True)
class Pin:
    name: str
    cells: tuple[Cell, ...]      # run, strict; empty for a pin held by a reproduction
    verdict: Verdict
    item: str                    # the bold lead of a §12 Open bullet
    sibling: str | None = None   # an operation whose scripts are withheld with the pinned one
    reproduction: Callable[[], None] | None = None

class Hooks(Protocol):
    def script(self, sink: object, site: str) -> tuple[str, ...] | None: ...   # asked at every visit
    def new_sink(self) -> object: ...                 # the sink a script's `configure` installs

PINS: tuple[Pin, ...]
def cells() -> tuple[Cell, ...]: ...          # all 637
def withheld(cell: Cell) -> bool: ...         # computed from PINS
```

## File & Folder Structure

```
tests/
├── conftest.py             # changed: a terminal-summary hook prints the worst-margin line from
│                           #   each test's recorded property, which is what survives -n 12
├── model_oracles.py        # new: constants, failure classes, the pure oracles
├── model_sinks.py          # new: the recorder, the four personalities, the call timer, the watchdog
├── model_pins.py           # new: Cell, Pin, PINS, cells(), withheld()
├── test_model_cells.py     # new: the cells, the child runner's use, the reproduction
└── test_model_oracles.py   # new: the oracle and runner corpora, the pin gates, the count literals
docs/
├── architecture.md         # changed: §12 gains four items and one item is corrected in place
├── invariants.md           # changed: the Guarded lines of 3, 5 and 12 name the cells
└── spec-delivery/SPEC-085-adversarial-sink-cells.md   # new, at completion
```

The three `model_*.py` files are helpers, not collected, as `tests/sink_rigs.py` is.

## Implementation Phases

Each phase lands green: a phase that adds cells adds their pins and their §12 items with them.

### Phase 1: The oracles and the child runner (FR-002, FR-003)

- `tests/model_oracles.py` and its corpora: own time, the wait, the three close rules.
- The call timer, the watchdog and the child runner, with the runner's corpus.
- The session's worst-margin line and its `tests/conftest.py` hook.

### Phase 2: The calling personalities and their pins (FR-001, FR-004, FR-005)

- `reentrant` and `factory`, their 616 cells, `tests/model_pins.py` with the five pins those
  cells need, the pin gates and the count literals for the table so far.
- §12: items (a), (b) and (c), and the drain-thread correction.
- FR-005's rows for #341 and #345, and FR-004's `_swap_sink` mutant.

### Phase 3: The waiter, the forker and the reproduction (FR-001, FR-004, FR-005)

- `waiter` and `forker`, their 21 cells, and the `fork-mid-swap` pin.
- The orphan-log reproduction, its pin and §12 item (d).
- The final count literals; FR-005's row for #337; the three recorded mutations.
- `docs/invariants.md`'s `Guarded:` lines; FR-003's owed margin criterion named in the pull
  request body and settled on the green run before the merge.

## Evidence

Measured 2026-11-14 on Python 3.13.1, macOS, 10 cores, against trees exported with `git archive`
and confirmed to be the tree imported. A prototype of this spec produced every number. It is
not in the repository and is not the implementation: each row is anchored to a commit so the
build re-measures it, and FR-004 says what happens when a number has moved.

| What | Measurement |
|---|---|
| The whole table at `a0f44b5f` | 637 cells, 105 red, identical verdicts in 4 runs under `-n 12` |
| The cells that run | 462 cells (442 pass, 20 pinned), identical in 4 runs, 3 of them at a load average of 50; 15 s wall under `-n 12`, against 57 s for all 637 |
| Slowest call that had nothing to wait for | 0.09 s own time across the 442 passing cells at load 50; 0.375 s across the passive model's calls at load 33 to 45; an independent reviewer measured 18 ms over 7,365 calls |
| `drain-flush` | 36 red: at each of three site-paths, 12 of the 14 scripts holding `flush-none` never return, hung in `worker.flush`; the 2 that return follow a nested `shutdown`. A nested `flush(timeout=2.0)` takes 2.00 s and passes. A `configure(sink=…)` from the same place returns in 0.00 s |
| `nested-shutdown` | 56 red: all 14 scripts holding `shutdown-none`, at 4 site-paths, 2.00 s each; a nested `shutdown(timeout=3.0)` takes 2.00 s and passes |
| `empty-submission` | 8 red. `flush` then `shutdown` from the setter during the worker's build: the span is delivered once, yet `queued=1` with an empty list on the retired queue, `submitted_after_shutdown=1` and one "lost 1 submission(s)" line. `shutdown` then `flush`: `queued=2`, `submitted_after_shutdown=2` for one span |
| `orphan-arm-in-close` | 2 red. `configure(sink=B)` then `info()` from inside the close: B has one write and no close when `shutdown()` returns, is in the owed record, and nothing is written to stderr; the exit drain closes it. A `span` from the same place is closed by the same `shutdown()` (closes entered at counts 1 and 3) |
| `installed-closed-twice` | 2 red. The installed sink has one write and closes entered at counts 1 and 1. With `shutdown` alone, no `info` first, it is closed once at count 0 |
| `fork-mid-swap` | 1 red: the child's orphan log lands in the installed sink and its span in the outgoing one. The other 8 `forker` cells pass |
| `orphan-write-after-close` | the reproduction: `writes=2`, one close entered and returned at count 1, nothing owed, `orphan_lost=0`, on 7 of 7 runs and on 3 of 3 by an independent reviewer; the unparked control closes at count 2. Unforced: 3 of 900 runs of the concurrent model with `shutdown()` in its mix |
| Oracle corrections made while authoring | 24 cells of the table, 16 of them among those that run, were red under a released rule asserted before the exit drain (a sink written during its own close, then owed and closed at exit); 1 under repeat-close in a cell with an announced unserialized change; 1 cell's verdict varied between runs until the reaching flush outlasted the script's finite waits |
| #337 at `641dfc86` | the two orphan-path `child-` cells: wait served 4.00 s and 4.01 s past the stop; the other 10 `waiter` cells pass |
| #341 at `b2528383` | 112 of 112 `setter` cells `HangFound`; the innermost frame is `_note_orphan_emit`, `_get_worker`, `_shutdown_worker`, `_flush_live_sink` or `_swap_sink` by the first operation |
| #345 at `144f87c8` | both named cells hang as in FR-005's table; at `a0f44b5f` they return in 0.11 s and 0.00 s |
| The watchdog | a pinned `flush-none` cell is ended with own time 6.01 to 6.05 s, at budget 0 plus `EPSILON` plus `HANG_AFTER`, with every thread's stack in the report |
| The repeat-close exemption | keyed on the library's line: 50 running cells exempt, and the `_swap_sink` mutant leaves the gate at 442 passed, 20 pinned. Keyed on overlapping `configure` calls: 30 exempt, the mutant reddens 8 cells, all `reentrant` `emit`/`worker`, and the unmutated head still reads 442 and 20 |
| The session line | a value recorded with `record_property` in an `-n 2` run is readable from the controller's terminal summary |

## Revision history

- **2026-11-14, replaced after the spec review.** The first draft specified cells and the
  generative model together and failed its review on three points, each found by building: the
  sequential model is red under `reentrant` for defects no cell expresses; the time oracle
  subtracted only the caller's thread and flagged a legitimate wait on a closer thread; and the
  red set was a sample. This spec is the deterministic half, with the complete measured red set.
  The generative half is the arc's next spec.
- **2026-11-15, revised in place after the second review**, which rebuilt the spec and
  reproduced its numbers. The repeat-close exemption is narrowed from the library's line to two
  overlapping sink changes, and a named mutant holds it; the margin line and its owed criterion
  speak of passing cells; the child's timeout, the hooks the next spec drives, the abandoned
  term's observable, a nested child's ending and the verdict keys' numbers are stated; and the
  #341 row asserts the innermost frame per operation.
