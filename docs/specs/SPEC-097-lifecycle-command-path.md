---
type: Spec
id: SPEC-097
title: "Lifecycle Commands Take Their Turn, and a Call Made From Inside One Never Waits on It"
description: "Makes a sink change a command from before its config write, so a shutdown meets it and never has its close undone by it; has a shutdown made from inside a running command merge into it or run when it ends, within the change's budget; refuses a flush made on the drain thread; and has a lifecycle call made on a thread that holds a library lock, a signal handler's, run once that thread lets go, so 94 adversarial cells turn green, no other cell moves, and the signal-handler deadlock is gone."
spec_status: Completed
depends_on: [SPEC-054, SPEC-068, SPEC-077, SPEC-078, SPEC-083, SPEC-085, SPEC-086, SPEC-087, SPEC-088, SPEC-089, SPEC-091]
arc: audit-2026-11-14
arc_order: 13
---

## Overview

The library has four calls that change its lifecycle: `configure(sink=...)`, `flush()`,
`shutdown()` and the exit drain that `atexit` runs. Each was written to be safe against the others,
one interleaving at a time, and the 2026-11-14 audit found what that leaves. A shutdown that lands
while a sink change is half done closes the new sink, and the change then puts it back to be closed
again. A shutdown called from inside a sink's own `close()` waits two seconds for the close it is
inside. A `flush()` called from a sink's `emit` on the drain thread waits on that same thread, for
ever if its timeout is `None`. And a signal handler that calls `shutdown()` while the main thread is
inside one of the library's lock sections deadlocks the process for good: 65 of 600 fresh processes
in the audit's run of its probe. Andrew took the direction on 2026-11-14: lifecycle commands go
through one command path, where a caller from inside a command is queued or refused rather than
waiting on it. This spec builds that path. A sink change becomes a command from the moment before it
writes the config, so a shutdown meets it, and a sink the shutdown closed is not armed again. A
shutdown made from inside a running command, by the sink's own code or by the library's own
threads, merges into a running stop or runs when a running sink change ends, inside that change's
budget. A `flush()` on the drain thread returns at once with a new reason. A lifecycle call on a
thread that holds one of the library's locks, which is what a signal handler or a finalizer
interrupting the library is, runs once that thread has let go. The spec was prototyped before it was
written, against SPEC-085's 637 adversarial cells: 94 cells turn from red to green, three pins are
removed, and no other cell moves.

## Scope

### In Scope

- A sink change is in flight from before its config write; a `shutdown()` or the exit drain meets
  it as it meets a swap today, and a swap whose installed sink a stop's take closed since the config
  named it does not arm that sink again (FR-001).
- A `shutdown()` made from inside a running command never waits on it: it merges into a running
  stop, or runs on the changing thread when a running sink change ends, with every close detached and
  every wait inside that change's swap budget (FR-002).
- A `flush()` made on the live worker's drain thread returns at once, falsy, with the new reason
  `"re-entrant"` (FR-003).
- A lifecycle call made on a thread that holds one of the library's locks, or that is inside its
  own `flush()`'s wait on its marker, never waits on that lock: `shutdown()` and `configure()` are
  held and run at the end of the public call the thread was in, before any stop that thread makes
  later, and `flush()` returns `"re-entrant"` (FR-004).
- The locks become `threading.RLock`, read for ownership. A level call or a traced call made from
  inside a section re-enters it, which every section a workload reaches is proved to survive, and a
  recording lock proves nothing else in the suite re-enters one (FR-005).
- A forked child inherits no command: nothing it calls waits on, or is deferred to, a command its
  parent had in flight (FR-006).
- The record: three pins removed and their items resolved, the decisions that state the old
  behaviour rewritten in place, the invariants page, the README and the pending release notes
  (FR-007).

### Out of Scope

- **Serializing the worker build and the orphan log's arm.** The audit's lifecycle frame listed
  them as commands. Both are on the event path: the first traced call builds the worker and every
  first orphan log arms its sink. A command gate there puts a wait on the caller's thread for a
  lifecycle call on another thread, which invariant 4's outside-a-span half and the ledger's fence
  (*never add a lock to the accept path*) both refuse. They stay as they are.
- **An orphan log that passed its owed check before a `shutdown()` took its sink.** §12 names the
  command path as one way to close it, and it does not: the orphan path is not a command, and
  SPEC-085's reproduction stays red at its pin on the prototype. Re-arming the sink after the
  delivery returns was prototyped and rejected: it turned 34 cells red (`CloseBroken:repeat@1` on
  26, `repeat@2` on 8), because the re-arm cannot tell a write after the close from one before it.
  It needs a mark around the orphan path's delivery that a closer's take consults, which is §12's
  inside-emit item and is not this spec's.
- **A wrapper that leaves its member alone (`Leaves(A)`).** §12 had assigned its closure, the closer
  ordering a member after its holder, to this spec. Andrew placed it in a spec of its own, written
  later in this arc (2026-11-16). `lifecycle/leaves_remaining_holder.py` reads `A.closes=0` on the
  prototype, as at `ead40798`.
- **A forked child rebuilt from its configuration.** `fork-mid-swap` stays at its pin and
  `lifecycle/p6_fork_mid_swap.py` and `p7_fork_inside_close.py` read as at `ead40798`. That is the
  next lifecycle spec of the arc. FR-006 covers only what the command path itself leaves in a
  child.
- **The two pins this path does not touch.** `empty-submission` (an empty batch submitted after a
  `shutdown()`) and `orphan-arm-in-close` (an orphan log made inside a shutdown's close) keep their
  pins and verdicts.
- **The stop signal a sink holds.** Which event a sink is handed, and when, is SPEC-098's. This
  spec hands signals exactly as `ead40798` does.
- **Deleting the mechanisms the audit expected the command path to retire** (`_await_swap_decisions`,
  `_take_declines`, `_shutdown_running`, `_late_worker`, `_sink_lock` with `_sink_holder`). The
  prototype added the path beside them and did not delete them, so their deletion is unmeasured.
  This spec deletes none of them, and FR-007 leaves the deletion open in §12, owned by a lifecycle
  spec of this arc written after the child-rebuild spec. SPEC-098 deletes the hand machinery, which
  is a different set.
- **A finalizer or a signal handler on one of the library's own threads.** Signal handlers run
  only on the main thread. A finalizer that runs on the drain thread or a closer thread while that
  thread holds a library lock is §13's recorded finalizer hazard and stays there.
- **A sink's own transport lock.** FR-004 covers the library's lifecycle locks. A handler that
  interrupts a sink's `emit` while the sink holds its own lock, and then closes that sink, is the
  sink's lock and §12's inside-emit item.

**Built after SPEC-089 and SPEC-091, and measured without them.** Both are Drafts ahead of this spec
in the arc's order, and every measurement below is on `ead40798`, which has neither. SPEC-089
changes what `flush()` returns: a refused call (FR-003, FR-004) is one of its "other outcomes" and
carries the loss counts it reads on the caller. SPEC-091 adds a sweep that takes `_sweep_lock` on the
drain thread, a registry the traced call writes at its open and removes at its close, and changes
`decorator._begin` and `_end`. That moves three things this spec must re-derive on the built tree,
not inherit from this Evidence: the set of public calls that take a library lock (FR-004's sweep),
the set of sections a level or traced call can re-enter (FR-005's sweep, which must reach the
closing span's wait on `_sweep_lock`), and the cells, which SPEC-091 re-pins.

---

## Functional Requirements

**The command path, as built here.** Four calls change the lifecycle: a sink change, `flush()`,
`shutdown()` and the exit drain. A **command** is a sink change (from just before its config write
to its decision) or a stop (`shutdown()` or the exit drain, from its entry to its return). A call
is **made from inside** a command when it runs on the command's own thread, or on one of the
library's `log-foundry-` threads while the command is in flight, since the command may be waiting
on that thread: a drain, a closer, or the close a stop is running inline. A call is made from
**inside a section** when its thread holds one of the library's locks, which is what a signal
handler or a finalizer that interrupted the library is. The rule for every such call is the same:
it never waits on the thing it is inside. A `shutdown()` merges or runs later, a `configure()` runs
later, and a `flush()` returns `"re-entrant"`.

**When both apply, FR-004 governs.** A handler's call can land on a sink change's own thread while
that thread is inside one of the change's lock sections, so it is both inside a command (FR-002) and
inside a section (FR-004). It is held by FR-004 and runs at the end of the `configure()` call, after
the change and after any stop FR-002 deferred during it. FR-004's test is decidable on the calling
thread alone, from locks it owns, and is made before any other: FR-002's test reads the change's
holder, which is safe only once the thread is known to hold no lock that a stop would then take.

### FR-001: A sink change is in flight from before its config write, and never arms a sink a stop closed since

#### Description:

`config._change_sink` writes the new sink to the config (`_rebind`) before `_swap_sink` adds its
operation to `Runtime._swaps_running`. A `shutdown()` landing between the two reads no swap in
flight, takes the new sink an orphan log has just armed, and closes it; the swap then arms it as the
sink it installs, and a later take closes it again with nothing written between. That is §12's item
*A sink a swap installs is closed twice with nothing written between when a concurrent `shutdown()`
took it before the swap decided*, which `lifecycle/p11_swap_arms_a_sink_a_shutdown_closed.py`
forces in 7 of 7 runs at `ead40798`.

A non-re-entrant sink change now publishes its `_Operation` in `_swaps_running` before `_rebind`,
lock-free as the token sets are discarded today, so taking `_state._lock` there adds no acquisition
to the counts `tests/test_runtime_state.py` pins. The same operation is the one `_swap_sink`
decides with; its budget and deadline are set from what the wait for `config._sink_lock` left, when
the swap starts, as today. The operation records the sink it installs, and the change clears that
reference when it ends. A `_close_owed` take that takes the sink a published change installs marks
that change **withheld**. A withheld change does not arm the sink on the no-worker branch, and after
`Worker.retarget` it does not arm it only when the verdict is `declined`. On `fenced` and `unfenced`
the arm stays, because it is the only arm the worker's new sink gets.

Three bounds, each from §12's record of this route. The mark is set only by a `_close_owed` take,
never by a swap's stale take: a withhold keyed on any take left a sink written after its last close
and owed by nobody, measured 3 of 3 by a reviewer at `027b39a5`. The mark is per change, not one
global slot, which an unserialized or re-entrant `configure()` would overwrite. And the mark is the
config write's, not the swap's entry: a mark taken at the swap's entry left p11 at 3 of 3 double
closes.

A `declined` swap that arms anyway is the redundant close SPEC-054 recorded as an accepted trade
(*One lifecycle owner*, `docs/decisions/pipeline/`). The withhold removes it for a declined swap a
stop overtook, which reverses that trade, by Andrew's answer of 2026-11-16.

#### Acceptance Criteria:

- [ ] `p11_swap_arms_a_sink_a_shutdown_closed.py`'s `forced` variant closes S twice at one write
      count in 0 of 7 runs, and S's closes enter at `[1]` in all 7; each control closes S once, at
      `[1]`, in 7 of 7 (invariant 5).
- [ ] A test in `tests/test_shutdown_mid_swap.py` forces the same window on each branch, the
      no-worker swap and a retarget answering `declined`, and asserts the installed sink is closed
      once; on a retarget answering `fenced` it asserts the installed sink is armed and closed once
      after its last write (invariants 5 and 6).
- [ ] A test in which a second, unserialized sink change's stale take closes the first change's
      sink before the first change's retarget asserts that sink's closes enter at `[1, 2]`: a stale
      take does not withhold (invariant 5).
- [ ] `test_a_declined_swap_re_arms_a_sink_already_closed_and_that_is_the_trade` is rewritten to
      assert the installed sink is closed once, with its new name and docstring saying the trade is
      reversed (invariant 5).
- [ ] A test records every operation a `configure(sink=...)` publishes and asserts that, once each
      `configure()` has returned, none references a sink; with the clearing removed it fails (a
      mutant measured on the prototype: 1 passed, then 1 failed). And
      `test_superseded_sinks_never_logged_to_go_weak_and_unclosed`,
      `test_a_dropped_captured_sink_is_collected_and_pruned_at_the_next_configure` and
      `test_five_hundred_reconfigures_leave_a_bounded_record` in `tests/test_ownership_record.py`
      pass: those three are the SPEC-081 tests that fail when a published change keeps its sink
      after the change ends (measured with a mutant that kept every published operation)
      (invariant 5).
- [ ] `tests/test_runtime_state.py`'s pinned lock counts for every `configure-*` row are the counts
      at `ead40798` (invariant 3).

### FR-002: A stop made from inside a running command merges into it, or runs when it ends, inside the change's budget

#### Description:

At `ead40798` a `shutdown()` called from inside a running command takes locks and waits as if it
were alone. From a sink's own `close()` during a stop it waits out the closer grace on the close it
is inside: 2.01 s on every row of `lifecycle/p3_reentry_from_close.py` that calls `shutdown`, with
`timeout=None` too. From a sink's `emit` on the drain thread during a sink change it does not wait
for the change's decision (`_swaps_to_await` skips every `log-foundry-` thread), takes and closes
the sink the change installs, and the change then arms it again: the two cells pinned
`installed-closed-twice`.

Now, at the entry of `_shutdown_worker`, which both `shutdown()` and the exit drain bind, once
FR-004's test has found the thread holding no library lock:

- **Merged.** When a stop is running and the caller is that stop's own thread or a `log-foundry-`
  thread, the call returns at once and does nothing else. The running stop is its effect: it has
  already moved the retirement count, set the stop signal and will close what is owed.
- **Deferred.** When a sink change is registered, under `_deferred_lock`, as the one that will take
  deferred stops, that change is still running, and the caller is that change's thread (so a
  stop-signal setter the change runs) or a `log-foundry-` thread, the call records its own deadline
  on the change's operation and returns at once. A change is running exactly while its
  `config._change_sink` frame is on its thread's stack, which is read, never recorded in a slot a
  later statement must clear: an interrupt can land between any two statements, and three rounds
  of review each found a slot left set that made every later `shutdown()` and the exit drain defer
  to a change that had already unwound. A registered change that is no longer running reads as no
  change, and the deadlines deferred to it are dropped with it: its thread took the interrupt, and
  the next `shutdown()` or the exit drain is the stop that runs. A change registers when it
  publishes, if no running change is registered, so a change made inside another (a re-entrant
  `configure(sink=...)` on the changing thread, or one a `log-foundry-` thread starts) takes
  nothing: a stop made during it defers to the outer change, or with none running runs as today.
  The registered change runs one stop for every call deferred to it, after the change has released
  `config._sink_lock` and before `configure()` returns, and only it takes them, found by its own
  frame in the same section that ends its registration. `_deferred_lock` is one of FR-004's
  oracle locks, so a signal handler's `shutdown()` landing inside one of its sections is held. That
  stop's timeout is what is left of the latest deadline among the deferred calls, or none if any
  was unbounded, cut at the end of the change's swap budget.
- **Otherwise** the call runs as today.

**A deferred stop closes nothing on the changing thread.** A stop's timeout bounds its drain and its
waits, never a `close()`, which takes no timeout (invariant 3's recorded exception), so a deferred
stop that closed a slow sink inline took `configure()` past its budget: 10.01 s with the installed
sink's `close()` blocking 10 s, and 15.02 s with both sinks' blocking, against a 5 s budget
(the spec reviewer's `fr002_bound.py`). Every close the deferred stop takes is detached onto a closer
thread, as a swap already detaches the displaced sink's, and the stop waits for them only within its
closer grace, which is what is left of the change's budget and never more than
`DEFAULT_CLOSER_GRACE`. A close that outlives that wait goes on running on its closer thread. It is
counted in `health().closing_sinks` while it runs, as a displaced sink's is: it read 1 just after
`configure()` returned with the installed sink slow, and 2 with both. The next `shutdown()`, or the
exit drain, waits for it as a bystander within its own closer grace (SPEC-050), and one still running
when the exit drain's grace ends ends with the process, with no line, as a detached close that
outlives the exit does at `ead40798`. So `configure()` keeps invariant 3's bound with no new
exception.

A deferred stop returns before the stop it asked for has happened. A sink's code that calls
`shutdown()` from inside a sink change and then waits for its own backoff to be cut waits until the
change ends; at `ead40798` the same call took and closed the sink under the change. That is a
caller-visible change and FR-007 names it.

#### Acceptance Criteria:

- [ ] Every `shutdown` and `shutdown-none` row of `p3_reentry_from_close.py`, on the orphan path,
      the worker path and with two sinks, reads `outer=0.00s inner=0.0s` with `S.closes=1` and
      `state=CLOSED` (it reads 2.01 s at `ead40798`) (invariant 3).
- [ ] The four cells pinned `nested-shutdown` and the 52 cells that pin withholds pass (invariant 3).
- [ ] The two cells pinned `installed-closed-twice`, `reentrant-swap-emit-worker-info+shutdown` and
      `...+shutdown-none`, pass, each sink closed once at write count 1 (invariant 5).
- [ ] `test_a_setter_shutdown_after_the_reassign_is_declined_not_unfenced` is rewritten to the
      measured outcome `fired=True incomplete=0 line=False a_owed=False p_closes=1`, and
      `test_a_setter_shutdown_that_expires_before_the_drain_leaves_stays_unfenced` to
      `fired=True drain_ended=False incomplete=0 line=False a_owed=False a_closes=1`: the swap
      completes and the deferred stop closes the displaced sink (invariant 5).
- [ ] A test ported from the spec reviewer's `fr002_bound.py` runs its three rows, the displaced
      sink's `close()` blocking 10 s, the installed sink's, and both, each with a sink whose `emit`
      calls `shutdown(timeout=None)` during a `configure(sink=...)` with a 5 s budget, and asserts
      for each that `configure()` returns within 6.0 s with under 1 s of CPU, that each sink's
      `close()` was entered once by the time the exit drain returns, and that
      `health().closing_sinks` read just after `configure()` returns counts each close still
      running (invariants 3 and 5).
- [ ] A test asserts that a merged call moves no counter: `retirements` and every ledger term read
      the same before and after it (invariant 2).
- [ ] `tests/test_change_interrupts.py` raises `KeyboardInterrupt` at every line event, one per
      child, of the eight functions a sink change and its deferral run, the rows derived from the
      code at run time and the functions each mode reaches asserted, for a `configure(sink=...)`
      on the main thread whose stop-signal setter defers a `shutdown()` and for the same change
      made on the drain thread. After each, the interrupted thread owns no library lock,
      `_sink_lock` is free and `_sink_holder` clear, no deferral is left on a running change, a
      later unrelated `configure(sink=...)` retires nothing, a later `shutdown()` retires, and
      every sink given events is closed once by the exit. Four categories are excused, each by a
      reason derived from the code and asserted, never by a count: a lock left owned on one of
      its function's `with` lines, whose `__exit__` the interrupt skipped (the same rows hang the
      child at `c9068a46`); a lock left owned on a line holding an instruction, inside a `with`,
      that no exception-table entry covers; an interrupt between `_swap_sink`'s `took` of a
      displaced sink and the `release` that starts its closer, code unchanged from `c9068a46`;
      and an interrupt on the statements that release `_sink_lock` or clear `_sink_holder`
      themselves. The branches no row reaches are pinned by named tests the corpus's docstring
      lists, and a `configure(sink=...)` leaves no reference cycle through its own frame
      (invariants 3 and 5).

### FR-003: A `flush()` made on the drain thread returns at once, with the reason `"re-entrant"`

#### Description:

A `flush()` asks the drain thread to answer a marker. Made on the drain thread, from a sink's
`emit` on the worker path or a `sink_factory` the worker built, it waits on itself: 2.00 s for
`timeout=2.0`, for ever for `None` (§12, *`flush()` called on the drain thread waits on itself*).
That item's closure is a decided answer, which needs a new `FlushResult.reason`. Invariant 10
allows one: results grow by new reason values. Andrew chose this answer on 2026-11-16.

At the entry of `_flush_worker`, when the calling thread is the live worker's drain thread, the call
returns `FlushResult(ok=False, reason="re-entrant")` before it sweeps a span, queues a marker, hands
a signal or flushes a sink's client. The thread is compared by identity with the live worker's
`_thread`, not by its name, so a forked child's forking thread, which ran on the parent's drain
thread and has the name, is not refused: the prototype refused it by name and two forker cells read
`ChildBroken:state` until the comparison changed. A refused call is one of SPEC-089's "other
outcomes" and carries the counts SPEC-089 reads on the caller.

#### Acceptance Criteria:

- [ ] The three cells pinned `drain-flush` and the 33 cells that pin withholds pass (invariant 3).
- [ ] A test calls `flush(timeout=None)` from a sink's `emit` on the worker path and asserts it
      returns in under 0.5 s with `ok` false and `reason == "re-entrant"`, that no submission and no
      marker was queued, and that every ledger term reads as before the call (invariants 2 and 3).
      (Build, 2026-11-18: this read under 0.1 s. `tests/test_timing_floors.py` refuses any ceiling
      under its 0.5 s stall allowance, which a CI stall can reach; 0.5 s still tells the refusal
      from the call it replaces, which waits for ever.)
- [ ] Every `forker` cell passes except `forker-swap-emit-worker-fork`, which stays at its pin
      (invariant 12).
- [ ] `FlushResult`'s docstring and `flush()`'s list `"re-entrant"` among the reasons, saying when
      it occurs, and `tests/test_public_surface.py` passes (invariant 10).

### FR-004: A lifecycle call on a thread that holds a library lock is held, and runs when that thread lets go

#### Description:

A signal handler runs on the main thread between two bytecodes, and a finalizer runs wherever the
collector does. When either interrupts the library inside a lock section and calls `shutdown()`,
the call waits for a lock its own thread holds. `lifecycle/p1b_run.py`, with a one-shot timer at a
random instant, hung 5, 29 and 32 of 200 fresh processes in its three modes at `ead40798`, and 6,
17 and 28 in a second run. The audit found the handler blocked on `Runtime._lock`,
`_CloseRegistry._lock`, `Worker._lock` and the worker queue's mutex; the prototype found one more,
the condition lock of the event a `flush()` waits on for its marker, which the handler's stop sets.

The locks a lifecycle call takes on its own thread become `threading.RLock` (FR-005 says why that is
safe): `Runtime._lock`, `_CloseRegistry._lock`, `Worker._lock`, the worker queue's `mutex` (with its
three conditions rebuilt on it), `config._config_lock`, `decorator._sweep_lock` and
`decorator._loss_lock`. (Build, on `c9068a46`: `_sweep_lock` was already an `RLock`, SPEC-091's,
so six are converted. The ledger's registration lock and `Runtime._owned_lock`, also taken by
lifecycle calls, are `RLock`s re-entrant by design, SPEC-089 FR-003 AC-15 and SPEC-081 FR-003, and
stay out of the oracle; `tests/test_lock_oracle.py` records each lock outside it with its reason.) A lifecycle call reads them as an **ownership oracle**, `_is_owned()`, and
never re-enters one. A `shutdown()`, a `configure()` (its config write and its sink change) or a
`flush()` made on a thread that owns any of them, or that is inside its own `flush()`'s wait on its
marker, takes no lock. A `flush()` returns `FlushResult(ok=False, reason="re-entrant")`. A
`shutdown()` or `configure()` is **held**: it is recorded on its thread and returns at once.

**Where a held call runs.** At the end of the public call the thread was in, once the thread owns
no library lock. Every public call that can take a library lock runs its thread's held calls in a
`finally`, and the set is derived, not listed: `spec097_held_sweep.py` runs every callable in
`log_foundry.__all__` (a callable with no recipe there fails the sweep), raises SIGTERM at the first
library line it runs holding a library lock, and has the handler call `shutdown(timeout=2)`. On
`ead40798`'s surface the calls that take a lock are `configure()`, `flush()`, `health()`,
`shutdown()`, a traced call and a level call whose sink is not yet owed; in the first prototype
`configure()` and `health()` did not run held calls, and a handler's stop that landed in either was
held until the thread's next library call, so with no further call it never ran, where at
`ead40798` the `configure()` row closed. A held call runs with what is left of its own timeout, and
its time is the handler's: a handler's call has always run inside the interrupted call's time. The
mark that makes a public call run held calls, `runs_held`, is typed with a `ParamSpec`: written
untyped, it erased `configure()`'s signature, and `tests/test_typed_consumer.py`'s
`test_the_probe_can_fail` failed because three planted bad calls to `configure()` were no longer
reported.

**The order of a held call and a stop.** A stop runs every call held on its thread before it begins,
so a held `configure(sink=X)` runs while the worker is still live and is never left after the drain
it would race: the reviewer's `sig_exit.py`, which holds `configure(sink=other)` during `health()`
and then exits, left the config naming an unclosed `other` with the state `CLOSED` on the first
prototype, and reads `config_is_other=True other_closes=1 state=CLOSED` now. The calls held during
a stop run at the end of the first public call its thread finishes: normally the stop itself, so
once it is over (at the end of `shutdown()`, and of the exit drain, which binds the stop
directly); but a sink's own code the stop runs on that thread, an inline `close()` that logs, say,
makes a public call that runs them while the stop is still in progress (the frame-1 diff
reviewer's `probe_midstop.py log-in-close`). Every public call that can take a library lock runs
them at its end, whatever its own path, and also once a traced call's open returns, before the
function body, so a held stop does not wait out the body. (Build, 2026-11-18: this paragraph said a stop ran the calls held during it "before
its last close pass, then makes that pass". That pass was built and removed: it is unreachable,
since the only held call that could arm a sink, a held `configure(sink=...)`, arms nothing when it
runs after a stop, finding no live worker and nothing owed, and its mutant survived the whole
suite.)

**Only the flush's own wait is marked.** The first prototype marked the whole of `flush()`, so a
sink's own `flush()` hook, which the library calls on the caller's thread holding no lock, saw its
lifecycle calls refused or held: the reviewer's `inflush.py` read `re-entrant` for an inner
`flush()` and a `RUNNING` state after an inner `shutdown()`. That was not intended. The mark now
covers only `Worker.flush`'s wait on its marker, and all six of `inflush.py`'s rows read as at
`ead40798`.

**No Python frame is added between an acquire and the code it guards, or between that code and the
release.** The prototype first counted sections with a lock wrapper written in Python. It closed the
hangs, but under `core-pipeline/p11_assembly.py`'s recursion faults it left 8 events unaccounted,
where `ead40798` and the shipped design leave 0: a `RecursionError` raised inside the wrapper skipped
a counting section, and one raised inside its release would leave the lock held for good. The C
`RLock` adds no frame.

A handler that calls `shutdown()` and then `os._exit()` exits before the stop runs, so nothing is
drained; at `ead40798` the same handler hung for good whenever the signal landed in a lock section.
A handler that raises `SystemExit` instead (`sys.exit`) unwinds the interrupted call, whose end runs
the stop, and the exit drain runs after. FR-007 puts that in the README.

#### Acceptance Criteria:

- [ ] `p1b_run.py <p1b_child.py> 200` reports 0 of 200 hung in each of its three modes, in each of
      three runs. Its child calls `os._exit` right after the handler's `shutdown()`, so this
      criterion proves only that no handler hangs; the next criterion carries that a held stop runs
      (invariants 1 and 3).
- [ ] `docs/audits/2026-11-14-probes/core-pipeline/spec097_signal_in_section.py` exits 0 on all
      eleven rows: the handler's call returns in 0.0 s, and once the interrupted call has returned,
      read with no further library call, a held `shutdown()` has retired the process and closed the
      sink once (rows `flush-section`, `orphan-arm`, `submit`, `rebind`, `swap` and `health`), a
      `flush()` returned falsy, a `configure()` installed its sink, and `unaccounted=0` in every row
      (invariants 1, 2, 3 and 5).
- [ ] `spec097_held_sweep.py` prints no `NO RECIPE` row, and every row whose call fired reads
      `retired=True held=0` (invariants 1 and 3).
- [ ] A test ported from the reviewer's `sig_exit.py` holds `configure(sink=other)` during `health()`
      and exits, and asserts that at the end of the exit the config names `other`, `other` was
      closed once and the state is `CLOSED`; a second row holds it during a stop and asserts that
      the config names `other`, the state is `CLOSED` and `other` was closed **0** times (invariant
      5). (Build, Andrew's decision of 2026-11-18: this row read "the same", closed once. A held
      change that runs after a stop finds no live worker and nothing owed and arms nothing, the
      no-worker arming rule; at `9ee07a88` a plain `configure(sink=other)` after `shutdown()`, with
      no writes, leaves `other` unclosed at exit too, measured `other_closes=0 other_writes=0`, so
      the held change matches existing behaviour.)
- [ ] The six rows of the reviewer's `inflush.py` (orphan and worker, an inner `flush`, `shutdown`
      and `configure`) read as at `ead40798`: the inner `flush()` truthy, the inner `shutdown()`
      leaving `CLOSED`, the inner `configure()` installing its sink (invariant 6).
- [ ] `p11_assembly.py` prints `unaccounted=0` (invariant 2).
- [ ] A test derived from the AST of `src/log_foundry/` fails naming a lock that a lifecycle call
      takes on its own thread and that is neither an `RLock` in the oracle's list nor recorded with
      its reason; a fixture adding a plain `threading.Lock()` to `Runtime.__init__` makes it fail
      for that reason (invariant 1).
- [ ] `tests/test_runtime_state.py`'s pinned lock counts, with its counting double answering
      `_is_owned`, equal the counts at `ead40798` (invariant 3).
- [ ] `tests/test_typed_consumer.py` passes with every public call that runs held calls so marked
      (invariant 10).

### FR-005: A level or traced call from inside a section re-enters it, safely where measured, and nothing else does

#### Description:

FR-004's `RLock`s let a handler's level call or traced call re-enter a lock its interrupted thread
holds, midway through that section. *A stop-signal setter runs under no lock its own calls would wait
on* (`docs/decisions/sink-lifecycle/`) rejected an `RLock` on exactly that ground, for a setter's
call, naming `_state._lock` and `_sweep_lock`. A setter still never runs under a library lock, so
that ground still holds for setters; what the `RLock` admits is a handler's or finalizer's call. The
alternative is to hold level and traced calls as FR-004 holds lifecycle calls, at a cost on every
log and every traced call of reading eight locks' ownership. This spec re-enters, on the evidence of
`spec097_reentry_sweep.py`, and holds level calls instead if that evidence fails on the built tree.

The sweep derives its sections. It reads every `with` on one of the lifecycle locks from the AST of
`src/log_foundry` (57 on the prototype), runs a workload on the main thread under
`sys.settrace`, and for each section the workload enters, and each line in `queue.py` or
`threading.py` entered holding a library lock, runs a child that raises SIGTERM there. The handler
makes an orphan log, a traced call that logs once, and a second orphan log. The workload makes every
public lifecycle call, a sink change, a sink that raises, and a traced call and a log after the
shutdown. A child passes when the ledger balances, each sink is closed as often as in a control
child that fires nothing, no span is current, and the handler's three calls were made. On the
prototype it reached 28 of the AST's sections and 2 lines of `queue.py`, and all 30 passed. It did
not reach 29: 3 that run only in a forked child's repair and 7 that run only on the drain thread
(`Worker._emit` three times, `_take_marker`, `_release_marker`, `_nothing_lost_since` and
`_terminal_failure`), where no signal handler runs; 8 in the hand machinery (`_settle_signal`,
`_hand_after_close`, `_hand_retargeted`, `_pay_marks_after_stop`, `defer_hand`, `unmark`,
`rearm_if_released` and `offer_stop_signal`'s child branch), which SPEC-098 deletes but which runs on
the main thread until it does; and 11 on main-thread paths the workload does not take
(`worker.submit`'s full-queue branch, `worker.stop`'s expiry, `_record_incomplete_swap`, the two
late-worker sections of a stop, `_close_owed`'s put-back, `_ensure_sink`'s default build,
`_abandon`, `_note_in_span_loss`, `_read_losses` and the console's lock). The build extends the
workload to reach those nineteen, or holds level calls.

**Nothing else re-enters a library lock.** A plain lock deadlocks when library code takes it twice
by mistake; an `RLock` lets that through silently. So the whole suite runs under a re-entry
recording lock, the spec reviewer's `_reentry.py`, installed in place of each of the oracle's locks:
it records the call site whenever a thread that owns the lock acquires it again. At the prototype
the suite records 0 re-entries, and `spec097_signal_in_section.py` records exactly the 2 its design
makes, a handler's orphan log re-entering `_state._lock` in `_arm_and_hand` and a handler's traced
call re-entering the queue's mutex in `Worker._enqueue`, where the submit's `put` runs (build,
2026-11-18: this read `Worker.submit`; the recorder names the frame that acquires). The reviewer's
planted mutant, `with
self._lock, self._lock:` in the worker's health snapshot, deadlocks `tests/test_worker.py` at
`ead40798`; on the prototype that suite passes, and the recorder catches it.

#### Acceptance Criteria:

- [ ] `spec097_reentry_sweep.py` reads `failed=0`, and every AST section it reports not reached is
      one that runs only on the drain thread or in a forked child's repair; if any section fails, or a main-thread section stays unreached, level and traced calls made
      inside a section are held as FR-004 holds lifecycle calls, and the sweep is run again on that
      (invariants 2, 5 and 9).
- [ ] The suite, run with every one of the oracle's locks replaced by the re-entry recording lock,
      records no re-entry outside the tests that port `spec097_signal_in_section.py`, which record
      exactly their two by design (invariant 1).
- [ ] With `with self._lock, self._lock:` planted in `Worker.health`, that recording run of
      `tests/test_runtime_state.py` and `tests/test_worker.py` records the re-entry, naming
      `worker.py` and `health` (359 records on the prototype), and the criterion above fails
      (invariant 1).

### FR-006: A forked child inherits no command

#### Description:

`_clear_after_fork` already clears the swap and stop tokens, on the premise that no call survived
the fork. The command path adds state that would outlive it. The deferred stops would wait for a
sink change that does not exist in the child, and the calls FR-004 holds belong to the parent's
interrupted call. And `config._sink_holder`, the thread holding the sink-change lock, is inherited as
the parent's: in the prototype a forking closer or drain thread's `shutdown()` in the child read the
holder as a change in flight and deferred itself to it, and the child ended `RUNNING` with its span
undelivered (two forker cells, `ChildBroken:state`). The audit's lifecycle frame had raised the
inherited holder as a hypothesis (H1, `lifecycle/p2_stale_sink_holder_after_fork.py`).

In the child, `_clear_after_fork` clears the deferred stops and sets `config._sink_holder` to `None`,
including when the forking thread was the holder. A child whose forking thread was inside a sink
change then runs its own `shutdown()` at once, as it did at `ead40798`; deferring it there would
wait on a change whose other half, the parent's drain and closers, is not in the child.

#### Acceptance Criteria:

- [ ] `tests/test_fork_mid_hand.py::test_a_childs_shutdown_cuts_short_a_wait_on_the_offered_event`
      reads `own=False ok=True cut=True second=ok` on all four rows, `retarget` included (invariants
      3 and 12).
- [ ] `forker-swap-close-orphan-fork` and `forker-swap-close-worker-fork` pass (invariant 12).
- [ ] A test forks from the displaced sink's `close()` on the swap's closer thread while the sink
      change still holds `config._sink_lock`, and asserts that, in the child, `config._sink_holder`
      is `None`, no stop is deferred, and a `shutdown()` leaves the child `CLOSED` with its span
      delivered (invariant 12).

### FR-007: The record says what changed, rewritten in place and dated

#### Description:

The pins, the open items, the decisions, the invariants page, the README and the pending release
notes each state the behaviour this spec changes. The decisions are found by a search, run at build
time over what the build changes, not by a list written here:

```
grep -l '_swaps_running\|_swaps_to_await\|_await_swap_decisions\|_sink_holder\|signal handler\|declined swap\|re-entrant\|RLock' docs/decisions/*/*.md
```

At `ead40798` it returns thirteen files, and fourteen on `c9068a46`, where SPEC-091 added *An
open span's events ship on the worker's interval*, whose line on a `flush()` inside a sweep hold
the build rewrites in place (Andrew, 2026-11-18, "re-entrant"). Five state a behaviour this spec
changes and are rewritten in place, each with a dated History line: *A shutdown waits for a swap in
flight to decide, and a
swap releases only what the record owes* (a re-entrant or signal-handler stop is merged, deferred or
held; a stop now meets a change from its config write; the swap withholds), *One lifecycle owner*
(the declined-swap trade, reversed), *Concurrent sink changes are serialized by one lock whose wait
is spent from the swap budget* (the change is published before its write and runs the stops deferred
inside it, with every close detached), *Lifecycle state has one holder, Runtime* (the token set for
swaps is published lock-free before the write, and its locks are `RLock`s read for ownership), and
*A stop-signal setter runs under no lock its own calls would wait on* (its *Rejected: an `RLock`*
paragraph, at lines 118 to 120, is narrowed: a setter still runs under no lock, and the `RLock` that
FR-005 admits is re-entered only by a handler's or finalizer's level or traced call, on FR-005's
evidence). The build reads the others and states for each why it is untouched.

The caller-visible changes, for `docs/release-notes/v1.2.1.md`:

- `flush()` made on the drain thread, or from a signal handler or finalizer that interrupted the
  library, returns at once with `ok` false and the new reason `"re-entrant"`, where it waited its
  timeout or for ever.
- `shutdown()` made from a sink's own code during a sink change returns at once, and the stop runs
  when `configure()` finishes, before it returns, within its swap budget; a close that stop starts
  and that outlives the budget goes on in the background, counted in `health().closing_sinks`.
- `shutdown()` made from a sink's own code during a stop, its `close()` included, returns at once,
  where it waited the 2 s closer grace.
- `shutdown()` and `configure()` made from a signal handler or finalizer that interrupted the
  library return at once and run when the interrupted call returns, and before any later stop; a
  handler that then calls `os._exit()` drains nothing.
- A sink a `configure()` installs is no longer closed twice when a concurrent or re-entrant
  `shutdown()` closed it before the change decided.
- A sink's own `flush()` hook that calls `flush()`, `shutdown()` or `configure()` behaves as it does
  at `ead40798`. This is listed because the first prototype changed it; the spec does not.

#### Acceptance Criteria:

- [ ] `tests/model_pins.py` holds no `drain-flush`, `nested-shutdown` or `installed-closed-twice`
      pin; the cells those pins withheld are collected and pass; the literals counting pinned and
      scheduled cells equal the derived counts; and `test_the_session_prints_the_worst_margin_line`
      names a cell still pinned (serves no invariant: the pins are the harness's record).
- [ ] `architecture.md` §12 moves to *Resolved*, naming this spec, the two installed-closed-twice
      items, *`shutdown()` called from a signal handler deadlocks*, *`shutdown()` called from a
      sink's own `close()`* and *`flush()` called on the drain thread*. It keeps the orphan-write
      item open, saying why this spec does not close it; it keeps *Leaves(A)* open, saying it is a
      spec of its own later in this arc; and it gains an open item, *The command path's deletion
      half*, naming the mechanisms this spec left in place and the arc's lifecycle spec after the
      child-rebuild spec as its owner (serves no invariant).
- [ ] The five decisions named above are rewritten in place with a History line dated the day the
      build lands, and the delivery doc lists the search's other files with one line each (serves
      no invariant).
- [ ] On `docs/invariants.md`, entry 5's recorded exception (lines 350 to 355 at `ead40798`, 368
      to 372 at `c9068a46`), that a re-entrant `shutdown()` "from a sink's own code on the drain
      thread or the changing thread, or a signal handler" landing between a change's retarget and
      its decision may leave the installed sink open, is struck in place with this spec's number.
      Entry 3's *Guarded* paragraph (lines 286 to 290 at `ead40798`, 307 to 308 at `c9068a46`),
      that `tests/test_model_cells.py` holds every call a sink makes to its budget plus one
      second "except in the cells pinned", is left in force, and the delivery doc records that the
      three pins it excused here are gone. Entry 3's *Guarded* also names the ports of
      `spec097_signal_in_section.py` and `spec097_held_sweep.py` (serves no invariant).
- [ ] The README's `shutdown()` and `flush()` sections state the deferral, the hold, the new reason
      and the `os._exit()` consequence, and `docs/release-notes/v1.2.1.md` carries the six entries
      above (serves no invariant).

---

## Data Model

```python
# src/log_foundry/_lifecycle.py
class _Operation:                       # a SWAP gains four fields, a STOP one
    installing: Sink | None             # the sink a published change installs; None once it ends
    withheld: bool                      # a _close_owed take took `installing` since the config named it
    detach: bool                        # a STOP deferred to a change's end: every close on a closer thread
    change: FrameType | None            # the _change_sink frame that published a SWAP; running while on its stack
    deferred: list[float | None]        # deadlines of stops deferred to this SWAP while it is the taker

_deferred_taker: _Operation | None      # the change that takes deferred stops; counts only while running
_sections: threading.local              # per thread: .waiting (calls FR-004 holds), .flushing (the marker wait)

def runs_held(fn): ...                  # marks a public call that runs its thread's held calls at its end

# Runtime._lock, _CloseRegistry._lock: threading.RLock (ownership oracle; never re-entered by a
# lifecycle call)

# src/log_foundry/worker.py
Worker._lock: threading.RLock
_CountingQueue.mutex: threading.RLock   # not_empty, not_full, all_tasks_done rebuilt on it

# src/log_foundry/config.py
_config_lock: threading.RLock           # configure() is @runs_held

# src/log_foundry/__init__.py
health()                                # @runs_held

# src/log_foundry/decorator.py
_sweep_lock, _loss_lock: threading.RLock

# src/log_foundry/results.py
FlushResult.reason                      # gains "re-entrant"
```

## API / Interface Contract

No public signature changes. `FlushResult.reason` gains one value:

```python
result = log_foundry.flush(timeout=None)    # from a sink's emit on the drain thread
assert not result and result.reason == "re-entrant"
```

## File & Folder Structure

```
src/log_foundry/
├── _lifecycle.py      # the published change, the withhold, merge and defer, detached closes, the
│                      # oracle, the hold and its order, the child reset
├── config.py          # publishes the change before _rebind; runs the deferred stops; holds in a section
├── __init__.py        # health() runs held calls
├── worker.py          # Worker._lock and the queue's mutex as RLock
├── decorator.py       # the two locks as RLock; _begin and _end run held calls
├── api.py             # _log runs held calls
└── results.py         # the "re-entrant" reason in the docstring
tests/
├── model_pins.py                      # three pins removed
├── test_shutdown_mid_swap.py          # FR-001, FR-002
├── test_deferred_stop_bound.py        # FR-002 (new), the port of fr002_bound.py
├── test_reentrant_flush.py            # FR-003 (new)
├── test_signal_handler_calls.py       # FR-004 (new), the ports of the signal probes
├── test_lock_reentry.py               # FR-005 (new), the recording run and its mutant
└── test_fork_mid_hand.py              # FR-006
```

## Implementation Phases

### Phase 1: The published change and the withhold (FR-001)

- Publish the change before `_rebind`, set its budget when the swap starts, record `installing`.
- Mark withheld in `_close_owed`'s take; skip the arm on the no-worker branch and for `declined`.
- The p11 tests, the rewritten trade test, the published-change test and its mutant.

### Phase 2: Merge, defer and refuse (FR-002, FR-003, FR-006)

- The merge and the deferral at `_shutdown_worker`'s entry; the deferred stop at the end of
  `_change_sink`, its closes detached and its grace inside the budget.
- The drain-thread refusal in `_flush_worker`, by identity.
- The child reset in `_clear_after_fork`.

### Phase 3: The lock oracle and the hold (FR-004, FR-005)

- The seven locks as `RLock`, the ownership check, the per-thread held list, `runs_held` on every
  public call the held sweep finds taking a lock, the order of held calls around a stop, the mark on
  the flush's marker wait only; the fork roster and the inventories updated for the new types.
- The AST-derived lock roster test and its fixture; the held sweep and the re-entry sweep, re-run on
  the tree with SPEC-091 built; the recording run and its mutant.

### Phase 4: The record (FR-007)

- Pins, §12, the five decisions, the invariants page, the README, the release notes.
- (Build: each pin was removed in the phase whose fix turned its cells green, since
  `model_pins.judge` fails a pinned cell that passes: `installed-closed-twice` in Phase 1, which
  FR-001's withhold already closed, and `drain-flush` and `nested-shutdown` in Phase 2.)

---

## Evidence

Measured against `ead40798` (the base) and the prototype, which is
`docs/audits/2026-11-14-probes/core-pipeline/spec097_prototype.patch`, applied with `patch -p1` to
`git archive ead40798 src tests docs/audits/2026-11-14-probes`. The patch carries this spec's part
and SPEC-098's together; the per-spec rows below were measured on each part applied alone. Python
3.13.1 on macOS, on a machine other sessions loaded (load average 24 to 35). The spec reviewer's
probes named here are in the scratch tree of that review and are not committed; their ports are this
spec's tests.

**The cells.** Every one of SPEC-085's 637 cells and its reproduction was run with pins and
withholding ignored, by a scratch module that calls `test_model_cells.run_cell` for each and records
its verdict; the comparison is cell by cell.

| Tree | Red | Of which |
|---|---|---|
| base | 106 | 20 pinned cells at their pins, 85 withheld cells (33 `HangFound:flush-none@worker.flush`, 52 `Stalled:shutdown-none`), the reproduction |
| this spec's part alone | 12 | `empty-submission` 8, `orphan-arm-in-close` 2, `fork-mid-swap` 1, the reproduction, each at its pin's verdict |
| SPEC-098's part alone | 106 | the base's, verdict for verdict |
| both | 12 | as this spec's part alone, verdict for verdict |

94 cells move, every one from red to green: the 3 `drain-flush` cells and 33 they withheld (FR-003),
the 4 `nested-shutdown` cells and 52 they withheld (FR-002), and the 2 `installed-closed-twice` cells
(FR-002). No cell moves from green to red, and no pinned cell moves to a different verdict. Run
twice before the spec review and again on the revised prototype, this spec's part alone and both
parts, with the same verdicts cell for cell.

**The §12 items that name the command path or the stable signal as their closure**, each with its
own reproduction on the prototype:

| Item | Here | Its reproduction on the prototype |
|---|---|---|
| *A sink the library wrote to and never closes, behind a wrapper that leaves its member alone* | open: its own spec, later in this arc (Andrew, 2026-11-16) | `leaves_remaining_holder.py`: `A.closes=0 ... B.closes=1`, as at base |
| *A kept sink a straddling orphan log put back in the record can be handed a stop signal on its own* | closed by SPEC-098 | read, never reproduced; SPEC-098's `sequence` row hands a sink one event, once |
| *A sink a swap installs is closed twice ... re-entrant `shutdown()`* | closed (FR-002) | its two cells pass |
| *A sink a swap installs is closed twice ... concurrent `shutdown()`* | closed (FR-001) | `p11`: `forced` 0 of 7 (base 7 of 7); both controls 0 of 7 |
| *An orphan log that passed its owed check before a `shutdown()` took its sink writes after the close* | open (Out of Scope) | the reproduction stays `CloseBroken:unreleased@1` |
| *`shutdown()` called from a signal handler deadlocks for good* | closed (FR-004) | `p1b`: 0 of 200 in every mode, three runs (base 5/29/32 and 6/17/28); `p1a` does not run on the prototype, since it reads `_state._lock.locked()`, which a Python 3.13 `RLock` lacks: `spec097_signal_in_section.py` is its port |
| *`shutdown()` called from a sink's own `close()` waits out the closer grace* | closed (FR-002) | `p3`: 18 of 18 rows `outer=0.00s` (base 2.01 s on its four `shutdown` rows) |
| *One stable stop signal per sink changes a documented public protocol* | closed by SPEC-098 | SPEC-098's Evidence |

Two items name neither mechanism and move too: *`flush()` called on the drain thread* (closed,
FR-003) and *A second `shutdown()` made from a stop-signal setter during a forked child's fork
repair* (closed by SPEC-098).

**FR-002's bound**, the reviewer's `fr002_bound.py`, a 5 s budget, a `close()` blocking 10 s:

| Slow `close()` | base | first prototype | revised prototype |
|---|---|---|---|
| displaced sink | `configure` 0.00 s | 5.01 s | 5.01 s |
| installed sink | 0.00 s | 10.01 s | 2.01 s, `closing_sinks=1` |
| both | 0.00 s | 15.02 s | 5.01 s, `closing_sinks=2` |

At base the sink's `shutdown()` runs inline under the change and the slow closes fall to the exit;
on the revised prototype every row enters each sink's `close()` once by the exit, and the process
exits 2 s after `configure()` returns, the exit drain's grace on the close still running.

**`spec097_signal_in_section.py`**, eleven rows, SIGTERM raised at the first line run while the main
thread holds the row's lock, read with no further library call:

| Rows | base | revised prototype |
|---|---|---|
| `flush-section`, `orphan-arm`, `submit` with `shutdown`; `flush-section` with `flush` and `configure`; `orphan-arm` with `info`; `submit` with `span`; `swap/shutdown`; `health/shutdown` | 9 rows hang to the 6 s watchdog | all return; every held `shutdown()` reads `retired=True sink_closes=1` |
| `flush-section/info` | returns (an `info()` whose sink is already owed takes no lock) | returns |
| `rebind/shutdown` | returns, `retired=True sink_closes=1` | returns, `retired=True sink_closes=1` |

Every row on the prototype reads `unaccounted=0`. With SPEC-098's deletion, the orphan arm's
section moved into `_note_orphan_emit`, where the probe's two `orphan-arm` rows now fire, and all
eleven rows read the same. On the first prototype the reviewer's
`sig_configure.py` read `retirements=0 state=RUNNING waiting=1` on its `rebind` and `swap` rows, the
held stop never run; the revised prototype reads `retirements=1 state=CLOSED waiting=0` on all three
of its rows.

**`spec097_held_sweep.py`**, 24 rows over `log_foundry.__all__` and two variants: on the first
prototype `configure`, `configure-sink` and `health` read `held=1`; on the revised prototype every
fired row reads `retired=True held=0`, and the calls that fire are `configure`, `configure-sink`,
`flush`, `health`, `shutdown`, `trace` and `info-first`.

**`spec097_reentry_sweep.py`:** a control child, then 30 sections reached (28 of the AST's 57 and 2
lines of `queue.py`), 30 passed, 29 not reached (FR-005 lists them).

**The re-entry recorder**, the reviewer's `_reentry.py` installed in place of the seven locks in this
spec's part: 0 records over the whole suite (11,468 passed and 57 failed under the recording locks);
2 over `spec097_signal_in_section.py`, the two by design. With the planted mutant, `with self._lock,
self._lock:` in `Worker.health`, `tests/test_runtime_state.py` and `tests/test_worker.py` record 359
re-entries, every one naming `worker.py` and `health`; the same mutant at `ead40798` hangs those two
modules until a 600 s timeout ends the run.

**The suite.** Base: 11,511 passed, none failed. This spec's part alone, before the review: 41
failed, sorted by their first assertion line. 11 are the nine pinned cells and the two margin-line
tests, which FR-007 rewrites. 14 are test doubles that stand in for a plain lock or a worker
(`_CountingLock`, `TrackingLock`, `SpyWorker`) and lack `_is_owned`, ten of them
`test_runtime_state.py`'s pinned lock counts. 11 are rosters and inventories reading the prototype's
new state, its new lock types or its renamed `_log`, `_begin` and `_end`. 5 are this spec's changed
outcomes: the trade test, the two setter-shutdown tests, and the two decline tests that count the
change published before its write. The revised prototype reads the same: 41 failed, the same 41
(an untyped `runs_held` had added a 42nd, `test_the_probe_can_fail`), and both parts together 141.
The passive model gate (`test_invariants_model.py`, SPEC-086) passes.

**The soak**, `LOG_FOUNDRY_MODEL_SOAK=1 LOG_FOUNDRY_MODEL_SEEDS=0-999`, 3,009 items, unforced, which
gates nothing. Base: red on 18 items in each of two runs, the second's 17 `late-write` and one
`repeat@2`. The prototype before the review: red on 2 and 7, every one `late-write`, which is the
orphan-write item this spec leaves open; an earlier build read 14 and 3. A soak is a finding
generator, and its counts move from run to run.

**Costs.** Isolated, by `spec097_cost.py`: running held calls at the end of a call adds 200 ns to
each wrapped call, so 0.2 µs to an orphan log, 0.4 µs to a traced call, and 0.2 µs to a
`configure()` or `health()`; an uncontended `with RLock:` costs 65 to 73 ns against 58 to 60 ns for a
`Lock`. End to end, interleaved six times, an orphan `info()` read 8.07 to 10.49 µs on base and 9.53
to 12.30 µs on the prototype, and a traced call with one log 27.7 to 45.4 µs and 27.8 to 51.3 µs:
the load hides the difference, so no criterion states a ratio. A swap of a live worker took a median
0.29 ms on base and 0.31 ms on the prototype, and an uncontended `shutdown()` 0.10 and 0.12 ms.

**What is not proven.** The deletion of the mechanisms the audit expected this path to retire, left
open in §12 by FR-007. Re-entry at the nineteen main-thread sections the sweep's workload does not
reach, which FR-005's first criterion makes the build reach or replace with a hold. Every
measurement with SPEC-089 and SPEC-091 built. `test_runtime_state.py`'s lock counts under the
`RLock`s, which its doubles could not read on the prototype. A run of the cells on Linux; every
number here is macOS.

---

## Revision history

- 2026-11-16, authored with SPEC-098 as one split: the two mechanisms are separate seams, which event
  a sink holds and when a lifecycle call may run, and the prototype measured each alone (this part
  moves 94 cells, SPEC-098's moves none).
- **Andrew's call, 2026-11-16:** reverse SPEC-054's trade. FR-001's withhold does, for a `declined`
  swap a stop overtook.
- **Andrew's call, 2026-11-16:** a `flush()` on the drain thread returns falsy with the reason
  `"re-entrant"` (FR-003).
- **Andrew's call, 2026-11-16:** `Leaves(A)` gets a spec of its own, written later in this arc; §12
  says so (Out of Scope, FR-007).
- 2026-11-16, revised after the spec review. B1: a stop deferred to a change's end ran its closes
  inline, so `configure()` took 10.01 s and 15.02 s against 5 s; every close of a deferred stop is
  now detached and its grace bounded by the change's budget (5.01, 2.01 and 5.01 s), and FR-002 says
  where a close that outlives the budget goes. B2: `configure()` and `health()` did not run held
  calls, so a held stop that landed in either never ran with no further call; the end-runners are now
  every public call that takes a library lock, derived by `spec097_held_sweep.py`, FR-004 governs
  when FR-002 also applies, held calls run before a stop and before its last close pass, and
  `spec097_signal_in_section.py` gains the rows `rebind`, `swap` and `health`. M3: FR-005 is new: it
  derives the sections a handler's level or traced call can re-enter and runs each, and names the
  sink-lifecycle decision whose rejected `RLock` it narrows. M4: FR-005's recorder criterion and its
  mutant. M5: the first prototype marked the whole of `flush()`, which changed a sink's own `flush()`
  hook's calls; only the marker wait is marked now, and the hook reads as at `ead40798`. m7: FR-007
  cites invariant 3's and 5's real text. m8: FR-001's published-change criterion can fail, and its
  three SPEC-081 tests are named by a measurement. m9: FR-004's p1b criterion says what it does not
  prove. m10: the dependency on SPEC-089 and SPEC-091 is stated. m11: §12 keeps the deletion half
  open with an owner. The FRs grew from six to seven; the fork FR is now FR-006 and the record FR-007.
- Rejected while prototyping, with the measurement: a lock wrapper written in Python to count
  sections (8 unaccounted events under `p11_assembly.py`'s recursion faults); a re-arm after the
  orphan path's delivery (34 cells red); refusing a drain-thread `flush()` by thread name (two forker
  cells red); keeping the inherited sink-change holder in a child (the same two cells, and the
  `retarget` row of the fork-mid-hand test); marking the whole of `flush()` (a sink's own `flush()`
  hook's calls changed); closing a deferred stop's sinks inline (`configure()` past its budget); an
  untyped `runs_held` (`test_the_probe_can_fail` failed).
- **Andrew's call, 2026-11-18:** a `flush()` made inside a SPEC-091 sweep hold returns
  `"re-entrant"`, not `"abandoned"`: FR-004 governs for `_sweep_lock`.
- 2026-11-18, built on `c9068a46` (SPEC-089, SPEC-090, SPEC-091, SPEC-096, SPEC-102 and SPEC-103
  built), and the Evidence re-measured there. **The cells**, every one of the 637 and the
  reproduction run with pins and withholding ignored: 98 red at `c9068a46` (56
  `Stalled:shutdown-none`, 36 `HangFound:flush-none@worker._flush_marked`, 2
  `CloseBroken:repeat@1`, 2 `CloseBroken:never-closed@1`, 1 `ChildBroken:two-sinks`, the
  reproduction; SPEC-091 had already resolved `empty-submission`), 4 red on the build, each at its
  pin's verdict: 94 cells from red to green, none from green to red, no pinned verdict moved.
  **The probes**: p11 `forced` 0 of 7, every row `[1]`; p3 0.00 s on all six `shutdown` rows; p1b 0
  of 200 in every mode, three runs; `spec097_signal_in_section.py` all eleven rows exit 0;
  `spec097_held_sweep.py` no `NO RECIPE` and every fired row `retired=True held=0`, the calls that
  fire now including `continue_trace` (SPEC-091's sweep hold); `p11_assembly.py` `unaccounted=0`.
  **Premises that moved**, each recorded where it is used above: `_sweep_lock` was already an
  `RLock`, so six locks are converted; the ledger's registration lock and `_owned_lock` stay out of
  the oracle (plan review B1); a level and a traced call run held calls inline at their end, on the
  exceptional path too (an inline `except BaseException` that runs them and re-raises, without
  letting a held call's failure replace the exception), since a wrapper frame around `_log`,
  `_begin` and `_end` let a `RecursionError` escape their guards and `p11_assembly.py` read
  `unaccounted=2`; `configure()` is held at its entry after validation (plan
  review M2); `ConsoleWriter._lock` stays a `Lock`, exempt with its reason (M3); and each pin left
  in the phase that turned its cells green. **FR-005's sweep** reads `failed=0` over every section
  its three workloads reach (`main`, a `late` one whose first stop builds a late worker, and a
  `direct` one that drives the hand machinery, the put-back and the overflow count); the sections
  it does not reach each run only in a forked child's repair or only on the drain thread, except
  `_ledger.restart`, which the test harness alone calls. The sweep prints both populations, so
  re-running it counts them rather than this text. **The recorder** records no re-entry over the
  whole suite (the tests that fail under it each read a lock's type or its fork repair), and
  records the planted `with self._lock, self._lock:` in `Worker.health`, every record naming
  `worker.py` and `health`. Its positive half, the signal-in-section rows run under it in process,
  records exactly the two re-entries the design makes, one each: `orphan-arm/info` at
  `_lifecycle.py:_arm_and_hand`, and `submit/span` at `worker.py:_enqueue`, every other row none;
  `tests/test_lock_reentry.py` holds each row to that.
- 2026-11-18, revised after the second diff review (B1). A stop deferred during a re-entrant sink
  change, one a sink's drain-thread code or a `log-foundry-` thread made while another change was in
  flight, was appended to a list only the outer change's end took, or none did, and never ran (the
  reviewer's `p_reentrant_defer.py`, in both its modes). A stop now defers only to the change
  registered to take the list (FR-002's Deferred clause), and `p_reentrant_defer.py` reads as the
  reviewer's base tree does in both modes. The nested change no longer adopts an outer change's
  published operation either (m1): a swap adopts only the operation that installs its own sink.
- 2026-11-18, revised after the execution review (frame 3, B1, M1, M2). A `KeyboardInterrupt`
  landing between a change's registration as the taker and its end, or inside the end's section
  before it deregistered, left the registration set for good, and every later `shutdown()` and the
  exit drain deferred to it and closed nothing (the reviewer's `p8_real_sigint.py`, a real
  `SIGINT`, read `taker_left true`); one landing between the deregistration and the list's take
  left the list for the next unrelated `configure()`, which retired the library. It was the third
  round of the same class: a deferral resting on a slot that only a later statement clears. The
  taker now counts only while its change's frame is on its thread's stack, the deadlines are held
  on the change's operation, and the change's end finds what it published, and its registration,
  by that frame (FR-002's Deferred clause). `p8_real_sigint.py` now reads `taker_left false` with
  the main thread's `shutdown()` retiring. `_deferred_lock` joined FR-004's oracle (M2): a
  handler's `shutdown(timeout=None)` inside the end's section ran a whole stop there and
  deadlocked against a drain thread waiting for that lock, and is now held. The interrupt corpus
  (`tests/test_change_interrupts.py`) replaces the review loop for this class.
- 2026-11-18, revised after the final execution review. MAJOR 1: `configure(sink=...)` bound its
  own frame to a local, a reference cycle that kept its frame, its caller's and every superseded
  sink alive until a garbage collection (`p_cycle.py`, the collector disabled); the frame is now
  passed inline, each operation drops it at the change's end, and a test with the collector off
  holds it. MAJOR 2: `_sink_lock` and `_sink_holder` were released in the end section's
  `finally`, so an interrupt anywhere in that section left them for good and every later
  `configure(sink=...)` on the thread was read as re-entrant (`p_holder.py`). Each is now released
  in a `finally` of its own outside the end section, and the acquire is the first statement of
  the innermost `try`, on one line. Measured with the corpus child on both trees: rows leaving
  the lock held or the holder set went from 29 of 197 (main thread) and 23 of 137 (drain thread)
  at `5f538b8b` to 5 of 202 and 5 of 142, every one on the release statements themselves. The
  corpus now checks both, excuses each owned-lock row only by a derived reason, and names the
  tests that pin the branches no row reaches.
