---
type: Decision
title: "A hand the worker build or a retarget declines for a close in flight is paid when that close finishes"
description: "while a stop is running, a worker build whose sink has a close in flight, or a `Worker.retarget` onto such a sink (`_hand_retargeted`), hands it no stop signal and marks that close (`_CloseRegistry.defer_hand`); the entry's discharge hands the current signal, never a refreshed one, where `_will_hand` allows, under no lock (`_hand_after_close`; `put_back` pays after releasing `_state._lock`); a mark whose close outlives the stop is paid by the last stop out or, landing after that stop looked, by its writer (`_pay_marks_after_stop`), and a payment reached inside a stop-signal hand runs when its thread leaves its last hand (`handing`). Never settle it through `_signal_owed`, never pay under `_state._lock`, never defer outside a stop. (fix after SPEC-079)"
---

**The defect.** A no-worker `shutdown()` sets the stop signal and closes the configured sink; a
sink's `close()` that backs off on its signal is cut short at once. A `@trace` landing inside that
close builds the late worker, `Worker.__init__` refreshes the set signal to a fresh one, and the
build handed the sink that fresh, unset event after its section. The close, already waiting on the
sink's signal, then served its whole backoff. Measured at `9f4d4dfd` in a forked child with a 4 s
backoff: `shutdown(timeout=1)` took 4.00 s, the close waiting on the late worker's unset event,
which the main thread had handed inside the close; an `info()` in the same place took 0.00 s,
because `_will_hand` refuses a sink whose close is in flight (SPEC-044 FR-003). The build never
asked that question.

**The build declines and marks; the close's discharge pays.** In its critical section, while
`_state._stops_running` is non-empty, the build asks `_CloseRegistry.defer_hand(sink)`, which in
one acquisition of the registry's leaf finds the sink's in-flight entry and sets its `hand_owed`.
A marked build opens no hand token and hands nothing. `finish` reports the mark to the participant
that discharges the entry, read under the same leaf, so exactly one caller pays and no hand is lost
between them: either the build marked the entry before it was removed, or it found none and handed
the sink itself. The payer runs `_hand_after_close`, which asks `_will_hand` under `_state._lock`,
reads `_state._stop` as it is, and hands it with no lock held through `_hand_out`, so the settle
rule carries over unchanged. Measured with the fix: the close waits on the shutdown's set signal
(0.00 s), the late worker's event is handed, unset, on the closing thread once the close is over,
and the shutdown's second pass then stops the late worker, setting that event, and closes the sink
on it.

**Every discharge pays, and none of them under the lock.**

- `release()`'s inline exit, in its `finally`: the shutdown's inline close and its `JOINED` fan-out
  (through `_close_taken`), a `GRACED` closer thread (through `_close_guarded`), and every wrapper's
  forwarded release. None holds a lifecycle lock. A close that raised still pays; a setter that
  raises `KeyboardInterrupt` or `SystemExit` there replaces the close's exception, which
  `release()`'s `Raises:` says.
- `release()`'s refused exit and its detached thread-start-failure exit pay too. The only callers
  holding `_state._lock` are `_swap_sink`'s stale and fenced releases, and each takes, releases and
  discharges in **one** critical section, while the mark is written only under the same lock, so
  their entries are never marked; `_close_owed`'s `GRACED` start-failure needs a worker holding the
  sink unfenced, and while any worker exists no build runs.
- `put_back` holds `_state._lock`, and an interrupt between `_close_owed`'s take and its close can
  leave a marked entry there. It reports the mark; `_close_owed` pays after releasing the lock and
  after joining its fan-out, in a `finally` of its own, so a slow or raising setter cannot cut that
  join short.
- `_clear_after_fork` drops entries without a discharge; the child's signal debt and its worker
  rebuild's offer cover them, as they cover every inherited entry.

`tests/test_lock_held_discharges.py` holds the second and third bullets' shape over the source,
with a fixture corpus naming each reason.

**A retarget's hand declines and marks the same way.** `Worker.retarget` reassigns, then hands its
new sink the worker's `_stop`, and that hand asked nothing about a close of the sink in flight. A
swap during a `shutdown()` that moved a late or tail worker onto a sink the stop was closing handed
that close the worker's unset event, and the close served its backoff: measured at `a8e91fac`, a
no-worker `shutdown(timeout=1)` parked in its inline close of S, the late worker retargeted onto S
by `configure(sink=S)`, took 4.01 s whether the worker was built on S and swapped away and back,
or built on another sink and moved onto S. So `retarget` hands through `_hand_retargeted`, which,
while a stop is running, takes `_state._lock` and asks `defer_hand(sink)`; if it marked, it hands
nothing, and the mark is paid exactly as a build's is, by the close's discharge or by the last stop
out. Measured with the fix: S's close waits on the stop's set signal (0.00 s), and the worker's
event reaches S on the closing thread once the close is over. The gate is the whole stop, for the
build's reason below: a tail worker retargeted onto S inside the second pass's inline close is
deferred too, and gating on `_shutdown_running` instead let that hand into the close
(`test_a_retarget_inside_a_stops_second_pass_close_keeps_that_close_short` reddens). A swap with no
stop running reads the stop tokens without the lock and makes no added acquisition; a stop entering
just after that read sets `_state._stop`, which is the live worker's own `_stop` unless that worker
was stopped directly, when its `_stop` is already set; so the hand made then delivers an event that
is set or that stop sets, and the stop cannot take the sink while the worker holds it.
The mark has two writers now, both under `_state._lock`, so the shape above holds for both, and
`tests/test_lock_held_discharges.py` rosters both. `_close_owed`'s `GRACED` start-failure exit,
which no build could mark while a worker exists, can now be marked by a retarget; that release
runs with no lock held, so it pays as the inline exit does.

**Only during a stop, and paid at the stop's end if the close outlives it.** Outside a
`shutdown()` a close in flight is a swap's or a wrapper's, which nothing will cut short with the
signal, so the build hands at once, as it always did;
`test_a_build_with_no_stop_running_hands_at_once_during_a_close` pins that boundary, and
`test_a_retarget_with_no_stop_running_hands_at_once_during_a_close` pins it for the retarget. The
gate is the whole stop (`_state._stops_running`), not the window in which the build's worker is that
stop's late worker (`_shutdown_running`): a no-worker stop's second pass closes the sink inline
after that count drops, and a build inside that close, handed at once, made the close serve its
whole backoff — measured 4.92 s against 0.91 s. So a build in the stop's tail defers too, though
its worker is live and stopped by nobody. Where the close it marked is not one the stop performs —
a swap's displaced close still running — waiting for that close's discharge left the live worker
delivering into a sink on the shutdown's set signal: measured, three 0.3 s backoffs then waited
0.00 s each, against 0.30 s each at the base. So **the last stop out pays** (`_pay_marks_after_stop`,
run by `_shutdown_worker` after it discards its token): finding no stop left under
`_state._lock`, it clears the mark on the live worker's sink (`_CloseRegistry.unmark`) and hands
that sink the worker's own signal with no lock held. It skips only `_will_hand`'s refusal of a sink
whose close is in flight — no stop awaits that close any more, which is the moment the build
handed it before this fix — and keeps every other term: the worker live and draining into the
sink, its `_stop` the current signal and unset; and it never runs inside a stop-signal hand on its
own thread (below). It never refreshes. The mark is consumed once under the registry's leaf, so of two stops finishing together
only one hands, and the close's own discharge, finding no mark, pays nothing. A `shutdown()` that
left no mark asks a lock-free read and one leaf acquisition (`_CloseRegistry.marked`), so no pinned
count of `_state._lock` moves. A fork during that hand leaves the child its own repair, as for any
hand: its in-flight entries go with their marks and it owes its sinks its own signal.
`test_a_worker_built_in_a_stops_tail_is_handed_its_signal_when_the_stop_ends` and
`test_a_build_inside_a_stops_second_pass_close_keeps_that_close_short` hold the two halves.

**A mark that lands after the stop's end looked is paid by its writer.** Each writer reads a stop
token under `_state._lock` and then marks, while the last stop's `finally` discards its token with
no lock held and only then asks `_CloseRegistry.marked()`. So this order is legal: the writer reads
the token, the stop discards it and finds no mark, and the writer marks. A mark left that way on a
close that outlives the stop, a swap's, was paid by nothing until that close ended, and the live
tail worker delivered into S on the stop's set signal after `shutdown()` returned. Forced in a
forked child, a 0.3 s backoff after the shutdown then waited 0.0 s: at `609d5c37` for both writers,
and at `a8e91fac` for the build, the retarget not deferring there. So each writer that marked
calls `_pay_a_mark_the_stop_missed` once it has released the lock, outside every lifecycle lock;
that runs `_pay_marks_after_stop`, whose first line is a lock-free re-read of the stop tokens. The
stop discards and then reads the marks, the writer marks and then reads the tokens, so at least
one of the two sees the other; `unmark` consumes a mark once under the leaf, so at most one pays.
With the fix the backoff waits 0.3 s and S holds the live worker's own signal.
`test_a_mark_made_after_the_stops_end_looked_is_still_paid` forces the order for both writers. A
second, writer-side copy of the token check before that call was an equivalent mutant (made
unconditional, the whole suite stayed green), since `_pay_marks_after_stop` makes the same check
first, so it was dropped. `tests/test_lock_held_discharges.py` counts both stop-end payers among
the payments it refuses under `_state._lock`. The re-read takes no lock and the payment takes it
only when a mark exists, so no pinned lock count moves.

**A payment reached inside a stop-signal hand runs when that hand ends.** The stop-end payment
declined while its thread was handing the sink (`handing_here`), so the setter would not run inside
itself, and the hand already running declined too, because its settle refuses a sink whose close is
in flight. Two routes reach that: a setter calls `shutdown()` and a build in that stop's tail, on
the same thread, marks the sink's swap close, so the stop's own end runs inside the hand; or a build
a setter runs marks after the last stop's end looked, so its writer's payment does. Either way
nothing paid, and the live worker delivered into the sink on the stop's set signal until the close
ended: in a forked child, a 0.3 s backoff waited 0.0 s with the mark still set, at `ab7f620d` and at
`a8e91fac` alike, the stop's own route unforced and the writer's forced. Paying at once instead runs
the setter nested inside itself. So `_pay_marks_after_stop`, finding a mark while its thread is
inside any hand (`_mid_hand`), sets `payment_owed` on that thread and returns, and `handing()`'s
exit runs it when the thread leaves its last hand, with no setter on its stack: every caller of
`handing()` exits with no lifecycle lock held. The payment's `handing_here` term went with it, since
it can no longer refuse. An ordinary hand pays one thread-local read at its last exit, and no pinned
lock count moves. `test_a_stop_end_payment_inside_a_hand_is_paid_when_the_hand_ends` holds both
routes, asserting the signal S holds, the mark gone, the backoff waiting 0.3 s, and that the hand
came after the setter's own hand rather than inside it; never setting the flag, never paying at the
exit, and paying at once with no deferral each redden it.

**Rejected: declining without paying.** The late worker then delivers into a sink still on the
shutdown's set signal, so any backoff of the sink's own collapses to nothing — SPEC-033 FR-004's
tight retry loop — until a later orphan log happens to refresh it, which a process that only
traces never makes.

**Rejected: settling it through `Runtime._signal_owed`.** A debt is paid by the next orphan log or
`flush()`, and those refresh. Measured: a `flush()` between the late worker's stop and the
shutdown's second take paid the debt by handing a fresh unset event into that take's close, 4.00 s
against 0.00 s with the entry mark, and the late worker's own event never reached the sink.

**Rejected: a token for the deferred build.** It would send other threads' orphan logs to the
locked branch, where `_will_hand` refuses the sink while its close is in flight, so they would hand
it nothing; `docs/decisions/sink-lifecycle/a-stop-signal-setter-runs-under-no-lock-its-own-calls-would-wait-on-the-owner-decides-then-hands.md`'s
fence names this as its one tokenless publication.

**What is left.**

- **Between the discharge and the payment.** The discharge deletes the entry before the payment's
  section reads the moment, and no token is open, so an orphan log on another thread can take the
  fast path there and emit with the sink still on the set signal.
- **The payment's moment check is not separately held.** Its `handing_here` half is
  (`test_a_paid_hand_never_runs_a_setter_inside_itself`: without it the setter ran nested); its
  `_may_replace` half survived mutation over every shipped test, because each interleaving that
  reaches it hands an event a shutdown has already set. It stays, as the one test every hand asks.
- **The stop-end payment's locked re-read of `_stops_running` is unreachable, kept as a guard.**
  A stop's entry retires the one worker in the same critical section that adds its token, so any
  stop still running when the payment's section reads the tokens has already made
  `live_worker()` `None` there, and the payment would return on that term anyway. Its refusal of
  a set or foreign signal has no test of its own either. The mark being consumed once under the
  leaf is held: `test_a_mark_paid_at_the_stops_end_is_not_paid_again_by_the_close` reddens when
  `unmark` leaves the mark set, the close's discharge then handing the setter the signal again.
- **The stop-end hand runs the setter on the `shutdown()` caller's thread, after the graced
  wait.** Its time is that call's: a setter that itself calls `shutdown(timeout=1)` there made the
  outer `shutdown(timeout=1)` take 2.02 s. Invariant 3 bounds a call by its budget plus the sink's
  one in-flight call, and the setter is that call; the close-end payment already runs a setter
  inside the `shutdown()` that discharged the close, the same way. `test_reentrant_stop_signal.py`'s
  `stop-end` and `close-end` rows hold all five re-entrant calls to returning.
- ~~**`Worker.retarget`'s hand is the twin this does not cover.**~~ Covered since 2026-11-13: the
  retarget defers through `_hand_retargeted`, above.
- ~~**The orphan path's own window**, between a shutdown setting the signal and its take.~~ Closed
  since 2026-11-13, with `flush()`'s twin of it: while a stop has a take ahead, those sections hand
  the current signal and never refresh it
  (`docs/decisions/sink-lifecycle/a-hand-while-a-stop-has-a-take-ahead-hands-the-current-signal-and-never-refreshes-it.md`).
- **A late worker built during a no-worker stop's take-ahead makes the current signal unset.** Its
  build publishes its own unset `_stop` as `_state._stop`, which that stop sets only at the late
  worker's stop, after its first pass. So a hand of the current signal to another owed sink R before
  the first pass takes R — a settle re-reading the signal, `flush()` under the build's token, and
  this decision's payment alike — delivers an unset event that R's close then waits on. To
  reproduce: an orphan hand to R whose setter is still running as the stop enters, then a `@trace`
  building the late worker on another sink before the first take, so the hand's settle re-reads
  the late worker's unset signal; or a `flush()` while that build's hand token is open. With a 4 s
  backoff in R's close and `shutdown(timeout=1)`, R's first-pass close waited 4.00 s to 4.01 s at
  `a8e91fac` and with the 2026-11-13 fixes alike. It is a different source from the
  two windows closed here — a build publishing a signal the pending take will not see set — and
  the answer is structural (a stop's close not depending on the last event the sink was handed), so
  it is left to a session of its own.

## History

### 2026-11-13

- **Refined**: the fence extends to `Worker.retarget`, whose hand of its new sink now defers into a
  stop's close in flight through `_hand_retargeted` and is paid by the same discharge or stop-end
  payment; measured at `a8e91fac` that hand held a `shutdown(timeout=1)` 4.01 s. *What is left*
  loses the retarget twin and the orphan window, now closed (the latter by its own decision), and
  gains the late-build case the plan review found, with its measurement.
- **Refined** the same day, after the diff review: a writer that marks after the last stop's end
  looked for marks pays the mark itself (`_pay_a_mark_the_stop_missed`), closing a race the
  stop-end payment left open for both writers.
- **Refined** again, after the second system-frame round: a stop-end payment reached inside a
  stop-signal hand on its thread is owed to that thread and runs when it leaves its last hand
  (`handing`), rather than declining with nothing left to pay it; the fence now says so.

### 2026-11-12

- **Refined** the same day, before it landed: a build in a stop's tail is paid by the last stop
  out when the close it marked outlives the stop, and the retarget twin is recorded as left.
- **Settled**, as a fix after SPEC-079.
