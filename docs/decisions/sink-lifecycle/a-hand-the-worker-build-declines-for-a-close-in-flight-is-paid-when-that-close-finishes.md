---
type: Decision
title: "A hand the worker build declines for a close in flight is paid when that close finishes"
description: "while a stop is running, a worker build whose sink has a close in flight hands it no stop signal and marks that close's entry (`_CloseRegistry.defer_hand`); the participant that discharges the entry hands the current signal, never a refreshed one, where `_will_hand` allows, under no lock (`_hand_after_close`), and `put_back`, which holds `_state._lock`, pays after it; a mark whose close outlives the stop is paid by the last stop out (`_pay_marks_after_stop`). Never settle it through `_signal_owed`, never pay under `_state._lock`, never defer outside a stop. (fix after SPEC-079)"
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

**Only during a stop, and paid at the stop's end if the close outlives it.** Outside a
`shutdown()` a close in flight is a swap's or a wrapper's, which nothing will cut short with the
signal, so the build hands at once, as it always did;
`test_a_build_with_no_stop_running_hands_at_once_during_a_close` pins that boundary. The gate is
the whole stop (`_state._stops_running`), not the window in which the build's worker is that
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
sink, its `_stop` the current signal and unset, this thread not already handing the sink. It never
refreshes. The mark is consumed once under the registry's leaf, so of two stops finishing together
only one hands, and the close's own discharge, finding no mark, pays nothing. A `shutdown()` that
left no mark asks a lock-free read and one leaf acquisition (`_CloseRegistry.marked`), so no pinned
count of `_state._lock` moves. A fork during that hand leaves the child its own repair, as for any
hand: its in-flight entries go with their marks and it owes its sinks its own signal.
`test_a_worker_built_in_a_stops_tail_is_handed_its_signal_when_the_stop_ends` and
`test_a_build_inside_a_stops_second_pass_close_keeps_that_close_short` hold the two halves.

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
- **The stop-end payment's concurrency terms are argued, not held by a test.** Its re-read of
  `_stops_running` under the lock (only the last stop out pays) and its refusal of a set or a
  foreign signal have no test of their own; the mark being consumed once under the leaf is what
  keeps two payers from both handing.
- **`Worker.retarget`'s hand is the twin this does not cover.** A retarget hands its new sink the
  worker's own `_stop` after the reassign, asking nothing about a close of that sink in flight; a
  late worker built during a shutdown's close of S and retargeted back onto S while that close
  still runs hands S the worker's unset signal, and the close serves its backoff — measured 4.01 s
  at the base and on this fix alike. Older than this fix, and left to a session of its own.
- **The orphan path's own window**, between a shutdown setting the signal and its take — an
  `info()` there refreshes and hands an unset event the close then waits on, measured at 4.00 s, and
  so does one between a late worker's stop and the second take — is older than this fix and not
  closed by it.

## History

### 2026-11-12

- **Refined** the same day, before it landed: a build in a stop's tail is paid by the last stop
  out when the close it marked outlives the stop, and the retarget twin is recorded as left.
- **Settled**, as a fix after SPEC-079.
