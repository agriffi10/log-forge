---
type: Decision
title: "A hand the worker build declines for a close in flight is paid when that close finishes"
description: "while a stop is running, a worker build whose sink has a close in flight hands it no stop signal and marks that close's entry (`_CloseRegistry.defer_hand`); the participant that discharges the entry hands the current signal, never a refreshed one, where `_will_hand` allows, under no lock (`_hand_after_close`), and `put_back`, which holds `_state._lock`, pays after it. Never settle it through `_signal_owed`, never pay under `_state._lock`, never defer outside a stop. (fix after SPEC-079)"
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

**Only during a stop.** Outside a `shutdown()` a close in flight is a swap's or a wrapper's, which
nothing will cut short with the signal, so the build hands at once, as it always did;
`test_a_build_with_no_stop_running_hands_at_once_during_a_close` pins that boundary.

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
- **The orphan path's own window**, between a shutdown setting the signal and its take — an
  `info()` there refreshes and hands an unset event the close then waits on, measured at 4.00 s, and
  so does one between a late worker's stop and the second take — is older than this fix and not
  closed by it.

## History

### 2026-11-12

- **Settled**, as a fix after SPEC-079.
