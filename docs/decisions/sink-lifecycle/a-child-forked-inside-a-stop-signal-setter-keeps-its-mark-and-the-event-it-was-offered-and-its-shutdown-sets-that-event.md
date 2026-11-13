---
type: Decision
title: "A child forked inside a stop-signal setter keeps its mark and the event it was offered, and its shutdown sets that event"
description: "`offer_stop_signal` records each offer on the offering thread (`Runtime._offering_here`); a forked child keeps the forking thread's offered events, exact `threading.Event` only and re-initialised, and its `shutdown()` sets them where it sets its own signal; `handing_here` also reads the offer record, and the resumed worker's fork-handler hand skips a sink it reports; the stop-end payment asks the `handing` mark alone. Never reset either mark in a child, never hand an offered sink from the fork handler, keep no per-sink history of hands. (SPEC-083)"
---

**The defect.** A sink's `log_foundry_stop_signal` setter is the sink's own code, and it can
start a process. If it does, for example with a fork-context `multiprocessing.Process` whose child
leaves through `os._exit`, the child stays inside that setter call for its whole life. At
`53b68203` two things went wrong in such a child.

- **Its `shutdown()` did not cut that sink's backoff short.** The child was still marked as
  handing the sink its signal, so every later decision skipped the sink, and the outer hand's
  settle loop that would have handed it never ran. The sink kept the parent's event, and the
  child's `shutdown()` set only the child's own. With a sink that does not subclass `Sink` and a
  5 s wait on the event it held, the wait ran 5.00 s after `shutdown(timeout=1)`. A sink that does
  subclass `Sink` was not affected, because the fork repair's shared memo gives it the child's
  signal.
- **On the worker paths it ran the setter again inside itself.** The fork handler resumes the
  inherited worker, and `Worker._reinit_after_fork` offered that worker's sink the child's signal,
  nested inside the setter call that forked. On the `Worker.retarget` path the child's first log
  did it once more, because `_hand_retargeted` offers without the `handing` mark. A setter holding
  its own `threading.Lock` hung the child on both paths before the child reached its first line.

**What was built.** The library records each event it offers on the offering thread, popped in a
`finally`. It records `id(sink)` rather than the sink, so a child that never pops the entry keeps
no sink alive. `_clear_after_fork` copies the forking thread's events into
`Runtime._stale_signals`, replacing whatever a grandparent left. It keeps exact `threading.Event`
objects only, so no `set` that sink code overrides runs. It re-initialises each event's condition,
because `Event.set` takes that condition and a parent thread may have held it at the fork: without
the re-initialisation, the child's `shutdown()` never returned. `_shutdown_worker` sets these
events in the critical section where it sets `_state._stop`. Both of its branches pass through that
section, and so does the `atexit` drain. A set reaches only the child's private copy, and only a
sink still holding that copy sees it. `handing_here(sink)` is true for a sink under `handing` or on
the offer record, and `Worker._reinit_after_fork` skips a sink it reports, so that sink keeps the
event the child's `shutdown()` will set. With these changes, the child ran the setter zero times on
the orphan, worker-build and retarget paths, and the wait ended at the child's `shutdown()`.

**It also fixes the same nesting in the parent, with no fork involved.** At `53b68203`, a setter
that `Worker.retarget` ran during a `configure(sink=...)` with a live worker, and that logged, was
handed its signal again from inside itself: two calls, depth two. A setter holding its own lock
deadlocked the process that way. The offer record now makes that log skip the sink, so the setter
runs once (`test_a_setter_a_retarget_runs_that_logs_is_not_handed_again`).

**Rejected: resetting `_handing_here` in the child.** It is the obvious fix, and it is what
SPEC-081 did for the release depth. It is wrong here because the mark is true: the child's code
runs from the setter's frame. Clearing it made the child's first log run the setter inside the
setter call that forked. Measured at `53b68203` with a setter that holds its own lock: without the
reset the child returns, and with it the child hangs on that lock. Measured at `c21755f1`: a child
that unwinds got a nested setter call it never had.
`test_a_lock_holding_setter_that_forks_is_never_reentered_in_the_child` turns red if both marks are
reset in the handler.

**The stop-end payment keeps the `handing` mark alone.** `_pay_marks_after_stop` parks a payment
on its thread (`payment_owed`), and only `handing()`'s exit pays it. A payment parked on the
widened read, inside `_hand_retargeted`'s offer, which runs outside any `handing` block, would have
no exit to pay it. So the payment asks `_in_handing` and is unchanged. No test pins this choice,
because no reachable state tells the two readings apart:

- A thread inside an offer with no `handing` mark is inside a retarget, which needs the process's
  one live worker. `_state._worker` is assigned once and never cleared.
- The only payment such a thread can make is its own `shutdown()`'s. That shutdown retires that
  worker before its `finally` pays, so the payment returns at its liveness check, with `_stop` set,
  before it reaches the predicate.

The spec's probes found the two readings identical. The mutant that gives the payment the widened
read survived the full suite, as expected.

**Do not build.**

- A reset of either mark in a child.
- A hand from the fork handler to a sink that the forking thread is offering.
- The widened read in `_pay_marks_after_stop`.
- A per-sink record of earlier hands. That would mean a reference to every sink ever handed a
  signal, which is what SPEC-081 bounded the ownership record to avoid.

**What is left.** Both items are recorded in `architecture.md` §13.

- A setter that forks before it stores the event leaves the sink holding what it held before. The
  child's `shutdown()` does not set that event unless the repair replaced it.
- After the child's `shutdown()`, the sink keeps the event that shutdown set for the rest of the
  child's life, so its later backoffs do not wait. Handing it a fresh signal is the nesting this
  decision forbids.

A child that unwinds out of the setter, as with a bare `os.fork()`, needs nothing new. On the orphan
and build paths the outer hand's settle loop hands it its own signal. On the retarget path it holds
the parent's event until its first log or `flush()` settles the fork's signal debt, and a
`shutdown()` before that point is still cut short by the set.

## History

### 2026-11-13

- **Settled** by SPEC-083, after the spec review moved the stop-end payment onto the `handing`
  mark alone, and after the plan review added the parent-side retarget test.
