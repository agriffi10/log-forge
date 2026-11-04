---
type: Decision
title: "An expired shutdown is final, and a join the drain thread owes is handed over once"
description: "an expired `shutdown()` is final: a later caller waits only for a stop still in progress, and the first caller's recorded deadline stays write-once — except that a stop claimed on the drain thread, which cannot join itself, hands the gate and the deadline once to the next caller that can. (SPEC-068)"
---

**An expired shutdown is final, and a join the drain thread owes is handed over once** — three
rules for the moment a `shutdown()`'s join has expired, or cannot be made, while the drain thread
is still inside the sink.

**An expired shutdown is final.** A `stop()` that loses the once-only gate waits on
`_drain_settled` within its own budget while the drain is still running, and an abandonment
releases that wait. After an expiry `draining` is false, so a later caller does not wait on the
drain thread: it writes no line, sets no `stopped_reason` and calls no `_release_waiters`. The
closer then closes the sink only if it finds the drain thread ended; otherwise the sink stays owed
for a later call.
**The cost:** in an `atexit` exit after an expired `shutdown()`, a buffering sink whose drain thread
is still inside an emit is neither flushed nor closed. **The reason:** an exit that has already
spent the caller's budget does not spend a second one. Measured at `fda134a`, a second
`shutdown(timeout=3.0)` after an expired one returned in 0.00 s; waiting on the thread instead would
add `atexit`'s `DEFAULT_SHUTDOWN_TIMEOUT` (30 s), or forever under `timeout=None`, to every exit
over a wedged sink. SPEC-035 FR-004 AC-5 made the same call.

**The first caller's deadline stays write-once.** `claim_stop` writes `_exit_deadline` only in the
decision that wins the gate. A deadline written by a caller that lost the gate is one the joining
caller never budgeted for: if a later, larger budget extended the record while the first caller was
still joining, the drain would retry against it, and the first caller's join would expire over
retries it never asked for, announcing a timeout and leaving the sink open. That is SPEC-062's
measured race, and a later caller never re-opens a retry the first caller's deadline declined.

**The one exception: a join the drain thread owes is handed over, once.** A sink's `emit` can call
`shutdown()`, and then the drain thread is the caller. It cannot join itself: at `fda134a` a first
caller there raised `RuntimeError` out of `Thread.join`, and a losing one waited on an event only
its own exit could set. So a drain-thread winner records that a join is owed and returns without
joining, and the next caller **not** on the drain thread takes the gate over, records its own
deadline, and does the first caller's whole work — the join, and on expiry the line. That keeps
SPEC-062's rule that the caller whose timeout the join uses is the caller whose deadline the drain
declines against, because the drain-thread caller never joins; and it re-opens nothing, because no
join has expired. The flag is cleared in the decision that takes it, so only one handover happens.
A losing caller on the drain thread never waits on its own thread and never takes the owed join
over. Neither drain-thread caller is spared the rest of `shutdown()`: the closer and its grace
still run there, as for any caller.

**Rejected:**

- **A later caller that waits on the drain thread after an expiry.** It closes the sink an expired
  call left open, at the price of a second full budget on every exit over a wedged sink, which is
  the overrun the exit budget exists to remove.
- **Returning early on the drain thread with no handover.** The first prototype did that, and the
  spec review measured the hole: an emit that wedges *after* the drain-thread call is never
  joined, so it is never announced, and every later caller, `atexit` included, spends its whole
  budget waiting in silence — a 30.12 s exit with no line and no close.
- **A later caller extending the recorded deadline.** SPEC-062's race, above.

What stays out: `flush()` and `configure(sink=…)` made on the drain thread, which still wait on
themselves and need their own decision (`architecture.md` §12), and the after-shutdown submission
line, which still announces a loss the final drain can deliver (§12).

## History

### 2026-11-04

- **Settled** by SPEC-068. Refines *The exit drain declines only a retry predicted to end past the
  shutdown deadline* (`docs/decisions/sink-lifecycle/`) with the drain-thread handover, the one
  route by which a deadline is written twice.
