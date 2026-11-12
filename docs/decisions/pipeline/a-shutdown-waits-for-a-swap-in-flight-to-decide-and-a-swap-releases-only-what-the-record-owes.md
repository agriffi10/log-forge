---
type: Decision
title: "A shutdown waits for a swap in flight to decide, and a swap releases only what the record owes"
description: "a `shutdown()` or the exit drain waits on each swap in flight's `_Operation.decided`, snapshotted in its entry section, within its one closer grace and never on its own thread or a `log-foundry-` thread, before it drains; and a swap's fenced branch releases the displaced sink only while `_CloseRegistry.owes` it. Never hold or wait on `config._sink_lock` for this. (fix after SPEC-079)"
---

**The defect.** `configure(sink=B)` with a live worker on A retargets the worker (A to B, verdict
`fenced`, A dropped from the worker's held set) and then, in a second critical section, arms B and
takes A to release it detached. A `shutdown()` landing between the two found A owed and unheld,
took it and closed it inline; B was the worker's sink and was drained into, but was not owed yet,
so the shutdown did not close it. The swap's decision then took A again — `take` popped with a
default — and released it a second time on `log-foundry-sink-close`, and armed B after the
shutdown's last take. Measured at `624d817f` with the swap parked after its retarget, a fresh
process per run: A closed at `[(3, MainThread), (3, log-foundry-sink-close)]` in 20 of 20 runs, the
same for the exit drain, and `shutdown()` returning with B open after it had received 6 events in 5
of 5 — invariant 5's observable failing. Probes: `tests/test_shutdown_mid_swap.py` forces each.

**Part 1: a swap releases its displaced sink only while the record still owes it.** The fenced
branch's condition gains `_state._closes.owes(previous)` (identity, not `in`), and `take` is
documented as taking only a sink its caller found owed: `_close_owed` and the stale loop already
take only from the record's own values. This is what keeps A once on every path the wait below
cannot cover — a re-entrant `shutdown()`, one whose wait ran out, and the unserialized configure
pair. **It is not the arm re-check the `_sink_lock` decision rejected.** Skipping an *arm* can leave
a sink owed by nobody, because nothing else may arm it. Skipping a *release* of a sink the record
no longer owes cannot, because every route out of the record is a take whose taker performs the
close (`take` moves it to in flight in one call), and the worker's sink is always armed — the build
arms it, the swap arms what it installs, a forked child re-arms `worker.sink` — so "not owed" on the
fenced branch means "taken", never "never armed". An unserialized second swap retargeting B to C
before B's swap has armed B finds B not owed and skips it; B's own swap then arms it, so B is closed
once, later, instead of twice.

**Part 2: a shutdown waits for each swap in flight to take its decision.** `_Operation` carries
`decided`, a `threading.Event`, and `thread`, the ident of the call that built it. `_swap_sink` sets
`decided` right after its last critical section — the decision on the worker branch, or the
absorbed retarget exception; the first section on the no-worker branch — before it joins its
closers, and again in its `finally`. The shutdown snapshots, in its entry critical section after
the retirement and `_stop.set()` (no added lock acquisition, so the pinned counts hold), every token
not yet decided whose `thread` is not its own; on a `log-foundry-` thread it snapshots none. A swap
not in the snapshot reads the worker after the retirement, finds none live and meets the drain at
its own entry (SPEC-078 FR-004). The wait sits at the head of each branch — before `worker.stop`,
and as the first statement inside the no-worker branch's `try`, so an interrupt there still lowers
`_shutdown_running`.

**On the no-worker branch the decision is published before the stop-signal hand.** That branch's
decision is whole when its first section ends — the stale sinks taken, the new sink armed, the
signal chosen — and the hand after it runs the sink's own setter. Published only after the hand,
a slow setter held a `shutdown()` on another thread for the whole grace: measured, a setter parked
3 s made `shutdown(timeout=5)` take 2.00 s and write the not-decided line for a decision taken
long before; published first, 0.00 s and no line. The shutdown may then close the new sink while
its setter still runs, the window the stop-signal decision records under *What is left*; the
setter stores the event that shutdown set. **The worker branch keeps the wait**: its retarget
hands the new sink its signal before the decision arms it, so a slow setter there still holds a
shutdown on another thread for the grace, measured at 2.00 s with the line. That is this
decision's bound for somebody else's slow step, not a gap, because returning before the arm is
the defect Part 2 exists to prevent.

**Bounded by the call's one closer grace, and charged to it, not to the drain.** The wait ends at
`now + closer_grace(op.deadline)`, so `shutdown(None)` cannot hang on a stuck swap. What it waited
is recorded as `op.grace_spent`, and `_open_grace` fixes `grace_end` at
`now + max(0, closer_grace(deadline) - grace_spent)`, so the call spends at most one grace (SPEC-078
FR-002) and the drain in between is not charged to it. A free snapshot waits on nothing and records
nothing. A swap that does not decide in time is proceeded past: Part 1 still closes A once. The swap
may yet decide during the drain, and then its arm lands before the call's take and the call closes
B, so the `pending` line saying the installed sink may be left open until the process exits is
written only after the call's last take, and only for a swap that had still not decided in that
take's critical section (`_Operation.unsettled`, narrowed by `_close_owed`). Written at the wait, it
was false on the frame-2 review's reproduction: a swap stuck in its first flush behind a 3 s `emit`
declined and armed B in time for the same call to close it.

**Re-entrant never waits.** A shutdown on the swap's own thread (a stop-signal setter run by the
retarget, a signal handler interrupting `configure()`) skips its own token; one on the drain thread
(a sink's `emit`, which a retarget's flush may be waiting on) waits on no swap. Either wait is on a
thread an undecided swap may be waiting on, so it would last the whole grace. The rule is written as
`config._change_sink`'s `log-foundry-` prefix, so it also covers a closer thread, which is harmless
rather than needed: a swap joins its closers only after it sets `decided`, and a decided swap is
never in the snapshot.

**A retirement after the retarget's reassign is a declined swap once the drain thread has
ended.** A re-entrant `shutdown()` from the new sink's stop-signal setter, or a cross-thread one
that retires the worker before the retarget's fence flush reads the count, makes that flush
answer `retired` without trying. Where the drain thread has ended by then — the setter's own
`shutdown()` joins it — that shutdown has drained into the new sink and taken what the record
owed, and nothing of the drain can be inside the displaced sink. `Worker.retarget` read it as
`unfenced`, so `incomplete_swaps` went to 1 and a line said the displaced sink "could not be
confirmed drained" and was "left open" though it had been closed; it now reports `declined`,
and the owner's path is the same for both. **With the drain thread still alive it stays
`unfenced`**, line and count as before: a cross-thread shutdown waiting for this swap's decision
has not stopped the drain yet, and one that expired leaves the thread inside the displaced sink,
which then really is left open until a later `shutdown()` finds the thread ended — measured,
reporting `declined` there wrote nothing and undercounted `incomplete_swaps` for that open sink.
**A first drain that failed stays `unfenced`** too, deliberately, even when such a shutdown lands
after the reassign: that drain really was unconfirmed, and the verdict reports it. Its line then
still says the displaced sink is "left open", which is false if that shutdown already closed it;
reading the retirement there too would bury the failed drain under the shutdown's verdict.

**Rejected: holding `config._sink_lock` for the shutdown (v1).** The plan review built it: six
shipped tests broke, and a backoff that the stop signal should cut short was held behind the lock
instead (a 4 s backoff under `shutdown(2)`: 0.00 s at the base, 3.80 s and a `ShutdownTimeout` with
the lock). **Rejected: passing `config._sink_lock` as a barrier (v2).** The lock is held through the
swap's closer join, so the barrier also waited out slow displaced closes; it opened the call's grace
before the drain and starved the graced waits after it; it printed a line claiming a wait it had
not made; and it sat before the no-worker branch's `try`, so an interrupt leaked
`_shutdown_running`. What the shutdown needs is narrower than the lock — that no swap is between its
retarget and its decision when it takes — so it waits on exactly that.

**Not covered, recorded.** A swap that starts after the shutdown's entry and retargets a *late*
worker (one built during a no-worker shutdown) is not in the snapshot. Part 1 keeps the displaced
sink once; the installed one may be left owed until the exit closes it. Nor is a re-entrant
shutdown (on the drain thread, on the swap's own thread, or in a signal handler) landing between the
retarget and the decision: it drains and returns with the installed sink not yet owed, so that sink
is left open until a later `shutdown()` or the exit closes it, and no line says so; the displaced
one is still closed once. `invariants.md` §5 records both beside the not-decided fallback, the only
one of the three that writes a `pending` line. And a `shutdown()` nested in a sink's `close()`, or
in a signal handler on a thread that is not the swap's, waits its own grace on the same stuck swap,
so the outer call can spend two graces and the line prints twice (measured 1.01 s at a 0.5 s
grace); accepted, because invariant 3 still bounds it and a fix needs a thread-local flag in user
code.

## History

### 2026-11-12

- **Refined**: the no-worker swap sets `decided` before its stop-signal hand again, as this file
  always stated; #341 had moved it after the hand, and a slow setter held a cross-thread
  `shutdown()` for the grace. The worker branch's wait, unchanged, is recorded beside it.
- **Refined**: a retarget whose fence flush finds the worker retired and its drain thread ended
  reports `declined`, so a setter's `shutdown()` no longer counts `incomplete_swaps` or writes the
  unfenced line; with the thread still alive, or after a failed first drain, it stays `unfenced`.
- **Settled**, as a fix after SPEC-079. Supersedes SPEC-078's `STOP` row's `SWAPPING` cell ("drains
  and closes; the swap's retarget declines"), marked in that spec.
