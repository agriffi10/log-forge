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

**Bounded by the call's one closer grace, and charged to it, not to the drain.** The wait ends at
`now + closer_grace(op.deadline)`, so `shutdown(None)` cannot hang on a stuck swap. What it waited
is recorded as `op.grace_spent`, and `_open_grace` fixes `grace_end` at
`now + max(0, closer_grace(deadline) - grace_spent)`, so the call spends at most one grace (SPEC-078
FR-002) and the drain in between is not charged to it. A free snapshot waits on nothing and records
nothing. A swap that does not decide in time is proceeded past: Part 1 still closes A once, and one
`pending` line says the installed sink may be left open until the process exits.

**Re-entrant never waits.** A shutdown on the swap's own thread (a stop-signal setter run by the
retarget, a signal handler interrupting `configure()`) skips its own token; one on a library thread
(the drain's `emit`, which a retarget's flush is waiting on; a closer a swap is joining) waits on no
swap. Either wait is on a thread that is waiting on it, so it would last the whole grace. The
`log-foundry-` prefix is `config._change_sink`'s rule for a re-entrant `configure()`.

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
sink once; the installed one may be left owed until the exit closes it. `invariants.md` §5 records it
beside the not-decided fallback.

## History

### 2026-11-12

- **Settled**, as a fix after SPEC-079. Supersedes SPEC-078's `STOP` row's `SWAPPING` cell ("drains
  and closes; the swap's retarget declines"), marked in that spec.
