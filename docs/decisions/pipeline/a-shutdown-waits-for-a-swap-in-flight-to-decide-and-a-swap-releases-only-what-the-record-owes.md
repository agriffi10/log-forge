---
type: Decision
title: "A shutdown waits for a swap in flight to decide, and a swap releases only what the record owes"
description: "a `shutdown()` or the exit drain waits on each sink change in flight's `_Operation.decided`, published before its config write and snapshotted in its entry section, within its one closer grace, before it drains; one made from inside a running stop or change, or inside a library lock section, never waits: it is merged, deferred or held (SPEC-097). A swap's fenced branch releases the displaced sink only while `_CloseRegistry.owes` it, and cedes it when the installed graph still holds it; a no-worker or `declined` swap does not arm a sink a stop's `_close_owed` take took since its config write. Never hold or wait on `config._sink_lock` for this. (fix after SPEC-079; SPEC-087; SPEC-097)"
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
no longer owes cannot, because the worker's sink leaves the record only by a take whose taker
performs the close (`take` moves it to in flight in one call). The two other ways a sink leaves the
record since SPEC-087 never apply to it: the closer's guard passes over only a sink lying beneath
one still in use, and puts it back, so it reads as owed again; and `_CloseRegistry.cede`, the one
route out that closes nothing, is made only of a kept sink, in the section that arms the graph
holding it, never of the worker's current sink. And the worker's sink is always armed — the build
arms it, the swap arms what it installs, a forked child re-arms `worker.sink` — so "not owed" on the
fenced branch means "taken", never "never armed". An unserialized second swap retargeting B to C
before B's swap has armed B finds B not owed and skips it; B's own swap then arms it, so B is closed
once, later, instead of twice.

**When the graph the swap installs still holds the displaced sink, the fenced branch cedes it
rather than releasing it** (SPEC-087). The swap reads that graph once, before its first critical
section; where the displaced sink is in it and the record owes it, the branch takes it out of the
record with `_CloseRegistry.cede` and closes nothing, so the sink joins the installed graph and is
closed once, by that graph's own forward, when it is released. The `owes` and `held` terms still
decide whether the branch acts on the sink at all; the cede only replaces the release. Releasing it
here closed the live destination: `configure(A)` then `configure(MultiSink(A, B))` lost every
later event a sink that refuses after `close()` was offered. The rule, and what it rejected:
*A swap keeps the sinks the graph it installs holds, and one decision closes a sink once*, in this
directory.

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

**The change is in flight from before its config write** (SPEC-097 FR-001). `config._change_sink`
writes the new sink to the config before `_swap_sink` reaches its first section, and a shutdown
landing between the two read no swap in flight. A non-re-entrant change now publishes its
`_Operation` in `_swaps_running` before `_rebind` (`_lifecycle.open_command_swap`), lock-free as
the tokens are discarded, so `configure()`'s pinned lock counts do not move, and `_swap_sink`
decides with that same operation, its budget set from what the wait for `config._sink_lock` left.
The operation records the sink it installs (`installing`) and drops it when the change ends
(`close_command_swap`), so a published change never keeps a superseded sink alive.

**Part 3: a swap does not arm a sink a stop closed since its config write** (SPEC-097 FR-001). A
`_close_owed` take that takes a published change's `installing` marks that change `withheld`, in
the same section. A withheld change does not arm the sink on the no-worker branch, nor after a
retarget that answered `declined`; on `fenced` and `unfenced` it arms, because that is the only arm
the worker's new sink gets. Without it, `p11_swap_arms_a_sink_a_shutdown_closed.py`'s forced
variant closed the installed sink twice at one write count in 7 of 7 runs at `ead40798`; with it,
in 0 of 7. **This is not the arm re-check the `_sink_lock` decision rejects**, which can leave a
sink owed by nobody. The mark is set only by a `_close_owed` take, whose taker closes the sink, and
never by a swap's stale take: a withhold keyed on any take left a sink written after its last close
and owed by nobody, measured 3 of 3 by a reviewer at `027b39a5`, and with the stale take not
marking, that sink's closes enter at `[1, 2]`. The mark is per change, since one global slot is
overwritten by an unserialized or re-entrant `configure()`, and it is the config write's, since a
mark taken at the swap's entry left p11 at 3 of 3 double closes. The `declined` half reverses
SPEC-054's accepted trade (*One lifecycle owner*, in this directory), by Andrew's answer of
2026-11-16.

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

**Re-entrant never waits, and since SPEC-097 never runs under the command it is inside.** At
`_shutdown_worker`'s entry, first (FR-004): a call on a thread that owns one of the library's
locks, or that is inside its own `flush()`'s wait on its marker, which is what a signal handler or
a finalizer that interrupted the library is, is held on its thread and runs at the end of the
public call the thread was in. Then (FR-002): one on a running stop's own thread, or on a
`log-foundry-` thread while a stop runs, is merged and returns, the running stop being its
effect; one on the thread of the change registered as the taker of deferred stops (so a
stop-signal setter the retarget runs) or on a `log-foundry-` thread (a sink's `emit` that a
retarget's flush waits on, a closer), while that change's frame is still on its thread's stack, is
deferred to it, and that change runs one stop for every call deferred to it once it has released
its lock, before `configure()` returns, with every close detached and
its waits inside the change's swap budget. Until SPEC-097 such a call drained and returned: a
drain-thread stop took and closed the sink the change installs, which the change then armed again
(the two `installed-closed-twice` cells), and one nested in a sink's `close()` waited out the
closer grace on the close it was inside. The snapshot still skips its own thread's token and takes
none on a `log-foundry-` thread, for a call that reaches it on a thread an undecided swap may be
waiting on: a setter's `shutdown()` during a change whose wait for the lock ran out, while the
change holding the lock is the registered taker, is one.

**A retirement after the retarget's reassign is a declined swap once the displaced sink is no
longer owed.** A cross-thread `shutdown()` that retires the worker before the retarget's fence
flush reads the count makes that flush answer `retired` without trying. (A `shutdown()` from the
new sink's stop-signal setter did the same until SPEC-097; it is now deferred to the change's end,
so the swap completes, counting nothing, and the deferred stop closes the displaced sink once.) The verdict turns on the fact the unconfirmed-swap line reports:
whether the displaced sink is still owed, read after that flush. Not owed, the retiring shutdown
took it — a take happens only once nothing of the drain can be inside it, and the taker closes it;
no swap cedes the worker's own sink, and the closer's guard puts back only a sink lying beneath one
still held — so `Worker.retarget` reports `declined`; it used to read `unfenced`, so `incomplete_swaps` went
to 1 and a line said the displaced sink "could not be confirmed drained" and was "left open"
though it had been closed. **Still owed, it stays `unfenced`**, line and count as before: a
cross-thread shutdown waiting for this swap's decision has not taken it yet, and one that expired
with the drain inside it left it open until a later `shutdown()` finds the thread ended, where the
line is true. The owner's path is the same for both verdicts. A first rule keyed on the drain
thread's liveness was wrong in a measured case: read after the setter returned, it found a thread
that had left the displaced sink only after the retiring shutdown had expired, reported `declined`
and wrote nothing, for a sink left owed and open. **A first drain that failed stays `unfenced`**
too, deliberately, even when such a shutdown lands after the reassign: that drain really was
unconfirmed, and the verdict reports it. Its line then still says the displaced sink is "left
open", which is false if that shutdown already closed it; reading the record there too would bury
the failed drain under the shutdown's verdict.

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
sink once; the installed one may be left owed until the exit closes it. `invariants.md` §5 records
it beside the not-decided fallback, the only one of the two that writes a `pending` line. A
`shutdown()` whose wait for a `fenced` change runs out takes and closes the installed sink before
the change decides, and the change, which keeps its arm on `fenced`, arms it again, so the exit
closes it a second time at the same write count; `architecture.md` §12 holds that open. ~~Nor is
a re-entrant shutdown (on the drain thread, on the swap's own thread, or in a signal handler)
landing between the retarget and the decision: it drains and returns with the installed sink not
yet owed~~, and ~~a `shutdown()` nested in a sink's `close()`, or in a signal handler on a thread
that is not the swap's, waits its own grace on the same stuck swap~~ — struck by SPEC-097: each is
now merged into the running stop, deferred to the change's end or held on its thread, so none
drains under the change and none waits on the close it is inside.

## History

### 2026-11-18

- **Refined by SPEC-097's diff reviews**: a stop defers to the change registered as the taker while
  its frame is on its thread's stack, not to the holder of `config._sink_lock`; the re-entrant
  paragraph says so, and
  `docs/decisions/public-api/concurrent-sink-changes-are-serialized-by-one-lock-whose-wait-is-spent-from-the-swap-budget.md`
  says why.
- **Refined by SPEC-097**: a sink change is published before its config write, so a shutdown
  meets it from there; a no-worker or `declined` swap withholds its arm of a sink a stop's
  `_close_owed` take closed since that write (Part 3, reversing SPEC-054's trade); and a stop made
  from inside a running stop or change, or inside a library lock section, is merged, deferred or
  held at `_shutdown_worker`'s entry rather than draining under the command. The fence says so,
  the setter case leaves the retirement paragraph, and *Not covered* strikes the re-entrant
  shutdown and the nested two-grace wait in place, adding the expired-wait `fenced` double close
  that §12 holds open.

### 2026-11-15

- **Refined by SPEC-087**: the fenced branch cedes the displaced sink, closing nothing, when the
  graph the swap installs still holds it, rather than releasing it under the graph that now
  delivers to it; fence and Part 1 say so. Part 1's reason that "not owed" means "taken" no longer
  rests on every route out of the record being a take, which `cede` and the closer's guard made
  false as written: it now says why neither applies to the worker's sink. The retirement
  paragraph's verdict is unchanged, with the same reason added.

### 2026-11-12

- **Refined**: the no-worker swap sets `decided` before its stop-signal hand again, as this file
  always stated; #341 had moved it after the hand, and a slow setter held a cross-thread
  `shutdown()` for the grace. The worker branch's wait, unchanged, is recorded beside it.
- **Refined**: a retarget whose fence flush finds the worker retired and the displaced sink no
  longer owed reports `declined`, so a setter's `shutdown()` no longer counts `incomplete_swaps` or
  writes the unfenced line; with the sink still owed, or after a failed first drain, it stays
  `unfenced`. Keyed first on the drain thread's liveness, which a late-leaving thread fooled.
- **Settled**, as a fix after SPEC-079. Supersedes SPEC-078's `STOP` row's `SWAPPING` cell ("drains
  and closes; the swap's retarget declines"), marked in that spec.
