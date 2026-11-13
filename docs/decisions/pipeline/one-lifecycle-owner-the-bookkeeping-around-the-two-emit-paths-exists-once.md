---
type: Decision
title: "One lifecycle owner: the bookkeeping around the two emit paths exists once"
description: "the *emit* paths stay two (arch §9, SPEC-028); what existed twice was the bookkeeping around them, and thirteen twin pairs are now one retirement **count**, one stop event, one owed-close record, one closer and one `Health` assembly. A count rather than a latch, because a latch makes a worker built after a `shutdown()` returned count every event it delivers as stranded. Retiring the closed-sink latch raises three close counts from one to two, each following the delivery it discharges. (SPEC-054; arch §9.2, §12)"
---

**One lifecycle owner: the bookkeeping around the two emit paths exists once** — the library delivers events two ways and that stays (arch §9, SPEC-028): a `@trace`d
call hands its buffer to a worker, a level call with no active span emits on the caller's thread.
What existed **twice** was the bookkeeping — who owes a sink a close, which stop event a sink
holds, whether the process is retired, whether a close is in flight — once on `Worker` and once in
`_lifecycle`, with a set of predicates whose only job was to let each side ask the other what it
held. Measured at `98c7e78` by reading both modules end to end: **thirteen twin pairs**, and across
the seven specs that fixed lifecycle defects **13 of their 36 FRs** edited a mechanism on both
sides or added a guard on one that read the other's state. `docs/invariants.md` invariant 6 names
the consequence, and a probe found a live instance on its first run — `health().sink` read
`SinkLosses(dropped=0, failed=3)` inside a span and `None` outside one against the same sink,
because the no-worker branch synthesized eight fields and not that one. SPEC-040 is excluded from
that count because it *moved* rather than fixed: it put the orphan side's seven globals on one
object and left the worker's on `Worker`, so "one owner" had been the name of a file since
2026-08-11 and not yet a fact. (SPEC-054)

**Retirement is a count, not a latch, and that is what makes one latch possible.** A worker built
after a `shutdown()` **returned** still delivers (SPEC-044 FR-001, preserved), and against a
latched boolean every event it delivered would be counted as queued where nothing will drain it —
`Health`'s definition of `submitted_after_shutdown` (SPEC-030 FR-001). Against the count, that
worker records the count at its build as its `_epoch` and reads as live until the *next*
`shutdown()` moves it, which is exactly when its submissions start being stranded. The count moves
a **second** time before a late worker is stopped, **when that worker is still live**, or that
worker reads as live after being stopped: delivering nothing, its next submission uncounted, and a
later swap waiting a whole budget for a fence it cannot confirm. The condition is since 2026-11-13,
when two stops became able to stop one worker built since their entry — the one that popped it as
its late worker and one whose take declined for it — so the count moves once per worker as well as
once per call
(`docs/decisions/sink-lifecycle/a-stop-never-takes-while-the-current-signal-is-the-unset-event-of-a-worker-built-since-its-entry.md`). `worker.py` binds the owner once at import rather than reaching
through the module per read — measured over 5M iterations in one process, best of 7:
`self._shutdown_done` 3.80 ns, `_lifecycle._state.retirements > self._epoch` 14.86 ns, bound
7.55 ns; the whole-`submit` harness cannot resolve any of that, spreading ~20 ns across runs of one
tree. (SPEC-054 FR-001)

**One owed-close record, armed by delivery and discharged at the close, retires two mechanisms
that contradicted it.** A sink enters the record when an orphan emit lands on it, when a worker is
built on it, and when a swap installs it in a worker; it leaves at the moment a close of it
*starts*, under the lock, in the same critical section that decides who performs it and registers
that close — so there is no instant at which a sink is neither owed nor in flight, which a
preempted orphan emit re-arms and a racing `shutdown()` then closes alongside. The **closed-sink
latch** refused re-arming for the most recently closed sink only, and what it protected against — a
close performed against a sink the drain thread may still be inside — is answered at close time by
`held`, not by refusing the arming. **`Worker(sink_released=)`** made a late worker inherit a
discharged close; under one record its build arms its sink and the closer's second pass closes it
after that worker's drain. Both retirements raise a close count from 1 to 2, and each second close
follows the delivery it discharges — SPEC-045 FR-002's one close per write-epoch, asserted by the
sink's event count at each close rather than by the count alone. One case is a genuine redundancy
and is the accepted trade: a swap the worker **declined** arms the new sink anyway, because
SPEC-035 FR-003 guarantees a declined sink is owned by somebody, and not arming it was built and
measured costing that guarantee outright. (SPEC-054 FR-002)

**One closer for every exit, and a caller waits on the registry rather than on the record.** Four
functions closed sinks on one path each; one takes every owed sink nothing `held` says something
may be inside, closes the live one inline and the rest on threads joined in a `finally`, and grants
the closer grace **once**, at the end, on every path. A caller waits on every close registered by
somebody else for `closer_grace(deadline)` — keyed on the registry and not on the record, because
by the time a bystander arrives the caller inside the close has already discharged that sink, so
the record is empty and a record-keyed wait never fires (measured: the second caller returned
through a 0.6 s close at `closed == 0`). A sink held only by a *drain thread* costs no grace, since
nothing will release it inside one. A registration the closer did not hand off is discharged in a
`finally`, because an entry nobody sets is permanent: that sink is skipped by the signal refresh
forever, never taken by a later closer, and every later bystander waits out the whole grace on it.
The registration is held on a small object rather than as a bare `Event` in the registry dict,
because `_fork`'s repair walk assigns a replacement back **by name** and cannot reach a primitive
that is only a dict value. (SPEC-054 FR-003)

> **Superseded in part by SPEC-079.** A forked child no longer repairs an in-flight close's event:
> its one walk starts from the runtime's roots, `_state` is not one, and `_clear_after_fork` drops
> every in-flight entry with its event. The registration's shape is unchanged. The fork order lives
> in `docs/decisions/pipeline/only-the-forked-child-is-repaired-and-its-order-of-work-is-the-contract.md`.

**`health()` is assembled once, and every field has one authority** — the worker's counters where a
worker exists, `retired` from the count, `sink` from the **configured** sink, `closing_sinks` from
the closer registry, `inherited_sink` from the config, the loss counters from `decorator`. Two
branches assembling eight fields apiece is what let `sink` be filled on one path only. Reading the
config rather than `worker.sink` is an observable change on the worker path — the two differ
permanently after a declined swap and after any `configure()` on a retired worker — taken because
`architecture.md` §12 already named the config the authority for "installed", and it closes that
section's `inherited_sink` open item in the same edit. (SPEC-054 FR-005)

## History

### 2026-11-13

- **Refined**: the second move of the count before a late worker is stopped is now conditional on
  that worker still being live, since two stops can stop one worker built since their entry; see
  the sink-lifecycle decision on a take declining for that worker.

### 2026-09-06

- **Settled.**
