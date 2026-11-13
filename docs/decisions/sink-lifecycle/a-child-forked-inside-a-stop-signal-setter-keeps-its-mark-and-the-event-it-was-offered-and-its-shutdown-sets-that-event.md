---
type: Decision
title: "A child forked inside a stop-signal setter keeps its mark and the event it was offered, and its shutdown sets that event"
description: "`offer_stop_signal` records each offer on the offering thread (`Runtime._offering_here`); a forked child keeps the forking thread's offered events, exact `threading.Event` only and re-initialised, and its `shutdown()` sets them where it sets its own signal; `handing_here` also reads the offer record and, from any thread of a child, the forking thread's inherited offers (`_fork_offers[:_fork_inherited]`); the resumed worker's fork-handler hand skips a sink it reports; the stop-end payment asks the `handing` mark alone. Never reset either mark in a child, never hand an offered sink from the fork handler, keep no per-sink history of hands. (SPEC-083)"
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

**Every thread of the child sees what the forking thread is still offering.** The mark and the
offer record are per thread, so in a child that leaves through `os._exit` any other thread read
neither, and it could hand the sink again while the setter's frame was still live. The system-frame
diff review measured this at `d3950848`. In the first case, another thread made the child's first
log, which settles the fork's signal debt; that thread blocked for the child's life on a setter
holding its own lock. In the second case, the resumed drain thread reached the built sink through a
factory wrapper's build; it delivered nothing, and the child's `shutdown()` spent its whole timeout.

`_clear_after_fork` therefore stores the forking thread's live offer list in
`Runtime._fork_offers` and its length in `Runtime._fork_inherited`, both rewritten on every fork.
`handing_here` also answers from the first `_fork_inherited` entries, comparing ids only, from any
thread. Storing the live list rather than a copy lets `offer_stop_signal`'s `finally` lower the
count as the forking thread pops below it. As a result, a child that unwinds out of the setter
stops sharing that offer, and an offer the forking thread makes after the fork lands beyond the
count, so it is never shared.

**Leaving an inherited offer re-owes the fork's signal debt.** While the forking thread is still
inside the offer, another thread of the child that pays the debt leaves the sink out and clears the
debt. On the retarget path, which has no settle loop, nothing then handed the sink the child's own
signal after the forking thread returned. The execution review measured this at `471eef5e`:
`alive=False debt=False before=False own=False cut=True`. So where `offer_stop_signal`'s `finally`
lowers `_fork_inherited`, it also sets `_signal_owed`, with both writes under one `_state._lock`.
That branch runs only in a child, on the forking thread. No caller holds that lock across an offer,
because a setter never runs under a lifecycle lock.

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
no exit to pay it. So the payment asks `_in_handing` and is unchanged. It reads no offer record, including a
child's inherited offers. No test pins this choice, because no reachable state tells the two
readings apart.

The readings differ only for a payment made on a thread inside an offer that has no `handing`
block, about the sink offered. Four offers have none: `_hand_retargeted`'s, `Worker._reinit_after_fork`'s
in the fork handler, the factory wrapper's `_forward_stop_signal` of its built sink, and
`Worker.__init__`'s with `offer_signal=True`. The argument first recorded here named only the
retarget, which was incomplete, and `_in_handing`'s docstring now argues all four cases from the
code:

- **Retarget.** The process's one worker is retired by the thread's own `shutdown()` before that
  shutdown pays, so the payment returns at its liveness check.
- **The fork handler.** It has already dropped every marked close.
- **The factory wrapper.** It offers its built sink, while the worker's sink is the wrapper.
- **A caller-built worker.** It is not the process's worker.

The spec's probes measured the retarget case and found the two readings identical. The mutant that gives the payment the widened
read survived the full suite, as expected.

**Do not build.**

- A reset of either mark in a child.
- A hand from the fork handler to a sink that the forking thread is offering.
- The widened read in `_pay_marks_after_stop`.
- A per-sink record of earlier hands. That would mean a reference to every sink ever handed a
  signal, which is what SPEC-081 bounded the ownership record to avoid.

**What is left.** All three items are recorded in `architecture.md` §13.

- A setter that forks before it stores the event leaves the sink holding what it held before. The
  child's `shutdown()` does not set that event unless the repair replaced it.
- After the child's `shutdown()`, the sink keeps the event that shutdown set for the rest of the
  child's life, so its later backoffs do not wait. Handing it a fresh signal is the nesting this
  decision forbids.
- A grandchild forked by another thread of a child that is still inside a lock-holding setter
  hangs on its first log. `_clear_after_fork` replaces `_fork_offers` with the new forking thread's
  list, so the child's view does not carry into that grandchild. It was the same at `d3950848`. It
  is not closed because the control hangs too: with no fork inside a hand at all, one thread
  inside a locked setter and another thread forking also dies on `SIGALRM`. That is the generic
  hazard of forking while another thread holds a lock the caller's code owns, which no record can
  close, so carrying the old prefix would fix only one shape of it.

A child that unwinds out of the setter, as with a bare `os.fork()`, needs nothing beyond the
re-owe. On the orphan and build paths, the outer hand's settle loop hands it its own signal. On the
retarget path, it holds the parent's event until its first log or `flush()` settles the fork's
signal debt, and that debt is owed again if another thread paid it first. A `shutdown()` before
that point is still cut short by the set.

## History

### 2026-11-13

- **Refined** after the execution review of the cross-thread view. Leaving an inherited offer now
  re-owes the fork's signal debt. A grandchild forked by another thread of a child that is still
  inside a lock-holding setter is recorded as a third limit, because the same hang occurs with no
  fork inside a hand at all.
- **Refined** after the system-frame diff review. A child's other threads now see the forking
  thread's inherited offers (`_fork_offers`, `_fork_inherited`), because another thread's first log
  and the resumed drain thread through a factory wrapper still handed the sink again. The payment's
  equivalence argument now covers all four offers that have no `handing` block.
- **Settled** by SPEC-083, after the spec review moved the stop-end payment onto the `handing`
  mark alone, and after the plan review added the parent-side retarget test.
