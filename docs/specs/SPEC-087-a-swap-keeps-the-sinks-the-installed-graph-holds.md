---
type: Spec
id: SPEC-087
title: "A Swap Keeps the Sinks the Graph It Installs Still Holds"
description: "Stops configure(sink=...) from closing a sink that the outgoing and the incoming sink graph share, so a caller who adds a destination beside a sink, or drops one, loses no event, and every sink is still closed exactly once."
spec_status: Completed
depends_on: [SPEC-032, SPEC-042, SPEC-045, SPEC-054, SPEC-078, SPEC-079, SPEC-081]
arc: audit-2026-11-14
arc_order: 3
---

## Overview

A process may change its sink while it runs, and the new sink graph may share a sink with the old
one. A caller who logs to a file and later adds a second destination writes
`configure(sink=MultiSink(file, other))`; one who later drops the second destination writes
`configure(sink=file)`. Since `v1.0.0` either call has closed the shared sink while it was the
live destination. A sink that has released its transport refuses from then on, so every later
event offered to it was lost: counted and announced, but lost (finding 4 of the 2026-11-14 audit,
`docs/architecture.md` §12). This spec makes a sink change leave open every sink the new graph
still holds, close the sinks it no longer holds as it does today, and close the shared sink
exactly once, at `shutdown()`, at exit, or when a later change drops it.

## Scope

### In Scope

- Which sinks a swap keeps: the graph it installs, read once by an existing walk. A kept sink the
  swap would have released leaves the owed record with no close and becomes a member of the graph
  that holds it (FR-001).
- One close per sink in every release the library starts: a shipped wrapper's forwarded release
  skips a sink that is kept, that a sink still in the record holds, or that the same decision has
  already closed, in the swap and in the closer `shutdown()` and the exit drain run (FR-002).
- The observable on every twin path: both delivery paths, an expired `shutdown()`, events queued
  at a later swap, an unconfirmed swap, a swap issued from sink code, a `shutdown()` and a swap in
  flight together, a forked child, the exit drain, and long-lived reconfiguration; and the guard
  that keeps either take from closing a sink that lies beneath one still in use (FR-003).
- What the read does not reach and what the library cannot spare, held to today's counted and
  announced behaviour and recorded, with what a caller who writes a wrapper can do (FR-004).
- The tests that replace SPEC-045's pin, with a sink that refuses after close (FR-005).
- The settled record, rewritten in place, and the release-notes entry (FR-006).

### Out of Scope

- **The lifecycle command path and the stable stop signal.** Both are later specs of this arc.
  This spec adds no field to `Runtime`, no lock and no lock site, and it leaves the stop-signal
  hands as they are. FR-003's guard reaches `_close_owed`'s existing put-back section, which takes
  `_state._lock`, on a path that reached it only after an interrupt before.
- **Changing `Runtime.held`, `Runtime.in_flight` or any of the readers FR-001 names.** They keep
  deciding by identity. FR-001 keeps a kept sink out of the record, and FR-003's guard covers the
  one route by which it can come back, at the two places a sink is taken to be closed.
- **What a wrapper's own `close()` does for a caller who calls it.** A caller closing a
  `MultiSink`, `FilteringSink` or `TransformSink` themselves still closes every member, a
  configured one included. The skip in FR-002 applies only inside a release the library starts, so
  no public behaviour of a wrapper changes (invariant 10).
- **A child forked while a swap is in flight** (audit finding 6) and **a fork from inside
  `close()`** (finding 19). FR-003 covers a child forked after the swap has returned.
- **A sink the library has already released, handed back.** `configure(A)`, then `configure(B)`,
  then `configure(A)` shares no sink between the graphs at either swap: A was released when nothing
  installed held it, and a shipped sink then refuses (SPEC-032). Measured at `5cb0f449`, a
  `FileSink` handed back kept 1 of 3 lines. That is a different defect with a different closure,
  and it is its own open item in `docs/architecture.md` §12, with its probe,
  `docs/audits/2026-11-14-probes/lifecycle/released_sink_handed_back.py`. This spec leaves that
  item as it is.
- **A wrapper's forwarded release that meets a close of the same member already in flight under
  another decision.** It still calls `close()` beside it (SPEC-078's participant rule), which the
  closed guard makes safe for a shipped sink (SPEC-076).
- **Holding a displaced wrapper open because closing it would close a kept sink.** A wrapper the
  package does not ship closes its member with its own code: the library cannot stop that
  wrapper's own `close()`. Deferring the wrapper's release was built and is rejected. It trades a
  counted, announced loss in one unusual call sequence for an owed record that grows with every
  reconfiguration and for overlapping closes of the shared sink, which is worse on invariant 5 and
  on SPEC-081's bounds. FR-004 records the constraint and what a caller can do, and *Evidence*
  the measurement.
- **A public way for a wrapper to declare its members to the library.** None exists:
  `_fork._CHILD_SINK_SLOTS` is private and keyed by the package's own modules. This spec adds
  none.
- **Wrapper graphs in `tests/test_invariants_model.py`.** That belongs to this arc's harness
  track. This spec carries its own seeded test (FR-003).
- **Any public name.** `configure()`, `health()`, `Sink` and every shipped sink keep their
  signatures, defaults and return values.

---

## Functional Requirements

### FR-001: A swap keeps every sink the graph it installs holds, and a kept sink joins that graph

#### Description:

The **installed graph** of a `configure(sink=…)` or `configure(sink_factory=…)` call is the sink it
hands the library and every sink beneath that sink. `_swap_sink` reads it once, before its first
critical section and with no lifecycle lock held, with the walk `_capture` already makes beneath a
sink (SPEC-081 FR-002): in a holder the package defines, its declared child-sink slots from
`_fork._CHILD_SINK_SLOTS` and its sink-shaped attributes; in any other holder, its attributes and
members; a plain container read one level; every read static; bounded at `_MARKING_CEILING`
objects entered. No second walk is built: the loop is factored out of `_capture` and both call it.
Membership is by identity, never equality. A sink in the installed graph is **kept**.

**The swap closes no kept sink.** Where it would have taken a kept sink out of the owed record and
released it, it takes it out of the record and closes nothing. Those places are the two that
exist today: the stale loop, for an owed sink that is not the new sink, not the worker's current
sink and not `Runtime.held`; and the fenced branch, for the displaced sink once the worker
confirmed its drain. The move is a registry method of its own, `_CloseRegistry.cede`, so every way
out of the record still has one home (SPEC-078 FR-001), and it is made in the critical section
that arms the sink that holds the kept one: the only section with no worker, and the section
after the retarget with one, where a kept stale sink passed over by the first section joins too.
A kept sink the swap would have left in the record stays there exactly as today: one displaced
without a confirmed fence, one a retired worker keeps, one that is held.

After the move the kept sink is a member of the installed graph and nothing else, which is the
state `configure()` of that graph alone produces. It is closed when its holder is released, by
the holder's forward: at `shutdown()`, at exit, or by a later swap that drops both. A later swap
that keeps it spares it again (FR-002), and a later `configure()` of the sink itself arms it as a
root again.

**Why it leaves the record.** The first draft of this spec left a kept sink owed, to be closed by
its own take. Five readers decide by the identity of the worker's top-level sink:
`Worker.may_be_inside` behind `Runtime.held`, `Runtime.in_flight`, `Worker.holds_unfenced`,
`_inline_close_choice`, and the stale loop's `stale is old`. To each of them a kept sink in the
record is a root nothing is using, while the drain thread is inside it through the wrapper.
Measured on that draft: an expired `shutdown(timeout=0.3)` closed the kept sink under the live
drain thread, and took 6.03 s when the sink's `close` shared its `emit` lock; and a later swap's
stale loop closed it before the drain, so 4 of 10 queued events arrived. Out of the record, none
of the five is ever asked about it on the fenced and the no-worker path, and on the unconfirmed
and declined paths the worker's own record still names the sink, so identity is right there.

**The read answers "what does the sink being installed hold"**, so it needs the sink the swap is
handed and nothing else. It does not read the configuration, and no release asks the question
again later (*Evidence* records what that costs).

**A read that trips its bound or raises** keeps what it had found and the swap goes on. It writes
one `_diag` line, each time it happens, saying that reading the sinks beneath a sink stopped
short and that a sink it shares with another may be closed while still in use, which is the
behaviour before this spec for whatever the read did not reach. The same line is written by
FR-002's read of what remains and by FR-003's guard, at a swap or at a `shutdown()`, so its text
names no sink change. Nothing is stored to make the line once-only, so it adds no state and no
inventory row; each of those calls is the caller's own, as the unconfirmed-swap line's is. The
line is the read's own:
`_announce_capture` and its once-per-process flag stay the demotion's alone. The read takes its
bound and its per-holder reader as arguments, so a test trips or faults it without touching
`_MARKING_CEILING`, `stamp` or `_capture`.

#### Acceptance Criteria:

- [ ] For each of `MultiSink(A, B)`, `FilteringSink(A)`, `TransformSink(A)` and
      `MultiSink(FilteringSink(A), B)` as `W`: `configure(A)`, one write, `configure(W)`, two
      writes, `shutdown()`. `A.close()` is not called before `shutdown()` and exactly once by it,
      A receives every event, no loss term of `health()` moves and stderr is empty, with no span
      and with a span around each write (invariants 2, 5 and 6).
- [ ] `docs/audits/2026-11-14-probes/lifecycle/p4b_forward.py`, whose shared sink is a shipped
      `FileSink`, prints 4 lines of 4 and 8 lines of 8 (invariant 2).
- [ ] When the forward `configure(W)` has returned, the owed record holds W and not A, and
      `health().closing_sinks` is 0 (invariant 5).
- [ ] `configure(A1)`, one write, `configure(MultiSink(A2, B))`, where `A1 == A2` and
      `A1 is not A2`: the swap closes A1 once and A2 is not closed before `shutdown()`
      (invariant 5).
- [ ] `configure(A)` then `configure(MultiSink(A, B))` with nothing written closes nothing, and a
      write followed by `shutdown()` then closes A once and B once (invariant 5).
- [ ] `tests/test_runtime_state.py::test_lock_counts_are_pinned`,
      `tests/test_runtime_holder.py::test_the_lock_sites_are_the_base_commits`,
      `tests/test_lock_held_discharges.py`, `tests/test_lifecycle_inventory.py` and
      `tests/test_sink_ownership.py::test_an_orphan_log_performs_no_stamp_walk` pass with no edit
      (invariant 3).
- [ ] With the read made to raise, and again with its bound passed as 1, each through its own
      argument: `configure()` returns, the new sink receives the next event, the read's line is
      written naming an exception by type and carrying no value, and
      `tests/test_ownership_record.py::test_a_capture_that_trips_or_raises_is_absorbed_with_one_line`
      passes with no edit (invariants 1 and 11).

### FR-002: A release the library starts closes no sink that is kept, still held, or already closed by the same decision

#### Description:

A shipped wrapper's `close()` forwards a release for each sink in its declared slots
(`release(member, owner=wrapper)`). That is how the reverse direction closed the live sink: the
swap released the outgoing `MultiSink(A, B)` and the wrapper's `close()` closed A.

A **decision** is one `_swap_sink` call or one take in `_close_owed`. Every release a decision
starts carries a **keep**, and a forwarded release made inside it, at any depth of wrapper on the
thread performing the release, asks the keep before it closes. The keep has three parts, each
read at one stated moment.

1. **Kept**: the installed graph, for a swap. Read once, before the swap's first critical
   section (FR-001).
2. **Remaining**: the sinks the record will still hold once the critical section that starts the
   release has made its takes. In `_swap_sink` the section works the list out before it takes
   anything, from the sinks it is about to take, because each take stays inside its own
   `release(…)` call as it is today, which is the shape `tests/test_lock_held_discharges.py`
   holds; `_close_owed`, whose takes and releases are already apart, works it out after its
   takes. The list is made under the lock the section already holds. The graphs beneath those
   sinks are read by FR-001's walk on the first forwarded release that asks, with no lock held,
   and never read if no forward asks.
3. **Closed**: the sinks this decision closes. It is one set for the whole decision, shared by
   every release it starts in either critical section. A take adds its sink when it is made, and
   a forwarded release claims its sink with a token of its own and goes ahead only if the claim
   it reads back is that token, so of two forwards that race, one closes.

A forwarded release whose sink is kept, or lies in the graph of a remaining sink, or is already
in the closed set, is skipped: it performs no close, registers no close in flight, and returns as
a refused release does. Otherwise it adds its sink to the closed set and closes it. The wrapper's
own `close()` still runs and its other members are still closed.

Each part answers a measured double. Kept is the reverse direction above. Remaining is the same
defect one level down: a retired worker's drain thread inside `W1 = MultiSink(A, B)`, a second
wrapper `W2 = MultiSink(A, C)` armed beside it, and a `shutdown()` that takes W2 closed A under
that thread at `5cb0f449`. Closed is two owed wrappers that share a member and are taken in one
pass: at `5cb0f449` an unconfirmed swap from `MultiSink(A, B)` to `MultiSink(A, C)` followed by
`shutdown()` closed A twice, overlapping.

Two things the first review had to guess are fixed here. The skip comes after the `releasable`
check, so a sink this process may not release is refused first, as today. A forwarded release
nested inside another forwarded release inherits the keep of the release that encloses both.

A forwarded release outside any release the library started carries no keep, so a caller closing
a wrapper themselves is unaffected. `release` stays the one path by which the library closes a
sink (SPEC-042), which is why one rule there covers every shipped wrapper, the `sink_factory`
wrapper included, with no change to any wrapper's `close()`.

#### Acceptance Criteria:

- [ ] For each `W` of FR-001's first criterion: `configure(W)`, one write, `configure(A)`, two
      writes, `shutdown()`. Each unshared member has been closed exactly once when
      `configure(A)` returns, `W.close()` has run once, A is not closed before `shutdown()` and
      exactly once by it, A receives every event and no loss term moves, on both delivery paths
      (invariants 2, 5 and 6).
- [ ] `p4b_shared_child_shipped_sink.py`, whose shared sink is a shipped `FileSink`, prints 4
      lines of 4 and 8 lines of 8, and `p4_shared_child_swap.py` prints `X.closes=1` and
      `X.events_after_its_close=0` on all four rows (invariants 2 and 5).
- [ ] `configure(MultiSink(A, B))`, one write, `configure(MultiSink(A, C))`, two writes,
      `shutdown()`: B is closed once by the swap, A and C once each at `shutdown()`, and A receives
      every event (invariant 5).
- [ ] `configure(sink_factory=lambda: A)`, one write, `configure(sink=A)`, two writes,
      `shutdown()`: A receives every event and is closed once, at `shutdown()` (invariant 5).
- [ ] `configure(MultiSink(A, A, B))`, one write, `configure(FilteringSink(A))`, one write,
      `shutdown()`: A is closed once, at `shutdown()`, and B once, by the swap (invariant 5).
- [ ] With spans, `configure(A)`, `configure(MultiSink(A, B))`, `configure(C)`, a write after
      each: the last swap closes A exactly once and B exactly once, and no close of A overlaps
      another (invariant 5).
- [ ] **Closed.** An unconfirmed swap from `MultiSink(A, B)` to `MultiSink(A, C)`, then
      `shutdown()` once the drain thread has ended: A, B and C are each closed exactly once and no
      close of A overlaps another (invariant 5).
- [ ] **Remaining.** With a retired worker's drain thread parked inside `A.emit` through
      `MultiSink(A, B)`, `configure(MultiSink(A, C))`, an orphan write, `shutdown(timeout=0.3)`:
      A is not closed while that thread is inside it, and once it has left a further `shutdown()`
      closes A exactly once (invariants 3 and 5).
- [ ] With A configured, a caller's own `MultiSink(A, B).close()` closes A and B, as at `5cb0f449`
      (invariant 10).

### FR-003: The shared sink is closed once, after its last write, on every twin path

#### Description:

FR-001 and FR-002 change the swap, the closer and the one release path, which every lifecycle path
runs through, so each path gets its own observable rather than an argument that it follows. In
every criterion below A is a sink both graphs hold, A refuses after `close()`, "forward" is
`configure(A)` then `configure(MultiSink(A, B))`, "reverse" is the opposite order, and the outcome
required is the same: A refuses no event, and `A.close()` is called exactly once, after the last
event A received.

**The guard: no take closes a sink that lies beneath one still in use.** FR-001 keeps a kept
sink out of the record, and one route brings it back. A log with no span reads the configured
sink, and arms it a moment later (`api.py`, `_ensure_sink()` then `_note_orphan_emit`). If a swap
that keeps that sink runs between the two, the arm lands after the join and the kept sink is a
root in the record again, where FR-001's five readers do not see the drain thread inside it.
Forced on the real path, with the arm held on an event until `configure()` had returned, the
unguarded mechanism then closed the kept sink under the drain thread at an expired
`shutdown(timeout=0.3)`, which took 10.01 s, and a later swap closed it before its drain, so 5 of
9 events arrived. Two rules close it, one at each place a sink is taken to be closed.

- **The closer's take.** After its take, `_close_owed` reads the graphs of the sinks the take
  left in the record, which is the keep's *remaining* read of FR-002, made here before any close
  is started. A taken sink that lies in one of them is not handed to a closer. It stays among the
  sinks the call did not hand off, so the put-back the function already has returns it to the
  record and discharges its entry, and a later call closes it once nothing holds it. The read is
  made only when the take left something behind.
- **The swap's stale take.** Before its first critical section, and only when the record holds
  more than one sink, `_swap_sink` reads the graph of each owed sink, with no lock held. In the
  stale loop it passes over an owed sink that lies in the graph of an owed sink something may be
  inside: the worker's current sink, or one `Runtime.held`. If such a sink was armed after the
  read, so that its graph is unknown, every stale sink is passed over. A sink passed over stays
  owed, as a held one does today, and a later swap or `shutdown()` closes it. Kept is tested
  first: a stale sink that is both kept and beneath a sink in use is ceded by FR-001, not passed
  over, since both of its holders then account for it.

**What the guard costs, and what it can wait on.** It adds no lock, no lock site and no second
put-back. At run time the closer's rule takes `_state._lock` once more, through the existing
put-back section, on a call that passed a sink over. `_close_owed` is called only from
`_shutdown_worker`, with no lock held, and it opens by taking that lock for its take, so the
put-back section can wait on nothing the take did not: `shutdown()` from a caller's thread, from
a sink's `emit` on the drain thread, from a `close()` or a setter, or from the exit drain reaches
both the same way. A setter runs under `config._sink_lock`, which orders before `_state._lock`
(the lifecycle frame's state map), and the section takes the close registry's leaf lock inside
`put_back`, the order the function already uses. A hand deferred onto the entry meanwhile is
reported by `put_back` and paid after the lock is released, which is the shape
`tests/test_lock_held_discharges.py` holds. Both reads are static and take no lock.

**The window the guard opens, recorded.** A sink the closer passed over reads as having a close
in flight from the take until the put-back, which is the length of that call's other closes. In
that window `Runtime.held` is true for it, so a swap passes it over, and a second `shutdown()`
counts it among the closes it waits for; that wait ends no later than the first call's own
closes, which the second call was waiting for already, and is bounded by its one closer grace.
The put-back section is also one more place a signal handler can interrupt a holder of
`_state._lock`, which is audit finding 3's existing item and not new in kind.

The lifecycle fixes of 2026-11-11 to 2026-11-13 sit beside this change, and their own tests are
the guard on the neighbouring behaviour: `tests/test_concurrent_configure.py` (two sink changes
serialized), `tests/test_shutdown_mid_swap.py` (a shutdown waits for a swap's decision, and the
fenced branch releases only what the record owes), `tests/test_reentrant_stop_signal.py` and
`tests/test_lock_held_discharges.py` (hands under no lock, a hand deferred into a close in flight,
the take-ahead rule and the declining take), `tests/test_factory_reentrant.py`,
`tests/test_ownership_record.py` (the record's bounds), `tests/test_fork_child_state.py`,
`tests/test_fork_mid_hand.py` and `tests/test_sinks_sqlite_fork.py`.

#### Acceptance Criteria:

- [ ] **An expired shutdown.** Forward, with spans, the drain thread parked inside `A.emit`, and
      A's `close` taking the lock its `emit` holds: `shutdown(timeout=0.3)` returns in under 1 s
      with `A.close()` not called and the timed-out line saying the sink is left open; once the
      emit has returned, a further `shutdown()` closes A exactly once (invariants 3 and 5).
- [ ] **Events queued at a later swap.** After the forward swap, three spans queued behind a slow
      `A.emit`, then `configure(C)`: A receives every queued event and is closed exactly once, by
      that swap, after the last of them (invariants 2 and 5).
- [ ] **The straddling log, at an expired shutdown.** With a log with no span held at its arm on
      an event until the forward `configure()` has returned, then released, and the drain thread
      then parked inside `A.emit` on an event: `shutdown(timeout=0.3)` returns in under 1 s,
      `A.close()` is not called while the drain thread is inside `A.emit`, and once the emit has
      returned and a further `shutdown()` has run, A's close count at process end is one
      (invariants 3 and 5).
- [ ] **The straddling log, at a later swap.** The same straddle, then three spans queued behind
      an `A.emit` parked on an event, then `configure(C)`: A receives every queued event, is not
      closed while the drain thread is inside it, and its close count at process end is one
      (invariants 2 and 5).
- [ ] A `shutdown()` that passes a sink over leaves it in the owed record with no close in flight
      for it when the call returns, and `tests/test_runtime_holder.py`'s lock sites and
      `tests/test_lock_held_discharges.py`'s recorded sites are unchanged (invariants 3 and 5).
- [ ] **Unconfirmed swap.** With a swap whose drain cannot be confirmed in its budget, forward and
      reverse: `health().incomplete_swaps` is 1, the unconfirmed-swap line is written as before,
      and the outcome holds at `shutdown()` (invariants 5 and 6).
- [ ] **A swap from sink code.** `configure(sink=A)` issued from a member's `emit` inside
      `MultiSink(A, B)`, on each delivery path, and from a stop-signal setter during the hand of
      `MultiSink(A, S)`: the outcome holds (invariants 5 and 6).
- [ ] **A shutdown during a swap.** With the swap parked in its first drain and a `shutdown()` on
      another thread, forward and reverse: the outcome holds and the owed record is empty once
      both calls have returned (invariants 5 and 6).
- [ ] **A swap during a shutdown.** With a `shutdown()` parked in another sink's close and the
      swap on another thread, forward and reverse: the outcome holds, and A's one close is the
      shutdown's (invariants 5 and 6).
- [ ] **The exit drain.** Forward and reverse with no `shutdown()` call: the outcome holds at
      interpreter exit, and in reverse B was closed by the swap (invariants 5 and 6).
- [ ] **A forked child.** With a `FileSink` as A, forward and reverse, a child forked after the
      swap writes three events and shuts down: all three are in A's file, the parent's later
      events still reach it, and the parent closes A exactly once. The child closes A once in
      reverse, and not at all forward, where A sits beneath the inherited wrapper that a child
      never releases (invariant 12).
- [ ] **Reconfiguration.** 200 rounds of `configure(MultiSink(A, B_i))`, a write, `configure(A)`,
      a write: A receives every event and is closed once, each `B_i` is closed exactly once and
      before `shutdown()`, and the owed record holds no more sinks after round 200 than after
      round 1 (invariant 5).
- [ ] **Concurrent callers.** Over at least 16 seeds, threads logging with and without spans and
      calling `flush()` run beside 150 swaps among graphs that all hold A, then a `shutdown()`
      races one more swap: the outcome holds, no close of A overlaps another, and no other sink has
      two closes overlapping or more closes than the wrapper holding it was armed, or than one
      where that wrapper was never armed (invariants 2, 5 and 6). A wrapper armed twice is owed two closes, made one after the
      other: a log with no span reads the wrapper, a swap takes it, and the log then arms it
      again, which is invariant 5's recorded exception for a sink written to after its close
      (SPEC-045, `p8_second_close_after_a_rearm.py`).
- [ ] A sink whose `close()` is its delivery and which keeps accepting afterwards delivers every
      event it took and is closed exactly once, forward and reverse (invariants 2 and 5).
- [ ] Every test file named in the Description passes with no edit to it (invariant 6).

### FR-004: What the read does not reach, and what the library cannot spare, is recorded with what a caller can do

#### Description:

A kept sink is closed by the graph that holds it. For a shipped wrapper that is the library's own
forward, so FR-001 and FR-002 are the whole story. For a wrapper the package does not ship it is
the wrapper's own code, and four shapes follow from that or from a fact that does not exist when
the swap decides. Each is recorded as a Known constraint in `docs/architecture.md` §13. The
fourth is also an open item in §12, because a sink the library wrote to and never closes is not a
limit to accept for good, and what would close it is known (FR-006). A fifth is what FR-003's
guard leaves, and is an open item in §12 too.

- **A `sink_factory` that has not built.** `configure(A)` then
  `configure(sink_factory=lambda: A)`: the wrapper holds nothing when the swap reads it, so A is
  released, and the factory later returns a closed sink. A factory builds a sink of its own
  (`docs/decisions/sink-lifecycle/`, *A sink factory builds each process's own sink lazily*).
- **A sink past the read's bound**: more than one plain container below its holder, or past
  `_MARKING_CEILING` objects entered.
- **A wrapper the package does not ship, which closes its member itself.** `Mine.close()` calls
  `A.close()` with its own code, which no rule in `release` can skip, and so does a subclass of a
  shipped wrapper that overrides `close()` to call its member's. Two things follow. Displaced
  while A is kept, `configure(Mine(A))` then `configure(A)`, it closes the live sink, as today.
  And wherever the record also owes A, `Mine` closes A beside the record's own close: after an
  unconfirmed `configure(A)` then `configure(Mine(A))`, and after FR-003's straddle with
  `Mine(A)`, `shutdown()` closed A twice, overlapping, with no event lost. `5cb0f449` does the
  same on the unconfirmed sequence, and on the straddle closed A three times and lost events.
- **A wrapper that leaves its member alone.** Wherever such a wrapper is the holder on whose
  account the library does not close a sink, the wrapper closes nothing, so the sink is closed by
  nobody. That is so when it is the installed graph: `configure(A)` then `configure(Leaves(A))`,
  or `configure(MultiSink(A, B))` then `configure(Leaves(A))`, ends with A never closed, where
  `5cb0f449` closed it once at the swap and lost the events after. It is also so when it is a
  sink still in the record: with a retired worker's drain thread parked inside `Leaves(A)`, then
  `configure(MultiSink(A, B))`, a write, an expired `shutdown()` and a further one, FR-002 skips
  the forward of A for `Leaves` and A is never closed. There `5cb0f449` closes A once, under
  that drain thread, whose two events A then refused on the author's run; this change closes it
  0 times and loses nothing. No event is lost in any of the three; the close is the caller's to make.
  Keeping the sink owed when a forward is skipped for a remaining holder was built. It closes A
  once behind `Leaves`, and twice, overlapping, behind a wrapper that closes its member, which
  closes it once without the rule. The library cannot tell the two wrappers apart, and the one
  that closes its member is the natural one to write, so the shape is recorded and not held.
- **A straddled kept sink can be handed a stop signal on its own.** FR-003's guard covers the two
  places a sink is taken to be closed. `Runtime.in_flight`, which decides whether a sink is
  handed a refreshed stop signal, still decides by identity, so a kept sink that a straddling log
  put back in the record can be handed one directly while the worker drains into it through its
  wrapper. No close is involved. This is read from the code and was not reproduced: it needs the
  straddle, then a `shutdown()`, then a further log with no span while a worker built since is
  draining. The hands are the stable stop signal's ground (Out of Scope).

On a fenced swap, and with no worker, the forward direction of a caller's wrapper is covered,
whichever way it closes: the read reaches into such a holder, so `configure(A)` then
`configure(Mine(A))` keeps A open, A joins the graph, and `Mine.close()` is its one close. A
wrapper that holds its events and writes them to A from its own `close()` finds A open, for the
same reason. Where A stays in the record instead, the third shape's second half applies.

In the first and third shapes every event the shared sink refuses is counted and announced
exactly as today, so invariant 2 holds and the loss is visible.

**What a caller who writes a wrapper can do**, in the code as this spec leaves it. Both routes
were measured on the prototype, in both directions and on both delivery paths, and neither lost
an event.

- **Build the wrapper on a shipped one and inherit its `close()`**: `class Mine(TransformSink)`,
  or `FilteringSink` or `MultiSink`. Its member then sits in a declared slot and its close goes
  through `release`, so the library closes the member exactly once in every sequence above.
- **Leave the member's close out of the wrapper's own `close()`, and close the member
  yourself** after `shutdown()`. The library then closes the member only when it is configured
  directly.

There is no third way, because a wrapper has no public means to declare its members to the
library. The build writes this into `README.md`, section *Writing your own sink*, in the passage
that begins "If you write a **wrapper** sink", as the two routes above and the sentence that a
wrapper which closes a sink it was handed must not have that sink configured directly after it.

#### Acceptance Criteria:

- [ ] On a fenced swap and with no worker: `configure(A)`, one write, `configure(Mine(A))`, two
      writes, `shutdown()`, for a `Mine` that subclasses `Sink` and for one that subclasses
      nothing, each closing its member itself. A is not closed before `shutdown()`, receives
      every event, and is closed exactly once with no overlapping close (invariants 2 and 5).
- [ ] After an unconfirmed `configure(A)` then `configure(Mine(A))`, and after FR-003's straddle
      with `Mine(A)`: A is not closed before `shutdown()` and refuses no event (invariant 2).
- [ ] The same sequence for a `Mine` that holds its events and writes them to A from its own
      `close()` before closing A, with every `A.close()` held open on an event the test sets
      later: A receives every event and is closed exactly once (invariants 2 and 5).
- [ ] `configure(Mine(A))`, one write, `configure(A)`, two writes: every event A refuses is
      counted, in `orphan_lost` with no span and by its batch in `failed_batches` with one, and
      announced (invariant 2).
- [ ] `configure(A)`, one write, `configure(sink_factory=lambda: A)`, two writes: every event A
      refuses is counted and announced in the same two terms (invariant 2).
- [ ] For a `Leaves` that never closes its member, `configure(MultiSink(A, B))` then
      `configure(Leaves(A))`, and `configure(A)` then `configure(Leaves(A))`, each with a write
      before and two after: A receives every event, no loss term moves, and the library never
      calls `A.close()` (invariant 2).
- [ ] With a retired worker's drain thread parked inside `Leaves(A)` on an event, then
      `configure(MultiSink(A, B))`, a write, `shutdown(timeout=0.3)`, the emit released, and
      `shutdown()`: A is not closed while that thread is inside it and refuses no event
      (invariants 2 and 3).
- [ ] For a `class Mine(TransformSink)` that inherits `close()`: `configure(Mine(A))`, one write,
      `configure(A)`, two writes, `shutdown()`, and the same forward, on both delivery paths. A
      receives every event and is closed exactly once, at `shutdown()` (invariants 2, 5 and 6).
- [ ] `docs/architecture.md` §13 carries the first four shapes, each with its sequences and its
      measured counts, the third's two overlapping closes and the fourth's 0 closes against 1
      among them; §12 carries the fourth as an open item (FR-006) and the fifth marked as read
      and not reproduced; and the README passage named above carries the two routes and the
      sentence (invariants 2 and 5).

### FR-005: The superseded pin is replaced by tests that hold a sink which refuses after close

#### Description:

SPEC-045 FR-002's third criterion pinned that the shared sink "must not become a loss", and its
test, `test_a_sink_shared_with_the_graph_that_replaces_it_is_not_stranded` in
`tests/test_owed_closes.py`, could not fail on the defect: its double keeps accepting after
`close()`, and it ran one direction. A test that claims to catch a bug must be run against that
bug.

The replacement is `tests/test_shared_sink_swap.py`. Its wrapper rows are derived from
`_fork._CHILD_SINK_SLOTS`, so a wrapper added later with no row fails the test rather than going
unchecked. The two wrappers that build their own inner sink, `LogstashSink` and `SentrySink`, are
exempted by name with that reason, and the `sink_factory` wrapper's row runs in reverse only,
because its forward direction is FR-004's first shape. The old test is kept, because the
keeps-accepting shape is the one for which a skipped close is the loss: its docstring is rewritten
to the claim this spec makes, it runs both directions, and it asserts one close.

Three source rosters ask this change for a row, and each is the roster working. The build adds
the rows and nothing else in those files. `tests/test_worker_predicate_roster.py` classifies the
swap's changed and added conditions, the guard's among them: the fenced branch's, the join's
`owes` and `held` pair, the stale take's test of what something may be inside, and the keep's
own condition. `tests/test_lifecycle_races.py` gains `cede` among the record's moves and a
disposition for each of its call sites, and the existing `_close_owed` put-back row's reason
gains the sink the guard passed over. `tests/test_child_sink_slots.py` gains an exemption for the
keep's record of sinks, beside `_CloseRegistry.owed` and `Worker._held`.

Three gates that might have asked for a row do not, measured on the prototype with the guard:
`tests/test_runtime_holder.py`'s lock sites, `tests/test_lock_held_discharges.py`'s sites and
`tests/test_lifecycle_inventory.py` pass with no edit, because the guard goes through the
put-back section that exists and stores nothing.

#### Acceptance Criteria:

- [ ] `tests/test_shared_sink_swap.py` runs every wrapper row in each direction the row has, with
      no span and with spans, twice: with a double that refuses after `close()` and counts its
      closes, and with a shipped `FileSink`. Each run asserts that every event written after the
      swap reached the destination and that every sink's close count is exactly one at the end
      (invariants 2, 5 and 6).
- [ ] A `_fork._CHILD_SINK_SLOTS` entry for a sink class that has neither a row nor a named
      exemption fails the test (invariant 6).
- [ ] Run against `5cb0f449`, every row fails on an event count or a close count, and the
      delivery doc records the counts it failed with (invariant 2).
- [ ] The old test asserts `closes == 1` and full delivery in both directions, and no sentence in
      `tests/test_owed_closes.py` says the swap closes a sink that is live in the new graph
      (invariant 5).
- [ ] The three rosters pass with the rows named in the Description added, and the diff to each
      of those three files adds rows and changes existing text only at a site FR-006's search
      returns or in the put-back row's reason (invariant 6).
- [ ] The delivery doc records the output of the three `p4` probes run from the repository
      against the built tree (invariant 2).

### FR-006: The settled record is rewritten in place

#### Description:

At build time the completion ritual rewrites each of these against the code as built, and dates
each with the day it is done.

- **`docs/architecture.md` §12, the item this spec closes.** *A sink can be released while it is
  still live inside the graph that replaced it* is struck in place, marked closed by SPEC-087,
  and moved under *Resolved*. Its rewrite states two corrections to the item's own **Closed by**
  sentence, each with this spec's *Evidence* as its reason. First, "not releasing a sink reachable
  from the graph just handed over" closes one direction only: in the other the sink released is
  the outgoing wrapper, which the new graph does not reach, and it is the wrapper's forwarded
  release that closes the live sink, so the closure is FR-001 and FR-002 together. Second,
  "decidable only against a live config read" is wrong as a requirement: the swap is handed the
  sink it installs and reads nothing else. A rule that does read the configuration at each
  forwarded release delivers the same events, and costs two things: `_swap_sink` then answers
  from module state rather than its argument, which five existing tests that call it directly
  fail on; and a later sink change answers for an earlier one, so a first swap's closer still
  inside a slow close closed the shared sink a second time after a second swap had displaced and
  closed it (2 closes against 1).
- **`docs/architecture.md` §12, three other items.** The item about a released sink handed back,
  which sits below the closed one, is left open and untouched. A new open item records FR-004's
  fifth shape and nothing wider: the stop-signal hand to a straddled kept sink, marked as read and
  not reproduced, with the sequence that would reproduce it and the stable stop signal as what
  closes it.
- **A second new open item in §12: a sink the library wrote to and never closes, behind a wrapper
  that leaves its member alone** (invariant 5; no spec yet). It is FR-004's fourth shape, written
  in the section's house style, with the build's own measurement beside the numbers below.
  - **The shape.** A caller's wrapper `Leaves(A)` whose `close()` does not close its member, as
    the holder on whose account the library does not close A: installed over a kept sink
    (`configure(A)` then `configure(Leaves(A))`, or `configure(MultiSink(A, B))` then
    `configure(Leaves(A))`), or still in the record with a retired worker's drain thread inside
    it while a later `configure(MultiSink(A, B))` is shut down.
  - **The numbers**, for the last sequence, on this spec's prototype:

    | Remaining holder | `5cb0f449` | This spec | With the member kept owed |
    |---|---|---|---|
    | `Leaves(A)` | 1 close, under the drain thread | 0 closes, no event lost | 1 close |
    | A wrapper that closes its member | 1 close, under the drain thread | 1 close | 2 closes, overlapping |

    For the two installed sequences: 0 closes, where `5cb0f449` closed A once at the swap and
    lost the events after it.
  - **The reproduction.** The build writes the last sequence as a probe with no harness, at
    `docs/audits/2026-11-14-probes/lifecycle/leaves_remaining_holder.py`, from FR-004's criterion
    for it, which is the second spec review's `leaves_remaining_retired` case: `configure(Leaves(A))`,
    a span; a second span parks the drain thread inside `A.emit` on an event;
    `shutdown(timeout=0.2)`; `configure(MultiSink(A, B))`, a log with no span;
    `shutdown(timeout=0.3)`; the emit released; `shutdown()`. It prints A's close count at the
    end and whether A was closed while the drain thread was inside it. Its docstring says it was
    written for SPEC-087 and is not one of the audit's own, the probes' `README.md` gains the
    same sentence, and the item quotes its output on the built tree.
  - **Closed by** the closer ordering a member after its holder: the member is kept owed when a
    forward is skipped on a holder's account, and closed after that holder's own close has
    returned, so that a wrapper which closes its member gives a second close in sequence, which
    a sink tolerates, and never two at once. That restructures `_close_owed`'s take and its
    fan-out, so it belongs in this arc's lifecycle command-path spec and not in a further patch
    to the closer.
- **`docs/architecture.md` §7 and §13.** In §7's *Sink-swap semantics*, step 4, "closes the
  previous sink.", gains the exception for a sink the new one still holds. §13 gains FR-004's
  first four shapes.
- **`docs/invariants.md` §5.** "the swap for the sink it replaced" becomes the swap for the sinks
  the graph it installs no longer holds. The entry gains the join and the one-close-per-decision
  rule, FR-004's shapes as recorded exceptions, and `tests/test_shared_sink_swap.py` under
  *Guarded*. SPEC-045's exception, a sink written to after its close is owed a second close,
  stays, with a clause saying a swap between graphs that share the sink no longer reaches it.
- **SPEC-045.** The superseded markers in `docs/specs/SPEC-045-every-owed-close-is-performed.md`
  and `docs/spec-delivery/SPEC-045-every-owed-close-is-performed.md` each gain "closed by
  SPEC-087" and no longer say "no spec yet".
- **Decisions.** One new file in `docs/decisions/pipeline/` carries the rule and its fences: the
  swap reads the installed graph once and a kept sink joins it; one decision closes a sink once.
  Never leave a kept sink in the record as a root, because the worker's readers decide by identity;
  never take, to close, a sink that lies beneath one still in use; never re-read the configuration
  at a release, because a later change then answers for an earlier one; never keep a stored
  per-sink count; never defer the displaced graph's release. In the same directory, *A shutdown
  waits for a swap in flight to decide, and a swap releases only what the record owes* has its
  fence and entry rewritten in place to add that the fenced branch cedes a kept sink rather than
  releasing it, with a dated History entry.
- **The rule `cede` and the guard end, at every site that states it.** Until this spec "every
  route out of the record is a take whose taker closes it", and a sink was never "neither owed
  nor in flight". Both are now false as written: `cede` is a route out with no close, a take the
  guard passes over ends in a put-back, and a ceded sink is neither owed nor in flight, by
  design. The sites are found by a search, not a list, because a list is what missed five of
  them. With line wraps and string continuations joined, search every tracked `.md` and `.py`
  under `docs/`, `src/`, `tests/` and `README.md`, outside `docs/audits/`, `docs/specs/` and
  `docs/spec-delivery/`, for the phrases `route out of`, `taker`, `means "taken"` and
  `neither owed nor in flight`, and for the registry's list of moves, written `arm, take`,
  `` `arm`, `take` ``, `arm/take/put_back` or `arms, takes`. As run:

  ```sh
  git ls-files docs src tests README.md | grep -E '\.(md|py)$' |
    grep -vE '^docs/(audits|specs|spec-delivery)/' |
  while read -r f; do
    tr -s '" \n' ' ' < "$f" |
      grep -oE 'route out of|taker|means taken|neither owed nor in flight|arm`?[,/] ?`?take|arms, takes' |
      sed "s|^|$f: |"
  done
  ```

  At `c321c260` that returns 30 matches in 10 files:
  - 4 in the decision this spec already rewrites, at its lines 25, 28 and 83;
  - 3 in `docs/decisions/pipeline/lifecycle-state-has-one-holder-runtime-and-state-observes-it.md`,
    at lines 21, 22 and 106, which hold the registry's list of moves;
  - 1 in `docs/decisions/pipeline/one-lifecycle-owner-the-bookkeeping-around-the-two-emit-paths-exists-once.md`,
    at line 44;
  - 2 in `docs/decisions/public-api/concurrent-sink-changes-are-serialized-by-one-lock-whose-wait-is-spent-from-the-swap-budget.md`,
    at line 81;
  - 9 in `src/log_foundry/_lifecycle.py`, in the docstrings of `_CloseRegistry`, `_close_owed`
    and `_swap_sink`;
  - 1 in `src/log_foundry/worker.py`, in `Worker.retarget`'s docstring at line 1054;
  - 4 in `tests/test_lifecycle_races.py`, 2 in `tests/test_worker_predicate_roster.py`, 1 in
    `tests/test_sink_release_roster.py`, and 3 in `tests/test_owed_closes.py`.

  Two of the 30 are not the rule: `tests/test_owed_closes.py`'s test name and message about
  concurrent takers, which stay. The build repeats the search on its own tree and rewrites every
  other site it returns in place, each decision file with a dated History entry, or says in the
  delivery doc why one stands. `Worker.retarget`'s `declined` verdict still follows from what it
  reads and its docstring says why: the sink it asks about is the worker's own, which is taken
  only once the drain thread has ended, and the guard passes a take over only for a sink still
  held.
- **Docstrings.** Beside the sites above: `release`, `_capture`, and `configure` in
  `src/log_foundry/config.py`, whose "the previous sink is closed" gains the exception.
- **`README.md`.** Two passages. In the `configure(...)` section, the sentence "The previous sink
  **is** closed, so do not hand it back to a later call." gains the exception for a sink the new
  one still holds. In *Writing your own sink*, the passage that begins "If you write a
  **wrapper** sink" gains FR-004's two routes and its sentence.
- **Release notes.** The pending notes for the next tag under `docs/release-notes/`, which the
  ritual creates if no earlier spec has, gain one entry under *Fixed*: a sink that the old and the
  new sink graph share is no longer closed by `configure()`; it stays open and is closed at
  `shutdown()`, at exit, or by a later change that drops it. The entry names what a caller can
  see: the loss lines of the two sequences are gone; a shared sink that delivers only when it is
  closed now delivers at `shutdown()` rather than at the swap, and `flush()` asks it sooner; a
  sink the new graph does not hold is closed by the swap as before; a caller's wrapper that
  leaves its member alone now leaves it unclosed where the swap used to close it; and the one new
  `_diag` line of FR-001.

#### Acceptance Criteria:

- [ ] Both SPEC-045 markers name SPEC-087 and neither still says that no spec closes the item,
      and the README's sentence "The previous sink **is** closed" and §7's step 4 each carry the
      exception for a sink the new one still holds.
- [ ] The §12 item is struck, names SPEC-087, and sits under *Resolved*; its rewrite states both
      corrections to its **Closed by** sentence and cites the evidence for each, and nowhere says
      that reading the configuration never closes the shared sink.
- [ ] §12 carries the new open item for FR-004's fifth shape, which names no close, and the item
      about a released sink handed back is byte-identical to the one at the commit before the
      build.
- [ ] §12 carries the open item for FR-004's fourth shape: it cites invariant 5, gives the shape,
      the table's numbers and the probe's output on the built tree, and its **Closed by** names
      the closer's ordering and the lifecycle command-path spec.
- [ ] `docs/audits/2026-11-14-probes/lifecycle/leaves_remaining_holder.py` exists, runs with the
      probes' documented command and no other file, and is named in the probes' `README.md` as
      written for SPEC-087.
- [ ] The new decision file has a fence within `DIGEST_MAX_BYTES` naming the five "never"
      clauses, and each decision file the build rewrites has a dated SPEC-087 History entry.
- [ ] The delivery doc records the search of the Description run on the built tree, its count,
      and for every match either the rewrite or the reason it stands; no match outside
      `tests/test_owed_closes.py`'s two still says that every route out of the record is a take,
      or that no sink is ever neither owed nor in flight.
- [ ] The release-notes entry exists and names each of the five caller-visible points.
- [ ] `sh scripts/gen-index.sh` leaves no diff, and `sh scripts/docs-lint.sh`,
      `sh scripts/spec-lint.sh` and `poetry run python scripts/docstring-lint.py` exit 0.
- [ ] Invariant 5: the page's statement, its recorded exceptions and its *Guarded* list match the
      code as built, so a reviewer checking the invariant reads no owner and no exception that no
      longer exists.

---

## Data Model

No public type changes. The private additions, in `src/log_foundry/_lifecycle.py`:

```python
def _walk_beneath(
    roots: list[object],
    *,
    ceiling: int = _MARKING_CEILING,
    read: Callable[[object, list[object]], list[object]] = _beneath,
) -> tuple[list[object], int, bool, Exception | None]:
    """``_capture``'s loop, factored out: what it found, how many objects it entered, whether
    it tripped and what it raised. ``_capture`` still returns the found list and that count.

    It announces nothing. ``_capture`` calls it and keeps ``_announce_capture``;
    ``_installed_graph`` calls it and writes the line of FR-001.
    """


def _installed_graph(
    root: object,
    *,
    ceiling: int = _MARKING_CEILING,
    read: Callable[[object, list[object]], list[object]] = _beneath,
) -> dict[int, object]:
    """The sink a swap installs and every sink ``_walk_beneath`` finds beneath it, by ``id``."""


class _Keep:
    """What one release of one decision must not close through a wrapper's forward (FR-002)."""

    kept: Mapping[int, object]      # the installed graph; empty for the closer's take
    remaining: Sequence[Sink]       # what the record still held after this section's takes
    closed: dict[int, tuple[object, object]]  # by id: the sink and its claim's token; shared

    def spares(self, sink: object) -> bool:
        """Whether a forwarded release of ``sink`` is skipped; records the sink when it is not."""


class _CloseRegistry:
    def cede(self, sink: Sink) -> None:
        """Takes ``sink`` out of the owed record with no close, under ``_state._lock``."""


def release(
    sink: Sink,
    *,
    detached: bool = False,
    owner: object = None,
    closing: _Close | None = None,
    keeping: _Keep | None = None,  # new: the decision's keep
) -> threading.Thread | None: ...
```

`_close_owed` asks its keep for the graphs of what remains before it starts a close (FR-003), and
`_swap_sink` reads the graph of each owed sink with `_installed_graph` before its first critical
section when the record holds more than one.

`keeping` is passed by `_swap_sink` and `_close_owed` only. `release` holds it for the length of
`sink.close()` on the performing thread, beside the depth `Runtime._releasing_here` already
carries, and a forwarded release (`owner` given) reads it there. A value is in a set when the
entry at its `id` is that very object. `_Keep` holds no lock. A forward claims with one
`dict.setdefault` of the sink and a token made for that call, and has the claim only if the
token it reads back is its own; a take's entry carries a token no forward holds. Two closer
threads that both read the remaining graphs read the same thing.

---

## API / Interface Contract

`configure()`, `shutdown()`, `flush()`, `health()` and every sink's `emit`, `flush` and `close`
keep their signatures and return values. What changes is when one close happens:

```python
lf.configure(sink=file)                      # events go to the file
lf.configure(sink=MultiSink(file, other))    # before: closed `file`; now: closes nothing
lf.configure(sink=file)                      # before: closed `file` and `other`; now: closes `other`
lf.shutdown()                                # closes `file`, once
```

## File & Folder Structure

```
src/log_foundry/_lifecycle.py           # _walk_beneath, _installed_graph, _Keep, and
                                        # _CloseRegistry.cede; _swap_sink, release, _close_owed,
                                        # _capture, and the two closer-thread bodies
src/log_foundry/config.py               # configure's docstring only
src/log_foundry/worker.py               # Worker.retarget's docstring only
tests/test_sink_release_roster.py       # one reason FR-006's search returns
tests/test_shared_sink_swap.py          # new: FR-001 to FR-005
tests/test_owed_closes.py               # the old pin, rewritten (FR-005)
tests/test_worker_predicate_roster.py   # rows, and the reasons FR-006's search returns
tests/test_lifecycle_races.py           # rows, and the reasons FR-006's search returns
tests/test_child_sink_slots.py          # rows only
docs/decisions/pipeline/                # one new decision; one rewritten in place
docs/architecture.md, docs/invariants.md, README.md, docs/release-notes/
docs/audits/2026-11-14-probes/lifecycle/leaves_remaining_holder.py   # new probe (FR-006)
docs/audits/2026-11-14-probes/README.md                              # names the new probe
docs/specs/SPEC-045-*.md, docs/spec-delivery/SPEC-045-*.md, docs/spec-delivery/SPEC-087-*.md
```

No file under `src/log_foundry/sinks/` changes behaviour: each shipped wrapper already forwards its
close through `release(member, owner=self)`.

## Implementation Phases

### Phase 1: The kept sinks, the join and the keep (FR-001, FR-002, FR-005)

- `_walk_beneath` factored out of `_capture`; `_installed_graph` with its line and its two
  arguments; `_CloseRegistry.cede`; the swap's join; `_Keep` through `release`, `_close_owed` and
  the closer threads.
- `tests/test_shared_sink_swap.py` with its direction, wrapper and path table, run red against
  `5cb0f449` first; FR-001's and FR-002's own criteria, the read's fault and trip among them; the
  old pin rewritten.
- The roster rows for what this phase adds, and the docstrings of the functions touched.

### Phase 2: The twin paths and the boundaries (FR-003, FR-004)

- The guard at the two takes, with the two straddle tests, the predicate-roster rows for the
  conditions it adds to `_swap_sink` and `_close_owed`, and the put-back row's reason.
- The expired-shutdown, queued, unconfirmed, re-entrant, mid-swap, mid-shutdown, exit, fork,
  reconfiguration and seeded tests.
- FR-004's criteria: the caller's wrappers in each direction, the unbuilt factory, the wrapper
  that leaves its member alone, and the two routes.

### Phase 3: The record (FR-006)

- §7, §12 with its two new open items and the new probe, §13, invariant 5, SPEC-045's two
  markers, the new decision file, every site FR-006's search returns, the README, the release
  notes, and the completion ritual.

## Evidence

Measured at `5cb0f449` with Python 3.13.1 on macOS, on throwaway copies of the tree that were not
kept. The instruments written for this spec: a matrix of 32 fresh-process cases (eight graph
shapes, two directions, two delivery paths) whose shared sink refuses after close; 19 twin-path
cases; and, after the first review, the reviewer's own 58-row matrix and two reproductions. The
suite at `5cb0f449` is 9,857 passed and 35 skipped.

| Candidate | Matrix cases with no loss | The shared sink's closes | Suite | What ran red |
|---|---|---|---|---|
| Unchanged tree | 0 of 32 | 2: at the swap, at shutdown | 9,857 passed | the audit's three probes: 1 of 4 and 2 of 8 lines, `X.closes=2` |
| Defer the outgoing graph's release until the shared sink's own | 26 | 2 at shutdown, from two owners | 9,854 passed | 200 reconfigurations left 201 sinks owed and no unshared member closed before shutdown; the shared sink was closed 201 times |
| The same, with one close per decision added | 26 | 1 in 18 of the 26 | 9,854 passed | still 201 owed and none of 200 members closed before shutdown |
| A stored per-sink reference count in the registry | 22 | 1 where the count was right | 9,814 passed, 43 failed, 2 errors | the count is stale wherever the graph changes after the arm, so a built `sink_factory` lost 2 of 3; arming must walk the graph, which `test_an_orphan_log_performs_no_stamp_walk` forbids; the other failures were not triaged |
| Read the live configuration at each forwarded release, on the first draft's record | 26 | as the first draft's, and one more when a later swap answers for an earlier one | 9,851 passed, 6 failed | the roster, and five tests that call `_swap_sink` without writing the configuration |
| Keep the installed graph and leave the kept sink owed (this spec's first draft) | 26 | 1 in 22 of the 26, 2 overlapping in the other 4 | 9,856 passed, 1 failed | an expired shutdown: 6.03 s against a 0.3 s timeout, and the kept sink closed under the drain thread; events queued at a later swap: 4 of 10 |
| **Chosen: keep the installed graph, the kept sink joins it, one close per decision, and the guard at the two takes** | 26 | 1 in all 26 | 9,854 passed, 3 failed | three rosters, each asking for a row (FR-005) |

The first three candidates and the first form of the fifth shared one harness, whose selector
and two unused fields fail three source-roster tests; those three are in the counts of the first
three and say nothing about the candidate.

**The live-read rival, corrected.** The first draft's row for it said the shared sink was never
closed and 266 tests failed. That was a defect in the prototype, which applied the skip to every
inline release, the shutdown's included. The review rebuilt it scoped to the swap's releases, and
that is the row above: the same matrix as the chosen candidate, all three probes clean, and six
failures. It is not simpler, because it needs the record's half of the keep as well, and it
re-asks at each forwarded release. With no worker, `configure(MultiSink(B, A))`, then
`configure(A)` returning at its budget while B's close is still running, then `configure(C)`:
the first swap's closer reached A after the second swap had displaced and closed it, read a
configuration that no longer held A, and closed it again. The carried keep closed it once.

**The blocking finding of the first review, before and after.** `configure(A)`, a write,
`configure(MultiSink(A, B))`, then `shutdown(timeout=0.3)` with the drain inside `A.emit`:

| Tree | The call took, A's `close` sharing `emit`'s lock | A closed while the drain was inside | A's closes at the end |
|---|---|---|---|
| `5cb0f449` | not reached: A, closed at the swap, refuses before the drain can park in it | 0 | 2 |
| First draft | 6.03 s | 1 | 2 |
| Chosen | 0.31 s | 0 | 1 |

The chosen candidate also printed 4 of 4 and 8 of 8 on both `p4b` probes and `X.closes=1` on all
four `p4` rows; held all 19 twin-path cases; held the concurrent case on 46 seeds, where the join
without the keep's closed set closed the shared sink twice on 12 of them; delivered 10 of 10 queued
events at a later swap; and closed a member shared by a held wrapper and a taken one once, after
the drain thread had left it, where `5cb0f449` closed it under that thread. On the review's 58 rows
no close of the shared sink overlaps another: 50 rows refuse nothing, 44 of them closing it exactly
once and 6 being a wrapper that leaves its member alone, and the other 8 are FR-004's first and
third shapes. Its six remaining matrix cases are FR-004's: the unbuilt factory on two paths, and a
displaced wrapper the package does not ship, in two forms on two paths.

Deferring only such a displaced wrapper was built too, on the first draft. It brought the matrix
to 30 of 32, and under 200 rounds of `configure(Mine(A))` and `configure(A)` it left 201 sinks
owed, closed no wrapper before shutdown, and called `A.close()` 201 times at shutdown, 199 of
them overlapping; without it the same rounds lose 399 of 401 events, counted. The owed record
growing with every reconfiguration is the defect SPEC-081 removed, so the deferral is Out of Scope
and the shape is recorded in §13 instead.

**The guard, without and with.** The straddling log forced on the real path, its arm held on an
event until the forward `configure()` had returned:

| | `shutdown(timeout=0.3)` took | A closed while the drain was inside | A's closes at the end | Queued events at a later swap |
|---|---|---|---|---|
| Without the guard | 10.01 s | 1 | 2 | 5 of 9, A closed under the drain thread |
| With it | 0.31 s | 0 | 1 | 9 of 9 |

The guard's first form put the passed-over sinks back in a section of its own before the closes.
It closed the sequence and failed two further gates, the lock sites and the lock-held discharges,
by adding a lock site and a second put-back. The form in FR-003 leaves them to the put-back that
exists, adds neither, and has the same suite result with and without it: the row above. The
chosen candidate's other numbers in this section were re-run with the guard in place and did not
move.

## Revision history

- 2026-11-15: drafted from audit finding 4 and `docs/architecture.md` §12, re-measured at
  `5cb0f449`. The three audit probes reproduced their recorded output exactly.
- **Three corrections to the audit's account, made while running it.** First, §12 names the
  closure as "not releasing a sink reachable from the graph just handed over". That covers one
  direction. In the other the sink released is the outgoing wrapper, which the new graph does not
  reach, and it is the wrapper's forwarded release that closes the live sink, so FR-002 is half of
  the closure. Second, §12 calls the rule "decidable only against a live config read". The swap is
  handed the sink it installs and reads nothing else. Third, the defect is wider than the two
  probed wrappers: `TransformSink`, a nested wrapper, two wrappers sharing a member and a built
  `sink_factory` wrapper all lost the same share, and an unconfirmed swap, a shutdown during a
  swap and a swap from a stop-signal setter each closed the shared sink more than once.
- 2026-11-15, two answers from the orchestrating session before the spec review. The displaced
  wrapper that closes its own member stays a recorded constraint, and the deferral stays Out of
  Scope; FR-004 gains what a caller can do, measured, and names the README passage. The released
  sink handed back gets its own open item in `docs/architecture.md` §12 and its probe, and FR-006
  names the two corrections the build writes into the struck item.
- 2026-11-15, spec review 1 (build frame): one blocking finding and six others, each answered.
  - **Blocking, fixed by changing the mechanism.** A kept sink left in the record was closed by
    an expired `shutdown()` under the drain thread. FR-001 now takes it out of the record, and
    names the five readers that share the blind spot. The same blind spot had a second
    reproduction the review did not have, the queued events, which FR-003 now holds.
  - **Finding 2, fixed on a fenced swap and with no worker, by the same change.** There a
    forward `configure(Mine(A))` no longer closes A twice: A joins the graph and `Mine` makes
    its one close. ~~Fixed~~ on every path was an overclaim, corrected by review 2: where A
    stays in the record the two closes remain, and FR-004 records them. The Out of Scope
    sentence about close ordering is gone.
  - **Finding 3, fixed.** FR-002 states when each part of the keep is read, and holds the
    review's sequence. The join made a second double reachable, two owed wrappers sharing a
    member, which is the keep's closed set.
  - **Finding 4, answered by removing the state.** The read's line is written each time and
    nothing is stored, so there is no home to choose and no inventory row; FR-005 predicts the
    three roster rows the change does add.
  - **Finding 5, fixed.** The path is gone on the forward swap, and the criterion that replaces
    it holds the close open on an event.
  - **Finding 6, accepted.** The live-read row was a prototype defect. *Evidence* gives the
    corrected comparison, and FR-006 no longer has the build write that the rival never closes
    the shared sink.
  - **Finding 7, fixed.** FR-006 names §7's step 4. A wrapper that leaves its member alone is
    recorded as a limit in FR-004 with its measurement, and in the release notes.
  - **Cosmetics, fixed.** The read's line is in Phase 1; the read has its own seam; the Data
    Model names the shared walk; the two guesses are stated in FR-002.
  - ~~**Not taken.** Guarding the closer's take against a kept sink armed again by a straddling
    orphan log.~~ Taken, in the next entry.
- 2026-11-15, the orchestrating session's answer on the residue: guard it in this spec. FR-003
  gains the guard at the two takes, the two forced straddle criteria, what the guard costs and
  can wait on, and the window it opens. FR-004's fifth shape narrows to the stop-signal hand,
  which is read and not reproduced, and FR-006's §12 item describes only that. FR-005 names the
  rows the guard adds and the three gates it leaves alone.
- 2026-11-15, spec review 2 (build frame): the mechanism held on an independent build, and what
  it found is text.
  - **Blocking, fixed.** FR-004 claimed a forward caller's wrapper is covered whichever way it
    closes. That holds on a fenced swap and with no worker. After an unconfirmed swap, and after
    the straddle, a wrapper that closes its member closes it beside the record's close: two
    overlapping closes, no event lost, re-run on the author's prototype and recorded in the third
    shape.
  - **Finding 2, fixed.** FR-006 now states the search that finds every site of the rule `cede`
    and the guard end, with its count, 30 matches in 10 files, in place of a list that had missed
    five of them.
  - **Finding 3, recorded and not held.** A wrapper that leaves its member alone, as a holder
    still in the record, leaves the member closed by nobody: 0 closes against `5cb0f449`'s 1,
    which is made under the drain thread and loses two events. Keeping the member owed was built
    and closes it once there, and twice, overlapping, behind a wrapper that closes its member.
    FR-004's fourth shape now states the whole of it.
  - **Finding 4, fixed.** FR-002 says when the remaining list is worked out and why, and FR-003
    says a stale sink that is both kept and beneath a sink in use is ceded.
  - **Cosmetics, fixed.** The walk returns its visit count; the closed set stores a claim token;
    the read's line names no sink change; each phase carries the roster rows for what it adds.
    The spec's swap-during-shutdown criterion rests on the author's case, which hands nothing
    back, not on the review's first-round case, which did.
- 2026-11-15, the orchestrating session's last answer: item 3 of review 2 stays unheld in this
  spec, and must not end as a limit nobody is asked to fix. FR-006 now has the build file
  FR-004's fourth shape as an open item in `docs/architecture.md` §12 as well as a §13
  constraint, with both numbers, a probe of its own and a **Closed by** that names the closer's
  ordering and the lifecycle command-path spec.
- 2026-11-15, the build: FR-003's concurrent criterion corrected. It asked that no sink but A be
  closed twice. Under four logging threads, two of them with no span, a seed sometimes closed an
  unshared member twice, in sequence and never overlapping: a log with no span read the wrapper
  as the configured sink, a swap took that wrapper, and the log then armed it again and wrote into
  it before its detached close ran, so a later decision closed it a second time. That is the
  re-arm invariant 5 already records as owing a second close (SPEC-045). With each member's
  closes held to its wrapper's arms, counted by wrapping the registry's `arm`, the test passed 6 of
  6 serial and 5 of 5 parallel runs of its 16 seeds, where the unchanged assertion had failed on 3
  of 6 serial runs. The criterion now holds each member to its wrapper's
  arms and to no overlap, and every assertion on A is unchanged.
