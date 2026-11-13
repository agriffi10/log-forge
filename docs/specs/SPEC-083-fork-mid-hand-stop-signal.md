---
type: Spec
id: SPEC-083
title: "Fork Mid-Hand: A Child Forked Inside a Stop-Signal Setter Has Its Backoff Cut Short and Never Re-Enters the Setter"
description: "Makes a forked child's shutdown() cut short a backoff on the stop signal a sink was being handed when its setter forked, and keeps the child from ever running that setter again inside the call that forked it, for applications whose sinks start a process from their log_foundry_stop_signal setter."
spec_status: Completed
depends_on: [SPEC-027, SPEC-033, SPEC-039, SPEC-054, SPEC-068, SPEC-079, SPEC-081]
---

## Overview

The library hands each sink a stop signal through the sink's `log_foundry_stop_signal` setter, so a
`shutdown()` can cut a sink's backoff short (SPEC-027, SPEC-033 FR-004). The setter is the sink's
own code. If it forks, for example by starting a fork-context `multiprocessing.Process`, and the
child leaves through `os._exit`, then the child is still inside that setter call for its whole life.
Two things go wrong in such a child at `53b68203`.

**The child's `shutdown()` does not cut that sink's backoff short.** The child is still marked as
handing that sink its signal (`handing_here(sink)` is true), so every later decision to hand it the
child's own signal skips it. The outer hand's settle loop would have handed it, but it never runs.
The sink keeps the parent's event, the child's `shutdown()` sets only the child's signal, and a wait
on the event the sink holds runs its full length. Measured at `53b68203` with a sink that does not
subclass `Sink`, whose setter stores the event and forks a child, and a 5 s wait on the held event
in that child: the wait runs 5.00 s after the child's `shutdown(timeout=1)`. A sink that subclasses
`Sink` is not affected, because the child's repair walk replaces the event it holds with the
child's own signal (invariant 12; measured 0.00 s).

**On the worker paths the child runs the setter again, inside the setter call that forked it.** A
child forked while a worker build or `Worker.retarget` is handing the sink its signal inherits a
live worker. The fork handler resumes that worker, and `Worker._reinit_after_fork` hands the sink
the child's signal by running the setter, inside the fork handler, nested in the call that forked.
On the retarget path the child's first log then runs it once more, because that hand was never
marked. A setter that holds its own `threading.Lock` across its body is reasonable code, and it
hangs the child on both paths (measured at `53b68203`; the child never reaches its first line).

This spec fixes both, on Andrew's standing direction to harden rather than record. The library
records the event it is offering on the offering thread for the length of each offer. A child's
fork handler keeps the forking thread's offered events and makes them safe to set, and the child's
`shutdown()` sets them. A sink that a thread is in the middle of being offered is treated, by every
decision to hand it again, as one that thread is already handing. In a child, every thread treats
the sink the forking thread is still being offered that way. The resumed worker's hand in the fork
handler skips such a sink too. No setter, and no other sink code, runs in the new handler
code. The child's mark is deliberately kept: clearing it is the obvious fix, and it makes the child
re-enter the setter (FR-004 records why it is rejected).

## Scope

### In Scope

- A per-thread record of the stop signals being offered, pushed and popped by `offer_stop_signal`
  around the probe and the setter (FR-001).
- The child's fork handler capturing the forking thread's offered events into
  `Runtime._stale_signals` and re-initialising each event's internal condition (FR-002).
- The child's `shutdown()` setting those events, in the section where it sets its own signal, on
  both of its branches (FR-003).
- `handing_here` answering true for a sink on the offer record and, in a child, for a sink the
  forking thread was still being offered, on every thread of the child. Also, the resumed worker's
  hand in the fork handler skipping a sink `handing_here` reports. The child's handing mark stays
  as it is (FR-004).
- The docstrings that say a forking thread's own `finally` clears its entries, including
  `_building_here`'s, which gets a docstring correction and no behaviour change. Also the inventory
  rows, `architecture.md` §13, invariants 3 and 12, a decision file and the 1.2.0 release notes
  (FR-005).

### Out of Scope

- **Resetting `_handing_here` in the child.** This was the first fix proposed. FR-004 records the
  measurement that rejects it.
- **A setter that forks before it stores the offered event.** That sink keeps whatever it held
  before the call, and the child's `shutdown()` does not set it unless the repair walk replaced it.
  The library sets only the events on the forking thread's offer record. It keeps no per-sink record
  of earlier hands, and keeping one would mean holding a reference to every sink ever handed a
  signal, which SPEC-081 bounded the ownership record to avoid. This is a recorded limit
  (`architecture.md` §13, FR-005).
- **A signal handed to that sink again after the child's `shutdown()`.** After the child's
  `shutdown()`, the sink keeps the event that shutdown set for the rest of the child's life. Its
  later backoffs on that event do not wait, which is SPEC-033 FR-004's tight-retry shape, bounded by
  the sink's own retry count. A sink that subclasses `Sink` in the same child already does this at
  `53b68203`. Measured: after the child's `shutdown()`, an `info()` and a `flush()`, the held event
  is still set and still the one the sink held. Handing it a fresh signal means running its setter
  inside the call that forked, which FR-004 forbids. This is a recorded limit (`architecture.md`
  §13, FR-005).
- **A child that unwinds out of the setter needs nothing beyond FR-004's re-owe**, for example a bare
  `os.fork()` whose child returns from the setter. Its own `finally` blocks pop the offer record
  and the handing mark. What it then holds depends on the path. This was measured at `53b68203`,
  with and without this spec's mechanism patched in:
  - **Orphan path:** the outer hand's settle loop hands it the child's signal in both cases, as
    `test_a_setter_that_forks_leaves_the_child_its_own_signal` in
    `tests/test_reentrant_stop_signal.py` already holds.
  - **Worker-build path:** the settle loop of `_hand_out` hands it the child's signal in both cases.
  - **Retarget path:** `_hand_retargeted` has no settle loop. At `53b68203` the child holds its own
    signal only because the fork handler ran the setter again, which FR-004 removes. With this
    spec's mechanism, the child holds the parent's event until its first orphan log or `flush()`
    settles the fork's signal debt, and then it holds its own. Its `shutdown()` before that point
    still cuts the wait short, through FR-003's set. Measured wait: 0.00 s in both cases. Another
    thread of the child may pay that debt while the forking thread is still inside the setter. If
    it does, the debt is re-owed as the forking thread leaves the offer (FR-004), so the later log
    or `flush()` still hands the sink its own signal.
- **A grandchild forked by another thread of a child that is still inside a lock-holding
  setter.** That grandchild hangs on its first log. `_clear_after_fork` replaces `_fork_offers`
  with the new forking thread's list, so the grandchild does not inherit the child's view of the
  sink still being offered. The execution review of the cross-thread view found it hanging, and
  found the same at `d3950848`. It is not closed, because the control hangs too: with no
  fork inside a hand at all, one thread inside a locked setter and another thread forking also
  dies on `SIGALRM`. That is the generic hazard of forking while another thread holds a lock the
  caller's code owns, and no record can close it, so carrying the old prefix would fix only one
  shape of it. This is a recorded limit (`architecture.md` §13, FR-005).
- **Behaviour change for `_building_here`.** A child forked inside a sink factory keeps the factory
  build's entry, and that is harmless (FR-005 records why). Only its docstring changes.
- **An offered event whose type is not exactly `threading.Event`.** The capture keeps only exact
  `threading.Event` objects, so no `set` or re-initialisation that sink code overrides runs. Every
  event the library offers is one it built itself.
- **Running sink code in the fork handler in any new place.** The resumed worker's hand to a sink no
  thread was offering stays as SPEC-039 and SPEC-068 FR-005 left it.

---

## Functional Requirements

### FR-001: Every offer is recorded on the offering thread for its length

#### Description:

`offer_stop_signal(sink, stop)` is where the library runs a sink's setter. The package wrappers'
own setters assign inner sinks' setters directly, but they run nested inside an outer offer, so
that outer offer's record already covers them. Before the `hasattr` probe, the function pushes
`(id(sink), stop)` onto this thread's list in `Runtime._offering_here`, and it pops the entry in a
`finally`. It is a `threading.local` for the reason `_handing_here` gives: it records a call on this
thread's stack. Holding the `id` rather than the sink keeps no sink alive in a child that never pops
the entry. The record is never reset, so a child inherits the forking thread's record as that
thread left it (FR-002 reads it).

#### Acceptance Criteria:

- [ ] Inside a setter called through `offer_stop_signal(sink, E)`, this thread's record ends with
  `(id(sink), E)`. Another thread's record is empty at that moment.
- [ ] Inside a setter that itself calls `offer_stop_signal` on an inner sink, as the factory
  wrapper's does, the inner setter sees both entries on the record, the outer one first.
- [ ] After `offer_stop_signal` returns, after the setter raises an `Exception` (which is absorbed),
  and after the setter raises `KeyboardInterrupt` (which propagates), the record is as it was
  before the call.
- [ ] Invariants 3 and 12: the record is what lets a forked child reach the signal it was being
  offered.

### FR-002: A child keeps the forking thread's offered events, safe to set

#### Description:

`_clear_after_fork` runs in the child on the forking thread. It sets `Runtime._stale_signals` to the
events on that thread's offer record, keeping only objects whose type is exactly `threading.Event`.
It replaces whatever a grandparent left, so a grandchild forked from another thread starts empty.
It calls `_at_fork_reinit()` on each kept event. That is CPython's own after-fork method, and it
re-initialises the condition `Event.set` takes, which a parent thread may have held at the fork.
Typeshed does not declare the method, so the call carries a
`# type: ignore[attr-defined]` directive. The handler reads only the thread-local list. It runs no
setter, probe or other sink code, and it leaves `_offering_here` and `_handing_here` as they are.

Measured at `53b68203` with a parent thread holding the offered event's condition across the fork:
without the re-initialisation the child's `shutdown()` never returns, and with it the child
returns at once.

#### Acceptance Criteria:

- [ ] A child forked inside a setter called with `E` has `_stale_signals` holding `E` itself (by
  identity) and nothing else.
- [ ] A child forked by a thread that is inside no offer has `_stale_signals == []`.
- [ ] A grandchild forked from a thread of that child other than the forking one has
  `_stale_signals == []`, and a fresh thread of that grandchild reads `handing_here(sink)` false.
  A grandchild forked by the child's still-inside-the-setter thread holds `E` again, and a fresh
  thread of it reads `handing_here(sink)` true.
- [ ] An offer of a `threading.Event` subclass instance is recorded by FR-001 and left out of
  `_stale_signals`.
- [ ] With a parent thread holding `E._cond` across the fork, the child's `shutdown(timeout=1)`
  returns within the test's bounded wait. With the `_at_fork_reinit()` call removed, the child
  reports nothing and the test fails on its bound.
- [ ] Invariant 12: the child never hangs on a lock a parent thread held, and the handler runs no
  sink code.

### FR-003: The child's shutdown sets the signals it was being offered

#### Description:

`_shutdown_worker` sets each event in `_stale_signals` inside the critical section where it sets
`_state._stop`. That section is shared by both branches, so a `shutdown()` with a live worker and
one without both reach the same set. The `atexit` drain binds the same function. Setting a stale
event affects only the child, because the event is the child's private copy of a parent object.
The only sinks that see it are sinks still holding it, and the child's shutdown should cut those
short anyway.

For a sink that subclasses `Sink` nothing changes. When the offered event was the parent's current
signal at the fork, the repair walk already gave that sink the child's signal. When it was not, the
parent's signal had moved on, which happens only once an event is set, so the copy the sink holds
is set already.

Measured at `53b68203` with this spec's mechanism patched in, using a sink that does not subclass
`Sink` and a 5 s wait in the child on the event the sink holds: 0.00 s after `shutdown(timeout=1)`
on the orphan path, the worker-build path and the retarget path, and on the orphan path with the
sink inside a `FilteringSink`. Unpatched, the orphan path and the wrapped orphan path wait 5.00 s.
The two worker paths read 0.00 s unpatched only because the fork handler runs the setter again,
which FR-004 removes; once it is removed, this FR's set is what cuts their wait short.

#### Acceptance Criteria:

- [ ] A sink that does not subclass `Sink` stores the event and forks a `run_in_child` child from
  its setter during an orphan log's hand. A thread in that child waiting 5 s on the held event
  returns within 1 s of the child's `shutdown(timeout=1)` returning. This fails at `53b68203`.
- [ ] The same holds with that sink inside a `FilteringSink`, and with the hand made by a worker
  build (the child has a live worker, so `shutdown()` takes its worker branch) and by
  `Worker.retarget`.
- [ ] With the capture in `_clear_after_fork` removed, the first criterion's wait runs its full
  5 s. With the set in `_shutdown_worker` removed, the same.
- [ ] With FR-004 in place and the set in `_shutdown_worker` removed, the worker-build and
  `Worker.retarget` rows of the second criterion also wait the full 5 s. At `53b68203` those rows
  pass only because the fork handler re-runs the setter, so this is what makes them guard FR-003.
- [ ] A child that unwinds out of a setter called by `Worker.retarget` (a bare `os.fork()`): its
  `shutdown()` cuts a wait on the event the sink holds short, and after a `flush()` the sink holds
  the child's `_state._stop`.
- [ ] The same child with another of its threads calling `flush()` first, while the forking thread
  is still inside the setter: that thread returns and the debt reads cleared. After the forking
  thread unwinds, a `flush()` still gives the sink the child's `_state._stop`. Removing FR-004's
  re-owe turns this row red (`own=False`) and no other.
- [ ] After the child returns, the event the sink holds in the parent is not set.
- [ ] A second `shutdown()` in the child returns normally.
- [ ] Invariants 3, 6 and 12: a shutdown cuts the wait short on both `shutdown()` branches and on
  every hand path that can fork, and the parent is untouched.

### FR-004: A child never runs a sink's setter inside the setter call that forked it

#### Description:

Three paths could run the setter again in such a child, and so could any thread of the child
other than the forking one. Each needs a guard.

**The orphan path keeps the child's handing mark.** It would be natural to clear `_handing_here`
in the child, as SPEC-081 cleared the release depth. That is rejected. The mark is true: the
child's code runs from the setter's frame, so clearing it makes the child's first log run the
setter inside the setter call that forked. Measured at `53b68203` with a setter that holds its own
`threading.Lock` across its body: without the reset the child returns, and with it the child hangs
on the sink's own lock. The starting session also measured, at `c21755f1`, that with the reset a
child that unwinds gets a nested setter call it never had.

**`handing_here` also answers from the offer record.** `Worker.retarget`'s hand
(`_hand_retargeted`) calls `offer_stop_signal` without the `handing` mark. Its child's first log
therefore found `handing_here` false and handed the sink again: two setter calls in the child, and
a hang for a lock-holding setter, measured at `53b68203`. So `handing_here(sink)` is true when the
sink is under `handing` or on this thread's offer record.

**Every thread of the child sees the offers the child inherited.** The mark and the offer record
are per thread. In a child that leaves through `os._exit`, any thread other than the forking one
read neither, so it could hand the sink again while the setter's frame was still live, and a
setter holding its own lock hung that thread for the child's life. Two cases were measured at
`d3950848`, the build before this fix:
- another thread making the child's first log, which settles the fork's signal debt, read
  `T_alive=True child_calls=1`;
- the resumed drain thread, reaching the built sink through a factory wrapper's build, read
  `emits=0 calls=1 cut=False fast=False`, and the child's `shutdown()` spent its whole 1 s.

So `_clear_after_fork` stores the forking thread's live offer list in `Runtime._fork_offers` and
its length in `Runtime._fork_inherited`, both rewritten on every fork. `handing_here` also answers
from `_fork_offers[:_fork_inherited]`, comparing ids only, on every thread. The live list rather
than a copy is stored so that the count can follow it down: `offer_stop_signal`'s `finally` lowers
`_fork_inherited` whenever the forking thread pops below it. A child that unwinds out of the setter
therefore stops sharing that offer, and an offer the forking thread makes after the fork lands
beyond the count, so it is never shared.

**Leaving an inherited offer re-owes the fork's signal debt.** While the forking thread is inside
the offer, another thread of the child that pays the debt, with its first orphan log or `flush()`,
leaves the sink out because `handing_here` is true for it, and then clears the debt. On the
retarget path, which has no settle loop, nothing then handed the sink the child's own signal once
the forking thread returned. Measured at `471eef5e`: `alive=False debt=False before=False own=False
cut=True`. Invariant 3 still held. So where `offer_stop_signal`'s `finally` lowers
`_fork_inherited`, it also sets `_state._signal_owed`, with both writes in one `_state._lock`
section. A payer computes its candidates and clears the flag in one locked section, so either it
sees the sink as left and hands it, or it ran first and its clear is undone. That branch runs only
in a child, on the forking thread, once per inherited level. No caller holds `_state._lock` across
an offer, because a setter never runs under a lifecycle lock; this was checked by reading every
caller. The lock-site roster in `tests/test_runtime_holder.py` gains the one site.

Each caller of the predicate takes one of two readings:

| Caller | Site at `53b68203` | Reading |
|---|---|---|
| `_will_hand`, and through it `_hand_after_close`, `_arm_and_hand`, `_owed_candidates`, `_swap_sink` and `_flush_live_sink` | `_lifecycle.py:4095` | widened: `handing_here`, which is the mark, this thread's offer record, and a child's inherited offers |
| `_get_worker`'s build | `_lifecycle.py:3597` | widened: `handing_here` |
| the factory wrapper's `_FactorySink._build` | `sinks/_factory.py:204` | widened: `handing_here` |
| `Worker._reinit_after_fork`'s new skip | `worker.py` | widened: `handing_here` |
| `_pay_marks_after_stop`'s parking of a stop-end payment | `_lifecycle.py:3239` | the `handing` mark alone: `_in_handing(sink)`, which is `id(sink) in _mid_hand()`. It reads neither offer record, including a child's inherited one |

The stop-end payment keeps the mark alone because `handing()`'s exit is the only thing that pays a
parked payment (`payment_owed`). A payment parked on a widened read inside `_hand_retargeted`'s
offer, which runs outside any `handing` block, would have no exit to pay it. So the payment is
unchanged from `53b68203`.

No criterion pins that choice, because no reachable state tells the two readings apart. The
readings differ only for a payment made on a thread inside an offer that has no `handing` block,
about the sink offered.

Four offers have no `handing` block:
- `_hand_retargeted`'s;
- `Worker._reinit_after_fork`'s, in a child's fork handler;
- the factory wrapper's `_forward_stop_signal` of its built sink;
- `Worker.__init__`'s, when a caller builds a worker with `offer_signal=True` (the library passes
  `False`).

An earlier version of this paragraph named only the retarget, which was incomplete. A payment
reaches the predicate only with a live, draining worker, no stop running, and a close that the stop
marked. It then decides about that worker's sink. The four cases, argued from the code:

- **Inside the retarget's offer.** That worker is the process's only one: `_state._worker` is
  assigned at one site, `_get_worker`'s build, and never cleared. Every call of
  `_pay_marks_after_stop` such a thread can make comes from its own `shutdown()`. That shutdown
  takes the worker branch and retires that worker before its `finally` asks for the payment, so the
  payment returns at its liveness check, with `_stop` set.
  
  The other entries to the payment do not apply. `_pay_a_mark_the_stop_missed` is reached only by a
  writer that marked, and such a thread can be neither kind of writer: it cannot build, because a
  worker exists, and it cannot retarget, because its `configure()` finds `config._sink_lock` held
  and swaps unconfirmed. `handing()`'s exit pays only what an earlier parked payment left.
- **In the fork handler.** `_clear_after_fork` has already dropped every in-flight close with its
  mark. A new mark needs a stop, and that stop's `shutdown()` retires the worker in the same way.
- **The factory wrapper.** It offers its built sink, but the worker's sink is the wrapper itself,
  so the readings could differ only for a factory whose built sink is also the worker's sink. No
  ordinary configuration makes one. That inner offer also runs nested inside an offer of the wrapper
  itself, which is one of the other cases.
- **A worker a caller builds directly.** It is not the process's worker, so no payment decides
  about its sink.

`_in_handing`'s docstring carries this argument. Only the retarget case was measured.

The authoring session probed this at `53b68203` with both readings patched in. In
`probe_payment2.py` the setter called by `Worker.retarget` calls `shutdown()` while the sink's
earlier close is still in flight. `probe_payment3.py` adds a second thread's `shutdown()` and a
worker build injected inside the setter's own stop. In every run, every payment call found no live
worker and a set `_stop`, and the two readings produced the same result. A mutant that gives the
payment the widened predicate is therefore equivalent. The reading is set by this table rather
than by a test.

**The resumed worker skips a sink the forking thread is offering.**
`Worker._reinit_after_fork(resume=True)` offers the child's signal to `self.sink` only when
`_lifecycle.handing_here(self.sink)` is false. Otherwise the sink keeps the event it held, and
FR-003 sets that event at the child's shutdown.

Measured at `53b68203` with all of this patched in by monkeypatch: the child ran the setter zero
times and returned on all three paths, both with a lock-holding setter and without one. The full
suite also passed under that patch, which gave the payment the widened read too.

#### Acceptance Criteria:

- [ ] On the orphan path, a setter holding its own `threading.Lock` across a body that forks a
  `run_in_child` child, which logs, flushes and shuts down: the child returns within its bound and
  reads `handing_here(sink)` true. Resetting `_handing_here` and `_offering_here` in the child's
  handler, after the capture, makes the child fail on its bound.
- [ ] The same setter on the worker-build path: the child returns and the setter ran zero times in
  the child. With the skip in `Worker._reinit_after_fork` removed, the child fails on its bound.
- [ ] The same setter on the `Worker.retarget` path: the child returns and the setter ran zero
  times in the child. With the offer-record read removed from `handing_here`, the child fails on
  its bound.
- [ ] Each of the three paths above, with the child's first log made from **another** thread of
  the child: that thread returns within its bound, and the setter ran zero times in the child.
  Removing the `_fork_offers` read from `handing_here` leaves that thread hung on the setter's lock.
- [ ] A sink built through `sink_factory=` whose setter, holding its own lock, forks from the
  parent's drain thread: in the child, the resumed drain thread delivers the batch, and the setter
  ran zero times. The batch's backoff on the kept event ends at the child's `shutdown()`, which
  returns in under 0.5 s.
- [ ] In a child still inside the setter for S that offers another sink R, another thread reads S
  as being offered and R as not. Removing the `[:_fork_inherited]` bound makes it read R as being
  offered.
- [ ] In a child that unwinds out of the setter for S (a bare `os.fork()`) and then offers R from
  the same thread, another thread reads neither S nor R as being offered. Removing the shrink in
  `offer_stop_signal`'s `finally` makes it read R as being offered.
- [ ] A child forked by a thread inside no offer, with a live worker on a sink that does not
  subclass `Sink`, still has its resumed worker hand that sink the child's signal: the sink holds
  `_state._stop` after the fork.
- [ ] `tests/test_reentrant_stop_signal.py` and `tests/test_fork_lifecycle.py` pass with their
  assertions unchanged.
- [ ] Invariants 3, 6 and 12: no thread of a child waits forever on a lock its own setter frame
  holds, on any hand path.

### FR-005: The record says what a mid-hand child keeps, and why

#### Description:

The docstrings of `Runtime._handing_here` and `Runtime._building_here` say that a forked child keeps
the forking thread's entries, "which that thread's own `finally` removes". That is true only for a
child that unwinds. `_handing_here`'s docstring gains why the child keeps the mark (FR-004).

`_building_here`'s docstring gains why the stale entry is harmless. This was measured at
`53b68203` with a child forked inside a `sink_factory` callable:

- the child's `flush()` and `shutdown()` each yield the parent's build, and its `yielded` count
  goes from 0 to 2, but nothing in the child waits on that build;
- a wrapper the child reaches claims a new build over it, because the claim's owner pid is the
  parent's;
- with another parent thread holding the old wrapper's `_build_lock` across the fork, the child
  reads that lock as unlocked, because the fork repair replaced it, and `flush()` returns ok.

#### Acceptance Criteria:

- [ ] `_handing_here`'s and `_building_here`'s docstrings no longer claim the forking thread's
  `finally` clears the child's entries. They state the os._exit case and why each mark is kept.
- [ ] `_clear_after_fork`'s docstring gains the capture paragraph. `offer_stop_signal`,
  `handing_here` and `Worker._reinit_after_fork` state their new behaviour, and the two new
  `Runtime` attributes carry docstrings.
- [ ] `tests/test_lifecycle_inventory.py` has the row `_offering_here` (inherited) and the row
  `_stale_signals` (reset: `_lifecycle._clear_after_fork`). The text of the `_handing_here` and
  `_building_here` rows no longer says a `finally` clears them, and the inventory tests pass.
- [ ] `architecture.md` §13 *Known constraints* records the three limits in Out of Scope: forking
  before the store, the signal kept after the child's shutdown, and the grandchild forked by
  another thread of a child that is still inside a lock-holding setter. In `docs/invariants.md`, the
  wait entry's recorded exceptions point to that entry, and the fork entry's *Guarded* list names
  the new test file.
- [ ] A decision file in `docs/decisions/sink-lifecycle/` carries the fence, the rejected reset and
  its measurement. `docs/release-notes/v1.2.0.md` gains an entry for FR-003 and FR-004.
- [ ] `docstring-lint.py`, `docs-lint.sh` and `gen-index.sh` are clean.
- [ ] Serves no invariant: prose, the inventory gate's table and the record.

---

## Data Model

```python
# src/log_foundry/_lifecycle.py

class Runtime:
    ...
    _offering_here: threading.local
        # .offers: list[tuple[int, threading.Event]], this thread's offers in progress,
        # innermost last; pushed and popped by offer_stop_signal; never reset; inherited by a child
    _stale_signals: list[threading.Event]
        # the forking thread's offered events, exact threading.Event only, each _at_fork_reinit()'d;
        # rewritten by _clear_after_fork on every fork; [] in a process never forked mid-offer
    _fork_offers: list[tuple[int, threading.Event]]
        # in a child, the forking thread's live offer list itself, shared with every thread;
        # rewritten by _clear_after_fork on every fork ([] when that thread was inside no offer)
    _fork_inherited: int
        # how many of _fork_offers' entries the child inherited and is still inside; set by
        # _clear_after_fork, lowered by offer_stop_signal as the forking thread pops below it
```

## API / Interface Contract

Public: none. No public signature, field or return value changes.

Private:

```python
# src/log_foundry/_lifecycle.py
def offer_stop_signal(sink: Sink, stop: threading.Event) -> None
    # unchanged signature; pushes (id(sink), stop) before the probe, pops in finally,
    # and lowers _fork_inherited when the pop takes the inherited list below it
def _offers_here() -> list[tuple[int, threading.Event]]
    # this thread's offer record, created on first use (the shape of _builds_here)
def handing_here(sink: object) -> bool
    # true when sink is under handing(), on this thread's offer record,
    # or among _fork_offers[:_fork_inherited], from any thread
def _in_handing(sink: object) -> bool
    # id(sink) in _mid_hand(): the handing mark alone; _pay_marks_after_stop's one reading

# src/log_foundry/worker.py
class Worker:
    def _reinit_after_fork(self, *, resume: bool) -> None
        # when resume: refresh_stop_signal() as before; offer it only if not handing_here(self.sink)
```

## Configuration / Environment

None.

## File & Folder Structure

```
src/log_foundry/_lifecycle.py          # _offering_here, _stale_signals, _offers_here,
                                       # offer_stop_signal, handing_here, _clear_after_fork,
                                       # _shutdown_worker, the docstrings
src/log_foundry/worker.py              # Worker._reinit_after_fork's skip and docstring
tests/test_fork_mid_hand.py            # FR-001 to FR-004 (new)
tests/test_lifecycle_inventory.py      # the two new rows, two row texts
docs/architecture.md                   # §13 entry
docs/invariants.md                     # invariants 3 and 12
docs/decisions/sink-lifecycle/<new decision>.md
docs/release-notes/v1.2.0.md
```

## Implementation Phases

### Phase 1: The record, the capture and the set (FR-001, FR-002, FR-003)

- Add `_offering_here`, `_offers_here` and the push and pop in `offer_stop_signal`.
- Add `_stale_signals`, the capture and `_at_fork_reinit()` in `_clear_after_fork`, and the set in
  `_shutdown_worker`'s entry section.
- Add the inventory rows. Write FR-001 to FR-003's tests in `tests/test_fork_mid_hand.py`, using a
  sink that does not subclass `Sink`.

### Phase 2: No re-entry (FR-004)

- Widen `handing_here` to the offer record, give `_pay_marks_after_stop` `_in_handing`, and add
  the skip in `Worker._reinit_after_fork`.
- Write FR-004's three lock-holding-setter tests.
- Mutation-test each guard whose failure would be silent: the capture, the set, the
  `_at_fork_reinit()` call, the `handing_here` offer read, the resumed worker's skip, and a reset of
  both marks in the handler. Each must turn its test red.

### Phase 3: The record (FR-005)

- Rewrite the docstrings, the inventory row texts, §13, invariants 3 and 12, the decision file and
  the release-note entry.

## Revision history

- 2026-11-13: drafted from the design brief and the findings that the starting session measured at
  `c21755f1`. Re-measured at `53b68203` with the authoring session's probes (`proto.py`,
  `proto_after.py`, `proto_sinksub.py`, `proto_wrapped.py`, `probe_cond.py`, `probe_retarget.py`,
  `drive.py` and its `offerpatch.py`).
- **Two corrections to the brief, made while checking it against the code.** First, the brief
  stated the defect for every sink. A sink that subclasses `Sink` is not affected, because the
  repair walk's shared memo gives it the child's signal, so the observable tests use a sink that
  does not subclass `Sink`. Second, the brief's no-re-entry guard held only on the orphan path. The
  worker-build and retarget paths re-entered the setter at `53b68203`, in the fork handler and on
  the child's first log. That is why FR-004 widens `handing_here` and adds the resumed worker's
  skip.
- 2026-11-13, spec review: three findings answered. First, the widened `handing_here` no longer
  reaches the stop-end payment, and FR-004 lists every caller with its reading. The payment's
  criterion is replaced by the reason no reachable state distinguishes the two readings, with the
  probes (`probe_payment2.py`, `probe_payment3.py`). Second, the unwinding-child bullet now states
  the outcome per path, re-measured with `probe_unwind2.py`, and FR-003 gains the retarget
  unwinding criterion. Third, FR-003 gains the criterion that turns its worker-path rows into
  guards.
- 2026-11-13, system-frame diff review: the review found FR-004's twin path. The mark and the offer
  record are per thread, so another thread of an `os._exit` child could hand the sink again: the
  child's first log from another thread, or the resumed drain thread through a factory wrapper.
  `Runtime._fork_offers` and `Runtime._fork_inherited` now share the inherited offers with every
  thread of the child. FR-004 gains the four criteria that hold this, and FR-002's grandchild
  criterion gains the fresh-thread read. The payment's equivalence argument now covers all four
  offers that have no `handing` block, instead of only the retarget.
- 2026-11-13, execution review of the cross-thread view: the review planted four mutants, and all
  were killed. It raised two MINORs. First, a debt paid by another thread while the forking thread
  was inside the setter was not re-owed, so FR-004 gains the re-owe and FR-003 gains the
  `another-thread` row. Second, a grandchild forked by another thread of such a child hangs on a
  lock-holding setter. That is recorded as a third limit under Out of Scope, with the control that
  shows it is the generic fork-while-locked hazard.
