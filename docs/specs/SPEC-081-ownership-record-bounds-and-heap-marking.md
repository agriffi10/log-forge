---
type: Spec
id: SPEC-081
title: "Ownership Record: Pin Only What the Library Holds, and Mark the Heap When a Child's Marking Trips"
description: "Bounds the ownership record to the sinks the library still holds, so superseded sinks are collected and a long-lived process's forked children stop tripping the marking ceiling, and makes a child whose marking trips mark the inherited sinks on its heap, so a parent's sink the child hands over again is never closed."
spec_status: Draft
depends_on: [SPEC-039, SPEC-042, SPEC-044, SPEC-053, SPEC-054, SPEC-077, SPEC-078, SPEC-079]
arc: stability-redesign
arc_order: 12
---

## Overview

A process releases only a transport it acquired here (SPEC-042). The library keeps that promise
with an **ownership record**: every sink it is handed is written there with the pid of the process
that acquired it, and every close the library performs asks the record first. A forked child
inherits the record, and its one walk (SPEC-079) marks the inherited sinks the parent never
recorded as `_FOREIGN`, so the child can never claim them.

Two defects remain in that design, and both end with the library leaking memory or closing a
transport another process is still using.

**Defect A: the record grows without bound.** Each entry holds a strong reference to its sink, for
two stated reasons: an `id` is reused once its object dies, and a collected sink may close itself
from a finalizer. So every sink ever handed to the library stays alive for the life of the process.
That was accepted because `configure()` was taken to be a startup call. It is not always one.
Measured at `9f4d4dfd` with `configure(sink=MemorySink())`, 100 `info()` calls and a `flush()`,
repeated (the authoring session's `growth.py` probe):

- the record holds one entry per `configure()`, and every superseded `MemorySink` stays alive,
  about 90 KiB each (`tracemalloc`): 9.1 MB at 100 configures and 72.6 MB at 800;
- the child's marking walk seeds from each sink that the record pins, so **a child forked after
  500 configures trips `_MARKING_CEILING`** (one forked after 400 does not).

**Defect B: a child whose marking trips closes the parent's transport.** A tripped marking sets
`_marking_failed`, which refuses the close of a sink that stays unrecorded. But a child that hands
an unrecorded inherited sink to `configure()` again has `stamp` record it as the child's own, and the
child's `shutdown()` then closes the parent's transport. Reproduced at `9f4d4dfd` for a bare
re-hand, a `MultiSink(s)` and a `FilteringSink(s)`. Every library-wrapper re-hand is affected.

This spec fixes both, on Andrew's direction: **drop the strong reference once this process is done
with a sink, and keep only what `id`-reuse safety needs.** An entry holds its sink strongly only
while the library still holds the sink, and weakly afterwards, and an entry whose sink has died
answers for nothing. A child still pins every inherited sink that was alive at the fork. When a
child's marking trips anyway, the child marks the inherited sinks on its heap instead, which is
Andrew's decision of 2026-11-12, taken with the question tool. Only if that heap scan itself fails
does the child fall back to refusing every sink it is handed with `sink=`.

## Scope

### In Scope

- A record entry that holds its sink strongly only while the library holds it, and weakly after a
  performed close or a supersession, re-pinned at the end of each swap and each performed release,
  with the sinks beneath an entry captured before it is demoted (FR-001).
- Reading a dead entry as absent everywhere the record is read or written, and pruning dead
  entries without any weak-reference callback (FR-002).
- The child's at-fork pin of every alive inherited entry, and record seeds that leave out what the
  parent finished closing (FR-003).
- The heap fallback when the marking frontier trips, with its once-only announcement (FR-004).
- The last resort when the heap scan raises: `stamp(sink, *, built=False)`, and the once-per-process
  announcement pointing at `sink_factory=` (FR-005).
- The decision file, `architecture.md` §13, invariant 12's recorded exceptions, the docstrings
  that state the old design, and the 1.2.0 release notes (FR-006).
- The FR count is six, within the aim of 3 to 6.

### Out of Scope

- **A third-party wrapper's own `close()`** (`architecture.md` §13, the "eight things" list,
  item 5). The library never sees that call. In a normal child, `TP2(s)` still closes `s` through
  its own `close()`, as at `9f4d4dfd`.
- **Repair past a tripped ceiling.** The heap fallback marks; it does not repair locks or collect
  `reacquire_after_fork` hooks on what the marking frontier did not reach. Invariant 12's "past a
  tripped ceiling it is not" stays true.
- **Changing `_MARKING_CEILING`'s value.** It stays 100,000.
- **The stamp walk's container bound** (§13 item 6). `stamp` still reads a container one level deep.
- **The marking's reach in a child whose marking did not trip.** The marking frontier reads as
  SPEC-079 left it; only its record seeds change (FR-003).
- **Running the heap scan when the walk itself raises.** A raise there (for example while resolving
  the live roots) sets `_marking_failed` as today and goes straight to FR-005's last resort.
- **Capturing beyond the capture's reach.** A sink more than one container level below a
  third-party holder, one reachable only through a callable, and one placed beneath a demoted
  sink after its demotion are not captured (*Known residuals*, item 2).
- **Bounding the record for sinks that cannot be weakly referenced.** Each keeps a strong entry
  (*Known residuals*, item 1).
- **Clearing the record between tests.** The `tests/conftest.py` reset that SPEC-079 added stays,
  as test isolation.

---

## Functional Requirements

### FR-001: The record pins only what the library still holds

#### Description:

Each entry in `Runtime._owned` becomes an `_Entry` (*Data Model*) instead of a `(pid, sink)`
tuple. The pid and the write-once rule are unchanged. What changes is how the entry holds its sink.

**Strong references.** An entry naming **this** process holds its sink strongly while the library
still holds that sink. That means any of these is true:

- the sink is reachable by the stamp walk (`_reachable_sinks` without `after_fork`) from a live
  root: the configured sink, the live worker's sink or `_late_worker`'s sink;
- it is owed (`_state._closes.owed`);
- its close is in flight (`_state._closes`' in-flight entries).

**Weak references.** Every other entry naming this process holds a `weakref.ref` with no callback.
That covers two cases:

- a sink whose close this process performed, whether `close()` returned or raised. Its entry also
  gets `closed = True`;
- a sink superseded without ever being owed. Invariant 5 promises a close only for a sink that
  received an event, so five `configure(sink=R())` calls with nothing logged between them leave
  four sinks that are never closed. Measured: today they leave five entries and zero closes. So
  dropping an entry only when its close finishes would not bound the record.

**Entries naming another process** are strong for the life of the process. In a child those are
the parent's stamps and the walk's `_FOREIGN` marks, and FR-003 pins them at the fork.

**The re-pin.** At the end of `_swap_sink`, and at the end of every `release` that performed a
close, `_repin` works out the strong set from the three conditions above. It promotes the own
entries in that set, demotes every other own entry to weak, and drops the dead ones (FR-002). It
runs the stamp walk outside `_owned_lock`, as `stamp` does, and writes under it.

- **The epoch guard.** `stamp` and `reclaim` bump `Runtime._owned_epoch` and write it on the entry.
  A re-pin demotes only entries whose epoch is no later than the value it read when it began.
- **A sink re-handed while its earlier close is in flight.** `stamp` finds that sink's own,
  alive entry. Write-once keeps its pid, but `stamp` makes the entry strong, clears `closed` and
  bumps its epoch. The `release` that finishes the earlier close compares the entry's epoch with
  the one it read before calling `close()`. When they differ, it leaves `closed` clear and the entry
  strong.
- **A closed entry keeps its weak reference rather than being deleted.** `releasable` answers for a
  closed sink that is still alive exactly as it does today: recorded, so releasable again.
  Nothing changes for a caller who closes twice.

**Capture before demotion.** A strong entry is what kept a superseded wrapper alive, and the
child's marking reached a sink beneath it from that wrapper. When the wrapper can be collected,
that sink must carry an entry of its own. So before `_repin` demotes an own entry from strong to
weak, `_capture` records every sink-shaped object reachable beneath the demoted sink that has no
live entry, and every `weakref` proxy there, as the marking records one. Each is recorded
**weakly**, with this process's pid and `closed` false. Without it, a weak entry alone would turn
`test_a_superseded_wrapper_still_shields_the_transport_beneath_it` into a destructive close. That
is read from the code at `9f4d4dfd`, not measured: the stamp walk does not descend into a
third-party holder, so `conn` has no entry of its own, and once the dropped wrapper is collected
nothing the child seeds from reaches `conn`.

- **Its reach** is the stamp walk's, extended into third-party holders.
  - A package holder is entered as the stamp walk enters it: its namespace, plus its declared
    `_CHILD_SINK_SLOTS`, with a container read one level and only candidates pushed. A superseded
    `MemorySink`'s event dicts are plain, so they are skipped, as in SPEC-042's measurement of
    about 2 ms on 100k events.
  - A third-party holder is read as the marking reads one: its namespace members through
    `_fork._namespace_items`, and one container level through `_bounded_children`. Each object
    there that is not a plain builtin is entered in turn.
  - Every read is static, with the existing predicates, so no code of a sink runs.
- **Its bound** is `_MARKING_CEILING` objects entered. A plain member that is read and skipped is
  not a visit. A trip or a raise is absorbed and leaves what is left unrecorded (*Known residuals*,
  item 2). It is announced once per process, through `_diag.pending` for a trip and
  `_diag.absorbed` for a raise, with this detail:
  `recording the sinks beneath a superseded sink stopped short; a forked child may close one of them if it configures that sink itself`.
  `Runtime._capture_announced` makes the line once-only, and `_walk_after_fork` clears it in
  each child.
- **Its locks.** `_capture` runs outside every library lock, like `stamp`'s walk. `_repin` picks
  the entries to demote under `_owned_lock`, holds a strong reference to each in a local list,
  releases the lock and captures. It then takes the lock again to write the captured entries and
  to demote, still under the epoch guard.
- **What it keeps alive: nothing.** A captured sink that the application still holds stays alive,
  so a child inherits its entry, FR-003 pins that entry strong at the fork, and a child re-hand is
  refused. A captured sink that the application dropped dies, and `_repin` prunes its entry, so the
  record stays bounded.

The capture covers both shapes of a sink held beneath a third-party wrapper that the library no
longer holds:

- the wrapper superseded with nothing logged and dropped by the application (the pinned test);
- the wrapper closed by the parent, whose `close()` did not close the sink beneath it.

**A sink that cannot be weakly referenced keeps a strong entry.** That is a type whose instances
have no `__weakref__` slot, so `weakref.ref` raises `TypeError`. The attempt runs none of the
sink's code. Every shipped sink can be weakly referenced, since none declares `__slots__` (a grep
of `src/log_foundry/sinks/`). This is *Known residuals*, item 1.

**Twin paths (invariant 6).** The worker path and the orphan path both close through `release`,
and a swap closes through `_swap_sink`, so the same re-pin runs on each. A `flush()` and a
`shutdown()` read the same record.

**Readers of the entry's shape** change with it:

- in `src/`: the `_lifecycle` sites that read `_owned`, together with `_fork` and `worker`, whose
  docstrings describe the tuple;
- in `tests/`: `conftest.py`, `test_fork_lifecycle.py`, `test_config.py`,
  `test_child_sink_slots.py`, `test_sink_ownership.py`, `test_fork_child_state.py`,
  `test_invariants_model.py`, `test_fork_one_walk.py`, `test_owed_closes.py`,
  `test_lifecycle_races.py`, `test_runtime_holder.py` and `test_lifecycle_inventory.py`. The
  last also gains rows for the three new `Runtime` fields.

Each test keeps its name and its promise.

#### Acceptance Criteria:

- [ ] After 500 iterations of `configure(sink=MemorySink())`, 100 `info()` calls and `flush()`,
  `len(_state._owned)` is no greater than it was after 50 iterations. Of the weak references the
  test took to the 499 superseded sinks, at most one is alive after `gc.collect()`. Every entry
  the capture wrote holds no strong reference.
- [ ] After five `configure(sink=RecordingSink())` calls with nothing logged, the four superseded
  entries hold no strong reference, and each sink is collected once the test drops it. None was
  closed.
- [ ] The entries for the configured sink, for an owed sink and for a sink parked inside its
  `close()` each hold a strong reference. After that close returns, and in a second case after it
  raises, the entry is weak with `closed` true.
- [ ] A sink re-handed with `configure(sink=s)` while its earlier close is parked has, after that
  close finishes, a strong entry with `closed` false. Removing the epoch comparison from `release`
  turns this red.
- [ ] A superseded, closed sink whose class declares `__slots__` without `__weakref__` keeps a
  strong entry, and `configure()` returned normally.
- [ ] A closed sink still held by the test reads `releasable(s)` true in the process that closed it.
- [ ] In a child, every entry that names the parent's pid or `_FOREIGN` still holds a strong
  reference after the child's own swap and its own performed release.
- [ ] `test_a_superseded_wrapper_still_shields_the_transport_beneath_it` passes exactly as it
  stands at `9f4d4dfd`. With the `_capture` call removed from `_repin`, it turns red.
- [ ] The closed-wrapper twin: the parent configures `TP(conn)`, where `TP.close()` does nothing,
  then calls `info()` and `flush()` and supersedes it, so `TP` is closed. It drops `TP` and keeps
  `conn`. In the child, re-handing `conn` and calling `shutdown()` leaves `conn.closes == 0`.
- [ ] Superseding a `MemorySink` that holds 100k events completes its capture in fewer than 100
  visits, as `_capture` counts them.
- [ ] A capture that trips the ceiling, and one that raises, each leave `configure()` returning
  normally. Across both, the detail above is written once.
- [ ] Every `pytest --collect-only -q` name at the build's base is present at the head.
- [ ] Invariants 5, 11 and 12: a sink that received an event is closed once by its owner, no
  transport the parent opened is closed by a child, and the capture's line is written once.

### FR-002: A dead entry answers for nothing

#### Description:

An `id` is free for reuse the moment its object dies. Measured with the `idreuse.py` probe over
10,000 trials each, for `MemorySink` and for a plain structural class: a new object of the same type
lands on a just-dead sink's `id` **99.93%** of the time. Across types with different instance sizes
it was 0%. A weak entry that outlives its sink would therefore answer, almost every time, for the
next sink of that type. It would either refuse that fresh sink (a leak) or answer with a dead
sink's verdict. This FR is the "what `id`-reuse safety needs" half of Andrew's direction.

**Every read and every write treats an entry as absent** when its referent is dead or is not the
object asked about. `_entry_for(sink)` is the one reader:

- `releasable` answers for the sink, and for its `owner`, as it would for an unrecorded one;
- `stamp`, the capture (FR-001), the walk's record write, the heap fallback's write (FR-004) and
  `reclaim` all overwrite such an entry instead of keeping it;
- `_record_seeds` skips such an entry.

An identity check on a strong entry stays as the safe-direction backstop that `releasable` already
has.

**Pruning takes no weak-reference callback.** A callback runs inside the garbage collector, which
can fire on a thread that already holds `_owned_lock`. That lock is not re-entrant, so a callback
taking it would deadlock the thread. Dead entries are dropped **lazily**, by `_repin` (FR-001)
under `_owned_lock`, and no `weakref.ref` the record creates carries a callback.

#### Acceptance Criteria:

- [ ] The test makes a dead entry for sink `A`, then builds `RecordingSink` objects until one, `B`,
  lands on `id(A)`, and asserts that it did. `releasable(B)` is then true. After
  `configure(sink=B)`, `info()` and `shutdown()`, `B`'s entry names this pid and `B`, and `B` was
  closed once.
- [ ] With a dead entry at an owner's `id`, `releasable(inner, owner=W)` answers as it does with no
  owner entry.
- [ ] With a dead entry at `id(B)`, four writes each replace it with an entry naming `B`:
  `stamp(B)` and the capture write this pid, while `_walk_after_fork` reaching `B` and the heap
  fallback each write `_FOREIGN`.
- [ ] After that dead entry's sink is gone, one `configure(sink=…)` leaves the record with no entry
  whose referent is dead.
- [ ] Every weak reference in the record has `__callback__` `None`.
- [ ] A thread holds `_owned_lock`, drops the last reference to a weakly recorded sink caught in a
  reference cycle, and calls `gc.collect()`. It returns, and is joined within 5 s.
- [ ] Invariants 1, 5 and 12: no call hangs on the record's lock, a fresh sink on a reused `id` is
  closed by its owner, and a dead entry never lends its verdict to another object.

### FR-003: The child's marking is bounded by what the parent still holds

#### Description:

**Inherited entries are pinned first.** As its first step, before it resolves a root,
`_walk_after_fork` takes `_owned_lock` (the roster has already replaced it) and upgrades to strong
every weak entry whose referent is alive. This keeps the second reason for the old strong
reference: a sink that the child's application stops referencing is not collected in the child, so
no finalizer can send a goodbye on the parent's transport. The pin runs before any registered
handler. It is bounded by what was alive at the fork.

**`_record_seeds`** returns the sinks in `_state._closes.owed`, then every entry whose referent is
alive, **except entries this process finished closing** (`closed` true). Those transports are
closed. Leaving them out drops the closed, superseded `MemorySink`s, which were the whole cost of
the tripped ceiling. An entry the capture wrote (FR-001) was never closed by this process, so it is
a seed for as long as its sink is alive.

**What the seeds still protect.** Take a superseded third-party wrapper `TP(s)` that the
application still holds and that was never closed. Its entry is weak, alive and not closed, so it is
still a seed, and the marking still reaches `s` and marks it `_FOREIGN`. When the application
drops the wrapper instead, the capture has already given `s` an entry of its own (FR-001).

**What bounds the walk.** It is bounded by the library's live and owed graphs, plus whatever
un-closed sinks the application itself keeps alive after handing them over.

#### Acceptance Criteria:

- [ ] A child forked after FR-001's 500-iteration loop reads `_marking_failed` false. Its stderr
  holds no line containing `passed 100000`.
- [ ] After that loop, the parent's `_record_seeds()` contains no sink whose entry is `closed`.
- [ ] The parent configures `TP(s)`, keeps the wrapper and supersedes it with nothing logged. In
  the child, `s`'s entry is `_FOREIGN`. `configure(sink=MultiSink(s))`, `info()` and `shutdown()`
  then leave `s.closes == 0`.
- [ ] The parent keeps a closed, superseded sink `c` whose `__del__` calls `close()`. When
  `_reclaim_after_fork` runs, `c`'s entry is strong. After the child deletes its only reference
  and runs `gc.collect()`, `c` is alive and `c.closes` is unchanged.
- [ ] `test_a_child_cannot_claim_a_sink_the_parent_never_recorded` (both shapes) and
  `test_the_record_holds_a_strong_reference` pass unedited, apart from FR-001's shape change.
- [ ] Invariants 5 and 12: a child closes no transport the parent opened in any case above, and
  never trips the ceiling on the record alone.

### FR-004: A tripped marking falls back to marking the whole heap

#### Description:

This is Andrew's decision of 2026-11-12, taken with the question tool. When the marking frontier
raises `_CeilingTripped`, the child calls `_mark_heap`. That function reads every object from
`_heap_objects()`, which returns `gc.get_objects()` and is the seam a test patches. Each object is
judged only by the existing static predicates:

- skip it when `id(type(o))` is in `_fork._PLAIN_TYPE_IDS`;
- otherwise keep it when `_fork._sink_shaped(o)` or `_fork._is_weak_proxy(o)` is true.

A class is never kept, because `_sink_shaped` refuses one. No object's code runs. Each object that
the scan keeps joins what the walk found, and the walk's record write then `setdefault`s it as
`_FOREIGN`, strongly. A sink the parent stamped keeps that stamp, and a dead entry is overwritten (FR-002).

**Why this is safe and what it covers.**

- The scan runs inside the at-fork walk, before any handler or application code, so every object
  it can find was inherited.
- Every instance of a Python class is GC-tracked. Measured: `gc.is_tracked` is true for an empty
  `__slots__`, a named slot, a plain class and an `int` subclass.
- The cost is paid once per tripped fork, and only there. Measured with `heapscan.py` and
  `heapscan2.py`: `gc.get_objects()` takes 2 ms at 71k objects, 11 ms at 521k and 52 ms at 2.5M,
  and the classification takes about 0.65 s per million objects.

**The child then carries on as a normal child.** `_marking_failed` stays clear, so a sink the child
builds after the fork is stamped and closed as usual. `_reclaim_after_fork` still overrides the
marks for a sink whose `reacquire_after_fork` returned.

**The trip is still announced once, with new wording.** The old line said the child "will not
close an inherited sink it has no record of unless it configures that sink itself", and that is now
false. Nothing is lost, so the line is written through `_diag.pending`, which has no `lost N`
prefix, with this exact detail:

`the walk marking a forked child's inherited sinks passed {_MARKING_CEILING} objects; this child marked the sink-shaped objects on its inherited heap instead, and closes none of them, even one it configures itself`

**Accepted side effect.** Only in a tripped child, an inherited sink that the parent never handed
to the library (§13 item 1, the gunicorn-master case) is refused rather than closed.

**Test hazard: restore the ceiling before the child's own `configure()`.** A criterion that trips
the marking by lowering `_MARKING_CEILING` must restore it in the child before calling
`configure()`. One way is an `os.register_at_fork(after_in_child=…)` registered **after** the
library's handler. The reason is that `_MARKING_CEILING` also bounds `_bounded_children`, which is
`stamp`'s own read of `MultiSink._sinks`. Left at 0, it hides `s` from the child's `stamp`, and the
`MultiSink` case passes vacuously. That is exactly how the first probe misreported the `MultiSink`
twin as refused (*Revision history*).

#### Acceptance Criteria:

- [ ] Each case below is an executed `os.fork()`. The parent runs `configure(sink=TP(s))`, `info()`
  and `flush()`, and trips the marking by lowering the ceiling. The child asserts that
  `_MARKING_CEILING == 100_000` before it configures. Re-handing `s` bare, then in `MultiSink(s)`,
  then in `FilteringSink(s)`, each followed by `shutdown()`, leaves `s.closes == 0`, with
  `_marking_failed` false.
- [ ] With `_mark_heap`'s call removed, the bare and `MultiSink` cases turn red.
- [ ] In a tripped child, a `RecordingSink` built after the fork is closed once by `shutdown()`. A
  configured `FileSink` re-acquired there reads `releasable` true and is closed.
- [ ] A sink the parent built and never handed over, re-handed by a tripped child, keeps
  `closes == 0`.
- [ ] A heap object whose `__class__` property, `__getattribute__` and metaclass `__hash__` and
  `__eq__` record each call records none in a tripped child, as `tests/test_fork_static_reads.py`
  counts them.
- [ ] A tripped child writes the detail above exactly once, and no `lost` line from the walk. No
  file in `src/` contains `unless it configures that sink itself`.
- [ ] Invariants 5, 11 and 12: a tripped child closes no transport the parent opened and closes the
  sinks it built, and its one line says what happened.

### FR-005: When the heap scan raises, the child refuses every sink it is handed with `sink=`

#### Description:

This is the last resort, and Andrew accepted it as part of the heap fallback. It applies when
`_mark_heap` raises, and also when the walk itself raises as it does today. In either case
`_marking_failed` is set, and the existing `_diag.absorbed` line names the exception type with new
wording: `this child cannot tell what it inherited and will close only a sink the library built
itself; configure sink_factory= to build one`.

**`stamp(sink, *, built=False)`.** In a process whose `_marking_failed` is set, `stamp` records
**as `_FOREIGN`** every sink it finds that has no live entry, apart from one exception. When
`built` is true, the root is recorded as this process's own. `built=True` is passed only where the
library constructed the root:

- `config._ensure_sink`'s lazy default;
- `configure()`, for the `_FactorySink` that its `sink_factory=` branch builds;
- `_FactorySink`'s build, for what the factory returned;
- `_adopt_factory_after_fork`, for the adopted `_FactorySink`.

Only the root is exempt, and *Known residuals*, item 3 states the cost. In a process whose
`_marking_failed` is clear, `built` changes nothing.

**Why it costs what it costs.** A fresh `sink=` in such a child is never closed. From §13's cost
table, that means `KafkaSink` and `GooglePubSubSink` buffers are not delivered, `SQLiteSink` and
`PostgresSink` are not committed, and `NATSSink` is not drained. So the first `configure(sink=…)`
whose root is recorded `_FOREIGN` this way writes one `_diag.pending` line per process, with this
exact detail:

`configure(sink=...) in a forked child that could not tell what it inherited: this child will not close that sink; configure sink_factory= so each process builds a sink of its own`

`Runtime._stamp_refusal_announced` makes the line once-only. `_walk_after_fork` clears it in each
child, together with `_marking_failed`.

#### Acceptance Criteria:

- [ ] In a tripped child whose `_heap_objects` raises, `_marking_failed` is true. A bare re-hand of
  the parent's `s`, `info()` and `shutdown()` leave `s.closes == 0`, and `s`'s entry is `_FOREIGN`.
- [ ] In that child, `configure(sink_factory=lambda: RecordingSink())`, `info()` and `shutdown()`
  close the built sink once.
- [ ] In a child of a parent that configured nothing, with the scan made to raise, the lazy default
  reads `releasable` true.
- [ ] In that child, two `configure(sink=RecordingSink())` calls write the pending line exactly
  once, and `shutdown()` closes neither sink. A `sink_factory=` configure writes no such line.
- [ ] In a child whose `_live_roots` raises, a bare re-hand of the parent's `s` leaves
  `s.closes == 0`. It is 1 at `9f4d4dfd`.
- [ ] An AST scan finds `built=` passed to `stamp` in exactly `_ensure_sink`, `configure`,
  `_FactorySink`'s build and `_adopt_factory_after_fork`.
- [ ] Invariants 5, 11 and 12: in such a child no parent transport is closed, a library-built sink is
  closed by this process, and the announcement is written once and is true.

### FR-006: The record of the decision is rewritten in place

#### Description:

At build time, the completion ritual rewrites these, each against the code as built.

- **The decision file.**
  `docs/decisions/sink-lifecycle/a-process-releases-only-a-transport-it-acquired-here-and-unrecorded-must-be-unclaimable-rather-than-merely-unreleasable.md`
  is rewritten in place, with a dated History entry headed "Revised by SPEC-081".
  - Its fence (the `description`) stays as it is, because it stays true: a process releases only a
    transport it acquired here, and unrecorded stays unclaimable.
  - The History entry says plainly that **Defect B was a hole in that fence**: a re-hand in a
    tripped child had become claimable.
  - The body gains four things: a strong reference only while the library holds the sink; a weak
    one afterwards, where a dead entry is absent; inherited entries pinned strong at the fork; and
    the heap fallback with its last resort.
- **`architecture.md` §13.**
  - Retire the items "After a tripped marking ceiling, a child that re-configures an unrecorded
    inherited sink owns it and closes it" and "The ownership record grows for the life of the
    process, and a forked child walks all of it". Strike each through and mark it "closed by
    SPEC-081", as §12 marks a closed item.
  - Rewrite item 8 of the "eight things" list ("The record never shrinks"), and item 7, which says
    `_marking_failed` catches the visit ceiling.
  - Add *Known residuals* items 1 to 5 below.
  - Rewrite the sentence on callables that says a trip would "refuse every unrecorded sink in every
    child".
  - Check §9's fork paragraph and the third-party-sink item's "past a tripped ceiling it is
    neither", which stays true for repair.
- **`architecture.md` §12.** The resolved `_reinit_primitives` item says a trip "degrades to a
  leaked handle". It gains a superseded marker naming SPEC-081.
- **Invariant 12.** Its recorded exceptions lose "After a tripped marking ceiling, a child that
  re-configures an unrecorded inherited sink owns it and closes it (§13, pre-existing)". "Past a
  tripped ceiling it is not" stays, because it is about repair.
- **Docstrings.** Rewrite the docstrings of `Runtime._owned`, `Runtime._FORK_SKIP`,
  `Runtime._marking_failed`, `_MARKING_CEILING`, `_record_seeds`, `_walk_after_fork`, `stamp`,
  `releasable`, `reclaim` and `release`, and the `_owned` sentence in `Worker._FORK_SKIP`'s
  docstring.
- **Release notes.** `docs/release-notes/v1.2.0.md` gains two entries under *Fixed*: superseded
  sinks are now collected, and a child forked from a long-lived reconfiguring process no longer
  trips the marking; and a child whose marking trips no longer closes a parent's sink that it hands
  over again.

#### Acceptance Criteria:

- [ ] The decision file's `description` is byte-identical to `9f4d4dfd`'s. Its History has a dated
  SPEC-081 entry naming Defect B as a hole in the fence, and its body names the weak entry, the
  dead-entry rule, the inherited pin and the heap fallback.
- [ ] No sentence outside `docs/specs/`, struck text and History entries contains `never shrinks`
  or `unless it configures that sink itself`.
- [ ] `docs/invariants.md` §12 no longer carries the tripped-ceiling exception for a child that
  re-configures, and still carries "past a tripped ceiling it is not".
- [ ] §13 carries the five *Known residuals* below, and both retired items are struck through and
  name SPEC-081.
- [ ] `sh scripts/docs-lint.sh` and `poetry run python scripts/docstring-lint.py` exit 0.
- [ ] Invariant 12: the page's recorded exceptions match the code as built, so a reviewer checking
  the invariant reads no exception that no longer exists.

---

## Data Model

```python
# src/log_foundry/_lifecycle.py

class _Entry:
    """One record entry; mutated in place under Runtime._owned_lock."""
    __slots__ = ("pid", "strong", "weak", "closed", "epoch")
    pid: int                                   # os.getpid() at the write, or _FOREIGN
    strong: object | None                      # the sink while pinned; None while weak
    weak: weakref.ReferenceType[object] | None # no callback; None when the sink cannot be weakly referenced
    closed: bool                               # a close this process performed has finished (returned or raised)
    epoch: int                                 # Runtime._owned_epoch at this entry's last stamp or reclaim

    def referent(self) -> object | None: ...   # strong, else weak(); None means dead

class Runtime:
    ...
    _owned: dict[int, _Entry]                  # was dict[int, tuple[int, object]]
    _owned_epoch: int = 0                      # bumped by every stamp and reclaim write; inherited
    _stamp_refusal_announced: bool = False     # FR-005's line; reset by _walk_after_fork
    _capture_announced: bool = False           # FR-001's capture line; reset by _walk_after_fork
```

The rules for an entry:

| Entry | Holds its sink | Becomes weak | Dropped |
|---|---|---|---|
| this process's, live, owed or in flight | strong | at the next re-pin after none holds | never while alive |
| this process's, closed or superseded unowed | weak (`closed` set after a performed close) | n/a | by `_repin`, once dead |
| this process's, written by the capture | weak (`closed` false) | n/a | by `_repin`, once dead |
| another pid's, or `_FOREIGN` | strong | never | never |
| a sink with no `__weakref__` | strong | never | never |

## API / Interface Contract

Public: none. No public signature, field or return value changes.

Private:

```python
# src/log_foundry/_lifecycle.py
def stamp(sink: object, *, built: bool = False) -> None
    # built=True: the library constructed this root; matters only while _marking_failed is set
def releasable(sink: object, *, owner: object = None) -> bool      # unchanged signature
def reclaim(sink: object) -> None                                  # unchanged signature
def _entry_for(sink: object) -> _Entry | None
    # caller holds _owned_lock; None when absent, dead, or not identical to sink
def _repin() -> None
    # end of _swap_sink and of every release that performed a close; captures before it demotes
def _capture(demoted: list[object]) -> tuple[list[object], int]
    # FR-001: the sinks and weakref proxies beneath the demoted sinks, and the objects entered;
    # no lock held; bounded by _MARKING_CEILING; a trip or raise is absorbed and announced once
def _pin_inherited() -> None
    # first step of _walk_after_fork: every alive weak entry becomes strong
def _heap_objects() -> list[object]
    # gc.get_objects(); the test seam FR-005 makes raise
def _mark_heap(found: list[object]) -> None
    # FR-004: appends each inherited sink-shaped object or weakref proxy on the heap

# Example (FR-005): the four library-built roots
_lifecycle.stamp(default, built=True)    # config._ensure_sink
```

## Configuration / Environment

None.

## File & Folder Structure

```
src/log_foundry/_lifecycle.py        # _Entry, _entry_for, _repin, _capture, _pin_inherited,
                                     # _heap_objects, _mark_heap; stamp's built=; the seeds;
                                     # the three new lines
src/log_foundry/config.py            # built=True at _ensure_sink and configure's factory branch
src/log_foundry/sinks/_factory.py    # built=True at the build
src/log_foundry/worker.py            # Worker._FORK_SKIP's docstring
tests/test_ownership_record.py       # FR-001 and FR-002 (new)
tests/test_fork_heap_marking.py      # FR-003, FR-004 and FR-005 (new)
tests/conftest.py and the test files FR-001 lists      # the entry's shape
docs/decisions/sink-lifecycle/a-process-releases-only-a-transport-it-acquired-here-and-unrecorded-must-be-unclaimable-rather-than-merely-unreleasable.md
docs/architecture.md, docs/invariants.md, docs/release-notes/v1.2.0.md
```

## Implementation Phases

### Phase 1: The entry, the re-pin and the dead-entry rule (FR-001, FR-002)

- Add `_Entry`, `_entry_for`, `_owned_epoch` and `_repin`, and move every reader and writer of
  `_owned` onto them.
- Call `_repin` at the end of `_swap_sink` and of every `release` that performed a close, with the
  epoch comparison in `release`.
- Add `_capture` and its once-only line, run by `_repin` with no lock held before it demotes.
- Re-express the test files FR-001 lists for the entry's shape, and diff the `--collect-only` names
  on both refs.
- Write `tests/test_ownership_record.py`, including the `id`-reuse loop, the lock-held GC case,
  the closed-wrapper twin and the capture's visit count.

### Phase 2: The child (FR-003, FR-004, FR-005)

- Add `_pin_inherited` as the walk's first step, and the `closed` exclusion in `_record_seeds`.
- Add `_heap_objects`, `_mark_heap`, the new trip line, `stamp`'s `built=` at its four sites, the
  last-resort rule and its once-per-process line.
- Write `tests/test_fork_heap_marking.py`, observing the test hazard in FR-004. Re-express
  `test_a_runaway_container_trips_the_ceiling_and_refuses_everything` (its child now reads
  `failed=False` and still refuses the unrecorded sink).
- Mutation-test the guards whose failure would be silent: the epoch comparison, the capture call
  in `_repin`, the inherited pin, the `closed` seed exclusion, the dead-entry overwrite in each
  writer, the `_mark_heap` call and the root-only `built` exemption.

### Phase 3: The record (FR-006)

- Rewrite the decision file, §12's marker, §13, invariant 12 and the docstrings, and add the
  release-note entries.

## Known residuals

1. **A sink that cannot be weakly referenced keeps a strong entry.** The record grows by one entry
   per such sink handed over, as every entry grew before this spec. No shipped sink is one.
2. **What the capture does not reach stays unrecorded once its holder is gone.** That covers:
   - the rest of a capture that tripped `_MARKING_CEILING` or raised;
   - a sink more than one container level below a third-party holder;
   - a sink reachable only through a bound method, a closure cell or a `functools.partial`;
   - a sink placed beneath a superseded sink after that sink was demoted.

   If the application keeps such a sink and drops the holder, a child that hands the sink over
   again claims it, as it would a sink never handed over at all (§13 item 1). The capture's line
   names the first case.
3. **The last resort leaves library-built graphs half-owned and fresh `sink=` sinks unclosed.** In a
   child whose heap scan or walk raised, a fresh `sink=` is never closed, with FR-005's costs. A
   factory that returns a wrapper has only the root recorded as the child's, so the wrapper's
   members are refused their close.
4. **A C-extension sink type without GC support** is not in `gc.get_objects()`. It stays unrecorded
   in a tripped child, as in a normal child whose walk did not reach it.
5. **In a tripped child, an inherited sink the parent never handed over is refused rather than
   closed** (FR-004's accepted side effect). Garbage sinks that were not yet collected at the fork
   are pinned in that child too.

## Revision history

- 2026-11-12: drafted from the authoring session's design brief, on measurements at `9f4d4dfd`
  (`growth.py`, `idreuse.py`, `normal_child.py`, `multi_restored.py`, `heapscan.py` and
  `heapscan2.py`). Andrew chose the heap fallback for Defect B with the question tool that day,
  and accepted the last resort as part of it.
- **The `MultiSink` probe correction.** The first probe (`scen.py ceiling_claim`, run with
  `_MARKING_CEILING = 0`) reported the `MultiSink(s)` twin as correctly refused. That was an
  artifact. The same ceiling bounds `_bounded_children`, so the child's `stamp` never saw `s`.
  With the ceiling restored before the child's `configure()`, `MultiSink(s)` and `FilteringSink(s)`
  both record `s` as the child's and close it. FR-004 states the hazard so that no criterion
  repeats the mistake.
- **Where the brief left the walk-raise path open,** this spec sends a raised walk straight to the
  last resort, without the heap scan. It also adds `configure()`'s `sink_factory=` branch as a
  fourth `built=True` site, because without it a factory configured in such a child would never
  be closed.
- **Revised after authoring, on the orchestrator's decision of 2026-11-12, inside Andrew's
  direction.** The first draft accepted that a weak entry would let
  `test_a_superseded_wrapper_still_shields_the_transport_beneath_it` become a destructive close,
  and listed a dropped or closed wrapper's inner sink as a residual. That turned a pinned refusal
  into a regression. FR-001 now captures the sinks beneath an entry before demoting it, which keeps
  that test unchanged and also covers the brief's closed-wrapper case. That residual is withdrawn.
