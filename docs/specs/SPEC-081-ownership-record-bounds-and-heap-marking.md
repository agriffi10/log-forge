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
while the library still holds the sink, and weakly afterwards. Before an entry goes weak, the sinks
beneath it are recorded too, so a third-party wrapper that is collected leaves no sink unrecorded.
An entry whose sink has died answers for nothing. A child pins every inherited sink that was alive
at the fork. When a child's marking trips anyway, the child marks the inherited sinks on its heap
instead, which is Andrew's decision of 2026-11-12, taken with the question tool. Only if that heap
scan fails does the child fall back to refusing every sink it is handed with `sink=`.

## Scope

### In Scope

- A record entry that holds its sink strongly only while the library holds it, and weakly after a
  performed close or a supersession, re-pinned at the end of each swap and each performed release
  (FR-001).
- Capturing the sinks beneath an entry before it is demoted (FR-002).
- Reading a dead entry as absent everywhere the record is read or written, pruning dead entries
  without any weak-reference callback, and making `_owned_lock` re-entrant so a finalizer cannot
  hang on it (FR-003).
- The child's pin of every alive inherited entry, before anything else in its handler, and record
  seeds that leave out what the parent finished closing in a package sink (FR-004).
- The heap fallback when the marking frontier trips or the child inherited a failed marking, with
  its once-only announcement, and a frozen heap counted as a failed scan (FR-005).
- The last resort when the heap scan fails: `stamp(sink, *, built=False)`, and the
  once-per-process announcement pointing at `sink_factory=` (FR-006).
- The decision files, `architecture.md` §9, §12 and §13, invariant 12, the docstrings that state
  the old design, and the 1.2.0 release notes (FR-007).
- **Seven FRs, above the aim of 3 to 6 and within the ceiling of 8.** The capture was split out of
  FR-001 at the spec review. It is a second walk with its own bound, failure line and cost
  criterion, and folding it back would put two mechanisms in one FR.

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
  SPEC-079 left it; only its record seeds change (FR-004).
- **Running the heap scan when the walk itself raises.** A raise there (for example while resolving
  the live roots) sets `_marking_failed` as today and goes straight to FR-006's last resort.
- **Unfreezing a frozen heap to scan it.** FR-005 records why it is rejected.
- **Capturing beyond the capture's reach** (*Known residuals*, item 2).
- **Bounding the record for sinks that cannot be weakly referenced** (*Known residuals*, item 1).
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

**Weak references.** Every other entry naming this process holds its sink only through its weak
reference. That covers two cases:

- a sink whose close this process performed, whether `close()` returned or raised. Its entry also
  gets `closed = True`;
- a sink superseded without ever being owed. Invariant 5 promises a close only for a sink that
  received an event, so five `configure(sink=R())` calls with nothing logged between them leave
  four sinks that are never closed. Measured: today they leave five entries and zero closes. So
  dropping an entry only when its close finishes would not bound the record.

**The entry's shape never tears.** For a sink that can be weakly referenced, `weak` is a
`weakref.ref` with no callback, created when the entry is written and kept for the entry's life.
Only `strong` toggles between the sink and `None`. A fork that lands mid-write can therefore see a
stale `strong`, but never an entry with neither reference.

**Entries naming another process** are strong for the life of the process. In a child those are
the parent's stamps and the walk's `_FOREIGN` marks, and FR-004 pins them at the fork.

**The re-pin.** `_repin` works out the strong set from the three conditions above. It promotes the
own entries in that set, demotes every other own entry to weak (after FR-002's capture), and drops
the dead ones (FR-003). It runs the stamp walk outside `_owned_lock`, as `stamp` does, and writes
under it.

- **Where it runs.** On every exit of `_swap_sink`, the early return included, from its outer
  `finally`. And at the end of every `release` that performed a close, after `_closes.finish`.
- **It never raises.** A fault is absorbed and announced once per process through
  `_diag.absorbed`, guarded by `Runtime._repin_announced`. In `release` it runs inside the
  `finally` and must never replace the exception `close()` raised.
- **The locks held where it runs.**
  - At the end of `_swap_sink`, the thread holds `config._sink_lock` when its caller,
    `config._change_sink`, acquired it, and none of `_state._lock`, `_config_lock`, the close
    registry's lock or `_owned_lock`.
  - At the end of a performed `release`, the library's own inline callers (`_close_guarded`,
    `_close_taken` and the inline close `_close_owed` chooses) hold none of those four.
  - A release that a wrapper forwards from its `close()` holds whatever that `close()`'s caller
    holds.
  - The one case in which `_owned_lock` is held is a finalizer re-entering on a thread already
    inside it (FR-003).
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

**The last strong reference to a demoted sink is dropped outside every lock.** `_repin` copies the
sinks it demotes into a local list before it sets their `strong` to `None`. That list outlives
every lock section in `_repin`, so when the list goes, any finalizer runs with none of
`_state._lock`, `_owned_lock`, `_config_lock` or the registry's lock held.

**A sink that cannot be weakly referenced keeps a strong entry.** That is a type whose instances
have no `__weakref__` slot, so `weakref.ref` raises `TypeError`. The attempt runs none of the
sink's code. A `weakref.proxy` is such an object, so its entry stays strong whether it was
configured or captured. A proxy holds nothing alive, so that pins only the proxy. No shipped sink is
such a type. A grep for `__slots__` under `src/log_foundry/sinks/` at `9f4d4dfd` finds ten
declarations, among them `sinks/_deferral.py`'s, and each is on a helper class that is not a sink.
This is *Known residuals*, item 1.

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
  last also gains rows for the four new `Runtime` fields.

Each test keeps its name and its promise.

#### Acceptance Criteria:

- [ ] After 500 iterations of `configure(sink=MemorySink())`, 100 `info()` calls and `flush()`,
  `len(_state._owned)` is at most 8. Of the weak references the test took to the 499 superseded
  sinks, at most one is alive after `gc.collect()`.
- [ ] After five `configure(sink=RecordingSink())` calls with nothing logged, the four superseded
  entries have `strong` `None`, and each sink is collected once the test drops it. None was closed.
- [ ] The entries for the configured sink, for an owed sink and for a sink parked inside its
  `close()` each hold a strong reference. After that close returns, and in a second case after it
  raises, the entry is weak with `closed` true. The entry's `weak` is the same object throughout.
- [ ] A sink re-handed with `configure(sink=s)` while its earlier close is parked has, after that
  close finishes, a strong entry with `closed` false. Removing the epoch comparison from `release`
  turns this red.
- [ ] A superseded, closed sink whose class declares `__slots__` without `__weakref__` keeps a
  strong entry, and so does a superseded `weakref.proxy` sink. `configure()` returned normally.
- [ ] A closed sink still held by the test reads `releasable(s)` true in the process that closed it.
- [ ] In a child, every entry that names the parent's pid or `_FOREIGN` still holds a strong
  reference after the child's own swap and its own performed release.
- [ ] A spy finds `_repin` called on `_swap_sink`'s early return. In the single-threaded suites, at
  every entry to `_repin`, `_state._lock`, `_config_lock` and the registry's lock are unlocked.
- [ ] With `_repin` made to raise, `configure()` returns, and a `release` whose `close()` raised
  `OSError` still raises that `OSError`. The absorbed line is written once.
- [ ] A demoted sink whose `__del__` records the locks held finds `_state._lock`, `_config_lock`
  and the registry's lock unlocked, and `_owned_lock._is_owned()` false, when `_repin` drops it.
- [ ] Every `pytest --collect-only -q` name at the build's base is present at the head.
- [ ] Invariants 5 and 12: a sink that received an event is closed once by its owner, and no
  transport the parent opened is closed by a child.

### FR-002: Before an entry is demoted, the sinks beneath it are captured

#### Description:

A strong entry is what kept a superseded wrapper alive, and the child's marking reached a sink
beneath it from that wrapper. Once the wrapper can be collected, that sink must carry an entry of
its own. So before `_repin` demotes an own entry from strong to weak, `_capture` records:

- each sink-shaped object reachable beneath the demoted sink that has no live entry;
- each `weakref` proxy there, as the marking records one.

Each is recorded **weakly** where it can be (FR-001), with `closed` false and `captured` true. It
carries this process's pid, or `_FOREIGN` while `_marking_failed` is set (FR-006).

Without the capture, a weak entry alone would turn
`test_a_superseded_wrapper_still_shields_the_transport_beneath_it` into a destructive close. That
is read from the code at `9f4d4dfd`, not measured: the stamp walk does not descend into a
third-party holder, so `conn` has no entry of its own, and once the dropped wrapper is collected,
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
  not a visit. A trip or a raise is absorbed and leaves the rest unrecorded (*Known residuals*,
  item 2). It is announced once per process, through `_diag.pending` for a trip and
  `_diag.absorbed` for a raise, with this detail:
  `recording the sinks beneath a superseded sink stopped short; a forked child may close one of them if it configures that sink itself`.
  `Runtime._capture_announced` makes the line once-only, and `_walk_after_fork` clears it in
  each child.
- **Its locks.** `_capture` takes no lock. `_repin` picks the entries to demote under
  `_owned_lock`, holds a strong reference to each in its local list (FR-001), releases the lock and
  captures. It then takes the lock again to write the captured entries and to demote, still under
  the epoch guard. Only a finalizer re-entering `_repin` (FR-003) captures with `_owned_lock`
  held.
- **What it keeps alive: nothing except proxy objects, which hold nothing alive.**
  - A captured sink that the application still holds stays alive, so a child inherits its entry,
    FR-004 pins that entry strong at the fork, and a child re-hand is refused.
  - A captured sink that the application dropped dies, and `_repin` prunes its entry, so the
    record stays bounded.

The capture covers both shapes of a sink held beneath a third-party wrapper that the library no
longer holds:

- the wrapper superseded with nothing logged and dropped by the application (the pinned test);
- the wrapper closed by the parent, whose `close()` did not close the sink beneath it.

#### Acceptance Criteria:

- [ ] `test_a_superseded_wrapper_still_shields_the_transport_beneath_it` passes exactly as it
  stands at `9f4d4dfd`. With the `_capture` call removed from `_repin`, it turns red.
- [ ] The closed-wrapper twin: the parent configures `TP(conn)`, where `TP.close()` does nothing,
  then calls `info()` and `flush()` and supersedes it, so `TP` is closed. It drops `TP` and keeps
  `conn`. In the child, re-handing `conn` and calling `shutdown()` leaves `conn.closes == 0`.
- [ ] Superseding a `MemorySink` that holds 100k events completes its capture in fewer than 100
  visits, as `_capture` counts them.
- [ ] Each entry the capture wrote has `captured` true and `closed` false, and names this pid, or
  `_FOREIGN` when `_marking_failed` is set. Where its sink can be weakly referenced, `strong` is
  `None`.
- [ ] A captured sink that the test drops is collected, and its entry is gone after the next
  `configure()`.
- [ ] A capture that trips the ceiling, and one that raises, each leave `configure()` returning
  normally. Across both, the detail above is written once.
- [ ] Invariants 5, 11 and 12: no transport the parent opened beneath a superseded wrapper is closed
  by a child, the record stays bounded, and the capture's line is written once and is true.

### FR-003: A dead entry answers for nothing, and the record's lock survives a finalizer

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
- `stamp`, the capture (FR-002), the walk's record write, the heap fallback's write (FR-005) and
  `reclaim` all overwrite such an entry instead of keeping it;
- `_record_seeds` skips such an entry.

An identity check on a strong entry stays as the safe-direction backstop that `releasable` already
has.

**Pruning takes no weak-reference callback.** A callback runs inside the garbage collector, which
can fire on any thread at any allocation. Dead entries are dropped **lazily**, by `_repin` (FR-001)
under `_owned_lock`, and no `weakref.ref` the record creates carries a callback.

**`Runtime._owned_lock` becomes a `threading.RLock`.** A weak entry makes it possible for a sink to
be collected while a thread holds `_owned_lock`. A sink whose finalizer forwards a `close()` into
a library wrapper then reaches `releasable` and `release` on that same thread. With a plain `Lock`
the thread hangs. The reviewer's `finalizer.py` probe hangs at `9f4d4dfd` with a `FilteringSink`
subclass collected inside `with _state._owned_lock:`. So:

- the lock is re-entrant;
- every section under it iterates a snapshot (`list(_owned.items())`), never the live dict, so a
  re-entrant write during the section is safe.

**Knock-ons the builder owns:**

- the fork roster's entry for `_state._owned_lock` (`_fork._GLOBAL_PRIMITIVES`). Its tests are
  in `tests/test_fork_roster.py`, and `_fork._fresh_primitive` must mint an `RLock` for it, which
  it already does for an `RLock`;
- `tests/test_runtime_holder.py`'s `LOCK_SITES_AT_BASE`;
- the lock-order docstrings: `Runtime`'s "Three locks", `Runtime._owned_lock`'s, and
  `tests/test_sink_ownership.py::test_the_record_lock_is_last_in_the_process_order`.

#### Acceptance Criteria:

- [ ] The test makes a dead entry for a `RecordingSink` `A`, then builds `RecordingSink`
  objects, at most 10,000, until one, `B`, lands on `id(A)`, and asserts that one did.
  `releasable(B)` is then true. After `configure(sink=B)`, `info()` and `shutdown()`, `B`'s entry
  names this pid and `B`, and `B` was closed once.
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
- [ ] The `finalizer.py` shape: a `FilteringSink` subclass whose `__del__` calls `close()`, made
  garbage in a cycle and collected inside `with _state._owned_lock:` on a thread, lets that thread
  return, joined within 5 s.
- [ ] `_owned_lock` is an `RLock` in the parent and in a forked child, and `tests/test_fork_roster.py`
  passes.
- [ ] Invariants 1, 5 and 12: no call hangs on the record's lock, a fresh sink on a reused `id` is
  closed by its owner, and a dead entry never lends its verdict to another object.

### FR-004: The child pins what it inherited, and its marking is bounded by what the parent still holds

#### Description:

**Inherited entries are pinned before anything else.** `_pin_inherited` upgrades to strong every
weak entry whose referent is alive. It runs **first** in the child's handler, before the roster
replaces any lock, registered with `_fork.register_pin`. `_fork` keeps its rule that it imports
nothing but `_diag`. The pin takes no lock: the child is single-threaded while at-fork handlers run.
Its fault is absorbed as every other step's is.

This keeps the second reason for the old strong reference: a sink that the child's application
stops referencing is not collected in the child, so no finalizer can send a goodbye on the
parent's transport. The pin is bounded by what was alive at the fork. Another library's
`after_in_child` handlers that were registered before log-foundry's still run before the pin
(*Known residuals*, item 8).

**`_record_seeds`** returns:

- the sinks in `_state._closes.owed`;
- then every entry whose referent is alive, **except a closed entry whose sink is package-owned**
  (`closed` true and `_fork._is_owned(sink)`).

A package sink that this process finished closing has released its transport through the closed
guard (SPEC-076), and what it holds was stamped when it was handed over. Leaving those out drops the
closed, superseded `MemorySink`s, which were the whole cost of the tripped ceiling.

A closed **third-party** entry is still a seed while it is alive. Its `close()` is the caller's
code and may have closed nothing beneath it, and what it holds may sit deeper than the capture
reads. The reviewer's `pool.py` probe is the shape: a wrapper holding `{"primary": [conn]}`, two
containers down.

An entry the capture wrote (FR-002) was never closed, so it is a seed for as long as its sink is
alive.

**What the seeds protect.**

- A superseded `TP(s)` that the application still holds is a seed. The marking reaches `s` and
  marks it `_FOREIGN` wherever the capture had not already recorded `s` with the parent's pid.
- A sink placed beneath that wrapper after its demotion is one the capture cannot have seen, so the
  wrapper's seed is what refuses it.

**What bounds the walk.** It is bounded by the library's live and owed graphs, plus whatever
un-closed or third-party sinks the application itself keeps alive after handing them over.

#### Acceptance Criteria:

- [ ] A child forked after FR-001's 500-iteration loop writes no stderr line containing
  `passed 100000`.
- [ ] The same holds when the test keeps all 499 superseded `MemorySink`s alive. With the closed
  exclusion removed from `_record_seeds`, that case turns red.
- [ ] After that loop, the parent's `_record_seeds()` contains no closed package-owned sink.
- [ ] The parent configures `TP(s)`, keeps the wrapper and supersedes it with nothing logged. In
  the child, `s`'s entry names the parent's pid or `_FOREIGN`. `configure(sink=MultiSink(s))`,
  `info()` and `shutdown()` then leave `s.closes == 0`.
- [ ] The parent configures an empty `TP`, keeps it, supersedes it with nothing logged, and then
  assigns `tp.inner = s`, asserting that `s` has no entry. In the child, `s`'s entry is `_FOREIGN`,
  and a bare re-hand of `s` and `shutdown()` leave `s.closes == 0`.
- [ ] The `pool.py` shape: a third-party wrapper holding `{"primary": [conn]}`, with a `close()`
  that does nothing, is logged to, superseded and kept. In the child, a re-hand of `conn` and
  `shutdown()` leave `conn.closes == 0`.
- [ ] The parent keeps a closed, superseded sink `c` whose `__del__` calls `close()`. When the roster
  replacement starts, `c`'s entry is already strong. After the child deletes its only reference
  and runs `gc.collect()`, `c` is alive and `c.closes` is unchanged.
- [ ] `test_a_child_cannot_claim_a_sink_the_parent_never_recorded` (both shapes) and
  `test_the_record_holds_a_strong_reference` pass unedited, apart from FR-001's shape change.
- [ ] Invariants 5 and 12: a child closes no transport the parent opened in any case above, and
  never trips the ceiling on the record alone.

### FR-005: A tripped marking falls back to marking the whole heap

#### Description:

This is Andrew's decision of 2026-11-12, taken with the question tool. `_mark_heap` reads every
object from `_heap_objects()`, which returns `gc.get_objects()` and is the seam a test patches.
Each object is judged only by the existing static predicates:

- skip it when `id(type(o))` is in `_fork._PLAIN_TYPE_IDS`;
- otherwise keep it when `_fork._sink_shaped(o)` or `_fork._is_weak_proxy(o)` is true.

A class is never kept, because `_sink_shaped` refuses one. No object's code runs. Each object that
the scan keeps joins what the walk found, and the walk's record write then `setdefault`s it as
`_FOREIGN`, strongly. A sink the parent stamped keeps that stamp, and a dead entry is overwritten
(FR-003).

**When it runs.** The scan runs in `_walk_after_fork`, after the marking frontier, in two cases:

- the marking frontier raised `_CeilingTripped`;
- the child inherited `_marking_failed` set, because an ancestor's marking failed. Such a child's
  record lacks marks its ancestor could not make, so its own walk succeeding does not make an
  unrecorded sink safe.

When the scan succeeds, `_marking_failed` is cleared. When it fails, the flag is set (or stays
set), and FR-006 applies. Clearing the flag is a deliberate change from `9f4d4dfd`, where nothing
ever cleared it, so every descendant of a failed child stayed in the failed state.

**A frozen heap is a failed scan.** In a child where `gc.get_freeze_count() > 0`,
`gc.get_objects()` cannot see the permanent generation. A scan there would miss every frozen sink,
so `_mark_heap` does not read the heap at all and reports the scan as failed. Andrew's decision
said that if the heap scan fails, the last resort applies, and a scan that cannot see the heap has
failed.

**Rejected alternative: unfreeze, scan, refreeze.** It is cheap, because both are list splices in
constant time. But it changes the application's GC state: everything in generations 0 to 2 at the
fork would be frozen for the child's life, including garbage that would then never be collected.
A library must not change that behind the caller's back.

**Why this is safe and what it costs.**

- The scan runs inside the at-fork walk, before any registered handler and before the
  application's code resumes, so every object it can find was inherited.
- Every instance of a Python class is GC-tracked. Measured: `gc.is_tracked` is true for an empty
  `__slots__`, a named slot, a plain class and an `int` subclass.
- The time is paid once per scanning fork, and only there. Measured with `heapscan.py` and
  `heapscan2.py`: `gc.get_objects()` takes 2 ms at 71k objects, 11 ms at 521k and 52 ms at 2.5M,
  and the classification takes about 0.65 s per million objects.
- **The memory cost is copy-on-write.** `gc.get_objects()` takes a reference to every tracked
  object, which writes its reference count, so a scanning child copies every page that holds an
  inherited GC-tracked object. This is reasoned from how CPython stores reference counts, not
  measured, and it is accepted with the scan.

**The child then carries on as a normal child.** `_marking_failed` is clear, so a sink the child
builds after the fork is stamped and closed as usual. `_reclaim_after_fork` still overrides the
marks for a sink whose `reacquire_after_fork` returned.

**What it says.** After a trip whose scan succeeded, the child writes one line through
`_diag.pending`, which has no `lost N` prefix, because nothing is lost. The old line ("will not
close an inherited sink it has no record of unless it configures that sink itself") is now false.
The new detail is exactly:

`the walk marking a forked child's inherited sinks passed {_MARKING_CEILING} objects; this child marked the sink-shaped objects on its inherited heap instead, and closes none of them, even one it configures itself`

A scan that fails writes FR-006's line instead of this one, since this line would then be false.
A scan run only because the flag was inherited, which then succeeds, writes nothing.

**Accepted side effect.** Only in a scanning child, an inherited sink that the parent never handed
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
- [ ] The parent calls `gc.freeze()` just before that fork, and calls `gc.unfreeze()` after it. The
  tripped child reads `_marking_failed` true, and a bare re-hand of `s` and `shutdown()` leave
  `s.closes == 0`.
- [ ] A last-resort child forks a grandchild whose own walk does not trip. The grandchild's scan
  succeeds, its `_marking_failed` is false, and its re-hand of the grandparent's `s` and its
  `shutdown()` leave `s.closes == 0`.
- [ ] In a tripped child, a `RecordingSink` built after the fork is closed once by `shutdown()`. A
  configured `FileSink` re-acquired there reads `releasable` true and is closed.
- [ ] A sink the parent built and never handed over, re-handed by a tripped child, keeps
  `closes == 0`.
- [ ] A heap object whose `__class__` property, `__getattribute__` and metaclass `__hash__` and
  `__eq__` record each call records none in a tripped child, as `tests/test_fork_static_reads.py`
  counts them.
- [ ] A tripped child whose scan succeeds writes the detail above exactly once, and no `lost` line
  from the walk. No file in `src/` contains `unless it configures that sink itself`.
- [ ] Invariants 5, 11 and 12: a scanning child closes no transport the parent opened and closes
  the sinks it built, and its one line says what happened.

### FR-006: When the heap scan fails, the child refuses every sink it is handed with `sink=`

#### Description:

This is the last resort, and Andrew accepted it as part of the heap fallback. It applies in three
cases, each of which sets `_marking_failed`:

- `_mark_heap` raised;
- the heap was frozen (FR-005);
- the walk itself raised, as it can today.

Each writes one line with the same closing words:

- after a raise, the existing `_diag.absorbed` line, naming the exception type;
- after a frozen heap, a `_diag.pending` line.

Its detail is:

`this child cannot tell what it inherited and will close only a sink the library built itself; configure sink_factory= to build one`

The frozen-heap line puts `the heap is frozen (gc.freeze); ` in front of it.

**`stamp(sink, *, built=False)`.** In a process whose `_marking_failed` is set, `stamp` records
**as `_FOREIGN`** every sink it finds that has no live entry, apart from one exception. When
`built` is true, the root is recorded as this process's own. `built=True` is passed only where the
library constructed the root:

- `config._ensure_sink`'s lazy default;
- `config._change_sink`, for the `_FactorySink` that `configure()`'s `sink_factory=` branch builds;
- `_FactorySink._make`, for what the factory returned;
- `_adopt_factory_after_fork`, for the adopted `_FactorySink`.

Only the root is exempt, and *Known residuals*, item 3 states the cost. In a process whose
`_marking_failed` is clear, `built` changes nothing. The capture writes `_FOREIGN` here too
(FR-002).

**Why it costs what it costs.** A fresh `sink=` in such a child is never closed. From §13's cost
table, that means `KafkaSink` and `GooglePubSubSink` buffers are not delivered, `SQLiteSink` and
`PostgresSink` are not committed, and `NATSSink` is not drained. So the first `configure(sink=…)`
whose root is recorded `_FOREIGN` this way writes one `_diag.pending` line per process, with this
exact detail:

`configure(sink=...) in a forked child that could not tell what it inherited: this child will not close that sink; configure sink_factory= so each process builds a sink of its own`

`Runtime._stamp_refusal_announced` makes the line once-only, and `_walk_after_fork` clears it in
each child. `_marking_failed` is not cleared blindly; FR-005 says when it is.

#### Acceptance Criteria:

Each case first asserts that `_marking_failed` is true in the child. It reaches that state through
a heap scan made to raise, through `_live_roots` patched to raise, or through FR-005's frozen heap,
and never through a parent that configured nothing.

- [ ] In a tripped child whose `_heap_objects` raises, a bare re-hand of the parent's `s`, `info()`
  and `shutdown()` leave `s.closes == 0`, and `s`'s entry is `_FOREIGN`.
- [ ] In that child, `configure(sink_factory=lambda: RecordingSink())`, `info()` and `shutdown()`
  close the built sink once.
- [ ] In a child whose `_live_roots` raises, forked from a parent whose config holds no sink,
  `info()` builds the lazy default, which reads `releasable` true and is closed once by
  `shutdown()`.
- [ ] In a child whose `_live_roots` raises, two `configure(sink=RecordingSink())` calls write the
  pending line exactly once, and `shutdown()` closes neither sink. A `sink_factory=` configure there
  writes no such line.
- [ ] In a child whose `_live_roots` raises, a bare re-hand of the parent's `s` leaves
  `s.closes == 0`. It is 1 at `9f4d4dfd`.
- [ ] An AST scan finds `built=` passed to `stamp` in exactly `config._ensure_sink`,
  `config._change_sink`, `_FactorySink._make` and `_lifecycle._adopt_factory_after_fork`.
- [ ] Invariants 5, 11 and 12: in such a child no parent transport is closed, a library-built sink is
  closed by this process, and the announcement is written once and is true.

### FR-007: The record of the decision is rewritten in place

#### Description:

At build time, the completion ritual rewrites these, each against the code as built.

- **The ownership decision.**
  `docs/decisions/sink-lifecycle/a-process-releases-only-a-transport-it-acquired-here-and-unrecorded-must-be-unclaimable-rather-than-merely-unreleasable.md`
  is rewritten in place, with a dated History entry headed "Revised by SPEC-081".
  - Its fence (the `description`) stays as it is, because it stays true: a process releases only a
    transport it acquired here, and unrecorded stays unclaimable.
  - The History entry says plainly that **Defect B was a hole in that fence**: a re-hand in a
    tripped child had become claimable.
  - The body gains: a strong reference only while the library holds the sink; the capture before
    demotion; a weak reference afterwards, where a dead entry is absent; the re-entrant record
    lock; inherited entries pinned first at the fork; and the heap fallback, the frozen heap and
    the last resort.
- **The order-of-work decision.**
  `docs/decisions/pipeline/only-the-forked-child-is-repaired-and-its-order-of-work-is-the-contract.md`
  has its fence rewritten to put the record pin before the roster, with a dated History entry.
- **`architecture.md` §9, the fork paragraph.**
  - The numbered order of work gains a new first step: "**Pin the inherited record.** Every record
    entry whose sink is alive is held strongly before anything else runs (SPEC-081)". The three
    steps after it are renumbered.
  - In the roster-and-walk step, "first in first out under a visit ceiling, then the records."
    becomes "first in first out under a visit ceiling, then the record's live entries other than a
    closed package sink; a trip marks the inherited sinks on the heap instead (SPEC-081)."
  - The "Unrecorded has to be unclaimable" paragraph needs no edit.
- **`architecture.md` §13.**
  - Retire the items "After a tripped marking ceiling, a child that re-configures an unrecorded
    inherited sink owns it and closes it" and "The ownership record grows for the life of the
    process, and a forked child walks all of it". Strike each through and mark it "closed by
    SPEC-081", as §12 marks a closed item.
  - Rewrite item 8 of the "eight things" list ("The record never shrinks"), and item 7, which says
    `_marking_failed` catches the visit ceiling.
  - Add *Known residuals* items 1 to 8 below.
  - Rewrite the sentence on callables that says a trip would "refuse every unrecorded sink in every
    child".
  - The third-party-sink item's "past a tripped ceiling it is neither" stays, because it is about
    repair.
- **`architecture.md` §12.** The resolved `_reinit_primitives` item says a trip "degrades to a
  leaked handle". It gains a superseded marker naming SPEC-081.
- **Invariant 12.**
  - Its first sentence gains the pin: "the inherited record pinned first, then the package's
    global locks and events from one roster, ...".
  - Its recorded exceptions lose "After a tripped marking ceiling, a child that re-configures an
    unrecorded inherited sink owns it and closes it (§13, pre-existing)". "Past a tripped ceiling
    it is not" stays, because it is about repair.
- **Docstrings.** Rewrite the docstrings of:
  - `Runtime` (its "Three locks" paragraph), `Runtime._owned`, `Runtime._owned_lock`,
    `Runtime._FORK_SKIP` and `Runtime._marking_failed`;
  - `_MARKING_CEILING`, `_record_seeds`, `_walk_after_fork`, `stamp`, `releasable`, `reclaim` and
    `release`;
  - `_fork._SKIP_ATTRIBUTE` and `_fork._reinit_after_fork` (its order of work);
  - the `_owned` sentence in `Worker._FORK_SKIP`'s docstring.
- **Release notes.** `docs/release-notes/v1.2.0.md` gains two entries under *Fixed*:
  - superseded sinks are now collected, and a child forked from a long-lived process that
    reconfigures no longer trips the marking;
  - a child whose marking trips no longer closes a parent's sink that it hands over again.

#### Acceptance Criteria:

- [ ] The ownership decision's `description` is byte-identical to `9f4d4dfd`'s. Its History has a
  dated SPEC-081 entry naming Defect B as a hole in the fence, and its body names the weak entry,
  the capture, the dead-entry rule, the inherited pin and the heap fallback.
- [ ] The order-of-work decision's fence names the record pin first, and its History has a dated
  SPEC-081 entry.
- [ ] `architecture.md` §9's order of work begins with the pin, and its walk step names the closed
  package exclusion and the heap fallback.
- [ ] No sentence outside `docs/specs/`, struck text and History entries contains `never shrinks`
  or `unless it configures that sink itself`.
- [ ] `docs/invariants.md` §12 names the pin first, no longer carries the tripped-ceiling exception
  for a child that re-configures, and still carries "past a tripped ceiling it is not".
- [ ] §13 carries the eight *Known residuals* below, and both retired items are struck through and
  name SPEC-081.
- [ ] `sh scripts/docs-lint.sh` and `poetry run python scripts/docstring-lint.py` exit 0.
- [ ] Invariant 12: the page's statement and recorded exceptions match the code as built, so a
  reviewer checking the invariant reads no order and no exception that no longer exists.

---

## Data Model

```python
# src/log_foundry/_lifecycle.py

class _Entry:
    """One record entry; written under Runtime._owned_lock, read through a snapshot."""
    __slots__ = ("pid", "strong", "weak", "closed", "captured", "epoch")
    pid: int                                   # os.getpid() at the write, or _FOREIGN
    strong: object | None                      # the sink while pinned; None while weak; the only field that toggles
    weak: weakref.ReferenceType[object] | None # set at the write and kept for life, no callback;
                                               # None when the sink cannot be weakly referenced
    closed: bool                               # a close this process performed has finished (returned or raised)
    captured: bool                             # written by _capture (FR-002), not by stamp, reclaim or the walk
    epoch: int                                 # Runtime._owned_epoch at this entry's last stamp or reclaim

    def referent(self) -> object | None: ...   # strong, else weak(); None means dead

class Runtime:
    ...
    _owned: dict[int, _Entry]                  # was dict[int, tuple[int, object]]
    _owned_lock: threading.RLock               # was threading.Lock (FR-003)
    _owned_epoch: int = 0                      # bumped by every stamp and reclaim write; inherited
    _stamp_refusal_announced: bool = False     # FR-006's line; reset by _walk_after_fork
    _capture_announced: bool = False           # FR-002's line; reset by _walk_after_fork
    _repin_announced: bool = False             # FR-001's absorbed line; reset by _walk_after_fork
```

The rules for an entry:

| Entry | Holds its sink | Becomes weak | Dropped |
|---|---|---|---|
| this process's, live, owed or in flight | strong | at the next re-pin after none holds, after the capture | never while alive |
| this process's, closed or superseded unowed | weak (`closed` set after a performed close) | n/a | by `_repin`, once dead |
| this process's, written by the capture | weak (`captured` set, `closed` clear) | n/a | by `_repin`, once dead |
| another pid's, or `_FOREIGN` | strong | never | never |
| a sink with no `__weakref__`, a `weakref.proxy` included | strong | never | never |

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
    # every exit of _swap_sink, and every release that performed a close; never raises
def _capture(demoted: list[object]) -> tuple[list[object], int]
    # FR-002: the sinks and weakref proxies beneath the demoted sinks, and the objects entered;
    # takes no lock; bounded by _MARKING_CEILING; a trip or raise is absorbed and announced once
def _pin_inherited() -> None
    # first step of the child's handler, before the roster; takes no lock
def _heap_objects() -> list[object]
    # gc.get_objects(); the test seam FR-006 makes raise
def _mark_heap(found: list[object]) -> bool
    # FR-005: appends each inherited sink-shaped object or weakref proxy on the heap;
    # False, reading nothing, when gc.get_freeze_count() > 0

# src/log_foundry/_fork.py
def register_pin(fn: Callable[[], None]) -> None
    # the step _reinit_after_fork runs first, before the roster

# Example (FR-006): one of the four library-built roots
_lifecycle.stamp(default, built=True)    # config._ensure_sink
```

## Configuration / Environment

None.

## File & Folder Structure

```
src/log_foundry/_lifecycle.py        # _Entry, _entry_for, _repin, _capture, _pin_inherited,
                                     # _heap_objects, _mark_heap; stamp's built=; the seeds;
                                     # the RLock; the new lines
src/log_foundry/_fork.py             # register_pin; _reinit_after_fork runs it first
src/log_foundry/config.py            # built=True at _ensure_sink and _change_sink
src/log_foundry/sinks/_factory.py    # built=True at _FactorySink._make
src/log_foundry/worker.py            # Worker._FORK_SKIP's docstring
tests/test_ownership_record.py       # FR-001, FR-002 and FR-003 (new)
tests/test_fork_heap_marking.py      # FR-004, FR-005 and FR-006 (new)
tests/conftest.py and the test files FR-001 lists      # the entry's shape
tests/test_fork_roster.py, tests/test_runtime_holder.py # the RLock
docs/decisions/sink-lifecycle/a-process-releases-only-a-transport-it-acquired-here-and-unrecorded-must-be-unclaimable-rather-than-merely-unreleasable.md
docs/decisions/pipeline/only-the-forked-child-is-repaired-and-its-order-of-work-is-the-contract.md
docs/architecture.md, docs/invariants.md, docs/release-notes/v1.2.0.md
```

## Implementation Phases

### Phase 1: The entry, the re-pin, the capture and the dead-entry rule (FR-001, FR-002, FR-003)

- Add `_Entry`, `_entry_for`, `_owned_epoch` and `_repin`, make `_owned_lock` an `RLock` with
  snapshot iteration, and move every reader and writer of `_owned` onto them. Update the roster
  tests and `LOCK_SITES_AT_BASE`.
- Call `_repin` from `_swap_sink`'s outer `finally` and from every `release` that performed a
  close, with the epoch comparison, the absorbed fault and the local list of demoted sinks.
- Add `_capture` and its once-only line, run by `_repin` with no lock held before it demotes.
- Re-express the test files FR-001 lists for the entry's shape, and diff the `--collect-only` names
  on both refs.
- Write `tests/test_ownership_record.py`.

### Phase 2: The child (FR-004, FR-005, FR-006)

- Add `_fork.register_pin` and `_pin_inherited`, run first, and the closed-package exclusion in
  `_record_seeds`.
- Add `_heap_objects`, `_mark_heap` with the frozen-heap refusal, the inherited-flag scan, the new
  lines, `stamp`'s `built=` at its four sites, and the last-resort rule.
- Write `tests/test_fork_heap_marking.py`, observing the test hazard in FR-005. Re-express
  `test_a_runaway_container_trips_the_ceiling_and_refuses_everything`: its child now reads
  `failed=False` and still refuses the unrecorded sink.
- Mutation-test the guards whose failure would be silent: the epoch comparison, the capture call
  in `_repin`, the pin, the closed-package seed exclusion and its `_is_owned` test, the dead-entry
  overwrite in each writer, the `_mark_heap` call, the freeze check, the inherited-flag scan and the
  root-only `built` exemption.

### Phase 3: The record (FR-007)

- Rewrite both decision files, §9, §12's marker, §13, invariant 12 and the docstrings, and add the
  release-note entries.

## Known residuals

1. **A sink that cannot be weakly referenced keeps a strong entry.** The record grows by one entry
   per such sink handed over or captured, as every entry grew before this spec. A `weakref.proxy`
   is one, whether configured or captured, but it pins only the proxy. No shipped sink is one.
2. **What the capture does not reach stays unrecorded once its holder is gone.** A holder the
   application keeps is a seed (FR-004), so this applies only after the holder is collected:
   - the rest of a capture that tripped `_MARKING_CEILING` or raised, which its line names;
   - a sink more than one container level below a third-party holder;
   - a sink reachable only through a bound method, a closure cell or a `functools.partial`;
   - a sink placed beneath a superseded sink after that sink was demoted.

   If the application keeps such a sink and drops the holder, a child that hands the sink over
   again claims it, as it would a sink never handed over at all (§13 item 1).
3. **The last resort leaves library-built graphs half-owned and fresh `sink=` sinks unclosed.** In a
   child whose heap scan failed or whose walk raised, a fresh `sink=` is never closed, with
   FR-006's costs. A factory that returns a wrapper has only the root recorded as the child's, so
   the wrapper's members are refused their close.
4. **A frozen heap sends a tripped child to the last resort.** A gunicorn-style prefork server that
   calls `gc.freeze()` and whose children trip the marking gets FR-006 in every such child, so a
   fresh `sink=` there is not closed. Use `sink_factory=`, which the last resort still closes.
5. **A C-extension sink type without GC support** is not in `gc.get_objects()`. It stays unrecorded
   in a scanning child, as in a normal child whose walk did not reach it.
6. **A scanning child pays in refusals and in memory.** An inherited sink the parent never handed
   over is refused rather than closed (FR-005's accepted side effect). Garbage sinks not yet
   collected at the fork are pinned. Every page holding an inherited GC-tracked object is copied.
7. **A finalizer that runs under another library lock can still hang.** A GC that fires while the
   thread holds `_state._lock`, `_config_lock` or the close registry's lock can run a finalizer
   that forwards into a library wrapper's `close()`, and that can block on the lock the thread
   holds. This was already possible for a sink never handed over; this spec widens it to
   superseded ones. It is narrow: no shipped sink defines `__del__` (a grep of `src/log_foundry`
   for `__del__` at `9f4d4dfd` finds none), and a plain third-party sink's `close()` takes no
   library lock. This spec does not audit the library's own reference drops inside those sections
   for the same effect.
8. **Another library's at-fork handlers run before the pin.** An `os.register_at_fork(after_in_child=…)`
   handler registered before log-foundry's runs first. If it drops the child's last reference to an
   inherited sink whose entry was weak, that sink is collected before `_pin_inherited` runs.

## Revision history

- 2026-11-12: drafted from the authoring session's design brief, on measurements at `9f4d4dfd`
  (`growth.py`, `idreuse.py`, `normal_child.py`, `multi_restored.py`, `heapscan.py` and
  `heapscan2.py`). Andrew chose the heap fallback for Defect B with the question tool that day,
  and accepted the last resort as part of it.
- **The `MultiSink` probe correction.** The first probe (`scen.py ceiling_claim`, run with
  `_MARKING_CEILING = 0`) reported the `MultiSink(s)` twin as correctly refused. That was an
  artifact. The same ceiling bounds `_bounded_children`, so the child's `stamp` never saw `s`.
  With the ceiling restored before the child's `configure()`, `MultiSink(s)` and `FilteringSink(s)`
  both record `s` as the child's and close it. FR-005 states the hazard so that no criterion
  repeats the mistake.
- **Where the brief left the walk-raise path open,** this spec sends a raised walk straight to the
  last resort, without the heap scan. It also adds `configure()`'s `sink_factory=` branch as a
  fourth `built=True` site, because without it a factory configured in such a child would never
  be closed.
- **Revised after authoring, on the orchestrator's decision of 2026-11-12, inside Andrew's
  direction.** The first draft accepted that a weak entry would let
  `test_a_superseded_wrapper_still_shields_the_transport_beneath_it` become a destructive close,
  and listed a dropped or closed wrapper's inner sink as a residual. That turned a pinned refusal
  into a regression. The capture before demotion now keeps that test unchanged and also covers the
  brief's closed-wrapper case. That residual is withdrawn.
- **Revised after the spec review (2026-11-12, against `b0aec74e`).**
  - A frozen heap is now a failed scan; unfreezing to scan it is recorded as rejected.
  - Closed third-party entries stay seeds.
  - The last resort's criteria are reached by real routes.
  - An inherited failed marking re-scans rather than persisting.
  - `_owned_lock` is re-entrant, and finalizers have rules.
  - The capture is its own FR, the pin runs first, and the names are corrected.
