---
type: Spec
id: SPEC-079
title: "Fork: One Walk From the Runtime's Roots, the Global Lock Roster, and sink_factory="
description: "Repairs a forked child by re-creating the package's global locks from one explicit roster and reaching every sink through one bounded walk from the runtime's roots, deletes the module-wide walk and the separate inheritance marking, and adds sink_factory= so each process can build a sink of its own."
spec_status: Completed
depends_on: [SPEC-039, SPEC-042, SPEC-044, SPEC-053, SPEC-054, SPEC-068, SPEC-072, SPEC-077, SPEC-078, SPEC-080]
arc: stability-redesign
arc_order: 11
---

## Overview

A process that forks hands its child a copy of every lock, buffer and sink it holds, and the
threads that held them do not come along. The library repairs the child in a fixed order (SPEC-039),
refuses to close what the child inherited (SPEC-042), and marks inherited sinks so the child cannot
claim them later (SPEC-042, SPEC-053). Five specs built that, each closing a case the last one
missed. Today **two walks over the object graph run in every child**. `_fork._reinit_primitives`
starts at every package module and is unbounded by design. `_lifecycle._mark_inherited` walks
again, from the live sinks and the recorded ones, to mark what the child inherited. A third walk,
`_lifecycle._reachable_sinks`, runs only when the library is handed a sink, through `stamp`
(`config.py:257` and `:414`), never in a child.

This spec, the last of the `stability-redesign` arc, makes the child's repair **one traversal** from
the roots SPEC-077 and SPEC-078 gave the runtime. The package's own global locks are re-created from
an **explicit roster**, held complete by a test that fails on a global lock the roster does not
name. Everything else the child must repair is reached from the runtime's roots by
`_reachable_sinks`, extended so that the same traversal also marks what the child inherited. What a
child does with an inherited sink does not change: it delivers to it and never closes it, except a
`FileSink` it re-acquired, which it owns. One thing is new and public. `configure(sink_factory=…)`
lets every process build its own sink, lazily, on its first use, and a forked child never inherits
the parent's.

**This honours the arc's fixed decision that fork keeps one bounded walk from the runtime's roots.**
The walk is one traversal, run once per fork where two run today. It has two frontiers that share
one memo and one `seen` set, so no object is visited twice. Each frontier is bounded:

- The **repair frontier** is bounded by its reach. It enters only package objects and sinks, reads
  a container one level deep, at most `_MARKING_CEILING` members per read, and pushes only sinks and
  package objects. It has no visit cap, for SPEC-039's reason: a cap there would be a lock it could
  not promise to find.
- The **marking frontier** is bounded by `_MARKING_CEILING` visits in total, and by the same
  per-read bound on every container.

The marking frontier keeps the reach of today's marking (FR-003). Deleting that reach would reopen
the two measured destructive closes that
`tests/test_sink_ownership.py::test_a_child_cannot_claim_a_sink_the_parent_never_recorded` pins. In
each, a child re-wrapped an inherited sink the parent never recorded and closed the parent's socket.
Keeping the reach is the orchestrator's decision, taken on 2026-11-04.

**This spec builds on SPEC-068, which has landed, in 1.1.0, and is in `depends_on`.** SPEC-068 resets `Worker._stopped` when a fork resumes a worker
(`architecture.md` §12). This spec leaves `Worker._reinit_after_fork` as SPEC-068 leaves it.

**Measured at `fda134a`, with the instrument named.**

- **The package's global primitives.** The probe imports every `log_foundry` module that imports
  without an optional extra. It lists every `Lock`, `RLock` or `Event` bound to a module global,
  or to an attribute of a package-owned object bound to one, and removes duplicates by identity.
  It finds **nine**:
  - `_lifecycle.py`: `_state._lock` (`:157`) and `_state._stop` (`:160`, replaced at `:272`), and
    the module globals `_closers_lock` (`:357`), `_closing_now_lock` (`:430`) and `_owned_lock`
    (`:624`);
  - `config._config_lock` (`config.py:54`);
  - `decorator._sweep_lock` (`decorator.py:31`) and `decorator._loss_lock` (`:38`);
  - `api._console._lock`, built at `console.py:63` and bound at `api.py:28`.
  It finds eleven bindings, because `worker._lifecycle_state` is a second name for `_state`. After
  SPEC-077 moves three of the module globals onto `_state` and SPEC-078 merges two into
  `_state._closes._lock`, the set is **eight**.
- **What today's repair reaches.** The probe configures a `FileSink` three ways, holds its `_lock`
  on a parent thread across `os.fork()`, and reports from the child. Configured directly, the lock
  is replaced and the record names the child, because the sink re-acquired. Below a `MemorySink`
  subclass holding it two containers down (`self.buried = [[sink]]`), the result is the same,
  because the module-wide walk recurses into containers. **Behind a third-party wrapper**, the lock
  is not replaced and `acquire(timeout=1)` fails, so the child's first emit through that wrapper
  hangs. The record there reads `_FOREIGN`.
- **`_shutdown_running` survives a fork** (SPEC-077's measurement). With the count at 1 in the
  parent, the child's first `@trace` registered its new worker as `_late_worker`.

## Scope

### In Scope

- The global primitive roster in `_fork.py`, its resolution in the child, and the derived test
  that holds it complete, the locks held inside containers included (FR-001).
- One walk from the runtime's roots that repairs, collects hooks and marks, with one memo shared
  with the roster (FR-002 and FR-003).
- Deleting `_fork._reinit_primitives`, `_fork._container_children`, `_fork._is_traversable`,
  `_fork._FORK_SKIP`, `_lifecycle._mark_inherited` and `_lifecycle._inheritance_roots`, and the
  closed list of tests re-expressed with them (FR-003).
- The child's delivery and ownership kept as today, and every holder the new walk reaches
  differently named with what the child does. Also resetting `_shutdown_running` and
  `_late_worker` in the child, the defect SPEC-077 left here (FR-004).
- `configure(sink_factory=…)` and `Config.sink_factory`, with their refusals (FR-005), and the
  lazy, per-process build behind a private wrapper sink, `_FactorySink` (FR-006).
- Refusing at `configure()`, and in `FilteringSink`, `TransformSink` and `MultiSink`, a sink that
  is one of the unpatchable builtin values, through one helper, which closes
  `architecture.md` §12's open item "`configure(sink=object())` is accepted, and every batch is then
  lost on the drain thread" (FR-005).
- The FR count is six, within the aim of 3 to 6.

### Out of Scope

- **SPEC-068's `Worker._stopped` reset on resume**, named in the Overview.
- **A third-party sink's own locks.** The walk replaces primitives only on objects this package
  defines, and on `SinkCounters` reached through `log_foundry_counters` (SPEC-072). A sink that
  satisfies `Sink` structurally keeps its own dead lock in a child, as `architecture.md` §13
  records today.
- **A sink the application holds but never gave the library**: one held in a closure, a user
  global or a framework object, configured for the first time in the child. Neither walk ever
  reached it. It is `architecture.md` §13's first ownership residual, and the remedy is still to
  build the sink in the worker process. `sink_factory=` is now the library's way to do that.
- **A parent-side `before` handler.** It never runs for a C-level fork (SPEC-039).
- **What a child without a factory does with what it inherited.** It still delivers to an
  inherited sink and is refused its close (FR-004).
- **`_FactorySink` offering `deliver`.** It calls its inner sink's `emit`, as `MultiSink` calls each
  child's, so the inner sink's engine runs inside that call (SPEC-072, SPEC-075).
- **An async factory, a factory with arguments, a factory per wrapper child, and a public name for
  `_FactorySink`.**
- **Bounding `Sink.close` or the factory call.** Each is the caller's code, unbounded as
  `architecture.md` §13 records for `close()`.
- **`architecture.md` §12's first open item** (a sink released while live inside the graph that
  replaced it), and the third-party buffers of Kafka and Pub/Sub (§13).
- **Splitting `_lifecycle.py`.**
- **Refusing a `sink=` with no `close`, or any sink that could still gain an `emit`** (FR-005, by
  Andrew's decisions of 2026-11-05). That includes a user-defined class with no `emit` and an
  unpatchable C type outside the closed list, whose batches are lost counted and announced, as at
  `4fbfd60`. It is recorded in §13 as a constraint.

---

## Functional Requirements

### FR-001: The child re-creates the package's global primitives from one roster

#### Description:

A lock that is module-level state, or state on an object bound to a module global, is held by the
package, not by any sink. Today the module-wide walk finds it by starting at every package module.
This spec lists them instead. `_fork._GLOBAL_PRIMITIVES` is a tuple of `(module, path)` pairs, each
a dotted attribute path from a module of this package. The child resolves each pair through
`sys.modules[f"{_PACKAGE}.{module}"]`, so a vendored install still resolves it, and `_fork` still
imports nothing but `_diag` (SPEC-039 FR-006). It replaces each value through `_fresh_primitive`,
using the memo the walk in FR-002 then shares. Using one memo is what keeps every sink's
`log_foundry_stop_signal` the same `Event` as `_state._stop` in the child (SPEC-027).

The eight entries are the Overview's set after SPEC-078:

| `module` | `path` |
|---|---|
| `_lifecycle` | `_state._lock` |
| `_lifecycle` | `_state._stop` |
| `_lifecycle` | `_state._owned_lock` |
| `_lifecycle` | `_state._closes._lock` |
| `config` | `_config_lock` |
| `decorator` | `_sweep_lock` |
| `decorator` | `_loss_lock` |
| `api` | `_console._lock` |

If a pair's module is absent from `sys.modules`, the process never built that primitive, so the
pair is skipped silently. If a pair names a module that is present but an attribute that is not,
`_diag.absorbed` announces it and the other pairs are still replaced. The roster runs first, before
the walk. The handlers that take these locks therefore still find them repaired, and the order of
work in `docs/decisions/pipeline/only-the-forked-child-is-repaired-and-its-order-of-work-is-the-contract.md`
is unchanged.

**Held complete by a derived test, not by care.** `tests/test_fork_roster.py` has two halves, and
each catches what the other cannot.

- **The live half** imports every package module that imports without an extra. From each module
  global it steps through the attributes of package-owned objects, and through the members of
  every container held there, read through `_bounded_children`. The steps stop at sinks and at a
  `Worker`, which are the walk's to reach. A primitive found as an attribute on this path must
  resolve to a roster pair. A primitive found on an object **inside a container** on this path
  fails, because neither the roster nor the walk reaches it. The one exception is a container
  declared as emptied in the child, which the test lists with its handler. That is
  `_state._closes._in_flight`, emptied by `_clear_after_fork`, and the test forks to show it is
  empty there. Duplicates are removed by identity. The test asserts that the attribute
  primitives equal the resolved roster, so a global lock with no entry fails by name, and so does
  an entry whose attribute is gone.
- **The static half** is an AST scan of `src/`. It finds every class that assigns a `Lock`, `RLock`
  or `Event` to a `self` attribute, and requires each to be declared in the test's
  `_PRIMITIVE_HOMES` with one home:
  - `roster`: its instance is on a roster path (`Runtime`, `_CloseRegistry`, `ConsoleWriter`);
  - `walk`: it is sink-shaped, or a `Worker`, or held by an attribute of one (`SinkCounters`,
    SPEC-076's guard);
  - `dropped: <handler>`: a child handler drops its instances (`_Close`, by `_clear_after_fork`;
    `_FlushMarker`, by `_rebuild_worker_after_fork`, which replaces the queue that holds it).
  An undeclared class fails by name, and so does an entry whose class is gone. That catches a new
  lock-holding class even while no instance of it exists, which the live half cannot.

SPEC-077's lifecycle inventory gains a fourth child value, `roster`. Its rows for the eight entries
change from `walk` to `roster`, and the inventory test requires every `roster` row to match a
roster pair.

`tests/test_fork_lifecycle.py::test_every_lock_is_assigned_where_the_walk_can_reach_it` keeps its
shape rule: a module global or a `self` attribute. Its docstring now says which mechanism reaches
each kind. A module global is reached by the roster. A `self` attribute is reached by the walk, or
by the roster when its object is a global, and `tests/test_fork_roster.py` holds both.

**Twin paths (invariant 6).** The roster's locks are taken on both delivery paths:
`_config_lock` on the orphan path's lazy default, and `_state._lock` on the worker's build. One
roster repairs both before either path runs.

#### Acceptance Criteria:

- [ ] `_fork._GLOBAL_PRIMITIVES` holds exactly the eight pairs above, and in a child each resolved
  value is a different object from the parent's, of the same type, with an `Event`'s set state
  carried across.
- [ ] `tests/test_fork_roster.py` passes at the build's head. Each of these fixtures makes it fail,
  naming the site:
  - an extra `threading.Lock()` bound to a package module global;
  - a deleted roster entry;
  - a `_lifecycle` global `_pending: dict[int, _P] = {}` holding one `_P`, where `_P.__init__`
    sets `self._lock = threading.Lock()` (the live half);
  - the same class `_P` with no instance and no `_PRIMITIVE_HOMES` entry (the static half).
  A lock under a `Worker` or a sink leaves it silent.
- [ ] In a child forked while a parent thread holds each roster lock in turn, the child's
  `configure()`, orphan `info()`, a `@trace` call, `flush()`, `health()`, `shutdown()` and a
  console-echoed log all return within 5 s. Removing that entry from the roster turns its case red,
  `decorator._loss_lock`'s through `health()`.
- [ ] A pair whose attribute is missing is announced on stderr by type and does not stop the other
  seven from being replaced.
- [ ] `_fork.py` still imports nothing from the package but `_diag` and `sinks.base`
  (`test_fork_imports_nothing_from_the_package_but_diag` passes unedited).
- [ ] Invariants 1 and 12: a forked child never hangs on a package global lock its parent held, and
  no fault in resolving the roster reaches the caller.

### FR-002: One walk from the runtime's roots repairs what the child can reach

#### Description:

`_lifecycle._reachable_sinks` stays the walk `stamp` runs, with a container scanned one level deep
and only sinks and package objects pushed; its reach is unchanged and its predicates become
static (below). In a forked child, the same function runs
with an `after_fork` argument, a `_fork._Repair` that carries the memo FR-001 used. With that
argument it does more at every holder on its **repair frontier**:

- On a package-owned holder, it replaces every `Lock`, `RLock` and `Event` in the holder's own
  namespace, except the names in the holder's `_FORK_SKIP`, using `_fork._assign` and the shared
  memo. If the holder offers `reacquire_after_fork`, it collects the holder for the hook step. It
  forwards to the marking frontier of FR-003 every value it does not push itself: a container, and
  an object that is neither a sink nor a package object.
- On a sink-shaped holder this package does not own, it replaces nothing. It forwards the
  holder's `_namespace_items` to the marking frontier. Without that step, the holder is already
  in the shared `seen` set when the marking frontier reaches it, so nothing beneath it would ever
  be visited.
- A name in a holder's `_FORK_SKIP` is neither replaced, entered, pushed nor forwarded: forwarding
  `Worker._held` would hook a superseded sink, which is what the opt-out exists to stop.

**A registry of live `SinkCounters` replaces SPEC-072's counters reach.** `sinks/verdict.py`
keeps a `weakref.WeakSet`, `_LIVE_COUNTERS`, of the `SinkCounters` objects that are alive, and
`_walk_after_fork` reads it from `sys.modules` (no import in a child) and repairs the members on
the repair frontier, checking `seen` first so none is repaired twice. It repairs a third-party
engine sink's counters whatever holds them, including a wrapper that keeps only the engine
sink's bound `emit`, which no reading walk reaches. It differs from a registry of every package
primitive holder, which would stop being one walk from the runtime's roots, because invariant 12
names this registry as SPEC-072's follow-up and it covers the one class the walk cannot reach by
reading. `_fork._counters_reach`, `_counters_type`, `_container_push`, `_SCALAR_TYPES`,
`_SKIPPED_TYPE_IDS` and `_COUNTERS_ATTRIBUTE` are deleted with the module-wide walk.

**Every read the walk makes on an object runs none of the object's code**, in both modes of
`_reachable_sinks` and in the marking frontier. Ownership, container, primitive and sink-shape
tests read the real type's MRO and namespaces through `sinks.base`'s unbound accessors and compare
by identity; a namespace is read through `sinks.base._instance_dict`; a slot through its member
descriptor; a container's members through its builtin base type's own iteration, never the
subclass's; `_FORK_SKIP` and `reacquire_after_fork` through `sinks.base._static_value`. A
container subclass is read both ways: its builtin storage and its namespace. The sink-shape test
is `_fork._sink_shaped`, the static twin of `isinstance(x, Sink)`; it differs from `isinstance`
for a class carrying `emit` and `close`, for a value whose `__class__` lies, and for a
`weakref.proxy`, whose C attribute forwarding no static read follows.

**Ownership is recorded by position where position is known, and by shape only where it is
not.** Recording runs none of a value's code — an `id` and a reference — so a value the library
was handed as a sink is recorded unconditionally, whatever a static read says it looks like: the
root `stamp` is handed; a value in a package wrapper's child-sink slot, named in
`_fork._CHILD_SINK_SLOTS` (`MultiSink._sinks`, `_inner` on `FilteringSink`, `TransformSink` and
`_FactorySink`, `Worker.sink`, `Config.sink`, `SwapOutcome.previous`), on both frontiers; and the
live delivery sinks the runtime holds (`_live_sinks`). `tests/test_child_sink_slots.py` holds
the table complete by behaviour — a package class that stores a `Sink`-annotated value on `self`
however it is written, calls `require_sink`, or releases a sink with `owner=self` must have an
entry or a reasoned exemption — and builds each declared class around a sentinel sink, requiring
it recorded. A value `require_sink` would refuse, planted in a slot after construction, is never
recorded. `_fork._sink_shaped` decides
only inside holders this package does not define, where the marking frontier also records a
value whose exact type is a `weakref` proxy. A `super(Sub, inst)` is transparent: its `__self__`
is read in C and walked. A `weakref.proxy`, a `super(...)` or a `__getattr__` proxy configured as
`sink=` or held by a package wrapper is therefore recorded in the parent, closed by the parent,
and stays the parent's in a child, which refuses its close.

**The roots** are the runtime's live delivery targets: the config's sink
(`config._live_config().sink`), `_state._worker` and `_state._late_worker`. A worker is a
package-owned holder, so its `sink`, `_lock`, `_drain_finished`, `_drain_settled` and `_stop` are
reached through it. `Worker._FORK_SKIP` grows from `("_held", "_taken_markers")` to also name
`_queue` and `_thread`. Neither holds a primitive the walk could replace: `_queue` is replaced
whole by the worker rebuild, and `_thread` is a thread that did not survive. Neither holds anything
the marking needs.

`_state` itself is not a root. Its own primitives are on the roster, and its records are not live
state. `Runtime._FORK_SKIP = ("_owned",)` and `_CloseRegistry._FORK_SKIP = ("owed",)` stay as
SPEC-078 leaves them, as a guard for any other route by which the walk might reach `_state`. The
in-flight entries of `_state._closes` are emptied by `_clear_after_fork`, so their events are not
repaired. SPEC-078's inventory row for `_Close.event` changes from `walk` to `none`, "dropped with
its in-flight entry by `_clear_after_fork`", as `_FlushMarker.event` already is: the inventory's
reset check requires a write of the field, and `_clear_after_fork` drops the entry instead.

**`_fork` keeps the order and gains one registration.** `_fork.register_walk(fn)` stores the one
walk to run, and `_lifecycle` registers `_walk_after_fork` at import. A second registration
replaces the first, which is the reload case `_installed` already records. `_reinit_after_fork`
then works in this order:

1. rebind `reacquired_in_child`;
2. replace the roster;
3. run the registered walk with the shared `_Repair`;
4. run the hooks it collected (`_reacquire_transports`, unchanged);
5. run the registered handlers.

A walk that raises is absorbed and announced, the hooks it collected before the raise still run,
and the handlers still run.

**Twin paths (invariant 6).** The worker and the orphan path reach the same configured sink, and the
worker's own sink is reached through the worker. One walk repairs both, so neither path's first
emit in the child meets a dead lock.

#### Acceptance Criteria:

- [ ] Each existing fork test that holds a sink lock across the fork passes with the module-wide
  walk deleted: the direct sink, a user's subclass of a shipped sink, a `MultiSink` child, a
  `deque` and a list-subclass container, an owned container subclass, a cycle, and a foreign
  container subclass left unrewritten.
- [ ] A sink's `log_foundry_stop_signal` and the worker's `_stop` are one object in the child, and
  that object is the one the roster put at `_state._stop`.
- [ ] SPEC-072's fork criteria pass. For a structurally typed engine sink, and for one subclassing
  `log_foundry.Sink`, the child completes a run within 5 s while a parent thread holds the
  counters' lock. A third-party sink whose `__getattr__` raises keeps its other locks unreplaced.
- [ ] SPEC-076's guard on a shipped sink is reached through the sink (its fork criteria pass
  driven through `configure()`).
- [ ] For `configure(MultiSink(ThirdPartyWrapper(conn)))`, the child's walk records `conn`. A walk
  that does not forward a foreign holder's namespace to the marking frontier turns this red.
- [ ] A worker's `_queue` and `_thread` are not entered: a spy on `_fork._namespace_items` records
  neither.
- [ ] A walk that raises after collecting one hook still runs that hook, still runs every handler,
  and announces the raise by type.
- [ ] `test_the_handler_is_registered_once_for_a_double_import` counts calls to the registered walk
  and still reads one per fork.
- [ ] Invariants 3 and 12: a child forked mid-drain delivers its own events, never hangs on a lock
  a parent thread held in any sink or worker reached from the roots, and a backoff in the child is
  still cut short by the child's own stop signal.

### FR-003: The same walk marks what the child inherited, and the separate walks are deleted

#### Description:

Marking is what makes "no record" terminal in a child (SPEC-042 FR-001). Without it, a child that
re-wraps an inherited object the parent never recorded stamps it with its own pid, and then closes
the parent's transport. That was measured on a socket, and
`tests/test_sink_ownership.py::test_a_child_cannot_claim_a_sink_the_parent_never_recorded` pins
both of its shapes. The bounded repair frontier cannot reach either shape: a sink behind a
third-party wrapper, and one two containers down. So the walk keeps today's marking reach in a
second frontier of the same traversal, with the same memo and the same `seen` set.

**The marking frontier** is visited only once the repair frontier is empty. It has two groups of
seeds, visited in this order:

1. **Forwarded seeds**: what the repair frontier forwarded. Everything reached from them is part
   of a live delivery target's graph.
2. **Record seeds**: every sink in `_state._closes.owed`, then every reference in `_state._owned`.
   These are the sinks `_inheritance_roots` added at `fda134a`. The `_owned` references are the
   load-bearing seed for a superseded wrapper
   (`test_a_superseded_wrapper_still_shields_the_transport_beneath_it`).

The first group is drained before the second starts. Because `seen` is shared, an object reachable
from both groups is visited through the first.

How the marking frontier reads and what it changes:

- **How it reads.** It reads exactly as `_mark_inherited` reads today. It skips modules. It reads
  every container through `_bounded_children`. Into every other object that is not a plain
  builtin, third-party ones included, it descends through `_fork._namespace_items`, which runs no
  property and no `__getattr__` on a dictionary attribute.
- **What it replaces.** On every package-owned holder it reaches, it replaces the primitives as the
  repair frontier does.
- **Hooks from forwarded seeds.** On a holder reached from a forwarded seed, it collects
  `reacquire_after_fork`, as the repair frontier does. Such a holder is live: a `FileSink` behind
  a third-party wrapper, or two containers below a package holder, takes events in the child. Its
  pending bytes must be stranded before the child's next flush writes them a second time, which
  `docs/decisions/pipeline/a-value-the-child-inherits-is-stranded-never-merely-detached.md`
  requires and `test_the_same_child_duplicates_when_the_discard_is_taken_away` pins.
- **No hooks from record seeds.** On a holder reached only from a record seed, it **never collects
  a hook**. That keeps SPEC-044 FR-005: a superseded sink, which only `owed` or `_owned` still pin,
  is never re-acquired, so `reclaim` never overrides its record.
- **The order.** Each group is visited first in first out, so every forwarded seed is visited
  before any of their members: a large forwarded container (a `MemorySink`'s events) cannot spend
  the ceiling before a forwarded `FileSink` behind a third-party wrapper is reached.
- **The bound.** It is bounded by `_MARKING_CEILING` visits of the marking frontier across both
  groups (its own visits, not the size of the shared `seen` set). Tripping the bound
  sets `_marking_failed` and writes today's `_diag.lost` line. Because the repair frontier is
  drained first, a trip cannot cost a holder on the repair frontier its repair or its hook.
- **The record seeds are what tripped the ceiling in the suite.** `_state._owned` holds a strong
  reference to every sink the process ever stamped, and no conftest fixture cleared it, so in each
  xdist worker it grew for the whole session until a child's record-seed walk passed
  `_MARKING_CEILING` and announced it. `tests/conftest.py`'s reset now clears the record and
  `_marking_failed`, and the per-file fixture in `tests/test_sink_ownership.py` moves there. The
  same growth is not test-only: a long-lived process that reconfigures with buffering sinks and
  then forks grows the record the same way, which `architecture.md` §13 records as a constraint
  and a separate session fixes.

After both frontiers, every object that the walk recorded — by position or, in a foreign
holder, by shape — is written with
`_owned.setdefault(id(sink), (_FOREIGN, sink))` under `_state._owned_lock`, as `_mark_inherited`
writes it today. A sink the parent stamped keeps the parent's pid. An escaping exception sets
`_marking_failed`, exactly as today.

The first registered handler, `_reclaim_after_fork`, then calls `reclaim` for every sink in
`_fork.reacquired_in_child`. That is the half of `_mark_inherited` that was not a walk, and it keeps
its position: after the marks are written, before any other handler runs.

Every read the walk makes is bounded, because `_fork._container_children`'s unbounded
`list(container)` is deleted with `_reinit_primitives`. So `architecture.md` §12's item
"`_fork._reinit_primitives` can exhaust memory, not merely hang" is closed by this spec. A runaway
container now trips the ceiling in a real child, where today that child is killed by its watchdog
first.

**Deleted:** `_fork._reinit_primitives`, `_fork._container_children`, `_fork._is_traversable`,
`_fork._FORK_SKIP` (no module is walked, so nothing reads it), `_lifecycle._mark_inherited` and
`_lifecycle._inheritance_roots`. The registrations at the foot of `_lifecycle.py` become
`register_walk(_walk_after_fork)` followed by four handlers in this order: `_reclaim_after_fork`,
`_clear_after_fork`, `_adopt_factory_after_fork` (FR-006) and `_rebuild_worker_after_fork`.

**Tests re-expressed**, each keeping its promise and changing only what it drives:

- `tests/test_fork_lifecycle.py`:
  - `test_a_failing_repair_is_absorbed_and_writes_no_message` and
    `test_the_handler_is_registered_once_for_a_double_import` patch the registered walk;
  - `test_the_marking_handler_still_runs_before_the_worker_rebuild` asserts that the marks exist
    when the first handler runs, and that `_reclaim_after_fork` is the first handler;
  - `test_the_owed_swap_record_is_skipped_by_the_repair_walk` and
    `test_the_in_flight_marker_record_is_skipped_by_the_repair_walk` drive `_walk_after_fork` with
    a fresh `_Repair`.
- `tests/test_sink_ownership.py`:
  - `test_a_marking_walk_that_fails_refuses_everything_unrecorded` and
    `test_a_marking_walk_that_fails_still_honours_a_re_acquisition` make the roots raise;
  - `test_a_superseded_wrapper_still_shields_the_transport_beneath_it` builds its precondition from
    the walk's live roots;
  - `test_a_runaway_container_trips_the_ceiling_and_refuses_everything` runs in a real child;
  - `test_the_marking_handler_runs_before_the_worker_rebuild`,
    `test_the_reacquired_roster_does_not_survive_into_the_next_fork` and
    `test_the_reacquired_roster_is_empty_when_the_repair_walk_starts` drive the registered walk;
  - `test_the_fork_repair_walk_does_not_reach_a_superseded_sink` asserts that the superseded sink's
    hook is not called and drops its assertion on `_state._FORK_SKIP` (the module-level
    `_lifecycle._FORK_SKIP` it named at `fda134a`, which SPEC-077 moved onto `Runtime`).
- `tests/test_owed_closes.py::test_a_forked_child_repairs_every_owed_sink_not_only_the_last`
  changes only its docstring.
- `tests/test_lifecycle_races.py::test_a_child_still_refuses_to_close_an_inherited_superseded_sink`
  changes only its docstring.
- `tests/test_worker_predicate_roster.py` re-files `_inheritance_roots`' `_state.worker_exists()`
  site under the roots function of `_walk_after_fork`, which keeps that read. Its `_SITE_FLOOR` for
  `log_foundry._lifecycle` is not lowered.
- `tests/test_fork_lifecycle.py`, beyond the list above:
  `test_without_the_container_branch_the_multisink_child_blocks` and
  `test_without_the_reach_the_same_child_blocks` are re-derived as positive controls with
  `_MARKING_CEILING` patched in the parent, so the repair frontier is observed alone: the first
  takes away the repair frontier's one-level container read, and the `MultiSink`'s positional slot read, on a `MultiSink(FileSink)`, at a
  ceiling of one with the `MultiSink` holding one child (the ceiling also bounds every container
  read); the second removes the counters registry read, at a ceiling of zero (at one the marking
  frontier visits the forwarded counters first and repairs them).
  `test_the_counters_type_is_read_without_an_import` and
  `test_the_counters_reach_refuses_everything_but_exact_counters` keep their names and are
  re-expressed against the registry. `test_the_walk_terminates_on_a_cycle` and
  `test_every_lock_is_assigned_where_the_walk_can_reach_it` change their docstrings.
- `tests/test_lifecycle_races.py::test_a_forked_child_does_not_hook_a_superseded_sink` passes
  unedited; its docstring's claim that removing a `_FORK_SKIP` reddens it is false once `_state` is
  not on the walk, and `test_a_forked_child_does_not_hook_a_superseded_sink_the_worker_holds` is
  the test a forwarded `_FORK_SKIP` name reddens.
- `tests/test_sinks_nats_liveness.py::test_a_forked_child_refuses_without_touching_the_lock_loop_or_client`
  changes its docstring; `tests/test_delivery_engine.py`'s one `_counters_reach` assertion is
  re-expressed as the static reads that replaced it; `tests/test_runtime_holder.py`'s lock-site
  literal, `tests/test_lifecycle_inventory.py`'s rows, `tests/test_lifecycle_transitions.py`'s
  `_move` sites and `tests/test_orphan_sink_handoff.py`'s import set (narrowed to `_diag` and
  `_fork`, since `Sink` is no longer a runtime import) change with the code; `tests/conftest.py`
  clears the ownership record (FR-003's record seeds).

**Tests deleted, because each pins only a removed mechanism:** none. Every test above asserts a
promise that still holds.

**`scripts/docs-lint.sh` check 17** keeps its anchors, since `_FOREIGN` and "marking walk" still
name the mechanism, and adds `_walk_after_fork` to them. `scripts/docs-lint-test.sh` gains one
firing and one silent fixture for the new anchor.

**Twin paths (invariant 6).** `flush()` and `shutdown()` both reach the owed sinks the marking
frontier seeds from. A child's `flush()` asks them and a child's `shutdown()` is refused their
close, both answered from the same record.

#### Acceptance Criteria:

- [ ] `_fork.py` defines none of `_reinit_primitives`, `_container_children`, `_is_traversable` or
  `_FORK_SKIP`, and `_lifecycle.py` defines neither `_mark_inherited` nor `_inheritance_roots`
  (an AST scan).
- [ ] `test_a_child_cannot_claim_a_sink_the_parent_never_recorded` passes for both its shapes, and
  `test_a_superseded_wrapper_still_shields_the_transport_beneath_it` passes. A walk with its
  marking frontier removed turns all three red.
- [ ] `test_the_marking_walk_leaves_a_childs_own_sinks_alone` and
  `test_the_marking_walk_does_not_escape_through_a_module_reference` pass with no edit beyond
  SPEC-077's and SPEC-078's path changes.
- [ ] A forked child whose configured sink holds a `list` subclass with a non-terminating
  `__iter__` never enters that `__iter__`, and a configured sink holding more than
  `_MARKING_CEILING` distinct objects trips the ceiling in a real child, with `_marking_failed`
  true and an unrecorded sink refused.
- [ ] `test_a_forked_child_does_not_hook_a_superseded_sink` passes with no edit beyond SPEC-078's
  path changes. A walk that collects hooks from record seeds turns it red.
- [ ] A child forked while a parent thread is inside `FileSink.emit`, between its write loop and its
  flush, writes none of the parent's pending bytes again. That holds for a `FileSink` behind a
  third-party wrapper and for one two containers below a package holder. A walk that collects no
  hooks from forwarded seeds turns both red.
- [ ] A spy on `_fork._namespace_items` records each object at most once per fork.
- [ ] Every test named above as re-expressed is present at the head under the same name. The
  `pytest --collect-only -q` names at the build's base are all present at the head but one, and
  the only additions are this spec's. The one is
  `test_numeric_door_roster.py::test_removing_a_helper_call_is_reported[worker.py:Worker.max_retries@418]`,
  whose id embeds a line number that `Worker._FORK_SKIP`'s longer docstring moves.
- [ ] `sh scripts/docs-lint-test.sh` passes with the two new fixtures, and removing
  `_walk_after_fork` from check 17's anchors turns the firing one red.
- [ ] Invariants 2, 5 and 12: a child writes no inherited pending byte a second time, releases no
  transport the parent opened in either measured claiming shape, and never re-acquires a superseded
  sink.

### FR-004: The child keeps today's delivery and ownership, and does not inherit a shutdown that is not running

#### Description:

Without a factory (FR-006), a forked child behaves as it does at `fda134a`:

- it delivers to the sink it inherited, on both paths;
- `flush()` asks every inherited owed sink;
- its `shutdown()` and every wrapper's forwarded release refuse to close an inherited sink;
- a `FileSink` or `RotatingFileSink` whose `reacquire_after_fork` returned is re-stamped for the
  child, so the child owns it and closes it (SPEC-039, SPEC-042, SPEC-053).

The existing suites are the proof: `tests/test_fork_lifecycle.py`, `tests/test_sink_ownership.py`,
`tests/test_lifecycle_races.py` and `tests/test_owed_closes.py` pass, edited only as FR-003 lists.

**Every holder the walk now reaches differently**, against the module-wide walk at `fda134a`, and
what the child does with each:

| Holder | At `fda134a` | After this spec |
|---|---|---|
| a sink configured directly, a wrapper's child one container down, a worker's sink | repaired, hooked | the same, on the repair frontier |
| a package object two or more containers below a package holder, such as `self.buried = [[sink]]` | repaired, hooked | repaired and hooked from a forwarded seed, within the ceiling. Past a tripped ceiling it is neither, where the unbounded walk reached it. No shipped class holds this shape |
| a package object inside caller data that a buffering sink holds, such as a sink inside an event's `fields` | repaired, hooked | the same as the row above |
| a package object behind a third-party holder, such as a `FileSink` inside a user's wrapper | **not repaired, not hooked** (the probe's dead lock); marked | repaired and hooked from a forwarded seed, within the ceiling, so the child re-acquires and owns it |
| an owed sink that is not live | not reached by the repair walk (`_owed` was skipped) | repaired from a record seed, never hooked |
| a superseded sink, reached only through `_owned` | skipped | repaired from a record seed, never hooked |
| a third-party sink's own locks | not repaired | not repaired (Out of Scope) |
| a sink in a closure, a user global or a framework object, never handed to the library | not reached | not reached (Out of Scope) |
| in-flight close events (`_Closing` at `fda134a`, `_Close` after SPEC-078) | repaired | dropped with their registry entries by `_clear_after_fork` |

`architecture.md` §13's item "A fork's repair stops at the library's own objects" is rewritten for
the new roots: the walk starts from the runtime's roots, not from the package's modules. Its row for
a package object behind a third-party holder changes from "not repaired" to "repaired and
re-acquired, within the ceiling". Its sixth ownership residual (the stamp walk's container bound)
keeps its wording. The marking that compensates for it still runs.

**A child does not inherit a shutdown that is not running.** `_shutdown_running` counts the
no-worker `shutdown()` calls inside their body. `_get_worker` registers a worker it builds while the
count is above zero as `_late_worker`, for that shutdown to stop (SPEC-044 FR-001). In a child, no
such call survived the fork. The count is still inherited, though, so every worker the child builds
is registered for a shutdown that will never come (SPEC-077's measurement). `_late_worker` is the
count's pair.

`_clear_after_fork` therefore sets `_state._shutdown_running = 0` and `_state._late_worker = None`,
beside the token sets it already empties. The inherited worker keeps the liveness its epoch gives
it. A child forked inside SPEC-078's `_shutdown_running` window, or in the drain's tail
(`_lifecycle.py:1875`), inherits that worker as its live worker either way. Neither field is read by
SPEC-078's `_current` rules.

**A child re-settles what a fork inside a critical section half-wrote.** A fork landing inside one
of SPEC-078's four sections, between a field write and that section's `_move`, leaves a child
whose stored state differs from its derived one: the parent thread holds `_state._lock`, the
roster gives the child a fresh one, and the child sees the fields written up to the fork point.
The same fork inside `_get_worker`, between `_state._worker = worker` and the arm, leaves a live
worker's sink unowed, so a re-acquired `FileSink` is never closed by the child. So
`_clear_after_fork` takes `_state._lock`, arms the live worker's sink when the owed record lacks
it, and calls `_state._move()`, which makes it a fifth `_move` site. SPEC-078's inventory row for
`_current` becomes `("inherited", "reset: _lifecycle._clear_after_fork")`, and
`docs/decisions/pipeline/lifecycle-state-has-one-holder-runtime-and-state-observes-it.md`'s fence
is rewritten to name the fifth site.

The observable asserted is the field values. A caller-visible consequence was not constructed:
once a child builds a worker, every later `shutdown()` in it takes the worker branch, which never
reads `_late_worker`. SPEC-077's inventory rows for both fields change from `inherited` to
`reset: _clear_after_fork`, and its known-defect note is removed.

**Twin paths (invariant 6):**

- On the orphan path and the worker path, the child's first emit reaches the inherited sink through
  the same repaired lock.
- `flush()` and `shutdown()` answer from the same inherited record.
- The worker branch and the no-worker branch of `_shutdown_worker` both start from zero in a child.

#### Acceptance Criteria:

- [ ] The four suites named above pass, with each edit to them confined to FR-003's list.
- [ ] A child forked from a parent that configured a `FileSink` directly delivers its own event to
  the file, reads `releasable` true for it, and closes it at `shutdown()`. A child of a parent that
  configured a `SocketSink` or a `RecordingSink` delivers to it and reads `releasable` false, with
  `closes` unchanged.
- [ ] Each of the probe's three cases reads `repaired=True acquirable=True record=child` in the
  child: the third-party-wrapper case newly, the direct and two-containers-down cases as at
  `fda134a`.
- [ ] `health().inherited_sink` reads true in a child that did not reconfigure, on both delivery
  paths (the existing tests pass).
- [ ] A child forked while a parent thread is inside a no-worker `shutdown()`, held there by an
  injected preemption point in `_close_owed`, reads `_shutdown_running == 0` and
  `_late_worker is None`. Its first `@trace` call builds a worker that is not `_late_worker`.
- [ ] A child forked while a parent's late worker exists has that worker as `_worker`,
  `_late_worker` is `None`, and its `@trace` events are delivered. Removing either reset from
  `_clear_after_fork` turns its case red.
- [ ] SPEC-077's inventory test passes with both rows reading `reset: _clear_after_fork`, and
  SPEC-078's `_move` AST scan finds the four sections and `_clear_after_fork`, named.
- [ ] A child forked while a parent thread is parked inside `_get_worker`'s section, between
  `_worker =` and the arm, reads a stored state equal to the derived one, owes the live worker's
  sink, and closes the `FileSink` it re-acquired at its `shutdown()`. Removing the `_move` or the
  arm from `_clear_after_fork` turns it red. Every parking patch is gated on the parent's pid, so
  the child never runs it.
- [ ] `architecture.md` §13's fork-boundary item names the runtime's roots, and no sentence in
  `docs/architecture.md` or `src/` says the repair walk starts from the package's modules.
- [ ] Invariants 2, 5, 6 and 12: a child forked mid-drain delivers its own events exactly once on
  both paths, never closes a transport the parent opened, closes the re-acquired `FileSink` it
  owns, and stops the worker it built exactly once.

### FR-005: `sink_factory=` — the public surface, and what `configure()` refuses

#### Description:

`configure()` gains a keyword, `sink_factory: Callable[[], Sink] | None = None`, after `max_depth`.
`Config` gains a field, `sink_factory: Callable[[], Sink] | None = None`, after `max_depth`. Both
are keyword-only and appended, so the surface grows by appending (invariant 10). `__all__` gains no
name.

**The contract a caller writes against.** `sink_factory` and `sink` are one setting: where this
process's sink comes from.

- **Both in one call.** `configure()` raises `ValueError` naming both, before anything is assigned,
  because otherwise one would be silently ignored (invariant 13).
- **Each replaces the other.** A later `sink=X` clears `sink_factory` to `None`, and a later
  `sink_factory=g` replaces the sink. A call that passes neither keeps both, as every other field
  is kept ("repeated calls compose").
- **`sink_factory=f`** installs a new `_FactorySink(f)` as the sink. `stamp` records it, and when
  logging has started it is swapped in exactly as `sink=` is, with the same drain, close and
  bounded wait (SPEC-030, SPEC-033). A call passing the factory that is already configured (`is`)
  does nothing: no drain and no close, as with the live sink.
- **The factory is never called by `configure()`** (FR-006 says when it is).
- **`get_config().sink`** is then the `_FactorySink`, a `Sink`. **`get_config().sink_factory`** is
  the callable.

**What is refused where it is written.** A `sink_factory` that is not callable raises `TypeError`
naming `sink_factory` and the type, read through `_diag._STR_STR(_diag._STORED_NAME(type(x)))`. One whose signature cannot bind zero arguments also raises
`TypeError`: `inspect.signature(f).bind()` raises `TypeError`, so `sink_factory=FileSink` is refused
at `configure()`. One whose signature cannot be read, because `inspect.signature` raises
`ValueError`, is accepted. Each is checked with the ceilings and stamps, before anything is
assigned, so a refused call leaves the config and the ownership record as it found them. What a
factory returns can only be known by calling it, so FR-006 gives that case its defined answer.

**`sink=` is refused when it is a value that can never be a sink.** This closes, for what
`configure()` can decide, `architecture.md` §12's open item on `main` (`4fbfd60`):
"`configure(sink=object())` is accepted, and every batch is then lost on the drain thread". Probed
at `4fbfd60`, `configure(sink=object())` then one traced call leaves `flush()` falsy with nothing
delivered. `sink=` is the twin argument of `sink_factory=`, so its refusal lives here and not in
SPEC-072, which fences `configure()` as unchanged. Andrew decided on 2026-11-05, through the
question tool:
- **A missing `close` is never refused.** An emit-only sink delivers every event on `main` today;
  `flush()` returns ok, with one "absorbed a failure while closing the sink" line.
- **Only an unpatchable sink is refused.** A sink is refused when it has no callable `emit` *and* can
  never gain one. A sink may get its `emit` after `configure()`: a class with `emit = None` whose
  `start()` sets `self.emit`, or a `SimpleNamespace` given `emit` later. Both deliver at `4fbfd60`,
  and both are accepted.

**The predicate is a closed list, not the heap-type flag.** "Not a heap type, and no instance
`__dict__`" (`type.__dict__["__flags__"]` without `Py_TPFLAGS_HEAPTYPE`, `1 << 9`, and
`type.__dict__["__dictoffset__"]` of `0`) was probed at `4fbfd60` and over-refuses. It holds for
`weakref.proxy(sink)` and for `super(Sub, inst)`, and both deliver, because a C type's own
`tp_getattro` can forward attribute lookup, and Python cannot tell a forwarding `tp_getattro` from
the generic one. So the refused set is `_sink_arg._UNPATCHABLE`, the exact builtin types `object`,
`str`, `bytes`, `bytearray`, `int`, `float`, `complex`, `bool`, `list`, `tuple`, `dict`, `set`,
`frozenset` and `range`. Each was probed at `4fbfd60`:
- it is not a heap type and has a `__dictoffset__` of `0`;
- `setattr(x, "emit", f)` raises;
- no class in its MRO has an `emit`;
- configured as `sink=`, it loses every batch.

The test is `any(type(value) is t for t in _UNPATCHABLE)`. `type()` reads the object's type
pointer, and `is` compares identities, so neither a metaclass `__eq__` nor anything of the value
runs. A list and not a rule is a deliberate trade: another unpatchable C type (`memoryview`, an
extension type) is accepted and loses its batches counted, as today.

**The accepted limit.** A user-defined class with no `emit` is a heap type, which can always gain
one, so it is accepted. Its batches are lost counted and announced, as at `4fbfd60`:
`failed_batches` on the worker path, `orphan_lost` on the no-worker path. When the build resolves
the §12 item, its closing text says the item closes for what `configure()` can decide. The
user-class case moves to `architecture.md` §13 *Known constraints* as a stated constraint: a sink
`configure()` cannot prove unpatchable is accepted, and if it never gains an `emit` its batches are
lost counted and announced.

**One helper, for `configure()` and the wrappers.** `FilteringSink(object())`,
`TransformSink(object(), f)` and `MultiSink(object())` construct on `main` and then fail every
`emit` (probed at `4fbfd60`). They take the same refusal for their inner-sink arguments, naming
the argument: `inner`, `inner`, and `sinks[i]` for each child of `MultiSink`. The helper is
`require_sink(value, name)` in a new module, `src/log_foundry/_sink_arg.py`. It imports only
`_diag`, a leaf, so the import edges are `config` → `_sink_arg` → `_diag`, plus `sinks/filtering.py`,
`sinks/transform.py` and `sinks/multi.py` → `_sink_arg`. `config` imports nothing from `sinks/`,
and `_sink_arg` imports nothing else from the package, so no cycle is possible. It raises
`TypeError(f"{name} must be a sink with an emit method, got {stored}")`, with `stored` read through
`_diag._STR_STR(_diag._STORED_NAME(type(value)))` and `name` being `sink`, `FilteringSink inner`,
`TransformSink inner` or `MultiSink sinks[1]`.
- **In `configure()`** it runs only when `sink` is not `None`, with the ceilings and stamps, before
  `stamp` and before anything is assigned. The config and the ownership record are left unchanged.
- **In each wrapper** it runs first in `__init__`, before anything is stored.

The wrapper edits are this spec's, after SPEC-075 rewrote the same constructors; the build rebases
onto it.

**The guard.** The `sink_factory` refusal is `config._require_factory(value)`. `_sink_arg.require_sink`
and `config._require_factory` each have the door shape of SPEC-071 FR-005: a `TypeError` naming a
parameter's stored type name. The build adds exactly those two to that guard's pinned door set in
`tests/test_type_door_roster.py`. Neither tests its argument with `isinstance`.

**Test changes, named because this is public surface (invariant 10):**

- `tests/test_public_surface.py` pins `[f.name for f in fields(Config)][-1] == "sink_factory"`, pins
  `sink_factory` as the last keyword-only parameter of `configure` with default `None`, and keeps
  every existing pin unedited but one. `len(log_foundry.__all__)` is unchanged. The one is
  `test_configure_and_config_gain_nothing`, whose promise was another spec's "nothing added": it
  pinned the whole parameter list, so appending `sink_factory` reddens it by design, and it now
  pins the nine names unchanged and in order and the list as exactly those nine plus
  `sink_factory`.
- `tests/typed_consumer/accepts.py` gains `configure(sink_factory=lambda: MemorySink())`,
  `Config(sink_factory=None)` and a read of `get_config().sink_factory`.
- `tests/typed_consumer/rejects.py` gains `configure(sink_factory=3)` and a one-argument factory,
  each with its `want:` line. The one-argument factory is a `def`, not `lambda x: MemorySink()`:
  an unannotated lambda adds mypy's own "Cannot infer type of lambda" beside the planted error,
  and that corpus counts one error per planted line.
- `tests/test_config.py` gains each refusal above, and a test that a refused call leaves
  `get_config()` and the ownership record unchanged.

**Twin paths (invariant 6).** A factory swapped in after logging has started takes `_swap_sink`'s
worker branch and its no-worker branch, as `sink=` does. Both branches are tested. `sink=` and
`sink_factory=` are twin arguments of one call: a refusal of either leaves `get_config()` and the
ownership record unchanged. `configure(sink=)` and the three wrappers' inner-sink arguments are
refused by one helper, so they cannot drift apart. A sink that is accepted but never gains an
`emit` is lost counted on both delivery paths, `failed_batches` on the worker path and
`orphan_lost` on the no-worker path, as at `4fbfd60`.

#### Acceptance Criteria:

- [ ] `configure(sink_factory=f)` with a callable `f` that records its calls returns without
  calling `f`. Afterwards `get_config().sink_factory is f` and `get_config().sink` is a `Sink`.
- [ ] `configure(sink=X, sink_factory=f)` raises `ValueError` naming both. `sink_factory=3` raises
  `TypeError` naming `sink_factory`, and so does `sink_factory=FileSink`. After each,
  `get_config()` equals what it was before the call.
- [ ] `configure(sink_factory=f)` then `configure(sink=X)` leaves `sink_factory` `None` and the sink
  `X`. `configure(sink=X)` then `configure(sink_factory=f)` leaves `sink_factory is f`, and `X` is
  closed once, on both delivery paths.
- [ ] A second `configure(sink_factory=f)` with the same `f` performs no drain and no close.
- [ ] `configure(sink=x)` raises `TypeError` naming `sink` and the type, both with and without a
  running worker, for `x` each of `object()`, `"s"`, `5`, `[]` and `{}`. `get_config()` and the
  ownership record are unchanged, and nothing is stamped. At `4fbfd60` each was accepted, and a
  traced call left `flush()` falsy.
- [ ] `FilteringSink(object())`, `TransformSink(object(), f)` and `MultiSink(MemorySink(), object())`
  raise `TypeError` naming `FilteringSink inner`, `TransformSink inner` and `MultiSink sinks[1]`.
  At `4fbfd60` each constructed and failed every `emit`.
- [ ] Each of these is accepted, and delivers every event as at `4fbfd60`:
  - an emit-only sink with no `close`;
  - a class with `emit = None` whose `start()`, called after `configure()`, sets `self.emit`;
  - a `SimpleNamespace`, a slotted user class, a function and a module, each given `emit` after
    `configure()`;
  - a `__getattr__` proxy over a full sink;
  - `weakref.proxy(sink)`;
  - `super(Sub, inst)` over a sink.
- [ ] A user-defined class with no `emit` is accepted, and its batches are counted in
  `failed_batches` on the worker path and in `orphan_lost` on the no-worker path and announced, as
  at `4fbfd60`. That is the recorded limit.
- [ ] A test checks every member of `_sink_arg._UNPATCHABLE`: its heap-type flag is clear, its
  `__dictoffset__` is `0`, `setattr(x, "emit", f)` raises, and no class in its MRO has an `emit`.
  `weakref.ProxyType` and `super` are not members.
- [ ] During `configure()` and the three wrapper constructors, no `__getattr__`, `__getattribute__`,
  `__class__` getter, metaclass `__name__` or metaclass `__eq__` of the value runs, each counted by
  a probe. `configure(sink=h)` is probed with no logging started and on the swap path; on the swap
  path the one exception is the stop-signal offer, `_lifecycle.offer_stop_signal`, which reads
  `log_foundry_stop_signal` by name and is delivery machinery, excepted by that name.
- [ ] `tests/test_type_door_roster.py`'s pinned door set gains exactly `_sink_arg.require_sink` and
  `config._require_factory`.
- [ ] `tests/test_public_surface.py` and `tests/typed_consumer/` pass with the additions above, and
  every pin that existed at the build's base passes unedited, except
  `test_configure_and_config_gain_nothing` as re-expressed above.
- [ ] Invariants 10, 13 and 6: the surface only grows, every refusal above is raised at the
  `configure()` call that supplied the argument, naming it, and `sink=` and `sink_factory=`, and the
  worker and no-worker paths, behave alike.

### FR-006: The factory builds each process's own sink on its first use, never in the at-fork handler

#### Description:

`_FactorySink` is a private wrapper sink in `src/log_foundry/sinks/_factory.py`. It holds the
factory, a `_build_lock`, and at most one built sink, `_inner`, which starts as `None`. Its
`__init__` assigns those fields and nothing else.

**When the factory runs.** It runs on the first `emit` in each process. That is on whichever thread
emits: the drain thread for span events and the caller's thread on the orphan path. So inside a
span the caller's thread still carries no sink construction (invariant 4).

- The first emit takes `_build_lock`, calls the factory once, checks the result, calls `stamp` on it
  (so the process that built it owns it), forwards the stop signal it was offered, and stores it.
  Later emits read `_inner` without the lock and call `_inner.emit(batch)`.
- Concurrent first emits wait on `_build_lock` and call the factory once between them. It is a
  build lock, not a transport lock: held across the one factory call, never across a delivery.
  (Superseded 2026-11-12: the lock is no longer held across the factory call; see
  `docs/decisions/sink-lifecycle/a-sink-factory-runs-under-no-lock-and-a-build-that-calls-back-releases-its-waiters.md`.)
- An emit made **from inside the factory**, on the same thread, raises `SinkDeliveryError` at once
  rather than calling the factory again, and does not deadlock. On the orphan path that one event
  is counted in `orphan_lost` and announced.
- The configuring process runs the factory on its own first emit, not inside `configure()`, so a
  prefork master that never logs never builds one.

**Lazy in every process is Andrew's decision of 2026-11-04**, taken with two costs stated:

- A factory that raises, or returns no sink, is not refused at `configure()`. It is a counted,
  announced delivery failure on its first use, and the next emit retries it (below).
- `get_config().sink` is the library's private `_FactorySink` wrapper, not a sink the caller
  built. It stays the wrapper after the first build, because the wrapper remains the configured
  sink, and the built sink sits behind it.

The rejected alternative was calling the factory inside `configure()`. That would have refused a
raising factory where it is written, but it would also have built a sink in a master that never
logs. `docs/invariants.md` §13 gains this as a recorded exception: a `sink_factory` is called
lazily, so one that raises or returns no sink is accepted by `configure()`, and each emit until it
builds fails, counted and announced.

**What a failure is.** A factory that raises `Exception`, or returns something that is not a `Sink`
(the `runtime_checkable` test `isinstance(result, Sink)`), fails the build. `_FactorySink` itself
announces the failure through `_diag.absorbed`, naming the factory's exception type, or the
returned object's type, and never a value (invariant 11). It writes that line on the first failure
and on every `WARN_EVERY`-th after it, carrying the running total, because a factory that keeps
failing fails once per emit. It then raises `SinkDeliveryError`, chained from the factory's
exception. A non-sink result is dropped unclosed, since it has no `close` the library can trust.
The raise is an ordinary delivery failure on the path that made the call, which counts and
announces it as it would any sink's:

- on the worker path, the worker retries the batch under its `max_retries` and its stop-signal
  backoff, calling the factory again on each attempt, and if every attempt fails it counts the
  batch in `failed_batches` and writes its abandon line;
- on the orphan path, the event is counted in `orphan_lost` and the path writes its own line,
  which names `SinkDeliveryError`.

The next emit calls the factory again. A `BaseException` from the factory propagates unchanged
(SPEC-025). Invariant 13 cannot hold here, because the call is lazy. This is the defined answer
instead: the failure is counted, announced and retried, and it never reaches the caller.

**What it forwards.**

- `flush()` calls `flush_sink(_inner)` when a sink has been built and does nothing otherwise,
  returning `None` as every shipped wrapper's `flush` does.
- `losses()` returns `read_losses(_inner)` when a sink has been built and `SinkLosses()`
  otherwise. It counts nothing of its own.
- `close()` calls `_lifecycle.release(_inner, owner=self)` when a sink has been built, as the five
  shipped wrappers do, and releases nothing otherwise.
- After `close()`, an emit with a built sink is delegated to it, and the built sink decides
  (invariant 7). An emit with nothing built builds one, since it released nothing (SPEC-032).
- `log_foundry_stop_signal` is a property that stores the event and forwards it to `_inner` once
  built, the shape `FilteringSink` uses.
- It defines no `reacquire_after_fork` and no `deliver`.

**In a forked child.** `_adopt_factory_after_fork` is a handler, registered after
`_clear_after_fork` and before `_rebuild_worker_after_fork`. When `config._live_config().sink_factory`
is not `None`, it:

- builds a new `_FactorySink(factory)`, which does not call the factory, and `stamp`s it for the
  child;
- rebinds the child's config to it;
- if `_state._worker` exists, sets `worker.sink` to it and arms it in `_state._closes`, under
  `_state._lock`, which the roster repaired. The worker rebuild that follows offers it the child's
  stop signal and starts the drain thread.

The handler calls no factory, does no I/O and waits on nothing, so it cannot block `fork()`. The
child's first emit builds the child's own sink, which the child owns and closes at its `shutdown()`.
Because the child's configured sink is its own `_FactorySink`, `health().inherited_sink` reads
false there.

With a worker present, SPEC-078's rules decide `_current` from the worker, so the arm changes no
`_current` and no `_move` is added. Without a worker, nothing is armed until the child's first
orphan emit, which arms through `_note_orphan_emit` and its `_move`.

What the child inherited stays as FR-004 leaves it: the parent's `_FactorySink` and whatever it
built stay owed, are asked by `flush()`, are repaired and hooked by the walk, and are refused their
close. The child sends them no event. A built `FileSink` there is re-acquired by its hook and then
never written, so its descriptor stays open for the child's life.

**A factory returning an object that existed before the fork** is not detected. If the parent
stamped that object, the child delivers to it and is refused its close, as with any inherited sink.
If nothing recorded it, the child claims it, which is `architecture.md` §13's first ownership
residual. `configure()`'s docstring states the rule: the factory must build a new sink on each call.

**The derived rosters gain one class:**

- `tests/test_sink_release_roster.py`'s `_EXPECTED_CLOSERS` gains
  `("sinks/_factory", "_FactorySink.close")` as its eighth site;
- `tests/test_sink_concurrency.py`'s sink roster gains `_FactorySink`, and its floor of 34 holds;
- `test_every_wrapper_forwards_its_own_identity_as_the_owner` gains a `_FactorySink` row;
- `tests/test_fork_roster.py`'s `_PRIMITIVE_HOMES` declares `_FactorySink` as `walk`;
- `tests/test_deferral.py` and `tests/test_diag.py` re-derive their populations, and none of their
  floors is lowered.

**Twin paths (invariant 6):**

- the orphan path and the worker path each run the first build;
- `emit` and `flush` each forward to the built sink;
- `_drain` and `_final_drain` each reach `_FactorySink.emit`;
- `flush()` and `shutdown()` each forward to the built sink.

#### Acceptance Criteria:

- [ ] After `configure(sink_factory=f)`, `f` has been called zero times. The first orphan `info()`
  calls it once on the calling thread. In a fresh configuration whose first use is a `@trace`
  call, the factory is called once, on a thread named `log-foundry-worker`, and the `@trace`d
  function returns before the factory does.
- [ ] Eight threads making a first orphan `info()` at once call `f` once, and all eight events reach
  the built sink.
- [ ] A factory that calls `info()` from inside itself returns, is called once, and leaves
  `orphan_lost` at one, with the built sink receiving the triggering event.
- [ ] A factory that raises `ConnectionRefusedError` on its first two calls and then succeeds is
  driven by three orphan `info()` calls. Afterwards `orphan_lost` is 2, and stderr holds:
  - one `_FactorySink` line naming `ConnectionRefusedError`, from the first failure, the second
    being inside the throttle;
  - two orphan-path lines naming `SinkDeliveryError`.
  The third event is delivered.
- [ ] The same factory on the worker path, with a worker whose `max_retries` is 3, delivers the
  batch with `failed_batches` at 0. A factory that always raises leaves `failed_batches` at 1, with
  the abandon line written.
- [ ] A factory returning `object()` makes the triggering emit count a loss and announce one naming
  `object`, and no `AttributeError` reaches the caller.
- [ ] `shutdown()` after a build closes the built sink exactly once, and `releasable` reads true for
  it. `shutdown()` with nothing built closes nothing.
- [ ] A child forked from a parent that configured a factory, logged and built `I0`:
  - the at-fork handler calls `f` zero times, and a factory that blocks forever does not delay the
    child's return from `fork`;
  - the child's first event lands in a sink `f` built in the child, which is not `I0`;
  - at the child's `shutdown()` that sink is closed once and `I0`'s `closes` is unchanged;
  - `health().inherited_sink` reads false in the child.
  The same holds for a parent with no worker, and for one with a live worker whose drain thread is
  then inside `I0.emit`.
- [ ] A grandchild builds its own sink again, and `configure(sink=X)` in a child leaves its own
  children inheriting `X`.
- [ ] `shutdown(timeout=1.0)` while the drain thread is inside a factory call blocked for 3 s
  returns within the bound `architecture.md` §13 records for a sink blocked inside a call. The
  build then completes, and the built sink is closed at the next `shutdown()` or at exit.
- [ ] `docs/invariants.md` §13 carries the recorded exception above, citing Andrew's decision of
  2026-11-04.
- [ ] Invariants 1, 2, 4, 5 and 12: no factory fault reaches the caller; every event is delivered
  or counted once on both paths; a factory call never runs on a caller's thread inside a span; what
  a process's factory built is closed by that process once; and a child neither builds in the
  at-fork handler nor sends an event to the parent's sink.

---

## Data Model

```
// src/log_foundry/_fork.py

_GLOBAL_PRIMITIVES: tuple[tuple[str, str], ...] = (
    ("_lifecycle", "_state._lock"),
    ("_lifecycle", "_state._stop"),
    ("_lifecycle", "_state._owned_lock"),
    ("_lifecycle", "_state._closes._lock"),
    ("config", "_config_lock"),
    ("decorator", "_sweep_lock"),
    ("decorator", "_loss_lock"),
    ("api", "_console._lock"),
)

class _Repair:                        # one per fork; built by _reinit_after_fork
    memo: dict[int, object]           # id(old primitive) -> its replacement, shared by roster and walk
    keepalive: list[object]           # every replaced primitive, so no id in memo is reused
    hooked: list[object]              # holders offering reacquire_after_fork, outside record seeds
    def repair(self, holder: object) -> None: ...   # replaces holder's own primitives, minus _FORK_SKIP

_walk: Callable[[_Repair], None] | None   # set by register_walk; the one walk

// src/log_foundry/worker.py
Worker._FORK_SKIP = ("_held", "_taken_markers", "_queue", "_thread")

// src/log_foundry/config.py
@dataclass(frozen=True, kw_only=True)
class Config:
    ...                                    # every existing field, unchanged and in order
    max_depth: int = 8
    sink_factory: Callable[[], Sink] | None = None     # appended

// src/log_foundry/sinks/_factory.py
class _FactorySink:                        # private; a Sink
    _factory: Callable[[], Sink]
    _build_lock: threading.Lock            # held across the one factory call; not a transport lock
    _inner: Sink | None                    # at most one per instance; set once
    _building: int | None                  # thread ident inside the factory call, else None
    log_foundry_stop_signal = ForwardedStopSignal()  # stored in __dict__, forwarded once built
    _failures: int                         # build failures, for the throttled line
```

**The child's inventory values that change** (SPEC-077's column, as SPEC-078 leaves it):

| Field | Was | After this spec |
|---|---|---|
| `_state._lock`, `_state._stop`, `_state._owned_lock`, `_state._closes._lock` | walk | roster |
| `_config_lock`, `ConsoleWriter._lock` (as `api._console`), `_sweep_lock`, `_loss_lock` | walk | roster |
| `_state._shutdown_running`, `_state._late_worker` | inherited | reset: `_clear_after_fork` |
| `_Close.event` | walk | none (dropped with its entry by `_clear_after_fork`) |
| `_state._owned`, `_state._marking_failed` | reset: `_mark_inherited` | reset: `_walk_after_fork` |

The inventory's accepted reset functions gain the function registered with `_fork.register_walk`.

## API / Interface Contract

Public, appended:

```
configure(*, service=None, version=None, env=None, sink=None, defaults=None,
          max_value_bytes=None, max_stack_bytes=None, max_keys=None, max_depth=None,
          sink_factory: Callable[[], Sink] | None = None) -> None
    # ValueError if sink and sink_factory are both given; TypeError if sink_factory is not
    # callable or cannot bind zero arguments, or if sink provably has no callable emit
    # (_sink_arg.require_sink: an unpatchable builtin value). Never calls sink_factory.

Config.sink_factory: Callable[[], Sink] | None = None

// Example: one connection per gunicorn worker, configured once in the master
configure(service="api", sink_factory=lambda: SocketSink(host="logs", port=5000))
```

Private:

```
_fork.register_walk(fn: Callable[[_Repair], None]) -> None
_fork._reinit_after_fork()      # 1 rebind reacquired_in_child; 2 roster; 3 walk;
                                # 4 _reacquire_transports(repair.hooked); 5 handlers
_lifecycle._reachable_sinks(root, *, after_fork: _fork._Repair | None = None) -> list[object]
    # after_fork=None: today's bounded walk for stamp, unchanged
_lifecycle._walk_after_fork(repair: _fork._Repair) -> None
    # repair frontier from the live roots; then the marking frontier under _MARKING_CEILING,
    # forwarded seeds (hooked) before record seeds (never hooked); then setdefault(_FOREIGN)
_lifecycle._reclaim_after_fork() -> None        # handler 1
_lifecycle._clear_after_fork() -> None          # handler 2; + _shutdown_running, _late_worker
_lifecycle._adopt_factory_after_fork() -> None  # handler 3
_lifecycle._rebuild_worker_after_fork() -> None # handler 4, unchanged
```

## Configuration / Environment

One new `configure()` keyword and `Config` field, `sink_factory` (FR-005). No environment variable.

## File & Folder Structure

```
src/log_foundry/_fork.py              # roster, _Repair, register_walk; module-wide walk deleted
src/log_foundry/_lifecycle.py         # _walk_after_fork, _reclaim_after_fork, _adopt_factory_after_fork;
                                      # _mark_inherited and _inheritance_roots deleted; _clear_after_fork resets
src/log_foundry/worker.py             # Worker._FORK_SKIP
src/log_foundry/config.py             # sink_factory on configure() and Config; _require_factory
src/log_foundry/_sink_arg.py          # require_sink, _UNPATCHABLE (new; imports only _diag)
src/log_foundry/sinks/{filtering,transform,multi}.py   # require_sink on the inner sinks
src/log_foundry/sinks/_factory.py     # _FactorySink (new)
tests/test_fork_roster.py             # FR-001 (new)
tests/test_sink_factory.py            # FR-005 and FR-006 (new)
tests/test_fork_lifecycle.py, test_sink_ownership.py, test_lifecycle_races.py, test_owed_closes.py,
tests/test_worker_predicate_roster.py, test_sink_release_roster.py, test_public_surface.py,
tests/test_config.py, typed_consumer/accepts.py, typed_consumer/rejects.py
scripts/docs-lint.sh, scripts/docs-lint-test.sh, tests/docs-lint/   # check 17's new anchor
docs/architecture.md                  # §9's fork paragraph, §12 items closed, §13 fork boundary
tests/test_type_door_roster.py        # SPEC-071's guard: two doors added to its pinned set
docs/invariants.md                    # §13's recorded exception for sink_factory
docs/decisions/pipeline/, docs/decisions/sink-lifecycle/
```

## Implementation Phases

### Phase 1: The roster and the one walk (FR-001, FR-002, FR-003)

- Add `_GLOBAL_PRIMITIVES`, `_Repair` and `register_walk`, and reorder `_reinit_after_fork`.
- Extend `_reachable_sinks` with `after_fork`, and write `_walk_after_fork` with both frontiers,
  the two seed groups and `_reclaim_after_fork`.
- Delete the module-wide walk, `_mark_inherited` and `_inheritance_roots`, and extend
  `Worker._FORK_SKIP`.
- Write `tests/test_fork_roster.py`, both halves, with its fixtures. Re-express FR-003's tests and
  diff the `--collect-only` names on both refs. Re-file the predicate roster. Add check 17's anchor
  and its fixtures.

### Phase 2: The child's state (FR-004)

- Add the two resets to `_clear_after_fork`. Update SPEC-077's inventory rows.
- Run the four fork suites, add the probe cases and the duplicate-bytes cases as tests, and
  rewrite `architecture.md` §9, §12 and §13 for the new roots.

### Phase 3: `sink_factory=` (FR-005, FR-006)

- Add the keyword, the field and the refusals, `_sink_arg.require_sink` at `configure()` and in the
  three wrappers, `config._require_factory`, and the two door-set entries,
  and `_FactorySink` with its first-build lock, its
  same-thread guard and its throttled line.
- Add `_adopt_factory_after_fork` and its registration.
- Write `tests/test_sink_factory.py`. Add the public-surface pins and typed-consumer lines. Extend
  the derived rosters.
- Run the mutation set: each roster entry, both halves of the roster test, both resets, the
  foreign-holder forward, the no-hook rule for record seeds, the hook rule for forwarded seeds, the
  same-thread guard and the at-fork handler's no-call rule.

## Documentation

The build rewrites
`docs/decisions/pipeline/only-the-forked-child-is-repaired-and-its-order-of-work-is-the-contract.md`
in place, with a dated History entry. Its "the repaired roster is **derived, never listed** — a
walk over this package's own objects" becomes the package's global primitives **listed**, in one
roster held complete by a derived test, and everything else reached by one walk from the runtime's
roots. The order of work is kept, with the roster added as its first step.

`docs/decisions/sink-lifecycle/a-process-releases-only-a-transport-it-acquired-here-and-unrecorded-must-be-unclaimable-rather-than-merely-unreleasable.md`
gains a dated History line: the marking now runs inside the one walk, as its marking frontier,
and its reach is unchanged.
`docs/decisions/pipeline/a-value-the-child-inherits-is-stranded-never-merely-detached.md` gains a
dated History line: the hooks are collected from the repair frontier and the forwarded seeds, so a
live sink behind a third-party holder is now stranded too.

`docs/invariants.md` §12's recorded exception changes: it named the counters reach and the
third-party-wrapper boundary, and the registry and the forwarded seeds change both. `architecture.md`
§13 gains the residual the registry leaves for every other class: a package object reachable only
through a bound method, a closure cell or a `functools.partial` (`self._emit = file_sink.emit` in
a third-party wrapper) is neither repaired nor marked, because descending into callables
routinely escapes into application state and would trip the ceiling in every child.

`docs/invariants.md` §13 gains the recorded exception FR-006 states, citing Andrew's decision of
2026-11-04.

`architecture.md` §12's item "`configure(sink=object())` is accepted, and every batch is then lost
on the drain thread" moves to *Resolved*, closed by SPEC-079 for what `configure()` can decide. Its
closing sentence ("no callable `emit` and `close`") is rewritten to the rule FR-005 states. §13
*Known constraints* gains the user-class case: a sink `configure()` cannot prove unpatchable is
accepted, and if it never gains an `emit` its batches are lost counted and announced.
`docs/component-inventory.md` gains a row for `_sink_arg`. One new decision in
`docs/decisions/sink-lifecycle/` records `sink_factory=`, citing Andrew's
decision of 2026-11-04 and its two costs, and the `sink=` refusal with Andrew's decisions of
2026-11-05 (no `close` requirement; only an unpatchable sink is refused). It is lazy in every process, never called in the at-fork
handler, refused at `configure()` for what can be checked there, and a failed build is a counted
delivery failure that the next emit retries.

`docs/component-inventory.md`'s fork and lifecycle rows change, and it gains a row for
`_FactorySink`. `README.md`'s prefork advice names `sink_factory=`. The release notes for 1.2.0
carry `sink_factory=` under *New* and the third-party-wrapper repair under *Fixed*.

## Revision history

- Drafted from the stability plan's Change 4, items 1 to 5, and its step 10, on SPEC-072, SPEC-077
  and SPEC-078 as drafted. The plan's three walks reproduce at `fda134a`, but only two of them run
  in a child: `_reachable_sinks` runs only through `stamp`. The plan's global-lock list (the
  runtime's, `config`'s, the decorator's and the console's) reproduces as nine primitives at
  `fda134a`, and eight after SPEC-077 and SPEC-078.
- The plan's "the separate inheritance marking is deleted" is met as a **separate walk deleted**,
  not as marking deleted. The bounded walk alone cannot reach either shape
  `test_a_child_cannot_claim_a_sink_the_parent_never_recorded` pins, and each was a measured
  destructive close of a parent's socket. So the marking reach is kept as the second frontier of
  the one walk.
- Aligned with SPEC-078 as revised after its review. `_current` never reads the config term and is
  `inherited` in a child, and `_clear_after_fork` calls no `_move`. So FR-004's resets and FR-006's
  arm-with-a-worker add no `_move` site. An earlier idea, dropping the inherited sinks from a
  factory child's owed record, was abandoned: it would change the owed record when there is no
  worker, which SPEC-078's four `_move` sites do not cover.
- Re-checked against SPEC-078 as revised:
  - `closing_count` is gone, replaced by `Runtime._closes.graced_alive()`; this spec names neither;
  - `_current` never includes the config term;
  - `_clear_after_fork` calls no `_move`, and `_current`'s child value is `inherited`;
  - `BUILD` × `DRAINING` is split by the `_shutdown_running` window (`_lifecycle.py:1875`).
  FR-004's reset agrees with that split.
- After the spec was drafted, Andrew decided on 2026-11-04 that the factory is lazy in every
  process (FR-006), and the orchestrator decided the same day that the marking's reach is kept
  (Overview). Both are recorded where they apply.
- Revised after the spec review:
  - The marking frontier now hooks what it reaches from forwarded seeds, and only record seeds stay
    unhooked. An unhooked live `FileSink` would have written the parent's pending bytes again,
    which the stranding decision forbids.
  - A foreign holder on the repair frontier forwards its namespace, so the shared `seen` set
    cannot hide what sits beneath it.
  - The roster test gained a container descent and a static half, which catch a lock held inside
    a container.
  - The Overview now counts two walks per fork, and states each frontier's bound honestly.
  - `_FactorySink` announces its factory's exception type itself, throttled.
  - Invariant 13's recorded exception is listed.
  - `health()` joined the roster's call list, so removing `_loss_lock` reddens.
  - The two reset requirements were folded into FR-004, which brings the FR count to six.
- Build-order re-read: FR-003's dropped assertion names `_state._FORK_SKIP`, the name SPEC-077
  leaves, not the removed module-level `_lifecycle._FORK_SKIP`.
- 2026-11-05, the SPEC-080 follow-ups folded into the arc, at Andrew's direction: FR-005 refuses a
  `sink=` that provably has no callable `emit`, closing `architecture.md` §12's open item, on
  Andrew's two decisions of that day (no `close` requirement; only a provable absence, read
  statically). The descriptor rule (a found descriptor is accepted uncalled) and the shadowed-`__dict__`
  case were settled by probe at `4fbfd60`. The `sink_factory` refusal names its type's stored name,
  and both doors join SPEC-071 FR-005's guard.
- 2026-11-05, the fold-in's review: FR-005's static-lookup rule refused sinks that deliver, one
  whose `emit` is bound after `configure()`, so it is replaced on Andrew's decision of that day:
  only an unpatchable sink is refused. The heap-type predicate the review proposed was probed and
  also refuses `weakref.proxy` and `super` objects, which deliver, so the refused set is a closed
  list of builtin types. The same helper, `_sink_arg.require_sink`, now refuses the wrappers' inner
  sinks. The `sink_factory` refusal is named `config._require_factory`, and the user-class case
  moves to §13.
- 2026-11-05: rebased onto main after v1.1.0; the landed peer specs are now in depends_on.
- Build: the `test_a_child_of_a_process_that_built_no_worker_is_silent` failure on `44db79a6` was
  the marking ceiling tripping on an ownership record that grows for the life of the process,
  which this spec's record seeds keep. Measured at `44db79a6`: one xdist worker ended a full run
  at 92,940 marking visits, and a two-test reproducer fails deterministically. The conftest reset
  now clears the record, which fixes the suite; a long-lived process that reconfigures and forks
  grows it the same way, recorded in `architecture.md` §13 and handed to its own session (A1).
- Build: `configure(sink=h)` at `44db79a6` ran a hostile sink's `__class__` getter,
  `__getattribute__` and metaclass `__hash__` and `__eq__`, all from `stamp`'s walk, so FR-005's
  no-hook criterion could not hold with `_reachable_sinks` unchanged. The walk's predicates became
  static, which also closes SPEC-072's follow-up on hostile sink code at the at-fork sites, and
  FR-003's runaway criterion is restated: the runaway `__iter__` is never called, so the ceiling
  is asserted on a genuinely large graph (A3).
- Build, on Andrew's standing rule to harden rather than defer: invariant 12 names a registry of
  live `SinkCounters` as this spec's follow-up, and the forward made the static counters read an
  equivalent mutant within the ceiling (measured by tracing `_forked_engine_run`). The registry
  repairs every live counters whatever holds it, including a wrapper that keeps only the engine
  sink's bound `emit`, which no reach finds. The two unit tests of the removed reach keep their
  names, re-expressed against the registry (A4).
- Build: the marking frontier visits first in first out and counts its own visits, so a
  forwarded seed cannot lose its repair to the caller data of another seed (A5).
- Build: a name in a holder's `_FORK_SKIP` is neither replaced, entered, pushed nor forwarded (A6).
- Build: the deletion reddened tests outside FR-003's list; each is listed with what changes. The
  positive controls patch the ceiling to one and keep their `MultiSink` at one child, or to zero
  for the registry control, rather than splitting the per-read bound into a constant of its own
  (A7).
- Build: three stale references corrected against `44db79a6` (`_fork` imports `sinks.base` too;
  `SocketTransport` holds a guard, not a primitive; the record seeds' predecessor is named as
  history), and the collect-only rename recorded (A9).
- Build: the inventory's reset check requires a write of the field, so a dropped `_Close.event`
  is `none`, as `_FlushMarker.event` already is (A10).
- Build: the Documentation section gains `docs/invariants.md` §12, whose recorded exception
  named the counters reach and the third-party-wrapper boundary, and the §13 residual for a
  package object reachable only through a bound method, closure or partial (A11).
- Build: a fork landing between a section's field write and its `_move` left a child whose stored
  state differs from its derived one, and a fork inside `_get_worker` between `_worker =` and the
  arm left a live worker's sink unowed (a re-acquired `FileSink` then never closed by the child).
  `_clear_after_fork` re-settles both, which makes it a fifth `_move` site, and the lifecycle-state
  decision's fence is rewritten to name it (A2).
- Build: one public-surface pin is edited. `test_configure_and_config_gain_nothing` pinned the
  whole parameter list, so appending `sink_factory` reddens it by design; it now pins the prefix
  and the one appended name (A8).
- Build: FR-005's no-hook criterion keeps the swap path and excepts the stop-signal offer by name,
  since that offer is delivery machinery (A9 as revised by the plan review).
- Build: `_FactorySink`'s lock is `_build_lock`, a build lock rather than a transport lock, so the
  sink-concurrency lints read it as what it is; its stop signal is stored through
  `ForwardedStopSignal`, as `FilteringSink`'s is; its `flush` returns `None` as every shipped
  wrapper's does; and the typed consumer's one-argument factory is a `def` (A12).
- Build, after the first diff review: under static reads `stamp` stopped recording a
  `weakref.proxy` configured as `sink=` (the same hole existed at base for a `__getattr__` proxy and
  `super(Sub, inst)`), and the child's walk did not mark it, so a child whose walk succeeded found
  it releasable and its `shutdown()` closed the parent's transport. `stamp` now records the root it
  is handed, and the walk records every live delivery sink, both unconditionally; the earlier
  sentence saying such a proxy was refused its close was false and is replaced. The same review
  made a class never offer `reacquire_after_fork`, and pinned three `sink_factory` behaviours.
- Build, after the second diff review: a static shape predicate answering differently from
  `isinstance` changed an ownership record for the third time, now for a `weakref.proxy` held as a
  package wrapper's member (closed by nobody in the parent, claimable in a child). The mechanism
  changed rather than one more position being patched: ownership is recorded by position wherever
  position is known — the root, a package wrapper's child-sink slot from `_fork._CHILD_SINK_SLOTS`,
  a live delivery sink — and by shape only inside foreign holders, where a `weakref` proxy is
  recorded by its exact type. A slot roster and a differential corpus over every recording site
  and degenerate shape hold it. `super(Sub, inst)` became transparent to the walk, so a sink behind
  one is repaired; a sink behind a `weakref.proxy`, and a child's bare re-hand of an unrecorded
  sink after a tripped ceiling, are recorded as constraints.
- Build, after the execution review of the record-by-position change: the slot roster keyed on
  how a value is stored missed most natural future wrappers, so it is keyed on behaviour instead
  (a `Sink` annotation anywhere flowing to a `self` attribute, `require_sink`, `release(owner=self)`),
  with runtime keys and a sentinel per declared class; `LogstashSink` and `SentrySink` joined the
  table. A planted non-`str` namespace key is dropped before it is hashed; a slot value
  `require_sink` would refuse is not recorded; `_live_sinks` no longer lists a factory's built
  sink, which the `_inner` slot records; `MultiSink.close` closes a member held twice once.
