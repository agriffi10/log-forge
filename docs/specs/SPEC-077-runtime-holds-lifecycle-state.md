---
type: Spec
id: SPEC-077
title: "Runtime Holds Today's Lifecycle State, With No Behaviour Change, and the Model Test Drives It"
description: "Moves the lifecycle bookkeeping that lives in module globals onto one Runtime object, adds a State read off today's flags, and extends the model test to drive every lifecycle call against a state table."
spec_status: In Progress
depends_on: [SPEC-045, SPEC-046, SPEC-050, SPEC-054, SPEC-062, SPEC-066, SPEC-068]
arc: stability-redesign
arc_order: 9
---

## Overview

Fourteen specs of lifecycle repair share one cause, and `_lifecycle._Lifecycle` already named it
in SPEC-040: no single object says what state the process is in. Its docstring names four states
(cold, orphan-only, worker-backed, retired) and says which field decides each, but the real state
also depends on flags held on `Worker` and on module globals that each carry a lock of their own,
and nothing states how they combine. This spec is the first of three that fix that, and it changes
**no behaviour**. It gives the bookkeeping one home, `Runtime`; it names the process's five states
in an enum, `State`, that is **read off the flags that exist today** and decides nothing; and it
extends `tests/test_invariants_model.py` so that every lifecycle call is driven against a state
table and checked after every step. The next spec in the arc, SPEC-078, adds the owed-close
registry and makes the transitions checked, so that `State` becomes the authority; SPEC-079 then
reworks the fork repair from `Runtime`'s roots. Neither is built here, and this spec is shaped so
that each starts from a `Runtime` that already holds what it changes.

Two specs have landed, in 1.1.0, and this one builds on both: SPEC-066 (the `flush` and `shutdown`
timeout verdicts, which put a door at the public calls and leave `_shutdown_worker`, `claim_stop`
and `Worker.stop` alone) and SPEC-068 (shutdown expiry: `architecture.md` §12's double announce of
an expired exit drain, a second `shutdown()` after an expired one, and a fork-resumed worker not
resetting `Worker._stopped`). Both are in `depends_on`. This spec does not re-specify any of those
fixes and writes nothing that depends on how they come out, which is why `Worker._stopped` stays on `Worker` (FR-001).

**What "today's state" is, measured at `fda134a`.** The plan this arc follows counted eleven lock
and event sites across `_lifecycle.py` and `worker.py`; that reproduces, with
`grep -nE 'threading\.(Lock|RLock|Event|Condition|Semaphore)\(' src/log_foundry/*.py`, which finds
them at `_lifecycle.py` lines 157, 160, 272, 357, 390, 430 and 624 and `worker.py` lines 307, 413,
414 and 420, plus eight more outside the pair: `config.py:54`, `console.py:63`, `decorator.py:31`
and `:38`, and `_fork.py:25`, `:27`, `:351` and `:355` (two type probes at import and the repair's
two replacement factories, which hold no state). The grep cannot see `Worker._queue`, whose `queue.Queue` holds a mutex
and three conditions of its own. The plan's field list is also short of what the code holds:
`_Lifecycle` has `_atexit_registered` as well, and `Worker` has `_stop`, `_exit_deadline`,
`_in_flight`, `_thread` and `_queue`, of which `_exit_deadline` is SPEC-062's. The complete
inventory, field by field and lock by lock, is the Data Model.

## Scope

### In Scope

- Renaming `_lifecycle._Lifecycle` to `Runtime`, keeping the module global `_state` as its one
  instance, and moving the seven module-level names that hold lifecycle state — `_closers`,
  `_closers_lock`, `_closing_now`, `_closing_now_lock`, `_owned`, `_owned_lock` and
  `_marking_failed` — onto it as attributes, with every acquisition site unchanged (FR-001).
- The `State` enum and the read-only `Runtime.state` that derives it from today's flags, with the
  two token sets that make the swap and the stop visible, and their reset in a forked child
  (FR-002).
- A derived inventory test that names the home of every lifecycle field and every lock or event
  site, so that "nothing else holds lifecycle state" is a gate rather than a sentence (FR-003).
- The model test's extension: a sequential, seeded driver over `configure`, swap, `flush`,
  `shutdown`, the exit drain and fork, checked against a state table after every step (FR-004).
- The proof that nothing a caller can see changed, and the closed list of tests edited because
  they pin where a name lives (FR-005).
- The FR count is five, within the aim of 3 to 6; the arc's step 8 needed no merge or split.

### Out of Scope

- **Checked transitions.** No call is refused, answered or ordered by `State`; `configure()` after
  a `shutdown()` is accepted exactly as it is today. This spec records the transitions the code
  makes (FR-004's table) and does not forbid any of them. That, and the owed-close registry that
  replaces `_closers` and `_closing_now`, are SPEC-078's. Those two are *moved* here and not
  reshaped, so that SPEC-078 changes a field of `Runtime` rather than a module global.
- **Fork.** `_fork.py`'s walks, `_mark_inherited`, `reacquired_in_child`, the lock roster and
  `sink_factory=` are SPEC-079's. This spec touches fork in exactly three ways: `_owned`'s opt-out
  from the repair walk moves with it to `Runtime._FORK_SKIP` (FR-001), the two new token sets are
  emptied by the child handler that already clears the in-flight close registry (FR-002), and the
  model test forks (FR-004). What a child inherits of the *existing* fields is not altered.
- **A known defect, left for SPEC-079.** A child inherits `_shutdown_running` as the parent had
  it, and the call that raised it did not survive the fork. Measured at `fda134a` with the count
  set to 1 in the parent to stand in for a thread inside `shutdown()`'s no-worker branch, the
  child's first `@trace` registered its new worker as `_late_worker`; with the count at 0 it did
  not. Resetting it is not a one-line change, because `_late_worker` is its pair, and it is a
  behaviour change; this spec neither fixes nor pins it.
- **One lock.** The plan's "one lock" is not reached here, for the reason FR-001 gives. `Runtime`
  holds four locks after this spec, each guarding what it guards today.
- **A `Runtime.sink` field.** The sink being delivered to is held twice today, by `Worker.sink`
  and by the config, and the two differ on purpose after a declined swap (`_worker_health`'s
  docstring); a third copy would make a deliberate divergence into a three-way one.
- **`Worker`'s drain state** moving anywhere. Each field that stays is named and justified in the
  Data Model.
- **Splitting `_lifecycle.py` and `worker.py`**, `architecture.md` §12's unscheduled item.
- **SPEC-066's and SPEC-068's fixes**, named in the Overview, and the existing `_shutdown_running`
  and `_late_worker` semantics.
- **`config`, `decorator` and `console` locks**, which hold no lifecycle state; the fork roster is
  SPEC-079's.
- **A public name.** `Runtime` and `State` are private to `log_foundry._lifecycle`.

---

## Functional Requirements

### FR-001: One holder for the lifecycle bookkeeping

#### Description:

`_Lifecycle` becomes `Runtime`, and the seven module-level names listed under *In Scope* become
attributes assigned in `Runtime.__init__`, with their docstrings. They move, with no alias left at
module scope, because a second home is the defect this spec exists to remove. `_state` stays the
module global and the one instance, so `worker.py`'s `_lifecycle_state = _lifecycle._state` binding,
every read of `_state._worker`, `_owed`, `_lock`, `_stop` and `retirements` in `src/` and `tests/`,
and the `@trace` hot path are untouched. The functions that use the moved names (`_closing`,
`_register_closing`, `_finish_closing`, `_clear_after_fork`, `_close_owed`, `_start_closer`,
`join_closers`, `closing_count`, `_inheritance_roots`, `_mark_inherited`, `reclaim`, `stamp`,
`releasable`) stay module functions and read `_state._<name>`; no body changes except the path.
The module `_FORK_SKIP = ("_owned",)` goes, and `Runtime._FORK_SKIP` becomes
`("_owed", "_owned")`, because `_fork._skipped_names` reads the opt-out off the holder of the
attribute, and `_owned` now lives on `_state`.

**Why four locks remain.** Folding `_closing_now_lock` into `_state._lock` would make `in_flight`
and `held` deadlock: both are asked with `_state._lock` already held, and they take the leaf
`_closing_now_lock` by design (`_Lifecycle.in_flight`'s docstring). `_closers_lock` and
`_owned_lock` are taken beneath `_state._lock` by `release` and `releasable` and on closer threads
beside it, and `_state._lock` is held across a blocking `Worker(_ensure_sink())` build. Merging
them changes the lock order, which is behaviour. The order is kept as the moved docstrings state
it: `_state._lock` then `_config_lock` then `_owned_lock`, and `_state._lock` then `Worker._lock`,
`_closers_lock` and `_closing_now_lock` as leaves, with nothing acquiring `_state._lock` beneath any
of them.

**Twin paths (invariant 6).** The orphan and worker paths already share these records; nothing
here splits them. The fork child's `_clear_after_fork`, which empties `_closing_now`, and the swap's
and the shutdown's closes through `release`, are the sites that read the moved names;
each keeps its body except for the path and, in `_clear_after_fork`, FR-002's added reset.

#### Acceptance Criteria:

- [ ] `log_foundry._lifecycle` has a class `Runtime` and no `_Lifecycle`; `_state` is an instance
  of `Runtime` and no other instance is built at import.
- [ ] An AST scan of `_lifecycle.py` finds none of `_closers`, `_closers_lock`, `_closing_now`,
  `_closing_now_lock`, `_owned`, `_owned_lock`, `_marking_failed` or `_FORK_SKIP` bound at module
  level, and finds each of the first seven assigned on `self` in `Runtime.__init__`.
- [ ] `Runtime._FORK_SKIP == ("_owed", "_owned")`, and a child forked after `configure(A)`,
  `info()`, `configure(B)` runs no fork hook on the superseded `A` (the existing SPEC-044 FR-005
  fork test, edited only for the path).
- [ ] The set of `with <lock>:` sites in `_lifecycle.py` and `worker.py`, each as the function that
  holds it and the lock it takes, equals the set **derived on the build's base commit**, which a
  test in this spec holds as a literal, with a moved lock compared under its new path. Not the set
  at `fda134a`: SPEC-072, earlier in the arc, rewrites `Worker._emit` and adds `_retry_declined`, so
  the literal is recorded in this spec's first phase from the unmodified base, the way SPEC-071
  records its pins.
- [ ] No docstring in `src/` names `_Lifecycle` or a removed module-level name as a module
  attribute: ``grep -rnE '\b_Lifecycle\b|_lifecycle\.(_closers|_closing_now|_owned|_marking_failed|_FORK_SKIP)\b|:data:`(_closers|_closers_lock|_closing_now|_closing_now_lock|_owned|_owned_lock|_marking_failed|_FORK_SKIP)`' src/``
  is empty. At `fda134a` it finds 30 lines: 13 naming `_Lifecycle`, 13 `:data:` references and 4
  module-attribute paths.
- [ ] Invariants 5 and 12: the existing ownership, owed-close and fork-lifecycle tests pass, so a
  sink is still closed once by one owner and a forked child still releases only what it acquired.

### FR-002: `State` is read off today's flags, and is total

#### Description:

`State` has five members: `UNCONFIGURED`, `RUNNING`, `SWAPPING`, `DRAINING` and `CLOSED`, the
plan's five. `Runtime.state` is a property that derives one from fields the runtime already keeps,
plus two sets this spec adds. Nothing in `src/` reads it; it is an observer, so a wrong derivation
can fail a test and cannot change a delivery. The derivation reads each field once into a local,
takes no lock, and tests in this order:

1. `_stops_running` non-empty is `DRAINING`. `_shutdown_worker` adds a per-call token to it in its
   first critical section, the one that moves the retirement count, **before** it sets
   `_state._stop`, and discards the token in a `finally` after `join_closers`. That covers the
   worker branch, the no-worker branch, the late worker and the second `_close_owed` pass. The exit
   drain is the same function with the same default budget as `shutdown()`, so it is the same state.
2. `_swaps_running` non-empty is `SWAPPING`. `_swap_sink` adds its token in its first critical
   section, after its early return for no live worker and nothing owed and **before** it releases
   any stale sink, and discards it in a `finally` after it has joined its closers.
3. `live_worker()` not `None` is `RUNNING`: a worker whose epoch is the current retirement count,
   which includes a worker built after a `shutdown()` returned (SPEC-044 FR-001).
4. `retirements > 0` is `CLOSED`: retired, with no live worker. This matches `health().retired`
   being true and nothing being drained.
5. `_owed` non-empty, or the configured sink resolved, is `RUNNING`. The configured sink is the one
   input the runtime does not own, read through `config._live_config()` because the config is the
   recorded authority for the installed sink (`architecture.md` §12, `_worker_health`). It makes
   `configure()` leave `UNCONFIGURED`, as the plan's diagram has it, since `configure()` resolves
   the sink unconditionally (`config.py`, `_ensure_sink()`).
6. Otherwise `UNCONFIGURED`.

**Sets of tokens, not counters, and why.** Each is a depth count — its size is the number of
calls inside — for `_shutdown_running`'s reason: two concurrent `configure(sink=…)` or `shutdown()`
calls are documented as normal, and a boolean cleared by the first to finish would misreport the
second. A token is a fresh `object()` per call. It is **added** inside a critical section the call
already has, and **discarded without the lock**, by one method call on a builtin `set`, which
CPython performs atomically. That is what keeps the lowering from adding a lock acquisition: the
worker branch of `_shutdown_worker` has no critical section after `worker.stop()`, and an
`int -= 1` outside the lock is a read-modify-write that can lose a decrement. In a forked child
both sets are emptied by `_clear_after_fork`, the handler that already empties `_closing_now`,
because the calls that added the tokens did not survive the fork.

**The lock count is pinned.** Measured at `fda134a` by replacing `_state._lock` with a wrapper that
counts `__enter__`, one call at a time: `configure(sink=…)` with a live worker takes it 2 times,
with no worker and something owed 1, and with nothing owed and no live worker 1; `shutdown()` on
the worker branch 2 and on the no-worker branch 3; the exit drain (`_shutdown_worker()`) on the
worker branch 2; `flush()` with a worker 1; a first orphan `info()` 1 and a second 0; a first
`@trace` call (the build) 1 and later ones 0. This spec changes none of those numbers.

**Where today's code and the plan's diagram part, recorded rather than fixed.** `CLOSED` is not
terminal: a `@trace` after a `shutdown()` that found no worker builds a live worker and the state
returns to `RUNNING`, and `configure()` and a swap run in `CLOSED`. `SWAPPING` is entered from
`CLOSED` as well as `RUNNING`, and is skipped by a `configure(sink=…)` that finds nothing owed and
no live worker. FR-004's table states each; SPEC-078 decides which of them stay.

**The hot path.** The `@trace` per-call check is `_lifecycle._get_worker()`'s outer
`_state.worker_exists()`, deliberately unlocked (`docs/decisions/pipeline/` → *A worker guard asks
one of three questions*), and it is unchanged. `state` is a fourth reader that takes no lock, for
the same reason the three questions take none: several guards ask with the lock held and the lock
is not reentrant.

#### Acceptance Criteria:

- [ ] `State`'s members are exactly the five names above, and `Runtime.state` returns the rule's
  `State` for fields a test sets directly, one case per numbered rule, including rules 3 and 4
  apart (a live worker built after a retirement is `RUNNING`; a retired worker is `CLOSED`).
- [ ] Precedence holds: a token in `_stops_running` reads `DRAINING` over a live worker and over a
  token in `_swaps_running`; a token in `_swaps_running` reads `SWAPPING` over `RUNNING` and over
  `CLOSED`. A `state` mutated to test the swap before the stop turns this case red.
- [ ] `_swaps_running` is empty after `_swap_sink` returns normally and after a `BaseException`
  (a `KeyboardInterrupt` injected into `Worker.retarget`, and one into a closer's join), and the
  early return (no live worker, nothing owed) never adds to it. Moving the discard out of its
  `finally` turns the injected case red.
- [ ] `_stops_running` is empty after `_shutdown_worker` returns on the worker branch and on the
  no-worker branch with a late worker, and after a `KeyboardInterrupt` injected into `Worker.stop`
  and one into `_close_owed`. Moving the discard out of its `finally` turns the injected case red.
- [ ] Two overlapping swaps, and two overlapping shutdowns, hold two tokens while both are inside
  and none after both return, and one is still held after the first returns.
- [ ] Each call above takes `_state._lock` exactly the number of times listed under *The lock count
  is pinned*, read through the same counting wrapper.
- [ ] `state` returns while another thread holds `_state._lock`, and nothing in `src/` outside
  `Runtime` reads `.state`.
- [ ] A child forked with a token in each set reads both empty and a state other than `SWAPPING`
  or `DRAINING`; removing the reset from `_clear_after_fork` turns this case red.
- [ ] `_get_worker`'s outer check is still a bare `_state.worker_exists()` before the `with`, which
  the predicate roster files.
- [ ] Invariants 1 and 3: `state` never raises for any combination the cases above set, and no
  public call's bound moves, since no call gains a lock acquisition.

### FR-003: The home of every lifecycle field and lock is declared and gated

#### Description:

"Nothing else holds lifecycle state" is only true until the next field is added. A new test,
`tests/test_lifecycle_inventory.py`, derives three populations by AST and requires each member to
appear in a declared table, and each table entry to match a member. A new lock or flag with no
entry fails by name; so does an entry whose site is gone, which is how a refactor that renames a
field shrinks a roster silently. The table is the Data Model's, in code.

- **Construction sites.** Every call of `threading.Lock`, `RLock`, `Event`, `Condition` or
  `Semaphore` in `_lifecycle.py`, `worker.py`, `_fork.py`, `config.py`, `decorator.py` and
  `console.py`, keyed by `(file, enclosing function or <module>, assignment target or <expr>,
  constructor)`. A site that binds no state — `_fork.py`'s type probes at `:25` and `:27` and its
  replacement factory at `:351` and `:355` — is declared with home `none` and that reason, so it
  is listed rather than skipped.
- **Fields.** Every attribute name assigned (plainly, augmented or annotated) on `self` in **any**
  method of `Runtime` or `Worker`, and on `_state` or `_lifecycle._state` anywhere in `src/`. A
  field first written outside `__init__` is in the population; `__init__` alone would miss one.
  Every `Worker` attribute is declared, its tunables (`batch_size`, `flush_interval`,
  `max_retries`, `_max_queue`) and `Health` counters included, each with its home and reason.
- **Module-level state in `_lifecycle.py`.** A module-level binding is a **constant**, and outside
  the population, when its name with leading underscores removed is upper case, no function names
  it in a `global` statement, and its value is a literal, a numeric expression of literals, or a
  `tuple` display, or a `tuple` or `frozenset` call over a tuple or set display of literals, names
  and `type(...)` calls — never a bare `set`, which is mutable. Every other module-level
  binding that is not an import, a function or a class is **state**. At `fda134a` that classifier
  puts the three `DEFAULT_*` values, `_FOREIGN`, `_MARKING_CEILING` and `_PLAIN_TYPES` among the
  constants, and `_state` and the seven names FR-001 moves among the state; after the move,
  `_state` alone.

Each table row also carries what a **forked child** does with the field: `walk` (a lock or event
the repair walk replaces), `reset: <handler>` (a registered child handler writes it), or
`inherited` (kept as the parent had it). That column is what turns "every flag a child must repair
is named" into a check.

The population floors are measured at `fda134a` and asserted as lower bounds: eleven construction
sites in `_lifecycle.py` and `worker.py` and eight outside them (the Overview's grep), none lowered
by the moves, since a moved lock is still one construction. Two existing derived rosters are
affected and are re-derived, not lowered. `tests/test_worker_predicate_roster.py` keys its sites by
`_Lifecycle.<method>`, so its keys become `Runtime.<method>`, its `_lifecycle.py` floor does not
fall, and the boolean positions in `Runtime.state` are filed with a category.
`tests/test_fork_lifecycle.py`'s rule that every lock is bound at a module global or a `self`
attribute already accepts a `self._x = threading.Lock()` in `Runtime.__init__`, so the three moved
locks stay inside its population, and its count of reachable constructions in `_lifecycle.py` is
unchanged.

#### Acceptance Criteria:

- [ ] The inventory test passes at the build's head with every population derived and every
  member declared, and asserts at least eleven construction sites in the pair and at least eight
  outside it.
- [ ] A fixture corpus asserts the failure reason for each check: an undeclared lock, an undeclared
  `Runtime` field, an undeclared `Worker` field first assigned outside `__init__`, an undeclared
  module-level state binding, a stale entry, a field declared with the wrong home, and a `reset`
  row whose handler does not write the field; it asserts silence for a declared field, for each of
  the constant shapes above, and for an import.
- [ ] Each of those checks is mutation-tested: deleting it turns its fixture red.
- [ ] `tests/test_worker_predicate_roster.py` passes with `Runtime.*` keys, with its `_lifecycle.py`
  floor not lowered and the sites in `Runtime.state` filed.
- [ ] Invariants 6 and 12: every row has a child value; every `walk` row is a lock or event
  construction; every `reset: <function>` row names a function registered with
  `_fork.register_child_handler`, a method one of them calls, or `_fork._reinit_after_fork`
  itself, whose AST assigns that field or calls a method on it; and the token sets' row reads
  `reset: _clear_after_fork`.

### FR-004: The model test drives every lifecycle call against the state table

#### Description:

`tests/test_invariants_model.py` today runs four threads of random calls and checks one identity at
the end; its docstring says a fork is not in the operation set. It keeps that concurrent test and
gains a **sequential driver**: one thread, seeded, drawing steps from `configure(sink=…)` (the swap,
when there is work for one), an orphan `info()`, a span, `flush()`, `shutdown()`, the exit drain
and a fork through `run_in_child`. The exit drain is `_lifecycle._shutdown_worker()` called with no
argument, which is what `atexit` calls; it is the same function and the same default budget as a
bare `shutdown()`, and is drawn as its own step only so the `atexit` binding is exercised.

**Every sink is a recorder, including a default one.** A step taken before any `configure(sink=…)`
resolves the default sink through `config._ensure_sink()`, which builds a `StdoutSink`. The driver
monkeypatches `log_foundry.sinks.stdout.StdoutSink` to a factory that builds a recorder and enters
it in the ledger, so a default-resolved sink is a recorder too and goes through the real
`_ensure_sink` path, `stamp` included. In a forked child the same factory is inherited, so a sink the
child resolves for itself is a recorder in the child's memory.

**The oracle.** Sequential is what lets one exist: with one caller the state after a step is
determined. The test carries its own **state table**, independent of `Runtime.state`, keyed by the
state and four booleans the test tracks from what its own steps did — the sink resolved, something
owed, a worker exists, that worker is live — and giving the state and booleans after each step and
whether the step passes through a transient state. "Owed" is set by an orphan `info()`, a worker's
build and a swap that does work, and cleared by a shutdown; it is what decides whether a
`configure(sink=…)` has work (`_swap_sink` returns early with no live worker and nothing owed,
`_lifecycle.py:1969`). The rows, which are today's behaviour and not a proposal:

| Step | `UNCONFIGURED` | `RUNNING` | `CLOSED` |
|---|---|---|---|
| `configure(sink=…)` | `RUNNING`; no swap work | `RUNNING`, through `SWAPPING` if owed or a live worker; directly if neither | `CLOSED`, through `SWAPPING` if owed; directly if not |
| orphan `info()` | `RUNNING`, owed | `RUNNING`, owed | `CLOSED`, owed |
| span | `RUNNING`, a live worker built, owed | `RUNNING`; a worker built if none exists | `RUNNING` if no worker exists, a live one built and owed; `CLOSED` if a retired one exists, the submission counted |
| `flush()` | unchanged, touching no sink | unchanged | unchanged |
| `shutdown()` or exit drain | `CLOSED`, through `DRAINING` | `CLOSED`, through `DRAINING`, nothing owed | `CLOSED`, through `DRAINING`, nothing owed |
| fork, in the child | `UNCONFIGURED` | `RUNNING` | `CLOSED` |

**After every step** the driver asserts: `state` equals the table's answer; both token sets are
empty; the pair (before, after) is a row of the table; and the invariant checks below.

**During a step** it observes the transient states without a second driver thread. The recorder
reads `_state.state` inside its own `close()` and `emit()`, and the driver brackets each step with
the step's name, set before the call and cleared after it returns. A sample is asserted only
inside a bracket, and only where the transient state has provably begun:

- **every `close()`** sampled inside a `configure(sink=…)` bracket reads `SWAPPING`, and inside a
  `shutdown()` or exit-drain bracket reads `DRAINING`. A close can begin only after the call's
  token is added, since both functions add it in their first critical section, before any
  release;
- **a drain-thread `emit()`** sampled inside a `shutdown()` or exit-drain bracket reads `DRAINING`
  when `_state._stop.is_set()` was already true at the sample. `_shutdown_worker` adds its token
  before it sets that event, in the same critical section, so a sample that sees the event set is
  after the token; an emit of an earlier span that starts before the event is set is not asserted.

**The fork step** runs `run_in_child` from a step boundary, so no token is held. The child reports
its `state`, then logs one orphan event, calls `flush()` and calls `shutdown()`. The parent asserts
that the child's initial state is the table's row for the parent's state, that all three calls
returned within a bound, that the child's event landed in a recorder in the child (the inherited
one, or one the child resolved through the factory from `UNCONFIGURED`), and that the child called
`close()` on no recorder it inherited; afterwards the parent's state and ledger are as before the
fork.

**Which invariants it checks, and which it cannot.** Checked after each step:

- **1** (never fails the caller): no step raises, `BaseException` included.
- **2** (delivered or counted, once): orphans balance exactly after every step (delivered plus
  `orphan_lost` equals calls); spans balance (`span.end` delivered plus `queued` plus `dropped`
  equals started, markers subtracted as the concurrent test does) after every `flush()` that
  returned truthy and after every `shutdown()`, since between those points a batch can be inside
  the drain.
- **3** (bounded, idle waits), the wall-clock half: each step is timed against its budget plus the
  slack the file already uses. **Not checked: the CPU half.** A CPU measure over a hundred steps
  under `-n 12` is the flake `tests/test_worker_exit_budget.py` measures one case at a time.
- **4** (no sink I/O on the caller's thread inside a span, and it on the caller's outside one): the
  recorder notes the thread that called `emit`, and every `span.end` came from the drain thread and
  every orphan event from the calling thread.
- **5** (closed once, by one owner, waited for): after each `shutdown()` every sink that received
  an event has `closes >= 1`, the owed record is empty, no two `close()` calls overlapped (the
  existing overlap counter), and none was called by a child on an inherited sink.
- **6** (twin paths): by construction, since one sequence drives both delivery paths and both
  `flush()` and `shutdown()`; it is a property of the run and no assertion.
- **12** (a forked child is repaired): the child half above. The parent half is observable only as
  the parent's ledger continuing to balance, which it asserts.

**Not checked, and why:** **7** needs a sink that raises, and the recorder never does, so no
total or partial failure is driven (`tests/test_worker.py` and `tests/test_sink_losses.py` own it);
**8** needs a hostile value and no step supplies one (`tests/test_sanitize_corpus.py`); **9** needs
baggage or an adopted context and no step sets one; **10** is the public surface and `Runtime` is
private; **11** needs a value that could leak into stderr and none is supplied; **13** needs a bad
argument and every step is valid.

The concurrent test keeps its identity and gains three checks: after the final `shutdown()` the
state is `CLOSED` and both token sets are empty; a sampler thread reads `state` throughout and asserts
only that it returns a `State`; and its docstring is rewritten, since it now says the fork is not in
the operation set. The sequential driver is the only one that asserts exact states, because under
concurrency the state after a step is not determined.

#### Acceptance Criteria:

- [ ] The sequential driver runs a fixed range of seeds and asserts, after every step, that `state`
  equals its table, both token sets are empty and the pair is a table row; a failing seed prints
  the step history so it replays from the seed alone.
- [ ] Over the seed range the histories include every cell of the table at least once — from
  `RUNNING` and from `CLOSED`, a `configure(sink=…)` that does swap work and one that does not,
  and a fork from each of the three states — and the driver asserts that coverage rather than
  leaving it to chance.
- [ ] Inside a bracket, a `close()` reads `SWAPPING` for a `configure(sink=…)` step and `DRAINING`
  for a `shutdown()` or exit-drain step, and a drain-thread `emit()` that saw `_state._stop` set
  reads `DRAINING`; no sample outside those conditions is asserted.
- [ ] The fork step asserts the child's initial state is the table's row for the parent's state, the
  child's three calls return within a bound, its event is in a child recorder, and its inherited
  recorders show `closes` unchanged.
- [ ] Each step records its invariant checks by number in its assertion message, and the file's
  existing check that every `invariant N` it cites exists on the page still passes.
- [ ] Invariants 1, 2, 3, 4, 5, 6 and 12 are checked as listed; the file's docstring names 7, 8,
  9, 10, 11 and 13 and the CPU half of 3 as not checked, with the reason.
- [ ] Each of these mutants, which a sequential run can reach, turns the driver red: a
  `_shutdown_worker` that does not discard its token on the no-worker branch, a `_swap_sink` that
  does not discard its token on a normal return, a `state` that tests rule 4 before rule 3 (the
  `CLOSED` span row that builds a live worker), and a `state` that ignores the configured sink (the
  `UNCONFIGURED` `configure` row). The three mutants a sequential run cannot reach — a discard
  outside its `finally`, swap tested before stop, no child reset — are FR-002's to kill.
- [ ] The fork step uses `run_in_child`, so the repo's every-fork-collects-first lint still passes.

### FR-005: Nothing a caller or a suite can see changes

#### Description:

The proof that this is a refactor is the existing suites, edited only where they pin a location.
The edits are the moves: tests that read a module-level name now read it off `_state`, and nothing
else about them changes. The files are found **on the build's base commit**, as SPEC-071 derives its
pins, with `grep -rlw -- <name> tests/` for each of the seven names and `grep -rlw _Lifecycle
tests/`; the earlier specs of the arc add and edit tests, so the list below, measured at `fda134a`,
is where the search starts and not its answer. At `fda134a` they are `tests/conftest.py` (its
reset fixture clears `_closers` and `_closing_now` and takes their locks, and also empties the two new token
sets, which is an addition), `test_sink_ownership.py`, `test_config.py`, `test_owed_closes.py`,
`test_lifecycle_races.py`, `test_shutdown_lifecycle.py`, `test_fork_lifecycle.py`,
`test_sink_release_roster.py`, `test_concurrent_owed_closes.py` and
`test_worker_predicate_roster.py` (whose keys name `_Lifecycle`). The same grep also finds
`test_public_surface.py`, whose only hit (line 921) is a docstring naming `_owned_lock` by its bare
name, which stays true, so that file is not edited.

**One assertion inverts rather than moves, and it is named so it is not mistaken for a weakened
test.** `test_lifecycle_races.py` asserts `"_owned" in _fork._skipped_names(_lifecycle)` with the
message "the module-level declaration stays" (line 1247). After FR-001 the declaration is on
`Runtime`, so that assertion becomes `"_owned" in _fork._skipped_names(_lifecycle._state)`, and a
second one asserts the module declares no `_FORK_SKIP`; the promise it guards — the walk never
reaches `_owned` — is unchanged and still asserted.

`test_sink_ownership.py` also sets `_state._orphan_closed_sink`, an attribute that no longer
exists; `Runtime` carries no `__slots__`, so that line keeps working and is not touched. No test
that asserts behaviour named in `docs/invariants.md` is deleted, and none is weakened.

#### Acceptance Criteria:

- [ ] The test names `pytest --collect-only -q` reports at the build's base commit are all present
  at the head, and the only additions are FR-001's, FR-002's, FR-003's, FR-004's and this FR's own
  (`tests/test_runtime_holder.py` holds FR-001's and FR-005's criteria).
- [ ] The diff of each named test file consists only of lines whose change is a moved name's path
  or the `Runtime` rename, with these named exceptions: the token-set reset in `conftest.py` (and
  its docstring paragraph); the inverted `_skipped_names` assertion in `test_lifecycle_races.py`
  (and the docstring sentence that said `_owned` is a module global); the `Runtime.state` rows in
  `test_worker_predicate_roster.py` (FR-003); FR-004's rewrite of `test_invariants_model.py`; the
  `_FORK_SKIP` assertion in `test_sink_ownership.py` (`_lifecycle._FORK_SKIP` becomes
  `_lifecycle._state._FORK_SKIP`, the declaration having moved to `Runtime`); and the rename at
  `test_owed_closes.py` (`_lifecycle._Lifecycle._FORK_SKIP`). The search on the build's base
  commit also finds `test_invariants_model.py`, whose docstring named `_lifecycle._owned`, and
  `test_sinks_broker_engine.py`, whose hit is an unrelated local helper named `_owned` and is not
  edited.
- [ ] `log_foundry.__all__` and `tests/test_public_surface.py` are unchanged and pass;
  `from log_foundry import Runtime` and `State` both raise `ImportError`.
- [ ] `poetry run pytest`, `ruff`, `mypy`, `docstring-lint.py`, `spec-lint.sh` and `docs-lint.sh`
  pass, and every moved docstring still opens on a one-sentence summary and carries the trio.
- [ ] Invariants 1, 3 and 10: no public call gains a name or a lock acquisition (FR-002's pinned
  counts), and the `@trace` path takes no lock it did not take at `fda134a`.

---

## Data Model

```
// src/log_foundry/_lifecycle.py

class State(enum.Enum):             # values are enum.auto(): nothing reads them, and a literal
                                    # "draining" is filed by the predicate roster as a worker question
    UNCONFIGURED = enum.auto()      # nothing resolved, owed, built or retired
    RUNNING      = enum.auto()      # a live worker, or a sink resolved or owed, not retired
    SWAPPING     = enum.auto()      # _swap_sink inside its work (_swaps_running non-empty)
    DRAINING     = enum.auto()      # _shutdown_worker inside its body (_stops_running non-empty)
    CLOSED       = enum.auto()      # retirements > 0 and no live worker

class Runtime:                      # was _Lifecycle; _state: Runtime stays the one instance
    # held today
    _lock: threading.Lock                  # the state lock, held across Worker(_ensure_sink())
    _worker: Worker | None
    _late_worker: Worker | None
    _owed: dict[int, Sink]                 # the one owed-close record
    _stop: threading.Event                 # the one stop signal; replaced, never cleared
    retirements: int                       # the retirement count
    _shutdown_running: int                 # no-worker shutdown depth; gates the late worker
    _atexit_registered: bool
    # moved in from module globals of _lifecycle.py
    _closers: list[threading.Thread]       # was line 356
    _closers_lock: threading.Lock          # was line 357
    _closing_now: dict[int, _Closing]      # was line 393
    _closing_now_lock: threading.Lock      # was line 430
    _marking_failed: bool                  # was line 598
    _owned: dict[int, tuple[int, object]]  # was line 606
    _owned_lock: threading.Lock            # was line 624
    # new, observer-only: one object() token per call inside; added under _lock in the
    # call's first critical section, discarded lock-free in its finally
    _swaps_running: set[object]
    _stops_running: set[object]
    _FORK_SKIP = ("_owed", "_owned")
    state: State                           # property; derived, takes no lock, never raises
```

Line numbers are `_lifecycle.py` at `fda134a`. `_Closing` (line 359) stays where it is.

**The inventory, field by field, at `fda134a`.** "Stays" is a decision, with its reason.

The last column is FR-003's child value: `walk` (replaced by `_fork._reinit_primitives`),
`reset: <function>` (written by that child handler, or by a method it calls), or `inherited`.

| Field | File and line | Guard | After this spec | In a child |
|---|---|---|---|---|
| `_lock` | `_lifecycle.py:157` | itself | `Runtime` | walk |
| `_worker`, `_late_worker`, `_owed`, `retirements`, `_atexit_registered` | `_lifecycle.py:156-163` | `_lock` (reads of `_worker` and `retirements` are unlocked, single loads) | `Runtime`, unchanged | inherited |
| `_shutdown_running` | `_lifecycle.py:162` | `_lock` | `Runtime`, unchanged | inherited — the known defect under Out of Scope |
| `_stop` | `_lifecycle.py:160`, replaced at `:272` | `_lock` | `Runtime`, unchanged | walk; in a resumed child then reset: `_rebuild_worker_after_fork`, whose `Worker._reinit_after_fork` calls `refresh_stop_signal` (`worker.py:581` at `93113e1b`), replacing it if set; a retired child keeps the walked event |
| `_closers` | `_lifecycle.py:356` | `_closers_lock` | moves to `Runtime` | inherited (its threads read dead and are pruned) |
| `_closers_lock`, `_closing_now_lock`, `_owned_lock` | `_lifecycle.py:357`, `:430`, `:624` | themselves | move to `Runtime` | walk |
| `_closing_now` | `_lifecycle.py:393` | `_closing_now_lock` | moves to `Runtime` | reset: `_clear_after_fork` |
| `_owned`, `_marking_failed` | `_lifecycle.py:606`, `:598` | `_owned_lock`; `_marking_failed` none, written by the child's one thread | move to `Runtime` | reset: `_mark_inherited` |
| `_swaps_running`, `_stops_running` | new | none: added under `_lock`, discarded atomically | `Runtime` | reset: `_clear_after_fork` |
| `_Closing.event` | `_lifecycle.py:390` | the registry lock | stays, a per-close event owned by the entry | walk |
| `Worker._stopped` | `worker.py:415` | `Worker._lock` | stays: the once-only stop gate **of one worker**, and there can be two (`_worker` and `_late_worker`), so a field on `Runtime` would need keying by worker; SPEC-068 changed how it resets, in 1.1.0 | reset: `_rebuild_worker_after_fork`, to `False` when the child resumes (`worker.py:578` at `93113e1b`, SPEC-068); inherited by a retired child |
| `Worker._exit_deadline`, `_in_flight` | `worker.py:416-417` | `_exit_deadline` under `Worker._lock` by `claim_stop`; `_in_flight` by the drain thread | stay: the drain's own bookkeeping (SPEC-062) | reset: `_rebuild_worker_after_fork` — `_in_flight` in every child, `_exit_deadline` only when the child resumes, so a retired child inherits it |
| `Worker._join_owed`, `_join_taken_over` | `worker.py:431-432` at `93113e1b` | `Worker._lock` by `claim_stop` | stay: the join a stop claimed on the drain thread owes, and whether a later caller took it over (SPEC-068 FR-006); per worker, like `_stopped` | reset: `_rebuild_worker_after_fork`, to `False` when the child resumes (`worker.py:579-580`); inherited by a retired child |
| `Worker._drain_finished`, `_drain_settled` | `worker.py:413-414` | events | stay: the drain loop's own events, ordered against each other (`_run`'s docstring), read by `draining` and `flush` | walk, then reset: `_rebuild_worker_after_fork` |
| `Worker._held`, `_taken_markers` | `worker.py:419`, `:418` | `Worker._lock` | stay: what the worker's own thread may still be inside, and the `flush()` callers it owes; both in `Worker._FORK_SKIP` | reset: `_rebuild_worker_after_fork` |
| `Worker._epoch` | `worker.py:412` | none | stays: the retirement count copied at that worker's build, which makes `live_worker` a comparison | inherited |
| `Worker._stop` | `worker.py:411` | none | stays: a reference to `Runtime._stop` taken at the build, or a fresher event for a late worker; not a second signal | walk; in a resumed child then reset: `_rebuild_worker_after_fork`, to `refresh_stop_signal()`'s event (`worker.py:581` at `93113e1b`); a retired child keeps the walked event |
| `Worker._lock` | `worker.py:420` | itself | stays: guards the worker's counters, markers and `_stopped` | walk |
| `Worker.sink` | `worker.py:400` | `Worker._lock` in `retarget` | stays; no `Runtime.sink` is added (Out of Scope) | inherited |
| `Worker.batch_size`, `flush_interval`, `max_retries`, `_max_queue` | `worker.py:401-403`, `:409` | none, set once | stay: tunables | inherited |
| `Worker._thread`, `_queue` | `worker.py:422`, `:410` | `queue.Queue`'s own mutex | stay: the drain thread and its buffer | reset: `_rebuild_worker_after_fork` |
| `Worker` counters (`dropped`, `failed_batches`, `stopped_reason`, `submitted_after_shutdown`, `incomplete_swaps`) | `worker.py:404-408` | `Worker._lock` | stay: `Health` figures, not lifecycle state | reset: `_rebuild_worker_after_fork` |
| `_FlushMarker.event` | `worker.py:307` | none | stays: one per `flush()` call | none: it lives in the queue the child replaces |
| `_config_lock`, `Console._lock`, `_sweep_lock`, `_loss_lock` | `config.py:54`, `console.py:63`, `decorator.py:31`, `:38` | themselves | stay: no lifecycle state (`_loss_lock` is deliberately not the lifecycle lock) | walk |
| `_fork._RLOCK_TYPE`, `_LOCK_TYPES`, `_fresh_primitive`'s two constructions | `_fork.py:25`, `:27`, `:351`, `:355` | none | stay: type probes and the repair's replacement factory; home `none` | none |
| `_fork._child_handlers`, `_installed` | `_fork.py:31`, `:70` | none | stay: the fork repair's own state, SPEC-079's | inherited |
| `_fork.reacquired_in_child` | `_fork.py:54` | none | stays, SPEC-079's | reset: `_fork._reinit_after_fork` |

## API / Interface Contract

No public signature changes. The private contract after this spec:

```
_lifecycle._state: Runtime                  # unchanged name; module global
_lifecycle._state.state -> State            # new, lock-free observer; read by tests only
_lifecycle._state._swaps_running: set[object]  # new; token added under _lock, discarded in finally
_lifecycle._state._stops_running: set[object]  # new; token added under _lock, discarded in finally

# the seven names move: _lifecycle._owned  ->  _lifecycle._state._owned   (and likewise the rest)
```

A test that needs an arbitrary `state` sets the fields directly, as `tests/conftest.py`'s
`install_worker` does, and reads the property.

## File & Folder Structure

```
src/log_foundry/_lifecycle.py                 # Runtime, State; the seven names moved
src/log_foundry/worker.py                     # docstrings only (names of the moved fields)
src/log_foundry/_fork.py                      # docstrings only
tests/test_invariants_model.py                # sequential driver, state table, fork step
tests/test_lifecycle_inventory.py             # FR-003's derived inventory and its corpus
tests/conftest.py and the nine files in FR-005
docs/decisions/pipeline/                      # one new decision; one History line
```

## Implementation Phases

### Phase 1: The holder (FR-001, FR-005)

- Rename `_Lifecycle` to `Runtime`; move the seven names onto it with their docstrings; replace
  `_FORK_SKIP`; update the module functions' reads.
- Update the named tests' paths and the predicate roster's keys; diff `--collect-only` names.
- Update the `src/` docstrings that name a moved name.

### Phase 2: The observer (FR-002)

- Add `State`, `Runtime.state`, the two token sets with their `finally` discards and the child
  reset.
- Add the derivation, precedence, token-exit, lock-count and fork-reset tests, each with its
  mutant; empty the sets in `conftest.py`.

### Phase 3: The inventory and the model test (FR-003, FR-004)

- Write the derived inventory test with its corpus and mutation run.
- Add the sequential driver, the table, the transient-state sampling and the fork step; rewrite the
  file's docstring; mutation-test the four guards named in FR-004.
- Record the decision (below) and regenerate the indexes.

## Documentation

The build records one decision in `docs/decisions/pipeline/`: lifecycle state has one holder,
`Runtime`, and `State` is an observer read off the flags until a later spec makes the transitions
checked, with the four remaining locks and why they are not one. It adds a dated History line to
`docs/decisions/pipeline/a-worker-guard-asks-one-of-three-questions-and-the-set-is-enforced-rather-than-remembered.md`
saying `state` is a fourth lock-free reader and the guards' home is now `Runtime`; no decision is
reversed. `docs/component-inventory.md`'s lifecycle-owner row and `architecture.md` §9.2 are
updated for the rename and for the three loose names that moved.

## Revision history

- Drafted from the stability plan's Change 1 and its step 8. The plan's eleven lock and event
  sites reproduce at `fda134a`; its list of fields was short by `_atexit_registered`,
  `Worker._stop`, `_exit_deadline`, `_in_flight`, `_thread` and `_queue`.
- The plan's "one lock" is not reached here and the plan's `Runtime.sink` is not added; FR-001 and
  the Out of Scope list give the reasons, read from the code's lock order and its docstrings.
- Revised after the spec review, in place: the state table gained the "owed" fact, without which
  it could not say whether a `configure(sink=…)` swaps (a probe after `shutdown()` showed one that
  did no work and one that did); the depth counters became token sets discarded without the lock,
  so the lowering adds no acquisition, and the lock counts are pinned by measurement; the inventory
  population was corrected to eight sites outside the pair and every `Worker` field, the
  module-state classifier and a child column were defined; mutants a sequential run cannot reach
  moved to FR-002; the transient-state samples were given a defined start; default-resolved sinks
  became recorders; the inverted `_skipped_names` assertion and the `test_public_surface.py` hit
  were named; the docstring grep was widened; and `_shutdown_running`'s inheritance by a child is
  recorded as a measured defect rather than pinned.
- Build-order re-read: FR-001's lock-site literal and FR-005's file list are derived on the build's
  base commit, as SPEC-071's pins are, since SPEC-072 rewrites `Worker._emit` and adds
  `_retry_declined` before this spec builds.
- 2026-11-05: rebased onto main after v1.1.0; the landed peer specs are now in depends_on.
- 2026-11-05, drift review: the inventory adds `Worker._join_owed` and `_join_taken_over` (SPEC-068 FR-006), and its child column now has `Worker._stopped`, `Worker._stop` and `Runtime._stop` reset on resume through `refresh_stop_signal`, `_exit_deadline` reset only on resume, and each retired child's case.
- 2026-11-09, build (base `3d65cf32`): `State`'s values are `enum.auto()` rather than the Data
  Model's strings, because `tests/test_worker_predicate_roster.py`'s sentinel vocabulary contains
  `draining` and filed the literal as a worker question; nothing reads the values. FR-005's
  criteria are amended to name every addition and every edit beyond a path: FR-001's and FR-005's
  own tests in `tests/test_runtime_holder.py`, the `Runtime.state` roster rows, FR-004's rewrite of
  the model test, the `_FORK_SKIP` assertion in `test_sink_ownership.py` and the rename in
  `test_owed_closes.py`. FR-003 as built: the inventory keys by name, so the Data Model's line
  numbers (measured at `fda134a`) are not load-bearing and are left as they were; the AST finds 9
  constructions outside the pair where the line grep finds 8 (`_fork.py` builds two on one line),
  and the floor asserts 9; the child column also takes `none` and a sequence (`walk`, then
  `reset`); a `walk` row may hold a lock or event one hop away (`Worker._stop` is handed
  `Runtime.refresh_stop_signal()`'s return); `reset` handlers reach their writers through a
  closure resolved by receiver, not by bare name; and `_fork._child_handlers`, `_installed` and
  `reacquired_in_child` stay prose here and out of the code table, since they belong to no
  FR-003 population.
- 2026-11-09, first diff review: the constant classifier no longer accepts a `set` display or call
  (`_PENDING = set()` and `_SEEN = {1}` are mutable, so state); `frozenset` and tuple are kept.
  Both module-scope scans — FR-003's module-state population and FR-001's "nothing bound at module
  level" check — share one walker that enters every compound statement at module scope (`if`,
  `try` with its handlers, `else` and `finally`, `try`/`except*`, `with`, `for`, `while`, `match`)
  and no function or class body, since reading top-level statements alone let a module-level
  `try:` re-bind `_closing_now`. The predicate roster's `_lifecycle` floor is raised 44 to 47 for
  `Runtime.state`'s three sites. FR-004's transient column is asserted against the samples where
  a step samples anything, and five cells that never sample (a shutdown or the exit drain from
  `UNCONFIGURED`, and a `configure(sink=…)` with no swap work) have their column held by a check
  of the table against FR-002's rules instead.
