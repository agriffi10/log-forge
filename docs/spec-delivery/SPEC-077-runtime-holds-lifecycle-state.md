# Completed Spec — SPEC-077: Runtime Holds Today's Lifecycle State

## What was completed?

- `_lifecycle._Lifecycle` is now `Runtime`, and `_state` is still its one instance. Seven
  module-level names moved onto it as attributes, with no alias left at module scope: `_closers`,
  `_closers_lock`, `_closing_now`, `_closing_now_lock`, `_owned`, `_owned_lock` and
  `_marking_failed`. The module functions that use them read `_state._<name>`, and no body changed
  except the path. The module's `_FORK_SKIP` is gone, and `Runtime._FORK_SKIP` is now
  `("_owed", "_owned")`.
- Four locks remain, each guarding what it guarded before, in the same order (`Runtime`'s class
  docstring says why they are not one). No call gained a `_state._lock` acquisition.
- `State` (`UNCONFIGURED`, `RUNNING`, `SWAPPING`, `DRAINING`, `CLOSED`) and the property
  `Runtime.state`. It takes no lock, applies six rules in order, and nothing in `src/` reads it.
  Two token sets, `_swaps_running` and `_stops_running`, make the swap and the stop visible to it.
  `_swap_sink` and `_shutdown_worker` each add a fresh `object()` under the lock in their first
  critical section and discard it lock-free in a `finally`. `_clear_after_fork` empties both in a
  child.
- `tests/test_lifecycle_inventory.py` derives three populations by AST and requires a row for each
  member: lock and event constructions in six modules, every field of `Runtime` and `Worker`, and
  module-level state in `_lifecycle.py`. Each row carries a child value, which is `walk`,
  `reset: <handler>`, `inherited` or `none`, and the test checks that value against the fork
  handlers. A fixture corpus asserts every check's failure reason, and the cases that must stay
  silent. `module_scope` is its walker over module scope, and `tests/test_runtime_holder.py` reuses
  it.
- `tests/test_invariants_model.py` gains a sequential, seeded driver over seven steps: `configure`,
  an orphan `info()`, a span, `flush()`, `shutdown()`, the exit drain and a fork. After every step
  it checks a 25-cell state table, the empty token sets and invariants 1 to 5 and 12; invariant 6
  holds by construction, since one sequence drives both delivery paths. It samples the
  transient state from inside each recorder's `close()` and `emit()`, and a pure test asserts that
  the seed range visits every cell.

## Deviations (each recorded in the spec's revision history)

- `State`'s values are `enum.auto()`, because the predicate roster filed a literal `"draining"` as
  a worker question.
- The inventory is keyed by name, not by line. It finds 9 constructions outside the pair where the
  spec's line grep found 8, because `_fork.py` builds two on one line. A child value may also be
  `none` or a sequence. A `walk` row may hold a primitive one call away.
- `_fork._child_handlers`, `_installed` and `reacquired_in_child` stay prose in the spec's Data
  Model. They belong to none of the three populations.
- The constant classifier refuses a `set` display or call, which is mutable state. Both scans of
  module scope enter every compound statement, not only the top level.
- Five table cells never sample anything, such as a shutdown from `UNCONFIGURED`. For those cells,
  `test_the_table_transients_follow_the_spec_rules` holds the transient column instead.
- FR-005's criteria now name every test edit beyond a path.

## What changed from earlier specs?

- No public surface changed. `from log_foundry import Runtime` and `State` both raise
  `ImportError`.
- A test that reached `_lifecycle._owned` or one of the other six moved names now reads it off
  `_lifecycle._state`. One assertion was inverted rather than moved: `_owned` is now in
  `_fork._skipped_names(_lifecycle._state)`, and a second assertion checks the module declares no
  `_FORK_SKIP`. No test was deleted and no floor was lowered.
- `docs/decisions/pipeline/`: one new decision, *Lifecycle state has one holder, `Runtime`*. *A
  worker guard asks one of three questions* gained a dated entry. `architecture.md` §9.2, §12 and §13
  and the component inventory were updated.

## Verification and the guards a later spec reddens

- The claims above are pinned by `tests/test_runtime_holder.py` (FR-001, FR-005),
  `tests/test_runtime_state.py` (FR-002), `tests/test_lifecycle_inventory.py` (FR-003) and
  `tests/test_invariants_model.py` (FR-004). The PR's CI run is the record of the full suite.
- **Expect these to redden when SPEC-078 or SPEC-079 changes the runtime. Re-derive them; do not
  delete them:**
  - the inventory's `INVENTORY` table, and `PAIR_FLOOR` (11) and `OUTSIDE_FLOOR` (9), which are
    lower bounds;
  - `LOCK_SITES_AT_BASE` in `tests/test_runtime_holder.py`, a multiset of (function, lock) pairs
    derived at `3d65cf32`;
  - the predicate roster's `_lifecycle` floor, 47, which counts `Runtime.state`'s three sites;
  - `LOCK_COUNTS` in `tests/test_runtime_state.py`, 13 pinned `_state._lock` counts. These are the
    spec's numbers plus a configure on a retired worker and a no-worker shutdown with nothing
    owed;
  - the sequential driver's `TABLE`, `UNSAMPLED` and `_Model`. SPEC-078 decides which transitions
    stay.

## Open and accepted edges

- **Known defect, left for SPEC-079:** a forked child inherits `_shutdown_running` as the parent had
  it. With the count at 1 in the parent, the child's first `@trace` registers its new worker as
  `_late_worker`. This spec neither fixes nor pins it. SPEC-079's draft resets it, together with
  `_late_worker`.
- Accepted: the token sets are discarded lock-free, which departs from `python.md` §13. A lost
  discard could only misreport an observer that nothing in `src/` reads (see the decision).
