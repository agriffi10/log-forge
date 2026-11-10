# Completed Spec — SPEC-079: Fork — One Walk From the Runtime's Roots, the Global Lock Roster, and sink_factory=

## What was completed?

- **A roster of the package's global primitives** (FR-001). `_fork._GLOBAL_PRIMITIVES` lists the
  eight locks and events held as module state, replaced first in a child through one memo.
  `tests/test_fork_roster.py` holds it complete from a live sweep and an AST scan, each with a
  fixture corpus; each roster lock held across a fork has a control that blocks without its entry.
- **One walk from the runtime's roots** (FR-002, FR-003). `_lifecycle._walk_after_fork`, registered
  with `_fork.register_walk`: a repair frontier (`_reachable_sinks` with a shared `_Repair`) from the
  configured sink, the worker and the late worker, then a marking frontier, first in first out
  under `_MARKING_CEILING`, from the forwarded seeds (hooked) and then the records (never hooked),
  then the `_FOREIGN` `setdefault`. `_reclaim_after_fork` is the first handler.
  - Deleted: `_reinit_primitives`, `_container_children`, `_is_traversable`, `_fork._FORK_SKIP`,
    the counters reach, `_mark_inherited` and `_inheritance_roots`.
  - Every read the walk and `stamp` make is static.
  - A registry of live `SinkCounters` replaces the counters reach.
  - `Worker._FORK_SKIP` gains `_queue` and `_thread`.
- **The child's state** (FR-004). `_clear_after_fork` resets `_shutdown_running` and
  `_late_worker`, arms a live worker's unowed sink and re-settles the stored state under
  `_state._lock`: a fifth `_move` site.
- **`sink_factory=`** (FR-005, FR-006). `configure()` and `Config` gain it, appended. The private
  `_FactorySink` builds each process's sink on its first emit; `_adopt_factory_after_fork` gives a
  child its own wrapper. `_sink_arg.require_sink` refuses an unpatchable `sink=` and inner sink;
  `config._require_factory` refuses a bad factory. Both join the type-door roster.
- **Deviations, each amended in the spec** (A1 to A12): the conftest reset clears the ownership
  record, which was the suite's fork-walk-cap flake; the static predicates; the registry; FIFO
  marking; `_FORK_SKIP` names not forwarded; the re-settle; `_Close.event` as `none`; one
  public-surface pin re-expressed; `_build_lock`.

## What changed from earlier specs?

- **Pins changed on purpose:** the inventory's `roster` value and rows, the lock-site literal, the
  `_move` scan's fifth site, the owed-site table, the predicate roster (floor kept at 54), the
  release, post-close and wrapper-owner rosters, the door set (thirteen), the import guard on
  `_lifecycle` (narrowed to `_diag` and `_fork`), and the docs-lint case floor (308).
- **A caller sees:** `sink_factory=`; the refusal of an unpatchable `sink=` and inner sink; a
  library sink behind a third-party wrapper repaired and re-acquired in a child; an engine sink's
  counters repaired however held; `configure()` and the fork repair running no hook of a sink.
  A proxy-shaped `sink=` (`weakref.proxy`, `super(...)`, a `__getattr__` proxy) is recorded by
  `stamp` and by the child's walk unconditionally, so a child never closes the parent's.
- **Decisions:** the fork-order decision rewritten; the lifecycle-state fence names the fifth
  `_move` site; History lines on the marking and the stranding decisions; one new sink-lifecycle
  decision for `sink_factory=` and the `sink=` refusal.

## Verification

All of these exited 0 at the head: ruff, mypy, pytest, `spec-lint.sh`, `docs-lint.sh`,
`docstring-lint.py`, `spec-lint-test.sh` and `docs-lint-test.sh`. The deterministic reproducer of
the flake fails at the base and passes at the head. Every base test name survives but one id whose
embedded line number moved. The mutation set is all killed, each for its stated reason.
