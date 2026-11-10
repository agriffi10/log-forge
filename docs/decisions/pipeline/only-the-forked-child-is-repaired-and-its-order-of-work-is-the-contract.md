---
type: Decision
title: "Only the forked *child* is repaired, and its order of work is the contract"
description: "the global-lock roster, then one walk from the runtime's roots, then inherited buffers, then the registered handlers — a lock re-initialised after a handler that takes it is a handler that hangs. `before` does not run for a C-level fork at all, and the repair is never wrapped in `gc.disable()`. (SPEC-039, SPEC-079; arch §9, §13)"
---

**Only the forked *child* is repaired, and its order of work is the contract** — the package's global locks and events from one roster, then one walk from the runtime's roots, then inherited buffers, then the registered handlers, because a lock re-initialised after a handler that takes it is a handler that hangs on the child's only thread. `before` does not run for a C-level fork at all (uWSGI calls `PyOS_AfterFork_Child` only), so the child handler must be sufficient regardless.

**The package's global primitives are listed, in one roster held complete by a derived test; everything else is reached by one walk from the runtime's roots** (SPEC-079). `_fork._GLOBAL_PRIMITIVES` names the eight locks and events the package holds as module state; `tests/test_fork_roster.py` holds it complete from both sides, a live sweep of every module global and an AST scan of every class that assigns a primitive to `self`, so a global lock with no entry fails by name. The walk, registered by `_lifecycle` as `_walk_after_fork`, starts from the configured sink, the worker and the late worker: its repair frontier replaces the locks and events of every package holder it reaches and collects the re-acquisition hooks, and its marking frontier follows what the repair frontier forwarded and then the records, within a visit ceiling. One identity memo is shared by the roster and the walk, so a sink's `log_foundry_stop_signal` stays the worker's `_stop` and the roster's `_state._stop`. An AST lint still forbids a primitive built where the repair cannot write it back. The roster replaced a derived walk over every package module, which was unbounded, read every container with `list(container)` and ran hostile sink code; the walk's reads are static.

The worker is rebuilt **in place** with a replaced queue *object*; a retired parent forks a retired child. What the walk cannot reach is the caller's and is recorded, not half-fixed — including the shared sink, which both processes also *close*. **The repair is never wrapped in `gc.disable()`**, and that was built rather than assumed: it hides a macOS crash instead of fixing one — a forked child that finalizes an inherited, unclosed `sqlite3.Connection` faults inside Apple's `os_log`, and suppressing collection during the repair only greens a child that exits immediately, while re-enabling the collector in a child whose parent had disabled it. arch §13 carries the mechanism. (SPEC-039, SPEC-079, arch §9, §13)

## History

### 2026-11-10

- **Refined by SPEC-079.** "The repaired roster is derived, never listed — a walk over this package's own objects" became the global primitives **listed** in one roster held complete by a derived test, and everything else reached by one walk from the runtime's roots. The order of work is kept, with the roster as its first step and the walk as its second.

### 2026-08-10

- **Settled.**
