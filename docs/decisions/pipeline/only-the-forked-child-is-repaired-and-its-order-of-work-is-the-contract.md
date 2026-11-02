---
type: Decision
title: "Only the forked *child* is repaired, and its order of work is the contract"
description: "locks and events, then inherited buffers, then the registered handlers — a lock re-initialised after a handler that takes it is a handler that hangs. `before` does not run for a C-level fork at all, and the repair is never wrapped in `gc.disable()` — that hides a macOS `os_log` crash rather than fixing one. (SPEC-039; arch §13)"
---

**Only the forked *child* is repaired, and its order of work is the contract** — locks and events, then inherited buffers, then the registered handlers, because a lock re-initialised after a handler that takes it is a handler that hangs on the child's only thread. `before` does not run for a C-level fork at all (uWSGI calls `PyOS_AfterFork_Child` only), so the child handler must be sufficient regardless. The repaired roster is **derived, never listed** — a walk over this package's own objects, with an AST lint forbidding a primitive built where the walk cannot write it back, and an identity memo so a sink's `log_foundry_stop_signal` stays the worker's `_stop`. The worker is rebuilt **in place** with a replaced queue *object*; a retired parent forks a retired child. What the walk cannot reach is the caller's and is recorded, not half-fixed — including the shared sink, which both processes also *close*. **The repair is never wrapped in `gc.disable()`**, and that was built rather than assumed: it hides a macOS crash instead of fixing one — a forked child that finalizes an inherited, unclosed `sqlite3.Connection` faults inside Apple's `os_log`, and suppressing collection during the repair only greens a child that exits immediately, while re-enabling the collector in a child whose parent had disabled it. arch §13 carries the mechanism. (SPEC-039, arch §9, §13)

## History

### 2026-08-10

- **Settled.**
