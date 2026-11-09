---
type: Decision
title: "Lifecycle state has one holder, Runtime, and State observes it until transitions are checked"
description: "the lifecycle bookkeeping lives on `_lifecycle._state`, a `Runtime`, with no module-level alias; `State` is derived lock-free from today's flags and nothing in `src/` reads it until a later spec makes transitions checked. Four locks remain, not one, because merging any changes the lock order. The swap and stop token sets are added under `_lock` and discarded lock-free, a deliberate, bounded exception to `python.md` §13. (SPEC-077; arch §9.2)"
---

**One holder.** SPEC-040 put the orphan side's seven globals on one object, `_lifecycle._state`,
and SPEC-054 merged the twin bookkeeping (*One lifecycle owner*). Seven more names still sat at
module scope in `_lifecycle.py`, each with its own lock where it had one. They were the closer
roster, the in-flight-close registry, the ownership record and its marking flag, and their three
locks. SPEC-077 renamed `_Lifecycle` to `Runtime` and moved all seven onto it. No alias was left
at module scope, because a second home is the defect the move removes. `_state` stays the module
global and the one instance, so the `@trace` hot path and `worker.py`'s binding are untouched. The
fork opt-out moved with `_owned`. `_fork._skipped_names` reads `_FORK_SKIP` from whatever holds
the attribute, so `Runtime._FORK_SKIP` is now `("_owed", "_owned")` and the module declares none.
`tests/test_lifecycle_inventory.py` makes "nothing else holds lifecycle state" a gate rather than a
sentence. It derives every lock or event construction, every `Runtime` and `Worker` field and every
module-level state binding, and fails on one that has no declared home.

**`State` is an observer.** `Runtime.state` names one of five states (`UNCONFIGURED`, `RUNNING`,
`SWAPPING`, `DRAINING`, `CLOSED`) from fields the runtime already keeps, using six rules in a fixed
order. Nothing in `src/` reads it, so a wrong derivation can fail a test and cannot change a
delivery. It records where today's code departs from the stability plan's diagram rather than
fixing it. `CLOSED` is not terminal, because a `@trace` after a no-worker `shutdown()` builds a live
worker. `SWAPPING` is entered from `CLOSED` as well as from `RUNNING`. `state` takes no lock, which
makes it a fourth lock-free reader beside the three worker questions, for their reason: several
guards ask with `_lock` held, and that lock is not reentrant.

**Four locks, not one.** `_lock` guards the worker, the owed record, the retirement count and the
stop event. `_closers_lock`, `_closing_now_lock` and `_owned_lock` each guard the one structure they
are named for. Folding `_closing_now_lock` into `_lock` deadlocks `in_flight` and `held`, which are
asked with `_lock` held and take that leaf by design. `_closers_lock` and `_owned_lock` are taken
beneath `_lock` and also on closer threads beside it. `_lock` is also held across a blocking
`Worker(_ensure_sink())` build, so merging either of them into it changes the lock order, and the
lock order is behaviour. The order stays as `Runtime`'s docstring states it: `_lock`, then
`_config_lock`, then `_owned_lock`; and `_lock` with `Worker._lock`, `_closers_lock` and
`_closing_now_lock` as leaves beneath it. Nothing acquires `_lock` beneath any of them.

**The token sets are discarded without the lock, and that departs from `python.md` §13 on
purpose.** `_swap_sink` and `_shutdown_worker` each add a fresh `object()` to `_swaps_running` or
`_stops_running` inside a critical section they already have. Each discards it in a `finally` with
one `set.discard`. A token's hash runs no Python code, so under the GIL the discard is one
operation. They are sets, not booleans, because two concurrent `configure(sink=…)` or `shutdown()`
calls are documented as normal, and the first to finish would clear a boolean while the second
still runs. They are sets, not `int` counters, because the worker branch of `_shutdown_worker` has
no critical section after `worker.stop()`, so a counter's decrement would need a new lock
acquisition or would be a read-modify-write that can lose an update. The `_state._lock` counts per
call are pinned in `tests/test_runtime_state.py`, measured at `fda134a` and again at `3d65cf32`
before the sets existed, and no call gained an acquisition. §13 says not to rely on a builtin's
atomicity across threads. The consequence of breaking that rule here is bounded: a lost discard
would misreport an observer that nothing in `src/` reads. A forked child empties both sets in
`_clear_after_fork`, because the calls that added the tokens did not survive the fork.

**Rejected:**

- **One lock**, the stability plan's target. It deadlocks or reorders, for the reasons above.
- **A `Runtime.sink` field.** The live sink is held twice already, by `Worker.sink` and by the
  config, and the two differ on purpose after a declined swap. A third copy would turn that
  deliberate divergence into a three-way one.
- **Moving `Worker`'s drain state onto `Runtime`.** `_stopped`, `_join_owed` and the drain events
  belong to one worker, and there can be two (`_worker` and `_late_worker`), so a field on
  `Runtime` would need keying by worker.
- **Module-level aliases for the moved names**, kept for compatibility. An alias would be a second
  home, which is the defect the move removes.

**Do NOT** let code in `src/` branch on `Runtime.state` or on the token sets while the sets are
discarded lock-free. The §13 exception holds only while the state is an observer. A later spec
that makes `State` decide must also put the sets' writes under a lock, or replace them. Do NOT add
lifecycle state at module scope in `_lifecycle.py`, or a field to `Runtime` or `Worker`, without a
row in the inventory test. Do NOT merge the four locks without re-deriving the lock order. (SPEC-077
FR-001 to FR-003)

## History

### 2026-11-09

- **Settled** by SPEC-077: one holder, `Runtime`; `State` as a lock-free observer; four locks kept;
  the token sets' lock-free discard as a bounded exception to `python.md` §13.
