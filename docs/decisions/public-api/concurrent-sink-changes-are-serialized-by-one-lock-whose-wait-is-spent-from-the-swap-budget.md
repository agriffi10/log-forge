---
type: Decision
title: "Concurrent sink changes are serialized by one lock whose wait is spent from the swap budget"
description: "a `configure()` passing `sink` or `sink_factory` holds `config._sink_lock` from the config write to the end of the swap, so two at once run in sequence and the second wins config and delivery; the wait comes out of that call's one swap budget, and a wait that outlasts it proceeds unserialized with a `pending` line rather than hang or drop the configure. Never re-check the swap's post-retarget decision instead. (fix after SPEC-079)"
---

**Two `configure(sink=…)` calls at once are serialized, not reasoned about.** With a live worker,
both swaps retarget it, and `Worker.retarget` serializes those, so the worker goes A to C and then
C to B. Each swap then decides, in a second critical section, what to arm and what to release, from
what its own retarget saw. B's swap closed C as the sink it displaced; C's swap, deciding
afterwards, re-armed C; the exit closed C again with nothing written to it in between. The same gap
closes A twice by a second route: B's first critical section finds A, already displaced and no
longer held, still owed and releases it as stale, and C's swap then releases A as its own displaced
sink. Forcing the gap, both happen every run (`tests/test_concurrent_configure.py`). Measured at
`641dfc86`: 12 of 100 runs closed a sink twice, and 15 of 100 left `get_config()` naming one sink
while delivery went to the other, because the config write and the retarget were ordered
independently. With the lock, 0 of 100 for both. SPEC-078's frame-2 review first measured the double
close (9 of 25 at `fcbc3fb7`, 3 of 25 at its head); SPEC-078 had put serializing out of scope
because `configure()` was documented as not thread-safe (SPEC-045).

**The lock covers the config write too, not only the swap.** Inside the swap alone it closes each
sink once, but the config and the worker can still end on different sinks. Held from the factory
comparison through `_rebind`, `_ensure_sink` and `_swap_sink`, the call that writes the config last
is the call that swaps last. Andrew chose the wider scope on 2026-11-11; it narrows the documented
"not thread-safe" to the one thing still true, that a span finishing mid-swap may land on either
sink.

**The wait is spent from the call's budget, not added to it** (invariant 3). The budget is resolved
once, before the wait, and the swap gets what is left. That wait can run out while the holder is
still finishing, even with two callers: each clock starts before its own wait, `threading.Lock`
grants no arrival order, and a holder that spends its whole budget overruns it by its last critical
section and the release. Measured by the frame-2 review, two callers with a sink whose `emit` takes
0.1 s against a 0.3 s budget fell back in 75 of 150 rounds, none defective, and with a 0.02 s `emit`
against 1.0 s in none of 60. A call whose wait ran out proceeds without the lock and writes one
`pending` line, since running alongside the holder is the unserialized pair again: a sink may be
closed twice, or the config may name another sink than delivery. Its swap runs with nothing left,
so its drain is unconfirmed and counted in `incomplete_swaps`. Waiting without bound would break
the bound, and giving up would lose the configure, which `configure()`'s `Raises:` disowns; giving
the waiter a fresh budget after the wait was rejected for the same bound. A budget above
`threading.TIMEOUT_MAX`, `inf` included, waits without bound as `None` does,
`sinks._retry.wait_budget`'s rule (SPEC-066), because `acquire` raises `OverflowError` for it; it
reaches the swap as `None` too, where `inf` had made the swap absorb an `OverflowError` of its own
and leave the worker on the old sink.

**A sink's own code re-entering `configure()` never waits.** On the thread already holding the lock
(a stop-signal setter during the retarget), or on one of the library's own `log-foundry-` threads
(the drain, a closer), the swap is waiting on that very thread, so a wait is a stall of the whole
budget, or with an unbounded one a hang the base commit did not have. Such a call tries the lock
once and otherwise runs unserialized, announced. The no-worker swap's hand is the same case: it
runs after the swap's first section but still inside this lock, so a setter's `configure()` there
tries the lock once, swaps unconfirmed and announces it. A setter's `configure()` from the hands
outside a sink change — the first log, `flush()`, the worker build — is not re-entrant in this
sense and waits like any caller, because no library lock is held across those hands
(`docs/decisions/sink-lifecycle/a-stop-signal-setter-runs-under-no-lock-its-own-calls-would-wait-on-the-owner-decides-then-hands.md`). The lock is bound to a local and released through
it, so a thread that forks from inside the call does not release the child's fresh copy and raise
`RuntimeError` out of `configure()` there (frame-2 review, measured).

**Rejected: re-checking the post-retarget decision inside `_swap_sink`.** Arming the new sink only
while it is still the worker's looks local, but an unfenced second swap leaves its displaced sink in
the record for a later `shutdown()`. A first swap that skipped arming that sink would then leave it
owed by nobody, and every further guard of that kind answers one interleaving. The lock turns the
pair into the sequential case the swap already handles.

**The lock is outermost and not taken on the orphan path.** `stamp`, `_rebind`, `_ensure_sink` and
the swap take `_owned_lock`, `_config_lock`, `_state._lock` and the worker's lock beneath it, and
nothing holding any of those takes it. A `configure()` with neither argument, or whose
`sink_factory` is already the configured one, does not take it. It is
on `_fork._GLOBAL_PRIMITIVES`, so a child forked while a parent thread held it starts unlocked.

## History

### 2026-11-12

- **Refined, not reversed.** A `shutdown()` deliberately does not take this lock, or wait on it:
  holding it broke six shipped tests and held a stop-signal backoff behind the swap, and as a
  barrier it also waited out the swap's slow displaced closes, since the lock is held through the
  closer join. A shutdown landing mid-swap instead waits on each swap's `_Operation.decided`, within
  its closer grace (`docs/decisions/pipeline/`). The swap's fenced branch now releases its displaced
  sink only while the record still owes it, and that is not the decision re-check rejected above:
  skipping an *arm* can orphan a sink, while skipping a *release* of a sink the record no longer
  owes cannot, because every route out of the record is a take whose taker closes it.
- A sentence on a setter's `configure()` from the hands that no longer run under a lock, which
  waits like any caller (the stop-signal fix after SPEC-079).

### 2026-11-11

- **Settled**, as a fix after SPEC-079. Supersedes SPEC-078's out-of-scope line and the `SWAP` row's
  `SWAPPING` cell ("runs alongside the swap in flight"), both marked in that spec.
