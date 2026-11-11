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
once, before the wait, and the swap gets what is left. A holder started earlier and runs inside its
own budget, which ends first, so the wait ends in time unless the holder overruns. In that case the
call proceeds without the lock and writes one `pending` line, since it cannot know whether the call
it overtook will close a sink twice. Waiting without bound would break the bound, and giving up
would lose the configure, which `configure()`'s `Raises:` disowns. A squeezed second swap may find
too little budget left to confirm its drain, and then records `incomplete_swaps` as any unconfirmed
swap does.

**Rejected: re-checking the post-retarget decision inside `_swap_sink`.** Arming the new sink only
while it is still the worker's looks local, but an unfenced second swap leaves its displaced sink in
the record for a later `shutdown()`. A first swap that skipped arming that sink would then leave it
owed by nobody, and every further guard of that kind answers one interleaving. The lock turns the
pair into the sequential case the swap already handles.

**The lock is outermost and not taken on the orphan path.** `stamp`, `_rebind`, `_ensure_sink` and
the swap take `_owned_lock`, `_config_lock`, `_state._lock` and the worker's lock beneath it, and
nothing holding any of those takes it. A `configure()` with neither argument does not take it. It is
on `_fork._GLOBAL_PRIMITIVES`, so a child forked while a parent thread held it starts unlocked.

## History

### 2026-11-11

- **Settled**, as a fix after SPEC-079. Supersedes SPEC-078's out-of-scope line and the `SWAP` row's
  `SWAPPING` cell ("runs alongside the swap in flight"), both marked in that spec.
