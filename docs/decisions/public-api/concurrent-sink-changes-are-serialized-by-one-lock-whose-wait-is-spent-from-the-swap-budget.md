---
type: Decision
title: "Concurrent sink changes are serialized by one lock whose wait is spent from the swap budget"
description: "a `configure()` passing `sink` or `sink_factory` holds `config._sink_lock` from the config write to the end of the swap, so two at once run in sequence and the second wins config and delivery; the wait comes out of that call's one swap budget, and a wait that outlasts it proceeds unserialized with a `pending` line rather than hang or drop the configure. A non-re-entrant change is published before that write and, once it releases the lock, runs the stops deferred to it, every close detached, within its budget. Never re-check the swap's post-retarget decision instead. (fix after SPEC-079; SPEC-097)"
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

**The change is published before its config write, and runs the stops deferred to it**
(SPEC-097 FR-001, FR-002). Inside the lock, a non-re-entrant change publishes its `_Operation` in
`_swaps_running` before `_rebind`, lock-free, so a `shutdown()` on another thread meets it from the
write rather than from the swap's first section, and ends it in the `finally` before the lock is
released. A `shutdown()` made on the changing thread (a stop-signal setter the retarget runs) or
on a `log-foundry-` thread while the change is in flight is deferred rather than run under the
change. It defers only to the change registered as the taker when it published, the first running
one, and only while that change's `config._change_sink` frame is on its thread's stack, so a stop
made inside a nested or unwound change defers to the outer one or, with none running, runs as it
did before SPEC-097. That rule is read from the live stack, never from a slot, because a deferral
recorded in a slot that only a later statement clears was left stale three rounds running. A
change that raised left its deadline for the next, unrelated change, which retired the library; a
stop deferred during a re-entrant change on a library thread went onto a list only the outer
change took, or none did, and never ran; and an interrupt between a change's registration and its
end left it registered, so every later `shutdown()` and the exit drain deferred to it and closed
nothing. Each fix
reset its slot and the next review found another statement to interrupt; positive provenance from
the stack ended the class, and `tests/test_change_interrupts.py` now interrupts every line the
change runs instead of a further review. Once the lock is released, and before `configure()` returns, the change runs one stop for
every deferred call, its timeout what is left of the latest deferred deadline cut at the end of
this call's swap budget. That stop closes nothing on the changing thread: every close it takes is
detached and waited for only within a closer grace the budget bounds, so `configure()` keeps
invariant 3's bound. Run inline instead, a slow `close()` took `configure()` to 10.01 s and
15.02 s against a 5 s budget on the spec reviewer's `fr002_bound.py`. A close that outlives the
wait goes on, counted in `health().closing_sinks`, and the next stop or the exit drain waits for
it as a bystander. A `configure()` made from a signal handler or finalizer that interrupted the
library inside one of its lock sections is held after validation and runs when the interrupted call
returns (SPEC-097 FR-004).

**Rejected: re-checking the post-retarget decision inside `_swap_sink`.** Arming the new sink only
while it is still the worker's looks local, but an unfenced second swap leaves its displaced sink in
the record for a later `shutdown()`. A first swap that skipped arming that sink would then leave it
owed by nobody, and every further guard of that kind answers one interleaving. The lock turns the
pair into the sequential case the swap already handles. The swap's fenced branch releasing its
displaced sink only while the record still owes it is not that re-check: skipping an *arm* can
orphan a sink, while skipping a *release* of a sink the record no longer owes cannot, because the
worker's sink leaves the record only by a take whose taker closes it. A cede, the one route out
that closes nothing, is made only of a sink the installed graph still holds, in the section that
arms that graph, and the closer's guard puts back what it passes over, so neither applies to it
(SPEC-087; `docs/decisions/pipeline/`). SPEC-097's withhold skips an arm, but not on this ground:
it skips only the arm of a sink a stop's `_close_owed` take took since the config write, whose
taker closes it, never on a swap's stale take, so it cannot leave a sink owed by nobody; keyed on
any take, it did, 3 of 3 (`docs/decisions/pipeline/`, Part 3).

**The lock is outermost and not taken on the orphan path.** `stamp`, `_rebind`, `_ensure_sink` and
the swap take `_owned_lock`, `_config_lock`, `_state._lock` and the worker's lock beneath it, and
nothing holding any of those takes it. A `configure()` with neither argument, or whose
`sink_factory` is already the configured one, does not take it. It is
on `_fork._GLOBAL_PRIMITIVES`, so a child forked while a parent thread held it starts unlocked.

## History

### 2026-11-18

- **Refined by SPEC-097's diff reviews**: a stop defers only to the change registered as the taker,
  and only while that change's frame is on its thread's stack; the deadlines live on its
  operation. The body says why a slot was not enough, and the fence says "deferred to it".
- **Refined by SPEC-097**: the change is published before its config write and ends before the
  lock is released; a `shutdown()` made inside it, on its own thread or a `log-foundry-` thread,
  is deferred and run by the change once the lock is released, every close detached and every wait
  inside the swap budget; a `configure()` made inside a library lock section is held. The fence
  says so, and the rejected re-check says why SPEC-097's withhold of an arm is not it.

### 2026-11-15

- **Refined by SPEC-087**: the reason the fenced branch's `owes` term is not the rejected re-check
  said every route out of the owed record is a take whose taker closes it. SPEC-087 added `cede`,
  a route out that closes nothing, and a closer's take that passes a sink over and puts it back, so
  the reason is narrowed to the worker's sink, which neither reaches, and the conclusion stands.
  The body now carries it beside the rejected re-check, and the 2026-11-12 entry's clause is
  struck in place.

### 2026-11-12

- **Refined, not reversed.** A `shutdown()` deliberately does not take this lock, or wait on it:
  holding it broke six shipped tests and held a stop-signal backoff behind the swap, and as a
  barrier it also waited out the swap's slow displaced closes, since the lock is held through the
  closer join. A shutdown landing mid-swap instead waits on each swap's `_Operation.decided`, within
  its closer grace (`docs/decisions/pipeline/`). The swap's fenced branch now releases its displaced
  sink only while the record still owes it, and that is not the decision re-check rejected above:
  skipping an *arm* can orphan a sink, while skipping a *release* of a sink the record no longer
  owes cannot, because ~~every route out of the record is a take whose taker closes it~~ the
  worker's sink leaves the record only by a take whose taker closes it (corrected 2026-11-15, see
  that entry).
- A sentence on a setter's `configure()` from the hands that no longer run under a lock, which
  waits like any caller (the stop-signal fix after SPEC-079).

### 2026-11-11

- **Settled**, as a fix after SPEC-079. Supersedes SPEC-078's out-of-scope line and the `SWAP` row's
  `SWAPPING` cell ("runs alongside the swap in flight"), both marked in that spec.
