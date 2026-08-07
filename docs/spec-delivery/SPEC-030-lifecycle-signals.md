# Completed Spec — SPEC-030: Lifecycle Signals

## What was completed?

Two documented user errors produced total, permanent, **silent** log loss. Both now signal.

- **Logging after `shutdown()` is visible** (FR-001, FR-002). `Health` gains `retired` and
  `submitted_after_shutdown`; `Worker.submit` counts a post-shutdown submission and warns once,
  then on `_DROP_WARN_EVERY`'s existing throttle, naming `flush()` as the remedy. The check is one
  unlocked read of a flag that is only ever set — SPEC-019 excluded a *liveness* check from
  `submit`, and this is not one. `stopped_reason` is untouched and stays `None` after a clean
  shutdown, which is why the state needed its own pair of fields: the documented alert idiom could
  not fire on it before.
- **A late `configure(sink=...)` swaps the live target** (FR-003). `Worker.swap_sink` drains to the
  old sink, reassigns, fences with a second drain, then closes the old sink — reassignment rather
  than a rebuild, so the queue, thread, counters and `atexit` registration survive. Wired through
  `decorator._swap_sink` and `config._swap_live_sink`. Both drains share one deadline
  (`DEFAULT_SWAP_TIMEOUT = 5.0`).
- **Both lifecycles are documented at the point of invitation** (FR-004): `configure()`,
  `shutdown()`, `health()`, README (`configure(...)`, the `health()` table and idiom, the serverless
  handler example) and `architecture.md` §7, §9, §13.

**Deviation: a third `Health` field.** FR-003 requires an unconfirmed drain to reach `health()` and
the spec's Data Model provided nowhere to put it. `incomplete_swaps` was added and the Data Model
amended in place. `stopped_reason` could not carry it (it means the thread is *gone*, and it is
alive) and neither could `failed_batches` (a different fact, a different remedy).

**Deviation: the fence drain.** The spec's outline was `flush` → swap → `close`. A second drain runs
after the reassignment, because the first only proves the *pre-swap* events landed: a span finishing
on another thread in that window can leave the drain thread inside the old sink's `emit`, and
closing under a writer is what SPEC-028 spent a spec preventing. It costs nothing on an idle queue
and shares the same deadline.

## What changed from earlier specs?

- **SPEC-013's `shutdown()` is still terminal and the worker still never restarts** — the Out of
  Scope held. What changed is only that the consequence is now reported.
- **SPEC-019's `stopped_reason` semantics are unchanged and were deliberately not reused.** A
  boolean was acceptable here where an `alive` flag was not: `retired` is `False` for a process that
  never logged, which is true, because it describes an action the caller took rather than a failure
  the library detected.
- **SPEC-026's `Health.sink` now describes whichever sink is live**, so a swap takes the previous
  sink's absorbed losses out of the snapshot with it. Recorded in `health()`'s docstring.
- **SPEC-027 FR-004's reasoning is reused verbatim** for the unconfirmed-drain path: the old sink is
  left open, because a leaked resource beats a close raced against a write.
- `configure()` gained a side effect it never had. It is still a startup call and still not
  thread-safe; a span finishing on another thread mid-swap may land on either sink.

## Notes for the next spec

**`incomplete_swaps` fires on any unconfirmed drain, including one whose `flush()` returned `False`
because a batch was abandoned rather than because it timed out.** `flush()` answers with one
boolean, so the swap cannot tell the two apart, and it treats both conservatively: count, leave the
old sink open, announce. That double-signals with `failed_batches` in the abandoned case. Accepted —
the alternative is a false claim that the old sink is idle.

**Post-close sink loss is still unowned, and this spec is not its home.** SPEC-028's delivery doc
asked that it be named here explicitly: `GooglePubSubSink.emit` after `close()` appends futures
nothing will resolve, and `KafkaSink` accepts produces past close. That is the SPEC-026 silent-loss
shape one call later, at the *sink* level. SPEC-030's signals do not reach it — `retired` describes
the worker, and a sink accepting writes after its own `close()` is invisible to it — and neither
sink takes a transport lock, so neither is in SPEC-028's enforced roster. SPEC-031 does not cover it
either. It needs a fix of its own, alongside the two sinks that already refuse (`SQLiteSink`,
`MongoDBSink`) and the lint-scope gap SPEC-028 recorded.

**The end-to-end swap budget is resolved at call time, not bound as a default argument.**
`_swap_live_sink` passes `worker.DEFAULT_SWAP_TIMEOUT` explicitly. A default argument is bound at
definition, which would have left the bounded-swap test pinning a number nothing reads.

## Verification

- 1006 tests pass; `ruff`, `mypy --strict` and `spec-lint` clean. Full suite run three times for
  thread-timing flakiness.
- **Every new assertion was mutation-checked in place** (not in a repo copy — the editable install
  resolves back to the working tree, which is what makes the check meaningful). Eleven mutants, each
  applied alone and reverted: `submit`'s retired check removed; `retired` pinned to `False`; the
  warning un-throttled; the counter moved after the announcement; the pre-swap drain removed; the
  old sink closed despite an unconfirmed drain; the stop signal not re-offered; the retired guard
  dropped; the same-sink no-op guard dropped; `configure()` swapping without a `sink=`; the fence
  drain removed and the fence given a fresh full budget. Every one is killed by the test that
  advertises it.
- **One mutant initially survived and produced a new test.** Moving the counter after the
  announcement changed nothing, because `_diag` swallows an `Exception` from stderr, so the
  record-first ordering was unpinned. It is now pinned the way SPEC-029 pins the others — a stderr
  that raises a `BaseException`, which `_diag` passes through by design — and that test is the only
  place in the suite where `submit()` raises into its caller, which is correct and deliberate.
- **The fence drain is pinned structurally, not by racing it** (the precedent is SPEC-028's
  in-the-lock assertions): the test records the live sink and remaining budget at each drain and
  asserts `[old, new]` with a shrinking deadline. Reaching the window it protects would need a span
  finishing on another thread mid-swap.
