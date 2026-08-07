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

**A swap discards the previous sink's absorbed losses from `Health.sink`, and that is intended.**
`Health.sink` is a live read of whichever sink is current, so at the moment of a swap the old
sink's `dropped`/`failed` leave the snapshot for good. Carrying them forward would change what the
field means — SPEC-026 defines it as *the configured sink's own* counters, and a sum across sinks
that no longer exist is a different claim. Documented in `health()`'s docstring; the stderr lines
those losses produced remain the durable record.

~~**Post-close sink loss is still unowned, and this spec is not its home.** SPEC-028's delivery doc
asked that it be named here explicitly: `GooglePubSubSink.emit` after `close()` appends futures
nothing will resolve, and `KafkaSink` accepts produces past close. That is the SPEC-026 silent-loss
shape one call later, at the *sink* level. SPEC-030's signals do not reach it — `retired` describes
the worker, and a sink accepting writes after its own `close()` is invisible to it — and neither
sink takes a transport lock, so neither is in SPEC-028's enforced roster. SPEC-031 does not cover it
either. It needs a fix of its own, alongside the two sinks that already refuse (`SQLiteSink`,
`MongoDBSink`) and the lint-scope gap SPEC-028 recorded.~~ — **closed by SPEC-032**, which is that
fix. The reasoning above was confirmed by measurement rather than merely inherited: an
`info()` after `shutdown()` into a `KafkaSink` lost the event with `submitted_after_shutdown`
reading `0`, because the orphan path emits on the caller's thread and never passes through
`submit`. SPEC-032 took the lint-scope gap with it, since the post-close roster derives from that
gate.

**The end-to-end swap budget is resolved at call time, not bound as a default argument.**
`_swap_live_sink` passes `worker.DEFAULT_SWAP_TIMEOUT` explicitly. A default argument is bound at
definition, which would have left the bounded-swap test pinning a number nothing reads.

## Review round (pre-merge, PR #117)

Returned MERGE WITH CHANGES on green CI, with two confirmed defects the suite could not see.

- **A `shutdown()` landing mid-swap leaked the live sink forever.** `swap_sink` re-took only half
  its guard after the first drain: it re-checked `old is new_sink` but not `_shutdown_done`. A
  blocking call cannot be trusted to return into the state it left — `shutdown()` closes whatever
  `self.sink` was at that moment and latches its once-only flag, so the swap then installed a sink
  **nothing would ever close**, and announced that the previous sink was "left open" when it had
  just been closed. Both a leak and a false diagnostic. Fixed with the missing re-check, and pinned
  by a test that injects the race rather than running it.
- **`configure()` is not bounded end to end.** The shared deadline covers the two drains; closing
  the previous sink is outside it, because `Sink.close` takes no timeout — measured at 8.0 s
  against a 5 s budget. Four places claimed otherwise, including a ticked acceptance criterion.
  This is `architecture.md` §13's existing `shutdown()` constraint reached by a second route, with
  the same fix (an interruptible close, i.e. a sink-contract change) and the same rejected
  alternative (SPEC-028 built and reverted the daemon-thread close). Not closing the old sink at
  all would leak it on every swap. So: recorded rather than fixed, the four claims corrected, the
  criterion amended in place, and a test pins the behaviour so a later reader cannot mistake it for
  a bounded call.
- **`decorator._swap_sink`'s exception guard was untested** — deleting the whole `try/except` left
  all 1006 tests green. A SPEC-025 guarantee at a new call site, pinned by nothing. Now tested.
- **FR-002's "Normal submission is not measurably slowed" was ticked with no measurement.** The
  criterion overstated what a test can show at that scale; amended in place to the claim the test
  actually establishes.
- **`_close_swapped_out`'s docstring overstated its own guarantee** — the fence proves the *drain
  thread* is out of the old sink's `emit`, but an orphan-path emitter that resolved the sink before
  `configure()` reassigned it can still be inside one. Corrected, pointing at the `close()`
  tolerance `sinks/base.py` already requires.

The reviewer independently reproduced all eleven claimed mutant kills and added ten more, finding
no vacuous tests among the new ones.

## Second review round (pre-merge, PR #117)

Verified both code fixes as real — 3000 randomized three-thread interleavings across the blocking
drain found no further TOCTOU window, no double close and no orphaned sink — and returned three
findings, all taken.

- **The unbounded-close test was flaky, and would have reddened CI rather than caught anything.**
  It gave the drains a 10 ms budget, and an unconfirmed drain returns *before* the close, so under
  load the test failed on its first assertion: 122 of 200 runs on a loaded machine, 0 of 25 idle.
  The budget must be generous and the *gap* between budget and close tight, not the reverse. Now
  0.3 s against a 0.5 s close: 40 of 40 under the same eight-thread load, and it still kills a
  daemon-thread-bounded close.
- **The §13 constraint cited a precedent whose main leg does not reach this site.**
  `_close_if_owed` rejects a threaded close for two reasons, and the first — the daemon killed
  mid-`commit()` — is an *interpreter-exit* hazard that cannot occur at a swap in a live process.
  Only the second survives: an expired join still cannot tell a slow-but-successful close from a
  stuck one, so it would report a loss for a swap that completed. Same conclusion, half the
  argument, now said explicitly in §13 and in `_close_swapped_out`.
- **FR-004's first criterion still certified `configure()`'s docstring as saying the swap "is
  bounded"** — the sibling of the criterion amended in the first round, missed when that one was
  fixed, and by then certifying a docstring that says the opposite. Amended in place. Two of its own mutants survived and were correctly judged
pre-existing conventions rather than regressions: the worker's counter increments are not pinned
in-the-lock the way SPEC-028 pins the sinks', on `main` as well as here.

## Verification

- 1009 tests pass; `ruff`, `mypy --strict` and `spec-lint` clean. Full suite run three times for
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
