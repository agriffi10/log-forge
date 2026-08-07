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
  `decorator._swap_sink` and `config._swap_live_sink`. All three waits — both drains and the close
  — share one deadline (`DEFAULT_SWAP_TIMEOUT = 5.0`); see the follow-up below for the close.
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
- ~~**`configure()` is not bounded end to end.**~~ **Closed by the follow-up below.** It was not:
  the shared deadline covered the two drains, and closing the previous sink sat outside it,
  because `Sink.close` takes no timeout — measured at 8.0 s
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
  The budget must be generous and the *gap* between budget and close tight, not the reverse. The
  test survives into the follow-up below, inverted to assert the bound it once denied.
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

## Follow-up: the close was bounded after all

Both reviews accepted "record it, don't fix it" for the unbounded close. That was overturned on
instruction, and the second review's own finding is what opened the door: it observed that of
SPEC-028's two reasons for reverting a threaded close, only one reaches a swap.

- The close runs on its own thread, joined for what is left of the swap's budget, so **one
  deadline covers all four steps** and `configure()` cannot be held past it.
- **Nothing is derived from an expired join** — no counter, no line. That dissolves SPEC-028's
  objection ("an expired join cannot tell a slow-but-successful close from a stuck one, so it
  reports a loss for a swap that completed") instead of arguing with it. `incomplete_swaps` keeps
  its narrower meaning, a *drain* that could not be confirmed, so it cannot latch on a healthy swap.
- `Health.closing_sinks` is the observable that replaces the signal not taken — a **live gauge** of
  the closes running at the instant it is read, the ninth field and the only one that falls as well
  as rises. A live fact carries none of the ambiguity of an inference from a timeout, and without
  it a permanently hung close would be invisible where it used to be a visibly hung `configure()`.
- **Neither thread flag is sufficient on its own, and both were built.** Non-daemon shipped for
  the first review round and was measured worse: CPython joins non-daemon threads *before*
  running `atexit`, so one hung close stopped the exit drain entirely — the **live** sink never
  drained or closed, its buffered events lost, the application's own exit handlers never run, and
  the process hung until killed. Daemon alone loses the opposite case, which the second review
  measured: a close that is slow but *succeeding* is killed at exit, and for a sink whose
  `close()` **is** its delivery (`KafkaSink.close()` flushes the producer) that is its whole
  buffer — the same swap kept those events under a non-daemon thread and lost them under a daemon.
- **So the flag is not the mechanism; the capped grace is.** `shutdown()` drains and closes the
  live sink, then joins any outstanding closer for `DEFAULT_CLOSER_GRACE = 2.0`, carved from its
  own budget. A slow close finishes and a hung one costs only the grace. The cap is what does the
  work: without it a stuck close holds the process at exit for the whole 30 s shutdown budget, and
  a close still running at this point already had the swap's entire budget, so it is far more
  likely stuck than slow. The grace is granted on the idempotent path too — a first `shutdown()`
  that expired on a wedged drain thread returns before reaching it, and the `atexit` call behind
  it would otherwise return instantly, denying the grace to a close moments from finishing.
- **The ordering is defence in depth, not the guarantee, and the third review caught me claiming
  otherwise.** Swapping `_close_if_owed` and `_join_closers` leaves the whole suite green, and a
  real process-exit measurement delivers the live sink identically either way, because the cap
  returns control long before anything is at risk. It is still the right order — it is what holds
  if an external deadline kills the process *during* the grace — so it is now pinned by a test
  that records which ran first, and the prose says "defence in depth" rather than "the whole
  point".
- **The SPEC-028 reading that seemed to forbid a thread here was the wrong one, but its
  interpreter-exit objection does reach this site once the close outlives `configure()`.** That
  spec refused to abandon the sink the worker was *still delivering to*, and this one is fenced out
  by two confirmed drains — but an abandoned close is killed wherever it has reached, which for
  `SQLiteSink` can be inside `commit()`: the partial write SPEC-027 FR-004 ranks worse than a
  leaked handle. The grace makes it unlikely rather than impossible, and §13 says so rather than
  claiming the objection does not apply.
- `shutdown()`'s close is **unchanged** and stays inline. Its constraint in §13 stands; only the
  swap's half is struck through as closed.
- Two residual costs in §13: a close still running after the grace is abandoned, losing that
  sink's own tail with `closing_sinks` as the only warning; and that abandonment can land inside a
  `commit()`.

Twenty mutants across the four rounds, restored from a scratchpad copy named by base SHA rather
than `git checkout --` (which would have reverted the fix under test; a reviewer hit the adjacent
trap of an unnamed copy from an earlier round and silently restored the wrong commit). Beyond the
earlier fourteen: the two `shutdown` calls swapped in order; the grace skipped on the idempotent
path; the roster lock held across the joins; the grace given per closer rather than shared; and
two on the `timeout=None` path (grace skipped, and joined forever). Each is killed by the test
that advertises it — the last of them off-thread, so a regression that joins forever fails the
bound instead of hanging the suite.

**Six of those mutants were a review's finding, not mine, and two rounds of my own new tests were
vacuous.** Round two: `fresh`, `tenx` and `forget` all survived the entire 1036-test suite, because
`assert elapsed < 5.0` against a 0.3 s budget only says "not *totally* unbounded" and a fast close
completes before the assertion runs whether or not anything joined it. Round three: my three grace
tests survived their own mutants for the same class of reason — the first released the hung close
*before* calling `shutdown()`, so the daemon finished on its own and the assertion passed against a
`shutdown` that skipped the grace entirely. The bound is now asserted against the budget, the
shared deadline is pinned structurally by recording what each of the three stages is handed, and
the grace is released by a timer *during* `shutdown` so only a real join can satisfy it.

## Verification

- 1047 tests pass; `ruff`, `mypy --strict` and `spec-lint` clean. Full suite run three times for
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
