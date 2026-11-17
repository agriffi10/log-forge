# Completed Spec — SPEC-103: Repeated Sink Lines at Exit Are Folded

## What was completed?

- **The fold** (FR-001), in `_diag` and `worker.py`.
  - From the first abandoned exit batch on, the worker opens a fold window around each later exit
    batch's sink call: the engine run, or each 1.x `emit`.
  - Inside it, `lost`, `absorbed` and `pending` record one row per distinct line in
    `Worker._exit_fold`, a `_diag.FoldTable`.
  - `take_folded` writes each row once, ending `; <k> line(s) folded at exit`. It runs from
    `_write_exit_tallies` before the tallies, or from `stop()` before its expiry line, which also
    closes the fold.
  - A recorder never waits on `_diag._FOLD_LOCK`. One that cannot take it writes at once, which
    is what keeps a `__del__` that writes a line inside the locked section from deadlocking.
  - No take waits on it without bound either. `stop()`'s closing take waits 0.1 s at most. The
    drain's take waits in 0.1 s steps and gives way once the table is closed, so a signal handler
    that calls `shutdown()` inside the closing take cannot hold the drain (diff review frame 2).
    Rows a timed-out closing take leaves are written only if the drain's take reaches the lock;
    at interpreter exit after an expired join they are not. Their losses are still counted.
  - `lost(..., running_total=True)` is never folded. It is passed at the three throttled sites
    and at `_lifecycle._check_ledger`, whose count is a snapshot. A sweep of every `_diag.lost`
    in `src/` found no other count that is not an increment.
  - A forked child withdraws the window as `_fork._reinit_after_fork`'s first step.
    `_FOLD_LOCK` joined `_GLOBAL_PRIMITIVES` and the lifecycle inventory.
- **Timing and batching are today's** (FR-002): the SQS slowdown guard, the exit budget, the
  poison and recovered probes.
- **The record** (FR-003):
  - §12's dead-sink item is struck and moved under *Resolved*;
  - the pipeline and diagnostics decisions are refined in place, and so is the fork-order
    decision, with invariant 12, because the withdrawal now comes first;
  - SPEC-090 carries a superseded marker;
  - the README, the inventory and `docs/release-notes/v1.2.1.md` are updated.
  - "The exit drain sizes a batch from a rate it measured earlier…" is left as found.
- **The design this replaced.** SPEC-103 was first written and built to grow a dead sink's exit
  batches behind a zero target, on branch `spec-103-exit-zero-target` (last `7f400f51`, never
  merged). Six growth designs, E through E6, each broke the `shutdown()` budget for some cost
  shape. The last two broke under `spec103_sqs_slowdown_exit.py` and
  `spec103_http_slowdown_exit.py`, a sink that turns slow mid-exit. Andrew chose "Fold lines,
  keep batches" on 2026-11-17, and the size-limited trade he had accepted for growth was
  withdrawn the same day. The spec's Evidence and the pipeline decision carry the designs and
  their defects.

## What changed from earlier specs?

- **SPEC-090 FR-003** said a sink's and the engine's lines at exit follow the number of exit
  batches. After the first abandoned exit batch they are now folded.
- **SPEC-039, SPEC-079 and SPEC-081's fork order** gains a first step, ahead of the pin.
- **`_diag`** gains a fifth stderr write, and `lost` a keyword.

## Verification

- **Probes at `f28f17d4`**, identical on 3.13.1 and 3.12.12, with the before at `4599a0c5`:

  | Probe | Before | After |
  |---|---|---|
  | `spec090_dead_http_exit.py zero 10000 3` (calls / lines / `failed_batches`) | 16,016 / 1,004 / 1,001 | 16,016 / 6 / 1,001 |
  | `cap` mode | 321 / 22 / 20 | 321 / 4 / 20 |
  | `spec090_exit_budget.py tree` | — | unchanged |
  | SQS and SNS slowdown | — | the drain ends 0.18 to 0.20 s before the deadline |
  | HTTP slowdown `1.0` and `1.0 0.002` | — | one close |
  | poison | — | 3,002 of 4,002 |
  | `spec103_recovered_slow_exit.py 2 2000 1.2` | — | the join completed |

- **The growth guard against `7f400f51`.** The committed guard,
  `test_a_sqs_sink_that_turns_slow_mid_exit_ends_before_the_deadline`, was copied into an export
  of `7f400f51` and run there at completion, on 3.13.1. It failed with the drain ending 1.60,
  2.81 and 6.42 s past the deadline, and it passes on the build.
- **Mutation.** The build's plan names a mutant for every check in the fold, the rosters and the
  fork step (M1 to M29), and the build session reported their results. At completion nine were
  re-run in place, on 3.13.1, against the six SPEC-103 test files under `-n 8`, each restored and
  checked by hash. Eight were killed by the test for their reason, as below; the ninth, M15, is
  equivalent:

  | Mutant | Killed by |
  |---|---|
  | running totals folded | the running-total test |
  | `context`'s keyword dropped | the roster |
  | `closed` ignored | the closed-table and expiry tests |
  | `close=False` in `stop()` | the expiry test |
  | `fold = exit_batch` | the before-the-first-abandon, twin and retry-declined tests |
  | the child withdrawal dropped | all three fork cases |
  | the roster row dropped | the roster and inventory tests |
  | `reopen()` dropped | the reset and inventory tests |

  **M15, `fold = self._exit_abandoned_seen`, is equivalent.** The flag is set only by
  `_tally_exit_abandon` for an exit batch, after the stop. `_stopped` is never cleared while it
  stands: `_reinit_after_fork` clears both together. And `_emit_pending` is `_emit`'s only
  caller. So whenever the flag is set, `exit_batch` is true.

  One test, `test_a_finalizer_that_writes_inside_the_fold_lock_does_not_deadlock_the_exit`, also
  failed on its premise under `-n 8` on the unmutated build. It passed alone under `-n 0`. It
  relied on `gc.set_threshold(1, 1, 1)` to run a finalizer inside the locked section, which
  depends on what ran before it in the process. It now makes the re-entry deterministic: the
  table's rows are a `dict` whose lookup, made under the lock, writes a `_diag` line. Each
  folding writer carries its own inline acquire, so the guard is per writer: the never-waits unit
  test and that re-entry test are each parametrized over `lost`, `absorbed` and `pending`. Three
  mutants, each turning one writer's non-blocking acquire into a blocking one, were each killed
  by both: the unit test with "`<writer>` waited 10.01s on the fold lock", the re-entry test with
  "the drain thread deadlocked on the fold lock". ~~M24 (recorders block)~~, which blocked all
  three at once and so could not show that each is guarded, is replaced by those three. M25
  (every acquire blocks, the closing take's too) was killed with "stop() did not return".

  After diff review frame 2, a recording stand-in for the lock asserts that each writer asks
  with `blocking=False`. A bounded `acquire(timeout=0.5)` in each writer had survived the timing
  test, and is now killed per writer ("asked for the lock: [((), {'timeout': 0.5})]"). An
  unbounded drain take is killed by the signal-handler test ("the nested shutdown() waited
  5.00s") and by a unit test. `_check_ledger` without the keyword is killed by the snapshot
  roster.
