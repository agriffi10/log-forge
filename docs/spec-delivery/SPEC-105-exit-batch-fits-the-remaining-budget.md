# Completed Spec — SPEC-105: Once an Exit Batch Is Delivered, Each Later One Fits the Remaining Shutdown Budget

## What was completed?

- **The rate** (FR-001), in `worker.py`: `Worker._exit_rate`, the events a second of the most
  recent delivered exit batch, `0.0` when none is recorded; set by `_emit_pending` after a
  delivered exit batch, `0.0` for one whose `elapsed` is zero or below or whose emit straddled a
  sink change; reset in `__init__` and in `_reinit_after_fork` before its `resume` split; one row
  in `tests/test_lifecycle_inventory.py`.
- **The bound** (FR-001, FR-002): `Worker._fit_exit_target`, called after every emitted exit batch
  that did not straddle a sink change, delivered or failed, lowers the target to at most
  `_exit_rate × (deadline − now − _JOIN_MARGIN)`. A failed exit batch still keeps its target, and
  the bound then lowers what it kept.
- **The guard** (FR-003): the bound applies only when that room is at least `_exit_longest`, when
  the exit budget's prediction would let the next batch start. A declined exit batch, once a rate
  is recorded, restores the rate rule's target, `min(cap, _exit_rate × flush_interval / 2)`, so a
  decline after the guard passed costs at most one declined batch.
- **The docstrings**: `_emit_pending`, `_set_target`, `_retry_declined`, `_fit_exit_target`,
  `Worker.__init__`'s exit fields, `_reinit_after_fork`, `_JOIN_MARGIN`, `_emit`, and
  `sinks/nats.py`'s module headline, each sentence the §12 item showed false now qualified.
- **The record** (FR-006): the sink-lifecycle decision *The exit drain declines a retry, or a later
  batch, predicted to end past the shutdown deadline* refined (fence, body, two trades and a
  `retarget` note, four rejected alternatives); the pipeline decision *A backlog drains in larger
  batches, and capacity is not a setting* names the bound and qualifies its interval sentence;
  invariant 3's *Guarded* clause qualified; §12's "The exit drain sizes a batch from a rate it
  measured earlier…" has its first shape closed in place, with a *Resolved* entry, and stays open
  for the other two; the `Worker` inventory row; SPEC-103's marker; the README and
  `docs/release-notes/v1.2.1.md` caveat narrowed.
- **Two corrections to the spec at completion**, in its revision history: the sentence FR-006
  named in "the `Worker` class docstring" is in `Worker.__init__`'s; and FR-002's two `retarget`
  criteria pin state through `_reassign`, a test stand-in for `retarget`'s locked reassign, since
  no `retarget` can reassign during a `shutdown()`'s exit drain, which makes the exit path's
  sink-change handling defensive.

## What changed from earlier specs?

- SPEC-090's size target is now lowered, never raised, after an exit batch when the budget binds.
  At a tight `shutdown()` the sink is handed fewer events and the join completes where it
  expired.
- In the last `_JOIN_MARGIN` before the deadline, a batch SPEC-090's rule started and delivered
  whole by luck is trimmed, and SPEC-062's prediction declines its tail, counted in the existing
  declined line (Andrew, 2026-11-17).

## Measured

`docs/audits/2026-11-14-probes/core-pipeline/spec103_recovered_slow_exit.py 2 2000 <t>` at
`e46c367c`, two runs a cell. **The machine was not quiet:** other sessions were running, with the
one-minute load average at 61 at the start and 27 at the end.

| Timeout | 3.12.12 | 3.13.1 |
|---|---|---|
| 0.45 s | completes, 0.34 to 0.35 s; second exit batch 36 to 38 | completes, 0.34 to 0.35 s; 38 to 44 |
| 0.5 s | 0.39 s; 56 to 62 | 0.38 to 0.39 s; 58 to 60 |
| 0.6 s | 0.48 s; 102 to 104 | 0.47 to 0.51 s; 100 to 120 |
| 0.65 s | 0.50 to 0.53 s; 116 to 128 | 0.51 to 0.52 s; 118 to 122 |
| 2.0 s | 1.67 to 1.69 s; 708 to 718 handed | 1.67 to 1.71 s; 716 to 734 |
| 30 s | 8.31 s; 3,982 handed | 8.30 to 8.32 s; 3,982 |

At `d9f2f097`, before the build, the join expired at 0.45 to 0.65 s on 3.13.1 with a second exit
batch of 222 to 248 events (the §12 item's before-and-after table).

**Mutation runs** at `901b6690`, each mutant scoped to one method and restored from the committed
bytes:

| Mutant | Killed by, and the reason |
|---|---|
| M1 guard removed | the ledger test (`2 exit batch(es) declined`, against 1); the within-the-margin test (980 in 9 against 8); the delivered-path test (926 in 9 against 8); the 199-span test (256 in 3 against 2) |
| M2 guard weakened to `room > 0` | **only the 0.6 s, 199-span test** (256 in 3 against 2); it survives the ledger test, the 200-span test (258 in 3 either way), the within-the-margin test and the delivered-path test |
| M3 delivered-path bound removed | the fake-clock tests on both twins (`[20, 128]` against `[20, 54]`), the margin-band test (`[20, 122]`), and both FR-004 real-clock tests |
| M4 failed-path bound removed | the flapping test (`[20, 118, 118, 118, 118]`) |
| M5 failed-path generation check dropped | the straddle test (target 102.0 against 118.0) |
| M6 rate-record generation check dropped | the delivered-straddle test (`_exit_rate` 256.0 against 0.0) |
| M7 restore removed | the restore test on both twins (980 in 49 against 9) |
| M8 restore without the rate check | the no-rate decline test (1,980 in 50 against 2) |
| M9 guard `>=` turned to `>` | the boundary test (`[20, 128]` against `[20, 20]`) |

On FR-004 AC3, M3 failed
`test_a_tight_shutdown_completes_its_join_behind_a_sink_that_recovered_by_exit` on
`stopped_reason`, `ShutdownTimeout`, with 228 events in flight; the return-time assertion passed
under the mutant, so it alone could not tell the two apart.

**From the second diff review.** A decline after the guard passed, in a window narrower than one
take, cut the whole declined backlog at the lowered target (980 events in 49 batches against 8); a
declined exit batch now restores the rate rule's target, and that case names 9, the accepted
one-batch trade. The delivered-path test now asserts its declined line exactly (926 in 8), which
is what lets M1 fail it. An exit batch whose emit the clock cannot see records no rate and is
flagged as an open remainder in §12, not built.

## Verification

Ruff, mypy, pytest (13,213 passed on 3.12.12 and 3.13.1), spec-lint and its corpus, docs-lint,
docstring-lint, the all-extras suite and the integration suite against nine services all exited 0
at `e46c367c`, and the suite again at `901b6690` (13,218 passed on 3.12.12 and 3.13.1). The two
FR-004 real-clock tests passed 20 of 20 runs in a row beside a full parallel suite, at both heads.
Runner-collected test ids over the four touched test files went from 147 at `c0fd19a1` to 165 at
`e46c367c`, with none removed.
