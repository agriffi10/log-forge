# Completed Spec — SPEC-104: NATSSink's Abandoned-Publish Latch Ages, as Its PING Latch Does

## What was completed?

- **The aged latch** (FR-001), in `sinks/nats.py`: `self._stuck` records each abandoned publish
  beside the `time.monotonic()` instant `_publish_bounded` abandoned it, and `_stuck_refusal`
  reads the new `_stuck_aged`, so the latch refuses before the lock and under it only once the
  publish has been pending one `publish_timeout` since its abandonment. `_publish_stuck` keeps its
  meaning, any age, and `close()` still reads it. `_stuck_error` builds the one error text every
  refusal of the latch carries. The uncalled `_refuse_while_stuck` is deleted.
- **The wait** (FR-002): `_await_stuck` is the first step of `_round_on_loop` and
  `_flush_on_loop`. It waits with `asyncio.wait` on the pending abandoned publish until the
  window's end, never cancels it, reads no stop signal, and re-reads `_publish_stuck` afterwards;
  a call that finds it still pending refuses with the latch's error, `RETRYABLE` for a round.
- **The docstrings** (FR-004): `DRIVER_DEADLINE_MARGIN` names the abandonment-dating exception and
  why; the class docstring and every docstring and `Raises:` clause naming the latch, the ones
  FR-004 AC6 lists, `_verdict_of`, `_round`, `_register`, `close` and `flush` among them, state the
  aged rule, the old sentences struck where they were stated.
- **The tests.** In `tests/test_sinks_nats_liveness.py`: the latch before and under the lock, the
  record clearing, the wait for `emit`, `deliver` and `flush`, the window's end, the order of the
  driver's calls on the worker, orphan and `flush` paths, and the caller's CPU. In
  `tests/test_sinks_nats_matrix.py`: the abandoned-publish next-caller cells assert the aged
  refusal, the four queued cells take `STUCK_QUEUED_BOUND = 2.5 * P`, a `*-emit1-young-window`
  column, and the orphan-path test. In `tests/test_sinks_nats.py`: FR-003's four library-level
  tests, and the stop-signal test's `[young-abandoned]` case.
- **The record** (FR-004): the decision *A driver's liveness flag is not evidence*
  (`docs/decisions/sink-lifecycle/`) refined in place, with a marker in *An event a NATS round
  never handed to the driver is resendable* (`docs/decisions/sink-delivery/`); invariants 2, 3 and
  4; §12's item moved under *Resolved*; markers on SPEC-061, SPEC-074, SPEC-102 and SPEC-061's
  delivery doc; and `docs/release-notes/v1.2.1.md`. The README states neither rule.
- **Four corrections to the spec at completion**, in its revision history: FR-003 AC3 needs an
  exit backlog of more than one exit batch; FR-001 AC1's and FR-002 AC9's tests live in the matrix
  file; FR-002 AC8 reads CPU through `conftest.cpu_window("thread")`; and the exit-bound sentence
  lives in the `DEFAULT_PUBLISH_TIMEOUT` and class docstrings, not the module docstring.

## What changed from earlier specs?

- SPEC-061's abandoned-publish latch refused every later call at once while the publish was
  pending. A call made within one `publish_timeout` of the abandonment now waits for it on the
  loop, so an orphan-path level call can block the application's thread for up to one
  `publish_timeout` where it was refused at once (invariant 4's new recorded exception), and a
  caller queued behind the abandoning round takes about `2 * publish_timeout`.
- SPEC-102's resend order is kept by the wait and its re-check rather than by an immediate refusal.
- The frozen-clock harness of SPEC-074's matrix (`tests/nats_engine_matrix_harness.py`) dates the
  abandonment on its frozen clock, so the latch never ages there: each refused round now spends
  one real budget (0.2 s) before the same refusal, and no recorded case changes.

## Measured

A queued caller takes about `2 * P`, inside `STUCK_QUEUED_BOUND`. The transient exit test
returned in about 1.8 s with `batch_lost` 0 and `sink_lost` 1; the slow exit (`P` 0.5 s, `S`
2.0 s) returned in 1.50 s. In three runs of each case the loss lines summed exactly to the
ledger's losses. All measured on SPEC-104's final tree.

## Verification

- On SPEC-104's final tree: ruff, mypy, spec-lint, docs-lint and docstring-lint exit 0. pytest
  exits 0 on 3.13.1 in three full runs, and on 3.12.12 in a separate venv. The integration suite
  run as CI runs it: 34 passed, `test_nats.py` alone 13. The extras unit suite: 12030 passed. The
  five NATS test files exited 0 five times running under `-n 12`.
- The collect-only diff against `origin/main` at `1dc174fb`, 12028 to 12052 IDs, is additions
  only, plus three changes: the liveness test renamed to
  `test_an_abandoned_publish_holds_later_callers_to_its_window_until_it_finishes`, the stop-signal
  test gaining `[clear]` and `[young-abandoned]`, and
  `test_numeric_door_roster.py::test_removing_a_helper_call_is_reported` re-keying
  `NATSSink.max_reconnect_attempts@488` to `@508`, not a removal.
- Eleven mutants, each planted inside its function and reverted, each killed on its stated reason:
  - M1a (`_stuck_aged`'s `>=` to `<`): the 12 matrix next-caller cells, and
    `test_a_young_abandoned_publish_is_not_refused_before_the_lock_and_an_aged_one_is`.
  - M1b (never ages): the same 12 cells, and
    `test_a_caller_that_takes_the_lock_after_the_window_aged_puts_nothing_on_the_loop`.
  - M2 (any pending): `test_a_transient_stall_at_the_stop_loses_no_batch` (`batch_lost=100`),
    `test_a_transient_stall_in_the_steady_drain_loses_no_batch` (`batch_lost=10`),
    `test_a_publish_finishing_inside_its_window_is_waited_for` and the young/aged read test.
  - M3 (waits to the call's own deadline): the 4 young-window cells, and
    `test_a_call_entering_late_in_the_window_is_refused_at_the_windows_end_not_its_own_deadline`
    ("waited 2.00s").
  - M4 (no re-check): `test_no_publish_or_flush_starts_while_an_abandoned_publish_is_pending` on
    its order in all three cases, the young-window cells, and
    `test_a_call_in_the_young_window_of_a_long_stall_is_refused_at_the_windows_end`.
  - M5 (the under-lock read returns `None`): the nothing-on-the-loop test.
  - M6 (`close()` reads `_stuck_aged`): `owned-core-close-after-stuck` on "1 PING(s)", and
    `owned-js-close-after-stuck` on the closing PING's `TimeoutError`.
  - M7 (cancels on timeout): the young-window-of-a-long-stall test.
  - M8 (a stop signal shortens the wait): the stop-signal test's `[young-abandoned]` case.
  - M9 (a caller-side spin): `test_a_wait_of_one_window_is_idle_on_the_callers_thread`, 0.962 s of
    CPU, run against the liveness file alone, since against all six files the spin crashed the
    xdist workers.
  - M10 (dated from the call's start): both transient-stall tests and the waited-for test.
- One unrelated flake: `test_sanitize.py::test_the_finite_path_is_not_measurably_more_expensive`
  failed once in a full run, then passed alone and in every later run.
- Owed to CI: nothing beyond the normal jobs.
