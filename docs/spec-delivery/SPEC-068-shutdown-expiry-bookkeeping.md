# Completed Spec — SPEC-068: Shutdown-Expiry Bookkeeping

## What was completed?

- `_diag.pending` (a fourth writer): a line for an outcome not yet decided, with no `lost N ...`
  prefix. The `ShutdownTimeout` expiry line and the unconfirmed-swap line are written on it, with a
  queued clause from `Worker._queued_submissions` (replacing `_queued_or_unknown`), which counts
  submissions only, never the `_SHUTDOWN` sentinel or a flush marker. Each event queued or in
  flight at an expiry is announced lost at most once; the swap line now fits `_diag`'s bound.
- An expired `shutdown()` is final, recorded as a decision and pinned, including in a real
  interpreter exit: `atexit` returns at once and leaves a sink still inside an emit unclosed.
- A `shutdown()` made on the drain thread (inside a sink's `emit`) never joins and never waits on
  that thread. A drain-thread winner records `Worker._join_owed`, and the next caller off the
  thread takes the gate over in `claim_stop` with its own deadline — once.
- `_reinit_after_fork`'s resume branch resets `_stopped` and `_join_owed` and takes its stop event
  from `refresh_stop_signal()`, so a worker stopped directly and then forked delivers in the child
  and declines retries against the child's own deadline.
- Tests: `tests/test_shutdown_expiry_bookkeeping.py`, plus the FR-003 pins in
  `tests/test_shutdown_lifecycle.py` and the `pending` rows in `tests/test_diag.py`.
- Deviations: the plan review found two fork tests that passed only through the FR-005 defect;
  they now really retire their child (listed in the spec's Scope). FR-005 is not caller-visible —
  only a direct `Worker.stop()` leaves a live worker stopped — so it has no release note, and the
  spec's API section says so.

## What changed from earlier specs?

- SPEC-062 FR-005 AC-4's byte-for-byte promise for the expiry line is superseded: the line no
  longer opens `lost N item(s); `. SPEC-062's decision is refined with the drain-thread handover.
- SPEC-039's note that the drain thread never reads `self._thread` is struck in the docstring and
  in `architecture.md` §13; the window it protected is argued unreachable instead.
- `architecture.md` §12: three SPEC-062 items Resolved; two new Open items (the after-shutdown
  submission line; `flush()` and `configure(sink=…)` on the drain thread). Invariant 5 records the
  expired-shutdown exception.

## Verification

Ruff, mypy, pytest (3.13 and 3.12), spec-lint, docs-lint and docstring-lint locally; every
regression pin and the two re-armed fork tests were run once against their named mutant.
