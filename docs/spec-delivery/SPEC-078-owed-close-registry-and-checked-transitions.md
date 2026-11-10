# Completed Spec — SPEC-078: The Owed-Close Registry and Checked Lifecycle Transitions

## What was completed?

- **One close registry** (FR-001). `_lifecycle._CloseRegistry`, held as `Runtime._closes`, holds
  every owed close and every close in flight. It replaces `_owed`, `_closing_now` and its lock,
  `_closers` and its lock, `_Closing`, `join_closers`, `closing_count` and `_close_owed`'s thread
  list.
  - `take` moves a sink from owed to in flight in one call.
  - Each in-flight `_Close` records its `_Rule` (`INLINE`, `JOINED` or `GRACED`) and its thread,
    and holds no sink.
  - The two leaf locks merged into one.
- **One grace per shutdown** (FR-002). Every graced wait of a call ends at its `_Operation`'s
  `grace_end`, which removed the double grace (4.00 s against 2.0 s at `fda134a`).
- **A stored, checked state** (FR-003, FR-004). `Runtime._current` is written only by `_move`, in
  four critical sections. `_column()` is the transition table's column.
  - The table is total over six calls by five states.
  - One cell changed: a swap during a drain with no live worker declines.
  - `CLOSED` keeps its arrows.
- **Operations** (FR-005). A `SWAP` or `STOP` builds one `_Operation` before its first critical
  section, and every slow step reads its deadline.
- **Tests** (FR-006). `tests/test_lifecycle_transitions.py` drives all 30 cells and adds a
  totality check, the stored-state and deadline AST scans with fixture corpora, and the stuck-emit
  test. The sequential model driver checks the stored state against its own derivation after
  every step.
- **Deviations, each amended in the spec:**
  - Only `_swap_sink` reads `_column()`, because the other five rows are the same code in every
    column (ruling a).
  - `_Operation` is built before the lock, so a lock wait does not eat the budget (ruling b).
  - `Worker.stop` keeps its signature. A claimed stop joins until the recorded `_exit_deadline`,
    and `timeout` only renders the expiry line (ruling B1).
  - `_Close` counts its participants, so a forwarded release cannot discharge a detached close
    still running. The plan review found this edge case.

## What changed from earlier specs?

- **SPEC-077's pins, changed on purpose:**
  - The token sets hold `_Operation`s.
  - `Runtime._FORK_SKIP` is `("_owned",)`, and `_CloseRegistry._FORK_SKIP` is `("owed",)`.
  - The inventory's pair floor fell from 11 to 10.
  - The lock-site literal was re-recorded.
  - `Runtime.state` reads `_current` instead of deriving.
  - The predicate roster's `_lifecycle` floor rose from 47 to 53.
- **A caller sees three changes:**
  - The second grace is gone.
  - A `configure(sink=…)` during a drain with no live worker only updates the config.
  - `log_foundry.shutdown`'s docstring states both shapes of a later span.
- **`Worker.stop`, when claimed**, ends its join at the claimed deadline rather than at a
  stop-local one.

## Verification

All of these exited 0 at the head:
- ruff, mypy, pytest, `spec-lint.sh`, `docs-lint.sh` and `docstring-lint.py`;
- the unit suite again with every extra installed;
- five repeat runs of the exit-budget and timing-floor tests.

Every base test name survives. The mutation set M1 to M10 is all killed, as are the edge-case
mutant, the `Worker.stop` mutant and every check of the new gates.
