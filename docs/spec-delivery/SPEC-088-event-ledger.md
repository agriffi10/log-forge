# Completed Spec — SPEC-088: One Event Ledger: Every Accepted Event Is in Exactly One Term

## What was completed?

- **The ledger** (FR-001, FR-005):
  - `src/log_foundry/_ledger.py`, a new leaf, gives each thread its own counter cell. A
    `weakref.finalize` retires the cell when the thread's local storage is freed. The cells sit
    behind a re-entrant registration lock.
  - The worker's terms are in `worker.py`. `_CountingQueue._put` counts the events of every item
    the drain will take. The drain thread is the only writer of its taking count, its `in_flight`
    and its exits.
  - `worker._read_ledger` is the one read order (`docs/decisions/pipeline/`).
  - The credits and debits sit at the accept sites in `decorator.py` and `api.py`.
- **Six accounting defects closed** (FR-002). Each is held by a test in
  `tests/test_event_ledger_defects.py`. The probes' readings before and after are quoted in
  `architecture.md` §12's *Resolved* items.
- **The check** (FR-003). `_lifecycle._check_ledger` runs at the end of every `shutdown()`. It
  writes one line for each distinct `unaccounted` and never raises.
- **The surface** (FR-004):
  - `Health.events` and `EventLedger`, which is exported.
  - The private `_lifecycle._ledger_snapshot()`, for SPEC-089.
  - A forked child's ledger restarts in `_restart_ledger_after_fork` (FR-005).
- **The fixture** (FR-006):
  - The autouse `_ledger_balance` fixture in `tests/conftest.py` calls `tests/ledger_check.py`.
    The `ledger_exempt` marker is strict.
  - The write-site gate is `tests/test_ledger_write_sites.py`.
  - The fixture's corpus is `tests/test_ledger_check.py`.
  - The shared sinks are in `tests/ledger_sinks.py`.
- **Deviations.** Each one is revised into the spec and struck in place there:
  - The read order is two passes, with the worker's terms read first.
  - The worker's `in_flight` is measured, and `unaccounted` also counts a drain residual.
  - The registration lock is an `RLock` over an immutable pair, replaced by compare-and-set.
  - The expiry clause is shortened to fit `_diag`'s bound.
  - FR-006's two readings of an open span were both run, and the stricter one was built.
  - The counting `_put` appends to the deque itself instead of calling `super()._put`. With the
    `super()` call it measured 48 to 55 ns, which crosses FR-005's 50 ns bound.

## Measured on the final branch

Every number below and every probe reading in `architecture.md` §12 was measured on
`74940965`. Its `src/` is the code as merged, because the later commits on the branch touch only
docs.

- **The probes.** p02, p03 2000, p04, p08, p11, p14, p15, p16 and p17 were re-run. Each prints the
  reading quoted in §12. p03 is a race whose counts move between runs, and §12 quotes this
  head's run.
- **p18, run with the `nats` extra.** Its batch of 3 lands in `batch_lost` and in no other term.
  The sink's `failed` of 9 is its per-attempt count in `health().sink`, outside the ledger. So the
  `NATSSink` §12 item is struck.
- **The cost**, measured with `docs/audits/2026-11-14-probes/core-pipeline/spec088_cost.py` by the
  spec's Evidence method. Each figure is the minimum over the runs, taken under load. The traced
  call's figure is a sum of the isolated writes, not measured as one.

| Write | 3.12.12 | 3.13.1 | Bound |
|---|---|---|---|
| One credit through `_ledger.cell()` | 35.0 ns | 30.2 ns | under 50 ns |
| The counting `_put`, over a plain one | 26.9 ns | 31.2 ns | under 50 ns |
| A traced call's writes: two credits, one hand-over, one put | 131.9 ns | 121.8 ns | under 200 ns |
| One snapshot with 1, 9 and 50 live cells | 3.25, 4.18 and 8.35 µs | 2.70, 3.52 and 7.47 µs | under 12 µs at 9 |

## FR-006: two readings that differ, and the fixture's limit

- **Two of the seven exempt tests read differently from FR-006's table.** That table was
  measured on the spec's prototype at `ae0b7674`. No other test fails the fixture, and the other
  five read as the table says.

| Test | The table says | The branch reads |
|---|---|---|
| `test_synchronous_loss.py::test_the_increment_happens_under_the_lock` | `unaccounted` 1 | orphan residual -1, `unaccounted` 0 |
| `test_synchronous_loss.py::test_the_lock_is_a_module_global_the_fork_walk_can_reach` | `unaccounted` 2 | `unaccounted` 1, orphan residual -1 |

  Both tests call `_note_orphan_loss` with nothing credited, and the second also calls
  `_note_in_span_loss`. By FR-001, a negative orphan residual is for the fixture to catch, never
  the shutdown check.
- **The fixture checks only the workers a test still holds at teardown.**
  `ledger_check._tracked` is a `WeakSet`. Holding the workers strongly was tried in the all-extras
  environment, and it failed exactly 34 worker-path cases of
  `test_sinks_nats_engine_matrix.py`. Their harness hands `Worker._emit` a batch that no queue took
  in. The marker is strict, so a marker on the whole function would wrongly fail those functions'
  `emit`-path cases, which balance. So the weak tracking stays. Either of two changes would close
  the gap: strong tracking with a marker per case, or a harness that sends its batch through the
  queue.

## Rosters

- `_fork._GLOBAL_PRIMITIVES` gains `_ledger._registry.lock`, which makes ten.
- `test_lifecycle_inventory.py`:
  - `_ledger.py` joins `FILES`, which makes seven modules.
  - `OUTSIDE_FLOOR` rises from 9 to 10.
  - The new `Worker` and `Runtime` fields get rows.
- `test_runtime_holder.py` gains one lock site: `_restart_ledger_after_fork` takes `_loss_lock`.
- `test_public_surface.py` counts 36 exports.
- `test_worker_predicate_roster.py` gains two `EXISTENCE` rows, in `_worker_health` and
  `_check_ledger`. Its `_SITE_FLOOR` for `_lifecycle` is now 96, which is the derived count. It
  was 92 on `main`, while the branch point `9d05e793` already read 94.

## What changed from earlier specs?

- SPEC-050 FR-003, fourth criterion: a failed close now counts its buffer, plus `span.end` when
  `span.end` was never appended.
- SPEC-030: `submitted_after_shutdown` counts a late submission only once its put has succeeded.
- SPEC-019: the line a dying worker writes now names the events it held, and those events are
  counted in `drain_lost`.
- SPEC-062: the expired-shutdown line names `health().events.in_flight`.
- SPEC-039: a forked child's `orphan_lost` and `in_span_lost` start at zero.
- SPEC-036 FR-004: a log made under an abandoned span keeps that span's ids.

## Verification

- **Mutation runs.** Each mutant was restored by copy, and each one the spec names was caught:
  - Reading the hand-overs after the span credits reads `unaccounted=1`.
  - Reading the worker's exits after the span credits makes `accepted` read below the exits and
    losses.
  - A plain `Lock` as the registration lock makes a log inside it wait on its own thread.
  - With no restart in a forked child, the three fork tests fail.
  - Breaking any one of the write-site gate's eleven checks fails at least one case.
  - Breaking any one of the fixture's twelve checks fails at least one case.
- **FR-006's third criterion**, each mutant run against the whole suite:

| Removed | What failed |
|---|---|
| `_terminal_failure`'s debit | 34 tests, naming `unaccounted` |
| The open guard's debit | 11 tests, naming `open_spans` |
| The orphan guard's `BaseException` debit | `test_a_keyboardinterrupt_from_the_sink_still_propagates`, naming `unaccounted` |

- **Test ids.** The ids that `pytest --collect-only -q` collects were compared against
  `9d05e793`. One id left, by a rename derived from a line number.
- **Earlier runs, on the pre-rebase tree `5232c410`:**
  - the all-extras suite: 11,070 passed;
  - `tests/integration` against the nine services: 34 passed.
