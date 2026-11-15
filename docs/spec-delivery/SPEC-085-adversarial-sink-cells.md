# Completed Spec — SPEC-085: Adversarial Sink Cells

## What was completed?

- **Four sink personalities** (FR-001) in `tests/model_sinks.py`: `reentrant`, `factory`, `waiter`
  and `forker`, built on one `Recorder` and driven by a `Hooks` pair (`script`, `new_sink`) that the
  arc's next spec replaces with drawn scripts. No library code changed.
- **A table of 637 cells** (504, 112, 9 and 12) generated in `tests/model_pins.py` and run by
  `tests/test_model_cells.py::test_cell`, each in a child made by `run_in_child` with a watchdog
  that turns a hang into `HangFound:<call>@<module>.<function>` with every thread's stack (FR-002).
- **The oracles** (FR-003) in `tests/model_oracles.py`: own time against budget plus `EPSILON`, a
  wait cut short, the three close rules, the ledger, the end state and a forked child's report, each
  a pure function held by a corpus in `tests/test_model_oracles.py`. `tests/conftest.py` prints the
  worst margin over passing cells at the end of a session.
- **Seven pins** (FR-004): 20 pinned cells, 175 withheld, 442 that must pass, and the
  orphan-write-after-close reproduction. `docs/architecture.md` §12 gains items (a) to (d) and the
  drain-thread item is corrected in place; `docs/invariants.md`'s *Guarded* lines for 3, 5 and 12
  name the cells.
- **Deviations:** none. The spec is unchanged since its plan-review revision.

## What changed from earlier specs?

- `tests/conftest.py` gains a `pytest_terminal_summary` hook, a thin caller of
  `model_oracles.margin_line`.
- §12's "`flush()` and `configure(sink=…)` called on the drain thread wait on themselves" now leads
  "`flush()` called on the drain thread waits on itself"; its `configure` half is struck through
  with the measurement beside it.

## Verification

Every run below was made against a `git archive` export of a commit, outside the repo, with
`python -m pytest -p no:cacheprovider` from the export. The build head is `c34c26d8`, whose `src/`
is byte-identical to `a0f44b5f` and `ae0b7674`. Every cell records the pytest process's and the
child's `log_foundry.__file__`, and in every run below each was under that run's export (a hung
child reports none). Loads are the one-minute average.

**FR-004, the gate at `c34c26d8`.** `test_model_cells.py::test_cell`: 442 passed, 20 expected
failures, each pinned cell at its pin's verdict; the whole file adds the reproduction (an expected
failure at `CloseBroken:unreleased@1`, 10 of 10 runs) and its passing control.

**FR-004, the `_swap_sink` mutant**, planted alone in an export of `c34c26d8`: `::test_cell` gives
8 failed, 434 passed, 20 expected failures. The eight are the named `reentrant-emit-worker-` cells:
`configure`, `configure+info`, `configure+shutdown`, `configure+shutdown-none`, `configure+span`
and `span+configure` at `CloseBroken:repeat@1`; `info+configure` at `CloseBroken:repeat@2`;
`configure+configure` at both `CloseBroken:repeat@0` and `CloseBroken:repeat@1`.

**FR-004, three register mutations** in `tests/model_pins.py`, each in a fresh export of `c34c26d8`:

| Mutation | Reddened |
|---|---|
| `reentrant-emit-orphan-info` removed from the generated table | `test_the_table_and_the_pins_hold_these_counts`, and both rows of `test_the_session_prints_the_worst_margin_line`, whose nested session selects that cell |
| `reentrant-close-worker-configure+info` removed from `orphan-arm-in-close` | `test_the_table_and_the_pins_hold_these_counts`, and the cell itself, now unpinned at `CloseBroken:never-closed@1` |
| `drain-flush`'s item misspelled | `test_every_pin_names_an_open_item` |

Four further mutations (a pin's verdict, the `strict` mark, a crash pin, `withheld()` ignoring the
sibling) each reddened the test that guards it.

**FR-004, the comparison with the spec.** No cell's verdict differed from the spec. On macOS the
table ran with no cell pinned or withheld: during Phase 2, 104 red of the 616 calling cells in
two runs, and at `c34c26d8` 105 red of 637, 0 differences each. On Linux under Python 3.12 and 3.13, which ran the
scheduled cells and not the withheld ones, the cells and oracle files gave 552 passed and 21
expected failures on each leg: the 20 pinned cells and the reproduction, each at its pin's verdict.

**FR-005, the parents of three fixes**, each with the four model files copied over the export's
`tests/` and its own `conftest.py` kept:

| Fix | Parent | Selection | Result |
|---|---|---|---|
| #337 | `641dfc86` | `tests/test_model_cells.py -k "waiter-"` | 2 failed, 10 passed: `waiter-emit-orphan-child-shutdown` and `waiter-emit-orphan-child-exit` at `WaitNotCut:wait`, served 4.005 s and 4.002 s past the stop; both worker-path `child-` cells pass |
| #341 | `b2528383` | `tests/test_model_cells.py -k "reentrant-setter-"` | 112 of 112 failed at `HangFound:<first operation>@_lifecycle.<function>`, 0 not as stated: `_note_orphan_emit` (`info`, 16), `_get_worker` (`span`, 16; `flush` and `flush-none` worker, 8 each), `_shutdown_worker` (`shutdown` and `shutdown-none`, 16 each), `_swap_sink` (`configure`, 16), `_flush_live_sink` (`flush` and `flush-none` orphan, 8 each) |
| #345 | `144f87c8` | `test_cell[factory-factory-orphan-span+flush-none]` and `[…span+shutdown-none]` | 2 failed: `HangFound:flush-none@worker.flush` and `HangFound:shutdown-none@worker.stop`, the `log-foundry-worker` thread inside `sinks._factory._make` in both |

All three were caught as stated. At `c34c26d8` all twelve `waiter` cells pass, none of the 112
`setter` cells hangs, and both #345 cells pass.

**FR-003, the worst margin over passing cells** (bound `MARGIN_REDERIVE`, 0.5 s): macOS, Python
3.13.1, 0.080 to 0.094 s in every run at `c34c26d8`; Linux in a container, 0.174 s and 0.077 s on
Python 3.12 and 0.075 s on Python 3.13. The two CI legs, Python 3.12 and Python 3.13, are owed: they
settle on the pull request's green CI run and are recorded here after it.

**Wall time** at `-n 12` on macOS: the cells file alone, 13 s in each of three runs at a load near
6. The whole suite, 86 s at `ae0b7674` (load 6.7 before) and 111 s at `c34c26d8` (load 38.4
before), one run each, back to back. Those two runs began at very different loads, so the 25 s
difference is an upper reading of the cost, not a measurement of it.
