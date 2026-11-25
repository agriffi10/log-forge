# Completed Spec — SPEC-099: A Nightly Soak of the Model Test, Known Findings Named by Their Open Item, New Ones Block the Queue Until Filed

## What was completed?

- **The rule and the record** (FR-001): `tests/model_oracles.py` gains `Note`, `Arm.by`, `Take.by`
  and `owed_between`. `check_unowed_repeat` now also treats a second close as owed when a log
  entered after the first take and wrote before the second. In `tests/test_invariants_model.py`
  the soak's wrappers record who armed and who took, and `_sink_record` prints those labels and
  ends with `after the last close: …`.
- **The known shapes** (FR-002): `tests/soak_known.py` is a clearing file. It holds the one `KNOWN`
  row, `orphan-write-after-close`, with its predicate, and `FILED_NIGHTS`. `tests/soak_verdict.py`
  holds the matching code (`signatures_of`, `explain`, `explain_run`, `reason`) and is not a
  clearing file. `tests/soak_ceilings.py` holds `CEILINGS` (21 for the one row) and
  `UNMEASURED_CEILING` (3). A soak item whose every finding is known ends `xfailed` with the
  `soak_known` property. `tests/test_soak_known.py` holds the predicate's corpus, the write-site
  roster, the open-item and ceiling checks, and `test_no_known_row_explains_shape_a`.
- **The workflow** (FR-003): `.github/workflows/model-soak.yml` runs every night at 09:37 UTC, on
  dispatch, and on a pull request that touches the paths that define the soak. It soaks a window of
  5,000 seeds chosen by the run number.
- **The report and the guard** (FR-004): `scripts/model-soak-report.py` has two commands. `seeds`
  picks the seeds and `report` writes the run's summary. The report exits 1 on a broken floor or
  ceiling and exits 0 on a new finding, since pytest's own exit is what makes that run red.
  `tests/test_model_soak_workflow.py` holds the workflow to its sixteen checks (each with a planted
  row, except the seeds rule, which its own tests hold), the report to its corpus, and the record
  to FR-005's phrase check.
- **The record** (FR-005): the build rewrote SPEC-086's working-rules decision in place, keeping its
  file name. It also rewrote invariant 5's *Guarded* line, §12's model item and CI item, and the
  model file's docstrings.
- **The queue** (FR-006): `scripts/pr-queue/queue.sh` reads the newest scheduled night's verdict
  from every attempt. It clears a night only by that attempt's `FILED_NIGHTS` entry on
  `refs/pr-queue/main`, which it fetches with `--refmap=`. `main-state` answers 4 while a night
  blocks, and `turn`, `acquire` and `pre-push` then admit only a clearing branch.
  `scripts/pr-queue-soak-test.sh` is the queue's corpus, with a stub `gh` that applies the
  caller's `--jq`. `PROTOCOL.md`, `operational-traps.md`, the supervise skill, the multi-agent
  briefing and `session-rhythm.md` describe the soak's `WAIT`.
- **Beyond the spec's letter:**
  - `_force_fenced` is extracted in `tests/test_shutdown_mid_swap.py`, so that
    `test_the_forced_fenced_choreography_records_the_fixed_record` records the live `fenced`
    record against the corpus copy.
  - A phrase-check row was added after the sweep found a bare phrase in a `SPEC-099` paragraph
    slipping through.

## What changed from earlier specs?

- **SPEC-086's decision**, *The model gates its close count only where one caller makes it exact,
  and soaks it under concurrency every night* (`docs/decisions/working-rules/`). The build reversed
  "set by no workflow" and partly reversed "gates nothing": the soak now gates a pull request that
  changes it, and a failed night stops the queue. SPEC-086's delivery doc is unedited.
- **SPEC-086's repeat rule** now treats more second closes as owed (FR-001). Every row SPEC-086's
  corpus held keeps its verdict.
- **The queue**: `main_green` gains state 4. `pr-queue-test.sh` and `install-test.sh` are now
  hermetic against a real `gh`, and their case names are unchanged.

## Verification

- **Gates at `da095631`:**
  - All exit 0: ruff, mypy, pytest (13311 passed, 48 skipped, 10 xfailed), spec-lint and its
    corpus, docs-lint, docstring-lint, `pr-queue-test.sh` (23 plants), `install-test.sh` (44 cases)
    and `pr-queue-soak-test.sh` (151 cases, 33 plants).
  - Python 3.12.12: 13310 passed with `PYTHONPATH=src`.
  - zizmor 1.32.0 `--offline` reports the same one low `self-repository` finding on this workflow
    as on `ci.yml`.
- **Mutation sweeps:** every mutant was reddened by a named row — Phase 1 29 of 29, Phase 2 13 of
  13 plus a permanent planted row per workflow check, Phase 3 33 shell plants, and Phase 4 4 of 4.
- **Measured runs:**
  - The workflow's soak step at seeds 0 to 99 ran 307 passed and 1 known; the report read
    `300 of 300` and `8 of 8` and exited 0.
  - Forks over the nightly selection: 0.
  - The live `fenced` test passed 20 of 20, both in parallel and serially under load.
  - The real `gh` read through `soak_verdict`'s lines against `integration.yml` printed
    `0 45246571727-1 2026-11-23T19:20:28Z`.
  - Phase 1 soak on macOS, seeds 0 to 999: 8, 3 and 2 known items at loads 34, 21 and 19, every one
    the orphan late write; no `repeat`, `untaken` or gating failure.
- **Owed:**
  - **Before the merge:** this pull request's own Model soak run, reading `0-99` and `300 of 300`.
  - **After the merge**, recorded by a docs-only follow-up pull request cut from fresh `main`:
    - `install.sh` re-run, and `queue.sh soak-verdict` printing its pinned form;
    - a no-input `workflow_dispatch` on `main`;
    - the first scheduled night within 26 hours, with its summary and the pytest step's duration;
    - the late-write ceiling re-derived from the Linux nights.
