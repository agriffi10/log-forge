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
  clearing file. `tests/soak_ceilings.py` holds `CEILINGS` (800 for the one row, re-derived from the real runner) and
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
- **The queue** (FR-006): `scripts/pr-queue/queue.sh` reads every scheduled night in its 30-run
  window, every attempt of a re-run one, and blocks while one of them failed and is not filed. It clears a night only by that attempt's `FILED_NIGHTS` entry on
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

- **Gates after the third code round** (the commit on top of `580f25e2`):
  - All exit 0: ruff, mypy, pytest (13343 passed, 48 skipped, 10 xfailed), spec-lint and its
    corpus, docs-lint, docstring-lint, `pr-queue-test.sh` (23 plants), `install-test.sh` (44 cases)
    and `pr-queue-soak-test.sh` (183 cases, 43 plants).
  - Python 3.12.12: 13342 passed with `PYTHONPATH=src`, which child interpreters need to import
    the uninstalled package.
  - zizmor 1.32.0 `--offline` reports the same one low `self-repository` finding on this workflow
    as on `ci.yml`.
- **Mutation sweeps:** every mutant was reddened by a named row — Phase 1 29 of 29, Phase 2 13 of
  13 plus a permanent planted row per workflow check, Phase 3 33 shell plants, and Phase 4 4 of 4.
  The code round added 4 of 4 Python mutants (a signature with no shape skipped in `explain`, the
  predicate reading only the last late write, the report's no-row exit dropped, every commit
  anchor removed from a ceiling) and 2 shell plants (the head's fields read space-separated, the
  clearing range read through porcelain `diff`), each reddened by its new row. The second code
  round added 5 of 5 Python mutants (a row of any kind honoured, the generic phrases read
  everywhere, and three reversions of machinery tests to the live table, each reddened by the
  future-edit check) and 6 shell plants (a newer green night clearing an older red one, a filed
  night ending the scan, a verdict-less night ending it, a one-attempt night read through the API,
  the gh calls unbounded, the open-PR remedy dropped), each reddened by its new case. The second
  review's reproductions re-run against the fix: a red night under a newer green one now blocks;
  a clearing pull request's three edits pass SPEC-099's tests whole, and the orphan item's closure
  fails only its row's five predicate rows; an open Dependabot PR's `WAIT` names the remedy; a
  planted `untaken` row explains nothing; an ordinary `turn` costs three `gh` calls, not five.
  The third code round made the future-edit check structural (the dict and the tuple found through
  the AST, two filings in turn, the check run on the filed copy behind a recursion guard; 2 of 2
  Python mutants, a literal anchor reintroduced and the module left out of the nested run), cut
  the fetches per queue call to one (2 shell plants: the cache line dropped, the verdict read in a
  subshell again), and validated the bound (1 shell plant). Frame 3's filing scenarios against it:
  one filing then a second, two filings at once, a filing with a row, the same with its ceiling,
  each passing SPEC-099's tests and the future-edit check; the closure failing exactly the live
  row's corpus and the closing test.
- **Measured runs:**
  - The workflow's soak step at seeds 0 to 99 ran 307 passed and 1 known; the report read
    `300 of 300` and `8 of 8` and exited 0.
  - Forks, counted by a `-p` plugin that registers `os.register_at_fork(before=...)`, at `-n 0`:
    0 over the nightly selection at seeds 0 to 99 (308 items), and 15 over its positive control,
    the sequential half's first four seeds (4 items). The commands:
    `LOG_FOUNDRY_MODEL_SOAK=1 LOG_FOUNDRY_MODEL_SEEDS=0-99 PYTHONPATH=<plugin dir> poetry run pytest -n 0 -p forkcount tests/test_invariants_model.py -k "<the workflow's -k>"`
    and `LOG_FOUNDRY_MODEL_SEEDS=0-3 PYTHONPATH=<plugin dir> poetry run pytest -n 0 -p forkcount tests/test_invariants_model.py -k test_the_sequential_driver_holds_the_state_table`.
  - The live `fenced` test passed 20 of 20, both in parallel and serially under load.
  - The real `gh` read through `soak_verdict`'s lines against `integration.yml` printed
    `0 45246571727-1 2026-11-23T19:12:53Z` (a one-attempt night's time is the run's `createdAt`,
    read from the list with no API call; `main-state` 0 through the same lines).
  - Phase 1 soak on macOS, seeds 0 to 999: 8, 3 and 2 known items at loads 34, 21 and 19, every one
    the orphan late write; no `repeat`, `untaken` or gating failure.
- **The real runner** (2026-11-25): the pull request's own `Model soak` job on `ubuntu-latest`,
  run 45561346799 at `12d4472a`, seeds 0 to 99, three attempts all green: 6, 3 and 7 known items of
  300 (every one `orphan-write-after-close`), 0 new, wiring 8 of 8, pytest step 12.0, 14.0 and
  18.4 s. That is about 1.8% of items against the Docker stand-in's 1 in 30,000, so a ceiling of 21
  would have stopped the queue every night; on Andrew's decision the ceiling is 800, from the most
  measured rate (350 a night: 700 under, 1,050 over).
- **Owed, now settled:**
  - **Before the merge:** this pull request's own Model soak run read `0-99` and `300 of 300`
    (run 45561346799, above).
  - **After the merge**, recorded by the follow-up pull request on 2026-11-26, all at `eee6737d` on
    `ubuntu-latest` (the spec's *Evidence*, *After the merge*, has the table):
    - `install.sh` re-run once `model-soak.yml` was on `main`; `queue.sh soak-verdict` printed
      `0 none`, its pinned form, and `main-state` 0. Installed any earlier, the queue's
      `gh run list --workflow model-soak.yml` answers HTTP 404, so every session reads 3 and waits;
    - the no-input dispatch, run 45625671047: seeds 15000 to 19999, `15000 of 15000`, 303 known,
      0 new, wiring 8 of 8, pytest step 749.9 s;
    - the first scheduled night, run 45793914793, created 09:38:53 UTC on 2026-11-26: seeds 20000
      to 24999, `15000 of 15000`, 62 known, 0 new, wiring 8 of 8, pytest step 415.1 s;
    - the ceiling of 800 holds: 303 and 62 are both under the 350 a night it is derived from;
    - `WINDOW` and `MAX_SEEDS` re-checked: the window cost 83 to 150 ms a seed, so `MAX_SEEDS` is
      re-derived from 10,000 to 5,000 and `WINDOW` stays 5,000, held to the same bound by
      `test_a_night_soaks_no_more_than_a_dispatch_may_ask_for`; the owed "at most 10 minutes",
      which the dispatch broke, now reads "fits the 1,020 s the dispatch bound is derived against".
