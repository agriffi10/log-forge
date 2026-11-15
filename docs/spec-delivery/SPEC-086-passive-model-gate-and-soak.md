# Completed Spec — SPEC-086: The Model Test's Passive Gate and Soak

## What was completed?

- **The time bound** (FR-001). `BOUND_SLACK` is gone from `tests/`: every call both halves of
  `tests/test_invariants_model.py` make is held to its own budget plus SPEC-085's `EPSILON`, and the
  state sampler's join has a constant of its own. `tests/conftest.py`'s summary hook prints a
  `model gate:` line from `model_oracles.gate_margin_line`, the worst margin over the gating runs.
- **The close count where one caller makes it exact** (FR-001). The passive recorders count a write
  at the first statement of the delivery method the library calls. The sequential half asserts
  `check_repeat` after every step and the new `check_late_write` after every `shutdown` and exit
  step. The concurrent gating runs keep their two existing close checks.
- **The two switches** (FR-002, FR-003). `LOG_FOUNDRY_MODEL_SEEDS` and `LOG_FOUNDRY_MODEL_SOAK` are
  parsed by `model_oracles.parse_seeds` / `parse_soak`, read through `seeds_from` / `soak_from`, and
  refused before collection by `tests/conftest.py`'s `pytest_configure` as a usage error. Test ids
  carry form and seed, and a failure ends with `rerun_command`'s command.
- **The soak** (FR-003). `test_soak_the_close_count_under_concurrency` is collected only when
  `LOG_FOUNDRY_MODEL_SOAK=1`. It wraps `_CloseRegistry.arm`, `_CloseRegistry.take` and
  `api._note_orphan_emit` by name, records `Arm` / `Take` / `Write` / `Close` events per sink, pairs
  each close with the earliest undischarged take (`take_of_each_close`), and judges by
  `check_unowed_repeat` and `check_late_write`. `test_the_soak_is_wired`, once per passive form
  at seed 2, and `test_a_name_the_soak_cannot_wrap_fails_it` run in every session, and
  `test_a_failure_ends_with_its_rerun` holds the rerun note.
- **The record** (FR-005). The two §12 items and three probes came with the Draft. The build adds
  the sink-change taker and its forced measurement to the body of SPEC-085's orphan-log item without
  touching its lead, and strikes the stale clauses of "The one generative test cannot find the
  lifecycle defects it is cited as guarding". The `Guarded:` lines of invariants 3 and 5 say what
  the model now asserts.
- **Deviations:** the spec's *Revision history* records the build's revision: the earliest-take
  pairing with `untaken`, the switch check in `conftest.py`, and FR-005 naming the stale item's
  correction. At `a4391701` the refused-switch test runs two nested sessions, one per variable, and
  `test_the_session_hook_refuses_every_bad_switch_value` holds eight more values in process.

## What changed from earlier specs?

- SPEC-085's `model_oracles` gains functions and types additively, and its summary hook prints a
  second line. Its pins, cells and `EPSILON` are unchanged.
- SPEC-085's orphan-log item in `docs/architecture.md` §12 gains a body paragraph. Its bold lead is
  byte for byte as it was.

## Verification

All of the runs below were on macOS, Python 3.13.1 and 10 cores. Loads are one-minute averages.
They were run at `acf5196e` unless a row says otherwise. Between that commit and the head,
`a4391701`, `src/` and `.github/` did not change. The tests changed in only two places: a red soak
item now raises its report instead of asserting it, and the refused-switch tests took their shape
at the head. `git diff --stat origin/main -- .github/ src/` is empty for the branch (FR-003).

**FR-001, the owed CI margin.** This is owed to the pull request's green run of the `CI` workflow.
Each leg must be at most 0.5 s.

| Leg | Run | Commit | Worst margin and test id |
|---|---|---|---|
| Python 3.12 | **OWED: fill in after CI** | **OWED** | **OWED** |
| Python 3.13 | **OWED: fill in after CI** | **OWED** | **OWED** |

These are the local readings. The whole suite at `a4391701` gave 10972 passed, 35 skipped and 21
xfailed, and its `model gate:` line read 0.031 s at `[deliver-seed15]`. At `acf5196e` the whole
suite gave 10968 passed, 35 skipped and 21 xfailed, at 0.006 s at `[http-seed14]`, and `-n 0` on
the model file read 0.008 s at `[seed0]`.

**FR-002, the gating set over seeds 0 to 999.** The command was
`LOG_FOUNDRY_MODEL_SEEDS=0-999 poetry run pytest tests/test_invariants_model.py -p no:cacheprovider -q`.
It ran green: exit 0, with **4007 passed** in 80.66 s at `-n 12`. That is 3,000 concurrent items,
1,000 sequential items and 7 others, at a load of 6.4 before and 11.8 after. The gate line read
0.108 s at `[http-seed764]`.

**FR-004, the parent of #339, `31dda162`.** The tree was exported with `git archive 31dda162 | tar -x`
into a scratch directory, and `tests/test_invariants_model.py` and `tests/model_oracles.py` at
`acf5196e` were copied over the export's `tests/`. The soak function was then run with
`LOG_FOUNDRY_MODEL_SOAK=1 python -I -B -m pytest -c <export>/pyproject.toml --rootdir <export>`.
`PYTHONPATH` and every `PYTEST_` and `LOG_FOUNDRY_MODEL_` variable were removed for the run. Every
failure message carried a `log_foundry.__file__` under the export's `src/`, so every run counts. In
the three invocations, **27, 30 and 30 of 48** items failed. Of the 24 items with no `shutdown()`
in their mix, **20, 23 and 23** failed at `CloseBroken:repeat@<n>`, against a floor of 12. Every red
item was a soak report at `repeat@<n>`, with no `late-write`, no `untaken` and no gating
assertion. One example is `http-seed6`, sink 2: two closes at count 1, 2.5 ms apart, with only
`arm(change) take` between the takes.

**FR-004, the head check.** The 48 default soak items were run ten times with
`LOG_FOUNDRY_MODEL_SOAK=1 poetry run pytest -p no:cacheprovider -q -rf 'tests/test_invariants_model.py::test_soak_the_close_count_under_concurrency'`.
**480 of 480 passed**, with no unowed repeat close and no late write. Each invocation took 2.97 to
3.58 s at loads of 9.7 to 10.7.

**FR-003, the soak of seeds 0 to 1999.** The command was the same with
`LOG_FOUNDRY_MODEL_SEEDS=0-1999`. It ran **6,000 soak items: 15 failed and 5985 passed** in
148.2 s, at a load of 10.5 before and 64.6 after. All 15 are soak reports, with
`log_foundry.__file__` under the worktree's `src/`. The soak does not hold the merge.

| Signature | Items |
|---|---|
| `late-write@0` | `deliver-seed322`, `deliver-seed470`, `deliver-seed1977`, `http-seed658`, `http-seed748` |
| `late-write@1` | `deliver-seed64`, `deliver-seed124`, `http-seed168`, `http-seed1658` |
| `late-write@2`, `@3`, `@6`, `@13` | `http-seed812`, `http-seed974`, `http-seed648`, `http-seed1158` |
| `repeat@0` | `deliver-seed502`: sink 1 had no writes and was closed twice by the runner thread, 2.3 ms apart, with `arm(change) take` before each close. Its stderr had "lost 1 submission(s); logged after shutdown(), which is terminal" |
| `repeat@1` | `http-seed578`: sink 1 had one write and was closed by two runner threads, 5.8 ms apart. The arms and takes were `arm(writer) take arm(change)` before the first close and `take` before the second |

These 13 late writes are the defect in SPEC-085's orphan-log item.

**Three unowed repeat closes.** The first diff review reproduced a third at `07362573`:
`deliver-seed470`, `CloseBroken:repeat@1`, about 1 in 650 runs over 13 seeds. Its record is
`arm(writer) write(Thread-1) take arm(change)` before the first close, then `take` before the
second, on `log-foundry-sink-close`, 5.4 ms later. All three are seeds with `shutdown()` in
their mix, and in each the only arm between the two takes is not a writer's. The record cannot
tell a swap's arm from `_get_worker`'s, because `arm(change)` stands for any arm made outside
`_note_orphan_emit`. None of them came back in the 480 head runs.

**"New" means not seen on the earlier library. It does not mean proven a regression.** `src/`
changed between the spec's measurements at `5447d26a` and these, made on the library of
`6bd0b66e`: 46 files, including SPEC-087's swap change (#374). The spec's evidence found no
unowed repeat in 6,900 runs on the earlier library. Nobody has traced these three, so whether one
is a defect, or which change brought it, is not known.

A session to trace all three is proposed for Andrew to start. It would instrument the taker and
the arm, bisect across `5447d26a..origin/main`, and file a §12 item with a probe for each one
that is a defect. Under FR-003 they did not hold the merge.

**FR-005, the three probes at the head**, each run with `.venv/bin/python -B` from
`docs/audits/2026-11-14-probes/lifecycle/`:

| Probe | Spec (`5447d26a`) | Head (`acf5196e`) |
|---|---|---|
| `p8_second_close_after_a_rearm.py` | 2 of 300, nothing on stderr | **0 of 300** at seed 1653 (a load near 16): disagrees |
| `p9_second_stop_in_fork_repair.py` | one stop returns; second `None` never; second finite 3.0 s; outside a fork 0.0 s | one stop 0.06 s, setter's calls `[0.0]`; second `None` still in the fork repair at 6.0 s; second finite 3.03 s, `[3.0]`; outside `[0.0]`: **matches row for row** |
| `p10_fork_with_stderr_lock_held.py` | 100, 100 and 0 of 100 (review 3: 100, 98, 0) | **85, 76 and 0 of 100** (`library`, `plain`, `quiet`), run beside the soaks: lower, and the same order |

**FR-005, the forced orphan-log write with a sink change as the taker.** This used SPEC-085's
`_orphan_write_after_close` with a displacing `configure(sink=…)` as the taker. It is a scratch
probe and is not committed. Each of seven runs was a fresh process with `PYTHONPATH=src`. Parked,
**7 of 7** gave `writes=2`, entry `[1]`, return `[1]` and nothing owed after the exit drain. The
unparked control gave 7 of 7 at `[2]` / `[2]`. Both match the spec.

**Mutation checks.** Each mutant ran in a fresh export of `acf5196e` with `-I`, except the last
row, which ran in the worktree at `a4391701`.

| Mutant | Selection | Result |
|---|---|---|
| `time.sleep(1.5)` at the top of `api.py`'s orphan branch | both gating tests | 72 of 72 red at `Stalled:info` / `Stalled:child-info` (own time 1.50 to 1.51 s, budget 0.000 s) |
| `release`'s inline path skips `close()` for a sink already closed once | sequential driver | 17 of 24 red: 16 at `CloseBroken:late-write@<n>`, and 1 at "every sink written to is closed" |
| the same mutant with the pre-spec model file (`886b303c`) | sequential driver | 24 of 24 green: the old checks cannot see it |
| `release`'s inline path calls `close()` twice | sequential driver | 24 of 24 red at `CloseBroken:repeat@<n>` |
| the soak's `_note_orphan_emit` wrapper never marks the thread | `test_the_soak_is_wired` | red, recording `Arm(by_writer=False)` |
| `self.record.wrote(self)` deleted from `DeliverRecorder.deliver` and `HTTPRecorder.deliver` (in the worktree, restored by copy) | `test_the_soak_is_wired` | `[deliver]` and `[http]` red, `[emit]` green. Before the wiring test took the three forms, the first diff review measured this mutant at 249 passed, and 48 of 48 soak items passed |
| `_rerun_on_failure`'s `add_note` removed (in the worktree, restored by copy) | `test_a_failure_ends_with_its_rerun` | 3 of 3 red |
| `pytest_configure` swallows the `ValueError` | the refused-switch test and the session-hook test | at `a4391701`, 10 of 10 red; each nested session read `63 passed, 12 errors`, exit 1 |

**Test ids, collected by the runner.** The ids were collected with `-n 0 --collect-only` from
exports of `886b303c` and `a4391701`. Base has 10,968 ids and head has 11,028. After the model's
renamed ids are mapped (`[3-deliver]` to `[deliver-seed3]`), **0 are removed and 60 are added**:
four soak tests in the model file and 56 in `test_model_oracles.py`.
