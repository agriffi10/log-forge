# Completed Spec — SPEC-086: The Model Test's Passive Gate and Soak

## What was completed?

- **The time bound** (FR-001). `BOUND_SLACK` is gone from `tests/`: every call both halves of
  `tests/test_invariants_model.py` make is held to its own budget plus SPEC-085's `EPSILON`, and the
  state sampler's join has a constant of its own. `tests/conftest.py`'s summary hook prints a
  `model gate:` line from `model_oracles.gate_margin_line`, the worst margin over the gating runs.
  A bound failure names the machine's load average and CPU count, so starvation reads apart from
  a stall; nothing is skipped or widened under load.
- **The close count where one caller makes it exact** (FR-001). The passive recorders count a write
  at the first statement of the delivery method the library calls. The sequential half asserts
  `check_repeat` after every step and the new `check_late_write` after every `shutdown` and exit
  step. The concurrent gating runs keep their two existing close checks.
- **The two switches** (FR-002, FR-003). `LOG_FOUNDRY_MODEL_SEEDS` and `LOG_FOUNDRY_MODEL_SOAK` are
  parsed by `model_oracles.parse_seeds` / `parse_soak`, read through `seeds_from` / `soak_from`, and
  refused before collection by `tests/conftest.py`'s `pytest_configure` as a usage error. Test ids
  carry form and seed, and a failure ends with `rerun_command`'s command.
- **The soak** (FR-003). `test_soak_the_close_count_under_concurrency` is collected only when
  `LOG_FOUNDRY_MODEL_SOAK=1`. It wraps four names by name: `_CloseRegistry.arm`,
  `_CloseRegistry.take`, `api._note_orphan_emit` and `_lifecycle._get_worker`. It records `Arm` /
  `Take` / `Write` / `Close` events per sink, and pairs each close with the earliest undischarged
  take (`take_of_each_close`). It judges by `check_unowed_repeat` and `check_late_write`. A
  writer's arm is an arm of the sink an orphan-path call was made for, or any arm inside a worker
  build. Five tests run in every session:
  - `test_the_soak_is_wired`, once per passive form at seed 2, asserting only what the draw makes
    certain;
  - `test_the_soak_records_the_worker_path`, the deterministic build arm and drain-thread write;
  - `test_a_writers_arm_is_matched_to_its_sink`;
  - `test_a_name_the_soak_cannot_wrap_fails_it`, for the four names;
  - `test_a_failure_ends_with_its_rerun`.
- **The record** (FR-005). The two §12 items and three probes came with the Draft. The build adds
  the sink-change taker and its forced measurement to the body of SPEC-085's orphan-log item without
  touching its lead, and strikes the stale clauses of "The one generative test cannot find the
  lifecycle defects it is cited as guarding". The `Guarded:` lines of invariants 3 and 5 say what
  the model now asserts.
- **Deviations:** the spec's *Revision history* records the build's revision: the earliest-take
  pairing with `untaken`, the switch check in `conftest.py`, and FR-005 naming the stale item's
  correction. At `a4391701` the refused-switch test runs two nested sessions, one per variable, and
  `test_the_session_hook_refuses_every_bad_switch_value` holds eight more values in process. After
  the second diff review, the soak counts a worker build's arm as a writer's, and wraps
  `_get_worker` to see it. That is a fourth name, and the spec's FR-003 and Data Model say so.

## What changed from earlier specs?

- SPEC-085's `model_oracles` gains functions and types additively, and its summary hook prints a
  second line. Its pins, cells and `EPSILON` are unchanged.
- SPEC-085's orphan-log item in `docs/architecture.md` §12 gains a body paragraph. Its bold lead is
  byte for byte as it was.

## Verification

All of the runs below were on macOS, Python 3.13.1 and 10 cores. Loads are one-minute averages.
They were run at `acf5196e` unless a row says otherwise. That is the build commit on the
library of `6bd0b66e`, before the branch was rebased onto `bb6656ff`. The gating set and the
whole suite were run again at the rebased `8bc746c3`. Between `acf5196e` and the head, the branch
itself changed no file under `src/` or `.github/`. Its tests changed in four places:
- a red soak item raises its report instead of asserting it;
- the refused-switch tests took their final shape;
- the wiring test runs once per passive form;
- the rerun note has a test of its own.

`git diff --stat origin/main -- .github/ src/` is empty for the rebased branch (FR-003).

**FR-001, the CI margin.** Settled on the pull request's green run of the `CI` workflow: each leg
must be at most 0.5 s, and both are. Both legs printed 11357 passed, 38 skipped and 21 xfailed.

| Leg | Run | Commit | Worst margin and test id |
|---|---|---|---|
| Python 3.12 | `CI` run 43780951657, job `test (py3.12)` | `26f47e3b` | **0.016 s** at `[http-seed7]` |
| Python 3.13 | `CI` run 43780951657, job `test (py3.13)` | `26f47e3b` | **0.014 s** at `[deliver-seed0]` |

These are the local readings. The whole suite at `9af4dd60` gave **11356 passed, 39 skipped and 21
xfailed**, exit 0. Its gate line read 0.022 s at `[deliver-seed10]`, at a load near 150 from
another session. The whole suite at the rebased `8bc746c3` gave 11352 passed, 39
skipped and 21 xfailed, and its `model gate:` line read 0.009 s at `[http-seed12]`. At
`a4391701` it gave 10972 passed, 35 skipped and 21 xfailed, at 0.031 s at `[deliver-seed15]`. At
`acf5196e` the whole
suite gave 10968 passed, 35 skipped and 21 xfailed, at 0.006 s at `[http-seed14]`, and `-n 0` on
the model file read 0.008 s at `[seed0]`.

**FR-002, the gating set over seeds 0 to 999, at `9af4dd60`.** The command was
`LOG_FOUNDRY_MODEL_SEEDS=0-999 poetry run pytest tests/test_invariants_model.py -p no:cacheprovider -q`,
run alone by this session. Another session's test run was loading the machine at the time. It ran
green: exit 0, with **4016 passed** in 81.32 s at `-n 12`. That is 3,000 concurrent items, 1,000
sequential items and 16 others, at a load of 15.4 before and 12.2 after. The gate line read
0.043 s at `[deliver-seed707]`. The commits after `9af4dd60` change only this document.

At the rebased `8bc746c3`, before the second review's changes, the same command gave 4012 passed
in 91.33 s at loads of 11.5 to 19.6, with the gate line at 0.061 s at `[http-seed208]`. If `main` moves before the merge, the branch is rebased again inside the queue lock,
and the pull request says so.

Before the rebase, at `acf5196e`, the same command gave 4007 passed in 80.66 s (load 6.4 to 11.8),
with the gate line at 0.108 s at `[http-seed764]`.

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

### After the second diff review: the four-name rule, at `9af4dd60`

The rule changed, so the soak readings above were taken again. They describe the three-name
rule. The second review forced a worker-build arm between two takes with
`rev2/forced/build_arm.py`. The soak flagged `CloseBroken:repeat@2` 5 of 5 on the worker path,
while the orphan-path twin was silent 3 of 3. `_get_worker`'s build arms the configured sink for
the worker to write, which FR-003's own definition calls owed. So the soak now wraps
`_lifecycle._get_worker` by name, and counts any arm inside it as a writer's.

An orphan-path arm now counts only for the sink its `_note_orphan_emit` was called for. The
build's sink is resolved inside the build, where a wrapper cannot read it without racing a
concurrent `configure()`, so a build's arm is attributed by the thread being inside the build.
`decorator.py` calls `_lifecycle._get_worker()` as a module attribute and is its only caller, so
the patch intercepts every call. The name exists at `31dda162`, called the same way.

**Re-plants at `9af4dd60`.**
- `build_arm.py` on the worker path is **silent 5 of 5**. Its record is `arm(writer) write[MainThread]
  take arm(writer) write[log-foundry-worker] close@2 take close@2`. The orphan twin is silent 3 of 3.
- Two mutants were planted in the worktree and restored by copy:
  - `by_writer` ignoring the sink reddens `test_a_writers_arm_is_matched_to_its_sink`;
  - the build wrapper not marking its frame reddens `test_the_soak_records_the_worker_path`.
- Under 24 busy-loops (PIDs recorded, all killed, `pgrep` empty), at loads of 46 to 52:
  `test_the_soak_is_wired` and `test_the_soak_records_the_worker_path` were green 8 of 8, and the
  model file 3 of 3.

**FR-004 at `31dda162`, three invocations.** The model files of `9af4dd60` were copied over the
export, and every failure's `log_foundry.__file__` was under the export. **24, 30 and 30 of 48**
failed. Of the 24 items with no `shutdown()`, **20, 20 and 22** failed at `repeat@<n>`, against a
floor of 12. Every red item was a soak report at `repeat@<n>` only, at loads of 22 to 25.

**FR-004, the head check:** **480 of 480 passed** in ten invocations, at 3.9 to 4.9 s each and loads
of 20.5 to 23.8.

**FR-003, the soak of seeds 0 to 1999:** 6,000 items, **7 failed and 5993 passed** in 138.8 s, at
loads of 21.9 to 22.5.

| Signature | Items |
|---|---|
| `late-write@0` | `deliver-seed1368`, `http-seed436` |
| `late-write@1` | `deliver-seed406`, `http-seed686`, `http-seed1834` |
| `late-write@4` | `http-seed228` |
| `repeat@1` | **`deliver-seed962`**: sink 2, one write. `arm(writer) write(Thread-1549 (runner)) take`, then close 0 at 1 on that runner thread. Then `arm(change) take`, and close 1 at 1 on `log-foundry-sink-close`, 4.6 ms later. The stderr had "logged after shutdown(), which is terminal" |

**Which of the three known unowed repeats this rule excuses cannot be read from their records.**
`deliver-seed502`, `http-seed578` and `deliver-seed470` were recorded under the three-name rule.
In each, the only arm between the takes was `arm(change)`, which then meant any arm outside
`_note_orphan_emit`, so a build's arm and a swap's looked alike. This rule excuses each one if
its arm was a build's, and still flags it if the arm was a swap's. None of the three recurred in
this soak, and none in the 480 head runs.

**What this rule still flags is a swap's arm.** Under it, `arm(change)` in the parent can only be
a swap arming the sink it installs (`_lifecycle.py`, the two `_state._closes.arm(new_sink)`
calls). The fork paths arm only in a child. So `deliver-seed962` is a sink change arming its
installed sink after another caller had taken and closed it, followed by a second close at the
same count. That is the shape of #339's "sink change deciding late", and it is the strongest
candidate for the tracing session above. That session now covers four items: the three above,
and `deliver-seed962`.

> **Traced after the merge (2026-11-15).** `deliver-seed962` has the record of the defect probe
> `p11` forces, which `docs/architecture.md` §12 now holds: a sink change arms the sink a concurrent
> `shutdown()` took and closed. It is present at every tree measured, `31dda162` to `9d70f782`.
> It is not #339's double close: the take ahead of the swap's arm is a `shutdown()`'s, which
> #339's lock does not cover. That tracing session is done. The other three records cannot name their arm, and
> nothing further is owed on them.

**The bound under overload.** The second review ran the model file beside 40 busy-loops on this
10-core Mac, at loads of 81 to 112. **4 of 6 runs failed**, at `Stalled:info` or `Stalled:span`,
with own times of 1.03 to 1.35 s on a zero-budget call. The passing runs' worst margins were 0.82 to
0.97 s. At loads of 50 to 55 it was green **13 of 13**, with a worst margin of 0.462 s. In a Linux
container at `--cpus=2` it was green **4 of 4**, worst 0.091 s; the two logs kept here read 0.047 s
and 0.049 s.

**This disagrees with the spec's Evidence row** "no bound failure up to load 97": past a load of
about 80 a zero-budget call can be starved past one second. `EPSILON` is SPEC-085's constant and
is unchanged, and nothing is skipped under load. Instead, every bound failure now ends with the
load average and the CPU count, in the form `load average <1-min> / <5-min> / <15-min> on <n> CPUs`, so a reader
can tell starvation from a stall. `test_a_bound_failure_names_the_machines_load` holds that.

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
exports of the rebased `8bc746c3` and of `origin/main` at `bb6656ff`. Base has 11,347 ids and
head has 11,412. After the model's renamed ids are mapped (`[3-deliver]` to `[deliver-seed3]`,
and the bare wiring id to `[emit]`), **0 are removed and 65 are added**. Nine of them are in the
model file: the three wiring items, the three unwrappable-name items and the three rerun items.
The other 56 are in `test_model_oracles.py`.

The same diff against `886b303c` finds 2 removed and 446 added. Both removals, and every added id
outside the two files above, come from `main`'s #377 to #379.
The two removed ids are `test_numeric_door_roster.py` ids that carry `sinks/http.py` line
numbers, which #379 moved. The earlier diff, from exports of `886b303c` and `a4391701`, found 0
removed and 60 added: four soak tests in the model file and 56 in `test_model_oracles.py`.
