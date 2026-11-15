---
type: Spec
id: SPEC-086
title: "The Model Test's Passive Gate and Soak: A One-Second Time Bound, an Exact Close Count Where One Caller Makes It Exact, and the Same Count Under Concurrency on Request"
description: "Tightens the generative model test for the passive sink it runs today: every call is held to its timeout plus one second, a sink's closes are counted against its writes where a single caller makes that exact, seeds are named and widened from the environment, and the same close count runs under concurrency as an on-request soak that reads the library's own arms and takes to tell a second close it owes from one it does not."
spec_status: In Progress
depends_on: [SPEC-077, SPEC-085]
arc: audit-2026-11-14
arc_order: 2
---

## Overview

`tests/test_invariants_model.py` is the repo's one generative test. Its concurrent half runs
seeded mixes of public calls on four threads and its sequential half runs seeded steps on one,
both against a passive sink. Each call is held to its timeout plus ten seconds, so a four-second
overrun passes, and a sink's closes are checked only for "none" and "overlapping", so a sink
closed twice with nothing written between passes too; the model was green at the parent of the
fix for exactly that. This spec tightens both, and is careful about what may gate a pull
request. The one-second bound gates everywhere. The exact close count gates in the sequential
half, where one caller makes it exact. Under four callers a second close with no write between
is sometimes owed, and the sink alone cannot tell which, so there the count is a soak run on
request that also reads the library's own record of what it armed and took. The spec also files
two defects its authoring found.

It does not drive the model with SPEC-085's adversarial sinks. That was drafted here twice and
is specified against SPEC-085's built modules; `docs/specs/ARCS.md` says why.

## Scope

### In Scope

- The gating model: the time bound in both halves, the close count in the sequential half
  (FR-001).
- A seed named in every test id, and a seed range read from the environment (FR-002).
- The soak: the close count in the concurrent half, on request, not gating (FR-003).
- Proof that the soak's close count bites at the parent of #339 (FR-004).
- Two open items in `docs/architecture.md` §12 with their probes, added by the change that
  carries this spec, and one extension the build makes to an item SPEC-085's build files
  (FR-005).

### Out of Scope

- **Any change under `src/`**, any change under `.github/`, and any new dependency.
- **Any personality but the passive one.** No drawn script, no adversarial sink, no forked
  child beyond the model's existing fork step.
- **Gating on the soak, or scheduling it.** No workflow sets its switch.
- **SPEC-085's overlap exemption.** SPEC-085 exempts a cell in which two of the model's own
  `configure` calls overlapped. Under four callers such calls overlap constantly, and a spec
  review measured that exemption hiding the repeat close in 24 of 24 runs at the parent of #339.
  The soak's rule is FR-003's and exempts no run.
- **Fixing or pinning what the soak finds.**

**What this spec uses from SPEC-085**, as built and unchanged unless the Data Model names an
addition: in `tests/model_oracles.py`, `EPSILON`, `check_call` (with no sink-code intervals),
`Closes`, `check_repeat` and the `CloseBroken` class; `tests/test_model_oracles.py`, to which it
adds corpora; the session hook in `tests/conftest.py`, to which it adds one line; and the
orphan-log open item SPEC-085's build files, which this spec's build extends without changing
its lead.

---

## Functional Requirements

Line numbers are `tests/test_invariants_model.py` at `5447d26a`. *Evidence* at the end holds
every measurement, each attributed; a criterion that is a rate carries its run count.

### FR-001: The gate: the time bound in both halves, the close count where one caller makes it exact

#### Description:

**The gating set** is what runs in every `pytest` invocation, as today: the three passive forms
(`emit`, `deliver`, `http`) of the concurrent half on seeds 0 to 15, and the sequential half on
seeds 0 to 23. Only this default range gates; a wider range is a measurement someone asked for.
Every assertion those runs make today stays.

**The time bound.** `BOUND_SLACK = 10.0` (`:103`) is added to every budget the model asserts
(`:615-618`, `:895`, `:1097-1098`). It is replaced by `elapsed ≤ budget + EPSILON`, with
SPEC-085's `EPSILON` of 1.0 s. The budget is the call's timeout, `DEFAULT_SWAP_TIMEOUT` for a
sink-changing `configure`, `DEFAULT_SHUTDOWN_TIMEOUT` for the exit drain, and zero for `info`
and a traced call, as `_budget` (`:823`) already says. `BOUND_SLACK` is also the timeout of the
state sampler's thread join (`:500`); that is not a bound on the library and gets a constant of
its own.

**The close count, in the sequential half.** Every recorder counts the delivery calls that
enter it, at the first statement of the method the library calls (`deliver` for the `deliver`
and `http` forms, `emit` for the `emit` form), and records that count at each `close`'s entry
and return. Counted later, a delivery already inside the sink when a close begins reads as a
write after it. Two rules, beside today's close checks (`:1039-1046`):

- **Repeat close**, after every step: no close of a sink enters at the same count as the close
  before it. This is SPEC-085's `check_repeat`.
- **Late write**, after every `shutdown` and `exit` step: no sink's count is above its count
  when its last close returned.

The late-write rule is this spec's own and is named apart from SPEC-085's released rule, which
reads the count at a close's entry after an exit drain. It is read only after a step that
closes, because a log made after a shutdown leaves its sink written and owed until the next
one, which invariant 5 records as correct. A passive recorder's `close` writes nothing, so that
is the whole of the rule here.

**Why the sink alone is enough here and not under four callers.** A second close with no write
between is owed when a writer armed the sink again after the first close was taken, and its
write entered before that close ran. With one caller nothing can arm a sink between a take and
its close, so the sink's own record is exact. With four it happens: FR-003 measures it and says
what the soak reads instead. The gating concurrent runs keep today's two close checks.

**The model reports its margin.** The session line SPEC-085 adds covers its cells. This spec
adds a line for the gating model: the worst `elapsed − budget` over its passing runs.

#### Acceptance Criteria:

- [ ] The name `BOUND_SLACK` no longer appears under `tests/`, and every call both halves make
      in the gating set is held to `elapsed ≤ budget + EPSILON` with the budgets above
      (invariant 3).
- [ ] In the sequential half the repeat-close rule is asserted after every step and the
      late-write rule after every `shutdown` and `exit` step, with a write counted once at the
      first statement of the delivery method the library calls (invariant 5).
- [ ] The gating concurrent runs assert neither rule, and every assertion both halves make today
      is still made, on the orphan path and the worker path alike (invariant 6).
- [ ] The late-write rule is a pure function with a fixture corpus asserting silence for a sink
      closed unwritten, one written again after a close and closed again, and a write that
      entered before a close returned, and asserting `CloseBroken:late-write@<count>` for a write
      after the last close returned.
- [ ] The pytest session prints one line naming the worst margin over the gating model's passing
      runs and its test id, under `-n 12` as under `-n 0`.
- [ ] The module docstring says what the file now asserts and no longer says it is not a bound
      test, and the `Guarded:` lines of invariants 3 and 5 in `docs/invariants.md` name the
      one-second bound and the close count where each gates.
- [ ] **Owed until the pull request's green CI run:** the delivery doc records the worst margin
      that line printed on each Python leg of the `CI` workflow, and each is at most 0.5 s. The
      bound was measured on macOS only, so the body of the pull request that lands the bound
      names this criterion as owed. A leg above 0.5 s holds that merge: `EPSILON` is SPEC-085's
      constant and changes only by a revision of that spec, re-derived from the margin CI
      printed. The integration workflow also runs this gate on its own triggers; its margin is
      not part of this criterion.

### FR-002: A seed is named in its test id, and the range widens from the environment

#### Description:

The file's docstring (`:60-61`) tells a reader to soak by editing `SEEDS`. This FR makes the
range a setting. `LOG_FOUNDRY_MODEL_SEEDS` holds a comma-separated list of non-negative integers
and inclusive `a-b` ranges, such as `0-999` or `3,17,40-44`. Unset, the concurrent half runs
seeds 0 to 15 and the sequential half 0 to 23, as today (`:89`, `:623`). Set, both halves run
exactly the seeds it names. A value that does not parse, an empty one included, is a collection
error: a fallback would let a mistyped soak run the defaults and report green.

A concurrent seed replays its draw, the operations each thread makes, and not its schedule,
which is the operating system's. A sequential seed replays exactly.

#### Acceptance Criteria:

- [ ] Unset, the concurrent half runs seeds 0 to 15 and the sequential half seeds 0 to 23; set,
      both run exactly the seeds named.
- [ ] An empty or malformed value fails collection with a message naming the variable and the
      value, and no test runs.
- [ ] A concurrent test id carries its form and its seed and a sequential one its seed, and
      every failure ends with the one command that re-runs that item alone, with the variable
      set to that seed and `-n 0`; a concurrent failure also says the seed replays its draw and
      not its schedule.
- [ ] `test_the_seed_range_covers_every_cell` (`:742`) reads the default range whatever the
      variable holds.
- [ ] The gating set is run once with `LOG_FOUNDRY_MODEL_SEEDS=0-999` at the commit that lands,
      and the delivery doc records the command and the counts as a measurement. A red run there
      means a gating oracle is not an invariant, so it holds the merge until the owner has
      answered.
- [ ] This FR serves no invariant: it decides which runs exist, not what any of them asserts.

### FR-003: The soak: the close count under four callers, on request, not gating

#### Description:

`LOG_FOUNDRY_MODEL_SOAK=1` adds one test item for each passive form and each seed in the range.
It runs the concurrent half exactly as the gating item does, and after the final shutdown
judges every sink the run handed the library by the two rules below. Any other value, the empty
one included, is a collection error. Unset, no soak item is collected, so a bare `pytest` and CI
never run one, and no workflow sets the variable.

**What the sink cannot see.** Seed 1653 closes its first sink twice at one write count in about
one run in a hundred. Traced, every case is one sequence: an orphan `info()` arms the sink; a
sink change takes it; the same `info()` arms it again through `rearm_if_released`; its write
enters; the first close runs at count one; a second sink change takes the re-armed sink and
closes it at count one. That is one take for each close and a writer's arm between the takes,
which invariant 5 records as correct: "a sink written to after its close is owed a second
close" (SPEC-045). At the sink it is two closes at one count, the same as the double close #339
fixed. The difference is in who armed the sink between the two takes: there a writer, in #339 a
sink change deciding late.

**So the soak reads the library's record.** It wraps three private names for the length of the
run, and a rename of any of them fails the item instead of silencing it, because the wrapper is
installed by name: `_lifecycle._CloseRegistry.arm`, `_lifecycle._CloseRegistry.take`, and
`log_foundry.api._note_orphan_emit`, the orphan path's call that arms a sink for a write. An arm
recorded on a thread that is inside `_note_orphan_emit` is a **writer's arm**. A close's take is
the earliest `take` of that sink recorded before the close entered that no earlier close has
discharged. Not the latest: a sink change can hand its close to a closer thread that has not
started when a `shutdown()` takes the re-armed sink and closes it inline, so two takes precede two
closes, the latest would serve both, and the writer's arm between them would be missed. A close
with no take left to discharge is reported as `CloseBroken:untaken@<count>`, a wiring failure the
wiring test also asserts, and is never judged against a wider window.

- **Unowed repeat close.** Two closes of a sink that entered at one write count are a finding
  unless a writer's arm of that sink was recorded between the first close's take and the
  second's. Its signature is `CloseBroken:repeat@<count>`.
- **Late write.** FR-001's rule, read once after the final shutdown.

A rule that excused any arm between the takes would excuse #339 itself, whose second close
follows a sink change's arm: at its parent that weaker rule fails 6 to 9 of the 48 default
items, and this one 28 to 30.

**Every seed is soaked.** An earlier draft left out seeds with `shutdown()` in the mix, where
the plain repeat rule fails one run in five. Each of those is a writer's arm between two takes,
most with a `shutdown()` as the first taker, and the rule above does not flag them.

**What a red soak item reports.** Its form and seed; the signature; that sink's record, the
write count at each close's entry and return with the closing thread's name, the milliseconds
between closes, and the arms and takes recorded between them; the lines the library wrote to
stderr; and the command that re-runs it.

**A red soak item is a finding, not a regression in the change under test.** The late-write
rule fails on today's library at a low rate, for the defect FR-005 hands to SPEC-085's item.
The unowed-repeat rule did not fail in any run measured at `5447d26a`.

**The soak cannot rot unseen.** One test collected in every run executes the soak's body for
one seed and asserts its wiring: the three wrappers were installed, arms, takes, writes and
closes were recorded, and the two rules were evaluated. It does not assert their verdict, so it
cannot fail at a race's rate.

#### Acceptance Criteria:

- [ ] With `LOG_FOUNDRY_MODEL_SOAK` unset, the model file's collection holds no soak item; with
      it set to `1`, one soak item exists per form and per seed in the range; any other value
      fails collection naming the variable.
- [ ] A soak item judges every sink the run handed the library by the two rules above, with no
      run and no sink exempt (invariant 5), for a sink the worker wrote to and one the orphan
      path wrote to (invariant 6).
- [ ] The unowed-repeat rule is a pure function over a recorded sequence of arms, takes, writes
      and closes, with a fixture corpus asserting silence for the traced sequence above and for
      the same with a `shutdown()` as the first taker, and asserting `CloseBroken:repeat@<count>`
      for two closes with no arm between their takes and for two closes with only a sink
      change's arm between them.
- [ ] A soak item fails, and does not pass, when any of the three private names cannot be
      wrapped.
- [ ] A red soak item's message carries its form, seed, signature, the failing sink's record as
      described, the library's stderr lines, and its re-run command.
- [ ] A test collected in every run executes the soak's body for one seed and asserts the wiring
      listed above, and nothing about the rules' verdict.
- [ ] `git diff --stat origin/main -- .github/ src/` is empty for the branch.
- [ ] The delivery doc records one soak of seeds 0 to 1999 at the commit that lands: the
      command, the number of soak items, and each red item's message, as a measurement that does
      not hold the merge.

### FR-004: The soak's close count fails at the parent of #339

#### Description:

#339 (`624d817f`) serialized concurrent `configure(sink=…)` calls after a review measured a sink
closed twice with nothing written between. The model test was green at its parent, `31dda162`.
The soak's unowed-repeat rule is not. The demonstration exports the parent's tree with
`git archive 31dda162 | tar -x -C <scratch>`, copies the model file and what it imports from
`tests/` over the export's `tests/`, and runs the soak items of the default seeds from that
directory. Each failure message carries `log_foundry.__file__`, and a run is void unless that
path is under the scratch directory: this repo has been caught by an editable install resolving
a copied tree back to the original. The three names the soak wraps exist at `31dda162`.

The defect is a race, so both halves of the criterion are counts of runs. If the parent does not
fail as stated, that is a finding: it is recorded with its measurement, the spec does not move
to `Completed`, and it goes to the owner.

#### Acceptance Criteria:

- [ ] At `31dda162`, of the 24 default-seed soak items whose mix has no `shutdown()` (eight
      seeds, three forms), at least 12 fail with `CloseBroken:repeat@<count>` from the
      unowed-repeat rule (invariant 5).
- [ ] At the branch head the 48 soak items of the default seeds, run ten times over, show no
      unowed repeat close; the delivery doc records the 480 runs as a measurement.
- [ ] Every failing run at the parent reports `log_foundry.__file__` under the scratch directory,
      and the delivery doc records the command, the counts and the signatures.

### FR-005: The findings are filed

#### Description:

Authoring this spec, and the reviews that built it, found defects no open item recorded. Two are
filed by the change that carries this spec, each as one bullet under `### Open` in
`docs/architecture.md` §12 with a probe that needs no test harness, beside the audit's own in
`docs/audits/2026-11-14-probes/lifecycle/`. Each probe's first line says it was written while
authoring this spec and is not the audit's.

| Item's lead | Probe | Measured at `5447d26a` |
|---|---|---|
| A second `shutdown()` made from a stop-signal setter during a forked child's fork repair waits on a drain thread the repair has not started | `p9_second_stop_in_fork_repair.py` | one stop returns; a second with `timeout=None` never does; a finite second takes its whole 3.0 s; outside a fork, 0.0 s |
| A child forked while another thread of the parent is writing to `sys.stderr` hangs on its first diagnostic | `p10_fork_with_stderr_lock_held.py` | 100 of 100 children hang; so do 100 of 100 that run no library code; 0 of 100 with no writer thread |

The second is the generic fork hazard reached through the library's own diagnostic, and its
item says so. Neither is pinned or fixed here.

**One thing that looked like a third is not a defect.** The double close at seed 1653 was first
filed as an open item and then traced, as FR-003 describes, to a writer's arm between two takes.
The item is withdrawn. Its probe stays, renamed `p8_second_close_after_a_rearm.py` and
documented as the trace of a behaviour invariant 5 records as correct.

**One finding is not filed here, because its item does not exist yet.** The soak's late write
(an orphan log delivered after a sink change's close returned, at seeds 519, 1335 and 1695 of
the `http` form) is the defect SPEC-085's build files as "An orphan log that passed its owed
check before a `shutdown()` took its sink writes after the close, and the sink is owed nothing",
with a sink change as the taker. Forced, it is the same: with the log parked after its owed
check, a displacing `configure(sink=…)` made and returned, and the log released, the sink has
two writes and one close entered at count one, and nothing is owed after a later `shutdown()`
and the exit drain. This spec's build adds that to the item's body, with its forced measurement
and the three seeds. It does not change the item's lead, which SPEC-085's `orphan-write-after-close`
pin carries.

**One item goes stale and is corrected in place.** §12's "The one generative test cannot find the
lifecycle defects it is cited as guarding" says the model's bound carries `BOUND_SLACK = 10.0` and
that its close oracle cannot see two closes with no write between them. The build strikes those
two clauses in that item's body with what the model now asserts, leaves its lead alone, and leaves
it open: its sink is still passive.

#### Acceptance Criteria:

- [ ] `docs/architecture.md` §12 carries the two items above under `### Open`, each naming its
      probe, its measurement, the invariant and what closes it, and the three probes exist at
      the paths given.
- [ ] The build runs each of the three probes against its own head and records the output in
      the delivery doc beside the output recorded here, whether or not they agree.
- [ ] The build extends the body of SPEC-085's orphan-log item in place as described, leaves its
      bold lead byte for byte as it was, and files no second item; SPEC-085's pin gates still
      pass.
- [ ] This FR serves no invariant: it is the record of the findings; the promises they break are
      named in the items.

---

## Data Model

```python
# tests/model_oracles.py: SPEC-085's module. Reused unchanged: EPSILON, check_call, Closes,
# check_repeat, CloseBroken. Added by this spec:

def check_late_write(closes: Closes) -> None: ...
#   raises CloseBroken("late-write@<count>") when closes.writes > the count at the last close's
#   return; silent for a sink with no close

@dataclass(frozen=True)
class Arm:
    by_writer: bool          # recorded on a thread inside api._note_orphan_emit

@dataclass(frozen=True)
class Take: ...

@dataclass(frozen=True)
class Write: ...

@dataclass(frozen=True)
class Close:
    count: int               # delivery calls entered when this close entered
    thread: str
    at: float

SinkEvent = Arm | Take | Write | Close

def check_unowed_repeat(events: Sequence[SinkEvent]) -> None: ...
#   one sink's events in the order recorded; raises CloseBroken("repeat@<count>") for two closes
#   at one count with no writer's arm between the first close's take and the second's
```

The corpora for both functions go in SPEC-085's `tests/test_model_oracles.py`.

## Configuration / Environment

| Variable | Unset | Set |
|---|---|---|
| `LOG_FOUNDRY_MODEL_SEEDS` | concurrent seeds 0 to 15, sequential 0 to 23 | a comma-separated list of integers and inclusive `a-b` ranges; both halves run exactly those |
| `LOG_FOUNDRY_MODEL_SOAK` | no soak item is collected | `1`: one soak item per form and seed, which does not gate |

Both are read at collection by the model file only, and validated before collection by
`tests/conftest.py`'s `pytest_configure`, through the model's own parsers, which raises
`pytest.UsageError` naming the variable and the value: under the default `-n 12` an import error in
the model file fails that file alone while every other file runs. Neither is a library setting,
and an empty or unrecognised value of either stops the session before any test, with exit code 4.

```bash
# Soak the gating set:
LOG_FOUNDRY_MODEL_SEEDS=0-999 poetry run pytest tests/test_invariants_model.py

# The close count under concurrency, on request:
LOG_FOUNDRY_MODEL_SOAK=1 LOG_FOUNDRY_MODEL_SEEDS=0-1999 poetry run pytest tests/test_invariants_model.py
```

## File & Folder Structure

```
tests/
├── conftest.py               # changed: the session hook SPEC-085 adds prints one more line
├── model_oracles.py          # changed, additively: check_late_write, check_unowed_repeat and
│                             #   the four event types
├── test_invariants_model.py  # changed: the bound, the close record and rules, the two switches,
│                             #   the soak items and their wiring test
└── test_model_oracles.py     # changed: the two rules' corpora, the switch-parsing tests
docs/
├── architecture.md           # changed by this spec's own change: two items in §12;
│                             #   by its build: the body of SPEC-085's orphan-log item
├── audits/2026-11-14-probes/ # changed by this spec's own change: three probes and a README note
├── invariants.md             # changed: the Guarded lines of 3 and 5 say what the model asserts
├── specs/ARCS.md             # changed by this spec's own change: why the adversarial run waits
└── spec-delivery/SPEC-086-passive-model-gate-and-soak.md   # new, at completion
```

## Implementation Phases

Both phases land green: the first changes only what gates and is measured an invariant there,
and the second adds to the default collection only a test that asserts wiring.

### Phase 1: The gate and the seed switch (FR-001, FR-002)

- Entry-counted writes and the close record on the three passive recorders; `check_late_write`
  with its corpus; the two rules asserted in the sequential half.
- `elapsed ≤ budget + EPSILON` over every call; `BOUND_SLACK` removed; the model's margin line.
- `LOG_FOUNDRY_MODEL_SEEDS`, the ids and the re-run command; the one soak of the gating set.
- The module docstring and the `Guarded:` lines of invariants 3 and 5.
- FR-001's owed margin criterion, named in this phase's pull request body and settled on its
  green run before the merge.

### Phase 2: The soak, its proof, and the record (FR-003, FR-004, FR-005)

- `check_unowed_repeat` with its corpus; the three wrappers; `LOG_FOUNDRY_MODEL_SOAK`, the soak
  items, their report and the wiring test.
- The demonstration at `31dda162`; the one soak of seeds 0 to 1999.
- The three probes re-run and recorded; the body of SPEC-085's orphan-log item extended.

## Evidence

Measured 2026-11-14 and 2026-11-15 on Python 3.13.1, macOS, 10 cores, against trees exported
with `git archive` and confirmed to be the tree imported. *Author* rows are this spec's
authoring prototypes, which are not in the repository; *review 1*, *2* and *3* are the spec
reviews' independent builds. The library's source is the same at `a0f44b5f` and `5447d26a`.

| What | Measurement |
|---|---|
| Slowest passive call | *Author:* 375 ms over twelve runs at load 33 to 45. *Review 1:* 0.155 s, no bound failure up to load 97. *Review 2:* 0.067 s |
| The gating set as this spec defines it | *Review 2:* 2,400 runs over seeds 0 to 199 with 0 red; green 3 of 3 at `-n 12`, 3 of 3 at `-n 0`, and beside load |
| Close rules, sequential half | *Author:* 0 of 300 seeds. *Review 1:* 0 of 1,432 |
| The double close at seed 1653, traced | *Review 3:* 3 hits in 1,200 runs. *Author:* 10 hits in 1,200 runs. All 13 are the sequence FR-003 gives: arm by an orphan log, take by a sink change, re-arm by the same log through `rearm_if_released`, write, close, take by a second sink change, close; nothing on stderr |
| Plain repeat rule (sink only), no `shutdown()` in the mix | *Review 1:* `1653-emit` 2 of 400, also `1653-deliver` and `235-emit`. *Review 2:* `1653-emit` 4 of 300. *Author:* 1 of 4,500 over the five seeds below |
| Plain repeat rule, `shutdown()` in the mix | *Author:* 238 of 1,200 runs of the default seeds |
| Unowed-repeat rule at `5447d26a` | *Author:* 0 of 4,500 runs (seeds 1653, 235, 519, 1335 and 1695, three forms, 300 runs each); 0 of 2,400 runs of the default seeds, 1,200 of them with `shutdown()` in the mix |
| Late-write rule at `5447d26a` | *Author:* 0 of those 4,500; 1 of the 2,400, on a seed with `shutdown()` in the mix. *Review 1:* at `519-http`, `1335-http` and `1695-http`, among 7 failures of either rule in about 15,300 runs. *Review 2:* 0 of 300 at each of those three |
| Unowed-repeat rule at `31dda162`, the 48 default soak items, three invocations | *Author:* 28, 30 and 28 fail; of the 24 with no `shutdown()` in the mix, 22, 24 and 22 |
| The weaker rule (any arm between the takes excuses) at `31dda162` | *Author:* 9, 7 and 6 of 48; of the 24, 7, 7 and 6 |
| The plain rule at `31dda162`, the 24 no-shutdown items | *Author:* 20 to 24. *Review 1:* 22 to 24. *Review 2:* 24, and 0 with SPEC-085's overlap exemption applied |
| Writes counted at the HTTP opener, not at `deliver` | *Author:* 113 of 1,000 `http` runs fail the late-write rule; counted at entry, none |
| The orphan-log write with a sink change as the taker, forced | *Author:* 7 of 7, control clean. *Review 2:* 8 of 8, control clean |
| Default collection | *Review 2:* no soak item with both switches unset; empty and malformed switch values exit 2 |
| The three probes | *Review 3:* `p9` matches row for row; `p10` 100, 98 and 0 of 100; `p8` 2 of 300, nothing on stderr |

## Revision history

- **2026-11-15, revised during the build** after the plan review. The soak pairs each close with
  the earliest undischarged take rather than the latest, and reports a close with none as
  `untaken`; the switches are validated in `tests/conftest.py`, because a collection error in one
  file does not stop a `-n 12` session (measured: 24 passed, 12 errors, exit 1); and FR-005 names
  the correction to the finding-21 item.

- **2026-11-14, cut from SPEC-085's first draft** after its review, as the generative half.
- **2026-11-15, replaced after two reviews of that half.** The adversarial generative run is out
  of this spec (`docs/specs/ARCS.md`). What stays is what independent builds measured: the
  passive gate, the switches, the soak and its proof. ~~`LOG_FOUNDRY_MODEL_SEEDS=0-999` is green
  for the passive forms~~ is struck: with the close count in the concurrent half it was a rate.
- **2026-11-15, revised after the third review**, which traced the double close at seed 1653.
  ~~Two sink changes, both serialized and neither announced, can close one sink twice~~ is
  withdrawn as an open item: it is a writer's re-arm between two takes, which invariant 5 records
  as correct. The soak's repeat rule now reads the library's arms and takes to tell that from an
  unowed second close, ~~seeds with `shutdown()` in the mix are not soaked~~ no longer holds
  since the rule does not flag them, and the extension to SPEC-085's orphan-log item leaves its
  lead alone.
