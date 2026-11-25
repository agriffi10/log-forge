---
type: Spec
id: SPEC-099
title: "A Nightly Soak of the Model Test: Known Findings Named by Their Open Item, New Ones Block the Queue Until Filed"
description: "Runs SPEC-086's soak of the model test's close count every night on main, reads a second close that a log entering after the first take owes as owed, names each red item whose shape an open item of architecture.md §12 already records as known by that item until the item closes or its nightly ceiling is passed, and turns the run red for anything else; a scheduled night that fails stops the PR queue until a pull request files that night, so that what the soak finds is seen and answered rather than learned to be ignored."
spec_status: In Progress
depends_on: [SPEC-085, SPEC-086, SPEC-097]
arc: audit-2026-11-14
arc_order: 15
---

## Overview

SPEC-086 built a soak of the model test: under four callers, it judges every sink for a second
close nobody owed and for a write after the last close. It runs only when someone sets
`LOG_FOUNDRY_MODEL_SOAK=1`, and no workflow sets it. On 2026-11-14 Andrew took a nightly soak of
the model test (`docs/architecture.md` §12, *CI cannot see a hang, a platform difference or an
untested line*). This spec is that soak. Run as it stands on `main` (`d9f2f097`), it would be red
nearly every night, and not for anything new. Over 12,000 soak items on macOS it was red on 6, on
seven sinks, and every finding was a write after the sink's last close on a caller's thread. By
its signature and thread that is an orphan log writing after its sink's close, an open item no
spec owns yet; whether each one meets that item's predicate cannot be read from today's record,
which drops what came after the last close (FR-001 adds it). At that rate a night's 15,000 items
would hold about seven or eight on macOS, and on a Linux stand-in about one in two nights. The
soak's rule also misreads a second close the library owes. When this spec was written at
`eda19f32` the soak was red for two more reasons, and both have since closed. SPEC-097 closed a
swap arming a sink that a `shutdown()` already closed, so a record of that shape is now either a
regression of SPEC-097 or the open item SPEC-097 left for a `fenced` retarget, and stays red
either way. #393 closed a gating oracle that had gone stale when SPEC-090 changed a stderr line,
which was this spec's start condition. A job that is red every
night for known reasons teaches people to ignore it. So this spec fixes the misreading. It names
each red item whose shape an open item records, by that item, while the item is open and the
shape stays under a nightly ceiling. It keeps the run red for anything else, and guards the job
against rotting unseen. On 2026-11-17 Andrew decided that a red night blocks merging. So a
scheduled night that fails stops the PR queue as a durable fact, and the way out is a pull request
that does nothing but file that night. The library does not change.

## Scope

### In Scope

- The soak's repeat rule reads a write whose log entered after the first close's take as owing the
  second close. The soak records which library function armed or took a sink, and prints what came
  after the last close (FR-001).
- A table of known soak shapes, each naming an open item of §12, one row at authoring, and their
  nightly ceilings in a file of their own. A red soak item whose every finding has a known shape is
  reported as known. A row whose item has left `### Open` fails the default suite (FR-002).
- `.github/workflows/model-soak.yml`: the soak every night on `main`, on request, and on a pull
  request that changes it (FR-003).
- The run's report, its floor and its ceilings, and a default-suite test that holds the workflow to
  what the soak needs (FR-004).
- The record: SPEC-086's decision rewritten in place, §12's items, invariant 5's *Guarded* line and
  the model file's docstring (FR-005).
- **The PR queue reads the newest scheduled night's verdict**, and lets through only a branch that
  files that night, in
  `scripts/pr-queue/queue.sh`, `scripts/pr-queue/pre-push` and `scripts/pr-queue/PROTOCOL.md`
  (FR-006). These are installed copies: the queue in use runs from the directory `install.sh`
  writes under `$HOME`, so the change reaches sessions only when `install.sh` is run again after the
  merge (`docs/process/several-agents.md`).

### Out of Scope

- **Any change under `src/`.** The library is unchanged. The soak reads it through the four names
  SPEC-086 wraps.
- **Fixing what the soak finds.** *An orphan log that passed its owed check before a `shutdown()`
  took its sink writes after the close, and the sink is owed nothing* has no spec: SPEC-097 left it
  open, and §12 names what would close it. It stays known here until its item closes.
- **A known `repeat` row at authoring, and at any time a row that explains Shape A's record,
  including one for *A `fenced` retarget whose stop's wait ran out re-arms the sink the stop
  closed, so it is closed a second time at the same write count*.** That item is open, but its
  record cannot be told from a regression of the swap item SPEC-097 closed (FR-002 gives the
  measurement). A night that meets either is red and is filed as FR-006 says, with no row.
- **The lost-line oracle.** When this spec was written, the gating model's `_WORKER_LOST` read
  neither of the two lines the exit drain has written since SPEC-090, and that failed a gating
  assertion about once in 2,000 runs (*Evidence*, at `eda19f32`). This spec's own change filed it
  as a §12 item and made its closure this spec's start condition, because the nightly run would
  otherwise have stopped the queue every morning. #393 (`4b7e4b86`) closed it: `_WORKER_LOST` is
  now built by `_worker_lost_shapes()` (`test_invariants_model.py:2290`, assigned at `:2341`), and
  the item is under *Resolved*. The start condition is met. FR-002 never excuses a gating
  assertion, so a gating failure in a soak item stays red.
- **The sequential half, and the gating items at wide seeds.** The nightly run collects the soak
  items and the soak's wiring tests, and nothing else. Every soak item makes every gating assertion
  of the concurrent half too, because it runs `_interleaving` as the gating item does. The
  sequential half forks, which the soak's forms do not (FR-003 measures it), and it replays
  exactly, so the same seed teaches nothing a second night.
- **A Python 3.13 leg.** The job runs on 3.12, the floor, as `integration.yml` and `pip-audit.yml`
  do, and `tests/test_python_version_roster.py` holds a single pin to the floor.
- **A macOS leg.** Declined by Andrew on 2026-11-14 and recorded as *CI runs on Linux only: a macOS
  leg was offered and declined*.
- **Coverage, `ResourceWarning` as an error, a property-based generator, and a required status
  check.** They are not his decisions, and nothing here needs them. Without a required check a
  merge is not gated by the night's run, which FR-006 states as a consequence.
- **Opening an issue, commenting, or posting anywhere else on a red run.** Andrew declined it on
  2026-11-17. The workflow is read-only.
- **SPEC-085's adversarial personalities in the soak.** The soak keeps SPEC-086's passive forms.
- **An alarm for a night with no run.** GitHub can drop a scheduled run under load, and disables a
  public repository's schedules after 60 days with no activity. Either way there is no run, so
  nothing goes red and nothing blocks. This repository merges most days, and the minute is off the
  hour, which is GitHub's advice against drops. `gh run list --workflow model-soak.yml --event
  schedule -L 3` shows the last nights. FR-003's owed criterion checks the first night only.

---

## Functional Requirements

Line numbers are at `d9f2f097`. *Evidence* holds every measurement with the commit it was made
at, and a rate carries its run count.

### FR-001: A write whose log entered after the first take owes the second close, and the record says who armed and took

#### Description:

`check_unowed_repeat` (`model_oracles.py:847`) excuses two closes at one write count only when a
writer's arm lies between the two closes' takes. Invariant 5 owes a sink a second close when it is
written after its first close was taken (SPEC-045). A writer's arm is how the soak reads that, but
it is not the only way. When something else re-armed the sink after the first take, the log finds
the sink already owed. It takes the fast path, arms nothing, and writes. The second close is owed
all the same, and the soak flags it. That is Shape B in the hand-over that traced the soak's
repeats, met unforced at `eda19f32` (*Evidence*). It can still happen at `d9f2f097`: SPEC-097 kept
the arm a sink change makes on a `fenced` or `unfenced` retarget (§12, its resolved swap item), so
something other than the log can still re-arm a sink after a take. The soak met no `repeat` at all
in 12,000 items there. The fix is to record the log itself. The soak's wrapper on
`api._note_orphan_emit` (defined in `_lifecycle`, called through `api`'s binding) adds a
`Note(thread)` for the sink at its entry, before the owed check. Every orphan write passes through
that call on its own thread first (`api.py:209-215`: `_note_orphan_emit(sink)`, then
`_engine.run_delivery(...)` or `sink.emit(...)`).

**The rule.** Two closes at one count are owed when, strictly between the first close's take and
the second's, there is a writer's arm, or a `Note` on a thread followed later by a `Write` on that
same thread. The second case is a log that entered after the first take and wrote before the
second. Nothing else between the takes owes the second close: not a `Note` alone, not a `Write`
whose thread has no `Note` before it in the stretch, and not a `Note` on one thread with a `Write`
on another.

**Not any write.** The hand-over suggested excusing any `Write` between the takes. That hides a
record whose second close only a sink change's arm made. When the writer passed its owed check
*before* the first take and wrote after it, the write is the race of the orphan-log item, and
nothing it did owes the second close. At `eda19f32` that record was Shape A, a sink change arming
a sink that a `shutdown()` had already taken and closed, and §12 recorded the boundary at
`deliver-seed600` in that swap item, which SPEC-097 has since closed. The authoring prototype
met it unforced twice, at `deliver-seed348` and `http-seed576`, and the any-write rule reads both as
owed (*Evidence*, at `eda19f32`). Met now, it is a regression of SPEC-097 or the open `fenced`
item, and either must be red.

**Who armed and took.** The soak's wrappers on `_CloseRegistry.arm` and `.take` record the name of
the library function that called each, as `Arm.by` and `Take.by`. The name is read from the
caller's frame. No known shape reads them since the swap row went (FR-002). They stay for the
record: a red night is filed by someone reading it, and the labels are what tell a `_close_owed`
take from a sink change's, which is #339's shape, and a log's arm from a worker build's. At
`d9f2f097` the arms are made by `_note_orphan_emit`, `rearm_if_released`, `_get_worker` and
`_swap_sink` (two sites), and by the fork paths (`_clear_after_fork`,
`_adopt_factory_after_fork`), which no soak form reaches. The takes are made by `_close_owed` and
`_swap_sink` (three sites).

**What came after.** `_sink_record` (`test_invariants_model.py:1121`) prints the arms, writes and
takes before each close, and drops everything after the last one. A late write is exactly what
comes after it. The record gains a last line, `after the last close: ...`. Each mark carries its
label: `arm(writer:_note_orphan_emit)`, `take(_close_owed)`, `note(<thread>)`.

#### Acceptance Criteria:

- [ ] `check_unowed_repeat` excuses two closes at one count exactly when the rule above holds
      strictly between their takes, and excuses nothing else (invariant 5).
- [ ] The rule's fixture corpus asserts silence for two records: the traced re-arm at seed 1653,
      and Shape B, where a sink change's arm precedes a log that enters, finds the sink owed and
      writes. It asserts `CloseBroken:repeat@<count>` for six records:
      - Shape A;
      - #339's shape;
      - the boundary, a `Note` before the first take and its `Write` after it;
      - (a) a log that enters between the takes and writes only after the second take;
      - (b) the boundary beside another thread's `Note` between the takes;
      - (c) the boundary followed by the same thread's next `Note`.

      Every row SPEC-086's corpus holds today keeps its verdict.
- [ ] Five mutants of the rule are each reddened by a named row:

      | Mutant | Reddened by |
      |---|---|
      | M1, any `Write` owes | the boundary, (b) and (c) |
      | M2, the `Note` clause dropped | Shape B |
      | M3, a `Note` and a `Write` on different threads owe | (b) |
      | M4, a `Note` alone owes | (a), (b) and (c) |
      | M5, a `Write` before its `Note` owes | (c) |
- ~~p11's `forced` choreography is flagged `repeat` in 7 of 7 runs~~: struck 2026-11-24, since
  SPEC-097 closed it and p11 no longer closes its sink twice (*Revision history*).
- [ ] Every `Arm` and `Take` the soak records carries the calling function's name.
      `test_the_soak_is_wired` and `test_the_soak_records_the_worker_path` assert the names their
      deterministic steps make: `_note_orphan_emit` for the coda's orphan log, preceded by its
      `Note`, and `_get_worker` for the build (invariant 6).
- [ ] `_sink_record` over the forced `fenced` record (FR-002) prints `note(MainThread)`,
      `arm(writer:_note_orphan_emit)`, `take(_close_owed)` and `arm(change:_swap_sink)`, and a
      mutant that drops the `by` label from the rendering reddens it.
- [ ] A red soak item's record ends with what was recorded after the sink's last close, or
      `nothing`.

### FR-002: A red soak item of a known shape is reported as known, by its open item, until the item closes or the shape passes its ceiling

#### Description:

`tests/soak_known.py` holds a table of shapes. Each row has:

- a name;
- the bold lead of one bullet under `### Open` in `docs/architecture.md` §12;
- the signature kind it explains;
- a predicate over one sink's events and close record.

Its nightly ceiling is held apart, in `tests/soak_ceilings.py` (below). One row exists at
authoring:

| Name | Kind | The predicate | Ceiling | §12 item |
|---|---|---|---|---|
| `orphan-write-after-close` | `late-write` | every write after the last close returned is on a thread not named `log-foundry-*`, whose last `Note` for the sink came before the sink's last take, and nothing armed the sink after that take | 21 | *An orphan log that passed its owed check before a `shutdown()` took its sink writes after the close, and the sink is owed nothing* |

**No `repeat` row.** At `eda19f32` a second row, `swap-arms-a-sink-a-shutdown-took`, named the swap
item *A sink a swap installs is closed twice with nothing written between when a concurrent
`shutdown()` took it before the swap decided*. SPEC-097 closed that item, so by the rule below the
row cannot exist, and a Shape A record is now a regression of SPEC-097 and red. The open item
nearest it, *A `fenced` retarget whose stop's wait ran out re-arms the sink the stop closed, so it
is closed a second time at the same write count*, gets no row either, for two measured reasons:
- **It cannot be told from that regression.** Recorded with the soak's event types, the
  choreography that
  `test_a_retarget_answering_fenced_keeps_its_arm_and_closes_the_sink_once_after_its_write`
  forces reads `note arm(writer:_note_orphan_emit) write take(_close_owed) | close@1
  arm(change:_swap_sink) take(_close_owed) | close@1` in 7 of 7 runs at `d9f2f097`. That is Shape
  A's record exactly, and the dropped row's predicate matched it 7 of 7. A row for it would read a
  regression of a closed item as known, which this table must never do.
- **It was not met unforced.** It needs a sink change that has already retargeted to stay
  undecided until a `shutdown()`'s wait for it runs out, up to `DEFAULT_CLOSER_GRACE`, 2 s. The
  soak met no `repeat` in 12,000 items at `d9f2f097`.

If a night meets it, the night is red, Andrew hears, and FR-006's clearing pull request files it
under its open item. With no row, the swap predicate, its corpus, its ceiling and its mutant go
too.

**No row may ever explain Shape A's record**, not only at authoring. Shape A's record is a pair of
closes at one count whose first take is `_close_owed`'s, with one or more `_swap_sink` arms and no
other arm between the two takes, and no `Note` followed by its `Write` there. A clearing pull
request that adds a `repeat` row for the `fenced` item would pass the open-item test, since that
item is open, and would read every later Shape A night as known. So
`test_no_known_row_explains_shape_a` runs `explain_run` against the live `KNOWN` over two fixed
records, the Shape A corpus record of FR-001 and the forced `fenced` record above, and fails
unless it returns `[]` for each. FR-006 files such a night under its item with no row.

**The late-write predicate rests on one fact about the library.** A write on a caller's thread can
be read as that thread's last `Note` only while every caller-thread write follows its own entry to
`_note_orphan_emit`. That holds today. Outside `sinks/`, the library hands a sink a batch in two
functions only:
- the orphan path's `_log` (`api.py:209-215`), which calls `_note_orphan_emit` first;
- the worker's `_emit` (`worker.py:3424`, `:3476`), which writes on `log-foundry-worker`.

So the case of a second log on the same thread that writes without its own `Note` cannot happen
today. A roster derived from the AST holds the set of call sites to those four. A fifth site fails
it, and must be read against this predicate before it lands.

**The verdict.** When a soak item's rules find something, every signature of every failing sink is
matched against the table. If every one has a shape, the item records the shapes' names as the
JUnit property `soak_known` and ends in `pytest.xfail`. The reason's first line is `known soak
finding: <name> (§12: <item>)` for each shape, and the record follows. Anything else fails:
- a signature with no shape;
- `untaken@<count>`;
- any gating assertion `_interleaving` makes;
- a run in which one sink's finding is known and another's is not.

The expected failure is not strict, because the soak measures a rate.

**The ceiling.** A known shape that turns much more frequent may be a new defect behind an old
name. So each row has a nightly ceiling. When one night's known items of a shape exceed it, the
report makes the run red and names the row and the count (FR-004).

The ceilings live in `tests/soak_ceilings.py`, not in either clearing file (FR-006). So a clearing
pull request cannot raise one:
- `CEILINGS` maps a row's name to its measured ceiling;
- a row it does not name takes `UNMEASURED_CEILING`, 3. A shape a clearing pull request has just
  filed has no rate yet. Three lets the night that filed it recur at a like count, and anything
  more needs an ordinary reviewed pull request that measures the shape and writes its ceiling.

A default-suite test fails:
- when a `CEILINGS` key names no row, so renaming a row cannot shed its measured ceiling;
- when an entry's line lacks a comment giving its measurement, the commit it was measured at, and
  "x2 under, x3 over".

Each ceiling is set from its shape's rate in one night of 15,000 items, so that a rate that
doubles stays under it and one that triples passes it, for every rate measured. The late-write
row has two macOS rates: 10 a night at `eda19f32` (24 in 36,000 items) and 7.5 a night at
`d9f2f097` (6 in 12,000). Its ceiling is 21. Doubled, 20 and 15 stay under it; tripled, 30 and 22.5
pass it. The comment beside the number gives both measurements and their commits, and the test
above holds it there.

On Linux the ceiling is loose. The stand-in met none in 18,000 items at `eda19f32`, and 1 in
30,000 at `d9f2f097`, about 0.5 a night, so only a rate some forty times that would pass 21 there.
The first scheduled nights on a real runner, which FR-003 already owes, are where its Linux rate is
read; a ceiling set from them is an ordinary reviewed pull request.

**A row's life.** A known item stays visible: it is counted and listed in every run's summary
(FR-004). It drops out only when its item closes. A default-suite test reads §12 with SPEC-085's
`open_item_leads()`, and fails when any row's item is not an open lead. The spec that resolves an
item therefore removes its row in the same change, and from then on that shape is red. A row is
added only for an item already filed under `### Open`, in the change that files it, which is
FR-006's clearing pull request. This is the rule SPEC-085's pins keep (*A red adversarial cell is
pinned to its open item at its exact verdict, and a pin moves only after the owner hears why*). For
a soak, the red night is how the owner hears.

#### Acceptance Criteria:

- [ ] A soak item whose every finding matches a row ends `xfailed`, with the `soak_known` property
      and a reason naming each shape and its §12 item. An item with any unmatched signature, any
      `untaken`, or any gating assertion failure ends `failed`, and so does a run with one known
      and one unknown sink (invariant 5).
- [ ] `test_no_known_row_explains_shape_a` passes against the live `KNOWN`: `explain_run` returns
      `[]` for the Shape A corpus record and for the forced `fenced` record, so either ends
      `failed` whatever rows a clearing pull request adds. Planting
      `Known("fenced-rearm", <the fenced item's lead>, "repeat")` with the dropped swap predicate
      reddens it, and the same row passes `test_every_known_shape_names_an_open_item`, which is
      why this test exists (invariant 5).
- [ ] The late-write predicate's corpus asserts a match for the orphan late write. It asserts no
      match for:
      - a late write on `log-foundry-worker` (invariant 6: a worker-path late write is never read
        as the orphan path's);
      - a `Note` after the last take;
      - an arm after the last take.
- [ ] `test_every_place_the_library_hands_a_sink_a_batch_is_known` derives, from the AST of every
      module directly under `src/log_foundry/`, the calls to `run_delivery`, `emit`, `deliver` and
      `emit_via_deliver`. It asserts they are exactly the four sites above. A planted
      `sink.emit([])` in another module fails it, naming that site.
- [ ] `test_every_known_shape_names_an_open_item` fails when a row's item is struck through or
      moved under `### Resolved`, shown by one mutant of each in a scratch copy of §12.
- [ ] `test_every_ceiling_names_a_row_and_carries_its_derivation` passes. It fails, naming the
      line, when an entry's comment loses its measurement, its commit or "x2 under, x3 over", and
      when a `CEILINGS` key names no row. A row `CEILINGS` does not name reads
      `UNMEASURED_CEILING`.

### FR-003: The soak runs every night on `main`, on request, and on a pull request that changes it

#### Description:

`.github/workflows/model-soak.yml`, one job, `soak`, on `ubuntu-latest`:

- **Triggers.**
  - `schedule`, `cron: "37 9 * * *"`: once a day at 09:37 UTC. That is off the hour, as GitHub
    advises, and after the Monday schedules of the other workflows (04:53, 05:23, 06:41 and 07:17
    UTC).
  - `workflow_dispatch`, with one optional input, `seeds`.
  - `pull_request`, filtered to the paths that define the soak or install what it runs:
    - the workflow;
    - `.github/actions/install-poetry/**`;
    - `scripts/model-soak-report.py`;
    - `tests/soak_known.py` and `tests/soak_ceilings.py`;
    - every module under `tests/` that `tests/test_invariants_model.py` and `tests/conftest.py`
      import, directly or through each other, the two included. At `d9f2f097`, before this spec's
      own imports, that is thirteen: `_reentry`, `conftest`, `ledger_check`, `model_oracles`,
      `model_pins`, `model_sinks`, `shadow_check`, `sink_rigs`, `test_fork_lifecycle`,
      `test_invariants_model`, `test_public_surface`, `test_sink_concurrency` and
      `test_sinks_http`. The workflow lists them by path, and FR-004's guard derives the set and
      holds the list to it, so an import added later cannot leave the filter short unseen;
    - `pyproject.toml` and `poetry.lock`.

    This trigger makes the soak gate a pull request that changes it, which partly reverses
    SPEC-086's "gates nothing" (FR-005). It is the only way the job body runs on a runner before
    the merge. A scheduled run fires only from the default branch, and a dispatch needs the
    workflow file there too (GitHub's *Events that trigger workflows*: "This event will only
    trigger a workflow run if the workflow file exists on the default branch").
- **Hardening**, as the other workflows do:
  - `permissions: contents: read`;
  - every third-party action pinned to the SHA `ci.yml` pins, with its `# vX.Y.Z` comment;
  - `persist-credentials: false`;
  - Python `"3.12"`;
  - every `${{ }}` expression either in the concurrency group or as a whole `env` value, never
    inside `run:`.
- **Concurrency.** The group is
  `model-soak-${{ github.event_name == 'pull_request' && github.ref || github.run_id }}`, with
  `cancel-in-progress: true`. Concurrency groups are repository-wide. `ci.yml`'s group is
  `ci-${{ ... }}`, so a literal copy would put a pull request's CI run and its soak run in one group,
  and each would cancel the other. The `model-soak-` prefix keeps them apart. A pull request's soak
  supersedes its own earlier soak. A scheduled or dispatched run keys on its own `run_id`, so no
  later dispatch can displace a pending scheduled run, and no night is lost to one.
- **Seeds.** `scripts/model-soak-report.py seeds --event <event> --asked <input> --run-number
  <github.run_number>` chooses them:
  - a dispatch's `seeds`. They are counted from the text first, each written range by its ends,
    and refused when the count passes `MAX_SEEDS`, before any set is built. An overlap counts
    twice, so the count is never below the seeds named. Then they are parsed by SPEC-086's
    `parse_seeds` and printed normalised as comma-separated ranges of digits and hyphens;
  - else `0-99` on a pull request;
  - else the run's window, `(run_number mod WINDOWS) × WINDOW` and the `WINDOW - 1` seeds after it.

  `github.run_number` "does not change if you re-run the workflow run" (GitHub's *Contexts
  reference*). So a re-run of a red night soaks the same seeds, and in any case FR-006 reads every
  attempt, so a re-run clears nothing. The run number counts every run of the workflow, pull
  requests' and dispatches' included, so the window rotates per run and not per night: a night
  after two dispatches soaks the window two places on. Nothing in the choice reads a clock. Consecutive runs take consecutive windows and wrap
  after `WINDOWS`, so the nights widen the draws the soak has met. A concurrent seed replays its
  draw and not its schedule, so a re-run is the same draw under a new schedule, not a fresh draw.
  The normalised output means a dispatch input carrying a newline, such as `"1,\n2"`, which
  `parse_seeds` accepts, cannot write a second line into `$GITHUB_OUTPUT`.
- **Budget.** `WINDOW` is 5,000 seeds, 15,000 soak items, and `WINDOWS` is 30, so the nights cover
  seeds 0 to 149,999 every 30 runs. On the 4-CPU Linux stand-in at `eda19f32`, a soak item cost
  18.7 ms of wall clock at the default `-n 12`, linearly from 3,000 to 15,000 items, and a window
  of 15,008 items took 281.6 s and 280.5 s in two runs. At `d9f2f097` the soak items of a window,
  15,000, took 307.6 s over seeds 0 to 4999 and 284.3 s over 5000 to 9999, at most 20.5 ms an item
  (*Evidence*). At `-n 4` the same items took three times as long at `eda19f32`, so the step stays
  a bare `pytest` and inherits `addopts`. `timeout-minutes: 30`.
- **The dispatch bound.** `MAX_SEEDS` is 10,000. Here is why, beside the number. The job has 1,800
  s. 180 s is allowed for the install, and 600 s for one hung item's guard and its dump, which leaves
  1,020 s for the soak. 10,000 seeds is 561 s at the stand-in's 56 ms a seed at `eda19f32`, and
  842 s at half again that. At `d9f2f097` its 61.5 ms a seed makes 615 s, and half again 923 s,
  both under 1,020 s. A dispatch never reaches the queue (FR-006). The bound keeps it inside the job's own
  timeout, so that it ends with its report rather than being killed with none.
- **The run.** The test step is:

  ```
  poetry run pytest tests/test_invariants_model.py \
    -k "test_soak_the_close_count_under_concurrency or test_the_soak_is_wired or test_the_soak_records_the_worker_path or test_a_name_the_soak_cannot_wrap_fails_it" \
    -p no:cacheprovider -rfEx --junitxml=soak.xml
  ```

  It runs with `LOG_FOUNDRY_MODEL_SOAK: "1"`, and with `LOG_FOUNDRY_MODEL_SEEDS` set from the seeds
  step. That selects the soak items and the **wiring set**:
  - `test_the_soak_is_wired[emit]`, `[deliver]` and `[http]`;
  - `test_the_soak_records_the_worker_path`;
  - the four rows of `test_a_name_the_soak_cannot_wrap_fails_it`.

  At `LOG_FOUNDRY_MODEL_SEEDS=0-1` that is 14 items. SPEC-086's broader `-k soak` collects 15,
  because one `test_a_failure_ends_with_its_rerun` row has `soak` in its id. Then a step with
  `if: always()` writes the report to the job summary (FR-004).
- **What red means.** The run is red when any of these happens:
  - the seeds step refuses its input;
  - pytest exits non-zero, which it does on a new finding, a gating failure in a soak item, a
    wiring item failing, or a hang the guard kills;
  - the report's floor or a ceiling is broken.

  It is green when every red soak item is known and under its ceiling. A red scheduled run
  notifies the user who last changed the workflow's `cron` line, which is GitHub's rule for
  scheduled workflows. Only a scheduled run's verdict reaches the queue (FR-006). A red dispatch
  or pull-request run is red on its own page and nowhere else.
- **Forks.** None of the collected items forks, so the faulthandler watchdog's fork hazard
  (`pyproject.toml`) does not reach the soak (*Evidence*). A new soak form that forks keeps to
  `run_in_child`'s rules.

#### Acceptance Criteria:

- [ ] The workflow exists with the three triggers, the path filter, the hardening, the concurrency
      group and the seeds rule above. Run against it, `zizmor --offline` 1.32.0 or later reports
      no finding that it does not also report against `ci.yml`.
- [ ] `seeds` returns `5000-9999` for run number 1 and the same for run number 31. It returns
      `0-99` for `pull_request`, and `1-2,4` for a dispatch of `"1,\n2, 4"`. It refuses a dispatch
      of 10,001 seeds, or of `"0-3\nfoo=bar"`, with exit 1 and no output, and accepts one of
      exactly 10,000. A mutant that takes the window from anything but the run number reddens a
      test. A dispatch of `0-300000` is refused with less than 1 MB traced, so the bound is read
      before the set is built, and a mutant that builds it first reddens that test.
- [ ] Collected with the workflow's own `-k` and `LOG_FOUNDRY_MODEL_SEEDS=0-1`, the selection is
      exactly 6 soak items and the eight items of the wiring set. Run over seeds 0 to 99 with a
      plugin counting forks through `os.register_at_fork`, it counts 0, where the same plugin
      counts at least one over the sequential half's first four seeds.
- [ ] `tests/test_python_version_roster.py` passes with the workflow in the tree.
- [ ] `timeout-minutes` is 30. The comments beside `WINDOW` and `MAX_SEEDS` give the measurements
      they rest on, anchored to a commit.
- [ ] **Owed until the pull request's own run:** the pull-request trigger runs the job on its
      branch, and its summary reads `0-99` and `300 of 300`. The pull request body names this
      criterion as owed until that run is green.
- [ ] **Owed after the merge:**
      - Within 26 hours of the merge, `gh run list --workflow model-soak.yml --event schedule
        --branch main` lists a completed run, and its summary reads `15000 of 15000`.
      - A `workflow_dispatch` run on `main` with no input is made the day of the merge, and its
        summary is recorded too.
      - Both are recorded in the delivery doc, with the pytest step's duration, which is at most
        10 minutes.

      A missing scheduled run is a finding for the owner, not a pass.
- [ ] This FR serves no invariant: it decides when the soak runs and where its verdict goes. What
      the soak asserts is FR-001's, FR-002's and SPEC-086's.

### FR-004: The run reports what it found, and cannot shrink, rot or hide a surge unseen

#### Description:

**The report.** `scripts/model-soak-report.py report soak.xml <seeds> --summary <path>` reads the
JUnit XML and writes Markdown to the job summary and to stdout. It writes the file itself rather
than through a pipe, because a pipe would hide its exit status under the runner's default
`bash -e`, which sets no `pipefail` (GitHub's *Workflow syntax*). The report carries:
- the seeds;
- the soak items run, against `3 × |seeds|` asked for;
- the clean, known and new counts;
- the wiring items passed, of eight;
- each known shape with its count, its ceiling and its §12 item;
- each new item with its first line and the command that re-runs it.

A known item's full record is in the XML and the log.

**The floor and the ceilings.** The report exits 1, and says why, in each of these cases:
- the soak items run are not exactly `3 × |seeds|`;
- a soak item was skipped;
- a soak item is an expected failure without both the `soak_known` property and a reason starting
  `known soak finding:`;
- an item of the wiring set did not run, or did not pass;
- a known shape's count exceeds its ceiling as `tests/soak_ceilings.py` reads it, or names no
  row.

An exit code alone is not a floor, as the working-rules decision on SPEC-041's job says. All of
these exit 0 from pytest over fewer items, or over none that judged anything:
- a `-k` that matches less;
- a collection that quietly drops the soak;
- a skip mark on the soak;
- an `xfail` marker added to it.

**The workflow guard.** `tests/test_model_soak_workflow.py`, collected in every run, reads
`.github/workflows/model-soak.yml` as text, as `test_python_version_roster.py` does, because no
YAML parser is a dependency. It asserts:
- the `cron:` line;
- `workflow_dispatch`;
- `LOG_FOUNDRY_MODEL_SOAK: "1"`;
- `RUN_NUMBER: ${{ github.run_number }}`;
- that every `${{ }}` is the concurrency group or a whole `env` value;
- the report step with `if: always()`, `soak.xml`, the seeds and `--summary`;
- the `model-soak-` group prefix;
- `permissions: contents: read` and nothing that writes;
- `persist-credentials: false`;
- every `uses:` pinned to a 40-character SHA with a `# vX.Y.Z` comment, or local;
- Python `"3.12"`;
- that the `pull_request` path filter names, as a literal path, every module of FR-003's derived
  set. The set is read from the AST: every `import` and `from ... import` with no leading dot,
  anywhere in `tests/test_invariants_model.py` and `tests/conftest.py`, whose top-level name is a
  module directly under `tests/`, followed through the modules it reaches.

It collects the workflow's own `-k` in a subprocess, under `LOG_FOUNDRY_MODEL_SOAK=1` and
`LOG_FOUNDRY_MODEL_SEEDS=0-1`, and asserts FR-003's selection. It also asserts FR-003's seeds rule
directly. This is a gate, so every check in it, in the report and in the seeds rule is
mutation-tested.

#### Acceptance Criteria:

- [ ] The report's fixture corpus asserts the exit status and the reason for each of twelve rows:
      - clean;
      - known;
      - new, with its re-run command;
      - one soak item short;
      - all skipped by a skip mark;
      - an `xfail` with neither the property nor the known text;
      - the known text without the property;
      - the property without the known text;
      - a wiring item absent;
      - wiring items failed;
      - one known item over a ceiling;
      - exactly at a ceiling.
- [ ] One mutant per check is planted, and each is reddened by a named row or test:
      - the count floor;
      - the skipped-or-unexplained floor;
      - the known text;
      - the known property;
      - the wiring set present;
      - the wiring set passed;
      - the ceiling;
      - the window from the run number;
      - the dispatch bound, read before the set is built;
      - the normalised output;
      - each of the fourteen workflow assertions, including the selection narrowed to
        `test_soak_the_close_count_under_concurrency`, and the derived set read from the two
        roots' direct imports only.
- [ ] The path-filter check passes against the workflow, and fails, naming the path, when the
      filter drops `tests/model_sinks.py`, which `test_invariants_model.py` imports directly, and
      when it drops `tests/sink_rigs.py`, which neither root imports directly.
- [ ] Every other test still passes with the soak switch unset, and the default collection holds
      no soak item.
- [ ] This FR serves no invariant: it guards a job and its report.

### FR-005: The record says the soak runs nightly, gates a pull request that changes it, and what a known finding is

#### Description:

This spec reverses two parts of SPEC-086's decision, *The model gates its close count only where
one caller makes it exact, and soaks it under concurrency on request*:
- "set by no workflow", and its *Do NOT build*, "a workflow or default that sets
  `LOG_FOUNDRY_MODEL_SOAK`", reversed by Andrew's decision of 2026-11-14 that the soak runs
  nightly;
- "gates nothing", which is **partly reversed**. The soak gates a pull request that changes it
  (FR-003's path filter), and a red night stops the queue (FR-006, Andrew 2026-11-17).

The decision is rewritten in place, with the same file name (`knowledge-layout.md`: "A decision's
file name is chosen once and never changes ... A reversal rewrites the title in place"). Its title
drops "on request", and a dated History entry names SPEC-099 and both reversals. Its fence then
says:
- the nightly workflow runs the soak;
- the soak gates a pull request that changes the soak;
- a scheduled night's failed or timed-out attempt stops the PR queue until that night's own filing
  is on `main`, and nothing else stops or clears it;
- the soak excuses a repeat close for a writer's arm, or for a log that entered after the first
  take and wrote before the second;
- a red item of a known shape is reported as known while its §12 item is open and the shape is
  under its ceiling.

Its *Do NOT build* lists:
- a soak rule that excuses any write between the takes;
- a known row without an open item;
- a known row that explains Shape A's record;
- a ceiling in a clearing file;
- a gating assertion excused as known;
- a window drawn from the clock;
- a queue that reads the soak from `main`'s head, clears a night by a time or a re-run, or lets a
  dispatch's or a pull request's run block it.

The other sites that state the old behaviour are rewritten in place:
- the model file's docstring (`test_invariants_model.py:83-93`), and the soak item's
  (`:1296-1301`);
- invariant 5's *Guarded* line, which says "its soak, run only on request and gating nothing";
- §12's *The one generative test cannot find the lifecycle defects it is cited as guarding*, which
  says the soak is "run on request" and "gates nothing".

The §12 item *CI cannot see a hang, a platform difference or an untested line* says its soak half
is closed by SPEC-099 and its hang half by PR #370, and it stays open for what neither decided.
SPEC-086 and its delivery doc are frozen records and are not edited.

#### Acceptance Criteria:

- [ ] The decision file keeps its name. Its title, fence and body state the nightly run, the gated
      pull request, the queue's stop, the rule of FR-001 and the known rule of FR-002. Its History
      carries the date, SPEC-099 and both reversals.
- [ ] Each of these phrases is read with every file's text joined across its line breaks and its
      whitespace collapsed: "no workflow sets", "set by no workflow", "run only on request",
      "soak run on request", "run on request", "gating nothing", "gates nothing". The files read
      are `docs/invariants.md`, `docs/architecture.md`, every file under `docs/decisions/`, and
      every file under `tests/`. None contains any of the phrases except where it is struck
      through and marked with SPEC-099.
- [ ] §12's CI item names SPEC-099 for the soak and PR #370 for the hang, and stays under
      `### Open`.
- [ ] This FR serves no invariant: it is the record.

### FR-006: A failed night blocks the PR queue until a pull request files that night

#### Description:

Andrew decided on 2026-11-17 that a red night blocks merging. The queue today reads every run on
`main`'s head and answers `IS RED` for any failure, and `PROTOCOL.md` (`:51-55`) lets only the
session whose own merge turned `main` red work through that. A first draft of this FR read the
soak from the head sha too. An execution-frame review showed that ordinary events then cleared or
dodged the block:
- a green re-run read as clear;
- a merge made while the night ran moved the head, and the red landed on a sha nobody read;
- a cancelled night, a red dispatch, or a dispatch's typo read as a finding.

So the queue does not read the soak from the head. **It reads the night's verdict as a durable
fact, and only a recorded act clears it.**

**Which runs count.** Only scheduled runs of `Model soak` carry a verdict. A dispatch or a pull
request's run never affects the queue: a red dispatch is someone's experiment, and a dispatch's
typo or timeout is harmless. Runs of `Model soak` on `main`'s head are left out of the head's
reading entirely.

**Which run is read.** `queue.sh`'s `soak_verdict` lists the newest 30 scheduled runs, whatever
their status, on any sha, newest first. It reads the first run that has a verdict. So a merge made
while the night ran cannot step past the night.

**Re-runs.** `gh run list` reports only a run's latest attempt: measured on run 43801629163, it
read `attempt=2 conclusion=success`. `gh api .../actions/runs/<id>/attempts/<n>` reports each
attempt, and read attempt 1 `failure` and attempt 2 `success` for the same run. So `soak_verdict`
reads every attempt through the API, and **a run blocks if any attempt concluded `failure` or
`timed_out`**. A re-run cannot turn a red night green. A re-run still in progress leaves the run
`in_progress`, so the run is listed whatever its status and its finished attempts are still read.
If the API cannot show an attempt, the verdict is unreadable.

**Which conclusions block.**
- `failure` and `timed_out` block. A timeout may be a hang the guard could not catch.
- `success` is a verdict that does not block.
- `cancelled`, `startup_failure`, `action_required`, `skipped`, `neutral`, `stale`, and an
  attempt not yet finished, are no verdict. A night whose every attempt is one of these is passed
  over for the night before it.

This is the orchestrating session's reading of Andrew's choice, recorded as its call. The choice
was put to him as "a failed night would mean a new suspected data-loss bug", and a night that
judged nothing suspects nothing.

The consequence: a run of verdict-less nights hides nothing new, but it also checks nothing.
`turn`'s `READY` answer then carries a line saying so:
- `note: the nightly soak's newest verdict is from <time>` when the newest verdict is more than 48
  hours old;
- `note: the nightly soak has no verdict in its last 30 scheduled runs` when there is none.

Every session that asks for its turn sees the line. GitHub emails the user who last changed the
`cron` line about a failed run. Whether it emails about a cancelled one or a `startup_failure` was
not measured.

**What clears it.** A night is cleared only by its own filing. `tests/soak_known.py` holds
`FILED_NIGHTS`, one entry per line in the form `    "<run id>-<attempt>": "<§12 lead>",`. A
night that blocks names its last failing attempt. `soak_verdict` then:
1. fetches `main` into `origin/main` with `--no-write-fetch-head`, because several worktrees
   hitting `FETCH_HEAD` at once would race;
2. reads `tests/soak_known.py` there;
3. clears the night only if that attempt's own entry is present.

Nothing else clears a night. A commit touching `tests/soak_known.py` for another night does not
clear it. A filing of attempt 1 does not clear a failed attempt 2.

The filing is the night's key and not a time. So no clock is compared: committer dates are set by
whoever commits, and a run id does not exist before its run does. A filing cannot be carried by
the tree the night tested. It can be made while the night is still running, by someone who knows
its id and that it will fail. It still answers that night.

**The clearing branch.** A clearing branch changes `tests/soak_known.py` and
`docs/architecture.md` and nothing else, leaving each a regular file (mode 100644). `queue.sh
clearing-range <from> <to>` decides it from `git diff --raw -z --no-abbrev --no-renames
--no-ext-diff --no-relative --no-color --no-textconv`:
- names are read NUL-delimited and compared whole;
- a rename reads as a deletion and an addition;
- no `diff.*` or `color.*` setting can change what is read;
- an empty range, or one whose changes revert each other, is not clearing.

`tests/soak_ceilings.py` is not a clearing file. So a clearing pull request cannot raise a ceiling
(FR-002).

While the verdict blocks:
- `turn` answers `READY` to a clearing branch, and `WAIT` to any other branch, naming the night;
- `acquire` refuses to stamp the lock onto any other branch, even for the lock's holder, whose
  `turn` answers before it reads the trunk.

**The hook.** It asks `queue.sh soak-verdict`, and while the verdict blocks it refuses a push
whose commits since `main`'s head are not a clearing range. That holds whatever else is red, not
only when `main_green` answers 4. It fails open only when the verdict cannot be read: `gh`
missing, `gh` failing, or a queue it cannot run, as it does today without this check.

A stale branch is not read for the soak. It is refused for its base, and the rebase message is the
only one it gets. The hook reads a push to the trunk too, such as `git push origin codefix:main`.
What actually stops a trunk push is the server's `main` ruleset, which requires a pull request and
which an admin can bypass. This spec does not change it.

**The state.** `main_green` reads the head's runs other than `Model soak` as today: 1 red, 2
unsettled, 3 unreadable. When those are clear, it answers:
- 4 when the soak's verdict blocks;
- 3 when the verdict cannot be read;
- else 0.

`queue.sh main-state` and `queue.sh soak-verdict` print and exit with their answers. If the
workflow file is renamed or removed from `main`, `gh run list --workflow model-soak.yml` fails
(measured: HTTP 404 and exit 1 for a workflow not on the default branch). The queue then answers 3
and waits, which is loud rather than silent, and `test_model_soak_workflow.py` fails in CI on the
same change.

**Who files it, and when.** A session that reads the soak's `WAIT` escalates the night to Andrew,
naming the run, as it does for any red. **A clearing pull request is written only once Andrew has
answered that escalation.** This is his rule of 2026-11-17. The pull request:
- adds the night's `FILED_NIGHTS` entry;
- files the finding under its §12 item, a new item under `### Open` when it is new;
- adds a `KNOWN` row when the finding is a soak signature of a new shape, which takes
  `UNMEASURED_CEILING`, except a `repeat` whose record is Shape A's (FR-002). That one is filed
  under its item, the `fenced` item or a new item for a regression of SPEC-097, with its
  `FILED_NIGHTS` entry and no row, and `test_no_known_row_explains_shape_a` refuses a row that
  would explain it.

A finding no row can explain, such as a gating failure, is filed the same way. The next night is
red again until its fix lands, and that fix is an ordinary pull request made while the queue is
clear. A re-run cannot clear a night, and there is no acknowledgement command.

**Consequences, each stated in `PROTOCOL.md`:**
- **Stale-lock reaping stops while the verdict blocks** (`queue.sh:199`, `main_green || return
  0`). A lock whose holder died is kept until the night is filed, and while it is held no clearing
  branch can acquire. Breaking it is Andrew's, as it is today under any red. A live holder can
  file the night itself: it converts its open pull request to a draft and re-acquires from a
  clearing branch.
- **Merging needs no lock** (`docs/process/several-agents.md`), and the `main` ruleset requires no
  status check. So a holder whose pull request was pushed before the night went red can still
  merge it. The merge does not clear the night, because only the filing does, but it lands
  unreviewed against the night's finding. `PROTOCOL.md` tells every session to run `queue.sh
  main-state` before merging. Nothing enforces that, and a required status check, which would, is
  not decided.
- **The installed queue must be updated.** Sessions run the copies `install.sh` wrote, so this
  takes effect only when `install.sh` is run again after the merge. Until then a red scheduled run
  on the head reads `IS RED` and escalates as today, which still blocks.

#### Acceptance Criteria:

- [ ] `scripts/pr-queue-soak-test.sh` runs with a stub `gh` (run lists and per-attempt API answers
      from the environment), a bare remote and no network. Each case asserts its state or exit,
      and a line of what the queue said, including the cases that must stay silent. The cases:
      - a failed, a timed-out, a passed night;
      - a green re-run of a red attempt;
      - a red re-run of a green one;
      - two red attempts;
      - each verdict-less conclusion falling back to a failed and to a passed night;
      - a red night being re-run;
      - a night still running;
      - a red night on an older sha;
      - no night, and only verdict-less nights;
      - a red dispatch, and a soak running, on the head;
      - another red, and another run in progress, beside a blocking verdict;
      - `gh`, its API, and one attempt unreadable, and `gh` absent;
      - filings of the blocking attempt, of an earlier attempt and of another run;
      - a filing in a comment, at the wrong indent, and on a local branch only;
      - an unreadable origin, and a peer's filing not yet fetched;
      - `turn` for 22 branch shapes, among them renames onto and off a clearing file, a deletion,
        a mode change, a symlink, a gitlink, paths with a space, both names in one path, a top-level `soak_known.py`, a newline or a glob
        character,
        `tests/soak_ceilings.py`, a merge bringing in `src/`, and hostile `diff.*` settings;
      - `acquire` by a holder onto each kind of branch;
      - `turn`'s note for a verdict 48 hours old and for none, and its silence otherwise;
      - the hook on a clearing push, an added `src` commit, another red, `gh` failing or absent
        (each passing, with nothing said of the soak), an unblocked night, a trunk push, a stale
        clearing branch (the rebase message only) and a path with a space;
      - reaping kept while blocked, and done once filed.
- [ ] Every check is mutation-tested, one mutant each, and each mutant is reddened by a named case.
      The two mutants that survived the execution-frame review are planted again and are now
      reddened: a green attempt clearing a red one, and the hook refusing when the verdict cannot
      be read.
- [ ] `sh scripts/pr-queue-test.sh` and `sh scripts/pr-queue/install-test.sh` still pass.
- [ ] `PROTOCOL.md` states:
      - which runs count;
      - the attempt rule;
      - which conclusions block;
      - the filing;
      - the clearing branch;
      - that a clearing pull request is written only after Andrew has answered;
      - the three consequences above.
- [ ] **Owed after the merge:** `install.sh` is run once, and `queue.sh soak-verdict` from the
      installed directory exits 0, 3 or 4 against the real repository. The delivery doc records
      both.
- [ ] This FR serves no invariant: it is the repository's process, not the library's behaviour.

---

## Data Model

```python
# tests/model_oracles.py: changed additively.
FORMS_SOAKED = ("emit", "deliver", "http")   # moved here from FORMS so the report reads the same tuple

@dataclass(frozen=True)
class Arm:
    by_writer: bool
    by: str = ""             # the library function that called _CloseRegistry.arm

@dataclass(frozen=True)
class Take:
    by: str = ""             # the library function that called _CloseRegistry.take

@dataclass(frozen=True)
class Note:
    thread: str              # an orphan log entered api._note_orphan_emit for this sink

SinkEvent = Arm | Take | Write | Close | Note

def owed_between(between: Sequence[SinkEvent]) -> bool: ...
#   a writer's arm, or a Note on a thread followed later by a Write on that same thread

# tests/soak_known.py: new; imports model_oracles only.
@dataclass(frozen=True)
class Known:
    name: str
    item: str                # the bold lead of a bullet under ### Open in architecture.md §12
    kind: str                # "repeat" | "late-write"

KNOWN: tuple[Known, ...]
FILED_NIGHTS: dict[str, str] # "<run id>-<attempt>" -> the §12 lead it was filed under, one a line
def explain_run(sinks: Sequence[tuple[Sequence[SinkEvent], Closes]]) -> list[Known]: ...
#   [] when nothing was found or any finding has no known shape
def reason(known: Sequence[Known]) -> str: ...

# tests/soak_ceilings.py: new; not a clearing file.
UNMEASURED_CEILING = 3
CEILINGS: dict[str, int]     # a row's measured ceiling, its derivation and commit on its line
def ceiling_of(name: str) -> int: ...
```

```python
# scripts/model-soak-report.py: new; imports tests/model_oracles.py and tests/soak_known.py.
WINDOW = 5000
WINDOWS = 30
MAX_SEEDS = 10_000
PULL_REQUEST_SEEDS = "0-99"
WIRING: tuple[str, ...]      # the eight items of FR-003's wiring set
def normalise(seeds: tuple[int, ...]) -> str: ...
def seeds(event: str, asked: str, run_number: int) -> str: ...
def report(xml: pathlib.Path, seeds: str) -> tuple[str, int]: ...   # (Markdown, exit status)
```

```sh
# scripts/pr-queue/queue.sh: changed.
SOAK_WORKFLOW="Model soak"; SOAK_FILE="model-soak.yml"; SOAK_LOOKBACK=30; SOAK_QUIET_HOURS=48
main_green       # 0 green · 1 RED · 2 building · 3 could not run · 4 the soak's verdict blocks
head_green       # main_green's old reading of the head, with Model soak's runs left out
soak_verdict     # 0 · 4 · 3, printing "<state> <run id>[-<attempt>] <time> [filed]"
filed ID-N       # 0 when origin/main's tests/soak_known.py files that attempt
clearing_range A B   # 0 when A..B changes only the two clearing files, each left mode 100644
clearing_branch  # clearing_range from the branch's base on origin/main
soak_note        # turn's line for a verdict over 48 hours old, or none
queue.sh main-state | soak-verdict | clearing-range A B
```

## Configuration / Environment

| Name | Where | Meaning |
|---|---|---|
| `seeds` | `workflow_dispatch` input of `model-soak.yml` | `LOG_FOUNDRY_MODEL_SEEDS` for that run, at most 10,000 seeds; empty for the run's window |

No new environment variable and no library setting. The workflow sets SPEC-086's two switches.

```bash
# Re-run a red item once, as its failure and the report print:
LOG_FOUNDRY_MODEL_SOAK=1 LOG_FOUNDRY_MODEL_SEEDS=<seed> poetry run pytest -n 0 '<node id>'
# A concurrent seed replays its draw and not its schedule, so a race may need many runs:
for i in $(seq 300); do LOG_FOUNDRY_MODEL_SOAK=1 LOG_FOUNDRY_MODEL_SEEDS=<seed> \
  poetry run pytest -n 0 -q '<node id>' || break; done
```

## File & Folder Structure

```
.github/workflows/model-soak.yml        # new (FR-003)
scripts/model-soak-report.py            # new (FR-003, FR-004)
scripts/pr-queue/queue.sh               # changed (FR-006): the soak's verdict, its filing, the clearing range
scripts/pr-queue/pre-push               # changed (FR-006): the push check while the verdict blocks
scripts/pr-queue/PROTOCOL.md            # changed (FR-006)
scripts/pr-queue-soak-test.sh           # new (FR-006)
tests/
├── model_oracles.py                    # changed: Note, Arm.by, Take.by, owed_between, FORMS_SOAKED
├── soak_known.py                       # new (FR-002, FR-006): the shapes and FILED_NIGHTS
├── soak_ceilings.py                    # new (FR-002): the ceilings, outside the clearing files
├── test_invariants_model.py            # changed: labels, Note, the record's tail, the known verdict
├── test_model_oracles.py               # changed: the rule's corpus rows
├── test_soak_known.py                  # new: the shapes' corpora, the write-site roster
└── test_model_soak_workflow.py         # new: the open-item check, the workflow guard, the report
docs/
├── architecture.md                     # changed by the build: §12's CI item and model item (FR-005)
├── invariants.md                       # changed: invariant 5's Guarded line (FR-005)
├── decisions/working-rules/the-model-gates-its-close-count-only-where-one-caller-makes-it-exact-and-soaks-it-under-concurrency-on-request.md   # rewritten in place, name kept
├── audits/2026-11-14-probes/lifecycle/spec099_rule_corpus.py   # on main since #391; not changed
├── audits/2026-11-14-probes/lifecycle/spec099_prototype.patch  # on main since #391, against eda19f32;
│                                                               #   it does not apply to d9f2f097
└── spec-delivery/SPEC-099-nightly-model-soak.md                # new, at completion
```

## Implementation Phases

**Start condition:** met. §12's item *The model test's lost-line oracle does not read the two lines
the exit drain has written since SPEC-090...* was closed by #393 (`4b7e4b86`), and the specs before
this one in the lifecycle lane (`docs/specs/ARCS.md`), SPEC-097, SPEC-098 and SPEC-101, have
landed. `spec099_prototype.patch` was made against `eda19f32` and does not apply to `main`; the
build writes its change against `main` and uses the patch only as a reference.

One pull request. Everything it adds is inert until it merges: the schedule fires only from
`main`, the pull-request trigger runs the job against the branch, and the queue change runs only
from an installed copy. It touches `scripts/pr-queue/`, so its gates include `pr-queue-test.sh`
and `install-test.sh`, and `install.sh` is run once after the merge (FR-006).

### Phase 1: The rule, the record and the known shapes (FR-001, FR-002)

- `Note`, the labels, `owed_between`, the record's tail, and their corpora and mutants.
- `tests/soak_known.py` and `tests/soak_ceilings.py`, the soak item's known verdict and property,
  the open-item and ceiling tests, the shapes' corpora and the write-site roster.
- A soak of seeds 0 to 999 three times with the switch on, and each red item read against the
  table.

### Phase 2: The workflow, the report and the guard (FR-003, FR-004)

- `scripts/model-soak-report.py`, with `WINDOW` and `MAX_SEEDS` and their reasons.
- `.github/workflows/model-soak.yml`.
- `tests/test_model_soak_workflow.py` and its mutants. `zizmor` and the roster.

### Phase 3: The queue (FR-006)

- `queue.sh`, `pre-push` and `PROTOCOL.md`; `scripts/pr-queue-soak-test.sh` and its mutants.

### Phase 4: The record (FR-005)

- The decision file, invariant 5, §12 and the docstring.
- FR-003's and FR-006's owed criteria, named in the pull request body.

## Evidence

Two commits are measured here. **`d9f2f097`** (`main` on 2026-11-24) is what the spec must be true
of, and the first subsection holds everything measured there. **`eda19f32`** (`main` on 2026-11-16
and 2026-11-17, when the spec was written) is history; FR-001's Shape B and boundary records, one
of FR-002's two late-write rates, the `-n 4` cost and every prototype result rest on it, and none
of that was re-run at `d9f2f097`.

### At `d9f2f097`, measured 2026-11-24

The macOS figures are Python 3.13 on 10 cores at `-n 12`, loads of about 8 to 10. The Linux
figures are the spec reviewer's.

**How red the soak is on `main`.** `LOG_FOUNDRY_MODEL_SOAK=1 LOG_FOUNDRY_MODEL_SEEDS=0-999 poetry
run pytest tests/test_invariants_model.py -k soak_the_close`, four runs of 3,000 items, 65.9 to
68.1 s each:

| Run | Red | Findings |
|---|---|---|
| 1 | 1 | `late-write@1` at `http-seed922` |
| 2 | 4 | `late-write@3` at `deliver-seed202` and `deliver-seed556`; `late-write@5` at `http-seed972`; `late-write@5` and `late-write@0` on two sinks at `deliver-seed898` |
| 3 | 0 | |
| 4 | 1 | `late-write@1` at `http-seed604` |

That is 6 red items in 12,000, on seven sinks, every finding `late-write` and every write on a
`Thread-N (runner)` thread: the orphan late write by its signature and thread. No `repeat`, no
`untaken`, and no gating failure. SPEC-086's record drops what came after the last close, so whether
each meets FR-002's late-write predicate cannot be read from these runs; FR-001's record tail is
what makes that readable, and Phase 1's soak reads it.

**On the Linux stand-in.** Docker `python:3.12-slim`, `--cpus=4 --memory=16g`, the pinned
`pytest` 9.2.0, `pytest-xdist` 3.8.0, `pytest-asyncio` 1.4.1.post1 and `opentelemetry-proto`
1.45.2, over `git archive d9f2f097`, at `-n 12`, with `-k
test_soak_the_close_count_under_concurrency`:

| Seeds | Items | Red | Wall |
|---|---|---|---|
| 0 to 4999 | 15,000 | 1: `late-write@8` at `deliver-seed4276`, written on a runner thread | 307.6 s |
| 5000 to 9999 | 15,000 | 0 | 284.3 s |

No `repeat`, no `untaken` and no gating failure in 30,000 items: about 0.5 orphan late writes a
night there, against 7.5 on macOS.

**The swap item is closed.**
`PYTHONPATH=src python docs/audits/2026-11-14-probes/lifecycle/p11_swap_arms_a_sink_a_shutdown_closed.py all 7`
reports `forced: 0 of 7 runs closed S twice at one write count`, S's closes entering at `[1]` in
all 7, and both controls 0 of 7.

**The `fenced` item's record.** A pytest plugin wrapped `_CloseRegistry.arm` and `.take` (the
caller read from the frame), `api._note_orphan_emit` (a `note` at entry, and a writer's arm while
inside it) and the test's `Sink.emit` and `.close`, and ran
`tests/test_shutdown_mid_swap.py::test_a_retarget_answering_fenced_keeps_its_arm_and_closes_the_sink_once_after_its_write`
7 times. Sink B's record was the same each time:
`note(MainThread) arm(writer:_note_orphan_emit) write(MainThread) take(_close_owed) | close@1
arm(change:_swap_sink) take(_close_owed) | close@1`. SPEC-086's rule and FR-001's rule each report
`repeat@1`, and the dropped swap row's predicate matched, 7 of 7.

**Call sites.** An AST sweep of every module directly under `src/log_foundry/` finds the arms
listed in FR-001 (`_lifecycle.py`: `_clear_after_fork`, `_get_worker`, `_note_orphan_emit`,
`_adopt_factory_after_fork`, `rearm_if_released`, and `_swap_sink` twice) and its takes
(`_close_owed` once, `_swap_sink` three times). `_arm_and_hand` is gone, removed by SPEC-098. The
calls to `run_delivery`, `emit`, `deliver` and `emit_via_deliver` are exactly four: `api._log` at
`api.py:211` and `:215`, and `worker._emit` at `worker.py:3424` and `:3476`.

**The prototype patch.** `git apply --check spec099_prototype.patch` fails at
`tests/test_invariants_model.py`.

### At `eda19f32`, measured 2026-11-16 and 2026-11-17

`eda19f32` was then `main`. The macOS figures are Python
3.13.1 on a 10-core Mac at `-n 12`, at the loads given. The Linux figures are Docker
`python:3.12-slim` with `--cpus=4 --memory=16g` on that Mac, standing in for `ubuntu-latest`,
which has 4 CPUs and 16 GB for a public repository, with the pinned `pytest`, `pytest-xdist`,
`pytest-asyncio` and `opentelemetry-proto`. *Prototype* rows ran `spec099_prototype.patch` applied
to `git archive eda19f32`. The first prototype is the patch at `914d2154`. The revised one is the
patch the 2026-11-17 revision committed. Where a row below names the swap row, it is the row this
revision drops, and where it names the lost-line oracle, it is the item #393 closed.

**How red the soak was.** `LOG_FOUNDRY_MODEL_SOAK=1 LOG_FOUNDRY_MODEL_SEEDS=0-999 pytest
tests/test_invariants_model.py -k soak_the_close`, four runs of 3,000 items, 65 to 67 s each at
loads of 4 to 15:

| Run | Red | Findings |
|---|---|---|
| 1 | 4 | `repeat@1` at `http-seed668` and `emit-seed4`; `late-write@2` at `http-seed974`; the lost-line oracle at `deliver-seed642` |
| 2 | 2 | `repeat@1` at `deliver-seed258`; `late-write@1` at `http-seed398` |
| 3 | 1 | `late-write@3` at `emit-seed248` |
| 4 | 2 | the lost-line oracle at `deliver-seed924`; `repeat@1` at `emit-seed962` |

That is 9 red in 12,000. Read by their records:
- three `repeat`s are Shape A, with `arm(writer) write take arm(change) | take`;
- `deliver-seed258` is the boundary record, `arm(writer) take write arm(change) | take`;
- three are the orphan late write;
- two are the lost-line oracle.

**The lost-line oracle** (closed by #393 since). The gating items alone, `LOG_FOUNDRY_MODEL_SEEDS=0-999 -k
accounting_identity`, were red on 1 of 2 runs, at `http-seed78`. A copy of the model file whose
failure printed the whole of stderr was run four times over the `deliver` and `http` forms (8,000
items) and was red twice, at `deliver-seed790` and `deliver-seed450`. In both, the worker had two
failed runs, and stderr held one `batch abandoned` line and one `lost <n> event(s); 1 more exit
batch(es) abandoned`, the line `worker.py:2667` writes since SPEC-090. `_WORKER_LOST` matches the
first and not the second.

The spec reviewer's independent run of the gating model over seeds 0 to 999 was red 3 of 4 times on
the same assertion, at `deliver-seed60`, `deliver-seed572` and `http-seed510`.

Over every authoring run in this section, on `main` and on the first prototype, on macOS and on the
Linux stand-in, the assertion failed 28 of 56,000 `deliver` and `http` runs, about 1 in 2,000. A
pull request's `CI` runs 32 such items on each of its two legs, so about 3 in 100 `CI` runs would
be red on it.

The declined line, `worker.py:2668-2673`, is read from the code and was not met.

**The first prototype's verdicts.** Its nightly selection was `-k soak`, with the switch on:

| Where | Items | Known: swap / orphan late write | New | Wall |
|---|---|---|---|---|
| macOS, seeds 0 to 999, six runs, loads 9 to 20 | 18,000 | 5 / 18 | 7, all the lost-line oracle | 67 to 79 s each |
| macOS, seeds 0 to 1999, three runs, loads 6 to 13 | 18,000 | 6 / 6 | 6, all the lost-line oracle | 134 s each |
| Linux stand-in, `-n 12`, seeds 0 to 999 | 3,000 | 0 / 0 | 2, the lost-line oracle | 57.3 s |
| Linux stand-in, `-n 4`, seeds 0 to 999 | 3,000 | 1 / 0 | 2, the lost-line oracle | 172.6 s |
| Linux stand-in, `-n 12`, seeds 1000 to 1999 | 3,000 | 3 / 0 | 1, the lost-line oracle | 58.3 s |
| Linux stand-in, `-n 12`, seeds 2000 to 4999 | 9,000 | 4 / 0 | 5, the lost-line oracle | 168.6 s |

Every new item was the lost-line oracle, and each known item's record was read against its row.
No soak signature went unmatched, and no `untaken` appeared. The orphan late write did not appear
on the Linux stand-in in 18,000 items, against 24 in 36,000 on macOS.

The second macOS set ran a copy that also judged each sink by SPEC-086's rule and logged where the
two disagreed. They disagreed once, at `deliver-seed1772`, which is Shape B:
`take(_close_owed) write(1272) arm(change:_swap_sink) note(1271) write(1271) | take(_close_owed)`.
Thread 1271's log entered after the first take and wrote, so the second close is owed. SPEC-086's
rule reports `repeat@2`, and the swap row would have called it Shape A.

The boundary record was met unforced at `deliver-seed348`, `note(177) arm(writer)
take(_close_owed) write(177) arm(change:_swap_sink) | take(_close_owed)`, and at `http-seed576`.
Both are flagged by this rule and read as owed by the any-write rule.

**The revised prototype's verdicts**, with FR-003's selection:
- macOS, seeds 0 to 999, two runs at load 1.4: 3,008 passed each in 67.5 s. No finding, not even
  the lost-line oracle. The rates in this section depend on load.
- Linux stand-in, the full window of seeds 0 to 4999, two runs of 15,008 items: 281.6 s and
  280.5 s. They held 5 and 8 known swap items and no orphan late write, and 5 new items each, every
  one the lost-line oracle. The report read `15000 of 15000` and `8 of 8` both times and exited 0.
  The runs were red through pytest, as they should be.

**The rule against Shape A** (`spec099_rule_corpus.py`):

| Record | SPEC-086's rule | any write | this rule | Known shape |
|---|---|---|---|---|
| p11 `forced`, 7 runs | repeat 7/7 | repeat 7/7 | repeat 7/7 | swap 7/7 |
| Shape A as at `deliver-seed962` | repeat | repeat | repeat | swap |
| the boundary as at `deliver-seed600` | repeat | **owed** | repeat | swap |
| Shape B as at `emit-seed1582` | repeat | owed | owed | none |
| the re-arm at seed 1653 | owed | owed | owed | none |
| #339's shape, a sink change's take first | repeat | repeat | repeat | **none**: red |

**The rule's mutants** (the spec reviewer's `mut_rule.sh`, run on the revised prototype). All 16
rows pass unmutated:

| Mutant | Rows reddened |
|---|---|
| M1 | the boundary, (b) and (c) |
| M2 | Shape B |
| M3 | (b) |
| M4 | (a), (b) and (c) |
| M5 | (c) |

On the first prototype, whose corpus lacked (a) to (c), M3, M4 and M5 survived.

**The gate's mutants.** In the revised prototype, 23 mutants of the report, the seeds rule and the
workflow were each killed by a named row or test. Two had survived a first sweep:
- the known-text check;
- an input expression inside a `run:` block, which the first guard read on the `run:` line only.

Two changes closed them: the row with the property but without the text, and the all-expressions
check. Further results:
- The swap predicate as first drafted reddens `swap-arm-plus-unknown-arm-caller`.
- A planted `sink.emit([])` in `config.py` reddens the write-site roster.
- The open-item check reddens on the late-write item struck through. The spec reviewer's
  `mut_openitem.sh` showed the same for the swap item moved under `### Resolved`.

**The report against the spec reviewer's fixtures.** The first prototype read two fixtures as a
clean night: all six soak items skipped, read as `clean: 6`, and an `xfail` with the reason "flaky
under load", read as known. Both now break the floor.

**Hostile dispatch inputs** (the reviewer's `seeds_probe.py`, run against the revised `seeds`):

| Input | Result |
|---|---|
| `"1,\n2"` | `1-2` |
| `" 5 "` | `5` |
| `"1\n"` | `1` |
| `"0-3\nfoo=bar"` | exit 1, no output |
| `"1;id"` | exit 1, no output |
| `"3-1"` | exit 1, no output |
| `"$(id)"` | exit 1, no output |

The first prototype wrote `"1,\n2"` raw into `$GITHUB_OUTPUT`. Run numbers 1, 2, 29, 30 and 31 give
the windows `5000-9999`, `10000-14999`, `145000-149999`, `0-4999` and `5000-9999`. A dispatch of
`0-9999` is accepted and `0-10000` refused.

**What gh reports, measured 2026-11-17 with gh 2.93.0 against this repository.**
- `gh run list` reports a run's latest attempt only. Run 43801629163 read `attempt=2 completed
  success`.
- `gh api repos/{owner}/{repo}/actions/runs/43801629163/attempts/1` read `failure` with its own
  `updated_at` (`2026-11-16T04:06:50Z`). `.../attempts/2` read `success`, and `.../attempts/3`
  answered HTTP 404.
- `gh run list --workflow integration.yml --event schedule --json databaseId,attempt,headSha`
  lists that workflow's scheduled runs, and the API reads each attempt with `event=schedule`.
- `gh run list --workflow no-such.yml` answers "HTTP 404: workflow no-such.yml not found on the
  default branch" and exits 1.
- A re-run in progress was not observed. That its run lists as `in_progress`, and that an unfinished
  attempt's `conclusion` reads `null`, come from GitHub's documented run model. They are not
  measured here, and the queue handles both by listing every status and treating `null` as no
  verdict.

**The first queue prototype, at `240bb9a7`, failed the execution-frame review**, because it read the
soak from `main`'s head. A green re-run read clear. A red night on a sha a later merge had passed
was not read. A cancelled night, a red dispatch and a malformed dispatch each read as blocking. Its
hook refused only on exactly 4. Its names were matched inside a space-separated list, so a path
with a space in it could pass. Its stale clearing branch got the soak's message, not the rebase
message. And a clearing pull request could raise a ceiling.

**The queue as revised.** `scripts/pr-queue-soak-test.sh`, grown from the reviewer's harness, passes
122 cases on the revised prototype, every one asserting where it lands and a line of what the queue
said. Twenty-six mutants were planted, one per check, and each is reddened:

| Mutant | Reddened by |
|---|---|
| the reviewer's: a green attempt clears a red one | "a green re-run does not clear a red attempt" |
| the reviewer's: the hook refuses when the verdict is unreadable | the hook with `gh` failing, and with it absent |
| soak runs on the head read as any run | "a red dispatch on the head does not block" |
| only the latest attempt read | the green re-run, and "a red night being re-run still blocks" |
| `timed_out` not blocking | "a timed-out night blocks" |
| a verdict-less night read as passed | each "... falls back to a failed one" |
| a verdict-less night read as failed | each "... falls back to a passed one" |
| only completed runs listed | "a red night being re-run still blocks" |
| the newest night only, with no fallback | each "... falls back to a failed one" |
| a filing of any attempt clears | "filing an earlier attempt does not clear" |
| the filing's format not held | the comment, the wrong indent, the local branch |
| `origin/main` not fetched | "filing the blocking attempt clears", and the unreadable origin |
| an unreadable verdict read as clear by `main-state` | "...and main-state reads it as 3, not clear" |
| the mode not held | the deletion, the executable, the symlink, the gitlink |
| the path not held | every non-clearing branch shape |
| names matched inside a list | "a path holding both names, in order", and the top-level `soak_known.py` |
| renames detected | "tests/soak_known.py renamed out" |
| an empty range accepted | no commits, and a commit with its revert |
| `turn` admits any branch | every non-clearing branch shape |
| `acquire` not reading the verdict | the holder's re-stamp |
| the quiet note never printed | "an old verdict: turn names its date" |
| `turn` on 0 saying nothing of a quiet soak | both notes |
| the hook not reading the verdict | the added `src` commit, the other red, the trunk push, the space path |
| the hook reading `main-state` instead | "the same push while another red is on the head" |
| the hook reading stale refs | "...and not the soak's" |
| the hook skipping the trunk | "a trunk push with src while it blocks" |

Two of these first survived and were answered with new cases:
- an unreadable verdict read as clear, answered by `main-state` with only the API failing;
- names matched inside a list, answered by the path holding both names and the top-level file.

The same sweep over the report, the seeds rule, the ceilings and the workflow guard planted 25
mutants. Each is reddened, the bound read after the set is built and a ceiling without its
derivation among them. One mutant, of a second bound after the parse, survived, because the
text-first count makes that bound unreachable, and the second bound was deleted.

`pr-queue-test.sh` and `install-test.sh` pass against the changed hook and queue. With no
reachable GitHub there, `soak-verdict` cannot answer, and the hook does as before.

**The dispatch bound.** Before the revision, `seeds --asked 0-10000000` built ten million seeds
before refusing them: 914 MB resident, and 122 MB for `0-1000000`. Counted from the text first,
both are refused at 21 MB.

**Cost on a 4-CPU Linux runner.** On the stand-in:
- 3,009 items took 57.3 s, and 9,009 took 168.6 s, at `-n 12`: 18.7 ms an item;
- 3,009 items took 172.6 s at `-n 4`;
- a full window: see the revised prototype above.

The spec reviewer measured 57.2 s at `--cpus=4` and 56.8 s with `--cpuset-cpus=0-3` for 3,009
items at `-n 12`, and 166.6 s and 167.4 s at `-n 4` and `-n auto`. The suite is sleep-bound, as
`pyproject.toml`'s comment on `-n 12` says. A real `ubuntu-latest` run is FR-003's owed criterion.

**Forks.** A plugin counting every fork through `os.register_at_fork` counted 0 over the nightly
selection at seeds 0 to 99 (309 items, `-n 0`). It counted 15 over the sequential half's seeds 0 to
3 (4 items), its control. A soak item has no fork step: `_actor`'s mix is spans, logs, flushes,
sink changes and shutdowns.

**GitHub's documented behaviour**, read 2026-11-16 and 2026-11-17:
- "Notifications for scheduled workflows are sent to the user who last modified the cron syntax in
  the workflow file".
- "In a public repository, scheduled workflows are automatically disabled when no repository
  activity has occurred in 60 days".
- `github.run_number` "does not change if you re-run the workflow run".
- A concurrency group admits "at most one running job or workflow ... at any time", groups are
  repository-wide, and a pending run is replaced by a newer one in its group.
- The default Linux shell is `bash -e {0}`, with no `pipefail`.

`gh run list --branch main` lists `schedule` runs with `headSha` equal to `main`'s head at the
time, and `queue.sh` reads every run on the head sha.

**zizmor.** 1.32.0 `--offline` over the prototype workflow: one finding, `self-repository` at the
`./.github/actions/install-poetry` step, low. That is the same finding it reports at that line of
`ci.yml` and `pip-audit.yml`, and `zizmor.yml`'s comment declines it. `--persona pedantic` adds
nothing.

## Revision history

- **2026-11-24, re-validated against `d9f2f097`**, after SPEC-087 to SPEC-101 landed. Re-checked
  and still true: the workflow names and crons, `ci.yml`'s concurrency group and pins, the
  install-poetry action, the four wrapped names, the 14-item selection at seeds 0 to 1 and 15
  under `-k soak`, `open_item_leads`, `parse_seeds`, the decision file's name, invariant 5's and
  §12's "on request ... gating nothing" wording, §12's CI item, `queue.sh`'s `main_green` states
  and its line 199, and the orphan late-write item, still open with no spec. What changed:
  - **The swap row is dropped, and no `repeat` row replaces it.** SPEC-097 closed its item, and
    p11 now closes its sink twice in 0 of 7 runs. The open `fenced` item was weighed as its
    replacement and declined: its forced record is Shape A's exactly (7 of 7 at `d9f2f097`), so a
    row for it would read a regression of SPEC-097 as known (FR-002). With it go the swap
    predicate, its corpus, its ceiling of 20 and its derivation, the "as first drafted" mutant,
    and FR-001's p11 criterion, struck in place. `Arm.by` and `Take.by` stay, for the record a red
    night is filed from (FR-001). Shape A's record is now red.
  - **The start condition is met.** #393 (`4b7e4b86`) closed the lost-line item. The Overview, the
    Out of Scope bullet and the start condition say so; `worker.py:2667` and `:2668-2673` survive
    only in the `eda19f32` Evidence.
  - **`_arm_and_hand` is gone** (SPEC-098). FR-001's caller list and its wiring criterion name
    `_note_orphan_emit` for the orphan log's arm.
  - **The Overview's counts** are the 6 red in 12,000 measured at `d9f2f097`, every one a
    `late-write` on a caller's thread. ~~The late-write ceiling stays 25~~: struck by the review
    below, which set it to 21.
  - **Line anchors** move to `d9f2f097`, and the Evidence says which commit each measurement is
    from. The prototype patch is said not to apply to `main`, measured.
  - **`tests/soak_ceilings.py` joins FR-003's path filter**: it decides the report's verdict, and
    the 2026-11-17 revision that created it did not add it there.
  - `depends_on` gains SPEC-097, which the known table and the lifecycle lane rest on.

  The fresh-context spec review of `e6491b6b` found two MAJOR and three MINOR issues, all fixed
  here. **MAJOR:** a clearing pull request could still add a `repeat` row explaining Shape A's
  record, since the `fenced` item is open; FR-006 now files such a night with no row,
  `test_no_known_row_explains_shape_a` holds the live table to it, FR-005's *Do NOT build* names
  it, and FR-002's first criterion no longer claims every `repeat` is red. **MAJOR:** a ceiling of
  25 broke FR-002's own "x3 over" for the `d9f2f097` rate; it is 21, true of both rates, and the
  reviewer's Linux stand-in run at `d9f2f097` (1 in 30,000, about 0.5 a night) is recorded, with
  the ceiling said to be loose there and FR-003's budget updated from its window timings.
  **MINOR:** a criterion now holds the labels to the printed record; the Overview no longer calls
  every Shape A record a regression nor reads the six findings as the orphan item's predicate; and
  FR-003's path filter now covers every `tests/` module the soak imports, derived and held by
  FR-004's guard rather than listed by hand.
- **2026-11-17, revised after the spec review**, which found seven MAJOR issues. Andrew's calls,
  recorded as his:
  - **"A red night BLOCKS merging."** This was the opposite of the recommendation in the first
    draft, which kept the queue's existing behaviour and stated its cost. The revision makes the
    block workable: re-runs replay the same seeds (FR-003), a dispatch is bounded so it cannot time
    `main` out (FR-003), a clearing pull request is the way out (FR-006), and the reaping and merge
    consequences are stated (FR-006).
  - **"No auto-opened issue."** The workflow stays read-only.

  The orchestrating session's call: the pull-request trigger stays, and FR-005's fence now says it
  gates a pull request that changes the soak, which partly reverses SPEC-086's "gates nothing".

  Other changes:
  - FR-001's corpus gains rows (a), (b) and (c), and all five rule mutants are reddened.
  - The report's floor now counts a skipped soak item or an unexplained expected failure, and
    every check in it is mutation-tested.
  - The concurrency group takes a `model-soak-` prefix.
  - The lost-line oracle names both unmatched lines and is filed as a §12 item, and its closure is
    this spec's start condition.
  - Each known row has a nightly ceiling.
  - The swap predicate requires every non-writer arm to be the swap's.
  - The late-write predicate's premise is held by a write-site roster.
  - The selection is explicit, at 14 items for seeds 0 to 1.
  - ~~"the failure names which sinks' findings were known"~~ is dropped.
  - FR-005's phrase check reads text joined across lines.
  - The prototype's `seeds` no longer reads the date.
- **2026-11-17, revised after the execution-frame review of FR-006.** That review found every MAJOR
  issue to be one class of defect: the queue read the soak from `main`'s head, so a re-run, a merge
  made while the night ran, a cancelled night or a dispatch cleared or dodged the block. FR-006 is
  restructured rather than patched case by case:
  - only scheduled runs carry a verdict;
  - the newest night with a verdict is read on any sha;
  - every attempt is read through the API;
  - `failure` and `timed_out` block;
  - only the night's own `FILED_NIGHTS` entry on `main` clears it.

  The orchestrating session's call, recorded as its own: `cancelled`, `startup_failure`,
  `action_required` and `skipped` nights carry no verdict, because the block was put to Andrew as
  "a failed night would mean a new suspected data-loss bug". Andrew's rule, recorded: a clearing
  pull request is written only once he has answered its escalation, and there is no acknowledgement
  command.

  Further changes:
  - The ceilings move to `tests/soak_ceilings.py`, outside the clearing files, with their
    derivation held by a test.
  - Paths are read NUL-delimited and compared whole.
  - The hook refuses whenever the verdict blocks, fails open only when it cannot read it, and
    gives a stale branch the rebase message alone.
  - A dispatch is bounded before its seeds are built.
  - The run number is noted as counting every run.

  ~~"The PR queue reads a red night ... a red run of `Model soak` on `main`'s head"~~ is struck.
- 2026-11-16, authored.
