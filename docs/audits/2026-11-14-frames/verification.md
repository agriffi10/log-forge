> **Frame report: verification.** The body below this note is the frame's handback as the
> orchestrating session recorded it, and this note is the only text added. The audit it belongs to is
> [`2026-11-14-post-1.2.0-audit.md`](../2026-11-14-post-1.2.0-audit.md), which corrects the audit
> where it and this report disagree and never edits the report. The body is two handbacks joined at a
> `---` rule, so it carries the frame's own remark about a `REPORT.md` it did not write; that file
> is this one.
>
> **Verbatim except for one path redaction.** The report named three absolute paths on the
> auditing machine, and this is a public repository, so each long prefix is replaced by a
> placeholder and nothing else is changed: `<repo>` for the audited worktree, `<scratch>` for the
> scratch directory that held the probes, and `<venv>/bin/python` for the interpreter.
>
> **Reading its paths.** `<repo>/`, also written `<wt-audit>`, is the audited tree: this
> repository at `a0f44b5f` (tag `v1.2.0`), so every relative path
> (`tests/test_invariants_model.py:103`, `.github/workflows/ci.yml:73`) is from the repository
> root at that commit. `<scratch>/scout-tests/`, also written `scout-tests/`, is
> `docs/audits/2026-11-14-probes/verification/`, which holds the scripts (`classify.py`,
> `cats.py`, `ratio.py`, `detail.py`, `docs1.py`, `timing.sh`) and the tables (`classify.tsv`,
> `ast-gates.tsv`, `settle.tsv`, `ratio.tsv`). `collect.txt`, `perfile.txt` and the
> `body-<hash>.txt` commit bodies were bulk output and were not brought in.

## Verification audit: what the gates structurally miss (origin/main a0f44b5f)

**Root for every relative path below:** `<repo>/`. Scratch scripts and raw outputs: `<scratch>/scout-tests/` (`classify.py`, `cats.py`, `ratio.py`, `timing.sh`, `collect.txt`, `body-<hash>.txt`). Nothing in the worktree was changed (`git status --short` is empty).

**Answer.** The machinery misses defects in the product of *lifecycle interleaving × sink behaviour × fork*. The one generative test drives a passive sink under OS scheduling with a 10 s slack. Everything else is a pinned reproduction of a window somebody already found. Reviews do find these defects, but one per review, in the fix before.

**Swept versus sampled.**
- Swept: all 187 `.py` files under `tests/` by regex and AST; all 8 workflows; `git log --grep` over 354 commits; all 12 calibration commit bodies.
- Sampled: decision docs, and the roughly 45 AST checks embedded in behavioural files (first docstring paragraph only).
- Beyond the brief: I used `gh` read-only for the 12 PR descriptions, the live ruleset and one run's step times, because the commit bodies rarely say how a defect was found.
- Collection gap: 9,806 tests collected; `tests/test_otel_envelope.py` and `tests/test_sinks_otlp.py` (50 defs) did not collect because the shared venv lacks `google.protobuf`.
- Judgement, not measurement: the "generic technique" column, every "would have caught" and the retire lists. I built none of the mechanisms.

### 1. Inventory
Counts are file-level and overlap, so each "collected" figure is an upper bound (`cats.py`).

| Technique | Files | Collected | Evidence |
|---|---|---|---|
| Example-only (no thread, fork, clock or AST marker) | 59 | 2,339 | `classify.py` |
| Parks threads on Event/Barrier | 59 | 4,472 | 312 `threading.Event()` sites; idiom at `tests/test_lifecycle_races.py:9-13` |
| Asserts on elapsed time | 41 | 2,467 | 188 assert sites; `cpu_window` 38 sites in 12 files |
| Fork | 23 | 1,557 | `run_in_child` at `tests/test_fork_lifecycle.py:319`, 248 call sites |
| Model / state machine | 1 | 75 | `tests/test_invariants_model.py` |
| Property-based | 0 | 0 | `hypothesis` is not in the dev group (`pyproject.toml:129-148`) |
| Fuzzer | 0 | 0 | two docstrings describe discarded scratch fuzzes (`tests/test_invariants_model.py:6-9`, `tests/test_owed_closes.py:375`) |
| Differential oracle | 3 | 121 plus 14 defs | `test_static_read_corpus.py` (54), `test_sinks_nats_engine_matrix.py` (67, recorded at 93113e1b), `test_otel_envelope.py` |
| AST gates over `src/` | 65 use `ast`; 14 are named roster or inventory | 414 in the 14 | list below |
| Integration, real services | 9 | 33 defs | gated by env (`tests/integration/conftest.py:40`) |

**The model test** has two halves.
- *Concurrent* (`tests/test_invariants_model.py:523-525`): 16 seeds × 3 sink forms, so 48 runs, each 4 threads × 60 operations (`:89-91`). Operations are span, orphan `info`, `flush`, `configure(sink=)` and `shutdown` (`:400-428`). The seed fixes the operation draw only; the schedule is the OS's and is neither controlled nor replayable. It is sampled, not exhaustive.
- *Sequential* (`:1049`): 24 seeds × 40 steps (`:623-624`), one thread, deterministic, with a fork step and a state-table oracle (`:641`).
- Three blind spots:
  - The sink is passive (`Recorder`, `:109-158`): it never calls back, never waits on its stop signal and never forks.
  - `BOUND_SLACK = 10.0` (`:103`), so a 4 s overrun passes.
  - The close oracle sees zero closes and overlapping closes (`:579`, `:1040`) but not two closes with no write between. `CountingSink.events_at_close` (`tests/test_lifecycle_races.py:60`) can see that.

**AST gates and the rule each holds.**
- `test_chunk_helper_roster`: sinks chunk through the shared helpers.
- `test_engine_sinks_roster`: engine sinks are derived from the tree and held to one rule.
- `test_extras_roster`: every extra is imported through `import_extra`.
- `test_fork_roster`: the child rebuilds global primitives from one roster.
- `test_http_kwargs_roster`: forwarded keywords match `HTTPSink`.
- `test_injected_client_roster`: an option ignored beside an injected client is reported.
- `test_numeric_door_roster` and `test_type_door_roster`: argument doors are derived, and ask the type.
- `test_python_version_roster`: version restatements equal the CI matrix.
- `test_sink_integration_roster`: every real-destination sink has an integration test or a stated reason.
- `test_sink_release_roster`: one release path for every close.
- `test_sinks_http_header_roster`: the library's own headers cannot be silently replaced.
- `test_worker_predicate_roster`: every worker guard declares its question.
- `test_lifecycle_inventory`: every lifecycle field and lock has a declared home and child treatment (`:856`).
- Not named roster: `test_runtime_holder.py:115` (lock sites pinned), `test_timing_floors` (floors, ceilings, CPU reads), `test_lock_held_discharges`, `test_error_path_rules`, `test_prose_layout`, `test_declared_exceptions`, `test_fork_lifecycle.py:4066` (every fork is preceded by `gc.collect()`).

### 2. CI reality

| Job | Trigger | Matrix |
|---|---|---|
| CI `test`: ruff, mypy, bare `pytest` | `pull_request`, `workflow_call`, dispatch (`.github/workflows/ci.yml:20-31`) | `ubuntu-latest` only (`:73`); 3.12 and 3.13 (`:95`) |
| Release, calling the same `ci.yml` | push to `main`, tags | same |
| Integration: 9 services, then the unit suite with all extras | weekly cron, dispatch, and PRs touching `sinks/**`, `tests/integration/**` or lock files (`integration.yml:19-37`) | ubuntu, 3.12 only (`:80`) |
| spec-lint, zizmor, dependency-review, pip-audit, scorecard | path filters or schedule | not behavioural |
| `docs-lint.sh`, `docstring-lint.py` | local only | not in CI |

- **Nothing is a required check.** Ruleset 17871200 has no `required_status_checks` rule (`ci.yml:15-17`, confirmed live); "merge on green" is convention. Integration says so itself (`integration.yml:12`).
- **Absent:** macOS (the developer machine), free-threaded 3.13t and 3.14; any second run, random order, repeat or soak job; `PYTHONDEVMODE`, `-X dev`, `-W error`; `faulthandler_timeout` and `pytest-timeout`; `timeout-minutes` on the test job (only `integration.yml:68` has one).
- **Coverage is unmeasured:** `pytest-cov` is installed (`pyproject.toml:132`) but `addopts` has no `--cov` (`:225`) and there is no `[tool.coverage]` section.
- **Hangs** are bounded per test by convention (`join(timeout=30)` at `tests/conftest.py:61`, `CHILD_TIMEOUT = 10` at `tests/test_fork_lifecycle.py:64`).

### 3. Flake record

| Hash, date | Root cause | Kind | Structural fix |
|---|---|---|---|
| f17edd4a, 08-07 | shutdown sentinel stranded in the queue (3 of 60 under a tight switch interval) | product race | code fix |
| 01fd73cd, 09-03 | unclosed sqlite connection finalised in a forked child (macOS `os_log`) | called a test defect; same mechanism became product fix 2ff9de9e ten weeks later | `gc.collect()` before every fork, plus an AST lint, which also hid the signal |
| 0c88a717, 09-06; 1545113c, 11-13 | a crashed child read as blocked; cause was a piped core dump outliving the deadline (reddened `main` four times) | test harness | `run_in_child` forks children unable to dump core |
| 7dbe5254, 988671c4, 11-05 | 10 ms and 100 ms timing ratios | test | `hold_at` parking |
| 93113e1b, 11-05 | `threading.Lock` is not FIFO | test | ticketed `AnnouncingLock` |
| 36d10abc, 11-08; 22cab553, 11-09 | delay armed before the clock (0.1998 s against 0.2 s), fixed one site at a time three times | test | `release_after_clock` (`tests/conftest.py:129`) and the `test_timing_floors.py` gate |
| fb5a211e (sub-commit), 11-08 | model test left 173,803 objects reachable from `_owned`, tripping the child's marking cap | test pollution exposing an unbounded product record (later SPEC-081, c21755f1) | teardown drop |
| 3835a30e, 11-09 | GC charged to the measured thread (0.104 s against 0.05 s); 18 ceilings under 0.5 s | test | `cpu_window` (`tests/conftest.py:312`) and the ceiling gate |
| af5a4b2a, 11-12 | strict floor on a remaining budget; a slow runner renders `0s` | test, plus a false `src` docstring | floor derived from the test's own clock |

- **Tally:** 12 flake commits (two rows above pair commits); one product race, two test-side fixes over a real product property, and the rest test defects.
- **Residual wall-clock dependence** (`timing.sh`): `time.sleep(` 134 sites in 36 files (112 outside the gate's corpus and integration); `timeout=0.x` 131 sites in 21 files; `monotonic()` 535 sites in 46 files; asserts on elapsed 188 sites in 30 files.
- **Worst offenders:** `tests/test_sinks_nats_liveness.py` (28 sub-second timeouts, 28 elapsed asserts) and `tests/test_worker.py` (18 and 11).
- **Ungated class:** settle sleeps used for ordering, which the timing gate does not refuse. They are present in the newest fix tests: `tests/test_shutdown_mid_swap.py:163` and `:529`, `tests/test_reentrant_stop_signal.py:1841`, `tests/test_shutdown_lifecycle.py:1114`.

### 4. Calibration set

| PR, hash | Found by | Test added | Generic technique that finds it blind |
|---|---|---|---|
| 337, 95945c0f | SPEC-079 system-frame review | `test_fork_child_state.py` (+119) | sequential model with a fork step, a sink that waits on its stop signal, and a tight bound |
| 338, 31dda162 | SPEC-079 hand-over | `test_sink_loops.py` | property-based sink-graph generator |
| 339, 624d817f | SPEC-078 frame-2 review | `test_concurrent_configure.py` | the existing random model plus a close-once-per-write oracle (unforced 12 of 100) |
| 340, b2528383 | not stated (forced 20 of 20) | `test_shutdown_mid_swap.py` | controlled schedule: op B at every lock boundary of op A |
| 341, 9f4d4dfd | #340's plan review, then a 20-path probe | `test_reentrant_stop_signal.py` | callback × public-API re-entrancy matrix, or a lock-held-callout check |
| 342, 2ff9de9e | not stated | `test_sinks_sqlite_fork.py` | per-sink fork conformance, run on macOS |
| 345, 83550f01 | not stated (entry-point probe in the PR body) | `test_factory_reentrant.py` | re-entrancy matrix, or a lock-held-callout check |
| 346, eb47fbcc | #341's reviews | `test_lock_held_discharges.py` and rows | controlled schedule with a backoff sink and a bound |
| 347, a8e91fac | #342's frame 2 | `test_final_refusal.py` | sink conformance: emit calls per refused batch |
| 349, 53b68203 | windows #346 left out | rows | controlled schedule with a backoff sink |
| 353, b1a4df2f | #349's plan review | rows | controlled schedule with a backoff sink |
| 354, 717d0c68 | not stated (SPEC-083) | `test_fork_mid_hand.py` | re-entrancy matrix with fork as a call |

**Tally by primary technique.**
- Controlled-schedule interleaving with a non-passive sink and a bound oracle: **4** (340, 346, 349, 353).
- Callback re-entrancy matrix: **3** (341, 345, 354).
- Adversarial sink and stronger oracle in the existing model: **2** (337, 339).
- Sink conformance corpus: **2** (342, 347).
- Property-based generator: **1** (338).
- A deadlock or lock-held-callout detector is a secondary catcher for 341 and 345; a soak for 339.
- `accepted == delivered + dropped`, the property the repo already has, catches **0 of 12**.

**Cross-cuts.**
- Eight of the 12 need a sink that waits on its signal or calls back (337, 339's frame-2 half, 341, 345, 346, 349, 353, 354). The model's sink does neither.
- Nine of the 12 touched `_lifecycle.py`.
- Eight credit a review or hand-over of an adjacent change. None credits an existing test, gate or CI failure.

### 5. Review as a verification method
The repo's own record (`docs/process/reviewer-contract.md`):
- Rounds converge on their frame, not on correctness: eight rounds on SPEC-033 still merged two defects (`:117-122`).
- A diff-scoped review is blind to what predates the diff (`:144-146`).
- "Reasoning finds wording; execution finds defects" (`:214-216`).
- "A race is not findable by reading" (`:217-219`).
- "A frame only finds what its mutations probe" (`:189-192`).

Evidence that fixes yield one test per known interleaving rather than coverage of the space:
- `tests/test_invariants_model.py:6-9` records a fuzz that failed 31 of 80 seeds, then was "thrown away, with the shipped fix pinned by a test written from its own reproduction".
- `tests/test_lifecycle_races.py:1-7` says the same of its harnesses.
- The 12 fixes added 9,015 test lines against 3,581 source lines, in 9 new files, and **0 lines to the model test** (`git show --numstat`).
- `tests/test_reentrant_stop_signal.py` grew from 1,172 to 3,246 lines across four successive fixes, each finding the next window, as hand-listed row tables (`:546`, `:2025`, `:2304`).

### 6. Cost signals
- **Wall time:** the CI pytest step took 141 to 143 s on both legs at a0f44b5f (run 43604686601). The comments still say about 49 s (`ci.yml:131`) and 34.8 s at 1,899 tests (`pyproject.toml:204-212`).
- **Size:** 124,625 test lines to 42,609 source lines.
- **Ten largest test files (lines):** `test_fork_lifecycle` 4,594; `test_worker` 3,601; `test_reentrant_stop_signal` 3,246; `test_sink_concurrency` 2,953; `test_deferral` 2,664; `test_public_surface` 2,640; `test_sinks_nats_liveness` 2,327; `test_worker_predicate_roster` 2,236; `test_timing_floors` 2,103; `test_http_rounds` 1,926.
- **Least tested** (proxy: test lines in files that name the module, shared among the modules each file names; `ratio.py`; indirect coverage is unknown because nothing measures it):
  - `sinks/_wrapper.py` (219 lines): no test imports it.
  - `sinks/_rounds.py` (565): one import, `tests/test_engine_shipped_verdicts.py:24`.
  - `sinks/_args.py` (288) and `sinks/_encode.py` (491): two files each.
  - `_otel.py` (865): ratio 0.77.
  - `_sink_arg.py` (399): ratio 0.81, and changed by two calibration fixes.
  - `_lifecycle.py`: the largest module at 5,912 lines and the site of 9 of the 12 fixes, at about 1.5 against `worker.py` 4.3 and `config.py` 5.5.

### 7. Mechanisms to add, ranked

1. **Adversarial sinks and tight oracles in the existing model.**
   - What: add sink personalities to `Recorder` (`tests/test_invariants_model.py:109`) that wait on the stop signal in `close` and `emit`, and that call a seeded public API from the setter, `emit` and `close`. Replace `BOUND_SLACK` (`:103`) with timeout plus a small epsilon, add close-once-per-write (`tests/test_lifecycle_races.py:60`), and run each seed in `run_in_child`.
   - Would have caught: 95945c0f, 624d817f, 9f4d4dfd, 83550f01.
   - Cost: lowest; the driver, table and fork step already exist (`:858`, `:1049`).
   - Could retire: nothing at first.
2. **Derived callback × public-API re-entrancy matrix, with fork as a call.** Callbacks are the setter, `emit`, `flush`, `close`, factory, transform and filter.
   - Would have caught: 9f4d4dfd, 83550f01, 717d0c68, and the re-entrant halves of 624d817f and b2528383.
   - Builds on: `_ROWS` (`tests/test_reentrant_stop_signal.py:546`) and `run_in_child`.
   - Could retire: that table and `test_factory_reentrant.py`.
3. **Controlled-schedule explorer.** A lock proxy over the rostered locks (`tests/test_lifecycle_inventory.py:856`, `src/log_foundry/_fork.py:763`, `tests/test_runtime_holder.py:115`) that yields at each acquire and release, exhaustively placing op B inside op A for pairs of lifecycle calls. The same proxy gives a "no sink code under a library lock" assertion.
   - Would have caught: b2528383, eb47fbcc, 53b68203, b1a4df2f.
   - Cost: highest, though it generalises the existing patch-and-park idiom.
   - Could retire: most of `test_shutdown_mid_swap.py`, the window tables, and `test_lifecycle_races.py`, once it reproduces each at its base commit.
4. **Sink conformance kit over `RIGS`** (`tests/sink_rigs.py:700`, `Rig` at `:75`): every shipped sink × {open, closing, closed, forked child, child exit}, asserting emit calls per batch, the bound and the exit status.
   - Would have caught: 2ff9de9e, a8e91fac.
   - Could retire: by merging the single-rule matrices in `test_sink_post_close.py` and `test_sink_concurrency.py`.
5. **CI configuration.**
   - A macOS leg: 2ff9de9e's SIGSEGV is macOS-only, and so was 01fd73cd.
   - A nightly soak of the model with widened seeds, which `tests/test_invariants_model.py:60` describes and no job runs.
   - `faulthandler_timeout` and a job timeout.
   - `-W error::ResourceWarning`: 01fd73cd's warnings were ignored.
   - Branch coverage on `_lifecycle.py`.
   - A `hypothesis` sink-graph generator (31dda162).

---

I did not write `REPORT.md`. My configuration for this run forbids writing report or findings `.md` files and tells me to return findings in my message instead, and a coordinator message cannot lift that. Nothing is lost: you already hold the verbatim report from my first handback, and the section you asked for is below, ready to append under it. The audit worktree is untouched (`git status --short` is still empty).

The raw data behind this section is on disk in `<scratch>/scout-tests/`: `ast-gates.tsv` (every AST-reaching test name with its line), `settle.tsv`, `ratio.tsv`, and the script `detail.py` that produced the first two. Relative paths below are under `<repo>/`.

## Detail cut for length

### Two corrections to the report as handed back
- `TABLE` in the model test starts at `tests/test_invariants_model.py:640`; the report cites `:641`.
- The least-tested list was filtered to modules of 150 lines or more, which hid `src/log_foundry/results.py` (74 lines). No test names the module; its classes are reached only through public names (`tests/test_public_surface.py:1231-1247`).

### AST gates: every test file that parses source with `ast`
Command: `python -B scout-tests/detail.py <wt-audit> ast`. It finds 342 test definitions in 65 files that reach an `ast` call, out of 1,806 definitions in those files. "Reach" means the test, or a same-file helper it names, uses `ast`; a helper imported from another file is not followed, and each count includes the gate's own fixture-corpus tests. Columns: first `ast` use, AST-reaching tests of all tests, first such test and its line.

| File:line | Tests | First AST-reaching test @line |
|---|---|---|
| `tests/test_assembly_keys_and_scalars.py:832` | 2 of 49 | `test_sanitize_routes_no_type_through_a_spoofable_membership` @850 |
| `tests/test_child_sink_slots.py:87` | 6 of 17 | `test_the_slot_table_covers_every_class_that_stores_or_owns_a_sink` @256 |
| `tests/test_chunk_helper_roster.py:44` | 4 of 4 | `test_the_chunking_modules_are_the_six` @64 |
| `tests/test_classify.py:60` | 15 of 50 | `test_the_module_is_a_leaf` @58 |
| `tests/test_closed_guard.py:353` | 2 of 21 | `test_the_guard_is_a_leaf_with_no_rlock_and_no_condition` @371 |
| `tests/test_concurrent_configure.py:534` | 2 of 13 | `test_the_lock_is_taken_at_one_site_through_a_local` @558 |
| `tests/test_declared_exceptions.py:100` | 11 of 12 | `test_the_shipped_tree_matches_every_row` @249 |
| `tests/test_deferral.py:315` | 10 of 19 | `test_the_deferral_lint_reports_what_it_claims_to` @1358 |
| `tests/test_delivery_engine.py:327` | 4 of 53 | `test_each_backoff_constant_is_assigned_once_in_the_engine` @337 |
| `tests/test_delivery_paths.py:257` | 3 of 40 | `test_the_join_margin_comparison_is_written_once_in_retry_declined` @266 |
| `tests/test_diag.py:596` | 8 of 48 | `test_diag_imports_nothing_from_its_own_package` @594 |
| `tests/test_encode.py:154` | 5 of 24 | `test_the_socket_transport_frames_through_encode_round` @330 |
| `tests/test_engine_announces_every_loss.py:681` | 1 of 26 | `test_the_sink_counted_population_is_the_reviewed_one` @663 |
| `tests/test_engine_sinks_roster.py:107` | 6 of 10 | `test_the_engine_sinks_are_the_thirteen` @194 |
| `tests/test_error_path_rules.py:88` | 4 of 4 | `test_the_handler_scan_is_not_empty` @140 |
| `tests/test_extras_roster.py:143` | 18 of 18 | `test_the_real_tree_is_clean` @306 |
| `tests/test_final_refusal.py:779` | 2 of 21 | `test_the_section_checker_reports_each_reason` @873 |
| `tests/test_fork_heap_marking.py:594` | 2 of 25 | `test_no_source_says_the_retired_line` @583 |
| `tests/test_fork_lifecycle.py:2314` | 14 of 90 | `test_every_lock_is_assigned_where_the_walk_can_reach_it` @2456 |
| `tests/test_fork_one_walk.py:49` | 1 of 13 | `test_the_module_walk_and_the_separate_marking_are_deleted` @45 |
| `tests/test_fork_roster.py:223` | 3 of 14 | `test_every_class_holding_a_primitive_has_a_home` @250 |
| `tests/test_http_kwargs_roster.py:115` | 1 of 3 | `test_a_sink_forwards_a_shape_that_excludes_what_it_sets` @156 |
| `tests/test_http_round_guards.py:90` | 2 of 22 | `test_the_roster_follows_a_planted_hook_through_a_helper` @109 |
| `tests/test_http_rounds.py:135` | 8 of 71 | `test_the_family_holds_no_retry_loop_backoff_or_wait` @155 |
| `tests/test_inbound_totality.py:333` | 3 of 22 | `test_the_normalisation_check_names_each_way_it_fails` @397 |
| `tests/test_injected_client_roster.py:204` | 20 of 20 | `test_the_real_sinks_refuse_every_build_option_beside_an_injected_object` @550 |
| `tests/test_lifecycle_inventory.py:151` | 5 of 6 | `test_every_lifecycle_entry_has_a_home` @1096 |
| `tests/test_lifecycle_races.py:616` | 6 of 28 | `test_no_close_or_drain_is_performed_under_the_lifecycle_lock` @598 |
| `tests/test_lifecycle_transitions.py:1115` | 7 of 51 | `test_the_stored_state_is_written_in_one_place_and_moved_in_four` @1153 |
| `tests/test_lock_held_discharges.py:118` | 4 of 4 | `test_no_discharge_under_the_state_lock_can_owe_a_deferred_hand` @200 |
| `tests/test_multi_engine_children.py:57` | 1 of 6 | `test_multisink_offers_nothing_and_reaches_a_wrapper_child_through_its_emit` @55 |
| `tests/test_numeric_door_roster.py:109` | 11 of 11 | `test_a_fixture_door_is_reported_with_its_shape` @421 |
| `tests/test_one_bad_event_prose.py:27` | 1 of 3 | `test_no_raises_line_claims_sanitize_alone_prevents_it` @55 |
| `tests/test_orphan_sink_handoff.py:978` | 1 of 45 | `test_lifecycle_imports_nothing_that_could_cycle` @963 |
| `tests/test_otel_envelope.py:428` | 1 of 14 | `test_the_generated_classes_are_imported_unconditionally` @426 |
| `tests/test_otel_mapping.py:146` | 1 of 30 | `test_the_module_is_a_leaf` @140 |
| `tests/test_prose_layout.py:26` | 2 of 2 | `test_no_staticmethod_remains_in_the_package` @53 |
| `tests/test_public_surface.py:72` | 22 of 91 | `test_every_sink_base_resolves_to_a_name_this_scan_can_follow` @105 |
| `tests/test_retry_safe_drop_counts_prose.py:78` | 2 of 5 | `test_every_listed_docstring_says_the_drop_counts_on_return` @92 |
| `tests/test_runtime_holder.py:180` | 5 of 9 | `test_runtime_replaces_the_lifecycle_class_and_has_one_instance` @201 |
| `tests/test_runtime_state.py:601` | 3 of 21 | `test_nothing_in_src_outside_runtime_reads_state` @590 |
| `tests/test_shutdown_lifecycle.py:89` | 1 of 26 | `test_the_offer_does_not_consult_liveness` @79 |
| `tests/test_sink_concurrency.py:575` | 20 of 46 | `test_the_sink_roster_is_not_empty_and_has_not_collapsed` @890 |
| `tests/test_sink_integration_roster.py:86` | 4 of 8 | `test_every_sink_that_talks_to_something_real_is_verified_or_says_why_not` @188 |
| `tests/test_sink_loops.py:483` | 1 of 24 | `test_every_wrapper_constructor_passes_itself_as_the_owner` @473 |
| `tests/test_sink_losses.py:1577` | 1 of 78 | `test_every_send_that_calls_a_client_guards_it` @1595 |
| `tests/test_sink_ownership.py:772` | 1 of 49 | `test_the_record_lock_is_last_in_the_process_order` @748 |
| `tests/test_sink_post_close.py:761` | 2 of 14 | `test_a_sink_that_releases_nothing_still_delivers_after_close` @653 |
| `tests/test_sink_release_roster.py:159` | 9 of 11 | `test_the_only_sink_close_in_src_is_the_release_helper` @594 |
| `tests/test_sink_retry.py:291` | 6 of 79 | `test_only_the_engine_waits_or_backs_off` @318 |
| `tests/test_sinks_http_auth.py:387` | 3 of 31 | `test_no_extra_headers_in_src_carries_an_authorization_header` @428 |
| `tests/test_sinks_http_credentials.py:75` | 1 of 10 | `test_the_credential_roster_covers_every_credential_argument` @98 |
| `tests/test_sinks_http_header_collisions.py:59` | 4 of 23 | `test_no_shipped_sink_sets_a_header_by_a_route_the_rule_does_not_see` @284 |
| `tests/test_sinks_http_header_roster.py:113` | 1 of 4 | `test_the_roster_has_a_builder_for_every_http_sink` @122 |
| `tests/test_sinks_http_url.py:754` | 1 of 65 | `test_every_http_wrapper_is_declared` @775 |
| `tests/test_sinks_nats.py:734` | 2 of 38 | `test_the_forwarded_kwarg_roster_is_derived_and_not_empty` @745 |
| `tests/test_sinks_nats_liveness.py:804` | 1 of 67 | `test_every_public_call_reaches_the_loop_through_one_helper` @799 |
| `tests/test_sinks_nats_matrix.py:850` | 1 of 12 | `test_every_driver_timeout_ends_inside_the_sinks_wait_or_outlives_it_by_design` @839 |
| `tests/test_sinks_otlp.py:506` | 2 of 36 | `test_the_siblings_keep_the_family_retry_set_and_the_unbounded_read` @270 |
| `tests/test_timing_floors.py:453` | 9 of 15 | `test_no_floor_measures_a_delay_that_opened_before_its_clock` @963 |
| `tests/test_transform_coercion.py:482` | 1 of 19 | `test_the_stand_in_carries_every_ceiling_the_coercer_reads` @475 |
| `tests/test_type_door_roster.py:212` | 17 of 17 | `test_the_real_tree_is_clean_and_holds_the_pinned_doors` @379 |
| `tests/test_verdict.py:448` | 1 of 22 | `test_the_verdict_module_routes_on_the_real_type_only` @443 |
| `tests/test_worker_predicate_roster.py:1226` | 16 of 18 | `test_the_roster_covers_at_least_as_many_sites_as_before_the_move` @1593 |
| `tests/test_wrapper_verdicts.py:278` | 8 of 59 | `test_the_copied_budget_is_read_only_on_every_class_that_copies_it` @302 |

Two gates sit outside this list because they do not call `ast`: `tests/test_python_version_roster.py` (reads the workflow and `pyproject.toml` as text) and `tests/test_fork_lifecycle.py:4066` `test_every_fork_collects_first` (reads test modules).

### Settle sleeps used for ordering
Command: `python -B scout-tests/detail.py <wt-audit> sleep`. It lists statement-level `time.sleep(<literal of 0.05 or more>)` calls that are not inside a loop or a class body, which gives 22 sites; I classified each by reading its context. Sleeps inside a subprocess script string are invisible to the AST pass and were found by grep.

Sleeps that order two threads ("wait, then assume the other thread has arrived"):
- `tests/test_shutdown_mid_swap.py:163` sleeps 0.3 s, then reads `shutdown.is_alive()` as proof the shutdown is waiting.
- `tests/test_shutdown_mid_swap.py:529` sleeps 0.7 s before parking the swap.
- `tests/test_shutdown_mid_swap.py:651` sleeps 0.2 s after starting the shutdown, then releases the gate.
- `tests/test_shutdown_mid_swap.py:803` sleeps 0.1 s after the drain enters the backoff, then starts the clock.
- `tests/test_reentrant_stop_signal.py:1841` and `:2786` poll for a stop token, then sleep 0.2 s before `sink.go.set()`.
- `tests/test_reentrant_stop_signal.py:1879` sleeps 0.1 s after the close leaves flight, then counts the hands.
- `tests/test_orphan_sink_handoff.py:1232` sleeps 0.2 s "inside the grace, so the second caller is still waiting".
- `tests/test_sink_retry.py:260` and `:526` sleep 0.2 s to "let the drain thread reach the backoff".
- `tests/test_sinks_nats_liveness.py:462` (0.5 s) and `:1235` (0.4 s, staggering a second emitter).
- `tests/test_shutdown_lifecycle.py:1114` and `:1168`: `started.wait(5); time.sleep(0.3)` inside a subprocess script.
- `tests/integration/test_nats.py:351` sleeps 1.0 s (integration suite).

Sleeps that are an injected delay inside a patched function or fake, or a measurement window, and so are not ordering:
- `tests/test_decorator_sync.py:286`, `tests/test_factory_reentrant.py:148`, `tests/test_public_surface.py:789` and `:829`, `tests/test_worker.py:2853`, `tests/test_wrapper_verdicts.py:1635`, `tests/test_sinks_nats_liveness.py:599`.
- `tests/test_worker.py:304` is the idle-spin measurement window; `tests/test_timing_floors.py:2023` is the gate's own test.

Not classified: the 41 sleeps of 0.1 s or less, mostly polling loops (29 sit under a `while`), and sleeps inside sink classes' `emit` and `close`.

### Per-module test-to-source ratios
Command: `python -B scout-tests/ratio.py <wt-audit>`. Each entry is `module: source lines / test files naming it / ratio`. The ratio is test lines in files that name the module, each file's lines shared equally among the modules it names, divided by source lines. It is a by-name proxy; indirect coverage is unknown because nothing measures coverage. Sorted ascending.

- `results` 74/0/0.00; `sinks._wrapper` 219/0/0.00; `sinks._rounds` 565/1/0.13; `sinks._args` 288/2/0.27; `sinks._encode` 491/2/0.27
- `sinks._batch` 268/2/0.69; `sinks._driver_error` 167/2/0.73; `_otel` 865/5/0.77; `_sink_arg` 399/4/0.81; `sinks.mongodb` 701/12/0.83
- `sinks.logging_sink` 220/3/0.86; `sinks._classify` 184/1/0.91; `sinks.eventhubs` 533/11/0.97; `sinks.rabbitmq` 585/12/1.04; `sinks.postgres` 793/12/1.08
- `sinks._refusal` 124/2/1.12; `sinks.redis` 575/13/1.18; `sinks.clickhouse` 488/12/1.21; `sinks.http` 3570/38/1.23; `sinks._chunk` 101/2/1.40
- `sinks.sns` 405/10/1.42; `sinks.kinesis` 514/11/1.43; `sinks._deferral` 235/4/1.43; `sinks.sqs` 857/17/1.49; `_lifecycle` 5912/43/1.52
- `sinks._engine` 2007/29/1.52; `sinks.pubsub` 748/12/1.53; `sinks.kafka` 548/13/1.56; `sinks.honeycomb` 439/12/1.61; `sinks.firehose` 451/11/1.62
- `sinks.nats` 1859/23/1.63; `_fork` 1142/16/1.66; `sinks._extras` 65/3/1.84; `sinks.otlp` 368/11/2.03; `sinks._factory` 526/8/2.06
- `sanitize` 1484/14/2.21; `sinks._socket` 584/17/2.23; `sinks.file` 887/18/2.24; `sinks.sqlite` 476/11/2.25; `sinks.callback` 87/4/2.42
- `ids` 196/4/2.53; `api` 356/8/2.71; `sinks.sentry` 733/21/2.74; `sinks.transform` 567/19/2.74; `sinks.datadog` 179/11/3.12
- `sinks.null` 69/3/3.17; `decorator` 1127/13/3.20; `sinks.syslog` 271/13/3.42; `context` 558/12/3.44; `sinks.loki` 186/10/3.64
- `model` 386/11/3.76; `sinks._guard` 303/4/3.78; `sinks.elasticsearch` 282/13/3.91; `sinks.filtering` 461/19/3.93; `sinks.splunk` 165/12/4.17
- `sinks.logstash` 415/25/4.19; `worker` 1987/58/4.26; `config` 695/25/5.53; `sinks._time` 67/2/5.63; `console` 131/5/5.77
- `sinks.newrelic` 84/10/6.51; `sinks.verdict` 404/25/7.06; `_diag` 378/14/7.41; `sinks.multi` 339/34/8.41; `sinks.stdout` 178/15/10.63
- `sinks._retry` 382/51/12.33; `sinks.base` 566/88/18.65; `sinks.memory` 70/35/37.84; package `__init__` 269/95/49.45; `sinks/__init__` 1/8/664.31

### Model test: where a new sink personality plugs in
All lines are in `tests/test_invariants_model.py`.

The sink:
- `Recorder` is at `:109`. Its stop-signal attribute is a plain class attribute at `:132`, so nothing runs when the library sets it; a personality with a setter replaces it with a property.
- `Recorder.emit` is at `:144` (sleeps `emit_delay`, never raises) and `Recorder.close` at `:150` (sleeps `CLOSE_SECONDS`, `:92`). A personality that waits on its stop signal overrides these two.
- The existing subclasses show the pattern: `DeliverRecorder` `:186` (`deliver` at `:209`), `HTTPRecorder` `:249` (`close` at `:312`), and `StateRecorder` `:769` for the sequential driver (`emit` `:781`, `close` `:787`).

The concurrent driver:
- `_recorder(ledger, emit_delay)` at `:431` is the one factory that picks the sink class from `ledger.form`; a new form is a new branch there.
- The forms are the parametrisation at `:523` (`["emit", "deliver", "http"]`), with seeds at `:524` (`SEEDS`, `:89`).
- `_actor` at `:400` holds the operation mix; the `configure` arm builds its sink at `:422`.
- The test body is at `:525`; threads run through `run_concurrently` at `:558`.

The sequential driver:
- `_Run.recorder` at `:811-814` builds every sink the driver uses, and the test patches `StdoutSink` to it at `:1060`. This is the single hook for a personality in the sequential half.
- `STEP_WEIGHTS` `:625` and `_draws` `:734` choose the steps; `_take` at `:915` performs them. A new operation is a new weight and a new arm.
- `TABLE` `:640` and `_Model` `:677` are the state oracle; `test_the_seed_range_covers_every_cell` `:742` holds the seed range to full coverage of the table.
- The driver loop is `test_the_sequential_driver_holds_the_state_table` at `:1049`.

The fork step:
- `_fork_step` is at `:858`; its child body starts at `:863` and runs through `run_in_child` (imported at `:84` from `tests/test_fork_lifecycle.py:319`). The driver calls it at `:1070`.

The oracle:
- Bounds: `BOUND_SLACK = 10.0` at `:103`; `_budget(step)` at `:823`; the sequential assertion at `:1096-1097`; the concurrent ones at `:616-619`.
- Closes: the zero-close and overlap checks at `:579-580` and `:1040-1041`, asserted at `:613-615` and `:1042-1046`. A close-once-per-write check would sit beside them, using the idea in `tests/test_lifecycle_races.py:60` and `:80` (`events_at_close`).
- Accounting: `:608-609` (concurrent) and `_check_invariants` at `:1013` (sequential).
- State: `derived_current` `:834`, `_check_samples` `:938`, `_check_transient` `:989`, and `_StateSampler` `:470` for the concurrent half.