# Completed Spec — SPEC-091: An Open Span's Events Ship on the Worker's Interval

## What was completed?

- **The registry and the take** (FR-001), in `decorator.py`: `_open_spans`, written by `_begin`
  after the push and removed first by `_close_span` and `_abandon`; `Span.held`; and
  `_sweep_registered`, which takes each registered span's buffer past `held` in place, one
  hand-in per span, debiting a take whose hand-in raised to `in_span_lost`. `Worker.submit` is
  split into `_enqueue` and `_announce`, so no library line is written inside the hold.
  `backfill_baggage` iterates a snapshot.
- **The re-entry guard** (FR-001, revised 2026-11-17): `_sweep_lock` is an `RLock` entered only
  through `_sweep_hold` (`_SweepHold`, `.within(wait)` via `_BoundedHold` and `_NotHeldError`); its
  exit, `_release_hold`, swaps the buffers of `_deferred_closes` (`_drain_deferred_closes`) before
  the release and submits them after it. The five sites ask `_sweep_held_here()` first. No lock but
  the queue's mutex is taken under the hold, and that one only for at most `_SWEEP_WAIT`
  (`Worker._offer`).
- **The worker sweeps** (FR-002, FR-003): `_begin` builds the worker before its credit;
  `Worker(sweep=)`, typed by the protocol `worker._Sweep`; `_run_sweep` and `_hand_in`; the drain's
  own clock with `_SWEEP_WAIT` (0.05 s); one sweep at the head of `_final_drain`; `flush()`'s
  registry sweep in `_lifecycle._sweep_every_registered_span`; and the child's
  `_forget_open_spans_after_fork`, registered just before `_rebuild_worker_after_fork`.
- **The ledger and harnesses** (FR-004): the hand-over credit at the two new sites, the span
  identities in `tests/test_invariants_model.py` and `tests/model_oracles.py` restated over
  `span.end` events (`model_sinks.span_ends_queued()`), `_flush`'s empty guard, and the
  `empty-submission` pin removed.
- **The record** (FR-006): the decision *An open span's events ship on the worker's interval* and
  three refined in place (`docs/decisions/pipeline/`); `architecture.md` §2, §4, §9, §12 and §13;
  the README; `ARCS.md`'s two markers; `docs/release-notes/v1.2.1.md`.
- **Tests**: `tests/test_open_span_sweep.py` (FR-001 to FR-005's criteria, forced interleavings
  through `list` subclasses) and `tests/test_span_events_site_rule.py` (the AST site rule and the
  hold-site gate, each with a fixture corpus).

**Decided in the build, beyond the spec's text** (each read in SPEC-091's diff of `src/` against
its base `33a44e99`):

- `continue_trace`'s check and re-parent live in `_adopt_unswept`, which is the hold site the
  gate's roster names; it returns the refusal and the caller writes the line after the release.
- `_flush_worker` treats only an explicit `False` from `_sweep_open_spans` as not swept.
- `_abandon` removes the span from the registry and is a hold site: under a nested hold it marks
  the span and defers its take and debit, returning 0.
- Both sweeps bind the thread's ledger cell **before** taking the hold, so a first credit's
  registration lock never nests inside it.
- A deferred close that finds no worker, which no path reaches, is counted in `in_span_lost`.
- Under a backlog a carry right after a sweep restarts the time trigger, so a swept submission
  waits behind the backlog; FR-002's one-interval bound is a drain's with nothing waiting
  (`Worker._drain`'s docstring).
- The `lf` fixture is unchanged. An instrumented count found no worker-sweep take during an `lf`
  test in three full runs (one-minute load averages about 55 to 90).
- `tests/conftest.py::_reset_worker` also clears the registry and the deferred closes, for
  isolation; no failure was traced to their absence.

## The suite, re-measured on the tree with SPEC-088 to SPEC-090

The spec's Evidence lists were the prototype's against `a2cd4b36`. On the build, the base
`33a44e99` collects 11,721 ids and the build 11,797; four exist only at the base, three renamed
tests and one id carrying a line number. The existing tests that failed before they were
rewritten: up to 19 rows of `test_invariants_model.py::test_the_accounting_identity_holds_under_every_interleaving`
and, **new against the Evidence**, its `test_the_soak_is_wired`; the eight `empty-submission`
cells (`test_model_oracles.py` now reads 12 pinned, 168 held, 457 unpinned, 469 scheduled); the
three premises the Evidence named in `test_span_sweep.py` and `test_lifecycle_transitions.py`; the
three rosters it named (`test_worker_predicate_roster.py`'s floors 101 to 104 for `_lifecycle`
and 4 to 15 for `decorator`). Also **new against the Evidence**, each from SPEC-088 to SPEC-090
or the guard: `test_span_sweep.py::test_the_close_path_detaches_under_the_same_lock_as_the_sweep`
(its lock wrapper gains `_is_owned`), `test_event_ledger.py::test_shutdown_inside_a_traced_function_balances`,
`test_backlog_batching.py::test_no_public_signature_changes` (`sweep=`),
`test_inbound_totality.py`'s rejection text, and SPEC-089's `open_spans == 2` test, rewritten
as FR-004 says (`sink_lost == 2`, `open_spans == 1`).

## Measurements

On SPEC-091's build over `33a44e99`, run from `docs/audits/2026-11-14-probes/core-pipeline/`
on a shared macOS machine (10 cores). One-minute load averages: about 39 at the start of the 3.13.1 runs and
137 at the start of the 3.12.12 runs. Pairs read 3.13.1, then 3.12.12.

| Probe | Before (`a2cd4b36`, the spec's Evidence) | Build |
|---|---|---|
| `p12_kill.py`, `SIGKILL` 3.5 s into the root span | 0 of 51 | 50 of 51 on both, the held `span.start` missing; children 9 of 9 |
| `p12_kill.py`, `SIGTERM` after five closed spans | 0 of 15 | 0 of 15: queued, not open |
| `spec091_exactly_once.py`, 5 runs each | — | `dup=0 missing=0 bad_boundary=0 end_not_last=0`; 6,319 to 9,129 and 4,142 to 5,352 shipped before their close; `swap` 5 runs each, `dup=0 missing=0 bad_boundary=0`, with `end_not_last` 30 to 101: with `swap` the events of one span reach two sinks whose batches interleave, so order across them means nothing, and the probe's docstring says those two counts do not apply in that mode (FR-001 AC-1, revised 2026-11-17) |
| `spec091_latency.py 6`, idle and `busy` | every sample at the 5 s cap | max 1.001 s and 1.000 s; 1.000 s and 1.000 s |
| `spec091_fork.py` | — | no `child` row on both |
| `spec091_exit.py` `exit` / `inside` / `inside-warm` | 0 / all 8 / 0 of 8 | 5 / 5 / 5 of 8, `after_shutdown=1` in the latter two, on both |
| `spec091_continue_trace.py`, 20 trials | — | `split_spans=0`, `refused=6` and `8` (the prototype refused none; the 1 ms interval sweeps between the logs and the call, FR-003's timing dependence) |

**Cost** (FR-005), `spec091_hot_path.py`, best of 7. **These were taken under heavy load and are
noisy, not precise**: `isolated` read 2 to 6 times the spec's quiet figures, and one `close` run
put the check and the `RLock` below a bare `Lock`. `isolated`: register 82.6 and 125.3 ns,
register and remove 232.1 and 271.1 ns, worker read 3.3 and 17.5 ns, three-event snapshot 106.2
and 160.6 ns. `hold`, 10, 100 and 1,000 spans of ten events: 13.5, 123.2 and 554.7 µs; 13.6,
121.1 and 633.8 µs. `close` (bare `Lock`, `RLock`, check and `RLock`, check and hold): 174.2,
185.1, 133.8 and 434.8 ns; 271.3, 209.4, 241.4 and 248.5 ns; an earlier 3.13.1 run at a load of
144 read 129.2, 169.0, 139.5 and 176.6 ns. A quiet-machine re-run would make them comparable to
the spec's. `whole` was not run on the build.

## Diff review, frame 2 (the system frame)

Four findings, each fixed on the branch and each with a test that its mutant reddens:

- **A sweep overflowed the queue once about 10,000 spans were open.** One submission per span,
  `max_queue` 10,000 submissions, filled in one pass under the hold: `r_many_spans.py 12000 3` read
  `delivered=54000 expected=60000 dropped=6000 overflow_lost=6000`, where the code before SPEC-091
  delivered all 60,000. A sweep now offers each take through `Worker._offer`, which never drops: a
  full queue puts nothing, the take goes back on its span at `held` and the pass stops, crediting
  nothing. On SPEC-091's build over `33a44e99` the same run reads `delivered=60000 expected=60000
  dropped=0 overflow_lost=0 unaccounted=0`. FR-001's `dropped` sentence and FR-004 AC-3 and AC-5
  are revised in place. Tests: `test_a_sweep_into_a_full_queue_leaves_every_take_on_its_span`,
  `test_a_sweep_stops_at_capacity_and_the_rest_ship_at_their_close` (twelve spans, five free slots)
  and `test_a_flush_whose_registry_sweep_meets_a_full_queue_leaves_the_events`. Mutants: no restore
  on a full queue, and no capacity check, each killed.
- **A deadlock across threads between the hold and the queue's mutex.** The drain held the hold
  while `put_nowait` took the queue's mutex, and a thread inside that mutex's section that took the
  hold waited on the drain. `Worker._offer` is `queue.Queue.put`'s body with the mutex acquired for
  at most `_SWEEP_WAIT`, and on a timeout the take goes back on its span. The worker's `_lock`, a
  fault's `in_span_lost` debit and a deferred close's submit, the other locks a hold took, now run
  after the release. Tests: `test_a_sweep_gives_up_on_a_queue_mutex_another_thread_holds`,
  `test_an_own_sweep_that_cannot_take_the_queue_mutex_puts_the_buffer_back` and
  `test_a_deferred_close_is_submitted_with_no_hold_held`. Mutants: an unbounded mutex wait (the
  test reddens, then the deadlocked threads hang its teardown, which is the defect), no restore
  in the own sweep, and the deferred submit moved under the hold, each killed. The reviewer's
  `r_mutex_cycle.py` still reports a hang, and its stacks show why: its thread closes a traced span
  **inside** the queue's mutex, so that close's own submit waits on a mutex its thread already
  holds, with the hold free. That hang is `queue.Queue`'s and predates SPEC-091 (a traced close
  inside the queue's mutex does not return on the base `33a44e99` either); it is recorded in
  `architecture.md` §13.
  > **Superseded by SPEC-097**: the queue's mutex is an `RLock`, so a traced close made inside it
  > re-enters it and returns; the §13 residual is struck (`docs/architecture.md` §13).
- **A skipped sweep was retried on every pass**, so a held hold slowed the drain to about twenty
  submissions a second: `r_hold_backlog.py 200`'s in-hold `flush()` returned after 11.46 s. A skipped
  sweep now waits an interval from the end of its attempt; the same run returns `abandoned` in
  0.05 s. Test: `test_a_skipped_sweep_does_not_slow_the_drain_behind_a_backlog`; its mutant, retry
  every pass, killed.
  > **Superseded in part by SPEC-097**: a public `flush()` made inside a sweep hold now returns
  > `"re-entrant"` at once (Andrew, 2026-11-18); the test times the drain's bound with the marker
  > wait alone (`docs/decisions/pipeline/an-open-spans-events-ship-on-the-workers-interval.md`).
- **`flush()` skipped the registry sweep when the open-time build had failed**, reporting success
  over another thread's 5 buffered logs (`r_no_worker_flush.py`). It now builds the worker when a
  registered span holds events, as the own sweep does; the same run delivers the 5 logs before the
  close. Test: `test_a_flush_with_no_worker_builds_one_for_another_contexts_span`; its mutant killed.

**Decided here, beyond the review's text:** the caller's own sweep in `flush()` keeps SPEC-089
FR-003 AC-3's overflow on a full queue, dropping and counting its buffer as before, with the count
now made after the hold; only a busy mutex makes it put the buffer back. Making it never drop too
would change SPEC-089's criterion, so it was left for the coordinator.

The probes on the fixed build: `spec091_exactly_once.py` three runs, `dup=0 missing=0
bad_boundary=0 end_not_last=0` with 5,415, 8,134 and 7,681 shipped before their close;
`p12_kill.py` 50 of 51 and 9 of 9, `SIGTERM` 0 of 15; `spec091_exit.py` 5, 5 and 5 with
`after_shutdown=1` in the latter two; `spec091_fork.py` no child row.

## Diff review, execution frame

The extra review in the execution frame found the mechanism correct, `Worker._offer` matching
`queue.Queue.put` on 3.12 and 3.13 in a differential test and the ledger exact under stress, and
five findings, each fixed with a test its mutant reddens:

- **A full queue at the stop left an open span's logs neither delivered nor counted**
  (`exit_full.py`: 0 of 5, `open_spans=6`, no counter). The exit drain's one sweep now hands in past
  `max_queue` (`Worker._hand_in_at_exit`, `_offer(past_capacity=True)`), since that drain is the
  queue's only consumer and fixes its bound right after; the mutex bound is kept. FR-003 says so.
  Test: `test_the_exit_sweep_ships_an_open_span_past_a_full_queue`; the mutant that keeps the
  capacity check at exit is killed.
- **Neither restore position was pinned.** Appending a refused take, or the own sweep's refused
  buffer, instead of restoring it at `held` or at the front survived the suite while breaking
  `span.end`-last. Tests: `test_a_refused_take_goes_back_before_what_the_owner_appended` and
  `test_an_own_sweep_puts_its_buffer_back_before_a_sharing_threads_append`, each asserting the
  delivered order; both append mutants are killed.
- **The `break` after a refused offer was not pinned.** With 20 open spans and the queue's mutex
  held elsewhere, continuing past a refusal held the hold about a second, against about one
  `_SWEEP_WAIT`. Test: `test_a_refused_offer_stops_the_sweep_so_the_hold_is_soon_free` (under
  0.5 s); the `continue` mutant is killed.
- **The prose claimed more than the mutex bound does.** `Worker._offer`'s docstring,
  `_sweep_registered`'s and FR-001 now say what it does: it keeps a sweep from waiting for ever on a
  thread inside the queue's section, so a thread there that then takes the hold gets it, and it
  does not rescue a span closed inside that section.
- **The mutex test hung its teardown under its own mutant.** Thread A now takes the hold with a
  bounded wait and lets go of the mutex on expiry, so the unbounded-wait mutant fails on the
  test's assertion (exit 1) instead of deadlocking teardown.

## Residuals and known gaps

- **Accepted, in `architecture.md` §13:** an asynchronous `BaseException` between a successful put
  and `_offer`'s return in `flush()`'s registry sweep counts that take twice, queued and in
  `in_span_lost`; a collector callback's own stderr lines and orphan emits inside a hold are
  written there; a traced close made while its own thread holds the worker queue's mutex never
  returns, as before SPEC-091; the free-threaded case is unproven.
- **Known gap:** one instrumented suite run reported a second failure whose name was not captured
  (the instrumentation itself fails `test_stored_name_roster.py`). Three un-instrumented full runs
  and six repeats of the new and changed test files were clean.

## What changed from earlier specs?

- SPEC-036 FR-001's bound, the calling context, is widened to every registered span; SPEC-089's
  "events on other threads' open spans are swept by no `flush()`" is superseded, and its test
  inverted.
- The worker's build moves from a span's close to its open, so a `shutdown()` inside the first
  traced call now retires a worker that exists.
- SPEC-024's `continue_trace` refusal now depends on timing, with a new rejection line and a
  second one for a nested call; §13's "`Span.swept` is not a synchronization primitive" is closed.
- SPEC-085's `empty-submission` pin is removed; §12's item is under Resolved.
- `Worker.submit` is `_enqueue` then `_announce`; `_count_undeliverable` returns its total and
  writes nothing.

## Verification

Full suite on CPython 3.13.1, exit 0 in three runs on SPEC-091's build over `33a44e99`: 11,745
passed, 39 skipped, 13 xfailed. After the diff review's first frame added four tests and the
branch moved onto `dd77b3fb`: 11,894 passed, 39 skipped, 13 xfailed, exit 0. Not
run locally on 3.12, where no environment has the test dependencies; CI's matrix runs it. The
probes ran on both. The completion's gates, `gen-index.sh`, `spec-lint.sh`, `docs-lint.sh` and
`docstring-lint.py`, ran on this branch.
