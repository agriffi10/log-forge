# Completed Spec — SPEC-091: An Open Span's Events Ship on the Worker's Interval

## What was completed?

- **The registry and the take** (FR-001), in `decorator.py`: `_open_spans`, written by `_begin`
  after the push and removed first by `_close_span` and `_abandon`; `Span.held`; and
  `_sweep_registered`, which takes each registered span's buffer past `held` in place, one
  hand-in per span, debiting a take whose hand-in raised to `in_span_lost`. `Worker.submit` is
  split into `_enqueue` and `_announce`, so no library line is written inside the hold.
  `backfill_baggage` iterates a snapshot.
- **The re-entry guard** (FR-001, revised 2026-11-17): `_sweep_lock` is an `RLock` entered only
  through `_sweep_hold` (`_SweepHold`, `.within(wait)` via `_BoundedHold` and `_NotHeld`); its
  exit, `_release_hold`, drains `_deferred_closes` (`_drain_deferred_closes`) before the release
  and announces after it. The five sites ask `_sweep_held_here()` first.
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

**Decided in the build, beyond the spec's text** (each read in `git diff 548155e4..459d1592 -- src/`):

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
`548155e4` collects 11,721 ids and the build 11,797; four exist only at the base, three renamed
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

On the build at `459d1592`, run from `docs/audits/2026-11-14-probes/core-pipeline/` on a shared
macOS machine (10 cores). One-minute load averages: about 39 at the start of the 3.13.1 runs and
137 at the start of the 3.12.12 runs. Pairs read 3.13.1, then 3.12.12.

| Probe | Before (`a2cd4b36`, the spec's Evidence) | Build |
|---|---|---|
| `p12_kill.py`, `SIGKILL` 3.5 s into the root span | 0 of 51 | 50 of 51 on both, the held `span.start` missing; children 9 of 9 |
| `p12_kill.py`, `SIGTERM` after five closed spans | 0 of 15 | 0 of 15: queued, not open |
| `spec091_exactly_once.py`, 5 runs each | — | `dup=0 missing=0 bad_boundary=0 end_not_last=0`; 6,319 to 9,129 and 4,142 to 5,352 shipped before their close; `swap` 5 runs each, `dup=0 missing=0 bad_boundary=0` |
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

## Residuals and known gaps

- **Accepted, in `architecture.md` §13:** an asynchronous `BaseException` between a successful put
  and `_enqueue`'s return in `flush()`'s registry sweep counts that take twice, queued and in
  `in_span_lost`; a collector callback's own stderr lines and orphan emits inside a hold are
  written there; whether the deferred-close drain runs before or after the release is not
  distinguishable by a single-thread test, and the gate checks only that `_release_hold` is its
  one caller; the free-threaded case is unproven.
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

Full suite on CPython 3.13.1, exit 0 in three runs: 11,745 passed, 39 skipped, 13 xfailed. Not
run locally on 3.12, where no environment has the test dependencies; CI's matrix runs it. The
probes ran on both. The completion's gates, `gen-index.sh`, `spec-lint.sh`, `docs-lint.sh` and
`docstring-lint.py`, ran on this branch.
