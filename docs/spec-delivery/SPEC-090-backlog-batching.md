# Completed Spec — SPEC-090: Under a Backlog the Worker Sends Larger Batches, and the Exit Drain Sends Its Backlog in Batches

## What was completed?

- **The rule** (FR-001), in `worker.py`: `_take` (the hold first, then join or carry), `_complete`,
  `_size_target` and `_set_target`, measured around `_emit_pending`, with the private cap
  `_BACKLOG_BATCH_CAP = 1_000` and a sink generation `retarget` bumps. A carry emit restarts the
  time trigger's interval, which the plan review found and the spec gained on 2026-11-16. Ten new
  `Worker` fields, each a row in `tests/test_lifecycle_inventory.py`; all but `_sink_generation`
  are reset in a forked child.
- **The exit drain** (FR-002, FR-003): `_final_drain` continues `_drain`'s batch, takes at most
  what was queued when it began, and sends it through `_take` and `_complete`. Both drains answer a
  marker only through `_answer_marker`. `_emit_pending` declines a later exit batch unattempted
  through `_decline_exit_batch`; `_write_exit_tallies` writes the declined and abandoned tallies
  from `_final_drain`'s `finally`, from `stop()`'s expiry branch before its line, and from `_run`'s
  outer `finally`. An abandoned exit batch leaves the size target as it was, as a declined one
  does, which the second diff review found: resetting it had cut a dead sink's exit backlog into
  ten-span batches, each writing the sink's own line, 15,217 requests and 953 lines for a refusing
  `HTTPSink` with 10,000 spans at exit, now 321 and 22, against 321 and 21 before SPEC-090.
- **The sinks** (FR-004): `tests/test_batch_bound_roster.py` derives every public sink from the
  tree against a table of how it takes a batch, hands every `RIGS` sink and the HTTP family 1,000
  events, and runs a JetStream `NATSSink` behind a slow ack. No sink's code changed; `nats.py`'s
  prose did.
- **The record** (FR-005): the decision *A backlog drains in larger batches, and capacity is not a
  setting* (`docs/decisions/pipeline/`); the exit-drain decision in `docs/decisions/sink-lifecycle/`
  rewritten in place, its file name kept; §12's findings 9 and 10 struck and the `NATSSink`
  residual filed, then a second residual, a dead sink whose target is already zero at the stop
  (`spec090_dead_http_exit.py zero`: 16,016 requests and 1,004 lines, against 336 and 23); the README; and `docs/release-notes/v1.2.1.md`.

## Premises that had moved

The spec's `file:line` premises were written at `3e27a0a6`, before PRs #378 to #383; the plan
re-read each at `75e7bf1e` and every one had moved or held. Two were false. FR-002's expiry-line
figure was already SPEC-088's `in_flight`, which `_hold` and `_settle` keep, so it was owed a test
and no code. A pending `docs/release-notes/v1.2.1.md` existed, so the notes went there rather than
under a "Release notes owed" heading. The record reached sites FR-005's table did not name; the
spec's revision history lists them.

## The probes against each criterion

Run from `docs/audits/2026-11-14-probes/core-pipeline/` on 2026-11-16 with pyenv's CPython 3.13.1
and 3.12.12: the **build** at `be686953` (its `src/` is `5d51c047`'s plus prose), and **today**
as `git archive 75e7bf1e src`. The machine was shared; load averages (1 minute) ran from about 4
to 36. Each pair of figures is 3.13.1, then 3.12.12.

| Criterion | Probe | Today (`75e7bf1e`) | Build (`be686953`) |
|---|---|---|---|
| FR-001 AC1 | `p13_burst.py` | `{20: 1015}` and `{20: 1038}`, drained in 27.1 s and 25.8 s, `dropped` 19,850 and 19,620 | largest 1,000, drained in 0.5 s and 0.6 s, `dropped` 10,269 and 13,660 |
| FR-001 AC2 | `spec090_burst_gated.py` | `dropped=19990`; 1,000 calls of 20, 26.91 s and 26.63 s | `dropped=19990`; 22 calls, largest 1,000, 0.52 s and 0.69 s |
| FR-002 AC1, FR-003 AC2 | `p04_exit.py 0.02 10000` | 2 calls, largest 19,980; `close()` not run; `19980 event(s) in flight` | ~~60 and 61 calls, largest 234 and 200; `close()` run; one line declining 8,918 and 9,260 events, which with the 11,082 and 10,740 received make 20,000~~ re-run after the second diff review's fix: 61 calls on both, largest 234 and 240; `close()` run; one line declining 6,828 and 7,040 events in 32 batches, which with the 13,172 and 12,960 received make 20,000. The fix touches no batch here, since none fails; the figures move with the machine's load |
| FR-003 AC1 | `spec090_exit_budget.py` | `batched`: 1.20 s, join expired, 3 attempts, `close()` 0 times | `tree`: ~~1.07 s~~ 1.05 s and 1.06 s on the re-run after the second diff review's fix, join completed, 2 attempts, `close()` once, `failed_batches=3` |
| FR-003 AC3 | `spec090_dead_sink_exit_lines.py` | 2 lines, 20,002 events, `failed_batches=2` | 3 lines, 20,002 events, `failed_batches=1001`, the last `999 more exit batch(es) abandoned`; unchanged on the re-run after the second diff review's fix, since its pre-stop batch fails and the target is already zero at the stop |
| FR-004 AC4 | `spec090_nats_ack_backlog.py ~~0.002 200 10~~ 0.002 1000 10` | not run | ~~400 acked, `failed` 0, longest `deliver` 0.510 s and 0.505 s~~ 2,000 acked, `failed` 0, 12 calls, longest `deliver` 0.505 s and 0.513 s, re-run after the first diff review found 200 spans could not fail the criterion (the spec's revision history) |

The first 3.13.1 build run of `p13` met a 2.81 s burst at a load average near 27 and left no
backlog (`dropped=0`, drained in 0.1 s); the row above is its re-run at a load near 10. Also run,
as evidence: `spec090_exit_marker.py` read `[(2, False), (10, False), (10, True) ×5]` on the build
and `[(2, False), (60, False)]` today, on both interpreters; `spec090_frozen_clock.py` delivered
4,020 of 4,020 in 5 emits, largest 1,000; `spec090_per_event_sink.py 0.005 1000` sent no batch
above 98 events, the longest emit 0.50 s. The `NATSSink` residual at a 0.2 s `publish_timeout` is
in `architecture.md` §12 with its runs.

## Tests

- **Three existing tests rewritten in place**, the old claim struck and SPEC-090 cited:
  `test_shutdown_expiry_bookkeeping.py::test_case_a_released_to_fail_announces_each_event_lost_once`
  (the exit's two submissions are now two batches, so the lines name 1, 2 and 1 events),
  `test_event_ledger.py::test_abandoned_batches_and_a_failed_orphan_land_in_their_terms` (the cap
  is patched to 30 so each batch is ten spans again and `p05`'s numbers stand) and
  `test_event_ledger_defects.py::test_an_expired_shutdown_leaves_the_batch_in_flight` (FR-003's
  intended change: 40 delivered, 960 in `batch_lost` and one declined line, where all 1,000 were
  delivered late). `tests/model_sinks.py`'s ledger wrapper moved from `_emit` to `_emit_pending`, where a declined
  exit batch passes too, and
  `test_runtime_holder.py` records two new leaf `_lock` sites.
- **One equivalent mutant.** Removing the generation clause inside `_set_target` survives:
  `_set_target` records the generation it measured under, and `_size_target` reads a target from
  an older generation as zero, so a measurement that straddled a swap is discarded either way.

## Owed

- **FR-002 AC5's `unwinding` half**, until SPEC-089 lands. SPEC-089 is not on `main`; whichever
  of the two lands second moves the other's change into `_answer_marker`, and the half that
  asserts `unwinding` on a marker answered while a `BaseException` unwinds is enabled then. The
  rest of AC5, every marker answered or released, is asserted now.

## What changed from earlier specs?

- SPEC-062's rule, "the first attempt of every batch is always made", now holds for the exit
  drain's first batch only (FR-003); its decision was rewritten in place.
- SPEC-088's `batch_lost` and `failed_batches` also count a batch the exit drain declined
  unattempted. The expiry line's figure is unchanged in kind; it can now include the batch being
  built and one carried submission.
- SPEC-068's sentinel-stranding closure "re-reading the queue after the stop" is struck, since the
  exit drain is bounded; its window is wider by the old pull loop's duration.

## Verification

The probes above, each run's verbatim output kept with its load average by the build. The
completion's own gates, `gen-index.sh`, `spec-lint.sh` and `docs-lint.sh`, run on this branch;
the code gates' exit codes are the PR's record.
