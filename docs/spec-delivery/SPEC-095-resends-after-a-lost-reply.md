# Completed Spec — SPEC-095: A Resend After a Lost Reply Is Counted, and Its Key Is the Event's Own `log_id`

## What was completed?

- **The count** (FR-001): `RunContext.may_have_landed`, validated, rewound and delegated as
  `sink_counted` is; `_run` counts each offer of an event again after a round that marked it, into
  `Delivery.resent` and, through the commit's `Deferral`, `SinkCounters.resent_events` on a settled
  or failed run, with one `_diag.resent` line. `SinkLosses.resent` is appended with a default of
  `0`, and every `SinkLosses` call in `src/` passes `resent=`. `HTTPSink._send` hands its count to
  `_run`'s new `resent_into`, which commits it to the sink's own counters in the same commit as the
  sink's own counts. The HTTP family's `_counter_lock` is now its counters' lock, as the other
  engine sinks' is, so a `losses()` read never sees a run's resends without its `failed`; no
  counter and no unit changed. A run whose own commit raises names the resends it could not
  count as well as the events (2026-11-20, in the diff review).
- **The marks** (FR-002): `_classify.may_have_landed`, `Called.reached` and
  `_rounds.mark_may_have_landed`; each shipped engine sink marks by the decision's table, and
  `tests/test_resend_marks.py`'s derived roster holds every engine sink to a marking case or an
  `EXEMPT` entry (`MongoDBSink`, the two file sinks, the two stream sinks, `NATSSink`). The five
  departure phrases are in the docstrings FR-002 names.
- **MongoDB** (FR-003): `_settle_bulk` reads a `11000` on `_id` for a driver-stamped `_id` an earlier
  raised call of the run carried as `ACCEPTED`, and registers no line when nothing was rejected.
- **The corpus** (FR-004): the `Recorder` records each round's marks, `resends_from` rebuilds them,
  `agrees` compares with `Delivery.resent`; `duplicate`, the new `resent-when-knowable` and
  `unannounced` read the count; the PostgreSQL and MongoDB doubles take FR-004's shapes. On the
  `main` base (SPEC-094 is not on `main`): 256 rows, 22 pinned under 11 pins, 31 departure rows
  under 12 departures, 203 that pass. `reply-resent` is gone, and five departures are new.
- **The record** (FR-005): the decision *A resend after a send that may have landed is counted, and
  its key is the event's `log_id`*; *No shipped sink declares itself idempotent* refined with the
  three proofs; History entries on *A client exception costs its chunk…* and on the `_diag`
  decision in `failure-paths/` (five writers, six writes); §12's finding-5 item struck and moved
  under *Resolved*, the "counted as lost" item completed, and two items added (SNS FIFO topics;
  a raising commit in a run that resent, closed by this spec's diff review and moved under
  *Resolved*); invariant 7's exception and the *Guarded* lines of
  invariants 2 and 7; the README; `docs/release-notes/v1.2.1.md`; the `_diag`, `_classify` and
  `_rounds` inventory rows refreshed (no module was added, so no new row).

**Departures from the spec's text**, each in its *Revision history*: FR-004's round wrapper is
`conformance_backend.Recorder` (2026-11-19); a PostgreSQL `commit()` raising `XX000` counts 0,
because `_retryable_error` does not retry class `XX`, and *The bound* gained the re-offer a closed
sink refuses (2026-11-19); `SentrySink[http]`'s half of FR-004's `Delivery.resent` criterion is
measured with `resent_into` dropped, since `_send` no longer reads `Delivery.resent` (2026-11-20).

## What changed from earlier specs?

- SPEC-092's corpus: `duplicate` reads a counted resend, `resent-when-knowable` is its eighth
  check, and its pins, departures and totals moved as above. SPEC-092's frozen runner,
  `spec092_build_mutants.py`, run at `3da245a9`, exits 1 with three failures: `src/` differs from
  `origin/main`, and its `redis-round-unknown` mutant leaves both Redis sinks' `refused-once` rows
  green rather than `ambiguous-when-knowable`, since those rows now read `resent-when-knowable`.
  Its `without:duplicate` and `without:lost-but-held` checks still pass, vacuously, because
  `HTTPSink-reply-lost` and `MongoDBSink-reply-lost` read green with or without those checks. It is
  a record of SPEC-092's build at `f8549fcd` and was not updated.
- SPEC-074's `MongoDBSink`: a duplicate driver-stamped `_id` on its own resend is no longer
  `REJECTED`. SPEC-072's `Delivery` and the public `SinkLosses` each gain `resent`.

## Verification

All from the runner `docs/audits/2026-11-14-probes/sinks/spec095_build_mutants.py`, at `f0e51c33`
(whose `src/` and `tests/` equal `3da245a9`'s) against base `c67931cd`, each tree a `git archive`
export with its `__file__` printed; output in `spec095_build_results.txt` (Python 3.13.1) and
`spec095_build_results_py312.txt` (3.12.12), identical apart from paths and one timing. It ends
"ALL CRITERIA HOLD".

- **The rows** (FR-004): base 256 rows, 86 red; head 53 red. Exactly 71 move: the 50 `reply-resent`
  rows from `duplicate` to green, `MongoDBSink` `taken-then-raised` and `reply-lost` from
  `lost-but-held` to green, and 19 to `resent-when-knowable`. The head corpus against the base tree
  moves 0. 77 rows count a resend at head.
- **Source mutants**, each in its own export: `may_have_landed` always `True`, 34 rows to
  `resent-when-knowable` (HTTP family and TCP sockets, FR-002); the context dropping every mark, 69
  (the 50 back to `duplicate`, the 19 to green); `_diag.resent` silent, all 77 gain `unannounced`;
  `Delivery.resent` never set, 75 gain `partition` (`SentrySink[http]`'s two are judged per batch);
  `resent_into` dropped, those two read `duplicate`. In process, `resent-when-knowable` removed
  from `judge`: the 19 read green.
- **MongoDB** (FR-003): in exports, removing condition 3 fails
  `test_a_caller_set_id_is_never_read_as_landed`, removing condition 4 fails
  `test_a_first_send_refused_on_id_is_rejected`; unmutated, both pass.
- **The audit's probes** re-run at `3da245a9`: `probe_aws_more.py` case 1 reads
  `SinkLosses(dropped=0, failed=10, resent=150)` with 160 messages for 10 events; `probe_sqs.py`
  D and D' and `probe_misc.py` cases 1 and 6 hold their copies as before, each now with one line.
- **The local MongoDB integration run** of `test_a_resend_after_a_lost_reply_is_read_as_delivered`
  passed with the rest of `tests/integration/test_mongo.py`: 35 passed, exit 0.

**Owed until the PR's CI:** the `integration.yml` run FR-003's last criterion names, against the
real MongoDB service, triggered by the PR since its path filter includes
`src/log_foundry/sinks/**`; and the ordinary `CI / test` job on Python 3.12 and 3.13.
