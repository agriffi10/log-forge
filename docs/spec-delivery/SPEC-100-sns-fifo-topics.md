# Completed Spec — SPEC-100: SNSSink Writes an SNS FIFO Topic: Each Message Carries Its Trace's Group and Its Event's `log_id`, Resent Unchanged

## What was completed?

- **One FIFO mechanism, `sinks/_fifo.py`** (FR-002). It holds `DEFAULT_GROUP_ID`, `MAX_ID_LEN`,
  `GroupIdSource`, `DedupIdSource`, `judge`, `bounded`, `group_id`, `dedup_id`, `prepare`,
  `plan_round` (the per-run entry cache, kept under `"prepared"`, `"prepared_round"` and `"limit"`
  in the calling sink's run notes, chunked with `chunk_items`), `PrepareOne`, `Prepared`, `Plan`,
  `Entry`, `hold`, `block`, `answer`, `announce_id_failures` (the one id-failure line) and
  `send_round`, the one round loop both sinks' `_round` call (the last-round gate, the hold and
  its held line, the done marks and the blocks). An entry is `done` at every final fate, so a
  repeated event object's retried occurrence is never handed another occurrence's finished
  entry.
  `SQSSink` delegates to it; each sink keeps its own `encode_round` call and passes `send_round`
  only its `_send`, its API's name and its class name.
- **`SNSSink(fifo=, message_group_id=, message_deduplication_id=)`** (FR-001, FR-002, FR-004):
  keyword-only after `max_message_bytes`, stored as public attributes; `GroupIdSource` and
  `DedupIdSource` exported from `sinks.sns` as the same objects. On a FIFO topic each entry carries
  both ids (`trace_id` and `log_id` by default), resolved once per run, resent unchanged, costed
  against the run's limit, and held behind an earlier message of its group through `_fifo.hold`.
  `SNSSink._bodies` is replaced by the shared preparation and `SNSSink._register_drops`, which adds
  the id-callable line.
- **One judgement on both sinks** (FR-001): `_fifo.judge`, after every refusal each constructor
  made before, refuses a non-`bool` `fifo`, `fifo=True` without `.fifo` and an id argument on a
  sink that is not FIFO, and a destination that is not a `str` with a `TypeError` naming it;
  `fifo=False` on a `.fifo` name stays accepted.
- **The digest** (FR-003): on both sinks a stripped de-duplication answer over 128 characters is
  sent as its SHA-256 hex digest.
- **Ids costed inside the `try`, and asked their type** (FR-002, found while planning and by the
  diff review): an answer that is not a `str` (`bytes`, or an object whose own `strip` returns a
  `str`) is a `TypeError` id failure of its event, a lone-surrogate answer an id failure at its
  costing, and a `str` subclass is stripped through `str.strip`, on both sinks.
- **The corpus** (FR-006): `SNSSink[standard]` (renamed) and `SNSSink[fifo]` beside
  `SQSSink[fifo]`, over an SNS double that de-duplicates.
- **Tests:** `tests/test_sinks_sns_fifo.py` (FR-001 to FR-005, most on both sinks), and the
  rosters and pins FR-001, FR-002 and FR-006 name.

## What changed from earlier specs?

- SPEC-016, SPEC-071: `SQSSink` refuses `fifo` that is not a `bool` or `None`, `fifo=True` on a URL
  without `.fifo`, and an id argument on a queue that is not FIFO; it sends an over-long
  de-duplication id as its digest rather than its first 128 characters; and an id answer that
  cannot be costed drops its event rather than its round. `SQSSink._group_id` and `_dedup_id` are
  now uncalled delegates; `sqs._Prepared`, `_Plan`, `_Entry`, `_bounded`, `_hold` and `_block`
  are bound to the `_fifo` objects.
- SPEC-074: the FIFO hold covers `SNSSink`; neither sink calls `chunk_items` itself
  (`tests/test_chunk_helper_roster.py` names `_fifo`).
- SPEC-092: `SNSSink`'s variant is `SNSSink[standard]`; the corpus is 275 rows, 6 pinned under 4
  pins, 36 departure rows under 14 departures, 233 that must pass, `aws-client-retries` 12 rows,
  56 variants.
- The record: invariants 2, 7 and 13; `architecture.md` §12 (the SNS FIFO item struck, one new
  open item); the new decision *A de-duplication id too long to send is sent as its digest,
  never as a prefix*, and History entries on five sink-delivery decisions and the resend
  decision's key table; the README; `docs/release-notes/v1.2.1.md`; the component inventory.

## Verification

- **Gates** on `b145f9a2`: ruff, mypy, pytest (twice, parallel), the extras legs on 3.12 and 3.13,
  spec-lint, docs-lint, docstring-lint; after the diff review's fixes, the same on 3.13 (the
  extras leg on 3.13 only).
- **Mutants**, each planted once and killed by an assertion: the cache disabled in `plan_round`
  (the resend-ids test on both sinks, the de-duplicating-topic test, 6 held for 3 events, and
  `test_sinks_queue_engine.py::test_fifo_ids_are_resolved_once_per_run`); the digest replaced by
  truncation (the prefix test on both sinks, the 129-character boundary, the digested resend, and
  `test_sinks_sqs_fifo.py::test_over_long_dedup_id_is_sent_as_its_digest`); `hold` returning all
  `False` (the twelve-event, always-failing and declined-run tests); and the ids costed outside
  the `try` (all eight cases of the uncostable-answer test, with the base's `raised 2 after 1
  delivery round(s)`).
- **Diff review frame 1's mutants**, re-planted in `_fifo.send_round` once the round loop moved
  there (the sinks' call sites hold no copy of either expression): holding in the run's last
  round (`holding = fifo`) is killed on both sinks by an assertion in
  `test_the_last_round_releases_a_held_call_behind_an_always_failing_one` (`[0, 1] != [0, 1, 10,
  11]`) and `test_a_forwarded_deliver_takes_last_from_the_outer_runs_budget`, and also reddens
  `test_the_last_round_sends_a_held_event_after_its_predecessors_final_send` (the run raises) and
  `test_no_round_before_the_last_sends_a_held_event` (no last-round send); not marking a landed
  occurrence (`pass` for what was then `cached.landed = True`) is killed on both sinks by
  `test_a_repeated_event_never_resends_a_landed_occurrences_ids` (the resend carries the landed
  occurrence's id). The answer's type check disabled is killed by
  `test_every_answer_not_a_str_refuses_the_batch_naming_the_id` on both sinks and both ids (the
  refusal names `strip`, not the id; the drop itself is equivalent, since `str.strip` refuses a
  non-`str` too), and the destination's type check disabled by
  `test_a_destination_that_is_not_a_str_is_refused_naming_it` on both sinks.
- **Diff review frame 2's mutants**, each re-planted once in `_fifo`: the done mark narrowed back
  to an accepted fate and `plan_round` handing out dropped entries again is killed on both sinks
  by `test_a_repeated_events_id_failure_is_never_handed_to_its_retried_twin`,
  `test_a_repeated_events_sender_fault_keeps_its_id_off_its_twins_resend` and
  `test_a_repeated_events_oversize_drop_is_never_handed_to_its_retried_twin`; the id-failure
  line's count fixed at 1 by `test_the_id_failure_line_counts_every_failed_event` (`lost 1` for
  `lost 2`); the digest's `surrogatepass` dropped by
  `test_a_lone_surrogate_answer_over_the_limit_is_sent_as_its_digest`.
- **Recorded, not fixed:** a FIFO group waits behind a message refused as final or left
  unadjudicated, which `_fifo.block` blocks on though it is never resent (`architecture.md` §12,
  *Open*); the change is a reading of the hold decision, so it is the orchestrator's.
- **moto re-run** (scratch venv; Python 3.13.1, boto3 1.44.4, moto 5.2.5) against this tree:
  case E, 4 calls, every event carrying one de-duplication id on every call, one message of the
  group sent per round, `SinkLosses(dropped=0, failed=0, resent=3)`, the FIFO subscriber holding
  3 for 3 events and the standard one 6; case F, the queue holding 2 of 2,
  `SinkLosses(0, 0, 0)`, no line; case A, both `emit`s returned with nothing lost; B, C and D as
  the spec's *Evidence* apart from moto's random ids. `spec100_refusals`, `ids_once`, `ordering`
  (A to E) and `costing` read as *Evidence*.
- **FR-006's renamed rows**, read through `cb.run_row` on the base: `SNSSink-<fault>` green for
  `clean`, `partial`, `taken-then-raised`, `reply-lost` and `no-verdict-shape`, and
  `refused-once` and `refused-always` under `aws-client-retries` at `resent-when-knowable`, with
  no pin. After: `SNSSink[standard]-<fault>` identical for all seven, and `SNSSink[fifo]` equal to
  `SQSSink[fifo]` on all seven.
