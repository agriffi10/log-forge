# Completed Spec — SPEC-094: Client-Buffered Sinks Wait for the Delivery Report, on the Delivery Engine

## What was completed?

Built with SPEC-093 on one branch and landed in the same PR, after SPEC-093's commits.

- **`_retry.pause_on`**: a stop signal already set does not cut a pause short.
- **`KafkaSink` on the engine** (FR-001): `deliver`, `log_foundry_counters`,
  `log_foundry_stop_signal`; `failed` and `rejected` as assignable properties; the round's report
  table, the sliced wait (`_REPORT_SLICE`, `flush(slice)`), `DEFINITIVE_CODES = {21, 87}`, the
  `flush()` hook that serves reports and never raises, and a `close()` that counts nothing.
- **`GooglePubSubSink` on the engine** (FR-002): `deliver`, `log_foundry_counters`, the per-round
  wait under `overflow_timeout` (`_await_round`), the `done()` reading of a resolved
  `TimeoutError`, the definitive set (`InvalidArgument`, `FlowControlLimitError`), and the pending
  list's machinery removed; `max_pending` is validated and inert. `close()` waits up to
  `overflow_timeout`, not cut short by the stop signal, on the futures its rounds left
  `UNKNOWN`, counting nothing and writing no line, so a normal exit lets the client finish
  sending them (Andrew's "Wait at close (Recommended)", 2026-11-22, after the second diff
  review measured a flag-only close losing every publish still in flight at exit).
- **The corpus** (FR-003): the Kafka double reports code 87, the Pub/Sub double raises a stub
  `google.api_core.exceptions` hierarchy, and two pins removed. On the merged tree, after SPEC-095
  and SPEC-093: 268 rows, 6 pinned under 4 pins, 34 departure rows, 228 that must pass; before
  SPEC-095 landed, 58 pinned under 5 pins, 15 departure rows and 195.
- **New tests:** `tests/test_kafka_report_wait.py`, `tests/test_pubsub_round_wait.py`; `kafka` joins
  `WAIT_EXCLUSIONS`, and the engine-sink roster reads 18.
- **The record** (FR-004): `architecture.md` §12 (the finding-12 verdict half and the
  unboundable-future item struck and moved under *Resolved*, *Whether a full client queue is
  retried* opened), five §13 bullets, and dated corrections where §8, §9 and §13 still described
  the pending futures, the close flush as the whole buffer, or the two sinks' flush hooks; two new decisions and five refined, plus a superseded
  marker in *A closing sink refuses at once…* (`docs/decisions/sink-lifecycle/`), whose fence named
  `KafkaSink`'s `released_during` count; `invariants.md` 2, 3, 4, 6, 7 and 13; the README; the
  release notes; the inventory's queue-sink, delivery-helper and `_retry` rows. One §12 item is
  opened and left unfixed under the audit scope freeze: *`DEFAULT_CLOSER_GRACE` does not bound
  the live sink's close*, found by the execution review, present before this spec. Invariant 3's
  recorded exceptions say the grace bounds the exit, but the live sink's close runs inline, so
  `shutdown(timeout=1.0)` over a 5 s close took 5.01 s.

**Deviations:**

- FR-002's resolved-`TimeoutError` criterion asserts 1.0 s, not 0.1 s, because
  `tests/test_timing_floors.py` refuses a ceiling under its 0.5 s stall allowance; the probe measured
  0.000 s. Corrected in the spec (*Revision history*, 2026-11-20). FR-001's stuck-partition test
  runs at `flush_timeout=1.0` with a 0.5 s ceiling on the later emits, for the same gate, also
  corrected in the spec.
- `test_a_close_landing_mid_batch_counts_the_rest_unconfirmed_and_does_not_raise` keeps its name and
  behavioural assertions; its one assertion on `sink._futures` went with the pending list.
- `tests/test_deferral.py` gains `ASSIGNABLE_DROPPED`, exempting only the two `rejected` setters
  (the spec keeps `rejected` assignable) and reporting a stale entry; mutation-tested.

## What changed from earlier specs?

- Kafka's `_on_delivery` count, its close-flush count and SPEC-076's `released_during` count, and
  Pub/Sub's reap, overflow wait and close-time counting, are gone; every loss is counted once by
  its round. Pub/Sub's `close()` still waits, bounded, on what its rounds left unconfirmed, but
  counts nothing.
- SPEC-036: neither sink's `flush()` hook waits or raises.
- SPEC-092: `test_a_check_removed_from_judge_frees_the_row_it_holds` drops its partition case, since
  no shipped row reads `partition` now; `test_fate_of_misreading_rejected_is_caught_by_the_delivery_comparison`
  and `judge`'s fixtures still hold that check.

## Verification

Gates green at `ea882c91`, the runner-collected test names going from 12,685 to 12,824 across both
specs (16 Pub/Sub mechanism tests retired, 8 deleted with their subject). Against SPEC-093's verdicts
exactly the four rows FR-001 and FR-002 name moved, and each FR-001 to FR-003 re-plant reddened its
named check; `probe_misc.py` case 3 and `spec094_stuck_partition.py` read as the criteria ask
(`docs/audits/2026-11-14-probes/sinks/spec093_094_build_results.txt`). The local integration run at
`1b562822` passed the new Kafka and Pub/Sub integration tests. No criterion of this spec is owed;
its integration tests run again on the shared PR's `integration.yml` run, which settles SPEC-093's
owed criterion.
