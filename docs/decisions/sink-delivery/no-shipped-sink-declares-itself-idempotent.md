---
type: Decision
title: "No shipped sink declares itself idempotent"
description: "an `UNKNOWN` outcome or an exception escaping `deliver` is never resent, because a declaration needs three proofs no shipped sink has: a destination that refuses or absorbs a second copy for as long as the engine can resend, with a refusal of its own resend read as delivery; a per-event key identical on every resend; and an `integration.yml` test against the real service quoting the vendor's window. SQS FIFO and, since SPEC-100, SNS FIFO fail the first and third, and `MongoDBSink` lacks the third until its owed run is green and stays undeclared by choice. (SPEC-074, SPEC-095, SPEC-100)"
---

**No shipped sink declares `log_foundry_idempotent`** (SPEC-072's opt-in), so the engine resends neither an `UNKNOWN` event nor a round whose `deliver` raised. A sink may declare it only when it can prove all three of:

1. **A destination that refuses or absorbs a second copy carrying the same key, for as long as the engine can resend:** either no window, or a window the sink bounds every resend to. Where the destination refuses the copy rather than absorbing it silently, the sink must also read that refusal of its own resend as delivery, not as a loss, as `MongoDBSink` does since SPEC-095 FR-003.
2. **A key the sink sets per event, identical on every resend the engine makes.**
3. **An `integration.yml` test against the real service** showing that a resend of an event that landed leaves one copy and is counted delivered, with the vendor's window quoted in the decision.

**No shipped sink meets all three.** `SQSSink` writing a FIFO queue fails the first: SQS accepts but does not deliver a message whose `MessageDeduplicationId` it has seen within 5 minutes, and "can't detect duplicate messages" resent after that interval, while SPEC-095's `spec095_fifo_window.py` shows the engine does not bound a resend to it (with each call ending after five botocore attempts at the default 60-second read timeout, round 2 starts more than 300 s after round 1). It fails the third too, because there is no SQS integration service; and `log_foundry_idempotent` is a class attribute while the same class writes standard queues. `MongoDBSink` meets the first two: within a run it keeps each document with its pymongo-stamped `_id`, and since SPEC-095 reads a duplicate-key refusal of that `_id` on its own resend as delivery. It meets the third only when SPEC-095's owed `integration.yml` run is green, and it is not declared even then, by a choice made in session on 2026-11-16 by the orchestrating session: declaring it reverses part of this fence for a gain only on a `writeErrors` entry that cannot be paired (*A resend after a send that may have landed is counted, and its key is the event's `log_id`*).

The rest fail the first condition outright. Kinesis and Firehose tell the caller to handle duplicates at the destination (Firehose `PutRecordBatch`, "We recommend that you handle any duplicates at the destination"); an SNS standard topic and an SQS standard queue de-duplicate nothing (SNS documents de-duplication for FIFO topics only; since SPEC-100 `SNSSink` writes one, and a FIFO `SNSSink` fails the conditions as set out below); ClickHouse de-duplicates a repeated insert block only under `insert_deduplicate` on a replicated table or `non_replicated_deduplication_window`, which the sink cannot see; `PostgresSink`'s `INSERT` has no conflict clause; Redis `XADD` with an automatic id and `RPUSH`, RabbitMQ, Event Hubs, NATS core and the socket peers deliver every copy they are sent; JetStream de-duplication needs a `Nats-Msg-Id` header `NATSSink` does not send; `ElasticsearchSink` and `OpenSearchSink` send no `_id`.

What is resent at all, at the cost *A client exception costs its chunk, and is provable non-delivery for it* accepts, is a `RETRYABLE` event; since SPEC-095 each such resend of an event a send may have landed is counted in `losses().resent` and announced.

**A FIFO `SNSSink` (SPEC-100) fails the first and third conditions, and meets the second only within a run.** (1) An SNS FIFO topic accepts but does not deliver a message whose `MessageDeduplicationId` it has seen within its interval, a "5 minute minimum", and under `FifoThroughputScope=MessageGroup` only when the group id is identical too; it absorbs a copy only inside that interval, and `spec095_fifo_window.py` shows the engine's second round can start more than 300 s after the first when each call ends after botocore's five attempts at its 60-second read timeout. `SNSSink` uses the same client stack. (2) Its ids are resolved once per run and every resend carries them unchanged, so within a run the key is identical; across runs only a stable source keeps it, since a callable that answers differently, or the minted fallback for an event with no `log_id`, gives a new id to a caller's own whole-batch resend. (3) `tests/integration/docker-compose.yml` runs no SNS service, and moto's SNS does not de-duplicate: its `publish_batch` says the `MessageDeduplicationId` parameter is not yet implemented, and `spec100_moto_shapes.py` case C saw both copies of one id forwarded. Like `SQSSink`, the class also writes standard topics.

**Do NOT** declare a shipped sink idempotent without all three proofs; do NOT declare `SQSSink` or `SNSSink` idempotent for FIFO destinations alone through the class attribute.

## History

### 2026-11-24

- **Refined** by SPEC-100 FR-007. `SNSSink` now writes SNS FIFO topics, so the sentence that said it could not is rewritten, and a FIFO `SNSSink` is set against the three conditions: it fails the first, since the topic absorbs a copy only inside its interval and the engine does not bound a resend to it; it meets the second within a run and across runs only with a stable source; and it fails the third, with no SNS integration service and no de-duplication in moto's SNS. The rule is unchanged: no shipped sink is declared.

### 2026-11-20

- **Refined** by SPEC-095 FR-005. The fence now states what a declaration needs, the three conditions above, where it listed why each destination fell short. `MongoDBSink`'s reason changed: it used to be that the duplicate `_id` came back `REJECTED` and counted although the document landed, and since SPEC-095 FR-003 that refusal of its own resend reads as delivery, so it now lacks only the integration proof and is undeclared by choice. SQS FIFO's 5-minute window, called unmeasured, is now shown by SPEC-095's `spec095_fifo_window.py` not to bound a resend. The rule itself is unchanged: no shipped sink is declared.

### 2026-11-09

- **Settled** by SPEC-074 FR-006.
