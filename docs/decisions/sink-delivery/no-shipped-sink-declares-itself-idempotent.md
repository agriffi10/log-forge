---
type: Decision
title: "No shipped sink declares itself idempotent"
description: "an `UNKNOWN` outcome or an exception escaping `deliver` is never resent, because no destination the library ships de-duplicates a resend the sink can see or bound: Kinesis and Firehose leave duplicates to the destination, SNS and SQS standard queues de-duplicate nothing, ClickHouse only under settings the table's owner chooses, Postgres has no conflict clause, the brokers deliver every copy, MongoDB would turn the duplicate into a miscounted rejection, and SQS FIFO's 5-minute window is unmeasured and the same class writes standard queues. (SPEC-074)"
---

**No shipped sink declares `log_foundry_idempotent`** (SPEC-072's opt-in), so the engine resends neither an `UNKNOWN` event nor a round whose `deliver` raised. The evidence per destination: Kinesis and Firehose tell the caller to handle duplicates at the destination (Firehose `PutRecordBatch`, "We recommend that you handle any duplicates at the destination"); an SNS standard topic and an SQS standard queue de-duplicate nothing (SNS documents de-duplication for FIFO topics only, and `SNSSink` sets no de-duplication id); ClickHouse de-duplicates a repeated insert block only under `insert_deduplicate` on a replicated table or `non_replicated_deduplication_window`, which the sink cannot see; `PostgresSink`'s `INSERT` has no conflict clause; Redis `XADD` with an automatic id and `RPUSH`, RabbitMQ, Event Hubs, NATS core and the socket peers deliver every copy they are sent; JetStream de-duplication needs a `Nats-Msg-Id` header `NATSSink` does not send.

**MongoDB** keeps one run's documents and their pymongo-stamped `_id`s across its rounds, so within a run a resend of a landed document is refused as a duplicate key rather than stored twice — but that refusal comes back as a `writeErrors` entry, `REJECTED` and counted although the document landed, so resending an unknown outcome would turn it into a miscounted failure, not a delivery. **An SQS FIFO queue** accepts but does not deliver a message whose `MessageDeduplicationId` it has seen within 5 minutes, and `SQSSink` prepares each entry once per run, ids included, so a resend within a run is identical; it is still not declared, because `log_foundry_idempotent` is a class attribute and the same class writes standard queues, and SQS absorbs a duplicate only within a window the sink neither measures nor bounds a run to.

**Do NOT** declare a shipped sink idempotent without a destination-side de-duplication the sink can see and bound; do NOT declare `SQSSink` idempotent for FIFO queues alone through the class attribute.

## History

### 2026-11-09

- **Settled** by SPEC-074 FR-006.
