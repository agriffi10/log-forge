---
type: Decision
title: "A sink round resends only what failed, and an SQS FIFO group keeps its order"
description: "the SDK, driver and socket sinks make one attempt per chunk, message or frame per round and the delivery engine resends only what may land within 1.x's total budget, so a failed chunk is resent after its round's later ones; only an SQS FIFO message group holds a later message back, per chunk at send time, for its predecessor's `max_retries + 1` sends. (SPEC-074)"
---

**A sink round resends only what failed, and an SQS FIFO group keeps its order** — `SQSSink`, `SNSSink`, `KinesisSink`, `FirehoseSink`, `ClickHouseSink`, `MongoDBSink`, `PostgresSink`, both Redis sinks, `RabbitMQSink`, `AzureEventHubsSink`, `NATSSink` and `SocketTransport` each make **one attempt per chunk (message, frame, pack) per round** and report every event's fate; the delivery engine resends only the `RETRYABLE` ones, waiting the 1.x backoff between rounds with no lock held, within `(s + 1) x (w + 1)` rounds on the worker path — 1.x's total — and `s + 1` through the inline adapter. A chunk that failed is resent **after** the later chunks of its round, where 1.x's loop retried it before moving on, so one entry that keeps failing costs only itself (measured by the spec reviewer: 100 SQS events with one entry failing every attempt landed 99 under 1.x and would have landed 36 had a round stopped at its first retryable chunk).

**The one exception is an SQS FIFO message group**, whose order SQS documents as a guarantee and 1.x kept for as long as its loop retried a chunk. The hold is decided per chunk at send time, from the run's replies so far (`sqs._hold`): an event is held (`RETRYABLE`, not sent) only when an earlier event of its group was sent in an earlier round or an earlier chunk of this round and has neither landed nor used up its `max_retries + 1` sends, this round's send counted, or when that earlier event is itself held. Same-group events never sent travel together in one chunk, as 1.x sent them, so a healthy one-group batch is one call. A held event never sent when the run ends is counted `failed` with the line `SQSSink, 0 attempt(s), held behind an earlier message of its FIFO group`. Rejected: holding the group for the worker's whole budget (it trades delivery for an order the consumer can rebuild from `timestamp`), and holding one event per group per round (a healthy five-event group then took four rounds and lost the fifth — plan review B1). The alternative reading — sending held events in the run's last round, after the predecessor's final send — is a change to `_hold` alone, and is Andrew's call.

**Do NOT** make a round stop at its first retryable chunk; do NOT let any sink but `SQSSink` FIFO hold an event back for order (Kinesis `PutRecords` promises none across records); do NOT hold a lock across the engine's wait.

## History

### 2026-11-09

- **Settled** by SPEC-074 FR-002 to FR-005.
