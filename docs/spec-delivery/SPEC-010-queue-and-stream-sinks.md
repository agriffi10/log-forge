# Completed Spec — SPEC-010: Queue and Stream Buffer Sinks

## What was completed?

Nine durable-buffer sinks extending the `SQSSink` path (arch §9.1) to the other queues/streams teams
run in front of their indexers. Each follows the SPEC-005 recipe — client behind an optional extra +
**lazy import**, injectable client for tests, per-transport re-chunking, bounded partial-failure
retry, oversized drop-with-warning, and a meaningful `close()`. All are `isinstance`-checkable and
operate on already-built event dicts (arch §8).

- **`sinks.kinesis` / `sinks.firehose` / `sinks.sns`** (new) — boto3 (`aws` extra); ≤500/5 MB,
  ≤500/4 MB, ≤10-entry chunking with per-record/entry failure retry (FR-003, FR-004, FR-010).
- **`sinks.kafka`** (new) — confluent-kafka (`kafka`); produce-per-event, `close()` flush,
  delivery-callback error counting (FR-002).
- **`sinks.redis`** (new) — redis (`redis`); `RedisStreamsSink` (`XADD`) + `RedisListSink` (`RPUSH`),
  pipelined, ownership-aware close (FR-005).
- **`sinks.rabbitmq`** (new) — pika (`amqp`); persistent publish + reconnect (FR-006).
- **`sinks.nats`** (new) — nats-py (`nats`); async client driven from sync `emit` via a managed
  loop, optional JetStream (FR-007).
- **`sinks.pubsub`** (new) — google-cloud-pubsub (`gcp-pubsub`); publish futures flushed on close
  (FR-008).
- **`sinks.eventhubs`** (new) — azure-eventhub (`azure-eventhubs`); 1 MB `EventDataBatch` packing +
  oversized drop (FR-009).
- **`sinks._chunk`** (new) — shared `chunk_items` (count + byte re-chunking) used by the AWS sinks.

**Deviation from the Draft:** several constructors gained an optional `url`/`servers`/`bootstrap_
servers` for the owned-client case (the Draft's representative signatures showed only the injected
`client=`), and required-arg validation runs *before* the lazy import so the `ValueError` is
reachable without the extra installed.

## What changed from earlier specs?

Per the spec, the SPEC-005 **`sqs` extra was renamed to `aws`** (no alias — pre-release) and now
also backs `SNSSink`/`KinesisSink`/`FirehoseSink`. Ripples applied: `pyproject.toml`, `poetry.lock`,
CLAUDE.md Tech Stack + Layout, the `SQSSink` lazy-import comment (`# optional 'aws' extra`), and the
SPEC-005 delivery doc's extra reference. Six new extras added: `kafka`, `redis`, `amqp`, `nats`,
`gcp-pubsub`, `azure-eventhubs`. No behavioral change to `SQSSink` or any earlier module.

## Verification

Local gates green — `ruff` clean, `mypy --strict` clean (43 src files), `pytest` **242 passed**. 51
new tests inject fake clients/producers/connections (no network/broker) covering chunk boundaries,
partition keying, `Failed`/`FailedRecordCount`/`FailedPutCount` retry + persistent-failure counting,
oversized drop, Kafka close-flush + delivery errors, pipelined Redis + ownership-aware close,
RabbitMQ persistent publish + reconnect, NATS JetStream + drain, Pub/Sub future flush, Event Hubs
1 MB packing + oversized drop. Smoke-tested `NATSSink` + `KinesisSink` through the real worker thread
(the NATS event loop, created on the main thread, drives correctly from the worker thread).
