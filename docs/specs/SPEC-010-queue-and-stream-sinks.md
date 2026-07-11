# Spec: Queue and Stream Buffer Sinks

**ID:** SPEC-010
**Status:** Completed
**Last Updated:** 2026-07-11
**Depends On:** SPEC-005

## Overview

This spec extends the **durable-buffer** path that `SQSSink` established (arch §9.1) to the other
message queues and streaming platforms teams run in front of their log indexers. It adds `KafkaSink`,
`KinesisSink`, `FirehoseSink`, `RedisStreamsSink`, `RedisListSink`, `RabbitMQSink`, `NATSSink`,
`GooglePubSubSink`, `AzureEventHubsSink`, and `SNSSink`. Every one follows the proven SPEC-005
recipe: the client library is an **optional dependency** behind an extra and imported lazily inside
the sink (never at module top), a client can be injected for tests, incoming batches are re-chunked to
the transport's per-request limits, and partial failures are surfaced and retried within a bounded
count. A downstream consumer drains these buffers into ELK — that consumer is out of scope for this
library (arch §9.1, §13).

## Scope

### In Scope

- `KafkaSink(topic, *, producer=None, bootstrap_servers=None, key_field="trace_id")` — produce one
  message per event; the producer batches internally; `close()` flushes.
- `KinesisSink(stream_name, *, client=None)` — `put_records` in chunks ≤ 500 records and ≤ 5 MB.
- `FirehoseSink(delivery_stream, *, client=None)` — `put_record_batch` in chunks ≤ 500 records and
  ≤ 4 MB.
- `RedisStreamsSink(stream, *, client=None)` (`XADD`) and `RedisListSink(key, *, client=None)`
  (`RPUSH`), pipelined per batch.
- `RabbitMQSink(*, exchange, routing_key, connection=None)` — publish persistent messages (`pika`).
- `NATSSink(subject, *, client=None, jetstream=False)` — publish per event, optionally to JetStream
  for durability.
- `GooglePubSubSink(topic, *, client=None)` — publish per event; `close()` flushes pending futures.
- `AzureEventHubsSink(*, producer=None, connection_str=None, eventhub=None)` — send `EventData`
  batches respecting the 1 MB batch limit.
- `SNSSink(topic_arn, *, client=None)` — `publish_batch` (≤ 10 entries), `Failed`-list handling like
  `SQSSink`.
- Shared: lazy client import, injectable client, per-transport chunking, bounded partial-failure
  retry, oversized-event drop-with-warning, and a meaningful `close()`.

### Out of Scope

- Consuming from any of these buffers and indexing into ELK — a separate component (arch §9.1, §13).
- FIFO / exactly-once / strict global ordering guarantees — standard delivery only; ordering and
  dedup are deferred (as in the `SQSSink` spec).
- Schema-registry encodings (Avro/Protobuf/JSON-Schema) — message bodies are `json.dumps` of the
  event dict only.
- Managing topics/streams/queues lifecycle (creation, ACLs, retention) — the target is assumed to
  exist; the sink only publishes to it.

---

## Functional Requirements

### FR-001: Shared sink contract and lazy dependencies

#### Description:

Every queue sink is a drop-in `Sink` whose client dependency is optional and lazily imported.

#### Acceptance Criteria:

- [ ] Each sink satisfies `emit(batch) -> None` / `close() -> None` and passes an
      `isinstance(sink, Sink)` runtime check.
- [ ] Each sink imports its client library inside the constructor/method (never at module top), so
      importing the sink module does not require the extra unless the sink is instantiated without an
      injected client.
- [ ] Each sink accepts an injected client/producer/connection so tests exercise it with a fake and no
      network/broker access.

### FR-002: KafkaSink

#### Description:

Produce events to a Kafka topic.

#### Acceptance Criteria:

- [ ] `emit(batch)` produces one message per event to `topic`, body = `json.dumps(event)`; when
      `key_field` is set, the message key is that event field (for partition affinity) else `None`.
- [ ] The sink relies on the `confluent-kafka` producer's internal batching and does not block per
      message (`produce()` enqueues locally); `close()` calls `flush()` so buffered messages are sent
      before exit.
- [ ] Delivery errors — surfaced asynchronously via the `confluent-kafka` delivery callback (serviced
      by `poll()`/`flush()`) — are counted and logged, not raised uncontrolled out of `emit`.

### FR-003: KinesisSink

#### Description:

Put event records to a Kinesis Data Stream.

#### Acceptance Criteria:

- [ ] `emit(batch)` calls `put_records` in chunks of ≤ 500 records **and** ≤ 5 MB per request, each
      record's `Data` = `json.dumps(event)` with a configurable partition key.
- [ ] The response `FailedRecordCount`/per-record error is inspected; only failed records are retried
      (bounded), successes are not re-sent, and records still failing past the bound are counted/logged.
- [ ] A single event exceeding the per-record limit is dropped with a counted warning; the rest send.

### FR-004: FirehoseSink

#### Description:

Put event records to a Kinesis Data Firehose delivery stream.

#### Acceptance Criteria:

- [ ] `emit(batch)` calls `put_record_batch` in chunks of ≤ 500 records **and** ≤ 4 MB per request.
- [ ] The response `RequestResponses`/`FailedPutCount` is inspected; failed entries are retried
      (bounded) then counted/logged; successes are not re-sent.

### FR-005: RedisStreamsSink / RedisListSink

#### Description:

Buffer events in Redis via a stream or a list.

#### Acceptance Criteria:

- [ ] `RedisStreamsSink(stream)` issues one `XADD` per event (event serialized into the stream entry)
      and pipelines the whole batch into one round trip.
- [ ] `RedisListSink(key)` issues `RPUSH` of one `json.dumps(event)` per event, pipelined.
- [ ] A connection error is retried (bounded) then counted/logged; `close()` releases the connection
      if the sink owns it (an injected client is not closed).

### FR-006: RabbitMQSink

#### Description:

Publish events to a RabbitMQ exchange.

#### Acceptance Criteria:

- [ ] `emit(batch)` publishes one persistent message per event (delivery mode = persistent) to the
      configured `exchange`/`routing_key`, body = `json.dumps(event)`.
- [ ] A dropped/closed connection is re-established (bounded retry) before the batch is abandoned and
      counted/logged; `close()` closes the channel and connection.

### FR-007: NATSSink

#### Description:

Publish events to a NATS subject, optionally via JetStream.

#### Acceptance Criteria:

- [ ] `emit(batch)` publishes one message per event to `subject`; when `jetstream=True`, publishes via
      JetStream for durable acknowledgement.
- [ ] The async NATS client is driven from the synchronous `emit` correctly (e.g. via a managed loop),
      so `emit` returns only after the batch is handed off; `close()` drains/flushes and closes.

### FR-008: GooglePubSubSink

#### Description:

Publish events to a Google Cloud Pub/Sub topic.

#### Acceptance Criteria:

- [ ] `emit(batch)` publishes one message per event to `topic`, body = `json.dumps(event)` encoded to
      bytes.
- [ ] `close()` flushes pending publish futures so buffered messages are sent; publish errors are
      counted/logged.

### FR-009: AzureEventHubsSink

#### Description:

Send events to an Azure Event Hub.

#### Acceptance Criteria:

- [ ] `emit(batch)` packs events into one or more `EventDataBatch` objects respecting the 1 MB batch
      limit and sends them; a single event too large for an empty batch is dropped with a counted
      warning.
- [ ] `close()` closes the producer; send errors are retried (bounded) then counted/logged.

### FR-010: SNSSink

#### Description:

Publish events to an SNS topic.

#### Acceptance Criteria:

- [ ] `emit(batch)` uses `publish_batch` in chunks of ≤ 10 entries, each message = `json.dumps(event)`.
- [ ] The response `Failed` list is inspected and retried (bounded) then counted/logged, mirroring the
      `SQSSink` partial-failure policy.

### FR-011: Shared chunking, partial-failure, and oversized handling

#### Description:

Chunking and failure handling are consistent across the family and reuse the SPEC-005 conventions.

#### Acceptance Criteria:

- [ ] Each sink re-chunks an incoming batch to its transport's own count/byte limits (no assumption
      that the worker's batch already fits).
- [ ] Partial failures are retried within a bounded attempt count; entries still failing are counted
      (e.g. `failed`) and logged, never silently dropped.
- [ ] A single event too large to ever fit one message/record is dropped with a counted
      (`dropped_oversized`) warning and does not prevent the rest of the batch from being sent.

---

## Data Model

```
# One module per sink under src/log_forge/sinks/. Shared shape:
<QueueSink> {
  target: str                  # topic / stream / queue / subject / topic_arn
  client: <lazily-imported or injected transport client>
  max_retries: int = 3
  failed: int                  # entries abandoned past the retry bound
  dropped_oversized: int       # events too large to ever fit one message/record

  # transport-specific hard limits, e.g.:
  # KinesisSink:  MAX_RECORDS = 500;  MAX_BYTES = 5 * 1024 * 1024
  # FirehoseSink: MAX_RECORDS = 500;  MAX_BYTES = 4 * 1024 * 1024
  # SNSSink:      MAX_BATCH   = 10
  # EventHubs:    MAX_BYTES   = 1 * 1024 * 1024 (per batch)
}
```

Each event becomes one message/record body via `json.dumps` (or the client's native `EventData` for
Event Hubs). Events are the SPEC-001 `LogEvent` dicts.

---

## API / Interface Contract

```python
# Representative constructors (each in its own sinks/<name>.py module)
class KafkaSink:
    def __init__(self, topic, *, producer=None, bootstrap_servers=None, key_field="trace_id") -> None: ...
class KinesisSink:
    def __init__(self, stream_name, *, client=None, partition_key_field="trace_id", max_retries=3) -> None: ...
class FirehoseSink:
    def __init__(self, delivery_stream, *, client=None, max_retries=3) -> None: ...
class RedisStreamsSink:
    def __init__(self, stream, *, client=None) -> None: ...
class RabbitMQSink:
    def __init__(self, *, exchange, routing_key, connection=None) -> None: ...
class NATSSink:
    def __init__(self, subject, *, client=None, jetstream=False) -> None: ...
class GooglePubSubSink:
    def __init__(self, topic, *, client=None) -> None: ...
class AzureEventHubsSink:
    def __init__(self, *, producer=None, connection_str=None, eventhub=None) -> None: ...
class SNSSink:
    def __init__(self, topic_arn, *, client=None, max_retries=3) -> None: ...

# Usage
import log_forge
from log_forge.sinks.kafka import KafkaSink
log_forge.configure(sink=KafkaSink(topic="logs", bootstrap_servers="broker:9092"))
```

## Configuration / Environment

- New **optional extras**, each pulling only its client and imported lazily (added to `pyproject.toml`
  and CLAUDE.md's Tech Stack at implementation time):
  `kafka` (`confluent-kafka`), `redis` (`redis`), `amqp` (`pika`), `nats`
  (`nats-py`), `gcp-pubsub` (`google-cloud-pubsub`), `azure-eventhubs` (`azure-eventhub`).
- Kinesis, Firehose, and SNS are boto3-based and use a new **`aws`** extra (`boto3>=1.34`). Implementing
  this spec **renames the SPEC-005 `sqs` extra to `aws`** (no `sqs` alias retained — the package is
  pre-release, so the breaking rename is acceptable) and moves `SQSSink` onto `aws`; the ripple updates
  are `pyproject.toml`, CLAUDE.md's Tech Stack, and the SPEC-005 delivery doc's extra reference.
- Credentials/endpoints are resolved by each client's standard mechanism (boto3 chain, Kafka
  `bootstrap_servers`, Redis URL, AMQP URI, GCP ADC, Event Hubs connection string); log-forge adds no
  new credential configuration of its own.

## File & Folder Structure

```
src/log_forge/sinks/
├── kafka.py        # KafkaSink                                   (new)
├── kinesis.py      # KinesisSink (put_records, 500/5MB chunks)   (new)
├── firehose.py     # FirehoseSink (put_record_batch, 500/4MB)    (new)
├── redis.py        # RedisStreamsSink + RedisListSink            (new)
├── rabbitmq.py     # RabbitMQSink (pika, persistent publish)     (new)
├── nats.py         # NATSSink (+ JetStream option)               (new)
├── pubsub.py       # GooglePubSubSink                            (new)
├── eventhubs.py    # AzureEventHubsSink                          (new)
└── sns.py          # SNSSink (publish_batch, Failed handling)    (new)
tests/
├── test_sinks_kinesis.py    # 500/5MB chunking + FailedRecordCount retry (fake client) (new)
├── test_sinks_firehose.py   # 500/4MB chunking + FailedPutCount retry                  (new)
├── test_sinks_sns.py        # 10-entry batches + Failed retry                          (new)
├── test_sinks_kafka.py      # produce-per-event + close flush (fake producer)          (new)
├── test_sinks_redis.py      # XADD/RPUSH pipelined (fake client)                       (new)
├── test_sinks_rabbitmq.py   # persistent publish + reconnect (fake connection)         (new)
├── test_sinks_nats.py       # publish + JetStream path (fake client)                   (new)
├── test_sinks_pubsub.py     # publish + future flush (fake client)                     (new)
└── test_sinks_eventhubs.py  # 1MB batch packing + oversized drop (fake producer)       (new)
```

## Implementation Phases

### Phase 1: AWS streaming family (reuse the SQSSink boto3 pattern)

- Implement `KinesisSink`, `FirehoseSink`, and `SNSSink` with lazy boto3, count/byte chunking, and
  partial-failure retry (FR-001, FR-003, FR-004, FR-010, FR-011).
- Test each against an injected fake boto3 client: chunk boundaries, full coverage, and Failed-list
  retry.

### Phase 2: Kafka and Redis

- Implement `KafkaSink` (produce + `close()` flush) and `RedisStreamsSink`/`RedisListSink` (pipelined
  `XADD`/`RPUSH`) (FR-002, FR-005).
- Test produce-per-event with a fake producer and pipelined writes with a fake Redis client.

### Phase 3: RabbitMQ and NATS

- Implement `RabbitMQSink` (persistent publish + reconnect) and `NATSSink` (sync-driven publish, +
  JetStream) (FR-006, FR-007).
- Test publish parameters/reconnect and the JetStream branch with fake connections/clients.

### Phase 4: Google Pub/Sub and Azure Event Hubs

- Implement `GooglePubSubSink` (publish + future flush) and `AzureEventHubsSink` (1 MB batch packing +
  oversized drop) (FR-008, FR-009).
- Test future flushing and batch packing/oversized handling with fake clients/producers.
