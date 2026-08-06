# Spec: Sink Loss Visibility

**ID:** SPEC-026  
**Status:** Completed  
**Last Updated:** 2026-08-06  
**Depends On:** SPEC-017, SPEC-018, SPEC-019, SPEC-021

## Overview

Four specs built a loss-reporting apparatus: `failed_batches` counts abandoned batches (SPEC-017),
`dropped_unadjudicated` counts records a response could not describe (SPEC-018), `stopped_reason`
reports a dead drain thread (SPEC-019), and `flush()` returns whether the drain that carried a
caller's events actually delivered them (SPEC-021). `health()` is documented as the way an operator
notices absorbed loss: *"this is how you notice them."*

Against every shipped remote transport, none of it fires.

Each of those sinks catches its own failures, counts them on a private instance attribute, writes a
line to stderr and returns normally — `_socket.py:83`, `http.py:127-139`, `kafka.py:70`,
`kinesis.py:109`, `firehose.py:102`, `sns.py:77`, `elasticsearch.py:63`, `postgres.py:68`,
`clickhouse.py:95`, `mongodb.py:62`, `redis.py:47`, `nats.py:65`, `pubsub.py:42`, `rabbitmq.py:80`,
`eventhubs.py:85`. A sink that reports success is a sink the worker believes: the retry never
engages, `failed_batches` stays at zero, and `flush()` returns `True`. With a dead syslog socket the
measured result is `flush() == True` and `health() == (0, 0, 0, None)` while every message is lost —
the exact reading SPEC-017 existed to make impossible.

The counters that *do* record the loss are unreachable. `SocketTransport.failed`,
`HTTPSink.failed`, `SQSSink.dropped_oversized`, `KinesisSink.dropped_unadjudicated`,
`MultiSink.failed` and the rest are bare instance attributes with no accessor and no aggregation —
and when `_ensure_sink()` builds the default `StdoutSink` (`config.py:120-123`), the application
holds no reference to the sink object at all.

This spec makes sink-level loss reach the operator: total failure of a batch signals the worker, and
partial loss is readable through `health()`. It is SPEC-017 FR-004's rule — an all-children-down
`MultiSink` must not report success — generalized to every sink that ships.

## Scope

### In Scope

- A rule for when a sink must raise rather than absorb, applied across the sink family.
- An optional `losses()` protocol method on `Sink`, for loss a sink absorbs legitimately.
- Aggregation of that into `health()`, so the documented detector sees what actually happened.
- Retro-fitting both to the shipped sinks, including `SocketTransport`.
- Documenting the contract in `sinks/base.py`, so a third-party sink can satisfy it.
- Correcting `architecture.md` §9 and the `health()` docstring, which currently read as general
  guarantees.

### Out of Scope

- **Changing the sinks' retry policies.** How many times and how long a sink retries before giving
  up belongs to SPEC-027. This spec governs only what it *reports* once it has.
- **Making sinks raise on partial failure.** A batch where 9 of 10 records landed must not be
  retried wholesale — that re-delivers the 9 and creates duplicates, which SPEC-017 FR-004 and
  SPEC-018 both settled as worse than the counted loss. Partial loss is reported through `losses()`,
  never by raising.
- **Retrying `dropped_unadjudicated` or `dropped_oversized` records.** SPEC-018 settled that an
  unadjudicable chunk is abandoned rather than re-sent, and an oversized event can never fit. Both
  stay abandoned; this spec only makes them visible.
- **A metrics or callback interface.** `health()` is a poll, deliberately. An emit-time hook for
  loss events is a larger design and is not opened here.
- **Removing the per-sink stderr lines.** They stay — an operator reading logs and an operator
  polling `health()` are different people. (Their *content* is SPEC-029's concern.)
- **`StdoutSink`, `FileSink`, `SQLiteSink`, `LoggingSink`, `CallbackSink`.** These already
  propagate, which is correct; they gain `losses()` only where they already count something.

---

## Functional Requirements

### FR-001: A sink that delivered nothing raises

#### Description:

When a sink's `emit` call results in **none** of the batch reaching the destination, it raises
rather than returning. That is the signal the worker's bounded retry and `failed_batches` accounting
are built on, and the condition under which a retry cannot create duplicates — there is nothing
downstream to duplicate.

This is `MultiSink`'s existing rule (`multi.py:67-72`), stated once and applied to the family. A
sink that delivered *some* of the batch does not raise; see FR-002.

The exception is raised after the sink has exhausted its own retries, so the worker's retry composes
on top of the sink's rather than replacing it. Sinks whose own retry budget makes the worker's
redundant should say so in their docstring rather than silently absorbing.

#### Acceptance Criteria:

- [ ] `SocketTransport.send_all` raises when every message in the call failed; `SyslogSink.emit` and
      `LogstashSink.emit` (socket mode) propagate it.
- [ ] `HTTPSink.emit` raises when the request was abandoned, and every platform subclass
      (Datadog, Splunk, New Relic, Honeycomb, Loki, Elasticsearch, Logstash-HTTP) inherits that.
- [ ] The same holds for `KafkaSink`, `RedisSink`, `RabbitMQSink`, `NATSSink`, `PubSubSink`,
      `EventHubsSink`, `SNSSink`, `KinesisSink`, `FirehoseSink`, `MongoSink`, `PostgresSink`,
      `ClickHouseSink` on total failure of a batch.
- [ ] With such a sink permanently failing, `health().failed_batches` becomes non-zero and
      `log_foundry.flush()` returns `False` — the two assertions that fail today.
- [ ] The worker still survives: the thread stays alive, `stopped_reason` stays `None`, and draining
      continues with later batches.
- [ ] A sink that succeeds is unaffected — no new exception type, no change to the success path.
- [ ] An empty batch is not a total failure: `emit([])` remains a no-op and never raises.

### FR-002: Partial and abandoned loss is reported, not raised

#### Description:

Loss a sink absorbs deliberately — an oversized event, an unadjudicable chunk, a partially-failed
batch past its retry bound, a `MultiSink` child that failed while siblings succeeded — is counted
and made readable, without raising.

`Sink` gains an **optional** `losses()` method returning a `SinkLosses` snapshot. Optional because
`Sink` is a `Protocol` and a third-party sink written against the current interface must keep
working; the worker probes for it and treats its absence as "reports nothing".

#### Acceptance Criteria:

- [ ] `SinkLosses` carries `dropped: int` (events the sink discarded before sending) and
      `failed: int` (events it sent and could not confirm), both cumulative for the sink's lifetime.
- [ ] Every shipped sink that today counts loss on a private attribute exposes it through
      `losses()`, mapping its existing counters onto those two fields.
- [ ] `MultiSink.losses()` sums its children's, so a fan-out reports the whole tree.
- [ ] A sink with no `losses()` method is handled without error and contributes zero.
- [ ] `losses()` never raises and is safe to call concurrently with `emit`.
- [ ] An oversized-event drop and an unadjudicated chunk both appear, so SPEC-018's counter finally
      has a reader.

### FR-003: `health()` reports sink loss

#### Description:

`Health` gains one field carrying the sink's `losses()` snapshot, so the documented alert idiom
covers loss the worker never saw.

It is a nested value rather than two more flat integers: the worker's counters and the sink's mean
different things — `dropped` on the worker is backpressure at the queue, `dropped` on the sink is an
event the destination could never accept — and flattening them into one number would make the
remedies indistinguishable, which is the mistake SPEC-019 avoided with `stopped_reason`.

The field is appended, as SPEC-019 appended `stopped_reason`, keeping attribute and index access
working.

#### Acceptance Criteria:

- [ ] `Health` carries `sink: SinkLosses | None`, appended after `stopped_reason`, defaulting to
      `None`.
- [ ] It is `None` when no worker exists and when the sink implements no `losses()`.
- [ ] It reflects the configured sink's counters at the moment `health()` is called.
- [ ] Reading it never raises, even if the sink's `losses()` does — a failing accessor yields `None`
      rather than propagating into `health()`, which is documented "Never raises".
- [ ] The existing fields keep their positions and meanings.
- [ ] The README's alert idiom is extended to cover it, and states that a non-zero sink `dropped`
      has a different remedy from a non-zero worker `dropped`.

### FR-004: The contract is documented where a sink author will read it

#### Description:

`sinks/base.py` is the whole interface definition and today says only "Ship a batch of serialized
event dicts". It must state the raise-on-total-failure rule and the `losses()` option, because a
third-party sink that absorbs silently reintroduces exactly this defect.

#### Acceptance Criteria:

- [ ] `Sink.emit`'s docstring states that total failure must raise and partial failure must not.
- [ ] `Sink.losses` is defined as optional, with its semantics.
- [ ] `architecture.md` §9's "Emit failures are retried with backoff" is qualified with what the
      library relies on the sink to do.
- [ ] The `health()` docstring and README describe the new field.
- [ ] The README's "writing your own sink" guidance (or a new short section) states both rules.

---

## Data Model

```python
# src/log_foundry/sinks/base.py

class SinkLosses(NamedTuple):
    dropped: int   # events the sink discarded without attempting delivery (oversized, filtered out)
    failed: int    # events sent whose delivery could not be confirmed (abandoned, unadjudicated)


@runtime_checkable
class Sink(Protocol):
    def emit(self, batch: list[dict[str, object]]) -> None: ...
    def close(self) -> None: ...
    # Optional — probed with hasattr; absence means "reports nothing".
    # def losses(self) -> SinkLosses: ...


# src/log_foundry/worker.py

class Health(NamedTuple):
    queued: int
    dropped: int
    failed_batches: int
    stopped_reason: str | None = None
    sink: SinkLosses | None = None      # new
```

`SinkLosses` lives in `base.py` beside the Protocol it belongs to, and `worker.py` imports it under
`TYPE_CHECKING` — the same discipline `config.py` uses for `Sink`, so no import cycle appears.

---

## API / Interface Contract

```python
# The rule, as a sink implements it:

def emit(self, batch):
    delivered = 0
    for chunk in self._chunks(batch):
        if self._send(chunk):        # sink's own bounded retry
            delivered += len(chunk)
        else:
            self._failed += len(chunk)
    if delivered == 0 and batch:
        raise SinkDeliveryError(f"{type(self).__name__} delivered none of {len(batch)} event(s)")


def losses(self):
    return SinkLosses(dropped=self._dropped_oversized, failed=self._failed)


# Caller side — the alert idiom, complete:
h = log_foundry.health()
if h.dropped or h.failed_batches or h.stopped_reason or (h.sink and (h.sink.dropped or h.sink.failed)):
    ...  # logs were lost
```

`SinkDeliveryError` is a new `Exception` subclass in `sinks/base.py`, so an operator reading a
stderr line or a `stopped_reason` can tell a delivery failure from a bug in a sink. Sinks that
already have a natural exception to re-raise (a driver error, a `MultiSink` child's) re-raise that
instead — the rule is that *something* propagates, not that it must be this type.

## Configuration / Environment

None.

## File & Folder Structure

```
src/log_foundry/
├── worker.py                # modified — Health.sink, reading it safely
├── sinks/
│   ├── base.py              # modified — SinkLosses, SinkDeliveryError, the documented contract
│   ├── _socket.py           # modified — raise on total failure; losses()
│   ├── http.py              # modified — raise when a request is abandoned; losses()
│   ├── multi.py             # modified — losses() summing children
│   └── (each remote sink)   # modified — total-failure raise + losses()
└── __init__.py              # modified — health() docstring

tests/
├── test_worker.py           # modified — Health.sink aggregation, failing losses()
├── test_sinks_compose.py    # modified — MultiSink aggregation
└── (per-sink test modules)  # modified — total-failure raises, partial does not

docs/architecture.md         # modified — §9 qualification
README.md                    # modified — alert idiom, sink-author rules
```

## Implementation Phases

### Phase 1: The contract

- `SinkLosses`, `SinkDeliveryError`, and the documented rules in `sinks/base.py`.
- `Health.sink` and its safe read in `Worker.health`.
- Tests: aggregation, absent `losses()`, raising `losses()`, field positions.

### Phase 2: The zero-dependency and composition sinks

- `SocketTransport` (and `SyslogSink`/`LogstashSink` socket mode), `HTTPSink` and its platform
  subclasses, `MultiSink`.
- Tests: total failure raises and reaches `failed_batches`/`flush()`; partial does not raise.

### Phase 3: The queue, stream and database sinks

- Kafka, Redis, RabbitMQ, NATS, Pub/Sub, Event Hubs, SNS, Kinesis, Firehose, Mongo, Postgres,
  ClickHouse; `SQSSink`'s response-`Failed` path.
- Tests per sink, following the Phase 2 pattern.

### Phase 4: Documentation

- `architecture.md` §9; `health()` docstring; README alert idiom and sink-author rules.
