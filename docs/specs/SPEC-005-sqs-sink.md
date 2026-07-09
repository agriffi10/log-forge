# Spec: SQSSink and Optional `sqs` Extra

**ID:** SPEC-005
**Status:** Draft
**Last Updated:** 2026-07-09
**Depends On:** SPEC-004

## Overview

`StdoutSink` is the zero-dependency dev default; `SQSSink` is the headline production path
(architecture §8, §9.1). SQS is the durable buffer that decouples the application from ELK
availability — events accumulate safely in the queue during downstream spikes or outages
instead of being lost or back-pressuring the app. The background worker (SPEC-004) already
batches by count and time; this spec adds the sink that ships those batches to an SQS queue,
respecting SQS's hard limits (≤ 10 messages and ≤ 256 KB per `SendMessageBatch`) by
re-chunking on both count and bytes, handling partial-batch failures via the response's
`Failed` list, and dropping (with a logged warning) any single event too large to ever fit.
`boto3` stays an **optional** dependency so stdout-only users remain dependency-free.

## Scope

### In Scope

- `SQSSink(queue_url, client=None)` implementing the SPEC-001 `Sink` protocol
  (`emit`/`close`).
- Re-chunking an incoming batch into SQS-valid sends: ≤ 10 messages **and** ≤ 256 KB per send,
  each event serialized with `json.dumps`.
- Calling `send_message_batch` and inspecting the response `Failed` list; retrying/logging
  failed entries.
- Dropping an individual event that exceeds the per-message/256 KB limit on its own, with a
  logged warning, rather than crashing the whole batch.
- A local `import boto3` inside `SQSSink` (never at module top) so `boto3` is required only when
  the sink is actually used. The `sqs` extra (`boto3>=1.34`) is already declared in
  `pyproject.toml`.

### Out of Scope

- FIFO-queue semantics (`MessageGroupId`/dedup) — standard queues only in this spec.
- Consuming from SQS / indexing into ELK — a separate component, out of scope for this library
  (architecture §9.1, §13).
- Changing the worker's batching contract — the sink re-chunks; it does not assume the worker's
  batch already fits.
- Other sinks (`FileSink`, `HTTPSink`, `KafkaSink`) — deferred (architecture §8).

---

## Functional Requirements

### FR-001: SQSSink conforms to the Sink protocol

#### Description:

`SQSSink` is a drop-in `Sink` usable via `configure(sink=SQSSink(queue_url=...))` with no
change elsewhere in the pipeline.

#### Acceptance Criteria:

- [ ] `SQSSink(queue_url, client=None)` stores the queue URL and uses the injected `client`, or
      creates one via `boto3.client("sqs")` when `client is None`.
- [ ] `import boto3` happens inside `SQSSink` (constructor/method), not at module top level, so
      importing `log_forge.sinks.sqs` does not require `boto3` unless instantiated.
- [ ] `SQSSink` satisfies `emit(batch: list[dict]) -> None` and `close() -> None` and passes an
      `isinstance(sink, Sink)` runtime-checkable check.
- [ ] A test can inject a fake SQS client and assert on the `send_message_batch` calls without
      any AWS/network access.

### FR-002: Count- and byte-aware chunking

#### Description:

An incoming batch is split into SQS-valid sends respecting both the 10-message and 256 KB
limits.

#### Acceptance Criteria:

- [ ] No `send_message_batch` call contains more than 10 message entries.
- [ ] No `send_message_batch` call's combined payload exceeds 256 KB.
- [ ] A batch larger than one send is split across multiple `send_message_batch` calls covering
      every event exactly once (no loss, no duplication).
- [ ] Each message body is the `json.dumps` serialization of one event dict.

### FR-003: Partial-failure handling

#### Description:

`send_message_batch` can partially fail; failed entries are surfaced and retried, not silently
lost.

#### Acceptance Criteria:

- [ ] After each `send_message_batch`, the response's `Failed` list is inspected.
- [ ] Failed entries are retried (bounded attempts) and/or logged; a partial failure does not
      discard the successfully-sent entries.
- [ ] Entries still failing past the retry bound are logged (counted), not silently dropped.

### FR-004: Oversized-event handling

#### Description:

A single event too large to fit one message must not crash the whole batch.

#### Acceptance Criteria:

- [ ] An individual event whose serialized size exceeds the per-message / 256 KB limit is
      dropped with a logged warning.
- [ ] Dropping one oversized event does not prevent the remaining events in the batch from being
      sent.

### FR-005: close()

#### Description:

`SQSSink` holds no internal buffer, so `close()` is a no-op beyond satisfying the protocol.

#### Acceptance Criteria:

- [ ] `SQSSink.close()` returns without error and requires no flush (nothing is buffered inside
      the sink itself).

---

## Data Model

```
# src/log_forge/sinks/sqs.py
SQSSink {
  MAX_BATCH = 10             # SQS SendMessageBatch hard limit (entries)
  MAX_BYTES = 256 * 1024     # 256 KB per batch
  queue_url: str
  client: <boto3 sqs client> # injected or boto3.client("sqs")
}
```

Events are the SPEC-001 `LogEvent` dicts; each becomes one SQS message body via `json.dumps`.

---

## API / Interface Contract

```python
# sinks/sqs.py
class SQSSink:
    MAX_BATCH = 10
    MAX_BYTES = 256 * 1024
    def __init__(self, queue_url: str, client=None) -> None: ...
    def emit(self, batch: list[dict]) -> None: ...   # chunk ≤10 & ≤256KB, send, handle Failed
    def close(self) -> None: ...                     # no-op

# Usage
import log_forge
from log_forge.sinks.sqs import SQSSink

log_forge.configure(service="payments", sink=SQSSink(queue_url="https://sqs..."))
```

## Configuration / Environment

- Installed via the existing optional extra: `pip install log-forge[sqs]` /
  `poetry install --extras sqs` (`boto3>=1.34`, already declared in `pyproject.toml`).
- AWS credentials/region are resolved by `boto3` through its standard chain — log-forge adds no
  new credential configuration.

## File & Folder Structure

```
src/log_forge/
└── sinks/
    └── sqs.py         # SQSSink: chunking, send_message_batch, Failed handling   (new)
tests/
└── test_sinks_sqs.py  # chunking by count+bytes, Failed retry, oversized drop    (new; fake client)
```

## Implementation Phases

### Phase 1: SQSSink emit + chunking

- Implement `SQSSink` with the local `boto3` import, `MAX_BATCH`/`MAX_BYTES` constants, and
  count+byte re-chunking into `send_message_batch` calls (FR-001, FR-002, FR-005).
- Test chunking boundaries with a fake SQS client (10-entry cap, 256 KB cap, full coverage).

### Phase 2: Failure paths

- Add `Failed`-list inspection with bounded retry and oversized-event drop-with-warning
  (FR-003, FR-004).
- Test partial-failure retry and that one oversized event is dropped without sinking the rest.
