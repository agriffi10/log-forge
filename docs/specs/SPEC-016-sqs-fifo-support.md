# Spec: FIFO Queue Support for `SQSSink`

**ID:** SPEC-016
**Status:** Completed
**Last Updated:** 2026-07-23
**Depends On:** SPEC-005 (`SQSSink`, chunking + `Failed`-list retry)

## Overview

`SQSSink` cannot ship to a FIFO queue. SPEC-005 scoped FIFO out ("standard queues only"), so every
batch entry is built as `{"Id", "MessageBody"}` — and a FIFO queue rejects any entry without a
`MessageGroupId`. The rejection is a *per-entry* sender fault, so nothing raises: the entries come
back in the response's `Failed` list, the bounded-retry loop re-sends the same invalid entries
`max_retries + 1` times, and the batch ends as one line on stderr with every event lost. A user who
points the sink at a `.fifo` queue gets silence, four times the API calls, and no logs.

This spec makes a FIFO queue a supported destination. The default `MessageGroupId` is the event's
own `trace_id`, which is the grouping the library already means: FIFO guarantees ordering *within* a
group, and a trace is precisely the unit whose events should stay ordered. Per-trace groups also
keep traces independent of one another, so the queue delivers them in parallel rather than
serializing the whole process behind one group. The group is configurable — a constant for callers
who want strict global ordering, or a callable for anything else.

## Scope

### In Scope

- **FIFO detection**: a `.fifo` queue URL suffix selects FIFO behaviour, with an explicit override.
- **`MessageGroupId` on every entry when FIFO**, defaulting to that event's `trace_id`, and
  configurable as a constant string or a callable over the event.
- **`MessageDeduplicationId` on every entry when FIFO**, defaulting to that event's `log_id` — a
  per-event UUID, so no two distinct events are ever collapsed by SQS's 5-minute dedup window.
- **Byte accounting for the added parameters**, so a FIFO batch cannot overshoot the 256 KB request
  limit that SPEC-005's chunker enforces.
- **Not retrying sender-fault entries.** An entry SQS marks `SenderFault: true` is invalid as
  written; re-sending it unchanged can never succeed. This is in scope because it is the mechanism
  that turns a FIFO misconfiguration into four silent, doomed attempts — and it is on the same code
  path this spec is already editing. It is severable: dropping FR-006 leaves the rest coherent.
- Standard (non-FIFO) queues keep byte-identical behaviour — a regression guard, not a refactor.

### Out of Scope

- **`SNSSink` FIFO topics.** `sinks/sns.py:66` has the identical gap (`{"Id", "Message"}` entries;
  SNS FIFO topics also require a `MessageGroupId`). Same fix, different transport — a follow-up
  spec, so this one stays reviewable.
- **Omitting the dedup id to rely on queue-side content-based deduplication.** An explicit
  `MessageDeduplicationId` is valid on both queue configurations and takes precedence where both
  are present, so always sending one is strictly more predictable than branching on queue state the
  sink cannot see.
- **Queue-side configuration.** Creating the queue, enabling high-throughput mode, and the FIFO
  throughput ceiling (300 TPS, 3000 with 10-entry batching) are the operator's, not the library's.
- **Ordering guarantees under partial failure.** If one entry fails and is retried while a
  same-group entry ahead of it succeeded, the retry lands *after* it. FIFO ordering is therefore
  best-effort across a retry boundary. Documented as a known limitation (FR-002), not solved here —
  solving it means blocking a whole group on one failure, which trades log delivery for ordering
  the consumer can already reconstruct from `timestamp`.
- **Changing the worker's batching contract**, the `Sink` protocol, or any public API outside
  `SQSSink.__init__`.

---

## Functional Requirements

### FR-001: FIFO detection with an explicit override

#### Description:

The sink decides once, at construction, whether it is talking to a FIFO queue. AWS requires every
FIFO queue name to end in `.fifo`, so the URL suffix is a contract rather than a heuristic — but a
caller can still state it outright.

#### Acceptance Criteria:

- [ ] `SQSSink(queue_url=".../logs.fifo")` sets `self.fifo` to `True`; a URL without the suffix sets
      it to `False`.
- [ ] `SQSSink(..., fifo=True)` forces FIFO behaviour on a URL lacking the `.fifo` suffix.
- [ ] `SQSSink(..., fifo=False)` forces standard behaviour on a `.fifo` URL.
- [ ] Detection is case-sensitive on the documented `.fifo` suffix and is evaluated once in
      `__init__`, not per `emit`.

### FR-002: `MessageGroupId` per entry, defaulting to `trace_id`

#### Description:

When FIFO, every batch entry carries a `MessageGroupId` derived from *its own* event, so a batch
spanning several traces produces several groups in one request.

#### Acceptance Criteria:

- [ ] Every entry in every `send_message_batch` call includes a non-empty `MessageGroupId`.
- [ ] By default, an entry's `MessageGroupId` equals `str(event["trace_id"])` for the event that
      entry was built from.
- [ ] A batch containing events from three different traces produces three distinct
      `MessageGroupId` values within a single `send_message_batch` call, each paired with the right
      body.
- [ ] `message_group_id="constant"` puts `"constant"` on every entry regardless of `trace_id`.
- [ ] `message_group_id=lambda event: ...` is called once per event and its return value is used.
- [ ] An event with no `trace_id` key, or one whose value is empty or whitespace, falls back to the
      module constant `DEFAULT_GROUP_ID` rather than sending an empty parameter.
- [ ] A **callable** returning an empty or whitespace value falls back the same way — SQS rejects an
      empty group id whatever produced it.
- [ ] `message_group_id=""` (or all-whitespace) raises `ValueError` at construction. It is a
      deterministic config error the caller can fix, so it fails fast rather than becoming a silent
      substitution that surfaces as a mystery group in their queue.
- [ ] A derived group id longer than SQS's 128-character maximum is truncated to 128 characters.
- [ ] The retry-reordering limitation is recorded in the `SQSSink` docstring.

### FR-003: `MessageDeduplicationId` per entry, defaulting to `log_id`

#### Description:

When FIFO, every entry carries a deduplication id that is unique per event, so SQS's 5-minute
dedup window never collapses two genuinely distinct log records.

#### Acceptance Criteria:

- [ ] Every entry in every `send_message_batch` call includes a non-empty
      `MessageDeduplicationId` when FIFO.
- [ ] By default, an entry's `MessageDeduplicationId` equals `str(event["log_id"])`.
- [ ] Two events identical in every field except `log_id` produce two different dedup ids.
- [ ] `message_deduplication_id=lambda event: ...` overrides the default and is called once per
      event.
- [ ] An event with no `log_id`, or an empty derived value, falls back to a freshly generated UUID
      (never an empty parameter, never a value shared with another entry).
- [ ] A derived dedup id longer than 128 characters is truncated to 128 characters.

### FR-004: Standard queues are unaffected

#### Description:

Every change here is gated on FIFO. A standard queue must produce exactly the entries it produces
today.

#### Acceptance Criteria:

- [ ] On a non-FIFO sink, each entry's keys are exactly `{"Id", "MessageBody"}` — no
      `MessageGroupId`, no `MessageDeduplicationId`.
- [ ] The existing `tests/test_sinks_sqs.py` passes unmodified.
- [ ] Chunk boundaries (≤ 10 entries, ≤ 256 KB) are unchanged for a non-FIFO sink given the same
      input batch.

### FR-005: FIFO parameters count toward the request byte budget

#### Description:

SPEC-005's chunker packs to 256 KB counting message bodies alone. FIFO adds up to 256 bytes of
parameters per entry, which must not push a request over the limit.

#### Acceptance Criteria:

- [ ] When FIFO, the chunker's running byte total includes each entry's `MessageGroupId` and
      `MessageDeduplicationId` lengths alongside its body.
- [ ] A batch of bodies that fits one standard-queue request but exceeds 256 KB once FIFO
      parameters are added is split into more than one `send_message_batch` call.
- [ ] The oversized-single-event drop (SPEC-005 FR-004) is judged on everything that travels in
      the entry — body plus, on FIFO, the resolved ids. *(Revised post-delivery: this criterion
      originally required the drop to trigger on body size alone "so droppability does not depend
      on queue type", which contradicted this FR's own description in a narrow band. A body just
      under 256 KB passed the body-alone check, then shipped as a lone request over the limit. SQS
      rejects that as a sender fault, which FR-006 never retries — so the event was lost anyway,
      as an opaque `failed` rather than a labelled `dropped_oversized`.)*

### FR-006: Sender-fault entries are not retried

#### Description:

SQS marks an entry `SenderFault: true` when the request itself is invalid. Re-sending it unchanged
cannot succeed, so the retry budget is spent for nothing and the real cause stays buried.

#### Acceptance Criteria:

- [ ] An entry returned with `SenderFault: true` is not re-sent; it is counted in `failed`
      immediately.
- [ ] An entry returned with `SenderFault: false` (a throttle or internal error) is still retried
      under the existing `max_retries` bound.
- [ ] A response mixing both kinds retries only the non-sender-fault entries.
- [ ] The stderr warning for abandoned sender-fault entries names the SQS error `Code` from the
      first such entry, so a missing-parameter failure is diagnosable from the log line alone.
- [ ] A sink misconfigured against a FIFO queue makes exactly one `send_message_batch` call per
      chunk, not `max_retries + 1`.

---

## Data Model

```python
# src/log_foundry/sinks/sqs.py

DEFAULT_GROUP_ID = "log-foundry"   # fallback when an event carries no usable trace_id
MAX_ID_LEN = 128                   # SQS limit for MessageGroupId / MessageDeduplicationId

GroupIdSource = str | Callable[[dict[str, object]], str] | None
DedupIdSource = Callable[[dict[str, object]], str] | None

SQSSink {
  MAX_BATCH = 10
  MAX_BYTES = 256 * 1024
  queue_url: str
  client: <boto3 sqs client>
  max_retries: int
  fifo: bool                       # detected from the .fifo suffix, or forced
  dropped_oversized: int
  failed: int
}
```

`_chunks` currently returns `list[list[str]]` — bodies only, with the source event discarded. A
per-entry group id needs more, so it becomes `list[list[_Prepared]]`:

```python
class _Prepared(NamedTuple):
    body: str
    group_id: str | None      # None on a standard queue
    dedup_id: str | None
```

The ids are resolved **once**, in `_chunks`, rather than again at send time. Deriving twice would
be a latent bug, not just waste: the dedup fallback mints a fresh UUID, so the byte budget would be
billed for one value while a different one went on the wire. This is internal; `emit`/`close` are
unchanged.

A FIFO entry:

```python
{
  "Id": "0",
  "MessageBody": '{"timestamp": ..., "trace_id": "4bf92f...", "log_id": "..."}',
  "MessageGroupId": "4bf92f3577b34da6a3ce929d0e0e4736",   # event["trace_id"]
  "MessageDeduplicationId": "9f1c2d5e-...",               # event["log_id"]
}
```

---

## API / Interface Contract

```python
class SQSSink:
    def __init__(
        self,
        queue_url: str,
        client: Any = None,
        *,
        max_retries: int = 3,
        fifo: bool | None = None,                  # None = detect from the .fifo suffix
        message_group_id: GroupIdSource = None,    # None = event["trace_id"]
        message_deduplication_id: DedupIdSource = None,  # None = event["log_id"]
    ) -> None: ...

# Default: one group per trace — ordered within a trace, parallel across traces.
log_foundry.configure(service="payments", sink=SQSSink(queue_url="https://sqs.../logs.fifo"))

# Strict global ordering: one group for the whole process (caps throughput at ~300 msg/s).
SQSSink(queue_url="https://sqs.../logs.fifo", message_group_id="payments")

# Anything else: group by tenant, by service, by span.
SQSSink(
    queue_url="https://sqs.../logs.fifo",
    message_group_id=lambda event: str(event["fields"].get("tenant_id", DEFAULT_GROUP_ID)),
)
```

## Configuration / Environment

No new config keys, env vars, or dependencies. FIFO support is constructor arguments on `SQSSink`
only; the `aws` extra (`boto3>=1.34`) already covers it, and `configure()` is untouched.

## File & Folder Structure

```
src/log_foundry/
└── sinks/
    └── sqs.py                  # FIFO detection, per-entry ids, byte accounting, sender-fault  (edit)
tests/
├── test_sinks_sqs.py           # unchanged — the FR-004 regression guard                      (existing)
└── test_sinks_sqs_fifo.py      # detection, group/dedup derivation, byte budget, sender fault  (new)
```

## Implementation Phases

### Phase 1: FIFO entries

- Add `fifo` detection + override, the `message_group_id` / `message_deduplication_id` arguments,
  and the `DEFAULT_GROUP_ID` / `MAX_ID_LEN` constants (FR-001).
- Rework `_chunks` to carry `(body, event)` pairs and add the FIFO byte accounting (FR-005).
- Derive and attach per-entry `MessageGroupId` and `MessageDeduplicationId`, with the fallback and
  truncation rules (FR-002, FR-003).
- Tests: a mixed-trace batch yields one group per trace in one request; constant and callable
  overrides; missing/empty/oversized `trace_id` and `log_id`; a standard-queue sink still emits
  exactly `{"Id", "MessageBody"}` (FR-004).

### Phase 2: Sender-fault handling

- Split the `Failed` list on `SenderFault` in `_send`: retry the retryable, abandon and count the
  rest, and name the SQS `Code` in the stderr warning (FR-006).
- Tests: sender-fault-only, retryable-only, and mixed responses; a FIFO-misconfigured sink makes one
  call per chunk rather than `max_retries + 1`.
