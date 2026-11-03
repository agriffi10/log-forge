---
type: Decision
title: "A client exception costs its chunk, and is provable non-delivery for it"
description: "guarded *inside* `_send`, since an outer guard reports a partial success as \"nothing delivered\"; it feeds SQS's recoverable term, or a total failure returns silently. (SPEC-048)"
---

**A client exception costs its chunk, and is provable non-delivery for it** — the four AWS batch sinks called their client unguarded inside a chunk loop, so a `ClientError` on chunk N escaped `emit` after chunks 1..N-1 had landed, and the worker retries *whole batches*: measured `duplicates=10` for a 25-event SQS batch and `duplicates=500` for 1,000 Kinesis records, `losses()` reading `(0, 0)` throughout. The exit drain is one large batch by construction, so it is exactly this shape. **The guard goes inside `_send`, never around it in `emit`**: by the time `_send` raises it may hold a non-zero `accepted` from an earlier attempt and have narrowed its entries to the retryable subset, so an outer guard charges the whole chunk and reports a partial success as "nothing delivered" — provoking the very retry it was added to remove. **Treated as provable non-delivery**, which feeds `SQSSink`'s `recoverable_loss` term so a wholly-failed batch still raises; without that the guard converts a total failure into a silent one, which is SPEC-026's defect reintroduced by its own fix. The cost is real and recorded rather than argued away: a read timeout means the request went out and the *reply* was lost, so a re-send may duplicate — SPEC-018's "cannot prove nothing landed". It is taken on expected cost, because an unreachable endpoint is the common client exception and is precisely what the worker's retry exists for, while suppressing the raise for it would lose every event of every batch for a whole outage; `boto3`'s own `max_attempts` already carries the duplication property. `KinesisSink`/`FirehoseSink`'s `unknown` term is **not** set by the guard: SPEC-018's "unadjudicable" describes a *response*, and an exception is not a response. The roster that keeps this true is derived on **shape** — every `_send` calling `self.client.<method>(...)` — with a floor, because a hard-coded four-module list leaves a fifth AWS-shaped sink green. (SPEC-048 FR-002)

## History

### 2026-09-02

- **Settled.**
