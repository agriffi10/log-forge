---
type: Decision
title: "A FIFO message group is a trace, not the process"
description: "on an SQS FIFO queue and, since SPEC-100, an SNS FIFO topic, `MessageGroupId` defaults to the event's `trace_id`, which keeps traces parallel instead of serialising everything behind one group. Ordering is best-effort across a retry boundary — since SPEC-074 only past the sink's own retries, a later message being held while an earlier one of its group has sends left — and sender faults are abandoned rather than re-sent. (SPEC-016, SPEC-074, SPEC-100)"
---

**A FIFO message group is a trace, not the process** — `MessageGroupId` defaults to the event's `trace_id`: SQS orders *within* a group, and a trace is the unit whose events must stay ordered, while per-trace groups keep traces parallel instead of serializing everything behind one group. Overridable with a constant or a callable. Ordering is best-effort across a retry boundary, and sender faults are abandoned rather than re-sent byte-identical. (SPEC-016) Refined by SPEC-074: a later message of a group is held back, per chunk at send time, while an earlier one has neither landed nor used its `max_retries + 1` sends, so ordering is best-effort only past the sink's own retries (*A sink round resends only what failed, and an SQS FIFO group keeps its order*). Widened by SPEC-100 to `SNSSink`: a `.fifo` topic ARN selects FIFO behaviour, and its group is resolved, defaulted, cut to 128 characters and held by the same code as `SQSSink`'s, `sinks/_fifo.py`, so the rule cannot read differently on the two sinks (invariant 6).

## History

### 2026-11-24

- **Widened** by SPEC-100 FR-002 and FR-004: the rule covers a FIFO `SNSSink` as well as `SQSSink`, through the one module both sinks call, `sinks/_fifo.py`. Nothing about `SQSSink`'s group changed.

### 2026-11-09

- **Refined** by SPEC-074 FR-002: the group keeps its order for an earlier message's `max_retries + 1` sends, and is best-effort past them.

### 2026-07-22

- **Settled.**
