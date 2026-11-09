---
type: Decision
title: "A FIFO message group is a trace, not the process"
description: "`MessageGroupId` defaults to the event's `trace_id`, which keeps traces parallel instead of serialising everything behind one group. Ordering is best-effort across a retry boundary — since SPEC-074 only past the sink's own retries, a later message being held while an earlier one of its group has sends left — and sender faults are abandoned rather than re-sent. (SPEC-016, SPEC-074)"
---

**A FIFO message group is a trace, not the process** — `MessageGroupId` defaults to the event's `trace_id`: SQS orders *within* a group, and a trace is the unit whose events must stay ordered, while per-trace groups keep traces parallel instead of serializing everything behind one group. Overridable with a constant or a callable. Ordering is best-effort across a retry boundary, and sender faults are abandoned rather than re-sent byte-identical. (SPEC-016) Refined by SPEC-074: a later message of a group is held back, per chunk at send time, while an earlier one has neither landed nor used its `max_retries + 1` sends, so ordering is best-effort only past the sink's own retries (*A sink round resends only what failed, and an SQS FIFO group keeps its order*).

## History

### 2026-11-09

- **Refined** by SPEC-074 FR-002: the group keeps its order for an earlier message's `max_retries + 1` sends, and is best-effort past them.

### 2026-07-22

- **Settled.**
