---
type: Decision
title: "A FIFO message group is a trace, not the process"
description: "`MessageGroupId` defaults to the event's `trace_id`, which keeps traces parallel instead of serialising everything behind one group. Ordering is best-effort across a retry boundary, and sender faults are abandoned rather than re-sent. (SPEC-016)"
---

**A FIFO message group is a trace, not the process** — `MessageGroupId` defaults to the event's `trace_id`: SQS orders *within* a group, and a trace is the unit whose events must stay ordered, while per-trace groups keep traces parallel instead of serializing everything behind one group. Overridable with a constant or a callable. Ordering is best-effort across a retry boundary, and sender faults are abandoned rather than re-sent byte-identical. (SPEC-016)

## History

### 2026-07-22

- **Settled.**
