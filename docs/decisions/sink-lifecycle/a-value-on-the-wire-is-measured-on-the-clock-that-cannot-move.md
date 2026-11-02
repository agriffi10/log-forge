---
type: Decision
title: "A value on the wire is measured on the clock that cannot move"
description: "rotation deadlines are `time.monotonic()`, since a wall-clock deadline is defeated by any step larger than the interval. The *label* stays wall-clock wherever one exists. (SPEC-031)"
---

**A value on the wire is measured on the clock that cannot move** — `RotatingFileSink`'s rotation deadline is `time.monotonic()`, as `Span.start_ts` already was, because a wall-clock deadline is defeated by any step larger than the interval. The *label* stays wall-clock wherever one exists; here none does, since backups are numbered rather than timestamped. (SPEC-031 FR-001)

## History

### 2026-08-07

- **Settled.**
