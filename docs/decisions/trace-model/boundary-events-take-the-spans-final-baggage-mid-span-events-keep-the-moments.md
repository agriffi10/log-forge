---
type: Decision
title: "Boundary events take the span's *final* baggage; mid-span events keep the moment's"
description: "backfill only at close. Backfilling everything inverts `build_event`'s precedence and lets baggage beat a per-call field. (SPEC-015)"
---

**Boundary events take the span's *final* baggage; mid-span events keep the moment's** — one backfill at close completes `span.start`/`span.end` (which describe the whole span and carry the outcome), while an `info` is left exactly as it was emitted. Backfilling everything would also invert `build_event`'s precedence by letting baggage beat a per-call field. (SPEC-015)

## History

### 2026-07-22

- **Settled.**
