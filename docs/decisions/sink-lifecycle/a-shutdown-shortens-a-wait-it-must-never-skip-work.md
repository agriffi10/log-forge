---
type: Decision
title: "A shutdown shortens a *wait*; it must never skip *work*"
description: "the stop event is set for the whole of `_final_drain`, so any sink consulting it to do *less* degrades itself on the exit drain — the one path a serverless process has. (SPEC-038)"
---

**A shutdown shortens a *wait*; it must never skip *work*** — `Worker.shutdown` sets the stop event **before** joining the drain thread, so it is set for the whole of `_final_drain`. Any sink consulting it to do *less* is therefore degrading itself on the exit drain, which is the one path a serverless process has. Measured four times in one spec: `HTTPSink` ending its 413 search delivered **nothing** of a 2,000-event backlog that had been going out in 30 requests; `KafkaSink` cutting its flush to zero delivered **0 of 11**, since `produce()` is a local hand-off and `flush()` is the only thing that drains the producer. `_retry.wait` is the one place the signal belongs — it shortens a backoff and cancels no attempt. (SPEC-038 FR-001 AC-4a, FR-006 AC-3)

## History

### 2026-08-10

- **Settled.**
