---
type: Decision
title: "A shutdown shortens a *wait*; it must never skip *work*"
description: "the stop event is set for the whole of `_final_drain`, so any sink consulting it to do *less* degrades itself on the exit drain — the one path a serverless process has. (SPEC-038)"
---

**A shutdown shortens a *wait*; it must never skip *work*** — `Worker.shutdown` sets the stop event **before** joining the drain thread, so it is set for the whole of `_final_drain`. Any sink consulting it to do *less* is therefore degrading itself on the exit drain, which is the one path a serverless process has. Measured four times in one spec: `HTTPSink` ending its 413 search delivered **nothing** of a 2,000-event backlog that had been going out in 30 requests; `KafkaSink` cutting its flush to zero delivered **0 of 11**, since `produce()` is a local hand-off and `flush()` is the only thing that drains the producer. `_retry.wait` is the one place the signal belongs — it shortens a backoff and cancels no attempt. (SPEC-038 FR-001 AC-4a, FR-006 AC-3) **Refined by SPEC-062:** the worker may decline the *start* of a retry predicted to end after the `shutdown()` deadline, because that retry is not work the process gets; the first attempt is never declined and no attempt in flight is cut short. See *The exit drain declines only a retry predicted to end past the shutdown deadline*.

## History

### 2026-11-03

- **Refined** by SPEC-062, not reversed. A sink still does no less inside an attempt, and the first attempt of every batch is always made. The worker declines only the start of a retry predicted to end after the caller's deadline, which the join would abandon anyway with the sink left open. The reasoning is in *The exit drain declines only a retry predicted to end past the shutdown deadline*.

### 2026-08-10

- **Settled.**
