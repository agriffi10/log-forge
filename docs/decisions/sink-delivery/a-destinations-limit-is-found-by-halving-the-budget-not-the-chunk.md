---
type: Decision
title: "A destination's limit is found by halving the *budget*, not the chunk"
description: "recursive chunk-halving is `2N-1` requests because each accepted size is rediscovered in every branch; capping the recursion *depth* instead is the trap — a cap of 4 against a 250x ratio delivered 2 events of 2,000. (SPEC-038)"
---

**A destination's limit is found by halving the *budget*, not the chunk** — recursive chunk-halving is `2N-1` requests (11,954 measured for one exit backlog), because each accepted size is rediscovered in every branch; halving the budget re-chunks the remainder once per reduction and converges in `log2(ratio)` (8 for a 5 MB default against a 20 kB endpoint). Capping the recursion *depth* instead is the trap: a 250× ratio needs ~8 halvings, so a cap of 4 delivered 2 events of 2,000. Each reduction halves **what was refused**, not the nominal budget. A `413` is never retried — it is a verdict on the bytes — and a size already refused is not asked about again, **except under `gzip`**, where the refusal is uncompressed and the destination judges the wire. (SPEC-038 FR-001)

## History

### 2026-08-10

- **Settled.**
