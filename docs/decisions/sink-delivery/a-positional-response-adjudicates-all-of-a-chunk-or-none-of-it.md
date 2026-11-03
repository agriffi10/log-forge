---
type: Decision
title: "A positional response adjudicates all of a chunk or none of it"
description: "a mismatch is evidence of misalignment, so even the overlapping prefix is refused. What cannot be adjudicated is abandoned and counted (`dropped_unadjudicated`), never retried. (SPEC-018)"
---

**A positional response adjudicates all of a chunk or none of it** — an id-less per-record array must prove it describes the records sent (same length, right shape) before entry *i* may be read as record *i*; a mismatch is evidence of misalignment, so even the overlapping prefix is refused. What it cannot adjudicate is **abandoned and counted** (`dropped_unadjudicated`), never retried — the API reported a failure count, so some of the chunk landed and re-sending would duplicate downstream forever, while an abandoned record is a loss counted here and now. Id-keyed responses (`SQSSink`, `SNSSink`) select by `Id`, cannot mis-pair, and are deliberately not unified with this. (SPEC-018)

## History

### 2026-07-28

- **Settled.**
