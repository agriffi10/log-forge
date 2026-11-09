---
type: Decision
title: "A positional response adjudicates all of a chunk or none of it"
description: "a mismatch is evidence of misalignment, so even the overlapping prefix is refused, and so is a failed count its coded records contradict. What cannot be adjudicated is abandoned and counted (`dropped_unadjudicated`), never retried; a keyed response (SQS, SNS `Failed`, MongoDB `writeErrors`) is read by the same rule. (SPEC-018, SPEC-070, SPEC-074)"
---

**A positional response adjudicates all of a chunk or none of it** — an id-less per-record array must prove it describes the records sent (same length, right shape) before entry *i* may be read as record *i*; a mismatch is evidence of misalignment, so even the overlapping prefix is refused. What it cannot adjudicate is **abandoned and counted** (`dropped_unadjudicated`), never retried — the API reported a failure count, so some of the chunk landed and re-sending would duplicate downstream forever, while an abandoned record is a loss counted here and now. ~~Id-keyed responses (`SQSSink`, `SNSSink`) select by `Id`, cannot mis-pair, and are deliberately not unified with this.~~ Superseded by SPEC-074: they are read by the same all-or-none rule. (SPEC-018) **A failed count is the reply's second statement of the same verdict, and the two must agree** (SPEC-070 FR-003). `KinesisSink` and `FirehoseSink` used to read `FailedRecordCount` / `FailedPutCount` first and return the chunk as sent when it was `0` or absent, so a `0` beside coded records, no count beside them, or a positive count beside none counted nothing and printed nothing. Both now pass the count to `adjudicate_positional`: the chunk is delivered only when the count is the `int` `0` or absent and no usable result is coded, and a result array that is present must itself be usable — one entry that is not a mapping makes the whole array unreadable, which cannot show that nothing is coded, so it is unadjudicated, while a `0` with no array at all stays delivered; the coded records are retried only when the count is a positive `int` (a `bool` is not one), the arrays are the same length and exactly that many are coded; every other combination — an explicit `None` count included — is unadjudicated, counted and announced once with the count named by its value or type, never resent, because the reply's halves disagree about which records landed and a resend of the coded ones could duplicate records the count says were taken.

## History

### 2026-11-09

- **Extended** by SPEC-074 FR-002 to a keyed response: `_batch.adjudicate_keyed` reads SQS's and SNS's `Failed` list by `Id` and MongoDB's `writeErrors` by `index` with the same discipline — a list that is not a list, an item that is not a mapping, a key naming no sent entry of the same exact type, or two items naming one key adjudicate none of the chunk, counted once and never resent, where a malformed SQS or SNS item used to escape `emit` raw. `dropped_unadjudicated` is counted through the delivery engine's run context, `ALWAYS`. The body's sentence that Id-keyed responses are deliberately not unified with this is superseded.

### 2026-11-07

- **Refined** by SPEC-070 FR-003: a Kinesis or Firehose failed count its coded records contradict is unadjudicated, where the sinks used to read a falsy count as success before looking at the records.

### 2026-07-28

- **Settled.**
