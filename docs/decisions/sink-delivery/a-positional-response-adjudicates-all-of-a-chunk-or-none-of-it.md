---
type: Decision
title: "A positional response adjudicates all of a chunk or none of it"
description: "a mismatch is evidence of misalignment, so even the overlapping prefix is refused, and so is a failed count its coded records contradict; a reply carrying none of its verdict fields adjudicates none of its chunk. What cannot be adjudicated is abandoned and counted (`dropped_unadjudicated`), never retried; a keyed response (SQS, SNS `Failed`, MongoDB `writeErrors`) is read by the same rule, and in one that lists `Successful` an entry named in neither list is `UNKNOWN`. (SPEC-018, SPEC-070, SPEC-074, SPEC-093)"
---

**A positional response adjudicates all of a chunk or none of it** — an id-less per-record array must prove it describes the records sent (same length, right shape) before entry *i* may be read as record *i*; a mismatch is evidence of misalignment, so even the overlapping prefix is refused. What it cannot adjudicate is **abandoned and counted** (`dropped_unadjudicated`), never retried — the API reported a failure count, so some of the chunk landed and re-sending would duplicate downstream forever, while an abandoned record is a loss counted here and now. ~~Id-keyed responses (`SQSSink`, `SNSSink`) select by `Id`, cannot mis-pair, and are deliberately not unified with this.~~ Superseded by SPEC-074: they are read by the same all-or-none rule. (SPEC-018) **A failed count is the reply's second statement of the same verdict, and the two must agree** (SPEC-070 FR-003). `KinesisSink` and `FirehoseSink` used to read `FailedRecordCount` / `FailedPutCount` first and return the chunk as sent when it was `0` or absent, so a `0` beside coded records, no count beside them, or a positive count beside none counted nothing and printed nothing. Both now pass the count to `adjudicate_positional`: the chunk is delivered only when the count is the `int` `0` or absent and no usable result is coded, and a result array that is present must itself be usable — one entry that is not a mapping makes the whole array unreadable, which cannot show that nothing is coded, so it is unadjudicated, while a `0` with no array at all stays delivered; the coded records are retried only when the count is a positive `int` (a `bool` is not one), the arrays are the same length and exactly that many are coded; every other combination — an explicit `None` count included — is unadjudicated, counted and announced once with the count named by its value or type, never resent, because the reply's halves disagree about which records landed and a resend of the coded ones could duplicate records the count says were taken.

**A reply that carries none of its verdict fields adjudicates none of its chunk** (SPEC-093 FR-003, Andrew's decision A of 2026-11-15). A Kinesis or Firehose reply with no failed count and no per-record array is unadjudicated: `adjudicate_positional` takes `absent=True` from `settle_positional`, and the chunk is `UNKNOWN`, counted on `dropped_unadjudicated` with the unadjudicated line; a `0` count with no array stays delivered, and a direct caller that leaves `absent` out gets SPEC-070's reading. An SQS or SNS reply with neither `Failed` nor `Successful` is unread, every entry `UNKNOWN`. A keyed reply that lists `Successful` is read with the same discipline as `Failed`: an entry it names is `ACCEPTED`, one `Failed` names keeps that entry's outcome, one named in neither is `UNKNOWN`, and a `Successful` list the discipline refuses, or an entry named in both, makes the whole call unread. A `Failed`-only reply still reads an entry it does not name as `ACCEPTED`. Every `UNKNOWN` entry of an SQS or SNS call is counted in `failed` and in `dropped_unadjudicated`, which there is a sub-count of `failed`. The reasoning is *A batch reply that names no event adjudicates none of it*.

## History

### 2026-11-20

- **Refined** by SPEC-093 FR-003, on Andrew's decision A of 2026-11-15: a reply carrying none of its verdict fields, `{}` among them, adjudicates none of its chunk, where it read as full delivery; and a keyed reply that lists `Successful` makes an entry named in neither list `UNKNOWN`. Fence rewritten; a `Failed`-only keyed reply and a `0` count with no array read as before.

### 2026-11-09

- **Extended** by SPEC-074 FR-002 to a keyed response: `_batch.adjudicate_keyed` reads SQS's and SNS's `Failed` list by `Id` and MongoDB's `writeErrors` by `index` with the same discipline — a list that is not a list, an item that is not a mapping, a key naming no sent entry of the same exact type, or two items naming one key adjudicate none of the chunk, counted once and never resent, where a malformed SQS or SNS item used to escape `emit` raw. `dropped_unadjudicated` is counted through the delivery engine's run context, `ALWAYS`. The body's sentence that Id-keyed responses are deliberately not unified with this is superseded.

### 2026-11-07

- **Refined** by SPEC-070 FR-003: a Kinesis or Firehose failed count its coded records contradict is unadjudicated, where the sinks used to read a falsy count as success before looking at the records.

### 2026-07-28

- **Settled.**
