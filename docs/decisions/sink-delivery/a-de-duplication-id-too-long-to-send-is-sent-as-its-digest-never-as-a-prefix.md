---
type: Decision
title: "A de-duplication id too long to send is sent as its digest, never as a prefix"
description: "on `SQSSink` and `SNSSink`, a `MessageDeduplicationId` answer longer than 128 characters once stripped goes out as the SHA-256 hex digest of the stripped answer, 64 characters, so two answers sharing a prefix keep two ids; one of 1 to 128 is sent as it is and a blank one is minted. The group id keeps its cut to 128, which merges groups and costs order, never delivery. (SPEC-100)"
---

**A de-duplication id too long to send is sent as its digest, never as a prefix.** AWS takes a
`MessageDeduplicationId` of at most 128 characters on an SQS FIFO queue and an SNS FIFO topic, and
a FIFO destination accepts, but does not deliver, a message whose de-duplication id it has seen
within its interval. Until SPEC-100, `SQSSink` cut an over-long answer to its first 128 characters,
the cut it also applies to the group id. For the de-duplication id that is a silent loss under
invariant 2: two events whose callable answered ids sharing their first 128 characters went out
under one id, and the destination discarded the second. Measured with
`docs/audits/2026-11-14-probes/sinks/spec100_moto_shapes.py` case F, two events whose callable
answered 200 characters sharing the first 128, sent by `SQSSink` to moto's FIFO queue: on
`03dbf502` the queue held 1 of 2, and the sink counted `SinkLosses(dropped=0, failed=0)` and wrote
no line. The default key, the event's `log_id`, is 32 characters, so only a caller's callable
reaches the limit.

**The rule.** `sinks/_fifo.dedup_id`, which both FIFO sinks call, strips the answer. A stripped
answer longer than `MAX_ID_LEN` (128) is sent as
`hashlib.sha256(stripped.encode("utf-8", "surrogatepass")).hexdigest()`: 64 characters from the
set AWS accepts, the same for the same stripped answer and different for different ones, so the
resend of a digested id carries the same digest and two answers sharing a prefix keep two ids.
`surrogatepass` lets a lone surrogate digest rather than raise. A stripped answer of 1 to 128
characters is sent as it is, and a blank one keeps the freshly minted fallback, so two answers
that differ only in surrounding whitespace share an id, as they share one when short. On
`b145f9a2` the same case F read 2 of 2 held, with `SinkLosses(dropped=0, failed=0, resent=0)`.

**The group id keeps its truncation.** A group id cut to 128 characters can merge two groups, which
costs their relative order and never a delivery, so `_fifo.bounded` still cuts it (SPEC-016).

**Rejected**, both offered to Andrew on 2026-11-17, who chose "Send its SHA-256 digest
(Recommended)":

- *Keeping the truncation and recording the loss in `architecture.md` §12.* It leaves a silent
  loss that the sink can avoid at the cost of a hash per long id.
- *Dropping an event whose id is over-long, as an id failure, counted and announced.* It turns
  an event the destination can take into a loss, where a digest delivers it.

**Do NOT** cut a de-duplication id to a prefix, on either FIFO sink; do NOT digest an id of 128
characters or fewer, which would change the id a caller already sees downstream; do NOT digest the
group id, whose cut costs only order. Guarded by
`tests/test_sinks_sns_fifo.py::test_two_long_dedup_answers_sharing_a_prefix_get_two_digests`,
`::test_the_digest_boundaries` and `::test_the_resend_of_a_digested_id_carries_the_same_digest`,
each on both sinks, and `tests/test_sinks_sqs_fifo.py::test_over_long_dedup_id_is_sent_as_its_digest`.
With the digest replaced by truncation, SPEC-100's build saw the first and the third fail on both
sinks, the second fail in its 129-character case, and the SQS test fail.

## History

### 2026-11-24

- **Settled** by SPEC-100 FR-003, Andrew's call of 2026-11-17: on both FIFO sinks an over-long
  de-duplication id is sent as its SHA-256 digest, closing the silent loss the truncation caused
  on `SQSSink`.
