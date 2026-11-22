---
type: Decision
title: "A batch reply that names no event adjudicates none of it"
description: "an SQS or SNS reply with neither `Failed` nor `Successful`, a Kinesis or Firehose reply with no failed count and no record array, and a `2xx` `_bulk` object with neither `errors` nor `items` adjudicate none of their chunk: each event is `UNKNOWN`, counted on `dropped_unadjudicated`, announced as one that may have landed, never resent. A keyed entry named in neither of `Successful` and `Failed` is `UNKNOWN`; a `Failed`-only reply reads as 1.x. (SPEC-093)"
---

**Andrew's decision A, 2026-11-15.** He was asked whether an AWS batch reply that names no failure
should read as delivered, with `{}`, a `Successful` list with no `Failed`, and a Kinesis or Firehose
reply with neither a count nor records as the examples. He answered **"Unadjudicated
(Recommended)"**: every event the reply does not name is `UNKNOWN`, counted in `failed` and
`dropped_unadjudicated`, announced once as one that may have landed, and never resent. That matches
the existing rule for an unreadable `2xx` on `_bulk`, Honeycomb and OTLP. Before it, `{}` read as full
delivery: `probe_sqs.py` G2 lost 25 of 25 SQS messages with nothing counted, and
`probe_aws_more.py` 25 of 25 Kinesis records (2026-11-14 audit, finding 14).

**The keyed reply, `SQSSink` and `SNSSink`** (`_rounds.read_keyed`, `settle_keyed`). A reply with
neither `Failed` nor `Successful` is unread, so every entry of the call is `UNKNOWN`. A reply that
carries `Successful` is read with `adjudicate_keyed`'s discipline (a list of mappings, each `Id`
naming one entry sent, none twice): an entry it names is `ACCEPTED`, one named in `Failed` takes that
entry's outcome as before, and one named in neither is `UNKNOWN`; a `Successful` list the discipline
refuses, or an entry named in both lists, makes the whole call unread. **The `Failed`-only reply reads
as 1.x did**: an entry it does not name is `ACCEPTED`. Decision A names the empty object and
`Successful` without `Failed`; a reply with only a `Failed` list states its failures in the field the
1.x reader trusts, and an earlier draft that read every entry `Successful` did not name as `UNKNOWN`
failed nine tests whose stand-ins return `{"Failed": []}`. Both sinks gain `dropped_unadjudicated`,
which there is a sub-count of `failed`, since `failed` is the engine's `failed_events` and already
holds every `UNKNOWN` event. An unread call keeps 1.x's line with decision A's ending, `<Class> could
not adjudicate a <call> reply (N entry(ies) sent); may have landed, not retried`, and an unnamed
entry has its own, `<Class>, a <call> reply named neither success nor failure for N entry(ies); may
have landed, not retried`; both are `ALWAYS`.

**The positional reply, `KinesisSink` and `FirehoseSink`.** `adjudicate_positional` takes
`absent: bool = False`, meaning the reply carried no per-record array at all, and `settle_positional`
passes it. A reply with no failed count and no array is unadjudicated: its chunk is `UNKNOWN`, counted
on `dropped_unadjudicated` with 1.x's line. A `0` count with no array stays delivered, as SPEC-070
settled and decision A does not name, and a direct caller that does not pass `absent` gets SPEC-070's
reading. On these sinks, and on the HTTP family, `dropped_unadjudicated` stays outside `failed`, as
1.x kept it.

**The `_bulk` reply, decided in session.** Decision A's principle is that a reply which names no
verdict is never counted as delivered. A `2xx` object carrying neither `errors` nor `items` cannot
have adjudicated any item, so `ElasticsearchSink` and `OpenSearchSink` give it up through
`HTTPSink._unadjudicated`, the path an undecodable body takes. `{}` and an error document such as
`{"error": {...}, "status": 500}` take it. An object with `errors` absent or false beside `items`, and
`{"errors": false}`, still read as success, unverified, so an endpoint that omits only `errors` is not
charged with every success; an earlier rule, "no `items`", failed four tests over exactly that. This
refines *A 2xx _bulk response with no readable verdict is counted, never retried*.

**Do NOT** read a reply that names no event as delivered; do NOT resend an unnamed entry, which may
have landed; do NOT read a `Failed`-only keyed reply's unnamed entries as `UNKNOWN`; do NOT route a
`_bulk` object that carries `items` to unadjudicated for want of `errors` before the Astra `_bulk`
profile settles that shape (`docs/architecture.md` §12).

## History

### 2026-11-20

- **Settled** by SPEC-093 FR-003, on Andrew's decision A of 2026-11-15; the `_bulk` extension was
  decided in session. `tests/test_reply_names_no_event.py` holds it.
