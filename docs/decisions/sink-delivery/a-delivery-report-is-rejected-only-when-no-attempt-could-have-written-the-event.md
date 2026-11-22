---
type: Decision
title: "A delivery report is `REJECTED` only when no attempt could have written the event"
description: "a client retries a message on its own, so a refusal is `REJECTED` only when it judges what every attempt carries unchanged or names a message the client never sent: `KafkaSink`'s `DEFINITIVE_CODES` are 21 and 87, and `GooglePubSubSink`'s set is `InvalidArgument` and `FlowControlLimitError`, read by class; every other report, size and authorization refusals among them, and no report, is `UNKNOWN`, except a Kafka message with no report once the producer's queue reads empty, which is `ACCEPTED`. (SPEC-094)"
---

**Why a refusal is not enough.** A client retries a message on its own, and a later attempt's
refusal says nothing about an earlier attempt that timed out. In librdkafka v2.16.0 a timed-out
attempt leaves a message `POSSIBLY_PERSISTED`, and a later error never downgrades it to
`NOT_PERSISTED`, but confluent-kafka does not expose that status. google-cloud-pubsub 2.43.0 retries
a publish on `DeadlineExceeded` among others, so a later `NotFound` or `PermissionDenied` can follow
an attempt the server kept. So `REJECTED`, which fails a batch of nothing else and makes `flush()`
falsy, is kept for a refusal no attempt of the message could have escaped: one that judges something
every attempt carries unchanged, or one for a message the client never sent. A refusal that judges
what an attempt carried beside the message, such as the batch the client built around it, does not
qualify, because the client builds that again for each attempt. Everything else is `UNKNOWN`:
counted in `failed`, announced as may-have-landed, never resent.

**`KafkaSink`: `DEFINITIVE_CODES = frozenset({21, 87})`.** 21, `INVALID_REQUIRED_ACKS`, judges the
producer's `acks` setting, which is the producer's for its life. 87, `INVALID_RECORD`, is the broker's
per-record validation naming the record, whose key, value and headers go unchanged into every attempt.
Left out: 10, `MESSAGE_TOO_LARGE`, and 18, `RECORD_LIST_TOO_LARGE`, which judge the record batch
librdkafka rebuilds for each attempt (against a real broker a 2 MB message with the producer's
`message.max.bytes` above the topic's read `UNKNOWN`); 29 and 31, authorization, which can change
between attempts; 2, `CORRUPT_MESSAGE`, which can be one attempt's transient corruption; and 17,
`INVALID_TOPIC_EXCEPTION`, which could not be verified never to follow an earlier attempt. A code is
read through `_diag.is_a(value, int)`, which admits an `int` subclass, compared as the `int` it holds.
That neither 21 nor 87 follows an attempt that wrote the message is an inference from the Kafka
protocol and librdkafka's source, not a measurement: a broker or topic setting changed between
attempts, such as a topic made compacted, would break it for 87.

**`GooglePubSubSink`: `InvalidArgument` and `FlowControlLimitError`, resolved through
`_classify.driver_classes`, so nothing is imported.** The server returns `InvalidArgument` for a
publish request it refuses whole, and the client sets that error on every future in its batch, so it
does not judge one message's own bytes. What holds is that no attempt wrote it: the server applies a
publish request whole or not at all, and the publish retry does not retry `InvalidArgument`. That the
refused request is applied to none of its messages is an inference from the API's meaning.
`FlowControlLimitError` is set on a future the client never sent. `PermissionDenied`,
`Unauthenticated` and `NotFound` are `UNKNOWN`, because the publish retry can end in one of them after
a `DeadlineExceeded` attempt the server kept.

**No report.** A Kafka message with no report is `ACCEPTED` when the wait ended on `flush(slice)`
returning the `int` 0, meaning nothing waits to be sent, acknowledged or served, and `UNKNOWN`
otherwise. A producer configured with `delivery.report.only.error` sends no report for a success, so
its round ends when the queue reads empty and its unreported messages are `ACCEPTED`; with another
message stuck the queue never reads empty, so the round waits its timeout and those successes read
`UNKNOWN`, a recorded constraint in `docs/architecture.md` §13. A stand-in whose `flush` returns
something other than an `int` gives no empty reading. A Pub/Sub future with no result by the deadline
or the stop is `UNKNOWN`.

**A full client queue reads differently in the two sinks**, because their clients report it
differently: a Kafka `produce()` that raises keeps 1.x's reading, dropped and counted beside a
hand-off that worked and resent when nothing was handed over, while a full Pub/Sub queue arrives as a
`FlowControlLimitError` future and is `REJECTED`. Whether a full queue should be retried is open in
`docs/architecture.md` §12, and invariant 6 records the difference.

**Do NOT** add a code or class to either set without showing that no attempt of the message could
have been written; do NOT read a size or authorization refusal as definitive; do NOT read an
unreported Kafka message as `ACCEPTED` unless the queue read empty.

## History

### 2026-11-20

- **Settled** by SPEC-094 FR-001 and FR-002. The corpus's Kafka double reports code 87 and its
  Pub/Sub double raises a stub `google.api_core.exceptions` class, so the definitive set is read by
  class (FR-003's re-plant with the double's local class reads `ambiguous-when-knowable`).
