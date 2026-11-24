---
type: Decision
title: "A resend after a send that may have landed is counted, and its key is the event's `log_id`"
description: "each engine offer of an event again after a round that marked it with `RunContext.may_have_landed` is counted once in `SinkCounters.resent_events`, `SinkLosses.resent` and `Delivery.resent`, an upper bound, with one `_diag.resent` line per run; a shipped sink marks a call that may have reached its destination, every raise of a client that retries inside the call included; the key a destination may de-duplicate on is the event's own `log_id`, already in every body, and no new key goes on the wire. (SPEC-095)"
---

**A resend after a lost reply is counted and announced, not stopped.** The resend itself is the
settled cost of *A client exception costs its chunk, and is provable non-delivery for it*, within
Andrew's 1.x total of 16 sends (2026-11-04). What the 2026-11-14 audit found (finding 5) is that
it was invisible: a standard SQS queue held 35 messages for 25 events, and an HTTP collector and a
PostgreSQL table 50 for 25, with `failed=0` and no line.

**The count** (FR-001). `RunContext.may_have_landed(indices)` marks `RETRYABLE` positions of a
round's batch whose call may have reached the destination. It is validated, rewound and delegated
as `sink_counted` is: an index that is not an exact `int` in range, or whose outcome is not
`RETRYABLE`, fails the round as an escaped exception does; a round the engine sets aside leaves no
mark standing; `delegate` moves it into a wrapper's positions; an immediate context ignores it.
`_run` remembers, per event, whether the last round that carried it marked it, and adds one to the
run's `resent` when it offers that event again, after the wait and the decline check. The event's
fate is unchanged. The count goes into `Delivery.resent` and, through the commit's `Deferral`, into
`SinkCounters.resent_events` on a settled run and on a failed one, since the sends happened either
way. The commit then writes one line, `log-foundry: resent <n> event(s) a lost reply may have
landed; they may be duplicated; <Sink>`, through `_diag.resent`. `SinkLosses.resent` is appended
with a default of `0`, so `health().sink.resent` reports it and a `SinkLosses` built with
`dropped=` and `failed=` still builds. Every site in `src/` that builds a `SinkLosses` passes
`resent=` (a test walks the AST, with no allowlist). `HTTPSink._send` hands its count to the engine
through `_run`'s `resent_into`, which commits it to the sink's own counters. A resend is not an
event, so the event ledger does not move.

**The rule a sink marks by** (FR-002). A `RETRYABLE` event is marked when its call may have
reached the destination. A reply that refuses the event or the request is not marked, except a
`5xx`, which says the request failed rather than that nothing was stored, and a Firehose record
coded `ServiceUnavailableException` or `InternalFailure`, after which AWS says "the write might
have completed". A raise is marked unless the sink can show the request never left, by where the
call raised or by the exception's class (`_classify.may_have_landed`, which clears a builtin
`ConnectionRefusedError`, a `socket.gaierror` and a `urllib` `URLError` other than an `HTTPError`).
A client that retries inside the call can show neither, so every raise from botocore, the Event
Hubs producer, redis-py and clickhouse-connect is marked, whatever its class. Their retries are
left as they are, because removing them would change delivery, not reporting.

| Sink | Marked | Not marked |
|---|---|---|
| HTTP family, via `_exchange`, and `_send` | any retried exception `may_have_landed` does not clear, and a retryable status `>= 500` | a `URLError` other than an `HTTPError`, and a builtin refusal; `429` |
| `SQSSink`, `SNSSink`, `KinesisSink`, via `settle_keyed` and `settle_positional` | every retried raise: botocore retries inside the call | every entry a reply names |
| `FirehoseSink`, via `settle_positional` | every retried raise, and every record carrying an `ErrorCode` | none |
| `SocketTransport`, TCP | the frame whose `sendall` raised, unless `may_have_landed` clears the exception | a frame whose connection could not be opened, and a `sendall` refused with `ConnectionRefusedError`; UDP never |
| `RabbitMQSink` | a publish raising `StreamLostError`, `ConnectionClosedByBroker`, `AMQPHeartbeatTimeout` or `ConnectionBlockedTimeout`, or an exception outside pika's `AMQPError` that `may_have_landed` does not clear | any other pika error, such as a channel or connection it could not open |
| `PostgresSink` | a raise from `commit()` whose SQLSTATE is in class `08`, `57` or `XX`, or absent, and the same from any statement on an `autocommit` connection | a raise before `commit()` in a transaction, and any other SQLSTATE |
| `AzureEventHubsSink` | every retried raise: the producer retries inside the call | none |
| `ClickHouseSink`, `RedisStreamsSink`, `RedisListSink` | every retried raise: each client retries inside the call | none |

`MongoDBSink`, the file sinks, the stream sinks and `NATSSink` never mark, each in the derived
roster's `EXEMPT` with its reason: MongoDB's `_id` refuses a second copy within a run, and its
duplicate driver-stamped `_id` on its own resend is read as delivery (FR-003); a line the file or
stream took is never written again; `NATSSink`'s resend is §12's `Nats-Msg-Id` item. The sinks that
mark every failed call over-count on a refused one, and the conformance corpus reads those rows as
departures, each quoting its sink's docstring.

**The key, destination by destination.** The key a resend reuses is the event's `log_id`, a UUID4
hex string minted once at assembly and carried unchanged in every body every sink sends.

| Sink | What de-duplicates a resend there | What the library does |
|---|---|---|
| `SQSSink`, FIFO queue | `MessageDeduplicationId`, already `log_id`, for 5 minutes | counts the resend; the engine does not bound a resend to the interval |
| `SQSSink`, standard queue | nothing; SQS tells consumers to be idempotent | counts; the consumer de-duplicates on the body's `log_id` |
| `SNSSink`, FIFO topic (since SPEC-100) | `MessageDeduplicationId`, `log_id` by default, for the topic's interval, a "5 minute minimum" | counts the resend; the engine does not bound a resend to the interval |
| `SNSSink`, standard topic | nothing | counts |
| `KinesisSink`, `FirehoseSink` | nothing; AWS says to embed a primary key and handle duplicates at the destination | counts; `log_id` is that key |
| HTTP family, `OTLPSink` | none set; `OTLPSink` sends `log_id` as `log.record.uid` | counts |
| `ElasticsearchSink`, `OpenSearchSink` | an action line's `_id`, which these sinks do not send | counts |
| `ClickHouseSink` | on a replicated table, a resent identical block by its hash; on a plain `MergeTree`, nothing unless the owner sets a window | counts, an upper bound |
| `PostgresSink` | nothing: the `INSERT` has no conflict clause | counts |
| `MongoDBSink` | `_id`'s unique index, permanently | reads its own resend's duplicate `_id` as delivery |
| Redis, RabbitMQ, Event Hubs, the socket sinks | nothing | counts |
| `NATSSink`, JetStream | `Nats-Msg-Id`, which the sink does not send | not counted; its own §12 item |

**The bound, exactly.** `resent` counts the engine's own resends of events whose previous send may
have landed.

- **It over-counts** a resend that adds no copy:
  - on a FIFO queue inside its 5-minute interval;
  - on a replicated ClickHouse table;
  - on a `MongoDBSink`-style destination that refuses the copy, which no counted sink is;
  - wherever a client that retries internally raised for a call none of whose attempts landed.
    That is every failed AWS, Event Hubs, Redis and ClickHouse call the sink cannot tell apart;
  - a re-offer the sink refuses unsent, because it was closed during the backoff. The count is
    taken when the engine offers the event again, so the closed sink's refusal still counts it.
- **It under-counts** in three ways:
  - **A cleared class that did leave.** It misses a copy only if an exception FR-002 clears did in
    fact leave a request on the wire: a `urllib` `URLError`, a socket that could not connect or was
    refused, a pika channel or connection that could not open, or a PostgreSQL error before
    `COMMIT` or outside classes `08`, `57` and `XX` at it.
  - **An entry a reply names as failed.** Some resends follow a reply naming an entry as failed
    after an internal attempt in the same call had landed it. An SQS or SNS `Failed` entry and a
    Kinesis coded record are not marked, because the reply is the service's word on the attempt it
    answers, and the sink cannot see an earlier attempt.
  - **Copies the engine did not make**, listed in *Out of Scope*. The main one is a client's
    internal retry inside a call that then returned.

The copies the engine did not make are, as SPEC-095's *Out of Scope* lists them: a client's
internal retry inside a call that then returned, an entry a reply names as failed after an internal
attempt had landed it, the worker's whole-batch resend of a sink with no `deliver` (`MultiSink`
included), `NATSSink`'s resend, and a third-party sink's own loop of `deliver` calls. The mark is
private, so a third-party engine sink's resends are not counted either.

**Three choices made in session on 2026-11-16, by the orchestrating session, not by Andrew:**

- **The count lives in `SinkLosses.resent`**, appended with a default of `0`, so `health().sink`
  shows it. Rejected: `SinkCounters` alone, which `health()` cannot see, and the event ledger, where
  a duplicate is not an event.
- **No `_id` for `ElasticsearchSink` and `OpenSearchSink`.** Sending `log_id` as `_id` would make a
  resend overwrite rather than duplicate, but would change every document's `_id` from one
  Elasticsearch generates, add about 44 bytes to each action line, and cost the existing-id check
  Elastic calls "a costly operation". The count already accounts for the copy.
- **`MongoDBSink` is not declared idempotent.** It would meet the proof in *No shipped sink
  declares itself idempotent* once SPEC-095's integration run is green, but declaring it reverses
  part of a settled fence for a gain only on a `writeErrors` entry that cannot be paired.

**What stays open.** An event a send may have landed, and that no later send landed, keeps its last
round's fate and is counted lost: Andrew answered "Lost, flush falsy (Recommended)" on 2026-11-16.
`architecture.md` §12 holds it. (It also held SNS FIFO topics, which `SNSSink` could not write until SPEC-100 closed that item.)

**Do NOT** add a key to the wire (a message attribute, header or document field) for a destination
that does not de-duplicate on it; do NOT turn off a client's internal retries to make its raises
countable; do NOT count a resend a sink can show never left; do NOT write the resend line outside
`_diag.resent`.

## History

### 2026-11-24

- **Updated** by SPEC-100: `SNSSink` writes SNS FIFO topics, with the event's `log_id` as its default `MessageDeduplicationId`, so the key table gives the FIFO topic its own row and the "what stays open" paragraph no longer lists SNS FIFO topics. The rule is unchanged.

### 2026-11-20

- **Settled** by SPEC-095 FR-001 to FR-005.
