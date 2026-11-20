---
type: Spec
id: SPEC-095
title: "A Resend After a Lost Reply Is Counted, and Its Key Is the Event's Own `log_id`"
description: "Counts and announces every event the delivery engine sends again after a send that may have landed it, has each shipped sink mark such sends, counting every raise from a client that retries internally, reads a MongoDB duplicate driver-stamped _id on such a resend as delivery, and settles that the event's log_id, already in every body, is the idempotency key and what a sink must prove before it declares itself idempotent, so SPEC-092's 50 reply-resent rows and two MongoDB rows turn green and 19 rows move to new departures."
spec_status: Completed
depends_on: [SPEC-072, SPEC-073, SPEC-074, SPEC-075, SPEC-088, SPEC-092]
arc: audit-2026-11-14
arc_order: 11
---

## Overview

When a sink sends a batch and the reply is lost, the sink cannot tell whether the destination took
it, and the library sends it again. That resend is a settled, accepted cost: it is what keeps an
outage from losing every event, and the total number of sends is Andrew's 16 of 2026-11-04. What the
2026-11-14 audit found (finding 5) is that the resend is invisible. A standard SQS queue held 35
messages for 25 events, an HTTP collector and a PostgreSQL table 50 for 25, and nothing counted or
said so. When every reply was lost, each of 10 events landed 16 times while the library counted all
10 as lost. This spec makes the resend visible. Every event the library sends again after a send
that may have landed it is counted on the sink, shown in `health()`, and announced on stderr in
one line per batch that had any. Each shipped sink says which of its sends may have landed. Where a
send provably did not leave, it is not counted. Where a client library retries inside the call, as
botocore, the Event Hubs producer and redis-py do, every failed send is counted, because an attempt
the sink never saw may have landed. MongoDB's `_id` already refuses the second copy, and that refusal
is now read as delivery rather than counted as a loss. The spec also settles the idempotency key:
it is the event's own `log_id`, which every event already carries in its body and which SQS FIFO
queues already de-duplicate on. No new key goes on the wire. Last, it states what a sink must prove
before it may declare itself idempotent and so have an outcome it cannot read resent.

## Scope

### In Scope

- The delivery engine counts each event it offers again after a round that marked the event as
  possibly landed: in `SinkCounters.resent_events`, `SinkLosses.resent` and `Delivery.resent`, with
  one stderr line per run, and every shipped `losses()` reports it (FR-001).
- Each shipped engine sink marks the sends that may have reached its destination, by one rule with a
  stated form per family, and the sinks that never resend a send that may have landed are listed
  with their reasons (FR-002).
- `MongoDBSink` reads a duplicate-key error on a driver-stamped `_id`, for a document an earlier
  call of the same run carried and that call raised, as landed, and writes no loss line for it
  (FR-003).
- SPEC-092's corpus reads the count: a counted possible duplicate passes `duplicate`, a new check
  fails a count on a send whose outcome was knowable, and the pins, departures and totals move
  (FR-004).
- The record: `architecture.md` §12, the decisions, the invariants page, the README and the pending
  release notes, including the rule a sink must meet before it declares `log_foundry_idempotent`
  (FR-005).

### Out of Scope

- **Stopping the resend.** It is settled. The cost belongs to *A client exception costs its chunk,
  and is provable non-delivery for it*, and the 16-send total to Andrew's decision of 2026-11-04
  (`docs/specs/ARCS.md`, *1.2.0 stability redesign*). Neither changes.
- **Turning off a client's internal retries.** The sinks that build their own botocore, Event Hubs or
  redis-py client keep the client's default retries, because removing them would change delivery,
  not reporting. FR-002 counts what they may cause instead.
- **Resending an `UNKNOWN` outcome.** No shipped sink declares `log_foundry_idempotent`, before or
  after this spec (FR-005 states the proof a declaration needs, and why no sink meets it here).
- **Reading an event that a send may have landed, and that no later send landed, as `UNKNOWN`.**
  Andrew answered "Lost, flush falsy (Recommended)" on 2026-11-16: the event keeps its last round's
  fate, so a run in which every reply is lost still fails, `flush()` is falsy, and its events are
  counted lost. The resend count says how many copies may exist. The two `reply-counted-lost` rows
  this leaves red move to a new §12 item (FR-005).
- **A new key on the wire.** No message attribute, header or document field is added to any request.
  `log_id` is already in every body (`model.py:219`), and on SQS FIFO it is already the
  `MessageDeduplicationId` (`sqs.py:540-564`). `ElasticsearchSink` and `OpenSearchSink` could send it
  as `_id`, and do not (*Revision history*).
- **`NATSSink`.** A JetStream publish whose ack wait expired is resent through the worker, and that
  resend stays uncounted here. Its closure is §12's *A timed-out JetStream publish is not retried,
  and cannot safely be*, whose `Nats-Msg-Id` header that item calls its own spec.
- **SNS FIFO topics.** `SNSSink` sends no `MessageGroupId` (`sns.py:370-405`), and SNS refuses a FIFO
  publish without one. Andrew answered "Follow-up spec (Recommended)" on 2026-11-16: writing a FIFO
  topic becomes its own spec later in this arc, and FR-005's §12 item records it until then.
- **Copies the count does not see.** *The bound*, under *Functional Requirements*, lists them. In
  short: a client's internal retry inside a call that then returned, an entry a reply names as
  failed after an internal attempt had landed it, the worker's whole-batch resend of a sink with no
  `deliver` (`MultiSink` included), `NATSSink`'s resend, and a third-party sink's own loop of
  `deliver` calls. The mark is private, so a third-party engine sink's resends are not counted
  either.
- **Any change to `Outcome`, to the run-failed rule, or to a counter's unit.**
- **Real-service proof for any sink except MongoDB.** FR-003 owes one `integration.yml` run. No
  integration service exists for SQS, SNS, Kinesis or Firehose.

---

## Functional Requirements

**Bases.** Every premise below was read at `97a03f45`. Rows are SPEC-092's prototype corpus
(`docs/audits/2026-11-14-probes/sinks/spec092_*.py`), measured on two bases, because SPEC-093 and
SPEC-094 are reviewed but not yet on `main`:

- **`main`**: SPEC-092's corpus as on `main`. 256 rows: 74 pinned under twelve pins, 12 departure
  rows under seven departures, and 170 that must pass.
- **After SPEC-094**: SPEC-093's and SPEC-094's prototype patches applied (`sinks/spec093_prototype.patch`
  and `sinks/spec094_prototype.patch`). 268 rows: 58 pinned under five pins, 15 departure rows
  under nine departures, and 195 that must pass.

The build uses whichever base is on `main` when it starts. *Evidence* shows that this spec moves
the same 71 rows on both.

**What this spec builds on.**

- *A sink may report per event* (SPEC-072): the engine resends only `RETRYABLE` events, and an
  `UNKNOWN` one only for a sink declaring `log_foundry_idempotent = True`.
- The run context's marks: `final_rejections`, `sink_counted` and `refuse`
  (`_engine.py:712-803`). They are validated against the round's verdict (`_read_marks`,
  `:1322`) and moved into a wrapper's positions by `delegate` (`:803`). FR-001 adds a fourth mark
  of the same kind.
- **Andrew's settled answers that bear on it.**
  - SPEC-092's decision A, for finding 14 (2026-11-15, "Unadjudicated (Recommended)"): an AWS reply
    that names no failure counts every unnamed event `UNKNOWN`.
  - SPEC-089 ("Keep truthy, add counts") and SPEC-094 ("Truthy, counted", 2026-11-16): `flush()`
    stays truthy over a counted `UNKNOWN`.
  - 2026-11-04: keep every sink, and no breaking change in 1.x.
  - 2026-11-16, for this spec: "Lost, flush falsy (Recommended)" and "Follow-up spec
    (Recommended)" (*Out of Scope*).

**The key, destination by destination.** The key a resend reuses is the event's `log_id`, a UUID4
hex string minted once at assembly (`model.py:219`, `ids.py:new_log_id`) and carried unchanged in
every body every sink sends. What each destination does with a resend:

| Sink | What de-duplicates a resend there | What this spec does |
|---|---|---|
| `SQSSink`, FIFO queue | `MessageDeduplicationId`, already `log_id`, for 5 minutes | counts the resend; the queue absorbs it inside the interval, and the engine does not bound a resend to it (*Evidence*) |
| `SQSSink`, standard queue | nothing; SQS delivers at least once and tells consumers to be idempotent | counts; the consumer de-duplicates on the body's `log_id` |
| `SNSSink` | FIFO topics only, which `SNSSink` cannot write | counts |
| `KinesisSink`, `FirehoseSink` | nothing; AWS says to embed a primary key and handle duplicates at the destination | counts; `log_id` is that key |
| HTTP family, `OTLPSink` | the sinks set no de-duplication key; `OTLPSink` already sends `log_id` as `log.record.uid` (`_otel.py:634`) | counts |
| `ElasticsearchSink`, `OpenSearchSink` | an action line's `_id`, which these sinks do not send (`elasticsearch.py:127`) | counts; *Revision history* |
| `ClickHouseSink` | on a replicated table, a resent identical block by its hash; on a plain `MergeTree`, nothing unless the owner sets a window | counts, an upper bound; the resend is already an identical block |
| `PostgresSink` | nothing: the `INSERT` has no conflict clause | counts |
| `MongoDBSink` | `_id`'s unique index, permanently | FR-003 |
| Redis, RabbitMQ, Event Hubs, the socket sinks | nothing | counts |
| `NATSSink`, JetStream | `Nats-Msg-Id`, which the sink does not send | out of scope |

**Should a resend be counted?** Yes. It is the one event in the pipeline that can put two copies at
the destination, and today it leaves `failed=0` and no line. It is counted as an upper bound, as
every `failed` is.

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

### FR-001: The engine counts each resend of an event a send may have landed, and says so once per run

#### Description:

**The mark.** `RunContext.may_have_landed(indices)` marks `RETRYABLE` positions of the round's batch
whose call may have reached the destination. It behaves as `sink_counted` does:

- it is read when the round returns, and an index that is not an exact `int` in range, or whose
  outcome is not `RETRYABLE`, fails the round as an escaped exception does;
- `_mark` and `_rewind` carry it, so a round the engine sets aside leaves no mark standing;
- `delegate` moves it into a wrapper's positions;
- an immediate context ignores it.

**The count.** `_run` remembers, for each event, whether the last round that carried it marked
it. When it offers the event again, after the wait and the decline check, it adds one to the run's
`resent`. So an event marked twice and resent twice counts two. An event whose last mark is in the
run's last round, or before a round the worker declines, counts nothing, because it was not sent
again. Its fate is unchanged: `RETRYABLE` still fails it at the end of the run, as today.

**Where it goes.**

- `Delivery` gains `resent: int = 0`.
- The commit's `Deferral` adds `resent` to `SinkCounters.resent_events` on a settled run and on a
  failed one, since the sends happened either way. When it is non-zero, the commit announces one
  line, `log-foundry: resent <n> event(s) a lost reply may have landed; they may be duplicated;
  <Sink>`, through a new `_diag.resent`.
- `SinkCounters.losses()` returns it as `SinkLosses.resent`. That field is appended with a default
  of `0`, as SPEC-088 appended `Health.events`. So `SinkLosses(dropped=…, failed=…)` still builds,
  and `__match_args__` stays empty.
- `health().sink.resent` therefore reports it with no change to `Worker.health`.

**Every `losses()` reports it.** Fifteen sites in `src/` build a `SinkLosses` at `97a03f45`, and each
passes `resent=` explicitly after this spec:

- `verdict.py:224`, `SinkCounters.losses`;
- the overrides that fold their own counters: `http.py:3121`, `elasticsearch.py:229` (for
  `ElasticsearchSink` and `OpenSearchSink`), `honeycomb.py:394`, `otlp.py:340`, `nats.py:1859` and
  `sentry.py:405`, each reading `resent` from the counters it owns, and `SentrySink` from its HTTP
  backend's;
- the wrappers' composition, `_deferral.py:234-235`, which carries the inner sink's;
- `MultiSink`, `multi.py:301`, which sums its children's;
- the sites with no engine counter, `kafka.py:244`, `pubsub.py:190`, `sqlite.py:290`,
  `logstash.py:415` and `_factory.py:490`, which pass `resent=0`.

On the base after SPEC-094, the list is whatever SPEC-094 left. The criterion below derives it, so
the build does not hand-list it.

**`HTTPSink._send`.** It runs its one-request rounds through `_engine._run` with a throwaway
`SinkCounters` (`http.py:3239`). It commits its run's `resent` to the sink's own
`log_foundry_counters` through one `Deferral`, one per request, and writes the same line.
`SentrySink`'s HTTP backend sends one event per request, so its count is in events. A third-party
subclass on its 1.x path counts requests, as `HTTPSink.failed` does.

**Twin paths (invariant 6).** The worker, the orphan path and `emit_via_deliver` all call `_run`,
so one change serves all three. A wrapper on the engine shares the inner sink's `SinkCounters`
(SPEC-075), so a resend through a wrapper lands in the same counter. `MultiSink`'s child runs
through the adapter.

**The ledger does not move.** A resend is not an event. SPEC-088's terms count each accepted event
once, and a second copy at the destination is outside the ledger, as a sink's `failed` is.

#### Acceptance Criteria:

- [ ] A `deliver` that marks all N events of its first round and answers `RETRYABLE`, then accepts
      them, moves `losses().resent`, `health().sink.resent` and `Delivery.resent` by exactly N,
      writes one line matching `^log-foundry: resent N event\(s\) `, and moves no other counter
      (invariants 2 and 11).
- [ ] That batch through the worker, the orphan path and `emit_via_deliver` gives the same `resent`
      and the same one line on each (invariant 6).
- [ ] An event marked in two rounds and offered after each counts 2. One marked only in the run's
      last round, or in the round before a round the worker declines, counts 0 (invariant 2).
- [ ] A mark on a non-`int`, an out-of-range index, or an index whose outcome is not `RETRYABLE`
      fails the round as an escaped exception does, and the run counts no resend for it
      (invariant 7).
- [ ] A round the engine sets aside leaves none of its marks standing, so the next round's offer
      of those events counts nothing. That covers a `deliver` that marked and then raised, and one
      that marked and returned an unreadable verdict (invariant 2).
- [ ] A run that fails commits its `resent` and writes its line, as a settled one does (invariant 2).
- [ ] `FilteringSink` declining event 0 of 3, over an inner sink that marks its own index 0 and
      then accepts, counts `resent` 1, for the wrapper's event 1 (invariant 6).
- [ ] `SinkLosses(dropped=1, failed=2)` builds and equals `SinkLosses(dropped=1, failed=2,
      resent=0)`, and `SinkLosses.__match_args__ == ()` (invariant 10).
- [ ] A test walks the AST of every module under `src/log_foundry/` and fails for any call that
      builds a `SinkLosses` without a `resent=` keyword. It has no allowlist, and it fails when it
      finds fewer than 15 such calls (invariant 2).
- [ ] For every shipped sink class whose `losses()` returns a `SinkLosses`, derived from the tree,
      `losses().resent` equals its counters' `resent_events` after a run that resent N, or is 0 for
      a class with no `log_foundry_counters`. This covers `ElasticsearchSink`, `OpenSearchSink`,
      `HoneycombSink`, `OTLPSink`, `NATSSink`, `KafkaSink` and `GooglePubSubSink`, bare and under
      each shipped wrapper. A `MultiSink`'s `losses().resent` is the sum of its children's
      (invariants 2 and 6).
- [ ] An `HTTPSink._send` whose first attempt raises `TimeoutError` and whose second gets a `200`
      adds 1 to `log_foundry_counters.resent_events` and writes the line, and
      `SentrySink.losses().resent` reports its HTTP backend's count (invariant 2).
- [ ] After the first criterion's run, `health().events` has the same terms as the same run with
      the mark removed, and `unaccounted == 0` (invariant 2).
- [ ] The line is written only by `_diag.resent`. It names the sink by its stored class name and
      carries no value, and `tests/test_diag.py`'s guards pass over it (invariant 11).

### FR-002: Each shipped sink marks the sends that may have reached its destination

#### Description:

**The rule.** A `RETRYABLE` event is marked when its call may have reached the destination.

- **A reply** that refuses the event or the request is not marked: a throttle, a `4xx`, an entry
  an SQS or SNS `Failed` list names, or a Kinesis record carrying an `ErrorCode`. Each is the
  destination's word that it did not take the event. There are two exceptions:
  - A `5xx` is marked, because it says the request failed, not that nothing was stored. A
    gateway's `502` or `504` is an upstream reply it lost.
  - A Firehose record carrying an `ErrorCode` is marked. Its only two codes,
    `ServiceUnavailableException` and `InternalFailure`, are the internal error and the
    unavailability after which AWS says "the write might have completed".

  An item a `2xx` body refuses is never resent, so it raises no question of marking. That covers a
  `_bulk` item's status, a Honeycomb entry's status (including a `5xx` one) and an OTLP
  `partialSuccess`: each is `REJECTED` (`elasticsearch.py:207`, `honeycomb.py:374`, `otlp.py:323`).
- **A raise** is marked unless the sink can show the request never left. It shows that by where the
  call raised, when it can tell, and otherwise by the exception's class. **A client that retries
  inside the call can show neither**, because its final exception can follow an attempt that
  landed:
  - botocore retries `ReadTimeoutError`, `ConnectionClosedError` and `EndpointConnectionError` in
    both of its modes;
  - the Event Hubs producer retries `retry_total` times, 3 by default (`_configuration.py:26`,
    `_client_base.py:608` at azure-eventhub 5.15.1);
  - redis-py 8.1.0's client defaults to `Retry(retries=10)` (`client.py:285-290`,
    `_defaults.py:37`);
  - clickhouse-connect retries a connection the server closed (`httpclient.py:615-628` at 1.4.2).

  So every raise from those four clients is marked, whatever its class.

`_classify.may_have_landed(err) -> bool` holds the classes the single-attempt transports share:
builtin `ConnectionRefusedError` and `socket.gaierror`, and a `urllib.error.URLError` other than an
`HTTPError`. `urllib` raises a `URLError` only around connecting and writing the request (`do_open`).
A sink that can tell by call site passes its own answer.

| Sink | Marked | Not marked |
|---|---|---|
| HTTP family, via `_exchange` (`http.py:3262`), and `_send` | any retried exception `may_have_landed` does not clear (`TimeoutError` and `http.client.RemoteDisconnected` from `getresponse` among them), and a retryable status `>= 500` | a `URLError` other than an `HTTPError`, and a builtin refusal; `429` |
| `SQSSink`, `SNSSink`, `KinesisSink`, via `settle_keyed` and `settle_positional` (`_rounds.py:332`, `:436`) | every retried raise: botocore retries inside the call | every entry a reply names |
| `FirehoseSink`, via `settle_positional` | every retried raise, and every record carrying an `ErrorCode` | none |
| `SocketTransport`, TCP (`_socket.py:409`, `:501-521`) | the frame whose `sendall` raised, unless `may_have_landed` clears the exception | a frame whose connection could not be opened, and a `sendall` refused with `ConnectionRefusedError`; UDP never, since a datagram whose `sendto` raised was not sent |
| `RabbitMQSink` (`rabbitmq.py:417-441`) | a publish raising `StreamLostError`, `ConnectionClosedByBroker`, `AMQPHeartbeatTimeout` or `ConnectionBlockedTimeout` (the set SPEC-093 reads as an unconfirmed publish), or an exception outside pika's `AMQPError` that `may_have_landed` does not clear | any other pika error, such as a channel or connection it could not open (`ChannelWrongStateError`, `ConnectionWrongStateError`, any other `AMQPConnectionError`); pika does not retry inside a publish |
| `PostgresSink` (`postgres.py:446-470`) | a raise from `commit()` whose SQLSTATE is in class `08`, `57` or `XX`, or absent, and the same from any statement on an `autocommit` connection | a raise before `commit()` in a transaction, which the server rolls back, and any other SQLSTATE; psycopg does not retry |
| `AzureEventHubsSink` (`eventhubs.py:398`) | every retried raise: the producer retries inside the call | none |
| `ClickHouseSink` (`clickhouse.py:352`), `RedisStreamsSink` and `RedisListSink` (`redis.py:289-302`) | every retried raise: each client retries inside the call, and its class does not separate a refused connection from a lost reply (`redis/connection.py:1391`, `:1423`) | none |

**Sinks that never mark**, each in a derived roster's `EXEMPT` with its reason:

- `MongoDBSink`: its `_id` refuses a second copy (FR-003).
- `FileSink` and `RotatingFileSink`: a line the file took is never written again (SPEC-082).
- `StdoutSink` and `StderrSink`: a handed line is never handed again (SPEC-084).
- `NATSSink`: its resend is §12's `Nats-Msg-Id` item (*Out of Scope*).
- If SPEC-094 is on `main` when this is built, `KafkaSink` and `GooglePubSubSink` too, with the
  reason that their only `RETRYABLE` is a hand-off the client refused before it took the message.

**The departures this makes.** A sink that marks every failed call over-counts on a refused one.
That is the upper bound, and SPEC-092's new check (FR-004) reads it as a departure, whose record each
docstring carries:

- `_rounds.py`, in `settle_keyed`: "botocore retries a call inside the client, so a raise can follow
  an attempt that landed, and every raise an AWS sink's call makes is counted as one that may have
  landed".
- `firehose.py`: "AWS says a record coded `ServiceUnavailableException` or `InternalFailure` might
  have completed, so each is counted as one that may have landed".
- `eventhubs.py`: "the Event Hubs producer retries a send inside the client, so every send it fails
  is counted as one that may have landed".
- `clickhouse.py`: "clickhouse-connect retries inside the client and raises `OperationalError`
  whether or not the insert reached the server, so every insert it fails is counted as one that may
  have landed".
- `redis.py`, in `_RedisSink`: "redis-py retries a command inside the client and raises
  `ConnectionError` both for a connection it could not open and for one lost after the pipeline was
  written, so every pipeline it fails is counted as one that may have landed".

#### Acceptance Criteria:

- [ ] For each row of the table, a double raising or answering the "Marked" case marks every event
      of the call, and one raising or answering the "Not marked" case marks none. One test per row
      and case asserts the run's `resent` (invariants 2 and 7).
- [ ] An AWS sink's call raising botocore's `EndpointConnectionError`, and an Event Hubs send
      raising `ConnectError`, each counts `resent` equal to the call's events on the next round
      (invariant 2).
- [ ] A `FirehoseSink` reply coding a record `InternalFailure` counts that record's resend. A
      `KinesisSink` reply coding one `ProvisionedThroughputExceededException` counts none
      (invariant 2).
- [ ] An `HTTPSink` round answered `503` and then `200` counts `resent` equal to the chunk's
      events. One answered `429` and then `200` counts 0 (invariant 7).
- [ ] A `PostgresSink` transaction whose `executemany` raises `08006` counts 0. One whose `commit()`
      raises `08006` or `57P01`, or raises with no SQLSTATE, counts every row. One whose `commit()`
      raises `XX000` is classified as a call that may have landed, but `_retryable_error` does not
      retry class `XX`, so the round is `REJECTED`, makes one attempt and counts 0. One whose
      `commit()` raises `40001` counts 0 (invariant 2).
- [ ] A test derives the engine sinks from the tree. It fails for any sink that is in `EXEMPT` and
      marks in its test, and for any sink that is neither in `EXEMPT` nor marking in one of this
      FR's tests. Each `EXEMPT` entry carries its reason (invariant 2).
- [ ] With `_classify.may_have_landed` answering `True` for every exception, in an export of the
      branch head, every `refused-once` and `refused-always` row of the HTTP family (its four
      wrapper variants and `SentrySink[http]` included) and the two TCP socket variants reads
      `resent-when-knowable`: 34 rows on either base (invariant 2).
- [ ] With `RunContext.may_have_landed` doing nothing, in an export of the branch head, the 50
      `taken-then-raised` and `reply-lost` rows FR-004 turns green read `duplicate` again
      (invariant 2).

### FR-003: `MongoDBSink` reads a duplicate driver-stamped `_id` on its own resend as landed

#### Description:

`MongoDBSink` keeps a run's documents, with the `_id` pymongo stamped into each, across the run's
rounds (`mongodb.py:383-450`). So a resend after an `insert_many` that raised carries the same
`_id`, and MongoDB refuses the copy with a duplicate-key `writeErrors` entry, code `11000`. Today
`_settle_bulk` (`:452`) reads that as `REJECTED`. The document is counted lost and announced as
rejected, although the earlier send stored it. That is SPEC-092's `reply-counted-lost` rows for
MongoDB.

`_settle_bulk` reads such an entry as `ACCEPTED` when all four conditions hold:

1. its `code` is exactly `11000`;
2. its `keyPattern` is exactly `{"_id": 1}`, or, when the entry carries no `keyPattern`, its
   `errmsg` contains ` index: _id_ `;
3. the event the document was built from carries no `_id`, so the `_id` is the one the driver
   stamped in this run and no other event can share it;
4. the document was carried by an earlier call of this run, and that call raised (`sends_of` above
   1, since the only earlier call that leaves a document to resend is one that raised).

Any other duplicate key is `REJECTED`, as today. That covers another unique index, a first send,
and an `_id` the event carried itself: a `TransformSink` that gives two events one `_id` must see the
second refused.

Such an `ACCEPTED` document is in `encoded_landed`, and `rejects` does not count it. When every entry
reads `ACCEPTED`, `_settle_bulk` registers no line. Today it would write
`log-foundry: lost 0 document(s)`.

`MongoDBSink` still declares no `log_foundry_idempotent` (FR-005), and it never marks a resend
(FR-002): within a run, its resend cannot leave a second copy.

#### Acceptance Criteria:

- [ ] An `insert_many` that stores every document and raises `NetworkTimeout`, then a second round
      answered `11000` on `_id` for each, leaves each document `ACCEPTED`. `failed_events`,
      `losses().failed` and `losses().resent` stay 0, and no line matching `^log-foundry: lost` is
      written (invariants 2 and 7).
- [ ] A first send answered `11000` on `_id` is `REJECTED` and counted, as today (invariant 2).
- [ ] A resend answered `11000` whose `keyPattern` is `{"log_id": 1}` is `REJECTED` (invariant 2).
- [ ] A resend answered `11000` with no `keyPattern` and an `errmsg` naming `index: _id_` is
      `ACCEPTED`. One whose `errmsg` names another index is `REJECTED` (invariant 7).
- [ ] Two events given the same `_id` by a `TransformSink`, sent in a call that raised and then
      resent, with the second answered `11000` on `_id`, leave the second `REJECTED` and counted
      (invariant 2).
- [ ] With condition 3 removed, in an export of the branch head, the previous criterion fails. With
      condition 4 removed, the first-send criterion fails (invariant 2).
- [ ] **Owed until a green run:** `tests/integration/test_mongo.py` runs a `MongoDBSink` against the
      real MongoDB service, with a collection whose `insert_many` stores the documents and then
      raises pymongo's `NetworkTimeout` once. Afterwards the collection holds each document once,
      `losses()` reads `failed=0`, and the server's `writeErrors` entries carry `keyPattern`
      `{"_id": 1}`. It is triggered by the PR, since `integration.yml`'s path filter includes
      `src/log_foundry/sinks/**` (invariants 2 and 7).

### FR-004: The conformance corpus reads a counted resend, and its pins and departures move

#### Description:

These are changes to SPEC-092's corpus files as its build delivers them, after SPEC-093's and
SPEC-094's where those are on `main`.

- **The reconstruction.** The round wrapper in `conformance_backend.Recorder` (the build's name for
  the prototype's `runs()`) also records each round's `may_have_landed` marks, read from the run's context after `deliver` returns. `resends_from`
  rebuilds, per event and per run, how many times the run offered the event again after a round
  that marked it.
- **The oracle.** `agrees` compares that reconstruction with the run's `Delivery.resent`, and a
  difference fails `partition`, as a fate count does.
- **`duplicate`.** For a per-event row it fails when the backend holds an event more times than one
  plus the resends counted for it. For an `emit`-only row it fails when the extra copies held
  outnumber the move in `losses().resent` over the row.
- **`resent-when-knowable`**, a new check after `ambiguous-when-knowable`, fails in two cases: a
  resend was counted for an event that no unknowable call carried, or, for an `emit`-only row, the
  row's `losses().resent` moved and none of its calls was unknowable. It is what keeps the count
  from being a way out: a sink that marked every raise would pass `duplicate` and fail this.
- **`unannounced`** also fails a batch that counted a resend and wrote no line matching
  `^log-foundry: resent \d+ ` inside its own `Worker._emit`.
- **The doubles.**
  - The PostgreSQL double makes every call of a transaction unknowable when its commit's reply is
    lost. Today it marks only the first `executemany`, though the commit applied every chunk.
  - The MongoDB double's duplicate-key entry takes the server's shape: `code` `11000`, `keyPattern`,
    `keyValue`, and an `errmsg` naming `index: _id_`.

  On `src` as it stands, neither moves a row.
- **Pins.**
  - `reply-resent` is removed, because its 50 rows read green.
  - `reply-counted-lost` keeps `PostgresSink[borrowed]-reply-lost` and
    `RabbitMQSink[borrowed]-taken-then-raised` at `lost-but-held`, under the new §12 item FR-005
    adds, and loses its two `MongoDBSink` rows, which read green.
- **Departures.** Five are new, each holding its rows at `resent-when-knowable` and quoting FR-002's
  phrase:
  - `aws-client-retries` holds `refused-once` and `refused-always` of `SQSSink[standard]`,
    `SQSSink[fifo]`, `SNSSink`, `KinesisSink` and `FirehoseSink` (10);
  - `firehose-coded-may-land` holds `FirehoseSink-partial` (1);
  - `eventhubs-client-retries` holds `AzureEventHubsSink` `refused-once` and `refused-always` (2);
  - `clickhouse-error-unclassed` holds `ClickHouseSink` `refused-once` and `refused-always` (2);
  - `redis-client-retries` holds the same two faults of `RedisStreamsSink` and `RedisListSink` (4).
- **Totals.**

  | Base | Rows | Pinned | Departure rows | Must pass |
  |---|---|---|---|---|
  | `main` | 256 | 22, under 11 pins | 31, under 12 departures | 203 |
  | after SPEC-094 | 268 | 6, under 4 pins | 34, under 14 departures | 228 |

#### Acceptance Criteria:

- [ ] The corpus's literals read the totals of the base the build starts from (invariants 2 and 7).
- [ ] Against the corpus before this spec, exactly 71 rows change verdict, and every other row
      reads as before (invariants 2 and 7):
      - the 50 `reply-resent` rows, from `duplicate` to green;
      - `MongoDBSink` `taken-then-raised` and `reply-lost`, from `lost-but-held` to green;
      - the 19 departure rows, from green to `resent-when-knowable`.
- [ ] With this FR's corpus changes and `src/` before this spec, no row changes verdict
      (invariants 2 and 7).
- [ ] `judge`'s standing corpus in `tests/test_conformance_gate.py` holds these hand-built reports,
      each asserting its exact verdict (invariants 2 and 7):
      - for `duplicate`, per event: a second copy counted, which passes, and one not counted, which
        fails;
      - for `duplicate`, per batch: both the same ways;
      - for `resent-when-knowable`, per event: a resend on a knowable call, which fails, and one on
        an unknowable call, which passes;
      - for `resent-when-knowable`, per batch: a moved `losses().resent` with every call knowable,
        which fails, and with one call unknowable, which passes;
      - for `unannounced`: a batch with a resend and no resend line, which fails;
      - for `partition`: a reconstruction disagreeing with `Delivery.resent`, which fails.
- [ ] With `_diag.resent` writing nothing, in an export of the branch head, every row that counts a
      resend reads `unannounced` (invariant 2).
- [ ] With `Delivery.resent` never set, in an export of the branch head, every per-event row that
      counts a resend reads `partition`. With `_run` never committing the count it was asked to
      commit to other counters (`resent_into`, the route `HTTPSink._send` takes),
      `SentrySink[http]`'s two such rows, judged per batch, read `duplicate` (invariant 2).
- [ ] With `resent-when-knowable` removed from `judge`, the 19 departure rows read green, which
      fails each departure (invariant 2).

### FR-005: The record says what a resend costs and what a declaration of idempotence needs

#### Description:

These are the completion ritual's edits.

- **§12.**
  - **The finding-5 item is struck as closed by SPEC-095.**
    - Its closure carries the audit probes' numbers before and after (*Evidence*), and *The bound*
      word for word.
    - Its direction promised "an idempotency key derived from `log_id` wherever the destination can
      honour one". The closure says this spec narrowed that: the key is carried in every body and
      in SQS FIFO's `MessageDeduplicationId`, and is not sent as an Elasticsearch or OpenSearch
      `_id`. That was decided in session on 2026-11-16, for the reasons the *Revision history*
      gives. JetStream's `Nats-Msg-Id` stays with its own item.
  - **A new item: "An event a send may have landed is counted as lost when no later send lands
    it".**
    - It holds the two `reply-counted-lost` rows, and the every-reply-lost case: in
      `probe_aws_more.py` case 1, ten events are each held 16 times, with `failed=10`, `flush()`
      falsy and now `resent=150`.
    - It quotes Andrew's "Lost, flush falsy (Recommended)" of 2026-11-16 as the decision that keeps
      it open.
    - **Closed by** the engine reading such an event `UNKNOWN`, at the cost that a destination which
      times out every reply no longer fails its batches.
  - **A new item for SNS FIFO topics: "`SNSSink` cannot write an SNS FIFO topic".** It quotes
    Andrew's "Follow-up spec (Recommended)" of 2026-11-16, and is **Closed by** the follow-up spec of
    this arc that adds the topic's group and de-duplication ids.
- **Decisions in `docs/decisions/sink-delivery/`.**
  - **A new decision, with fence and entry: "A resend after a send that may have landed is
    counted, and its key is the event's `log_id`".**
    - It records FR-001's count and line, FR-002's rule with its table (the clients that retry
      inside the call among it), the key table above, and *The bound* word for word.
    - It records the three choices decided in session on 2026-11-16, each as the orchestrating
      session's: the count lives in `SinkLosses.resent`, no Elasticsearch `_id`, and `MongoDBSink`
      is not declared idempotent.
  - ***No shipped sink declares itself idempotent* is refined in place, with a dated History
    entry.** It now states what a declaration needs:
    1. A destination that refuses or absorbs a second copy carrying the same key, for as long as the
       engine can resend: either no window, or a window the sink bounds every resend to. Where the
       destination refuses the copy rather than absorbing it silently, the sink must also read that
       refusal of its own resend as delivery, not as a loss, as FR-003 does for `MongoDBSink`.
    2. A key the sink sets per event, identical on every resend the engine makes.
    3. An `integration.yml` test against the real service showing that a resend of an event that
       landed leaves one copy and is counted delivered, with the vendor's window quoted in the
       decision.

    No shipped sink meets all three in this spec:
    - `SQSSink` FIFO fails the first, because `spec095_fifo_window.py` shows a resend can fall
      past the 5-minute interval. It fails the third too, because there is no SQS service.
    - `MongoDBSink` meets the first two once FR-003 lands, and the third only when FR-003's owed
      run is green. It is not declared, decided in session (*Revision history*).
  - ***A client exception costs its chunk, and is provable non-delivery for it* gains a dated
    History entry:** the duplicate it accepts is now counted and announced (SPEC-095).
- **`docs/invariants.md`.** Invariant 7's observable ("no event landed twice") gains its recorded
  exception: a resend after a lost reply can land an event twice, counted in `losses().resent` and
  announced, never resent beyond the 1.x total. Invariant 2's and 7's *Guarded* lines name the new
  tests.
- **README.**
  - *Never crashes the app* (`README.md:631-637`): "there is nothing downstream to duplicate" is
    replaced by the cost and the count.
  - The `health()` section (`:1350`) says `h.sink` carries `dropped` and `failed`, and now names
    `resent` too.
  - The custom-sink examples (`:1100-1102`, `:1217-1218`) stay as they are, and the text beside
    them says a `SinkLosses` built by keyword needs no change.
  - The `SNSSink` row (`:889`) says a FIFO topic is not supported yet.
- **The release notes**: in the pending `docs/release-notes/v1.2.1.md` if it is on `main` when this
  spec completes, and under a "Release notes owed" heading in the delivery doc otherwise.
  - *Added*: `SinkLosses.resent`, `SinkCounters.resent_events` and the line.
  - *Changed*:
    - `MongoDBSink`'s duplicate driver-stamped `_id` on its own resend is delivered, not counted
      in `failed`;
    - the AWS, Event Hubs, Redis and ClickHouse sinks count every failed call they resend.
  - The compatibility list gains four entries:
    - code that compares a `SinkLosses` by `repr`, `dataclasses.astuple` or `dataclasses.fields`;
    - code that relies on `SinkLosses` or `Health` equality or hash. Two snapshots that were equal
      differ once `resent` does, and their hashes do too;
    - code that matches stderr;
    - code that relies on MongoDB's `failed` counting such a document.
  - Every sentence saying no constructor signature changed is rewritten, since `SinkLosses` gains a
    keyword.
- **`docs/component-inventory.md`**: no row is owed, since no module is added.

#### Acceptance Criteria:

- [ ] §12 strikes the finding-5 item, dated and naming SPEC-095. It states *The bound* and the
      narrowing of the `log_id` key, and carries the two new items with their **Closed by**.
- [ ] The new decision file and the two dated History entries exist. The new decision names the
      three in-session choices as the orchestrating session's. The refined fence states the three
      conditions, with the first requiring the destination's refusal of a second copy to be read as
      delivery, and names `SQSSink` FIFO and `MongoDBSink` against them.
- [ ] `docs/invariants.md` records the resend after a lost reply beside "no event landed twice",
      and no README sentence says a sink's resend cannot duplicate.
- [ ] The release notes name the field, the line, the two *Changed* entries and the four
      compatibility entries, and no sentence there says no constructor signature changed.
- [ ] This FR serves no invariant: it is the record of FR-001 to FR-004, and the promises it
      describes are theirs.

---

## Data Model

```python
# src/log_foundry/sinks/_engine.py
@dataclass(frozen=True, kw_only=True)
class Delivery:
    ...                                  # unchanged fields
    resent: int = 0                      # events offered again after a round that marked them

class _Marks(NamedTuple):
    final: tuple[object, ...]
    refused: Exception | None
    counted: tuple[object, ...]
    reached: tuple[object, ...]          # RunContext.may_have_landed, as given

# src/log_foundry/sinks/verdict.py
class SinkCounters:
    dropped_events: int
    failed_events: int
    resent_events: int                   # written only by an engine commit (or _send's Deferral)

# src/log_foundry/sinks/base.py
@dataclass(frozen=True, kw_only=True)
class SinkLosses:
    dropped: int
    failed: int
    resent: int = 0                      # appended; __match_args__ stays ()

# src/log_foundry/sinks/_rounds.py
class Called[T](NamedTuple):
    result: T | None
    error: Exception | None = None
    retryable: bool = False
    reached: bool | None = None          # a sink's own answer; None asks may_have_landed
```

## API / Interface Contract

```python
# src/log_foundry/sinks/_engine.py
RunContext.may_have_landed(self, indices: Iterable[int]) -> None

# src/log_foundry/sinks/_classify.py
def may_have_landed(err: Exception) -> bool: ...   # False only for a class that shows nothing left

# src/log_foundry/_diag.py
def resent(count: int, detail: str = "") -> None: ...
#   log-foundry: resent <count> event(s) a lost reply may have landed; they may be duplicated; <detail>

# public, read by a caller
health().sink.resent        # int; 0 when nothing was resent
sink.losses().resent        # int
```

## File & Folder Structure

```
src/log_foundry/
├── _diag.py                      # resent()
└── sinks/
    ├── _engine.py                # the mark, the count, Delivery.resent, the commit and line
    ├── _classify.py              # may_have_landed()
    ├── _rounds.py                # Called.reached; the AWS settle helpers mark
    ├── verdict.py, base.py       # resent_events, SinkLosses.resent
    ├── _deferral.py, multi.py, _factory.py, logstash.py   # losses() pass resent
    ├── http.py, elasticsearch.py, honeycomb.py, otlp.py, sentry.py   # marks, _send, losses()
    ├── nats.py, kafka.py, pubsub.py, sqlite.py           # losses() pass resent
    ├── firehose.py, _socket.py, rabbitmq.py, postgres.py, clickhouse.py, eventhubs.py, redis.py
    └── mongodb.py                # FR-003
tests/
├── test_resend_count.py          # FR-001, with the SinkLosses AST guard and the losses() roster
├── test_resend_marks.py          # FR-002, with the derived roster
├── test_sinks_mongodb_resend.py  # FR-003
├── test_conformance*.py          # FR-004, SPEC-092's files
└── integration/test_mongo.py     # FR-003's owed run
```

## Implementation Phases

### Phase 1: The count (FR-001)

- The mark, its validation, rewind and delegation; the count in `_run`; `Delivery.resent`;
  `SinkCounters.resent_events`, `SinkLosses.resent` and every `SinkLosses` call site, with the AST
  guard; the line; `_send`.

### Phase 2: The marks and MongoDB (FR-002, FR-003)

- `_classify.may_have_landed`, `Called.reached`, each family's site, the derived roster and
  `EXEMPT`, the five departure phrases; `_settle_bulk`'s duplicate-key reading, its silent all-landed
  case, and its integration test.

### Phase 3: The corpus and the record (FR-004, FR-005)

- The reconstruction, the oracle, `duplicate`, `resent-when-knowable`, `unannounced`, the two
  doubles, the pins, departures and totals, the gate fixtures and mutants; then the record.

## Evidence

**The prototype.** Measured on macOS with Python 3.13.1 and no extra installed, at `97a03f45`.

- **The patches.** `docs/audits/2026-11-14-probes/sinks/spec095_prototype.patch` applies with
  `patch -p1` to `git archive 97a03f45 src tests docs/audits/2026-11-14-probes`. Its twin,
  `spec095_prototype_after_094.patch`, applies to that tree after SPEC-093's and SPEC-094's
  prototype patches. It differs only where those changed `rabbitmq.py` and `sentry.py`.
- **What they cover.** SPEC-095's `src` change and its corpus change, including the overrides in
  `elasticsearch.py`, `honeycomb.py`, `otlp.py`, `nats.py`, `_deferral.py` and `multi.py`.
- **What they leave out.** The build's tests, the `_send` `Deferral`, `resent=0` at the five sites
  with no engine counter, and the `EXEMPT` roster.
- **The review's reproductions.** Run on the revised prototype, the review's `es_losses.py` reads
  `losses().resent=8` beside `Delivery.resent=8` for `ElasticsearchSink`, `OpenSearchSink`,
  `HoneycombSink` and `OTLPSink`, where the first revision read 0. Its `mongo_zero.py` finds no
  `lost 0` line on any MongoDB row.

**The rows.** `spec095_compare.py`, run against fresh exports:

| Base | Before | After |
|---|---|---|
| `main` | 256 rows, 170 green, 86 red | 203 green, 53 red |
| after SPEC-094 | 268 rows, 195 green, 73 red | 228 green, 40 red |

On each base, 71 rows move:

- 50 from `duplicate` to green;
- `MongoDBSink` `taken-then-raised` and `reply-lost`, from `lost-but-held` to green;
- 19 from green to `resent-when-knowable`.

The first revision of this spec moved six rows to `resent-when-knowable`: ClickHouse's and the two
Redis sinks' refusals. The review's findings 2 and 3 added 13 more:

- the `refused-once` and `refused-always` rows of the five AWS variants and of
  `AzureEventHubsSink`, 12 rows, because a raise from a client that retries internally is now
  marked;
- `FirehoseSink-partial`, because Firehose's coded records are now marked.

No other row moved. The prototype's corpus, run against each base's own `src`, moves no row. Each
comparison gave identical output in two runs on Python 3.13.1 and one on Python 3.12.12, apart from
the wall time.

**The mutants**, `spec095_mutants.py` on each prototype tree:

| Mutant | Rows that move |
|---|---|
| `may_have_landed` always `True` | 34, green to `resent-when-knowable`: the `refused-once` and `refused-always` rows of 17 variants, the HTTP family's and the TCP sinks' |
| the context drops every mark | 69: the 50 back to `duplicate`, and the 19 departure rows to green |
| `_diag.resent` silent | 77 on each base, every row that counts a resend, each gaining `unannounced` |
| `Delivery.resent` never set | the same 77, each gaining `partition`, except `SentrySink[http]`'s two rows, which read `duplicate` because they are judged per batch from `losses()` |

**The audit's probes, before and after**, run on the two `main` exports:

| Probe | Before | After |
|---|---|---|
| `probe_sqs.py` case D (standard queue, one reply lost) | 35 messages for 25 events, `failed=0`, no line | the same 35, and `resent 10` with one line, which is the 10 extra copies exactly |
| `probe_sqs.py` case D' (FIFO, the fake de-duplicating) | 25 held | 25 held, and `resent 10`: an upper bound |
| `probe_aws_more.py` case 1 (every reply lost) | each of 10 events held 16 times, `failed=10`, `flush()` falsy | the same, with `SinkLosses(dropped=0, failed=10, resent=150)` and one resend line; 150 is the extra copies exactly |
| `probe_misc.py` cases 1 and 6 (PostgreSQL commit reply lost; HTTP connection closed with no status line) | 50 rows or bodies for 25, no line | the same 50, and one resend line each |

**The cost.** `spec095_cost.py`, three runs per tree, one-minute load 3.4. The revisions after this
measurement touched no engine code. Each figure is a median of `_engine._run` over one of two
`deliver`s:

- **clean**: it accepts everything;
- **one lost reply**: it answers `RETRYABLE` for every event once and then accepts. After this
  spec, this case includes the resend line, written to `/dev/null`.

| Events | Clean, before | Clean, after | One lost reply, before | One lost reply, after |
|---|---|---|---|---|
| 1 | 8.5 to 8.6 µs | 8.9 to 9.1 µs | 11.4 µs | 17.4 to 17.8 µs |
| 100 | 44.7 to 45.6 µs | 47.0 to 48.0 µs | 61.8 to 63.1 µs | 79.5 to 80.8 µs |
| 1000 | 361.6 to 364.4 µs | 368.0 to 374.3 µs | 500.3 to 503.2 µs | 608.6 to 621.1 µs |

A clean run costs up to 3% more. A run that lost a reply, already the slow path, costs about 6 µs,
18 µs or 115 µs more. No request carries another byte.

**The FIFO interval.** `spec095_fifo_window.py` computes, with the engine's own wait functions, when
each round can start, counted from the first:

- At `SQSSink`'s defaults (16 rounds) with calls failing at once, the last round starts 2.87 s after
  the first.
- With each call ending after five botocore attempts at the default 60-second read timeout
  (`botocore/endpoint.py` `DEFAULT_TIMEOUT = 60`), round 2 starts more than 300 s after round 1.
  Five is the default `legacy` mode at botocore 1.43.127.
- With three attempts, the `standard` mode, round 3 does.
- With `max_retries=20` and calls failing at once, round 13 does.

So the engine does not bound a resend to the de-duplication interval.

**Read live on 2026-11-16, quoted.**

- *SQS `SendMessage`*:
  - `MessageDeduplicationId`: "This parameter applies only to FIFO (first-in-first-out) queues."
  - "any messages sent with the same MessageDeduplicationId are accepted successfully but aren't
    delivered during the 5-minute deduplication interval"
  - "If a message is sent successfully but the acknowledgement is lost and the message is resent
    with the same MessageDeduplicationId after the deduplication interval, Amazon SQS can't detect
    duplicate messages."
- *SQS at-least-once delivery*: "you might get that message copy again when you receive messages.
  Design your applications to be idempotent".
- *SQS message metadata*: "All components of a message attribute are included in the 1 MiB message
  size restriction." So an attribute copy of `log_id` would cost bytes and tell a consumer nothing
  the body does not.
- *SNS `PublishBatchRequestEntry`*:
  - `MessageDeduplicationId`: "This parameter applies only to FIFO (first-in-first-out) topics."
  - `MessageGroupId`, FIFO topics: "You must associate a non-empty MessageGroupId with a message.
    If you do not provide a MessageGroupId, the action fails."
- *SNS message deduplication for FIFO topics*: "any message published with the same deduplication
  ID, within the five minute deduplication interval, is accepted but not delivered."
- *Kinesis, handling duplicate records*: "Applications that need strict guarantees should embed a
  primary key within the record to remove duplicates later when processing."
- *Firehose `PutRecordBatch`*:
  - An unsuccessful record's `ErrorCode` "is one of the following values: ServiceUnavailableException
    or InternalFailure".
  - "If there is an internal server error or a timeout, the write might have completed or it might
    have failed."
  - "We recommend that you handle any duplicates at the destination."
- *boto3, retries*:
  - Legacy mode: "A default value of 5 for maximum attempts (including the initial request)",
    retrying `ConnectionError`, `ConnectionClosedError`, `ReadTimeoutError` and
    `EndpointConnectionError`.
  - Standard mode: "A default value of 3 for maximum attempts (including the initial request)",
    retrying `ConnectionError` and `HTTPClientError`, which `ReadTimeoutError` subclasses
    (`botocore/exceptions.py:140` at 1.43.127).
  - The guide calls standard the default. The installed botocore 1.43.127 chooses `legacy` unless
    `AWS_NEW_RETRIES_2026` enables the new retries (`botocore/configprovider.py:23-41`), and either
    mode retries a lost reply inside the call.
- *Azure Event Hubs exceptions*: "ConnectError: Failed to connect to the Event Hubs service."
- *Azure `EventHubProducerClient`*: `retry_total`, "Default value is 3."
- *Elastic, bulk API*: "A create action fails if a document with the same ID already exists in the
  target", and "An index action adds or replaces a document as necessary."
- *Elastic, indexing speed*: "When indexing a document that has an explicit id, Elasticsearch needs
  to check whether a document with the same id already exists within the same shard, which is a
  costly operation".
- *ClickHouse, deduplicating inserts on retries*:
  - "For *ReplicatedMergeTree engines, the deduplication log is enabled by default"
  - "A plain MergeTree table therefore deduplicates nothing until you set that window to a positive
    value."
  - Each block's `block_id` "is a hash of the data in that block".
- *NATS JetStream streams*: "Duplicate Window: 2m0s. For two minutes after a message is stored, the
  server turns away a second message that carries the same Nats-Msg-Id header."
- *MongoDB `insertMany`*: "If the document contains an _id field, the _id value must be unique
  within the collection to avoid duplicate key error." The error codes reference lists `11000`
  `DuplicateKey`.

## Revision history

- **Authored on 2026-11-16 against `97a03f45`.** Five choices were open. Two were product calls for
  Andrew, and three were decided in session.
- **Andrew's answers, 2026-11-16.**
  - **The last fate**, "Lost, flush falsy (Recommended)". An event that a send may have landed, and
    that no later send landed, keeps its last round's fate: the run fails, `flush()` is falsy and the
    event is counted lost. The alternative was to read it `UNKNOWN`. That would have turned the two
    `reply-counted-lost` rows green, but would have made a destination that times out every reply
    settle each batch, so `failed_batches` would stop moving in that outage.
  - **SNS FIFO topics**, "Follow-up spec (Recommended)". Writing an SNS FIFO topic becomes its own
    spec later in this arc, recorded in §12 until then (FR-005).
- **Decided in session on 2026-11-16, by the orchestrating session, not by Andrew.**
  - **The count lives in `SinkLosses.resent`**, appended with a default of 0, so `health().sink`
    shows it. The alternatives were `SinkCounters` alone, which `health()` cannot see, and the event
    ledger, where a duplicate is not an event.
  - **No `_id` for `ElasticsearchSink` and `OpenSearchSink`.** Sending `log_id` as `_id` would make
    a resend overwrite rather than duplicate. It would also change every document's `_id` from one
    Elasticsearch generates, add about 44 bytes to each action line, and cost the existing-id check
    Elastic calls "a costly operation". The count already turns these rows green.
  - **`MongoDBSink` is not declared idempotent.** It would meet the proof once FR-003's integration
    run is green, but declaring it reverses part of a settled fence for a gain only on a
    `writeErrors` that cannot be paired.
- **Revised on 2026-11-16 after the spec review.** Every finding was fixed and none was rejected:
  1. **`losses()` overrides.** `ElasticsearchSink`, `OpenSearchSink`, `HoneycombSink` and `OTLPSink`
     dropped `resent`. FR-001 now names all fifteen `SinkLosses` sites. It adds an AST guard with
     no allowlist and a roster over every class whose `losses()` returns one, and the prototype
     carries the overrides.
  2. **Clients that retry internally.**
     - A raise from botocore, the Event Hubs producer, redis-py or clickhouse-connect is now
       marked, whatever its class. Their retries are left as they are.
     - The 12 AWS and Event Hubs refusal rows moved to two new departures.
     - *The bound* now states the count exactly. The sentence that called a client's earlier
       attempts that library's business is gone.
  3. **Firehose `ErrorCode`.** Firehose's coded records are marked, since AWS says such a write
     "might have completed". `FirehoseSink-partial` moved to a new departure, and Kinesis is
     unchanged.
  4. **MongoDB caller-set `_id`.** FR-003 adds condition 3: the event carried no `_id`, so only a
     driver-stamped one is read as landed. A criterion covers two events sharing a transform-set
     `_id`.
  5. **False loss line.** `_settle_bulk` registers no line when nothing was rejected, and FR-003's
     first criterion asserts no `^log-foundry: lost` line.
  6. **PostgreSQL `commit()`.** Classes `57` and `XX` at `commit()` are marked.
  7. **botocore's default.** *Evidence*, *Out of Scope* and `spec095_fifo_window.py` now say the
     installed botocore defaults to `legacy`, five attempts. The probe gained that case, which puts
     round 2 past the 5-minute interval.
  8. **Compatibility list.** It names `SinkLosses` and `Health` equality and hash.
  9. **§12 closure.** It says the session's "no Elasticsearch `_id`" narrowed the `log_id` key the
     audit item promised, and why.
  10. **FR-005's proof.** Its first condition now requires the destination's refusal of a second
      copy to be read as delivery.
  11. **Wording.**
      - The description names the two MongoDB rows and the 19 departure rows.
      - "Decision A" is cited as SPEC-092's finding-14 decision.
      - FR-002 says a per-item refusal inside a `2xx` body, a `5xx` one included, is `REJECTED`
        and never resent.
      - This history counts five choices: two answered by Andrew and three decided in session.
- **Corrected on 2026-11-19 at build start.** FR-004 named the prototype's round wrapper in `runs()`;
  SPEC-092's build delivered it as `conformance_backend.Recorder`, and the text now says so. A
  rename, not a change of scope.
- **Corrected on 2026-11-19 at build start, decided in session.** FR-002's `PostgresSink` criterion
  said a `commit()` raising `XX000` counts every row, but `_retryable_error` does not retry class
  `XX`, so nothing is resent: the criterion now says such a call is classified as one that may have
  landed and counts 0. *The bound*'s over-count list gains a re-offer the sink refuses unsent
  because it was closed during the backoff, since the count is taken on the offer, as FR-001 says.
- **Corrected on 2026-11-20 during the build, decided in session.** The plan review had
  `HTTPSink._send` hand its resends to the engine, which commits them to the sink's own counters
  (`_run`'s `resent_into`) before writing the one line, rather than `_send` reading
  `Delivery.resent` and committing them itself. So `Delivery.resent` no longer feeds
  `SentrySink[http]`'s count, and FR-004's `Delivery.resent` mutant moves only the per-event rows;
  the `SentrySink[http]` half of that criterion is now measured with the `resent_into` commit
  removed instead. A correction of how the criterion is measured, not of what it holds.
