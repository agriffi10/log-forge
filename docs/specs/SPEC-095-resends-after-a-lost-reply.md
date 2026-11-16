---
type: Spec
id: SPEC-095
title: "A Resend After a Lost Reply Is Counted, and Its Key Is the Event's Own `log_id`"
description: "Counts and announces every event the delivery engine sends again after a send that may have landed it, has each shipped sink mark exactly such sends, reads a MongoDB duplicate _id on such a resend as delivery, and settles that the event's log_id, already in every body, is the idempotency key and what a sink must prove before it declares itself idempotent, so SPEC-092's reply-resent rows turn green and no other row moves except six new departures."
spec_status: Draft
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
one line per batch that had any. Each shipped sink says which of its sends may have landed and which provably did not, so
a refused connection is not counted as a possible duplicate. MongoDB's `_id` already refuses the
second copy, and that refusal is now read as delivery rather than counted as a loss. The spec also
settles the idempotency key: it is the event's own `log_id`, which every event already carries in
its body and which SQS FIFO queues already de-duplicate on. No new key goes on the wire. Last, it
states what a sink must prove before it may declare itself idempotent and so have an outcome it
cannot read resent.

## Scope

### In Scope

- The delivery engine counts each event it offers again after a round that marked the event as
  possibly landed: in `SinkCounters.resent_events`, `SinkLosses.resent` and `Delivery.resent`, with
  one stderr line per run (FR-001).
- Each shipped engine sink marks the sends that may have reached its destination, and only those,
  by one rule with a stated form per family, and the sinks that never resend a send that may have
  landed are listed with their reasons (FR-002).
- `MongoDBSink` reads a duplicate-key error on `_id`, for a document an earlier call of the same run
  carried and that call raised, as landed (FR-003).
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
- **Resending an `UNKNOWN` outcome.** No shipped sink declares `log_foundry_idempotent`, before or
  after this spec (FR-005 states the proof a declaration needs, and why no sink meets it here).
- **Reading an event that a send may have landed, and that no later send landed, as `UNKNOWN`.**
  This spec keeps the engine's rule that an event's fate is its last round's. So a run in which
  every reply is lost still fails, and its events are still counted lost. The resend count now says
  how many copies may exist. The two `reply-counted-lost` rows this leaves red move to a new §12 item
  (FR-005). *Revision history* gives the choice this side was taken on.
- **A new key on the wire.** No message attribute, header or document field is added to any request.
  `log_id` is already in every body (`model.py:219`), and on SQS FIFO it is already the
  `MessageDeduplicationId` (`sqs.py:540-564`). *Revision history* gives the choice made for
  `ElasticsearchSink` and `OpenSearchSink`, which could send it as `_id`.
- **`NATSSink`.** A JetStream publish whose ack wait expired is resent through the worker, and that
  resend stays uncounted here. Its closure is §12's *A timed-out JetStream publish is not retried,
  and cannot safely be*, whose `Nats-Msg-Id` header that item calls its own spec.
- **SNS FIFO topics.** `SNSSink` sends no `MessageGroupId` (`sns.py:370-405`), and SNS refuses a FIFO
  publish without one. Writing a FIFO topic is a new capability, not a key for an existing write.
- **Resends the engine does not make.** Three kinds go uncounted:
  - a client library's own internal retries: botocore's standard mode makes up to three attempts per
    call, and the Azure Event Hubs producer's `retry_total` defaults to 3;
  - the worker's whole-batch resend of a sink with no `deliver`, `MultiSink` included;
  - a third-party sink's own loop of `deliver` calls.

  The mark is private, so a third-party engine sink's resends are not counted either.
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
- **After SPEC-094**: SPEC-093's and SPEC-094's prototype patches applied, from the branch
  `docs/spec-093-acknowledgement-fixes`. 268 rows: 58 pinned under five pins, 15 departure rows
  under nine departures, and 195 that must pass.

The build uses whichever base is on `main` when it starts. *Evidence* shows that this spec moves
the same 58 rows on both.

**What this spec builds on.**

- *A sink may report per event* (SPEC-072): the engine resends only `RETRYABLE` events, and an
  `UNKNOWN` one only for a sink declaring `log_foundry_idempotent = True`.
- The run context's marks: `final_rejections`, `sink_counted` and `refuse`
  (`_engine.py:712-803`). They are validated against the round's verdict (`_read_marks`,
  `:1322`) and moved into a wrapper's positions by `delegate` (`:803`). FR-001 adds a fourth mark
  of the same kind.
- **Andrew's settled answers that bear on it.** Finding 14 (2026-11-15, "Unadjudicated"): an AWS
  reply that names no failure counts every unnamed event `UNKNOWN`. SPEC-089 ("Keep truthy, add
  counts") and SPEC-094 ("Truthy, counted", 2026-11-16): `flush()` stays truthy over a counted
  `UNKNOWN`. And 2026-11-04: keep every sink, and no breaking change in 1.x.

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
the destination, and today it leaves `failed=0` and no line. It is counted as an upper bound, like
every `failed`. On a FIFO queue, a replicated ClickHouse table, or a send that never reached the
destination despite raising a class that cannot say so, the count is higher than the duplicates.
It is lower only for a resend the engine does not make (*Out of Scope*), or where an exception FR-002
clears did in fact reach the destination.

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

**Every `losses()` reports it.** Each shipped sink whose `losses()` builds its own `SinkLosses`
carries `resent` from its counters:

- the HTTP family (`http.py:3106-3127`), and `SentrySink` from its HTTP backend;
- the wrappers' composition (`_deferral.py:234-235`);
- `MultiSink`, which sums its children (`multi.py:301-304`).

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
- [ ] For every engine sink `tests/test_engine_sinks_roster.py` derives, bare and under each shipped
      wrapper, `losses().resent` equals its counters' `resent_events` after a run that resent. A
      `MultiSink`'s `losses().resent` is the sum of its children's (invariants 2 and 6).
- [ ] An `HTTPSink._send` whose first attempt raises `TimeoutError` and whose second gets a `200`
      adds 1 to `log_foundry_counters.resent_events` and writes the line, and
      `SentrySink.losses().resent` reports its HTTP backend's count (invariant 2).
- [ ] After the first criterion's run, `health().events` has the same terms as the same run with
      the mark removed, and `unaccounted == 0` (invariant 2).
- [ ] The line is written only by `_diag.resent`. It names the sink by its stored class name and
      carries no value, and `tests/test_diag.py`'s guards pass over it (invariant 11).

### FR-002: Each shipped sink marks the sends that may have reached its destination, and only those

#### Description:

**The rule.** A `RETRYABLE` event is marked when its call may have reached the destination.

- A **reply** that refuses the event or the request is not marked, with one exception. A throttle,
  a `4xx`, an entry an SQS or SNS `Failed` list names, and a Kinesis or Firehose record carrying an
  `ErrorCode` are the destination's word that it did not take the event. The exception is a `5xx`,
  which says the request failed, not that nothing was stored (a gateway's `502` or `504` is an
  upstream reply it lost), so a `5xx` is marked.
- A **raise** is marked unless the sink can show the request never left. It shows that by where
  the call raised, when it can tell, and otherwise by the exception's class.

The classification lives in `_classify.may_have_landed(err) -> bool`, beside the retry predicates,
for the classes several sinks share. A sink that can tell by call site passes its own answer. Each
sink judges by the exception it sees, so an earlier attempt a client library made inside the same
call is that library's (*Out of Scope*).

| Sink | Marked | Not marked |
|---|---|---|
| HTTP family, via `_exchange` (`http.py:3262`) | any retried exception that is not a `urllib.error.URLError` (`TimeoutError` and `http.client.RemoteDisconnected` from `getresponse` among them), and a retryable status `>= 500` | a `URLError` other than an `HTTPError`, which `urllib` raises only around connecting and writing the request (`do_open`); `429` |
| `SQSSink`, `SNSSink`, `KinesisSink`, `FirehoseSink`, via `settle_keyed` and `settle_positional` (`_rounds.py:332`, `:436`) | a retried raise not listed opposite, and a `ClientError` whose HTTP status is `>= 500` | botocore `EndpointConnectionError`, `ConnectTimeoutError` and `ProxyConnectionError`, builtin `ConnectionRefusedError` and `socket.gaierror`, a `ClientError` below `500`, and every entry a reply names |
| `SocketTransport`, TCP (`_socket.py:409`, `:501-521`) | the frame whose `sendall` raised, unless `may_have_landed` clears the exception | a frame whose connection could not be opened, and a `sendall` refused with `ConnectionRefusedError`; UDP never, since a datagram whose `sendto` raised was not sent |
| `RabbitMQSink` (`rabbitmq.py:417-441`) | a publish raising `StreamLostError`, `ConnectionClosedByBroker`, `AMQPHeartbeatTimeout` or `ConnectionBlockedTimeout`, the set SPEC-093 reads as an unconfirmed publish, or an exception outside pika's `AMQPError` that `may_have_landed` does not clear | any other pika error, such as a channel or connection it could not open (`ChannelWrongStateError`, `ConnectionWrongStateError`, any other `AMQPConnectionError`) |
| `PostgresSink` (`postgres.py:446-470`) | a raise from `commit()`, or from any statement on an `autocommit` connection, whose SQLSTATE is in class `08` or absent | a raise before `commit()` in a transaction, which the server rolls back, and any SQLSTATE outside class `08` |
| `AzureEventHubsSink` (`eventhubs.py:398`) | `OperationTimeoutError`, `ConnectionLostError` and any other retried raise | `ConnectError`, "Failed to connect to the Event Hubs service" |
| `ClickHouseSink` (`clickhouse.py:352`), `RedisStreamsSink` and `RedisListSink` (`redis.py:289-302`) | every retried raise | none: `OperationalError` and redis-py's `ConnectionError` are raised both for a connection that could not be opened and for one lost after the request was written (`redis/connection.py:1391`, `:1423` at redis 8.1.0), so the class cannot say |

**Sinks that never mark**, each in a derived roster's `EXEMPT` with its reason:

- `MongoDBSink`: its `_id` refuses a second copy (FR-003).
- `FileSink` and `RotatingFileSink`: a line the file took is never written again (SPEC-082).
- `StdoutSink` and `StderrSink`: a handed line is never handed again (SPEC-084).
- `NATSSink`: its resend is §12's `Nats-Msg-Id` item (*Out of Scope*).
- If SPEC-094 is on `main` when this is built, `KafkaSink` and `GooglePubSubSink` too, with the
  reason that their only `RETRYABLE` is a hand-off the client refused before it took the message.

**The departures this makes.** `ClickHouseSink` and the two Redis sinks over-count on a refused
connection. That is the upper bound, and SPEC-092's new check (FR-004) reads it as a departure,
whose record each class docstring carries:

- `clickhouse.py`: "`OperationalError` does not say whether the insert reached the server, so every
  insert it fails is counted as one that may have landed".
- `redis.py`, in `_RedisSink`: "redis-py raises `ConnectionError` both for a connection it could not
  open and for one lost after the pipeline was written, so every pipeline it fails is counted as one
  that may have landed".

#### Acceptance Criteria:

- [ ] For each row of the table, a double raising or answering the "Marked" case marks every event
      of the call, and one raising or answering the "Not marked" case marks none. One test per row
      and case asserts the run's `resent` (invariants 2 and 7).
- [ ] An `HTTPSink` round answered `503` and then `200` counts `resent` equal to the chunk's
      events. One answered `429` and then `200` counts 0 (invariant 7).
- [ ] A `PostgresSink` transaction whose `executemany` raises `08006` counts 0. One whose `commit()`
      raises `08006`, or raises with no SQLSTATE, counts every row. One whose `commit()` raises
      `40001` counts 0 (invariant 2).
- [ ] A test derives the engine sinks from the tree. It fails for any sink that is in `EXEMPT` and
      marks in its test, and for any sink that is neither in `EXEMPT` nor marking in one of this
      FR's tests. Each `EXEMPT` entry carries its reason (invariant 2).
- [ ] With `_classify.may_have_landed` answering `True` for every exception, in an export of the
      branch head, every `refused-once` and `refused-always` row of the HTTP family (its four
      wrapper variants and `SentrySink[http]` included), the five AWS variants, the two TCP socket
      variants and `AzureEventHubsSink` reads `resent-when-knowable`: 46 rows on either base
      (invariant 2).
- [ ] With `RunContext.may_have_landed` doing nothing, in an export of the branch head, the 50
      `taken-then-raised` and `reply-lost` rows FR-004 turns green read `duplicate` again
      (invariant 2).

### FR-003: `MongoDBSink` reads a duplicate `_id` on its own resend as landed

#### Description:

`MongoDBSink` keeps a run's documents, with the `_id` pymongo stamped into each, across the run's
rounds (`mongodb.py:383-450`). So a resend after an `insert_many` that raised carries the same
`_id`, and MongoDB refuses the copy with a duplicate-key `writeErrors` entry, code `11000`. Today
`_settle_bulk` (`:452`) reads that as `REJECTED`. The document is counted lost and announced as
rejected, although the earlier send stored it. That is SPEC-092's `reply-counted-lost` rows for
MongoDB.

`_settle_bulk` reads such an entry as `ACCEPTED` when all three hold:

1. its `code` is exactly `11000`;
2. its `keyPattern` is exactly `{"_id": 1}`, or, when the entry carries no `keyPattern`, its
   `errmsg` contains ` index: _id_ `;
3. the document was carried by an earlier call of this run, and that call raised (`sends_of` above
   1, since the only earlier call that leaves a document to resend is one that raised).

Any other duplicate key is `REJECTED`, as today: another unique index, or a first send. Such an
`ACCEPTED` document is in `encoded_landed`, and `rejects` does not count it.

`MongoDBSink` still declares no `log_foundry_idempotent` (FR-005), and it never marks a resend
(FR-002): within a run, its resend cannot leave a second copy.

#### Acceptance Criteria:

- [ ] An `insert_many` that stores every document and raises `NetworkTimeout`, then a second round
      answered `11000` on `_id` for each, leaves each document `ACCEPTED`. `failed_events`,
      `losses().failed` and `losses().resent` stay 0, and no `rejected` line is written
      (invariants 2 and 7).
- [ ] A first send answered `11000` on `_id` is `REJECTED` and counted, as today (invariant 2).
- [ ] A resend answered `11000` whose `keyPattern` is `{"log_id": 1}` is `REJECTED` (invariant 2).
- [ ] A resend answered `11000` with no `keyPattern` and an `errmsg` naming `index: _id_` is
      `ACCEPTED`. One whose `errmsg` names another index is `REJECTED` (invariant 7).
- [ ] With condition 3 removed, in an export of the branch head, the first-send criterion fails
      (invariant 2).
- [ ] **Owed until a green run:** `tests/integration/test_mongo.py` runs a `MongoDBSink` whose
      collection's `insert_many` stores the documents and then raises pymongo's `NetworkTimeout`
      once, against the real MongoDB service. The collection then holds each document once,
      `losses()` reads `failed=0`, and the server's `writeErrors` entries carry `keyPattern`
      `{"_id": 1}`. It is triggered by the PR, since `integration.yml`'s path filter includes
      `src/log_foundry/sinks/**` (invariants 2 and 7).

### FR-004: The conformance corpus reads a counted resend, and its pins and departures move

#### Description:

These are changes to SPEC-092's corpus files as its build delivers them, after SPEC-093's and
SPEC-094's where those are on `main`.

- **The reconstruction.** The round wrapper in `runs()` also records each round's
  `may_have_landed` marks, read from the run's context after `deliver` returns. `resends_from`
  rebuilds, per event and per run, how many times the run offered the event again after a round
  that marked it.
- **The oracle.** `agrees` compares that reconstruction with the run's `Delivery.resent`, and a
  difference fails `partition`, as a fate count does.
- **`duplicate`.** For a per-event row it fails when the backend holds an event more times than one
  plus the resends counted for it. For an `emit`-only row it fails when the extra copies held
  outnumber the move in `losses().resent` over the row.
- **`resent-when-knowable`**, a new check after `ambiguous-when-knowable`, fails when a resend was
  counted for an event that no unknowable call carried, or, for an `emit`-only row, when the row's
  `losses().resent` moved and none of its calls was unknowable. It is what keeps the count from
  being a way out: a sink that marked every raise would pass `duplicate` and fail this.
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
- **Departures.** Two are new, each holding its rows at `resent-when-knowable` and quoting FR-002's
  phrase:
  - `clickhouse-error-unclassed` holds `ClickHouseSink` `refused-once` and `refused-always`;
  - `redis-error-unclassed` holds the same two faults of `RedisStreamsSink` and `RedisListSink`.
- **Totals.**

  | Base | Rows | Pinned | Departure rows | Must pass |
  |---|---|---|---|---|
  | `main` | 256 | 22, under 11 pins | 18, under 9 departures | 216 |
  | after SPEC-094 | 268 | 6, under 4 pins | 21, under 11 departures | 241 |

#### Acceptance Criteria:

- [ ] The corpus's literals read the totals of the base the build starts from (invariants 2 and 7).
- [ ] Against the corpus before this spec, exactly 58 rows change verdict:
      - the 50 `reply-resent` rows, from `duplicate` to green;
      - `MongoDBSink` `taken-then-raised` and `reply-lost`, from `lost-but-held` to green;
      - the six departure rows, from green to `resent-when-knowable`.
      Every other row reads as before (invariants 2 and 7).
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
      counts a resend reads `partition`, and `SentrySink[http]`'s two such rows, judged per batch,
      read `duplicate` (invariant 2).
- [ ] With `resent-when-knowable` removed from `judge`, the six departure rows read green, which
      fails each departure (invariant 2).

### FR-005: The record says what a resend costs and what a declaration of idempotence needs

#### Description:

These are the completion ritual's edits.

- **§12.**
  - The finding-5 item is struck as closed by SPEC-095. Its closure carries the audit probes'
    numbers before and after (*Evidence*), and names the residues *Out of Scope* lists.
  - A new item is added: "**An event a send may have landed is counted as lost when no later send
    lands it**". It holds the two `reply-counted-lost` rows, and the every-reply-lost case:
    `probe_aws_more.py` case 1, ten events each held 16 times, with `failed=10`, `flush()` falsy and
    now `resent=150`. **Closed by** the engine reading such an event `UNKNOWN`, at the cost that a
    destination which times out every reply no longer fails its batches.
- **Decisions in `docs/decisions/sink-delivery/`.**
  - A new decision, with fence and entry: "**A resend after a send that may have landed is counted,
    and its key is the event's `log_id`**". It records FR-001's count and line, FR-002's rule with
    its table, the key table above, and the residues.
  - *No shipped sink declares itself idempotent* is refined in place, with a dated History entry.
    It now states what a declaration needs:
    1. a destination that refuses or absorbs a second copy carrying the same key for as long as the
       engine can resend, so either no window, or a window the sink bounds every resend to;
    2. a key the sink sets per event, identical on every resend the engine makes;
    3. an `integration.yml` test against the real service showing that a resend of an event that
       landed leaves one copy, and the vendor's window quoted in the decision.

    No shipped sink meets all three in this spec. `SQSSink` FIFO fails the first, because
    `spec095_fifo_window.py` shows a resend can fall past the 5-minute interval, and the third,
    because there is no SQS service. `MongoDBSink` meets the first two once FR-003 lands, and the
    third only when FR-003's owed run is green. *Revision history* gives the choice.
  - *A client exception costs its chunk, and is provable non-delivery for it* gains a dated History
    entry: the duplicate it accepts is now counted and announced (SPEC-095).
- **`docs/invariants.md`.** Invariant 7's observable ("no event landed twice") gains its recorded
  exception: a resend after a lost reply can land an event twice, counted in `losses().resent` and
  announced, never resent beyond the 1.x total. Invariant 2's and 7's *Guarded* lines name the new
  tests.
- **README.**
  - *Never crashes the app* (`README.md:631-637`): "there is nothing downstream to duplicate" is
    replaced by the cost and the count.
  - The `health()` section (`:1350`), which says `h.sink` carries `dropped` and `failed`, names
    `resent` too.
  - The custom-sink examples (`:1100-1102`, `:1217-1218`) stay as they are, and the text beside
    them says a `SinkLosses` built by keyword needs no change.
  - The `SNSSink` row (`:889`) says a FIFO topic is not supported.
- **The release notes**: in the pending `docs/release-notes/v1.2.1.md` if it is on `main` when this
  spec completes, and under a "Release notes owed" heading in the delivery doc otherwise.
  - *Added*: `SinkLosses.resent`, `SinkCounters.resent_events` and the line.
  - *Changed*: `MongoDBSink`'s duplicate `_id` on its own resend is delivered, not counted in
    `failed`.
  - The compatibility list gains three entries: code that compares a `SinkLosses` by `repr`,
    `dataclasses.astuple` or `dataclasses.fields`; code that matches stderr; and code that relies on
    MongoDB's `failed` counting such a document.
  - Every sentence saying no constructor signature changed is rewritten, since `SinkLosses` gains a
    keyword.
- **`docs/component-inventory.md`**: no row is owed, since no module is added.

#### Acceptance Criteria:

- [ ] §12 strikes the finding-5 item, dated and naming SPEC-095, and carries the new item with its
      **Closed by**.
- [ ] The new decision file and the two dated History entries exist. The refined fence states the
      three conditions and names `SQSSink` FIFO and `MongoDBSink` against them.
- [ ] `docs/invariants.md` records the resend after a lost reply beside "no event landed twice",
      and no README sentence says a sink's resend cannot duplicate.
- [ ] The release notes name the field, the line, the MongoDB change and the three compatibility
      entries, and no sentence there says no constructor signature changed.
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
    ├── _rounds.py                # Called.reached, the AWS settle helpers mark
    ├── verdict.py, base.py       # resent_events, SinkLosses.resent
    ├── _deferral.py, multi.py    # losses() carry resent
    ├── http.py, sentry.py        # _exchange, the round, _send, losses()
    ├── _socket.py, rabbitmq.py, postgres.py, clickhouse.py, eventhubs.py, redis.py
    └── mongodb.py                # FR-003
tests/
├── test_resend_count.py          # FR-001
├── test_resend_marks.py          # FR-002, with the derived roster
├── test_sinks_mongodb_resend.py  # FR-003
├── test_conformance*.py          # FR-004, SPEC-092's files
└── integration/test_mongo.py     # FR-003's owed run
```

## Implementation Phases

### Phase 1: The count (FR-001)

- The mark, its validation, rewind and delegation; the count in `_run`; `Delivery.resent`;
  `SinkCounters.resent_events`, `SinkLosses.resent` and every `losses()` that builds one; the line;
  `_send`.

### Phase 2: The marks and MongoDB (FR-002, FR-003)

- `_classify.may_have_landed`, `Called.reached`, each family's site, the derived roster and
  `EXEMPT`, the two departure phrases; `_settle_bulk`'s duplicate-key reading and its integration
  test.

### Phase 3: The corpus and the record (FR-004, FR-005)

- The reconstruction, the oracle, `duplicate`, `resent-when-knowable`, `unannounced`, the two
  doubles, the pins, departures and totals, the gate fixtures and mutants; then the record.

## Evidence

Measured on macOS with Python 3.13.1 and no extra installed, at `97a03f45`. The prototype is
`docs/audits/2026-11-14-probes/sinks/spec095_prototype.patch`, applied with `patch -p1` to `git
archive 97a03f45 src tests docs/audits/2026-11-14-probes`. Its twin
`spec095_prototype_after_094.patch` applies to that tree after SPEC-093's and SPEC-094's prototype
patches, and differs only where those changed `rabbitmq.py` and `sentry.py`. The prototype is
SPEC-095's `src` change and its corpus change, without the build's tests, the `_send` `Deferral`,
the wrappers' and `MultiSink`'s `losses()`, or the `EXEMPT` roster.

**The rows.** `spec095_compare.py` against fresh exports:

| Base | Before | After |
|---|---|---|
| `main` | 256 rows, 170 green, 86 red | 216 green, 40 red |
| after SPEC-094 | 268 rows, 195 green, 73 red | 241 green, 27 red |

On each base, 58 rows move:

- 50 from `duplicate` to green;
- `MongoDBSink` `taken-then-raised` and `reply-lost`, from `lost-but-held` to green;
- `ClickHouseSink`, `RedisStreamsSink` and `RedisListSink` `refused-once` and `refused-always`,
  from green to `resent-when-knowable`.

The prototype's corpus run against each base's own `src` moves no row. Each comparison gave
identical output in two runs on Python 3.13.1 and one on Python 3.12.12, apart from the wall
time.

**The mutants**, `spec095_mutants.py` on each prototype tree:

| Mutant | Rows that move |
|---|---|
| `may_have_landed` always `True` | 46, green to `resent-when-knowable`: the `refused-once` and `refused-always` rows of 23 variants |
| the context drops every mark | the 50 back to `duplicate`, and the six departures to green |
| `_diag.resent` silent | 64 on `main` and 64 after SPEC-094, every row that counts a resend, each gaining `unannounced` |
| `Delivery.resent` never set | the same 64, each gaining `partition`, except `SentrySink[http]`'s two rows, which read `duplicate` because they are judged per batch from `losses()` |

**The audit's probes, before and after**, run on the two `main` exports:

| Probe | Before | After |
|---|---|---|
| `probe_sqs.py` case D (standard queue, one reply lost) | 35 messages for 25 events, `failed=0`, no line | the same 35, and `resent 10` with one line, which is the 10 extra copies exactly |
| `probe_sqs.py` case D' (FIFO, the fake de-duplicating) | 25 held | 25 held, and `resent 10`: an upper bound |
| `probe_aws_more.py` case 1 (every reply lost) | each of 10 events held 16 times, `failed=10`, `flush()` falsy | the same, with `SinkLosses(dropped=0, failed=10, resent=150)` and one resend line; 150 is the extra copies exactly |
| `probe_misc.py` cases 1 and 6 (PostgreSQL commit reply lost; HTTP connection closed with no status line) | 50 rows or bodies for 25, no line | the same 50, and one resend line each |

**The cost.** `spec095_cost.py`, three runs per tree, one-minute load 3.4. These are medians of
`_engine._run` over a `deliver` that accepts everything (clean), and over one that answers
`RETRYABLE` for every event once and then accepts (one lost reply). The lost-reply case after this
spec includes the resend line, written to `/dev/null`.

| Events | Clean, before | Clean, after | One lost reply, before | One lost reply, after |
|---|---|---|---|---|
| 1 | 8.5 to 8.6 µs | 8.9 to 9.1 µs | 11.4 µs | 17.4 to 17.8 µs |
| 100 | 44.7 to 45.6 µs | 47.0 to 48.0 µs | 61.8 to 63.1 µs | 79.5 to 80.8 µs |
| 1000 | 361.6 to 364.4 µs | 368.0 to 374.3 µs | 500.3 to 503.2 µs | 608.6 to 621.1 µs |

A clean run costs up to 3% more. A run that lost a reply, already the slow path, costs about 6 µs,
18 µs or 115 µs more. No request carries another byte.

**The FIFO interval.** `spec095_fifo_window.py` computes when each round can start, with the
engine's own wait functions:

- At `SQSSink`'s defaults (16 rounds) with calls failing at once, the last round starts 2.87 s after
  the first.
- With each call ending after three botocore attempts at the default 60-second read timeout
  (`botocore/endpoint.py` `DEFAULT_TIMEOUT = 60` at botocore 1.43.127), round 3 starts more than
  300 s after round 1.
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
- *Firehose `PutRecordBatch`*: "We recommend that you handle any duplicates at the destination", and
  "Re-invoking the Put API operations (for example, PutRecord and PutRecordBatch) can result in data
  duplicates."
- *boto3, retries*: "Standard mode is the default mode used by any Boto3 client you create", with "A
  default value of 3 for maximum attempts (including the initial request)", retrying
  `ConnectionError` and `HTTPClientError`. Botocore's `ReadTimeoutError` subclasses
  `HTTPClientError` (`botocore/exceptions.py:140` at 1.43.127).
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

- Authored on 2026-11-16 against `97a03f45`. Four choices are product calls and were taken on the
  side recommended, for Andrew to confirm or reverse:
  1. **Where the count lives.** It is in `SinkLosses.resent`, appended with a default of 0, so
     `health().sink.resent` reports it. It could instead stay on `SinkCounters` alone, which is
     invisible to `health()`, or go into the event ledger, where a duplicate is not an event.
  2. **An event that a send may have landed, and that no later send landed, keeps its last round's
     fate.** So the run still fails, `flush()` stays falsy and the event is counted lost. Reading it
     `UNKNOWN` instead would turn the two `reply-counted-lost` rows green and match decision A and
     SPEC-094's "Truthy, counted". It would also make a destination that times out every reply
     settle each batch as "may have landed", so `failed_batches` would stop moving in that outage.
  3. **No `_id` for `ElasticsearchSink` and `OpenSearchSink`.** Sending `log_id` as `_id` with the
     `index` action would make a resend overwrite rather than duplicate. It would also change
     every document's `_id` from one Elasticsearch generates to the event's `log_id`, add about 44
     bytes to each action line, and cost the existing-id check Elastic documents as "a costly
     operation". The count already turns these rows green.
  4. **No sink declares `log_foundry_idempotent`.** `MongoDBSink` would meet the proof once FR-003's
     integration run is green. Declaring it would reverse part of a settled fence, for a gain only
     on a `writeErrors` that cannot be paired.
