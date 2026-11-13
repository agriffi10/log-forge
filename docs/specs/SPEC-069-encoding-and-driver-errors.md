---
type: Spec
id: SPEC-069
title: "Encoding and Driver Errors — One Bad Event Costs Only Itself in Every Sink"
description: "Gives every record-building sink one encode step before its bulk call, so an event it cannot encode costs only itself and one its destination refuses is repaired and delivered, and counts, announces and types the driver errors the SQLite and file sinks let escape."
spec_status: Completed
depends_on: [SPEC-026, SPEC-058, SPEC-059, SPEC-061, SPEC-066]
arc: stability-redesign
arc_order: 1
---

## Overview

A batch is the worker's unit of retry, so one event a sink cannot encode costs every event beside
it, and in eight sink classes it costs more than that: the sink has already handed the events
ahead of the bad one to its client when it raises, and the worker's retry delivers them again.
SPEC-058 fixed this for the HTTP family and for caller code in front of a sink, and left the rest
standing. This spec is the first of the arc that standardises the sinks (`stability-redesign`,
additive, release 1.2.0). It settles one question every sink answers differently today: *what
does an event the destination cannot take cost?* The answer has two halves. An event the sink
cannot encode at all costs **only itself**: it is counted, announced, and the rest of the batch
goes on. An event the sink *can* encode but whose destination refuses one value in it is
**repaired and delivered**, marked the way assembly marks a substitution. The spec also closes two
driver-error escapes (SQLite and the file sinks) and one stale value (`SyslogSink`'s process id in
a forked child).

Invariant 8 means the pipeline cannot hand a sink an event that is not valid JSON, so the bare
`bytes` event used below to measure the problem is reachable only through a third-party wrapper or
a direct `emit`. Two destinations refuse what assembly allows, and **those are reachable from
`@trace`, `info()` and `configure()`**. PostgreSQL's JSONB and its text columns refuse U+0000.
MongoDB's BSON refuses an integer beyond 64 bits and a NUL in a key. Sent through the public API,
assembly delivers `2**70`, `-2**64`, `"a\u0000b"` and a key `"k\u0000ey"` into `fields`
unchanged, and `configure(service="s\x00x")` is accepted. That stamp lands in a PostgreSQL text
column on every event. Each refusal costs the whole batch today, so it is FR-003's subject.

All measurements are at `fda134a`, with `src/` from this tree. The population was found by three
instruments:

- a behavioural probe over every shipped record-building class, with a recording stand-in for its
  client, emitting `[ok, bad, ok]` where `bad` holds a `bytes` value;
- an AST scan of every `json.dumps` call under `src/log_foundry/` with its enclosing class and
  function, and whether an enclosing `try` handles `Exception`;
- for the two destination refusals, the real `MongoDBSink` against MongoDB 7.0.40 (pymongo 4.19.0)
  and the real `PostgresSink` against PostgreSQL 17.11 (psycopg 3.3.8).

The probes found four outcomes:

- **Sixteen classes, fifteen `emit` implementations, raise `TypeError` with nothing counted**
  (`losses()` unchanged, or no `losses()` at all). They are `StdoutSink`, `StderrSink` (which
  inherits `StdoutSink.emit`), `FileSink`, `RotatingFileSink`, `SQLiteSink`, `ClickHouseSink`,
  `RabbitMQSink`, `KafkaSink`, `KinesisSink`, `FirehoseSink`, `SNSSink`, `SQSSink`, `MongoDBSink`,
  `AzureEventHubsSink`, `SyslogSink` and `LogstashSink` in socket mode. The plan this arc follows
  counted twelve at `ce2e1fe` and gave no list, so the difference cannot be attributed.
- **Eight of those classes had delivered earlier events before they raised**, so the retry
  duplicates them. They are `StdoutSink`, `StderrSink`, `FileSink`, `RotatingFileSink`,
  `ClickHouseSink` (at a chunk boundary), `RabbitMQSink`, `KafkaSink` and `AzureEventHubsSink` (when
  its `EventDataBatch` fills before the bad event). The other eight raise before anything is handed
  over, so the bad event costs them only a pointless retry.
- **`RedisStreamsSink`, `RedisListSink` and `PostgresSink` retry the encode.** It sits inside their
  retry loop, and once the loop is spent they count the whole batch `failed` and raise: `failed=3`
  for a batch of three with one event at fault.
- **Some sinks already isolate the event.** `GooglePubSubSink` counts it in `rejected` (reported as
  `dropped`) and returns normally, and `HTTPSink` and its subclasses do the same (SPEC-058 FR-005).
  `NATSSink` and `SentrySink` isolate it per event too, but count it `failed`.

The two destination refusals, as measured:

- **MongoDB.** A batch of three with one `2**64` in `fields` went through
  `MongoDBSink(max_retries=1)` and raised `SinkDeliveryError` after two attempts, with `losses()`
  of `failed=3` and no document inserted. `bson.encode` rejects `2**63`, `-2**63-1`, `2**64` and a
  NUL in a key name, raising `OverflowError` or `InvalidDocument`. It accepts `2**63-1`, `-2**63`,
  a NUL in a value, and keys holding `$` or `.`.
- **PostgreSQL.** One U+0000 in `fields` cost **the whole batch**, and it was retried
  (`UntranslatableCharacter`, SQLSTATE `22P05`, two attempts). Every chunk's `executemany` runs in
  one transaction that commits once, so one bad row rolls back all of them. A U+0000 in a text
  column fails client-side with `DataError`, and a clean batch is unaffected. PostgreSQL documents
  that `jsonb` cannot hold `\u0000` because the `text` type cannot (§8.14, JSON types).

**Three things this spec resolves rather than leaves implied.**

*What a batch with nothing encodable does.* The plan says it raises. Read as *count it `dropped`,
then raise*, that breaks invariant 2. A `dropped` count is made only by an `emit` that returned,
and one made inside a raising `emit` is made again on every worker attempt and again by the
catcher's abandon line. Read as *raise and count nothing*, it keeps invariant 2 just as settling
does. Its cost is elsewhere: it reverses the recorded exception on invariant 7 (an
all-unrenderable HTTP batch settles, counted, "because a retry cannot help", SPEC-058 FR-005), and
it spends the worker's retries on an input no retry can change. **Andrew chose on 2026-11-04 that
1.2.0 settles such a batch, each event counted `dropped` and announced, with `emit` returning, over
the plan's wording** (FR-002). This is a product choice the invariants allow; neither of them
forces it.

*What a destination's own refusal does.* **Andrew chose on 2026-11-04 that the sink repairs only
what its destination refuses and delivers the event** (FR-003). U+0000 becomes U+FFFD, and an
integer out of range becomes assembly's too-large-number placeholder. Both are marked with
`truncated` as assembly marks the same substitutions, so no event is lost to these refusals.
Invariant 8's promise is unchanged: assembly still makes every event valid JSON. The repair is a
second, destination-specific step that assembly cannot take, because only the sink knows its
destination.

*Why `NATSSink` and `SentrySink` stay outside the one rule.* The principle is that **an outcome
that has not changed keeps its counter**. Its source is the additive-release decision in this
arc's brief (Andrew, 2026-11-04: "No count is reclassified in a minor release") and the plan's
Change 2 item 6 ("Outcomes land in the counters they land in today"). `NATSSink` already isolates
an unencodable event, counts it `failed` and never retries it (`_publish_all`, `nats.py:1306-1320`
at `ebf9d680`).
`SentrySink` does the same per envelope (`_capture`). For them nothing about the outcome changes,
so the counter stays, and `tests/test_sinks_nats.py` pins `SinkLosses(dropped=0, failed=1)` for it.
The sinks this spec *does* move are different: their outcome changes. Today Postgres, both Redis
sinks and Mongo lose and retry the whole batch, counting it `failed`. Under this spec they deliver
the rest, so the count follows the new outcome. Every such move is listed in the release-notes
entry.

This spec is a caller-visible behaviour change in a minor release, so it carries a release-notes
entry. Three changes are visible to a caller: a sink stops raising on an event it cannot encode,
two refusals are repaired instead of losing the batch, and three sinks raise a new subclass. The
only overlap with SPEC-061 is that SPEC-061 rewrites `NATSSink.emit`. This spec does not touch
`NATSSink`, so SPEC-061 leaves nothing to rebase. SPEC-061 has landed, in 1.1.0, and is in
`depends_on`, because FR-001's and FR-002's `NATSSink` criteria pin the sink as SPEC-061 left it.

## Scope

### In Scope

- One private helper, `src/log_foundry/sinks/_encode.py`, that runs one event through a sink's
  per-event encode function under a guard and records a failure on the call's `Deferral` (FR-001).
- The sixteen classes above, plus `RedisStreamsSink`, `RedisListSink`, `PostgresSink` and
  `GooglePubSubSink`, taking that helper so each encodes the whole batch before its first bulk call
  (FR-001).
- A batch with nothing encodable settling, counted `dropped` (FR-002).
- Repairing and delivering an event with PostgreSQL's U+0000 or MongoDB's BSON refusals (FR-003).
- `SQLiteSink`, `FileSink` and `RotatingFileSink` counting and announcing a driver error, and
  raising it as a private class that is both `SinkDeliveryError` and the driver's own type
  (FR-004).
- `SyslogSink` reading its process id per message (FR-005).
- The recorded text each of these makes false, and one release-notes entry.
- Five FRs, inside the 3–6 aim.

### Out of Scope

- **`NATSSink` and `SentrySink`.** They are the declared exceptions above: they stay per-event and
  stay `failed`, and their all-failed batch raises exactly as today. No file of either is edited.
- **The HTTP family** (`HTTPSink` and every subclass, `LogstashSink`'s HTTP mode included). It is the
  reference this spec generalises. `_items`, `_unrenderable` and their tests stay as they are.
- **Which exceptions a sink retries.** Postgres, Redis and ClickHouse keep their retry loops and the
  predicates they have; this spec only moves the encode out of them. Classifying exceptions is the
  next spec in the arc.
- **The `Verdict`/`deliver` contract, the engine and the counter object.** The `_unencodable` and
  `_failed` counters this spec adds are private attributes of each sink, and the engine specs fold
  them into whatever they build.
- **A stream or file that fails partway through a batch.** `StdoutSink`'s stream is
  caller-supplied and may raise anything. A `FileSink` or `RotatingFileSink` write can also fail
  after earlier lines of the batch were written. Either way the earlier lines stay, and the
  worker's retry writes them again. This spec closes the *encode* route to that duplicate, not the
  I/O route. FR-004 counts and types a file driver error; it does not make a partial write atomic.
  *Settled 2026-11-13: an accepted edge for 1.2.0, recorded in the driver-error decision and as an
  exception to invariant 7 (delivery doc, Follow-ups).*
- **SQLite column values that cannot be bound.** An integer past 64 bits raises the builtin
  `OverflowError` and a `dict` raises `sqlite3.ProgrammingError` when `SQLiteSink` binds its
  projected columns, and either still costs the batch. Only a direct `emit` can hand the sink one,
  since assembly cannot produce them in those columns, and neither is measured or handled here.
  *Resolved 2026-11-13: a `TransformSink` can produce them too, and each is now bound as its JSON
  text (delivery doc, Follow-ups).*
- **Oversize.** `dropped_oversized` and every size decision stay as they are. An event that is both
  unencodable and oversized counts once, as unencodable, because the encode step runs first.
- **Refusals no one has measured.** Only the PostgreSQL and MongoDB refusals above are repaired, on
  FR-003's admission rule. That leaves one known route. A lone surrogate reaching `PostgresSink`
  through a direct `emit` becomes an escape JSONB may refuse. Assembly and `TransformSink` both
  replace lone surrogates, so it is outside the pipeline, and it is not measured here.
  *Resolved 2026-11-13: measured costing the batch, and now dropped alone (delivery doc,
  Follow-ups).*
- **New public names beyond these.** Every class that gains a counter gets a public `losses()`
  method: `StdoutSink`, `StderrSink`, `FileSink`, `RotatingFileSink` and `SQLiteSink`. That grows
  the surface, which invariant 10 allows, and freezes the method for 1.x. Everything else this spec
  adds is private: the two helper modules, `Unencodable`, the driver-error class, `_unencodable`
  and `_failed`. Peers such as `PostgresSink` expose `failed` publicly; the new sinks do not,
  because every new public name is frozen for 1.x and `losses()` already reports the count. There
  is no new `configure()` key or constructor argument.
- **`KafkaSink`'s `losses()` model.** It keeps `losses()` and gets no `deliver`. Its `rejected`
  counter takes the unencodable count, and nothing else about it changes.

### Twin paths (invariant 6)

- **Worker path and orphan path.** Both call the sink's `emit`, the orphan path with a one-event
  batch.
  - An all-unencodable batch settles inside the sink on both paths: the event is in `dropped` and
    `orphan_lost` stays 0, the way an all-oversized event already behaves (FR-002).
  - A repaired event is delivered on both paths (FR-003).
  - A driver error raises on both, and the catcher counts it (FR-004).
- **`MultiSink` with one of these as a child.** A child that settles returns normally, so
  `MultiSink` sums its `dropped` and adds nothing. A child that raises is counted by `MultiSink`, as
  SPEC-058 FR-007 says. A repair never mutates the shared event, so a sibling sees the original.
- **Two classes, one rule.** These pairs are twins:
  - `StdoutSink` and `StderrSink`;
  - `FileSink` and `RotatingFileSink`;
  - the two Redis sinks;
  - `LogstashSink`'s socket and HTTP modes;
  - the HTTP family's `_items` and the new helper.

  Each criterion that names one names the other, and a roster test derives the population, so a
  new sink cannot join one side only (FR-001).
- **`emit` and `flush`.** No sink's `flush` encodes or decides a drop, so none changes.

---

## Functional Requirements

### FR-001: Every record-building sink encodes the whole batch before its first bulk call

#### Description:

Each class on the rule builds one wire form per event (a line, a body, a row, a frame, a
document) in a single encode step at the top of `emit`. The step runs **outside any lock and
outside any retry loop**, and the sink makes its first bulk call, write, produce or insert only
when every event has been through it. The step is one helper, written once, the way SPEC-058's
`Deferral` is:

- `encode_events(batch, encode, pending, ...)` calls `encode(event)` for each event under a guard
  that catches `Exception` and never `BaseException`. It returns the encodable events with their
  wire forms, in order. For the failures it records one count and one announcement on `pending`,
  the call's `Deferral`. A failure is therefore **counted and announced only when the `emit` that
  decided it returns** (invariant 2, the SPEC-058/059 rule), and a retried batch decides it afresh.
- The announcement is one `_diag.lost` line per call. It names the sink's class and the **type**
  of the first failure, never its message or the event (invariant 11).
- The count goes to `_unencodable`, a private attribute folded into `losses().dropped`. Only the
  `Deferral` commit writes it, which `tests/test_deferral.py` derives and enforces. Three sinks
  count elsewhere:
  - `KafkaSink` and `GooglePubSubSink` count into `rejected`, the dropped counter they already have
    (Pub/Sub already counts an encode failure there).
  - On the socket transport, the count lives on `SocketTransport`, which owns the ledger that
    `SyslogSink` and `LogstashSink` enter.

**Each sink's per-event part is pulled out into an `encode` function.** Today the work is mixed
in with batch handling or I/O. The `encode` function takes one event and returns its wire form (or
a small tuple of wire form and size, key or ids):

- `_records` (Kinesis, Firehose), `_bodies` (SNS), `_chunks` (SQS) and `_documents` (Mongo) take
  the whole batch plus `pending`, and serialise and size each event inside their loop. The
  serialisation moves into `encode`; the loop keeps sizing, chunking and the oversize drop over the
  encoded forms.
- Redis's `_stage(pipe, event)` both serialises and calls `xadd`/`rpush`. It splits into an
  `encode` that serialises and a stage that only stages the encoded value.
- `_row` (Postgres, ClickHouse) and `_frame` (Syslog) are already per-event and become `encode` as
  they are.
- `KafkaSink`, `RabbitMQSink`, `AzureEventHubsSink`, the file and stdout sinks and `SQLiteSink`
  encode inline in their `emit` loop. That code moves into an `encode` function, and the loop
  keeps only the I/O. Kafka's key goes into its `encode`, since `_key`'s `.encode("utf-8")` can
  also raise.

The cost is that the encoded form of a whole batch is held at once. The AWS sinks and MongoDB
already do that, and the exit drain's single large batch is the largest case.

**A `losses()` for the sinks that had none.** `StdoutSink`, `StderrSink`, `FileSink`,
`RotatingFileSink` and `SQLiteSink` have no `losses()` and need one to report a count. It returns
`None` until the sink has something to report, and `SinkLosses` from then on: the rule
`wrapped_losses` uses. A clean default-configured process therefore still reads `health().sink is
None`. Each of these sinks also gains a `_counter_lock` of its own for the `Deferral` commit.

`SQSSink._chunks` keeps SPEC-058 FR-004's rule that the first exception is re-raised when every
event's id callable failed. It gains one addition: an event dropped as unencodable counts as
"something deliberately dropped". A batch of one id failure and one unencodable event therefore
returns, counting both.

#### Acceptance Criteria:

- [ ] Over each class on the rule, derived from the tree and not hand-listed, a batch of an
      encodable event, one holding `bytes` and another encodable event delivers exactly the two
      encodable events, each once; `emit` returns; `losses().dropped` is 1 (`rejected` is 1 for
      Kafka and Pub/Sub) and `failed` is 0.
- [ ] On the eight classes that used to deliver earlier events before raising, nothing is handed
      to the client, stream, producer or socket until every event is encoded. With the unencodable
      event first, in the middle and last, each position delivers the same two events once.
- [ ] Retry probe: with a client whose first bulk call raises and second succeeds, nothing is
      counted and no line is written after the first attempt; after the second, the unencodable
      event is counted exactly once and one line is written. With a client that fails every
      attempt, the count stays 0 and the worker's abandon line is the batch's only count.
- [ ] Postgres, both Redis sinks and ClickHouse encode each event once per `emit`, not once per
      attempt: with three attempts, the encode function is called `len(batch)` times.
- [ ] The line names the sink class and the first failure's exception type; a sentinel in the
      exception message or in the event appears nowhere on stderr, and one line is written per
      call however many events failed (invariant 11).
- [ ] A `KeyboardInterrupt` raised by an encode function propagates out of `emit` with nothing
      counted and no line written.
- [ ] A clean batch writes the same bytes as before, asserted per sink against a literal wire form
      (line, body, row, frame or document), and writes nothing to stderr.
- [ ] `losses()` on `StdoutSink`, `StderrSink`, `FileSink`, `RotatingFileSink` and `SQLiteSink` is
      `None` after only clean batches and `SinkLosses(dropped=1, failed=0)` after the batch above;
      `health().sink` of a default-configured process after clean events is `None`.
- [ ] After a fork repair, a child can call `emit` and `losses()` on `StdoutSink` and `StderrSink`
      while a parent thread holds the new counter lock (invariant 12).
- [ ] `SyslogSink` and socket-mode `LogstashSink` report the count through the transport:
      `losses().dropped` includes it, and `dropped_oversized` still means oversize only.
- [ ] `SQSSink` FIFO: given one event whose id callable raises and one that is unencodable, and
      nothing else, `emit` returns, the client is never called and `losses().dropped == 2`.
- [ ] `NATSSink` and `SentrySink` are unchanged: one unencodable event among several is counted in
      `failed`, `dropped` stays 0, and `tests/test_sinks_nats.py`'s isolation test passes untouched.
- [ ] Through `MultiSink(<a sink on the rule>, MemorySink())`, a batch with one unencodable event
      leaves `losses().dropped == 1` and `failed == 0`: one term, once.
- [ ] A roster test checks every class under `src/log_foundry/` whose `emit` reaches `json.dumps`,
      directly or through a method it calls.
      - It refuses any class that is on none of the helper, `HTTPSink._items` (with each subclass's
        `_render`, and `_otel._dumps` beneath `OTLPSink`'s), `NATSSink`, `SentrySink` and
        `TransformSink`. `TransformSink`'s `_check` is invariant 8's check of a transform's output
        (SPEC-058 FR-001), not an encode for delivery.
      - It pins a floor of the population it must find, and a fixture corpus asserts each check's
        failure text and its silent cases.
      - `DEFERRING_SITES`, `IMMEDIATE` and `DROPPED_FLOOR` in `tests/test_deferral.py` gain the
        converted sites and `_unencodable`.
- [ ] Serves invariants 2, 6, 7, 8 and 11; invariant 6 on the worker and orphan paths and on every
      twin named in *Twin paths*.

### FR-002: A batch with nothing encodable settles, counted

#### Description:

When every event of a call is unencodable, the sink makes **no bulk call**: no transaction, no
produce, no request and no write. It returns normally, and the count and line land through the
same commit as any other drop. This is Andrew's choice of 2026-11-04 (see the Overview). It is
SPEC-058 FR-005's rule for an all-unrenderable HTTP batch, and the rule the sinks already apply to
an all-oversized batch. Re-encoding identical input in the sink's own code fails identically, so a
retry cannot help. It is deliberately not SPEC-058 FR-002's re-raise, which exists because a
caller's callable can fail differently on the next attempt.

The settle keeps the ledger whole:

- A `dropped` count is made only by an `emit` that returned, so invariant 2 holds as stated.
- If the encodable remainder of a call then fails (the client refuses everything), the call
  raises and **counts nothing**, the unencodable events included. Whoever catches it owns the
  batch, and the call that finally returns counts the unencodable events once.
- A closed sink still raises as it does today and counts nothing.
- After an all-unencodable batch, `flush()` is truthy, `failed_batches` stays 0 and the loss is in
  `health().sink.dropped`. That is exactly what an all-oversized HTTP batch does today.

An event FR-003 repairs is encodable, so it never makes a batch "nothing encodable".

#### Acceptance Criteria:

- [ ] Over each class on the rule, a batch of nothing but unencodable events returns normally, the
      client, stream, producer, socket or connection records no call, `losses().dropped` equals the
      batch size, `failed` is 0, and exactly one line is written.
- [ ] Through the worker, for one sink of each shape (a bulk sink, Kafka, Pub/Sub, `StdoutSink`):
      `flush()` is truthy, `failed_batches == 0` and `health().sink.dropped` equals the batch size.
- [ ] Orphan path: a single unencodable event that reaches the sink through `info()` outside a
      span settles inside the sink, with `losses().dropped == 1` and `orphan_lost == 0`, one term.
      The event is made unencodable by a third-party wrapper sink that adds a `bytes` value after
      assembly.
- [ ] One unencodable event beside encodable events whose bulk call fails on every attempt: `emit`
      raises, this call counts nothing, and when a retry succeeds the unencodable event is counted
      once.
- [ ] Under `MultiSink(<a sink on the rule>, MemorySink())`, an all-unencodable batch yields
      `losses().dropped == N` and `failed == 0`, so no event is in two terms.
- [ ] An all-unencodable batch on a closed sink raises as a closed sink does and counts nothing.
- [ ] `NATSSink` and `SentrySink` raise exactly as before (`SinkDeliveryError`, `failed` counted)
      for a batch of nothing but unencodable events.
- [ ] `tests/test_sinks_http_render.py` and the invariant 7 tests in `tests/test_sink_losses.py`
      pass unchanged: a destination that fails every call still raises with a counter moved.
- [ ] Serves invariants 2 and 7, and invariant 6 on the worker and orphan paths.

### FR-003: A destination's own refusal is repaired, marked and delivered

#### Description:

**Andrew's decision of 2026-11-04: the sink repairs only what its destination refuses, and
delivers the event.** This is SPEC-058's principle (a bad event costs only itself), carried to the
point where nothing is lost. Assembly makes every event valid JSON (invariant 8); a destination
can still refuse one value in it, and only the sink knows which destination it writes to. The
repair is that second, destination-specific step. It runs inside the sink's `encode` function on a
**copy** of the event and never mutates the batch: a `MultiSink` sibling and the worker's retry
both hand the same objects on.

A refusal is repaired when all three of these hold:

- the pipeline, `configure()` or a post-assembly path can produce it;
- a real driver or service was measured refusing the whole bulk call for it;
- the sink can decide it from that one event alone.

Two destinations meet that bar.

- **PostgreSQL, U+0000.** JSONB refuses the escape `\u0000`, and psycopg refuses U+0000 in a text
  column. The repair replaces **every U+0000 in every string value and every key, at every depth,
  with U+FFFD** before `json.dumps` runs. The six text columns are top-level values of the same
  event, so the same repair covers them. This includes the `service`, `version` and `env` stamps,
  which bypass assembly.
  - *Why repair the strings rather than the JSON text.* A real NUL in an event is U+0000 in a
    Python `str` before encoding. Replacing it there gives the JSON and the columns one
    representation, with no escape-parsing rule to get wrong. Setting `truncated` means building a
    new event anyway.
  - *The detector.* The JSON text the step already builds is matched against
    `(?<!\\)(?:\\\\)*\\u0000`: a `u0000` whose preceding run of backslashes has odd length, so the
    last backslash is the escape's own. The walk runs only when this matches, so a clean event
    costs one regex search over its text.
  - *Accuracy.* Measured against PostgreSQL 17.11, the pattern agreed with the server on seven
    cases: a NUL in a value, in a key and nested; the six characters `\u0000`, which JSONB accepts
    as text; those six with an extra backslash before them; a real NUL after a backslash; and a
    clean event. A plain substring test would also match the six characters; the repair would still
    leave them alone, because the walk finds no real U+0000 in them, so the pattern only keeps the
    walk off them and off clean text.
- **MongoDB, BSON.** The detector is the driver's own `bson.encode`, run on the event. It took
  about 0.98 µs per typical event, against 1.92 µs for `json.dumps` (CPython 3.13.1, `timeit`, at
  `fda134a`). When it refuses the event, the repair walks it:
  - an integer outside `[-2**63, 2**63-1]` becomes assembly's too-large-number placeholder,
    `<int: ~N digits>`, with `N` computed by the same expression;
  - a NUL in a key becomes U+FFFD.

  The walk then runs `bson.encode` again.
  - *Placeholder text.* The expression is factored out of `sanitize._int_text` (SPEC-066), which
    already renders `<int: ~N digits>` for a refusal message, into one private function in
    `sanitize.py`, `int_placeholder`. `_int_text`'s placeholder branch and the sink both call it,
    so `_int_text`'s output is unchanged. The other copies keep their inline expression:
    `_Coercer`'s two (`_bounded_int` and `integer`, inlined because they run once per integer) and
    `_otel._int_digits` (a leaf that imports nothing from the package). A test holds all three
    sites equal to `int_placeholder`, so the text cannot drift. The sites agree on the text but
    not on when they render it: their ceilings differ near 4300 digits and under
    `sys.set_int_max_str_digits(0)`. So every input is a positive or negative integer past every
    site's ceiling at the default limit and the default `max_value_bytes` (well beyond 4300
    rendered digits, sign included), where each site renders the placeholder, and the test straddles no
    threshold.
  - *Values left alone.* A NUL in a *value* is not touched, because BSON accepts it.
  - *Importing `bson`.* It is imported lazily on the first `emit` that reaches the check, never at
    construction, and read by name. A `bson` that is missing, or has no `encode`, disables the
    check rather than raising. With an injected client double and no driver installed, there is
    therefore no check, since such a client is not a BSON encoder either. SPEC-071's
    `import_extra`, which lands later in this arc, takes this import over with every other
    optional import. The rule that an injected client never forces an import at construction
    holds through that change.
  - *Oversize.* The existing `json.dumps` size measure stays, for the 16 MB drop.

**Marking follows invariant 8 exactly.** `truncated: True` is set on the event when the repair
makes either substitution:

- U+0000 → U+FFFD, the same mark assembly sets when it replaces a lone surrogate with U+FFFD;
- an integer → `<int: ~N digits>`, the same mark assembly sets for its too-large-number
  placeholder.

An event the repair did not change gets no `truncated` key. Two keys that repair to the same text
are resolved as assembly resolves two hostile keys on one placeholder: the later wins, and the
event is marked. A repair is not loss, so nothing is counted and nothing is announced, as with
assembly's coercion (invariant 2).

**What is still dropped.** An event that still fails after the repair is unencodable under FR-001:
dropped, counted and announced. Two kinds fall here: an event `json.dumps` cannot encode at all,
such as `bytes` from a direct `emit`, and one `bson.encode` still refuses for a reason the repair
does not cover. No pipeline-reachable refusal measured here falls in that class.

#### Acceptance Criteria:

- [ ] `PostgresSink`: a batch of an encodable event, one with U+0000 in a `fields` string and
      another encodable event inserts all three in one transaction, at one attempt. The repaired
      string holds U+FFFD where the NUL was, that event carries `truncated: True`, and the other
      two are byte-identical to the input with no `truncated` key. Nothing is counted and stderr
      is silent.
- [ ] The same holds for U+0000 in a key, nested in a list, and in each of the six text columns.
- [ ] With `configure(service="s\x00x")` and a `PostgresSink`, every event of a traced call is
      inserted with `service == "s�x"` and `truncated: True`; `failed_batches == 0` and
      `health().sink` reports `dropped=0, failed=0`.
- [ ] Three events are inserted unchanged, with no `truncated` key: one whose text holds the six
      characters `\u0000`, one with a backslash before them, and one with U+0001.
- [ ] `MongoDBSink`: `2**63`, `-2**63-1` and `2**64` in `fields` are each inserted as
      `<int: ~N digits>`, equal to what the shared function returns, with `truncated: True`.
      `2**63-1` and `-2**63` are inserted unchanged.
- [ ] `MongoDBSink`: a NUL in a key is inserted as U+FFFD with `truncated: True`, and a NUL in a
      value is inserted unchanged with no mark. Two keys repairing to one text leave the later
      value, marked.
- [ ] The repair never mutates the batch. Under `MultiSink(PostgresSink(...), MemorySink())` the
      sibling receives the original U+0000, and each input event is deep-equal to a copy taken
      before `emit`.
- [ ] `MongoDBSink` with a stand-in `bson` that refuses every event, before and after repair, drops
      each event under FR-001, counted once. With no `bson` importable, or one with no `encode`,
      every event is inserted and nothing is imported at construction when a client is injected.
- [ ] An event that is both BSON-refused after repair and over 16 MB is counted once, in
      `_unencodable`, and `dropped_oversized` does not move.
- [ ] Orphan path: a single event with U+0000 sent through `info()` outside a span to
      `PostgresSink` is inserted repaired, with `orphan_lost == 0` and nothing counted.
- [ ] Against the real services: MongoDB stores the repaired events above, one document each, and
      PostgreSQL stores three rows for the three-event batch. This is owed: it settles on the
      Integration job's run against the compose services, not before the push.
- [ ] Serves invariants 8, 2 and 7: the repair is marked as invariant 8 marks the same
      substitutions, and no repaired event enters a loss term.

### FR-004: SQLite and file driver errors are counted, announced and typed as both

#### Description:

`SQLiteSink.emit` lets `sqlite3.Error` escape. `FileSink.emit` and `RotatingFileSink.emit` let
`OSError` escape: `OSError(28)` from a full disk was measured. None of the three has a `losses()`
to say so, and none writes a line; only the worker's abandon line, after its retries, says
anything. Each now does what every counting sink does. When its I/O section raises `sqlite3.Error`
or `OSError`, the sink:

1. adds the batch size to a private `_failed` immediately (the recorded upper bound of
   invariant 2), under the sink's own `_counter_lock`;
2. writes one `_diag.lost` line naming the class, the exception type and, for `OSError`,
   `errno=N`;
3. raises.

What it raises keeps every existing `except` clause matching. One private factory in
`src/log_foundry/sinks/_driver_error.py` builds a subclass of **both** `SinkDeliveryError` and the
original exception's own class (`sqlite3.OperationalError`, `PermissionError`, `OSError`), and
caches it per original class. The factory then:

- instantiates it from the original's `args`;
- copies only the attributes the original actually set and that `args` does not carry:
  `filename` and `filename2` when they are not `None`, and the `sqlite_errorcode` and
  `sqlite_errorname` the driver stores on the instance. Copying unset attributes corrupts
  `str()`: `filename2=None` gives `... 'f' -> None`, and a plain `OSError("plain")` becomes
  `[Errno None] None: None -> None`. So `str()` and `args` equal the original's;
- keeps the original's `__name__`, so the worker's lines read as they did;
- chains the original as `__cause__`;
- defines `__reduce__` to rebuild **the original class** from its `args` and copied attributes,
  so a pickled instance unpickles as the driver's own exception, without the `SinkDeliveryError`
  base. Without it every instance fails to pickle, since the class is built at run time.

If the factory cannot build the class, the sink raises the original exception unchanged, counted
and announced as above, because the library never fails the caller on its own fault (invariant 1).

**The class is not public surface.** A frozen name is inherited and kept for all of 1.x (the
decision *A protocol that is exported is a protocol that will be inherited*), and nothing needs
the name: `isinstance(exc, SinkDeliveryError)` and `except sqlite3.Error`, `except OSError` or
`except PermissionError` already match it. It is in no `__all__` and has no documented import
path, and `log_foundry.__all__` and `sinks.base.__all__` do not change.

Some behaviour stays as it is, and one interaction is named:

- The count never takes the emit lock, so a `losses()` poll does not wait behind a write
  (SPEC-028 FR-003).
- A closed sink still raises a plain `SinkDeliveryError` and counts nothing.
- FR-001's unencodable count is committed through a `Deferral` beside the immediate `_failed`
  store, and `IMMEDIATE` in `tests/test_deferral.py` names that store.
- A file write that fails partway leaves the earlier lines of the batch on disk, and the retry
  writes them again (Out of Scope).

#### Acceptance Criteria:

- [ ] With its table dropped under it, `SQLiteSink.emit` raises an exception that:
      - is an instance of `SinkDeliveryError`, `sqlite3.OperationalError` and `sqlite3.Error`;
      - has the original as `__cause__`, and the original's `sqlite_errorcode`, `str()` and `args`.

      `losses().failed` equals the batch size, and one line names `SQLiteSink` and
      `OperationalError`.
- [ ] On `FileSink` and `RotatingFileSink`, a stream whose write raises `OSError(28, ...)` (and,
      separately, one whose flush does) produces an exception that is an instance of
      `SinkDeliveryError` and `OSError`, with `errno == 28` and `str()` and `args` equal to the
      original's. A raised `PermissionError` stays an instance of `PermissionError`, and a plain
      `OSError("plain")` keeps `str() == "plain"`. `failed` equals the batch size, and one line
      names the class, `OSError` and `errno=28`.
- [ ] An `except sqlite3.Error`, an `except OSError` and an `except SinkDeliveryError` each catch
      the raised exception.
- [ ] The raised exception's `type().__name__` is the original's, so the worker's abandon line is
      unchanged from 1.1.0.
- [ ] A pickle round trip of the raised exception gives an instance of the original class with
      equal `str()` and `args`.
- [ ] A factory that cannot build the class (an exception whose constructor rejects its own
      arguments) leaves the original exception raised unchanged, with `failed` and the line as
      above.
- [ ] Through the worker, `failed_batches` and `flush()`'s `abandoned` reading are as before, and
      `health().sink.failed` is now non-zero. On the orphan path `orphan_lost` counts the event and
      the sink's `failed` moves with it: the recorded upper-bound exception of invariant 2.
- [ ] A closed sink raises a plain `SinkDeliveryError` and moves no counter, and a successful batch
      writes the same bytes as before.
- [ ] `losses()` returns while an `emit` is parked holding the emit lock. After a fork repair, a
      child can call `emit` and `losses()` on each of the three sinks while a parent thread holds
      the counter lock (invariant 12).
- [ ] Under `MultiSink(SQLiteSink(...), MemorySink())` with the table dropped, `MultiSink` counts
      the batch once in its absorbed term, as SPEC-058 FR-007 says.
- [ ] `log_foundry.__all__` and `log_foundry.sinks.base.__all__` are unchanged, no name other
      than the five `losses()` methods enters the public surface, and `tests/test_public_surface.py`
      and the typed-consumer probe pass (invariant 10).
- [ ] Serves invariants 7, 2, 11, 10 and 12. Invariant 7 is served in this sense: a total failure
      raises with a counter moved. A partial write is outside it (Out of Scope).

### FR-005: `SyslogSink` stamps the process that sends the message

#### Description:

`SyslogSink` reads `os.getpid()` once, in `__init__`, and writes it as the RFC 5424 PROCID of every
message, so a child forked after construction stamps its messages with the parent's pid. At
`fda134a`, a forked child emitting through a UDP sink produced a datagram whose PROCID was the
parent's pid. The pid is now read in `_frame`, per message. That costs 11.6 ns, against 1,381 ns
for one `json.dumps` of a typical event (CPython 3.13.1, `timeit`, at `fda134a`). `_hostname` is
unchanged, since a fork does not change it.

#### Acceptance Criteria:

- [ ] After a real `os.fork()`, a message the child sends carries the child's pid as its PROCID,
      and a message the parent sends afterwards carries the parent's. The fork is bounded by
      `signal.alarm` and a parent-side kill, the idiom of `tests/test_fork_lifecycle.py`.
- [ ] The PROCID is still the decimal pid, and the rest of the frame is byte-identical to 1.1.0
      for an unforked process.
- [ ] Restoring the construction-time read reddens the child test.
- [ ] This FR serves no invariant: PROCID is message content, and no invariant states what a
      message's content says about its sender. The fork invariant's observable covers locks, delivery
      and ownership in a child, not the identity an event carries.

---

## Recorded text the build changes

The completion ritual rewrites each of these in place, with a dated History entry where the file is
a decision, and leaves a superseded marker at every other site that still states the old claim.

- **`docs/invariants.md`.**
  - Invariant 2: the sentence listing the five losses counted but never retried widens "an
    unrenderable HTTP event" to an event a sink cannot encode. The sentence listing the deferring
    sites adds the encode step of every sink on the rule.
  - Invariant 7: the recorded exception widens "an all-unrenderable HTTP batch" to a batch of
    nothing but events the sink cannot encode, on every sink on the rule.
  - Invariant 8: "so a sink may `json.dumps` bare" becomes "assembly's promise holds, and a sink
    encodes under a guard". A new recorded exception states the destination-specific repair (FR-003):
    PostgreSQL's U+0000 and MongoDB's out-of-range integer and NUL key are repaired at the sink,
    after assembly, marked with `truncated` as assembly marks the same substitutions.
- **Decisions**, each rewritten in place with a dated History entry:
  - `caller-code-that-fails-for-one-event-costs-only-that-event-a-batch-of-nothing-but-failures-re-raises`:
    its HTTP-half paragraph, and "do NOT apply the all-failed re-raise to HTTP's unrenderable
    events", generalise to every sink on the rule.
  - `a-sink-that-delivered-nothing-raises-one-that-delivered-something-reports`: "three cases stay
    silent by prior decision" gains the unencodable batch.
  - `a-raising-emit-counts-nothing-whoever-catches-the-raise-owns-the-batchs-count` and
    `a-dropped-count-is-written-only-by-a-deferral-commit-and-the-gate-derives-which-counters-are-counted`:
    the sites and the counters gain `_unencodable`.
  - `an-event-is-safe-by-construction--coerced-and-bounded-once-at-assembly-not-per-sink`: a
    marker, because the `json.dumps` calls are no longer bare and two sinks repair after assembly.
- **Decisions**, three new ones:
  - in the sink-delivery area, that an event a sink cannot encode costs only itself, is counted
    `dropped` and settles an all-unencodable batch, with the two declared exceptions and Andrew's
    2026-11-04 choice;
  - in the sink-delivery area, that a sink's own driver error is counted, announced and raised as
    both types;
  - in the event-assembly area, that a destination's refusal is repaired at the sink and marked as
    assembly marks, with Andrew's 2026-11-04 choice.
- **`docs/architecture.md`.** Two passages each get a superseded marker: the §6 passage ending "an
  HTTP sink drops an event whose render still raises, alone", and the §13 *Known constraints*
  note on a transform output nested past the C stack, which says "every JSON sink's own
  `json.dumps`" crashes.
- **`README.md`.**
  - The `health()` section sentence "Most sinks drop only what can never fit" gains the unencodable
    event.
  - The *Field values are coerced and bounded* section gains the two repairs.
  - The sink-authoring notes keep saying a sink that delivered nothing raises.
- **Docstrings.**
  - The `Raises:` clauses on `_row`, `_frame`, `_render` and the other encode functions that say
    "TypeError: if the event is not JSON-serializable" are rewritten.
  - `SinkDeliveryError`'s docstring says a sink may raise a subclass that is also the driver's
    type.
  - The docstring of `tests/test_public_surface.py::test_nothing_expensive_or_reentrant_runs_under_the_config_lock`
    claims "`StdoutSink.__init__` is a single attribute assignment". It is edited to name the lock
    and counter the constructor now also creates.
- **`docs/component-inventory.md`.** One row each for `_encode.py` and `_driver_error.py`.
- **Tests that pin a removed mechanism**, each rewritten to the new outcome:
  - `tests/test_sinks_file.py::test_the_rotating_sink_reports_no_losses_for_what_retention_discards`
    asserts `not hasattr(sink, "losses")`, "deliberately". It now asserts `losses() is None` after
    retention, which keeps its point: retention is not loss.
  - Any test asserting that an unencodable event raises out of one of these `emit`s, or is retried
    inside Postgres or Redis, is rewritten to the FR-001 and FR-002 outcome.
  - `tests/test_sinks_nats.py`'s isolation test and `tests/test_sinks_http_render.py` stay.

---

## Data Model

```
// src/log_foundry/sinks/_encode.py   (private; in no __all__)
encode_events(
    batch: list[dict[str, object]],
    encode: Callable[[dict[str, object]], T],
    pending: Deferral,
    *,
    sink: str,                     # the class name written on the line
    counter: str = "_unencodable", # "rejected" for KafkaSink and GooglePubSubSink
) -> list[tuple[dict[str, object], T]]

repair_nul(event) -> dict[str, object] | None        # a marked copy, or None when nothing to repair
repair_bson(event) -> dict[str, object] | None       # ints outside int64 and NUL keys, marked

// src/log_foundry/sanitize.py   (private)
int_placeholder(value: int) -> str                   # "<int: ~N digits>", called by _int_text; _Coercer and _otel held equal

// src/log_foundry/sinks/_driver_error.py   (private; in no __all__)
delivery_error(err: Exception) -> Exception | None
    # an instance of SinkDeliveryError and of type(err) with err's str() and args, or None
```

## Configuration / Environment

None. No new argument, `configure()` key or environment variable.

## File & Folder Structure

```
src/log_foundry/
├── sanitize.py             # edited: int_placeholder factored out of _int_text
└── sinks/
    ├── _encode.py          # new: encode_events, repair_nul, repair_bson
    ├── _driver_error.py    # new: delivery_error
    └── (stdout, file, sqlite, clickhouse, rabbitmq, kafka, kinesis, firehose, sns, sqs, mongodb,
         eventhubs, syslog, logstash, _socket, postgres, redis, pubsub: edited)
tests/
├── test_encode.py          # new: the helper, the repairs, the roster test and its corpus
├── test_sinks_encode.py    # new: FR-001 to FR-003 over the derived roster
└── integration/            # test_mongo.py and test_postgres.py gain the FR-003 cases
```

## Implementation Phases

### Phase 1: The helper and its gate

- `_encode.py` and `encode_events`, with unit tests of the guard, the count and the line.
- The roster test, with its floor and fixture corpus. `DEFERRING_SITES`, `IMMEDIATE` and
  `DROPPED_FLOOR` entries are added as sinks convert.

### Phase 2: The sinks that deliver before they raise

- `KafkaSink`, `RabbitMQSink`, `AzureEventHubsSink`, `ClickHouseSink`, `StdoutSink`/`StderrSink`,
  `FileSink` and `RotatingFileSink`, with `losses()` and a counter lock on the four that lack them.

### Phase 3: The rest of the roster

- `SQLiteSink`, the AWS sinks, `MongoDBSink`, `SyslogSink`, `LogstashSink` and the transport,
  `PostgresSink`, both Redis sinks and `GooglePubSubSink`.
- The all-unencodable settle across the roster, with `MultiSink`, worker and orphan criteria.

### Phase 4: Destination repairs

- `int_placeholder` in `sanitize.py`, with its equality test.
- The Postgres detector and NUL repair, and the Mongo `bson` detector and repair.
- The integration cases.

### Phase 5: Driver errors and the pid

- `_driver_error.py`, SQLite and the two file sinks, then `SyslogSink`'s pid.

### Phase 6: Recorded text and release notes

- The rewrites listed above, the three decisions, the inventory rows and the release-notes entry.

## Revision history

- Drafted against `fda134a`. The plan's count of twelve raising sinks re-measured as sixteen
  classes (fifteen `emit` implementations). The plan's two sinks that "count it failed"
  (`NATSSink`, `SentrySink`) are kept as declared exceptions, on the principle that an outcome
  that has not changed keeps its counter.
- Revised after the spec review, which raised twelve numbered findings plus nits; findings 1 and 3
  were the two product calls, and Andrew answered both on 2026-11-04:
  - An all-unencodable batch settles, and the Overview now records that as Andrew's choice over
    the plan's wording rather than something an invariant forces.
  - FR-003 changed from "drop" to "repair and deliver". Its NUL detector was restated, because the
    draft's prose rule read inverted, and it now repairs strings before encoding rather than
    pattern-matching the JSON.
  - Counts were corrected: sixteen classes raise, eight duplicate.
  - `TransformSink` joined the roster's exemptions.
  - The per-event part of each helper is now described as pulled out.
  - The driver-error factory now copies only attributes the original set, and pickles as the
    original.
  - New public `losses()` methods, private `_failed`, the `StdoutSink` counter lock and the two
    edited tests are named.
  - FR-005 now serves no invariant.
  - The Postgres loss is now described as the whole batch (one commit across all chunks).
  - FR-005's octet-count criterion was dropped, because it already passes today.
- 2026-11-05: rebased onto main after v1.1.0; the landed peer specs are now in depends_on.
- 2026-11-05, drift review: SPEC-061 (its `NATSSink` criteria pin SPEC-061's sink) and SPEC-066 joined `depends_on`; `int_placeholder` is factored out of SPEC-066's `sanitize._int_text`, and its equality test covers `_Coercer`'s two inline copies and `_otel._int_digits`.
- 2026-11-05, execution-frame review: the placeholder equality test's inputs are stated as past every site's ceiling at the default limit, since the sites' thresholds differ near 4300 digits and under `set_int_max_str_digits(0)`.
- 2026-11-06, plan review and build:
  - The roster test classifies the HTTP family by class, not by function: a class is "on
    `HTTPSink._items`" when its resolved `emit` is `HTTPSink.emit` **and** its resolved `_items` is
    `HTTPSink._items`. FR-001's last criterion names `_render` and `_otel._dumps` as what the
    family reaches, but `LokiSink._body` also calls `json.dumps` (on label dicts `_item_size` has
    already encoded inside `_items`' guard), from `HTTPSink.emit` and outside `_items`, so a
    function-level boundary at `_items` would refuse Loki. The cost of the class rule is that a
    new `json.dumps` in an HTTP subclass's `_body` is not seen; the family is out of scope. A
    subclass that overrides `_items` is held to the helper rule, and a corpus case pins it.
  - The Overview's `nats.py:343-355` citation was stale: SPEC-061 rewrote the file, and
    `_publish_all`'s per-event encode now sits at `nats.py:1306-1320` (at `ebf9d680`). The
    behaviour is as stated.
  - `SocketTransport.send_all` returned on an empty list before its closed check, so a closed
    `SyslogSink` or socket-mode `LogstashSink` would have settled an all-unencodable batch and
    committed its count, against FR-002's closed-sink criterion. The empty return now follows
    the closed check.
  - Out of Scope gains the SQLite bind failures (an integer past 64 bits, a `dict`) that a
    direct `emit` can still use to cost a batch.
- 2026-11-06, completion: the two `architecture.md` passages were cited as §2 and §12; they sit in
  §6 and in §13's *Known constraints*, and the citation is corrected. The `StdoutSink.__init__`
  docstring in `tests/test_public_surface.py` was rewritten in phase 2, so the completion found it
  already true.
- 2026-11-06, system-frame diff review: FR-003's sentence that a plain substring test "would
  repair an event JSONB accepts" was false of the design as built. `repair_nul` walks for a real
  U+0000 and returns `None` for the six-character lookalike, so the walk decides correctness and
  the pattern only keeps the walk off clean text; the sentence, the `NUL_ESCAPE` docstring and
  the event-assembly decision now say so.
