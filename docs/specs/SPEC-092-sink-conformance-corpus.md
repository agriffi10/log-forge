---
type: Spec
id: SPEC-092
title: "A Sink Conformance Corpus Keyed on Acknowledgement Class, with an Explicit Ambiguous Outcome"
description: "Adds one test corpus that runs every shipped sink through the fault scripts of its acknowledgement class against a stateful fake backend, and judges each row by one assertion: what the backend holds, against what the sink and its catcher reported, event by event, with an ambiguous outcome of its own, so that a sink which loses, duplicates or miscounts under a fault fails a test, and every row red today is pinned to the open item that owns it."
spec_status: Draft
depends_on: [SPEC-026, SPEC-041, SPEC-049, SPEC-061, SPEC-069, SPEC-071, SPEC-072, SPEC-073, SPEC-074, SPEC-075, SPEC-079, SPEC-082, SPEC-084, SPEC-085, SPEC-088, SPEC-089, SPEC-090, SPEC-091]
arc: audit-2026-11-14
arc_order: 8
---

## Overview

The library promises that every event is delivered or counted and announced once (invariant 2), and
that a sink reports what it delivered truthfully (invariant 7). Today each sink is held to those
promises by its own tests, written one fault at a time, and the 2026-11-14 audit found six ways the
sinks break them that no test had tried: frames written into a socket whose peer had closed, events
sent twice when a reply was lost, events an SDK discarded counted as sent, a verdict given when a
client queued a message rather than when the broker answered, a reply that names no failure read as
full delivery, and an autocommit connection breaking an all-or-nothing write (finding 22). This
spec adds one corpus that asks every shipped sink the same questions. Each sink declares what it can
know about whether its backend took an event, its **acknowledgement class**. Each class has one fake
backend that records what it durably holds and one script of faults. One assertion judges every row:
what the backend holds against what the sink and its catcher reported, event by event, where
"ambiguous" (the sink cannot know) is an outcome of its own. The corpus changes no library code. A
row that is red on today's code is pinned to the open item that owns its defect, so the suite stays
green and turns red the day a sink is fixed or a new defect appears; the arc's next sink specs fix
the sinks and flip the pins.

## Scope

### In Scope

- Ten acknowledgement classes, and every shipped sink class mapped to one per constructor variant,
  as data in `tests/`, held to the sink roster and to each sink's constructor parameters (FR-001).
- One fake backend and one fault script per class, a client double per sink that translates only
  call and reply shapes and raises its driver's own classes, every shipped sink covered with no
  Docker and no network, and the corpus's cost measured (FR-002).
- One assertion of seven checks, the ambiguous outcome read from the engine's own verdicts, the
  per-event reading proved against the engine's `Delivery` run by run, a corpus of hand-built
  reports with one case per way each check fails, and a mutant for every check (FR-003).
- The register: 74 rows pinned to nine open items, 12 permitted departures each quoting its record,
  the register gates as pure functions with a standing fixture corpus, and the record the build
  changes, which carries two decisions Andrew made on 2026-11-15 into §12 (FR-004).
- Proof that the corpus bites: at three older trees it reports the stream and file sinks' fixed
  duplicates and miscounts, and nothing else moves (FR-005).

### Out of Scope

- **Fixing any sink.** Every red row is pinned, never fixed. The writes nothing acknowledges
  (findings 1, 11, 12 and RabbitMQ's confirms) and the reading of a reply that names no failure
  (finding 14) are the arc's next two sink specs, SPEC-093 (findings 1, 11 and 14 and RabbitMQ's
  confirms) and SPEC-094 (finding 12), which use these classes and flip these pins as Andrew decided
  (FR-004).
- **Idempotency keys and a count of the sends that may have duplicated** (finding 5). A later spec
  of this arc. This spec adds no counter: the corpus reads "ambiguous" test-side (FR-003).
- **Any change under `src/`**, any public attribute, and any change to a stderr line.
- **`SQSSink.MAX_BYTES`** against the service's current limit (`docs/architecture.md` §12).
- **Real-backend integration tests.** `.github/workflows/integration.yml` runs the extras-backed
  sinks against nine real services (SPEC-041) and is where a real client's call and reply shapes
  are measured. The corpus models those shapes with fakes and does not replace it; the corpus does
  run inside that workflow's unit-suite step, with every extra installed (FR-002).
- **The nightly soak** and any change under `.github/` (the arc's CI track).
- **The orphan path and a caller's own `emit` call as drivers.** Every row runs the worker path
  through the public API (FR-002); `tests/test_delivery_paths.py` holds the paths to one ledger
  (invariant 6), and the wrapper rows reach the inline adapter through `MultiSink` and
  `_FactorySink`.
- **Faults the audit read and did not run, and no class script models:** `AzureEventHubsSink`'s
  `create_batch` raising after an earlier pack was sent, `KafkaSink.emit`'s `poll(0)` outside its
  `try`, `NATSSink`'s round deadline (held by `tests/test_sinks_nats_engine_matrix.py`, including
  SPEC-074's pinned 1.x row "landed beside the unattempted"), and `OTLPSink`'s count-only
  `partialSuccess`, which names no event (held by `tests/test_sinks_otlp.py`).
- **Third-party sinks.** The corpus covers the classes this package ships.

---

## Functional Requirements

*Evidence* at the end of this spec holds every measurement the requirements rest on. Each was taken
at `6bd0b66e` on macOS with Python 3.13.1 and no extra installed, by the prototype committed under
`docs/audits/2026-11-14-probes/sinks/spec092_*.py`, which runs every row the build's corpus runs,
and the determinism runs were repeated on Python 3.12.12.

**Two decisions Andrew made on 2026-11-15** bind the expected verdicts below.

- **A, finding 14.** Asked whether an AWS batch reply that names no failure (`{}`, a `Successful`
  list with no `Failed`, or a Kinesis or Firehose reply with neither a count nor records) should
  read as delivered, he answered **"Unadjudicated (Recommended)"**: every event the reply does not
  name is `UNKNOWN`, counted in `failed` and `dropped_unadjudicated`, announced once as one that may
  have landed, and never resent. SPEC-093 builds it; here the rows stay pinned
  until it does, and the reading the corpus expects is that one.
- **B, finding 12.** Asked whether a client-buffered sink's verdict may come
  before the client's report, he answered **"Wait for the report (Recommended)"**: each batch's
  `emit` waits for the client's delivery reports within the sink's existing timeout, as core NATS
  confirms, ~~so a truthy `flush()` means the broker acknowledged~~. *Superseded on 2026-11-16:*
  Andrew answered "Truthy, counted (Recommended)", so a truthy `flush()` means every event was
  acknowledged or counted; an indefinite report is counted `UNKNOWN` and leaves it truthy, and a
  definite rejection makes it falsy (SPEC-094). SPEC-094 builds it; here the corpus requires it,
  and the rows flip when it lands.

### FR-001: Every shipped sink declares one acknowledgement class per constructor variant, as data

#### Description:

A sink's **acknowledgement class** is what it can know about whether its backend took each event,
read from the code that decides "delivered" in each sink. Ten classes cover the tree:

| Class | What the sink can know |
|---|---|
| `keyed-reply` | one reply per call names each failed entry by key; an entry it does not name landed |
| `positional-reply` | one reply per call carries one result per entry, in order |
| `request-reply` | one status or return for the whole call |
| `transactional` | the call commits all of its rows or none |
| `local-write` | each write call says what it took: a file's byte count, a stream's return or raise |
| `socket-stream` | a write that returned was taken by the kernel; nothing confirms a frame |
| `per-event-ack` | the backend acknowledges each event on its own call |
| `client-buffered` | a client library takes each event into its buffer and reports on it later |
| `in-process` | an in-process callable, logger or list; its return is the hand-off |
| `wrapper` | it forwards to an inner sink and adds only its own decisions |

**A class belongs to a constructor variant, not to a Python class**, because the constructor picks
what the sink can know. `LogstashSink` sends over HTTP or a socket, `SentrySink` through the SDK or
its own HTTP request, and `NATSSink` to JetStream, which acknowledges each publish, or to core NATS,
which confirms a batch by a round trip only when the sink connected the client itself (SPEC-061).
`PostgresSink` and `RabbitMQSink` reopen a connection they built and never one they were handed, so
a lost connection costs a borrowed one every later round. A variant is named only where the class,
the outcome or a departure (FR-004) differs, so 39 classes give 53 variants:

| Class | Variants |
|---|---|
| `keyed-reply` | `SQSSink` `standard` and `fifo`, `SNSSink`, `MongoDBSink` (its `writeErrors` name each failure by `index`) |
| `positional-reply` | `KinesisSink`, `FirehoseSink`, `ElasticsearchSink`, `OpenSearchSink`, `HoneycombSink` |
| `request-reply` | `HTTPSink`, `DatadogSink`, `SplunkHECSink`, `NewRelicSink`, `LokiSink`, `OTLPSink`, `LogstashSink` `http`, `SentrySink` `http`, `ClickHouseSink`, `AzureEventHubsSink`, `RedisStreamsSink`, `RedisListSink` |
| `transactional` | `PostgresSink` `owned`, `borrowed` and `autocommit`, `SQLiteSink` and its `autocommit` variant |
| `local-write` | `FileSink`, `RotatingFileSink`, `StdoutSink`, `StderrSink` |
| `socket-stream` | `SyslogSink` `tcp` and `udp`, `LogstashSink` `tcp` and `udp`, `RabbitMQSink` `owned` and `borrowed` (no publisher confirms) |
| `per-event-ack` | `NATSSink` `jetstream` and `jetstream-injected` |
| `client-buffered` | `NATSSink` `core` and `core-injected`, `KafkaSink`, `GooglePubSubSink`, `SentrySink` `sdk` |
| `in-process` | `CallbackSink`, `LoggingSink`, `MemorySink`, `NullSink` |
| `wrapper` | `FilteringSink` and `TransformSink` each `over-http` and `over-callback`, `MultiSink` `over-http`, `_FactorySink` `over-http` |

`_FactorySink` is private but is what `configure(sink_factory=…)` installs (SPEC-079), so it has a
row. `_RedisSink` is the private base of the two Redis sinks, which no caller builds, and is listed
in `ABSTRACT` with that reason. `base.Sink` is the Protocol.

**The data lives in `tests/conformance_classes.py`, not on the sink.** A public attribute would add
to the surface of 39 classes for good (invariant 10 lets it grow, never shrink); it could not say
that one `LogstashSink` is a `socket-stream` and another a `request-reply` without becoming an
instance property; and the corpus is its only reader, since a third-party sink is not in it. The
next sink spec edits rows here when it changes what a sink can know (`RabbitMQSink`'s opt-in
confirmed variants in `per-event-ack`, for one).

**A new sink cannot ship without a class, and a new constructor argument cannot ship without a
verdict on whether it makes a variant.** The roster is the one `tests/test_public_surface.py`
derives from the source, every class under `sinks/` that defines or inherits `emit`, read without
importing so an absent extra cannot hide one. `PARAMETERS` records every rostered class's
`__init__` parameter names, read by AST and inherited where a class defines none: 168 names over 39
classes at `6bd0b66e`. Each parameter is either one a variant's `picks` sets (`transport`,
`backend`, `jetstream`, `client` beside `servers`, `connection` beside `dsn` or `url`) or listed in
`NEUTRAL` for its class as one that does not change what the sink can know.

#### Acceptance Criteria:

- [ ] `VARIANTS` in `tests/conformance_classes.py` holds the 53 variants above, each naming exactly
      one of the ten classes, and a test asserts both literals (invariants 2 and 7).
- [ ] `roster_gap(derived, declared)` is a pure function returning one failure line per name on
      either side alone, and a test applies it to `_sink_classes_with_emit()` minus `Sink` against
      the names in `VARIANTS` and `ABSTRACT`, expecting none (invariants 2 and 7).
- [ ] Standing fixtures in `tests/test_conformance_gate.py` hold `roster_gap` to its failure text: a
      derived `ExampleSink` with no entry yields a line naming `ExampleSink`, an entry `GhostSink`
      the tree lacks yields a line naming `GhostSink`, and equal sets yield none (invariants 2 and
      7).
- [ ] A test asserts that the AST-derived parameters equal `PARAMETERS`, failing with the class and
      the parameter gained or lost, and that for each class `PARAMETERS` equals the union of its
      variants' `picks` keys and its `NEUTRAL` entry; a fixture class with one extra parameter
      yields a line naming both (invariants 2 and 7).
- [ ] Every variant's builder returns an instance whose class has the name its entry gives
      (invariants 2 and 7).
- [ ] A test asserts that every `ABSTRACT` entry is a class no `VARIANTS` entry builds and that at
      least one `VARIANTS` entry builds a subclass of it (invariants 2 and 7).
- [ ] No file under `src/` changes, and `inspect.signature` of every shipped sink's constructor is
      unchanged (invariant 10).

### FR-002: One fake backend and one fault script per class, and every shipped sink runs on them

#### Description:

**The backend.** Each row has one `Backend`: a counter of what it durably holds, keyed by
`log_id`, and the class's fault script. Every client double of the row writes through it. A double
knows only its client's call and reply shapes: on each client call it asks the backend for an
action, takes the events the action says, and answers in its client's own exception or reply. What
"taken" means is the backend's alone, and a double never decides it. Events carry their `log_id`s
from a patched `model.new_log_id`, so the backend counts by identity, never by position.

**The faults.** A fault acts on the first client call of its batch unless it says otherwise.

| Fault | What the backend does |
|---|---|
| `clean` | takes every event; every call succeeds |
| `refused-once` | the first call is refused before anything is taken; later calls succeed |
| `refused-always` | every call is refused before anything is taken |
| `partial` | the first call takes its first half, refuses the next quarter retryably and the last quarter on every call; the reply says which |
| `taken-then-raised` | the first call takes its first half, rounded up, then raises a transient error |
| `reply-lost` | the first call takes everything, then raises a transient error (for `transactional`, the commit is applied and the connection then drops) |
| `peer-closed` | a clean first batch; the peer then closes, the second batch's first three writes return having reached nobody, the fourth raises, and a new connection takes the rest (`udp`: no listener, nothing taken, nothing raised) |
| `no-verdict-shape` | the first call takes its first half and replies with no verdict: `{}`, or for `MongoDBSink` a `writeErrors` that is not a list |
| `never-acknowledged` | the client takes every event and its later report is **definitive** that each was refused: a Kafka delivery error `10` (`MESSAGE_TOO_LARGE`; ~~`29`~~, superseded by SPEC-094 because an authorization error can follow an earlier attempt that wrote the message), a Pub/Sub `InvalidArgument` (~~`PermissionDenied`~~, likewise), a Sentry `capture_event` that returns `None` |
| `report-indefinite` | the client takes every event and its report never says whether the backend kept it: a Kafka `-192` (`_MSG_TIMED_OUT`) or a Pub/Sub `DeadlineExceeded` for an event the backend holds, the Sentry SDK's transport dropping an event after `capture_event` returned its id, or a core NATS PING never answered (core NATS runs this under `never-acknowledged`) |
| `autocommit` | each statement is durable at once, and the second raises once with a serialization failure (`40001`), which leaves the connection usable |
| `caller-code-raises` | the wrapper's predicate or function raises for one event |

| Class | Faults run |
|---|---|
| `keyed-reply`, `positional-reply` | `clean`, `refused-once`, `refused-always`, `partial`, `taken-then-raised`, `reply-lost`, `no-verdict-shape` |
| `request-reply` | `clean`, `refused-once`, `refused-always`, `taken-then-raised`, `reply-lost` |
| `transactional` | those five and `autocommit`, which only the `autocommit` variants run; `SQLiteSink` skips `reply-lost`, since an in-process database has no reply to lose |
| `local-write` | `clean`, `refused-once`, `refused-always`, `taken-then-raised` |
| `socket-stream` | those four and `peer-closed`; `udp` skips `taken-then-raised`, since a datagram is sent whole or not at all |
| `per-event-ack` | `clean`, `refused-once`, `refused-always`, `reply-lost` |
| `client-buffered` | `clean`, `refused-once`, `refused-always`, `never-acknowledged`, `report-indefinite`; the two core NATS variants skip `report-indefinite`, since their `never-acknowledged` is that case |
| `in-process` | `clean`, `refused-once`, `refused-always`, `taken-then-raised`; `MemorySink` and `NullSink` run `clean` only, since neither calls anything that can fail |
| `wrapper` | its inner variant's class's faults, and `caller-code-raises` for `FilteringSink` and `TransformSink` |

`partial` does not apply where no reply names an event; `peer-closed` only where a connection is
held between batches; `no-verdict-shape` only where the reply is the verdict; the two report faults
only where a report follows the hand-off; `autocommit` only to a transaction. That is 256 rows.

**Shapes three pins depend on.** `PostgresSink` is built with `chunk_size=3`, so eight events are
three statements and `autocommit` can fail the second. `SQLiteSink`'s `autocommit` script applies
the first half of one `executemany`, rounded up, and then raises "database is locked" once. The
Postgres double's connection is lost for good on a connection error (`08006`): it sets `broken` and
`closed`, as psycopg's does, and every later call on it raises "the connection is closed"; a refusal
and `autocommit`'s failure are a serialization failure (`40001`), which a transaction survives. The
RabbitMQ double's connection is lost for good the same way, and a channel asked of it raises
`ConnectionWrongStateError`. The `owned` variants build through a stub `psycopg.connect` or
`pika.BlockingConnection` that returns a new connection each time.

**Knowable or not.** A call's outcome is *unknowable* when the sink cannot tell what its backend
kept: `taken-then-raised`, `reply-lost`, `peer-closed`, `no-verdict-shape` and `report-indefinite`,
and a text stream's raising `write` whatever the fault, because its contract does not say what was
kept. Two doubles say what was kept and make `taken-then-raised` knowable: a file's `write` returns
the bytes it took, and a transaction that raises before its commit is rolled back.

**Decision B, as the corpus states it.** A client-buffered sink's verdict must wait for the
client's report. A count a report moves after the batch's verdict was given (at a later batch or at
`close()`) is late, and a late count fails `partition` (FR-003). So a definitive refusal reported
after `emit` returned is red today, and green once `emit` waits for it and counts it before
returning; an indefinite report is ambiguous once the sink waits for it.

**The driver.** A row runs the public path the worker serves: `configure(sink=…)`, one `@trace`d
call per batch logging six events inside its span (eight with `span.start` and `span.end`, one
submission), `flush()` between the two batches of `peer-closed`, and `shutdown(timeout=None)`. The
exit drain delivers with the stop event set, so every backoff returns at once and no retry is
declined, which shortens a row and changes no verdict (*A shutdown shortens a wait; it must never
skip work*), and `shutdown()` closes the sink, which is where a client buffer's late report lands.
The worker's `flush_interval` default is raised to an hour, because SPEC-091 has the worker take an
open span's events about once an interval, which could split a row's one submission across two
batches, and the corpus reads a batch as one submission; under SPEC-090's sizing one submission is
still one batch. For the corpus's duration `socket.socket.connect` and `socket.getaddrinfo` raise,
so a row that reaches the network fails rather than waits.

**Drivers and extras.** No row needs an extra. A sink imports its client through
`sinks/_extras.import_extra` only when it builds that client itself (SPEC-071), so each SDK, driver
and socket sink gets an injected client double, or a patched seam where it builds its own or needs
the extra beside one: `_socket._make_tcp` and `_make_udp`, `eventhubs._event_data_cls`, a `nats`
module whose `connect` returns the double (as `tests/nats_engine_matrix_harness.py` builds a sink
that owns its client), and stub `psycopg` and `pika` modules. Where a sink's retry predicate reads
driver classes through `_classify.driver_classes`, which reads `sys.modules`, a stub of that driver
module is installed for the row and the double raises its class: botocore's
`EndpointConnectionError` and `ReadTimeoutError`, clickhouse-connect's `OperationalError`, Event
Hubs' `ConnectError` and `OperationTimeoutError`, pymongo's `AutoReconnect`, `NetworkTimeout` and
`BulkWriteError`, pika's `AMQPConnectionError` and `StreamLostError`, psycopg's
`OperationalError` with its `sqlstate`, and redis' `ConnectionError` and `TimeoutError`. No stub
class subclasses a builtin, so the raise reaches the driver-class branch. Elsewhere a double raises
what its client raises: `urllib`'s errors, `OSError`, `sqlite3`'s, `BufferError`. A row would be
skipped only where its class cannot be faked, and none is.

**Why `tests/sink_rigs.py` does not serve.** Its `RIGS` table (`:725` at `6bd0b66e`; the audit's
`:700` was its line at `a0f44b5f`) builds 20 record-building sinks over doubles that fail whole
calls only (`State.call`'s `error`, `fail_next` and `down`), and leaves out the nine HTTP-family
sinks, `NATSSink`, `SentrySink`, `LoggingSink`, `CallbackSink`, `MemorySink`, `NullSink` and the
wrappers. Since the audit, `tests/test_file_sink_partial_write.py` and
`tests/test_stream_sink_partial_write.py` take a write partway under a real file-size limit, and
`tests/test_sinks_nats_engine_matrix.py` scripts NATS publishes and acknowledgements, each for its
own sinks; no double compares what a backend holds with what was counted, for any sink.

#### Acceptance Criteria:

- [ ] The corpus generates exactly the 256 rows the tables above give, and a test asserts the
      literal and that every row's faults are its class's minus the variant's listed skips
      (invariants 2 and 7).
- [ ] Every row runs in the ordinary `CI / test` job with no Docker and no extra installed, under a
      `monkeypatch` that makes `socket.socket.connect` and `socket.getaddrinfo` raise, and none is
      skipped (invariants 2 and 7).
- [ ] With the driver stubs' classes unrecognised (`driver_classes` answering `()`), at least the
      `taken-then-raised` and `reply-lost` rows of `SQSSink[standard]`, `ClickHouseSink` and
      `RedisListSink` change verdict, which shows the rows reach the driver-class branch; the
      delivery doc lists every row that moved (invariants 2 and 7).
- [ ] For every fault a wrapper variant and its inner variant both run, the wrapper's row has the
      inner's verdict (`HTTPSink`'s for `over-http`, `CallbackSink`'s for `over-callback`)
      (invariants 6 and 7).
- [ ] The corpus's test file runs under `pytest -n 0` in under 10 s on the build machine, measured
      three times and recorded with the one-minute load in the delivery doc (invariant 2).
- [ ] Every patched seam, the `flush_interval` default and the network guard are set with
      `monkeypatch`, so a renamed seam fails the row rather than leaving it unpatched (invariants 2
      and 7).
- [ ] **Owed until a green run:** the corpus's verdicts are the same in `integration.yml`'s
      unit-suite step, with every extra installed, triggered by `workflow_dispatch` on the PR's
      branch, since its path filter does not trigger on `tests/` (invariants 2 and 7).

### FR-003: One assertion judges every row, and every check fails a mutant planted where it must fire

#### Description:

**A row's report.** Each event of a row gets one **fate**, read from what the library did:

- An event of a run the engine made (SPEC-072; the HTTP family since SPEC-073, the SDK, driver,
  socket, file and stream sinks since SPEC-074, SPEC-082 and SPEC-084, the wrappers since SPEC-075)
  is read from the rounds of that run, captured by wrapping the `deliver` callable `_engine._run`
  is handed, which both the worker and the inline adapter call, and tagged with the run that made
  them. Its fate in a run is its outcome in that run's last round that carried it, read by one
  function, `fate_of`: `ACCEPTED` is **landed**; `REJECTED`, `DROPPED`, `RETRYABLE` and a round that
  raised are **lost**; `UNKNOWN` and an unreadable verdict are **ambiguous**; `SKIPPED` is
  **skipped**. Every event of a run that failed is lost, because its catcher owns it. A batch can
  take several runs, when the worker resends a wrapper with no `deliver` around an engine child;
  each run is read on its own and the last decides. The judged sink is the row's sink, or the
  engine child behind a wrapper that offers no `deliver` (`MultiSink`, `_FactorySink`).
- An event of a sink with no `deliver` is read per batch from `Worker._emit`, wrapped to see each
  batch: an abandoned batch is lost; one whose `emit` returned is landed less what its
  `losses()` moved during that `emit`, which is lost.
- A count that moves after a batch's verdict was in, at a later batch or at `close()`, is **late**.

**The ambiguous outcome needs no new counter here.** Today the library expresses it as `UNKNOWN`,
which the engine counts in `failed_events` (an upper bound), several sinks also in
`dropped_unadjudicated`, and SPEC-088's ledger in `sink_lost` beside definite losses; nothing on
`health()` tells it apart. The corpus reads it per event from the rounds and proves that reading
against the engine's own `Delivery` (`unknown`), run by run. A library-side count of "may have
landed" or "may have duplicated" belongs to the spec that adds idempotency keys, and `fate_of` is
the one place it would be read.

**The assertion.** A row fails each check whose condition holds, and its verdict is the tuple of
those checks in this order:

| Check | Fails when |
|---|---|
| `partition` | the fates do not cover the row's events exactly once, a run's fates disagree with that run's `Delivery` counts, or a count is late |
| `silent-loss` | an event reported landed is not held (for a sink with no `deliver`: more reported landed than held) |
| `duplicate` | the backend holds an event more than once, since nothing counts a possible duplicate today |
| `lost-but-held` | an event reported lost or skipped is held (for a sink with no `deliver`: more counted than not held) |
| `unannounced` | a batch lost or left ambiguous an event and wrote no `log-foundry: lost N` line inside its own `Worker._emit`, or a late count has no such line after the last batch |
| `ambiguous-when-knowable` | an event reported ambiguous was carried only by calls whose outcome was knowable |
| `ledger` | `health().events.unaccounted` is not 0 at the row's end, or the row's moves of `delivered`, `skipped`, `sink_lost` and `batch_lost` differ from its fates, as below |

For `ledger`, an engine run's landed events are its `delivered`, its skipped its `skipped`, a
settled run's lost and ambiguous its `sink_lost`, and a failed run's events its `batch_lost`; a
batch whose `emit` returned is all `delivered` and an abandoned one all `batch_lost`, because
SPEC-088's ledger ends where `emit` returns.

`unannounced` asks for a line per batch, attributed by where in stderr the batch's `_emit` began and
ended, not for the counts the lines name to add up: the HTTP family names its losses in requests,
its recorded unit, so a `MultiSink` child that failed every request names `lost 1 request(s)` for
eight events, and a sum would call that row red for its unit. `ambiguous-when-knowable` is what
keeps "ambiguous" from being a way out: a sink that called every raise ambiguous would pass
`taken-then-raised` and `reply-lost`, and this check fails it on `refused-once`.

**Decisions A and B in the assertion.** Once SPEC-093 and SPEC-094 land, a
`no-verdict-shape` row's unnamed events are ambiguous, which passes every check (they were carried
by an unknowable call, and a line names them), and a client-buffered row whose sink waits for the
report has no late count. Both are what the checks already ask; neither needs a change to `judge`.

#### Acceptance Criteria:

- [ ] A row's verdict is the tuple of the checks above that fail, in the table's order (invariants
      2 and 7).
- [ ] `judge`'s standing corpus in `tests/test_conformance_gate.py` holds one hand-built report per
      way each check fails, on each branch it has: per event, `partition` by coverage, by
      disagreement with a run's `Delivery` and by a late count, `unannounced` by a silent batch and
      by a silent late count, `ledger` by `unaccounted` and by a term that differs, and each other
      check once; per batch, the same for every check that branch has; and one report on each
      branch that fails none. Each case asserts its exact verdict (invariants 2 and 7).
- [ ] With `partition`, `silent-loss`, `duplicate` and `lost-but-held` each removed from `judge` in
      turn, `KafkaSink-never-acknowledged` reads `silent-loss` in the first, and
      `SyslogSink[tcp]-peer-closed`, `HTTPSink-reply-lost` and `MongoDBSink-reply-lost` read green in
      the other three, one each; the delivery doc lists every row each removal moves (invariants 2
      and 7).
- [ ] With `_diag.lost` returning at once, in an export of the branch head,
      `SQSSink[standard]-refused-always` reads `unannounced`, and the delivery doc lists every row
      that moved (invariants 2 and 7).
- [ ] With `outcome = Outcome.RETRYABLE if retryable else Outcome.REJECTED` in `_RedisSink._round`
      replaced by `outcome = Outcome.UNKNOWN`, in an export of the branch head,
      `RedisStreamsSink-refused-once` and `RedisListSink-refused-once` read
      `ambiguous-when-knowable` (invariants 2 and 7).
- [ ] With `fate_of` reading `REJECTED` as landed, `SQSSink[standard]-partial` reads `partition`
      among its checks, which only the comparison with `Delivery` can give it (invariants 2 and 7).
- [ ] With `duplicate`'s condition changed from "held more than once" to "held at least once",
      every green `clean` row reads `duplicate` (invariants 2 and 7).
- [ ] With the `sink_lost` increment SPEC-088 places at a settled run in `Worker._emit`
      (`worker.py:1940` in its FR-001 table) removed, in an export of the branch head,
      `SQSSink[standard]-partial` reads `ledger` (invariant 2).
- [ ] No corpus test carries SPEC-088's `ledger_exempt` marker, and every row balances in its
      autouse fixture (invariant 2).

### FR-004: The red rows are pinned, the departures quote their records, and the register cannot go quiet

#### Description:

At `6bd0b66e`, 86 of the 256 rows are red, the same 86 at the same verdicts in eight runs on Python
3.13 and three on 3.12. This spec fixes that set. A row red at build time and not listed here, or
listed and green, is a finding the build reports before it changes anything.

**A pin** names rows, the exact verdict each has, and its **item**: the bold lead of an open bullet
under `### Open` in `docs/architecture.md` §12, as SPEC-085 pinned its cells (*A red adversarial
cell is pinned to its open item at its exact verdict, and a pin moves only after the owner hears
why*). A pinned row runs and is reported as an expected failure only at its pin's verdict; a pass
or any other verdict fails the suite. **Nothing is withheld:** SPEC-085 withheld a cell that
repeats a pin because each cost seconds of waiting, and no row here costs more than milliseconds,
while a wrapper row that repeats its inner sink's pin is what holds the wrapper to it (FR-002).

| Pin | Rows | Verdict | Item |
|---|---|---|---|
| `reply-resent` | `taken-then-raised` and `reply-lost` of `SQSSink[standard]`, `SNSSink`, the five `positional-reply` variants, the twelve `request-reply` variants and the four `wrapper` variants `over-http`; `PostgresSink[owned]-reply-lost`; `taken-then-raised` of `SyslogSink[tcp]`, `LogstashSink[tcp]` and `RabbitMQSink[owned]` (50) | `duplicate` | "A send whose reply is lost is sent again, and the resend is neither counted nor keyed" (finding 5) |
| `reply-counted-lost` | `MongoDBSink` `taken-then-raised` and `reply-lost`, `PostgresSink[borrowed]-reply-lost`, `RabbitMQSink[borrowed]-taken-then-raised` (4) | `lost-but-held` | the same item |
| `reply-without-verdict` | `no-verdict-shape` of `SQSSink[standard]`, `SQSSink[fifo]`, `SNSSink`, `KinesisSink`, `FirehoseSink` (5) | `silent-loss` | "An AWS batch reply that names no failure is read as full delivery" (finding 14) |
| `bulk-without-errors` | `no-verdict-shape` of `ElasticsearchSink`, `OpenSearchSink` (2) | `silent-loss` | "`ElasticsearchSink` reads a `2xx` object without a truthy `errors` as success, unverified." |
| `peer-closed-tcp` | `peer-closed` of `SyslogSink[tcp]`, `LogstashSink[tcp]` (2) | `silent-loss` | "A TCP `SyslogSink` or `LogstashSink` counts a frame as accepted when `sendall` returns, so frames written after the peer closed vanish uncounted" (finding 1) |
| `publish-without-confirms` | `peer-closed` of `RabbitMQSink[owned]`, `RabbitMQSink[borrowed]` (2) | `silent-loss` | "`RabbitMQSink` publishes without confirms, so an unroutable message or one written to a dead socket reads as accepted" |
| `jetstream-ack-lost` | `reply-lost` of `NATSSink[jetstream]`, `NATSSink[jetstream-injected]` (2) | `lost-but-held` | "A timed-out JetStream publish is not retried, and cannot safely be" |
| `verdict-before-refusal` | `never-acknowledged` of `KafkaSink`, `GooglePubSubSink` (2) | `partition`, `silent-loss` | "`KafkaSink` and `GooglePubSubSink` give their verdict when the client queues a message, not when the broker answers" (finding 12) |
| `verdict-before-indefinite-report` | `report-indefinite` of `KafkaSink`, `GooglePubSubSink` (2) | `partition`, `lost-but-held` | the same item |
| `sdk-discard-counted-sent` | `SentrySink[sdk]-never-acknowledged` (1) | `silent-loss` | "`SentrySink`'s SDK backend counts an event the SDK discarded as sent" (finding 11) |
| `autocommit-postgres` | `PostgresSink[autocommit]-autocommit` (1) | `duplicate` | "An injected autocommit connection breaks the database sinks' all-or-nothing write" (finding 15) |
| `autocommit-sqlite` | `SQLiteSink[autocommit]-autocommit` (1) | `duplicate`, `lost-but-held` | the same item |

That is 74 pinned rows. `GooglePubSubSink`'s half of finding 12, which the audit read and did not
run, is now run. The five `reply-without-verdict` rows turn green when SPEC-093 builds decision A,
and the four finding-12 rows when SPEC-094 builds decision B.

**A departure** is a red row the library permits: the row passes only at the departure's exact
verdict, and the departure's **record** is a file and a phrase quoted from it verbatim, which the
gate finds in the file after collapsing runs of whitespace to one space.

| Departure | Rows | Verdict | Record: file, and the phrase quoted from it |
|---|---|---|---|
| `sqlite-failed-before-raise` | `SQLiteSink` `refused-once` and `taken-then-raised` (2) | `lost-but-held` | `docs/invariants.md`: "the batch size a `SQLiteSink` adds to `failed` when its driver raises, before it raises in turn (SPEC-069 FR-004)" |
| `udp-fire-and-forget` | `peer-closed` of `SyslogSink[udp]`, `LogstashSink[udp]` (2) | `silent-loss` | `docs/architecture.md`: "A UDP datagram is fire-and-forget: a `sendto` that returned is all `SyslogSink` and `LogstashSink` can know." (added by this spec, below) |
| `injected-nats-unprobed` | `NATSSink[core-injected]-never-acknowledged` (1) | `silent-loss` | `docs/architecture.md`: "`NATSSink` cannot drive a client it did not connect, so an injected client's liveness is the caller's." |
| `callable-raises-after-keeping` | `taken-then-raised` of `CallbackSink`, `FilteringSink[over-callback]`, `TransformSink[over-callback]` (3) | `duplicate` | `docs/architecture.md`: "A caller's callable that keeps events and then raises has them resent." (added by this spec, below) |
| `handler-absorbs` | `LoggingSink` `refused-once` and `refused-always` (2) | `silent-loss` | `docs/specs/SPEC-049-sink-construction-validation-and-driver-bounds.md` (FR-007): "a handler failing in the ordinary way is invisible here" |
| `null-discards` | `NullSink-clean` (1) | `silent-loss` | `docs/spec-delivery/SPEC-026-sink-loss-visibility.md` (*Deliberate deviations from the spec as written*): "`NullSink` deliberately exposes no `losses()`: discarding is what it is *for*" |
| `sdk-transport-drop` | `SentrySink[sdk]-report-indefinite` (1) | `silent-loss` | `src/log_foundry/sinks/sentry.py` (`SentrySink.flush`): "It cannot report a failure, and that is the SDK's shape rather than a choice here." |

That is 12 departure rows and 170 that must pass.

**The gates are pure functions with a standing fixture corpus**, because a gate whose checks have
no fixtures goes quiet unseen. `register_faults(rows, pins, departures, texts)` returns one failure
line per problem, reading `texts`, a mapping of file to content, so no fixture touches the
repository. It reads `docs/architecture.md` §12 bullet by bullet as paragraphs, each bullet's text
joined across its lines, because leads wrap (`architecture.md:1698`, `:1765` and `:1792` at
`6bd0b66e`), and it takes a lead only from an open bullet, not one struck through with `~~`.

**The record the build changes**, and nothing else in either file:

- §13 *Known constraints* gains two bullets. "**A UDP datagram is fire-and-forget: a `sendto` that
  returned is all `SyslogSink` and `LogstashSink` can know.**" With no listener, every datagram is
  lost and nothing is counted, which §12's finding-1 item already calls inherent to UDP. And "**A
  caller's callable that keeps events and then raises has them resent.** `CallbackSink` (and a
  wrapper over it) cannot see what the callable kept; its raise tells the worker that nothing landed
  (invariant 7), and the worker resends the batch whole."
- §12's finding-5 item gains two sentences: `MongoDBSink`'s form of it is a resend that the unique
  `_id` refuses and that is counted as a rejection although the document landed, as *No shipped
  sink declares itself idempotent* records; and a borrowed `PostgresSink` or `RabbitMQSink`
  connection, which the sink may not reopen, leaves the events the backend took counted as lost
  when the reply was lost with the connection. Both are held by the `reply-counted-lost` pin.
- §12's finding-14 item's **Closed by** is rewritten in place, dated: "**Decided by Andrew on
  2026-11-15.** Asked whether a reply that names no failure should read as delivered, he answered
  'Unadjudicated (Recommended)': every event the reply does not name is `UNKNOWN`, counted in
  `failed` and `dropped_unadjudicated`, announced once as one that may have landed, and never
  resent. **Closed by** SPEC-093 building that reading, which turns the
  `reply-without-verdict` rows of `tests/test_conformance.py` green."
- §12's finding-12 item's **Closed by** is rewritten in place. *Superseded on 2026-11-16, before
  this spec was built:* ~~the closure through the spec that adds loss counts to `FlushResult` is
  struck through as superseded by this decision, and the new text reads that a truthy `flush()`
  means the broker acknowledged~~. The closure through the spec that adds loss counts to
  `FlushResult` stays, and the new text reads "**Decided by Andrew on 2026-11-15 and 2026-11-16.**
  Asked whether the verdict may come before the client's report, he answered 'Wait for the report
  (Recommended)': each batch's `emit` waits for the client's delivery reports within the sink's
  existing timeout, as core NATS confirms. Asked what a truthy `flush()` then means, he answered
  'Truthy, counted (Recommended)': every event was acknowledged or counted. **Closed by** SPEC-094
  building that, which turns the `verdict-before-refusal` and `verdict-before-indefinite-report`
  rows of `tests/test_conformance.py` green, and by a decision on whether a full client queue is
  retried."
- §12's finding-22 item is struck through as closed by SPEC-092, with the totals above, and says
  that `unannounced` asks for a loss line per batch rather than for the counts the lines name to
  add up to the events, since the HTTP family names requests.
- `docs/invariants.md`'s *Guarded* line of each promise FR-003 serves names
  `tests/test_conformance.py`.
- `docs/component-inventory.md` gains rows for `tests/conformance_classes.py`,
  `conformance_backend.py`, `conformance_doubles.py` and `conformance_pins.py`, as SPEC-085's
  `tests/model_*.py` helpers have (completion ritual).
- One decision in `docs/decisions/sink-delivery/`, written by the completion ritual, whose fence
  states this spec's rule: every shipped sink's acknowledgement class is declared per constructor
  variant in `tests/conformance_classes.py`, never on the sink; every row is judged by what its fake
  backend holds against the fates the library reported, with "ambiguous" an outcome of its own read
  from the engine's verdicts and a check that refuses it where the call was knowable; a red row is
  pinned to its §12 item or is a departure quoting its record; and a library-side count of an
  ambiguous outcome is not added here.

No release-notes entry is owed: nothing a caller can see changes (`completion-ritual.md` step 6).

#### Acceptance Criteria:

- [ ] `PINS` holds the twelve pins and `DEPARTURES` the seven departures above, and a test asserts
      as literals 256 rows, 74 pinned, 12 departures and 170 that must pass.
- [ ] A pinned row is an expected failure only at its pin's exact verdict, and a departure row
      passes only at its departure's exact verdict; any other outcome of either fails the suite.
- [ ] `register_faults` over the repository's files returns no line: every pinned and departure row
      is a generated row in one entry only, every pin's item is the bold lead of an open bullet
      between `### Open` and `### Resolved` read as a paragraph, and every departure's phrase is in
      its file after whitespace is collapsed.
- [ ] Standing fixtures hold `register_faults` to its failure text for a row in two entries, a row
      the table does not generate, a pin item misspelled, a pin item whose bullet is struck through,
      and a departure phrase its file lacks; and to silence for a bold lead that wraps across two
      lines and for a phrase that wraps differently in its file.
- [ ] Every one of the 170 rows passes on the branch, and the six local gates are green.
- [ ] A test reads the corpus files' source and fails on `skip`, `skipif`, `importorskip` or a
      non-strict `xfail`.
- [ ] `docs/architecture.md` gains the two §13 bullets and the finding-5 sentences, rewrites the
      finding-14 and finding-12 closures in the words above, strikes the finding-22 item, and its
      diff against `origin/main` touches nothing else; in `docs/invariants.md` the *Guarded* line of
      each promise FR-003 serves names the corpus; `docs/component-inventory.md` has the four rows.
- [ ] A row whose verdict at build time differs from this spec's is recorded in the delivery doc
      with its verdict and reported to the owner before a pin or departure is added, changed or
      removed.
- [ ] This FR serves no invariant: it is the register of where the corpus is red and the gate on
      that register; the promises are FR-003's.

### FR-005: At three older trees the corpus reports the defects fixed since, and nothing else

#### Description:

A gate that has only ever met today's code has not shown that it can see a defect. Three trees hold
defects the corpus should report: `a0f44b5f` (`v1.2.0`), and the parents of the commits that landed
SPEC-082 (`9b54b370`) and SPEC-084 (`a2cd4b36`). Before SPEC-084, a stream that took a line and then
raised had the whole batch written again; before SPEC-082, a file sink also counted as lost a batch
the worker's retry then delivered (finding 13). Each tree is exported with `git archive <ref> src
tests`, and the branch's corpus is run against it with the `ledger` check off, since those trees
have no ledger. The comparison is with the verdicts this spec records at `6bd0b66e`, not the branch
head's: the specs this one depends on change the tree between the two, and a row they move is
recorded in the delivery doc and carried into the comparison.

#### Acceptance Criteria:

- [ ] Against `a2cd4b36^`, `StdoutSink-taken-then-raised` and `StderrSink-taken-then-raised` read
      `duplicate`, and every other row reads as at `6bd0b66e`, apart from rows the delivery doc
      records as moved by a dependency (invariants 2 and 7).
- [ ] Against `9b54b370^` and against `a0f44b5f`, those two rows read `duplicate`,
      `FileSink-refused-once` and `RotatingFileSink-refused-once` read `lost-but-held`, and every
      other row reads as at `6bd0b66e`, with the same allowance (invariants 2 and 7).
- [ ] The delivery doc records each run, with `log_foundry.__file__` under its export (invariants 2
      and 7).

---

## Data Model

```python
# tests/conformance_classes.py — data only; imports nothing from log_foundry at module scope
AckClass = Literal[
    "keyed-reply", "positional-reply", "request-reply", "transactional", "local-write",
    "socket-stream", "per-event-ack", "client-buffered", "in-process", "wrapper",
]
Fault = Literal[
    "clean", "refused-once", "refused-always", "partial", "taken-then-raised", "reply-lost",
    "peer-closed", "no-verdict-shape", "never-acknowledged", "report-indefinite", "autocommit",
    "caller-code-raises",
]

@dataclass(frozen=True, kw_only=True)
class Variant:
    sink: str                      # the class name under sinks/
    variant: str                   # "" where the class has one variant
    ack: AckClass
    build: str                     # the name of its builder in tests/conformance_doubles.py
    picks: Mapping[str, object] = MappingProxyType({})   # constructor arguments that select it
    wraps: str | None = None       # a wrapper's inner variant, "HTTPSink" or "CallbackSink"
    skips: Mapping[Fault, str] = MappingProxyType({})   # fault -> reason
    only: tuple[Fault, ...] = ()   # a variant that exists for one fault

VARIANTS: tuple[Variant, ...]                   # 53
ABSTRACT: Mapping[str, str]                     # {"_RedisSink": reason}
PARAMETERS: Mapping[str, tuple[str, ...]]       # 39 classes, by AST
NEUTRAL: Mapping[str, frozenset[str]]           # parameters that never change the class
APPLIES: Mapping[AckClass, tuple[Fault, ...]]   # the FR-002 table

def roster_gap(derived: Iterable[str], declared: Iterable[str]) -> list[str]: ...

# tests/conformance_backend.py
Action = Literal["ok", "refuse", "partial", "taken-raise", "reply-lost", "noshape", "nack"]
Fate = Literal["landed", "lost", "ambiguous", "skipped"]
Check = Literal[
    "partition", "silent-loss", "duplicate", "lost-but-held", "unannounced",
    "ambiguous-when-knowable", "ledger",
]

class Backend:
    held: Counter[str]             # log_id -> copies durably held
    calls: list[Call]              # what each client call carried, and whether it was knowable
    def decide(self, carried: list[str], *, knowable: bool | None = None) -> Action: ...
    def take(self, carried: list[str]) -> None: ...

@dataclass(frozen=True, kw_only=True)
class Report:
    fates: Mapping[str, Fate] | None       # None for a sink with no deliver
    landed: int; lost: int; ambiguous: int; late: int
    batch_losses: tuple[tuple[int, int], ...]   # per batch: events lost or ambiguous, its lines
    late_lines: int
    disagreement: str | None               # a run's fates against that run's Delivery
    ledger: Mapping[str, int] | None       # the row's moves of SPEC-088's terms

def fate_of(outcome: object) -> Fate: ...   # the one reading of a round's outcome
def judge(events: list[str], backend: Backend, report: Report) -> tuple[Check, ...]: ...

# tests/conformance_doubles.py — the client doubles, the driver stubs and each variant's builder
DRIVERS: Mapping[str, Mapping[str, tuple[str, ...]]]   # module -> class -> bases
def install_drivers(monkeypatch: pytest.MonkeyPatch) -> None: ...

# tests/conformance_pins.py
@dataclass(frozen=True, kw_only=True)
class Pin:
    name: str
    rows: tuple[str, ...]          # row ids, "<sink>[<variant>]-<fault>"
    verdict: tuple[Check, ...]
    item: str                      # the bold lead of an open §12 bullet

@dataclass(frozen=True, kw_only=True)
class Departure:
    name: str
    rows: tuple[str, ...]
    verdict: tuple[Check, ...]
    record: tuple[str, str]        # (file, a phrase quoted from it verbatim)

PINS: tuple[Pin, ...]              # 12
DEPARTURES: tuple[Departure, ...]  # 7

def register_faults(
    rows: Iterable[str], pins: Iterable[Pin], departures: Iterable[Departure],
    texts: Mapping[str, str],
) -> list[str]: ...
```

---

## File & Folder Structure

```
tests/
├── conformance_classes.py       # new: AckClass, Fault, Variant, VARIANTS, ABSTRACT, PARAMETERS,
│                                #      NEUTRAL, APPLIES, roster_gap
├── conformance_backend.py       # new: Backend, the fault scripts, fate_of, judge, the driver
├── conformance_doubles.py       # new: the client doubles, DRIVERS and install_drivers, the builders
├── conformance_pins.py          # new: Pin, Departure, PINS, DEPARTURES, register_faults
├── test_conformance.py          # new: one test per row, strict at its pin's or departure's verdict
└── test_conformance_gate.py     # new: the literals, the roster, PARAMETERS, the builders, and the
                                 #      standing fixtures of judge, roster_gap and register_faults
docs/
├── architecture.md              # §12 finding-5, -12 and -14 text and finding-22 struck; §13 two bullets
├── invariants.md                # Guarded lines of invariants 2 and 7
├── component-inventory.md       # four rows, by the completion ritual
└── decisions/sink-delivery/     # one decision, by the completion ritual
```

`docs/audits/2026-11-14-probes/sinks/spec092_*.py` is the prototype, committed with this spec: the
corpus, the doubles, the variant table and `init_parameters`, a runner and the mutant runner.

## Implementation Phases

### Phase 1: The backend, the assertion and two classes (FR-002, FR-003)

- `Backend`, the fault scripts, `fate_of`, `judge`, the public-path driver and the network guard,
  with `judge`'s standing corpus.
- The driver stubs, the `keyed-reply` and `request-reply` doubles and rows, and the `ledger` check
  against SPEC-088's terms as built.

### Phase 2: Every class, the roster and the parameters (FR-001, FR-002)

- The other eight classes' doubles and rows, `VARIANTS`, `ABSTRACT`, `PARAMETERS`, `NEUTRAL`,
  `roster_gap` and their fixtures.
- The runtime measurement.

### Phase 3: The register, the mutants and the record (FR-003, FR-004, FR-005)

- `PINS`, `DEPARTURES`, `register_faults` and its fixtures; the check mutants and the three older
  trees.
- The §12, §13, invariants and inventory edits; the `workflow_dispatch` run of `integration.yml`,
  owed in the PR body until green.

## Evidence

Every number here was taken at `6bd0b66e` on macOS with Python 3.13.1 and no extra installed, by
the prototype under `docs/audits/2026-11-14-probes/sinks/`, run from the repository root as its
docstrings say. The prototype is the build's corpus without its pytest wrapping, its register gates
and its `ledger` check, and it predates SPEC-088 to SPEC-091's builds, which the build re-measures
against.

**The rows.** 256 rows over 53 variants: `keyed-reply` 28, `positional-reply` 35, `request-reply`
60, `transactional` 16, `local-write` 16, `socket-stream` 28, `per-event-ack` 8, `client-buffered`
23, `in-process` 10, `wrapper` 32. 170 green and 86 red, the same 86 at the same verdicts in eight
runs of `spec092_run.py` on Python 3.13.1 (a one-minute load of 7.6 read just after them), and in
three on Python 3.12.12, each exiting 0. The red rows are the tables in FR-004, row for row. No row trips the
network guard and every builder builds the class its entry names. Rows worth naming among the green:
`SQSSink[fifo]` `taken-then-raised` and `reply-lost` (the FIFO queue's de-duplication absorbs the
resend), `HoneycombSink` and `MongoDBSink` `no-verdict-shape` (read `UNKNOWN`, counted and
announced), `NATSSink[core]-never-acknowledged` (the confirming round trip counts the batch), the
`taken-then-raised` rows of both `PostgresSink` connection variants (rolled back; the owned one
reconnects and lands every event once, the borrowed one counts them lost), and every `local-write`
row (SPEC-082 and SPEC-084).

**Owned against borrowed.** With the Postgres and RabbitMQ doubles losing a connection for good,
the `owned` variant reconnects and resends, so `PostgresSink[owned]-reply-lost` and
`RabbitMQSink[owned]-taken-then-raised` read `duplicate`; the `borrowed` variant cannot reconnect,
so the same faults read `lost-but-held`. An earlier double that kept a dropped borrowed connection
usable had put the borrowed rows at `duplicate`.

**The findings the corpus was asked to catch** each show as red: finding 1 at `peer-closed-tcp`,
finding 5 at `reply-resent` and `reply-counted-lost`, finding 11 at `sdk-discard-counted-sent`,
finding 12 at the two `verdict-before-*` pins (eight late `failed` counts at `close()` over eight
events the worker had counted delivered), finding 14 at `reply-without-verdict`, finding 15 at the
two `autocommit` pins, and RabbitMQ at `publish-without-confirms`.

**The driver branch.** With `driver_classes` answering `()` everywhere, 21 rows moved: the
`taken-then-raised` and `reply-lost` rows of `SQSSink` (both variants), `SNSSink`, `KinesisSink`,
`FirehoseSink`, `ClickHouseSink`, `AzureEventHubsSink`, both Redis sinks and both `PostgresSink`
connection variants, and `RabbitMQSink[owned]-taken-then-raised`, most from `duplicate` to
`lost-but-held`, since an unrecognised error is refused at once rather than retried.

**Cost.** `spec092_run.py` runs all 256 rows in 0.37 to 0.49 s. As 256 parametrized pytest items in
a scratch copy of the tree, with `tests/conftest.py`'s fixtures, 0.50 to 0.55 s under `-n 0` in
three runs at a load near 3.8. FR-002's 10 s leaves the build room for the register, the gate and
the `ledger` reads, and for a CI runner several times slower than this machine.

**The checks.** Removing each check from `judge` in turn (`spec092_run.py --mutants`) moves 4 rows
for `partition`, 21 for `silent-loss`, 55 for `duplicate` and 11 for `lost-but-held`, and none for
`unannounced` or `ambiguous-when-knowable`, which no row on today's code needs. So each of those two
was run against a planted mutant (`spec092_mutants.py`, each in a `git archive` export): with
`_diag.lost` returning at once, 89 rows moved to include `unannounced` (75 of them green before),
among them `SQSSink[standard]-refused-always`; with `_RedisSink._round` answering `UNKNOWN` for every
raise, the `refused-once` and `refused-always` rows of both Redis sinks went red at
`ambiguous-when-knowable` while their `taken-then-raised` and `reply-lost` rows went green, which is
the trade that check exists to refuse. Two harness mutants: `fate_of` reading `REJECTED` as landed
moved 16 rows (13 of them green before), every one to a verdict that includes `partition` from the
comparison with `Delivery`; `duplicate` firing at one copy moved 148 rows, 119 of them green before
and the rest already red for another check, to include `duplicate`. The `ledger` check was not
prototyped: SPEC-088 is not built at `6bd0b66e`.

**A summed line count was tried and rejected for `unannounced`.** Comparing the counts the loss
lines name with the events lost made `MultiSink[over-http]-refused-always` red, because its child
names `lost 1 request(s)` for eight events and `MultiSink`'s absorbed line names no count. The
units differ by record, not by defect, so the check asks for a line per batch.

**Older trees.** Against an export of `a2cd4b36^`, 88 red: the 86 and the two stream rows at
`duplicate`. Against `9b54b370^` and `a0f44b5f`, 90 red: those, and the two file rows at
`lost-but-held`. Every other row read as at `6bd0b66e`, so every audit finding the corpus pins was
already red at `v1.2.0`.

**What the prototype could not settle.** The `ledger` check, for the reason above. The verdicts with
every extra installed, which FR-002 owes to an `integration.yml` run. And whether each double's
reply shape and exception class are the real client's: each follows what the sink's own reader,
retry predicate and existing tests expect, and the integration suite remains where real shapes are
measured.

## Revision history

- Revised the same day after the fresh-context spec review (18 findings, all accepted) and two
  decisions Andrew made on 2026-11-15, quoted under *Functional Requirements*. The Postgres and
  RabbitMQ doubles now lose a connection for good, which split each into `owned` and `borrowed`
  variants and moved the borrowed rows from `duplicate` to `lost-but-held`; Kafka's and Pub/Sub's
  definitive and indefinite reports became two faults; the Sentry SDK's transport drop became a
  departure; the doubles raise their drivers' classes through stubs; `unannounced` became per batch;
  the reconstruction reads each run on its own; the gates became pure functions with standing
  fixtures; departure records became quoted phrases (`NullSink`'s had cited SPEC-026 FR-002, which
  does not mention it); and FR-005 compares with the verdicts at `6bd0b66e`. Rows went from 243 to
  256, red from 80 to 86, pins from 69 rows to 74 and departures from 11 rows to 12.
- Edited on 2026-11-15 by SPEC-093's revision, text only. The acknowledgement-fixes spec this spec
  named became two: SPEC-093 builds decision A and fixes findings 1, 11 and 14 and adds RabbitMQ's
  confirms, and SPEC-094 builds decision B (finding 12). So every sentence that named "the
  acknowledgement-fixes spec" now names the one that does the work, and the example in FR-001 names
  `RabbitMQSink`'s opt-in confirmed variants, since Andrew chose on 2026-11-15 that confirms are off
  by default. No requirement, row or total changed.
- Edited on 2026-11-16 by SPEC-094's revision, text only. Decision B's gloss, "so a truthy `flush()`
  means the broker acknowledged", is struck at both sites that state it, with the reading Andrew
  gave on 2026-11-16 ("Truthy, counted (Recommended)") beside it, and the §12 text FR-004 writes no
  longer strikes the `FlushResult` closure, which that answer keeps. The `never-acknowledged`
  fault's Kafka and Pub/Sub examples become `10` and `InvalidArgument`, because SPEC-094 reads a
  report as definitive only when no attempt could have written the event. No requirement, row or
  total changed.
