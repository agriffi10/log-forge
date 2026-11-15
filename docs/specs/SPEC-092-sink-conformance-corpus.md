---
type: Spec
id: SPEC-092
title: "A Sink Conformance Corpus Keyed on Acknowledgement Class, with an Explicit Ambiguous Outcome"
description: "Adds one test corpus that runs every shipped sink through the fault scripts of its acknowledgement class against a stateful fake backend, and judges each row by one assertion: what the backend holds, against what the sink and its catcher reported, event by event, with an ambiguous outcome of its own, so that a sink which loses, duplicates or miscounts under a fault fails a test, and every row red today is pinned to the open item that owns it."
spec_status: Draft
depends_on: [SPEC-026, SPEC-041, SPEC-049, SPEC-061, SPEC-069, SPEC-071, SPEC-072, SPEC-073, SPEC-074, SPEC-075, SPEC-079, SPEC-082, SPEC-084, SPEC-085, SPEC-088]
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
  as data in `tests/`, held to the sink roster the tree derives (FR-001).
- One fake backend and one fault script per class, a client double per sink that translates only
  call and reply shapes, every shipped sink covered with no Docker and no network, and the corpus's
  cost measured (FR-002).
- One assertion of seven checks, the ambiguous outcome read from the engine's own verdicts, the
  per-event reading proved against the engine's `Delivery`, and a mutant for every check (FR-003).
- The register: 69 rows pinned to nine open items, 11 permitted departures each naming its record,
  the gate on that register, and the record the build changes: two `docs/architecture.md` §13
  constraints, one sentence in one §12 item, the finding-22 item closed, the *Guarded* lines of
  invariants 2 and 7, and one decision (FR-004).
- Proof that the corpus bites: at three older trees it reports the stream and file sinks' fixed
  duplicates and miscounts, and nothing else moves (FR-005).

### Out of Scope

- **Fixing any sink.** Every red row is pinned, never fixed. The writes nothing acknowledges
  (findings 1, 11, 12 and RabbitMQ's confirms) are the next sink spec of this arc, which uses these
  classes and flips these pins.
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
`docs/audits/2026-11-14-probes/sinks/spec092_*.py`, which runs every row the build's corpus runs.

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

**A class belongs to a constructor variant, not to a Python class**, because three sinks pick their
backend at construction: `LogstashSink` sends over HTTP or a socket, `SentrySink` through the SDK or
its own HTTP request, and `NATSSink` to JetStream, which acknowledges each publish, or to core NATS,
which confirms a batch by a round trip after it when the sink connected the client itself
(SPEC-061). A variant is named only where the class or a
departure (FR-004) differs, so 39 classes give 51 variants:

| Class | Variants |
|---|---|
| `keyed-reply` | `SQSSink` `standard` and `fifo`, `SNSSink`, `MongoDBSink` (its `writeErrors` name each failure by `index`) |
| `positional-reply` | `KinesisSink`, `FirehoseSink`, `ElasticsearchSink`, `OpenSearchSink`, `HoneycombSink` |
| `request-reply` | `HTTPSink`, `DatadogSink`, `SplunkHECSink`, `NewRelicSink`, `LokiSink`, `OTLPSink`, `LogstashSink` `http`, `SentrySink` `http`, `ClickHouseSink`, `AzureEventHubsSink`, `RedisStreamsSink`, `RedisListSink` |
| `transactional` | `PostgresSink` and its `autocommit` variant, `SQLiteSink` and its `autocommit` variant |
| `local-write` | `FileSink`, `RotatingFileSink`, `StdoutSink`, `StderrSink` |
| `socket-stream` | `SyslogSink` `tcp` and `udp`, `LogstashSink` `tcp` and `udp`, `RabbitMQSink` (no publisher confirms) |
| `per-event-ack` | `NATSSink` `jetstream` and `jetstream-injected` |
| `client-buffered` | `NATSSink` `core` and `core-injected`, `KafkaSink`, `GooglePubSubSink`, `SentrySink` `sdk` |
| `in-process` | `CallbackSink`, `LoggingSink`, `MemorySink`, `NullSink` |
| `wrapper` | `FilteringSink` and `TransformSink` each `over-http` and `over-callback`, `MultiSink` `over-http`, `_FactorySink` `over-http` |

`_FactorySink` is private but is what `configure(sink_factory=…)` installs (SPEC-079), so it has a
row.
`_RedisSink` is the private base of the two Redis sinks, which no caller builds, and is listed in
`ABSTRACT` with that reason. `base.Sink` is the Protocol.

**The data lives in `tests/conformance_classes.py`, not on the sink.** A public attribute would add
to the surface of 39 classes for good (invariant 10 lets it grow, never shrink); it could not say
that one `LogstashSink` is a `socket-stream` and another a `request-reply` without becoming an
instance property; and the corpus is its only reader, since a third-party sink is not in it. The
next sink spec edits rows here when it changes what a sink can know (`RabbitMQSink` to
`per-event-ack` with confirms, for one).

**A new sink cannot ship without a class.** The roster is the one `tests/test_public_surface.py`
derives from the source, every class under `sinks/` that defines or inherits `emit`, read without
importing so an absent extra cannot hide one.

#### Acceptance Criteria:

- [ ] `VARIANTS` in `tests/conformance_classes.py` holds the 51 variants above, each naming exactly
      one of the ten classes, and a test asserts both literals (invariants 2 and 7).
- [ ] A test asserts that the set of class names in `VARIANTS` plus `ABSTRACT` equals the roster
      `_sink_classes_with_emit()` derives minus `Sink`, naming each name on either side alone in its
      failure (invariants 2 and 7).
- [ ] That test fails naming `ExampleSink` against a copy of `src/log_foundry/sinks/` holding one
      more module defining `class ExampleSink` with an `emit`, and fails naming `GhostSink` with a
      `VARIANTS` entry for a class the tree does not define; both are recorded in the delivery doc
      (invariants 2 and 7).
- [ ] A test asserts that every `ABSTRACT` entry is a class no other `VARIANTS` entry builds and
      that at least one `VARIANTS` entry builds a subclass of it (invariants 2 and 7).
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
| `peer-closed` | a clean first batch; the peer then closes, the second batch's first three writes return having reached nobody, the fourth raises `BrokenPipeError`, and a new connection takes the rest (`udp`: no listener, nothing taken, nothing raised) |
| `no-verdict-shape` | the first call takes its first half and replies with no verdict: `{}`, or for `MongoDBSink` a `writeErrors` that is not a list |
| `never-acknowledged` | the client takes every event and its later report says each failed: a Kafka delivery error, a Pub/Sub future that raises, a Sentry `capture_event` that returns `None`, a core NATS PING never answered |
| `autocommit` | each statement is durable at once, and the second raises once |
| `caller-code-raises` | the wrapper's predicate or function raises for one event |

| Class | Faults run |
|---|---|
| `keyed-reply`, `positional-reply` | `clean`, `refused-once`, `refused-always`, `partial`, `taken-then-raised`, `reply-lost`, `no-verdict-shape` |
| `request-reply` | `clean`, `refused-once`, `refused-always`, `taken-then-raised`, `reply-lost` |
| `transactional` | those five and `autocommit`, which only the `autocommit` variants run; `SQLiteSink` skips `reply-lost`, since an in-process database has no reply to lose |
| `local-write` | `clean`, `refused-once`, `refused-always`, `taken-then-raised` |
| `socket-stream` | those four and `peer-closed`; `udp` skips `taken-then-raised`, since a datagram is sent whole or not at all |
| `per-event-ack` | `clean`, `refused-once`, `refused-always`, `reply-lost` |
| `client-buffered` | `clean`, `refused-once`, `refused-always`, `never-acknowledged` |
| `in-process` | `clean`, `refused-once`, `refused-always`, `taken-then-raised`; `MemorySink` and `NullSink` run `clean` only, since neither calls anything that can fail |
| `wrapper` | its inner variant's class's faults, and `caller-code-raises` for `FilteringSink` and `TransformSink` |

`partial` does not apply where no reply names an event; `peer-closed` only where a connection is
held between batches; `no-verdict-shape` only where the reply is the verdict; `never-acknowledged`
only where a report follows the hand-off; `autocommit` only to a transaction. That is 243 rows.

**Knowable or not.** A call's outcome is *unknowable* when the sink cannot tell what its backend
kept: `taken-then-raised`, `reply-lost`, `peer-closed` and `no-verdict-shape`, and a text stream's
raising `write` and a core NATS PING never answered whatever the fault, because neither contract
says what was kept. Two doubles say what was kept and make `taken-then-raised` knowable: a file's
`write` returns the bytes it took, and a transaction that raises before its commit is rolled back.

**The driver.** A row runs the public path the worker serves: `configure(sink=…)`, one `@trace`d
call per batch logging six events inside its span (eight with `span.start` and `span.end`, one
submission), `flush()` between the two batches of `peer-closed`, and `shutdown(timeout=None)`. The
exit drain delivers with the stop event set, so every backoff returns at once and no retry is
declined, which shortens a row and changes no verdict (*A shutdown shortens a wait; it must never
skip work*), and `shutdown()` closes the sink, which is where a client buffer's late report lands.

**Extras.** No row needs an extra. A sink imports its client through `sinks/_extras.import_extra`
only when it builds that client itself (SPEC-071), so each SDK, driver and socket sink gets an
injected client double, or a patched seam where it builds its own or needs the extra beside one: `_socket._make_tcp` and `_make_udp`,
`eventhubs._event_data_cls`, and a `nats` module whose `connect` returns the double, which is how
`tests/nats_engine_matrix_harness.py` builds a sink that owns its client. A row would be skipped
only where its class cannot be faked, and none is.

**Why `tests/sink_rigs.py` does not serve.** Its `RIGS` table (`:725` at `6bd0b66e`; the audit's
`:700` was its line at `a0f44b5f`) builds 20 record-building sinks over doubles that fail whole
calls only (`State.call`'s `error`, `fail_next` and `down`), and leaves out the nine HTTP-family
sinks, `NATSSink`, `SentrySink`, `LoggingSink`, `CallbackSink`, `MemorySink`, `NullSink` and the
wrappers. Since the audit, `tests/test_file_sink_partial_write.py` and
`tests/test_stream_sink_partial_write.py` take a write partway under a real file-size limit, and
`tests/test_sinks_nats_engine_matrix.py` scripts NATS publishes and acknowledgements, each for its
own sinks; no double compares what a backend holds with what was counted, for any sink.

#### Acceptance Criteria:

- [ ] The corpus generates exactly the 243 rows the tables above give, and a test asserts the
      literal and that every row's faults are its class's minus the variant's listed skips
      (invariants 2 and 7).
- [ ] Every row runs in the ordinary `CI / test` job, with no Docker, no network connection and no
      extra installed, and none is skipped (invariants 2 and 7).
- [ ] For every fault a wrapper variant and its inner variant both run, the wrapper's row has the
      inner's verdict (`HTTPSink`'s for `over-http`, `CallbackSink`'s for `over-callback`)
      (invariants 6 and 7).
- [ ] The corpus's test file runs under `pytest -n 0` in under 10 s on the build machine, measured
      three times and recorded with the one-minute load in the delivery doc (invariant 2).
- [ ] Every patched seam is set with `monkeypatch.setattr`, so a renamed seam fails the row rather
      than leaving it unpatched (invariants 2 and 7).
- [ ] **Owed until a green run:** the corpus's verdicts are the same in `integration.yml`'s
      unit-suite step, with every extra installed, triggered by `workflow_dispatch` on the PR's
      branch, since its path filter does not trigger on `tests/` (invariants 2 and 7).

### FR-003: One assertion judges every row, and every check fails a mutant planted where it must fire

#### Description:

**A row's report.** Each event of a row gets one **fate**, read from what the library did:

- An event of a run the engine made (SPEC-072; the HTTP family since SPEC-073, the SDK, driver,
  socket, file and stream sinks since SPEC-074, SPEC-082 and SPEC-084, the wrappers since SPEC-075)
  is read from the rounds of that run, captured by wrapping the `deliver` callable `_engine._run`
  is handed, which both the worker and the inline adapter call.
  Its fate is its outcome in the last round that carried it: `ACCEPTED` is **landed**; `REJECTED`,
  `DROPPED`, `RETRYABLE` and a round that raised are **lost**; `UNKNOWN` and an unreadable verdict
  are **ambiguous**; `SKIPPED` is **skipped**. Every event of a run that failed is lost, because its
  catcher owns it. The judged sink is the row's sink, or the engine child behind a wrapper that
  offers no `deliver` (`MultiSink`, `_FactorySink`).
- An event of a sink with no `deliver` is read per batch from `Worker._emit`, wrapped to see each
  batch: an abandoned batch is lost; one whose `emit` returned is landed less what its
  `losses()` moved during that `emit`, which is lost.
- A count that moves after a batch's verdict was in, at a later batch or at `close()`, is **late**.

**The ambiguous outcome needs no new counter here.** Today the library expresses it as `UNKNOWN`,
which the engine counts in `failed_events` (an upper bound), several sinks also in
`dropped_unadjudicated`, and SPEC-088's ledger in `sink_lost` beside definite losses; nothing on
`health()` tells it apart. The corpus reads it per event from the rounds and proves that reading
against the engine's own `Delivery` (`unknown`). A library-side count of "may have landed" or "may
have duplicated" belongs to the spec that adds idempotency keys, and the corpus's one reading
function, `fate_of`, is the place it would be read.

**The assertion.** A row fails each check whose condition holds, and its verdict is the tuple of
those checks in this order:

| Check | Fails when |
|---|---|
| `partition` | the fates do not cover the row's events exactly once, a run's fates disagree with its `Delivery`'s counts, or a count is late |
| `silent-loss` | an event reported landed is not held (for a sink with no `deliver`: more reported landed than held) |
| `duplicate` | the backend holds an event more than once, since nothing counts a possible duplicate today |
| `lost-but-held` | an event reported lost or skipped is held (for a sink with no `deliver`: more counted than not held) |
| `unannounced` | an event is lost or ambiguous and the row wrote no `log-foundry: lost N` line |
| `ambiguous-when-knowable` | an event reported ambiguous was carried only by calls whose outcome was knowable |
| `ledger` | `health().events.unaccounted` is not 0 at the row's end, or the row's moves of `delivered`, `skipped`, `sink_lost` and `batch_lost` differ from its fates, as below |

For `ledger`, an engine run's landed events are its `delivered`, its skipped its `skipped`, a
settled run's lost and ambiguous its `sink_lost`, and a failed run's events its `batch_lost`; a
batch whose `emit` returned is all `delivered` and an abandoned one all `batch_lost`, because
SPEC-088's ledger ends where `emit` returns.

`unannounced` asks for a line, not a count of events: the HTTP family names its losses in
requests, its recorded unit, so a `MultiSink` child that failed every request names `lost 1
request(s)` for eight events, and a summed count would call that row red for its unit.
`ambiguous-when-knowable` is what keeps "ambiguous" from being a way out: a sink that called every
raise ambiguous would pass `taken-then-raised` and `reply-lost`, and this check fails it on
`refused-once`.

#### Acceptance Criteria:

- [ ] A row's verdict is the tuple of the checks above that fail, in the table's order, and a test
      runs `judge` over a hand-built corpus of reports holding at least one case that fails each
      check alone and one that fails none (invariants 2 and 7).
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
- [ ] With the reading of `REJECTED` changed to landed in `fate_of`, `SQSSink[standard]-partial`
      reads `partition` among its checks, which only the comparison with `Delivery` can give it
      (invariants 2 and 7).
- [ ] With `duplicate`'s condition changed from "held more than once" to "held at least once",
      every green `clean` row reads `duplicate` (invariants 2 and 7).
- [ ] With the engine's increment of `sink_lost` at a settled run removed, in an export of the
      branch head, `SQSSink[standard]-partial` reads `ledger` (invariant 2).
- [ ] No corpus test carries SPEC-088's `ledger_exempt` marker, and every row balances in its
      autouse fixture (invariant 2).

### FR-004: The red rows are pinned, the departures are rows with their records, and the register cannot go quiet

#### Description:

At `6bd0b66e`, 80 of the 243 rows are red, the same 80 at the same verdicts in eight runs. This
spec fixes that set. A row red at build time and not listed here, or listed and green, is a finding
the build reports before it changes anything.

**A pin** names rows, the exact verdict each has, and its **item**: the bold lead of a bullet
under `### Open` in `docs/architecture.md` §12, as SPEC-085 pinned its cells (*A red adversarial
cell is pinned to its open item at its exact verdict, and a pin moves only after the owner hears
why*). A pinned row runs and is reported as an expected failure only at its pin's verdict; a pass
or any other verdict fails the suite. **Nothing is withheld:** SPEC-085 withheld a cell that
repeats a pin because each cost seconds of waiting, and no row here costs more than milliseconds,
while a wrapper row that repeats its inner sink's pin is what holds the wrapper to it (FR-002).

| Pin | Rows | Verdict | Item |
|---|---|---|---|
| `reply-resent` | `taken-then-raised` and `reply-lost` of `SQSSink[standard]`, `SNSSink`, the five `positional-reply` variants, the twelve `request-reply` variants and the four `wrapper` variants `over-http`; `PostgresSink-reply-lost`; `taken-then-raised` of `SyslogSink[tcp]`, `LogstashSink[tcp]` and `RabbitMQSink` (50) | `duplicate` | "A send whose reply is lost is sent again, and the resend is neither counted nor keyed" (finding 5) |
| `resend-refused-by-id` | `MongoDBSink` `taken-then-raised` and `reply-lost` (2) | `lost-but-held` | the same item |
| `reply-without-verdict` | `no-verdict-shape` of `SQSSink[standard]`, `SQSSink[fifo]`, `SNSSink`, `KinesisSink`, `FirehoseSink` (5) | `silent-loss` | "An AWS batch reply that names no failure is read as full delivery" (finding 14) |
| `bulk-without-errors` | `no-verdict-shape` of `ElasticsearchSink`, `OpenSearchSink` (2) | `silent-loss` | "`ElasticsearchSink` reads a `2xx` object without a truthy `errors` as success, unverified." |
| `peer-closed-tcp` | `peer-closed` of `SyslogSink[tcp]`, `LogstashSink[tcp]` (2) | `silent-loss` | "A TCP `SyslogSink` or `LogstashSink` counts a frame as accepted when `sendall` returns, so frames written after the peer closed vanish uncounted" (finding 1) |
| `publish-without-confirms` | `RabbitMQSink-peer-closed` (1) | `silent-loss` | "`RabbitMQSink` publishes without confirms, so an unroutable message or one written to a dead socket reads as accepted" |
| `jetstream-ack-lost` | `reply-lost` of `NATSSink[jetstream]`, `NATSSink[jetstream-injected]` (2) | `lost-but-held` | "A timed-out JetStream publish is not retried, and cannot safely be" |
| `verdict-on-enqueue` | `never-acknowledged` of `KafkaSink`, `GooglePubSubSink` (2) | `partition`, `silent-loss` | "`KafkaSink` and `GooglePubSubSink` give their verdict when the client queues a message, not when the broker answers" (finding 12) |
| `sdk-discard-counted-sent` | `SentrySink[sdk]-never-acknowledged` (1) | `silent-loss` | "`SentrySink`'s SDK backend counts an event the SDK discarded as sent" (finding 11) |
| `autocommit-postgres` | `PostgresSink[autocommit]-autocommit` (1) | `duplicate` | "An injected autocommit connection breaks the database sinks' all-or-nothing write" (finding 15) |
| `autocommit-sqlite` | `SQLiteSink[autocommit]-autocommit` (1) | `duplicate`, `lost-but-held` | the same item |

That is 69 pinned rows. `GooglePubSubSink`'s half of finding 12, which the audit read and did not
run, is now run. The five `reply-without-verdict` rows wait on finding 14's item, which is closed by
deciding whether a reply carrying neither `Failed` nor `Successful`, or neither a count nor records,
is unadjudicated; that decision is not this spec's, and whichever way it is made the rows move as a
pin moves.

**A departure** is a red row the library permits: the row passes only at the departure's exact
verdict, and the departure names its **record**, a settled source that states the behaviour.

| Departure | Rows | Verdict | Record |
|---|---|---|---|
| `sqlite-failed-before-raise` | `SQLiteSink` `refused-once` and `taken-then-raised` (2) | `lost-but-held` | `docs/invariants.md` invariant 2's recorded exception: the batch size `SQLiteSink` adds to `failed` before it raises (SPEC-069 FR-004) |
| `udp-fire-and-forget` | `peer-closed` of `SyslogSink[udp]`, `LogstashSink[udp]` (2) | `silent-loss` | a `docs/architecture.md` §13 constraint this spec adds (below) |
| `injected-nats-unprobed` | `NATSSink[core-injected]-never-acknowledged` (1) | `silent-loss` | §13: "`NATSSink` cannot drive a client it did not connect, so an injected client's liveness is the caller's." |
| `callable-raises-after-keeping` | `taken-then-raised` of `CallbackSink`, `FilteringSink[over-callback]`, `TransformSink[over-callback]` (3) | `duplicate` | a §13 constraint this spec adds (below) |
| `handler-absorbs` | `LoggingSink` `refused-once` and `refused-always` (2) | `silent-loss` | SPEC-049 FR-007: a handler failing through the default `handleError` is unobservable to `LoggingSink` |
| `null-discards` | `NullSink-clean` (1) | `silent-loss` | SPEC-026 FR-002: `NullSink` discards by design and exposes no `losses()` |

That is 11 departure rows and 163 that must pass.

**The record the build changes**, and nothing else in either file:

- §13 *Known constraints* gains two bullets. "**A UDP datagram is fire-and-forget: a `sendto` that
  returned is all `SyslogSink` and `LogstashSink` can know.**" With no listener, every datagram is
  lost and nothing is counted, which §12's finding-1 item already calls inherent to UDP. And "**A
  caller's callable that keeps events and then raises has them resent.** `CallbackSink` (and a
  wrapper over it) cannot see what the callable kept; its raise tells the worker that nothing landed
  (invariant 7), and the worker resends the batch whole."
- §12's finding-5 item gains one sentence: `MongoDBSink`'s form of it is a resend that the unique
  `_id` refuses and that is counted as a rejection although the document landed, as *No shipped
  sink declares itself idempotent* records, held by the `resend-refused-by-id` pin.
- §12's finding-22 item is struck through as closed by SPEC-092, with the totals above.
- `docs/invariants.md`'s *Guarded* lines for invariants 2 and 7 name `tests/test_conformance.py`
  and the classes it covers.
- One decision in `docs/decisions/sink-delivery/`, written by the completion ritual, whose fence
  states this spec's rule: every shipped sink's acknowledgement class is declared per constructor
  variant in `tests/conformance_classes.py`, never on the sink; every row is judged by what its fake
  backend holds against the fates the library reported, with "ambiguous" an outcome of its own read
  from the engine's verdicts and a check that refuses it where the call was knowable; a red row is
  pinned to its §12 item or is a departure naming its record; and a library-side count of an
  ambiguous outcome is not added here.

No release-notes entry is owed: nothing a caller can see changes (`completion-ritual.md` step 6).

#### Acceptance Criteria:

- [ ] `PINS` holds the eleven pins and `DEPARTURES` the six departures above, and a test asserts as
      literals 243 rows, 69 pinned, 11 departures and 163 that must pass.
- [ ] A pinned row is an expected failure only at its pin's exact verdict, and a departure row
      passes only at its departure's exact verdict; any other outcome of either fails the suite.
- [ ] A test asserts that every pinned and departure row is a generated row and in one entry only,
      that every pin's item is the bold lead of a bullet between `### Open` and `### Resolved` in
      `docs/architecture.md`, and that every departure's record names a heading or bold lead that
      exists in the file it names.
- [ ] Every one of the 163 rows passes on the branch, and the six local gates are green.
- [ ] The delivery doc records five register mutations and the test each reddened: a row removed
      from the generated table, a row removed from a pin, a pin's item misspelled, a departure's
      record misspelled, and a pin's verdict changed.
- [ ] A test reads the corpus files' source and fails on `skip`, `skipif`, `importorskip` or a
      non-strict `xfail`.
- [ ] `docs/architecture.md` gains the two §13 bullets and the finding-5 sentence, strikes the
      finding-22 item, and its diff against `origin/main` touches nothing else; in
      `docs/invariants.md` the *Guarded* line of each promise FR-003 serves names the corpus.
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
have no ledger.

#### Acceptance Criteria:

- [ ] Against `a2cd4b36^`, `StdoutSink-taken-then-raised` and `StderrSink-taken-then-raised` read
      `duplicate`, and every other row reads as at the branch head (invariants 2 and 7).
- [ ] Against `9b54b370^` and against `a0f44b5f`, those two rows read `duplicate`,
      `FileSink-refused-once` and `RotatingFileSink-refused-once` read `lost-but-held`, and every
      other row reads as at the branch head (invariants 2 and 7).
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
    "peer-closed", "no-verdict-shape", "never-acknowledged", "autocommit", "caller-code-raises",
]

@dataclass(frozen=True, kw_only=True)
class Variant:
    sink: str                      # the class name under sinks/
    variant: str                   # "" where the class has one variant
    ack: AckClass
    build: str                     # the name of its builder in tests/conformance_doubles.py
    wraps: str | None = None       # a wrapper's inner variant, "HTTPSink" or "CallbackSink"
    skips: Mapping[Fault, str] = MappingProxyType({})   # fault -> reason
    only: tuple[Fault, ...] = ()   # a variant that exists for one fault

VARIANTS: tuple[Variant, ...]                   # 51
ABSTRACT: Mapping[str, str]                     # {"_RedisSink": reason}
APPLIES: Mapping[AckClass, tuple[Fault, ...]]   # the FR-002 table

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
    landed: int; lost: int; ambiguous: int; late: int; lines: int
    disagreement: str | None               # a run's fates against its Delivery
    ledger: Mapping[str, int] | None       # the row's moves of SPEC-088's terms

def fate_of(outcome: object) -> Fate: ...   # the one reading of a round's outcome
def judge(events: list[str], backend: Backend, report: Report) -> tuple[Check, ...]: ...

# tests/conformance_pins.py
@dataclass(frozen=True, kw_only=True)
class Pin:
    name: str
    rows: tuple[str, ...]          # row ids, "<sink>[<variant>]-<fault>"
    verdict: tuple[Check, ...]
    item: str                      # the bold lead of a §12 Open bullet

@dataclass(frozen=True, kw_only=True)
class Departure:
    name: str
    rows: tuple[str, ...]
    verdict: tuple[Check, ...]
    record: tuple[str, str]        # (file, heading or bold lead it holds)

PINS: tuple[Pin, ...]              # 11
DEPARTURES: tuple[Departure, ...]  # 6
```

---

## File & Folder Structure

```
tests/
├── conformance_classes.py       # new: AckClass, Fault, Variant, VARIANTS, ABSTRACT, APPLIES
├── conformance_backend.py       # new: Backend, the fault scripts, fate_of, judge, the driver
├── conformance_doubles.py       # new: one client double per sink, and each variant's builder
├── conformance_pins.py          # new: Pin, Departure, PINS, DEPARTURES
├── test_conformance.py          # new: one test per row, strict at its pin's or departure's verdict
└── test_conformance_gate.py     # new: the roster, the literals, judge's corpus, the register gates
docs/
├── architecture.md              # §12 finding-5 sentence and finding-22 struck; §13 two bullets
├── invariants.md                # Guarded lines of invariants 2 and 7
└── decisions/sink-delivery/     # one decision, by the completion ritual
```

`docs/audits/2026-11-14-probes/sinks/spec092_*.py` is the prototype, committed with this spec: the
corpus, the doubles, the variant table, a runner and the mutant runner.

## Implementation Phases

### Phase 1: The backend, the assertion and two classes (FR-002, FR-003)

- `Backend`, the fault scripts, `fate_of`, `judge` and the public-path driver, with `judge`'s corpus
  of hand-built reports.
- The `keyed-reply` and `request-reply` doubles and rows, and the `ledger` check against SPEC-088's
  terms as built.

### Phase 2: Every class and the roster (FR-001, FR-002)

- The other eight classes' doubles and rows, `VARIANTS`, `ABSTRACT` and the roster test.
- The runtime measurement.

### Phase 3: The register, the mutants and the record (FR-003, FR-004, FR-005)

- `PINS`, `DEPARTURES` and the register gates; the check mutants and the three older trees.
- The §12, §13 and invariants edits; the `workflow_dispatch` run of `integration.yml`, owed in the
  PR body until green.

## Evidence

Every number here was taken at `6bd0b66e` on macOS with Python 3.13.1 and no extra installed, by
the prototype under `docs/audits/2026-11-14-probes/sinks/`, run from the repository root as its
docstrings say. The prototype is the build's corpus without its pytest wrapping, its register and
its `ledger` check.

**The rows.** 243 rows over 51 variants: `keyed-reply` 28, `positional-reply` 35, `request-reply`
60, `transactional` 11, `local-write` 16, `socket-stream` 23, `per-event-ack` 8, `client-buffered`
20, `in-process` 10, `wrapper` 32. 163 green and 80 red, the same 80 at the same verdicts in eight
runs of `spec092_run.py` at one-minute loads between 1.6 and 1.9, each exiting 0. The red rows are the tables
in FR-004, row for row. Rows worth naming among the green: `SQSSink[fifo]` `taken-then-raised` and
`reply-lost` (the FIFO queue's de-duplication absorbs the resend), `HoneycombSink` and
`MongoDBSink` `no-verdict-shape` (read `UNKNOWN`, counted and announced), `NATSSink[core]-never-acknowledged`
(the confirming round trip counts the batch), and every `local-write` row (SPEC-082 and SPEC-084).

**The findings the corpus was asked to catch** each show as red: finding 1 at `peer-closed-tcp`,
finding 5 at `reply-resent` (two copies of each taken event for one lost reply, as the audit's
`probe_sqs.py` D and `probe_misc.py` 1 and 6 measured), finding 11 at `sdk-discard-counted-sent`,
finding 12 at `verdict-on-enqueue` (eight late `failed` counts at `close()` over eight events the
worker had counted delivered), finding 14 at `reply-without-verdict`, finding 15 at the two
`autocommit` pins, and RabbitMQ at `publish-without-confirms`.

**Cost.** `spec092_run.py` runs all 243 rows in 0.33 s. As 243 parametrized pytest items in a
scratch copy of the tree, with `tests/conftest.py`'s fixtures, 0.45 s under `-n 0` in each of
three runs and 1.13 s under the default `-n 12`, at a load near 1.9. FR-002's 10 s leaves the build room for the register, the gate and the
`ledger` reads, and for a CI runner several times slower than this machine.

**The checks.** Removing each check from `judge` in turn (`spec092_run.py --mutants`) moves 2 rows
for `partition`, 19 for `silent-loss`, 55 for `duplicate` and 7 for `lost-but-held`, and none for
`unannounced` or `ambiguous-when-knowable`, which no row on today's code needs. So each of those two
was run against a planted mutant (`spec092_mutants.py`, each in a `git archive` export): with
`_diag.lost` returning at once, 80 rows moved to include `unannounced`, among them
`SQSSink[standard]-refused-always`; with `_RedisSink._round` answering `UNKNOWN` for every raise,
the `refused-once` and `refused-always` rows of both Redis sinks went red at
`ambiguous-when-knowable` while their `taken-then-raised` and `reply-lost` rows went green, which is the trade that check exists to
refuse. Two harness mutants: `fate_of` reading `REJECTED` as landed moved 16 rows, every one of
them to a verdict that includes `partition` from the comparison with `Delivery`; `duplicate` firing at one copy moved 140 green
rows to `duplicate`. The `ledger` check was not prototyped: SPEC-088 is not built at `6bd0b66e`.

**A summed line count was tried and rejected for `unannounced`.** Comparing the counts the loss
lines name with the events lost made `MultiSink[over-http]-refused-always` red, because its child
names `lost 1 request(s)` for eight events and `MultiSink`'s absorbed line names no count. The
units differ by record, not by defect, so the check asks for a line.

**Older trees.** Against an export of `a2cd4b36^`, 82 red: the 80 and the two stream rows at
`duplicate`. Against `9b54b370^` and `a0f44b5f`, 84 red: those, and the two file rows at
`lost-but-held`. Every other row read as at `6bd0b66e`, so every audit finding the corpus pins was
already red at `v1.2.0`.

**What the prototype could not settle.** The `ledger` check, for the reason above. The verdicts with
every extra installed, which FR-002 owes to an `integration.yml` run. And whether each double's
reply shape is the real client's: each follows the shape the sink's own reader and its existing
tests expect, and the integration suite remains where real shapes are measured.
