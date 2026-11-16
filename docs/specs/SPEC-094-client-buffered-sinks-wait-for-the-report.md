---
type: Spec
id: SPEC-094
title: "Client-Buffered Sinks Wait for the Delivery Report, on the Delivery Engine"
description: "Moves KafkaSink and GooglePubSubSink onto the delivery engine, with each round waiting, within the sink's own timeout and until the stop signal, for the client's report on every message it handed over, so that a rejection no attempt could have written fails the batch, every other report is counted as one that may have landed, and no count arrives after the batch's verdict."
spec_status: Draft
depends_on: [SPEC-069, SPEC-072, SPEC-074, SPEC-075, SPEC-076, SPEC-092, SPEC-093]
arc: audit-2026-11-14
arc_order: 10
---

## Overview

`KafkaSink` and `GooglePubSubSink` hand each message to a client library that sends it later, and
both give their verdict when the hand-off returns. So `flush()` reads true while every delivery
report is an error, and the loss is counted afterwards, when a later batch or `close()` reads it. The
2026-11-14 audit found this (finding 12), and SPEC-092's corpus pins it red. Andrew decided on
2026-11-15 that each batch waits for its reports, and on 2026-11-16 what a truthy `flush()` then
means. This spec builds both. Each sink becomes a delivery-engine sink whose round hands every message
over and then waits, within the sink's existing timeout and until the stop signal, for the reports on
that round's messages. A report that no attempt could have written the message is a loss, and it
fails the batch when nothing else landed. Every other report, and no report in time, is counted and
announced as one that may have landed, and it is never sent again. Nothing about the batch is counted
after its verdict.

## Scope

### In Scope

- `KafkaSink` on the delivery engine, waiting for this round's delivery reports (FR-001).
- `GooglePubSubSink` on the delivery engine, waiting for this round's publish futures (FR-002).
- The conformance corpus's Kafka and Pub/Sub doubles and the two finding-12 pins (FR-003).
- The record: `architecture.md` §12 and §13, the decisions with Andrew's answers quoted, the
  invariants page, the README, the rosters and the pending release notes (FR-004).

### Out of Scope

- **Whether a full client queue is retried.** That is the rest of finding 12, and it stays open in
  §12. `KafkaSink` keeps 1.x's reading of a `produce()` that raises: dropped and counted beside a
  hand-off that worked, and resent when nothing was handed over, because the 1.x worker resent a
  batch refused whole. `GooglePubSubSink` keeps 1.x's reading of a `publish()` that raises, the same
  way. A full Pub/Sub queue is not that case: the client returns a future that carries
  `FlowControlLimitError`, which 1.x counted in `failed` at its next reap and FR-002 counts as
  `REJECTED` in the round. The two sinks therefore read a full client queue differently, because
  their clients report it differently, and FR-004 records that against invariant 6.
- **`NATSSink`**, which already confirms core batches (SPEC-061), and its round deadline.
- **`SentrySink`'s transport-side drop**, which stays SPEC-092's `sdk-transport-drop` departure.
- **The socket, Sentry discard, AWS, `_bulk` and RabbitMQ fixes.** They are SPEC-093, the arc's
  previous spec. This spec depends on it: both land in one PR, and FR-003 edits the corpus and the
  pending release notes as SPEC-093 leaves them.
- **Idempotency keys** (finding 5). An `UNKNOWN` message is never sent again here, because no
  shipped sink declares itself idempotent.
- **A new constructor argument, setting or default.** The wait is bounded by the timeout each sink
  already has.
- **Any change to the engine, `Outcome`, the worker or SPEC-090's batching.**

---

## Functional Requirements

Every premise below was first read at `33582484`, and the prototype under *Evidence* was measured
there. Two branches that touched `src/log_foundry/sinks/` were open while this spec was written,
`claude/driver-error-rework-c373af` and `claude/isinstance-guard-9eeb88`. Both landed, as `8bd0b54a`
and `bb6656ff`, and SPEC-086 and SPEC-088 then landed, up to `97a03f45`. None of them changes a line
this spec cites in `sinks/`, and every `file:line` here was re-read at `97a03f45`. The prototype was
rebuilt on `97a03f45`. **The build re-verifies every `file:line` against `main` as it stands.**
Neither sink calls `delivery_error`, so this spec assumes no definition of it. Every type test FR-001
and FR-002 add is `_diag.is_a`, as *A type test reads the real type, never the value's __class__*
(`docs/decisions/failure-paths/`) requires.

**Andrew's two answers bind every FR here.**

- **2026-11-15, decision B.** Asked whether a client-buffered sink's verdict may come before the
  client's report, he answered **"Wait for the report (Recommended)"**: each batch's `emit` waits for
  the client's delivery reports within the sink's existing timeout, as core NATS confirms.
- **2026-11-16, what a truthy `flush()` means.** The question gave decision B's gloss, "a truthy
  `flush()` means the broker acknowledged", beside the earlier decision on `flush()`'s truthiness,
  "Keep truthy, add counts", recorded at `3e27a0a6` (SPEC-088) and `84386551` (SPEC-089), before
  decision B was recorded at `33582484` (SPEC-092). He answered **"Truthy, counted (Recommended)"**:
  an indefinite report leaves `flush()` truthy, its events are counted `UNKNOWN` in `failed` with a
  line and never resent, and a definite rejection still makes `flush()` falsy. So decision B is
  reworded here to **a truthy `flush()` means every event was acknowledged or counted**. The
  reconciliation rests on that answer, not on which decision came later. FR-004 marks every site that
  still states the old gloss.

**The rule both sinks share.** A round hands every remaining event to the client, then waits for the
reports on the messages it handed over. The wait ends when every one of them has a report, when the
sink's own timeout has passed, or when the sink's `log_foundry_stop_signal` is set after the wait's
first slice. So every round waits at least one slice, 50 ms at most, for reports already on their
way, and a shutdown then cuts the rest short. Each message the round handed over has one outcome:

| The report | Outcome | Counted | Line, once per round unless it says otherwise |
|---|---|---|---|
| success | `ACCEPTED` | nothing | none |
| a rejection no attempt could have written (the sink's list below) | `REJECTED` | `failed` | one per message, 1.x's text |
| any other error, none by the deadline, or none when the stop cut the wait | `UNKNOWN` | `failed` | `may have landed, not retried` |

**Why a rejection is not enough to be `REJECTED`.** A client retries a message on its own, and a
later attempt's refusal says nothing about an earlier attempt that timed out. In librdkafka v2.16.0,
a timed-out attempt leaves a message `POSSIBLY_PERSISTED` (`rdkafka_request.c:4520-4522`), and a later
error never downgrades it to `NOT_PERSISTED` (`rdkafka_msg.c:1011-1018`), but confluent-kafka does not
expose that status. google-cloud-pubsub 2.43.0 retries a publish on `DeadlineExceeded` among others
(`google/pubsub_v1/services/publisher/transports/base.py:179-197`), so a later `NotFound` or
`PermissionDenied` can follow an attempt the server kept. So `REJECTED` is kept for a refusal that
every attempt would have met the same way, because it judges the message's own bytes, or for a
message the client never sent. Everything else is `UNKNOWN`.

A run with no event `ACCEPTED`, at least one `REJECTED` and none `UNKNOWN` fails, by the engine's rule
(SPEC-072). The worker then counts one failed batch, and `flush()` reads false. A run with any
`UNKNOWN` event settles. A report that arrives after its round settled changes no count and writes no
line.

**On the exit drain**, the stop signal is set for the whole drain, so each round waits one slice and
reads the rest `UNKNOWN`. Against a broker that answers within the slice, nothing changes. Against a
slower one, the exit batches are counted as may-have-landed, and `close()`'s own bounded flush still
gives them a chance to land. If `shutdown()`'s join expires while a round waits (`worker.py:1795`),
the worker leaves the sink open and writes its pending line (`:1796-1812`). Since the stop is set
before the join (`:1792`), a round already in its wait ends within one slice, and the case needs a
round started before the stop and a join budget under one slice. `GooglePubSubSink`'s default
`overflow_timeout`, 30 s, equals `DEFAULT_SHUTDOWN_TIMEOUT` (`_lifecycle.py:27`). The stop is what
keeps a shutdown from waiting it out.

### FR-001: `KafkaSink` waits for this round's delivery reports

#### Description:

**Today.** `emit` returns once `produce()` has queued each message (`sinks/kafka.py:310-319`). The
delivery callback counts `failed` whenever a later `poll` or `flush` serves it (`:499-520`). `close()`
counts what is still queued when its flush expires (`:470-479`). The audit's `probe_misc.py` case 3
ran 25 events whose every report is `-192`. At `33582484` it reads `flush()=True`, `failed_batches=0`,
`failed=25` and 25 lines. The counts come after the verdict.

**On the engine.** `KafkaSink` gains `deliver`, `log_foundry_counters` and `log_foundry_stop_signal`,
as SPEC-074's sinks did. Its `emit` becomes the inline adapter. `failed` and `rejected` become
assignable properties over the counters' `failed_events` and `dropped_events`, and `losses()` reports
the counters. The class docstring now says the sink exposes `log_foundry_stop_signal`
(`kafka.py:68`). The engine waits between rounds on it, and so does the report wait. The sink has no
`max_retries`, so through the worker a run has the worker's `max_retries + 1` rounds, which is the
1.x total of attempts.

**The round.**

- **Hand-off.** A closed or closing sink refuses through the guard with 1.x's error, as today. Every
  event is encoded once per run (`encode_round`), and an unencodable one is `DROPPED` with SPEC-069's
  line. Each encodable event is produced with a callback bound to the round's own report table, and
  `poll(0)` follows each produce, as today.
- **A refused `produce()`.** In a round where another message was produced, the refused message is
  `DROPPED` and counted in `rejected` only when the run settles, with 1.x's line `KafkaSink produce,
  <Type>`. In a round where nothing was produced, every refused message is `RETRYABLE`, and the
  engine resends it after its wait.
- **The wait, for this round's messages only.** A blocking `rd_kafka_flush` waits on the producer's
  whole backlog, so one message stuck on a leaderless partition held every later round for the full
  timeout. The reviewer's probe measured 0.50 s per emit for four emits in a row. So the round never
  blocks in `flush`. Each pass of its wait does three things:
  - it calls `producer.flush(0)`, which serves the reports already in, wakes the broker threads
    while it marks the producer as flushing (`rdkafka.c:6486-6494`), and returns the queue length
    without blocking (`Producer.c:480-567` takes the non-chunked path under 200 ms);
  - it polls for one slice, `producer.poll(slice)`, of at most `_REPORT_SLICE` (0.05 s);
  - when the poll served nothing, it waits the rest of the slice on the stop signal through
    `_retry.wait`, so a `poll` that returns at once does not spin, and a poll that served a report
    goes straight to the next pass.

  The wait stops when any of these holds:
  - every position produced this round has a report;
  - `flush(0)` returned the `int` 0, meaning no message waits to be sent or acknowledged and no
    report waits to be served;
  - `flush_timeout` has passed since the wait began;
  - the stop signal is set after the first slice.

  `kafka` joins `tests/test_sink_retry.py`'s `WAIT_EXCLUSIONS`, as `pubsub` is, with the reason "it
  waits on its own delivery reports, which is not a retry backoff".
- **Each produced message.**
  - A report with no error: `ACCEPTED`.
  - A report whose error code is in `DEFINITIVE_CODES`: `REJECTED`, with 1.x's line `KafkaSink
    delivery, <Type> code=N`, one per message.
  - A report with any other error: `UNKNOWN`.
  - No report: `ACCEPTED` when the wait ended on `flush(0)` returning 0, and `UNKNOWN` otherwise.
- **`delivery.report.only.error`.** A producer configured that way, built from `producer_config` or
  injected, sends no report for a success. Its round ends when the queue reads empty, and its
  unreported messages are then `ACCEPTED`. When another message is stuck, the queue never reads
  empty, so the round waits the timeout and its unreported messages read `UNKNOWN`. A stand-in
  producer whose `flush` returns something other than an `int` gives no empty reading, so its
  unreported messages are `UNKNOWN` at the deadline. One whose `flush` serves its reports, as the
  repository's own test doubles do, ends the wait on its first pass.

  The round's `UNKNOWN` messages share one line: `lost N message(s); KafkaSink, no definitive delivery
  report within <flush_timeout>s; may have landed, not retried`.
- **Late reports.** The report table is closed when the round reads it, so a report served later, by
  another round's poll or by `close()`, changes nothing. The table takes no lock, because a write
  after the round read it lands in a table nothing reads.
- **The `flush()` hook.** It refuses when the sink is closed, as today. Otherwise it calls
  `producer.flush(0)`, which serves any pending report and asks the broker threads to send at once,
  and it returns without raising for a remainder, because every message a remainder holds was already
  counted by its round. 1.x waited `flush_timeout` and raised for a remainder, which made
  `log_foundry.flush()` falsy (`reason='sink-flush'`) over an indefinite report and, at the defaults,
  overran its 5 s budget. That contradicts Andrew's 2026-11-16 answer, and the hook now matches
  `GooglePubSubSink`'s.
- **The close.** `_flush_bounded` still flushes within `flush_timeout`, so messages a round left
  `UNKNOWN` may still land. It counts nothing, because every message was settled by its own round.
  1.x's "still queued when the close flush expired" count and the `released_during` count both go,
  since each counted a message after its batch's verdict.

**`DEFINITIVE_CODES`** is `frozenset({10, 18, 21, 87})`: `MESSAGE_TOO_LARGE`, `RECORD_LIST_TOO_LARGE`,
`INVALID_REQUIRED_ACKS` and `INVALID_RECORD`. Each was checked against the principle above.
- The broker returns 10 and 18 for the request's bytes against its size limits.
- It returns 87 for a record its validation refuses.
- It returns 21 for the producer's own `acks` setting.

So every attempt carrying the same message under the same settings meets the same refusal, and none
could have written it. The draft's other codes do not qualify:
- 29 and 31 are authorization, which can change between attempts.
- 2, `CORRUPT_MESSAGE`, can be a transient corruption of one attempt.
- 17, `INVALID_TOPIC_EXCEPTION`, is left out because this spec could not verify that librdkafka
  never sends it after an earlier attempt.

A broker's size limit changed between two attempts would break the reasoning for 10 and 18. That is
an inference, not a measurement, and *Evidence* says so. A code is read as `_code` reads it: `code()`
must return a value `_diag.is_a(value, int)` admits, which includes an `int` subclass (`kafka.py:546`),
compared as the `int` it holds.

**A caller sees:**
- a broker round trip on every batch;
- every batch waiting `flush_timeout`, 10 s by default, against a broker that never answers, with its
  messages counted `UNKNOWN`;
- with one partition stuck and every other report arriving, batches that return as soon as their own
  messages are reported;
- an orphan log, one outside any span, holding the caller's thread for the same wait (invariant 4):
  the probe measured 0.50 s for a 0.5 s timeout against a dead broker;
- `failed` counting each lost message once per run, where 1.x counted at every report;
- a batch whose every report is in `DEFINITIVE_CODES` reading `flush()` false, where 1.x read it true;
- a batch whose reports are authorization errors (29), which 1.x counted as failures beside a truthy
  `flush()`, still counted, as `UNKNOWN`, with `flush()` truthy;
- a `FilteringSink` or `TransformSink` around the sink now offering `deliver` (SPEC-075);
- the inline adapter counting a failed run's events in `failed` before it raises, where 1.x's raising
  `emit` counted nothing.

#### Acceptance Criteria:

- [ ] `KafkaSink-never-acknowledged` and `KafkaSink-report-indefinite` of `tests/test_conformance.py`
      read green, and `KafkaSink`'s other rows stay green (invariants 2 and 7).
- [ ] With every report carrying code 10, a batch through the worker reads `flush()` false and one
      failed batch, and each message is produced once. With every report carrying code 29, it reads
      `flush()` true with every message `UNKNOWN`, counted once in `failed`, and one line (invariants 2
      and 7).
- [ ] `probe_misc.py` case 3 against the branch reads `flush()=True`, `failed_batches=0`, `failed=25`
      and one line, with no count after `flush()` returned (invariants 2 and 11).
- [ ] `spec094_stuck_partition.py`, re-run as a test: with one message never reported and every other
      reported at the next poll, `flush_timeout=0.5`, the first emit returns in no less than 0.5 s
      and within 1.5 s, and each of the next three returns within 0.1 s with no new `failed` count.
      The same test, with the round calling `flush(flush_timeout)` instead, takes 0.5 s for each
      emit, which shows it reads the per-round wait (invariants 2 and 3).
- [ ] A report served after its round settled, by a later round's poll or by `close()`, moves no
      counter and writes no line (invariant 2).
- [ ] With no callback served and `flush(0)` returning 0, every message is `ACCEPTED`. With it
      returning 1, every message is `UNKNOWN` at the deadline (invariants 2 and 7).
- [ ] With no report ever and `flush_timeout=0.5`, one `emit` returns in no less than 0.5 s and within
      1.5 s, its messages counted once in `failed`. With the stop signal set 0.3 s in and
      `flush_timeout=5`, it returns in no less than 0.3 s and within 1.3 s, its messages `UNKNOWN`
      (invariants 3 and 7).
- [ ] With no report ever, `log_foundry.flush(timeout=5)` returns true, and the `flush()` hook itself
      returns in no less than 0 s and within 1 s without raising (invariants 3 and 7).
- [ ] A round where every `produce()` raises `BufferError` is resent by the engine, and a round where
      one of two raises counts that one in `rejected` once the run settles (invariant 2).
- [ ] `KafkaSink.__init__`'s signature is unchanged, and `failed`, `rejected`, `losses()`, `flush()`
      and `close()` keep their names (invariant 10).

### FR-002: `GooglePubSubSink` waits for this round's publish futures

#### Description:

**Today.** `emit` appends each publish future to a pending list and returns (`sinks/pubsub.py:266-
291`). `_reap` resolves the ones that have settled at the next `emit` (`:330-374`), `_await_overflow`
waits on the oldest beyond `max_pending` (`:376-421`), and `close()` resolves the rest (`:673-`).
Each failure is counted by `_resolve` when it is read (`:568-615`), which is after the batch's verdict.

**On the engine.** `GooglePubSubSink` gains `deliver`, `log_foundry_counters` and the adapter, and
keeps its `log_foundry_stop_signal`. `failed` and `rejected` become assignable properties over
`failed_events` and `dropped_events`. It keeps its own closed flag and `_futures_lock`, not the closed
guard (SPEC-076 FR-005), and the sink-delivery decision *The sinks keep four declared exceptions to
their shared rules* keeps row 3.

**The round.**

- **Hand-off.** A sink closed on entry refuses with 1.x's error. Events are encoded once per run.
  A publish that raises is read as `KafkaSink`'s refused `produce()` is, with 1.x's line
  `GooglePubSubSink refused the publish, <Type>`.
- **Close mid-round.** A publish whose hand-off finds the sink closed is `UNKNOWN`, with 1.x's line
  `GooglePubSubSink publish unconfirmed, the sink closed mid-batch`, and it is not waited on. That is
  row 3 of the declared exceptions.
- **The wait.** The round's futures are waited on under one deadline, `overflow_timeout` (30 s by
  default), in slices of `_POLL_INTERVAL` through `result(timeout=slice)`, and between slices on the
  stop signal. Every future is asked at least once. Once the stop signal is set after the first
  slice, each later ask waits nothing, and a future still unresolved reads as unknown.
- **Each future.**
  - A result: `ACCEPTED`.
  - An exception in the definitive set: `REJECTED`, with the line `GooglePubSubSink publish refused,
    <Type>`, one per event.
  - Any other exception, no result by the deadline or the stop, or a `result()` that takes no
    `timeout` (`TypeError`): `UNKNOWN`. The round's `UNKNOWN` events share one line,
    `GooglePubSubSink publish unconfirmed (<first Type>); may have landed, not retried`.
- **The definitive set**, resolved through `_classify.driver_classes`, so nothing is imported:
  - `google.api_core.exceptions.InvalidArgument`. The server returns it for the request's content,
    so a retry of the same message meets it again. That is an inference from the status code's
    meaning: `google/api_core/exceptions.py:266-275` only maps status 400 and gRPC `INVALID_ARGUMENT` to the class.
  - `google.cloud.pubsub_v1.publisher.exceptions.FlowControlLimitError`, which google-cloud-pubsub
    2.43.0 sets on a future it never sent (`publisher/client.py:405-423`).

  The draft's `PermissionDenied`, `Unauthenticated` and `NotFound` are now `UNKNOWN`, because the
  publish retry (`transports/base.py:179-197`) can end in one of them after a `DeadlineExceeded`
  attempt the server kept. A batch whose RPC failed after its retries carries that RPC's
  `GoogleAPIError` on every future (`publisher/_batch/thread.py:344-371`).
- **What goes.** Nothing is outstanding between emits, so `_reap`, `_await_overflow`,
  `_resolve_within`, `_drain_pending`, `_resolve` and the pending list's use go. `max_pending` is still
  accepted and validated, because it is frozen surface, and it has no effect, which its docstring
  says. FR-004 records it in invariant 13's exceptions and in the bad-argument decision. `flush()`
  refuses when closed and otherwise returns, since nothing is in flight. `close()` sets the flag.

**A caller sees:**
- a round trip per batch;
- a batch that holds the drain thread for up to `overflow_timeout` against a server that never
  answers, where 1.x waited only past `max_pending`, unless a shutdown cuts it;
- an orphan log holding the caller's thread for the same wait;
- `failed` counting once per run;
- `flush()` reading false for a batch every future of which was refused with `InvalidArgument` or
  `FlowControlLimitError`;
- a future type without a `timeout` reading as `UNKNOWN` at once, where 1.x waited on it, unbounded,
  at `close()`;
- a wrapper around the sink offering `deliver`;
- the adapter counting a failed run's events in `failed` before it raises.

#### Acceptance Criteria:

- [ ] `GooglePubSubSink-never-acknowledged` and `GooglePubSubSink-report-indefinite` read green, and
      the sink's other rows stay green (invariants 2 and 7).
- [ ] With every future raising an `InvalidArgument` from a stub `google.api_core.exceptions`, a batch
      through the worker reads `flush()` false and one failed batch. With every future raising
      `PermissionDenied`, `NotFound` or `DeadlineExceeded`, the batch settles, its events are counted
      in `failed` once, and one line names the type (invariants 2, 7 and 11).
- [ ] With futures that never resolve and `overflow_timeout=0.5`, one `emit` returns in no less than
      0.5 s and within 1.5 s with its events `UNKNOWN`. With the stop signal set 0.3 s in and
      `overflow_timeout=5`, it returns in no less than 0.3 s and within 1.3 s (invariants 3 and 7).
- [ ] A close landing mid-round makes the later publishes `UNKNOWN` without raising, and
      `test_a_close_landing_mid_batch_counts_the_rest_unconfirmed_and_does_not_raise` passes
      (invariant 7).
- [ ] `GooglePubSubSink.__init__`'s signature is unchanged, and `max_pending` still refuses a non-`int`
      with `TypeError` (invariants 10 and 13).

### FR-003: The corpus holds the reports the two sinks now wait for

#### Description:

These are changes to SPEC-092's files as its build delivers them, after SPEC-093's.

- **The Kafka double.** It already serves its reports at `flush`, which the round's `flush(0)`
  reaches. Its definitive report becomes code 10 rather than 29, which SPEC-092's fault table now
  says. 1.x reads every report error alike, so no row moves at `97a03f45`.
- **The Pub/Sub double** raises `InvalidArgument` and `DeadlineExceeded` as classes of a stub
  `google.api_core.exceptions`, installed through `DRIVERS` with google-api-core's hierarchy for the
  classes it names: `GoogleAPIError`, `GoogleAPICallError`, `ClientError`, `BadRequest`,
  `InvalidArgument`, `Forbidden`, `PermissionDenied`, `ServerError`, `GatewayTimeout` and
  `DeadlineExceeded`. It no longer uses the local stand-in classes. At `97a03f45` no row moves with
  this change, because the sink reads no class.
- **Pins.** `verdict-before-refusal` and `verdict-before-indefinite-report` are removed, because their
  four rows read green.
- **Totals**, after SPEC-093's: 268 rows, 58 pinned under five pins, 15 departure rows and 195 that
  must pass.

#### Acceptance Criteria:

- [ ] The corpus's literals read 268 rows, 58 pinned, 15 departure rows and 195 that must pass
      (invariants 2 and 7).
- [ ] Against SPEC-093's verdicts, only the four rows of FR-001 and FR-002 change (invariants 2
      and 7).
- [ ] With `KafkaSink._round`'s wait removed, so that no report is waited for, in an export of the
      branch head, `KafkaSink-never-acknowledged` and `KafkaSink-clean` read
      `ambiguous-when-knowable` (invariant 2).
- [ ] With the Pub/Sub double raising its local `PermissionDenied` again, in an export of the branch
      head, `GooglePubSubSink-never-acknowledged` reads `ambiguous-when-knowable`, which shows the
      definitive set is read by class (invariant 7).

### FR-004: The record says the two sinks wait for the report

#### Description:

These are the completion ritual's edits, made on the pushed branch.

- **The old gloss, at every site.** "A truthy `flush()` means the broker acknowledged" stood at
  SPEC-092's decision B (`SPEC-092-sink-conformance-corpus.md:93-95` before this revision) and in the
  §12 text SPEC-092's FR-004 writes (`:478-483`). This revision strikes both in SPEC-092, with
  Andrew's 2026-11-16 answer beside them. The build checks that no other site states the gloss
  unmarked, among them §12, the decisions and the README.
- **§12.**
  - The finding-12 item, as SPEC-092 and SPEC-093 leave it, is rewritten in place and dated. Its
    verdict half is struck as closed by SPEC-094, with `probe_misc.py` case 3's numbers before and
    after. Its remaining clause becomes an open item of its own, led "**Whether a full client queue
    is retried**". That item says that `KafkaSink` keeps 1.x's reading of a `produce()` that raises,
    and that a full Pub/Sub queue arrives as a `FlowControlLimitError` future and is `REJECTED`.
  - The item "**`GooglePubSubSink.close()`'s bound does not cover an unboundable future**"
    (`architecture.md:1222-1232` at `97a03f45`) is struck as closed by SPEC-094. Such a future is now
    `UNKNOWN` in its round, counted, and never waited on at `close()`.
- **§13 gains four bullets.**
  - "**A client-buffered sink's batch costs a broker round trip.**" It carries the measured figures
    at batch 10 and at 1,000.
  - "**Against a broker that never answers, a `KafkaSink` or `GooglePubSubSink` batch holds the drain
    thread for the sink's timeout, and an orphan log holds the caller's thread for it.**" It carries
    the 0.50 s measured for a 0.5 s timeout.
  - "**A `KafkaSink` with `delivery.report.only.error` and a stuck partition reads its unreported
    successes as `UNKNOWN`.**"
  - "**On the exit drain a round waits one slice for its reports**", and it says what a shutdown's
    expired join does.
- **Decisions in `docs/decisions/sink-delivery/`.**
  - A new decision for decision B. It quotes both of Andrew's answers with their dates, gives the
    chronology (the truthiness decision first, decision B second), records "a truthy `flush()` means
    every event was acknowledged or counted" as resting on the 2026-11-16 answer, and carries the
    shared rule's table and the stop-signal rule.
  - A new decision for the principle that a report is `REJECTED` only when no attempt could have
    written the event, with the two sets, the librdkafka and google-cloud-pubsub reading behind them,
    and the no-report and `delivery.report.only.error` readings.
- **Settled decisions refined in place, each with a dated History entry naming SPEC-094.**
  - *A sink may report per event* (`docs/decisions/sink-delivery/`): the engine sinks now include
    these two.
  - *A raising `emit` counts nothing…*: the immediate counts of `_on_delivery`, `_resolve`, the close
    flush and `released_during` are gone. For these two sinks a raising `emit`, the inline adapter
    over a failed run, now counts that run's events in `failed` before it raises, as every engine
    sink's adapter does.
  - *A wrapper offers `deliver` exactly when what it wraps does…*: wrappers over these sinks now
    offer it.
  - *The sinks keep four declared exceptions…*: row 3 is unchanged, and its History says why.
  - *A bad argument is floored if it works and refused if it is already broken…*: `max_pending` is
    still validated and floored, and it no longer bounds anything.
- **`docs/invariants.md`.**
  - Invariant 2: the hand-offs `KafkaSink` and `GooglePubSubSink` see refused, and their `rejected`
    count, are re-described for the engine.
  - Invariant 4: its recorded exceptions gain these two sinks' round trip, and their full timeout
    against a dead broker, on the caller's thread outside a span, beside core `NATSSink`'s.
  - Invariant 6: the full-client-queue asymmetry between the two sinks is recorded as a known
    difference.
  - Invariant 7: its recorded exceptions gain a batch these sinks settle on an indefinite report.
  - Invariant 13: its recorded exceptions gain `max_pending`, validated and inert.
  - The *Guarded* lines name the new tests.
- **README.**
  - The wrapper paragraph (`README.md:688-690`) drops "other than `KafkaSink` and
    `GooglePubSubSink`".
  - The queue-and-stream paragraph (`:960-965`) says both sinks wait for this round's reports within
    `flush_timeout` and `overflow_timeout` and until a shutdown.
  - The `KafkaSink` and `GooglePubSubSink` rows (`:998`, `:1003`) say the same, and say that
    `max_pending` has no effect.
- **Rosters and prose gates that name these sinks**, updated to the tree. The engine sinks become 18,
  `kafka` joins `WAIT_EXCLUSIONS`, and the adopter, deferral, retry-loop, fork and retry-safe-prose
  rosters change as *Evidence* lists.
- **The release notes**, where SPEC-093's went, and worded as SPEC-090 words it. The file's title
  and opener, as SPEC-093 rewrites them, stay true with these changes in. The entry lists:
  - both sinks on the engine, with `deliver`;
  - the per-round wait, its bound, and the stop signal that cuts it;
  - the partial-outage cost: with one partition stuck, only that partition's messages wait;
  - the dead-broker cost: each batch, and each orphan log, waits the timeout;
  - definite rejections (the two sets) failing the batch, and every other report, authorization
    errors among them, counted `UNKNOWN` with `flush()` truthy;
  - the `flush()` hook no longer waiting or raising;
  - no count after the verdict, and the close counts that go;
  - `max_pending` inert;
  - wrappers offering `deliver`;
  - the adapter counting a failed run before it raises.

  Its compatibility list names each one for a `>=1,<2` pin.
- **`docs/component-inventory.md`.** The two sinks' rows say they are engine sinks. No row is added,
  because no module is added.

#### Acceptance Criteria:

- [ ] §12's finding-12 item and its unboundable-future item are struck as closed by SPEC-094, the
      full-client-queue item is open with its own lead, and §13 carries the four bullets.
- [ ] The decision-B file quotes "Wait for the report (Recommended)" dated 2026-11-15 and "Truthy,
      counted (Recommended)" dated 2026-11-16, in that order, and states the reworded meaning. The
      definitive-report decision and the five dated History entries exist.
- [ ] No file under `docs/` or the README states "a truthy `flush()` means the broker acknowledged"
      unless it is struck or marked superseded.
- [ ] `docs/invariants.md` names the new recorded exceptions of the caller's-thread, sink-reporting
      and bad-argument promises, and the full-queue difference beside the twin-path promise.
- [ ] The README sites state the wait, and no README sentence says either sink hands off and returns
      without waiting.
- [ ] The release-notes entry exists and names each change listed.
- [ ] This FR serves no invariant: it is the record of FR-001 to FR-003, and the promises it
      describes are theirs.

---

## Data Model

```python
# src/log_foundry/sinks/kafka.py
DEFINITIVE_CODES: frozenset[int] = frozenset({10, 18, 21, 87})
_REPORT_SLICE: float = 0.05

class KafkaSink:
    log_foundry_counters: SinkCounters
    log_foundry_stop_signal: threading.Event | None          # new
    failed: int      # property over failed_events, assignable
    rejected: int    # property over dropped_events, assignable
    def deliver(self, batch: list[dict[str, object]]) -> Verdict: ...   # new

# src/log_foundry/sinks/pubsub.py
class GooglePubSubSink:
    log_foundry_counters: SinkCounters
    failed: int      # property over failed_events, assignable
    rejected: int    # property over dropped_events, assignable
    max_pending: int # accepted and validated; no effect
    def deliver(self, batch: list[dict[str, object]]) -> Verdict: ...   # new
```

## File & Folder Structure

```
src/log_foundry/sinks/
├── kafka.py      # FR-001
└── pubsub.py     # FR-002
tests/
├── conformance_doubles.py, conformance_pins.py   # FR-003
└── test_*.py     # new tests, and the ones that change (Evidence)
docs/             # FR-004
```

## Implementation Phases

### Phase 1: `KafkaSink` on the engine (FR-001)

- The round, the per-round wait, the report table, `DEFINITIVE_CODES`, the counters as properties,
  the stop signal and the `flush()` hook, with the tests that change.

### Phase 2: `GooglePubSubSink` on the engine (FR-002)

- The round, the stop-aware wait, the definitive set, and the removal of the pending-list machinery,
  with the tests that change.

### Phase 3: The corpus and the record (FR-003, FR-004)

- The Kafka and Pub/Sub doubles, the pins, the rosters and the record.

## Evidence

The prototype is `docs/audits/2026-11-14-probes/sinks/spec094_prototype.patch`. It applies with
`patch -p1` after `spec093_prototype.patch` to `git archive 97a03f45 src tests
docs/audits/2026-11-14-probes`. SPEC-092's prototype stands in for its corpus, as in SPEC-093.
Numbers were taken on macOS with Python 3.13.1 and no extra installed, at `97a03f45` unless they say
otherwise.

**The rows.** On top of SPEC-093's prototype, 268 rows read 195 green and 73 red. That is the same in
three runs on Python 3.13.1 and one on 3.12.12. Exactly these four rows move:

- `KafkaSink-never-acknowledged` and `GooglePubSubSink-never-acknowledged`, from
  `partition`+`silent-loss` to green;
- `KafkaSink-report-indefinite` and `GooglePubSubSink-report-indefinite`, from
  `partition`+`lost-but-held` to green.

With FR-003's double changes and `src/` at `97a03f45`, the Kafka and Pub/Sub rows read exactly as
SPEC-092 records.

**An emit-only wait was tried first and fails the corpus.** Waiting for the reports inside a 1.x
`emit`, with no `deliver`, turned `KafkaSink-never-acknowledged` green and left
`KafkaSink-report-indefinite` at `lost-but-held`. An `emit`-only sink can report a batch only as
landed less what its `losses()` moved, so an event counted in `failed` that the broker kept is read as
lost and held. Only a verdict can say `UNKNOWN`, which is why both sinks move onto the engine.

**The mutant.** With `KafkaSink._round`'s wait removed, `KafkaSink-clean`, `-refused-once` and
`-never-acknowledged` read `ambiguous-when-knowable`, and `-refused-always` and `-report-indefinite`
stay green: with no wait, every produced message is `UNKNOWN`. That was measured on the per-round
wait at `97a03f45`, as it was on the draft's `flush`-based wait at `33582484`.

**The Pub/Sub double's classes matter.** With the double raising its local `PermissionDenied`, the
prototype read `GooglePubSubSink-never-acknowledged` as `ambiguous-when-knowable`. A refusal the sink
cannot recognise by class reads as indefinite. With the stub module's class, the row read green.

**The per-round wait.** `spec094_stuck_partition.py`, with one message never reported:

| | emit 0 | emit 1 | emit 2 | emit 3 |
|---|---|---|---|---|
| the draft, `flush(flush_timeout)` | 0.50 s | 0.50 s | 0.51 s | 0.50 s |
| this spec, per-round polls | 0.50 s | 0.00 s | 0.00 s | 0.00 s |

Both counted the stuck message once in `failed`. `spec094_dead_broker_flush.py`, with a producer that
never reports and `flush_timeout=0.5`:
- under the draft, `log_foundry.flush(timeout=5)` returned `FlushResult(ok=False,
  reason='sink-flush')` after 1.01 s;
- under this spec it returned `ok=True` after 0.50 s.

In both, an orphan `info()` held the caller's thread for 0.50 s and every event was counted in
`failed`. With the stop signal set 0.3 s into a wait whose timeout is 5 s, an `emit` returned after
0.32 to 0.34 s for each sink, its events `UNKNOWN`.

**The suite.** The full suite was run against the prototype in a scratch export with no `docs/`.
Against SPEC-093's prototype at `97a03f45`, 54 more tests fail. The draft's `flush`-based prototype
failed 52 at `33582484` and 53 at `bb6656ff`, which added `tests/test_class_claim.py`. The 54th is
`test_sink_retry.py::test_only_the_engine_waits_or_backs_off`, which FR-001 changes by adding `kafka`
to `WAIT_EXCLUSIONS`. A first version of the per-round wait, which polled and read `__len__` without
`flush(0)`, failed eight more. Each used a stand-in producer that serves reports only at `flush`, so
it waited the full timeout and read its messages as `UNKNOWN`; the `flush(0)` pass fixed all eight.
In an earlier run, at a one-minute load near 100, two unrelated `_otel` tests failed once and passed
when rerun serially. They are not counted. The 54 are in these files:

| File | Tests |
|---|---|
| `test_sinks_pubsub.py` | 16 |
| `test_sinks_refusal_deferral.py` | 7 |
| `test_sinks_kafka.py` | 5 |
| `test_classify.py` | 5 |
| `test_retry_safe_drop_counts_prose.py` | 3 |
| `test_deferral.py` | 3 |
| `test_stored_name_sinks.py` | 2 |
| `test_sink_losses.py` | 2 |
| `test_sink_flush_hook.py` | 2 |
| `test_final_refusal.py` | 2 |
| `test_delivery_engine.py` | 2 |
| `test_sink_retry.py` | 1 |
| `test_sink_post_close.py` | 1 |
| `test_fork_roster.py` | 1 |
| `test_engine_sinks_roster.py` | 1 |
| `test_class_claim.py` | 1 |

With each sink's change applied alone on top of SPEC-093's, at `33582484` with the draft's wait, 25
of the 52 failed with `KafkaSink`'s and 33 with `GooglePubSubSink`'s. 7 failed with either, and one
failed only with both: `test_deferral.py::test_every_run_context_count_of_a_dropped_counter_is_settled`,
which expects a stored `rejected` counter that neither sink then keeps. Each of the 54 asserts what
FR-001 or FR-002 changes:

- **The pending list and its reap, overflow and close resolution.** These are the 16 in
  `test_sinks_pubsub.py` and the two in `test_sink_flush_hook.py`.
- **A count made by a report or a future after the batch's verdict.**
  - In `test_sinks_refusal_deferral.py`: `test_the_kafka_delivery_callback_counts_failed_before_emit_returns`,
    `test_the_pubsub_reap_counts_failed_before_emit_returns` and
    `test_the_pubsub_orphaned_future_counts_failed_before_emit_returns`.
  - In `test_sinks_kafka.py`: `test_delivery_errors_are_counted`,
    `test_a_non_zero_remainder_is_counted_and_announced_once`,
    `test_an_overtaken_emit_counts_every_message_it_handed_over` and
    `test_a_produce_released_after_close_is_counted_and_announced`.
- **A whole-batch refusal that 1.x raised and the worker resent, which the engine now resends
  itself.** These are `test_sinks_refusal_deferral.py::test_a_batch_refused_on_every_attempt_counts_no_refusal`,
  `::test_a_refusal_followed_by_a_raise_counts_nothing` and `::test_a_fanout_absorbs_the_refused_batch`
  (two), and the two in `test_sink_losses.py`. So is
  `test_class_claim.py::test_a_client_raise_whose_class_property_raises_is_settled_by_its_real_type[GooglePubSubSink]`,
  whose rig expects no line from a batch every publish of which was refused. The engine's round now
  announces that batch when its last round ends.
- **The worker now calls `deliver`, not `emit`.** The two in `test_final_refusal.py` count the
  sink's `emit` calls for a closed sink's refused batch, and now read `emits() == 0`, because the
  worker reaches an engine sink through `deliver`.
- **A roster derived from the tree that now finds two more engine sinks, or a gate's fixture planted
  in code that moved.**
  - `test_engine_sinks_roster.py::test_the_engine_sinks_are_the_sixteen`.
  - The two in `test_delivery_engine.py`.
  - The five in `test_classify.py`, the retry-loop sweep and the wrapper exclusions.
  - `test_sink_retry.py::test_only_the_engine_waits_or_backs_off`.
  - `test_sink_post_close.py::test_every_hand_listed_adopter_population_is_the_derived_one`.
  - Two of the three in `test_deferral.py`: `test_every_dropped_counter_is_written_only_by_a_deferral_commit`,
    and `test_every_run_context_count_of_a_dropped_counter_is_settled`, the one that needs both
    changes.
  - The third, `test_a_rejected_store_planted_in_an_unreached_kafka_method_is_named`, is a planted
    mutant: it plants a store to `rejected` in a `KafkaSink` method, and `rejected` is now a property
    over the counters.
  - `test_fork_roster.py::test_every_class_holding_a_primitive_has_a_home`, whose `_PRIMITIVE_HOMES`
    entry for `KafkaSink` changes, because the sink's lock now lives in its `SinkCounters`.
- **1.x's docstring and line text.** These are the three in `test_retry_safe_drop_counts_prose.py`,
  `test_sinks_kafka.py::test_a_delivery_error_reports_the_code_not_the_message`, and the two in
  `test_stored_name_sinks.py`.

**The cost.** `spec094_report_wait_cost.py` used a fake client that acknowledges each message a set
time after it was handed over, and whose `poll` blocks until it serves a report or its timeout
passes, as librdkafka's does. It gives the median `emit` of seven, at a one-minute load between 9
and 14:

| Sink | ack | batch 10 | batch 100 | batch 1,000 |
|---|---|---|---|---|
| `KafkaSink` at `97a03f45` | any | 0.02 ms | 0.13 to 0.14 ms | 1.3 to 1.4 ms |
| `KafkaSink`, waiting | 1 ms | 1.10 ms | 1.29 ms | 3.74 ms |
| `KafkaSink`, waiting | 5 ms | 5.17 ms | 5.44 ms | 7.84 ms |
| `KafkaSink`, waiting | 20 ms | 20.24 ms | 20.70 ms | 24.29 ms |
| `GooglePubSubSink` at `97a03f45` | any | 0.02 ms | 0.13 to 0.14 ms | 1.2 to 1.4 ms |
| `GooglePubSubSink`, waiting | 1 ms | 1.54 ms | 2.24 ms | 6.18 ms |
| `GooglePubSubSink`, waiting | 5 ms | 6.37 ms | 6.70 ms | 9.05 ms |
| `GooglePubSubSink`, waiting | 20 ms | 24.93 ms | 25.00 ms | 31.97 ms |

So one batch costs about one round trip. At today's batches of 10 events and a 5 ms round trip, the
drain thread delivers about 1,900 events a second to Kafka and 1,600 to Pub/Sub, where it delivered
about 600,000 at `97a03f45`. At SPEC-090's cap of 1,000 events a batch it delivers about 128,000 and
110,000. SPEC-090's size target is `events / elapsed × flush_interval / 2`, which reaches the cap under
a backlog once a round trip is the cost, so a backlog lifts the batch size to where the wait matters
least. With a client that never acknowledges and the timeout at 0.5 s, one `emit` took 500.6 ms for
`KafkaSink` and 502.8 ms for `GooglePubSubSink`, against 0.02 ms at `97a03f45`. The fake's blocking
`poll` matters: with a `poll` that returns at once, every Kafka round paid a 50 ms slice (54 to
65 ms per `emit`), which is why a pass skips the slice's wait after a poll that served a report.

**What the prototype could not settle.**

- Whether a broker thread woken by `flush(0)` sends before `linger.ms` (librdkafka's default 5 ms,
  `rdkafka_conf.c:1652-1660`) has passed. At worst a round pays that 5 ms.
- That `flush(0)` returns the queue length that librdkafka's `rd_kafka_outq_len` documents, which
  counts unserved reports as well as unsent messages. That is read from the documentation and
  `Producer.c`, not measured against a broker.
- That librdkafka never sends one of `DEFINITIVE_CODES` after an earlier attempt that wrote the
  message. The four codes judge the message's bytes or the producer's settings, so every attempt
  meets them alike, but a broker or topic setting changed between attempts would break that. It is
  read from the Kafka protocol and librdkafka v2.16.0's source, not measured.
- Which librdkafka the confluent-kafka 2.16.1 wheels bundle. Its source requires librdkafka 2.16.0 or
  later (`confluent_kafka.h:147-151`), and the bundled version is unverified.
- That the server returns `InvalidArgument` only for a request's content, an inference from the
  status code.
- How the real clients behave. That is owed to `integration.yml`'s Kafka and Pub/Sub tests on the
  PR's branch.

## Revision history

- Drafted on 2026-11-15, split from SPEC-093 at the delivery engine. This spec moves two sinks onto
  it, and SPEC-093 changes no sink's path through it.
- Revised the same day, after the fresh-context review of both specs. SPEC-093 is now in
  `depends_on`, since both land in one PR, and the totals follow SPEC-093's revised corpus.
- Revised on 2026-11-16, after this spec's own review and Andrew's answer.
  - **Andrew, 2026-11-16: "Truthy, counted (Recommended)".** An indefinite report leaves `flush()`
    truthy, its events are counted `UNKNOWN` in `failed` with a line and never resent, and a definite
    rejection makes it falsy. Decision B reads "every event was acknowledged or counted". The previous
    revision's chronology was false. It said the truthiness decision came later. In fact "Keep
    truthy, add counts" was recorded first, at `3e27a0a6` and `84386551`, and "Wait for the report"
    later, at `33582484`. The reconciliation now rests on Andrew's answer, and SPEC-092's two sites of
    the old gloss are struck.
  - **The review's findings, each fixed.**
    - **B1.** The round waits for its own reports, serving them with a non-blocking `flush(0)` and
      polling in slices, not for the producer's whole backlog with a blocking `flush`, so a stuck
      partition no longer holds every later round. `delivery.report.only.error` is handled through
      the queue's empty reading. A first version polled without `flush(0)` and read `__len__`; it
      made eight more tests fail, all with stand-in producers that serve reports only at `flush`,
      and each waited the full timeout. The probe's re-plant is a criterion, and the
      partial-outage cost is in §13 and the release notes.
    - **M5.** The `flush()` hook no longer waits or raises.
    - **M6.** A report is `REJECTED` only when no attempt could have written the event. Kafka's set
      narrowed to `{10, 18, 21, 87}`, each one checked, and Pub/Sub's to `InvalidArgument` and
      `FlowControlLimitError`. The linger bullet is dropped: `rdkafka.c:6486-6494` makes `flush` treat
      `linger.ms` as immediate while it runs. Whether the round's non-blocking `flush(0)`, which
      holds that mark only briefly, also sends at once is unmeasured and listed as such. The
      bundled librdkafka version is stated as unverified.
    - **M7.** The orphan log's wait is named in §13 and invariant 4. The wait honours the stop
      signal, the expired join and the 30 s coincidence are stated, the unboundable-future §12 item
      is struck, and the adapter's count before a raise is named in the refined decision.
    - **8.** The full-queue asymmetry is named, and Out of Scope no longer claims 1.x's reading for a
      Pub/Sub full queue.
    - **9.** `max_pending` going inert is recorded in invariant 13 and the bad-argument decision.
    - **10.** The premises are corrected: `_code`'s `_diag.is_a` admits a subclass,
      `Producer_flush` spans `Producer.c:480-567`, and `exceptions.py` only maps codes to classes.
    - **11.** The timing criteria have a lower bound and allow one second over, as SPEC-086 does.
    - **12.** The suite attributions are corrected.
