---
type: Spec
id: SPEC-094
title: "Client-Buffered Sinks Wait for the Delivery Report, on the Delivery Engine"
description: "Moves KafkaSink and GooglePubSubSink onto the delivery engine, with each round waiting, within the sink's own timeout and until the stop signal, for the client's report on every message it handed over, so that a rejection no attempt could have written fails the batch, every other report is counted as one that may have landed, and no count arrives after the batch's verdict."
spec_status: Completed
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
sink's own timeout has passed, or when the sink's `log_foundry_stop_signal` is set once the wait's
first slice has passed. The first slice is a deadline, the wait's start plus 50 ms, not a count of
passes, so a round under a set stop still takes every report that arrives inside those 50 ms, and a
shutdown then cuts the rest short. A pause between two asks is never cut short by a stop that was
already set when it began, so a round under a set stop does not ask in a tight loop. Each message the round handed over has one outcome:

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
no attempt of the message could have escaped: one that judges something every attempt carries
unchanged, or one for a message the client never sent. A refusal that judges what an attempt
carried beside the message, such as the batch the client built around it, does not qualify,
because the client builds that again for each attempt. Everything else is `UNKNOWN`.

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
- **The wait, for this round's messages only, in slices.** A single blocking
  `flush(flush_timeout)` waits on the producer's whole backlog, so one message stuck on a leaderless
  partition held every later round for the full timeout: the reviewer's probe measured 0.50 s per
  emit for four emits in a row. So the round asks in slices of at most `_REPORT_SLICE` (0.05 s) and
  checks its own reports between them. Each pass does two things:
  - it calls `producer.flush(slice)`, which serves reports for at most that slice, returns early
    once the producer's queue is empty, and returns the queue length. While it runs it holds the
    producer's flushing mark (`rdkafka.c:6486-6494`), under which librdkafka sends what is queued
    without waiting out `linger.ms` (`:6492-6493`). That is why the pass flushes for the slice
    rather than calling `flush(0)` and then `poll(slice)`: the mark is held only while `flush`
    runs, so the earlier design's `flush(0)` dropped it at once, and every round then waited out
    `linger.ms` in its poll. Against a real broker that cost a round the producer's whole
    `linger.ms`, 1 s of it at `linger.ms=1000` (*Evidence*);
  - when the pass served no report, it waits the rest of its slice through `_retry.wait`, so a
    `flush` that returns at once, as a stand-in's may, does not spin. The pause is cut short by the
    stop signal, unless the signal was already set when the pause began (`_retry.pause_on`), since
    a set signal would end every pause at once.

  The wait stops when any of these holds:
  - every position produced this round has a report;
  - `flush(slice)` returned the `int` 0, meaning no message waits to be sent or acknowledged and no
    report waits to be served;
  - `flush_timeout` has passed since the wait began;
  - the stop signal is set and the first slice, the wait's start plus `_REPORT_SLICE`, has passed.

  A message stuck on one partition costs each later round one slice, because a pass whose own
  reports are all in still ends only at the check after `flush(slice)` returns, and with the stuck
  message queued that `flush` returns at the end of its slice. The probe measured 0.05 to 0.06 s
  per later emit, against 0.50 s for the single blocking flush. A producer whose `linger.ms` is at
  or above `flush_timeout` costs nothing more, because the flushing mark overrides `linger.ms`
  while each slice runs: measured against a real broker at `linger.ms=1000` with `flush_timeout=5`
  and at `linger.ms=3000` with `flush_timeout=1` (*Evidence*).

  `kafka` joins `tests/test_sink_retry.py`'s `WAIT_EXCLUSIONS`, as `pubsub` and, since SPEC-093,
  `_socket` are, with the reason "it waits on its own delivery reports, which is not a retry
  backoff". SPEC-093 has `tests/test_engine_sinks_roster.py`'s wait finding skip a module that list
  names, so both sinks, now engine sinks that wait, pass it for the same reason.
- **Each produced message.**
  - A report with no error: `ACCEPTED`.
  - A report whose error code is in `DEFINITIVE_CODES`: `REJECTED`, with 1.x's line `KafkaSink
    delivery, <Type> code=N`, one per message.
  - A report with any other error: `UNKNOWN`.
  - No report: `ACCEPTED` when the wait ended on `flush(slice)` returning 0, and `UNKNOWN` otherwise.
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
  another round's `flush` or `poll`, or by `close()`, changes nothing. A round's `flush` can serve
  another round's callbacks on another thread, so every write, the round's reads and the close
  take one lock, the sink's counters lock. The table reads that lock from the sink's
  `SinkCounters` at each use and never keeps it, because a settled table lives on in the callback
  librdkafka holds, which no fork repair reaches, and the repair rebinds the lock in a child.
- **The `flush()` hook.** It refuses when the sink is closed, as today. Otherwise it calls
  `producer.flush(0)`, which serves any pending report and returns at once, and it returns without
  raising for a remainder, because every message a remainder holds was already counted by its round.
  It does not make librdkafka send sooner, since the flushing mark it sets is dropped as it returns
  (`rdkafka.c:6492-6493`), and it has no need to: every message a round handed over was waited for
  by that round. 1.x waited `flush_timeout` and raised for a remainder, which made
  `log_foundry.flush()` falsy (`reason='sink-flush'`) over an indefinite report and, at the defaults,
  overran its 5 s budget. That contradicts Andrew's 2026-11-16 answer, and the hook now matches
  `GooglePubSubSink`'s.
- **The close.** `_flush_bounded` still flushes within `flush_timeout`, so messages a round left
  `UNKNOWN` may still land. It counts nothing, because every message was settled by its own round.
  1.x's "still queued when the close flush expired" count and the `released_during` count both go,
  since each counted a message after its batch's verdict.

**`DEFINITIVE_CODES`** is `frozenset({21, 87})`: `INVALID_REQUIRED_ACKS` and `INVALID_RECORD`. Each
was checked against the principle above.
- The broker returns 21 for the producer's `acks` setting, which is the producer's for its life,
  so every attempt carries the same one.
- It returns 87 for a record that its per-record validation refuses, and it names the record. The
  record's key, value and headers are the message's own and go unchanged into every attempt.

So no attempt of a message refused with either could have been written. The broker's own settings
can still change between attempts, a topic's compaction among them, and that would break the
reasoning for 87; it is an inference from the Kafka protocol and librdkafka v2.16.0's source, not
a measurement, and *Evidence* says so. The codes left out:
- 10, `MESSAGE_TOO_LARGE`, and 18, `RECORD_LIST_TOO_LARGE`, judge the record batch, which librdkafka
  builds again for each attempt. With the producer's `message.max.bytes` above the topic's
  `max.message.bytes`, a message whose first, smaller batch timed out `POSSIBLY_PERSISTED` can meet
  10 in a larger batch on its retry. Measured against a real broker, a 2 MB message read `UNKNOWN`.
- 29 and 31 are authorization, which can change between attempts.
- 2, `CORRUPT_MESSAGE`, can be a transient corruption of one attempt.
- 17, `INVALID_TOPIC_EXCEPTION`, is left out because this spec could not verify that librdkafka
  never sends it after an earlier attempt. A code is read as `_code` reads it: `code()`
must return a value `_diag.is_a(value, int)` admits, which includes an `int` subclass (`kafka.py:546`),
compared as the `int` it holds.

**A caller sees:**
- a broker round trip on every batch;
- every batch waiting `flush_timeout`, 10 s by default, against a broker that never answers, with its
  messages counted `UNKNOWN`;
- with one partition stuck and every other report arriving, later batches that return one 50 ms
  slice after their own messages are reported, not after the timeout;
- an orphan log, one outside any span, holding the caller's thread for the same wait (invariant 4):
  the probe measured 0.50 s for a 0.5 s timeout against a dead broker;
- `failed` counting each lost message once per run, where 1.x counted at every report;
- a batch whose every report is in `DEFINITIVE_CODES` reading `flush()` false, where 1.x read it true;
- a batch whose reports are size refusals (10 or 18), which 1.x counted as failures beside a truthy
  `flush()`, now counted as `UNKNOWN`, with `flush()` still truthy;
- a batch whose reports are authorization errors (29), which 1.x counted as failures beside a truthy
  `flush()`, still counted, as `UNKNOWN`, with `flush()` truthy;
- a `FilteringSink` or `TransformSink` around the sink now offering `deliver` (SPEC-075);
- the inline adapter counting a failed run's events in `failed` before it raises, where 1.x's raising
  `emit` counted nothing.

#### Acceptance Criteria:

- [ ] `KafkaSink-never-acknowledged` and `KafkaSink-report-indefinite` of `tests/test_conformance.py`
      read green, and `KafkaSink`'s other rows stay green (invariants 2 and 7).
- [ ] With every report carrying code 87, a batch through the worker reads `flush()` false and one
      failed batch, and each message is produced once. With every report carrying code 10, it reads
      `flush()` true with every message `UNKNOWN`. With every report carrying code 29, it reads
      `flush()` true with every message `UNKNOWN`, counted once in `failed`, and one line (invariants 2
      and 7).
- [ ] `probe_misc.py` case 3 against the branch reads `flush()=True`, `failed_batches=0`, `failed=25`
      and one line, with no count after `flush()` returned (invariants 2 and 11).
- [ ] `spec094_stuck_partition.py`, re-run as a test: with one message never reported and every other
      reported at the next poll, `flush_timeout=1.0`, the first emit returns in no less than 1.0 s
      and within 2.0 s, and each of the next three returns within 0.5 s with no new `failed` count.
      The same test, with the round calling `flush(flush_timeout)` instead, takes 1.0 s for each
      emit, which shows it reads the per-round wait (invariants 2 and 3).
- [ ] A report served after its round settled, by a later round's poll or by `close()`, moves no
      counter and writes no line (invariant 2).
- [ ] With no callback served and `flush` returning 0, every message is `ACCEPTED`. With it
      returning 1, every message is `UNKNOWN` at the deadline (invariants 2 and 7).
- [ ] Under a stop signal set before the round, with a stand-in whose `flush` serves one partition's
      reports per call and returns, the four partitions' reports due at 10, 18, 26 and 34 ms, all 40
      messages are `ACCEPTED`. The same test, with the first slice's end set to the wait's start,
      reads 30 of the 40 `UNKNOWN`, which shows the first slice is a deadline and not one pass
      (invariants 2 and 7).
- [ ] With a stand-in whose `flush` returns 1 at once and never reports, `flush_timeout=0.5`, one
      `emit` uses less than 0.1 s of CPU time across its 0.5 s wait, with the stop signal unset and
      with it set before the round. The same test, with the pause between passes removed, uses
      CPU time within 0.1 s of its wall time, and with `pause_on` returning the set signal, uses
      that much over the 50 ms first slice (invariant 3).
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
  default), in slices of `_POLL_INTERVAL` (0.05 s) through `result(timeout=slice)`. Every future is
  asked at least once. The first slice is a deadline, the wait's start plus `_POLL_INTERVAL`: once
  it has passed and the stop signal is set, each later ask waits nothing, and a future still
  unresolved reads as unknown. A `result` that raises `TimeoutError` before its slice is over, as a
  stand-in's may, is followed by a pause for the rest of the slice through `_retry.wait`, cut short
  by the stop signal unless it was already set when the pause began (`_retry.pause_on`).
- **A future resolved with a `TimeoutError`.** `result()` raises the builtin `TimeoutError` both for
  a future still pending and for one whose outcome is a `TimeoutError`. The two are told apart by
  the future's `done()`: a done future's `TimeoutError` is its outcome, read as any other
  exception, so `UNKNOWN` at once rather than after the whole deadline. A future with no `done()`,
  or whose `done()` raises, reads as pending.
- **Each future.**
  - A result: `ACCEPTED`.
  - An exception in the definitive set: `REJECTED`, with the line `GooglePubSubSink publish refused,
    <Type>`, one per event.
  - Any other exception, no result by the deadline or the stop, or a `result()` that takes no
    `timeout` (`TypeError`): `UNKNOWN`. The round's `UNKNOWN` events share one line,
    `GooglePubSubSink publish unconfirmed (<first Type>); may have landed, not retried`.
- **The definitive set**, resolved through `_classify.driver_classes`, so nothing is imported:
  - `google.api_core.exceptions.InvalidArgument`. The server returns it for a publish request it
    refuses whole, and the client sets the request's error on every future in the client's batch
    (`publisher/_batch/thread.py:344-371`). So it does not judge one message's own bytes: a valid
    message batched beside an invalid one is refused with it. What holds is that no attempt wrote
    it, because the server applies a publish request whole or not at all, and the publish retry
    (`transports/base.py:179-197`) does not retry `InvalidArgument`, so the refusal follows no
    attempt the server kept. That a refused request is applied to none of its messages is an
    inference from the API's meaning, not a measurement, and *Evidence* says so:
    `google/api_core/exceptions.py:266-275` only maps status 400 and gRPC `INVALID_ARGUMENT` to the
    class.
  - `google.cloud.pubsub_v1.publisher.exceptions.FlowControlLimitError`, which google-cloud-pubsub
    2.43.0 sets on a future it never sent (`publisher/client.py:405-423`).

  The draft's `PermissionDenied`, `Unauthenticated` and `NotFound` are now `UNKNOWN`, because the
  publish retry (`transports/base.py:179-197`) can end in one of them after a `DeadlineExceeded`
  attempt the server kept. A batch whose RPC failed after its retries carries that RPC's
  `GoogleAPIError` on every future (`publisher/_batch/thread.py:344-371`).
- **What goes.** Every future has its verdict when its round returns, so nothing awaits one
  between emits, and `_reap`, `_await_overflow`, `_resolve_within`, `_drain_pending`, `_resolve`
  and the pending list's use go. `max_pending` is still accepted and validated, because it is
  frozen surface, and it has no effect, which its docstring says. FR-004 records it in invariant
  13's exceptions and in the bad-argument decision. `flush()` refuses when closed and otherwise
  returns, since no verdict is outstanding.
- **`close()` waits for the unconfirmed** (Andrew's "Wait at close (Recommended)", 2026-11-22).
  A round keeps, under `_futures_lock`, the futures it left `UNKNOWN`, and only those, dropping
  at each keep the ones already kept that have resolved; a future whose `result` takes no
  `timeout` is not kept, and one whose `done()` raises is not waited on. `close()` sets the flag
  and takes the kept
  futures under `_futures_lock`, then waits on them outside it under one deadline of
  `overflow_timeout`, **not** cut short by the stop signal. The wait counts nothing and writes
  no line: each verdict was given and counted by its round (SPEC-088's one-term ledger). A second
  `close()` waits on nothing, a round settling after the close keeps nothing, and a list kept under
  another pid, a parent's inherited through `os.fork()`, is dropped, so a child's own `close()`
  waits on nothing its parent kept (invariant 12). The reason is
  the exit: the exit drain runs with the stop signal set, so a round reads a publish still in
  flight after its first 0.05 s slice as `UNKNOWN`, while google-cloud-pubsub's daemon commit
  thread is still sending it; a process that exits on that verdict ends the send. Measured with
  the second diff review's stand-in at 0.2 s latency, flag-only landed none of seven.

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
- `close()` waiting up to `overflow_timeout`, whatever the stop signal says, for the publishes its
  rounds left `UNKNOWN`, so a normal exit lets the client finish sending them, counting nothing,
  and an exit against a server that never answers takes up to that much longer;
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
- [ ] Under a stop signal set before the round, with 1,000 futures that resolve between 10 and 30 ms
      after the publish, every event is `ACCEPTED`. The same test, with the first slice's end set
      to the wait's start, reads events `UNKNOWN`, which shows the first slice is a deadline
      (invariants 2 and 7).
- [ ] With futures whose `result` raises `TimeoutError` at once and whose `done()` is false,
      `overflow_timeout=0.5`, one `emit` uses less than 0.1 s of CPU time across its 0.5 s wait,
      with the stop signal unset and with it set before the round. The same test, with the pause
      removed, uses CPU time within 0.1 s of its wall time (invariant 3).
- [ ] A future that is done and whose `result` raises `TimeoutError` reads `UNKNOWN`, its line
      naming `TimeoutError`, and the `emit` returns within 1.0 s with `overflow_timeout=5`, so it
      never waited on the deadline; the build's probe measures how much sooner (invariants 3 and
      7).
- [ ] A close landing mid-round makes the later publishes `UNKNOWN` without raising, and
      `test_a_close_landing_mid_batch_counts_the_rest_unconfirmed_and_does_not_raise` passes
      (invariant 7).
- [ ] `GooglePubSubSink.__init__`'s signature is unchanged, and `max_pending` still refuses a non-`int`
      with `TypeError` (invariants 10 and 13).
- [ ] A process that logs five events inside one `@trace` span, seven with the span's own, and
      exits normally, against a stand-in client whose daemon threads take 0.2 s to send, lands
      seven of seven, and the `failed` its exit drain's round counted, seven, is unchanged by
      `close()`. With `close()` back to setting the flag only, in an export of the branch head,
      the test fails (invariants 2 and 7).
- [ ] `close()` over futures that never resolve, after a round under a set stop signal, returns in
      no less than `overflow_timeout` (1.0 s) and within 1.5 s, changes no counter, writes no
      line, and a second `close()` returns within 0.5 s (invariants 3 and 7).

### FR-003: The corpus holds the reports the two sinks now wait for

#### Description:

These are changes to SPEC-092's files as its build delivers them, after SPEC-093's.

- **The Kafka double.** It already serves its reports at `flush`, which the round's `flush(slice)`
  reaches. Its definitive report becomes code 87 rather than 29, which SPEC-092's fault table now
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
    `UNKNOWN` in its round, counted, and never waited on at `close()`, which waits under
    `overflow_timeout` only on kept futures whose `result` takes a `timeout`.
- **§13 gains five bullets.**
  - "**A client-buffered sink's batch costs a broker round trip.**" It carries the measured figures
    at batch 10 and at 1,000, and the real broker's figures, which do not grow with `linger.ms`.
  - "**With one Kafka partition stuck, each later `KafkaSink` round pays one 50 ms slice.**" It
    carries the 0.05 to 0.06 s measured against 0.50 s for a single blocking flush.
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
  - the partial-outage cost: with one partition stuck, only that partition's messages wait the
    timeout, and each later round pays one 50 ms slice;
  - the dead-broker cost: each batch, and each orphan log, waits the timeout;
  - definite rejections (the two sets) failing the batch, and every other report, authorization and
    size refusals among them, counted `UNKNOWN` with `flush()` truthy;
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
      full-client-queue item is open with its own lead, and §13 carries the five bullets.
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
# src/log_foundry/sinks/_retry.py
def pause_on(stop: threading.Event | None) -> threading.Event | None: ...   # new: None once set

# src/log_foundry/sinks/kafka.py
DEFINITIVE_CODES: frozenset[int] = frozenset({21, 87})
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
├── _retry.py     # FR-001, FR-002: pause_on
├── kafka.py      # FR-001
└── pubsub.py     # FR-002
tests/
├── conformance_doubles.py, conformance_pins.py   # FR-003
└── test_*.py     # new tests, and the ones that change (Evidence)
docs/             # FR-004
```

## Implementation Phases

### Phase 1: `KafkaSink` on the engine (FR-001)

- `_retry.pause_on`, then the round, the sliced wait, the report table, `DEFINITIVE_CODES`, the
  counters as properties, the stop signal and the `flush()` hook, with the tests that change.

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
three runs on Python 3.13.1 and one on 3.12.12, from the committed patches applied to a fresh
export, and row for row the same as the previous revision's prototype. Exactly these four rows move:

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
stay green: with no wait, every produced message is `UNKNOWN`. That was measured on this revision's
sliced wait and on the previous revision's polls at `97a03f45`, as it was on the draft's
`flush`-based wait at `33582484`.

**The Pub/Sub double's classes matter.** With the double raising its local `PermissionDenied`, the
prototype read `GooglePubSubSink-never-acknowledged` as `ambiguous-when-knowable`. A refusal the sink
cannot recognise by class reads as indefinite. With the stub module's class, the row read green.

**The per-round wait.** `spec094_stuck_partition.py`, with one message never reported:

| | emit 0 | emit 1 | emit 2 | emit 3 |
|---|---|---|---|---|
| the draft, `flush(flush_timeout)` | 0.50 s | 0.50 s | 0.51 s | 0.50 s |
| the previous revision, `flush(0)` and polls | 0.50 s | 0.00 s | 0.00 s | 0.00 s |
| this spec, `flush(slice)` | 0.50 s | 0.06 s | 0.05 s | 0.05 s |

Each counted the stuck message once in `failed`. The slice each later emit pays is the stand-in's
`flush` holding the slice while the stuck message is queued, which a real `rd_kafka_flush` does
too, since it returns early only once the queue is empty. `spec094_dead_broker_flush.py`, with a producer that
never reports and `flush_timeout=0.5`:
- under the draft, `log_foundry.flush(timeout=5)` returned `FlushResult(ok=False,
  reason='sink-flush')` after 1.01 s;
- under this spec it returned `ok=True` after 0.50 s.

In both, an orphan `info()` held the caller's thread for 0.50 s and every event was counted in
`failed`. With the stop signal set 0.3 s into a wait whose timeout is 5 s, an `emit` returned after
0.32 to 0.34 s for each sink, its events `UNKNOWN`.

**The first slice, the pause and a resolved `TimeoutError`.** `spec094_first_slice_and_pause.py`
against this revision's prototype, against the previous revision's (`59c1fbf0`), and against this
revision with each re-plant:

| Case | this revision | `59c1fbf0` | re-plant |
|---|---|---|---|
| Kafka, stop set before the round, 40 reports by 34 ms | 0 of 40 `UNKNOWN`, 34 to 35 ms | 40 of 40 | first slice ends at the start: 30 of 40, 10.8 ms |
| Pub/Sub, stop set before the round, 1,000 futures by 30 ms | 0 of 1,000 `UNKNOWN` | 349 of 1,000 | first slice ends at the start: 536 of 1,000 |
| Kafka, `flush` returns 1 at once, 0.5 s timeout, stop unset | CPU 0.001 s of 0.504 s, 11 calls | CPU 0.001 s | no pause: CPU 0.500 s of 0.500 s, 2,044,100 calls |
| the same, stop set before the round | CPU 0.000 s of 0.054 s, 2 calls | returned at once | `pause_on` returning the set stop: CPU 0.050 s of 0.050 s, 51,294 calls |
| Pub/Sub, `result` raises `TimeoutError` at once, 0.5 s timeout, stop unset | CPU 0.001 s of 0.503 s, 11 calls | CPU 0.013 s | no pause: CPU 0.500 s of 0.500 s, 2,102,089 calls |
| the same, stop set before the round | CPU 0.000 s of 0.052 s, 2 calls | returned at once | `pause_on` returning the set stop: CPU 0.050 s of 0.050 s, 68,729 calls |
| Pub/Sub, a future done with `TimeoutError`, 5 s timeout | `UNKNOWN` in under 1 ms | `UNKNOWN` after 5.003 s | none |

The review's own stand-ins, `kstop.py` and `pstop.py`, read 30 of 40 and 900 of 1,000 `UNKNOWN`
against `59c1fbf0`. The previous revision's rows under a set stop returned at once because its
first slice ended at its first pass, which is S3; with the slice made a deadline and no
`pause_on`, the same stand-ins burned the 50 ms slice in a tight loop, which is the re-plant column.

**The suite.** The full suite was run against the prototype in a scratch export with no `docs/`.
Against SPEC-093's prototype at `97a03f45`, 52 more tests fail, 129 in all against 77. The draft's
`flush`-based prototype failed 52 at `33582484` and 53 at `bb6656ff`, which added
`tests/test_class_claim.py`, and the previous revision failed 54. Two of those 54,
`test_sink_retry.py::test_only_the_engine_waits_or_backs_off` and
`test_engine_sinks_roster.py::test_the_engine_sinks_are_the_sixteen`, now fail on SPEC-093's
prototype already, because its end-of-round wait trips the same two gates, so they are SPEC-093's
count and not this spec's. The sliced `flush`, the first-slice deadline and `pause_on` fail no test
the previous revision did not. A first version of the per-round wait, which polled and read
`__len__` without `flush`, failed eight more. Each used a stand-in producer that serves reports only
at `flush`, so it waited the full timeout and read its messages as `UNKNOWN`. In an earlier run, at
a one-minute load near 100, two unrelated `_otel` tests failed once and passed when rerun serially.
They are not counted. The 52 are in these files:

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
| `test_sink_post_close.py` | 1 |
| `test_fork_roster.py` | 1 |
| `test_class_claim.py` | 1 |

With each sink's change applied alone on top of SPEC-093's, at `33582484` with the draft's wait, 25
of the 52 failed with `KafkaSink`'s and 33 with `GooglePubSubSink`'s. 7 failed with either, and one
failed only with both: `test_deferral.py::test_every_run_context_count_of_a_dropped_counter_is_settled`,
which expects a stored `rejected` counter that neither sink then keeps. Each of the 52 asserts what
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
  in code that moved.** `test_engine_sinks_roster.py::test_the_engine_sinks_are_the_sixteen`, which
  also finds the two new engine sinks, already fails on SPEC-093's prototype and is counted there.
  - The two in `test_delivery_engine.py`.
  - The five in `test_classify.py`, the retry-loop sweep and the wrapper exclusions.
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
time after it was handed over, and whose `flush` serves reports until its queue is empty or its
timeout passes, as librdkafka's does. It gives the median `emit` of seven, at a one-minute load near
5, measured on this revision's sliced wait; the rows at `97a03f45` are the previous revision's:

| Sink | ack | batch 10 | batch 100 | batch 1,000 |
|---|---|---|---|---|
| `KafkaSink` at `97a03f45` | any | 0.02 ms | 0.13 to 0.14 ms | 1.3 to 1.4 ms |
| `KafkaSink`, waiting | 1 ms | 1.10 ms | 1.32 ms | 4.07 ms |
| `KafkaSink`, waiting | 5 ms | 5.17 ms | 5.40 ms | 8.05 ms |
| `KafkaSink`, waiting | 20 ms | 20.22 ms | 20.57 ms | 23.18 ms |
| `GooglePubSubSink` at `97a03f45` | any | 0.02 ms | 0.13 to 0.14 ms | 1.2 to 1.4 ms |
| `GooglePubSubSink`, waiting | 1 ms | 1.32 ms | 1.45 ms | 3.28 ms |
| `GooglePubSubSink`, waiting | 5 ms | 6.30 ms | 6.47 ms | 8.02 ms |
| `GooglePubSubSink`, waiting | 20 ms | 23.88 ms | 22.65 ms | 27.62 ms |

So one batch costs about one round trip. At today's batches of 10 events and a 5 ms round trip, the
drain thread delivers about 1,900 events a second to Kafka and 1,600 to Pub/Sub, where it delivered
about 600,000 at `97a03f45`. At SPEC-090's cap of 1,000 events a batch it delivers about 124,000 to
each. SPEC-090's size target is `events / elapsed × flush_interval / 2`, which reaches the cap under
a backlog once a round trip is the cost, so a backlog lifts the batch size to where the wait matters
least. With a client that never acknowledges and the timeout at 0.5 s, one `emit` took 500.3 ms for
`KafkaSink` and 502.9 ms for `GooglePubSubSink`, against 0.02 ms at `97a03f45`.

**Against a real broker.** `spec094_real_broker_linger.py` ran `KafkaSink.deliver` with a
confluent-kafka 2.16.1 `Producer`, whose bundled librdkafka reads `2.16.0` from `libversion()`,
against the Kafka 3.9.1 image `tests/integration/docker-compose.yml` pins, from a container on the
broker's network, ten messages a `deliver`. Milliseconds per `deliver`, first to last:

| Case | the previous revision, `flush(0)` and `poll(slice)` | this revision, `flush(slice)` |
|---|---|---|
| `linger.ms` 5 (the default) | 6.3 to 8.5, all `ACCEPTED` | 0.7 to 4.5, all `ACCEPTED` |
| `linger.ms` 100 | 104.4 to 108.7, all `ACCEPTED` | 0.5 to 2.8, all `ACCEPTED` |
| `linger.ms` 1000, `flush_timeout` 5 | 1,006.0 to 1,008.4, all `ACCEPTED` | 0.4 to 2.8, all `ACCEPTED` |
| `linger.ms` 3000, `flush_timeout` 1 | 1,001.0 to 1,002.1, **all `UNKNOWN`** | 0.5 to 2.2, all `ACCEPTED` |
| `delivery.report.only.error`, `linger.ms` 200 | 205.0 to 209.3, all `ACCEPTED` | 0.7 to 3.1, all `ACCEPTED` |
| stop set before the round, `linger.ms` 1000 | 51.3 to 55.9, **all `UNKNOWN`** | 0.5 to 2.7, all `ACCEPTED` |
| no broker, `flush_timeout` 0.5 | 501.4 to 502.7, all `UNKNOWN` | 501.0 to 501.9, all `UNKNOWN` |
| no broker, stop set before the round | 2.4 to 52.6, all `UNKNOWN` | 51.2 to 52.2, all `UNKNOWN` |
| a 2 MB message, producer `message.max.bytes` above the topic's | `REJECTED` (code 10) | `UNKNOWN` |

So under the previous revision each round paid the producer's whole `linger.ms`, and a `linger.ms`
at or above `flush_timeout`, or a set stop, read every delivered message as `UNKNOWN`. Under
`flush(slice)` no round paid `linger.ms`, and each delivered message read `ACCEPTED`. The first
`deliver` of each case pays the connection to the partition leader.

**What the prototype could not settle.**

- That librdkafka never sends 21 or 87 after an earlier attempt that wrote the message. Both judge
  what every attempt carries unchanged, the producer's `acks` and the record's own content, but a
  broker or topic setting changed between attempts, such as a topic made compacted, would break that
  for 87. It is read from the Kafka protocol and librdkafka v2.16.0's source, not measured.
- That Pub/Sub applies a publish request it refuses with `InvalidArgument` to none of its messages,
  an inference from the API's meaning.
- How Pub/Sub's real client behaves, and Kafka's beyond the cases measured above. That is owed to
  `integration.yml`'s Kafka and Pub/Sub tests on the PR's branch.

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
- Revised again on 2026-11-16, after the execution-frame review of both specs.
  - **S2, the linger cost.** The round's `flush(0)` held librdkafka's flushing mark only while it
    ran (`rdkafka.c:6492-6493`), so the poll after it waited out `linger.ms`: measured against a real
    broker, a round cost the producer's whole `linger.ms`. Each pass now calls `flush(slice)`, which
    holds the mark for the slice, and the same broker measured each round at a few milliseconds
    whatever `linger.ms` was, including `linger.ms` above `flush_timeout`. The cost this moves is a
    stuck partition's: each later round pays one slice, 0.05 to 0.06 s, which FR-001 states and its
    criterion allows. The "at worst a round pays that 5 ms" bullet is withdrawn, and the bundled
    librdkafka is measured at 2.16.0.
  - **S3.** The first slice is a deadline from the wait's start, not one pass, so a round under a
    set stop takes every report inside it; each sink has a criterion with its re-plant.
  - **S4.** A pause between two asks now waits even when the stop was set before it began
    (`_retry.pause_on`). The review's stand-ins burned the whole slice in a tight loop under a set
    stop, and each sink now has a CPU-time criterion with its re-plants. The same pause bounds a
    stand-in whose `flush` returns at once, which was S7.
  - **S6.** A Pub/Sub future resolved with a `TimeoutError` is told from a pending one by `done()`.
  - **S8.** Kafka's set narrowed to `{21, 87}`: 10 and 18 judge the batch librdkafka builds again
    for each attempt, and a real broker's 2 MB refusal now reads `UNKNOWN`. Pub/Sub's
    `InvalidArgument` stays definitive on "no attempt wrote it", not on the message's own bytes,
    since the refusal lands on the whole client batch. The corpus's Kafka double and SPEC-092's
    example now use 87.
- Corrected on 2026-11-20, at completion, from the build.
  - **FR-002's ceiling for a future resolved with a `TimeoutError` is 1.0 s, not 0.1 s.** The
    repository's timing-floor gate, `tests/test_timing_floors.py`, refuses a test ceiling under
    its 0.5 s stall allowance, because a CI stall can reach one. The test asserts 1.0 s against an
    `overflow_timeout` of 5 s, which still shows the round never waited on the deadline, and the
    build's probe, `docs/audits/2026-11-14-probes/sinks/spec093_094_build_results.txt`, measured
    the `emit` at 0.000 s. FR-001's stuck-partition criterion kept its 0.2 s ceiling at
    completion; the first diff review corrected it below.
- Corrected again on 2026-11-20, by the first diff review's ruling.
  - **FR-001's stuck-partition criterion uses `flush_timeout=1.0`, with 0.5 s for the later
    emits.** Its 0.2 s ceiling sat under the timing-floor gate's 0.5 s stall allowance, which a CI
    stall can reach. With a 1.0 s timeout the later emits' ceiling is the allowance itself, the
    first emit is bounded at 1.0 s to 2.0 s, and the one-blocking-flush re-plant, which costs every
    emit the whole timeout, still fails it.
  - **The report table takes a lock.** The previous text said it took none, relying on a dict's
    atomic store across threads; a round's `flush` can serve another round's callbacks on another
    thread, so every write, the round's reads and the close now take the sink's counters lock.
- Corrected on 2026-11-22, by the second diff review's ruling.
  - **The report table reads the counters lock at each use.** The 2026-11-20 correction had the
    table keep the lock object it was handed, on the reasoning that a fork's repair walk reaches
    that lock. It reaches it through the sink's `SinkCounters`, and rebinds it there in a child;
    a settled table is reachable only through the callback closure librdkafka holds, so it kept
    the parent's lock, and a late report a child's close flush or `flush()` hook served would
    block on it if a parent thread held it at the fork (invariant 12). The table now holds the
    `SinkCounters` and reads `_counter_lock` at each use, as 1.x's `_on_delivery` read it
    through the sink on every call.
  - **`GooglePubSubSink.close()` waits for the unconfirmed: Andrew, 2026-11-22, "Wait at close
    (Recommended)".** The second diff review found that a normal exit lost every publish still in
    flight: the exit drain's round reads it `UNKNOWN` after its first slice under the set stop
    signal, and `close()`, which only set the flag, let the process end the client's daemon
    sending thread. FR-002's *What goes* bullet, its `close()` sentence and *A caller sees* are
    corrected in place, and two criteria are added: the exit delivery, and the bound on the
    close's wait.
  - **The kept list is dropped in a forked child**, by the execution review's ruling: it records
    the pid that kept it, so a caller's direct `close()` in a child no longer waits
    `overflow_timeout` on futures only the parent's client threads resolve (invariant 12).
