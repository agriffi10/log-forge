---
type: Spec
id: SPEC-094
title: "Client-Buffered Sinks Wait for the Delivery Report, on the Delivery Engine"
description: "Moves KafkaSink and GooglePubSubSink onto the delivery engine, with each round waiting within the sink's own timeout for the client's report on every message it handed over, so that a definitive refusal fails the batch, an indefinite report is counted as one that may have landed, and no count arrives after the batch's verdict."
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
2026-11-15 that each batch waits for its reports. This spec builds that decision. Each sink becomes a
delivery-engine sink whose round hands every message over and then waits, within the sink's existing
timeout, for the reports on that round's messages. A report that says the broker refused is a loss,
and it fails the batch when nothing else landed. A report that cannot say whether the broker kept the
message, or no report in time, is counted and announced as one that may have landed, and it is never
sent again. Nothing about the batch is counted after its verdict.

## Scope

### In Scope

- `KafkaSink` on the delivery engine, waiting for its delivery reports in the round (FR-001).
- `GooglePubSubSink` on the delivery engine, waiting for its publish futures in the round (FR-002).
- The conformance corpus's Pub/Sub double and the two finding-12 pins (FR-003).
- The record: `architecture.md` §12 and §13, the decisions with Andrew's answer quoted, the
  invariants page, the README, the rosters and the pending release notes (FR-004).

### Out of Scope

- **Whether a full client queue is retried.** That is the rest of finding 12, and it stays open in
  §12. FR-001 and FR-002 keep 1.x's reading of a refused hand-off exactly. A refusal beside a hand-off
  that worked is dropped and counted, as before. A round that handed nothing over is sent again,
  because the 1.x worker resent a batch refused whole.
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
and `bb6656ff`, and SPEC-086 then landed as `9d70f782`. None of the three changes a line this spec
cites, and every `file:line` here was re-read at `9d70f782`. The prototype was rebuilt on
`9d70f782`. **The build re-verifies every `file:line` against `main` as it stands.** Neither sink
calls `delivery_error`, so this spec assumes no definition of it. Every type test FR-001 and FR-002
add is `_diag.is_a`, as *A type test reads the real type, never the value's __class__*
(`docs/decisions/failure-paths/`) requires.

**Andrew's decision B, made on 2026-11-15**, binds every FR here. He was asked whether a
client-buffered sink's verdict may come before the client's report, and he answered **"Wait for the
report (Recommended)"**. Each batch's `emit` waits for the client's delivery reports within the sink's
existing timeout, as core NATS confirms. The question's gloss went on to say that a truthy `flush()`
then means the broker acknowledged. That gloss is reconciled here with SPEC-089's settled decision,
made by Andrew later the same day: `flush()` stays truthy over a counted loss ("Keep truthy, add
counts"). So under this spec **a truthy `flush()` means every event was either acknowledged or
counted**, not that the broker acknowledged every one. A definitive refusal of a whole batch fails it
and `flush()` reads false. An indefinite report is counted as one that may have landed, announced, and
leaves `flush()` truthy, as core NATS does for a batch whose confirmation never came (invariant 7's
recorded exception). A caller sees those counts in `health().sink`, and in `FlushResult`'s loss counts
once SPEC-089 is built.

**The rule both sinks share.** A round hands every remaining event to the client, then waits for the
reports on the messages it handed over. The wait is bounded by one deadline, the sink's own timeout,
and it is not cut short by the stop signal. *A shutdown shortens a wait; it must never skip work*, and
a verdict cut short on the exit drain would make every exit batch `UNKNOWN`, which core NATS's
confirmation also refuses. Each message the round handed over then has one outcome:

| The report | Outcome | Counted | Line, once per round unless it says otherwise |
|---|---|---|---|
| success | `ACCEPTED` | nothing | none |
| a definitive refusal (the sink's list below) | `REJECTED` | `failed` | one per message, 1.x's text |
| any other error, or none by the deadline | `UNKNOWN` | `failed` | `may have landed, not retried` |

A run with no event `ACCEPTED`, at least one `REJECTED` and none `UNKNOWN` fails, by the engine's rule
(SPEC-072). The worker then counts one failed batch, and `flush()` reads false. A run with any
`UNKNOWN` event settles. A report that arrives after its round settled changes no count and writes no
line.

### FR-001: `KafkaSink` waits for its delivery reports inside the round

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
(`kafka.py:68`). The engine waits between rounds on it, and the in-round report wait does not. The
sink has no `max_retries`, so through the worker a run has the worker's `max_retries + 1` rounds,
which is the 1.x total of attempts.

**The round.**

- **Hand-off.** A closed or closing sink refuses through the guard with 1.x's error, as today. Every
  event is encoded once per run (`encode_round`), and an unencodable one is `DROPPED` with SPEC-069's
  line. Each encodable event is produced with a callback bound to the round's own report table, and
  `poll(0)` follows each produce, as today.
- **A refused `produce()`.** In a round where another message was produced, the refused message is
  `DROPPED` and counted in `rejected` only when the run settles, with 1.x's line `KafkaSink produce,
  <Type>`. In a round where nothing was produced, every refused message is `RETRYABLE`, and the
  engine resends it after its wait, as the 1.x worker resent a batch refused whole.
- **The wait.** After the last produce, `producer.flush(flush_timeout)`. The documented
  `Producer.flush` serves every report of the messages queued when it was called before it returns
  0. It returns the number still queued when its timeout expires. confluent-kafka 2.16.1's
  `Producer_flush` waits in 200 ms chunks so that a signal can interrupt it (`Producer.c:480-530`).
- **Each produced message.**
  - A report with no error: `ACCEPTED`.
  - A report whose error code is in `DEFINITIVE_CODES`: `REJECTED`, with 1.x's line `KafkaSink
    delivery, <Type> code=N`, one per message.
  - A report with any other error: `UNKNOWN`.
  - No report: `ACCEPTED` when `flush` returned the `int` 0, because the producer then served every
    report it will send, and one configured with `delivery.report.only.error` sends none for a
    success (`Producer.c:163-165`). `UNKNOWN` otherwise.

  The round's `UNKNOWN` messages share one line: `lost N message(s); KafkaSink, no definitive delivery
  report within <flush_timeout>s; may have landed, not retried`.
- **Late reports.** The report table is closed when the round reads it, so a report served later, by
  another round's flush or by `close()`, changes nothing. The table takes no lock, because a write
  after the round read it lands in a table nothing reads. The fork roster
  (`tests/test_fork_roster.py`) needs no new home.
- **The close.** `_flush_bounded` still flushes within `flush_timeout`, so messages a round left
  `UNKNOWN` may still land. It counts nothing, because every message was settled by its own round.
  1.x's "still queued when the close flush expired" count and the `released_during` count both go,
  since each counted a message after its batch's verdict.

**`DEFINITIVE_CODES`** is `frozenset({2, 10, 17, 18, 21, 29, 31, 87})`. These are the Kafka protocol
codes a broker returns only for a produce request it did not write: `CORRUPT_MESSAGE`,
`MESSAGE_TOO_LARGE`, `INVALID_TOPIC_EXCEPTION`, `RECORD_LIST_TOO_LARGE`, `INVALID_REQUIRED_ACKS`,
`TOPIC_AUTHORIZATION_FAILED`, `CLUSTER_AUTHORIZATION_FAILED` and `INVALID_RECORD`. Every other code is
indefinite. That includes `REQUEST_TIMED_OUT` (7), `NOT_ENOUGH_REPLICAS_AFTER_APPEND` (20) and
librdkafka's local `-192` (`_MSG_TIMED_OUT`), because each can follow a write the leader kept. A code
is read only when `code()` returns an exact `int`, as `_code` reads it today.

**A caller sees** a broker round trip on every batch, on the drain thread and, outside a span, on the
caller's own thread (invariant 4). With a broker that never answers, every batch waits
`flush_timeout`, 10 s by default, and its messages are counted `UNKNOWN`. `failed` counts each lost
message once per run, where 1.x counted at every report. A batch whose every report is a definitive
refusal reads `flush()` false, where 1.x read it true. A `FilteringSink` or `TransformSink` around the
sink now offers `deliver` (SPEC-075).

#### Acceptance Criteria:

- [ ] `KafkaSink-never-acknowledged` and `KafkaSink-report-indefinite` of `tests/test_conformance.py`
      read green, and `KafkaSink`'s other rows stay green (invariants 2 and 7).
- [ ] With every report carrying code 29, a batch through the worker reads `flush()` false and one
      failed batch, and each message is produced once (invariant 7).
- [ ] `probe_misc.py` case 3 against the branch reads `flush()=True`, `failed_batches=0`, `failed=25`
      and one line, with no count after `flush()` returned (invariants 2 and 11).
- [ ] A report served after its round settled, by a later round's flush or by `close()`, moves no
      counter and writes no line (invariant 2).
- [ ] With `flush` returning 0 and no callback served, every message is `ACCEPTED`. With `flush`
      returning 1 and no callback served, every message is `UNKNOWN` (invariants 2 and 7).
- [ ] With no report ever and `flush_timeout=0.5`, one `emit` returns within 0.5 s plus 0.1 s,
      counting its messages in `failed` once, and a set stop signal does not shorten it (invariants 3
      and 7).
- [ ] A round where every `produce()` raises `BufferError` is resent by the engine, and a round where
      one of two raises counts that one in `rejected` once the run settles (invariant 2).
- [ ] `KafkaSink.__init__`'s signature is unchanged, and `failed`, `rejected`, `losses()`, `flush()`
      and `close()` keep their names (invariant 10).

### FR-002: `GooglePubSubSink` waits for its publish futures inside the round

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
  default), in slices of `_POLL_INTERVAL` through `result(timeout=slice)`. The deadline is not cut
  short by the stop signal.
- **Each future.**
  - A result: `ACCEPTED`.
  - An exception in the definitive set: `REJECTED`, with the line `GooglePubSubSink publish refused,
    <Type>`, one per event.
  - Any other exception, no result by the deadline, or a `result()` that takes no `timeout`
    (`TypeError`): `UNKNOWN`. The round's `UNKNOWN` events share one line,
    `GooglePubSubSink publish unconfirmed (<first Type>); may have landed, not retried`.
- **The definitive set**, resolved through `_classify.driver_classes`, so nothing is imported:
  - `google.api_core.exceptions` `InvalidArgument`, `Unauthenticated`, `PermissionDenied` and
    `NotFound`. In google-api-core 2.31.0 these are status codes 400, 401, 403 and 404, and the
    server returns them only for a request it did not take (`google/api_core/exceptions.py:272-321`).
  - `google.cloud.pubsub_v1.publisher.exceptions.FlowControlLimitError`. google-cloud-pubsub 2.43.0
    sets it on a future it never sent (`publisher/client.py:405-423`).

  A batch whose publish RPC failed after its retries carries that RPC's `GoogleAPIError` on every
  future (`publisher/_batch/thread.py:344-371`). So a `DeadlineExceeded`, a `ServiceUnavailable` or a
  `PublishError` is `UNKNOWN`.
- **What goes.** Nothing is outstanding between emits, so `_reap`, `_await_overflow`,
  `_resolve_within`, `_drain_pending`, `_resolve` and the pending list's use go. `max_pending` is still
  accepted and validated, because it is frozen surface, and it has no effect, which its docstring
  says. `flush()` refuses when closed and otherwise returns, since nothing is in flight. `close()`
  sets the flag.

**A caller sees** a round trip per batch, and a batch that holds the drain thread for up to
`overflow_timeout` against a server that never answers, where 1.x waited only past `max_pending`.
`failed` counts once per run. `flush()` reads false for a batch every future of which was refused
definitively. A future type without a `timeout` reads as `UNKNOWN` at once, where 1.x waited on it,
unbounded, at `close()`. A wrapper around the sink offers `deliver`.

#### Acceptance Criteria:

- [ ] `GooglePubSubSink-never-acknowledged` and `GooglePubSubSink-report-indefinite` read green, and
      the sink's other rows stay green (invariants 2 and 7).
- [ ] With every future raising a `PermissionDenied` from a stub `google.api_core.exceptions`, a batch
      through the worker reads `flush()` false and one failed batch (invariant 7).
- [ ] With every future raising `DeadlineExceeded`, the batch settles, its events are counted in
      `failed` once, and one line names the type (invariants 2 and 11).
- [ ] With futures that never resolve and `overflow_timeout=0.5`, one `emit` returns within 0.6 s
      with its events `UNKNOWN`, and a set stop signal does not shorten it (invariants 3 and 7).
- [ ] A close landing mid-round makes the later publishes `UNKNOWN` without raising, and
      `test_a_close_landing_mid_batch_counts_the_rest_unconfirmed_and_does_not_raise` passes
      (invariant 7).
- [ ] `GooglePubSubSink.__init__`'s signature is unchanged, and `max_pending` still refuses a non-`int`
      with `TypeError` (invariants 10 and 13).

### FR-003: The corpus holds the reports the two sinks now wait for

#### Description:

These are changes to SPEC-092's files as its build delivers them, after SPEC-093's.

- **The Pub/Sub double** raises `PermissionDenied` and `DeadlineExceeded` as classes of a stub
  `google.api_core.exceptions`, installed through `DRIVERS` with google-api-core's hierarchy for the
  classes it names: `GoogleAPIError`, `GoogleAPICallError`, `ClientError`, `Forbidden`,
  `PermissionDenied`, `ServerError`, `GatewayTimeout` and `DeadlineExceeded`. It no longer uses the
  local stand-in classes. At `33582484` no row moves with this change, because the sink reads no
  class.
- **The Kafka double** is unchanged. It already serves reports at `flush`.
- **Pins.** `verdict-before-refusal` and `verdict-before-indefinite-report` are removed, because their
  four rows read green.
- **Totals**, after SPEC-093's: 268 rows, 58 pinned under five pins, 15 departure rows and 195 that
  must pass.

#### Acceptance Criteria:

- [ ] The corpus's literals read 268 rows, 58 pinned, 15 departure rows and 195 that must pass
      (invariants 2 and 7).
- [ ] Against SPEC-093's verdicts, only the four rows of FR-001 and FR-002 change (invariants 2
      and 7).
- [ ] With `KafkaSink._round`'s `flush` call replaced by `remaining = len(produced)`, so that no
      report is waited for, in an export of the branch head, `KafkaSink-never-acknowledged` and
      `KafkaSink-clean` read `ambiguous-when-knowable` (invariant 2).
- [ ] With the Pub/Sub double raising its local `PermissionDenied` again, in an export of the branch
      head, `GooglePubSubSink-never-acknowledged` reads `ambiguous-when-knowable`, which shows the
      definitive set is read by class (invariant 7).

### FR-004: The record says the two sinks wait for the report

#### Description:

These are the completion ritual's edits, made on the pushed branch.

- **§12.** The finding-12 item, as SPEC-092 and SPEC-093 leave it, is rewritten in place and dated.
  Its verdict half is struck as closed by SPEC-094, with `probe_misc.py` case 3's numbers before and
  after. Its remaining clause becomes an open item of its own, led "**Whether a full client queue is
  retried**", which says that FR-001 and FR-002 keep 1.x's reading.
- **§13 gains two bullets.**
  - "**A client-buffered sink's batch costs a broker round trip.**" The bullet carries the measured
    figures at batch 10 and at 1,000.
  - "**Against a broker that never answers, a `KafkaSink` or `GooglePubSubSink` batch holds the drain
    thread for the sink's timeout.**"
- **Decisions in `docs/decisions/sink-delivery/`.**
  - A new decision for decision B. It quotes Andrew's question and answer with the date, records
    the reconciliation with SPEC-089's "Keep truthy, add counts" (a truthy `flush()` means every
    event was acknowledged or counted), and carries the shared rule's table.
  - A new decision for the two definitive sets and the no-report reading.
- **Settled decisions refined in place, each with a dated History entry naming SPEC-094.**
  - *A sink may report per event* (`docs/decisions/sink-delivery/`): the engine sinks now include
    these two.
  - *A raising `emit` counts nothing…*: the immediate counts of `_on_delivery`, `_resolve`, the close
    flush and `released_during` are gone.
  - *A wrapper offers `deliver` exactly when what it wraps does…*: wrappers over these sinks now
    offer it.
  - *The sinks keep four declared exceptions…*: row 3 is unchanged, and its History says why.
- **`docs/invariants.md`.**
  - Invariant 2: the list of hand-offs `KafkaSink` and `GooglePubSubSink` see refused, and their
    `rejected` count, are re-described for the engine.
  - Invariant 4: its recorded exceptions gain these two sinks' round trip on the caller's thread
    outside a span, beside core `NATSSink`'s.
  - Invariant 7: its recorded exceptions gain a batch these sinks settle on an indefinite report.
  - The *Guarded* lines name the new tests.
- **README.**
  - The wrapper paragraph (`README.md:688-690`) drops "other than `KafkaSink` and
    `GooglePubSubSink`".
  - The queue-and-stream paragraph (`:960-965`) says both sinks wait for the report within
    `flush_timeout` and `overflow_timeout`.
  - The `KafkaSink` and `GooglePubSubSink` rows (`:998`, `:1003`) say the same, and say that
    `max_pending` has no effect.
- **Rosters and prose gates that name these sinks**, updated to the tree. The engine sinks become 18,
  and the adopter, deferral, retry-loop, fork and retry-safe-prose rosters change as *Evidence* lists.
- **The release notes**, where SPEC-093's went, and worded as SPEC-090 words it. The file's title
  and opener, as SPEC-093 rewrites them, stay true with these changes in. The entry lists:
  - both sinks on the engine, with `deliver`;
  - the per-batch wait and its bound;
  - definitive refusals failing the batch;
  - indefinite reports counted once, with the new line;
  - no count after the verdict, and the close counts that go;
  - `max_pending` inert;
  - wrappers offering `deliver`;
  - the round trip on an orphan log.

  Its compatibility list names each one for a `>=1,<2` pin.
- **`docs/component-inventory.md`.** The two sinks' rows say they are engine sinks. No row is added,
  because no module is added.

#### Acceptance Criteria:

- [ ] §12's finding-12 item is struck as closed by SPEC-094, and the full-client-queue item is open
      with its own lead, and §13 carries the two bullets (invariant 2).
- [ ] The decision-B file quotes "Wait for the report (Recommended)" with the date 2026-11-15 and
      states the reconciliation with "Keep truthy, add counts", and the two new decision files and
      the four dated History entries exist (invariant 7).
- [ ] Invariants 4 and 7 name the new recorded exceptions (invariants 4 and 7).
- [ ] The README sites state the wait, and no README sentence says either sink hands off and returns
      without waiting (invariant 2).
- [ ] The release-notes entry exists and names each change listed (invariant 10).

---

## Data Model

```python
# src/log_foundry/sinks/kafka.py
DEFINITIVE_CODES: frozenset[int] = frozenset({2, 10, 17, 18, 21, 29, 31, 87})

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
└── test_*.py     # new tests, and the 53 that change (Evidence)
docs/             # FR-004
```

## Implementation Phases

### Phase 1: `KafkaSink` on the engine (FR-001)

- The round, the report table, `DEFINITIVE_CODES`, the counters as properties, and the stop signal,
  with the tests that change.

### Phase 2: `GooglePubSubSink` on the engine (FR-002)

- The round, the in-round wait, the definitive set, and the removal of the pending-list machinery,
  with the tests that change.

### Phase 3: The corpus and the record (FR-003, FR-004)

- The Pub/Sub double, the pins, the rosters and the record.

## Evidence

Every number was taken at `33582484` on macOS with Python 3.13.1 and no extra installed, unless it
says otherwise. The prototype is `docs/audits/2026-11-14-probes/sinks/spec094_prototype.patch`. It
applies with `patch -p1` after `spec093_prototype.patch` to `git archive 9d70f782 src tests
docs/audits/2026-11-14-probes`. SPEC-092's prototype stands in for its corpus, as in SPEC-093.

**The rows.** On top of SPEC-093's prototype, rebuilt on `9d70f782`, 268 rows read 195 green and 73
red. That is the same in three runs on Python 3.13.1 and one on 3.12.12. Exactly these four rows move:

- `KafkaSink-never-acknowledged` and `GooglePubSubSink-never-acknowledged`, from
  `partition`+`silent-loss` to green;
- `KafkaSink-report-indefinite` and `GooglePubSubSink-report-indefinite`, from
  `partition`+`lost-but-held` to green.

**An emit-only wait was tried first and fails the corpus.** Waiting for the reports inside a 1.x
`emit`, with no `deliver`, turned `KafkaSink-never-acknowledged` green and left
`KafkaSink-report-indefinite` at `lost-but-held`. An `emit`-only sink can report a batch only as
landed less what its `losses()` moved, so an event counted in `failed` that the broker kept is read as
lost and held. Only a verdict can say `UNKNOWN`, which is why both sinks move onto the engine.

**The mutant.** With `KafkaSink._round`'s `flush` replaced by `remaining = len(produced)`,
`KafkaSink-clean`, `-refused-once` and `-never-acknowledged` read `ambiguous-when-knowable`, and
`-refused-always` and `-report-indefinite` stay green: with no wait, every produced message is
`UNKNOWN`.

**The Pub/Sub double's classes matter.** With the double raising its local `PermissionDenied`, the
prototype read `GooglePubSubSink-never-acknowledged` as `ambiguous-when-knowable`. A refusal the sink
cannot recognise by class reads as indefinite. With the stub module's class, the row read green.

**The suite.** Against SPEC-093's prototype, 52 more tests fail at `33582484`. Rebased, 53 fail at
`bb6656ff`, which added `tests/test_class_claim.py`, and the same 53 at `9d70f782`. In that last run,
at a one-minute load near 100, two unrelated tests also failed once,
`test_otel_envelope.py::test_every_record_the_mapping_criteria_build_round_trips` and
`test_otel_mapping.py::test_a_shared_subtree_spends_the_node_budget_exactly_once`, and both passed when
rerun serially. They are not counted. The 53 are in these files:

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
| `test_engine_sinks_roster.py` | 1 |
| `test_class_claim.py`, from `bb6656ff` on | 1 |

With each sink's change applied alone on top of SPEC-093's at `33582484`, 25 of the 52 fail with
`KafkaSink`'s and 33 with `GooglePubSubSink`'s. 7 fail with either: the roster tests in `test_classify.py`,
`test_deferral.py::test_every_dropped_counter_is_written_only_by_a_deferral_commit` and
`test_engine_sinks_roster.py`. One fails only with both:
`test_deferral.py::test_every_run_context_count_of_a_dropped_counter_is_settled`, which expects a
stored `rejected` counter that neither sink then keeps. Each of the 53 asserts what FR-001 or FR-002
changes:

- **The pending list and its reap, overflow and close resolution.** These are 16 in
  `test_sinks_pubsub.py` and the two in `test_sink_flush_hook.py`.
- **A count made by a report or a future after the batch's verdict.**
  - In `test_sinks_refusal_deferral.py`: `test_the_kafka_delivery_callback_counts_failed_before_emit_returns`,
    `test_the_pubsub_reap_counts_failed_before_emit_returns` and
    `test_the_pubsub_orphaned_future_counts_failed_before_emit_returns`.
  - In `test_sinks_kafka.py`: `test_delivery_errors_are_counted`,
    `test_a_non_zero_remainder_is_counted_and_announced_once`, `test_an_overtaken_emit_counts_every_message_it_handed_over`
    and `test_a_produce_released_after_close_is_counted_and_announced`.
- **A whole-batch refusal that 1.x raised and the worker resent, which the engine now resends
  itself.** These are `test_sinks_refusal_deferral.py::test_a_batch_refused_on_every_attempt_counts_no_refusal`,
  `::test_a_refusal_followed_by_a_raise_counts_nothing` and `::test_a_fanout_absorbs_the_refused_batch`
  (two), the two in `test_sink_losses.py`, and the two in `test_final_refusal.py`. So is
  `test_class_claim.py::test_a_client_raise_whose_class_property_raises_is_settled_by_its_real_type[GooglePubSubSink]`,
  whose rig expects no line from a batch every publish of which was refused. The engine's round now
  announces that batch when its last round ends.
- **A roster derived from the tree that now finds two more engine sinks.** These are
  `test_engine_sinks_roster.py::test_the_engine_sinks_are_the_sixteen`, the two in
  `test_delivery_engine.py`, the five in `test_classify.py` (the retry-loop sweep and the wrapper
  exclusions), the three in `test_deferral.py`, `test_sink_post_close.py::test_every_hand_listed_adopter_population_is_the_derived_one`
  and `test_fork_roster.py::test_every_class_holding_a_primitive_has_a_home`, whose
  `_PRIMITIVE_HOMES` entry for `KafkaSink` names a lock the sink no longer holds.
- **1.x's docstring and line text.** These are the three in `test_retry_safe_drop_counts_prose.py`,
  `test_sinks_kafka.py::test_a_delivery_error_reports_the_code_not_the_message`, and the two in
  `test_stored_name_sinks.py`.

**The cost.** `spec094_report_wait_cost.py` used a fake client that acknowledges each message a set
time after it was handed over. It gives the median `emit` of seven, at a one-minute load of about 6:

| Sink | ack | batch 10 | batch 100 | batch 1,000 |
|---|---|---|---|---|
| `KafkaSink` at `33582484` | any | 0.01 to 0.02 ms | 0.11 to 0.12 ms | 1.2 to 1.3 ms |
| `KafkaSink`, waiting | 1 ms | 1.28 ms | 1.49 ms | 3.56 ms |
| `KafkaSink`, waiting | 5 ms | 5.24 ms | 5.50 ms | 7.95 ms |
| `KafkaSink`, waiting | 20 ms | 20.24 ms | 20.79 ms | 25.89 ms |
| `GooglePubSubSink` at `33582484` | any | 0.02 ms | 0.14 to 0.15 ms | 1.2 to 3.0 ms |
| `GooglePubSubSink`, waiting | 1 ms | 1.56 ms | 1.69 ms | 3.19 ms |
| `GooglePubSubSink`, waiting | 5 ms | 7.55 ms | 7.76 ms | 9.33 ms |
| `GooglePubSubSink`, waiting | 20 ms | 26.15 ms | 25.12 ms | 28.95 ms |

So one batch costs about one round trip. At today's batches of 10 events and a 5 ms round trip, the
drain thread delivers about 1,900 events a second to Kafka and 1,300 to Pub/Sub. At `33582484` it
delivered 661,000 to 690,000 and 559,000 to 656,000. At SPEC-090's cap of 1,000 events a batch it delivers about 126,000 and
107,000. SPEC-090's size target is `events / elapsed × flush_interval / 2`, which reaches the cap under
a backlog once a round trip is the cost. So a backlog lifts the batch size to where the wait matters
least. With a client that never acknowledges and the timeout at 0.5 s, one `emit` took 500.3 ms for
`KafkaSink` and 507.4 ms for `GooglePubSubSink`, against 0.02 ms at `33582484`.

**What the prototype could not settle.**

- That librdkafka reports each code in `DEFINITIVE_CODES` only for a message the broker did not write.
  The set is read from the Kafka protocol's error table, not measured.
- Whether `Producer.flush` sends at once rather than after `linger.ms`. The fake has no linger.
- How the real clients behave. That is owed to `integration.yml`'s Kafka and Pub/Sub tests on the
  PR's branch.

## Revision history

- Drafted on 2026-11-15, split from SPEC-093 at the delivery engine. This spec moves two sinks onto
  it, and SPEC-093 changes no sink's path through it.
- Revised the same day, after the fresh-context review of both specs. Decision B's gloss, that a
  truthy `flush()` means the broker acknowledged, is reconciled with the later SPEC-089 decision
  ("Keep truthy, add counts", Andrew, 2026-11-15): a truthy `flush()` means every event was
  acknowledged or counted. That is recorded as the earlier decision read in the light of the later
  one, and the decision-B file says so (FR-004). SPEC-093 is now in `depends_on`, since both land in
  one PR, and the Out of Scope sentence says why. The totals follow SPEC-093's revised corpus, in
  which `RabbitMQSink`'s confirms are opt-in.
