---
type: Spec
id: SPEC-074
title: "The Delivery Engine for the SDK and Driver Sinks, Including Id-Keyed Batch Results"
description: "Puts the queue-service, database, broker and socket sinks on SPEC-072's engine through a per-event deliver, reads Id-keyed batch results in one shared function, and deletes their eleven retry loops, backoff constants and hand-built loss reports while every documented counter and losses() stays true."
spec_status: In Progress
depends_on: [SPEC-061, SPEC-066, SPEC-068, SPEC-069, SPEC-070, SPEC-071, SPEC-072, SPEC-073]
arc: stability-redesign
arc_order: 6
---

## Overview

Eleven sinks each carry their own copy of one loop: try the vendor call, back off, try again, and
on the last attempt count the loss and decide whether to raise. When the loop gives up and raises,
the worker resends the **whole** batch and the loop runs again inside every resend, so events the
destination already took are sent again and the loss is counted once per worker attempt. SPEC-072
wrote the alternative once, as a per-event `Verdict` and an engine that resends only what a verdict
calls retryable; SPEC-073 put the HTTP family on it. This spec puts the SDK and driver sinks on it:
each gains a `deliver` that makes **one attempt per chunk per round** and says what happened to every
event, its `emit` runs SPEC-072's inline adapter, and its loop, its backoff constant and its
hand-built `SinkLosses` go. What a caller can read stays true: `losses()`, `.failed`,
`.dropped_oversized`, `.dropped_unadjudicated` and `read_losses(sink)` report the same terms for the
same outcomes, a refusal moves no sink counter, and a caller of `emit` sees a raise exactly when
nothing landed.

It also settles one parsing question the queue sinks answer by hand. `SQSSink` and `SNSSink` read
their vendor's `Failed` list, keyed by the entry `Id` the sink assigned, each in its own way, and a
`Failed` item that is not a mapping escapes `emit` raw. `MongoDBSink` reads a `BulkWriteError` by
counting `writeErrors`, never looking at the `index` each one names. `_batch.py` gains one keyed
reader beside `adjudicate_positional`, with the positional rule's discipline: a response either
describes the chunk or it does not, and one it cannot read is counted, never retried.

This spec builds on SPEC-061 (`NATSSink` liveness and a bounded close), SPEC-066 (every
`max_retries` is an exact, floored `int`) and SPEC-068 (shutdown expiry), which have landed, in 1.1.0,
and are in `depends_on`; `NATSSink` is converted
**as SPEC-061 leaves it**, and no SPEC-061 criterion is rewritten here. It builds on SPEC-073 (the
HTTP family on the engine), earlier in this arc, and uses its run context unchanged (FR-001). It
consumes SPEC-069's encode guard through a sibling of its helper (FR-002), SPEC-070's
`_retryable_error` and entry predicates, and SPEC-071's `chunk_items` in `SQSSink`.

**The budget is 1.x's.** SPEC-072 gives a `deliver` sink's run, on the worker path,
`(s + 1) x (w + 1) - 1` further rounds for a sink declaring `max_retries = s` under a worker with
`max_retries = w` (15 at the defaults), and `w` for a sink that declares none; on the orphan path and
through the inline adapter it gives `s`, or 0. That is the number of attempts a 1.x chunk could get
from the sink's loop inside the worker's, so a destination that refuses everything is called as
often as before.

**What a caller sees change**, listed once here and in the release notes:

- `.failed` counts each lost event once per batch, not once per worker attempt (`NATSSink`
  excepted: it counts as its 1.x `emit` did, once per raised publish per call, FR-004). The probe below
  made 12 calls and counted `failed=15` for 5 events; on the engine it makes the same 12 calls and
  counts `failed=5`. Same term, a tighter upper bound; nothing moves between terms.
- A chunk that keeps failing beside chunks that landed is retried within the whole worker-path
  budget. Under 1.x the batch returned once anything landed, so that chunk had the sink's
  `max_retries` alone.
- Only the events that failed are resent: under 1.x a raising batch was resent whole, chunks that
  had landed included.
- An exception the sink does not declare retryable (SPEC-070) costs one attempt in total, where the
  worker used to resend it. This includes an SQS FIFO batch whose every id callable raised: the caller's own exception is still re-raised from
  `emit` by identity and still counted by no sink counter, but the worker no longer re-runs the
  callables (FR-002).
- A `Failed` item, `writeErrors` entry or positional result the sink cannot read is counted in
  `.failed` and never resent, where it used to escape `emit` raw (SQS, SNS) or be counted by length
  alone (MongoDB).
- A message, frame or chunk that failed is resent in the next round, after the later ones of its
  round, where the 1.x loop retried it before sending the next. Only an SQS FIFO message group,
  whose order SQS documents as a guarantee, keeps 1.x order (FR-002).
- A converted sink's `emit` that fails raises the engine adapter's `SinkDeliveryError`, whose
  message is "delivered none of N event(s): …", where 1.x raised each sink's own text ("inserted
  none of …", "published none of …"), as SPEC-073 FR-002 changed the HTTP family's. A refusal —
  a closed sink, a NATS latch, a produced-no-chunk branch — still raises its 1.x error by identity.
- A `close()` that arrives while `ClickHouseSink`, `PostgresSink`, `RabbitMQSink`,
  `AzureEventHubsSink` or the socket transport waits between retries now returns at once, because
  the lock is no longer held across the wait; the rest of that batch is refused, counted where 1.x
  counted a closed sink's refusal.

**Figures**, each anchored to `fda134a` with its instrument:

- **Eleven loops in this spec's population.** `grep -rl 'for attempt in range' src/log_foundry/sinks`
  finds twelve modules; `http` is SPEC-073's, leaving `_socket`, `clickhouse`, `eventhubs`,
  `firehose`, `kinesis`, `mongodb`, `postgres`, `rabbitmq`, `redis`, `sns` and `sqs`. Each also
  assigns `_BACKOFF_BASE = 0.1` (`grep -rln '^_BACKOFF_BASE' src/log_foundry`, twelve with `http`).
- **Twelve hand-built loss reports in this population.** `grep -rl 'SinkLosses(' src/log_foundry/sinks`
  finds 23 files, 21 once `base.py` (docstring) and `_deferral.py` (`wrapped_losses`) are set aside.
  Twelve are this spec's: the eleven above and `nats`. The other nine stay: `http`, `elasticsearch`,
  `honeycomb`, `otlp` and `sentry` (SPEC-073), `logstash` (SPEC-075), and `kafka`, `pubsub` and
  `multi` (fixed decision). The plan counted 20 modules. SPEC-069 adds a `losses()` to three more
  modules outside this population (`stdout`, `file`, `sqlite`), which stay as SPEC-069 leaves them
  (FR-006).
- **The retry product.** A probe at `fda134a` (the shipped `RedisListSink`, `max_retries=3`, over a
  client whose `pipeline().execute()` raises `ConnectionError`, driven by `Worker(sink,
  max_retries=2)._emit` on a 5-event batch) made **12** client calls, left `sink.failed == 15` and
  `failed_batches == 1`, and wrote four `lost` lines, three reading `RedisListSink, 4 attempts,
  ConnectionError` and one `batch abandoned after 3 emit attempts`. `(3 + 1) x (2 + 1) = 12`.
- **One unit everywhere.** Every sink in the population counts `failed` in messages, records, rows,
  documents or events, and each is one per event (read from each `failed +=` site), so its `failed`
  maps onto SPEC-072's `failed_events` with no conversion.
- **Counters the population documents.** `README.md` names `.failed`, `.dropped_oversized` and
  `.dropped_unadjudicated` (`grep -n` over `README.md`); `_id_failed` (SQS) and SPEC-069's
  `_unencodable` are private.
- **Two sinks keep `max_retries` private.** `RabbitMQSink` and `SocketTransport` store `_max_retries`,
  and `SyslogSink` holds none (`grep -n max_retries` over the three modules), so the engine, which
  reads `max_retries` by name, would fall back to the caller's default for them.
- **What deleting `send_all` touches.** `grep -rn send_all tests` finds 32 references in ten files:
  `test_deferral.py`, `test_fork_lifecycle.py`, `test_retry_safe_drop_counts_prose.py`,
  `test_sink_concurrency.py`, `test_sink_flush_hook.py`, `test_sink_losses.py`,
  `test_sink_release_roster.py`, `test_sink_retry.py`, `test_socket_oversize_deferral.py` and
  `test_socket_transport.py`.
- **Patch points that move.** Tests that patch a converted module's `wait` or `_BACKOFF_BASE`
  (`grep -rn` for both names under `tests/`): `test_sink_concurrency.py:1188` (`kinesis.wait`),
  five in `test_sink_losses.py` (`_socket._BACKOFF_BASE`), `test_sink_retry.py:475` and `:544`
  (`redis`, `sqs`), `test_socket_oversize_deferral.py:67`, two in `test_socket_transport.py`, three in
  `test_sinks_syslog.py`, and one each in `test_sinks_{clickhouse,eventhubs,firehose,kinesis,mongodb,
  postgres,rabbitmq,redis,sns,sqs,sqs_fifo,sqs_id_failures}.py`.

## Scope

### In Scope

- How a converted sink counts and announces through SPEC-073's run context, and four private
  additions to the engine: one commit under one lock, final rejections, the refusal marker, and
  sink-counted rounds (FR-001).
- `_batch.py`'s keyed reader, and `SQSSink`, `SNSSink`, `KinesisSink` and `FirehoseSink` on the
  engine (FR-002).
- `ClickHouseSink`, `MongoDBSink` (on the keyed reader) and `PostgresSink` on the engine (FR-003).
- `RedisStreamsSink`, `RedisListSink`, `RabbitMQSink`, `AzureEventHubsSink` and `NATSSink` on the
  engine (FR-004).
- `SocketTransport` gaining `deliver`, `SyslogSink` forwarding to it, and socket-mode
  `LogstashSink`'s `emit` running the adapter over it (FR-005).
- What holds the population to the rule: the derived roster of engine sinks, the deletions, the
  counters, the idempotency declarations, Kafka and Pub/Sub unchanged, and every roster whose
  population this spec moves (FR-006).
- Prose and records: each converted sink's docstrings, `docs/invariants.md` entries 2 and 3,
  `docs/architecture.md` §8, the README's sink table and partial-delivery text,
  `docs/component-inventory.md`, and the decision files under *Recorded text the build changes*.
- A deliberate narrowing of the plan's "every shipped sink's `emit` runs the engine": `StdoutSink`,
  `StderrSink`, `FileSink`, `RotatingFileSink` and `SQLiteSink` stay off it (no `deliver`), because
  none has a retry loop, the engine has nothing to resend for them, and SPEC-069's driver error,
  both `SinkDeliveryError` and the driver's own class, must reach the caller as raised.
- Six FRs, inside the 3–6 aim. The split is by shared mechanism, as the arc plan asks: one engine
  extension, then queue services, databases, brokers and the socket transport.

### Out of Scope

- **The HTTP family** (`HTTPSink` and every subclass, and `HTTPSink`'s `_BACKOFF_BASE`): SPEC-073.
- **`SentrySink`** is SPEC-073's: it gets no `deliver`, and its HTTP backend's `HTTPSink._send` runs
  on the engine there. Nothing in `sentry.py` changes here.
- **Wrappers.** `FilteringSink`, `TransformSink` and `LogstashSink`'s `deliver` are SPEC-075's.
  SPEC-075's `LogstashSink.deliver` delegates to whichever backend offers one, this spec's
  `SocketTransport` included; this spec gives `LogstashSink` no `deliver` of its own.
- **`KafkaSink`, `GooglePubSubSink` and `MultiSink`** get no `deliver` and keep `losses()` (fixed
  decision). Nothing in `kafka.py`, `pubsub.py` or `multi.py` changes; FR-006 pins that.
- **The local sinks** (`StdoutSink`, `StderrSink`, `FileSink`, `RotatingFileSink`, `SQLiteSink`):
  they keep `emit`, for the reason under *In Scope*.
- **A predicate for `NATSSink`.** SPEC-070 declared none; a publish that raises is `REJECTED` beside
  a publish that landed and `RETRYABLE` when nothing in its round landed, as 1.x (FR-004), and naming
  NATS's transient exceptions is a later change.
- **Declaring any shipped sink idempotent.** FR-006 records why none qualifies.
- **The closed-guard helper** is SPEC-076's. This spec keeps each sink's own closed flag and lock,
  and a closed sink's `deliver` keeps today's observable through the refusal marker: the
  `SinkDeliveryError` its `emit` raises today, and no sink counter moved.
- **Lifecycle, owed closes and the fork walk** (SPEC-077 to SPEC-079). SPEC-072 makes the walk reach
  `log_foundry_counters`; this spec adds no global and no lock outside a sink, and makes no new
  object a structural `Sink` (FR-005).
- **Any new `Outcome` member, a public name beyond the sink attributes FR-006 lists, or a change to
  `emit`'s contract, `SinkLosses`, `configure()` or `Config`.**
- **Retry-after hints.** None of these vendors returns one through its SDK; every verdict here
  carries `retry_after=None`.
- **Encoding and the vendor classifications themselves** (SPEC-069, SPEC-070). This spec calls them
  from `deliver` and changes neither.

### Twin paths (invariant 6)

- **Worker and orphan paths.** Both reach a converted sink through `run_delivery` (SPEC-072); each FR
  drives one total-failure and one partial case through both and compares the ledgers.
- **`deliver` and `emit`** of every converted sink: `emit` runs the adapter, so one roster (FR-006)
  holds every converted class to an `emit` whose body is the adapter call, on the class that
  defines `deliver`.
- **SQS and SNS** share the keyed reader and `sender_fault`; **Kinesis and Firehose** share
  `adjudicate_positional`; **the two Redis sinks** share `_RedisSink.deliver`; **Syslog and
  socket-mode Logstash** share `SocketTransport.deliver`. Each criterion naming one names the other.
- **`emit` and `flush`.** No converted sink's `flush` retries or decides a drop, so none changes;
  `NATSSink.flush` stays SPEC-061's.

---

## Functional Requirements

### FR-001: The engine carries what a shipped sink needs to keep its 1.x counts and lines

#### Description:

SPEC-072's public `Verdict` names an outcome per event and nothing else. A shipped sink also
documents sub-counts beside its two terms (`.dropped_oversized`, `.dropped_unadjudicated`), names a
vendor code, a byte limit or an attempt count on stderr, returns normally for an all-sender-fault
SQS batch, and refuses without counting. SPEC-073 adds the **run context** to `sinks/_engine.py`:
`run_context()` gives a round its run's `round` number, `notes(owner)`, a per-run `dict` kept across
rounds, and `count(owner, name, n, when=...)` and `announce(line, when=...)`, committed when the run
ends as `ALWAYS`, `SETTLED` or `LAST` says; a run in which the sink announced anything is announced
by the sink's lines alone. This spec uses it unchanged and adds four private things beside it.

**How a converted sink counts.** `losses()` returns `self.log_foundry_counters.losses()`, so the
twelve hand-built `SinkLosses` go and the two terms are SPEC-072's event totals, which for this
population are the 1.x terms (every `failed` here counts one per event, Overview). The documented
sub-counts stay plain attributes written only through the run context: `dropped_oversized` with
`when=SETTLED`, `dropped_unadjudicated` with `when=ALWAYS`. `.failed` is a property over
`failed_events`, less `dropped_unadjudicated` for Kinesis and Firehose, whose 1.x `losses().failed`
summed the two; it **stays assignable**, as a 1.x plain attribute was: the setter writes
`failed_events` (adding `dropped_unadjudicated` back for Kinesis and Firehose) under the lock. A
converted sink's lines follow one rule: a drop is announced `SETTLED`, a `REJECTED` or `UNKNOWN`
event `ALWAYS`, and the `RETRYABLE` events of a round in `LAST` lines that name the type and an
attempt count that is **the number of calls that carried those events**, not the round number: the
sink records each event's sends in `notes(self)`, and events with different counts get separate
lines. That is SPEC-070's `N attempt(s), <Type>` reading, true per event (invariant 11, the defect
SPEC-049 FR-006 fixed once).

**One lock, one commit (addition 1).** A converted sink's `_counter_lock` is a property over its
counters' lock: reading it returns `log_foundry_counters._counter_lock`, and assigning it replaces
that lock, so a test that installs a counting lock on `sink._counter_lock` counts every
acquisition. When a run-context owner's `_counter_lock` is the sink's `SinkCounters` lock, the
engine commits that owner's counts and the `SinkCounters` totals under **one** acquisition, so a
reader of `.failed`, `.dropped_oversized` or `losses()` never sees half a commit, and a run makes one
acquisition, as a 1.x `emit` did.

**Final rejections (addition 2).** `RunContext.final_rejections(indices)` marks indices of the
round's verdict whose `REJECTED` is one the same request is refused again; they do **not** count
toward the run failing. Only `SQSSink` and `SNSSink` call it, for a sender fault, which keeps 1.x: a
batch lost only to sender faults returns (invariants 2 and 7 list the sender fault as counted and
never retried). It is read when the round returns: an index that is not `REJECTED` in that round's
verdict raises `ValueError` there, a sink bug that SPEC-072's escaped-exception rule fails, so
SPEC-072's `outcomes_of` stays the one definition of a malformed verdict.

**The refusal marker (addition 3).** `RunContext.refuse(error)` is how a shipped `deliver` that
handed no event of the round to its transport says the round was a refusal. `NATSSink`'s
wedged-loop backstop is the one use after events may have reached the transport: the sink has
closed itself, so the round is refused like a closed sink's (FR-004's adjudication). It returns an ordinary
verdict, `REJECTED` for a refusal a resend cannot change and `RETRYABLE` for one a later round can
pass, and calls `refuse` with the exception a 1.x `emit` raised for it. Nothing escapes `deliver`,
so SPEC-072's rule for an escaped exception is untouched. The engine reads the marker at the end of
the run:

- If the run **fails**, an event whose final outcome came from a refused round commits nothing to
  `SinkCounters` or the run context: the catcher's term (`failed_batches`, `orphan_lost`) is its only
  count, and the inline adapter raises the marker's `error` **by identity**. That keeps the settled
  texts: "Refusing moves no `losses()` counter" (*A sink that released its transport refuses*),
  "re-raised unchanged and nothing is counted" (*Caller code that fails for one event costs only
  that event*), and the README's `sink.failed` as attempted delivery.
- If the run **settles** (an earlier round landed something), the refused round's events are counted
  `failed_events` with one line, because no catcher sees a settled run, and invariant 2 forbids an
  uncounted loss.

The shipped refusals are a closed or closing sink (`REJECTED`, the 1.x closed `SinkDeliveryError`),
`NATSSink`'s forked child (`REJECTED`) and its disconnected client, both latches and unanswered probe
(`RETRYABLE`, SPEC-061's `SinkDeliveryError`), and `SQSSink`'s batch whose every FIFO id callable
raised (`REJECTED`, the caller's own first exception). That last is the arc's one rule for caller code
that failed on every event of a batch, which SPEC-075's wrappers follow too: the events are
`REJECTED` with `refuse(first_exc)`, so the batch costs one attempt and is never retried (an
exception from the caller's code is not one the sink's predicate calls transient, SPEC-070), no sink
counter moves, the catcher's `failed_batches` or `orphan_lost` counts it as any failed run, and the
`emit` path re-raises the caller's own first exception by identity.

**Sink-counted rounds (addition 4).** `RunContext.sink_counted(indices)` marks indices of the
round's verdict, each `RETRYABLE`, whose loss for this round the sink has already registered
through `count` and `announce`, `ALWAYS`, as its 1.x `emit` counted and wrote it. The engine commits
nothing to `failed_events` for an event whose final outcome came from such a round, and writes no
line for it; the sink's registrations and the catcher's term are its only counts. It is read when
the round returns: an index that is not `RETRYABLE` in that round's verdict raises `ValueError`
there, as for `final_rejections`. Only `NATSSink` calls it (FR-004), because its 1.x `failed` moves
once per raised publish per `emit` call, which a count of each event once per run cannot reproduce.

#### Acceptance Criteria:

- [ ] A converted test sink whose round counts `dropped_oversized` `SETTLED` for one of three events
      reads `.dropped_oversized == 1` and `losses().dropped == 1` after a settled run and 0 and 0
      after a failed one; `dropped_unadjudicated` `ALWAYS` beside two `UNKNOWN` events reads 2, and
      for a Kinesis-shaped sink `.failed` excludes them while `losses().failed` includes them.
- [ ] `sink.failed = 0` after a loss makes `.failed` 0 and, for Kinesis and Firehose,
      `losses().failed` equal to `.dropped_unadjudicated`, as 1.x read it.
- [ ] A `CountingLock` assigned to `sink._counter_lock` is the counters' lock afterwards, a run that
      counts makes exactly one acquisition, and `losses()` makes one; a reader thread polling across
      1,000 commits never sees `losses().dropped` and `.dropped_oversized` from different commits (a
      planted second acquisition between the two writes reddens it).
- [ ] A chunk sent in rounds 1, 2 and 3 and an event first sent in round 2 (an SQS FIFO hold), both
      `RETRYABLE` to the end of a three-round run, are announced as `3 attempt(s)` and `2 attempt(s)`
      respectively; a later round's `ACCEPTED` writes no line for those events.
- [ ] `[REJECTED, REJECTED]` marked `final_rejections({0, 1})` settles with `failed_events == 2`;
      marking `{0}` alone fails the run; marking an index whose outcome is `ACCEPTED` fails the run
      as an escaped `ValueError` would, and `outcomes_of` is unchanged.
- [ ] A round returning `Verdict.uniform(Outcome.REJECTED, n)` with `refuse(err)`: one `deliver` call,
      `losses()` unchanged, the worker adds one to `failed_batches`, an orphan `info()` adds one to
      `orphan_lost`, and the adapter raises `err` by identity.
- [ ] A round returning all `RETRYABLE` with `refuse(err)` on every round is resent within the
      budget, then fails with `losses()` unchanged and `err` raised by the adapter; the same refusal
      on round one and `ACCEPTED` on round two delivers once with nothing counted; a refused round
      after a round that landed some events settles, counts the refused events `failed_events` once
      and writes one line.
- [ ] A round returning all `RETRYABLE` for three events, marked `sink_counted({0, 1, 2})` and
      registering `count(sink.log_foundry_counters, "failed_events", 1, when=ALWAYS)` with one line
      (never `count(sink, "failed", …)`: the commit holds the counters' lock, and the `failed`
      setter takes it, so that count would deadlock), three rounds under
      `Worker(max_retries=2)`: `.failed == 3`, three lines then the worker's abandon line,
      `failed_batches == 1`; the same round followed by an `ACCEPTED` round leaves `.failed == 1`
      and one line; marking an index whose outcome is `REJECTED` fails the run as an escaped
      `ValueError` would.
- [ ] A `deliver` raising `KeyboardInterrupt` propagates by identity and commits nothing.
- [ ] Serves invariants 2, 7 and 11: each event lands in exactly one exact term or the documented
      upper bound, a refusal is counted by its catcher and never also by the sink, a sub-count and
      its term are committed together, a run lost only to final rejections reports rather than
      raises, and every line's attempt count is the calls that carried its events.

### FR-002: The queue services deliver per entry, and Id-keyed results are read in one place

#### Description:

**The keyed reader.** `_batch.py` gains `adjudicate_keyed(keys, failed, *, key)`, beside
`adjudicate_positional`. `keys` are the identifiers the sink assigned, in order. It returns, per
sent position, the failed entry that names it or `None`, or `unadjudicated == len(keys)` when the
response cannot be read: `failed` is not a list, an item is not a mapping, an item's `key` value is
not one of `keys` **of the same exact type** (so `True` does not name `1`), or two items name the
same key. That is the positional decision's rule, *a response adjudicates all of a chunk or none of
it*, applied to a keyed response. A missing `Failed` is the empty list, as today, so a well-formed
reply reads exactly as 1.x read it. `SQSSink` and `SNSSink` call it with `key="Id"`; `MongoDBSink`
calls it with `key="index"` (FR-003).

**The encode step in engine mode.** SPEC-069's `encode_events(batch, encode, pending, *, sink,
counter="_unencodable")` counts on a `Deferral` into `_unencodable`, which these sinks no longer have
(FR-006) and which a run spanning several `deliver` calls cannot use. So `_encode.py` gains a
**sibling**, `encode_round(owner, batch, encode, *, sink)`, and `encode_events` keeps its signature
and its callers (the local sinks, Kafka and Pub/Sub). Both run each event through one private guard,
so the guard (`Exception` caught, never `BaseException`) is still written once. `encode_round`
returns the round's positions and wire forms of the encodable events, in order; every position it
omits is unencodable, and `deliver` makes it `DROPPED`. Its count is that outcome: SPEC-072's engine
adds `DROPPED` events to `dropped_events` only when the run settles, which is the `Deferral`'s
"counted only by a call that returned", and no `run_context().count` is made for it. Its line is
SPEC-069's, unchanged in text, registered as `run_context().announce(<the line>, when=SETTLED)`.

**Each event is encoded once per run.** `encode_round` keeps every wire form, and every failure, in
`run_context().notes(owner)`, keyed by event identity, as MongoDB keeps its documents (FR-003): a
later round looks the event up instead of calling `encode` again. That keeps SPEC-069 FR-001's
criterion that Postgres, the Redis sinks and ClickHouse call `encode` `len(batch)` times however many
attempts are made, which a per-round encode would break, and it registers the line once per run,
on the round that first meets a failure, which is the run's first round, since every later round's
events are a subset of it. A round outside any run (SPEC-073: its own one-round run) encodes once.

**The common round** of the four queue sinks, which FR-003 to FR-005 also follow, `NATSSink` excepted
from step 1:

1. Build the batch's wire forms through `encode_round` and the sink's own drop decisions:
   an unencodable event is `DROPPED` with SPEC-069's line, an oversized one is `DROPPED` with
   `dropped_oversized` counted `SETTLED` and its 1.x line, and an SQS event whose FIFO id callable
   raised is `DROPPED` with its line beside deliverable siblings. When every SQS event's id callable
   raised, the round is a refusal (FR-001): all `REJECTED`, with `refuse` given the caller's first
   exception, so the batch costs one attempt, no sink counter moves, and the adapter re-raises that
   exception by identity. A FIFO batch that **mixes** the two failures, with no deliverable event
   (one id failure and one unencodable event, say), is neither case and is SPEC-069 FR-001's: its
   events are all `DROPPED`, the run settles with `dropped_events == 2`, nothing is resent, and there
   is no `refuse`, because not every event failed in caller code. `NATSSink` does not take this step.
   SPEC-069 leaves it off the encode step, counting an unencodable event in `failed` as a publish
   that raised, and FR-004 keeps that: the event takes FR-004's outcome for a publish that raised,
   with SPEC-061's publish line, so `.failed` moves and `losses().dropped` does not.
2. Chunk with the sink's existing chunker (`chunk_items`, kept from SPEC-071) and send **every
   remaining chunk once per round**, in order, through `_send`, now one attempt with no loop, which
   keeps SPEC-048's guard inside it. A chunk's failure does not hold back the chunks after it, so
   one entry that keeps failing costs only itself, as in 1.x.
3. A call that raises runs the sink's cleanup and asks `is_retryable(self._retryable_error, err)`
   (SPEC-070), never the predicate directly, so a predicate that raises answers `False`: `True` makes
   the chunk `RETRYABLE`; `False` makes it `REJECTED` with an `ALWAYS` line in the arc's one form for
   an exception, SPEC-070's `<Class>, N attempt(s), <Type>`, which SPEC-073 also writes, with `N` the
   calls that carried the chunk (FR-001): `1 attempt(s)` for an exception refused on its first send.
   On the engine this supersedes SPEC-070 FR-001's worker criterion: through `Worker(max_retries=2)`
   a Redis client raising `TypeError` is called **once**, not three times, and `.failed` moves once
   for the batch, because a `REJECTED` event is never resent and the worker no longer resends a whole
   batch (SPEC-072). SPEC-070 FR-001's line criterion (one line naming `TypeError` and `1 attempt`)
   still holds, in this one form.
4. A response is adjudicated per entry. SQS and SNS: the keyed reader, then SPEC-070's
   `sender_fault`: a sender fault is `REJECTED`, marked a final rejection, with the 1.x line naming
   the first `Code`; any other failed entry is `RETRYABLE`; the rest `ACCEPTED`. Kinesis and Firehose:
   `adjudicate_positional` as today, a failed record `RETRYABLE` (SPEC-070's entry predicate), the
   rest `ACCEPTED`. An unreadable response makes the chunk `UNKNOWN` with a line; Kinesis and
   Firehose also count `dropped_unadjudicated` `ALWAYS`, so `.dropped_unadjudicated` keeps its
   meaning.
5. **SQS FIFO keeps message-group order.** SQS documents ordering within a message group as a FIFO
   guarantee, and 1.x kept it for as long as its loop retried a chunk, `max_retries + 1` sends, before
   moving on. The hold is decided **per chunk at send time**, from this run's replies so far: an event
   of group *g* is held (`RETRYABLE`, not sent) only when an earlier event of *g* was sent in an
   earlier round, or in an earlier chunk of this round, and has neither landed nor used up its
   `max_retries + 1` sends (this round's send of it counted), or when that earlier event is itself
   held. Same-group events not yet sent travel together in one chunk, as 1.x sent them, so a healthy
   one-group batch is one call. A held event never sent when the run ends is counted `failed` with
   the line `SQSSink, 0 attempt(s), held behind an earlier message of its FIFO group` (reading (a),
   Andrew's call pending; the decision is one function, `sqs._hold`, so reading (b) — send held
   events in the run's last round, after the predecessor's final send — is a contained change). The
   send counts are kept in `notes(self)`. No other sink here promises an order across chunks
   (Kinesis `PutRecords` promises none across records), so none holds anything.

**SQS's chunker changes signature.** SPEC-071 FR-004 kept `_chunks(batch, pending)`, handing its
drops to the caller's `Deferral`, which this spec removes from `SQSSink`. It becomes
`_chunks(batch) -> _Plan`, a `NamedTuple` of `chunks: list[list[_Prepared]]` (still built with
`chunk_items`, still a list, every drop decided before the first send), `dropped: dict[int, str]`
(each dropped position of the round's batch and its reason: oversize, id failure or unencodable) and
`refused: Exception | None` (the first id callable's exception, set only when every event's id
callable raised). `_Prepared` gains `position`, the event's position (named so rather than `index`,
which would shadow `tuple.index`), so each chunk's outcomes map back to the verdict. `_chunks` counts and announces nothing; `deliver` turns `dropped` into `DROPPED`
outcomes, writing the oversize and id-failure lines (`encode_round` registered the unencodable
one), and `refused` into the refusal. SPEC-071's boundary corpus,
`tests/test_sinks_sqs_chunks.py`, is kept and re-pointed at the new signature with the same cases
and the same literal chunk lengths; it is not rewritten from the new code.

**An SQS entry is prepared once per run, ids included.** In 1.x the sink's loop resent the same
prepared entries, so a retry inside one `emit` kept its `MessageDeduplicationId` and SQS's 5-minute
de-duplication interval could absorb a duplicate. Resolving the ids again each round would lose that,
and `_dedup_id`'s random fallback would mint a new id per round. So `_chunks` keeps each event's
prepared entry (body, `MessageGroupId`, `MessageDeduplicationId` and costed size) or its drop reason
in `run_context().notes(self)`, keyed by event identity beside the wire forms `encode_round` keeps:
the id callables and the random fallback run **once per event per run**, and every round of one run
resends the identical entry. A round's `_Plan` is assembled from that cache: its chunks are re-formed
over the events the round still holds, and each `_Prepared.position` is the event's position in that
round's batch, so only the grouping and the position are per round. An event identity that appears
twice in a batch keeps one entry per occurrence, and a round hands an occurrence the first cached
entry of its identity not yet accepted, so a resend never reuses an id of an occurrence that landed;
each entry's batch `Id` is kept across the run's rounds too, as 1.x's loop kept it, and replaced only
when it would clash within one call.

The run's raise-or-return then is the engine's, and it is 1.x's: SQS raised exactly when nothing
landed and a retryable loss remained; Kinesis and Firehose when nothing landed and nothing was
unadjudicated; SNS (with SPEC-070) as SQS.

#### Acceptance Criteria:

- [ ] `adjudicate_keyed(["0", "1", "2"], [{"Id": "1", "Code": "X"}], key="Id")` pairs the entry
      with position 1 only; a missing `Failed` (passed as `[]`) pairs none; `failed` as a `dict`, an
      item that is a `str`, an item naming `"7"`, two items naming `"1"`, an item with no `Id`, and
      `adjudicate_keyed([0, 1], [{"index": True}], key="index")` each give `unadjudicated` equal to
      the keys sent and pair none. It never raises on any input.
- [ ] One bad entry: 100 SQS events in ten chunks, one entry `SenderFault: False` on every attempt,
      `max_retries=3`: 99 land, each once, `.failed == 1`, and the bad entry is sent 4 times (the
      reviewer's `probe_sqs_one_bad_entry.py` shape, where 1.x also lands 99).
- [ ] SQS and SNS: a chunk of three whose `Failed` names one entry with `SenderFault: True` and one
      with `False` makes one more call holding only the second entry; the sender fault is counted
      `failed` once, never resent, announced once with its `Code` and `not retried`.
- [ ] SQS and SNS: a batch lost only to sender faults returns from `emit`, `flush()` is truthy and
      `failed_batches == 0`; a sender-fault chunk beside a chunk that stays retryable past the budget
      raises from `emit` and fails the worker's batch (1.x twins, unchanged).
- [ ] SQS and SNS: each malformed `Failed` of the first criterion makes the chunk counted `failed`
      once with one line, never resent, and `emit` returns; under 1.x the `str` item escaped
      `emit` as `AttributeError`.
- [ ] SQS FIFO: group *A*'s event in chunk 1 retryable once, and *A*'s and *B*'s events in chunk 2:
      chunk 2's *A* event is first sent after chunk 1's *A* event lands, *B*'s is sent in round one,
      and no *A* event reaches the client before an earlier *A* event; a standard queue sends chunk 2
      whole in round one. With *A*'s chunk-1 event failing on every attempt and `max_retries=3`, the
      later *A* events are first sent in the round of its fourth send and land, as 1.x sent them
      after abandoning it. A one-group FIFO batch on a healthy client is one call.
- [ ] Kinesis and Firehose: a response whose `Records` length disagrees is counted
      `.dropped_unadjudicated` and in `losses().failed`, `.failed` excludes it, it is never resent,
      and the 1.x line text is unchanged.
- [ ] For each of the four, a client raising a retryable exception on every call over a three-chunk
      batch with `max_retries=3`: through `emit`, 12 calls (4 per chunk); through
      `Worker(max_retries=2)`, 36 calls (12 per chunk), `failed_batches == 1`, `.failed` equal to the
      batch size, and the sink's lines read `4 attempt(s)` and `12 attempt(s)` respectively.
- [ ] For each of the four, a chunk-one exception that `_retryable_error` refuses is one call,
      counted, with chunks two and three sent in the same round, and its line reads
      `1 attempt(s), <Type>`.
- [ ] Encoded once per run: for `PostgresSink`, both Redis sinks and `ClickHouseSink` (SPEC-069
      FR-001's four) and for each of the four queue sinks, over a client that fails retryably twice
      and then accepts, with one unencodable event in the batch, `encode` is called `len(batch)` times
      in all, SPEC-069's line for the unencodable event is written once, and the run settles with
      `losses().dropped == 1`; a mutant that drops the `notes` cache reddens the call count.
- [ ] SQS FIFO, a batch of one event whose id callable raises and one unencodable event and nothing
      else: no client call, `deliver` called once, the run settles, `losses().dropped == 2`,
      `.failed` unchanged, `emit` returns, and through `Worker(max_retries=2)` `failed_batches == 0`.
- [ ] SQS FIFO ids once per run: over a client that fails one entry retryably twice and then
      accepts it, with counting `group_id` and `dedup_id` callables (the latter returning a blank
      string for one event, so the random fallback runs; a `None` is an id failure), each callable is called once per event in the
      run, and every resend of the failed entry carries the same `MessageDeduplicationId` and
      `MessageGroupId` as its first send. A mutant that resolves ids per round reddens both the call
      count and the id equality.
- [ ] `tests/test_sinks_sqs_chunks.py` (SPEC-071 FR-004) is kept and re-pointed at the new
      `_chunks(batch) -> _Plan`: every case keeps its batch and its literal chunk lengths; a drop it
      used to read from the `Deferral` is read as that position in `_Plan.dropped`; the
      all-id-failed case asserts `_Plan.refused` is the first exception by identity instead of a
      raise. No case is deleted, which a diff of its collected test names on both refs shows.
- [ ] Oversized and unencodable events are counted once, on a settled run only, `.dropped_oversized`
      and `losses().dropped` both moving for an oversized one and only `losses().dropped` for an
      unencodable one; a failed run moves neither.
- [ ] SQS FIFO with every id callable raising `KeyError`, `max_retries=3`: no client call,
      `losses()` and `.failed` unchanged, and the callables called once per event through `emit`,
      which re-raises that `KeyError` by identity; through `Worker(max_retries=2)`, one `deliver`
      call and `failed_batches == 1`; as an orphan `info()`, one call and `orphan_lost == 1`.
- [ ] Worker and orphan paths: a one-event sender fault records `orphan_lost == 0` and `.failed == 1`
      on both; a one-event batch the client refuses retryably every time records `orphan_lost == 1`
      (orphan) and `failed_batches == 1` (worker) with the same `.failed`.
- [ ] `tests/test_sinks_{sqs,sns,kinesis,firehose,sqs_fifo,sqs_id_failures}.py` and their rows in
      `tests/test_sink_losses.py` pass, editing only the named patch points (Overview) and assertions
      on `failed` totals that multiplied by worker attempts, each such edit named in the PR.
- [ ] Serves invariants 2, 3, 6 and 7: every entry is accepted or in one term once and a bad entry
      costs only itself, no wait is taken but the engine's, the worker and orphan twins and SQS/SNS
      and Kinesis/Firehose keep one ledger each, and the raise-or-report rule is 1.x's.

### FR-003: The database sinks make one attempt per chunk per round

#### Description:

`ClickHouseSink`, `MongoDBSink` and `PostgresSink` follow FR-002's common round with their own unit
of a call.

- **ClickHouse** encodes every row through `encode_round` (FR-002), chunks with `chunk_list`, and
  inserts every remaining
  chunk once per round. 1.x raised when no chunk inserted; the engine's rule is the same.
- **MongoDB** drops oversized and unencodable documents as FR-002 does and makes one `insert_many(...,
  ordered=False)` per round. The documents are **built once per run** and kept across its rounds in
  `notes(self)`, keyed by event identity, as 1.x built them once per `emit` (`mongodb.py:357`), so a
  resend carries the `_id` pymongo stamped into each document in place on the first attempt. A
  `BulkWriteError` whose `details["writeErrors"]` the keyed reader can pair by `index` makes each named
  document `REJECTED` (with the 1.x line) and the rest `ACCEPTED`, never resent; one it cannot pair
  makes the call's documents `UNKNOWN`, counted once with a line. Any other exception goes to
  `_retryable_error`. `writeErrors` keeps its 1.x precedence over the predicate.
- **PostgreSQL** runs every chunk of the round in one transaction, as today, so the round's events are
  all `ACCEPTED` on commit or all `RETRYABLE` or `REJECTED` after `_rollback()`, with the borrowed-
  broken-connection suffix carried in its line exactly as 1.x wrote it.

**Locks.** `ClickHouseSink` and `PostgresSink` hold their transport lock, `self._lock`, for **one
round**, inside `deliver`, and no longer across a backoff, which now runs in the engine between
rounds (SPEC-072 holds no lock across `deliver`). A `close()` arriving during a backoff therefore
takes the lock between rounds, where under 1.x it waited out the retries, and the next round is a
refusal (FR-001). `MongoDBSink` holds no lock across I/O, in 1.x or here: its `_close_lock` guards
the close alone.

#### Acceptance Criteria:

- [ ] ClickHouse: three chunks with a retryable failure on chunk two's first insert are sent in the
      order 1, 2, 3, 2, each landing once; a client that always fails retryably is called `max_retries + 1` times per
      chunk through `emit` and `.failed` equals the rows; a non-retryable `DataError` on chunk one is
      one call with chunks two and three inserted.
- [ ] MongoDB: a `BulkWriteError` naming indices 0 and 2 of four documents counts `.failed == 2`,
      returns, and is never resent; naming all four raises from `emit`; naming index 9, the same index
      twice, or index `True`, counts all four `failed` once, returns, and writes one line.
- [ ] MongoDB: an `AutoReconnect` on round one and success on round two sends the same document
      objects, carrying the same `_id` values, in both calls.
- [ ] PostgreSQL: a retryable `OperationalError` on every attempt calls `_rollback()` once per round,
      commits nothing, and counts the batch once; SPEC-070's `StringDataRightTruncation` case is one
      attempt; a borrowed broken connection's line keeps its 1.x suffix.
- [ ] ClickHouse and PostgreSQL: `close()` issued from another thread during a backoff returns before
      the next round would start, and that round makes no client call.
- [ ] For all three, `emit` after `close()` raises the `SinkDeliveryError` it raises at `fda134a`,
      with the same message, before any client call, and `losses()` is unchanged;
      `tests/test_sinks_oversize_deferral.py::test_a_closed_sink_raises_and_counts_nothing` passes
      unedited.
- [ ] Worker and orphan paths: the total-failure and partial cases of each sink give the same
      `losses()` on both paths.
- [ ] `tests/test_sinks_{clickhouse,mongodb,postgres}.py`, `tests/integration/test_{clickhouse,mongo,postgres}.py`
      and the `tests/test_sink_losses.py` rows pass, editing only the named patch points and
      multiplied-total assertions, each named in the PR; the integration cases settle on the
      Integration job's run against the compose services, recorded as owed until then.
- [ ] Serves invariants 2, 3, 6 and 7: a document or row is counted once whatever the response's
      shape and a resent document is the same document, the lock is never held across a wait, both
      paths keep one ledger, and a closed sink still refuses without counting.

### FR-004: The broker sinks, `NATSSink` among them, report per message

#### Description:

- **Redis** (`_RedisSink`, so both `RedisStreamsSink` and `RedisListSink`): one pipeline per round
  over the encodable events (`encode_round`, FR-002), all `ACCEPTED` on `execute()`, or all
  `RETRYABLE` or
  `REJECTED` by `_retryable_error`. `deliver` and `emit` are defined on `_RedisSink`, and neither
  subclass defines `emit`, so SPEC-072's override rule offers `deliver` on both. A closed sink's round
  is a refusal (FR-001).
- **RabbitMQ**: one publish per remaining message per round, in order, under `self._lock` for the
  round. A publish that raises runs `_reset()`; a retryable one makes that message `RETRYABLE`, a
  non-retryable one `REJECTED`, and the round goes on to the next message.
- **Azure Event Hubs**: the round packs the events still to send into `EventDataBatch`es as today and
  sends every pack once, under `self._lock` for the round; a retryable `send_batch` failure makes
  that pack's events `RETRYABLE`; an event too large for an empty batch is `DROPPED` with
  `dropped_oversized` counted `SETTLED` and its 1.x line.
- **NATS, as SPEC-061 leaves it.** `deliver` runs SPEC-061's coroutine for one round, under
  `self._lock` and its bounds, and maps what it learns. Each refusal SPEC-061 makes before the first
  publish is a refusal round (FR-001) carrying SPEC-061's own `SinkDeliveryError`: `RETRYABLE` for a
  disconnected client, the probe's latch, an unanswered pre-publish probe and the abandoned-publish
  latch (`_refuse_while_stuck`, read for every client, an injected one included, before the lock and
  again under it), each of which a later round can pass, so the worker retries a batch refused by
  any of them exactly as it retries the raise in 1.x; `REJECTED` for a forked child, a closed or a
  closing sink. A publish that raises, and an event the deadline left unattempted, take the
  outcome the rule below gives (NATS declares no predicate). A core batch whose confirmation went
  unanswered makes its published events `UNKNOWN` with SPEC-061's `unconfirmed` line, never resent.
  A publish abandoned at the deadline on JetStream or an injected client, which SPEC-061 FR-001
  counts as unconfirmed where it is abandoned, is `UNKNOWN` with SPEC-061's `unconfirmed, ...
  abandoned` line, never resent, since `NATSSink` declares no idempotence (SPEC-072 FR-004); on the
  sink's own core client the confirming round trip judges it, as the row before says.
  The wedged-loop backstop is a `REJECTED` refusal round carrying SPEC-061's `SinkDeliveryError`
  (`refuse(err)`): the sink has just closed itself, so no later round can pass, the adapter raises
  that error by identity, and nothing is counted on the sink, as 1.x's raise counted nothing.
  **Adjudicated at build** (the reviewer contract's method for two requirements that cannot both
  hold): this spec first made the backstop an escaped exception, which the engine answers with the
  adapter's own `SinkDeliveryError`, while this FR's criterion holds `tests/test_sinks_nats.py`
  unedited. Both readings were built and the NATS suites and the full suite run on each. The
  escaped-exception reading fails two shipped tests,
  `tests/test_sinks_nats.py::test_the_unbounded_implementation_fails_that_bound`
  (`match="wedged"`) and
  `tests/test_sinks_nats_liveness.py::test_the_backstop_closes_the_sink_when_a_publish_never_returns`;
  the refusal reading fails neither, so it stands. It is the one use of the refusal marker after
  events may have reached the transport (FR-001's marker is defined for a round that handed none). JetStream and an injected client keep
  SPEC-061's behaviour, and a JetStream publish that raised takes the rule below.

  After the first publish, the round's outcome follows the one fact 1.x's `emit` acts on: whether
  anything in the round was published. An abandoned publish counts as published, as
  `_emit_on_loop` counts it.
  - **Nothing published.** 1.x's `emit` counts each publish that raised (`failed` +1 and SPEC-061's
    `NATSSink publish, <Type>` line, as it happens), counts and writes nothing for an event the
    deadline left unattempted, and raises, and the worker resends the whole batch. So every event
    of the round is `RETRYABLE` and marked `sink_counted` (addition 4), and the round registers,
    `ALWAYS`, exactly those 1.x counts and lines in 1.x's order: one per publish that raised, an ack
    wait that expired in the driver included, and none for an unattempted event, whose loss, if the
    run fails, is the catcher's alone (the worker's abandon or declined line, or `orphan_lost`). An
    event `json.dumps` cannot encode is a publish that raised, since it stays inside the publish
    path, off FR-002's step 1.
  - **Something published.** 1.x's `emit` returns, counting the rest in `failed`, and the worker
    resends nothing, so a publish that raised (an unencodable event included) and an event the
    deadline left unattempted are `REJECTED`, announced `ALWAYS` with the same two lines.

**NATS's budget.** `NATSSink` takes no `max_retries` and gains none, so SPEC-072 gives it the
worker's `max_retries` further rounds on the drain thread and none on the orphan path or through
`emit` (the adapter uses the sink's own `max_retries`, or 0). So SPEC-061's `emit` against a stalled
server still takes one `publish_timeout` and raises with `losses()` unchanged, and the worker still
retries a refused batch, or one in which nothing was published, as its 1.x loop did: `w + 1`
`deliver` calls for 1.x's `w + 1` `emit` calls, each carrying the whole batch and each counting
what that `emit` counted, then `failed_batches` once with 1.x's `batch abandoned after {w + 1} emit
attempts` line (SPEC-072 FR-005).

**Why a batch with nothing published is still resent:** the 1.2.0 release is additive, so `emit`'s
contract and every documented counter stay true for 1.x callers and no count is reclassified, and
1.x resends that batch (`docs/invariants.md` 2's JetStream asymmetry, which therefore stays true as
written and is not rewritten).

#### Acceptance Criteria:

- [ ] Redis, both sinks, `max_retries=3`: an always-retryable `execute()` is 4 calls through `emit`
      and 12 through `Worker(max_retries=2)` (the Overview's probe), `.failed` equals the batch size
      once, and the line names the type; a `TypeError` is one call through `emit` and one through
      `Worker(max_retries=2)` (SPEC-070 FR-001's three, superseded on the engine), `.failed` moves
      once, and the one line reads `1 attempt(s), TypeError`.
- [ ] RabbitMQ: with message two's first publish retryable and its second accepted, the broker sees
      1, 2, 3, 2, `_reset()` runs once, and nothing is counted; a non-retryable failure on message two
      leaves 1 and 3 published and `.failed == 1`.
- [ ] Event Hubs: an oversized event beside two packs counts `.dropped_oversized == 1` on a settled
      run; a retryable failure of pack one leaves pack two sent in the same round and pack one sent
      in the next.
- [ ] RabbitMQ and Event Hubs: `close()` from another thread during a backoff returns before the next
      round would start, and that round publishes nothing.
- [ ] Redis: `tests/test_sinks_redis.py::test_a_closed_sink_refuses_a_batch_without_moving_a_counter`
      passes unedited.
- [ ] NATS: `tests/test_sinks_nats.py` and `tests/integration/test_nats.py` pass unedited, SPEC-061's
      refusal criteria (nothing counted on a refusal) among them; through `Worker(max_retries=2)` an
      unanswered probe is three `deliver` calls with no publish, a PONG on the second delivers the
      batch once, and refused throughout gives `failed_batches == 1` with `losses()` unchanged.
- [ ] NATS: one publish raising beside two unconfirmed is `.failed == 3` and `emit` returns; every
      publish raising, a JetStream ack-wait expiry among them, is `.failed == n` and `emit` raises;
      SPEC-061's blocked-publish case still returns with `.failed` equal to the batch size.
- [ ] NATS differential matrix: `tests/test_sinks_nats_engine_matrix.py` runs each case below on the
      engine, through `emit` and through `Worker(max_retries=2)`, and asserts per case and path
      `losses().failed`, `failed_batches` as `health()` reports it, the number of `deliver` and
      `emit` calls, the number of publishes the client saw, and the exact `log-foundry:` stderr
      lines in order. The expected values are **recorded, not hand-written**: a recorder script
      runs the same harness against 1.x at `93113e1b` (`emit` where the engine has `deliver`) and
      writes a fixture committed beside the test, naming the commit and the command that
      regenerates it; the test compares against that fixture. Cases, each on JetStream and core and
      on an injected and an owned client wherever the case exists on that path: every publish
      raising; every ack wait expiring; every event unencodable; a publish raising after the
      deadline with the rest unattempted (`n = 5`, injected, `publish_timeout=0.2`: on `93113e1b`,
      three `emit` calls, `failed == 3`, three `NATSSink publish, RuntimeError` lines then `lost 5
      ... batch abandoned after 3 emit attempts`, and no `publish_timeout` line); a landed publish
      beside a raise; a landed publish beside an unencodable event; a landed publish beside `n - 1`
      unattempted; an abandoned publish; and `_refuse_while_stuck`. Planting one case's verdict as
      the other branch's (`RETRYABLE` for `REJECTED`, or dropping `sink_counted`) reddens it.
- [ ] NATS keeps SPEC-069's ledger: one unencodable event among several is `.failed == 1` and
      `losses().dropped == 0`, and `emit` returns; a batch of nothing but unencodable events raises
      `SinkDeliveryError` with `.failed` equal to the batch size. SPEC-069 FR-001's and FR-002's NATS
      criteria pass unedited.
- [ ] `tests/test_sinks_{redis,rabbitmq,eventhubs}.py`, `tests/integration/test_{redis,rabbitmq}.py`
      and the `tests/test_sink_losses.py` rows pass, editing only the named patch points and
      multiplied-total assertions, each named in the PR; the integration cases settle on the
      Integration job's run, recorded as owed until then.
- [ ] Worker and orphan paths: each sink's total-failure and partial cases give the same `losses()`
      on both paths, and the two Redis sinks give the same ledger as each other.
- [ ] Serves invariants 2, 3, 6 and 7: a refusal is counted by its catcher alone and an unconfirmed
      publish once by the sink, no lock is held across a wait, both paths and both Redis sinks keep
      one ledger, and a broker that takes nothing still makes `emit` raise.

### FR-005: The socket transport gains `deliver`, and the two socket sinks share it

#### Description:

`SocketTransport` takes frames and a caller's `Deferral` today, and retries each frame on its own
budget holding its lock across the backoff. It becomes the one engine object both socket users run,
because it owns the ledger both already read (`SyslogSink.losses()` and socket-mode
`LogstashSink.losses()` return `self._socket.losses()`). It does **not** become a structural `Sink`:
it gains no `emit`, so `isinstance(transport, Sink)` stays false, `_reachable_sinks` and the fork
and ownership walks still see only the sink that holds it, and
`tests/test_sink_release_roster.py::test_a_transport_that_is_not_a_sink_is_out_of_scope` keeps
passing on its derived check (its docstring's mention of `send_all` is updated).

- **Construction** takes the caller's frame function, `frame: Callable[[dict[str, object]], bytes]`
  (optional, an event's UTF-8 JSON by default, so a transport built directly still works), and the
  owning sink's name, `sink_name`, for SPEC-069's unchanged encode line; the transport gains `log_foundry_counters` (an instance attribute), `deliver`, a read-only
  `max_retries` and a `_counter_lock` property as FR-001 describes. `send_all`, its `pending`
  parameter, `_send_one`'s loop and `_BACKOFF_BASE` are deleted.
- **A round** frames every event through `encode_round` (FR-002; `DROPPED` with SPEC-069's line),
  drops a UDP frame over `max_datagram_bytes` as `DROPPED` with `dropped_oversized` counted
  `SETTLED` and the
  1.x line, then sends every remaining frame once, in order, under `self._lock` for the round. An
  exception runs `_reset()` (SPEC-070); one `_retryable_error` accepts (outside `_PERMANENT_ERRNOS`)
  makes that frame `RETRYABLE`, its `LAST` line naming the type and errno as the 1.x line did; one it
  refuses (`EMSGSIZE`, or a non-`OSError`) is `REJECTED`; the round goes on to the next frame. A
  closed transport's round is a refusal (FR-001) with the `SinkDeliveryError` `send_all` raises today.
- **`SyslogSink`** passes `self._frame`, defines a `deliver` that returns the transport's `deliver` of
  the same batch (one index space, nothing to map) and an `emit` that runs the adapter, sets
  `self.log_foundry_counters` to the transport's counters object as an instance attribute (SPEC-072
  reads it statically, so a property would not be offered), and exposes `max_retries`. Its
  `log_foundry_stop_signal` becomes a plain instance attribute rather than a property forwarding
  to the transport, for the same reason: the engine reads the stop signal of the sink it runs
  statically, and a property reads as no signal.
- **`LogstashSink` in socket mode** passes its line framer, and its `emit` runs the adapter over the
  transport, `emit_via_deliver(self._socket, batch)`, so its 1.x `emit` contract is the engine's. What
  `LogstashSink` offers the worker is SPEC-075's, whose `deliver` delegates to `self._socket`.

#### Acceptance Criteria:

- [ ] `SyslogSink` over a peer refusing every connect with `max_retries=3` raises from `emit` after 4
      rounds, counts `.failed == 5` for a 5-event batch once, and its line reads `4 attempt(s)`; with
      the third frame's first send failing transiently, the peer receives frames 1, 2, 4, 5, 3, each
      once.
- [ ] `OSError(errno.EMSGSIZE)` on frame two is one send, counted, and frames 1 and 3 are delivered;
      `ValueError` from the socket is one send, counted, and does not stop the round.
- [ ] A UDP frame over `max_datagram_bytes` counts `.dropped_oversized == 1` and `losses().dropped == 1`
      on a settled run and neither on a failed one; `dropped_oversized` still means oversize only.
- [ ] `syslog.log_foundry_counters is syslog._socket.log_foundry_counters`, `read_deliver(syslog)` is
      the bound method, `syslog.losses() == syslog._socket.losses()` after every case above, and
      `isinstance(syslog._socket, Sink)` is false.
- [ ] Socket-mode `LogstashSink`'s `emit` against the peers above returns or raises as `SyslogSink`'s
      `emit` does and leaves the same `losses()`; `tests/test_sinks_logstash.py` and
      `tests/integration/test_logstash.py` pass, editing only patch points.
- [ ] A `close()` from another thread during a backoff returns before the next round, the next round
      sends nothing and raises today's closed `SinkDeliveryError` from `emit`, and `losses()` is
      unchanged by it; the transport lock is free during every engine wait.
- [ ] Worker and orphan paths give the same `losses()` for `SyslogSink`.
- [ ] Serves invariants 2, 3, 6 and 7: each frame is delivered or counted once on the one ledger both
      sinks read, no lock is held across a wait, Syslog and socket-mode Logstash run one transport,
      and a peer that takes nothing still makes `emit` raise.

### FR-006: The population is held to the rule, and every roster this spec moves is re-derived

#### Description:

**The engine sinks**, derived from the tree: every class under `src/log_foundry/sinks/` that defines
`deliver`, other than a named exclusion. The exclusions are named in the roster, each with its
reason: `HTTPSink`'s family (SPEC-073); SPEC-075 adds `FilteringSink`, `TransformSink` and
`LogstashSink`, and the loop roster below excludes the same three. At this spec's build they are
`SQSSink`, `SNSSink`, `KinesisSink`, `FirehoseSink`, `ClickHouseSink`, `MongoDBSink`, `PostgresSink`,
`_RedisSink`, `RabbitMQSink`, `AzureEventHubsSink`, `NATSSink`, `SocketTransport` and `SyslogSink`:
thirteen. Each holds `log_foundry_counters` as an instance attribute holding a `SinkCounters`,
returns `self.log_foundry_counters.losses()` from `losses()`, makes no call to `sinks._retry.wait`,
and writes a counter only through the run context or the `.failed` setter, and none registers a count against `failed`, whose setter
takes the counters' lock the commit already holds; a failed-event count is registered against
`log_foundry_counters` as `failed_events`. Each but
`SocketTransport` defines **`def emit(self, batch)` whose body is the one call
`emit_via_deliver(self, batch)`** on the class that defines `deliver`, a function rather than an
assignment so that every AST roster keyed on a `def emit` keeps finding it. `_id_failed` and, on these
sinks, SPEC-069's `_unencodable` are deleted, folded into `dropped_events`. `RabbitMQSink`,
`SocketTransport` and `SyslogSink` expose `max_retries` read-only, through `_args.ReadOnlyCount`: a
data descriptor with a refusing `__set__` and no `__get__`, so both ordinary access and
`base._static_value` read the instance's own value while an assignment is refused (a `property`
reads as the property object to the static reader, so the engine would give the sink one round). Eleven of the thirteen define
`_retryable_error` (SPEC-070); `NATSSink` defines none, and `SyslogSink` forwards.

**The deletions.** The eleven `for attempt in range` loops, the eleven `_BACKOFF_BASE` constants,
`SocketTransport.send_all`, and the twelve hand-built `SinkLosses` of the Overview go, so after this
spec (and SPEC-073) no module under `sinks/` but `_engine.py` assigns a backoff constant or calls
`wait`, and no converted module builds a `SinkLosses`. Counted on SPEC-069's ledger rather than
`fda134a`'s: the Overview's grep, run on the build's base commit and again after the build, loses
exactly the twelve converted modules. What it still finds is the nine the Overview names, plus
`stdout`, `file` and `sqlite`, whose `losses()` SPEC-069 adds and this spec does not touch, for any
of the three that builds its report with a literal `SinkLosses(` rather than `wrapped_losses`.

**Idempotency: no shipped sink declares itself idempotent**, so an `UNKNOWN` outcome or an escaped
exception is never resent (SPEC-072). The evidence per destination: Kinesis and Firehose tell the
caller to handle duplicates at the destination (Firehose `PutRecordBatch`, *"We recommend that you
handle any duplicates at the destination"*,
`https://docs.aws.amazon.com/firehose/latest/APIReference/API_PutRecordBatch.html`); an SNS standard
topic and an SQS standard queue de-duplicate nothing (SNS documents de-duplication for FIFO topics
only, `https://docs.aws.amazon.com/sns/latest/api/API_PublishBatch.html`, and `SNSSink` sets no
de-duplication id); ClickHouse de-duplicates a repeated insert block only under settings the table's
owner chooses (`insert_deduplicate` on a replicated table, `non_replicated_deduplication_window`
otherwise), which the sink cannot see; `PostgresSink`'s `INSERT` has no conflict clause; Redis `XADD`
with an automatic id and `RPUSH`, RabbitMQ, Event Hubs, NATS core and the socket peers deliver every
copy they are sent. **MongoDB** keeps one run's documents and their pymongo-stamped `_id`s across its
rounds (FR-003), so within a run a resend of a landed document is refused as a duplicate key rather
than stored twice; it is still not declared, because that refusal comes back as a `writeErrors`
entry, `REJECTED` and counted although the document landed, so resending an unknown outcome would
turn it into a miscounted failure, not a delivery. The one other candidate is an **SQS FIFO queue**,
which accepts but does not deliver a message whose `MessageDeduplicationId` it has seen within a
5-minute interval (the documentation string of `SendMessageBatchRequestEntry.MessageDeduplicationId`
in botocore 1.43.123's SQS service model). Within one run the resend is identical (FR-002 prepares
each entry once per run, ids included), but it is still not declared: `log_foundry_idempotent` is a
class attribute, and the same class writes to standard queues, which de-duplicate nothing; and on a
FIFO queue SQS absorbs a duplicate only within that 5-minute interval, which the sink neither
measures nor bounds a run to. JetStream de-duplication needs a `Nats-Msg-Id` header that `NATSSink` does not send
(SPEC-061, Out of Scope).

**Kafka and Pub/Sub.** Nothing changes for them: `kafka.py` and `pubsub.py` are not edited, neither
defines `deliver` nor `log_foundry_counters`, and each keeps `rejected` and `losses()`.

**Rosters whose population moves**, each re-derived rather than lowered:

- `tests/test_sink_retry.py`'s `_modules_with_a_retry_loop` (pinned by SPEC-073 to the base set less
  `http`, eleven names) finds
  none after SPEC-073 and this spec. Its pin and its two tests are replaced by a derived check over
  `sinks/*.py` that no module but `_engine.py` and `_retry.py` calls `wait` or assigns a backoff
  constant, with a fixture corpus that plants each, and the stop-signal test is restated over the
  engine sinks. The tests that set `redis_mod._BACKOFF_BASE` and `sqs_mod._BACKOFF_BASE` patch the
  engine's.
- SPEC-070's loop roster (floor 11 as SPEC-073 leaves it) is restated over the engine sinks: each
  class under `sinks/` that defines `deliver`, other than a named exclusion, defines
  `_retryable_error` and calls it in its round's exception handler as
  `is_retryable(self._retryable_error, <the caught exception>)`; a direct `self._retryable_error(err)`
  fails it, as in SPEC-070 FR-001. The exclusions are named in the roster, each with its reason:
  `HTTPSink` (its classification is SPEC-073's, held by that spec's engine-only check), `NATSSink`
  (no predicate) and `SyslogSink` (forwards to its transport); SPEC-075 adds `FilteringSink`,
  `TransformSink` and `LogstashSink` to the same list. The population is the eleven engine sinks other
  than `NATSSink` and `SyslogSink`; floor 11.
- `tests/test_sink_concurrency.py`'s `_takes_the_transport_lock` and its three callers (the lock
  roster, the locked-sink close test, and the decision-doc check) read `emit` and `send_all` only; once
  the I/O moves into `deliver` an `emit` that only calls the adapter enters no lock, and the roster
  would fall back silently to the docstring claim. They are re-aimed to read `deliver` and the
  same-class helpers it calls, and their population to include every class defining `deliver`, so
  `ClickHouseSink`, `PostgresSink`, `RabbitMQSink`, `AzureEventHubsSink`, `NATSSink` and
  `SocketTransport` are still found taking `self._lock`, with a floor equal to that set.
- `tests/test_deferral.py`: the converted sites leave `DEFERRING_SITES` and `IMMEDIATE`, with the
  stale-entry check confirming each leaves. `DROPPED_FLOOR` loses `_id_failed`, which no sink reports
  any more, named in the floor's docstring with the guard that replaced it; `_unencodable` stays
  while the local sinks report it (SPEC-069), and `dropped_oversized`, still derived through
  `HTTPSink.losses()` (SPEC-073), keeps the store check reaching every sink that writes it, so a
  converted sink's run-context count is the only writer the check allows.
- `tests/test_retry_safe_drop_counts_prose.py`'s `_LISTED` docstring pin names
  `SocketTransport.send_all`, `_sendable` and the converted sinks' drop functions; each converted
  entry is re-pointed at the function that now decides the drop, whose docstring says it is counted
  when the run settles.
- `tests/test_sink_losses.py`: `_sends_with_a_client_call` still finds the four AWS `_send`s (floor
  4). The ten `send_all` files of the Overview are rewritten to `deliver` or `emit`, each named in the
  PR.
- `tests/test_diag.py`'s roster covers every new line site; `tests/test_public_surface.py` pins the new
  sink attributes.

#### Acceptance Criteria:

- [ ] A derived roster finds exactly the thirteen engine sinks and fails, naming the class, on a
      fixture that defines `deliver` without `log_foundry_counters`, has an `emit` whose body is not the
      one adapter call, defines `emit` below the class defining `deliver`, or writes
      `dropped_oversized` outside the run context; its floor is 13, and a fixture tree of 12 fails on
      the floor.
- [ ] SPEC-070's restated loop roster finds exactly the eleven classes, floor 11, and fails, naming
      the class, on a fixture whose round's handler calls `self._retryable_error(err)` directly
      rather than through `is_retryable`; removing one named exclusion reddens it naming that class.
- [ ] `grep -rln '^_BACKOFF_BASE\|for attempt in range\|def send_all' src/log_foundry/sinks` finds
      nothing, and `BACKOFF_BASE` is assigned once in `src/log_foundry`, in `_engine.py`.
- [ ] Over the thirteen, `losses()` reads `log_foundry_counters`, and `.failed`, `.dropped_oversized`
      and `.dropped_unadjudicated`, set through the engine, equal the 1.x formula; `.failed` is
      assignable.
- [ ] A subclass of each converted sink that overrides `emit` is delivered to through its `emit` by
      the worker (SPEC-072's override rule), and its events are counted once.
- [ ] No class under `sinks/` defines `log_foundry_idempotent`; `kafka.py`, `pubsub.py` and `multi.py`
      are byte-identical to the commit the build branches from, and their tests pass unedited.
- [ ] A forked child of each engine sink, forked while a parent thread holds its counters' lock,
      completes one `emit` within 5 s.
- [ ] `tests/test_sink_concurrency.py`'s CountingLock tests on `SQSSink` and `KinesisSink`
      (`_counter_lock` replaced, acquisitions counted) pass unedited, and its transport-lock roster,
      re-aimed at `deliver`, finds the six locked classes; a mutant that drops `self._lock` from one
      `deliver` reddens it.
- [ ] Each roster named above is re-derived as stated, and each replacement check is mutation-tested
      once (the check removed or the population emptied) and reddens on its own reason;
      `tests/test_worker_predicate_roster.py`'s floors are unchanged.
- [ ] `tests/test_invariants_model.py` passes unedited with its `deliver` recorder, and the full
      suite, `tests/test_sink_losses.py` and `tests/test_deferral.py` included, is green.
- [ ] Serves invariants 2, 6, 7, 10 and 12: every engine sink counts through one commit and reports
      what it reported, the population cannot grow a twin that skips the rule, `emit` still raises when
      nothing landed, the public surface only grows, and a forked child never hangs on a sink's
      counters' lock.

---

## Data Model

```
# src/log_foundry/sinks/_engine.py  -- private additions beside SPEC-073's RunContext
class RunContext:                          # SPEC-073's, plus:
    def final_rejections(self, indices: Iterable[int]) -> None: ...
    # REJECTED indices of this round's verdict that do not count toward the run failing;
    # an index that is not REJECTED raises ValueError when the round returns
    def refuse(self, error: Exception) -> None: ...
    # the refusal marker: this round handed nothing to the transport. On a failed run the
    # round's final outcomes commit nothing and the adapter raises `error` by identity; on a
    # settled run they are counted failed_events with one line
    def sink_counted(self, indices: Iterable[int]) -> None: ...
    # RETRYABLE indices whose loss this round the sink registered ALWAYS itself, as 1.x's emit
    # counted it; the engine commits no failed_events and no line for them (NATSSink only)

# the commit: an owner whose _counter_lock is the sink's SinkCounters lock is committed with the
# SinkCounters totals under one acquisition

# src/log_foundry/sinks/_batch.py
class KeyedAdjudication(NamedTuple):
    failed: list[Mapping[str, Any] | None]   # per sent position: the entry naming it, or None
    unadjudicated: int                       # len(keys) when the response cannot be read, else 0

def adjudicate_keyed(keys: Sequence[object], failed: object, *, key: str) -> KeyedAdjudication: ...
    # a key matches only a sent key of the same exact type

# src/log_foundry/sinks/_encode.py  -- SPEC-069's module; encode_events unchanged beside it
def encode_round(owner: object, batch: list[dict[str, object]],
                 encode: Callable[[dict[str, object]], T], *, sink: str) -> list[tuple[int, T]]: ...
    # (position, wire form) of each encodable event, in order; an omitted position is DROPPED.
    # Forms and failures cached per run in run_context().notes(owner), keyed by event identity;
    # SPEC-069's line registered once per run with announce(..., when=SETTLED)

# src/log_foundry/sinks/sqs.py
class _Prepared(NamedTuple): position: int; body: str; group_id: str | None; dedup_id: str | None; size: int
class _Plan(NamedTuple):
    chunks: list[list[_Prepared]]
    dropped: dict[int, str]                  # position -> oversize | id failure | unencodable
    refused: Exception | None                # the first id exception, when every id callable raised
SQSSink._chunks(self, batch: list[dict[str, object]]) -> _Plan   # was (batch, pending) -> list
    # each event's entry (body, ids, size) or drop reason cached per run in notes(self); ids once per run
```

## API / Interface Contract

What each converted sink gains (public, additive):

```python
class SQSSink:                                   # and the other engine sinks
    log_foundry_counters: SinkCounters           # instance attribute
    def deliver(self, batch: list[dict[str, object]]) -> Verdict: ...   # one round
    def emit(self, batch: list[dict[str, object]]) -> None:
        emit_via_deliver(self, batch)            # 1.x contract, SPEC-072's adapter
    def losses(self) -> SinkLosses: ...          # self.log_foundry_counters.losses()
    failed: int                                  # property over failed_events; assignable
    dropped_oversized: int                       # plain attribute, written only by the run context

class RabbitMQSink:                              # also SocketTransport, SyslogSink
    max_retries = ReadOnlyCount()                # the floored constructor value, read-only:
    # a data descriptor defining only a refusing __set__, the value in the instance __dict__;
    # a property would read as the property object to the engine's static reader, one round
```

## File & Folder Structure

```
src/log_foundry/sinks/
├── _engine.py      # + RunContext.final_rejections, RunContext.refuse, RunContext.sink_counted, the one-lock commit
├── _batch.py       # + adjudicate_keyed
├── _encode.py      # + encode_round, beside SPEC-069's encode_events
├── sqs.py sns.py kinesis.py firehose.py           # FR-002
├── clickhouse.py mongodb.py postgres.py           # FR-003
├── redis.py rabbitmq.py eventhubs.py nats.py      # FR-004
└── _socket.py syslog.py logstash.py               # FR-005 (logstash: socket mode's emit only)

tests/
├── test_engine_shipped_verdicts.py   # FR-001
├── test_batch_keyed.py               # FR-002's reader
├── test_engine_sinks_roster.py       # FR-006
└── the files named in the Overview's figures and FR-006's rosters   # edited as named

docs/decisions/sink-delivery/
├── a-sink-round-resends-only-what-failed-and-an-sqs-fifo-group-keeps-its-order.md   # new
└── no-shipped-sink-declares-itself-idempotent.md                                     # new
```

## Recorded text the build changes

Each rewritten in place with a dated History entry at completion, with a superseded marker at any
other site still stating the old claim:

- `docs/decisions/sink-delivery/a-dropped-count-is-written-only-by-a-deferral-commit-and-the-gate-derives-which-counters-are-counted.md`:
  for an engine sink the engine's commit is the writer, and the sentence that the socket transport's
  ledger is "entered in the calling sink's `emit` and passed to `send_all`" is superseded.
- `docs/decisions/sink-delivery/a-positional-response-adjudicates-all-of-a-chunk-or-none-of-it.md`:
  extended to a keyed response, with `dropped_unadjudicated` counted through the run context.
- `docs/decisions/sink-delivery/a-client-exception-costs-its-chunk-and-is-provable-non-delivery-for-it.md`:
  its fence's "feeds SQS's recoverable term" now reads as a `RETRYABLE` chunk.
- `docs/decisions/sink-delivery/a-sink-that-released-its-transport-refuses-one-that-released-nothing-keeps-accepting.md`:
  a dated History line saying the engine keeps it through the refusal marker; its fence is
  unchanged.
- `docs/decisions/sink-delivery/caller-code-that-fails-for-one-event-costs-only-that-event-a-batch-of-nothing-but-failures-re-raises.md`:
  a dated History line records the rule for an `SQSSink` FIFO batch only: when every event's id
  callable raised, the events are `REJECTED` with the refusal marker, so the batch costs one attempt
  and is **no longer retried by the worker**, no sink counter moves, and `emit` re-raises the first
  exception by identity. The wrappers' half is not recorded here, because `FilteringSink.deliver` and
  `TransformSink.deliver` do not exist until SPEC-075, which adds its own History line when it
  builds them. The file is edited as SPEC-069 leaves it (SPEC-069 generalises its HTTP half), and its
  fence's "re-raised unchanged and nothing is counted" stays as written.
- `docs/invariants.md` 2 (an engine sink's drop is counted by a run that settled) and 3 (guarded by
  the engine's tests). Entry 7 already names the SNS sender fault beside the SQS one (SPEC-070), and
  this spec does not add it again; `README.md`'s
  sink rows that name `max_retries` per chunk or per message.

## Implementation Phases

### Phase 1: The engine additions and the keyed reader

- `RunContext.final_rejections`, `RunContext.refuse`, `RunContext.sink_counted`, the `_counter_lock` property and the one-lock
  commit in `_engine.py`, on SPEC-073's run context, against test sinks; `adjudicate_keyed` in
  `_batch.py`.
- `tests/test_engine_shipped_verdicts.py`, `tests/test_batch_keyed.py`.

### Phase 2: The queue services

- `SQSSink`, `SNSSink`, `KinesisSink`, `FirehoseSink`: `deliver`, the round, the FIFO hold, the
  counters; their tests, the `tests/test_sink_losses.py` rows and the `DEFERRING_SITES` entries in the
  same commit.

### Phase 3: The databases

- `ClickHouseSink`, `MongoDBSink`, `PostgresSink`, and their unit and integration tests.

### Phase 4: The brokers

- `_RedisSink`, `RabbitMQSink`, `AzureEventHubsSink`, then `NATSSink` on SPEC-061's code, and their tests.

### Phase 5: The socket transport

- `SocketTransport`, `SyslogSink`, socket-mode `LogstashSink`'s `emit`, the `send_all` test rewrites,
  and their tests.

### Phase 6: The rosters, prose and records

- FR-006's roster and the re-derived rosters, each mutation-tested once; the docstrings, invariants,
  architecture §8, README, inventory, the decision edits and the two new decisions.

## Revision history

- Drafted against `fda134a` from the arc plan's Change 2 items 5–8 and Change 3's rows "Retry loop",
  "Backoff constant", "Loss report" and "Per-entry batch results". The plan's "~20" loss reports
  re-measured as twelve in this population of 21.
- Rebased FR-001 onto SPEC-073's run context once that spec reached the directory: an earlier draft
  carried its own private verdict subclass for sub-counts, reason lines and a cause.
- Orchestrator's decisions: `SentrySink` is SPEC-073's; the local sinks' exclusion is stated under
  *In Scope* as a narrowing of the plan; a private `Refused` exception that carved SPEC-072's FR-004 was
  removed; the NATS all-raised case is accepted and squared with SPEC-061.
- Spec review, first round. Andrew's decision keeps 1.x's total budget (SPEC-072's `(s+1) x (w+1) - 1`
  worker-path rounds), so the probe's 12 calls stay 12. A round no longer stops at its first retryable
  chunk: measured by the reviewer, 100 SQS events with one entry failing every attempt landed 99 under
  1.x and 36 under that rule; only an SQS FIFO group holds. Refusals move no sink counter, through the
  new refusal marker, so the settled refusal texts and SPEC-061's criteria hold unedited. MongoDB keeps
  one run's documents and `_id`s across rounds; its lock claim was dropped. `SocketTransport` gains no
  `emit`, so it stays outside the structural-`Sink` walks. Each line's attempt count is the calls that
  carried its events. `.failed` stays assignable. `emit` is a one-call function so the AST rosters keyed
  on `def emit` keep finding the sinks, and `_takes_the_transport_lock` is re-aimed at `deliver`. The
  keyed reader matches the exact key type. FR-006 cites invariant 12.
- Cross-arc review: an SQS FIFO batch whose every id callable raised is `REJECTED` with the refusal
  marker, one attempt and never retried, the one rule SPEC-075's wrappers now share, and this spec
  owns that decision's History line; the restated loop roster calls through `is_retryable`, names its
  exclusions (SPEC-075's wrappers among them) and is floored at 11 as SPEC-073 leaves it;
  `_modules_with_a_retry_loop`'s pin is SPEC-073's; the PostgreSQL criterion cites
  `StringDataRightTruncation`; invariant 7's SNS sender fault is left to SPEC-070.
- Build-order re-read: the encode step on the engine is a named sibling, `encode_round`, whose count
  is the `DROPPED` outcome and whose line is a `SETTLED` announcement, and it caches each event's
  wire form per run so SPEC-069's encode-once criterion holds; `NATSSink` is exempt from step 1 and
  keeps its unencodable count on `failed`; the mixed id-failure and unencodable FIFO batch settles
  with `dropped == 2`; SQS's `_chunks` becomes `_chunks(batch) -> _Plan` with SPEC-071's corpus
  re-pointed; the caller-code decision's History line records the SQS half only; a refused
  exception's line is the arc's one form, `N attempt(s), <Type>`, superseding SPEC-070 FR-001's
  three-call worker criterion on the engine; the `SinkLosses` builders left are recounted on
  SPEC-069's ledger.
- Orchestrator's decision after the re-read: an SQS entry, its FIFO ids and `_dedup_id`'s random
  fallback included, is prepared once per run and cached in `notes(self)`, so every round resends
  the identical `MessageDeduplicationId` and `MessageGroupId`, as 1.x's loop did inside one `emit`;
  a criterion kills the per-round mutant, and FR-006's idempotency reasoning no longer rests on
  per-round ids.
- 2026-11-05: rebased onto main after v1.1.0; the landed peer specs are now in depends_on.
- 2026-11-05, drift review: the NATS mapping gives a publish abandoned at the deadline (JetStream or an injected client) `UNKNOWN`, never resent, and names the abandoned-publish latch `_refuse_while_stuck` as a `RETRYABLE` refusal round, which keeps 1.x's worker retry of the refused batch.
- 2026-11-05, the additive rule restored on SPEC-061's code: a NATS round in which nothing was published is `RETRYABLE` throughout, so the worker resends it `w + 1` times as 1.x does and `docs/invariants.md` 2's JetStream asymmetry stays true unrewritten; a publish that raised or an event left unattempted beside a publish that landed is `REJECTED`, counted in `failed` and never resent, as 1.x. The reason that argued for one attempt is replaced by the additive rule, and the Overview's release-notes line no longer names NATS.
- 2026-11-05, execution-frame review: a NATS round with nothing published now registers 1.x's own counts and lines `ALWAYS` (one per raised publish, none for an unattempted event) and marks its `RETRYABLE` events `sink_counted`, a fourth run-context addition, so the engine adds no count; a differential matrix criterion holds every NATS case to values recorded by running 1.x at `93113e1b`, replacing the two hand-written NATS criteria.
- 2026-11-08, build (plan review B1, M1, M2): the SQS FIFO hold is decided per chunk at send time, so same-group events never sent travel together and a healthy one-group batch is one call; a held event never sent is counted with a `0 attempt(s), held behind …` line (reading (a), pending Andrew's call); a repeated event identity keeps one cached entry per occurrence and entry `Id`s persist across rounds; a sink-counted loss is registered as `failed_events` on `log_foundry_counters`, never against the `failed` setter; `_Prepared.index` is `position`; the dedup-callable criterion's fallback case returns a blank string.
- 2026-11-08, build (plan review Q1, Q2, Q3): `max_retries` on RabbitMQ, the socket transport and Syslog is a set-only refusing descriptor rather than a `property`, which the engine's static reader cannot see; a converted sink's total-failure message is the adapter's (listed under *What a caller sees change*); FR-004's wedged-loop backstop is a `REJECTED` refusal round, the reading the shipped NATS tests left standing when both were built and run.
- 2026-11-08, build (FR-005): `SocketTransport` takes an optional `frame` and the owning sink's `sink_name`; `SyslogSink.log_foundry_stop_signal` is a plain attribute the engine can read statically.
