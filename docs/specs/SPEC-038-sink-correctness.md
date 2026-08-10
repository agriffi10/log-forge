# Spec: Sink Correctness

**ID:** SPEC-038  
**Status:** Draft  
**Last Updated:** 2026-08-09  
**Depends On:** SPEC-018, SPEC-026, SPEC-027, SPEC-032

## Overview

Ten defects in individual sinks, from the sink surface of the 2026-08-07 audit
(`docs/audits/2026-08-07-pre-1.0.md`, K1–K10). They are grouped because they are all "this sink
does not match what its destination actually requires", and because ten one-FR specs would be
worse paperwork than one spec with ten FRs — SPEC-031's precedent.

Three are silent loss of the shape this project has spent ten specs removing, and they are first.
**K1 is the worst and is not sink-specific in origin**: `_final_drain` hands the whole exit
backlog to the sink as a single batch — measured at **5,980 events in one `emit`** once the queue
backs up — and the HTTP family is the only one that never re-chunks. Datadog caps at 1,000
entries, New Relic at 1 MB, Honeycomb at 5 MB; the resulting 400/413 is not in `_send`'s
retryable set, so the entire backlog is abandoned in one request at the moment a process exits.

**Six of the ten were found by reading, not running**, because every sink behind an optional
extra is uninstalled in this environment. That is recorded per-FR — and the four FRs that cannot
be honestly closed without a real service have **moved to [SPEC-041](SPEC-041-sink-integration-verification.md)**
along with the CI job they need. What is left here is every fix that a fake can settle, which is
ten of the fourteen, and none of them waits on containers in CI.

~~FR-011 addresses the gap itself: a correctness spec for sinks nobody can execute is a spec that
can only be half-verified.~~ — struck (SPEC-021). The observation stands and the packaging was
wrong: standing up Postgres, Redis, Kafka and Logstash in CI is infrastructure work with its own
flakiness budget and its own "when does it earn the merge gate" decision, and putting it in front
of `FR-001`'s 5,980-event silent loss made the urgent wait on the slow. Split, both halves move.

## Scope

### In Scope

- Batch sizing at the HTTP sinks, and the exit drain that overruns it.
- `PostgresSink`'s rollback and its dead connection.
- `GooglePubSubSink`'s unbounded futures.
- `FirehoseSink`'s missing delimiter, and the Kinesis/Firehose byte budgets.
- `KafkaSink.close()`'s unbounded, unmeasured flush.
- `SyslogSink` retrying a permanent failure.
- The Redis sinks growing without bound at the destination.
- `LogstashSink`'s content type.
- `RotatingFileSink`'s default.
- Re-homing the utility sinks out of `sinks.util`, handed over by SPEC-034.

### Out of Scope

- **Anything needing a real service to verify** — `PostgresSink`'s dead connection,
  `LogstashSink`'s content type, the three queue sinks with no bounded retry, and the CI job all
  four need. SPEC-041, split out of this spec for the reason in the Overview.
- **Anything about the `Sink` protocol itself** — the flush hook is SPEC-036 FR-002, the
  `log_foundry_stop_signal` contract is SPEC-034 FR-006.
- **Re-chunking inside the worker.** FR-001 fixes the sinks, not the drain: a batch size is the
  destination's constraint and the destination's business, and `chunk_items` already exists for
  exactly this. Bounding `_final_drain` instead would leave a sink that receives a large batch by
  any other route still broken.
- **New sinks, or new transports for existing ones.**

---

## Functional Requirements

### FR-001: The HTTP sinks re-chunk to their destination's limits

**Reproduced.** `sinks/http.py`, `datadog.py`, `newrelic.py`, `honeycomb.py`, `loki.py`.

Every queue/stream sink re-chunks through `chunk_items`; the HTTP family does not, so it delivers
whatever batch it is handed. `Worker._final_drain` hands it the entire pending backlog at exit —
5,980 events measured. Datadog's intake rejects it, `_send` treats 400/413 as non-retryable
(429/5xx only), and `_abandon` fires on the whole thing.

**The stated fix reaches one of the four sinks, and that was checked before building rather than
during** (2026-08-10). `DatadogSink`, `LokiSink` and `HoneycombSink` all **override `emit`** —
each builds a body from the whole batch and calls `_send` once — so a chunking loop added to
`HTTPSink.emit` would apply to `NewRelicSink` alone, which is the only one that inherits it. The
FR's own AC-1 says "`HTTPSink` … loops `_send` over `chunk_items`", which is true and
insufficient.

The shape that works is a template method: `HTTPSink.emit` owns the chunk loop and the subclasses
override a `_body(chunk) -> tuple[bytes, str]` hook instead of `emit`. All four already have
exactly that shape inside their `emit` (`if not batch: return` → build a body → `_send`), so it is
a mechanical change, and Loki's stream grouping still works because it groups *within* a chunk.
**AC-1a is the part that makes it stay fixed**: a lint asserting no `HTTPSink` subclass overrides
`emit`. Without it a fifth platform sink silently bypasses the chunking, which is the roster
lesson SPEC-032 and SPEC-035 have already paid for twice.

#### Acceptance Criteria:

- [ ] AC-1: `HTTPSink` gains `max_batch_count` and `max_batch_bytes` and loops `_send` over
      `chunk_items`, with each platform subclass setting its own documented values. **The loop
      lives in `HTTPSink.emit` and the subclasses override a `_body` hook**, per the note above —
      three of them override `emit` today and would otherwise keep their unchunked path.
- [ ] AC-1a: A lint asserts no `HTTPSink` subclass overrides `emit`, derived from the class
      hierarchy rather than a list, so a later platform sink cannot silently bypass the chunking.
- [ ] AC-2: A single `emit` of 6,000 events against a fake HTTP server produces multiple requests,
      each within both limits. This is the reproduction, run against `http.server`.
- [ ] AC-3: A chunk that fails does not abandon chunks that succeeded — partial delivery is
      counted per SPEC-026, not reported as total failure.
- [ ] AC-4: 413 joins the retryable set only if retrying could help; the FR states which and why.
      A payload too large is *permanent* for that chunk, so the right answer is to split, not
      retry — and if a single event exceeds the limit it is dropped and counted
      `dropped_oversized`, as the AWS sinks already do.
- [ ] AC-5: The per-platform limits are cited to their documentation in each subclass docstring.
- [ ] AC-6: `README.md`'s `HTTPSink` signature table gains `max_batch_count`/`max_batch_bytes`.
      The README PR is adding `max_retry_after` and `opener` to that same table, so this AC exists
      to stop the two passes leaving it half-right.

### FR-002: `PostgresSink`'s rollback cannot bypass the retry, the counter and the diagnostic

**Reproduced.** `sinks/postgres.py:157`.

`self._conn.rollback()` is unguarded inside the `except`. When the server closed the session —
the most common reason the insert failed — psycopg raises from `rollback()` too, and that escapes
mid-handler. Measured with `max_retries=3`: attempts drop from 4 to **1**, `losses()` reports
`failed=0` after a totally lost batch, no `_diag.lost` line is written, and a raw driver
exception reaches the worker instead of `SinkDeliveryError`.

#### Acceptance Criteria:

- [ ] AC-1: A `rollback()` that raises does not prevent the remaining attempts.
- [ ] AC-2: A total failure still counts `failed`, still writes one `_diag.lost`, and still
      raises `SinkDeliveryError` — not the driver's exception.
- [ ] AC-3: Reproduced with a fake connection whose `rollback` raises, so it runs without a
      server.
- [ ] AC-4: The same audit is applied to every other sink's error path, derived from the roster:
      no cleanup call inside an `except` may be unguarded. This is FR-002 as a *rule*, per
      `docs/process.md`.

### ~~FR-003: `PostgresSink` recovers from a broken connection~~ — moved to SPEC-041

The connection is opened once in `__init__` and never reopened, so one failover ends log delivery
for the life of the process. Its AC-4 said "verified against a real Postgres … closed only
against a **green** run of FR-011", and a fake connection cannot show that a psycopg handle is
unusable. It moved with the job it depends on.


### FR-004: `GooglePubSubSink` reaps its futures

**Read-only analysis.** `sinks/pubsub.py:138, 174-186`.

`self._futures` is append-only for the process lifetime: unbounded memory proportional to total
events logged, and `result()` is never called until `close()`, so `failed` stays 0 and
`health()` reports clean through an entire Pub/Sub outage.

#### Acceptance Criteria:

- [ ] AC-1: `emit` drains already-completed futures and counts failures, so an outage is visible
      in `losses()` while it is happening.
- [ ] AC-2: The pending list is bounded at **1,000 outstanding futures** — roughly one drain
      interval's worth at the default batch size, large enough not to serialize ordinary
      publishing and small enough to bound memory. At the bound `emit` blocks on the oldest
      future rather than dropping, since the event has already been handed to the client and
      dropping it here would lose something the client may yet deliver.
- [ ] AC-3: `close()` still resolves whatever remains.
- [ ] AC-4: Memory does not grow with total events logged. A test asserts the list length under a
      sustained load rather than asserting the code shape.

### FR-005: `FirehoseSink` delimits its records

**Read-only analysis.** `sinks/firehose.py:180`.

Firehose concatenates record payloads verbatim into the delivery buffer; the producer must supply
the separator. As written the S3 objects contain `{"a":1}{"b":2}` — unparseable by Athena, Glue
and OpenSearch ingest, and inconsistent with the NDJSON every other sink in this repo emits.

#### Acceptance Criteria:

- [x] AC-1: Each record's `Data` ends with `\n`.
- [x] AC-2: The newline is charged to the per-record and per-request byte budgets.
- [x] AC-3: A test asserts the concatenation of a chunk's payloads parses as NDJSON.

### FR-006: `KafkaSink.close()` is bounded and counts what it lost

**Read-only analysis.** `sinks/kafka.py:178`.

`flush()` is called with no timeout and its return value discarded. `Worker.shutdown` closes the
live sink **inline and unbounded** (arch §13), so an unreachable broker holds process exit for
`message.timeout.ms` — five minutes by default — despite `shutdown(timeout=30)`. And
`Producer.flush()` *returns* the number of messages still queued, which is exactly the count lost
at exit, thrown away.

#### Acceptance Criteria:

- [x] AC-1: `close()` passes a bounded timeout, and the sink documents its worst case as the
      other retrying sinks do.
- [x] AC-2: A non-zero remainder is counted into `failed` with one `_diag.lost` line.
- [x] AC-3: ~~The bound interacts correctly with SPEC-027's stop signal — a shutdown cuts it
      short.~~ **Amended by evidence** (2026-08-10), as FR-001 AC-4a was and on the same
      reasoning. It was built as written and the result was total loss: `Worker.shutdown` sets
      the stop event *before* the join, so it is always set when `close()` runs, and `produce()`
      is a local hand-off while `flush()` is the only thing that drains the producer's batch —
      so cutting the timeout short switched Kafka's exit delivery off entirely. Measured through
      a real `shutdown()`: 11 buffered, `flush(0)`, **zero delivered**, all 11 booked as
      `failed`, which is worse than the five-minute hang the bound exists to fix. The stop
      signal is therefore **not consulted**, the sink no longer carries the attribute, and
      `flush_timeout` is the whole bound. A shutdown shortens a *wait*; it must never skip
      *work* on the exit drain.

### FR-007: `SyslogSink` drops an oversized datagram rather than retrying it

**Reproduced.** `sinks/syslog.py:110`, loop at `_socket.py:236-256`.

A 70 KB event produced `OSError errno=40` (EMSGSIZE) — a permanent condition — retried 4× with
backoff, counted as `failed` (transient), then raised `SinkDeliveryError`, sending the worker
round for 3 more rounds: ~16 futile sends and seconds of backoff on the single drain thread,
never converging. Every other size-limited sink drops-and-counts before sending.

#### Acceptance Criteria:

- [x] AC-1: An event over the datagram limit is dropped before the send and counted
      `dropped_oversized`.
- [x] AC-2: The limit is configurable with a documented default (~64 KB for UDP), and TCP is
      unaffected.
- [x] AC-3: The retry loop no longer treats a permanent `errno` as transient. The permanent set
      is **`EMSGSIZE` only** — every other socket errno the transport can raise (`ECONNREFUSED`,
      `EHOSTUNREACH`, `ENETDOWN`, `EPIPE`, `ETIMEDOUT`) describes a destination that may come
      back, and treating any of those as permanent would turn a transient outage into silent
      loss. Stated as a set in the code, not inferred at the call site.

### FR-008: The Redis sinks can bound their destination

**Read-only analysis.** `sinks/redis.py:210, 249`.

Neither `xadd` nor `rpush` takes a length bound and neither exposes an option to. Used as the
durable buffer arch §9.1 recommends, a stalled consumer OOMs the Redis instance with no ceiling
the operator can set from this library.

#### Acceptance Criteria:

- [x] AC-1: A `maxlen: int | None` constructor argument, passed as
      `xadd(..., maxlen=n, approximate=True)` and an `ltrim` after `rpush`.
- [x] AC-2: The default is `None` — unbounded, today's behaviour — because silently discarding a
      user's buffered logs is not a default this library may choose.
- [x] AC-3: The docstring states that trimming discards at the *destination*, outside anything
      `losses()` can see.

### FR-009: Kinesis and Firehose size their requests correctly

**Read-only analysis.** `sinks/firehose.py:47`, `kinesis.py:117-119`.

Firehose's per-record limit is 1,000 KiB (1,024,000 bytes), not `1024*1024`; records between the
two are passed through and rejected by the service.

Separately, **Kinesis** sizes its request with `size_of=lambda r: len(r["Data"])`, ignoring the
up-to-256-byte `PartitionKey` per record — up to ~128 KB per 500-record request — while `SQSSink`
deliberately charges its FIFO ids and documents why. This applies to Kinesis **only**: a Firehose
record is `{"Data": data}` and the API has no partition key, so a draft AC requiring "both"
`size_of` lambdas to include one was unimplementable for one of the two.

#### Acceptance Criteria:

- [x] AC-1: `MAX_RECORD_BYTES = 1_024_000` in Firehose.
- [x] AC-2: `KinesisSink`'s `size_of` includes `len(r["PartitionKey"])`. Firehose's is unchanged
      except for FR-005's newline.
- [x] AC-3: A test at each boundary — one byte under and one byte over — for both the per-record
      and per-request limits, per sink and against that sink's real budget.

### ~~FR-010: `LogstashSink` sends a content type Logstash parses~~ — moved to SPEC-041

Read-only analysis and the **lowest-confidence finding in the audit**, whose own AC-1 said to
verify against a real Logstash *before changing anything* — "the one finding where the fix could
be worse than the defect if the analysis is wrong". That is precisely a criterion that cannot be
met here.


### ~~FR-011: The extras-backed sinks are executable in CI~~ — moved to SPEC-041

The gap is real and the reasoning is carried across unchanged. It is not a sink fix: it is a CI
job with containers, a flakiness budget and a "when does this earn the merge gate" decision, and
three FRs depended on it. Making the other ten wait behind it is what the split undoes.


### FR-012: `RotatingFileSink`'s default keeps a generation

#### Description:

Carried from the earlier SPEC-034 draft, twice reviewed. With `backup_count=0` — the **default** —
`_rotate` calls `os.remove(self._path)`, so everything since the last rotation is destroyed.
`RotatingFileSink("app.log", max_bytes=10_000_000)` silently throws away 10 MB at each rollover.

No counter is added, for *any* `backup_count`: `RotatingFileSink` is a bounded ring buffer,
retention is the configuration, and discarding the oldest generation is that configuration
working — the precedent being `MemorySink(maxlen)`, which "behaves as a bounded ring", counts
nothing, and implements no `losses()`. Neither `dropped` (defined as discarded *before*
attempting delivery) nor `failed` fits an event that was written and flushed to disk.

#### Acceptance Criteria:

- [x] AC-1: The default becomes `backup_count=1`; a rotation leaves `path` and `path.1`.
- [x] AC-2: `backup_count=0` still truncates, unchanged.
- [x] AC-3: No counter and no `losses()`; the docstring records the decision so its absence does
      not read as an oversight.
- [x] AC-4: The docstring and README state what `backup_count=0` discards, and what the new
      default costs on disk: **2 × `max_bytes` under a size trigger, one full rollover period
      under a time-only one**, where `max_bytes` defaults to `0` and bounds nothing.
- [x] AC-5: The docstring's existing claim "No event is lost across a rotation" is narrowed to
      the pending event it actually means.
- [x] AC-6: A test writes past `max_bytes` **twice** and asserts `path.1` holds the *second*
      generation and `path.2` does not exist.

### FR-013: The utility sinks leave `sinks.util`

#### Description:

`MemorySink`, `NullSink` and `StderrSink` live in `log_foundry.sinks.util`. `MemorySink` is the
sink every downstream test suite will import, and `util` is a poor permanent home — moving it
after `1.0.0` costs a major version. SPEC-034 hands this over rather than doing it, because it is
a pure move with no design content and this spec is already in the sink package.

#### Acceptance Criteria:

- [x] AC-1: `MemorySink` and `NullSink` get their own modules; `StderrSink` sits with
      `StdoutSink`, which is what it is a variant of.
- [x] AC-2: Import paths in `README.md`, `docs/component-inventory.md` and every test are updated,
      **and the one live reference that is not an import**:
      `tests/test_diag.py::test_only_diag_writes_to_stderr`'s docstring names `sinks/util.py` as a
      module that legitimately writes to `sys.stderr` as a destination for the user's events. The
      lint itself is `rglob`-derived and so needs no edit, which is precisely why the stale
      sentence would survive a grep for imports. Historical records — SPEC-008 and its delivery
      doc, the audit — are left alone; they describe what shipped then.
- [x] AC-3: `sinks/util.py`'s `__all__` is exactly these three classes, so the module is left
      **empty and deleted** rather than kept as a shell.
- [x] AC-4: No compatibility alias is left behind in `sinks.util` — an alias would have to live
      for all of `1.x`, which is the cost the move is being made to avoid.

### ~~FR-014: Three queue sinks have no bounded retry, and the docs said they did~~ — moved to SPEC-041

`KafkaSink`, `NATSSink` and `GooglePubSubSink` neither import `sinks/_retry` nor accept
`max_retries`. Its AC-1 is "closed only against a **green** run of FR-011", because whether each
client's own retry already satisfies SPEC-027 — bounded, and cut short by a shutdown — is a
question about the client, not about this code.

---

## Implementation Phases

### Phase 1: The silent-loss three (FR-001, FR-002, FR-004)

All reproducible without a real service, and the ones that lose data. FR-001 is the worst and
needs nothing from anywhere.

### Phase 2: The rest (FR-005..FR-009, FR-012, FR-013)

Independent of each other; reviewable in one pass, and parallelisable across more than one if
that helps. FR-013 is a pure move and should be its own commit inside it, so the diff stays
readable.

~~Phase 2: the integration job. Phase 3: the service-verified fixes. Phase 5: FR-014.~~ — struck
with the FRs that needed them (SPEC-041).
