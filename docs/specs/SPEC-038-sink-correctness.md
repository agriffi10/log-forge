# Spec: Sink Correctness

**ID:** SPEC-038  
**Status:** Draft  
**Last Updated:** 2026-08-07  
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
extra is uninstalled in this environment. That is recorded per-FR, and FR-011 addresses the gap
itself: a correctness spec for sinks nobody can execute is a spec that can only be half-verified.

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
- Making the extras-backed sinks executable in CI.
- Re-homing the utility sinks out of `sinks.util`, handed over by SPEC-034.
- Three queue/stream sinks that have no bounded retry at all.

### Out of Scope

- **Anything about the `Sink` protocol itself** — the flush hook is SPEC-036 FR-002, the
  `stop_signal` contract is SPEC-034 FR-006.
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

- [ ] AC-1: `HTTPSink` gains `max_batch_count` and `max_batch_bytes` and loops `_send` over
      `chunk_items`, with each platform subclass setting its own documented values.
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

- [ ] AC-1: A `rollback()` that raises does not prevent the remaining attempts.
- [ ] AC-2: A total failure still counts `failed`, still writes one `_diag.lost`, and still
      raises `SinkDeliveryError` — not the driver's exception.
- [ ] AC-3: Reproduced with a fake connection whose `rollback` raises, so it runs without a
      server.
- [ ] AC-4: The same audit is applied to every other sink's error path, derived from the roster:
      no cleanup call inside an `except` may be unguarded. This is FR-002 as a *rule*, per
      `docs/process.md`.

### FR-003: `PostgresSink` recovers from a broken connection

**Read-only analysis.** `sinks/postgres.py:76-80, 149-170`.

The connection is opened once in `__init__` and never reopened. A psycopg `Connection` is
permanently unusable after the server closes it, so one restart, failover or idle timeout ends
log delivery for the life of the process — the in-batch retries all run against the same dead
handle. Every sibling recovers: `SocketTransport._reset`, boto3, clickhouse-connect's pool,
pymongo's pool.

- [ ] AC-1: After a connection failure on an **owned** connection, the sink reconnects on the
      next attempt and delivery resumes.
- [ ] AC-2: An **injected** connection is never reconnected — it is the caller's object, per
      arch §13's borrowed-client constraint — and the sink says so rather than failing silently.
- [ ] AC-3: Reconnection is bounded by the existing retry budget, not a new unbounded loop.
- [ ] AC-4: Verified against a real Postgres (FR-011), since a fake connection cannot show that
      psycopg's handle is unusable. FR-011 AC-2 makes that job non-gating, so this AC is closed
      only against a **green** run of it, not merely against its existence.

### FR-004: `GooglePubSubSink` reaps its futures

**Read-only analysis.** `sinks/pubsub.py:138, 174-186`.

`self._futures` is append-only for the process lifetime: unbounded memory proportional to total
events logged, and `result()` is never called until `close()`, so `failed` stays 0 and
`health()` reports clean through an entire Pub/Sub outage.

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

- [ ] AC-1: Each record's `Data` ends with `\n`.
- [ ] AC-2: The newline is charged to the per-record and per-request byte budgets.
- [ ] AC-3: A test asserts the concatenation of a chunk's payloads parses as NDJSON.

### FR-006: `KafkaSink.close()` is bounded and counts what it lost

**Read-only analysis.** `sinks/kafka.py:178`.

`flush()` is called with no timeout and its return value discarded. `Worker.shutdown` closes the
live sink **inline and unbounded** (arch §13), so an unreachable broker holds process exit for
`message.timeout.ms` — five minutes by default — despite `shutdown(timeout=30)`. And
`Producer.flush()` *returns* the number of messages still queued, which is exactly the count lost
at exit, thrown away.

- [ ] AC-1: `close()` passes a bounded timeout, and the sink documents its worst case as the
      other retrying sinks do.
- [ ] AC-2: A non-zero remainder is counted into `failed` with one `_diag.lost` line.
- [ ] AC-3: The bound interacts correctly with SPEC-027's stop signal — a shutdown cuts it short.

### FR-007: `SyslogSink` drops an oversized datagram rather than retrying it

**Reproduced.** `sinks/syslog.py:110`, loop at `_socket.py:236-256`.

A 70 KB event produced `OSError errno=40` (EMSGSIZE) — a permanent condition — retried 4× with
backoff, counted as `failed` (transient), then raised `SinkDeliveryError`, sending the worker
round for 3 more rounds: ~16 futile sends and seconds of backoff on the single drain thread,
never converging. Every other size-limited sink drops-and-counts before sending.

- [ ] AC-1: An event over the datagram limit is dropped before the send and counted
      `dropped_oversized`.
- [ ] AC-2: The limit is configurable with a documented default (~64 KB for UDP), and TCP is
      unaffected.
- [ ] AC-3: The retry loop no longer treats a permanent `errno` as transient. The permanent set
      is **`EMSGSIZE` only** — every other socket errno the transport can raise (`ECONNREFUSED`,
      `EHOSTUNREACH`, `ENETDOWN`, `EPIPE`, `ETIMEDOUT`) describes a destination that may come
      back, and treating any of those as permanent would turn a transient outage into silent
      loss. Stated as a set in the code, not inferred at the call site.

### FR-008: The Redis sinks can bound their destination

**Read-only analysis.** `sinks/redis.py:210, 249`.

Neither `xadd` nor `rpush` takes a length bound and neither exposes an option to. Used as the
durable buffer arch §9.1 recommends, a stalled consumer OOMs the Redis instance with no ceiling
the operator can set from this library.

- [ ] AC-1: A `maxlen: int | None` constructor argument, passed as
      `xadd(..., maxlen=n, approximate=True)` and an `ltrim` after `rpush`.
- [ ] AC-2: The default is `None` — unbounded, today's behaviour — because silently discarding a
      user's buffered logs is not a default this library may choose.
- [ ] AC-3: The docstring states that trimming discards at the *destination*, outside anything
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

- [ ] AC-1: `MAX_RECORD_BYTES = 1_024_000` in Firehose.
- [ ] AC-2: `KinesisSink`'s `size_of` includes `len(r["PartitionKey"])`. Firehose's is unchanged
      except for FR-005's newline.
- [ ] AC-3: A test at each boundary — one byte under and one byte over — for both the per-record
      and per-request limits, per sink and against that sink's real budget.

### FR-010: `LogstashSink` sends a content type Logstash parses

**Read-only analysis, lower confidence.** `sinks/logstash.py:76-79`.

HTTP mode sends `application/x-ndjson`. Logstash's `http` input maps only `application/json` to a
JSON codec by default, falling back to `plain` — so the whole body arrives as a single event with
every line stuffed into `message`. It works only if the user has set `codec => json_lines`.

- [ ] AC-1: Verified against a real Logstash (FR-011) before changing anything — this is the
      one finding where the fix could be worse than the defect if the analysis is wrong. As with
      FR-003 AC-4, closed against a green run of that job rather than its existence.
- [ ] AC-2: Whichever way it resolves, the class docstring states the Logstash-side configuration
      the sink expects.

### FR-011: The extras-backed sinks are executable in CI

#### Description:

Six of the ten findings above are read-only because no extra is installed in the development
environment or in CI, and CI's no-extras environment is deliberately the contract (`CLAUDE.md`).
That is right for the *type* gate and wrong for correctness: it means the sinks most likely to be
run in production are the ones this project can least verify.

The gap is not hypothetical — FR-003 and FR-010 cannot be honestly closed without it, and this
spec would otherwise ship two FRs whose acceptance criteria are "we read it again".

#### Acceptance Criteria:

- [ ] AC-1: A CI job, separate from the gating no-extras one, installs the extras and runs the
      sink suite against real services in containers — at minimum Postgres, Redis, Kafka and a
      Logstash, which are the four with findings here that a fake cannot settle.
- [ ] AC-2: It is **not** required for a merge initially. A new, flaky-by-nature integration job
      that blocks every PR is a job that gets disabled; it earns the gate by being green for a
      while first, and the FR says when that decision gets revisited.
- [ ] AC-3: The no-extras environment stays exactly as it is, since `mypy --strict`'s
      `type: ignore[import-not-found]` comments depend on it (`CLAUDE.md`).
- [ ] AC-4: Which sinks remain unverified after this job exists is written down, so the next
      audit knows what it is still reading rather than running.

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

- [ ] AC-1: The default becomes `backup_count=1`; a rotation leaves `path` and `path.1`.
- [ ] AC-2: `backup_count=0` still truncates, unchanged.
- [ ] AC-3: No counter and no `losses()`; the docstring records the decision so its absence does
      not read as an oversight.
- [ ] AC-4: The docstring and README state what `backup_count=0` discards, and what the new
      default costs on disk: **2 × `max_bytes` under a size trigger, one full rollover period
      under a time-only one**, where `max_bytes` defaults to `0` and bounds nothing.
- [ ] AC-5: The docstring's existing claim "No event is lost across a rotation" is narrowed to
      the pending event it actually means.
- [ ] AC-6: A test writes past `max_bytes` **twice** and asserts `path.1` holds the *second*
      generation and `path.2` does not exist.

### FR-013: The utility sinks leave `sinks.util`

#### Description:

`MemorySink`, `NullSink` and `StderrSink` live in `log_foundry.sinks.util`. `MemorySink` is the
sink every downstream test suite will import, and `util` is a poor permanent home — moving it
after `1.0.0` costs a major version. SPEC-034 hands this over rather than doing it, because it is
a pure move with no design content and this spec is already in the sink package.

- [ ] AC-1: `MemorySink` and `NullSink` get their own modules; `StderrSink` sits with
      `StdoutSink`, which is what it is a variant of.
- [ ] AC-2: Import paths in `README.md`, `docs/component-inventory.md` and every test are updated.
- [ ] AC-3: `sinks/util.py`'s `__all__` is exactly these three classes, so the module is left
      **empty and deleted** rather than kept as a shell.
- [ ] AC-4: No compatibility alias is left behind in `sinks.util` — an alias would have to live
      for all of `1.x`, which is the cost the move is being made to avoid.

### FR-014: Three queue sinks have no bounded retry, and the docs said they did

#### Description:

`KafkaSink`, `NATSSink` and `GooglePubSubSink` neither import `sinks/_retry` nor accept
`max_retries`. The README's "All publish + retry within a bound" was false for three of the seven
queue/stream sinks, and the README PR narrows the prose — which fixes the *claim* and leaves the
*gap*, so three sinks ship at 1.0 outside SPEC-027's guarantee.

That may be the right answer: each of the three has a client with its own retry and its own
delivery timeout, and layering a second retry on top of `librdkafka`'s can multiply the worst-case
delay rather than bound it. But it has to be a decision, not a documentation edit.

- [ ] AC-1: Closed only against a **green** run of FR-011's integration job, as FR-003 AC-4 and
      FR-010 AC-1 are — Phase 5 already sequences it there. For each of the three, the FR states
      whether the client's own retry satisfies
      SPEC-027's guarantee — bounded, and cut short by a shutdown — with the setting that makes it
      so.
- [ ] AC-2: Where it does, the class docstring states the worst-case total delay as every other
      retrying sink does, and the stop signal is honoured (which is what "cut short by a shutdown"
      requires and what SPEC-034 FR-006 is about to make a documented contract member).
- [ ] AC-3: Where it does not, bounded retry is added.
- [ ] AC-4: `README.md`'s claim matches the outcome, whichever way each resolves.

---

## Implementation Phases

### Phase 1: The silent-loss three (FR-001, FR-002, FR-004)

All reproducible without a real service, and the ones that lose data.

### Phase 2: The integration job (FR-011)

Before Phase 3, because FR-003 and FR-010 depend on it.

### Phase 3: The service-verified fixes (FR-003, FR-010)

### Phase 4: The rest (FR-005..FR-009, FR-012, FR-013)

Independent of each other; reviewable in one pass. FR-013 is a pure move and should be its own
commit inside it, so the diff stays readable.

### Phase 5: FR-014

Last, because AC-1 needs each client's retry behaviour verified against a real service — which is
what FR-011 provides.
