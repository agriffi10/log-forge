# Completed Spec — SPEC-069: Encoding and Driver Errors — One Bad Event Costs Only Itself in Every Sink

## What was completed?

Five FRs, built in `82f25317`..`bc2cc3b5` on the first spec of the `stability-redesign` arc. The
principle: **an event the sink cannot encode costs only itself, and one its destination refuses is
repaired and delivered.**

- **One encode step before the first bulk call** (FR-001): `sinks/_encode.encode_events` runs a
  sink's per-event `encode` over the whole batch under an `Exception` guard, outside any lock and
  retry loop, and records the failures on the call's `Deferral` with one `encoding raised <Type>`
  line per call. Twenty sink classes take it — the sixteen that raised `TypeError`, plus both
  Redis sinks, `PostgresSink` and `GooglePubSubSink` — so the eight that duplicated delivery no
  longer do, and Postgres, Redis and ClickHouse encode once per `emit`, not per attempt. The count
  lands in a private `_unencodable`, in `rejected` for Kafka and Pub/Sub, and on
  `SocketTransport._unencodable` for the socket sinks. `StdoutSink`, `StderrSink`, `FileSink`,
  `RotatingFileSink` and `SQLiteSink` gain a public `losses()` (`None` until something is counted)
  and a `_counter_lock`. A roster test in `tests/test_encode.py`, with a floor and a fixture corpus,
  refuses a class whose `emit` reaches `json.dumps` and is on neither the helper nor a named
  exemption; `tests/test_deferral.py` gains the sites and `_unencodable`.
- **A batch of nothing encodable settles, counted** (FR-002), Andrew's choice of 2026-11-04.
  `SocketTransport.send_all` now runs its closed check before its empty-list return, so a closed
  socket sink still raises for one.
- **Destination repairs** (FR-003): `PostgresSink` detects a real U+0000 with `NUL_ESCAPE` over its
  JSON text and `repair_nul` replaces each with U+FFFD; `MongoDBSink` asks a lazily imported
  `bson.encode` and `repair_bson` replaces an out-of-int64 integer with `sanitize.int_placeholder`
  (factored out of `_int_text`, held equal to its three inline copies) and a NUL key with U+FFFD.
  Both work on a marked (`truncated`) copy and count nothing.
- **Driver errors** (FR-004): `SQLiteSink` and the file sinks add `len(batch)` to a private
  `_failed`, write one line (with `errno=N` for `OSError`) and raise
  `sinks/_driver_error.delivery_error`'s instance of both `SinkDeliveryError` and the driver's class,
  or the original unchanged if it cannot be built.
- **`SyslogSink`'s PROCID** (FR-005) is read per message, so a forked child stamps its own pid.

Guarded by `tests/test_encode.py`, `tests/test_sinks_encode.py` (over rigs in `tests/sink_rigs.py`),
`tests/test_driver_error.py`, the fork cases in `tests/test_fork_lifecycle.py` (the new counter
locks, and FR-005's pid), and the FR-003 cases in `tests/integration/test_postgres.py` and
`test_mongo.py`.

Decisions: `docs/decisions/sink-delivery/an-event-a-sink-cannot-encode-costs-only-itself-and-a-batch-of-nothing-encodable-settles-counted.md`,
`docs/decisions/sink-delivery/a-sinks-own-driver-error-is-counted-announced-and-raised-as-both-types.md`
and `docs/decisions/event-assembly/a-destinations-refusal-is-repaired-at-the-sink-and-marked-as-assembly-marks.md`;
five more rewritten in place (History dated 2026-11-06). Invariants 2, 7 and 8 widened.

## Measured base figures

From the spec, at `fda134a`: with `[ok, bad, ok]` and `bad` holding `bytes`, sixteen classes raised
`TypeError` with nothing counted, eight of them after handing earlier events over; Postgres and both
Redis sinks counted `failed=3`. One `2**64` cost `MongoDBSink(max_retries=1)` the batch against
MongoDB 7.0.40, and one U+0000 cost `PostgresSink` the batch against PostgreSQL 17.11.

## What changed from earlier specs?

- **SPEC-058**: the HTTP family's settle-don't-re-raise rule for an event the sink's own encode
  refuses is generalised to every record-building sink; the HTTP family itself is untouched.
- **SPEC-059**: the deferring-site roster and the derived `dropped` counters gain every converted
  sink and `_unencodable`.
- **SPEC-026**: `SocketTransport.send_all` refuses an empty call when closed; three sinks raise a
  `SinkDeliveryError` subclass that is also the driver's type.
- **SPEC-038**: `RotatingFileSink` now has a `losses()`; its retention test asserts `None` instead
  of no accessor.
- **SPEC-066**: `sanitize.int_placeholder` is factored out of `_int_text`, whose output is unchanged.

## What changed for a caller

In `docs/release-notes/v1.2.0.md`, *Fixed*: the four SPEC-069 entries, with the counter table. Five
sinks gain a public `losses()`; no other public name, argument or `configure()` key changes.

## Follow-ups

- **Owed until the PR's CI is green:** FR-003's real-service criterion settles on the Integration
  job against the compose PostgreSQL and MongoDB, not before the push.
- ~~**Out of scope, still open:** a partial file or stream write leaves earlier lines for the retry
  to write again; a SQLite column value that cannot be bound (an integer past 64 bits, a `dict`)
  from a direct `emit` still costs the batch; a lone surrogate reaching `PostgresSink` through a
  direct `emit` is unmeasured.~~ All three resolved on 2026-11-13 by the SPEC-069 follow-up fix
  (no spec):
  - **The partial write:** ruled an accepted edge for 1.2.0 by Andrew, measured at `a8e91fac` (69 to 200
    of a 200-event batch landed twice when the fault cleared before the retry, and at one cap a
    line was torn) and recorded in
    `docs/decisions/sink-delivery/a-sinks-own-driver-error-is-counted-announced-and-raised-as-both-types.md` and as an exception to invariant 7; the closure, the file sinks on `deliver`, is
    to be specified after 1.2.0.
  - **The SQLite column:** fixed. A container, or an integer outside the signed 64-bit range, is
    bound as its JSON text (`sinks/_encode.column_value`); `PostgresSink` takes the same rule for
    the `dict` and mixed or ragged `list` psycopg cannot adapt. Recorded in `docs/decisions/event-assembly/a-destinations-refusal-is-repaired-at-the-sink-and-marked-as-assembly-marks.md`.
  - **The lone surrogate:** measured against PostgreSQL 17.11, where it cost the batch, and fixed:
    the event is dropped alone, counted and announced (`sinks/_encode.refuse_surrogate`), in the
    same decision. Guarded by `tests/test_sinks_column_binding.py` and
    `tests/integration/test_postgres.py`.

## Verification

The tests named above; the gate results are recorded in the PR.
