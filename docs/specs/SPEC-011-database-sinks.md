# Spec: Database Sinks

**ID:** SPEC-011
**Status:** Draft
**Last Updated:** 2026-07-10
**Depends On:** SPEC-001, SPEC-005

## Overview

Some teams want their structured span events landing directly in a queryable database rather than a
file or a queue. This spec adds three external-database sinks: `ClickHouseSink` (columnar, the
observability-scale favorite), `MongoDBSink` (near-zero impedance — the events are already dicts, so
`insert_many` takes them as-is), and `PostgresSink` (a `JSONB` column plus a few extracted columns for
indexing). Each follows the SPEC-005 recipe: the driver is an **optional dependency** behind an extra
and imported lazily, a client/connection can be injected for tests, each `emit` batch-inserts inside a
single transaction, and `close()` commits and releases the owned connection. Like every sink (arch §8)
these operate purely on already-built event dicts; they are **write-only** — querying is the
downstream tool's job.

## Scope

### In Scope

- `ClickHouseSink(table, *, client=None, dsn=None, create_table=False)` — batch insert via
  `clickhouse-connect`.
- `MongoDBSink(*, client=None, uri=None, database, collection)` — `insert_many(batch)`.
- `PostgresSink(table, *, connection=None, dsn=None, create_table=False)` — batch insert into a table
  with a `JSONB` `event` column plus extracted columns.
- Shared: lazy driver import, injectable client/connection, single-transaction batch insert per emit,
  bounded failure retry, and `close()` that commits + closes an owned connection.
- Optional idempotent `create_table` (schema-ensure) as a convenience, off by default.

### Out of Scope

- Schema migrations / DDL management beyond the optional `create_table` convenience — the user owns
  their schema and indexes.
- ORMs / query builders — raw driver batch inserts only.
- Read/query/aggregation APIs — these sinks are write-only.
- Embedded SQLite — that is the zero-dependency `SQLiteSink` in SPEC-008, not an external-driver sink.

---

## Functional Requirements

### FR-001: Shared DB sink contract and lazy dependencies

#### Description:

Every database sink is a drop-in `Sink` whose driver is optional and lazily imported.

#### Acceptance Criteria:

- [ ] Each sink satisfies `emit(batch) -> None` / `close() -> None` and passes an
      `isinstance(sink, Sink)` runtime check.
- [ ] Each sink imports its driver inside the constructor/method (never at module top), so importing
      the sink module does not require the extra unless the sink is instantiated without an injected
      client/connection.
- [ ] A fake/injected client or connection lets tests assert on the insert calls with no database
      access.

### FR-002: ClickHouseSink

#### Description:

Batch-insert events into a ClickHouse table.

#### Acceptance Criteria:

- [ ] `emit(batch)` inserts all events in the batch in a single `insert` call, mapping each event dict
      to a row: extracted typed columns (`timestamp`, `level`, `trace_id`, `span_id`, `function`,
      `service`, `duration_ms`, `status`) plus the full event as a JSON/`String` column.
- [ ] With `create_table=True`, a `CREATE TABLE IF NOT EXISTS` with a sensible `MergeTree` schema runs
      once before the first insert; with `create_table=False` (default) the table is assumed to exist.
- [ ] An insert failure is retried within a bounded count, then counted (`failed`) and logged; a
      partial batch is not silently lost.

### FR-003: MongoDBSink

#### Description:

Insert events into a MongoDB collection.

#### Acceptance Criteria:

- [ ] `emit(batch)` calls `insert_many(batch, ordered=False)` on the configured collection so one bad
      document does not abort the rest of the batch.
- [ ] A `BulkWriteError` is caught; the count of failed inserts is recorded (`failed`) and logged while
      successfully-inserted documents are retained.
- [ ] The event dicts are inserted as-is (no reshaping required); `close()` closes an owned client.

### FR-004: PostgresSink

#### Description:

Insert events into a Postgres table with a `JSONB` column.

#### Acceptance Criteria:

- [ ] `emit(batch)` batch-inserts every event in one transaction (e.g. `execute_values`/`executemany`
      or `COPY`) into a table with a `JSONB` `event` column plus extracted columns (`timestamp`,
      `level`, `trace_id`, `span_id`, `function`, `service`).
- [ ] The transaction commits on success; on failure it rolls back and the batch is retried within a
      bounded count, then counted/logged.
- [ ] With `create_table=True`, an idempotent `CREATE TABLE IF NOT EXISTS` runs once; default is off.

### FR-005: close() commits and releases

#### Description:

Buffered/uncommitted work is flushed and owned resources are released on close.

#### Acceptance Criteria:

- [ ] `close()` commits any pending transaction and closes a connection/client the sink opened itself;
      an injected connection/client is committed/flushed but **not** closed.
- [ ] `close()` is idempotent (a second call is a no-op).

### FR-006: Shared chunking and oversized handling

#### Description:

Large batches and oversized documents are handled without crashing the worker.

#### Acceptance Criteria:

- [ ] A very large batch is split into driver-friendly insert chunks (configurable chunk size) rather
      than issuing one unbounded statement.
- [ ] A single document that exceeds a hard driver limit (e.g. MongoDB's 16 MB document cap) is dropped
      with a counted (`dropped_oversized`) warning and does not prevent the rest of the batch from
      being inserted.

---

## Data Model

```
# One module per sink under src/log_forge/sinks/. Shared shape:
<DBSink> {
  target: str                  # table or collection name
  client / connection: <lazily-imported or injected driver handle>
  create_table: bool = False   # (ClickHouse/Postgres) idempotent schema-ensure convenience
  chunk_size: int = 1000
  max_retries: int = 3
  failed: int
  dropped_oversized: int
}
```

Row/column projection (ClickHouse, Postgres) extracts `timestamp`, `level`, `trace_id`, `span_id`,
`function`, `service` (and, where present, `duration_ms`/`status`) and keeps the full event as a
JSON/`JSONB` column. `MongoDBSink` stores the event dict verbatim. Events are the SPEC-001 `LogEvent`
dicts.

---

## API / Interface Contract

```python
# sinks/clickhouse.py
class ClickHouseSink:
    def __init__(self, table, *, client=None, dsn=None, create_table=False,
                 chunk_size=1000, max_retries=3) -> None: ...
    def emit(self, batch: list[dict]) -> None: ...
    def close(self) -> None: ...

# sinks/mongodb.py
class MongoDBSink:
    def __init__(self, *, client=None, uri=None, database, collection, max_retries=3) -> None: ...

# sinks/postgres.py
class PostgresSink:
    def __init__(self, table, *, connection=None, dsn=None, create_table=False,
                 chunk_size=1000, max_retries=3) -> None: ...

# Usage
import log_forge
from log_forge.sinks.clickhouse import ClickHouseSink
log_forge.configure(sink=ClickHouseSink("log_events", dsn="clickhouse://user:pw@host/db",
                                        create_table=True))
```

## Configuration / Environment

- New **optional extras**, each pulling only its driver and imported lazily (added to `pyproject.toml`
  and CLAUDE.md's Tech Stack at implementation time): `clickhouse` (`clickhouse-connect`), `mongo`
  (`pymongo`), `postgres` (`psycopg[binary]`).
- Connection details are passed as a DSN/URI or an injected client; credentials are resolved by the
  driver (or the caller's own environment). log-forge adds no credential configuration of its own.

## File & Folder Structure

```
src/log_forge/sinks/
├── clickhouse.py   # ClickHouseSink (columnar batch insert)          (new)
├── mongodb.py      # MongoDBSink (insert_many, ordered=False)         (new)
└── postgres.py     # PostgresSink (JSONB + extracted columns)         (new)
tests/
├── test_sinks_clickhouse.py   # column projection + batch insert (fake client)     (new)
├── test_sinks_mongodb.py      # insert_many ordered=False + BulkWriteError (fake)   (new)
└── test_sinks_postgres.py     # transactional batch insert + rollback/retry (fake)  (new)
```

## Implementation Phases

### Phase 1: MongoDBSink

- Implement `MongoDBSink` with lazy `pymongo`, `insert_many(ordered=False)`, `BulkWriteError`
  handling, oversized-document drop, and `close()` (FR-001, FR-003, FR-005, FR-006).
- Test insert-as-is, unordered partial failure, and a 16 MB oversized drop with a fake collection.

### Phase 2: PostgresSink

- Implement `PostgresSink` with lazy `psycopg`, single-transaction chunked batch insert into a `JSONB`
  + extracted-columns table, rollback-and-retry, and optional `create_table` (FR-004).
- Test transactional insert, column projection, rollback/retry, and schema-ensure with a fake
  connection/cursor.

### Phase 3: ClickHouseSink

- Implement `ClickHouseSink` with lazy `clickhouse-connect`, columnar row projection, single-call
  batch insert, optional `MergeTree` `create_table`, and bounded retry (FR-002, FR-006).
- Test row projection and batch insert against a fake client.
