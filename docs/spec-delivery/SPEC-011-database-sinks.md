# Completed Spec — SPEC-011: Database Sinks

## What was completed?

Three external-database sinks that land structured events directly in a queryable store. Each follows
the SPEC-005 recipe — driver behind an optional extra + **lazy import**, injectable client/connection
for tests, single-transaction batch insert per `emit`, bounded retry, and a `close()` that commits
and releases an *owned* connection (an injected one is committed/flushed but not closed). Write-only
(arch §8); `isinstance`-checkable.

- **`sinks.mongodb`** (new) — pymongo (`mongo` extra); `insert_many(ordered=False)`, `BulkWriteError`
  caught with the failed count recorded and successes retained, 16 MB oversized-document drop
  (FR-003, FR-005, FR-006).
- **`sinks.postgres`** (new) — psycopg v3 (`postgres`); `JSONB` `event` column + extracted columns,
  chunked single-transaction insert, rollback-and-retry, optional idempotent `create_table`
  (FR-004, FR-005, FR-006).
- **`sinks.clickhouse`** (new) — clickhouse-connect (`clickhouse`); columnar row projection
  (typed extracted columns + full-event `String`), single `insert` per chunk, optional `MergeTree`
  `create_table` (FR-002, FR-005, FR-006).
- **`sinks._chunk`** (extended) — added `chunk_list` (size-N slicing) and `valid_identifier`
  (SQL-identifier table-name validation) shared by the two SQL sinks.

**Deviation from the Draft:** none of substance — `MongoDBSink` inserts shallow *copies* of each
event so pymongo's `_id` insertion never mutates the caller's dicts (relevant under `MultiSink`
fan-out); duck-typed `BulkWriteError` detection (via `.details.writeErrors`) avoids importing pymongo
just to reference its error class.

## What changed from earlier specs?

Purely additive to `src`. Added three optional extras: `mongo` (pymongo), `postgres`
(psycopg[binary]), `clickhouse` (clickhouse-connect). `clickhouse-connect` caps at Python `<3.15`, so
its dependency carries a PEP 508 marker (`python_version < '3.15'`) — otherwise the project's
unbounded `>=3.13` range fails to resolve in `poetry.lock`. Noted in `pyproject.toml` and CLAUDE.md's
Tech Stack.

## Verification

Local gates green — `ruff` clean, `mypy --strict` clean (46 src files), `pytest` **270 passed**. 28
new tests inject fake clients/connections (no DB) covering row/column projection + missing-key
`NULL`s, unordered insert + `BulkWriteError` (no-retry, successes kept), oversized-document drop,
transactional chunked insert + rollback/retry + persistent-failure counting, `create_table` DDL
(incl. `MergeTree`), owned-vs-injected close, and invalid-table-name rejection. Smoke-tested
`PostgresSink` through the real worker thread (`create_table` DDL ran, rows projected, committed
cross-thread).
