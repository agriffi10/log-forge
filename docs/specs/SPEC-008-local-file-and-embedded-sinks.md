# Spec: Local File and Embedded Sinks

**ID:** SPEC-008
**Status:** Completed
**Last Updated:** 2026-07-11
**Depends On:** SPEC-001

## Overview

Not every deployment ships logs to a cloud queue; many want them on the local disk or in an embedded
database for local dev, debugging, air-gapped hosts, or simple archival. This spec adds a family of
**zero-dependency** local sinks: `FileSink` (append NDJSON to a file), `RotatingFileSink` (size- and
time-based rotation with backup retention), `SQLiteSink` (batch-insert structured rows into an
embedded SQLite database, queryable with plain SQL), and three small utilities — `StderrSink`
(NDJSON to stderr, matching the twelve-factor convention), `NullSink` (discard, for benchmarks and
disabling output), and `MemorySink` (collect into an in-process list, for tests and notebooks). All
use only the standard library (`io`, `os`, `sqlite3`) and, like every sink (arch §8), operate purely
on already-built event dicts.

## Scope

### In Scope

- `FileSink(path, *, encoding="utf-8")` — append one `json.dumps` line per event; flush per emit.
- `RotatingFileSink(path, *, max_bytes=0, backup_count=0, when=None, interval=1)` — rotate the active
  file on a size threshold and/or a time interval, keeping `backup_count` numbered backups.
- `SQLiteSink(database, *, table="log_events", connection=None, create_table=True)` — optionally
  ensure the target table exists, then batch-insert each event (raw JSON plus a few extracted
  columns) in one transaction.
- `StderrSink(stream=None)` — the `StdoutSink` shape targeting `sys.stderr` by default.
- `NullSink()` — discard every batch (optional `dropped` counter).
- `MemorySink(maxlen=None)` — append events to an in-memory list exposed as `.events` (optional ring
  buffer when `maxlen` is set).
- `close()` that flushes/commits and releases file handles / DB connections where held.
- Zero runtime dependency (standard library only).

### Out of Scope

- Asynchronous or `mmap`/`O_DIRECT` file I/O — synchronous stdlib writes only.
- Shipping or tailing these files/DBs downstream (that is a separate consumer, arch §9.1) — write side
  only.
- External databases (Postgres, ClickHouse, MongoDB) — SPEC-011.
- Concurrent writers to the same file/DB from multiple processes — a single process/worker-thread
  writer is assumed (arch §9); cross-process coordination is out of scope.

---

## Functional Requirements

### FR-001: FileSink

#### Description:

Append events as NDJSON to a single file — the simplest durable local sink.

#### Acceptance Criteria:

- [ ] `FileSink(path)` opens `path` in append mode (text, given `encoding`) and `emit(batch)` writes
      one `json.dumps(event)` line terminated by `\n` per event, then flushes.
- [ ] Writing to a path whose parent directory exists succeeds; the file is created if absent and
      appended to (never truncated) if present.
- [ ] `close()` flushes and closes the file handle; a second `close()` is a no-op.
- [ ] `isinstance(FileSink(path), Sink)` is `True`; the sink references no span/context types.

### FR-002: RotatingFileSink

#### Description:

Bound on-disk growth by rotating the active file on size and/or time, retaining a fixed number of
backups.

#### Acceptance Criteria:

- [ ] With `max_bytes > 0`, when appending the current batch would push the active file past
      `max_bytes`, the file is rotated before/as it exceeds the limit rather than growing unbounded.
- [ ] With a time trigger (`when`/`interval`), the active file rotates once the interval since the
      last rotation has elapsed.
- [ ] Rotation renames the active file through numbered backups (`path.1` … `path.N`), deletes any
      backup beyond `backup_count`, and opens a fresh active file; no event is lost across a rotation.
- [ ] `backup_count=0` means "rotate by truncating/replacing, keep no backups"; the active file never
      exceeds the configured bound for long.
- [ ] `close()` flushes and closes the active handle.

### FR-003: SQLiteSink

#### Description:

Persist events as queryable rows in an embedded SQLite database.

#### Acceptance Criteria:

- [ ] With `create_table=True` (the default), `SQLiteSink(database)` ensures a table (default
      `log_events`) exists with at least columns `log_id`, `trace_id`, `span_id`, `timestamp`,
      `level`, `function`, and `event` (the full event as a JSON string); creation is idempotent
      (`CREATE TABLE IF NOT EXISTS`) so the sink owns its own schema out of the box.
- [ ] With `create_table=False`, the sink runs no DDL and assumes the caller has provisioned the
      table (e.g. via their own migrations); a missing or column-incompatible table surfaces as a
      normal `sqlite3` error at insert time rather than being silently created.
- [ ] `emit(batch)` inserts every event in the batch within a single transaction (executemany), and
      the extracted columns are populated from the corresponding event keys (missing keys → `NULL`).
- [ ] A `connection` may be injected (e.g. `sqlite3.connect(":memory:")`) so tests run without touching
      disk; when none is injected the sink opens/owns the connection to `database`.
- [ ] `close()` commits any pending transaction and closes the owned connection (an injected
      connection is committed but not closed); idempotent.

### FR-004: StderrSink

#### Description:

The `StdoutSink` shape, but to stderr — matching the common convention of logs on stderr, app output
on stdout.

#### Acceptance Criteria:

- [ ] `StderrSink()` writes each event as one `json.dumps` line to `sys.stderr` and flushes.
- [ ] An injectable stream (default `sys.stderr`) lets tests capture output.
- [ ] `close()` flushes the stream.

### FR-005: NullSink

#### Description:

Discard everything — to disable output or benchmark the pipeline without a real sink.

#### Acceptance Criteria:

- [ ] `NullSink().emit(batch)` returns without writing anywhere and increments a `dropped` counter by
      `len(batch)`.
- [ ] `close()` is a no-op; `isinstance(NullSink(), Sink)` is `True`.

### FR-006: MemorySink

#### Description:

Collect events in memory for tests, notebooks, and introspection.

#### Acceptance Criteria:

- [ ] `MemorySink().emit(batch)` appends each event to an internal list accessible as `.events`,
      preserving order across multiple `emit` calls.
- [ ] With `maxlen` set, `.events` behaves as a bounded ring keeping only the most recent `maxlen`
      events.
- [ ] `close()` is a no-op; the collected events remain readable after `close()`.

---

## Data Model

```
# src/log_foundry/sinks/file.py
FileSink { path: str; encoding: str = "utf-8" }
RotatingFileSink {
  path: str
  max_bytes: int = 0          # 0 => no size trigger
  backup_count: int = 0
  when: str | None = None     # e.g. "H"/"D"; None => no time trigger
  interval: int = 1
}

# src/log_foundry/sinks/sqlite.py
SQLiteSink {
  database: str
  table: str = "log_events"
  connection: sqlite3.Connection | None = None   # injected for tests
  create_table: bool = True                      # False => caller owns the schema (no DDL)
}
# table columns: log_id, trace_id, span_id, timestamp, level, function, event (JSON text)

# src/log_foundry/sinks/util.py
StderrSink { stream: TextIO = sys.stderr }
NullSink   { dropped: int = 0 }
MemorySink { events: list[dict]; maxlen: int | None = None }
```

Events are the SPEC-001 `LogEvent` dicts; the SQLite columns are projections of those keys.

---

## API / Interface Contract

```python
# sinks/file.py
class FileSink:
    def __init__(self, path: str, *, encoding: str = "utf-8") -> None: ...
class RotatingFileSink:
    def __init__(self, path: str, *, max_bytes: int = 0, backup_count: int = 0,
                 when: str | None = None, interval: int = 1) -> None: ...

# sinks/sqlite.py
class SQLiteSink:
    def __init__(self, database: str, *, table: str = "log_events", connection=None,
                 create_table: bool = True) -> None: ...

# sinks/util.py
class StderrSink:
    def __init__(self, stream=None) -> None: ...
class NullSink: ...
class MemorySink:
    def __init__(self, maxlen: int | None = None) -> None: ...

# Usage
import log_foundry
from log_foundry.sinks.file import RotatingFileSink
log_foundry.configure(sink=RotatingFileSink("logs/app.ndjson", max_bytes=50_000_000, backup_count=5))
```

## Configuration / Environment

None. All sinks are standard-library only — no new config keys, env vars, or dependencies.

## File & Folder Structure

```
src/log_foundry/sinks/
├── file.py         # FileSink + RotatingFileSink                    (new)
├── sqlite.py       # SQLiteSink (schema-ensure + batch insert)      (new)
└── util.py         # StderrSink, NullSink, MemorySink               (new)
tests/
├── test_sinks_file.py     # append + size/time rotation + backup retention (new)
├── test_sinks_sqlite.py   # schema-ensure, batch insert, :memory: injection (new)
└── test_sinks_util.py     # stderr formatting, null drop-count, memory ring (new)
```

## Implementation Phases

### Phase 1: FileSink + RotatingFileSink

- Implement append-NDJSON `FileSink` and `RotatingFileSink` with size and time triggers plus numbered
  backup retention (FR-001, FR-002).
- Test append-not-truncate, size rotation across a threshold, time rotation, and backup pruning.

### Phase 2: SQLiteSink

- Implement idempotent schema-ensure, single-transaction batch insert with extracted columns, and
  injectable connection (FR-003).
- Test against an in-memory connection: schema creation, row projection, missing-key `NULL`s, commit
  on close.

### Phase 3: Utility sinks

- Implement `StderrSink`, `NullSink`, `MemorySink` (FR-004, FR-005, FR-006).
- Test stderr line formatting, null drop-counting, and memory collection with and without `maxlen`.
