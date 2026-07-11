# Completed Spec — SPEC-008: Local File and Embedded Sinks

## What was completed?

A family of **zero-dependency** (stdlib-only) local sinks for deployments that keep logs on local
disk or in an embedded DB (local dev, debugging, air-gapped hosts, archival). All implement the
SPEC-001 `Sink` protocol, are `isinstance`-checkable, and operate purely on already-built event
dicts (arch §8); a single-process/worker-thread writer is assumed (arch §9).

- **`sinks.file`** (new) — `FileSink(path, *, encoding="utf-8")` appends one `json.dumps` line per
  event and flushes per emit (append-never-truncate; idempotent `close`); `RotatingFileSink(path, *,
  max_bytes=0, backup_count=0, when=None, interval=1)` rotates on size and/or time, keeping numbered
  backups (`path.1…path.N`) pruned to `backup_count`, rotating *before* a write would exceed the
  bound so no event is lost (FR-001, FR-002).
- **`sinks.sqlite`** (new) — `SQLiteSink(database, *, table="log_events", connection=None,
  create_table=True)` batch-inserts each event (full JSON + projected `log_id/trace_id/span_id/
  timestamp/level/function` columns) in one transaction; injectable connection for tests; owned
  connections are closed on `close`, injected ones only committed (FR-003).
- **`sinks.util`** (new) — `StderrSink(stream=None)` (subclass of `StdoutSink`, defaults to
  `sys.stderr`), `NullSink()` (discard + `dropped` counter), `MemorySink(maxlen=None)` (collect into
  `.events`, optional ring) (FR-004..FR-006).

**Deviation from the Draft:** FR-003 gained a **`create_table: bool = True`** flag (agreed
in-session). Default `True` keeps the batteries-included `CREATE TABLE IF NOT EXISTS`; `False` runs
no DDL so callers can own the schema via their own migrations (a missing/incompatible table then
surfaces as a normal `sqlite3` error at insert). Spec header/FR-003/data-model/API-contract updated.
Two hardening choices not in the Draft: owned SQLite connections open `check_same_thread=False` (the
worker thread is the sole writer, arch §9), and the `table` name is validated against a plain-SQL
identifier regex (SQLite can't parameterize identifiers).

## What changed from earlier specs?

Nothing — purely additive (three new modules + their tests). No change to the `Sink` protocol, the
worker, the batching contract, or any earlier module; no new runtime dependency or extra.

## Verification

Local gates green — `ruff check` clean, `mypy --strict` clean (21 src files), `pytest` **149 passed**
(36 new across `test_sinks_file.py` / `test_sinks_sqlite.py` / `test_sinks_util.py`, covering every
acceptance criterion incl. append-not-truncate, size + time rotation, backup pruning, `:memory:`
injection, missing-key `NULL`s, owned-vs-injected close, both `create_table` paths, stderr
formatting, null drop-count, memory ring). Also smoke-tested end-to-end through the real worker
thread (traced fn → background flush → `MultiSink(SQLiteSink, RotatingFileSink)` → `shutdown()`):
rows committed cross-thread, file rotated to the configured backup bound.
