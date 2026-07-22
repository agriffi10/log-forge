"""SQLiteSink — persist events as queryable rows in an embedded SQLite database (arch §8, SPEC-008).

For local dev, debugging, air-gapped hosts, or simple archival, an embedded SQLite file is a durable
sink you can open later and query with plain SQL. Each event is stored as its full JSON plus a few
extracted columns (identity + level + function) projected out for cheap filtering. Like every sink it
receives *already-built* event dicts and knows nothing about spans or context.

Standard library only (``sqlite3``) — no runtime dependency. A single-process, single-worker-thread
writer is assumed (arch §9); concurrent/cross-process writers and external databases (Postgres,
ClickHouse, …) are out of scope here (the latter is SPEC-011).
"""

from __future__ import annotations

import json
import re
import sqlite3

__all__ = ["SQLiteSink"]

# Columns projected out of each event for cheap SQL filtering. ``event`` (full JSON) is stored
# alongside these and is the source of truth; these are convenience projections (NULL if absent).
_COLUMNS = ("log_id", "trace_id", "span_id", "timestamp", "level", "function")

# A table name is a config value, not untrusted input, but it is interpolated into DDL/DML (SQLite
# cannot parameterize identifiers), so restrict it to a plain SQL identifier to foreclose injection.
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SQLiteSink:
    """A :class:`~log_foundry.sinks.base.Sink` that batch-inserts events into a SQLite table.

    By default (``create_table=True``) the sink owns its schema: it runs an idempotent
    ``CREATE TABLE IF NOT EXISTS`` so it works out of the box against a fresh database file. Pass
    ``create_table=False`` when the caller provisions the table themselves (e.g. via migrations); the
    sink then runs no DDL and a missing/incompatible table surfaces as a normal ``sqlite3`` error at
    insert time.

    A ``connection`` may be injected (e.g. ``sqlite3.connect(":memory:")``) so tests never touch
    disk. An injected connection is borrowed — committed but never closed by this sink; a connection
    the sink opens itself is owned and closed on ``close()``.
    """

    def __init__(
        self,
        database: str,
        *,
        table: str = "log_events",
        connection: sqlite3.Connection | None = None,
        create_table: bool = True,
    ) -> None:
        if not _IDENTIFIER.match(table):
            raise ValueError(f"invalid table name {table!r}; expected a plain SQL identifier")
        self._table = table
        self._owns_connection = connection is None
        # check_same_thread=False: the background worker (a different thread than the one that ran
        # configure()) is the sole writer, so SQLite's same-thread guard would only get in the way
        # (arch §9 single-writer assumption; concurrent writers are out of scope, SPEC-008).
        self._conn = (
            connection
            if connection is not None
            else sqlite3.connect(database, check_same_thread=False)
        )
        self._closed = False
        if create_table:
            self._ensure_schema()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Insert every event in one transaction; extracted columns default to NULL (FR-003)."""
        rows = [
            (*(event.get(col) for col in _COLUMNS), json.dumps(event)) for event in batch
        ]
        placeholders = ", ".join("?" * (len(_COLUMNS) + 1))
        columns = ", ".join((*_COLUMNS, "event"))
        # ``with connection`` opens a transaction and commits on success / rolls back on error, so
        # the whole batch lands atomically.
        with self._conn:
            self._conn.executemany(
                f'INSERT INTO "{self._table}" ({columns}) VALUES ({placeholders})', rows
            )

    def close(self) -> None:
        """Commit pending work; close only a connection the sink owns. Idempotent (FR-003)."""
        if self._closed:
            return
        self._conn.commit()
        if self._owns_connection:
            self._conn.close()
        self._closed = True

    # -- internals ----------------------------------------------------------------------

    def _ensure_schema(self) -> None:
        """Idempotently create the target table (``CREATE TABLE IF NOT EXISTS``)."""
        columns = ", ".join(f"{col} TEXT" for col in _COLUMNS)
        with self._conn:
            self._conn.execute(
                f'CREATE TABLE IF NOT EXISTS "{self._table}" '
                f"(id INTEGER PRIMARY KEY AUTOINCREMENT, {columns}, event TEXT NOT NULL)"
            )
