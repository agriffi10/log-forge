"""SQLiteSink — persist events as queryable rows in embedded SQLite (arch §8, SPEC-008)."""

from __future__ import annotations

import json
import sqlite3

from log_foundry.sinks._chunk import valid_identifier

__all__ = ["SQLiteSink"]

_COLUMNS = ("log_id", "trace_id", "span_id", "timestamp", "level", "function")


class SQLiteSink:
    """A :class:`~log_foundry.sinks.base.Sink` that batch-inserts events into a SQLite table.

    For local dev, debugging, air-gapped hosts or simple archival, an embedded SQLite file is a
    durable sink you can open later and query with plain SQL. Each event is stored as its full
    JSON — the source of truth — plus a few columns projected out for cheap filtering, which are
    ``NULL`` when absent. Standard library only, and a single-process, single-worker-thread
    writer is assumed (arch §9).
    """

    def __init__(
        self,
        database: str,
        *,
        table: str = "log_events",
        connection: sqlite3.Connection | None = None,
        create_table: bool = True,
    ) -> None:
        """Connects to the database and, by default, provisions the schema.

        The connection is opened with ``check_same_thread=False``: the background worker is a
        different thread from the one that ran ``configure()`` and is the sole writer, so
        SQLite's same-thread guard would only get in the way.

        Args:
          database: The database file to open, ignored when a connection is injected.
          table: The target table, validated as a plain SQL identifier.
          connection: A connection to borrow, such as an in-memory one for tests. A borrowed
            connection is committed but never closed by this sink, while one the sink opens
            itself is owned and closed on :meth:`close`.
          create_table: Whether the sink owns its schema. Pass ``False`` when the caller
            provisions the table via migrations; the sink then runs no DDL and a missing or
            incompatible table surfaces as a normal ``sqlite3`` error at insert time.

        Returns:
          None.

        Raises:
          ValueError: If the table name is not a plain SQL identifier.
          sqlite3.Error: If the database cannot be opened or the schema cannot be created.
        """
        self._table = valid_identifier(table)
        self._owns_connection = connection is None
        self._conn = (
            connection
            if connection is not None
            else sqlite3.connect(database, check_same_thread=False)
        )
        self._closed = False
        if create_table:
            self._ensure_schema()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Inserts every event in one transaction (FR-003).

        ``with connection`` opens a transaction and commits on success or rolls back on error,
        so the whole batch lands atomically.

        Args:
          batch: The events to insert.

        Returns:
          None.

        Raises:
          sqlite3.Error: If the insert fails.
        """
        rows = [
            (*(event.get(col) for col in _COLUMNS), json.dumps(event)) for event in batch
        ]
        placeholders = ", ".join("?" * (len(_COLUMNS) + 1))
        columns = ", ".join((*_COLUMNS, "event"))
        with self._conn:
            self._conn.executemany(
                f'INSERT INTO "{self._table}" ({columns}) VALUES ({placeholders})', rows
            )

    def close(self) -> None:
        """Commits pending work and closes only a connection the sink owns (FR-003).

        Idempotent.

        Args:
          None.

        Returns:
          None.

        Raises:
          sqlite3.Error: If the commit or close fails.
        """
        if self._closed:
            return
        self._conn.commit()
        if self._owns_connection:
            self._conn.close()
        self._closed = True

    def _ensure_schema(self) -> None:
        """Idempotently creates the target table.

        Args:
          None.

        Returns:
          None.

        Raises:
          sqlite3.Error: If the DDL fails.
        """
        columns = ", ".join(f"{col} TEXT" for col in _COLUMNS)
        with self._conn:
            self._conn.execute(
                f'CREATE TABLE IF NOT EXISTS "{self._table}" '
                f"(id INTEGER PRIMARY KEY AUTOINCREMENT, {columns}, event TEXT NOT NULL)"
            )
