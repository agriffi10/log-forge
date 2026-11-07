"""SQLiteSink — persist events as queryable rows in embedded SQLite (arch §8, SPEC-008)."""

from __future__ import annotations

import json
import sqlite3
import threading

from log_foundry import _diag
from log_foundry.sinks._chunk import valid_identifier
from log_foundry.sinks._deferral import Deferral
from log_foundry.sinks._driver_error import delivery_error
from log_foundry.sinks._encode import encode_events
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

__all__ = ["SQLiteSink"]

_COLUMNS = ("log_id", "trace_id", "span_id", "timestamp", "level", "function")


class SQLiteSink:
    """A :class:`~log_foundry.sinks.base.Sink` that batch-inserts events into a SQLite table.

    For local dev, debugging, air-gapped hosts or simple archival, an embedded SQLite file is a
    durable sink you can open later and query with plain SQL. Each event is stored as its full
    JSON — the source of truth — plus a few columns projected out for cheap filtering, which are
    ``NULL`` when absent. Standard library only.

    The driver requirement satisfied (SPEC-028 FR-002): ``check_same_thread=False`` switches off
    ``sqlite3``'s own same-thread guard, and ``with connection`` is a transaction scope on the
    shared connection rather than a per-caller one — two threads inside it at once share a single
    implicit transaction, so one thread's rollback discards rows the other had already inserted.
    A lock restores the one-writer-at-a-time the guard used to enforce. Cross-*process* writers
    to one database file remain out of scope.


    It keeps **no** client buffer (SPEC-036 FR-002): ``emit`` commits its own transaction, so
    nothing is uncommitted once it returns. The commit in ``close`` is belt and braces, not a
    buffer.
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
        different thread from the one that ran ``configure()``, so SQLite's same-thread guard
        would reject every insert. It is not the sole writer — the orphan path emits on the
        caller's thread (SPEC-028) — which is why the lock below replaces what the guard gave up.

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
        self._lock = threading.Lock()
        self._unencodable = 0
        self._failed = 0
        self._counter_lock = threading.Lock()
        if create_table:
            self._ensure_schema()

    def losses(self) -> SinkLosses | None:
        """Reports unencodable events and driver failures, once it has any (SPEC-069).

        ``dropped`` is the events :meth:`emit` could not encode (FR-001); ``failed`` the events of
        a batch whose insert raised ``sqlite3.Error`` (FR-004), an upper bound. ``None`` until
        something is counted, the rule ``wrapped_losses`` applies. Read under the counter lock,
        never the emit lock, so a poll does not wait behind an insert (SPEC-028 FR-003).

        Args:
          None.

        Returns:
          ``None`` while nothing is counted, else the counters.

        Raises:
          None.
        """
        with self._counter_lock:
            if not (self._unencodable or self._failed):
                return None
            return SinkLosses(dropped=self._unencodable, failed=self._failed)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Inserts every event in one transaction (FR-003).

        ``with connection`` opens a transaction and commits on success or rolls back on error,
        so the whole batch lands atomically. The lock holds that transaction to one thread
        (SPEC-028 FR-002). Row-building is deliberately *outside* it: those lines touch nothing
        shared, so serializing the ``json.dumps`` of a whole batch would lengthen the critical
        section for no gain.

        Row-building runs through the encode helper (SPEC-069 FR-001), so an event whose row
        cannot be built costs only itself, counted and announced when this call returns, rather
        than the whole batch; a batch of nothing encodable runs no transaction and returns, once
        the closed check has passed (FR-002).

        **A driver error is counted, announced and raised as both types** (SPEC-069 FR-004).
        When the insert raises ``sqlite3.Error`` the batch size is added to ``failed`` at once,
        under the counter lock and never the emit lock, one line names the class and the
        exception's type, and the error is raised as an instance of both ``SinkDeliveryError``
        and the driver's own class (``_driver_error.delivery_error``), so ``except
        sqlite3.Error`` and ``except SinkDeliveryError`` both still catch it. Before, it
        escaped with nothing counted and nothing written. An event this call dropped as
        unencodable is discarded with the raise, like any other per-event drop.

        Args:
          batch: The events to insert.

        Returns:
          None.

        Raises:
          sqlite3.Error: If the insert fails, as an instance of ``SinkDeliveryError`` too, with
            the original as its ``__cause__`` — or the original itself, unchanged, if the
            dual-typed exception cannot be built.
          SinkDeliveryError: If the sink is closed — whether the close landed while this call
            was building its rows, or long before it. Either way the batch would otherwise reach
            the driver as "Cannot operate on a closed database", and the library has a word of
            its own for "none of this was delivered" (SPEC-026 FR-001). What the library should
            *signal* about logging after a completed shutdown is a separate question, and
            SPEC-030's; this only keeps driver internals out of the answer.
        """
        if not batch:
            return
        placeholders = ", ".join("?" * (len(_COLUMNS) + 1))
        columns = ", ".join((*_COLUMNS, "event"))
        with Deferral(self) as pending:
            encoded = encode_events(batch, self._row, pending, sink=type(self).__name__)
            try:
                with self._lock:
                    if self._closed:
                        raise SinkDeliveryError(
                            f"SQLiteSink inserted none of {len(batch)} event(s): the sink is "
                            f"closed"
                        )
                    if not encoded:
                        return
                    self._insert([row for _, row in encoded], columns, placeholders)
            except sqlite3.Error as err:
                with self._counter_lock:
                    self._failed += len(batch)
                _diag.lost("event", len(batch), f"{type(self).__name__}, {type(err).__name__}")
                built = delivery_error(err)
                if built is None:
                    raise
                raise built from err

    def _row(self, event: dict[str, object]) -> tuple[object, ...]:
        """Builds one row: the projected columns, then the whole event as JSON.

        Args:
          event: The event to convert.

        Returns:
          The statement parameters, in column order.

        Raises:
          TypeError: If the event is not JSON-serializable, which ``encode_events`` turns into a
            drop of this one event. Neither source of events produces one: assembly coerces every
            value (``sanitize``), and a ``TransformSink`` checks and coerces what its function
            returns (SPEC-058 FR-001).
        """
        return (*(event.get(col) for col in _COLUMNS), json.dumps(event))

    def _insert(
        self, rows: list[tuple[object, ...]], columns: str, placeholders: str
    ) -> None:
        """Runs the insert transaction, with the emit lock already held.

        Args:
          rows: The prepared row tuples.
          columns: The comma-separated column list.
          placeholders: The matching parameter placeholders.

        Returns:
          None.

        Raises:
          sqlite3.Error: If the insert fails.
        """
        with self._conn:
            self._conn.executemany(
                f'INSERT INTO "{self._table}" ({columns}) VALUES ({placeholders})', rows
            )

    def close(self) -> None:
        """Commits pending work and closes only a connection the sink owns (FR-003).

        Idempotent, and takes the emit lock so it never closes the connection out from under an
        in-flight transaction (SPEC-028 FR-002).

        Args:
          None.

        Returns:
          None.

        Raises:
          sqlite3.Error: If the commit or close fails.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._conn.commit()
            if self._owns_connection:
                self._conn.close()

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
