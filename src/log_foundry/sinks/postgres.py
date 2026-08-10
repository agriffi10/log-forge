"""PostgresSink — insert events into a Postgres table with a JSONB column (arch §8, SPEC-011)."""

from __future__ import annotations

import json
import threading
from typing import Any

from log_foundry import _diag
from log_foundry.sinks._chunk import chunk_list, valid_identifier
from log_foundry.sinks._retry import wait
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

__all__ = ["PostgresSink"]

_BACKOFF_BASE = 0.1

_COLUMNS = ("timestamp", "level", "trace_id", "span_id", "function", "service")


class PostgresSink:
    """A :class:`~log_foundry.sinks.base.Sink` that batch-inserts events into a Postgres table.

    Each event is stored as a ``JSONB`` ``event`` column plus a few extracted columns for
    indexing. ``psycopg`` v3 is the optional ``postgres`` extra, imported lazily. The sink is
    write-only, and the worst-case delay (SPEC-027 FR-005) is ``max_retries`` interruptible waits
    per batch, 0.7 s at the defaults.

    The driver requirement satisfied (SPEC-028 FR-002): a ``psycopg`` connection carries one
    transaction, and this sink's unit of work is a ``cursor`` / ``commit`` / ``rollback`` sequence
    that assumes it owns it. Two unserialized emits share that transaction — one thread's
    ``commit`` publishes the other's half-written batch, and its ``rollback`` on a failure
    discards rows the other had already inserted and is about to report as delivered. A lock
    gives the sequence the exclusivity it was written for.
    """

    def __init__(
        self,
        table: str,
        *,
        connection: Any = None,
        dsn: str | None = None,
        create_table: bool = False,
        chunk_size: int = 1000,
        max_retries: int = 3,
    ) -> None:
        """Connects to the database and prepares the insert statement.

        Args:
          table: The target table, validated as a plain SQL identifier.
          connection: A ``psycopg``-shaped connection to borrow, or ``None`` to open one.
          dsn: The connection string used when opening a connection.
          create_table: An idempotent convenience, off by default because the user owns their
            schema and indexes.
          chunk_size: How many rows go in one driver statement.
          max_retries: Retries per batch, floored at zero as ``Worker._emit`` floors its own
            (SPEC-021) — a negative value returned having attempted no insert at all, and
            reported success.

        Returns:
          None.

        Raises:
          ValueError: If the table name is not a plain SQL identifier.
          ImportError: If the ``postgres`` extra is not installed.
        """
        self._table = valid_identifier(table)
        self._chunk_size = chunk_size
        self.max_retries = max(max_retries, 0)
        self.log_foundry_stop_signal: threading.Event | None = None
        self.failed = 0
        self._closed = False
        self._lock = threading.Lock()
        self._counter_lock = threading.Lock()
        self._owns_connection = connection is None
        if connection is None:
            import psycopg  # type: ignore[import-not-found]

            connection = psycopg.connect(dsn)
        self._conn = connection
        columns = ", ".join((*_COLUMNS, "event"))
        placeholders = ", ".join(["%s"] * len(_COLUMNS) + ["%s::jsonb"])
        self._insert_sql = f"INSERT INTO {self._table} ({columns}) VALUES ({placeholders})"
        if create_table:
            self._ensure_schema()

    def losses(self) -> SinkLosses:
        """Reports events abandoned past the retry bound (SPEC-026 FR-002).

        Reads under the counter lock rather than the emit lock (SPEC-028 FR-003), so a poll
        never waits on an in-flight insert and its backoff.

        Args:
          None.

        Returns:
          The counters.

        Raises:
          None.
        """
        with self._counter_lock:
            return SinkLosses(dropped=0, failed=self.failed)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Inserts the whole batch in one transaction, rolling back and retrying (FR-004).

        The diagnostic names the exception's type and never its repr: ``_row`` binds the whole
        serialized event as a statement parameter, and a psycopg error repr routinely reprints
        the failing statement and its parameters, so the old line reprinted the event, PII
        included, into a stream nobody was asked to secure (SPEC-029 FR-002, arch §6).

        Args:
          batch: The events to insert. An empty batch is a no-op.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When the retry bound is spent. One transaction, rolled back on
            failure, means such a batch inserted nothing, and the rollback is what makes the
            worker's retry safe here — there are no committed rows to duplicate (SPEC-026
            FR-001).
        """
        if not batch:
            return
        with self._lock:
            if self._closed:
                raise SinkDeliveryError(
                    f"PostgresSink inserted none of {len(batch)} event(s): the sink is closed"
                )
            self._insert_batch(batch)

    def _insert_batch(self, batch: list[dict[str, object]]) -> None:
        """Runs the transaction sequence, with the emit lock already held.

        Split out so the lock's extent is one line in :meth:`emit` rather than an extra
        indentation level over the whole retry loop.

        Args:
          batch: The events to insert, known non-empty.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When the retry bound is spent.
        """
        for attempt in range(self.max_retries + 1):
            try:
                with self._conn.cursor() as cur:
                    for chunk in chunk_list(batch, self._chunk_size):
                        cur.executemany(self._insert_sql, [self._row(event) for event in chunk])
                self._conn.commit()
                return
            except Exception as err:
                self._conn.rollback()
                if attempt < self.max_retries:
                    wait(_BACKOFF_BASE * (2**attempt), self.log_foundry_stop_signal)
                    continue
                with self._counter_lock:
                    self.failed += len(batch)
                _diag.lost(
                    "event",
                    len(batch),
                    f"PostgresSink, {self.max_retries + 1} attempts, {type(err).__name__}",
                )
                raise SinkDeliveryError(
                    f"PostgresSink inserted none of {len(batch)} event(s)"
                ) from None

    def close(self) -> None:
        """Commits pending work and closes only an owned connection (FR-005).

        Idempotent, and takes the emit lock so the final commit never lands in the middle of
        another thread's transaction (SPEC-028 FR-002).

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the driver raises on commit or close.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._conn.commit()
            if self._owns_connection:
                self._conn.close()

    def _row(self, event: dict[str, object]) -> tuple[object, ...]:
        """Builds one row: the extracted columns, then the whole event as JSON.

        Args:
          event: The event to convert.

        Returns:
          The statement parameters, in column order.

        Raises:
          TypeError: If the event is not JSON-serializable, which ``sanitize`` prevents.
        """
        return (*(event.get(col) for col in _COLUMNS), json.dumps(event))

    def _ensure_schema(self) -> None:
        """Idempotently creates the target table.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the driver raises on the DDL.
        """
        columns = ", ".join(f"{col} TEXT" for col in _COLUMNS)
        with self._conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {self._table} "
                f"(id BIGSERIAL PRIMARY KEY, {columns}, event JSONB NOT NULL)"
            )
        self._conn.commit()
