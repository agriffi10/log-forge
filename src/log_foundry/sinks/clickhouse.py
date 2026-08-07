"""ClickHouseSink — batch-insert events into a ClickHouse table (arch §8, SPEC-011)."""

from __future__ import annotations

import json
import threading
from typing import Any

from log_foundry import _diag
from log_foundry.sinks._chunk import chunk_list, valid_identifier
from log_foundry.sinks._retry import wait
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

__all__ = ["ClickHouseSink"]

_BACKOFF_BASE = 0.1

_COLUMNS = (
    "timestamp", "level", "trace_id", "span_id", "function", "service", "duration_ms", "status"
)
_COLUMN_NAMES = [*_COLUMNS, "event"]

_COLUMN_TYPES = {
    "timestamp": "Nullable(String)",
    "level": "Nullable(String)",
    "trace_id": "Nullable(String)",
    "span_id": "Nullable(String)",
    "function": "Nullable(String)",
    "service": "Nullable(String)",
    "duration_ms": "Nullable(Float64)",
    "status": "Nullable(String)",
}


class ClickHouseSink:
    """A :class:`~log_foundry.sinks.base.Sink` that batch-inserts events into ClickHouse.

    ClickHouse is the columnar, observability-scale favorite. ``clickhouse-connect`` is the
    optional ``clickhouse`` extra, imported lazily. Each event maps to a row of extracted typed
    columns plus the full event as a ``String`` column, inserted in a single call per chunk. The
    sink is write-only, and the worst-case delay (SPEC-027 FR-005) is ``max_retries``
    interruptible waits per chunk, 0.7 s at the defaults.

    The driver requirement satisfied (SPEC-028 FR-002): a ``clickhouse-connect`` client holds
    per-session state across an insert and the project does not publish it as safe to share
    between threads, so this sink serializes its use rather than assuming otherwise. A lock is
    the conservative reading — one client per thread would be the alternative, and that is the
    connection-pool design FR-002 puts out of scope.
    """

    def __init__(
        self,
        table: str,
        *,
        client: Any = None,
        dsn: str | None = None,
        create_table: bool = False,
        chunk_size: int = 1000,
        max_retries: int = 3,
    ) -> None:
        """Connects to the server and, optionally, provisions the schema.

        Args:
          table: The target table, validated as a plain SQL identifier.
          client: A ``clickhouse-connect``-shaped client to borrow, or ``None`` to open one.
          dsn: The connection string used when opening a client.
          create_table: An idempotent ``MergeTree`` convenience, off by default. Every extracted
            column is nullable, to tolerate missing keys — a span-start event has no
            ``duration_ms`` or ``status``.
          chunk_size: How many rows go in one insert call.
          max_retries: Retries per chunk, floored at zero as ``Worker._emit`` floors its own
            (SPEC-021) — a negative value returned from ``_insert`` having attempted nothing, and
            reported success.

        Returns:
          None.

        Raises:
          ValueError: If the table name is not a plain SQL identifier.
          ImportError: If the ``clickhouse`` extra is not installed.
        """
        self._table = valid_identifier(table)
        self._chunk_size = chunk_size
        self.max_retries = max(max_retries, 0)
        self.stop_signal: threading.Event | None = None
        self.failed = 0
        self._closed = False
        self._lock = threading.Lock()
        self._counter_lock = threading.Lock()
        self._owns_client = client is None
        if client is None:
            import clickhouse_connect  # type: ignore[import-not-found]

            client = clickhouse_connect.get_client(dsn=dsn)
        self.client = client
        if create_table:
            self._ensure_schema()

    def losses(self) -> SinkLosses:
        """Reports rows in a chunk abandoned past the retry bound (SPEC-026 FR-002).

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
        """Inserts each chunk as one columnar call, retrying on failure (FR-002).

        Args:
          batch: The events to insert. An empty batch is a no-op.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When every chunk failed (SPEC-026 FR-001), which is the whole batch
            for any batch that fits one chunk, the ordinary case. A partially-inserted batch does
            not raise: the chunks that landed are committed, and the worker's retry would
            duplicate them.
        """
        if not batch:
            return
        chunks = inserted = 0
        with self._lock:
            for chunk in chunk_list(batch, self._chunk_size):
                chunks += 1
                inserted += self._insert([self._row(event) for event in chunk])
        if chunks and not inserted:
            raise SinkDeliveryError(f"ClickHouseSink inserted none of {chunks} chunk(s)")

    def close(self) -> None:
        """Closes the client only if the sink owns it (FR-005).

        Idempotent, and takes the emit lock so the client is never closed mid-insert
        (SPEC-028 FR-002).

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the client raises on close.
        """
        with self._lock:
            if self._closed:
                return
            if self._owns_client:
                self.client.close()
            self._closed = True

    def _row(self, event: dict[str, object]) -> list[object]:
        """Builds one row: the extracted columns, then the whole event as JSON.

        Args:
          event: The event to convert.

        Returns:
          The row values, in column order.

        Raises:
          TypeError: If the event is not JSON-serializable, which ``sanitize`` prevents.
        """
        return [*(event.get(col) for col in _COLUMNS), json.dumps(event)]

    def _insert(self, rows: list[list[object]]) -> int:
        """Inserts one chunk within the retry bound (FR-006).

        Args:
          rows: The chunk's rows.

        Returns:
          1 when the chunk landed, 0 once it is abandoned.

        Raises:
          None. This is an isolation boundary: a driver fault must never crash the worker.
        """
        for attempt in range(self.max_retries + 1):
            try:
                self.client.insert(self._table, data=rows, column_names=_COLUMN_NAMES)
                return 1
            except Exception as err:
                if attempt < self.max_retries:
                    wait(_BACKOFF_BASE * (2**attempt), self.stop_signal)
                    continue
                with self._counter_lock:
                    self.failed += len(rows)
                _diag.lost(
                    "row",
                    len(rows),
                    f"ClickHouseSink, {self.max_retries + 1} attempts, {type(err).__name__}",
                )
                return 0
        return 0

    def _ensure_schema(self) -> None:
        """Idempotently creates the target ``MergeTree`` table.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the client raises on the DDL.
        """
        columns = ", ".join(f"{col} {_COLUMN_TYPES[col]}" for col in _COLUMNS)
        self.client.command(
            f"CREATE TABLE IF NOT EXISTS {self._table} "
            f"({columns}, event String) ENGINE = MergeTree ORDER BY tuple()"
        )
