"""ClickHouseSink — batch-insert events into a ClickHouse table (arch §8, SPEC-011).

ClickHouse is the columnar, observability-scale favorite. ``clickhouse-connect`` is the optional
``clickhouse`` extra, imported lazily. Each event maps to a row of extracted typed columns plus the
full event as a ``String`` column, inserted in a single ``insert`` call per chunk. An optional
idempotent ``MergeTree`` ``create_table`` convenience is off by default. Write-only.
"""

from __future__ import annotations

import json
import sys
import time
from typing import Any

from log_forge.sinks._chunk import chunk_list, valid_identifier

__all__ = ["ClickHouseSink"]

_BACKOFF_BASE = 0.1

# Extracted columns (typed in the MergeTree schema); ``event`` (full JSON String) is stored too.
_COLUMNS = (
    "timestamp", "level", "trace_id", "span_id", "function", "service", "duration_ms", "status"
)
_COLUMN_NAMES = [*_COLUMNS, "event"]

# Per-column DDL types for the optional create_table convenience (all nullable to tolerate missing
# keys — e.g. span-start events have no duration_ms/status).
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
    """A :class:`~log_forge.sinks.base.Sink` that batch-inserts events into a ClickHouse table."""

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
        self._table = valid_identifier(table)
        self._chunk_size = chunk_size
        self.max_retries = max_retries
        self.failed = 0
        self._closed = False
        self._owns_client = client is None
        if client is None:
            import clickhouse_connect  # type: ignore[import-not-found]  # 'clickhouse' extra

            client = clickhouse_connect.get_client(dsn=dsn)
        self.client = client
        if create_table:
            self._ensure_schema()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Insert each chunk as one columnar ``insert`` call, retrying on failure (FR-002)."""
        if not batch:
            return
        for chunk in chunk_list(batch, self._chunk_size):
            rows = [self._row(event) for event in chunk]
            self._insert(rows)

    def close(self) -> None:
        """Close the client only if the sink owns it; idempotent (FR-005)."""
        if self._closed:
            return
        if self._owns_client:
            self.client.close()
        self._closed = True

    # -- internals ----------------------------------------------------------------------

    def _row(self, event: dict[str, object]) -> list[object]:
        return [*(event.get(col) for col in _COLUMNS), json.dumps(event)]

    def _insert(self, rows: list[list[object]]) -> None:
        for attempt in range(self.max_retries + 1):
            try:
                self.client.insert(self._table, data=rows, column_names=_COLUMN_NAMES)
                return
            except Exception as err:  # isolation boundary: never crash the worker (FR-006)
                if attempt < self.max_retries:
                    time.sleep(_BACKOFF_BASE * (2**attempt))
                    continue
                self.failed += len(rows)
                sys.stderr.write(
                    f"log-forge: ClickHouseSink abandoned {len(rows)} row(s) after "
                    f"{self.max_retries + 1} attempts ({err!r})\n"
                )
                return

    def _ensure_schema(self) -> None:
        columns = ", ".join(f"{col} {_COLUMN_TYPES[col]}" for col in _COLUMNS)
        self.client.command(
            f"CREATE TABLE IF NOT EXISTS {self._table} "
            f"({columns}, event String) ENGINE = MergeTree ORDER BY tuple()"
        )
