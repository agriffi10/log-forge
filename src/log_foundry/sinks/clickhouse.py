"""ClickHouseSink — batch-insert events into a ClickHouse table (arch §8, SPEC-011).

ClickHouse is the columnar, observability-scale favorite. ``clickhouse-connect`` is the optional
``clickhouse`` extra, imported lazily. Each event maps to a row of extracted typed columns plus the
full event as a ``String`` column, inserted in a single ``insert`` call per chunk. An optional
idempotent ``MergeTree`` ``create_table`` convenience is off by default. Write-only.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import threading

from log_foundry import _diag
from log_foundry.sinks._chunk import chunk_list, valid_identifier
from log_foundry.sinks._retry import wait
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

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
    """A :class:`~log_foundry.sinks.base.Sink` that batch-inserts events into a ClickHouse table."""

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
        # Floored as ``Worker._emit`` floors its own (SPEC-021): a negative value returned
        # from ``_insert`` having attempted nothing, and reported success.
        self.max_retries = max(max_retries, 0)
        # Set by the worker when this sink is the configured one (SPEC-027 FR-002).
        self.stop_signal: threading.Event | None = None
        self.failed = 0
        self._closed = False
        self._owns_client = client is None
        if client is None:
            import clickhouse_connect  # type: ignore[import-not-found]  # 'clickhouse' extra

            client = clickhouse_connect.get_client(dsn=dsn)
        self.client = client
        if create_table:
            self._ensure_schema()

    def losses(self) -> SinkLosses:
        """Rows in a chunk abandoned past the retry bound (SPEC-026 FR-002). Never raises."""
        return SinkLosses(dropped=0, failed=self.failed)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Insert each chunk as one columnar ``insert`` call, retrying on failure (FR-002).

        Raises when every chunk failed (SPEC-026 FR-001) — which is the whole batch for any
        batch that fits one chunk, the ordinary case. A partially-inserted batch does not raise:
        the chunks that landed are committed, and the worker's retry would duplicate them.
        """
        if not batch:
            return
        chunks = inserted = 0
        for chunk in chunk_list(batch, self._chunk_size):
            chunks += 1
            inserted += self._insert([self._row(event) for event in chunk])
        if chunks and not inserted:
            raise SinkDeliveryError(f"ClickHouseSink inserted none of {chunks} chunk(s)")

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

    def _insert(self, rows: list[list[object]]) -> int:
        """Insert one chunk within the retry bound; ``1`` if it landed, ``0`` once abandoned."""
        for attempt in range(self.max_retries + 1):
            try:
                self.client.insert(self._table, data=rows, column_names=_COLUMN_NAMES)
                return 1
            except Exception as err:  # isolation boundary: never crash the worker (FR-006)
                if attempt < self.max_retries:
                    wait(_BACKOFF_BASE * (2**attempt), self.stop_signal)
                    continue
                self.failed += len(rows)
                _diag.lost(
                    "row",
                    len(rows),
                    f"ClickHouseSink, {self.max_retries + 1} attempts, {type(err).__name__}",
                )
                return 0
        return 0  # unreachable: the loop returns on every path (mypy needs the exit)

    def _ensure_schema(self) -> None:
        columns = ", ".join(f"{col} {_COLUMN_TYPES[col]}" for col in _COLUMNS)
        self.client.command(
            f"CREATE TABLE IF NOT EXISTS {self._table} "
            f"({columns}, event String) ENGINE = MergeTree ORDER BY tuple()"
        )
