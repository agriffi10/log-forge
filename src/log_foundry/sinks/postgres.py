"""PostgresSink — insert events into a Postgres table with a JSONB column (arch §8, SPEC-011).

Each event is stored as a ``JSONB`` ``event`` column plus a few extracted columns for indexing.
``psycopg`` (v3) is the optional ``postgres`` extra, imported lazily. The whole batch inserts in a
single transaction (chunked into driver-friendly statements); on failure the transaction is rolled
back and retried within a bounded count. Write-only. An optional idempotent ``create_table``
convenience is off by default — the user owns their schema and indexes.
"""

from __future__ import annotations

import json
import time
from typing import Any

from log_foundry import _diag
from log_foundry.sinks._chunk import chunk_list, valid_identifier
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

__all__ = ["PostgresSink"]

_BACKOFF_BASE = 0.1

# Columns extracted from each event for indexing; ``event`` (full JSONB) is stored alongside.
_COLUMNS = ("timestamp", "level", "trace_id", "span_id", "function", "service")


class PostgresSink:
    """A :class:`~log_foundry.sinks.base.Sink` that batch-inserts events into a Postgres table."""

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
        self._table = valid_identifier(table)
        self._chunk_size = chunk_size
        # Floored as ``Worker._emit`` floors its own (SPEC-021): a negative value returned
        # having attempted no insert at all, and reported success.
        self.max_retries = max(max_retries, 0)
        self.failed = 0
        self._closed = False
        self._owns_connection = connection is None
        if connection is None:
            import psycopg  # type: ignore[import-not-found]  # optional 'postgres' extra

            connection = psycopg.connect(dsn)
        self._conn = connection
        columns = ", ".join((*_COLUMNS, "event"))
        placeholders = ", ".join(["%s"] * len(_COLUMNS) + ["%s::jsonb"])
        self._insert_sql = f"INSERT INTO {self._table} ({columns}) VALUES ({placeholders})"
        if create_table:
            self._ensure_schema()

    def losses(self) -> SinkLosses:
        """Events abandoned past the retry bound (SPEC-026 FR-002). Never raises."""
        return SinkLosses(dropped=0, failed=self.failed)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Insert the whole batch in one transaction, rolling back and retrying on error (FR-004).

        One transaction, rolled back on failure, so a batch past the retry bound inserted
        *nothing*: it is counted and then raised (SPEC-026 FR-001). The rollback is what makes
        the worker's retry safe here — there are no committed rows to duplicate.
        """
        if not batch:
            return
        for attempt in range(self.max_retries + 1):
            try:
                with self._conn.cursor() as cur:
                    for chunk in chunk_list(batch, self._chunk_size):
                        cur.executemany(self._insert_sql, [self._row(event) for event in chunk])
                self._conn.commit()
                return
            except Exception as err:  # isolation boundary: never crash the worker (FR-006)
                self._conn.rollback()
                if attempt < self.max_retries:
                    time.sleep(_BACKOFF_BASE * (2**attempt))
                    continue
                self.failed += len(batch)
                # The type, never the repr. ``_row`` binds the whole ``json.dumps(event)`` as a
                # statement parameter, and a psycopg error repr routinely reprints the failing
                # statement *and* its parameters — so the old line reprinted the event, PII
                # included, into a stream nobody was asked to secure (SPEC-029 FR-002, arch §6).
                _diag.lost(
                    "event",
                    len(batch),
                    f"PostgresSink, {self.max_retries + 1} attempts, {type(err).__name__}",
                )
                raise SinkDeliveryError(
                    f"PostgresSink inserted none of {len(batch)} event(s)"
                ) from None

    def close(self) -> None:
        """Commit pending work; close only an owned connection; idempotent (FR-005)."""
        if self._closed:
            return
        self._conn.commit()
        if self._owns_connection:
            self._conn.close()
        self._closed = True

    # -- internals ----------------------------------------------------------------------

    def _row(self, event: dict[str, object]) -> tuple[object, ...]:
        return (*(event.get(col) for col in _COLUMNS), json.dumps(event))

    def _ensure_schema(self) -> None:
        columns = ", ".join(f"{col} TEXT" for col in _COLUMNS)
        with self._conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {self._table} "
                f"(id BIGSERIAL PRIMARY KEY, {columns}, event JSONB NOT NULL)"
            )
        self._conn.commit()
