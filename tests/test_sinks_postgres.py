"""SPEC-011 — PostgresSink: transactional chunked insert, projection, rollback/retry (fake conn)."""

from __future__ import annotations

import json
import sys
import types
from typing import Self

import pytest

from log_foundry.sinks.base import Sink
from log_foundry.sinks.postgres import PostgresSink


class FakeCursor:
    def __init__(self, owner: FakeConnection) -> None:
        self._owner = owner

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def execute(self, sql, params=None) -> None:
        self._owner.executed.append((sql, params))

    def executemany(self, sql, rows) -> None:
        self._owner.executemany_calls.append((sql, [tuple(r) for r in rows]))
        if self._owner._fail_times != 0:
            if self._owner._fail_times > 0:
                self._owner._fail_times -= 1
            raise RuntimeError("insert failed")


class FakeConnection:
    def __init__(self, fail_times: int = 0) -> None:
        self.executed: list[tuple] = []
        self.executemany_calls: list[tuple] = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self._fail_times = fail_times

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr("log_foundry.sinks.postgres.time.sleep", lambda _s: None)


def test_is_a_sink() -> None:
    assert isinstance(PostgresSink("logs", connection=FakeConnection()), Sink)


def test_batch_insert_projects_columns_and_commits() -> None:
    conn = FakeConnection()
    PostgresSink("logs", connection=conn).emit(
        [
            {"timestamp": "t", "level": "INFO", "trace_id": "tr", "span_id": "sp",
             "function": "fn", "service": "svc", "extra": 1}
        ]
    )
    sql, rows = conn.executemany_calls[0]
    assert "INSERT INTO logs" in sql
    assert "%s::jsonb" in sql
    row = rows[0]
    assert row[:6] == ("t", "INFO", "tr", "sp", "fn", "svc")
    assert json.loads(row[6])["extra"] == 1
    assert conn.commits == 1


def test_missing_keys_become_none() -> None:
    conn = FakeConnection()
    PostgresSink("logs", connection=conn).emit([{"level": "INFO"}])
    row = conn.executemany_calls[0][1][0]
    assert row[:6] == (None, "INFO", None, None, None, None)


def test_chunks_within_one_transaction() -> None:
    conn = FakeConnection()
    PostgresSink("logs", connection=conn, chunk_size=2).emit([{"i": i} for i in range(5)])
    assert [len(rows) for _sql, rows in conn.executemany_calls] == [2, 2, 1]
    assert conn.commits == 1  # a single transaction spans all chunks


def test_rollback_and_retry_then_succeed() -> None:
    conn = FakeConnection(fail_times=1)
    sink = PostgresSink("logs", connection=conn, max_retries=2)
    sink.emit([{"a": 1}])
    assert conn.rollbacks == 1
    assert conn.commits == 1
    assert sink.failed == 0


def test_persistent_failure_counted() -> None:
    conn = FakeConnection(fail_times=-1)
    sink = PostgresSink("logs", connection=conn, max_retries=1)
    sink.emit([{"a": 1}, {"a": 2}])
    assert conn.rollbacks == 2
    assert conn.commits == 0
    assert sink.failed == 2


def test_create_table_runs_ddl() -> None:
    conn = FakeConnection()
    PostgresSink("logs", connection=conn, create_table=True)
    assert any("CREATE TABLE IF NOT EXISTS logs" in sql for sql, _ in conn.executed)


def test_invalid_table_name_raises() -> None:
    with pytest.raises(ValueError):
        PostgresSink("logs; DROP TABLE x", connection=FakeConnection())


def test_close_commits_injected_but_does_not_close() -> None:
    conn = FakeConnection()
    sink = PostgresSink("logs", connection=conn)
    sink.close()
    assert conn.commits == 1
    assert conn.closed is False
    sink.close()  # idempotent


def test_owned_connection_is_closed(monkeypatch) -> None:
    conn = FakeConnection()
    monkeypatch.setitem(sys.modules, "psycopg", types.SimpleNamespace(connect=lambda dsn: conn))
    sink = PostgresSink("logs", dsn="postgresql://x")
    sink.close()
    assert conn.closed is True
