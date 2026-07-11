"""SPEC-011 — ClickHouseSink: columnar projection, batch insert, create_table, retry (fake client)."""

from __future__ import annotations

import json
import sys
import types

import pytest

from log_forge.sinks.base import Sink
from log_forge.sinks.clickhouse import ClickHouseSink


class FakeClickHouse:
    def __init__(self, fail_times: int = 0) -> None:
        self.inserts: list[tuple] = []
        self.commands: list[str] = []
        self.closed = False
        self._fail_times = fail_times

    def insert(self, table, data=None, column_names=None) -> None:
        self.inserts.append((table, [list(r) for r in data], list(column_names)))
        if self._fail_times != 0:
            if self._fail_times > 0:
                self._fail_times -= 1
            raise RuntimeError("insert failed")

    def command(self, sql: str) -> None:
        self.commands.append(sql)

    def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr("log_forge.sinks.clickhouse.time.sleep", lambda _s: None)


def test_is_a_sink() -> None:
    assert isinstance(ClickHouseSink("log_events", client=FakeClickHouse()), Sink)


def test_row_projection_and_column_names() -> None:
    client = FakeClickHouse()
    event = {
        "timestamp": "t", "level": "ERROR", "trace_id": "tr", "span_id": "sp", "function": "fn",
        "service": "svc", "duration_ms": 12.5, "status": "error", "extra": 1,
    }
    ClickHouseSink("log_events", client=client).emit([event])
    table, data, columns = client.inserts[0]
    assert table == "log_events"
    assert columns == [
        "timestamp", "level", "trace_id", "span_id", "function", "service",
        "duration_ms", "status", "event",
    ]
    row = data[0]
    assert row[:8] == ["t", "ERROR", "tr", "sp", "fn", "svc", 12.5, "error"]
    assert json.loads(row[8])["extra"] == 1


def test_missing_keys_become_none() -> None:
    client = FakeClickHouse()
    ClickHouseSink("log_events", client=client).emit([{"level": "INFO"}])
    row = client.inserts[0][1][0]
    assert row[6] is None and row[7] is None  # duration_ms, status absent on this event


def test_batch_is_a_single_insert_call() -> None:
    client = FakeClickHouse()
    ClickHouseSink("log_events", client=client).emit([{"i": i} for i in range(3)])
    assert len(client.inserts) == 1
    assert len(client.inserts[0][1]) == 3


def test_large_batch_is_chunked() -> None:
    client = FakeClickHouse()
    ClickHouseSink("log_events", client=client, chunk_size=2).emit([{"i": i} for i in range(5)])
    assert [len(data) for _t, data, _c in client.inserts] == [2, 2, 1]


def test_create_table_runs_mergetree_ddl() -> None:
    client = FakeClickHouse()
    ClickHouseSink("log_events", client=client, create_table=True)
    assert client.commands
    assert "CREATE TABLE IF NOT EXISTS log_events" in client.commands[0]
    assert "MergeTree" in client.commands[0]


def test_insert_failure_retried_then_counted() -> None:
    client = FakeClickHouse(fail_times=-1)
    sink = ClickHouseSink("log_events", client=client, max_retries=1)
    sink.emit([{"a": 1}, {"a": 2}])
    assert len(client.inserts) == 2  # initial + 1 retry
    assert sink.failed == 2


def test_invalid_table_name_raises() -> None:
    with pytest.raises(ValueError):
        ClickHouseSink("log_events; DROP TABLE x", client=FakeClickHouse())


def test_injected_client_not_closed() -> None:
    client = FakeClickHouse()
    sink = ClickHouseSink("log_events", client=client)
    sink.close()
    assert client.closed is False
    sink.close()  # idempotent


def test_owned_client_is_closed(monkeypatch) -> None:
    client = FakeClickHouse()
    monkeypatch.setitem(
        sys.modules,
        "clickhouse_connect",
        types.SimpleNamespace(get_client=lambda dsn=None: client),
    )
    sink = ClickHouseSink("log_events", dsn="clickhouse://x")
    sink.close()
    assert client.closed is True
