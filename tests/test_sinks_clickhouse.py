"""SPEC-011 — ClickHouseSink: columnar projection, batch insert, create_table, retry (fake client)."""

from __future__ import annotations

import json
import sys
import types

import pytest

from log_foundry.sinks.base import Sink, SinkDeliveryError
from log_foundry.sinks.clickhouse import ClickHouseSink


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
    # ``wait`` is bound into each sink at import, and its Event branch never reaches
    # ``time.sleep`` — patching either centrally would leave this fixture inert.
    monkeypatch.setattr("log_foundry.sinks.clickhouse.wait", lambda _delay, _stop=None: None)


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


def test_insert_failure_retried_then_counted(capsys) -> None:
    client = FakeClickHouse(fail_times=-1)
    sink = ClickHouseSink("log_events", client=client, max_retries=1)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}, {"a": 2}])  # the only chunk failed (SPEC-026 FR-001)
    assert len(client.inserts) == 2  # initial + 1 retry
    assert sink.failed == 2
    assert "lost 2 row(s)" in capsys.readouterr().err, "the line carries the count"


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


# --- SPEC-049 FR-002 -------------------------------------------------------------------------


@pytest.mark.parametrize("bad", [0, -1, -5])
def test_a_non_positive_chunk_size_is_refused(bad: int) -> None:
    """`0` raised out of `range()` per batch; a negative was the silent one.

    A negative made the chunker yield nothing, so `emit` returned having inserted no rows with
    `losses()` at zero -- silent total loss, which is the condition SPEC-026 exists to end.
    """
    with pytest.raises(ValueError, match="chunk_size"):
        ClickHouseSink("t", client=FakeClickHouse(), chunk_size=bad)


def test_a_batch_that_produces_no_chunk_raises_rather_than_returning(monkeypatch) -> None:
    """The branch, not the route -- and the route the first draft named cannot reach it.

    Refusing a non-positive `chunk_size` closes the only known way into `chunks == 0`, but the
    branch stays unguarded and a future chunker change walks back into it. Reached here by
    patching the chunker itself: monkeypatching `_chunk_size` after construction, which this
    spec's first draft specified, now raises a raw `ValueError` out of the first `next()` inside
    the emit lock and never gets here -- the two changes cancelled each other.
    """
    client = FakeClickHouse()
    sink = ClickHouseSink("t", client=client)
    monkeypatch.setattr("log_foundry.sinks.clickhouse.chunk_list", lambda items, size: iter(()))

    with pytest.raises(SinkDeliveryError, match="no chunk"):
        sink.emit([{"i": 1}, {"i": 2}])
    assert client.inserts == [], "and nothing was sent"
