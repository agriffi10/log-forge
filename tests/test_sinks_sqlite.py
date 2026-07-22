"""SPEC-008 — SQLiteSink: schema-ensure, batch insert, column projection, connection ownership.

Most tests inject an in-memory ``sqlite3`` connection so nothing touches disk; the owned-connection
path uses a real file under ``tmp_path`` to prove commit + close on the connection the sink opens.
"""

from __future__ import annotations

import json
import sqlite3

import pytest

from log_foundry.sinks.base import Sink
from log_foundry.sinks.sqlite import SQLiteSink

_ALL_COLUMNS = ("log_id", "trace_id", "span_id", "timestamp", "level", "function", "event")


def make_event(**overrides: object) -> dict[str, object]:
    """A representative built event; override individual keys per test."""
    event: dict[str, object] = {
        "log_id": "l1",
        "trace_id": "t1",
        "span_id": "s1",
        "timestamp": "2026-07-11T00:00:00.000Z",
        "level": "INFO",
        "function": "fn",
        "message": "m",
        "fields": {},
    }
    event.update(overrides)
    return event


# --- FR-003: schema + insert ------------------------------------------------------------


def test_is_a_sink() -> None:
    assert isinstance(SQLiteSink("ignored", connection=sqlite3.connect(":memory:")), Sink)


def test_creates_table_with_expected_columns() -> None:
    conn = sqlite3.connect(":memory:")
    SQLiteSink("ignored", connection=conn)
    columns = {row[1] for row in conn.execute("PRAGMA table_info(log_events)")}
    assert set(_ALL_COLUMNS) <= columns


def test_schema_creation_is_idempotent() -> None:
    conn = sqlite3.connect(":memory:")
    SQLiteSink("ignored", connection=conn)
    SQLiteSink("ignored", connection=conn)  # second ensure must not raise
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='log_events'"
    ).fetchall()
    assert len(tables) == 1


def test_emit_inserts_every_event_and_projects_columns() -> None:
    conn = sqlite3.connect(":memory:")
    sink = SQLiteSink("ignored", connection=conn)
    sink.emit([make_event(log_id="a", level="INFO"), make_event(log_id="b", level="ERROR")])
    rows = conn.execute("SELECT log_id, level, event FROM log_events ORDER BY id").fetchall()
    assert [(r[0], r[1]) for r in rows] == [("a", "INFO"), ("b", "ERROR")]
    # The event column round-trips the whole event as JSON.
    assert json.loads(rows[0][2])["log_id"] == "a"


def test_missing_keys_become_null() -> None:
    conn = sqlite3.connect(":memory:")
    sink = SQLiteSink("ignored", connection=conn)
    sink.emit([{"message": "hi"}])  # no identity/level/function keys
    row = conn.execute(
        "SELECT log_id, trace_id, span_id, timestamp, level, function, event FROM log_events"
    ).fetchone()
    assert row[:6] == (None, None, None, None, None, None)
    assert json.loads(row[6]) == {"message": "hi"}


def test_custom_table_name() -> None:
    conn = sqlite3.connect(":memory:")
    sink = SQLiteSink("ignored", connection=conn, table="events_2")
    sink.emit([make_event(log_id="x")])
    assert conn.execute("SELECT log_id FROM events_2").fetchone()[0] == "x"


def test_invalid_table_name_raises() -> None:
    with pytest.raises(ValueError):
        SQLiteSink(":memory:", table="log; DROP TABLE x")


# --- FR-003: create_table flag ----------------------------------------------------------


def test_create_table_false_runs_no_ddl_and_errors_at_insert() -> None:
    conn = sqlite3.connect(":memory:")
    sink = SQLiteSink("ignored", connection=conn, create_table=False)
    # No table was created…
    assert conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() == []
    # …so the insert surfaces a normal sqlite error rather than silently creating one.
    with pytest.raises(sqlite3.OperationalError):
        sink.emit([make_event()])


def test_create_table_false_uses_caller_provisioned_table() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE log_events (id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "log_id TEXT, trace_id TEXT, span_id TEXT, timestamp TEXT, level TEXT, "
        "function TEXT, event TEXT NOT NULL)"
    )
    sink = SQLiteSink("ignored", connection=conn, create_table=False)
    sink.emit([make_event(log_id="x")])
    assert conn.execute("SELECT log_id FROM log_events").fetchone()[0] == "x"


# --- FR-003: connection ownership + close ----------------------------------------------


def test_close_commits_injected_connection_but_does_not_close_it() -> None:
    conn = sqlite3.connect(":memory:")
    sink = SQLiteSink("ignored", connection=conn)
    sink.emit([make_event(log_id="x")])
    sink.close()
    # Still usable → the injected connection was not closed; the row is committed.
    assert conn.execute("SELECT COUNT(*) FROM log_events").fetchone()[0] == 1
    sink.close()  # idempotent


def test_close_closes_the_owned_connection_and_commits(tmp_path) -> None:
    database = str(tmp_path / "log.db")
    sink = SQLiteSink(database)
    sink.emit([make_event(log_id="x")])
    sink.close()
    # The owned connection is closed…
    with pytest.raises(sqlite3.ProgrammingError):
        sink._conn.execute("SELECT 1")
    # …and the data was committed to the file (readable via a fresh connection).
    verify = sqlite3.connect(database)
    assert verify.execute("SELECT log_id FROM log_events").fetchone()[0] == "x"
    verify.close()
    sink.close()  # idempotent, even though the owned connection is already closed
