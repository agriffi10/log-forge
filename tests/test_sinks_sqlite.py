"""SPEC-008 — SQLiteSink: schema-ensure, batch insert, column projection, connection ownership.

Most tests inject an in-memory ``sqlite3`` connection so nothing touches disk; the owned-connection
path uses a real file under ``tmp_path`` to prove commit + close on the connection the sink opens.
"""

from __future__ import annotations

import json
import sqlite3
from typing import TYPE_CHECKING

import pytest

from log_foundry.sinks.base import Sink
from log_foundry.sinks.sqlite import SQLiteSink

if TYPE_CHECKING:
    from collections.abc import Iterator

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


@pytest.fixture
def conn() -> Iterator[sqlite3.Connection]:
    """An in-memory connection closed at teardown, which removes a leak rather than a crash.

    A ``sqlite3.Connection`` left unclosed is closed by its finalizer instead, in whichever
    process collects it — and on macOS a forked child that inherits one segfaults there:
    ``connection_close`` reaches ``sqlite3_log``, which reaches ``os_log``, which is not
    fork-safe. Nine connections leaked from this module were what crashed ``test_fork_lifecycle``
    under a rotating set of test names, whenever ``--dist worksteal`` put the two modules in one
    worker in that order: measured at 7 of 8 for
    ``pytest -n 0 tests/test_sinks_sqlite.py tests/test_fork_lifecycle.py`` before this fixture
    and 0 of 10 after. ``architecture.md`` section 13 carries the mechanism.

    **This fixture is the hygiene half and not the guard.** What makes the crash impossible is
    that every fork site in the suite collects in the parent first — ``run_in_child`` in
    ``test_fork_lifecycle`` states why. A ``ResourceWarning``-based gate was built here instead
    and rejected: the ``unclosed database`` warning does not exist before Python 3.13, so it
    covered one leg of a two-leg CI matrix, and under the suite's own ``-n 12`` an unraisable
    raised in a worker's teardown never reaches the session's exit code.
    """
    connection = sqlite3.connect(":memory:")
    try:
        yield connection
    finally:
        connection.close()


# --- FR-003: schema + insert ------------------------------------------------------------


def test_is_a_sink(conn: sqlite3.Connection) -> None:
    assert isinstance(SQLiteSink("ignored", connection=conn), Sink)


def test_creates_table_with_expected_columns(conn: sqlite3.Connection) -> None:
    SQLiteSink("ignored", connection=conn)
    columns = {row[1] for row in conn.execute("PRAGMA table_info(log_events)")}
    assert set(_ALL_COLUMNS) <= columns


def test_schema_creation_is_idempotent(conn: sqlite3.Connection) -> None:
    SQLiteSink("ignored", connection=conn)
    SQLiteSink("ignored", connection=conn)  # second ensure must not raise
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='log_events'"
    ).fetchall()
    assert len(tables) == 1


def test_emit_inserts_every_event_and_projects_columns(conn: sqlite3.Connection) -> None:
    sink = SQLiteSink("ignored", connection=conn)
    sink.emit([make_event(log_id="a", level="INFO"), make_event(log_id="b", level="ERROR")])
    rows = conn.execute("SELECT log_id, level, event FROM log_events ORDER BY id").fetchall()
    assert [(r[0], r[1]) for r in rows] == [("a", "INFO"), ("b", "ERROR")]
    # The event column round-trips the whole event as JSON.
    assert json.loads(rows[0][2])["log_id"] == "a"


def test_missing_keys_become_null(conn: sqlite3.Connection) -> None:
    sink = SQLiteSink("ignored", connection=conn)
    sink.emit([{"message": "hi"}])  # no identity/level/function keys
    row = conn.execute(
        "SELECT log_id, trace_id, span_id, timestamp, level, function, event FROM log_events"
    ).fetchone()
    assert row[:6] == (None, None, None, None, None, None)
    assert json.loads(row[6]) == {"message": "hi"}


def test_custom_table_name(conn: sqlite3.Connection) -> None:
    sink = SQLiteSink("ignored", connection=conn, table="events_2")
    sink.emit([make_event(log_id="x")])
    assert conn.execute("SELECT log_id FROM events_2").fetchone()[0] == "x"


def test_invalid_table_name_raises() -> None:
    with pytest.raises(ValueError):
        SQLiteSink(":memory:", table="log; DROP TABLE x")


# --- FR-003: create_table flag ----------------------------------------------------------


def test_create_table_false_runs_no_ddl_and_errors_at_insert(conn: sqlite3.Connection) -> None:
    sink = SQLiteSink("ignored", connection=conn, create_table=False)
    # No table was created…
    assert conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() == []
    # …so the insert surfaces a normal sqlite error rather than silently creating one.
    with pytest.raises(sqlite3.OperationalError):
        sink.emit([make_event()])


def test_create_table_false_uses_caller_provisioned_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE log_events (id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "log_id TEXT, trace_id TEXT, span_id TEXT, timestamp TEXT, level TEXT, "
        "function TEXT, event TEXT NOT NULL)"
    )
    sink = SQLiteSink("ignored", connection=conn, create_table=False)
    sink.emit([make_event(log_id="x")])
    assert conn.execute("SELECT log_id FROM log_events").fetchone()[0] == "x"


# --- FR-003: connection ownership + close ----------------------------------------------


def test_close_commits_injected_connection_but_does_not_close_it(conn: sqlite3.Connection) -> None:
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


# -- SPEC-055 FR-001 AC-4: the surrogate that cost the batch no longer reaches the column --------


def test_a_lone_surrogate_in_the_span_name_no_longer_costs_the_batch() -> None:
    """The audit's reproduction: `lost 3 event(s); batch abandoned after 4 emit attempts`.

    The worker inserts from its own thread, so the connection is opened `check_same_thread=False`;
    the module's `conn` fixture would fail every insert for an unrelated reason.
    """
    import os

    import log_foundry as lf

    bad = os.fsdecode(b"file-\xff.txt")
    connection = sqlite3.connect(":memory:", check_same_thread=False)
    try:
        sink = SQLiteSink("ignored", connection=connection)
        lf.configure(sink=sink)

        @lf.trace(name=bad)
        def work() -> None:
            lf.info(bad, path=bad)

        work()
        assert lf.flush(timeout=10)
        assert lf.health().failed_batches == 0
        rows = connection.execute("SELECT function, event FROM log_events").fetchall()
        assert len(rows) == 3
        assert {row[0] for row in rows} == {"file-�.txt"}
        payloads = [json.loads(row[1]) for row in rows]
        logged = next(p for p in payloads if p["message"] not in ("span.start", "span.end"))
        assert logged["message"] == "file-�.txt"
        assert logged["fields"]["path"] == "file-�.txt"
        assert all(p["truncated"] is True for p in payloads)
    finally:
        lf.shutdown()
        connection.close()
