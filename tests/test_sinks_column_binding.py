"""SPEC-069 follow-ups: a value a SQL driver cannot bind, and a lone surrogate, cost only their event.

``SQLiteSink`` lost a whole batch to one projected value ``sqlite3`` could not bind — a ``dict``
or ``list`` as ``ProgrammingError``, an integer past 64 bits as a bare, uncounted
``OverflowError`` — and ``PostgresSink`` to a ``dict`` psycopg could not adapt or a lone
surrogate PostgreSQL refused. The projected column now holds the value's JSON text, and a lone
surrogate drops its event alone. The real-server half is in ``tests/integration/test_postgres.py``.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Self

import pytest

from log_foundry.sinks._encode import column_value, refuse_surrogate
from log_foundry.sinks.postgres import PostgresSink
from log_foundry.sinks.sqlite import SQLiteSink
from log_foundry.sinks.transform import TransformSink

UNBINDABLE: list[Any] = [
    2**63,
    -(2**63) - 1,
    2**70,
    {"x": 1},
    [1, 2],
    [1, "a"],
    [{"a": 1}],
    (1, 2),
    {},
]


def _sqlite() -> tuple[SQLiteSink, sqlite3.Connection]:
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    return SQLiteSink(":memory:", connection=conn), conn


def _levels(conn: sqlite3.Connection) -> list[tuple[object, ...]]:
    return conn.execute("SELECT log_id, level, event FROM log_events ORDER BY id").fetchall()


@pytest.mark.parametrize("value", UNBINDABLE, ids=repr)
def test_sqlite_binds_an_unbindable_column_as_its_json_text_and_delivers_the_batch(
    value: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    """Every event lands; the bad one's column is the JSON the event column holds for it."""
    sink, conn = _sqlite()
    bad = {"log_id": "b", "level": value}
    sink.emit([{"log_id": "a", "level": "INFO"}, bad, {"log_id": "c", "level": "WARN"}])

    rows = _levels(conn)
    assert [row[0] for row in rows] == ["a", "b", "c"]
    assert rows[1][1] == json.dumps(value)
    assert rows[1][2] == json.dumps(bad)
    assert sink.losses() is None
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize("value", [2**63 - 1, -(2**63), True, 0, 1.5, "INFO", None], ids=repr)
def test_a_value_the_driver_binds_is_handed_over_unchanged(value: object) -> None:
    assert column_value(value, int64=True) is value
    assert column_value(value, int64=False) is value


def test_postgres_binds_an_integer_past_64_bits_unchanged() -> None:
    big = 2**70
    assert column_value(big, int64=False) is big
    assert column_value(big, int64=True) == str(big)


def test_the_range_test_runs_no_comparison_a_subclass_defines() -> None:
    class Hostile(int):
        def __le__(self, other: object) -> bool:
            raise AssertionError("a subclass comparison ran")

        __ge__ = __lt__ = __gt__ = __le__

    assert column_value(Hostile(2**70), int64=True) == str(2**70)
    small = Hostile(5)
    assert column_value(small, int64=True) is small


def test_sqlite_drops_only_the_event_whose_column_holds_a_lone_surrogate(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The column cannot be bound and only a direct ``emit`` brings one: dropped, not repaired."""
    sink, conn = _sqlite()
    sink.emit([{"log_id": "a"}, {"log_id": "b", "level": "x\ud800"}, {"log_id": "c"}])

    assert [row[0] for row in _levels(conn)] == ["a", "c"]
    assert sink.losses() is not None
    assert (sink.losses().dropped, sink.losses().failed) == (1, 0)
    assert "SQLiteSink, encoding raised UnicodeEncodeError" in capsys.readouterr().err


def test_sqlite_keeps_a_lone_surrogate_outside_the_projected_columns() -> None:
    """SQLite stores the escaped JSON text, so dropping that event would invent a loss."""
    sink, conn = _sqlite()
    sink.emit([{"log_id": "a", "msg": "x\ud800"}])

    assert json.loads(_levels(conn)[0][2])["msg"] == "x\ud800"
    assert sink.losses() is None


def test_a_transform_putting_a_container_in_a_column_no_longer_costs_the_batch() -> None:
    """The pipeline route that admits the repair: a transform's output is JSON, not bindable."""
    sink, conn = _sqlite()
    wrapped = TransformSink(sink, lambda e: {**e, "level": {"x": 1}} if e["log_id"] == "b" else e)
    wrapped.emit([{"log_id": "a"}, {"log_id": "b"}, {"log_id": "c"}])

    assert [(row[0], row[1]) for row in _levels(conn)] == [
        ("a", None),
        ("b", '{"x": 1}'),
        ("c", None),
    ]


@pytest.mark.parametrize("text", ["x\udc80", "\ud83d\ude00"], ids=["lone", "pair as two"])
def test_column_value_refuses_a_string_neither_driver_encodes(text: str) -> None:
    with pytest.raises(UnicodeEncodeError):
        column_value(text, int64=False)


def test_sqlite_reads_the_event_column_before_the_projected_ones(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """An event that is not JSON is dropped for that, before a bad column is ever read."""
    sink, conn = _sqlite()
    sink.emit([{"log_id": "a"}, {"log_id": "b", "level": "x\ud800", "tags": {1, 2}}])

    assert [row[0] for row in _levels(conn)] == ["a"]
    assert "SQLiteSink, encoding raised TypeError" in capsys.readouterr().err


@pytest.mark.parametrize(
    "event",
    [
        {"msg": "smile \U0001f600"},
        {"msg": "\\ud800 as text"},
        {"msg": "plain"},
        {"msg": "\ud83d\ude00"},
        {"k": "\\\\ud800"},
    ],
    ids=["emoji pair", "escaped backslash", "clean", "pair as two code points", "backslash run"],
)
def test_refuse_surrogate_passes_what_postgres_accepts(event: dict[str, object]) -> None:
    refuse_surrogate(event, json.dumps(event))


@pytest.mark.parametrize(
    "event",
    [
        {"msg": "a\ud800b"},
        {"fields": {"k\udc80": 1}},
        {"list": ["\udfff"]},
        {"msg": "\ude00\ud83d"},
        {"msg": "\\\ud800"},
    ],
    ids=["value", "key", "nested", "pair reversed", "backslash then a real one"],
)
def test_refuse_surrogate_raises_for_a_real_lone_surrogate(event: dict[str, object]) -> None:
    with pytest.raises(UnicodeEncodeError):
        refuse_surrogate(event, json.dumps(event))


class _Cursor:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows = rows

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def execute(self, sql: str, params: object = None) -> None:
        return None

    def executemany(self, sql: str, rows: list[tuple[object, ...]]) -> None:
        self._rows.extend(tuple(row) for row in rows)


class _Connection:
    def __init__(self) -> None:
        self.rows: list[tuple[object, ...]] = []

    def cursor(self) -> _Cursor:
        return _Cursor(self.rows)

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None


def test_postgres_drops_only_the_event_with_a_lone_surrogate(
    capsys: pytest.CaptureFixture[str],
) -> None:
    conn = _Connection()
    sink = PostgresSink("logs", connection=conn, max_retries=0)
    sink.emit([{"n": 1}, {"n": 2, "fields": {"k\ud800": "v"}}, {"n": 3}])

    assert [json.loads(str(row[-1]))["n"] for row in conn.rows] == [1, 3]
    assert (sink.losses().dropped, sink.losses().failed) == (1, 0)
    assert "PostgresSink, encoding raised UnicodeEncodeError" in capsys.readouterr().err


def test_postgres_binds_a_container_column_as_its_json_text() -> None:
    conn = _Connection()
    sink = PostgresSink("logs", connection=conn, max_retries=0)
    sink.emit([{"level": {"x": 1}, "service": [1, "a"]}])

    row = conn.rows[0]
    assert row[1] == '{"x": 1}'
    assert row[5] == '[1, "a"]'
    assert json.loads(str(row[6])) == {"level": {"x": 1}, "service": [1, "a"]}
