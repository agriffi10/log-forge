"""SPEC-074 FR-003 — the database sinks make one attempt per chunk per round on the engine.

``ClickHouseSink`` inserts every remaining chunk once per round, ``MongoDBSink`` makes one
unordered ``insert_many`` of documents built once per run, and ``PostgresSink`` runs the round in
one transaction. ClickHouse and Postgres hold their transport lock for one round only, so a
``close()`` during a backoff returns at once and the next round is refused. Each double here
records what reached it; every expectation is read from the double, never from the sink's rule.
Serves invariants 2, 3, 6 and 7.
"""

from __future__ import annotations

import json
import threading
import types
from typing import Any, Self

import pytest

import log_foundry
from conftest import install_worker
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.clickhouse import ClickHouseSink
from log_foundry.sinks.mongodb import MongoDBSink
from log_foundry.sinks.postgres import PostgresSink
from log_foundry.sinks.redis import RedisListSink, RedisStreamsSink
from log_foundry.worker import Worker

KINDS = ("ClickHouseSink", "MongoDBSink", "PostgresSink")


@pytest.fixture(autouse=True)
def _no_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("log_foundry.sinks._retry.wait", lambda _delay, _stop=None: None)


class DataError(Exception):
    """A stand-in for a driver's permanent refusal: no predicate retries it."""


class StringDataRightTruncation(Exception):
    """psycopg's class for a value too long for its column, permanent (SPEC-070)."""


def events(n: int) -> list[dict[str, object]]:
    return [{"i": i, "message": f"m{i}"} for i in range(n)]


class Script:
    """Answers each call in turn: an exception to raise, or ``None`` to accept."""

    def __init__(self, *steps: BaseException | None, then: BaseException | None = None) -> None:
        self.steps = list(steps)
        self.then = then
        self.calls: list[list[object]] = []

    def next(self, carried: list[object]) -> None:
        self.calls.append(carried)
        step = self.steps.pop(0) if self.steps else self.then
        if step is not None:
            raise step


class ClickHouse:
    def __init__(self, script: Script) -> None:
        self.script = script

    def insert(self, table: str, data: Any = None, column_names: Any = None) -> None:
        self.script.next([json.loads(row[-1]).get("i") for row in data])

    def close(self) -> None:
        pass


class Cursor:
    def __init__(self, owner: PGConnection) -> None:
        self.owner = owner

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def execute(self, sql: str, params: object = None) -> None:
        pass

    def executemany(self, sql: str, rows: list[tuple[object, ...]]) -> None:
        self.owner.script.next([json.loads(str(row[-1])).get("i") for row in rows])


class PGConnection:
    def __init__(self, script: Script) -> None:
        self.script = script
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self.broken = False

    def cursor(self) -> Cursor:
        return Cursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


class Collection:
    def __init__(self, script: Script) -> None:
        self.script = script
        self.documents: list[list[dict[str, Any]]] = []

    def insert_many(self, documents: list[dict[str, Any]], ordered: bool = True) -> None:
        assert ordered is False
        self.documents.append(list(documents))
        for document in documents:
            document.setdefault("_id", f"oid-{len(self.documents)}-{id(document)}")
        self.script.next([document.get("i") for document in documents])


class MongoClient:
    def __init__(self, collection: Collection) -> None:
        self.collection = collection

    def __getitem__(self, name: str) -> Any:
        return {"events": self.collection}

    def close(self) -> None:
        pass


def build(kind: str, script: Script, **kwargs: Any) -> tuple[Any, Any]:
    if kind == "ClickHouseSink":
        return ClickHouseSink("logs", client=ClickHouse(script), **kwargs), script
    if kind == "PostgresSink":
        kwargs.pop("chunk_size", None)
        connection = PGConnection(script)
        return PostgresSink("logs", connection=connection, **kwargs), connection
    kwargs.pop("chunk_size", None)
    collection = Collection(script)
    sink = MongoDBSink(client=MongoClient(collection), database="d", collection="events", **kwargs)
    return sink, collection


def lost(err: str) -> list[str]:
    return [line for line in err.splitlines() if line.startswith("log-foundry: lost")]


def _worker(sink: object, **kwargs: Any) -> Worker:
    kwargs.setdefault("batch_size", 1000)
    kwargs.setdefault("flush_interval", 60.0)
    worker = Worker(sink, **kwargs)  # type: ignore[arg-type]
    install_worker(worker)
    return worker


def bulk_error(indices: list[object]) -> Exception:
    error = RuntimeError("bulk failed")
    error.details = {"writeErrors": [{"index": i, "code": 11000} for i in indices]}  # type: ignore[attr-defined]
    return error


# --- ClickHouse ---------------------------------------------------------------------------------


def test_clickhouse_resends_only_the_failed_chunk() -> None:
    script = Script(None, ConnectionError("down"), None)
    sink, _ = build("ClickHouseSink", script, chunk_size=1, max_retries=3)
    sink.emit(events(3))
    assert script.calls == [[0], [1], [2], [1]]
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


def test_clickhouse_spends_max_retries_plus_one_per_chunk() -> None:
    script = Script(then=ConnectionError("down"))
    sink, _ = build("ClickHouseSink", script, chunk_size=1, max_retries=2)
    with pytest.raises(SinkDeliveryError):
        sink.emit(events(2))
    assert len(script.calls) == 6
    assert sink.failed == 2


def test_clickhouse_refused_chunk_costs_only_itself(capsys: pytest.CaptureFixture[str]) -> None:
    script = Script(DataError("bad row"))
    sink, _ = build("ClickHouseSink", script, chunk_size=1, max_retries=3)
    sink.emit(events(3))
    assert script.calls == [[0], [1], [2]]
    assert sink.failed == 1
    assert lost(capsys.readouterr().err) == [
        "log-foundry: lost 1 row(s); ClickHouseSink, 1 attempt(s), DataError"
    ]


# --- MongoDB ------------------------------------------------------------------------------------


def test_mongodb_named_write_errors_are_rejected_and_the_rest_accepted(
    capsys: pytest.CaptureFixture[str],
) -> None:
    script = Script(bulk_error([0, 2]))
    sink, collection = build("MongoDBSink", script, max_retries=3)
    sink.emit(events(4))
    assert len(collection.documents) == 1, "never resent"
    assert sink.failed == 2
    assert lost(capsys.readouterr().err) == [
        "log-foundry: lost 2 document(s); MongoDBSink bulk write; the rest were inserted"
    ]


def test_mongodb_all_named_raises() -> None:
    sink, collection = build("MongoDBSink", Script(bulk_error([0, 1, 2, 3])), max_retries=3)
    with pytest.raises(SinkDeliveryError):
        sink.emit(events(4))
    assert len(collection.documents) == 1
    assert sink.failed == 4


@pytest.mark.parametrize("indices", [[9], [1, 1], [True]], ids=["unknown", "twice", "bool"])
def test_mongodb_unpairable_write_errors_are_unknown(
    indices: list[object], capsys: pytest.CaptureFixture[str]
) -> None:
    sink, collection = build("MongoDBSink", Script(bulk_error(indices)), max_retries=3)
    sink.emit(events(4))
    assert len(collection.documents) == 1
    assert sink.failed == 4
    lines = lost(capsys.readouterr().err)
    assert len(lines) == 1 and "could not pair" in lines[0], lines


def test_mongodb_resends_the_same_documents(monkeypatch: pytest.MonkeyPatch) -> None:
    errors = types.ModuleType("pymongo.errors")

    class PyMongoError(Exception):
        pass

    class ConnectionFailure(PyMongoError):
        pass

    class AutoReconnect(ConnectionFailure):
        pass

    errors.PyMongoError = PyMongoError  # type: ignore[attr-defined]
    errors.ConnectionFailure = ConnectionFailure  # type: ignore[attr-defined]
    errors.AutoReconnect = AutoReconnect  # type: ignore[attr-defined]
    monkeypatch.setitem(__import__("sys").modules, "pymongo.errors", errors)
    sink, collection = build(
        "MongoDBSink", Script(AutoReconnect("primary stepped down")), max_retries=3
    )
    sink.emit(events(2))
    first, second = collection.documents
    assert all(a is b for a, b in zip(first, second, strict=True)), "the same document objects"
    assert [d["_id"] for d in second] == [d["_id"] for d in first], "carrying the same _id"


def test_mongodb_builds_a_document_per_occurrence_of_one_event() -> None:
    """M1: the same event twice is two documents, never one object inserted twice."""
    event: dict[str, object] = {"i": 0}
    sink, collection = build("MongoDBSink", Script(None))
    sink.emit([event, event])
    first, second = collection.documents[0]
    assert first is not second


# --- PostgreSQL ---------------------------------------------------------------------------------


def test_postgres_rolls_back_once_per_round() -> None:
    script = Script(then=ConnectionError("down"))
    sink, connection = build("PostgresSink", script, max_retries=2)
    with pytest.raises(SinkDeliveryError):
        sink.emit(events(3))
    assert connection.rollbacks == 3
    assert connection.commits == 0
    assert sink.failed == 3


def test_postgres_truncation_is_one_attempt() -> None:
    script = Script(then=StringDataRightTruncation("value too long"))
    sink, connection = build("PostgresSink", script, max_retries=3)
    with pytest.raises(SinkDeliveryError):
        sink.emit(events(2))
    assert len(script.calls) == 1
    assert connection.rollbacks == 1


def test_postgres_borrowed_broken_line_keeps_its_suffix(capsys: pytest.CaptureFixture[str]) -> None:
    script = Script(then=ConnectionError("down"))
    sink, connection = build("PostgresSink", script, max_retries=0)
    connection.broken = True
    with pytest.raises(SinkDeliveryError):
        sink.emit(events(1))
    assert lost(capsys.readouterr().err) == [
        (
            "log-foundry: lost 1 event(s); PostgresSink, 1 attempt(s), ConnectionError, borrowed "
            "connection is broken and this sink may not reopen it"
        )
    ]


# --- a close during a backoff ---------------------------------------------------------------------


@pytest.mark.parametrize("kind", ["ClickHouseSink", "PostgresSink"])
def test_a_close_during_a_backoff_returns_before_the_next_round(
    kind: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    waiting = threading.Event()
    release = threading.Event()

    def wait(delay: float, stop: object = None) -> None:
        waiting.set()
        assert release.wait(10.0)

    monkeypatch.setattr("log_foundry.sinks._retry.wait", wait)
    script = Script(then=ConnectionError("down"))
    sink, _ = build(kind, script, max_retries=3)
    outcome: list[BaseException | None] = []

    def emit() -> None:
        try:
            sink.emit(events(2))
            outcome.append(None)
        except BaseException as err:
            outcome.append(err)

    thread = threading.Thread(target=emit)
    thread.start()
    assert waiting.wait(10.0), "the engine reached its backoff"
    closer = threading.Thread(target=sink.close)
    closer.start()
    closer.join(2.0)
    assert not closer.is_alive(), "close() waited on a lock held across the backoff"
    release.set()
    thread.join(10.0)
    assert len(script.calls) == 1, "the round after the close made no client call"
    assert isinstance(outcome[0], SinkDeliveryError) and "the sink is closed" in str(outcome[0])
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


# --- a closed sink refuses ------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("kind", "unit"),
    [("ClickHouseSink", "event"), ("MongoDBSink", "document"), ("PostgresSink", "event")],
)
def test_emit_after_close_raises_the_1x_error(kind: str, unit: str) -> None:
    """The message is ``fda134a``'s, read from each module there: ``inserted none of N <unit>(s):
    the sink is closed``, raised before any client call with nothing counted."""
    expected = f"{kind} inserted none of 3 {unit}(s): the sink is closed"
    script = Script()
    sink, _ = build(kind, script)
    sink.close()
    with pytest.raises(SinkDeliveryError) as info:
        sink.emit(events(3))
    assert str(info.value) == expected
    assert script.calls == []
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


# --- the worker and orphan twins ------------------------------------------------------------------


def _orphan_losses(sink: Any) -> SinkLosses:
    log_foundry.configure(service="t", sink=sink)
    log_foundry.info("one")
    log_foundry.info("two")
    return sink.losses()


def _worker_losses(sink: Any) -> SinkLosses:
    worker = _worker(sink, max_retries=0)
    for _ in range(2):
        worker.submit(events(1))
        worker.flush(timeout=30.0)
    return sink.losses()


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("case", ["total", "partial"])
def test_the_twins_keep_one_ledger(kind: str, case: str) -> None:
    def make() -> Any:
        if case == "total":
            script = Script(then=ConnectionError("down"))
        elif kind == "MongoDBSink":
            script = Script(bulk_error([0]))
        else:
            script = Script(DataError("bad"))
        return build(kind, script, max_retries=1, chunk_size=1)[0]

    assert _orphan_losses(make()) == _worker_losses(make())


class ScriptedPipeline:
    """A redis-py pipeline whose ``execute`` answers from a :class:`Script`."""

    def __init__(self, script: Script) -> None:
        self.script = script
        self.staged = 0

    def xadd(self, name: str, fields: Any, **options: Any) -> None:
        self.staged += 1

    def rpush(self, name: str, value: Any) -> None:
        self.staged += 1

    def ltrim(self, name: str, start: int, end: int) -> None:
        pass

    def execute(self) -> list[object]:
        self.script.next([self.staged])
        return []


class ScriptedRedis:
    def __init__(self, script: Script) -> None:
        self.script = script

    def pipeline(self) -> ScriptedPipeline:
        return ScriptedPipeline(self.script)

    def close(self) -> None:
        pass


def _encoding(kind: str, script: Script) -> Any:
    if kind == "RedisStreamsSink":
        return RedisStreamsSink("k", client=ScriptedRedis(script), max_retries=3)
    if kind == "RedisListSink":
        return RedisListSink("k", client=ScriptedRedis(script), max_retries=3)
    return build(kind, script, max_retries=3)[0]


@pytest.mark.parametrize(
    ("kind", "method"),
    [
        ("ClickHouseSink", "_row"),
        ("PostgresSink", "_row"),
        ("RedisStreamsSink", "_encode"),
        ("RedisListSink", "_encode"),
    ],
)
def test_each_event_is_encoded_once_per_run(
    kind: str, method: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-002's encode-once criterion for SPEC-069 FR-001's database sinks and the Redis sinks."""
    script = Script(ConnectionError("down"), ConnectionError("down"), None)
    sink = _encoding(kind, script)
    real = getattr(sink, method)
    calls: list[object] = []

    def counted(event: dict[str, object]) -> object:
        calls.append(event)
        return real(event)

    setattr(sink, method, counted)
    sink.emit([*events(3), {"i": 9, "blob": b"raw"}])
    assert len(script.calls) == 3, "three rounds were made"
    assert len(calls) == 4
    lines = [line for line in capsys.readouterr().err.splitlines() if "encoding raised" in line]
    assert lines == [f"log-foundry: lost 1 event(s); {kind}, encoding raised TypeError"]
    assert sink.losses().dropped == 1
