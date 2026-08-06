"""SPEC-011 — MongoDBSink: insert_many(ordered=False), BulkWriteError, oversized drop (fake client)."""

from __future__ import annotations

import sys
import types

import pytest

from log_foundry.sinks.base import Sink, SinkDeliveryError
from log_foundry.sinks.mongodb import MongoDBSink


class FakeBulkWriteError(Exception):
    """Duck-typed stand-in for pymongo's BulkWriteError (carries ``.details.writeErrors``)."""

    def __init__(self, rejects: int) -> None:
        super().__init__("bulk write error")
        self.details = {"writeErrors": [{"index": i} for i in range(rejects)]}


class FakeCollection:
    def __init__(self, error: Exception | None = None, error_times: int = 0) -> None:
        self.inserted: list[tuple] = []
        self._error = error
        self._error_times = error_times

    def insert_many(self, documents, ordered: bool = True):
        self.inserted.append((list(documents), ordered))
        if self._error is not None and self._error_times != 0:
            if self._error_times > 0:
                self._error_times -= 1
            raise self._error


class FakeDB:
    def __init__(self, collection: FakeCollection) -> None:
        self._collection = collection

    def __getitem__(self, name: str) -> FakeCollection:
        return self._collection


class FakeMongoClient:
    def __init__(self, collection: FakeCollection) -> None:
        self._db = FakeDB(collection)
        self.closed = False

    def __getitem__(self, name: str) -> FakeDB:
        return self._db

    def close(self) -> None:
        self.closed = True


def make_sink(collection: FakeCollection, **kwargs) -> MongoDBSink:
    return MongoDBSink(
        client=FakeMongoClient(collection), database="logs", collection="events", **kwargs
    )


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr("log_foundry.sinks._retry.time.sleep", lambda _s: None)


def test_is_a_sink() -> None:
    assert isinstance(make_sink(FakeCollection()), Sink)


def test_insert_many_unordered_with_events_as_is() -> None:
    collection = FakeCollection()
    make_sink(collection).emit([{"a": 1}, {"a": 2}])
    documents, ordered = collection.inserted[0]
    assert ordered is False
    assert documents == [{"a": 1}, {"a": 2}]


def test_does_not_mutate_caller_events() -> None:
    collection = FakeCollection()
    events = [{"a": 1}]
    make_sink(collection).emit(events)
    assert events == [{"a": 1}]  # a copy is inserted, so no _id leaks back onto the caller's dict


def test_bulk_write_error_counts_rejects_without_retry(capsys) -> None:
    collection = FakeCollection(error=FakeBulkWriteError(2), error_times=1)
    sink = make_sink(collection)
    sink.emit([{"a": 1}, {"a": 2}, {"a": 3}])
    assert sink.failed == 2
    assert "lost 2 document(s)" in capsys.readouterr().err
    assert len(collection.inserted) == 1  # unordered insert not retried


def test_connection_error_retried_then_counted(capsys) -> None:
    collection = FakeCollection(error=RuntimeError("no primary"), error_times=-1)
    sink = make_sink(collection, max_retries=2)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}, {"a": 2}])  # nothing was inserted (SPEC-026 FR-001)
    assert len(collection.inserted) == 3  # initial + 2 retries
    assert sink.failed == 2
    assert "lost 2 document(s)" in capsys.readouterr().err


def test_oversized_document_is_dropped(capsys) -> None:
    collection = FakeCollection()
    sink = make_sink(collection)
    sink.emit([{"pad": "x" * (16 * 1024 * 1024 + 100)}, {"ok": 1}])
    assert sink.dropped_oversized == 1
    assert "lost 1 document(s)" in capsys.readouterr().err
    assert collection.inserted[0][0] == [{"ok": 1}]


def test_injected_client_not_closed() -> None:
    client = FakeMongoClient(FakeCollection())
    sink = MongoDBSink(client=client, database="logs", collection="events")
    sink.close()
    assert client.closed is False
    sink.close()  # idempotent


def test_owned_client_is_closed(monkeypatch) -> None:
    client = FakeMongoClient(FakeCollection())
    monkeypatch.setitem(
        sys.modules, "pymongo", types.SimpleNamespace(MongoClient=lambda uri: client)
    )
    sink = MongoDBSink(uri="mongodb://x", database="logs", collection="events")
    sink.close()
    assert client.closed is True
