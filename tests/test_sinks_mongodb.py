"""SPEC-011 — MongoDBSink: insert_many(ordered=False), BulkWriteError, oversized drop (fake client)."""

from __future__ import annotations

import sys
import types

import pytest

from log_foundry.sinks.base import Sink, SinkDeliveryError
from log_foundry.sinks.mongodb import DEFAULT_SOCKET_TIMEOUT, MongoDBSink


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
    # ``wait`` is bound into each sink at import, and its Event branch never reaches
    # ``time.sleep`` — patching either centrally would leave this fixture inert.
    monkeypatch.setattr("log_foundry.sinks.mongodb.wait", lambda _delay, _stop=None: None)


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
        sys.modules, "pymongo", types.SimpleNamespace(MongoClient=lambda uri, **kwargs: client)
    )
    sink = MongoDBSink(uri="mongodb://x", database="logs", collection="events")
    sink.close()
    assert client.closed is True


# --- SPEC-049 FR-005: pymongo's unbounded socket wait is bounded, the URI's own value wins ------


def _pymongo_stub(monkeypatch) -> list[tuple]:
    """Installs a ``pymongo`` stand-in whose ``MongoClient`` records ``(uri, kwargs)``."""
    calls: list[tuple] = []

    def make_client(uri, **kwargs):
        calls.append((uri, kwargs))
        return FakeMongoClient(FakeCollection())

    monkeypatch.setitem(sys.modules, "pymongo", types.SimpleNamespace(MongoClient=make_client))
    return calls


def test_an_owned_client_gets_the_socket_bound_as_a_literal(monkeypatch) -> None:
    """The literal, not something derived from the constant: 30.0 forwarded raw is a 30 ms bound.

    pymongo's own ``socketTimeoutMS`` default is ``None`` — a read that never returns holds the
    drain thread for good — so the library supplies one when nobody else did.
    """
    calls = _pymongo_stub(monkeypatch)
    MongoDBSink(uri="mongodb://h/db", database="logs", collection="events")
    assert calls == [("mongodb://h/db", {"socketTimeoutMS": 30000})]
    assert DEFAULT_SOCKET_TIMEOUT == 30.0


def test_no_uri_at_all_still_gets_the_bound(monkeypatch) -> None:
    calls = _pymongo_stub(monkeypatch)
    MongoDBSink(database="logs", collection="events")
    assert calls[0][1] == {"socketTimeoutMS": 30000}


@pytest.mark.parametrize(
    "uri",
    [
        "mongodb://h/db?socketTimeoutMS=5000",
        "mongodb://h/db?sockettimeoutms=5000",
        "mongodb://h/db?retryWrites=true;socketTimeoutMS=5000",
        "mongodb://u:p@h1:27017,h2:27018/db?replicaSet=rs&socketTimeoutMS=5000",
    ],
)
def test_a_uri_that_names_the_option_is_not_overridden(monkeypatch, uri: str) -> None:
    """pika's precedence, not psycopg's: the library default never overrides a value the caller
    wrote. Measured, ``MongoClient(uri, socketTimeoutMS=30000)`` lets the keyword win over the
    URI, so the default has to be *withheld* rather than passed; case-insensitive and on either
    separator because pymongo accepts both, and multi-host URIs are legal."""
    calls = _pymongo_stub(monkeypatch)
    MongoDBSink(uri=uri, database="logs", collection="events")
    assert calls[0][1] == {}, "the URI's own socketTimeoutMS stands"


def test_an_explicit_socket_timeout_overrides_the_uri(monkeypatch) -> None:
    calls = _pymongo_stub(monkeypatch)
    MongoDBSink(
        uri="mongodb://h/db?socketTimeoutMS=5000",
        database="logs",
        collection="events",
        socket_timeout=2.0,
    )
    assert calls[0][1] == {"socketTimeoutMS": 2000}


def test_a_sub_millisecond_bound_rounds_up_rather_than_to_zero(monkeypatch) -> None:
    """``int(0.0004 * 1000)`` is ``0``, which pymongo reads as *no* timeout — the hole reopened."""
    calls = _pymongo_stub(monkeypatch)
    MongoDBSink(uri="mongodb://h/db", database="logs", collection="events", socket_timeout=0.0004)
    assert calls[0][1] == {"socketTimeoutMS": 1}


def test_server_selection_is_forwarded_only_when_given(monkeypatch) -> None:
    calls = _pymongo_stub(monkeypatch)
    MongoDBSink(
        uri="mongodb://h/db", database="logs", collection="events", server_selection_timeout=1.5
    )
    assert calls[0][1] == {"socketTimeoutMS": 30000, "serverSelectionTimeoutMS": 1500}


@pytest.mark.parametrize("name", ["socket_timeout", "server_selection_timeout"])
@pytest.mark.parametrize("bad", [0.0, -1.0, float("nan"), float("inf")])
def test_an_unusable_bound_is_refused(monkeypatch, name: str, bad: float) -> None:
    """New arguments have no working configuration to protect, so FR-001's rule refuses them."""
    calls = _pymongo_stub(monkeypatch)
    with pytest.raises(ValueError, match=f"MongoDBSink {name}"):
        MongoDBSink(uri="mongodb://h/db", database="logs", collection="events", **{name: bad})
    assert calls == [], "refused before any client was built"


def test_a_bound_alongside_an_injected_client_is_refused() -> None:
    """SPEC-043's rule: an already-connected client cannot consume a connect-time argument."""
    with pytest.raises(ValueError, match="cannot apply server_selection_timeout, socket_timeout"):
        MongoDBSink(
            client=FakeMongoClient(FakeCollection()),
            database="logs",
            collection="events",
            socket_timeout=1.0,
            server_selection_timeout=1.0,
        )
