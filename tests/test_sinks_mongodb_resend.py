"""SPEC-095 FR-003: ``MongoDBSink`` reads a duplicate driver-stamped ``_id`` on its own resend as landed.

The run keeps each document, with the ``_id`` the driver stamped into it, across its rounds, so
a resend after an ``insert_many`` that stored the documents and then raised carries the same
``_id`` and is refused with ``11000``. That refusal is delivery when the index is ``_id``'s, the
event carried no ``_id`` of its own, and an earlier call of the run carried the document. Any
other duplicate key is a rejection, as before. Serves invariants 2 and 7.
"""

from __future__ import annotations

import itertools
import re
import threading
from typing import Any

import pytest

import driver_standins as standins
from log_foundry.sinks import _engine
from log_foundry.sinks.mongodb import MongoDBSink
from log_foundry.sinks.transform import TransformSink
from log_foundry.sinks.verdict import Outcome, Verdict

LOST_LINE = re.compile(r"^log-foundry: lost", re.MULTILINE)
_IDS = itertools.count()


class BulkWriteError(Exception):
    """pymongo's ``BulkWriteError`` in the shape the sink reads: its ``details``."""

    def __init__(self, details: dict[str, Any]) -> None:
        super().__init__("bulk write error")
        self.details = details


def entry(index: int, value: object, shape: str) -> dict[str, Any]:
    """A duplicate-key ``writeErrors`` entry, in the server's shape or a variation of it.

    Args:
      index: The document's position in the insert.
      value: Its ``_id``.
      shape: ``id`` (the server's), ``log_id`` (another unique index), ``errmsg-id`` (no
        ``keyPattern``, the ``errmsg`` naming ``_id_``), ``errmsg-other`` (no ``keyPattern``,
        another index).

    Returns:
      The entry.

    Raises:
      KeyError: For an unknown shape.
    """
    index_name = {"id": "_id_", "log_id": "log_id_1", "errmsg-id": "_id_", "errmsg-other": "x_1"}
    found: dict[str, Any] = {
        "index": index,
        "code": 11000,
        "errmsg": (
            f"E11000 duplicate key error collection: d.c index: {index_name[shape]} dup key: "
            f"{{ _id: {value} }}"
        ),
    }
    if shape == "id":
        found["keyPattern"] = {"_id": 1}
        found["keyValue"] = {"_id": value}
    elif shape == "log_id":
        found["keyPattern"] = {"log_id": 1}
        found["keyValue"] = {"log_id": value}
    return found


class Collection:
    """A collection that stamps ``_id`` as pymongo does and refuses a duplicate as MongoDB does.

    ``lose`` is how many calls store what they can and then raise ``NetworkTimeout``, the reply
    lost; ``first_duplicate`` makes the first call refuse every document as a duplicate ``_id``.
    """

    def __init__(
        self, timeout: type[Exception], *, lose: int = 0, shape: str = "id", first: bool = False
    ) -> None:
        self.timeout = timeout
        self.lose = lose
        self.shape = shape
        self.first = first
        self.stored: dict[object, dict[str, Any]] = {}
        self.calls = 0

    def insert_many(self, documents: list[dict[str, Any]], ordered: bool = True) -> None:
        self.calls += 1
        errors: list[dict[str, Any]] = []
        for index, document in enumerate(documents):
            document.setdefault("_id", f"oid{next(_IDS)}")
            if (self.first and self.calls == 1) or document["_id"] in self.stored:
                errors.append(entry(index, document["_id"], self.shape))
                continue
            self.stored[document["_id"]] = dict(document)
        if self.lose:
            self.lose -= 1
            raise self.timeout("timed out")
        if errors:
            raise BulkWriteError({"writeErrors": errors})


class Client:
    """A ``MongoClient`` shape over one collection."""

    def __init__(self, collection: Collection) -> None:
        self.collection = collection

    def __getitem__(self, name: str) -> dict[str, Collection]:
        return {"c": self.collection}

    def close(self) -> None:
        pass


def _events(n: int) -> list[dict[str, object]]:
    return [{"log_id": f"e{i}", "level": "INFO", "message": f"m{i}"} for i in range(n)]


def run(sink: Any, n: int) -> tuple[_engine.Delivery, list[Verdict]]:
    """One engine run, one resend allowed and no waits; with every verdict the sink returned.

    Args:
      sink: The sink, or a wrapper over it.
      n: How many events.

    Returns:
      The run's ``Delivery`` and its rounds' verdicts.

    Raises:
      BaseException: Whatever the run raises.
    """
    verdicts: list[Verdict] = []

    def deliver(batch: list[dict[str, object]]) -> Verdict:
        verdict = sink.deliver(batch)
        verdicts.append(verdict)
        return verdict  # type: ignore[no-any-return]

    stop = threading.Event()
    stop.set()
    delivery = _engine._run(
        sink,
        deliver,
        sink.log_foundry_counters,
        _events(n),
        sink_retries=1,
        worker_retries=0,
        stop=stop,
        decline=None,
    )
    return delivery, verdicts


@pytest.fixture
def pymongo(monkeypatch: pytest.MonkeyPatch) -> Any:
    """pymongo's error tree, installed as a stand-in."""
    return standins.install(monkeypatch, standins.PYMONGO)


def _sink(collection: Collection) -> MongoDBSink:
    return MongoDBSink(client=Client(collection), database="d", collection="c", max_retries=1)


def test_a_resend_refused_on_its_own_id_is_delivered(
    pymongo: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    """AC1: stored, reply lost, resend answered ``11000`` on ``_id``: delivered, nothing counted."""
    collection = Collection(pymongo.NetworkTimeout, lose=1)
    sink = _sink(collection)
    delivery, verdicts = run(sink, 3)
    assert collection.calls == 2 and len(collection.stored) == 3
    assert verdicts[-1].outcomes == (Outcome.ACCEPTED,) * 3
    assert not delivery.failed and delivery.accepted == 3
    assert sink.log_foundry_counters.failed_events == 0
    assert sink.losses().failed == 0 and sink.losses().resent == 0
    assert not LOST_LINE.search(capsys.readouterr().err)


def test_a_first_send_refused_on_id_is_rejected(
    pymongo: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    """AC2: no earlier call of the run carried the document, so it is a rejection, as before."""
    collection = Collection(pymongo.NetworkTimeout, first=True)
    sink = _sink(collection)
    delivery, verdicts = run(sink, 2)
    assert collection.calls == 1
    assert verdicts[0].outcomes == (Outcome.REJECTED,) * 2
    assert delivery.rejected == 2 and sink.losses().failed == 2
    assert LOST_LINE.search(capsys.readouterr().err)


def test_another_unique_index_is_rejected(pymongo: Any) -> None:
    """AC3: a resend refused on ``{"log_id": 1}`` is not the driver's own ``_id``."""
    collection = Collection(pymongo.NetworkTimeout, lose=1, shape="log_id")
    sink = _sink(collection)
    _, verdicts = run(sink, 2)
    assert collection.calls == 2
    assert verdicts[-1].outcomes == (Outcome.REJECTED,) * 2
    assert sink.losses().failed == 2


@pytest.mark.parametrize(
    ("shape", "outcome"), [("errmsg-id", "ACCEPTED"), ("errmsg-other", "REJECTED")]
)
def test_errmsg_names_the_index_when_keypattern_is_absent(
    pymongo: Any, shape: str, outcome: str
) -> None:
    """AC4: with no ``keyPattern``, the ``errmsg``'s `` index: _id_ `` decides."""
    collection = Collection(pymongo.NetworkTimeout, lose=1, shape=shape)
    sink = _sink(collection)
    _, verdicts = run(sink, 2)
    assert collection.calls == 2
    assert verdicts[-1].outcomes == (Outcome[outcome],) * 2


def test_a_caller_set_id_is_never_read_as_landed(pymongo: Any) -> None:
    """AC5: two events given one ``_id`` by a ``TransformSink``; the second is rejected, counted.

    The first was stored by the call whose reply was lost, but its ``_id`` is the caller's, not
    the driver's, so the sink cannot tell it from another event's: it is counted too, the cost of
    a caller-set ``_id`` that FR-003 accepts.
    """
    collection = Collection(pymongo.NetworkTimeout, lose=1)
    inner = _sink(collection)

    def same_id(event: dict[str, object]) -> dict[str, object]:
        return {**event, "_id": "shared"}

    sink = TransformSink(inner, same_id)
    _, verdicts = run(sink, 2)
    assert collection.calls == 2 and list(collection.stored) == ["shared"]
    assert verdicts[-1].outcomes[1] is Outcome.REJECTED
    assert verdicts[-1].outcomes == (Outcome.REJECTED, Outcome.REJECTED)
    assert inner.log_foundry_counters.failed_events == 2


def test_the_reader_needs_code_11000_exactly(pymongo: Any) -> None:
    """AC1's first condition: a ``code`` that is not exactly the ``int`` 11000 is a rejection."""
    from log_foundry.sinks.mongodb import _own_id_again

    good = entry(0, "x", "id")
    assert _own_id_again(good)
    assert not _own_id_again({**good, "code": 11001})
    assert not _own_id_again({**good, "code": "11000"})
    assert not _own_id_again({**good, "code": True})
    assert not _own_id_again({**good, "keyPattern": {"_id": 1, "x": 1}})
    assert not _own_id_again({**good, "keyPattern": "{_id: 1}"})
    assert not _own_id_again("not a mapping")
