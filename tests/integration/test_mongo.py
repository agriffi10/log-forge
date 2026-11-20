"""SPEC-041 FR-001 — MongoDBSink against a real MongoDB."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

import pymongo
import pymongo.errors

from log_foundry.sinks.mongodb import MongoDBSink

if TYPE_CHECKING:
    from integration.conftest import Endpoint


def test_a_batch_lands_as_documents(services_are_up: dict[str, Endpoint]) -> None:
    uri = f"mongodb://{services_are_up['mongo'].url_host}"
    collection = f"lf_{uuid.uuid4().hex[:8]}"
    sink = MongoDBSink(uri=uri, database="log_foundry_integration", collection=collection)
    sink.emit([{"n": 1}, {"n": 2}, {"n": 3}])
    sink.close()

    client = pymongo.MongoClient(uri)
    stored = client["log_foundry_integration"][collection]
    assert sorted(doc["n"] for doc in stored.find({}, {"n": 1})) == [1, 2, 3]
    stored.drop()


def test_what_bson_refuses_is_repaired_and_stored(services_are_up: dict[str, Endpoint]) -> None:
    """SPEC-069 FR-003: the real driver's refusals are repaired, marked and each stored once.

    The detector is the real ``bson.encode``, so this is the half the unit tests' stand-in cannot
    settle: that what this pymongo refuses is what the repair fixes, and the server stores it.
    """
    from log_foundry.sanitize import int_placeholder

    uri = f"mongodb://{services_are_up['mongo'].url_host}"
    collection = f"lf_{uuid.uuid4().hex[:8]}"
    sink = MongoDBSink(uri=uri, database="log_foundry_integration", collection=collection)
    events: list[dict[str, object]] = [
        {"n": 1, "fields": {"big": 2**63}},
        {"n": 2, "fields": {"small": -(2**63) - 1}},
        {"n": 3, "fields": {"huge": 2**64}},
        {"n": 4, "fields": {"edge": 2**63 - 1, "low": -(2**63)}},
        {"n": 5, "fields": {"k\x00": 1}},
        {"n": 6, "fields": {"k": "v\x00"}},
    ]
    sink.emit(events)
    assert sink.losses().dropped == 0
    sink.close()

    client = pymongo.MongoClient(uri)
    stored = client["log_foundry_integration"][collection]
    docs = {doc["n"]: doc for doc in stored.find({}, {"_id": 0})}
    try:
        assert sorted(docs) == [1, 2, 3, 4, 5, 6], "one document each"
        assert docs[1]["fields"]["big"] == int_placeholder(2**63)
        assert docs[2]["fields"]["small"] == int_placeholder(-(2**63) - 1)
        assert docs[3]["fields"]["huge"] == int_placeholder(2**64)
        assert all(docs[n]["truncated"] is True for n in (1, 2, 3, 5))
        assert docs[4] == {"n": 4, "fields": {"edge": 2**63 - 1, "low": -(2**63)}}
        assert docs[5]["fields"] == {"k�": 1}
        assert docs[6] == {"n": 6, "fields": {"k": "v\x00"}}
    finally:
        stored.drop()


class _LosesFirstReply:
    """A real collection whose first ``insert_many`` stores the documents, then loses the reply.

    The first call goes to the server and then raises pymongo's own ``NetworkTimeout``, as a
    reply lost after the write would; every later call goes to the server as is. Each
    ``BulkWriteError`` the server raises has its ``details`` recorded before it is re-raised.
    """

    def __init__(self, real: Any) -> None:
        self.real = real
        self.lost = False
        self.details: list[dict[str, Any]] = []

    def insert_many(self, documents: list[dict[str, Any]], ordered: bool = True) -> Any:
        try:
            result = self.real.insert_many(documents, ordered=ordered)
        except pymongo.errors.BulkWriteError as err:
            self.details.append(err.details)
            raise
        if not self.lost:
            self.lost = True
            raise pymongo.errors.NetworkTimeout("the reply was lost")
        return result


def test_a_resend_after_a_lost_reply_is_read_as_delivered(
    services_are_up: dict[str, Endpoint],
) -> None:
    """SPEC-095 FR-003: the server refuses the resend on ``_id``, and that refusal is delivery.

    The documents land once, ``losses()`` reads no failure, and the server's own ``writeErrors``
    carry ``keyPattern`` ``{"_id": 1}``, the shape the sink reads.
    """
    uri = f"mongodb://{services_are_up['mongo'].url_host}"
    collection = f"lf_{uuid.uuid4().hex[:8]}"
    sink = MongoDBSink(
        uri=uri, database="log_foundry_integration", collection=collection, max_retries=2
    )
    proxy = _LosesFirstReply(sink._collection)
    sink._collection = proxy
    client = pymongo.MongoClient(uri)
    stored = client["log_foundry_integration"][collection]
    try:
        sink.emit([{"n": 1}, {"n": 2}, {"n": 3}])
        sink.close()
        assert proxy.lost, "the first reply was lost"
        assert sorted(doc["n"] for doc in stored.find({}, {"n": 1})) == [1, 2, 3]
        assert sink.losses().failed == 0
        errors = [entry for details in proxy.details for entry in details["writeErrors"]]
        assert len(errors) == 3, proxy.details
        assert all(entry["code"] == 11000 for entry in errors), errors
        assert all(entry["keyPattern"] == {"_id": 1} for entry in errors), errors
    finally:
        stored.drop()
