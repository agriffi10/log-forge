"""SPEC-041 FR-001 — MongoDBSink against a real MongoDB."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import pymongo

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
