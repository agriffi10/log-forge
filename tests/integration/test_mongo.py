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
