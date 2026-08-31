"""SPEC-041 FR-001 — RedisStreamsSink and RedisListSink against a real Redis."""

from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING

import pytest
import redis as redis_mod

from log_foundry.sinks.redis import RedisListSink, RedisStreamsSink

if TYPE_CHECKING:
    from integration.conftest import Endpoint


@pytest.fixture
def client(services_are_up: dict[str, Endpoint]):
    return redis_mod.Redis.from_url(f"redis://{services_are_up['redis'].url_host}")


def test_a_batch_lands_as_stream_entries(services_are_up: dict[str, Endpoint], client) -> None:
    key = f"lf-stream-{uuid.uuid4().hex[:8]}"
    sink = RedisStreamsSink(key, url=f"redis://{services_are_up['redis'].url_host}")
    sink.emit([{"n": 1}, {"n": 2}, {"n": 3}])
    sink.close()

    entries = client.xrange(key)
    assert [json.loads(fields[b"event"])["n"] for _, fields in entries] == [1, 2, 3]
    client.delete(key)


def test_a_batch_lands_as_list_items_newest_last(
    services_are_up: dict[str, Endpoint], client
) -> None:
    key = f"lf-list-{uuid.uuid4().hex[:8]}"
    sink = RedisListSink(key, url=f"redis://{services_are_up['redis'].url_host}")
    sink.emit([{"n": 1}, {"n": 2}, {"n": 3}])
    sink.close()

    assert [json.loads(item)["n"] for item in client.lrange(key, 0, -1)] == [1, 2, 3]
    client.delete(key)
