"""SPEC-010 — RedisStreamsSink/RedisListSink: pipelined XADD/RPUSH, retry, ownership (fake client)."""

from __future__ import annotations

import json
import sys
import types

import pytest

from log_foundry.sinks.base import Sink, SinkDeliveryError
from log_foundry.sinks.redis import RedisListSink, RedisStreamsSink


class FakePipeline:
    def __init__(self, fail: bool = False) -> None:
        self.ops: list[tuple] = []
        self.executed = 0
        self._fail = fail

    def xadd(self, name, fields) -> None:
        self.ops.append(("xadd", name, fields))

    def rpush(self, name, value) -> None:
        self.ops.append(("rpush", name, value))

    def execute(self) -> list:
        self.executed += 1
        if self._fail:
            raise ConnectionError("redis down")
        return []


class FakeRedis:
    def __init__(self, fail: bool = False) -> None:
        self.pipelines: list[FakePipeline] = []
        self._fail = fail
        self.closed = False

    def pipeline(self) -> FakePipeline:
        pipe = FakePipeline(fail=self._fail)
        self.pipelines.append(pipe)
        return pipe

    def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    # ``wait`` is bound into each sink at import, and its Event branch never reaches
    # ``time.sleep`` — patching either centrally would leave this fixture inert.
    monkeypatch.setattr("log_foundry.sinks.redis.wait", lambda _delay, _stop=None: None)


def test_streams_is_a_sink() -> None:
    assert isinstance(RedisStreamsSink("s", client=FakeRedis()), Sink)


def test_streams_xadd_pipelined() -> None:
    client = FakeRedis()
    RedisStreamsSink("mystream", client=client).emit([{"a": 1}, {"a": 2}])
    pipe = client.pipelines[0]
    assert pipe.executed == 1
    assert pipe.ops == [
        ("xadd", "mystream", {"event": json.dumps({"a": 1})}),
        ("xadd", "mystream", {"event": json.dumps({"a": 2})}),
    ]


def test_list_rpush_pipelined() -> None:
    client = FakeRedis()
    RedisListSink("mylist", client=client).emit([{"a": 1}, {"a": 2}])
    pipe = client.pipelines[0]
    assert pipe.ops == [
        ("rpush", "mylist", json.dumps({"a": 1})),
        ("rpush", "mylist", json.dumps({"a": 2})),
    ]


def test_connection_error_retried_then_counted(capsys) -> None:
    client = FakeRedis(fail=True)
    sink = RedisStreamsSink("s", client=client, max_retries=2)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}, {"a": 2}])  # one pipeline, all or nothing (SPEC-026 FR-001)
    assert len(client.pipelines) == 3  # initial + 2 retries
    assert sink.failed == 2  # whole batch abandoned
    assert "lost 2 event(s)" in capsys.readouterr().err


def test_injected_client_is_not_closed() -> None:
    client = FakeRedis()
    RedisStreamsSink("s", client=client).close()
    assert client.closed is False


def test_owned_client_is_closed(monkeypatch) -> None:
    client = FakeRedis()

    class _Ctor:
        def __call__(self, *a, **k):
            return client

        def from_url(self, url):
            return client

    monkeypatch.setitem(sys.modules, "redis", types.SimpleNamespace(Redis=_Ctor()))
    sink = RedisStreamsSink("s")  # no client -> owned, imports the (faked) redis module
    sink.close()
    assert client.closed is True
