"""SPEC-010/032 — Redis sinks: pipelined XADD/RPUSH, retry, ownership, post-close refusal."""

from __future__ import annotations

import json
import sys
import types

import pytest

from log_foundry import SinkLosses
from log_foundry.sinks.base import Sink, SinkDeliveryError
from log_foundry.sinks.redis import RedisListSink, RedisStreamsSink


class FakePipeline:
    def __init__(self, fail: bool = False) -> None:
        self.ops: list[tuple] = []
        self.executed = 0
        self._fail = fail

    def xadd(self, name, fields, **options) -> None:
        self.ops.append(("xadd", name, fields, options))

    def rpush(self, name, value) -> None:
        self.ops.append(("rpush", name, value))

    def ltrim(self, name, start, end) -> None:
        self.ops.append(("ltrim", name, start, end))

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
        ("xadd", "mystream", {"event": json.dumps({"a": 1})}, {}),
        ("xadd", "mystream", {"event": json.dumps({"a": 2})}, {}),
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


def test_a_closed_sink_refuses_a_batch_without_moving_a_counter() -> None:
    """Refusing is a reported failure, not absorbed loss (SPEC-032 FR-001)."""
    client = FakeRedis()
    sink = RedisListSink("logs", client=client)
    sink._owns_client = True
    sink.close()

    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}])

    assert sink.losses() == SinkLosses(dropped=0, failed=0), "a refusal moved a loss counter"
    assert client.pipelines == [], "the sink asked a closed client for a pipeline"


def test_a_borrowed_client_surviving_its_sink_is_not_permission_to_keep_writing() -> None:
    """A closed sink refuses whether or not it owned the connection (SPEC-032 FR-001).

    ``close()`` leaves an injected client open — that is the caller's to release — so the client
    would happily take the write. The flag marks *this sink* as released, and a guard keyed on
    ownership instead would leave every borrowed-client sink accepting after ``shutdown()``,
    which is the majority configuration in tests and in any app that manages its own pool.
    """
    client = FakeRedis()
    sink = RedisStreamsSink("s", client=client)
    sink.close()

    assert client.closed is False, "an injected client must not be closed"
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}])
    assert client.pipelines == []


def test_close_is_idempotent() -> None:
    """A second ``close()`` reaches the client no further than the first."""
    closes: list[int] = []

    class CountingRedis(FakeRedis):
        def close(self) -> None:
            closes.append(1)
            super().close()

    client = CountingRedis()
    sink = RedisListSink("logs", client=client)
    sink._owns_client = True
    sink.close()
    sink.close()
    assert closes == [1]


# --- SPEC-038 FR-008: the destination can be bounded --------------------------------------


def test_the_stream_is_unbounded_by_default() -> None:
    """AC-2. Silently discarding a user's buffered logs is not a default this library chooses."""
    client = FakeRedis()
    RedisStreamsSink("s", client=client).emit([{"a": 1}])
    kind, name, _fields, options = client.pipelines[0].ops[0]
    assert (kind, name) == ("xadd", "s")
    assert options == {}, "no maxlen is passed unless one was configured"


def test_a_stream_maxlen_is_passed_as_an_approximate_trim() -> None:
    """AC-1. `approximate=True` is what makes Redis trim at a radix boundary rather than exactly.

    An exact trim costs Redis O(n) per insert; approximate is the form the driver documents for
    a capped stream, and the cap is a buffer ceiling rather than an exact retention promise.
    """
    client = FakeRedis()
    RedisStreamsSink("s", client=client, maxlen=500).emit([{"a": 1}, {"a": 2}])
    for kind, _name, _fields, options in client.pipelines[0].ops:
        assert kind == "xadd"
        assert options == {"maxlen": 500, "approximate": True}


def test_the_list_is_unbounded_by_default() -> None:
    client = FakeRedis()
    RedisListSink("k", client=client).emit([{"a": 1}])
    assert [op[0] for op in client.pipelines[0].ops] == ["rpush"], "no LTRIM without a maxlen"


def test_a_list_maxlen_trims_to_the_newest_entries_after_each_push() -> None:
    """AC-1. `ltrim(key, -n, -1)` keeps the newest `n`, which is the end RPUSH appends to."""
    client = FakeRedis()
    RedisListSink("k", client=client, maxlen=100).emit([{"a": 1}, {"a": 2}])
    ops = client.pipelines[0].ops
    assert [op[0] for op in ops] == ["rpush", "ltrim", "rpush", "ltrim"]
    assert ops[1] == ("ltrim", "k", -100, -1), "keep the newest 100, not the oldest"


def test_trimming_moves_no_loss_counter_because_it_happens_at_the_destination() -> None:
    """AC-3. Redis drops the entries itself, after this sink reported the write delivered.

    Those events are invisible to `health()`, which is the trade a bounded buffer makes and why
    it is opt-in rather than a default.
    """
    client = FakeRedis()
    sink = RedisStreamsSink("s", client=client, maxlen=1)
    sink.emit([{"a": i} for i in range(50)])
    assert sink.losses() == SinkLosses(dropped=0, failed=0)
