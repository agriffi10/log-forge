"""SPEC-010 — NATSSink: sync-driven publish, JetStream path, drain-on-close (fake client)."""

from __future__ import annotations

import json

from log_foundry.sinks.base import Sink
from log_foundry.sinks.nats import NATSSink


class FakeJetStream:
    def __init__(self, owner: FakeNATS) -> None:
        self._owner = owner

    async def publish(self, subject, payload) -> None:
        self._owner.js_published.append((subject, payload))


class FakeNATS:
    def __init__(self, fail: bool = False) -> None:
        self.published: list[tuple] = []
        self.js_published: list[tuple] = []
        self.drained = False
        self._fail = fail

    async def publish(self, subject, payload) -> None:
        if self._fail:
            raise RuntimeError("no responders")
        self.published.append((subject, payload))

    def jetstream(self) -> FakeJetStream:
        return FakeJetStream(self)

    async def drain(self) -> None:
        self.drained = True


def test_is_a_sink() -> None:
    sink = NATSSink("subject", client=FakeNATS())
    assert isinstance(sink, Sink)
    sink.close()


def test_publishes_one_message_per_event() -> None:
    client = FakeNATS()
    sink = NATSSink("logs", client=client)
    sink.emit([{"a": 1}, {"a": 2}])
    sink.close()
    assert client.published == [
        ("logs", json.dumps({"a": 1}).encode("utf-8")),
        ("logs", json.dumps({"a": 2}).encode("utf-8")),
    ]


def test_jetstream_path_publishes_via_jetstream() -> None:
    client = FakeNATS()
    sink = NATSSink("logs", client=client, jetstream=True)
    sink.emit([{"a": 1}])
    sink.close()
    assert client.js_published == [("logs", json.dumps({"a": 1}).encode("utf-8"))]
    assert client.published == []


def test_publish_errors_counted(capsys) -> None:
    client = FakeNATS(fail=True)
    sink = NATSSink("logs", client=client)
    sink.emit([{"a": 1}, {"a": 2}])
    sink.close()
    assert sink.failed == 2
    assert capsys.readouterr().err.count("lost 1 event(s)") == 2


def test_close_drains_the_connection() -> None:
    client = FakeNATS()
    sink = NATSSink("logs", client=client)
    sink.close()
    assert client.drained is True
    sink.close()  # idempotent (loop already closed)
